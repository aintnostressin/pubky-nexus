use async_trait::async_trait;
use futures::channel::{mpsc, oneshot};
use futures::stream::{self, BoxStream};
use futures::{SinkExt, StreamExt};
use neo4rs::Row;
use std::io;

use super::query::Query;

/// Rows a query task may read ahead of its caller.
const ROWS_AHEAD: usize = 64;

/// Abstraction over graph database operations.
/// Callers depend on this trait, not the concrete implementations.
#[async_trait]
pub trait GraphOps: Send + Sync {
    /// Execute query, return boxed row stream.
    async fn execute(
        &self,
        query: Query,
    ) -> neo4rs::Result<BoxStream<'static, Result<Row, neo4rs::Error>>>;

    /// Fire-and-forget query execution.
    async fn run(&self, query: Query) -> neo4rs::Result<()>;
}

/// Thin wrapper around `neo4rs::Graph` implementing `GraphOps` without any observability overhead.
///
/// Every query runs in its own task, so a caller dropped mid-query (a client
/// disconnect, a timeout) cannot cut its Bolt exchange short. neo4rs 0.8 would
/// pool such a connection with a reply still unread, and the next query on it
/// would read that reply as its own: a write that commits but returns no rows.
/// neo4rs fixed this after 0.9.0-rc.10 (neo4j-labs/neo4rs#298).
#[derive(Clone)]
pub struct Graph {
    inner: neo4rs::Graph,
}

impl Graph {
    pub fn new(graph: neo4rs::Graph) -> Self {
        Self { inner: graph }
    }
}

#[async_trait]
impl GraphOps for Graph {
    async fn execute(
        &self,
        query: Query,
    ) -> neo4rs::Result<BoxStream<'static, Result<Row, neo4rs::Error>>> {
        let graph = self.inner.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let (mut rows_tx, rows_rx) = mpsc::channel(ROWS_AHEAD);
        let task = tokio::spawn(async move {
            let mut rows = match graph.execute(query.into()).await {
                Ok(rows) => rows,
                Err(e) => {
                    let _ = started_tx.send(Err(e));
                    return;
                }
            };
            let _ = started_tx.send(Ok(()));
            // `next` returns only once it has read a whole reply, so stopping
            // between calls never leaves one on the connection.
            while let Some(row) = rows.next().await.transpose() {
                let failed = row.is_err();
                if rows_tx.send(row).await.is_err() || failed {
                    break;
                }
            }
        });

        started_rx.await.map_err(|_| task_stopped())??;
        // A task that panics mid-stream ends with an error, not a short result.
        let end = stream::once(task)
            .filter_map(|joined| async move { joined.is_err().then(|| Err(task_stopped())) });
        Ok(rows_rx.chain(end).boxed())
    }

    async fn run(&self, query: Query) -> neo4rs::Result<()> {
        let graph = self.inner.clone();
        tokio::spawn(async move { graph.run(query.into()).await })
            .await
            .map_err(|_| task_stopped())?
    }
}

/// The query task panicked or its runtime shut down.
fn task_stopped() -> neo4rs::Error {
    io::Error::other("Neo4j query task stopped").into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Neo4JConfig;
    use futures::TryStreamExt;
    use std::time::{Duration, Instant};
    use tokio::time::timeout;

    /// Keeps Neo4j busy for a while and returns no rows, so its only reply is the
    /// SUCCESS a dropped caller would leave unread on the connection.
    const SLOW_READ: &str = "UNWIND range(1, 50000000) AS x WITH x WHERE x < 0 RETURN x";
    /// The same work as a write that writes nothing: Neo4j skips a read whose
    /// results `run` discards.
    const SLOW_WRITE: &str = "UNWIND range(1, 50000000) AS x WITH x WHERE x < 0 CREATE ()";

    /// One connection, so the probe reuses the connection the dropped query held.
    async fn single_connection_graph() -> neo4rs::Graph {
        let config = Neo4JConfig::default();
        let config = neo4rs::ConfigBuilder::default()
            .uri(&config.uri)
            .user(&config.user)
            .password(&config.password)
            .max_connections(1)
            .build()
            .unwrap();
        neo4rs::Graph::connect(config).await.unwrap()
    }

    /// Waits on another connection until Neo4j has finished `cypher` and sent its
    /// last reply.
    async fn wait_until_finished(cypher: &str) {
        let observer = single_connection_graph().await;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let query = neo4rs::query(
                "SHOW TRANSACTIONS YIELD currentQuery
                 WHERE currentQuery = $cypher
                 RETURN count(*) AS running",
            )
            .param("cypher", cypher);
            let mut rows = observer.execute(query).await.unwrap();
            let running: i64 = rows.next().await.unwrap().unwrap().get("running").unwrap();
            if running == 0 {
                return;
            }
            assert!(Instant::now() < deadline, "{cypher} is still running");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The next query on the connection must read its own reply.
    async fn assert_in_sync(graph: &Graph) {
        let query = Query::new("probe", "RETURN 42 AS answer");
        let mut rows = graph.execute(query).await.unwrap();
        let row = rows.try_next().await.unwrap().expect("the probe's own row");
        assert_eq!(row.get::<i64>("answer").unwrap(), 42);
    }

    #[tokio_shared_rt::test(shared)]
    async fn dropped_read_leaves_the_connection_in_sync() {
        let graph = Graph::new(single_connection_graph().await);

        let mut rows = graph
            .execute(Query::new("slow_read", SLOW_READ))
            .await
            .unwrap();
        let read = timeout(Duration::from_millis(50), rows.try_next()).await;
        assert!(
            read.is_err(),
            "the read finished before it could be dropped"
        );
        drop(rows);

        wait_until_finished(SLOW_READ).await;
        assert_in_sync(&graph).await;
    }

    #[tokio_shared_rt::test(shared)]
    async fn dropped_run_leaves_the_connection_in_sync() {
        let graph = Graph::new(single_connection_graph().await);

        let run = timeout(
            Duration::from_millis(50),
            graph.run(Query::new("slow_write", SLOW_WRITE)),
        )
        .await;
        assert!(
            run.is_err(),
            "the write finished before it could be dropped"
        );

        wait_until_finished(SLOW_WRITE).await;
        assert_in_sync(&graph).await;
    }
}
