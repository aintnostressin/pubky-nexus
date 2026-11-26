//! Test harness for the migration suite: a testnet with a primary and two
//! external homeservers, nexus's indexers driven one step at a time, and user
//! actions performed through per-device SDK clients.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, ensure, Context, Result};
use nexus_common::config::EventRetryConfig;
use nexus_common::db::graph::Query;
use nexus_common::db::{
    fetch_all_rows_from_graph, fetch_key_from_graph, fetch_row_from_graph, PubkyClientError,
    PubkyClientResult, PubkyConnector,
};
use nexus_common::models::homeserver::{Homeserver, HsBlacklist};
use nexus_common::models::notification::Notification;
use nexus_common::models::post::PostCounts;
use nexus_common::models::user::{
    fresh_pkdns_record_of, get_user_homeserver, pkdns_record_of, HomeserverMapping,
    HomeserverRecheck, PerUserRead, PkdnsHomeserverResolver, PkdnsRecord, StaleMapping, UserCounts,
    UserHsCursor, UserIngestor,
};
use nexus_common::types::Pagination;
use nexus_common::utils::test_utils::default_ingestor_tests;
use nexus_common::{StackConfig, StackManager, DEFAULT_MAX_FILE_SIZE};
use nexus_watcher::events::retry::{
    InMemoryRetryStore, IndexKey, InitialBackoff, RetryEvent, RetryProcessor, RetryScheduler,
    RetryStore,
};
use nexus_watcher::events::{DefaultEventHandler, EventHandler, Moderation};
use nexus_watcher::homeserver::{HomeserverClient, HsError, HsStreamEvent, PubkyHomeserverClient};
use nexus_watcher::service::indexer::{KeyBasedEventProcessor, TEventProcessor};
use nexus_watcher::service::runner::{UserNotFoundBackoff, UserResumePoints};
use nexus_watcher::service::stats::ProcessorRunStatus;
use nexus_watcher::service::user_hs_resolver::{self, UnboundBackoff};
use nexus_watcher::service::{
    HsEventProcessorRunner, RunError, TEventProcessorRunner, KEY_BASED_RUN_BUDGET,
};
use nexus_watcher::EventProcessorError;
use pubky::pkarr::dns::rdata::TXT;
use pubky::pkarr::dns::Name;
use pubky::pkarr::SignedPacket;
use pubky::{ClientId, Keypair, Pubky, PubkyResource, PubkySession, PublicKey};
use pubky_social_specs::legacy_v0::traits::{HasIdPath, HashId};
use pubky_social_specs::legacy_v0::PubkyAppBookmark;
use pubky_social_specs::PubkyId;
use pubky_testnet::Testnet;
use tempfile::TempDir;
use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;

pub use crate::layout::Layout;
use crate::layout::Object;

/// Client ID the suite's devices sign in with: since pubky 0.10 a session is a grant issued
/// to a client.
fn test_client_id() -> ClientId {
    ClientId::new("nexus-watcher.migration").expect("static test client id is valid")
}

/// A user's public tree: owner-relative paths and their bytes.
pub type Tree = Vec<Object>;

pub struct MigrationTest {
    pub testnet: Testnet,
    /// The layout every user of this test writes.
    pub layout: Layout,
    /// Bulk-indexed by nexus.
    pub primary: PubkyId,
    pub hs_a: PubkyId,
    pub hs_b: PubkyId,
    bulk: HsEventProcessorRunner,
    /// Nexus's calls to the homeservers, through its own client.
    client: Arc<dyn HomeserverClient>,
    handler: Arc<dyn EventHandler>,
    ingestor: Arc<UserIngestor>,
    retry: Arc<RetryScheduler>,
    retry_queue: Arc<RetryQueue>,
    not_found_backoff: Arc<UserNotFoundBackoff>,
    shutdown_rx: watch::Receiver<bool>,
    _shutdown_tx: watch::Sender<bool>,
    _files: TempDir,
}

impl MigrationTest {
    /// A test whose users write `pubky.app`.
    pub async fn setup() -> Result<Self> {
        Self::setup_in(Layout::PubkyApp).await
    }

    /// A test whose users write `layout`.
    pub async fn setup_in(layout: Layout) -> Result<Self> {
        StackManager::setup(&StackConfig::default())
            .await
            .map_err(|e| anyhow!("stack setup failed: {e:?}"))?;
        let files = TempDir::new()?;

        let mut testnet = Testnet::new_unseeded().await?;
        testnet.create_http_relay().await?;
        let primary = PubkyId::from(testnet.create_random_homeserver().await?.public_key());
        let hs_a = PubkyId::from(testnet.create_random_homeserver().await?.public_key());
        let hs_b = PubkyId::from(testnet.create_random_homeserver().await?.public_key());
        Homeserver::persist_if_unknown(primary.clone())
            .await
            .map_err(|e| anyhow!("persisting the primary: {e}"))?;

        // Nexus's own client: one per process, bound to this testnet.
        PubkyConnector::init_from(testnet.sdk()?)
            .await
            .map_err(|e| anyhow!("pubky connector: {e}"))?;
        let pubky = PubkyConnector::get().map_err(|e| anyhow!("pubky connector: {e}"))?;
        let client: Arc<dyn HomeserverClient> =
            Arc::new(PubkyHomeserverClient::new(pubky, DEFAULT_MAX_FILE_SIZE));

        let moderation = Arc::new(Moderation {
            id: PubkyId::from(Keypair::random().public_key()),
            tags: Vec::new(),
        });
        let ingestor = default_ingestor_tests();
        let retry_queue = Arc::new(RetryQueue::default());
        let store: Arc<dyn RetryStore> = retry_queue.clone();
        let handler: Arc<dyn EventHandler> = Arc::new(DefaultEventHandler::new(
            moderation,
            ingestor.clone(),
            client.clone(),
            files.path().to_path_buf(),
            store.clone(),
        ));
        let retry = Arc::new(RetryScheduler::new(
            store,
            InitialBackoff {
                missing_dep_ms: 60_000,
                transient_ms: 10_000,
            },
        ));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let bulk = HsEventProcessorRunner {
            limit: 1000,
            event_handler: handler.clone(),
            client: client.clone(),
            shutdown_rx: shutdown_rx.clone(),
            primary_homeserver: primary.clone(),
            retry_scheduler: retry.clone(),
        };

        Ok(Self {
            testnet,
            layout,
            primary,
            hs_a,
            hs_b,
            bulk,
            client,
            handler,
            ingestor,
            retry,
            retry_queue,
            not_found_backoff: Arc::new(UserNotFoundBackoff::default()),
            shutdown_rx,
            _shutdown_tx: shutdown_tx,
            _files: files,
        })
    }

    // ----- nexus, one step at a time -----

    /// One poll of the primary's bulk stream from the stored cursor.
    pub async fn index_primary(&self) -> Result<()> {
        let stats = self.bulk.run().await.map_err(|e| anyhow!("{e}"))?;
        for run in &stats.0.stats {
            ensure!(
                run.status == ProcessorRunStatus::Ok,
                "bulk run of {} ended with {:?}",
                run.hs_id,
                run.status
            );
        }
        Ok(())
    }

    /// One per-user read of every user bound to `hs`.
    pub async fn index_external(&self, hs: &PubkyId) -> Result<()> {
        self.key_based(hs.clone(), self.client.clone())
            .await?
            .run()
            .await
            .map_err(|e| anyhow!("{e:?}"))
    }

    /// A per-user read of `hs` that has listed its users and stops right
    /// before fetching `user`'s events, until [`PausedRead::resume`]. One read
    /// reaches the users of a busy homeserver minutes apart, so this is a read
    /// that started before a switch, caught while the switch happens.
    pub async fn index_external_paused(&self, hs: &PubkyId, user: &Keypair) -> Result<PausedRead> {
        let source = Arc::new(PausingClient {
            inner: self.client.clone(),
            user: user.public_key(),
            reached: Notify::new(),
            resume: Notify::new(),
        });
        let run = tokio::spawn(self.key_based(hs.clone(), source.clone()).await?.run());
        tokio::time::timeout(Duration::from_secs(30), source.reached.notified())
            .await
            .context("the read never reached the user")?;
        Ok(PausedRead { source, run })
    }

    /// One per-user read of the primary, for the users its bulk stream left:
    /// how a user's own history there is read after a switch to it, and what
    /// they write there from then on.
    pub async fn catch_up(&self) -> Result<()> {
        self.index_external(&self.primary).await
    }

    /// One resolver sweep with stale mapping disabled, in which only `user` is
    /// looked up, with a fresh network lookup. Every other user in the shared
    /// graph gets a failed lookup, which the resolver leaves alone.
    pub async fn resolve(&self, user: &Keypair) -> Result<()> {
        self.sweep(user, Lookup::Fresh).await
    }

    /// One sweep with stale mapping disabled in which only `user` is looked
    /// up, the way the production resolver does it: cache-first, through
    /// `client`. A client other than nexus's own stands for the watcher
    /// process when the recheck runs in the API process: each process has its
    /// own pkarr cache, and the watcher's holds what its last sweep saw.
    pub async fn resolve_cached(&self, user: &Keypair, client: &Pubky) -> Result<()> {
        self.sweep(user, Lookup::Cached(client.clone())).await
    }

    async fn sweep(&self, user: &Keypair, how: Lookup) -> Result<()> {
        let resolver = TestLookup {
            user: user.public_key().to_z32(),
            how,
        };
        let mut shutdown_rx = self.shutdown_rx.clone();
        user_hs_resolver::run(
            &resolver,
            0,
            &UnboundBackoff::default(),
            &mut shutdown_rx,
            StaleMapping::Disabled,
        )
        .await
        .map_err(|e| anyhow!("{e}"))
    }

    /// One pass of the retry processor over everything queued so far, as if
    /// every entry's backoff had passed.
    pub async fn retry(&self) -> Result<()> {
        self.retry_queue.all_due.store(true, Ordering::SeqCst);
        Arc::new(RetryProcessor {
            event_handler: self.handler.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
            config: EventRetryConfig::default(),
            store: self.retry_queue.clone(),
        })
        .run()
        .await
        .map_err(|e| anyhow!("{e}"))
    }

    /// What `PUT /v0/recheck/{user}` does: a fresh lookup of the user's record,
    /// followed wherever it points.
    pub async fn recheck(&self, user: &Keypair) -> Result<HomeserverRecheck> {
        self.ingestor
            .recheck_user(&PubkyId::from(user.public_key()))
            .await
            .map_err(|e| anyhow!("rechecking the user: {e}"))?
            .ok_or_else(|| anyhow!("user unknown to nexus"))
    }

    /// What nexus does when a known user first references an unknown one:
    /// a stub node, a binding to the homeserver the record names, cursor 0.
    pub async fn ingest(&self, user: &Keypair) -> Result<()> {
        self.ingestor
            .maybe_ingest_user(&PubkyId::from(user.public_key()))
            .await
            .map_err(|e| anyhow!("ingesting the user: {e}"))
    }

    async fn key_based(
        &self,
        hs: PubkyId,
        client: Arc<dyn HomeserverClient>,
    ) -> Result<Arc<KeyBasedEventProcessor>> {
        // As the external loop builds it: the primary is read for the users its bulk read left.
        let left_by_bulk_read = if hs == self.primary {
            Some(PerUserRead::starts(&hs).await?)
        } else {
            None
        };
        Ok(Arc::new(KeyBasedEventProcessor {
            homeserver_id: hs,
            left_by_bulk_read,
            limit: 100,
            event_handler: self.handler.clone(),
            client,
            user_not_found_backoff: self.not_found_backoff.clone(),
            hs_blacklist: HsBlacklist::default(),
            retry_scheduler: self.retry.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
            // One read at a time, each a full pass: nothing to resume.
            user_resume_points: Arc::new(UserResumePoints::default()),
            budget: KEY_BASED_RUN_BUDGET,
        }))
    }

    // ----- users, through their own devices -----

    /// A user living on `hs` as nexus first meets them: signed up through a
    /// new device with `posts` written, ingested, and read once. Returns the
    /// device, the session and the post ids.
    pub async fn resident(
        &self,
        user: &Keypair,
        hs: &PubkyId,
        name: &str,
        posts: &[&str],
    ) -> Result<(Pubky, PubkySession, Vec<String>)> {
        let device = self.device()?;
        let session = self.signup(&device, user, hs, name).await?;
        let mut ids = Vec::with_capacity(posts.len());
        for text in posts {
            ids.push(self.post(&session, text, None).await?);
        }
        self.ingest(user).await?;
        self.index_external(hs).await?;
        Ok((device, session, ids))
    }

    /// The client's move: sign `user` up on `hs` through a new device, which
    /// publishes the record naming `hs`, and copy the public tree over as
    /// `from` reads it. Returns the new device and session.
    pub async fn signup_and_copy(
        &self,
        user: &Keypair,
        hs: &PubkyId,
        name: &str,
        from: &Pubky,
    ) -> Result<(Pubky, PubkySession)> {
        let device = self.device()?;
        let session = self.signup(&device, user, hs, name).await?;
        self.write_tree(&session, &self.public_tree(from, user).await?)
            .await?;
        Ok((device, session))
    }

    /// A fresh SDK client with its own pkarr cache: one user device.
    pub fn device(&self) -> Result<Pubky> {
        Ok(self.testnet.sdk()?)
    }

    /// Signs `user` up on `hs` through `device`, signs in and writes a profile. The SDK
    /// publishes the record naming `hs`.
    pub async fn signup(
        &self,
        device: &Pubky,
        user: &Keypair,
        hs: &PubkyId,
        name: &str,
    ) -> Result<PubkySession> {
        let session = self.open_account(device, user, hs).await?;
        self.write_profile(&session, name).await?;
        Ok(session)
    }

    /// [`Self::signup`] without the profile: an account on `hs`, the record
    /// naming it and a session, for a copy that writes the profile later.
    pub async fn open_account(
        &self,
        device: &Pubky,
        user: &Keypair,
        hs: &PubkyId,
    ) -> Result<PubkySession> {
        let hs_pk = hs.to_public_key().map_err(anyhow::Error::msg)?;
        let signer = device.signer(user.clone());
        signer.signup(&hs_pk, None).await?;
        Ok(signer.signin(test_client_id()).await?)
    }

    pub async fn write_profile(&self, session: &PubkySession, name: &str) -> Result<()> {
        self.put(session, self.layout.profile(name)?).await
    }

    pub async fn delete_profile(&self, session: &PubkySession) -> Result<()> {
        session
            .storage()
            .delete(self.layout.profile_path().as_str())
            .await?;
        Ok(())
    }

    /// A session on the homeserver `device` currently resolves for `user`.
    pub async fn session(&self, device: &Pubky, user: &Keypair) -> Result<PubkySession> {
        Ok(device.signer(user.clone()).signin(test_client_id()).await?)
    }

    /// Writes a short post, a reply when `parent` is given. Returns the post id.
    pub async fn post(
        &self,
        session: &PubkySession,
        text: &str,
        parent: Option<String>,
    ) -> Result<String> {
        let (id, post) = self.layout.post(text, parent)?;
        self.put(session, post).await?;
        // Post ids are timestamps: keep consecutive posts distinct.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        Ok(id)
    }

    /// Edits a post that is no reply.
    pub async fn edit_post(&self, session: &PubkySession, post_id: &str, text: &str) -> Result<()> {
        self.put(session, self.layout.post_edit(post_id, text)?)
            .await
    }

    pub async fn delete_post(&self, session: &PubkySession, post_id: &str) -> Result<()> {
        session
            .storage()
            .delete(self.layout.post_path(post_id).as_str())
            .await?;
        Ok(())
    }

    /// The address a reply, a tag or a bookmark names `user`'s post by.
    pub fn post_uri(&self, user: &Keypair, post_id: &str) -> String {
        self.layout.post_uri(user.public_key().to_z32(), post_id)
    }

    pub async fn follow(&self, session: &PubkySession, target: &str) -> Result<()> {
        self.put(session, self.layout.follow(target)?).await
    }

    pub async fn tag(&self, session: &PubkySession, uri: String, label: &str) -> Result<()> {
        self.put(session, self.layout.tag(uri, label)?).await
    }

    /// `pubky.app` only: a `social/v1` bookmark is private, and nexus reads
    /// what the event streams carry, which is the public tree.
    pub async fn bookmark(&self, session: &PubkySession, uri: String) -> Result<()> {
        ensure!(
            self.layout == Layout::PubkyApp,
            "a social/v1 bookmark is private: nexus never sees one"
        );
        let bookmark = PubkyAppBookmark {
            uri,
            created_at: chrono::Utc::now().timestamp_millis(),
        };
        let path = PubkyAppBookmark::create_path(&bookmark.create_id());
        session
            .storage()
            .put(path.as_str(), serde_json::to_vec(&bookmark)?)
            .await?;
        Ok(())
    }

    /// Uploads `bytes` as the app does: in `pubky.app` a blob, then the file
    /// record naming it; in `social/v1` the one file.
    pub async fn upload(&self, session: &PubkySession, user: &Keypair, bytes: &[u8]) -> Result<()> {
        let upload = self.layout.upload(user.public_key().to_z32(), bytes)?;
        self.write_tree(session, &upload).await
    }

    async fn put(&self, session: &PubkySession, (path, bytes): Object) -> Result<()> {
        session.storage().put(path.as_str(), bytes).await?;
        Ok(())
    }

    /// Every public resource `user` wrote in the test's layout, as `source`
    /// sees it: the read goes to whichever homeserver `source`'s cache names.
    /// The profile comes first, as a copy would write it.
    pub async fn public_tree(&self, source: &Pubky, user: &Keypair) -> Result<Tree> {
        let root = self.layout.tree_root(user.public_key().to_z32());
        let resources = source
            .public_storage()
            .list(root.as_str())?
            .limit(1000)
            .send()
            .await?;
        let mut tree = Vec::with_capacity(resources.len());
        for resource in resources {
            let bytes = source
                .public_storage()
                .get(resource.to_pubky_url().as_str())
                .await?
                .bytes()
                .await?;
            tree.push((resource.path.as_str().to_string(), bytes.to_vec()));
        }
        let profile = self.layout.profile_path();
        tree.sort_by_key(|(path, _)| (!same_path(path, &profile), path.clone()));
        Ok(tree)
    }

    /// `tree` in the order of its paths, as a copy that walks the folders
    /// writes it. In `social/v1` that puts files, follows and posts ahead of
    /// `profile.json` and tags after it; in `pubky.app` only tags come after.
    pub fn in_path_order(mut tree: Tree) -> Tree {
        tree.sort();
        tree
    }

    /// Writes `tree` through `session`: the copy step of a migration.
    pub async fn write_tree(&self, session: &PubkySession, tree: &Tree) -> Result<()> {
        for (path, bytes) in tree {
            session.storage().put(path.as_str(), bytes.clone()).await?;
        }
        Ok(())
    }

    /// Deletes every path in `tree` through `session`, the profile last.
    pub async fn delete_tree(&self, session: &PubkySession, tree: &Tree) -> Result<()> {
        for (path, _) in tree.iter().rev() {
            session.storage().delete(path.as_str()).await?;
        }
        Ok(())
    }

    /// Publishes the user's record naming `hs`, from `device`.
    pub async fn switch_record(&self, device: &Pubky, user: &Keypair, hs: &PubkyId) -> Result<()> {
        let hs = &hs.to_public_key().map_err(anyhow::Error::msg)?;
        device
            .signer(user.clone())
            .pkdns()
            .publish_homeserver_force(Some(hs))
            .await
            .context("publishing the record")?;
        Ok(())
    }

    /// Publishes a packet for `user` with no `_pubky` record at all.
    pub async fn publish_without_homeserver(&self, device: &Pubky, user: &Keypair) -> Result<()> {
        let packet = SignedPacket::builder()
            .txt(
                Name::new("_lab")?,
                TXT::new().with_string("no homeserver here")?,
                300,
            )
            .sign(user)?;
        device
            .client()
            .pkarr()
            .publish(&packet)
            .await
            .context("publishing the packet")?;
        Ok(())
    }

    // ----- what nexus holds -----

    /// The user's `HOSTED_BY` binding as nexus stores it, if any.
    pub async fn binding(&self, user: &Keypair) -> Result<Option<HomeserverMapping>> {
        Ok(get_user_homeserver(&user.public_key().to_z32()).await?)
    }

    pub async fn bound_to(&self, user: &Keypair) -> Result<Option<String>> {
        Ok(self.binding(user).await?.map(|mapping| mapping.hs_id))
    }

    /// [`Self::bound_to`] by name, for readable assertions: `primary`, `hs_a`,
    /// `hs_b`, `elsewhere` or `nowhere`.
    pub async fn bound_name(&self, user: &Keypair) -> Result<&'static str> {
        Ok(match self.bound_to(user).await? {
            None => "nowhere",
            Some(hs) if hs == self.primary.as_ref() => "primary",
            Some(hs) if hs == self.hs_a.as_ref() => "hs_a",
            Some(hs) if hs == self.hs_b.as_ref() => "hs_b",
            Some(_) => "elsewhere",
        })
    }

    /// The user's per-user cursor on `hs`, 0 when none is stored.
    pub async fn cursor(&self, user: &Keypair, hs: &PubkyId) -> Result<u64> {
        let user_id = user.public_key().to_z32();
        let cursors = UserHsCursor::read(&[user_id.as_str()], hs.as_ref()).await?;
        Ok(cursors.into_iter().next().unwrap_or_default())
    }

    /// The bulk cursor on the primary.
    pub async fn bulk_cursor(&self) -> Result<u64> {
        Ok(Homeserver::get_from_index(self.primary.as_ref())
            .await?
            .ok_or_else(|| anyhow!("primary missing from the index"))?
            .cursor)
    }

    /// Non-deleted posts of `user` in the graph.
    pub async fn live_posts(&self, user: &Keypair) -> Result<i64> {
        let query = Query::new(
            "migration_live_posts",
            "MATCH (u:User {id: $id})-[:AUTHORED]->(p:Post)
             WHERE NOT coalesce(p.deleted, false)
             RETURN count(p) AS n",
        )
        .param("id", user.public_key().to_z32());
        Ok(fetch_key_from_graph(query, "n").await?.unwrap_or(0))
    }

    /// The indexed content of one of `user`'s posts.
    pub async fn post_content(&self, user: &Keypair, post_id: &str) -> Result<String> {
        let query = Query::new(
            "migration_post_content",
            "MATCH (u:User {id: $id})-[:AUTHORED]->(p:Post {id: $post_id}) RETURN p.content AS c",
        )
        .param("id", user.public_key().to_z32())
        .param("post_id", post_id.to_string());
        fetch_key_from_graph(query, "c")
            .await?
            .ok_or_else(|| anyhow!("post missing"))
    }

    /// `(deleted, name)` of the user node.
    pub async fn profile_state(&self, user: &Keypair) -> Result<(bool, String)> {
        let row = fetch_row_from_graph(
            Query::new(
                "migration_profile_state",
                "MATCH (u:User {id: $id})
                 RETURN coalesce(u.deleted, false) AS deleted, coalesce(u.name, '') AS name",
            )
            .param("id", user.public_key().to_z32()),
        )
        .await?
        .ok_or_else(|| anyhow!("user node missing"))?;
        Ok((row.get("deleted")?, row.get("name")?))
    }

    /// `user`'s posts as nexus holds them, by id: `(id, deleted, indexed_at)`.
    /// The time is the post's place in every timeline.
    pub async fn post_times(&self, user: &Keypair) -> Result<Vec<(String, bool, i64)>> {
        let query = Query::new(
            "migration_post_times",
            "MATCH (u:User {id: $id})-[:AUTHORED]->(p:Post)
             RETURN p.id AS id, coalesce(p.deleted, false) AS deleted, p.indexed_at AS at
             ORDER BY id",
        )
        .param("id", user.public_key().to_z32());
        let mut posts = Vec::new();
        for row in fetch_all_rows_from_graph(query).await? {
            posts.push((row.get("id")?, row.get("deleted")?, row.get("at")?));
        }
        Ok(posts)
    }

    /// What `user` points at with a relationship of `kind` (`FOLLOWS`,
    /// `TAGGED`, `BOOKMARKED`) and when nexus indexed each, by target. A tag
    /// is named by its label too.
    pub async fn edge_times(&self, user: &Keypair, kind: &str) -> Result<Vec<(String, i64)>> {
        let query = Query::new(
            "migration_edge_times",
            "MATCH (u:User {id: $id})-[r]->(n)
             WHERE type(r) = $kind
             RETURN coalesce(r.label + ' on ', '') + n.id AS target, r.indexed_at AS at
             ORDER BY target",
        )
        .param("id", user.public_key().to_z32())
        .param("kind", kind.to_string());
        let mut edges = Vec::new();
        for row in fetch_all_rows_from_graph(query).await? {
            edges.push((row.get("target")?, row.get("at")?));
        }
        Ok(edges)
    }

    /// The counts nexus serves for `user` and for each of their posts.
    pub async fn counts(&self, user: &Keypair) -> Result<serde_json::Value> {
        let user_id = user.public_key().to_z32();
        let mut posts = serde_json::Map::new();
        for (post_id, _, _) in self.post_times(user).await? {
            let counts = PostCounts::get_by_id(&user_id, &post_id).await?;
            posts.insert(post_id, serde_json::to_value(counts)?);
        }
        Ok(serde_json::json!({
            "user": UserCounts::get_by_id(&user_id).await?,
            "posts": posts,
        }))
    }

    /// `(file records of user in the graph, files nexus stores on disk)`. The
    /// disk is this test's own, so the second number counts every user's.
    pub async fn files(&self, user: &Keypair) -> Result<(i64, usize)> {
        let query = Query::new(
            "migration_files",
            "MATCH (f:File {owner_id: $id}) RETURN count(f) AS n",
        )
        .param("id", user.public_key().to_z32());
        let records = fetch_key_from_graph(query, "n").await?.unwrap_or(0);
        Ok((records, stored_files(self._files.path())?))
    }

    /// How many events are waiting in the retry queue.
    pub async fn queued_retries(&self) -> Result<usize> {
        Ok(self
            .retry_queue
            .entries
            .fetch_ready(i64::MAX, None)
            .await
            .map_err(|e| anyhow!("{e}"))?
            .len())
    }

    /// The notification kinds of `user` with their times, oldest first. Two
    /// notifications with the same body share one entry whose time is the
    /// latest, so a notification issued again shows up here as a newer time,
    /// not as a second entry.
    pub async fn notification_times(&self, user: &Keypair) -> Result<Vec<(String, i64)>> {
        let pagination = Pagination {
            limit: Some(1000),
            ..Pagination::default()
        };
        let mut entries: Vec<(String, i64)> =
            Notification::get_by_id(&user.public_key().to_z32(), pagination)
                .await?
                .into_iter()
                .map(|n| (notification_kind(&n), n.timestamp))
                .collect();
        entries.reverse();
        Ok(entries)
    }

    /// The notification kinds of `user`, oldest first.
    pub async fn notifications(&self, user: &Keypair) -> Result<Vec<String>> {
        let times = self.notification_times(user).await?;
        Ok(times.into_iter().map(|(kind, _)| kind).collect())
    }
}

/// A resolver for the sweep that looks up one user, this test's, and fails
/// the lookup of everyone else so the shared graph is left untouched.
struct TestLookup {
    user: String,
    how: Lookup,
}

/// How the sweep looks the user up.
enum Lookup {
    /// A fresh network lookup through nexus's own client, which also refreshes
    /// its pkarr cache.
    Fresh,
    /// `get_homeserver_of` through a client of its own, as the production
    /// resolver does: pkarr's `resolve` answers from that client's cache, even
    /// with an expired packet, and refreshes it in the background.
    Cached(Pubky),
}

#[async_trait::async_trait]
impl PkdnsHomeserverResolver for TestLookup {
    async fn resolve_record(&self, user_pk: &PublicKey) -> PubkyClientResult<PkdnsRecord> {
        if user_pk.to_z32() != self.user {
            return Err(PubkyClientError::RequestFailed {
                message: "not the user under test".to_string(),
            });
        }
        match &self.how {
            Lookup::Fresh => {
                let pubky = PubkyConnector::get()?;
                fresh_pkdns_record_of(&pubky, user_pk).await
            }
            Lookup::Cached(client) => pkdns_record_of(client, user_pk).await,
        }
    }
}

/// The client of [`MigrationTest::index_external_paused`]: nexus's own, with
/// the fetch of one user's events held until resumed.
struct PausingClient {
    inner: Arc<dyn HomeserverClient>,
    user: PublicKey,
    reached: Notify,
    resume: Notify,
}

#[async_trait::async_trait]
impl PkdnsHomeserverResolver for PausingClient {
    async fn resolve_record(&self, user_pk: &PublicKey) -> PubkyClientResult<PkdnsRecord> {
        self.inner.resolve_record(user_pk).await
    }

    async fn resolve_record_fresh(&self, user_pk: &PublicKey) -> PubkyClientResult<PkdnsRecord> {
        self.inner.resolve_record_fresh(user_pk).await
    }
}

#[async_trait::async_trait]
impl HomeserverClient for PausingClient {
    async fn poll_events(
        &self,
        hs: &PubkyId,
        cursor: u64,
        limit: u16,
    ) -> Result<Vec<String>, HsError> {
        self.inner.poll_events(hs, cursor, limit).await
    }

    async fn user_events(
        &self,
        hs: &PubkyId,
        user: &PublicKey,
        cursor: u64,
        limit: u16,
    ) -> Result<Vec<HsStreamEvent>, HsError> {
        if *user == self.user {
            self.reached.notify_one();
            self.resume.notified().await;
        }
        self.inner.user_events(hs, user, cursor, limit).await
    }

    async fn get_resource(
        &self,
        hs: &PubkyId,
        resource: &PubkyResource,
    ) -> Result<Vec<u8>, HsError> {
        self.inner.get_resource(hs, resource).await
    }

    async fn get_blob(
        &self,
        hs: Option<&PubkyId>,
        resource: &PubkyResource,
    ) -> Result<Vec<u8>, HsError> {
        self.inner.get_blob(hs, resource).await
    }
}

/// A per-user read stopped before one user's fetch.
pub struct PausedRead {
    source: Arc<PausingClient>,
    run: JoinHandle<Result<(), RunError>>,
}

impl PausedRead {
    /// Lets the read fetch the user's events now and finish.
    pub async fn resume(self) -> Result<()> {
        self.source.resume.notify_one();
        self.run.await?.map_err(|e| anyhow!("{e:?}"))
    }
}

/// The suite's retry queue: in memory, so tests running in parallel processes
/// never see each other's entries, and with a switch that makes every entry
/// due for one pass of the retry processor.
#[derive(Default)]
struct RetryQueue {
    entries: InMemoryRetryStore,
    all_due: AtomicBool,
}

#[async_trait::async_trait]
impl RetryStore for RetryQueue {
    async fn put(&self, event: &RetryEvent) -> Result<(), EventProcessorError> {
        self.entries.put(event).await
    }

    async fn get(&self, index_key: &IndexKey) -> Result<Option<RetryEvent>, EventProcessorError> {
        self.entries.get(index_key).await
    }

    async fn remove(&self, index_key: &IndexKey) -> Result<(), EventProcessorError> {
        self.entries.remove(index_key).await
    }

    async fn remove_if(
        &self,
        index_key: &IndexKey,
        expected_nonce: i64,
    ) -> Result<bool, EventProcessorError> {
        self.entries.remove_if(index_key, expected_nonce).await
    }

    async fn put_if(
        &self,
        event: &RetryEvent,
        expected_nonce: i64,
    ) -> Result<bool, EventProcessorError> {
        self.entries.put_if(event, expected_nonce).await
    }

    /// The first fetch after [`MigrationTest::retry`] takes every entry; the
    /// processor's next fetch sees only what is really due, which ends the pass.
    async fn fetch_ready(
        &self,
        now: i64,
        limit: Option<usize>,
    ) -> Result<Vec<(IndexKey, RetryEvent)>, EventProcessorError> {
        let now = match self.all_due.swap(false, Ordering::SeqCst) {
            true => i64::MAX,
            false => now,
        };
        self.entries.fetch_ready(now, limit).await
    }
}

fn notification_kind(notification: &Notification) -> String {
    serde_json::to_value(&notification.body)
        .ok()
        .and_then(|v| v.get("type").and_then(|t| t.as_str()).map(str::to_string))
        .unwrap_or_default()
}

/// How many files named `main`, the stored original of an upload, are under `dir`.
fn stored_files(dir: &std::path::Path) -> Result<usize> {
    let mut count = 0;
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            count += stored_files(&path)?;
        } else if path.file_name().is_some_and(|name| name == "main") {
            count += 1;
        }
    }
    Ok(count)
}

/// Whether two owner-relative paths name one resource, with or without the
/// leading slash.
pub fn same_path(a: &str, b: &str) -> bool {
    a.trim_start_matches('/') == b.trim_start_matches('/')
}
