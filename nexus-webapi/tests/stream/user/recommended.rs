//! Recommended users over the fixture in docker/test-graph/mocks/recommended.cypher.
//! Each test reads a different observer's view of the same graph, so the cold
//! cache one test forces is never refilled by another.

use crate::utils::{
    recommended::{recommend_cold, D2, D3, D4, DELETED, FOLLOWED, HOP, OBS, SHORT},
    server::TestServiceServer,
};
use anyhow::Result;
use nexus_common::db::{fetch_all_rows_from_graph, queries};

/// The recommended ids of `user_id` as the graph query returns them, sorted: with a
/// trust ranking published the query breaks trust ties at random, and without one it
/// does not order at all.
async fn get_sorted_recommended_ids_from_graph(user_id: &str) -> Result<Vec<String>> {
    let (mut recommended_ids, _) = recommend_cold(user_id, 20).await?;
    recommended_ids.sort();
    Ok(recommended_ids)
}

#[tokio_shared_rt::test(shared)]
async fn test_stream_recommended_result_set() -> Result<()> {
    let recommended_ids = get_sorted_recommended_ids_from_graph(OBS).await?;

    assert!(
        !recommended_ids.contains(&FOLLOWED.to_string()),
        "A directly followed user should not be recommended, even if reachable at depth 2"
    );
    assert!(
        !recommended_ids.contains(&HOP.to_string()),
        "A directly followed user should not be recommended"
    );
    assert!(
        !recommended_ids.contains(&OBS.to_string()),
        "A follow cycle should not recommend the user to themselves"
    );
    assert!(
        !recommended_ids.contains(&SHORT.to_string()),
        "A user one post short of the threshold should not be recommended"
    );
    assert!(
        !recommended_ids.contains(&DELETED.to_string()),
        "A deleted user should not be recommended"
    );
    assert!(
        !recommended_ids.contains(&D4.to_string()),
        "A user at depth 4 should not be recommended"
    );

    // D2 is on the post threshold and reached over two paths: listed exactly once
    let mut expected_ids = vec![D2.to_string(), D3.to_string()];
    expected_ids.sort();
    assert_eq!(
        recommended_ids, expected_ids,
        "Only the active users at depth 2 and 3 should be recommended, each once"
    );

    Ok(())
}

/// HOP follows everyone OBS reaches at depth 2, which moves the whole graph one
/// step closer: D4 comes into range at depth 3 and the depth 2 users of OBS are
/// directly followed.
#[tokio_shared_rt::test(shared)]
async fn test_stream_recommended_depth_is_relative_to_the_user() -> Result<()> {
    let recommended_ids = get_sorted_recommended_ids_from_graph(HOP).await?;

    let mut expected_ids = vec![D3.to_string(), D4.to_string()];
    expected_ids.sort();
    assert_eq!(
        recommended_ids, expected_ids,
        "Only the users at depth 2 and 3 of HOP should be recommended"
    );

    Ok(())
}

/// The endpoint runs the trust rule while the fixture ranking is published, so the cases
/// above pin the query without it here: that is what serves an install with no ranking.
/// Every user in this fixture is ranked, so the expected sets are the same.
#[tokio_shared_rt::test(shared)]
async fn test_stream_recommended_result_set_without_the_trust_rule() -> Result<()> {
    // Ensure the server is running, so the Neo4j pool is initialized
    TestServiceServer::get_test_server().await;

    for (user_id, expected) in [(OBS, [D2, D3]), (HOP, [D3, D4])] {
        let query = queries::get::recommend_users(user_id, 30, false);
        let mut recommended_ids = fetch_all_rows_from_graph(query)
            .await?
            .iter()
            .map(|row| row.get::<String>("recommended_user_id"))
            .collect::<Result<Vec<_>, _>>()?;
        recommended_ids.sort();

        let mut expected_ids = expected.map(str::to_string);
        expected_ids.sort();
        assert_eq!(
            recommended_ids, expected_ids,
            "Without the trust rule, {user_id} should get the active users at depth 2 and 3, each once"
        );
    }

    Ok(())
}
