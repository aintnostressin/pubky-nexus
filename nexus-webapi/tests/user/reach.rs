use crate::post::CAIRO_USER;
use crate::stream::post::{AMSTERDAM, BOGOTA};
use crate::utils::follows::{
    A, B, C, D, E, HUB, HUB_FOLLOWERS, HUB_FOLLOWERS_NEWEST, HUB_FOLLOWING, HUB_FOLLOWING_NEWEST,
    HUB_FRIENDS, HUB_FRIENDS_NEWEST, LONER,
};
use crate::utils::recommended::{DELETED, HOP};
use crate::utils::{get_request, invalid_get_request, post_request, server::TestServiceServer};
use anyhow::Result;
use axum::http::StatusCode;
use nexus_common::db::RedisOps;
use nexus_common::models::follow::{Followers, Following, Friends, UserFollows};
use nexus_common::models::post::{PostStream, StreamSource};

const NON_EXISTING_USER_ID: &str = "qca6wzjg4okp6g1hwr9g8hmx1po1jpoirjfau9ejsws1qz3t7iiy";

#[tokio_shared_rt::test(shared)]
async fn test_get_followers() -> Result<()> {
    let user_id = "4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro";
    let res = get_request(&format!("/v0/user/{user_id}/followers")).await?;

    assert!(res.is_array());
    let followers: Vec<String> = res
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_str().unwrap().to_string())
        .collect();

    // List of specified IDs expected to follow the user
    let specified_follower_ids = vec![
        "ywng83zf5paobxptt8ipkdgq5karppe3edxy1kigb7kgwai56uxo".to_string(),
        "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy".to_string(),
        "uxni6dn45bbnd7mw6ypf3swoyey9wjntmjo4h1ph9xab1jfhp8do".to_string(),
        "o1gg96ewuojmopcjbz8895478wdtxtzzuxnfjjz8o8e77csa1ngo".to_string(),
        "kt1ujy3zxs1tpxsxrqkdpmon5co959paiknw1s4r1rf1gsnqxnao".to_string(),
        "hs8iszgmxharf4omxwr7zej196zr4rs4a53ks4tg1ya1efejupty".to_string(),
        "hj6e38w9dkmpkdmb9c9n6k1yt85ekbqhh3s4aagksdj4zssxg36o".to_string(),
        "h3fghnb3x59oh7r53x8y6a5x38oatqyjym9b31ybss17zqdnhcoy".to_string(),
        "gxk8itzrnikrpshfsudgsgtxrz59ojp4iwmp4w9iff3ess6zfr4y".to_string(),
        "9x86hgp5tya98csx4wfdj1aorcxszxq5mwa3rdeh8a7oz1u6sg3y".to_string(),
    ];

    // Check if the user has the expected number of followers
    assert_eq!(
        followers.len(),
        specified_follower_ids.len(),
        "Unexpected number of followers"
    );

    // Check if all specified follower IDs are present in the followers list
    for id in &specified_follower_ids {
        assert!(followers.contains(id), "Missing follower ID: {id}");
    }

    // Test non-existing user
    invalid_get_request(
        &format!("/v0/user/{NON_EXISTING_USER_ID}/followers"),
        StatusCode::NOT_FOUND,
    )
    .await?;

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_get_following() -> Result<()> {
    let user_id = "4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro";
    let res = get_request(&format!("/v0/user/{user_id}/following")).await?;

    assert!(res.is_array());
    let following: Vec<String> = res
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_str().unwrap().to_string())
        .collect();

    // List of specified IDs the user is expected to be following
    let specified_ids = vec![
        "5g3fwnue819wfdjwiwm8qr35ww6uxxgbzrigrtdgmbi19ksioeoy".to_string(),
        "9x86hgp5tya98csx4wfdj1aorcxszxq5mwa3rdeh8a7oz1u6sg3y".to_string(),
        "end1obs8cy3ssqzhm73hiojwpakb4ac1fiubbmk5zfuruaaumwso".to_string(),
        "gxk8itzrnikrpshfsudgsgtxrz59ojp4iwmp4w9iff3ess6zfr4y".to_string(),
        "hj6e38w9dkmpkdmb9c9n6k1yt85ekbqhh3s4aagksdj4zssxg36o".to_string(),
        "hs8iszgmxharf4omxwr7zej196zr4rs4a53ks4tg1ya1efejupty".to_string(),
        "hy1njbk65oh5jqdmq37rdh9xfmxojmn1jn93o7zuka5kx59fdd3o".to_string(),
        "ige6m13kshp3tyhq8ragfapdibixbkzetg8zcdztwwjy16r6erno".to_string(),
        "ijfadmjkfxd6mng41jbuaqgm4adcesr5rcs1epnqtny9e43br4ro".to_string(),
        "kg671gqu6akiyuzdsqgqtupftuhbuwfx5zh6tbygmexuw6b55s4y".to_string(),
        "kt1ujy3zxs1tpxsxrqkdpmon5co959paiknw1s4r1rf1gsnqxnao".to_string(),
        "o1gg96ewuojmopcjbz8895478wdtxtzzuxnfjjz8o8e77csa1ngo".to_string(),
        "pxnu33x7jtpx9ar1ytsi4yxbp6a5o36gwhffs8zoxmbuptici1jy".to_string(),
        "uxni6dn45bbnd7mw6ypf3swoyey9wjntmjo4h1ph9xab1jfhp8do".to_string(),
        "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy".to_string(),
    ];

    // Check if the user is following the specified number of users
    assert_eq!(
        following.len(),
        specified_ids.len(),
        "Unexpected number of users followed"
    );

    // Check if all specified IDs are present in the following list
    for id in &specified_ids {
        assert!(following.contains(id), "Missing following ID: {id}");
    }

    // Test non-existing user
    invalid_get_request(
        &format!("/v0/user/{NON_EXISTING_USER_ID}/following"),
        StatusCode::NOT_FOUND,
    )
    .await?;

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_get_following_details() -> Result<()> {
    // Same users, same order, same pages as /following
    for query in ["", "?skip=1&limit=3"] {
        let ids = get_request(&format!("/v1/user/{HOP}/following{query}")).await?;
        let details = get_request(&format!("/v1/user/{HOP}/following/details{query}")).await?;

        let details = details.as_array().expect("Response should be an array");
        let detail_ids: Vec<&str> = details.iter().map(|d| d["id"].as_str().unwrap()).collect();
        let ids: Vec<&str> = ids
            .as_array()
            .unwrap()
            .iter()
            .map(|id| id.as_str().unwrap())
            .collect();
        assert!(!ids.is_empty(), "Expected followed users for {query:?}");
        assert_eq!(detail_ids, ids, "Details should follow /following{query}");

        for user in details {
            assert!(user["name"].is_string(), "Name should be a string");
            assert!(user.get("tags").is_none(), "Tags should not be returned");
            assert!(
                user.get("counts").is_none(),
                "Counts should not be returned"
            );
        }
    }

    // A followed tombstone comes back flagged, not filtered out
    let details = get_request(&format!("/v1/user/{HOP}/following/details")).await?;
    let deleted = details
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["id"] == DELETED)
        .expect("Expected the followed deleted user");
    assert_eq!(deleted["deleted"], true, "Expected deleted: true");

    invalid_get_request(
        &format!("/v1/user/{NON_EXISTING_USER_ID}/following/details"),
        StatusCode::NOT_FOUND,
    )
    .await?;
    invalid_get_request(
        &format!("/v1/user/{HOP}/following/details?limit=201"),
        StatusCode::BAD_REQUEST,
    )
    .await?;
    // /v0 is frozen: the route is /v1 only
    invalid_get_request(
        &format!("/v0/user/{HOP}/following/details"),
        StatusCode::NOT_FOUND,
    )
    .await?;

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_get_friends() -> Result<()> {
    let user_id = "4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro";
    let res = get_request(&format!("/v0/user/{user_id}/friends")).await?;

    assert!(res.is_array());
    let following: Vec<String> = res
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_str().unwrap().to_string())
        .collect();

    // List of specified IDs the user is expected to be following
    let specified_ids = vec![
        "9x86hgp5tya98csx4wfdj1aorcxszxq5mwa3rdeh8a7oz1u6sg3y".to_string(),
        "gxk8itzrnikrpshfsudgsgtxrz59ojp4iwmp4w9iff3ess6zfr4y".to_string(),
        "hj6e38w9dkmpkdmb9c9n6k1yt85ekbqhh3s4aagksdj4zssxg36o".to_string(),
        "hs8iszgmxharf4omxwr7zej196zr4rs4a53ks4tg1ya1efejupty".to_string(),
        "kt1ujy3zxs1tpxsxrqkdpmon5co959paiknw1s4r1rf1gsnqxnao".to_string(),
        "o1gg96ewuojmopcjbz8895478wdtxtzzuxnfjjz8o8e77csa1ngo".to_string(),
        "uxni6dn45bbnd7mw6ypf3swoyey9wjntmjo4h1ph9xab1jfhp8do".to_string(),
        "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy".to_string(),
    ];

    // Check if the user friends the specified number of users
    assert_eq!(
        following.len(),
        specified_ids.len(),
        "Unexpected number of friends"
    );

    // Check if all specified IDs are present in the friend list
    for id in &specified_ids {
        assert!(following.contains(id), "Missing friend ID: {id}");
    }

    // Test non-existing user
    invalid_get_request(
        &format!("/v0/user/{NON_EXISTING_USER_ID}/friends"),
        StatusCode::NOT_FOUND,
    )
    .await?;

    Ok(())
}

/// An existing user with no mutual follows must return `200 []`, not `404`.
///
/// Regression test: `Friends::get_by_id` used to collapse "user exists but has
/// no mutual friends" and "user does not exist" into the same `None`, so an
/// existing user with an empty intersection was reported as `404 User not found`
/// (inconsistent with `/followers` and `/following`, which return `200 []`).
#[tokio_shared_rt::test(shared)]
async fn test_get_friends_existing_user_without_mutuals() -> Result<()> {
    // This user exists in the fixture (it is in the main user's following list,
    // see `test_get_following`) and has both followers and followees, but no
    // reciprocal follow, so the mutual-friends intersection is empty.
    let user_id = "5g3fwnue819wfdjwiwm8qr35ww6uxxgbzrigrtdgmbi19ksioeoy";

    // `get_request` asserts a 200 status, so a regression to 404 fails here.
    let res = get_request(&format!("/v0/user/{user_id}/friends")).await?;

    assert!(
        res.is_array(),
        "an existing user's friends response must be a JSON array"
    );
    assert_eq!(
        res.as_array().unwrap().len(),
        0,
        "an existing user with no mutual follows should return an empty list"
    );

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_follows_limit_cap() -> Result<()> {
    let user_id = "4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro";

    // limit=99999999 exceeds BoundedLimit<50, 200>::MAX → 400
    invalid_get_request(
        &format!("/v0/user/{user_id}/followers?limit=99999999"),
        StatusCode::BAD_REQUEST,
    )
    .await?;

    invalid_get_request(
        &format!("/v0/user/{user_id}/following?limit=99999999"),
        StatusCode::BAD_REQUEST,
    )
    .await?;

    invalid_get_request(
        &format!("/v0/user/{user_id}/friends?limit=99999999"),
        StatusCode::BAD_REQUEST,
    )
    .await?;

    // skip=99999999 exceeds BoundedSkip<10_000>::MAX → 400
    invalid_get_request(
        &format!("/v0/user/{user_id}/followers?skip=99999999"),
        StatusCode::BAD_REQUEST,
    )
    .await?;

    // limit=0 is rejected → 400
    invalid_get_request(
        &format!("/v0/user/{user_id}/followers?limit=0"),
        StatusCode::BAD_REQUEST,
    )
    .await?;

    // limit=200 (at MAX) is accepted → 200
    get_request(&format!("/v0/user/{user_id}/followers?limit=200")).await?;

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_follows_skip_past_end() -> Result<()> {
    // The fixture user has 10 followers and follows 15 users. Skipping past the
    // end of either list must return 200 with an empty array, not 404.
    let user_id = "4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro";

    let res = get_request(&format!("/v0/user/{user_id}/followers?skip=100")).await?;
    assert!(res.is_array());
    assert!(
        res.as_array().unwrap().is_empty(),
        "Expected empty followers page past the end"
    );

    let res = get_request(&format!("/v0/user/{user_id}/following?skip=100")).await?;
    assert!(res.is_array());
    assert!(
        res.as_array().unwrap().is_empty(),
        "Expected empty following page past the end"
    );

    // A nonexistent user still 404s, regardless of pagination
    invalid_get_request(
        &format!("/v0/user/{NON_EXISTING_USER_ID}/followers?skip=100"),
        StatusCode::NOT_FOUND,
    )
    .await?;

    invalid_get_request(
        &format!("/v0/user/{NON_EXISTING_USER_ID}/following?skip=100"),
        StatusCode::NOT_FOUND,
    )
    .await?;

    Ok(())
}

/// The ids a follow list endpoint returns, in its order.
async fn follow_ids(endpoint: &str) -> Result<Vec<String>> {
    let res = get_request(endpoint).await?;
    Ok(res
        .as_array()
        .expect("a follow list is a JSON array")
        .iter()
        .map(|id| id.as_str().expect("a follow list holds ids").to_string())
        .collect())
}

/// Owned copies of `ids`, as the follow models hold them.
fn ids(ids: &[&str]) -> Vec<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

/// The `(following, followed_by)` flags of `user_id` as `viewer_id` sees them.
async fn relationship(user_id: &str, viewer_id: &str) -> Result<(bool, bool)> {
    let res = get_request(&format!("/v0/user/{user_id}/relationship/{viewer_id}")).await?;
    Ok((
        res["following"].as_bool().expect("following is a boolean"),
        res["followed_by"]
            .as_bool()
            .expect("followed_by is a boolean"),
    ))
}

#[tokio_shared_rt::test(shared)]
async fn test_follow_lists_oldest_first() -> Result<()> {
    // The fixture creates its follows in an order that matches none of these lists,
    // forwards or backwards, and none of them is in id order: only the query's own
    // ordering gives them.
    assert_eq!(
        follow_ids(&format!("/v0/user/{HUB}/followers")).await?,
        HUB_FOLLOWERS,
        "followers: oldest follow first, same-millisecond follows by id"
    );
    assert_eq!(
        follow_ids(&format!("/v0/user/{HUB}/following")).await?,
        HUB_FOLLOWING,
        "following: oldest follow first, same-millisecond follows by id"
    );
    // B only follows HUB and E is only followed by it: neither is a friend
    assert_eq!(
        follow_ids(&format!("/v0/user/{HUB}/friends")).await?,
        HUB_FRIENDS,
        "friends: the mutual follows, in the order of the following list"
    );

    Ok(())
}

/// The read behind the post streams of a follow source: the follows made last,
/// the newest first, cut to `limit`.
#[tokio_shared_rt::test(shared)]
async fn test_most_recent_follows() -> Result<()> {
    // Ensure the server is running, so the graph connection is initialized
    TestServiceServer::get_test_server().await;

    let followers = Followers::get_most_recent(HUB, 10).await?;
    let following = Following::get_most_recent(HUB, 10).await?;
    let friends = Friends::get_most_recent(HUB, 10).await?;
    assert_eq!(followers.map(|f| f.0), Some(ids(&HUB_FOLLOWERS_NEWEST)));
    assert_eq!(following.map(|f| f.0), Some(ids(&HUB_FOLLOWING_NEWEST)));
    assert_eq!(friends.map(|f| f.0), Some(ids(&HUB_FRIENDS_NEWEST)));

    // A limit below the list keeps the newest follows, a tie cut by id
    let followers = Followers::get_most_recent(HUB, 1).await?;
    let following = Following::get_most_recent(HUB, 3).await?;
    let friends = Friends::get_most_recent(HUB, 2).await?;
    assert_eq!(
        followers.map(|f| f.0),
        Some(ids(&HUB_FOLLOWERS_NEWEST[..1]))
    );
    assert_eq!(
        following.map(|f| f.0),
        Some(ids(&HUB_FOLLOWING_NEWEST[..3]))
    );
    assert_eq!(friends.map(|f| f.0), Some(ids(&HUB_FRIENDS_NEWEST[..2])));

    // An existing user with no follows against a user with no node
    let loner = Following::get_most_recent(LONER, 10).await?;
    assert_eq!(loner.map(|f| f.0), Some(Vec::new()));
    let missing = NON_EXISTING_USER_ID;
    assert!(Followers::get_most_recent(missing, 10).await?.is_none());
    assert!(Friends::get_most_recent(missing, 10).await?.is_none());

    Ok(())
}

/// A post stream by a follow source reads the posts of the follows made last:
/// its cut keeps the newest of the list, and leaves the observer out.
#[tokio_shared_rt::test(shared)]
async fn test_post_stream_source_users_are_the_newest_follows() -> Result<()> {
    // Ensure the server is running, so the graph connection is initialized
    TestServiceServer::get_test_server().await;

    let observer_id = HUB.to_string();
    let following = StreamSource::Following {
        observer_id: observer_id.clone(),
    };
    let followers = StreamSource::Followers {
        observer_id: observer_id.clone(),
    };
    let friends = StreamSource::Friends { observer_id };

    // A cut below each list: the oldest follows are the ones left out
    assert_eq!(
        PostStream::source_user_ids(&following, 2).await?,
        ids(&HUB_FOLLOWING_NEWEST[..2])
    );
    assert_eq!(
        PostStream::source_user_ids(&followers, 2).await?,
        ids(&HUB_FOLLOWERS_NEWEST[..2])
    );
    assert_eq!(
        PostStream::source_user_ids(&friends, 2).await?,
        ids(&HUB_FRIENDS_NEWEST[..2])
    );

    // AMSTERDAM follows itself in the mock data, its newest follow, then BOGOTA
    // and CAIRO: the observer is left out of its own source
    let self_following = StreamSource::Following {
        observer_id: AMSTERDAM.to_string(),
    };
    assert_eq!(
        PostStream::source_user_ids(&self_following, 200).await?,
        ids(&[BOGOTA, CAIRO_USER])
    );

    // A source that is not a follow list has no users to read
    let author = StreamSource::Author {
        author_id: HUB.to_string(),
    };
    assert!(PostStream::source_user_ids(&author, 2).await?.is_empty());

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_follow_lists_pages() -> Result<()> {
    let lists: [(&str, &[&str]); 3] = [
        ("followers", &HUB_FOLLOWERS),
        ("following", &HUB_FOLLOWING),
        ("friends", &HUB_FRIENDS),
    ];

    for (list, expected) in lists {
        // Pages of one and of two: followers and following each hold a tie, so a
        // page edge falls inside it
        for limit in [1, 2] {
            let mut paged = Vec::new();
            for skip in (0..expected.len()).step_by(limit) {
                let page =
                    follow_ids(&format!("/v0/user/{HUB}/{list}?skip={skip}&limit={limit}")).await?;
                assert_eq!(
                    page.len(),
                    limit.min(expected.len() - skip),
                    "{list}: size of the page at skip={skip}, limit={limit}"
                );
                paged.extend(page);
            }
            assert_eq!(
                paged, expected,
                "{list}: pages of {limit} should join into the whole list"
            );
        }

        // Past the end an existing user gets an empty page, not a 404
        let past_end = follow_ids(&format!("/v0/user/{HUB}/{list}?skip=100")).await?;
        assert!(past_end.is_empty(), "{list}: expected an empty page");
    }

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_follow_lists_of_user_without_follows() -> Result<()> {
    // An existing user with no follows gets empty lists; only a missing user is a 404
    for list in ["followers", "following", "friends"] {
        let ids = follow_ids(&format!("/v0/user/{LONER}/{list}")).await?;
        assert!(ids.is_empty(), "{list}: LONER has no follows");

        invalid_get_request(
            &format!("/v0/user/{NON_EXISTING_USER_ID}/{list}"),
            StatusCode::NOT_FOUND,
        )
        .await?;
    }

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_relationship_one_way_and_mutual() -> Result<()> {
    // (viewed user, viewer follows them, they follow the viewer), the viewer being HUB
    let cases = [
        (A, true, true),
        (B, false, true),
        (E, true, false),
        (LONER, false, false),
    ];

    for (user_id, following, followed_by) in cases {
        assert_eq!(
            relationship(user_id, HUB).await?,
            (following, followed_by),
            "relationship of {user_id} as HUB sees it"
        );
        // The same follows read from the other side
        assert_eq!(
            relationship(HUB, user_id).await?,
            (followed_by, following),
            "relationship of HUB as {user_id} sees it"
        );
    }

    // A page of users reads all its relationships in one query: it must agree with
    // the single route, whatever order the ids come in, and skip the missing user
    let page = post_request(
        "/v0/stream/users/by_ids",
        serde_json::json!({
            "user_ids": [LONER, E, NON_EXISTING_USER_ID, B, A],
            "viewer_id": HUB
        }),
    )
    .await?;
    let page = page.as_array().expect("a user stream is a JSON array");
    assert_eq!(page.len(), cases.len(), "one view per existing user");
    for (user_id, following, followed_by) in cases {
        let view = page
            .iter()
            .find(|view| view["details"]["id"] == user_id)
            .unwrap_or_else(|| panic!("{user_id} should be in the page"));
        assert_eq!(
            (
                view["relationship"]["following"].as_bool(),
                view["relationship"]["followed_by"].as_bool()
            ),
            (Some(following), Some(followed_by)),
            "stream relationship of {user_id} as HUB sees it"
        );
    }

    // A missing viewer leaves the flags at their default instead of failing the page
    let page = post_request(
        "/v0/stream/users/by_ids",
        serde_json::json!({ "user_ids": [A], "viewer_id": NON_EXISTING_USER_ID }),
    )
    .await?;
    assert_eq!(page[0]["relationship"]["following"], false);
    assert_eq!(page[0]["relationship"]["followed_by"], false);

    Ok(())
}

/// The watcher's follow sets can lag the graph or be missing. The API reads only
/// the graph, and a read must not write the sets back.
#[tokio_shared_rt::test(shared)]
async fn test_follow_reads_ignore_the_follow_sets() -> Result<()> {
    // Ensure the server is running, so the Redis pool is initialized
    TestServiceServer::get_test_server().await;

    // Make the sets disagree with the graph: LONER added as a follower of HUB that
    // HUB follows back, D dropped from HUB's followers, HUB's following set gone
    Followers(ids(&[LONER])).put_to_index(HUB).await?;
    Following(ids(&[HUB])).put_to_index(LONER).await?;
    Followers(ids(&[D])).del_from_index(HUB).await?;
    Following(ids(&HUB_FOLLOWING)).del_from_index(HUB).await?;

    let followers = follow_ids(&format!("/v0/user/{HUB}/followers")).await?;
    let following = follow_ids(&format!("/v0/user/{HUB}/following")).await?;
    let friends = follow_ids(&format!("/v0/user/{HUB}/friends")).await?;
    let loner_following = follow_ids(&format!("/v0/user/{LONER}/following")).await?;
    let with_loner = relationship(HUB, LONER).await?;
    let with_d = relationship(HUB, D).await?;

    // What the reads left in Redis, taken before the sets are put back
    let (following_set_exists, _) = Following::check_set_member(&[HUB], C).await?;
    let d_in_followers_set = Followers::check_in_index(HUB, D).await?;

    // Put the sets back as the mock data had them before asserting anything
    Followers(ids(&[LONER])).del_from_index(HUB).await?;
    Following(ids(&[HUB])).del_from_index(LONER).await?;
    Followers(ids(&[D])).put_to_index(HUB).await?;
    Following(ids(&HUB_FOLLOWING)).put_to_index(HUB).await?;

    assert_eq!(followers, HUB_FOLLOWERS, "followers come from the graph");
    assert_eq!(following, HUB_FOLLOWING, "following comes from the graph");
    assert_eq!(friends, HUB_FRIENDS, "friends come from the graph");
    assert!(loner_following.is_empty(), "LONER follows nobody");
    assert_eq!(with_loner, (false, false), "LONER does not follow HUB");
    assert_eq!(with_d, (true, true), "D and HUB follow each other");

    assert!(
        !following_set_exists,
        "reading the following list must not write the set back"
    );
    assert!(
        !d_in_followers_set,
        "reading the followers list must not write the set back"
    );

    Ok(())
}
