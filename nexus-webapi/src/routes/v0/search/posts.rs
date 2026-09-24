use crate::models::{
    BoundedLimit, BoundedPagination, BoundedSkip, PostSearchQuery, PubkyAppPostKind, PubkyId,
    TagLabel,
};
use crate::routes::v0::endpoints::{SEARCH_POSTS_BY_CONTENT_ROUTE, SEARCH_POSTS_BY_TAG_ROUTE};
use crate::routes::{Path, Query};
use crate::{Error, Result};
use axum::Json;
use nexus_common::db::kv::AuthorFilter;
use nexus_common::models::follow::reach::{reach_contains, reach_user_ids};
use nexus_common::models::post::search::{
    PostsByContentSearch, PostsByTagSearch, MAX_REACH_AUTHORS_FT,
};
use nexus_common::types::{StreamReach, StreamSorting};
use opentelemetry::metrics::{Histogram, Meter};
use opentelemetry::{global, KeyValue};
use serde::Deserialize;
use std::sync::LazyLock;
use tracing::debug;
use utoipa::OpenApi;

#[derive(Deserialize)]
pub struct SearchPostsQuery {
    pub sorting: Option<StreamSorting>,
    pub user_id: Option<PubkyId>,
    pub reach: Option<StreamReach>,
    #[serde(flatten)]
    pub pagination: BoundedPagination<10_000, 20, 200>,
    pub start: Option<f64>,
    pub end: Option<f64>,
}

#[utoipa::path(
    get,
    path = SEARCH_POSTS_BY_TAG_ROUTE,
    description = "Search Posts by Tag. With `user_id` and `reach`, only parent posts authored by users in that reach are returned (the observer's own posts excluded), and the `total_engagement` score counts taggers, replies and reposts but not mentions. `start`/`end` cursors are not interchangeable between the reach and non-reach modes",
    tag = "Search",
    params(
        ("tag" = TagLabel, Path, description = "Tag name"),
        ("sorting" = Option<StreamSorting>, Query, description = "StreamSorting method"),
        ("user_id" = Option<PubkyId>, Query, description = "User ID to base reach on. Must be provided together with reach"),
        ("reach" = Option<StreamReach>, Query, example = "wot_2", description = "Reach type: `followers` | `following` | `friends` | `wot` | `wot_1`..`wot_3`. To apply that, user_id is required. Bare `wot` defaults to depth 2."),
        ("start" = Option<f64>, Query, description = "The start of the stream timeframe (score cursor for `total_engagement`). Posts with a score greater than this value will be excluded from the results"),
        ("end" = Option<f64>, Query, description = "The end of the stream timeframe (score cursor for `total_engagement`). Posts with a score less than this value will be excluded from the results"),
        ("skip" = Option<BoundedSkip<10_000>>, Query, description = "Skip N results (max 10000)"),
        ("limit" = Option<BoundedLimit<20, 200>>, Query, description = "Limit the number of results (1–200, default 20)")
    ),
    responses(
        (status = 200, description = "Search results", body = Vec<PostsByTagSearch>),
        (status = 400, description = "Invalid parameters"),
        (status = 429, description = "Rate limit exceeded", headers(("Retry-After" = u64, description = "Seconds until retry"))),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn search_posts_by_tag_handler(
    Path(tag): Path<TagLabel>,
    Query(query): Query<SearchPostsQuery>,
) -> Result<Json<Vec<PostsByTagSearch>>> {
    let sorting = query.sorting;

    debug!(
        "GET {SEARCH_POSTS_BY_TAG_ROUTE} tag:{}, sort_by: {:?}, user_id: {:?}, reach: {:?}, start: {:?}, end: {:?}, skip: {}, limit: {}",
        tag, sorting, query.user_id, query.reach, query.start, query.end,
        query.pagination.skip_value(), query.pagination.limit_value()
    );

    if query.user_id.is_some() ^ query.reach.is_some() {
        return Err(Error::invalid_input(
            "user_id and reach should be both provided together",
        ));
    }

    let pagination = query.pagination.to_pagination(query.start, query.end);

    if let (Some(user_id), Some(reach)) = (query.user_id, query.reach) {
        let posts =
            PostsByTagSearch::get_by_label_with_reach(&tag, sorting, &user_id, reach, pagination)
                .await?;
        return Ok(Json(posts));
    }

    match PostsByTagSearch::get_by_label(&tag, sorting, pagination).await? {
        Some(posts_list) => Ok(Json(posts_list)),
        None => Ok(Json(vec![])),
    }
}

const METER_NAME: &str = "search.posts.by_content";

/// How large the reaches a full-text content search resolves are, and how often
/// `MAX_REACH_AUTHORS_FT` cut one short. The instrument is a no-op when no
/// `SdkMeterProvider` is registered (i.e. when OTLP is not configured), so
/// there is zero overhead in that case.
struct ContentSearchMetrics {
    reach_users: Histogram<u64>,
}

impl ContentSearchMetrics {
    fn new(meter: &Meter) -> Self {
        Self {
            reach_users: meter
                .u64_histogram("search.posts.by_content.reach.users")
                .with_description(
                    "Users a content search's reach resolved to, by reach/depth/met_limit",
                )
                .with_unit("{user}")
                .build(),
        }
    }

    /// `reach`/`depth` are the attributes the reach graph queries already
    /// carry, so a reach can be followed across both. `met_limit` splits off the
    /// searches that only saw part of the reach.
    fn record_reach_resolution(&self, reach: &StreamReach, users: usize, met_limit: bool) {
        let (name, depth) = reach.telemetry_dimensions();
        let mut attrs = vec![
            KeyValue::new("reach", name),
            KeyValue::new("met_limit", met_limit),
        ];
        if let Some(depth) = depth {
            attrs.push(KeyValue::new("depth", i64::from(depth)));
        }
        self.reach_users.record(users as u64, &attrs);
    }
}

static METRICS: LazyLock<ContentSearchMetrics> =
    LazyLock::new(|| ContentSearchMetrics::new(&global::meter(METER_NAME)));

#[derive(Deserialize)]
pub struct SearchPostsByContentQuery {
    pub q: PostSearchQuery,
    pub author: Option<PubkyId>,
    pub kind: Option<PubkyAppPostKind>,
    pub user_id: Option<PubkyId>,
    pub reach: Option<StreamReach>,
    #[serde(flatten)]
    pub pagination: BoundedPagination<1000, 20, 100>,
}

#[utoipa::path(
    get,
    path = SEARCH_POSTS_BY_CONTENT_ROUTE,
    description = "Full-text search over post content",
    tag = "Search",
    params(
        ("q" = PostSearchQuery, Query, description = "Search query (2–30 characters, up to 4 terms)"),
        ("author" = Option<PubkyId>, Query, description = "Optional author Pubky ID to scope results"),
        ("kind" = Option<PubkyAppPostKind>, Query, description = "Optional post kind to filter by: short, long, image, video, link, file, collection"),
        ("user_id" = Option<PubkyId>, Query, description = "User ID to base reach on. Must be provided together with reach"),
        ("reach" = Option<StreamReach>, Query, example = "following", description = "Reach type: `followers` | `following` | `friends` | `wot` | `wot_1`..`wot_3`. Scopes results to posts authored by users in that reach, never by user_id itself. To apply that, user_id is required. Bare `wot` defaults to depth 2. Combined with `author`, results are that author's posts if the author is in reach, and empty otherwise"),
        ("skip" = Option<BoundedSkip<1000>>, Query, description = "Skip N results (max 1000)"),
        ("limit" = Option<BoundedLimit<20, 100>>, Query, description = "Limit the number of results (1–100, default 20)")
    ),
    responses(
        (status = 200, description = "Search results ordered by relevance score", body = Vec<PostsByContentSearch>),
        (status = 400, description = "Invalid query or limit parameter"),
        (status = 429, description = "Rate limit exceeded", headers(("Retry-After" = u64, description = "Seconds until retry"))),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn search_posts_by_content_handler(
    Query(query): Query<SearchPostsByContentQuery>,
) -> Result<Json<Vec<PostsByContentSearch>>> {
    let skip = query.pagination.skip_value();
    let limit = query.pagination.limit_value();

    debug!(
        "GET {SEARCH_POSTS_BY_CONTENT_ROUTE} q:{}, author:{:?}, kind:{:?}, user_id:{:?}, reach:{:?}, skip:{skip}, limit:{limit}",
        query.q, query.author, query.kind, query.user_id, query.reach
    );

    if query.user_id.is_some() ^ query.reach.is_some() {
        return Err(Error::invalid_input(
            "user_id and reach should be both provided together",
        ));
    }

    let kind_str = query.kind.as_ref().map(|k| k.to_string());

    let reach_ids;
    let author = match (query.author.as_ref(), query.user_id, query.reach) {
        (author, Some(user_id), Some(reach)) => match author {
            // author and reach intersect: the author's posts, if in reach
            Some(author) => {
                if !reach_contains(&user_id, &reach, author).await? {
                    return Ok(Json(vec![]));
                }
                Some(AuthorFilter::One(author))
            }
            None => {
                // Over the cap, the most prolific authors are kept; how often
                // that happens is in the `met_limit` attribute of the metric
                let reach_users = reach_user_ids(&user_id, &reach, MAX_REACH_AUTHORS_FT).await?;
                reach_ids = reach_users.user_ids;
                METRICS.record_reach_resolution(&reach, reach_ids.len(), reach_users.met_limit);
                Some(AuthorFilter::AnyOf(&reach_ids))
            }
        },
        (author, _, _) => author.map(AuthorFilter::One),
    };

    let results =
        PostsByContentSearch::search(query.q.as_str(), author, kind_str.as_deref(), skip, limit)
            .await?;
    Ok(Json(results))
}

#[derive(OpenApi)]
#[openapi(
    paths(search_posts_by_tag_handler, search_posts_by_content_handler),
    components(schemas(
        PostsByTagSearch,
        PostsByContentSearch,
        PostSearchQuery,
        PubkyAppPostKind,
        StreamReach
    ))
)]
pub struct SearchPostsApiDocs;

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_common::types::WotDepth;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, Metric, MetricData};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

    fn parse_query(
        s: &str,
    ) -> std::result::Result<SearchPostsByContentQuery, serde_urlencoded::de::Error> {
        serde_urlencoded::from_str(s)
    }

    #[test]
    fn author_missing_parses_unscoped() {
        let q = parse_query("q=bitcoin").expect("valid query must parse");
        assert!(q.author.is_none());
        assert!(q.kind.is_none());
    }

    #[test]
    fn author_invalid_format_rejected() {
        assert!(parse_query("q=bitcoin&author=not-a-pubky").is_err());
    }

    #[test]
    fn reach_parses_with_user_id() {
        let q = parse_query(
            "q=bitcoin&user_id=wnhrmj3b1tt3n6fr7fhedgak4q11e9i1uxm4dmiactgeobyu9wpy&reach=wot_3",
        )
        .expect("valid reach must parse");
        assert!(q.user_id.is_some());
        assert!(matches!(q.reach, Some(StreamReach::Wot(depth)) if depth.get() == 3));
    }

    #[test]
    fn reach_invalid_rejected() {
        assert!(parse_query("q=bitcoin&reach=wot_4").is_err());
        assert!(parse_query("q=bitcoin&reach=everyone").is_err());
    }

    #[test]
    fn kind_missing_parses_unscoped() {
        let q = parse_query("q=bitcoin").expect("valid query must parse");
        assert!(q.kind.is_none());
    }

    #[test]
    fn kind_valid_short_accepted() {
        let q = parse_query("q=bitcoin&kind=short").expect("valid kind must parse");
        assert_eq!(q.kind, Some(PubkyAppPostKind::Short));
    }

    #[test]
    fn kind_unknown_parses_as_unknown() {
        let q =
            parse_query("q=bitcoin&kind=not-a-kind").expect("lenient kind parsing must not error");
        assert_eq!(q.kind, Some(PubkyAppPostKind::Unknown));
    }

    /// `attr=value` pairs of a data point, sorted, so the assertions don't
    /// depend on the order the SDK keeps them in.
    fn attrs(point: impl Iterator<Item = KeyValue>) -> Vec<String> {
        let mut attrs: Vec<String> = point.map(|kv| format!("{}={}", kv.key, kv.value)).collect();
        attrs.sort();
        attrs
    }

    /// Every point of `name` as `(attributes, sum)`, sorted by sum.
    fn points(exported: &[&Metric], name: &str) -> Vec<(Vec<String>, u64)> {
        let data = exported
            .iter()
            .find(|m| m.name() == name)
            .unwrap_or_else(|| panic!("{name} must be exported"))
            .data();
        let mut points: Vec<_> = match data {
            AggregatedMetrics::U64(MetricData::Histogram(h)) => h
                .data_points()
                .map(|p| (attrs(p.attributes().cloned()), p.sum()))
                .collect(),
            other => panic!("unexpected aggregation for {name}: {other:?}"),
        };
        points.sort_by_key(|(_, sum)| *sum);
        points
    }

    /// Asserts on the exported points, not on the calls: an instrument renamed
    /// or an attribute dropped is what breaks the dashboards.
    #[test]
    fn records_reach_size_and_whether_the_limit_was_met() {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let metrics = ContentSearchMetrics::new(&provider.meter(METER_NAME));
        let wot_3 = StreamReach::Wot(WotDepth::new(3).expect("valid depth"));

        metrics.record_reach_resolution(&StreamReach::Following, 7, false);
        metrics.record_reach_resolution(&wot_3, MAX_REACH_AUTHORS_FT, true);
        provider.force_flush().expect("flush must succeed");

        let collected = exporter.get_finished_metrics().expect("metrics collected");
        let exported: Vec<&Metric> = collected
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
            .collect();

        // met_limit splits the searches that missed part of the reach off the
        // same instrument, so no second one is needed to tell them apart
        assert_eq!(
            points(&exported, "search.posts.by_content.reach.users"),
            vec![
                (
                    vec!["met_limit=false".to_string(), "reach=following".to_string()],
                    7
                ),
                (
                    vec![
                        "depth=3".to_string(),
                        "met_limit=true".to_string(),
                        "reach=wot".to_string()
                    ],
                    MAX_REACH_AUTHORS_FT as u64
                ),
            ]
        );
    }
}
