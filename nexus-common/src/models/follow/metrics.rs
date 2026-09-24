//! OpenTelemetry instruments for reach resolution.
//!
//! Mirrors the `GraphMetrics` pattern in `db::graph::instrumented`: instruments
//! come from the global meter and are no-ops when no `SdkMeterProvider` is
//! registered (i.e. when OTLP is not configured), so there is zero overhead in
//! that case. The `reach`/`depth` attributes are the ones the graph queries
//! already carry, so a reach can be followed across both.
//!
//! | Instrument                  | Reads as                                          |
//! |-----------------------------|---------------------------------------------------|
//! | `search.reach.users`        | how large the resolved reaches are, by reach/depth |
//! | `search.reach.truncated`    | truncated / `count(search.reach.users)` is the share of searches that missed part of the reach |

use std::sync::LazyLock;

use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::{global, KeyValue};

use crate::types::StreamReach;

const METER_NAME: &str = "search";

struct ReachMetrics {
    /// Users a reach resolved to, after trimming.
    users: Histogram<u64>,
    /// Resolutions that hit the caller's limit, so part of the reach was left out.
    truncated: Counter<u64>,
}

impl ReachMetrics {
    fn new(meter: &Meter) -> Self {
        Self {
            users: meter
                .u64_histogram("search.reach.users")
                .with_description("Users a reach resolved to, after trimming, by reach/depth")
                .with_unit("{user}")
                .build(),
            truncated: meter
                .u64_counter("search.reach.truncated")
                .with_description("Reach resolutions trimmed to the caller's limit, by reach/depth")
                .build(),
        }
    }

    fn record_resolution(&self, reach: &StreamReach, users: usize, truncated: bool) {
        let attrs = reach_attrs(reach);
        self.users.record(users as u64, &attrs);
        if truncated {
            self.truncated.add(1, &attrs);
        }
    }
}

/// `reach=<name>`, plus `depth` for the WoT reaches only.
fn reach_attrs(reach: &StreamReach) -> Vec<KeyValue> {
    let (name, depth) = reach.telemetry_dimensions();
    let mut attrs = vec![KeyValue::new("reach", name)];
    if let Some(depth) = depth {
        attrs.push(KeyValue::new("depth", i64::from(depth)));
    }
    attrs
}

static METRICS: LazyLock<ReachMetrics> =
    LazyLock::new(|| ReachMetrics::new(&global::meter(METER_NAME)));

/// Record one resolved reach: how many users it came to, and whether the
/// caller's limit cut it short.
pub(super) fn record_reach_resolution(reach: &StreamReach, users: usize, truncated: bool) {
    METRICS.record_resolution(reach, users, truncated);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::WotDepth;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, Metric, MetricData};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

    fn wot(depth: u8) -> StreamReach {
        StreamReach::Wot(WotDepth::new(depth).expect("valid depth"))
    }

    /// `attr=value` pairs of a data point, sorted, so the assertions don't
    /// depend on the order the SDK keeps them in.
    fn attrs(point: impl Iterator<Item = KeyValue>) -> Vec<String> {
        let mut attrs: Vec<String> = point.map(|kv| format!("{}={}", kv.key, kv.value)).collect();
        attrs.sort();
        attrs
    }

    /// Every point of `name` as `(attributes, value)`, sorted by value.
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
            AggregatedMetrics::U64(MetricData::Sum(s)) => s
                .data_points()
                .map(|p| (attrs(p.attributes().cloned()), p.value()))
                .collect(),
            other => panic!("unexpected aggregation for {name}: {other:?}"),
        };
        points.sort_by_key(|(_, value)| *value);
        points
    }

    /// Asserts on the exported points, not on the calls: an instrument renamed
    /// or an attribute dropped is what breaks the dashboards.
    #[test]
    fn records_reach_size_and_truncation() {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let metrics = ReachMetrics::new(&provider.meter(METER_NAME));

        metrics.record_resolution(&StreamReach::Following, 7, false);
        metrics.record_resolution(&wot(3), 1_000, true);
        provider.force_flush().expect("flush must succeed");

        let collected = exporter.get_finished_metrics().expect("metrics collected");
        let exported: Vec<&Metric> = collected
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
            .collect();

        let following = vec!["reach=following".to_string()];
        let wot_3 = vec!["depth=3".to_string(), "reach=wot".to_string()];
        assert_eq!(
            points(&exported, "search.reach.users"),
            vec![(following, 7), (wot_3.clone(), 1_000)]
        );
        // Only the trimmed resolution counts, so truncated / count(users) is the rate
        assert_eq!(
            points(&exported, "search.reach.truncated"),
            vec![(wot_3, 1)]
        );
    }
}
