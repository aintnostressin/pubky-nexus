use super::translation::{self, EventRoute, SkipReason, UNKNOWN_LABEL};
use crate::errors::EventProcessorError;
use crate::METER_NAME;
use nexus_common::models::event::EventLine;
use opentelemetry::metrics::Counter;
use opentelemetry::{global, KeyValue};
use pubky::Event as StreamEvent;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::LazyLock;
use tracing::{debug, warn};

/// Counter for events skipped before any fetch because their `social` epoch is not indexed.
/// Labelled by `epoch` and `resource` kind only, never by user or object id.
static EPOCH_SKIPPED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter(METER_NAME)
        .u64_counter("watcher.epoch.skipped")
        .with_description(
            "Events skipped before fetching because their social epoch is not indexed",
        )
        .build()
});

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EventType {
    Put,
    Del,
}

impl From<pubky::EventType> for EventType {
    fn from(value: pubky::EventType) -> Self {
        match value {
            pubky::EventType::Put { .. } => Self::Put,
            pubky::EventType::Delete => Self::Del,
        }
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let upper_case_str = match self {
            EventType::Put => "PUT",
            EventType::Del => "DEL",
        };
        write!(f, "{upper_case_str}")
    }
}

/// Result of parsing an event line from a homeserver.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ParseResult {
    /// Successfully parsed into a known, actionable event.
    Parsed(Event),
    /// Known resource that Nexus does not handle (a v0 last-read, feed or blob), or a path in a
    /// `social` epoch that is not indexed. Nothing is fetched for it.
    Skipped { reason: SkipReason },
    /// URI matched no route: it has no namespace to route on, or its namespace's parser rejects
    /// it (an app-specific path that is not a universal tag, a social path without an epoch).
    /// Callers log `reason` and drop the event.
    UnrecognizedUri {
        event_type: EventType,
        uri: String,
        reason: String,
    },
}

impl ParseResult {
    fn unrecognized_uri(event_type: EventType, uri: String, reason: String) -> Self {
        Self::UnrecognizedUri {
            event_type,
            uri,
            reason,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Event {
    /// Pubky resource URI from the homeserver event line.
    pub uri: String,

    /// Operation represented by the event, used to dispatch to PUT or DEL handlers.
    pub event_type: EventType,

    /// [`Self::uri`] routed by epoch. Parsing builds events for legacy routes only.
    pub route: EventRoute,

    /// Original event line as received from the homeserver.
    event_line: String,
}

impl Event {
    /// Parse event from a line returned by the homeserver's `/events` endpoint.
    pub fn parse_event(line: &str) -> Result<ParseResult, EventProcessorError> {
        let parts: Vec<&str> = line.split(' ').collect();
        if parts.len() != 2 {
            return Err(EventProcessorError::InvalidEventLine(format!(
                "Malformed event line, {line}"
            )));
        }

        let event_type = match parts[0] {
            "PUT" => Ok(EventType::Put),
            "DEL" => Ok(EventType::Del),
            other => Err(EventProcessorError::InvalidEventLine(format!(
                "Unknown event type: {other}"
            ))),
        }?;

        let uri = parts[1].to_string();
        let event_line = line.to_string();

        Self::parse_event_parts(event_type, uri, event_line)
    }

    /// Constructs a nexus [`Event`] directly from a [`StreamEvent`], avoiding
    /// the string round-trip through [`Self::parse_event`].
    pub fn from_stream_event(
        stream_event: &StreamEvent,
    ) -> Result<Option<Self>, EventProcessorError> {
        let event_type: EventType = stream_event.event_type.clone().into();

        let uri = stream_event.resource.to_pubky_url();
        debug!(%event_type, %uri, "New stream event");

        let event_line = format!("{event_type} {uri}");
        match Self::parse_event_parts(event_type, uri, event_line)? {
            ParseResult::Parsed(event) => Ok(Some(event)),
            ParseResult::Skipped { .. } => Ok(None),
            ParseResult::UnrecognizedUri { reason, .. } => {
                warn!(%reason, "Unrecognized event URI");
                Ok(None)
            }
        }
    }

    fn parse_event_parts(
        event_type: EventType,
        uri: String,
        event_line: String,
    ) -> Result<ParseResult, EventProcessorError> {
        let route = match translation::route(&uri) {
            EventRoute::Malformed { reason } => {
                return Ok(ParseResult::unrecognized_uri(event_type, uri, reason))
            }
            route => route,
        };

        if let Some(reason) = translation::skip_reason(&route, &uri)? {
            record_skip(&uri, &route);
            return Ok(ParseResult::Skipped { reason });
        }

        Ok(ParseResult::Parsed(Event {
            uri,
            event_type,
            route,
            event_line,
        }))
    }

    pub fn to_event_line(&self) -> EventLine {
        EventLine::new(self.event_line.clone())
    }
}

/// Logs and counts the skip of a `social` epoch event; the skipped v0 resources stay silent.
fn record_skip(uri: &str, route: &EventRoute) {
    let epoch = match route {
        EventRoute::Social { epoch, .. } => {
            debug!(%uri, "Skipping social v{epoch} event until that epoch is indexed");
            format!("v{epoch}")
        }
        EventRoute::UnsupportedEpoch { version } => {
            warn!(
                %uri,
                %version,
                "Skipping event of an unsupported social epoch; upgrade Nexus to index it"
            );
            epoch_label(version)
        }
        EventRoute::Legacy(_) | EventRoute::Malformed { .. } => return,
    };
    EPOCH_SKIPPED.add(
        1,
        &[
            KeyValue::new("epoch", epoch),
            KeyValue::new("resource", route.resource_name()),
        ],
    );
}

/// The `epoch` label of an unsupported version. The segment comes from an untrusted path, so only
/// canonical spellings of the spec's `social` epochs, `v1` to `v255`, get a label of their own:
/// epoch 0 spells itself `pubky.app`, never `social/v0`.
fn epoch_label(version: &str) -> String {
    match version.strip_prefix('v').map(str::parse::<u8>) {
        Some(Ok(epoch)) if epoch > 0 && version == format!("v{epoch}") => version.to_string(),
        _ => UNKNOWN_LABEL.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::translation::test_support::{uri, HASH, TS};

    /// The reason of a skipped line, or a panic naming what came back.
    fn skipped(line: &str) -> SkipReason {
        match Event::parse_event(line) {
            Ok(ParseResult::Skipped { reason }) => reason,
            other => panic!("{line} should be skipped, got {other:?}"),
        }
    }

    /// Lines are built the way the retry processor rebuilds a stored entry, so this also covers a
    /// v1 line re-parsed from the retry queue.
    #[test]
    fn v1_lines_are_skipped_as_not_indexed() {
        for path in [
            format!("pub/social/v1/posts/{TS}/{TS}.json"),
            "pub/social/v1/profile.json".to_string(),
            format!("pub/social/v1/tags/{HASH}.json"),
            "pub/social/v1/widgets/ABC".to_string(),
        ] {
            for event_type in [EventType::Put, EventType::Del] {
                let line = format!("{event_type} {}", uri(&path));
                assert_eq!(
                    skipped(&line),
                    SkipReason::EpochNotIndexed { epoch: 1 },
                    "{line}"
                );
            }
        }
    }

    #[test]
    fn v2_line_is_skipped_as_unsupported() {
        let line = format!("PUT {}", uri(&format!("pub/social/v2/posts/{TS}")));
        assert_eq!(
            skipped(&line),
            SkipReason::UnsupportedEpoch {
                version: "v2".into()
            }
        );
    }

    #[test]
    fn unindexed_v0_lines_are_still_skipped() {
        let cases = [
            ("pub/pubky.app/last_read".to_string(), SkipReason::LastRead),
            (format!("pub/pubky.app/feeds/{HASH}"), SkipReason::Feed),
            (format!("pub/pubky.app/blobs/{HASH}"), SkipReason::Blob),
        ];
        for (path, expected) in cases {
            assert_eq!(skipped(&format!("PUT {}", uri(&path))), expected, "{path}");
        }
    }

    #[test]
    fn v0_post_line_parses_into_a_legacy_event() {
        let line = format!("PUT {}", uri(&format!("pub/pubky.app/posts/{TS}")));
        match Event::parse_event(&line) {
            Ok(ParseResult::Parsed(event)) => {
                assert_eq!(event.event_type, EventType::Put);
                assert!(matches!(event.route, EventRoute::Legacy(_)));
                assert_eq!(event.route.resource_id().as_deref(), Some(TS));
            }
            other => panic!("expected a parsed event, got {other:?}"),
        }
    }

    #[test]
    fn epoch_less_social_line_is_unrecognized() {
        let line = format!("PUT {}", uri("pub/social/tags/ABC"));
        assert!(matches!(
            Event::parse_event(&line),
            Ok(ParseResult::UnrecognizedUri { .. })
        ));
    }

    #[test]
    fn unknown_v0_resource_is_still_an_error() {
        let line = format!("PUT {}", uri("pub/pubky.app/widgets/ABC"));
        assert!(matches!(
            Event::parse_event(&line),
            Err(EventProcessorError::InvalidEventLine(_))
        ));
    }

    #[test]
    fn epoch_label_bounds_untrusted_versions() {
        assert_eq!(epoch_label("v1"), "v1");
        assert_eq!(epoch_label("v2"), "v2");
        assert_eq!(epoch_label("v255"), "v255");
        assert_eq!(epoch_label("v0"), "unknown");
        assert_eq!(epoch_label("v256"), "unknown");
        assert_eq!(epoch_label("v02"), "unknown");
        assert_eq!(epoch_label("v123456789012"), "unknown");
    }
}
