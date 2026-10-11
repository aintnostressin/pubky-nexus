//! Fixtures shared by the routing, translation and parsing unit tests: the user writing every
//! path, another user it points at, a timestamp id and a hash id.

use pubky_social_specs::PubkyId;

pub(crate) const HOST: &str = "operrr8wsbpr3ue9d4qj41ge1kcc6r7fdiy6o3ugjrrhi4y77rdo";
pub(crate) const OTHER: &str = "8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo";
pub(crate) const TS: &str = "0032SSN7Q4EVG";
pub(crate) const HASH: &str = "8Z8CWH8NVYQY39ZEBFGKQWWEKG";

pub(crate) fn uri(path: &str) -> String {
    format!("pubky://{HOST}/{path}")
}

pub(crate) fn host() -> PubkyId {
    PubkyId::try_from(HOST).unwrap()
}

pub(crate) fn other() -> PubkyId {
    PubkyId::try_from(OTHER).unwrap()
}
