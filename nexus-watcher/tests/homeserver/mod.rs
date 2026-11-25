mod active_homeservers;
mod content;
mod per_user_read;
mod user_events;

use pubky_social_specs::legacy_v0::PubkyAppUser;

/// A profile with only a name.
fn profile(name: &str) -> PubkyAppUser {
    PubkyAppUser {
        bio: None,
        image: None,
        links: None,
        name: name.to_string(),
        status: None,
    }
}
