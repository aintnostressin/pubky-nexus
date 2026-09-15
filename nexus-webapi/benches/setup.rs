use nexus_common::models::post::set_hide_unranked_authors;
use nexus_common::{Level, StackConfig, StackManager};
use std::sync::Once;
use tokio::runtime::Runtime;

static INIT: Once = Once::new();

pub fn run_setup() {
    INIT.call_once(|| {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let config = StackConfig {
                log_level: Level::Error,
                ..Default::default()
            };
            StackManager::setup(&config)
                .await
                .expect("stack setup failed; benches need the docker stack up");
            // The fixture ranks three users, so `source=all` benches would
            // measure a near-empty filtered stream (see tests/utils/server.rs).
            set_hide_unranked_authors(false);
        });
    });
}
