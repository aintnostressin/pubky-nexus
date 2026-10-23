use clap::Parser;
use nexus_common::file::{default_config_dir_path, validate_and_expand_path};
use nexus_common::models::user::PubkyConnectorResolver;
use nexus_common::{types::DynError, ApiConfig};
use nexus_watcher::events::UserIngestor;
use nexus_webapi::{api_context::ApiContextBuilder, NexusApi, NexusApiBuilder};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(about = "Example Nexus API server", long_about = None)]
struct Opt {
    /// Path to a directory containing `api-config.toml`, which is created with the defaults if missing
    /// If omitted, runs the built-in default API config
    #[arg(short, long, value_name = "DIR")]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let opts = Opt::parse();
    // The API's ingest-user endpoint ingests through the watcher's ingestor
    match opts.config {
        Some(path) => {
            let expanded_path = validate_and_expand_path(path)?;
            NexusApi::start_from_path(
                expanded_path,
                |config| {
                    let resolver = Arc::new(PubkyConnectorResolver);
                    Arc::new(UserIngestor::from_config(&config.stack, resolver))
                },
                None,
            )
            .await?
        }
        None => {
            let api_config = ApiConfig::default();
            let resolver = Arc::new(PubkyConnectorResolver);
            let ingestor = UserIngestor::from_config(&api_config.stack, resolver);
            let api_context =
                ApiContextBuilder::new(api_config, default_config_dir_path(), Arc::new(ingestor))
                    .try_build()?;
            NexusApiBuilder::new(api_context).start(None).await?
        }
    };

    Ok(())
}
