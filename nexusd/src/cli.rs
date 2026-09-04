use clap::{Args, Parser, Subcommand};
use nexus_common::file::{default_config_dir_path, validate_and_expand_path};
use nexus_webapi::mock::MockType;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "pubky-nexus")]
#[command(about = "Pubky Nexus CLI", long_about = None)]
pub struct Cli {
    /// Directory containing `config.toml`
    #[arg(short, long, default_value_os_t = default_config_dir_path(), value_parser = validate_config_dir_path)]
    pub config_dir: PathBuf,

    #[command(subcommand)]
    pub command: Option<NexusCommands>,
}

impl Cli {
    pub fn receive_command(cli: Cli) -> NexusCommands {
        match cli.command {
            // The top-level `config_dir` was already captured by the caller; the
            // synthetic `Run` carries no subcommand-level override.
            None => NexusCommands::Run(ConfigDirArgs { config_dir: None }),
            Some(command) => command,
        }
    }
}

/// Validate that the data_dir path is a directory.
/// It doesnt need to exist, but if it does, it needs to be a directory.
fn validate_config_dir_path(path: &str) -> Result<PathBuf, String> {
    validate_and_expand_path(PathBuf::from(path)).map_err(|e| e.to_string())
}

#[derive(Subcommand, Debug)]
pub enum NexusCommands {
    /// Run the API service
    Api(ConfigDirArgs),

    /// Run the event watcher
    Watcher(ConfigDirArgs),

    /// Run scheduled jobs on demand
    #[command(subcommand)]
    Jobs(JobCommands),

    /// Database operations
    #[command(subcommand)]
    Db(DbCommands),

    /// Run the API, the Watcher and the scheduled Jobs (default when no arguments are given)
    #[command(hide = true)]
    Run(ConfigDirArgs),
}

#[derive(Args, Debug)]
pub struct ConfigDirArgs {
    /// Directory containing `config.toml`. Overrides the top-level `--config-dir`
    #[arg(short, long, value_parser = validate_config_dir_path)]
    pub config_dir: Option<PathBuf>,
}

impl ConfigDirArgs {
    /// The subcommand-level value wins; otherwise fall back to the top-level one.
    pub fn resolve(self, root: PathBuf) -> PathBuf {
        self.config_dir.unwrap_or(root)
    }
}

#[derive(Subcommand, Debug)]
pub enum JobCommands {
    /// Run a single job once, now
    Run(JobRunArgs),

    /// List the available jobs
    List,
}

#[derive(Args, Debug)]
pub struct JobRunArgs {
    /// Name of the job to run (see `jobs list`)
    #[arg(required = true)]
    pub name: String,

    #[command(flatten)]
    pub config: ConfigDirArgs,
}

#[derive(Subcommand, Debug)]
pub enum DbCommands {
    /// Clear the databases (destructive, requires --yes)
    Clear {
        /// Confirm wiping the Redis logical database (FLUSHDB) and every node
        /// in the Neo4j graph
        #[arg(long)]
        yes: bool,

        #[command(flatten)]
        config: ConfigDirArgs,
    },

    /// Mock the database (optional redis/graph). Usually for tests
    Mock(MockArgs),

    /// Manage database migrations
    #[command(subcommand)]
    Migration(MigrationCommands),
}

#[derive(Args, Debug)]
pub struct MockArgs {
    /// Specify which part of the database to mock: redis, graph, or both (default: both)
    #[arg(long)]
    pub mock_type: Option<MockType>,

    #[command(flatten)]
    pub config: ConfigDirArgs,
}

#[derive(Subcommand, Debug)]
pub enum MigrationCommands {
    /// Create a new migration with a required migration name
    New(MigrationNewArgs),

    /// Run pending migrations
    Run,

    /// Check for pending migrations without running them.
    /// Exits 0 when nothing is pending, 10 when at least one migration has pending work.
    Check,
}

#[derive(Args, Debug)]
pub struct MigrationNewArgs {
    /// The name of the new migration
    #[arg(required = true)]
    pub name: String,
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    /// Every subcommand that reads a config dir, i.e. takes a `ConfigDirArgs`.
    /// When you add a new variant carrying `ConfigDirArgs`, add it here too —
    /// the table-driven tests below are what keep its override behavior honest.
    const CONFIG_DIR_SUBCOMMANDS: &[&[&str]] = &[
        &["api"],
        &["watcher"],
        &["run"],
        &["jobs", "run", "some-job"],
        &["db", "clear", "--yes"],
        &["db", "mock"],
    ];

    /// Extract the `ConfigDirArgs` from a parsed `Cli` and resolve it against
    /// the top-level dir, so table-driven tests can assert on the effective dir.
    fn resolved_config_dir(cli: Cli, root: &str) -> Option<PathBuf> {
        match cli.command {
            Some(NexusCommands::Api(args))
            | Some(NexusCommands::Watcher(args))
            | Some(NexusCommands::Run(args)) => Some(args.resolve(root.into())),
            Some(NexusCommands::Jobs(JobCommands::Run(JobRunArgs { config, .. }))) => {
                Some(config.resolve(root.into()))
            }
            Some(NexusCommands::Db(DbCommands::Clear { config, .. })) => {
                Some(config.resolve(root.into()))
            }
            Some(NexusCommands::Db(DbCommands::Mock(args))) => {
                Some(args.config.resolve(root.into()))
            }
            _ => None,
        }
    }

    /// Catches duplicate arg IDs and malformed flattens at test time.
    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn subcommand_inherits_top_level_config_dir() {
        for sub in CONFIG_DIR_SUBCOMMANDS {
            let mut argv: Vec<&str> = vec!["nexusd", "-c", "root-dir"];
            argv.extend_from_slice(sub);
            let label = sub.join(" ");
            let cli = Cli::try_parse_from(argv)
                .unwrap_or_else(|err| panic!("`nexusd {label}` failed to parse: {err}"));
            let resolved = resolved_config_dir(cli, "root-dir")
                .unwrap_or_else(|| panic!("`nexusd {label}` does not carry a ConfigDirArgs"));
            assert_eq!(
                resolved,
                PathBuf::from("root-dir"),
                "`nexusd {label}` must inherit the top-level config dir"
            );
        }
    }

    #[test]
    fn subcommand_config_dir_overrides_top_level() {
        for sub in CONFIG_DIR_SUBCOMMANDS {
            let mut argv: Vec<&str> = vec!["nexusd", "-c", "root-dir"];
            argv.extend_from_slice(sub);
            argv.extend_from_slice(&["-c", "sub-dir"]);
            let label = sub.join(" ");
            let cli = Cli::try_parse_from(argv)
                .unwrap_or_else(|err| panic!("`nexusd {label}` failed to parse: {err}"));
            let resolved = resolved_config_dir(cli, "root-dir")
                .unwrap_or_else(|| panic!("`nexusd {label}` does not carry a ConfigDirArgs"));
            assert_eq!(
                resolved,
                PathBuf::from("sub-dir"),
                "`nexusd {label}` must let the subcommand-level -c win"
            );
        }
    }

    #[test]
    fn bare_invocation_uses_top_level_config_dir() {
        let cli = Cli::try_parse_from(["nexusd", "-c", "root-dir"]).expect("should parse");
        assert!(
            cli.command.is_none(),
            "bare invocation must not carry a subcommand"
        );
        match Cli::receive_command(cli) {
            NexusCommands::Run(args) => {
                assert_eq!(
                    args.resolve(PathBuf::from("root-dir")),
                    PathBuf::from("root-dir")
                );
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    /// Pins the help-surface intent: subcommands that do not read a config dir
    /// must not accept -c, so a future `global = true` fails here instead of
    /// silently advertising a flag that would be ignored.
    #[test]
    fn config_dir_absent_from_subcommands_that_ignore_it() {
        for argv in [
            &["nexusd", "jobs", "list", "-c", "dir"][..],
            &["nexusd", "db", "migration", "new", "x", "-c", "dir"][..],
        ] {
            let label = argv[1..].join(" ");
            let err =
                Cli::try_parse_from(argv).expect_err("-c must be rejected by this subcommand");
            assert!(
                matches!(err.kind(), clap::error::ErrorKind::UnknownArgument),
                "expected UnknownArgument for `nexusd {label}`, got {err:?}"
            );
        }
    }

    #[test]
    fn db_clear_without_yes_parses_as_unconfirmed() {
        let cli = Cli::try_parse_from(["nexusd", "db", "clear"]).expect("should parse");
        match cli.command {
            Some(NexusCommands::Db(DbCommands::Clear { yes, .. })) => assert!(!yes),
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn db_clear_with_yes_parses_as_confirmed() {
        let cli = Cli::try_parse_from(["nexusd", "db", "clear", "--yes"]).expect("should parse");
        match cli.command {
            Some(NexusCommands::Db(DbCommands::Clear { yes, .. })) => assert!(yes),
            other => panic!("unexpected command: {other:?}"),
        }
    }

    /// The top-level --config-dir must be available to db commands so they
    /// operate on the configured stack rather than the default one.
    #[test]
    fn db_clear_keeps_top_level_config_dir() {
        let cli = Cli::try_parse_from([
            "nexusd",
            "--config-dir",
            "/custom/dir",
            "db",
            "clear",
            "--yes",
        ])
        .expect("should parse");
        assert_eq!(cli.config_dir, PathBuf::from("/custom/dir"));
        match cli.command {
            Some(NexusCommands::Db(DbCommands::Clear { yes, .. })) => assert!(yes),
            other => panic!("unexpected command: {other:?}"),
        }
    }
}
