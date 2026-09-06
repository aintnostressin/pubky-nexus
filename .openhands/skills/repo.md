# pubky-nexus repo guide

Environment is ALREADY SET UP by `.openhands/setup.sh` before this session
started. Do NOT reinstall toolchains (rustup/cargo), cargo-nextest, sccache, or
system packages, and do NOT restart or rebuild the docker services. Caches
(`CARGO_HOME`, `CARGO_TARGET_DIR`, `SCCACHE_DIR`) live under `/cache` and are
already exported in your shell (also appended to `~/.bashrc`).

## Crate layout

Cargo workspace (resolver = "2"), members:

- `nexus-common` — library: DB connectors (Neo4j via neo4rs, Redis via
  deadpool-redis), models, queries. Shared by the other crates.
- `nexus-webapi` — REST API server (axum + utoipa; Swagger UI at
  `/swagger-ui`). Routes under `src/routes/`.
- `nexus-watcher` — event aggregator: consumes Pubky homeserver events into the
  social graph.
- `nexusd` — CLI/daemon binary (`main.rs`) that runs `api`, `watcher`, `db`,
  `jobs`, migrations, and trust recompute.
- `examples` — small example binaries (crate `pubky-nexus-examples`; has its
  own README).

## Commands (exact)

- Build: `cargo build --workspace --all-targets`
- Lint: `cargo clippy --all-targets -- -D warnings` (warnings are errors)
- Format: `cargo fmt -- --check` (fix with `cargo fmt`)
- Tests use **cargo-nextest**, not `cargo test`:
  - `cargo nextest run -p nexus-common --no-fail-fast`
  - `cargo nextest run -p nexus-webapi --no-fail-fast`
  - `cargo nextest run -p nexus-watcher --no-fail-fast`
  - `cargo nextest run -p nexusd --no-fail-fast` (run LAST, see below)
  - Single test/filter: `cargo nextest run -p nexus-watcher files::create --no-fail-fast`
- Bench: `cargo bench -p nexus-webapi [--bench user]`
- Run the app: `cargo run -p nexusd` (also `... -- watcher`, `... -- api`,
  `... -- db clear --yes`, `... -- db migration run`, `... -- jobs list|run <name>`)

## Test services (external dependencies)

Integration tests require Neo4j, Redis, and Postgres. `.openhands/setup.sh`
starts the `docker/` compose stack (profile `tests`) when a docker daemon is
reachable. If docker is unavailable, point the app at services on the host via
`host.docker.internal` using `NexusConfig::test_config()` /
`NexusConfig::default()`, or run `cargo run -p nexusd -- --config-dir=<dir>`
with a config whose endpoints use `host.docker.internal`.

Default local endpoints: Neo4j bolt `localhost:7687` (HTTP 7474, auth
`neo4j/12345678`), Redis `localhost:6379`, Postgres `localhost:5432`
(see `docker/.env-sample`).

- **Load mock data first** or tests fail out of sync:
  `cargo run -p nexusd -- db mock` (set `CONTAINER_RUNTIME=podman` if using podman)
- **nexus-watcher** tests also need Postgres:
  `export TEST_PUBKY_CONNECTION_STRING=postgres://test_user:test_pass@localhost:5432/postgres?pubky-test=true`
  (from `docker/.env-sample`; adjust host/port if using host services)
- **nexusd** tests (trust-rank) require the Neo4j GDS plugin, baked into the
  compose image `pubky-nexus/neo4j:5.26.27-gds2.13.10` (built from
  `docker/neo4j/Dockerfile`). Run the nexusd suite LAST: its trust recompute
  writes a `trust` property on all `:User` nodes and would skew other suites.

## Conventions / gotchas

- Migrations: scaffold with `cargo run -p nexusd -- db migration new <Name>`,
  then register it in `import_migrations` in `nexusd/src/migrations/mod.rs`
  (guide: `examples/migration.rs`).
- Cron config (`[jobs.<name>]` in config.toml) is SECONDS-FIRST, 6 fields
  (`sec min hour dom mon dow [year]`), not the standard 5-field crontab.
- App config defaults to `$HOME/.pubky-nexus/config.toml`.
- Do not commit: `docker/.env`, `docker/.database*`, `examples/static`,
  `nexus-webapi/static`, `nexus-watcher/static`, `/target` (all gitignored).
- CI (`.github/workflows/`): `format.yml` = `cargo fmt -- --check`,
  `lint.yml` = clippy with `-D warnings`, `test.yml` = compose stack + mock
  data + the four nextest suites in the order above.
