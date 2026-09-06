#!/bin/bash
# OpenHands repo setup: runs automatically before the agent starts working.
# Keeps ALL toolchain/build setup out of agent steps (wall-clock only, no tokens).
#
# Cache strategy: cargo home, target dir, and sccache live under /cache, which
# the user is expected to bind-mount to host dirs so builds are warm across
# sandbox lifecycles, e.g.:
#   SANDBOX_VOLUMES="$PWD:/workspace:rw,\
#   $HOME/.cache/oh/cargo:/cache/cargo:rw,\
#   $HOME/.cache/oh/target:/cache/target:rw,\
#   $HOME/.cache/oh/sccache:/cache/sccache:rw"
#   SANDBOX_USER_ID=$(id -u)   # avoid root-owned files in those host dirs
# If /cache is not mounted we fall back to the image defaults (cold but correct).
set -euo pipefail

CACHE_ROOT=/cache
if [ -d "$CACHE_ROOT" ] && [ -w "$CACHE_ROOT" ]; then
  export CARGO_HOME=$CACHE_ROOT/cargo
  export CARGO_TARGET_DIR=$CACHE_ROOT/target
  mkdir -p "$CARGO_HOME" "$CARGO_TARGET_DIR" "$CACHE_ROOT/sccache"
else
  export CARGO_HOME=${CARGO_HOME:-$HOME/.cargo}
  export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$PWD/target}
fi

# sccache as the rustc wrapper only when it is available.
if command -v sccache >/dev/null 2>&1; then
  export RUSTC_WRAPPER=sccache
  if [ -d "$CACHE_ROOT" ] && [ -w "$CACHE_ROOT" ]; then
    export SCCACHE_DIR=$CACHE_ROOT/sccache
  fi
fi

# Persist the environment for shells the agent opens later in the session.
{
  echo "export CARGO_HOME=$CARGO_HOME"
  echo "export CARGO_TARGET_DIR=$CARGO_TARGET_DIR"
  [ -n "${RUSTC_WRAPPER:-}" ] && echo "export RUSTC_WRAPPER=$RUSTC_WRAPPER"
  [ -n "${SCCACHE_DIR:-}" ] && echo "export SCCACHE_DIR=$SCCACHE_DIR"
  echo '[ -f "$CARGO_HOME/env" ] && source "$CARGO_HOME/env"'
} >> ~/.bashrc

# Rust toolchain: install only if missing (a custom image makes this a no-op).
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain stable --profile default
fi
# rustup installs shims under CARGO_HOME; make them visible in this script.
[ -f "$CARGO_HOME/env" ] && source "$CARGO_HOME/env"

# Lint/format components must match CI (format.yml / lint.yml).
rustup component add clippy rustfmt >/dev/null 2>&1 || true

# cargo-nextest: the test runner used by CI. Avoid a long source build on first
# run via the pre-built binary when possible.
if ! command -v cargo-nextest >/dev/null 2>&1; then
  if [ "$(uname -s)-$(uname -m)" = "Linux-x86_64" ]; then
    mkdir -p "$CARGO_HOME/bin"
    curl -LsSf https://get.nexte.st/latest/linux | tar zxf - -C "$CARGO_HOME/bin" \
      || cargo install cargo-nextest --locked
  else
    cargo install cargo-nextest --locked
  fi
fi

cargo fetch --locked || true

# Warm the build cache (incremental thanks to /cache + sccache; first run is
# slow, every run after is warm). Never let a warm-up failure block the agent.
cargo build --workspace --all-targets || true

# ---- Test services (Neo4j + Redis + Postgres) -------------------------------
# Tests need Neo4j (with the GDS plugin baked into docker/neo4j/Dockerfile),
# Redis, and Postgres. Start the compose stack when a working container
# runtime is present; otherwise the agent should point NexusConfig at services
# on the host via host.docker.internal (see .openhands/skills/repo.md).
port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
export -f port_open

if docker info >/dev/null 2>&1; then
  cd docker
  [ -f .env ] || cp .env-sample .env
  docker compose --profile tests up -d || true

  # Wait on healthchecks so tests don't flake on cold services.
  timeout 60 bash -c 'until port_open 6379; do sleep 1; done' \
    && echo "Redis is ready" || echo "WARN: Redis not reachable on 6379"
  timeout 180 bash -c 'until curl -sf http://localhost:7474 > /dev/null; do sleep 2; done' \
    && echo "Neo4j is ready" || echo "WARN: Neo4j not reachable on 7474"
  timeout 60 bash -c 'until port_open 5432; do sleep 1; done' \
    && echo "Postgres is ready" || echo "WARN: Postgres not reachable on 5432"
  cd ..
else
  echo "Docker daemon not reachable; skipping test service startup."
  echo "Run tests against services on the host via host.docker.internal."
fi
