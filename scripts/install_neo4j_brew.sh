#!/usr/bin/env bash
set -euo pipefail

repo_root_now() {
  cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd
}

load_benchmark_env_now() {
  REPO_ROOT="$(repo_root_now)"
  export REPO_ROOT
  cd "$REPO_ROOT"
  BENCH_ENV_FILE="${KNIGHT_BUS_ENV_FILE:-$REPO_ROOT/.env.neo4j.local}"
  export BENCH_ENV_FILE
  NEO4J_URI_VALUE="${NEO4J_URI:-bolt://127.0.0.1:7687}"
  NEO4J_USER_VALUE="${NEO4J_USER:-neo4j}"
  NEO4J_PASSWORD_VALUE="${NEO4J_PASSWORD:-knightbus-local-neo4j}"
  NEO4J_DATABASE_VALUE="${NEO4J_DATABASE:-neo4j}"
}

bootout_neo4j_service_now() {
  local launch_agent="$HOME/Library/LaunchAgents/homebrew.mxcl.neo4j.plist"
  if [[ -f "$launch_agent" ]]; then
    launchctl bootout "gui/$(id -u)" "$launch_agent" >/dev/null 2>&1 || true
  fi
}

require_command_now() {
  local command_name="$1"
  if [[ "$command_name" = /* ]]; then
    if [[ ! -x "$command_name" ]]; then
      echo "Missing required command: $command_name" >&2
      exit 1
    fi
    return 0
  fi
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Missing required command: $command_name" >&2
    exit 1
  fi
}

find_neo4j_binary_now() {
  local formula_prefix
  formula_prefix="$(/opt/homebrew/bin/brew --prefix neo4j 2>/dev/null || true)"
  if [[ -n "$formula_prefix" && -x "$formula_prefix/bin/neo4j" ]]; then
    echo "$formula_prefix/bin/neo4j"
    return 0
  fi
  command -v neo4j || true
}

find_neo4j_admin_now() {
  local formula_prefix
  formula_prefix="$(/opt/homebrew/bin/brew --prefix neo4j 2>/dev/null || true)"
  if [[ -n "$formula_prefix" && -x "$formula_prefix/bin/neo4j-admin" ]]; then
    echo "$formula_prefix/bin/neo4j-admin"
    return 0
  fi
  command -v neo4j-admin || true
}

install_neo4j_formula_now() {
  if /opt/homebrew/bin/brew list neo4j >/dev/null 2>&1; then
    echo "Neo4j already installed via Homebrew"
    return 0
  fi
  /opt/homebrew/bin/brew install neo4j
}

write_benchmark_env_now() {
  cat >"$BENCH_ENV_FILE" <<EOF
NEO4J_URI=$NEO4J_URI_VALUE
NEO4J_USER=$NEO4J_USER_VALUE
NEO4J_PASSWORD=$NEO4J_PASSWORD_VALUE
NEO4J_DATABASE=$NEO4J_DATABASE_VALUE
EOF
}

initialize_password_now() {
  local neo4j_admin_bin="$1"
  if [[ -f "$BENCH_ENV_FILE" ]]; then
    echo "Benchmark env already exists at $BENCH_ENV_FILE"
    return 0
  fi
  /opt/homebrew/bin/brew services stop neo4j >/dev/null 2>&1 || true
  if "$neo4j_admin_bin" dbms set-initial-password "$NEO4J_PASSWORD_VALUE" >/tmp/knight_bus_neo4j_password.log 2>&1; then
    write_benchmark_env_now
    return 0
  fi
  echo "Existing installation not managed by this repo; could not initialize password." >&2
  cat /tmp/knight_bus_neo4j_password.log >&2 || true
  exit 1
}

start_neo4j_service_now() {
  bootout_neo4j_service_now
  /opt/homebrew/bin/brew services start neo4j
}

print_neo4j_version_now() {
  local neo4j_bin="$1"
  "$neo4j_bin" version
}

main() {
  load_benchmark_env_now
  require_command_now /opt/homebrew/bin/brew
  require_command_now java
  install_neo4j_formula_now
  local neo4j_bin
  local neo4j_admin_bin
  neo4j_bin="$(find_neo4j_binary_now)"
  neo4j_admin_bin="$(find_neo4j_admin_now)"
  if [[ -z "$neo4j_bin" || -z "$neo4j_admin_bin" ]]; then
    echo "Neo4j binaries not found after Homebrew install" >&2
    exit 1
  fi
  initialize_password_now "$neo4j_admin_bin"
  start_neo4j_service_now
  print_neo4j_version_now "$neo4j_bin"
  echo "Wrote benchmark env to $BENCH_ENV_FILE"
}

main "$@"
