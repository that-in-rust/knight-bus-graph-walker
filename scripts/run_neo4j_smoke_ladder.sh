#!/usr/bin/env bash
set -euo pipefail

repo_root_now() {
  cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd
}

load_runtime_paths_now() {
  REPO_ROOT="$(repo_root_now)"
  export REPO_ROOT
  cd "$REPO_ROOT"
  BENCH_VENV_DIR="${KNIGHT_BUS_VENV_DIR:-$REPO_ROOT/.venv-bench}"
  ARTIFACTS_DIR="${KNIGHT_BUS_ARTIFACTS_DIR:-$REPO_ROOT/artifacts}"
  REPORTS_DIR="${KNIGHT_BUS_REPORTS_DIR:-$REPO_ROOT/reports}"
  BENCH_ENV_FILE="${KNIGHT_BUS_ENV_FILE:-$REPO_ROOT/.env.neo4j.local}"
  INSTALL_SCRIPT="$REPO_ROOT/scripts/install_neo4j_brew.sh"
  REQUIREMENTS_FILE="$REPO_ROOT/benchmarks/walk_hopper_v1/requirements.txt"
  PYTHON_BIN="${KNIGHT_BUS_PYTHON_BIN:-python3}"
  KNIGHT_BUS_BIN_PATH="${KNIGHT_BUS_BIN_PATH:-$REPO_ROOT/target/release/knight-bus}"
  FINAL_JOURNAL_SCRIPT="$REPO_ROOT/scripts/render_final_testing_journal.py"
  export BENCH_VENV_DIR ARTIFACTS_DIR REPORTS_DIR BENCH_ENV_FILE INSTALL_SCRIPT REQUIREMENTS_FILE PYTHON_BIN
}

bootout_neo4j_service_now() {
  local launch_agent="$HOME/Library/LaunchAgents/homebrew.mxcl.neo4j.plist"
  if [[ -f "$launch_agent" ]]; then
    launchctl bootout "gui/$(id -u)" "$launch_agent" >/dev/null 2>&1 || true
  fi
}

wait_for_neo4j_stopped_now() {
  local timeout_seconds="${1:-60}"
  local started_at
  started_at="$(date +%s)"
  while true; do
    if ! lsof -nP -iTCP:7687 -sTCP:LISTEN >/dev/null 2>&1; then
      return 0
    fi
    if (( $(date +%s) - started_at >= timeout_seconds )); then
      echo "Neo4j did not stop within ${timeout_seconds}s" >&2
      exit 1
    fi
    sleep 2
  done
}

wait_for_neo4j_ready_now() {
  local timeout_seconds="${1:-60}"
  local started_at
  started_at="$(date +%s)"
  while true; do
    if "$PYTHON_BIN" - <<PY >/dev/null 2>&1
import socket
sock = socket.socket()
sock.settimeout(1.0)
sock.connect(("127.0.0.1", 7687))
sock.close()
PY
    then
      return 0
    fi
    if (( $(date +%s) - started_at >= timeout_seconds )); then
      echo "Neo4j did not become ready within ${timeout_seconds}s" >&2
      exit 1
    fi
    sleep 2
  done
}

force_failure_if_requested_now() {
  if [[ "${KNIGHT_BUS_FORCE_SMOKE_FAILURE:-0}" == "1" ]]; then
    mkdir -p "$ARTIFACTS_DIR" "$REPORTS_DIR"
    echo "forced smoke failure for stop-rule verification" >&2
    exit 1
  fi
}

ensure_local_env_now() {
  if [[ "${KNIGHT_BUS_SKIP_NEO4J_INSTALL:-0}" == "1" ]]; then
    echo "Skipping Neo4j install because KNIGHT_BUS_SKIP_NEO4J_INSTALL=1"
    return 0
  fi
  SECONDS=0
  "$INSTALL_SCRIPT"
  if (( SECONDS > 2400 )); then
    echo "Neo4j install/setup exceeded 40 minutes" >&2
    exit 1
  fi
}

source_local_env_now() {
  if [[ ! -f "$BENCH_ENV_FILE" ]]; then
    echo "Missing benchmark env file: $BENCH_ENV_FILE" >&2
    exit 1
  fi
  # shellcheck disable=SC1090
  source "$BENCH_ENV_FILE"
  export NEO4J_URI NEO4J_USER NEO4J_PASSWORD NEO4J_DATABASE
}

create_benchmark_venv_now() {
  "$PYTHON_BIN" -m venv "$BENCH_VENV_DIR"
  "$BENCH_VENV_DIR/bin/python" -m pip install -r "$REQUIREMENTS_FILE"
}

build_knight_bus_binary_now() {
  cargo build --release --manifest-path "$REPO_ROOT/Cargo.toml"
  if [[ ! -x "$KNIGHT_BUS_BIN_PATH" ]]; then
    echo "knight-bus binary not found at $KNIGHT_BUS_BIN_PATH" >&2
    exit 1
  fi
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

start_neo4j_service_now() {
  bootout_neo4j_service_now
  /opt/homebrew/bin/brew services start neo4j
}

run_single_stage_now() {
  local stage_name="$1"
  local target_mb="$2"
  local per_family="$3"
  local seed="$4"
  local artifact_dir="$ARTIFACTS_DIR/$stage_name"
  local snapshot_dir="$artifact_dir/snapshot"
  local corpus_snapshot_dir="$artifact_dir/query_corpus_snapshot"
  local neo4j_import_dir="$artifact_dir/neo4j_import"
  local report_dir="$REPORTS_DIR/$stage_name"
  local neo4j_admin_bin

  rm -rf "$artifact_dir" "$report_dir"
  mkdir -p "$artifact_dir" "$report_dir"
  "$BENCH_VENV_DIR/bin/python" "$REPO_ROOT/benchmarks/walk_hopper_v1/generate_code_sparse_data.py" \
    --target-raw-mb "$target_mb" \
    --seed "$seed" \
    --output "$artifact_dir"
  "$BENCH_VENV_DIR/bin/python" "$REPO_ROOT/benchmarks/walk_hopper_v1/build_dual_csr_snapshot.py" \
    --dataset "$artifact_dir" \
    --output "$corpus_snapshot_dir"
  "$KNIGHT_BUS_BIN_PATH" build \
    --nodes-csv "$artifact_dir/nodes.csv" \
    --edges-csv "$artifact_dir/edges.csv" \
    --output "$snapshot_dir"
  "$BENCH_VENV_DIR/bin/python" "$REPO_ROOT/benchmarks/walk_hopper_v1/export_neo4j_import.py" \
    --dataset "$artifact_dir" \
    --output "$neo4j_import_dir"
  "$BENCH_VENV_DIR/bin/python" - <<PY
from pathlib import Path
from benchmarks.walk_hopper_v1.query_walk_snapshot import build_query_corpus_now
build_query_corpus_now(Path("$corpus_snapshot_dir"), Path("$artifact_dir/query_corpus.csv"), per_family=$per_family)
PY

  neo4j_admin_bin="$(find_neo4j_admin_now)"
  if [[ -z "$neo4j_admin_bin" ]]; then
    echo "neo4j-admin not found" >&2
    exit 1
  fi

  /opt/homebrew/bin/brew services stop neo4j >/dev/null 2>&1 || true
  wait_for_neo4j_stopped_now 90
  bootout_neo4j_service_now
  "$neo4j_admin_bin" database import full \
    --dry-run=true \
    --overwrite-destination=true \
    --report-file "$report_dir/import-dry-run.txt" \
    neo4j \
    --nodes="$neo4j_import_dir/nodes.header.csv,$neo4j_import_dir/nodes.data.csv" \
    --relationships="$neo4j_import_dir/relationships.header.csv,$neo4j_import_dir/relationships.data.csv"
  local import_started_ns
  import_started_ns="$("$PYTHON_BIN" - <<'PY'
import time
print(time.perf_counter_ns())
PY
)"
  "$neo4j_admin_bin" database import full \
    --overwrite-destination=true \
    --report-file "$report_dir/import-report.txt" \
    neo4j \
    --nodes="$neo4j_import_dir/nodes.header.csv,$neo4j_import_dir/nodes.data.csv" \
    --relationships="$neo4j_import_dir/relationships.header.csv,$neo4j_import_dir/relationships.data.csv"
  local import_finished_ns
  import_finished_ns="$("$PYTHON_BIN" - <<'PY'
import time
print(time.perf_counter_ns())
PY
)"
  "$PYTHON_BIN" - <<PY
import json
from pathlib import Path
started = int("$import_started_ns")
finished = int("$import_finished_ns")
payload = {
    "import_duration_ms": round((finished - started) / 1_000_000.0, 6),
}
Path("$report_dir/import-meta.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
PY
  start_neo4j_service_now
  wait_for_neo4j_ready_now 90

  "$BENCH_VENV_DIR/bin/python" "$REPO_ROOT/benchmarks/walk_hopper_v1/bench_walk_vs_neo4j.py" \
    --dataset "$artifact_dir" \
    --snapshot "$snapshot_dir" \
    --knight-bus-bin "$KNIGHT_BUS_BIN_PATH" \
    --neo4j-uri "$NEO4J_URI" \
    --neo4j-user "$NEO4J_USER" \
    --neo4j-password "$NEO4J_PASSWORD" \
    --neo4j-database "$NEO4J_DATABASE" \
    --per-family "$per_family" \
    --report "$report_dir"
}

main() {
  load_runtime_paths_now
  force_failure_if_requested_now
  ensure_local_env_now
  source_local_env_now
  create_benchmark_venv_now
  build_knight_bus_binary_now
  mkdir -p "$ARTIFACTS_DIR" "$REPORTS_DIR"
  run_single_stage_now "neo4j_smoke_1mb" "1" "6" "7"
  run_single_stage_now "neo4j_preflight_50mb" "50" "20" "7"
  "$BENCH_VENV_DIR/bin/python" "$FINAL_JOURNAL_SCRIPT" \
    --repo-root "$REPO_ROOT" \
    --reports-dir "$REPORTS_DIR"
}

main "$@"
