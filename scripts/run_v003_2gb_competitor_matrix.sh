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
  MATRIX_REPORT_DIR="${KNIGHT_BUS_MATRIX_REPORT_DIR:-$REPORTS_DIR/v003_2gb_competitors}"
  V003_OUTPUT_DIR="${KNIGHT_BUS_V003_OUTPUT_DIR:-$REPO_ROOT/v003-research}"
  BENCH_ENV_FILE="${KNIGHT_BUS_ENV_FILE:-$REPO_ROOT/.env.neo4j.local}"
  INSTALL_SCRIPT="$REPO_ROOT/scripts/install_neo4j_brew.sh"
  RENDER_SCRIPT="$REPO_ROOT/scripts/render_v003_competitor_markdown.py"
  REQUIREMENTS_FILE="$REPO_ROOT/benchmarks/walk_hopper_v1/requirements.txt"
  PYTHON_BIN="${KNIGHT_BUS_PYTHON_BIN:-python3}"
  KNIGHT_BUS_BIN_PATH="${KNIGHT_BUS_BIN_PATH:-$REPO_ROOT/target/release/knight-bus}"
  DATASET_DIR="${KNIGHT_BUS_DATASET_DIR:-$ARTIFACTS_DIR/code_sparse_2gb}"
  SNAPSHOT_DIR="${KNIGHT_BUS_SNAPSHOT_DIR:-$DATASET_DIR/snapshot}"
  CORPUS_PATH="${KNIGHT_BUS_CORPUS_PATH:-$DATASET_DIR/query_corpus.csv}"
  CORPUS_SNAPSHOT_DIR="${KNIGHT_BUS_CORPUS_SNAPSHOT_DIR:-$DATASET_DIR/query_corpus_snapshot}"
  export BENCH_VENV_DIR ARTIFACTS_DIR REPORTS_DIR MATRIX_REPORT_DIR V003_OUTPUT_DIR BENCH_ENV_FILE
  export INSTALL_SCRIPT RENDER_SCRIPT REQUIREMENTS_FILE PYTHON_BIN KNIGHT_BUS_BIN_PATH
  export DATASET_DIR SNAPSHOT_DIR CORPUS_PATH CORPUS_SNAPSHOT_DIR
}

ensure_local_env_now() {
  if [[ "${KNIGHT_BUS_SKIP_NEO4J_INSTALL:-0}" == "1" ]]; then
    echo "Skipping Neo4j install because KNIGHT_BUS_SKIP_NEO4J_INSTALL=1"
    return 0
  fi
  "$INSTALL_SCRIPT"
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

ensure_dataset_exists_now() {
  if [[ -f "$DATASET_DIR/nodes.csv" && -f "$DATASET_DIR/edges.csv" && -f "$DATASET_DIR/manifest.json" ]]; then
    return 0
  fi
  mkdir -p "$ARTIFACTS_DIR"
  "$BENCH_VENV_DIR/bin/python" "$REPO_ROOT/benchmarks/walk_hopper_v1/generate_code_sparse_data.py" \
    --target-raw-gb 2 \
    --seed 7 \
    --output "$DATASET_DIR"
}

ensure_snapshot_exists_now() {
  if [[ -f "$SNAPSHOT_DIR/manifest.json" ]]; then
    return 0
  fi
  "$KNIGHT_BUS_BIN_PATH" build \
    --nodes-csv "$DATASET_DIR/nodes.csv" \
    --edges-csv "$DATASET_DIR/edges.csv" \
    --output "$SNAPSHOT_DIR"
}

ensure_query_corpus_now() {
  if [[ -f "$CORPUS_PATH" ]]; then
    return 0
  fi
  if [[ ! -f "$CORPUS_SNAPSHOT_DIR/manifest.json" ]]; then
    "$BENCH_VENV_DIR/bin/python" "$REPO_ROOT/benchmarks/walk_hopper_v1/build_dual_csr_snapshot.py" \
      --dataset "$DATASET_DIR" \
      --output "$CORPUS_SNAPSHOT_DIR"
  fi
  "$BENCH_VENV_DIR/bin/python" - <<PY
from pathlib import Path
from benchmarks.walk_hopper_v1.query_walk_snapshot import build_query_corpus_now
build_query_corpus_now(Path("$CORPUS_SNAPSHOT_DIR"), Path("$CORPUS_PATH"), per_family=20)
PY
}

run_competitor_matrix_now() {
  mkdir -p "$MATRIX_REPORT_DIR"
  "$BENCH_VENV_DIR/bin/python" "$REPO_ROOT/benchmarks/walk_hopper_v1/bench_walk_competitors.py" \
    --dataset "$DATASET_DIR" \
    --snapshot "$SNAPSHOT_DIR" \
    --corpus "$CORPUS_PATH" \
    --report-dir "$MATRIX_REPORT_DIR" \
    --knight-bus-bin "$KNIGHT_BUS_BIN_PATH" \
    --neo4j-uri "$NEO4J_URI" \
    --neo4j-user "$NEO4J_USER" \
    --neo4j-password "$NEO4J_PASSWORD" \
    --neo4j-database "$NEO4J_DATABASE" \
    --backends neo4j memgraph kuzu falkordb hugegraph apache-age janusgraph dgraph
}

render_tracked_markdown_now() {
  "$BENCH_VENV_DIR/bin/python" "$RENDER_SCRIPT" \
    --summary-json "$MATRIX_REPORT_DIR/summary.json" \
    --output-dir "$V003_OUTPUT_DIR"
}

main() {
  load_runtime_paths_now
  create_benchmark_venv_now
  build_knight_bus_binary_now
  ensure_dataset_exists_now
  ensure_snapshot_exists_now
  ensure_query_corpus_now
  ensure_local_env_now
  source_local_env_now
  run_competitor_matrix_now
  render_tracked_markdown_now
}

main "$@"
