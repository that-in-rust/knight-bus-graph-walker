from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from benchmarks.walk_hopper_v1.bench_walk_vs_neo4j import (
    EngineMeasurement,
    build_neo4j_runner_now,
    measure_engine_latency_now,
    validate_engine_parity_now,
    write_report_bundle_now,
)
from benchmarks.walk_hopper_v1.build_dual_csr_snapshot import build_dual_csr_snapshot
from benchmarks.walk_hopper_v1.common import parse_dense_node_index_now, write_json_file_now
from benchmarks.walk_hopper_v1.export_neo4j_import import export_neo4j_import_files
from benchmarks.walk_hopper_v1.generate_code_sparse_data import generate_sparse_code_graph
from benchmarks.walk_hopper_v1.query_walk_snapshot import (
    build_query_corpus_now,
    collect_truth_answers_now,
    load_query_corpus_now,
    load_snapshot_graph_now,
    query_snapshot_family_now,
)


FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "tiny_graph"
REPO_ROOT = FIXTURE_DIR.parents[3]


def materialize_fixture_dataset_now(target_dir: Path) -> Path:
    dataset_dir = target_dir / "fixture_dataset"
    shutil.copytree(FIXTURE_DIR, dataset_dir)
    actual_raw_bytes = (dataset_dir / "nodes.csv").stat().st_size + (dataset_dir / "edges.csv").stat().st_size
    write_json_file_now(
        dataset_dir / "manifest.json",
        {
            "graph_model": "code_sparse",
            "seed": 7,
            "target_raw_bytes": actual_raw_bytes,
            "actual_raw_bytes": actual_raw_bytes,
            "node_count": 7,
            "edge_count": 8,
            "layer_count": 4,
            "degree_palette": [2, 4, 8, 12, 14],
            "degree_summary": {
                "average_out_degree": 1.1429,
                "max_out_degree": 14,
                "min_out_degree": 2,
                "edge_byte_share_estimate": 0.5783,
                "density_ratio": 0.16326531,
            },
            "nodes_csv": "nodes.csv",
            "edges_csv": "edges.csv",
        },
    )
    return dataset_dir


def load_expected_answers_now() -> dict[tuple[str, str], list[str]]:
    raw_answers = json.loads((FIXTURE_DIR / "expected_answers.json").read_text(encoding="utf-8"))
    return {
        tuple(key.split("|", 1)): value
        for key, value in raw_answers.items()
    }


def test_req_bench_001_generator_writes_manifest_now(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "generated"
    manifest = generate_sparse_code_graph(
        target_raw_bytes=2_000_000,
        seed=7,
        output_dir=dataset_dir,
        layer_count=16,
    )
    assert (dataset_dir / "nodes.csv").exists()
    assert (dataset_dir / "edges.csv").exists()
    assert (dataset_dir / "manifest.json").exists()
    with (dataset_dir / "nodes.csv").open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        assert next(reader) == ["node_id", "node_type", "label", "parent_id", "file_path", "span"]
    assert abs(manifest["actual_raw_bytes"] - 2_000_000) <= int(2_000_000 * 0.05)
    assert manifest["node_count"] > 0
    assert manifest["edge_count"] > 0


def test_req_bench_002_sparse_dag_shape_now(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "generated"
    manifest = generate_sparse_code_graph(
        target_raw_bytes=750_000,
        seed=9,
        output_dir=dataset_dir,
        layer_count=24,
    )
    node_layers: dict[str, str] = {}
    with (dataset_dir / "nodes.csv").open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            node_layers[row["node_id"]] = row["file_path"].split("/", 1)[0]
    edge_rows = list(csv.DictReader((dataset_dir / "edges.csv").open("r", encoding="utf-8", newline="")))
    assert manifest["degree_summary"]["average_out_degree"] < 16
    for row in edge_rows[:250]:
        assert node_layers[row["from_id"]] < node_layers[row["to_id"]]
    edge_bytes = (dataset_dir / "edges.csv").stat().st_size
    total_bytes = edge_bytes + (dataset_dir / "nodes.csv").stat().st_size
    assert edge_bytes / total_bytes >= 0.85


def test_req_bench_003_dense_ids_and_determinism_now(tmp_path: Path) -> None:
    left_dir = tmp_path / "left"
    right_dir = tmp_path / "right"
    left_manifest = generate_sparse_code_graph(
        target_raw_bytes=600_000,
        seed=11,
        output_dir=left_dir,
        layer_count=10,
    )
    right_manifest = generate_sparse_code_graph(
        target_raw_bytes=600_000,
        seed=11,
        output_dir=right_dir,
        layer_count=10,
    )
    assert parse_dense_node_index_now("fn:node_000000000123") == 123
    assert left_manifest["node_count"] == right_manifest["node_count"]
    left_snapshot = build_dual_csr_snapshot(left_dir, left_dir / "snapshot")
    right_snapshot = build_dual_csr_snapshot(right_dir, right_dir / "snapshot")
    left_corpus = build_query_corpus_now(left_dir / "snapshot", left_dir / "query_corpus.csv", per_family=12)
    right_corpus = build_query_corpus_now(right_dir / "snapshot", right_dir / "query_corpus.csv", per_family=12)
    assert left_snapshot["edge_count"] == right_snapshot["edge_count"]
    assert hashlib.sha256((left_dir / "query_corpus.csv").read_bytes()).hexdigest() == hashlib.sha256(
        (right_dir / "query_corpus.csv").read_bytes()
    ).hexdigest()
    assert left_corpus == right_corpus


def test_req_bench_004_snapshot_roundtrip_now(tmp_path: Path) -> None:
    dataset_dir = materialize_fixture_dataset_now(tmp_path)
    snapshot_dir = tmp_path / "snapshot"
    build_dual_csr_snapshot(dataset_dir, snapshot_dir)
    graph = load_snapshot_graph_now(snapshot_dir)
    reconstructed_edges: set[tuple[str, str]] = set()
    for node_index in range(int(graph.manifest["node_count"])):
        source_key = f"fn:node_{node_index:012d}"
        for target_key in query_snapshot_family_now(graph, "forward_one", source_key):
            reconstructed_edges.add((source_key, target_key))
    fixture_edges = {
        (row["from_id"], row["to_id"])
        for row in csv.DictReader((dataset_dir / "edges.csv").open("r", encoding="utf-8", newline=""))
    }
    assert reconstructed_edges == fixture_edges


def test_req_bench_005_snapshot_parity_now(tmp_path: Path) -> None:
    dataset_dir = materialize_fixture_dataset_now(tmp_path)
    snapshot_dir = tmp_path / "snapshot"
    build_dual_csr_snapshot(dataset_dir, snapshot_dir)
    graph = load_snapshot_graph_now(snapshot_dir)
    query_rows = load_query_corpus_now(dataset_dir / "query_corpus.csv")
    expected_answers = load_expected_answers_now()
    truth_answers = collect_truth_answers_now(dataset_dir, query_rows)
    for row in query_rows:
        row_key = (row["family_name"], row["node_id"])
        assert truth_answers[row_key] == expected_answers[row_key]
        assert query_snapshot_family_now(graph, row["family_name"], row["node_id"]) == expected_answers[row_key]


def test_req_bench_005_benchmark_aborts_on_mismatch_now() -> None:
    with pytest.raises(ValueError):
        validate_engine_parity_now(
            engine_name="walk_hopper",
            query_runner=lambda family_name, node_id: ["fn:node_000000000999"],
            query_rows=[{"family_name": "forward_one", "node_id": "fn:node_000000000000"}],
            truth_answers={("forward_one", "fn:node_000000000000"): ["fn:node_000000000002"]},
        )


def test_req_bench_006_export_headers_now(tmp_path: Path) -> None:
    dataset_dir = materialize_fixture_dataset_now(tmp_path)
    export_dir = tmp_path / "neo4j_export"
    manifest = export_neo4j_import_files(dataset_dir, export_dir)
    with (export_dir / "nodes.header.csv").open("r", encoding="utf-8", newline="") as handle:
        assert next(csv.reader(handle)) == [
            "node_id:ID",
            ":LABEL",
            "node_type",
            "label",
            "parent_id",
            "file_path",
            "span",
        ]
    with (export_dir / "relationships.header.csv").open("r", encoding="utf-8", newline="") as handle:
        assert next(csv.reader(handle)) == [":START_ID", ":END_ID", ":TYPE", "edge_type"]
    assert (export_dir / "nodes.data.csv").exists()
    assert (export_dir / "relationships.data.csv").exists()
    assert manifest["nodes_header_sha256"]
    assert manifest["nodes_data_sha256"]
    assert manifest["relationships_header_sha256"]
    assert manifest["relationships_data_sha256"]


@pytest.mark.skipif(
    not os.environ.get("NEO4J_TEST_URI"),
    reason="requires NEO4J_TEST_URI, NEO4J_TEST_USER, and NEO4J_TEST_PASSWORD",
)
def test_req_bench_006_neo4j_sample_parity_now(tmp_path: Path) -> None:
    from neo4j import GraphDatabase  # type: ignore

    dataset_dir = materialize_fixture_dataset_now(tmp_path)
    query_rows = load_query_corpus_now(dataset_dir / "query_corpus.csv")
    expected_answers = load_expected_answers_now()
    driver = GraphDatabase.driver(
        os.environ["NEO4J_TEST_URI"],
        auth=(os.environ["NEO4J_TEST_USER"], os.environ["NEO4J_TEST_PASSWORD"]),
    )
    try:
        with driver.session(database=os.environ.get("NEO4J_TEST_DATABASE")) as session:
            session.run("MATCH (n:Entity) DETACH DELETE n").consume()
            for row in csv.DictReader((dataset_dir / "nodes.csv").open("r", encoding="utf-8", newline="")):
                session.run(
                    "CREATE (:Entity {node_id: $node_id, node_type: $node_type, label: $label, "
                    "parent_id: $parent_id, file_path: $file_path, span: $span})",
                    **row,
                ).consume()
            for row in csv.DictReader((dataset_dir / "edges.csv").open("r", encoding="utf-8", newline="")):
                session.run(
                    "MATCH (a:Entity {node_id: $from_id}), (b:Entity {node_id: $to_id}) "
                    "CREATE (a)-[:DEPENDS_ON]->(b)",
                    **row,
                ).consume()
            query_runner = build_neo4j_runner_now(session)
            for row in query_rows:
                row_key = (row["family_name"], row["node_id"])
                assert query_runner(row["family_name"], row["node_id"]) == expected_answers[row_key]
    finally:
        driver.close()


def test_req_bench_007_corpus_builder_deterministic_now(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "generated"
    generate_sparse_code_graph(
        target_raw_bytes=1_200_000,
        seed=13,
        output_dir=dataset_dir,
        layer_count=18,
    )
    snapshot_dir = tmp_path / "snapshot"
    build_dual_csr_snapshot(dataset_dir, snapshot_dir)
    first_rows = build_query_corpus_now(snapshot_dir, dataset_dir / "query_corpus.csv", per_family=15)
    second_rows = build_query_corpus_now(snapshot_dir, tmp_path / "query_corpus_again.csv", per_family=15)
    assert first_rows == second_rows
    for family_name in ("forward_one", "reverse_one", "reverse_two"):
        family_rows = [row for row in first_rows if row["family_name"] == family_name]
        if family_rows:
            assert {"low", "medium", "high"}.issubset({row["degree_bucket"] for row in family_rows})


def test_req_bench_008_report_contains_metrics_now(tmp_path: Path) -> None:
    dataset_dir = materialize_fixture_dataset_now(tmp_path)
    snapshot_dir = tmp_path / "snapshot"
    snapshot_manifest = build_dual_csr_snapshot(dataset_dir, snapshot_dir)
    query_rows = load_query_corpus_now(dataset_dir / "query_corpus.csv")
    payload = write_report_bundle_now(
        report_dir=tmp_path / "report",
        dataset_manifest=json.loads((dataset_dir / "manifest.json").read_text(encoding="utf-8")),
        snapshot_manifest=snapshot_manifest,
        query_rows=query_rows,
        measurements=[
            EngineMeasurement(
                engine_name="walk_hopper",
                status="ok",
                reason=None,
                open_start_ms=1.5,
                operation_count=12,
                mean_ms=0.2,
                p50_ms=0.1,
                p95_ms=0.4,
                p99_ms=0.6,
                rss_bytes=1024,
                version="snapshot-v1",
                cold_run=False,
            )
        ],
    )
    assert payload["environment"]["python_version"]
    assert payload["dataset_manifest"]["actual_raw_bytes"]
    assert payload["measurements"][0]["p95_ms"] == 0.4
    assert (tmp_path / "report" / "summary.md").exists()


def test_req_bench_009_measurement_reports_failure_now() -> None:
    failure_metrics = measure_engine_latency_now(
        engine_name="walk_hopper",
        query_runner=lambda family_name, node_id: (_ for _ in ()).throw(RuntimeError("boom")),
        query_rows=[{"family_name": "forward_one", "node_id": "fn:node_000000000000"}],
    )
    assert failure_metrics["status"] == "failed"
    assert "boom" in failure_metrics["reason"]

    degraded_metrics = measure_engine_latency_now(
        engine_name="walk_hopper",
        query_runner=lambda family_name, node_id: [],
        query_rows=[{"family_name": "forward_one", "node_id": "fn:node_000000000000"}],
        rss_limit_bytes=1,
    )
    assert degraded_metrics["status"] in {"degraded", "failed"}


def test_req_neo4j_001_gitignore_covers_local_runtime_now() -> None:
    ignore_text = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8")
    for expected_line in (".venv-bench/", "artifacts/", "reports/", ".env.neo4j.local"):
        assert expected_line in ignore_text


def test_req_neo4j_001_requirements_exists_now() -> None:
    requirements_path = REPO_ROOT / "benchmarks" / "walk_hopper_v1" / "requirements.txt"
    requirements_text = requirements_path.read_text(encoding="utf-8")
    for expected_package in ("numpy==", "psutil==", "neo4j==", "pytest=="):
        assert expected_package in requirements_text


def test_req_neo4j_002_scripts_and_runbook_exist_now() -> None:
    assert (REPO_ROOT / "scripts" / "install_neo4j_brew.sh").exists()
    assert (REPO_ROOT / "scripts" / "run_neo4j_smoke_ladder.sh").exists()
    assert (REPO_ROOT / "docs" / "neo4j-smoke-runbook.md").exists()


def test_req_neo4j_007_corpus_sizes_now(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "generated"
    generate_sparse_code_graph(
        target_raw_bytes=1_200_000,
        seed=17,
        output_dir=dataset_dir,
        layer_count=18,
    )
    snapshot_dir = tmp_path / "snapshot"
    build_dual_csr_snapshot(dataset_dir, snapshot_dir)
    smoke_rows = build_query_corpus_now(snapshot_dir, tmp_path / "smoke_query_corpus.csv", per_family=6)
    preflight_rows = build_query_corpus_now(snapshot_dir, tmp_path / "preflight_query_corpus.csv", per_family=20)
    assert len(smoke_rows) == 18
    assert len(preflight_rows) == 60


def test_req_neo4j_009_smoke_runner_stops_after_forced_failure_now(tmp_path: Path) -> None:
    env = os.environ.copy()
    env["KNIGHT_BUS_SKIP_NEO4J_INSTALL"] = "1"
    env["KNIGHT_BUS_FORCE_SMOKE_FAILURE"] = "1"
    env["KNIGHT_BUS_ARTIFACTS_DIR"] = str(tmp_path / "artifacts")
    env["KNIGHT_BUS_REPORTS_DIR"] = str(tmp_path / "reports")
    env["KNIGHT_BUS_ENV_FILE"] = str(tmp_path / ".env.neo4j.local")
    result = subprocess.run(
        [str(REPO_ROOT / "scripts" / "run_neo4j_smoke_ladder.sh")],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "forced smoke failure" in (result.stdout + result.stderr).lower()
    assert not (tmp_path / "artifacts" / "neo4j_preflight_50mb").exists()
