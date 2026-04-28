from __future__ import annotations

from pathlib import Path

import pytest

from benchmarks.walk_hopper_v1.bench_walk_competitors import resolve_backend_order_now
from benchmarks.walk_hopper_v1.competitor_backends import (
    DEFAULT_BACKEND_ORDER,
    BackendRunContext,
    build_backend_registry_now,
    measurement_payload_now,
)
from benchmarks.walk_hopper_v1.competitor_markdown import write_v003_markdown_bundle_now


def build_stub_context_now(tmp_path: Path) -> BackendRunContext:
    return BackendRunContext(
        dataset_dir=tmp_path,
        query_rows=[],
        truth_answers={},
        work_dir=tmp_path / "work",
        warmup_passes=1,
        measure_passes=1,
        rss_limit_bytes=None,
        cold_run=False,
        neo4j_uri=None,
        neo4j_user=None,
        neo4j_password=None,
        neo4j_database=None,
    )


def test_v003_registry_resolves_all_backends_now() -> None:
    registry = build_backend_registry_now()
    assert tuple(registry.keys()) == DEFAULT_BACKEND_ORDER
    for backend_name, adapter in registry.items():
        assert set(adapter.query_map_now().keys()) == {"forward_one", "reverse_one", "reverse_two"}
        assert adapter.backend_name == backend_name


def test_v003_unknown_backend_fails_clearly_now() -> None:
    registry = build_backend_registry_now()
    with pytest.raises(ValueError, match="unknown backend"):
        resolve_backend_order_now(["neo4j", "not-real"], registry)


def test_v003_unsupported_backend_emits_shared_schema_now(tmp_path: Path) -> None:
    registry = build_backend_registry_now()
    measurement = registry["memgraph"].run_measurement_now(build_stub_context_now(tmp_path))
    payload = measurement_payload_now(measurement)
    assert payload["backend_name"] == "memgraph"
    assert payload["status"] == "unsupported"
    assert payload["parity_checked"] is False
    assert payload["parity_passed"] is False
    assert set(payload.keys()) == {
        "backend_name",
        "status",
        "reason",
        "version",
        "import_duration_ms",
        "open_start_ms",
        "operation_count",
        "mean_ms",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "rss_bytes",
        "rss_scope",
        "rss_source",
        "parity_checked",
        "parity_passed",
        "query_language",
        "source_note",
    }


def test_v003_markdown_includes_all_status_rows_now(tmp_path: Path) -> None:
    summary_payload = {
        "dataset_manifest": {
            "graph_model": "code_sparse",
            "actual_raw_bytes": 2_147_483_648,
            "node_count": 100,
            "edge_count": 900,
        },
        "environment": {
            "platform": "macOS-test",
            "machine": "arm64",
            "ram_total_bytes": 17_179_869_184,
        },
        "snapshot_size_bytes": 123_456_789,
        "query_corpus_size": 60,
        "baseline": {
            "engine_name": "knight_bus_rust",
            "status": "ok",
            "reason": None,
            "open_start_ms": 10.0,
            "operation_count": 180,
            "mean_ms": 0.01,
            "p50_ms": 0.005,
            "p95_ms": 0.02,
            "p99_ms": 0.03,
            "rss_bytes": 123_000_000,
            "rss_scope": "runtime_process_only",
            "rss_source": "getrusage_self",
            "version": "snapshot-v2",
            "cold_run": False,
        },
        "competitors": [
            {
                "backend_name": "neo4j",
                "status": "ok",
                "reason": None,
                "version": "2026.03.1",
                "import_duration_ms": 100.0,
                "open_start_ms": 20.0,
                "operation_count": 180,
                "mean_ms": 1.0,
                "p50_ms": 0.9,
                "p95_ms": 1.1,
                "p99_ms": 1.2,
                "rss_bytes": 1_000_000_000,
                "rss_scope": "server_process_only",
                "rss_source": "psutil_server_process",
                "parity_checked": True,
                "parity_passed": True,
                "query_language": "cypher",
                "source_note": None,
            },
            {
                "backend_name": "memgraph",
                "status": "degraded",
                "reason": "rss cap exceeded",
                "version": "abc1234",
                "import_duration_ms": 110.0,
                "open_start_ms": 25.0,
                "operation_count": 90,
                "mean_ms": None,
                "p50_ms": None,
                "p95_ms": None,
                "p99_ms": None,
                "rss_bytes": 2_000_000_000,
                "rss_scope": "server_process_only",
                "rss_source": "sampled",
                "parity_checked": True,
                "parity_passed": True,
                "query_language": "cypher",
                "source_note": None,
            },
            {
                "backend_name": "kuzu",
                "status": "failed",
                "reason": "import failed",
                "version": "def5678",
                "import_duration_ms": None,
                "open_start_ms": None,
                "operation_count": 0,
                "mean_ms": None,
                "p50_ms": None,
                "p95_ms": None,
                "p99_ms": None,
                "rss_bytes": None,
                "rss_scope": "unavailable",
                "rss_source": "unavailable",
                "parity_checked": True,
                "parity_passed": False,
                "query_language": "cypher",
                "source_note": None,
            },
            {
                "backend_name": "dgraph",
                "status": "unsupported",
                "reason": "adapter placeholder",
                "version": "ghi9999",
                "import_duration_ms": None,
                "open_start_ms": None,
                "operation_count": 0,
                "mean_ms": None,
                "p50_ms": None,
                "p95_ms": None,
                "p99_ms": None,
                "rss_bytes": None,
                "rss_scope": "unavailable",
                "rss_source": "unavailable",
                "parity_checked": False,
                "parity_passed": False,
                "query_language": "dql",
                "source_note": "placeholder",
            },
        ],
    }

    output_dir = tmp_path / "v003-research"
    write_v003_markdown_bundle_now(summary_payload, output_dir)

    readme_text = (output_dir / "README.md").read_text(encoding="utf-8")
    benchmark_text = (output_dir / "competitor-2gb-benchmark.md").read_text(encoding="utf-8")

    assert "What This Matrix Proves" in readme_text
    assert "Current Backend Status" in readme_text
    assert "`unsupported`" in readme_text
    assert "| Backend | Query layer | Status | Same answers | Import | Open | p50 | p95 | p99 | Runtime RSS | Version | Reason |" in benchmark_text
    assert "| neo4j | cypher | ok | yes |" in benchmark_text
    assert "| memgraph | cypher | degraded | yes |" in benchmark_text
    assert "| kuzu | cypher | failed | no |" in benchmark_text
    assert "| dgraph | dql | unsupported | not checked |" in benchmark_text
