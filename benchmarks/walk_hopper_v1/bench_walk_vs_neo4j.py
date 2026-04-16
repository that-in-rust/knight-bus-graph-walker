#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

if __package__ in (None, ""):
    import sys

    sys.path.append(str(Path(__file__).resolve().parents[2]))

from benchmarks.walk_hopper_v1.common import (
    collect_directory_size_now,
    collect_runtime_env_now,
    percentile_value_now,
    read_json_file_now,
    write_json_file_now,
)
from benchmarks.walk_hopper_v1.query_walk_snapshot import (
    build_query_corpus_now,
    collect_truth_answers_now,
    load_query_corpus_now,
    load_snapshot_graph_now,
    query_snapshot_family_now,
)


def load_psutil_module_now():
    try:
        import psutil  # type: ignore
    except ImportError:
        return None
    return psutil


def load_neo4j_module_now():
    try:
        from neo4j import GraphDatabase  # type: ignore
    except ImportError as exc:  # pragma: no cover - exercised only without neo4j installed
        raise RuntimeError("neo4j Python driver is required to benchmark Neo4j") from exc
    return GraphDatabase


@dataclass(frozen=True)
class EngineMeasurement:
    engine_name: str
    status: str
    reason: str | None
    open_start_ms: float | None
    operation_count: int
    mean_ms: float | None
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    rss_bytes: int | None
    version: str | None
    cold_run: bool


def corpus_identity_now(row: dict[str, str]) -> tuple[str, str]:
    return row["family_name"], row["node_id"]


def validate_engine_parity_now(
    engine_name: str,
    query_runner: Callable[[str, str], list[str]],
    query_rows: list[dict[str, str]],
    truth_answers: dict[tuple[str, str], list[str]],
) -> None:
    for row in query_rows:
        row_key = corpus_identity_now(row)
        actual_answers = query_runner(row["family_name"], row["node_id"])
        expected_answers = truth_answers[row_key]
        if actual_answers != expected_answers:
            raise ValueError(
                f"{engine_name} parity mismatch for {row['family_name']} {row['node_id']}: "
                f"expected {expected_answers}, got {actual_answers}"
            )


def measure_engine_latency_now(
    engine_name: str,
    query_runner: Callable[[str, str], list[str]],
    query_rows: list[dict[str, str]],
    warmup_passes: int = 1,
    measure_passes: int = 3,
    rss_limit_bytes: int | None = None,
) -> dict[str, Any]:
    psutil = load_psutil_module_now()
    process = psutil.Process() if psutil is not None else None
    latency_samples: list[float] = []
    max_rss = process.memory_info().rss if process is not None else None

    try:
        for _ in range(max(warmup_passes, 0)):
            for row in query_rows:
                query_runner(row["family_name"], row["node_id"])
        for _ in range(max(measure_passes, 1)):
            for row in query_rows:
                started = time.perf_counter_ns()
                query_runner(row["family_name"], row["node_id"])
                finished = time.perf_counter_ns()
                latency_samples.append((finished - started) / 1_000_000.0)
                if process is not None:
                    max_rss = max(max_rss or 0, process.memory_info().rss)
                if rss_limit_bytes is not None and max_rss is not None and max_rss > rss_limit_bytes:
                    return {
                        "status": "degraded",
                        "reason": f"{engine_name} exceeded rss_limit_bytes={rss_limit_bytes}",
                        "operation_count": len(latency_samples),
                        "mean_ms": None,
                        "p50_ms": None,
                        "p95_ms": None,
                        "p99_ms": None,
                        "rss_bytes": max_rss,
                    }
    except Exception as exc:
        return {
            "status": "failed",
            "reason": str(exc),
            "operation_count": len(latency_samples),
            "mean_ms": None,
            "p50_ms": None,
            "p95_ms": None,
            "p99_ms": None,
            "rss_bytes": max_rss,
        }

    mean_ms = (sum(latency_samples) / len(latency_samples)) if latency_samples else None
    return {
        "status": "ok",
        "reason": None,
        "operation_count": len(latency_samples),
        "mean_ms": mean_ms,
        "p50_ms": percentile_value_now(latency_samples, 0.50),
        "p95_ms": percentile_value_now(latency_samples, 0.95),
        "p99_ms": percentile_value_now(latency_samples, 0.99),
        "rss_bytes": max_rss,
    }


def open_walk_engine_now(snapshot_dir: Path) -> tuple[Any, float, str]:
    started = time.perf_counter_ns()
    snapshot_graph = load_snapshot_graph_now(snapshot_dir)
    open_start_ms = (time.perf_counter_ns() - started) / 1_000_000.0
    return snapshot_graph, open_start_ms, f"snapshot-v{snapshot_graph.manifest['version']}"


def open_neo4j_engine_now(
    uri: str,
    user: str,
    password: str,
    database: str | None,
) -> tuple[Any, Any, float, str]:
    GraphDatabase = load_neo4j_module_now()
    started = time.perf_counter_ns()
    driver = GraphDatabase.driver(uri, auth=(user, password))
    driver.verify_connectivity()
    session = driver.session(database=database) if database else driver.session()
    open_start_ms = (time.perf_counter_ns() - started) / 1_000_000.0
    try:
        result = session.run(
            "CALL dbms.components() YIELD versions RETURN versions[0] AS version LIMIT 1"
        ).single()
        version = result["version"] if result is not None else "unknown"
    except Exception:
        version = "unknown"
    return driver, session, open_start_ms, version


def build_neo4j_runner_now(session: Any) -> Callable[[str, str], list[str]]:
    query_map = {
        "forward_one": (
            "MATCH (n {node_id: $node_id})-[:DEPENDS_ON]->(m) "
            "RETURN m.node_id AS node_id ORDER BY node_id"
        ),
        "reverse_one": (
            "MATCH (n {node_id: $node_id})<-[:DEPENDS_ON]-(m) "
            "RETURN m.node_id AS node_id ORDER BY node_id"
        ),
        "reverse_two": (
            "MATCH (n {node_id: $node_id})<-[:DEPENDS_ON*1..2]-(m) "
            "RETURN DISTINCT m.node_id AS node_id ORDER BY node_id"
        ),
    }

    def run_query_now(family_name: str, node_id: str) -> list[str]:
        query_text = query_map[family_name]
        result = session.run(query_text, node_id=node_id)
        return [record["node_id"] for record in result]

    return run_query_now


def write_report_bundle_now(
    report_dir: Path,
    dataset_manifest: dict[str, Any],
    snapshot_manifest: dict[str, Any],
    query_rows: list[dict[str, str]],
    measurements: list[EngineMeasurement],
) -> dict[str, Any]:
    report_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "dataset_manifest": dataset_manifest,
        "snapshot_manifest": snapshot_manifest,
        "query_corpus_size": len(query_rows),
        "environment": collect_runtime_env_now(),
        "measurements": [asdict(item) for item in measurements],
    }
    write_json_file_now(report_dir / "report.json", payload)

    summary_lines = [
        "# WALK Hopper vs Neo4j",
        "",
        f"- dataset raw bytes: {dataset_manifest.get('actual_raw_bytes')}",
        f"- snapshot bytes: {snapshot_manifest.get('snapshot_bytes')}",
        f"- query rows: {len(query_rows)}",
        "",
        "| engine | status | open ms | p50 ms | p95 ms | p99 ms | mean ms | rss bytes | reason |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for measurement in measurements:
        summary_lines.append(
            "| {engine_name} | {status} | {open_start_ms} | {p50_ms} | {p95_ms} | {p99_ms} | "
            "{mean_ms} | {rss_bytes} | {reason} |".format(
                engine_name=measurement.engine_name,
                status=measurement.status,
                open_start_ms=measurement.open_start_ms,
                p50_ms=measurement.p50_ms,
                p95_ms=measurement.p95_ms,
                p99_ms=measurement.p99_ms,
                mean_ms=measurement.mean_ms,
                rss_bytes=measurement.rss_bytes,
                reason=measurement.reason or "",
            )
        )
    (report_dir / "summary.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    return payload


def run_benchmark_now(args: argparse.Namespace) -> dict[str, Any]:
    dataset_dir = args.dataset
    snapshot_dir = args.snapshot
    report_dir = args.report
    dataset_manifest = read_json_file_now(dataset_dir / "manifest.json")
    snapshot_manifest = read_json_file_now(snapshot_dir / "manifest.json")
    corpus_path = dataset_dir / "query_corpus.csv"
    if not corpus_path.exists():
        build_query_corpus_now(snapshot_dir=snapshot_dir, output_path=corpus_path, per_family=args.per_family)
    query_rows = load_query_corpus_now(corpus_path)
    truth_answers = collect_truth_answers_now(dataset_dir, query_rows)

    measurements: list[EngineMeasurement] = []

    walk_graph, walk_open_ms, walk_version = open_walk_engine_now(snapshot_dir)
    walk_runner = lambda family_name, node_id: query_snapshot_family_now(walk_graph, family_name, node_id)
    validate_engine_parity_now("walk_hopper", walk_runner, query_rows, truth_answers)
    walk_metrics = measure_engine_latency_now(
        engine_name="walk_hopper",
        query_runner=walk_runner,
        query_rows=query_rows,
        warmup_passes=args.warmup_passes,
        measure_passes=args.measure_passes,
        rss_limit_bytes=args.rss_limit_bytes,
    )
    measurements.append(
        EngineMeasurement(
            engine_name="walk_hopper",
            status=walk_metrics["status"],
            reason=walk_metrics["reason"],
            open_start_ms=walk_open_ms,
            operation_count=walk_metrics["operation_count"],
            mean_ms=walk_metrics["mean_ms"],
            p50_ms=walk_metrics["p50_ms"],
            p95_ms=walk_metrics["p95_ms"],
            p99_ms=walk_metrics["p99_ms"],
            rss_bytes=walk_metrics["rss_bytes"],
            version=walk_version,
            cold_run=args.cold_run,
        )
    )

    neo4j_measurement: EngineMeasurement
    try:
        driver, session, neo4j_open_ms, neo4j_version = open_neo4j_engine_now(
            uri=args.neo4j_uri,
            user=args.neo4j_user,
            password=args.neo4j_password,
            database=args.neo4j_database,
        )
        try:
            neo4j_runner = build_neo4j_runner_now(session)
            validate_engine_parity_now("neo4j", neo4j_runner, query_rows, truth_answers)
            neo4j_metrics = measure_engine_latency_now(
                engine_name="neo4j",
                query_runner=neo4j_runner,
                query_rows=query_rows,
                warmup_passes=args.warmup_passes,
                measure_passes=args.measure_passes,
                rss_limit_bytes=args.rss_limit_bytes,
            )
            neo4j_measurement = EngineMeasurement(
                engine_name="neo4j",
                status=neo4j_metrics["status"],
                reason=neo4j_metrics["reason"],
                open_start_ms=neo4j_open_ms,
                operation_count=neo4j_metrics["operation_count"],
                mean_ms=neo4j_metrics["mean_ms"],
                p50_ms=neo4j_metrics["p50_ms"],
                p95_ms=neo4j_metrics["p95_ms"],
                p99_ms=neo4j_metrics["p99_ms"],
                rss_bytes=neo4j_metrics["rss_bytes"],
                version=neo4j_version,
                cold_run=args.cold_run,
            )
        finally:
            session.close()
            driver.close()
    except Exception as exc:
        neo4j_measurement = EngineMeasurement(
            engine_name="neo4j",
            status="failed",
            reason=str(exc),
            open_start_ms=None,
            operation_count=0,
            mean_ms=None,
            p50_ms=None,
            p95_ms=None,
            p99_ms=None,
            rss_bytes=None,
            version=None,
            cold_run=args.cold_run,
        )
    measurements.append(neo4j_measurement)

    payload = write_report_bundle_now(
        report_dir=report_dir,
        dataset_manifest=dataset_manifest,
        snapshot_manifest=snapshot_manifest,
        query_rows=query_rows,
        measurements=measurements,
    )
    if any(item.status != "ok" for item in measurements):
        raise SystemExit(1)
    return payload


def build_arg_parser_now() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark WALK Hopper against Neo4j.")
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--neo4j-uri", type=str, required=True)
    parser.add_argument("--neo4j-user", type=str, required=True)
    parser.add_argument("--neo4j-password", type=str, required=True)
    parser.add_argument("--neo4j-database", type=str, default=None)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--per-family", type=int, default=200)
    parser.add_argument("--warmup-passes", type=int, default=1)
    parser.add_argument("--measure-passes", type=int, default=3)
    parser.add_argument("--rss-limit-bytes", type=int, default=None)
    parser.add_argument("--cold-run", action="store_true")
    return parser


def main() -> None:
    args = build_arg_parser_now().parse_args()
    payload = run_benchmark_now(args)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
