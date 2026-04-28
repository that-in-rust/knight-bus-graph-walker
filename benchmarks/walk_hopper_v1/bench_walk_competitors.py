#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    import sys

    sys.path.append(str(Path(__file__).resolve().parents[2]))

from benchmarks.walk_hopper_v1.bench_walk_vs_neo4j import (
    EngineMeasurement,
    default_knight_bus_bin_now,
    failed_measurement_now,
    run_knight_bus_rust_measurement_now,
    run_knight_bus_verify_now,
)
from benchmarks.walk_hopper_v1.common import (
    collect_directory_size_now,
    collect_runtime_env_now,
    read_json_file_now,
    write_json_file_now,
)
from benchmarks.walk_hopper_v1.competitor_backends import (
    DEFAULT_BACKEND_ORDER,
    BackendRunContext,
    CompetitorMeasurement,
    build_backend_registry_now,
    measurement_payload_now,
)
from benchmarks.walk_hopper_v1.query_walk_snapshot import (
    collect_truth_answers_now,
    load_query_corpus_now,
)


def resolve_backend_order_now(
    requested_backends: list[str] | None,
    registry: dict[str, Any],
) -> list[str]:
    if not requested_backends:
        return list(DEFAULT_BACKEND_ORDER)
    missing_names = [name for name in requested_backends if name not in registry]
    if missing_names:
        raise ValueError(f"unknown backend(s): {', '.join(missing_names)}")
    ordered_names: list[str] = []
    seen_names: set[str] = set()
    for backend_name in requested_backends:
        if backend_name in seen_names:
            continue
        seen_names.add(backend_name)
        ordered_names.append(backend_name)
    return ordered_names


def backend_context_now(
    args: argparse.Namespace,
    dataset_dir: Path,
    query_rows: list[dict[str, str]],
    truth_answers: dict[tuple[str, str], list[str]],
    work_dir: Path,
) -> BackendRunContext:
    return BackendRunContext(
        dataset_dir=dataset_dir,
        query_rows=query_rows,
        truth_answers=truth_answers,
        work_dir=work_dir,
        warmup_passes=args.warmup_passes,
        measure_passes=args.measure_passes,
        rss_limit_bytes=args.rss_limit_bytes,
        cold_run=args.cold_run,
        neo4j_uri=args.neo4j_uri,
        neo4j_user=args.neo4j_user,
        neo4j_password=args.neo4j_password,
        neo4j_database=args.neo4j_database,
    )


def write_backend_report_now(
    backend_dir: Path,
    backend_name: str,
    query_map: dict[str, str],
    measurement: CompetitorMeasurement,
) -> None:
    backend_dir.mkdir(parents=True, exist_ok=True)
    payload = measurement_payload_now(measurement)
    payload["query_map"] = query_map
    payload["backend_name"] = backend_name
    write_json_file_now(backend_dir / "report.json", payload)


def write_matrix_report_now(
    report_dir: Path,
    dataset_manifest: dict[str, Any],
    snapshot_manifest: dict[str, Any],
    snapshot_dir: Path,
    corpus_path: Path,
    query_rows: list[dict[str, str]],
    baseline: EngineMeasurement,
    rust_verify: dict[str, Any] | None,
    competitors: list[CompetitorMeasurement],
    backend_order: list[str],
) -> dict[str, Any]:
    report_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "dataset_manifest": dataset_manifest,
        "snapshot_manifest": snapshot_manifest,
        "snapshot_size_bytes": collect_directory_size_now(snapshot_dir),
        "snapshot_dir": str(snapshot_dir),
        "query_corpus_path": str(corpus_path),
        "query_corpus_size": len(query_rows),
        "environment": collect_runtime_env_now(),
        "baseline": asdict(baseline),
        "rust_verify": rust_verify,
        "competitors": [measurement_payload_now(item) for item in competitors],
        "backend_order": backend_order,
    }
    write_json_file_now(report_dir / "summary.json", payload)

    summary_lines = [
        "# v003 2 GB Competitor Matrix",
        "",
        f"- dataset raw bytes: {dataset_manifest.get('actual_raw_bytes')}",
        f"- dataset nodes: {dataset_manifest.get('node_count')}",
        f"- dataset edges: {dataset_manifest.get('edge_count')}",
        f"- snapshot bytes: {payload['snapshot_size_bytes']}",
        f"- query rows: {len(query_rows)}",
        f"- query corpus path: {corpus_path}",
        "",
        "## Knight Bus Baseline",
        "",
        "| engine | status | open ms | p50 ms | p95 ms | p99 ms | mean ms | rss bytes | version | reason |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        (
            f"| {baseline.engine_name} | {baseline.status} | {baseline.open_start_ms} | "
            f"{baseline.p50_ms} | {baseline.p95_ms} | {baseline.p99_ms} | "
            f"{baseline.mean_ms} | {baseline.rss_bytes} | {baseline.version} | "
            f"{baseline.reason or ''} |"
        ),
        "",
        "## Competitors",
        "",
        "| backend | status | parity checked | parity passed | import ms | open ms | p50 ms | p95 ms | p99 ms | rss bytes | query language | version | reason |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for competitor in competitors:
        summary_lines.append(
            "| {backend_name} | {status} | {parity_checked} | {parity_passed} | {import_duration_ms} | "
            "{open_start_ms} | {p50_ms} | {p95_ms} | {p99_ms} | {rss_bytes} | "
            "{query_language} | {version} | {reason} |".format(
                backend_name=competitor.backend_name,
                status=competitor.status,
                parity_checked=competitor.parity_checked,
                parity_passed=competitor.parity_passed,
                import_duration_ms=competitor.import_duration_ms,
                open_start_ms=competitor.open_start_ms,
                p50_ms=competitor.p50_ms,
                p95_ms=competitor.p95_ms,
                p99_ms=competitor.p99_ms,
                rss_bytes=competitor.rss_bytes,
                query_language=competitor.query_language,
                version=competitor.version or "",
                reason=competitor.reason or "",
            )
        )
    if rust_verify is not None:
        summary_lines.extend(
            [
                "",
                "## Rust Verify",
                "",
                f"- status: {rust_verify.get('status')}",
                f"- checked_nodes: {rust_verify.get('checked_nodes')}",
                f"- checked_forward_edges: {rust_verify.get('checked_forward_edges')}",
                f"- checked_reverse_edges: {rust_verify.get('checked_reverse_edges')}",
                f"- peak_rss_bytes: {rust_verify.get('peak_rss_bytes')}",
                f"- peak_rss_source: {rust_verify.get('peak_rss_source')}",
            ]
        )
    (report_dir / "summary.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    return payload


def run_competitor_matrix_now(args: argparse.Namespace) -> dict[str, Any]:
    dataset_dir = args.dataset.resolve()
    snapshot_dir = args.snapshot.resolve()
    report_dir = args.report_dir.resolve()
    dataset_manifest = read_json_file_now(dataset_dir / "manifest.json")
    snapshot_manifest = read_json_file_now(snapshot_dir / "manifest.json")
    corpus_path = (args.corpus or (dataset_dir / "query_corpus.csv")).resolve()
    if not corpus_path.exists():
        raise FileNotFoundError(f"expected fixed corpus file at {corpus_path}")

    query_rows = load_query_corpus_now(corpus_path)
    truth_answers = collect_truth_answers_now(dataset_dir, query_rows)

    rust_verify_summary: dict[str, Any] | None = None
    try:
        rust_verify_summary = run_knight_bus_verify_now(
            dataset_dir=dataset_dir,
            snapshot_dir=snapshot_dir,
            knight_bus_bin=args.knight_bus_bin,
        )
        baseline = run_knight_bus_rust_measurement_now(
            snapshot_dir=snapshot_dir,
            corpus_path=corpus_path,
            report_dir=report_dir / "baseline",
            knight_bus_bin=args.knight_bus_bin,
            cold_run=args.cold_run,
        )
    except Exception as exc:
        baseline = failed_measurement_now("knight_bus_rust", str(exc), args.cold_run)

    registry = build_backend_registry_now()
    backend_order = resolve_backend_order_now(args.backends, registry)
    competitors: list[CompetitorMeasurement] = []
    for backend_name in backend_order:
        adapter = registry[backend_name]
        backend_dir = report_dir / backend_name
        context = backend_context_now(
            args=args,
            dataset_dir=dataset_dir,
            query_rows=query_rows,
            truth_answers=truth_answers,
            work_dir=backend_dir,
        )
        measurement = adapter.run_measurement_now(context)
        write_backend_report_now(
            backend_dir=backend_dir,
            backend_name=backend_name,
            query_map=adapter.query_map_now(),
            measurement=measurement,
        )
        competitors.append(measurement)

    payload = write_matrix_report_now(
        report_dir=report_dir,
        dataset_manifest=dataset_manifest,
        snapshot_manifest=snapshot_manifest,
        snapshot_dir=snapshot_dir,
        corpus_path=corpus_path,
        query_rows=query_rows,
        baseline=baseline,
        rust_verify=rust_verify_summary,
        competitors=competitors,
        backend_order=backend_order,
    )
    return payload


def build_arg_parser_now() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark Knight Bus against a 2 GB competitor matrix.")
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--corpus", type=Path, default=None)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--backends", nargs="*", default=None)
    parser.add_argument("--neo4j-uri", type=str, default=None)
    parser.add_argument("--neo4j-user", type=str, default=None)
    parser.add_argument("--neo4j-password", type=str, default=None)
    parser.add_argument("--neo4j-database", type=str, default=None)
    parser.add_argument("--knight-bus-bin", type=Path, default=default_knight_bus_bin_now())
    parser.add_argument("--warmup-passes", type=int, default=1)
    parser.add_argument("--measure-passes", type=int, default=3)
    parser.add_argument("--rss-limit-bytes", type=int, default=None)
    parser.add_argument("--cold-run", action="store_true")
    return parser


def main() -> None:
    args = build_arg_parser_now().parse_args()
    payload = run_competitor_matrix_now(args)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
