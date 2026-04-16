#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
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
    collect_truth_answers_now,
    load_query_corpus_now,
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
    rss_scope: str
    rss_source: str
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
    rss_process: Any | None = None,
    rss_scope: str = "runtime_process_only",
    rss_source: str = "psutil_current_process",
) -> dict[str, Any]:
    psutil = load_psutil_module_now()
    process = rss_process
    if process is None and psutil is not None:
        process = psutil.Process()

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
                        "rss_scope": rss_scope,
                        "rss_source": rss_source,
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
            "rss_scope": rss_scope,
            "rss_source": rss_source,
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
        "rss_scope": rss_scope,
        "rss_source": rss_source,
    }


def default_knight_bus_bin_now() -> Path:
    return Path(__file__).resolve().parents[2] / "target" / "release" / "knight-bus"


def parse_kv_line_output_now(raw_output: str) -> dict[str, Any]:
    payload: dict[str, Any] = {"phase_peaks": []}
    for raw_line in raw_output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("phase_peak "):
            _prefix, phase_name, peak_value = line.split(maxsplit=2)
            payload["phase_peaks"].append(
                {
                    "phase": phase_name,
                    "peak_rss_bytes": int(peak_value),
                }
            )
            continue
        if ": " not in line:
            continue
        key, value = line.split(": ", 1)
        if value.isdigit():
            payload[key] = int(value)
            continue
        try:
            payload[key] = float(value)
            continue
        except ValueError:
            payload[key] = value
    return payload


def failed_measurement_now(engine_name: str, reason: str, cold_run: bool) -> EngineMeasurement:
    rss_scope = "runtime_process_only" if engine_name == "knight_bus_rust" else "server_process_only"
    rss_source = "unavailable"
    return EngineMeasurement(
        engine_name=engine_name,
        status="failed",
        reason=reason,
        open_start_ms=None,
        operation_count=0,
        mean_ms=None,
        p50_ms=None,
        p95_ms=None,
        p99_ms=None,
        rss_bytes=None,
        rss_scope=rss_scope,
        rss_source=rss_source,
        version=None,
        cold_run=cold_run,
    )


def run_knight_bus_verify_now(
    dataset_dir: Path,
    snapshot_dir: Path,
    knight_bus_bin: Path,
) -> dict[str, Any]:
    command = [
        str(knight_bus_bin),
        "verify",
        "--snapshot",
        str(snapshot_dir),
        "--nodes-csv",
        str(dataset_dir / "nodes.csv"),
        "--edges-csv",
        str(dataset_dir / "edges.csv"),
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        reason = (result.stdout + "\n" + result.stderr).strip() or "knight-bus verify failed"
        raise RuntimeError(reason)
    parsed = parse_kv_line_output_now(result.stdout)
    parsed["status"] = parsed.get("verification", "ok")
    parsed["stdout"] = result.stdout
    return parsed


def run_knight_bus_rust_measurement_now(
    snapshot_dir: Path,
    corpus_path: Path,
    report_dir: Path,
    knight_bus_bin: Path,
    cold_run: bool,
) -> EngineMeasurement:
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / "knight_bus_rust_report.json"
    command = [
        str(knight_bus_bin),
        "bench-corpus",
        "--snapshot",
        str(snapshot_dir),
        "--corpus",
        str(corpus_path),
        "--report",
        str(report_path),
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        reason = (result.stdout + "\n" + result.stderr).strip() or "knight_bus_rust failed"
        return failed_measurement_now("knight_bus_rust", reason, cold_run)
    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
        return EngineMeasurement(**payload)
    except Exception as exc:
        return failed_measurement_now("knight_bus_rust", f"failed to load Rust report: {exc}", cold_run)


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


def process_matches_neo4j_now(process: Any) -> bool:
    try:
        process_name = (process.name() or "").lower()
    except Exception:
        process_name = ""
    try:
        process_cmdline = " ".join(process.cmdline()).lower()
    except Exception:
        process_cmdline = ""
    if "neo4j" in process_name or "neo4j" in process_cmdline:
        return True
    return "org.neo4j" in process_cmdline


def resolve_neo4j_server_process_now(bolt_port: int = 7687) -> Any:
    psutil = load_psutil_module_now()
    if psutil is None:  # pragma: no cover - requirements install psutil for real runs
        raise RuntimeError("psutil is required to resolve the Neo4j server process")

    command_rank_pairs: list[tuple[int, Any]] = []
    for process in psutil.process_iter(["name", "cmdline"]):
        if not process_matches_neo4j_now(process):
            continue

        try:
            for connection in process.net_connections(kind="tcp"):
                local_address = getattr(connection, "laddr", None)
                if local_address is None or getattr(local_address, "port", None) != bolt_port:
                    continue
                if getattr(connection, "status", "") == "LISTEN":
                    return process
        except Exception:
            pass

        try:
            process_cmdline = " ".join(process.cmdline()).lower()
        except Exception:
            process_cmdline = ""

        command_rank = 0
        if "org.neo4j.server.neo4jcommunity" in process_cmdline:
            command_rank += 3
        if "org.neo4j.server.startup.neoboot" in process_cmdline:
            command_rank += 2
        if "-dapp.name=neo4j" in process_cmdline:
            command_rank += 1
        command_rank_pairs.append((command_rank, process))

    if command_rank_pairs:
        command_rank_pairs.sort(key=lambda item: item[0], reverse=True)
        return command_rank_pairs[0][1]

    raise RuntimeError(f"failed to resolve Neo4j server process for Bolt port {bolt_port}")


def write_report_bundle_now(
    report_dir: Path,
    dataset_manifest: dict[str, Any],
    snapshot_manifest: dict[str, Any],
    snapshot_dir: Path,
    corpus_path: Path,
    query_rows: list[dict[str, str]],
    measurements: list[EngineMeasurement],
    rust_verify: dict[str, Any] | None = None,
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
        "measurements": [asdict(item) for item in measurements],
        "rust_verify": rust_verify,
    }
    write_json_file_now(report_dir / "report.json", payload)

    summary_lines = [
        "# Knight Bus Rust vs Neo4j",
        "",
        f"- dataset raw bytes: {dataset_manifest.get('actual_raw_bytes')}",
        f"- snapshot bytes: {payload['snapshot_size_bytes']}",
        f"- query rows: {len(query_rows)}",
        f"- query corpus path: {corpus_path}",
        "",
        "| engine | status | open ms | p50 ms | p95 ms | p99 ms | mean ms | rss bytes | rss scope | rss source | reason |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for measurement in measurements:
        summary_lines.append(
            "| {engine_name} | {status} | {open_start_ms} | {p50_ms} | {p95_ms} | {p99_ms} | "
            "{mean_ms} | {rss_bytes} | {rss_scope} | {rss_source} | {reason} |".format(
                engine_name=measurement.engine_name,
                status=measurement.status,
                open_start_ms=measurement.open_start_ms,
                p50_ms=measurement.p50_ms,
                p95_ms=measurement.p95_ms,
                p99_ms=measurement.p99_ms,
                mean_ms=measurement.mean_ms,
                rss_bytes=measurement.rss_bytes,
                rss_scope=measurement.rss_scope,
                rss_source=measurement.rss_source,
                reason=measurement.reason or "",
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


def run_benchmark_now(args: argparse.Namespace) -> dict[str, Any]:
    dataset_dir = args.dataset.resolve()
    snapshot_dir = args.snapshot.resolve()
    report_dir = args.report.resolve()
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
        rust_measurement = run_knight_bus_rust_measurement_now(
            snapshot_dir=snapshot_dir,
            corpus_path=corpus_path,
            report_dir=report_dir,
            knight_bus_bin=args.knight_bus_bin,
            cold_run=args.cold_run,
        )
    except Exception as exc:
        rust_measurement = failed_measurement_now("knight_bus_rust", str(exc), args.cold_run)

    measurements: list[EngineMeasurement] = [rust_measurement]

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
            neo4j_process = resolve_neo4j_server_process_now()
            neo4j_metrics = measure_engine_latency_now(
                engine_name="neo4j",
                query_runner=neo4j_runner,
                query_rows=query_rows,
                warmup_passes=args.warmup_passes,
                measure_passes=args.measure_passes,
                rss_limit_bytes=args.rss_limit_bytes,
                rss_process=neo4j_process,
                rss_scope="server_process_only",
                rss_source="psutil_server_process",
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
                rss_scope=neo4j_metrics["rss_scope"],
                rss_source=neo4j_metrics["rss_source"],
                version=neo4j_version,
                cold_run=args.cold_run,
            )
        finally:
            session.close()
            driver.close()
    except Exception as exc:
        neo4j_measurement = failed_measurement_now("neo4j", str(exc), args.cold_run)
    measurements.append(neo4j_measurement)

    payload = write_report_bundle_now(
        report_dir=report_dir,
        dataset_manifest=dataset_manifest,
        snapshot_manifest=snapshot_manifest,
        snapshot_dir=snapshot_dir,
        corpus_path=corpus_path,
        query_rows=query_rows,
        measurements=measurements,
        rust_verify=rust_verify_summary,
    )
    if any(item.status != "ok" for item in measurements):
        raise SystemExit(1)
    return payload


def build_arg_parser_now() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark Knight Bus Rust against Neo4j.")
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--corpus", type=Path, default=None)
    parser.add_argument("--neo4j-uri", type=str, required=True)
    parser.add_argument("--neo4j-user", type=str, required=True)
    parser.add_argument("--neo4j-password", type=str, required=True)
    parser.add_argument("--neo4j-database", type=str, default=None)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--knight-bus-bin", type=Path, default=default_knight_bus_bin_now())
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
