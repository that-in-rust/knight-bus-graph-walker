from __future__ import annotations

import argparse
import hashlib
import json
import platform
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from benchmarks.walk_hopper_v1.common import percentile_value_now
from benchmarks.walk_hopper_v1.query_walk_snapshot import (
    collect_truth_answers_now,
    load_query_corpus_now,
)


QUERY_TEXT_BY_FAMILY = {
    "forward_one": (
        "MATCH (n:Entity {node_id: $node_id})-[:DEPENDS_ON]->(m:Entity) "
        "RETURN m.node_id AS node_id ORDER BY node_id"
    ),
    "reverse_one": (
        "MATCH (n:Entity {node_id: $node_id})<-[:DEPENDS_ON]-(m:Entity) "
        "RETURN m.node_id AS node_id ORDER BY node_id"
    ),
    "reverse_two": (
        "MATCH (n:Entity {node_id: $node_id})<-[:DEPENDS_ON*1..2]-(m:Entity) "
        "RETURN DISTINCT m.node_id AS node_id ORDER BY node_id"
    ),
}


@dataclass
class StartedKnightBusServer:
    process: subprocess.Popen[str]
    driver: Any
    readiness_ms: float
    port: int


class ProcessMemorySampler:
    def __init__(self, process: Any, interval_seconds: float = 0.005) -> None:
        self.process = process
        self.interval_seconds = interval_seconds
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.peak_rss_bytes = 0
        self.peak_virtual_bytes = 0
        self.sample_count = 0

    def start_sampling_process_memory(self) -> None:
        self.sample_process_memory_once()
        self.thread = threading.Thread(
            target=self.sample_process_memory_loop,
            name="compat-memory-sampler",
            daemon=True,
        )
        self.thread.start()

    def stop_sampling_process_memory(self) -> None:
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=2)
        self.sample_process_memory_once()

    def sample_process_memory_loop(self) -> None:
        while not self.stop_event.wait(self.interval_seconds):
            self.sample_process_memory_once()

    def sample_process_memory_once(self) -> None:
        try:
            processes = [self.process, *self.process.children(recursive=True)]
            rss_bytes = 0
            virtual_bytes = 0
            for process in processes:
                memory = process.memory_info()
                rss_bytes += int(memory.rss)
                virtual_bytes += int(memory.vms)
            self.peak_rss_bytes = max(self.peak_rss_bytes, rss_bytes)
            self.peak_virtual_bytes = max(self.peak_virtual_bytes, virtual_bytes)
            self.sample_count += 1
        except Exception:
            return


def query_text_for_family_now(family_name: str) -> str:
    return QUERY_TEXT_BY_FAMILY[family_name]


def hash_ordered_rows_now(rows: list[str]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        encoded = row.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "little"))
        digest.update(encoded)
    return digest.hexdigest()


def compare_result_rows_exact(
    engine_name: str,
    family_name: str,
    node_id: str,
    actual_rows: list[str],
    expected_rows: list[str],
) -> None:
    if actual_rows != expected_rows:
        raise AssertionError(
            f"{engine_name} mismatch for {family_name}/{node_id}: "
            f"actual_hash={hash_ordered_rows_now(actual_rows)} "
            f"expected_hash={hash_ordered_rows_now(expected_rows)} "
            f"actual_count={len(actual_rows)} expected_count={len(expected_rows)}"
        )


def summarize_latency_samples_now(samples: list[float]) -> dict[str, float | int | None]:
    return {
        "sample_count": len(samples),
        "mean_ms": sum(samples) / len(samples) if samples else None,
        "p50_ms": percentile_value_now(samples, 0.50),
        "p95_ms": percentile_value_now(samples, 0.95),
        "p99_ms": percentile_value_now(samples, 0.99),
        "maximum_ms": max(samples) if samples else None,
    }


def reserve_local_bolt_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def load_required_python_modules() -> tuple[Any, Any]:
    try:
        import neo4j
        import psutil
    except ImportError as error:
        raise RuntimeError(
            "install benchmarks/walk_hopper_v1/requirements.txt before running"
        ) from error
    return neo4j, psutil


def start_knight_bus_server(
    binary_path: Path,
    snapshot_path: Path,
    username: str,
    password: str,
    query_timeout_ms: int,
    maximum_result_rows: int,
    neo4j_module: Any,
) -> StartedKnightBusServer:
    port = reserve_local_bolt_port()
    started = time.perf_counter_ns()
    process = subprocess.Popen(
        [
            str(binary_path),
            "--snapshot",
            str(snapshot_path),
            "--bind",
            f"127.0.0.1:{port}",
            "--username",
            username,
            "--password",
            password,
            "--query-timeout-ms",
            str(query_timeout_ms),
            "--max-result-rows",
            str(maximum_result_rows),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    driver = neo4j_module.GraphDatabase.driver(
        f"bolt://127.0.0.1:{port}",
        auth=(username, password),
        encrypted=False,
    )
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if process.poll() is not None:
            _, stderr = process.communicate(timeout=1)
            driver.close()
            raise RuntimeError(f"Knight Bus exited before readiness:\n{stderr}")
        try:
            driver.verify_connectivity()
            readiness_ms = (time.perf_counter_ns() - started) / 1_000_000.0
            return StartedKnightBusServer(process, driver, readiness_ms, port)
        except Exception:
            time.sleep(0.05)
    driver.close()
    process.terminate()
    process.communicate(timeout=5)
    raise TimeoutError("Knight Bus Bolt endpoint did not become ready in 120 seconds")


def stop_knight_bus_server(server: StartedKnightBusServer) -> None:
    server.driver.close()
    if server.process.poll() is None:
        server.process.terminate()
        try:
            server.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.process.kill()
            server.process.wait(timeout=10)
    server.process.communicate(timeout=1)


def resolve_neo4j_server_process(psutil_module: Any) -> Any:
    candidates = []
    for process in psutil_module.process_iter(["pid", "name", "cmdline"]):
        try:
            command = " ".join(process.info.get("cmdline") or []).lower()
            name = (process.info.get("name") or "").lower()
        except (psutil_module.NoSuchProcess, psutil_module.AccessDenied):
            continue
        if "neo4j" in command and "java" in name:
            candidates.append(process)
    if not candidates:
        raise RuntimeError("unable to identify the Neo4j Java server process")
    return max(candidates, key=lambda process: process.memory_info().rss)


def run_driver_query_exact(session: Any, family_name: str, node_id: str) -> tuple[list[str], Any]:
    query_text = query_text_for_family_now(family_name)
    result = session.run(query_text, node_id=node_id)
    rows = [record["node_id"] for record in result]
    summary = result.consume()
    return rows, summary


def collect_plan_operators_now(plan: Any) -> list[str]:
    if isinstance(plan, dict):
        operator = plan["operatorType"]
        children = plan.get("children", [])
    else:
        operator = plan.operator_type
        children = plan.children
    operators = [str(operator)]
    for child in children:
        operators.extend(collect_plan_operators_now(child))
    return operators


def prepare_neo4j_index_now(session: Any, sample_node_id: str) -> dict[str, Any]:
    index_name = "knight_bus_entity_node_id"
    session.run(
        f"CREATE INDEX {index_name} IF NOT EXISTS "
        "FOR (n:Entity) ON (n.node_id)"
    ).consume()
    session.run("CALL db.awaitIndexes(300)").consume()
    index_record = session.run(
        "SHOW INDEXES YIELD name, state, type, entityType, labelsOrTypes, properties "
        "WHERE name = $index_name "
        "RETURN name, state, type, entityType, labelsOrTypes, properties",
        index_name=index_name,
    ).single(strict=True)
    plan_summary = session.run(
        "EXPLAIN " + query_text_for_family_now("forward_one"),
        node_id=sample_node_id,
    ).consume()
    operators = collect_plan_operators_now(plan_summary.plan)
    index_seek_verified = any("IndexSeek" in operator for operator in operators)
    if not index_seek_verified:
        raise RuntimeError(
            "Neo4j comparison query did not plan an index seek: " + ", ".join(operators)
        )
    return {
        "index": dict(index_record),
        "plan_operators": operators,
        "index_seek_verified": index_seek_verified,
    }


def verify_engine_truth_exact(
    engine_name: str,
    session: Any,
    query_rows: list[dict[str, str]],
    truth_answers: dict[tuple[str, str], list[str]],
) -> dict[str, Any]:
    aggregate = hashlib.sha256()
    query_receipts = []
    for row in query_rows:
        family_name = row["family_name"]
        node_id = row["node_id"]
        expected = truth_answers[(family_name, node_id)]
        actual, summary = run_driver_query_exact(session, family_name, node_id)
        compare_result_rows_exact(engine_name, family_name, node_id, actual, expected)
        result_hash = hash_ordered_rows_now(actual)
        identity = f"{family_name}\0{node_id}\0{result_hash}\n".encode("utf-8")
        aggregate.update(identity)
        metadata = getattr(summary, "metadata", {}) or {}
        receipt = metadata.get("knight_bus_receipt")
        if receipt is not None:
            if receipt.get("result_hash") != result_hash:
                raise AssertionError(
                    f"{engine_name} receipt hash mismatch for {family_name}/{node_id}"
                )
            query_receipts.append(receipt)
    return {
        "query_count": len(query_rows),
        "aggregate_result_hash": aggregate.hexdigest(),
        "query_receipts": query_receipts,
    }


def measure_first_query_now(
    engine_name: str,
    session: Any,
    query_row: dict[str, str],
    truth_answers: dict[tuple[str, str], list[str]],
) -> float:
    family_name = query_row["family_name"]
    node_id = query_row["node_id"]
    started = time.perf_counter_ns()
    actual, _summary = run_driver_query_exact(session, family_name, node_id)
    elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000.0
    compare_result_rows_exact(
        engine_name,
        family_name,
        node_id,
        actual,
        truth_answers[(family_name, node_id)],
    )
    return elapsed_ms


def measure_driver_latency_now(
    session: Any,
    query_rows: list[dict[str, str]],
    warmup_passes: int,
    measure_passes: int,
    memory_process: Any,
) -> dict[str, Any]:
    for _ in range(max(warmup_passes, 0)):
        for row in query_rows:
            run_driver_query_exact(session, row["family_name"], row["node_id"])

    sampler = ProcessMemorySampler(memory_process)
    latency_samples = []
    sampler.start_sampling_process_memory()
    try:
        for _ in range(max(measure_passes, 1)):
            for row in query_rows:
                started = time.perf_counter_ns()
                run_driver_query_exact(session, row["family_name"], row["node_id"])
                latency_samples.append(
                    (time.perf_counter_ns() - started) / 1_000_000.0
                )
    finally:
        sampler.stop_sampling_process_memory()
    return {
        "latency": summarize_latency_samples_now(latency_samples),
        "peak_rss_bytes": sampler.peak_rss_bytes,
        "peak_virtual_bytes": sampler.peak_virtual_bytes,
        "memory_sample_count": sampler.sample_count,
        "memory_sampling_interval_ms": sampler.interval_seconds * 1_000.0,
        "rss_scope": "server_process_plus_recursive_children",
    }


def measure_snapshot_mappings_now(process: Any, snapshot_path: Path) -> dict[str, Any]:
    mapped_size = 0
    resident_size = 0
    matched_paths = set()
    try:
        for mapping in process.memory_maps(grouped=False):
            mapping_path = getattr(mapping, "path", "")
            try:
                inside_snapshot = Path(mapping_path).is_relative_to(snapshot_path)
            except (TypeError, ValueError):
                inside_snapshot = False
            if not inside_snapshot:
                continue
            mapped_size += int(getattr(mapping, "size", 0))
            resident_size += int(getattr(mapping, "rss", 0))
            matched_paths.add(mapping_path)
        return {
            "status": "measured",
            "mapped_bytes": mapped_size,
            "resident_bytes": resident_size,
            "mapped_file_count": len(matched_paths),
        }
    except Exception as error:
        return {
            "status": "unavailable",
            "reason": str(error),
            "mapped_bytes": None,
            "resident_bytes": None,
            "mapped_file_count": None,
        }


def hash_file_contents_now(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def hash_snapshot_directory_now(snapshot_path: Path) -> str:
    digest = hashlib.sha256()
    for file_path in sorted(path for path in snapshot_path.iterdir() if path.is_file()):
        relative = file_path.name.encode("utf-8")
        digest.update(len(relative).to_bytes(8, "little"))
        digest.update(relative)
        digest.update(bytes.fromhex(hash_file_contents_now(file_path)))
    return digest.hexdigest()


def snapshot_file_bytes_now(snapshot_path: Path) -> int:
    return sum(path.stat().st_size for path in snapshot_path.iterdir() if path.is_file())


def write_graph_profile_manifest_now(snapshot_path: Path) -> None:
    snapshot_manifest = json.loads(
        (snapshot_path / "manifest.json").read_text(encoding="utf-8")
    )
    profile = {
        "schema_version": 1,
        "profile_version": "knight-bus-neighborhood-walk-v1",
        "node_label": "Entity",
        "start_node_id_property": "node_id",
        "result_node_id_property": "node_id",
        "relationship_type": "DEPENDS_ON",
        "minimum_hops": 1,
        "maximum_hops": 2,
        "node_count": snapshot_manifest["node_count"],
        "relationship_count": snapshot_manifest["edge_count"],
    }
    (snapshot_path / "compatibility-profile.json").write_text(
        json.dumps(profile, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def write_compatibility_reports_now(report_path: Path, receipt: dict[str, Any]) -> None:
    report_path.mkdir(parents=True, exist_ok=True)
    receipt_path = report_path / "compatibility-receipt.json"
    receipt_path.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    knight = receipt["engines"]["knight_bus"]
    neo4j = receipt["engines"]["neo4j"]
    verdict = receipt["release_gate"]["status"]
    summary_lines = [
        "# Cypher Bolt Walk Compatibility Summary",
        "",
        f"Verdict: **{verdict}**",
        "",
        "Authorized scope: `knight-bus-neighborhood-walk-v1`, Neo4j Python driver "
        f"`{receipt['driver_version']}`, direct `bolt://`, read-only auto-commit, "
        f"fixed corpus `{receipt['corpus']['query_count']}` queries.",
        "",
        "| engine | warm p99 ms | peak RSS bytes | result hash |",
        "| --- | ---: | ---: | --- |",
        f"| Knight Bus | {knight['measurement']['latency']['p99_ms']:.6f} | "
        f"{knight['measurement']['peak_rss_bytes']} | "
        f"`{knight['verification']['aggregate_result_hash']}` |",
        f"| Neo4j | {neo4j['measurement']['latency']['p99_ms']:.6f} | "
        f"{neo4j['measurement']['peak_rss_bytes']} | "
        f"`{neo4j['verification']['aggregate_result_hash']}` |",
        "",
        "Cold-open measurements and mmap residency are reported in "
        "`compatibility-receipt.json`; Neo4j server cold boot is outside this runner.",
        "",
    ]
    (report_path / "compatibility-summary.md").write_text(
        "\n".join(summary_lines), encoding="utf-8"
    )


def build_release_gate_now(receipt: dict[str, Any]) -> dict[str, Any]:
    knight = receipt["engines"]["knight_bus"]
    neo4j = receipt["engines"]["neo4j"]
    same_results = (
        knight["verification"]["aggregate_result_hash"]
        == neo4j["verification"]["aggregate_result_hash"]
    )
    knight_p99 = knight["measurement"]["latency"]["p99_ms"]
    neo4j_p99 = neo4j["measurement"]["latency"]["p99_ms"]
    knight_rss = knight["measurement"]["peak_rss_bytes"]
    neo4j_rss = neo4j["measurement"]["peak_rss_bytes"]
    conditions = {
        "ordered_results_match": same_results,
        "neo4j_index_seek_verified": receipt["engines"]["neo4j"]["preflight"][
            "index_seek_verified"
        ],
        "knight_bus_warm_p99_lower": knight_p99 < neo4j_p99,
        "knight_bus_peak_rss_lower": knight_rss < neo4j_rss,
    }
    return {
        "status": "passed" if all(conditions.values()) else "failed",
        "conditions": conditions,
        "latency_ratio_neo4j_over_knight_bus": neo4j_p99 / knight_p99,
        "rss_ratio_neo4j_over_knight_bus": neo4j_rss / knight_rss,
    }


def run_compatibility_benchmark_now(arguments: argparse.Namespace) -> dict[str, Any]:
    neo4j_module, psutil_module = load_required_python_modules()
    dataset_path = arguments.dataset.resolve()
    snapshot_path = arguments.snapshot.resolve()
    corpus_path = arguments.corpus.resolve()
    query_rows = load_query_corpus_now(corpus_path)
    if len(query_rows) != arguments.expected_query_count:
        raise ValueError(
            f"expected {arguments.expected_query_count} query rows, found {len(query_rows)}"
        )
    truth_answers = collect_truth_answers_now(dataset_path, query_rows)
    write_graph_profile_manifest_now(snapshot_path)

    server = start_knight_bus_server(
        arguments.knight_bus_bolt_bin.resolve(),
        snapshot_path,
        arguments.knight_bus_user,
        arguments.knight_bus_password,
        arguments.query_timeout_ms,
        arguments.max_result_rows,
        neo4j_module,
    )
    neo4j_started = time.perf_counter_ns()
    neo4j_driver = neo4j_module.GraphDatabase.driver(
        arguments.neo4j_uri,
        auth=(arguments.neo4j_user, arguments.neo4j_password),
    )
    try:
        neo4j_driver.verify_connectivity()
        neo4j_connect_ms = (time.perf_counter_ns() - neo4j_started) / 1_000_000.0
        knight_process = psutil_module.Process(server.process.pid)
        neo4j_process = resolve_neo4j_server_process(psutil_module)
        knight_server_info = server.driver.get_server_info()
        neo4j_server_info = neo4j_driver.get_server_info()
        with server.driver.session(database="neo4j") as knight_session:
            with neo4j_driver.session(database=arguments.neo4j_database) as neo4j_session:
                neo4j_preflight = prepare_neo4j_index_now(
                    neo4j_session, query_rows[0]["node_id"]
                )
                knight_first_query_ms = measure_first_query_now(
                    "knight_bus", knight_session, query_rows[0], truth_answers
                )
                neo4j_first_query_ms = measure_first_query_now(
                    "neo4j", neo4j_session, query_rows[0], truth_answers
                )
                knight_verification = verify_engine_truth_exact(
                    "knight_bus", knight_session, query_rows, truth_answers
                )
                neo4j_verification = verify_engine_truth_exact(
                    "neo4j", neo4j_session, query_rows, truth_answers
                )
                knight_measurement = measure_driver_latency_now(
                    knight_session,
                    query_rows,
                    arguments.warmup_passes,
                    arguments.measure_passes,
                    knight_process,
                )
                neo4j_measurement = measure_driver_latency_now(
                    neo4j_session,
                    query_rows,
                    arguments.warmup_passes,
                    arguments.measure_passes,
                    neo4j_process,
                )
                neo4j_version_record = neo4j_session.run(
                    "CALL dbms.components() YIELD name, versions "
                    "WHERE name = 'Neo4j Kernel' "
                    "RETURN versions[0] AS version LIMIT 1"
                ).single(strict=True)
                neo4j_version = neo4j_version_record["version"]

        receipt = {
            "schema_version": "knight-bus-cypher-bolt-receipt-v1",
            "created_unix_ns": time.time_ns(),
            "profile_version": "knight-bus-neighborhood-walk-v1",
            "driver_version": neo4j_module.__version__,
            "uri_mode": "direct bolt://",
            "transaction_mode": "read-only auto-commit",
            "environment": {
                "platform": platform.platform(),
                "machine": platform.machine(),
                "python": platform.python_version(),
                "processor": platform.processor(),
            },
            "corpus": {
                "query_count": len(query_rows),
                "query_text_by_family": QUERY_TEXT_BY_FAMILY,
                "corpus_path": str(corpus_path),
                "corpus_sha256": hash_file_contents_now(corpus_path),
                "nodes_bytes": (dataset_path / "nodes.csv").stat().st_size,
                "nodes_sha256": hash_file_contents_now(dataset_path / "nodes.csv"),
                "edges_bytes": (dataset_path / "edges.csv").stat().st_size,
                "edges_sha256": hash_file_contents_now(dataset_path / "edges.csv"),
                "snapshot_bytes": snapshot_file_bytes_now(snapshot_path),
                "snapshot_sha256": hash_snapshot_directory_now(snapshot_path),
            },
            "engines": {
                "knight_bus": {
                    "version": "0.0.2",
                    "server_agent": knight_server_info.agent,
                    "negotiated_bolt_protocol": list(
                        knight_server_info.protocol_version
                    ),
                    "readiness_ms": server.readiness_ms,
                    "first_query_ms": knight_first_query_ms,
                    "verification": knight_verification,
                    "measurement": knight_measurement,
                    "snapshot_mapping": measure_snapshot_mappings_now(
                        knight_process, snapshot_path
                    ),
                    "compatibility_owned_processes": 1
                    + len(knight_process.children(recursive=True)),
                },
                "neo4j": {
                    "version": neo4j_version,
                    "server_agent": neo4j_server_info.agent,
                    "negotiated_bolt_protocol": list(neo4j_server_info.protocol_version),
                    "driver_connect_ms": neo4j_connect_ms,
                    "first_query_ms": neo4j_first_query_ms,
                    "server_cold_boot": "unavailable_not_owned_by_runner",
                    "preflight": neo4j_preflight,
                    "verification": neo4j_verification,
                    "measurement": neo4j_measurement,
                },
            },
        }
        receipt["release_gate"] = build_release_gate_now(receipt)
        write_compatibility_reports_now(arguments.report.resolve(), receipt)
        return receipt
    finally:
        neo4j_driver.close()
        stop_knight_bus_server(server)


def build_argument_parser_now() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--knight-bus-bolt-bin", type=Path, required=True)
    parser.add_argument("--knight-bus-user", default="neo4j")
    parser.add_argument("--knight-bus-password", default="compatibility-proof-password")
    parser.add_argument("--neo4j-uri", required=True)
    parser.add_argument("--neo4j-user", required=True)
    parser.add_argument("--neo4j-password", required=True)
    parser.add_argument("--neo4j-database", default="neo4j")
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--warmup-passes", type=int, default=1)
    parser.add_argument("--measure-passes", type=int, default=1)
    parser.add_argument("--expected-query-count", type=int, default=60)
    parser.add_argument("--query-timeout-ms", type=int, default=30_000)
    parser.add_argument("--max-result-rows", type=int, default=1_000_000)
    return parser


def main() -> None:
    receipt = run_compatibility_benchmark_now(build_argument_parser_now().parse_args())
    print(json.dumps(receipt["release_gate"], indent=2, sort_keys=True))
    if receipt["release_gate"]["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
