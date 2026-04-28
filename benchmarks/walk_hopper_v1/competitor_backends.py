from __future__ import annotations

import socket
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from benchmarks.walk_hopper_v1.bench_walk_vs_neo4j import (
    build_neo4j_runner_now,
    measure_engine_latency_now,
    open_neo4j_engine_now,
    resolve_neo4j_server_process_now,
    validate_engine_parity_now,
)
from benchmarks.walk_hopper_v1.export_neo4j_import import export_neo4j_import_files


QUERY_FAMILY_ORDER = ("forward_one", "reverse_one", "reverse_two")
DEFAULT_BACKEND_ORDER = (
    "neo4j",
    "memgraph",
    "kuzu",
    "falkordb",
    "hugegraph",
    "apache-age",
    "janusgraph",
    "dgraph",
)

CYTHERISH_QUERY_MAP = {
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

GREMLIN_QUERY_MAP = {
    "forward_one": "g.V().has('node_id', node_id).out('DEPENDS_ON').values('node_id').dedup().order()",
    "reverse_one": "g.V().has('node_id', node_id).in('DEPENDS_ON').values('node_id').dedup().order()",
    "reverse_two": "g.V().has('node_id', node_id).repeat(__.in('DEPENDS_ON')).times(2).values('node_id').dedup().order()",
}

DGRAPH_QUERY_MAP = {
    "forward_one": "{ q(func: eq(node_id, $node_id)) { forward_one: DEPENDS_ON { node_id } } }",
    "reverse_one": "{ q(func: eq(node_id, $node_id)) { reverse_one: ~DEPENDS_ON { node_id } } }",
    "reverse_two": "{ q(func: eq(node_id, $node_id)) { reverse_two: ~DEPENDS_ON @recurse(depth: 2, loop: false) { node_id ~DEPENDS_ON } } }",
}

REFERENCE_REPO_ROOT = Path(__file__).resolve().parents[2] / "ref-repo-folder" / "graph-walk-competitors"
REFERENCE_REPO_DIRS = {
    "neo4j": "neo4j",
    "memgraph": "memgraph",
    "kuzu": "kuzu",
    "falkordb": "falkordb",
    "hugegraph": "hugegraph",
    "apache-age": "apache-age",
    "janusgraph": "janusgraph",
    "dgraph": "dgraph",
}


@dataclass(frozen=True)
class CompetitorMeasurement:
    backend_name: str
    status: str
    reason: str | None
    version: str | None
    import_duration_ms: float | None
    open_start_ms: float | None
    operation_count: int
    mean_ms: float | None
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    rss_bytes: int | None
    rss_scope: str
    rss_source: str
    parity_checked: bool
    parity_passed: bool
    query_language: str
    source_note: str | None


@dataclass(frozen=True)
class BackendRunContext:
    dataset_dir: Path
    query_rows: list[dict[str, str]]
    truth_answers: dict[tuple[str, str], list[str]]
    work_dir: Path
    warmup_passes: int
    measure_passes: int
    rss_limit_bytes: int | None
    cold_run: bool
    neo4j_uri: str | None
    neo4j_user: str | None
    neo4j_password: str | None
    neo4j_database: str | None


def measurement_payload_now(measurement: CompetitorMeasurement) -> dict[str, Any]:
    return asdict(measurement)


def reference_repo_version_now(backend_name: str) -> str | None:
    repo_dir_name = REFERENCE_REPO_DIRS.get(backend_name)
    if repo_dir_name is None:
        return None
    repo_dir = REFERENCE_REPO_ROOT / repo_dir_name
    if not repo_dir.exists():
        return None
    result = subprocess.run(
        ["git", "-C", str(repo_dir), "rev-parse", "--short", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def missing_command_reason_now(command_name: str) -> str:
    return f"required command not found: {command_name}"


def unsupported_measurement_now(
    backend_name: str,
    query_language: str,
    reason: str,
    version: str | None = None,
    source_note: str | None = None,
) -> CompetitorMeasurement:
    return CompetitorMeasurement(
        backend_name=backend_name,
        status="unsupported",
        reason=reason,
        version=version,
        import_duration_ms=None,
        open_start_ms=None,
        operation_count=0,
        mean_ms=None,
        p50_ms=None,
        p95_ms=None,
        p99_ms=None,
        rss_bytes=None,
        rss_scope="unavailable",
        rss_source="unavailable",
        parity_checked=False,
        parity_passed=False,
        query_language=query_language,
        source_note=source_note,
    )


def command_exists_now(command_name: str) -> bool:
    result = subprocess.run(
        ["bash", "-lc", f"command -v {command_name} >/dev/null 2>&1"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def find_neo4j_admin_now() -> str | None:
    formula_prefix = subprocess.run(
        ["/opt/homebrew/bin/brew", "--prefix", "neo4j"],
        capture_output=True,
        text=True,
        check=False,
    )
    if formula_prefix.returncode == 0:
        candidate = Path(formula_prefix.stdout.strip()) / "bin" / "neo4j-admin"
        if candidate.exists():
            return str(candidate)
    result = subprocess.run(
        ["bash", "-lc", "command -v neo4j-admin || true"],
        capture_output=True,
        text=True,
        check=False,
    )
    path_value = result.stdout.strip()
    return path_value or None


def bootout_neo4j_service_now() -> None:
    launch_agent = Path.home() / "Library" / "LaunchAgents" / "homebrew.mxcl.neo4j.plist"
    if not launch_agent.exists():
        return
    user_id = subprocess.run(
        ["id", "-u"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    subprocess.run(
        ["launchctl", "bootout", f"gui/{user_id}", str(launch_agent)],
        capture_output=True,
        text=True,
        check=False,
    )


def wait_for_port_state_now(port: int, should_listen: bool, timeout_seconds: int) -> None:
    started = time.time()
    while True:
        sock = socket.socket()
        sock.settimeout(1.0)
        try:
            sock.connect(("127.0.0.1", port))
            listening = True
        except OSError:
            listening = False
        finally:
            sock.close()
        if listening == should_listen:
            return
        if time.time() - started >= timeout_seconds:
            desired_state = "ready" if should_listen else "stopped"
            raise RuntimeError(f"port {port} did not become {desired_state} within {timeout_seconds}s")
        time.sleep(2.0)


class BenchmarkBackendAdapter:
    backend_name: str
    query_language: str

    def prepare_import_inputs(self, context: BackendRunContext) -> dict[str, Any]:
        return {}

    def import_dataset(self, context: BackendRunContext, prepared_inputs: dict[str, Any]) -> float | None:
        return None

    def open_backend(self, context: BackendRunContext, prepared_inputs: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def run_query_family(self, backend_state: dict[str, Any], family_name: str, node_id: str) -> list[str]:
        raise NotImplementedError

    def collect_version(self, backend_state: dict[str, Any]) -> str | None:
        return backend_state.get("version")

    def collect_rss(self, backend_state: dict[str, Any]) -> tuple[Any | None, str, str]:
        return None, "unavailable", "unavailable"

    def teardown_backend(self, backend_state: dict[str, Any]) -> None:
        return None

    def query_map_now(self) -> dict[str, str]:
        raise NotImplementedError

    def run_measurement_now(self, context: BackendRunContext) -> CompetitorMeasurement:
        try:
            prepared_inputs = self.prepare_import_inputs(context)
            import_duration_ms = self.import_dataset(context, prepared_inputs)
            backend_state = self.open_backend(context, prepared_inputs)
        except Exception as exc:
            return CompetitorMeasurement(
                backend_name=self.backend_name,
                status="failed",
                reason=str(exc),
                version=None,
                import_duration_ms=None,
                open_start_ms=None,
                operation_count=0,
                mean_ms=None,
                p50_ms=None,
                p95_ms=None,
                p99_ms=None,
                rss_bytes=None,
                rss_scope="unavailable",
                rss_source="unavailable",
                parity_checked=False,
                parity_passed=False,
                query_language=self.query_language,
                source_note=None,
            )

        try:
            query_runner = lambda family_name, node_id: self.run_query_family(backend_state, family_name, node_id)
            validate_engine_parity_now(
                self.backend_name,
                query_runner,
                context.query_rows,
                context.truth_answers,
            )
            rss_process, rss_scope, rss_source = self.collect_rss(backend_state)
            metrics = measure_engine_latency_now(
                engine_name=self.backend_name,
                query_runner=query_runner,
                query_rows=context.query_rows,
                warmup_passes=context.warmup_passes,
                measure_passes=context.measure_passes,
                rss_limit_bytes=context.rss_limit_bytes,
                rss_process=rss_process,
                rss_scope=rss_scope,
                rss_source=rss_source,
            )
            return CompetitorMeasurement(
                backend_name=self.backend_name,
                status=metrics["status"],
                reason=metrics["reason"],
                version=self.collect_version(backend_state),
                import_duration_ms=import_duration_ms,
                open_start_ms=backend_state.get("open_start_ms"),
                operation_count=metrics["operation_count"],
                mean_ms=metrics["mean_ms"],
                p50_ms=metrics["p50_ms"],
                p95_ms=metrics["p95_ms"],
                p99_ms=metrics["p99_ms"],
                rss_bytes=metrics["rss_bytes"],
                rss_scope=metrics["rss_scope"],
                rss_source=metrics["rss_source"],
                parity_checked=True,
                parity_passed=metrics["status"] in {"ok", "degraded"},
                query_language=self.query_language,
                source_note=None,
            )
        except Exception as exc:
            return CompetitorMeasurement(
                backend_name=self.backend_name,
                status="failed",
                reason=str(exc),
                version=self.collect_version(backend_state),
                import_duration_ms=import_duration_ms,
                open_start_ms=backend_state.get("open_start_ms"),
                operation_count=0,
                mean_ms=None,
                p50_ms=None,
                p95_ms=None,
                p99_ms=None,
                rss_bytes=None,
                rss_scope="unavailable",
                rss_source="unavailable",
                parity_checked=True,
                parity_passed=False,
                query_language=self.query_language,
                source_note=None,
            )
        finally:
            self.teardown_backend(backend_state)


class Neo4jBackendAdapter(BenchmarkBackendAdapter):
    backend_name = "neo4j"
    query_language = "cypher"

    def query_map_now(self) -> dict[str, str]:
        return dict(CYTHERISH_QUERY_MAP)

    def prepare_import_inputs(self, context: BackendRunContext) -> dict[str, Any]:
        neo4j_import_dir = context.dataset_dir / "neo4j_import"
        required_files = (
            neo4j_import_dir / "nodes.header.csv",
            neo4j_import_dir / "nodes.data.csv",
            neo4j_import_dir / "relationships.header.csv",
            neo4j_import_dir / "relationships.data.csv",
        )
        if not all(path.exists() for path in required_files):
            export_neo4j_import_files(context.dataset_dir, neo4j_import_dir)
        return {"neo4j_import_dir": neo4j_import_dir}

    def import_dataset(self, context: BackendRunContext, prepared_inputs: dict[str, Any]) -> float | None:
        if not context.neo4j_uri or not context.neo4j_user or not context.neo4j_password:
            raise RuntimeError("missing Neo4j connection settings")
        if not Path("/opt/homebrew/bin/brew").exists():
            raise RuntimeError(missing_command_reason_now("/opt/homebrew/bin/brew"))
        neo4j_admin_bin = find_neo4j_admin_now()
        if neo4j_admin_bin is None:
            raise RuntimeError(missing_command_reason_now("neo4j-admin"))

        subprocess.run(
            ["/opt/homebrew/bin/brew", "services", "stop", "neo4j"],
            capture_output=True,
            text=True,
            check=False,
        )
        wait_for_port_state_now(7687, should_listen=False, timeout_seconds=90)
        bootout_neo4j_service_now()

        neo4j_import_dir = Path(prepared_inputs["neo4j_import_dir"])
        dry_run_command = [
            neo4j_admin_bin,
            "database",
            "import",
            "full",
            "--dry-run=true",
            "--overwrite-destination=true",
            "--report-file",
            str(context.work_dir / "neo4j-import-dry-run.txt"),
            "neo4j",
            f"--nodes={neo4j_import_dir / 'nodes.header.csv'},{neo4j_import_dir / 'nodes.data.csv'}",
            f"--relationships={neo4j_import_dir / 'relationships.header.csv'},{neo4j_import_dir / 'relationships.data.csv'}",
        ]
        dry_result = subprocess.run(dry_run_command, capture_output=True, text=True, check=False)
        if dry_result.returncode != 0:
            raise RuntimeError(dry_result.stderr.strip() or dry_result.stdout.strip() or "neo4j import dry run failed")

        import_started = time.perf_counter_ns()
        import_command = [
            neo4j_admin_bin,
            "database",
            "import",
            "full",
            "--overwrite-destination=true",
            "--report-file",
            str(context.work_dir / "neo4j-import-report.txt"),
            "neo4j",
            f"--nodes={neo4j_import_dir / 'nodes.header.csv'},{neo4j_import_dir / 'nodes.data.csv'}",
            f"--relationships={neo4j_import_dir / 'relationships.header.csv'},{neo4j_import_dir / 'relationships.data.csv'}",
        ]
        import_result = subprocess.run(import_command, capture_output=True, text=True, check=False)
        import_finished = time.perf_counter_ns()
        if import_result.returncode != 0:
            raise RuntimeError(import_result.stderr.strip() or import_result.stdout.strip() or "neo4j import failed")

        start_result = subprocess.run(
            ["/opt/homebrew/bin/brew", "services", "start", "neo4j"],
            capture_output=True,
            text=True,
            check=False,
        )
        if start_result.returncode != 0:
            raise RuntimeError(start_result.stderr.strip() or start_result.stdout.strip() or "failed to start neo4j")
        wait_for_port_state_now(7687, should_listen=True, timeout_seconds=90)
        return round((import_finished - import_started) / 1_000_000.0, 6)

    def open_backend(self, context: BackendRunContext, prepared_inputs: dict[str, Any]) -> dict[str, Any]:
        driver, session, open_start_ms, version = open_neo4j_engine_now(
            uri=context.neo4j_uri or "",
            user=context.neo4j_user or "",
            password=context.neo4j_password or "",
            database=context.neo4j_database,
        )
        return {
            "driver": driver,
            "session": session,
            "runner": build_neo4j_runner_now(session),
            "open_start_ms": open_start_ms,
            "version": version,
            "rss_process": resolve_neo4j_server_process_now(),
        }

    def run_query_family(self, backend_state: dict[str, Any], family_name: str, node_id: str) -> list[str]:
        return backend_state["runner"](family_name, node_id)

    def collect_rss(self, backend_state: dict[str, Any]) -> tuple[Any | None, str, str]:
        return backend_state["rss_process"], "server_process_only", "psutil_server_process"

    def teardown_backend(self, backend_state: dict[str, Any]) -> None:
        backend_state["session"].close()
        backend_state["driver"].close()


class UnsupportedBackendAdapter(BenchmarkBackendAdapter):
    def __init__(
        self,
        backend_name: str,
        query_language: str,
        query_map: dict[str, str],
        unsupported_reason: str,
    ) -> None:
        self.backend_name = backend_name
        self.query_language = query_language
        self._query_map = dict(query_map)
        self._unsupported_reason = unsupported_reason

    def query_map_now(self) -> dict[str, str]:
        return dict(self._query_map)

    def run_measurement_now(self, context: BackendRunContext) -> CompetitorMeasurement:
        return unsupported_measurement_now(
            backend_name=self.backend_name,
            query_language=self.query_language,
            reason=self._unsupported_reason,
            version=reference_repo_version_now(self.backend_name),
            source_note="v003 adapter placeholder: semantics mapped, local runtime integration not yet implemented",
        )


def build_backend_registry_now() -> dict[str, BenchmarkBackendAdapter]:
    return {
        "neo4j": Neo4jBackendAdapter(),
        "memgraph": UnsupportedBackendAdapter(
            backend_name="memgraph",
            query_language="cypher",
            query_map=CYTHERISH_QUERY_MAP,
            unsupported_reason="Memgraph adapter not wired in the local v003 harness",
        ),
        "kuzu": UnsupportedBackendAdapter(
            backend_name="kuzu",
            query_language="cypher",
            query_map=CYTHERISH_QUERY_MAP,
            unsupported_reason="Kuzu adapter not wired in the local v003 harness",
        ),
        "falkordb": UnsupportedBackendAdapter(
            backend_name="falkordb",
            query_language="opencypher",
            query_map=CYTHERISH_QUERY_MAP,
            unsupported_reason="FalkorDB adapter not wired in the local v003 harness",
        ),
        "hugegraph": UnsupportedBackendAdapter(
            backend_name="hugegraph",
            query_language="opencypher",
            query_map=CYTHERISH_QUERY_MAP,
            unsupported_reason="HugeGraph adapter not wired in the local v003 harness",
        ),
        "apache-age": UnsupportedBackendAdapter(
            backend_name="apache-age",
            query_language="opencypher",
            query_map=CYTHERISH_QUERY_MAP,
            unsupported_reason="Apache AGE adapter not wired in the local v003 harness",
        ),
        "janusgraph": UnsupportedBackendAdapter(
            backend_name="janusgraph",
            query_language="gremlin",
            query_map=GREMLIN_QUERY_MAP,
            unsupported_reason="JanusGraph adapter not wired in the local v003 harness",
        ),
        "dgraph": UnsupportedBackendAdapter(
            backend_name="dgraph",
            query_language="dql",
            query_map=DGRAPH_QUERY_MAP,
            unsupported_reason="Dgraph adapter not wired in the local v003 harness",
        ),
    }
