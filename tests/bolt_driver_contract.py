from __future__ import annotations

import json
import socket
import subprocess
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
import unittest

import neo4j
from neo4j import GraphDatabase
from neo4j.exceptions import AuthError, ClientError, Neo4jError, ServiceUnavailable


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FORWARD_ONE_QUERY = (
    "MATCH (n {node_id: $node_id})-[:DEPENDS_ON]->(m) "
    "RETURN m.node_id AS node_id ORDER BY node_id"
)
REVERSE_ONE_QUERY = (
    "MATCH (n {node_id: $node_id})<-[:DEPENDS_ON]-(m) "
    "RETURN m.node_id AS node_id ORDER BY node_id"
)
REVERSE_TWO_QUERY = (
    "MATCH (n {node_id: $node_id})<-[:DEPENDS_ON*1..2]-(m) "
    "RETURN DISTINCT m.node_id AS node_id ORDER BY node_id"
)


def reserve_local_port_now() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def write_graph_profile_now(
    snapshot_path: Path,
    node_count: int,
    relationship_count: int,
) -> None:
    profile = {
        "schema_version": 1,
        "profile_version": "knight-bus-neighborhood-walk-v1",
        "node_label": "Entity",
        "start_node_id_property": "node_id",
        "result_node_id_property": "node_id",
        "relationship_type": "DEPENDS_ON",
        "minimum_hops": 1,
        "maximum_hops": 2,
        "node_count": node_count,
        "relationship_count": relationship_count,
    }
    (snapshot_path / "compatibility-profile.json").write_text(
        json.dumps(profile, indent=2) + "\n",
        encoding="utf-8",
    )


@contextmanager
def connect_bounded_driver_now(
    snapshot_path: Path,
    *server_arguments: str,
) -> Iterator[neo4j.Driver]:
    port = reserve_local_port_now()
    uri = f"bolt://127.0.0.1:{port}"
    process = subprocess.Popen(
        [
            "cargo",
            "run",
            "--quiet",
            "--bin",
            "knight-bus-bolt",
            "--",
            "--snapshot",
            str(snapshot_path),
            "--bind",
            f"127.0.0.1:{port}",
            "--username",
            "neo4j",
            "--password",
            "test-password",
            *server_arguments,
        ],
        cwd=REPOSITORY_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    driver = GraphDatabase.driver(
        uri,
        auth=("neo4j", "test-password"),
        encrypted=False,
    )
    try:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if process.poll() is not None:
                _, stderr = process.communicate(timeout=1)
                raise RuntimeError(f"bounded Bolt server exited early:\n{stderr}")
            try:
                driver.verify_connectivity()
                break
            except Exception:
                time.sleep(0.1)
        else:
            raise TimeoutError("bounded Bolt server did not become ready")
        yield driver
    finally:
        driver.close()
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        process.communicate(timeout=1)


class BoltDriverContract(unittest.TestCase):
    process: subprocess.Popen[str]
    driver: neo4j.Driver
    temp_dir: tempfile.TemporaryDirectory[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.TemporaryDirectory(prefix="knight-bus-bolt-")
        root = Path(cls.temp_dir.name)
        nodes_path = root / "nodes.csv"
        edges_path = root / "edges.csv"
        snapshot_path = root / "snapshot"
        cls.snapshot_path = snapshot_path
        nodes_path.write_text(
            "node_id,node_type,label,parent_id,file_path,span\n"
            "A,function,A,,A,\n"
            "B,function,B,,B,\n"
            "C,function,C,,C,\n"
            "D,function,D,,D,\n",
            encoding="utf-8",
        )
        edges_path.write_text(
            "from_id,edge_type,to_id\n"
            "A,DEPENDS_ON,A\n"
            "A,DEPENDS_ON,B\n"
            "C,DEPENDS_ON,A\n"
            "D,DEPENDS_ON,C\n",
            encoding="utf-8",
        )
        subprocess.run(
            [
                "cargo",
                "run",
                "--quiet",
                "--bin",
                "knight-bus",
                "--",
                "build",
                "--nodes-csv",
                str(nodes_path),
                "--edges-csv",
                str(edges_path),
                "--output",
                str(snapshot_path),
            ],
            cwd=REPOSITORY_ROOT,
            check=True,
            text=True,
            capture_output=True,
        )
        write_graph_profile_now(snapshot_path, node_count=4, relationship_count=4)

        cls.port = reserve_local_port_now()
        cls.uri = f"bolt://127.0.0.1:{cls.port}"
        cls.process = subprocess.Popen(
            [
                "cargo",
                "run",
                "--quiet",
                "--bin",
                "knight-bus-bolt",
                "--",
                "--snapshot",
                str(snapshot_path),
                "--bind",
                f"127.0.0.1:{cls.port}",
                "--username",
                "neo4j",
                "--password",
                "test-password",
            ],
            cwd=REPOSITORY_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        cls.driver = GraphDatabase.driver(
            cls.uri,
            auth=("neo4j", "test-password"),
            encrypted=False,
        )
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if cls.process.poll() is not None:
                _, stderr = cls.process.communicate(timeout=1)
                raise RuntimeError(f"Knight Bus Bolt server exited early:\n{stderr}")
            try:
                cls.driver.verify_connectivity()
                return
            except Exception:
                time.sleep(0.1)
        raise TimeoutError("Knight Bus Bolt server did not become ready")

    @classmethod
    def tearDownClass(cls) -> None:
        if hasattr(cls, "driver"):
            cls.driver.close()
        if hasattr(cls, "process") and cls.process.poll() is None:
            cls.process.terminate()
            try:
                cls.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                cls.process.kill()
                cls.process.wait(timeout=5)
        if hasattr(cls, "temp_dir"):
            cls.temp_dir.cleanup()

    def query_node_ids_now(self, query: str, node_id: str) -> list[str]:
        with self.driver.session() as session:
            result = session.run(query, node_id=node_id)
            records = list(result)
            summary = result.consume()
        self.assertEqual(summary.query, query)
        self.assertTrue(all(record.keys() == ["node_id"] for record in records))
        return [record["node_id"] for record in records]

    def test_official_driver_runs_three_unchanged_queries_now(self) -> None:
        self.assertEqual(neo4j.__version__, "6.1.0")
        self.assertEqual(self.query_node_ids_now(FORWARD_ONE_QUERY, "A"), ["A", "B"])
        self.assertEqual(self.query_node_ids_now(REVERSE_ONE_QUERY, "A"), ["A", "C"])
        self.assertEqual(self.query_node_ids_now(REVERSE_TWO_QUERY, "A"), ["A", "C", "D"])
        self.assertEqual(self.query_node_ids_now(REVERSE_TWO_QUERY, "missing"), [])

    def test_typed_failure_recovers_connection_now(self) -> None:
        with self.driver.session() as session:
            with self.assertRaises(ClientError) as malformed:
                list(session.run("MATCH (n RETURN n", node_id="A"))
            self.assertEqual(malformed.exception.code, "Neo.ClientError.Statement.SyntaxError")

            with self.assertRaises(ClientError) as unsupported:
                list(
                    session.run(
                        "MATCH (n {node_id: $node_id})-[:KNOWS]->(m) "
                        "RETURN m.node_id AS node_id ORDER BY node_id",
                        node_id="A",
                    )
                )
            self.assertEqual(
                unsupported.exception.code,
                "Neo.ClientError.Statement.UnsupportedFeature",
            )

            recovered = list(session.run(FORWARD_ONE_QUERY, node_id="A"))
            self.assertEqual([record["node_id"] for record in recovered], ["A", "B"])

    def test_invalid_credentials_fail_before_query_now(self) -> None:
        invalid_driver = GraphDatabase.driver(
            self.uri,
            auth=("neo4j", "wrong-password"),
            encrypted=False,
        )
        try:
            with self.assertRaises(AuthError):
                invalid_driver.verify_connectivity()
        finally:
            invalid_driver.close()

    def test_explicit_transaction_is_rejected_honestly_now(self) -> None:
        with self.driver.session() as session:
            with self.assertRaises(ClientError) as failure:
                session.begin_transaction()
            self.assertEqual(
                failure.exception.code,
                "Neo.ClientError.Transaction.TransactionStartFailed",
            )

    def test_query_summary_carries_redacted_execution_receipt_now(self) -> None:
        secret_node_id = "A"
        with self.driver.session() as session:
            summary = session.run(FORWARD_ONE_QUERY, node_id=secret_node_id).consume()

        receipt = summary.metadata["knight_bus_receipt"]
        self.assertEqual(summary.server.agent, "knight-bus/0.0.2")
        self.assertEqual(summary.server.protocol_version[0], 5)
        self.assertEqual(
            receipt["profile_version"], "knight-bus-neighborhood-walk-v1"
        )
        self.assertEqual(receipt["parameter_names"], ["node_id"])
        self.assertEqual(receipt["result_row_count"], 2)
        self.assertEqual(receipt["termination_status"], "success")
        self.assertEqual(receipt["resource_high_water_status"], "unavailable")
        self.assertEqual(len(receipt["query_hash"]), 64)
        self.assertEqual(len(receipt["canonical_plan_hash"]), 64)
        self.assertEqual(len(receipt["snapshot_hash"]), 64)
        self.assertEqual(len(receipt["result_hash"]), 64)
        self.assertGreaterEqual(receipt["parse_compile_micros"], 0)
        self.assertGreaterEqual(receipt["execution_micros"], 0)
        self.assertNotIn(secret_node_id, json.dumps(receipt))

    def test_configured_execution_bounds_reach_driver_now(self) -> None:
        scenarios = [
            ("--max-result-rows", "1", "result_row_limit_exceeded"),
            ("--query-timeout-ms", "0", "deadline_exceeded"),
        ]
        for argument_name, argument_value, expected_termination in scenarios:
            with self.subTest(argument_name=argument_name):
                with connect_bounded_driver_now(
                    self.snapshot_path,
                    argument_name,
                    argument_value,
                ) as bounded_driver:
                    with bounded_driver.session() as session:
                        with self.assertRaises(Neo4jError) as failure:
                            list(session.run(FORWARD_ONE_QUERY, node_id="A"))
                        self.assertEqual(
                            failure.exception.code,
                            "Neo.ClientError.Transaction.Terminated",
                        )
                        failure_payload = json.loads(failure.exception.message)
                        receipt = failure_payload["knight_bus_receipt"]
                        self.assertEqual(
                            receipt["termination_status"],
                            expected_termination,
                        )
                        self.assertEqual(receipt["parameter_names"], ["node_id"])
                        self.assertEqual(receipt["result_row_count"], 0)
                        self.assertNotIn("A", json.dumps(receipt))

        self.assertEqual(self.query_node_ids_now(FORWARD_ONE_QUERY, "A"), ["A", "B"])

    def test_oversized_message_drops_only_bad_connection_now(self) -> None:
        with connect_bounded_driver_now(
            self.snapshot_path,
            "--max-message-size",
            "1024",
        ) as bounded_driver:
            with bounded_driver.session() as session:
                oversized_query = FORWARD_ONE_QUERY + (" " * 4096)
                with self.assertRaises(ServiceUnavailable):
                    list(session.run(oversized_query, node_id="A"))

            with bounded_driver.session() as recovered_session:
                recovered = list(recovered_session.run(FORWARD_ONE_QUERY, node_id="A"))
                self.assertEqual(
                    [record["node_id"] for record in recovered],
                    ["A", "B"],
                )

    def test_excluded_protocol_surfaces_fail_honestly_now(self) -> None:
        bookmarks = neo4j.Bookmarks.from_raw_values(["knight-bus:unsupported"])
        with self.driver.session(bookmarks=bookmarks) as session:
            with self.assertRaises(ClientError) as bookmark_failure:
                list(session.run(FORWARD_ONE_QUERY, node_id="A"))
            self.assertEqual(
                bookmark_failure.exception.code,
                "Neo.ClientError.Request.Invalid",
            )

        with self.driver.session(impersonated_user="other-user") as session:
            with self.assertRaises(ClientError) as impersonation_failure:
                list(session.run(FORWARD_ONE_QUERY, node_id="A"))
            self.assertEqual(
                impersonation_failure.exception.code,
                "Neo.ClientError.Request.Invalid",
            )

        with self.driver.session(database="system") as session:
            with self.assertRaises(ClientError) as database_failure:
                list(session.run(FORWARD_ONE_QUERY, node_id="A"))
            self.assertEqual(
                database_failure.exception.code,
                "Neo.ClientError.Request.Invalid",
            )

        with self.driver.session() as session:
            with self.assertRaises(ClientError) as write_failure:
                list(session.run("CREATE (:Entity {node_id: $node_id})", node_id="write"))
            self.assertEqual(
                write_failure.exception.code,
                "Neo.ClientError.Statement.UnsupportedFeature",
            )

        routing_driver = GraphDatabase.driver(
            self.uri.replace("bolt://", "neo4j://"),
            auth=("neo4j", "test-password"),
            encrypted=False,
        )
        try:
            with self.assertRaises(Neo4jError):
                routing_driver.verify_connectivity()
        finally:
            routing_driver.close()

        self.assertEqual(self.query_node_ids_now(FORWARD_ONE_QUERY, "A"), ["A", "B"])


if __name__ == "__main__":
    unittest.main()
