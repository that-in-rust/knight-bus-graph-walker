from __future__ import annotations

import json
import os
import socket
import subprocess
import tempfile
import time
import unittest
import uuid
from pathlib import Path

from neo4j import GraphDatabase


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


def load_local_neo4j_environment_now() -> dict[str, str]:
    environment = os.environ.copy()
    for line in (REPOSITORY_ROOT / ".env.neo4j.local").read_text().splitlines():
        if line and not line.startswith("#"):
            name, value = line.split("=", 1)
            environment[name] = value
    return environment


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


class CypherWalkDifferential(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        environment = load_local_neo4j_environment_now()
        cls.temp_dir = tempfile.TemporaryDirectory(prefix="knight-bus-differential-")
        cls.root = Path(cls.temp_dir.name)
        cls.neo4j_driver = GraphDatabase.driver(
            environment["NEO4J_URI"],
            auth=(environment["NEO4J_USER"], environment["NEO4J_PASSWORD"]),
        )
        cls.neo4j_database = environment["NEO4J_DATABASE"]
        cls.neo4j_driver.verify_connectivity()
        cls.namespace = f"knight-bus-differential-{uuid.uuid4().hex}-"

        cls.empty_process, cls.empty_driver = cls.start_knight_bus_now([], [], "empty")
        cls.empty_results = {
            query: cls.run_query_now(cls.empty_driver, query, "missing")
            for query in (FORWARD_ONE_QUERY, REVERSE_ONE_QUERY, REVERSE_TWO_QUERY)
        }
        cls.stop_knight_bus_now(cls.empty_process, cls.empty_driver)

        logical_nodes = [
            "isolated",
            "forward-a",
            "forward-b",
            "fanin-root",
            "fanin-one",
            "fanin-two",
            "chain-zero",
            "chain-one",
            "chain-two",
            "diamond-zero",
            "diamond-left",
            "diamond-right",
            "diamond-two",
            "self",
            "cycle-zero",
            "cycle-one",
            "unreachable-zero",
            "unreachable-one",
        ]
        logical_edges = [
            ("forward-a", "forward-b"),
            ("fanin-one", "fanin-root"),
            ("fanin-two", "fanin-root"),
            ("chain-one", "chain-zero"),
            ("chain-two", "chain-one"),
            ("diamond-left", "diamond-zero"),
            ("diamond-right", "diamond-zero"),
            ("diamond-two", "diamond-left"),
            ("diamond-two", "diamond-right"),
            ("self", "self"),
            ("cycle-zero", "cycle-one"),
            ("cycle-one", "cycle-zero"),
            ("unreachable-zero", "unreachable-one"),
        ]
        cls.nodes = [cls.fixture_node_id_now(node_id) for node_id in logical_nodes]
        cls.edges = [
            (cls.fixture_node_id_now(source), cls.fixture_node_id_now(target))
            for source, target in logical_edges
        ]
        cls.seed_neo4j_graph_now()
        cls.knight_process, cls.knight_driver = cls.start_knight_bus_now(
            cls.nodes, cls.edges, "combined"
        )

    @classmethod
    def tearDownClass(cls) -> None:
        if hasattr(cls, "knight_process"):
            cls.stop_knight_bus_now(cls.knight_process, cls.knight_driver)
        if hasattr(cls, "neo4j_driver"):
            cls.delete_neo4j_fixture_now()
            cls.neo4j_driver.close()
        if hasattr(cls, "temp_dir"):
            cls.temp_dir.cleanup()

    @classmethod
    def fixture_node_id_now(cls, logical_node_id: str) -> str:
        return f"{cls.namespace}{logical_node_id}"

    @classmethod
    def delete_neo4j_fixture_now(cls) -> None:
        with cls.neo4j_driver.session(database=cls.neo4j_database) as session:
            session.run(
                "MATCH (n:Entity) WHERE n.node_id STARTS WITH $namespace "
                "DETACH DELETE n",
                namespace=cls.namespace,
            ).consume()

    @classmethod
    def seed_neo4j_graph_now(cls) -> None:
        with cls.neo4j_driver.session(database=cls.neo4j_database) as session:
            session.run(
                "UNWIND $nodes AS node_id CREATE (:Entity {node_id: node_id})",
                nodes=cls.nodes,
            ).consume()
            session.run(
                "UNWIND $edges AS edge "
                "MATCH (source:Entity {node_id: edge[0]}) "
                "MATCH (target:Entity {node_id: edge[1]}) "
                "CREATE (source)-[:DEPENDS_ON]->(target)",
                edges=[list(edge) for edge in cls.edges],
            ).consume()

    @classmethod
    def start_knight_bus_now(
        cls, nodes: list[str], edges: list[tuple[str, str]], name: str
    ):
        fixture_root = cls.root / name
        fixture_root.mkdir()
        nodes_path = fixture_root / "nodes.csv"
        edges_path = fixture_root / "edges.csv"
        snapshot_path = fixture_root / "snapshot"
        nodes_path.write_text(
            "node_id,node_type,label,parent_id,file_path,span\n"
            + "".join(f"{node},function,{node},,{node},\n" for node in nodes),
            encoding="utf-8",
        )
        edges_path.write_text(
            "from_id,edge_type,to_id\n"
            + "".join(
                f"{source},DEPENDS_ON,{target}\n" for source, target in edges
            ),
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
            capture_output=True,
            text=True,
        )
        write_graph_profile_now(snapshot_path, len(nodes), len(edges))
        port = reserve_local_port_now()
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
                "differential-password",
            ],
            cwd=REPOSITORY_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        driver = GraphDatabase.driver(
            f"bolt://127.0.0.1:{port}",
            auth=("neo4j", "differential-password"),
            encrypted=False,
        )
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if process.poll() is not None:
                _, stderr = process.communicate(timeout=1)
                raise RuntimeError(f"Knight Bus exited early:\n{stderr}")
            try:
                driver.verify_connectivity()
                return process, driver
            except Exception:
                time.sleep(0.1)
        raise TimeoutError("Knight Bus did not become ready")

    @staticmethod
    def stop_knight_bus_now(process, driver) -> None:
        driver.close()
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

    @staticmethod
    def run_query_now(driver, query: str, node_id: str) -> tuple[list[str], list[str]]:
        with driver.session() as session:
            result = session.run(query, node_id=node_id)
            keys = result.keys()
            values = [record["node_id"] for record in result]
        return keys, values

    def assert_query_parity_now(self, query: str, node_id: str) -> None:
        with self.neo4j_driver.session(database=self.neo4j_database) as session:
            neo4j_result = session.run(query, node_id=node_id)
            neo4j_rows = (neo4j_result.keys(), [row["node_id"] for row in neo4j_result])
        knight_rows = self.run_query_now(self.knight_driver, query, node_id)
        self.assertEqual(knight_rows, neo4j_rows, (query, node_id))

    def test_empty_graph_matches_missing_neo4j_seed_now(self) -> None:
        missing_node_id = self.fixture_node_id_now("missing")
        for query, knight_rows in self.empty_results.items():
            with self.neo4j_driver.session(database=self.neo4j_database) as session:
                result = session.run(query, node_id=missing_node_id)
                neo4j_rows = (result.keys(), [row["node_id"] for row in result])
            self.assertEqual(knight_rows, neo4j_rows)

    def test_all_adversarial_graph_families_match_neo4j_now(self) -> None:
        cases = [
            (FORWARD_ONE_QUERY, "isolated"),
            (FORWARD_ONE_QUERY, "missing"),
            (FORWARD_ONE_QUERY, "forward-a"),
            (REVERSE_ONE_QUERY, "fanin-root"),
            (REVERSE_TWO_QUERY, "chain-zero"),
            (REVERSE_TWO_QUERY, "diamond-zero"),
            (REVERSE_TWO_QUERY, "self"),
            (REVERSE_TWO_QUERY, "cycle-zero"),
            (FORWARD_ONE_QUERY, "unreachable-zero"),
            (REVERSE_TWO_QUERY, "forward-b"),
        ]
        for query, node_id in cases:
            self.assert_query_parity_now(query, self.fixture_node_id_now(node_id))


if __name__ == "__main__":
    unittest.main()
