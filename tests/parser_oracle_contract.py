from __future__ import annotations

import base64
import json
import os
import subprocess
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PATH = REPOSITORY_ROOT / "tests/fixtures/cypher_walk_queries.json"
BRIDGE_ROOT = REPOSITORY_ROOT / "compat/cypher-parser-bridge"
BRIDGE_JAR = BRIDGE_ROOT / "target/cypher-parser-bridge-1.0.0.jar"
REFERENCE_MAVEN = (
    REPOSITORY_ROOT / "gitrefrepo/Neo4j family/cypher-dsl-src/mvnw"
)
JAVA_HOME = Path("/opt/homebrew/opt/openjdk@21")


def encode_query_line_now(query: str) -> str:
    return base64.b64encode(query.encode("utf-8")).decode("ascii")


def run_reference_parser_now(query: str) -> dict[str, str]:
    process = subprocess.run(
        [str(JAVA_HOME / "bin/java"), "-jar", str(BRIDGE_JAR)],
        input=encode_query_line_now(query) + "\n",
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(process.stdout.strip())


def run_native_compiler_now(query: str) -> dict[str, object]:
    process = subprocess.run(
        [
            str(REPOSITORY_ROOT / "target/debug/knight-bus-cypher-check"),
            "--query-base64",
            encode_query_line_now(query),
            "--node-id",
            "fixture-node",
        ],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(process.stdout)


class ParserOracleContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not REFERENCE_MAVEN.exists():
            raise RuntimeError(f"Neo4j Maven wrapper is missing: {REFERENCE_MAVEN}")
        environment = os.environ.copy()
        environment["JAVA_HOME"] = str(JAVA_HOME)
        subprocess.run(
            [
                str(REFERENCE_MAVEN),
                "-q",
                "-f",
                str(BRIDGE_ROOT / "pom.xml"),
                "-DskipTests",
                "package",
            ],
            cwd=REPOSITORY_ROOT,
            env=environment,
            check=True,
        )
        subprocess.run(
            ["cargo", "build", "--quiet", "--bin", "knight-bus-cypher-check"],
            cwd=REPOSITORY_ROOT,
            check=True,
        )
        cls.fixtures = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))

    def test_supported_variants_share_reference_acceptance_and_plan_now(self) -> None:
        for fixture in self.fixtures["supported"]:
            queries = [fixture["query"], *fixture["variants"]]
            plan_hashes: set[str] = set()
            for query in queries:
                reference = run_reference_parser_now(query)
                native = run_native_compiler_now(query)
                self.assertEqual(reference["outcome"], "accepted", fixture["id"])
                self.assertEqual(native["outcome"], "accepted", fixture["id"])
                plan_hashes.add(str(native["plan_hash"]))
            self.assertEqual(len(plan_hashes), 1, fixture["id"])

    def test_syntax_and_support_categories_agree_with_oracle_now(self) -> None:
        for fixture in self.fixtures["rejected"]:
            reference = run_reference_parser_now(fixture["query"])
            native = run_native_compiler_now(fixture["query"])
            self.assertEqual(
                reference["outcome"], fixture["reference_outcome"], fixture["id"]
            )
            self.assertEqual(native["outcome"], fixture["native_outcome"], fixture["id"])


if __name__ == "__main__":
    unittest.main()
