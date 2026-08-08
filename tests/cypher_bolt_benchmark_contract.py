from __future__ import annotations

import unittest

from benchmarks.walk_hopper_v1.bench_cypher_bolt_compat import (
    collect_plan_operators_now,
    compare_result_rows_exact,
    hash_ordered_rows_now,
    query_text_for_family_now,
    summarize_latency_samples_now,
)


class CypherBoltBenchmarkContract(unittest.TestCase):
    def test_query_family_maps_to_unchanged_profile_text_now(self) -> None:
        self.assertEqual(
            query_text_for_family_now("forward_one"),
            "MATCH (n:Entity {node_id: $node_id})-[:DEPENDS_ON]->(m:Entity) "
            "RETURN m.node_id AS node_id ORDER BY node_id",
        )
        with self.assertRaises(KeyError):
            query_text_for_family_now("unknown")

    def test_ordered_result_hash_is_unambiguous_now(self) -> None:
        self.assertNotEqual(
            hash_ordered_rows_now(["a", "bc"]),
            hash_ordered_rows_now(["ab", "c"]),
        )
        self.assertEqual(
            hash_ordered_rows_now(["a", "bc"]),
            hash_ordered_rows_now(["a", "bc"]),
        )

    def test_parity_failure_names_engine_and_query_now(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "knight_bus.*reverse_two.*node-7",
        ):
            compare_result_rows_exact(
                "knight_bus",
                "reverse_two",
                "node-7",
                ["actual"],
                ["expected"],
            )

    def test_latency_summary_reports_tail_values_now(self) -> None:
        summary = summarize_latency_samples_now([1.0, 2.0, 3.0, 4.0])
        self.assertEqual(summary["sample_count"], 4)
        self.assertEqual(summary["mean_ms"], 2.5)
        self.assertEqual(summary["p50_ms"], 2.5)
        self.assertAlmostEqual(summary["p99_ms"], 3.97)

    def test_collects_driver_six_plan_dictionary_now(self) -> None:
        plan = {
            "operatorType": "ProduceResults@neo4j",
            "children": [
                {
                    "operatorType": "NodeIndexSeek@neo4j",
                    "children": [],
                }
            ],
        }
        self.assertEqual(
            collect_plan_operators_now(plan),
            ["ProduceResults@neo4j", "NodeIndexSeek@neo4j"],
        )


if __name__ == "__main__":
    unittest.main()
