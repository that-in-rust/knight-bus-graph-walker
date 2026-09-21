"""Certify-or-replay must avoid source access only when correctness permits."""

from fractions import Fraction as F
import importlib
import random
import unittest

from probe_residual_degree_envelope import build_bounded_quotient_summary
from test_residual_degree_envelope import evaluate_original_modularity_direct


class PartitionSelectionExecutorTests(unittest.TestCase):
    def setUp(self):
        self.subject = importlib.import_module("probe_partition_selection_executor")

    def test_certificate_avoids_source(self):
        edges = [(0, 1, 1), (2, 3, 1), (4, 5, 1), (2, 3, 3)]
        summary = build_bounded_quotient_summary(6, iter(edges), 2)
        candidates = [("base", tuple(range(6))), ("a", (0, 0, 1, 1, 2, 2)), ("b", (0, 0, 1, 2, 1, 2))]
        def reject_any_source_access():
            self.fail("certified choice must not open or read source")
        result = self.subject.select_certified_partition_family(summary, candidates, F(1), F(1), reject_any_source_access, mode="joint")
        self.assertEqual(result["winner"], "a")
        self.assertEqual(result["mode"], "certified")
        self.assertEqual(result["replay_records"], 0)
        self.assertLess(result["score_lower"], result["score_upper"])

    def test_ambiguity_requires_replay(self):
        a, b = (0, 0, 1, 1, 2, 2), (0, 0, 1, 2, 1, 2)
        candidates = [("base", tuple(range(6))), ("a", a), ("b", b)]
        for edges, winner in [([(0, 1, 1), (2, 3, 1), (4, 5, 1)], "a"),
                              ([(0, 1, 1), (2, 4, 1), (3, 5, 1)], "b")]:
            summary = build_bounded_quotient_summary(6, iter(edges), 2)
            opens = []
            def open_counted_source_once():
                opens.append(1)
                return iter(edges)
            result = self.subject.select_certified_partition_family(summary, candidates, F(1), F(1), open_counted_source_once)
            self.assertEqual(opens, [1])
            self.assertEqual(result["mode"], "replayed")
            self.assertEqual(result["winner"], winner)
            self.assertEqual(result["replay_records"], 3)
            self.assertEqual(result["score_lower"], F(2, 3))
            self.assertEqual(result["score_upper"], F(2, 3))

    def test_exact_score_replays(self):
        edges = [(0, 1, 1), (2, 3, 1), (4, 5, 1), (2, 3, 3)]
        summary = build_bounded_quotient_summary(6, iter(edges), 2)
        candidates = [("base", tuple(range(6))), ("a", (0, 0, 1, 1, 2, 2)), ("b", (0, 0, 1, 2, 1, 2))]
        result = self.subject.select_certified_partition_family(summary, candidates, F(1), F(1), lambda: iter(edges), require_exact_score=True)
        self.assertEqual(result["mode"], "replayed")
        self.assertEqual(result["winner"], "a")
        self.assertEqual(result["score_lower"], F(1, 2))
        self.assertEqual(result["score_upper"], F(1, 2))

    def test_seeded_family_optimality(self):
        randomizer = random.Random(91046)
        for case in range(100):
            size = 3 + case % 6
            edges = [(randomizer.randrange(size), randomizer.randrange(size), F(randomizer.randrange(6), 2)) for _ in range(30)]
            budget = case % 10
            summary = build_bounded_quotient_summary(size, iter(edges), budget)
            cap = sum(summary.residual_degrees) / (2 * (budget + 1)) if budget else None
            candidates = [("base", tuple(range(size)))] + [(str(j), tuple(randomizer.randrange(size) for _ in range(size))) for j in (1, 2)]
            actuals = {name: evaluate_original_modularity_direct(size, edges, labels, F(1)) for name, labels in candidates}
            for mode in ("independent", "refinement", "joint"):
                result = self.subject.select_certified_partition_family(summary, candidates, F(1), cap, lambda: iter(edges), mode=mode)
                self.assertEqual(actuals[result["winner"]], max(actuals.values()))
                self.assertLessEqual(result["score_lower"], actuals[result["winner"]])
                self.assertGreaterEqual(result["score_upper"], actuals[result["winner"]])

    def test_invalid_family_rejected(self):
        summary = build_bounded_quotient_summary(2, [(0, 1, 1)], 1)
        for candidates, mode in [([], "joint"), ([("a", (0, 1)), ("a", (1, 1))], "joint"),
                                 ([("a", (0,))], "joint"), ([("a", (0, 1))], "unsupported")]:
            with self.assertRaises(ValueError):
                self.subject.select_certified_partition_family(summary, candidates, F(1), F(), lambda: iter(()), mode=mode)


if __name__ == "__main__":
    unittest.main()
