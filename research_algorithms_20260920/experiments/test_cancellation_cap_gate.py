"""Independent witnesses and actual residual streams for the primary cap bound."""

from fractions import Fraction as F
from itertools import combinations
import importlib
import random
import unittest

from probe_residual_degree_envelope import build_bounded_quotient_summary, bound_residual_internal_mass


class CancellationCapGateTests(unittest.TestCase):
    def setUp(self):
        self.subject = importlib.import_module("probe_cancellation_cap_gate")

    def test_complete_graph_witness(self):
        result = self.subject.bound_capped_residual_internal((3, 3, 3, 3), (0, 0, 1, 1), F(1))
        self.assertEqual(result, (F(2), F(2)))

    def test_preserves_known_gap(self):
        self.assertEqual(self.subject.bound_capped_residual_internal((1, 2, 2, 3), (0, 0, 1, 2), F(1)), (F(), F(1)))
        self.assertEqual(self.subject.bound_capped_residual_internal((2,) * 8, (0, 0, 0, 0, 1, 1, 1, 1), F(1)), (F(), F(8)))

    def test_weighted_stream_containment(self):
        randomizer = random.Random(91044)
        for case in range(160):
            size = 3 + case % 6
            edges = [(randomizer.randrange(size), randomizer.randrange(size), F(randomizer.randrange(7), 3)) for _ in range(35)]
            budget = 1 + case % 11
            summary = build_bounded_quotient_summary(size, iter(edges), budget)
            residual = {pair: F() for pair in combinations(range(size), 2)}
            for left, right, weight in edges:
                if left != right:
                    pair = tuple(sorted((left, right)))
                    residual[pair] += weight
            for pair, weight in summary.retained.items():
                residual[pair] -= weight
            cap = sum(summary.residual_degrees) / (2 * (budget + 1))
            self.assertTrue(all(F() <= value <= cap for value in residual.values()))
            for _ in range(8):
                labels = tuple(randomizer.randrange(size) for _ in range(size))
                lower, upper = self.subject.bound_capped_residual_internal(summary.residual_degrees, labels, cap)
                old_lower, old_upper = bound_residual_internal_mass(summary.residual_degrees, labels)
                actual = sum(w for (u, v), w in residual.items() if labels[u] == labels[v])
                self.assertLessEqual(old_lower, lower)
                self.assertLessEqual(lower, actual)
                self.assertLessEqual(actual, upper)
                self.assertLessEqual(upper, old_upper)

    def test_zero_residual_boundary(self):
        self.assertEqual(self.subject.bound_capped_residual_internal((0, 0, 0), (0, 1, 1), F()), (F(), F()))
        self.assertEqual(self.subject.bound_capped_residual_internal((), (), F()), (F(), F()))

    def test_invalid_metadata_rejected(self):
        for degrees, labels, cap in [((3, 3), (0, 1), 1), ((1, 1), (0,), 1), ((-1, 1), (0, 1), 1), ((0, 0), (0, 1), -1)]:
            with self.assertRaises(ValueError):
                self.subject.bound_capped_residual_internal(degrees, labels, F(cap))


if __name__ == "__main__":
    unittest.main()
