"""Dense independent arithmetic controls for a matrix-free dual evaluator."""

from fractions import Fraction as F
from importlib import import_module, util
from itertools import combinations
import random
import unittest


def evaluate_dense_dual_control(degrees, a, b, cap, potentials):
    value = sum((r * y for r, y in zip(degrees, potentials)), F())
    gradient = list(map(F, degrees))
    for i, j in combinations(range(len(degrees)), 2):
        coefficient = int(a[i] == a[j]) - int(b[i] == b[j]) - potentials[i] - potentials[j]
        if coefficient > 0:
            value += cap * coefficient
            gradient[i] -= cap
            gradient[j] -= cap
    return value, tuple(gradient)


class ImplicitPartitionDualTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(util.find_spec("probe_implicit_partition_dual"), "implicit dual implementation is absent")
        self.module = import_module("probe_implicit_partition_dual")

    def test_negative_potential_witness(self):
        result = self.module.evaluate_implicit_partition_dual((1, 2, 2, 2, 3), (0, 0, 1, 1, 1),
                                                              (0, 0, 1, 2, 0), 1, (0, 0, 0, 0, -1))
        self.assertEqual(result["upper"], 2)
        self.assertEqual(result["gradient"], (1, 2, 0, 0, 1))

    def test_seeded_dense_equivalence(self):
        randomizer = random.Random(40717)
        for _ in range(300):
            size = randomizer.randrange(0, 12)
            cap = F(randomizer.randrange(5), 3)
            edges = [(i, j, cap * F(randomizer.randrange(5), 4)) for i, j in combinations(range(size), 2)]
            degrees = [sum((w for i, j, w in edges if v in (i, j)), F()) for v in range(size)]
            a, b = [randomizer.randrange(4) for _ in degrees], [randomizer.randrange(5) for _ in degrees]
            y = tuple(F(randomizer.randrange(-8, 9), 4) for _ in degrees)
            z = tuple(F(randomizer.randrange(-8, 9), 4) for _ in degrees)
            got = self.module.evaluate_implicit_partition_dual(degrees, a, b, cap, y)
            expected, gradient = evaluate_dense_dual_control(degrees, a, b, cap, y)
            self.assertEqual(got["upper"], expected)
            self.assertEqual(got["gradient"], gradient)
            actual = sum((w * (int(a[i] == a[j]) - int(b[i] == b[j])) for i, j, w in edges), F())
            self.assertLessEqual(actual, expected)
            other, _ = evaluate_dense_dual_control(degrees, a, b, cap, z)
            self.assertGreaterEqual(other, expected + sum((g * (q - p) for g, p, q in zip(gradient, y, z)), F()))
            self.assertEqual(got["metrics"]["threshold_visits"], 8 * size)
            self.assertLessEqual(got["metrics"]["pointer_decrements"], 8 * size)

    def test_ties_identity_cancellation(self):
        for potentials in ((0, 0, 0, 0), (F(1, 2),) * 4, (-1, 0, 0, 1)):
            got = self.module.evaluate_implicit_partition_dual((1, 1, 1, 1), (0, 0, 1, 1), (8, 8, 5, 5), 1, potentials)
            value, gradient = evaluate_dense_dual_control((1, 1, 1, 1), (0, 0, 1, 1), (8, 8, 5, 5), 1, potentials)
            self.assertEqual((got["upper"], got["gradient"]), (value, gradient))

    def test_large_linear_accounting(self):
        size = 4096
        labels = tuple(i % 31 for i in range(size))
        got = self.module.evaluate_implicit_partition_dual((2,) * size, labels, labels, 1, (0,) * size)
        self.assertEqual(got["upper"], 0)
        self.assertEqual(got["gradient"], (F(2),) * size)
        self.assertEqual(got["metrics"]["membership_records"], 4 * size)
        self.assertEqual(got["metrics"]["prefix_additions"], 4 * size)
        self.assertEqual(got["metrics"]["threshold_visits"], 8 * size)
        self.assertEqual(got["metrics"]["implicit_pairs"], size * (size - 1) // 2)

    def test_fixed_round_validity(self):
        degrees, a, b = (1, 2, 2, 2, 3), (0, 0, 1, 1, 1), (0, 0, 1, 2, 0)
        got = self.module.search_bounded_partition_dual(degrees, a, b, 1, rounds=12)
        self.assertEqual(len(got["trace"]), 13)
        self.assertEqual(got["upper"], min(got["trace"]))
        self.assertGreaterEqual(got["upper"], 2)
        self.assertLessEqual(got["upper"], 3)
        self.assertEqual(self.module.evaluate_implicit_partition_dual(degrees, a, b, 1, got["potentials"])["upper"], got["upper"])

    def test_strict_global_witnesses(self):
        from probe_signed_control_ablation import bound_signed_residual_controls
        examples = [((1, 2, 1, 1), (0, 1, 1, 2), (0, 1, 2, 0),
                     (-F(1, 2), F(1, 2), F(1, 2), -F(1, 2)), F(1, 2)),
                    ((1, 3, 3, 2, 2), (0, 1, 2, 0, 3), (0, 1, 1, 2, 0),
                     (F(1, 2), -F(1, 2), -F(1, 2), F(1, 2), -F(1, 2)), -F(1, 2))]
        for degrees, a, b, potentials, expected in examples:
            got = self.module.evaluate_implicit_partition_dual(degrees, a, b, 1, potentials)
            self.assertEqual(got["upper"], expected)
            self.assertLess(got["upper"], bound_signed_residual_controls(degrees, a, b, 1)["endpoint"][1])

    def test_separating_bounded_score(self):
        from probe_residual_degree_envelope import build_bounded_quotient_summary
        from probe_residual_native_gate import evaluate_candidate_score_bounds
        a, b = (0, 1, 2, 0, 3), (0, 1, 1, 2, 0)
        edges = [(0, 2, F(1, 2)), (0, 3, F(1, 2)), (2, 3, F(1, 2)),
                 (1, 2, F(1)), (1, 3, F(1)), (1, 4, F(1)), (2, 4, F(1))]
        summary = build_bounded_quotient_summary(5, iter(edges), 0)
        self.assertEqual(summary.residual_degrees, (F(1), F(3), F(3), F(2), F(2)))
        # D=1 is independently certified by this simple weighted fixture, not MG capacity zero.
        got = self.module.search_bounded_partition_dual(summary.residual_degrees, a, b, 1, rounds=12)
        self.assertEqual(got["upper"], -F(24847, 55440))
        gamma, mass = F(1, 2), sum(summary.original_degrees) / 2
        offset = evaluate_candidate_score_bounds(summary, a, gamma)["known_lower"] - evaluate_candidate_score_bounds(summary, b, gamma)["known_lower"]
        self.assertEqual(offset, F(9, 121))
        self.assertGreater(offset, 0)  # Even exact separate marginal differences only give residual upper zero.
        self.assertLess(offset + got["upper"] / mass, 0)
        actual_mass = sum((w * (int(a[i] == a[j]) - int(b[i] == b[j])) for i, j, w in edges), F())
        self.assertEqual(actual_mass, -F(1, 2))
        self.assertLessEqual(offset + actual_mass / mass, offset + got["upper"] / mass)

    def test_invalid_inputs_rejection(self):
        for degrees, a, b, cap, y in [((1, 1), (0,), (0, 0), 1, (0, 0)),
                                      ((1, 1), (0, 1), (0, 0), -1, (0, 0)),
                                      ((-1, 1), (0, 1), (0, 0), 1, (0, 0)),
                                      ((1, 1), (0, 1), (0, 0), 1, (0,))]:
            with self.assertRaises(ValueError):
                self.module.evaluate_implicit_partition_dual(degrees, a, b, cap, y)
        with self.assertRaises(ValueError):
            self.module.search_bounded_partition_dual((1, 1), (0, 0), (0, 1), 1, rounds=-1)

    def test_projected_guarantee_schedule(self):
        self.assertTrue(hasattr(self.module, "solve_projected_partition_dual"), "projected guarantee schedule absent")
        for r, a, b, optimum in [((1, 2, 1, 1), (0, 1, 1, 2), (0, 1, 2, 0), F(1, 2)),
                                 ((1, 3, 3, 2, 2), (0, 1, 2, 0, 3), (0, 1, 1, 2, 0), -F(1, 2))]:
            for scale in (F(1, 3), F(1), F(2)):
                for root in (1, 2, 4):
                    degrees = tuple(scale * value for value in r)
                    got = self.module.solve_projected_partition_dual(degrees, a, b, scale, root_rounds=root)
                    self.assertEqual(got["evaluations"], root * root)
                    self.assertEqual(got["threshold_visits"], 8 * len(r) * root * root)
                    self.assertEqual(got["optimal_gap_bound"], scale * F(len(r) ** 2 * (len(r) - 1), root))
                    self.assertLessEqual(scale * optimum, got["upper"])
                    self.assertLessEqual(got["upper"] - scale * optimum, got["optimal_gap_bound"])
                    self.assertTrue(all(abs(value) <= len(r) for value in got["potentials"]))
                    self.assertNotIn("trace", got)
                    self.assertEqual(self.module.evaluate_implicit_partition_dual(degrees, a, b, scale, got["potentials"])["upper"], got["upper"])

    def test_projected_zero_invalid(self):
        self.assertTrue(hasattr(self.module, "solve_projected_partition_dual"), "projected guarantee schedule absent")
        result = self.module.solve_projected_partition_dual((0, 0, 0), (0, 0, 0), (0, 1, 2), 0, root_rounds=3)
        self.assertEqual((result["upper"], result["optimal_gap_bound"], result["evaluations"]), (0, 0, 0))
        for degrees, cap, root in [((1, 1), 1, 2), ((1, 1, 1), 0, 2),
                                   ((1, 1, 1), 1, 0), ((1, 1, 1), 1, True), ((3, 3, 3), 1, 2)]:
            with self.assertRaises(ValueError):
                self.module.solve_projected_partition_dual(degrees, (0,) * len(degrees), (1,) * len(degrees), cap, root_rounds=root)


if __name__ == "__main__":
    unittest.main()
