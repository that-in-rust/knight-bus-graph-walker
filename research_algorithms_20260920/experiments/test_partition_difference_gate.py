"""Joint residual certificates versus independent scores and refinement."""

from fractions import Fraction as F
import importlib
import random
import unittest

from probe_residual_degree_envelope import build_bounded_quotient_summary
from test_residual_degree_envelope import evaluate_original_modularity_direct


class PartitionDifferenceGateTests(unittest.TestCase):
    def setUp(self):
        self.subject = importlib.import_module("probe_partition_difference_gate")

    def test_crossing_partition_witness(self):
        a, b = (0, 0, 1, 1, 2, 2), (0, 0, 1, 2, 1, 2)
        summary = build_bounded_quotient_summary(6, [(0, 1, 1), (2, 3, 1), (4, 5, 1)], 2)
        result = self.subject.bound_original_partition_difference(summary, a, b, F(1), F(1))
        self.assertFalse(result["a_refines_b"])
        self.assertFalse(result["b_refines_a"])
        self.assertEqual((result["control_lower"], result["control_upper"]), (F(-1), F(1)))
        self.assertEqual((result["joint_lower"], result["joint_upper"]), (F(-2, 3), F(2, 3)))

    def test_identical_labels_cancel(self):
        r = (F(1),) * 6
        a, b = (0, 0, 1, 1, 2, 2), (5, 5, 4, 4, 7, 7)
        self.assertEqual(self.subject.bound_joint_residual_difference(r, a, b, F(1)), (F(), F()))
        self.assertEqual(self.subject.compare_partition_refinement_flags(a, b), (True, True))

    def test_refinement_sign_respected(self):
        a, b = (0, 1, 2, 3), (0, 0, 1, 1)
        lower, upper = self.subject.bound_joint_residual_difference((1, 1, 1, 1), a, b, F(1))
        self.assertLessEqual(upper, 0)
        self.assertEqual(self.subject.compare_partition_refinement_flags(a, b), (True, False))
        self.assertEqual(self.subject.bound_joint_residual_difference((1, 1, 1, 1), b, a, F(1)), (-upper, -lower))

    def test_seeded_original_differences(self):
        randomizer = random.Random(91045)
        for case in range(120):
            size = 3 + case % 7
            edges = [(randomizer.randrange(size), randomizer.randrange(size), F(randomizer.randrange(7), 3)) for _ in range(40)]
            budget = case % 12
            summary = build_bounded_quotient_summary(size, iter(edges), budget)
            mass = sum(summary.residual_degrees) / 2
            cap = mass / (budget + 1) if budget else None
            gamma = (F(1, 2), F(1), F(3, 2))[case % 3]
            for _ in range(10):
                a = tuple(randomizer.randrange(size) for _ in range(size))
                b = tuple(randomizer.randrange(size) for _ in range(size))
                result = self.subject.bound_original_partition_difference(summary, a, b, gamma, cap)
                actual = evaluate_original_modularity_direct(size, edges, a, gamma) - evaluate_original_modularity_direct(size, edges, b, gamma)
                self.assertLessEqual(result["control_lower"], result["joint_lower"])
                self.assertLessEqual(result["joint_lower"], actual)
                self.assertLessEqual(actual, result["joint_upper"])
                self.assertLessEqual(result["joint_upper"], result["control_upper"])
                reverse = self.subject.bound_original_partition_difference(summary, b, a, gamma, cap)
                self.assertEqual((result["joint_lower"], result["joint_upper"]), (-reverse["joint_upper"], -reverse["joint_lower"]))

    def test_zero_residual_exact(self):
        self.assertEqual(self.subject.bound_joint_residual_difference((), (), (), F()), (F(), F()))
        summary = build_bounded_quotient_summary(3, [(0, 0, 2), (0, 1, 3), (1, 2, 4)], 2)
        result = self.subject.bound_original_partition_difference(summary, (0, 1, 2), (0, 0, 1), F(1), F())
        self.assertEqual(result["joint_lower"], result["joint_upper"])

    def test_invalid_metadata_rejected(self):
        for degrees, a, b, cap in [((1, 1), (0,), (0, 1), 1), ((1, 1), (0, 1), (0,), 1),
                                    ((3, 3), (0, 1), (1, 1), 1), ((-1, 1), (0, 1), (1, 1), 1),
                                    ((0, 0), (0, 1), (1, 1), -1)]:
            with self.assertRaises(ValueError):
                self.subject.bound_joint_residual_difference(degrees, a, b, F(cap))


if __name__ == "__main__":
    unittest.main()
