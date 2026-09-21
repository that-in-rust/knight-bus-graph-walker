"""Independent objective checks for additional signed-comparison controls."""

from fractions import Fraction as F
from importlib import import_module, util
from itertools import combinations
import random
import unittest


class SignedControlTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(util.find_spec("probe_signed_control_ablation"), "signed-control implementation is absent")
        self.bound = import_module("probe_signed_control_ablation").bound_signed_residual_controls

    def test_crossing_endpoint_strengthening(self):
        got = self.bound((1, 2, 2, 2, 3), (0, 0, 1, 1, 1), (0, 0, 1, 2, 0), F(1))
        self.assertEqual(got["row"], (-F(3, 2), F(5, 2)))
        self.assertEqual(got["endpoint"], (-F(1), F(2)))
        self.assertEqual(got["sign"], (-F(2), F(3)))

    def test_agreement_support_cancellation(self):
        got = self.bound((1,) * 6, (0, 0, 1, 1, 2, 2), (0, 0, 1, 2, 1, 2), F(3))
        self.assertEqual(got["support"], (-F(2), F(2)))
        self.assertEqual(got["row"], got["support"])

    def test_seeded_exact_containment(self):
        randomizer = random.Random(61403)
        for _ in range(160):
            size = randomizer.randrange(2, 12)
            cap = F(randomizer.randrange(1, 8), 3)
            edges = [(i, j, cap * F(randomizer.randrange(5), 4)) for i, j in combinations(range(size), 2)]
            degrees = [sum((w for i, j, w in edges if v in (i, j)), F()) for v in range(size)]
            mass = sum(degrees) / 2
            for _ in range(5):
                a = tuple(randomizer.randrange(4) for _ in degrees)
                b = tuple(randomizer.randrange(4) for _ in degrees)
                actual = sum((w * (int(a[i] == a[j]) - int(b[i] == b[j])) for i, j, w in edges), F())
                got = self.bound(degrees, a, b, cap)
                reverse = self.bound(degrees, b, a, cap)
                generic = self.bound(degrees, a, b, mass)
                for name, (low, high) in got.items():
                    self.assertLessEqual(low, actual)
                    self.assertLessEqual(actual, high)
                    self.assertEqual(reverse[name], (-high, -low))
                    self.assertLessEqual(generic[name][0], low)
                    self.assertLessEqual(high, generic[name][1])
                self.assertLessEqual(got["row"][0], got["endpoint"][0])
                self.assertLessEqual(got["endpoint"][1], got["row"][1])

    def test_equivalent_zero_partitions(self):
        self.assertTrue(all(x == (0, 0) for x in self.bound((0, 0), (1, 2), (4, 4), 0).values()))
        self.assertTrue(all(x == (0, 0) for x in self.bound((1, 1, 1, 1), (0, 0, 1, 1), (8, 8, 5, 5), 1).values()))

    def test_invalid_metadata_rejection(self):
        for degrees, a, b, cap in [((1, 1), (0,), (0, 0), 1), ((1, -1), (0, 1), (0, 0), 1),
                                   ((1, 1), (0, 1), (0, 0), -1), ((3, 1), (0, 1), (0, 0), 1)]:
            with self.assertRaises(ValueError):
                self.bound(degrees, a, b, cap)


if __name__ == "__main__":
    unittest.main()
