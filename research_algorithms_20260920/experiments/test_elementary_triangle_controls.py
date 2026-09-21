"""Stronger same-input controls, checked against direct finite root counts."""

import importlib.util
import itertools
import math
import random
import types
import unittest

from test_shared_residue_triangles import count_direct_periodic_roots

if importlib.util.find_spec("probe_elementary_triangle_controls"):
    import probe_elementary_triangle_controls as control
else:
    control = types.SimpleNamespace()


class ElementaryControlTests(unittest.TestCase):
    def require_control_function_exists(self, name):
        self.assertTrue(callable(getattr(control, name, None)), name)
        return getattr(control, name)

    def test_floor_matches_direct(self):
        solve = self.require_control_function_exists("sum_euclidean_floor_values")
        rng = random.Random(27062026)
        for _ in range(4000):
            n, m, a, b = rng.randrange(41), rng.randrange(1, 41), rng.randrange(81), rng.randrange(81)
            value, steps = solve(n, m, a, b)
            self.assertEqual(value, sum((a * k + b) // m for k in range(n)))
            self.assertLessEqual(steps, 2 * m.bit_length() + 2)
        n = 10**30
        self.assertEqual(solve(n, 7, 14, 21)[0], n * (n - 1) + 3 * n)

    def test_two_masks_exhaustive(self):
        solve = self.require_control_function_exists("count_two_periodic_masks")
        cases = 0
        for a, b in itertools.product(range(1, 5), repeat=2):
            for bits_a, bits_b in itertools.product(range(1 << a), range(1 << b)):
                masks = [[(x, x + 1) for x in range(q) if bits & (1 << x)]
                         for q, bits in ((a, bits_a), (b, bits_b))]
                length = 2 * math.lcm(a, b)
                value, metrics = solve(length, (a, b), masks)
                self.assertEqual(value, count_direct_periodic_roots(length, (a, b), masks))
                self.assertEqual(metrics["events_processed"], 0)
                self.assertLessEqual(metrics["overlap_steps"], metrics["histogram_runs"])
                cases += 1
        self.assertEqual(cases, 900)

    def test_three_masks_oracle(self):
        solve = self.require_control_function_exists("count_support_reduced_roots")
        rng = random.Random(302817)
        methods = set()
        for _ in range(900):
            periods = tuple(rng.randrange(1, 17) for _ in range(3))
            masks = [[(x, x + 1) for x in range(q) if rng.random() < 0.53] for q in periods]
            length = math.lcm(*periods) * rng.randrange(1, 4)
            value, metrics = solve(length, periods, masks)
            self.assertEqual(value, count_direct_periodic_roots(length, periods, masks))
            methods.add(metrics["method"])
        self.assertIn("shared_events", methods)
        self.assertIn("two_histograms", methods)

    def test_duplicate_period_adversary(self):
        solve = self.require_control_function_exists("count_support_reduced_roots")
        for n in (8, 64, 1024, 8192, 2**80):
            value, metrics = solve(2 * n, (2, 2 * n, 2 * n),
                                   [[(0, 1)], [(0, n)], [(0, n)]], event_budget=0)
            self.assertEqual(value, n // 2)
            self.assertEqual(metrics["method"], "two_histograms")
            self.assertEqual(metrics["equal_period_merges"], 1)
            self.assertEqual(metrics["events_processed"], 0)
            self.assertLessEqual(metrics["histogram_runs"], 4)
            self.assertLessEqual(metrics["overlap_steps"], 3)

    def test_singletons_skip_events(self):
        solve = self.require_control_function_exists("count_support_reduced_roots")
        cases = [((101, 103, 10403), [[(0, 1)], [(0, 1)], [(0, 10402)]]),
                 ((4, 6, 9), [[(1, 2)], [(3, 4)], [(1, 8)]]),
                 ((4, 6, 9), [[(1, 2)], [(2, 3)], [(1, 8)]]),
                 ((6, 10, 15), [[(4, 5)], [(4, 5)], [(3, 5), (8, 12)]])]
        for periods, masks in cases:
            length = 3 * math.lcm(*periods)
            value, metrics = solve(length, periods, masks, event_budget=0)
            self.assertEqual(value, count_direct_periodic_roots(length, periods, masks))
            self.assertEqual(metrics["method"], "singleton_crt")
            self.assertEqual(metrics["events_processed"], 0)

    def test_huge_distinct_singletons(self):
        solve = self.require_control_function_exists("count_support_reduced_roots")
        a, b, c = 1000003, 1000033, 1000037
        periods = a * b, a * c, b * c
        value, metrics = solve(a * b * c, periods, [[(17, 18)]] * 3, event_budget=0)
        self.assertEqual(value, 1)
        self.assertEqual(metrics["method"], "singleton_crt")
        self.assertEqual(metrics["events_processed"], 0)
        self.assertLessEqual(metrics["floor_steps"], 4)
        periods = a, b, c
        value, metrics = solve(a * b * c, periods,
                               [[(7, 8)], [(11, 12)], [(19, 808)]], event_budget=0)
        self.assertEqual(value, 789)
        self.assertLessEqual(metrics["floor_steps"], 4 * c.bit_length() + 4)

    def test_full_empty_simplification(self):
        solve = self.require_control_function_exists("count_support_reduced_roots")
        cases = [((2, 3, 5), [[(0, 2)], [(0, 3)], [(0, 5)]], 30, "full"),
                 ((2, 3, 5), [[(0, 2)], [(1, 2)], [(0, 5)]], 10, "one_mask"),
                 ((2, 2, 5), [[(0, 1)], [(1, 2)], [(0, 5)]], 0, "empty")]
        for periods, masks, expected, method in cases:
            value, metrics = solve(30, periods, masks, event_budget=0)
            self.assertEqual((value, metrics["method"]), (expected, method))

    def test_fallback_retains_admission(self):
        solve = self.require_control_function_exists("count_support_reduced_roots")
        periods = 12, 18, 20
        masks = [[(1, 5), (8, 11)], [(2, 9)], [(0, 3), (14, 19)]]
        value, metrics = solve(180, periods, masks, event_budget=16)
        self.assertEqual((value, metrics["method"], metrics["events_processed"]),
                         (20, "shared_events", 16))
        with self.assertRaisesRegex(ValueError, "event budget"):
            solve(180, periods, masks, event_budget=15)

    def test_invalid_shortcuts_rejected(self):
        solve = self.require_control_function_exists("count_support_reduced_roots")
        cases = [(6, (2, 3), [[], []]), (6, (2, 3, 4), [[], [], []]),
                 (6, (True, 2, 3), [[], [], []]),
                 (6, (1, 2, 3), [[], [], [(0, 0.5)]]),
                 (6, (1, 2, 3), [[], [], [(0, 1), (0, 1)]])]
        for args in cases:
            with self.subTest(args=args), self.assertRaises(ValueError):
                solve(*args)
        for budget in (-1, True, 0.5):
            with self.assertRaises(ValueError):
                solve(6, (1, 2, 3), [[], [], []], event_budget=budget)
        floor = self.require_control_function_exists("sum_euclidean_floor_values")
        for args in ((-1, 2, 0, 0), (1, 0, 0, 0), (1, 2, -1, 0), (1, 2, 0, -1), (True, 2, 0, 0)):
            with self.assertRaises(ValueError):
                floor(*args)


if __name__ == "__main__":
    unittest.main()
