"""Independent finite oracles for the centered periodic-mask scalar kernel."""

import importlib.util
import itertools
import math
import random
import unittest
from unittest.mock import patch

from probe_shared_residue_triangles import count_shared_residue_roots
from probe_shared_residue_triangles import iterate_local_triangle_rows
from test_masked_matching_triangles import expand_masked_graph_counts


def encode_boolean_mask_runs(period, bits):
    runs = []
    for point in range(period):
        if bits & (1 << point):
            if runs and runs[-1][1] == point:
                runs[-1] = runs[-1][0], point + 1
            else:
                runs.append((point, point + 1))
    return runs


def count_explicit_mask_roots(length, periods, masks):
    return sum(all(any(a <= x % q < b for a, b in runs)
                   for q, runs in zip(periods, masks)) for x in range(length))


class CenteredResidueTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(importlib.util.find_spec("probe_centered_residue_triangles"),
                             "centered scalar kernel has not been implemented")
        import probe_centered_residue_triangles
        self.kernel = probe_centered_residue_triangles

    def test_exhaustive_small_masks(self):
        choices = [(q, encode_boolean_mask_runs(q, bits))
                   for q in range(1, 5) for bits in range(1 << q)]
        checked = 0
        for fixture in itertools.product(choices, repeat=3):
            periods, masks = zip(*fixture)
            length = math.lcm(*periods)
            expected = count_explicit_mask_roots(length, periods, masks)
            actual, stats = self.kernel.count_centered_residue_roots(length, periods, masks)
            self.assertEqual(actual, expected, fixture)
            self.assertEqual(stats["compatible_pairs"], stats["planned_pairs"])
            checked += 1
        self.assertEqual(checked, 27000)

    def test_random_weighted_histograms(self):
        rng, signs, checked = random.Random(74091), set(), 0
        for _ in range(600):
            periods = tuple(rng.randrange(1, 19) for _ in range(3))
            masks = [encode_boolean_mask_runs(q, rng.randrange(1 << q)) for q in periods]
            length = math.lcm(*periods) * rng.randrange(1, 4)
            actual, stats = self.kernel.count_centered_residue_roots(length, periods, masks)
            self.assertEqual(actual, count_explicit_mask_roots(length, periods, masks))
            signs.update(stats["deviation_signs"])
            checked += int(any(q != h for q, h in zip(periods, stats["marginal_periods"])))
        self.assertEqual(signs, {-1, 1})
        self.assertGreater(checked, 400)

    def test_interval_pair_enumeration(self):
        for modulus in range(1, 9):
            intervals = [(a, b) for a in range(8) for b in range(a + 1, 9)]
            for first, second in itertools.product(intervals, repeat=2):
                expected = {(u, v) for u in range(*first) for v in range(*second)
                            if (u - v) % modulus == 0}
                actual = list(self.kernel.iterate_compatible_interval_pairs(first, second, modulus))
                self.assertEqual(len(actual), len(set(actual)))
                self.assertEqual(set(actual), expected)

    def test_huge_dense_complements(self):
        a = 2 ** 80
        b, c = a + 1, 2 * a + 1
        periods, length = (a * b, a * c, b * c), a * b * c
        masks = [[(1, q)] for q in periods]
        actual, stats = self.kernel.count_centered_residue_roots(
            length, periods, masks, pair_budget=1, run_pair_budget=1)
        self.assertEqual(actual, length - a - b - c + 2)
        self.assertEqual(stats["centers"], (1, 1, 1))
        self.assertEqual(stats["planned_pairs"], 1)
        self.assertEqual(stats["compatible_pairs"], 1)
        self.assertEqual(stats["run_pairs_visited"], 1)
        self.assertEqual(stats["shared_events_control"], 2 * (a + b + c) - 3)

    def test_small_event_comparison(self):
        periods, masks = (35, 55, 77), [[(1, 35)], [(1, 55)], [(1, 77)]]
        ordinary, old = count_shared_residue_roots(385, periods, masks)
        candidate, new = self.kernel.count_centered_residue_roots(385, periods, masks)
        self.assertEqual(candidate, ordinary)
        self.assertEqual(old["events_processed"], new["shared_events_control"])
        self.assertEqual(new["planned_pairs"], 1)
        self.assertEqual(old["events_processed"], 43)

    def test_zero_pair_shortcut(self):
        periods = (20, 28, 140)
        masks = [[(2, 20)], [(0, 2), (4, 28)], [(1, 140)]]
        with patch.object(self.kernel, "iterate_compatible_interval_pairs",
                          side_effect=AssertionError("zero plan entered join")):
            actual, stats = self.kernel.count_centered_residue_roots(
                140, periods, masks, pair_budget=0, run_pair_budget=0)
        self.assertEqual(actual, count_explicit_mask_roots(140, periods, masks))
        self.assertEqual(stats["planned_pairs"], 0)

    def test_budget_refusal_preflight(self):
        periods, masks = (35, 55, 77), [[(1, 35)], [(1, 55)], [(1, 77)]]
        for budgets in ({"pair_budget": 0}, {"run_pair_budget": 0}):
            stats = {}
            with patch.object(self.kernel, "iterate_compatible_interval_pairs",
                              side_effect=AssertionError("refusal entered join")):
                with self.assertRaisesRegex(ValueError, "budget"):
                    self.kernel.count_centered_residue_roots(385, periods, masks,
                                                            stats=stats, **budgets)
            self.assertEqual(stats["planned_pairs"], 1)
            self.assertEqual(stats["compatible_pairs"], 0)
            self.assertEqual(stats["run_pairs_visited"], 0)

    def test_inputs_before_shortcuts(self):
        bad = [(4, (1, 2, 4), [[], [(0, 2)], [(0, 5)]], {}),
               (4, (1, 2, True), [[], [], []], {}),
               (4, (1, 2, 4), [[], [], []], {"pair_budget": True}),
               (4, (1, 2, 4), [[], [], []], {"run_pair_budget": -1}),
               (4, (1, 2, 3), [[], [], []], {}),
               (4, (1, 2, 4), [[], [], [(0, 2), (1, 3)]], {})]
        for length, periods, masks, kwargs in bad:
            with self.assertRaises(ValueError):
                self.kernel.count_centered_residue_roots(length, periods, masks, **kwargs)

    def test_constant_histogram_centers(self):
        for masks, answer in (([[(0, 4)], [(0, 6)], [(0, 9)]], 36),
                              ([[], [(0, 6)], [(0, 9)]], 0)):
            result, stats = self.kernel.count_centered_residue_roots(
                36, (4, 6, 9), masks, pair_budget=0, run_pair_budget=0)
            self.assertEqual(result, answer)
            self.assertEqual(stats["planned_pairs"], 0)

    def test_complete_native_graphs(self):
        solve = getattr(self.kernel, "compute_centered_period_triangles", None)
        self.assertTrue(callable(solve), "centered graph executor missing")
        rng = random.Random(574821)
        for case in range(80):
            length, bases = (6, 12, 30)[case % 3], rng.randrange(2, 6)
            divisors = [q for q in range(1, length + 1) if length % q == 0]
            classes, periods = {}, {}
            for a, b in itertools.combinations(range(bases), 2):
                for shift in rng.sample(range(length), rng.randrange(3)):
                    q = rng.choice(divisors)
                    classes[a, b, shift] = encode_boolean_mask_runs(q, rng.randrange(1 << q))
                    periods[a, b, shift] = q
            source, total, stats = solve(bases, length, classes, periods)
            flat = {key: [(offset + a, offset + b) for offset in range(0, length, periods[key])
                          for a, b in runs] for key, runs in classes.items()}
            self.assertEqual((list(iterate_local_triangle_rows(source, 1 + case % 9)), total),
                             expand_masked_graph_counts(bases, length, flat))
            self.assertEqual(stats["retained_triangle_terms"], 0)

    def test_graph_cumulative_budgets(self):
        solve = getattr(self.kernel, "compute_centered_period_triangles", None)
        self.assertTrue(callable(solve), "centered graph executor missing")
        periods = {(0, 1, 0): 35, (1, 2, 0): 55, (0, 2, 0): 77,
                   (0, 3, 0): 35, (2, 3, 0): 55}
        classes = {key: [(1, q)] for key, q in periods.items()}
        source, total, stats = solve(4, 385, classes, periods, pair_budget=2, run_pair_budget=2)
        self.assertEqual(total, 2 * (385 - 5 - 7 - 11 + 2))
        self.assertEqual(stats["compatible_pairs"], 2)
        self.assertEqual(stats["closed_class_triples"], 2)
        for budgets in ({"pair_budget": 1}, {"run_pair_budget": 1}):
            with self.assertRaisesRegex(ValueError, "budget"):
                solve(4, 385, classes, periods, **budgets)
        for budget in (True, -1):
            with self.assertRaises(ValueError):
                solve(1, 1, {}, {}, pair_budget=budget)

    def test_halfmask_work_refusal(self):
        a = 2 ** 80
        b, c = a + 1, 2 * a + 1
        periods, length = (a * b, a * c, b * c), a * b * c
        stats = {}
        with patch.object(self.kernel, "iterate_compatible_interval_pairs",
                          side_effect=AssertionError("huge refusal entered join")):
            with self.assertRaisesRegex(ValueError, "pair budget"):
                self.kernel.count_centered_residue_roots(
                    length, periods, [[(0, q // 2)] for q in periods], pair_budget=1, stats=stats)
        self.assertGreaterEqual(stats["planned_pairs"], length // 9)
        self.assertEqual(stats["compatible_pairs"], 0)
        self.assertEqual(stats["run_pairs_visited"], 0)

    def test_binary_join_obstruction(self):
        n, factors = 8, (23, 29, 31)
        periods, masks = [], []
        for a, b in itertools.combinations(factors, 2):
            points = set()
            for r in range(1, n + 1):
                for x, y in ((0, r), (r, 0)):
                    points.add(x + a * (((y - x) * pow(a, -1, b)) % b))
            periods.append(a * b)
            masks.append([(u, u + 1) for u in sorted(points)])
        result, stats = self.kernel.count_centered_residue_roots(math.prod(factors), periods, masks)
        self.assertEqual(result, 0)
        self.assertEqual(stats["centers"], (0, 0, 0))
        self.assertEqual(stats["pair_cardinalities"], (n * n + n,) * 3)
        self.assertEqual(stats["compatible_pairs"], 72)


if __name__ == "__main__":
    unittest.main()
