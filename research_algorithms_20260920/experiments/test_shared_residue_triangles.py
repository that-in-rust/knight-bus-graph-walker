"""Exact root and expanded-graph oracles for native periodic masks."""

import importlib.util
import itertools
import math
import random
import types
import unittest

from test_masked_matching_triangles import expand_masked_graph_counts

if importlib.util.find_spec("probe_shared_residue_triangles"):
    import probe_shared_residue_triangles as candidate
else:
    candidate = types.SimpleNamespace()


def count_direct_periodic_roots(length, periods, masks):
    supports = [{x for left, right in runs for x in range(left, right)} for runs in masks]
    return sum(all(g % q in support for q, support in zip(periods, supports))
               for g in range(length))


class SharedResidueTests(unittest.TestCase):
    def require_candidate_function_exists(self, name):
        self.assertTrue(callable(getattr(candidate, name, None)), name)
        return getattr(candidate, name)

    def check_exact_periodic_count(self, periods, masks, repeats=1):
        solve = self.require_candidate_function_exists("count_shared_residue_roots")
        length = math.lcm(*periods) * repeats
        result, metrics = solve(length, periods, masks)
        self.assertEqual(result, count_direct_periodic_roots(length, periods, masks))
        self.assertEqual(metrics["events_processed"], metrics["planned_events"])
        self.assertLessEqual(metrics["shared_cyclic_events"], metrics["raw_cyclic_events"])
        return result, metrics

    def test_histogram_preserves_multiplicity(self):
        histogram = self.require_candidate_function_exists("build_residue_histogram_runs")
        runs = histogram(12, [(1, 6), (8, 11)], 3)
        expanded = [value for left, right, value in runs for _ in range(left, right)]
        self.assertEqual(expanded, [2, 3, 3])
        self.assertEqual(sum(expanded), 8)

    def test_coprime_huge_periods(self):
        solve = self.require_candidate_function_exists("count_shared_residue_roots")
        periods = (1000003, 1000033, 1000037)
        self.assertTrue(all(math.gcd(a, b) == 1 for a, b in itertools.combinations(periods, 2)))
        result, metrics = solve(math.prod(periods), periods,
                                [[(7, 130)], [(11, 467)], [(19, 808)]], event_budget=0)
        self.assertEqual(result, 123 * 456 * 789)
        self.assertEqual(metrics["shared_period"], 1)
        self.assertEqual(metrics["histogram_runs"], 3)
        self.assertEqual(metrics["events_processed"], 0)

    def test_shared_prime_powers(self):
        _, metrics = self.check_exact_periodic_count((12, 18, 20),
            [[(1, 5), (8, 11)], [(2, 9)], [(0, 3), (14, 19)]], 2)
        self.assertEqual(metrics["shared_period"], 12)
        self.assertEqual(metrics["marginal_periods"], (12, 6, 4))

    def test_all_small_mask_combinations(self):
        periods = (2, 3, 4)
        for bits in range(1 << sum(periods)):
            masks, start = [], 0
            for q in periods:
                masks.append([(g, g + 1) for g in range(q) if bits & (1 << (start + g))])
                start += q
            self.check_exact_periodic_count(periods, masks)

    def test_random_periodic_intersections(self):
        rng = random.Random(9473201)
        for _ in range(700):
            periods = tuple(rng.randrange(1, 13) for _ in range(3))
            masks = [[(g, g + 1) for g in range(q) if rng.random() < 0.55] for q in periods]
            self.check_exact_periodic_count(periods, masks, rng.randrange(1, 3))

    def test_full_empty_histograms(self):
        for masks in ([[(0, 6)], [(0, 10)], [(0, 15)]],
                      [[], [(0, 10)], [(0, 15)]]):
            _, metrics = self.check_exact_periodic_count((6, 10, 15), masks)
            self.assertEqual(metrics["planned_events"], 0)

    def test_event_budget_refusal(self):
        solve = self.require_candidate_function_exists("count_shared_residue_roots")
        with self.assertRaisesRegex(ValueError, "event budget"):
            solve(12, (3, 4, 12), [[(0, 1)], [(0, 2)], [(0, 11)]], event_budget=0)

    def test_invalid_native_profiles(self):
        solve = self.require_candidate_function_exists("count_shared_residue_roots")
        cases = [(6, (2, 3), [[], []]), (6, (2, 3, 4), [[], [], []]),
                 (6, (True, 2, 3), [[], [], []]),
                 (6, (1, 2, 3), [[(0, 0.5)], [], []]),
                 (6, (1, 2, 3), [[(0, 1), (0, 1)], [], []])]
        for args in cases:
            with self.subTest(args=args), self.assertRaises(ValueError):
                solve(*args)

    def test_false_independence_rejected(self):
        result, _ = self.check_exact_periodic_count((2, 2, 1),
                                                   [[(0, 1)], [(1, 2)], [(0, 1)]], 2)
        self.assertEqual(result, 0)

    def test_large_shared_interval(self):
        solve = self.require_candidate_function_exists("count_shared_residue_roots")
        q = 10**12
        result, metrics = solve(q, (q, q, q), [[(q // 2, q)]] * 3, event_budget=3)
        self.assertEqual(result, q // 2)
        self.assertEqual(metrics["events_processed"], 3)
        self.assertEqual(metrics["histogram_runs"], 6)

    def compare_complete_native_graph(self, bases, length, classes, periods, block):
        solve = self.require_candidate_function_exists("compute_local_period_triangles")
        output = self.require_candidate_function_exists("iterate_local_triangle_rows")
        source, total, metrics = solve(bases, length, classes, periods)
        flat = {key: [(offset + left, offset + right)
                      for offset in range(0, length, periods[key]) for left, right in runs]
                for key, runs in classes.items()}
        self.assertEqual((list(output(source, block)), total),
                         expand_masked_graph_counts(bases, length, flat))
        self.assertEqual(metrics["retained_triangle_terms"], 0)
        return source, total, metrics

    def test_complete_shifted_graph(self):
        classes = {(0, 1, 7): [(0, 1)], (1, 2, 11): [(1, 3)],
                   (0, 2, 18): [(0, 4)]}
        periods = {(0, 1, 7): 2, (1, 2, 11): 3, (0, 2, 18): 5}
        for block in (1, 4, 64):
            _, total, _ = self.compare_complete_native_graph(4, 30, classes, periods, block)
            self.assertEqual(total, 8)

    def test_degrees_counted_once(self):
        classes = {(a, b, 0): [(0, 1)] for a, b in itertools.combinations(range(4), 2)}
        periods = {key: 1 for key in classes}
        _, total, _ = self.compare_complete_native_graph(4, 1, classes, periods, 2)
        self.assertEqual(total, 4)

    def test_histogram_not_local_indicator(self):
        classes = {(0, 1, 0): [(0, 1)], (1, 2, 0): [(0, 1)], (0, 2, 0): [(0, 1)]}
        periods = dict(zip(classes, (2, 3, 5)))
        source, total, _ = self.compare_complete_native_graph(3, 30, classes, periods, 4)
        output = self.require_candidate_function_exists("iterate_local_triangle_rows")
        rows = list(output(source, 4))
        self.assertEqual(total, 1)
        self.assertEqual([row[1] for row in rows[:30]], [1] + [0] * 29)

    def test_random_native_graphs(self):
        rng = random.Random(3780124)
        for case in range(60):
            length, bases = (6, 12, 30)[case % 3], rng.randrange(2, 6)
            divisors = [q for q in range(1, length + 1) if length % q == 0]
            classes, periods = {}, {}
            for a, b in itertools.combinations(range(bases), 2):
                for d in rng.sample(range(length), rng.randrange(3)):
                    q = rng.choice(divisors)
                    classes[a, b, d] = [(g, g + 1) for g in range(q) if rng.random() < 0.6]
                    periods[a, b, d] = q
            self.compare_complete_native_graph(bases, length, classes, periods, 1 + case % 7)

    def test_huge_graph_compact_source(self):
        solve = self.require_candidate_function_exists("compute_local_period_triangles")
        output = self.require_candidate_function_exists("iterate_local_triangle_rows")
        qs = (1000003, 1000033, 1000037)
        keys = ((0, 1, 2), (1, 2, 3), (0, 2, 5))
        classes = dict(zip(keys, [[(7, 130)], [(11, 467)], [(19, 808)]]))
        source, total, metrics = solve(3, math.prod(qs), classes, dict(zip(keys, qs)), event_budget=0)
        self.assertEqual(total, 123 * 456 * 789)
        self.assertEqual(len(source["classes"]), 3)
        self.assertEqual(metrics["retained_triangle_terms"], 0)
        self.assertEqual(metrics["closed_class_triples"], 1)
        self.assertEqual(metrics["histogram_events"], 0)
        self.assertEqual(list(itertools.islice(output(source, 4), 8)), [(0, 0)] * 7 + [(1, 0)])

    def test_graph_contract_refusals(self):
        solve = self.require_candidate_function_exists("compute_local_period_triangles")
        output = self.require_candidate_function_exists("iterate_local_triangle_rows")
        with self.assertRaises(ValueError):
            solve(3, 6, {(0, 1, 0): [(0, 1)]}, {})
        source, _, _ = solve(3, 6, {}, {})
        for block in (0, True, 0.5):
            with self.subTest(block=block), self.assertRaises(ValueError):
                list(output(source, block))
        classes = {(0, 1, 0): [(0, 1)], (1, 2, 0): [(0, 2)], (0, 2, 0): [(0, 11)]}
        periods = dict(zip(classes, (3, 4, 12)))
        with self.assertRaisesRegex(ValueError, "event budget"):
            solve(3, 12, classes, periods, event_budget=0)

    def test_period_mapping_aliases(self):
        solve = self.require_candidate_function_exists("compute_local_period_triangles")
        for key in ((False, 1, 0), (0, True, 0), (0, 1, False),
                    (0.0, 1, 0), (0, 1.0, 0), (0, 1, 0.0)):
            for mask in ([], [(0, 2)]):
                with self.subTest(key=key, mask=mask), self.assertRaises(ValueError):
                    solve(2, 2, {(0, 1, 0): mask}, {key: 2})


if __name__ == "__main__":
    unittest.main()
