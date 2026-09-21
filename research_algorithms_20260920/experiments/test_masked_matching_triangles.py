"""Independent expanded-graph tests; no physical resource claim."""

import itertools
import random
import unittest

import probe_masked_matching_triangles as candidate


def expand_masked_graph_counts(bases, period, classes):
    edges = set()
    for (a, b, delta), intervals in classes.items():
        for left, right in intervals:
            for g in range(left, right):
                u, v = a * period + g, b * period + (g + delta) % period
                edges.add((u, v))
    return count_actual_edge_triangles(bases * period, edges)


def count_actual_edge_triangles(vertices, edges):
    neighbors = [set() for _ in range(vertices)]
    for u, v in edges:
        neighbors[u].add(v)
        neighbors[v].add(u)
    local = [0] * len(neighbors)
    total = 0
    for u, v, w in itertools.combinations(range(len(neighbors)), 3):
        if v in neighbors[u] and w in neighbors[u] and w in neighbors[v]:
            total += 1
            for x in (u, v, w):
                local[x] += 1
    return [(len(neighbors[v]), local[v]) for v in range(len(neighbors))], total


class MaskedMatchingTests(unittest.TestCase):
    def require_candidate_function_exists(self, name):
        self.assertTrue(callable(getattr(candidate, name, None)), name)
        return getattr(candidate, name)

    def check_complete_candidate_answers(self, bases, period, classes):
        solve = self.require_candidate_function_exists("compute_masked_triangle_counts")
        rows, total, metrics = solve(bases, period, classes)
        expected, expected_total = expand_masked_graph_counts(bases, period, classes)
        self.assertEqual((rows, total), (expected, expected_total))
        self.assertLessEqual(metrics["root_intervals"], metrics["join_run_work"])
        self.assertLessEqual(metrics["triangle_events"], 10 * metrics["root_intervals"])
        return metrics

    def test_shifted_interval_wraparound(self):
        shift = self.require_candidate_function_exists("shift_mask_interval_runs")
        self.assertEqual(shift([(2, 5)], 3, 7), [(0, 1), (5, 7)])
        self.assertEqual(shift([(0, 7)], -3, 7), [(0, 7)])

    def test_nonzero_shift_triangle(self):
        classes = {(0, 1, 2): [(0, 1)], (1, 2, 3): [(2, 3)],
                   (0, 2, 5): [(0, 1)]}
        self.check_complete_candidate_answers(3, 7, classes)
        self.assertEqual(expand_masked_graph_counts(3, 7, classes)[1], 1)

    def test_unbalanced_base_cycle(self):
        classes = {(0, 1, 0): [(0, 5)], (1, 2, 0): [(0, 5)],
                   (0, 2, 1): [(0, 5)]}
        self.check_complete_candidate_answers(3, 5, classes)
        self.assertEqual(expand_masked_graph_counts(3, 5, classes)[1], 0)

    def test_exhaustive_boolean_graphs(self):
        keys = [(a, b, d) for a, b in itertools.combinations(range(3), 2)
                for d in range(2)]
        for bits in range(1 << 12):
            classes = {key: [(g, g + 1) for g in range(2)
                             if bits & (1 << (2 * j + g))]
                       for j, key in enumerate(keys)}
            self.check_complete_candidate_answers(3, 2, classes)

    def test_random_complete_graphs(self):
        rng = random.Random(93021)
        for _ in range(180):
            bases, period = rng.randrange(2, 7), rng.randrange(1, 9)
            classes = {(a, b, d): [(g, g + 1) for g in range(period)
                                   if rng.random() < 0.4]
                       for a, b in itertools.combinations(range(bases), 2)
                       for d in range(period) if rng.random() < 0.4}
            self.check_complete_candidate_answers(bases, period, classes)

    def test_compiled_signed_defects(self):
        compile_masks = self.require_candidate_function_exists("compile_cover_defect_masks")
        base = {(0, 1): 0, (1, 2): 0, (0, 2): 0}
        defects = [(0, 0, 1, 0, -1), (0, 0, 2, 0, -1)]
        classes = compile_masks(3, 5, base, defects)
        self.check_complete_candidate_answers(3, 5, classes)
        self.assertEqual(expand_masked_graph_counts(3, 5, classes)[1], 4)
        with self.assertRaises(ValueError):
            compile_masks(3, 5, base, defects + [defects[0]])
        with self.assertRaises(ValueError):
            compile_masks(3, 5, base, [(0, 1, 0, 2, 1)])

    def test_repeated_matching_runs(self):
        compile_masks = self.require_candidate_function_exists("compile_cover_defect_masks")
        p, k, period, h = 5, 7, 30, 20
        base = {(a, z): 0 for a in range(p) for z in range(p, p + k)}
        defects = [(a, g, b, g, 1) for a, b in itertools.combinations(range(p), 2)
                   for g in range(h)]
        classes = compile_masks(p + k, period, base, defects)
        solve = self.require_candidate_function_exists("compute_masked_triangle_counts")
        rows, total, metrics = solve(p + k, period, classes)
        self.assertEqual(total, h * (k * p * (p - 1) // 2 + p * (p - 1) * (p - 2) // 6))
        for a in range(p + k):
            for g in range(period):
                if a < p:
                    expected = (k + (p - 1 if g < h else 0),
                                k * (p - 1) + (p - 1) * (p - 2) // 2 if g < h else 0)
                else:
                    expected = (p, p * (p - 1) // 2 if g < h else 0)
                self.assertEqual(rows[a * period + g], expected)
        self.assertEqual(metrics["root_intervals"], 80)
        self.assertEqual(metrics["source_runs"], 45)

    def test_random_defect_compilation(self):
        compile_masks = self.require_candidate_function_exists("compile_cover_defect_masks")
        solve = self.require_candidate_function_exists("compute_masked_triangle_counts")
        rng = random.Random(49931)
        for _ in range(80):
            bases, period = rng.randrange(2, 7), rng.randrange(1, 8)
            base = {(a, b): rng.randrange(period)
                    for a, b in itertools.combinations(range(bases), 2)
                    if rng.random() < 0.6}
            actual = {(a * period + g, b * period + (g + d) % period)
                      for (a, b), d in base.items() for g in range(period)}
            possible = [(a, g, b, h) for a, b in itertools.combinations(range(bases), 2)
                        for g in range(period) for h in range(period)]
            chosen = rng.sample(possible, min(18, len(possible)))
            defects = []
            for a, g, b, h in chosen:
                pair = a * period + g, b * period + h
                sign = -1 if pair in actual else 1
                if sign < 0:
                    actual.remove(pair)
                else:
                    actual.add(pair)
                defects.append((a, g, b, h, sign) if rng.random() < 0.5
                               else (b, h, a, g, sign))
            classes = compile_masks(bases, period, base, defects)
            rows, total, _ = solve(bases, period, classes)
            self.assertEqual((rows, total), count_actual_edge_triangles(bases * period, actual))

    def test_fragmentation_removes_compression(self):
        solve = self.require_candidate_function_exists("compute_masked_triangle_counts")
        common = {(0, 2, 0): [(0, 30)], (1, 2, 0): [(0, 30)]}
        compact = dict(common)
        compact[(0, 1, 0)] = [(0, 15)]
        fragmented = dict(common)
        fragmented[(0, 1, 0)] = [(g, g + 1) for g in range(0, 30, 2)]
        _, first_count, first_metrics = solve(3, 30, compact)
        _, second_count, second_metrics = solve(3, 30, fragmented)
        self.assertEqual((first_count, second_count), (15, 15))
        self.assertEqual((first_metrics["source_runs"], second_metrics["source_runs"]), (3, 17))
        self.assertEqual((first_metrics["root_intervals"], second_metrics["root_intervals"]), (1, 15))

    def test_invalid_profiles_refused(self):
        solve = self.require_candidate_function_exists("compute_masked_triangle_counts")
        compile_masks = self.require_candidate_function_exists("compile_cover_defect_masks")
        for classes in ({(0, 0, 1): [(0, 1)]}, {(0, 1, 3): [(0, 1)]},
                        {(0, 1, 0): [(0, 2), (1, 3)]}, {(0, 1, 0): [(0, 4)]}):
            with self.assertRaises(ValueError):
                solve(2, 3, classes)
        for defects in ([(0, 0, 1, 0, 1)], [(0, 0, 1, 1, -1)],
                        [(0, 3, 1, 0, 1)], [(0, 0, 1, 0, 0)]):
            with self.assertRaises(ValueError):
                compile_masks(2, 3, {(0, 1): 0}, defects)

    def test_noninteger_profile_refused(self):
        solve = self.require_candidate_function_exists("compute_masked_triangle_counts")
        compile_masks = self.require_candidate_function_exists("compile_cover_defect_masks")
        for bases, period, classes in (
                (2, 3, {(0, 0.5, 0): [(0, 3)]}),
                (2, 3, {(0, 1, 0.5): [(0, 3)]}),
                (2, 3, {(0, 1, 0): [(0.5, 1.5)]}),
                (2, 3, {(False, 1, 0): [(0, 3)]}),
                (True, 3, {}), (2, 3.0, {})):
            with self.subTest(bases=bases, period=period, classes=classes):
                with self.assertRaises(ValueError):
                    solve(bases, period, classes)
        for base, defects in (
                ({}, [(0, 0.5, 1, 0.5, 1)]),
                ({(0, 1): 0.5}, []),
                ({(0, 0.5): 0}, []),
                ({}, [(0, 0, 1, 0, True)])):
            with self.subTest(base=base, defects=defects):
                with self.assertRaises(ValueError):
                    compile_masks(2, 3, base, defects)

    def test_full_orbit_aggregation(self):
        period = 31
        shifts = ({g for g in range(period) if g % 5 != 0},
                  {g for g in range(period) if g % 7 in (0, 1, 3, 4)},
                  {g for g in range(period) if g % 11 in (0, 2, 3, 5, 7, 9)})
        classes = {(a, b, d): [(0, period)] for (a, b), group in
                   zip(((0, 1), (1, 2), (0, 2)), shifts) for d in group}
        rows, total, metrics = candidate.compute_masked_triangle_counts(3, period, classes, strategy="hybrid")
        self.assertEqual(total, 7316)
        self.assertEqual(rows, [(41, 236)] * period + [(42, 236)] * period + [(35, 236)] * period)
        self.assertEqual(metrics["full_convolutions"], 1)
        self.assertEqual(metrics["partial_pair_probes"], 0)

    def test_hybrid_boolean_exhaustive(self):
        keys = [(a, b, d) for a, b in itertools.combinations(range(3), 2)
                for d in range(2)]
        for bits in range(1 << 12):
            classes = {key: [(g, g + 1) for g in range(2)
                             if bits & (1 << (2 * j + g))]
                       for j, key in enumerate(keys)}
            rows, total, _ = candidate.compute_masked_triangle_counts(3, 2, classes, strategy="hybrid")
            self.assertEqual((rows, total), expand_masked_graph_counts(3, 2, classes))

    def test_minimum_pair_selection(self):
        period = 128
        classes = {(a, b, d): [(0, period)] for a, b in ((0, 1), (1, 2)) for d in range(period)}
        classes[(0, 2, 3)] = [(0, 1)]
        rows, total, metrics = candidate.compute_masked_triangle_counts(3, period, classes, strategy="hybrid")
        old_rows, old_total, old_metrics = candidate.compute_masked_triangle_counts(3, period, classes)
        self.assertEqual((rows, total), (old_rows, old_total))
        self.assertEqual(metrics["partial_pair_probes"], period)
        self.assertEqual(old_metrics["composable_pairs"], period * period)

    def test_hybrid_random_snapshots(self):
        rng = random.Random(217139)
        for _ in range(160):
            bases, period = rng.randrange(3, 7), rng.randrange(1, 9)
            classes = {}
            for a, b in itertools.combinations(range(bases), 2):
                for d in range(period):
                    choice = rng.randrange(4)
                    if choice == 0:
                        classes[(a, b, d)] = [(0, period)]
                    elif choice == 1:
                        classes[(a, b, d)] = [(g, g + 1) for g in range(period) if rng.random() < 0.5]
            rows, total, _ = candidate.compute_masked_triangle_counts(bases, period, classes, strategy="hybrid")
            self.assertEqual((rows, total), expand_masked_graph_counts(bases, period, classes))


if __name__ == "__main__":
    unittest.main()
