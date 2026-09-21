"""Independent expanded-graph oracle for periodic coordinate refinement."""

import importlib.util
import itertools
import random
import types
import unittest

from test_masked_matching_triangles import expand_masked_graph_counts

if importlib.util.find_spec("probe_periodic_triangle_refinement"):
    import probe_periodic_triangle_refinement as candidate
else:
    candidate = types.SimpleNamespace()


class PeriodicRefinementTests(unittest.TestCase):
    def require_candidate_function_exists(self, name):
        self.assertTrue(callable(getattr(candidate, name, None)), name)
        return getattr(candidate, name)

    def compare_complete_refined_answers(self, bases, period, classes, refinement, backend):
        solve = self.require_candidate_function_exists("compute_refined_triangle_counts")
        expand = self.require_candidate_function_exists("iterate_refined_output_rows")
        residues, total, metrics = solve(bases, period, classes, refinement, backend)
        rows = list(expand(bases, period, refinement, residues))
        self.assertEqual((rows, total), expand_masked_graph_counts(bases, period, classes))
        self.assertEqual(len(residues), bases * refinement)
        return residues, total, metrics

    def test_carry_preserves_closure(self):
        classes = {(0, 1, 5): [(0, 6)], (1, 2, 3): [(0, 6)],
                   (0, 2, 2): [(0, 6)]}
        for p in (1, 2, 3, 6):
            for backend in ("pairs", "convolution"):
                _, total, _ = self.compare_complete_refined_answers(3, 6, classes, p, backend)
                self.assertEqual(total, 6)

    def test_partial_periodic_masks(self):
        classes = {(0, 1, 1): [(0, 1), (2, 3), (4, 5)],
                   (1, 2, 2): [(1, 2), (3, 4), (5, 6)],
                   (0, 2, 3): [(0, 1), (2, 3), (4, 5)]}
        for backend in ("pairs", "convolution"):
            rows, total, metrics = self.compare_complete_refined_answers(3, 6, classes, 2, backend)
            self.assertEqual(total, 3)
            self.assertEqual(rows, [(2, 1), (0, 0), (0, 0), (2, 1), (0, 0), (2, 1)])
            self.assertEqual(metrics["refined_classes"], 3)

    def test_invalid_period_refused(self):
        solve = self.require_candidate_function_exists("compute_refined_triangle_counts")
        for args in ((3, 6, {(0, 1, 0): [(0, 1)]}, 2),
                     (3, 6, {}, 4), (3, 6, {}, 0), (3, 6, {}, True),
                     (3, 6, {(0, 1, 0.5): [(0, 6)]}, 1)):
            with self.subTest(args=args), self.assertRaises(ValueError):
                solve(*args)

    def test_random_periodic_snapshots(self):
        rng = random.Random(482731)
        for _ in range(120):
            bases, p, repeats = rng.randrange(2, 5), rng.randrange(1, 5), rng.randrange(1, 4)
            length = p * repeats
            classes = {}
            for a, b in itertools.combinations(range(bases), 2):
                for delta in range(length):
                    if rng.random() < 0.35:
                        roots = [g for g in range(p) if rng.random() < 0.5]
                        classes[a, b, delta] = [(j * p + g, j * p + g + 1)
                                               for j in range(repeats) for g in roots]
            for backend in ("pairs", "convolution"):
                self.compare_complete_refined_answers(bases, length, classes, p, backend)

    def test_large_period_compaction(self):
        solve = self.require_candidate_function_exists("compute_refined_triangle_counts")
        expand = self.require_candidate_function_exists("iterate_refined_output_rows")
        length = 10**12
        classes = {(0, 1, 7): [(0, length)], (1, 2, 11): [(0, length)],
                   (0, 2, 18): [(0, length)]}
        rows, total, metrics = solve(3, length, classes, 1, "pairs")
        self.assertEqual(rows, [(2, 1)] * 3)
        self.assertEqual(total, length)
        self.assertEqual(metrics["refined_classes"], 3)
        self.assertEqual(metrics["pair_probes"], 1)
        self.assertEqual(list(itertools.islice(expand(3, length, 1, rows), 5)), [(2, 1)] * 5)

    def test_refinement_resource_tradeoff(self):
        classes = {(0, 1, 0): [(0, 12)], (1, 2, 0): [(0, 12)],
                   (0, 2, 0): [(0, 12)]}
        for p in (1, 2, 3, 4, 6, 12):
            _, _, metrics = self.compare_complete_refined_answers(3, 12, classes, p, "convolution")
            self.assertEqual(metrics["refined_classes"], 3 * p)
            self.assertEqual(metrics["max_convolution_period"], 12 // p)
            self.assertEqual(metrics["convolutions"], p)

    def test_empty_graph_isolates(self):
        self.compare_complete_refined_answers(4, 6, {}, 2, "pairs")

    def test_native_periodic_source(self):
        solve = self.require_candidate_function_exists("compute_refined_triangle_counts")
        length = 10**12
        classes = {(0, 1, 1): [(0, 1)], (1, 2, 2): [(1, 2)],
                   (0, 2, 3): [(0, 1)]}
        periods = {key: 2 for key in classes}
        rows, total, metrics = solve(3, length, classes, 2, "pairs", mask_periods=periods)
        self.assertEqual(total, length // 2)
        self.assertEqual(rows, [(2, 1), (0, 0), (0, 0), (2, 1), (0, 0), (2, 1)])
        self.assertEqual(metrics["refined_classes"], 3)
        self.assertEqual(metrics["source_runs"], 3)

    def test_native_periods_validation(self):
        solve = self.require_candidate_function_exists("compute_refined_triangle_counts")
        classes = {(0, 1, 0): [(0, 1)]}
        for periods in ({}, {(0, 1, 0): 4}, {(0, 1, 0): True},
                        {(0, 1, 0): 2, (0, 2, 0): 2}):
            with self.subTest(periods=periods), self.assertRaises(ValueError):
                solve(3, 6, classes, 2, mask_periods=periods)

    def test_native_mixed_periods(self):
        solve = self.require_candidate_function_exists("compute_refined_triangle_counts")
        expand = self.require_candidate_function_exists("iterate_refined_output_rows")
        classes = {(0, 1, 1): [(0, 1)], (1, 2, 2): [(0, 2)],
                   (0, 2, 3): [(0, 1)]}
        periods = {(0, 1, 1): 2, (1, 2, 2): 3, (0, 2, 3): 1}
        flat = {key: [(j + left, j + right) for j in range(0, 12, periods[key])
                       for left, right in runs] for key, runs in classes.items()}
        for backend in ("pairs", "convolution"):
            rows, total, _ = solve(3, 12, classes, 6, backend, mask_periods=periods)
            self.assertEqual((list(expand(3, 12, 6, rows)), total),
                             expand_masked_graph_counts(3, 12, flat))


if __name__ == "__main__":
    unittest.main()
