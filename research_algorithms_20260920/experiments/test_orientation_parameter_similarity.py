"""Exact pair envelopes parameterized by strict orientation splits."""

import importlib.util
import itertools
import random
import types
import unittest
from fractions import Fraction

from probe_overlap_frontier_similarity import compute_overlap_capacity_bound, compute_query_threshold_bound
from probe_selective_capacity_similarity import compute_interval_capacity_bound
from test_laminar_capacity_similarity import compute_independent_group_counts

if importlib.util.find_spec("probe_orientation_parameter_similarity"):
    import probe_orientation_parameter_similarity as candidate
else:
    candidate = types.SimpleNamespace()


class OrientationParameterTests(unittest.TestCase):
    def require_orientation_solver_function(self):
        solve = getattr(candidate, "compute_orientation_capacity_bound", None)
        self.assertTrue(callable(solve), "compute_orientation_capacity_bound")
        return solve

    def test_actual_summary_envelopes(self):
        solve = self.require_orientation_solver_function()
        subsets = [frozenset(j for j in range(5) if mask & (1 << j)) for mask in range(32)]
        checked = 0
        for groups in (2, 4):
            families = {}
            for rows in itertools.combinations_with_replacement(subsets, 2):
                union = rows[0] | rows[1]
                caps = tuple(map(max, zip(*(compute_independent_group_counts(row, groups) for row in rows))))
                families.setdefault((union, min(map(len, rows)), caps), set()).update(rows)
            for (union, minimum, caps), feasible in families.items():
                populations = compute_independent_group_counts(union, groups)[groups:]
                for raw, outside in itertools.product(subsets, (False, True)):
                    source = raw | ({9} if outside else set())
                    counts = compute_independent_group_counts(source & union, groups)[groups:]
                    expected = max(Fraction(len(source & row), len(source | row))
                                   if source & row else Fraction(0) for row in feasible)
                    stats = {}
                    self.assertEqual(solve(len(source), minimum, caps, counts, populations, stats=stats), expected)
                    self.assertEqual(stats["branches_visited"], 1 << stats["strict_nodes"])
                    self.assertLessEqual(stats["node_visits"], stats["reserved_node_visits"])
                    self.assertLessEqual(stats["table_slots_peak"], 2 * groups - 1)
                    checked += 1
        self.assertEqual(checked, 29440)

    def test_large_both_cardinalities(self):
        solve = self.require_orientation_solver_function()
        for scale in (1, 10**6, 10**12):
            caps = [value * scale for value in (0, 20, 4, 16, 3, 3, 16, 0)]
            counts, populations = [2 * scale, 2 * scale, 0, 0], [4 * scale, 4 * scale, 16 * scale, 0]
            stats = {}
            args = 4 * scale, 20 * scale, caps, counts, populations
            self.assertEqual(solve(*args, branch_cap=2, work_cap=34, stats=stats), Fraction(1, 7))
            self.assertEqual(compute_interval_capacity_bound(*args, 2), Fraction(1, 5))
            self.assertEqual((stats["strict_nodes"], stats["branches_visited"]), (1, 2))
            self.assertEqual(stats["reserved_node_visits"], 34)
            if scale > 1:
                for old in (compute_overlap_capacity_bound, compute_query_threshold_bound):
                    with self.assertRaisesRegex(ValueError, "state"):
                        old(*args, state_cap=1000)

    def test_deeper_reference_comparison(self):
        solve = self.require_orientation_solver_function()
        generator = random.Random(921007)
        for _ in range(256):
            rows = [set(j for j in range(24) if generator.randrange(2)) for _ in range(2)]
            union = rows[0] | rows[1]
            caps = list(map(max, zip(*(compute_independent_group_counts(row, 8) for row in rows))))
            for _ in range(4):
                source = set(j for j in range(26) if generator.randrange(2))
                counts = compute_independent_group_counts(source & union, 8)[8:]
                populations = compute_independent_group_counts(union, 8)[8:]
                args = len(source), min(map(len, rows)), caps, counts, populations
                self.assertEqual(solve(*args), compute_overlap_capacity_bound(*args))

    def test_refusal_and_invalid_metadata(self):
        solve = self.require_orientation_solver_function()
        args = 4, 20, [0, 20, 4, 16, 3, 3, 16, 0], [2, 2, 0, 0], [4, 4, 16, 0]
        for options, reason in (({"branch_cap": 1}, "branch"), ({"work_cap": 33}, "work")):
            stats = {}
            with self.assertRaisesRegex(ValueError, reason):
                solve(*args, stats=stats, **options)
            self.assertEqual(stats["branches_visited"], 0)
        for options in ({"branch_cap": True}, {"work_cap": -1}):
            with self.assertRaises(ValueError):
                solve(*args, **options)
        with self.assertRaisesRegex(ValueError, "no complete pair"):
            solve(0, 1, [0, 3, 2, 2], [0, 0], [2, 2])
        self.assertEqual(solve(0, 0, [0, 0], [0], [0]), 0)
        self.assertEqual(solve(10**12, 10**12, [0, 10**12], [10**12], [10**12]), 1)


if __name__ == "__main__":
    unittest.main()
