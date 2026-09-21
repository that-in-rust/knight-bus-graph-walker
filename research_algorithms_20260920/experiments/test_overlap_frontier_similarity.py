"""Complete-pair oracle for attained maxima with nonzero intersection."""

import importlib.util
import itertools
import random
import types
import unittest
from fractions import Fraction

from probe_laminar_capacity_similarity import compute_paired_capacity_bound
from probe_selective_capacity_similarity import compute_interval_capacity_bound
from test_laminar_capacity_similarity import compute_independent_group_counts

if importlib.util.find_spec("probe_overlap_frontier_similarity"):
    import probe_overlap_frontier_similarity as candidate
else:
    candidate = types.SimpleNamespace()


class OverlapFrontierTests(unittest.TestCase):
    def require_overlap_solver_function(self):
        solver = getattr(candidate, "compute_overlap_capacity_bound", None)
        self.assertTrue(callable(solver), "compute_overlap_capacity_bound")
        return solver

    def require_threshold_solver_function(self):
        solver = getattr(candidate, "compute_query_threshold_bound", None)
        self.assertTrue(callable(solver), "compute_query_threshold_bound")
        return solver

    def test_complete_overlap_envelopes(self):
        solve = self.require_overlap_solver_function()
        compact = self.require_threshold_solver_function()
        subsets = [frozenset(j for j in range(5) if mask & (1 << j)) for mask in range(32)]
        checked = 0
        for groups in (2, 4):
            families = {}
            for rows in itertools.combinations_with_replacement(subsets, 2):
                union = rows[0] | rows[1]
                caps = tuple(map(max, zip(*(compute_independent_group_counts(row, groups) for row in rows))))
                key = (union, min(map(len, rows)), caps)
                families.setdefault(key, set()).update(rows)
            for (union, minimum, caps), feasible in families.items():
                populations = compute_independent_group_counts(union, groups)[groups:]
                for raw, outside in itertools.product(subsets, (False, True)):
                    source = raw | ({9} if outside else set())
                    counts = compute_independent_group_counts(source & union, groups)[groups:]
                    expected = max(Fraction(len(source & row), len(source | row))
                                   if source & row else Fraction(0) for row in feasible)
                    actual = solve(len(source), minimum, caps, counts, populations)
                    interval = compute_interval_capacity_bound(len(source), minimum, caps, counts, populations, 2)
                    self.assertEqual(actual, expected, (groups, union, minimum, caps, source))
                    self.assertEqual(compact(len(source), minimum, caps, counts, populations), expected)
                    self.assertLessEqual(actual, interval)
                    if len(union) == minimum + caps[1]:
                        self.assertEqual(actual, compute_paired_capacity_bound(len(source), minimum, caps, counts, populations))
                    checked += 1
        self.assertEqual(checked, 29440)

    def test_local_shared_count_guard(self):
        solve = self.require_overlap_solver_function()
        self.assertEqual(solve(2, 2, [0, 2, 1, 1], [2, 0], [2, 1]), Fraction(1, 3))
        self.assertEqual(solve(0, 0, [0, 0], [0], [0]), 0)
        self.assertEqual(solve(2, 3, [0, 3], [2], [3]), Fraction(2, 3))

    def test_admission_before_enumeration(self):
        solve = self.require_overlap_solver_function()
        stats = {}
        with self.assertRaisesRegex(ValueError, "state"):
            solve(1, 10**12, [0, 10**12], [1], [10**12], state_cap=10, stats=stats)
        self.assertEqual(stats.get("states", 0), 0)
        with self.assertRaisesRegex(ValueError, "work"):
            solve(2, 2, [0, 2, 1, 1], [1, 1], [1, 1], work_cap=0)
        for args in ((True, 0, [0, 1], [0], [1]), (1, 0, [0, 1], [2], [1]),
                     (1, 0, [0, 1, 1, 1], [1, 0], [1, 1]),
                     (1, 0, [0, 1], [0], [3]), (1, 0, [0, 1.0], [0], [1])):
            with self.subTest(args=args), self.assertRaises(ValueError):
                solve(*args)

    def test_summary_space_accounting(self):
        solve = self.require_overlap_solver_function()
        stats = {}
        self.assertEqual(solve(2, 2, [0, 3, 2, 1], [1, 1], [2, 1], stats=stats), Fraction(1))
        self.assertGreater(stats["states"], 0)
        self.assertLessEqual(stats["states"], stats["reserved_states"])
        self.assertLessEqual(stats["transitions"], stats["reserved_transitions"])
        self.assertEqual(stats["intersection"], 2)

    def test_query_parameter_scale(self):
        compact = self.require_threshold_solver_function()
        for scale in (1, 16, 1000, 10**12):
            caps, counts, populations = [0, scale + 4, 4, scale, 3, 3, scale, 0], [2, 2, 0, 0], [4, 4, scale, 0]
            stats = {}
            self.assertEqual(compact(4, scale + 4, caps, counts, populations, state_cap=100, stats=stats), Fraction(3, scale + 5))
            self.assertEqual((stats["reserved_states"], stats["states"]), (38, 38))
            self.assertEqual((stats["operations"], stats["reserved_operations"]), (70, 70))
            if scale >= 1000:
                with self.assertRaisesRegex(ValueError, "state"):
                    self.require_overlap_solver_function()(4, scale + 4, caps, counts, populations, state_cap=100)
        self.assertEqual(compact(1, scale, [0, scale], [1], [scale], state_cap=4), Fraction(1, scale))
        with self.assertRaisesRegex(ValueError, "state"):
            compact(scale, scale, [0, scale], [scale], [scale], state_cap=10)
        with self.assertRaisesRegex(ValueError, "work"):
            compact(1, scale, [0, scale], [1], [scale], work_cap=0)

    def test_nonconcave_threshold_arrays(self):
        self.require_threshold_solver_function()
        build = candidate.build_query_threshold_frontiers
        self.assertEqual(build([0, 3, 1, 3], [1, 0], [1, 3])[1], ([0, 2], [0, 0]))
        self.assertEqual(build([0, 3, 1, 3], [0, 3], [1, 3])[1], ([0, 0, 0, 0], [0, 1, 2, 2]))

    def test_zero_threshold_feasibility(self):
        compact = self.require_threshold_solver_function()
        caps, counts, populations = [0, 3, 2, 2], [0, 0], [2, 2]
        self.assertEqual(candidate.build_query_threshold_frontiers(caps, counts, populations)[1], ([1], [1]))
        self.assertEqual(compact(0, 2, caps, counts, populations), 0)
        with self.assertRaisesRegex(ValueError, "no complete pair"):
            compact(0, 1, caps, counts, populations)

    def test_deeper_frontier_equivalence(self):
        compact = self.require_threshold_solver_function()
        solve = self.require_overlap_solver_function()
        generator = random.Random(920520)
        for _ in range(256):
            rows = [set(j for j in range(24) if generator.randrange(2)) for _ in range(2)]
            union = rows[0] | rows[1]
            caps = list(map(max, zip(*(compute_independent_group_counts(row, 8) for row in rows))))
            minimum = min(map(len, rows))
            populations = compute_independent_group_counts(union, 8)[8:]
            for _ in range(4):
                source = set(j for j in range(26) if generator.randrange(2))
                counts = compute_independent_group_counts(source & union, 8)[8:]
                args = len(source), minimum, caps, counts, populations
                stats = {}
                self.assertEqual(compact(*args, stats=stats), solve(*args))
                self.assertLessEqual(stats["operations"], stats["reserved_operations"])
                self.assertEqual(stats["states"], stats["reserved_states"])

    def test_covering_anchor_reduction(self):
        compact = self.require_threshold_solver_function()
        solve = self.require_overlap_solver_function()
        for weights in ((1,), (1, 3), (1, 3, 9), (2, 2, 3)):
            total = sum(weights)
            groups = 2
            while groups < 2 * (len(weights) + 1):
                groups *= 2
            caps, populations, counts = [0] * (2 * groups), [0] * groups, [0] * groups
            for j, weight in enumerate(weights):
                populations[2 * j:2 * j + 2] = weight, 2 * weight
                counts[2 * j] = weight
                caps[groups + 2 * j:groups + 2 * j + 2] = weight, 2 * weight
            populations[2 * len(weights)] = counts[2 * len(weights)] = 4 * total
            caps[groups + 2 * len(weights)] = 4 * total
            for node in range(groups - 1, 0, -1):
                caps[node] = caps[2 * node] + caps[2 * node + 1]
            for j, weight in enumerate(weights):
                caps[(groups + 2 * j) // 2] = 2 * weight
            for node in range(groups // 2 - 1, 0, -1):
                caps[node] = caps[2 * node] + caps[2 * node + 1]
            subset_sums = [sum(weight for j, weight in enumerate(weights) if mask & (1 << j))
                           for mask in range(1 << len(weights))]
            for target in range(1, total + 1):
                cover = min(value for value in subset_sums if value >= target)
                expected = Fraction(5 * total - cover, 6 * total + cover)
                args = 5 * total, 2 * total - target, caps, counts, populations
                self.assertEqual(compact(*args), expected)
                self.assertEqual(solve(*args), expected)


if __name__ == "__main__":
    unittest.main()
