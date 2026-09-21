"""Compact-core inverse frontiers, checked against complete compatible pairs."""

import importlib
import importlib.util
import itertools
import random
import unittest
from fractions import Fraction

from probe_exception_core_similarity import compile_modal_capacity_core, compute_exception_core_bound
from probe_overlap_frontier_similarity import compute_query_threshold_bound
from test_additive_core_similarity import construct_actual_pair_metadata
from test_laminar_capacity_similarity import compute_independent_group_counts
from test_static_core_similarity import SinglePassPairStream


class CompressedDualSimilarityTests(unittest.TestCase):
    def load_required_dual_module(self):
        name = "probe_compressed_dual_similarity"
        self.assertIsNotNone(importlib.util.find_spec(name), "compressed dual implementation missing")
        return importlib.import_module(name)

    def test_complete_pair_envelopes(self):
        module = self.load_required_dual_module()
        subsets = [frozenset(j for j in range(5) if mask & (1 << j)) for mask in range(32)]
        checked = 0
        for groups in (2, 4):
            families = {}
            for rows in itertools.combinations_with_replacement(subsets, 2):
                union, minimum, caps, _ = construct_actual_pair_metadata(rows, groups)
                families.setdefault((union, minimum, caps), set()).update(rows)
            for (union, minimum, caps), feasible in families.items():
                populations = compute_independent_group_counts(union, groups)[groups:]
                core = compile_modal_capacity_core(caps, populations, query_limit=5)
                for raw, outside in itertools.product(subsets, (False, True)):
                    query = raw | ({9} if outside else set())
                    counts = compute_independent_group_counts(query & union, groups)[groups:]
                    expected = max(Fraction(len(query & row), len(query | row))
                                   if query & row else Fraction() for row in feasible)
                    stats = {}
                    actual = module.compute_compressed_threshold_bound(
                        core, len(query), minimum, [(j, q) for j, q in enumerate(counts) if q], stats=stats)
                    self.assertEqual(actual, expected)
                    self.assertEqual(stats["states"], stats["reserved_states"])
                    self.assertEqual(stats["operations"], stats["reserved_operations"])
                    checked += 1
        self.assertEqual(checked, 29440)

    def test_bundle_forced_occupancy(self):
        module = self.load_required_dual_module()
        core = compile_modal_capacity_core([0, 4, 2, 2], [3, 3], query_limit=3)
        self.assertEqual(module.compute_compressed_threshold_bound(core, 3, 2, [(1, 3)]), Fraction(2, 5))
        self.assertEqual(module.compute_compressed_threshold_bound(core, 3, 3, [(1, 3)]), Fraction(1, 2))
        self.assertEqual(module.compute_compressed_threshold_bound(core, 3, 4, [(1, 3)]), Fraction(2, 5))

    def test_conflicted_small_query(self):
        module = self.load_required_dual_module()
        groups = 256
        caps = [0] * groups + [3] * groups
        for node in range(groups - 1, 0, -1):
            caps[node] = caps[2 * node] + caps[2 * node + 1] - (2 if node >= groups // 2 else 0)
        core = compile_modal_capacity_core(caps, [4] * groups, query_limit=2)
        self.assertEqual(core["strict_nodes"], 128)
        with self.assertRaisesRegex(ValueError, "budget"):
            compute_exception_core_bound(core, 4, caps[1], [(0, 2), (1, 2)])
        stats = {}
        self.assertEqual(module.compute_compressed_threshold_bound(
            core, 4, caps[1], [(0, 2), (1, 2)], state_cap=10000, work_cap=30000, stats=stats),
            Fraction(3, caps[1] + 1))
        self.assertLessEqual(stats["plan_nodes"], 2 * len(core["nodes"]) - 1)
        self.assertEqual(stats["orientations_enumerated"], 0)

    def test_fixed_conflict_scaling(self):
        module = self.load_required_dual_module()
        reservations = set()
        for groups in (4, 8, 16, 32, 64, 128, 256):
            caps = [0] * groups + [3, 3] + [2] * (groups - 2)
            for node in range(groups - 1, 0, -1):
                caps[node] = caps[2 * node] + caps[2 * node + 1] - (2 if node == groups // 2 else 0)
            core = compile_modal_capacity_core(caps, [4, 4] + [2] * (groups - 2), query_limit=2)
            stats, stream = {}, SinglePassPairStream([(0, 2), (1, 2)])
            self.assertEqual(module.compute_compressed_threshold_bound(core, 4, caps[1], stream, stats=stats),
                             Fraction(3, caps[1] + 1))
            self.assertEqual(stream.iterations, 1)
            reservations.add((stats["plan_nodes"], stats["reserved_states"], stats["reserved_operations"]))
        self.assertEqual(len(reservations), 1)

    def test_deeper_threshold_parity(self):
        module = self.load_required_dual_module()
        rng, checked = random.Random(93029), 0
        for groups in (1, 2, 4, 8, 16, 32):
            for _ in range(40):
                rows = [set(j for j in range(80) if rng.randrange(3)) for _ in range(2)]
                union, minimum, caps, populations = construct_actual_pair_metadata(rows, groups)
                core = compile_modal_capacity_core(caps, populations, query_limit=80)
                query = set(j for j in range(83) if rng.randrange(4) == 0)
                counts = compute_independent_group_counts(query & union, groups)[groups:]
                self.assertEqual(module.compute_compressed_threshold_bound(
                    core, len(query), minimum, [(j, q) for j, q in enumerate(counts) if q]),
                    compute_query_threshold_bound(len(query), minimum, caps, counts, populations))
                checked += 1
        self.assertEqual(checked, 240)

    def test_preallocation_budget_refusals(self):
        module = self.load_required_dual_module()
        core = compile_modal_capacity_core([0, 4, 3, 3], [4, 4], query_limit=2)
        stats = {}
        module.compute_compressed_threshold_bound(core, 4, 4, [(0, 2), (1, 2)], stats=stats)
        for budget in ("state", "work"):
            reservation = stats["reserved_states" if budget == "state" else "reserved_operations"]
            refused = {}
            with self.assertRaisesRegex(ValueError, "budget"):
                module.compute_compressed_threshold_bound(core, 4, 4, [(0, 2), (1, 2)],
                    **{budget + "_cap": reservation - 1}, stats=refused)
            self.assertEqual((refused["states"], refused["operations"]), (0, 0))
        huge = 10**100
        large = compile_modal_capacity_core([0, huge], [huge], query_limit=huge)
        refused = {}
        with self.assertRaisesRegex(ValueError, "budget"):
            module.compute_compressed_threshold_bound(large, huge, huge, [(0, huge)], stats=refused)
        self.assertEqual(refused["states"], 0)
        self.assertEqual(compute_exception_core_bound(large, huge, huge, [(0, huge)]), Fraction(1))

    def test_invalid_infeasible_queries(self):
        module = self.load_required_dual_module()
        core = compile_modal_capacity_core([0, 3, 2, 2], [2, 2], query_limit=2)
        self.assertEqual(module.compute_compressed_threshold_bound(core, 0, 2, []), Fraction())
        with self.assertRaisesRegex(ValueError, "realizes"):
            module.compute_compressed_threshold_bound(core, 0, 1, [])
        for pairs in ([(0, 3)], [(0, 1), (0, 1)], [(True, 1)], [(0, True)]):
            with self.assertRaises(ValueError):
                module.compute_compressed_threshold_bound(core, 4, 2, pairs)
        for options in ({"state_cap": True}, {"work_cap": -1}):
            with self.assertRaises(ValueError):
                module.compute_compressed_threshold_bound(core, 4, 2, [(0, 1)], **options)


if __name__ == "__main__":
    unittest.main()
