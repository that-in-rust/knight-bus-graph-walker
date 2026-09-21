"""Query-local conflicts must preserve complete compatible-pair envelopes."""

import importlib
import importlib.util
import itertools
import random
import unittest
from fractions import Fraction

from probe_exception_core_similarity import compile_modal_capacity_core, compute_exception_core_bound
from probe_compressed_dual_similarity import compute_compressed_threshold_bound
from test_additive_core_similarity import construct_actual_pair_metadata
from test_laminar_capacity_similarity import compute_independent_group_counts
from test_static_core_similarity import SinglePassPairStream


class UnscannableChildSequence:
    def __iter__(self):
        raise AssertionError("query scanned inactive boundary children")


class QuerySkeletonSimilarityTests(unittest.TestCase):
    def load_required_skeleton_module(self):
        name = "probe_query_skeleton_similarity"
        self.assertIsNotNone(importlib.util.find_spec(name), "query skeleton implementation missing")
        return importlib.import_module(name)

    def test_inactive_shared_floor(self):
        module = self.load_required_skeleton_module()
        static = compile_modal_capacity_core([0, 4, 1, 3, 1, 0, 2, 2], [1, 0, 2, 2], query_limit=1)
        index = module.compile_query_skeleton_index(static)
        stats = {}
        actual = module.compute_query_skeleton_bound(index, 1, 2, [(0, 1)], stats=stats)
        self.assertEqual(actual, Fraction(1, 4))
        self.assertEqual(stats["local_strict_nodes"], 0)
        self.assertEqual(stats["skeleton_nodes"], 1)
        self.assertEqual(index["shared"][static["root"]], 1)

    def test_complete_pair_envelopes(self):
        module = self.load_required_skeleton_module()
        subsets = [frozenset(j for j in range(5) if mask & (1 << j)) for mask in range(32)]
        checked = 0
        for groups in (2, 4):
            families = {}
            for rows in itertools.combinations_with_replacement(subsets, 2):
                union, minimum, caps, _ = construct_actual_pair_metadata(rows, groups)
                families.setdefault((union, minimum, caps), set()).update(rows)
            for (union, minimum, caps), possible in families.items():
                populations = compute_independent_group_counts(union, groups)[groups:]
                static = compile_modal_capacity_core(caps, populations, query_limit=5)
                index = module.compile_query_skeleton_index(static)
                for raw, outside in itertools.product(subsets, (False, True)):
                    query = raw | ({9} if outside else set())
                    counts = compute_independent_group_counts(query & union, groups)[groups:]
                    expected = max(Fraction(len(query & row), len(query | row)) if query & row else Fraction()
                                   for row in possible)
                    self.assertEqual(module.compute_query_skeleton_bound(index, len(query), minimum,
                        [(j, q) for j, q in enumerate(counts) if q]), expected)
                    checked += 1
        self.assertEqual(checked, 29440)

    def test_global_conflicts_disappear(self):
        module = self.load_required_skeleton_module()
        counts = set()
        for groups in (4, 16, 64, 256):
            caps = [0] * groups + [3] * groups
            for node in range(groups - 1, 0, -1):
                caps[node] = caps[2 * node] + caps[2 * node + 1] - (2 if node >= groups // 2 else 0)
            static = compile_modal_capacity_core(caps, [4] * groups, query_limit=2)
            index = module.compile_query_skeleton_index(static)
            guarded = [dict(node) for node in static["nodes"]]
            guarded[static["root"]]["children"] = UnscannableChildSequence()
            index["static"] = dict(static, nodes=tuple(guarded))
            stats, stream = {}, SinglePassPairStream([(0, 2), (1, 2)])
            value = module.compute_query_skeleton_bound(index, 4, caps[1], stream, stats=stats)
            self.assertEqual(value, Fraction(3, 2 * groups + 1))
            self.assertEqual(stream.iterations, 1)
            counts.add((stats["local_strict_nodes"], stats["skeleton_nodes"],
                        stats["branches_visited"], stats["branch_work"]))
            if groups == 256:
                with self.assertRaisesRegex(ValueError, "budget"):
                    compute_exception_core_bound(static, 4, caps[1], [(0, 2), (1, 2)])
                self.assertEqual(value, compute_compressed_threshold_bound(static, 4, caps[1], [(0, 2), (1, 2)]))
        self.assertEqual(len(counts), 1)
        self.assertEqual(next(iter(counts))[:3], (1, 4, 2))

    def test_deeper_envelope_parity(self):
        module = self.load_required_skeleton_module()
        rng = random.Random(9216301)
        checked = 0
        for groups in (1, 2, 4, 8, 16, 64):
            for _ in range(30):
                rows = [set(j for j in range(80) if rng.randrange(3)) for _ in range(2)]
                union, minimum, caps, populations = construct_actual_pair_metadata(rows, groups)
                static = compile_modal_capacity_core(caps, populations, query_limit=80)
                index = module.compile_query_skeleton_index(static)
                query = set(rng.sample(range(85), 4))
                counts = compute_independent_group_counts(query & union, groups)[groups:]
                pairs = [(j, q) for j, q in enumerate(counts) if q]
                stats = {}
                self.assertEqual(module.compute_query_skeleton_bound(index, len(query), minimum, pairs, stats=stats),
                                 compute_compressed_threshold_bound(static, len(query), minimum, pairs))
                self.assertLessEqual(stats["marked_nodes"], len(static["nodes"]))
                self.assertLessEqual(stats["marked_nodes"], len(pairs) * groups.bit_length())
                checked += 1
        self.assertEqual(checked, 180)

    def test_empty_invalid_metadata(self):
        module = self.load_required_skeleton_module()
        static = compile_modal_capacity_core([0, 3, 2, 2], [2, 2], query_limit=2)
        index = module.compile_query_skeleton_index(static)
        stats = {}
        self.assertEqual(module.compute_query_skeleton_bound(index, 0, 2, [], stats=stats), 0)
        self.assertEqual((stats["marked_nodes"], stats["skeleton_nodes"]), (0, 1))
        with self.assertRaisesRegex(ValueError, "realizes"):
            module.compute_query_skeleton_bound(index, 0, 1, [])
        for pairs in ([(0, 3)], [(0, 1), (0, 1)], [(1, 1), (0, 1)], [(True, 1)], [(0, True)]):
            with self.assertRaises(ValueError):
                module.compute_query_skeleton_bound(index, 4, 2, pairs)
        for options in ({"branch_cap": True}, {"work_cap": -1}, {"skeleton_cap": True}):
            with self.assertRaises(ValueError):
                module.compute_query_skeleton_bound(index, 4, 2, [(0, 1)], **options)

    def test_prebranch_budget_refusal(self):
        module = self.load_required_skeleton_module()
        static = compile_modal_capacity_core([0, 4, 3, 3], [4, 4], query_limit=2)
        index = module.compile_query_skeleton_index(static)
        stats = {}
        module.compute_query_skeleton_bound(index, 4, 4, [(0, 2), (1, 2)], stats=stats)
        for options in ({"branch_cap": 1}, {"work_cap": stats["reserved_branch_work"]-1}):
            paid = {}
            with self.assertRaisesRegex(ValueError, "budget"):
                module.compute_query_skeleton_bound(index, 4, 4, [(0, 2), (1, 2)], stats=paid, **options)
            self.assertEqual(paid["branches_visited"], 0)
            self.assertGreater(paid["skeleton_nodes"], 0)
        with self.assertRaisesRegex(ValueError, "skeleton budget"):
            module.compute_query_skeleton_bound(index, 4, 4, [(0, 2), (1, 2)], skeleton_cap=1)


if __name__ == "__main__":
    unittest.main()
