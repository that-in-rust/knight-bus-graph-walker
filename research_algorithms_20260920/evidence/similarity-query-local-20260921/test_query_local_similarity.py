"""Shared local plans must retain inactive feasibility and exact top-k output."""

import importlib
import importlib.util
import itertools
import random
import unittest
from fractions import Fraction

from probe_compressed_dual_similarity import compute_compressed_threshold_bound
from probe_compressed_interval_similarity import compute_compressed_interval_bounds
from probe_exception_core_similarity import compile_modal_capacity_core
from probe_query_skeleton_similarity import compile_query_skeleton_index, prepare_query_skeleton_core
from profile_large_group_similarity import prepare_large_group_source
from test_additive_core_similarity import construct_actual_pair_metadata
from test_bounded_seed_similarity import compute_finite_similarity_oracle
from test_laminar_capacity_similarity import compute_independent_group_counts
from test_query_skeleton_similarity import UnscannableChildSequence
from test_static_core_similarity import SinglePassPairStream


class QueryLocalSimilarityTests(unittest.TestCase):
    def load_required_local_module(self):
        name = "probe_query_local_similarity"
        self.assertIsNotNone(importlib.util.find_spec(name), "local inverse implementation missing")
        return importlib.import_module(name)

    def prepare_local_test_plan(self, module, static, a, minimum, pairs):
        index = compile_query_skeleton_index(static)
        core = prepare_query_skeleton_core(index, a, minimum, pairs)
        return module.build_skeleton_threshold_plan(index, core)

    def test_positive_offset_population(self):
        module = self.load_required_local_module()
        static = compile_modal_capacity_core([0, 4, 1, 3, 1, 0, 2, 2], [1, 0, 2, 2], query_limit=1)
        plan = self.prepare_local_test_plan(module, static, 1, 2, [(0, 1)])
        node = plan["nodes"][plan["root"]]
        self.assertEqual((node["population"], node["floor"], node["capacity"]), (5, 1, 4))
        self.assertEqual(module.evaluate_skeleton_interval_control(plan, 1, 2), Fraction(1, 4))
        self.assertEqual(module.evaluate_skeleton_threshold_plan(plan, 1, 2), Fraction(1, 4))
        self.assertEqual(compute_compressed_interval_bounds(static, 1, 2, [(0, 1)])["interval"], Fraction(1, 2))

    def test_complete_compatible_envelopes(self):
        module = self.load_required_local_module()
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
                index = compile_query_skeleton_index(static)
                for raw, outside in itertools.product(subsets, (False, True)):
                    query = raw | ({9} if outside else set())
                    counts = compute_independent_group_counts(query & union, groups)[groups:]
                    pairs = [(j, q) for j, q in enumerate(counts) if q]
                    core = prepare_query_skeleton_core(index, len(query), minimum, pairs)
                    plan = module.build_skeleton_threshold_plan(index, core)
                    expected = max(Fraction(len(query & row), len(query | row)) if query & row else Fraction()
                                   for row in possible)
                    actual = module.evaluate_skeleton_threshold_plan(plan, len(query), minimum)
                    control = module.evaluate_skeleton_interval_control(plan, len(query), minimum)
                    old = compute_compressed_interval_bounds(static, len(query), minimum, pairs)["interval"]
                    self.assertEqual(actual, expected)
                    self.assertLessEqual(actual, control)
                    self.assertLessEqual(control, old)
                    if core["strict_nodes"] == 0:
                        self.assertEqual(control, actual)
                    checked += 1
        self.assertEqual(checked, 29440)

    def test_deeper_fullcore_parity(self):
        module = self.load_required_local_module()
        rng = random.Random(9217031)
        for groups in (1, 2, 4, 8, 16, 64):
            for _ in range(30):
                rows = [set(j for j in range(80) if rng.randrange(3)) for _ in range(2)]
                union, minimum, caps, populations = construct_actual_pair_metadata(rows, groups)
                static = compile_modal_capacity_core(caps, populations, query_limit=80)
                query = set(rng.sample(range(85), 8))
                counts = compute_independent_group_counts(query & union, groups)[groups:]
                pairs = [(j, q) for j, q in enumerate(counts) if q]
                plan = self.prepare_local_test_plan(module, static, len(query), minimum, pairs)
                stats = {}
                actual = module.evaluate_skeleton_threshold_plan(plan, len(query), minimum, stats=stats)
                self.assertEqual(actual, compute_compressed_threshold_bound(static, len(query), minimum, pairs))
                self.assertEqual(stats["states"], stats["reserved_states"])
                self.assertEqual(stats["operations"], stats["reserved_operations"])

    def test_global_conflicts_localize(self):
        module = self.load_required_local_module()
        costs = set()
        for groups in (4, 16, 64, 256):
            caps = [0] * groups + [3] * groups
            for node in range(groups - 1, 0, -1):
                caps[node] = caps[2 * node] + caps[2 * node + 1] - (2 if node >= groups // 2 else 0)
            static = compile_modal_capacity_core(caps, [4] * groups, query_limit=2)
            index = compile_query_skeleton_index(static)
            guarded = [dict(node) for node in static["nodes"]]
            guarded[static["root"]]["children"] = UnscannableChildSequence()
            index["static"] = dict(static, nodes=tuple(guarded))
            stream = SinglePassPairStream([(0, 2), (1, 2)])
            core = prepare_query_skeleton_core(index, 4, caps[1], stream)
            plan = module.build_skeleton_threshold_plan(index, core)
            stats = {}
            actual = module.evaluate_skeleton_threshold_plan(plan, 4, caps[1], stats=stats)
            self.assertEqual(actual, Fraction(3, 2 * groups + 1))
            self.assertEqual(stream.iterations, 1)
            costs.add((len(plan["nodes"]), stats["states"], stats["operations"]))
        self.assertEqual(len(costs), 1)

    def test_threshold_budget_boundaries(self):
        module = self.load_required_local_module()
        static = compile_modal_capacity_core([0, 4, 3, 3], [4, 4], query_limit=2)
        plan = self.prepare_local_test_plan(module, static, 4, 4, [(0, 2), (1, 2)])
        stats = {}
        expected = module.evaluate_skeleton_threshold_plan(plan, 4, 4, stats=stats)
        for options in ({"state_cap": stats["states"]-1}, {"work_cap": stats["operations"]-1}):
            paid = {}
            with self.assertRaisesRegex(ValueError, "budget"):
                module.evaluate_skeleton_threshold_plan(plan, 4, 4, stats=paid, **options)
            self.assertEqual((paid["states"], paid["operations"]), (0, 0))
        self.assertEqual(module.evaluate_skeleton_threshold_plan(plan, 4, 4,
            state_cap=stats["states"], work_cap=stats["operations"]), expected)
        for options in ({"state_cap": True}, {"work_cap": -1}):
            with self.assertRaises(ValueError):
                module.evaluate_skeleton_threshold_plan(plan, 4, 4, **options)
        empty = self.prepare_local_test_plan(module, static, 0, 4, [])
        self.assertEqual(module.evaluate_skeleton_threshold_plan(empty, 0, 4), 0)
        for a, minimum in ((True, 4), (0, 9), (0, 1)):
            with self.assertRaises(ValueError):
                module.evaluate_skeleton_threshold_plan(empty, a, minimum)

    def test_complete_small_topk(self):
        module = self.load_required_local_module()
        subsets = [set(i for i in range(2) if mask & (1 << i)) for mask in range(4)]
        checked = 0
        for groups, rows in itertools.product((1, 4), itertools.product(subsets, repeat=3)):
            targets = list(zip((-3, 2, 7), rows))
            source = module.prepare_skeleton_source_view(prepare_large_group_source(targets, groups, query_limit=2))
            self.assertNotIn("target_posts", source)
            for block in source["blocks"]:
                self.assertNotIn("caps", block)
                self.assertNotIn("populations", block)
            for query, sid, k in itertools.product(subsets, (None, -3), (0, 1, 5)):
                expected = compute_finite_similarity_oracle(targets, query, sid, k)
                for mode in ("neutral", "dual"):
                    actual, stats = module.run_skeleton_similarity_query(source, query, sid, k, mode=mode)
                    self.assertEqual(actual, expected)
                    self.assertEqual(stats["dense_histogram_cells"], 0)
                    self.assertEqual(stats["skeleton_preparations"], stats["control_evaluations"])
                checked += 1
        self.assertEqual(checked, 3072)

    def test_refusal_exact_fallback(self):
        module = self.load_required_local_module()
        targets = [(0, {0, 1, 2, 77}), (1, {4, 5}), (2, {0, 1, 3}), (3, {0, 2, 3})]
        query = {0, 1, 2}
        for limit, options, metric in ((0, {}, "profile_refusals"),
                (10, {"skeleton_cap": 0}, "skeleton_refusals"),
                (10, {"state_cap": 0}, "threshold_refusals")):
            source = module.prepare_skeleton_source_view(prepare_large_group_source(targets, 4, query_limit=limit))
            actual, stats = module.run_skeleton_similarity_query(source, query, None, 1, **options)
            self.assertEqual(actual, compute_finite_similarity_oracle(targets, query, None, 1))
            self.assertGreater(stats[metric], 0)
        for options in ({"k": True}, {"k": 1, "work_cap": -1}, {"k": 1, "mode": "oops"}):
            with self.assertRaises(ValueError):
                module.run_skeleton_similarity_query(source, query, None, **options)


if __name__ == "__main__":
    unittest.main()
