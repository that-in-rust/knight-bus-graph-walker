"""Same-information compact controls, not a weakened dense comparator."""

import importlib
import importlib.util
import itertools
import random
import unittest
from fractions import Fraction

from probe_exception_core_similarity import compile_modal_capacity_core, prepare_exception_query_core
from probe_compressed_dual_similarity import compute_compressed_threshold_bound
from probe_laminar_capacity_similarity import compute_laminar_jaccard_bound
from probe_selective_capacity_similarity import compute_interval_capacity_bound
from test_additive_core_similarity import construct_actual_pair_metadata
from test_laminar_capacity_similarity import compute_independent_group_counts
from test_static_core_similarity import SinglePassPairStream


class CompressedIntervalSimilarityTests(unittest.TestCase):
    def load_required_interval_module(self):
        name = "probe_compressed_interval_similarity"
        self.assertIsNotNone(importlib.util.find_spec(name), "compressed interval implementation missing")
        return importlib.import_module(name)

    def compare_all_control_values(self, rows, query, groups):
        module = self.load_required_interval_module()
        union, minimum, caps, populations = construct_actual_pair_metadata(rows, groups)
        counts = compute_independent_group_counts(query & union, groups)[groups:]
        static = compile_modal_capacity_core(caps, populations, query_limit=max(counts, default=0))
        stats = {}
        values = module.compute_compressed_interval_bounds(static, len(query), minimum,
                    [(j, q) for j, q in enumerate(counts) if q], stats=stats)
        for mode in ("union", "laminar"):
            self.assertEqual(values[mode], compute_laminar_jaccard_bound(
                len(query), minimum, caps, counts, mode))
        self.assertEqual(values["interval"], compute_interval_capacity_bound(
            len(query), minimum, caps, counts, populations, 2))
        self.assertEqual(stats["control_nodes_visited"], len(static["nodes"]))
        self.assertEqual(stats["control_child_reductions"], len(static["nodes"]) - 1)
        self.assertEqual(stats["control_records_peak"], len(static["nodes"]))
        if static["strict_nodes"] == 0:
            self.assertEqual(values["interval"], compute_compressed_threshold_bound(
                static, len(query), minimum, [(j, q) for j, q in enumerate(counts) if q]))
        return values

    def test_bundle_preserves_occupancy(self):
        module = self.load_required_interval_module()
        core = compile_modal_capacity_core([0, 4, 2, 2], [3, 3], query_limit=3)
        for minimum, expected in ((2, Fraction(2, 5)), (3, Fraction(1, 2)), (4, Fraction(2, 5))):
            result = module.compute_compressed_interval_bounds(core, 3, minimum, [(1, 3)])
            self.assertEqual(result["interval"], expected)

    def test_complete_small_controls(self):
        subsets = [frozenset(i for i in range(4) if mask & (1 << i)) for mask in range(16)]
        checked = 0
        for rows in itertools.combinations_with_replacement(subsets, 2):
            for query, outside, groups in itertools.product(subsets, (False, True), (1, 2, 4, 8)):
                self.compare_all_control_values(rows, query | ({9} if outside else set()), groups)
                checked += 1
        self.assertEqual(checked, 17408)

    def test_seeded_deeper_controls(self):
        rng = random.Random(9214701)
        for groups in (2, 4, 8, 16, 64, 256):
            for _ in range(30):
                rows = [set(j for j in range(150) if rng.randrange(3)) for _ in range(2)]
                query = set(j for j in range(160) if rng.randrange(4) == 0)
                self.compare_all_control_values(rows, query, groups)

    def test_strict_companion_distinction(self):
        module = self.load_required_interval_module()
        static = compile_modal_capacity_core([0, 4, 3, 3], [4, 4], query_limit=2)
        result = module.compute_compressed_interval_bounds(static, 4, 4, [(0, 2), (1, 2)])
        self.assertEqual(result["interval"], Fraction(1))
        self.assertEqual(compute_compressed_threshold_bound(static, 4, 4, [(0, 2), (1, 2)]),
                         Fraction(3, 5))
        # The control does not assert a companion exists for every relaxed row.
        static = compile_modal_capacity_core([0, 3, 2, 2], [2, 2], query_limit=2)
        self.assertEqual(module.compute_compressed_interval_bounds(static, 0, 1, [])["interval"], 0)
        with self.assertRaisesRegex(ValueError, "realizes"):
            compute_compressed_threshold_bound(static, 0, 1, [])

    def test_fixed_conflict_resources(self):
        module = self.load_required_interval_module()
        resource_counts = set()
        for groups in (4, 16, 64, 256):
            caps = [0] * groups + [3, 3] + [2] * (groups - 2)
            for node in range(groups - 1, 0, -1):
                caps[node] = caps[2 * node] + caps[2 * node + 1] - (2 if node == groups // 2 else 0)
            static = compile_modal_capacity_core(caps, [4, 4] + [2] * (groups - 2), query_limit=2)
            stats, stream = {}, SinglePassPairStream([(0, 2), (1, 2)])
            result = module.compute_compressed_interval_bounds(static, 4, caps[1], stream, stats=stats)
            self.assertEqual(result["interval"], Fraction(4, caps[1]))
            self.assertEqual(stream.iterations, 1)
            resource_counts.add((stats["control_nodes_visited"], stats["control_child_reductions"],
                                 stats["control_records_peak"], stats["query_core_copies"]))
        self.assertEqual(resource_counts, {(4, 3, 4, 4)})

    def test_prepared_budget_boundaries(self):
        module = self.load_required_interval_module()
        static = compile_modal_capacity_core([0, 4, 3, 3], [4, 4], query_limit=2)
        size = len(static["nodes"])
        stream, refused = SinglePassPairStream([(0, 2), (1, 2)]), {}
        with self.assertRaisesRegex(ValueError, "record budget"):
            module.compute_compressed_interval_bounds(static, 4, 4, stream, record_cap=size-1, stats=refused)
        self.assertEqual(stream.iterations, 0)
        self.assertEqual(refused["query_core_copies"], 0)
        core = prepare_exception_query_core(static, 4, [(0, 2), (1, 2)])
        prepared = module.evaluate_prepared_interval_bounds(core, 4, 4, record_cap=size)
        self.assertEqual(prepared, module.compute_compressed_interval_bounds(
            static, 4, 4, [(0, 2), (1, 2)], record_cap=size))
        with self.assertRaisesRegex(ValueError, "record budget"):
            module.evaluate_prepared_interval_bounds(core, 4, 4, record_cap=size-1)

    def test_numeric_invalid_inputs(self):
        module = self.load_required_interval_module()
        huge = 10**100
        static = compile_modal_capacity_core([0, huge], [huge], query_limit=huge)
        result = module.compute_compressed_interval_bounds(static, huge, huge, [(0, huge)])
        self.assertEqual([result[key] for key in ("union", "laminar", "interval")], [1, 1, 1])
        for a, minimum, pairs, options in (
            (True, huge, [], {}), (0, -1, [], {}), (0, huge + 1, [], {}),
            (huge, huge, [(0, True)], {}), (huge, huge, [(0, 1), (0, 1)], {}),
            (huge, huge, [(0, huge + 1)], {}), (0, huge, [], {"record_cap": True}),
            (0, huge, [], {"record_cap": -1})):
            with self.assertRaises(ValueError):
                module.compute_compressed_interval_bounds(static, a, minimum, pairs, **options)


if __name__ == "__main__":
    unittest.main()
