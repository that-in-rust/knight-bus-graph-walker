"""Bounded A05 static compilation and streamed sparse-query contracts."""

import gc
import importlib
import importlib.util
import itertools
import random
import unittest
import weakref
from fractions import Fraction
from types import MappingProxyType
from unittest.mock import patch

from probe_additive_core_similarity import (
    build_additive_orientation_core,
    compute_additive_core_bound,
)
from probe_orientation_parameter_similarity import compute_orientation_capacity_bound
from test_additive_core_similarity import construct_actual_pair_metadata
from test_laminar_capacity_similarity import compute_independent_group_counts


def construct_fixed_conflict_fixture(groups):
    populations = [4, 4] + [2] * (groups - 2)
    capacities = [0] * groups + [3, 3] + [2] * (groups - 2)
    for node in range(groups - 1, 0, -1):
        capacities[node] = (capacities[2 * node] + capacities[2 * node + 1]
                            - (2 if node == groups // 2 else 0))
    return capacities, populations


class IndexedLeafTableGuard:
    def __init__(self, table, expected):
        self.table = table
        self.expected = expected
        self.lookups = 0

    def __len__(self):
        raise AssertionError("query must use the compiled group count")

    def __iter__(self):
        raise AssertionError("full leaf-table scan")

    def __getitem__(self, index):
        if type(index) is not int or self.lookups >= len(self.expected):
            raise AssertionError("unexpected lookup or table slice")
        if index != self.expected[self.lookups]:
            raise AssertionError("query inspected a leaf outside the sparse stream")
        self.lookups += 1
        return self.table[index]


class SinglePassPairStream:
    def __init__(self, pairs):
        self.pairs = pairs
        self.iterations = 0

    def __iter__(self):
        self.iterations += 1
        if self.iterations != 1:
            raise AssertionError("query iterable consumed twice")
        yield from self.pairs


class StaticCoreSimilarityTests(unittest.TestCase):
    def load_required_static_module(self):
        name = "probe_static_core_similarity"
        self.assertIsNotNone(importlib.util.find_spec(name), "static-core implementation missing")
        return importlib.import_module(name)

    def test_compile_zero_query(self):
        module = self.load_required_static_module()
        capacities, populations = construct_fixed_conflict_fixture(16)
        stats = {}
        with patch.object(module, "build_additive_orientation_core",
                          wraps=build_additive_orientation_core) as builder:
            core = module.compile_static_capacity_core(capacities, populations, stats=stats)
        self.assertEqual(builder.call_count, 1)
        self.assertEqual(tuple(builder.call_args.args[1]), (0,) * 16)
        self.assertEqual(core["query_union_size"], 0)
        self.assertEqual((core["groups"], len(core["leaf_table"])), (16, 16))
        self.assertEqual(stats["preprocessing_node_visits"], 31)
        self.assertEqual(stats["ownership_node_visits"], 31)
        self.assertEqual(stats["static_leaf_records"], 16)
        self.assertEqual((core["strict_nodes"], len(core["nodes"])), (1, 4))
        for node in core["nodes"]:
            if node["bundle"] is not None:
                lower, upper, intercept, ceiling = node["bundle"]
                self.assertEqual((intercept, ceiling), (lower, 0))
                self.assertLessEqual(lower, upper)

    def test_complete_actual_envelopes(self):
        module = self.load_required_static_module()
        subsets = [frozenset(j for j in range(5) if mask & (1 << j)) for mask in range(32)]
        checked = 0
        for groups in (2, 4):
            families = {}
            for rows in itertools.combinations_with_replacement(subsets, 2):
                union, minimum, capacities, _ = construct_actual_pair_metadata(rows, groups)
                families.setdefault((union, minimum, capacities), set()).update(rows)
            for (union, minimum, capacities), feasible in families.items():
                populations = compute_independent_group_counts(union, groups)[groups:]
                core = module.compile_static_capacity_core(capacities, populations)
                for raw, outside in itertools.product(subsets, (False, True)):
                    query = raw | ({9} if outside else set())
                    counts = compute_independent_group_counts(query & union, groups)[groups:]
                    expected = max(Fraction(len(query & row), len(query | row))
                                   if query & row else Fraction() for row in feasible)
                    pairs = ((index, q) for index, q in enumerate(counts) if q)
                    actual = module.compute_static_core_bound(core, len(query), minimum, pairs)
                    args = len(query), minimum, capacities, counts, populations
                    self.assertEqual(actual, expected)
                    self.assertEqual(actual, compute_additive_core_bound(*args))
                    self.assertEqual(actual, compute_orientation_capacity_bound(*args))
                    checked += 1
        self.assertEqual(checked, 29440)

    def test_deeper_reference_parity(self):
        module = self.load_required_static_module()
        rng = random.Random(921095)
        checked = 0
        for _ in range(256):
            rows = [set(j for j in range(24) if rng.randrange(2)) for _ in range(2)]
            union, minimum, capacities, populations = construct_actual_pair_metadata(rows, 8)
            core = module.compile_static_capacity_core(capacities, populations)
            for _ in range(4):
                query = set(j for j in range(26) if rng.randrange(2))
                counts = compute_independent_group_counts(query & union, 8)[8:]
                pairs = ((index, q) for index, q in enumerate(counts) if q)
                actual = module.compute_static_core_bound(core, len(query), minimum, pairs)
                args = len(query), minimum, capacities, counts, populations
                self.assertEqual(actual, compute_additive_core_bound(*args))
                self.assertEqual(actual, compute_orientation_capacity_bound(*args))
                checked += 1
        self.assertEqual(checked, 1024)

    def test_prepared_bundle_parity(self):
        module = self.load_required_static_module()
        rng = random.Random(921096)
        cases = [([{0, 4, 2}, {1, 3, 7}], {0, 1, 2, 3}),
                 ([{0, 2}, {1, 3}], {0, 3})]
        for _ in range(128):
            cases.append(([set(j for j in range(8) if rng.randrange(2)) for _ in range(2)],
                          set(j for j in range(9) if rng.randrange(2))))
        strict_seen, zero_bundles = set(), 0
        for rows, query in cases:
            union, _, capacities, populations = construct_actual_pair_metadata(rows, 4)
            counts = compute_independent_group_counts(query & union, 4)[4:]
            core = module.compile_static_capacity_core(capacities, populations)
            expected = build_additive_orientation_core(capacities, counts, populations)
            actual = module.prepare_static_query_core(
                core, len(query), ((index, q) for index, q in enumerate(counts) if q))
            for key in expected:
                self.assertEqual(actual[key], expected[key])
            strict_seen.add(core["strict_nodes"])
            zero_bundles += sum(node["bundle"] == (0, 0, 0, 0) and bool(node["children"])
                                for node in core["nodes"])
            for index, (owner, lower, upper) in enumerate(core["leaf_table"]):
                node = core["nodes"][owner]
                self.assertIsNone(node["strict_bit"])
                original = 4 + index
                while original > node["original_node"]:
                    original //= 2
                self.assertEqual(original, node["original_node"])
                self.assertEqual((lower, upper), (populations[index] - capacities[4 + index],
                                                 capacities[4 + index]))
        self.assertEqual(strict_seen, {0, 1, 2, 3})
        self.assertGreater(zero_bundles, 0)

    def test_positive_intercept_preserved(self):
        module = self.load_required_static_module()
        core = module.compile_static_capacity_core([0, 4, 2, 2], [3, 3])
        prepared = module.prepare_static_query_core(core, 3, iter([(1, 3)]))
        self.assertEqual(prepared["nodes"][0]["bundle"], (2, 4, 1, 2))
        self.assertEqual(core["nodes"][0]["bundle"], (2, 4, 2, 0))
        self.assertEqual(module.compute_static_core_bound(core, 3, 2, [(1, 3)]), Fraction(2, 5))

    def test_indexed_singlepass_queries(self):
        module = self.load_required_static_module()
        for groups in (4, 8, 16, 32, 64, 128, 256):
            capacities, populations = construct_fixed_conflict_fixture(groups)
            core = module.compile_static_capacity_core(capacities, populations)
            for pairs in ([(0, 2), (1, 2)], [], [(groups - 1, 1)]):
                guarded = IndexedLeafTableGuard(core["leaf_table"], [index for index, _ in pairs])
                wrapped = MappingProxyType(dict(core, leaf_table=guarded))
                stream, stats = SinglePassPairStream(pairs), {}
                counts = [0] * groups
                for index, q in pairs:
                    counts[index] = q
                a = sum(counts) + 1
                expected = compute_additive_core_bound(a, capacities[1], capacities, counts, populations)
                with patch.object(module, "build_additive_orientation_core",
                                  side_effect=AssertionError("query recompiled the original tree")):
                    actual = module.compute_static_core_bound(wrapped, a, capacities[1], stream, stats=stats)
                self.assertEqual(actual, expected)
                self.assertEqual(stream.iterations, 1)
                self.assertEqual(guarded.lookups, len(pairs))
                self.assertEqual(stats["query_leaf_lookups"], len(pairs))
                self.assertEqual(stats["query_core_copies"], 4)
                self.assertEqual(stats["row_record_slots_peak"], 4)
                self.assertEqual(stats["reserved_branch_work"], 48)
                self.assertEqual(stats["branch_work"], 48)

    def test_huge_count_parity(self):
        module = self.load_required_static_module()
        for scale in (1, 10**12, 10**80):
            capacities = [value * scale for value in (0, 20, 4, 16, 3, 3, 16, 0)]
            populations = [4 * scale, 4 * scale, 16 * scale, 0]
            counts = [2 * scale, 2 * scale, 0, 0]
            core = module.compile_static_capacity_core(capacities, populations)
            for outside in (0, 7 * scale):
                a = 4 * scale + outside
                actual = module.compute_static_core_bound(
                    core, a, 20 * scale, ((index, q) for index, q in enumerate(counts) if q))
                args = a, 20 * scale, capacities, counts, populations
                self.assertEqual(actual, compute_additive_core_bound(*args))
                self.assertEqual(actual, compute_orientation_capacity_bound(*args))
                self.assertEqual(actual, Fraction(3 * scale, 21 * scale + outside))

    def test_empty_singleleaf_ties(self):
        module = self.load_required_static_module()
        empty = module.compile_static_capacity_core([0, 0], [0])
        for a in (0, 10**80):
            self.assertEqual(module.compute_static_core_bound(empty, a, 0, iter(())), 0)
        tied = module.compile_static_capacity_core([0, 2], [3])
        for q in range(4):
            for outside in (0, 2):
                for minimum in (1, 2):
                    pairs = [] if q == 0 else [(0, q)]
                    expected = compute_additive_core_bound(q + outside, minimum, [0, 2], [q], [3])
                    self.assertEqual(module.compute_static_core_bound(tied, q + outside, minimum, pairs), expected)

    def test_infeasible_companion_rejected(self):
        module = self.load_required_static_module()
        core = module.compile_static_capacity_core([0, 2, 1, 2], [1, 2])
        stats = {}
        self.assertEqual(module.compute_static_core_bound(core, 2, 1, [(0, 1), (1, 1)], stats=stats),
                         Fraction(1, 2))
        self.assertEqual(stats["feasible_branches"], 1)
        impossible = module.compile_static_capacity_core([0, 3, 2, 2], [2, 2])
        snapshot = repr(impossible)
        with self.assertRaisesRegex(ValueError, "no complete pair"):
            module.compute_static_core_bound(impossible, 0, 1, [])
        self.assertEqual(repr(impossible), snapshot)

    def test_exact_budget_refusals(self):
        module = self.load_required_static_module()
        core = module.compile_static_capacity_core([0, 4, 3, 3], [4, 4])
        snapshot = repr(core)
        stats = {}
        result = module.compute_static_core_bound(core, 4, 4, [(0, 2), (1, 2)], stats=stats)
        branches, work = stats["reserved_branches"], stats["reserved_branch_work"]
        self.assertEqual((branches, work), (2, 34))
        self.assertEqual(module.compute_static_core_bound(
            core, 4, 4, [(0, 2), (1, 2)], branch_cap=branches, work_cap=work), result)
        for options, reason in (({"branch_cap": branches - 1}, "branch"),
                                ({"branch_cap": 0}, "branch"),
                                ({"work_cap": work - 1}, "work"), ({"work_cap": 0}, "work")):
            stream = SinglePassPairStream([(0, 2), (1, 2)])
            with patch.object(module, "orient_additive_core_labels",
                              side_effect=AssertionError("orientation before admission")):
                with self.assertRaisesRegex(ValueError, reason):
                    module.compute_static_core_bound(core, 4, 4, stream, stats=stats, **options)
            self.assertEqual((stats["branches_visited"], stats["branch_work"]), (0, 0))
            self.assertEqual((stats["query_core_copies"], stats["query_leaf_lookups"]), (0, 0))
            self.assertEqual(stream.iterations, 0)
            self.assertEqual(repr(core), snapshot)
        additive = module.compile_static_capacity_core([0, 2], [3])
        self.assertEqual(module.compute_static_core_bound(
            additive, 1, 1, [(0, 1)], branch_cap=1, work_cap=5), 1)
        with self.assertRaisesRegex(ValueError, "work"):
            module.compute_static_core_bound(additive, 1, 1, [(0, 1)], work_cap=4)

    def test_exponential_budget_factoring(self):
        module = self.load_required_static_module()
        groups = 256
        capacities = [0] * groups + [groups] * groups
        for node in range(groups - 1, 0, -1):
            capacities[node] = capacities[2 * node] + capacities[2 * node + 1] - 1
        core = module.compile_static_capacity_core(capacities, [groups] * groups)
        stats = {}
        with patch.object(module, "orient_additive_core_labels",
                          side_effect=AssertionError("must refuse without enumeration")):
            with self.assertRaisesRegex(ValueError, "branch"):
                module.compute_static_core_bound(core, 0, groups - 1, [], stats=stats)
        self.assertEqual(stats["reserved_branches"], 1 << 255)
        self.assertEqual(stats["branches_visited"], 0)

    def test_invalid_sparse_pairs(self):
        module = self.load_required_static_module()
        core = module.compile_static_capacity_core([0, 4, 3, 3], [4, 4])
        snapshot = repr(core)

        class NonBuiltinIntegerFixture(int):
            pass

        bad_queries = [None, 3, [None], [(0,)], [(0, 1, 2)], ["01"],
                       [(-1, 1)], [(2, 1)], [(True, 1)], [(0.0, 1)],
                       [(NonBuiltinIntegerFixture(0), 1)], [(0, NonBuiltinIntegerFixture(1))],
                       [(0, True)], [(0, 1.0)], [(0, "1")], [(0, 0)], [(0, -1)],
                       [(0, 5)], [(0, 1), (0, 1)], [(1, 1), (0, 1)], [(0, 3), (1, 2)]]
        for queries in bad_queries:
            with self.subTest(queries=queries):
                stats = {}
                with self.assertRaises(ValueError):
                    module.compute_static_core_bound(core, 4, 4, queries, stats=stats)
                self.assertEqual(stats["branches_visited"], 0)
                self.assertEqual(repr(core), snapshot)

    def test_invalid_scalar_inputs(self):
        module = self.load_required_static_module()
        core = module.compile_static_capacity_core([0, 4, 3, 3], [4, 4])

        class NonBuiltinIntegerFixture(int):
            pass

        for name in ("a", "minimum", "branch_cap", "work_cap"):
            for value in (-1, True, 4.0, "4", None, NonBuiltinIntegerFixture(4)):
                options = dict(a=4, minimum=4, branch_cap=100, work_cap=1000)
                options[name] = value
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    module.compute_static_core_bound(core, leaf_queries=[], **options)
        for a, minimum in ((4, 5), (4, 3), (1, 4)):
            with self.assertRaises(ValueError):
                module.compute_static_core_bound(core, a, minimum, [(0, 2)])
        for a in (True, -1, 1.0):
            with self.assertRaises(ValueError):
                module.prepare_static_query_core(core, a, [])

    def test_static_metadata_validation(self):
        module = self.load_required_static_module()
        cases = [([], []), ([0] * 6, [0] * 3), ([0] * 1024, [0] * 512),
                 ([0, 1, 1], [1]), ([1, 1], [1]), ([0, -1], [0]),
                 ([0, 1], [True]), ([0, True], [1]), ([0, 1.0], [1]),
                 ([0, 1], [1.0]), ([0, 2], [1]), ([0, 1], [3]),
                 ([0, 2, 3, 1], [3, 1]), ([0, 3, 1, 1], [2, 2])]
        for capacities, populations in cases:
            with self.subTest(capacities=capacities[:8]), self.assertRaises(ValueError):
                module.compile_static_capacity_core(capacities, populations)

    def test_static_ownership_immutability(self):
        module = self.load_required_static_module()

        class WeakReferenceListFixture(list):
            pass

        enabled = gc.isenabled()
        gc.disable()
        try:
            inputs = [WeakReferenceListFixture(values) for values in ([0, 4, 3, 3], [4, 4])]
            references = [weakref.ref(values) for values in inputs]
            core = module.compile_static_capacity_core(*inputs)
            inputs[0][1] = 999
            inputs[1][0] = 999
            del inputs
            self.assertTrue(all(reference() is None for reference in references))
            self.assertEqual(core["major_size"], 4)
        finally:
            if enabled:
                gc.enable()
            gc.collect()
        with self.assertRaises(TypeError):
            core["major_size"] = 0
        with self.assertRaises(TypeError):
            core["nodes"][0]["capacity"] = 0
        with self.assertRaises(TypeError):
            core["nodes"][0]["bundle"][0] = 0
        with self.assertRaises(TypeError):
            core["leaf_table"][0] = (0, 0, 0)
        with self.assertRaises(TypeError):
            core["leaf_table"][0][1] = 0
        with self.assertRaises(TypeError):
            core["nodes"][-1]["children"][0] = 0
        snapshot = repr(core)
        prepared = module.prepare_static_query_core(core, 4, [(0, 2), (1, 2)])
        self.assertNotIn("leaf_table", prepared)
        for original, copy in zip(core["nodes"], prepared["nodes"]):
            self.assertIsNot(original, copy)
            copy["capacity"] = 999
        for pairs, a in (([(0, 4)], 4), ([], 9), ([(1, 3)], 7), ([(0, 2), (1, 2)], 4)):
            module.compute_static_core_bound(core, a, 4, iter(pairs))
            self.assertEqual(repr(core), snapshot)

    def test_interrupted_reentrant_streams(self):
        module = self.load_required_static_module()
        core = module.compile_static_capacity_core([0, 4, 3, 3], [4, 4])
        snapshot = repr(core)

        def interrupt_sparse_query_stream():
            yield 0, 2
            raise RuntimeError("source failed")

        with self.assertRaisesRegex(RuntimeError, "source failed"):
            module.compute_static_core_bound(core, 4, 4, interrupt_sparse_query_stream())
        self.assertEqual(repr(core), snapshot)

        def interleave_sparse_query_stream():
            yield 0, 2
            self.assertEqual(module.compute_static_core_bound(core, 0, 4, []), 0)
            yield 1, 2

        stream = interleave_sparse_query_stream()
        reference = weakref.ref(stream)
        self.assertEqual(module.compute_static_core_bound(core, 4, 4, stream), Fraction(3, 5))
        del stream
        self.assertIsNone(reference())
        self.assertEqual(repr(core), snapshot)


if __name__ == "__main__":
    unittest.main()
