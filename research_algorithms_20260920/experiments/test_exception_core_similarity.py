"""Query-limit exception summaries, including the rejected floor-only idea."""

import gc
import importlib
import importlib.util
import itertools
import math
import random
import unittest
import weakref
from fractions import Fraction
from unittest.mock import patch

from probe_additive_core_similarity import build_additive_orientation_core, compute_additive_core_bound
from test_additive_core_similarity import construct_actual_pair_metadata
from test_laminar_capacity_similarity import compute_independent_group_counts
from test_static_core_similarity import SinglePassPairStream


def construct_marked_capacity_tree(groups, strict, scale=1):
    capacities = [0] * groups + [3 * scale] * groups
    for node in range(groups - 1, 0, -1):
        capacities[node] = capacities[2 * node] + capacities[2 * node + 1] - int(node in strict) * scale
    return capacities, [4 * scale] * groups


def find_reference_leaf_owner(leaf, groups, strict):
    node = leaf + groups
    while node > 1 and node // 2 not in strict:
        node //= 2
    return node


class ExceptionCoreSimilarityTests(unittest.TestCase):
    def setUp(self):
        name = "probe_exception_core_similarity"
        self.assertIsNotNone(importlib.util.find_spec(name), "exception-core implementation missing")
        self.module = importlib.import_module(name)

    def test_all_owner_patterns(self):
        checked = 0
        for groups in (1, 2, 4, 8):
            for bits in range(1 << (groups - 1)):
                strict = {node for node in range(1, groups) if bits >> (node - 1) & 1}
                capacities, populations = construct_marked_capacity_tree(groups, strict)
                core = self.module.compile_exception_capacity_core(capacities, populations, query_limit=1)
                actual = [core["nodes"][owner]["original_node"]
                          for a, b, owner in core["owner_runs"] for _ in range(a, b)]
                self.assertEqual(actual, [find_reference_leaf_owner(j, groups, strict) for j in range(groups)])
                self.assertEqual(core["strict_nodes"], len(strict))
                self.assertLessEqual(len(core["owner_runs"]), 3 * len(strict) + 1)
                self.assertEqual(core["exceptions"], ())
                self.assertNotIn("leaf_table", core)
                checked += 1
        self.assertEqual(checked, 139)

    def test_sharp_owner_holes(self):
        for groups, strict, expected in ((8, {5}, 4), (16, {9, 11}, 7)):
            capacities, populations = construct_marked_capacity_tree(groups, strict)
            core = self.module.compile_exception_capacity_core(capacities, populations, query_limit=1)
            self.assertEqual(len(core["owner_runs"]), expected)
            self.assertEqual(expected, 3 * len(strict) + 1)
            owners = [owner for _, _, owner in core["owner_runs"]]
            self.assertGreater(len(owners), len(set(owners)))

    def test_complete_actual_envelopes(self):
        subsets = [frozenset(j for j in range(5) if bits >> j & 1) for bits in range(32)]
        accepted = refused = 0
        for groups in (2, 4):
            families = {}
            for rows in itertools.combinations_with_replacement(subsets, 2):
                union, minimum, capacities, _ = construct_actual_pair_metadata(rows, groups)
                families.setdefault((union, minimum, capacities), set()).update(rows)
            for (union, minimum, capacities), feasible in families.items():
                populations = compute_independent_group_counts(union, groups)[groups:]
                profiles = [self.module.compile_exception_capacity_core(capacities, populations, query_limit=limit)
                            for limit in (1, 5)]
                for raw, outside in itertools.product(subsets, (False, True)):
                    query = raw | ({9} if outside else set())
                    counts = compute_independent_group_counts(query & union, groups)[groups:]
                    pairs = [(j, q) for j, q in enumerate(counts) if q]
                    expected = max(Fraction(len(query & row), len(query | row))
                                   if query & row else Fraction() for row in feasible)
                    for core in profiles:
                        if any(q > core["query_limit"] for q in counts):
                            with self.assertRaisesRegex(ValueError, "query limit"):
                                self.module.compute_exception_core_bound(core, len(query), minimum, iter(pairs))
                            refused += 1
                        else:
                            actual = self.module.compute_exception_core_bound(core, len(query), minimum, iter(pairs))
                            self.assertEqual(actual, expected)
                            accepted += 1
        self.assertEqual(accepted + refused, 58880)
        print("exception actual envelopes:", accepted, "accepted,", refused, "refused")

    def test_prepared_frontier_equality(self):
        rng, checked = random.Random(93021), 0
        for groups in (2, 4, 8):
            for _ in range(64):
                rows = [set(j for j in range(48) if rng.randrange(2)) for _ in range(2)]
                _, minimum, capacities, populations = construct_actual_pair_metadata(rows, groups)
                limit = rng.randrange(1, 5)
                core = self.module.compile_exception_capacity_core(capacities, populations, query_limit=limit)
                counts = [rng.randrange(min(h, limit) + 1) for h in populations]
                a = sum(counts) + rng.randrange(4)
                pairs = [(j, q) for j, q in enumerate(counts) if q]
                prepared = self.module.prepare_exception_query_core(core, a, iter(pairs))
                self.assertEqual(prepared, build_additive_orientation_core(capacities, counts, populations))
                self.assertEqual(self.module.compute_exception_core_bound(core, a, minimum, iter(pairs)),
                                 compute_additive_core_bound(a, minimum, capacities, counts, populations))
                checked += 1
        self.assertEqual(checked, 192)

    def test_retained_exception_scaling(self):
        for groups in (8, 16, 32, 64, 128, 256):
            big = 10**12
            leaves = [big] * groups
            populations = [3 * big // 2] * groups
            leaves[2:4], populations[2:4] = [3, 3], [4, 4]
            capacities = [0] * groups + leaves
            for node in range(groups - 1, 0, -1):
                capacities[node] = capacities[2 * node] + capacities[2 * node + 1] - 2 * int(node == groups // 2 + 1)
            stats = {}
            core = self.module.compile_exception_capacity_core(capacities, populations, query_limit=2, stats=stats)
            self.assertEqual((len(core["nodes"]), len(core["owner_runs"]), len(core["exceptions"])), (4, 4, 2))
            self.assertEqual(stats["ownership_node_visits"], 2 * groups - 1)
            self.assertEqual(stats["static_leaf_records"], 0)
            self.assertEqual(stats["exception_leaf_records"], 2)
            pairs, stats = [(2, 2), (3, 2)], {}
            stream = SinglePassPairStream(pairs)
            with patch.object(self.module, "build_additive_orientation_core",
                              side_effect=AssertionError("query revisited input tree")):
                actual = self.module.compute_exception_core_bound(core, 4, capacities[1], stream, stats=stats)
            self.assertEqual(actual, Fraction(3, capacities[1] + 1))
            self.assertLess(actual, Fraction(4, capacities[1]))
            self.assertEqual(stream.iterations, 1)
            self.assertEqual(stats["query_owner_lookups"], 2)
            self.assertEqual(stats["query_exception_hits"], 2)
            self.assertLessEqual(stats["owner_intervals_crossed"], 3)
            self.assertEqual(stats["query_core_copies"], 4)

    def test_regular_exception_mixture(self):
        capacities, populations = [0, 6, 2, 4], [2, 6]
        core = self.module.compile_exception_capacity_core(capacities, populations, query_limit=2)
        self.assertEqual(core["exception_keys"], (0,))
        for counts in itertools.product(range(3), repeat=2):
            pairs = [(j, q) for j, q in enumerate(counts) if q]
            stats = {}
            value = self.module.compute_exception_core_bound(core, sum(counts), 2, iter(pairs), stats=stats)
            self.assertEqual(value, compute_additive_core_bound(sum(counts), 2, capacities, counts, populations))
            self.assertEqual(stats["query_exception_hits"], int(counts[0] > 0))
        with self.assertRaisesRegex(ValueError, "query limit"):
            self.module.compute_exception_core_bound(core, 3, 2, [(1, 3)])
        empty = self.module.compile_exception_capacity_core([0, 0, 0, 0], [0, 0], query_limit=1)
        with self.assertRaisesRegex(ValueError, "population"):
            self.module.compute_exception_core_bound(empty, 1, 0, [(0, 1)])

    def test_floor_total_counterexample(self):
        capacities, populations = [0, 4, 2, 2], [2, 4]
        core = self.module.compile_exception_capacity_core(capacities, populations, query_limit=1)
        self.assertEqual(core["exception_keys"], (0,))
        self.assertEqual(core["nodes"][0]["bundle"], (2, 4, 2, 0))
        self.assertEqual(self.module.compute_exception_core_bound(core, 1, 2, [(0, 1)]), Fraction(1, 4))
        self.assertEqual(self.module.compute_exception_core_bound(core, 1, 2, [(1, 1)]), Fraction(1, 2))

    def test_floor_only_collapse(self):
        checked = 0
        for groups in (2, 4, 8):
            capacities, populations = construct_marked_capacity_tree(groups, {groups // 2})
            core = self.module.compile_exception_capacity_core(capacities, populations, query_limit=1)
            self.assertEqual(core["exceptions"], ())
            for bits in range(1 << groups):
                pairs = [(j, 1) for j in range(groups) if bits >> j & 1]
                q, minimum = len(pairs), capacities[1]
                for outside in (0, 5):
                    actual = self.module.compute_exception_core_bound(core, q + outside, minimum, iter(pairs))
                    self.assertEqual(actual, Fraction(q, minimum + outside) if q else Fraction())
                    checked += 1
        self.assertEqual(checked, 552)

    def test_budget_before_stream(self):
        capacities, populations = construct_marked_capacity_tree(8, {5})
        core = self.module.compile_exception_capacity_core(capacities, populations, query_limit=2)
        for cap in ({"branch_cap": 1}, {"work_cap": 47}):
            stream, stats = SinglePassPairStream([(2, 1)]), {}
            with self.assertRaisesRegex(ValueError, "budget"):
                self.module.compute_exception_core_bound(core, 1, capacities[1], stream, stats=stats, **cap)
            self.assertEqual(stream.iterations, 0)
            self.assertEqual(stats["query_core_copies"], 0)
            self.assertEqual(stats["branches_visited"], 0)
        stats = {}
        self.module.compute_exception_core_bound(core, 1, capacities[1], [(2, 1)], branch_cap=2, work_cap=48, stats=stats)
        self.assertEqual(stats["reserved_branch_work"], 48)

    def test_query_limit_frontier(self):
        capacities, populations = [0, 9, 3, 6, 1, 2, 3, 3], [1, 3, 5, 6]
        sizes = []
        for limit in range(5):
            core = self.module.compile_exception_capacity_core(capacities, populations, query_limit=limit)
            sizes.append(len(core["exceptions"]))
            self.assertEqual(self.module.compute_exception_core_bound(core, 0, 6, ()), 0)
        self.assertEqual(sizes, [0, 1, 2, 3, 4])
        for limit in (True, -1, 0.5):
            with self.assertRaises(ValueError):
                self.module.compile_exception_capacity_core(capacities, populations, query_limit=limit)

    def test_exception_capacity_saturation(self):
        big = 10**100
        core = self.module.compile_exception_capacity_core([0, big], [big + 1], query_limit=3)
        self.assertEqual(core["exceptions"], ((0, 1, 3),))
        for query in range(1, 4):
            self.assertEqual(self.module.compute_exception_core_bound(core, query, 1, [(0, query)]),
                             compute_additive_core_bound(query, 1, [0, big], [query], [big + 1]))
        small = self.module.compile_exception_capacity_core([0, 1], [1], query_limit=3)
        self.assertEqual(small["exceptions"], ((0, 0, 1),))
        with self.assertRaisesRegex(ValueError, "population"):
            self.module.compute_exception_core_bound(small, 2, 0, [(0, 2)])

    def test_exception_position_information(self):
        profiles = queries = 0
        for groups in (4, 8):
            for count in range(1, groups // 2 + 1):
                answers, common_nodes = set(), None
                for positions in itertools.combinations(range(groups), count):
                    exceptions = set(positions)
                    capacities = [0] * groups + [2 if j in exceptions else 1 for j in range(groups)]
                    for node in range(groups - 1, 0, -1):
                        capacities[node] = capacities[2 * node] + capacities[2 * node + 1]
                    core = self.module.compile_exception_capacity_core(capacities, [2] * groups, query_limit=1)
                    self.assertEqual(core["strict_nodes"], 0)
                    self.assertEqual(core["exception_keys"], positions)
                    if common_nodes is None:
                        common_nodes = core["nodes"]
                    self.assertEqual(core["nodes"], common_nodes)
                    minimum, maximum = groups - count, groups + count
                    result = tuple(self.module.compute_exception_core_bound(core, 1, minimum, [(j, 1)])
                                   for j in range(groups))
                    self.assertEqual(result, tuple(Fraction(1, maximum if j in exceptions else minimum)
                                                   for j in range(groups)))
                    answers.add(result)
                    profiles += 1
                    queries += groups
                self.assertEqual(len(answers), math.comb(groups, count))
        self.assertEqual((profiles, queries), (172, 1336))

    def test_validation_and_emptiness(self):
        core = self.module.compile_exception_capacity_core([0, 3], [4], query_limit=2)
        for pairs in ([(0, 0)], [(0, -1)], [(True, 1)], [(0, True)], [(0, 0.5)],
                      [(1, 1)], [(-1, 1)], [(0, 1), (0, 1)], [(0,)], ["bad"]):
            with self.assertRaises(ValueError):
                self.module.compute_exception_core_bound(core, 1, 1, pairs)
        for a, minimum in ((True, 1), (1, True), (-1, 1), (1, 0), (0, 1)):
            with self.assertRaises(ValueError):
                self.module.compute_exception_core_bound(core, a, minimum, [(0, 1)])
        empty = self.module.compile_exception_capacity_core([0, 0], [0], query_limit=0)
        self.assertEqual(self.module.compute_exception_core_bound(empty, 0, 0, iter(())), 0)
        with self.assertRaises(ValueError):
            self.module.compile_exception_capacity_core([0, 1, 2, 2], [3, 3], query_limit=1)

    def test_ownership_and_failures(self):
        class Counts(list):
            pass
        capacities, populations = construct_marked_capacity_tree(8, {5})
        capacities, populations = Counts(capacities), Counts(populations)
        refs = weakref.ref(capacities), weakref.ref(populations)
        was_enabled = gc.isenabled()
        gc.disable()
        try:
            core = self.module.compile_exception_capacity_core(capacities, populations, query_limit=2)
            del capacities, populations
            self.assertTrue(all(ref() is None for ref in refs))
        finally:
            if was_enabled:
                gc.enable()
        before = repr(core)
        with self.assertRaises(TypeError):
            core["groups"] = 2
        with self.assertRaises(TypeError):
            core["nodes"][0]["capacity"] = 100
        def interrupt_after_valid_pair():
            yield 2, 1
            raise RuntimeError("input interrupted")
        with self.assertRaisesRegex(RuntimeError, "interrupted"):
            self.module.compute_exception_core_bound(core, 2, core["major_size"], interrupt_after_valid_pair())
        self.assertEqual(repr(core), before)


if __name__ == "__main__":
    unittest.main()
