"""Independent finite envelopes and conditioned domains for the A05 core."""

import importlib
import importlib.util
import itertools
import random
import unittest
import gc
import weakref
from fractions import Fraction

from test_laminar_capacity_similarity import compute_independent_group_counts
from probe_orientation_parameter_similarity import compute_orientation_capacity_bound


def construct_actual_pair_metadata(rows, groups):
    union = rows[0] | rows[1]
    capacities = tuple(map(max, zip(*(compute_independent_group_counts(row, groups) for row in rows))))
    populations = compute_independent_group_counts(union, groups)[groups:]
    return union, min(map(len, rows)), capacities, populations


def enumerate_original_row_labels(capacities, assignment):
    groups, bit = len(capacities)//2, 0
    labels = [0]*len(capacities)
    for node in range(1, groups):
        if capacities[2*node]+capacities[2*node+1] > capacities[node]:
            labels[2*node] = (assignment >> bit) & 1
            labels[2*node+1] = 1-labels[2*node]
            bit += 1
        else:
            labels[2*node] = labels[2*node+1] = labels[node]
    return labels


def enumerate_subtree_count_frontier(node, row, labels, capacities, counts, populations):
    """Enumerate leaf counts directly, without a capped-linear recurrence."""
    groups = len(counts)
    descendants, leaves, stack = [], [], [node]
    while stack:
        current = stack.pop()
        descendants.append(current)
        if current >= groups:
            leaves.append(current)
        else:
            stack.extend((2*current, 2*current+1))
    domains = [([capacities[v]] if labels[v] == row else
                range(populations[v-groups]-capacities[v], capacities[v]+1)) for v in leaves]
    result = {}
    for values in itertools.product(*domains):
        sizes = dict(zip(leaves, values))
        for v in sorted(descendants, reverse=True):
            if v < groups:
                sizes[v] = sizes[2*v]+sizes[2*v+1]
        if any(sizes[v] > capacities[v] or (labels[v] == row and sizes[v] != capacities[v])
               for v in descendants):
            continue
        score = sum(min(counts[v-groups], sizes[v]) for v in leaves)
        result[sizes[node]] = max(result.get(sizes[node], -1), score)
    return result


class AdditiveCoreSimilarityTests(unittest.TestCase):
    def load_required_core_module(self):
        name = "probe_additive_core_similarity"
        self.assertIsNotNone(importlib.util.find_spec(name), "additive-region core implementation missing")
        return importlib.import_module(name)

    def test_complete_actual_pair_envelopes(self):
        module = self.load_required_core_module()
        subsets = [frozenset(j for j in range(5) if mask & (1 << j)) for mask in range(32)]
        checked = 0
        for groups in (2, 4):
            families = {}
            for rows in itertools.combinations_with_replacement(subsets, 2):
                union, minimum, caps, _ = construct_actual_pair_metadata(rows, groups)
                families.setdefault((union, minimum, caps), set()).update(rows)
            for (union, minimum, caps), feasible in families.items():
                populations = compute_independent_group_counts(union, groups)[groups:]
                for raw, outside in itertools.product(subsets, (False, True)):
                    query = raw | ({9} if outside else set())
                    counts = compute_independent_group_counts(query & union, groups)[groups:]
                    expected = max(Fraction(len(query & row), len(query | row))
                                   if query & row else Fraction() for row in feasible)
                    stats = {}
                    self.assertEqual(module.compute_additive_core_bound(
                        len(query), minimum, caps, counts, populations, stats=stats), expected)
                    self.assertLessEqual(stats["core_nodes"], 3*stats["strict_nodes"]+1)
                    self.assertLessEqual(stats["branch_work"], stats["reserved_branch_work"])
                    checked += 1
        self.assertEqual(checked, 29440)

    def test_conditioned_domains_and_frontiers(self):
        module = self.load_required_core_module()
        rng = random.Random(921091)
        checks, strict_seen, infeasible = 0, set(), 0
        cases = [([{0, 4, 2}, {1, 3, 7}], {0, 1, 2, 3})]
        for _ in range(256):
            rows = [set(j for j in range(8) if rng.randrange(2)) for _ in range(2)]
            query = set(j for j in range(9) if rng.randrange(2))
            cases.append((rows, query))
        for rows, query in cases:
            union, _, caps, populations = construct_actual_pair_metadata(rows, 4)
            counts = compute_independent_group_counts(query & union, 4)[4:]
            core = module.build_additive_orientation_core(caps, counts, populations)
            strict_seen.add(core["strict_nodes"])
            for assignment in range(1 << core["strict_nodes"]):
                original_labels = enumerate_original_row_labels(caps, assignment)
                labels = module.orient_additive_core_labels(core, assignment)
                for row in (0, 1):
                    records = module.evaluate_additive_core_records(core, labels, row)
                    for item, record in zip(core["nodes"], records):
                        expected = enumerate_subtree_count_frontier(
                            item["original_node"], row, original_labels, caps, counts, populations)
                        actual = ({} if record is None else
                                  {x: min(x-record[2], record[3]) for x in range(record[0], record[1]+1)})
                        self.assertEqual(actual, expected, (caps, counts, populations, assignment, row, item))
                        infeasible += not expected
                        checks += 1
        self.assertEqual(strict_seen, {0, 1, 2, 3})
        self.assertEqual(checks, 2932)
        self.assertGreater(infeasible, 0)

    def test_bundle_preserves_forced_nonquery_occupancy(self):
        module = self.load_required_core_module()
        core = module.build_additive_orientation_core([0, 4, 2, 2], [0, 3], [3, 3])
        self.assertEqual(len(core["nodes"]), 1)
        labels = module.orient_additive_core_labels(core, 0)
        minor = module.evaluate_additive_core_records(core, labels, 1)[-1]
        self.assertEqual(minor, (2, 4, 1, 2))
        self.assertEqual([min(x-minor[2], minor[3]) for x in range(2, 5)], [1, 2, 2])
        self.assertNotEqual([min(x, 3) for x in range(2, 5)], [1, 2, 2])

    def test_fixed_conflict_core_scaling(self):
        module = self.load_required_core_module()
        for groups in (4, 8, 16, 32, 64, 128, 256):
            populations, counts = [4, 4]+[2]*(groups-2), [2, 2]+[0]*(groups-2)
            caps = [0]*groups+[3, 3]+[2]*(groups-2)
            for node in range(groups-1, 0, -1):
                caps[node] = caps[2*node]+caps[2*node+1]-(2 if node == groups//2 else 0)
            stats = {}
            args = (4, caps[1], caps, counts, populations)
            result = module.compute_additive_core_bound(*args, stats=stats)
            self.assertEqual(result, compute_orientation_capacity_bound(*args))
            self.assertEqual(result, Fraction(3, caps[1]+1))
            self.assertEqual(stats["preprocessing_node_visits"], 2*groups-1)
            self.assertEqual((stats["strict_nodes"], stats["core_nodes"], stats["component_nodes"]), (1, 4, 3))
            self.assertEqual(stats["reserved_branch_work"], 48)
            self.assertEqual(stats["branch_work"], 48)
        huge = 10**12
        args = (4*huge, 20*huge, [v*huge for v in (0, 20, 4, 16, 3, 3, 16, 0)],
                [2*huge, 2*huge, 0, 0], [4*huge, 4*huge, 16*huge, 0])
        self.assertEqual(module.compute_additive_core_bound(*args), Fraction(1, 7))

    def test_nested_strict_reference_cases(self):
        module = self.load_required_core_module()
        rng = random.Random(921092)
        checked = 0
        for _ in range(256):
            rows = [set(j for j in range(24) if rng.randrange(2)) for _ in range(2)]
            union, minimum, caps, populations = construct_actual_pair_metadata(rows, 8)
            for _ in range(4):
                query = set(j for j in range(26) if rng.randrange(2))
                counts = compute_independent_group_counts(query & union, 8)[8:]
                args = len(query), minimum, caps, counts, populations
                self.assertEqual(module.compute_additive_core_bound(*args), compute_orientation_capacity_bound(*args))
                checked += 1
        self.assertEqual(checked, 1024)

    def test_refusal_and_empty_components(self):
        module = self.load_required_core_module()
        args = 4, 4, [0, 4, 3, 3], [2, 2], [4, 4]
        stats = {}
        self.assertEqual(module.compute_additive_core_bound(*args, stats=stats), Fraction(3, 5))
        reservation = stats["reserved_branch_work"]
        for options, reason in (({"branch_cap": 1}, "branch"), ({"work_cap": reservation-1}, "work")):
            refused = {}
            with self.assertRaisesRegex(ValueError, reason):
                module.compute_additive_core_bound(*args, stats=refused, **options)
            self.assertEqual(refused["branches_visited"], 0)
            self.assertEqual(refused["branch_work"], 0)
        self.assertEqual(module.compute_additive_core_bound(*args, work_cap=reservation), Fraction(3, 5))
        for options in ({"branch_cap": True}, {"work_cap": -1}):
            with self.assertRaises(ValueError):
                module.compute_additive_core_bound(*args, **options)
        with self.assertRaisesRegex(ValueError, "no complete pair"):
            module.compute_additive_core_bound(0, 1, [0, 3, 2, 2], [0, 0], [2, 2])
        self.assertEqual(module.compute_additive_core_bound(0, 0, [0, 0], [0], [0]), 0)
        self.assertEqual(module.compute_additive_core_bound(10**12, 10**12, [0, 10**12], [10**12], [10**12]), 1)
        with self.assertRaises(ValueError):
            module.compute_additive_core_bound(0, 1, [0, 3, 1, 1], [0, 0], [2, 2])

    def test_parent_capacity_local_validation(self):
        module = self.load_required_core_module()
        with self.assertRaisesRegex(ValueError, "child maximum exceeds parent"):
            module.build_additive_orientation_core([0, 2, 3, 1], [0, 0], [3, 1])

    def test_infeasible_companion_branch_discarded(self):
        module = self.load_required_core_module()
        stats = {}
        result = module.compute_additive_core_bound(2, 1, [0, 2, 1, 2], [1, 1], [1, 2], stats=stats)
        self.assertEqual(result, Fraction(1, 2))
        self.assertEqual(stats["feasible_branches"], 1)

    def test_compiler_releases_input_arrays(self):
        module = self.load_required_core_module()

        class WeakReferenceListFixture(list):
            pass

        enabled = gc.isenabled()
        gc.disable()
        try:
            inputs = [WeakReferenceListFixture(values) for values in
                      ([0, 4, 3, 3], [2, 2], [4, 4])]
            references = [weakref.ref(values) for values in inputs]
            core = module.build_additive_orientation_core(*inputs)
            del inputs
            self.assertTrue(all(reference() is None for reference in references),
                            "compiler closure retains input arrays until cyclic garbage collection")
            self.assertEqual(core["major_size"], 4)
        finally:
            if enabled:
                gc.enable()
            gc.collect()


if __name__ == "__main__":
    unittest.main()
