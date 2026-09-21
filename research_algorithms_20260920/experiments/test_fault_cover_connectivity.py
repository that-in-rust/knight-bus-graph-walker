"""Verification-only resident fixtures for the prepared fault-cover executor."""

from importlib import import_module
from itertools import combinations
import random
import unittest


def load_required_connectivity_module():
    try:
        return import_module("probe_fault_cover_connectivity")
    except ModuleNotFoundError as error:
        if error.name != "probe_fault_cover_connectivity":
            raise
        raise AssertionError("fault-cover implementation is absent") from error


def calculate_expanded_component_labels(vertices, groups, deleted, added):
    edges = set().union(*(set(combinations(sorted(group), 2)) for group in groups))
    edges = (edges - set(deleted)) | set(added)
    neighbors = {vertex: set() for vertex in vertices}
    for left, right in edges:
        neighbors[left].add(right)
        neighbors[right].add(left)
    labels = {}
    for vertex in vertices:
        if vertex in labels:
            continue
        reached, pending = {vertex}, [vertex]
        while pending:
            current = pending.pop()
            for neighbor in neighbors[current] - reached:
                reached.add(neighbor)
                pending.append(neighbor)
        minimum = min(reached)
        labels.update((item, minimum) for item in reached)
    return labels


def find_minimum_cover_size(vertices, edges):
    best = len(vertices)
    for bits in range(1 << len(vertices)):
        candidate = {vertex for rank, vertex in enumerate(vertices) if bits & (1 << rank)}
        if all(left in candidate or right in candidate for left, right in edges):
            best = min(best, len(candidate))
    return best


class PreparedResidentTestSource:
    """Fixture memory is deliberately NOT the candidate workspace claim."""

    def __init__(self, vertices, groups, deleted=(), added=(), cover=()):
        self.vertices, self.groups = tuple(vertices), tuple(map(tuple, groups))
        self.vertex_count, self.factor_count = len(vertices), len(groups)
        self.deleted, self.added, self.cover = tuple(deleted), tuple(added), frozenset(cover)
        self.calls = 0
        self.rows = {vertex: tuple(i for i, group in enumerate(groups) if vertex in group) for vertex in vertices}
        self.core = [tuple(vertex for vertex in group if vertex in self.cover) for group in groups]
        self.negative = {vertex: [] for vertex in vertices}
        self.positive = {vertex: [] for vertex in vertices}
        for left, right in self.deleted:
            self.negative[left].append(right)
            self.negative[right].append(left)
        for left, right in self.added:
            self.positive[left].append(right)
            self.positive[right].append(left)

    def iterate_complete_edit_edges(self):
        self.calls += 1
        return iter(self.deleted + self.added)

    def iterate_complete_vertex_rows(self):
        self.calls += 1
        return ((vertex, iter(self.rows[vertex]), iter(self.positive[vertex])) for vertex in self.vertices)

    def iterate_vertex_factor_memberships(self, vertex):
        self.calls += 1
        return iter(self.rows[vertex])

    def iterate_factor_cover_members(self, factor):
        self.calls += 1
        return iter(self.core[factor])

    def iterate_vertex_deleted_neighbors(self, vertex):
        self.calls += 1
        return iter(self.negative[vertex])

    def iterate_inserted_cover_edges(self):
        self.calls += 1
        return ((left, right) for left, right in self.added if left in self.cover and right in self.cover)


class GeneratedStarDeletionSource:
    """No n-sized container; represents one clique and a center-deletion star."""

    factor_count = 1

    def __init__(self, count):
        self.vertex_count = count

    def iterate_complete_edit_edges(self):
        return ((0, other) for other in range(1, self.vertex_count))

    def iterate_complete_vertex_rows(self):
        return ((vertex, iter((0,)), iter(())) for vertex in range(self.vertex_count))

    def iterate_vertex_factor_memberships(self, vertex):
        return iter((0,))

    def iterate_factor_cover_members(self, factor):
        return iter((0,))

    def iterate_vertex_deleted_neighbors(self, vertex):
        return iter(range(1, self.vertex_count))

    def iterate_inserted_cover_edges(self):
        return iter(())


class FaultCoverConnectivityContracts(unittest.TestCase):
    def compare_complete_component_output(self, vertices, groups, deleted=(), added=(), cover=None):
        module = load_required_connectivity_module()
        if cover is None:
            cover = module.build_streaming_edit_cover(iter(tuple(deleted) + tuple(added)), max_cover_vertices=len(vertices))
        source = PreparedResidentTestSource(vertices, groups, deleted, added, cover)
        output = {}
        receipt = module.run_prepared_fault_connectivity(source, cover, lambda vertex, label: output.__setitem__(vertex, label), max_state_nodes=len(groups) + len(cover))
        expected = calculate_expanded_component_labels(vertices, groups, deleted, added)
        self.assertEqual(output, expected)
        self.assertEqual(receipt["state_nodes"], len(groups) + len(cover))
        self.assertEqual(receipt["output_rows"], len(vertices))
        return output, receipt

    def test_exhaustive_snapshot_equivalence(self):
        vertices, pairs, cases = tuple(range(4)), tuple(combinations(range(4), 2)), 0
        for incidence in range(256):
            groups = tuple(tuple(vertex for vertex in vertices if incidence & (1 << (factor * 4 + vertex))) for factor in range(2))
            base = set().union(*(set(combinations(group, 2)) for group in groups))
            for current_mask in range(64):
                current = {pair for bit, pair in enumerate(pairs) if current_mask & (1 << bit)}
                self.compare_complete_component_output(vertices, groups, sorted(base - current), sorted(current - base))
                cases += 1
        self.assertEqual(cases, 16384)

    def test_seeded_nonminimal_covers(self):
        rng = random.Random(20260921)
        for _ in range(500):
            vertices = tuple(rng.sample(range(1000), rng.randrange(1, 10)))
            groups = [tuple(sorted(v for v in vertices if rng.randrange(2))) for _ in range(rng.randrange(7))]
            base = set().union(*(set(combinations(group, 2)) for group in groups))
            pairs = tuple(combinations(sorted(vertices), 2))
            current = {pair for pair in pairs if rng.randrange(3) == 0}
            self.compare_complete_component_output(vertices, groups, sorted(base - current), sorted(current - base), cover=vertices)

    def test_factor_specific_missing_counts(self):
        result, receipt = self.compare_complete_component_output(tuple(range(5)), ((0, 1, 2), (0, 3, 4)), ((0, 1), (0, 2)), cover=(0,))
        self.assertEqual(result, {0: 0, 1: 1, 2: 1, 3: 0, 4: 0})
        self.assertEqual(receipt["negative_membership_reads"], 2)

    def test_isolate_insertion_bridge_minimum(self):
        result, receipt = self.compare_complete_component_output((9, 0, 5, 101), (), added=((0, 5), (0, 9)), cover=(5, 9))
        self.assertEqual(result, {0: 0, 5: 0, 9: 0, 101: 101})
        self.assertEqual(receipt["state_nodes"], 2)
        self.compare_complete_component_output((0, 1), ((0, 1),), deleted=((0, 1),), cover=(0, 1))
        self.compare_complete_component_output((0, 1), ((0, 1), (0, 1)), deleted=((0, 1),), cover=(0,))

    def test_streamed_star_state_bound(self):
        module = load_required_connectivity_module()
        for count in (8, 10000):
            rows = 0
            def verify_streamed_vertex_label(vertex, label):
                nonlocal rows
                self.assertEqual(label, 0 if vertex == 0 else 1)
                rows += 1
            receipt = module.run_prepared_fault_connectivity(GeneratedStarDeletionSource(count), (0,), verify_streamed_vertex_label, max_state_nodes=2)
            self.assertEqual(rows, count)
            self.assertEqual(receipt["state_nodes"], 2)
            self.assertEqual(receipt["negative_membership_reads"], count - 1)

    def test_edit_cover_approximation(self):
        module = load_required_connectivity_module()
        vertices, pairs = range(5), tuple(combinations(range(5), 2))
        for mask in range(1024):
            edges = tuple(pair for bit, pair in enumerate(pairs) if mask & (1 << bit))
            cover = module.build_streaming_edit_cover(iter(edges), max_cover_vertices=5)
            minimum = find_minimum_cover_size(vertices, edges)
            self.assertLessEqual(len(cover), 2 * minimum)
            self.assertTrue(all(left in cover or right in cover for left, right in edges))

    def test_preallocation_and_cover_refusals(self):
        module = load_required_connectivity_module()
        source = PreparedResidentTestSource((0, 1), ((0, 1),), deleted=((0, 1),), cover=(0,))
        with self.assertRaises(ValueError):
            module.run_prepared_fault_connectivity(source, (0,), lambda *x: None, max_state_nodes=1)
        self.assertEqual(source.calls, 0)
        for cover in ((), (0, 0), (False,), (9,)):
            with self.assertRaises(ValueError):
                module.run_prepared_fault_connectivity(source, cover, lambda *x: None, max_state_nodes=8)
        with self.assertRaises(ValueError):
            module.build_streaming_edit_cover(iter(((0, 1), (2, 3))), max_cover_vertices=3)

    def test_safe_attachments_skip_expansion(self):
        _, receipt = self.compare_complete_component_output(tuple(range(6)), (tuple(range(6)),), ((0, 2), (1, 3)), cover=(0, 1))
        self.assertEqual(receipt["core_posting_reads"], 0)
        self.assertEqual(receipt["negative_membership_reads"], 0)
        self.assertEqual(receipt["candidate_flag_visits"], 0)

    def test_blocked_incidence_scan_bound(self):
        deleted = ((0, 1), (0, 2), (0, 3), (0, 4), (0, 5), (0, 6), (1, 3), (2, 4))
        result, receipt = self.compare_complete_component_output(tuple(range(7)), (tuple(range(7)),), deleted, cover=(0, 1, 2))
        self.assertEqual(result, {0: 0, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 1})
        self.assertEqual(receipt["core_posting_reads"], 3)
        self.assertEqual(receipt["negative_membership_reads"], 4)


if __name__ == "__main__":
    unittest.main()
