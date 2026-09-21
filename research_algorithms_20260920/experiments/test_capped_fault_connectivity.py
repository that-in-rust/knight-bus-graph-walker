"""Capped core scheduling contracts; resident fixture data is oracle state."""

from itertools import combinations
import random
import unittest

from probe_fault_cover_connectivity import build_streaming_edit_cover, run_prepared_fault_connectivity
from test_fault_cover_connectivity import PreparedResidentTestSource, calculate_expanded_component_labels


class SourceProbeCursorRow:
    def __init__(self, row, wrong_length=False):
        self.row = row
        self.length = len(row) + int(wrong_length)
        self.position = 0
        self.closed = False

    def read_next_neighbor_exact(self):
        if self.closed:
            raise AssertionError("read after cursor close")
        if self.position == len(self.row):
            raise AssertionError("read beyond prepared row")
        value = self.row[self.position]
        self.position += 1
        return value

    def close_current_cursor_exact(self):
        if self.closed:
            raise AssertionError("cursor closed twice")
        self.closed = True


class CappedResidentTestSource(PreparedResidentTestSource):
    snapshot_id = "fixture-epoch-v1"

    def __init__(self, *args, wrong_length=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.opened = []
        self.wrong_length = wrong_length

    def open_cover_deletion_row(self, snapshot_id, vertex):
        if snapshot_id != self.snapshot_id:
            raise ValueError("stale snapshot")
        cursor = SourceProbeCursorRow(self.negative[vertex], self.wrong_length)
        self.opened.append((vertex, cursor))
        return cursor


class CappedFaultConnectivityTests(unittest.TestCase):
    def execute_capped_fixture_exact(self, vertices, groups, deleted=(), added=(), cover=None):
        if cover is None:
            cover = build_streaming_edit_cover(iter(tuple(deleted) + tuple(added)), max_cover_vertices=len(vertices))
        source = CappedResidentTestSource(vertices, groups, deleted, added, cover)
        output = {}
        try:
            receipt = run_prepared_fault_connectivity(
                source, cover, lambda vertex, label: output.__setitem__(vertex, label),
                max_state_nodes=len(groups) + len(cover), core_plan="capped",
            )
        except TypeError as error:
            if "core_plan" in str(error):
                self.fail("capped core plan has not been implemented")
            raise
        self.assertEqual(output, calculate_expanded_component_labels(vertices, groups, deleted, added))
        self.assertEqual(receipt["output_rows"], len(vertices))
        self.assertTrue(all(cursor.closed for _, cursor in source.opened))
        self.assertEqual(receipt["local_deletion_opens"], len(source.opened))
        self.assertEqual(receipt["local_deletion_records"], sum(cursor.position for _, cursor in source.opened))
        self.assertEqual(receipt["local_search_events"], receipt["local_deletion_opens"] + receipt["local_deletion_records"] + receipt["local_candidate_tests"])
        self.assertLessEqual(receipt["fallback_core_posting_reads"], receipt["deferred_scan_budget"])
        self.assertLessEqual(receipt["deferred_scan_budget"], receipt["local_search_events"])
        self.assertLessEqual(receipt["fallback_passes"], 1)
        return source, receipt

    def test_exhaustive_snapshot_equivalence(self):
        vertices = tuple(range(4))
        pairs = tuple(combinations(vertices, 2))
        for incidence in range(256):
            groups = [tuple(v for v in vertices if incidence >> (4 * f + v) & 1) for f in range(2)]
            base = set().union(*(set(combinations(group, 2)) for group in groups))
            for mask in range(64):
                current = {pair for bit, pair in enumerate(pairs) if mask >> bit & 1}
                self.execute_capped_fixture_exact(vertices, groups, sorted(base - current), sorted(current - base))

    def test_seeded_varied_covers(self):
        rng = random.Random(20260922)
        for _ in range(500):
            vertices = tuple(rng.sample(range(1000), rng.randrange(1, 10)))
            groups = [tuple(sorted(v for v in vertices if rng.randrange(2))) for _ in range(rng.randrange(8))]
            base = set().union(*(set(combinations(group, 2)) for group in groups))
            pairs = tuple(combinations(sorted(vertices), 2))
            current = {pair for pair in pairs if rng.randrange(3) == 0}
            deleted, added = sorted(base - current), sorted(current - base)
            cover = set(build_streaming_edit_cover(iter(deleted + added), max_cover_vertices=len(vertices)))
            cover.update(v for v in vertices if rng.randrange(2))
            self.execute_capped_fixture_exact(vertices, groups, deleted, added, tuple(sorted(cover)))

    def test_sparse_core_matching(self):
        vertices = tuple(range(64))
        deleted = tuple((v, v + 1) for v in range(0, 64, 2))
        source, receipt = self.execute_capped_fixture_exact(vertices, (vertices,), deleted, cover=vertices)
        self.assertEqual(receipt["core_setup_posting_reads"], 64)
        self.assertEqual(receipt["local_search_events"], 68)
        self.assertEqual(receipt["local_candidate_tests"], 64)
        self.assertEqual(receipt["fallback_passes"], 0)
        self.assertEqual([cursor.position for _, cursor in source.opened], [1, 1])

    def test_outside_seed_floor(self):
        vertices, cover = tuple(range(65)), tuple(range(64))
        deleted = tuple((v, v + 1) for v in range(0, 64, 2)) + tuple((v, 64) for v in range(32))
        _, receipt = self.execute_capped_fixture_exact(vertices, (vertices,), deleted, cover=cover)
        self.assertEqual(receipt["core_seed_unions"], 32)
        self.assertEqual(receipt["local_search_events"], 0)
        self.assertEqual(receipt["core_setup_posting_reads"], 64)
        self.assertEqual(receipt["fallback_passes"], 0)

    def test_shared_fallback_cancellation(self):
        cover = tuple(range(13))
        outside = tuple(range(100, 200))
        groups = tuple((0, v) for v in cover[1:]) + ((0,) + outside,)
        deleted = tuple((0, v) for v in outside)
        source, receipt = self.execute_capped_fixture_exact(cover + outside, groups, deleted, cover=cover)
        self.assertEqual(receipt["core_search_caps"], 12)
        self.assertEqual(receipt["local_search_events"], 48)
        self.assertEqual(receipt["fallback_core_posting_reads"], 48)
        self.assertEqual(receipt["fallback_negative_records"], 100)
        self.assertEqual(receipt["fallback_negative_opens"], 13)
        self.assertEqual(receipt["fallback_passes"], 1)
        self.assertEqual([cursor.position for _, cursor in source.opened], [3] * 12)

    def test_dense_negative_fallback(self):
        vertices = tuple(range(24))
        _, receipt = self.execute_capped_fixture_exact(vertices, (vertices,), tuple(combinations(vertices, 2)), cover=vertices)
        self.assertEqual(receipt["local_search_events"], 576)
        self.assertEqual(receipt["fallback_core_posting_reads"], 576)
        self.assertEqual(receipt["core_search_caps"], 1)

    def test_uncapped_complement_control(self):
        cover, outside = tuple(range(13)), tuple(range(100, 200))
        groups = tuple((0, vertex) for vertex in cover[1:]) + ((0,) + outside,)
        deleted = tuple((0, vertex) for vertex in outside)
        source = CappedResidentTestSource(cover + outside, groups, deleted, cover=cover)
        output = {}
        with self.subTest("ordinary seeded search must be an available comparator"):
            try:
                receipt = run_prepared_fault_connectivity(
                    source, cover, lambda vertex, label: output.__setitem__(vertex, label),
                    max_state_nodes=26, core_plan="complement",
                )
            except ValueError as error:
                self.fail(str(error))
        self.assertEqual(output, calculate_expanded_component_labels(cover + outside, groups, deleted, ()))
        self.assertEqual(receipt["local_search_events"], 1224)
        self.assertEqual(receipt["fallback_passes"], 0)
        self.assertTrue(all(cursor.closed for _, cursor in source.opened))

    def test_bridge_and_overlap(self):
        self.execute_capped_fixture_exact((9, 0, 5, 101), (), added=((0, 5), (0, 9)), cover=(5, 9))
        self.execute_capped_fixture_exact((0, 1, 2), ((0, 1, 2),), deleted=((0, 2),), cover=(0, 1))
        self.execute_capped_fixture_exact((0, 1), ((0, 1), (0, 1)), deleted=((0, 1),), cover=(0, 1))

    def test_mismatched_row_metadata(self):
        vertices = tuple(range(4))
        source = CappedResidentTestSource(vertices, (vertices,), deleted=((0, 1), (2, 3)), cover=vertices, wrong_length=True)
        output = []
        try:
            with self.assertRaisesRegex(ValueError, "row length"):
                run_prepared_fault_connectivity(source, vertices, lambda *record: output.append(record), max_state_nodes=5, core_plan="capped")
        except TypeError as error:
            if "core_plan" in str(error):
                self.fail("capped core plan has not been implemented")
            raise
        self.assertEqual(output, [])
        self.assertTrue(all(cursor.closed for _, cursor in source.opened))


if __name__ == "__main__":
    unittest.main()
