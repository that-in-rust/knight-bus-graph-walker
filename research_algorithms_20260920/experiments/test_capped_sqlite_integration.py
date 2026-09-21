"""Complete file-backed labels against a separately expanded tiny oracle."""

from itertools import combinations
from pathlib import Path
import random
import tempfile
import unittest
from unittest.mock import patch

from fault_cover_sqlite_source import build_sqlite_fault_source, SqlitePreparedFaultSource
from probe_fault_cover_connectivity import build_streaming_edit_cover, run_prepared_fault_connectivity
from test_fault_cover_connectivity import calculate_expanded_component_labels


class CappedSqliteIntegrationTests(unittest.TestCase):
    def compare_filebacked_complete_labels(self, path, vertices, groups, deleted=(), added=(), cover=None):
        if cover is None:
            cover = build_streaming_edit_cover(iter(tuple(deleted) + tuple(added)), max_cover_vertices=len(vertices))
        build = build_sqlite_fault_source(path, iter(vertices), (iter(group) for group in groups),
                                         iter(deleted), iter(added), iter(cover), cache_kib=32)
        expected = calculate_expanded_component_labels(vertices, groups, deleted, added)
        receipts = {}
        for plan in ("blocked", "complement", "capped"):
            output = {}
            with SqlitePreparedFaultSource(path, cache_kib=32, max_open_cursors=4) as source:
                receipt = run_prepared_fault_connectivity(
                    source, cover, lambda vertex, label: output.__setitem__(vertex, label),
                    max_state_nodes=len(groups) + len(cover), core_plan=plan,
                )
                self.assertEqual(output, expected)
                self.assertEqual(source.events["active_cursors"], 0)
                self.assertLessEqual(source.events["peak_cursors"], 3)
                self.assertEqual(source.snapshot_id, build["snapshot_id"])
                self.assertEqual(source.events["record_fetches"], source.events["record_reads"])
                receipts[plan] = receipt
        return receipts

    def test_seeded_filebacked_snapshots(self):
        rng = random.Random(20260923)
        with tempfile.TemporaryDirectory() as directory:
            for case in range(60):
                vertices = tuple(rng.sample(range(1000), rng.randrange(1, 10)))
                groups = [tuple(sorted(vertex for vertex in vertices if rng.randrange(2))) for _ in range(rng.randrange(8))]
                base = set().union(*(set(combinations(group, 2)) for group in groups))
                current = {pair for pair in combinations(sorted(vertices), 2) if rng.randrange(3) == 0}
                deleted, added = sorted(base - current), sorted(current - base)
                cover = set(build_streaming_edit_cover(iter(deleted + added), max_cover_vertices=len(vertices)))
                cover.update(vertex for vertex in vertices if rng.randrange(2))
                self.compare_filebacked_complete_labels(Path(directory) / f"{case}.sqlite", vertices, groups,
                                                       deleted, added, tuple(sorted(cover)))

    def test_filebacked_adversarial_schedules(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            vertices = tuple(range(64))
            receipts = self.compare_filebacked_complete_labels(root / "matching.sqlite", vertices, (vertices,),
                tuple((vertex, vertex + 1) for vertex in range(0, 64, 2)), cover=vertices)
            self.assertEqual(receipts["blocked"]["core_posting_reads"], 4096)
            self.assertEqual(receipts["capped"]["local_search_events"], 68)
            cover, outside = tuple(range(13)), tuple(range(100, 200))
            receipts = self.compare_filebacked_complete_labels(root / "replay.sqlite", cover + outside,
                tuple((0, vertex) for vertex in cover[1:]) + ((0,) + outside,),
                tuple((0, vertex) for vertex in outside), cover=cover)
            self.assertEqual(receipts["complement"]["local_search_events"], 1224)
            self.assertEqual(receipts["capped"]["local_search_events"], 48)
            self.assertEqual(receipts["capped"]["fallback_negative_records"], 100)
            self.assertEqual(receipts["capped"]["fallback_core_posting_reads"], 48)
            vertices = tuple(range(24))
            self.compare_filebacked_complete_labels(root / "dense.sqlite", vertices, (vertices,),
                                                   tuple(combinations(vertices, 2)), cover=vertices)
            self.compare_filebacked_complete_labels(root / "isolates.sqlite", (2**63 - 1, 19, 0), (),
                                                   added=((0, 2**63 - 1),), cover=(0,))
            self.compare_filebacked_complete_labels(root / "empty.sqlite", (), ((),))

    def test_provider_failure_not_cap(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "failure.sqlite"
            build_sqlite_fault_source(path, range(4), (range(4),), ((0, 1), (2, 3)), cover=range(4))
            output = []
            with SqlitePreparedFaultSource(path) as source:
                real_open = source.open_cover_deletion_row
                def open_injected_failure_cursor(snapshot_id, vertex):
                    cursor = real_open(snapshot_id, vertex)
                    def raise_injected_read_failure():
                        raise OSError("injected storage read failure")
                    cursor.read_next_neighbor_exact = raise_injected_read_failure
                    return cursor
                # Complete-row scans call the same opener. Inject only after
                # attachment scans, when the core helper opens its local row.
                from probe_fault_cover_connectivity import execute_capped_core_search
                def execute_injected_core_search(*args, **kwargs):
                    with patch.object(source, "open_cover_deletion_row", open_injected_failure_cursor):
                        return execute_capped_core_search(*args, **kwargs)
                with patch("probe_fault_cover_connectivity.execute_capped_core_search", execute_injected_core_search):
                    with self.assertRaisesRegex(OSError, "storage read failure"):
                        run_prepared_fault_connectivity(source, range(4), lambda *row: output.append(row),
                                                        max_state_nodes=5, core_plan="capped")
                self.assertEqual(source.events["active_cursors"], 0)
            self.assertEqual(output, [])

    def test_binary_workflow_oracle(self):
        import measure_capped_sqlite_workflow as measurement
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for kind, _, _ in measurement.CASES:
                vertices, groups, deleted, cover = measurement.construct_generated_source_streams(kind, 8, 6)
                vertices, groups, deleted, cover = tuple(vertices), tuple(map(tuple, groups)), tuple(deleted), tuple(cover)
                path = root / f"{kind}.sqlite"
                build_sqlite_fault_source(path, vertices, groups, deleted, cover=cover)
                expected = calculate_expanded_component_labels(vertices, groups, deleted, ())
                for plan in measurement.PLANS:
                    output = root / f"{kind}-{plan}.labels"
                    receipt = measurement.run_sqlite_connectivity_worker(path, output, plan)
                    audit = measurement.audit_generated_output_labels(output, kind, 8, 6)
                    actual = dict(measurement.struct.iter_unpack("<QQ", output.read_bytes()))
                    self.assertEqual(actual, expected)
                    self.assertEqual(receipt["output_sha256"], audit["output_sha256"])


if __name__ == "__main__":
    unittest.main()
