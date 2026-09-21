"""Exact endpoint-merge control, including admission and cleanup boundaries."""

import itertools
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import probe_bounded_seed_similarity as candidate
from test_bounded_seed_similarity import (
    compute_finite_similarity_oracle,
    load_runwise_reference_module,
)


class MergedRunwiseTests(unittest.TestCase):
    def require_merged_selector_exists(self):
        solve = getattr(candidate, "select_merged_runwise_answers", None)
        self.assertTrue(callable(solve), "select_merged_runwise_answers")
        return solve

    def test_complete_exhaustive_merge(self):
        solve = self.require_merged_selector_exists()
        reference = load_runwise_reference_module()
        subsets = (set(), {0}, {1}, {0, 1})
        with tempfile.TemporaryDirectory() as directory:
            for case, sets in enumerate(itertools.product(subsets, repeat=3)):
                targets = list(zip((9, -3, 2), sets))
                snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / str(case), targets))
                for values, sid, k in itertools.product(subsets, (None, -3), (0, 1, 2, 5)):
                    source = snapshot.root / "source"
                    reference.write_normalized_source_file(source, values)
                    rows, metrics = solve(reference, snapshot, source, sid, k, merge_cap=2)
                    self.assertEqual(rows, compute_finite_similarity_oracle(targets, values, sid, k))
                    self.assertLessEqual(metrics["merge_streams"], len(values))
                    self.assertEqual(metrics["merged_events"], metrics["E"])
                    self.assertFalse(list(snapshot.root.glob("query-*")))

    def test_nonexpanding_long_runs(self):
        solve = self.require_merged_selector_exists()
        reference = load_runwise_reference_module()
        targets = [(1024 - j, {0, 1, 2, 100 + j}) for j in range(1024)]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, {0, 1, 2, 99999})
            before = reference.stats.copy()
            rows, metrics = solve(reference, snapshot, source, None, 3, merge_cap=3, disk_cap=272)
            self.assertEqual(rows, compute_finite_similarity_oracle(targets, {0, 1, 2, 99999}, None, 3))
            self.assertEqual((metrics["a"], metrics["merge_streams"], metrics["L"], metrics["E"], metrics["R"]), (4, 3, 3, 6, 1))
            self.assertEqual(reference.stats["build_pairs_records"], before["build_pairs_records"])
            self.assertEqual(reference.stats["sort_buffer_peak"], before["sort_buffer_peak"])
            self.assertFalse(list(snapshot.root.glob("query-*")))

    def test_merge_budget_refusals(self):
        solve = self.require_merged_selector_exists()
        reference = load_runwise_reference_module()
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", [(9, {0, 1}), (1, {1})]))
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, {0, 1})
            for options in ({"merge_cap": 1}, {"disk_cap": 32}, {"merge_cap": -1}, {"merge_cap": True}):
                with self.subTest(options=options), self.assertRaises((AssertionError, ValueError)):
                    solve(reference, snapshot, source, None, 1, **options)
                self.assertFalse(list(snapshot.root.glob("query-*")))
            reference.write_normalized_source_file(source, {9999})
            rows, metrics = solve(reference, snapshot, source, None, 2, merge_cap=0)
            self.assertEqual([row[0] for row in rows], [1, 9])
            self.assertEqual(metrics["merge_streams"], 0)

    def test_merged_read_failure(self):
        solve = self.require_merged_selector_exists()
        reference = load_runwise_reference_module()
        targets = [(j, {0} if j % 2 else {1}) for j in range(10)]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, {0, 1})
            original = reference.read_fixed_file_records
            live = set()

            def inject_interval_read_failure(path, layout, tag, *args, **kwargs):
                marker = object()
                live.add(marker)
                try:
                    for index, row in enumerate(original(path, layout, tag, *args, **kwargs)):
                        if path == snapshot.posts and index == 1:
                            raise OSError("injected interval read")
                        yield row
                finally:
                    live.remove(marker)

            with patch.object(reference, "read_fixed_file_records", inject_interval_read_failure):
                with self.assertRaisesRegex(OSError, "interval read"):
                    solve(reference, snapshot, source, None, 3)
            self.assertFalse(live)
            self.assertFalse(list(snapshot.root.glob("query-*")))

    def test_sort_budget_configuration(self):
        try:
            reference = load_runwise_reference_module(sort_capacity=16, sort_fanin=4)
        except TypeError:
            self.fail("load_runwise_reference_module must accept explicit sort budgets")
        with tempfile.TemporaryDirectory() as directory:
            arena = reference.Arena(Path(directory), 100000)
            records = [(value,) for value in range(63, -1, -1)]
            path = reference.sort_finite_record_stream(iter(records), reference.U, 64, arena)
            self.assertEqual(list(reference.read_fixed_file_records(path, reference.U, "check")), sorted(records))
            self.assertEqual(reference.stats["sort_buffer_peak"], 16)
            self.assertEqual(reference.stats["fanin_peak"], 4)
            arena.remove(path)


if __name__ == "__main__":
    unittest.main()
