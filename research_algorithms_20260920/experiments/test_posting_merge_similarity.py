"""Same-source DAAT comparator, with complete positive and zero-score output."""

import importlib.util
import itertools
import tempfile
import types
import unittest
from pathlib import Path

from test_bounded_seed_similarity import compute_finite_similarity_oracle, load_runwise_reference_module

if importlib.util.find_spec("probe_posting_merge_similarity"):
    import probe_posting_merge_similarity as comparator
else:
    comparator = types.SimpleNamespace()


class PostingMergeTests(unittest.TestCase):
    def require_posting_comparator_exists(self):
        solve = getattr(comparator, "select_posting_merge_answers", None)
        self.assertTrue(callable(solve), "select_posting_merge_answers")
        return solve

    def test_exhaustive_complete_answers(self):
        solve = self.require_posting_comparator_exists()
        reference = load_runwise_reference_module()
        subsets = (set(), {0}, {1}, {0, 1})
        with tempfile.TemporaryDirectory() as directory:
            for case, sets in enumerate(itertools.product(subsets, repeat=3)):
                targets = list(zip((9, -3, 2), sets))
                snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / str(case), targets))
                for values, sid, k in itertools.product(subsets, (None, -3), (0, 1, 2, 5)):
                    source = snapshot.root / "source"
                    reference.write_normalized_source_file(source, values)
                    rows, metrics = solve(reference, snapshot, source, sid, k)
                    self.assertEqual(rows, compute_finite_similarity_oracle(targets, values, sid, k))
                    self.assertLessEqual(metrics["heap_peak"], len(rows))
                    self.assertFalse(list(snapshot.root.glob("query-*")))

    def test_sparse_membership_visits(self):
        solve = self.require_posting_comparator_exists()
        reference = load_runwise_reference_module(sort_capacity=32, sort_fanin=4)
        targets = [(100 - j, {1000 + j} | ({0} if j < 3 else set())) for j in range(100)]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, {0})
            rows, metrics = solve(reference, snapshot, source, None, 5)
            self.assertEqual(rows, compute_finite_similarity_oracle(targets, {0}, None, 5))
            self.assertEqual((metrics["membership_visits"], metrics["candidate_targets"], metrics["zero_rows"]), (3, 3, 2))

    def test_long_run_expansion(self):
        solve = self.require_posting_comparator_exists()
        reference = load_runwise_reference_module(sort_capacity=32, sort_fanin=4)
        targets = [(128 - j, {0, 1, 1000 + j}) for j in range(128)]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, {0, 1})
            rows, metrics = solve(reference, snapshot, source, None, 2)
            self.assertEqual(rows, compute_finite_similarity_oracle(targets, {0, 1}, None, 2))
            self.assertEqual((metrics["L"], metrics["membership_visits"], metrics["candidate_targets"]), (2, 256, 128))

    def test_admission_and_cleanup(self):
        solve = self.require_posting_comparator_exists()
        reference = load_runwise_reference_module()
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", [(1, {0}), (2, {1})]))
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, {0, 1})
            for options in ({"merge_cap": 1}, {"heap_cap": 0}, {"output_cap": 0}, {"disk_cap": 0}):
                with self.subTest(options=options), self.assertRaises(AssertionError):
                    solve(reference, snapshot, source, None, 1, **options)
                self.assertFalse(list(snapshot.root.glob("query-*")))
            with self.assertRaisesRegex(OSError, "sink"):
                solve(reference, snapshot, source, None, 2, fail_after=1)
            self.assertFalse(list(snapshot.root.glob("query-*")))


if __name__ == "__main__":
    unittest.main()
