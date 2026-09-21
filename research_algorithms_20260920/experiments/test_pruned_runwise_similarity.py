"""Check exact whole-run pruning before expensive range winner access."""

import itertools
import tempfile
import unittest
from pathlib import Path

import probe_bounded_seed_similarity as candidate
from test_bounded_seed_similarity import compute_finite_similarity_oracle, load_runwise_reference_module


class PrunedRunwiseTests(unittest.TestCase):
    def require_pruned_selector_exists(self):
        solve = getattr(candidate, "select_pruned_runwise_answers", None)
        self.assertTrue(callable(solve), "select_pruned_runwise_answers")
        return solve

    def test_fragmented_dominated_runheads(self):
        solve = self.require_pruned_selector_exists()
        reference = load_runwise_reference_module(sort_capacity=32, sort_fanin=4)
        values = set(range(8))
        targets = [(0, values)] + [(j, ({0} if j % 2 == 0 else set()) | {1000 + j}) for j in range(1, 256)]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, values)
            rows, metrics = solve(reference, snapshot, source, None, 1, heap_cap=1)
            old_rows, old = candidate.select_merged_runwise_answers(reference, snapshot, source, None, 1, heap_cap=1)
            self.assertEqual(rows, compute_finite_similarity_oracle(targets, values, None, 1))
            self.assertEqual(rows, old_rows)
            self.assertEqual((metrics["R"], metrics["seed_intervals_seen"], metrics["seed_rmq_calls"], metrics["bound_pruned_runs"]), (256, 256, 1, 255))
            self.assertEqual(old["seed_rmq_calls"], 256)
            self.assertEqual((metrics["L"], metrics["E"], metrics["merged_events"]), (old["L"], old["E"], old["E"]))

    def test_equal_bound_identifiers(self):
        solve = self.require_pruned_selector_exists()
        reference = load_runwise_reference_module()
        targets = [(9, {0}), (2, {5}), (1, {0})]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, {0, 1})
            rows, metrics = solve(reference, snapshot, source, None, 1)
            self.assertEqual(rows, [(1, 2, 1, 1)])
            self.assertEqual((metrics["seed_rmq_calls"], metrics["bound_pruned_runs"]), (2, 1))

    def test_incomplete_seed_witnesses(self):
        solve = self.require_pruned_selector_exists()
        reference = load_runwise_reference_module()
        targets = [(9, {0, 1}), (2, {0}), (1, set()), (4, {0})]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            source = snapshot.root / "source"
            for sid, values, k in ((None, {0, 1}, 4), (9, {0, 1}, 3), (None, set(), 4)):
                reference.write_normalized_source_file(source, values)
                rows, metrics = solve(reference, snapshot, source, sid, k)
                self.assertEqual(rows, compute_finite_similarity_oracle(targets, values, sid, k))
                self.assertEqual(metrics["bound_pruned_runs"], 0)

    def test_exhaustive_pruned_answers(self):
        solve = self.require_pruned_selector_exists()
        reference = load_runwise_reference_module()
        subsets = (set(), {0}, {1}, {0, 1})
        skipped = 0
        with tempfile.TemporaryDirectory() as directory:
            for case, sets in enumerate(itertools.product(subsets, repeat=3)):
                targets = list(zip((9, -3, 2), sets))
                snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / str(case), targets))
                for values, sid, k in itertools.product(subsets, (None, -3), (0, 1, 2, 5)):
                    source = snapshot.root / "source"
                    reference.write_normalized_source_file(source, values)
                    rows, metrics = solve(reference, snapshot, source, sid, k)
                    self.assertEqual(rows, compute_finite_similarity_oracle(targets, values, sid, k))
                    self.assertEqual(metrics["seed_intervals_seen"], metrics["seed_rmq_calls"] + metrics["bound_pruned_runs"])
                    self.assertLessEqual(metrics["heap_peak"], max(0, 2 * len(rows) - 1))
                    skipped += metrics["bound_pruned_runs"]
                    self.assertFalse(list(snapshot.root.glob("query-*")))
        self.assertGreater(skipped, 0)

    def test_public_source_selection(self):
        import bench_bounded_seed_similarity as benchmark
        choose = getattr(benchmark, "select_public_query_sources", None)
        self.assertTrue(callable(choose), "select_public_query_sources")
        graph = {node: set(range(node)) for node in range(10)}
        strata = choose(graph)
        expanded = choose(graph, sample_count=2, sample_seed=920520)
        self.assertEqual(strata, [0, 2, 5, 7, 8, 9])
        self.assertEqual(expanded[:6], strata)
        self.assertEqual(len(set(expanded)), 8)
        self.assertEqual(expanded, choose(graph, sample_count=2, sample_seed=920520))
        self.assertTrue(set(expanded[6:]).isdisjoint(strata))
        for count in (-1, 5, True):
            with self.subTest(count=count), self.assertRaises(ValueError):
                choose(graph, sample_count=count)


if __name__ == "__main__":
    unittest.main()
