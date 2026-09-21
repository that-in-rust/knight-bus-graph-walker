"""Selective block access and a same-information lower/upper-quota oracle."""

import importlib.util
import gzip
import itertools
import tempfile
import types
import unittest
from fractions import Fraction
from pathlib import Path

from probe_laminar_capacity_similarity import build_laminar_capacity_sidecar
from test_bounded_seed_similarity import compute_finite_similarity_oracle, load_runwise_reference_module
from test_laminar_capacity_similarity import compute_independent_group_counts

if importlib.util.find_spec("probe_selective_capacity_similarity"):
    import probe_selective_capacity_similarity as candidate
else:
    candidate = types.SimpleNamespace()


class SelectiveCapacityTests(unittest.TestCase):
    def require_selective_functions_exist(self):
        names = ("compute_interval_capacity_bound", "build_selective_capacity_index", "select_indexed_capacity_answers")
        for name in names:
            self.assertTrue(callable(getattr(candidate, name, None)), name)
        return tuple(getattr(candidate, name) for name in names)

    def test_interval_relaxation_enumeration(self):
        bound, _, _ = self.require_selective_functions_exist()
        subsets = [set(j for j in range(4) if mask & (1 << j)) for mask in range(16)]
        for rows in itertools.combinations_with_replacement(subsets, 2):
            union = set.union(*rows)
            populations = compute_independent_group_counts(union, 4)
            capacities = [max(values) for values in zip(*(compute_independent_group_counts(row, 4) for row in rows))]
            minimum, maximum = sorted(map(len, rows))
            lower = [max(0, h - c) for h, c in zip(populations, capacities)]
            feasible = [row for row in subsets if row <= union and len(row) in (minimum, maximum)
                        and all(lo <= x <= hi for lo, x, hi in zip(lower, compute_independent_group_counts(row, 4), capacities))]
            for raw, outside in itertools.product(subsets, (False, True)):
                source = raw | ({8} if outside else set())
                counts = compute_independent_group_counts(source & union, 4)[4:]
                expected = max(Fraction(len(source & row), len(source | row)) if source & row else Fraction(0)
                               for row in feasible)
                self.assertEqual(bound(len(source), minimum, capacities, counts, populations[4:], 2), expected)
        self.assertEqual(bound(4, 4, [0, 4, 3, 3], [2, 2], [4, 4], 2), 1)

    def test_complete_selective_answers(self):
        _, build, solve = self.require_selective_functions_exist()
        reference = load_runwise_reference_module(sort_capacity=32, sort_fanin=4)
        subsets = (set(), {0}, {1}, {0, 1})
        with tempfile.TemporaryDirectory() as directory:
            for case, sets in enumerate(itertools.product(subsets, repeat=3)):
                targets = list(zip((9, -3, 2), sets))
                snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / str(case), targets))
                sidecar, _ = build_laminar_capacity_sidecar(reference, snapshot, groups=4, block_size=2)
                index, metrics = build(reference, snapshot, sidecar)
                self.assertEqual(metrics["retained_bytes"], sum(path.stat().st_size for path in index.root.iterdir()))
                for raw, sid, k, mode in itertools.product(subsets, (None, -3), (0, 1, 2, 5),
                                                          ("union", "partition", "laminar", "interval", "paired")):
                    source = snapshot.root / "source"
                    values = raw | {100}
                    reference.write_normalized_source_file(source, values)
                    rows, local = solve(reference, snapshot, index, source, sid, k, mode=mode)
                    self.assertEqual(rows, compute_finite_similarity_oracle(targets, values, sid, k))
                    self.assertLessEqual(local["heap_peak"], len(rows))
                    self.assertFalse(list(snapshot.root.glob("query-*")))

    def test_sparse_block_access(self):
        _, build, solve = self.require_selective_functions_exist()
        reference = load_runwise_reference_module(sort_capacity=32, sort_fanin=4)
        targets = [(100 - j, {1000 + j} | ({0} if j < 3 else set())) for j in range(100)]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            sidecar, _ = build_laminar_capacity_sidecar(reference, snapshot, groups=4, block_size=2)
            index, _ = build(reference, snapshot, sidecar)
            source = snapshot.root / "source"
            for values, sid in (({0}, None), (set(), None), ({1000}, 100)):
                reference.write_normalized_source_file(source, values)
                rows, metrics = solve(reference, snapshot, index, source, sid, 5)
                self.assertEqual(rows, compute_finite_similarity_oracle(targets, values, sid, 5))
                self.assertLessEqual(metrics["blocks_seen"], 2)
                self.assertLessEqual(metrics["body_targets"], 4)

    def test_discrete_pruning_advantage(self):
        _, build, solve = self.require_selective_functions_exist()
        reference = load_runwise_reference_module()
        targets = [(0, {0, 2, 1, 3, 8}), (1, {0, 2, 1, 3, 9}),
                   (10, {0, 2, 4, 1}), (11, {6, 3, 5, 7})]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            sidecar, _ = build_laminar_capacity_sidecar(reference, snapshot, groups=2, block_size=2)
            index, _ = build(reference, snapshot, sidecar)
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, {0, 2, 1, 3})
            rows, interval = solve(reference, snapshot, index, source, None, 2, mode="interval")
            paired_rows, paired = solve(reference, snapshot, index, source, None, 2, mode="paired")
            self.assertEqual(rows, paired_rows)
            self.assertEqual((interval["body_targets"], paired["body_targets"]), (4, 2))

    def test_admission_cleanup_contract(self):
        _, build, solve = self.require_selective_functions_exist()
        reference = load_runwise_reference_module()
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", [(1, {0, 2}), (2, {1})]))
            sidecar, _ = build_laminar_capacity_sidecar(reference, snapshot, groups=4, block_size=2)
            with self.assertRaises(AssertionError):
                build(reference, snapshot, sidecar, disk_cap=1)
            self.assertFalse(list(snapshot.root.glob("selective-build-*")))
            index, _ = build(reference, snapshot, sidecar)
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, {0, 1, 2})
            for options in ({"source_cap": 2}, {"merge_cap": 1}, {"heap_cap": 0}, {"output_cap": 0}, {"disk_cap": 0}, {"group_cap": 2}):
                with self.subTest(options=options), self.assertRaises(AssertionError):
                    solve(reference, snapshot, index, source, None, 1, **options)
            with self.assertRaisesRegex(OSError, "sink"):
                solve(reference, snapshot, index, source, None, 2, fail_after=1)
            self.assertFalse(list(snapshot.root.glob("query-*")))

    def test_three_row_interval(self):
        bound, _, _ = self.require_selective_functions_exist()
        subsets = (set(), {0}, {1}, {0, 1})
        for rows in itertools.combinations_with_replacement(subsets, 3):
            union = set.union(*rows)
            populations = compute_independent_group_counts(union, 2)
            capacities = [max(values) for values in zip(*(compute_independent_group_counts(row, 2) for row in rows))]
            minimum, maximum = min(map(len, rows)), max(map(len, rows))
            lower = [max(0, h - 2 * c) for h, c in zip(populations, capacities)]
            feasible = [row for row in subsets if row <= union and minimum <= len(row) <= maximum
                        and all(lo <= x <= hi for lo, x, hi in zip(lower, compute_independent_group_counts(row, 2), capacities))]
            for raw, outside in itertools.product(subsets, (False, True)):
                source = raw | ({8} if outside else set())
                counts = compute_independent_group_counts(source & union, 2)[2:]
                expected = max(Fraction(len(source & row), len(source | row)) if source & row else Fraction(0)
                               for row in feasible)
                self.assertEqual(bound(len(source), minimum, capacities, counts, populations[2:], 3), expected)
        self.assertEqual(bound(2, 1, [0, 3, 2, 1], [1, 1], [2, 1], 3), 1)

    def test_mapped_interval_coalescing(self):
        _, build, solve = self.require_selective_functions_exist()
        reference = load_runwise_reference_module()
        targets = [(j, {0} if j % 2 == 0 else {1}) for j in range(6)]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            sidecar, _ = build_laminar_capacity_sidecar(reference, snapshot, groups=2, block_size=2)
            index, _ = build(reference, snapshot, sidecar)
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, {0})
            rows, metrics = solve(reference, snapshot, index, source, None, 1)
            self.assertEqual(rows, compute_finite_similarity_oracle(targets, {0}, None, 1))
            self.assertEqual((snapshot.feature(0)[2], metrics["posting_intervals"], metrics["block_memberships"]), (3, 1, 3))

    def test_public_driver_receipt(self):
        self.assertIsNotNone(importlib.util.find_spec("bench_selective_capacity_similarity"), "selective benchmark driver")
        from bench_selective_capacity_similarity import run_selective_public_comparison
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data, output = root / "fixture.gz", root / "receipt.json"
            with gzip.open(data, "wt", encoding="ascii") as stream:
                stream.write("1 2\n2 3\n3 4\n4 1\n")
            report = run_selective_public_comparison(data, output, sample_count=0, repeats=1)
            self.assertEqual(report["complete_results_checked"], 6 * len(report["selected_source_ids"]))
            self.assertTrue(output.exists())


if __name__ == "__main__":
    unittest.main()
