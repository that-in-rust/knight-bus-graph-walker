"""Independent feasible-row oracle and finite-file certificate contracts."""

import importlib.util
import gzip
import itertools
import tempfile
import types
import unittest
from fractions import Fraction
from pathlib import Path

from test_bounded_seed_similarity import compute_finite_similarity_oracle, load_runwise_reference_module

if importlib.util.find_spec("probe_laminar_capacity_similarity"):
    import probe_laminar_capacity_similarity as candidate
else:
    candidate = types.SimpleNamespace()


def compute_independent_group_counts(values, groups):
    result = [0] * (2 * groups)
    for feature in values:
        index = groups + feature % groups
        while index:
            result[index] += 1
            index //= 2
    return result


class LaminarCapacityTests(unittest.TestCase):
    def require_capacity_functions_exist(self):
        names = ("compute_laminar_jaccard_bound", "build_laminar_capacity_sidecar", "select_laminar_capacity_answers")
        for name in names:
            self.assertTrue(callable(getattr(candidate, name, None)), name)
        return tuple(getattr(candidate, name) for name in names)

    def test_exact_relaxation_envelope(self):
        bound, _, _ = self.require_capacity_functions_exist()
        subsets = [set(j for j in range(4) if mask & (1 << j)) for mask in range(16)]
        for left, right in itertools.combinations_with_replacement(subsets, 2):
            union = left | right
            capacities = [max(a, b) for a, b in zip(compute_independent_group_counts(left, 4),
                                                   compute_independent_group_counts(right, 4))]
            minimum, maximum = min(len(left), len(right)), max(len(left), len(right))
            feasible = [row for row in subsets if row <= union and minimum <= len(row) <= maximum
                        and all(x <= c for x, c in zip(compute_independent_group_counts(row, 4), capacities))]
            for raw, unknown in itertools.product(subsets, (False, True)):
                source = raw | ({8} if unknown else set())
                counts = compute_independent_group_counts(source & union, 4)[4:]
                expected = max(Fraction(len(source & row), len(source | row)) if source & row else Fraction(0)
                               for row in feasible)
                actual = bound(len(source), minimum, capacities, counts, "laminar")
                flat = bound(len(source), minimum, capacities, counts, "partition")
                plain = bound(len(source), minimum, capacities, counts, "union")
                self.assertEqual(actual, expected, (left, right, source))
                self.assertLessEqual(actual, flat)
                self.assertLessEqual(flat, plain)

    def test_internal_capacity_separation(self):
        bound, _, _ = self.require_capacity_functions_exist()
        capacities = [0, 2, 1, 1, 1, 1, 1, 1]
        self.assertEqual(bound(2, 2, capacities, [1, 1, 0, 0], "union"), 1)
        self.assertEqual(bound(2, 2, capacities, [1, 1, 0, 0], "partition"), 1)
        self.assertEqual(bound(2, 2, capacities, [1, 1, 0, 0], "laminar"), Fraction(1, 3))

    def test_complete_file_answers(self):
        _, build, solve = self.require_capacity_functions_exist()
        reference = load_runwise_reference_module(sort_capacity=32, sort_fanin=4)
        subsets = (set(), {0}, {1}, {0, 1})
        with tempfile.TemporaryDirectory() as directory:
            for case, sets in enumerate(itertools.product(subsets, repeat=3)):
                targets = list(zip((9, -3, 2), sets))
                snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / str(case), targets))
                sidecar, built = build(reference, snapshot, groups=4, block_size=2)
                self.assertEqual(sidecar.stat().st_size, built["retained_bytes"])
                for values, sid, k, mode in itertools.product(subsets, (None, -3), (0, 1, 2, 5),
                                                              ("union", "partition", "laminar", "paired", "paired_lazy")):
                    source = snapshot.root / "source"
                    reference.write_normalized_source_file(source, values | {100})
                    rows, metrics = solve(reference, snapshot, sidecar, source, sid, k, mode=mode)
                    self.assertEqual(rows, compute_finite_similarity_oracle(targets, values | {100}, sid, k))
                    self.assertLessEqual(metrics["heap_peak"], len(rows))
                    self.assertFalse(list(snapshot.root.glob("query-*")))

    def test_pruning_avoids_bodies(self):
        _, build, solve = self.require_capacity_functions_exist()
        reference = load_runwise_reference_module(sort_capacity=32, sort_fanin=4)
        targets = [(0, {0, 1, 4}), (1, {0, 1, 5}), (10, {0, 2}), (11, {1, 3}),
                   (12, {0, 2}), (13, {1, 3})]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            sidecar, _ = build(reference, snapshot, groups=4, block_size=2)
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, {0, 1})
            expected = compute_finite_similarity_oracle(targets, {0, 1}, None, 2)
            for mode in ("union", "partition"):
                rows, metrics = solve(reference, snapshot, sidecar, source, None, 2, mode=mode)
                self.assertEqual(rows, expected)
                self.assertEqual(metrics["body_targets"], 6)
            reader = reference.read_fixed_file_records

            def reject_pruned_body_reads(path, layout, tag, offset=0, count=None):
                if tag == "capacity_body_pairs" and offset >= 6 * reference.PAIR.size:
                    raise AssertionError("pruned body opened")
                return reader(path, layout, tag, offset, count)

            reference.read_fixed_file_records = reject_pruned_body_reads
            rows, metrics = solve(reference, snapshot, sidecar, source, None, 2)
            self.assertEqual(rows, expected)
            self.assertEqual((metrics["body_targets"], metrics["blocks_pruned"]), (2, 2))
            self.assertEqual(metrics["body_memberships"], 6)

    def test_equal_score_identifiers(self):
        _, build, solve = self.require_capacity_functions_exist()
        reference = load_runwise_reference_module()
        targets = [(8, {0}), (9, {0}), (-5, {0}), (10, {0}), (11, {0})]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            sidecar, _ = build(reference, snapshot, groups=2, block_size=1)
            source = snapshot.root / "source"
            for values in ({0}, set()):
                reference.write_normalized_source_file(source, values)
                rows, metrics = solve(reference, snapshot, sidecar, source, 8, 1)
                self.assertEqual(rows, compute_finite_similarity_oracle(targets, values, 8, 1))
                self.assertEqual(rows[0][0], -5)
                self.assertGreater(metrics["blocks_pruned"], 0)

    def test_admission_failure_cleanup(self):
        _, build, solve = self.require_capacity_functions_exist()
        reference = load_runwise_reference_module()
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", [(1, {0, 2}), (2, {1})]))
            sidecar, _ = build(reference, snapshot, groups=4, block_size=2)
            old = sidecar.read_bytes()
            for options in ({"union_cap": 1}, {"disk_cap": 1}, {"groups": 3}, {"group_cap": 2}):
                with self.subTest(options=options), self.assertRaises(AssertionError):
                    build(reference, snapshot, **({"groups": 4, "block_size": 2} | options))
                self.assertEqual(sidecar.read_bytes(), old)
                self.assertFalse(list(snapshot.root.glob("capacity-build-*")))
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, {0, 1, 2})
            for options in ({"source_cap": 2}, {"heap_cap": 0}, {"output_cap": 0}, {"disk_cap": 0}, {"group_cap": 2}):
                with self.subTest(options=options), self.assertRaises(AssertionError):
                    solve(reference, snapshot, sidecar, source, None, 1, **options)
                self.assertFalse(list(snapshot.root.glob("query-*")))
            with self.assertRaisesRegex(OSError, "sink"):
                solve(reference, snapshot, sidecar, source, None, 2, fail_after=1)
            self.assertFalse(list(snapshot.root.glob("query-*")))
            snapshot.token = snapshot.meta_token = snapshot.inverse_token = (99,) + snapshot.token[1:]
            with self.assertRaisesRegex(AssertionError, "generation"):
                solve(reference, snapshot, sidecar, source, None, 1)

    def test_empty_snapshot_output(self):
        _, build, solve = self.require_capacity_functions_exist()
        reference = load_runwise_reference_module()
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", []))
            sidecar, built = build(reference, snapshot, groups=4, block_size=2)
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, {0})
            rows, _ = solve(reference, snapshot, sidecar, source, None, 3)
            self.assertEqual(rows, [])
            self.assertEqual(built["retained_bytes"], 40)

    def test_specialized_summary_payloads(self):
        _, build, solve = self.require_capacity_functions_exist()
        reference = load_runwise_reference_module()
        targets = [(1, {0, 2}), (2, {1, 3})]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, {0, 1})
            for mode, words in (("union", 0), ("partition", 4), ("laminar", 7)):
                sidecar, built = build(reference, snapshot, groups=4, block_size=2, mode=mode)
                self.assertEqual(built["retained_bytes"], 40 + 56 + 8 * 4 + 8 * words)
                rows, metrics = solve(reference, snapshot, sidecar, source, None, 1, mode=mode)
                self.assertEqual(rows, compute_finite_similarity_oracle(targets, {0, 1}, None, 1))
                self.assertEqual(metrics["capacity_bytes"], words * 8)

    def test_public_comparison_receipt(self):
        spec = importlib.util.find_spec("bench_laminar_capacity_similarity")
        self.assertIsNotNone(spec, "bench_laminar_capacity_similarity")
        from bench_laminar_capacity_similarity import run_laminar_public_comparison
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data, output = root / "graph.gz", root / "receipt.json"
            with gzip.open(data, "wt", encoding="ascii") as stream:
                stream.write("1 2\n2 3\n3 4\n4 5\n5 1\n")
            report = run_laminar_public_comparison(data, output, groups=4, block_size=2,
                                                   sample_count=0, repeats=1)
            self.assertEqual(report["complete_results_checked"], 4 * len(report["selected_source_ids"]))
            self.assertEqual(set(report["sidecar_builds"]), {"union", "partition", "laminar"})
            self.assertTrue(output.exists())
            paired_report = run_laminar_public_comparison(data, output, groups=4, block_size=2,
                                                          sample_count=0, repeats=1, include_paired=True)
            self.assertEqual(paired_report["complete_results_checked"], 6 * len(report["selected_source_ids"]))
            self.assertIn("paired", paired_report["summaries"][0])

    def test_paired_complete_envelope(self):
        paired = getattr(candidate, "compute_paired_capacity_bound", None)
        self.assertTrue(callable(paired), "compute_paired_capacity_bound")
        subsets = [set(j for j in range(4) if mask & (1 << j)) for mask in range(16)]
        for left, right in itertools.combinations_with_replacement(subsets, 2):
            if left & right:
                continue
            union = left | right
            occupancy = compute_independent_group_counts(union, 4)
            capacities = [max(a, b) for a, b in zip(compute_independent_group_counts(left, 4),
                                                   compute_independent_group_counts(right, 4))]
            minimum, maximum = sorted((len(left), len(right)))
            compatible = []
            for row in subsets:
                if not row <= union or sorted((len(row), len(union - row))) != [minimum, maximum]:
                    continue
                other = union - row
                maxima = [max(a, b) for a, b in zip(compute_independent_group_counts(row, 4),
                                                  compute_independent_group_counts(other, 4))]
                if maxima == capacities:
                    compatible.append(row)
            for raw, unknown in itertools.product(subsets, (False, True)):
                source = raw | ({8} if unknown else set())
                counts = compute_independent_group_counts(source & union, 4)[4:]
                expected = max(Fraction(len(source & row), len(source | row)) if source & row else Fraction(0)
                               for row in compatible)
                self.assertEqual(paired(len(source), minimum, capacities, counts, occupancy[4:]), expected)
        self.assertEqual(paired(4, 4, [0, 4, 3, 3], [2, 2], [4, 4]), Fraction(3, 5))

    def test_paired_body_pruning(self):
        _, build, solve = self.require_capacity_functions_exist()
        reference = load_runwise_reference_module()
        targets = [(0, {0, 2, 1, 3, 8}), (1, {0, 2, 1, 3, 9}),
                   (10, {0, 2, 4, 1}), (11, {6, 3, 5, 7}), (20, {20}), (21, {21})]
        source_set = {0, 2, 1, 3}
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            sidecar, _ = build(reference, snapshot, groups=2, block_size=2)
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, source_set)
            expected = compute_finite_similarity_oracle(targets, source_set, None, 2)
            rows, before = solve(reference, snapshot, sidecar, source, None, 2)
            self.assertEqual(rows, expected)
            self.assertEqual(before["body_targets"], 4)
            rows, after = solve(reference, snapshot, sidecar, source, None, 2, mode="paired")
            self.assertEqual(rows, expected)
            self.assertEqual((after["body_targets"], after["paired_eligible"], after["paired_tighter"]), (2, 2, 1))
            rows, lazy = solve(reference, snapshot, sidecar, source, None, 2, mode="paired_lazy")
            self.assertEqual(rows, expected)
            self.assertEqual((lazy["body_targets"], lazy["paired_evaluations"]), (2, 1))

    def test_three_rows_fallback(self):
        _, build, solve = self.require_capacity_functions_exist()
        reference = load_runwise_reference_module()
        targets = [(1, {0, 2}), (2, {0, 1}), (3, {1, 3})]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            sidecar, _ = build(reference, snapshot, groups=2, block_size=3)
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, {0, 1})
            rows, metrics = solve(reference, snapshot, sidecar, source, None, 1, mode="paired")
            self.assertEqual(rows, compute_finite_similarity_oracle(targets, {0, 1}, None, 1))
            self.assertEqual(metrics["paired_eligible"], 0)

    def test_paired_certificate_limitations(self):
        paired = candidate.compute_paired_capacity_bound
        with self.assertRaisesRegex(AssertionError, "inconsistent"):
            paired(2, 1, [0, 3, 2, 2], [1, 1], [2, 2])
        self.assertEqual(paired(2, 2, [0, 2, 1, 1], [1, 1], [2, 2]), 1)
        # Loose caps can be safe upper bounds but cannot be inverted as attained maxima.
        self.assertEqual(paired(2, 2, [0, 2, 2, 2], [1, 1], [2, 2]), Fraction(1, 3))


if __name__ == "__main__":
    unittest.main()
