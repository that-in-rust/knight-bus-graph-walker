"""Budgeted pair-bound dispatch and complete selective-query contracts."""

import importlib.util
import gzip
import inspect
import itertools
import tempfile
import types
import unittest
from fractions import Fraction
from pathlib import Path
from unittest.mock import patch

from probe_laminar_capacity_similarity import build_laminar_capacity_sidecar
from probe_selective_capacity_similarity import build_selective_capacity_index, select_indexed_capacity_answers
from test_bounded_seed_similarity import compute_finite_similarity_oracle, load_runwise_reference_module

if importlib.util.find_spec("probe_admitted_similarity_bounds"):
    import probe_admitted_similarity_bounds as candidate
else:
    candidate = types.SimpleNamespace()


class AdmittedOverlapTests(unittest.TestCase):
    def require_admitted_solver_function(self):
        solver = getattr(candidate, "solve_admitted_pair_envelope", None)
        self.assertTrue(callable(solver), "solve_admitted_pair_envelope")
        return solver

    def test_dispatch_without_allocation(self):
        solve = self.require_admitted_solver_function()
        for shared, method in ((1, "dense"), (16, "threshold"), (10**12, "threshold")):
            args = 4, shared + 4, [0, shared + 4, 4, shared, 3, 3, shared, 0], [2, 2, 0, 0], [4, 4, shared, 0]
            score, metrics = solve(*args, state_cap=1000, work_cap=1000)
            self.assertEqual(score, Fraction(3, shared + 5))
            self.assertEqual(metrics["solver"], method)
            self.assertLessEqual(metrics["states"], metrics["reserved_states"])
            self.assertLessEqual(metrics["work"], metrics["reserved_work"])
        with patch.object(candidate, "compute_overlap_capacity_bound", side_effect=AssertionError("dense allocated")), \
             patch.object(candidate, "compute_query_threshold_bound", side_effect=AssertionError("threshold allocated")):
            score, metrics = solve(*args, state_cap=0, work_cap=0)
            self.assertIsNone(score)
            self.assertEqual(metrics["solver"], "refused")
            self.assertEqual(metrics["states"], 0)

    def test_forced_solvers_and_errors(self):
        solve = self.require_admitted_solver_function()
        args = 4, 20, [0, 20, 4, 16, 3, 3, 16, 0], [2, 2, 0, 0], [4, 4, 16, 0]
        for strategy in ("dense", "threshold"):
            score, metrics = solve(*args, strategy=strategy, state_cap=200, work_cap=1000)
            self.assertEqual(score, Fraction(1, 7))
            self.assertEqual(metrics["solver"], strategy)
        for options in ({"strategy": "unknown"}, {"state_cap": True}, {"work_cap": -1}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                solve(*args, **options)
        with self.assertRaisesRegex(ValueError, "no complete pair"):
            solve(0, 1, [0, 3, 2, 2], [0, 0], [2, 2])

    def test_overlap_pruning_and_fallback(self):
        self.require_admitted_solver_function()
        reference = load_runwise_reference_module()
        query = {0, 4, 1, 5, 2}
        targets = [(0, query | {77}), (1, query | {78}),
                   (10, {0, 4, 8, 1, 2}), (11, {12, 5, 9, 13, 2})]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            sidecar, _ = build_laminar_capacity_sidecar(reference, snapshot, groups=4, block_size=2)
            index, _ = build_selective_capacity_index(reference, snapshot, sidecar)
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, query)
            expected = compute_finite_similarity_oracle(targets, query, None, 2)
            rows, interval = select_indexed_capacity_answers(reference, snapshot, index, source, None, 2, mode="interval")
            self.assertEqual(rows, expected)
            self.assertEqual(interval["body_targets"], 4)
            for mode in ("overlap", "overlap_dense", "overlap_threshold"):
                rows, metrics = select_indexed_capacity_answers(reference, snapshot, index, source, None, 2, mode=mode)
                self.assertEqual(rows, expected)
                self.assertEqual(metrics["body_targets"], 2)
                self.assertEqual(metrics["overlap_evaluations"], 1)
            for options in ({"pair_state_cap": 0}, {"pair_work_cap": 0},
                            {"pair_query_work_cap": 0}):
                rows, metrics = select_indexed_capacity_answers(reference, snapshot, index, source, None, 2,
                                                                 mode="overlap", **options)
                self.assertEqual(rows, expected)
                self.assertEqual(metrics["body_targets"], 4)
                self.assertEqual(metrics["overlap_refusals"], 1)
                self.assertEqual(metrics["overlap_evaluations"], 0)

    def test_complete_overlap_file_matrix(self):
        self.require_admitted_solver_function()
        reference = load_runwise_reference_module(sort_capacity=32, sort_fanin=4)
        subsets = (set(), {0}, {1}, {0, 1})
        checked = 0
        with tempfile.TemporaryDirectory() as directory:
            for case, values in enumerate(itertools.product(subsets, repeat=3)):
                targets = list(zip((9, -3, 2), values))
                snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / str(case), targets))
                sidecar, _ = build_laminar_capacity_sidecar(reference, snapshot, groups=2, block_size=2)
                index, _ = build_selective_capacity_index(reference, snapshot, sidecar)
                source = snapshot.root / "source"
                for query, sid, k, mode in itertools.product(subsets, (None, -3), (0, 1, 2, 5),
                                                              ("overlap", "overlap_dense", "overlap_threshold", "refused")):
                    reference.write_normalized_source_file(source, query | {99})
                    options = {"mode": mode} if mode != "refused" else {"mode": "overlap", "pair_state_cap": 0}
                    rows, metrics = select_indexed_capacity_answers(reference, snapshot, index, source, sid, k, **options)
                    self.assertEqual(rows, compute_finite_similarity_oracle(targets, query | {99}, sid, k))
                    self.assertLessEqual(metrics["pair_reserved_work_total"], 10**7)
                    self.assertFalse(list(snapshot.root.glob("query-*")))
                    checked += 1
        self.assertEqual(checked, 8192)

    def test_cumulative_budget_debits(self):
        solve = self.require_admitted_solver_function()
        reference = load_runwise_reference_module()
        query = {0, 4, 1, 5, 2}
        pair = ({0, 4, 8, 1, 2}, {12, 5, 9, 13, 2})
        targets = [(0, query | {77}), (1, query | {78})] + [(10 + j, pair[j % 2]) for j in range(4)]
        _, cost = solve(5, 5, [0, 5, 4, 1, 3, 3, 1, 0], [2, 2, 1, 0], [4, 4, 1, 0])
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            sidecar, _ = build_laminar_capacity_sidecar(reference, snapshot, groups=4, block_size=2)
            index, _ = build_selective_capacity_index(reference, snapshot, sidecar)
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, query)
            rows, metrics = select_indexed_capacity_answers(reference, snapshot, index, source, None, 2,
                                                             mode="overlap", pair_query_work_cap=cost["reserved_work"])
            self.assertEqual(rows, compute_finite_similarity_oracle(targets, query, None, 2))
            self.assertEqual((metrics["overlap_evaluations"], metrics["overlap_refusals"], metrics["body_targets"]), (1, 1, 4))
            self.assertEqual(metrics["pair_reserved_work_total"], cost["reserved_work"])

    def test_nonpair_and_failed_output(self):
        self.require_admitted_solver_function()
        reference = load_runwise_reference_module()
        targets = [(9, {0, 1}), (-3, {0}), (2, {1}), (8, {0, 1})]
        with tempfile.TemporaryDirectory() as directory:
            for width in (1, 3):
                snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / str(width), targets))
                sidecar, _ = build_laminar_capacity_sidecar(reference, snapshot, groups=2, block_size=width)
                index, _ = build_selective_capacity_index(reference, snapshot, sidecar)
                source = snapshot.root / "source"
                reference.write_normalized_source_file(source, {0, 1})
                rows, metrics = select_indexed_capacity_answers(reference, snapshot, index, source, 9, 2, mode="overlap")
                self.assertEqual(rows, compute_finite_similarity_oracle(targets, {0, 1}, 9, 2))
                self.assertEqual(metrics["overlap_evaluations"], 0)
                with self.assertRaisesRegex(OSError, "sink"):
                    select_indexed_capacity_answers(reference, snapshot, index, source, 9, 2, mode="overlap", fail_after=1)
                self.assertFalse(list(snapshot.root.glob("query-*")))

    def test_reservation_slack_exhaustion(self):
        self.require_admitted_solver_function()
        reference = load_runwise_reference_module()
        query = {0, 1}
        pair = ({0, 2, 1}, {1, 3})
        targets = [(-100, {0, 1, 9}), (-99, {0, 1, 11})]
        targets += [(j, pair[j % 2]) for j in range(6)]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = reference.Snapshot(reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            sidecar, _ = build_laminar_capacity_sidecar(reference, snapshot, groups=2, block_size=2)
            index, _ = build_selective_capacity_index(reference, snapshot, sidecar)
            source = snapshot.root / "source"
            reference.write_normalized_source_file(source, query)
            rows, metrics = select_indexed_capacity_answers(reference, snapshot, index, source, None, 2,
                                                             mode="overlap_dense", pair_query_work_cap=76)
            self.assertEqual(rows, compute_finite_similarity_oracle(targets, query, None, 2))
            self.assertEqual((metrics["overlap_evaluations"], metrics["overlap_refusals"]), (1, 2))
            self.assertEqual(metrics["pair_reserved_work_total"], 40)
            self.assertEqual(metrics["pair_actual_work_total"], 36)

    def test_overlap_benchmark_receipt(self):
        from bench_selective_capacity_similarity import run_selective_public_comparison
        self.assertIn("include_overlap", inspect.signature(run_selective_public_comparison).parameters)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / "fixture.gz"
            with gzip.open(data, "wt", encoding="ascii") as stream:
                stream.write("1 2\n2 3\n3 4\n4 1\n")
            result = run_selective_public_comparison(data, root / "report.json", sample_count=0, repeats=1,
                                                      include_overlap=True, merge_cap=4, pair_state_cap=16)
            self.assertEqual(result["complete_results_checked"], 9 * len(result["selected_source_ids"]))
            self.assertEqual(result["query_admission"]["merge_cap"], 4)
            self.assertEqual(result["query_admission"]["pair_state_cap"], 16)
            self.assertEqual(result["physical_pair_profile"]["full_pairs"], 2)


if __name__ == "__main__":
    unittest.main()
