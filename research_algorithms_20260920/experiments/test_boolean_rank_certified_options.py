"""Explicit source-restricted solver/certificate publication contracts."""

from fractions import Fraction
import importlib
from itertools import combinations
import math
import os
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import patch
import weakref

from test_stream_boolean_rank_solver import (
    ResidentBooleanSourceFixture, compute_expanded_rational_pagerank,
)


class BooleanRankOptionsTests(unittest.TestCase):
    def load_certified_options_module(self):
        try:
            return importlib.import_module("boolean_rank_certified_options")
        except ModuleNotFoundError as error:
            if error.name == "boolean_rank_certified_options":
                self.fail("explicit certified solver/certificate options are absent")
            raise

    def build_uniform_source_fixture(self, factors=4, height=2):
        rows = [(100 + i, pair, float((i * 7 + 3) % 11))
                for i, pair in enumerate(pair for pair in combinations(range(factors), 2)
                                         for _ in range(height))]
        return rows, ResidentBooleanSourceFixture(rows, factors)

    def publish_selected_test_options(self, module, source, output, **changes):
        options = dict(alpha=0.85, epsilon=1e-10, solver_method="factor-cg",
                       certificate_method="deflated", max_factor_slots=source.factor_count,
                       max_class_slots=source.active_class_count, precision=60)
        options.update(changes)
        return module.publish_uniform_rank_result(source, output, **options)

    def test_all_solver_certificate_combinations(self):
        module = self.load_certified_options_module()
        with tempfile.TemporaryDirectory() as directory:
            for alpha in (0.0, 0.85):
                rows, _ = self.build_uniform_source_fixture()
                truth = compute_expanded_rational_pagerank(rows, alpha)
                for solver in ("factor-cg", "class-cg", "uniform-pair-direct"):
                    for certificate in ("generic", "deflated", "exact-target"):
                        with self.subTest(alpha=alpha, solver=solver, certificate=certificate):
                            _, source = self.build_uniform_source_fixture()
                            path = Path(directory) / f"{alpha}-{solver}-{certificate}.ranks"
                            receipt = self.publish_selected_test_options(module, source, path,
                                alpha=alpha, solver_method=solver, certificate_method=certificate)
                            actual = dict(struct.iter_unpack("<Qd", path.read_bytes()))
                            error = sum(abs(Fraction(actual[v]) - value) for v, value in truth.items())
                            self.assertEqual(set(actual), set(truth))
                            self.assertLessEqual(error, Fraction(receipt["certificate"]["l1_error_upper"]))
                            self.assertEqual(receipt["certificate"]["output_sha256"], receipt["output_sha256"])
                            self.assertEqual(receipt["certificate"]["snapshot_id"], receipt["snapshot_id"])
                            self.assertTrue(receipt["accepted"])
                            self.assertEqual(receipt["output_bytes"], 16 * len(rows))
                            self.assertEqual(receipt["solver_method"], solver)
                            self.assertEqual(receipt["certificate_method"], certificate)
                            if certificate == "exact-target":
                                self.assertEqual(receipt["certificate"]["scratch_bytes"], 0)
            self.assertEqual(len(list(Path(directory).iterdir())), 18)

    def test_near_one_admission_options(self):
        module = self.load_certified_options_module()
        with tempfile.TemporaryDirectory() as directory:
            for solver in ("factor-cg", "class-cg", "uniform-pair-direct"):
                for certificate in ("generic", "deflated", "exact-target"):
                    _, source = self.build_uniform_source_fixture(factors=8)
                    path = Path(directory) / f"{solver}-{certificate}.ranks"
                    options = dict(alpha=math.nextafter(1.0, 0.0), solver_method=solver,
                                   certificate_method=certificate)
                    if certificate == "generic":
                        with self.assertRaises(module.UniformRankCertificateRefusal) as caught:
                            self.publish_selected_test_options(module, source, path, **options)
                        self.assertFalse(caught.exception.receipt["accepted"])
                        self.assertGreater(Fraction(caught.exception.receipt["certificate"]["l1_error_upper"]),
                                           Fraction(1e-10))
                        self.assertFalse(path.exists())
                    else:
                        receipt = self.publish_selected_test_options(module, source, path, **options)
                        self.assertTrue(receipt["accepted"])
            self.assertEqual(len(list(Path(directory).iterdir())), 6)

    def test_existing_destination_preserved(self):
        module = self.load_certified_options_module()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "existing.ranks"
            path.write_bytes(b"keep this")
            _, source = self.build_uniform_source_fixture()
            with self.assertRaises(FileExistsError):
                self.publish_selected_test_options(module, source, path)
            self.assertEqual(path.read_bytes(), b"keep this")
            self.assertEqual(source.calls, 0)

    def test_invalid_options_precede_scans(self):
        module = self.load_certified_options_module()
        changes = ({"solver_method": "unknown"}, {"certificate_method": "unknown"},
                   {"alpha": 1.0}, {"epsilon": float("nan")}, {"epsilon": 0.0},
                   {"max_factor_slots": -1}, {"max_class_slots": True}, {"precision": 1})
        with tempfile.TemporaryDirectory() as directory:
            for change in changes:
                with self.subTest(change=change):
                    _, source = self.build_uniform_source_fixture()
                    with self.assertRaises(ValueError):
                        self.publish_selected_test_options(module, source, Path(directory) / "out", **change)
                    self.assertEqual(source.calls, 0)
                    self.assertEqual(list(Path(directory).iterdir()), [])

    def test_ineligible_source_never_publishes(self):
        module = self.load_certified_options_module()
        provider = importlib.import_module("boolean_rank_sqlite_source")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            provider.build_boolean_rank_source(root / "source.sqlite", [(0, (0,), 1.0)], factor_count=4)
            with provider.SqliteBooleanRankSource(root / "source.sqlite") as source:
                with self.assertRaises(ValueError):
                    self.publish_selected_test_options(module, source, root / "out")
                self.assertEqual(source.events["active_cursors"], 0)
                self.assertEqual(source.vertex_count, 1)
            self.assertEqual([p.name for p in root.iterdir()], ["source.sqlite"])

    def test_tight_tolerance_cleans_candidate(self):
        module = self.load_certified_options_module()
        with tempfile.TemporaryDirectory() as directory:
            _, source = self.build_uniform_source_fixture()
            with self.assertRaises(module.UniformRankCertificateRefusal):
                self.publish_selected_test_options(module, source, Path(directory) / "out", epsilon=1e-30)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_mismatched_certificate_never_publishes(self):
        module = self.load_certified_options_module()
        original = module.certify_boolean_deflated_output
        with tempfile.TemporaryDirectory() as directory:
            for key, wrong in (("output_sha256", "wrong"), ("snapshot_id", "wrong"), ("nrows", 0)):
                _, source = self.build_uniform_source_fixture()
                def replace_selected_receipt_field(*args, **kwargs):
                    receipt = original(*args, **kwargs)
                    receipt[key] = wrong
                    return receipt
                with patch.object(module, "certify_boolean_deflated_output", replace_selected_receipt_field):
                    with self.assertRaisesRegex(ValueError, "staged output"):
                        self.publish_selected_test_options(module, source, Path(directory) / "out")
                self.assertEqual(list(Path(directory).iterdir()), [])

    def test_solver_arrays_released_before_certificate(self):
        module = self.load_certified_options_module()
        original_solver = module.solve_boolean_rank_state
        original_certificate = module.certify_boolean_deflated_output
        references = []
        def capture_solver_array_references(*args, **kwargs):
            state = original_solver(*args, **kwargs)
            references.extend(weakref.ref(state[name]) for name in ("factor_scores", "class_scores")
                              if state[name] is not None)
            return state
        def check_released_array_references(*args, **kwargs):
            self.assertTrue(references)
            self.assertTrue(all(reference() is None for reference in references))
            return original_certificate(*args, **kwargs)
        with tempfile.TemporaryDirectory() as directory:
            _, source = self.build_uniform_source_fixture()
            with patch.object(module, "solve_boolean_rank_state", capture_solver_array_references), \
                 patch.object(module, "certify_boolean_deflated_output", check_released_array_references):
                self.publish_selected_test_options(module, source, Path(directory) / "out", solver_method="class-cg")

    def test_publication_race_preserves_destination(self):
        module = self.load_certified_options_module()
        original = os.link
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "out"
            _, source = self.build_uniform_source_fixture()
            def create_competing_destination_first(candidate, destination):
                path.write_bytes(b"another writer")
                original(candidate, destination)
            with patch.object(module.os, "link", create_competing_destination_first):
                with self.assertRaises(FileExistsError):
                    self.publish_selected_test_options(module, source, path)
            self.assertEqual(path.read_bytes(), b"another writer")
            self.assertEqual(list(Path(directory).iterdir()), [path])

    def test_filebacked_options_release_cursors(self):
        module = self.load_certified_options_module()
        provider = importlib.import_module("boolean_rank_sqlite_source")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows, _ = self.build_uniform_source_fixture()
            provider.build_boolean_rank_source(root / "source.sqlite", rows, factor_count=4)
            with provider.SqliteBooleanRankSource(root / "source.sqlite") as source:
                for solver in ("factor-cg", "class-cg", "uniform-pair-direct"):
                    for certificate in ("generic", "deflated", "exact-target"):
                        path = root / f"{solver}-{certificate}"
                        self.publish_selected_test_options(module, source, path,
                            solver_method=solver, certificate_method=certificate)
                        self.assertEqual(source.events["active_cursors"], 0)
                original = module.certify_boolean_deflated_output
                def fail_after_actual_certificate(*args, **kwargs):
                    original(*args, **kwargs)
                    raise OSError("injected certificate failure")
                with patch.object(module, "certify_boolean_deflated_output", fail_after_actual_certificate):
                    with self.assertRaisesRegex(OSError, "injected"):
                        self.publish_selected_test_options(module, source, root / "failed")
                self.assertEqual(source.events["active_cursors"], 0)
                self.assertFalse((root / "failed").exists())
            self.assertEqual(len(list(root.iterdir())), 10)


if __name__ == "__main__":
    unittest.main()
