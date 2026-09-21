"""Solve-to-published-answer tests, without remapping previously emitted scores."""

import importlib.util
import json
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import patch

import probe_native_incidence_pagerank as base

MODULE = Path(__file__).with_name("benchmark_integrated_accuracy_rank.py")


class IntegratedAccuracyComparisonTests(unittest.TestCase):
    def load_required_comparison_module(self):
        self.assertTrue(MODULE.exists(), "integrated accuracy comparison is not implemented")
        spec = importlib.util.spec_from_file_location("integrated_accuracy", MODULE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_all_methods_share_solver_state(self):
        module = self.load_required_comparison_module()
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            store = folder/"rows"
            base.build_incidence_row_store(store, 7, [[0, 1, 2], [1, 3], [3, 4], [0, 4]])
            setup = module.prepare_integrated_source_manifest(store, folder)
            self.assertGreater(setup["metadata_bytes"], 0)
            results = []
            for method in module.METHODS:
                output = folder/method
                output.mkdir()
                result = module.run_integrated_accuracy_session(store, folder, output, method, 1e-10)
                self.assertTrue(result["all_accepted"], result)
                self.assertEqual(len(result["queries"]), 3)
                self.assertEqual(result["score_to_factor_mapping_scans"], 0)
                for i, q in enumerate(result["queries"]):
                    self.assertTrue(q["audit"]["accepted"])
                    self.assertEqual(len(struct.unpack("<7d", (output/f"rank-{i}.bin").read_bytes())), 7)
                    self.assertGreaterEqual(q["total_query_ms"], q["solver"]["total_ms"])
                results.append(result)
            for result in results[1:]:
                self.assertEqual([q["state_sha256"] for q in result["queries"]],
                                 [q["state_sha256"] for q in results[0]["queries"]])

    def test_prior_score_path_is_never_called(self):
        module = self.load_required_comparison_module()
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            store = folder/"rows"
            base.build_incidence_row_store(store, 2, [[0, 1]])
            module.prepare_integrated_source_manifest(store, folder)
            with patch.object(base, "run_incidence_rank_solver", side_effect=AssertionError("old score pipeline used")):
                result = module.run_integrated_accuracy_session(store, folder, folder, "exact_midpoint", 1e-10)
            self.assertTrue(result["all_accepted"])
            self.assertFalse(any(folder.glob("factor-*.bin")))
            self.assertFalse(any(folder.glob("upstream-*.bin")))

    def test_refused_accuracy_does_not_publish(self):
        module = self.load_required_comparison_module()
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            store = folder/"rows"
            base.build_incidence_row_store(store, 2, [[0, 1], [0, 1]])
            module.prepare_integrated_source_manifest(store, folder)
            (folder/"rank-0.bin").write_bytes(b"old-answer")
            result = module.run_integrated_accuracy_session(store, folder, folder, "rump_lower", 1e-30)
            self.assertFalse(result["all_accepted"])
            self.assertFalse(result["queries"][0]["accepted"])
            self.assertEqual((folder/"rank-0.bin").read_bytes(), b"old-answer")
            self.assertIsNone(result["queries"][0]["audit"])

    def test_wrong_source_manifest_is_refused(self):
        module = self.load_required_comparison_module()
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            store = folder/"rows"
            base.build_incidence_row_store(store, 2, [[0, 1]])
            module.prepare_integrated_source_manifest(store, folder)
            base.build_incidence_row_store(store, 2, [[0, 1], [0, 1]])
            with self.assertRaisesRegex(ValueError, "identity"):
                module.run_integrated_accuracy_session(store, folder, folder, "binary_direct", 1e-10)

    def test_failed_worker_retains_prior_triplets(self):
        module = self.load_required_comparison_module()
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            store, receipt = folder/"rows", folder/"receipt.json"
            base.build_incidence_row_store(store, 2, [[0, 1]])
            call = 0

            def synthetic_worker(*args):
                nonlocal call
                call += 1
                if call == 4:
                    raise RuntimeError("intentional failed worker")
                return {"all_accepted": True, "total_session_ms": 1.0}

            argv = ["benchmark", "--store", str(store), "--folder", str(folder),
                    "--receipt", str(receipt), "--repeats", "1"]
            with patch.object(module.sys, "argv", argv), \
                    patch.object(module, "execute_integrated_worker_session", side_effect=synthetic_worker):
                module.main_integrated_accuracy_experiment()
            result = json.loads(receipt.read_text())
            self.assertEqual(len(result["triplets"]), 4)
            self.assertTrue(result["triplets"][0]["all_accepted"])
            self.assertFalse(result["triplets"][1]["all_accepted"])
            self.assertIsNone(result["triplets"][1]["candidate_control_ratio"])
            self.assertIn("intentional failed worker", result["triplets"][1]["before"]["error"])


if __name__ == "__main__":
    unittest.main()
