"""End-to-end fixtures for a certificate-stage, not solver-stage comparison."""

import importlib.util
import json
from pathlib import Path
import struct
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import probe_native_incidence_pagerank as base

MODULE = Path(__file__).with_name("benchmark_rank_publication_comparison.py")


class PublicationComparisonTests(unittest.TestCase):
    def load_required_comparison_module(self):
        self.assertTrue(MODULE.exists(), "publication comparison is not implemented")
        spec = importlib.util.spec_from_file_location("publication_comparison", MODULE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_both_paths_publish_certified(self):
        module = self.load_required_comparison_module()
        with tempfile.TemporaryDirectory() as folder:
            folder = Path(folder)
            store = folder/"rows"
            base.build_incidence_row_store(store, 7, [[0, 1, 2], [1, 3], [3, 4], [0, 4]])
            preparation = module.prepare_shared_factor_states(store, folder)
            self.assertEqual(len(preparation["states"]), 3)
            all_hashes = {}
            for method in ("direct", "direct_same", "stream", "stream_rump"):
                target = folder/method
                target.mkdir()
                result = module.run_publication_stage_session(store, folder, target, method)
                self.assertEqual(len(result["queries"]), 3)
                for j, query in enumerate(result["queries"]):
                    self.assertTrue(query["accepted"])
                    self.assertTrue(query["audit"]["accepted"])
                    self.assertEqual(query["output_bytes"], 56)
                    self.assertEqual(len(struct.unpack("<7d", (target/f"rank-{j}.bin").read_bytes())), 7)
                self.assertGreater(result["total_stage_ms"], 0)
                self.assertGreater(result["peak_rss_before_audit_bytes"], 0)
                all_hashes[method] = [q["output_sha256"] for q in result["queries"]]
            self.assertEqual(all_hashes["direct_same"], all_hashes["stream"])
            self.assertEqual(all_hashes["stream_rump"], all_hashes["stream"])

    def test_stage_rejects_state_tampering(self):
        module = self.load_required_comparison_module()
        with tempfile.TemporaryDirectory() as folder:
            folder = Path(folder)
            store = folder/"rows"
            base.build_incidence_row_store(store, 3, [[0, 1], [1, 2]])
            module.prepare_shared_factor_states(store, folder)
            path = folder/"factor-0.bin"
            path.write_bytes(path.read_bytes()[:-1])
            with self.assertRaisesRegex(ValueError, "state"):
                module.run_publication_stage_session(store, folder, folder, "direct")

    def test_direct_rejects_input_aliases(self):
        module = self.load_required_comparison_module()
        from array import array
        with tempfile.TemporaryDirectory() as folder:
            folder = Path(folder)
            store, metadata, alias = folder/"rows", folder/"meta", folder/"alias"
            base.build_incidence_row_store(store, 2, [[0, 1]])
            manifest = module.streaming.build_certificate_source_manifest(store, metadata)
            originals = (store.read_bytes(), metadata.read_bytes())
            alias.symlink_to(store)
            for same in (False, True):
                for output in (store, metadata, alias):
                    with self.assertRaisesRegex(ValueError, "replace"):
                        module.publish_direct_rank_certificate(
                            store, metadata, manifest, array("d", [1]), output, None, 1e-10, same)
                    self.assertEqual((store.read_bytes(), metadata.read_bytes()), originals)

    def test_malformed_workloads_are_refused(self):
        module = self.load_required_comparison_module()
        with tempfile.TemporaryDirectory() as folder:
            folder = Path(folder)
            store = folder/"rows"
            base.build_incidence_row_store(store, 3, [[0, 1], [1, 2]])
            preparation = module.prepare_shared_factor_states(store, folder)
            states = preparation["states"]
            for invalid in ([], states[:2], list(reversed(states))):
                preparation["states"] = invalid
                (folder/"preparation.json").write_text(json.dumps(preparation))
                with self.assertRaisesRegex(ValueError, "workload"):
                    module.run_publication_stage_session(store, folder, folder, "direct")

    def test_failed_workers_remain_recorded(self):
        module = self.load_required_comparison_module()
        with tempfile.TemporaryDirectory() as folder:
            folder = Path(folder)
            store, receipt = folder/"rows", folder/"receipt.json"
            base.build_incidence_row_store(store, 2, [[0, 1]])
            preparation = module.prepare_shared_factor_states(store, folder)
            counter = 0

            def collect_synthetic_worker_outcome(*args):
                nonlocal counter
                counter += 1
                if counter == 4:
                    raise subprocess.CalledProcessError(1, ["synthetic"], stderr="intentional refusal")
                return {"total_session_ms": 1.0, "all_accepted": True}

            argv = ["comparison", "--store", str(store), "--folder", str(folder),
                    "--receipt", str(receipt), "--repeats", "1"]
            with patch.object(module.sys, "argv", argv), \
                    patch.object(module, "prepare_shared_factor_states", return_value=preparation), \
                    patch.object(module, "execute_publication_worker_session", side_effect=collect_synthetic_worker_outcome):
                module.main_publication_comparison_experiment()
            result = json.loads(receipt.read_text())
            self.assertEqual(len(result["triplets"]), 3)
            self.assertTrue(result["triplets"][0]["all_accepted"])
            self.assertFalse(result["triplets"][1]["all_accepted"])
            self.assertIsNone(result["triplets"][1]["candidate_control_ratio"])
            self.assertIn("intentional refusal", result["triplets"][1]["before"]["error"])

    def test_direct_receipt_precedes_commit(self):
        module = self.load_required_comparison_module()
        from array import array
        with tempfile.TemporaryDirectory() as folder:
            store, metadata, output = [Path(folder)/name for name in ("rows", "metadata", "answer")]
            base.build_incidence_row_store(store, 2, [[0, 1]])
            manifest = module.streaming.build_certificate_source_manifest(store, metadata)
            output.write_bytes(b"prior")
            real_stat = Path.stat

            def reject_postcommit_source_stat(path, *args, **kwargs):
                if path == store and output.read_bytes() != b"prior":
                    raise OSError("postcommit source stat")
                return real_stat(path, *args, **kwargs)

            with patch.object(Path, "stat", reject_postcommit_source_stat):
                result = module.publish_direct_rank_certificate(
                    store, metadata, manifest, array("d", [1]), output, None, 1e-10)
            self.assertTrue(result["accepted"])


if __name__ == "__main__":
    unittest.main()
