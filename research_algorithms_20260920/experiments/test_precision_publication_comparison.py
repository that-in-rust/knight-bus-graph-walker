"""Controls must include complete output and actual numerical admission."""

from array import array
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import probe_native_incidence_pagerank as base
from test_rank_publication_comparison import PublicationComparisonTests

MODULE = Path(__file__).with_name("benchmark_precision_publication_comparison.py")


class PrecisionPublicationComparisonTests(PublicationComparisonTests):
    def load_required_comparison_module(self):
        self.assertTrue(MODULE.exists(), "precision publication comparison is not implemented")
        spec = importlib.util.spec_from_file_location("precision_comparison", MODULE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_extended_paths_publish_certified(self):
        module = self.load_required_comparison_module()
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            store = folder/"rows"
            base.build_incidence_row_store(store, 7, [[0, 1, 2], [1, 3], [3, 4], [0, 4]])
            module.prepare_shared_factor_states(store, folder)
            answers = {}
            for method in ("direct", "precision", "binary_direct", "stream_rump"):
                target = folder/method
                target.mkdir()
                result = module.run_publication_stage_session(store, folder, target, method)
                self.assertEqual(result["method"], method)
                self.assertTrue(result["all_accepted"], result)
                self.assertEqual(len(result["queries"]), 3)
                answers[method] = [q["output_sha256"] for q in result["queries"]]
                if method == "binary_direct":
                    self.assertTrue(all(q["factor_state_consumed"] for q in result["queries"]))
                    self.assertTrue(all(q["retained_decimal_vector_values"] == 0 for q in result["queries"]))
            self.assertEqual(answers["direct"], answers["binary_direct"])

    def test_binary_control_releases_factor_state(self):
        module = self.load_required_comparison_module()
        with tempfile.TemporaryDirectory() as directory:
            store, metadata, output = [Path(directory)/s for s in ("rows", "meta", "out")]
            base.build_incidence_row_store(store, 2, [[0, 1]])
            manifest = module.streaming.build_certificate_source_manifest(store, metadata)
            h = array("d", [1.0])
            actual = module.binary.certify_binary64_rank_output

            def certify_after_state_release(*args, **kwargs):
                self.assertEqual(len(h), 0, "solver state still alive at certificate allocation")
                return actual(*args, **kwargs)

            with patch.object(module.binary, "certify_binary64_rank_output", side_effect=certify_after_state_release):
                result = module.publish_binary64_control_certificate(store, metadata, manifest, h, output, None, 1e-10)
            self.assertTrue(result["accepted"])
            self.assertEqual(result["selected_factor_payload_peak_bytes"], 16)

    def test_binary_control_preserves_failed_output(self):
        module = self.load_required_comparison_module()
        with tempfile.TemporaryDirectory() as directory:
            store, metadata, output = [Path(directory)/s for s in ("rows", "meta", "out")]
            base.build_incidence_row_store(store, 2, [[0, 1]])
            manifest = module.streaming.build_certificate_source_manifest(store, metadata)
            output.write_bytes(b"old-output")
            result = module.publish_binary64_control_certificate(store, metadata, manifest, array("d", [0.0]), output, None, 1e-30)
            self.assertFalse(result["accepted"])
            self.assertEqual(output.read_bytes(), b"old-output")
            for path in (store, metadata):
                with self.assertRaisesRegex(ValueError, "replace"):
                    module.publish_binary64_control_certificate(store, metadata, manifest, array("d", [1]), path, None, 1e-10)


if __name__ == "__main__":
    unittest.main()
