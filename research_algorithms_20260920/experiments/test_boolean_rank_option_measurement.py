"""Option-study receipts and full-output checks, not timing evidence."""

import importlib
import math
from pathlib import Path
import tempfile
import unittest

from measure_boolean_rank_workflow import prepare_boolean_measurement_source


class BooleanOptionMeasurementTests(unittest.TestCase):
    def load_option_measurement_module(self):
        try:
            return importlib.import_module("measure_boolean_rank_options")
        except ModuleNotFoundError as error:
            if error.name == "measure_boolean_rank_options":
                self.fail("explicit option comparison driver is absent")
            raise

    def test_complete_option_worker_receipts(self):
        module = self.load_option_measurement_module()
        case = dict(name="tiny", kind="pairs", active_factors=4, factors=4,
                    height=2, isolates=0, alpha=0.85, epsilon=1e-10)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prepare_boolean_measurement_source(root, case)
            for index, mode in enumerate(module.MODES):
                result = module.run_boolean_option_query(root, root, case, mode, index)
                self.assertTrue(result["accepted"])
                self.assertEqual(result["source_events"]["active_cursors"], 0)
                self.assertEqual((root / result["output"]).stat().st_size, 192)
                self.assertEqual(result["postprocess_output_sha256"], result["pipeline"]["output_sha256"])

    def test_refusal_receipt_retains_certificate(self):
        module = self.load_option_measurement_module()
        case = dict(name="near", kind="pairs", active_factors=8, factors=8,
                    height=2, isolates=0, alpha=math.nextafter(1.0, 0.0), epsilon=1e-10)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prepare_boolean_measurement_source(root, case)
            result = module.run_boolean_option_query(root, root, case, "factor-generic", 0)
            self.assertFalse(result["accepted"])
            self.assertFalse(result["pipeline"]["certificate"]["accepted"])
            self.assertEqual(result["pipeline"]["output_rows"], 56)
            self.assertEqual(result["source_events"]["active_cursors"], 0)
            self.assertFalse((root / result["output"]).exists())

    def test_bracket_keeps_unstable_observations(self):
        module = self.load_option_measurement_module()
        records = [dict(query_seconds=t, accepted=True) for t in (1.0, 0.5, 2.0)]
        summary = module.summarize_option_control_bracket(*records)
        self.assertFalse(summary["timing_stable"])
        self.assertEqual(summary["candidate_to_control_ratio"], 1 / 3)
        records[1]["accepted"] = False
        summary = module.summarize_option_control_bracket(*records)
        self.assertFalse(summary["all_accepted"])
        self.assertNotIn("candidate_to_control_ratio", summary)


if __name__ == "__main__":
    unittest.main()
