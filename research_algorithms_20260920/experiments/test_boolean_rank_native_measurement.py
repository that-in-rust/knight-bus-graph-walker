"""Native comparison receipts and failure retention; not timing evidence."""

import importlib
import math
from pathlib import Path
import tempfile
import unittest


class NativeRankMeasurementTests(unittest.TestCase):
    def load_native_measurement_module(self):
        try:
            return importlib.import_module("measure_boolean_rank_native")
        except ModuleNotFoundError as error:
            if error.name == "measure_boolean_rank_native":
                self.fail("native complete-cost comparison driver is absent")
            raise

    def test_all_modes_preserve_outputs(self):
        module = self.load_native_measurement_module()
        case = dict(name="tiny", kind="cycle", factors=8, height=2, alpha=0.85, epsilon=1e-10)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prepared = module.prepare_native_measurement_source(root, case)
            self.assertEqual(prepared["classes"], 8)
            self.assertGreater(prepared["vertices"], 8)
            for index, mode in enumerate(module.MODES):
                result = module.run_native_measurement_query(root, case, mode, index)
                self.assertTrue(result["accepted"])
                self.assertEqual(result["source_events"]["active_cursors"], 0)
                self.assertEqual((root/result["output"]).stat().st_size, 16*prepared["vertices"])
                self.assertEqual(result["postprocess_output_sha256"], result["pipeline"]["output_sha256"])
                self.assertEqual(result["pipeline"]["preflight_class_rows"], 0 if mode.endswith("generic") else 16)

    def test_near_one_refusal_is_recorded(self):
        module = self.load_native_measurement_module()
        case = dict(name="near", kind="cycle", factors=8, height=2,
                    alpha=math.nextafter(1., 0.), epsilon=1e-10)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            module.prepare_native_measurement_source(root, case)
            refused = module.run_native_measurement_query(root, case, "factor-generic", 0)
            self.assertFalse(refused["accepted"])
            self.assertFalse(refused["pipeline"]["certificate"]["accepted"])
            self.assertFalse((root/refused["output"]).exists())
            admitted = module.run_native_measurement_query(root, case, "factor-native", 1)
            self.assertTrue(admitted["accepted"])

    def test_fixture_stream_shapes_and_ids(self):
        module = self.load_native_measurement_module()
        for kind in ("cycle", "path", "chorded", "star", "dense"):
            case = dict(kind=kind, factors=8, height=2)
            rows = list(module.iterate_native_fixture_rows(case))
            self.assertEqual([row[0] for row in rows], list(range(len(rows))))
            pairs = {row[1] for row in rows}
            self.assertGreaterEqual(len(pairs), 7)
            self.assertLess(len(pairs), 28)
            self.assertEqual({v for pair in pairs for v in pair}, set(range(8)))
            self.assertTrue(all(a < b for a, b in pairs))

    def test_unstable_and_refused_brackets(self):
        module = self.load_native_measurement_module()
        records = [dict(query_seconds=t, accepted=True) for t in (1., 0.5, 2.)]
        summary = module.summarize_native_control_bracket(*records)
        self.assertFalse(summary["timing_stable"])
        self.assertEqual(summary["candidate_to_control_ratio"], 1/3)
        records[1]["accepted"] = False
        summary = module.summarize_native_control_bracket(*records)
        self.assertFalse(summary["all_accepted"])
        self.assertNotIn("candidate_to_control_ratio", summary)


if __name__ == "__main__":
    unittest.main()
