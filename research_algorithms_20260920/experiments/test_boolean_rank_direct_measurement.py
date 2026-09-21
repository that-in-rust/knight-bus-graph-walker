"""Complete direct-control worker receipts, not performance assertions."""

import importlib
import math
from pathlib import Path
import tempfile
import unittest

from measure_boolean_rank_native import prepare_native_measurement_source


class DirectControlMeasurementTests(unittest.TestCase):
    def load_direct_measurement_module(self):
        try:
            return importlib.import_module("measure_boolean_rank_direct")
        except ModuleNotFoundError as error:
            if error.name == "measure_boolean_rank_direct":
                self.fail("direct-control comparison driver is absent")
            raise

    def test_staged_outputs_and_refusals(self):
        module=self.load_direct_measurement_module()
        case=dict(name="tiny",kind="star",factors=5,height=2,alpha=.85,epsilon=1e-10)
        with tempfile.TemporaryDirectory() as directory:
            source_root=Path(directory)/"inputs"
            output_root=Path(directory)/"outputs"
            source_root.mkdir()
            output_root.mkdir()
            prepare_native_measurement_source(source_root,case)
            for trial,mode in enumerate(("stationary","clique-direct","factor-generic","factor-native")):
                report=module.run_direct_measurement_query(source_root,output_root,case,mode,trial)
                self.assertEqual(report["accepted"],mode!="stationary")
                self.assertEqual((output_root/report["output"]).exists(),report["accepted"])
                self.assertEqual(report["source_events"]["active_cursors"],0)
                if report["accepted"]:
                    self.assertEqual(report["postprocess_output_sha256"],report["pipeline"]["output_sha256"])
                else:
                    self.assertFalse(report["pipeline"]["certificate"]["accepted"])

    def test_near_stationary_query_admits(self):
        module=self.load_direct_measurement_module()
        case=dict(name="tiny",kind="cycle",factors=8,height=2,
                  alpha=math.nextafter(1.,0.),epsilon=1e-10)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            prepare_native_measurement_source(root,case)
            report=module.run_direct_measurement_query(root,root,case,"stationary",0)
            self.assertTrue(report["accepted"])
            self.assertEqual(report["pipeline"]["certificate"]["scratch_bytes"],0)
            self.assertEqual(report["pipeline"]["certificate"]["row_passes"],1)


if __name__=="__main__":
    unittest.main()
