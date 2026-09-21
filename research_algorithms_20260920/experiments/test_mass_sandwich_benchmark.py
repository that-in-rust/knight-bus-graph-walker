"""Predeclared local-stationarity screen, not a proof of host isolation."""

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import probe_native_incidence_pagerank as base

MODULE = Path(__file__).with_name("benchmark_mass_pagerank_sandwich.py")


class SandwichBenchmarkTests(unittest.TestCase):
    def test_unstable_controls_are_flagged(self):
        self.assertTrue(MODULE.exists(), "paired timing driver is not implemented")
        spec = importlib.util.spec_from_file_location("paired", MODULE)
        probe = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(probe)
        stable = probe.assess_sandwich_control_pair(100, 80, 110)
        self.assertTrue(stable["locally_stable"])
        self.assertAlmostEqual(stable["candidate_control_ratio"], 80/(100*110)**.5)
        unstable = probe.assess_sandwich_control_pair(500, 400, 100)
        self.assertFalse(unstable["locally_stable"])
        self.assertIsNone(unstable["candidate_control_ratio"])
        for bad in (0, -1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                probe.assess_sandwich_control_pair(bad, 80, 100)

    def test_driver_retains_complete_triplet(self):
        self.assertTrue(MODULE.exists(), "paired timing driver is not implemented")
        with tempfile.TemporaryDirectory() as folder:
            rows, receipt = Path(folder)/"rows", Path(folder)/"receipt.json"
            base.build_incidence_row_store(rows, 3, [[0,1],[1,2]])
            subprocess.run([sys.executable, str(MODULE), "--store", str(rows), "--receipt", str(receipt),
                            "--repeats", "1", "--modes", "projected"], check=True, capture_output=True)
            result = json.loads(receipt.read_text())
            self.assertEqual(len(result["triplets"]), 1)
            triplet = result["triplets"][0]
            for role in ("before", "candidate", "after"):
                self.assertEqual(len(triplet[role]["queries"]), 3)
                self.assertGreater(triplet[role]["session_cpu_ms"], 0)
                self.assertTrue(all(q["certificate"]["accepted"] for q in triplet[role]["queries"]))


if __name__ == "__main__":
    unittest.main()
