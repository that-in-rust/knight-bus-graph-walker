"""Driver correctness only; timings from these tests are not evidence."""

from fractions import Fraction
import importlib
from pathlib import Path
import struct
import tempfile
import unittest

from test_stream_boolean_rank_solver import compute_expanded_rational_pagerank


class BooleanRankMeasurementTests(unittest.TestCase):
    def load_boolean_measurement_module(self):
        try:
            return importlib.import_module("measure_boolean_rank_workflow")
        except ModuleNotFoundError:
            self.fail("Boolean complete-workflow driver is absent")

    def test_complete_worker_results(self):
        module = self.load_boolean_measurement_module()
        case = dict(name="tiny", kind="pairs", active_factors=3, factors=5, height=2,
                    isolates=1, alpha=0.85, epsilon=1e-10)
        expected = compute_expanded_rational_pagerank(list(module.iterate_boolean_fixture_rows(case)), case["alpha"])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            preparation = module.prepare_boolean_measurement_source(root, case)
            self.assertEqual(preparation["vertices"], len(expected))
            results = [module.run_boolean_measurement_query(root, case, method, 0)
                       for method in ("class-cg", "factor-cg")]
            for result in results:
                self.assertTrue(result["accepted"])
                self.assertEqual(result["source_events"]["active_cursors"], 0)
                actual = dict(struct.iter_unpack("<Qd", (root / result["output"]).read_bytes()))
                error = sum(abs(Fraction(value) - expected[v]) for v, value in actual.items())
                self.assertLessEqual(error, Fraction(result["pipeline"]["certificate"]["l1_error_upper"]))
            audit = module.audit_boolean_output_pair(root / results[0]["output"], root / results[1]["output"], 2e-10)
            self.assertEqual(audit["rows"], len(expected))

    def test_refusal_is_preserved(self):
        module = self.load_boolean_measurement_module()
        case = dict(name="underflow", kind="underflow", active_factors=1, factors=1,
                    height=1, isolates=1, alpha=0.85, epsilon=1e-10)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            module.prepare_boolean_measurement_source(root, case)
            for method in ("factor-cg", "class-cg"):
                result = module.run_boolean_measurement_query(root, case, method, 0)
                self.assertFalse(result["accepted"])
                self.assertEqual(result["failure_type"], "ArithmeticError")
                self.assertEqual(result["source_events"]["active_cursors"], 0)
                self.assertFalse((root / result["output"]).exists())

    def test_pair_audit_rejects(self):
        module = self.load_boolean_measurement_module()
        with tempfile.TemporaryDirectory() as directory:
            left, right = Path(directory) / "left", Path(directory) / "right"
            left.write_bytes(struct.pack("<Qd", 0, 0.5))
            for raw in (struct.pack("<Qd", 1, 0.5), struct.pack("<Qd", 0, 0.6), b"bad"):
                right.write_bytes(raw)
                with self.assertRaises(AssertionError):
                    module.audit_boolean_output_pair(left, right, 1e-10)


if __name__ == "__main__":
    unittest.main()
