"""Publication admission and file-backed original-answer integration."""

from fractions import Fraction
import importlib
from pathlib import Path
import random
import struct
import tempfile
import unittest

from test_stream_boolean_rank_solver import ResidentBooleanSourceFixture, compute_expanded_rational_pagerank


class BooleanRankPipelineTests(unittest.TestCase):
    def load_rank_pipeline_module(self):
        try:
            return importlib.import_module("boolean_rank_pipeline")
        except ModuleNotFoundError as error:
            if error.name == "boolean_rank_pipeline":
                self.fail("certified Boolean publication pipeline is absent")
            raise

    def test_successful_certified_publication(self):
        module = self.load_rank_pipeline_module()
        rows = [(0, (0, 1), 9.0), (1, (0, 1), 1.0), (2, (1,), 2.0), (3, (), 0.25)]
        expected = compute_expanded_rational_pagerank(rows, 0.85)
        with tempfile.TemporaryDirectory() as directory:
            for method in ("factor-cg", "class-cg"):
                source = ResidentBooleanSourceFixture(rows, 2)
                output = Path(directory) / f"{method}.ranks"
                receipt = module.publish_boolean_rank_result(source, output, alpha=0.85, epsilon=1e-10,
                    method=method, max_factor_slots=2, max_class_slots=3)
                actual = dict(struct.iter_unpack("<Qd", output.read_bytes()))
                error = sum(abs(Fraction.from_float(actual[v]) - exact) for v, exact in expected.items())
                self.assertTrue(receipt["certificate"]["accepted"])
                self.assertLessEqual(error, Fraction(receipt["certificate"]["l1_error_upper"]))
                self.assertEqual(receipt["output_bytes"], 16 * len(rows))
                self.assertEqual(receipt["output_sha256"], receipt["certificate"]["output_sha256"])
            self.assertEqual(len(list(Path(directory).iterdir())), 2)

    def test_refusal_leaves_no_output(self):
        module = self.load_rank_pipeline_module()
        rows = [(0, (0, 1), 9.0), (1, (0, 1), 1.0)]
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "refused.ranks"
            source = ResidentBooleanSourceFixture(rows, 2)
            with self.assertRaisesRegex(ValueError, "certificate"):
                module.publish_boolean_rank_result(source, output, alpha=0.85, epsilon=1e-30,
                    method="factor-cg", max_factor_slots=2, max_class_slots=0)
            self.assertEqual(list(Path(directory).iterdir()), [])
            with self.assertRaisesRegex(ValueError, "negative"):
                module.publish_boolean_rank_result(source, output, alpha=0.85, epsilon=1e-10,
                    method="factor-cg", max_factor_slots=2, max_class_slots=0, maximum_iterations=0)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_existing_destination_preserved(self):
        module = self.load_rank_pipeline_module()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "existing.ranks"
            output.write_bytes(b"existing user content")
            source = ResidentBooleanSourceFixture([(0, (), 1.0)], 0)
            with self.assertRaises(FileExistsError):
                module.publish_boolean_rank_result(source, output, alpha=0.85, epsilon=1e-10,
                    method="factor-cg", max_factor_slots=0, max_class_slots=0)
            self.assertEqual(output.read_bytes(), b"existing user content")
            self.assertEqual(source.calls, 0)

    def test_empty_and_isolated_publication(self):
        module = self.load_rank_pipeline_module()
        with tempfile.TemporaryDirectory() as directory:
            for case, rows in enumerate(([], [(0, (), 1.0)], [(0, (), 3.0), (1, (0,), 0.0), (2, (0,), 0.0)])):
                source = ResidentBooleanSourceFixture(rows, 2)
                receipt = module.publish_boolean_rank_result(source, Path(directory) / str(case),
                    alpha=0.85, epsilon=1e-10, method="factor-cg", max_factor_slots=2, max_class_slots=0)
                self.assertTrue(receipt["certificate"]["accepted"])
                self.assertEqual(receipt["output_bytes"], 16 * len(rows))

    def test_seeded_filebacked_certified_outputs(self):
        module = self.load_rank_pipeline_module()
        provider = importlib.import_module("boolean_rank_sqlite_source")
        rng = random.Random(20260925)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for case in range(24):
                factors, count = rng.randrange(1, 7), rng.randrange(1, 8)
                rows = [(v, tuple(rng.sample(range(factors), rng.randrange(min(2, factors) + 1))),
                         rng.choice((0.0, 0.125, 3.0))) for v in rng.sample(range(1000), count)]
                rows[0] = (rows[0][0], rows[0][1], 1.0)
                path = root / f"{case}.sqlite"
                provider.build_boolean_rank_source(path, iter(rows), factor_count=factors, cache_kib=32)
                expected = compute_expanded_rational_pagerank(rows, 0.85)
                for method in ("factor-cg", "class-cg"):
                    with provider.SqliteBooleanRankSource(path, cache_kib=32) as source:
                        output = root / f"{case}-{method}.ranks"
                        receipt = module.publish_boolean_rank_result(source, output, alpha=0.85, epsilon=1e-10,
                            method=method, max_factor_slots=factors, max_class_slots=source.active_class_count)
                        actual = dict(struct.iter_unpack("<Qd", output.read_bytes()))
                        error = sum(abs(Fraction.from_float(actual[v]) - exact) for v, exact in expected.items())
                        self.assertEqual(set(actual), set(expected))
                        self.assertLessEqual(error, Fraction(receipt["certificate"]["l1_error_upper"]))
                        self.assertTrue(receipt["certificate"]["accepted"])


if __name__ == "__main__":
    unittest.main()
