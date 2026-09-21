"""Original-byte mass-deflation admission, not reduced-state convergence."""

from decimal import Context, Decimal, Inexact, ROUND_CEILING, localcontext
from fractions import Fraction
import importlib
from itertools import combinations
import math
import os
from pathlib import Path
import struct
import tempfile
import unittest

from test_stream_boolean_rank_solver import ResidentBooleanSourceFixture, compute_expanded_rational_pagerank


class BooleanRankDeflatedCertificateTests(unittest.TestCase):
    def load_deflated_certificate_module(self):
        try:
            return importlib.import_module("boolean_rank_deflated_certificate")
        except ModuleNotFoundError:
            self.fail("directed Boolean mass-deflation certificate is absent")

    def build_personalized_pair_rows(self, factors=4, height=1):
        return [(i, pair, float(i % 5 + 1)) for i, pair in enumerate(
            pair for pair in combinations(range(factors), 2) for _ in range(height))]

    def write_aligned_score_file(self, path, source, values):
        with path.open("wb") as output:
            for class_id, _, _, _, _ in source.iterate_class_records():
                for vertex, _ in source.iterate_class_vertex_rows(class_id):
                    output.write(struct.pack("<Qd", vertex, values[vertex]))

    def test_exact_original_answer_enclosure(self):
        module = self.load_deflated_certificate_module()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "scores"
            for factors, height in ((4, 1), (4, 2), (5, 1)):
                rows = self.build_personalized_pair_rows(factors, height)
                for alpha in (0.0, 0.85, math.nextafter(1.0, 0.0)):
                    source = ResidentBooleanSourceFixture(rows, factors)
                    truth = compute_expanded_rational_pagerank(rows, alpha)
                    values = {v: float(x) for v, x in truth.items()}
                    self.write_aligned_score_file(path, source, values)
                    report = module.certify_boolean_deflated_output(source, path, alpha=alpha,
                        epsilon=1e-10, max_factor_slots=factors, scratch_dir=directory)
                    error = sum(abs(Fraction(values[v]) - truth[v]) for v in values)
                    self.assertLessEqual(error, Fraction(report["l1_error_upper"]))
                    self.assertTrue(report["accepted"])
                    self.assertEqual(report["retained_decimal_values"], 2 * factors)
                    self.assertEqual(report["scratch_records"], source.class_count)
                    self.assertEqual(report["nrows"], len(rows))
                    self.assertEqual(report["row_passes"], 2)

    def test_mass_error_cannot_disappear(self):
        module = self.load_deflated_certificate_module()
        rows = [(v, groups, 1.) for v, groups, _ in self.build_personalized_pair_rows()]
        source = ResidentBooleanSourceFixture(rows, 4)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "half"
            self.write_aligned_score_file(path, source, {v: 0.5 / len(rows) for v, _, _ in rows})
            report = module.certify_boolean_deflated_output(source, path,
                alpha=math.nextafter(1., 0.), epsilon=0.01, max_factor_slots=4)
            self.assertFalse(report["accepted"])
            self.assertGreaterEqual(Fraction(report["l1_error_upper"]), Fraction(1, 2))
            self.assertGreaterEqual(Fraction(report["mass_error_upper"]), Fraction(1, 2))

    def test_perturbed_values_and_precision(self):
        module = self.load_deflated_certificate_module()
        rows = self.build_personalized_pair_rows()
        source = ResidentBooleanSourceFixture(rows, 4)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "perturbed"
            for alpha in (0.0, 0.85, math.nextafter(1., 0.)):
                truth = compute_expanded_rational_pagerank(rows, alpha)
                for exponent in (-20, -5, 1):
                    values = {v: float(x) for v, x in truth.items()}
                    values[0] += 2.**exponent
                    self.write_aligned_score_file(path, source, values)
                    error = sum(abs(Fraction(values[v]) - truth[v]) for v in values)
                    a = Fraction(alpha)
                    total = sum(Fraction(row[2]) for row in rows)
                    residuals = []
                    for vertex, groups, weight in rows:
                        incoming = Fraction()
                        for other, other_groups, _ in rows:
                            degree = sum(v != other and bool(set(g) & set(other_groups)) for v, g, _ in rows)
                            if vertex != other and set(groups) & set(other_groups):
                                incoming += Fraction(values[other]) / degree
                        residuals.append((1-a)*Fraction(weight)/total + a*incoming - Fraction(values[vertex]))
                    variance = len(rows)*sum(r*r for r in residuals) - sum(residuals)**2
                    for precision in (2, 7, 60):
                        report = module.certify_boolean_deflated_output(source, path, alpha=alpha,
                            epsilon=1e-10, precision=precision, max_factor_slots=4)
                        self.assertLessEqual(error, Fraction(report["l1_error_upper"]))
                        self.assertGreaterEqual(Fraction(report["variance_upper"]), variance)
                        self.assertGreaterEqual(Fraction(report["mass_error_upper"]),
                                                abs(sum(Fraction(x) for x in values.values()) - 1))
                        self.assertFalse(report["accepted"])

    def test_zero_crossing_absolute_interval(self):
        module = self.load_deflated_certificate_module()
        self.assertEqual(module.calculate_interval_absolute_bounds(Decimal("-0.1"), Decimal("0.1")),
                         (Decimal(0), Decimal("0.1")))
        self.assertEqual(module.calculate_interval_absolute_bounds(Decimal("-0.3"), Decimal("-0.1")),
                         (Decimal("0.1"), Decimal("0.3")))
        with self.assertRaises(ValueError):
            module.calculate_interval_absolute_bounds(Decimal(1), Decimal(0))

    def test_extreme_personalization_values(self):
        module = self.load_deflated_certificate_module()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "extreme"
            for weights in ((5e-324,) * 6, (float.fromhex("0x1.fffffffffffffp+1023"),) * 6,
                            (5e-324, 1., 0., 7., 0.125, 0.)):
                rows = [(v, groups, weights[v]) for v, groups, _ in self.build_personalized_pair_rows()]
                source = ResidentBooleanSourceFixture(rows, 4)
                for alpha in (5e-324, 0.85, math.nextafter(1., 0.)):
                    truth = compute_expanded_rational_pagerank(rows, alpha)
                    values = {v: float(x) for v, x in truth.items()}
                    self.write_aligned_score_file(path, source, values)
                    report = module.certify_boolean_deflated_output(source, path, alpha=alpha,
                        epsilon=1e-10, max_factor_slots=4)
                    self.assertLessEqual(sum(abs(Fraction(values[v]) - truth[v]) for v in values),
                                         Fraction(report["l1_error_upper"]))
                    self.assertTrue(report["accepted"])

    def test_directional_root_is_upper(self):
        module = self.load_deflated_certificate_module()
        for precision in (1, 2, 7, 60):
            upper = Context(prec=precision, rounding=ROUND_CEILING)
            for spelling in ("0", "1", "2", "3", "0.001", "1e-600", "1e600", "9.999999"):
                value = Decimal(spelling)
                root = module.calculate_directed_root_upper(value, upper)
                self.assertGreaterEqual(Fraction(root)**2, Fraction(value))
                if value == 0:
                    self.assertEqual(root, 0)

    def test_rejected_source_and_limits(self):
        module = self.load_deflated_certificate_module()
        source = ResidentBooleanSourceFixture(self.build_personalized_pair_rows(), 4)
        with self.assertRaises(ValueError):
            module.certify_boolean_deflated_output(source, "absent", alpha=0.85,
                epsilon=1e-10, max_factor_slots=3)
        self.assertEqual(source.calls, 0)
        source.classes[0] = (0, (0,), 1, 4, Fraction(1))
        with self.assertRaises(ValueError):
            module.certify_boolean_deflated_output(source, "absent", alpha=0.85,
                epsilon=1e-10, max_factor_slots=4)

    def test_failure_keeps_source_reusable(self):
        module = self.load_deflated_certificate_module()
        from boolean_rank_sqlite_source import build_boolean_rank_source, SqliteBooleanRankSource
        rows = self.build_personalized_pair_rows(4, 2)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            build_boolean_rank_source(root / "source.sqlite", rows, factor_count=4)
            with SqliteBooleanRankSource(root / "source.sqlite") as source:
                self.write_aligned_score_file(root / "scores", source, {v: 1. / len(rows) for v, _, _ in rows})
                raw = (root / "scores").read_bytes()
                for broken in (b"short", struct.pack("<Qd", 999, 0.1) + raw[16:],
                               struct.pack("<Qd", 0, float("nan")) + raw[16:]):
                    (root / "scores").write_bytes(broken)
                    for _ in range(10):
                        with self.assertRaises(ValueError):
                            module.certify_boolean_deflated_output(source, root / "scores", alpha=0.85,
                                epsilon=1e-10, max_factor_slots=4, scratch_dir=root)
                        self.assertEqual(source.events["active_cursors"], 0)
                    self.assertEqual(len(list(root.iterdir())), 2)

    def test_independent_decimal_context(self):
        module = self.load_deflated_certificate_module()
        rows = self.build_personalized_pair_rows()
        source = ResidentBooleanSourceFixture(rows, 4)
        truth = compute_expanded_rational_pagerank(rows, 0.85)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "scores"
            self.write_aligned_score_file(path, source, {v: float(x) for v, x in truth.items()})
            baseline = module.certify_boolean_deflated_output(source, path, alpha=0.85, epsilon=1e-10, max_factor_slots=4)
            with localcontext() as context:
                context.prec = 1
                context.traps[Inexact] = True
                result = module.certify_boolean_deflated_output(source, path, alpha=0.85, epsilon=1e-10, max_factor_slots=4)
            self.assertEqual(result["l1_error_upper"], baseline["l1_error_upper"])

    def test_same_descriptor_mutation_rejected(self):
        module = self.load_deflated_certificate_module()
        rows = self.build_personalized_pair_rows()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "scores"

            class MutatingSource(ResidentBooleanSourceFixture):
                scans = 0

                def iterate_class_records(self):
                    self.scans += 1
                    if self.scans == 3:
                        path.write_bytes(b"".join(struct.pack("<Qd", v, 0.5 / len(rows)) for v, _, _ in rows))
                    return super().iterate_class_records()

            source = MutatingSource(rows, 4)
            path.write_bytes(b"".join(struct.pack("<Qd", v, 1. / len(rows)) for v, _, _ in rows))
            with self.assertRaisesRegex(ValueError, "changed"):
                module.certify_boolean_deflated_output(source, path, alpha=0.85, epsilon=10., max_factor_slots=4)

    def test_consumed_prefix_mutation_rejected(self):
        module = self.load_deflated_certificate_module()
        rows = [(v, groups, 1.) for v, groups, _ in self.build_personalized_pair_rows()]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "scores"
            class PrefixMutatingSource(ResidentBooleanSourceFixture):
                scans = 0

                def iterate_class_records(self):
                    self.scans += 1
                    return super().iterate_class_records()

                def iterate_class_vertex_rows(self, class_id):
                    for row in super().iterate_class_vertex_rows(class_id):
                        yield row
                        if self.scans == 3 and class_id == 0:
                            with path.open("r+b", buffering=0) as writer:
                                writer.seek(8)
                                writer.write(struct.pack("<d", 0.5))
                            stamp = path.stat()
                            os.utime(path, ns=(stamp.st_atime_ns, stamp.st_mtime_ns + 1_000_000_000))
            source = PrefixMutatingSource(rows, 4)
            path.write_bytes(b"".join(struct.pack("<Qd", v, 1. / len(rows)) for v, _, _ in rows))
            with self.assertRaisesRegex(ValueError, "changed"):
                module.certify_boolean_deflated_output(source, path, alpha=0.85,
                    epsilon=1e-10, max_factor_slots=4, scratch_dir=directory)
            self.assertEqual(list(Path(directory).iterdir()), [path])


if __name__ == "__main__":
    unittest.main()
