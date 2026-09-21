"""Independent, deliberately resident expanded-graph certificate oracles."""

import hashlib
import importlib.util
import itertools
import json
import math
from decimal import Decimal, Inexact, ROUND_UP, Rounded, localcontext
from fractions import Fraction
from pathlib import Path
import random
import struct
import tempfile
import unittest
from unittest import mock


MODULE = Path(__file__).with_name("boolean_rank_output_certificate.py")
RECORD = struct.Struct("<Qd")


def build_expanded_boolean_adjacency(memberships, weighted=False):
    return [[(len(set(left) & set(right)) if weighted else
              int(bool(set(left) & set(right)))) if i != j else 0
             for j, right in enumerate(memberships)]
            for i, left in enumerate(memberships)]


class ResidentPreparedSource:
    """Fixture only: graph expansion is intentionally independent of the verifier."""

    def __init__(self, memberships, weights, factor_count=None, ids=None):
        self.memberships = [tuple(sorted(set(groups))) for groups in memberships]
        self.weights = [float(value) for value in weights]
        self.vertex_count = len(weights)
        self.factor_count = (factor_count if factor_count is not None else
                             1 + max(itertools.chain.from_iterable(memberships), default=-1))
        self.ids = (ids if ids is not None else
                    [(1 << 63) + 37 * (len(weights) - i) for i in range(len(weights))])
        self.adjacency = build_expanded_boolean_adjacency(memberships)
        self.degrees = [sum(row) for row in self.adjacency]
        self.total_weight = sum(map(Fraction.from_float, self.weights), Fraction())
        self.isolate_weight = sum((Fraction.from_float(weight)
                                   for weight, degree in zip(self.weights, self.degrees)
                                   if degree == 0), Fraction())
        self.snapshot_id = "independent-resident-fixture-v1"
        self.records = []
        self.class_rows = {}
        for ordinal, signature in enumerate(sorted(set(self.memberships))):
            indices = [i for i, groups in enumerate(self.memberships) if groups == signature]
            class_id = ordinal * 3 + 7
            rows = sorted((self.ids[i], self.weights[i]) for i in indices)
            self.class_rows[class_id] = rows
            self.records.append((class_id, signature, len(indices), self.degrees[indices[0]],
                                 sum((Fraction.from_float(weight) for _, weight in rows), Fraction())))
        self.class_count = len(self.records)
        self.active_class_count = sum(record[3] > 0 for record in self.records)
        self.class_passes = 0
        self.row_cursors = 0
        self.pass_hook = None

    def iterate_class_records(self):
        self.class_passes += 1
        if self.pass_hook:
            self.pass_hook(self.class_passes)
        yield from self.records

    def iterate_class_vertex_rows(self, class_id):
        self.row_cursors += 1
        yield from self.class_rows[class_id]

    def iterate_active_factor_counts(self):
        raise AssertionError("certificate must not need factor counts or rederive degrees")


def solve_dense_stationary_scores(source, alpha, weighted=False):
    adjacency = build_expanded_boolean_adjacency(source.memberships, weighted)
    degrees = [sum(row) for row in adjacency]
    damping = Fraction.from_float(alpha)
    preferences = [Fraction.from_float(weight) / source.total_weight for weight in source.weights]
    size = source.vertex_count
    rows = [[Fraction(i == j) - damping * (
        Fraction(adjacency[i][j], degrees[j]) if degrees[j] else preferences[i])
             for j in range(size)] + [(1 - damping) * preferences[i]]
            for i in range(size)]
    for column in range(size):
        pivot = next(i for i in range(column, size) if rows[i][column])
        rows[column], rows[pivot] = rows[pivot], rows[column]
        divisor = rows[column][column]
        rows[column] = [value / divisor for value in rows[column]]
        for i in range(size):
            if i != column:
                coefficient = rows[i][column]
                rows[i] = [left - coefficient * right
                           for left, right in zip(rows[i], rows[column])]
    return [row[-1] for row in rows]


def evaluate_exact_residual_norm(source, scores, alpha):
    damping = Fraction.from_float(alpha)
    exact = list(map(Fraction.from_float, scores))
    dangling = sum((value for value, degree in zip(exact, source.degrees) if not degree), Fraction())
    residual = Fraction()
    for i, value in enumerate(exact):
        preference = Fraction.from_float(source.weights[i]) / source.total_weight
        incoming = sum((source.adjacency[i][j] * exact[j] / source.degrees[j]
                        for j in range(len(exact)) if source.degrees[j]), Fraction())
        residual += abs((1 - damping) * preference + damping * (incoming + preference * dangling) - value)
    return residual


def write_published_binary_records(path, source, scores):
    by_id = dict(zip(source.ids, scores))
    with path.open("wb") as output:
        for class_id, _, _, _, _ in source.records:
            for original_id, _ in source.class_rows[class_id]:
                output.write(RECORD.pack(original_id, by_id[original_id]))


class BooleanCertificateTests(unittest.TestCase):
    def load_certificate_module_required(self):
        self.assertTrue(MODULE.exists(), "actual-bytes Boolean certificate is not implemented")
        spec = importlib.util.spec_from_file_location("boolean_certificate", MODULE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def assert_certificate_encloses_residual(self, report, source, scores, alpha):
        exact = evaluate_exact_residual_norm(source, scores, alpha)
        self.assertLessEqual(Fraction(report["residual_l1_lower"]), exact)
        self.assertLessEqual(exact, Fraction(report["residual_l1_upper"]))
        self.assertLessEqual(exact / (1 - Fraction.from_float(alpha)),
                             Fraction(report["l1_error_upper"]))

    def test_dense_oracle_answers(self):
        module = self.load_certificate_module_required()
        layouts = [((0, 1), (0, 1), (0, 2)),
                   ((0, 1), (0, 1)),
                   ((0, 1), (0, 1), (0,), (1,), (), (2, 3)),
                   ((0,), (0,), (1,), (1,), ()),
                   ((), (0,), (1, 2))]
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "output.bin"
            for memberships in layouts:
                size = len(memberships)
                for weights in ([1.0] * size, [1.0] + [0.0] * (size - 1),
                                [float(i + 1) for i in range(size)]):
                    for alpha in (0.0, 0.5, 0.85, 2.0 ** -60, math.ulp(0.0)):
                        with self.subTest(memberships=memberships, weights=weights, alpha=alpha):
                            source = ResidentPreparedSource(memberships, weights, factor_count=7)
                            oracle = solve_dense_stationary_scores(source, alpha)
                            scores = list(map(float, oracle))
                            write_published_binary_records(output, source, scores)
                            report = module.certify_boolean_rank_output(
                                source, output, alpha=alpha, epsilon=1e-10, max_factor_slots=7,
                                scratch_dir=folder)
                            self.assertTrue(report["accepted"], report)
                            self.assert_certificate_encloses_residual(report, source, scores, alpha)
                            error = sum(abs(Fraction.from_float(x) - y) for x, y in zip(scores, oracle))
                            self.assertLessEqual(error, Fraction(report["l1_error_upper"]))

    def test_boolean_weighted_distinction(self):
        module = self.load_certificate_module_required()
        source = ResidentPreparedSource([(0, 1), (0, 1), (0, 2)], [1.0] * 3)
        oracle = solve_dense_stationary_scores(source, 0.5, weighted=True)
        self.assertEqual(oracle, [Fraction(5, 14), Fraction(5, 14), Fraction(2, 7)])
        scores = list(map(float, oracle))
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            write_published_binary_records(output, source, scores)
            report = module.certify_boolean_rank_output(source, output, alpha=0.5,
                                                       epsilon=1e-10, max_factor_slots=3)
            self.assertFalse(report["accepted"])
            self.assert_certificate_encloses_residual(report, source, scores, 0.5)

    def test_within_class_preferences(self):
        module = self.load_certificate_module_required()
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            for weights, expected in [([1.0, 1.0], True), ([1.0, 0.0], False)]:
                source = ResidentPreparedSource([(0, 1)] * 2, weights)
                write_published_binary_records(output, source, [0.5, 0.5])
                report = module.certify_boolean_rank_output(source, output, alpha=0.5,
                                                           epsilon=1e-10, max_factor_slots=2)
                self.assertEqual(report["accepted"], expected)

    def test_perturbed_residual_enclosures(self):
        module = self.load_certificate_module_required()
        randomizer = random.Random(20260921)
        source = ResidentPreparedSource([(0, 1), (0, 1), (0, 2), (1,), (), (3,)],
                                        [0.1, 0.7, 0.0, 1.3, 2.1, 0.4])
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            for alpha in (0.0, 0.85, math.nextafter(1.0, 0.0), math.ulp(0.0)):
                for precision in (1, 2, 7, 20, 60):
                    for exponent in (-1074, -60, 0, 100, 1000):
                        scores = [math.ldexp(randomizer.random(), exponent) for _ in source.weights]
                        write_published_binary_records(output, source, scores)
                        report = module.certify_boolean_rank_output(
                            source, output, alpha=alpha, epsilon=1e-10, precision=precision,
                            max_factor_slots=source.factor_count)
                        self.assert_certificate_encloses_residual(report, source, scores, alpha)
                        if report["accepted"]:
                            self.assertLessEqual(Fraction(report["l1_error_upper"]), Fraction.from_float(1e-10))

    def test_actual_dangling_mass(self):
        module = self.load_certificate_module_required()
        source = ResidentPreparedSource([(0,), (0,), (), (1,)], [0.0, 0.0, 0.1, 0.3])
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            for scores, accepted in [([0.0, 0.0, 0.25, 0.75], True),
                                     ([0.0, 0.0, 0.5, 1.5], False)]:
                write_published_binary_records(output, source, scores)
                report = module.certify_boolean_rank_output(source, output, alpha=0.85,
                                                           epsilon=1e-10, max_factor_slots=2)
                self.assertEqual(report["accepted"], accepted)
                self.assert_certificate_encloses_residual(report, source, scores, 0.85)

    def test_near_one_damping(self):
        module = self.load_certificate_module_required()
        source = ResidentPreparedSource([(0, 1)] * 2, [1.0, 1.0])
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            write_published_binary_records(output, source, [0.5, 0.5])
            precise = module.certify_boolean_rank_output(source, output, alpha=math.nextafter(1.0, 0.0),
                                                        epsilon=1e-12, max_factor_slots=2)
            self.assertTrue(precise["accepted"], precise)
            coarse = module.certify_boolean_rank_output(source, output, alpha=math.nextafter(1.0, 0.0),
                                                       epsilon=1e-12, precision=2, max_factor_slots=2)
            self.assertFalse(coarse["accepted"])
            self.assert_certificate_encloses_residual(coarse, source, [0.5, 0.5], math.nextafter(1.0, 0.0))

    def test_binary64_tolerance_boundary(self):
        module = self.load_certificate_module_required()
        source = ResidentPreparedSource([(), ()], [1.0, 0.0])
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            write_published_binary_records(output, source, [1.0, 0.1])
            # The exact binary64 0.1 exceeds Decimal("0.1"). Equality must pass.
            epsilon = 0.1
            report = module.certify_boolean_rank_output(source, output, alpha=0.0,
                                                       epsilon=epsilon, max_factor_slots=0)
            self.assertTrue(report["accepted"], report)
            report = module.certify_boolean_rank_output(source, output, alpha=0.0,
                                                       epsilon=math.nextafter(epsilon, 0.0), max_factor_slots=0)
            self.assertFalse(report["accepted"])

    def test_extreme_weight_preferences(self):
        module = self.load_certificate_module_required()
        maximum = float.fromhex("0x1.fffffffffffffp+1023")
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            for weights in ([maximum, maximum, math.ulp(0.0), 0.0],
                            [math.ulp(0.0), 0.0, math.ulp(0.0), math.ulp(0.0)],
                            [0.1, 0.2, 0.3, 0.4]):
                source = ResidentPreparedSource([(0, 1), (0, 1), (), (2,)], weights)
                for alpha in (0.0, math.ulp(0.0), 0.85, math.nextafter(1.0, 0.0)):
                    scores = list(map(float, solve_dense_stationary_scores(source, alpha)))
                    write_published_binary_records(output, source, scores)
                    report = module.certify_boolean_rank_output(source, output, alpha=alpha,
                                                               epsilon=1e-10, max_factor_slots=3)
                    self.assert_certificate_encloses_residual(report, source, scores, alpha)
                    if alpha != math.nextafter(1.0, 0.0):
                        self.assertTrue(report["accepted"], report)

    def test_unordered_distinct_memberships(self):
        module = self.load_certificate_module_required()
        source = ResidentPreparedSource([(0, 1)] * 2, [1.0, 1.0])
        source.records = [(class_id, tuple(reversed(groups)), height, degree, weight)
                          for class_id, groups, height, degree, weight in source.records]
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            write_published_binary_records(output, source, [0.5, 0.5])
            report = module.certify_boolean_rank_output(source, output, alpha=0.5, epsilon=1e-10,
                                                       max_factor_slots=2)
            self.assertTrue(report["accepted"])

    def test_generated_class_stream(self):
        module = self.load_certificate_module_required()

        class GeneratedPreparedSource:
            factor_count = 17
            vertex_count = class_count = factor_count * (factor_count - 1) // 2
            active_class_count = class_count
            total_weight = Fraction(vertex_count)
            isolate_weight = Fraction()
            snapshot_id = "generated-pair-source-v1"
            completed_cursors = 0

            def iterate_class_records(self):
                for class_id, groups in enumerate(itertools.combinations(range(self.factor_count), 2)):
                    before = self.completed_cursors
                    yield class_id, groups, 1, 2 * self.factor_count - 4, Fraction(1)
                    if self.completed_cursors != before + 1:
                        raise AssertionError("materialized class stream before reading its rows")

            def iterate_class_vertex_rows(self, class_id):
                yield 1000 + class_id, 1.0
                self.completed_cursors += 1

        source = GeneratedPreparedSource()
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            with output.open("wb") as values:
                for class_id in range(source.vertex_count):
                    values.write(RECORD.pack(1000 + class_id, 1 / source.vertex_count))
            report = module.certify_boolean_rank_output(source, output, alpha=0.85, epsilon=1e-10,
                                                       max_factor_slots=17, scratch_dir=folder)
            self.assertTrue(report["accepted"])
            self.assertEqual(report["scratch_records"], 136)
            self.assertEqual(report["retained_decimal_values"], 34)
            self.assertEqual(source.completed_cursors, 272)
            self.assertEqual(set(Path(folder).iterdir()), {output})

    def test_negative_zero_allowed(self):
        module = self.load_certificate_module_required()
        source = ResidentPreparedSource([(), ()], [1.0, 0.0], ids=[0, (1 << 64) - 1])
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            write_published_binary_records(output, source, [1.0, -0.0])
            report = module.certify_boolean_rank_output(source, output, alpha=0.85, epsilon=1e-10,
                                                       max_factor_slots=0, scratch_dir=folder)
            self.assertTrue(report["accepted"])

    def test_ambient_context_independence(self):
        module = self.load_certificate_module_required()
        source = ResidentPreparedSource([(0, 1), (0, 1), ()], [0.1, 0.2, 0.7])
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            scores = list(map(float, solve_dense_stationary_scores(source, 0.85)))
            write_published_binary_records(output, source, scores)
            expected = module.certify_boolean_rank_output(source, output, alpha=0.85,
                                                         epsilon=1e-10, max_factor_slots=2)
            with localcontext() as ambient:
                ambient.prec = 1
                ambient.rounding = ROUND_UP
                ambient.Emin = -9
                ambient.Emax = 9
                ambient.traps[Inexact] = True
                ambient.traps[Rounded] = True
                actual = module.certify_boolean_rank_output(source, output, alpha=0.85,
                                                           epsilon=1e-10, max_factor_slots=2)
            self.assertEqual(actual, expected)

    def test_rejects_malformed_outputs(self):
        module = self.load_certificate_module_required()
        source = ResidentPreparedSource([(0, 1), (0, 1), ()], [1.0, 1.0, 1.0],
                                        ids=[0, (1 << 64) - 1, 7])
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            write_published_binary_records(output, source, [0.4, 0.4, 0.2])
            valid = output.read_bytes()
            original_id, _ = RECORD.unpack(valid[:16])
            variants = [valid[:-1], valid + b"x", valid + valid[:16],
                        RECORD.pack(123, 0.2) + valid[16:],
                        valid[:16] + valid[:16] + valid[32:],
                        valid[16:32] + valid[:16] + valid[32:]]
            variants.extend(RECORD.pack(original_id, value) + valid[16:]
                            for value in (-1.0, -math.ulp(0.0), math.nan, math.inf, -math.inf))
            for data in variants:
                output.write_bytes(data)
                with self.subTest(data=data), self.assertRaises(ValueError):
                    module.certify_boolean_rank_output(source, output, alpha=0.85,
                                                       epsilon=1e-10, max_factor_slots=2, scratch_dir=folder)
                self.assertEqual(set(Path(folder).iterdir()), {output})
                self.assertEqual(output.read_bytes(), data)

    def test_factor_capacity_preflight(self):
        module = self.load_certificate_module_required()

        class CapacityOnlySource:
            factor_count = 100

            def __getattr__(self, name):
                raise AssertionError("source work happened before factor-capacity admission: " + name)

        with mock.patch.object(module, "Decimal", side_effect=AssertionError("allocated Decimal")):
            with self.assertRaisesRegex(ValueError, "factor"):
                module.certify_boolean_rank_output(CapacityOnlySource(), "does-not-exist",
                                                   alpha=0.85, epsilon=1e-10, max_factor_slots=99)

    def test_report_resource_accounting(self):
        module = self.load_certificate_module_required()
        source = ResidentPreparedSource([(0, 1), (0, 2), (1, 2), ()], [1.0] * 4,
                                        factor_count=19)
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            sentinel = Path(folder) / "class-totals.jsonl"
            sentinel.write_text("caller owned", encoding="ascii")
            write_published_binary_records(output, source,
                                           list(map(float, solve_dense_stationary_scores(source, 0.5))))
            report = module.certify_boolean_rank_output(source, output, alpha=0.5, epsilon=1e-10,
                                                       max_factor_slots=19, scratch_dir=folder)
            self.assertTrue(report["accepted"])
            self.assertEqual(report["nrows"], 4)
            self.assertEqual(report["row_passes"], 2)
            self.assertEqual(source.class_passes, 2)
            self.assertEqual(source.row_cursors, 2 * source.class_count)
            self.assertEqual(report["retained_decimal_values"], 38)
            self.assertEqual(report["decimal_precision"], 60)
            self.assertEqual(report["snapshot_id"], source.snapshot_id)
            self.assertEqual(report["output_sha256"], hashlib.sha256(output.read_bytes()).hexdigest())
            self.assertGreater(report["scratch_bytes"], 0)
            self.assertEqual(report["scratch_records"], source.class_count)
            self.assertLessEqual(len(report["events"]), 8)
            self.assertEqual(set(Path(folder).iterdir()), {output, sentinel})
            self.assertEqual(sentinel.read_text(encoding="ascii"), "caller owned")
            json.dumps(report)

    def test_detects_betweenpass_tamper(self):
        module = self.load_certificate_module_required()
        source = ResidentPreparedSource([(0, 1)] * 2, [1.0, 1.0])
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            write_published_binary_records(output, source, [0.5, 0.5])

            def mutate_second_pass_bytes(pass_number):
                if pass_number == 2:
                    write_published_binary_records(output, source, [0.25, 0.75])

            source.pass_hook = mutate_second_pass_bytes
            with self.assertRaisesRegex(ValueError, "chang|hash"):
                module.certify_boolean_rank_output(source, output, alpha=0.5, epsilon=10.0,
                                                   max_factor_slots=2, scratch_dir=folder)
            self.assertEqual(set(Path(folder).iterdir()), {output})

    def test_output_descriptor_pinned(self):
        module = self.load_certificate_module_required()
        source = ResidentPreparedSource([(0, 1)] * 2, [1.0, 1.0])
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            replacement = Path(folder) / "replacement"
            write_published_binary_records(output, source, [0.5, 0.5])
            original_hash = hashlib.sha256(output.read_bytes()).hexdigest()
            write_published_binary_records(replacement, source, [0.0, 0.0])

            def replace_second_pass_path(pass_number):
                if pass_number == 2:
                    replacement.replace(output)

            source.pass_hook = replace_second_pass_path
            report = module.certify_boolean_rank_output(source, output, alpha=0.5, epsilon=1e-10,
                                                       max_factor_slots=2, scratch_dir=folder)
            self.assertTrue(report["accepted"])
            self.assertEqual(report["output_sha256"], original_hash)
            self.assertNotEqual(report["output_sha256"], hashlib.sha256(output.read_bytes()).hexdigest())

    def test_source_failure_cleanup(self):
        module = self.load_certificate_module_required()
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            for fail_pass in (1, 2):
                source = ResidentPreparedSource([(0,), (0,)], [1.0, 1.0])
                write_published_binary_records(output, source, [0.5, 0.5])
                failure = OSError("prepared source cursor failed")

                def raise_in_selected_pass(pass_number):
                    if pass_number == fail_pass:
                        raise failure

                source.pass_hook = raise_in_selected_pass
                with self.assertRaises(OSError) as caught:
                    module.certify_boolean_rank_output(source, output, alpha=0.5, epsilon=1e-10,
                                                       max_factor_slots=1, scratch_dir=folder)
                self.assertIs(caught.exception, failure)
                self.assertEqual(set(Path(folder).iterdir()), {output})

    def test_snapshot_epoch_changes(self):
        module = self.load_certificate_module_required()
        source = ResidentPreparedSource([(0,), (0,)], [1.0, 1.0])
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            write_published_binary_records(output, source, [0.5, 0.5])

            def change_second_pass_epoch(pass_number):
                if pass_number == 2:
                    source.snapshot_id = "changed-source-epoch"

            source.pass_hook = change_second_pass_epoch
            with self.assertRaisesRegex(ValueError, "snapshot"):
                module.certify_boolean_rank_output(source, output, alpha=0.5, epsilon=1e-10,
                                                   max_factor_slots=1, scratch_dir=folder)
            self.assertEqual(set(Path(folder).iterdir()), {output})

    def test_midstream_failure_cleanup(self):
        module = self.load_certificate_module_required()
        failure = RuntimeError("row cursor failed after first yield")

        class FailingPreparedSource(ResidentPreparedSource):
            def iterate_class_vertex_rows(self, class_id):
                for row in super().iterate_class_vertex_rows(class_id):
                    yield row
                    raise failure

        source = FailingPreparedSource([(0,), (0,)], [1.0, 1.0])
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            write_published_binary_records(output, source, [0.5, 0.5])
            with self.assertRaises(RuntimeError) as caught:
                module.certify_boolean_rank_output(source, output, alpha=0.5, epsilon=1e-10,
                                                   max_factor_slots=1, scratch_dir=folder)
            self.assertIs(caught.exception, failure)
            self.assertEqual(set(Path(folder).iterdir()), {output})

    def test_empty_universe_certificate(self):
        module = self.load_certificate_module_required()
        source = ResidentPreparedSource([], [], factor_count=3)
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            output.touch()
            report = module.certify_boolean_rank_output(source, output, alpha=0.85, epsilon=math.ulp(0.0),
                                                       max_factor_slots=3, scratch_dir=folder)
            self.assertTrue(report["accepted"])
            self.assertEqual(Fraction(report["l1_error_upper"]), 0)
            self.assertEqual(report["nrows"], 0)
            self.assertEqual(report["scratch_bytes"], 0)
            self.assertEqual(report["row_passes"], 2)

    def test_invalid_parameters_rejected(self):
        module = self.load_certificate_module_required()
        source = ResidentPreparedSource([()], [1.0])
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "answer"
            write_published_binary_records(output, source, [1.0])
            defaults = dict(alpha=0.85, epsilon=1e-10, precision=60, max_factor_slots=0)
            for name, values in {"alpha": [-1.0, 1.0, math.inf, math.nan],
                                 "epsilon": [0.0, -1.0, math.inf, math.nan],
                                 "precision": [0, -1, 1.5, True],
                                 "max_factor_slots": [-1, 0.5, True]}.items():
                for value in values:
                    with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                        module.certify_boolean_rank_output(source, output, **dict(defaults, **{name: value}))


if __name__ == "__main__":
    unittest.main()
