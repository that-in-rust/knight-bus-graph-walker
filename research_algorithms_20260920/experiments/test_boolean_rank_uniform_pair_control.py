"""Independent expanded rational checks; resident oracle arrays are test-only."""

from array import array
from fractions import Fraction
import hashlib
import importlib
from itertools import combinations
import math
import os
from pathlib import Path
import random
import struct
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from boolean_rank_sqlite_source import build_boolean_rank_source, SqliteBooleanRankSource
from stream_boolean_rank_solver import iterate_boolean_rank_output
from test_stream_boolean_rank_solver import ResidentBooleanSourceFixture


def compute_expanded_rational_pagerank(rows, alpha):
    """Test-owned dense oracle from original Boolean adjacency, not class algebra."""
    damping = Fraction.from_float(alpha)
    count = len(rows)
    adjacency = [[int(i != j and bool(set(left[1]) & set(right[1])))
                  for j, right in enumerate(rows)] for i, left in enumerate(rows)]
    degrees = [sum(neighbors) for neighbors in adjacency]
    total = sum((Fraction.from_float(row[2]) for row in rows), Fraction())
    matrix = [[Fraction(i == j) - damping * Fraction(adjacency[i][j], degrees[j])
               for j in range(count)] + [(1 - damping) * Fraction.from_float(rows[i][2]) / total]
              for i in range(count)]
    for column in range(count):
        pivot = next(i for i in range(column, count) if matrix[i][column])
        matrix[column], matrix[pivot] = matrix[pivot], matrix[column]
        scale = matrix[column][column]
        matrix[column] = [value / scale for value in matrix[column]]
        for i in range(count):
            if i != column:
                scale = matrix[i][column]
                matrix[i] = [value - scale * base for value, base in zip(matrix[i], matrix[column])]
    return {row[0]: matrix[i][-1] for i, row in enumerate(rows)}


def build_uniform_pair_rows(factors, height, weights=None):
    pairs = [pair for pair in combinations(range(factors), 2) for _ in range(height)]
    if weights is None:
        weights = [float((i * 7) % 13) for i in range(len(pairs))]
    return [(9001 + 17 * i, pair, weight) for i, (pair, weight) in enumerate(zip(pairs, weights))]


def compute_exact_factor_formula(source, alpha):
    damping = Fraction.from_float(alpha)
    factors = source.factor_count
    height = source.classes[0][2]
    degree = height * (2 * factors - 3) - 1
    eigenvalue = height * (factors - 3) - 1
    return [Fraction(2, factors * degree) + (1 - damping) * (
        sum((weight for _, groups, _, _, weight in source.classes if factor in groups), Fraction())
        / source.total_weight - Fraction(2, factors)) / (degree - damping * eigenvalue)
        for factor in range(factors)]


def compute_projected_rational_rank(rows, factors, height, alpha):
    """Resolve the four eigenspaces, independently of the factor-score formula."""
    damping = Fraction.from_float(alpha)
    degree = height * (2 * factors - 3) - 1
    total = sum((Fraction.from_float(weight) for _, _, weight in rows), Fraction())
    preference = [Fraction.from_float(weight) / total for _, _, weight in rows]
    uniform = Fraction(1, len(rows))
    means = {pair: sum((p for (_, groups, _), p in zip(rows, preference) if groups == pair),
                      Fraction()) / height for pair in combinations(range(factors), 2)}
    centered = {pair: value - uniform for pair, value in means.items()}
    incident = [sum((value for pair, value in centered.items() if factor in pair), Fraction())
                for factor in range(factors)]
    result = {}
    for (vertex, pair, _), p in zip(rows, preference):
        factor_mode = sum((incident[factor] for factor in pair), Fraction()) / (factors - 2)
        pair_mode = centered[pair] - factor_mode
        within_mode = p - means[pair]
        result[vertex] = uniform + (1 - damping) * (
            factor_mode / (1 - damping * Fraction(height * (factors - 3) - 1, degree))
            + pair_mode / (1 + damping * Fraction(height + 1, degree))
            + within_mode / (1 + damping / degree))
    return result


class UniformPairControlTests(unittest.TestCase):
    def load_uniform_control_module(self):
        try:
            return importlib.import_module("boolean_rank_uniform_pair_control")
        except ModuleNotFoundError as error:
            if error.name == "boolean_rank_uniform_pair_control":
                self.fail("uniform-pair direct solver is absent")
            raise

    def compare_expanded_rational_output(self, rows, factors, alpha):
        module = self.load_uniform_control_module()
        source = ResidentBooleanSourceFixture(rows, factors)
        expected = compute_expanded_rational_pagerank(rows, alpha)
        degree = source.classes[0][3]
        exact_factors = compute_exact_factor_formula(source, alpha)
        for factor in range(factors):
            self.assertEqual(exact_factors[factor], sum((expected[v] / degree
                for v, groups, _ in rows if factor in groups), Fraction()))
        self.assertEqual(compute_projected_rational_rank(rows, factors, source.classes[0][2], alpha), expected)
        state = module.solve_uniform_pair_rank_state(source, alpha=alpha, max_factor_slots=factors)
        self.assertEqual(set(state), {"snapshot_id", "alpha", "gamma", "method", "factor_scores", "class_scores", "metrics"})
        self.assertEqual(state["snapshot_id"], source.snapshot_id)
        self.assertEqual(state["alpha"], alpha)
        self.assertEqual(state["gamma"], Fraction(1) - Fraction.from_float(alpha))
        self.assertEqual(state["method"], "uniform-pair-direct")
        self.assertIsNone(state["class_scores"])
        self.assertIsInstance(state["factor_scores"], array)
        self.assertEqual(state["factor_scores"].typecode, "d")
        self.assertEqual(list(state["factor_scores"]), [float(value) for value in exact_factors])
        output = dict(iterate_boolean_rank_output(source, state))
        self.assertEqual(set(output), set(expected))
        self.assertTrue(all(math.isfinite(value) and value >= 0 for value in output.values()))
        error = sum((abs(Fraction.from_float(output[v]) - value) for v, value in expected.items()), Fraction())
        self.assertLess(error, Fraction(1, 10**14), (factors, alpha, error))
        self.assertLess(abs(sum(output.values()) - 1), 1e-14)
        return source, state

    def test_exact_expanded_oracle(self):
        for factors, height in ((4, 1), (4, 3), (5, 1), (5, 2), (6, 1)):
            for alpha in (0.0, 0.5, 0.85, 0.999, math.nextafter(1.0, 0.0)):
                with self.subTest(factors=factors, height=height, alpha=alpha):
                    self.compare_expanded_rational_output(build_uniform_pair_rows(factors, height), factors, alpha)

    def test_extreme_personalization_weights(self):
        minimum = math.ulp(0.0)
        maximum = sys.float_info.max
        weights = ([1.0] + [0.0] * 11, [minimum] * 12, [maximum] * 12,
                   [maximum, minimum, maximum, 0.0, minimum, 1.0] * 2,
                   [1.0, 2.0**-53, 2.0**-54, 2.0**-55, 0.0, 0.0] * 2)
        for values in weights:
            for alpha in (0.0, 2.0**-60, 0.85, math.nextafter(1.0, 0.0)):
                with self.subTest(weights=values[:3], alpha=alpha):
                    self.compare_expanded_rational_output(build_uniform_pair_rows(4, 2, values), 4, alpha)

    def test_seeded_arbitrary_preferences(self):
        randomizer = random.Random(20260920)
        for _ in range(8):
            factors = randomizer.randrange(4, 7)
            height = randomizer.randrange(1, 3)
            weights = [math.ldexp(randomizer.random(), randomizer.randrange(-100, 100))
                       for _ in range(height * factors * (factors - 1) // 2)]
            self.compare_expanded_rational_output(build_uniform_pair_rows(factors, height, weights), factors, 0.85)

    def test_native_prepared_input(self):
        module = self.load_uniform_control_module()
        rows = build_uniform_pair_rows(5, 2)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "native.sqlite"
            receipt = build_boolean_rank_source(path,
                ((vertex, (groups[1], groups[0], groups[1]), weight) for vertex, groups, weight in reversed(rows)),
                factor_count=5, cache_kib=32, batch_records=3)
            with SqliteBooleanRankSource(path, cache_kib=32) as source:
                with patch.object(source, "iterate_class_vertex_rows", side_effect=AssertionError("solver read vertices")):
                    state = module.solve_uniform_pair_rank_state(source, alpha=0.85, max_factor_slots=5)
                self.assertEqual(source.events["active_cursors"], 0)
                self.assertEqual(state["snapshot_id"], receipt["snapshot_id"])
                expected = compute_expanded_rational_pagerank(rows, 0.85)
                output = dict(iterate_boolean_rank_output(source, state))
                self.assertLess(sum(abs(Fraction.from_float(output[v]) - value) for v, value in expected.items()),
                                Fraction(1, 10**14))
                self.assertEqual(source.events["active_cursors"], 0)

    def test_full_factor_reservation(self):
        module = self.load_uniform_control_module()
        for factors, capacity in ((4, 3), (9, 8), (4, True), (4, 4.0), (4, -1)):
            source = ResidentBooleanSourceFixture(build_uniform_pair_rows(4, 1), factors)
            with self.subTest(factors=factors, capacity=capacity):
                with patch.object(module, "array", side_effect=AssertionError("allocated before admission")):
                    with self.assertRaises(ValueError):
                        module.solve_uniform_pair_rank_state(source, alpha=0.85, max_factor_slots=capacity)
                self.assertEqual(source.calls, 0)

    def test_invalid_damping_options(self):
        module = self.load_uniform_control_module()
        source = ResidentBooleanSourceFixture(build_uniform_pair_rows(4, 1), 4)
        for alpha in (-0.1, 1.0, math.inf, math.nan, True, 0, Fraction(1, 2), "0.85"):
            with self.subTest(alpha=alpha), self.assertRaises(ValueError):
                module.solve_uniform_pair_rank_state(source, alpha=alpha, max_factor_slots=4)
            self.assertEqual(source.calls, 0)

    def test_invalid_topology_refusal(self):
        module = self.load_uniform_control_module()
        valid = build_uniform_pair_rows(4, 2)
        cases = (([], 4), (build_uniform_pair_rows(3, 1), 3), (valid[:-1], 4),
                 (valid[2:], 4), (valid + [(99999, (), 1.0)], 4),
                 (valid + [(99999, (0,), 1.0)], 4), (valid, 5))
        for rows, factors in cases:
            with self.subTest(count=len(rows), factors=factors):
                source = ResidentBooleanSourceFixture(rows, factors)
                with self.assertRaises(ValueError):
                    module.solve_uniform_pair_rank_state(source, alpha=0.0, max_factor_slots=factors)
        for field, value in ((1, (0, 1)), (2, 3), (3, 999)):
            source = ResidentBooleanSourceFixture(valid, 4)
            record = list(source.classes[1])
            record[field] = value
            source.classes[1] = tuple(record)
            with self.subTest(field=field), self.assertRaises(ValueError):
                module.solve_uniform_pair_rank_state(source, alpha=0.85, max_factor_slots=4)

    def test_exact_accumulator_accounting(self):
        module = self.load_uniform_control_module()
        rows = build_uniform_pair_rows(5, 2, [sys.float_info.max, math.ulp(0.0)] * 10)
        source = ResidentBooleanSourceFixture(rows, 5)
        expected = [Fraction()] * 5
        numerator_peak, denominator_peak = 0, 5
        for _, groups, _, _, weight in source.classes:
            for factor in groups:
                expected[factor] += weight
                numerator_peak = max(numerator_peak, sum(value.numerator.bit_length() for value in expected))
                denominator_peak = max(denominator_peak, sum(value.denominator.bit_length() for value in expected))
        state = module.solve_uniform_pair_rank_state(source, alpha=0.85, max_factor_slots=5)
        metrics = state["metrics"]
        self.assertEqual(metrics["dimension"], 5)
        self.assertEqual(metrics["factor_slots"], 5)
        self.assertEqual(metrics["exact_factor_sum_slots"], 5)
        self.assertEqual(metrics["exact_factor_sum_updates"], 20)
        self.assertEqual(metrics["exact_factor_numerator_bits_peak"], numerator_peak)
        self.assertEqual(metrics["exact_factor_denominator_bits_peak"], denominator_peak)
        self.assertGreater(numerator_peak, 5 * 2000)
        self.assertEqual(metrics["factor_score_payload_bytes"], 5 * array("d").itemsize)
        self.assertEqual(metrics["class_passes"], 1)
        self.assertEqual(metrics["class_records"], 10)
        self.assertEqual(metrics["matrix_applications"], 0)
        self.assertEqual(metrics["iterations"], 0)
        self.assertNotIn("vector_payload_upper_bytes", metrics)
        self.assertNotIn("recomputed_relative_residual", metrics)
        self.assertFalse(metrics["certified"])

    def test_source_failure_cleanup(self):
        module = self.load_uniform_control_module()
        rows = build_uniform_pair_rows(4, 2)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "failure.sqlite"
            build_boolean_rank_source(path, iter(rows), factor_count=4)
            for failure_pass in (1, 2):
                with SqliteBooleanRankSource(path) as source:
                    original = source.iterate_class_records
                    pass_count = 0

                    def open_failing_class_cursor():
                        nonlocal pass_count
                        pass_count += 1
                        cursor = original()
                        if pass_count == failure_pass:
                            def decode_exploding_class_record(record):
                                raise RuntimeError("injected class decode failure")
                            cursor._decoder = decode_exploding_class_record
                        return cursor

                    with patch.object(source, "iterate_class_records", open_failing_class_cursor):
                        with self.assertRaisesRegex(RuntimeError, "injected class decode failure"):
                            module.solve_uniform_pair_rank_state(source, alpha=0.85, max_factor_slots=4)
                    self.assertEqual(source.events["active_cursors"], 0)

    def test_snapshot_binding_output(self):
        source, state = self.compare_expanded_rational_output(build_uniform_pair_rows(4, 1), 4, 0.85)
        source.snapshot_id = "different-snapshot"
        with self.assertRaisesRegex(ValueError, "snapshot"):
            list(iterate_boolean_rank_output(source, state))

    def test_consumer_failure_closes(self):
        module = self.load_uniform_control_module()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nondyadic.sqlite"
            build_boolean_rank_source(path, iter(build_uniform_pair_rows(4, 1)), factor_count=4)
            with SqliteBooleanRankSource(path) as source:
                original = source.iterate_class_records
                source._total_weight = Fraction(2)

                def open_nondyadic_class_cursor():
                    cursor = original()
                    decoder = cursor._decoder

                    def decode_nondyadic_class_record(record):
                        decoded = decoder(record)
                        return (*decoded[:4], Fraction(1, 3))

                    cursor._decoder = decode_nondyadic_class_record
                    return cursor

                with patch.object(source, "iterate_class_records", open_nondyadic_class_cursor):
                    with self.assertRaisesRegex(ValueError, "dyadic"):
                        module.solve_uniform_pair_rank_state(source, alpha=0.85, max_factor_slots=4)
                # The decode succeeded and the cursor still had five rows:
                # cleanup must come from the consumer's exception path.
                self.assertEqual(source.events["active_cursors"], 0)

    def test_midstream_snapshot_refusal(self):
        module = self.load_uniform_control_module()
        source = ResidentBooleanSourceFixture(build_uniform_pair_rows(4, 1), 4)
        original = source.iterate_class_records

        def iterate_mutating_class_records():
            records = original()
            yield from records
            if source.calls == 2:
                source.snapshot_id = "changed-during-solve"

        with patch.object(source, "iterate_class_records", iterate_mutating_class_records):
            with self.assertRaisesRegex(ValueError, "changed"):
                module.solve_uniform_pair_rank_state(source, alpha=0.85, max_factor_slots=4)


class UniformPairCertificateTests(unittest.TestCase):
    def load_exact_certificate_module(self):
        module = importlib.import_module("boolean_rank_uniform_pair_control")
        self.assertTrue(callable(getattr(module, "certify_uniform_pair_exact_output", None)),
                        "uniform-pair exact-target certificate is absent")
        return module

    def prepare_dense_oracle_output(self, rows, alpha):
        expected = compute_expanded_rational_pagerank(rows, alpha)
        ordered = sorted(rows, key=lambda row: (tuple(sorted(row[1])), row[0]))
        raw = b"".join(struct.pack("<Qd", vertex, float(expected[vertex])) for vertex, _, _ in ordered)
        error = sum((abs(Fraction.from_float(float(value)) - value) for value in expected.values()), Fraction())
        return raw, error, expected

    def test_certificate_dense_oracle(self):
        module = self.load_exact_certificate_module()
        fixtures = ((4, 1, None), (5, 2, None), (4, 2, [1.0] + [0.0] * 11),
                    (4, 2, [sys.float_info.max, math.ulp(0.0)] * 6),
                    (4, 2, [sys.float_info.max, math.ulp(0.0), sys.float_info.max / 2, 1.0, 0.0, math.ulp(0.0)] * 2),
                    (4, 2, [math.ulp(0.0)] * 12), (4, 2, [sys.float_info.max] * 12))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "scores.bin"
            for factors, height, weights in fixtures:
                for alpha in (0.0, 2.0**-60, 0.85, math.nextafter(1.0, 0.0)):
                    rows = build_uniform_pair_rows(factors, height, weights)
                    source = ResidentBooleanSourceFixture(rows, factors)
                    raw, expected_error, _ = self.prepare_dense_oracle_output(rows, alpha)
                    path.write_bytes(raw)
                    with self.subTest(factors=factors, height=height, alpha=alpha):
                        with patch.object(module, "array", side_effect=AssertionError("rounded factor state")):
                            with patch.object(module, "solve_uniform_pair_rank_state", side_effect=AssertionError("rounded solver")):
                                report = module.certify_uniform_pair_exact_output(source, path,
                                    alpha=alpha, epsilon=1e-12, max_factor_slots=factors)
                        self.assertEqual(Fraction(report["l1_error_upper"]), expected_error)
                        self.assertTrue(report["accepted"])
                        self.assertEqual(report["output_sha256"], hashlib.sha256(raw).hexdigest())
                        self.assertEqual(report["nrows"], len(rows))
                        self.assertEqual(report["row_passes"], 1)
                        self.assertEqual(report["scratch_bytes"], 0)
                        self.assertEqual(report["output_read_bytes"], len(raw))
                        self.assertEqual(report["snapshot_id"], source.snapshot_id)
                        self.assertEqual(report["retained_fraction_values"], factors)
                        self.assertIn("exact", report["scope"])

    def test_certificate_threshold_equality(self):
        module = self.load_exact_certificate_module()
        rows = build_uniform_pair_rows(4, 2)
        source = ResidentBooleanSourceFixture(rows, 4)
        alpha = math.nextafter(1.0, 0.0)
        raw, _, oracle = self.prepare_dense_oracle_output(rows, alpha)
        vertex, value = struct.unpack("<Qd", raw[:16])
        raw = struct.pack("<Qd", vertex, value + 0.001) + raw[16:]
        exact_error = sum((abs(Fraction.from_float(value) - oracle[vertex])
                           for vertex, value in struct.iter_unpack("<Qd", raw)), Fraction())
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "perturbed.bin"
            path.write_bytes(raw)
            for epsilon, accepted in ((exact_error, True), (exact_error / 2, False)):
                report = module.certify_uniform_pair_exact_output(source, path,
                    alpha=alpha, epsilon=epsilon, max_factor_slots=4)
                self.assertEqual(Fraction(report["l1_error_upper"]), exact_error)
                self.assertEqual(report["accepted"], accepted)
                self.assertEqual(report["nrows"], len(rows))
                self.assertEqual(report["output_sha256"], hashlib.sha256(raw).hexdigest())

    def test_certificate_factor_accounting(self):
        module = self.load_exact_certificate_module()
        rows = build_uniform_pair_rows(5, 2, [sys.float_info.max, math.ulp(0.0)] * 10)
        source = ResidentBooleanSourceFixture(rows, 5)
        exact = compute_exact_factor_formula(source, 0.85)
        raw, error, _ = self.prepare_dense_oracle_output(rows, 0.85)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bits.bin"
            path.write_bytes(raw)
            report = module.certify_uniform_pair_exact_output(source, path,
                alpha=0.85, epsilon=1e-12, max_factor_slots=5)
        metrics = report["metrics"]
        self.assertEqual(metrics["exact_factor_fraction_slots"], 5)
        self.assertEqual(metrics["exact_factor_state_numerator_bits_final"],
                         sum(value.numerator.bit_length() for value in exact))
        self.assertEqual(metrics["exact_factor_state_denominator_bits_final"],
                         sum(value.denominator.bit_length() for value in exact))
        self.assertGreater(metrics["exact_factor_state_numerator_bits_peak"], 10000)
        self.assertGreater(metrics["exact_factor_state_denominator_bits_peak"], 5000)
        self.assertGreaterEqual(metrics["l1_error_numerator_bits_peak"], error.numerator.bit_length())
        self.assertGreaterEqual(metrics["l1_error_denominator_bits_peak"], error.denominator.bit_length())
        self.assertEqual(metrics["factor_score_payload_bytes"], 0)
        self.assertEqual(report["class_passes"], 3)
        self.assertEqual(report["class_records"], 30)

    def test_certificate_admission_precedes(self):
        module = self.load_exact_certificate_module()
        cases = [dict(max_factor_slots=value) for value in (3, -1, True, 4.0)]
        cases += [dict(alpha=value) for value in (1.0, -0.1, math.nan, math.inf, True, 0)]
        cases += [dict(epsilon=value) for value in (0.0, -1.0, math.nan, math.inf, True, "0.01")]
        for overrides in cases:
            source = ResidentBooleanSourceFixture(build_uniform_pair_rows(4, 1), 4)
            options = dict(alpha=0.85, epsilon=1e-12, max_factor_slots=4)
            options.update(overrides)
            with self.subTest(options=overrides):
                with patch.object(module, "open", side_effect=AssertionError("opened before admission"), create=True):
                    with self.assertRaises(ValueError):
                        module.certify_uniform_pair_exact_output(source, "absent.bin", **options)
                self.assertEqual(source.calls, 0)
        source = ResidentBooleanSourceFixture(build_uniform_pair_rows(4, 1), 9)
        with self.assertRaises(ValueError):
            module.certify_uniform_pair_exact_output(source, "absent.bin", alpha=0.85,
                epsilon=1e-12, max_factor_slots=8)
        self.assertEqual(source.calls, 0)
        source = ResidentBooleanSourceFixture(build_uniform_pair_rows(4, 1)[:-1], 4)
        with patch.object(module, "open", side_effect=AssertionError("opened invalid topology"), create=True):
            with self.assertRaises(ValueError):
                module.certify_uniform_pair_exact_output(source, "absent.bin", alpha=0.0,
                    epsilon=1e-12, max_factor_slots=4)

    def test_certificate_malformed_cleanup(self):
        module = self.load_exact_certificate_module()
        rows = build_uniform_pair_rows(4, 2)
        raw, _, _ = self.prepare_dense_oracle_output(rows, 0.85)
        vertex = rows[0][0]
        malformed = [b"", raw[:-1], raw[:-16], raw + b"x", raw + raw[:16],
                     struct.pack("<Qd", vertex + 1, 0.25) + raw[16:],
                     raw[16:32] + raw[:16] + raw[32:], raw[:16] * 2 + raw[32:]]
        malformed += [struct.pack("<Qd", vertex, value) + raw[16:] for value in (math.nan, math.inf, -math.inf, -0.1)]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "malformed.bin"
            database = Path(directory) / "native.sqlite"
            build_boolean_rank_source(database, iter(rows), factor_count=4)
            with SqliteBooleanRankSource(database) as source:
                for index, data in enumerate(malformed):
                    path.write_bytes(data)
                    opened = []

                    def open_tracking_output_file(*args, **kwargs):
                        handle = open(*args, **kwargs)
                        opened.append(handle)
                        return handle

                    with self.subTest(case=index), patch.object(module, "open", open_tracking_output_file, create=True):
                        with self.assertRaises(ValueError):
                            module.certify_uniform_pair_exact_output(source, path,
                                alpha=0.85, epsilon=1e-12, max_factor_slots=4)
                    self.assertTrue(all(handle.closed for handle in opened))
                    self.assertEqual(source.events["active_cursors"], 0)

    def test_certificate_native_singlepass(self):
        module = self.load_exact_certificate_module()
        rows = build_uniform_pair_rows(5, 2)
        alpha = math.nextafter(1.0, 0.0)
        raw, error, _ = self.prepare_dense_oracle_output(rows, alpha)
        with tempfile.TemporaryDirectory() as directory:
            path, database = Path(directory) / "native.bin", Path(directory) / "source.sqlite"
            path.write_bytes(raw)
            build_boolean_rank_source(database, iter(reversed(rows)), factor_count=5)
            with SqliteBooleanRankSource(database) as source:
                with open(path, "rb", buffering=0) as handle:
                    observed = MagicMock(wraps=handle)
                    observed.__enter__.return_value = observed
                    observed.__exit__.side_effect = lambda *args: handle.close()
                    observed.seek.side_effect = AssertionError("output was rescanned")
                    with patch.object(module, "open", return_value=observed, create=True) as opener:
                        report = module.certify_uniform_pair_exact_output(source, path,
                            alpha=alpha, epsilon=1e-12, max_factor_slots=5)
                    self.assertEqual(opener.call_count, 1)
                    self.assertEqual([call.args[0] for call in observed.read.call_args_list], [16] * len(rows) + [1])
                    self.assertTrue(handle.closed)
                self.assertEqual(Fraction(report["l1_error_upper"]), error)
                self.assertEqual(source.events["active_cursors"], 0)
            self.assertEqual(sorted(item.name for item in Path(directory).iterdir()), ["native.bin", "source.sqlite"])

    def test_certificate_read_failure(self):
        module = self.load_exact_certificate_module()
        rows = build_uniform_pair_rows(4, 2)
        raw, _, _ = self.prepare_dense_oracle_output(rows, 0.85)
        with tempfile.TemporaryDirectory() as directory:
            path, database = Path(directory) / "failure.bin", Path(directory) / "source.sqlite"
            path.write_bytes(raw)
            build_boolean_rank_source(database, iter(rows), factor_count=4)
            with SqliteBooleanRankSource(database) as source, open(path, "rb", buffering=0) as handle:
                observed = MagicMock(wraps=handle)
                observed.__enter__.return_value = observed
                observed.__exit__.side_effect = lambda *args: handle.close()
                observed.read.side_effect = OSError("injected output failure")
                with patch.object(module, "open", return_value=observed, create=True):
                    with self.assertRaisesRegex(OSError, "injected"):
                        module.certify_uniform_pair_exact_output(source, path,
                            alpha=0.85, epsilon=1e-12, max_factor_slots=4)
                self.assertTrue(handle.closed)
                self.assertEqual(source.events["active_cursors"], 0)

    def test_certificate_snapshot_refusal(self):
        module = self.load_exact_certificate_module()
        rows = build_uniform_pair_rows(4, 1)
        raw, _, _ = self.prepare_dense_oracle_output(rows, 0.85)
        source = ResidentBooleanSourceFixture(rows, 4)
        original = source.iterate_class_vertex_rows

        def iterate_mutating_vertex_records(class_id):
            yield from original(class_id)
            source.snapshot_id = "mutated-during-certificate"

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "snapshot.bin"
            path.write_bytes(raw)
            with patch.object(source, "iterate_class_vertex_rows", iterate_mutating_vertex_records):
                with self.assertRaisesRegex(ValueError, "snapshot"):
                    module.certify_uniform_pair_exact_output(source, path,
                        alpha=0.85, epsilon=1e-12, max_factor_slots=4)

    def test_certificate_source_refusal(self):
        module = self.load_exact_certificate_module()
        rows = build_uniform_pair_rows(4, 2)
        raw, _, _ = self.prepare_dense_oracle_output(rows, 0.85)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.bin"
            path.write_bytes(raw)
            for mutation in ("short", "long", "order", "weight", "nan", "negative", "id"):
                source = ResidentBooleanSourceFixture(rows, 4)
                first, second = source.by_class[0]
                replacements = {"short": [first], "long": [first, second, (99999, 1.0)],
                    "order": [second, first], "weight": [(first[0], first[1] + 1.0), second],
                    "nan": [(first[0], math.nan), second], "negative": [(first[0], -1.0), second],
                    "id": [(True, first[1]), second]}
                source.by_class[0] = replacements[mutation]
                with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                    module.certify_uniform_pair_exact_output(source, path,
                        alpha=0.85, epsilon=1e-12, max_factor_slots=4)

    def test_certificate_actual_solver(self):
        module = self.load_exact_certificate_module()
        rows = build_uniform_pair_rows(5, 2)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "direct.bin"
            for alpha in (0.0, 0.85, math.nextafter(1.0, 0.0)):
                source = ResidentBooleanSourceFixture(rows, 5)
                oracle = compute_expanded_rational_pagerank(rows, alpha)
                state = module.solve_uniform_pair_rank_state(source, alpha=alpha, max_factor_slots=5)
                raw = b"".join(struct.pack("<Qd", *row) for row in iterate_boolean_rank_output(source, state))
                path.write_bytes(raw)
                error = sum((abs(Fraction.from_float(value) - oracle[vertex])
                             for vertex, value in struct.iter_unpack("<Qd", raw)), Fraction())
                report = module.certify_uniform_pair_exact_output(source, path,
                    alpha=alpha, epsilon=1e-12, max_factor_slots=5)
                self.assertTrue(report["accepted"])
                self.assertEqual(Fraction(report["l1_error_upper"]), error)

    def test_certificate_interrupt_cleanup(self):
        module = self.load_exact_certificate_module()
        rows = build_uniform_pair_rows(4, 2)
        raw, _, _ = self.prepare_dense_oracle_output(rows, 0.85)
        with tempfile.TemporaryDirectory() as directory:
            path, database = Path(directory) / "interrupt.bin", Path(directory) / "source.sqlite"
            path.write_bytes(raw)
            build_boolean_rank_source(database, iter(rows), factor_count=4)
            with SqliteBooleanRankSource(database) as source, open(path, "rb", buffering=0) as handle:
                observed = MagicMock(wraps=handle)
                observed.__enter__.return_value = observed
                observed.__exit__.side_effect = lambda *args: handle.close()
                original = source.iterate_class_vertex_rows

                def open_interrupting_vertex_cursor(class_id):
                    cursor = original(class_id)

                    def decode_interrupted_vertex_record(record):
                        raise KeyboardInterrupt("injected source interruption")

                    cursor._decoder = decode_interrupted_vertex_record
                    return cursor

                with patch.object(source, "iterate_class_vertex_rows", open_interrupting_vertex_cursor):
                    with patch.object(module, "open", return_value=observed, create=True):
                        with self.assertRaisesRegex(KeyboardInterrupt, "injected"):
                            module.certify_uniform_pair_exact_output(source, path,
                                alpha=0.85, epsilon=1e-12, max_factor_slots=4)
                self.assertEqual(source.events["active_cursors"], 0)
                self.assertTrue(handle.closed)

    def test_certificate_descriptor_mutation(self):
        module = self.load_exact_certificate_module()
        rows = build_uniform_pair_rows(4, 2)
        raw, _, _ = self.prepare_dense_oracle_output(rows, 0.85)
        source = ResidentBooleanSourceFixture(rows, 4)
        original = source.iterate_class_vertex_rows
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "mutating.bin"
            path.write_bytes(raw)
            metadata = path.stat()

            def iterate_modified_output_vertices(class_id):
                yield from original(class_id)
                if class_id == 0:
                    os.utime(path, ns=(metadata.st_atime_ns, metadata.st_mtime_ns + 1000000))

            with patch.object(source, "iterate_class_vertex_rows", iterate_modified_output_vertices):
                with self.assertRaisesRegex(ValueError, "descriptor changed"):
                    module.certify_uniform_pair_exact_output(source, path,
                        alpha=0.85, epsilon=1e-12, max_factor_slots=4)


if __name__ == "__main__":
    unittest.main()
