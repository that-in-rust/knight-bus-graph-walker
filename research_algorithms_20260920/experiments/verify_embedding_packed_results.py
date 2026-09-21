"""Independent rational oracle for the packed A06 workflow experiment."""

import argparse
from fractions import Fraction
import json
from math import isfinite, isqrt, lcm
from pathlib import Path
import struct
import tempfile
import unittest


MASK = (1 << 64) - 1
C1 = 0x9E3779B97F4A7C15
C2 = 0xBF58476D1CE4E5B9
C3 = 0x94D049BB133111EB


def compute_seeded_counter_hash(value):
    value = (value + C1) & MASK
    value = ((value ^ (value >> 30)) * C2) & MASK
    value = ((value ^ (value >> 27)) * C3) & MASK
    return value ^ (value >> 31)


def initialize_exact_source_row(seed, node, dimension):
    return [Fraction(1) if j == 0 else Fraction(
        compute_seeded_counter_hash(seed ^ ((node*C1) & MASK) ^ ((j*C2) & MASK)) % 3 - 1
    ) for j in range(dimension)]


def read_canonical_membership_source(path):
    with path.open("rb") as stream:
        header = stream.read(32)
        if len(header) != 32 or header[:8] != b"KBMEAN01":
            raise ValueError("invalid canonical source header")
        nodes, features, seed = struct.unpack("<QQQ", header[8:])
        if not (2 <= features <= nodes // 2 and nodes <= 128):
            raise ValueError("oracle requires 2 <= F <= N/2 and N <= 128")
        counts, rows = [0] * features, []
        for expected in range(nodes):
            record = stream.read(16)
            if len(record) != 16:
                raise ValueError("truncated canonical source")
            node, left, right = struct.unpack("<QII", record)
            if node != expected or left == right or max(left, right) >= features:
                raise ValueError("invalid canonical membership record")
            rows.append((left, right))
            counts[left] += 1
            counts[right] += 1
        if stream.read(1) or any(count == 1 for count in counts):
            raise ValueError("extra source bytes or invalid feature cardinality")
    return rows, counts, seed


def enclose_exact_normalized_row(values, precision=96):
    denominator = lcm(*(value.denominator for value in values))
    integers = [int(value * denominator) for value in values]
    square = sum(value * value for value in integers)
    if square == 0:
        return [(Fraction(0), Fraction(0)) for _ in integers]
    sqrt_precision = precision + 8
    lower = isqrt(square << (2 * sqrt_precision))
    upper = lower + 1
    result = []
    for value in integers:
        numerator = value << (precision + sqrt_precision)
        low_den, high_den = (upper, lower) if value >= 0 else (lower, upper)
        low = Fraction(numerator // low_den, 1 << precision)
        high = Fraction(-((-numerator) // high_den), 1 << precision)
        if value >= 0:
            assert 0 <= low <= high and low*low*square <= value*value <= high*high*square
        else:
            assert low <= high <= 0 and high*high*square <= value*value <= low*low*square
        result.append((low, high))
    return result


def compute_dense_exact_target(rows, counts, seed, dimension, depth):
    if not (1 <= dimension <= 64 and 0 <= depth <= 16):
        raise ValueError("oracle dimension/depth outside supported experiment")
    weights = [1 + feature % 17 for feature in range(len(counts))]
    degrees = [sum(weights[f] * (counts[f] - 1) for f in row) for row in rows]
    matrix = [[sum(weights[f] for f in set(left) & set(right)) if i != j else 0
               for j, right in enumerate(rows)] for i, left in enumerate(rows)]
    assert [sum(row) for row in matrix] == degrees
    current = [initialize_exact_source_row(seed, node, dimension) for node in range(len(rows))]
    answer = [[[Fraction(0), Fraction(0)] for _ in range(dimension)] for _ in rows]
    for layer in range(depth + 1):
        coefficient = Fraction(2 if layer % 2 == 0 else -1, 8)
        for i, row in enumerate(current):
            for j, (low, high) in enumerate(enclose_exact_normalized_row(row)):
                if coefficient < 0:
                    low, high = high, low
                answer[i][j][0] += coefficient * low
                answer[i][j][1] += coefficient * high
        if layer < depth:
            current = [[sum(Fraction(matrix[i][other], degrees[i]) * current[other][j]
                            for other in range(len(rows))) for j in range(dimension)]
                       for i in range(len(rows))]
    return answer


def verify_complete_packed_output(source, output, tolerance):
    if tolerance <= 0:
        raise ValueError("tolerance must be positive")
    rows, counts, seed = read_canonical_membership_source(source)
    with output.open("rb") as stream:
        header = stream.read(40)
        if len(header) != 40 or header[:8] != b"KBOUT001":
            raise ValueError("invalid result header")
        nodes, dimension, depth, grid_bits = struct.unpack("<QQQQ", header[8:])
        if nodes != len(rows) or grid_bits > 28:
            raise ValueError("result header disagrees with source/experiment contract")
        exact = compute_dense_exact_target(rows, counts, seed, dimension, depth)
        maximum_error, checked = Fraction(0), 0
        for node in range(nodes):
            record = stream.read(8 * (dimension + 1))
            if len(record) != 8 * (dimension + 1):
                raise ValueError("missing complete output row")
            actual_node = struct.unpack("<Q", record[:8])[0]
            if actual_node != node:
                raise ValueError("result original node ID mismatch")
            for actual, (low, high) in zip(struct.unpack("<" + "d" * dimension, record[8:]), exact[node]):
                if not isfinite(actual):
                    raise ValueError("nonfinite output")
                rational = Fraction.from_float(actual)
                upper_error = max(abs(rational - low), abs(rational - high))
                if upper_error > tolerance:
                    raise AssertionError(f"node {node}: certified oracle discrepancy {upper_error} exceeds {tolerance}")
                maximum_error = max(maximum_error, upper_error)
                checked += 1
        if stream.read(1):
            raise ValueError("unexpected trailing output bytes")
    return {"nodes": nodes, "dimension": dimension, "depth": depth, "grid_bits": grid_bits,
            "coordinates_checked": checked, "maximum_error_upper": float(maximum_error),
            "tolerance": str(tolerance), "status": "passed"}


class IndependentRationalOracleTests(unittest.TestCase):
    def test_known_splitmix_values(self):
        self.assertEqual(compute_seeded_counter_hash(0), 0xE220A8397B1DCDAF)
        self.assertEqual(compute_seeded_counter_hash(1), 0x910A2DEC89025CC1)

    def test_signed_exact_normalizer(self):
        result = enclose_exact_normalized_row([Fraction(-1), Fraction(2), Fraction(2)])
        for (low, high), expected in zip(result, (Fraction(-1, 3), Fraction(2, 3), Fraction(2, 3))):
            self.assertLessEqual(low, expected)
            self.assertGreaterEqual(high, expected)
        self.assertEqual(enclose_exact_normalized_row([Fraction(0)]), [(Fraction(0), Fraction(0))])

    def test_single_coordinate_target(self):
        rows, counts = [(0, 1)] * 4, [4, 4]
        result = compute_dense_exact_target(rows, counts, 7, 1, 4)
        expected = Fraction(2 - 1 + 2 - 1 + 2, 8)
        for row in result:
            self.assertLessEqual(row[0][0], expected)
            self.assertGreaterEqual(row[0][1], expected)

    def test_complete_output_verification(self):
        with tempfile.TemporaryDirectory() as directory:
            source, output = Path(directory)/"source.bin", Path(directory)/"result.bin"
            source.write_bytes(b"KBMEAN01"+struct.pack("<QQQ",4,2,7)+
                               b"".join(struct.pack("<QII",i,0,1) for i in range(4)))
            output.write_bytes(b"KBOUT001"+struct.pack("<QQQQ",4,1,2,24)+
                               b"".join(struct.pack("<Qd",i,0.375) for i in range(4)))
            result=verify_complete_packed_output(source,output,Fraction(1,10000))
            self.assertEqual(result["coordinates_checked"],4)

    def test_unused_feature_columns_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            source, output = Path(directory)/"source.bin", Path(directory)/"result.bin"
            source.write_bytes(b"KBMEAN01"+struct.pack("<QQQ",8,4,7)+
                               b"".join(struct.pack("<QII",i,0,1) for i in range(8)))
            output.write_bytes(b"KBOUT001"+struct.pack("<QQQQ",8,1,2,24)+
                               b"".join(struct.pack("<Qd",i,0.375) for i in range(8)))
            result=verify_complete_packed_output(source,output,Fraction(1,10000))
            self.assertEqual(result["coordinates_checked"],8)

    def test_oracle_rejects_corruption(self):
        with tempfile.TemporaryDirectory() as directory:
            source, output = Path(directory)/"source.bin", Path(directory)/"result.bin"
            source.write_bytes(b"KBMEAN01"+struct.pack("<QQQ",4,2,7)+
                               b"".join(struct.pack("<QII",i,0,1) for i in range(4)))
            header=b"KBOUT001"+struct.pack("<QQQQ",4,1,2,24)
            correct=header+b"".join(struct.pack("<Qd",i,0.375) for i in range(4))
            mutations=[correct[:-1],correct+b"x",
                       header+struct.pack("<Qd",0,float("nan"))+correct[56:],
                       header+struct.pack("<Qd",9,0.375)+correct[56:],
                       header+struct.pack("<Qd",0,1.375)+correct[56:]]
            for payload in mutations:
                output.write_bytes(payload)
                with self.assertRaises((ValueError,AssertionError)):
                    verify_complete_packed_output(source,output,Fraction(1,10000))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, nargs="?")
    parser.add_argument("output", type=Path, nargs="?")
    parser.add_argument("tolerance", type=Fraction, nargs="?", default=Fraction(1, 10000))
    parser.add_argument("--self-test", action="store_true")
    arguments = parser.parse_args()
    if arguments.self_test:
        unittest.main(argv=["independent-oracle"], exit=True)
    if arguments.source is None or arguments.output is None:
        parser.error("source and output are required unless --self-test is selected")
    print(json.dumps(verify_complete_packed_output(arguments.source, arguments.output, arguments.tolerance), sort_keys=True))
