"""Exact arithmetic checks for the proposed C6/C5 output-plane formats."""

from fractions import Fraction
from itertools import product
from math import isqrt
from random import Random
import json

from verify_embedding_packed_results import enclose_exact_normalized_row


def enclose_fixed_integer_row(row):
    square = sum(value * value for value in row)
    if not square:
        return [(0, 0)] * len(row)
    lower = isqrt(square << 56)
    upper = lower + 1
    result = []
    for value in row:
        numerator = value << 56
        low_den, high_den = (upper, lower) if value >= 0 else (lower, upper)
        result.append((numerator // low_den, -((-numerator) // high_den)))
    return result


def choose_center_record_width(depth):
    magnitude = (1 << 29) * sum(2 if layer % 2 == 0 else 1 for layer in range(depth + 1))
    return (1 + magnitude.bit_length() + 7) // 8


def verify_center_storage_algebra():
    normalizer_rows = normalizer_coordinates = 0
    for dimension in range(1, 5):
        for values in product(range(-2, 3), repeat=dimension):
            for scale in (0, 1, 2, 7, 31, 1 << 24, 1 << 30):
                row = [value * scale for value in values]
                bounds = enclose_fixed_integer_row(row)
                oracle = enclose_exact_normalized_row([Fraction(value) for value in row])
                for (lo, hi), (a, b) in zip(bounds, oracle):
                    assert -(1 << 28) <= lo <= hi <= 1 << 28
                    assert 0 <= hi - lo <= 2
                    assert Fraction(lo, 1 << 28) <= a <= b <= Fraction(hi, 1 << 28)
                    normalizer_coordinates += 1
                normalizer_rows += 1
    random = Random(42)
    traces = coordinates = 0
    for case in range(80):
        nodes = 1 + case % 4
        dimension = 1 + case % 9
        depth = (0, 1, 2, 8, 24)[case % 5]
        widths = [0] * (depth + 1)
        old = [[(0, 0) for _ in range(dimension)] for _ in range(nodes)]
        centers = [[0] * dimension for _ in range(nodes)]
        spreads = [[0] * dimension for _ in range(nodes)]
        real = [[[Fraction(0), Fraction(0)] for _ in range(dimension)] for _ in range(nodes)]
        for layer in range(depth + 1):
            coefficient = 2 if layer % 2 == 0 else -1
            for node in range(nodes):
                row = [random.randrange(-1024, 1025) for _ in range(dimension)]
                if (layer + node) % 7 == 0:
                    row = [0] * dimension
                bounds = enclose_fixed_integer_row(row)
                exact = enclose_exact_normalized_row([Fraction(value) for value in row])
                for j, ((lo, hi), (a, b)) in enumerate(zip(bounds, exact)):
                    widths[layer] = max(widths[layer], hi - lo)
                    low, high = old[node][j]
                    u, v = (lo, hi) if coefficient >= 0 else (hi, lo)
                    old[node][j] = (low + coefficient * u, high + coefficient * v)
                    centers[node][j] += coefficient * (lo + hi)
                    spreads[node][j] += abs(coefficient) * (hi - lo)
                    if coefficient < 0:
                        a, b = b, a
                    real[node][j][0] += Fraction(coefficient, 8) * a
                    real[node][j][1] += Fraction(coefficient, 8) * b
                    low, high = old[node][j]
                    assert centers[node][j] == low + high
                    assert spreads[node][j] == high - low
                    assert (centers[node][j] - spreads[node][j]) % 2 == 0
                    width = choose_center_record_width(depth)
                    packed = centers[node][j].to_bytes(width, "little", signed=True) + bytes([spreads[node][j]])
                    assert int.from_bytes(packed[:-1], "little", signed=True) == low + high
                    assert packed[-1] == high - low
        shared = sum((2 if t % 2 == 0 else 1) * value for t, value in enumerate(widths))
        for node in range(nodes):
            for j in range(dimension):
                center = Fraction(centers[node][j], 1 << 32)
                radius = Fraction(spreads[node][j], 1 << 32)
                low, high = real[node][j]
                assert center - radius <= low <= high <= center + radius
                assert spreads[node][j] <= shared <= 2 * sum(2 if t % 2 == 0 else 1 for t in range(depth + 1))
                assert Fraction.from_float(float(center)) == center
                coordinates += 1
        traces += 1
    for depth in range(65):
        width = choose_center_record_width(depth)
        assert width == (4 if depth <= 1 else 5)
        bound = (1 << 29) * sum(2 if t % 2 == 0 else 1 for t in range(depth + 1))
        for value in (-bound, bound):
            assert int.from_bytes(value.to_bytes(width, "little", signed=True), "little", signed=True) == value
        assert 2 * sum(2 if t % 2 == 0 else 1 for t in range(depth + 1)) <= 196
    # Signed radii are not safe: centers [1,1], true values [0,2], weights [2,-1].
    candidate, target = 2 * 1 - 1 * 1, 2 * 0 - 1 * 2
    assert abs(candidate - target) > 2 * 1 - 1 * 1
    assert abs(candidate - target) <= 2 * 1 + 1 * 1
    assert max(2 * 2 + 1 * 0, 2 * 0 + 1 * 2) == 4 < 6
    nd, depth = 262144 * 64, 8
    old_traffic, old_peak = 6171918728, 476053576
    projections = {}
    for width, traffic, peak in ((6, 3152019848, 308281416), (5, 2850029960, 291504200)):
        assert old_traffic - 2 * (depth + 1) * (16 - width) * nd == traffic
        assert old_peak - (16 - width) * nd == peak
        projections[str(width)] = {"traffic": traffic, "peak_owned_files": peak}
    print(json.dumps({"status": "passed", "normalizer_rows": normalizer_rows,
        "normalizer_coordinates": normalizer_coordinates, "layer_traces": traces,
        "complete_trace_coordinates": coordinates, "depth_width_checks": 65,
        "projection_checks": projections, "negative_examples": 2}, sort_keys=True))


if __name__ == "__main__":
    verify_center_storage_algebra()
