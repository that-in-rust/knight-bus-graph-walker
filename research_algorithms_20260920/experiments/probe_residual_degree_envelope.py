"""Logical exact-rational score bounds, not a streaming or RAM-cap implementation."""

from dataclasses import dataclass
from fractions import Fraction


@dataclass(frozen=True)
class BoundedQuotientResidualSummary:
    original_degrees: tuple
    residual_degrees: tuple
    retained: dict
    loop_mass: Fraction
    input_records: int
    peak_counters: int


def build_bounded_quotient_summary(node_count, edge_stream, counter_budget):
    """Weighted Misra-Gries control plus exact degree accounting; consumes once."""
    if not isinstance(node_count, int) or not isinstance(counter_budget, int):
        raise ValueError("integer node count and counter budget required")
    if node_count < 0 or counter_budget < 0:
        raise ValueError("negative node count or counter budget")
    degrees = [Fraction() for _ in range(node_count)]
    residual = [Fraction() for _ in range(node_count)]
    counters = {}
    loop_mass = Fraction()
    records = peak = 0
    for left, right, raw_weight in edge_stream:
        if not isinstance(left, int) or not isinstance(right, int) or not (0 <= left < node_count and 0 <= right < node_count):
            raise ValueError("edge endpoint outside declared nodes")
        weight = Fraction(raw_weight)
        if weight < 0:
            raise ValueError("negative edge weight")
        records += 1
        degrees[left] += weight
        degrees[right] += weight
        if left == right:
            loop_mass += weight
            continue
        residual[left] += weight
        residual[right] += weight
        if not weight or counter_budget == 0:
            continue
        key = (min(left, right), max(left, right))
        if key in counters:
            counters[key] += weight
        elif len(counters) < counter_budget:
            counters[key] = weight
        else:
            # Standard weighted frequent-item cancellation, not a new sketch.
            decrement = min(weight, min(counters.values()))
            for previous in tuple(counters):
                counters[previous] -= decrement
                if not counters[previous]:
                    del counters[previous]
            weight -= decrement
            if weight:
                counters[key] = weight
        peak = max(peak, len(counters))
    for (left, right), weight in counters.items():
        residual[left] -= weight
        residual[right] -= weight
    return BoundedQuotientResidualSummary(tuple(degrees), tuple(residual), counters, loop_mass, records, peak)


def score_bounded_quotient_partition(summary, labels, gamma):
    if len(labels) != len(summary.original_degrees):
        raise ValueError("one label per coarse node required")
    known = summary.loop_mass + sum(
        (weight for (left, right), weight in summary.retained.items() if labels[left] == labels[right]),
        Fraction(),
    )
    return bound_original_partition_score(summary.original_degrees, summary.residual_degrees, known, labels, gamma)


def bound_residual_internal_mass(residual_degrees, labels):
    """Bounds for any loopless weighted completion of the residual degrees."""
    degrees = tuple(Fraction(value) for value in residual_degrees)
    if len(degrees) != len(labels) or any(value < 0 for value in degrees):
        raise ValueError("nonnegative degrees and one label per node required")
    mass = sum(degrees, Fraction()) / 2
    if max(degrees, default=0) > mass:
        raise ValueError("degrees cannot realize a loopless weighted graph")
    groups = {}
    for degree, label in zip(degrees, labels):
        total, largest = groups.get(label, (Fraction(), Fraction()))
        groups[label] = (total + degree, max(largest, degree))
    forced = tuple(max(Fraction(), 2 * largest - total) for total, largest in groups.values())
    lower = max(Fraction(), max((total for total, _ in groups.values()), default=0) - mass)
    upper = mass - max(max(forced, default=0), sum(forced, Fraction()) / 2)
    return lower, upper


def bound_original_partition_score(original_degrees, residual_degrees, retained_internal_mass, labels, gamma):
    """Caller certifies that original = retained + loopless residual graph."""
    degrees = tuple(Fraction(value) for value in original_degrees)
    residual = tuple(Fraction(value) for value in residual_degrees)
    known = Fraction(retained_internal_mass)
    resolution = Fraction(gamma)
    if len(degrees) != len(labels) or len(residual) != len(degrees):
        raise ValueError("degree and label lengths differ")
    if any(d < r or r < 0 for d, r in zip(degrees, residual)) or resolution <= 0:
        raise ValueError("invalid residual degrees or resolution")
    mass = sum(degrees, Fraction()) / 2
    retained_mass = mass - sum(residual, Fraction()) / 2
    if mass <= 0 or known < 0 or known > retained_mass:
        raise ValueError("positive original mass and feasible retained mass required")
    volumes = {}
    for degree, label in zip(degrees, labels):
        volumes[label] = volumes.get(label, Fraction()) + degree
    penalty = resolution * sum(value * value for value in volumes.values()) / (4 * mass * mass)
    lower, upper = bound_residual_internal_mass(residual, labels)
    return (known + lower) / mass - penalty, (known + upper) / mass - penalty
