"""Exact finite coordinate-refinement probe, not a bounded native executor."""

from collections import defaultdict

from probe_masked_matching_triangles import (
    count_full_shift_triangles,
    enumerate_oriented_shift_pairs,
    require_integer_scalar_values,
    shift_mask_interval_runs,
    validate_mask_interval_runs,
)


def compile_periodic_refined_classes(bases, period, classes, refinement, mask_periods=None):
    require_integer_scalar_values(bases, period, refinement)
    if bases < 1 or period < 1 or refinement < 1 or period % refinement:
        raise ValueError("refinement must be a positive divisor of the period")
    if mask_periods is not None and mask_periods.keys() != classes.keys():
        raise ValueError("source periods must match the edge-class keys exactly")
    normalized = {}
    for (a, b, delta), intervals in classes.items():
        require_integer_scalar_values(a, b, delta)
        if not 0 <= a < b < bases or not 0 <= delta < period:
            raise ValueError("invalid canonical edge class")
        source_period = period if mask_periods is None else mask_periods[a, b, delta]
        require_integer_scalar_values(source_period)
        if source_period < 1 or period % source_period:
            raise ValueError("source mask period must divide the graph period")
        runs = validate_mask_interval_runs(intervals, source_period)
        if shift_mask_interval_runs(runs, refinement, source_period) != runs:
            raise ValueError("mask is not invariant under the supplied refinement")
        if runs:
            normalized[a, b, delta] = source_period, runs
    reduced, refined = period // refinement, {}
    for (a, b, delta), (source_period, runs) in normalized.items():
        for offset in range(0, refinement, source_period):
            for left, right in runs:
                for residue in range(offset + left, min(offset + right, refinement)):
                    target, carry = (residue + delta) % refinement, (residue + delta) // refinement
                    key = a * refinement + residue, b * refinement + target, carry % reduced
                    if key in refined:
                        raise AssertionError("coordinate map unexpectedly merged edge classes")
                    refined[key] = None
    edges = sum((period // q) * sum(right - left for left, right in runs)
                for q, runs in normalized.values())
    if reduced * len(refined) != edges:
        raise AssertionError("refined source does not preserve edge cardinality")
    return refined, edges, sum(len(runs) for _, runs in normalized.values())


def compute_refined_triangle_counts(bases, period, classes, refinement, backend="pairs", mask_periods=None):
    if backend not in ("pairs", "convolution"):
        raise ValueError("unknown exact backend")
    refined, edges, source_runs = compile_periodic_refined_classes(bases, period, classes, refinement, mask_periods)
    reduced = period // refinement
    pairs, adjacent = defaultdict(set), defaultdict(set)
    degrees, local = [0] * (bases * refinement), [0] * (bases * refinement)
    for a, b, delta in refined:
        pairs[a, b].add(delta)
        adjacent[a].add(b)
        adjacent[b].add(a)
        degrees[a] += 1
        degrees[b] += 1
    metrics = {"source_runs": source_runs, "refined_classes": len(refined), "refined_vertices": bases * refinement,
               "base_neighbor_probes": 0, "base_triangles": 0, "pair_probes": 0,
               "convolutions": 0, "max_convolution_period": 0}
    total = 0
    for a, b in pairs:
        smaller, other = (adjacent[a], adjacent[b]) if len(adjacent[a]) <= len(adjacent[b]) else (adjacent[b], adjacent[a])
        for c in smaller:
            metrics["base_neighbor_probes"] += 1
            if c <= b or c not in other:
                continue
            metrics["base_triangles"] += 1
            groups = pairs[a, b], pairs[b, c], pairs[a, c]
            if backend == "convolution":
                count = count_full_shift_triangles(*groups, reduced)
                metrics["convolutions"] += 1
                metrics["max_convolution_period"] = reduced
            else:
                work = {"partial_pair_probes": 0}
                count = sum(1 for _ in enumerate_oriented_shift_pairs(groups, reduced, work))
                metrics["pair_probes"] += work["partial_pair_probes"]
            total += reduced * count
            for vertex in (a, b, c):
                local[vertex] += count
    if reduced * sum(local) != 3 * total or reduced * sum(degrees) != 2 * edges:
        raise AssertionError("local/global conservation failure")
    if any(not 0 <= count <= degree * (degree - 1) // 2 for degree, count in zip(degrees, local)):
        raise AssertionError("invalid local count")
    return list(zip(degrees, local)), total, metrics


def iterate_refined_output_rows(bases, period, refinement, residues):
    require_integer_scalar_values(bases, period, refinement)
    if bases < 1 or period < 1 or refinement < 1 or period % refinement:
        raise ValueError("invalid output dimensions")
    if len(residues) != bases * refinement:
        raise ValueError("incorrect residue row count")
    for base in range(bases):
        for coordinate in range(period):
            yield residues[base * refinement + coordinate % refinement]
