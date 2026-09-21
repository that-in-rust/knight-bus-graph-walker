"""Experimental Boolean two-membership PageRank, without dense matrices.

Prepared metadata is trusted. CG residuals are numerical stopping screens;
only a separate original-operator actual-output certificate admits a result.
"""

from array import array
from contextlib import contextmanager
from fractions import Fraction
import math


@contextmanager
def manage_source_iterator_lifetime(records):
    try:
        yield records
    finally:
        close = getattr(records, "close", None)
        if close is not None:
            close()


def allocate_zero_rank_vector(count):
    return array("d", [0.0]) * count


def accumulate_compensated_rank_value(values, corrections, index, value):
    adjusted = value - corrections[index]
    combined = values[index] + adjusted
    corrections[index] = (combined - values[index]) - adjusted
    values[index] = combined


def calculate_scaled_vector_norm(values):
    scale = max((abs(value) for value in values), default=0.0)
    if not math.isfinite(scale):
        raise ArithmeticError("nonfinite numerical vector")
    return 0.0 if not scale else scale * math.sqrt(math.fsum((value / scale)**2 for value in values))


def solve_preconditioned_conjugate_gradient(apply, rhs, diagonal, tolerance, maximum_iterations):
    dimension = len(rhs)
    if any(not math.isfinite(value) or value <= 0 for value in diagonal):
        raise ArithmeticError("invalid numerical preconditioner")
    x, residual = allocate_zero_rank_vector(dimension), array("d", rhs)
    norm_rhs = calculate_scaled_vector_norm(rhs)
    if not norm_rhs:
        return x, dict(iterations=0, converged=True, recomputed_relative_residual=0.0)
    z = array("d", (value / scale for value, scale in zip(residual, diagonal)))
    direction = array("d", z)
    rz = math.fsum(value * other for value, other in zip(residual, z))
    iterations = 0
    for _ in range(maximum_iterations):
        product = apply(direction)
        curvature = math.fsum(value * other for value, other in zip(direction, product))
        if not math.isfinite(curvature) or curvature <= 0 or not math.isfinite(rz) or rz <= 0:
            raise ArithmeticError("CG numerical breakdown; no result is certified")
        step = rz / curvature
        for i in range(dimension):
            x[i] += step * direction[i]
            residual[i] -= step * product[i]
        iterations += 1
        if calculate_scaled_vector_norm(residual) <= tolerance * norm_rhs:
            break
        z = array("d", (value / scale for value, scale in zip(residual, diagonal)))
        next_rz = math.fsum(value * other for value, other in zip(residual, z))
        beta = next_rz / rz
        for i in range(dimension):
            direction[i] = z[i] + beta * direction[i]
        rz = next_rz
    product = apply(x)
    for i in range(dimension):
        residual[i] = rhs[i] - product[i]
    relative = calculate_scaled_vector_norm(residual) / norm_rhs
    return x, dict(iterations=iterations, converged=relative <= tolerance * 10,
                   recomputed_relative_residual=relative)


def solve_boolean_rank_state(source, *, alpha, method, max_factor_slots,
                             max_class_slots, tolerance=1e-13, maximum_iterations=1000):
    """Return provisional F or P state; reservations are not RAM byte caps."""
    factors, classes, n = source.factor_count, source.active_class_count, source.vertex_count
    for value in (factors, classes, n, max_factor_slots, max_class_slots, maximum_iterations):
        if type(value) is not int or value < 0:
            raise ValueError("invalid count or numerical reservation")
    if method not in ("factor-cg", "class-cg"):
        raise ValueError("unsupported numerical method")
    if type(alpha) is not float or not math.isfinite(alpha) or not 0 <= alpha < 1:
        raise ValueError("alpha must be a binary64 value in [0,1)")
    if not math.isfinite(tolerance) or tolerance <= 0:
        raise ValueError("positive finite stopping tolerance required")
    if factors > max_factor_slots or (method == "class-cg" and classes > max_class_slots):
        raise ValueError("numerical slot reservation exceeded before allocation")
    if n and source.total_weight <= 0:
        raise ValueError("nonempty personalization must have positive total weight")
    snapshot = source.snapshot_id
    exact_alpha = Fraction.from_float(alpha)
    gamma = (1 - exact_alpha) / (1 - exact_alpha * source.isolate_weight / source.total_weight) if n else Fraction(1)
    dimension = factors if method == "factor-cg" else classes
    metrics = dict(dimension=dimension, factor_slots=factors, active_classes=classes,
                   class_passes=0, class_records=0, matrix_applications=0,
                   vector_payload_upper_bytes=8 * (12 * dimension + 6 * factors),
                   payload_scope="conservative packed solver vector payload only, not RSS or certificate state")

    def iterate_validated_class_records():
        metrics["class_passes"] += 1
        active = 0
        with manage_source_iterator_lifetime(source.iterate_class_records()) as records:
            for record in records:
                metrics["class_records"] += 1
                _, groups, h, degree, _ = record
                if type(h) is not int or type(degree) is not int or not 1 <= h <= 2**53 or not 0 <= degree <= 2**53:
                    raise ValueError("class integer cannot be represented exactly")
                if degree:
                    if not 1 <= len(groups) <= 2:
                        raise ValueError("active class requires one or two groups")
                    active += 1
                    yield record
        if active != classes:
            raise ValueError("active class count changed")

    def calculate_class_rhs_value(weight):
        return float(gamma * weight / source.total_weight)

    if not n or alpha == 0.0:
        metrics.update(iterations=0, converged=True, recomputed_relative_residual=0.0)
        return dict(snapshot_id=snapshot, alpha=alpha, gamma=gamma, method=method,
                    factor_scores=allocate_zero_rank_vector(factors),
                    class_scores=allocate_zero_rank_vector(classes) if method == "class-cg" else None,
                    metrics=metrics)

    if method == "factor-cg":
        counts = array("Q", [0]) * factors
        seen = 0
        with manage_source_iterator_lifetime(source.iterate_active_factor_counts()) as records:
            for factor, count in records:
                if factor != seen or type(count) is not int or not 0 <= count <= 2**53:
                    raise ValueError("invalid active group count")
                counts[factor] = count
                seen += 1
        if seen != factors:
            raise ValueError("incomplete active group counts")
        rhs, ground, diagonal = (allocate_zero_rank_vector(factors) for _ in range(3))
        rhs_error, ground_error, diagonal_error = (allocate_zero_rank_vector(factors) for _ in range(3))
        with manage_source_iterator_lifetime(iterate_validated_class_records()) as records:
            for _, groups, h, degree, weight in records:
                t = degree + alpha * (1 + (len(groups) - 1) * h)
                b = calculate_class_rhs_value(weight)
                for factor in groups:
                    accumulate_compensated_rank_value(rhs, rhs_error, factor, counts[factor] * b / t)
                    g = float(1 - exact_alpha) * counts[factor] * h * degree / t
                    accumulate_compensated_rank_value(ground, ground_error, factor, g)
                if len(groups) == 2:
                    left, right = groups
                    edge = alpha * h * counts[left] * counts[right] / t
                    for factor in groups:
                        accumulate_compensated_rank_value(diagonal, diagonal_error, factor, edge)
        for factor in range(factors):
            if counts[factor] == 0:
                ground[factor] = 1.0
            diagonal[factor] += ground[factor]
        del rhs_error, ground_error, diagonal_error

        def apply_grounded_factor_operator(values):
            metrics["matrix_applications"] += 1
            result = array("d", (g * value for g, value in zip(ground, values)))
            correction = allocate_zero_rank_vector(factors)
            with manage_source_iterator_lifetime(iterate_validated_class_records()) as records:
                for _, groups, h, degree, _ in records:
                    if len(groups) == 2:
                        left, right = groups
                        t = degree + alpha * (1 + h)
                        edge = alpha * h * counts[left] * counts[right] / t
                        difference = edge * (values[left] - values[right])
                        accumulate_compensated_rank_value(result, correction, left, difference)
                        accumulate_compensated_rank_value(result, correction, right, -difference)
            return result

        solution, numeric = solve_preconditioned_conjugate_gradient(
            apply_grounded_factor_operator, rhs, diagonal, tolerance, maximum_iterations)
        factor_scores = array("d", (count * value for count, value in zip(counts, solution)))
        class_scores = None
    else:
        rhs, diagonal = allocate_zero_rank_vector(classes), allocate_zero_rank_vector(classes)
        with manage_source_iterator_lifetime(iterate_validated_class_records()) as records:
            for i, (_, _, h, degree, weight) in enumerate(records):
                rhs[i] = calculate_class_rhs_value(weight)
                diagonal[i] = h * (float(1 - exact_alpha) * (h - 1) + (degree - h + 1))

        def gather_class_factor_scores(values):
            totals, correction = allocate_zero_rank_vector(factors), allocate_zero_rank_vector(factors)
            with manage_source_iterator_lifetime(iterate_validated_class_records()) as records:
                for i, (_, groups, h, _, _) in enumerate(records):
                    for factor in groups:
                        accumulate_compensated_rank_value(totals, correction, factor, h * values[i])
            return totals

        def apply_symmetric_class_operator(values):
            metrics["matrix_applications"] += 1
            centers, totals, correction = (allocate_zero_rank_vector(factors) for _ in range(3))
            counts = array("Q", [0]) * factors
            # Center differences before summation: a constant vector must leave
            # only the small positive ground, even when alpha is almost one.
            with manage_source_iterator_lifetime(iterate_validated_class_records()) as records:
                for i, (_, groups, h, _, _) in enumerate(records):
                    for factor in groups:
                        if counts[factor] == 0:
                            centers[factor] = values[i]
                        counts[factor] += h
                        if counts[factor] > 2**53:
                            raise ValueError("active group count cannot be represented exactly")
                        accumulate_compensated_rank_value(totals, correction, factor,
                                                          h * (values[i] - centers[factor]))
            result = allocate_zero_rank_vector(classes)
            with manage_source_iterator_lifetime(iterate_validated_class_records()) as records:
                for i, (_, groups, h, degree, _) in enumerate(records):
                    differences = math.fsum(counts[f] * (values[i] - centers[f]) - totals[f] for f in groups)
                    result[i] = float(1 - exact_alpha) * h * degree * values[i] + alpha * h * differences
            return result

        class_scores, numeric = solve_preconditioned_conjugate_gradient(
            apply_symmetric_class_operator, rhs, diagonal, tolerance, maximum_iterations)
        factor_scores = gather_class_factor_scores(class_scores)
    metrics.update(numeric)
    return dict(snapshot_id=snapshot, alpha=alpha, gamma=gamma, method=method,
                factor_scores=factor_scores, class_scores=class_scores, metrics=metrics)


def iterate_boolean_rank_output(source, state):
    """Stable local lift, in source class/ID order; still needs certification."""
    if source.snapshot_id != state["snapshot_id"]:
        raise ValueError("solver state belongs to a different snapshot")
    alpha, gamma = state["alpha"], state["gamma"]
    active, rows = 0, 0
    with manage_source_iterator_lifetime(source.iterate_class_records()) as records:
        for class_id, groups, h, degree, weight in records:
            neighbor_sum = 0.0
            if degree and alpha:
                group_sum = math.fsum(state["factor_scores"][factor] for factor in groups)
                if state["class_scores"] is None:
                    b = float(gamma * weight / source.total_weight)
                    total = (b + alpha * h * group_sum) / (degree + alpha * (1 + (len(groups) - 1) * h))
                else:
                    total = h * state["class_scores"][active]
                neighbor_sum = group_sum - (len(groups) - 1) * total
            with manage_source_iterator_lifetime(source.iterate_class_vertex_rows(class_id)) as vertices:
                for original_id, original_weight in vertices:
                    b = float(gamma * Fraction.from_float(original_weight) / source.total_weight)
                    value = degree * ((b + alpha * neighbor_sum) / (degree + alpha)) if degree and alpha else b
                    if not math.isfinite(value) or value < 0:
                        raise ValueError("nonfinite or negative provisional PageRank output")
                    rows += 1
                    yield original_id, value
            active += degree > 0
    if rows != source.vertex_count or active != source.active_class_count:
        raise ValueError("output universe changed")
