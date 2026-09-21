"""Export the frozen probe's actual CG state, without publication/certification.

The caller must first validate exact degrees/factor cardinalities and keep the
source immutable through solve and publication. The six-array payload budget
does not bound row decoding, Python objects, allocator overhead, or whole RAM.
"""

from array import array
from contextlib import closing
import hashlib
import math
from pathlib import Path
import time
import traceback

import probe_native_incidence_pagerank as base


def solve_incidence_factor_state(path, source, epsilon, maximum_iterations=20000,
                                 maximum_factor_bytes=2**30):
    """Return (owned array('d') h, scalar metrics), never a score-derived state.

    Success means only that the inherited epsilon/8 *uncertified* recursive
    residual screen passed. The final publisher must certify its actual bytes.
    maximum_factor_bytes admits 6*8*F logical array payload bytes before any
    factor allocation. No files are created or modified by this function.
    """
    started = time.perf_counter()
    if type(epsilon) not in (int, float):
        raise ValueError("invalid finite positive epsilon")
    try:
        epsilon = float(epsilon)
    except OverflowError as error:
        raise ValueError("invalid finite positive epsilon") from error
    if not math.isfinite(epsilon) or epsilon <= 0:
        raise ValueError("invalid finite positive epsilon")
    if type(maximum_iterations) is not int or maximum_iterations < 0:
        raise ValueError("invalid nonnegative iteration budget")
    if type(maximum_factor_bytes) is not int or maximum_factor_bytes < 0:
        raise ValueError("invalid nonnegative factor byte budget")
    n, f, z = base.read_incidence_store_header(path)
    if source is not None and (type(source) is not int or not 0 <= source < n):
        raise ValueError("invalid personalization source")
    selected_peak = 6*8*f
    if selected_peak > maximum_factor_bytes:
        raise ValueError("factor array payload budget exceeded before allocation")
    store_bytes = Path(path).stat().st_size

    h = None
    try:
        # Only h escapes this fresh frame; counts/RHS/Krylov arrays die first.
        h, metrics = _run_incidence_cg_state(path, source, epsilon, maximum_iterations, n, f, z)
        digest = hashlib.sha256()
        for value in h:
            if not math.isfinite(value):
                raise ValueError("CG produced nonfinite factor state")
            digest.update(base.DOUBLE.pack(value))
        metrics.update(state_sha256=digest.hexdigest(), state_bytes=8*f,
                       selected_array_phase_peak_bytes=selected_peak,
                       row_scans=metrics["noncertificate_row_scans"],
                       logical_read_bytes=base.HEADER.size+metrics["noncertificate_row_scans"]*store_bytes,
                       total_ms=1000*(time.perf_counter()-started))
        return h, metrics
    except BaseException as error:
        # A retained refusal/cancellation traceback must not own work arrays.
        traceback.clear_frames(error.__traceback__)
        raise
    finally:
        h = None


def _run_incidence_cg_state(path, source, epsilon, maximum_iterations, n, f, z):
    scans = 0

    def scan_prepared_entity_rows():
        nonlocal scans
        scans += 1
        return closing(base.iter_incidence_row_records(path))

    def apply_reduced_symmetric_operator(vector):
        answer = array("d", vector)
        with scan_prepared_entity_rows() as rows:
            for degree, ids in rows:
                if degree:
                    value = base.ALPHA*math.fsum(vector[j] for j in ids)/(degree+base.ALPHA*len(ids))
                    for j in ids:
                        answer[j] -= value
        return answer

    h = base.create_zero_float_vector(f)
    counts = base.create_zero_float_vector(f)
    rhs = base.create_zero_float_vector(f)
    isolate_count, source_isolate = 0, False
    with scan_prepared_entity_rows() as rows:
        for i, (degree, ids) in enumerate(rows):
            if not degree:
                isolate_count += 1
                source_isolate |= i == source
            else:
                p = base.select_personalization_node_value(i, n, source)
                inverse_k = 1/(degree+base.ALPHA*len(ids))
                for factor in ids:
                    h[factor] += p/degree
                    counts[factor] += 1
                    rhs[factor] += p*inverse_k
    pz = isolate_count/n if source is None else float(source_isolate)
    bscale = (1-base.ALPHA)/(1-base.ALPHA*pz)
    for j in range(f):
        rhs[j] *= bscale

    product = apply_reduced_symmetric_operator(h)
    # Exact-sized storage avoids generator-constructor capacity growth; same ops.
    residual = base.create_zero_float_vector(f)
    for j in range(f):
        residual[j] = rhs[j]-product[j]
    del product
    direction = array("d", residual)
    squared = math.fsum(value*value for value in residual)
    screen_target = epsilon/8
    for iterations in range(maximum_iterations+1):
        screen = base.ALPHA/(1-base.ALPHA)*math.fsum(counts[j]*abs(residual[j]) for j in range(f))
        if not math.isfinite(screen) or not math.isfinite(squared):
            raise ValueError("CG produced nonfinite residual screen or norm")
        if screen <= screen_target:
            break
        # Unlike the frozen failure path, never take an unscreened extra step.
        if iterations == maximum_iterations:
            raise ValueError("CG iteration budget exhausted")
        if squared <= 0:
            raise ValueError("CG residual norm underflow or breakdown")
        product = apply_reduced_symmetric_operator(direction)
        denominator = math.fsum(direction[j]*product[j] for j in range(f))
        if denominator <= 0 or not math.isfinite(denominator):
            raise ValueError("CG lost positive curvature")
        step = squared/denominator
        if not math.isfinite(step):
            raise ValueError("CG produced nonfinite step")
        for j in range(f):
            h[j] += step*direction[j]
            residual[j] -= step*product[j]
        next_squared = math.fsum(value*value for value in residual)
        if not math.isfinite(next_squared):
            raise ValueError("CG produced nonfinite residual norm")
        for j in range(f):
            direction[j] = residual[j]+next_squared/squared*direction[j]
        squared = next_squared
        del product
    else:
        raise ValueError("CG iteration budget exhausted")
    return h, {
        "method": "cg", "source_zero_based": source, "vertices": n,
        "factors": f, "memberships": z, "epsilon": epsilon,
        "iterations_or_checks": iterations, "screen_bound_uncertified": screen,
        "screen_target_uncertified": screen_target,
        "isolate_count": isolate_count, "source_isolate": source_isolate, "bscale": bscale,
        "noncertificate_row_scans": scans,
        "noncertificate_membership_visits": scans*z,
        "operator_membership_visits": 2*(scans-1)*z,
    }
