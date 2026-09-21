"""Evaluate unchanged cheap controls on trusted sparse-prepared modal cores."""

from fractions import Fraction

from probe_exception_core_similarity import prepare_exception_query_core


def validate_compact_control_inputs(core, a, minimum, record_cap, metrics):
    metrics.update(control_nodes_visited=0, control_child_reductions=0,
                   control_records_peak=0, control_record_slots_reserved=len(core["nodes"]))
    if any(type(value) is not int or value < 0 for value in (a, minimum, record_cap)):
        raise ValueError("sizes and record cap must be nonnegative builtin integers")
    if minimum > core["major_size"] or minimum + core["major_size"] < core["union_size"]:
        raise ValueError("inconsistent row sizes")
    if len(core["nodes"]) > record_cap:
        raise ValueError("control record budget exceeded before query consumption")


def combine_control_record_group(records):
    lower = upper = low_reward = high_reward = population = rank = 0
    for lo, hi, debt, ceiling, union, reward in records:
        lower += lo
        upper += hi
        low_reward += min(lo - debt, ceiling)
        high_reward += min(hi - debt, ceiling)
        population += union
        rank += reward
    return lower, upper, lower - low_reward, high_reward, population, rank


def evaluate_prepared_interval_bounds(core, a, minimum, *, record_cap=1000000, stats=None):
    metrics = {} if stats is None else stats
    validate_compact_control_inputs(core, a, minimum, record_cap, metrics)
    total = core["query_union_size"]
    if type(total) is not int or not 0 <= total <= min(a, core["union_size"]):
        raise ValueError("invalid prepared query union size")
    states = []
    for node in core["nodes"]:
        children = node["children"]
        if node["strict_bit"] is None:
            lo, hi, debt, ceiling = node["bundle"]
            bundle = lo, hi, debt, ceiling, lo + hi, ceiling
            # Additive regions impose no constraint beyond their boundary pieces.
            record = combine_control_record_group(
                (bundle if child is None else states[child] for child in (None, *children)))
        else:
            record = combine_control_record_group(states[child] for child in children)
            lo, hi, debt, ceiling, population, rank = record
            capacity = node["capacity"]
            record = max(lo, population - capacity), min(hi, capacity), debt, ceiling, population, min(rank, capacity)
        if record[0] > record[1]:
            raise ValueError("inconsistent interval certificate")
        states.append(record)
        metrics["control_nodes_visited"] += 1
        metrics["control_child_reductions"] += len(children)
        metrics["control_records_peak"] = len(states)
    lo, hi, debt, ceiling, _, rank = states[core["root"]]
    scores = []
    for size in (minimum, core["major_size"]):
        if max(lo, minimum) <= size <= hi:
            overlap = min(size - debt, ceiling)
            scores.append(Fraction(overlap, a + size - overlap) if overlap else Fraction())
    if not scores:
        raise ValueError("inconsistent interval root sizes")
    union_rank = min(total, core["major_size"])
    return {
        "union": Fraction(union_rank, a + max(minimum, union_rank) - union_rank) if union_rank else Fraction(),
        "laminar": Fraction(rank, a + max(minimum, rank) - rank) if rank else Fraction(),
        "interval": max(scores),
        "interval_record": (lo, hi, debt, ceiling),
        "laminar_rank": rank,
    }


def compute_compressed_interval_bounds(static_core, a, minimum, leaf_queries, *,
                                       record_cap=1000000, stats=None):
    metrics = {} if stats is None else stats
    metrics["query_core_copies"] = 0
    validate_compact_control_inputs(static_core, a, minimum, record_cap, metrics)
    core = prepare_exception_query_core(static_core, a, leaf_queries, stats=metrics)
    return evaluate_prepared_interval_bounds(core, a, minimum, record_cap=record_cap, stats=metrics)
