"""Exact query-limited two-row envelopes with paid O(r+e) static records.

Prepared count inputs only. A retained summary is not a physical RAM guarantee;
compilation still reads and temporarily uses O(G) input/count words.
"""

from bisect import bisect_left
from fractions import Fraction
from itertools import groupby
from types import MappingProxyType

from probe_additive_core_similarity import (
    build_additive_orientation_core,
    evaluate_additive_core_records,
    orient_additive_core_labels,
)


def compile_exception_capacity_core(capacities, union_counts, *, query_limit, stats=None):
    if type(query_limit) is not int or query_limit < 0:
        raise ValueError("query limit must be a nonnegative builtin integer")
    groups = len(union_counts)
    if not 1 <= groups <= 256 or groups & (groups - 1) or len(capacities) != 2 * groups:
        raise ValueError("invalid feature tree dimensions or group budget")
    metrics = {} if stats is None else stats
    core = build_additive_orientation_core(capacities, [0] * groups, union_counts, stats=metrics)
    tops = {node["original_node"]: index for index, node in enumerate(core["nodes"])
            if node["strict_bit"] is None}
    runs, exceptions, stack = [], [], [(1, None)]
    metrics["ownership_node_visits"] = 0
    while stack:
        original, owner = stack.pop()
        owner = tops.get(original, owner)
        metrics["ownership_node_visits"] += 1
        if original < groups:
            stack.extend(((2 * original + 1, owner), (2 * original, owner)))
            continue
        leaf = original - groups
        if runs and runs[-1][2] == owner:
            runs[-1] = runs[-1][0], leaf + 1, owner
        else:
            runs.append((leaf, leaf + 1, owner))
        capacity = capacities[original]
        lower = union_counts[leaf] - capacity
        if lower < query_limit:
            exceptions.append((leaf, lower, min(capacity, query_limit)))
    if len(runs) > 3 * core["strict_nodes"] + 1:
        raise AssertionError("owner interval bound violated")
    core["nodes"] = tuple(MappingProxyType(node) for node in core["nodes"])
    core.update(groups=groups, query_limit=query_limit, owner_runs=tuple(runs),
                exceptions=tuple(exceptions), exception_keys=tuple(leaf for leaf, _, _ in exceptions))
    metrics.update(owner_run_records=len(runs), exception_leaf_records=len(exceptions),
                   exception_index_records=len(exceptions), static_leaf_records=0)
    return MappingProxyType(core)


def compile_modal_capacity_core(capacities, union_counts, *, query_limit, stats=None):
    metrics = {} if stats is None else stats
    core = dict(compile_exception_capacity_core(capacities, union_counts, query_limit=query_limit, stats=metrics))
    groups, runs = core["groups"], core["owner_runs"]
    records, run_index = [], 0
    for leaf, population in enumerate(union_counts):
        while leaf >= runs[run_index][1]:
            run_index += 1
        capacity = capacities[groups + leaf]
        records.append((runs[run_index][2], min(population - capacity, query_limit),
                        min(capacity, query_limit)))
    records.sort()
    defaults, largest = [None] * len(core["nodes"]), [0] * len(core["nodes"])
    distinct = 0
    for (owner, lower, capacity), repeated in groupby(records):
        count = sum(1 for _ in repeated)
        distinct += 1
        if count > largest[owner]:
            defaults[owner], largest[owner] = (lower, capacity), count
    exceptions, run_index = [], 0
    for leaf, population in enumerate(union_counts):
        while leaf >= runs[run_index][1]:
            run_index += 1
        capacity = capacities[groups + leaf]
        signature = min(population - capacity, query_limit), min(capacity, query_limit)
        if signature != defaults[runs[run_index][2]]:
            exceptions.append((leaf, *signature))
    metrics.update(fixed_exception_records=len(core["exceptions"]),
                   modal_signature_records_peak=groups, modal_distinct_signatures=distinct,
                   modal_default_slots=len(defaults), exception_leaf_records=len(exceptions),
                   exception_index_records=len(exceptions))
    core.update(component_defaults=tuple(defaults), exceptions=tuple(exceptions),
                exception_keys=tuple(leaf for leaf, _, _ in exceptions))
    return MappingProxyType(core)


def prepare_exception_query_core(static_core, a, leaf_queries, *, stats=None):
    if type(a) is not int or a < 0:
        raise ValueError("query size must be a nonnegative builtin integer")
    metrics = {} if stats is None else stats
    metrics.update(query_core_copies=0, query_owner_lookups=0, query_exception_hits=0,
                   owner_intervals_crossed=0, query_union_size=0, sparse_pairs_read=0)
    try:
        iterator = iter(leaf_queries)
    except TypeError as error:
        raise ValueError("sparse query must be an iterable of pairs") from error
    nodes = tuple(dict(node) for node in static_core["nodes"])
    metrics["query_core_copies"] = len(nodes)
    previous, total, run_index = -1, 0, 0
    runs, keys = static_core["owner_runs"], static_core["exception_keys"]
    defaults = static_core.get("component_defaults")
    for pair in iterator:
        metrics["sparse_pairs_read"] += 1
        if not isinstance(pair, (tuple, list)) or len(pair) != 2:
            raise ValueError("sparse query entries must be two-item list/tuple pairs")
        leaf, query = pair
        if type(leaf) is not int or not 0 <= leaf < static_core["groups"]:
            raise ValueError("invalid leaf index")
        if leaf <= previous:
            raise ValueError("leaf indices must be sorted and unique")
        if type(query) is not int or query <= 0:
            raise ValueError("sparse counts must be positive builtin integers")
        if query > static_core["query_limit"]:
            raise ValueError("query limit exceeded; use a wider prepared profile")
        total += query
        if total > a:
            raise ValueError("query union size exceeds query size")
        while leaf >= runs[run_index][1]:
            run_index += 1
            metrics["owner_intervals_crossed"] += 1
        owner = runs[run_index][2]
        metrics["query_owner_lookups"] += 1
        index = bisect_left(keys, leaf)
        signature = defaults[owner] if defaults is not None else None
        if index < len(keys) and keys[index] == leaf:
            _, *signature = static_core["exceptions"][index]
            metrics["query_exception_hits"] += 1
        low_reward = high_reward = query
        if signature is not None:
            lower, capacity = signature
            if query > lower + capacity:
                raise ValueError("query count exceeds leaf union population")
            low_reward, high_reward = min(query, lower), min(query, capacity)
        lo, hi, intercept, ceiling = nodes[owner]["bundle"]
        nodes[owner]["bundle"] = lo, hi, intercept - low_reward, ceiling + high_reward
        previous = leaf
        metrics["query_union_size"] = total
    return {"nodes": nodes, "root": static_core["root"], "strict_nodes": static_core["strict_nodes"],
            "component_nodes": static_core["component_nodes"], "union_size": static_core["union_size"],
            "major_size": static_core["major_size"], "query_union_size": total}


def compute_exception_core_bound(static_core, a, minimum, leaf_queries, *,
                                 branch_cap=1000000, work_cap=10000000, stats=None):
    metrics = {} if stats is None else stats
    metrics.update(branches_visited=0, branch_work=0, feasible_branches=0,
                   query_core_copies=0, query_owner_lookups=0, query_exception_hits=0,
                   owner_intervals_crossed=0, query_union_size=0, sparse_pairs_read=0,
                   row_record_slots_peak=0, reserved_branches=0, reserved_branch_work=0)
    if any(type(value) is not int or value < 0 for value in (a, minimum, branch_cap, work_cap)):
        raise ValueError("sizes and budgets must be nonnegative builtin integers")
    major_size = static_core["major_size"]
    if minimum > major_size or minimum + major_size < static_core["union_size"]:
        raise ValueError("inconsistent row sizes")
    size, strict = len(static_core["nodes"]), static_core["strict_nodes"]
    branches = 1 << strict
    per_branch = 5 * size - 2 + 2 * static_core["component_nodes"]
    metrics.update(reserved_branches=branches, reserved_branch_work=branches * per_branch)
    if branches > branch_cap or branches * per_branch > work_cap:
        raise ValueError("branch or work budget exceeded before enumeration")
    core = prepare_exception_query_core(static_core, a, leaf_queries, stats=metrics)
    metrics["row_record_slots_peak"] = size
    best = None
    for assignment in range(branches):
        metrics["branches_visited"] += 1
        labels = orient_additive_core_labels(core, assignment, stats=metrics)
        major = evaluate_additive_core_records(core, labels, 0, stats=metrics)[core["root"]]
        minor = evaluate_additive_core_records(core, labels, 1, stats=metrics)[core["root"]]
        if (major is None or minor is None or not major[0] <= major_size <= major[1]
                or not minor[0] <= minimum <= minor[1]):
            continue
        metrics["feasible_branches"] += 1
        for record, row_size in ((major, major_size), (minor, minimum)):
            overlap = min(row_size - record[2], record[3])
            score = Fraction(overlap, a + row_size - overlap) if overlap else Fraction()
            best = score if best is None else max(best, score)
    if best is None:
        raise ValueError("no complete pair realizes the metadata")
    return best
