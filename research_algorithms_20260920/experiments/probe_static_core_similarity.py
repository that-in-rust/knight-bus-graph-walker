"""Count-only A05 queries with paid O(G) immutable, memory-resident leaf storage.

Static compilation costs O(G); each sparse query costs O(s + (r+1)*2**r)
arithmetic and O(r+1) EXTRA words. These are not physical RAM or graph-engine
claims. Only compiler-produced static objects are supported, not serialized or
caller-forged cores. Logical branch budgets do not bound input preparation.
"""

from fractions import Fraction
from types import MappingProxyType

from probe_additive_core_similarity import (
    build_additive_orientation_core,
    evaluate_additive_core_records,
    orient_additive_core_labels,
)


def compile_static_capacity_core(capacities, union_counts, *, stats=None):
    """Validate supplied counts once and own immutable core/leaf-table records."""
    groups = len(union_counts)
    if not 1 <= groups <= 256 or groups & (groups - 1) or len(capacities) != 2 * groups:
        raise ValueError("invalid feature tree dimensions or group budget")
    metrics = stats if stats is not None else {}
    core = build_additive_orientation_core(capacities, [0] * groups, union_counts, stats=metrics)
    component_tops = {node["original_node"]: index for index, node in enumerate(core["nodes"])
                      if node["strict_bit"] is None}
    leaf_table = [None] * groups
    stack = [(1, None)]
    metrics["ownership_node_visits"] = 0
    while stack:
        original, owner = stack.pop()
        metrics["ownership_node_visits"] += 1
        owner = component_tops.get(original, owner)
        if original < groups:
            stack.extend(((2 * original + 1, owner), (2 * original, owner)))
        else:
            leaf = original - groups
            capacity = capacities[original]
            leaf_table[leaf] = (owner, union_counts[leaf] - capacity, capacity)
    core["nodes"] = tuple(MappingProxyType(node) for node in core["nodes"])
    core.update(groups=groups, leaf_table=tuple(leaf_table))
    metrics["static_leaf_records"] = groups
    return MappingProxyType(core)


def prepare_static_query_core(static_core, a, leaf_queries, *, stats=None):
    """Consume sorted unique (zero-based leaf_id, positive q) list/tuple pairs.

    The outer iterable is single-pass. This helper only prepares rewards; the
    bound evaluator additionally validates row sizes and admits branch work.
    """
    if type(a) is not int or a < 0:
        raise ValueError("query size must be a nonnegative integer")
    metrics = stats if stats is not None else {}
    metrics.update(query_core_copies=0, query_leaf_lookups=0, sparse_pairs_read=0, query_union_size=0)
    try:
        iterator = iter(leaf_queries)
    except TypeError as error:
        raise ValueError("sparse query must be an iterable of pairs") from error
    nodes = tuple(dict(node) for node in static_core["nodes"])
    metrics["query_core_copies"] = len(nodes)
    previous, total = -1, 0
    # Do not enumerate, copy, slice, or take len() of the paid indexed table.
    while True:
        try:
            pair = next(iterator)
        except StopIteration:
            break
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
        owner, lower, capacity = static_core["leaf_table"][leaf]
        metrics["query_leaf_lookups"] += 1
        if query > lower + capacity:
            raise ValueError("query count exceeds leaf union population")
        total += query
        if total > a:
            raise ValueError("query union size exceeds query size")
        lo, hi, intercept, ceiling = nodes[owner]["bundle"]
        nodes[owner]["bundle"] = (lo, hi, intercept - min(query, lower),
                                  ceiling + min(query, capacity))
        previous = leaf
        metrics["query_union_size"] = total
    return {"nodes": nodes, "root": static_core["root"], "strict_nodes": static_core["strict_nodes"],
            "component_nodes": static_core["component_nodes"], "union_size": static_core["union_size"],
            "major_size": static_core["major_size"], "query_union_size": total}


def compute_static_core_bound(static_core, a, minimum, leaf_queries, *,
                              branch_cap=1000000, work_cap=10000000, stats=None):
    """Return the exact either-row envelope, or refuse without an unsafe fallback.

    Budgets cover the same reserved orientation/row work as the current core,
    not compilation or streaming. A budget refusal leaves the stream untouched.
    """
    metrics = stats if stats is not None else {}
    metrics.update(branches_visited=0, branch_work=0, feasible_branches=0,
                   query_core_copies=0, query_leaf_lookups=0, sparse_pairs_read=0,
                   query_union_size=0, row_record_slots_peak=0,
                   reserved_branches=0, reserved_branch_work=0)
    if any(type(value) is not int or value < 0 for value in (a, minimum, branch_cap, work_cap)):
        raise ValueError("sizes and budgets must be nonnegative builtin integers")
    major_size = static_core["major_size"]
    if minimum > major_size or minimum + major_size < static_core["union_size"]:
        raise ValueError("inconsistent row sizes")
    size, strict = len(static_core["nodes"]), static_core["strict_nodes"]
    branches = 1 << strict
    per_branch = 3 * size + 2 * (size - 1) + 2 * static_core["component_nodes"]
    metrics.update(strict_nodes=strict, core_nodes=size,
                   component_nodes=static_core["component_nodes"], reserved_branches=branches,
                   reserved_branch_work=branches * per_branch)
    if branches > branch_cap:
        raise ValueError("branch budget exceeded before enumeration")
    if metrics["reserved_branch_work"] > work_cap:
        raise ValueError("work budget exceeded before enumeration")
    core = prepare_static_query_core(static_core, a, leaf_queries, stats=metrics)
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
