"""Exact two-row envelopes on an additive-region core, not a new flow primitive.

Prepared count summaries only. Logical branch work is admitted explicitly;
neither these counters nor the compact core establish a physical RAM cap.
"""

from fractions import Fraction

from probe_overlap_frontier_similarity import validate_overlap_metadata_inputs


def build_additive_orientation_core(capacities, counts, union_counts, *, stats=None):
    groups, populations = validate_overlap_metadata_inputs(capacities, counts, union_counts)
    strict_indices = {}
    for node in range(1, groups):
        if max(capacities[2*node], capacities[2*node+1]) > capacities[node]:
            raise ValueError("child maximum exceeds parent")
        deficit = capacities[2*node]+capacities[2*node+1]-capacities[node]
        if deficit < 0:
            raise ValueError("parent exceeds sum of child maxima")
        if deficit:
            strict_indices[node] = len(strict_indices)
    metrics = stats if stats is not None else {}
    metrics["preprocessing_node_visits"] = 0
    nodes = []

    def compile_original_subtree_core(top):
        if top in strict_indices:
            metrics["preprocessing_node_visits"] += 1
            children = (compile_original_subtree_core(2*top), compile_original_subtree_core(2*top+1))
            record = {"original_node": top, "capacity": capacities[top], "children": children,
                      "strict_bit": strict_indices[top], "bundle": None}
        else:
            lower = upper = base = ceiling = 0
            children, stack = [], [top]
            while stack:
                node = stack.pop()
                if node in strict_indices:
                    children.append(compile_original_subtree_core(node))
                    continue
                metrics["preprocessing_node_visits"] += 1
                if node < groups:
                    stack.extend((2*node+1, 2*node))
                else:
                    capacity, population, query = capacities[node], populations[node], counts[node-groups]
                    lower += population-capacity
                    upper += capacity
                    base += min(query, population-capacity)
                    ceiling += min(query, capacity)
            record = {"original_node": top, "capacity": capacities[top], "children": tuple(children),
                      "strict_bit": None, "bundle": (lower, upper, lower-base, ceiling)}
        nodes.append(record)
        return len(nodes)-1

    try:
        root = compile_original_subtree_core(1)
    finally:
        # Break the recursive closure's cycle; do not defer input release to GC.
        compile_original_subtree_core = None
    strict, size = len(strict_indices), len(nodes)
    components = size-strict
    metrics.update(strict_nodes=strict, core_nodes=size, component_nodes=components,
                   core_edges=size-1, original_tree_nodes=2*groups-1)
    return {"nodes": tuple(nodes), "root": root, "strict_nodes": strict,
            "component_nodes": components, "union_size": populations[1],
            "query_union_size": sum(counts), "major_size": capacities[1]}


def orient_additive_core_labels(core, assignment, *, stats=None):
    if type(assignment) is not int or not 0 <= assignment < 1 << core["strict_nodes"]:
        raise ValueError("invalid orientation assignment")
    labels = [0]*len(core["nodes"])
    for index in range(len(labels)-1, -1, -1):
        if stats is not None:
            stats["branch_work"] += 1
        node = core["nodes"][index]
        if node["strict_bit"] is None:
            for child in node["children"]:
                labels[child] = labels[index]
        else:
            left, right = node["children"]
            labels[left] = (assignment >> node["strict_bit"]) & 1
            labels[right] = 1-labels[left]
    return labels


def merge_capped_frontier_records(records):
    lower = upper = base = ceiling = 0
    for record in records:
        if record is None:
            return None
        lo, hi, intercept, cap = record
        lower += lo
        upper += hi
        base += min(lo-intercept, cap)
        ceiling += min(hi-intercept, cap)
    return lower, upper, lower-base, ceiling


def evaluate_additive_core_records(core, labels, row, *, stats=None):
    """Conditioned subtree records; core and labels come from the helpers above."""
    if type(row) is not int or row not in (0, 1) or len(labels) != len(core["nodes"]):
        raise ValueError("invalid row or orientation labels")
    table = []
    for index, node in enumerate(core["nodes"]):
        if stats is not None:
            stats["branch_work"] += 1

        def iterate_child_frontier_records():
            if node["bundle"] is not None:
                if stats is not None:
                    stats["branch_work"] += 1
                lo, hi, intercept, ceiling = node["bundle"]
                yield (hi if labels[index] == row else lo, hi, intercept, ceiling)
            for child in node["children"]:
                if stats is not None:
                    stats["branch_work"] += 1
                yield table[child]

        record = merge_capped_frontier_records(iterate_child_frontier_records())
        if record is not None:
            lo, hi, intercept, ceiling = record
            hi = min(hi, node["capacity"])
            if labels[index] == row:
                lo = max(lo, node["capacity"])
            record = (lo, hi, intercept, ceiling) if lo <= hi else None
        table.append(record)
    return table


def compute_additive_core_bound(a, minimum, capacities, counts, union_counts,
                                *, branch_cap=1000000, work_cap=10000000, stats=None):
    if any(type(value) is not int or value < 0 for value in (a, minimum, branch_cap, work_cap)):
        raise ValueError("sizes and budgets must be nonnegative integers")
    metrics = stats if stats is not None else {}
    metrics.update(branches_visited=0, branch_work=0, feasible_branches=0)
    core = build_additive_orientation_core(capacities, counts, union_counts, stats=metrics)
    major_size = core["major_size"]
    if a < core["query_union_size"] or minimum > major_size or minimum+major_size < core["union_size"]:
        raise ValueError("inconsistent query or row sizes")
    branches, size = 1 << core["strict_nodes"], len(core["nodes"])
    # Label-node visits plus two row passes over nodes, edges and leaf bundles.
    per_branch = 3*size+2*(size-1)+2*core["component_nodes"]
    metrics.update(reserved_branches=branches, reserved_branch_work=branches*per_branch,
                   row_record_slots_peak=size, branch_state_scope="core labels and one row table; no input arrays retained by core")
    if branches > branch_cap:
        raise ValueError("branch budget exceeded before enumeration")
    if metrics["reserved_branch_work"] > work_cap:
        raise ValueError("work budget exceeded before enumeration")
    best = None
    for assignment in range(branches):
        metrics["branches_visited"] += 1
        labels = orient_additive_core_labels(core, assignment, stats=metrics)
        major = evaluate_additive_core_records(core, labels, 0, stats=metrics)[core["root"]]
        minor = evaluate_additive_core_records(core, labels, 1, stats=metrics)[core["root"]]
        if major is None or minor is None or not major[0] <= major_size <= major[1] or not minor[0] <= minimum <= minor[1]:
            continue
        metrics["feasible_branches"] += 1
        for record, row_size in ((major, major_size), (minor, minimum)):
            overlap = min(row_size-record[2], record[3])
            score = Fraction(overlap, a+row_size-overlap) if overlap else Fraction()
            best = score if best is None else max(best, score)
    if best is None:
        raise ValueError("no complete pair realizes the metadata")
    return best
