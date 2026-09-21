"""Exact query-local orientation cores with paid neutral feasibility indexes."""

from bisect import bisect_left, bisect_right
from fractions import Fraction

from probe_additive_core_similarity import evaluate_additive_core_records, orient_additive_core_labels


def compile_query_skeleton_index(static_core, *, stats=None):
    metrics = {} if stats is None else stats
    nodes = static_core["nodes"]
    parents, populations, shared = [-1] * len(nodes), [], []
    for position, node in enumerate(nodes):
        children = node["children"]
        for child in children:
            if not 0 <= child < position or parents[child] != -1:
                raise ValueError("invalid static core tree")
            parents[child] = position
        population = sum(populations[child] for child in children)
        if node["strict_bit"] is None:
            lower, upper, _, _ = node["bundle"]
            population += lower + upper
            minimum = sum(shared[child] for child in children)
        else:
            left, right = children
            deficit = nodes[left]["capacity"] + nodes[right]["capacity"] - node["capacity"]
            x = 2 * nodes[left]["capacity"] - populations[left] - deficit
            y = 2 * nodes[right]["capacity"] - populations[right] - deficit
            if shared[left] > x or shared[right] > y:
                raise ValueError("no complete pair realizes the metadata")
            minimum = min(y + shared[left], x + shared[right])
        if not 0 <= minimum <= 2 * node["capacity"] - population:
            raise ValueError("no complete pair realizes the metadata")
        populations.append(population)
        shared.append(minimum)
    if any(parent == -1 and node != static_core["root"] for node, parent in enumerate(parents)):
        raise ValueError("disconnected static core")
    metrics.update(index_nodes_compiled=len(nodes), index_parent_entries=len(parents),
                   index_population_entries=len(populations), index_shared_entries=len(shared),
                   index_owner_endpoints=len(static_core["owner_runs"]))
    return dict(static=static_core, parents=tuple(parents), populations=tuple(populations),
                shared=tuple(shared), owner_ends=tuple(end for _, end, _ in static_core["owner_runs"]))


def prepare_query_skeleton_core(index, a, minimum, leaf_queries, *, skeleton_cap=1000000, stats=None):
    metrics = {} if stats is None else stats
    metrics.update(marked_nodes=0, skeleton_nodes=0, local_strict_nodes=0,
                   sparse_pairs_read=0, query_union_size=0, neutral_capsules=0,
                   owner_lookups=0, exception_hits=0, active_child_reductions=0)
    if any(type(value) is not int or value < 0 for value in (a, minimum, skeleton_cap)):
        raise ValueError("sizes and skeleton cap must be nonnegative builtin integers")
    static, population, shared, parents = index["static"], index["populations"], index["shared"], index["parents"]
    root, major = static["root"], static["major_size"]
    if minimum > major or minimum + major - population[root] < shared[root]:
        raise ValueError("no complete pair realizes the root sizes")
    if skeleton_cap == 0:
        raise ValueError("skeleton budget exceeded before query consumption")
    nodes, updates, marked, active_children = static["nodes"], {}, set(), {}
    total, previous = 0, -1
    for pair in leaf_queries:
        metrics["sparse_pairs_read"] += 1
        if not isinstance(pair, (tuple, list)) or len(pair) != 2:
            raise ValueError("sparse query entries must be two-item pairs")
        leaf, query = pair
        if type(leaf) is not int or not 0 <= leaf < static["groups"] or leaf <= previous:
            raise ValueError("leaf indices must be valid sorted unique integers")
        if type(query) is not int or not 0 < query <= static["query_limit"]:
            raise ValueError("query count exceeds profile or is not positive integer")
        total += query
        if total > a:
            raise ValueError("query union size exceeds query size")
        owner = static["owner_runs"][bisect_right(index["owner_ends"], leaf)][2]
        metrics["owner_lookups"] += 1
        default = static.get("component_defaults")
        signature = default[owner] if default is not None else None
        deviation = bisect_left(static["exception_keys"], leaf)
        if deviation < len(static["exception_keys"]) and static["exception_keys"][deviation] == leaf:
            _, *signature = static["exceptions"][deviation]
            metrics["exception_hits"] += 1
        low_reward = high_reward = query
        if signature is not None:
            lower, capacity = signature
            if query > lower + capacity:
                raise ValueError("query count exceeds leaf union population")
            low_reward, high_reward = min(query, lower), min(query, capacity)
        low, high = updates.get(owner, (0, 0))
        updates[owner] = low + low_reward, high + high_reward
        current = owner
        while current not in marked:
            if len(marked) >= skeleton_cap:
                raise ValueError("skeleton budget exceeded while marking ancestors")
            marked.add(current)
            metrics["marked_nodes"] = len(marked)
            parent = parents[current]
            if parent == -1:
                break
            active_children.setdefault(parent, []).append(current)
            current = parent
        previous = leaf
        metrics["query_union_size"] = total
    reduced, roots = [], {}

    def append_skeleton_core_record(record):
        if len(reduced) >= skeleton_cap:
            raise ValueError("skeleton budget exceeded before record publication")
        reduced.append(record)
        metrics["skeleton_nodes"] = len(reduced)
        return len(reduced) - 1

    def append_neutral_subtree_capsule(original):
        capacity = nodes[original]["capacity"]
        lower = population[original] - capacity + shared[original]
        metrics["neutral_capsules"] += 1
        return append_skeleton_core_record(dict(original_node=original, capacity=capacity, children=(),
            strict_bit=None, bundle=(lower, capacity, lower, 0)))

    if not marked:
        roots[root] = append_neutral_subtree_capsule(root)
    else:
        stack = [(root, False)]
        while stack:
            original, ready = stack.pop()
            active = active_children.get(original, ())
            if not ready:
                stack.append((original, True))
                stack.extend((child, False) for child in active)
                continue
            node = nodes[original]
            metrics["active_child_reductions"] += len(active)
            if node["strict_bit"] is not None:
                children = tuple(roots[child] if child in marked else append_neutral_subtree_capsule(child)
                                 for child in node["children"])
                record = dict(original_node=original, capacity=node["capacity"], children=children,
                              strict_bit=metrics["local_strict_nodes"], bundle=None)
                metrics["local_strict_nodes"] += 1
            else:
                lo, hi, _, _ = node["bundle"]
                inactive_capacity = node["capacity"] - hi
                inactive_population = population[original] - lo - hi
                inactive_shared = shared[original]
                for child in active:
                    inactive_capacity -= nodes[child]["capacity"]
                    inactive_population -= population[child]
                    inactive_shared -= shared[child]
                if min(inactive_capacity, inactive_population, inactive_shared) < 0:
                    raise ValueError("invalid inactive boundary totals")
                lower = lo + inactive_population - inactive_capacity + inactive_shared
                upper = hi + inactive_capacity
                low_reward, ceiling = updates.get(original, (0, 0))
                record = dict(original_node=original, capacity=node["capacity"],
                    children=tuple(roots[child] for child in active), strict_bit=None,
                    bundle=(lower, upper, lower - low_reward, ceiling))
            roots[original] = append_skeleton_core_record(record)
    strict = metrics["local_strict_nodes"]
    return dict(nodes=tuple(reduced), root=roots[root], strict_nodes=strict,
                component_nodes=len(reduced) - strict, major_size=major,
                union_size=population[root], query_union_size=total)


def compute_query_skeleton_bound(index, a, minimum, leaf_queries, *, skeleton_cap=1000000,
                                 branch_cap=1000000, work_cap=10000000, stats=None):
    metrics = {} if stats is None else stats
    metrics.update(branches_visited=0, branch_work=0, feasible_branches=0,
                   reserved_branches=0, reserved_branch_work=0, row_record_slots_peak=0)
    if any(type(value) is not int or value < 0 for value in (branch_cap, work_cap)):
        raise ValueError("branch and work caps must be nonnegative builtin integers")
    core = prepare_query_skeleton_core(index, a, minimum, leaf_queries, skeleton_cap=skeleton_cap, stats=metrics)
    size = len(core["nodes"])
    branches = 1 << core["strict_nodes"]
    work = branches * (5 * size - 2 + 2 * core["component_nodes"])
    metrics.update(reserved_branches=branches, reserved_branch_work=work)
    if branches > branch_cap or work > work_cap:
        raise ValueError("branch or work budget exceeded before enumeration")
    metrics["row_record_slots_peak"] = size
    best = None
    for assignment in range(branches):
        metrics["branches_visited"] += 1
        labels = orient_additive_core_labels(core, assignment, stats=metrics)
        major = evaluate_additive_core_records(core, labels, 0, stats=metrics)[core["root"]]
        minor = evaluate_additive_core_records(core, labels, 1, stats=metrics)[core["root"]]
        if (major is None or minor is None or not major[0] <= core["major_size"] <= major[1]
                or not minor[0] <= minimum <= minor[1]):
            continue
        metrics["feasible_branches"] += 1
        for record, row_size in ((major, core["major_size"]), (minor, minimum)):
            overlap = min(row_size - record[2], record[3])
            score = Fraction(overlap, a + row_size - overlap) if overlap else Fraction()
            best = score if best is None else max(best, score)
    if best is None:
        raise ValueError("no complete pair realizes the metadata")
    return best
