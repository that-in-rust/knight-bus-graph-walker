"""Neutral-aware local plans for exact resident two-row similarity queries.

The inverse recurrence is inherited from probe_compressed_dual_similarity;
only base offsets and the shared query-local preparation differ. Old measured
modules remain unchanged. Logical cell/work caps are not process RAM limits.
"""

from collections import Counter
from fractions import Fraction
from heapq import heappush, heapreplace, merge
from itertools import groupby

from probe_compressed_interval_similarity import combine_control_record_group
from probe_query_skeleton_similarity import compile_query_skeleton_index, prepare_query_skeleton_core
from probe_sparse_control_similarity import prepare_compact_source_view


class QueryThresholdBudgetExceeded(ValueError):
    pass


def build_skeleton_threshold_plan(index, core):
    plan, roots = [], []
    for node in core["nodes"]:
        children = tuple(roots[child] for child in node["children"])
        population = index["populations"][node["original_node"]]
        if node["strict_bit"] is not None:
            plan.append(dict(children=children, capacity=node["capacity"], population=population,
                             scores=sum(plan[child]["scores"] for child in children), baseline=None, floor=None))
        else:
            lower, upper, debt, ceiling = node["bundle"]
            actual = population - sum(plan[child]["population"] for child in children)
            floor, baseline = lower + upper - actual, lower - debt
            if not (0 <= floor and 0 <= lower <= upper and 0 <= baseline <= ceiling
                    and ceiling - baseline <= upper - lower):
                raise ValueError("invalid offset bundle")
            plan.append(dict(children=(), capacity=upper, population=actual,
                             scores=ceiling, baseline=baseline, floor=floor))
            current = len(plan) - 1
            for child in children:
                left, right = plan[current], plan[child]
                plan.append(dict(children=(current, child), capacity=left["capacity"]+right["capacity"],
                    population=left["population"]+right["population"],
                    scores=left["scores"]+right["scores"], baseline=None, floor=None))
                current = len(plan) - 1
        if plan[-1]["capacity"] != node["capacity"] or plan[-1]["population"] != population:
            raise ValueError("local plan boundary mismatch")
        roots.append(len(plan) - 1)
    return dict(nodes=tuple(plan), root=roots[core["root"]], major_size=core["major_size"],
                union_size=core["union_size"], minimum_shared=index["shared"][index["static"]["root"]],
                query_union_size=core["query_union_size"], local_strict_nodes=core["strict_nodes"])


def validate_skeleton_solver_inputs(plan, a, minimum, *budgets):
    if any(type(value) is not int or value < 0 for value in (a, minimum, *budgets)):
        raise ValueError("sizes and budgets must be nonnegative builtin integers")
    if (minimum > plan["major_size"] or minimum+plan["major_size"]-plan["union_size"] < plan["minimum_shared"]
            or not 0 <= plan["query_union_size"] <= min(a, plan["union_size"])):
        raise ValueError("invalid query size or infeasible root sizes")


def evaluate_skeleton_interval_control(plan, a, minimum, *, stats=None):
    validate_skeleton_solver_inputs(plan, a, minimum)
    metrics = {} if stats is None else stats
    metrics.update(control_nodes_visited=0, control_records_peak=0)
    records = []
    for node in plan["nodes"]:
        if not node["children"]:
            lo = node["population"] - node["capacity"] + node["floor"]
            record = (lo, node["capacity"], lo-node["baseline"], node["scores"],
                      node["population"], node["scores"])
        else:
            lo, hi, debt, ceiling, population, rank = combine_control_record_group(
                records[child] for child in node["children"])
            record = (max(lo, population-node["capacity"]), min(hi, node["capacity"]),
                      debt, ceiling, population, min(rank, node["capacity"]))
        if record[0] > record[1]:
            raise ValueError("infeasible local interval")
        records.append(record)
        metrics["control_nodes_visited"] += 1
    metrics["control_records_peak"] = len(records)
    lo, hi, debt, ceiling, _, _ = records[plan["root"]]
    scores = []
    for size in (minimum, plan["major_size"]):
        if lo <= size <= hi:
            overlap = min(size-debt, ceiling)
            scores.append(Fraction(overlap, a+size-overlap) if overlap else Fraction())
    if not scores:
        raise ValueError("infeasible local root interval")
    return max(scores)


def evaluate_skeleton_threshold_plan(plan, a, minimum, *, state_cap=1000000, work_cap=10000000, stats=None):
    metrics = {} if stats is None else stats
    metrics.update(states=0, operations=0, reserved_states=0, reserved_operations=0)
    validate_skeleton_solver_inputs(plan, a, minimum, state_cap, work_cap)
    nodes, root = plan["nodes"], plan["root"]
    reserved = 2 * sum(node["scores"]+1 for node in nodes)
    operations = 2 * (nodes[root]["scores"]+1)
    for node in nodes:
        if not node["children"]:
            operations += 2 * (node["scores"]+1)
        else:
            left, right = (nodes[child] for child in node["children"])
            deficit = left["capacity"]+right["capacity"]-node["capacity"]
            operations += (2*(left["scores"]+1)*(right["scores"]+1) if deficit == 0
                           else 4*(node["scores"]+1)+2*(left["scores"]+right["scores"]+2))
    metrics.update(reserved_states=reserved, reserved_operations=operations)
    if reserved > state_cap or operations > work_cap:
        raise QueryThresholdBudgetExceeded("threshold state or work budget exceeded before enumeration")
    tables = []

    def lookup_threshold_reward_value(table, shared):
        result = None
        for score, threshold in enumerate(table):
            metrics["operations"] += 1
            if threshold is not None and threshold <= shared:
                result = score
        return result

    for node in nodes:
        major, minor = [None]*(node["scores"]+1), [None]*(node["scores"]+1)
        if not node["children"]:
            for score in range(node["scores"]+1):
                metrics["operations"] += 2
                major[score] = node["floor"]
                minor[score] = node["floor"]+max(0, score-node["baseline"])
        else:
            left_id, right_id = node["children"]
            left, right = nodes[left_id], nodes[right_id]
            deficit = left["capacity"]+right["capacity"]-node["capacity"]
            if deficit == 0:
                for role, target in enumerate((major, minor)):
                    for i, x in enumerate(tables[left_id][role]):
                        for j, y in enumerate(tables[right_id][role]):
                            metrics["operations"] += 1
                            if x is not None and y is not None:
                                previous = target[i+j]
                                target[i+j] = x+y if previous is None else min(previous, x+y)
            else:
                left_limit = 2*left["capacity"]-left["population"]-deficit
                right_limit = 2*right["capacity"]-right["population"]-deficit
                left_values = [lookup_threshold_reward_value(table, left_limit) for table in tables[left_id]]
                right_values = [lookup_threshold_reward_value(table, right_limit) for table in tables[right_id]]
                options = (
                    ((right_limit, left_limit, tables[left_id][0], right_values[1]),
                     (left_limit, right_limit, tables[right_id][0], left_values[1])),
                    ((left_limit, right_limit, tables[right_id][1], left_values[0]),
                     (right_limit, left_limit, tables[left_id][1], right_values[0])),
                )
                for target, branches in zip((major, minor), options):
                    for score in range(node["scores"]+1):
                        for fixed, limit, table, fixed_score in branches:
                            metrics["operations"] += 1
                            if fixed_score is None:
                                continue
                            needed = max(0, score-fixed_score)
                            if needed >= len(table) or table[needed] is None or table[needed] > limit:
                                continue
                            value, previous = fixed+table[needed], target[score]
                            target[score] = value if previous is None else min(previous, value)
        tables.append((major, minor))
        metrics["states"] += len(major)+len(minor)
    shared = minimum+plan["major_size"]-plan["union_size"]
    overlaps = [lookup_threshold_reward_value(table, shared) for table in tables[root]]
    if any(value is None for value in overlaps):
        raise ValueError("no complete pair realizes the metadata")
    return max(Fraction(value, a+size-value) if value else Fraction()
               for value, size in zip(overlaps, (plan["major_size"], minimum)))


def prepare_skeleton_source_view(source):
    compact = prepare_compact_source_view(source)
    for block in compact["blocks"]:
        block["index"] = compile_query_skeleton_index(block["core"]) if block["core"] is not None else None
    return compact


def run_skeleton_similarity_query(source, query, sid, k, *, mode="dual", skeleton_cap=4096,
                                  state_cap=65536, work_cap=65536):
    if mode not in ("neutral", "dual"):
        raise ValueError("unknown local query mode")
    if any(type(value) is not int or value < 0 for value in (k, skeleton_cap, state_cap, work_cap)):
        raise ValueError("invalid query budget")
    query = frozenset(query)
    if any(type(value) is not int or value < 0 for value in query):
        raise ValueError("invalid query feature")
    count = min(k, len(source["target_ids"])-int(sid in source["sizes"]))
    stats = Counter(a=len(query), k=count, dense_histogram_cells=0)
    heap = []

    def update_local_witness_heap(node, overlap):
        if node == sid or not overlap or not count:
            return
        size = source["sizes"][node]
        record = (Fraction(overlap, len(query)+size-overlap), -node, node, len(query), size, overlap)
        if len(heap) < count:
            heappush(heap, record)
        elif record > heap[0]:
            heapreplace(heap, record)

    def bound_rejects_current_block(bound, minimum_id):
        return bound < heap[0][0] or (bound == heap[0][0] and minimum_id > heap[0][2])

    def iterate_local_posting_records(feature):
        for bid in source["block_posts"].get(feature, ()):
            yield bid, feature % source["groups"]

    if count:
        streams = [iterate_local_posting_records(feature) for feature in sorted(query)]
        stats["posting_streams"] = sum(bool(source["block_posts"].get(feature)) for feature in query)
        for bid, occurrences in groupby(merge(*streams), key=lambda row: row[0]):
            core = plan = None
            sparse = [(leaf, sum(1 for _ in repeated))
                      for leaf, repeated in groupby(occurrences, key=lambda row: row[1])]
            total = sum(value for _, value in sparse)
            stats["sparse_pairs_peak"] = max(stats["sparse_pairs_peak"], len(sparse))
            stats["sparse_pairs_constructed"] += len(sparse)
            stats["block_memberships"] += total
            stats["blocks_seen"] += 1
            block = source["blocks"][bid]
            if len(heap) == count:
                rank = min(total, block["major"])
                bound = Fraction(rank, len(query)+max(block["minimum"], rank)-rank) if rank else Fraction()
                stats["union_evaluations"] += 1
                if not bound_rejects_current_block(bound, block["min_id"]) and block["index"] is not None:
                    if max(value for _, value in sparse) > source["query_limit"]:
                        stats["profile_refusals"] += 1
                    else:
                        prepared, core = {}, None
                        stats["skeleton_attempts"] += 1
                        try:
                            core = prepare_query_skeleton_core(block["index"], len(query), block["minimum"],
                                iter(sparse), skeleton_cap=skeleton_cap, stats=prepared)
                        except ValueError as error:
                            if not str(error).startswith("skeleton budget"):
                                raise
                            stats["skeleton_refusals"] += 1
                        for key in ("marked_nodes", "skeleton_nodes", "local_strict_nodes", "sparse_pairs_read",
                                    "owner_lookups", "exception_hits", "active_child_reductions"):
                            stats[key] += prepared.get(key, 0)
                        stats["skeleton_records_peak"] = max(stats["skeleton_records_peak"], prepared.get("skeleton_nodes", 0))
                        if core is not None:
                            stats["skeleton_preparations"] += 1
                            plan = build_skeleton_threshold_plan(block["index"], core)
                            stats["plan_nodes"] += len(plan["nodes"])
                            stats["plan_records_peak"] = max(stats["plan_records_peak"], len(plan["nodes"]))
                            paid = {}
                            control = evaluate_skeleton_interval_control(plan, len(query), block["minimum"], stats=paid)
                            assert control <= bound
                            bound = control
                            stats["control_evaluations"] += 1
                            stats["control_nodes_visited"] += paid["control_nodes_visited"]
                            if mode == "dual" and not bound_rejects_current_block(bound, block["min_id"]):
                                if core["strict_nodes"] == 0:
                                    stats["exact_offset_shortcuts"] += 1
                                else:
                                    paid = {}
                                    stats["threshold_attempts"] += 1
                                    try:
                                        exact = evaluate_skeleton_threshold_plan(plan, len(query), block["minimum"],
                                            state_cap=state_cap, work_cap=work_cap, stats=paid)
                                    except QueryThresholdBudgetExceeded:
                                        stats["threshold_refusals"] += 1
                                    else:
                                        assert exact <= bound
                                        bound = exact
                                        stats["threshold_evaluations"] += 1
                                    for key in ("states", "operations", "reserved_states", "reserved_operations"):
                                        stats[key] += paid[key]
                                    stats["threshold_cells_peak"] = max(stats["threshold_cells_peak"], paid["states"])
                if bound_rejects_current_block(bound, block["min_id"]):
                    stats["blocks_pruned"] += 1
                    continue
            for node, features in block["rows"]:
                stats["body_targets"] += 1
                stats["body_memberships"] += len(features)
                update_local_witness_heap(node, len(query & features))
    if len(heap) < count:
        positive = {record[2] for record in heap}
        for node in source["target_ids"]:
            stats["zero_id_visits"] += 1
            if node == sid or node in positive:
                continue
            heappush(heap, (Fraction(), -node, node, len(query), source["sizes"][node], 0))
            stats["zero_rows"] += 1
            if len(heap) == count:
                break
    assert len(heap) == count
    return [record[2:] for record in sorted(heap, reverse=True)], stats
