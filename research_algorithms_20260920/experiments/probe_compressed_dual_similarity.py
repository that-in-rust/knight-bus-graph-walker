"""Exact inverse-overlap DP on prepared additive regions, not original leaves.

Resource caps bound threshold cells and counted recurrence work, not physical
bytes. Modal compilation, sparse query mapping and O(r) planning remain paid.
"""

from fractions import Fraction

from probe_exception_core_similarity import prepare_exception_query_core


def build_compressed_threshold_plan(core):
    plan, roots = [], []
    for node in core["nodes"]:
        children = tuple(roots[child] for child in node["children"])
        if node["strict_bit"] is not None:
            left, right = (plan[child] for child in children)
            plan.append({"children": children, "capacity": node["capacity"],
                         "population": left["population"] + right["population"],
                         "scores": left["scores"] + right["scores"], "baseline": None})
        else:
            lower, upper, intercept, ceiling = node["bundle"]
            plan.append({"children": (), "capacity": upper, "population": lower + upper,
                         "scores": ceiling, "baseline": lower - intercept})
            current = len(plan) - 1
            for child in children:
                left, right = plan[current], plan[child]
                plan.append({"children": (current, child),
                             "capacity": left["capacity"] + right["capacity"],
                             "population": left["population"] + right["population"],
                             "scores": left["scores"] + right["scores"], "baseline": None})
                current = len(plan) - 1
            if plan[-1]["capacity"] != node["capacity"]:
                raise AssertionError("component capacity is not additive")
        roots.append(len(plan) - 1)
    return plan, roots[core["root"]]


def compute_compressed_threshold_bound(static_core, a, minimum, leaf_queries, *,
                                       state_cap=1000000, work_cap=10000000, stats=None):
    metrics = {} if stats is None else stats
    metrics.update(states=0, operations=0, reserved_states=0, reserved_operations=0,
                   plan_nodes=0, orientations_enumerated=0)
    if any(type(value) is not int or value < 0 for value in (a, minimum, state_cap, work_cap)):
        raise ValueError("sizes and budgets must be nonnegative builtin integers")
    major_size, population = static_core["major_size"], static_core["union_size"]
    if minimum > major_size or minimum + major_size < population:
        raise ValueError("inconsistent row sizes")
    core = prepare_exception_query_core(static_core, a, leaf_queries, stats=metrics)
    plan, root = build_compressed_threshold_plan(core)
    metrics["plan_nodes"] = len(plan)
    reserved_states = 2 * sum(node["scores"] + 1 for node in plan)
    operations = 2 * (plan[root]["scores"] + 1)
    for node in plan:
        if not node["children"]:
            operations += 2 * (node["scores"] + 1)
        else:
            left, right = (plan[child] for child in node["children"])
            deficit = left["capacity"] + right["capacity"] - node["capacity"]
            operations += (2 * (left["scores"] + 1) * (right["scores"] + 1) if deficit == 0
                           else 4 * (node["scores"] + 1) + 2 * (left["scores"] + right["scores"] + 2))
    metrics.update(reserved_states=reserved_states, reserved_operations=operations)
    if reserved_states > state_cap or operations > work_cap:
        raise ValueError("threshold state or work budget exceeded before enumeration")
    tables = []

    def lookup_threshold_reward_value(table, shared):
        result = None
        for score, threshold in enumerate(table):
            metrics["operations"] += 1
            if threshold is not None and threshold <= shared:
                result = score
        return result

    for node in plan:
        major, minor = [None] * (node["scores"] + 1), [None] * (node["scores"] + 1)
        if not node["children"]:
            for score in range(node["scores"] + 1):
                metrics["operations"] += 2
                major[score] = 0
                minor[score] = max(0, score - node["baseline"])
        else:
            left_id, right_id = node["children"]
            left, right = plan[left_id], plan[right_id]
            deficit = left["capacity"] + right["capacity"] - node["capacity"]
            if deficit == 0:
                for role, target in enumerate((major, minor)):
                    for i, x in enumerate(tables[left_id][role]):
                        for j, y in enumerate(tables[right_id][role]):
                            metrics["operations"] += 1
                            if x is not None and y is not None:
                                previous = target[i + j]
                                target[i + j] = x + y if previous is None else min(previous, x + y)
            else:
                left_limit = 2 * left["capacity"] - left["population"] - deficit
                right_limit = 2 * right["capacity"] - right["population"] - deficit
                left_values = [lookup_threshold_reward_value(table, left_limit) for table in tables[left_id]]
                right_values = [lookup_threshold_reward_value(table, right_limit) for table in tables[right_id]]
                options = (
                    ((right_limit, left_limit, tables[left_id][0], right_values[1]),
                     (left_limit, right_limit, tables[right_id][0], left_values[1])),
                    ((left_limit, right_limit, tables[right_id][1], left_values[0]),
                     (right_limit, left_limit, tables[left_id][1], right_values[0])),
                )
                for target, branches in zip((major, minor), options):
                    for score in range(node["scores"] + 1):
                        for fixed, limit, table, fixed_score in branches:
                            metrics["operations"] += 1
                            if fixed_score is None:
                                continue
                            needed = max(0, score - fixed_score)
                            if needed >= len(table) or table[needed] is None or table[needed] > limit:
                                continue
                            value, previous = fixed + table[needed], target[score]
                            target[score] = value if previous is None else min(previous, value)
        tables.append((major, minor))
        metrics["states"] += len(major) + len(minor)
    shared = minimum + major_size - population
    overlaps = [lookup_threshold_reward_value(table, shared) for table in tables[root]]
    if any(value is None for value in overlaps):
        raise ValueError("no complete pair realizes the metadata")
    return max(Fraction(value, a + size - value) if value else Fraction()
               for value, size in zip(overlaps, (major_size, minimum)))
