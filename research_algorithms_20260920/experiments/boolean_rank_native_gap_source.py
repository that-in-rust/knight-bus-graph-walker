"""Two-pass native pair-source admission with an exact tree comparison gap.

Missing pairs and unequal heights are allowed, but the group graph must be
connected on every supplied group. This is not an actual-output certificate.
"""

from contextlib import closing
from fractions import Fraction
import hashlib
import json

from stream_boolean_rank_solver import manage_source_iterator_lifetime


def iterate_checked_native_classes(source, factors, expected):
    count, previous = 0, (-1, -1)
    with manage_source_iterator_lifetime(source.iterate_class_records()) as records:
        for class_id, groups, height, degree, weight in records:
            if type(class_id) is not int or class_id != count or count >= expected:
                raise ValueError("native class IDs must be contiguous and match source count")
            if (type(groups) is not tuple or len(groups) != 2
                    or any(type(v) is not int for v in groups)
                    or not 0 <= groups[0] < groups[1] < factors or groups <= previous):
                raise ValueError("native pair classes must be strictly ordered, unique and in range")
            if type(height) is not int or not 1 <= height <= 2**53:
                raise ValueError("positive supported class height required")
            if type(degree) is not int or not 1 <= degree <= 2**53:
                raise ValueError("positive supported original degree required")
            if type(weight) is not Fraction or weight < 0:
                raise ValueError("nonnegative exact class weight required")
            count, previous = count + 1, groups
            yield class_id, groups, height, degree, weight
    if count != expected:
        raise ValueError("native class stream does not match declared count")


def hash_native_class_record(digest, record):
    cid, groups, height, degree, weight = record
    digest.update(json.dumps([cid, groups, height, degree, weight.numerator, weight.denominator],
                             separators=(",", ":")).encode("ascii") + b"\n")


def find_native_group_root(parents, vertex):
    while parents[vertex] != vertex:
        parents[vertex] = parents[parents[vertex]]
        vertex = parents[vertex]
    return vertex


def calculate_native_tree_congestion(tree, populations):
    """Exact length-weighted canonical-path congestion from linear tree moments."""
    factors = len(populations)
    parent, weights, order = [0] * factors, [0] * factors, [0]
    for vertex in order:
        for neighbor, height in tree[vertex]:
            if neighbor != parent[vertex]:
                parent[neighbor], weights[neighbor] = vertex, height
                order.append(neighbor)
    if len(order) != factors:
        raise ValueError("internal spanning tree is incomplete")
    volume = sum(populations)
    subtree, inside, distances = populations.copy(), [0] * factors, [0] * factors
    for index in range(factors - 1, 0, -1):
        vertex = order[index]
        ancestor = parent[vertex]
        subtree[ancestor] += subtree[vertex]
        inside[ancestor] += inside[vertex] + subtree[vertex]
    distances[0] = inside[0]
    congestion = Fraction()
    for index in range(1, factors):
        vertex = order[index]
        ancestor, inside_mass = parent[vertex], subtree[vertex]
        outside_mass = volume - inside_mass
        distances[vertex] = distances[ancestor] + volume - 2 * inside_mass
        outside_moment = distances[ancestor] - inside[vertex] - inside_mass
        demand = (outside_mass * inside[vertex] + inside_mass * outside_moment
                  + inside_mass * outside_mass)
        if outside_moment < 0 or demand <= 0:
            raise ValueError("inconsistent tree distance moments")
        congestion = max(congestion, Fraction(demand, volume * weights[vertex]))
    return congestion


def validate_native_boolean_source(source, *, max_factor_slots):
    """Return scalar topology/gap metadata after paid, matching class passes.

    Population/union-find/tree/moment arrays retain O(F) exact integers and tree
    arcs, never P/n arrays. No original vertex row is scanned. Metadata validity
    and pinning remain trusted source-builder contracts, not adversarial DB
    authentication. All returned gaps are rational lower bounds, not timings.
    """
    factors, classes, vertices = source.factor_count, source.class_count, source.vertex_count
    if (type(max_factor_slots) is not int or max_factor_slots < 0
            or type(factors) is not int or not 3 <= factors <= max_factor_slots):
        raise ValueError("native pair source requires F>=3 within factor reservation")
    if (type(classes) is not int or not factors - 1 <= classes <= factors * (factors - 1) // 2
            or type(source.active_class_count) is not int or source.active_class_count != classes
            or type(vertices) is not int or vertices < classes):
        raise ValueError("inconsistent native pair source counts")
    if type(source.total_weight) is not Fraction or source.total_weight <= 0 or source.isolate_weight != 0:
        raise ValueError("positive exact weight and no isolates required")
    snapshot = source.snapshot_id
    populations, parents, sizes = [0] * factors, list(range(factors)), [1] * factors
    incidences = [0] * factors
    tree = [[] for _ in range(factors)]
    edge_count = original_count = 0
    smallest, largest = vertices, 0
    total_weight = Fraction()
    first_hash = hashlib.sha256()
    with closing(iterate_checked_native_classes(source, factors, classes)) as records:
        for record in records:
            _, (left, right), height, _, weight = record
            hash_native_class_record(first_hash, record)
            populations[left] += height
            populations[right] += height
            incidences[left] += 1
            incidences[right] += 1
            original_count += height
            total_weight += weight
            smallest, largest = min(smallest, height), max(largest, height)
            a, b = find_native_group_root(parents, left), find_native_group_root(parents, right)
            if a != b:
                if sizes[a] < sizes[b]:
                    a, b = b, a
                parents[b] = a
                sizes[a] += sizes[b]
                tree[left].append((right, height))
                tree[right].append((left, height))
                edge_count += 1
    if (original_count != vertices or total_weight != source.total_weight
            or edge_count != factors - 1 or min(populations) <= 0 or source.snapshot_id != snapshot):
        raise ValueError("disconnected, changed or inconsistent native source")
    # Union-find is no longer needed when the tree moment arrays are allocated.
    mediating_count = sum(count >= 2 for count in incidences)
    mediating_minimum = min(populations[a] for a in range(factors) if incidences[a] >= 2)
    del parents, sizes, incidences
    volume = maximum_degree = 0
    second_hash = hashlib.sha256()
    with closing(iterate_checked_native_classes(source, factors, classes)) as records:
        for record in records:
            _, (left, right), height, degree, _ = record
            hash_native_class_record(second_hash, record)
            if degree != populations[left] + populations[right] - height - 1:
                raise ValueError("inconsistent original Boolean degree")
            maximum_degree = max(maximum_degree, degree)
            volume += height * degree
    if first_hash.digest() != second_hash.digest() or source.snapshot_id != snapshot:
        raise ValueError("native source changed between validation passes")
    congestion = calculate_native_tree_congestion(tree, populations)
    group_gap = 1 / congestion
    unrefined_gap = min(Fraction(1), Fraction(min(populations), maximum_degree) * group_gap)
    tree_gap = min(Fraction(1), Fraction(mediating_minimum, maximum_degree) * group_gap)
    complete = classes == factors * (factors - 1) // 2
    all_pair_gap = (min(Fraction(1), Fraction(smallest * smallest * factors, largest * maximum_degree))
                    if complete else Fraction())
    gap = max(tree_gap, all_pair_gap)
    return dict(snapshot_id=snapshot, factors=factors, class_count=classes, vertices=vertices,
                volume=volume, max_degree=maximum_degree, min_population=min(populations),
                min_height=smallest, max_height=largest, complete_pairs=complete,
                gap_lower=gap, spectral_upper=1-gap, tree_gap_lower=tree_gap,
                group_gap_lower=group_gap, all_pair_gap_lower=all_pair_gap,
                unrefined_tree_gap_lower=unrefined_gap,
                min_mediating_population=mediating_minimum, mediating_group_count=mediating_count,
                tree_congestion=congestion, validation_class_passes=2,
                validation_class_rows=2*classes, original_vertex_reads=0,
                tree_edges=edge_count, tree_arcs=2*edge_count,
                population_counter_entries=factors, tree_auxiliary_vector_entries=6*factors,
                class_incidence_counter_entries=factors,
                class_stream_sha256=first_hash.hexdigest(),
                scope="trusted connected native pair source; exact tree/all-pair comparison; no physical cap")
