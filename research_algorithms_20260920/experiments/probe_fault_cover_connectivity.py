"""Exact WCC on a trusted, prepared incidence-plus-edit snapshot.

The input provider owns immutable membership, edit and cover-posting views.
They may live on disk; retaining them in a test fixture is not kernel RAM.
Cross-view consistency, duplicate removal and native-source validation belong
to paid preparation. This module does not implement that general builder.
"""

from collections import deque


class LocalSearchBudgetExhausted(Exception):
    """Internal stop before the next event; source failures are not caps."""


def execute_capped_core_search(source, snapshot_id, selected, core_index,
                              outside_count, deletion_lengths, core_degrees,
                              find_root, merge_nodes, receipt, *, cap_enabled=True):
    """Freeze one factor's seed class; share a single fallback across caps."""
    factor_count = len(outside_count)
    deferred = bytearray(factor_count)
    for factor in range(factor_count):
        receipt["core_posting_lookups"] += 1
        members = []
        for vertex in source.iterate_factor_cover_members(factor):
            receipt["core_setup_posting_reads"] += 1
            receipt["core_posting_reads"] += 1
            members.append(core_index[vertex])
        total = len(members)
        if total <= 1 or all(find_root(factor_count + rank) == find_root(factor_count + members[0]) for rank in members):
            receipt["core_resolved_factors"] += 1
            continue
        outside_root = find_root(factor) if outside_count[factor] else None
        seeds = {selected[rank] for rank in members
                 if outside_root is not None and find_root(factor_count + rank) == outside_root}
        blocked = [rank for rank in members if selected[rank] not in seeds]
        count = len(blocked)
        limit, spent = total * count, 0
        positions = {selected[rank]: position for position, rank in enumerate(blocked)}
        previous, following = list(range(-1, count - 1)), list(range(1, count)) + [-1]
        forbidden = [0] * count
        head = 0 if count else -1
        queue = deque()

        def charge_local_search_event(counter):
            nonlocal spent
            if cap_enabled and spent == limit:
                raise LocalSearchBudgetExhausted()
            spent += 1
            receipt["local_search_events"] += 1
            receipt[counter] += 1

        def scan_metered_deletion_row(rank, mark_epoch=None):
            charge_local_search_event("local_deletion_opens")
            cursor = source.open_cover_deletion_row(snapshot_id, selected[rank])
            seed_deletions = 0
            try:
                if type(cursor.length) is not int or cursor.length != deletion_lengths[rank]:
                    raise ValueError("prepared deletion row length changed")
                for _ in range(cursor.length):
                    charge_local_search_event("local_deletion_records")
                    neighbor = cursor.read_next_neighbor_exact()
                    receipt["negative_records_read"] += 1
                    if mark_epoch is None:
                        seed_deletions += neighbor in seeds
                    else:
                        position = positions.get(neighbor)
                        if position is not None:
                            forbidden[position] = mark_epoch
            finally:
                cursor.close_current_cursor_exact()
            return seed_deletions

        def remove_local_unvisited_member(position):
            nonlocal head
            left, right = previous[position], following[position]
            if left == -1:
                head = right
            else:
                following[left] = right
            if right != -1:
                previous[right] = left
            queue.append(position)

        try:
            if seeds:
                for position, rank in enumerate(blocked):
                    if core_degrees[rank] < len(seeds) or scan_metered_deletion_row(rank) < len(seeds):
                        merge_nodes(factor_count + rank, factor)
                        receipt["core_seed_unions"] += 1
                        remove_local_unvisited_member(position)
            while head != -1:
                if not queue:
                    remove_local_unvisited_member(head)
                    if head == -1:
                        break
                position = queue.popleft()
                rank, epoch = blocked[position], position + 1
                scan_metered_deletion_row(rank, epoch)
                other = head
                while other != -1:
                    charge_local_search_event("local_candidate_tests")
                    next_other = following[other]
                    if forbidden[other] != epoch:
                        merge_nodes(factor_count + rank, factor_count + blocked[other])
                        remove_local_unvisited_member(other)
                    other = next_other
            receipt["core_resolved_factors"] += 1
        except LocalSearchBudgetExhausted:
            deferred[factor] = 1
            receipt["core_search_caps"] += 1
            receipt["deferred_scan_budget"] += limit

    if not any(deferred):
        return
    receipt["fallback_passes"] += 1
    forbidden, candidate_stamp = [0] * len(selected), [0] * len(selected)
    for rank, vertex in enumerate(selected):
        epoch = rank + 1
        receipt["fallback_negative_opens"] += 1
        for neighbor in source.iterate_vertex_deleted_neighbors(vertex):
            receipt["fallback_negative_records"] += 1
            receipt["negative_records_read"] += 1
            other = core_index.get(neighbor)
            if other is not None:
                forbidden[other] = epoch
        candidates = []
        for factor in source.iterate_vertex_factor_memberships(vertex):
            receipt["cover_membership_reads"] += 1
            receipt["fallback_membership_reads"] += 1
            if not deferred[factor]:
                continue
            if outside_count[factor] and find_root(factor_count + rank) == find_root(factor):
                continue
            receipt["core_posting_lookups"] += 1
            for other_vertex in source.iterate_factor_cover_members(factor):
                other = core_index[other_vertex]
                receipt["core_posting_reads"] += 1
                receipt["fallback_core_posting_reads"] += 1
                if other != rank and forbidden[other] != epoch and candidate_stamp[other] != epoch:
                    candidate_stamp[other] = epoch
                    candidates.append(other)
        for other in candidates:
            receipt["candidate_flag_visits"] += 1
            merge_nodes(factor_count + rank, factor_count + other)


def build_streaming_edit_cover(edges, *, max_cover_vertices):
    """Established maximal-matching endpoint 2-approximation, with a cap."""
    if type(max_cover_vertices) is not int or max_cover_vertices < 0:
        raise ValueError("invalid cover reservation")
    selected = set()
    for left, right in edges:
        if type(left) is not int or type(right) is not int or min(left, right) < 0 or left == right:
            raise ValueError("invalid normalized edit edge")
        if left not in selected and right not in selected:
            if len(selected) + 2 > max_cover_vertices:
                raise ValueError("edit cover exceeds reservation")
            selected.add(left)
            selected.add(right)
    return tuple(sorted(selected))


def run_prepared_fault_connectivity(source, cover, emit_label, *, max_state_nodes,
                                    core_plan="blocked"):
    """Stream complete canonical labels using O(F+k) kernel workspace.

max_state_nodes bounds DSU nodes only, not Python bytes or total host RAM.
No n-sized label/parent array, edit-sized edge set or k-by-k matrix is built.
"""
    n, factor_count = source.vertex_count, source.factor_count
    if type(n) is not int or n < 0 or type(factor_count) is not int or factor_count < 0:
        raise ValueError("invalid prepared source header")
    if type(max_state_nodes) is not int or max_state_nodes < 0:
        raise ValueError("invalid union-state reservation")
    if factor_count + len(cover) > max_state_nodes:
        raise ValueError("union-state reservation exceeded before allocation")
    if any(type(vertex) is not int or vertex < 0 for vertex in cover):
        raise ValueError("invalid cover vertex")
    selected = tuple(sorted(cover))
    if any(left == right for left, right in zip(selected, selected[1:])):
        raise ValueError("duplicate cover vertex")
    if core_plan not in ("blocked", "capped", "complement"):
        raise ValueError("unsupported core plan")
    snapshot_id = source.snapshot_id if core_plan != "blocked" else None
    core_index = {vertex: rank for rank, vertex in enumerate(selected)}
    k = len(selected)
    edit_records = 0
    for left, right in source.iterate_complete_edit_edges():
        if left not in core_index and right not in core_index:
            raise ValueError("an edit edge is not covered")
        edit_records += 1

    nodes = factor_count + k
    parent, sizes = list(range(nodes)), [1] * nodes
    minima = [None] * factor_count + list(selected)
    outside_count, factor_stamp, missing_count = [0] * factor_count, [0] * factor_count, [0] * factor_count
    candidate_stamp, forbidden_stamp, seen_cover = [0] * k, [0] * k, bytearray(k)
    deletion_lengths = [0] * k if core_plan != "blocked" else None
    core_degrees = [0] * k if core_plan != "blocked" else None
    receipt = {
        "state_nodes": nodes,
        "cover_vertices": k,
        "edit_records_read": edit_records,
        "vertex_rows_read": 0,
        "membership_scan_reads": 0,
        "inserted_neighbor_reads": 0,
        "core_posting_reads": 0,
        "core_posting_lookups": 0,
        "negative_records_read": 0,
        "negative_membership_reads": 0,
        "negative_membership_lookups": 0,
        "cover_membership_reads": 0,
        "candidate_flag_visits": 0,
        "degree_floor_attachments": 0,
        "counted_attachments": 0,
        "blocked_incidences": 0,
        "union_attempts": 0,
        "output_rows": 0,
    }
    if core_plan != "blocked":
        receipt.update(dict.fromkeys((
            "local_search_events", "local_deletion_opens", "local_deletion_records",
            "local_candidate_tests", "core_setup_posting_reads", "core_search_caps",
            "core_resolved_factors", "core_seed_unions", "fallback_core_posting_reads",
            "fallback_negative_records", "fallback_negative_opens", "fallback_passes",
            "deferred_scan_budget", "fallback_membership_reads",
        ), 0))

    def find_union_state_root(node):
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def merge_union_state_nodes(left, right):
        receipt["union_attempts"] += 1
        left, right = find_union_state_root(left), find_union_state_root(right)
        if left == right:
            return left
        if sizes[left] < sizes[right]:
            left, right = right, left
        parent[right] = left
        sizes[left] += sizes[right]
        if minima[right] is not None:
            minima[left] = minima[right] if minima[left] is None else min(minima[left], minima[right])
        return left

    def record_union_vertex_minimum(node, vertex):
        root = find_union_state_root(node)
        minima[root] = vertex if minima[root] is None else min(minima[root], vertex)

    for vertex, memberships, inserted_neighbors in source.iterate_complete_vertex_rows():
        receipt["vertex_rows_read"] += 1
        rank = core_index.get(vertex)
        anchor = None
        if rank is not None:
            seen_cover[rank] = 1
        for factor in memberships:
            receipt["membership_scan_reads"] += 1
            if rank is None:
                outside_count[factor] += 1
                anchor = factor if anchor is None else merge_union_state_nodes(anchor, factor)
        for neighbor in inserted_neighbors:
            receipt["inserted_neighbor_reads"] += 1
            if rank is None:
                target = factor_count + core_index[neighbor]
                anchor = target if anchor is None else merge_union_state_nodes(anchor, target)
        if anchor is not None:
            record_union_vertex_minimum(anchor, vertex)
    if receipt["vertex_rows_read"] != n or not all(seen_cover):
        raise ValueError("vertex universe and cover do not match the prepared header")

    for rank, vertex in enumerate(selected):
        epoch = rank + 1
        memberships = tuple(source.iterate_vertex_factor_memberships(vertex))
        receipt["cover_membership_reads"] += len(memberships)
        outside_deleted = 0
        for neighbor in source.iterate_vertex_deleted_neighbors(vertex):
            receipt["negative_records_read"] += 1
            neighbor_rank = core_index.get(neighbor)
            if core_plan != "blocked":
                deletion_lengths[rank] += 1
                core_degrees[rank] += neighbor_rank is not None
            if neighbor_rank is not None:
                forbidden_stamp[neighbor_rank] = epoch
            else:
                outside_deleted += 1
        uncertain = False
        for factor in memberships:
            if outside_count[factor] > outside_deleted:
                merge_union_state_nodes(factor_count + rank, factor)
                receipt["degree_floor_attachments"] += 1
            elif outside_count[factor] > 0:
                factor_stamp[factor], missing_count[factor] = epoch, 0
                uncertain = True
        if uncertain:
            for neighbor in source.iterate_vertex_deleted_neighbors(vertex):
                receipt["negative_records_read"] += 1
                if neighbor not in core_index:
                    receipt["negative_membership_lookups"] += 1
                    for factor in source.iterate_vertex_factor_memberships(neighbor):
                        receipt["negative_membership_reads"] += 1
                        if factor_stamp[factor] == epoch:
                            missing_count[factor] += 1

        candidates = []
        for factor in memberships:
            blocked = outside_count[factor] == 0
            if factor_stamp[factor] == epoch:
                if missing_count[factor] > outside_count[factor]:
                    raise ValueError("negative multiplicity exceeds outside factor size")
                blocked = outside_count[factor] == missing_count[factor]
                if not blocked:
                    merge_union_state_nodes(factor_count + rank, factor)
                    receipt["counted_attachments"] += 1
            if blocked:
                receipt["blocked_incidences"] += 1
                if core_plan != "blocked":
                    continue
                receipt["core_posting_lookups"] += 1
                for other_vertex in source.iterate_factor_cover_members(factor):
                    other = core_index[other_vertex]
                    receipt["core_posting_reads"] += 1
                    if other != rank and forbidden_stamp[other] != epoch and candidate_stamp[other] != epoch:
                        candidate_stamp[other] = epoch
                        candidates.append(other)
        for other in candidates:
            receipt["candidate_flag_visits"] += 1
            merge_union_state_nodes(factor_count + rank, factor_count + other)

    for left, right in source.iterate_inserted_cover_edges():
        merge_union_state_nodes(factor_count + core_index[left], factor_count + core_index[right])

    if core_plan != "blocked":
        execute_capped_core_search(
            source, snapshot_id, selected, core_index, outside_count,
            deletion_lengths, core_degrees, find_union_state_root,
            merge_union_state_nodes, receipt, cap_enabled=core_plan == "capped",
        )

    for vertex, memberships, inserted_neighbors in source.iterate_complete_vertex_rows():
        receipt["vertex_rows_read"] += 1
        rank = core_index.get(vertex)
        anchor = None if rank is None else factor_count + rank
        for factor in memberships:
            receipt["membership_scan_reads"] += 1
            if anchor is None:
                anchor = factor
        for neighbor in inserted_neighbors:
            receipt["inserted_neighbor_reads"] += 1
            if anchor is None:
                anchor = factor_count + core_index[neighbor]
        label = vertex if anchor is None else minima[find_union_state_root(anchor)]
        if label is None:
            raise ValueError("an output row references an empty quotient node")
        emit_label(vertex, label)
        receipt["output_rows"] += 1
    if receipt["output_rows"] != n:
        raise ValueError("output universe changed between scans")
    return receipt
