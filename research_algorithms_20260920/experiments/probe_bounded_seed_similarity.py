"""Exact file-backed Jaccard selection with O(k) retained interval heads."""

from collections import Counter
from contextlib import closing
from functools import partial
from heapq import heapify, heappop, heappush, heapreplace, merge
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter


def select_bounded_runwise_answers(reference, snapshot, source, sid, k,
                                   heap_cap=100000, disk_cap=10**8,
                                   output_cap=10**6, source_epoch=None,
                                   fail_after=None, field_builder=None,
                                   prune_run_bounds=False):
    began = perf_counter()
    assert snapshot.token == snapshot.post_token == snapshot.tree_token == snapshot.meta_token == snapshot.inverse_token
    assert source_epoch is None or source_epoch == snapshot.token[0]
    assert k >= 0
    selfpos = snapshot.self_position(sid)
    population = snapshot.n - int(selfpos is not None)
    count = min(k, population)
    local = Counter(k=count, output_bytes=count * reference.OUTPUT.size)
    assert local["output_bytes"] <= output_cap and local["output_bytes"] <= disk_cap
    admitted = min(population, max(0, 2 * count - 1))
    assert admitted <= heap_cap, "heap admission"
    before_expansion = reference.stats["build_pairs_records"]
    with TemporaryDirectory(dir=snapshot.root, prefix="query-") as temporary:
        arena = reference.Arena(Path(temporary), disk_cap)
        field, heap = None, []

        def resolve_exact_interval_head(lo, hi, overlap):
            if lo >= hi:
                return None
            pos, node, size = snapshot.rmq(lo, hi, overlap > 0, local)
            score = reference.exact_jaccard_score_key(local["a"], size, overlap)
            return -score, node, pos, size, overlap, lo, hi

        def retain_initial_interval_seed(lo, hi, overlap):
            if lo >= hi:
                return
            local["seed_intervals_seen"] += 1
            if prune_run_bounds and len(heap) == count and local["a"]:
                threshold = heap[0][0]
                if overlap * threshold.denominator < local["a"] * threshold.numerator:
                    local["bound_pruned_runs"] += 1
                    return
            head = resolve_exact_interval_head(lo, hi, overlap)
            if head is None:
                return
            local["seed_rmq_calls"] += 1
            # Reverse both score and original-ID order: root is the worst seed.
            reverse = (-head[0], -head[1], *head[2:])
            if len(heap) < count:
                heappush(heap, reverse)
            elif reverse > heap[0]:
                heapreplace(heap, reverse)
                local["seed_replacements"] += 1
            local["seed_peak"] = max(local["seed_peak"], len(heap))

        if count:
            if field_builder is None:
                field = reference.construct_source_overlap_runs(snapshot, source, arena, local)
            else:
                field = field_builder(reference, snapshot, source, arena, local)
            for lo, hi, overlap in reference.read_fixed_file_records(field, reference.RUN, "field_read"):
                if selfpos is not None and lo <= selfpos < hi:
                    retain_initial_interval_seed(lo, selfpos, overlap)
                    retain_initial_interval_seed(selfpos + 1, hi, overlap)
                else:
                    retain_initial_interval_seed(lo, hi, overlap)
            local["retained_seeds"] = len(heap)
            local["discarded_seeds"] = local["seed_rmq_calls"] - len(heap)
            # Convert in place; no second seed list or retained discarded heads.
            for i in range(len(heap)):
                score, reverse_id, pos, size, overlap, lo, hi = heap[i]
                heap[i] = -score, -reverse_id, pos, size, overlap, lo, hi
            heapify(heap)
            local["heap_peak"] = len(heap)

        def insert_child_interval_head(lo, hi, overlap):
            head = resolve_exact_interval_head(lo, hi, overlap)
            if head is not None:
                local["child_rmq_calls"] += 1
                heappush(heap, head)
                local["heap_peak"] = max(local["heap_peak"], len(heap))
                assert len(heap) <= admitted, "heap admission"

        def iterate_complete_ranked_answers():
            for rank in range(count):
                assert heap, "incomplete result"
                _, node, pos, size, overlap, lo, hi = heappop(heap)
                if fail_after is not None and rank == fail_after:
                    raise OSError("injected failed sink")
                yield node, local["a"], size, overlap
                if rank + 1 < count:
                    insert_child_interval_head(lo, pos, overlap)
                    insert_child_interval_head(pos + 1, hi, overlap)

        stage, final = arena.root / "staged", arena.root / "published"
        written = arena.write(stage, reference.OUTPUT, iterate_complete_ranked_answers())
        assert written == count
        stage.replace(final)
        local["published"] = 1
        assert local["heap_peak"] <= admitted
        assert local["seed_peak"] <= count
        assert local["child_rmq_calls"] <= max(0, 2 * (count - 1))
        assert local["rmq_calls"] == local["seed_rmq_calls"] + local["child_rmq_calls"]
        assert local["seed_intervals_seen"] == local["seed_rmq_calls"] + local["bound_pruned_runs"]
        assert reference.stats["build_pairs_records"] == before_expansion
        # Test sink only; materialization occurs after complete file publication.
        rows = list(reference.read_fixed_file_records(final, reference.OUTPUT, "output_check"))
        arena.remove(final)
        if field is not None:
            arena.remove(field)
        assert arena.live == 0
    local["query_seconds"] = perf_counter() - began
    return rows, local


def construct_merged_overlap_runs(reference, snapshot, source, arena, local, fanin_cap=128):
    a, intervals, streams, previous = 0, 0, 0, None
    for (feature,) in reference.read_fixed_file_records(source, reference.U, "merge_source"):
        assert previous is None or previous < feature
        previous, a = feature, a + 1
        entry = snapshot.feature(feature)
        if entry is not None and entry[2]:
            streams += 1
            intervals += entry[2]
    assert streams <= fanin_cap, "merge admission"
    events = 2 * intervals
    assert reference.RUN.size * min(snapshot.n, events + 1) + local["output_bytes"] <= arena.cap
    local.update(a=a, L=intervals, E=events, merge_streams=streams)

    def iterate_sorted_feature_endpoints(entry):
        end_previous = -1
        records = reference.read_fixed_file_records(snapshot.posts, reference.INTERVAL,
                                                     "merged_intervals", entry[1], entry[2])
        with closing(records):
            for lo, hi in records:
                assert 0 <= lo < hi <= snapshot.n and lo > end_previous
                end_previous = hi
                local["merged_events"] += 1
                yield lo, 1
                local["merged_events"] += 1
                yield hi, -1

    cursors = []
    for (feature,) in reference.read_fixed_file_records(source, reference.U, "merge_source"):
        entry = snapshot.feature(feature)
        if entry is not None and entry[2]:
            cursors.append(iterate_sorted_feature_endpoints(entry))
    assert len(cursors) == streams
    ordered = merge(*cursors)
    path = arena.root / "field"
    try:
        runs = arena.write(path, reference.RUN,
                           reference.sweep_coalesced_overlap_runs(ordered, snapshot.n, a))
        assert local["merged_events"] == events, "incomplete endpoints"
        local["R"] = runs
        assert runs <= min(snapshot.n, events + 1)
    finally:
        ordered.close()
        for cursor in cursors:
            cursor.close()
    return path


def select_merged_runwise_answers(reference, snapshot, source, sid, k, merge_cap=128, **options):
    if type(merge_cap) is not int or merge_cap < 0:
        raise ValueError("merge budget must be a nonnegative integer")
    builder = partial(construct_merged_overlap_runs, fanin_cap=merge_cap)
    return select_bounded_runwise_answers(reference, snapshot, source, sid, k,
                                          field_builder=builder, **options)


def select_pruned_runwise_answers(reference, snapshot, source, sid, k, **options):
    return select_merged_runwise_answers(reference, snapshot, source, sid, k,
                                         prune_run_bounds=True, **options)


def scan_exact_similarity_targets(reference, snapshot, source, sid, k,
                                  heap_cap=100000, source_cap=100000,
                                  disk_cap=10**8, output_cap=10**6):
    began = perf_counter()
    assert snapshot.token == snapshot.post_token == snapshot.tree_token == snapshot.meta_token == snapshot.inverse_token
    assert k >= 0
    count = min(k, snapshot.n - int(snapshot.self_position(sid) is not None))
    local = Counter(k=count, output_bytes=count * reference.OUTPUT.size)
    assert count <= heap_cap, "heap admission"
    assert local["output_bytes"] <= output_cap and local["output_bytes"] <= disk_cap
    source_values, heap = set(), []
    if count:
        previous = None
        for (feature,) in reference.read_fixed_file_records(source, reference.U, "scan_source"):
            assert previous is None or previous < feature
            source_values.add(feature)
            assert len(source_values) <= source_cap, "source admission"
            previous = feature
        local["source_peak"] = len(source_values)
        pairs = reference.read_fixed_file_records(snapshot.pairs, reference.PAIR, "scan_pairs")
        for position, (node, size) in enumerate(reference.read_fixed_file_records(snapshot.meta, reference.META, "scan_meta")):
            overlap = 0
            for _ in range(size):
                feature, owner = next(pairs)
                assert owner == position, "scan requires canonical position-grouped pairs"
                overlap += int(feature in source_values)
                local["membership_visits"] += 1
            if node != sid:
                score = reference.exact_jaccard_score_key(len(source_values), size, overlap)
                record = score, -node, node, len(source_values), size, overlap
                if len(heap) < count:
                    heappush(heap, record)
                elif record > heap[0]:
                    heapreplace(heap, record)
                local["heap_peak"] = max(local["heap_peak"], len(heap))
        assert next(pairs, None) is None
    with TemporaryDirectory(dir=snapshot.root, prefix="query-") as temporary:
        arena = reference.Arena(Path(temporary), disk_cap)
        stage, final = arena.root / "staged", arena.root / "published"
        output = (record[2:] for record in sorted(heap, reverse=True))
        assert arena.write(stage, reference.OUTPUT, output) == count
        stage.replace(final)
        local["published"] = 1
        rows = list(reference.read_fixed_file_records(final, reference.OUTPUT, "output_check"))
        arena.remove(final)
        assert arena.live == 0
    local["query_seconds"] = perf_counter() - began
    return rows, local
