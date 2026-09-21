"""Selective token-group access; exact row, interval and attained-pair bounds."""

from collections import Counter
from contextlib import closing
from bisect import bisect_left
from fractions import Fraction
from heapq import merge, heappush, heapreplace
from itertools import groupby
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from types import SimpleNamespace

from probe_laminar_capacity_similarity import (
    BLOCK, GLOBAL, MAGIC, WORD, compute_laminar_jaccard_bound,
    compute_paired_capacity_bound, read_capacity_binary_record,
)
from probe_admitted_similarity_bounds import solve_admitted_pair_envelope


def compute_interval_capacity_bound(a, minimum, capacities, counts, union_counts, population):
    groups = len(counts)
    assert groups > 0 and groups & (groups - 1) == 0
    assert len(capacities) == 2 * groups and len(union_counts) == groups and population >= 1
    assert 0 <= minimum <= capacities[1] and sum(counts) <= a
    assert all(0 <= q <= h for q, h in zip(counts, union_counts))
    unions = [0] * groups + list(union_counts)
    states = [None] * (2 * groups)
    for index in range(2 * groups - 1, 0, -1):
        if index < groups:
            unions[index] = unions[2 * index] + unions[2 * index + 1]
        capacity, union = capacities[index], unions[index]
        assert 0 <= capacity <= union
        lower, upper = max(0, union - (population - 1) * capacity), capacity
        if index >= groups:
            debt, ceiling = 0, counts[index - groups]
        else:
            left, right = states[2 * index], states[2 * index + 1]
            floor = left[0] + right[0]
            debt = floor - min(left[0] - left[2], left[3]) - min(right[0] - right[2], right[3])
            ceiling = min(left[1] - left[2], left[3]) + min(right[1] - right[2], right[3])
            lower, upper = max(lower, floor), min(upper, left[1] + right[1])
        assert lower <= upper, "inconsistent interval certificate"
        states[index] = lower, upper, debt, ceiling
    lower, upper, debt, ceiling = states[1]
    lower = max(lower, minimum)
    assert lower <= upper
    sizes = (minimum, capacities[1]) if population == 2 else (max(lower, min(upper, debt + ceiling)),)
    scores = []
    for size in sizes:
        if lower <= size <= upper:
            overlap = min(size - debt, ceiling)
            scores.append(Fraction(overlap, a + size - overlap) if overlap else Fraction(0))
    assert scores
    return max(scores)


def open_selective_capacity_index(reference, snapshot, root):
    meta, posts, directory = root / "meta", root / "intervals", root / "directory"
    magic, n, groups, block_size, epoch = next(reference.read_fixed_file_records(meta, GLOBAL, "selective_open", count=1))
    assert magic == b"SCAP0001" and n == snapshot.n and epoch == snapshot.token[0]
    assert groups > 0 and groups & (groups - 1) == 0 and block_size > 0
    blocks = (n + block_size - 1) // block_size
    stride = BLOCK.size + WORD.size * (3 * groups - 1)
    assert meta.stat().st_size == GLOBAL.size + blocks * stride
    assert posts.stat().st_size % reference.INTERVAL.size == 0
    assert directory.stat().st_size % reference.DIRECTORY.size == 0
    return SimpleNamespace(root=root, meta=meta, posts=posts, directory=directory,
                           n=n, groups=groups, block_size=block_size, blocks=blocks,
                           stride=stride, token=snapshot.token,
                           F=directory.stat().st_size // reference.DIRECTORY.size)


def build_selective_capacity_index(reference, snapshot, sidecar, disk_cap=10**8, group_cap=256):
    began = perf_counter()
    assert snapshot.token == snapshot.post_token == snapshot.meta_token
    final = snapshot.root / "selective-capacity"
    assert not final.exists(), "immutable generation already exists"
    local = Counter()
    with TemporaryDirectory(dir=snapshot.root, prefix="selective-build-") as temporary:
        root = Path(temporary) / "generation"
        root.mkdir()

        def write_admitted_index_record(stream, layout, record):
            assert local["retained_bytes"] + layout.size <= disk_cap, "selective disk admission"
            reference.write_fixed_file_record(stream, layout, record, "selective_build_write")
            local["retained_bytes"] += layout.size

        with open(sidecar, "rb", buffering=0) as source, open(root / "meta", "wb", buffering=0) as meta:
            magic, n, groups, block_size, epoch = read_capacity_binary_record(source, GLOBAL, local, "input_header")
            assert magic == MAGIC and n == snapshot.n and epoch == snapshot.token[0], "generation mismatch"
            assert 0 < groups <= group_cap and groups & (groups - 1) == 0 and block_size > 0
            write_admitted_index_record(meta, GLOBAL, (b"SCAP0001", n, groups, block_size, epoch))
            for bid in range((n + block_size - 1) // block_size):
                header = read_capacity_binary_record(source, BLOCK, local, "input_header")
                lo, hi, _, minimum, maximum, _, union_count = header
                assert lo == bid * block_size and hi == min(n, lo + block_size)
                assert minimum <= maximum <= union_count
                caps = [read_capacity_binary_record(source, WORD, local, "input_capacity")[0]
                        for _ in range(2 * groups - 1)]
                assert caps[0] == maximum
                populations, previous = [0] * groups, None
                for _ in range(union_count):
                    feature = read_capacity_binary_record(source, WORD, local, "input_union")[0]
                    assert previous is None or previous < feature
                    previous = feature
                    populations[feature % groups] += 1
                write_admitted_index_record(meta, BLOCK, header)
                for value in caps + populations:
                    write_admitted_index_record(meta, WORD, (value,))
                local["blocks"] += 1
            assert source.read(1) == b""
        with open(root / "intervals", "wb", buffering=0) as posts, open(root / "directory", "wb", buffering=0) as directory:
            entries = reference.read_fixed_file_records(snapshot.directory, reference.DIRECTORY,
                                                         "selective_input_directory", count=snapshot.F)
            with closing(entries):
                for feature, offset, count in entries:
                    output_offset, output_count = posts.tell(), 0
                    previous_lo, previous_hi = None, None
                    intervals = reference.read_fixed_file_records(snapshot.posts, reference.INTERVAL,
                                                                 "selective_input_intervals", offset, count)
                    with closing(intervals):
                        for lo, hi in intervals:
                            assert 0 <= lo < hi <= snapshot.n
                            lo, hi = lo // block_size, (hi - 1) // block_size + 1
                            if previous_lo is None:
                                previous_lo, previous_hi = lo, hi
                            elif lo <= previous_hi:
                                previous_hi = max(previous_hi, hi)
                            else:
                                write_admitted_index_record(posts, reference.INTERVAL, (previous_lo, previous_hi))
                                output_count += 1
                                previous_lo, previous_hi = lo, hi
                    if previous_lo is not None:
                        write_admitted_index_record(posts, reference.INTERVAL, (previous_lo, previous_hi))
                        output_count += 1
                    assert output_count > 0
                    write_admitted_index_record(directory, reference.DIRECTORY, (feature, output_offset, output_count))
                    local["posting_intervals"] += output_count
                    local["features"] += 1
        root.rename(final)
    index = open_selective_capacity_index(reference, snapshot, final)
    local["build_seconds"] = perf_counter() - began
    return index, local


def select_indexed_capacity_answers(reference, snapshot, index, source, sid, k, mode="paired",
                                     source_cap=4096, merge_cap=128, group_cap=256,
                                     heap_cap=100000, disk_cap=10**8, output_cap=10**6,
                                     fail_after=None, pair_state_cap=4096, pair_work_cap=65536,
                                     pair_query_work_cap=10**7):
    began = perf_counter()
    assert mode in ("union", "partition", "laminar", "interval", "paired",
                    "overlap", "overlap_dense", "overlap_threshold")
    assert all(type(value) is int and value >= 0 for value in (pair_state_cap, pair_work_cap, pair_query_work_cap))
    assert index.token == snapshot.token == snapshot.meta_token == snapshot.inverse_token
    assert type(k) is int and k >= 0 and index.groups <= group_cap
    source_bytes = source.stat().st_size
    assert source_bytes % reference.U.size == 0
    a = source_bytes // reference.U.size
    assert a <= source_cap, "source admission"
    selfpos = snapshot.self_position(sid)
    count = min(k, snapshot.n - int(selfpos is not None))
    local = Counter(a=a, k=count, output_bytes=count * reference.OUTPUT.size)
    assert count <= heap_cap, "heap admission"
    assert local["output_bytes"] <= output_cap and local["output_bytes"] <= disk_cap, "output admission"
    values = [feature for (feature,) in reference.read_fixed_file_records(source, reference.U, "indexed_source", count=a)]
    assert all(values[j - 1] < values[j] for j in range(1, a))
    heap, cursors, groups = [], [], index.groups

    def iterate_feature_block_records(entry):
        records = reference.read_fixed_file_records(index.posts, reference.INTERVAL,
                                                    "indexed_intervals", entry[1], entry[2])
        previous = -1
        with closing(records):
            for lo, hi in records:
                assert 0 <= lo < hi <= index.blocks and lo > previous
                previous = hi
                for block in range(lo, hi):
                    yield block, entry[0] % groups

    if count:
        for feature in values:
            entry = reference.lookup_ordered_file_record(index.directory, reference.DIRECTORY,
                                                         index.F, feature, "indexed_lookup")
            if entry is not None and entry[2]:
                assert len(cursors) < merge_cap, "merge admission"
                cursors.append(iterate_feature_block_records(entry))
                local["posting_intervals"] += entry[2]
    local["merge_streams"] = len(cursors)
    ordered = merge(*cursors)
    try:
        for bid, occurrences in groupby(ordered, key=lambda row: row[0]):
            counts = [0] * groups
            for _, leaf in occurrences:
                counts[leaf] += 1
                local["block_memberships"] += 1
            local["blocks_seen"] += 1
            offset = GLOBAL.size + bid * index.stride
            lo, hi, pair_offset, minimum, maximum, min_id, union_count = next(
                reference.read_fixed_file_records(index.meta, BLOCK, "indexed_header", offset, 1))
            assert lo == bid * index.block_size and hi == min(snapshot.n, lo + index.block_size)

            def current_bound_prunes_block(bound):
                return len(heap) == count and (bound < heap[0][0] or (bound == heap[0][0] and min_id > heap[0][2]))

            if len(heap) == count:
                caps = [0] * (2 * groups)
                caps[1] = maximum
                upper = compute_laminar_jaccard_bound(a, minimum, caps, counts, "union")
                local["union_evaluations"] += 1
                if not current_bound_prunes_block(upper) and mode != "union":
                    cap_offset = offset + BLOCK.size
                    if mode == "partition":
                        cap_offset += (groups - 1) * WORD.size
                        start = groups
                    else:
                        start = 1
                    for j, (value,) in enumerate(reference.read_fixed_file_records(index.meta, WORD,
                                                         "indexed_capacity", cap_offset, 2 * groups - start), start=start):
                        caps[j] = value
                    upper = compute_laminar_jaccard_bound(a, minimum, caps, counts,
                                                         "partition" if mode == "partition" else "laminar")
                    local["capacity_evaluations"] += 1
                    if not current_bound_prunes_block(upper) and mode in ("interval", "paired", "overlap", "overlap_dense", "overlap_threshold"):
                        population_offset = offset + BLOCK.size + (2 * groups - 1) * WORD.size
                        populations = [value for (value,) in reference.read_fixed_file_records(index.meta, WORD,
                                                                    "indexed_population", population_offset, groups)]
                        refined = compute_interval_capacity_bound(a, minimum, caps, counts, populations, hi - lo)
                        assert refined <= upper
                        upper = refined
                        local["interval_evaluations"] += 1
                        if not current_bound_prunes_block(upper) and mode == "paired" and hi - lo == 2 and union_count == minimum + maximum:
                            refined = compute_paired_capacity_bound(a, minimum, caps, counts, populations)
                            assert refined <= upper
                            local["paired_evaluations"] += 1
                            local["paired_tighter"] += int(refined < upper)
                            upper = refined
                        if not current_bound_prunes_block(upper) and mode.startswith("overlap") and hi - lo == 2:
                            strategy = "auto" if mode == "overlap" else mode.removeprefix("overlap_")
                            remaining = pair_query_work_cap - local["pair_reserved_work_total"]
                            refined, paid = solve_admitted_pair_envelope(a, minimum, caps, counts, populations,
                                                                         strategy=strategy, state_cap=pair_state_cap,
                                                                         work_cap=min(pair_work_cap, remaining))
                            local["overlap_preflights"] += 1
                            if refined is None:
                                local["overlap_refusals"] += 1
                            else:
                                assert refined <= upper
                                local["overlap_evaluations"] += 1
                                local["overlap_" + paid["solver"] + "_calls"] += 1
                                local["overlap_tighter"] += int(refined < upper)
                                local["overlap_shared_calls"] += int(union_count < minimum + maximum)
                                local["pair_reserved_work_total"] += paid["reserved_work"]
                                local["pair_actual_work_total"] += paid["work"]
                                local["pair_reserved_cells_peak"] = max(local["pair_reserved_cells_peak"], paid["reserved_states"])
                                local["pair_actual_cells_peak"] = max(local["pair_actual_cells_peak"], paid["states"])
                                upper = refined
                if current_bound_prunes_block(upper):
                    local["blocks_pruned"] += 1
                    continue
            metadata = reference.read_fixed_file_records(snapshot.meta, reference.META, "indexed_body_meta",
                                                         lo * reference.META.size, hi - lo)
            pairs = reference.read_fixed_file_records(snapshot.pairs, reference.PAIR, "indexed_body_pairs", pair_offset)
            with closing(metadata), closing(pairs):
                for position, (node, size) in enumerate(metadata, start=lo):
                    overlap = 0
                    for _ in range(size):
                        record = next(pairs, None)
                        assert record is not None
                        feature, owner = record
                        assert owner == position
                        point = bisect_left(values, feature)
                        overlap += int(point < a and values[point] == feature)
                        local["body_memberships"] += 1
                    local["body_targets"] += 1
                    if position == selfpos or overlap == 0:
                        continue
                    record = Fraction(overlap, a + size - overlap), -node, node, a, size, overlap
                    if len(heap) < count:
                        heappush(heap, record)
                    elif record > heap[0]:
                        heapreplace(heap, record)
                    local["heap_peak"] = max(local["heap_peak"], len(heap))
    finally:
        ordered.close()
        for cursor in cursors:
            cursor.close()
    if len(heap) < count:
        positive_ids = {record[2] for record in heap}
        ids = reference.read_fixed_file_records(snapshot.inverse, reference.INVERSE, "indexed_zero_ids")
        with closing(ids):
            for node, position in ids:
                if node == sid or node in positive_ids:
                    continue
                actual, size = snapshot.metadata(position)
                assert actual == node
                heappush(heap, (Fraction(0), -node, node, a, size, 0))
                local["zero_rows"] += 1
                local["heap_peak"] = max(local["heap_peak"], len(heap))
                if len(heap) == count:
                    break
    assert len(heap) == count
    with TemporaryDirectory(dir=snapshot.root, prefix="query-") as temporary:
        arena = reference.Arena(Path(temporary), disk_cap)

        def iterate_complete_output_records():
            for rank, record in enumerate(sorted(heap, reverse=True)):
                if fail_after is not None and rank == fail_after:
                    raise OSError("injected failed sink")
                yield record[2:]

        stage, final = arena.root / "staged", arena.root / "published"
        assert arena.write(stage, reference.OUTPUT, iterate_complete_output_records()) == count
        stage.replace(final)
        local["published"] = 1
        rows = list(reference.read_fixed_file_records(final, reference.OUTPUT, "output_check"))
        arena.remove(final)
        assert arena.live == 0
    local["query_seconds"] = perf_counter() - began
    return rows, local
