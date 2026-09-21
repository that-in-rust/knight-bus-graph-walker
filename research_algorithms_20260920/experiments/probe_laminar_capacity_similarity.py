"""Paid block summaries for exact Jaccard; logical budgets, not process-RAM claims."""

from bisect import bisect_left
from collections import Counter
from contextlib import closing
from fractions import Fraction
from heapq import heappush, heapreplace
from pathlib import Path
from struct import Struct
from tempfile import TemporaryDirectory
from time import perf_counter


GLOBAL = Struct("<8sQQQQ")
BLOCK = Struct("<QQQQQqQ")
WORD = Struct("<Q")
MAGIC = b"LCAP0001"
FORMATS = {"union": b"UCAP0001", "partition": b"PCAP0001", "laminar": MAGIC}


def compute_laminar_jaccard_bound(a, minimum, capacities, counts, mode="laminar"):
    groups = len(counts)
    assert groups > 0 and groups & (groups - 1) == 0
    assert len(capacities) == 2 * groups and 0 <= minimum <= capacities[1]
    assert all(value >= 0 for value in capacities) and all(value >= 0 for value in counts)
    assert sum(counts) <= a
    assert mode in ("union", "partition", "laminar")
    if mode == "union":
        rank = min(sum(counts), capacities[1])
    else:
        ranks = [0] * groups + [min(counts[j], capacities[groups + j]) for j in range(groups)]
        if mode == "partition":
            rank = min(capacities[1], sum(ranks[groups:]))
        else:
            for index in range(groups - 1, 0, -1):
                ranks[index] = min(capacities[index], ranks[2 * index] + ranks[2 * index + 1])
            rank = ranks[1]
    return Fraction(rank, a + max(minimum, rank) - rank) if rank else Fraction(0)


def compute_paired_capacity_bound(a, minimum, capacities, counts, union_counts):
    groups = len(counts)
    assert groups > 0 and groups & (groups - 1) == 0 and len(union_counts) == groups
    assert len(capacities) == 2 * groups and 0 <= minimum <= capacities[1]
    assert all(0 <= q <= h for q, h in zip(counts, union_counts)) and sum(counts) <= a
    populations = [0] * groups + list(union_counts)
    for index in range(groups - 1, 0, -1):
        populations[index] = populations[2 * index] + populations[2 * index + 1]
    assert populations[1] == minimum + capacities[1], "not a complementary pair"
    states = [{} for _ in range(2 * groups)]
    for index in range(2 * groups - 1, 0, -1):
        capacity, population = capacities[index], populations[index]
        assert 0 <= capacity <= population <= 2 * capacity, "invalid attained maximum"
        choices = {capacity, population - capacity}
        if index >= groups:
            states[index] = {size: min(size, counts[index - groups]) for size in choices}
        else:
            for left_size, left_overlap in states[2 * index].items():
                for right_size, right_overlap in states[2 * index + 1].items():
                    size, overlap = left_size + right_size, left_overlap + right_overlap
                    if size in choices:
                        states[index][size] = max(states[index].get(size, -1), overlap)
    assert states[1], "inconsistent paired certificate"
    return max(Fraction(overlap, a + size - overlap) if overlap else Fraction(0)
               for size, overlap in states[1].items())


def build_laminar_capacity_sidecar(reference, snapshot, groups=16, block_size=64,
                                   union_cap=4096, group_cap=256, disk_cap=10**8, mode="laminar"):
    began = perf_counter()
    assert type(groups) is int and 0 < groups <= group_cap and groups & (groups - 1) == 0
    assert type(block_size) is int and block_size > 0 and union_cap >= 0 and disk_cap >= 0
    assert snapshot.token == snapshot.meta_token
    assert snapshot.meta.stat().st_size == snapshot.n * reference.META.size
    assert snapshot.pairs.stat().st_size == snapshot.memberships * reference.PAIR.size
    assert mode in FORMATS
    final = snapshot.root / (mode + "-capacity")
    old_bytes = final.stat().st_size if final.exists() else 0
    local = Counter(groups=groups, block_size=block_size, old_generation_bytes=old_bytes)
    metadata = reference.read_fixed_file_records(snapshot.meta, reference.META, "capacity_build_meta", count=snapshot.n)
    memberships = reference.read_fixed_file_records(snapshot.pairs, reference.PAIR, "capacity_build_pairs", count=snapshot.memberships)
    with TemporaryDirectory(dir=snapshot.root, prefix="capacity-build-") as temporary:
        stage = Path(temporary) / "sidecar"
        with closing(metadata), closing(memberships), open(stage, "wb", buffering=0) as output:
            def write_admitted_capacity_record(layout, record):
                assert old_bytes + local["retained_bytes"] + layout.size <= disk_cap, "sidecar disk admission"
                reference.write_fixed_file_record(output, layout, record, "capacity_build_write")
                local["retained_bytes"] += layout.size

            write_admitted_capacity_record(GLOBAL, (FORMATS[mode], snapshot.n, groups, block_size, snapshot.token[0]))
            pair_offset = 0
            for lo in range(0, snapshot.n, block_size):
                hi = min(snapshot.n, lo + block_size)
                union, capacities = set(), [0] * (2 * groups)
                minimum, maximum, min_id = None, 0, None
                first_offset = pair_offset
                for position in range(lo, hi):
                    node, size = next(metadata)
                    minimum = size if minimum is None else min(minimum, size)
                    maximum = max(maximum, size)
                    min_id = node if min_id is None else min(min_id, node)
                    occupancy, previous = [0] * (2 * groups), None
                    for _ in range(size):
                        record = next(memberships, None)
                        assert record is not None, "missing canonical membership"
                        feature, owner = record
                        assert owner == position and (previous is None or previous < feature)
                        previous = feature
                        if feature not in union:
                            assert len(union) < union_cap, "block union admission"
                            union.add(feature)
                        if mode != "union":
                            occupancy[groups + feature % groups] += 1
                        pair_offset += reference.PAIR.size
                    if mode == "laminar":
                        for index in range(groups - 1, 0, -1):
                            occupancy[index] = occupancy[2 * index] + occupancy[2 * index + 1]
                    if mode != "union":
                        for index in range(1 if mode == "laminar" else groups, 2 * groups):
                            capacities[index] = max(capacities[index], occupancy[index])
                    capacities[1] = maximum
                assert capacities[1] == maximum
                local["union_peak"] = max(local["union_peak"], len(union))
                local["union_words"] += len(union)
                local["blocks"] += 1
                write_admitted_capacity_record(BLOCK, (lo, hi, first_offset, minimum, maximum, min_id, len(union)))
                stored = capacities[1:] if mode == "laminar" else capacities[groups:] if mode == "partition" else ()
                for capacity in stored:
                    write_admitted_capacity_record(WORD, (capacity,))
                ordered_union = sorted(union)
                for feature in ordered_union:
                    write_admitted_capacity_record(WORD, (feature,))
                del ordered_union
            assert next(metadata, None) is None and next(memberships, None) is None
            assert pair_offset == snapshot.memberships * reference.PAIR.size
        stage.replace(final)
    assert final.stat().st_size == local["retained_bytes"]
    local["owned_peak_bytes"] = old_bytes + local["retained_bytes"]
    local["build_seconds"] = perf_counter() - began
    return final, local


def read_capacity_binary_record(stream, layout, local, tag):
    data = stream.read(layout.size)
    assert len(data) == layout.size, "truncated capacity sidecar"
    local[tag + "_bytes"] += layout.size
    local[tag + "_records"] += 1
    return layout.unpack(data)


def select_laminar_capacity_answers(reference, snapshot, sidecar, source, sid, k,
                                    mode="laminar", source_cap=4096, group_cap=256,
                                    heap_cap=100000, disk_cap=10**8, output_cap=10**6,
                                    fail_after=None):
    began = perf_counter()
    assert snapshot.token == snapshot.meta_token == snapshot.inverse_token
    assert type(k) is int and k >= 0 and mode in ("union", "partition", "laminar", "paired", "paired_lazy")
    pair_mode = mode in ("paired", "paired_lazy")
    base_mode = "laminar" if pair_mode else mode
    source_bytes = source.stat().st_size
    assert source_bytes % reference.U.size == 0, "truncated source"
    a = source_bytes // reference.U.size
    assert a <= source_cap, "source admission"
    selfpos = snapshot.self_position(sid)
    count = min(k, snapshot.n - int(selfpos is not None))
    local = Counter(a=a, k=count, output_bytes=count * reference.OUTPUT.size)
    assert count <= heap_cap, "heap admission"
    assert local["output_bytes"] <= output_cap and local["output_bytes"] <= disk_cap, "output admission"
    values = [feature for (feature,) in reference.read_fixed_file_records(source, reference.U, "capacity_source", count=a)]
    assert all(values[j - 1] < values[j] for j in range(1, a)), "source order"
    heap = []
    with open(sidecar, "rb", buffering=0) as index:
        magic, n, groups, block_size, epoch = read_capacity_binary_record(index, GLOBAL, local, "header")
        assert magic in FORMATS.values() and n == snapshot.n and epoch == snapshot.token[0], "sidecar generation"
        stored_mode = next(name for name, value in FORMATS.items() if value == magic)
        assert list(FORMATS).index(base_mode) <= list(FORMATS).index(stored_mode), "insufficient certificate"
        assert 0 < groups <= group_cap and groups & (groups - 1) == 0, "group admission"
        assert block_size > 0
        local["groups"] = groups
        previous_hi = 0
        for _ in range((n + block_size - 1) // block_size if count else 0):
            lo, hi, pair_offset, minimum, maximum, min_id, union_count = read_capacity_binary_record(index, BLOCK, local, "header")
            assert lo == previous_hi and hi == min(n, lo + block_size) and lo < hi
            assert 0 <= minimum <= maximum <= union_count
            assert pair_offset <= snapshot.memberships * reference.PAIR.size
            previous_hi = hi
            capacities = [0] * (2 * groups)
            capacities[1] = maximum
            if stored_mode != "union":
                for j in range(1 if stored_mode == "laminar" else groups, 2 * groups):
                    capacities[j] = read_capacity_binary_record(index, WORD, local, "capacity")[0]
            assert capacities[1] == maximum
            counts, previous, cursor = [0] * groups, None, 0
            union_counts = [0] * groups if pair_mode else None
            for _ in range(union_count):
                feature = read_capacity_binary_record(index, WORD, local, "union")[0]
                assert previous is None or previous < feature
                previous = feature
                if union_counts is not None:
                    union_counts[feature % groups] += 1
                while cursor < a and values[cursor] < feature:
                    cursor += 1
                if cursor < a and values[cursor] == feature:
                    counts[feature % groups] += 1
            local["blocks_seen"] += 1
            upper = compute_laminar_jaccard_bound(a, minimum, capacities, counts, base_mode)
            base_pruned = len(heap) == count and (upper < heap[0][0] or (upper == heap[0][0] and min_id > heap[0][2]))
            if pair_mode and hi - lo == 2 and union_count == minimum + maximum:
                local["paired_eligible"] += 1
                if mode == "paired" or (len(heap) == count and not base_pruned):
                    paired_upper = compute_paired_capacity_bound(a, minimum, capacities, counts, union_counts)
                    assert paired_upper <= upper
                    local["paired_evaluations"] += 1
                    local["paired_tighter"] += int(paired_upper < upper)
                    upper = paired_upper
            if len(heap) == count and (upper < heap[0][0] or (upper == heap[0][0] and min_id > heap[0][2])):
                local["blocks_pruned"] += 1
                local["targets_pruned"] += hi - lo
                continue
            metadata = reference.read_fixed_file_records(snapshot.meta, reference.META, "capacity_body_meta",
                                                         lo * reference.META.size, hi - lo)
            pairs = reference.read_fixed_file_records(snapshot.pairs, reference.PAIR, "capacity_body_pairs", pair_offset)
            with closing(metadata), closing(pairs):
                for position, (node, size) in enumerate(metadata, start=lo):
                    overlap = 0
                    for _ in range(size):
                        record = next(pairs, None)
                        assert record is not None, "missing body membership"
                        feature, owner = record
                        assert owner == position
                        cursor = bisect_left(values, feature)
                        overlap += int(cursor < a and values[cursor] == feature)
                        local["body_memberships"] += 1
                    local["body_targets"] += 1
                    if position == selfpos:
                        continue
                    score = Fraction(overlap, a + size - overlap) if overlap else Fraction(0)
                    result = score, -node, node, a, size, overlap
                    if len(heap) < count:
                        heappush(heap, result)
                    elif result > heap[0]:
                        heapreplace(heap, result)
                    local["heap_peak"] = max(local["heap_peak"], len(heap))
        if count:
            assert previous_hi == n and index.read(1) == b"", "sidecar trailing data"
    assert len(heap) == count, "incomplete result"
    with TemporaryDirectory(dir=snapshot.root, prefix="query-") as temporary:
        arena = reference.Arena(Path(temporary), disk_cap)

        def iterate_complete_output_records():
            for rank, result in enumerate(sorted(heap, reverse=True)):
                if fail_after is not None and rank == fail_after:
                    raise OSError("injected failed sink")
                yield result[2:]

        stage, final = arena.root / "staged", arena.root / "published"
        assert arena.write(stage, reference.OUTPUT, iterate_complete_output_records()) == count
        stage.replace(final)
        local["published"] = 1
        rows = list(reference.read_fixed_file_records(final, reference.OUTPUT, "output_check"))
        arena.remove(final)
        assert arena.live == 0
    local["query_seconds"] = perf_counter() - began
    return rows, local
