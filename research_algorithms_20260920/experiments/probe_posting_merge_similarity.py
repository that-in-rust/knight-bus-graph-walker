"""Ordinary bounded DAAT control; no full-population overlap dictionary."""

from collections import Counter
from contextlib import closing
from fractions import Fraction
from heapq import heappush, heapreplace, merge
from itertools import groupby
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter


def select_posting_merge_answers(reference, snapshot, source, sid, k, merge_cap=128,
                                 heap_cap=100000, disk_cap=10**8, output_cap=10**6,
                                 fail_after=None):
    began = perf_counter()
    assert snapshot.token == snapshot.post_token == snapshot.meta_token == snapshot.inverse_token
    assert k >= 0 and merge_cap >= 0
    selfpos = snapshot.self_position(sid)
    count = min(k, snapshot.n - int(selfpos is not None))
    local = Counter(k=count, output_bytes=count * reference.OUTPUT.size)
    assert count <= heap_cap, "heap admission"
    assert local["output_bytes"] <= output_cap and local["output_bytes"] <= disk_cap
    heap = []
    if count:
        previous, a, streams, intervals = None, 0, 0, 0
        for (feature,) in reference.read_fixed_file_records(source, reference.U, "daat_source"):
            assert previous is None or previous < feature
            previous, a = feature, a + 1
            entry = snapshot.feature(feature)
            if entry is not None and entry[2]:
                streams += 1
                intervals += entry[2]
        assert streams <= merge_cap, "merge admission"
        local.update(a=a, merge_streams=streams, L=intervals)

        def iterate_expanded_posting_positions(entry):
            end_previous = -1
            records = reference.read_fixed_file_records(snapshot.posts, reference.INTERVAL,
                                                         "daat_intervals", entry[1], entry[2])
            with closing(records):
                for lo, hi in records:
                    assert 0 <= lo < hi <= snapshot.n and lo > end_previous
                    end_previous = hi
                    yield from range(lo, hi)

        cursors = []
        for (feature,) in reference.read_fixed_file_records(source, reference.U, "daat_source"):
            entry = snapshot.feature(feature)
            if entry is not None and entry[2]:
                cursors.append(iterate_expanded_posting_positions(entry))
        assert len(cursors) == streams
        ordered = merge(*cursors)
        try:
            for position, occurrences in groupby(ordered):
                overlap = sum(1 for _ in occurrences)
                local["membership_visits"] += overlap
                if position == selfpos:
                    continue
                node, size = snapshot.metadata(position)
                assert 0 < overlap <= min(a, size)
                local["candidate_targets"] += 1
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
            records = reference.read_fixed_file_records(snapshot.inverse, reference.INVERSE, "daat_zero_ids")
            with closing(records):
                for node, position in records:
                    local["zero_id_visits"] += 1
                    if node == sid or node in positive_ids:
                        continue
                    actual, size = snapshot.metadata(position)
                    assert actual == node
                    heappush(heap, (Fraction(0), -node, node, a, size, 0))
                    local["zero_rows"] += 1
                    local["heap_peak"] = max(local["heap_peak"], len(heap))
                    if len(heap) == count:
                        break
        assert len(heap) == count, "incomplete result"

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
