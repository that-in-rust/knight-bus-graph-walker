"""Forward-only exact execution for the declared matching-defect graph family.

Inputs are TRUSTED, previously validated perfect-matching mate records in
original-ID order. Local shape checks do not certify reciprocal matching or
prove that an arbitrary raw graph equals the declared weighted template.
Callbacks must stream their outputs to preserve the workspace bound.
"""

from pathlib import Path
import shutil
import struct


def validate_matching_frontier_header(b, crossing_pairs):
    if type(b) is not int or b < 16 or b % 8 or 4 * b >= 2 ** 64:
        raise ValueError("b must be a supported multiple of eight, at least sixteen")
    k = 3 * b // 4 - 1
    if type(crossing_pairs) is not int or crossing_pairs < 1 or crossing_pairs % 2 != 1 or crossing_pairs > min(k, b - k):
        raise ValueError("invalid crossing-pair count")
    return k


def run_matching_frontier_streams(b, crossing_pairs, low_mates, high_mates, emit_low, emit_high, emit_move):
    """Emit Y labels by two ordered streams and all accepted original-ID moves.

Only two cursor records and constant scalar counters are retained here. This
is not a whole-process RAM cap, input validator, or general Louvain routine.
"""
    k = validate_matching_frontier_header(b, crossing_pairs)
    streams, emitters = (iter(low_mates), iter(high_mates)), (emit_low, emit_high)
    cursors, ends, crossing_reads = [0, k], (k, b), [0, 0]
    p, q = 1, 3 * b + 1
    for vertex in range(2 * b):
        if vertex != 1:
            emit_move(1, vertex, p)
    for vertex in range(2 * b, 3 * b):
        emit_move(1, vertex, p)
    for vertex in range(3 * b, 4 * b):
        if vertex != q:
            emit_move(1, vertex, q)
    for vertex in range(k):
        emit_move(2, 2 * b + vertex, q)

    sweep, completed = 2, 0

    def consume_matching_frontier_record(side, allow_move):
        vertex = cursors[side]
        try:
            mate = next(streams[side])
        except StopIteration as error:
            raise ValueError("truncated mate stream") from error
        if type(mate) is not int or not 0 <= mate < b or mate == vertex:
            raise ValueError("invalid mate ID")
        cursors[side] += 1
        crossing = (vertex < k) != (mate < k)
        moved = False
        if crossing:
            crossing_reads[side] += 1
            moved = mate >= cursors[1 - side]
            if moved and not allow_move:
                raise ValueError("remaining live pair contradicts supplied count")
            label = (p if side == 0 else q) if moved else (q if side == 0 else p)
            if moved:
                emit_move(sweep, 2 * b + vertex, label)
        else:
            label = q if side == 0 else p
        emitters[side](label)
        return moved

    side = 1
    while completed < crossing_pairs:
        if side == 0:
            sweep += 1
        accepted = 0
        while accepted < 2 and completed < crossing_pairs:
            if cursors[side] == ends[side]:
                raise ValueError("mate stream exhausted before required completions")
            if consume_matching_frontier_record(side, True):
                accepted += 1
                completed += 1
        side = 1 - side

    for side in (0, 1):
        while cursors[side] < ends[side]:
            consume_matching_frontier_record(side, False)
        try:
            next(streams[side])
        except StopIteration:
            pass
        else:
            raise ValueError("trailing mate records")
        if crossing_reads[side] != crossing_pairs:
            raise ValueError("crossing count does not match mate stream")

    return {
        "vertices": 4 * b,
        "forced_prefix": k,
        "crossing_pairs": crossing_pairs,
        "completion_moves": completed,
        "accepted_moves": 4 * b - 2 + k + completed,
        "sweeps": sweep + 1,
        "mate_records_read": b,
        "label_records_written": b,
    }


def iterate_packed_mate_records(handle, count, buffer_bytes):
    remaining = count
    while remaining:
        amount = min(remaining, buffer_bytes // 8)
        block = handle.read(8 * amount)
        if len(block) != 8 * amount:
            raise ValueError("truncated packed mate input")
        for (mate,) in struct.iter_unpack("<Q", block):
            yield mate
        remaining -= amount


def write_repeated_label_records(handle, label, count, buffer_bytes):
    capacity = buffer_bytes // 8
    block = struct.pack("<Q", label) * min(count, capacity)
    while count:
        amount = min(count, capacity)
        handle.write(block[:8 * amount])
        count -= amount


def run_matching_frontier_files(mate_path, output_directory, b, crossing_pairs, *, buffer_bytes=65536):
    """Run the trusted-input kernel with fixed buffers and sequential outputs.

Only initial seeks split the immutable mate file; no per-pair random lookup.
Errors may leave a partial output directory: production atomic publication,
quota enforcement, durability and a reciprocal-matching builder are absent.
"""
    k = validate_matching_frontier_header(b, crossing_pairs)
    if type(buffer_bytes) is not int or buffer_bytes < 8 or buffer_bytes % 8:
        raise ValueError("buffer_bytes must be a positive multiple of eight")
    mate_path, directory = Path(mate_path), Path(output_directory)
    if mate_path.stat().st_size != 8 * b:
        raise ValueError("packed input size does not match b")
    directory.mkdir()
    word, event = struct.Struct("<Q"), struct.Struct("<QQQ")
    low_path, high_path = directory / "low.bin", directory / "high.bin"
    with mate_path.open("rb", buffering=0) as low_input, mate_path.open("rb", buffering=0) as high_input, low_path.open("wb", buffering=buffer_bytes) as low_output, high_path.open("wb", buffering=buffer_bytes) as high_output, (directory / "moves.bin").open("wb", buffering=buffer_bytes) as move_output:
        high_input.seek(8 * k)
        receipt = run_matching_frontier_streams(
            b, crossing_pairs,
            iterate_packed_mate_records(low_input, k, buffer_bytes),
            iterate_packed_mate_records(high_input, b - k, buffer_bytes),
            lambda label: low_output.write(word.pack(label)),
            lambda label: high_output.write(word.pack(label)),
            lambda *move: move_output.write(event.pack(*move)),
        )
    with (directory / "labels.bin").open("wb", buffering=buffer_bytes) as output:
        write_repeated_label_records(output, 1, 2 * b, buffer_bytes)
        for part in (low_path, high_path):
            with part.open("rb", buffering=0) as source:
                shutil.copyfileobj(source, output, length=buffer_bytes)
        write_repeated_label_records(output, 3 * b + 1, b, buffer_bytes)
    low_path.unlink()
    high_path.unlink()
    moves_bytes = 24 * receipt["accepted_moves"]
    receipt.update({
        "buffer_bytes": buffer_bytes,
        "source_bytes_read": 8 * b,
        "scratch_bytes_written": 8 * b,
        "scratch_bytes_read": 8 * b,
        "retained_output_bytes": 32 * b + moves_bytes,
        "peak_owned_bytes": 48 * b + moves_bytes,
        "logical_read_write_bytes": 56 * b + moves_bytes,
    })
    return receipt
