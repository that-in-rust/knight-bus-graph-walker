"""Native clique DFS with prefix discovery and bounded external stack frames.

The provider is a trusted immutable prepared view, not part of the RAM claim.
No vertex-wide discovery/parent arrays or retained output live in this executor.
"""

from array import array
from pathlib import Path
from struct import Struct


FRAME = Struct("<QQ")
MAX_U64 = (1 << 64) - 1


class BoundedDiskFrameStack:
    """Two-block hysteresis prevents LIFO boundary thrashing; frames are 16 bytes."""

    def __init__(self, path, block_frames):
        if type(block_frames) is not int or block_frames < 1:
            raise ValueError("block_frames must be a positive integer")
        self.path = Path(path)
        self.block_frames = block_frames
        self.chunk_bytes = block_frames * FRAME.size
        self.buffer = bytearray(2 * self.chunk_bytes)
        self.resident = 0
        self.disk_frames = 0
        self.file = None
        self.resident_frame_peak = 0
        self.max_depth = 0
        self.bytes_written = 0
        self.bytes_read = 0
        self.peak_disk_bytes = 0

    def __enter__(self):
        self.file = self.path.open("x+b", buffering=0)
        return self

    def __exit__(self, *_):
        if self.file is not None:
            try:
                self.file.close()
            finally:
                self.path.unlink(missing_ok=True)
        return False

    @property
    def depth(self):
        return self.disk_frames + self.resident

    def push_vertex_frame_record(self, vertex, occurrence):
        if self.resident == 2 * self.block_frames:
            self.file.seek(self.disk_frames * FRAME.size)
            written = self.file.write(memoryview(self.buffer)[:self.chunk_bytes])
            if written != self.chunk_bytes:
                raise OSError("short DFS stack write")
            self.bytes_written += written
            self.disk_frames += self.block_frames
            self.peak_disk_bytes = max(self.peak_disk_bytes, self.disk_frames * FRAME.size)
            view = memoryview(self.buffer)
            view[:self.chunk_bytes] = view[self.chunk_bytes:2 * self.chunk_bytes]
            self.resident = self.block_frames
        FRAME.pack_into(self.buffer, self.resident * FRAME.size, vertex, occurrence)
        self.resident += 1
        self.resident_frame_peak = max(self.resident_frame_peak, self.resident)
        self.max_depth = max(self.max_depth, self.depth)

    def peek_vertex_frame_record(self):
        if not self.resident:
            raise IndexError("empty DFS stack")
        return FRAME.unpack_from(self.buffer, (self.resident - 1) * FRAME.size)

    def replace_vertex_frame_record(self, vertex, occurrence):
        if not self.resident:
            raise IndexError("empty DFS stack")
        FRAME.pack_into(self.buffer, (self.resident - 1) * FRAME.size, vertex, occurrence)

    def pop_vertex_frame_record(self):
        result = self.peek_vertex_frame_record()
        self.resident -= 1
        if not self.resident and self.disk_frames:
            self.disk_frames -= self.block_frames
            self.file.seek(self.disk_frames * FRAME.size)
            read = self.file.readinto(memoryview(self.buffer)[:self.chunk_bytes])
            if read != self.chunk_bytes:
                raise OSError("short DFS stack read")
            self.bytes_read += read
            self.file.truncate(self.disk_frames * FRAME.size)
            self.resident = self.block_frames
        return result


def run_prefix_cursor_dfs(source, emit, stack_path, *, max_factor_slots, block_frames=256):
    """Stream parent records; expose logical work, not an enforced process cap."""
    count = source.factor_count
    if type(count) is not int or count < 0:
        raise ValueError("factor count must be a nonnegative integer")
    if type(max_factor_slots) is not int or count > max_factor_slots:
        raise ValueError("factor reservation exceeded")
    if type(source.vertex_count) is not int or not 0 <= source.vertex_count <= MAX_U64:
        raise ValueError("vertex count does not fit the admitted word")
    if type(block_frames) is not int or block_frames < 1:
        raise ValueError("block_frames must be a positive integer")
    cursors = array("Q", [0]) * count
    sizes = array("Q", (source.factor_size(factor) for factor in range(count)))
    stats = {
        "factor_slots": count,
        "factor_record_reads": 0,
        "membership_record_reads": 0,
        "visited_membership_checks": 0,
        "discovered_vertices": 0,
        "isolates": 0,
    }

    def read_counted_occurrence_record(vertex, index):
        record = source.read_vertex_occurrence(vertex, index)
        stats["membership_record_reads"] += 1
        return record

    def check_vertex_prefix_seen(vertex):
        index = 0
        while True:
            occurrence = read_counted_occurrence_record(vertex, index)
            if occurrence is None:
                return False
            factor, position = occurrence
            stats["visited_membership_checks"] += 1
            if position < cursors[factor]:
                return True
            index += 1

    def consume_factor_candidate_record(factor):
        vertex = source.read_factor_vertex(factor, cursors[factor])
        stats["factor_record_reads"] += 1
        seen = check_vertex_prefix_seen(vertex)
        # Advancing first would turn every first occurrence into a false hit.
        cursors[factor] += 1
        return vertex, seen

    with BoundedDiskFrameStack(stack_path, block_frames) as stack:
        for root_factor in range(count):
            while cursors[root_factor] < sizes[root_factor]:
                root, seen = consume_factor_candidate_record(root_factor)
                if seen:
                    continue
                emit(root, None)
                stats["discovered_vertices"] += 1
                stack.push_vertex_frame_record(root, 0)
                while stack.depth:
                    vertex, index = stack.peek_vertex_frame_record()
                    occurrence = read_counted_occurrence_record(vertex, index)
                    if occurrence is None:
                        stack.pop_vertex_frame_record()
                        continue
                    factor, _ = occurrence
                    if cursors[factor] == sizes[factor]:
                        stack.replace_vertex_frame_record(vertex, index + 1)
                        continue
                    neighbor, seen = consume_factor_candidate_record(factor)
                    if not seen:
                        emit(neighbor, vertex)
                        stats["discovered_vertices"] += 1
                        stack.push_vertex_frame_record(neighbor, 0)
        for vertex in source.iterate_complete_vertices():
            if read_counted_occurrence_record(vertex, 0) is None:
                emit(vertex, None)
                stats["discovered_vertices"] += 1
                stats["isolates"] += 1
        stats.update({
            "resident_frame_peak": stack.resident_frame_peak,
            "frame_buffer_bytes": len(stack.buffer),
            "max_stack_depth": stack.max_depth,
            "stack_bytes_written": stack.bytes_written,
            "stack_bytes_read": stack.bytes_read,
            "peak_stack_disk_bytes": stack.peak_disk_bytes,
        })
    if stats["discovered_vertices"] != source.vertex_count:
        raise ValueError("prepared source vertex count is inconsistent")
    return stats
