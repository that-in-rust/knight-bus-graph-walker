"""Finite falsifiers for prefix-encoded discovery; fixture RAM is not kernel RAM."""

import importlib
import itertools
import random
import tempfile
import unittest
from pathlib import Path


class PreparedCliqueFixtureSource:
    def __init__(self, vertices, factors):
        self.vertices = tuple(vertices)
        self.factors = tuple(tuple(row) for row in factors)
        self.vertex_count = len(self.vertices)
        self.factor_count = len(self.factors)
        self.occurrences = {vertex: [] for vertex in vertices}
        for factor, row in enumerate(self.factors):
            for position, vertex in enumerate(row):
                self.occurrences[vertex].append((factor, position))

    def factor_size(self, factor):
        return len(self.factors[factor])

    def read_factor_vertex(self, factor, position):
        return self.factors[factor][position]

    def read_vertex_occurrence(self, vertex, position):
        row = self.occurrences[vertex]
        return row[position] if position < len(row) else None

    def iterate_complete_vertices(self):
        return iter(self.vertices)


class GeneratedSingleCliqueSource:
    def __init__(self, count):
        self.vertex_count = count
        self.factor_count = 1

    def factor_size(self, factor):
        return self.vertex_count

    def read_factor_vertex(self, factor, position):
        return self.vertex_count - 1 - position

    def read_vertex_occurrence(self, vertex, position):
        return (0, self.vertex_count - 1 - vertex) if position == 0 else None

    def iterate_complete_vertices(self):
        return iter(range(self.vertex_count))


def validate_emitted_forest_exact(test, source, records):
    test.assertEqual(len(records), source.vertex_count)
    parents = dict(records)
    test.assertEqual(set(parents), set(source.vertices))
    adjacency = {v: set() for v in source.vertices}
    for row in source.factors:
        for u, v in itertools.combinations(row, 2):
            adjacency[u].add(v)
            adjacency[v].add(u)
    ancestors = {}
    roots = {}
    for vertex in source.vertices:
        chain = set()
        current = vertex
        while parents[current] is not None:
            test.assertNotIn(current, chain)
            chain.add(current)
            parent = parents[current]
            test.assertIn(parent, adjacency[current])
            current = parent
        ancestors[vertex] = chain | {current}
        roots[vertex] = current
    for vertex, neighbors in adjacency.items():
        for neighbor in neighbors:
            test.assertEqual(roots[vertex], roots[neighbor])
            test.assertTrue(vertex in ancestors[neighbor] or neighbor in ancestors[vertex])
    for root in set(roots.values()):
        reached = {root}
        pending = [root]
        while pending:
            current = pending.pop()
            for neighbor in adjacency[current] - reached:
                reached.add(neighbor)
                pending.append(neighbor)
        test.assertEqual(reached, {v for v in roots if roots[v] == root})
    # The stronger covered-edit DFS control must hold for every tiny cover.
    degree = dict.fromkeys(source.vertices, 0)
    for vertex, parent in records:
        if parent is not None:
            degree[vertex] += 1
            degree[parent] += 1
    if len(source.vertices) <= 4:
        for mask in range(1 << len(source.vertices)):
            cover = [v for i, v in enumerate(source.vertices) if mask >> i & 1]
            test.assertLessEqual(sum(degree[v] for v in cover), 2 * len(cover) + source.factor_count)


class PrefixCursorDfsTests(unittest.TestCase):
    def setUp(self):
        try:
            self.module = importlib.import_module("probe_prefix_cursor_dfs")
        except ModuleNotFoundError:
            self.fail("prefix-cursor DFS executor has not been implemented")

    def execute_fixture_forest_exact(self, source, block=2):
        records = []
        with tempfile.TemporaryDirectory() as directory:
            stats = self.module.run_prefix_cursor_dfs(
                source, lambda vertex, parent: records.append((vertex, parent)),
                Path(directory) / "frames.bin", max_factor_slots=source.factor_count,
                block_frames=block,
            )
            self.assertFalse((Path(directory) / "frames.bin").exists())
        validate_emitted_forest_exact(self, source, records)
        total = sum(map(len, source.factors))
        self.assertEqual(stats["factor_record_reads"], total)
        self.assertLessEqual(stats["visited_membership_checks"], sum(len(row) ** 2 for row in source.occurrences.values()))
        self.assertEqual(stats["discovered_vertices"], source.vertex_count)
        self.assertLessEqual(stats["resident_frame_peak"], 2 * block)
        return records, stats

    def test_exhaustive_incidence_forests(self):
        vertices = [9, 2, 17, 0]
        for bits in range(256):
            factors = [[v for i, v in enumerate(vertices) if bits >> (4 * f + i) & 1] for f in range(2)]
            self.execute_fixture_forest_exact(PreparedCliqueFixtureSource(vertices, factors))

    def test_seeded_overlap_forests(self):
        rng = random.Random(20260921)
        for _ in range(500):
            vertices = rng.sample(range(1000), rng.randrange(1, 15))
            factors = []
            for _ in range(rng.randrange(12)):
                row = rng.sample(vertices, rng.randrange(len(vertices) + 1))
                factors.append(row)
            self.execute_fixture_forest_exact(PreparedCliqueFixtureSource(vertices, factors), block=3)

    def test_isolates_and_duplicates(self):
        self.execute_fixture_forest_exact(PreparedCliqueFixtureSource([], []))
        records, _ = self.execute_fixture_forest_exact(PreparedCliqueFixtureSource([90, 7, 1, 5], [[], [90, 7], [7, 90], [1]]))
        self.assertIn((5, None), records)
        self.assertEqual(sum(parent is None for _, parent in records), 3)

    def test_generated_deep_clique(self):
        for count in [8, 10000]:
            emitted = 0

            def consume_generated_parent_exact(vertex, parent):
                nonlocal emitted
                self.assertEqual(vertex, count - 1 - emitted)
                self.assertEqual(parent, None if emitted == 0 else vertex + 1)
                emitted += 1

            with tempfile.TemporaryDirectory() as directory:
                stats = self.module.run_prefix_cursor_dfs(
                    GeneratedSingleCliqueSource(count), consume_generated_parent_exact,
                    Path(directory) / "frames.bin", max_factor_slots=1, block_frames=4,
                )
            self.assertEqual(emitted, count)
            self.assertEqual(stats["factor_record_reads"], count)
            self.assertEqual(stats["visited_membership_checks"], count)
            self.assertEqual(stats["max_stack_depth"], count)
            self.assertLessEqual(stats["resident_frame_peak"], 8)
            self.assertLessEqual(stats["stack_bytes_written"], 16 * count)
            self.assertEqual(stats["stack_bytes_written"], stats["stack_bytes_read"])

    def test_stack_boundary_operations(self):
        rng = random.Random(71)
        for block in [1, 2, 7]:
            with tempfile.TemporaryDirectory() as directory:
                stack = self.module.BoundedDiskFrameStack(Path(directory) / "frames.bin", block)
                expected = []
                with stack:
                    for _ in range(5000):
                        if not expected or rng.randrange(3):
                            record = (rng.randrange(10000), rng.randrange(1000))
                            stack.push_vertex_frame_record(*record)
                            expected.append(record)
                        else:
                            self.assertEqual(stack.pop_vertex_frame_record(), expected.pop())
                        if expected:
                            vertex, index = expected[-1]
                            stack.replace_vertex_frame_record(vertex, index + 1)
                            expected[-1] = (vertex, index + 1)
                            self.assertEqual(stack.peek_vertex_frame_record(), expected[-1])
                        self.assertEqual(stack.depth, len(expected))
                    while expected:
                        self.assertEqual(stack.pop_vertex_frame_record(), expected.pop())
                self.assertLessEqual(stack.resident_frame_peak, 2 * block)
                self.assertEqual(stack.bytes_written, stack.bytes_read)

    def test_admission_before_source_calls(self):
        class RefusedSource:
            factor_count = 2
            vertex_count = 0

            def __getattr__(self, name):
                raise AssertionError("source called before reservation: " + name)

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "factor"):
                self.module.run_prefix_cursor_dfs(RefusedSource(), lambda *_: None, Path(directory) / "frames.bin", max_factor_slots=1)
            self.assertFalse((Path(directory) / "frames.bin").exists())

    def test_stack_failure_cleans_scratch(self):
        def fail_output_callback_exact(*_):
            raise RuntimeError("output failed")

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "frames.bin"
            with self.assertRaisesRegex(RuntimeError, "output failed"):
                self.module.run_prefix_cursor_dfs(GeneratedSingleCliqueSource(5), fail_output_callback_exact, path, max_factor_slots=1)
            self.assertFalse(path.exists())

    def test_buffer_preflight_order(self):
        class HeaderOnlySource:
            factor_count = 1
            vertex_count = 0

            def factor_size(self, factor):
                raise AssertionError("source read before block validation")

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "frames.bin"
            for invalid in (0, -1, True, 1.5, None):
                with self.assertRaisesRegex(ValueError, "block_frames"):
                    self.module.run_prefix_cursor_dfs(HeaderOnlySource(), lambda *_: None, path, max_factor_slots=1, block_frames=invalid)
                self.assertFalse(path.exists())

    def test_sharp_probe_amplification(self):
        for degree in (3, 4, 8, 32, 128):
            factors = [(0,)]
            factors.extend((0, index, index + 1) for index in range(1, degree - 2))
            factors.extend(((0, degree - 2), (1, 0)))
            source = PreparedCliqueFixtureSource(tuple(range(degree - 1)), factors)
            source.occurrences[0].sort(reverse=True)
            for vertex in range(1, degree - 1):
                source.occurrences[vertex].sort(key=lambda item: item[0] != vertex)
            _, stats = self.execute_fixture_forest_exact(source)
            incidences = 3 * degree - 4
            expected = (degree * degree + 11 * degree - 18) // 2
            self.assertEqual(stats["visited_membership_checks"], expected)
            self.assertEqual(stats["factor_record_reads"], incidences)
            self.assertEqual(stats["membership_record_reads"], expected + 2 * incidences + 3 * (degree - 1) - 1)


if __name__ == "__main__":
    unittest.main()
