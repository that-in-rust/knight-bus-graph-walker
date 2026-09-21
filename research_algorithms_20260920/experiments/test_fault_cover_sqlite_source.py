"""Focused provider tests; deliberately do not run the connectivity engine."""

import importlib.util
from itertools import combinations
from pathlib import Path
import random
import sqlite3
import tempfile
import unittest
from unittest.mock import patch


PROVIDER_PATH = Path(__file__).with_name("fault_cover_sqlite_source.py")


class SqlitePreparedSourceTests(unittest.TestCase):
    def load_sqlite_provider_module(self):
        self.assertTrue(PROVIDER_PATH.exists(), "SQLite prepared provider is missing")
        spec = importlib.util.spec_from_file_location("fault_cover_sqlite_source", PROVIDER_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def build_small_source_fixture(self, module, destination):
        return module.build_sqlite_fault_source(
            destination, iter((99, 9, 2, 7, 5, 0)),
            (iter(group) for group in ((7, 0, 2), (9, 2, 5), (), (7, 0, 2))),
            iter(((2, 9), (0, 7))), iter(((7, 9), (0, 99))), iter((9, 0)),
            cache_kib=64, batch_records=2,
        )

    def test_complete_source_rows(self):
        module = self.load_sqlite_provider_module()
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "graph.sqlite"
            receipt = self.build_small_source_fixture(module, destination)
            with module.SqlitePreparedFaultSource(destination, cache_kib=64) as source:
                self.assertEqual((source.vertex_count, source.factor_count), (6, 4))
                self.assertEqual(source.snapshot_id, receipt["snapshot_id"])
                expected = [(0, [0, 3], [99]), (2, [0, 1, 3], []),
                            (5, [1], []), (7, [0, 3], [9]), (9, [1], [7]), (99, [], [0])]
                for _ in range(2):
                    actual = [(vertex, list(memberships), list(inserted))
                              for vertex, memberships, inserted in source.iterate_complete_vertex_rows()]
                    self.assertEqual(actual, expected)
                self.assertEqual(list(source.iterate_complete_edit_edges()),
                                 [(0, 7), (0, 99), (2, 9), (7, 9)])
                self.assertEqual(list(source.iterate_vertex_factor_memberships(2)), [0, 1, 3])
                self.assertEqual(list(source.iterate_vertex_deleted_neighbors(9)), [2])
                self.assertEqual(list(source.iterate_vertex_deleted_neighbors(7)), [0])
                self.assertEqual(list(source.iterate_factor_cover_members(1)), [9])
                self.assertEqual(list(source.iterate_factor_cover_members(2)), [])
                self.assertEqual(list(source.iterate_inserted_cover_edges()), [])
                self.assertEqual(source.events["active_cursors"], 0)
                self.assertEqual(receipt["database_bytes"], destination.stat().st_size)
                self.assertGreater(receipt["elapsed_seconds"], 0)
                self.assertEqual(receipt["source_events"], dict(
                    original_vertices=6, factors=4, memberships=9,
                    deleted_pairs=2, added_pairs=2, cover_vertices=2))

    def test_prefix_degree_replay(self):
        module = self.load_sqlite_provider_module()
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "graph.sqlite"
            module.build_sqlite_fault_source(destination, range(200), (range(200),),
                                            ((0, vertex) for vertex in range(1, 200)), cover=(0,))
            with module.SqlitePreparedFaultSource(destination, cache_kib=32) as source:
                statements = []
                source._connection.set_trace_callback(statements.append)
                before = source.events["record_reads"]
                row = source.open_cover_deletion_row(source.snapshot_id, 0)
                self.assertEqual(row.length, 199)
                with self.assertRaises(AttributeError):
                    row.length = 1
                self.assertEqual([row.read_next_neighbor_exact() for _ in range(3)], [1, 2, 3])
                row.close_current_cursor_exact()
                row.close_current_cursor_exact()
                self.assertEqual(source.events["record_reads"] - before, 3)
                self.assertEqual(source.events["active_cursors"], 0)
                with self.assertRaises(StopIteration):
                    row.read_next_neighbor_exact()
                self.assertFalse(any("COUNT(" in statement.upper() for statement in statements))
                self.assertEqual(list(source.iterate_vertex_deleted_neighbors(0)), list(range(1, 200)))

    def test_readonly_snapshot_replay(self):
        module = self.load_sqlite_provider_module()
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "special ?#.sqlite"
            self.build_small_source_fixture(module, destination)
            original = destination.read_bytes()
            with module.SqlitePreparedFaultSource(destination, cache_kib=48) as source:
                snapshot = source.snapshot_id
                self.assertEqual(source._connection.execute("PRAGMA query_only").fetchone()[0], 1)
                self.assertEqual(source._connection.execute("PRAGMA cache_size").fetchone()[0], -48)
                self.assertEqual(source._connection.execute("PRAGMA temp_store").fetchone()[0], 1)
                self.assertEqual(source._connection.execute("PRAGMA mmap_size").fetchone()[0], 0)
                with self.assertRaises(sqlite3.OperationalError):
                    source._connection.execute("CREATE TABLE forbidden(value)")
                source._connection.execute("PRAGMA query_only=OFF").close()
                with self.assertRaises(sqlite3.OperationalError):
                    source._connection.execute("UPDATE vertices SET deleted_degree=0")
                source._connection.execute("PRAGMA query_only=ON").close()
                before = dict(source.events)
                for opener, identifier in ((source.open_cover_deletion_row, 0),
                                            (source.open_factor_core_posting, 0)):
                    with self.assertRaisesRegex(ValueError, "snapshot"):
                        opener("stale", identifier)
                self.assertEqual(source.events, before)
                row = source.open_factor_core_posting(snapshot, 0)
                self.assertEqual(row.length, 1)
                self.assertEqual(row.read_next_neighbor_exact(), 0)
                with self.assertRaises(StopIteration):
                    row.read_next_neighbor_exact()
                self.assertEqual(source.events["active_cursors"], 0)
            with module.SqlitePreparedFaultSource(destination, cache_kib=48) as reopened:
                self.assertEqual(reopened.snapshot_id, snapshot)
                self.assertEqual(list(reopened.iterate_vertex_deleted_neighbors(0)), [7])
                self.assertEqual(reopened.preparation_metadata["source_events"]["memberships"], 9)
            self.assertEqual(destination.read_bytes(), original)

    def test_legacy_cursor_cleanup(self):
        module = self.load_sqlite_provider_module()
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "graph.sqlite"
            self.build_small_source_fixture(module, destination)
            with module.SqlitePreparedFaultSource(destination, cache_kib=64) as source:
                for method, arguments in ((source.iterate_complete_edit_edges, ()),
                                          (source.iterate_vertex_factor_memberships, (2,)),
                                          (source.iterate_vertex_deleted_neighbors, (0,)),
                                          (source.iterate_factor_cover_members, (0,))):
                    iterator = method(*arguments)
                    next(iterator)
                    iterator.close()
                    self.assertEqual(source.events["active_cursors"], 0)
                rows = source.iterate_complete_vertex_rows()
                vertex, memberships, inserted = next(rows)
                self.assertEqual(vertex, 0)
                next(memberships)
                rows.close()
                self.assertEqual(source.events["active_cursors"], 0)
                self.assertEqual(list(memberships), [])
                self.assertEqual(list(inserted), [])

    def test_single_record_fetches(self):
        module = self.load_sqlite_provider_module()
        fetches = [0]

        class TrackingSqliteCursorFixture(sqlite3.Cursor):
            def fetchone(self):
                fetches[0] += 1
                return super().fetchone()

            def fetchmany(self, *args):
                raise AssertionError("batch fetch forbidden")

            def fetchall(self):
                raise AssertionError("row materialization forbidden")

        class TrackingSqliteConnectionFixture(sqlite3.Connection):
            def execute(self, statement, parameters=()):
                return self.cursor(factory=TrackingSqliteCursorFixture).execute(statement, parameters)

        original_connect = sqlite3.connect
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "graph.sqlite"
            module.build_sqlite_fault_source(destination, range(8), (range(8),),
                                            ((0, vertex) for vertex in range(1, 8)), cover=(0,))
            with patch.object(module.sqlite3, "connect", side_effect=lambda *args, **kwargs:
                              original_connect(*args, **kwargs, factory=TrackingSqliteConnectionFixture)):
                with module.SqlitePreparedFaultSource(destination) as source:
                    before = fetches[0]
                    row = source.open_cover_deletion_row(source.snapshot_id, 0)
                    self.assertEqual(fetches[0] - before, 1, "opening fetches just length metadata")
                    self.assertEqual(row.length, 7)
                    self.assertEqual(row.read_next_neighbor_exact(), 1)
                    self.assertEqual(fetches[0] - before, 2)
                    row.close_current_cursor_exact()
                    self.assertEqual(fetches[0] - before, 2, "closing must not drain")

    def test_cursor_reservation_cleanup(self):
        module = self.load_sqlite_provider_module()
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "graph.sqlite"
            self.build_small_source_fixture(module, destination)
            with module.SqlitePreparedFaultSource(destination, max_open_cursors=2) as source:
                rows = source.iterate_complete_vertex_rows()
                with self.assertRaisesRegex(ValueError, "cursor reservation"):
                    next(rows)
                self.assertEqual(source.events["active_cursors"], 0)
                cursor = source.open_cover_deletion_row(source.snapshot_id, 0)
                source.close()
                source.close()
                with self.assertRaises(StopIteration):
                    cursor.read_next_neighbor_exact()
                with self.assertRaisesRegex(ValueError, "closed"):
                    source.iterate_complete_edit_edges()

    def test_empty_boundary_ids(self):
        module = self.load_sqlite_provider_module()
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "empty.sqlite"
            module.build_sqlite_fault_source(destination, (), ((),))
            with module.SqlitePreparedFaultSource(destination) as source:
                self.assertEqual(list(source.iterate_complete_vertex_rows()), [])
                self.assertEqual(list(source.iterate_complete_edit_edges()), [])
                self.assertEqual(source.open_factor_core_posting(source.snapshot_id, 0).length, 0)
                with self.assertRaisesRegex(ValueError, "unknown"):
                    source.open_cover_deletion_row(source.snapshot_id, 0)
                with self.assertRaisesRegex(ValueError, "unknown"):
                    source.open_factor_core_posting(source.snapshot_id, 1)
                for identifier in (True, -1, 2**63, 0.0):
                    with self.assertRaises(ValueError):
                        source.iterate_vertex_factor_memberships(identifier)
            destination = Path(directory) / "isolated.sqlite"
            module.build_sqlite_fault_source(destination, (2**63 - 1, 0, 19), (),
                                            added=((0, 2**63 - 1),), cover=(0, 2**63 - 1))
            with module.SqlitePreparedFaultSource(destination) as source:
                self.assertEqual(list(source.iterate_inserted_cover_edges()), [(0, 2**63 - 1)])
                self.assertEqual(list(source.iterate_vertex_deleted_neighbors(2**63 - 1)), [])
                self.assertEqual(list(source.iterate_vertex_factor_memberships(0)), [])
                self.assertEqual([(vertex, list(memberships), list(inserted))
                                  for vertex, memberships, inserted in source.iterate_complete_vertex_rows()],
                                 [(0, [], [2**63 - 1]), (19, [], []), (2**63 - 1, [], [0])])

    def test_invalid_options_cleanup(self):
        module = self.load_sqlite_provider_module()
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "graph.sqlite"
            for name in ("cache_kib", "batch_records"):
                for value in (0, -1, True, 1.5, 2**31):
                    with self.assertRaises(ValueError):
                        module.build_sqlite_fault_source(destination, (), (), **{name: value})
                    self.assertEqual(list(Path(directory).iterdir()), [])
            destination.symlink_to(Path(directory) / "missing.sqlite")
            with self.assertRaises(FileExistsError):
                module.build_sqlite_fault_source(destination, (), ())
            self.assertTrue(destination.is_symlink())

    def test_invalid_normalization_rejection(self):
        module = self.load_sqlite_provider_module()
        baseline = dict(original_vertices=(0, 1, 2), groups=((0, 1),), cover=(0, 1))
        cases = [dict(original_vertices=(0, 0)), dict(original_vertices=(-1,)),
                 dict(original_vertices=(2**63,)), dict(original_vertices=(True,)),
                 dict(original_vertices=(1.0,)), dict(groups=((0, 0),)),
                 dict(groups=((0, 99),)), dict(cover=(0, 0)), dict(cover=(99,)),
                 dict(deleted=((1, 0),)), dict(deleted=((0, 0),)),
                 dict(deleted=((0, 2),)), dict(added=((0, 1),)),
                 dict(deleted=((0, 1), (0, 1))), dict(added=((0, 2), (0, 2))),
                 dict(added=((0, 99),)), dict(added=((0, True),)),
                 dict(deleted=((0, 1, 2),)), dict(deleted=((0, 1),), cover=()),
                 dict(deleted=((0, 1),), added=((0, 1),))]
        with tempfile.TemporaryDirectory() as directory:
            for overrides in cases:
                with self.subTest(overrides=overrides):
                    with self.assertRaises(ValueError):
                        module.build_sqlite_fault_source(Path(directory) / "bad.sqlite",
                                                        **(baseline | overrides), batch_records=1)
                    self.assertEqual(list(Path(directory).iterdir()), [])

    def test_failure_preserves_destination(self):
        module = self.load_sqlite_provider_module()
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "graph.sqlite"

            def stream_late_failure_members():
                yield 0
                yield 1
                raise RuntimeError("late input failure")

            with self.assertRaisesRegex(RuntimeError, "late input failure"):
                module.build_sqlite_fault_source(destination, range(2), (stream_late_failure_members(),),
                                                batch_records=1)
            self.assertEqual(list(Path(directory).iterdir()), [])
            destination.write_bytes(b"prior user data")
            with self.assertRaises(FileExistsError):
                module.build_sqlite_fault_source(destination, range(2), ((0, 1),))
            self.assertEqual(destination.read_bytes(), b"prior user data")

    def test_publication_refuses_races(self):
        module = self.load_sqlite_provider_module()
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "graph.sqlite"

            def stream_creating_rival_destination():
                yield iter((0, 1))
                destination.write_bytes(b"concurrent user data")

            with self.assertRaises(FileExistsError):
                module.build_sqlite_fault_source(destination, range(2), stream_creating_rival_destination())
            self.assertEqual(destination.read_bytes(), b"concurrent user data")
            self.assertEqual(list(Path(directory).iterdir()), [destination])

    def test_streamed_clique_storage(self):
        module = self.load_sqlite_provider_module()
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "graph.sqlite"
            size = 10_000
            receipt = module.build_sqlite_fault_source(
                destination, iter(range(size)), (iter(range(size)),),
                ((0, vertex) for vertex in range(1, size)), cover=iter((0,)),
                cache_kib=64, batch_records=257,
            )
            self.assertEqual(receipt["source_events"]["memberships"], size)
            self.assertEqual(receipt["source_events"]["deleted_pairs"], size - 1)
            self.assertEqual(receipt["native_membership_probes"], size - 1)
            self.assertLess(receipt["database_bytes"], 4_000_000)
            with module.SqlitePreparedFaultSource(destination, cache_kib=64) as source:
                row = source.open_cover_deletion_row(source.snapshot_id, 0)
                self.assertEqual(row.length, size - 1)
                self.assertEqual(row.read_next_neighbor_exact(), 1)
                row.close_current_cursor_exact()
                self.assertEqual(source.events["record_reads"], 1)
                self.assertEqual(list(source.iterate_vertex_deleted_neighbors(size - 1)), [0])
                self.assertEqual(list(source.iterate_vertex_factor_memberships(size - 1)), [0])
                for table, expected in (("memberships", size), ("edits", size - 1),
                                        ("neighbors", 2 * (size - 1)), ("core_postings", 1)):
                    self.assertEqual(source._connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0],
                                     expected)

    def test_finalization_failure_cleanup(self):
        module = self.load_sqlite_provider_module()
        execute_statement = module.execute_sqlite_statement_exact

        def inject_manifest_write_failure(connection, statement, parameters=()):
            if "INSERT INTO manifest" in statement:
                raise sqlite3.OperationalError("injected manifest failure")
            return execute_statement(connection, statement, parameters)

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "graph.sqlite"
            with patch.object(module, "execute_sqlite_statement_exact", side_effect=inject_manifest_write_failure):
                with self.assertRaisesRegex(sqlite3.OperationalError, "injected manifest failure"):
                    self.build_small_source_fixture(module, destination)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_small_graph_oracle(self):
        module = self.load_sqlite_provider_module()
        rng = random.Random(920)
        vertices = (0, 3, 11, 19, 41, 99, 2**63 - 1)
        pairs = list(combinations(vertices, 2))
        with tempfile.TemporaryDirectory() as directory:
            for case in range(25):
                # Only the tiny test oracle expands base edges, never the builder.
                groups = [rng.sample(vertices, rng.randrange(8)) for _ in range(rng.randrange(8))]
                base = {edge for group in groups for edge in combinations(sorted(group), 2)}
                cover = set(rng.sample(vertices, rng.randrange(8)))
                eligible = [pair for pair in pairs if cover.intersection(pair)]
                deleted = [pair for pair in eligible if pair in base and rng.random() < 0.5]
                added = [pair for pair in eligible if pair not in base and rng.random() < 0.5]
                destination = Path(directory) / f"graph-{case}.sqlite"
                module.build_sqlite_fault_source(destination, iter(reversed(vertices)),
                                                (iter(group) for group in groups),
                                                iter(deleted), iter(added), iter(cover), batch_records=3)
                with module.SqlitePreparedFaultSource(destination) as source:
                    self.assertEqual(list(source.iterate_complete_edit_edges()), sorted(deleted + added))
                    self.assertEqual(list(source.iterate_inserted_cover_edges()),
                                     [pair for pair in added if set(pair) <= cover])
                    for vertex, memberships, inserted in source.iterate_complete_vertex_rows():
                        expected = [factor for factor, group in enumerate(groups) if vertex in group]
                        self.assertEqual(list(memberships), expected)
                        self.assertEqual(list(source.iterate_vertex_factor_memberships(vertex)), expected)
                        self.assertEqual(list(inserted), sorted(right if vertex == left else left
                                                               for left, right in added if vertex in (left, right)))
                        expected = sorted(right if vertex == left else left
                                          for left, right in deleted if vertex in (left, right))
                        row = source.open_cover_deletion_row(source.snapshot_id, vertex)
                        self.assertEqual(row.length, len(expected))
                        self.assertEqual(list(row), expected)
                    for factor, group in enumerate(groups):
                        row = source.open_factor_core_posting(source.snapshot_id, factor)
                        expected = sorted(set(group).intersection(cover))
                        self.assertEqual(row.length, len(expected))
                        self.assertEqual(list(row), expected)
                    self.assertEqual(source.events["active_cursors"], 0)


if __name__ == "__main__":
    unittest.main()
