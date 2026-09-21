"""Tiny expanded oracles and lifecycle checks, not performance benchmarks."""

from contextlib import closing
from fractions import Fraction
import importlib.util
from itertools import product
import math
from pathlib import Path
import random
import sqlite3
import struct
import sys
import tempfile
import unittest
from unittest.mock import patch


PROVIDER_PATH = Path(__file__).with_name("boolean_rank_sqlite_source.py")


class BooleanRankSourceTests(unittest.TestCase):
    def load_boolean_provider_module(self):
        self.assertTrue(PROVIDER_PATH.exists(), "Boolean SQLite provider is missing")
        spec = importlib.util.spec_from_file_location("boolean_rank_sqlite_source", PROVIDER_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def assert_complete_expanded_oracle(self, module, path, rows, factors):
        receipt = module.build_boolean_rank_source(path, iter(rows), factor_count=factors,
                                                   cache_kib=32, batch_records=2)
        signatures = {identifier: tuple(sorted(set(groups))) for identifier, groups, _ in rows}
        weights = {identifier: weight for identifier, _, weight in rows}
        degrees = {identifier: sum(other != identifier and bool(set(groups) & set(signature))
                                  for other, signature in signatures.items())
                   for identifier, groups in signatures.items()}
        classes = sorted(set(signatures.values()), key=lambda signature:
                         (signature + (-1, -1))[:2])
        with module.SqliteBooleanRankSource(path, cache_kib=32) as source:
            self.assertEqual(source.snapshot_id, receipt["snapshot_id"])
            self.assertEqual((source.vertex_count, source.factor_count, source.class_count),
                             (len(rows), factors, len(classes)))
            expected_records = []
            visited = []
            for class_id, signature in enumerate(classes):
                members = sorted(identifier for identifier in signatures if signatures[identifier] == signature)
                expected_records.append((class_id, signature, len(members), degrees[members[0]],
                                         sum((Fraction.from_float(weights[i]) for i in members), Fraction())))
                self.assertEqual(list(source.iterate_class_vertex_rows(class_id)),
                                 [(identifier, weights[identifier]) for identifier in members])
                visited.extend(members)
            self.assertEqual(sorted(visited), sorted(signatures))
            self.assertEqual(list(source.iterate_class_records()), expected_records)
            self.assertEqual(list(source.iterate_class_records()), expected_records)
            self.assertEqual(source.active_class_count, sum(record[3] > 0 for record in expected_records))
            self.assertEqual(source.total_weight, sum(map(Fraction.from_float, weights.values()), Fraction()))
            self.assertEqual(source.isolate_weight,
                             sum((Fraction.from_float(weights[i]) for i in weights if degrees[i] == 0), Fraction()))
            self.assertEqual(list(source.iterate_active_factor_counts()),
                             [(factor, sum(factor in signatures[i] and degrees[i] > 0 for i in signatures))
                              for factor in range(factors)])
            self.assertEqual(source.events["active_cursors"], 0)
        self.assertEqual(receipt["database_bytes"], path.stat().st_size)
        self.assertGreaterEqual(receipt["elapsed_seconds"], receipt["preparation_seconds"])
        self.assertEqual(receipt["source_events"]["vertices"], len(rows))
        self.assertEqual(receipt["source_events"]["factors"], factors)
        self.assertEqual(receipt["source_events"]["membership_items"], sum(len(row[1]) for row in rows))
        self.assertEqual(receipt["source_events"]["memberships"], sum(map(len, signatures.values())))
        self.assertEqual(receipt["builder_events"]["class_scan_rows"], len(rows))
        self.assertEqual(receipt["builder_events"]["classes_written"], len(classes))

    def test_complete_boolean_oracle(self):
        module = self.load_boolean_provider_module()
        rows = [(99, (1, 0, 1), 0.25), (18, (0, 1), 0.75), (17, (0, 2), 1.0),
                (3, (), 2.0), (8, (4,), 0.0), (6, (5, 6), 3.0), (1, (2,), 4.0)]
        with tempfile.TemporaryDirectory() as directory:
            self.assert_complete_expanded_oracle(module, Path(directory) / "fixture.sqlite", rows, 9)
            with module.SqliteBooleanRankSource(Path(directory) / "fixture.sqlite") as source:
                pair = next(record for record in source.iterate_class_records() if record[1] == (0, 1))
                self.assertEqual(pair[3], 2, "shared two memberships still count one Boolean edge")

    def test_tiny_signature_layouts(self):
        module = self.load_boolean_provider_module()
        signatures = ((), (0,), (1,), (0, 1))
        rng = random.Random(20260921)
        layouts = list(product(signatures, repeat=2))
        layouts.extend(tuple(rng.choice(signatures) for _ in range(7)) for _ in range(8))
        with tempfile.TemporaryDirectory() as directory:
            for case, layout in enumerate(layouts):
                with self.subTest(case=case):
                    rows = [(100 - 3 * i, groups, float(i % 3)) for i, groups in enumerate(layout)]
                    self.assert_complete_expanded_oracle(module, Path(directory) / f"{case}.sqlite", rows, 3)

    def test_exact_dyadic_personalization(self):
        module = self.load_boolean_provider_module()
        tiny = float.fromhex("0x0.0000000000001p-1022")
        huge = sys.float_info.max
        rows = [(0, (0, 1), huge), (1, (0, 1), tiny), (2, (0,), huge),
                (3, (), tiny), (4, (7,), -0.0), (5, (5,), math.nextafter(1.0, 2.0))]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "dyadic.sqlite"
            self.assert_complete_expanded_oracle(module, path, rows, 8)
            with module.SqliteBooleanRankSource(path) as source:
                self.assertGreater(source.total_weight, Fraction.from_float(huge))
                self.assertEqual(sum((Fraction.from_float(w) / source.total_weight
                                      for record in source.iterate_class_records()
                                      for _, w in source.iterate_class_vertex_rows(record[0])), Fraction()), 1)
                result = {i: w for record in source.iterate_class_records()
                          for i, w in source.iterate_class_vertex_rows(record[0])}
                for identifier, _, weight in rows:
                    self.assertEqual(struct.pack(">d", result[identifier]), struct.pack(">d", weight))

    def test_empty_isolate_boundaries(self):
        module = self.load_boolean_provider_module()
        with tempfile.TemporaryDirectory() as directory:
            for number, rows, factors in ((0, [], 0), (1, [], 4),
                                         (2, [(0, (), 1.0), (2**63 - 1, (), 0.0)], 0),
                                         (3, [(7, (0,), 1.0), (8, (1, 2), 3.0)], 4)):
                self.assert_complete_expanded_oracle(module, Path(directory) / f"{number}.sqlite", rows, factors)

    def test_invalid_inputs_cleanup(self):
        module = self.load_boolean_provider_module()
        invalid = [[(i, (), 1.0)] for i in (True, 0.0, "1", -1, -2**63, -2**63 - 1, 2**63)]
        invalid += [[(0, (), w)] for w in (1, True, "1", Fraction(1), float("nan"),
                                         float("inf"), -float("inf"), -1.0)]
        invalid += [[(0, groups, 1.0)] for groups in ((0, 1, 2), (-1,), (3,), (False,), (0.0,), None)]
        invalid += [[(0, (), 1.0), (0, (1,), 2.0)], [(0, (), 0.0)], [(0,)], [None]]
        with tempfile.TemporaryDirectory() as directory:
            for rows in invalid:
                with self.subTest(rows=rows), self.assertRaises(ValueError):
                    module.build_boolean_rank_source(Path(directory) / "bad.sqlite", rows, factor_count=3)
                self.assertEqual(list(Path(directory).iterdir()), [])
            for name, values in (("factor_count", (-1, True, 1.0, 2**63)),
                                 ("cache_kib", (0, -1, True, 1.5, 2**31)),
                                 ("batch_records", (0, -1, True, 1.5, 2**31))):
                for value in values:
                    options = dict(factor_count=0)
                    options[name] = value
                    with self.subTest(option=name, value=value), self.assertRaises(ValueError):
                        module.build_boolean_rank_source(Path(directory) / "bad.sqlite", (), **options)
                    self.assertEqual(list(Path(directory).iterdir()), [])

    def test_stream_failure_cleanup(self):
        module = self.load_boolean_provider_module()

        def generate_failed_input_rows():
            yield 1, (0,), 1.0
            raise RuntimeError("input failed")

        def generate_failed_membership_items():
            yield 0
            raise RuntimeError("membership failed")

        with tempfile.TemporaryDirectory() as directory:
            for rows in (generate_failed_input_rows(), [(0, generate_failed_membership_items(), 1.0)]):
                with self.assertRaises(RuntimeError):
                    module.build_boolean_rank_source(Path(directory) / "bad.sqlite", rows,
                                                     factor_count=1, batch_records=1)
                self.assertEqual(list(Path(directory).iterdir()), [])

    def test_atomic_noreplace_publication(self):
        module = self.load_boolean_provider_module()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite"
            path.symlink_to(Path(directory) / "missing")
            with self.assertRaises(FileExistsError):
                module.build_boolean_rank_source(path, (), factor_count=0)
            self.assertTrue(path.is_symlink())
            path.unlink()

            def create_concurrent_destination_file(database, destination):
                Path(destination).write_bytes(b"another owner's artifact")
                raise FileExistsError(destination)

            with patch.object(module.os, "link", side_effect=create_concurrent_destination_file):
                with self.assertRaises(FileExistsError):
                    module.build_boolean_rank_source(path, [(1, (), 1.0)], factor_count=0)
            self.assertEqual(path.read_bytes(), b"another owner's artifact")
            self.assertEqual(list(Path(directory).iterdir()), [path])
            path.unlink()
            with patch.object(module.os, "link", side_effect=OSError("publication failed")):
                with self.assertRaises(OSError):
                    module.build_boolean_rank_source(path, (), factor_count=0)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_cursor_lifetime_limits(self):
        module = self.load_boolean_provider_module()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite"
            module.build_boolean_rank_source(path, ((i, (0,), 1.0) for i in range(20)), factor_count=2)
            with module.SqliteBooleanRankSource(path) as source:
                for iterator in (source.iterate_class_records(), source.iterate_class_vertex_rows(0),
                                 source.iterate_active_factor_counts()):
                    before = source.events["record_reads"]
                    next(iterator)
                    iterator.close()
                    iterator.close()
                    self.assertEqual(source.events["record_reads"] - before, 1)
                    self.assertEqual(list(iterator), [])
                self.assertEqual(source.events["active_cursors"], 0)
                cursors = [source.iterate_class_vertex_rows(0) for _ in range(source.max_open_cursors)]
                with self.assertRaisesRegex(ValueError, "cursor reservation"):
                    source.iterate_class_records()
                source.close()
                source.close()
                self.assertEqual(source.events["active_cursors"], 0)
                self.assertTrue(all(list(cursor) == [] for cursor in cursors))
                for method, args in ((source.iterate_class_records, ()), (source.iterate_class_vertex_rows, (0,)),
                                     (source.iterate_active_factor_counts, ())):
                    with self.assertRaisesRegex(ValueError, "closed"):
                        method(*args)

    def test_readonly_pinned_snapshot(self):
        module = self.load_boolean_provider_module()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source ?#.sqlite"
            module.build_boolean_rank_source(path, [(1, (), 1.0)], factor_count=0)
            with closing(sqlite3.connect(path)) as writer:
                writer.execute("PRAGMA journal_mode=WAL").close()
            with module.SqliteBooleanRankSource(path, cache_kib=48) as source:
                snapshot = source.snapshot_id
                self.assertTrue(source._connection.in_transaction)
                for pragma, expected in (("query_only", 1), ("cache_size", -48), ("temp_store", 1), ("mmap_size", 0)):
                    with closing(source._connection.execute(f"PRAGMA {pragma}")) as cursor:
                        self.assertEqual(cursor.fetchone()[0], expected)
                with self.assertRaises(sqlite3.OperationalError):
                    source._connection.execute("CREATE TABLE forbidden(value)")
                source._connection.execute("PRAGMA query_only=OFF").close()
                with self.assertRaises(sqlite3.OperationalError):
                    source._connection.execute("UPDATE vertices SET original_id=2")
                with closing(sqlite3.connect(path)) as writer:
                    writer.execute("UPDATE vertices SET original_id=9").close()
                    writer.commit()
                self.assertEqual(list(source.iterate_class_vertex_rows(0)), [(1, 1.0)])
                self.assertEqual(source.snapshot_id, snapshot)
            with module.SqliteBooleanRankSource(path) as reopened:
                self.assertEqual(list(reopened.iterate_class_vertex_rows(0)), [(9, 1.0)])

    def test_single_fetch_streaming(self):
        module = self.load_boolean_provider_module()
        fetches = [0]

        class TrackingSingleRowCursor(sqlite3.Cursor):
            def fetchone(self):
                fetches[0] += 1
                return super().fetchone()

            def fetchmany(self, *args):
                raise AssertionError("fetchmany forbidden")

            def fetchall(self):
                raise AssertionError("fetchall forbidden")

            def __iter__(self):
                raise AssertionError("use explicit fetchone")

        class TrackingSingleRowConnection(sqlite3.Connection):
            def execute(self, statement, parameters=()):
                return self.cursor(factory=TrackingSingleRowCursor).execute(statement, parameters)

        original_connect = sqlite3.connect
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite"
            with patch.object(module.sqlite3, "connect", side_effect=lambda *args, **kwargs:
                              original_connect(*args, **kwargs, factory=TrackingSingleRowConnection)):
                module.build_boolean_rank_source(path, ((i, (0, 0), 1.0) for i in range(10)), factor_count=2)
                with module.SqliteBooleanRankSource(path) as source:
                    before = fetches[0]
                    cursor = source.iterate_class_vertex_rows(0)
                    self.assertEqual(fetches[0] - before, 1, "one class metadata lookup")
                    next(cursor)
                    self.assertEqual(fetches[0] - before, 2)
                    cursor.close()
                    self.assertEqual(fetches[0] - before, 2, "close never drains")
                    self.assertEqual(len(list(source.iterate_class_records())), 1)
                    self.assertEqual(len(list(source.iterate_active_factor_counts())), 2)

    def test_input_order_invariance(self):
        module = self.load_boolean_provider_module()
        rows = [(7, (0, 2, 0), 0.5), (2, (0,), 2.0), (1, (), 3.0),
                (99, (2, 0), 0.25), (0, (8,), 1.0), (3, (3, 4), 4.0)]
        with tempfile.TemporaryDirectory() as directory:
            snapshots = []
            for number, ordered in enumerate((rows, list(reversed(rows)))):
                path = Path(directory) / f"{number}.sqlite"
                module.build_boolean_rank_source(path, iter(ordered), factor_count=10)
                with module.SqliteBooleanRankSource(path) as source:
                    snapshots.append((list(source.iterate_class_records()),
                                      [(record[0], list(source.iterate_class_vertex_rows(record[0])))
                                       for record in source.iterate_class_records()],
                                      list(source.iterate_active_factor_counts())))
            self.assertEqual(snapshots[0], snapshots[1])

    def test_membership_stream_boundaries(self):
        module = self.load_boolean_provider_module()

        class SingleUseInputIterable:
            def __init__(self, values):
                self.values = values
                self.calls = 0

            def __iter__(self):
                self.calls += 1
                if self.calls != 1:
                    raise AssertionError("input restarted")
                return iter(self.values)

        def generate_third_distinct_membership():
            yield 0
            yield 1
            yield 2
            raise AssertionError("read past third distinct membership")

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite"
            with self.assertRaisesRegex(ValueError, "two distinct"):
                module.build_boolean_rank_source(path, [(1, generate_third_distinct_membership(), 1.0)], factor_count=3)
            self.assertEqual(list(Path(directory).iterdir()), [])
            members = SingleUseInputIterable((0, 0, 1, 0, 1))
            rows = SingleUseInputIterable(((9, members, 1.0),))
            receipt = module.build_boolean_rank_source(path, rows, factor_count=3)
            self.assertEqual((members.calls, rows.calls), (1, 1))
            self.assertEqual(receipt["source_events"]["membership_items"], 5)
            self.assertEqual(receipt["source_events"]["memberships"], 2)
            with module.SqliteBooleanRankSource(path) as source:
                self.assertEqual(list(source.iterate_class_records()), [(0, (0, 1), 1, 0, Fraction(1))])

    def test_preparation_failure_cleanup(self):
        module = self.load_boolean_provider_module()
        original_execute = module.execute_sqlite_statement_exact
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite"
            for prefix in ("CREATE INDEX", "INSERT INTO classes", "INSERT INTO manifest"):
                def inject_preparation_statement_failure(connection, statement, parameters=()):
                    if statement.startswith(prefix):
                        raise sqlite3.OperationalError("injected preparation failure")
                    return original_execute(connection, statement, parameters)

                with patch.object(module, "execute_sqlite_statement_exact", side_effect=inject_preparation_statement_failure):
                    with self.assertRaisesRegex(sqlite3.OperationalError, "injected"):
                        module.build_boolean_rank_source(path, [(1, (0,), 1.0)], factor_count=1, batch_records=1)
                self.assertEqual(list(Path(directory).iterdir()), [])

    def test_source_constructor_cleanup(self):
        module = self.load_boolean_provider_module()
        closes = []

        class TrackingClosedSourceConnection(sqlite3.Connection):
            def close(self):
                closes.append(True)
                return super().close()

        original_connect = sqlite3.connect
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite"
            module.build_boolean_rank_source(path, (), factor_count=1)
            with closing(sqlite3.connect(path)) as writer:
                writer.execute("UPDATE manifest SET schema_version=-1").close()
                writer.commit()
            with patch.object(module.sqlite3, "connect", side_effect=lambda *args, **kwargs:
                              original_connect(*args, **kwargs, factory=TrackingClosedSourceConnection)):
                with self.assertRaisesRegex(ValueError, "schema"):
                    module.SqliteBooleanRankSource(path)
            self.assertEqual(closes, [True])
            for value in (0, -1, True, 1.5, 2**31):
                with self.assertRaises(ValueError):
                    module.SqliteBooleanRankSource(path, cache_kib=value)
            with self.assertRaises(sqlite3.OperationalError):
                module.SqliteBooleanRankSource(Path(directory) / "missing.sqlite")
            self.assertFalse((Path(directory) / "missing.sqlite").exists())

    def test_decoder_failure_closes(self):
        module = self.load_boolean_provider_module()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite"
            module.build_boolean_rank_source(path, [(0, (), 1.0), (1, (), 2.0)], factor_count=0)
            with module.SqliteBooleanRankSource(path) as source:
                with patch.object(module, "decode_source_vertex_record", side_effect=RuntimeError("decode failed")):
                    cursor = source.iterate_class_vertex_rows(0)
                with self.assertRaisesRegex(RuntimeError, "decode failed"):
                    next(cursor)
                self.assertEqual(source.events["active_cursors"], 0)
                self.assertEqual(list(cursor), [])
                for class_id in (-1, 1, True, 0.0, "0"):
                    with self.assertRaisesRegex(ValueError, "class ID"):
                        source.iterate_class_vertex_rows(class_id)
                with self.assertRaises(AttributeError):
                    source.max_open_cursors = 100
                for name in ("snapshot_id", "vertex_count", "factor_count", "class_count", "active_class_count",
                             "total_weight", "isolate_weight"):
                    with self.assertRaises(AttributeError):
                        setattr(source, name, 0)

    def test_indexed_nested_query_plans(self):
        module = self.load_boolean_provider_module()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite"
            module.build_boolean_rank_source(path, [(0, (0, 1), 1.0), (1, (0,), 2.0), (2, (), 3.0)], factor_count=4)
            with module.SqliteBooleanRankSource(path) as source:
                statements = []
                source._connection.set_trace_callback(statements.append)
                keys = set(source.events)
                with source.iterate_class_records() as classes:
                    for class_id, *_ in classes:
                        with source.iterate_class_vertex_rows(class_id) as rows:
                            list(rows)
                list(source.iterate_active_factor_counts())
                source._connection.set_trace_callback(None)
                self.assertEqual(set(source.events), keys)
                self.assertEqual(source.events["active_cursors"], 0)
                self.assertLessEqual(source.events["peak_cursors"], 2)
                self.assertFalse(any("SUM(" in statement.upper() or "COUNT(" in statement.upper()
                                     for statement in statements))
                for statement in statements:
                    with closing(source._connection.execute("EXPLAIN QUERY PLAN " + statement)) as cursor:
                        while True:
                            row = cursor.fetchone()
                            if row is None:
                                break
                            self.assertNotIn("TEMP B-TREE", row[-1])


if __name__ == "__main__":
    unittest.main()
