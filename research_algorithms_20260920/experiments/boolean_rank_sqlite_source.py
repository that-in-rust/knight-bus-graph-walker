"""Paid streaming preparation for Boolean two-membership PageRank.

The frozen personalization is p_v = Fraction.from_float(weight_v) / total_weight.
This module never rounds that normalization or expands a clique. SQLite cache
knobs are not physical-memory limits. Artifacts must be trusted builder output.
"""

from contextlib import closing
from fractions import Fraction
import json
import math
import os
from pathlib import Path
import sqlite3
import struct
import tempfile
import time
import uuid


MAX_SQLITE_ID = 2**63 - 1
SCHEMA_VERSION = 1

SCHEMA_SQL = """
CREATE TABLE vertices (
    original_id INTEGER PRIMARY KEY CHECK(original_id >= 0),
    group_a INTEGER NOT NULL CHECK(group_a >= -1),
    group_b INTEGER NOT NULL CHECK(group_b = -1 OR group_b > group_a),
    weight_bits BLOB NOT NULL CHECK(length(weight_bits) = 8),
    CHECK(group_a >= 0 OR group_b = -1)
);
CREATE TABLE factors (
    factor_id INTEGER PRIMARY KEY CHECK(factor_id >= 0),
    member_count INTEGER NOT NULL DEFAULT 0
        CHECK(typeof(member_count) = 'integer' AND member_count >= 0),
    active_member_count INTEGER NOT NULL DEFAULT 0
        CHECK(typeof(active_member_count) = 'integer' AND active_member_count >= 0)
);
CREATE TABLE classes (
    class_id INTEGER PRIMARY KEY CHECK(class_id >= 0),
    group_a INTEGER NOT NULL,
    group_b INTEGER NOT NULL,
    h INTEGER NOT NULL CHECK(typeof(h) = 'integer' AND h > 0),
    degree INTEGER NOT NULL CHECK(typeof(degree) = 'integer' AND degree >= 0),
    weight_numerator TEXT NOT NULL,
    weight_denominator TEXT NOT NULL,
    UNIQUE(group_a, group_b)
);
CREATE TABLE manifest (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    schema_version INTEGER NOT NULL,
    snapshot_id TEXT NOT NULL,
    vertex_count INTEGER NOT NULL,
    factor_count INTEGER NOT NULL,
    class_count INTEGER NOT NULL,
    active_class_count INTEGER NOT NULL,
    total_numerator TEXT NOT NULL,
    total_denominator TEXT NOT NULL,
    isolate_numerator TEXT NOT NULL,
    isolate_denominator TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);
"""


def validate_positive_option_exact(value, name):
    if type(value) is not int or not 1 <= value <= 2**31 - 1:
        raise ValueError(f"{name} must be a positive signed-32-bit integer")


def validate_original_identifier_exact(value):
    if type(value) is not int or not 0 <= value <= MAX_SQLITE_ID:
        raise ValueError("original IDs must be nonnegative signed-63-bit integers (not bool)")


def execute_sqlite_statement_exact(connection, statement, parameters=()):
    with closing(connection.execute(statement, parameters)):
        pass


def fetch_sqlite_single_record(connection, statement, parameters=()):
    with closing(connection.execute(statement, parameters)) as cursor:
        return cursor.fetchone()


def configure_sqlite_connection_limits(connection, cache_kib):
    execute_sqlite_statement_exact(connection, f"PRAGMA cache_size=-{cache_kib}")
    execute_sqlite_statement_exact(connection, f"PRAGMA temp.cache_size=-{cache_kib}")
    execute_sqlite_statement_exact(connection, "PRAGMA temp_store=FILE")
    execute_sqlite_statement_exact(connection, "PRAGMA mmap_size=0")


def decode_signature_groups_exact(group_a, group_b):
    if group_a == -1:
        return ()
    return (group_a,) if group_b == -1 else (group_a, group_b)


def decode_source_class_record(record):
    class_id, group_a, group_b, h, degree, numerator, denominator = record
    return class_id, decode_signature_groups_exact(group_a, group_b), h, degree, Fraction(int(numerator), int(denominator))


def decode_source_vertex_record(record):
    return record[0], struct.unpack(">d", record[1])[0]


def build_boolean_rank_source(destination, rows, *, factor_count, cache_kib=1024, batch_records=4096):
    """Build a new artifact, publishing with atomic no-replace hard linking.

    Rows are (original_id, memberships_iterable, weight_float). Floats must be
    finite and nonnegative; integers, bools and implicit conversions are rejected.
    At most two distinct memberships are admitted, without buffering an iterable.
    Input iterators are consumed once and remain caller-owned (not closed here).
    batch_records bounds transaction records, not a Python buffer or RAM.
    Return a fixed-field receipt with events, phase times and final database bytes.
    """
    started = time.perf_counter()
    validate_positive_option_exact(cache_kib, "cache_kib")
    validate_positive_option_exact(batch_records, "batch_records")
    if type(factor_count) is not int or not 0 <= factor_count <= MAX_SQLITE_ID:
        raise ValueError("factor_count must be a nonnegative signed-63-bit integer")
    destination = Path(destination).absolute()
    if os.path.lexists(destination):
        raise FileExistsError(destination)
    snapshot_id = uuid.uuid4().hex
    source_events = dict(vertices=0, factors=0, membership_items=0, memberships=0)
    builder_events = dict(class_scan_rows=0, classes_written=0, factor_count_reads=0,
                          active_factor_updates=0, commits=0, indexes_created=0)
    phase_seconds = dict(setup=0.0, factors=0.0, ingestion=0.0, indexing=0.0, classes=0.0)
    total_weight = Fraction()
    isolate_weight = Fraction()
    class_count = 0
    active_class_count = 0

    with tempfile.TemporaryDirectory(prefix=f".{destination.name}.prepare-", dir=destination.parent) as scratch:
        database = Path(scratch) / "snapshot.sqlite"
        connection = sqlite3.connect(database)
        try:
            configure_sqlite_connection_limits(connection, cache_kib)
            execute_sqlite_statement_exact(connection, "PRAGMA journal_mode=DELETE")
            execute_sqlite_statement_exact(connection, "PRAGMA synchronous=FULL")
            with closing(connection.executescript(SCHEMA_SQL)):
                pass

            def commit_builder_transaction_exact():
                connection.commit()
                builder_events["commits"] += 1

            phase_seconds["setup"] = time.perf_counter() - started
            phase_started = time.perf_counter()
            for factor in range(factor_count):
                execute_sqlite_statement_exact(connection, "INSERT INTO factors(factor_id) VALUES (?)", (factor,))
                source_events["factors"] += 1
                if source_events["factors"] % batch_records == 0:
                    commit_builder_transaction_exact()
            commit_builder_transaction_exact()
            phase_seconds["factors"] = time.perf_counter() - phase_started

            phase_started = time.perf_counter()
            for row in rows:
                try:
                    original_id, memberships, weight = row
                except (TypeError, ValueError) as error:
                    raise ValueError("rows must be (original_id, memberships, weight_float)") from error
                validate_original_identifier_exact(original_id)
                if type(weight) is not float or not math.isfinite(weight) or weight < 0:
                    raise ValueError("weights must be finite nonnegative float values")
                try:
                    memberships = iter(memberships)
                except TypeError as error:
                    raise ValueError("memberships must be iterable") from error
                groups = set()
                for factor in memberships:
                    source_events["membership_items"] += 1
                    if type(factor) is not int or not 0 <= factor < factor_count:
                        raise ValueError("membership IDs must be integers in 0..factor_count-1")
                    if factor not in groups:
                        if len(groups) == 2:
                            raise ValueError("at most two distinct memberships are allowed")
                        groups.add(factor)
                group_a, group_b = (tuple(sorted(groups)) + (-1, -1))[:2]
                if source_events["vertices"] == MAX_SQLITE_ID:
                    raise ValueError("vertex count exceeds signed-63-bit storage")
                execute_sqlite_statement_exact(connection, "INSERT INTO vertices VALUES (?, ?, ?, ?)",
                                               (original_id, group_a, group_b, struct.pack(">d", weight)))
                for factor in groups:
                    execute_sqlite_statement_exact(connection,
                        "UPDATE factors SET member_count=member_count+1 WHERE factor_id=?", (factor,))
                total_weight += Fraction.from_float(weight)
                source_events["vertices"] += 1
                source_events["memberships"] += len(groups)
                if source_events["vertices"] % batch_records == 0:
                    commit_builder_transaction_exact()
            if source_events["vertices"] and not total_weight:
                raise ValueError("nonempty input requires positive total_weight")
            commit_builder_transaction_exact()
            phase_seconds["ingestion"] = time.perf_counter() - phase_started

            phase_started = time.perf_counter()
            execute_sqlite_statement_exact(connection,
                "CREATE INDEX vertices_by_signature ON vertices(group_a, group_b, original_id)")
            builder_events["indexes_created"] += 1
            commit_builder_transaction_exact()
            phase_seconds["indexing"] = time.perf_counter() - phase_started

            def store_current_class_exact(signature, h, weight_sum):
                nonlocal class_count, active_class_count, isolate_weight
                groups = decode_signature_groups_exact(*signature)
                member_count = 0
                for factor in groups:
                    member_count += fetch_sqlite_single_record(connection,
                        "SELECT member_count FROM factors WHERE factor_id=?", (factor,))[0]
                    builder_events["factor_count_reads"] += 1
                degree = member_count - (len(groups) - 1) * h - 1 if groups else 0
                if not 0 <= degree < source_events["vertices"]:
                    raise ValueError("class degree exceeds vertex universe")
                execute_sqlite_statement_exact(connection, "INSERT INTO classes VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (class_count, *signature, h, degree, str(weight_sum.numerator), str(weight_sum.denominator)))
                if degree:
                    active_class_count += 1
                    for factor in groups:
                        execute_sqlite_statement_exact(connection,
                            "UPDATE factors SET active_member_count=active_member_count+? WHERE factor_id=?", (h, factor))
                        builder_events["active_factor_updates"] += 1
                else:
                    isolate_weight += weight_sum
                class_count += 1
                builder_events["classes_written"] += 1
                if class_count % batch_records == 0:
                    commit_builder_transaction_exact()

            phase_started = time.perf_counter()
            signature = None
            h = 0
            weight_sum = Fraction()
            with closing(connection.execute("""
                SELECT group_a, group_b, weight_bits FROM vertices INDEXED BY vertices_by_signature
                ORDER BY group_a, group_b, original_id
            """)) as cursor:
                while True:
                    record = cursor.fetchone()
                    if record is None:
                        break
                    builder_events["class_scan_rows"] += 1
                    if signature != record[:2]:
                        if signature is not None:
                            store_current_class_exact(signature, h, weight_sum)
                        signature = record[:2]
                        h = 0
                        weight_sum = Fraction()
                    h += 1
                    weight_sum += Fraction.from_float(struct.unpack(">d", record[2])[0])
            if signature is not None:
                store_current_class_exact(signature, h, weight_sum)
            commit_builder_transaction_exact()
            phase_seconds["classes"] = time.perf_counter() - phase_started

            # Include the upcoming manifest commit in the persisted fixed-size counters.
            builder_events["commits"] += 1
            metadata = dict(source_events=source_events, builder_events=builder_events,
                            phase_seconds=phase_seconds, cache_kib=cache_kib, batch_records=batch_records,
                            preparation_seconds=time.perf_counter() - started)
            execute_sqlite_statement_exact(connection, "INSERT INTO manifest VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (SCHEMA_VERSION, snapshot_id, source_events["vertices"], factor_count, class_count, active_class_count,
                 str(total_weight.numerator), str(total_weight.denominator),
                 str(isolate_weight.numerator), str(isolate_weight.denominator), json.dumps(metadata, sort_keys=True)))
            connection.commit()
        except sqlite3.IntegrityError as error:
            raise ValueError("duplicate original ID or invalid normalized source record") from error
        finally:
            connection.close()
        database_bytes = database.stat().st_size
        os.link(database, destination)
    return dict(metadata, snapshot_id=snapshot_id, database_bytes=database_bytes,
                elapsed_seconds=time.perf_counter() - started)


class SqliteBooleanRankCursor:
    """A bounded one-fetch-per-record iterator; close() never drains it."""

    def __init__(self, source, statement, parameters, length, decoder=None):
        self._source = source
        self._length = length
        self._remaining = length
        self._decoder = decoder
        self._cursor = None
        source._check_source_open_exact()
        if type(length) is not int or length < 0:
            raise ValueError("invalid persisted cursor length")
        if len(source._active_cursors) >= source.max_open_cursors:
            raise ValueError("open cursor reservation exceeded")
        source.events["row_opens"] += 1
        if length:
            self._cursor = source._connection.execute(statement, parameters)
            source._active_cursors.add(self)
            source.events["active_cursors"] = len(source._active_cursors)
            source.events["peak_cursors"] = max(source.events["peak_cursors"], len(source._active_cursors))

    @property
    def length(self):
        return self._length

    def __iter__(self):
        return self

    def __next__(self):
        if self._cursor is None:
            raise StopIteration
        try:
            self._source.events["record_fetches"] += 1
            record = self._cursor.fetchone()
            if record is None:
                raise ValueError("prepared rows shorter than persisted count")
            self._source.events["record_reads"] += 1
            self._remaining -= 1
            if self._remaining == 0:
                self.close()
            return self._decoder(record) if self._decoder else record
        except BaseException:
            self.close()
            raise

    def close(self):
        if self._cursor is not None:
            try:
                self._cursor.close()
            finally:
                self._cursor = None
                self._source._active_cursors.discard(self)
                self._source.events["cursor_closes"] += 1
                self._source.events["active_cursors"] = len(self._source._active_cursors)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class SqliteBooleanRankSource:
    """Pinned read-only view of trusted builder output, with no F/P/n arrays.

    Iterators are independent and explicitly closeable/context-managed. Closing
    a parent class iterator does not close a separately opened vertex iterator;
    closing the source closes all of them. Use closing() on interrupted streams.
    A UUID is build identity, not a content hash or hostile-file validation.
    """

    def __init__(self, path, *, cache_kib=1024):
        validate_positive_option_exact(cache_kib, "cache_kib")
        self._active_cursors = set()
        self._closed = False
        self.events = dict(row_opens=0, record_fetches=0, record_reads=0,
                           metadata_reads=0, cursor_closes=0, active_cursors=0, peak_cursors=0)
        uri = Path(path).absolute().as_uri() + "?mode=ro"
        self._connection = sqlite3.connect(uri, uri=True, isolation_level=None)
        try:
            configure_sqlite_connection_limits(self._connection, cache_kib)
            execute_sqlite_statement_exact(self._connection, "PRAGMA query_only=ON")
            execute_sqlite_statement_exact(self._connection, "BEGIN")
            header = self._fetch_source_metadata_exact("""
                SELECT schema_version, snapshot_id, vertex_count, factor_count, class_count, active_class_count,
                       total_numerator, total_denominator, isolate_numerator, isolate_denominator, metadata_json
                FROM manifest WHERE singleton=1
            """)
            if header is None or header[0] != SCHEMA_VERSION:
                raise ValueError("unsupported Boolean rank source schema")
            self._snapshot_id = header[1]
            self._vertex_count, self._factor_count, self._class_count, self._active_class_count = header[2:6]
            self._total_weight = Fraction(int(header[6]), int(header[7]))
            self._isolate_weight = Fraction(int(header[8]), int(header[9]))
            self.preparation_metadata = json.loads(header[10])
        except BaseException:
            self.close()
            raise

    @property
    def max_open_cursors(self):
        return 8

    @property
    def snapshot_id(self):
        return self._snapshot_id

    @property
    def vertex_count(self):
        return self._vertex_count

    @property
    def factor_count(self):
        return self._factor_count

    @property
    def class_count(self):
        return self._class_count

    @property
    def active_class_count(self):
        return self._active_class_count

    @property
    def total_weight(self):
        return self._total_weight

    @property
    def isolate_weight(self):
        return self._isolate_weight

    def _check_source_open_exact(self):
        if self._closed:
            raise ValueError("Boolean rank source is closed")

    def _fetch_source_metadata_exact(self, statement, parameters=()):
        self._check_source_open_exact()
        self.events["metadata_reads"] += 1
        return fetch_sqlite_single_record(self._connection, statement, parameters)

    def iterate_class_records(self):
        return SqliteBooleanRankCursor(self, """
            SELECT class_id, group_a, group_b, h, degree, weight_numerator, weight_denominator
            FROM classes ORDER BY class_id
        """, (), self.class_count, decode_source_class_record)

    def iterate_class_vertex_rows(self, class_id):
        self._check_source_open_exact()
        if type(class_id) is not int or not 0 <= class_id < self.class_count:
            raise ValueError("unknown class ID")
        group_a, group_b, h = self._fetch_source_metadata_exact(
            "SELECT group_a, group_b, h FROM classes WHERE class_id=?", (class_id,))
        return SqliteBooleanRankCursor(self, """
            SELECT original_id, weight_bits FROM vertices INDEXED BY vertices_by_signature
            WHERE group_a=? AND group_b=? ORDER BY original_id
        """, (group_a, group_b), h, decode_source_vertex_record)

    def iterate_active_factor_counts(self):
        return SqliteBooleanRankCursor(self,
            "SELECT factor_id, active_member_count FROM factors ORDER BY factor_id", (), self.factor_count)

    def close(self):
        if not self._closed:
            try:
                for cursor in tuple(self._active_cursors):
                    cursor.close()
            finally:
                self._closed = True
                self._connection.close()

    def __enter__(self):
        self._check_source_open_exact()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
