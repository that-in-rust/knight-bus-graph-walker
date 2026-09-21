"""Paid, disk-backed preparation for the fault-cover connectivity experiment.

No native clique expansion or n/edit-sized Python collection is constructed.
SQLite page-cache settings are not a whole-process or physical-memory cap.
Published artifacts are trusted builder output, not arbitrary SQLite files.
"""

from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import uuid


MAX_SQLITE_ID = 2**63 - 1
SCHEMA_VERSION = 1

SCHEMA_SQL = """
CREATE TABLE vertices (
    vertex_id INTEGER PRIMARY KEY CHECK(vertex_id >= 0),
    membership_count INTEGER NOT NULL DEFAULT 0,
    deleted_degree INTEGER NOT NULL DEFAULT 0,
    inserted_degree INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE factors (
    factor_id INTEGER PRIMARY KEY CHECK(factor_id >= 0),
    member_count INTEGER NOT NULL DEFAULT 0,
    core_count INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE memberships (
    vertex_id INTEGER NOT NULL REFERENCES vertices,
    factor_id INTEGER NOT NULL REFERENCES factors,
    PRIMARY KEY(vertex_id, factor_id)
) WITHOUT ROWID;
CREATE INDEX memberships_by_factor ON memberships(factor_id, vertex_id);
CREATE TABLE cover (
    vertex_id INTEGER PRIMARY KEY REFERENCES vertices
);
CREATE TABLE edits (
    left_id INTEGER NOT NULL REFERENCES vertices,
    right_id INTEGER NOT NULL REFERENCES vertices,
    kind INTEGER NOT NULL CHECK(kind IN (0, 1)),
    CHECK(left_id < right_id),
    PRIMARY KEY(left_id, right_id)
) WITHOUT ROWID;
CREATE TABLE neighbors (
    kind INTEGER NOT NULL CHECK(kind IN (0, 1)),
    vertex_id INTEGER NOT NULL REFERENCES vertices,
    neighbor_id INTEGER NOT NULL REFERENCES vertices,
    PRIMARY KEY(kind, vertex_id, neighbor_id)
) WITHOUT ROWID;
CREATE TABLE core_postings (
    factor_id INTEGER NOT NULL REFERENCES factors,
    vertex_id INTEGER NOT NULL REFERENCES cover,
    PRIMARY KEY(factor_id, vertex_id)
) WITHOUT ROWID;
CREATE TABLE inserted_cover_edges (
    left_id INTEGER NOT NULL REFERENCES cover,
    right_id INTEGER NOT NULL REFERENCES cover,
    PRIMARY KEY(left_id, right_id)
) WITHOUT ROWID;
CREATE TABLE manifest (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    schema_version INTEGER NOT NULL,
    snapshot_id TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);
"""


def validate_sqlite_identifier_exact(identifier):
    if type(identifier) is not int or not 0 <= identifier <= MAX_SQLITE_ID:
        raise ValueError("IDs must be nonnegative signed-63-bit integers (not bool)")
    return identifier


def validate_positive_option_exact(value, name):
    if type(value) is not int or not 1 <= value <= 2**31 - 1:
        raise ValueError(f"{name} must be a positive signed-32-bit integer")


def execute_sqlite_statement_exact(connection, statement, parameters=()):
    with closing(connection.execute(statement, parameters)):
        pass


def fetch_sqlite_single_record(connection, statement, parameters=()):
    with closing(connection.execute(statement, parameters)) as cursor:
        return cursor.fetchone()


def configure_sqlite_connection_limits(connection, cache_kib):
    validate_positive_option_exact(cache_kib, "cache_kib")
    execute_sqlite_statement_exact(connection, f"PRAGMA cache_size=-{cache_kib}")
    execute_sqlite_statement_exact(connection, "PRAGMA temp_store=FILE")
    execute_sqlite_statement_exact(connection, "PRAGMA mmap_size=0")
    execute_sqlite_statement_exact(connection, "PRAGMA foreign_keys=ON")


def build_sqlite_fault_source(destination, original_vertices, groups, deleted=(), added=(), cover=(),
                              *, cache_kib=1024, batch_records=4096):
    """Stream strict normalized inputs into a new, atomically published DB.

    Return a constant-size receipt with DB bytes, timing, and input events.
    batch_records limits input events per committed transaction, not RAM.
    No iterator is restarted; caller-owned iterators are not closed here.
    The executor must use exactly this cover set: core postings and inserted
    cover edges are cover-specific. Matching the executor's cover is a trusted
    cross-view contract, not a runtime check performed by this provider.
    """
    started = time.perf_counter()
    validate_positive_option_exact(cache_kib, "cache_kib")
    validate_positive_option_exact(batch_records, "batch_records")
    destination = Path(destination).absolute()
    if os.path.lexists(destination):
        raise FileExistsError(destination)
    snapshot_id = uuid.uuid4().hex
    source_events = dict(original_vertices=0, factors=0, memberships=0,
                         deleted_pairs=0, added_pairs=0, cover_vertices=0)
    pending_records = 0
    native_membership_probes = 0

    with tempfile.TemporaryDirectory(prefix=f".{destination.name}.prepare-",
                                     dir=destination.parent) as scratch:
        database = Path(scratch) / "snapshot.sqlite"
        connection = sqlite3.connect(database)
        try:
            configure_sqlite_connection_limits(connection, cache_kib)
            execute_sqlite_statement_exact(connection, "PRAGMA journal_mode=DELETE")
            execute_sqlite_statement_exact(connection, "PRAGMA synchronous=FULL")
            with closing(connection.executescript(SCHEMA_SQL)):
                pass

            def record_builder_input_event(kind):
                nonlocal pending_records
                source_events[kind] += 1
                pending_records += 1
                if pending_records >= batch_records:
                    connection.commit()
                    pending_records = 0

            for vertex in original_vertices:
                validate_sqlite_identifier_exact(vertex)
                execute_sqlite_statement_exact(connection, "INSERT INTO vertices(vertex_id) VALUES (?)", (vertex,))
                record_builder_input_event("original_vertices")
            for factor, members in enumerate(groups):
                validate_sqlite_identifier_exact(factor)
                execute_sqlite_statement_exact(connection, "INSERT INTO factors(factor_id) VALUES (?)", (factor,))
                record_builder_input_event("factors")
                for vertex in members:
                    validate_sqlite_identifier_exact(vertex)
                    execute_sqlite_statement_exact(connection, "INSERT INTO memberships VALUES (?, ?)", (vertex, factor))
                    execute_sqlite_statement_exact(connection,
                        "UPDATE vertices SET membership_count=membership_count+1 WHERE vertex_id=?", (vertex,))
                    execute_sqlite_statement_exact(connection,
                        "UPDATE factors SET member_count=member_count+1 WHERE factor_id=?", (factor,))
                    record_builder_input_event("memberships")
            for vertex in cover:
                validate_sqlite_identifier_exact(vertex)
                execute_sqlite_statement_exact(connection, "INSERT INTO cover VALUES (?)", (vertex,))
                record_builder_input_event("cover_vertices")

            for kind, pairs, event in ((0, deleted, "deleted_pairs"), (1, added, "added_pairs")):
                for pair in pairs:
                    try:
                        left, right = pair
                    except (TypeError, ValueError) as error:
                        raise ValueError("edits must be normalized two-ID pairs") from error
                    validate_sqlite_identifier_exact(left)
                    validate_sqlite_identifier_exact(right)
                    if left >= right:
                        raise ValueError("edit pairs must have left < right")
                    execute_sqlite_statement_exact(connection,
                        "INSERT INTO edits VALUES (?, ?, ?)", (left, right, kind))
                    native_membership_probes += 1
                    base = fetch_sqlite_single_record(connection, """
                        SELECT 1 FROM memberships AS left_members
                        JOIN memberships AS right_members
                          ON left_members.factor_id=right_members.factor_id
                        WHERE left_members.vertex_id=? AND right_members.vertex_id=? LIMIT 1
                    """, (left, right)) is not None
                    if base != (kind == 0):
                        raise ValueError("deleted edges must be base; added edges must be nonbase")
                    if fetch_sqlite_single_record(connection,
                        "SELECT 1 FROM cover WHERE vertex_id IN (?, ?) LIMIT 1", (left, right)) is None:
                        raise ValueError("an edit edge is not covered")
                    degree_column = "deleted_degree" if kind == 0 else "inserted_degree"
                    for vertex, neighbor in ((left, right), (right, left)):
                        execute_sqlite_statement_exact(connection,
                            "INSERT INTO neighbors VALUES (?, ?, ?)", (kind, vertex, neighbor))
                        execute_sqlite_statement_exact(connection,
                            f"UPDATE vertices SET {degree_column}={degree_column}+1 WHERE vertex_id=?", (vertex,))
                    record_builder_input_event(event)

            execute_sqlite_statement_exact(connection, """
                INSERT INTO core_postings SELECT m.factor_id, m.vertex_id
                FROM cover AS c JOIN memberships AS m ON c.vertex_id=m.vertex_id
            """)
            execute_sqlite_statement_exact(connection, """
                UPDATE factors SET core_count=(
                    SELECT COUNT(*) FROM core_postings AS c WHERE c.factor_id=factors.factor_id)
            """)
            execute_sqlite_statement_exact(connection, """
                INSERT INTO inserted_cover_edges SELECT e.left_id, e.right_id
                FROM edits AS e JOIN cover AS l ON e.left_id=l.vertex_id
                JOIN cover AS r ON e.right_id=r.vertex_id WHERE e.kind=1
            """)
            core_count = fetch_sqlite_single_record(connection, "SELECT COUNT(*) FROM core_postings")[0]
            inserted_cover_count = fetch_sqlite_single_record(connection, "SELECT COUNT(*) FROM inserted_cover_edges")[0]
            metadata = dict(source_events=source_events, cache_kib=cache_kib, batch_records=batch_records,
                            native_membership_probes=native_membership_probes,
                            core_posting_records=core_count, inserted_cover_pairs=inserted_cover_count,
                            preparation_seconds=time.perf_counter() - started)
            execute_sqlite_statement_exact(connection, "INSERT INTO manifest VALUES (1, ?, ?, ?)",
                                           (SCHEMA_VERSION, snapshot_id, json.dumps(metadata, sort_keys=True)))
            connection.commit()
        except sqlite3.IntegrityError as error:
            raise ValueError("invalid or duplicate normalized graph record") from error
        finally:
            connection.close()
        database_bytes = database.stat().st_size
        os.link(database, destination)
    return dict(metadata, snapshot_id=snapshot_id, database_bytes=database_bytes,
                elapsed_seconds=time.perf_counter() - started)


class SqlitePreparedRowCursor:
    """One SQLite row per explicit read, with immutable precomputed length."""

    def __init__(self, source, statement, parameters, length, *, scalar=True):
        self._source = source
        self._length = length
        self._remaining = length
        self._scalar = scalar
        self._cursor = None
        source._check_source_open_exact()
        if type(length) is not int or length < 0:
            raise ValueError("invalid persisted row length")
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

    def read_next_neighbor_exact(self):
        if self._cursor is None:
            raise StopIteration
        try:
            self._source.events["record_fetches"] += 1
            record = self._cursor.fetchone()
            if record is None:
                raise ValueError("prepared row shorter than persisted length")
            self._source.events["record_reads"] += 1
            self._remaining -= 1
            if self._remaining == 0:
                self.close_current_cursor_exact()
            return record[0] if self._scalar else record
        except BaseException:
            self.close_current_cursor_exact()
            raise

    def close_current_cursor_exact(self):
        if self._cursor is not None:
            try:
                self._cursor.close()
            finally:
                self._cursor = None
                self._source._active_cursors.discard(self)
                self._source.events["cursor_closes"] += 1
                self._source.events["active_cursors"] = len(self._source._active_cursors)

    def __iter__(self):
        return self

    def __next__(self):
        return self.read_next_neighbor_exact()

    def close(self):
        self.close_current_cursor_exact()


class SqlitePreparedFaultSource:
    """Replay a trusted artifact in a pinned, read-only SQLite transaction.

    Complete-row child iterators live only until their outer iterator advances
    or closes. Close interrupted iterators explicitly (or close the source).
    At most max_open_cursors record cursors and one metadata cursor can exist.
    The executor must pass the exact cover set used by the builder, not merely
    another valid edit cover. This source neither checks that handoff nor
    retains a resident cover copy; snapshot checks alone cannot validate it.
    """

    def __init__(self, path, *, cache_kib=1024, max_open_cursors=8):
        validate_positive_option_exact(cache_kib, "cache_kib")
        validate_positive_option_exact(max_open_cursors, "max_open_cursors")
        self.max_open_cursors = max_open_cursors
        self.cache_kib = cache_kib
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
            header = self._fetch_source_metadata_exact(
                "SELECT schema_version, snapshot_id, metadata_json FROM manifest WHERE singleton=1")
            if header is None or header[0] != SCHEMA_VERSION:
                raise ValueError("unsupported prepared snapshot schema")
            self._snapshot_id = header[1]
            self.preparation_metadata = json.loads(header[2])
            counts = self.preparation_metadata["source_events"]
            self.vertex_count = counts["original_vertices"]
            self.factor_count = counts["factors"]
        except BaseException:
            self.close()
            raise

    @property
    def snapshot_id(self):
        return self._snapshot_id

    def _check_source_open_exact(self):
        if self._closed:
            raise ValueError("prepared source is closed")

    def _check_source_snapshot_exact(self, snapshot_id):
        self._check_source_open_exact()
        if snapshot_id != self.snapshot_id:
            raise ValueError("stale or foreign snapshot ID")

    def _fetch_source_metadata_exact(self, statement, parameters=()):
        self._check_source_open_exact()
        self.events["metadata_reads"] += 1
        return fetch_sqlite_single_record(self._connection, statement, parameters)

    def _fetch_vertex_counts_exact(self, original_id):
        validate_sqlite_identifier_exact(original_id)
        record = self._fetch_source_metadata_exact(
            "SELECT membership_count, deleted_degree, inserted_degree FROM vertices WHERE vertex_id=?",
            (original_id,))
        if record is None:
            raise ValueError("unknown original vertex ID")
        return record

    def open_cover_deletion_row(self, snapshot_id, original_id):
        """Return ALL deleted neighbors, including for noncover row owners."""
        self._check_source_snapshot_exact(snapshot_id)
        length = self._fetch_vertex_counts_exact(original_id)[1]
        return SqlitePreparedRowCursor(self,
            "SELECT neighbor_id FROM neighbors WHERE kind=0 AND vertex_id=? ORDER BY neighbor_id",
            (original_id,), length)

    def open_factor_core_posting(self, snapshot_id, factor_id):
        self._check_source_snapshot_exact(snapshot_id)
        validate_sqlite_identifier_exact(factor_id)
        record = self._fetch_source_metadata_exact("SELECT core_count FROM factors WHERE factor_id=?", (factor_id,))
        if record is None:
            raise ValueError("unknown factor ID")
        return SqlitePreparedRowCursor(self,
            "SELECT vertex_id FROM core_postings WHERE factor_id=? ORDER BY vertex_id", (factor_id,), record[0])

    def iterate_vertex_factor_memberships(self, original_id):
        length = self._fetch_vertex_counts_exact(original_id)[0]
        return SqlitePreparedRowCursor(self,
            "SELECT factor_id FROM memberships WHERE vertex_id=? ORDER BY factor_id", (original_id,), length)

    def iterate_vertex_deleted_neighbors(self, original_id):
        return self.open_cover_deletion_row(self.snapshot_id, original_id)

    def iterate_factor_cover_members(self, factor_id):
        return self.open_factor_core_posting(self.snapshot_id, factor_id)

    def iterate_complete_edit_edges(self):
        counts = self.preparation_metadata["source_events"]
        return SqlitePreparedRowCursor(self,
            "SELECT left_id, right_id FROM edits ORDER BY left_id, right_id", (),
            counts["deleted_pairs"] + counts["added_pairs"], scalar=False)

    def iterate_inserted_cover_edges(self):
        return SqlitePreparedRowCursor(self,
            "SELECT left_id, right_id FROM inserted_cover_edges ORDER BY left_id, right_id", (),
            self.preparation_metadata["inserted_cover_pairs"], scalar=False)

    def iterate_complete_vertex_rows(self):
        rows = SqlitePreparedRowCursor(self,
            "SELECT vertex_id, membership_count, inserted_degree FROM vertices ORDER BY vertex_id", (),
            self.vertex_count, scalar=False)
        try:
            for vertex, membership_count, inserted_degree in rows:
                with closing(SqlitePreparedRowCursor(self,
                    "SELECT factor_id FROM memberships WHERE vertex_id=? ORDER BY factor_id",
                    (vertex,), membership_count)) as memberships:
                    with closing(SqlitePreparedRowCursor(self,
                        "SELECT neighbor_id FROM neighbors WHERE kind=1 AND vertex_id=? ORDER BY neighbor_id",
                        (vertex,), inserted_degree)) as inserted:
                        yield vertex, memberships, inserted
        finally:
            rows.close()

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
