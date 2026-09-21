# Connectivity SQLite Source Evidence

## Scope And Contract

This lane owns one experimental standard-library SQLite provider, its focused
tests, and this note. It does not change the engine, theorem, or other notes.
No commits, pushes, or engine performance runs are part of this lane.

Implemented public API:

```python
build_sqlite_fault_source(
    destination, original_vertices, groups, deleted=(), added=(), cover=(),
    *, cache_kib=1024, batch_records=4096,
)  # returns a constant-size preparation receipt

with SqlitePreparedFaultSource(destination, cache_kib=1024, max_open_cursors=8) as source:
    source.snapshot_id
    source.vertex_count
    source.factor_count
    row = source.open_cover_deletion_row(source.snapshot_id, original_id)
    row.length
    row.read_next_neighbor_exact()
    row.close_current_cursor_exact()
```

IDs must be Python integers (not booleans), in `0..2**63-1`. Original vertices,
including isolates, are supplied explicitly. Groups are a one-pass iterable
of one-pass member iterables; enumeration assigns factor IDs `0..F-1`.
Empty groups and repeated identical groups are allowed. Duplicate vertices,
duplicate members within a group, duplicate cover IDs, duplicate edit pairs,
unknown endpoints/members, loops, and nonnormalized pairs are errors.
Each edit must have `left < right`, and the supplied cover must hit every
edit. Deleted pairs must belong to a native clique; added pairs must not.
The provider is cover-specific; the executor must use that exact cover set.

**Trusted cross-view contract:** the cover supplied to the executor must
contain exactly the same original IDs as the cover consumed by the builder
(ordering need not match). Another valid edit cover, a subset, or a superset
is not interchangeable: persisted `C_f` postings and inserted-cover edge
views depend on that exact set. The source does not compare the executor's
cover against persisted cover IDs, retain a resident cover copy, or enforce
this handoff through `snapshot_id`. Passing a different cover violates the
prepared-source contract and can produce incorrect connectivity labels.
The caller must preserve or reproducibly regenerate the cover for engine
integration; a one-shot cover iterator consumed by the builder must not be
reused as though it were unconsumed.

The builder streams records into indexed disk tables, validates native
edges by indexed membership intersection, persists all-vertex deletion
degrees and factor-cover posting counts, and never expands clique edges.
Opening a prepared row fetches its persisted length without counting or
scanning its neighbors. Each in-range explicit read calls `fetchone()` once; close cancels
without draining. Exact reads beyond length or after close raise
`StopIteration`. Old `iterate_*` interfaces remain available.

Publication uses an owned temporary directory alongside the destination
and atomic no-replace hard linking of a closed database. Existing files and
symlinks are refused, including destinations created during preparation.
Failure removes only owned scratch. This is not a crash-durable publication
or hostile-filesystem protocol.

## Completed Plan

1. Add focused tests and observe the missing-provider failure.
2. Implement incremental preparation and the read-only, snapshot-pinned
   provider, including cancellation and the legacy iterator interfaces.
3. Verify strict validation, late-failure cleanup, publication races,
   row-prefix cancellation, sorted replay, and a generated 10,000-vertex
   clique/delete-star without invoking the engine.
4. Record exact APIs, test command/result, costs, and limitations here.

## Accounting Boundaries

Preparation, validation, indexes, disk storage, and persisted cover views
are paid separately from executor workspace. The provider reports
database bytes, preparation timing, input-event counts, and logical source
events. `cache_kib`, `temp_store=FILE`, and `mmap_size=0` configure SQLite;
they do not bound total process RSS, SQLite transient allocations, driver
read-ahead, or the OS page cache. No total physical-memory-cap claim is made.

## Progress

- Read the Source/API contract, the existing engine, and
  `PreparedResidentTestSource` using the existing graph index and targeted
  source reads.
- Initial RED: all eight initial tests failed with
  `SQLite prepared provider is missing` before implementation existed.
- Implemented builder, snapshot pinning, exact cursors, legacy iterators,
  and constant-size source/preparation ledgers.
- First implementation: 7/8 passed. Corrected a test that eagerly opened
  four independent iterators but expected closing one to close all four.
- Added direct SQLite cursor instrumentation and boundary/lifecycle checks.
- Intermediate GREEN: 12 tests passed in 0.388s, standard-library unittest.
  This is provider testing only, not engine integration or a memory benchmark.
- Final GREEN: 14 tests passed in 0.527s, including 25 deterministic small
  graph oracle cases and an injected manifest-write failure after loading.

## Test Receipt

Command run from the repository root:

```sh
python3 -B -m unittest discover -s research_algorithms_20260920/experiments -p test_fault_cover_sqlite_source.py -v
```

Result: `Ran 14 tests in 0.527s` / `OK` on Python 3.9.6 and SQLite 3.43.2.
Tests cover sorted complete replay, actual single-row `fetchone()` calls,
persisted lengths without query-time `COUNT`, immediate prefix cancellation,
stale-ID rejection before any source read, read-only URI enforcement even
with `query_only` toggled off, legacy/nested iterator cleanup, cursor
reservation failure cleanup, empty factors, isolates, maximum accepted ID,
invalid/duplicate inputs, late input and finalization failures, existing
destinations, dangling symlinks, and a concurrent destination-creation race.
Tiny oracle graphs are expanded only inside tests; the provider never does so.

Separate source-only generated-stream receipt (one observation, not a
repeatability or latency guarantee):

| Field | Observed value |
| --- | --- |
| Original vertices / factors / memberships | 10,000 / 1 / 10,000 |
| Deleted pairs / added pairs / cover vertices | 9,999 / 0 / 1 |
| Native membership-join validations | 9,999 |
| Core posting records / inserted core pairs | 1 / 0 |
| `cache_kib` / `batch_records` | 64 / 257 |
| Final database bytes | 622,592 |
| `preparation_seconds` | 0.21021625 |
| `elapsed_seconds` | 0.2109025 |
| Full deletion-row length | 9,999 |
| Returned prefix | 1, 2, 3 |
| Record fetches / returned records / cursor closes | 3 / 3 / 1 |
| Active cursors after prefix close / peak cursors | 0 / 1 |
| Metadata lookups including initial manifest | 2 |

The generated test additionally checks stored row counts: 10,000 memberships,
9,999 edits, 19,998 bidirectional neighbors, and one core posting. There is
no stored 49,995,000-pair clique expansion. This receipt measures preparation
wall time and final file bytes, not source/preparer peak RAM or physical I/O.
No engine performance test or engine integration test was run in this lane.

## Executor Handoff

```python
from fault_cover_sqlite_source import build_sqlite_fault_source, SqlitePreparedFaultSource

receipt = build_sqlite_fault_source(
    destination, original_vertices, groups, deleted, added, cover,
    cache_kib=1024, batch_records=4096,
)
with SqlitePreparedFaultSource(destination, cache_kib=1024) as source:
    snapshot = source.snapshot_id
    row = source.open_cover_deletion_row(snapshot, original_id)
    try:
        # The executor charges/admit-checks before each call.
        for _ in range(min(allowed_records, row.length)):
            neighbor = row.read_next_neighbor_exact()
    finally:
        row.close_current_cursor_exact()
```

For `run_prepared_fault_connectivity(source, cover, ...)`, the `cover`
argument must satisfy the trusted exact-set contract above. No new
cover-validation index scan, cover hash, or resident `k`/`n` copy is added
by this provider.

`open_cover_deletion_row(snapshot_id, original_id)` accepts any declared
original ID, including noncover owners and zero-degree isolates. Its row
contains ALL deleted neighbors, not just core IDs. The optional
`open_factor_core_posting(snapshot_id, factor_id)` has exactly the same
cursor contract and returns the persisted cover members of that factor.
Both methods reject stale/foreign snapshot IDs before opening a row.
Unknown IDs and invalid numeric IDs raise `ValueError`.

Existing interfaces, all with increasing IDs and complete deduplicated rows:

| Method | Records |
| --- | --- |
| `iterate_complete_edit_edges()` | `(left, right)` from D union A, lexicographic order |
| `iterate_complete_vertex_rows()` | `(original_id, membership_iterator, insertion_iterator)` |
| `iterate_vertex_factor_memberships(original_id)` | Factor IDs |
| `iterate_factor_cover_members(factor_id)` | Original IDs in that factor's cover posting |
| `iterate_vertex_deleted_neighbors(original_id)` | All deleted-neighbor original IDs |
| `iterate_inserted_cover_edges()` | Added pairs with BOTH endpoints in the cover |

Each ordinary row iterator supports `.close()`. Exhaustion closes its cursor
without an extra EOF fetch. Closing the complete-vertex iterator also closes
its current child iterators; advancing it ends the previous children's
lifetimes. Consume children before advancing the outer iterator. Interrupted
iteration must be explicitly closed, or the source context must be exited.
At most `max_open_cursors` live record cursors are admitted (default 8), plus
one temporary metadata cursor. Complete vertex scans need up to three live
record cursors. Source `.close()` is idempotent and cancels remaining cursors.

`source.events` holds fixed-size counters `row_opens`, `record_fetches`,
`record_reads`, `metadata_reads`, `cursor_closes`, `active_cursors`, and
`peak_cursors`. Opens include empty rows, although empty rows need no SQLite
record cursor. Metadata reads are one-row lookup attempts. `record_fetches`
counts explicit `fetchone()` calls, and `record_reads` successful returned
records. These are logical events, not disk-page or SQLite VM-step counters.

The returned builder receipt contains `snapshot_id`, `database_bytes`,
`elapsed_seconds`, `preparation_seconds`, `cache_kib`, `batch_records`,
`native_membership_probes`, `core_posting_records`, `inserted_cover_pairs`,
and `source_events`. The last contains six input-record counters:
`original_vertices`, `factors`, `memberships`, `deleted_pairs`, `added_pairs`,
and `cover_vertices`. Preparation metadata (except final publication time
and file bytes) is persisted and exposed as `source.preparation_metadata`.
`preparation_seconds` stops before writing the manifest/final commit;
`elapsed_seconds` includes those steps, publication, and scratch cleanup.
Database bytes are the completed SQLite file size, not peak scratch bytes.
`native_membership_probes` counts one existence-join validation per edit,
not every internal SQLite B-tree probe made by that join.

## Storage And Limits

Persisted tables contain vertices with membership/deletion/insertion counts,
factors with member/core counts, indexed memberships in both directions,
cover IDs, normalized edits, bidirectional D/A neighbors, cover postings,
inserted cover edges, and a fixed-size manifest. Disk record volume is
`O(n + F + Z + |D| + |A| + k + Z_C)` plus indexes; native base edges are
not represented. The membership-join validation is paid per edit and can
cost multiple index probes when endpoints have many factor memberships.

Builder Python state is current records/iterators and fixed-size counters,
not vertex/edit dictionaries or offset arrays. Each input event is inserted
directly; transactions commit every `batch_records` input events. Aggregate
counts and derived cover tables are separate paid SQL work, not bound by
that input-event batch setting. Caller-owned iterators are consumed once
and remain caller-owned; their own memory is not included in this claim.

The source opens an escaped `mode=ro` URI, enables `query_only`, and pins a
read transaction by reading the manifest after `BEGIN`. It does not use
SQLite's `immutable` URI shortcut. Builder and source use explicit negative
`cache_size` in KiB, `temp_store=FILE`, and `mmap_size=0`. SQLite and Python
driver read-ahead still exist. SQLite transient allocations, OS caching,
peak temporary disk usage, and RSS have not been measured here.

This provider trusts its own published artifacts and does not authenticate
files, independently revalidate every stored count, or defend against
hostile database replacement. It is single-threaded, uses stdlib sqlite3,
and fails on unsupported hard-link publication rather than using a
clobber-prone rename fallback. It does not establish production completeness
or change the connectivity theorem.
