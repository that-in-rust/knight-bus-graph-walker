# Boolean PageRank SQLite Source Evidence

Date: 2026-09-21. A01 source lane only. Owned additions: this note,
`experiments/boolean_rank_sqlite_source.py`, and
`experiments/test_boolean_rank_sqlite_source.py`. No commits or edits to other
files. The numerical solver, publisher, and actual-output certificate belong to
the lead's concurrent lane and are not implemented or certified by this note.

Lead integration correction: original IDs are now restricted to nonnegative
signed-63-bit values, matching the prepared pipeline and `<Qd` output contract.
Negative-ID rejection was first observed failing, then fixed in validation and
the schema. Old signed-ID fixtures were updated, without changing the graph
semantics. Source tests and the full 61-test integration suite pass; see the
[complete implementation evidence](PageRank-Boolean-Streaming-Evidence.md).

## Frozen API

```python
build_boolean_rank_source(destination, rows, *, factor_count,
                          cache_kib=1024, batch_records=4096) -> dict
SqliteBooleanRankSource(path, *, cache_kib=1024)  # context manager; .close()

source.snapshot_id          # str; per-build UUID, not a content hash
source.vertex_count         # int; complete original universe
source.factor_count         # int; supplied F, including inactive factors
source.class_count          # int; all nonempty signature classes, including isolates
source.active_class_count   # int; classes with degree > 0
source.total_weight         # Fraction; exact sum of all input weights
source.isolate_weight       # Fraction; exact sum over degree-zero vertices

source.iterate_class_records()
# (class_id, groups_tuple, h, degree, weight_sum: Fraction), ordered class_id
source.iterate_class_vertex_rows(class_id)
# (original_id, weight_float), ordered original_id within the class
source.iterate_active_factor_counts()
# (factor_id, active_member_count), EVERY factor 0..F-1, including zero counts
```

Every iterator supports `.close()`, context management, and a read-only `.length`.
Every source property above is read-only. Unknown, negative, Boolean, or non-int
class IDs raise `ValueError`. Closed-source iterator opens raise `ValueError`.
Exhausted/closed iterators raise `StopIteration`. The cursor reservation is a
read-only constant of eight; exceeding it raises `ValueError` before opening a
ninth SQLite record cursor. One short-lived metadata cursor may additionally
exist. Normal nested class/vertex streaming needs at most two record cursors.

Class IDs start at zero and follow lexicographic canonical signature order,
using `(-1, -1)` for empty, `(a, -1)` for singleton and `(a, b)` for a sorted pair.
Classes have at least one vertex; the **empty membership signature** is retained
when present. All original supplied group IDs are retained, without remapping.
Nested class/vertex scans provide deterministic complete output order, not a
globally original-ID-sorted output. No all-vertex iterator is exposed.

## Input And Personalization

Rows are `(original_id, memberships_iterable, weight_float)`.

- Original IDs use `0 .. 2**63-1`, the nonnegative part of SQLite's integer
  domain; negative, bool and non-int IDs are rejected. Factor IDs and
  `factor_count` are nonnegative integers, with `factor_count <= 2**63-1`.
  Every membership must be in `0..factor_count-1`.
- Weights require `type(weight) is float`, finite and nonnegative. All integer
  inputs, including exactly representable integers, are rejected deliberately;
  there is no silent conversion. Negative zero is accepted and its bits retained.
- Duplicate memberships are canonicalized using a set of at most two elements.
  A third distinct membership is rejected immediately; duplicate original IDs
  are rejected by the SQLite primary key. Input iterators are not restarted and
  remain caller-owned; the builder does not close them.
- Empty input is allowed at any admitted F. A nonempty universe requires a
  strictly positive exact total, so an all-zero nonempty input is rejected.
- `cache_kib` and `batch_records` must be actual integers in `1..2**31-1`.

The query contract is **exact normalized binary64 personalization**:

```text
w_v = Fraction.from_float(input_weight_v)
W   = sum_v w_v, exactly
p_v = w_v / W, exactly (when n > 0)
```

This does not mean rounding each p_v to binary64 before solving. A class record's
weight is the unnormalized exact W_H; isolate_weight is unnormalized W_Z.
The solver must form `W_H/W`, `W_Z/W`, and per-vertex `w_v/W` consistently.
Empty input has total_weight = isolate_weight = Fraction(0), without normalization.

Each weight is persisted as an eight-byte big-endian IEEE binary64 BLOB and
decoded to the identical float bits. Streaming `Fraction.from_float` additions
produce both total and class weights. There is no SQLite `SUM(real)`, floating
aggregate, or rounded total, including when the exact total exceeds finite
binary64 range. Fraction numerator/denominator are persisted as decimal TEXT.
All finite binary64 weights are dyadic with denominator dividing `2**1074`.
The sum denominator remains bounded by `2**1074`; numerator width grows with
`log(n)` rather than with a list of n denominators. Given the explicit vertex
count limit `n <= 2**63-1`, a loose numerator bound is 2161 bits. Fraction/GCD
arithmetic and decimal encoding are nevertheless paid work, not unit-time magic.

## Preparation And Degrees

The builder first creates all F factor rows on disk, then consumes vertices
once, validates IDs/weights/memberships, and updates exact integer factor counts.
The SQLite signature index is built explicitly after ingestion. An index-ordered
vertex scan retains one current signature, h, and Fraction W_H; it writes each
finished class without retaining an n/P/F-sized Python collection.

With original member counts N_a, the exact degrees are:

```text
empty:       0
singleton:   N_a - 1
pair {a,b}:  N_a + N_b - h - 1
```

Factor counts for the solver are accumulated only from positive-degree classes.
If a group contains an isolate, its original size was one; therefore deleting
isolates cannot change a positive-degree vertex's group counts or degree. A factor
with active members has equal original and active counts. Size-one factors on
an otherwise active vertex remain present. The provider retains **all supplied
F**, unlike the theorem note's optionally pruned active-F convention. The lead
must charge its full-F vectors/counts and handle zero active counts.

SQLite integer CHECK constraints reject counter promotion to REAL, Python
calculates degrees as exact integers, and degrees are checked against the full
vertex universe before storage. There are no adjacency, edge, clique-expansion,
or class-pair product tables.

## Lifecycle

Construction uses a uniquely owned temporary directory beside the destination,
DELETE journaling and FULL synchronization. Only after manifest commit and
connection close does `os.link` publish the database without replacement.
Existing paths, dangling symlinks, and a raced-in destination are preserved.
Input, index/class/manifest preparation, and publication failures clean up the
owned temporary directory. The destination parent must already exist; the
filesystem must support same-filesystem hard links. This is atomic visibility,
not a directory-fsync or power-loss durability guarantee. A successful link is
the publication boundary, not a promise to roll it back after an OS cleanup error.

Readers use an escaped file URI with `mode=ro`, `query_only=ON`, and an explicit
BEGIN followed by a manifest read that pins the SQLite snapshot. They do not use
the unsafe assumption `immutable=1` for pinning. A WAL test confirms that a live
reader retains old rows while another connection commits; reopening sees the
new rows. That mutation is a test of SQLite pinning, not an admitted artifact
update workflow: published builder output must remain immutable to consumers.
UUID identity alone does not detect tampering or a rewritten artifact.

Each record read uses exactly one explicit `fetchone()`. Stored counts avoid an
extra end-of-stream fetch for provider iterators; the builder's class scan does
one additional EOF fetch. No `fetchall`, `fetchmany`, or class buffering is used.
Exhaustion, explicit close, decode/read exception, or source close closes record
cursors. Closing does not drain a stream. Iterators are independent: closing an
outer class cursor does not close a separately opened vertex cursor. Use `with`
or `contextlib.closing` when stopping early, or close the source. An ordinary
`break` is not automatic cursor cleanup. Caller-retained yielded rows are outside
the provider's bounded-state claim.

The reader trusts builder output: schema version is checked, but it does not
validate arbitrary hostile SQLite schemas, metadata, counts, weights, or IDs.
The connection/cursors use SQLite's ordinary same-thread restriction.

## Accounting

Let C be all represented signatures, P the active subset, F the supplied factor
count, n the complete original universe, and m the raw membership items consumed,
including repetitions. C <= n, and P may be Theta(F^2).

| Paid Item | Explicit Cost Or Limitation |
| --- | --- |
| Input and original-ID uniqueness | n inserts into a primary-key B-tree, m membership validations, up to 2n factor-count updates, n exact Fraction additions |
| Supplied factor domain | F disk rows and F initialization events, even for empty/inactive groups; a factor scan returns F records |
| Signature preparation | O(n)-sized persistent signature index and SQLite sorting/index-building work; not a free oracle |
| Class preparation | One ordered n-row scan with table lookups through the index, n exact Fraction additions, C class inserts, up to 2C factor lookups/updates, exact isolate accumulation |
| On-disk artifact | O(n + C + F) records plus all B-tree, schema, manifest, and exact-number encoding overhead; final byte count includes these |
| Query class pass | C decoded rows, including isolates; filtering active classes is consumer work |
| Nested original output pass | C indexed class metadata lookups plus n vertex reads; the signature index orders vertices without a new query-time sort |
| Provider Python state | Fixed-field metadata/events, up to eight record cursors plus one temporary metadata cursor, current rows and exact scalars; no n/P/F-sized provider dictionaries/vectors |
| Builder Python state | At most two membership IDs, current row/class, exact total/class/isolate scalars, counters and receipt fields; no resident class or vertex map |
| SQLite and OS | Connection/statement caches, B-tree pages, sorter buffers, journal/temp files and OS cache all remain charged; `cache_kib` is not total physical RAM |
| Other lanes | Numerical vectors, repeated class passes, output bytes, certification scratch, complete-output rereads and actual byte certification are additional |

`cache_size=-cache_kib`, `temp.cache_size=-cache_kib`, `temp_store=FILE`, and
`mmap_size=0` are set on builder and reader connections. SQLite may still retain
sorter/runtime memory or cached pages. No RSS, cgroup, physical-memory, or peak
scratch-space bound is claimed. `batch_records` controls commits, not RAM;
index creation is one paid operation outside that batching. Input/container
allocations made by the caller are not made bounded by this module.

The returned receipt has fixed field sets, not per-vertex/class logs:

- `source_events`: vertices, factors, raw membership_items, canonical memberships.
- `builder_events`: class_scan_rows, classes_written, factor_count_reads,
  active_factor_updates, commits, indexes_created. `commits` counts explicit
  commit calls, including no-op calls; `indexes_created` counts the one explicit
  signature index, not schema-created primary/unique indexes.
- `phase_seconds`: setup (including schema/implicit indexes), factors, ingestion,
  indexing, classes. All are elapsed wall-clock measurements.
- `preparation_seconds`: start through class preparation, before manifest write.
- `elapsed_seconds`: start through manifest write/commit, close, final stat,
  publication and temporary-directory cleanup; this is the complete build charge.
- `database_bytes`: final file size, **not** peak scratch/journal space or physical
  I/O bytes. These unmeasured quantities must not be reported as zero.
- `snapshot_id`, `cache_kib`, `batch_records`.

The manifest stores preparation metadata; post-publication elapsed time and
final file bytes are returned only in the receipt. Provider `events` contains
row_opens, record_fetches, record_reads, metadata_reads, cursor_closes,
active_cursors, peak_cursors. These are logical API counters, not disk-page I/O
or all SQLite VM operations. Field counts are fixed; integer bit widths can grow
with repeated use. Metadata/setup SQL and exact decoding are real paid work.

## Verification

TDD order: ten contract tests were written first and all ten failed with the
expected assertion `Boolean SQLite provider is missing`. Implementation made
those tests pass. Six further hardening tests exercise ordering, membership-stream
boundaries, injected preparation failures, constructor/decoder cleanup, and
indexed query plans. Only tiny tests were run; no CPU-heavy benchmarks.

```sh
/Users/amuldotexe/.local/bin/python3.11 -B -m unittest discover \
  -s research_algorithms_20260920/experiments \
  -p test_boolean_rank_sqlite_source.py -v
```

Observed: **16 tests passed**, Python 3.11.15, SQLite 3.50.4. The oracle uses
explicit tiny Boolean adjacency, intentionally retaining dictionaries only in
test fixtures. It checks all class rows, vertex rows, degrees, exact weights,
counts and factor IDs on 16 exhaustive two-vertex signature layouts and eight
seeded seven-vertex layouts, plus targeted cases. Shared two-group membership
counts as one edge. Empty/all-isolate inputs, zero individual weights, duplicate
memberships, extreme IDs, smallest subnormal, largest finite float, overflowing
binary64 totals, and signed-zero bit preservation are covered.

Lifecycle tests cover one-row fetch instrumentation, cursor limits/early close,
read-only enforcement after disabling query_only, a real pinned WAL snapshot,
duplicate/invalid rows, missing/unsupported sources, partial-input exceptions,
index/class/manifest failures, destination races and hard-link failures. EXPLAIN
QUERY PLAN checks found no temporary B-tree in provider scans on the tiny fixture.
These tests are not a hostile-file security proof or a crash/fault-injection
campaign against SQLite/filesystem internals.

### Tiny Receipt

The seven-vertex fixture in `test_complete_boolean_oracle`, built with F=9,
cache_kib=32 and batch_records=2, produced this observed receipt summary:

```text
vertex_count=7, factor_count=9, class_count=6, active_class_count=3
total_weight=11, isolate_weight=5
active factor counts: (0,3), (1,2), (2,2), (3,0), (4,0), (5,0), (6,0), (7,0), (8,0)
database_bytes=28672
source_events: vertices=7, factors=9, membership_items=11, memberships=10
builder_events: class_scan_rows=7, classes_written=6, factor_count_reads=8,
                active_factor_updates=5, commits=15, indexes_created=1
provider events after one nested pass plus one factor pass:
  row_opens=8, record_fetches=22, record_reads=22, metadata_reads=7,
  cursor_closes=8, active_cursors=0, peak_cursors=2
preparation_seconds=0.005288999993354082
elapsed_seconds=0.0059332080418244
```

This single timing is bookkeeping evidence, not a benchmark, speedup, scaling
result, physical-memory measurement, or claim that F-state solving wins over a
P-state quotient. Solver/publisher/certificate integration remains the lead's
separate verification responsibility.

### Lead Integration Report

The lead subsequently reported that 24 random prepared snapshots, each exercised
with both solvers, produced complete outputs satisfying a separate rational
oracle, and that all five tests in its new `test_boolean_rank_pipeline.py` passed.
This is attributed integration evidence, not a second execution by this source
lane. The lead owns those pipeline files; this lane did not edit them. No further
timing runs were performed after the lead requested timing isolation.
