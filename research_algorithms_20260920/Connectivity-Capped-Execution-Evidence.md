# Capped Connectivity Execution And Source Evidence

Date: 2026-09-21. Active A02 implementation follow-on to the proved
[capped complement schedule](Connectivity-Cover-Complement-Study.md).
This document records actual implementation and receipts as they arrive;
it is not a physical RAM cap or a publication-readiness declaration.

## Scope And Design

Preserve `run_prepared_fault_connectivity`'s current blocked-scan plan as
the default and add `core_plan="capped"`. Share normalized-input validation,
outside contraction, exact attachment and canonical output. For the new
plan, finish every outside attachment and insertion before core search.
Cache each cover vertex's full deletion-row length and core deletion degree
in O(k) state during the existing first pass.

The new factor-local helper keeps only one factor posting and O(k) marks,
links and queue state. Freeze its outside-connected seed class at entry;
meter each local deletion-row open, returned record and candidate test.
Budget is t_f*p_f. An interrupted factor retains only sound unions and one
deferred bit. All deferred factors share ONE final deletion/membership row
pass. No per-factor fallback and no expanded negative-occurrence table.

Required cancellable interface for the new plan:

```text
source.snapshot_id
source.open_cover_deletion_row(snapshot_id, original_id)
    -> cursor.length                 prepared metadata, not a query COUNT
       cursor.read_next_neighbor_exact()
       cursor.close_current_cursor_exact()
```

Capture the query epoch once. Charge the open before calling the provider,
and each record before asking for it. Verify the returned length against
the previously scanned row length; never discover EOF by fetching beyond
the budget. A partial mark/count cannot justify a union. Close the cursor
on success, cancellation and exceptions. Initial/fallback complete scans
remain explicitly paid; provider prefetch and physical pages are separate.

An independent lane owns a SQLite prepared source, its builder/tests and
[source evidence](Connectivity-Sqlite-Source-Evidence.md). It must stream
input rather than retain n-wide dictionaries, persist deletion lengths,
use bounded cursors, and price its indexes and scratch. SQLite cache knobs
do not by themselves enforce a whole-host memory budget.

## Execution Plan

1. [x] Write failing capped-mode correctness and resource tests (8 expected
   failures: unsupported `core_plan`, lead replay on Python 3.11).
2. [x] Add capped core scheduling without changing default blocked behavior.
3. [x] Verify exhaustive/seeded full labels and exact cancellation counters.
4. [x] Inspect the independent source lane and run three plans on its files.
5. [x] Record preparation, query, output and index costs separately.
6. [x] Challenge the implementation independently; preserve all limitations.

The previous goal turn was substantive progress: it supplied the reviewed
schedule, a replayed independent checker, a separate prefix-DFS executor
with a corrected preflight defect, and a verified Boolean PageRank
extension. This turn executes the identified next step, not a fresh survey.

## Implementation And Correctness Receipts

The lead implemented `core_plan="capped"` in
`experiments/probe_fault_cover_connectivity.py`, preserving the original
`blocked` default and adding an ordinary seeded `complement` control.
The latter disables only the local cap; setup, seed classification, row
opening and output are otherwise shared. It is a strong ablation for the
schedule, not an independent implementation or a substitute for native
DFS/fragment baselines.

Initial lead RED: eight tests failed because `core_plan` did not exist.
GREEN: eight tests passed in 0.367 s; the original nine blocked tests passed
unchanged in 0.286 s. The subsequent uncapped control had an observed
unsupported-plan failure, then all nine capped/control tests passed in
0.376 s. Times here are test-suite observations, not benchmark claims.

Coverage includes 16,384 exhaustive four-vertex/two-factor graphs, 500
seeded nonmonotone-ID/nonminimal-cover cases, bridge/isolate minima,
overlapping factors with global deletions, frozen seeding, exact cap
cancellation, dense negatives, and mismatched row metadata. The matching
fixture reads 64 setup records and spends 68 metered events; the blocked
comparator reads 4,096 postings. The twelve-factor replay fixture consumes
only three records per interrupted 100-record row, with 48 attempted
events, 48 fallback posting records and one shared 100-record negative
scan. Ordinary uncapped search spends 1,224 metered events on that fixture.

The source lane is terminal and closed. The lead read the full provider,
its validation tests and evidence note, then replayed its 14 tests with
Python 3.11 in 0.419 s. It accepts streaming native memberships and edits,
persists row lengths, pins read-only snapshots and atomically publishes a
new SQLite file without replacing a preexisting destination. The source
is trusted preparation output; it is not a malicious-file validator.

`test_capped_sqlite_integration.py` additionally compares 60 seeded and
five adversarial/boundary file-backed snapshots against a separate expanded
graph oracle for all three plans: 195 complete-label comparisons. Storage
read failure is injected after attachment preparation: the exception
propagates, no output is emitted, and the local cursor is closed. All
normal queries finish with zero active source cursors; no extra EOF fetch
or full-row draining is needed. A further 15 small complete binary
workflows compare the benchmark's analytic output audit with the expanded
oracle, rather than trusting only cross-plan agreement.

Important handoff limitation: source cover postings are prepared for an
EXACT cover set. Passing a different otherwise-valid edit cover is outside
the trusted interface and can give wrong labels. The measurement worker
loads the cover directly from the same prepared database, paying that read
and O(k) materialization inside query timing. The generic executor itself
does not enforce this cross-view contract.

## Measurement Protocol

`experiments/measure_capped_sqlite_workflow.py` records all observations,
not just favorable families. Five generated native sources exercise sparse
core negatives, floor-certified outside seeds, global-row replay, dense
negatives and a deleted center. Preparation consumes lazy source streams;
these are not CSV ingestion or measured customer workloads.

For each artifact, run three rounds of the three plans in cyclically
rotated order, each query in a fresh Python worker. Query time starts
before loading the exact cover and includes opening the pinned source,
the complete kernel, every original-ID label, incremental SHA-256,
buffered binary publication, flush/fsync and source closure. Python startup
and imports precede that timer. Peak process RSS still includes them.
An independent parent pass validates each saved label against the analytic
answer, and checks the output hash/count; audit time is separately recorded.

The builder receipt includes input/validation event counts and elapsed
preparation time. File size, page count and per-object `dbstat` bytes price
the final indexes. Peak preparation RAM, transient scratch, actual storage
device traffic and page-cache consumption are NOT inferred from these
counters. SQLite cache is 256 KiB for these query workers; whole-process
RSS is measured separately, not capped. Fresh workers do not create a cold
OS page cache. All timings are small, cached constructed-family evidence.
An existing native DFS/fragment implementation and Neo4j are not timed.

## Independent Implementation Challenge

The separate [scheduler review](Connectivity-Capped-Implementation-Review.md)
found no actionable correctness/workspace defect under the declared trusted
source contract. The lead read its full independent fixture, closure oracle,
list-based reference schedule and failure injector, verified the scheduler
hash `3c1b4a85c161149aeacaa467ef5871ea574f65b540c4ca93e792b0c035182bec`,
and replayed the checker with Python 3.11 in 4.791 s.

It completed 144,418 independent successful executions plus 417 injected
failures and three invalid row-length rejections. There were 14,022 capped
factor searches in the initial exhaustive/seeded/named corpus, including
stops before opens, records and candidate tests. Complete labels, every
helper union's soundness, exact requested-I/O prefixes, retained unions and
the one-pass fallback bound were checked. A close failure is propagated;
the fake provider marks its resource closed before raising, so this is not
a guarantee that an arbitrary failing storage library releases resources.

The reviewer identified a material accounting caveat, now explicit here:
`h_f` is evaluated on the actual capped run's FROZEN factor-entry state.
A separate uncapped execution can finish more unions earlier and skip a
later factor. Its aggregate receipt must not be substituted into K as
though the two executions entered every factor with identical state.
The independent checker recomputes the reference at each capped entry.
The counterexample and source-level audit remain in the review document.

Both source and scheduler reviewers are terminal and closed before timing.
No code change was required by this review. Snapshot/normalization truth,
silent same-length source corruption, arbitrary sink failures and physical
resource enforcement remain outside its scheduler-only conclusion.

## Serial Prepared-Source Measurement

Command, from the repository root:

```sh
/Users/amuldotexe/.local/bin/python3.11 -B \
  research_algorithms_20260920/experiments/measure_capped_sqlite_workflow.py \
  --output research_algorithms_20260920/evidence/connectivity-capped-sqlite-20260921
```

The command succeeded. [Full structured receipt](evidence/connectivity-capped-sqlite-20260921/receipt.json)
and [append-only event record](evidence/connectivity-capped-sqlite-20260921/events.jsonl)
retain all 45 fresh-worker observations, artifact and source hashes, all
SQLite object sizes, and all output hashes. The same directory retains five
prepared databases and 45 full binary label files. All **147,042 emitted
original-ID labels** passed the separate analytic audit. The analytic audit
was itself compared to an expanded graph oracle on 15 smaller executions.

Environment: macOS 15.3.1 arm64, Python 3.11.15, SQLite 3.50.4. Source and
review agents were closed before the measurements; the independent checker
also finished before the benchmark started. The host and OS cache were not
isolated. Three cyclically ordered samples per plan are exploratory local
evidence, not statistical confidence intervals or production tail latency.

### Paid Preparation And Stored Representation

| Generated family | n / F / k | Memberships Z / deleted pairs | Build elapsed, ms | Prepared DB bytes | Full result bytes |
| --- | --- | --- | ---: | ---: | ---: |
| Sparse core matching | 1,024 / 1 / 1,024 | 1,024 / 512 | 9.713 | 110,592 | 16,384 |
| Outside-connected seeds | 1,025 / 1 / 1,024 | 1,025 / 1,024 | 14.080 | 126,976 | 16,400 |
| Global deletion-row replay | 4,161 / 65 / 65 | 4,225 / 4,096 | 59.006 | 303,104 | 66,576 |
| Dense negatives | 128 / 1 / 128 | 128 / 8,128 | 54.749 | 290,816 | 2,048 |
| Deleted clique center | 10,000 / 1 / 1 | 10,000 / 9,999 | 95.745 | 622,592 | 160,000 |

All final database bytes include metadata, primary tables, bidirectional
neighbor rows, both membership access directions and cover-specific views.
For example, the deleted-center database pays 94,208 bytes EACH for forward
memberships and their factor index, 196,608 for neighbors, 106,496 each for
vertices and edit pairs, and smaller other objects. Those indexes are not
free. Final file sizes do not measure peak builder scratch or total writes.
The 49,995,000 potential base edges of its clique were never materialized,
but this is a supplied native clique, not a demonstration of compressing an
arbitrary expanded edge list into that representation cheaply.

### Query Through Complete Durable Output

Times are per-plan medians in milliseconds. The last two columns are the
median WITHIN-ROUND capped/control ratios; they need not equal ratios of
the first three medians. Lower ratios are better for capped.

| Family | Blocked ms | Ordinary complement ms | Capped ms | Capped / blocked | Capped / complement |
| --- | ---: | ---: | ---: | ---: | ---: |
| Sparse core matching | 480.724 | 11.675 | 11.743 | 0.02451 | 1.00577 |
| Outside-connected seeds | 246.016 | 14.630 | 14.586 | 0.05912 | 1.00342 |
| Global deletion-row replay | 34.144 | 114.610 | 36.326 | 1.06142 | 0.31841 |
| Dense negatives | 12.649 | 14.633 | 22.034 | 1.73840 | 1.51170 |
| Deleted clique center | 79.518 | 80.134 | 79.785 | 1.00335 | 0.99564 |

The sparse and seeded wins over blocked scanning are almost entirely
already obtained by the ordinary seeded complement control. They must NOT
be presented as a unique cap innovation. The replay case isolates the
value of bounding repeated global rows: 68.16% less query/output time than
uncapped search, but still 6.14% MORE than blocked scanning. Dense negatives
are the retained loss: 73.84% slower than blocked, 51.17% slower than ordinary
complement, because capped search pays a failed attempt AND shared fallback.
The deleted-center variants are effectively tied at this sample scale.

Preparation is common and paid once for all three plans. An illustrative
first-answer accounting is `build + query + output`: sparse matching is
approximately 490.4 ms blocked versus 21.5 ms capped; the replay family is
93.2 versus 95.3 ms; dense negatives are 67.4 versus 76.8 ms. These sums use
one build observation plus the median query observation, not independently
sampled end-to-end distributions. They exclude transport, general raw
normalization, process launch and independent result auditing. Freshness,
rebuilds and all retained copies would matter in a deployed workflow.

### Logical Work And Actual RAM

| Family | Provider records: blocked / complement / capped | Capped setup postings | Capped metered events | Capped fallback postings / negative records |
| --- | --- | ---: | ---: | --- |
| Sparse core matching | 1,055,232 / 7,682 / 7,682 | 1,024 | 1,028 | 0 / 0 |
| Outside-connected seeds | 533,508 / 10,244 / 10,244 | 1,024 | 0 | 0 / 0 |
| Global deletion-row replay | 33,542 / 295,558 / 38,087 | 129 | 256 | 256 / 4,096 |
| Dense negatives | 41,408 / 41,281 / 67,478 | 128 | 16,384 | 16,384 / 16,256 |
| Deleted clique center | 79,998 / 79,998 / 79,998 | 1 | 0 | 0 / 0 |

Provider record counters include original-ID scans, memberships, negatives,
edits and postings. Each worker additionally loads k cover IDs from the
same artifact before opening the provider; those reads are timed but NOT
in the provider counters. Metadata lookups are reported separately in the
receipt; none is a physical-disk-read counter. Floor seeding's zero metered
events therefore does NOT mean zero work. All query workers ended with zero
active cursors, peak two active record cursors, and no extra EOF fetches.

Across all 45 workers, measured maximum RSS was **21,626,880 to 21,823,488
bytes (about 20.63-20.81 MiB)**. Capped values overlap both controls. There
is **no demonstrated RAM reduction among these three schedules**. The
kernel's `F+k` state is a logical bound; Python/SQLite overhead dominates
these tiny observations, while cached pages and preparation peaks remain
outside that bound. This is not evidence of a 4 GB whole-host contract or
50 GB prepared-workload performance, and no Neo4j/GDS comparison was run.

## Research Decision And Next Evidence

Keep the capped composition as a validated, source-backed adaptive schedule
with a precise work theorem and a clear losing family. It is not a universal
fastest plan. The achieved step is removal of the earlier implementation/
source gap, not establishment of a new complement-search primitive or seven
publication-ready algorithms.

The next A02 contribution gate is the full-cost comparison with equally
prepared native DFS/fragment repair, including its base forest preparation,
retained indexes and pair-edit handling, on a useful native source. It is
also worth testing admission shortcuts using already-paid row lengths, but
these should not be mistaken for a new asymptotic result. A workload-specific
plan selector must not infer an uncapped counterfactual from another run's
different union state.

The full seven-family goal remains active. A01's Boolean two-membership
extension still needs a matrix-free class-stream implementation and an
actual-output certificate against equally informed numerical controls;
A03-A07 retain their specific contribution gates in the main audit. This
A02 experiment supplies evidence for the portfolio, not a replacement
objective confined to connectivity.

## Final Local Checkpoint

The combined kernel, provider and integration suites passed **36 tests in
1.392 s** after documentation integration:

```sh
env PYTHONPATH=research_algorithms_20260920/experiments \
  /Users/amuldotexe/.local/bin/python3.11 -B -m unittest \
  test_fault_cover_connectivity test_capped_fault_connectivity \
  test_fault_cover_sqlite_source test_capped_sqlite_integration -q
```

The saved receipt has five prepared artifacts, 45 complete output files,
147,042 audited labels and zero queries ending with live cursors. Their
recorded output sizes match the saved files. This verification concerns
this A02 change, not all seven research families. All involved agents and
commands are terminal. No commit or push was made; research remains active.
