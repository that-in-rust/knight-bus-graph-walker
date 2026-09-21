# Similarity Large-Group Workload: Independent Review

Date: 2026-09-21. Bounded resident logical-decision audit.

## Verdict

**GO for decision-level interpretation of the revised driver.** No remaining
concrete top-k, pruning, orientation, or fallback defect was found. This is not
approval of a speedup, total-work reduction, physical-memory bound, production
layout, or mathematical novelty. A positive workload claim still requires the
lead's successfully published receipt; this audit did not rerun public profiling
or certify completion of that run.

The lead-reported posting comparator defect is repaired at
`experiments/profile_large_group_similarity.py:239`: expanded target-posting
memberships are compared with the old `membership_visits`, not `L`. The old
posting kernel counts compressed intervals in `L` and expanded occurrences in
`membership_visits` (`experiments/probe_posting_merge_similarity.py:35,57`).
The frozen Facebook receipt's source 11 illustrates the distinction: `L=1`,
`membership_visits=347`. No new blocking defect was identified.

## Evidence And Scope

Read the latest driver, its six tests, the workload gate, and only relevant
normalization, scoring, interval, modal-admission, and posting-counter code.
Previously reviewed kernel proofs were not reopened. Read frozen receipt
metadata for schema checks, without changing receipts or reprocessing public
source graphs.

- All six tests in `test_large_group_similarity.py` passed using
  `python3 -B` and `unittest.TestResult`, with zero failures/errors.
- Ran 60 additional bounded checks against an independent direct-set/Fraction
  oracle or explicit error contracts. No public profiling or timing study ran.
- The separating fixture was checked at G=4,16,64, in both assignments of row
  bodies to the second pair's IDs, across all five modes. Modal plans read two
  bodies; union/interval read four; all returned the same exact oracle rows.
- Additional checks covered self exclusion, equal scores, ascending-ID zero
  fill, empty query, no overlap, k=0, k above the eligible population, an odd
  singleton tail, negative constructed target IDs, and set deduplication.
- A genuine tau=8 fixture with nine queried features in one modulo leaf fell
  back correctly for both modal modes at every tested G. Patched solver entry
  points verified that tau refusal did not call the solver.
- Mutating the second block's core `major_size` to zero caused both modal
  modes to raise `ValueError("inconsistent row sizes")`, rather than silently
  convert invalid metadata into a budget fallback.
- A tiny normalization fixture preserved a loop-only isolated node, removed
  loops and reversed duplicate edges, and formed sorted original-ID pairs
  `[2,4]`, `[5,8]`. Its four adjacency memberships equaled the target-posting
  construction count.

## Decision And Schema Checks

The driver sorts original IDs and pairs consecutive positions, not adjacent
numeric values or degree-ranked rows. Features are deduplicated sets, and
grouping uses original feature ID modulo G. The undirected loader retains
endpoint IDs, removes self-loop edges and repeated undirected edges, and counts
both adjacency directions. Input hashes and normalization dictionaries are
checked against the prior receipts. Both prior source-ID lists contain 30 IDs;
the driver consumes those lists directly, without resampling.

Heap ordering uses exact rational Jaccard score and then smaller target ID.
Self targets never enter the heap. Equal-bound pruning uses the block minimum
ID conservatively. Bounds are used only after the positive heap is full;
otherwise all relevant blocks are scored, making subsequent zero fill sound.
Singleton blocks use interval fallback and never receive a two-row modal core.

Receipt rows have schema `[target_id, query_size, target_size, intersection]`;
the score is reconstructible exactly, with zero overlap assigned score zero,
including empty/empty. Each row list is compared with the direct-set oracle.
The target-posting merge is a second result control, not an interval-compressed
I/O implementation. Self postings count as visited even though self is excluded
from answers; `posted_targets` consequently is not the old nonself
`candidate_targets` counter.

At G=4 the driver checks the old `blocks_seen`, `block_memberships`,
`body_targets`, `body_memberships`, and `zero_rows`; it does not reproduce every
old diagnostic or physical-I/O counter. Posting memberships are checked at
each G. The expected completed matrix is 900 rows of query/mode results:
2 datasets x 3 group counts x 30 sources x 5 modes. The three identical
G-independent posting controls are not three independent workload samples.

## Admission And Accounting Limits

Modal calls follow union, capacity, and interval failure to prune. Tau refusal
preserves the interval bound. Only the two named pre-enumeration budget errors
are swallowed; other solver `ValueError`s propagate. Branch admission limits
core row-record slots; dual admission limits threshold cells. These are
different logical state units, not equal memory footprints.

Successful calls debit the kernel's reservation, not just actual recurrence
work, against the query's 10,000,000-unit reservation budget. Refused calls do
not debit a reservation. The existing cumulative-budget test passed: one
admitted call and two refusals retain exact results for both modal modes.

An independent zero-query-budget separating fixture showed the distinction:

| Counter | Branch refusal | Dual refusal |
| --- | ---: | ---: |
| Requested kernel work reservation | 48 | 80 |
| Driver successful reservation debit | 0 | 0 |
| Recurrence work executed | 0 | 0 |
| Query core records copied | 0 | 4 |
| Sparse pairs consumed by kernel | 0 | 3 |
| Dual plan nodes constructed | 0 | 5 |

Both refusals still built the driver's sparse histogram; both returned the
interval fallback result. Dual preparation and planning happen before its
budget check. Their counters survive refusal, but reservation requests on
refused calls are not retained in the final driver ledger. Therefore zero
`solver_actual_work` does not mean zero refusal work, and the query reservation
cap does not cap all preprocessing, planning, filtering, or traversal work.

Other required interpretation limits:

- `source_memberships` also gives target-posting occurrences constructed, not
  all passes over input. All modes share one resident preparation containing
  both posting indexes and modal summaries; its cost is not a baseline-only
  construction cost. Summary counts and compile visits do not account for
  every allocation, sorting comparison, or temporary record.
- `modal_compile_signature_records` sums per-block temporary-record peaks;
  it is not a simultaneous process peak. `solver_states_peak` covers admitted
  solver state in its mode-specific units, not all live query/static objects.
- `supplied_*` counts descriptors available to calls, not measured accesses.
  It does not separately count supplied deviation-index keys. Those keys are
  retained in the preparation ledger.
- Cheap-filter validation/scans, posting-merge comparisons, query sorting,
  heap operations, and output serialization are not exhaustively metered.
  In particular, the union helper scans dense capacities for validation even
  when `capacity_payload_values` is zero. Payload counts are logical model
  counts, not complete resident-array reads.
- `body_memberships` sums surviving row sizes, including self bodies; Python
  set intersection is not instrumented as that many primitive accesses.
  Output is retained through normalized k and complete rows, not an output
  byte-cost measurement. Branch work units and dual recurrence operations
  are not interchangeable CPU costs.

## Attribution Rule

Compare modal branch/dual against **interval at the same G**, with identical
sources, k=10, tau=8, pair ordering, and budgets. Attribute changes versus G=4
interval first to the changed modulo observations, not automatically to the
compressed-dual theorem. Different modulo trees need not give monotone bounds.

`modal_tighter` records a stricter bound, not necessarily a changed decision.
Use `modal_extra_pruned_blocks` and the corresponding reduction in body rows
and memberships to establish incremental decisions. Report preparation,
descriptor, histogram, refusal, reservation, and recurrence counters alongside
those savings. They are heterogeneous logical quantities, not an additive net
cost or evidence of elapsed-time savings. No further open-ended checks are
requested by this audit.

## Reviewed Snapshot

SHA-256 after the comparator repair:

```text
profile_large_group_similarity.py
f2777d8c25efb809d7ca47b2409471ea97d347c49a237fadfe39e6f2971be406
test_large_group_similarity.py
d003d4a99f596316957f1bbc83305e21e4730efcb1949c46ad3b1ccdc5ba8f61
Similarity-Large-Group-Workload-Gate.md
d63869302503458f0340b1f52a05f152720daab072207836dce0ff223be125e1
```

Only this review file was authored. No lead code, frozen receipts, other files,
or commits were changed by this audit.
