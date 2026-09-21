# Similarity Overlap Integration Review

Date: 2026-09-20. Scoped independent code review, not a repeat of the theorem review.

## Findings

**No actionable correctness defect or extra-DP-budget violation found in the inspected kernels under their declared probe contract.** No kernel change is requested on the evidence gathered here. The admission wrapper, exact feasibility preflight, one-solver dispatch, refusal fallback, and cumulative reservation accounting agree with that contract. Complete-result checks also found no top-k, self, tie, or zero-tail mismatch.

This is a bounded inspection and execution verdict, not production certification. It assumes valid immutable normalized snapshots, unique original IDs, the supported `G<=256` feature tree, and ordinary Python execution with assertions enabled. Physical memory, elapsed-time superiority, crash durability, adversarial file mutation, and publication priority were not established.

**Nonblocking test-coverage limitation in the inspected test version:** the three-row file matrix does not exercise the admitted pair solver. Its first two-row block is encountered before the heap is full, and its only later block is a singleton. Thus all 8,192 combinations can succeed without entering the new pair-DP branch. The dedicated four-row pruning test does exercise one such call, but those two tests do not protect cumulative nonzero query-budget exhaustion across multiple pair calls. See [the matrix](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/test_admitted_overlap_similarity.py:87) and [the dedicated pruning case](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/test_admitted_overlap_similarity.py:57). The independent multi-block cases below cover that risk for this review. The lead subsequently reports adding cumulative-budget and other regression cases; that newer test version was not reinspected, so this is not a claim that the gap remains in the latest suite.

## Inspected Code

The requested paths are under `research_algorithms_20260920/experiments/`.

| File | Inspected scope |
|---|---|
| [probe_admitted_similarity_bounds.py](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_admitted_similarity_bounds.py:1) | Entire 70-line admission and dispatch module |
| [probe_selective_capacity_similarity.py](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_selective_capacity_similarity.py:147) | Entire selector, interval bound, and supporting selective-index layout/build code |
| [probe_overlap_frontier_similarity.py](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_overlap_frontier_similarity.py:1) | Validation, both table builders, both root evaluators, and their resource counters |
| [test_admitted_overlap_similarity.py](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/test_admitted_overlap_similarity.py:1) | Entire new test file; inspected, not run as a suite |

Supporting reads were limited to the existing selective tests, laminar bound/sidecar helpers, fixture loader, and the fixed-record reader, snapshot ID lookup, and output arena used by the selector. No driver implementation or public experiment was reviewed.

The three kernel files had identical SHA-256 hashes before and after the execution checks:

```text
admitted:  9df3a59fa7dbb445ed298ffc4b6dd0a8b66e1baabcf4c6186d0071c7e633ab7c
selective: 961594203c21e9d953824109b4c1356f91527b79b9bc9c96161cbab68b2e9a46
frontier:  db4adc5334865f7758d808ebe56682188ba0e54f93a83d32eae40fee4c4cacee
```

Only this memo was retained. No source/test file, previous review, or existing index was edited. Independent file fixtures were created outside the repository in temporary directories and removed; Python bytecode writing was disabled.

## Admission and Dispatch

### Before Large Tables

[Resource estimation](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_admitted_similarity_bounds.py:9) validates tree dimensions, integer counts, local attained-capacity ranges, source size, and root size/intersection consistency. Its populations, query totals, spans, gaps, and dense bounds have lengths proportional to `G`. It does not loop over `I`, `Q`, or an estimated table size when calculating the reservations.

The wrapper validates nonnegative integer budgets, computes reservations, and filters choices before invoking a solver. A zero budget still pays this `O(G)` preflight, but allocates neither DP table. A direct check with `a=ell=C=H=10^12`, `G=1`, and both caps 1,000 returned `None`; both solver entry points were patched to fail if called. This tests refusal before large DP allocation, not a physical-RAM bound.

Malformed or unsupported metadata raises `ValueError`; it is not converted into a pruning result. The shared validator has an explicit hard group limit of 256. Raising a selector's configurable `group_cap` does not raise that kernel limit; larger trees are outside this reviewed supported domain.

### Exact Feasibility Preflight

The reverse tree pass implements the previously proved gap-domain recurrence at [lines 20-35](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_admitted_similarity_bounds.py:20). In code, `spans[v]` is the maximum feasible size difference, not the largest intersection:

```text
leaf: T=2C-H
d=A+B-C=0: T=T_left+T_right
d>0: require d<=min(T_left,T_right), then T=max(T_left,T_right)-d.
root: require C-ell<=T_root.
```

The earlier `I=ell+C-H>=0` and `ell<=C` checks supply the other root restrictions. The pass uses full local domains, not incorrectly truncated child domains. Its only iteration is over tree nodes. The impossible summary `a=0, ell=1, caps=[0,3,2,2], q=[0,0], H_leaf=[2,2]` was rejected before either solver could be invoked, even with zero budgets.

### Conservative Schedules

For dense DP, each node reserves at most two orientations for every intersection through `min(I,2C-H)`, removing the duplicate orientation only at `e=2C-H`. This bounds the actual dictionary entries even if the subtree has additional infeasible intersections. Child reservation products bound all attempted transitions, including rejected ones. The wrapper reserves and reports

```text
dense work = 2 * states + transitions.
```

The estimate uses reserved states/products; the actual charge uses the dense solver's recorded states/transitions. Passing the full reserved work as the dense kernel's transition cap is conservative: its transition-only check is weaker, but the wrapper's preflight already includes the state charge. See [estimates and dispatch](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_admitted_similarity_bounds.py:36) and [dense enumeration](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_overlap_frontier_similarity.py:31).

For threshold DP, the estimated cell count and work schedule match the existing kernel's loops: leaf assignments, both-role convolution pairs, strict-node fixed-child scans and branches, and both root table scans. The root scan is included once in the schedule and is actually counted by the root evaluator. See [threshold schedule](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_overlap_frontier_similarity.py:94) and [root extraction](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_overlap_frontier_similarity.py:177).

Auto dispatch minimizes `(reserved_work,reserved_cells,solver_name)` among admissible choices. Forced strategies do not silently try the other solver. Only the selected function is called, and the returned metrics contain integers rather than retained frontier tables. These are logical schedule comparisons, not evidence that auto chooses the lower CPU time or smaller physical allocation.

## Selector Integration

### Refusal and Cumulative Debits

The selector first establishes the exact interval bound. It invokes the additional solver only if cheaper bounds have not pruned, the heap is full, the selected mode is an overlap mode, and actual block population is two. See [the guard and dispatch](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_selective_capacity_similarity.py:225).

At each call, the work allowance is `min(pair_work_cap, pair_query_work_cap-reserved_total)`. A `None` result increments refusal count and leaves `upper` unchanged at the interval bound. A successful result, including a zero Fraction, is distinguished using `is None`, checked against the interval bound, and charged by its reservation. The query debit uses reserved rather than actual work, so unused dense capacity is not inadvertently reused. Peak cells are a maximum across calls; total work is a sum. See [accounting](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_selective_capacity_similarity.py:239).

This maintains nonnegative remaining budget inductively: every successful reservation fits the allowance passed to that call. The independent checks verified this at every call, not just at query completion.

The guard is `hi-lo==2`, correctly based on actual population rather than configured block width or eligible population after excluding self. A width-three index's final two-row block can use the solver; full three-row blocks and singleton tails cannot. Excluding self does not invalidate the original two-row metadata or its conservative bound for the remaining eligible row.

### Complete Ranking

- All source-feature occurrences for a block are consumed before its query counts are used. Source features absent from the index still remain in source size `a`.
- Pruning requires a full heap of exactly scored positive eligible rows. Fraction comparisons avoid rounded-score errors. At equal bound, pruning additionally requires block `min_id` to exceed the current worst retained original ID; an excluded self ID in that minimum can weaken pruning, not make it unsafe.
- Body scoring consumes every stored row's memberships, including self and zero rows, before excluding them from the positive heap. Heap tuples use score followed by negative original ID, yielding the specified ID tie order.
- Zero completion occurs only after exhausting and closing the merged block stream. A short final positive heap has never evicted a positive row, so it contains all eligible positives. The inverse-ID scan skips those IDs and self, then fills in original-ID order. Empty queries, absent features, oversized `k`, and `k=0` were checked.
- Output is staged after complete ranking; the reference fixed-record reader reads requested records rather than preloading entire target bodies. This is logical/payload behavior, not a physical page-I/O guarantee.

The relevant implementation is [aggregation and pruning](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_selective_capacity_similarity.py:193), [body scoring](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_selective_capacity_similarity.py:262), and [zero completion/output](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_selective_capacity_similarity.py:289).

## Independent Checks

Executed **212 complete-result calls** on small temporary file fixtures, plus three targeted admission/dispatch checks. No repository test suite, 29,440-case mathematical oracle, public driver, or timing comparison was run. The complete-result oracle independently sorts eligible rows by exact set Jaccard and original ID; the extra reservation-slack fixture has an explicit expected output.

| Check | Calls / result |
|---|---|
| Three later pair candidates; auto, forced dense, forced threshold; query budgets at zero and immediately below/on one, two, and three reservations | 18 complete outputs matched; every prefix debit and remaining allowance checked |
| Seven rows with nonphysical/negative IDs, ties, empty rows, and self; `G=1,4`, block width `1,2,3`; eight source/self/k cases; interval and all three overlap modes | 192 complete outputs matched; no pair preflight on non-pair-only layouts |
| Width-three layout with a final two-row block | One complete output matched; exactly one pair preflight |
| Conservative dense reservation strictly exceeds actual charge | One complete output matched; correct reservation-based exhaustion |
| Huge numeric counts, insufficient budgets; impossible summary; dense-only dispatch with threshold entry patched to fail | Refusal/rejection/one-solver behavior matched expectations |

All fixture query directories were removed, including those created by the selector. No failed result comparison or budget assertion occurred.

### Reproducible Budget Cases

The multi-block fixture uses query `S={0,4,1,5,2}`, first rows `(90,S union {77})`, `(91,S union {78})`, followed by three copies of the pair below with distinct IDs 10 through 15:

```text
{0,4,8,1,2}
{12,5,9,13,2}
```

Use `G=4`, block width two, and `k=2`. Every later pair reaches the extra-DP preflight while the two initial exact winners remain unchanged. Reservations are dense `(19 cells,60 work)` and threshold `(44 cells,86 work)`.

For auto/dense, query caps `0,59,60,119,120,180` admit `0,0,1,1,2,3` calls respectively. For forced threshold, caps `0,85,86,171,172,258` give the same admission counts. Refused blocks are verified using the existing interval/body path and do not change the final answers.

The reservation-slack fixture uses `S={0,1}`, initial rows `(-100,{0,1,9})`, `(-99,{0,1,11})`, followed by three copies of `{0,2,1}`, `{1,3}` with IDs 0 through 5. Use `G=2`, width two, `k=2`, forced dense, and query work cap 76. Each dense reservation is 40 but actual work is 36. The observed query admits **one** call, refuses two, and reports reserved 40 / actual 36. Debiting actual work instead would incorrectly admit a second reservation under this policy. Returned rows are exactly `(-100,2,3,2)` and `(-99,2,3,2)`.

These cases are suitable bounded additions to the new regression tests; they do not require rerunning the mathematical oracle.

## Cost and Review Limits

The extra-DP budget does **not** cap total query work. Source reading, directory probes, expanded feature/block memberships, metadata/header/population reads, the exact interval bound, preflight and repeated `O(G)` validation, body scoring, heap operations, inverse-ID zero completion, and output are outside that budget. Refused blocks can still pay preflight and all required verification costs. Exhausting the extra-DP budget therefore does not imply an early end to the query or even an end to preflight calls. This is consistent with the stated contract, not a discovered overrun.

Logical dense states, threshold cells, and schedule operations are not Python bytes, RSS, device reads, or CPU-time units. Dictionary overhead, array initialization, integer widths, Fraction operations, file handles, buffering, and output copies are not certified by these counters. The admission minimum is not a runtime-optimal dispatch theorem.

The matrix alone does not supply DP coverage; the reservation-slack case above is a useful distinct regression obligation even with the subsequently reported cumulative-budget test. Exception/sink-failure cleanup was inspected in the unchanged selector structure and existing tests but not fault-injected in this independent run. Corrupted or concurrently replaced files, crash recovery, optimized `python -O` execution, and larger-than-supported group layouts were not validated. The review does not certify any concurrently changed driver or test version.

**Closure:** the scoped code review is complete with no kernel defect found under the reviewed contract. This conclusion comes from actual code inspection and the bounded executions described above, not solely from the earlier mathematical review. Main goal7algorithms remains open.

## Terminal Checkpoint

The lead subsequently reports 59 combined passing tests, including cumulative admission of one block followed by refusal, 8,192 complete outputs, the non-pair guard, and failed-sink cleanup. These are lead-reported results, not additional independent evidence from this reviewer. The new nine-mode public driver and fixed public workloads are outside this review's inspected scope.

All commands and independent check processes launched by this reviewer returned completed before the checkpoint request. No reviewer-owned test process remains active, so none required termination. No further enumeration, suite, or benchmark was launched for this checkpoint. The limitations above remain explicitly unverified; there is no pending review work blocking the lead's serial public measurements. Only this review file was changed. This closes the scoped review, not the main research goal.
