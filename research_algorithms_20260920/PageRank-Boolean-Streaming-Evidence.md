# Boolean PageRank Streaming Implementation Plan And Evidence

Date: 2026-09-21. Active A01 follow-on; seven-family research remains open.

**Goal:** execute the exact Boolean two-membership reduction with streamed
class records and certify complete published binary64 results.

**Architecture:** paid SQLite preparation supplies class and original-ID
views. The lead implements an F-coordinate matrix-free grounded/Jacobi CG
solver and an ordinary P-coordinate class/Jacobi CG comparator. A separately
implemented verifier re-reads actual output twice and uses O(F) directed
interval accumulators plus a sequential O(C) scratch stream. Here C is all
signature classes, including degree-zero classes; P counts active classes.

**Tech stack:** standard-library Python, SQLite, packed `array('d')` vectors,
binary `<Qd` original-ID/score output, exact dyadic input weight sums and
directed Decimal arithmetic. This is research code, not a physical cap.

The design executes the existing
[Boolean manuscript](PageRank-Boolean-Two-Membership.md), not a new general
Neo4j rewrite. Its block elimination is known linear algebra; useful domain
composition, complete costs and scientific differentiation still need proof.

## Plan And Ownership

1. [x] Lead writes failing numerical/full-output tests against an expanded
   rational oracle in `experiments/test_stream_boolean_rank_solver.py`.
2. [x] Lead implements `experiments/stream_boolean_rank_solver.py` using
   F streaming coefficients, an equally informed P control and stable
   within-class personalization reconstruction. No adjacency expansion.
3. [x] Carson owns ONLY `experiments/boolean_rank_sqlite_source.py`, its
   source test and `PageRank-Boolean-Source-Evidence.md`: streamed preparation,
   immutable views, exact metadata and explicit input constraints.
4. [x] Lovelace owns ONLY `experiments/boolean_rank_output_certificate.py`,
   its certificate test and `PageRank-Boolean-Certificate-Evidence.md`:
   actual output ID/bit validation, two passes and disk class-total scratch.
5. [x] Lead integrates private output staging, certification, publication,
   and file-backed tests. Rejection must not publish an uncertified answer.
6. [x] Independent numerical review, separate certificate implementation,
   and lead implementation/certificate read and replay; preserve defects.
   This is not an independent second code audit of the certificate.
7. [x] Measure only after correctness work is terminal; price source build,
   retained indexes, solve, output, certificate and scratch separately.
   Include F>P and P>F, unequal preferences, isolates and difficult alpha.

No commits or push are part of this active research step. Previous goal turn
was progress: A02 obtained actual bounded-cursor integration, independent
code evidence and complete-output measurements including losing families.

## Numerical Contract

Input weights are finite nonnegative binary64 values; personalization is
their EXACT rational normalization, not a rounded SQLite sum. Alpha denotes
the exact supplied binary64 number, 0<=alpha<1. Empty input has empty output;
otherwise total input weight must be positive. The builder rejects repeated
original IDs and more than two distinct memberships. Original IDs are
nonnegative signed-63-bit integers; supplied group IDs are 0..F-1.

For this first provider, F includes supplied inactive groups. Their ground
equations are harmless identity equations with zero right-hand side. Charge
that full F state; do not silently report only the smaller active group set.
P is the active class count. Degree-zero classes are published analytically.
Degrees/counts used in numerical coefficients must fit exactly in binary64
integers (<=2^53); the prototype refuses larger coefficients explicitly.

For group CG, S=N*y and the manuscript's grounded L applies. Assemble the
positive ground and edge coefficients by additions; Jacobi uses the paid
diagonal. Its condition number is NOT bounded by the unscaled C estimate
without qualification. For the class control let q_H=Q_H/h_H. Its symmetric
system is

```text
(h_H*t_H)*q_H - alpha*h_H*u_H^T S = B_H
S_a = sum_(H incident a) h_H*q_H
diag_H = h_H * ((1-alpha)*(h_H-1) + d_H-h_H+1).
```

This is an ordinary exact quotient congruence. Its matvec uses two class
scans and group gathers, never P-by-P adjacency. Generic CG recurrence is
only a stopping screen; neither converged nor small recursive residual is
a publication certificate. Numerical breakdown refuses that plan. A disk
P-iterate comparator remains a required future memory control; an in-RAM
P solver does not prove that all quotient methods require resident P state.

The direct subtractive P matvec above describes the exact equation, NOT the
final floating evaluation. Independent review found a scalar condition-one
witness at `alpha=nextafter(1,0)` with apparent convergence but 0.5 actual L1
error. The implemented repair centers each group at its first class value,
gathers `T_a=sum_H h_H*(q_H-c_a)`, and evaluates
`(1-alpha)*h_H*d_H*q_H + alpha*h_H*sum_a(N_a*(q_H-c_a)-T_a)`.
This preserves the same SPD operator and two class passes. Counts, centers,
totals and compensation are paid O(F) arrays. It is ordinary numerical
centering, not an intrinsic F-solver advantage or a new numerical theorem.

Publish in class order, then increasing original ID within each class. An
ID-sorted global export requires a separately paid sort. Lift arbitrary
within-class preferences using the stable local equation, not equal shares.
Validate the original stationary Boolean operator after rounding, including
ACTUAL published dangling mass and all self/pair exclusions. No clipping or
normalization after certification.

## Resource And Failure Boundaries

Factor and class slot reservations precede vector allocation; these are
coordinate limits, not byte/RSS quotas. F CG holds no P or n changing vector.
Exact weight Fractions are dyadic while summing input, but their conversion
and normalization are paid CPU; the provider does not retain per-vertex
Fraction objects. Solver coefficients are binary64 and can fail precision.
The certificate has directed precision and can refuse a numerically useful
but unprovable result rather than falsely accept.

The source's preparation, indexes, cache and class records; publisher input
scans; verifier O(C) scratch and two full output reads; output bytes; and
runtime/OS memory are separate charges. Snapshot ID binds solver state to
one frozen source. Private staging prevents a rejected candidate being
mistaken for a certified public result. No universal speed/RAM win, customer
demand, 50 GB workload or seven-paper readiness follows from small tests.

## Implementation And Review Checkpoint

All source, certificate and numerical-review agents completed and were closed.
The lead read their implementations and replayed the combined suite: **61
tests pass in 0.566 s**, including three new measurement-driver tests. Test
durations are correctness receipts, not performance evidence.

- [Source evidence](PageRank-Boolean-Source-Evidence.md): exact normalization,
  class/ID disk views, bounded closeable cursors, no clique expansion.
- [Certificate evidence](PageRank-Boolean-Certificate-Evidence.md): original
  Boolean operator on actual bytes, two complete scans, directed intervals.
- [Independent numerical review](PageRank-Boolean-Numerical-Review.md): exact
  P/F systems, Jacobi equivalence, precision failures and centered repair.
- Lead replay before repair reproduced 552 ordinary solves, 21 operator
  cases and the 0.5-error scalar witness. After repair, the scalar error is
  zero for both plans; the 552 ordinary solves have maximum L1 error
  `3.3833285569832256e-14`. The 18 extra high-alpha solves have worst P error
  `4.904792456074901e-15`, versus about 0.775 before repair. This does not
  imply their output residual certificates all accept.
- Five lifecycle tests exposed signed-ID/output-format mismatch and leaked
  cursors on cancellation, writer failure, malformed output and numerical
  refusal. The source now admits nonnegative signed-63-bit IDs. All owned
  iterators close explicitly; twelve repeated failed/cancelled attempts are
  covered. The provider remains caller-owned and reusable.
- A sixth lifecycle test proves F/P result vectors are released before
  certification. Logical live vector phases need not overlap, although
  allocators can retain pages and peak process RSS does not decrease.
- Both numerical plans still refuse the independent dot-underflow witness
  with active weights `2^-540` and an isolate of weight 1. Scaling is future
  numerical robustness work, not silently counted as a completed result.

## Predeclared Measurement Design

`experiments/measure_boolean_rank_workflow.py` runs nine deterministic native
families, with three serial **P/F/P** triplets per family: 81 query attempts
and nine separately measured preparation processes. Controls use the repaired
centered P matvec. All query workers are fresh; the OS cache is NOT cleared.
The cases include F=8/64/128 with all pair signatures, repeated single/pair
classes, a cycle with isolates, 4,096 supplied factors but only two active,
near-one damping and the dot-underflow negative. Personalization varies.

All attempts, including refused results, are retained. A timing pair is
screened as locally stable only when the two control times differ by at
most 1.5x. That rule is declared before running; all raw timings remain.
For admitted triplets, complete original-ID files are compared in a streaming
audit using exact rational score differences bounded by the sum of their two
actual-output certificates. Per-worker hashes bind published bytes. The
certificate itself validates every original row against the pinned source.

Report preparation time/RSS, retained SQLite bytes, query solve/output/check
times, source reads and cursor counts, certificate scratch bytes, output
bytes and observed peak worker RSS. Python startup is excluded from query
timings and separately visible as subprocess wall time. Post-build metadata
inspection and post-query file hashing are outside those phase timings.
No hard memory enforcement, arbitrary graph ingestion, user-source
prevalence, cold-start result, disk-P control or Neo4j benchmark is claimed.

## Frozen Measurement Results

Raw artifacts: [receipt.json](evidence/boolean-rank-sqlite-20260921/receipt.json)
and [events.jsonl](evidence/boolean-rank-sqlite-20260921/events.jsonl), nine
prepared SQLite sources and all 63 accepted binary result files. There were
**81 query attempts**, **63 accepted complete outputs**, **18 refusals**, and
**516,816 accepted original-ID score rows**. Every admitted P/F/P triplet
passed both exact paired-file comparisons; all 21 admitted triplets passed
the predeclared 1.5x control consistency screen. No leaked source cursors;
the observed maximum was two active cursors. Agents were terminal and closed
before this serial measurement. No implementation file changed during it.

### Query Time, Including Publication And Certification

Times are milliseconds, with per-method medians. Delta is the median of the
three local `F / mean(P_before,P_after)` ratios, minus one; it is not computed
by dividing the two displayed medians. Negative means F is faster.

| Constructed case | n | Supplied F | Active P | F ms | P ms | Paired time delta | Interpretation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| All pairs, small | 112 | 8 | 28 | 4.01 | 4.29 | -6.96% | Very small absolute difference |
| All pairs, medium | 4,032 | 64 | 2,016 | 94.22 | 110.78 | -15.70% | F uses fewer class scans |
| All pairs, wide | 8,128 | 128 | 8,128 | 292.74 | 361.34 | -18.87% | Small F versus P, but full output still paid |
| One repeated pair | 20,000 | 2 | 1 | 170.43 | 171.26 | -0.20% | Essentially tied; P is already one coordinate |
| One repeated singleton | 20,000 | 1 | 1 | 165.77 | 167.63 | -0.87% | Essentially tied; no state reduction |
| Cycle plus isolates | 1,152 | 256 | 256 | 35.10 | 55.19 | -36.34% | Same dimensions/iterations, fewer class scans |
| Many unused factors | 4,000 | 4,096 | 1 | 39.23 | 36.08 | +8.34% | F loses; unused supplied slots are not free |
| Near-one damping | 56 | 8 | 28 | Refused | Refused | Not a speed comparison | Original residual bound exceeds epsilon |
| Tiny active weights | 3 | 1 | 1 | Refused | Refused | Not a speed comparison | Binary64 CG dot-product underflow |

The all-pair families took two F iterations versus three P iterations, with
four versus ten class passes including setup/recomputed residual/final gather.
The cycle took 39 iterations for BOTH plans, but 41 versus 82 class passes.
These are measurements of these implementations and sources, not asymptotic
iteration guarantees. Equal class dimensions do not imply equal scan work.

### Preparation, Storage And Memory

| Case | Build ms | Retained SQLite bytes | F modeled solver payload bytes | P modeled solver payload bytes |
| --- | ---: | ---: | ---: | ---: |
| All pairs, small | 3.99 | 28,672 | 1,152 | 3,072 |
| All pairs, medium | 30.56 | 237,568 | 9,216 | 196,608 |
| All pairs, wide | 74.52 | 577,536 | 18,432 | 786,432 |
| One repeated pair | 97.99 | 634,880 | 288 | 192 |
| One repeated singleton | 84.83 | 675,840 | 144 | 144 |
| Cycle plus isolates | 10.16 | 81,920 | 36,864 | 36,864 |
| Many unused factors | 24.64 | 192,512 | 589,824 | 196,704 |
| Near-one damping | 3.82 | 28,672 | No admitted result | No admitted result |
| Tiny active weights | 3.47 | 28,672 | No admitted result | No admitted result |

The payload columns are the conservative packed-vector model
`8*(12*dimension+6*F)`, NOT measured RSS. They exclude source/cache/runtime,
Fractions, scalar objects, certificate intervals and buffering. The repaired
P operator's counts, centers, totals and compensation fit the charged O(F)
term. Solver result arrays are released before certificate allocation, but
allocator high-water RSS can persist across those phases.

**Accepted workers observed 20.97-22.45 MiB peak process RSS, with overlapping
plan ranges. No consistent whole-process RAM saving is established.** Build
workers observed roughly 20.8-21.5 MiB. These are Mac process measurements,
not a Linux cgroup or a physical machine-wide memory reservation. No inference
from 8,128 to billions of original vertices is justified by this experiment.

For the wide case, F certificate scratch is **1,144,730 bytes**, larger than
its 577,536-byte retained database; P scratch is 1,144,774 bytes. The shared
complete output is 130,048 bytes per run. The retained/prepared size therefore
cannot be called total peak disk. For the repeated-pair case the source
implicitly represents 199,990,000 edges, but those edges were never supplied
as raw rows. The result demonstrates native membership handling, not free
compression of an arbitrary 200-million-edge import.

Certificate time takes about 57-60% of F query time in the two larger pair
cases and about 74-75% in the repeated classes. Faster CG alone has little
room to improve those repeated-class jobs. Adding the separately measured
build time to the median query times reduces the wide-case arithmetic time
advantage to about 15.7%. That sum is lifecycle bookkeeping, not an independently
measured cold first-answer run. Refresh, export sorting and recovery remain
unmeasured. Query-specific input weights are frozen into this first source;
changing personalization currently entails new preparation, not free reuse.

### Second Iteration: Diagnose The Refusals

After timing finished, the lead solved the 56-vertex near-one case with an
independent expanded exact-rational oracle. The provisional F answer has
actual L1 error `6.289259834586463e-17`; P has `8.326672684688674e-16`.
Both are much better than the requested `1e-10`, yet their valid generic
residual error upper bounds are about **0.451** and **0.500**. Both numerical
flags are false. No result was published and no frozen trial was replaced.

This is not a wrong-result acceptance. Dividing a rounded residual by
`1-alpha=2^-53` gives a valid but drastically pessimistic certificate. It
identifies a concrete next research target: graph-structure-aware certified
error bounds that separate total mass from contracting nonstationary modes.
A smaller actual error alone cannot authorize publication without a valid
efficient certificate. A higher Decimal precision also cannot erase the
actual rounded-output residual already present in the bytes.

The [mass-deflation follow-up](PageRank-Boolean-Mass-Deflation.md) supplies a
concrete theorem and exact-rational probe: charge total mass directly and use
a justified nonstationary spectral bound for the rest. Seven exact PSD checks
and 168 error-bound cases pass. On these same provisional answers, its exact
bound is about `1.41e-16` / `9.33e-16`. This is not yet directed byte-level
publication, and standard spectral/resolvent precedent is explicitly credited.

For the underflow case, both plans raise `ArithmeticError` before producing
an answer. This is a numerical range restriction, not evidence the exact
PageRank is undefined or requires large RAM. Power-of-two scaling or a cheap
absolute-error-admitted initial state are possible follow-ups and must be
tested against the original operator before being enabled.

### What This Changes

The former claim "no numerical implementation/source experiment" is now
obsolete. We have a functioning streamed builder, two ordinary numerical
plans, complete publication, actual-output verification, failed-run cleanup,
an independent numerical review and measured paired outcomes.

The remaining contribution is NOT a new Schur elimination, quotient, CG,
centering or residual theorem. This evidence supports a useful eligible-domain
execution option and exposes resource/certificate tradeoffs. Same-representation
disk-P execution, a stronger direct specialized solver for highly symmetric
families, useful public-source eligibility, physical enforcement and full
refresh economics remain necessary. Seven genuine differentiated contributions
and publication readiness remain unproved.

### Reproduction And Integrity

```sh
/Users/amuldotexe/.local/bin/python3.11 -B -m unittest discover -s research_algorithms_20260920/experiments -p 'test_boolean_rank*.py'
/Users/amuldotexe/.local/bin/python3.11 -B -m unittest discover -s research_algorithms_20260920/experiments -p test_stream_boolean_rank_solver.py
/Users/amuldotexe/.local/bin/python3.11 -B research_algorithms_20260920/experiments/measure_boolean_rank_workflow.py --directory /tmp/boolean-rank-new-run
```

The destination must not already exist; do not overwrite the frozen receipt.
`receipt.json` SHA-256:
`668906fdf7f2caba890b349f432c63e70eb3b6740f901cc3b2b4930cb213bded`.
`events.jsonl` SHA-256:
`926e28749c26c34a9240f6120a1d435813276acc7a70f5b594c96d82a2067603`.
The receipt contains hashes for all five implementation/driver files.
