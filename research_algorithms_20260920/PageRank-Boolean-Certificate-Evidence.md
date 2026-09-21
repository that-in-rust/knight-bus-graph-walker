# Boolean Actual-Bytes Certificate Evidence

Date: 2026-09-21. Independent A01 certificate lane. Only this note,
`experiments/boolean_rank_output_certificate.py`, and
`experiments/test_boolean_rank_output_certificate.py` are owned. No commits,
solver/source dependencies, benchmark timings, or physical-memory claims.

Lead integration follow-up: source cursor scopes were made explicit after a
malformed-output regression exposed two live cursors remaining on a pinned
SQLite source. Repeated refusal/cancellation tests now pass. The arithmetic
and certificate equations below are unchanged; [combined evidence](PageRank-Boolean-Streaming-Evidence.md)
records the read/replay and the later measurements separately from this lane.

## Contract And Implementation

```python
certify_boolean_rank_output(
    source, output_path, *, alpha, epsilon, precision=60,
    scratch_dir=None, max_factor_slots,
) -> dict
```

The implementation follows **Certificate For Actual Output Bytes** in
[Boolean Two-Membership PageRank](PageRank-Boolean-Two-Membership.md), not its
reduced recursive residual. The directed Decimal loop in
`experiments/probe_native_incidence_pagerank.py` is an arithmetic precedent,
but its weighted shared-membership operator is deliberately not reused.

The source is trusted, immutable, and pinned. Its prepared class records supply
validated Boolean degrees, deduplicated memberships, global original-ID
uniqueness, and exact weight totals. The verifier does not rediscover adjacency,
rederive degrees, recount active factors, or prove that source metadata encodes
the intended graph. It checks structural class/row counts, source epoch, and
alignment against fresh source cursors on both passes.

`max_factor_slots` bounds supplied **F**, including unused factors. Admission
reads only `source.factor_count` before checking the cap: no other source
metadata, Decimal allocation, output open, or scratch creation precedes it.
The separate numerical-state charge is **2F Decimal endpoint slots**, plus
constant current-class/per-row scalar state. Precision is a positive integer;
insufficient precision is allowed to refuse a correct answer.

Output is exactly `16*n` bytes of little-endian `<Qd` records: original uint64 ID
followed by binary64 score. Records must match source class-ID order, then
strictly increasing original-ID order within each class. Every source row must
appear exactly once. Scores must be finite and nonnegative; IEEE negative zero
is nonnegative and allowed. Malformed output, invalid parameters, or violated
structural metadata raise `ValueError`. Source and I/O exceptions propagate.
Well-formed output with an inadequate bound returns `accepted=False`.

## Directed Enclosure

1. The first row pass reads published scores as exact `Decimal.from_float`
   values. For each active row, divide by its trusted integer degree with
   ROUND_FLOOR/ROUND_CEILING contexts. Accumulate the two factor endpoint arrays
   and one current-class `Q'_H` interval. Accumulate actual published isolate
   mass `zeta'`, including classes with memberships but degree zero. Write one
   JSONL record `[class_id, lower_string, upper_string]` per class, including
   zero totals for isolates, into a new private temporary directory.
2. Seek the same output descriptor and scratch stream back to zero. Rescan fresh
   source cursors, joining each scratch record by class ID. For an active row,
   enclose

   ```text
   r_v = (1-alpha)*p_v
       + alpha*(p_v*zeta' + sum_{a in H} S'_a - (k-1)*Q'_H - x_v/d_H)
       - x_v.
   ```

   Isolate residuals omit all adjacency terms. The pair-class subtraction
   removes duplicate Boolean counting; subtracting the row's own `x_v/d_H`
   removes its self contribution. No analytic dangling mass, normalized output,
   solver state, equal-class-mass lifting, or reduced residual is substituted.
3. Enclose each absolute residual, sum downward/upward, and divide the upper
   sum upward by a strictly positive lower bound on exact `1-alpha`. Accept
   only if the resulting upper bound is at most exact binary64 epsilon.

Every rounded numerical operation uses explicit Decimal Context methods. The
contexts specify precision, direction, exponent range, traps, and flags instead
of inheriting ambient settings. Integers, Decimal strings, and float constructors
are exact. Personalization is enclosed from the exact Fraction
`Fraction.from_float(weight) / source.total_weight`. The damping is the exact
binary64 value, and the complement comes from the exact rational
`1 - Fraction.from_float(alpha)`, then directed conversion. This matters even
when low precision would round alpha to one or tiny alpha makes `1-alpha`
round to one in binary64. Epsilon uses `Decimal.from_float`, not its shortest
decimal spelling.

Subtraction crosses interval endpoints. Products between uncertain endpoints
only involve known nonnegative preference, dangling, and complement intervals;
signed inflow intervals are multiplied by exact nonnegative alpha. No score,
class total, or adjacency sum is clipped. The absolute-value lower bound is
zero exactly when the residual interval contains zero; otherwise its nearest
endpoint bounds the absolute value below. This is an interval identity, not an
assumption that a signed intermediate should be positive.

The trusted original operator is column-stochastic after its personalization
dangling update, so the resolvent inequality is
`||x-x*||_1 <= ||r(x)||_1/(1-alpha)`. It applies to the actual read scores and does
not require their total mass to be exactly one. The empty source returns a
two-pass empty certificate with zero residual and no class scratch bytes.

## Identity And Resource Accounting

The unbuffered read-only output descriptor stays open across both passes.
Incremental SHA-256 hashes must agree, and exact length/alignment checks run on
both passes. An in-place rewrite to other finite, correctly ordered values is
rejected even when a loose epsilon would otherwise permit those values.
Replacing the pathname does not redirect the already-open descriptor: the
receipt's hash identifies the certified bytes, not the replacement file.

Hash equality detects differences between observed byte streams, not arbitrary
adversarial rewrites that restore identical observations, future mutations, or
the semantic validity of degrees. A publisher must bind this receipt to the
same frozen output artifact before publication. Source epoch changes are
rejected, but a dishonest provider changing data under an unchanged epoch is
outside the trusted-source contract.

| Resource | Charge |
| --- | --- |
| Retained factor intervals | 2F Decimal endpoint slots; unused supplied factors included |
| Other numeric state | Constant number of per-row/current-class scalars and exact Fraction temporaries |
| Output | Two row passes, `32*n` actual record bytes read, plus EOF probes/stat metadata |
| Class scratch | One JSONL record per supplied class, written and read once; counts include isolates |
| Resident class totals | One current-class interval, no P-sized list or dictionary |
| Source work | Two class scans and two complete original-row scans, independent of provider storage details |
| Events | Four aggregate events, not one event per row/class |

The report exposes `accepted`, `residual_l1_lower`, `residual_l1_upper`, and
`l1_error_upper` as applicable Boolean/string values; `output_sha256`,
`snapshot_id`, `nrows`, `row_passes=2`, `decimal_precision`, `factor_count`,
`retained_decimal_values=2*F`, `retained_decimal_constant_state=True`,
`scratch_records`, `scratch_bytes`, read/write byte counters, `scope`, and
aggregate `events`. Scratch size is measured from the actual sequential file,
not estimated from P. Its private directory is removed on acceptance,
numerical refusal, and exceptions; caller-owned paths are never overwritten.

These are logical scalar/record counts, not constant-width byte reservations.
Decimal precision, Python objects, exact numerator/denominator bit lengths,
iterator/provider internals, I/O buffers, allocator, runtime, and OS memory all
remain additional costs. Scratch can contain Theta(F^2) class records. No disk
capacity cap, physical-RAM cap, RSS measurement, throughput, or speedup is
claimed.

## Executable Evidence

The independent resident fixture constructs explicit loop-free adjacency from
set intersections, derives degrees by summing adjacency, and solves the dense
stationary equations over exact Fractions. Its residual oracle evaluates that
expanded operator directly on Fractions of published binary64 scores, without
using the verifier's factor/class formula. The fixture is intentionally not a
bounded-memory source implementation.

| Requirement | Verification |
| --- | --- |
| BYTE-01: certify original Boolean semantics | Wrong weighted solution `(5/14,5/14,2/7)` on `(ab,ab,ac)` is refused |
| BYTE-02: arbitrary original-row preferences | 75 dense-oracle cases, including unequal preferences within a repeated pair, disconnected components, isolates, alpha 0, 0.5, 0.85, `2^-60`, and smallest positive binary64 |
| BYTE-03: enclose residual of actual scores | 100 perturbed-output cases across five precisions (1,2,7,20,60), five score exponents, and four damping values; exact rational residual lies between reported bounds |
| BYTE-04: exact binary64 query semantics | Exact epsilon equality at binary64 0.1 passes; its predecessor refuses; subnormal and maximum-finite input weights; 12 extreme-weight/damping cases |
| BYTE-05: sound cancellation and refusal | Near-one damping passes at precision 60 and can refuse at precision 2; hostile ambient Decimal precision/traps do not change the report |
| BYTE-06: complete aligned bytes | Incorrect/duplicate/reordered IDs, uint64 endpoints, truncation, trailing bytes, negative/subnormal-negative scores, NaN, infinities, negative zero, empty source |
| BYTE-07: stable read identity | In-place between-pass rewrite rejected by hash; pathname replacement still certifies original open descriptor; source epoch change rejected |
| BYTE-08: streaming state and cleanup | Generated F=17/P=136 class stream refuses premature class materialization; 34 reported factor endpoints; cap preflight before other source access; caller sentinel survives; source errors propagate unchanged and scratch disappears |

Initial TDD run: 16 test methods failed specifically because the implementation
module did not exist. After implementation all 16 passed. Six further methods
extended edge coverage. One exposed an unnecessary sorted-membership-tuple
restriction: the supplied protocol requires distinct factor IDs, not their
internal ordering. Relaxing that check made all **22 test methods pass**.

An in-memory negative control replaced exact epsilon construction with
`Decimal(str(epsilon))`; it incorrectly refused the binary64-0.1 equality case,
demonstrating the boundary test distinguishes those semantics. No source file
was changed for this mutation.

Reproduction from repository root (both commands passed, Python 3.9.6 and 3.11):

```sh
python3 -B -m unittest discover -s research_algorithms_20260920/experiments -p test_boolean_rank_output_certificate.py -v
/Users/amuldotexe/.local/bin/python3.11 -B -m unittest discover -s research_algorithms_20260920/experiments -p test_boolean_rank_output_certificate.py
```

A sample six-row fixture with signatures `(ab,ab,a,b,empty,cd)`, weights 1..6,
supplied F=7, alpha=0.85, and epsilon=1e-10 accepted at precision 60. It reported
96 output bytes, 192 output-read bytes, five scratch records totaling 406 bytes,
14 retained factor endpoints, and L1 error upper bound
`3.09199491373065703169171629686258921459826454080273300077083E-16`.
Its output SHA-256 was
`c9aa521aa9b8716dd9978d22b6b85f34a7af6228254a2cb7804b6e5e7b520f85`.

The optional older weighted-probe regression run was not fully runnable in the
available default environments: Python 3.9 lacks its `hashlib.file_digest`;
Python 3.11 passed seven of its eight tests, with the exhaustive solver test
blocked by missing NumPy. No dependencies were installed and no older file was
edited. The new verifier and all 22 of its tests use only the standard library.
Source/solver/pipeline lanes and their integration evidence are owned separately.
