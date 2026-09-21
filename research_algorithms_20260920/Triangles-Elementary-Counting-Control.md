# Elementary Arithmetic Control For Native Triangle Masks

Date: 2026-09-20. Active A07 comparison experiment. This is a constructed same-source control using established interval, CRT, histogram and Euclidean floor-sum operations, not a new graph-algorithm claim.

## Goal And Design

The [completed mathematical review](Triangles-Shared-Residue-Review.md) refuted a possible run-linear interpretation of the shared-residue sweep. A three-run input can retain only six histogram runs yet require exponentially many event visits in period bit length. Before attributing a useful separation to that sweep, implement a control that recognizes simple exact reductions without expanding periods.

The selected finite design has three routes: simplify predicates and use elementary counting; retain the existing shared-event solver for residual three-mask cases; leave fixed-dimensional lattice counting as a separately specified, unimplemented comparator. This continues the existing same-source comparison work, not a product/API redesign. No commit or push is part of this experiment.

Input is exactly three native Boolean masks with positive periods dividing L, after graph-root shifts have been applied. Output is an exact scalar cardinality and disaggregated logical counters. Validate all input before any empty/full shortcut. Intersect masks having the same declared period, remove always-true predicates, and return zero for any empty intersection. Do not change the original source masks used by local graph output.

- Zero remaining predicates: return L.
- One remaining predicate: return its cardinality times L/q.
- Two remaining predicates: use their gcd histograms and integrate their product over one gcd cycle, without periodic repetition.
- Three remaining predicates with at least two singleton masks: solve the two congruences by generalized CRT, then count hits of the resulting arithmetic progression in the third mask using exact floor sums.
- Other three-predicate cases: invoke the existing shared-residue solver with the requested event budget. The fallback imports the candidate; it is not an independent reimplementation or a proof of historical equivalence for the whole compiler.

## Verification-First Implementation Plan

1. Create `experiments/test_elementary_triangle_controls.py` with explicit missing-function assertions. Cover direct floor sums, exhaustive small two-mask inputs, complete three-mask root oracles, the exponential event adversary, shifted/incompatible singleton congruences, full/empty simplification and refusal paths.
2. Run that test file and record the expected missing-function failures before creating the implementation.
3. Create `experiments/probe_elementary_triangle_controls.py`, reusing existing strict interval validation and compact histogram construction. Keep the original shared-residue code unchanged so its counterexample and independent review remain reproducible.
4. Run focused tests and the combined triangle suite. Compare arithmetic counters with actual shared events on finite adversaries; do not call heterogeneous counter ratios speedups.
5. Record exact proofs, source attribution, paid bit/setup costs, remaining regimes, and a conclusion that changes the next research action rather than promoting another tested wrapper as an innovation.

## Established Foundations

[Gibson, A Density Chinese Remainder Theorem (2014)](https://math.colgate.edu/~integers/o22/o22.pdf), Section 3 equations (13)-(17), provides the two-mask gcd-histogram dot product. A simultaneous scan of compact histogram runs evaluates that sum without allocating one cell per gcd residue. Canonical same-period mask intersection is ordinary interval intersection.

[AtCoder Library mathematics documentation](https://atcoder.github.io/ac-library/production/document_en/math.html) defines generalized CRT and the floor sum `sum_(k=0..n-1) floor((a*k+b)/m)`, with logarithmic-in-modulus arithmetic work. The [official implementation](https://github.com/atcoder/ac-library/blob/master/atcoder/internal_math.hpp) exposes the Euclidean recurrence. The inspected native API has fixed-width input and overflow semantics; our finite Python reference must use unbounded integers and count bit costs separately. No claim of inventing floor sums or of executing that C++ library is intended.

## Exact Reductions

### Same-Period And Full Predicates

For predicates with the same q, their conjunction is the Boolean intersection of their canonical intervals in [0,q). The simultaneous endpoint scan costs linear time in the input runs; with at most three masks there are at most two such merges. Empty intersection makes the root set empty. A full [0,q) mask contributes no restriction and can be removed. These operations preserve the original root set, not just its cardinality.

The implementation first validates all three masks. A malformed later mask cannot be hidden by an earlier empty one. An empty intersection is not confused with an always-true removed predicate. These checks are necessary for a useful failure contract but are not algorithmic differentiation.

### Two-Mask Cardinality

For q1,q2, let g=gcd(q1,q2), Q=lcm(q1,q2), and A_i(s) count U_i residues congruent to s modulo g. Generalized CRT gives one root modulo Q for every compatible native residue pair, hence

```text
C(L) = (L/Q) * sum_(s=0..g-1) A_1(s) A_2(s).
```

The existing histogram constructor returns maximal constant runs covering the same interval [0,g). A two-cursor scan adds `(right-left)*value1*value2` over each intersection segment and advances every cursor ending there. It neither repeats a histogram nor enumerates g cells. The scan takes at most the sum of the two run counts; simultaneous endings only reduce work. Empty histograms here mean zero values over [0,g), not missing interval coverage.

This instantiates Gibson's published count, so it is a strong elementary control, not a new counting theorem. Its current reference shares validation and histogram construction with the candidate; the finite root oracle is separately implemented.

### Two Singletons Plus An Interval Mask

Suppose the first two predicates select only residues r1 modulo q1 and r2 modulo q2. Let g=gcd(q1,q2). If `(r2-r1) mod g != 0`, the answer is zero. Otherwise ordinary generalized CRT supplies a unique residue r in [0,M), M=lcm(q1,q2). Every allowed root in [0,L) has the unique form

```text
x = r + M*k,   0 <= k < n=L/M.
```

For a third-mask run [a,b) modulo q, define `F(n,q,M,c)=sum_(k=0..n-1) floor((M*k+c)/q)`. Its contribution is exactly

```text
F(n,q,M,r+q-a) - F(n,q,M,r+q-b).
```

To verify the signs and endpoints, write y=q*t+s with 0<=s<q. The summand becomes `floor((s+q-a)/q)-floor((s+q-b)/q)`, which equals one precisely for a<=s<b. Both intercepts are nonnegative. Disjoint runs prevent duplicate roots. This handles shifts, noncoprime periods and multiple complete repetitions; no assumption of uniformly distributed residues is made.

The floor-sum routine first strips quotient parts of slope and intercept. With `0<=a,b<m`, let h=a*n+b. If h<m, the remainder is zero. Otherwise transposing the integer-point count gives the same remainder problem with `(n,m,a,b)=(h//m,a,m,h mod m)`. The next quotient removal is a Euclidean remainder step. Thus termination and logarithmic-in-modulus arithmetic steps follow. For a=0 the routine returns before attempting a zero-modulus transformation. The reference is independently checked against direct floor summation, including zero-length sums and a 10^30-length closed-form case.

## Paid Costs And Contracts

Let R be the total input runs and b a bound on input integer bit width. Normalization uses O(R log(R+1)) comparisons and O(R) records. Equal-period intersection uses O(R) further work/state for the fixed three-mask contract. The two-mask branch adds O(R log(R+1)) histogram preparation and O(R) overlap work. A singleton branch needs gcd/inversion and O(R3 log(q3+1)) Euclidean iterations after normalization. Integers and intermediate floor sums are not constant-width bytes: intermediate sums can be much larger than the final root count, and operations on O(log L)-scale operands remain paid.

The fallback retains the candidate's O(R log(R+1)+K) arithmetic work and potential exponential K. The small number of native periods is fixed; this is not a variable-size constraint-satisfaction solver.

Metrics are deliberately disaggregated: `equal_period_merges`, `histogram_runs`, `overlap_steps`, `floor_steps`, and `events_processed`. Three retained histogram records do not mean three CPU instructions; source reads, validation, sorting, gcd and big-integer arithmetic are additional. `event_budget` limits only the repeated-event fallback. A budget of zero does not prohibit the other branches' arithmetic and is not a whole-job work, deadline or RAM contract. No least-cost selector is claimed: simple branches are chosen in a fixed order, and may have higher setup overhead on tiny cases.

The new control is a scalar operator comparison, not a second complete graph runtime. The existing graph exporter and its bL-row lower bound are unchanged. Applying this control after root alignment in the class-triple summation is exact by the same scalar contract, but that integration and its full-output cost are not measured here. None of these operations discovers a compact native-period representation in arbitrary customer data.

## Executed Results

Nine tests failed on explicit missing-function assertions before implementation. After adding the three functions they passed on the first run. The initial combined run reported 50 passing tests. A later independent review of the original shared-residue executor prompted one validation regression/fix; the latest combined command reports 51 passing tests: 15 masked/hybrid, 10 periodic-refinement, 17 shared-residue and nine elementary-control tests. The new control was outside that review's scope.

```sh
python3.11 -m unittest discover -s research_algorithms_20260920/experiments -p test_elementary_triangle_controls.py
python3.11 -m unittest discover -s research_algorithms_20260920/experiments -p 'test_*triangle*.py'
```

Actual finite coverage:

- 4,000 seeded floor-sum comparisons with direct summation; the large closed-form case is additional.
- 900 exhaustive two-mask combinations from ordered periods 1..4, using L=2*lcm(q1,q2). These include every Boolean subset, not 900 distinct period pairs.
- 900 new seeded three-mask cases with periods 1..16 and one to three complete repetitions, checked against direct root enumeration. Both ordinary reduction and shared-event fallback are exercised.
- Equal-period adversaries at N=8,64,1024,8192 and 2^80; shifted and incompatible singleton congruences; pairwise-distinct huge periods; full/empty predicate handling; invalid-input and residual event-budget refusal checks.

The table was separately executed from the retained functions. Event counts and overlap/floor steps have different units; their ratios are not speedups. No RSS, physical I/O or Neo4j comparison was run.

| Input | Exact roots | Shared-event executor | Elementary control | Control retained histograms |
| --- | ---: | --- | --- | ---: |
| Duplicate-period adversary, N=8 | 4 | 17 events | Two histogram overlaps, no repeated events | 3 |
| Same, N=64 | 32 | 129 events | Two histogram overlaps, no repeated events | 3 |
| Same, N=1,024 | 512 | 2,049 events | Two histogram overlaps, no repeated events | 3 |
| Same, N=8,192 | 4,096 | 16,385 events | Two histogram overlaps, no repeated events | 3 |
| Same, N=2^80 | 604,462,909,807,314,587,353,088 | 2^81+1 events derived only; not executed | Two histogram overlaps, no repeated events; executed | 3 |
| q=(101,103,10403), two zero singletons, third [0,10402) | 1 | 407 events | CRT plus two floor-sum iterations, no repeated events | 0 |
| q=(12,18,20), prior mixed-run fixture | 20 | 16 events | Same shared-event fallback, 16 events | 11 |

The huge equal-period example has length 2^81=2,417,851,639,229,258,349,412,352. This is an exact symbolic-cardinality calculation on three supplied intervals, not a physically stored graph of that size or a delivered local answer for every vertex.

## Rubber-Duck Findings And Research Decision

1. **The prior sweep's exponential adversary is real, but elementary simplification solves it.** Therefore there is no justification for presenting the sweep as unavoidable work for low retained state. The original reference is left unchanged to retain reproducibility.
2. **Merely comparing with a dense or repeated-boundary baseline is too weak.** The same source already makes simpler arithmetic possible; the next claimed separation must include these branches.
3. **A huge symbolic root count does not demonstrate a useful low-RAM product.** Preparation, native-source prevalence, all required output and refresh have not been measured.
4. **This control does not settle the general case.** Three distinct, nonsingleton predicates can still enter a large-H fallback. The fixed-dimensional lattice method remains an unimplemented stronger comparator, with real constants and scratch to be established.
5. **Do not count this wrapper as the next innovation.** Its scientific value is falsification and a precise remaining target: either a nontrivial composition/selection bound for genuinely coupled masks, or a measured lifecycle benefit on a useful native graph source. Another periodic encoding without that evidence would not make the seven-family objective more true.

Current status: implemented and finitely verified arithmetic comparison; no independent code review of this new control, no novelty claim, no physical memory/speed certification. The full research goal remains active.
