# Independent Centered-Join Implementation Audit

Date: 2026-09-21. Owns only this file. Read the centered probe/tests, their exact
imported helpers, [Shared-Residue Review](Triangles-Shared-Residue-Review.md), and
[Novelty Evidence Policy](Novelty-Baseline-Evidence-Policy.md). Consulted the
separate [mathematical review](Triangles-Centered-Residue-Review.md) after deriving
the distinct-minimal-period counterexample below. No code, shared journal,
portfolio, production, commit, or public benchmark was changed or created.

## Findings First

1. **Accept the exact scalar theorem and its implementation under the stated
   three-mask Boolean/integer contract.** Source inspection finds the correct
   original-mask pair moments, signed deviations, unique pair-to-H CRT lift,
   third-factor lookup, and L/Q scaling. No scalar-count defect was found.
2. **Modal centering is not globally work-optimal, as already qualified by the
   manuscript.** Unique modal centers on distinct,
   minimal periods `(15,21,35)` give pair cardinalities `(10,3,1)`. Changing only
   the first center from 0 to 1 enlarges its support from 6 to 9 but changes those
   counts to `(0,3,1)`. The exact result remains one. This is a heuristic/claim
   limitation, not an incorrect implementation of the specified modal choice.
3. **Admission is exact for two named counters, not total work.** For the chosen
   modal pair, the preflight predicts exactly the compatible support pairs and
   visited weighted-run pairs. Validation, normalization, sorting, histogram
   construction and preflight have already happened before refusal. Defaults
   `None` impose no cap. Different centers or pair choices are not searched after
   refusal, even if an alternative schedule would fit.
4. **No unconditional H-sized scan/allocation occurs in the scalar path, but
   admitted work can still be Theta(H).** Half masks with three distinct periods
   give that behavior with only three input runs. Run-sized retained storage is
   not bit-polynomial time, constant RAM, or a physical-memory certification.
5. **The added graph wrapper preserves root alignment and cumulative budgets.**
   Admission is per motif; a later motif may fail after earlier motifs were
   counted. Existing complete local output still visits the expanded vertices
   and is not accelerated by the scalar theorem.
6. **Keep the strong comparators.** The huge complement fixture is also solved
   by elementary inclusion-exclusion. Signed weighted joins/delta expansion have
   directly inspected primary precedent. Event sweeps and fixed-dimensional
   lattice counting remain controls; none of these labels proves priority for
   the precise compact periodic composition.

The final [lead manuscript](Triangles-Centered-Residue-Join.md) was also read.
It already states the modal-selection, expected hash-preprocessing, bit-cost,
per-motif admission, and unchanged-output limitations above. **No actual code
defect or contradicted current manuscript claim was found.** The concrete center
counterexample strengthens an acknowledged limitation; this audit requests no
new implementation scope or silent change to the specified plan.

## Code And Input Contract

References below describe the wrapper-inclusive probe at the SHA-256 recorded at
the end. [Probe](experiments/probe_centered_residue_triangles.py), lines 68-81:
L is a positive built-in integer, exactly three positive built-in integer q_i
divide L, and optional pair/run-pair budgets are nonnegative built-in integers.
The imported `require_integer_scalar_values` uses `type(value) is int`, rejecting
booleans, floats, integer subclasses, and coercion. No validation is suppressed.

`validate_mask_interval_runs` materializes the supplied runs, checks built-in
integer endpoints and `0<=left<right<=q`, sorts them, rejects overlap, and merges
adjacency. Thus multiplicity comes only from reduction modulo h, not overlapping
native Boolean runs. All masks are validated before empty/constant shortcuts.
Empty, full, unit, repeated, nonminimal, and noncoprime periods are supported.
Circular masks must already be split at zero; graph shifts use the existing
validated splitting helper. Malformed container types can raise normal Python
shape/type errors; this is not a general adversarial deserialization API.

`build_residue_histogram_runs` constructs a *complete* partition of `[0,h)`,
including zero-valued segments. Each native run contributes a constant quotient
plus a cyclic remainder arc. Its event map and sorted boundary lists have O(R_i)
records, not h entries. This complete-partition invariant is essential for the
two-pointer dot product, which is not independently hardened for arbitrary gapped
or overlapping helper records.

The mode code (lines 32-39) weights each histogram value by segment length, includes
zero values, and breaks ties toward the smaller value. It is not a mode of run
records. Adjacent deviation runs with different signed weights remain distinct;
only their Boolean supports are merged for support-cardinality preflight.

## Exact Theorem And Code Correspondence

Let `g_ij=gcd(q_i,q_j)`, `h_i=lcm(g_ij,g_ik)`, `H=lcm(g_12,g_13,g_23)`, and
`Q=lcm(q_1,q_2,q_3)`. Each h_i divides q_i. The incident gcd appears in both h_i
and h_j, and every common divisor of those h values divides q_i and q_j; hence
`gcd(h_i,h_j)=g_ij`. For exactly three masks,

```text
lcm(h_i,h_j) = lcm(g_ij,g_ik,g_jk) = H.
```

Thus each compatible pair of reduced residues determines exactly one t modulo H.
This is stronger than just `lcm(h_1,h_2,h_3)=H` and must not be generalized to an
arbitrary number of masks. The probe explicitly checks both identities at lines
105-106; they are theorems here, not unverified input assumptions.

For `A_i(s)=#{u in U_i:u=s mod h_i}`, CRT partitions compatible native tuples into
fibers indexed by t modulo H. Each native tuple determines one root modulo Q;
the t fiber has `product_i A_i(t mod h_i)` tuples. Therefore

```text
S = sum_(t<H) A_1(t) A_2(t) A_3(t),       answer = (L/Q)*S.
S_i = sum_(t<H) A_i(t) = (H/h_i)*|U_i|.
P_ij = sum_(t<H) A_i(t) A_j(t)
     = sum_(r<g_ij) B_i(r) B_j(r),
B_i(r) = #{u in ORIGINAL U_i:u=r mod g_ij}.
```

The pair equation uses the pair-lcm identity and aggregation from h_i to g_ij.
There is no extra H/g or Q/H multiplier. Lines 107-109 correctly build the pair
histograms from the original masks, not deviations, and lines 125-126 compute S_i.

For **any integer constants** c_i put D_i=A_i-c_i. Distributivity gives

```text
S = c_3*P_12 + c_2*P_13 + c_1*P_23
    - c_2*c_3*S_1 - c_1*c_3*S_2 - c_1*c_2*S_3
    + c_1*c_2*c_3*H + sum_(t<H) D_1(t) D_2(t) D_3(t).
```

Lines 127-129 have precisely these signs; 145 keeps the product of all three
signed weights; 148 adds the correction; 154 applies L/Q. The correction can be
negative and its magnitude need not be the number of triples. Zero pair work
eliminates the correction, not the baseline or whole answer.

A useful weighted example is q=(12,50,105), with U_1={0,6},
U_2={u<50:u mod 10 != 0}, U_3={0,15,30,45,60,75,90}. Here h=(6,10,15), H=30,
Q=2100, c=(0,5,0), and the only deviations are +2,-5,+7 at zero in their own
domains. The baseline is 70, correction -70, and result zero. Unsigned, clamped,
or merely Boolean deviation arithmetic would fail.

Python integers preserve negative differences and arbitrary count magnitudes.
The CRT division `(v-u)//g` is exact after congruence admission, including for a
negative numerator. The inverse is of h_i/g modulo h_j/g, which are coprime;
the modulus-one case is handled explicitly. Multiplication, gcd/lcm, modular
inverse, indexing comparisons, and division still have bit costs. The final
assertion `0<=S<=Q` is a useful check, not the correctness proof.

## Exact Preflight And Resource Accounting

Let E_i be the nonzero support of D_i, and d_i its cardinality. The support
histograms `F_i(r)=#{u in E_i:u=r mod g_ij}` are Boolean-support counts, regardless
of weight signs. Thus

```text
K_ij = sum_r F_i(r)*F_j(r)
     = #{(u,v) in E_i x E_j : u=v mod g_ij}.
```

Lines 111-118 compute all three exact K values, choose minimum K, break ties by
the product of weighted-run counts and then pair index, and reserve M=r_i*r_j
weighted-run visits when K>0 (otherwise M=0). The caps are checked with `>`, so
exact equality is admitted. Both counters are zero on preflight refusal. No pair
iterator, modular inverse, third-factor index, or correction loop is entered
before admission. This is exact *for this fixed schedule*; K is neither the
number of nonzero triples nor an exact instruction count.

Every weighted-run pair is visited even if its compatible output is empty, hence
the separate M cap matters. In the short-width enumeration branch, the second
run projects to at most two disjoint residue arcs. The implementation visits all
first-interval g-blocks for each arc; every interior block emits at least one
productive u, and at most two boundary blocks per arc can be empty. This differs
slightly from the tighter block bounds in the mathematical review but retains
O(1+emitted pairs) work per run pair. If the second width is at least g, every u
emits at least one v. Half-open arcs cannot duplicate a pair. Weighted runs are
disjoint, so their enumerations partition the exact K compatible pairs.

Each pair reconstructs a unique t modulo H and performs one binary search plus
containment test in the third signed-run list. The probe verifies actual K and M
equal their reservations at completion (lines 150-151). With R native runs and
r_i weighted deviation runs, its qualified arithmetic bound is

```text
preparation:  O(1 + R log(R+1))
correction:   O(M + K*(1+log(r_k+1)))
live records: O(R+1), including normalized inputs and transient run/event lists
```

This uses conventional dictionary-operation accounting; Python hash-table
pathologies and big-integer costs are not a deterministic unit-cost guarantee.
There is no materialized pair list, visited-t set, expanded-support table, h-sized
array, or mandatory t=0..H-1 loop in the audited scalar code. `range` objects in
the admitted interval enumerator are lazy, but their **iteration** is still paid.
No physical-RAM bound follows from the record count.

Caps do not bound normalization, sorting, input allocation, histogram preparation,
preflight, CRT bit cost, or lookup comparison counts. Selecting minimum K does
not minimize M, third-index cost, or total time; the implementation does not
search other pairs to satisfy a run-pair budget after selecting its minimum-K
pair. These are explicit scope limitations, not failures of its stated policy.

## Counterexample: Modal Centers Are Not Work-Optimal

Use the following three distinct, *minimal* native periods and masks at L=105:

```text
q = h = (15,21,35),          H=Q=105,
U_1 = {0,1,3,6,9,12},       runs [0,2),[3,4),[6,7),[9,10),[12,13)
U_2 = {0,3},                runs [0,1),[3,4)
U_3 = {0,1},                run  [0,2)
```

All A_i are Boolean. Their unique modes are zero: population counts (zero,one)
are `(9,6)`, `(19,2)`, and `(33,2)`. The supplied periods really are minimal, as
verified against every proper divisor in the checker; this is not an equal-mask,
equal-period, mode-tie, or nonminimal-period trick.

| Centers | Support sizes | (K_12,K_13,K_23) | Minimum K | Correction | Answer |
| --- | --- | --- | ---: | ---: | ---: |
| (0,0,0), unique modes | (6,2,2) | (10,3,1) | 1 | 1 | 1 |
| (1,0,0), alternate | (9,2,2) | (0,3,1) | 0 | 0 | 1 |

All U_2 residues are 0 modulo g_12=3, and U_1 includes the entire 0-mod-3 class
in `[0,15)`. Its complement has none, so the alternate E_1 cannot join E_2.
The baseline then supplies `c_1*P_23=1`. The only satisfying original root is zero.
Mode minimizes `h_i-population_i(c_i)` independently for each i, not compatibility
against another support's residue distribution. No implementation change is made
or proposed as automatic: choosing centers globally needs its own cost/selection
contract. The present code correctly refuses this modal plan under pair cap zero.

## Period-Scale Work And The Complement Fixture

For pairwise coprime a,b,c>=3, let q=(ab,ac,bc) and U_i=[0,floor(q_i/2)). These
are three one-run masks with distinct periods, mode zero, and r_i=1. For example
K_12 is at least `a*floor(b/2)*floor(c/2)>=H/9`; likewise for every pair. K<=H
by injectivity into t modulo H. Therefore the chosen K is Theta(H), not polynomial
in binary input length. Bounded independent checks give:

| q | H | (K_12,K_13,K_23) |
| --- | ---: | --- |
| (15,21,35) | 105 | (24,25,26) |
| (35,55,77) | 385 | (93,94,96) |
| (143,187,221) | 2431 | (603,604,606) |

The code can refuse this work without enumerating it; it cannot claim such work
never occurs. An event or lattice method can be preferable on other inputs.

For U_i=[1,q_i) instead, h_i=q_i, c_i=1, and D_i is -1 only at zero. Every K is
one and the correction is -1. Pair moments and singles yield exactly
`(L/Q)*(Q-a-b-c+2)`. The retained huge fixture uses a=2^80, b=a+1, c=2a+1;
the three factors are pairwise coprime. The independent checker uses just scalar
arithmetic for this case, not a huge list or period scan.

Elementary inclusion-exclusion is an equally direct control: forbidden roots
are multiples of ab, ac, or bc, with counts c,b,a; every pairwise and triple
intersection has one root modulo Q. Thus Q-(a+b+c)+3-1 gives the same answer.
This is not an invention of dense-complement counting. The event counter
`2(a+b+c)-3` refers to initialized literal shared streams, not a measured execution
of huge events or a speed ratio.

## Wrapper Audit

`compute_centered_period_triangles`, lines 157-188, uses the existing normalizer
and streamed native motif iterator. For a<b<c with shifts delta_ab and delta_bc,
the closing shift must be their sum modulo L. Root x at a reaches b at
x+delta_ab; consequently the second mask must be translated by **-delta_ab** into
the first root coordinate. Line 178 uses that sign and the second mask's native
period. Since every q divides L, reducing the shift modulo q is consistent.

Each triangle of the supported simple graph has distinct base vertices and a
unique root/class triple, so summing motif root counts counts it once. Existing
helper iteration enforces a<b<c and shift closure. Same-base edge classes are
rejected by the native source contract; this is not a general graph ingestion API.

Each motif receives `budget - completed_actual_work`. Scalar actual work equals
its admitted plan, so completed sums cannot overrun either cap. A later refusal
can occur after source normalization, class-triple discovery, earlier motif
counts, and its own preflight. No partially counted source is returned and no
global-before-any-work admission is claimed. Existing input/source storage,
base-neighbor probes, class-pair probes, and shift preparation remain paid.

`retained_triangle_terms=0` only describes the absence of a retained motif list;
the normalized source, class maps, adjacency and current scalar workspace still
exist. The returned source is consumed by the unchanged local-output iterator,
which iterates all `bases*L` vertices and rechecks relevant motifs in blocks. Its
degree/local-triangle output cost and buffers are **not** covered by the scalar
no-period-sweep statement. Existing source/local-output behavior was not rewritten.

## Directly Inspected Primary Precedent

This bounded audit directly inspected these two primary texts, not just titles
or search snippets. Summaries here do not establish earliest priority.

- [Koch et al., DBToaster, 2013 extended report](https://dbtoaster.github.io/papers/2013-dbtoaster-report.pdf),
  Sections 3.1-3.3, printed pages 5-7, and Figure 2's polynomial expansion rule:
  finite-support signed multiplicities, join multiplication, sum aggregation,
  additive inverse and a product-delta cross term are explicit. This establishes
  the algebraic machinery behind signed residual expansion. It does not state
  this native-interval CRT representation, mode policy, or exact K admission.
- [Kara et al., Counting Triangles under Updates in Worst-Case Optimal Time,
  ICDT 2019](https://drops.dagstuhl.de/storage/00lipics/lipics-vol127-icdt2019/LIPIcs.ICDT.2019.4/LIPIcs.ICDT.2019.4.pdf),
  equation (1), Section 1.1, and Section 2, printed pages 4:2-4:6: the triangle
  aggregate is a sum of three relation-weight products; relations map tuples to
  integers, and signed updates use distributive delta rules. Its dynamic-update
  bounds concern explicitly represented relations, not compact periodic masks.

Here is the explicit connection, derived in this audit: map s in E_1 to keys
`(s mod g_12,s mod g_13)` with weight D_1(s), similarly E_2 to keys (12,23), and
E_3 to (13,23). Each map is injective because its incident-key moduli have lcm h_i.
A matching weighted triangle is exactly a compatible reduced triple, hence one
t modulo H. Its aggregate is the correction. The present K-first plan is a binary
join followed by a third-relation lookup, not a new signed-join algebra. Expanding
these relations costs O(sum d_i) work/state, potentially enormous relative to R;
no free conversion to a stronger join engine is asserted.

The separate mathematical review records additional inspected mode/default,
functional-aggregate and worst-case-optimal join precedents. I consulted its
ledger but do not claim to have independently re-read all those papers. Likewise,
the strong four-dimensional lattice-counting reduction and inspected
Barvinok-Pommersheim procedure remain as documented in Shared-Residue Review;
they were not implemented or freshly benchmarked here. Event sweeps, elementary
controls, and lattice counting are not discarded because this kernel matches
them on some fixtures. A copied centered executor would instead be a reproducibility
control, not historical evidence. Publication-level novelty remains unresolved.

## Independent Checker

The following standard-library checker imports no project code and writes no
files. It deliberately uses dense tiny arrays, native root scans and finite sets
as independent oracles; these are not representations attributed to the lead
kernel. It checks all three pair orientations, not only the selected minimum.
Its large complement calculation uses only constant-count scalar operations.

```python
from collections import Counter
from itertools import combinations, product
from math import gcd, lcm, prod
from random import Random

pairs = tuple(combinations(range(3), 2))
checks = Counter()


def reconstruct_compatible_pair_root(u, v, m, n):
    g = gcd(m, n)
    assert (u - v) % g == 0
    reduced = n // g
    step = 0 if reduced == 1 else ((v - u) // g * pow(m // g, -1, reduced)) % reduced
    return (u + m * step) % lcm(m, n)


def verify_centered_mask_case(q, masks, multiplier=1, centers=None):
    gs = {(i, j): gcd(q[i], q[j]) for i, j in pairs}
    h = tuple(lcm(*(g for edge, g in gs.items() if i in edge)) for i in range(3))
    H, Q = lcm(*gs.values()), lcm(*q)
    A = [[sum(u % h[i] == s for u in masks[i]) for s in range(h[i])] for i in range(3)]
    modes = tuple(min(Counter(row), key=lambda v: (-Counter(row)[v], v)) for row in A)
    c = modes if centers is None else centers
    D = [[v - c[i] for v in row] for i, row in enumerate(A)]
    E = [[s for s, v in enumerate(row) if v] for row in D]
    moments, plans = {}, {}
    for i, j in pairs:
        g = gs[i, j]
        assert gcd(h[i], h[j]) == g and lcm(h[i], h[j]) == H
        original = [Counter(u % g for u in masks[x]) for x in (i, j)]
        moments[i, j] = sum(v * original[1][r] for r, v in original[0].items())
        assert moments[i, j] == sum(A[i][t % h[i]] * A[j][t % h[j]] for t in range(H))
        supports = [Counter(s % g for s in E[x]) for x in (i, j)]
        plans[i, j] = sum(v * supports[1][r] for r, v in supports[0].items())
    singles = [(H // h[i]) * len(masks[i]) for i in range(3)]
    baseline = sum(c[3-i-j] * moments[i, j] for i, j in pairs)
    baseline -= sum(singles[i] * c[(i+1) % 3] * c[(i+2) % 3] for i in range(3))
    baseline += prod(c) * H
    correction = sum(prod(D[i][t % h[i]] for i in range(3)) for t in range(H))
    dense = sum(prod(A[i][t % h[i]] for i in range(3)) for t in range(H))
    assert baseline + correction == dense
    actual = sum(all(x % qq in mask for qq, mask in zip(q, masks))
                 for x in range(multiplier * Q))
    assert multiplier * dense == actual
    for i, j in pairs:
        k, seen, subtotal, visits = 3-i-j, set(), 0, 0
        for u, v in product(E[i], E[j]):
            if (u - v) % gs[i, j]:
                continue
            t = reconstruct_compatible_pair_root(u, v, h[i], h[j])
            assert t not in seen and t % h[i] == u and t % h[j] == v
            seen.add(t)
            subtotal += D[i][u] * D[j][v] * D[k][t % h[k]]
            visits += 1
        assert visits == plans[i, j] and subtotal == correction
        checks['pair_crt_checks'] += 1
    checks['formula_cases'] += 1
    return c, tuple(map(len, E)), tuple(plans[p] for p in pairs), correction, actual


def enumerate_productive_block_pairs(first, second, g):
    a, b = first
    c, d = second
    if d - c >= g:
        arcs = [(0, g)]
    else:
        start, stop = c % g, c % g + d - c
        arcs = [(start, min(stop, g))] + ([(0, stop-g)] if stop > g else [])
    for left, right in arcs:
        low, high = (a-right) // g + 1, (b-1-left) // g
        for block in range(low, high+1):
            for u in range(max(a, block*g+left), min(b, block*g+right)):
                yield from ((u, v) for v in range(c + (u-c) % g, d, g))


options = [(q, {u for u in range(q) if bits >> u & 1})
           for q in range(1, 4) for bits in range(1 << q)]
for fixture in product(options, repeat=3):
    q, masks = zip(*fixture)
    verify_centered_mask_case(q, masks)
    checks['exhaustive_masks'] += 1
rng = Random(20260922)
for _ in range(128):
    q = tuple(rng.randrange(1, 13) for _ in range(3))
    masks = [{u for u in range(qq) if rng.randrange(2)} for qq in q]
    multiplier = rng.randrange(1, 4)
    verify_centered_mask_case(q, masks, multiplier)
    h = [lcm(*(gcd(q[i], q[j]) for j in range(3) if j != i)) for i in range(3)]
    alternate = tuple(rng.randrange(q[i] // h[i] + 1) for i in range(3))
    verify_centered_mask_case(q, masks, multiplier, alternate)
    checks['random_masks'] += 1

intervals = [(a, b) for a in range(6) for b in range(a+1, 7)]
for g, first, second in product(range(1, 6), intervals, intervals):
    actual = list(enumerate_productive_block_pairs(first, second, g))
    expected = {(u, v) for u in range(*first) for v in range(*second) if (u-v) % g == 0}
    assert len(actual) == len(set(actual)) and set(actual) == expected
    checks['interval_cases'] += 1

q, masks = (15, 21, 35), ({0, 1, 3, 6, 9, 12}, {0, 3}, {0, 1})
for qq, mask in zip(q, masks):
    minimal = min(d for d in range(1, qq+1) if qq % d == 0
                  and all((u in mask) == ((u+d) % qq in mask) for u in range(qq)))
    assert minimal == qq
modal = verify_centered_mask_case(q, masks)
alternate = verify_centered_mask_case(q, masks, centers=(1, 0, 0))
assert modal == ((0, 0, 0), (6, 2, 2), (10, 3, 1), 1, 1)
assert alternate == ((1, 0, 0), (9, 2, 2), (0, 3, 1), 0, 1)
print('modal_counterexample:', modal, alternate)

weighted = verify_centered_mask_case((12, 50, 105),
    ({0, 6}, {u for u in range(50) if u % 10}, set(range(0, 105, 15))))
assert weighted == ((0, 5, 0), (1, 1, 1), (1, 1, 1), -70, 0)
print('weighted_cancellation:', weighted)
for a, b, c in ((3, 5, 7), (5, 7, 11), (11, 13, 17)):
    q = (a*b, a*c, b*c)
    result = verify_centered_mask_case(q, [set(range(qq // 2)) for qq in q])
    assert min(result[2]) * 9 >= a*b*c
    print('half_mask:', q, a*b*c, result[2])

a = 2**80
b, c = a+1, 2*a+1
assert gcd(a, b) == gcd(a, c) == gcd(b, c) == 1
q, H = (a*b, a*c, b*c), a*b*c
assert lcm(*q) == H
P = []
for i, j in pairs:
    g = gcd(q[i], q[j])
    x, y = q[i] // g, q[j] // g
    P.append(g*x*y-x-y+1)
    assert reconstruct_compatible_pair_root(0, 0, q[i], q[j]) == 0
large = sum(P) - sum(H-H//qq for qq in q) + H - 1
assert large == H-a-b-c+2
checks['huge_symbolic_cases'] += 1
assert checks == Counter(formula_cases=3006, pair_crt_checks=9018,
                         exhaustive_masks=2744, random_masks=128,
                         interval_cases=2205, huge_symbolic_cases=1)
print('checks:', dict(sorted(checks.items())))
```

Reproduce without importing any project experiment (from repository root):

```sh
sed -n '/^```python$/,/^```$/p' \
  research_algorithms_20260920/Triangles-Centered-Join-Independent-Audit.md \
  | sed '1d;$d' | /Users/amuldotexe/.local/bin/python3.11 -B -
```

## Execution And Snapshot Receipts

The retained independent checker executed successfully with exit status zero:

```text
modal_counterexample: ((0, 0, 0), (6, 2, 2), (10, 3, 1), 1, 1) ((1, 0, 0), (9, 2, 2), (0, 3, 1), 0, 1)
weighted_cancellation: ((0, 5, 0), (1, 1, 1), (1, 1, 1), -70, 0)
half_mask: (15, 21, 35) 105 (24, 25, 26)
half_mask: (35, 55, 77) 385 (93, 94, 96)
half_mask: (143, 187, 221) 2431 (603, 604, 606)
checks: {'exhaustive_masks': 2744, 'formula_cases': 3006, 'huge_symbolic_cases': 1, 'interval_cases': 2205, 'pair_crt_checks': 9018, 'random_masks': 128}
```

The 3,006 formula cases include 2,744 exhaustive triples, two center choices on
each of 128 seeded fixtures, two versions of the modal counterexample, the weighted
cancellation case, and three half-mask cases. The 9,018 pair checks are three
orientations of those same cases, not independent datasets. The interval check
uses an independently written tight-block iterator; the audited lead's slightly
looser boundary-block schedule is justified in the source analysis above. No
finite checker establishes arbitrary-size correctness or machine-resource bounds
by itself. The separate mathematical review's 7,985 checks were consulted as
prior evidence, not silently added to this audit's counts or rerun here.

The separately executed lead command was:

```sh
cd /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments
/Users/amuldotexe/.local/bin/python3.11 -B -m unittest test_centered_residue_triangles
```

It passed **13 tests**, including 27,000 exhaustive small-mask cases, 600 seeded
mask cases, interval enumeration checks, huge complements, half-mask refusal,
the binary-join obstruction, 80 expanded-graph fixtures, and two-motif cumulative
budget checks. This command runs lead tests with project imports; it is distinct
from the self-contained checker above. The initial user checkpoint had nine
tests, then eleven with the wrapper; two further lead-owned tests were present
in the executed snapshot. No timings are reported or used as performance evidence.

Final inspected snapshot hashes, verified after the execution above:

| Artifact relative to this note | SHA-256 |
| --- | --- |
| `experiments/probe_centered_residue_triangles.py` | `5081a5f5372086b89a5546307b7caabf8bd08fa9d66209f2f80c6b70d7c4d8b2` |
| `experiments/test_centered_residue_triangles.py` | `a48c54d3a540d89e9748a4dc4fa90ffb5f7af7cf4b5ce5e737b790414cc9dbc0` |
| `experiments/probe_shared_residue_triangles.py` | `0700a97cf86ac00c7cd64facf5824fb93df13f5fb9c75782b5f9d942384c1baf` |
| `experiments/probe_masked_matching_triangles.py` | `b2640cb46a9e04a8ac7700c645c0e16de0108fb8e414cd7c2a333c0bcbef65a0` |
| `Triangles-Shared-Residue-Review.md` | `37a003ef08c391a4fe63bad03e62c2d3aa5f917a8d083781bab31b39ab2655c6` |
| `Novelty-Baseline-Evidence-Policy.md` | `db38de5c8e93bf2ee398242a941fd9a4bca7af046a013001cec2cbd9466d7016` |
| `Triangles-Centered-Residue-Review.md` | `61bdd47a21cbe5480813d4cabe590ff1495d2b3b77f57fc74fd0c86e3396fa89` |
| `Triangles-Centered-Residue-Join.md` | `ef4b1bb63b267cdc218a2e4aaf2bb5e63cbdbdc3e71c6e43ebcc3740f1e7f238` |

The lead added the wrapper and adversarial tests during this audit, so these
are the final inspected versions, not assertions that the shared repository was
static throughout. Only this audit Markdown was written by this task. Recompute
the two main hashes from the repository root with:

```sh
env LC_ALL=C LANG=C shasum -a 256 \
  research_algorithms_20260920/experiments/probe_centered_residue_triangles.py \
  research_algorithms_20260920/experiments/test_centered_residue_triangles.py
```

**Disposition:** accept the exact theorem, scalar implementation and scoped wrapper
admission. Retain the new distinct-minimal-period centering counterexample and
all existing negative/comparator receipts. No requested code repair; no novelty,
physical-resource, or complete-workflow performance promotion.
