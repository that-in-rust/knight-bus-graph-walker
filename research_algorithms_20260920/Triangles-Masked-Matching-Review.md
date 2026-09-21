# Independent Review: Masked Matching Triangles

Date: 2026-09-20. Bounded independent review; only this file is written. No source edits or commits. The lead's RED/GREEN history and unchanged ten-test suite were not rerun or claimed as independently witnessed.

Lead integration note, after the reviewed snapshot: integer-domain failures below were reproduced in a regression and fixed; the suite then had eleven passing tests. The lead subsequently implemented the separate [hybrid ownership extension](Triangles-Hybrid-Orbit-Ownership.md), with fifteen tests passing. That new mixed-case implementation was not reviewed in this document. Original findings, evidence labels and source hashes below deliberately describe the reviewed snapshot.

## Findings First

**Verdict:** the root-aligned counting proof is correct on the stated integer, Boolean, cross-base domain. I found no valid-profile counting counterexample. I did reproduce a silent input-validation failure. More importantly, there are concrete improvements before C and before Q/J; the latter is not merely another interval-intersection implementation.

1. **[P2, reproduced correctness boundary] Noninteger identifiers, shifts and interval endpoints pass validation.** The probe can silently drop contributions or return edges outside its vertex universe while its closing-event and local/global checks pass. Reject these inputs before constructing masks/events. This does not refute the theorem conditional on validated integer input.
2. **[Algorithm improvement, proved; not implemented] Full-period multi-shift classes admit convolution before Q.** Sum closed shift triples without enumerating them, then emit weighted constant counts on each fibre. Dense three-base instances have Q=Theta(L^2), but this subproblem has a known near-linear-in-L convolution algorithm. The manuscript's single-shift argument against convolution must not be generalized to the richer multi-shift source.
3. **[Algorithm improvement, lead-suggested and independently checked] Enumerate base triangles, then choose the cheapest two shift sets.** This replaces the fixed-direction pair count by the sum of the minimum of three pair products, with the base-triangle enumeration/index cost explicitly charged. It does not eliminate Q or J.
4. **[Verified adversary] H=0 does not prevent cubic J.** A three-base family below has R=Theta(L^2), C=Q=L^2, J=Theta(L^3), and no triangles. A paid root-support semijoin rejects the entire base triangle before C. The manuscript's upper bound remains correct; the family exposes an avoidable plan cost.
5. **[Proof strengthening] Event bounds can use global cut structure.** Degree events are at most 4R+2M; triangle events are at most 6H+4Q_plus, where Q_plus counts closed class triples with nonempty intersection. Both are tight. These improve constants, not the asymptotic O(R+H) claim.

None of these establishes novelty against a strongest same-input comparator. The useful scientific delta is an explicit, falsifiable plan-selection theorem for this storage domain, not priority for joins, convolution, gauge changes or prefix events.

## Scope And Evidence

Reviewed manuscript references below refer to the version with the exact R<=m+f proof, fixed-width caveats, and final executed-evidence section. The two experiment files are under `research_algorithms_20260920/experiments/`, not the repository-root `experiments/`.

Inspected scope:

- `Triangles-Masked-Matching-Runs.md`: source contract, proof, work bounds, separation and receipts.
- `experiments/probe_masked_matching_triangles.py`: validation, compiler, join, events and output.
- `experiments/test_masked_matching_triangles.py`: oracle independence and coverage boundaries.
- `Voltage-Cover-Triangles.md`: source/defect semantics and functional versus numeric output contracts; no challenge to unrelated results is made here.

Evidence labels used below: **proved** means a derivation under explicit assumptions; **executed** means a fresh targeted command completed with assertions; **proposal** means no implementation of the proposed execution plan was supplied. All review probes ran through `python3 -B` with inline code, so no additional experiment files or bytecode were written. Logical counters are not timings, RSS measurements or external-memory measurements.

## 1. Integer Validation Failure

Locations: probe lines 6-18, 47-67 and 93-103. Checks enforce order/range but not integer membership. Events with fractional coordinates/base IDs can be stored and then never visited by the integer output loops at lines 133-142.

**Executed minimal reproductions**, with the module imported as `p`:

```python
p.compute_masked_triangle_counts(2, 3, {(0, 0.5, 0): [(0, 3)]})
# rows: [(1, 0)] * 3 + [(0, 0)] * 3; total: 0; no exception

p.compute_masked_triangle_counts(2, 3, {(0, 1, 0.5): [(0, 3)]})
# rows: [(1, 0)] * 6; total: 0; no exception

p.compute_masked_triangle_counts(2, 3, {(0, 1, 0): [(0.5, 1.5)]})
# rows: [(0, 0)] * 6; total: 0; no exception

p.compile_cover_defect_masks(2, 3, {}, [(0, 0.5, 1, 0.5, 1)])
# {(0, 1, 0.0): [(0.5, 1.5)]}; solving it returns all-zero rows
```

The first result has total degree three: not even the degree sequence of any undirected graph on the declared vertices. The second accepts a displacement outside Z/3Z. The third accepts a mask outside the canonical format and ignores its noninteger events. Fractional base shifts and fractional base IDs are also accepted by the flat compiler and survive into its output.

**Correction:** require integer bases, period, base IDs, coordinates, shifts, interval endpoints and signs at the entry boundary; explicitly choose whether to reject Python booleans rather than treating them as integer IDs. Reject malformed values with a clear validation error before normalization. Add a degree conservation check `sum(degrees) == 2*sum(right-left over class intervals)` as an additional diagnostic, not a substitute for domain validation. The triangle-only checks cannot certify graph validity.

The existing profile tests, test-file lines 159-169, cover signs, overlaps and range failures but not these fractional inputs. This is a new adversarial failure, not a repetition of the existing exhaustive Boolean graphs.

## 2. Aggregate Full Orbits Before Q

Locations: manuscript lines 14, 32, 85-103 and 163; probe lines 115-131.

### Theorem And Proof

**Proved specialization.** Suppose all nonempty classes on one base triangle a<c<z have full masks [0,L). Let their displacement sets be A=D_ac, B=D_cz and D=D_az. Define indicator vectors u,v on [0,L), and their ordinary integer convolution w=u*v of length 2L-1, with absent coefficients zero. Then

```text
k_acz = sum over s in D of (w[s] + w[s+L])
T_acz = L*k_acz
```

Every vertex of each of the three fibres receives exactly k_acz triangles from this base triangle. Thus one can add k_acz to three per-base accumulators instead of emitting one event set per closed class triple. Degree on fibre a is the sum of |D_ac| over its incident base pairs. For a graph consisting entirely of full classes, sum k_acz over base triangles for each base; the answer is constant on every fibre.

**Reason:** a pair (x,y) in A x B closes precisely when `(x+y) mod L` is in D. The two coefficients fold the ordinary sum into its residue. A closing pair supplies one triangle at every root coordinate. At fibre c or z, fixing that vertex's coordinate determines the root uniquely, so the same k_acz credits every local vertex, not merely the global total. Different base triangles and different displacement pairs select different actual triangles. This proves the complete local result without enumerating Q.

This is an ordinary nonnegative convolution: its coefficients are multiplicities of distinct shift pairs, not signed defect corrections. Booleanizing the convolution would lose counts. Each coefficient is at most L; k_acz is at most L^2.

### Paid Work

The cited Bringmann-Fischer-Nakos paper's Theorem 1 gives deterministic sparse nonnegative convolution in O(t polylog(n*Delta)) time in its word-RAM model; section 2 states the model and defines cyclic convolution. Here n=L, Delta<=L, and t is the support of the **ordinary convolution**, at most 2L-1. This use satisfies nonnegativity. It is not a runtime guarantee in the number of matching closing shifts or final answer runs. [Primary paper, Theorem 1 and section 2](https://arxiv.org/pdf/2107.07625).

Charge construction/reads of all three displacement sets, convolution setup and scratch, folding, lookup or sorted intersection with D, and the independently paid base-triangle enumeration. A conservative per-base-triangle work bound is

```text
O(|A| + |B| + |D| + t*polylog(L))
```

in that model, with the trivial L=1 case handled directly. Backend working space must be separately specified and admitted. A dense exact convolution is another backend with length-L storage and the usual FFT-scale arithmetic work, but unqualified floating-point rounding is not an exact-count proof. Sparse logical work is not evidence that a particular backend fits 4 GB. Process base triangles sequentially instead of retaining all convolution products; any cross-triangle cache is additional paid state.

For three dense shift sets, the candidate can have C=Q=H=L^2 and J=4L^2. The convolution specialization gives a genuine pre-Q improvement for the native masked source, even though the degree/local output still costs Theta(L) if all numeric rows are required. With compact answer runs it needs only one weighted answer run per fibre after aggregation.

### Executed Checks

The all-shifts/full-mask three-base family gave:

| L | M=R | C=Q=H | J | Triangle events | Every row (degree, triangles) | T |
| --- | --- | --- | --- | --- | --- | --- |
| 4 | 12 | 16 | 64 | 96 | (8,16) | 64 |
| 8 | 24 | 64 | 256 | 384 | (16,64) | 512 |
| 16 | 48 | 256 | 1024 | 1536 | (32,256) | 4096 |

The completely dense example has additional elementary symmetry, so I also checked irregular displacement supports. At L=31, use

```text
A = {g : g mod 5 != 0}
B = {g : g mod 7 in {0,1,3,4}}
D = {g : g mod 11 in {0,2,3,5,7,9}}
```

The sizes are (24,18,17); the candidate makes 432 pair probes and finds k=236 closing triples, with T=7316. An independently computed exact polynomial product yields the same k and every local count. The three local degrees are respectively 41,42,35. No complete-tripartite formula was used for this check.

In addition, 56 structured full-orbit cases at L in {1,2,3,7,11,17,31} agreed in every degree/local row and total. The independent reference encoded the binary coefficient arrays in base 256^width, multiplied Python integers once, decoded coefficients, and folded modulo L. The digit base exceeded L, so no coefficient carries occurred. This validates the formula, **not** an implementation or benchmark of the cited fast algorithm. A minimal reproducible reference is included below.

### Critical Lifecycle Restriction

If these dense full masks must be built from the original one-shift-per-base-pair cover plus flat defects, adding Theta(L) new full shift classes costs Theta(L^2) explicit defects. The required input scan already costs Omega(L^2). Therefore the example proves a query-phase/native-format improvement, not a subquadratic end-to-end result from that flat source. It becomes an end-to-end discriminator only with a native full-mask input or a paid, amortized retained compilation. The matched comparator receives the same option.

For mixed full/partial classes, using this kernel only on an entirely full connected component is immediately safe. A broader hybrid needs a disjoint ownership rule for all-full versus partial-containing triangles and an enumeration plan that never visits all-full triples again. That hybrid is a **proposal**, not proved here with a complete mixed-case cost bound.

## 3. Minimum Shift-Pair Join Order

**Lead-suggested, independently assessed and executed on a targeted family.** Let m_ac denote the number of nonempty shift classes for one base pair. After enumerating an actual base triangle, choose the minimum of

```text
m_ac*m_cz, m_ac*m_az, m_cz*m_az.
```

The third displacement is uniquely derived by one of

```text
d_az = d_ac + d_cz
d_cz = d_az - d_ac
d_ac = d_az - d_cz                 (all modulo L)
```

Every closing class triple determines exactly one pair in the chosen orientation, proving no omission/duplication. Once the third class is found, retain the manuscript's original a-root intersection and destination-event formulas; choosing a different pair to enumerate does not require changing the root.

Writing W_base for the **actual** work to build/access pair metadata and enumerate base triangles, the corrected query accounting is

```text
C_min = sum over base triangles of
        min(m_ac*m_cz, m_ac*m_az, m_cz*m_az)

O(b + M + R + W_base + C_min + J), plus lookup/sort/I/O costs.
```

No closing base pair means no shift-pair product should be entered. W_base is not automatically linear, and a list of all base triangles must not be assumed resident. Use a streamed enumerator with explicit cost. This is standard join ordering, not a worst-case-optimal or novel join theorem. Q and J are unchanged on surviving triples.

**Executed:** D_01=D_12=Z/LZ and D_02={3 mod L}, all masks full. For L=8,32,128, old C was 64,1024,16384; choosing D_01 x D_02 needs 8,32,128 lookups. Q was respectively 8,32,128, with T=L^2 in every case. Derived closing shifts were checked directly against the candidate's class dictionary.

**No-closing-pair control:** place a star centre at base n, n leaves below it and n above it, one zero-shift class per edge, L=1. The current loop makes C=n^2 despite Q=H=J=0. Executed n=4,16,64 gave C=16,256,4096 with M=8,32,128. A base-triangle plan has nothing to join. This is a plan counterexample, not a contradiction of the honestly declared C term.

The convolution theorem is complementary: when all three shift sets are dense, C_min is still L^2, so reordering alone cannot obtain that aggregation benefit.

## 4. Cubic J With No Triangles

For even L>=4 and bases 0,1,2, include every displacement on all three pairs, with masks

```text
S_01(d) = [0,1)                     for every d
S_02(d) = [2,3)                     for every d
S_12(d) = union [g,g+1), g even     for every d
```

For every class triple, the first two root masks are disjoint, independently of displacement. Hence T=H=0, while

```text
M = 3L
R = 2L + L^2/2
C = Q = L^2
J = L^2*(L/2 + 3).
```

**Executed:** (L,R,C,Q,J,H) was (8,48,64,64,448,0), (16,160,256,256,2816,0), and (32,576,1024,1024,19456,0). All returned local triangle counts were zero. In the present Python implementation, line 125 shifts/materializes the long third mask for every closing triple before discovering the empty intersection; its repeated mask work is real. A genuinely lazy cursor plan may do less than J on this particular family; the logical J counter is an upper-bound charge, not measured cursor advances.

**Proved necessary-condition improvement:** before expanding a base triangle's shift products, form/check the union of all source masks on its two a-incident base pairs. If those unions are disjoint, no class triangle can survive. Here the unions are [0,1) and [2,3), certifying rejection before C. Interval hull disjointness is an even cheaper sufficient rejection on this example. Compilation can retain checked minimum/maximum endpoints per oriented base-pair support while it already reads runs.

For this three-base family, even explicitly building the unions costs only the paid O(R) scan, and degrees still require O(R) processing. For general graphs, charge base-triangle enumeration, every union construction/reuse, overlap check and retained index. There is no global O(R) union-join theorem here. A false positive from a hull/union test must proceed to the exact join. This semijoin is a **proposal** for the source implementation; only its rejection proof and the adversarial candidate costs were executed.

## 5. Stronger Event And Answer Bounds

The manuscript's 10H and 6R bounds are safe. They can be tightened without changing the algorithm.

**Proved:** rotating one disjoint circular mask can split at most one of its linear intervals at the new zero cut. For an edge class with r source runs, the destination emissions therefore contain at most r+1 pieces in total, even when emitted one source interval at a time. Its two fibres generate at most 2r+2(r+1)=4r+2 degree records. Summing gives

```text
E_degree <= 4R + 2M.
```

For one nonempty class-triple intersection with h root runs, each of its two destination rotations introduces at most one additional piece across the **whole** intersection. Therefore

```text
E_triangle <= 6H + 4Q_plus <= 6H + 4Q,
E_triangle <= 10H,
```

where Q_plus is the number of nonempty intersections. The bound excludes empty intersections rather than assigning them four records. It applies to the current per-interval emission scheme; it does not require a new coalescing backend.

**Executed sharpness witness:** L=7, root mask I=[0,2) union [3,5), d_01=3, d_02=6, d_12=3; assign S_01=S_02=I and S_12=I+3. The result has Q_plus=1, H=2, triangle events=16, R=7, M=3, degree events=34. Both improved bounds are attained. A single edge with mask I and displacement 3 also attains degree events=10 for R=2,M=1.

For a 64-byte event layout, an improved conservative initial payload allowance is

```text
64 * (min(10H, 6H+4Q_plus) + 4R+2M) bytes.
```

This still excludes merge generations, indexes and every other resident/disk allocation. The relevant statistics must be obtained or conservatively bounded before admission; knowing H/Q_plus is not a free preprocessing oracle.

Let A_out count maximal linear runs of the complete (degree,triangle-count) answer, including one all-zero run for each isolated fibre. An immediate output theorem is

```text
A_out <= min(bL, b + E_degree + E_triangle).
```

Only coordinates carrying a nonzero reduced event can split a fibre's answer. Cancellation can make A_out much smaller than H, so output-run count is not a lower bound on Q or on the chosen join's work. Full rows still require bL emissions.

## 6. Matched Temporal Comparator

The manuscript correctly rejects novelty based on relabeling a cyclic coordinate as time. Two further distinctions make the comparison more precise.

**Nonzero shifts are not by themselves a discriminator.** Suppose there are base potentials theta_a with `d_ac=theta_c-theta_a mod L` for every nonempty class. Replacing g by `g-theta_a` on fibre a makes every displacement zero and rotates S_ac to S_ac-theta_a. A spanning-forest assignment followed by checking every class certifies this in O(b+M) logical graph work, plus paid graph access and O(R+M) mask rotation/reindexing. Inconsistent cycles or distinct parallel shifts fail the certificate. It requires checking all cycles via potentials, not merely balanced triangles.

This change has a compact b-entry coordinate map, not an N-entry arbitrary ID map. Output can be rotated back per fibre; any ordering/split/reindex cost remains paid. It exposes a whole nonzero-shift subclass to the ordinary temporal snapshot sweep. **Executed:** a four-base example at L=7 with potentials [2,5,1,4] and six nonzero shifts [3,6,3,2,6,3] had T=6; all 28 local rows matched the zero-shift version after the inverse coordinate mapping. This is a metamorphic check of the coordinate transformation, not a second independent graph oracle.

**Do not transfer a temporal runtime theorem without a reduction.** Hu et al. discuss disjoint valid intervals and interval transformations, but the inspected shift-pattern example uses relation-specific fixed intervals. Here S_cz is shifted by d_ac, a value drawn from another tuple, with modular wraparound; naive per-context duplication could restore C/J-sized work. Their reported-result output is also not this aggregate local table. An arithmetic-aware comparator can implement the same join, but no quoted temporal theorem automatically yields its cost on the unchanged encoded input. [Primary paper, sections 2.1-2.3](https://cs.uwaterloo.ca/~xiaohu/papers/sigmod22-temporal.pdf).

The temporal passages were inspected through indexed primary-source text; direct PDF fetches failed. The convolution PDF and its theorem/model passages were directly accessible. No literature-priority conclusion is inferred from these limited checks or from missing search results.

## 7. Proof And Resource Boundary Audit

- **Root proof stands:** cross-base edges force three distinct bases. Canonical ordering selects one root; the actual endpoint coordinates uniquely determine all shifts. Closure plus the aligned Boolean intersection is necessary and sufficient. L=1 and wraparound do not break this argument. There is no independence-of-defects assumption.
- **Compiler bound stands:** for valid flat cross-base defects, surviving old masks use at most m+f_minus runs, added points at most f_plus runs, so R<=m+f and M<=R. The builder's scan and sort are still paid. This is not a run bound for an arbitrary unrelated native-mask source.
- **Domain inclusion needs its qualifier:** the new format contains more shift classes, but the original voltage source also admits same-base additions. Thus it is not a superset of the entire original defect domain. Reject or route the whole unsupported request to the exact fallback; do not discard those edges. The probe rejects them, and does not implement fallback integration.
- **Q/J/H are schedule quantities:** H<=J is sound; with nonempty integer intervals also Q_plus<=H<=T. Large Q may aggregate to constant output, or large Q/J may produce H=0. Neither reduced-event count nor final answer size certifies low query work.
- **Finite code is not the external schedule:** normalized masks/outgoing lists, all defect points and the duplicate set, accumulated events, and bL rows are resident. This is already disclosed. External compilation must price directed/canonical indexes, row offsets, seek/re-read traffic for reused masks and cursors, source/merge generations, and a sorted event reduction. Word-operation bounds must not be presented as block-I/O bounds.
- **Fixed widths:** with N<2^64, the stated final degree/local/global bit allowances are conservative. Keep checked size/product/address arithmetic distinct from graph-count widths. The proposed aggregate events carry k_acz, not just signs of magnitude one; any reused event layout must support that payload.
- **Run separation remains conditional:** native ordering can make R far smaller than f and answer runs far smaller than bL. A capable comparator can exploit the same ordering and aggregation. Charge flat compilation, arbitrary external-ID maps, rebuild/refresh fragmentation and the same requested export. The original repeated-matching example is a point-expansion separation only, as the updated manuscript already says.

## Reproducible Convolution Reference

This is the exact small-reference mechanism used in the targeted formula checks, not a fast-convolution implementation or a requested source change:

```python
def multiply_exact_cyclic_indicators(first, second, period):
    width = max(1, (period.bit_length() + 7) // 8)
    left = int.from_bytes(b"".join(
        int(g in first).to_bytes(width, "little")
        for g in range(period)), "little")
    right = int.from_bytes(b"".join(
        int(g in second).to_bytes(width, "little")
        for g in range(period)), "little")
    raw = (left * right).to_bytes(2 * period * width, "little")
    ordinary = [int.from_bytes(raw[g*width:(g+1)*width], "little")
                for g in range(2 * period)]
    return [ordinary[g] + ordinary[g+period] for g in range(period)]
```

The 56-case check used, for each stated L and q=0,...,7:

```text
A = {g : (g*g + 3*g + q) mod 5 != 0}
B = {g : (3*g*g + g + 2*q) mod 7 < 5}
D = {g : (g*g + 2*g + 3*q) mod 11 < 8}.
```

For each case, construct full masks at those shifts, compare total with `L*sum(convolution[s] for s in D)`, and compare all rows with the three incident-shift degrees and the same summed convolution value. The separate L=31 residue-set example above removes accidental full-set structure from some of these small cases.

## Integration Recommendation

Fix the integer admission failure before describing the finite probe as validating the complete source profile. Add the lead's base-triangle/minimum-pair plan as the straightforward C improvement, and preserve the empty-root family as a falsifier of unfiltered J work. State the full-orbit convolution theorem as a supported aggregate-before-Q option with a narrowly scoped native-source/query-phase separation. It is not evidence of a novel convolution algorithm or an advantage against a comparator allowed the same specialization.

The next discriminating implementation should compare these plans on identical inputs and outputs, including build cost and aggregate-event payloads. A mixed full/partial aggregate theorem and a physically admitted external schedule remain open work, not completed findings.

## Reviewed Snapshot

SHA-256 hashes captured after the lead's reported updates and before writing this review:

```text
69a7f74a66341bece3682df5c6e14e171263163884584a4bf92146d6c9b31074  Triangles-Masked-Matching-Runs.md
b8450070d1ad7bf0331ed1655e4c0cd6db4fb5a82cc2cbb9bf1099f449fb94af  experiments/probe_masked_matching_triangles.py
cea4095a7c66fc9d36ea39be4f5437c9477f529d3104852a615b81a47c2f2830  experiments/test_masked_matching_triangles.py
2932517ff3c9262fd3c32f47b27e7dafcbacb7df014adb229057aed928356d76  Voltage-Cover-Triangles.md
```
