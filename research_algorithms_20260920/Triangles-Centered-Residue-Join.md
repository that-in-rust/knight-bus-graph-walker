# Centered Residue Joins With Exact Candidate Admission

Date: 2026-09-21. A07 research follow-up. Implemented exact scalar kernel and
native-graph counting path; complete local output reuses the existing executor.
This is a candidate composition, not an established new CRT, sparse join, or
publication-ready graph algorithm. No physical RAM or public latency measurement.

## 1. The Specific Problem

The [shared-residue executor](Triangles-Shared-Residue-Composition.md) avoids
expanding all vertices and edges of a supplied periodic graph. It nevertheless
can revisit an enormous number of repeated histogram boundaries. Three short
interval descriptions can cause time exponential in their numeric bit width.

The [elementary control](Triangles-Elementary-Counting-Control.md) removes
equal-period, two-mask and two-singleton cases. The remaining question is whether
three distinct nonsingleton masks admit a useful different execution parameter.

This procedure uses the number of **compatible residual pairs** instead of the
number of repeated boundaries. Both parameters can be bad. Crucially, the new
pair count is computed exactly from compact histograms before any pair is
enumerated. The user-facing use is another explainable plan, not a faster plan
on every input or a promise that all jobs fit a deadline.

## 2. Contract And Notation

Exactly three positive integer periods q_i divide L. A Boolean set U_i inside
[0,q_i) is represented by disjoint integer half-open intervals. Overlapping runs
are rejected; adjacent intervals can be coalesced. All masks and limits are
validated before scalar shortcuts. Booleans are not accepted as integer fields.
Input shifts must already refer all three masks to the same root coordinate.

The scalar answer is the number of x in [0,L) satisfying all three masks.
Repeated and nonminimal periods, full masks, empty masks and private prime
factors are allowed. This is not a weighted graph-input API: histogram values
and signed deviations can exceed one even though each original mask is Boolean.

```text
g_ij = gcd(q_i,q_j)       h_i = lcm(g_ij,g_ik)
H    = lcm(g_12,g_13,g_23)
Q    = lcm(q_1,q_2,q_3)
A_i(s) = number of u in U_i with u mod h_i = s

C(L) = (L/Q) * sum_(t < H) A_1(t mod h_1) A_2(t mod h_2) A_3(t mod h_3)
```

The last identity is supported by the prior independent shared-residue proof.
The new [independent mathematical review](Triangles-Centered-Residue-Review.md)
proves the centered procedure without reading its implementation.

## 3. Representation And Exact Algorithm

Compute each A_i as a coalesced weighted interval list. A native interval gives
floor(length/h_i) to every residue, plus at most one circular remainder arc.
Use its endpoints rather than an h_i-element array.

Choose c_i as the value occupying the most residue positions of A_i, counting
interval lengths, not intervals. Ties choose the smaller value. Store the
nonzero weighted intervals of D_i=A_i-c_i. The modal choice minimizes each
individual support size. It does not minimize the eventual join cost.

For first and second moments compute:

```text
S_i  = (H/h_i) * |U_i|
P_ij = dot(original U_i histogram modulo g_ij,
           original U_j histogram modulo g_ij)

S = c_3 P_12 + c_2 P_13 + c_1 P_23
    - c_2 c_3 S_1 - c_1 c_3 S_2 - c_1 c_2 S_3
    + c_1 c_2 c_3 H + T
T = sum_(t < H) D_1(t mod h_1) D_2(t mod h_2) D_3(t mod h_3)
```

All these are exact integer operations, not approximations. First/second terms
are obtained by histogram sweeps; only T requires the residual join.

Let E_i be the support of D_i. Ignore weights solely when constructing the
support histogram. Adjacent different-weight runs must remain distinct for
the later weighted sum. Compute, for every pair:

```text
K_ij = dot(E_i histogram modulo g_ij, E_j histogram modulo g_ij)
     = number of (u,v) in E_i x E_j with u = v mod g_ij
```

Choose the pair by increasing `(K_ij, R_i' R_j', i, j)`, where R_i' is the
number of nonzero constant-weight deviation runs. Check the supplied pair and
run-pair caps before correction enumeration. For K=0, T=0 and no run pairs need
visiting. This does not imply S=0: retain all the other terms.

For each chosen weighted interval pair, enumerate only compatible (u,v). For
an interval of width at least g, every first-coordinate u has a partner. For
a narrower interval, reduce it to at most two arcs modulo g and jump directly
between the corresponding repeated first-coordinate intervals. Each interior
block emits a point; at most two clipped boundary blocks per arc are empty in
this implementation. It never scans all residue classes across an empty gap.

The generalized CRT yields the unique t modulo H from u modulo h_i and v
modulo h_j. An inverse is precomputed once, with a separate modulus-one branch.
Binary-search the third deviation's weighted run list, and accumulate
`D_i(u)*D_j(v)*D_k(t mod h_k)`. Pairs are streamed, not retained. Finally return
`(L/Q)*S` and the logical work counters.

## 4. Why It Is Correct

The pivotal three-mask property is `lcm(h_i,h_j)=H` for every pair, alongside
`gcd(h_i,h_j)=g_ij`. A compatible reduced pair therefore determines exactly
one shared root; no unseen multiplicity remains for a third-key join.

The gcd-histogram dot product counts compatible pairs exactly. Disjoint weighted
runs give each such pair exactly one owner. CRT maps those pairs bijectively to
the shared roots where the first two deviations are nonzero. The third lookup
supplies precisely the remaining weight, including zero misses and negative
contributions. Thus the streamed correction equals T. Expanding the product
of the three D_i and rearranging gives the displayed moment identity.

Two tempting shortcuts are false. Treating D_i as unsigned Boolean support
loses signs and multiplicities. Multiplying P_ij by Q/H or treating it as an
ordinary two-mask root count introduces an erroneous scale. The first review's
independent signed fixture includes weights -3 and +4 and checks these cases.

The pair-lcm identity is specific to three masks. For four periods (2,3,5,5),
the analogous reduced periods are (1,1,5,5), so the first pair determines only
one residue modulo 1, not a unique residue modulo H=5. No many-mask extension
is claimed by this implementation.

## 5. Work, State And Admission Scope

Let R be total native input runs, R_i' the weighted deviation-run counts, and
K the chosen pair count. With deterministic sorted/ordered-map construction:

```text
Preparation: O(1 + R log(R+1)) arithmetic/comparison operations
Correction:  O(R_i' R_j' + K * (1 + log(R_k'+1)))
Live state:  O(R+1) integer records, not O(K), O(H), or O(Q)
```

This Python probe reuses the prior hash-map event histogram constructor and
uses a dictionary for modal populations. Its preprocessing hash operations do
not provide an adversarial worst-case comparison bound; a deterministic packed
implementation must replace these with sorted events/values or an ordered map.
The exact candidate reservation and streamed enumeration do not depend on
hash-table performance. No deterministic wall-clock inference is made.

Arithmetic has bit costs. Histogram magnitudes are at most q_i/h_i; selected
K<=H<=Q. Signed terms and sequential accumulators can be bounded by a constant
multiple of Q, but CRT intermediate products, inverses, division and comparisons
still cost more at larger widths. Python records, allocator metadata, temporary
sort storage, source indexes and runtime memory are not encoded by O(R).

`pair_budget` caps the exact number of candidate pairs; `run_pair_budget` caps
the selected nested weighted-run visits. They do not cap preprocessing, source
validation, motif discovery, integer bit operations, output work or seconds.
Refusal happens after paid histogram preparation but before correction pairs.
The implementation selects one fixed modal plan before testing these caps;
refusal is not proof that no alternative centers or pair plan would fit.

The plan minimizes K among three fixed modal choices, not the entire cost
expression. More sophisticated selection needs to charge its own work and
demonstrate value; adding names to heuristic choices does not establish novelty.

## 6. Native Graph Integration And Full Output

`compute_centered_period_triangles` consumes the same source as the old native
executor: base-vertex pairs, matching shifts, native periods and Boolean masks.
It enumerates closing class triples without retaining their list, aligns the
second mask by the first shift, and adds the centered scalar count once per
oriented triple. Source preparation and unsuccessful closure probes are paid.

Pair/run-pair budgets are cumulative across motifs, but admitted one motif at
a time. A later refusal can follow earlier completed work. There is no partial
successful graph result, nor a claim of global admission before any graph work.

The returned source retains the exact total. The unchanged
`iterate_local_triangle_rows` streams every degree and local triangle count in
bounded row blocks. It still revisits source classes and terms and emits b*L
rows. Faster scalar aggregation neither removes that output lower bound nor
creates an H-periodic local-count answer. This distinction is tested against
expanded simple graphs, including nonzero shifts and mixed native periods.

Preparation from arbitrary CSV/Neo4j edges, discovery of useful native symmetry,
binary file layout, refresh, physical memory enforcement and comparative public
workload timings are not implemented by this follow-up. Those remain necessary
for the full user workflow and a storage-system paper.

## 7. Executed Positive And Negative Evidence

| Fixture or test | Observed result | What it does not establish |
| --- | --- | --- |
| All Boolean masks for all ordered q_i in 1..4 | 27,000 scalar answers match direct root enumeration | General performance or priority |
| 600 seeded masks with q_i in 1..18 and repetition multipliers | All match; both signs and private-factor histograms exercised | Population prevalence |
| 10,368 interval-pair cases | Exact sets and no duplicate pair emissions | Physical RAM |
| 80 new native graph fixtures | All full degree/local-count rows and total match expanded oracle | Large-file ingestion or latency |
| q=(35,55,77), U_i=[1,q_i) | Answer 364; one residual pair versus 43 noninitial old stream events | Separation from ordinary inclusion-exclusion |
| a=2^80, b=a+1, c=2a+1, q=(ab,ac,bc), same complement masks | Exact Q-a-b-c+2 with one pair/run pair | Processing or exporting that many physical vertices |
| Same huge periods, half-period masks | Preflight K>=H/9; cap one refuses before pair enumeration | A polynomial-bit counting algorithm |
| Three sparse two-key star relations, n=8 | Zero triangles after 72 candidate pairs for every orientation | Worst-case-optimal multiway joining |
| Two closing graph motifs | Two pairs allowed; budget one refuses on later motif | Global pre-work admission |
| Empty/constant masks and invalid late input | Correct baseline and validation before shortcuts | Serialized untrusted-source boundary hardening |

Nine missing-module assertions were observed before scalar implementation; nine
tests then passed. Two missing-graph-executor assertions were observed before
integration. The two independently supplied adversaries were then retained as
tests. The resulting focused suite passes **13 tests**. No public timings were
taken. Test-run elapsed seconds are not candidate-versus-baseline measurements.
The combined five-module triangle regression suite passes **64 tests** in the
lead replay, including all earlier shared-residue, elementary, periodic and
masked/hybrid checks.

The independent mathematical review's self-contained checker was replayed by
the lead: 7,776 interval-pair cases, 200 seeded masks and nine named cases, all
passing. It checks all three pair orientations using no repository imports.
That review does not constitute code audit of the lead implementation.

A [separate implementation audit](Triangles-Centered-Join-Independent-Audit.md)
is now complete for the recorded code/test hashes. It inspected the scalar
kernel, imported helpers and graph wrapper and found no implementation defect
within their stated contracts. Its independent checker was replayed by the
lead: 3,006 formula cases with 9,018 corresponding pair-orientation checks,
2,205 interval cases and one huge symbolic calculation passed. These are
overlapping check categories, not a sum of independent datasets. The audit
also ran the 13 lead tests separately.

Its stronger centering counterexample uses distinct minimal periods
`(15,21,35)` and masks `{0,1,3,6,9,12}`, `{0,3}`, `{0,1}`. Unique modal centers
`(0,0,0)` produce pair counts `(10,3,1)`. Changing only the first center to one
increases that support from six to nine residues but changes the counts to
`(0,3,1)`. Both choices give the exact answer one. Thus even unambiguous local
modes can lose a zero-pair plan. The implementation deliberately keeps its
specified modal policy; this is a retained heuristic limitation, not a hidden
counting failure or a silently implemented global optimizer.

This paragraph records completed review after its manuscript snapshot; it does
not alter the code or test hashes below.

## 8. Rubber-Duck Corrections And Closest Art

1. **Common values are not a new invention.** AWARE uses most-frequent defaults
   and frame-of-reference residuals, including algebraic correction for matrix
   multiplication. The residue-specific compiler, not default subtraction,
   would have to carry our contribution. [AWARE, Section 3.1 and Section 4.2,
   2023](https://mboehm7.github.io/resources/sigmod2023a.pdf).
2. **Signed product expansion is established.** DBToaster's generalized
   multiset model and delta join rule already make signed sum/product
   computation explicit. Static centering uses the same algebra without being
   a temporal update. [DBToaster, Sections 3.1-3.2,
   2012](https://arxiv.org/pdf/1207.0137).
3. **The huge complement result is not a strong novelty separator.** Ordinary
   inclusion-exclusion counts the three excluded zero-residue classes directly.
   It obtains exactly the same formula without the centered implementation.
4. **The new parameter can still explode.** Three half-period intervals have
   K=Theta(H). Exact refusal is useful but is not a solution to counting them.
5. **A binary join can lose even with no result.** The independent review maps
   the residue correction to a weighted triangle join and embeds the published
   pair-join obstruction. Expanded worst-case-optimal joins are a strong
   comparator; they pay support expansion absent from this compact plan.
   [Ngo et al., Example 2.2 and Section 4,
   2012](https://arxiv.org/pdf/1203.1952).
6. **A theoretical general control remains.** Per-run-triple fixed-dimensional
   lattice counting can avoid period-sized enumeration. That previously
   inspected reduction is unimplemented here; it cannot be dismissed because
   our Python kernel has fewer lines. See the shared-residue review and
   [Barvinok-Pommersheim, Theorem 4.4 and Algorithm 5.2,
   1999](https://library.slmath.org/books/Book38/files/barvinok.pdf).

The full mathematical review also positions Gibson's gcd-histogram counting,
Kim/Glass's three-period reduction and functional aggregate query methods, with
specific inspected passages and explicit access gaps. The lead newly inspected
AWARE's default/morphing passages and DBToaster's signed data model; the latter
publisher PDF timed out, so its arXiv copy is the successful source. Other
passage-level attributions are inherited explicitly from the linked reviews.

## 9. Reproduction And Next Scientific Gate

```sh
cd research_algorithms_20260920/experiments
/Users/amuldotexe/.local/bin/python3.11 -B -m unittest test_centered_residue_triangles
```

The implementation is [probe_centered_residue_triangles.py](experiments/probe_centered_residue_triangles.py),
with [retained tests](experiments/test_centered_residue_triangles.py). Its current
SHA-256 is `5081a5f5372086b89a5546307b7caabf8bd08fa9d66209f2f80c6b70d7c4d8b2`;
the test SHA-256 is `a48c54d3a540d89e9748a4dc4fa90ffb5f7af7cf4b5ce5e737b790414cc9dbc0`.
Hashes identify this tested version, not a signed release or provenance proof.

The next gate is a representation-aware comparison, not more tiny-mask counts:
find a useful native input family or real producer where compact residual
aggregation beats the strongest elementary and equally informed join controls,
including preparation and requested output. A theoretical route is a genuinely
stronger compressed multiway-oracle bound. A systems route needs a useful source
and measured whole-workflow benefit. Neither is satisfied by this document.

All seven-family contribution gates remain as recorded in the portfolio audit.
