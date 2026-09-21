# Factorized Triangles: Template-Defect Local Count Stencils

Date: 2026-09-20. Research candidate with exact small checks; no production implementation, measured 4 GB completion, or worldwide novelty claim.

## Premise Check

**Exact triangle computation on compressed graphs is established.** So are neighborhood-diversity counting, lossless summary graphs with signed edge corrections, cubic trace identities and higher-order delta processing. The proposed candidate is narrower: evaluate a complete **per-vertex** triangle/LCC answer using a type-wide bulk correction plus endpoint exceptions, with mixed defect terms computed from neighbor-type moments rather than expanded template edges or defect wedges.

The useful regime is an authoritative graph that has a small certified block template and relatively few edge defects, even when those defects destroy exact twin equivalence. The representation is an exact graph, not a stochastic block model or a low-rank approximation.

Repository baseline: [new portfolio](README.md), [D06/D15 decision map](../research_4gb_20260919/Architecture-Decision-Map.md), [decision brief](../research_4gb_20260919/Final-Research-Decision-Brief.md), [PRD04 storage analysis, section 6.6](../docs_PRD04/Algorithm-Storage-Decision-Analysis.md), and [PRD04 innovation atlas, A8](../docs_PRD04/A007-Custom-OLAP-Storage-Innovation-Atlas.md). The ordinary competent path is degree-oriented intersections with bounded counters/spill and hybrid compressed rows. The new work cannot be merely D06's avoided clique expansion or another materialized count column.

Expert lenses: graph counting, algebraic query maintenance, compressed representations, external-memory lifecycle, and an adversarial semantics check. In particular, a scalar trace formula does not automatically yield correct local counts.

## Candidate Approaches

| Candidate | Advantage | Disposition |
| --- | --- | --- |
| Exact type-graph triangle counting | Counts multiplicities without expanding twin classes | Required strong baseline, already in primary literature |
| Repeated compressed adjacency-matrix products | Reuses partial rows/bicliques | Established; does not itself eliminate the number of vector probes needed for an exact trace/diagonal |
| Enumerate only triangles touching defects | Reuses template counts | Useful baseline but one defect may touch a huge implicit neighborhood; naive defect wedges can be quadratic in hub degree |
| **Template Defect Diagonal Stencils (TDDS)** | Shared type-wide correction plus defect-endpoint exceptions; type moments replace mixed-term wedges | Main candidate; exact identities below, conditional work advantage |

After independent review, this is a **derived factorized-counting specialization**, not an established new counting algorithm. The explicit factorized-view comparator below obtains the same support and arithmetic bounds. The revision supplies a bounded keyed execution schedule and finite capacity tests; neither the polynomial expansion nor summary-plus-corrections nor aggregate pushdown is an invention claim. CSCJ's matched-cache moving-run separation is the stronger research lead in this pair.

The separate [contraction-frontier investigation](Triangles-Structure-Frontier.md) also retains a negative result: a weighted local-adjoint procedure is exact, but fused differentiation matches its storage and a split-certificate baseline beats its proposed family. Do not promote that follow-on as a replacement innovation. It documents which mechanism failed and why, while the search continues into nonuniform symbolic representations.

## Graph And Output Semantics

Take one consistent source snapshot. Apply explicitly selected node/edge filters, symmetrize selected relationships by unordered endpoint pair, discard source self-loops, and deduplicate parallel/reciprocal pairs. Retain isolates. The resulting adjacency `A` is symmetric Boolean with zero diagonal. Source weights are ignored in this profile; weighted triangles, directed motifs and multiplicity-weighted triangles are different operators.

If the source is memberships, the graph means **at least one** shared selected feature per pair, not the number of shared features. A raw incidence Gram matrix generally violates this contract. Membership provenance and duplicate elimination remain preparation costs unless the authoritative source directly supplies a validated template/defect representation.

Answers:

```text
t_v = number of unordered triples {v,u,w} with all three edges present
T = sum_v t_v / 3
d_v = sum_u A_vu
LCC_v = 0 if d_v < 2; otherwise t_v / choose(d_v,2)
```

Counts and the LCC rational numerator/denominator are exact integers. Decimal rendering is explicitly rounded and is not the correctness representation. Complete per-node output includes original IDs, including zeros for isolates; triangle **listing** is not promised. A request for all triangle triples has an unavoidable `Theta(T)` output floor and a separate plan.

## Exact Template And Defects

Partition the `n` vertices into `q` nonempty types with sizes `n_a`; `c(v)` is a vertex's type. Let `Z` be the one-hot membership matrix, `S=diag(n_a)`, and `W` a symmetric Boolean `q x q` matrix. Its diagonal `a_a=W_aa` means a clique inside type `a` when one, and an independent set when zero.

Define the loop-free template and signed defects:

```text
K = Z W Z^T - diag(a_{c(v)})
E = A - K
A = K + E
```

`E` is symmetric with zero diagonal. Each unordered nonzero defect has sign `+1` for an added edge in a zero template cell or `-1` for a removed edge in a one template cell. There are `f` such unordered defects. Check every sign against `K`, deduplicate defects, and reject any result outside `{0,1}`. These signs are arithmetic corrections, not social positive/negative edge semantics.

A block template plus arbitrary small defects need not have small neighborhood diversity: one unique missing edge per vertex can break many twins. TDDS retains the original small template types and pays for defects instead of refining every endpoint into a unique type.

## The Global Identity Is Only The Starting Point

For symmetric matrices, cyclicity of trace gives the known algebraic expansion:

```text
6T = tr(K^3) + 3tr(K^2 E) + 3tr(K E^2) + tr(E^3).
```

This avoids neither every expensive multiplication nor every local-output error by itself. For local counts, matrix factors cannot be cyclically permuted inside a fixed diagonal entry:

```text
2t_v = (K^3)_vv
       + 2(K^2 E)_vv + (K E K)_vv
       + 2(K E^2)_vv + (E K E)_vv
       + (E^3)_vv.
```

Transposition, not trace cyclicity, justifies each factor of two. There are eight ordered monomials before symmetry (base plus seven delta monomials), six grouped matrix terms above, and seven scalar contributions in the final formula below. These are different counts of the same expansion, not different results.

## Compressed Local Formula

All quantities below are integer-valued until the final division by two. Use `v` for an original vertex and `a=c(v)` for its type.

```text
B = W S W
F = Z^T E Z                         # signed, both orientations counted
h_vb = sum_{u: c(u)=b} E_vu         # signed neighbor-type moment
e_v = sum_u E_vu^2                 # unsigned defect degree
z_v = sum_b h_vb W_ab
```

For this Boolean-template contract, all defects in one type-pair have the same sign, so a nonempty type bucket cannot cancel internally. Specifically, `W_ab=0` permits only `+1` defects and `W_ab=1` only `-1` defects, with the vertex self-pair excluded. Therefore

```text
e_v = sum_b |h_vb|
sum_u a_{c(u)} E_vu^2 = sum_b a_b |h_vb|.
```

These identities make separate unsigned-degree/type-coverage storage optional: derive both corrections from moments, or retain unsigned degree as a validation check. Signed sums can cancel **across** types, so the signed row sum alone is not a defect-degree counter. Retain the actual defect edges for the edge stream and residual triangles. The lead independently identified this stronger sign invariant during review.

The template triangle count, common to every member of type `a`, is:

```text
2 tK_a = (W S W S W)_aa
         - 2 a_a B_aa
         - sum_b n_b a_b W_ab^2
         + 2 a_a.
```

The diagonal corrections remove self-pair walks introduced by `ZWZ^T`. This formula is an integer implementation of established type-graph counting, not its novelty claim. A simple `O(q^3)` evaluation is used here; known algebraic algorithms can improve that bound.

Define:

```text
G_a = (W F W)_aa
L_v = sum_b h_vb [B_ab - W_ab(a_a+a_b)]
J_v = sum_{b,c} h_vb W_bc h_vc - sum_u a_{c(u)} E_vu^2
R_v = sum_u E_vu [sum_b h_ub W_ab] - a_a e_v
tE_v = sum_{u<w} E_vu E_uw E_wv    # signed residual triangle count at v
```

Then the complete answer is

```text
2 t_v = 2 tK_a + G_a + 2L_v - 2a_a z_v + 2R_v + J_v + 2tE_v
d_v   = sum_b n_b W_ab - a_a + sum_b h_vb.
```

Accumulate the doubled numerator and check parity before dividing. The intermediate terms may be negative and much larger than the final count.

### Why This Gives A Small Local Answer State

If `v` is not incident to a defect, then its entire row of `E` is zero. Thus `h_v=0`, `L_v=J_v=R_v=tE_v=z_v=0`, and

```text
2t_v = 2tK_{c(v)} + G_{c(v)}.
```

Non-endpoints can still have changed triangle counts, but they share one changed value per template type. Store one bulk doubled count/degree per type and exceptions only for the `s <= min(n,2f)` defect endpoints. Exact full output is a merge with the original-ID membership tape, not an `n`-counter resident array. This locality statement follows from the formula; it does not claim that only endpoints' answers change.

## Eliminate Mixed-Term Defect Wedges

Let `D_v` be the set of types appearing among actual defect neighbors of `v`. Under the admitted sign contract this is also the nonzero support of `h_v`. Let `r_v=|D_v| <= min(q,e_v)` and `H=sum_v r_v <= 2f`.

For each defect endpoint `u`, compute only the values

```text
p_u,a = sum_b W_ab h_ub, for a in D_u.
```

Write at most `H` keyed records `(u,a,p_u,a)`, including zero payloads. Sort them by `(u,a)` and merge-join to directed defects sorted by `(u,c(v),v,sign)`. Emit `sign*p_u,c(v)` to the destination reduction, then subtract `a_c(v)*e_v`. Each directed defect is consumed once **in this join**, besides its separately charged preparation/sort scans. `J_v` also uses moment pairs, not individual defect-neighbor pairs.

The two-defect mixed terms cost `O(sum_v r_v^2 + f)` arithmetic instead of generating `sum_v choose(e_v,2)` defect wedges. No `E^2` matrix or wedge relation is materialized. A star of a million defect edges into one type has a million-scale edge stream but constant-size moments at its center. If every neighbor has a distinct type, this reduction vanishes.

Only the three-defect term `tE` still needs exact signed triangle counting on the residual support. Orient residual edges by a degree/ID total order, intersect forward lists, and add the product of the three signs once to each of the triangle's vertices. Orienting and intersecting is standard prior art.

## Pseudocode

```text
compute_template_defect_triangles(source, types, template, resource_budget):
    canonicalize source projection and original IDs
    validate A = K + E exactly; admit all build/output/refresh lifetimes
    build type-major ID tape and residual orientation
    external-sort directed defects by (u, type(v), v); retain sign
    grouped scan gives h(u,type), row offset/length index, and optional e checks
    aggregate directed defects into F by their two endpoint types
    compute B, template doubled local counts, and G using bounded type tiles
    for each endpoint v:
        compute L_v, J_v, z_v from type moments and exact template entries
    for each endpoint u, locating its moment range by the row index:
        for each bounded tile of target moments a in D_u:
            initialize at most c accumulators
            scan ONLY u's moment range; accumulate W_ab*h_ub per target a
            append (u,a,p) once per target, even if p=0
    external-sort the H payload records by (u,a)
    merge-join payloads to keyed directed defects, retaining one payload per key
    emit 2f destination contributions, with no full edge-row replay per tile
    retire payloads; externally sort/reduce contributions by destination
    subtract the diagonal correction derived from |h|
    enumerate signed triangles only in the residual support to accumulate tE
    combine all terms into one doubled-count exception per endpoint
    verify parity, nonnegativity, d bounds, and sum(t_v) divisibility by 3
    stream every original ID with exact t_v, degree and LCC numerator/denominator
    publish complete output and snapshot manifest atomically
```

Only **moment ranges** are replayed for target tiles. For a row of `r` moments and tile capacity `c>=1`, target reads plus source-moment reads are `r+r*ceil(r/c)`, and arithmetic is `r^2`. No edge lookup occurs in that loop. The following merge join has one payload and one edge cursor, including arbitrarily skewed `(u,a)` groups; do not buffer the group's `L` edges. The row index has one `(u,offset,length)` record per endpoint and may itself be scanned from disk. Signed residual contributions use a bounded accumulator, not a retained triangle list.

## Correctness Argument

**Template lemma.** With `H0=ZWZ^T` and `D=diag(a_c(v))`, expand `(H0-D)^3`. At vertex `v` of type `a`, the diagonal of `H0^3` is `(WSWSW)_aa`; the two outside-diagonal subtractions contribute `2a_a B_aa`, the middle subtraction contributes `sum_b n_b a_b W_ab^2`, and the remaining diagonal terms net to `2a_a`. This proves the formula for `2tK_a`.

**Mixed-term lemma.** For distinct vertices of types `a,b`, `(K^2)_vu=B_ab-W_ab(a_a+a_b)`. Consequently `(K^2E)_vv=L_v`. Expanding both template factors in `(KEK)_vv` gives `G_a-2a_a z_v`. Directly multiplying the defect row on both sides of `K` gives `(EKE)_vv=J_v`. Swapping the two finite sums in `(KE^2)_vv` gives `R_v`. Finally `(E^3)_vv=2tE_v` because the zero diagonal excludes repeated vertices and the two directions have the same signed product.

**Exact local-count theorem.** Substitute these identities into the ordered matrix expansion. Since the validated `A` is Boolean, symmetric and loop-free, `(A^3)_vv/2` is exactly the number of simple triangles containing `v`. The degree formula follows by summing its row. Therefore the algorithm produces exact `t_v`, `T` and rational `LCC_v` under the declared projection.

**Work-elimination and join lemma.** Grouping defect neighbors by type is a finite sum rearrangement. Every directed defect `(u,v)` has exactly one key `(u,c(v))` in the payload relation because `c(v)` belongs to `D_u`. Tiled summation visits every source moment for each such target exactly once arithmetically. Sorting changes no payload, and the one-to-many keyed join emits exactly one contribution per directed defect without buffering the group. This proves `O(R2+f)` mixed arithmetic, `H` payload records, and `2f` join emissions even for an oversized/skewed row. A zero signed row sum does not justify dropping unsigned corrections.

These are derived proofs in this note, not a mechanized proof. The new research question is whether this factorized local schedule is a materially useful and previously unexploited specialization, not whether the distributive law is novel.

## Counterexamples That Changed The Design

### Scalar Trace Cyclicity Gives Wrong Local Counts

Let the template be the path `1-2-3` and the only defect be the added edge `1-3`. The final graph is a triangle, so every vertex has local count one. If one incorrectly uses `3*diag(K^2E)/2` as the first-order local correction by borrowing trace cyclicity, vertices 1 and 3 receive `3/2`, while vertex 2 receives zero. The full seven-term diagonal expression instead gives `[1,1,1]`. Vertex 2 is not a defect endpoint, yet its answer changes. **Correction:** retain `KEK`, and represent its effect as a type-wide correction, not endpoint-only updates.

### First-Order Edge Corrections Miss Interacting Defects

Take template `K=K_4`. Delete edges `12` and `13`. The true remaining triangles are only `234`, giving global `T=1` and local `[0,1,1,1]`. Subtracting each deleted edge's original two triangles gives `4-2-2=0`: triangle `123` was subtracted twice. The cubic trace terms are

```text
T(K)=4, tr(K^2E)/2=-4, tr(KE^2)/2=1, tr(E^3)/6=0.
```

Delete `23` as well: only the star through vertex 4 remains, with zero triangles. Now the terms are `4-6+3-1=0`; omitting the three-defect term gives one. **Correction:** evaluate all mixed orders and retain signed residual triangles.

### Incidence Counts Are Not Simple Edges

Three vertices sharing two identical features have the simple projection `K_3`, hence `T=1`. Using off-diagonal shared-feature counts gives edge weight two and `tr(A^3)/6=8`. Dividing by a guessed overlap factor does not generalize to heterogeneous overlaps. **Correction:** validate the Boolean template/defect graph after the chosen projection; do not feed a weighted Gram operator into this simple-triangle algorithm.

A certified pair-codegree bound of one makes the off-diagonal incidence Gram Boolean, but TDDS neither assumes nor requires that restricted linear-hypergraph case. Another algebraic route uses `1[s>0]=sum_{j=1..D}(-1)^(j+1)*choose(s,j)` for `s<=D`. Its subset-incidence lift can require `sum_v sum_{j=1..D} choose(feature_degree_v,j)` memberships before any triangle work. That build/state expansion must be charged and can be prohibitive. It is a rejected alternative for this candidate, not an unreported preprocessing trick.

### Oversized Rows Need A Keyed Join

A defect-star center has `r` neighbor types and `L` leaves in each. Then `f=L*r`, `H=r+L*r`, and `R2=r^2+L*r`. Replaying its whole edge row for each target tile costs `L*r*ceil(r/c)` edge reads; for `L=r^2` this is `Theta(r^4/c)` rather than `O(R2)=O(r^3)`. **Actual design correction:** emit the `H` payloads before joining them to the type-keyed directed defect index. At `r=32,L=1024,c=8`, the old center-only replay costs 131,072 reads; the revised join consumes 65,536 directed records for the entire graph. Sort traffic is additional, not free.

### Moments Cannot Replace Residual Topology

Deleting a six-cycle or two disjoint triangles from `K6` gives identical moments: `h_v=-2`, `e_v=2`, `F=-12`, six defects in either case. Yet the final local vectors are respectively six ones and six zeros. Only the residual triangle term distinguishes them. Retain edge identities and signed residual intersections; no `h,F,e`-only answer representation is sufficient.

## Executed Falsification Evidence

Ephemeral JavaScript integer probes executed in the tool runtime on 2026-09-20. At the lead's request, dependency-free executable source was then retained in [community-triangles-Probe-Evidence.md](community-triangles-Probe-Evidence.md) and rerun directly from its JavaScript fence with Node.js v24.9.0. No standalone code or scaffolding was added.

```text
n=5; types=[0,0,1,1,2]; sizes=[2,2,1]
for every symmetric Boolean 3x3 W, including its diagonal: 64 templates
    build K with zero vertex diagonal
    for every simple undirected five-vertex A: 2^10=1024 graphs
        E=A-K, including all positive/negative defect combinations so induced
        compute B,F,h,L,G,R,J,z and signed tE
        evaluate the proposed local formula
        independently enumerate unordered vertex triples in A
        compare each of the five exact local counts
    separately compare template formula against triple enumeration in K
```

| Check | Observed result |
| --- | ---: |
| Template/graph combinations | 65,536 |
| Local-count comparisons | 327,680 |
| Local-count mismatches | 0 |
| Optimized type-batched `p` versus explicit `diag(K E^2)` comparisons | 327,680 |
| Optimized `p` mismatches | 0 |
| Exact degree comparisons | 327,680 |
| Degree mismatches | 0 |
| Type-pair sign/unsigned-degree identity comparisons / mismatches | 327,680 / 0 |
| Template-formula comparisons / mismatches | 320 / 0 |

All arithmetic in this tiny domain is exact within JavaScript's integer range. The local-count oracle inspects unordered triples. The **original** formula probe used direct finite-sum `R`; the retained original suite uses optimized `p` and compares it independently with explicit `K E^2`. Its 327,680 checks do not validate the later capacity procedure.

The revision adds executable bounded-record run generation, two-way merge passes, reverse-order target tiling, payload sorting, the exact keyed join, and a streamed destination reduction. It forces these finite configurations:

| Clique-deletion star | `r=12,L=37` | `r=32,L=1024` |
| --- | ---: | ---: |
| Unordered defects / moments | 444 / 456 | 32,768 / 32,800 |
| `R2` | 588 | 33,792 |
| Target tile / total record-buffer cap | 3 / 7 | 8 / 11 |
| Actual largest buffer | 7 | 11 |
| Moment visits during tiled `p` | 948 | 65,696 |
| Join directed-edge reads | 888 | 65,536 |
| Sort jobs / total merge passes | 3 / 23 | 3 / 38 |

Both return the independent closed-form oracle `R_center=0`, `R_leaf=f-1`; this oracle is `diag(K E^2)` on a clique-minus-star template, not a second evaluation of the tiled schedule. Another 64 six-vertex mixed-sign cases compare the capacity join to direct matrix multiplication, 384 vertex checks. On those same cases a new small sparse procedure computes `G` once per type, `J` over moment support, signed residual counts by degree/ID-oriented forward intersections, and a bulk/exception merge, then compares complete local counts with direct triples. The six-cycle/two-triangle witness is retained and executed separately.

These are **virtual-tape finite-capacity tests**, not physical external-storage benchmarks: backing tapes, source fixtures, template lookups and oracle arrays live in the JS process. The capped record buffers exercise actual branching, run/merge loops and streaming reductions; manifests/byte packing, type-matrix tiling, external residual counter indexes, source normalization, production overflow, disk recovery and a 4 GB physical run remain untested. The retained evidence distinguishes these tiers and includes the source and all recorded counters.

## RAM And Work Model

Use decimal bytes. The target is `4,000,000,000` physical bytes, with a provisional `3,000,000,000` worker ceiling and `1,000,000,000` shared OS/other reserve. Bound actual caches and backing allocations; an mmap limit is not a physical-memory guarantee.

Let `s` be the number of defect endpoints, `H=sum_v r_v`, `R2=sum_v r_v^2`, `delta_E` the maximum forward degree in the selected residual orientation, `T_E` the unsigned number of residual-support triangles, `b` bytes per disk page, and `w` the admitted integer/record width.

For a type-matrix implementation using ordinary cubic multiplication:

```text
CPU_kernel = O(q^3 + R2 + f + f*delta_E + n_output)
H <= 2f
R2 <= min(q, max_v e_v) * 2f
s <= min(n,2f)
```

`L`, `z` and unsigned diagonal corrections require `O(H+f)` after type matrices are ready. `G` is computed on types, not vertices. Degree-order orientation gives an `O(sqrt(f))` forward-degree bound up to constants, so the familiar general residual bound is `O(f^(3/2))`; a paid degeneracy orientation may improve it. Signed cancellation never licenses pruning residual support triangles without another certificate.

This is **kernel arithmetic/comparison work**, not whole CPU. For a comparison-sort implementation let `Csort(N)=O(N log(N+2))`. Add every sort's comparison work, ID/index construction, and the chosen accumulator's `O(T_E log(s+2))` key-search CPU. Thus a conservative whole-job expression is

```text
CPU_whole = CPU_parse_prepare + CPU_kernel
            + sum_{all sort jobs j} Csort(N_j)
            + O(T_E*log(s+2) + index_build_records)
```

For the R path alone, sort record counts are `2f,H,2f`; building `F` from a nonresident type-pair aggregate requires another `2f`-record sort, and residual degree/ID joins and output ordering add theirs. Multiprecision arithmetic/comparisons multiply these counts by the admitted bit-cost; they are not unit-time unconditionally.

For the global count alone, sum the bulk type contributions by multiplicity and the endpoint exceptions, avoiding `n` output work after type sizes and endpoint membership are validated. The full per-node query retains the `Omega(n)` ID/output floor.

### Explicit Bounded Execution Schedule

```text
R_worker <= R_runtime + R_type_tiles + R_row_moments + R_intersection_pages
            + R_external_counter_cache + R_sort_or_merge
            + R_IO + R_output + R_WAL
            <= 3,000,000,000.
```

Dense `B` and `F` cost `2wq^2` bytes if retained; they are never implicitly resident. Choose tile side `ell` with room for three `w*ell^2` tiles and scalar accumulators. Classical blocked type multiplication then uses `O(wq^3/ell+wq^2)` bytes of traffic per constant-number matrix stage; this follows by enumerating block triples and charging their tile reads. It does not claim cache-optimal sparse performance. `W` may be bitpacked or sparse, while arithmetic tiles remain exact integers.

Use the same join for both resident and oversized rows. `Sort(X)` denotes read/write traffic to sort `X` bytes, not scratch size. With payload width `w_p`, directed-key width `w_d`, moment width `w_h`, offset width `w_o`, contribution width `w_r`, and page width `b`, an explicit R-path bound is

```text
IO_R <= Sort(2f*w_d) + Sort(H*w_p) + Sort(2f*w_r)
        + O(b*R2 + w_h*(H + sum_u r_u*ceil(r_u/c))
             + 2f*(w_d+w_r) + H*w_p + s*w_o).
```

The sorts pay for the **keyed directed order**, **payload order**, and **destination order**, respectively. Grouped moment/offset creation needs no separate moment sort after that directed order. Endpoint `J` adds `O(R2)` lookups and similarly bounded moment tiling; `F` needs its own type-pair order or a separately charged bounded index. If `W` is resident, its lookups are memory work; otherwise charge up to `b*R2` cold bytes. Row-range boundary/page seeks add `O(b*(H+s+sum_u ceil(r_u/c)))`, also bounded by the displayed conservative page term on nonempty rows. No full edge row is replayed per target tile.

Schedule sort phases serially. Write `D` for the `2f` keyed edges, `P` for `H` payloads, `Hfile` for moments, `Ofile` for offsets, and `Rfile` for `2f` contributions. A safe additional live reservation during this path is

```text
S_R_live <= |Hfile| + |Ofile|
            + max(2|D|, |D|+2|P|, |D|+|P|+|Rfile|,
                  keep_D*|D|+2|Rfile|) + manifests + WAL
```

The first phase may not yet have moments, so this deliberately over-reserves. Two full run sets suffice for each bounded run/merge sort if input ownership permits retirement after a completed pass. `keep_D=1` if later stages/readers need the keyed edge index; otherwise retire it with `P` after the join. Already-counted shared immutable `D` bytes are counted once in the global live union, not both as retained and scratch. Run catalogs/page indexes, open-file limits, output pages and old readers require their own reservations. `H` may equal `2f`; this is real disk state, not three scalar registers.

Residual forward intersections need at most `O(f*delta_E)` neighbor comparisons. With indexed rows and bounded caches, the pessimistic I/O bound is `O(b*f*delta_E)`. Local signed triangle accumulation into an on-disk endpoint B-tree costs up to `O(b*T_E*log_{b/w}(s+2))` traffic, plus `O(ws)` live counter storage. This deliberately pessimistic plan does not retain `3T_E` output tuples. A faster chunked sort/reduce implementation must cap runs and account for repeated merging with the accumulated endpoint table; it may not silently reserve triangle-sized scratch.

### Worst Cases And Fallback

- A large `q` makes dense type algebra `Theta(q^3)` and its disk matrices unacceptable. A small summary is not enough if its type graph is algebraically expensive.
- Defect neighbors all in different types make `R2` comparable to raw wedge work. Dense residuals make `f*delta_E` expensive even if a compact final answer fits.
- On an arbitrary graph choose `q=1`, `W=0`, `E=A`. Every mixed/template term vanishes, leaving ordinary exact oriented triangle counting. This is a valid bounded fallback, not a speedup.
- Do not build a bad dense template when the zero template plus ordinary adjacency is cheaper. A conservative planner can refuse the fast path before constructing `q^2` matrices.
- Even counting with zero resident entity state cannot make arbitrary exact jobs finish before a fixed deadline. Admission includes disk and an explicit work estimate with uncertainty, not a promised universal latency.

## Preparation, Storage, Output And Refresh

### Full Preparation Is Paid

For expanded input, read the entire selected graph, map IDs, canonicalize unordered pairs and deduplicate through external sorts. For a sort of `X` bytes with `U` bytes of run memory and fan-in `F`, use the planning bound `2X*(1+ceil(log_F(max(1,ceil(X/U)))))` read/write bytes. Count source parsing, repeated ID joins and every index order separately.

Given proposed types and `W`, scan each grouped type-pair's source edges. In a zero template block, present pairs are `+1` defects. In a one block, enumerate gaps in the sorted expected Cartesian order as `-1` defects, excluding the vertex diagonal. Emitting `f` missing pairs is payable; an almost-empty block incorrectly declared complete can require quadratic output and must hit a scratch/storage admission guard. Do not store its whole Cartesian product in RAM or materialize it before detecting budget failure.

A symbolic source can avoid reading a huge expanded graph only if its types/defects are the source's authoritative semantics. General factor discovery is not free: first experiments should take supplied candidate types and certify them. SWeG-like discovery is a comparator with its own measured preparation cost, not an assumed 4 GB service. Reuse only factors valid for the exact filters and snapshot.

### Live-Set Equation

```text
S_retained = S_ID_membership + S_template + S_defect_indexes
             + S_saved_moments + S_answer + S_manifests
             + unique pinned older generations + other selected families
S_live_peak = S_retained + S_local_input + S_unpublished_new_blocks
              + S_sort_runs + S_type_matrix_scratch + S_WAL
```

Enforce `S_retained <= 50,000,000,000`. For the strict proposed experiment, also enforce the same ceiling on `S_live_peak`; otherwise report the separate peak-workspace allowance and do not claim a 50 GB total-disk workflow. Old/new full answer columns are distinct retained bytes, even when template storage is shared.

Example selected payload, not a measured dataset or speed result: `n=100M`, `q=1024`, `f=1M`, `s<=2M`, `H<=2M`. An 8-byte original-ID tape costs 0.8 GB. Budget 24 MB for canonical defects, **64 MB for the `(u,type(v),v,sign)` directed order** at 32 bytes/record, 16 MB for residual orientation, **48 MB for `(u,offset,length)`** at 24 bytes/endpoint, 128 MB for endpoint state and 48 MB for moments. Two 16-byte type matrices cost about 33.55 MB, and bitpacked `W` about 0.13 MB. These named non-output components total approximately **1.262 GB**, excluding metadata/provenance. The keyed order replaces the old directed row order; retaining both instead costs another 48 MB.

For i128 `p` payloads use 32-byte `(u,type,p)` records: up to 64 MB. Use 24-byte `(v,contribution:i128)` records: up to 48 MB. With `D=64 MB`, `P=64 MB`, `Rfile=48 MB`, the conservative R-path live formula is `48+48+max(128,192,176,160)=288 MB`, including its 64 MB shared directed index, moments and offsets. Its **increment above those already-retained 160 MB is 128 MB**, before catalogs, pages, WAL and overlapping generations. A separate `F` sort and type-matrix scratch can reuse reservations only after the prior stage retires; concurrent execution would require their sum.

A packed full row `(original_id:u64, degree:u64, triangle_count:u128, LCC_denominator:u128)` is 48 bytes, or **4.8 GB for 100M nodes**. LCC numerator reuses the count; for degree below two emit numerator zero and denominator one. Named generation plus full output is about **6.062 GB**, two distinct copies about **12.124 GB**, before local input, additional scratch, WAL and other families. These small storage numbers do not price roughly `1024^3` ordinary type operations or the unknown actual `R2`. An expanded trillions-edge source has a different input/time bill. Textual arbitrary-length IDs can substantially enlarge output.

The compact answer itself can retain only type bulk values and endpoint exceptions, with exact full rows generated under backpressure. That saves retention only when the consumer accepts streaming and the generation stays pinned until delivery. It does not reduce the bytes delivered or justify truncating output.

### Refresh And Crash Semantics

For a fixed vertex universe/type assignment/template, process a complete change batch into a canonical new `E'`. Opposite updates cancel only after respecting transaction order and deduplicated simple-edge semantics. Recompute the formulas on `E'` and publish a new version; the conservative cost is the full defect computation using `f'`, not an unproved `O(batch_size)` update claim. The template-only `B,tK` remain reusable.

Changing `F` can change `G_a` for entire types: old local answers outside the update endpoints are not automatically valid. New moments and two-defect terms can depend on neighbors' moments, so a proposed finer incremental algorithm needs complete affected-row closure and higher-order cross terms. This note deliberately does not claim that closure algorithm is implemented or proved.

If defects grow too large, select the zero-template fallback or rebase to a newly certified template. Old/new templates, defects, type matrices and output can coexist during rebase; refuse or defer if their live union exceeds the cap. Vertex insertion/deletion, changed filters and changing type membership require revalidated sizes, membership tape, baseline counts and defects, not just an edge-overlay append.

Bound all sort runs and journals, checkpoint completed reductions, and atomically publish a manifest referencing complete immutable blocks. Recovery either replays a bounded phase or discards an unpublished candidate after no reader can reference it. Signed overflow, failed validation, full disk, interruption and unavailable output sink must yield an explicit non-completion status rather than a truncated count vector.

### Integer Widths

Use signed arithmetic for intermediate cancellations, not unsigned counts. Final `t_v<=choose(n-1,2)` and `T<=choose(n,3)`, but intermediate `q`-matrix/defect sums must also be bounded before selecting i128. LCC denominators need exact multiplication. Admit a concrete maximum `n`, record widths and checked operations, or explicitly charge multiprecision. The small probe is not evidence of production overflow safety.

## Closest-Art Claim Matrix

Primary sources inspected on 2026-09-20. Each row records a mechanism boundary, not a priority verdict over all literature.

| Source and inspected locus | What is already established | Candidate delta or limitation |
| --- | --- | --- |
| [Cordasco, Gargano and Rescigno, section 5.1, Theorem 4](https://drops.dagstuhl.de/storage/00lipics/lipics-vol157-fun2021/LIPIcs.FUN.2021.21/LIPIcs.FUN.2021.21.pdf) | Exact triangle counting through type sizes and type-graph algebra, with `O(nd^omega+n+m)` stated complexity; separate listing cost | This rules out a novelty claim for the zero-defect type formula. TDDS keeps a small near-type template despite arbitrary signed defects and develops all local corrections |
| [Fan et al., Making graphs compact by lossless contraction, sections 3.2.1-3.2.2 and update discussion](https://link.springer.com/article/10.1007/s00778-022-00731-7) | Precount internal/two-in-one-supernode triangles; count other triangles through restored superedges; exact answers without restoring supernodes | Strong exact compressed-triangle baseline. TDDS aims to avoid restoring dense superedges and produce local answers through type moments and endpoint exceptions |
| [Shin et al., SWeG, section 3.1.4, Algorithm 4](https://www.cs.cmu.edu/~kijungs/papers/swegWWW2019.pdf) | Disjoint supernodes, superedges and positive/negative corrections exactly reconstruct a graph in lossless mode | The TDDS input representation is essentially this established model, not a new compression format |
| [Koch et al., DBToaster, delta derivation and algebraic simplification sections](https://www.cs.ox.ac.uk/files/9137/vldbj2014-dbtoaster-extended.pdf) | Higher-order delta processing, self-join cross terms and polynomial simplification | Cubic signed corrections are inherited algebra. Closest unresolved question: whether equivalent compact local stencils already arise in factorized IVM systems |
| [Kara, Nikolic, Olteanu and Zhang, F-IVM, sections 4-6, Example 9 and section 7](https://link.springer.com/article/10.1007/s00778-023-00817-w) | Grouped/free-key views, aggregate pushdown, signed payloads and factorized near-Cartesian relations with corrections | The explicit hand-specialization below matches TDDS's support/work; no asymptotic algorithmic delta established against it. An automatically generated plan was not tested |
| [Karande, Chellapilla and Andersen, 2009, sections 2-3](https://www.internetmathematicsjournal.com/article/1489-speeding-up-algorithms-on-compressed-web-graphs/attachment/4433.pdf) | Algorithms and matrix-vector products run directly on virtual-node compressed graphs | Direct compressed computation and omitted expanded edges are not new; an exact diagonal needs more than citing fast matvec |
| [Francisco et al., 2022, Proposition 1 and biclique/residual section](https://koeppl.github.io/bin/paper/sncs22graph.pdf) | Referenced-row results plus signed differences; shared biclique sums plus residual edges; explicit preprocessing tradeoffs | Reusing repeated row arithmetic is known. TDDS specializes the cubic diagonal and mixed residual terms, not generic matvec |
| [PRD04 A8](../docs_PRD04/A007-Custom-OLAP-Storage-Innovation-Atlas.md) | Degree orientation, hybrid intersections and per-node answer columns | Residual triangle processing uses these mechanisms; no novelty claim for the fallback |

The FUN paper is in the FUN 2021 proceedings but its [publisher record](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.FUN.2021.21) dates publication to 2020-09-16. The compressed-matvec papers were supplied by the lead and inspected as substantive prior art. In Francisco et al., preparation and locality costs visibly prevent equating compression ratio with end-to-end speed; this directly motivates the lifecycle and cold-I/O gates above.

### Matched Factorized Comparator

Following the independent review, use this concrete **manual specialization**, not a claim about a compiler's generated plan:

```text
count2(v) = SUM_{u,w} A(v,u)*A(u,w)*A(w,v)
h(u,b)    = SUM_{v:c(v)=b} E(u,v)
F(a,b)    = SUM_{u:c(u)=a,v:c(v)=b} E(u,v)
D(u,a)    = DISTINCT (u,c(v)) FROM directed E
p(u,a)    = SUM_b h(u,b)*W(a,b), restricted to D(u,a)
R(v)      = SUM_u E(u,v)*p(u,c(v)) - a_c(v)*e_v
```

Substitute `A=K+E`, retain free output key `v`, and use the same type bulk values and endpoint exceptions rather than forcing flat output. This is our construction from the known algebra, not source-attributed pseudocode. Cost each physical relation identically:

| View / physical step | Support or work | TDDS versus matched factorized plan |
| --- | --- | --- |
| Type matrices / bulk values | `O(q^2)` stored, `O(q^3)` ordinary work | Same |
| Grouped `h`, restricted `D` | `H<=2f` entries | Same; `D` is moment support in this contract |
| Marginalized `p` | `H` payloads, `R2` products | Same semijoin restriction and aggregate pushdown |
| Keyed `p`-defect join | `H+2f` cursor work, `2f` emissions | Same sort/merge schedule now specified for both |
| Residual cyclic query | `O(f*delta_E)` comparisons plus signed counter index | Same; no acyclic-query shortcut claimed |
| Saved local vector / full delivery | `O(q+s)` values / `Omega(n)` rows | Same compact-answer contract |

**Comparison result:** no separation in these bounds. The repaired external schedule eliminates a faulty row-replay implementation, not a competent factorized comparator. Its reproducible capacity evidence is useful engineering research, but the present TDDS novelty claim is downgraded. Whether F-IVM's existing optimizer generates this exact bounded plan without manual specialization remains untested and is not grounds to credit TDDS with a mathematical advantage. Clique-with-deletions also has a simple inclusion-exclusion baseline `t_v=choose(n-1,2)-f-(n-2)e_v+choose(e_v,2)+sum_{u in N_E(v)}e_u-tD_v`; the star is a capacity regression, not a novelty exhibit.

## Experiments And Rejection Criteria

1. Extend the exact oracle to weighted *defect coefficients only as algebra tests*, while continuing to reject non-Boolean final graphs for the product profile. Test arbitrary type sizes, all-empty/all-complete templates, isolates, cancellation across type moments, and both signs on residual triangles. Within-type cancellation would require a broader algebraic input than this Boolean-template profile.
2. Scale the now-executed keyed/tiled `p` schedule to real external files. Vary distinct neighbor types and group skew independently, and compare to the matched factorized plan and clique-deletion specialization. Use naive wedges only as a diagnostic negative baseline.
3. Compare against ordinary oriented exact counting, exact neighborhood-diversity counting, Fan-style contraction and competent summary-aware incremental counting, not only an intentionally expanded graph. Include an equivalent F-IVM/DBToaster-style aggregate plan when possible.
4. Sweep template size/density, defect fraction, defect clustering, residual degeneracy and factor-discovery cost. Include random graphs, complete multipartite graphs, cliques with matching/star/clique deletions, and adversarial templates with huge missing-edge complements.
5. Test first full answer, second query and refresh on a physical 4 GB machine. Include ID mapping, compression discovery, peak live disk, retained old readers, complete LCC output, slow clients and rebase fallback. Record cold and warm traffic separately.
6. Pre-register a whole-workflow improvement threshold and a defect/type-size admission envelope. Reject the contribution if building factors dominates useful runs, if the residual is routinely dense, if existing art already implements equivalent stencils, or if output/rebase consumes the alleged saving.

## Final Synthesis And Remaining Gaps

TDDS now has a specified and finite-capacity-tested **keyed payload join**, including oversized moment ranges, sort scratch and streamed reduction. New sparse local/counting probes supplement the original 65,536-case identity check. The explicit matched factorized comparator obtains the same bounds, so this is a derived specialization, not a supported new-algorithm claim. Remaining gaps are real-file/page behavior, physical capacity, factor prevalence/discovery, residual counter economics, exact width admission, refresh/recovery and end-to-end output cost. The external schedule repair is concrete; publication-level differentiation is not established.
