# Voltage Cover Triangles: Sparse Local Corrections

Date: 2026-09-20. Exclusive write scope: this new document. Existing A07, frontier research, evidence and portfolio files are not edited.

## Premise And Result

**Narrow useful result:** an exact local-triangle/LCC answer over a supplied cyclic cover plus `f` signed edge defects can be represented by base-node counts, `f` translated common-neighbor stencils, and numeric corrections only at defect endpoints. Its retained state and counting work have no factor `L`, the cover size, except integer bit widths. It need not store `L*m` edges or `L*b` counters. A stencil is a small formula referring to the base graph, not an expanded list of its affected vertices.

**Important correction to the requested output shape:** base counts plus numeric endpoint exceptions alone are insufficient. An unchanged common neighbor of a changed edge also changes its triangle count. The stencils account for those vertices. Alternatively materialize numeric exceptions at all affected witnesses, charging their potentially larger support. Full numeric output still costs `Omega(L*b)` rows. A functional compressed answer is not the same interface as an already materialized table.

**Claim boundary:** the graph representation is genuinely broader than A07's uniform blocks. A connected, twin-free family below proves a representation-size separation from every A07 block-plus-individual-defect encoding. There is **no established algorithmic advantage over a strongest same-voltage baseline**: ordinary voltage algebra, sparse operator application, and factorized incremental view maintenance reproduce every formula and bound here. The result is an explicit derived operator/storage contract with executable evidence, not a new cycle theorem or IVM theorem.

Expert lenses: graph covers and cycle closure; signed polynomial counting; factorized joins; bounded external state; adversarial same-input comparison. Of three mechanisms considered, full Fourier diagonalization introduces `L` modes unnecessarily, explicit lifted-neighborhood maintenance retains expanded edges unnecessarily, and the selected sparse-stencil plan avoids both. These are plan choices, not invention claims.

## Exact Source And Output Semantics

Let `H=(B,F)` be a simple loop-free undirected base graph, with `b=|B|`, `m=|F|`. For every oriented base edge `a->c` store one shift `s_ac in Z/LZ`, with `s_ca=-s_ac`. Require integer `L>=1`. Multiple labels for a base pair, base self-loops, arbitrary permutation lifts, weighted edges and non-free group actions are outside this first contract.

The vertex universe is exactly `B x {0,...,L-1}`, including isolated fibres. Let `N=bL`, and let the undamaged lift have Boolean adjacency

```text
K[(a,g),(c,h)] = 1 iff {a,c} in F and h-g = s_ac (mod L).
```

Every base edge denotes a perfect matching of `L` actual edges, not a complete bipartite block. A fibre's vertices usually have different, often disjoint, neighborhoods. The lift has `Lm` unordered edges and degree `dK_(a,g)=dH_a`.

The actual snapshot is `A=K+E`. Store `f` distinct unordered pairs of distinct actual vertices with `E_pq in {-1,+1}`. Negative means removal of an existing `K` edge; positive means addition where `K=0`. Check each pair against `K`, reject duplicates/non-Boolean final values and retain both directed incidence orders for execution. Additions between distinct coordinates of the same base node are allowed; actual self-loops are not. Signs are arithmetic corrections, not a signed-network triangle definition. Batch updates are first normalized to a final valid `E`, not summed with multiplicities blindly.

Output for every actual vertex `v`:

```text
t_v = number of unordered simple triangles containing v
d_v = degree in A
LCC_v = t_v / choose(d_v,2) if d_v>=2, else 0/1
T = sum_v t_v / 3.
```

All numerators and denominators are exact integers; reduced fractions are optional. The source's native `(base,g)` IDs or a checked arithmetic ID encoding avoid an `N`-entry identity map. Arbitrary external IDs require their full mapping tape and output join. Filters must preserve this certified representation or produce a newly verified one. Feature projection by OR is a separate operator; neither shared-count adjacency nor an unverified projected cover is admitted.

## Base Bulk And Shift Join

An oriented base triangle `a->c->d->a` produces `L` lifted triangles exactly when

```text
s_ac+s_cd+s_da = 0 (mod L).
```

Otherwise it produces no triangles, although the lifted base cycle may become longer cycles. Enumerate unordered base triangles once, test this equality and increment `tau_a,tau_c,tau_d`. Then `tK_(a,g)=tau_a` and `TK=L*TB`, where `TB=sum_a tau_a/3`. This is ordinary voltage/cycle theory. No `L` Fourier transforms, floating roots of unity or approximate zero tests are required.

For two distinct actual vertices `p=(a,g), q=(c,h)`, including a pair not joined in the base, their common `K` neighbors are exactly

```text
C(p,q) = { (z,g+s_az) : z in NH(a) intersect NH(c),
                            s_az-s_cz = h-g (mod L) }
kappa(p,q) = |C(p,q)| <= min(dH_a,dH_c).
```

Scan the smaller base adjacency row, join/probe the other by base-neighbor ID, and compare shifts. Recover each actual witness coordinate by one modular addition. Do not enumerate all `L` coordinates or prepare all length-two base paths. For the same base node at distinct coordinates this set is empty: the matching constraint gives disjoint neighborhoods.

The relative key `(a,c,h-g)` can share a stencil across translations, but a cache must charge its actual witness records, eviction and build. It is optional and not needed for the bounds below. The selected compact plan keeps the defining pair and sign, not a global pair/codegree matrix or all cached witnesses.

## All Correction Orders

Write `D(v)` for actual defect neighbors of `v`, `e_v=|D(v)|`, and `epsilon_pq=E_pq`. Quantities with endpoint subscripts default to zero off the defect endpoint set `S`, whose size is at most `2f`.

### Linear: Stencil Field And Endpoint Counts

```text
B_v = sum_{unordered defect {p,q}} epsilon_pq * K_vp * K_vq
ell_v = sum_{u in D(v)} epsilon_vu * kappa(v,u).
```

`B_v` is the translated common-neighbor stencil field. One changed edge contributes its sign once to every vertex in `C(p,q)`, and `sign*kappa(p,q)` to both its endpoints through `ell`. A first-order change to local counts is `B_v+ell_v`, not just `ell_v`. `B_v` may be nonzero outside `S`.

### Quadratic: Sparse Row Against Matching Operator

For every directed defect `(u,v)` compute

```text
r_uv = sum_{w in D(u)} epsilon_uw * K_vw
center_u = (1/2) * sum_{v in D(u)} epsilon_uv * r_uv
ends_v = sum_{u in D(v)} epsilon_uv * r_uv.
```

Compute `r_uv` using the cheaper of two exact routes:

1. Scan `D(u)` and test `K(v,w)` through the base shift index.
2. Scan the base row of `base(v)`. Each entry determines one actual neighbor `w=(c,g_v+s_base(v),c)`; probe the keyed defect incidence `(u,w)` and add its sign if present.

The second route scans at most `dH_base(v)` symbolic neighbors, not a full fibre. The first scans `e_u`. For one center, the ordered sum is even: each unordered defect wedge whose outside pair is a `K` edge appears twice. `center_u` credits the wedge's center once; `ends_v` credits each outside endpoint once. Both are needed.

This calculation avoids enumerating all defect wedges when base degree is smaller. It is an indexed sparse join, not a novel matrix-product identity. No `E^2` table, all-pairs common-neighbor cache or per-fibre moment array is retained.

### Cubic: Signed Residual Triangles

Enumerate triangles once in the support graph of `E`. For each `{u,v,w}`, add `epsilon_uv*epsilon_vw*epsilon_wu` to `cubic_u,cubic_v,cubic_w`, and once to global `TE`. Edge signs may differ. In particular, three removals contribute `-1`, not zero. Use smaller-row intersections or a standard degree-oriented kernel on this `f`-edge graph only.

### Complete Answer

```text
endpoint_v = ell_v + center_v + ends_v + cubic_v
t_v = tau_base(v) + B_v + endpoint_v
d_v = dH_base(v) + sum_{u in D(v)} epsilon_vu

T = L*TB + sum_{unordered defects {p,q}} epsilon_pq*kappa(p,q)
         + sum_{u in S} center_u + TE.
```

No division by three is used to manufacture missing local contributions. The global expression is independently checked against the local sum on finite probes.

## Answer Representation And Procedure

**Compact functional answer:** retain base graph/shifts, `L`, `tau`, base degrees, canonical defect stencils, and `(vertex,endpoint_correction,degree_delta)` only on `S`. Size is `O(b+m+f)` records. A point query evaluates `B_v` by scanning `f` stencils and making two base shift checks per stencil, then adds its endpoint correction. This simple decoder costs `O(f)` lookups; it is not a constant-time point-query claim. Its result is a concrete numeric local count/LCC.

**Numeric sparse answer:** stream the `Z=sum_defects kappa(p,q)` witness contributions, sort/reduce by actual vertex and merge with endpoint corrections. Retain nonzero local/degree overrides over the base default. Its support satisfies `X<=min(N,2f+Z)`, `Z<=f*Delta`, where `Delta=max_a dH_a`. It need not be endpoint-only. In the worst case `X=N`; no universal sparse numeric-output theorem is claimed.

**Full numeric table:** decode the numeric sparse answer against a generated `(base,g)` stream, or regenerate witness records during export. Time/output `Theta(N+Z)` plus sorting/joins, fixed cursor RAM, no resident `N`-counter array. All zero-count vertices are included. Triangle listing would instead need `Theta(T)` output and is not this operator.

```text
compute_voltage_local_corrections(base, L, defects, output_mode, caps):
    validate canonical base, shifts, domain IDs and defect signs
    reserve preparation, query, output and old/new-generation lifetimes
    enumerate base triangles once; test zero net shift; accumulate tau, TB
    build sorted directed defect rows and external pair/row indexes
    for each unordered defect p,q with sign:
        shift-join base neighbor rows to count kappa(p,q)
        add sign*kappa to endpoint ell[p],ell[q]
        retain the single stencil p,q,sign
        if numeric output requested, stream sign to each returned witness
    for each endpoint u:
        center_sum = 0
        for each directed defect u,v:
            r = smaller of defect-row scan and shifted base-row probe
            center_sum += epsilon_uv*r
            add epsilon_uv*r to endpoint ends[v]
        assert center_sum even; center[u] = center_sum/2
    enumerate signed residual triangles; update endpoint cubic and global TE
    reduce directed signs for endpoint degree deltas
    combine endpoint fields; compute global T without enumerating fibres
    publish compact functional answer, or reduce/export numeric answer
    verify exact bounds/parity and complete manifest before publication
```

Endpoint accumulators are external keyed records with fixed page caches, or sorted reductions. An oversized defect/base row is cursor-scanned; do not assume it fits. The `r` kernel needs one scalar accumulator; it need not retain its `2f` payloads. Residual triangle contributions update the bounded index directly, avoiding an unpriced `Theta(f^(3/2))` contribution spool. Optional alternative sorts are separately reserved.

## Correctness And Support Theorem

**Theorem (derived specialization).** Under the stated Boolean simple-graph and validated-source contract, this procedure returns exact `t_v,d_v,LCC_v,T`. Compact functional state is `O(b+m+f)` records independent of `L`; numeric witness support is bounded by `2f+Z` but not by `2f` alone.

**Proof.** Expand, for each unordered actual triple, its edge product `(K_uv+E_uv)(K_vw+E_vw)(K_wu+E_wu)`. The all-`K` term is counted by the zero-voltage base rule: a lifted triangle projects to three distinct base vertices because the base has no loops, and choosing one fibre coordinate uniquely determines the other two. Closure is exactly the shift-sum test.

For exactly one `E` factor, its two endpoints receive `ell`, while the third vertex receives `B`. The shift join gives precisely those third vertices with both remaining `K` edges. For exactly two `E` factors, there is a unique shared endpoint of those two edges. The ordered `r` sum counts this center twice, hence the exact factor `1/2`; `ends` credits each other endpoint once. For three `E` factors, signed residual enumeration credits every vertex once. These four disjoint monomial orders prove the local identity even when some signs are negative and several orders cancel. Summing rows proves degrees; summing triangle incidences proves the global formula. The Boolean validation makes these algebraic values simple-graph counts.

Every quadratic/cubic/`ell` term contains a defect incident to its local endpoint, so its support lies in `S`. The only other affected vertices are witnesses of a one-defect term. A stencil is one defect pair/sign plus the common-neighbor predicate referencing the shared base, so it has constant record size even when it affects many vertices. The state/support bounds follow. This theorem does not assert physical resource compliance, a discovery algorithm, or priority for the algebra.

## Non-Twin Separation From A07

For every `L>=5`, take base `K4` with oriented shifts for `a<c`:

```text
s_01=0, s_02=0, s_12=0, s_03=0, s_13=1, s_23=2.
```

Only base triangle `012` has zero net shift. The lift has `N=4L`, `M=6L`, every degree three, local bulk `[1,1,1,0]` by base, and `T=L`. It is **connected**: the base closed walk `0->1->3->0` has net shift one, so its repetitions reach all fibre coordinates, and the base connects all four vertex classes. This is not merely `L` disconnected copies.

It has **no true or false twin pair**. Vertices of the same base at different coordinates have disjoint nonempty neighborhoods. Vertices of different bases that are nonadjacent cannot be twins because one has a neighbor in the other's base class and the other has no same-base neighbor. Adjacent vertices would be true twins only if they shared both their other neighbors; that would put their base edge in two balanced base triangles. No base edge does. The proof is valid for every `L>=5`, not just tested values.

Let an arbitrary A07 representation use `q` Boolean uniform types and `fA` individual edge defects to represent this graph after our `f` edits. Outside the at most `2(f+fA)` endpoints of both defect sets, two vertices in the same A07 type would be twins in the undamaged lift. Therefore

```text
q + 2*fA >= 4L - 2f.
```

Thus for `f=O(1)` A07 must retain `Omega(L)` type/individual-defect records, irrespective of how it partitions the graph; this voltage source plus compact answer uses `O(1)` records of `O(log L)` bits. The probe also uses two actual defects: delete `((0,0),(1,0))`, add `((1,0),(3,0))`. Its triangle total is `L-1`. The removed edge destroys one `012` triangle. The added edge's only common neighbor in the original cover is `(0,0)`, but the required edge to `(1,0)` has just been removed. Global correction orders are `L + 0 - 1 + 0`; the quadratic term cancels the false replacement predicted by independent stale edge corrections.

This is a **representation and pre-export work** separation from A07's admitted structure, not a universal graph-compression lower bound. Full numeric output is `Theta(L)` for both. An ordinary explicit sparse kernel also handles the expanded bounded-degree graph in `Theta(L)` work. A strongest same-voltage plan already has the `O(1)` symbolic representation and the same local correction schedule.

## Strongest Baseline And Novelty Limit

The strongest matched baseline receives exactly the same `H,L,shifts,E`, symbolic ID convention and requested output interface. It may use:

- the zero-voltage base triangle rule;
- sparse phase-indexed base neighbor joins, with or without shared relative-key caches;
- all orders of signed delta expansion, not independent stale edge corrections;
- aggregate pushdown for `r_uv` and indexed endpoint accumulators;
- the same functional stencils instead of materializing numeric witnesses.

That baseline **matches the theorem, support and kernel bounds**. Rephrasing it as a sum of shift-permutation matrices or as a factorized self-join does not weaken it. Existing F-IVM does not automatically establish that a particular implementation recognizes a modular-arithmetic symbolic relation; the claim here is equivalence to a competent voltage-aware specialization, not a benchmark result about an off-the-shelf engine.

A second executable comparator below handles edge events sequentially. Before applying each event, it computes actual common neighbors from a symbolic `K` neighbor row plus sparse defect incidence, using current adjacency `K+E`. It updates numeric local overrides and degrees. This automatically includes interactions between previous events. It uses no expanded lifted-edge store. Its numeric overrides may have `O(2f+Z)` final support; a baseline that keeps the functional stencil form matches `O(f)` correction state too. We do not present an advantage obtained solely by forcing the comparator to use a different output format.

**Remaining contribution question:** is this source-native voltage relation and its bounded local-answer operator worth adding as a storage domain, and can real source metadata certify sufficiently large `L` and small `b,m,f` after filtering/refresh? A measured engine integration and corpus study could be useful even with entirely known mathematics. A new algorithmic claim would additionally need a strict same-voltage separation, for example a proved cheaper evaluation of many overlapping shifted witness requests including cache construction and invalidation. Neither such a theorem nor evidence of production tenant/security applicability is established here. Those applications are hypotheses, not examples asserted to have this topology.

## Counterexamples And Design Changes

1. **Base triangle is not necessarily a lifted triangle.** A `K3` base at `L=5` with `s_01=s_12=0,s_02=1` has no lifted triangles. Counting one per fibre without the shift test is wrong. Orientations matter: reverse labels must be negated.
2. **Non-endpoint answer changes.** Delete edge `((0,0),(1,0))` from the zero-shift `K3` cover. Vertex `(2,0)` loses its triangle although it has no incident defect. Endpoint-only numeric overrides return a false answer. Keep the `B` stencil field or witness exceptions.
3. **Linear updates miss cancellation.** In one zero-shift `K3` fibre, deleting two triangle edges gives per-vertex correction orders `1-2+1=0`. Deleting all three gives `1-3+3-1=0`. A signed cubic term cannot be replaced by unsigned residual counting.
4. **Degree changes need not follow triangle support.** Adding a lone edge to an empty cover changes two degrees and no triangle counts. Do not drop an endpoint record just because its triangle correction is zero; LCC's denominator also changes.
5. **Sparse input is not cheap materialized output.** Base `K_(2,k)` with zero shifts plus one added edge between its two degree-`k` vertices at coordinate zero creates `k` triangles. `k` third vertices change despite only two defect endpoints. A one-record stencil can describe them, but numeric exceptions require `Omega(k)` records. The probe executes `k=12`.

## Kernel, I/O, Widths And Lifecycle

Let `alphaH,alphaE` be arboricities of the base and unsigned defect support; take zero for an edgeless graph. With smaller-row intersections, define

```text
CH = sum_{base edges ac} min(dH_a,dH_c) <= 2*m*alphaH
CE = sum_{defect edges uv} min(e_u,e_v) <= 2*f*alphaE
J1 = sum_{defects pq} min(dH_base(p),dH_base(q)) <= f*Delta
J2 = sum_{directed defects uv} min(e_u,dH_base(v)) <= 2*f*Delta
Z  = sum_{defects pq} kappa(p,q) <= J1
W  = b+m+f+CH+CE+J1+J2.
```

Each small-row intersection probes the larger keyed row. Enumerate an unordered triangle only when its third vertex is larger than both edge endpoints, or use a degree-oriented equivalent; no triangle list is retained. The known arboricity lemma prices these scans. Counting arithmetic is `O(W)`, at worst `O(b+m^(3/2)+f^(3/2)+f*Delta)`. Deterministic balanced indexes add `O(log(b+m+f+2))` per lookup/update; comparison sorts and integer bit operations are separate. There is no promise that high-degree bases or dense defects finish quickly. `J2` also bounds the chosen row replays; skew does not grant unbounded RAM.

For a conservative external B-tree implementation, let `p` be page bytes, `Findex>=2` its effective fanout, `w` fixed record width, and `SortBytes(x)` the **total transferred bytes** for external sorting `x` bytes with the reserved memory. A deliberately pessimistic whole-run bound is

```text
CPU = O(W*log(b+m+f+2)) + normalize/sort CPU + output CPU
IO_compact <= SortBytes(O(w*(b+m+f)))
              + O(p*W*max(1,log_Findex(b+m+f+2)))
IO_numeric += SortBytes(O(w*(Z+f))) + ScanBytes(numeric_output + ID_mapping)
```

No hidden `J2` probe relation is materialized: the index plan emits updates directly. A batched sort-join plan may perform better, but must reserve its `J2` query runs. Producing `3*TE` contribution records would likewise require charging that spool; the selected plan uses bounded indexed additions. Disk random I/O could dominate the kernel even when RAM is bounded. These are bounds for a specified conservative plan, not a measured optimized external algorithm.

**Arithmetic widths.** Require `N<2^64` for a u64 native vertex ID or use a wider product-ID representation explicitly. Modular arithmetic uses sufficiently wide temporaries before reduction. Final local counts and LCC denominators fit u128 under that domain bound; a global count may require more than u128. Signed intermediate sums include cancellation. Reserving signed 256-bit counts covers all local/global degree-three expansions for `N<2^64`; it also prevents a misleading i128 global promise. The JS probe uses BigInt for counts and safe-integer coordinates, a narrower coordinate profile than u64. No floating comparison is part of correctness.

**RAM admission.** Physical cap is 4,000,000,000 bytes, not a worker RSS cap of 4 GB. A provisional worker reservation of 3 GB leaves 1 GB for OS/other processes, subject to measurement. Bound runtime, base/defect index page caches, row cursors, scalar accumulators, sort buffers, WAL and output queues together. Both base and defect tables may be external. Neither `Delta` nor maximum defect degree is a resident-buffer requirement. Full export is a cursor over IDs, not an `N`-element count vector. The finite probe uses resident Maps and oracle matrices and does not verify these physical claims.

**Retained and peak-live disk.** Reserve all local source, prepared state, scratch, output and pinned generations within 50,000,000,000 bytes for the strict experiment. Illustrative payloads before alignment/occupancy: base directed arcs `48m` bytes (two 24-byte records per unordered edge), base bulk `32b`, canonical defects about `24f`, directed keyed defects about `64f`, and endpoint `(id, signed correction, degree delta)` about `64|S|`. Relative stencils can reference canonical defects without another witness copy. Index orders, per-row offsets, wider aggregates, checksums, manifests, allocator fragmentation and WAL add bytes and must be budgeted. Let their measured multiplier be `rho`, not silently one:

```text
Scompact ~= rho*(48m + 32b + 88f + 64|S|) + headers
Slive = unique local input + pinned prepared/answer generations
        + new indexes + sort runs + WAL + unpublished output.
```

Numeric sparse output adds `O(wX)` retained bytes and `O(w(Z+f))` sort input plus merge generations at peak. Full output can encode `(id:u64,degree:u64,t:u128,denominator:u128)` in `48N` bytes, with the zero-degree convention in the manifest. At 100M vertices this alone is 4.8 GB. For the huge-cover probe's `N=4,000,000,028`, full output would be **192,000,001,344 bytes**, so the 50 GB retained-output profile must refuse it even though the compact functional answer is tiny. Streaming to an external consumer avoids retaining that table locally but does not waive bytes/time or authorize claiming a retained table fits.

**Supplied symbolic preparation.** Validate base pair uniqueness, inverse labels, coordinate ranges and each canonical defect against `K`; compute base counts, construct defect indexes and endpoint corrections. State and work above apply only when the symbolic source is authoritative. Payload parsing, external sorts, partial failed builds and selected output mode remain charged. Snapshot metadata identifies base/shift generation, `L`, filter contract, defect generation and ID codec.

**Explicit-graph preparation with supplied mapping.** Reading `Mactual` edges and `N` explicit node IDs costs at least `Omega(Mactual+N)`. Sort by base pair and coordinate, validate shifts on purported nominal edges, and compare each base-edge matching's sorted coordinate coverage with `[0,L)`. Gaps identify removals; out-of-pattern pairs identify additions. Detect a gap exceeding the defect budget before expanding it. This can avoid enumerating absent nominal edges one by one when a gap is represented as a range, but cannot avoid reading the explicit source. Charge `SortBytes(w*(Mactual+N))`, mapping storage and their live overlap with the new state. Without a supplied mapping/base, cover discovery and near-cover discovery are separate unsolved preparation tasks here, not a consequence of the counting theorem. Hash agreement is not an exact structural certificate.

**Refresh with fixed base.** Apply actual edge events to the current Boolean state, canonicalize the new `E` and recompute corrections with the new `f`; this conservative procedure has the displayed total-state work, not `O(batch)` latency. Old base data may be shared, but old/new defect indexes, endpoint records, answer metadata, scratch and output generations coexist. The event comparator can update a numeric answer incrementally from current common neighbors; it is a competent alternative, not defeated by this full recomputation. A base shift edit changes a whole edge orbit and can change `Theta(L)` answer rows. Revalidating all sparse defects and recomputing base bulk is mandatory for a new authoritative base. Re-encoding the *same* actual graph under a changed shift may require `Theta(L)` additional defects; do not treat that as a one-record actual edge update.

**Recovery/backpressure.** Publish only a complete, validated snapshot manifest. Persist the input generation, stage/cursor and accumulator checkpoint consistently; indexed increments need WAL/exactly-once recovery or restart of the uncommitted stage. Slow output pins its source/answer generation. Refuse admission or stop safely before exceeding RAM/disk reservations. No crash/refresh implementation or physical-cap telemetry is supplied by this research probe.

## Primary Prior-Art Inspection

Opened and inspected on 2026-09-20. Primary text, not search absence, determines these boundaries. No worldwide novelty claim follows from this bounded inspection.

| Closest source and inspected locus | Established mechanism | Remaining scope here |
| --- | --- | --- |
| [Liu, Peyerimhoff, Vdovina, Signatures, Lifts, and Eigenvalues of Graphs, 2014 preprint, sections 2-4](https://arxiv.org/pdf/1412.6841) | Inverse oriented signatures, cycle products, balanced signatures, fibre construction and the explicit cyclic shift rule; section 3 traces voltage theory to Gross/Tucker. | The zero-shift triangle rule is an immediate specialization, not new. This note specifies a sparse-defect local-answer contract. |
| [Nikolic and Olteanu, Incremental View Maintenance with Triple Lock Factorization Benefits, sections 4-6](https://arxiv.org/pdf/1703.07484) | Signed insert/delete payloads; all mixed product-delta terms; aggregate pushdown; factorized updates and cyclic triangle-query view-size tradeoffs. | Our correction polynomial and pushed-down `r` join are known mechanisms. An arithmetic voltage relation is a specialized source interface, not proof of a new IVM bound. |
| [Kara et al., Counting Triangles under Updates in Worst-Case Optimal Time, ICDT 2019, sections 1 and 3](https://drops.dagstuhl.de/storage/00lipics/lipics-vol127-icdt2019/LIPIcs.ICDT.2019.4/LIPIcs.ICDT.2019.4.pdf) | Current-neighbor delta joins, auxiliary-view costs, and degree-sensitive update/state tradeoffs on explicit relations. | Do not compare against stale independent edge deltas. The general explicit-relation lower bounds do not transfer unchanged to this restricted symbolic input or different output interface. |
| [Chiba and Nishizeki, Arboricity and Subgraph Listing Algorithms, 1985, Lemma 2 and section 3](https://www.cs.cornell.edu/courses/cs6241/2019sp/readings/Chiba-1985-arboricity.pdf) | Sum-of-minimum-degree bound and linear-state triangle listing with arboricity-sensitive work. | Prices the base/support intersection kernels; residual orientation/intersections are not inventions. |
| Strongest same-voltage specialization, explicitly constructed above | Combines the same cycle rule, signed expansion, modular joins and functional output. | Matches our bounds. Only the domain/contract and eventual engineering evidence remain candidates for contribution. |

A historical Gross/Tucker paper link was attempted, but a repeat fetch timed out; no specific theorem attribution relies on that unseen text. The accessible primary cyclic-lift paper supplies the definition used, and the triangle closure proof is given in full above.

## Executable Falsification Probe

Standalone Node.js, no dependencies or additional files. From the repository root:

```sh
awk '/^```javascript$/{active=1;next} /^```$/{if(active){active=0;exit}} active' research_algorithms_20260920/Voltage-Cover-Triangles.md | node
```

The candidate never allocates a fibre-sized structure. The independent verifier deliberately expands only small covers and enumerates actual triples. It checks each of the four polynomial orders, every local count, degree, exact rational LCC and global total. A separate event comparator uses actual current common neighbors and no expanded lifted-edge store. The large-`L` run uses only point decoding; it is not mislabeled full output. Operation/record counters are not measured RAM or I/O.

```javascript
"use strict";
const assert = require("node:assert/strict");

function add_sparse_signed_value(map, key, value) {
  const next = (map.get(key) || 0n) + value;
  if (next === 0n) map.delete(key); else map.set(key, next);
}

function enumerate_sparse_graph_triangles(rows, emit, stats, field) {
  for (const [u, row] of rows) for (const v of row.keys()) if (u < v) {
    const other = rows.get(v), small = row.size <= other.size ? row : other;
    const large = small === row ? other : row;
    for (const w of small.keys()) {
      stats[field]++;
      if (w > v && large.has(w)) emit(u, v, w);
    }
  }
}

function create_cyclic_cover_source(b, L, edges) {
  assert(Number.isSafeInteger(b) && b >= 0);
  assert(Number.isSafeInteger(L) && L >= 1 && Number.isSafeInteger(b * L));
  const mod = x => ((x % L) + L) % L;
  const rows = new Map(Array.from({ length: b }, (_, a) => [a, new Map()]));
  for (const [a, c, shift] of edges) {
    assert(Number.isSafeInteger(a) && Number.isSafeInteger(c) && Number.isSafeInteger(shift));
    assert(a >= 0 && a < c && c < b && !rows.get(a).has(c));
    rows.get(a).set(c, mod(shift)); rows.get(c).set(a, mod(-shift));
  }
  const base = v => Math.floor(v / L), phase = v => v % L;
  const K = (v, w) => {
    if (v === w) return 0;
    const shift = rows.get(base(v)).get(base(w));
    return shift !== undefined && mod(phase(w) - phase(v)) === shift ? 1 : 0;
  };
  function* neighbors(v) {
    for (const [c, shift] of rows.get(base(v))) yield c * L + mod(phase(v) + shift);
  }
  function* common(p, q, stats) {
    if (rows.get(base(p)).size > rows.get(base(q)).size) [p, q] = [q, p];
    const a = base(p), c = base(q), g = phase(p), h = phase(q);
    for (const [z, az] of rows.get(a)) {
      stats.commonProbes++;
      const cz = rows.get(c).get(z);
      if (cz !== undefined && mod(az - cz) === mod(h - g)) yield z * L + mod(g + az);
    }
  }
  const stats = { baseProbes: 0 }, tau = Array(b).fill(0n); let TB = 0n;
  enumerate_sparse_graph_triangles(rows, (a, c, d) => {
    if (mod(rows.get(a).get(c) + rows.get(c).get(d) + rows.get(d).get(a)) === 0) {
      TB++; tau[a]++; tau[c]++; tau[d]++;
    }
  }, stats, "baseProbes");
  return { b, L, N: b * L, rows, base, phase, mod, K, neighbors, common, tau, TB, stats };
}

function compute_voltage_sparse_answer(C, defects) {
  const E = new Map(), seen = new Set();
  const ell = new Map(), center = new Map(), ends = new Map(), cubic = new Map(), degree = new Map();
  const stats = { commonProbes: 0, mixedProbes: 0, residualProbes: 0, defectRowRoutes: 0, baseRowRoutes: 0 };
  let linearTotal = 0n, quadraticTotal = 0n, cubicTotal = 0n;
  for (const [p, q, sign] of defects) {
    assert(Number.isSafeInteger(p) && Number.isSafeInteger(q) && p >= 0 && p < q && q < C.N);
    assert((sign === 1 || sign === -1) && C.K(p, q) + sign >= 0 && C.K(p, q) + sign <= 1);
    const key = `${p},${q}`; assert(!seen.has(key)); seen.add(key);
    if (!E.has(p)) E.set(p, new Map()); if (!E.has(q)) E.set(q, new Map());
    E.get(p).set(q, BigInt(sign)); E.get(q).set(p, BigInt(sign));
    let kappa = 0n;
    for (const z of C.common(p, q, stats)) { assert(z !== p && z !== q); kappa++; }
    const contribution = BigInt(sign) * kappa; linearTotal += contribution;
    add_sparse_signed_value(ell, p, contribution); add_sparse_signed_value(ell, q, contribution);
    add_sparse_signed_value(degree, p, BigInt(sign)); add_sparse_signed_value(degree, q, BigInt(sign));
  }
  for (const [u, row] of E) {
    let sum = 0n;
    for (const [v, sign] of row) {
      let r = 0n;
      if (row.size <= C.rows.get(C.base(v)).size) {
        stats.defectRowRoutes++;
        for (const [w, otherSign] of row) { stats.mixedProbes++; if (C.K(v, w)) r += otherSign; }
      } else {
        stats.baseRowRoutes++;
        for (const w of C.neighbors(v)) { stats.mixedProbes++; r += row.get(w) || 0n; }
      }
      sum += sign * r; add_sparse_signed_value(ends, v, sign * r);
    }
    assert.equal(sum % 2n, 0n); add_sparse_signed_value(center, u, sum / 2n); quadraticTotal += sum / 2n;
  }
  enumerate_sparse_graph_triangles(E, (u, v, w) => {
    const value = E.get(u).get(v) * E.get(v).get(w) * E.get(w).get(u); cubicTotal += value;
    for (const x of [u, v, w]) add_sparse_signed_value(cubic, x, value);
  }, stats, "residualProbes");
  const endpoint = new Map();
  for (const map of [ell, center, ends, cubic]) for (const [v, value] of map) add_sparse_signed_value(endpoint, v, value);
  const field = v => {
    let value = 0n;
    for (const [p, q, sign] of defects) if (C.K(v, p) && C.K(v, q)) value += BigInt(sign);
    return value;
  };
  const orders = v => [C.tau[C.base(v)], field(v) + (ell.get(v) || 0n),
    (center.get(v) || 0n) + (ends.get(v) || 0n), cubic.get(v) || 0n];
  const query = v => {
    const t = C.tau[C.base(v)] + field(v) + (endpoint.get(v) || 0n);
    const d = BigInt(C.rows.get(C.base(v)).size) + (degree.get(v) || 0n);
    assert(d >= 0n && t >= 0n && t <= d * (d - 1n) / 2n);
    return { t, d, lcc: d < 2n ? [0n, 1n] : [t, d * (d - 1n) / 2n] };
  };
  function numeric() {
    const values = new Map(endpoint), expansion = { commonProbes: 0 }; let witnessRecords = 0;
    for (const [p, q, sign] of defects) for (const z of C.common(p, q, expansion)) {
      add_sparse_signed_value(values, z, BigInt(sign)); witnessRecords++;
    }
    return { values, witnessRecords, support: new Set([...values.keys(), ...degree.keys()]).size };
  }
  return { query, orders, numeric, E, endpoint, degree, stats,
    total: BigInt(C.L) * C.TB + linearTotal + quadraticTotal + cubicTotal,
    totals: [BigInt(C.L) * C.TB, linearTotal, quadraticTotal, cubicTotal],
    records: { baseNodes: C.b, baseArcs: [...C.rows.values()].reduce((s, r) => s + r.size, 0),
      stencils: defects.length, endpointDomain: E.size } };
}

function enumerate_explicit_local_oracle(C, defects) {
  const N = C.N, E = Array.from({ length: N }, () => Array(N).fill(0));
  for (const [p, q, sign] of defects) E[p][q] = E[q][p] = sign;
  // Expand forward-labelled matching edges independently of the candidate's K predicate.
  const K = Array.from({ length: N }, () => Array(N).fill(0));
  for (const [a, row] of C.rows) for (const [c, shift] of row) if (a < c) {
    for (let g = 0; g < C.L; g++) {
      const p = a * C.L + g, q = c * C.L + C.mod(g + shift); K[p][q] = K[q][p] = 1;
    }
  }
  const A = K.map((row, p) => row.map((value, q) => value + E[p][q]));
  assert(A.every(row => row.every(value => value === 0 || value === 1)));
  const counts = Array.from({ length: N }, () => [0n, 0n, 0n, 0n]); let total = 0n;
  for (let p = 0; p < N; p++) for (let q = p + 1; q < N; q++) for (let z = q + 1; z < N; z++) {
    const k = [K[p][q], K[q][z], K[z][p]], e = [E[p][q], E[q][z], E[z][p]];
    const terms = [k[0] * k[1] * k[2], e[0] * k[1] * k[2] + k[0] * e[1] * k[2] + k[0] * k[1] * e[2],
      e[0] * e[1] * k[2] + e[0] * k[1] * e[2] + k[0] * e[1] * e[2], e[0] * e[1] * e[2]].map(BigInt);
    for (const v of [p, q, z]) for (let i = 0; i < 4; i++) counts[v][i] += terms[i];
    total += BigInt(A[p][q] * A[q][z] * A[z][p]);
  }
  const answers = counts.map((row, v) => {
    const t = row.reduce((sum, x) => sum + x, 0n), d = BigInt(A[v].reduce((sum, x) => sum + x, 0));
    return { t, d, lcc: d < 2n ? [0n, 1n] : [t, d * (d - 1n) / 2n] };
  });
  return { counts, answers, total };
}

function verify_complete_snapshot_answer(C, defects) {
  const result = compute_voltage_sparse_answer(C, defects), oracle = enumerate_explicit_local_oracle(C, defects);
  assert.equal(result.total, oracle.total);
  const expanded = result.numeric(); let sum = 0n;
  for (let v = 0; v < C.N; v++) {
    assert.deepEqual(result.orders(v), oracle.counts[v]);
    assert.deepEqual(result.query(v), oracle.answers[v]);
    assert.equal(C.tau[C.base(v)] + (expanded.values.get(v) || 0n), oracle.answers[v].t);
    sum += result.query(v).t;
  }
  assert.equal(sum, 3n * result.total);
  assert(expanded.support <= Math.min(C.N, 2 * defects.length + expanded.witnessRecords));
  return result;
}

function verify_exhaustive_signed_snapshots() {
  const basePairs = [[0, 1], [0, 2], [1, 2]], pairs = [];
  for (let p = 0; p < 6; p++) for (let q = p + 1; q < 6; q++) pairs.push([p, q]);
  const subsets = [[]];
  for (let i = 0; i < pairs.length; i++) {
    subsets.push([i]);
    for (let j = i + 1; j < pairs.length; j++) {
      subsets.push([i, j]);
      for (let k = j + 1; k < pairs.length; k++) subsets.push([i, j, k]);
    }
  }
  let cases = 0, localChecks = 0, baseRoutes = 0, defectRoutes = 0;
  for (let encoding = 0; encoding < 27; encoding++) {
    let code = encoding; const edges = [];
    for (const [a, c] of basePairs) { const digit = code % 3; code = Math.floor(code / 3); if (digit) edges.push([a, c, digit - 1]); }
    const C = create_cyclic_cover_source(3, 2, edges);
    for (const subset of subsets) {
      const defects = subset.map(i => { const [p, q] = pairs[i]; return [p, q, C.K(p, q) ? -1 : 1]; });
      const result = verify_complete_snapshot_answer(C, defects);
      cases++; localChecks += C.N; baseRoutes += result.stats.baseRowRoutes; defectRoutes += result.stats.defectRowRoutes;
    }
  }
  assert(baseRoutes > 0 && defectRoutes > 0);
  return { cases, localChecks, orderChecks: 4 * localChecks, baseRoutes, defectRoutes, mismatches: 0 };
}

function create_current_neighbor_baseline(C) {
  const E = new Map(), canonical = new Map(), corrections = new Map(), degree = new Map();
  const stats = { events: 0, insertions: 0, deletions: 0, returnsToNominal: 0 };
  const actual = (p, q) => C.K(p, q) + Number(E.get(p)?.get(q) || 0n);
  const edit = (p, q, value) => {
    if (!E.has(p)) E.set(p, new Map());
    if (value === 0) E.get(p).delete(q); else E.get(p).set(q, BigInt(value));
    if (!E.get(p).size) E.delete(p);
  };
  const toggle = (p, q) => {
    if (p > q) [p, q] = [q, p]; assert(p !== q);
    const sign = actual(p, q) ? -1 : 1;
    if (sign === 1) stats.insertions++; else stats.deletions++;
    const candidates = new Set([...C.neighbors(p), ...(E.get(p)?.keys() || [])]); let count = 0n;
    for (const z of candidates) if (actual(p, z) && actual(q, z)) { add_sparse_signed_value(corrections, z, BigInt(sign)); count++; }
    for (const v of [p, q]) { add_sparse_signed_value(corrections, v, BigInt(sign) * count); add_sparse_signed_value(degree, v, BigInt(sign)); }
    const value = actual(p, q) + sign - C.K(p, q); edit(p, q, value); edit(q, p, value);
    if (value === 0) { canonical.delete(`${p},${q}`); stats.returnsToNominal++; }
    else canonical.set(`${p},${q}`, [p, q, value]);
    stats.events++;
  };
  return { toggle, defects: () => [...canonical.values()],
    query: v => ({ t: C.tau[C.base(v)] + (corrections.get(v) || 0n),
      d: BigInt(C.rows.get(C.base(v)).size) + (degree.get(v) || 0n) }), stats };
}

function verify_current_signed_update_sequences() {
  let seed = 123456789, snapshots = 0, localChecks = 0, insertions = 0, deletions = 0, returnsToNominal = 0;
  const random = n => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed % n; };
  for (let trial = 0; trial < 24; trial++) {
    const b = 3 + random(3), L = 1 + random(5), edges = [];
    for (let a = 0; a < b; a++) for (let c = a + 1; c < b; c++) if (random(3)) edges.push([a, c, random(L)]);
    const C = create_cyclic_cover_source(b, L, edges), baseline = create_current_neighbor_baseline(C);
    for (let event = 0; event < 24; event++) {
      const p = random(C.N); let q = random(C.N - 1); if (q >= p) q++;
      baseline.toggle(p, q);
      const result = verify_complete_snapshot_answer(C, baseline.defects());
      for (let v = 0; v < C.N; v++) {
        const answer = result.query(v); assert.deepEqual(baseline.query(v), { t: answer.t, d: answer.d });
      }
      snapshots++; localChecks += C.N;
    }
    assert.equal(baseline.stats.events, 24);
    insertions += baseline.stats.insertions; deletions += baseline.stats.deletions;
    returnsToNominal += baseline.stats.returnsToNominal;
  }
  assert(insertions > 0 && deletions > 0 && returnsToNominal > 0);
  return { snapshots, localChecks, insertions, deletions, returnsToNominal, mismatches: 0 };
}

function verify_skewed_defect_star() {
  const L = 32, C = create_cyclic_cover_source(3, L, [[1, 2, 0]]), defects = [];
  for (let g = 0; g < L; g++) defects.push([0, L + g, 1], [0, 2 * L + g, 1]);
  const result = verify_complete_snapshot_answer(C, defects);
  assert.equal(result.total, BigInt(L)); assert.deepEqual(result.orders(0), [0n, 0n, BigInt(L), 0n]);
  assert.equal(result.stats.mixedProbes, 2 * L);
  return { defects: defects.length, centerDegree: 2 * L, triangles: result.total.toString(),
    orderedDefectWedges: 2 * L * (2 * L - 1), mixedProbes: result.stats.mixedProbes,
    baseRowRoutes: result.stats.baseRowRoutes };
}

function verify_targeted_voltage_counterexamples() {
  const zero = create_cyclic_cover_source(3, 5, [[0, 1, 0], [0, 2, 0], [1, 2, 0]]);
  const shifted = create_cyclic_cover_source(3, 5, [[0, 1, 0], [0, 2, 1], [1, 2, 0]]);
  assert.equal(verify_complete_snapshot_answer(shifted, []).total, 0n);
  assert.equal(verify_complete_snapshot_answer(zero, [[0, 5, -1]]).query(10).t, 0n);
  const two = verify_complete_snapshot_answer(zero, [[0, 5, -1], [0, 10, -1]]);
  const three = verify_complete_snapshot_answer(zero, [[0, 5, -1], [0, 10, -1], [5, 10, -1]]);
  assert.deepEqual(two.orders(0), [1n, -2n, 1n, 0n]);
  assert.deepEqual(three.orders(0), [1n, -3n, 3n, -1n]);
  const empty = create_cyclic_cover_source(2, 3, []);
  assert.deepEqual(verify_complete_snapshot_answer(empty, [[0, 1, 1]]).query(0), { t: 0n, d: 1n, lcc: [0n, 1n] });
  const edges = []; for (let c = 2; c < 14; c++) edges.push([0, c, 0], [1, c, 0]);
  const witness = verify_complete_snapshot_answer(create_cyclic_cover_source(14, 2, edges), [[0, 2, 1]]);
  assert.equal(witness.total, 12n); assert.equal(witness.numeric().support, 14);
  assert.throws(() => compute_voltage_sparse_answer(zero, [[0, 5, 1]]));
  assert.throws(() => compute_voltage_sparse_answer(zero, [[0, 5, -1], [0, 5, -1]]));
  assert.throws(() => compute_voltage_sparse_answer(zero, [[0, 0, 1]]));
  return { semanticChecks: 6, invalidInputsRejected: 3, witnessEndpoints: 2, witnessNumericSupport: 14 };
}

function verify_nontwin_cover_family() {
  const rows = [], edges = [[0, 1, 0], [0, 2, 0], [1, 2, 0], [0, 3, 0], [1, 3, 1], [2, 3, 2]];
  for (const L of [5, 11, 31]) {
    const C = create_cyclic_cover_source(4, L, edges); let twins = 0;
    for (let p = 0; p < C.N; p++) for (let q = p + 1; q < C.N; q++) {
      let same = true;
      for (let z = 0; z < C.N; z++) if (z !== p && z !== q && C.K(p, z) !== C.K(q, z)) { same = false; break; }
      if (same) twins++;
    }
    assert.equal(twins, 0);
    const reached = new Set([0]), queue = [0];
    for (let i = 0; i < queue.length; i++) for (const w of C.neighbors(queue[i])) if (!reached.has(w)) { reached.add(w); queue.push(w); }
    assert.equal(reached.size, C.N);
    const result = verify_complete_snapshot_answer(C, [[0, L, -1], [L, 3 * L, 1]]);
    assert.equal(result.total, BigInt(L - 1));
    assert.notEqual(result.total, BigInt(L));
    assert.deepEqual(result.totals, [BigInt(L), 0n, -1n, 0n]);
    rows.push({ L, vertices: C.N, twins, connected: true, total: result.total.toString(), records: result.records, stats: result.stats });
  }
  const L = 1000000007, C = create_cyclic_cover_source(4, L, edges);
  const result = compute_voltage_sparse_answer(C, [[0, L, -1], [L, 3 * L, 1]]);
  assert.equal(result.total, BigInt(L - 1));
  const checks = [[0, 0n, 2n], [L, 0n, 3n], [2 * L, 0n, 3n], [3 * L, 0n, 4n], [2 * L + L - 2, 1n, 3n], [1, 1n, 3n]];
  for (const [v, t, d] of checks) { const answer = result.query(v); assert.deepEqual([answer.t, answer.d], [t, d]); }
  assert.deepEqual(result.records, rows[0].records); assert.deepEqual(result.stats, rows[0].stats);
  return { finite: rows, huge: { L, vertices: C.N, pointChecks: checks.length, total: result.total.toString(),
    records: result.records, stats: result.stats, retainedFullOutputBytes: (48n * BigInt(C.N)).toString() } };
}

console.log(JSON.stringify({ exhaustive: verify_exhaustive_signed_snapshots(),
  updates: verify_current_signed_update_sequences(), targeted: verify_targeted_voltage_counterexamples(),
  skew: verify_skewed_defect_star(), family: verify_nontwin_cover_family() }, null, 2));
```

## Executed Evidence

Executed the fenced source with Node.js v24.9.0 on 2026-09-20, exit status **0**. No production files or separate probe files were created.

| Check | Scope and observed result |
| --- | --- |
| Exhaustive signed snapshots | All 27 simple three-base-node cyclic sources at `L=2` (each base pair absent, shift zero or shift one), times all 576 actual edge-toggle subsets of size at most three: **15,552 snapshots**. |
| Full local and order verification | **93,312 local answers**, each checked for count, degree and exact LCC; **373,248 individual polynomial-order coefficients** checked against independent triple expansion. Both mixed-term execution routes are exercised. Zero mismatches. |
| Event baseline | 24 deterministic seeded source trials with 24 events each: **576 snapshots**, 6,504 complete local comparisons; 379 insertions, 197 deletions, 136 returns to nominal. Both the batch correction formula and current-neighbor baseline agree with the explicit oracle. |
| Targeted semantics | Six semantic witnesses, including nonzero voltage, non-endpoint changes, all correction orders and degree-only changes. Invalid sign, duplicate defect and self-loop each rejected. |
| Skew | An isolated base node gets 64 added edges to both sides of 32 matched pairs. Exactly 32 triangles, all quadratic. The chosen mixed kernel makes 64 probes rather than enumerating 4,032 ordered defect wedges. All 96 local answers match the oracle. |
| Non-twin family | `L=5,11,31`: zero twins and connected undamaged covers; complete edited local answers agree with the oracle. Totals are `4,10,30`. |
| Huge symbolic source | `L=1,000,000,007`, `N=4,000,000,028`: six selected local count/degree checks; total `1,000,000,006`. Same structural record counts and correction-probe counts as `L=5`. No fibre enumeration or huge full-output test. |

**Actual falsification during development:** the first run exited 1 at the family expectation `4n !== 5n`. I had incorrectly predicted that the added edge replaced the deleted triangle. The independently expanded oracle and the correction procedure already agreed on four; the false hand prediction was corrected, not the counting algorithm adjusted to fit it. The retained source now explicitly rejects `T=L` for this family and checks its global order vector `[L,0,-1,0]`. This is why the document keeps the two-defect witness and does not endorse stale independent edge deltas.

The 64-versus-4,032 skew comparison is mixed-kernel work only. It excludes preparation, base counting, residual intersections, sorting, output and index latency. The record counters below report base nodes/arcs, stencils and endpoint domain, not total JS allocations, actual packed bytes, cache misses or physical RAM. The same-voltage comparator is entitled to the same join optimization. No throughput or superiority claim is inferred from this finite run.

Exact output retained for reproducible equality checking:

```json
{
  "exhaustive": {"cases":15552,"localChecks":93312,"orderChecks":373248,"baseRoutes":29700,"defectRoutes":56160,"mismatches":0},
  "updates": {"snapshots":576,"localChecks":6504,"insertions":379,"deletions":197,"returnsToNominal":136,"mismatches":0},
  "targeted": {"semanticChecks":6,"invalidInputsRejected":3,"witnessEndpoints":2,"witnessNumericSupport":14},
  "skew": {"defects":64,"centerDegree":64,"triangles":"32","orderedDefectWedges":4032,"mixedProbes":64,"baseRowRoutes":128},
  "family": {
    "finite": [
      {"L":5,"vertices":20,"twins":0,"connected":true,"total":"4","records":{"baseNodes":4,"baseArcs":12,"stencils":2,"endpointDomain":3},"stats":{"commonProbes":6,"mixedProbes":6,"residualProbes":2,"defectRowRoutes":4,"baseRowRoutes":0}},
      {"L":11,"vertices":44,"twins":0,"connected":true,"total":"10","records":{"baseNodes":4,"baseArcs":12,"stencils":2,"endpointDomain":3},"stats":{"commonProbes":6,"mixedProbes":6,"residualProbes":2,"defectRowRoutes":4,"baseRowRoutes":0}},
      {"L":31,"vertices":124,"twins":0,"connected":true,"total":"30","records":{"baseNodes":4,"baseArcs":12,"stencils":2,"endpointDomain":3},"stats":{"commonProbes":6,"mixedProbes":6,"residualProbes":2,"defectRowRoutes":4,"baseRowRoutes":0}}
    ],
    "huge": {"L":1000000007,"vertices":4000000028,"pointChecks":6,"total":"1000000006","records":{"baseNodes":4,"baseArcs":12,"stencils":2,"endpointDomain":3},"stats":{"commonProbes":6,"mixedProbes":6,"residualProbes":2,"defectRowRoutes":4,"baseRowRoutes":0},"retainedFullOutputBytes":"192000001344"}
  }
}
```

## Next Admission Experiments

1. Hold the base and defect pattern fixed while varying `L`; measure compact bytes, preparation and decode separately from full output. Expect constant record counts, not constant bit widths or constant full-export time.
2. Vary base degree, residual arboricity and repeated relative-phase keys. Compare the exact same source/output interface against both a voltage-aware factorized plan and current-neighbor incremental maintenance. Charge cache construction/refresh; report `CH,CE,J1,J2,Z,X` beside whole CPU/I/O.
3. Force base and defect rows above cache capacity on a physically limited machine. Measure total resident memory, file cache pressure, index misses, sort runs and old/new/output overlap. Include a paused consumer, failed admission, interrupted accumulation and restart; the current JS model proves none of these lifecycle behaviors.
4. Inspect source-native metadata from candidate replicated deployments before claiming relevance. Test whether cyclic translations are authoritative, whether filters preserve them, and whether exceptions stay sparse through a real refresh. Do not discover a costly cover after expansion and report only the compact counting phase.

## Completion Boundary

The result broadens the input domain beyond uniform modules, with exact correction and output contracts. Ordinary voltage algebra plus competent IVM fully matches its mathematics. Keep that distinction when considering portfolio inclusion. Physical-cap execution, crash-safe external indexes, source discovery and real-world workload validation remain unperformed; they are not established by a small exact probe.
