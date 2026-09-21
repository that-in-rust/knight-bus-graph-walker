# Triangle Counts From Masked Matching Runs

Date: 2026-09-20. A07 research extension. Exact representation and schedule with a verified finite compiler/join/output probe. Independent challenge is complete, with the integer-domain fix and subsequent hybrid documented below. Not a demonstrated new general triangle-counting bound or a physical 4 GB implementation.

## The Unfinished Problem

The [voltage-cover method](Voltage-Cover-Triangles.md) retains a small functional answer but can expand `Z = sum_defect kappa(p,q)` witness contributions to produce numeric local counts. Repeated translated changes can make Z much larger than the final number of changed rows. Can we combine those changes before expanding their effects, while still computing every vertex's actual triangle count and degree?

The strongest baseline must receive the same supplied base, shifts, ordered coordinates and flat defects. It may discover interval runs, change its base when appropriate, use temporal joins and aggregate before enumeration. A saving over the old point-stencil schedule is not automatically a saving over that stronger baseline.

## Alternatives And Correction

1. **Expand point stencils.** Straightforward and exact; work/scratch depend on witness multiplicity Z, even when many contributions coalesce.
2. **Convolve translated defect masks.** Initially attractive by analogy with signal processing. However, for one fixed base pair and one witness base vertex, matching edges force one relative displacement. Its kernel is a single shift, not a dense convolution. A generic FFT can add an unnecessary length-L array and work. Multiple base pairs still need aggregation; invoking sparse convolution does not remove their input or output costs.
3. **Retain matching masks as interval runs and join their actual Boolean supports.** This avoids expanding a long run of repeated changes, and counts all interactions directly instead of treating changes independently. It is the selected exact variant.
4. **Sweep coordinate boundaries and maintain a small base graph.** This is a particularly strong control when all shifts are zero. With nonzero shifts, the three edge masks must be aligned to the same root coordinate before intersection; a single unshifted snapshot sweep is generally wrong.

Expert lenses are graph-cover algebra, interval query processing, external-memory scheduling and adversarial comparator design. The contribution being investigated is a supported storage/execution domain, not priority for interval intersection, prefix sums or voltage algebra.

## Supported Source

Vertices are `(a,g)` with `0<=a<b` and `g in Z/LZ`, including isolates. Each nonempty edge class is

```text
r = (a,c,delta,S), a<c, delta in Z/LZ
S = disjoint sorted half-open integer intervals in [0,L)

g in S represents exactly the undirected edge
          (a,g) -- (c,g+delta mod L).
```

Different deltas for the same base pair are allowed and denote different actual edges. Identical `(a,c,delta)` keys must be merged into a single Boolean mask after validating duplicates. Intervals cannot represent multiplicity. Canonical boundaries have integer coordinates and adjacent intervals are coalesced. No within-base edges, actual self-loops, weights or multigraph semantics are admitted in this first profile.

This source strictly includes a simple voltage cover whose matching orbits have insertion/deletion runs. A flat cover-plus-defect input can be converted without expanding its untouched matching orbits: externally sort defects by canonical `(a,c,delta,g)`, validate each sign against the original cover, subtract sorted deleted points from an original `[0,L)` mask, and coalesce added points into runs. Parsing and sorting the f explicit defects still cost at least Omega(f). For original same-base additions, select the existing exact A07 fallback; do not discard them or silently reinterpret the source.

Let M be the number of nonempty classes and R their total linear interval count. Each original full mask split by d deletions leaves at most d+1 intervals; each added point contributes at most one new interval before coalescing. Consequently R<=m+f and M<=m+f for this compiler. A deterministic external builder uses O(m+f) input records and O(m+f) worst-case output runs, with possible substantial run reduction on ordered changes. An arbitrary coordinate permutation can destroy this reduction. Discovering a favorable mapping is a separate task; no free reordering or ID mapping is assumed.

## Root-Aligned Triangle Join

Choose ordered bases `a<c<z` and edge classes with displacements `d_ac`, `d_cz`, `d_az`. A triangle can exist only if

```text
d_az = d_ac + d_cz (mod L).

I = S_ac intersect S_az intersect (S_cz - d_ac).
```

For every integer g in I, exactly one triangle has vertices

```text
(a,g), (c,g+d_ac), (z,g+d_az).
```

Shifted intervals crossing coordinate zero split into two half-open pieces. A cyclic rotation of the interval cursor can preserve sorted output without a length-L array; at most one extra linear interval is introduced by changing the cut of a disjoint circular mask. A simple materializing probe may instead sort its small interval list, but must not be cited as implementing the constant-cursor schedule.

Intersect the three sorted interval streams with three cursors: output the current overlap, advance every cursor ending at the smallest right boundary, and stop when a stream ends. No pairwise Cartesian intermediate is required. Each resulting interval contributes its length to the global triangle count and a range increment to each of the three vertex fibres, shifted to that vertex's coordinate system.

Degree is simpler: each edge-class interval increments degree over its source range and its shifted destination range. Empty classes do not contribute.

## Exact Procedure And Invariant

```text
validate_and_compile_masks(source, caps)
for every edge class (a,c,d,S):
    emit degree boundary events for S on fibre a
    emit degree boundary events for S+d on fibre c
for every composable ordered pair (a,c,d1), (c,z,d2):
    seek class (a,z,(d1+d2) mod L)
    if absent: continue
    for I in intersect(S_ac, S_az, shift(S_cz,-d1)):
        total += length(I)
        emit triangle boundaries on a for I
        emit triangle boundaries on c for I+d1
        emit triangle boundaries on z for I+d1+d2
externally sort/reduce events by (base,coordinate,metric)
sweep each fibre; emit all rows or maximal constant answer runs
verify final events close to zero and sum(local_triangles)=3*total
publish the complete chosen output and its source/format manifest
```

**Correctness theorem.** Under the declared source profile, the procedure returns exact local triangle counts, degrees, global count, and hence exact LCC with denominator `degree*(degree-1)/2` and the zero-degree convention. Every actual triangle has three distinct bases because within-base edges are forbidden. Ordering those bases chooses exactly one root and class triple. The three actual edges determine unique shifts satisfying closure; their simultaneous membership is exactly I. Conversely, every g in I supplies those three distinct vertices and edges. Range events are a lossless encoding of these integer contributions. Thus no triangle is missed or duplicated. Counting the actual Boolean graph includes all orders of interactions between defects, without a separate cubic correction expansion.

## Honest Work And Storage Bound

Let C be the number of composable class pairs examined, Q the number whose closing class exists, and

```text
J = sum over those Q triples of (r_ac + r_az + r_cz + 1)
H = number of emitted root intersection intervals, H <= J.
```

The stated direct enumeration is not automatically a worst-case-optimal multiway join. It can examine C large pairs with empty intersections. Price that work rather than report only surviving triangles. Indexed closing-class lookup adds its actual index cost. The logical work, excluding comparison/index and integer bit-operation factors, is

```text
prepare: read source + sort defects + validate/coalesce
query:   O(b + M + R + C + J)
events:  O(R + H) fixed-width records before reduction
export:  O(bL) rows for the complete numeric table
```

A range can split at wraparound for either destination, so a triangle interval generates at most ten signed triangle boundary records, and a degree interval at most six degree records. Those constants make scratch admission explicit. Event sorting needs bounded buffers and external merge runs. Index pages, three interval cursors, signed counters, output queues and sort buffers all need reservations; R, H and an entire fibre must not be silently resident. Peak disk includes old/new source and result generations plus live event/merge files. Full output is not waived by a small H.

For a fixed-width implementation profile, require N=bL<2^64 and use checked wider temporaries for modular addition and sizes. Final local triangle counts and LCC denominators fit unsigned 128 bits. Global counts and conservative signed intermediate accumulations can use 256 bits. A full `(id:u64,degree:u64,triangles:u128,denominator:u128)` table costs 48N payload bytes before headers. A possible 64-byte event layout would require at most `64*(10H+6R)` initial event-spool payload bytes; this is a design arithmetic allowance, not a measured encoding. Merge generations and indexes need additional disk. Integer bit costs remain explicit outside a fixed-width profile.

The interval probe uses resident dictionaries and output arrays for small oracle comparisons. It does not establish this external schedule, 4 GB physical compliance or a byte-level disk limit. A production implementation can stream all rows from the reduced events, but has not been supplied by this manuscript.

## What A Separation Would And Would Not Prove

Consider K_(p,k) as the zero-shift base and add all left-side pair edges over a contiguous coordinate interval of length h. There are `f=h*choose(p,2)` explicit added defects. The old linear stencil contribution alone expands `h*k*choose(p,2)` witnesses. This compiler reads those f defects and retains one mask per added matching class. Its closed class triangles number `k*choose(p,2)+choose(p,3)`, independent of h. It can represent the local-answer ranges without h-fold kernel repetition and can export all bL rows.

This is a valid work reduction relative to the specified point-expansion schedule, not a lower bound on triangle counting. An interval-aware zero-shift sweep maintains the changing base graph and matches or beats it. A same-input factorized aggregate can compute the repeated right-side contribution even more cheaply. If h=L, rebasing the whole actual graph as another ordinary cover also removes the point expansion. These controls invalidate a claim that the constructed family establishes new algorithmic complexity.

Nonzero shifts and multiple masks per base pair make the representation more general than independent unshifted snapshots, but do not defeat a comparator allowed to align intervals and perform the same join. A useful remaining contribution needs either a strictly better paid join/aggregation strategy or real evidence that compiling this source domain yields a lifecycle advantage over a capable alternative.

## Falsifiers

- Omit `-d_ac` in the third mask: shifted edges can create a real triangle that an unaligned join misses.
- Use only a base triangle and ignore voltage closure: an unbalanced base cycle does not close in the lift.
- Treat wraparound as a clipped interval: vertices across coordinate zero lose contributions.
- Add old per-edge triangle deltas independently: interacting removals can destroy one triangle twice; actual Boolean support avoids that mistake.
- Count interval length as one triangle: a run of h roots denotes h different actual triangles.
- Drop isolates, same-base restrictions or degree-only changes: the returned LCC table is then semantically incomplete.
- Alternate mask membership at every coordinate: R becomes Theta(L); no guaranteed compression remains.
- Count only H: large C and J can precede an empty output. The algorithm is not claimed output-linear.

## Inspected Primary Precedent

- [Liu, Peyerimhoff and Vdovina, Signatures, Lifts, and Eigenvalues of Graphs](https://arxiv.org/pdf/1412.6841): cyclic lifts and oriented inverse labels supply the underlying matching/closure algebra. Masked supports are a representation extension, not a new voltage identity.
- [Hu et al., Computing Complex Temporal Join Queries Efficiently, SIGMOD 2022](https://cs.uwaterloo.ca/~xiaohu/papers/sigmod22-temporal.pdf), introduction and section 2: multiway interval intersections, triangle joins, disjoint validity intervals and shifted interval predicates are explicit primary precedent. The paper warns that join-first filtering may produce excessive intermediate work, directly relevant to C and J here. Our cyclic coordinate is not necessarily time, but that relabeling does not make interval joins novel.
- [Kaufmann et al., Timeline Index, VLDB 2013](https://www.vldb.org/pvldb/vol6/p1210-kaufmann.pdf): temporal joins/aggregation and a version/event representation are precedent for replacing repeated states with boundaries. Prefix-event aggregation is credited, not claimed invented.
- [Bringmann, Fischer and Nakos, Deterministic and Las Vegas Algorithms for Sparse Nonnegative Convolution](https://arxiv.org/pdf/2107.07625), abstract/introduction inspected: advanced convolution already has strong sparse algorithms. Its nonnegative assumptions cannot simply be transferred to cancellation-heavy signed updates. The chosen interval join does not invoke its runtime theorem.

## Verification Plan

1. Test interval shifting/intersection and known zero/nonzero-shift triangles before implementing the new procedure.
2. Expand small sources independently into actual adjacency, enumerate actual triples, and compare every degree and local count, including isolates.
3. Exhaust all Boolean cross-base graphs with three bases and two coordinates; use random larger masks with multiple shifts and wraparound.
4. Compile valid flat signed defects and compare the resulting actual graph and complete answers against independently applying those defects.
5. Preserve orientation, closure, wraparound and independent-delta counterexamples. Measure logical C/J/H only as such, not as elapsed time or RSS.

## Executed Finite Evidence

The lead first ran seven new tests against a module without the candidate functions. All seven failed on explicit missing-function assertions. After implementing the declared procedure, all seven passed. Additional flat-defect/profile/fragmentation checks brought the final suite to ten passing tests:

```sh
python3.11 -m unittest discover -s research_algorithms_20260920/experiments -p test_masked_matching_triangles.py
```

- All 4,096 Boolean cross-base graphs with three bases and L=2 were checked. There are twelve possible actual cross-base edges; enumeration covers every subset, not merely 4,096 seeds. Every local count, degree and global count agrees with independent expanded-adjacency triple enumeration.
- Another 180 deterministic-seed random masks use two to six bases, L from one to eight, multiple displacements and wraparound. These use the same complete independent oracle.
- Eighty additional valid flat-defect snapshots, including reversed endpoint order, were applied independently to an actual edge set, then compared with compilation plus the interval algorithm. The compiler does not expand untouched orbits; the small oracle deliberately does.
- Focused checks preserve a shifted triangle, an unbalanced base cycle, wraparound, two interacting removals, invalid signs, duplicate defects, unsupported same-base edges, overlapping/out-of-range intervals and isolates.
- A fragmentation case has the same fifteen global triangles but changes three total input runs to seventeen, and one root intersection interval to fifteen. This is a counterexample to universal run compression, not a performance benchmark.

The repeated-matching case has p=5, k=7, L=30 and h=20. Two hundred explicit added defects compile with the base into 45 nonempty classes / 45 runs. The join examines 80 composable pairs, finds 80 closed triples and 80 root intervals, with J=320, 180 degree events and 480 triangle events. It produces all 360 degree/local-count rows and T=1,600 triangles, checked against a separate closed-form answer. The old first-order stencil expansion alone would emit 1,400 witness contributions. These are different logical work categories: it is incorrect to call 1,400/80 a measured speedup or to omit the 200-row build and 360-row export.

Source: [candidate probe](experiments/probe_masked_matching_triangles.py) and [independent tests](experiments/test_masked_matching_triangles.py). The candidate is a finite resident implementation of compilation, root alignment, interval intersection and complete event-sweep output; the proposed external cursor/index schedule is not implemented. Test suite wall time is not a graph-system benchmark. The extra tests exercise existing behavior and were not claimed as a second missing-implementation red phase.

## Scientific Status And Next Discriminator

The new result is a correctness/resource derivation for a richer exact storage domain, with a source-to-full-answer finite probe and an explicit point-expansion separation. It has not beaten a matched interval-aware/factorized comparator. The [independent proof and baseline challenge](Triangles-Masked-Matching-Review.md) is now complete for this interval algorithm. In particular, the zero-shift example is not evidence of novelty, and the cyclic coordinate need not correspond to a real customer's useful ordering.

Next: examine whether the aggregate answer can be computed without enumerating all Q closing class triples or their J intervals. A failed empty-intersection case with large C/J would be especially revealing. Any improvement must include its preparation/index cost and compare against existing multiway temporal joins, not only pairwise materialization. A real source corpus must independently establish stable matching classes, useful run lengths, mapping cost and refresh fragmentation before this becomes a product recommendation.

No seven-family completion follows from this additional format.

## Review Integration And Hybrid Follow-Through

The ten-test receipt above is historical. The independent review reproduced silently accepted fractional/bool profile values. A new regression first failed, then strict integer checks and degree conservation brought the suite to eleven passing tests. This fixes the input boundary; it does not change the proof on its declared integer domain.

The review also supplies sharper event bounds, a gauge-equivalent zero-shift control and large-C/J adversaries. Most importantly, full-period multi-shift classes admit ordinary multiplicity convolution, unlike the earlier one-shift/witness setting. Any earlier dismissal of Fourier aggregation is limited to that narrower setting.

The [new hybrid manuscript](Triangles-Hybrid-Orbit-Ownership.md) proves a disjoint FFF/P**/FP*/FFP partition and implements all-full aggregation plus cheapest-pair partial joins. Four new tests first failed before implementation; all fifteen subsequently passed. Its [separate independent challenge](Triangles-Hybrid-Independent-Review.md) is now complete and supports correctness while identifying exact published antecedents for the algebraic steps. Length-L numerical workspace is still resident, and complete results are not free.
