# Certified Paths

Date: 2026-09-20. A03 research candidate for exact reachability, BFS distances and nonnegative SSSP. No production implementation, 4 GB benchmark, or claim of global originality.

**Source gate, 2026-09-21:** the [reviewed unit-hop eligibility result](Paths-Unit-Hop-Source-Eligibility.md)
gives q_min=n-n1+c1 and D=r=0 for simple loopless unit digraphs, with an
anchor-free attaining forest. On the retained Neo4j-family file graph this
still leaves 81.75%/86.20% of old states forward/reverse. The twelve-profile
semantic study passes all target checks, but does not justify a large RAM-win
claim or building the full revival index for this source. This is a bound on
the stated representation, not all shortest-path or reachability engines.

**Current extension, 2026-09-21:** [single forest-cut repair](Paths-Forest-Cut-Repair.md)
proves one-old-row witness locality with explicitly charged raw revivals.
[Implicit partitioned rows](Paths-Cut-Partitioned-Rows.md) then avoids repeated
nested descendant domains and reports only actual heads in each surviving
interval. Its useful incidence bound is <=A old ledger records per source,
with O(a+1) sparse descriptors rather than rewritten owners. The lead replayed
both retained original-path checkers. The subsequent
[multicut extension](Paths-Multicut-Revival-Partition.md) now handles cumulative
selected-forest deletions with unique revival reporting and corrected current
tail ownership. Its completed independent mathematical review and two finite
probes support that restricted contract: 20,153 lead source queries and 10,562
independent target checks. This is not arbitrary-edge failure support, novelty
over equally indexed canonical controls, or a physical resource measurement.

## Premise Check

**Research candidate: a tail-sensitive source-access representation over a fracture-aware tight-forest quotient.** A prepared potential and surviving directed forest make many vertices reconstructible rather than independently mutable. Keep distance/queue state only at entry gates. A new query source can be promoted with a one-row split or handled by the source-seeded alternative. Formerly harmless self-arcs must remain accessible because they can become necessary for a different source.

This computes new exact answers for arbitrary seeds in the prepared projection, including answers changed by admitted edge updates. It is neither D03's physical read reordering nor D04's unchanged-distance certificate. The primary benefit sought is O(q) mutable state for q gates, rather than O(n) mutable state; immutable per-vertex coordinates and full output remain O(n).

The individual ideas of potentials, path domination, graph contraction and distance reconstruction are established. Independent review constructively shows that single-predecessor elimination reproduces this exact source-specific quotient. Its state reduction is therefore not a novel principle. The remaining research question concerns the tail-sensitive source-access index, read/storage behavior and fracture/rebuild accounting against that equally optimized comparator. Publication-level differentiation remains unestablished.

The follow-on [Paths-Access-Frontier.md](Paths-Access-Frontier.md) now supplies a concrete stronger candidate: two range aggregates plus a shared, current route-drift envelope can replace many source-entry seeds with one while preserving their full shortest-path closure. Its retained probe and lead replay cover 4,800 queries and 1,440 ledger updates; the explicit family uses 81 frontier records versus 1,024 per-head minima. That is an access-stage separation only. Equally compressed TNR/CH-style access indexes remain strong unresolved comparators, and a separate independent challenge of the new certificate is in progress.

Read with [D01-D06](../research_4gb_20260919/Architecture-Decision-Map.md), [the decision brief](../research_4gb_20260919/Final-Research-Decision-Brief.md), and [the shared evidence](connectivity-paths-evidence.md). Incidence posting-once BFS, alternate witnesses, potential-based answer validation, and graph skeletons are already repository mechanisms. PageRank and triangles are outside this document's ownership.

Expert lenses: exact paths and counterexamples; external-memory lifecycle; source/query semantics; skeptical prior-art comparison. These are analytical perspectives, not independent peer review.

## Contract And Baselines First

A snapshot is a finite directed graph G=(V,E,w), with all n declared vertices including isolates. Undirected queries explicitly replace each edge with two arcs. Parallel arcs may be kept separately or normalized to their minimum current weight with stable original-edge provenance; loops can be dropped for the distance/one-witness contract. All filters, missing-weight rules, types, IDs and authorization semantics are fixed for a published projection.

Mathematical statements assume exact arithmetic and nonnegative weights. A realizable initial numeric profile uses nonnegative integer weights or integers after an exact common fixed-point scale, a declared W, and (n-1)W <= 2^63-1. Use wider signed intermediates to test reduced costs; do not wrap unsigned subtraction. Distances and residual distances can use u64 with a separate infinity sentinel after checked bounds. Arbitrary floating input plus an epsilon test is NOT exact SSSP.

With 0<=h(v)<=(n-1)W, a shortest finite reduced distance is at most 2(n-1)W, strictly below the u64 maximum used as infinity. Relaxation sums can still overflow on non-shortest walks: compute them wider and discard/saturate only above the proved finite-distance bound, never wrap. The u32 vertex profile requires n<2^32 and explicit null encoding.

Supported outputs:

- Exact reached set.
- Exact original-graph hop distances when all original arc weights are one.
- Exact nonnegative weighted distances.
- One valid shortest parent forest, or one reconstructed source-target path, with original IDs/edge provenance.

Not promised: incumbent BFS visit order, canonical parent ties, all shortest paths, path counts, time-respecting paths, turn costs, negative weights, or permissions-policy evaluation. A depth cutoff can filter exact BFS distances after computation; early stopping needs a separate proof. A list containing every full source-to-vertex path can have quadratic size and is not interchangeable with a parent forest.

The conventional implementation is well-tuned CSR plus Dijkstra/BFS, with bounded external queues/state when necessary. External BFS already has sophisticated locality algorithms under declared graph classes; [Mehlhorn and Meyer](https://resources.mpi-inf.mpg.de/departments/d1/teaching/ws10/models_of_computation/ExternalMemoryBFS.pdf) distinguishes sparse undirected and directed Eulerian cases. Do not transfer its bound to arbitrary directed graphs. [External-memory decrease-key queues](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.ESA.2019.60) provide a serious directed baseline, including results with explicit density/model restrictions.

## Rejected Inventions

| Tempting proposal | Prior art or failure | Outcome |
| --- | --- | --- |
| Keep several tight parents | Existing D04 open direction; cycles of zero-weight parents need rooted support | Not the new contribution |
| Build an f-fault tight reachability subgraph | [Baswana, Choudhary and Roditty](https://web.iitd.ac.in/~keerti/Papers/k-ftrs.pdf) already give O(2^f n)-edge single-source fault-tolerant reachability structures | Useful baseline, not an invention; preserving reachability is not preserving all replacement distances |
| Layer vertices by allowed distance increase | [Fixed-Parameter Sensitivity Oracles, Section 4](https://arxiv.org/html/2112.03059) already uses nonnegative slack, auxiliary layers and fault-tolerant reachability to preserve bounded-detour distances | Reject the broad claim; copied original failures expand across layers |
| Run on a smaller distance graph and reconstruct | VC-index and contraction methods already do this | Must specify a narrower state relation and compare with these algorithms |
| Lazily check edges/witnesses on promising paths | [LazySP](https://personalrobotics.cs.washington.edu/publications/dellin2016lazysp.pdf), Algorithm 1 and Theorems 1-2, already supplies a complete/optimal lazy framework under admissible estimates | Generic laziness is not new |
| Contract every surviving tight edge | Directed zero reachability is not symmetric; an external source can enter the middle | Incorrect without entry closure and source promotion |

## Prepared Potential And Forest

Retain a finite potential h:V->R with

```text
r(u,v) = w(u,v) + h(u) - h(v) >= 0   for every current arc.
```

Also retain a rooted directed forest T whose tree arcs point from parent to child and satisfy r=0. T has no directed cycles, including at zero weight.

One fully specified bootstrap:

1. Pay for one ordinary exact SSSP from a declared anchor s0.
2. Let C be the largest finite distance, and set h(v)=d(s0,v) when finite, h(v)=C otherwise.
3. Traverse the tight subgraph from s0, selecting first-discovery parents. This produces a tree even when tight zero-weight SCCs exist. Old unreachable vertices are singleton roots.
4. Verify r>=0 on every normalized arc and exact tightness of every retained parent. Build tree ancestry coordinates with explicit bounded-memory traversal.

Why finite capping works: between finite-distance vertices the shortest-distance inequality holds. No edge can lead from a reachable vertex to an unreachable vertex. An unreachable tail has h=C, at least the finite h of any head; edges between unreachable vertices have r=w. Thus h is globally feasible.

This preparation is a paid SSSP, not a free certificate supplied by intuition. If an existing exact D04 answer is available, it can seed preparation but its construction/storage still belongs to the workload. h=0 with an empty forest always gives a correct uncompressed fallback, usually q=n.

Reduced costs and their telescoping path identity are old: [Eppstein's k-shortest-path paper, Section 2 and Lemmas 1-3](https://www.ics.uci.edu/~eppstein/pubs/Epp-SJC-98.pdf) uses a shortest-path tree and nonnegative sidetrack costs. This document uses the source-oriented sign convention.

## Fracture, Domination And Entry Closure

For a current generation, let F be the subset of T arcs still present and still tight under h. A deleted or increased-weight tree arc is removed from F. This fractures T into a directed forest; its old ancestry coordinates remain valid when combined with a same-fragment test.

For each current arc e=(u,v), apply this exact domination rule:

```text
u is an ancestor of v in F
    => F contains u->v with weight h(v)-h(u) <= w(u,v)
    => e can be omitted for distances and reachability.
```

The empty u=v path safely dominates a nonnegative loop. Keep F tree arcs as the representation; omit other dominated arcs. Let X be every remaining, non-dominated arc. X may be large; no sparsity is guaranteed.

Define base gates P as every F root plus every head of an arc in X. Split F immediately above each gate. Each resulting capsule is a directed tree rooted at one gate; pi(v) is its nearest gate ancestor.

This is head closure, not a guess that graph partitions are internally interchangeable. Every non-tree arc enters a gate by construction. Every crossing tree arc also enters a gate. A gate can reach every vertex of its capsule through zero-reduced-cost tree arcs.

Let b=|X|, q=|P|. Then q <= min(n, roots(F)+b), and at most q-roots(F) tree connectors cross capsules. Do not assume q is small merely because m is small or an edge representation compresses well.

## Tail-Aware Ledger

Store the following grouped by source capsule:

- Every exception arc in X, as (original_tail, head_gate, reduced_weight, original_edge_ID).
- Every tree arc crossing base capsules, with its actual parent/child endpoints and zero reduced weight.
- Original T parent data for tree reconstruction.

**Retain exceptions whose tail and head currently have the same capsule.** Also do not collapse exceptions solely to a minimum per capsule pair while discarding their original tails. Both transformations can be invalid for a later source inside that capsule.

The base quotient may ignore such self-arcs during a base-gate query, but its ledger must retain them as dormant arcs. This is a semantic requirement for arbitrary-source reuse, not additional shortest-path output.

No pairwise boundary distance matrix or generic shortcut fill is constructed: the ledger has at most b+q-roots(F) records. It can nevertheless approach m. For one fixed source, a transient quotient may aggregate identical gate pairs only after source promotion.

## Arbitrary Source With One Promotion

If source s is already a gate, run directly. Otherwise let g=pi(s), add a virtual gate s, and define

```text
pi_s(v) = s    if pi(v)=g and s is an ancestor of v in F
          pi(v) otherwise.
```

This moves precisely s's portion of the capsule, excluding descendant capsules rooted at existing gates. Add the surviving tree connector parent_F(s)->s, which becomes the zero-cost quotient arc g->s.

To read outgoing ledger rows:

- All rows except g and virtual s retain their old ownership.
- Read g's row for g, ignoring records whose original tail belongs to pi_s^{-1}(s).
- Read that same row for virtual s, retaining precisely those records.
- A dormant self-arc g->g may now become s->g and must be activated.
- Include the extra g->s tree connector when expanding g.

Thus a source change needs no n-element owner-column rewrite or scan of the raw graph. The shared row can be read at most twice in a complete query; a very large g row still makes this expensive. No cache-retention assumption is needed for the two-read bound.

Ancestor testing uses stored tree intervals and current owner equality. Within one base capsule every tree edge between ancestor and descendant is intact. The effective owner is computed on demand for reconstruction.

## Precise Procedure

```text
prepare_fracture_ledger_exact(current, h, T):
    verify h is finite and every current reduced arc weight is nonnegative
    F = current tight arcs of the selected original parent forest T
    determine F roots and ancestry/fragment relation
    X = every current arc not dominated by its directed F path
    P = roots(F) union heads(X)
    pi = nearest P ancestor in F
    ledger = X INCLUDING internal exceptions and original tails
    append F arcs crossing pi capsules to ledger
    group ledger by pi(original_tail), preserving original-edge witnesses
    atomically publish h,T,F/P metadata, pi,ledger as one generation

solve_promoted_source_exact(s):
    pin one published generation
    add virtual gate s if needed; define pi_s and its split-row cursor
    initialize indexed Dijkstra queue/state on at most q+1 gates
    D[s] = 0
    while queue nonempty:
        settle minimum (distance, gate_ID)
        stream that gate's effective ledger row
        ignore only effective self-arcs for this query
        relax each effective arc using its exact reduced weight
        record original witness on strict improvement to an unsettled gate
    for each requested vertex v:
        if D[pi_s(v)] is infinite: emit (unreachable,null); continue
        emit D[pi_s(v)] + h(v) - h(s)
        if v=s: emit null parent
        else: emit F parent if v is not a gate, otherwise selected entry witness
```

Even for original unweighted BFS, quotient costs can be zero or larger than one. **Do not run ordinary hop-count BFS on this quotient to obtain original distances.** Dijkstra or an exact suitable integer queue is required. For reached sets alone, ordinary graph traversal of the effective quotient suffices.

## Correctness

### 1. Domination

Replace an omitted arc with its surviving directed F path. Feasibility gives a replacement of no greater weight. All replacements are real paths in G. Thus the retained forest-plus-exception graph has exactly G's shortest distances and reachability. Nonnegative cycles can be removed, so replacing a shortest path by a walk is harmless.

This argument is snapshot-specific: if any tree witness arc ceases to be present/tight, the domination decision must be recomputed.

### 2. Capsule State Is Constant In Reduced Distance

In the retained graph R=F union X after domination pruning and source promotion, every path into a capsule first enters its gate, unless it starts there. All retained crossing arcs end at gates. Every vertex v is reachable from pi_s(v) with zero reduced cost.

Therefore shortest reduced distance to v in R is at most D(pi_s(v)). Conversely every source-to-v path in R first reaches that gate and then has nonnegative remaining reduced length, so it costs at least D(pi_s(v)). Hence the two are equal, including infinity. The domination lemma transfers equality of distances back to G; a dominated shortcut in G need not itself visit the gate.

Directed reachability in a capsule is only gate-to-vertex, never assumed vertex-to-gate. Promotion makes arbitrary sources obey this particular invariant; the source-seeded alternative below avoids promotion with a different initial-access rule.

### 3. Quotient And Reconstruction

Each quotient arc is an actual retained arc from a reachable tail within its source capsule into the destination gate. Prepend the zero-cost forest path to that tail to lift it to a graph path. Conversely contract a retained graph path into its sequence of gate crossings. The two constructions preserve reduced distance.

For any source-to-v path p,

```text
sum r(e) = sum w(e) + h(s)-h(v).
```

Combining this identity with the capsule equality proves

```text
dist_G(s,v) = dist_Qs(s,pi_s(v)) + h(v)-h(s).
```

This is an exact identity, not a small-error or unchanged-answer test.

### 4. Witnesses And Zero Cycles

Dijkstra assigns a gate predecessor only from an already settled gate to an unsettled one. Never overwrite a settled predecessor on an equal-distance tie. Gate parents are acyclic in settlement order; interior parents are acyclic in forest depth. Lifting produces a finite parent chain to s with exactly the reported weight. A set of zero-weight incoming edges without this rooted structure would not suffice.

## Source-Promotion Counterexample

Take 0->1->2, both weights one, plus 2->0 of weight ten. Let h=(0,1,2). There is one base gate, 0; the extra arc is a dormant self-arc with reduced weight 12.

For source 0, deleting that self-arc from the base quotient appears harmless. For source 2, the exact original distances are (10,11,0). Promoting 2 activates the arc 2->0. If the ledger discarded it, 0 and 1 would be wrongly unreachable.

Similarly, keeping only the cheapest exception per base capsule pair can discard the only usable tail after a source split. Tail identity is therefore part of the reusable state-elimination certificate.

## Preparation, Query And Refresh Costs

Let n be vertices, m current raw arcs, a=b+tree_connectors ledger records, B I/O bytes, q gates, R requested result bytes and I the achieved page-transfer count. Assume u32 internal vertex/gate IDs and u64 weights/edge IDs.

A conservative ledger record is 24 bytes. Optional offsets require 8(q+1) bytes. A concrete indexed-heap state reservation is 64(q+1) bytes, covering distances, heap IDs/positions, settled flags, gate identities and original predecessor witnesses with capacity overhead. This is an implementation allowance requiring enforcement, not a language-independent allocator theorem.

Immutable state remains linear in n: h, tree parents, ancestry coordinates, source-ID lookup and current pi. Page these or store them in vertex rows; never count them as free merely because they are immutable.

```text
query RAM <= 64(q+1) + fixed controlled services
CPU <= O((q+a) log(q+1) + n) for full output
ledger reads <= 24(a_reached + a_g) + page/offset overhead
    a_g = source's split base row, counted once additionally
reconstruction >= R bytes written, plus required vertex metadata reads
```

For arbitrary-ID ordered output, reconstruction may require external sorting or an inverse permutation pass. A selected target requires only its metadata plus one quotient query; a single path of length ell needs O(ell) parent records, potentially random I/O, and an external reverse stack if forward order is required.

**Bootstrap costs:** source extraction, ID normalization, the initial full SSSP, tight-tree selection, bounded DFS/Euler construction, remapping joins and ledger construction. An external DFS stack can be linear-size on disk and must not become an in-memory n-entry call stack. Euler ordering may require rewriting all endpoint records; account for old/raw/remapped coexistence.

**Ledger rebuild:** determine current F fragments, join endpoint metadata, classify all m arcs, mark exception heads, propagate gates down F, then group exceptions/connectors by owner. A safe external implementation uses O(1) full record-sort/join stages once tree order exists, plus tree processing. Expanded endpoint records may be wider than the 24-byte retained format. Do not quote only the final output size as sort traffic.

A simple sort of X normalized bytes with run memory M and fan-in f uses about 2X(1+p) read/write bytes, p=ceil(log_f(ceil(X/M))) for multiple runs. For example X=24 GB, M=1 GB, f=32 gives about 96 GB of traffic for that sort alone. Repeated joins add their own traffic. Random metadata probes are an alternative only with measured cache/I/O costs.

**Refresh:** first check complete changed arcs against unchanged h. Deletions and weight increases cannot violate feasibility; insertions/decreases can. Recompute affected pair minima/provenance before checking. If any r<0, use ordinary exact SSSP or rebuild the potential; do not clamp r to zero.

For an admitted potential, keep only current tight T arcs in F. Reclassify every raw arc whose old domination may have broken. The conservative specified implementation scans all raw arcs. It does not maintain an unbounded edge-to-every-tree-witness dependency index. Rebuild pi and the ledger under disk admission; h and original tree coordinates can remain shared. Tree failure is not an O(1) refresh merely because its change receipt is short.

Incomplete change coverage requires a complete current normalized snapshot and full validation/rebuild, not trust in a partial receipt. An update outside the declared W or ID bounds requires numeric/storage readmission. Source extraction/decompression, projection semantics, raw staging, merge scratch and recovery journals all belong to the job; a local source database must fit alongside the worker within physical 4 GB.

```text
T_workload = T_extract + T_normalize + T_bootstrap + T_build
           + sum(T_source_query + T_delivered_output + T_refresh + T_recovery)
D_peak_host = D_source_staging + D_unique_prepared_pinned
            + D_live_sort_join_scratch + D_local_outputs + D_journals
D_unique_prepared_pinned <= 50 GB across the selected portfolio
D_peak_host <= explicitly available local storage
```

Publish only after both peak disk and measured physical RAM admission, with bounded output backpressure or cancellation. The candidate wins a repeated-source workload only if saved query work pays for its additional bootstrap/index and refresh work, including complete output and parent-ID gathering. Actual source volume, scratch allowance and device rates are unspecified, so these are accounting obligations, not a numerical deadline promise.

## Worked Four-GB Shape

Constructed capacity example, not an observed dataset: n=200M, m=1B, q=2M, a=10M. Think directed dependency trees with many downward tree-dominated weighted arcs and relatively few exceptional entry heads. These exact counts must be measured before admission.

Assume internal IDs use tree preorder. Retain per vertex h:u64, parent:u32, subtree_end:u32, original_ID:u64: 24n=4.8 GB. Assume external IDs are a dense numeric domain, so inverse source lookup is another 4n=0.8 GB. Current pi is 4n=0.8 GB. Arbitrary string IDs require a separately priced dictionary, not this inverse-array assumption.

| Retained object | Decimal GB |
| --- | ---: |
| Raw weighted arcs, 24m | 24.000 |
| Vertex rows, 24n | 4.800 |
| Dense external-ID inverse lookup, 4n | 0.800 |
| Current capsule owner column, 4n | 0.800 |
| Tail-aware exception/connector ledger, 24a | 0.240 |
| Gate offsets, approximately 8q | 0.016 |
| Directory/metadata allowance | 0.250 |
| Normalized current delta allowance | 0.500 |
| Total without retained answer | 31.406 |
| Optional retained u64 distance column | 1.600 |
| Total with one distance answer | 33.006 |

At q=2M, selected mutable state is about 128 MB. Add 300 MB runtime, 512 MB metadata/state cache, 128 MB I/O, 128 MB decoding, 32 MB output, 64 MB directories, and 1,708 MB unassigned accounting headroom: approximately 3 GB. Minor q+1/offset constants come from headroom. The other physical 1 GB must include the OS, relevant cache/kernel allocations and every other local process. No resident full topology, second heap, client materialization or source DB is hidden in the table.

This is 100x fewer distance unknowns, not 100x less process RAM, storage, or latency. A conventional per-vertex u64 distance plus u32 parent is 2.4 GB before its heap, flags and services. A tuned external baseline can still complete, possibly faster.

One full (original_ID, distance, original_parent_ID) stream has at least 24n=4.8 GB payload, plus null/status encoding. The prepared table does not include a locally retained full row spool; add it if requested. A parent forest is not all source-to-vertex paths.

Recovering original parent IDs from stored u32 preorder parents needs an external gather/join or up to n metadata-page probes; it does not justify an uncounted n-entry resident ID map. For parallel-edge identity provenance, either probe the normalized raw pair/identity index while reconstructing, or retain an additional u64 original parent-edge column costing 8n=1.6 GB here. The displayed table includes endpoint parents, not that optional provenance column. Its query I/O or retained/overlap bytes must be added when original edge identities are requested.

During refresh, a distinct pi, ledger, offsets, delta and saved distance column add about 3.156 GB while old readers remain pinned, taking the modeled retained union to 36.162 GB. A further full 24 GB raw replacement would reach 60.162 GB and fail. Share unchanged raw blocks/use a bounded overlay, retire unpinned versions, or postpone reanchoring. Query fallback can use the current raw view without publishing a second full base, but needs separately admitted mutable-state/queue scratch.

## Nearest Art: Does The Contribution Survive?

### VC-Index

[Cheng et al., SIGMOD 2012](https://www.cse.cuhk.edu.hk/~jcheng/papers/VCindex_sigmod12.pdf), Sections 3-5, already reduces to distance graphs and reconstructs eliminated distances. The actual hierarchical index addresses the quadratic simple-index problem; comparing only with its introductory all-pairs matrix would be misleading. Theorem 3 proves distance preservation; Lemma 3 restricts eliminated segments to at most two hops, and Algorithm 5 uses block joins.

Our gate set need not be a vertex cover: an arbitrarily long directed tree capsule can have internal edges and one gate. Tail-sensitive access and current surviving-tree witnesses specify a different supported profile, but not an established novel contraction principle. The independent reduction below shows an exact equivalence with single-predecessor elimination.

### Sparse Signed Tree Models

[Bonnet, Geniet, Kim and Moon, ICALP 2026](https://drops.dagstuhl.de/storage/00lipics/lipics-vol374-icalp2026/html/LIPIcs.ICALP.2026.40/LIPIcs.ICALP.2026.40.html) already obtains shortest-path trees in O(p log n) time from unweighted signed tree models. Section 3 converts laminar signed rectangles into an interval biclique partition; negative children are removed geometrically, then DAG compression supplies a distance model. Its dynamic discussion also accounts for leaf edits and periodic rebuilds.

Thus "BFS without expanding compressed edges" and "tree representation with witnesses" do not survive as original claims. Our proposal instead eliminates mutable states using a weighted feasible potential and directed forest, without assuming a signed model. No 4 GB I/O guarantee follows from their RAM-model theorem, nor does our method improve it on its graph classes. Compare directly whenever a compact model is available.

### Contraction And Sidetracks

[Eppstein's reduced-cost/sidetrack framework](https://www.ics.uci.edu/~eppstein/pubs/Epp-SJC-98.pdf) already establishes the slack identity. [Customizable Contraction Hierarchies](https://ben-strasser.net/paper/customizable_contraction_hierarchies_arxiv_preprint.pdf) explicitly separates topology preparation, metric customization and queries. Reweighting, eliminating interiors, reconstructing paths and adapting to metric changes are not novel by themselves.

The [independent review](Connectivity-Paths-Independent-Review.md) makes the comparison decisive: in F union X every nongate has exactly one incoming arc, its zero-cost parent. Eliminate each nongate except the chosen source, redirecting its outgoing records to that predecessor. The complete resulting non-self arc multiset, with original edge identities, is exactly this source-specific quotient. No predecessor-times-successor fill is needed. Computing nearest retained ancestors directly avoids repeated redirects along long chains.

Therefore q-state compression and source promotion are **specialized single-predecessor elimination**, not established new shortest-path principles. The remaining candidate is the reusable tail-sensitive source-access representation and its read/storage tradeoff against an equally optimized elimination baseline. Dormant and parallel-tail information cannot be dropped blindly, but a range-minimum structure may encode it without keeping every original record. No minimality claim is proved.

### Source-Seeded Alternative Without A Virtual Gate

For an interior source s in base capsule g, retain the base quotient Q and define

```text
R_s = {v : pi(v)=g and s is an F ancestor of v}
alpha_s(p) = min {r(u,p) : ledger arc (u,p) has u in R_s}
Delta(p) = min_z [alpha_s(z) + dist_Q(z,p)]
rho_s(v) = 0 if v in R_s, else Delta(pi(v))
dist_G(s,v) = rho_s(v) + h(v) - h(s).
```

An empty minimum is infinity. Initialize multi-source Dijkstra on the q base gates using alpha_s and the selected original seed-edge witnesses. For s already a base gate, use ordinary base-gate Dijkstra from s. Dormant base self-arcs participate in source seeding even though ordinary Q traversal ignores them. Keep source-tail provenance for path reconstruction.

Before its first base gate, a retained path from s follows F within R_s. Each possible first exit is exactly one of the seeded ledger records. After that exit, Q represents all gate-to-gate travel. Every R_s vertex already has optimal reduced distance zero by nonnegativity. This proves the formula and explains why no virtual gate or rewritten owner plane is necessary.

Read the source capsule row once to construct alpha_s, then at most once during ordinary expansion if its base gate becomes reachable. This has the same asymptotic two-read guarantee as promotion. A row ordered by original tail preorder can restrict the first read to the source subtree interval; the extra order/index, seeks and retained size must be priced. The initial minimum by head can update the existing q-state distance array directly. Original output and witness reconstruction remain part of the job.

The independent probe checked both source-seeded and promoted results against original-arc Bellman-Ford for 12,879 admitted source queries, including 173,106 parent-chain checks. It also checked exact multiset equality with generic single-predecessor elimination. These are concrete evidence of correctness AND of a narrower originality boundary; they are not machine-scale performance evidence.

## Competent Comparators

1. Plain indexed-heap Dijkstra/BFS on identical normalized topology, same output and precision.
2. Tree-dominance-pruned graph with an ordinary n-state solver. This isolates state elimination from edge pruning.
3. Standard degree-one/path contraction and zero-cost SCC contraction, with arbitrary-source reconstruction. A directed one-entry branching capsule is not a zero SCC, but simpler contractions can overlap.
4. VC-index and CH/CCH where their storage and query contracts are suitable. Include index construction/customization and path reconstruction.
5. Signed-model/DAG-compression traversal on eligible unweighted input, without charging an unnecessary clique expansion to it.
6. D04 validation plus exact fallback for repeated identical roots; a true unchanged answer should use that cheaper gate.
7. Ordinary topological shortest paths on DAGs. The favorable weighted-forward example is a DAG; a competent DAG baseline is mandatory.
8. Exact fault/sensitivity structures for genuinely bounded failures, with their preprocessing and retained size included.

## Supported Shapes And Adversaries

**Favorable hypothesis:** many vertices lie in intact directed trees; non-tree arcs are either dominated by ancestor-to-descendant tree paths or enter a small set of gates. Sources vary, answers change, and q remains much smaller than n. No repeated adjacency rows or source incidence factorization is required.

**Constructive family:** a directed chain with unit tree weights plus forward arcs u->v weighted v-u+c, c>=0. All such arcs are dominated and the base has one gate, even with quadratically many distinct weighted arcs. This family proves a structural state reduction, not a practical reason to store a quadratic raw input. If an input formula is available, a formula-aware baseline must receive it too.

Adversaries:

- Unweighted forward shortcuts of several hops are NOT dominated by a longer tree path. The weighted dense example cannot be advertised as a BFS density win.
- Expander-like/cyclic graphs can put an exception head at every vertex: q=n, so metadata and preparation lose.
- Deleting one middle edge of the dense forward family invalidates many domination paths and can create Theta(n) gates and Theta(m) exceptions.
- An insertion with r<0 makes the old potential unsafe, even though its original weight is nonnegative.
- A new filter can delete tree witnesses across the graph; it is a new projection/refresh, not a free runtime flag.
- Zero-weight cycles defeat surviving-parent-count certificates. Use a real forest and settlement-ordered quotient parents.
- A huge single ledger row can be read twice for a new interior source; small q does not imply small I/O.
- Returning complete results always costs Omega(n) rows, while one path can still incur a long dependent-read chain.
- Identical q and a do not determine disk latency: physical page layout, decoder cost, ancestry probes, and output permutation matter.

## Changed-Design Rubber Duck

Initial design: "All tight forest edges have zero reduced cost; contract them and run Dijkstra on the remaining edges."

Challenge: zero-cost directed reachability does not allow entering the middle of a contracted tree and reaching its ancestors. Cross edges can also land in the middle. The contraction was unsound.

Revision: promote every non-dominated exception head and every forest root; promote the actual source. This makes entry through the capsule root an invariant, which proves constant reduced state.

Second challenge: "Then discard quotient self-loops and retain only the lightest arc per gate pair."

Counterexample: the 0->1->2->0 graph above. Changing the source activates a previously internal arc. Gate-pair minima can discard essential tail positions.

Revision: preserve original tails and dormant exceptions, split only the source capsule row, and reconstruct through a virtual owner function. The new-source query does not rebuild n owners.

Third challenge: "Refresh only the changed tree edge."

Counterexample: the twelve-vertex dense forward fixture in the evidence note grows from one to seven gates after deleting 5->6. Downstream distances actually increase by three.

Revision: fracture F, invalidate all affected domination decisions, and price a complete raw scan/rebuild as the initial correct implementation. Remove any claim of change-count-only refresh complexity.

## Actual Evidence

See [commands and outputs](connectivity-paths-evidence.md).

- All absent/zero/one three-vertex directed graphs, every single arc change, all preparation anchors and all sources: 78,732 trials; 70,470 admitted; zero distance or witness failures.
- Seeded eight-vertex edit batches: 24,000 trials; 15,528 admitted; zero admitted failures.
- All four-vertex unweighted digraphs, every anchor and source: 65,536 exact comparisons, including unreachable vertices.
- Source-row overlay versus fully rebuilt source-specific quotient: 16,384 comparisons; zero differences.
- Explicit dormant-self-arc and dense-forward-fracture counterexamples reproduced.
- Independent review's 12,879 original-arc Bellman-Ford/source-promotion/source-seeding/elimination comparisons and 173,106 parent chains; lead replay of the retained independent source reproduced its complete output with exit status 0.

The 16,734 potential-rejected path cases were rejected by the fast path, not magically solved by a negative-cost Dijkstra. The independent oracle computed their true distances, but no production fallback was implemented. All finite arithmetic in the probes used exact Python integers. These are small combinatorial checks, not real memory/I/O benchmarks or proof that a source CDC stream is complete.

## Final Synthesis And Hard Gap

This proposal gives a precise, testable specialization of single-predecessor elimination that replaces n mutable distance unknowns by q entry-gate unknowns while preserving the stated answers and witnesses. The reusable tail-sensitive ledger is more specific than existing unchanged-answer certificates or physical traversal scheduling, but has not demonstrated a new advantage over an equally optimized elimination/source-access index.

**The hard gap is persistent small q after real updates, with affordable discovery and rebuilding.** One broken forest edge can destroy the reduction. Bootstrap SSSP, endpoint joins, complete output and pinned generations may erase all benefit.

The decisive experiment must compare full source-to-first-answer and repeated-source/changed-data costs against the strongest applicable pruned/contraction baseline on physical 4 GB hardware, with <=50 GB retained prepared union and explicit scratch/output caps. Report both profitable and losing shapes. If q approaches n or index preparation dominates the workload, retain this as a correct specialized contraction, not a differentiated general graph algorithm.
