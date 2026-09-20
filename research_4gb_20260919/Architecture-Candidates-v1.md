# Four Gigabyte Algorithmic Architecture Candidates

Date: 2026-09-19. Status: provisional, corpus reading continues.

This is a developing design document, not the final recommendation. It combines the founder objective, the user's end-to-end workflow clarification, the first ten complete PRD04 reads, initial reader-lane evidence, fresh primary-source checks, and small executable mathematical sanity tests. No performance benchmark or 4 GB implementation has been completed here.

## Product Objective Before Mechanism

The customer wants a completed investigation or recurring report, not a particular storage representation. The leading hypothesis remains a dependency/security graph artifact whose owner cannot comfortably run a useful query on an affordable machine. Public pain signals do not yet establish paid demand, acceptable staleness, or that kernel RAM is the actual bottleneck.

The complete workflow is:

1. Declare the question, semantics, result format, recurrence, freshness and deadline.
2. Import CSV or obtain a consistent source extraction, retaining stable external IDs and selected properties.
3. Profile under a resource budget; offer build/run alternatives with explicit uncertainty.
4. Prepare only justified layouts under bounded RAM and temporary disk.
5. Run supported queries with bounded concurrency and streamed output.
6. Deliver IDs and provenance the customer's downstream tools understand.
7. Refresh, retain or retire artifacts without exceeding RAM or disk during overlap.

Preparation, source load and result delivery are not free exclusions from an end-to-end claim. A separate Neo4j-connected worker has additional source database cost; it is not a complete 4 GB Neo4j replacement merely because that worker fits.

## Resource Contract

Use decimal units: 4 GB = 4,000,000,000 physical bytes. A provisional 3,000,000,000-byte worker envelope leaves 1,000,000,000 bytes for the host. This is an engineering target needing measured validation, not a proven operating-system reserve.

For the new storage scenario, prepared retained artifacts have a 50,000,000,000-byte cap. Separately specify input staging, temporary disk, output destination, previous versions and recovery files. If 50 GB instead means all local disk, all of those must share it. The older PMF001 scenario described 50 GB of GDS RAM and cannot be substituted for this cap.

Illustrative worker allocation, not a measured minimum or a universal kernel configuration:

| Category | Decimal MB |
| --- | ---: |
| Runtime, threads, libraries and native overhead reserve | 300 |
| I/O buffers | 384 |
| Bounded state-page cache | 768 |
| Sort/join workspace | 512 |
| Decoded graph blocks | 128 |
| Output buffers | 64 |
| Resident directories and control data | 64 |
| Unassigned safety and accounting headroom | 780 |
| Total worker target | 3,000 |

This is a reservation plan. Linux cgroup current/peak, process metrics, swap, machine-wide memory and kernel behavior must establish what actually resides. Do not add heap to RSS or mapped pages to a total that already includes them. A cap that kills the worker is containment, not successful computation. See [Linux memory controller documentation](https://docs.kernel.org/admin-guide/cgroup-v2.html).

Build and query may initially run sequentially. If they overlap, their simultaneously live buffers plus shared services must fit the same envelope. Disk admission uses simultaneously retained files, not the sum of nominal final sizes alone. Two full 50 GB generations cannot coexist under a 50 GB all-disk cap.

## What Already Exists In The Research

The existing A007 atlas already proposes compressed rows, selective transpose, bounded frontiers, external sorting, quotients, answer artifacts, precision ladders, stateless walk generation, predecessor recomputation, per-family plans, and refresh modes. These are not new findings here.

The three candidates below make narrower changes that can be specified and falsified. No global novelty or patent claim is made. Further reading of `innovation-mega-arch-20260726v1.md` and the v2/v3 storage timelines found that twins, residual quotients and graph grammars are already repository ideas. Candidates 2 and 3 therefore refine and correct existing directions, rather than count as wholly new architectures. Candidate 1 adds explicit answer-inheritance certificates to the existing materialization/refresh story; it too draws on established dynamic-algorithm verification. The final research must add and compare further mechanisms, not satisfy the innovation objective by renaming these three alone.

## Candidate 1: Certified Answer Inheritance

### Customer Job

A team repeatedly asks dependency reachability, unweighted distance, or component questions on snapshots with modest updates. Today an implementation might discard every answer whenever the snapshot changes. Instead, try proving that a particular answer remains valid before paying to recompute it.

This is not merely serving an old answer with an old timestamp. On a successful certificate check, the old answer is certified for the new snapshot under a restricted contract.

### BFS Mechanism

Assume a fixed vertex universe, source, direction, relationship projection and unweighted simple directed graph. A verified old BFS result stores distance `d(v)` and one parent edge for each reachable non-root vertex. Unreachable vertices have infinity. Use the complete net edge delta between old and new snapshots.

Accept inheritance only when:

1. Every old parent edge still exists in the new graph.
2. Every inserted edge `(u,v)` satisfies `d(v) <= d(u) + 1`, with the usual infinity semantics.
3. The root, ID interpretation and projection semantics are unchanged.

Deletion of non-parent edges needs no graph traversal. If the certificate fails, fall back to recomputation or a separately verified repair plan; failure does not prove the answer changed.

### Correctness Argument

The surviving parent tree gives a new-graph path of length `d(v)` to every formerly reachable vertex, so its shortest distance cannot be larger. All unchanged edges already satisfy the distance inequality; the insertion test establishes it for every new edge. Along any root-to-vertex path those inequalities imply `d(v)` cannot exceed the path length. Consequently no shorter path or newly reachable vertex is possible, and the old distances remain exact.

This argument supports distances, reached sets and one valid shortest-path witness. It does not preserve an incumbent's exact traversal order, parent tie-breaking, every shortest path, path counts, weighted paths, or arbitrary Cypher path enumeration. Those are different contracts.

### WCC Mechanism

Persist a component label vector and a spanning forest that connects every component. Inheritance is valid if every forest edge survives and every inserted edge joins vertices that already have the same component label. Surviving trees prove internal connectivity; no inserted cross-component edge means old components cannot merge.

The forest condition is essential. Checking only that edges have equal endpoint labels would wrongly accept the one-component labeling for every graph. Component IDs are arbitrary; validation compares partitions.

### Bounded Storage And Execution

- Store distances/labels and witness edges in immutable paged columns; do not make the whole certificate resident.
- Join changed endpoints against those columns through a bounded indexed probe cache or external sorted merge. An index avoids a full label scan only when its random-page traffic is cheaper.
- Validate witness deletion against an edge-ID index or stable endpoint membership for the stated simple-graph contract.
- Reuse unchanged answer pages by reference in the new generation. Only manifest/certificate lineage changes when all checks pass.
- Pin the prior proof, full semantic key and a complete consistent delta. A missing change invalidates the argument. A file checksum does not prove change-feed completeness.

At V=100M, u32 distance plus u32 parent is 800 MB of logical answer payload, not a required resident allocation. An additional u32 component vector is 400 MB; its forest witness is additional. External-string IDs, page indexes and metadata also count. This can fit comfortably on disk for some 50 GB scenarios but is not guaranteed for arbitrary properties or many roots.

### Expected Delta And Failure Case

An accepted certificate can replace an `O(V+E)` recomputation with delta joins and small publication work. If indexed lookups are too random, a streaming certificate scan may still be `O(V)` bytes. There is no unconditional `O(delta)` latency claim.

Deleting a selected BFS parent edge can fail the check even when an alternate shortest path survives. A changed source or projection requires a new answer. Many roots multiply persistent certificates. Updates that alter global connectivity eliminate the easy path. Bounded-memory fallback is still needed.

### New Engineering Question

Which witness representation maximizes cheap successful validation per stored byte? One parent is compact but brittle; a bounded set of alternate equal-distance parent witnesses could improve acceptance. Such a witness set must still prove every reachable vertex has a surviving chain down to the root. Merely counting surviving incoming edges can admit unsupported cycles in a more general weighted contract. This extension is unimplemented and needs its own proof and cost model.

Dynamic shortest-path maintenance is established. [Incremental shortest-path notes](https://web.iitd.ac.in/~keerti/Courses/COL866-Notes/IncrementalSSSP.pdf) provide a primary teaching reference for distance/parent update conditions. Our proposed product-specific combination is a bounded external certificate validator, immutable answer-page reuse and selective recomputation across analytical generations, not a claim to have invented dynamic BFS.

## Candidate 2: Execute Shared Rows Once

### Customer Job

Run PageRank-like propagation on graphs with many identical outgoing neighborhoods, such as some repeated dependency or incidence structures. Measure that redundancy first; it is not universal in social or transaction graphs.

### Mechanism

Partition source vertices into classes with exactly equal normalized outgoing rows. For unweighted graphs this means equal sorted neighbor sets. For weighted graphs equality must include the normalized weights and the numerical contract; approximate row matching is not permitted in the exact structural plan.

For class `g`, let `P_g(v)` be its shared transition row and `m_g = sum(r_u for u in g)`. Then one PageRank update is:

`r_next(v) = (1-alpha)*p(v) + alpha*sum_g(m_g*P_g(v)) + dangling_term(v)`.

The graph stores one row per class plus source membership. In variant 2A, each iteration reads old ranks to obtain class masses and executes each shared row once. It does not expand a shared row for every member. This conservative variant retains full per-node state; variant 2B below eliminates that iterative state where its restricted contract applies.

This is algebraically equivalent over real arithmetic. Floating summation order differs, so finite-iteration bitwise GDS parity is not implied. Use a documented tolerance and pinned dangling, teleport, damping and stopping semantics. The first test profile uses a fixed iteration count and uniform teleportation.

### Storage And Execution

Order internal vertex IDs by source class during preparation, retaining a reversible external-ID map. Class masses can then be produced by a sequential rank scan. The target IDs in graph rows must be remapped consistently. For huge class tables, masses themselves are paged or streamed.

Use destination partitions to bound accumulation. The implementation must account for either rereading source masses, writing destination partial-sum runs, or maintaining a bounded destination-page cache. Factoring reduces repeated edge arithmetic but does not abolish scatter I/O or the full result vector.

Let V be vertices, E original edge entries, G distinct rows and K entries across distinct rows. A simple factored artifact is approximately:

`4*K + 8*(G+1) row offsets + 8*(G+1) class boundaries + ID mapping + properties`.

The formula assumes u32 target IDs and class-contiguous source IDs. Without reordering, source membership costs approximately another `4*V` bytes plus boundaries. Choose widths from actual counts, including sentinel values. General input properties and the external-ID map are not free.

### Quantitative Scenario

Hypothesis fixture: V=100M, E=1B, G=10M and K=100M. Each distinct ten-neighbor row appears ten times. Ignoring mapping/properties common to both designs:

| Term | Raw one-way CSR | Factored rows |
| --- | ---: | ---: |
| Target entries | 4.0 GB | 0.4 GB |
| Row offsets | 0.8 GB | 0.08 GB |
| Extra class boundaries | none | 0.08 GB |
| Sum of these terms | 4.8 GB | 0.56 GB |

The 88.3% reduction in these topology terms is arithmetic for this deliberately redundant fixture, not a compression forecast for customer data. Two full f64 rank vectors still contain 1.6 GB at V=100M; at V=1B they contain 16 GB and require external state. A 90% reduction in edge entries does not imply a 90% reduction in end-to-end time.

### Prior Art And Actual Research Delta

[Exploiting Computation-Friendly Graph Compression Methods](https://arxiv.org/abs/1708.07271) and [Graph Compression for Adjacency-Matrix Multiplication](https://link.springer.com/article/10.1007/s42979-022-01084-2) already study performing matrix-vector work directly on compressed graph representations. Biclique and row-reference approaches are precedents, not our invention.

The open hypothesis is whether a restricted, bounded-build row-factor compiler with per-block fallback and external state scheduling gives better complete-job economics on a 4 GB host than ordinary compressed tiled CSR. Exact-row factoring is deliberately simpler to verify than arbitrary overlapping biclique extraction. Broader pattern factoring is a later candidate, not assumed needed.

### Failure Cases

All rows distinct: G=V and K=E, leaving only extra metadata and build work. Tiny rows: offsets and mapping overwhelm saved edges. High update churn: group membership changes require reclassification and possibly expensive physical reordering. Weighted differences may destroy equality. Serving general neighbor enumeration must expand logical edges, so the PageRank gain does not transfer automatically to other queries.

The compiler should only publish this artifact when measured bytes and predicted amortization beat a plain block; a fallback representation is part of the format. Changing the physical plan must not silently change graph semantics.

### Variant 2B: Class-Mass Iteration With Routing Reconstruction

The older quotient proposal divides a class PageRank by its multiplicity. That is not valid merely because the members have equal outgoing rows. Example: `0->2`, `1->2`, `2->0`. Nodes 0 and 1 have the same outgoing neighbor, but only node 0 receives a link from node 2; their PageRank scores differ.

There is a stronger correct use of the same row equality. Define `M_g(t) = sum(r_u(t) for u in class g)` and `p_g = sum(p_u for u in class g)`. Define a quotient transition `Q[g,h] = sum(P_g(v) for v in class h)`. For a dangling source class, use the pinned teleport distribution as its stochastic transition, matching the chosen original operator.

The class totals obey the closed recurrence:

`M_h(t+1) = (1-alpha)*p_h + alpha*sum_g(M_g(t)*Q[g,h])`.

This follows by summing the original per-vertex recurrence over each destination class. Because all sources in a class have identical full transition rows, source rank distribution inside that class is unnecessary for the next update.

After T fixed iterations, reconstruct each original vertex using the PREVIOUS class totals:

`r_v(T) = (1-alpha)*p_v + alpha*sum_g(M_g(T-1)*P_g(v))`.

Do not divide a class total equally among members. Also do not reconstruct from `M(T)` when promising the result after exactly T iterations; that would take an extra propagation step. T=0 returns the original initialization instead.

The iterative vectors are now `16*G` bytes for two f64 class-total arrays, rather than `16*V`. At V=1B and G=10M, that is 160 MB rather than 16 GB, a 100x reduction in those two arrays. Teleport masses, quotient topology, buffers, ID maps and output are additional. If G approaches V, this advantage disappears. This is conditional exact structural compression, not floating-point precision reduction.

Two topology views may be justified:

- quotient rows Q for repeated iterations, aggregating targets by destination class;
- shared original-target rows P for final original-vertex reconstruction and other supported reads.

Both count against persistent storage. Alternatively derive quotient destinations while scanning shared rows, charging mapping lookups and repeated work. The compiler must compare those alternatives, not assume a second view is free.

Final reconstruction does not need all V results in RAM. Emit bounded `(target, contribution)` runs, merge by target, add teleport mass, and stream results. This incurs external accumulation and a full output pass once. A full f64 result at V=1B still contains 8 GB of values. Top-k-only consumption can reduce retained output, not remove the work needed to establish exact scores under the chosen iteration contract.

This variant preserves fixed-iteration mathematical propagation but changes summation grouping. It does not automatically preserve GDS's per-node stopping decision, returned iteration counts, traversal ordering, or bitwise values. A class-level norm can hide opposing per-node changes, so copying a per-node convergence threshold onto class totals is unsafe. For a separate residual-based contract, derive a bound for the full stochastic operator and include floating error; that extension is not yet verified here.

An arbitrary lumpable partition is also insufficient for this reconstruction formula unless each class's full row really is shared, or another proven reconstruction operator exists. Restricting the grouping is what makes the source-class masses sufficient. Equitable-partition literature is prior art; the contribution to this dossier is the precise admissible representation, correct expansion and complete memory/I/O accounting missing from the earlier universal quotient claims.

## Candidate 3: Similarity Classes With Exact Expansion

### Customer Job

Compute exact binary-neighborhood Jaccard top-k on repetitive entity-feature graphs. Distinct entities can have identical feature sets while still requiring separate output IDs. Instead of repeatedly comparing those same sets, compute on their equality classes and expand only the requested output.

### Mechanism

1. Canonicalize the selected neighbor sets according to the exact projection contract.
2. Hash rows for grouping, then verify full equality; hashes alone cannot establish identity.
3. Store one vector and a sorted member-ID list per equality class.
4. Compare class vectors with an exact join or scan. Reuse each class-pair score for all corresponding entity pairs.
5. For top-k, retain the first `k+1` candidate entity IDs per source class under score-descending, ID-ascending order, including self candidates. At query/export time remove the source entity itself and return the first k.

The `k+1` rule works because all members of a source class have the same score against every candidate entity, and removing one source ID can remove at most one retained candidate. It requires a common candidate universe and tie policy. Per-source permission filters, different label filters or different metric parameters can split a class or invalidate reuse.

When comparing a class against itself, multiplicity matters. A nonempty class with at least k+1 members can satisfy all k results at Jaccard 1, subject to the pinned tie policy. Empty-set similarity and zero-score inclusion must be defined; they are not assumed to match GDS defaults.

### Bounded Execution

Class vectors and postings stay on disk. Process a fixed number of source classes at a time and keep only their k+1 best candidates. The complete class-result artifact is not required in RAM. Member expansion streams to the output sink with backpressure.

If U is the number of unique sets, exhaustive score work falls from roughly N-squared to U-squared, but a strong existing exact prefix-join baseline can already avoid much of that work. Compare against that baseline, not just brute force. Output remains up to N*k entities; grouping does not remove the cost of delivering them.

Example with N=100M, U=1M, k=10, u32 IDs and f64 scores: a raw expanded top-k payload is 12 GB. A shared k+1 candidate table is 132 MB, plus a 400 MB dense node-to-class map before metadata. That is about 532 MB of these answer terms on disk, roughly 95.6% smaller than the 12 GB expanded answer. Full expanded export still writes 12 GB. The vector dictionary, source IDs, build scratch and postings are additional. Actual resident memory is determined by admitted blocks, not these total artifact sizes.

### Prior Art And New Testable Combination

Equality-class processing, dictionary encoding and exact set-similarity joins are established ideas. [SetSimilaritySearch](https://github.com/ekzhu/SetSimilaritySearch) documents a practical exact AllPairs implementation and links its research lineage. The proposal is a graph-specific multiplicity-aware answer representation with bounded class joins, stable original IDs and deterministic expansion, evaluated over the entire build/query/refresh lifecycle. Whether this combination is absent from the rest of the repository remains under review.

### Failure Cases

U approximately N provides no score reuse and adds a grouping pass. Dense nonduplicate neighborhoods can still make exact work enormous. Different per-node filters destroy exchangeability. A requested full similarity graph can have quadratic output. A graph procedure returning a sampled KNN result is not replaced compatibly by exact Jaccard. The format must declare its narrow support.

## Small Executable Checks Actually Run

All checks below ran as pure JavaScript in the tool runtime on 2026-09-19. They test restricted mathematical behavior, not Rust code, GDS integration, source extraction, large data, RAM enforcement or latency. Exhaustive finite cases are strong bug-finding evidence, not general proofs.

| Check | Enumerated scope | Result |
| --- | --- | --- |
| BFS certificate, arbitrary edge batches | All 64 loop-free directed graphs on three vertices, all ordered old/new pairs, every root: 12,288 cases | 2,304 accepted; zero accepted wrong distance vectors |
| BFS certificate, single toggles | All 4,096 loop-free directed graphs on four vertices, 12 edge toggles each, every root: 196,608 cases | 124,416 accepted; zero false acceptance; 1,536 unchanged answers conservatively rejected |
| WCC forest certificate | All 64 simple undirected four-vertex graphs, every ordered old/new pair: 4,096 cases | 346 accepted; zero false acceptance; 1,172 unchanged answers conservatively rejected |
| Factored PageRank | All 512 directed three-vertex graphs including self-loops, 20 iterations each | Maximum absolute difference 6.661338147750939e-16 across 10,240 iteration comparisons; 154 graphs had fewer factored nonzeros |
| Class-mass-only PageRank and reconstruction | Same 512 graphs and 20 iterations; next class state computed only through Q, not by regrouping the full oracle vector | Maximum absolute difference 5.551115123125783e-16 across 10,240 reconstructed-vector comparisons |
| Grouped Jaccard expansion | All 4,096 collections of four subsets of a three-feature universe; four source rows; k=1,2,3 | 49,152 top-k comparisons, zero mismatches, including empty sets and ties |
| Shared k+1 Jaccard answer table | Same 4,096 collections; one inclusive candidate table per source class, source excluded only at delivery | 49,152 top-k comparisons, zero mismatches |

BFS oracle: ordinary queue BFS with ascending adjacency; unreachable=Infinity. Certificate: surviving parent edges and distance inequality for every inserted edge. WCC oracle: minimum-root component traversal; witness=discovered spanning forest. Both tests use fixed vertex sets and complete computed graph deltas.

PageRank oracle: ordinary source-row propagation, damping .85, uniform teleport and dangling redistribution, starting vector [.1,.2,.7]. Factored version groups identical sorted outgoing rows and sums source mass first. This is tolerance evidence only; no bitwise parity claim.

Jaccard oracle: brute-force per-entity scoring, self excluded, descending score and ascending integer ID. Empty/empty=0 and zero scores are included for this test profile. The first grouped test expands at most k candidates from each class after source exclusion. The second explicitly constructs the common k+1 table and excludes self at delivery. Neither test implements on-disk pages or the production build path.

The asymmetric PageRank witness after 20 iterations from [.1,.2,.7] is approximately `[.45523782984952244, .05, .4947621701504773]`. Equal division of the first class total would instead give both members `.25261891492476124`, plainly wrong. Class-mass iteration plus original-target reconstruction returns the correct unequal scores within the stated floating error.

The displayed acceptance fractions reflect exhaustive tiny graph spaces, not expected customer refresh success rates. No percentage here estimates paid demand, speedup confidence or large-graph compression.

## Verification-First Experiments Still Required

| Experiment | Baselines and invariant | Reject or revise the idea when |
| --- | --- | --- |
| Certificate validation over external pages | Full BFS/WCC recompute; complete update batches; fresh labels equal old labels whenever accepted | Delta extraction is unreliable, joins cost as much as recompute, or reuse is rare on actual workload |
| Candidate-1 alternate witnesses | Parent-only certificate; dynamic repair and recompute; no accepted invalid result | Added witness storage/maintenance exceeds saved refresh work |
| Factored propagation | Existing compressed CSR plus ordinary 2D tiled execution; same numerical contract | Factoring build/refresh cost is not amortized, state traffic dominates, or sparse rows do not repeat |
| Similarity class execution | Tuned exact prefix join and GDS-compatible profile where available | Grouping does not reduce candidate work or output dominates all saved compute |
| Complete 4 GB lifecycle | Physical host/VM limit, no swap, bounded builder, cold and warm runs, slow output sink, retry, refresh | Any required phase escapes the budget or the job misses the agreed usefulness envelope |

Metrics must include first-answer time, repeated-query time, refresh time, successful exact jobs per day, logical and physical bytes, peak simultaneous disk, worker and whole-machine memory, queueing, output and source-server impact. Include a storage-layout-disabled ablation for each candidate; otherwise a win cannot be attributed to the proposed mechanism.

## Provisional Product Order

1. Deliver a bounded snapshot build plus useful dependency/reachability query; measure where preparation or export dominates.
2. Test certified answer inheritance for genuinely recurring roots/components. It addresses freshness and repeated cost without requiring a general streaming graph database.
3. Profile row repetition and feature-set duplication on real candidate workloads before implementing specialized compression. Let observed structure choose between factored propagation and class similarity.
4. Retain general exact external-memory fallbacks for nonredundant graphs; they establish usefulness when a specialization does not apply.

This ordering is a hypothesis, not a reduction of the full research goal. Remaining documents may supply stronger mechanisms or invalidate the priority. Final synthesis must reconcile all reader lanes, additional candidates, prior art and lifecycle feasibility before declaring the goal complete.

## Candidate 4: Preserve Incidence, Avoid Derived Cliques

Added after the full end-to-end workflow question and reading the July explanatory documents. Provisional mechanism refinement, not a global novelty claim. The earlier examples explicitly derive entity pairs from shared attributes; this alternative asks whether those pairs should ever be materialized for the selected algorithm.

### Customer Job And Admissible Input

Input is a binary incidence relation B between N entities and F attributes/groups. There are I true incidence entries. Attribute identity includes its domain and namespace; two unrelated fields with equal text are not automatically the same attribute. A projected simple entity graph connects distinct entities iff they share at least one eligible attribute. This is the standard [bipartite projection definition](https://networkx.org/documentation/networkx-2.5/reference/algorithms/generated/networkx.algorithms.bipartite.projection.projected_graph.html), not a new graph model.

The customer question is connectivity, unweighted entity-hop distance, or a specifically defined weighted ranking on that projection. It is not a general rule that connected entities are identical people or fraudulent. Thresholded overlap, direction, temporal conditions and arbitrary pair-specific predicates require separate derivations.

Instead of building every pair from a large group, store both incidence access orders:

1. entity -> eligible attributes;
2. attribute -> member entities.

The original user-facing graph can still be described as derived entity edges, while the execution representation preserves their source relation. This choice matters at build time: avoid producing and sorting a quadratic intermediate, rather than compressing it after it has already been created.

### Exact BFS And WCC Without Clique Construction

For BFS on the simple projected graph, process entity frontiers in nondecreasing distance. When an entity first exposes an unvisited attribute, mark that attribute visited and stream its member list once. Newly reached entities receive distance one greater than the entity that activated the attribute. Record that predecessor entity and attribute as a witness.

Why this is correct: every derived entity edge corresponds to a two-step incidence path. Conversely, alternating incidence paths produce entity paths by suppressing attribute vertices. Hence shortest incidence distance between entities is twice the projected unweighted distance. The first activation of an attribute occurs at its smallest reachable entity distance; later activations cannot improve any member's distance. They can be skipped for distances and one arbitrary valid witness, but not for every shortest witness or path count.

WCC can similarly union one representative member with the rest of each attribute list. Each group connects its members without creating all pairs. Components among entity vertices equal those of the derived projection. Empty and singleton attributes add no entity connections. Isolated entities must remain in the explicit entity input.

A bounded implementation still needs visited state, frontier storage and membership access. When they do not fit, external frontier/dedup runs and paged state add I/O. The in-memory logical bound O(N+F+I) does not automatically establish the same external-memory bound. One giant group can fill the output/frontier and must be streamed, not collected into a single vector.

### Exact Weighted Propagation Through Two Incidence Scans

A second supported contract sets the entity-edge weight to the number of shared attributes and omits self edges. This is a standard [weighted bipartite projection](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.bipartite.projection.weighted_projected_graph.html). It is NOT the unweighted projection when two entities share multiple attributes.

Define:

```text
A = B B^T - diag(q)       where q_i = number of attributes of entity i
d_i = sum over h containing i of (member_count(h) - 1)
y_i = x_i / d_i           when d_i > 0, otherwise 0
s_h = sum over i in h of y_i
z_i = sum over h containing i of (s_h - y_i)
x_next_i = (1-alpha)*p_i + alpha*z_i + alpha*dangling_mass*p_i
```

In real arithmetic, z is exactly the normalized weighted entity-edge propagation. Compute group sums with attribute-major incidence and return contributions with entity-major incidence. The diagonal subtraction removes each entity's self contribution once per incident attribute. Uniform or specified personalization and the chosen dangling policy complete the PageRank contract.

This avoids expanding B B^T and does not require identical entity rows. Arbitrary personalization is compatible with this specific fixed operator, unlike caching one finished global rank vector. The benefit depends on incidence volume being much smaller than the projected weighted adjacency, not on a promise that any graph has such a representation.

Fixed-iteration real-arithmetic equivalence is not GDS bitwise/stopping-rule parity. Floating-point cancellation in s_h-y_i, normalization, nonfinite values, weights and reductions require explicit numerical handling and validation. A scale-aware error policy must be designed before claiming production equivalence. Do not transfer the recurrence to a binary thresholded projection, general Jaccard weights, arbitrary edge deletions or nonlinear neighbor aggregations.

### Illustrative Size Model

Synthetic favorable shape: N=10,000,000 entities, F=10,000 disjoint groups of 1,000 entities, I=10,000,000 incidences. Every group induces a clique. Use u32 neighbor IDs and u64 offsets.

| Selected component | Expanded representation | Incidence representation |
| --- | ---: | ---: |
| Undirected logical entity edges | 4,995,000,000 | Not materialized |
| Stored entity adjacency entries | 9,990,000,000 | 20,000,000 incidence entries across both orders |
| Topology including offsets | 40,040,000,008 B | 160,080,016 B |
| Two f64 rank arrays, u64 weighted degrees and f64 group sums | Requires its own concrete baseline state | 240,080,000 B |

About 250x less topology in this intentionally favorable example is arithmetic, NOT a measured Neo4j RAM ratio or speedup. A competent baseline can also retain incidence or exploit clique structure. Keys, source properties, maps, build scratch, runtime, output and refresh storage are not included in these component sizes.

Even here, rank state does not shrink merely because topology does. For N=1 billion, the displayed 24N term alone exceeds 4 GB and requires an external-state or other proven representation. Nor can an arbitrary existing entity-edge export be factored cheaply: source incidence must be available, or the expense and exactness of discovering a factorization must be included.

### Build, Query, Output And Refresh

- Build: validate and externally deduplicate incidence according to the declared binary relation; preserve external entity IDs and attribute namespaces. Sort both access orders with a fixed merge budget. Compute member counts and per-entity weighted degrees without the pairwise expansion. All intermediate sort generations and mappings remain chargeable.
- Query: BFS may fetch only reached incidence lists, with seek/page overhead; PageRank logically processes incidence in two orientations per iteration, with scalar-state gathers. Neither is a universal sequential-only physical-I/O claim.
- Output: distances, component labels and one witness per entity remain O(N). Listing every derived edge or every path restores the enormous output. Return the exact requested contract or reject it explicitly, not a silently compressed substitute.
- Refresh: a single membership edit can alter the projected relationships of a large group, yet the stored incidence update is small. Nevertheless component or rank changes may be global, and weighted-degree normalization changes for every affected group member. Charge that expansion and recomputation; do not call it O(1) analytics refresh.
- Retention: immutable incidence generations and reader leases still consume old/new storage. Avoid global physical-ID reordering every refresh unless its rebuilt maps and indexes earn the cost.

### Prior Art And Novelty Boundary

Hypergraphs, incidence graphs, clique projections, factored matrix multiplication and algebraic random walks are established. [HyperBFS/HyperBC, 2013](https://doi.org/10.1016/j.socnet.2013.07.006) explicitly exploits hypergraph structure to avoid redundant computation. [Hypergraphs: connection and separation](https://arxiv.org/abs/1504.04274) studies connectivity through incidence representations. [Hypergraph Random Walks, Laplacians, and Clustering](https://arxiv.org/abs/2006.16377) develops incidence-based walk operators. Abstracts and selected public descriptions were checked, not all proofs or implementations.

The repository already proposes factorized incidence/hyperedges and WCC union without entity-edge materialization in [Sol-01](../docs_PRD04/Sol-01.md), lines 714-809, especially 794-798. Lines 1042-1056 also propose factor-native connectivity and alternating entity-feature recommendation propagation. Reader02 fully consumed this source and recorded the comparison in [its architecture evidence](02-prd04-architectures-Evidence.md). Avoiding clique construction, WCC factor union and generic two-stage propagation therefore are NOT new contributions of this study.

The narrower extension here is the specified posting-once BFS distance/witness contract, the self-loop-corrected weighted-projection PageRank recurrence, their restricted independent checks, and the full input/build/output/refresh accounting. These refine existing ideas rather than invent hypergraph algorithms. A future implementation needs a factor-native accessor: enumerating ordinary entity neighbors through every clique pair would lose the intended work reduction. Likewise, large-scale validation should certify the canonical incidence relation and operator semantics without expanding every derived edge merely to compute a graph checksum. Keep independent expanded-edge oracles for bounded fixtures.

### Executed Small Checks

Pure JavaScript enumeration on 2026-09-19 UTC, independent expanded-matrix/adjacency oracles, no persistent algorithm code or large-data benchmark:

| Check | Domain | Result |
| --- | --- | --- |
| Incidence BFS vs expanded simple projection | All 4,096 binary 4-entity/3-attribute incidence relations; all eight attribute filters and four roots | 131,072 root/filter cases; zero distance mismatches |
| Two-scan weighted PageRank vs expanded weighted recurrence | Same 4,096 relations; alpha 0.85, p=[0.1,0.2,0.3,0.4], initial x=[0.4,0.3,0.2,0.1], 30 fixed iterations | 122,880 compared iterations; maximum absolute discrepancy 3.34e-16 |

These checks include isolates, singleton attributes, overlapping groups and complete incidence. They do not establish arbitrary-weight, negative-weight, large-ID, production floating-bound, GDS or 4 GB implementation correctness. Next falsifiers: pair-specific filtering, thresholded overlap, adversarial degree skew, source without incidence provenance, output expansion, external sort/cold I/O and changed-membership refresh.

## Candidate 5: Eliminate Entity Rank State

The subsequent [Feature State Replaces Entity State](Feature-State-PageRank.md) derives an additional stationary PageRank option for Candidate4's exact shared-feature-count projection. It expresses each entity rank through feature values, solves only feature-sized mutable state without materializing a dense feature matrix, and reconstructs entity scores in a stream. It also derives a feature-sized bound on the original entity residual. This addresses the remaining rank-vector limitation above when F is much smaller than N, without requiring identical entity rows.

That document contains the full graph/dangling/numerical contract, algebra, convergence argument, storage/build/output/refresh accounting, prior-art limitations, a favorable billion-entity size model and 36,864 small stationary comparisons. Its modeled two-vector payload is 16 MB at F=1M versus 16 GB at N=1B, not a measured whole-Neo4j RAM ratio. The source incidence, repeated scan cost, numeric certification and full result delivery remain essential prerequisites. A stationary solve is not the same as returning GDS's fixed-iteration trajectory.
