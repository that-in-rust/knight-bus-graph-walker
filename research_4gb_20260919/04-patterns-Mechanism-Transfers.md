# Reference-Pattern Mechanism Transfers

Date: 2026-09-19. Lane: 04-patterns. Status: provisional research, not implemented or benchmarked.

## Decision Summary

Investigate four narrow extensions, not another renamed CSR architecture. H1 is a shared ownership/accounting prerequisite. H2 is the best first exact algorithm experiment. H3 serves join-heavy investigation queries. H4 is explicitly tolerance-based ranking reuse, not exact fixed-iteration compatibility.

| Hypothesis | Proposed product use | Narrow differentiation to test | Principal risk |
| --- | --- | --- | --- |
| H1 Retain Physical Allocation Leases | Complete a job despite a slow client, cancellation, or refresh overlap | Reservations follow allocation ownership through slices, FFI, results, and generation retention; admission includes progress resources | Accounting overhead or unmanaged allocations defeat the guarantee; boundedness alone can deadlock |
| H2 Certify Unchanged Shortest Paths | Repeated distance/one-witness dependency impact reports after small edits | Revalidate an existing answer without recomputation when a sufficient certificate survives | Tree changes reject many harmless batches; gate I/O can exceed recomputation |
| H3 Seal Boxed Factorized Queries | Typed/filterable fixed-length dependency motifs and triangle/common-neighbor analysis | Snapshot-complete masks, memory-bounded join boxes, factorized results, and byte-credit export form one execution contract | Index construction and unavoidable output dominate; generic Cypher claims would be false |
| H4 Certify Unchanged Ranking Answers | Repeated risk-priority ranking across snapshots under an explicit error tolerance | A conservative change certificate accepts an old vector before touching every edge | Bound is too loose, updates touch hubs, or customer needs incompatible PageRank semantics |

The accompanying [evidence ledger](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_4gb_20260919/04-patterns-Evidence.md) records 37/37 documents fully covered: 32 distinct streams and five hash-identical copies, with no unread reference spans. These are candidates not observed in the bounded main-design spans read by this lane; absence from the entire repo is NOT established. Integration with other lanes remains necessary.

## Baseline And Novelty Boundaries

The main A007 atlas already contains proof blocks, fixed-state capsules, answer artifacts, differential freshness, PageRank residual/precision ideas, deterministic random-walk streams, and compressed triangle intersections. Their exact consumed spans are in the evidence ledger. This study cannot count those as new.

The historical references already propose dense IDs, CSR/CSC, masks, adaptive sparse/dense frontiers, vectorized batches, external sorting, shards, tiled graph computation, delta-only updates, snapshot-valid caches, and split supernode adjacency. [GridGraph](https://www.usenix.org/conference/atc15/technical-session/presentation/zhu), for example, published partitioned out-of-core graph processing in 2015. Better composition or enforcement may be useful engineering, but does not make these ingredients inventions.

The now-complete canonical storage reference also proposes KV delta plus CSR checkpoint explicitly (298-415) and old-reader retention controls (1692-1700), further narrowing H1's differentiation. Its endpoint-only pseudo-optimization (435-490) requires semantic repair: not projecting path objects does not authorize collapsing duplicate endpoint matches or ignoring relationship uniqueness. Likewise its src/type/dst key shape (213-227) needs edge identity or another multiplicity representation for a multigraph. These are hazards to reject, not mechanisms to transfer literally.

Full-corpus correction: `supermeta-graph-database-patterns-2.md:1272-1387` already proposes a global reservation/spill manager. Supplement 01 lines 360-418 already tracks allocator commitment/decommit/purge and non-rollback commitments; the Rust/storage supplement lines 1954-2004 already carries OpenDAL permits through reader drop. H1 must therefore test specific ownership gaps, not claim a new memory manager. `supermeta-graph-database-patterns-1.md:1975-2023` already combines typed segmented posting-list sidecars, WAL deltas and schema/index contracts. That combination is not a new replacement architecture either.

GDS is not universally an object-per-edge graph. Its [pinned packed-adjacency source](https://github.com/neo4j/graph-data-science/blob/dc4417b3c1feffbdf9eb6293fbbd1ffb1b7233b9/core/src/main/java/org/neo4j/gds/core/compression/packed/AdjacencyPacking.java#L24-L47) has generated 64-value, width-selected packing. Its [official estimator](https://neo4j.com/docs/graph-data-science/current/common-usage/memory-estimation/) already estimates projection and algorithm memory. The useful comparison is actual end-to-end memory, build time, supported semantics and completed outputs, not Java versus Rust or presence versus absence of estimation. This lane has not established which compression backend every GDS configuration selects.

## H1 Retain Physical Allocation Leases

**Transfer source.** Reference `graph-database-patterns-3.md:54-180,460-518,830-870,1096-1130`; `supermeta-graph-database-patterns-3.md:455-481,755-777`; supplement 05 lines 620-676. Together these expose shared buffers, reservations that are not allocators, pinned pages, results outliving queries, FFI owners, and retry deadlocks.

**Hypothesis.** Attach a lease to each owned backing allocation or managed resident extent, not to its current operator or logical slice length. A slice, output collector, or FFI export carries a reference to that lease. Only release of the final relevant owner releases its charge. Registered shared extents are counted once globally even when several jobs use them. A result that outlives its query transfers to an output owner with its existing charge; destroying the query is not evidence that the result memory disappeared.

For each allocation, charge capacity plus allocator overhead, not populated rows. Managed mappings need a separate resident/pin policy: reserving virtual address space does not reserve physical memory, and unpinned dirty or cached pages do not vanish immediately. Track physical resident pages at the process/container boundary as an independent validation, with no double counting of file-backed RSS and the same page-cache pages.

Final-owner release may return storage to an allocator arena rather than the OS. Transfer that retained extent to the allocator/pool charge until it is reused or actually reclaimed; do not infer an RSS reduction from a destructor. Coarse slab ownership can make this accounting tractable. An OS memory limit is a last boundary, not proof that a job will complete before enforcement kills it.

When a small retained slice is the last reason a large backing allocation remains live, detaching it into a small allocation may lower subsequent memory. First reserve both old and new bytes for the copy interval. If other slices still retain the parent, that immediate saving is absent. Copy-on-retain is an adaptive policy to test, not an unconditional optimization.

**Safety and progress obligations.** At time t, all known live extents must be represented in the union of charged owners; every allocation request must fit before it occurs. A hard worker limit still needs conservative headroom for stacks, allocators, runtime and unmanaged libraries. Reserve an emergency spill/flush buffer and disk quota before admitting a spillable job. Never invoke a spill path that recursively waits for the same exhausted pool. Shared read leases may coexist; a pin does not imply exclusive mutation. Cancellation must release or explicitly transfer ownership, not invalidate buffers still owned by a client.

Two-resource admission is essential: a slow output can pin old sealed files while a refresh creates new ones, even when RAM is flat. Refuse new refresh admission or expire a documented output lease before exhausting retained-generation disk; do not silently remove an active snapshot.

**Cost and value.** Extra costs include a lease record per managed allocation/extent, reference operations, a bounded ownership registry, occasional detach copies, and a centralized admission decision. Use coarse bounded slabs, not one lease per graph edge. This is useful only if it enables completed export/refresh jobs without large safety margins. It cannot make unbounded output instantaneous or guarantee progress under a client that never reads.

**Prior art and residual claim.** Arrow ownership and buffer slicing are established. [Comet's official memory guide](https://datafusion.apache.org/comet/contributor-guide/memory_management.html) documents the specific gap between reservations, FFI-exported buffer lifetime, and container memory; a slow consumer can retain bytes after a producer releases its reservation. [Differential Dataflow trace management](https://timelydataflow.github.io/differential-dataflow/chapter_5/chapter_5_3.html) already treats retained readers and compaction frontiers as linked. The candidate is an end-to-end RAM/disk lifetime protocol to test for this graph pipeline, not ownership tracking itself.

**First falsification experiment.** Produce many small results that retain separate large slabs; hold them across cancellation, FFI release, and two refreshes. Compare operator-local reservations, lease accounting, and lease accounting plus detachment. Require no premature release, no double charge, bounded physical high-water mark, eventual reclamation after the last owner, and completion under a finite slow-consumer schedule. A single successful run cannot prove all allocator paths are covered. Kill or redesign if registry/copy costs erase the usable memory gain or any allocation path escapes the budget.

## H2 Certify Unchanged Shortest Paths

**Transfer source.** Existing answer-artifact and freshness designs in A007 lines 524-578 and 1820-1853, combined with supplement 05 lines 207-264 and 678-718: classify deltas before loading the full base, and validate caches at a specific snapshot. This is a sufficient no-change gate, not a proposed universal dynamic shortest-path algorithm.

**Exact contract.** Fixed vertex universe and root; directed eligible edges under fixed type/property filters; nonnegative integer weights with checked wide arithmetic; stable parallel-edge identities. Output is exact distances and optionally one valid selected shortest-path witness. It is NOT all paths, all shortest paths, path counts, a particular lexicographic tie winner, or a generic variable-length Cypher result.

An initial validated answer stores d(root)=0, unreachable markers, and a rooted acyclic predecessor tree T whose edges realize each finite distance. All eligible edges u->v obey d(v) <= d(u)+w(u,v), with infinity handled symbolically rather than by overflowing a sentinel.

For a committed edit batch, accept the old answer only if:

1. Every old tree edge remains eligible with the same weight and endpoints in the final snapshot. Test changed edges against an edge-ID membership index for T.
2. Every inserted or decreased-weight eligible edge satisfies the old-label inequality. An edge from finite d(u) to infinite d(v) fails immediately. Normalize endpoint rewiring, type changes and eligibility changes into removal of the old eligible edge and insertion of the new one, even if the edge ID or numerical weight is unchanged.
3. The root, vertex set, direction, filtering interpretation, weight policy, and output contract remain unchanged. The delta is complete and belongs to the exact predecessor/successor generations.

**Correctness argument.** Unchanged edges retain their inequalities. Deleting edges removes constraints; increasing a non-tree weight cannot violate its inequality. Checked new/decreased edges establish inequalities for the rest. Summing inequalities along any new root-to-v path makes d(v) a lower bound on that path's length. The preserved tree supplies a path of exactly d(v), so the bound is attained. No newly reachable vertex exists: a first edge from an old-reachable to an old-unreachable vertex would have failed the gate. The old rooted tree remains acyclic even with zero-weight edges. This establishes exact distances and the old witness, not tie-set or multiplicity stability.

**Resource accounting.** With 64-bit distances and 64-bit predecessor edge IDs, the persistent answer costs at least 16V bytes plus validity markers and metadata. Fast delta checks also need a tree edge membership index: for example, a packed sorted 64-bit edge-ID set costs about 8R bytes for R reachable nonroot vertices, plus block metadata. Avoid claiming O(delta) work while hiding an O(V) tree scan. If these arrays/indexes are not resident, sorted delta lookups or cached page access add explicit random/sequential I/O. Initial solve and certificate validation scan the eligible graph and validate rootedness; their cost is amortized, not erased.

A passing batch costs changed-edge validation, d lookups, tree membership checks, durable metadata publication, and requested output. It does not imply O(1) full-distance export: emitting V distances still costs Omega(V). A failing batch falls back to a correct memory-bounded solver. No local-repair guarantee is assumed. High-level label edits may generate a very large edge delta and must be charged accordingly.

**Prior art and residual claim.** Dynamic shortest paths are decades old; [Ramalingam's 1996 book](https://link.springer.com/book/10.1007/BFb0028290) includes incremental shortest-path algorithms, and [Ramalingam and Reps' primary report](https://ftp.cs.wisc.edu/pub/techreports/1992/TR1087.pdf) addresses mixed graph changes. Bellman inequalities plus a realizing tree are a standard certificate idea. The proposed repo extension is a conservative, storage-aware acceptance gate for persisted answer artifacts. No global novelty is claimed.

**First falsification experiment.** Enumerate small directed multigraphs and mixed edit batches; compare every accepted result with independent full recomputation. Include zero weights, parallel edges, equal-cost new alternatives, unreachable components, tree deletion/reinsertion, changed filters, and integer overflow boundaries. Tie-sensitive/all-shortest requests must refuse this contract even when distances match. Then measure pass rate and actual pages touched on repeated dependency snapshots. Abandon the fast-path business case if gate plus export costs do not beat full bounded recomputation after including initial certificate/index construction.

## H3 Seal Boxed Factorized Queries

**Transfer source.** Kuzu additions in `meta-graph-database-patterns-2.md:1952-2246`; query physical properties and exact/inexact pushdown in supermeta 3 lines 318-381; result lifetime and recursion contracts at 755-826; GraphBLAS masks in canonical 4 lines 618-760. Factorization, a mask, and a memory limit are separately insufficient to preserve full query semantics.

**Hypothesis.** For a deliberately narrow inner-join graph query fragment, execute disjoint lexicographic boxes over sorted edge/property indexes. Keep common bindings factorized until an operator truly requires expansion. A query-specific node/key mask becomes an exclusion predicate only after all producers for that snapshot and mask scope have completed. Until then it may guide scheduling, never prove absence. Publish a sealed mask together with its generation, predicate semantics, scope, and completeness frontier.

A box partition must cover the eligible binding domain exactly once, with a canonical owning box for every output tuple. Split oversize boxes and oversize child lists without losing the join prefix or multiplicity. Bound the union of loaded index windows, mask state, factor groups, join cursors, decompression buffers, and output queue bytes. Memory ownership follows H1, so a small factor cannot hide a giant retained parent buffer.

**Correctness boundary.** Start with fixed-length typed/filterable conjunctive patterns and explicit set/bag output. Preserve relationship IDs when parallel edges or relationship-unique trails matter. A reached-node bitmap is not a path collection. An exact existence query may stop after a witness; COUNT must preserve multiplicity and check numeric overflow; DISTINCT requires correct external deduplication; explicit path enumeration must emit every requested witness. Factorization alone does not make COUNT easy for every constraint.

Only apply sealed masks as conservative semijoin filters where the algebra proves safety. Nulls, optional matches, antijoins, correlated predicates and nondeterministic expressions need separate rules or exclusion from the first supported fragment. Inexact index pushdown retains its residual predicate. Returning a useful unsupported-semantics error is better than silently treating a path bag as a reached set, but product validation must include a nontrivial set of completed supported jobs.

**Build, query and refresh costs.** Several trie/index orders may be needed. Charge each index's sort passes and sealed bytes; do not assume a single CSR services all variable orders efficiently. Byte-credit export retains a bounded queue, with a cursor tied to the immutable snapshot. If one logical record exceeds the budget, a protocol-compatible streamed/spooled encoding is required; otherwise fail explicitly before promising success. A disconnected/slow reader pins a generation until its documented lease expires. Refresh creates new indexes/masks and never combines an old complete mask with new graph edges.

Input can be reread across boxes; track that amplification, not merely the largest box. Dense graph output can be enormous, and ordering may require another external sort. Neither worst-case-optimal joining nor factorization removes Omega(output bytes). A bitmap for V IDs costs approximately V/8 bytes per independent complete mask before metadata; many masks or worker-local copies can dominate.

**Prior art and residual claim.** [Leapfrog Triejoin](https://arxiv.org/abs/1210.0481) and [Zinn's 2015 out-of-core boxing](https://arxiv.org/abs/1501.06689) preclude claiming invention of bounded joins. [Kuzu's primary system paper](https://www.vldb.org/cidrdb/2023/kuzu-graph-database-management-system.html) already combines factorization with binary and multiway joins and discusses scan tradeoffs. The candidate is the explicit integration of snapshot-complete pruning, factorized bags, per-box memory ownership and resumable byte-bounded export. Even that combination may already exist elsewhere.

**First falsification experiment.** Exhaustively compare fixed-length motifs on small typed multigraphs with a simple tuple-enumerating oracle, including parallel relationships and filters that remove one of several paths. Delay one mask producer: no missing match may be pruned. Test exact box boundary ownership, one high-degree vertex, huge child lists, slow output and refresh overlap. Compare boxed LFTJ and Kuzu-compatible semantics as well as eager expansion. Reject the composition if masks/factorization overhead or repeated index reads erase its benefit; the measured whole job must include all requested rows or clearly select an aggregate workload.

## H4 Certify Unchanged Ranking Answers

**Transfer source.** Canonical 4 lines 418-489 covers PageRank parameters, sink behavior and traversal direction. A007 already proposes PageRank residual/precision ideas at 748-837 and answer freshness at 524-578. The extension to test is a cheap, conservative cross-generation reuse gate, not PageRank or residual stopping itself.

**Contract.** Fixed vertex set, damping 0 <= a < 1, personalization p, dangling-node rule, eligible-edge/weight policy, and desired L1 error epsilon. Let P be column-stochastic, including the agreed dangling correction, so x* = (1-a)p + a P x*. x is a persisted approximate vector with a certified bound beta >= ||(1-a)p + a P x - x||1. This is not necessarily GDS's default normalization, stopping criterion, or fixed-iteration result; establish the requested contract before comparison.

The standard residual bound is ||x-x*||1 <= beta/(1-a); [Gleich, PageRank Beyond the Web, Aside 3](https://arxiv.org/html/1407.5107v1) explicitly derives it. For the proposed refresh gate, keep x unchanged and let the new matrix be P'. Then a sufficient updated residual bound is:

```text
beta' = beta + a * sum over changed source columns j
                  (abs(x[j]) * norm1(P'[:,j] - P[:,j]))
             + certified numerical-rounding allowance
accept unchanged x only when beta' / (1-a) <= epsilon
```

**Why safe, and why possibly useless.** Substitute P' into the residual and apply the triangle inequality. This may greatly overestimate change, but cannot underestimate it if inputs and numerical bounds are valid. It uses a scalar old bound rather than another V-element residual vector. A coarse upper bound of two for the L1 difference of any two stochastic columns provides an O(number of changed sources) arithmetic gate once completeness of the affected-source set is established. Refine selected columns by scanning their old/new normalized adjacency only when the coarse gate fails.

Changing one outgoing edge alters normalization of the entire source column; exact refinement costs the old/new outdegree, not the number of edited edges. A hub update can touch most edges. A changed dangling column may involve dense personalization: use a proven aggregate bound rather than materializing it. New vertices or changed personalization invalidate this first gate's fixed-universe contract. Repeated scalar-bound accumulation can become too loose and force full refresh even when actual scores barely move.

This derivation is exact-arithmetic reasoning. A production certificate must use rigorous outward bounds for weights, normalization, stored scores and reductions, or conservatively reject uncertain cases. A convenient floating-point residual is not automatically a proof. Under one common error bound e, top-k membership is certified only if the kth and (k+1)th persisted scores have gap greater than 2e; ties or a smaller gap require more work. Full vector export still costs Omega(V).

**Costs.** Persist x (8V bytes for f64), beta, parameters, generation and normalization metadata. Collect changed sources from the complete committed delta. The coarse gate reads their scores and retains only bounded delta batches plus a scalar bound; random score reads still cost pages. Tighter checks read old/new adjacency and require both generations retained on disk. Failure invokes tiled iteration with the required incoming representation or charged alternative passes. Initial solving, the initial certified residual pass, optional transpose construction and all subsequent rejected gates remain in the lifecycle bill.

**Prior art and residual claim.** [Dynamic approximate personalized PageRank](https://arxiv.org/abs/1603.07796) and [dynamic PageRank algorithms/lower bounds](https://arxiv.org/abs/2404.16267) rule out novelty claims for incremental ranking and warn against universal cheap-update promises. Their guarantees use particular models and error definitions, not an automatic bound for this proposal. The repo candidate is the cheap-to-refine no-recompute gate under a declared numerical contract.

**First falsification experiment.** For tiny rational-weight graphs, compare accepted old vectors with high-precision/rational solutions after mixed edge changes, including dangling transitions, parallel weights, disconnected components and a close-to-one damping factor. Force rounding boundaries and repeated accepted batches. Require zero false acceptance against the stated error bound and no unjustified top-k claims. Then compare coarse gate, refined gate, warm-start full solve and cold full solve, counting rejected gates. Kill the claimed fast-refresh value if realistic changes consume the residual margin immediately or users require incompatible exact outputs.

## Complete Lifecycle Accounting

All quantities below are analytical envelopes or required measurements, not benchmark results. Use decimal bytes. Let the physical machine budget be H=4,000,000,000 and provisional worker budget W=3,000,000,000. The remaining billion is a hypothesis about OS/runtime coexistence, not a measured reserve. Use a genuinely constrained deployment for experiments; a heap-only limit on a larger workstation is not the same experiment.

At every time t, enforce and measure:

```text
worker(t) = unique live charged backing extents
          + managed resident/pinned file pages not already counted
          + stacks, allocator fragmentation, runtime and untracked allowance
worker(t) <= W
whole-machine physical working set(t) <= H
```

Do not independently give each concurrent job W. Share cached extents once; add each job's private scratch, results and unshared decode buffers. Include maintenance and refresh workers. If the fixed vectors alone do not fit, a fit-in-RAM plan is unavailable: use a demonstrably bounded external-state algorithm with measured additional passes, or select a different supported workload.

| Phase | RAM that must be charged | Disk, I/O and correctness obligations |
| --- | --- | --- |
| Extract/validate | Parser batches, network/decompression buffers, bounded rejects and queues | Retained source input, source snapshot/bookmark, checkpoint and restart semantics; malformed IDs/weights, nulls, duplicates and filter eligibility validated |
| ID mapping | Sort run buffer, bounded merge fan-in, map lookup windows | External-ID dictionary, reverse map, sorted endpoint joins; random IDs cannot assume dense identity; index construction and remapping passes count |
| Normalize/build | Edge run buffer, K merge buffers, heap/cursors, degree/offset pages, compression scratch | Sort by required orders; preserve edge IDs when needed; do not deduplicate parallel edges without permission; sealed adjacency, weights, properties, transpose and auxiliary indexes all count |
| Publish/recover | Manifest/WAL and checksum buffers | Flush and validate data before atomically publishing the new root; retained old root plus new root and scratch coexist; failed/retried builds need bounded orphan cleanup |
| Query | Unique graph windows, masks/vectors/queues, per-worker scratch, library workspaces | Cold reads versus warm cache; external frontier, join boxes or iterative state spills; exact semantics or explicit accepted tolerance |
| Export | Retained buffers, byte-bounded serializer/connection queues, optional detach overlap | External-ID conversion, requested sorting, full output or explicit aggregate, temporary output spool, slow-reader generation retention |
| Refresh | Delta validation, old/new lookup windows, compaction buffers, running-query state | Complete change set, old and new generation/index retention, update certificates, fallback recomputation and garbage collection after readers release |
| Concurrency/retry | Shared global pool plus private job/maintenance overhead | Reserve disk as well as RAM; avoid retry-slot and spill-allocation deadlocks; charge failed attempts in completed-job cost |

### Extraction Is Part Of The Contract

CSV ingestion needs a declared dialect, encoding, null/missing policy, numeric range, relationship identity and source-version manifest. Chunk boundaries may split quoted records or UTF-8 sequences. A bounded parser must stream/spool oversized fields or reject them explicitly; bounding row count alone cannot bound bytes. External sort and endpoint joins must validate orphan endpoints and preserve duplicate relationships unless the requested semantics authorize coalescing. Rejected records are bounded/spooled with counts, not accumulated in RAM.

For Neo4j extraction, record the selected stable IDs, labels/types, properties, direction, weights, and consistency method. A bookmark is a causal lower bound, not proof that separately paged node and edge exports share a snapshot. Use a verified stable export/snapshot or a quiesced source; otherwise declare the weaker semantics and refuse exact refresh certificates. Driver fetch size bounds records, not record bytes; eager list/DataFrame APIs can defeat bounded extraction. Retryable transactions writing to an external spool require attempt-isolated publication or an idempotent sink, and complete retry bytes/time count. A long export transaction may retain source versions/logs: if Neo4j co-resides on the 4 GB host, its heap, page cache, native buffers and retained disk also count. A remote Neo4j source is a separate deployment assumption, not evidence that the whole workflow fits on one machine.

Refresh requires a complete, ordered committed change interval tied to the exported generation, including deletions and eligibility-affecting property changes. Without that evidence, rebuild from a verified snapshot; an incomplete delta cannot justify H2/H4 acceptance. Checkpoint receipts must not advance beyond durable files. CSV replacement has full replacement cost unless a trustworthy delta exists.

### A Concrete Lower Bound

For V=10,000,000 and E=100,000,000 directed arcs, an uncompressed CSR with 64-bit offsets and 32-bit neighbor IDs uses 8(V+1)+4E = 480,000,008 bytes for topology alone. Two directions consume 960,000,016 bytes. A 64-bit dense-to-external map adds 80,000,000; a packed sorted external-to-dense map with 64-bit external keys and 32-bit values adds 120,000,000 before its index/format overhead. Two f64 rank vectors add 160,000,000. These selected components total 1,320,000,016, leaving 1,679,999,984 of the provisional W for everything omitted, not demonstrating a measured fit.

One f64 edge weight per arc adds 800,000,000 bytes; duplicating it with the reverse representation doubles that increment, while sharing weights requires an edge correspondence mechanism with its own bytes/I/O. H2's 16V answer adds 160,000,000 plus membership index/validity bits. Output, properties, optional labels, masks, offsets during build, sort buffers, allocator overhead and OS caches are not free. Use separate typed columns to justify packed map widths rather than relying on a padded language struct. For V=100,000,000 and E=1,000,000,000, just one such CSR topology is 4,800,000,008 bytes and cannot fit physically in H.

### Disk And Time Are Not Hidden Budgets

Let I be retained raw input, S normalized sort records, Aold/Anew sealed artifacts including required indexes, Rold retained older generations, D delta/WAL, and O output spool. A conservative external merge interval may retain one input-run generation and one output-run generation, approximately 2S, plus I and any required maps. Exact coexistence depends on the selected pipeline and crash recovery policy. Compute a time-indexed live-file maximum; do not claim sum-of-final-files is peak disk, or charge mutually exclusive phases as necessarily simultaneous.

During refresh, a relevant peak is Aold+Anew+Rold+D+active scratch+O+retained input/maps not already included. With a 50,000,000,000-byte persisted-storage scenario, this entire live set and free-space reserve must fit. A 50 GB raw input is a different scenario and may already exhaust that cap. If it does not fit, redesign run/generation retention or narrow the workload; do not assume scratch on an uncounted second drive. For illustration only, two 18 GB sealed generations leave 14 GB for input, maps not already inside the generations, delta, scratch, output and reserve. Two 30 GB generations cannot coexist under a 50 GB disk cap even with zero scratch. These are arithmetic constraints, not measured compression ratios or selected capacities.

For run budget B and merge fan-in K>1, the initial run count is roughly ceil(S/B), and merge levels roughly ceil(log_K(run count)); each full level reads and writes S before effects of compression/deduplication. Fan-in consumes K input buffers plus output and cursor state. Repeated endpoint remapping and additional index orders require their own sort/join passes. These are planning bounds, not disk-throughput predictions.

```text
Tcompleted = Textract + Tvalidate + Tmap + Tbuild + Tpublish
           + Tquery + Tserialize + Trefresh/recovery/retry attributable to job
```

The sum above is a serialized-plan accounting model. A pipelined plan must report actual elapsed critical-path time and phase overlap rather than double-counting overlapping durations; all bytes and CPU work remain chargeable. Record first-result and final-result times separately. Kernel speedup alone cannot establish Tcompleted improvement. For extra one-time build cost Btime and positive per-query saving s, a simple break-even estimate is ceil(Btime/s) queries before accounting for refresh; if s<=0 there is no such break-even. Include failed refresh gates in s and do not divide by a presumed endless sequence of warm queries.

## Verification-First Program

No experiment below has been run by this lane. These are proposed executable acceptance contracts, not reported results.

| ID | WHEN | THEN the experiment SHALL establish |
| --- | --- | --- |
| PAT-01 | H1 buffers cross slice/FFI/output owners during cancellation and refresh | The accounting charge remains until the final owner releases; no double charge; measured host/worker peaks stay within the explicit limits; the finite consumer schedule completes |
| PAT-02 | H2 accepts any enumerated small-graph edit batch | Distances and selected valid witnesses equal the declared exact contract under independent recomputation; tie-set requests are not silently accepted |
| PAT-03 | H3 boxes or masks split a fixed-length multigraph join | Output bags/sets match the oracle; delayed mask producers cannot remove valid tuples; all emitted tuples have exactly one box owner |
| PAT-04 | H4 accepts reuse after any tested update | High-precision oracle error is <= epsilon, and top-k certification only occurs with the required gap; numerical uncertainty cannot become acceptance |
| PAT-05 | A build/refresh/export crashes or runs out of allocated disk | Recovery exposes only a validated published generation; retries and orphan cleanup stay within the same quotas; active readers remain consistent |
| PAT-06 | Two jobs plus maintenance run in the physical 4 GB scenario | Shared accounting does not multiply the budget, and representative useful jobs complete without silent truncation, swapping away an unreported working set, or replacing exactness with approximation |

Start with PAT-02 correctness enumeration and PAT-01 ownership/liveness instrumentation. These are smaller, more decisive tests than building a broad new engine. Next test PAT-03's restricted query semantics and PAT-04's bound tightness. Only then benchmark larger graphs.

Benchmark matrix: sparse chains/trees; power-law graphs with one dominant hub; dense motif-rich partitions; many isolated vertices; random external IDs; skewed string properties; duplicate/parallel edges; weighted dangling transitions; growing output; changed-source versus changed-edge skew. Use graph sizes that fit and exceed W. Report useful jobs completed, error/tolerance, requested output bytes, p50/p95 completion latency, CPU time, bytes read/written, peak physical RAM, peak live disk, build/refresh time, and cold/warm state. Pin versions and query semantics.

Compare the actual GDS projection and estimator, a straightforward packed CSR/CSC baseline, and the relevant external-memory/join baseline. For H2 compare full exact recomputation; for H4 compare warm-start iteration as well as cold solve; for H3 compare compatible boxed join/factorized execution. Include import and ID mapping whenever the product pitch claims replacement of a complete Neo4j workflow. State separately when the customer already owns a reusable prepared snapshot.

The product hypothesis is a local analytical sidecar for selected dependency/security questions, not an immediate replacement for Neo4j transactions, the whole Cypher language, distributed service availability, or every GDS algorithm. Switching costs include reliable export, stable IDs, freshness semantics, output integration, accepted numerical tolerance and trust in repeated completed jobs. Validate those with the main product lane; this source review does not establish customer demand.

## External Verification Depth

Primary documentation/abstracts and selected mathematical passages were inspected on 2026-09-19. These are targeted prior-art checks, not full-paper reviews. The primary PageRank residual derivation was inspected in the HTML's lines 124-148. Comet's accounting and FFI lifetime sections were inspected through line 184 in the current render. The scanned Ramalingam/Reps PDF was located, but its complete proof was not read; the report abstract/search extract and publisher book contents establish prior existence, not equivalence to H2. One guessed Microsoft publication URL returned 404 and is not used as evidence.

This document distinguishes source-established ingredients, explicit deductions above, and untested engineering hypotheses. All assigned reference spans are now consumed; other lanes, the separately assigned historical raw captures and uninspected upstream literature can still invalidate repo-wide or broader novelty. No benchmark or algorithm implementation was performed.
