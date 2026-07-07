# Supermeta Graph Database Patterns 4: Algorithms, Sparse Layouts, and Memory-Efficient Traversal

Agent 4 scope: graph algorithms, HPC traversal, sparse linear algebra, CSR/CSC, frontier management, benchmark datasets, and memory-efficient graph processing for a Neo4j-like graph database rewrite in Rust.

This file is intentionally repo-grounded. Graph-intelligence tools were used only as navigation aids. Claims below are based on direct source reads from the referenced repositories under `gitrefrepo/`.

## Executive Takeaways For Neo4j-In-Rust

1. Keep the OLTP/property graph and the analytics traversal graph as separate representations. The high-performance repos repeatedly converge on immutable or append-built CSR/CSC snapshots for analytics, not pointer-heavy mutable adjacency objects.

2. Use dense internal vertex ids. cuGraph, ThunderRW, MiniGraph, GraphBLAS Graphalytics, and most CSR implementations assume vertices are renumbered into `0..n`. A Rust rewrite should treat public Neo4j ids as external ids and maintain a compact `NodeId32`/`NodeId64` map for algorithm snapshots.

3. Build both outgoing CSR and incoming CSC only when the workload justifies it. BFS bottom-up, PageRank pull, WCC, and SSSP variants often need incoming adjacency. But inverse adjacency can double edge-index memory, so expose it as a snapshot option.

4. Frontiers are not just queues. GAPBS switches between a `SlidingQueue` and bitmap frontier for direction-optimizing BFS; Ligra exposes `vertexSubset`; MiniGraph/GraphScope use active maps; LAGraph expresses frontiers as sparse vectors/masks. A Rust database should model frontier representation explicitly.

5. Canonicalization belongs in the loader/snapshot builder, not inside algorithms. GAPBS squishes sorted edge lists; sparsetools and sprs distinguish triplets from canonical CSR/CSC; petgraph CSR keeps per-row columns sorted. Algorithms should assume sorted/unique adjacency unless they explicitly support multigraph semantics.

6. GraphBLAS is a serious optional execution backend, not just a library curiosity. LAGraph, SuiteSparse:GraphBLAS, python-graphblas, and LDBC Graphalytics GraphBLAS all show BFS, PageRank, SSSP, and connected components as sparse matrix/vector operations with masks, semirings, and sparse result vectors.

7. Batch traversal and timestamp batching matter as much as raw algorithm code. Timely warns that per-record timestamps hurt progress tracking; Differential Dataflow circulates differences until fixed point; Raphtory benchmarks cached, windowed, materialized, layered, and persistent graph views.

8. External-memory processing is an architecture, not a spill flag. GridGraph, GraphChi, MiniGraph, and ThunderRW show file-backed vectors, shards, partitions, mmap/pread, O_DIRECT, and active-shard scheduling. If Neo4j-in-Rust must handle graphs larger than RAM, design shard APIs up front.

9. GPU paths favor large homogeneous analytics with renumbered CSR/CSC, not general transactional graph queries. cuGraph and Gunrock expose CSR/CSC/COO views, GPU buffers, renumbering, and load-balance options. Use GPU as an analytics accelerator with explicit transfer costs.

10. Benchmark oracles need both correctness fixtures and resource accounting. LDBC Graphalytics validates BFS, PageRank, WCC, etc. against expected outputs; GAPBS includes verifiers; Gunrock records DRAM/L1/L2 metrics; ThunderRW prints CSR memory cost. Memory accounting should be a first-class test target.

11. Prefer arrays over per-edge objects in hot paths. The repeated shape is `offsets`, `indices`, optional `weights`, and per-vertex arrays for labels/distances/scores. Properties can live in separate typed columns keyed by dense ids.

12. Do not hide allocation in algorithm APIs. Rust APIs should ask callers for workspaces/frontiers where practical, and return memory estimates before constructing snapshots or launching jobs.

## Pattern 1: Immutable CSR Core With Optional Inverse CSC

### Where Found

- `gapbs-src/src/graph.h`: `CSRGraph` stores `out_index_`, `out_neighbors_`, and optionally `in_index_`, `in_neighbors_`; constructors distinguish directed graphs from undirected graphs and control inverse construction through `MakeInverse` (lines 17-26, 98-150, 205-220).
- `petgraph-src/crates/petgraph/src/csr.rs`: CSR adjacency graph stores `row`, `column`, `edges`, `node_weights`, and `edge_count`, with fast outgoing neighbor slices and O(V + E) memory (lines 50-79, 393-430, 638-710).
- `sprs-src/sprs/src/sparse.rs`: compressed matrices are parameterized as CSR or CSC with `indptr`, `indices`, and `data`; row views are compatible with `CsMat` without copying (lines 14-29, 42-80, 86-207).
- `sparsetools-src/src/csr/csr.rs`: Rust CSR wrapper stores `rowptr`, `colidx`, and `values`; `transpose(self)` can reinterpret owned CSR buffers as CSC without copying, while `t(&self)` clones (lines 14-22, 24-51, 158-175).
- `igraph-src/include/igraph_datatype.h`: internal graph stores edge lists plus sorted edge indices and offsets for outgoing/incoming access; undirected edges are stored once/canonicalized (lines 65-116).
- `minigraph-src/minigraph/graphs/immutable_csr.h`: immutable CSR packs global ids, degrees, offsets, in/out edges, and id maps into a single graph buffer plus vertex data (lines 41-174).
- `thunderrw-src/util/graph/graph.h`: graph metadata includes CSR offsets, adjacency, degrees, vertex/edge properties, prefix sums, alias tables, and partition metadata (lines 19-115).
- `cugraph-src/cpp/examples/developers/vertex_and_edge_partition/vertex_and_edge_partition.cu`: local edge partition views expose offsets and indices; renumbered ids map to per-process vertex partitions (lines 171-323).

### Language/Framework

C++ (GAPBS, petgraph's model translated to Rust, igraph C internals, MiniGraph, ThunderRW, cuGraph), Rust (sprs, sparsetools, petgraph), CUDA/C++ (cuGraph).

### Engineering Pattern

Represent the algorithm graph as compact arrays:

```rust
struct CsrSnapshot {
    node_count: usize,
    edge_count: usize,
    out_offsets: Box<[u64]>,
    out_targets: Box<[u32]>,
    in_offsets: Option<Box<[u64]>>,
    in_sources: Option<Box<[u32]>>,
    weights_f64: Option<Box<[f64]>>,
}
```

The critical invariant is that `out_offsets[v]..out_offsets[v + 1]` is the contiguous neighbor slice for vertex `v`. Inverse adjacency is optional and should be explicit.

### Why It Matters For Neo4j-In-Rust

Neo4j's storage model is optimized for transactional property graph semantics. HPC traversal engines optimize for scanning adjacency without pointer chasing. A Rust rewrite can keep transactional records and property indexes separate from analytics snapshots. This avoids loading full relationship objects for PageRank, BFS, WCC, or SSSP.

### When To Use

- Read-mostly analytics over projected subgraphs.
- BFS/PageRank/SSSP/WCC/batch traversals where edge properties are a small typed payload.
- Memory-limited systems where per-edge object overhead dominates.
- Snapshots built from a transactional store or log.

### When Not To Use

- High-rate single-edge mutations where rebuilding or patching CSR on every write is unacceptable.
- Queries that need arbitrary relationship-property maps on every edge in the hot loop.
- Tiny graphs where clarity matters more than memory.

### Rust Translation

- Store `offsets` as `u64` if edge count can exceed `u32::MAX`; store vertex ids as `u32` until graph scale forces `u64`.
- Use typed newtypes (`DenseNodeId`, `EdgeOffset`) to avoid mixing external Neo4j ids with dense ids.
- Put weights/properties in separate columns keyed by edge position.
- Prefer immutable snapshots behind `Arc<CsrSnapshot>`.
- Build `CscSnapshot` only for algorithms that need incoming edges.

### Risks

- Duplicate edges and self-loops must be decided at snapshot build time. petgraph CSR rejects parallel edges, GAPBS can remove self loops during squishing, while rustworkx PageRank sums parallel edges.
- CSR insertion is expensive. petgraph CSR can insert while maintaining sorted rows, but row-offset updates are O(V) after insertion.
- Inverse adjacency can double memory.

### Memory, Concurrency, Testing

- Memory estimate: `(node_count + 1) * offset_width + edge_count * target_width + optional weights + optional inverse`.
- `Arc<CsrSnapshot>` is naturally shareable if arrays are immutable.
- Test with graphs containing empty rows, self loops, duplicate edges, high-degree hubs, and external ids outside `0..n`.
- Include a no-copy transpose/CSC test if buffers are reinterpreted.

### How Future Agents Should Apply It

Start every traversal feature by specifying its required snapshot shape: outgoing only, incoming only, both directions, weighted, unweighted, temporal, or partitioned. Do not let algorithm code reach back into the OLTP store for each edge.

## Pattern 2: Canonical Edge Build And In-Place CSR Construction

### Where Found

- `gapbs-src/src/builder.h`: `SquishCSR` sorts neighbors, removes self-loops and duplicates, computes prefix sums, and builds compact neighbors (lines 139-170). `MakeCSRInPlace` sorts/squishes an edge list, reinterprets the edge-list memory as outgoing neighbor storage, leaks the edge list to avoid copying, and optionally builds incoming structures (lines 188-234). The builder deletes the edge list early to save memory (lines 343-363).
- `graph-csr-openmp-src/inc/io.hxx`: OpenMP loader processes 256KB blocks, collects per-thread/per-partition edge lists and degree counts, then converts into CSR with scans and atomic captures (lines 226-394).
- `sparsetools-src/src/coord.rs`: COO-to-CSR counts nonzeros per row, computes cumulative row pointers, writes col/value arrays, then restores row pointers; input order is not assumed and duplicates are carried over (lines 3-78).
- `sparsetools-src/src/row.rs`: CSR canonical format means sorted and unique per row; helpers detect sorted/canonical rows and sort indices in place (lines 155-244).
- `sprs-src/sprs/src/sparse.rs`: `TriMat` holds unordered/repeated triplets and converts to CSR/CSC, while `CsMat` is the compressed representation expected by algorithms (lines 69-80, 184-207).

### Language/Framework

C++ (GAPBS), OpenMP/C++ (GVEL), Rust (sparsetools, sprs).

### Engineering Pattern

Separate ingestion from canonicalization:

```text
edge stream -> triplets/edge list
            -> count degree per dense source
            -> prefix sum offsets
            -> fill target array
            -> sort row slices
            -> deduplicate/sum/drop according to graph semantics
            -> freeze snapshot
```

Pseudocode:

```rust
fn convert_coo_to_csr(
    node_count: usize,
    sources: &[u32],
    targets: &[u32],
) -> CsrSnapshot {
    // 1. Count per source.
    // 2. Prefix sum into offsets.
    // 3. Scatter targets into adjacency.
    // 4. Restore offsets.
    // 5. Sort and canonicalize each row.
    // 6. Recompute compacted offsets if deduplicating.
}
```

### Why It Matters For Neo4j-In-Rust

Graph database edge ingestion often arrives as unordered relationship records, transaction logs, CSV imports, or projected query results. Traversal engines want canonical arrays. Making canonicalization a visible build phase keeps algorithms simple and makes memory predictable.

### When To Use

- Bulk import.
- Snapshot projection from labels/types/properties.
- Batch compaction after log-structured mutation.
- Analytics jobs that can tolerate snapshot latency.

### When Not To Use

- Per-transaction adjacency mutation.
- Algorithms that require edge multiplicity exactly as stored. For those, keep duplicate-preserving CSR or store multiplicity/weight during deduplication.

### Rust Translation

- Use a builder object with explicit phases: `count`, `prefix`, `fill`, `canonicalize`, `freeze`.
- Keep temporary arrays recyclable across builds.
- Use Rayon for row-local sorting after offsets are known.
- Expose a `DuplicatePolicy`: `Keep`, `Drop`, `SumWeights`, `CountMultiplicity`.

### Risks

- In-place tricks like GAPBS's edge-list reinterpretation are powerful but unsafe in Rust unless contained behind carefully audited ownership boundaries.
- Atomic scatter during parallel build can create nondeterministic order; canonical row sorting is needed for reproducible tests.
- Deduplication may change Cypher semantics if parallel relationships are meaningful.

### Memory, Concurrency, Testing

- Track peak build memory separately from frozen snapshot memory.
- Provide tests for unordered input, duplicate input, self loops, isolated vertices, and high-degree rows.
- Include determinism tests: same edge multiset should produce identical CSR bytes.

### How Future Agents Should Apply It

Design the graph projection subsystem before implementing algorithms. A low-RAM Neo4j rewrite wins or loses in the projection builder.

## Pattern 3: Dense Renumbering And External Id Mapping

### Where Found

- `cugraph-src/cpp/examples/developers/vertex_and_edge_partition/vertex_and_edge_partition.cu`: `create_graph_from_edgelist` may return a `renumber_map`; renumbered ids define local vertex partition ranges and edge partition offsets/indices (lines 137-323).
- `cugraph-src/cpp/src/c_api/bfs.cpp`: BFS copies sources, shuffles/renumbers external sources for multi-GPU, runs BFS on renumbered graph view, then unrenumbers predecessors (lines 63-148).
- `thunderrw-src/README.md`: input graph requires ids `0..N-1` and specific CSR binary files (`b_degree.bin`, `b_adj.bin`, etc.) (lines 49-62).
- `minigraph-src/minigraph/graphs/immutable_csr.h`: immutable CSR stores global ids and `localid_by_globalid` alongside local CSR arrays (lines 96-164).
- `ldbc_graphalytics_platforms_graphblas-src/src/main/c/src/graphio.cpp`: GraphBLAS platform reads a matrix plus a vertex mapping file in binary/text form (lines 8-66).
- `rustworkx-src/src/link_analysis.rs`: PageRank uses `node_bound()` rather than `node_count()` so removed nodes do not break dense array indexing (lines 101-116).

### Language/Framework

CUDA/C++ (cuGraph), C++ (ThunderRW, MiniGraph), C/GraphBLAS (Graphalytics), Rust/PyO3 (rustworkx).

### Engineering Pattern

Keep two id spaces:

```text
external id: original Neo4j node id, stable across transactions
dense id:    0..node_count_in_projection-1, compact for arrays
```

Snapshot algorithms use dense ids. API boundaries translate in and out.

### Why It Matters For Neo4j-In-Rust

Neo4j ids can be sparse, recycled, or much larger than a projected graph. Allocating arrays by max external id wastes RAM. Dense renumbering is the difference between `O(max_id)` and `O(projected_node_count)`.

### When To Use

- Every projected analytics graph.
- GPU transfers.
- GraphBLAS matrices.
- External-memory partitions.

### When Not To Use

- Tiny internal graphs.
- Data structures where external id ordering is semantically required.

### Rust Translation

```rust
struct DenseIdMap {
    external_to_dense: HashMap<i64, u32>,
    dense_to_external: Box<[i64]>,
}
```

For huge graphs, replace `HashMap` with a sorted map, perfect hash, or on-disk mapping table. Store dense-to-external contiguously because output unrenumbering often scans result arrays.

### Risks

- Mapping memory can rival graph memory. A hash map of millions of ids is not cheap.
- Multi-tenant projections need separate maps.
- Dynamic graph updates need a policy for stable dense ids versus compact rebuilds.

### Memory, Concurrency, Testing

- Report mapping memory in estimates.
- Fuzz external ids: sparse, negative if allowed, large, duplicate, deleted.
- Test algorithm outputs unrenumber correctly.

### How Future Agents Should Apply It

Never allocate a per-node algorithm array indexed by external Neo4j id. Always ask for the projection's dense id map.

## Pattern 4: Direction-Optimizing BFS With Queue/Bitmap Switching

### Where Found

- `gapbs-src/src/bfs.cc`: direction-optimizing BFS uses a `SlidingQueue` for top-down, a bitmap for bottom-up, converts queue-to-bitmap and bitmap-to-queue, and switches based on frontier edge volume (`alpha`, `beta`) (lines 25-40, 46-112, 122-179). The verifier checks a serial BFS tree and edge/depth constraints (lines 196-247).
- `gapbs-src/src/sliding_queue.h`: double-buffered queue has `slide_window()` and per-thread `QueueBuffer` with `fetch_and_add` flushes to avoid false sharing (lines 12-61, 87-108).
- `lagraph-src/src/algorithm/LAGr_BreadthFirstSearch.c`: LAGraph dispatches to push/pull BFS when SuiteSparse extensions and transpose/out-degree are available, otherwise uses vanilla push-only BFS (lines 18-44).
- `ligra-src/apps/BFS.C`: BFS defines update/updateAtomic/cond and iterates a `vertexSubset` through `edgeMap` until empty (lines 26-56).
- `graphscope-src/analytical_engine/apps/flash/traversal/bfs.h`: Flash BFS initializes distances and loops active vertex sets through `EdgeMap` while the frontier is nonempty (lines 41-61).
- `ldbc_graphalytics_platforms_graphblas-src/src/main/c/src/algorithms/bfs.cpp`: Graphalytics GraphBLAS calls `LAGr_BreadthFirstSearch`, then serializes sparse vector results and maps missing entries to infinity (lines 11-113).

### Language/Framework

C++/OpenMP (GAPBS, Ligra), C/GraphBLAS (LAGraph), C++/Flash (GraphScope), C/Graphalytics.

### Engineering Pattern

Top-down BFS scans edges from the current frontier. Bottom-up BFS scans unvisited vertices and checks whether any incoming neighbor is in the frontier. Switch when the frontier is large enough that scanning unvisited vertices is cheaper than expanding outgoing edges.

```rust
enum Frontier {
    Queue(Vec<u32>),
    Bitmap(FixedBitSet),
}

fn switch_frontier_for_bfs(
    frontier: Frontier,
    unexplored_edges: usize,
    frontier_edges: usize,
) -> Frontier {
    // If frontier_edges is large relative to unexplored_edges, switch to bitmap.
    // If bitmap frontier shrinks below threshold, convert back to queue.
}
```

### Why It Matters For Neo4j-In-Rust

Many graph database traversals begin as small frontiers but can explode across hubs. A pure queue BFS spends too much time expanding massive frontiers; a pure bitmap BFS wastes work on small frontiers. Neo4j-in-Rust should expose frontier representation as an internal strategy.

### When To Use

- Large unweighted BFS.
- Shortest unweighted path over projected graph.
- Reachability with high-degree hubs.
- Graphs with power-law degree distribution.

### When Not To Use

- Very small graph queries where conversion overhead dominates.
- Traversals with heavy per-edge predicates that cannot be evaluated from compressed columns.
- Graphs without inverse adjacency if bottom-up mode requires incoming neighbors.

### Rust Translation

- `Frontier::Queue` backed by `Vec<u32>` plus per-thread local buffers.
- `Frontier::Bitmap` backed by `fixedbitset` or a custom aligned bitset.
- Store parent/dist arrays as `AtomicI32`/workspace arrays for parallel BFS.
- Require `in_offsets/in_sources` for bottom-up mode.

### Risks

- Thresholds (`alpha`, `beta`) are workload-dependent.
- Bottom-up BFS needs incoming adjacency and a visited bitmap.
- Parent races must be benign and deterministic enough for tests.

### Memory, Concurrency, Testing

- Queue memory is `O(frontier_size)`, bitmap memory is `O(V / 8)`.
- Per-thread queue buffers reduce contention.
- Test queue-only, bitmap-only, and switching paths.
- Use LDBC/GAPBS-style verifiers that check distances, not exact parent tree when multiple shortest paths exist.

### How Future Agents Should Apply It

Implement BFS as a policy engine over frontiers, not as one monolithic queue loop. Keep conversion and thresholds isolated so benchmarks can tune them.

## Pattern 5: EdgeMap/VertexSubset As A General Traversal API

### Where Found

- `ligra-src/ligra/ligra.h`: `edgeMap` flags include `dense_forward`, `dense_parallel`, `sparse_no_filter`, `remove_duplicates`, `edge_parallel`, and `no_dense` (lines 47-55). Dense `edgeMap` scans incoming neighbors for vertices whose condition is true; sparse mode computes offsets by prefix scan, decodes outgoing neighbors, optionally removes duplicates, and filters (lines 58-215).
- `ligra-src/apps/BFS.C`: BFS becomes a short loop over `vertexSubset` and `edgeMap` with a small update functor (lines 26-56).
- `minigraph-src/minigraph/2d_pie/auto_map.h`: `ActiveEMap`, `ActiveVMap`, `ActiveMap`, and `ActiveEReduce` scan active vertices/edges with bitmaps and task runners (lines 45-177).
- `graphscope-src/analytical_engine/apps/flash/traversal/bfs.h`: Flash BFS uses `VertexMap`/`EdgeMap` style operations over active sets (lines 41-61).
- `graphscope-src/analytical_engine/apps/flash/ranking/pagerank.h`: PageRank uses dense edge mapping over all edges and vertex maps for local update (lines 41-66).

### Language/Framework

C++ templates (Ligra), C++ (MiniGraph), C++ Flash/GraphScope.

### Engineering Pattern

Expose a few traversal skeletons:

```text
vertex_map(active_vertices, vertex_fn)
edge_map(frontier, edge_fn, output_frontier, mode)
edge_reduce(active_edges, map_fn, reduce_fn)
```

Algorithms supply update logic and conditions. Runtime chooses sparse/dense/parallel/bitmap mode.

### Why It Matters For Neo4j-In-Rust

A graph database will have many algorithms and query operators that differ in update logic but share traversal mechanics. EdgeMap/VertexMap prevents duplicating scheduling, frontier conversion, and memory workspaces across BFS, WCC, label propagation, PageRank, and neighborhood expansion.

### When To Use

- Batch algorithms over a projected graph.
- Reusable graph algorithm library.
- Query runtime operators that expand neighbor sets.

### When Not To Use

- One-off transactional traversals with many dynamic property predicates.
- Algorithms needing fine-grained custom scheduling not expressible as edge/vertex maps.

### Rust Translation

Use generic closures where monomorphization is acceptable; use trait objects only outside the hot loop.

```rust
fn edge_map_frontier_apply<F>(
    graph: &CsrSnapshot,
    frontier: &[u32],
    mut update: F,
) where
    F: FnMut(u32, u32),
{
    for &src in frontier {
        for &dst in graph.out_neighbors(src) {
            update(src, dst);
        }
    }
}
```

For parallel versions, make update state explicit and split into per-thread buffers or atomics.

### Risks

- A too-generic API can hide expensive dynamic dispatch.
- Dense and sparse modes require different adjacency direction.
- Duplicate removal may be required for algorithms whose frontier semantics assume uniqueness.

### Memory, Concurrency, Testing

- Sparse edgeMap needs offsets/output arrays sized by frontier edge volume.
- Dense edgeMap often needs bitsets for active/visited.
- Test all flags/modes against a simple serial reference.

### How Future Agents Should Apply It

Define traversal skeletons before algorithm implementations. Treat BFS/PageRank/WCC as clients of the skeletons.

## Pattern 6: Pull PageRank With Incoming Adjacency And Sink Accounting

### Where Found

- `gapbs-src/src/pr.cc`: pull PageRank removes atomics and sees updated values immediately like Gauss-Seidel; it keeps `scores` and `outgoing_contrib`, pulls over in-neighbors, and uses dynamic scheduling and convergence error (lines 21-60, 76-94).
- `lagraph-src/src/algorithm/LAGr_PageRankGAP.c`: allocates vectors, initializes rank, pre-scales damping/outdegree, performs `r += A' * w` with `GrB_mxv`, and reduces diff per iteration (lines 103-143).
- `networkit-src/networkit/cpp/centrality/PageRank.cpp`: initializes degree/rank vectors, detects sinks, runs balanced parallel node loops over incoming edges, adds sink contribution, checks convergence, and normalizes (lines 21-122).
- `igraph-src/src/centrality/pagerank.c`: PageRank operator precomputes probability/outdegree, handles sink/teleport contribution, and iterates adjacency lists (lines 57-115).
- `rustworkx-src/src/link_analysis.rs`: PageRank sums parallel edge weights into a sparse Google matrix, handles personalization/dangling nodes, multiplies `&a * &popularity`, and checks L1 convergence (lines 32-75, 90-135, 136-226).
- `graphscope-src/analytical_engine/apps/flash/ranking/pagerank.h`: PageRank uses dense edge mapping and vertex updates (lines 41-66).

### Language/Framework

C++ (GAPBS, NetworKit, GraphScope), C/GraphBLAS (LAGraph), C (igraph), Rust/PyO3 using sprs/ndarray (rustworkx).

### Engineering Pattern

For each destination vertex, sum contributions from incoming neighbors:

```rust
fn run_pull_pagerank_iteration(
    graph: &CsrSnapshot,
    rank: &[f64],
    out_degree: &[u32],
    next: &mut [f64],
    damping: f64,
) {
    // Requires graph.in_neighbors(v).
    // next[v] = teleport + damping * sum(rank[u] / out_degree[u]).
}
```

Pull avoids atomics because each thread owns destination vertices. Sink nodes require separate mass accounting.

### Why It Matters For Neo4j-In-Rust

PageRank and similar iterative scoring algorithms are common graph-data-science workloads. Pull mode is predictable, memory-friendly, and avoids atomic updates on `next_rank`.

### When To Use

- Static snapshot with incoming adjacency.
- Iterative rank/scoring algorithms.
- CPU implementations where avoiding atomic writes matters.

### When Not To Use

- Graphs without CSC/incoming adjacency.
- Highly selective personalized PageRank where sparse frontier propagation is cheaper.
- Streaming graphs where incremental update is required.

### Rust Translation

- Store `rank`, `next`, and `out_contrib` as `Vec<f64>` or `Box<[f64]>`.
- Use Rayon `par_chunks_mut` over destination vertices.
- Keep sink mass in a reduction variable.
- Support weighted PageRank by storing normalized edge weights as a parallel edge column or computing row sums once.

### Risks

- Floating point nondeterminism across parallel reductions.
- Dangling/personalization behavior must match documented semantics.
- Convergence tolerance often differs across libraries; tests should compare within epsilon.

### Memory, Concurrency, Testing

- Requires two rank arrays plus degree/contribution arrays.
- Incoming adjacency doubles edge index memory unless preexisting.
- Test against NetworkX/rustworkx/LDBC fixtures with relative tolerance.

### How Future Agents Should Apply It

Implement PageRank over the snapshot layer and make sink/personalization policies explicit API fields.

## Pattern 7: Connected Components As Label Propagation, BFS, Or Sparse Algebra

### Where Found

- `networkit-src/networkit/cpp/components/ParallelConnectedComponents.cpp`: uses label propagation over `activeNodes` and `nextActiveNodes` char vectors, balanced parallel loops, and optional coarsening after iterations (lines 21-84).
- `igraph-src/src/connectivity/components.c`: weak connected components use bitsets, queues, optional cache fast path, and O(V + E) traversal (lines 46-150).
- `sparsetools-src/src/graph.rs`: strong components use iterative Tarjan/Pearce-style algorithm with storage requirement `2*V` integer arrays; undirected components traverse both CSR and transpose arrays (lines 141-153, 291-337). `cs_graph_components` uses preallocated flag and work arrays (lines 339-413).
- `lagraph-src/src/algorithm/LG_CC_FastSV7.c`: connected components uses GraphBLAS operations (`GrB_mxv`, `eWiseAdd`, parent matrix container tricks, grandparent extraction, and reductions) (lines 101-168).
- `ldbc_graphalytics-src/.../WeaklyConnectedComponentsValidationTest.java`: validation maps actual component ids to expected ids because component ids may be permuted (lines 42-150).

### Language/Framework

C++ (NetworKit), C (igraph, LAGraph), Rust (sparsetools), Java validation (LDBC).

### Engineering Pattern

There are multiple valid CC implementations:

- Queue/BFS per component: simple, memory O(V), serial or lightly parallel.
- Label propagation/hooking: parallel, more iterations.
- Sparse linear algebra: parent vector and semiring operations.
- Strong components: iterative DFS/Tarjan-style algorithms.

### Why It Matters For Neo4j-In-Rust

Weakly connected components is a graph analytics staple and a useful test of frontier infrastructure. Strongly connected components needs different machinery and should not be lumped together with WCC.

### When To Use

- WCC: undirected or direction-ignored projections.
- SCC: directed reachability equivalence.
- Label propagation/GraphBLAS: large parallel analytics.
- Queue BFS: simple reference and small graphs.

### When Not To Use

- Dynamic graphs where fully recomputing CC per mutation is too expensive.
- Graphs with edge predicates that change per query unless projection is rebuilt.

### Rust Translation

- Provide `weak_components` and `strong_components` as separate APIs.
- Make undirected WCC require either symmetric CSR or both CSR+CSC.
- For test oracles, compare partitions, not raw component labels.

### Risks

- Component label values are not stable across parallel execution.
- Direction semantics must be explicit.
- SCC algorithms can use deep stacks if implemented recursively; use iterative variants.

### Memory, Concurrency, Testing

- WCC label array: `V * label_width`.
- Active bitsets: `2 * V/8` for parallel propagation.
- SCC may need multiple `V` arrays.
- LDBC-style permutation-tolerant validation is essential.

### How Future Agents Should Apply It

Add a partition-equivalence assertion helper before implementing WCC/SCC. It will prevent false negatives in parallel tests.

## Pattern 8: Shortest Paths: Heap, Delta-Stepping, GraphBLAS, And GPU Variants

### Where Found

- `igraph-src/src/paths/dijkstra.c`: Dijkstra rejects negative weights, supports cutoff, uses binary heaps/indexed heaps, and documents complexity (lines 64-167).
- `petgraph-src/crates/petgraph/src/algo/dijkstra.rs`: generic Dijkstra requires non-negative costs and documents O((V + E) log V) time with O(V + E) auxiliary memory (lines 16-39, 92-105).
- `ldbc_graphalytics_platforms_graphblas-src/src/main/c/src/algorithms/sssp.cpp`: GraphBLAS SSSP sets diagonal to zero, creates a LAGraph graph, caches min edge, and calls `LAGr_SingleSourceShortestPath` with a Delta scalar (lines 11-80).
- `ldbc_graphalytics_platforms_arcadedb-src/.../SingleSourceShortestPathsComputation.java`: ArcadeDB Graph Analytical View path uses CSR-native Dijkstra when edge properties are available, avoiding OLTP access; otherwise it falls back (lines 37-120).
- `gunrock-src/python/src/gunrock/bindings.cu`: SSSP accepts output tensors and a GPU context with load-balancing/uniquify options (lines 143-222).
- `cugraph-src/cpp/src/c_api/bfs.cpp`: BFS C API shows the same GPU graph-view pattern and source renumbering that SSSP-like APIs need (lines 63-148).
- `networkit-src/include/networkit/algebraic/algorithms/AlgebraicBFS.hpp`: algebraic BFS uses a transposed adjacency matrix and repeated min-plus matrix-vector operations until distances stabilize (lines 18-68).

### Language/Framework

C (igraph, LAGraph), Rust (petgraph), Java (ArcadeDB platform), CUDA/C++ (Gunrock, cuGraph), C++/GraphBLAS-style (NetworKit algebraic BFS).

### Engineering Pattern

Use algorithm variants by edge-weight semantics:

- Unweighted: BFS.
- Non-negative weights: Dijkstra or delta-stepping.
- Integer/small positive weights: bucketed or delta-stepping.
- Sparse linear algebra backend: min-plus semiring operations.
- GPU: CSR graph plus device output arrays and renumbered sources.

### Why It Matters For Neo4j-In-Rust

Shortest path APIs often look uniform to users, but implementation choice is deeply dependent on weights, cutoffs, source count, and graph size. A low-RAM rewrite should avoid building heavy priority queues when BFS or bounded search is enough.

### When To Use

- Dijkstra: single-source non-negative weighted paths with moderate frontier.
- Delta-stepping: parallel weighted SSSP.
- GraphBLAS min-plus: batch/matrix-friendly workloads.
- GPU: huge homogeneous weighted analytics.

### When Not To Use

- Negative weights without Bellman-Ford-like algorithm.
- Transactional path queries requiring property predicates per edge in the hot loop unless projection includes them.

### Rust Translation

- Separate `bfs_shortest_paths`, `dijkstra_shortest_paths`, and `delta_stepping_paths`.
- Make `weight_column` optional and typed.
- Accept `cutoff` and target early stop as first-class options.
- Return sparse results for unreachable-heavy graphs rather than full maps by default.

### Risks

- Heap allocation dominates small traversals.
- Floating weights introduce reproducibility issues.
- GraphBLAS/GPU paths require converting and copying graph data.

### Memory, Concurrency, Testing

- Dijkstra memory: distance array, predecessor optional, heap.
- Delta-stepping memory: buckets plus relax workspaces.
- Test negative weight rejection, cutoff, unreachable nodes, zero-weight edges, and multiple equal shortest paths.

### How Future Agents Should Apply It

Before coding shortest path, classify by weight domain and output shape. Do not implement one generic path engine that hides all costs.

## Pattern 9: GraphBLAS Semiring Backend As Optional Analytics Layer

### Where Found

- `graphblas-src/Include/GraphBLAS.h`: GraphBLAS defines sparse matrix operations on semirings as equivalent to graph computations (lines 11-17). It exposes algorithm selection hints (`GxB_AxB_HASH`, `DOT`, etc.) and storage controls for row/column/offset integer widths, hypersparse/bitmap, iso, and sparsity status (lines 520-530, 1553-1578).
- `lagraph-src/src/algorithm/LAGr_BreadthFirstSearch.c`: BFS chooses push/pull if SuiteSparse extensions, transpose, and out-degree are available (lines 18-44).
- `lagraph-src/src/algorithm/LAGr_PageRankGAP.c`: PageRank is `A' * w` plus vector operations (lines 103-143).
- `lagraph-src/src/algorithm/LG_CC_FastSV7.c`: connected components uses GraphBLAS matrix/vector operators and parent-vector tricks (lines 101-168).
- `python-graphblas-src/docs/user_guide/fundamentals.rst`: output object, mask, accumulator, and descriptor are on the left side of `<<`; fused operations avoid masked temporaries (lines 49-71, 102-105).
- `python-graphblas-src/docs/user_guide/operators.rst`: semirings such as `min_plus` are used for shortest paths (lines 129-157).
- `ldbc_graphalytics_platforms_graphblas-src/src/main/c/src/algorithms/bfs.cpp` and `sssp.cpp`: Graphalytics uses LAGraph calls and serializes sparse vector outputs with infinity for missing entries (BFS lines 11-113, SSSP lines 11-80).
- `graphblas_sparse_linear_algebra-src/.../context.rs`: Rust wrapper initializes a process-wide GraphBLAS context with allocator hooks; review comments warn GraphBLAS context is once-per-process and cannot be per-thread (lines 53-60, 74-77, 122-209).
- `graphblas_sparse_linear_algebra-src/.../sparse_matrix.rs`: Rust sparse matrix wrapper owns a `GrB_Matrix` and marks it `Send`/`Sync` with explicit safety comments (lines 35-130).
- `graphblas_sparse_linear_algebra-src/.../lz4_serializer.rs`: serializer descriptor sets LZ4 compression for matrix serialization (lines 22-100).
- `graphblas-pointers-src/README.md`: curated pointer list calls out LAGraph, BFS push-pull papers, SSSP delta-stepping, connected components, triangle counting, and masked matrix multiplication as GraphBLAS motifs (lines 53-114).

### Language/Framework

C/SuiteSparse:GraphBLAS, C/LAGraph, Python graphblas, Rust GraphBLAS wrapper, LDBC Graphalytics C platform.

### Engineering Pattern

Treat graph algorithms as sparse matrix/vector operations:

```text
BFS:       frontier vector, mask = unvisited, vxm/mxv over adjacency
PageRank:  rank = teleport + A' * (rank / out_degree)
SSSP:      min-plus relaxations / delta-stepping
CC:        parent vector plus min/second semiring updates
```

The mask/accumulator/descriptor model fuses work to avoid building unnecessary temporaries.

### Why It Matters For Neo4j-In-Rust

GraphBLAS can be an analytics plugin behind the snapshot layer. It gives a high-performance sparse algebra implementation without hand-writing every kernel. It is especially valuable for batch algorithms and matrix-friendly workloads.

### When To Use

- Batch graph analytics over stable snapshots.
- Algorithms expressible with sparse vector/matrix primitives.
- Workloads that benefit from hypersparse or bitmap matrix formats.
- Cross-language validation against LAGraph/Graphalytics.

### When Not To Use

- Transactional traversals with per-edge property predicates not encoded in matrices.
- Per-thread isolated contexts if the backend is process-global.
- Very small queries where FFI/setup cost dominates.

### Rust Translation

- Wrap GraphBLAS context as a process singleton.
- Use explicit allocator integration if the database tracks memory.
- Provide conversions from `CsrSnapshot` to `GrB_Matrix` with integer-width choices.
- Keep GraphBLAS execution optional behind a feature flag.

### Risks

- FFI safety and global context lifecycle.
- Memory accounting across external allocators.
- Different backends may choose different storage formats and algorithm paths.
- Error handling must translate GraphBLAS status codes into database errors.

### Memory, Concurrency, Testing

- Include GraphBLAS-allocated bytes in query memory estimates.
- Test context init/finalize once-per-process.
- Serialize/deserialize matrix snapshots for cache tests.
- Compare BFS/PageRank/SSSP/WCC outputs against pure Rust reference implementations.

### How Future Agents Should Apply It

Design the snapshot API so both pure Rust and GraphBLAS backends consume the same projected graph. Do not make GraphBLAS the only representation.

## Pattern 10: Hypersparse, Bitmap, And Integer-Width Storage Controls

### Where Found

- `graphblas-src/Include/GraphBLAS.h`: exposes controls/status for row/column/offset integer widths, hypersparse and bitmap switches, iso matrices, and sparsity format (lines 1553-1578).
- `graphblas-src/Source/convert/GB_convert_int.c`: converts internal integer arrays (`p/h/i/Y`) between 32-bit and 64-bit only when dimensions and `nvals` permit; full/bitmap matrices can return quickly because they lack some index arrays (lines 10-20, 56-68).
- `graphblas-src/Source/convert/GB_convert_sparse_to_hyper_test.c`: converts sparse to hypersparse if number of nonempty vectors is below threshold; vectors themselves never become hypersparse (lines 10-34).
- `graphblas-src/Source/global/GB_Global.c`: global defaults control hypersparsity, bitmap switching, and CSR/CSC format; bitmap switch defaults depend on matrix shape (lines 36-42, 469-508).
- `graphblas-src/Source/container/GB_unload_into_container.c`: exported containers report `nvals`, orientation, sparsity format, iso state, jumbled state, and integer-width-specific arrays (lines 45-112).

### Language/Framework

C/SuiteSparse:GraphBLAS.

### Engineering Pattern

Sparse is not one format. Choose among:

- Standard sparse CSR/CSC.
- Hypersparse for mostly empty rows/columns.
- Bitmap for medium density where bitmaps beat index arrays.
- Full for dense result vectors/matrices.
- 32-bit or 64-bit index arrays based on dimension and `nvals`.

### Why It Matters For Neo4j-In-Rust

Graph workloads are uneven. A subgraph projection for one label/type may be hypersparse relative to the whole store; a visited set is a bitmap; PageRank ranks are dense; adjacency may fit in `u32` while offsets need `u64`. One format wastes RAM across all cases.

### When To Use

- Hypersparse: many empty rows after filtering.
- Bitmap: visited/frontier masks, medium-density matrices.
- Full/dense: rank/distance arrays for all vertices.
- 32-bit indices: projected graph has fewer than 2^32 nodes/edges per array dimension.

### When Not To Use

- Do not use bitmap for tiny sparse sets where a small vector is cheaper.
- Do not use 32-bit offsets if edge count may exceed limit.
- Do not expose format switches as user-facing complexity unless required.

### Rust Translation

```rust
enum SparseVector<T> {
    Sparse { indices: Box<[u32]>, values: Box<[T]> },
    Bitmap { bits: FixedBitSet, values: Option<Box<[T]>> },
    Dense(Box<[T]>),
}
```

Expose memory estimates for each candidate and choose by thresholds learned from benchmarks.

### Risks

- Format conversion costs can dominate short algorithms.
- More representations increase test matrix size.
- 32-bit overflow bugs are catastrophic and often silent in unsafe code.

### Memory, Concurrency, Testing

- Add property tests around conversion equivalence.
- Test max-boundary cases near `u32::MAX` if feasible with synthetic metadata.
- Track conversion allocations separately from algorithm allocations.

### How Future Agents Should Apply It

Any new frontier/result type should declare whether it is sparse, bitmap, or dense. Avoid a one-size-fits-all `Vec`.

## Pattern 11: External-Memory Shards And File-Backed Vectors

### Where Found

- `gridgraph-src/README.md`: preprocessing converts edge lists into grid format; applications accept path and memory budget, including BFS, WCC, SpMV, and PageRank (lines 12-56).
- `gridgraph-src/core/graph.hpp`: graph state includes memory budget, partition batch, row/column offsets, buffer pools, and partition metadata (lines 52-70). `stream_vertices` batches partitions when vertex data exceeds memory budget and scans bitmaps by words to skip zeros (lines 142-213). `hint()` computes `partition_batch` from memory budget (lines 215-239). `stream_edges` marks shards via bitmap, computes total bytes, uses `O_DIRECT` if the memory budget is below required bytes, queues worker tasks, reads aligned buffers with `pread`, and uses `posix_fadvise` (lines 241-390).
- `gridgraph-src/core/bigvector.hpp`: file-backed vectors create/truncate files, open with `O_DIRECT`, mmap data, load/save subranges with `pread`/`pwrite`, and remap (lines 64-179).
- `graphchi-cpp-src/src/engine/graphchi_engine.hpp`: engine tracks sliding shards, memory shards, intervals, vertex data handlers, scheduler, memory budget, block size, and max window (lines 61-207).
- `graphchi-cpp-src/src/engine/functional/functional_engine.hpp`: functional engine loads in-edges through memory shard, executes updates, then streams sliding shards for out-edge updates in parallel (lines 26-121).
- `graphchi-cpp-src/src/engine/auxdata/vertex_data.hpp`: vertex data can use mmap or striped I/O and handles resizing/remapping (lines 75-135).
- `minigraph-src/README.md`: out-of-core single-machine engine pipelines disk I/O and CPU, decouples compute from memory/scheduling, uses hybrid graph-centric/vertex-centric parallelism, state machines, and a `buffer_size` controlling resident fragments (lines 1-38, 84-108).
- `thunderrw-src/util/graph/graph.cpp`: loads CSR adjacency by mmaping binary files, copying in parallel, and unmapping; edge labels/weights and alias tables use mmap/prefix files (lines 254-407).

### Language/Framework

C++ (GridGraph, GraphChi, MiniGraph, ThunderRW).

### Engineering Pattern

When graph does not fit in RAM, make partitions/shards first-class:

```text
partition metadata
file-backed edge shards
vertex-state windows
active shard bitmap
memory budget -> batch size
pread/mmap/O_DIRECT -> bounded buffers
```

Algorithms run by streaming only required shards and vertex ranges.

### Why It Matters For Neo4j-In-Rust

A graph database is often memory constrained because properties, indexes, transaction state, and page cache compete with analytics. External-memory traversal lets analytics run without loading full adjacency into heap memory.

### When To Use

- Graphs larger than configured analytics memory.
- Batch analytics where latency can tolerate disk streaming.
- Snapshot materialization from cold storage.

### When Not To Use

- Low-latency transactional traversals.
- Algorithms with random edge revisits and poor locality unless redesigned.
- Small graphs where in-memory CSR is simpler.

### Rust Translation

- Model `GraphShard` with file offsets and checksums.
- Use `memmap2` for read-only shard mappings and `pread`/`pwrite` for bounded windows.
- Use explicit memory budget structs and expose computed partition batches.
- Store vertex state separately so only active ranges are loaded.

### Risks

- Direct I/O alignment requirements.
- mmap lifetime/safety.
- Disk I/O scheduling can swamp CPU parallelism.
- Crash consistency for materialized shard files.

### Memory, Concurrency, Testing

- Tests need tiny shard files with forced memory budgets to exercise streaming.
- Include simulated partial reads and corrupted shard metadata.
- Measure peak RSS and page-cache effects separately where possible.

### How Future Agents Should Apply It

If implementing graph projection persistence, use GridGraph/GraphChi/MiniGraph as the reference family. Do not bolt on spilling after an in-memory design is complete.

## Pattern 12: Parallel Scheduling, Worklists, And False-Sharing Avoidance

### Where Found

- `galois-src/lonestar/tutorial_examples/SSSPPushSimple.cpp`: SSSP push operator relaxes neighbors and pushes destinations into a context worklist; examples choose chunked, ordered, parameterized, and deterministic schedules (lines 68-155).
- `gapbs-src/src/sliding_queue.h`: per-thread `QueueBuffer` batches pushes and flushes to shared queue using `fetch_and_add`, avoiding false sharing (lines 12-61, 87-108).
- `gapbs-src/src/bfs.cc`: top-down step uses `QueueBuffer` and compare-and-swap on parent array (lines 67-89).
- `ligra-src/ligra/ligra.h`: sparse edgeMap computes offsets by `plusScan` and uses flags for dense/sparse/parallel modes (lines 111-215).
- `graph-csr-openmp-src/inc/io.hxx`: loader uses OpenMP dynamic schedules, per-thread lists, per-partition degrees, atomics, and partition combining (lines 226-394).
- `networkit-src/networkit/cpp/centrality/PageRank.cpp` and `ParallelConnectedComponents.cpp`: use balanced parallel loops over nodes and active-node vectors (PageRank lines 21-122; CC lines 21-84).

### Language/Framework

C++/Galois, C++/OpenMP, C++/Ligra, C++/NetworKit.

### Engineering Pattern

Separate "what work exists" from "how it is scheduled":

- FIFO queue for BFS reference.
- Chunked LIFO/FIFO for locality.
- Ordered worklist for priority algorithms.
- Dense parallel node loop for pull algorithms.
- Per-thread buffers to avoid shared writes.

### Why It Matters For Neo4j-In-Rust

Graph algorithms are irregular. A single Rayon `par_iter()` is not always enough. Worklist policy affects locality, contention, determinism, and memory.

### When To Use

- Parallel BFS/SSSP/WCC.
- High-degree skew graphs.
- Algorithms with dynamic work generation.

### When Not To Use

- Small jobs where scheduling overhead dominates.
- User-facing transactional queries where deterministic traversal order is required.

### Rust Translation

- Use per-thread `Vec<u32>` buffers and combine after each level.
- Use atomics for first-claim parent/distance updates.
- Consider `crossbeam_deque` or custom worklists for irregular SSSP.
- Keep deterministic mode for tests.

### Risks

- False sharing on visited/parent arrays.
- Atomic contention on high-degree hubs.
- Nondeterministic parents in BFS even when distances are correct.

### Memory, Concurrency, Testing

- Pad per-thread counters or use local accumulation.
- Test under different thread counts.
- Validate semantic invariants rather than exact worklist order.

### How Future Agents Should Apply It

Add scheduler parameters to algorithm internals, not public user APIs at first. Tune using benchmarks, then expose only stable high-level options.

## Pattern 13: GPU/CPU Tradeoff With Explicit CSR/CSC Device Views

### Where Found

- `gunrock-src/python/src/gunrock/bindings.cu`: bindings expose CSR/CSC/COO views, CSR construction from COO or binary, GPU context options (`advance_load_balance`, `enable_uniquify`, `best_effort_uniquify`, `uniquify_percent`), and BFS/SSSP accepting output tensors (lines 100-260).
- `gunrock-src/benchmarks/spmv_bench.cu`: NVBench captures DRAM throughput, L1/L2 hit rates, load/store efficiency, converts MatrixMarket to COO/CSR/graph, and runs SpMV on device vectors (lines 61-95).
- `cugraph-src/cpp/examples/developers/vertex_and_edge_partition/vertex_and_edge_partition.cu`: graph construction from edge list returns graph, optional weights, and optional renumber map; local edge partitions expose offsets/indices and optional weights (lines 137-323).
- `cugraph-src/cpp/src/c_api/bfs.cpp`: BFS enforces orientation, transposes if needed, allocates local distance/predecessor buffers, shuffles/renumbers sources in multi-GPU mode, runs BFS, then unrenumbers predecessors (lines 22-148).
- `galois-src/libcusp/include/galois/graphs/CuSPPartitioner.h`: partitioner distinguishes CSR/CSC input/output formats and transposition when partitioning graph files for distributed execution (lines 35-115).

### Language/Framework

CUDA/C++ (Gunrock, cuGraph), C++ distributed graph tooling (Galois CuSP).

### Engineering Pattern

GPU algorithms take already-compact graph views:

```text
host edge list -> renumber -> CSR/CSC -> device arrays
source/output tensors allocated -> GPU kernel -> unrenumber output
```

GPU contexts expose load-balance and uniqueness options because frontier duplicates and degree skew matter.

### Why It Matters For Neo4j-In-Rust

GPU acceleration is attractive for GDS-like batch jobs but usually inappropriate for row-by-row Cypher traversal. The API must make projection, transfer, and output buffers explicit.

### When To Use

- Large static projections.
- BFS/SSSP/PageRank/SpMV-like analytics.
- Many repeated algorithms over the same device-resident graph.

### When Not To Use

- Small or one-off queries.
- Traversals needing many property-store lookups.
- Workloads where PCIe transfer dominates.

### Rust Translation

- Put GPU behind a backend trait consuming `CsrSnapshot`.
- Cache device graph by snapshot id/version.
- Require memory estimates for device arrays and output tensors.
- Use feature-gated CUDA bindings; keep pure Rust fallback.

### Risks

- Device memory can be smaller than host memory.
- Renumbering/unrenumbering bugs.
- Duplicate-frontier handling affects correctness/performance.

### Memory, Concurrency, Testing

- Benchmark transfer time separately from kernel time.
- Test CPU/GPU result parity on fixtures.
- Track device memory and host pinned memory.

### How Future Agents Should Apply It

Do not design GPU as an afterthought to pointer-heavy graph objects. GPU starts at CSR/CSC snapshots.

## Pattern 14: Temporal, Incremental, And Differential Views

### Where Found

- `raphtory-src/raphtory-benchmark/src/common/mod.rs`: benchmarks count edges, temporal edges, cached views, edge existence, active-edge windows, and materialization; it compares base graph, full window, 10 percent time window, 10 percent subgraph, subgraph-window, layered window, and persistent layered window (lines 268-405, 642-790).
- `differential-dataflow-src/differential-dataflow/src/operators/iterate.rs`: iterative operators circulate differences until fixed point; documentation warns collections must consolidate or cancelable differences can circulate indefinitely; direct edit-variable construction can be more efficient because only small edits circulate (lines 1-31, 81-99, 182-191).
- `timely-dataflow-src/timely/src/progress/frontier.rs`: `Antichain` stores the minimal set of incomparable times; insertion evicts dominated elements and preserves frontier minimality (lines 1-47, 126-164).
- `timely-dataflow-src/mdbook/src/chapter_3/chapter_3_1.md`: batching inputs under the same timestamp is recommended because per-record timestamps hurt progress tracking (line 19).
- `timely-dataflow-src/mdbook/src/chapter_5/chapter_5_2.md`: progress changes are batched and broadcast to workers; pointstamp/path-summary safety governs when computation is complete (lines 59-63, 136-160).
- `graphscope-src/interactive_engine/groot-module/.../SnapshotManager.java`: write/query snapshot ids must persist before broadcast; consumed offsets can persist asynchronously and duplicate replay is acceptable (lines 89-170).
- `graphscope-src/interactive_engine/executor/engine/pegasus/pegasus/examples/page_rank.rs`: distributed PageRank uses dataflow iteration, repartitioning by target, fold partitions, broadcast, and delta convergence (lines 99-160).

### Language/Framework

Rust (Raphtory, Differential Dataflow, Timely Dataflow, Pegasus example), Java (GraphScope snapshot manager).

### Engineering Pattern

Temporal and incremental graph systems need:

- Logical time/frontiers.
- Windowed views.
- Cached/materialized views.
- Batched updates.
- Snapshot ids and replay semantics.
- Consolidation of deltas.

### Why It Matters For Neo4j-In-Rust

Neo4j workloads often need "graph at transaction T", "recent activity window", or incremental analytics after writes. Rebuilding full CSR after every change is wasteful; but unbounded deltas can also consume memory.

### When To Use

- Temporal graph queries.
- Streaming graph updates.
- Incremental analytics over stable base plus deltas.
- Multi-version read snapshots.

### When Not To Use

- Simple static analytics snapshot where full rebuild is cheaper and simpler.
- Workloads without clear snapshot/time semantics.

### Rust Translation

- Represent graph snapshots as `(base_csr, delta_log, valid_time_range)`.
- Batch transaction log entries by timestamp/epoch.
- Offer `materialize_window` and `cache_view` APIs with memory estimates.
- Use antichain/frontier concepts for distributed or multi-partition progress.

### Risks

- Delta accumulation can exceed base graph memory if not compacted.
- Materialized windows can duplicate edge storage.
- Snapshot correctness is subtle under concurrent writes.

### Memory, Concurrency, Testing

- Benchmark lazy view vs materialized snapshot like Raphtory.
- Test replay idempotence and duplicate replay tolerance.
- Include compaction thresholds and memory alarms.

### How Future Agents Should Apply It

When adding temporal support, copy Raphtory's benchmark matrix: base, cached, full-window, small-window, subgraph, subgraph-window, layered, persistent, materialized.

## Pattern 15: Random Walk Latency Hiding With Prefetch And Step Interleaving

### Where Found

- `thunderrw-src/README.md`: ThunderRW targets random-walk memory stalls, reporting that irregular accesses caused 73.1 percent stalls and step-centric Gather-Move-Update plus interleaving reduces stalls to 15 percent; supported algorithms include PPR, DeepWalk, Node2Vec, and MetaPath (lines 5-22, 83-88).
- `thunderrw-src/random_walk/types.h`: compile-time flags include prefetching, interleaving, and ring size 64; buffer slots store walker, previous vertex, random value, offset, sequence, weight, and alias (lines 13-23, 72-94).
- `thunderrw-src/random_walk/uniform_sampling.h`: uniform sampling pipeline stages prefetch degree offsets, compute neighbor positions/prefetch neighbor, then update walker/current/sequence (lines 11-44).
- `thunderrw-src/random_walk/creeper.h`: allocates per-thread weight/alias buffers based on max degree and ring size; splits walker tasks and sequence buffers; comments note output buffer assumes enough memory and double buffering to disk is future work (lines 24-90, 700-779).
- `thunderrw-src/util/graph/graph.cpp`: prints CSR memory cost formulas and loads CSR/edge properties with mmap and parallel copies (lines 254-407, 596-605).

### Language/Framework

C++ random-walk engine.

### Engineering Pattern

Random walks are latency-bound. Interleave many walkers so while one waits for adjacency memory, another progresses. Prefetch degree/adjacency data ahead of use and keep per-thread buffers.

### Why It Matters For Neo4j-In-Rust

Graph databases often expose random-walk-based embeddings or sampling. These workloads behave differently from BFS/PageRank because each walker follows unpredictable edges.

### When To Use

- Node2Vec/DeepWalk/PPR-like workloads.
- Graph embeddings and sampling.
- Large graphs with irregular adjacency access.

### When Not To Use

- Deterministic short traversals.
- Workloads requiring strict per-walk ordering as observed by user callbacks.

### Rust Translation

- Store walkers in SoA buffers, not objects.
- Use ring-buffer stages for gather/move/update.
- Use explicit prefetch intrinsics only behind architecture-specific modules.
- Keep output streaming/double-buffering in scope if sequences are large.

### Risks

- Unsafe prefetch code and architecture dependence.
- Huge output sequence buffers can exceed graph memory.
- Randomness/reproducibility under parallel scheduling.

### Memory, Concurrency, Testing

- Memory includes walker state, per-thread alias/weight buffers, and output sequences.
- Test fixed seeds and reproducibility under thread-count settings.
- Benchmark stalls/cache misses where possible.

### How Future Agents Should Apply It

Do not implement random walks as one `for step in walk` per walker loop. Use interleaving if the graph is large enough for memory latency to dominate.

## Pattern 16: Array Layout And Contiguity As API Contracts

### Where Found

- `ndarray-src/src/impl_methods.rs`: `as_standard_layout` returns a copy-on-write view without cloning if already standard layout, otherwise clones; mutable and read-only slices are only available for standard/contiguous layouts; flattening avoids copies only when possible (lines 1626-1683, 1779-1859, 2268-2289).
- `nalgebra-src/src/sparse/cs_matrix.rs`: compressed-column storage trait exposes shape, row indices, contiguous value buffer, column ranges, and lengths (lines 45-138).
- `nalgebra-src/src/base/edition.rs`: reshape can reinterpret dynamic matrix data without copying (lines 955-964).
- `nalgebra-src/src/sparse/cs_matrix_cholesky.rs`: builds `CsVecStorage` and `CsMatrix::from_data` while avoiding a transpose (lines 299-315).
- `sparsetools-src/src/row.rs`: dense output routines assume C-contiguous row-major dense matrices and CSR input arrays (lines 125-153).

### Language/Framework

Rust (`ndarray`, `nalgebra`, `sparsetools`).

### Engineering Pattern

Hot algorithms should know whether arrays are contiguous and in what logical order. Conversion to standard layout is explicit because it may allocate.

### Why It Matters For Neo4j-In-Rust

Memory usage is not just data structure choice. Hidden clones from layout conversion can double peak memory. If an algorithm exports to ndarray/nalgebra/GraphBLAS, it must know whether it is borrowing contiguous arrays or allocating copies.

### When To Use

- Numeric algorithms using dense vectors/matrices.
- FFI boundaries requiring contiguous memory.
- Serialization/deserialization of snapshots.

### When Not To Use

- Dynamic object/property maps where array layout is not stable.

### Rust Translation

- APIs should accept slices and return whether data is borrowed or owned.
- Use `Cow<[T]>` where layout conversion may allocate.
- Provide `as_contiguous_targets()` and similar methods with documented cost.

### Risks

- Silent clones in helper libraries.
- Unsafe assumptions about memory order.
- Borrowed views escaping snapshot lifetimes.

### Memory, Concurrency, Testing

- Test standard and non-standard layout inputs.
- Add allocation counters around conversion-heavy paths.
- Make FFI wrappers require contiguous slices.

### How Future Agents Should Apply It

When connecting algorithm arrays to numeric crates, look for clone-on-layout paths before calling them in memory-sensitive code.

## Pattern 17: Benchmark Datasets, Oracles, And Methodology

### Where Found

- `ldbc_graphalytics-src/.../BreadthFirstSearchValidationTest.java`: BFS validation tests directed/undirected execution, validation graphs, example graphs, expected outputs, vertex ids, and path length (lines 32-151).
- `ldbc_graphalytics-src/.../PageRankValidationTest.java`: PageRank validation covers directed/undirected runs, damping factor 0.85, iteration counts, and output epsilon/relative deviation (lines 39-149).
- `ldbc_graphalytics-src/.../WeaklyConnectedComponentsValidationTest.java`: WCC validation handles component-id permutation by mapping actual labels to expected labels (lines 42-150).
- `ldbc_graphalytics_docs-src/tex/appendix_data_format.tex`: benchmark results JSON includes system under test, benchmark config, experiment results, target scale, and resource usage (lines 1-18).
- `ldbc_graphalytics_platforms_graphblas-src/src/main/java/.../GraphblasPlatform.java`: platform loads graphs into intermediate files, dispatches by algorithm enum, executes jobs, and collects processing time (lines 40-151).
- `gapbs-src/src/bfs.cc`: BFS verifier checks tree/depth/edge constraints rather than exact traversal order (lines 196-247).
- `gunrock-src/benchmarks/spmv_bench.cu`: benchmark collects DRAM throughput, L1/L2 hit rates, and load/store efficiency in addition to runtime (lines 61-95).
- `raphtory-src/raphtory-benchmark/src/common/mod.rs`: benchmark matrix includes cached vs uncached, temporal counts, windowed/materialized/subgraph/layered/persistent views (lines 268-405, 642-790).

### Language/Framework

Java (LDBC Graphalytics), TeX docs, C++/CUDA (Gunrock), C++ (GAPBS), Rust (Raphtory).

### Engineering Pattern

Benchmarks need:

- Correctness oracles.
- Resource/memory accounting.
- Dataset scale metadata.
- Algorithm parameters.
- Repeatable output validation tolerant to allowed nondeterminism.
- Separate timings for load, projection, computation, and serialization.

### Why It Matters For Neo4j-In-Rust

Claiming lower RAM usage requires proving it. Graph algorithms often have correct but different output labels/parents. Benchmark infrastructure must encode semantic equality, not byte-for-byte output where nondeterminism is legitimate.

### When To Use

- Every algorithm landing in the database.
- Every new projection/snapshot format.
- Every optimization that claims memory or speed improvement.

### When Not To Use

- Never skip benchmarks for memory-sensitive changes; at minimum add micro fixtures.

### Rust Translation

- Build `algo_fixture` modules with tiny graphs and expected semantic outputs.
- Record `projection_bytes`, `workspace_bytes`, `result_bytes`, `peak_rss`, and `elapsed`.
- Validate WCC by partition equivalence and BFS by distance constraints.
- Store benchmark result JSON with version, dataset, and configuration.

### Risks

- Comparing only runtime hides memory regressions.
- Exact output comparisons can fail legitimate parallel implementations.
- Dataset loading time can be confused with algorithm execution.

### Memory, Concurrency, Testing

- Run tests under single-thread and multi-thread modes.
- Include high-degree, disconnected, directed, weighted, duplicate-edge, and temporal fixtures.
- Track peak memory during projection and during algorithm separately.

### How Future Agents Should Apply It

Add oracle tests before optimizing. Use LDBC/GAPBS validation style as the default for traversal algorithms.

## Pattern 18: Algorithm APIs Should Separate Graph, Workspace, Options, And Output

### Where Found

- `petgraph-src/crates/petgraph/src/visit/traversal.rs`: BFS structure keeps discovered map and stack/queue separate from the graph; it is generic over `IntoNeighbors` (lines 273-307).
- `petgraph-src/crates/petgraph/src/algo/dijkstra.rs`: Dijkstra API accepts graph, start, optional goal, edge-cost function, and returns distance map (lines 16-39, 92-105).
- `igraph-src/src/graph/visitors.c`: BFS API can record predecessor, rank, order, distance, and parents optionally; it uses queue and bitset workspaces internally and supports callback stop (lines 230-380).
- `ldbc_graphalytics_platforms_graphblas-src/src/main/c/src/algorithms/bfs.cpp`: GraphBLAS BFS separates graph loading, source mapping, algorithm execution, and output serialization (lines 11-113).
- `gunrock-src/python/src/gunrock/bindings.cu`: GPU BFS/SSSP take graph, source, preallocated output tensors, and context/options (lines 177-260).
- `graphblas_sparse_linear_algebra-src/.../sparse_matrix.rs`: matrix wrapper exposes constructors, clear, and number-of-values operations separately from algorithm logic (lines 35-130).

### Language/Framework

Rust (petgraph, GraphBLAS wrapper), C (igraph, Graphalytics), CUDA/C++ (Gunrock).

### Engineering Pattern

Do not make algorithms allocate everything implicitly. Shape APIs like:

```rust
struct BfsOptions {
    direction_optimizing: bool,
    depth_limit: Option<u32>,
    compute_predecessors: bool,
}

struct BfsWorkspace {
    distance: Vec<i32>,
    predecessor: Vec<i32>,
    frontier_a: Vec<u32>,
    frontier_b: Vec<u32>,
    visited: FixedBitSet,
}

fn run_bfs_with_workspace(
    graph: &CsrSnapshot,
    source: u32,
    options: &BfsOptions,
    workspace: &mut BfsWorkspace,
) -> BfsResultView<'_> {
    todo!()
}
```

### Why It Matters For Neo4j-In-Rust

Database query runtimes need predictable memory and cancellation. If algorithm APIs allocate internally, the memory manager cannot reject oversized jobs early or reuse workspaces across batched jobs.

### When To Use

- Production algorithm library.
- Batch query engine.
- Any algorithm with large frontiers/results.

### When Not To Use

- Simple teaching/reference implementations.

### Rust Translation

- Provide `estimate_*_memory_bytes` functions.
- Allow caller-provided workspace for repeated runs.
- Use result views over workspace arrays where lifetimes make sense.
- Keep ergonomic one-shot wrappers for tests and small jobs.

### Risks

- Workspace reuse can leak old values if not reset.
- Lifetimes can become complex; keep types simple.
- Public APIs can be too low-level; layer ergonomic wrappers above.

### Memory, Concurrency, Testing

- Test workspace reset.
- Test memory estimates against actual allocations.
- Test cancellation/drop paths.

### How Future Agents Should Apply It

Every algorithm should have: options, memory estimate, workspace, run, validate/reference test.

## Pattern 19: Mutable Graph Convenience Is Not The Same As Compact Analytics

### Where Found

- `networkit-src/include/networkit/graph/Graph.hpp`: mutable graph stores `inEdges/outEdges` as `vector<vector<node>>`, optional weights and ids, existence vector, and parallel iterator methods (lines 103-160).
- `petgraph-src/crates/petgraph/src/csr.rs`: CSR graph is compact and fast for outgoing access but has insertion complexity and no parallel edges (lines 50-79, 350-430).
- `igraph-src/src/graph/adjlist.c`: adjacency lists are recommended for repeated neighbor iteration; lazy adjacency lists query/store per vertex on first access and support simplification options (lines 58-150, 171-285).
- `sprs-src/sprs/src/sparse.rs`: compressed sparse matrices are immutable in structure for mutable views; triplets exist for construction, not hot traversal (lines 14-29, 69-80, 184-207).

### Language/Framework

C++ (NetworKit), Rust (petgraph, sprs), C (igraph).

### Engineering Pattern

There are two useful graph representations:

- Mutable ergonomic graph: good for construction, editing, and graph APIs.
- Compact analytics graph: good for scanning, memory, and vectorized algorithms.

Trying to make one representation serve both roles usually hurts one side.

### Why It Matters For Neo4j-In-Rust

Neo4j's core graph store needs updates and property semantics. The GDS/traversal layer needs compact scans. A Rust rewrite should not force core storage to look like CSR, nor force analytics to traverse core records.

### When To Use

- Mutable adjacency: ingest/edit/staging.
- CSR/CSC: frozen projection/snapshot.

### When Not To Use

- Do not run high-volume analytics over `Vec<Vec<NodeId>>` if memory is the goal.
- Do not mutate CSR per relationship write.

### Rust Translation

- Build snapshots from a mutable projection builder.
- Keep immutable algorithm snapshots versioned.
- Use copy-on-write or delta overlays only when benchmarked.

### Risks

- Duplicate source of truth.
- Snapshot staleness.
- Mapping between store ids and dense snapshot ids.

### Memory, Concurrency, Testing

- Test snapshot isolation under concurrent writes.
- Test rebuild/refresh memory peaks.
- Add version ids to all snapshot-backed results.

### How Future Agents Should Apply It

When a feature needs graph mutation and graph analytics, specify which representation owns which phase.

## Pattern 20: Memory Accounting As A Public Contract

### Where Found

- `gridgraph-src/core/graph.hpp`: memory budget controls partition batches and whether edge streaming uses direct I/O (lines 52-70, 215-390).
- `graphchi-cpp-src/src/engine/graphchi_engine.hpp`: engine reads memory budget, block size, scheduler settings, and shard intervals during construction (lines 131-207).
- `minigraph-src/README.md`: command-line `buffer_size` controls the number of fragments resident in memory (lines 93-108).
- `thunderrw-src/util/graph/graph.cpp`: prints CSR memory cost using vertices/edges and type sizes (lines 596-605).
- `ldbc_graphalytics_docs-src/tex/appendix_data_format.tex`: benchmark reports resource usage along with scale and experiment data (lines 1-18).
- `graphblas_sparse_linear_algebra-src/.../memory_allocator.rs`: allocator integration must match Rust's global allocator or risk undefined behavior; allocator function pointers are explicit (lines 9-70).

### Language/Framework

C++ external-memory engines, C/GraphBLAS through Rust allocator wrapper, LDBC docs.

### Engineering Pattern

Every projection and algorithm should answer before running:

```text
required graph bytes
required inverse bytes
required workspace bytes
required result bytes
peak temporary build bytes
external/backend allocator bytes
```

### Why It Matters For Neo4j-In-Rust

Lower RAM usage is not a vibe; it is a contract. A database must reject or spill jobs before OOM, not after half a snapshot is built.

### When To Use

- All graph projections.
- All algorithms.
- All external backends.
- All benchmark reports.

### When Not To Use

- There is no good exception for production memory-sensitive graph processing.

### Rust Translation

```rust
struct MemoryEstimate {
    graph_bytes: u64,
    inverse_bytes: u64,
    workspace_bytes: u64,
    result_bytes: u64,
    temporary_bytes: u64,
    backend_bytes: u64,
}
```

Have algorithms expose `estimate_snapshot_memory_bytes` and `estimate_workspace_memory_bytes` before allocation.

### Risks

- Under-counting allocator overhead and alignment.
- Ignoring external library allocations.
- Failing to include dense id maps.

### Memory, Concurrency, Testing

- Unit-test estimates on tiny graphs exactly.
- Integration-test estimates against allocation counters/RSS with tolerance.
- Include peak build memory, not only frozen snapshot memory.

### How Future Agents Should Apply It

Add memory estimate tests with every algorithm feature. If a future implementation cannot estimate memory, it is not ready for a low-RAM graph database.

## Pattern 21: Graph Analytical View As A Bridge From OLTP To CSR

### Where Found

- `ldbc_graphalytics_platforms_arcadedb-src/.../SingleSourceShortestPathsComputation.java`: SSSP checks whether the graph provider is a Graph Analytical View with edge properties and runs CSR-native Dijkstra with zero OLTP access; otherwise it falls back (lines 37-120).
- `ldbc_graphalytics_platforms_arcadedb-src/lsqb/systems/arcadedb.py`: data transfer is chunked to avoid loading the whole payload and socket limits; Graph Analytical View is built/rebuilt so traversal providers are registered (lines 168-215).
- `graphscope-src/README.md`: GraphScope positions itself as a distributed graph computing system with property graph model and Vineyard distributed in-memory management for graphs too large for one machine (lines 22-24, 88-106, 248-314).
- `graphscope-src/analytical_engine/README.md`: analytical engine uses GRAPE fix-point model, PIE, mutable fragments, Vineyard, and service mode (lines 5-9).

### Language/Framework

Java/Python platform integration (ArcadeDB Graphalytics), GraphScope distributed graph engine.

### Engineering Pattern

Expose an analytical view/projection separate from transactional storage. Build/rebuild/register it, then route algorithms through it when it has the required properties.

### Why It Matters For Neo4j-In-Rust

This is the closest pattern to "Neo4j core plus Graph Data Science". The database can keep transactional property storage while offering a graph analytical view that is CSR-native and algorithm-ready.

### When To Use

- Long-running analytics.
- Reused projections.
- Weighted algorithms requiring selected relationship properties.

### When Not To Use

- One-off queries with tiny result sets.
- Queries requiring all arbitrary properties during traversal.

### Rust Translation

- `GraphProjection` object with schema: node labels, relationship types, direction, edge weight column, property columns.
- `GraphAnalyticalView` stores dense id map plus CSR/CSC plus property columns.
- Algorithms ask the view for required capabilities.

### Risks

- View refresh semantics.
- Memory duplication between store and view.
- Property staleness.

### Memory, Concurrency, Testing

- Test fallback path when analytical view lacks weights/properties.
- Test rebuild idempotence and memory peak.
- Record view version in algorithm outputs.

### How Future Agents Should Apply It

Use "analytical view" as the boundary term in architecture docs. It makes the OLTP/CSR split concrete.

## Pattern 22: Source-Level Verification Fixtures And Reference Implementations

### Where Found

- `gapbs-src/src/bfs.cc`: verifier computes a serial BFS-like check and validates parent/depth constraints (lines 196-247).
- `ldbc_graphalytics-src` validation tests for BFS, PageRank, and WCC provide expected output and tolerance/permutation handling (BFS lines 32-151, PageRank lines 39-149, WCC lines 42-150).
- `sparsetools-src/src/test.rs`: provides CSR/CSC test data fixtures referenced by sparse tests (lines found via source listing; CSR/CSC fixture functions at lines 47-72).
- `sparsetools-src/src/coo/coo_test.rs`: tests conversion to CSC/CSR and duplicate summing behavior (lines 228-290 from search result context).
- `petgraph-src/crates/petgraph/src/visit/traversal.rs`: BFS implementation is generic and simple enough to serve as a reference for custom CSR traversal (lines 273-307).

### Language/Framework

C++ verification (GAPBS), Java tests (LDBC), Rust tests (sparsetools, petgraph).

### Engineering Pattern

Keep simple reference implementations beside optimized algorithms. Use them for small graphs and fuzzing.

### Why It Matters For Neo4j-In-Rust

Low-RAM optimizations often trade clarity for performance. Reference implementations prevent optimized traversal bugs from hiding behind nondeterministic parallel output.

### When To Use

- Every optimized BFS/PageRank/WCC/SSSP.
- Every CSR builder optimization.
- Every frontier representation conversion.

### When Not To Use

- Reference implementations should not be used for production-scale queries unless explicitly selected.

### Rust Translation

- Put serial reference algorithms under `#[cfg(test)]` or a `reference` module.
- Generate random small graphs and compare semantic outputs.
- Include fixtures with self loops, duplicates, disconnected components, and weighted edges.

### Risks

- Reference implementation can share the same bug if it reuses too much code.
- Floating point comparisons need tolerance.

### Memory, Concurrency, Testing

- Run reference comparisons under multiple thread counts.
- Compare BFS distances, WCC partitions, PageRank tolerance, SSSP distances.

### How Future Agents Should Apply It

Before tuning a kernel, write a slow boring oracle. This is especially important when changing frontier or storage formats.

## Cross-Repo Design Synthesis

### Adjacency Layout

The strongest consensus is `offsets + indices + optional values`. GAPBS, sprs, sparsetools, petgraph CSR, cuGraph, Gunrock, MiniGraph, ThunderRW, GridGraph, and GraphBLAS all use variations of this model. NetworKit and igraph show mutable/convenience alternatives, but even igraph keeps indexed edge vectors and offset arrays internally.

For Neo4j-in-Rust:

- Core store: stable records and property columns.
- Projection builder: mutable edge list/triplets.
- Analytics snapshot: immutable CSR/CSC arrays.
- Algorithm workspaces: dense arrays/bitsets keyed by dense id.

### CSR/CSC And Compressed Sparse Matrices

CSR is preferred for outgoing traversal; CSC or transpose is required for incoming traversal/pull algorithms. GraphBLAS treats orientation as a matrix descriptor concern, but the physical storage still matters. sparsetools shows CSR-to-CSC is linear and can be preallocated; sprs and nalgebra show Rust type shapes for compressed storage; SuiteSparse shows how formats evolve to hypersparse/bitmap/full.

For Neo4j-in-Rust:

- Default projection should build outgoing CSR.
- Add inverse CSC as a selectable capability.
- Store orientation in the snapshot metadata.
- Add `can_pull`, `can_push`, `can_weight`, and `can_temporal_window` capability checks.

### Frontier Queues

GAPBS's `SlidingQueue`, Ligra's `vertexSubset`, MiniGraph's active maps, GraphScope Flash, and GraphBLAS sparse vectors all encode frontiers differently. The common design is representation switching:

- Sparse queue/list for small frontiers.
- Bitmap for dense frontiers.
- Sparse vector/mask for GraphBLAS.
- Active partition/shard bitmap for external-memory processing.

For Neo4j-in-Rust:

- Do not let `Vec<NodeId>` become the only frontier.
- Implement a `Frontier` enum with conversion costs and memory estimates.
- Record frontier density in benchmark output.

### BFS/PageRank/WCC/SSSP

- BFS: direction-optimizing push/pull; verifier checks distances.
- PageRank: pull mode avoids atomics; sink/personalization handling is explicit.
- WCC: compare partitions, not labels; parallel label propagation and sparse algebra are valid.
- SSSP: choose by weight domain; Dijkstra, delta-stepping, min-plus, and GPU kernels are different tools.

For Neo4j-in-Rust:

- Algorithm APIs should expose options and memory estimates.
- Keep serial references.
- Choose implementation at runtime based on graph capabilities and options.

### Graph Partitioning And Batch Traversal

GridGraph's grid shards, GraphChi sliding shards, MiniGraph fragments, cuGraph partitions, and Galois CuSP partitioner all make partitioning explicit. Timely/Differential make batching explicit at the time/progress layer.

For Neo4j-in-Rust:

- Projection ids should include partitioning layout.
- External-memory views should know shard byte ranges and active shard bitmaps.
- Batch updates by snapshot epoch, not per edge.

### GPU/CPU Tradeoffs

GPU repos require compact CSR/CSC/COO and dense ids. They also expose load balancing and duplicate frontier controls. CPU repos give more flexible scheduling and easier integration with transactional systems.

For Neo4j-in-Rust:

- CPU path first.
- GPU path consumes the same snapshot, but with explicit device memory and transfer accounting.
- Avoid per-edge property callbacks in GPU kernels.

### Temporal Graphs

Raphtory benchmarks cache/materialization/window/layer/persistent combinations. Timely and Differential show progress frontiers and delta circulation. GraphScope snapshot manager shows persistence before broadcast and idempotent replay acceptance.

For Neo4j-in-Rust:

- Treat transaction id/time as a snapshot dimension.
- Benchmark lazy temporal views versus materialized windows.
- Compact deltas into CSR snapshots on thresholds.

### Benchmark Methodology

LDBC Graphalytics gives algorithm correctness fixtures. GAPBS gives verifier style. Gunrock gives hardware counters. Raphtory gives temporal view benchmark shapes. ThunderRW gives memory-cost reporting.

For Neo4j-in-Rust:

- Every algorithm benchmark should record load/projection/compute/output time.
- Every memory-sensitive benchmark should record graph/workspace/result/temporary bytes.
- Correctness should use semantic equality.

## Recommended Rust Architecture Sketch

```rust
struct GraphProjectionSpec {
    node_label_filter: LabelFilter,
    relationship_type_filter: RelationshipTypeFilter,
    direction: ProjectionDirection,
    weight_property: Option<PropertyKey>,
    build_inverse: bool,
}

struct GraphProjection {
    snapshot_id: u64,
    spec: GraphProjectionSpec,
    id_map: DenseIdMap,
    csr: Arc<CsrSnapshot>,
    csc: Option<Arc<CscSnapshot>>,
    memory: MemoryEstimate,
}

enum Frontier {
    Sparse(Vec<u32>),
    Bitmap(FixedBitSet),
    DenseRange { start: u32, end: u32 },
}

trait GraphAlgorithm {
    type Options;
    type Workspace;
    type Output;

    fn estimate_workspace_memory_bytes(
        graph: &GraphProjection,
        options: &Self::Options,
    ) -> MemoryEstimate;

    fn create_workspace_for_graph(
        graph: &GraphProjection,
        options: &Self::Options,
    ) -> Self::Workspace;

    fn run_algorithm_with_workspace(
        graph: &GraphProjection,
        options: &Self::Options,
        workspace: &mut Self::Workspace,
    ) -> Self::Output;
}
```

The names above are illustrative rather than final. The key architecture is projection -> compact snapshot -> explicit workspace -> semantic output.

## Apply/Do-Not-Apply Matrix

| Pattern | Use It When | Avoid It When | Main Memory Cost |
|---|---|---|---|
| CSR snapshot | Read-heavy analytics | Per-edge transactional mutation | offsets + targets |
| Inverse CSC | Pull/PageRank/bottom-up BFS | Outgoing-only traversals | another offsets + sources |
| Dense renumbering | External ids are sparse | Tiny graphs | id maps |
| Queue frontier | Sparse frontier | Frontier is dense | frontier length |
| Bitmap frontier | Dense frontier | Tiny sparse frontier | V / 8 |
| GraphBLAS backend | Matrix-friendly batch algorithms | Dynamic property predicates | backend matrices/workspaces |
| External-memory shards | Graph exceeds RAM budget | Low-latency small queries | shard buffers + vertex windows |
| GPU backend | Large repeated analytics | One-off small queries | device graph + outputs |
| Temporal deltas | Incremental/windowed views | Static graph | delta logs + materialization |
| Random-walk interleaving | Many long random walks | Simple deterministic traversals | walker/output buffers |

## Coverage Ledger

### Graph Tools Attempted

- `codebase-memory-mcp` indexed `gitrefrepo/gapbs-src` and found GAPBS traversal symbols such as `SlidingQueue`, `QueueBuffer`, `TDStep`, `BUStep`, `DOBFS`, `QueueToBitmap`, and `BitmapToQueue`. These hits were used only to navigate; BFS/queue/builder/PageRank claims were verified by direct source reads.
- `cgc` indexed `gitrefrepo/sprs-src` and found `CsMat`/sparse matrix symbols. These hits were used only to navigate; compressed matrix claims were verified in `sprs/src/sparse.rs`.
- I did not depend on graph-tool output as source of truth.

### Direct Source Reads By Repository

- `cugraph-src`: `cpp/examples/developers/vertex_and_edge_partition/vertex_and_edge_partition.cu`; `cpp/src/c_api/bfs.cpp`.
- `differential-dataflow-src`: `differential-dataflow/src/operators/iterate.rs`.
- `galois-src`: `lonestar/tutorial_examples/SSSPPushSimple.cpp`; `libcusp/include/galois/graphs/DistributedGraph.h`; `libcusp/include/galois/graphs/CuSPPartitioner.h`.
- `gapbs-src`: `src/graph.h`; `src/sliding_queue.h`; `src/bfs.cc`; `src/builder.h`; `src/pr.cc`.
- `graph-csr-openmp-src`: `README.md`; `main.cxx`; `inc/io.hxx`; `inc/Graph.hxx`.
- `graphblas-pointers-src`: `README.md`.
- `graphblas-src`: `Include/GraphBLAS.h`; `Source/serialize/GxB_Matrix_serialize.c`; `Source/convert/GB_convert_int.c`; `Source/convert/GB_convert_sparse_to_hyper_test.c`; `Source/global/GB_Global.c`; `Source/container/GB_unload_into_container.c`.
- `graphblas_sparse_linear_algebra-src`: `context/context.rs`; `context/memory_allocator/memory_allocator.rs`; `collections/sparse_matrix/sparse_matrix.rs`; `collections/serializer/lz4_serializer.rs`.
- `graphchi-cpp-src`: `src/engine/graphchi_engine.hpp`; `src/engine/functional/functional_engine.hpp`; `src/engine/auxdata/vertex_data.hpp`.
- `graphscope-src`: `README.md`; `analytical_engine/README.md`; `analytical_engine/apps/flash/traversal/bfs.h`; `analytical_engine/apps/flash/ranking/pagerank.h`; `interactive_engine/groot-module/src/main/java/com/alibaba/graphscope/groot/coordinator/SnapshotManager.java`; `interactive_engine/executor/engine/pegasus/pegasus/examples/page_rank.rs`.
- `gridgraph-src`: `README.md`; `core/graph.hpp`; `core/bigvector.hpp`.
- `gunrock-src`: `python/src/gunrock/bindings.cu`; `benchmarks/spmv_bench.cu`.
- `igraph-src`: `include/igraph_datatype.h`; `src/graph/adjlist.c`; `src/centrality/pagerank.c`; `src/centrality/closeness.c`; `src/graph/visitors.c`; `src/connectivity/components.c`; `src/paths/dijkstra.c`.
- `lagraph-src`: `src/algorithm/LAGr_BreadthFirstSearch.c`; `src/algorithm/LAGr_PageRankGAP.c`; `src/algorithm/LG_CC_FastSV7.c`.
- `ldbc_graphalytics-src`: BFS, PageRank, and WCC validation tests under `graphalytics-validation`.
- `ldbc_graphalytics_docs-src`: `tex/appendix_data_format.tex`.
- `ldbc_graphalytics_platforms_arcadedb-src`: `SingleSourceShortestPathsComputation.java`; `lsqb/systems/arcadedb.py`.
- `ldbc_graphalytics_platforms_graphblas-src`: `src/main/c/src/algorithms/bfs.cpp`; `src/main/c/src/algorithms/sssp.cpp`; `src/main/c/src/graphio.cpp`; `GraphblasPlatform.java`.
- `ligra-src`: `apps/BFS.C`; `ligra/ligra.h`.
- `minigraph-src`: `README.md`; `minigraph/graphs/immutable_csr.h`; `minigraph/2d_pie/auto_map.h`.
- `nalgebra-src`: `src/sparse/cs_matrix.rs`; `src/base/edition.rs`; `src/sparse/cs_matrix_cholesky.rs`.
- `ndarray-src`: `src/impl_methods.rs`.
- `networkit-src`: `include/networkit/graph/Graph.hpp`; `networkit/cpp/distance/BFS.cpp`; `networkit/cpp/centrality/PageRank.cpp`; `networkit/cpp/components/ParallelConnectedComponents.cpp`; `include/networkit/algebraic/algorithms/AlgebraicBFS.hpp`.
- `petgraph-src`: `crates/petgraph/src/csr.rs`; `crates/petgraph/src/visit/traversal.rs`; `crates/petgraph/src/algo/dijkstra.rs`.
- `python-graphblas-src`: `docs/user_guide/fundamentals.rst`; `docs/user_guide/operators.rst`.
- `raphtory-src`: `raphtory-benchmark/src/common/mod.rs`.
- `rustworkx-src`: `src/link_analysis.rs`.
- `sparsetools-src`: `README.md`; `src/coord.rs`; `src/graph.rs`; `src/csr/csr.rs`; `src/row.rs`.
- `sprs-src`: `sprs/src/sparse.rs`.
- `thunderrw-src`: `README.md`; `util/graph/graph.h`; `util/graph/graph.cpp`; `random_walk/types.h`; `random_walk/uniform_sampling.h`; `random_walk/creeper.h`.
- `timely-dataflow-src`: `timely/src/progress/frontier.rs`; `mdbook/src/chapter_3/chapter_3_1.md`; `mdbook/src/chapter_5/chapter_5_2.md`.

### Gaps And Cautions

- I did not build or run these repositories. This is a source-reading extraction, not a benchmark report.
- Some repositories are very large (`graphscope-src`, `cugraph-src`, `graphblas-src`), so coverage focused on files directly relevant to graph traversal, sparse storage, and benchmark methodology.
- GPU findings are based on bindings/examples/benchmarks, not kernel-level deep dives.
- GraphBLAS findings focus on SuiteSparse/LAGraph behavior visible in headers/source and Graphalytics usage, not the full GraphBLAS specification.
- LDBC validation coverage focused on BFS/PageRank/WCC and platform execution; other algorithms such as CDLP/LCC are not deeply inspected here.
- External-memory findings are architectural and should be validated with actual disk/RSS benchmarks before adoption.

## Final Guidance For Future Agents

1. Start with the projection/snapshot contract, not the algorithm.
2. Demand dense ids and exact memory estimates.
3. Keep CSR/CSC immutable and separate from transactional storage.
4. Implement frontiers as a representation family.
5. Add serial reference algorithms before parallel/GPU/GraphBLAS paths.
6. Treat GraphBLAS and GPU as optional backends behind the same snapshot API.
7. Benchmark load, projection, compute, output serialization, peak memory, and correctness separately.
8. For temporal graphs, benchmark cached/lazy/materialized/windowed views before choosing the storage model.
9. For external-memory graphs, design shard metadata and memory budgets as core APIs.
10. Never claim lower RAM usage without a test that counts graph, mapping, workspace, temporary, backend, and result memory.
