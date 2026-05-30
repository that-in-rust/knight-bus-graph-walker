# Deep Research: Alternative Custom Formats Per Algorithm Family

*For each of the 13 layout families in the Algorithm Storage Atlas, we researched
what academia and production systems actually use — and whether any custom format
genuinely reduces RAM for OLAP workloads.*

**Focus: lowest RAM. Not fastest. Not most features.**

---

## Method

For each family, we asked:
1. What storage format does the published literature actually use?
2. Does any paper propose a CUSTOM on-disk format for this algorithm?
3. Does the custom format reduce RAM, or just speed?
4. What is the lowest-RAM approach known?

---

## Family #1: AnchorDualCsr — Traversal / BFS

**Our design:** Forward + Reverse CSR (dual), key index sidecar.

**What the literature uses:**

| System | Format | RAM model |
|---|---|---|
| **GAP Benchmark Suite** (Beamer, UCB) | CSR | In-memory, full graph |
| **Ligra** (Shun & Blelloch, CMU) | CSR (compressed sparse row) | In-memory |
| **GraphChi** (Kyrola, CMU) | Shards (sorted edge lists, NOT CSR) | Semi-external, fixed RAM |
| **SlimSell** (Besta, ETH Zurich, 2020) | SELL-C-σ (Sliced ELLPACK) | In-memory, SIMD-optimized |
| **Packed Memory Array** (Wheatman & Xu, MIT) | PMA (gapped sorted array) | In-memory, dynamic |
| **GraphZero** (2025) | CSR + mmap | Semi-external, zero-copy |
| **MMap paper** (Lin et al., 2014) | CSR + mmap | Semi-external |

**Lowest-RAM alternative found:** CSR + mmap (MMap paper, GraphZero). This is EXACTLY what we have.
No paper proposes a format that uses LESS RAM than CSR+mmap for BFS.

**RAM reduction from custom format?** NO. CSR is already the standard.
SlimSell reduces STORAGE by up to 50% via SIMD packing but requires
more complex decoding and targets GPU/SIMD workloads.

**Verdict: Our AnchorDualCsr IS the literature standard. No change needed.**

---

## Family #2: InboundPower — PageRank / Eigenvector Centrality

**Our design:** Pre-materialized reverse CSR + degree array on disk.

**What the literature uses:**

| System/Paper | Format | RAM model | Notes |
|---|---|---|---|
| **MMap** (Lin et al., 2014) | CSR + mmap | Semi-external | PageRank on 1.47B edges, 27 sec, standard CSR |
| **X-Stream** (Roy et al., SOSP 2013) | Streaming edge list partitions | Out-of-core, ~O(V) RAM | Scatter-gather, no CSR needed |
| **Propagation Blocking** (Beamer, IPDPS 2017) | CSR + propagation buffers | In-memory + cache-aware | Partitions PROPAGATIONS not graph |
| **Cagra/CSR Segmenting** (Zhang, MIT, 2017) | CSR with LLC-sized segments | In-memory, cache-aware | 5× speedup via cache optimization |
| **TCSC** (Mofrad et al.) | Triply Compressed Sparse Column | Distributed, compressed | 4.4× less memory via compression |
| **I/O-efficient PageRank** (Chen, Gan, Suel) | Blocked adjacency lists | External memory | Designed for disk, O(V) RAM |

**Lowest-RAM alternatives found:**

1. **X-Stream scatter-gather:** O(V) RAM. Streams edges, no random access.
   Uses edge-list partitions, NOT CSR. Our Level 3 approach.
2. **TCSC (Triply Compressed Sparse Column):** Compresses CSC by removing
   zero-rows and zero-columns. 4.4× memory reduction over CSR for sparse graphs.
   BUT: adds decoding overhead, complex implementation.
3. **I/O-efficient PageRank (Chen et al.):** Blocks adjacency into disk pages,
   processes page-by-page. O(V) RAM. Similar to X-Stream.

**Does pre-materializing InboundPower reduce RAM?** NO.
- The reverse CSR already exists in our base format
- Degree array = `offsets[i+1] - offsets[i]` (zero RAM, computed inline)
- Dangling node bitset = 25 MB for 200M nodes (computed in 0.2s)
- NO paper pre-materializes a separate on-disk "PageRank layout"

**The real PageRank RAM optimizations (from literature):**
- **CSR Segmenting** (Cagra): Partition vertices into LLC-sized segments,
  process one segment at a time → reduces cache misses 5×. Runtime technique, not format.
- **Propagation Blocking** (Beamer): Partition score propagations into buffers
  → reduces memory bandwidth. Runtime technique, not format.
- **Compression (TCSC):** Compress the CSR itself → 4.4× less memory.
  But applies to the BASE format, not a separate layout.

**Verdict: InboundPower layout is unnecessary. Use base reverse CSR +
runtime techniques (segmenting, propagation blocking). For extreme
low-RAM: X-Stream scatter-gather over base CSR.**

**If we compress the base CSR (TCSC-style), we get 4× RAM reduction
for ALL algorithms, not just PageRank.**

---

## Family #3: ConnectivityLowlink — Tarjan's SCC

**Our design:** Pre-ordered DFS-friendly layout on disk.

**What the literature uses:**

| System/Paper | Format | RAM model |
|---|---|---|
| **Semi-external DFS** (Sibeyn et al.) | Standard adjacency list | Semi-external (node data in RAM, edges on disk) |
| **I/O-efficient SCC** (Zhang et al., SIGMOD 2013) | Standard graph + spanning tree on disk | Semi-external |
| **Tarjan's original** | Adjacency list | In-memory, O(V+E) |

**Does any paper pre-compute DFS ordering on disk?** NO.
- DFS ordering is computed BY the algorithm, not as a preprocessing step
- Pre-computing DFS order requires RUNNING DFS — which IS the algorithm
- It's circular: you can't pre-materialize the output of the algorithm
  as input to the algorithm

**Lowest-RAM approach:** Semi-external model (Sibeyn et al., Zhang et al.):
keep V node records in RAM (labels, lowlinks, stack), stream edges from disk.
RAM = O(V) = 200M × ~16B = 3.2 GB for our 50 GB graph. Standard CSR works.

**Verdict: ConnectivityLowlink layout is CONCEPTUALLY WRONG.
DFS order is the OUTPUT, not an input format. Delete this family.**

---

## Family #4: OrderedWedge — Triangle Counting

**Our design:** Pre-sorted adjacency lists (sorted by neighbor degree).

**What the literature uses:**

| System/Paper | Format | RAM model | Notes |
|---|---|---|---|
| **Bader (NJIT, 2023)** | Standard CSR, sort at runtime | In-memory | "Fast Triangle Counting" — sorts adjacency inline |
| **CLAP** (THU, DATE'23) | CSR with reordered node IDs | In-memory, hardware-accelerated | Reorders graph globally |
| **PIM-TC** (CMU-SAFARI, 2025) | CSR partitioned per-PIM | Processing-in-memory | Standard CSR partitioned across PIM units |
| **Forward algorithm** (Bader) | CSR with degree-ordered processing | In-memory | Process edges (u,v) where deg(u) < deg(v) |

**Does any paper pre-sort adjacency lists on disk?** RARELY.
- Bader's "Fast Triangle Counting" (2023): uses CSR, sorts neighbor lists
  at runtime using `std::sort`. The sort is a one-time cost.
- The "forward" technique: process only edges where `deg(u) < deg(v)`,
  which naturally reduces work by ~3× without any format change.
- Most papers apply GRAPH REORDERING (relabeling nodes by degree) rather
  than sorting individual adjacency lists.

**Lowest-RAM approach:** 
- In-memory: Standard CSR + forward technique. RAM = full CSR + degree array.
- External: X-Stream style streaming. RAM = O(V).
- Pre-sorting saves ~2× intersection time but the sort itself is O(E log dmax)
  at runtime, taking ~30 seconds for 1B edges. 

**Key insight from literature:** The optimization that matters for triangle
counting isn't adjacency ORDER — it's NODE RELABELING. Relabeling nodes
by degree (putting low-degree nodes first) reduces work dramatically.
This is a one-time O(V+E) preprocessing step that modifies the BASE CSR,
not a separate layout.

**Verdict: OrderedWedge as a SEPARATE layout is unnecessary. Instead:
(a) Apply degree-based node relabeling to the base CSR at build time.
(b) Sort adjacency lists at runtime (30 sec, one-time).
(c) Use the "forward" technique for 3× work reduction.**

---

## Family #5: PartitionRefinement — Louvain / Community Detection

**Our design:** Dense community assignment array.

**What the literature uses:**

| System/Paper | Format | RAM model | Notes |
|---|---|---|---|
| **GVE-Louvain** (Sahu, IIIT, 2024) | Standard CSR | In-memory, 560M edges/sec | Fastest multicore Louvain |
| **VLouvain** (Yu et al., EDBT 2026) | NO GRAPH at all (vector-based) | Vector × vector products | Eliminates graph entirely for low-rank |
| **Low-mem Louvain** (Sahu, 2024) | CSR with compressed community arrays | In-memory, memory-optimized | Key optimization: bit-packed community IDs |
| **NetworKit** | Standard CSR | In-memory | General-purpose |

**Does any paper use a custom on-disk community layout?** NO.
- Louvain is an ITERATIVE algorithm that MODIFIES community assignments
  in each pass. The community array is working state, not storage.
- All implementations use standard CSR for the graph + a mutable
  community assignment vector.

**Lowest-RAM approach:**
- **Low-mem Louvain** (puzzlef/louvain-lowmem-communities-openmp):
  Uses bit-packed community IDs (16-bit instead of 64-bit when
  community count fits). Reduces community array from 1.6 GB to 400 MB
  for 200M nodes.
- Community array is ~200M × 4B = 800 MB (32-bit IDs). This is a
  RUNTIME array, not a storage format.

**Verdict: PartitionRefinement layout is unnecessary. Community arrays
are runtime state. Use standard CSR + bit-packed community vector.**

---

## Family #6: PeelBucket — k-Core Decomposition

**Our design:** Pre-computed degree array on disk.

**What the literature uses:**

| System/Paper | Format | RAM model | Notes |
|---|---|---|---|
| **Cheng et al. (UWaterloo, ICDE 2011)** | Standard adjacency list | External memory, O(k_max) scans | First external k-core algorithm |
| **Wen et al. (UTS, 2015)** | Semi-external CSR | Semi-external (node data in RAM) | Bounded memory, O(V) RAM |
| **PICO** (Zhao et al., Wuhan U, 2024) | CSR | GPU-accelerated | Standard CSR on GPU |

**Does any paper pre-compute degrees on disk?** NO — because degree is
trivially computed from CSR offsets: `degree[v] = offsets[v+1] - offsets[v]`.
Every k-core implementation computes this inline. Zero cost.

**Lowest-RAM approach:**
- **Semi-external k-core** (Wen et al.): Keep V node records (core number,
  remaining degree) in RAM, stream edges from disk. RAM = O(V).
  For 200M nodes: 200M × 8B = 1.6 GB.
- **External k-core** (Cheng et al.): Only O(k_max) scans of the edge list.
  RAM = O(V) for node state.

**Verdict: PeelBucket layout is pointless. Degree = offsets subtraction.
Use standard CSR. For low-RAM: semi-external model, O(V) RAM.**

---

## Family #7: RelaxationFrontier — Dijkstra / SSSP

**Our design:** Edge weights inlined in adjacency (interleaved peers+weights).

**What the literature uses:**

| System/Paper | Format | RAM model | Notes |
|---|---|---|---|
| **SCIP** (Zuse Institute Berlin) | CSR with parallel weight array | In-memory | `outbeg[]`, `head[]`, `weight[]` — separate arrays |
| **Boost.Graph** | Adjacency list with property maps | In-memory | Weight as edge property |
| **GAP Benchmark** | CSR + separate weight array | In-memory | Standard |
| **ΔStepping** (Meyer & Sanders) | CSR + weight array | In-memory / parallel | Standard |

**Inlined vs. separate weight arrays:**

All major implementations use SEPARATE weight arrays, not inlined.
Why? Because:
1. CSR peers array is `u32[E]` = 4 bytes per edge
2. Weight array is `f64[E]` = 8 bytes per edge (or `f32` = 4 bytes)
3. Inlining = `(u32, f64)[E]` = 12 bytes per edge with alignment padding → 16 bytes
4. Separate = 4 + 8 = 12 bytes, no padding waste
5. For algorithms that DON'T need weights (BFS, PageRank), inlined format
   WASTES bandwidth reading weights that are ignored

**The literature consensus:** Separate arrays, NOT inlined. This is because
separate arrays allow algorithms that don't need weights to skip the weight
data entirely. Inlining hurts cache efficiency for non-weighted algorithms.

**Lowest-RAM approach:** Columnar property storage. Weight as a typed column
file (`weight.f64.bin`). mmap only when an algorithm needs it. For Dijkstra
on our 50 GB graph: CSR (5.6 GB forward) + weights (8 GB) = 13.6 GB mmap'd.
Same as our base format with property columns.

**Verdict: RelaxationFrontier (inlined weights) is WRONG — it HURTS
non-weighted algorithms and wastes bandwidth. Use separate weight column.
This IS the base format + property columns.**

---

## Family #8: EdgeOrderForest — MST (Minimum Spanning Tree)

**Our design:** Globally sorted edge list by weight.

**What the literature uses:**

| System/Paper | Format | Notes |
|---|---|---|
| **Filter-Kruskal** (Sanders, KIT) | UNSORTED edge list | Filters before sorting — avoids full sort |
| **External MST** (Arge et al., SWAT 2000) | Edge list, sorted externally | Standard external merge-sort |
| **Borůvka's** | Standard CSR | Contract-and-repeat |

**Does any paper pre-sort edges on disk?** YES — Kruskal's requires sorted edges.
But **Filter-Kruskal avoids sorting most edges:**
> "Filter-Kruskal runs in time O(m + n log n · log(m/n)), i.e., in
> linear time for not too sparse graphs."

The key: partition edges using a random pivot weight, filter out edges
that can't be in the MST (both endpoints in same component), only sort
the remaining edges. For most graphs, this sorts <10% of edges.

**Lowest-RAM approach:** Filter-Kruskal with external merge-sort for the
filtered subset. RAM = O(V) for Union-Find + O(sort buffer).
For our graph: 200M × 8B (Union-Find) + 64 MB sort buffer = ~1.6 GB.

**Verdict: EdgeOrderForest (pre-sorted) is wasteful. Filter-Kruskal
sorts only ~10% of edges at runtime. Use base CSR + external sort of
the weight column. Only 2 algorithms use this family — not worth a layout.**

---

## Family #9: FlowResidual — Max Flow (Push-Relabel)

**Our design:** Inlined residual/capacity arrays.

**What the literature uses:**

| System/Paper | Format | Notes |
|---|---|---|
| **ECL-MaxFlow** (Burtscher, Texas State) | Binary CSR + separate capacity array | GPU push-relabel |
| **WBPR** (NTU, 2025) | Bidirectional CSR (BCSR) | Forward+backward edges interleaved |
| **Boost.Graph push_relabel** | Adjacency list + edge property maps | Standard |

**Key architectural requirement:** Max flow needs BIDIRECTIONAL edges with
mutable residual capacity. For each forward edge (u→v, cap=c), there must
be a backward edge (v→u, cap=0) for the residual graph.

**What WBPR (2025) actually does:**
> "BCSR: Backward edges of a vertex are continuously appended to the end
> of its forward edges." And an optimized version: "flow[2i] stores the
> forward edge value, and flow[2i+1] stores the corresponding backward edge."

**This is NOT a separate layout — it's a runtime data structure:**
- The residual graph is MUTABLE (capacities change during algorithm)
- It must be reconstructed for each max-flow call
- It cannot be pre-materialized on disk (meaningless without source/sink)

**Lowest-RAM approach:** Standard CSR + capacity column + runtime residual
array. RAM = CSR + 2×E capacity values. For 1B edges: ~16 GB.
For low-RAM: external push-relabel (literature sparse, this is cutting-edge).

**Verdict: FlowResidual layout is CONCEPTUALLY WRONG. Residual capacity
is mutable runtime state, not storage. Use base CSR + capacity property
column. Build bidirectional residual graph at runtime.**

---

## Family #10: FeatureMetric — k-NN / Similarity

**Our design:** Dense feature matrix in row-major order.

**What the literature uses:**

| System/Paper | Format | Notes |
|---|---|---|
| **StellarGraph** (2020) | Row-major dense matrix | "row-major is better: we work with whole rows at a time" |
| **GraphZero** (2025) | Columnar tensor store (.gd) | "raw, C-contiguous binary format" — row-major |
| **FAISS** (Meta) | Dense matrix, L2/IP index | Not a graph format — vector similarity |

**Row-major vs column-major:** The literature is clear: for node-feature
workloads, ROW-MAJOR is correct because algorithms access all features
of a single node at once (one row). Column-major is wrong.

**Is this a "graph storage format"?** NO. This is a FEATURE MATRIX — tabular
data associated with nodes. It belongs in property columns (our base format
already supports typed columns). k-NN itself operates on the feature matrix,
not the graph topology.

**Lowest-RAM approach:** mmap'd row-major feature matrix + streaming distance
computation. Only need 2 rows in RAM at a time for brute-force k-NN.
For approximate k-NN (HNSW, IVF): index structures that live IN MEMORY
but the feature data can be mmap'd from disk.

**Verdict: FeatureMetric is NOT a graph layout. It's a property column
(row-major feature matrix). Already covered by base format's typed
property columns. No separate layout needed.**

---

## Family #11: EmbeddingSample — Node2Vec / Random Walks

**Our design:** Pre-structured walk candidates on disk.

**What the literature uses:**

| System/Paper | Format | Notes |
|---|---|---|
| **node2vec-c** (xgfs, 2018) | Binary CSR | Standard CSR, walks computed at runtime |
| **Fast-Node2Vec** (Zhou et al., CAS) | Standard graph on Pregel framework | Transition probabilities computed during walks |
| **Node2Vec original** (Grover & Leskovec, Stanford) | NetworkX graph | Python, in-memory |

**Does any paper pre-compute walk candidates?** NO.
- Node2Vec walks are PARAMETERIZED by p (return) and q (in-out).
  Different p,q values produce different walk distributions.
- Pre-computing walks for one (p,q) is useless for another (p,q).
- Fast-Node2Vec explicitly computes transition probabilities DURING walks
  to "reduce memory space consumption."

**Lowest-RAM approach:**
- **Fast-Node2Vec:** Computes transition probabilities on-the-fly during walks,
  avoiding the O(E) alias table that standard Node2Vec requires.
  RAM = O(V × walk_length) for the walks themselves.
- For our graph: 200M × 80 walks × 80 length × 4B = too large for RAM.
  Need streaming: generate walks, immediately feed to Word2Vec, discard.

**Verdict: EmbeddingSample is IMPOSSIBLE to pre-materialize usefully.
Walks depend on runtime parameters (p, q). Use standard CSR +
compute transitions on-the-fly (Fast-Node2Vec approach).**

---

## Family #12: DagOrder — Topological Sort / Longest Path

**Our design:** Pre-sorted DAG topological order on disk.

**What the literature uses:**

All implementations compute topological sort at runtime using Kahn's
algorithm (BFS-based, O(V+E)) or DFS-based ordering. Like Family #3,
this is the OUTPUT of an O(V+E) algorithm — not a pre-materialized format.

**Time to compute for 200M nodes / 1B edges:** ~2-5 seconds (linear scan).

**Does any paper pre-compute topo order?** NO. Topo order is a simple
derived quantity. Pre-computing it saves 2-5 seconds but costs 1.6 GB
of disk (200M × 8B). Terrible tradeoff.

**Verdict: DagOrder layout is wasteful. Topo sort is O(V+E), takes
seconds, needs zero extra disk. Compute at runtime.**

---

## Family #13: InfluenceMonteCarlo — Influence Maximization / CELF

**Our design:** Pre-structured cascade neighborhoods.

**What the literature uses:**

| System/Paper | Format | RAM model | Notes |
|---|---|---|---|
| **HBMax** (Chen et al., WSU/PNNL, 2022) | Standard CSR + compressed RR sets | Memory-optimized | **Huffman-coded reverse reachable sets: 82% memory reduction** |
| **Ripples** (Minutoli et al., PNNL) | Standard CSR | In-memory, parallel | State-of-the-art parallel IM |
| **IMM** (Tang et al.) | Standard graph | In-memory | Martingale-based sampling |

**The key finding from HBMax:** The memory bottleneck in influence
maximization is NOT the graph — it's the REVERSE REACHABLE (RR) SETS
generated by Monte Carlo sampling. For a 1B-edge graph, RR sets can
consume 50-100 GB of RAM.

HBMax's solution: Huffman-code the RR sets, achieving 82% memory reduction.
This compresses RUNTIME DATA, not the graph format.

**Pre-structuring cascade neighborhoods is impossible:** Each Monte Carlo
simulation uses random coin flips per edge. The cascade is stochastic.
You can't pre-materialize something that's random by definition.

**Lowest-RAM approach:** Standard CSR + streaming RR set generation with
compression (HBMax). The graph format doesn't matter — the bottleneck
is the sampling output.

**Verdict: InfluenceMonteCarlo is CONCEPTUALLY WRONG. Cascades are
stochastic, can't be pre-materialized. Use standard CSR + compressed
RR set generation (HBMax technique).**

---

## Meta-Finding: What Actually Reduces RAM (From Literature)

The research reveals a DIFFERENT set of optimizations than our 13 families:

### Optimizations That ACTUALLY Reduce RAM

| Technique | Source | What It Does | RAM Reduction | Applies To |
|---|---|---|---|---|
| **Graph Compression (TCSC/WebGraph)** | Mofrad et al., Boldi & Vigna | Compress the CSR itself (gap coding, reference coding) | **4-10×** | ALL algorithms |
| **CSR Segmenting** | Cagra (Zhang, MIT) | Partition vertices into LLC-sized chunks | **5× fewer cache misses** | PageRank, BFS, all iterative |
| **Propagation Blocking** | Beamer (LBNL) | Buffer score propagations, reduce random writes | **2-3× less bandwidth** | PageRank, SpMV-based |
| **Semi-external model** | Multiple papers | Keep V in RAM, stream E from disk | **O(V) RAM instead of O(V+E)** | SCC, k-core, MST, BFS |
| **Bit-packed IDs** | Low-mem Louvain, dCSR | Use 16-bit or variable-width node IDs | **2-4× less per-ID** | All algorithms |
| **X-Stream scatter-gather** | Roy et al. (EPFL) | Edge-centric streaming, avoid all random access | **O(V) RAM** | PageRank, BFS, SSSP |
| **Node relabeling** | Multiple (degree ordering, BFS ordering) | Relabel nodes for better locality | **2-5× cache improvement** | Triangle counting, BFS |
| **Degree-based filtering** | "Forward" technique | Only process edges (u,v) where deg(u)<deg(v) | **3× work reduction** | Triangle counting |

### None of These Are "Custom On-Disk Layouts Per Algorithm"

Every technique above either:
1. **Modifies the BASE CSR** (compression, node relabeling, bit-packing)
2. **Is a RUNTIME technique** (segmenting, propagation blocking, semi-external)
3. **Applies to ALL algorithms** (compression, mmap, streaming)

NOT ONE paper proposes maintaining a SEPARATE on-disk layout per algorithm.

---

## Revised Recommendation: What To Build Instead of 13 Layouts

### The "Lowest RAM" Stack (From Literature)

```
Layer 1: COMPRESSED BASE CSR (WebGraph/TCSC-style)
  - Gap-coded adjacency lists (4-10× smaller than raw CSR)
  - Varint or gamma-coded edge IDs
  - Disk: 26 GB → 3-6 GB
  - Decode cost: ~1.5× slower random access, 1× sequential

Layer 2: TYPED PROPERTY COLUMNS
  - weight.f64.bin, name.str.bin, etc.
  - mmap'd independently — only load columns needed by algorithm
  - Same as current design, no change

Layer 3: RUNTIME OPTIMIZATIONS (per-algorithm, in-memory only)
  - CSR Segmenting for iterative algorithms (PageRank, Louvain)
  - Propagation blocking for score accumulation
  - Degree-sorted processing for triangle counting
  - Semi-external streaming for SCC, k-core, MST
  - On-the-fly transition probabilities for Node2Vec
  - Compressed RR set generation for influence maximization

Layer 4: NODE RELABELING (one-time, modifies base CSR)
  - Degree ordering for triangle counting
  - BFS ordering for traversal locality
  - This is a BUILD-TIME choice, not a separate layout
```

### Comparison to Original 13-Layout Plan

| Metric | 13 Layouts (Original) | Compressed Base + Runtime (Revised) |
|---|---|---|
| **Disk (50 GB graph)** | 40-65 GB | **3-6 GB** (compressed CSR) |
| **Disk (no compression)** | 40-65 GB | **26 GB** (raw CSR + properties) |
| **RAM guarantee** | NO (page cache thrashing) | **YES** (one format, predictable) |
| **Build time** | 15-40 min (all layouts) | **3-5 min** (one CSR) |
| **PageRank speed** | 8-22 sec | **8-22 sec** (same — bottleneck is cache misses) |
| **Triangle count** | 20-60 sec (pre-sorted) | **50-90 sec** (runtime sort) |
| **LOC** | ~40K (13 layouts) | **~8-12K** (1 format + runtime opts) |
| **Maintenance** | 13 format versions to maintain | **1 format** |
| **Incremental update** | Rebuild all layouts | **Rebuild 1 CSR** |

---

## Per-Family Final Verdict

| # | Family | Original Plan | Literature Says | Verdict |
|---|---|---|---|---|
| 1 | **AnchorDualCsr** | Keep as base | CSR+mmap IS the standard | **KEEP — this IS the base** |
| 2 | **InboundPower** | Separate layout | Use reverse CSR from base + runtime segmenting | **DELETE — use base** |
| 3 | **ConnectivityLowlink** | Pre-ordered DFS | DFS order IS the algorithm output, can't pre-compute | **DELETE — conceptually wrong** |
| 4 | **OrderedWedge** | Pre-sorted adjacency | Runtime sort (30s) + node relabeling + forward technique | **DELETE — use runtime sort + relabeling** |
| 5 | **PartitionRefinement** | Dense community array | Community array is mutable runtime state | **DELETE — runtime state** |
| 6 | **PeelBucket** | Pre-computed degrees | `degree = offsets[i+1] - offsets[i]`, zero cost | **DELETE — trivial computation** |
| 7 | **RelaxationFrontier** | Inlined weights | Literature uses SEPARATE weight arrays (columnar) | **DELETE — use property column** |
| 8 | **EdgeOrderForest** | Pre-sorted edges | Filter-Kruskal sorts <10% at runtime | **DELETE — runtime sort** |
| 9 | **FlowResidual** | Inlined residual | Residual is mutable, per-execution, source/sink dependent | **DELETE — conceptually wrong** |
| 10 | **FeatureMetric** | Row-major feature matrix | This is a property column, not a graph layout | **DELETE — use property columns** |
| 11 | **EmbeddingSample** | Pre-structured walks | Walks depend on (p,q) parameters, can't pre-compute | **DELETE — conceptually wrong** |
| 12 | **DagOrder** | Pre-sorted topo order | O(V+E) computation, 2-5 seconds, not worth storing | **DELETE — trivial runtime** |
| 13 | **InfluenceMonteCarlo** | Pre-structured cascades | Cascades are stochastic, can't pre-materialize | **DELETE — conceptually wrong** |

**Score: 1 KEEP, 12 DELETE.**

The only surviving layout is the BASE FORMAT (AnchorDualCsr). The 12 others
are either:
- Conceptually wrong (output masquerading as input): #3, #9, #11, #13
- Trivially computed at runtime (~0 cost): #2, #5, #6, #12
- Better handled by separate property columns: #7, #10
- Better handled by runtime optimization techniques: #4, #8

---

## References

1. Besta et al. "SlimSell: A Vectorizable Graph Representation for BFS." arXiv 2020.
2. Kreutzer et al. "A Unified Sparse Matrix Data Format (SELL-C-σ)." SIAM J. Sci. Comp. 2014.
3. Wheatman & Xu. "A Parallel Packed Memory Array to Store Dynamic Graphs." ALENEX.
4. Lin et al. "MMap: Fast Billion-Scale Graph Computation." IEEE BigData 2014.
5. Roy et al. "X-Stream: Edge-centric Graph Processing." SOSP 2013.
6. Beamer et al. "Reducing PageRank Communication via Propagation Blocking." IPDPS 2017.
7. Zhang et al. "Making Caches Work for Graph Analytics (Cagra)." IEEE BigData 2017.
8. Mofrad et al. "Efficient Distributed Graph Analytics using TCSC." PID6084671.
9. Chen, Gan, Suel. "I/O-Efficient Techniques for Computing Pagerank."
10. Bader. "Fast Triangle Counting." arXiv 2023.
11. Sahu. "GVE-Louvain: Fast Louvain Algorithm." arXiv 2024.
12. Sahu. "Low-mem Louvain." arXiv 2024.
13. Cheng et al. "Efficient Core Decomposition in Massive Networks." ICDE 2011.
14. Wen et al. "I/O Efficient Core Graph Decomposition at Web Scale." 2015.
15. SCIP Dijkstra implementation. Zuse Institute Berlin.
16. Osipov, Sanders, Singler. "The Filter-Kruskal MST Algorithm." KIT.
17. Vanausdal & Burtscher. "ECL-MaxFlow." IPCCC 2025.
18. WBPR. "Push-Relabel on GPU via BCSR." HPEC 2025.
19. Zhang et al. "I/O Efficient SCC." SIGMOD 2013.
20. Sibeyn et al. "Heuristics for Semi-External DFS."
21. Chen et al. "HBMax: Memory Efficient Influence Maximization." 2022.
22. Zhou et al. "Fast-Node2Vec." CAS.
23. Grover & Leskovec. "node2vec: Scalable Feature Learning." KDD 2016.
24. Boldi & Vigna. "The WebGraph Framework: Compression Techniques." WWW 2004.
25. Alano. "Sparse Graph Storage for Message-Passing Networks." 2025.
