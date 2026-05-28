# Low-RAM OLAP Format Variants For A Rust Neo4j Rewrite

Date: 2026-05-26

## Premise Check

The product question is not "can we invent clever graph formats?" The product
question is:

> Can a Rust Neo4j-compatible system make a `50 GB` logical graph feel practical
> on much smaller machines, for analytics, without changing the Neo4j-facing
> user experience?

For that goal, "lowest RAM" must mean the business-visible machine envelope:

- heap and allocator-owned memory
- mmapped pages that become resident under load
- OS page cache pressure
- peak build, compaction, and algorithm scratch memory
- whether the box swaps, OOMs, or becomes unusably slow

The local repo already proves the direction of travel for narrow traversal:
Knight Bus `v002` keeps the Rust runtime in an immutable dual-CSR plus `mmap`
shape, measures process RSS, and shows lower runtime RAM than Neo4j on the
tested `1 MB`, `50 MB`, and `2 GB` datasets
([README.md:13](../README.md#L13),
[README.md:21](../README.md#L21),
[README.md:64](../README.md#L64)).
But those tests are fixed-hop traversal, not full Neo4j Graph Data Science
coverage.

The important correction to the original `13 persistent layout families`
thesis is:

> The RAM-first OLAP architecture should not materialize one durable graph layout
> per algorithm family. It should materialize a tiny number of durable byte
> families and move most algorithm-specific work into runtime profiles,
> bounded scratch, and result sidecars.

This document also folds in the conversation-supplied "1 keep, 12 delete"
critique of the original Atlas. I treat that critique as a design hypothesis to
verify, not as an external fact: each proposed durable layout must prove a
RAM-saving reason to exist beyond `base + property plane + bounded scratch`.

Neo4j itself gives the strongest precedent for this split. Neo4j GDS says its
algorithms run on projected graphs in an in-memory catalog and that the graph is
stored with compressed structures optimized for topology and property lookup
([GDS introduction](https://neo4j.com/docs/graph-data-science/current/introduction/),
[GDS graph management](https://neo4j.com/docs/graph-data-science/current/management-ops/)).
That means the fair OLAP baseline is **Neo4j + GDS projection**, not Cypher over
the transactional record store.

## Expert Lenses

| lens | what it protects against |
| --- | --- |
| `Database-kernel lens` | confusing OLTP correctness storage with OLAP execution storage |
| `Analytical-storage lens` | optimizing heap while ignoring page cache, mmap residency, and build peaks |
| `Graph-algorithm lens` | mistaking runtime scratch or algorithm output for persistent input layout |
| `Operator lens` | designing something that benchmarks well but cannot run on an `8 GB` box without swap storms |
| `Skeptical-engineer lens` | overbuilding exotic formats before proving that the base compressed CSR is insufficient |

## Neo4j Code Reality

Neo4j's record storage is good OLTP engineering, not the lowest-RAM OLAP target.
The local reference repo under `gitrefrepo/neo4j-src` shows why.

| Neo4j code fact | local evidence | OLAP implication |
| --- | --- | --- |
| Node records are fixed records with relationship and property pointers. | `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/NodeRecordFormat.java:31-32` defines a `15` byte node record with next relationship, next property, labels, and dense bit. | This is compact for mutable records, but analytics still has to turn node neighborhoods into cursor walks. |
| Relationship records are pointer-rich. | `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/RelationshipRecordFormat.java:31-35` defines a `34` byte record with endpoints, type, next/prev chain pointers, next property, and markers. | A raw unweighted CSR peer entry is commonly `4` bytes. Neo4j is paying for updateability and bidirectional chain maintenance. |
| Property records are separate fixed records with block decoding. | `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/PropertyRecordFormat.java:37-39` defines a `41` byte property record. | Analytics should avoid decoding generic property records when it only needs `weight.f32`, `community.u32`, or `feature.f32[]`. |
| Sparse relationship traversal follows chains; dense nodes go through relationship groups. | `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/internal/recordstorage/RecordRelationshipTraversalCursor.java:95-167` chooses chain vs group traversal, loads relationship records, and filters by selection. `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/internal/recordstorage/RecordRelationshipTraversalCursor.java:171-245` shows the dense-node group state machine. | Correct and flexible, but not the same hot loop as `offsets[v]..offsets[v+1]` over dense peer arrays. |
| Dense-node grouping threshold defaults to `50`. | `gitrefrepo/neo4j-src/community/configuration/src/main/java/org/neo4j/configuration/GraphDatabaseSettings.java:684-688`. | Neo4j has already specialized high-degree nodes for OLTP traversal, but still within a mutable record-store model. |

Neo4j memory configuration also makes the business-memory problem explicit:
the official settings guidance recommends reserving OS memory, giving JVM heap
enough space for transaction/query state, and allocating the rest to page cache
([Neo4j configuration settings](https://neo4j.com/docs/operations-manual/current/configuration/configuration-settings/)).
For an `8 GB` box, that leaves little room for both a transactional database,
a large GDS projection, and heavy algorithm scratch.

## GDS Code Reality

The missing reference repo is now cloned at
`gitrefrepo/neo4j-gds-src`. That matters because the fair OLAP
baseline is not just "Neo4j docs say GDS is projected"; the GDS source itself
shows a separate projected graph plane with CSR-like stores, compressed
adjacency implementations, a graph catalog, and explicit memory estimation.

| GDS code fact | local evidence | implication for Knight Bus |
| --- | --- | --- |
| GDS builds a `CSRGraphStore` from projected nodes and imported relationships. | `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/api/CSRGraphStoreFactory.java:67-86` constructs a `GraphStoreBuilder` with `nodes`, `relationshipImportResult`, schema, capabilities, and read concurrency. | Neo4j already separates OLAP projection from OLTP record traversal. Our Rust OLAP layer should compete with GDS projection, not only Cypher. |
| The projected graph store holds an ID map, relationship stores, node properties, graph properties, schema, and timestamps. | `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/loading/CSRGraphStore.java:79-150`. | A faithful replacement needs a projection catalog/data model, not just raw CSR files. |
| GDS stores projected graphs in a user/database/name catalog. | `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java:50-55` defines static catalog state; `GraphStoreCatalog.java:84-124` resolves graphs by user/database/name and handles ambiguity. | Knight Bus should have explicit snapshot/catalog identity and freshness metadata, not anonymous mmap files. |
| GDS projection import publishes graph stores to the catalog after building nodes, relationships, and schema. | `gitrefrepo/neo4j-gds-src/triplet-graph-builder/src/main/java/org/neo4j/gds/projection/GraphImporter.java:173-198`. | The Rust architecture needs an atomic projection publish step that resembles a catalog update. |
| GDS has compressed adjacency list implementations using pages, per-node degree arrays, and offsets. | `CompressedAdjacencyList.java:44-86` estimates compressed pages, degrees, and offsets; `CompressedAdjacencyList.java:101-111` stores `byte[][] pages`, `HugeIntArray degrees`, and `HugeLongArray offsets`. | The `CompressedDualCsrBaseV1` recommendation is not exotic; it is close to the shape GDS already optimizes around, except ours is durable/mmap-first. |
| GDS also has packed adjacency implementations with pages, degrees, and offsets. | `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/compression/packed/PackedAdjacencyList.java:33-40`. | Compression/packing should be a base-format option or hot artifact, not thirteen separate persistent layouts. |
| GDS exposes memory estimation that combines graph projection and algorithm memory. | `gitrefrepo/neo4j-gds-src/executor/src/main/java/org/neo4j/gds/executor/MemoryEstimationExecutor.java:80-153`. | Our benchmark plan must measure graph projection memory and algorithm scratch separately, matching the way GDS users reason about feasibility. |
| GDS adjacency access is cursor-based over degree and target IDs, with relationship properties still a design concern. | `gitrefrepo/neo4j-gds-src/core-api/src/main/java/org/neo4j/gds/api/AdjacencyList.java:25-31` and `AdjacencyList.java:40-62`. | This supports the three-format split: topology cursor, property plane, bounded scratch. |

This strengthens the conclusion rather than weakening it. GDS is already much
closer to `base topology + properties + algorithm scratch` than to "one OLTP
record store for everything." The Rust opportunity is to make that projection
durable, mmap-native, lower-overhead, and usable on smaller machines while
keeping the Neo4j-facing API unchanged.

## Candidate Format Variants

### Option A: One Persistent Format

Build only one durable OLAP projection:

```text
CompressedDualCsrBaseV1
  manifest.json
  node_id_map.*
  fwd.offsets.*
  fwd.peers.*
  rev.offsets.*
  rev.peers.*
  optional compression metadata
```

This is the purest RAM-first strategy. It maximizes reuse, minimizes disk, and
keeps the open path simple. It is also closest to the current Knight Bus proof:
immutable dual CSR plus `mmap`.

The downside is that weighted algorithms, feature algorithms, filtered
algorithms, and result management quickly need separate planes anyway. If those
are forced into the single topology artifact, the one-format design becomes a
large, leaky, semi-columnar format by accident.

Verdict: good minimum prototype, too cramped for a Neo4j replacement.

### Option B: Three Persistent Format Families

Build three durable byte families:

| family | purpose | RAM-first rule |
| --- | --- | --- |
| `CompressedDualCsrBaseV1` | topology and dense IDs | mmap by default; optionally compress peer lists; keep forward and reverse topology as the only always-built graph shape |
| `MmapColumnarPropertyPlaneV1` | typed node/edge/feature properties | one file per needed property or feature block; map only columns required by the query or algorithm |
| `BoundedScratchAndSidecarV1` | runtime scratch and persisted outputs | scratch has explicit memory budgets and spill paths; outputs are sidecars, not mutations of the base projection |

Allow an optional fourth tier:

| family | purpose | RAM-first rule |
| --- | --- | --- |
| `HotMaterializedArtifactV1` | repeated hot algorithm acceleration | build only after usage proves the artifact saves more RAM/time than it costs in disk, build memory, and invalidation complexity |

This keeps the durable format count low while still giving algorithms the bytes
they actually need. It also fits the repo's current OLTP/OLAP estimate:
`RAM-optimized` is modeled as one generic CSR-style projection with minimal
sidecars, while `Latency-optimized` adds hot specialized views
([A-20260525203023-oltp-olap-e2e-benchmark-estimates.md:25](../docs/strategic-research/A-20260525203023-oltp-olap-e2e-benchmark-estimates.md#L25),
[A-20260525203023-oltp-olap-e2e-benchmark-estimates.md:95](../docs/strategic-research/A-20260525203023-oltp-olap-e2e-benchmark-estimates.md#L95)).

Verdict: best risk-adjusted architecture for lowest RAM.

### Option C: Thirteen Persistent Algorithm Layout Families

The Atlas lists `13` layout families spanning `60` algorithms
([KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.md:88](../docs/KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.md#L88)).
That is useful as an algorithm vocabulary, but too heavy as the default
RAM-first storage plan.

Persistent per-family layouts create:

- disk amplification
- build-time memory amplification
- invalidation and freshness complexity
- duplicated topology bytes
- confusing product behavior when users ask why one algorithm is fresh and
  another is stale

The Atlas itself already warns that property-only workloads and
compute-dominated workloads should be downgraded or kept out of topology-shaped
snapshots
([KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.md:82](../docs/KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.md#L82)).

Verdict: keep as execution taxonomy and benchmark menu, not as default storage.

## Chosen Thesis

Choose **three persistent OLAP format families**, not one and not thirteen:

1. `CompressedDualCsrBaseV1`
2. `MmapColumnarPropertyPlaneV1`
3. `BoundedScratchAndSidecarV1`

Use `HotMaterializedArtifactV1` only as an opt-in or auto-promoted tier after
real usage shows repeated hot algorithms.

This gives the clearest product promise:

> Neo4j-compatible frontend, Neo4j-like OLTP truth, but a RAM-first OLAP plane
> that avoids loading every projected graph and every algorithm artifact into
> heap.

## Per-Family Verdict

| Atlas family | durable RAM-first home | reason |
| --- | --- | --- |
| `AnchorDualCsrLayoutV1` | `CompressedDualCsrBaseV1` | This is the base topology format. Traversal, BFS, DFS, degree, and simple random walk all start here. |
| `InboundPowerLayoutV1` | runtime profile over base + scratch | PageRank/HITS/Eigenvector need inbound slices and score vectors. The inbound topology is already in the base reverse CSR; score arrays belong in bounded scratch. |
| `ConnectivityLowlinkLayoutV1` | runtime scratch over base | DFS numbers, lowlinks, parent arrays, stacks, and union-find are algorithm state. Persisting them as input confuses output with storage. |
| `OrderedWedgeLayoutV1` | runtime profile; optional hot artifact | Sorted intersections matter, but the RAM-first version should use base adjacency, degree ordering, node relabeling, and bounded pair heaps. Persist only if triangle/similarity is repeatedly hot. |
| `PartitionRefinementLayoutV1` | runtime scratch + result sidecar | Community IDs, gains, cuts, and coarsening state are mutable algorithm state. Final partitions are result sidecars or input property planes for later metrics. |
| `PeelBucketLayoutV1` | runtime scratch | Degree is derivable from offsets for raw CSR. Buckets, colors, and remaining-degree arrays are execution state. |
| `RelaxationFrontierLayoutV1` | property plane + scratch | Weights and heuristics are typed property columns. Distances, predecessors, queues, heaps, and buckets are scratch or result sidecars. |
| `EdgeOrderForestLayoutV1` | runtime external sort; optional hot artifact | MST can use streamed/external edge sorting and union-find. A persisted sorted edge artifact is useful only if MST workloads are frequent. |
| `FlowResidualLayoutV1` | property plane + scratch | Residual capacity is mutable and source/sink dependent. Persist capacities/costs as columns; build residual state per run. |
| `FeatureMetricLayoutV1` | `MmapColumnarPropertyPlaneV1` | KNN/K-Means/HDBSCAN are feature-matrix workloads. They need mmap row-major or Arrow-like typed arrays, not graph topology layouts. |
| `EmbeddingSampleLayoutV1` | base + property plane + streaming scratch | Walks and samples depend on parameters and training configuration. Precomputing them by default fights the RAM-first goal. |
| `DagOrderLayoutV1` | runtime scratch; optional result sidecar | Topological order is cheap, linear derived state. Persist only if a workflow repeatedly reuses the same DAG order. |
| `InfluenceMonteCarloLayoutV1` | bounded scratch + compressed RR sidecar | Influence maximization is dominated by stochastic sampling output. Compress RR sets; do not pre-materialize cascades as topology. |

## Evidence And Verification

### Sourced Facts

| claim | status | evidence |
| --- | --- | --- |
| Knight Bus has measured lower runtime RSS than Neo4j on the current fixed-hop benchmark. | sourced local fact | [README.md:21](../README.md#L21), [README.md:39](../README.md#L39), [README.md:66](../README.md#L66) |
| The existing repo model already prefers RAM-optimized OLAP first, then latency artifacts later. | sourced local fact | [A-20260525203023-oltp-olap-e2e-benchmark-estimates.md:121](../docs/strategic-research/A-20260525203023-oltp-olap-e2e-benchmark-estimates.md#L121) |
| The Atlas has 13 families over 60 algorithms. | sourced local fact | [KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.md:88](../docs/KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.md#L88) |
| Neo4j's record store uses fixed node, relationship, and property records with pointer fields. | sourced local fact | `NodeRecordFormat.java:31-32`, `RelationshipRecordFormat.java:31-35`, `PropertyRecordFormat.java:37-39` |
| Neo4j record traversal follows relationship chains or dense-node relationship groups. | sourced local fact | `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/internal/recordstorage/RecordRelationshipTraversalCursor.java:95-167`, `gitrefrepo/neo4j-src/community/record-storage-engine/src/main/java/org/neo4j/internal/recordstorage/RecordRelationshipTraversalCursor.java:171-245` |
| GDS has a separate CSR graph-store factory, graph-store catalog, compressed adjacency implementations, and memory-estimation executor. | sourced local fact | `CSRGraphStoreFactory.java:67-86`, `GraphStoreCatalog.java:50-124`, `CompressedAdjacencyList.java:44-111`, `MemoryEstimationExecutor.java:80-153` |
| GDS uses projected in-memory graphs with compressed structures optimized for topology and property lookup. | sourced web fact | [Neo4j GDS introduction](https://neo4j.com/docs/graph-data-science/current/introduction/), [Neo4j GDS graph management](https://neo4j.com/docs/graph-data-science/current/management-ops/) |
| Neo4j server memory is not just heap; page cache and OS memory are part of the sizing problem. | sourced web fact | [Neo4j configuration settings](https://neo4j.com/docs/operations-manual/current/configuration/configuration-settings/) |
| External/semi-external graph processing is a proven family. | sourced web fact | [GraphChi](https://www.usenix.org/conference/osdi12/126-graphchi-large-scale-graph-computation-just-pc), [X-Stream](https://infoscience.epfl.ch/entities/publication/464b1137-af88-43ec-86e4-2d38f7a14f41) |
| Shared-memory graph frameworks commonly use compact array-backed graph representations rather than OLTP record chains. | sourced web fact | [Ligra paper](https://www.cs.cmu.edu/~guyb/papers/SB13.pdf), [GAP Benchmark Suite](https://arxiv.org/abs/1508.03619) |
| Graph compression can make topology dramatically smaller for web-like graphs. | sourced web fact | [WebGraph paper record](https://air.unimi.it/handle/2434/142632), [WebGraph Rust docs](https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph/index.html) |
| Runtime locality techniques can beat naive CSR execution without requiring one durable layout per algorithm. | sourced web fact | [Cagra / CSR Segmenting](https://commit.csail.mit.edu/papers/2017/zhang-bigdata17-cagra.pdf), [Propagation Blocking](https://scottbeamer.net/pubs/beamer-ipdps2017.pdf) |
| Some alternate graph formats reduce memory bandwidth or storage for specific kernels, but are better treated as optional execution/artifact choices than the universal base. | sourced web fact | [SlimSell](https://arxiv.org/abs/2010.09913), [Filter-Kruskal](https://epubs.siam.org/doi/10.1137/1.9781611972894.5) |
| Columnar formats are appropriate for typed property planes. | sourced web fact | [Apache Arrow columnar format](https://arrow.apache.org/docs/format/Columnar.html) |
| On-disk vector indexes are a precedent for feature-heavy algorithms that cannot keep all index data in RAM. | sourced web fact | [Faiss indexes that do not fit in RAM](https://github.com/facebookresearch/faiss/wiki/Indexes-that-do-not-fit-in-RAM), [Faiss OnDiskInvertedLists](https://faiss.ai/cpp_api/struct/structfaiss_1_1OnDiskInvertedLists.html) |
| Node2Vec-style walks and influence maximization have large runtime-state concerns. | sourced web fact | [Fast-Node2Vec](https://arxiv.org/abs/1805.00280), [HBMax](https://arxiv.org/abs/2208.00613) |
| The later `13 layouts vs base + runtime` critique is incorporated as an input to this note, not assumed as measured proof. | conversation input | The verdict table above turns that critique into falsifiable storage assignments and benchmark requirements. |

### Inferences

| inference | confidence | why |
| --- | --- | --- |
| Three persistent families are a better RAM-first architecture than thirteen durable layouts. | high | Most Atlas families collapse cleanly into topology, property columns, scratch, or result sidecars. |
| `HotMaterializedArtifactV1` should be usage-promoted, not prebuilt. | medium-high | The repo estimates latency artifacts help repeated hot algorithms but cost disk/build complexity. |
| A `50 GB` graph on an `8 GB` machine is plausible for read-only OLAP snapshots, not guaranteed for every algorithm. | medium | mmap and compression reduce active memory, but some algorithms require O(V), O(E), or large output state. |
| Compression should be optional in `CompressedDualCsrBaseV1`, not mandatory v1. | medium | Raw mmap CSR is simpler and already proven locally; compression has graph-dependent speed/space tradeoffs. |

### Speculation To Avoid Treating As Fact

| speculation | why it is not yet proven |
| --- | --- |
| `50 GB` always runs comfortably on `8 GB`. | Depends on graph shape, properties, algorithm scratch, concurrency, storage speed, and whether OLTP is colocated. |
| WebGraph-style compression gives `4x-10x` on all property graphs. | WebGraph results are strongest for web-like graphs with locality and similarity. Generic property graphs may compress less. |
| A Rust rewrite will beat Neo4j + GDS on all algorithms. | The current measured proof is fixed-hop traversal vs Neo4j over Bolt, not a full GDS benchmark. |
| Many hot artifacts will be cheap to maintain. | Freshness, rebuild, invalidation, and disk amplification need measurement. |

## 50GB On 8GB Machine Analysis

The honest answer is:

> A `50 GB` logical Neo4j dataset can become plausible on an `8 GB` machine for
> read-only OLAP workloads, if the OLAP projection is mmap-backed, compressed
> where useful, and algorithm scratch is bounded. It is not automatically
> plausible for every GDS algorithm, every property shape, or a colocated
> OLTP+OLAP server under concurrency.

### Why It Can Work

| mechanism | effect |
| --- | --- |
| mmap topology | the process does not allocate the whole graph on heap at open |
| dense IDs | adjacency entries can be `u32` when the projection has fewer than `2^32` nodes |
| typed property planes | algorithms map only the properties they need |
| compression | graph topology may shrink materially, especially if IDs are locality-friendly |
| semi-external execution | edge-heavy loops stream from disk while keeping O(V) or bounded scratch in memory |
| result sidecars | outputs do not mutate or duplicate the base graph |

### Why It Can Still Fail

| failure mode | example |
| --- | --- |
| page-fault storm | random access over a graph much larger than memory on slow storage |
| scratch explosion | APSP, high-concurrency PageRank, large KNN heaps, or full Node2Vec walk corpora |
| output explosion | Node Similarity or link prediction emitting huge pair sets |
| feature matrix dominates | `200M` nodes by wide float features can exceed topology size |
| compression hurts hot loops | decoding compressed adjacency can erase latency wins for small working sets |
| colocated OLTP pressure | transaction heap, WAL, indexes, page cache, and OLAP projection compete on the same box |

### Practical Memory Envelope

For an `8 GB` box, a RAM-first OLAP worker should target:

| component | target |
| --- | ---: |
| engine heap / allocator baseline | `< 512 MB` |
| query scratch default budget | `512 MB - 2 GB` |
| OS + filesystem headroom | `1.5 GB - 2 GB` |
| useful page cache / mmap residency | remaining memory |
| hard rule | no unbounded O(E) heap allocation without explicit user opt-in |

This suggests two product modes:

| mode | promise |
| --- | --- |
| `olap-low-ram` | bounded scratch, streaming/semi-external profiles, slower but avoids OOM |
| `olap-fast` | larger scratch and optional hot artifacts, for machines sized like GDS analytics servers |

## Format Contracts

### `CompressedDualCsrBaseV1`

Purpose: one sealed topology projection.

Required files:

```text
manifest.json
node_table.bin
key_index.bin
fwd.offsets.u64.bin
fwd.peers.u32.bin
rev.offsets.u64.bin
rev.peers.u32.bin
```

Optional compression files:

```text
fwd.blocks.meta.bin
fwd.peers.compressed.bin
rev.blocks.meta.bin
rev.peers.compressed.bin
node_relabel.map.u32.bin
```

Rules:

- raw CSR is the first implementation
- compressed CSR is selected per projection after measuring decode cost
- degree is derived from offsets unless compressed blocks make a cached degree
  plane cheaper
- node relabeling is a base-build option, not a separate algorithm layout

### `MmapColumnarPropertyPlaneV1`

Purpose: typed properties independent from topology.

Example files:

```text
edge.weight.f32.bin
edge.capacity.f32.bin
node.community.u32.bin
node.label.bitset.bin
node.features.rowmajor.f32.bin
node.features.row_offsets.u64.bin
```

Rules:

- one property plane can be mapped without mapping unrelated properties
- fixed-width numeric columns should be raw mmap-compatible
- variable-width or sparse columns must expose offsets plus values
- Arrow-compatible physical layout is preferred where it does not add complexity

### `BoundedScratchAndSidecarV1`

Purpose: keep runtime state out of durable graph storage.

Example scratch:

```text
score.curr.f32
score.next.f32
dist.f32
pred.u32
visited.bitset
frontier.queue.u32
union_find.parent.u32
```

Example sidecars:

```text
pagerank.score.f32.bin
wcc.component.u32.bin
dijkstra.pred.u32.bin
louvain.community.u32.bin
triangle.count.u64.json
```

Rules:

- every algorithm declares scratch bytes before execution
- scratch has memory budget, spill policy, and abort behavior
- results are versioned by projection snapshot ID
- sidecars are never the source of transactional truth

### `HotMaterializedArtifactV1`

Purpose: optional latency tier.

Examples:

```text
triangle.sorted_neighbors.*
mst.edge_order.*
knn.ondisk_ivf.*
pagerank.segmented_blocks.*
```

Rules:

- never built by default in `olap-low-ram`
- built only when usage history or explicit config justifies it
- must publish disk size, build RSS, freshness version, and invalidation policy
- must have a fallback to base + property plane + scratch

## Benchmark Plan

Run these before claiming server-cost PMF.

| benchmark | required measurement |
| --- | --- |
| `projection-build-50gb` | input size, output file sizes, build wall time, peak RSS, temp disk, checksum |
| `open-cold-warm` | open time, first query major faults, warm query RSS/PSS, page cache behavior |
| `cypher-baseline` | Neo4j Cypher traversal latency/RAM for exact-key 1-hop, 2-hop, and read-only blast-radius queries |
| `gds-baseline` | Neo4j GDS projection estimate, actual projection memory if feasible, algorithm memory estimate, algorithm runtime |
| `low-ram-algorithm-suite` | PageRank, WCC, Dijkstra, Triangle Count, Louvain, KNN, Node2Vec, CELF under `8 GB` cgroup |
| `scratch-budget-failure` | prove every algorithm errors cleanly or spills when scratch exceeds budget |
| `hot-artifact-ablation` | compare base-only vs hot artifact for top repeated algorithms: disk, build RSS, runtime, page faults |
| `mixed-day` | low-rate OLTP writes, periodic projection refresh, OLAP reads, projection lag, and box memory |

Minimum claim threshold:

- `50 GB` logical graph opens without loading the whole projection into heap
- no default algorithm allocates unbounded O(E) heap
- at least traversal, PageRank, WCC, and Dijkstra run under the target memory cap
- GDS baseline is reported separately from Cypher baseline
- all memory numbers include process RSS plus cgroup peak where available

## Final Synthesis

The best low-RAM OLAP architecture is not "one magic format" and not "thirteen
per-algorithm formats." It is:

```text
Neo4j-compatible OLTP truth
        |
        v
CompressedDualCsrBaseV1
        |
        +-- MmapColumnarPropertyPlaneV1
        |
        +-- BoundedScratchAndSidecarV1
        |
        +-- optional HotMaterializedArtifactV1
```

This preserves the strongest part of the Atlas: algorithms should be understood
by their dominant hot loop. But it changes the implementation doctrine:

- persistent formats should be few
- runtime profiles can be many
- sidecars should hold outputs
- hot artifacts should be earned by benchmarks

For Shreyas-Doshi-style adoption, the frontend must still feel like Neo4j. The
backend advantage should show up as a simpler promise:

> Same graph analytics journey, much smaller machine, transparent freshness
> metadata, and no surprise OOM for common workloads.

## Open Questions

- Should `CompressedDualCsrBaseV1` use raw CSR first and add WebGraph-style
  compression later, or should compression be part of the first `50 GB` proof?
- Should there be a separate `olap-low-ram` mode that deliberately trades
  latency for bounded memory?
- Should feature matrices use Arrow IPC files directly or a smaller custom
  fixed-width mmap layout first?
- How much projection lag is acceptable before users choose "latest truth"
  instead of "latest analytics snapshot"?
- Which three algorithms should be allowed to earn the first
  `HotMaterializedArtifactV1` promotion?

## Acceptance Checklist

| requirement | status |
| --- | --- |
| Explain why Neo4j's record store is not the OLAP target. | done |
| Explain why GDS is the fair OLAP baseline. | done |
| Give a concrete recommendation: 3 persistent formats, not 13. | done |
| Identify which original Atlas families become runtime profiles instead of stored layouts. | done |
| Include a `50 GB` on `8 GB` feasibility section. | done |
| List exact next benchmarks to prove or falsify the recommendation. | done |


# User Journey: 50 GB Dataset, CRUD, Queries, and OLTP/OLAP Lag

This local version folds the existing `50 GB` user-journey note into a deeper
Iggy-informed architecture addendum.

The short answer remains:

> **Do not stop OLTP queries or writes while OLAP catches up.**

But after studying [Apache Iggy](https://github.com/apache/iggy),
`compio`, and recent `io_uring` guidance, the more precise answer is:

> **Never block truth-plane reads or writes on OLAP refresh.**
> **Allow analytics to be stale by default, merged when cheap, and optionally
> wait only when the user explicitly asks for fresh analytics.**

---

## The Setup

Assume:

- `50 GB` graph dataset
- initial import already completed
- users keep doing CRUD
- users also run traversals and graph algorithms
- the system has:
  - `OLTP truth plane`
  - `OLAP projection plane`

The core UX question is:

> after a write lands in OLTP, what happens if the user immediately runs
> a traversal or `PageRank`?

---

## Baseline User Journey

### Phase 1: Initial Import

1. User imports `50 GB` of graph data.
2. OLTP truth is built first.
3. OLAP projection is built from that truth.
4. Once the first projection is ready:
   - OLTP reads and writes are available
   - OLAP traversals and algorithms are fast

At this point, both planes reflect the same data.

### Phase 2: User Starts Editing

1. User creates a node, edge, or property.
2. The write lands in OLTP immediately.
3. The OLAP projection is now older than truth.
4. Queries continue to work, but not all of them should have the same contract.

The clean contract is:

| query type | default source | freshness |
| --- | --- | --- |
| point lookup / CRUD readback | OLTP | immediate |
| transactional Cypher | OLTP | immediate |
| heavy traversal on compatible projection | OLAP | latest projection |
| graph algorithms | OLAP | latest projection |
| explicit "fresh analytics" request | OLAP after catch-up or merged path | user-selected |

This avoids the worst product sin:

- **silently returning stale analytics as if they were latest truth**

---

## Industry Baseline

The user journey above matches what serious mixed-workload systems already do.

- **Oracle Database In-Memory** keeps row and column representations together.
  Oracle documents a dual-format architecture and transactional consistency
  between row and in-memory column copies:
  [Oracle Database In-Memory](https://www.oracle.com/database/in-memory/),
  [In-Memory Column Store Architecture](https://docs.oracle.com/en/database/oracle/oracle-database/18/inmem/in-memory-column-store-architecture.html?source=%3Aow%3Alp%3Acpo%3A%3A)
- **TiDB + TiFlash** uses asynchronous columnar replicas while preserving
  consistency on reads, and also exposes stale-read semantics explicitly:
  [TiFlash Overview](https://docs.pingcap.com/tidb/stable/tiflash-overview/),
  [Stale Read](https://docs.pingcap.com/tidb/v6.1/dev-guide-use-stale-read/)
- **AlloyDB columnar engine** marks invalid columnar content after row updates,
  runs mixed row/column execution, and refreshes in the background:
  [About the AlloyDB columnar engine](https://docs.cloud.google.com/alloydb/docs/columnar-engine/about),
  [Maintain freshness of in-memory column store data](https://docs.cloud.google.com/alloydb/docs/columnar-engine/maintain-content-freshness)
- **Neo4j GDS** already separates transactional graph truth from projected
  analytics graphs:
  [GDS graph management](https://neo4j.com/docs/graph-data-science/current/management-ops/),
  [Understand the GDS workflow](https://graphacademy.neo4j.com/courses/gds-fundamentals/2-gds-basic-concepts/1-understand-gds-workflow/)

The repeated pattern is:

- writes do not wait for analytics refresh
- analytics may lag
- the system either merges tiers or exposes freshness semantics

---

## Candidate Timelines

### A. Snapshot Versioning

- Writes land in OLTP.
- OLAP serves the latest completed snapshot.
- Background rebuild creates the next snapshot.
- Staleness exists between rebuilds.

Good at:

- simplicity
- correctness
- easy rollback to the last good snapshot

Bad at:

- showing new data in analytics immediately
- full rebuild rewrite cost

### B. Overlay Model

- OLAP has immutable base CSR plus mutable overlay.
- Queries merge the base and overlay.
- Periodic recompact merges overlay into a new base.

Good at:

- zero logical staleness
- no "where is my node?" confusion

Bad at:

- more complex read logic
- whole-graph algorithms get slower as overlay grows

### C. Query Router

- OLTP and OLAP remain separate.
- Query type decides where execution runs.
- Analytics can be slightly stale while truth stays immediate.

Good at:

- clean product separation
- strong truth semantics

Bad at:

- query-classification complexity
- user confusion if freshness is not shown clearly

### D. Incremental CSR

- Keep CSR base mostly immutable.
- Append new edges or nodes into overflow structures.
- Compact later.

Good at:

- near-zero staleness
- often lower steady-state penalty than a fully generic overlay

Bad at:

- higher implementation complexity
- trickier algorithm invariants

---

## Current Best Path

For Knight Bus, the strongest near-term progression is still:

1. `v0.0.3`: manual snapshot rebuild
2. `v0.0.4`: scheduled/background rebuild
3. `v0.0.5`: explicit overlay or three-tier visibility
4. `v0.1.0`: incremental CSR only if the simpler overlay proves insufficient

That remains the recommended path even after studying Iggy.

What changed is **why**.

---

## Deep Exploration: What Apache Iggy Changes

### Premise Check

There are two different ideas hiding inside the question
"can we use `compio` or something?"

1. **Can Knight Bus borrow a faster write-path architecture from Iggy?**
   - `Yes.`
2. **Should Knight Bus replace its current OLAP mmap walker with `compio`
   and expect big speedups?**
   - `Probably no.`

The important correction is:

- `compio` and `io_uring` help most when your hot path is dominated by
  **many concurrent network and file I/O operations**
- Knight Bus OLAP today is dominated by:
  - `mmap`
  - page cache
  - dense array walking
  - memory bandwidth
  - algorithm inner loops

That means the hottest Knight Bus OLAP path is not waiting on the same thing
Iggy is optimizing.

### Expert Lenses

- `Storage-engine lens`: which parts of Iggy’s persistence design transfer to
  graph storage?
- `Async runtime lens`: where does `compio` materially outperform a simpler
  runtime choice?
- `Graph algorithm lens`: what helps traversals and PageRank versus what only
  helps append-heavy message logs?
- `Operator lens`: what new deployment and debugging burden comes with
  `io_uring`?
- `Skeptical lens`: are we chasing runtime novelty instead of the real
  bottleneck?

### Candidate Approaches

| approach | upside | downside | verdict |
| --- | --- | --- | --- |
| Replace Knight Bus wholesale with `compio`/`io_uring` | appealing performance story | likely little gain for mmap-heavy OLAP hot path; high complexity | reject |
| Ignore Iggy entirely | preserves simplicity | misses a strong write-path precedent | reject |
| Borrow Iggy’s persistence shape, not its full runtime ideology | targets the real problem: visibility, journaling, persistence tiers | still requires careful design | choose |

### Chosen Thesis

**Apache Iggy is highly relevant to Knight Bus, but mainly as a model for the
mutable plane, not as a reason to rewrite the immutable OLAP walker around
`compio`.**

The most transferable lessons are:

1. **Three-tier visibility**
   - persisted base
   - in-flight persisted-but-not-fully-published data
   - newest mutable journal

2. **Flush thresholds and durability knobs**
   - save after `N` messages
   - save after `N` bytes
   - periodic saver
   - optional `fsync` enforcement

3. **Append-first persistence**
   - journal first
   - publish optimized shape later

4. **Strong benchmark discipline**
   - benchmark the architecture transition itself, not just the end state

What is **not** the first thing to borrow:

1. full thread-per-core sharding
2. `compio` as a mandatory baseline for all code
3. Linux-specific `io_uring` operational assumptions in the OLAP read path

---

## Evidence and Verification

### Sourced Facts

- Apache Iggy explicitly describes itself as a persistent append-only log using
  `thread-per-core`, `shared nothing`, `io_uring`, and `compio`:
  [Apache Iggy README](https://github.com/apache/iggy)
- The Iggy architecture docs show a request flow where messages are first
  buffered in `MemoryMessageJournal` and then flushed to `.log` files:
  [Iggy architecture](https://iggy.apache.org/docs/introduction/architecture/)
- Iggy’s current code merges reads across **disk -> in-flight -> journal**,
  which is directly analogous to the visibility problem in base+overlay graph
  designs:
  [partition ops read path](https://github.com/apache/iggy/blob/master/core/server/src/streaming/partitions/ops.rs)
- Iggy exposes configurable persistence behavior such as periodic saving,
  `enforce_fsync`, buffered thresholds, and segment sizing:
  [Iggy server config](https://github.com/apache/iggy/blob/master/core/server/config.toml)
- Iggy’s migration write-up explains why they chose `compio`: the driver is
  disaggregated from the executor, but it also calls out non-zero complexity and
  even notes that request boxing introduces heap allocations:
  [Iggy migration to thread-per-core and io_uring](https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/)
- Iggy’s deployment docs make the operational cost concrete: the server needs
  working `io_uring` support and may require extra capabilities such as
  `IPC_LOCK` and relaxed seccomp settings in containerized environments:
  [Iggy Helm README](https://github.com/apache/iggy/blob/master/helm/charts/iggy/README.md)
- Recent DBMS research says `io_uring` is **not a panacea**: simply swapping
  interfaces yields only modest gains, while bigger wins come when the system is
  redesigned around batching and other capabilities:
  [High-Performance DBMSs with io_uring: When and How to use it](https://www.informatik.tu-darmstadt.de/media/systems/pdf_publications/iouring_vldb.pdf)
- `compio` is thread-local and centered on completion-based I/O rather than
  generic cross-thread async ergonomics:
  [compio docs](https://docs.rs/compio/latest/compio/runtime/struct.Runtime.html)

### Reasoned Inference

- Knight Bus OLAP queries are more likely to be **memory-bandwidth bound**
  after `mmap` than **syscall bound**.
- Therefore, moving the OLAP read path to `compio` is unlikely to unlock the
  kind of gains Iggy gets from network-plus-disk message streaming.
- The Knight Bus write and refresh path, by contrast, can benefit from Iggy’s
  style of:
  - journal
  - in-flight visibility
  - segmented persistence
  - explicit flush and `fsync` policy

### Verification Questions

1. **Does Iggy already solve a multi-tier visibility problem?**
   - `Yes.` Its read path explicitly merges disk, in-flight, and journal tiers.
2. **Is Iggy’s performance story mainly about immutable mmap scans?**
   - `No.` It is mainly about append-heavy, concurrent network-plus-disk I/O.
3. **Does the `io_uring` literature support "just swap the runtime"?**
   - `No.` The stronger gains come when the architecture is redesigned around
     batching and completion-based I/O.
4. **Would Knight Bus still need overlay or visibility logic even with compio?**
   - `Yes.` `compio` does not remove the need for a mutable truth layer.

---

## Rubber Duck Debug

If I tell a rubber duck:

> "We should use `compio` because Iggy is fast."

the duck should ask:

1. **What exactly is slow in Knight Bus today?**
   - not the steady-state mmap adjacency walk
   - the unsolved part is mutable visibility and refresh

2. **What does `compio` make faster?**
   - lots of concurrent completion-based I/O
   - network + file pipelines
   - shard-local async services

3. **What does `compio` not magically fix?**
   - overlay merge semantics
   - CSR mutation cost
   - algorithm cache invalidation
   - whole-graph recomputation policy

4. **So where should it go, if anywhere?**
   - the future OLTP daemon
   - the WAL / journal / flush worker
   - the background projection builder
   - possibly the ingestion pipeline

5. **Where should it not be the first move?**
   - replacing the current mmap OLAP read path just for fashion

If the duck is satisfied, the recommendation survives.

---

## Iggy-Informed Recommendation

### Borrow First

1. **Three-tier graph visibility**
   - `sealed CSR base`
   - `in-flight persisted delta`
   - `mutable overlay journal`

2. **Configurable freshness and durability policy**
   - rebuild interval
   - overlay-size threshold
   - delta-bytes threshold
   - optional stronger durability mode

3. **Segmented, append-friendly mutable plane**
   - not one giant mutable graph structure
   - a journal or delta segment stream that can later compact into CSR

4. **Measurement discipline**
   - benchmark rebuild latency
   - benchmark overlay growth impact
   - benchmark fresh readback latency

### Delay Until Later

1. **Full `compio` migration**
2. **Thread-per-core everywhere**
3. **Linux-only `io_uring` assumptions in the OLAP reader**

### Use `compio` If These Become True

- Knight Bus becomes a long-running Linux-first server
- the mutable plane is doing lots of concurrent file and socket I/O
- query routing and refresh work become I/O-driven bottlenecks
- the team is comfortable owning `io_uring` operational complexity

### Do Not Use `compio` First If These Remain True

- the core win is still mmap + dense arrays
- the next bottleneck is overlay semantics, not syscalls
- portability and simple developer setup matter more than shaving write-path tail latency

---

## Final Synthesis

The user journey does **not** change in its top-level contract:

- truth-plane queries never stop
- writes never wait for OLAP rebuild
- analytics may be stale unless merged or explicitly refreshed

What the Iggy study changes is the implementation opinion:

- **Do not chase `compio` as the first optimization for the OLAP walker.**
- **Do borrow Iggy’s journal, in-flight, and segmented persistence ideas for the mutable plane.**

In one sentence:

> **Iggy is a better template for Knight Bus OLTP and refresh mechanics than for Knight Bus OLAP execution mechanics.**

---

## Open Questions

1. What is the measured rebuild time for a synthetic `500M`-edge graph on the target hardware?
2. How large can the graph overlay become before traversal and PageRank slowdowns become user-visible?
3. Can the first overlay design be generic enough for traversal plus a few core algorithms without forcing a full incremental-CSR implementation?
4. Should the first user-facing freshness contract be:
   - `latest_truth`
   - `latest_analytics_snapshot`
   - `wait_for_fresh_analytics`
5. At what point does the mutable plane become I/O-bound enough that `compio` is worth the operational complexity?

# v003 Requirements: Full GDS Surface On A CSR-Centered OLAP Architecture

> Purpose: capture the complete first-pass requirements for aiming at the full
> Neo4j Graph Data Science surface while keeping the storage decision honest.
>
> Core correction: **Tilehouse is not a requirement.** A full-GDS target needs a
> CSR-centered multi-plane architecture. Tilehouse is one possible topology
> backend for update locality and bounded compaction, not the foundation of GDS
> compatibility.

## Requirement Thesis

The architecture SHALL be:

```text
Neo4j-compatible procedure surface
-> GDS inventory and compatibility registry
-> named projection catalog
-> topology backend abstraction
   -> flat generationed dual CSR first
   -> optional Tilehouse backend later
   -> optional Graph-LSM backend later if freshness demands it
-> columnar property plane
-> bounded scratch plane
-> result sidecar plane
-> model and pipeline artifact plane
-> OLTP writeback/freshness bridge
```

The architecture SHALL NOT assume:

```text
CSR alone can represent all GDS semantics.
Tilehouse is mandatory for all GDS.
Every GDS procedure can run under an 8 GB budget.
Unsupported procedures can look unknown.
Memory estimates can ignore page cache, scratch, deltas, or output state.
```

## Completeness Definition

"ALL GDS requirements" means more than procedure names. The requirements are
complete only when they cover every visible contract layer a Neo4j GDS user can
observe or depend on:

| layer | requirement scope |
| --- | --- |
| procedure discovery | all local `gds.*` procedures, alpha/beta/deprecated aliases, modes, and estimates |
| invocation ABI | procedure names, parameters, defaults, configs, output columns, error behavior |
| catalog semantics | graph projection, list, exists, drop, size, schema, mutation, export, sample, and filter |
| graph data model | dense IDs, Neo4j IDs, labels, relationship types, orientations, properties, defaults, nulls |
| topology execution | forward/reverse/undirected adjacency, global streams, inverse indexes, edge filters |
| property execution | scalar columns, vector columns, weights, labels, feature matrices, graph properties |
| algorithm execution | exactness, convergence, deterministic seeds, workspaces, scratch, result modes |
| memory accounting | heap, RSS, page cache, direct buffers, scratch, deltas, sidecars, output, model bytes |
| freshness | snapshot generation, delta inclusion, write visibility, stale-read policy, refresh policy |
| mutate/writeback | projected graph sidecars, OLTP writeback, counts, property names, partial failure handling |
| ML artifacts | model metadata, pipeline metadata, training configs, feature schemas, metrics, persistence |
| operations | progress, cancellation, cleanup, telemetry, sysinfo, version/license state |
| testing | inventory, ABI goldens, tiny oracles, parity, memory budgets, crash/restart, compatibility |

Therefore, this document treats the scanned GDS procedure bases as **surface
coverage**, and the requirement IDs as **implementation contracts**. The final
implementation must satisfy both.

## Source Baseline

Local source facts used by these requirements:

| fact | evidence |
| --- | --- |
| Current Knight Bus topology is flat immutable dual CSR plus mmap. | `README.md:13-17`, `src/snapshot.rs:14-21`, `src/runtime.rs:41-53` |
| Current runtime is walk-focused, not GDS-focused. | `src/runtime.rs:22-39`, `src/types.rs:145-183` |
| Current normalized graph data is topology-only. | `src/types.rs:265-280` |
| GDS graph store includes graph, node, relationship, property, label, inverse index, mutation, and graph-view APIs. | `gitrefrepo/neo4j-gds-src/core-api/src/main/java/org/neo4j/gds/api/GraphStore.java:48-238` |
| GDS builds CSR graph stores from projected nodes and relationships. | `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/api/CSRGraphStoreFactory.java:67-87` |
| GDS compressed adjacency estimates pages, degrees, and offsets. | `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/compression/varlong/CompressedAdjacencyList.java:44-112` |
| GDS facade includes catalog, algorithms, model catalog, operations, pipelines, and deprecated metrics. | `gitrefrepo/neo4j-gds-src/procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/GraphDataScienceProcedures.java:30-44` |

Local GDS scan result used as a working inventory baseline:

| module | procedure bases | annotation rows |
| --- | ---: | ---: |
| `catalog` | 35 | 54 |
| `centrality` | 15 | 96 |
| `community` | 26 | 154 |
| `embeddings` | 6 | 36 |
| `machine-learning` | 33 | 46 |
| `misc` | 28 | 39 |
| `path-finding` | 20 | 92 |
| `pipeline-catalog` | 2 | 6 |
| `similarity` | 6 | 44 |
| `sysinfo` | 3 | 3 |
| **total** | **174 bases** | **570 rows** |

The scan found `562` unique procedure names. The final inventory SHALL be a
checked artifact and SHALL be treated as more authoritative than this summary.

## Requirement Levels

Every GDS procedure SHALL have exactly one support level:

| level | meaning |
| --- | --- |
| `P0Registered` | Procedure is known, classified, config-shaped, schema-shaped, and returns deterministic unsupported behavior if not implemented. |
| `P1ExactLowRam` | Procedure is implemented exactly and can run when its memory estimate fits the requested budget. |
| `P2Later` | Procedure is intentionally deferred but known and registered. |
| `UnsupportedButRegistered` | Procedure is known to exist in GDS but not supported in this release. |

Requirement:

```text
WHEN a user calls any known GDS procedure
THEN Knight Bus SHALL either execute it or return a deterministic
UnsupportedButRegistered response
AND SHALL NOT report it as an unknown procedure.
```

## Architecture Plane Requirements

### REQ-PLANE-001: Procedure ABI Plane

**WHEN** a GDS procedure call enters the system
**THEN** the system SHALL resolve it through a procedure registry
**AND** SHALL classify family, mode, config schema, output schema, support level,
and estimate behavior before execution.

Verification:

| test | assertion |
| --- | --- |
| `gds_registry_knows_all_inventory_rows` | every inventory row has a registry entry |
| `gds_registry_rejects_duplicates` | duplicate procedure names fail registry validation |
| `gds_unknown_differs_from_registered_unsupported` | unknown and known-unsupported errors differ |

### REQ-PLANE-002: Catalog Plane

**WHEN** a user creates, lists, filters, mutates, writes, samples, exports, or
drops a GDS graph
**THEN** the catalog plane SHALL manage named projection state independently of
the physical topology backend.

Verification:

| test | assertion |
| --- | --- |
| `graph_catalog_lifecycle_matches_gds_shape` | project/list/exists/drop works for named graphs |
| `graph_catalog_size_estimate_includes_all_planes` | size includes topology, properties, sidecars, scratch, and outputs |
| `graph_catalog_does_not_duplicate_topology_by_default` | projection reuses topology where possible |

### REQ-PLANE-003: Topology Backend Plane

**WHEN** an algorithm needs graph adjacency
**THEN** it SHALL depend on a topology backend trait, not on flat CSR or
Tilehouse directly.

Minimum topology backend operations:

```text
node_count
relationship_count
node labels by dense id
relationship types
forward neighbors
reverse neighbors
undirected logical neighbors
global forward edge stream
global reverse edge stream
relationship type filtered stream
degree and weighted degree
optional inverse index
```

Backend requirements:

| backend | requirement |
| --- | --- |
| `FlatDualCsrBackend` | SHALL be first implementation and correctness oracle. |
| `TilehouseBackend` | MAY be added for local updates, local compaction, and bounded page-window planning. |
| `GraphLsmBackend` | MAY be added only if measured freshness pressure makes snapshots plus deltas insufficient. |

### REQ-PLANE-004: Columnar Property Plane

**WHEN** a procedure requires labels, relationship types, weights, features,
node properties, relationship properties, graph properties, defaults, or nulls
**THEN** the property plane SHALL serve typed columnar values without requiring a
new topology layout.

Supported value requirements:

| value area | requirement |
| --- | --- |
| node labels | label membership SHALL be queryable as bitsets or equivalent compressed columns |
| relationship types | type filters SHALL apply to adjacency streams |
| weights | numeric relationship weights SHALL support default and missing-property behavior |
| node properties | scalar and vector values SHALL support typed reads |
| relationship properties | edge-aligned properties SHALL support relationship type filters |
| graph properties | graph-level values SHALL be stored in catalog metadata or graph property sidecars |
| feature vectors | ML and KNN features SHALL be columnar and estimateable |
| embeddings | output embeddings SHALL be result sidecars, not topology |

### REQ-PLANE-005: Scratch Plane

**WHEN** an algorithm requires vectors, frontiers, queues, heaps, candidate pairs,
walk corpora, matrices, or contracted graphs
**THEN** the scratch plane SHALL allocate or spill that state under an explicit
execution budget.

Scratch classes:

```text
ScalarPerNode
VectorPerNode
FrontierBitset
DistanceVector
PriorityQueue
CandidateTopK
PairStream
EmbeddingMatrix
WalkCorpus
ContractedGraph
ModelTrainingBatch
```

### REQ-PLANE-006: Result Sidecar Plane

**WHEN** a procedure runs in `mutate` mode
**THEN** its output SHALL be stored as a projected graph sidecar
**AND** the catalog SHALL expose the new property or relationship type.

**WHEN** a procedure runs in `write` mode
**THEN** its output SHALL be written through the OLTP-facing writeback bridge
**AND** write counts, property names, and failure behavior SHALL match the GDS
procedure contract.

### REQ-PLANE-007: Model And Pipeline Artifact Plane

**WHEN** a procedure creates, trains, lists, predicts with, or drops a model or
pipeline
**THEN** model and pipeline metadata SHALL live in an artifact plane separate
from topology and property columns.

This plane SHALL support:

```text
model name
pipeline name
owner/database identity
feature schema
training configuration
metrics
artifact bytes
creation and modification metadata
versioning
drop/list/exists behavior
```

### REQ-PLANE-008: Freshness Bridge

**WHEN** OLTP data changes after an OLAP projection is built
**THEN** OLAP SHALL expose explicit freshness semantics:

```text
snapshot_only
snapshot_plus_bounded_delta
force_refresh_before_run
reject_until_compacted
```

The freshness bridge SHALL NOT require Tilehouse in v1. It MAY use:

| approach | requirement |
| --- | --- |
| generationed flat CSR rebuild | baseline correctness path |
| bounded global delta overlay | small update bridge |
| Tilehouse cell-local deltas | optional if local update/compaction wins are measured |
| Graph-LSM | later if near-real-time OLAP becomes mandatory |

## Cross-Cutting GDS Requirements

### REQ-GDS-INV-001: Full Procedure Inventory

**WHEN** the local GDS reference changes
**THEN** the inventory generator SHALL detect added, removed, or renamed
procedures
**AND** SHALL fail CI until support levels and schema metadata are updated.

### REQ-GDS-ABI-001: Procedure Modes

Every algorithm procedure SHALL classify modes:

| mode | requirement |
| --- | --- |
| `stream` | return rows without mutating catalog or OLTP |
| `stats` | return aggregate metrics without writing result sidecars |
| `mutate` | write result into projected graph sidecars |
| `write` | write result back through OLTP-facing bridge |
| `estimate` | return memory contract without executing algorithm work |
| `train` | create model artifact and metrics |
| `predict` | use model/pipeline artifacts and return or write predictions |

### REQ-GDS-ABI-002: Config Parsing

**WHEN** a procedure receives configuration
**THEN** the parser SHALL validate defaults, aliases, bad types, missing keys,
unknown keys, and deprecated config keys before execution.

### REQ-GDS-ABI-003: Output Schemas

**WHEN** a procedure executes or estimates
**THEN** output column names and value types SHALL match the checked GDS
inventory for that procedure and mode.

### REQ-GDS-ABI-004: Deprecated, Alpha, And Beta Procedures

**WHEN** a procedure is alpha, beta, or deprecated in GDS
**THEN** the registry SHALL record that status
**AND** SHALL expose a deterministic compatibility policy:

```text
support as alias
support as deprecated alias
register but unsupported
exclude only with explicit compatibility note
```

### REQ-GDS-ABI-005: Determinism

**WHEN** a procedure has stochastic behavior
**THEN** seeded execution SHALL be reproducible across flat CSR and any future
topology backend.

### REQ-GDS-ABI-006: Procedure Discovery And Metadata

**WHEN** procedure metadata is requested or inspected
**THEN** the system SHALL expose procedure names, modes, deprecation status,
descriptions where available, parameter names, default values, and output
columns consistently with the checked inventory.

### REQ-GDS-ABI-007: Error Semantics

**WHEN** a procedure fails before execution
**THEN** the error SHALL identify whether the cause is unknown procedure,
registered unsupported procedure, bad config, missing graph, missing property,
unsupported mode, insufficient memory budget, stale projection, or internal
failure.

**WHEN** a procedure fails during execution
**THEN** partial scratch, result sidecars, model artifacts, and catalog mutations
SHALL be cleaned up or marked failed deterministically.

### REQ-GDS-ABI-008: User, Database, And Ownership Context

**WHEN** a procedure reads or writes catalog/model/pipeline state
**THEN** graph names, model names, and pipeline names SHALL be scoped by database
identity and owner semantics compatible with Neo4j GDS expectations.

### REQ-GDS-ABI-009: Concurrency Config

**WHEN** a procedure accepts concurrency or read-concurrency configuration
**THEN** the planner SHALL translate that value into bounded worker, I/O, and
scratch budgets
**AND** SHALL reject settings that would violate the selected memory contract.

### REQ-GDS-ABI-010: Cancellation And Cleanup

**WHEN** a user cancels a running procedure or the server shuts down
**THEN** the procedure SHALL stop at a safe checkpoint, release scratch, preserve
durable catalog/model state, and report a deterministic cancelled or interrupted
status.

## Graph Data Model Requirements

### REQ-DATA-001: ID Mapping

**WHEN** data enters an OLAP projection
**THEN** the system SHALL maintain a stable mapping between Neo4j-facing IDs,
external keys where present, and internal dense IDs.

The mapping SHALL support:

```text
node id to dense id
dense id to node id
relationship id or synthetic id to edge position
edge position to source and target dense ids
generation identity
projection-local filtered ids
```

### REQ-DATA-002: Labels

**WHEN** a projection selects node labels
**THEN** label membership SHALL be represented as columnar or bitmap sidecars
that support union, intersection, exclusion, and empty-label behavior.

### REQ-DATA-003: Relationship Types

**WHEN** a projection selects relationship types
**THEN** type selection SHALL apply to adjacency streams, property streams,
counts, estimates, inverse indexes, and writeback targets.

### REQ-DATA-004: Orientation

**WHEN** a projection or algorithm requests `NATURAL`, `REVERSE`, or
`UNDIRECTED` orientation
**THEN** the logical graph view SHALL produce the correct edge direction without
rewriting base topology unless the physical plan explicitly chooses a derived
sidecar.

### REQ-DATA-005: Relationship Aggregation

**WHEN** projection config aggregates parallel relationships
**THEN** aggregation semantics SHALL be represented in the projection catalog,
topology view, relationship counts, and property values.

### REQ-DATA-006: Property Defaults And Nulls

**WHEN** a property is missing, null, or has a configured default
**THEN** the property plane SHALL resolve the value according to procedure config
before execution begins.

### REQ-DATA-007: Numeric Types And Coercion

**WHEN** a procedure requires numeric weights, features, labels, or outputs
**THEN** type coercion, overflow, NaN, infinity, and invalid-value behavior SHALL
be validated before execution.

### REQ-DATA-008: Vector Values

**WHEN** a procedure requires vector properties or embeddings
**THEN** dimensionality, physical value type, null behavior, and output layout
SHALL be explicit in the property or result sidecar manifest.

### REQ-DATA-009: Schema Reporting

**WHEN** graph schema is listed or streamed
**THEN** node labels, relationship types, graph properties, node properties,
relationship properties, and result sidecars SHALL be reported from catalog
metadata without scanning the full graph.

## Execution Semantics Requirements

### REQ-EXEC-001: Physical Plan Explanation

**WHEN** a procedure is estimated or executed
**THEN** the selected physical plan SHALL be explainable as one of:

```text
metadata_only
local_adjacency
global_mmap_scan
global_explicit_stream
frontier_traversal
priority_queue_traversal
iterative_vector_scan
intersection_scan
candidate_generation
matrix_or_embedding_job
pipeline_training_job
model_prediction_job
writeback_job
```

### REQ-EXEC-002: Row Ordering

**WHEN** a procedure returns rows
**THEN** row ordering SHALL be deterministic where GDS requires or implies it
**AND** unspecified ordering SHALL be documented and stable enough for tests.

### REQ-EXEC-003: Streaming And Backpressure

**WHEN** result rows are streamed
**THEN** the system SHALL avoid materializing all rows in heap unless the
procedure semantics require global sorting or aggregation.

### REQ-EXEC-004: Progress Reporting

**WHEN** a long-running procedure executes
**THEN** progress state SHALL report procedure name, graph name, phase, work
completed, work remaining where knowable, memory plan, and cancellation state.

### REQ-EXEC-005: Resource Isolation

**WHEN** multiple procedures run concurrently
**THEN** each procedure SHALL have independent budget accounting for heap,
scratch, spill, direct buffers, and result sidecars.

### REQ-EXEC-006: Temporary Artifact Lifecycle

**WHEN** a procedure creates scratch, spill files, candidate tapes, walk corpora,
or temporary contracted graphs
**THEN** those artifacts SHALL be namespaced by job/generation and cleaned up on
success, cancellation, and recoverable failure.

### REQ-EXEC-007: Result Atomicity

**WHEN** a mutate, write, train, or pipeline operation completes
**THEN** the catalog/model/writeback state SHALL become visible atomically at the
procedure boundary.

## Catalog Surface Requirements

Catalog module scope from local GDS scan:

```text
35 procedure bases
54 annotation rows
```

### REQ-CAT-001: Native Projection

**WHEN** a user calls `gds.graph.project`
**THEN** the system SHALL create a named graph projection from labels,
relationship types, orientation, and property selectors
**AND** SHALL not copy full topology unless the physical plan requires it.

### REQ-CAT-002: Cypher Projection

**WHEN** a user calls `gds.graph.project.cypher`
**THEN** the system SHALL support a compatibility path for node and relationship
queries
**AND** SHALL either execute the projection or return
`UnsupportedButRegistered` with a Cypher-projection reason.

### REQ-CAT-003: Graph Lifecycle

**WHEN** a user calls graph list, exists, drop, or size procedures
**THEN** the catalog SHALL reflect named projection state, owner/database
identity, schema, counts, memory estimates, and modification timestamps.

### REQ-CAT-004: Graph Filtering And Subgraphs

**WHEN** a user creates a filtered/subgraph projection
**THEN** the system SHALL represent filters as logical graph views over topology
and property planes where possible.

### REQ-CAT-005: Property Streaming

**WHEN** a user streams node, relationship, or graph properties
**THEN** the property plane SHALL stream typed values without materializing the
whole property set in heap.

### REQ-CAT-006: Property Mutation And Drop

**WHEN** a user mutates or drops graph, node, or relationship properties
**THEN** the catalog and sidecar metadata SHALL update atomically.

### REQ-CAT-007: Relationship Transformations

**WHEN** a user calls relationship transforms such as to-undirected,
index-inverse, relationship delete, or derived relationship procedures
**THEN** the system SHALL create logical or sidecar topology artifacts without
rewriting base CSR unless explicitly required.

### REQ-CAT-008: Export And Sampling

**WHEN** a user exports, samples, or generates graphs
**THEN** the system SHALL route through bounded streaming plans and report memory
usage before execution.

## Centrality Requirements

Centrality scope from local scan:

```text
15 procedure bases
96 annotation rows
```

### REQ-CENT-001: Degree Family

Degree centrality SHALL read offsets and optional weights from topology/property
planes and SHALL support stream/stats/mutate/write/estimate modes where present
in the inventory.

### REQ-CENT-002: PageRank Family

PageRank, ArticleRank, and Eigenvector SHALL run over global edge streams with
explicit vector estimates.

Requirements:

```text
score vector bytes SHALL be explicit
previous/current vectors SHALL be explicit
degree or weight arrays SHALL be explicit
mmap plans SHALL NOT claim deterministic RAM
strict-RAM plans SHALL use explicit stream/spill/reject behavior
```

### REQ-CENT-003: HITS

HITS SHALL support forward and reverse scans and estimate authority/hub vectors
separately.

### REQ-CENT-004: Closeness And Harmonic

Closeness and harmonic centrality SHALL treat all-pairs or many-source traversal
as high-risk and SHALL estimate source batching, frontier, and distance state.

### REQ-CENT-005: Betweenness

Betweenness SHALL estimate source sweeps, sigma/delta vectors, stacks,
predecessor state, and source batching before execution.

### REQ-CENT-006: Articulation Points And Bridges

Articulation point and bridge detection SHALL support DFS low-link state and
SHALL preserve deterministic traversal order for reproducible output.

### REQ-CENT-007: Influence Maximization

Influence/CELF procedures SHALL be registered and SHALL require explicit
simulation/sample/candidate state estimates before moving beyond unsupported
status.

## Pathfinding Requirements

Pathfinding scope from local scan:

```text
20 procedure bases
92 annotation rows
```

### REQ-PATH-001: BFS And DFS

BFS and DFS SHALL run over topology backend cursors with bounded visited,
frontier, path, and parent state.

### REQ-PATH-002: Weighted Shortest Path

Dijkstra, A*, Delta-Stepping, and Bellman-Ford SHALL require a weight sidecar
contract and SHALL fail deterministically if configured weights are absent or
invalid.

### REQ-PATH-003: All Shortest Paths

All-shortest-path procedures SHALL estimate repeated source sweeps and SHALL
reject unsafe exact plans under strict memory budgets.

### REQ-PATH-004: Yen's K Shortest Paths

Yen's procedures SHALL estimate candidate path heaps, suppressed path state, and
underlying shortest-path work.

### REQ-PATH-005: Random Walk

Random walk procedures SHALL use deterministic seeded RNG behavior and SHALL
store walk buffers or corpora in the scratch/result sidecar plane.

### REQ-PATH-006: Spanning And Steiner Trees

Spanning tree and Steiner procedures SHALL estimate parent arrays, candidate
heaps, prizes/weights, and writeback relationship counts.

### REQ-PATH-007: DAG Algorithms

Topological sort and longest path SHALL validate DAG assumptions and SHALL
return deterministic errors for cyclic input where required.

## Community And Structure Requirements

Community scope from local scan:

```text
26 procedure bases
154 annotation rows
```

### REQ-COMM-001: WCC And SCC

WCC and SCC SHALL run over topology backend cursors and SHALL estimate component
arrays, stacks, and frontier/union state.

### REQ-COMM-002: Triangle Count And Local Clustering

Triangle count and local clustering coefficient SHALL require sorted adjacency
or intersection-capable cursors and SHALL avoid double-counting across filters,
relationship types, and future Tilehouse boundaries.

### REQ-COMM-003: K-Core

K-core SHALL estimate degree arrays, peel queues, and result sidecars.

### REQ-COMM-004: Coloring

K-1 coloring SHALL estimate color arrays and conflict frontiers and SHALL define
deterministic tie-breaking.

### REQ-COMM-005: Label Propagation And SLPA

Label propagation and SLPA SHALL estimate label state, distributions, seed
behavior, and convergence iterations.

### REQ-COMM-006: Louvain And Leiden

Louvain and Leiden SHALL require a contracted-graph scratch artifact and SHALL
estimate community arrays, modularity state, and every contraction level.

### REQ-COMM-007: Modularity And Conductance

Modularity, modularity optimization, and conductance SHALL read community
assignments from property/result sidecars and SHALL stream cut/internal edge
counts.

### REQ-COMM-008: Max-K-Cut And K-Means

Max-k-cut and k-means SHALL be treated as property/state-heavy algorithms and
SHALL require explicit vector/assignment/centroid estimates.

## Similarity Requirements

Similarity scope from local scan:

```text
6 procedure bases
44 annotation rows
```

### REQ-SIM-001: Node Similarity

Node similarity SHALL use adjacency/property overlap without materializing all
`O(n^2)` pairs unless a configured budget explicitly allows it.

### REQ-SIM-002: KNN

KNN SHALL operate on feature/property vectors from the columnar property plane
and SHALL require candidate generation, topK heap, and cutoff estimates.

### REQ-SIM-003: Filtered Similarity

Filtered similarity procedures SHALL push label/property filters before
candidate expansion wherever possible.

## Embeddings Requirements

Embeddings scope from local scan:

```text
6 procedure bases
36 annotation rows
```

### REQ-EMB-001: FastRP

FastRP SHALL estimate `node_count * dimension * bytes_per_value` output and
intermediate propagation state before execution.

### REQ-EMB-002: Node2Vec

Node2Vec SHALL estimate walk corpus, context windows, embedding matrices, RNG
state, and training batches.

### REQ-EMB-003: GraphSAGE

GraphSAGE SHALL require model artifact support, sampled neighbor batches, feature
columns, and train/infer mode contracts.

### REQ-EMB-004: HashGNN

HashGNN SHALL define deterministic hash behavior and estimate feature hashes plus
embedding outputs.

## Machine Learning And Pipeline Requirements

Machine-learning module scope from local scan:

```text
33 procedure bases
46 annotation rows
```

Pipeline catalog scope:

```text
2 procedure bases
6 annotation rows
```

### REQ-ML-001: Pipeline Lifecycle

Pipeline create, list, exists, drop, configure, add-step, and train procedures
SHALL be backed by the model/pipeline artifact plane.

### REQ-ML-002: Node Classification And Regression

Node classification and regression SHALL require:

```text
feature schema
target property validation
train/test split metadata
model candidate metadata
metrics
prediction sidecars
writeback behavior
```

### REQ-ML-003: Link Prediction

Link prediction SHALL require relationship split metadata, negative sampling
state, feature extraction, model artifacts, and prediction output contracts.

### REQ-ML-004: KGE

Knowledge graph embedding procedures SHALL require typed relationship sidecars,
negative sampling, embedding matrices, model artifacts, and strict memory
estimates.

### REQ-ML-005: Split Relationships

Relationship splitting SHALL be deterministic with seed configuration and SHALL
write split membership as sidecars or relationship properties.

## Model Catalog Requirements

Model catalog scope from local scan:

```text
3 procedure bases
drop/list/exists behavior
```

### REQ-MODEL-001: Model Metadata

Models SHALL persist:

```text
model name
model type
creator
database identity
creation time
feature schema
training config
metrics
artifact location
version
```

### REQ-MODEL-002: Model Lifecycle

Model list, exists, and drop SHALL survive process restart and SHALL be
independent from CSR topology generation.

## Miscellaneous, Operations, And Sysinfo Requirements

Misc scope from local scan:

```text
28 procedure bases
39 annotation rows
```

Sysinfo scope:

```text
3 procedure bases
3 annotation rows
```

### REQ-MISC-001: Scale Properties

Scale properties SHALL operate on the columnar property plane and SHALL support
stream/mutate/write/estimate behavior where present.

### REQ-MISC-002: Collapse Path

Collapse path SHALL create derived relationship sidecars or topology artifacts
without rewriting base CSR unless required by config.

### REQ-MISC-003: To Undirected

To-undirected SHALL prefer logical projection or derived relationship sidecar
over base topology rewrite.

### REQ-MISC-004: Index Inverse

Index inverse SHALL create or validate reverse adjacency/inverse indexes for
relationship types.

### REQ-MISC-005: Progress And Memory Procedures

Progress and memory procedures SHALL expose running task state, estimates,
actual measurements, and historical summary where compatible.

### REQ-MISC-006: Feature Toggles

Feature procedures SHALL be registered and SHALL map to explicit Knight Bus
settings or deterministic unsupported responses.

### REQ-SYS-001: Version, License, And Debug Info

Version, license state, and sysinfo procedures SHALL be server/admin surface
requirements, not topology requirements.

## Memory Requirements

### REQ-MEM-001: Holistic Estimate Object

Every estimate SHALL include:

```text
required_bytes
heap_bytes
rss_budget_bytes
page_cache_expected_bytes
page_cache_unbounded_risk
direct_io_buffer_bytes
topology_bytes
property_sidecar_bytes
algorithm_state_bytes
scratch_bytes
delta_overlay_bytes
result_sidecar_bytes
model_artifact_bytes
writeback_bytes
spill_bytes
```

### REQ-MEM-002: Mmap Honesty

**WHEN** a plan uses mmap
**THEN** the estimate SHALL state that page-cache residency is OS-mediated
**AND** SHALL NOT claim deterministic RAM.

### REQ-MEM-003: Strict-RAM Execution

**WHEN** a user selects a strict memory budget
**THEN** the planner SHALL choose explicit-stream/spill execution or reject the
procedure before execution.

### REQ-MEM-004: 50 GB On 8 GB Decision

**WHEN** the graph is 50 GB-class and the machine budget is 8 GB-class
**THEN** each procedure SHALL produce:

```text
can_run
required_budget_bytes
dominant_state
execution_profile
freshness_mode
reason_if_rejected
```

## Freshness And Update Requirements

### REQ-FRESH-001: Generation Identity

Every projection SHALL record source generation, freshness watermark, and
whether deltas are included.

### REQ-FRESH-002: Small Update Behavior

**WHEN** 10 OLTP records change
**THEN** OLAP SHALL NOT be required to rebuild the entire projection unless the
selected freshness mode requires exact regenerated snapshots.

Allowed behavior:

```text
serve snapshot-only stale analytics
serve snapshot plus bounded delta
force refresh before run
reject until refresh if delta budget is exceeded
```

### REQ-FRESH-003: Tilehouse Optionality

Tilehouse SHALL be introduced only if one of these measured triggers occurs:

| trigger | measurement |
| --- | --- |
| flat rebuild lag violates freshness SLO | rebuild time and update rate |
| global delta overlay exceeds memory budget | delta bytes and query merge cost |
| local traversals churn page cache badly | major faults and resident set |
| dirty-region compaction beats generation rebuild | compaction time and scratch bytes |

## Operational Requirements

### REQ-OPS-001: Restart Durability

**WHEN** the process restarts
**THEN** catalog metadata, projection manifests, result sidecars, model
artifacts, pipeline artifacts, and durable freshness receipts SHALL either load
successfully or fail with a recoverable corruption report.

### REQ-OPS-002: Manifest Versioning

**WHEN** a stored artifact is opened
**THEN** the system SHALL validate format version, graph generation, checksum or
length metadata, feature flags, and required sidecar presence before serving it.

### REQ-OPS-003: Compatibility Versioning

**WHEN** GDS inventory is generated from a reference checkout
**THEN** the inventory SHALL record reference repo path, branch or tag, commit
hash where available, scan command, excluded paths, and generation timestamp.

### REQ-OPS-004: Telemetry

**WHEN** a procedure runs
**THEN** telemetry SHALL record estimate, selected plan, actual peak RSS where
available, scratch bytes, spill bytes, page-fault counters where available,
duration, row count, write count, and failure reason.

### REQ-OPS-005: Security And Access Control Boundary

**WHEN** a procedure accesses graph, model, pipeline, property, or writeback
state
**THEN** the architecture SHALL carry user/database context through the call even
if full Neo4j-compatible authorization is implemented later.

### REQ-OPS-006: Export And Import Boundaries

**WHEN** graph export, CSV export, database export, Arrow import, or equivalent
I/O-heavy procedures are called
**THEN** the system SHALL treat them as bounded streaming jobs with explicit
disk, memory, and cancellation behavior.

### REQ-OPS-007: Admin Surface

**WHEN** version, sysinfo, license, memory, debug, or feature procedures are
called
**THEN** the system SHALL route them through an admin/procedure plane independent
from topology storage.

### REQ-OPS-008: Documentation Traceability

**WHEN** a requirement is implemented
**THEN** the implementation PR SHALL cite the requirement ID, inventory rows,
and tests that prove the requirement.

## Testing Requirements

### REQ-TEST-001: Inventory Tests

Tests SHALL prove the local GDS inventory is complete and deterministic.

### REQ-TEST-002: ABI Golden Tests

Tests SHALL compare procedure names, modes, config argument names, and result
columns against checked inventory fixtures.

### REQ-TEST-003: Tiny Oracle Tests

Each implemented algorithm SHALL have tiny hand-computed graph fixtures.

### REQ-TEST-004: Flat CSR Oracle Tests

Each topology backend SHALL match flat CSR adjacency and global edge streams for
small generated graphs.

### REQ-TEST-005: Property Plane Tests

Tests SHALL cover labels, relationship types, numeric weights, missing values,
defaults, vector features, and null handling.

### REQ-TEST-006: Memory Contract Tests

Every implemented procedure SHALL have:

```text
estimate test
budget accept test
budget reject test
mmap honesty test if mmap is used
spill accounting test if spill is used
```

### REQ-TEST-007: Mode Tests

Every implemented mode SHALL have schema and side-effect tests:

| mode | test requirement |
| --- | --- |
| `stream` | no catalog mutation |
| `stats` | aggregate rows only |
| `mutate` | result sidecar exists and catalog reflects it |
| `write` | OLTP writeback count and property name validation |
| `estimate` | no algorithm execution |
| `train` | model artifact created |
| `predict` | prediction rows or writeback match schema |

### REQ-TEST-008: Crash And Cleanup Tests

Tests SHALL cover temp scratch cleanup, partial result failure, receipt replay,
and model/catalog durability.

## TDD Rollout Requirements

### REQ-ROLL-001: First Implementation PR

The first code-bearing PR SHALL be:

```text
GDS inventory and registry only.
```

It SHALL NOT implement Tilehouse or algorithm kernels.

### REQ-ROLL-002: Second Implementation PR

The second code-bearing PR SHALL be:

```text
Projection catalog skeleton and estimate shape.
```

### REQ-ROLL-003: Third Implementation PR

The third code-bearing PR SHALL be:

```text
GraphTopologyBackend over existing flat dual CSR.
```

### REQ-ROLL-004: Tilehouse Gate

Tilehouse SHALL NOT begin until flat CSR backend, property plane skeleton,
catalog skeleton, and memory estimate object exist.

### REQ-ROLL-005: Support Promotion Gate

No procedure SHALL move to `P1ExactLowRam` until:

```text
registry row exists
config parser exists
estimate exists
tiny oracle passes
flat CSR backend passes
mode schema test passes
budget reject test passes
determinism policy is documented
unsupported modes are deterministic
```

## Appendix A: Scanned Procedure Base Coverage

This appendix records the local procedure bases found in the GDS reference shelf
after excluding test fixtures and `proc/test`. It is not a substitute for the
future checked inventory artifact, but it is the current requirements coverage
map.

### `catalog`

| procedure base |
| --- |
| `gds.alpha.graph.graphProperty` |
| `gds.alpha.graph.nodeLabel` |
| `gds.alpha.graph.sample.rwr` |
| `gds.beta.graph.export.csv` |
| `gds.beta.graph.generate` |
| `gds.beta.graph.project.subgraph` |
| `gds.beta.graph.relationships` |
| `gds.beta.model` |
| `gds.graph` |
| `gds.graph.deleteRelationships` |
| `gds.graph.export` |
| `gds.graph.export.csv` |
| `gds.graph.filter` |
| `gds.graph.generate` |
| `gds.graph.graphProperty` |
| `gds.graph.nodeLabel` |
| `gds.graph.nodeProperties` |
| `gds.graph.nodeProperty` |
| `gds.graph.project` |
| `gds.graph.project.cypher` |
| `gds.graph.relationship` |
| `gds.graph.relationshipProperties` |
| `gds.graph.relationshipProperty` |
| `gds.graph.relationships` |
| `gds.graph.removeNodeProperties` |
| `gds.graph.sample.cnarw` |
| `gds.graph.sample.rwr` |
| `gds.graph.streamNodeProperties` |
| `gds.graph.streamNodeProperty` |
| `gds.graph.streamRelationshipProperties` |
| `gds.graph.streamRelationshipProperty` |
| `gds.graph.writeNodeProperties` |
| `gds.graph.writeRelationship` |
| `gds.internal.graph.sizeOf` |
| `gds.model` |

### `centrality`

| procedure base |
| --- |
| `gds.alpha.closeness.harmonic` |
| `gds.alpha.hits` |
| `gds.articleRank` |
| `gds.articulationPoints` |
| `gds.beta.closeness` |
| `gds.beta.influenceMaximization.celf` |
| `gds.betweenness` |
| `gds.bridges` |
| `gds.closeness` |
| `gds.closeness.harmonic` |
| `gds.degree` |
| `gds.eigenvector` |
| `gds.hits` |
| `gds.influenceMaximization.celf` |
| `gds.pageRank` |

### `community`

| procedure base |
| --- |
| `gds.alpha.conductance` |
| `gds.alpha.maxkcut` |
| `gds.alpha.modularity` |
| `gds.alpha.scc` |
| `gds.alpha.sllpa` |
| `gds.alpha.triangles` |
| `gds.beta.k1coloring` |
| `gds.beta.kmeans` |
| `gds.beta.leiden` |
| `gds.beta.modularityOptimization` |
| `gds.conductance` |
| `gds.k1coloring` |
| `gds.kcore` |
| `gds.kmeans` |
| `gds.labelPropagation` |
| `gds.leiden` |
| `gds.localClusteringCoefficient` |
| `gds.louvain` |
| `gds.maxkcut` |
| `gds.modularity` |
| `gds.modularityOptimization` |
| `gds.scc` |
| `gds.sllpa` |
| `gds.triangleCount` |
| `gds.triangles` |
| `gds.wcc` |

### `embeddings`

| procedure base |
| --- |
| `gds.beta.graphSage` |
| `gds.beta.hashgnn` |
| `gds.beta.node2vec` |
| `gds.fastRP` |
| `gds.hashgnn` |
| `gds.node2vec` |

### `machine-learning`

| procedure base |
| --- |
| `gds.alpha.ml.splitRelationships` |
| `gds.alpha.pipeline.linkPrediction.addMLP` |
| `gds.alpha.pipeline.linkPrediction.addRandomForest` |
| `gds.alpha.pipeline.linkPrediction.configureAutoTuning` |
| `gds.alpha.pipeline.nodeClassification.addMLP` |
| `gds.alpha.pipeline.nodeClassification.addRandomForest` |
| `gds.alpha.pipeline.nodeClassification.configureAutoTuning` |
| `gds.alpha.pipeline.nodeRegression` |
| `gds.alpha.pipeline.nodeRegression.addLinearRegression` |
| `gds.alpha.pipeline.nodeRegression.addNodeProperty` |
| `gds.alpha.pipeline.nodeRegression.addRandomForest` |
| `gds.alpha.pipeline.nodeRegression.configureAutoTuning` |
| `gds.alpha.pipeline.nodeRegression.configureSplit` |
| `gds.alpha.pipeline.nodeRegression.create` |
| `gds.alpha.pipeline.nodeRegression.predict` |
| `gds.alpha.pipeline.nodeRegression.selectFeatures` |
| `gds.beta.pipeline.linkPrediction` |
| `gds.beta.pipeline.linkPrediction.addFeature` |
| `gds.beta.pipeline.linkPrediction.addLogisticRegression` |
| `gds.beta.pipeline.linkPrediction.addNodeProperty` |
| `gds.beta.pipeline.linkPrediction.addRandomForest` |
| `gds.beta.pipeline.linkPrediction.configureSplit` |
| `gds.beta.pipeline.linkPrediction.create` |
| `gds.beta.pipeline.linkPrediction.predict` |
| `gds.beta.pipeline.nodeClassification` |
| `gds.beta.pipeline.nodeClassification.addLogisticRegression` |
| `gds.beta.pipeline.nodeClassification.addNodeProperty` |
| `gds.beta.pipeline.nodeClassification.addRandomForest` |
| `gds.beta.pipeline.nodeClassification.configureSplit` |
| `gds.beta.pipeline.nodeClassification.create` |
| `gds.beta.pipeline.nodeClassification.predict` |
| `gds.beta.pipeline.nodeClassification.selectFeatures` |
| `gds.ml.kge.predict` |

### `misc`

| procedure base |
| --- |
| `gds` |
| `gds.alpha.scaleProperties` |
| `gds.beta.collapsePath` |
| `gds.beta.graph.relationships.toUndirected` |
| `gds.beta.listProgress` |
| `gds.collapsePath` |
| `gds.features.adjacencyPackingStrategy` |
| `gds.features.adjacencyPackingStrategy.reset` |
| `gds.features.enableAdjacencyCompressionMemoryTracking` |
| `gds.features.enableAdjacencyCompressionMemoryTracking.reset` |
| `gds.features.enableArrowDatabaseImport` |
| `gds.features.enableArrowDatabaseImport.reset` |
| `gds.features.pagesPerThread` |
| `gds.features.pagesPerThread.reset` |
| `gds.features.useMixedAdjacencyList` |
| `gds.features.useMixedAdjacencyList.reset` |
| `gds.features.usePackedAdjacencyList` |
| `gds.features.usePackedAdjacencyList.reset` |
| `gds.features.useReorderedAdjacencyList` |
| `gds.features.useReorderedAdjacencyList.reset` |
| `gds.features.useUncompressedAdjacencyList` |
| `gds.features.useUncompressedAdjacencyList.reset` |
| `gds.graph.relationships.indexInverse` |
| `gds.graph.relationships.toUndirected` |
| `gds.listProgress` |
| `gds.memory` |
| `gds.memory.summary` |
| `gds.scaleProperties` |

### `path-finding`

| procedure base |
| --- |
| `gds.allShortestPaths` |
| `gds.allShortestPaths.delta` |
| `gds.allShortestPaths.dijkstra` |
| `gds.alpha.allShortestPaths` |
| `gds.alpha.kSpanningTree` |
| `gds.bellmanFord` |
| `gds.beta.spanningTree` |
| `gds.beta.steinerTree` |
| `gds.bfs` |
| `gds.dag.longestPath` |
| `gds.dag.topologicalSort` |
| `gds.dfs` |
| `gds.kSpanningTree` |
| `gds.prizeSteinerTree` |
| `gds.randomWalk` |
| `gds.shortestPath.astar` |
| `gds.shortestPath.dijkstra` |
| `gds.shortestPath.yens` |
| `gds.spanningTree` |
| `gds.steinerTree` |

### `pipeline-catalog`

| procedure base |
| --- |
| `gds.beta.pipeline` |
| `gds.pipeline` |

### `similarity`

| procedure base |
| --- |
| `gds.alpha.knn.filtered` |
| `gds.alpha.nodeSimilarity.filtered` |
| `gds.knn` |
| `gds.knn.filtered` |
| `gds.nodeSimilarity` |
| `gds.nodeSimilarity.filtered` |

### `sysinfo`

| procedure base |
| --- |
| `gds.debug.sysInfo` |
| `gds.license.state` |
| `gds.version` |

## Acceptance Checklist

This requirements document is complete enough for the next TDD phase when:

| item | status |
| --- | --- |
| It states Tilehouse is optional, not mandatory. | done |
| It defines the multi-plane architecture beyond CSR. | done |
| It covers catalog, algorithms, similarity, embeddings, ML, pipelines, model catalog, misc, operations, and sysinfo. | done |
| It defines procedure support levels. | done |
| It defines memory estimate requirements. | done |
| It defines freshness and update requirements. | done |
| It defines data model semantics beyond topology. | done |
| It defines execution, cancellation, progress, cleanup, and atomicity requirements. | done |
| It defines operational durability, telemetry, admin, and versioning requirements. | done |
| It lists every scanned procedure base by module as current surface coverage. | done |
| It defines test requirements before implementation. | done |
| It identifies inventory/registry as the first implementation PR. | done |


# v003 All-GDS Surface Supportability Matrix

> Purpose: answer the active architecture question directly:
>
> Can each Neo4j GDS surface area be supported by a Knight Bus CSR-centered OLAP
> architecture?

## Executive Verdict

Yes, **all scanned GDS surface areas can be represented by the proposed
CSR-centered multi-plane architecture**, but the reason is not "CSR can do
everything."

The real answer is:

```text
CSR supports topology.
Columnar sidecars support labels, types, weights, properties, and features.
Bounded scratch supports algorithms.
Result sidecars support mutate mode.
Writeback bridge supports write mode.
Artifact plane supports models and pipelines.
Admin/procedure plane supports sysinfo, license, feature, memory, and progress procedures.
```

Tilehouse is **not required** for full GDS surface support. It remains an
optional topology backend for freshness, dirty-region compaction, and locality if
measurements justify it.

## Evidence Snapshot

The local GDS reference shelf was scanned from `gitrefrepo/neo4j-gds-src`,
excluding test fixtures and `proc/test`.

| module | procedure bases | annotation rows | supportability verdict |
| --- | ---: | ---: | --- |
| `catalog` | 35 | 54 | supportable through catalog + topology + property planes |
| `centrality` | 15 | 96 | supportable through CSR topology + scratch vectors/frontiers |
| `community` | 26 | 154 | supportable; contraction-heavy algorithms need scratch artifacts |
| `embeddings` | 6 | 36 | supportable; dominated by embedding/model artifacts, not CSR |
| `machine-learning` | 33 | 46 | supportable only with model/pipeline artifact plane |
| `misc` | 28 | 39 | mixed; property transforms, admin settings, and derived topology |
| `path-finding` | 20 | 92 | supportable through CSR topology + weight sidecars + scratch |
| `pipeline-catalog` | 2 | 6 | supportable through artifact/catalog plane, not CSR |
| `similarity` | 6 | 44 | supportable with candidate generation + property/feature plane |
| `sysinfo` | 3 | 3 | supportable through admin/procedure plane, not CSR |
| **total** | **174** | **570** | **supportable by architecture; not by CSR alone** |

Key local source anchors:

| evidence | implication |
| --- | --- |
| `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/api/CSRGraphStoreFactory.java:67-87` builds graph stores from projected nodes and relationship imports. | A CSR-like topology substrate is compatible with GDS-style projected graphs. |
| `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/compression/varlong/CompressedAdjacencyList.java:44-112` estimates compressed pages, degrees, and offsets. | GDS itself treats adjacency, degrees, and offsets as central memory objects. |
| `gitrefrepo/neo4j-gds-src/core-api/src/main/java/org/neo4j/gds/api/GraphStore.java:48-238` includes properties, labels, relationship types, inverse indexes, and mutations. | CSR topology alone is insufficient; graph-store semantics need sidecar planes. |
| `gitrefrepo/neo4j-gds-src/procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/GraphDataScienceProcedures.java:30-44` exposes algorithms, graph catalog, model catalog, operations, and pipelines. | Full GDS surface is a product/API surface, not only algorithm kernels. |

## Supportability Categories

| category | meaning | CSR role |
| --- | --- | --- |
| `CSR-native` | Directly adjacency/degree/edge-stream driven. | CSR is the main substrate. |
| `CSR + property plane` | Needs topology plus labels/types/weights/properties/features. | CSR supplies graph shape; sidecars supply semantics. |
| `CSR + scratch plane` | Needs topology plus vectors/frontiers/heaps/workspaces. | CSR supplies iteration; scratch supplies algorithm state. |
| `Derived topology` | Creates logical/sidecar relationships or inverse indexes. | CSR is input; derived sidecar may become another topology view. |
| `Result sidecar/writeback` | Procedure writes projected result or OLTP-facing property/relationship. | CSR is input; output is sidecar/writeback. |
| `Artifact plane` | Models, pipelines, training metadata, metrics, and artifacts. | CSR may provide features, but artifact plane carries the surface. |
| `Admin/procedure plane` | Sysinfo, version, license, progress, memory, feature toggles. | CSR is incidental or irrelevant. |
| `High-risk exact` | Supportable, but may reject under 8 GB due to state/output size. | CSR does not remove the dominant memory cost. |

## Surface Area Summary

| GDS surface area | CSR-centered support? | Tilehouse needed? | dominant non-CSR plane | 50GB/8GB risk |
| --- | --- | --- | --- | --- |
| Graph projection/catalog | Yes | No | catalog + property plane | medium |
| Native/Cypher projection | Yes, with compatibility parser/executor | No | catalog + Cypher/projection bridge | medium-high |
| Graph filtering/subgraph | Yes | No | catalog logical views | medium |
| Graph export/import/sample/generate | Yes | No | streaming I/O jobs | medium-high |
| Node/relationship/graph properties | Yes | No | columnar property plane | medium |
| Relationship transforms | Yes | No | derived topology sidecars | medium |
| Centrality | Yes | No | scratch vectors/frontiers | medium to very high |
| Pathfinding | Yes | No | weight sidecars + scratch | medium to very high |
| Community/structure | Yes | No | scratch/result/contracted graph artifacts | medium to high |
| Similarity | Yes, with candidate strategy | No | candidate/topK/property plane | high to very high |
| Embeddings | Yes, but CSR is not dominant | No | embedding/model/result artifacts | very high |
| ML/pipelines | Yes, but not by CSR | No | artifact plane | high |
| Model catalog | Yes, not by CSR | No | artifact plane | low to medium |
| Progress/memory/features | Yes, not by CSR | No | admin/procedure plane | low |
| Sysinfo/version/license | Yes, not by CSR | No | admin/procedure plane | low |

## Catalog Surface

Catalog is supportable, but it is mostly **not algorithm work**. It is the named
graph and projection contract that algorithms depend on.

| procedure base group | support category | CSR fit | required planes | Tilehouse needed? | notes |
| --- | --- | --- | --- | --- | --- |
| `gds.graph.project`, `gds.graph.project.cypher` | `CSR + property plane` | strong | catalog, topology, property, Cypher/projection bridge | no | Native projection can target flat CSR first; Cypher projection needs query compatibility work. |
| `gds.graph`, `gds.graph.filter`, `gds.beta.graph.project.subgraph` | `CSR + property plane` | strong | catalog logical views, filters, sidecars | no | Filters should be views over topology/property planes when possible. |
| `gds.graph.nodeProperties`, `gds.graph.nodeProperty`, stream/write variants | `CSR + property plane` | indirect | property plane, result sidecars, writeback | no | CSR maps dense IDs; property plane does the real work. |
| `gds.graph.relationshipProperties`, `gds.graph.relationshipProperty`, stream/write variants | `CSR + property plane` | indirect | edge-aligned properties, type filters | no | Needs edge-position identity and relationship type filters. |
| `gds.graph.graphProperty`, `gds.alpha.graph.graphProperty` | `Result sidecar/writeback` | none | graph property catalog | no | Graph-level metadata, not topology. |
| `gds.graph.nodeLabel`, `gds.alpha.graph.nodeLabel` | `CSR + property plane` | indirect | label bitsets/columns, catalog mutation | no | Mutating projected labels is sidecar/catalog work. |
| `gds.graph.relationships`, `gds.beta.graph.relationships`, `gds.graph.relationship` | `CSR-native` plus sidecars | strong | topology streams, relationship type filters | no | Can stream from CSR backend. |
| `gds.graph.relationships.toUndirected`, `gds.graph.relationships.indexInverse` | `Derived topology` | strong input | derived topology sidecars, inverse index metadata | no | Flat CSR already has reverse topology; per-type inverse metadata still needed. |
| `gds.graph.deleteRelationships`, `gds.graph.removeNodeProperties` | `Result sidecar/writeback` | indirect | catalog mutation, sidecar deletion | no | Must be atomic at projection boundary. |
| `gds.graph.export`, `gds.graph.export.csv`, `gds.beta.graph.export.csv` | `Admin/procedure plane` plus streams | medium | streaming I/O job, property/topology cursors | no | Memory safety depends on streaming output, not Tilehouse. |
| `gds.graph.generate`, `gds.beta.graph.generate` | `Admin/procedure plane` | medium | generator, catalog, topology writer | no | Can emit flat CSR generation first. |
| `gds.graph.sample.rwr`, `gds.graph.sample.cnarw`, alpha sample | `CSR + scratch plane` | strong | random walk/sampling scratch, seeded RNG | no | Stochastic determinism is required. |
| `gds.internal.graph.sizeOf` | `Admin/procedure plane` | indirect | memory estimator | no | Needs holistic estimate model. |
| `gds.model`, `gds.beta.model` in catalog module | `Artifact plane` | none | model catalog bridge | no | Belongs with model catalog requirements. |

Verdict: **supportable.** CSR gives topology identity and streams; catalog and
property planes carry most user-visible semantics.

## Centrality Surface

Centrality is mostly CSR-friendly. The hard part is memory state, not topology.

| procedure base group | support category | CSR fit | required planes | 50GB/8GB risk | notes |
| --- | --- | --- | --- | --- | --- |
| `gds.degree` | `CSR-native` | excellent | optional weights, result sidecar | low | Degree can read offsets and filters. |
| `gds.pageRank`, `gds.articleRank`, `gds.eigenvector` | `CSR + scratch plane` | excellent | vectors, degree/weight sidecars, convergence state | high | Vectors dominate memory. Strict mode may spill or reject. |
| `gds.hits`, `gds.alpha.hits` | `CSR + scratch plane` | excellent | hub/authority vectors, forward/reverse streams | high | Needs both directions and multiple vectors. |
| `gds.closeness`, `gds.closeness.harmonic`, alpha/beta variants | `CSR + scratch plane` | strong | repeated BFS/SSSP frontiers, distances | very high | All-pairs mode likely rejects under low budget. |
| `gds.betweenness` | `High-risk exact` | strong | Brandes state, predecessor/sigma/delta, source batching | very high | Supportable, but exact large graph runs may be rejected. |
| `gds.articulationPoints`, `gds.bridges` | `CSR + scratch plane` | strong | DFS low-link arrays, deterministic traversal | medium | Needs robust DFS over backend. |
| `gds.influenceMaximization.celf`, beta variant | `High-risk exact` | partial | simulation/candidate heap, stochastic state | high | Supportable after sampling/seed/memory contracts. |

Verdict: **supportable.** CSR is the right substrate, but estimates must be
honest about O(V) and repeated-source state.

## Pathfinding Surface

Pathfinding is CSR-friendly, with weight and output sidecars.

| procedure base group | support category | CSR fit | required planes | 50GB/8GB risk | notes |
| --- | --- | --- | --- | --- | --- |
| `gds.bfs`, `gds.dfs` | `CSR + scratch plane` | excellent | visited/frontier/path buffers | medium | Source-limited traversals are feasible. |
| `gds.shortestPath.dijkstra`, `gds.shortestPath.astar` | `CSR + property plane` | strong | weight sidecars, priority queue, heuristic config | medium-high | A* also needs coordinate/heuristic properties. |
| `gds.shortestPath.yens` | `High-risk exact` | strong | repeated Dijkstra, candidate path heap | high | K and path outputs dominate. |
| `gds.allShortestPaths`, alpha variant | `High-risk exact` | strong | repeated source sweeps or global distance state | very high | Supportable as registered/exact, often rejected by budget. |
| `gds.allShortestPaths.delta`, `gds.allShortestPaths.dijkstra` | `CSR + scratch plane` | strong | distance vectors, buckets/queues, weights | high | Needs strict estimate and spill policy. |
| `gds.bellmanFord` | `CSR + scratch plane` | strong | edge stream, distance vectors, negative-cycle behavior | medium-high | Global stream works well. |
| `gds.randomWalk` | `CSR + scratch plane` | strong | RNG, walk buffers/corpus, boundary handoff | medium-high | Seed determinism is the hard contract. |
| `gds.spanningTree`, `gds.kSpanningTree`, alpha/beta variants | `CSR + property plane` | strong | weights, parent arrays, output relationships | medium-high | Write mode needs relationship writeback. |
| `gds.steinerTree`, `gds.prizeSteinerTree`, beta variant | `High-risk exact` | medium | prizes, weights, heaps, tree output | high | More algorithm-specific scratch. |
| `gds.dag.topologicalSort`, `gds.dag.longestPath` | `CSR + scratch plane` | strong | indegree/ordering arrays, cycle detection | medium | DAG validation required. |

Verdict: **supportable.** CSR is natural; weight/property semantics and path
outputs need sidecars and budgeted scratch.

## Community And Structure Surface

Community algorithms are supportable but increasingly scratch-heavy.

| procedure base group | support category | CSR fit | required planes | 50GB/8GB risk | notes |
| --- | --- | --- | --- | --- | --- |
| `gds.wcc` | `CSR + scratch plane` | excellent | component array, frontier/union state | medium | Good early implementation target. |
| `gds.scc`, alpha variant | `CSR + scratch plane` | excellent | forward/reverse DFS stacks, component arrays | medium | Flat dual CSR helps. |
| `gds.triangleCount`, `gds.triangles`, alpha variants | `CSR-native` plus scratch | strong | sorted adjacency/intersections | high for hubs | Requires intersection-capable cursors. |
| `gds.localClusteringCoefficient` | `CSR-native` plus scratch | strong | triangle/local degree state | high for hubs | Same intersection concern. |
| `gds.kcore` | `CSR + scratch plane` | strong | degree array, peel queue, output sidecar | medium | Good structural target. |
| `gds.k1coloring`, beta variant | `CSR + scratch plane` | strong | color array, conflict frontier | medium | Needs deterministic tie-breaking. |
| `gds.labelPropagation` | `CSR + scratch plane` | strong | labels, iterations, tie-breaking | medium-high | Output sidecar natural. |
| `gds.sllpa`, alpha variant | `High-risk exact` | strong | label distributions per node | high | Distribution state can grow. |
| `gds.louvain`, `gds.leiden`, beta variant | `CSR + scratch plane` | strong | contracted graph artifacts, community arrays | high | Requires contracted-graph sidecar. |
| `gds.modularity`, `gds.modularityOptimization`, alpha/beta variants | `CSR + property plane` | strong | community property, cut/internal scans | medium-high | Depends on community sidecar. |
| `gds.conductance`, alpha variant | `CSR + property plane` | strong | community/partition property, edge cuts | medium | Stream-friendly. |
| `gds.maxkcut`, alpha variant | `CSR + scratch plane` | strong | partition arrays, randomized/iterative state | high | Seed/config contract required. |
| `gds.kmeans`, beta variant | `CSR + property plane` | weak topology | feature vectors, centroids, assignments | high | More feature-matrix than graph topology. |

Verdict: **supportable.** CSR covers the graph walk/scan; contracted graphs and
feature matrices are separate scratch/property concerns.

## Similarity Surface

Similarity is supportable, but not by raw CSR scans alone at large scale.

| procedure base group | support category | CSR fit | required planes | 50GB/8GB risk | notes |
| --- | --- | --- | --- | --- | --- |
| `gds.nodeSimilarity`, alpha filtered variant | `High-risk exact` | medium-strong | candidate generation, overlap scans, topK heaps | very high | Must not materialize all pairs by default. |
| `gds.nodeSimilarity.filtered` | `CSR + property plane` | medium-strong | filter pushdown, candidate pruning | high | Filters are essential for RAM. |
| `gds.knn`, alpha filtered variant | `CSR + property plane` | weak topology | feature vectors, candidate sampler, topK heaps | very high | KNN is feature-plane dominant. |
| `gds.knn.filtered` | `CSR + property plane` | weak topology | feature filters, topK, vector scan | high | CSR mostly maps IDs and filters. |

Verdict: **supportable with constraints.** The architecture supports it only if
candidate generation and topK spill policies are first-class.

## Embeddings Surface

Embeddings are supportable, but CSR is input rather than the dominant storage.

| procedure base group | support category | CSR fit | required planes | 50GB/8GB risk | notes |
| --- | --- | --- | --- | --- | --- |
| `gds.fastRP` | `CSR + scratch plane` | strong | propagation state, embedding output sidecar | very high | Embedding matrix dominates. |
| `gds.node2vec`, beta variant | `CSR + scratch plane` | strong | random walks, walk corpus, embeddings | very high | Walk corpus and training state dominate. |
| `gds.beta.graphSage` | `Artifact plane` plus CSR | medium | model artifact, batches, features, sampling | very high | Training/inference framework required. |
| `gds.hashgnn`, beta variant | `CSR + property plane` | medium | hash features, embeddings | high | Deterministic hashing required. |

Verdict: **supportable**, but full support requires model/artifact and embedding
sidecar planes. CSR alone is far from enough.

## Machine Learning And Pipeline Surface

ML and pipelines are product/workflow surfaces. CSR helps feature extraction,
but the support lives in artifact management.

| procedure base group | support category | CSR fit | required planes | 50GB/8GB risk | notes |
| --- | --- | --- | --- | --- | --- |
| `gds.beta.pipeline.nodeClassification*`, alpha variants | `Artifact plane` | indirect | pipeline metadata, feature schema, models, metrics | high | Needs pipeline lifecycle and training runtime. |
| `gds.alpha.pipeline.nodeRegression*` | `Artifact plane` | indirect | target property, feature schema, model candidates | high | Same as classification with regression metrics. |
| `gds.beta.pipeline.linkPrediction*`, alpha variants | `Artifact plane` plus CSR | medium | relationship splits, negative sampling, features | high | CSR useful for topology features. |
| `gds.alpha.ml.splitRelationships` | `CSR + property plane` | medium | split sidecar, seeded sampling | medium | Supports ML setup, not model training. |
| `gds.ml.kge.predict` | `Artifact plane` plus property plane | medium | typed relationships, embeddings, model artifact | very high | Knowledge graph embeddings dominate. |

Verdict: **supportable only with a real model/pipeline artifact plane.** CSR is
necessary for graph features but insufficient for the surface.

## Pipeline Catalog Surface

| procedure base group | support category | CSR fit | required planes | 50GB/8GB risk | notes |
| --- | --- | --- | --- | --- | --- |
| `gds.pipeline`, `gds.beta.pipeline` | `Artifact plane` | none | pipeline catalog, owner/database identity | low | CRUD/list/exists/drop style surface. |

Verdict: **supportable, not CSR-dependent.**

## Misc Surface

Misc contains property transforms, feature toggles, memory/progress, and derived
topology helpers. It is mixed by nature.

| procedure base group | support category | CSR fit | required planes | 50GB/8GB risk | notes |
| --- | --- | --- | --- | --- | --- |
| `gds.scaleProperties`, alpha variant | `CSR + property plane` | none/indirect | columnar property scan, output sidecar | medium | Property-only OLAP. |
| `gds.collapsePath`, beta variant | `Derived topology` | strong input | derived relationship sidecar | medium-high | May emit new relationship view. |
| `gds.graph.relationships.toUndirected`, beta variant | `Derived topology` | strong | logical undirected view or derived sidecar | medium | No base rewrite needed. |
| `gds.graph.relationships.indexInverse` | `Derived topology` | strong | inverse index metadata | medium | Flat dual CSR helps. |
| `gds.listProgress`, beta variant | `Admin/procedure plane` | none | task registry/progress state | low | Execution telemetry. |
| `gds.memory`, `gds.memory.summary` | `Admin/procedure plane` | indirect | estimator/telemetry | low | Holistic memory reporting. |
| `gds.features.*` and `.reset` | `Admin/procedure plane` | none | feature/config registry | low | Must map to Knight Bus settings or unsupported. |
| `gds` | `Admin/procedure plane` | none | top-level metadata/help behavior | low | Needs procedure facade behavior. |

Verdict: **supportable**, but mostly outside CSR.

## Sysinfo Surface

| procedure base group | support category | CSR fit | required planes | 50GB/8GB risk | notes |
| --- | --- | --- | --- | --- | --- |
| `gds.version` | `Admin/procedure plane` | none | build/version metadata | low | Straight server metadata. |
| `gds.license.state` | `Admin/procedure plane` | none | license/edition policy | low | Needs product policy. |
| `gds.debug.sysInfo` | `Admin/procedure plane` | none | runtime/system info | low | Must avoid leaking unsupported internals accidentally. |

Verdict: **supportable, not CSR-dependent.**

## Per-Plane Completion Requirements

The full surface becomes supportable only when these planes exist:

| plane | proves support for |
| --- | --- |
| procedure ABI registry | every scanned procedure can be known, classified, and routed |
| catalog plane | graph/model/pipeline lifecycle and named projections |
| topology backend | CSR-native algorithms and graph streams |
| columnar property plane | labels, types, weights, features, scalar/vector properties |
| scratch plane | vectors, frontiers, heaps, candidate sets, contracted graphs, walk corpora |
| result sidecar plane | mutate mode and projected result properties |
| OLTP writeback bridge | write mode and persistent output counts |
| artifact plane | embeddings, models, pipelines, training metadata |
| admin/procedure plane | sysinfo, version, license, memory, features, progress |
| freshness bridge | snapshot/delta/refresh policy across all projections |

## Tilehouse Decision

Tilehouse is not required by any surface category.

| question | answer |
| --- | --- |
| Does catalog require Tilehouse? | no |
| Do centrality/pathfinding/community require Tilehouse? | no |
| Do similarity/embeddings/ML require Tilehouse? | no |
| Do model/pipeline/sysinfo require Tilehouse? | no |
| What does Tilehouse help? | update locality, dirty-region compaction, local page-window planning |
| When should Tilehouse start? | only after flat CSR backend, catalog, property plane, and memory estimator exist |

Decision:

```text
Flat generationed dual CSR SHALL be the first topology backend.
Tilehouse MAY be added only after measured freshness/locality pressure.
The GDS supportability plan SHALL NOT depend on Tilehouse.
```

## Risk Register

| risk | affected surface | mitigation |
| --- | --- | --- |
| Procedure inventory misses aliases or generated procedures. | all | checked inventory generator and ABI golden tests |
| CSR-only thinking leaks into requirements. | properties, ML, pipelines, admin | enforce multi-plane architecture |
| Memory estimates undercount page cache or scratch. | all algorithms | holistic estimate object and mmap honesty tests |
| Similarity materializes pair explosion. | similarity | candidate generation and topK spill gates |
| Embeddings silently allocate huge matrices. | embeddings, ML | explicit dimension and bytes-per-value estimates |
| Write/mutate modes create partial state. | catalog, algorithms, ML | atomic result sidecar/writeback contracts |
| Freshness semantics surprise users. | all OLAP procedures | generation/freshness watermark and explicit mode |
| Tilehouse starts too early. | topology/backend | keep flat CSR as first backend and require measured trigger |

## Final Supportability Answer

| claim | verdict |
| --- | --- |
| CSR alone supports all GDS. | false |
| CSR-centered multi-plane architecture supports all scanned GDS surface areas. | true as an architecture target |
| Tilehouse is required to support all GDS. | false |
| Some procedures may be registered but rejected under an 8 GB budget. | true |
| Full GDS support requires non-graph product surfaces. | true |

The architecture can support the full surface if it stays honest:

```text
Do not force every GDS feature into topology.
Do not start with Tilehouse.
Do build the procedure registry, catalog, property plane, memory estimator,
scratch/result/artifact/admin planes, then run algorithms over flat CSR first.
```

# v003 Diligence: CSR Tiles + GDS Surface TDD Plan

> Purpose: turn the v003 OLAP architecture direction into a source-backed,
> test-first diligence plan.
>
> Short answer: the earlier draft is a strong scaffold, but it is not complete
> enough to become the implementation source of truth. It must be expanded with
> local source evidence, GDS procedure inventory gates, realistic v003 scope, and
> a stricter memory contract.

## Verdict

Build the diligence sequence around this thesis:

```text
Neo4j-shaped OLTP remains the source of truth.
Flat dual CSR remains the proven OLAP oracle and global stream primitive.
Cellular CSR Tilehouse is the update-aware evolution if freshness pressure returns.
GDS compatibility is an ABI/inventory problem before it is an algorithm problem.
```

### Update: If Freshness Can Lag By Hours

If OLAP freshness is not a hard requirement and a few hours of lag is acceptable,
the storage recommendation becomes sharper:

```text
Default v003 OLAP storage should stay immutable dual CSR plus mmap/streaming
sidecars.

Cellular CSR Tilehouse should move from "preferred default evolution" to
"optional later update-locality tier."
```

Reason:

| point | implication |
| --- | --- |
| Small OLTP updates do not need immediate OLAP visibility. | The main reason to pay Tilehouse complexity disappears from the RAM-first v003 path. |
| Immutable files are easiest to mmap, stream, checksum, compress, swap, and test. | They are the lowest-risk way to keep server RAM predictable. |
| Most GDS algorithms read a stable graph and write algorithm state elsewhere. | The hard memory problem is usually vectors, frontiers, heaps, candidates, embeddings, or output sidecars, not live adjacency mutation. |
| Full snapshot rebuilds can run every few hours under a build memory budget. | Rebuild/swap is operationally simpler than delta overlays and cell compaction. |

The revised thesis for this assumption is:

```text
Use one immutable CSR topology plane, one immutable/mmap property-sidecar plane,
and bounded runtime scratch/spill/result sidecars.

Do not build Tilehouse or WAL-fed deltas until freshness pressure is measured.
```

The proposed note gets the important intuition right:

| claim | verdict |
| --- | --- |
| CSR tiles alone do not prove GDS compatibility. | correct |
| Procedure inventory should come before tile partitioning. | correct |
| Flat CSR should be the oracle before Tilehouse is trusted. | correct |
| Memory estimates must be executable contracts. | correct |
| Cellular CSR is better for update locality than flat global CSR. | correct |

But it is incomplete in five important ways:

| gap | why it matters |
| --- | --- |
| It treats "full GDS surface" too casually. | The local GDS shelf exposes hundreds of `gds.*` procedure annotations, not a small PageRank/BFS set. |
| It needs scope levels. | v003 cannot honestly implement all algorithms, modes, pipelines, model catalog, and writeback in one first release. |
| It overstates `O_DIRECT + compio` as the preferred OLAP baseline. | Current Knight Bus is mmap-first, and prior local analysis says not to chase `compio` for the OLAP walker first. |
| It needs exact source references. | Implementation agents need line-anchored evidence, not remembered architecture claims. |
| It needs hard unsupported behavior. | A Neo4j-compatible surface must distinguish "known but unsupported" from "unknown procedure." |

## Premise Check

### Target

v003 is not "add a faster PageRank." The target is:

```text
Keep OLTP Neo4j-shaped.
Add a low-RAM OLAP plane.
Preserve the external Neo4j/GDS-style surface over time.
Make every storage, API, and memory claim testable before implementation.
```

### Non-Goals For This Diligence Step

This document does not propose:

| non-goal | reason |
| --- | --- |
| Replacing the OLTP record store immediately | The current decision frame keeps OLTP Neo4j-shaped. |
| Implementing all GDS algorithms in v003 | The GDS surface is too large; first pass must classify and register it. |
| Replacing the current mmap walker with `compio` | Local analysis says the hot OLAP walk path is dense-array and memory-bandwidth dominated. |
| Adding code in this pass | This is a research/TDD planning artifact, not implementation. |

### Lowest Holistic RAM

"Lowest RAM" means the total machine-visible cost, not just Rust heap:

```text
heap
RSS
OS page cache
mmap residency
direct I/O buffers
projection/catalog metadata
algorithm vectors/frontiers/heaps
delta overlays
compaction scratch
snapshot build scratch
indexes and sidecars
```

The business-visible target is still the same:

```text
Can a 50 GB-class graph be useful on an 8 GB-class single machine?
```

That does not mean every algorithm is feasible under 8 GB. It means the engine
must know, before execution, whether a chosen graph/procedure/configuration fits
the requested memory contract.

## Local Evidence Ledger

### Knight Bus Evidence

| fact | local evidence | implication |
| --- | --- | --- |
| v002 is intentionally immutable dual CSR plus mmap. | `README.md:9-17` says v002 keeps the Rust runtime in the immutable dual-CSR plus mmap shape. | The current implementation is a valid seed, not something to discard casually. |
| v002 showed lower runtime RSS than Neo4j on the measured datasets. | `README.md:29-41` reports lower RAM and faster traversal latency for `1 MB`, `50 MB`, and `2 GB`. | The existing mmap CSR path is already part of the moat. |
| Snapshot output is a small fixed file set. | `src/snapshot.rs:14-21` names `manifest.json`, `node_table.bin`, `strings.bin`, forward/reverse offsets, peers, and `key_index.bin`. | Tilehouse should evolve this shape, not explode it into per-algorithm layouts. |
| Snapshot writing is abstracted behind `SnapshotArtifactWriter`. | `src/snapshot.rs:23-29` defines `write_snapshot_artifacts`. | A Tilehouse writer can be additive without deleting the flat writer. |
| Current manifest declares version `2` and `immutable_dual_csr`. | `src/snapshot.rs:104-120` builds the manifest and sets `storage_mode` to `immutable_dual_csr`. | v3 should preserve v2 readability and add Tilehouse metadata intentionally. |
| The runtime is walk-focused. | `src/runtime.rs:22-39` exposes `WalkQueryRuntime` with neighbor queries, family queries, all keys, and snapshot size. | GDS algorithms need a separate algorithm-facing graph access layer. |
| The runtime maps the snapshot files with `memmap2::Mmap`. | `src/runtime.rs:41-53` stores forward/reverse offsets, peers, node table, strings, and key index as mmaps. | Current RAM behavior is low explicit heap but page-cache mediated. |
| Current graph normalization is topology-only. | `src/types.rs:265-280` has node keys plus forward/reverse offsets and peers. | Labels, relationship types, weights, and properties must be sidecars. |
| Current query families are only 1-hop/2-hop forward/backward. | `src/types.rs:145-183` defines `ForwardOne`, `BackwardOne`, `ForwardTwo`, and `BackwardTwo`. | Existing API proves walks, not GDS compatibility. |
| Current build memory budget exists. | `src/types.rs:419-456` defines `BuildMemoryBudget` and spill buffer sizing. | v003 should generalize this idea into projection and algorithm execution budgets. |
| Current tests are narrow. | `tests/cli.rs` and `tests/library_contract.rs` are the only top-level test files in this pass. | The v003 plan needs a larger compatibility and memory test ladder. |

### Neo4j GDS Evidence

| fact | local evidence | implication |
| --- | --- | --- |
| GDS has a facade larger than algorithms alone. | `gitrefrepo/neo4j-gds-src/procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/GraphDataScienceProcedures.java:30-44` exposes log, algorithms, graph catalog, model catalog, operations, pipelines, and deprecated procedure metrics. | A GDS-compatible surface includes catalog and operations behavior, not just kernels. |
| Algorithms are grouped into facade families. | `gitrefrepo/neo4j-gds-src/procedures/algorithms-facade-api/src/main/java/org/neo4j/gds/procedures/algorithms/AlgorithmsProcedureFacade.java:34-88` exposes centrality, community, machine learning, miscellaneous, embeddings, path finding, and similarity. | The implementation registry should classify by family. |
| The graph catalog is a large API surface. | `gitrefrepo/neo4j-gds-src/procedures/graph-catalog-facade-api/src/main/java/org/neo4j/gds/procedures/catalog/GraphCatalogProcedureFacade.java:49-180` includes exists, drop, list, native/cypher project, estimates, filtering, size, property streams, topology streams, and writebacks. | Catalog semantics must come before algorithms. |
| PageRank stream has both execution and estimate procedures. | `gitrefrepo/neo4j-gds-src/proc/centrality/src/main/java/org/neo4j/gds/pagerank/PageRankStreamProc.java:41-57` defines `gds.pageRank.stream` and `gds.pageRank.stream.estimate`. | Each implemented procedure mode needs a paired memory estimate contract. |
| GDS has an explicit CSR graph type. | `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/api/CSRGraph.java:26-35` describes a graph subtype exposing CSR-specific structures such as `AdjacencyList`. | CSR is not an exotic Knight Bus-only bet; it matches how GDS exposes projected graph storage internally. |
| GDS projections build a CSR graph store. | `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/api/CSRGraphStoreFactory.java:67-86` builds graph-store state from nodes and imported relationships. | A durable immutable CSR projection is a fair low-RAM alternative to GDS's in-memory projection catalog. |
| GDS compressed adjacency stores pages, degrees, and offsets. | `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/compression/varlong/CompressedAdjacencyList.java:44-86` estimates compressed pages plus per-node degree and offset arrays; lines `101-110` store pages, degrees, and offsets. | A low-RAM Rust rewrite should study compression, but raw immutable CSR remains the simplest correctness baseline. |
| GDS adjacency access is cursor-oriented. | `gitrefrepo/neo4j-gds-src/core-api/src/main/java/org/neo4j/gds/api/AdjacencyList.java:33-104` exposes degree and adjacency cursor access. | Knight Bus algorithms can target cursor traits over immutable CSR without knowing the file layout. |
| GDS itself treats many algorithm states as memory-estimated per-node structures. | Examples: BFS estimates visited/result arrays in `BfsMemoryEstimateDefinition.java:32-69`; WCC estimates disjoint-set state in `WccMemoryEstimateDefinition.java:27-40`; KNN estimates per-node top-k and neighbor lists in `KnnMemoryEstimateDefinition.java:36-90`; FastRP estimates per-node embedding arrays in `FastRPMemoryEstimateDefinition.java:28-49`. | The limiting RAM is often algorithm state, not CSR topology. |
| Local GDS procedure surface is very large. | A read-only scan over non-test Java sources found `567` unique `gds.*` annotations and `575` annotation rows. | v003 must use support levels instead of pretending full implementation is immediate. |

### Procedure Scan Result

The local scan excluded Java paths containing `/src/test/`, `/integrationTest/`,
`/test-utils/`, and `/src/testFixtures/`.

| prefix | unique procedure count |
| --- | ---: |
| all `gds.*` annotations | 567 |
| `gds.graph.*` | 41 |
| `gds.pageRank*` | 8 |
| `gds.bfs*` | 6 |
| `gds.wcc*` | 8 |
| `gds.fastRP*` | 8 |
| `gds.beta.pipeline*` | 29 |
| `gds.model*` | 3 |

This is a coarse inventory count, not the final compatibility manifest. The
first implementation PR should check in a deterministic inventory generator or
checked TSV and test it.

### Existing Dirty Workspace Context

At the time this diligence doc was prepared, the working tree already had
uncommitted docs/reference-shelf housekeeping:

```text
M .gitignore
M docs/strategic-research/A-20260416121710-storage-runtime-alignment-eli5.md
M docs/strategic-research/A-20260525164835-faithful-rust-port-dossier.md
M docs_PRD02/Low-RAM-OLAP-Format-Variants.md
?? Docs-Readme.md
```

Implementation agents should stage this diligence file separately unless the
user explicitly asks to include the existing reference-shelf changes.

## Source, Inference, Speculation

| statement | type | confidence | evidence or reason |
| --- | --- | --- | --- |
| Current Knight Bus storage is flat immutable dual CSR with mmap runtime. | sourced fact | high | `README.md:13-17`, `src/snapshot.rs:14-21`, `src/runtime.rs:41-53` |
| Current Knight Bus does not yet have a GDS-compatible procedure/catalog layer. | sourced inference | high | Current public runtime is `WalkQueryRuntime`, and current tests are CLI/library walk contracts. |
| GDS compatibility is larger than algorithm kernels. | sourced fact | high | `GraphDataScienceProcedures.java:30-44`, `GraphCatalogProcedureFacade.java:49-180` |
| First v003 work should inventory/register procedures before implementing Tilehouse. | architecture inference | high | Prevents building storage that does not match the public surface. |
| Cellular CSR improves update locality and compaction locality over a single flat snapshot. | architecture inference | medium-high | Dirty cell and bounded compaction units reduce work for small localized updates. |
| Cellular CSR will reduce global algorithm RAM versus a flat explicit stream. | speculation | low | For full-graph algorithms, vectors/frontiers often dominate; tiles mostly improve planning and locality. |
| If hours of OLAP lag are acceptable, immutable CSR is the best default topology for lowest RAM. | architecture inference | high | It minimizes duplicate topology, avoids delta/compaction metadata, and supports mmap plus sequential streaming. |
| Immutable CSR is sufficient as the topology substrate for all listed GDS families. | architecture inference | medium-high | All families can be expressed as adjacency scans, neighbor cursors, edge streams, or property-column scans, with algorithm state outside topology. |
| Immutable CSR guarantees all GDS algorithms can fit on an 8 GB machine. | speculation rejected | high | Similarity, embeddings, all-pairs shortest paths, and some community algorithms are dominated by output/model/candidate state. |
| `compio` should not replace the OLAP mmap walker first. | sourced inference | high | `docs_PRD02/User-Journey-50GB-OLTP-OLAP-Lag.md:198-241`, `300-310`, `420-427` |
| `O_DIRECT` or explicit I/O should exist for strict-RAM global algorithms. | architecture inference | medium | It can avoid page-cache surprise, but adds complexity and platform constraints. |

## Corrected I/O Thesis

The pasted draft says:

```text
O_DIRECT + compio is preferred for deterministic RAM because it bypasses page
cache and makes buffer sizes explicit.
```

The corrected v003 stance should be:

```text
Keep mmap for the proven interactive/static CSR walk path.
Use explicit I/O, and possibly O_DIRECT, for strict-RAM global algorithms,
snapshot build, refresh, and compaction paths where page-cache residency must
not be counted as a surprise.
Borrow compio/Iggy ideas for segmented persistence and mutable-plane refresh
only after measurements show the refresh path is I/O-bound.
```

Reason:

| path | recommended I/O stance |
| --- | --- |
| 1-hop/2-hop static walks | mmap first |
| local immutable CSR reads | mmap CSR windows first |
| optional later Tilehouse cell reads | mmap or bounded mapped windows first |
| global PageRank strict-RAM mode | explicit stream or O_DIRECT candidate |
| snapshot build external sort | bounded buffered I/O first; O_DIRECT only if measured useful |
| WAL receipts and mutable delta logs | borrow segmented log ideas from Iggy; runtime choice is secondary |
| compaction | bounded buffered or explicit I/O; prove with RSS and page-cache metrics |

## Immutable CSR Suitability Under Hours-Lag Assumption

### Premise Check

If freshness can lag by a few hours, the main question is no longer:

```text
How do we make CSR update-local?
```

It becomes:

```text
Is one immutable CSR projection plus sidecars enough for the whole GDS-style
OLAP surface, and is it the lowest-RAM default?
```

Answer:

```text
Yes for topology.
No as a blanket guarantee for every algorithm fitting in 8 GB.
```

The careful claim is:

| claim | confidence | reason |
| --- | --- | --- |
| Immutable dual CSR is the best default topology for lowest RAM when freshness lag is allowed. | high | It has no live delta, tombstone, per-cell passport, boundary-index, or compaction overhead. |
| Every Phase 9 family can be planned against immutable CSR plus sidecars. | medium-high | The families need neighbor cursors, global edge streams, orientation views, property scans, or feature matrices. |
| Some families still require rejection/spill even with perfect CSR. | high | Algorithm state and outputs can dominate topology. |
| Tilehouse is still valuable if freshness/local compaction becomes a product need. | medium-high | Its main advantage is update locality, not lower global algorithm RAM. |

### Expert Lenses

| lens | judgment |
| --- | --- |
| Storage engineer | Immutable CSR is the simplest low-RAM durable projection: easy atomic swap, checksums, compression, and mmap/open behavior. |
| Graph algorithm engineer | CSR is the standard substrate for adjacency-heavy analytics, but algorithm scratch must be budgeted separately. |
| GDS compatibility engineer | CSR does not solve procedure names, configs, modes, result schemas, catalog behavior, or write/mutate semantics. |
| Operator | Multi-hour lag makes rebuild/swap much easier to explain than deltas and compaction. |
| Skeptical engineer | "CSR works for all algorithms" is true only as a topology statement; it is false if interpreted as "all algorithms fit cheaply." |

### Candidate Approaches

| approach | upside | downside | best use |
| --- | --- | --- | --- |
| Flat immutable dual CSR + sidecars | Lowest topology overhead, simplest build/swap, strongest mmap story | Rebuild needed for freshness; no local compaction | Default v003 RAM-first OLAP |
| Cellular CSR Tilehouse | Local updates, dirty-cell compaction, better regional locality | More metadata, boundary indexes, deltas, compaction rules | Later freshness/locality tier |
| Many per-algorithm layouts | Can speed hand-picked demos | Duplicates topology and destroys holistic RAM discipline | Avoid for default RAM-first mode |
| Graph-LSM / living CSR | Fresh analytics, research moat | Highest complexity and hidden compaction RAM | Only if lag becomes unacceptable |

Chosen approach under the hours-lag assumption:

```text
Build immutable CSR snapshots first.
Make the rebuild path low-RAM and atomic.
Add sidecars and spillable runtime state.
Postpone Tilehouse until measured freshness pressure justifies it.
```

### What Immutable CSR Must Include

The topology alone is not enough. The immutable CSR architecture needs these
planes:

| plane | required artifacts | why |
| --- | --- | --- |
| topology | forward CSR, reverse CSR, dense IDs, key index | Covers directed, reverse, undirected, traversal, PageRank, SCC, and edge scans. |
| graph catalog | manifest, generation, source tx range, projection config | Makes multi-hour freshness explicit and testable. |
| property sidecars | node/edge typed columns, null/default metadata, dictionaries | Supports weights, labels, features, partitions, and write/mutate results. |
| orientation views | natural, reverse, undirected, filtered relationship-type views | Avoids duplicating base topology for projection variants. |
| global edge streams | forward and reverse sequential edge cursors | Enables PageRank, Bellman-Ford, WCC scans, and strict-RAM execution. |
| sorted-neighbor option | per-type or global sorted adjacency where required | Needed for triangle/intersection-heavy algorithms without runtime full sort. |
| runtime scratch | bounded vectors, frontiers, heaps, candidate lists, spill tapes | Keeps algorithm state separate from durable topology. |
| result sidecars | immutable or generationed output properties/models | Supports mutate/write-like behavior without rewriting topology. |

### Can We Be Sure?

We can be sure of this narrower statement:

```text
Immutable CSR is a sufficient and likely lowest-RAM persistent topology
foundation for the broader GDS families when freshness lag is acceptable.
```

We cannot be sure of this broader statement:

```text
Immutable CSR makes every GDS algorithm practical on an 8 GB machine.
```

The falsifier is not usually adjacency layout. The falsifier is one of:

| falsifier | examples |
| --- | --- |
| O(V) state too large | PageRank vectors, WCC components, BFS parent arrays, SCC stacks |
| O(V * dimension) state too large | FastRP, Node2Vec, GraphSAGE, KGE, KMeans |
| O(V * topK) or candidate-pair state too large | KNN, nodeSimilarity |
| O(source_count * frontier) work too slow or too much spill | all-pairs shortest paths, betweenness, closeness |
| output is larger than the input | paths, walks, embeddings, dense pairwise similarities |

Therefore the confidence model should be:

| statement | answer |
| --- | --- |
| Is immutable CSR the right default persistent topology? | yes |
| Does every family have a path over immutable CSR? | yes, with sidecars and scratch |
| Does every exact algorithm fit lowest-RAM targets? | no |
| Should Tilehouse be default if lag is acceptable? | no |
| Should Tilehouse remain in the roadmap? | yes, but behind measured freshness/locality pressure |

## GDS As ABI

Treat GDS procedure compatibility like an ABI:

```text
procedure name
mode
config arguments
default values
result columns
estimate behavior
catalog side effects
write/mutate side effects
error class and message shape
```

The first scary failure is not a slow PageRank. The first scary failure is:

```text
The implementation silently narrows the GDS surface while claiming
Neo4j/GDS compatibility.
```

### Support Levels

Every inventoried procedure should be assigned one explicit level:

| level | meaning | user-visible behavior |
| --- | --- | --- |
| `P0 registered-compatible` | Procedure is known, parsed, categorized, and has deterministic unsupported behavior. | Calls do not look unknown; unsupported procedures return a stable compatibility error. |
| `P1 implemented exact low-RAM` | Procedure is implemented with exact semantics and memory estimates under the RAM-first architecture. | Procedure can run when its estimate fits the selected budget. |
| `P2 implemented later` | Procedure is known and planned, but not in the first implementation tranche. | Procedure remains registered as unsupported until promoted. |
| `UnsupportedButRegistered` | Procedure exists in GDS but is intentionally not implemented yet. | Deterministic error with procedure name, family, mode, and reason. |

### Procedure Inventory Artifact

First implementation artifact:

```text
v003-diligence-01/gds-procedure-inventory.tsv
```

Minimum columns:

```text
procedure_name
family
mode
estimate_name
source_file
source_line
config_args
result_type
stability
support_level
notes
```

Inventory tests should assert:

| acceptance test | why |
| --- | --- |
| Every row starts with `gds.`. | Avoid accidental helper/test procedure pollution. |
| `gds.pageRank.stream` and `gds.pageRank.stream.estimate` exist. | Representative algorithm + estimate pair. |
| `gds.graph.project`, list/drop/exists/size rows exist. | Catalog is a hard prerequisite. |
| Families include catalog, centrality, community, pathfinding, similarity, embeddings, ML, miscellaneous, operations, pipelines, and model catalog. | Matches GDS facade reality. |
| Deprecated/alpha/beta procedures are marked explicitly. | Prevents compatibility ambiguity. |
| Unknown procedure calls differ from registered unsupported calls. | Critical for Neo4j-like behavior. |

### Mode Contract

Every algorithm family must classify each mode:

| mode | required behavior |
| --- | --- |
| `stream` | Return rows without graph catalog mutation. |
| `stats` | Return aggregate timings/counts/distributions without per-node result sidecars. |
| `mutate` | Write result into projected graph sidecar/catalog state. |
| `write` | Write result back to the OLTP-facing store or a writeback bridge. |
| `estimate` | Return memory contract without executing the algorithm. |

Cross-cutting tests:

| test | requirement |
| --- | --- |
| Estimate no-work test | `.estimate` must not run algorithm kernels or scan full graph data beyond metadata. |
| Unsupported shape test | Unsupported registered procedure must include procedure name, family, mode, and support level. |
| Schema test | Output columns must match the inventory for implemented modes. |
| Budget gate test | `execute` must reject a plan if `estimate.required_bytes > budget.max_rss_bytes`. |

## Tilehouse As Physical Plan

Cellular CSR Tilehouse should be framed as a physical storage/execution plan,
not as a new public API.

Under the original freshness-sensitive framing, Tilehouse was the preferred
evolution because it made local updates and cell compaction possible. Under the
new assumption that OLAP may lag by hours, Tilehouse should be demoted:

```text
Default: immutable flat CSR + sidecars + atomic generation swap.
Later: Tilehouse if rebuild time, locality, or freshness pressure proves it.
```

This matters for lowest RAM. Tilehouse adds metadata that immutable flat CSR
does not need:

```text
cell passports
global-to-local id maps
boundary indexes
delta receipts
dirty watermarks
cell compaction scratch
multi-cell planning metadata
```

Those are justified when update locality matters. They are harder to justify
for a RAM-first v003 where the product can accept scheduled OLAP refresh.

### Invariants

```text
Flat dual CSR remains the correctness oracle.
Flat immutable CSR remains the default v003 storage target under hours-lag.
Tilehouse must produce the same logical adjacency as flat CSR if added later.
Tiles are independently readable, dirtyable, compactable, and budgetable.
Global algorithms must still be able to stream the whole graph exactly.
Cells improve locality and update granularity, not algorithm exactness.
```

### Proposed Shape

```text
snapshot_generation_42/
  manifest.json
  global_dense_id_map/
  cells/
    cell_000001/
      passport.json
      forward.offsets.bin
      forward.peers.bin
      reverse.offsets.bin
      reverse.peers.bin
      label_sidecars/
      reltype_sidecars/
      node_property_columns/
      edge_property_columns/
      delta_receipts.bin
    cell_000002/
      ...
  boundaries/
    cross_cell_edges.bin
    boundary_nodes.bin
  global_stream/
    logical_forward_order.index
    logical_reverse_order.index
```

### What Tilehouse Improves

| dimension | flat dual CSR | Cellular CSR Tilehouse |
| --- | --- | --- |
| static 1-hop walks | already strong | similar, sometimes better with locality |
| full-graph scans | excellent | must provide logical global stream to avoid regressions |
| small update freshness | full rebuild or broad overlay | dirty affected cells and bounded deltas |
| compaction unit | whole snapshot | cell or cell batch |
| page-cache behavior | OS decides mmap residency | can bound local windows and use explicit stream for strict mode |
| property/label filters | not first-class today | tile-local sidecars |
| implementation complexity | low | medium-high |

### What Tilehouse Does Not Magically Improve

| non-improvement | reason |
| --- | --- |
| Global PageRank vector RAM | Still dominated by per-node score vectors. |
| All-pairs centrality RAM | Still dominated by repeated frontier/distance state. |
| Dense embeddings | Still dominated by `node_count * dimension * bytes_per_value`. |
| Bad partitioning | High boundary ratios can erase locality wins. |
| Unbounded deltas | Multiple overlay layers can become a graph LSM mess. |

## Memory Contract

Every projection and algorithm estimate should produce a structured memory
contract:

```text
required_bytes
heap_bytes
rss_budget_bytes
page_cache_expected_bytes
page_cache_unbounded_risk
direct_io_buffer_bytes
algorithm_state_bytes
frontier_or_vector_bytes
sidecar_bytes
delta_overlay_bytes
compaction_scratch_bytes
snapshot_build_scratch_bytes
spill_bytes
output_sidecar_bytes
```

### Measured Versus Estimated

| value | definition |
| --- | --- |
| estimated | Derived from manifest counts, sidecar metadata, config, and algorithm state formulas. |
| planned | Physical execution plan chosen to fit a budget. |
| measured | Runtime RSS/page-fault/page-cache/procfs/cgroup observations. |

Rules:

| rule | test |
| --- | --- |
| `mmap` plans cannot claim deterministic RAM. | Estimate must flag page-cache residency as OS-mediated. |
| Strict-RAM global plans must name direct or explicit buffers. | Estimate must include buffer sizes and spill sizes. |
| Vectors must be explicit. | PageRank on `200M` nodes must show each vector and bytes per value. |
| Deltas must be capped. | Estimate must fail or force compaction when delta thresholds are exceeded. |
| Compaction must be budgeted. | Dirty-cell compaction must run under `BuildMemoryBudget` or successor budget. |

### 50 GB On 8 GB Decision Rule

For a `50 GB` logical graph on an `8 GB` machine, the planner should answer:

```text
can_run: yes/no
required_budget: bytes
dominant_state: topology/properties/vector/frontier/embedding/output
execution_profile: mmap_interactive | explicit_stream | spillable | reject
freshness_mode: snapshot_only | bounded_delta_merge | force_compaction
```

Example outcomes:

| workload | likely v003 decision |
| --- | --- |
| 1-hop local traversal | run with mmap CSR windows |
| graph catalog list/drop/exists | run from catalog metadata |
| degree centrality | run if output sidecar fits |
| BFS from one source | run with bounded frontier and visited state |
| WCC | run if component vector/union state fits or spills |
| PageRank | run only if vectors plus stream buffers fit, otherwise spill/reject |
| KNN exact all-pairs | reject unless candidate strategy and topK spill plan exists |
| Node2Vec large embeddings | reject or require explicit dimension/batch/spill budget |

## TDD Roadmap

### Phase 0: Preserve Current Behavior

Goal: keep current flat CSR behavior locked before adding a second physical
layout.

Acceptance tests:

| test | requirement |
| --- | --- |
| Snapshot manifest contract | Fixture snapshot has all v2 files, `version == 2`, and `storage_mode == immutable_dual_csr`. |
| Walk contract | Existing `ForwardOne`, `BackwardOne`, `ForwardTwo`, and `BackwardTwo` outputs remain unchanged. |
| Runtime corruption contract | Existing truncated/corrupt snapshot errors remain deterministic. |
| RSS harness smoke test | A small build/query reports peak RSS in a stable JSON shape. |

### Phase 1: GDS Inventory And Registry

Goal: know the surface before building storage for it.

Acceptance tests:

| test | requirement |
| --- | --- |
| Inventory generated/read | Checked inventory exists and is deterministic in CI. |
| No duplicate procedure names | Registry rejects duplicates. |
| PageRank pair present | `gds.pageRank.stream` links to `.estimate`. |
| Catalog present | Graph project/list/drop/exists/size rows are present. |
| Support levels present | Every row has `P0`, `P1`, `P2`, or `UnsupportedButRegistered`. |
| Known unsupported behavior | Registered unsupported procedure returns stable compatibility error. |

### Phase 2: Graph Projection Catalog Skeleton

Goal: represent named GDS graph projections before algorithm execution.

Acceptance tests:

| test | requirement |
| --- | --- |
| Project/list/drop/exists | Named graph lifecycle matches GDS-style expectations. |
| Orientation parsing | `NATURAL`, `REVERSE`, and `UNDIRECTED` are represented logically. |
| Property selectors | Node/relationship property requirements are parsed into sidecar needs. |
| Projection estimate | Estimate includes topology refs, sidecars, catalog metadata, and no duplicate full topology by default. |

### Phase 3: Algorithm-Facing Graph Access

Goal: make algorithms target a logical graph, not file layout.

Proposed trait:

```rust
pub trait GraphAdjacencyRuntime {
    fn node_count(&self) -> u64;
    fn relationship_count(&self) -> u64;
    fn neighbors(
        &self,
        node: DenseNodeId,
        direction: WalkDirection,
    ) -> Result<NeighborCursor<'_>, KnightBusError>;
    fn global_edges(
        &self,
        direction: WalkDirection,
    ) -> Result<EdgeCursor<'_>, KnightBusError>;
}
```

Acceptance tests:

| test | requirement |
| --- | --- |
| Flat adapter parity | `MmapWalkRuntime` adapter returns the same neighbors as `WalkQueryRuntime`. |
| Global edge cursor | Forward and reverse cursors return exact edge sets. |
| Small graph oracle | In-memory normalized graph and flat CSR agree. |
| Property-based graph oracle | Small generated graphs preserve adjacency parity. |

### Phase 4: Immutable CSR v3 Manifest And Sidecar Catalog

Goal: keep the topology immutable and flat, while adding enough manifest and
sidecar metadata to serve GDS-style projections.

Acceptance tests:

| test | requirement |
| --- | --- |
| v3 manifest JSON round-trip | Manifest validates after serialization and still opens v2 snapshots. |
| Generation metadata | Manifest records snapshot generation, source tx range, build time, and graph counts. |
| Sidecar catalog | Manifest lists label, relationship type, property, weight, feature, and result sidecars. |
| Orientation metadata | Natural, reverse, and undirected logical views are represented without duplicating topology by default. |
| Budget report | Build summary reports topology bytes, sidecar bytes, and scratch bytes separately. |

### Phase 5: Immutable CSR Global Streams And Compression Diligence

Goal: prove the immutable CSR snapshot can drive GDS algorithms through cursor
and stream interfaces without building Tilehouse first.

Acceptance tests:

| test | requirement |
| --- | --- |
| File sizes valid | Reader validates offsets, peers, node table, strings, key index, and sidecar lengths. |
| Global stream parity | Forward and reverse global edge streams match the normalized in-memory graph. |
| Logical orientation parity | Natural/reverse/undirected views match oracle graphs without rewriting base files. |
| Sorted-neighbor contract | Algorithms that need intersections either require sorted adjacency or declare runtime sort/spill. |
| Compression diligence | Raw CSR remains baseline; optional compressed peer lists must prove lower total working set before adoption. |

### Phase 6: Sidecar Columns

Goal: support labels, relationship types, weights, and properties without
prebuilding per-algorithm layouts.

Acceptance tests:

| test | requirement |
| --- | --- |
| Label filter | Node labels are queryable without heap materializing all labels. |
| Relationship type filter | Type filters select the correct edge subset across the immutable snapshot. |
| Weight sidecar | Weighted algorithms fail deterministically if required weight is missing. |
| Property null/default behavior | Missing values follow GDS-compatible config behavior. |
| Estimate integration | Sidecar bytes appear in projection and algorithm estimates. |

### Phase 7: First Kernels

Start with kernels that prove different pieces of the architecture:

| kernel | why first | acceptance highlights |
| --- | --- | --- |
| Degree centrality | Low algorithm complexity; validates orientation and output modes. | Hand oracle, stream/schema, estimate includes output state. |
| BFS | Proves frontier scheduling over immutable CSR. | Path oracle, source/target config, stable output order. |
| WCC | Proves global iterative traversal. | Component oracle, in-memory/flat-CSR parity, bounded component state. |
| PageRank | Proves vector memory estimates and global stream plans. | Numeric tolerance oracle, vector-by-vector estimate, strict-RAM plan/reject. |

### Phase 8: WAL Receipts And Cell Deltas

Goal: connect Neo4j-shaped OLTP updates to OLAP freshness without rebuilding the
whole snapshot for tiny changes.

Under the hours-lag assumption, Phase 8 should not block the v003 RAM-first
architecture. Treat it as a later branch:

```text
If users accept scheduled OLAP refresh, use low-RAM full rebuild + atomic
generation swap.

Only build WAL receipts, deltas, and cell compaction if freshness lag becomes
the bottleneck.
```

Acceptance tests:

| test | requirement |
| --- | --- |
| Receipt mapping | Node/relationship/property changes map to affected snapshot regions or cells. |
| Fresh overlay read | Query sees a receipt-applied edge through the delta overlay. |
| Sidecar-only update | Property/label updates dirty sidecars without rewriting topology. |
| Compaction budget | Dirty-cell compaction stays below configured scratch budget. |
| Crash recovery | Durable receipts replay after crash between append and compaction. |
| Delta cap | Query rejects or forces compaction when overlay layers exceed policy. |

### Phase 9: Broader GDS Families

Goal: roll out by dependency order, not excitement order.

Under the hours-lag assumption, Phase 9 should be compared against immutable
CSR, not Tilehouse. The default storage question becomes:

```text
Can this family run over immutable CSR topology plus sidecars and bounded
runtime scratch?
```

The answer is mostly yes, but with different confidence levels:

| tier | family | representative procedures | gate |
| --- | --- | --- | --- |
| 1 | catalog | graph project/list/drop/exists/size | Required before algorithms. |
| 1 | centrality | degree, PageRank | Proves scalar and vector state. |
| 1 | pathfinding | BFS, DFS, Dijkstra | Proves frontiers and weight sidecars. |
| 1 | community | WCC, SCC, triangle count, k-core | Proves global structural algorithms. |
| 2 | community | Louvain, Leiden, label propagation | Requires mutation and contracted-graph scratch. |
| 2 | similarity | nodeSimilarity, KNN | Requires candidate pruning and topK spill. |
| 3 | embeddings | FastRP, Node2Vec, GraphSAGE | Requires vector sidecars and model/output discipline. |
| 3 | ML/pipelines/model catalog | train/predict/model list/drop | Requires persistent model metadata. |
| 3 | operations/misc | progress, scaleProperties, feature flags | Requires telemetry and property-plane maturity. |

### Phase 9 Immutable CSR Fit

| family | immutable CSR fit | additional planes needed | lowest-RAM confidence | can we be sure? |
| --- | --- | --- | --- | --- |
| catalog | excellent | manifest, generation metadata, projection config | high | yes; catalog is metadata over snapshots |
| centrality | excellent for degree/PageRank/HITS; hard for all-pairs variants | vectors, frontier/distance arrays, result sidecars | medium-high | sure for substrate, not for every config fitting |
| pathfinding | excellent for BFS/DFS/Dijkstra/Bellman-Ford edge access | weight sidecars, priority queues, distance/parent arrays | medium-high | sure for exact access; output/path explosion still bounded by config |
| community/structure | strong for WCC/SCC/k-core/triangle; harder for Louvain/Leiden | component/community arrays, sorted neighbors, contracted graph scratch | medium | sure for first-tier families; contraction scratch needs measurement |
| similarity | adequate substrate but not enough by itself | candidate generation, topK heaps, filters, spill | low-medium | no; candidate state can dominate |
| embeddings | adequate for neighbor sampling/propagation | dense embedding matrices, walk corpus, model artifacts | low-medium | no; embedding state dominates topology |
| ML/pipelines/model catalog | CSR helps feature extraction only | feature columns, model storage, training batches | low-medium | no; model/training memory dominates |
| operations/misc | good for metadata/progress/property scans | telemetry, feature flags, property columns | high for simple ops | yes for simple ops; transforms need estimates |

Revised Phase 9 rule:

```text
Do not add a new persistent topology layout for a GDS family unless it reduces
working set under the memory contract.

Prefer immutable CSR + property sidecars + spillable runtime state.
```

## Algorithm Diligence Matrix

### Centrality

| group | access pattern | dominant state | spill strategy | first oracle | 50GB/8GB risk |
| --- | --- | --- | --- | --- | --- |
| Degree | offsets and filtered adjacency | output scalar per node | output sidecar or stream chunks | hand degree fixture | low |
| PageRank / ArticleRank / Eigenvector | global edge stream per iteration | 2-4 vectors per node | chunked vectors or spillable vector tape | tiny convergence fixture | medium-high |
| HITS | forward and reverse scans | hub and authority vectors | spillable vectors | bipartite toy graph | high |
| Harmonic / closeness | repeated BFS/SSSP | frontier and distance arrays | source batching | small unweighted graph | high |
| Betweenness | Brandes sweeps | stack, sigma, delta, predecessor state | source batching and spill | diamond graph | very high |
| Articulation points | DFS low-link | discovery, low, parent arrays | arrays or spill if needed | bridge fixture | medium |
| Influence/CELF | repeated simulations | candidate heap and sampled reachability | sampled reachability spill | tiny influence graph | high |

### Pathfinding

| group | access pattern | dominant state | spill strategy | first oracle | 50GB/8GB risk |
| --- | --- | --- | --- | --- | --- |
| BFS / DFS | immutable CSR frontier | visited, parent, distance | bitset/frontier spill | tree/cycle fixture | low-medium |
| Dijkstra / A* | wavefront plus weight sidecar | priority queue, distance, predecessor | bucket/queue spill | weighted diamond | medium |
| Delta-Stepping | bucketed frontier | distance vector and buckets | bucket spill | weighted multi-path | medium-high |
| Bellman-Ford | repeated global stream | distance vector | vector spill | negative-edge no-cycle | medium |
| All shortest paths | repeated source sweeps | many distance/frontier states | source batching | tiny all-pairs graph | very high |
| Yen's k-shortest | repeated Dijkstra | candidate path heap | path heap spill | 3-known-path graph | high |
| Random walk | CSR neighbor sampling | RNG state and walk buffers | walk corpus chunks | seeded walk fixture | medium |

### Community And Structure

| group | access pattern | dominant state | spill strategy | first oracle | 50GB/8GB risk |
| --- | --- | --- | --- | --- | --- |
| WCC | iterative union/frontier | component id per node | component vector spill | two-components fixture | low-medium |
| SCC | forward/reverse DFS | stacks and component arrays | stack spill if needed | directed SCC fixture | medium |
| Triangle count / LCC | sorted intersections | intersection buffers | high-degree chunking | triangle/square fixture | medium-high |
| k-core | degree peeling | degree array and queue | queue spill | k-core toy graph | medium |
| Coloring | iterative colors | color array and conflict frontier | conflict frontier spill | odd/even cycle | medium |
| Label propagation / SLPA | neighbor label scans | label distributions | label distribution spill | two-cluster fixture | medium-high |
| Louvain / Leiden | contraction levels | community ids and aggregate graph | contracted graph sidecar | modularity toy graph | high |
| Modularity / conductance | cut/internal scans | community sidecars | stream scans | known partition | medium |

### Similarity

| group | access pattern | dominant state | spill strategy | first oracle | 50GB/8GB risk |
| --- | --- | --- | --- | --- | --- |
| Node similarity | adjacency overlap | candidate pairs and topK heaps | candidate blocking and topK spill | bipartite overlap graph | very high |
| Filtered node similarity | filtered overlap | filtered candidates | filter-first blocking | filtered toy graph | high |
| KNN | vector/property scan | per-node topK and candidate sampler | blocked scan, topK spill | small vector set | very high |
| Filtered KNN | filtered vector scan | filtered topK heaps | filter-first blocks | filtered vector set | high |

Rule: no exact similarity implementation may materialize all `O(n^2)` pairs
without an explicit candidate strategy and budget rejection test.

### Embeddings

| group | access pattern | dominant state | spill strategy | first oracle | 50GB/8GB risk |
| --- | --- | --- | --- | --- | --- |
| FastRP | repeated sparse propagation | embedding matrix | chunked embedding sidecar | seeded tiny embedding | very high |
| Node2Vec | random walks plus training | walk corpus and embedding matrix | walk chunking and model batches | seeded walk corpus | very high |
| GraphSAGE | neighbor sampling | batches and model weights | batch scheduler | tiny train/infer fixture | very high |
| HashGNN | hashed features and aggregation | hashes and embeddings | chunked feature sidecars | deterministic hash fixture | high |

Rule: dimension, batch size, concurrency, walk length, and bytes per value must
be part of every embedding estimate.

### ML, Pipelines, Catalog, And Miscellaneous

| group | access pattern | dominant state | spill strategy | first oracle | 50GB/8GB risk |
| --- | --- | --- | --- | --- | --- |
| KGE | relationship type/property sidecars | embeddings and negative samples | batch training | tiny typed KG | very high |
| Split relationships | property/filter mutation | split tags/properties | sidecar chunks | deterministic seed | medium |
| Scale properties | property column scan | stats and output column | chunked column scan | numeric property fixture | low |
| To undirected | logical projection | doubled logical relationships | projection metadata | asymmetric fixture | medium |
| Collapse path / index inverse | topology transform | derived relationship sidecar | derived sidecar chunks | path fixture | medium-high |
| Model catalog | metadata/artifacts | model bytes and feature schema | artifact files | register/list/drop | high |
| Pipelines | metadata plus execution | feature extraction and model state | staged execution | train/list/drop | high |
| Operations/progress | execution telemetry | counters and task state | bounded task registry | progress fixture | low |

## Implementation PR Sequence

### PR 1: GDS Inventory And Registry Only

No storage changes.

Tests first:

| test | requirement |
| --- | --- |
| Inventory parse | Checked inventory reads deterministically. |
| Registry duplicate detection | No duplicate procedure names. |
| PageRank known | Stream and estimate known. |
| Catalog known | Graph catalog core rows known. |
| Unsupported known | Missing kernels return `UnsupportedButRegistered`. |

Value:

```text
Prevents API-surface drift before deep storage work.
```

### PR 2: Projection Catalog Skeleton

No algorithm kernels.

Tests first:

| test | requirement |
| --- | --- |
| project/list/drop/exists | Named graph lifecycle works. |
| orientation parsing | Natural/reverse/undirected represented logically. |
| estimate shape | Holistic memory estimate fields exist. |

### PR 3: `GraphAdjacencyRuntime` Over Flat CSR

No Tilehouse yet.

Tests first:

| test | requirement |
| --- | --- |
| neighbor cursor parity | Matches existing walk runtime. |
| global edge cursor parity | Matches flat CSR edge set. |
| old tests pass | Existing CLI/library contracts unaffected. |

### PR 4: Immutable CSR v3 Manifest And Sidecar Catalog

Tests first:

| test | requirement |
| --- | --- |
| manifest round-trip | v3 manifest validates and v2 snapshots still open. |
| sidecar catalog | Labels, types, weights, properties, features, and result sidecars are declared. |
| generation metadata | Source tx range and snapshot generation are visible. |

### PR 5: Immutable CSR Global Streams And Compression Diligence

Tests first:

| test | requirement |
| --- | --- |
| edge stream parity | Global forward/reverse streams match normalized graph. |
| orientation parity | Natural/reverse/undirected logical views match oracles. |
| compression gate | Any compressed peer format must prove lower working set than raw CSR. |

### PR 6: Sidecar Columns

Tests first:

| test | requirement |
| --- | --- |
| label/type filters | Correct node/edge subset selected. |
| property columns | Missing/default values handled. |
| estimate integration | Sidecar bytes counted. |

### PR 7: Degree + BFS + WCC

Tests first:

| test | requirement |
| --- | --- |
| degree oracle | Directed/reverse/undirected correct. |
| BFS oracle | Frontier traversal over immutable CSR is stable. |
| WCC oracle | In-memory/flat-CSR parity stable. |

### PR 8: PageRank Deterministic RAM Proof

Tests first:

| test | requirement |
| --- | --- |
| numeric oracle | Tiny graph converges within tolerance. |
| vector estimate | `200M` node estimate names all vectors. |
| strict mode | O_DIRECT/explicit stream candidate does not claim mmap determinism. |
| budget reject | Unsafe plan rejected before execution. |

### PR 9: Optional WAL Receipts And Cell Deltas

Tests first:

| test | requirement |
| --- | --- |
| receipt mapping | Affected snapshot regions or cells are correct. |
| overlay freshness | New edge visible through overlay. |
| compaction | Overlay removed and cell CSR updated under budget. |
| recovery | Receipt replay is deterministic. |

### PR 10+: Remaining Families

Roll out by the algorithm diligence matrix. Do not mark a procedure supported
until all rollout gates pass.

## Rollout Gates

No procedure can move to `P1 implemented exact low-RAM` until:

```text
G1 registry row exists
G2 config parser validates defaults and bad inputs
G3 estimate accounts for topology, sidecars, state, scratch, optional deltas, and I/O policy
G4 tiny oracle correctness test passes
G5 immutable CSR and in-memory oracle parity test passes where topology is involved
G6 supported modes have schema tests
G7 budget rejection test passes
G8 deterministic ordering and seed behavior is documented
G9 unsupported mode behavior is deterministic
G10 result sidecar or writeback semantics are tested if mutate/write is supported
```

## Acceptance Checklist

This diligence doc is implementation-ready when it satisfies:

| checklist item | status |
| --- | --- |
| Explains why the pasted draft is strong but incomplete. | done |
| Grounds Knight Bus claims in current local source line references. | done |
| Grounds GDS claims in local `gitrefrepo/neo4j-gds-src` line references. | done |
| Uses `gitrefrepo/` paths, not old reference-shelf paths. | done |
| Separates fact, inference, and speculation. | done |
| Corrects the `compio`/`O_DIRECT` thesis. | done |
| Recommends immutable CSR as default topology and Tilehouse as optional later work under the hours-lag assumption. | done |
| Defines GDS support levels. | done |
| Defines memory estimate fields and strict-RAM rules. | done |
| Provides a TDD PR sequence. | done |
| Provides an algorithm-family diligence matrix. | done |
| Defines the first concrete implementation task. | done |

## First Concrete Task

Start with:

```text
PR 1: GDS Inventory And Registry Only
```

Reason:

| reason | explanation |
| --- | --- |
| Cheapest uncertainty reducer | It requires no Tilehouse or algorithm implementation. |
| Protects compatibility | It makes the public surface visible before storage work dominates. |
| Enables scoped honesty | It lets v003 be explicit about `P0`, `P1`, `P2`, and `UnsupportedButRegistered`. |
| Feeds all later work | Every later algorithm/storage PR can test itself against the inventory. |

The first code-bearing PR should not start by writing a tile partitioner. It
should start by proving that the team knows what it means to look like GDS.
