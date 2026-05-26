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
The local reference repo under `ref-repo-folder/neo4j` shows why.

| Neo4j code fact | local evidence | OLAP implication |
| --- | --- | --- |
| Node records are fixed records with relationship and property pointers. | `ref-repo-folder/neo4j/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/NodeRecordFormat.java:31-32` defines a `15` byte node record with next relationship, next property, labels, and dense bit. | This is compact for mutable records, but analytics still has to turn node neighborhoods into cursor walks. |
| Relationship records are pointer-rich. | `ref-repo-folder/neo4j/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/RelationshipRecordFormat.java:31-35` defines a `34` byte record with endpoints, type, next/prev chain pointers, next property, and markers. | A raw unweighted CSR peer entry is commonly `4` bytes. Neo4j is paying for updateability and bidirectional chain maintenance. |
| Property records are separate fixed records with block decoding. | `ref-repo-folder/neo4j/community/record-storage-engine/src/main/java/org/neo4j/kernel/impl/store/format/standard/PropertyRecordFormat.java:37-39` defines a `41` byte property record. | Analytics should avoid decoding generic property records when it only needs `weight.f32`, `community.u32`, or `feature.f32[]`. |
| Sparse relationship traversal follows chains; dense nodes go through relationship groups. | `ref-repo-folder/neo4j/community/record-storage-engine/src/main/java/org/neo4j/internal/recordstorage/RecordRelationshipTraversalCursor.java:95-167` chooses chain vs group traversal, loads relationship records, and filters by selection. `ref-repo-folder/neo4j/community/record-storage-engine/src/main/java/org/neo4j/internal/recordstorage/RecordRelationshipTraversalCursor.java:171-245` shows the dense-node group state machine. | Correct and flexible, but not the same hot loop as `offsets[v]..offsets[v+1]` over dense peer arrays. |
| Dense-node grouping threshold defaults to `50`. | `ref-repo-folder/neo4j/community/configuration/src/main/java/org/neo4j/configuration/GraphDatabaseSettings.java:684-688`. | Neo4j has already specialized high-degree nodes for OLTP traversal, but still within a mutable record-store model. |

Neo4j memory configuration also makes the business-memory problem explicit:
the official settings guidance recommends reserving OS memory, giving JVM heap
enough space for transaction/query state, and allocating the rest to page cache
([Neo4j configuration settings](https://neo4j.com/docs/operations-manual/current/configuration/configuration-settings/)).
For an `8 GB` box, that leaves little room for both a transactional database,
a large GDS projection, and heavy algorithm scratch.

## GDS Code Reality

The missing reference repo is now cloned at
`ref-repo-folder/graph-data-science`. That matters because the fair OLAP
baseline is not just "Neo4j docs say GDS is projected"; the GDS source itself
shows a separate projected graph plane with CSR-like stores, compressed
adjacency implementations, a graph catalog, and explicit memory estimation.

| GDS code fact | local evidence | implication for Knight Bus |
| --- | --- | --- |
| GDS builds a `CSRGraphStore` from projected nodes and imported relationships. | `ref-repo-folder/graph-data-science/core/src/main/java/org/neo4j/gds/api/CSRGraphStoreFactory.java:67-86` constructs a `GraphStoreBuilder` with `nodes`, `relationshipImportResult`, schema, capabilities, and read concurrency. | Neo4j already separates OLAP projection from OLTP record traversal. Our Rust OLAP layer should compete with GDS projection, not only Cypher. |
| The projected graph store holds an ID map, relationship stores, node properties, graph properties, schema, and timestamps. | `ref-repo-folder/graph-data-science/core/src/main/java/org/neo4j/gds/core/loading/CSRGraphStore.java:79-150`. | A faithful replacement needs a projection catalog/data model, not just raw CSR files. |
| GDS stores projected graphs in a user/database/name catalog. | `ref-repo-folder/graph-data-science/core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java:50-55` defines static catalog state; `GraphStoreCatalog.java:84-124` resolves graphs by user/database/name and handles ambiguity. | Knight Bus should have explicit snapshot/catalog identity and freshness metadata, not anonymous mmap files. |
| GDS projection import publishes graph stores to the catalog after building nodes, relationships, and schema. | `ref-repo-folder/graph-data-science/triplet-graph-builder/src/main/java/org/neo4j/gds/projection/GraphImporter.java:173-198`. | The Rust architecture needs an atomic projection publish step that resembles a catalog update. |
| GDS has compressed adjacency list implementations using pages, per-node degree arrays, and offsets. | `CompressedAdjacencyList.java:44-86` estimates compressed pages, degrees, and offsets; `CompressedAdjacencyList.java:101-111` stores `byte[][] pages`, `HugeIntArray degrees`, and `HugeLongArray offsets`. | The `CompressedDualCsrBaseV1` recommendation is not exotic; it is close to the shape GDS already optimizes around, except ours is durable/mmap-first. |
| GDS also has packed adjacency implementations with pages, degrees, and offsets. | `ref-repo-folder/graph-data-science/core/src/main/java/org/neo4j/gds/core/compression/packed/PackedAdjacencyList.java:33-40`. | Compression/packing should be a base-format option or hot artifact, not thirteen separate persistent layouts. |
| GDS exposes memory estimation that combines graph projection and algorithm memory. | `ref-repo-folder/graph-data-science/executor/src/main/java/org/neo4j/gds/executor/MemoryEstimationExecutor.java:80-153`. | Our benchmark plan must measure graph projection memory and algorithm scratch separately, matching the way GDS users reason about feasibility. |
| GDS adjacency access is cursor-based over degree and target IDs, with relationship properties still a design concern. | `ref-repo-folder/graph-data-science/core-api/src/main/java/org/neo4j/gds/api/AdjacencyList.java:25-31` and `AdjacencyList.java:40-62`. | This supports the three-format split: topology cursor, property plane, bounded scratch. |

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
| Neo4j record traversal follows relationship chains or dense-node relationship groups. | sourced local fact | `ref-repo-folder/neo4j/community/record-storage-engine/src/main/java/org/neo4j/internal/recordstorage/RecordRelationshipTraversalCursor.java:95-167`, `ref-repo-folder/neo4j/community/record-storage-engine/src/main/java/org/neo4j/internal/recordstorage/RecordRelationshipTraversalCursor.java:171-245` |
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
