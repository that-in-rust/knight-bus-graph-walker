# Knight Bus Algorithm Storage Atlas

Companion matrix: [KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.csv](./KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.csv)

This atlas answers one question:

> if a Knight Bus successor wanted to beat generic property-graph execution on a narrow graph workload then what on-disk shape should each Neo4j GDS algorithm want?

The target system assumed here is:

- immutable snapshot build-heavy runtime
- dense integer node IDs
- `mmap` open path
- algorithm-needed properties only
- result sidecars instead of mutating the base snapshot

## Premise Check

- Neo4j GDS groups many very different workloads under "graph algorithms". Some are adjacency walks. Some are fixed-point vector updates. Some are residual network solvers. Some are just node-feature clustering.
- A single universal property-graph storage engine is optimized for flexibility and transactional completeness. It is not optimized for making every algorithm's inner loop boring.
- "Per-algorithm bespoke" is the right publication model for this atlas, but not the right implementation model for the engine. The engine should reuse a small set of byte-level layout families.
- Not every algorithm is equally storage-bound. Some workloads are dominated by math, simulation, or training rather than by graph fetch overhead. The skeptical lens should downgrade those.
- Filtered variants are still listed as separate atlas rows because Neo4j surfaces them separately, but the storage answer is usually "same base layout plus filter bitmaps".
- The current GDS surface normalizes to 60 unique algorithms in this atlas. `Longest Path` appears under DAG algorithms only, even though the pathfinding page also references it.

## Expert Lenses

- `Storage-systems lens`: optimize the byte layout for the dominant read and write pattern, not for query-language convenience.
- `Graph-algorithms lens`: classify each algorithm by its true primitive: slice replay, power iteration, wedge intersection, relaxation, residual flow, feature-metric search, or training.
- `Benchmark-fairness lens`: separate "beats Cypher over a property graph" from "beats GDS over a projected in-memory graph".
- `Operator lens`: prefer sealed artifacts, tiny sidecar indexes, explicit validation, and restart safety.
- `Skeptical engineer lens`: ask whether a custom format really wins or whether the workload is mostly compute-bound after data is loaded.

## Candidate Approaches

| approach | upside | downside | verdict |
| --- | --- | --- | --- |
| One universal snapshot | simple implementation story | leaves a lot of hot loops misaligned | reject |
| Fully bespoke base format per algorithm | maximum local fit | too many nearly-duplicate engines | reject |
| Hybrid engine with bespoke atlas contracts | honest about workload differences while still reusing runtime families | requires a vocabulary and normalization rules | choose |

## Chosen Thesis

The right design is:

1. publish a bespoke storage contract for every algorithm
2. implement those contracts on top of a small set of named byte-level layout families
3. let `FormatSelectionProfile` choose the family plus sidecars for each algorithm
4. keep the base graph immutable and sealed
5. keep exact key lookup separate from traversal or compute
6. store only the node, edge, weight, label, feature, and partition data the algorithm actually needs
7. write all scores, paths, clusters, flows, and embeddings as result sidecars

The core interface vocabulary for a Knight Bus successor should be:

- `BaseGraphSnapshot`: the sealed topology artifact
- `PropertyPlane`: typed numeric or categorical planes needed by one or more algorithms
- `AlgorithmArtifact`: the full algorithm-specific view chosen at open time
- `ComputeScratch`: temporary arrays, heaps, queues, buckets, bitsets, or tensors
- `ResultSidecar`: persisted output files beside the base snapshot
- `FormatSelectionProfile`: algorithm to layout-family mapping plus required planes and sidecars

## Evidence and Verification

### Sourced Facts

- Neo4j GDS currently groups the surface into centrality, community detection, similarity, path finding, DAG algorithms, node embeddings, and topological link prediction in the current manual: [GDS manual v2026.04](https://neo4j.com/docs/graph-data-science/current/).
- PageRank is an iterative score update over incoming relationships with damping and convergence tracking: [PageRank docs](https://neo4j.com/docs/graph-data-science/current/algorithms/page-rank/).
- ArticleRank is a PageRank variant that rescales contribution by average out-degree: [Article Rank docs](https://neo4j.com/docs/graph-data-science/current/algorithms/article-rank/).
- HITS requires inverse relationship access and returns both hub and authority values: [HITS docs](https://neo4j.com/docs/graph-data-science/current/algorithms/hits/).
- Maximum flow is a capacity-constrained residual-flow problem and minimum-cost maximum flow adds a cost plane: [Maximum flow docs](https://neo4j.com/docs/graph-data-science/current/algorithms/max-flow/), [Minimum cost maximum flow docs](https://neo4j.com/docs/graph-data-science/current/algorithms/min-cost-max-flow/).
- KNN compares node property arrays and may ignore the existing graph except for optional sampling seeding: [KNN docs](https://neo4j.com/docs/graph-data-science/current/algorithms/knn/).
- K-Means ignores relationships and clusters only on node property arrays: [K-Means docs](https://neo4j.com/docs/graph-data-science/current/algorithms/kmeans/).
- HDBSCAN also operates on property arrays and requires same-length numeric feature arrays: [HDBSCAN docs](https://neo4j.com/docs/graph-data-science/current/algorithms/hdbscan/).
- Node Similarity compares neighborhoods, not arbitrary properties, and GDS explicitly calls out its high pairwise time and space cost: [Node Similarity docs](https://neo4j.com/docs/graph-data-science/current/algorithms/node-similarity/).
- GraphSAGE is an inductive embedding algorithm that samples and aggregates node features from local neighborhoods: [GraphSAGE docs](https://neo4j.com/docs/graph-data-science/current/machine-learning/node-embeddings/graph-sage/).
- Conductance and Modularity are evaluation metrics over an existing community assignment rather than standalone community builders: [Conductance docs](https://neo4j.com/docs/graph-data-science/current/algorithms/conductance/), [Modularity metric docs](https://neo4j.com/docs/graph-data-science/current/algorithms/modularity/).
- CELF is influence maximization under an Independent Cascade model and estimates spread through Monte Carlo simulations: [CELF docs](https://neo4j.com/docs/graph-data-science/current/algorithms/celf/).

### Verification Decisions

- `Inventory count`: 60 unique algorithms after de-duplicating `Longest Path` into the DAG category.
- `Filtered variants`: kept as distinct rows in the CSV but marked as storage-thin wrappers when the base layout is unchanged.
- `Property-only workloads`: K-Means and HDBSCAN should not be forced through topology-shaped base snapshots.
- `Community-input workloads`: Conductance, Modularity metric, and Same Community should consume a community sidecar rather than recomputing partitions.
- `Compute-dominated workloads`: GraphSAGE, Node2Vec, HashGNN, CELF, and APSP deserve lower ROI scores even if a custom format is possible.

### Atlas Primitive Counts

| layout family | algorithms |
| --- | ---: |
| `AnchorDualCsrLayoutV1` | 4 |
| `InboundPowerLayoutV1` | 4 |
| `ConnectivityLowlinkLayoutV1` | 4 |
| `OrderedWedgeLayoutV1` | 9 |
| `PartitionRefinementLayoutV1` | 9 |
| `PeelBucketLayoutV1` | 3 |
| `RelaxationFrontierLayoutV1` | 10 |
| `EdgeOrderForestLayoutV1` | 2 |
| `FlowResidualLayoutV1` | 4 |
| `FeatureMetricLayoutV1` | 4 |
| `EmbeddingSampleLayoutV1` | 4 |
| `DagOrderLayoutV1` | 2 |
| `InfluenceMonteCarloLayoutV1` | 1 |

## Final Synthesis

The cleanest way to make storage "vibe with the algorithm" is to stop treating every algorithm as "graph query plus math" and instead treat it as "one dominant inner loop plus the smallest byte shape that feeds that loop well".

That leads to three practical rules:

- traversal-style algorithms want contiguous adjacency slices
- score-propagation algorithms want dense numeric planes over inbound edges
- feature, flow, and training workloads want their own storage primitives and should not be shoved through the same topology-only snapshot

## Open Questions

- Should a future engine allow one base snapshot to expose both CSR-style planes and tensor-ready feature planes without duplicated bytes?
- Should vector-heavy families support `f16` or quantized planes for cache density once correctness baselines are proven?
- Should filtered variants stay execution policies forever or eventually earn dedicated filter-first precomputed postings?
- Should flow algorithms be allowed a writable memory overlay for residual updates while keeping the durable snapshot immutable?
- Should APSP be implemented at all in a laptop-first engine or only as a blocked offline batch artifact builder?

## Per-Algorithm Atlas

The full 60-row machine-readable atlas lives in [KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.csv](./KNIGHT_BUS_ALGORITHM_STORAGE_ATLAS.csv). The sections below define the byte-level layout families that those per-algorithm profiles reference.

Each CSV row is the concrete per-algorithm contract. The markdown below is the normalized family spec those rows collapse onto. In other words:

- the CSV answers "what exact snapshot does `PageRank` or `Louvain` want?"
- the markdown answers "which byte-level family does that concrete snapshot belong to and why?"

Every CSV row carries the same contract fields:

- `Algorithm`
- `Neo4j category + tier`
- `Dominant hot loop`
- `Primary data primitive`
- `Base snapshot layout`
- `Required property planes`
- `Required auxiliary indexes`
- `Temporary compute state`
- `Result sidecar layout`
- `Why this format matches the algorithm`
- `Why generic property-graph storage is misaligned`
- `Expected payoff`
- `Implementation complexity`
- `Benchmark priority`

The concrete `format_name` values in the CSV are intentionally verbose. They publish bespoke contracts such as `PageRankInboundPowerSnapshotV1` and `TriangleCountOrderedWedgeSnapshotV1`, while the runtime can still implement those on top of the smaller reusable family set below.

### `AnchorDualCsrLayoutV1`

Use when the dominant operation is "exact anchor to one or two adjacency slices".

Files:

```text
manifest.json
node_table.bin
key_index.bin
fwd.offsets.u64.bin
fwd.peers.u32.bin
rev.offsets.u64.bin
rev.peers.u32.bin
degree.out.u32.bin            # optional
degree.in.u32.bin             # optional
walk.alias_prob.f32.bin       # optional
walk.alias_jump.u32.bin       # optional
```

Hot path:

```text
key -> dense_id
direction -> offsets[dense_id] .. offsets[dense_id + 1]
peer slice -> optional next frontier
```

Used by:
- Degree Centrality
- Breadth First Search
- Depth First Search
- Random Walk

Why it fits:
- the algorithm mostly wants contiguous neighbor slices
- reverse traversal is precomputed instead of derived
- random walks can bolt on alias tables without changing the base topology layout

### `InboundPowerLayoutV1`

Use when the dominant operation is repeated inbound score accumulation.

Files:

```text
manifest.json
node_table.bin
key_index.bin
in.offsets.u64.bin
in.peers.u32.bin
in.weight.f32.bin             # optional
out.mass.f32.bin
dangling.bitset.bin
partition.node_ranges.u32.bin # optional
```

Hot path:

```text
for node in partition:
  score_next[node] = base + sum(score_curr[src] * edge_weight / out_mass[src])
swap(score_curr score_next)
```

Used by:
- Article Rank
- Eigenvector Centrality
- PageRank
- HITS

Why it fits:
- the storage exposes the exact gather pattern
- out-mass is precomputed once instead of re-derived per iteration
- the engine can stream float planes without row materialization

### `ConnectivityLowlinkLayoutV1`

Use when the algorithm wants DFS numbering lowlinks reverse passes or undirected twin-edge identity.

Files:

```text
manifest.json
key_index.bin
fwd.offsets.u64.bin
fwd.peers.u32.bin
rev.offsets.u64.bin
rev.peers.u32.bin
undir.offsets.u64.bin
undir.peers.u32.bin
undir.edge_id.u32.bin
undir.twin_halfedge.u32.bin
```

Hot path:

```text
stack based DFS over dense IDs
or
finish order on fwd graph then replay on rev graph
```

Used by:
- Articulation Points
- Bridges
- Strongly Connected Components
- Weakly Connected Components

Why it fits:
- lowlink algorithms need stable half-edge identity and parent tracking
- SCC needs both forward and reverse passes without rebuilding inverse adjacency

### `OrderedWedgeLayoutV1`

Use when the dominant operation is sorted-neighbor intersection wedge counting or common-neighbor scoring.

Files:

```text
manifest.json
left.offsets.u64.bin
left.neighbors.u32.bin
degree.u32.bin
degeneracy.order.u32.bin
neighbor.weight.f32.bin       # optional
```

Hot path:

```text
pick lower degree endpoint first
intersect sorted neighbor lists
accumulate triangle clique or score contribution
```

Used by:
- Clique Counting
- Local Clustering Coefficient
- Triangle Count
- Node Similarity
- Filtered Node Similarity
- Adamic Adar
- Common Neighbors
- Resource Allocation
- Total Neighbors

Why it fits:
- these workloads are really about set intersections and wedge enumeration
- sorted arrays plus degree ordering beat generic relationship iteration

### `PartitionRefinementLayoutV1`

Use when the algorithm repeatedly updates or evaluates community assignments.

Files:

```text
manifest.json
undirected.offsets.u64.bin
undirected.peers.u32.bin
edge.weight.f32.bin           # optional
node.volume.f32.bin
community.seed.u32.bin        # optional
community.input.u32.bin       # optional
```

Hot path:

```text
node -> scan community labels on adjacent nodes
update local gain or majority vote
optionally coarsen graph and repeat
```

Used by:
- Conductance metric
- Label Propagation
- Leiden
- Louvain
- Modularity metric
- Modularity Optimization
- Approximate Maximum k-cut
- Speaker-Listener Label Propagation
- Same Community

Why it fits:
- the hot data is community ID and edge cut or volume counters
- community metrics should consume a partition sidecar not raw property records

### `PeelBucketLayoutV1`

Use when the algorithm repeatedly peels low-degree nodes or greedily assigns colors or degree-derived scores.

Files:

```text
manifest.json
undirected.offsets.u64.bin
undirected.peers.u32.bin
degree.u32.bin
bucket.head.u32.bin
bucket.next.u32.bin
neighbor_color.bitset.bin     # optional
```

Hot path:

```text
pop node from current bucket
decrement neighbors or assign minimal free color
update bucket membership
```

Used by:
- K-Core Decomposition
- K-1 Coloring
- Preferential Attachment

Why it fits:
- the data dependency is degree bucket state not generic graph navigation
- degree products should be pre-materialized from compact planes

### `RelaxationFrontierLayoutV1`

Use when the algorithm repeatedly relaxes weighted edges from a frontier or many sources.

Files:

```text
manifest.json
out.offsets.u64.bin
out.peers.u32.bin
edge.weight.f32.bin
edge.src.u32.bin              # optional edge-scan plane
edge.dst.u32.bin              # optional edge-scan plane
heuristic.f32.bin             # optional
rev.offsets.u64.bin           # optional
rev.peers.u32.bin             # optional
```

Hot path:

```text
frontier pop
relax outgoing edges
update distance predecessor and queue or bucket
```

Used by:
- Betweenness Centrality
- Closeness Centrality
- Harmonic Centrality
- Delta-Stepping SSSP
- Dijkstra Source-Target
- Dijkstra Single-Source
- A*
- Yen's Shortest Path
- Bellman-Ford SSSP
- All Pairs Shortest Path

Why it fits:
- shortest-path work wants flat weight planes and queue-friendly node IDs
- Bellman-Ford additionally benefits from an edge-scan plane rather than a neighbor-object cursor

### `EdgeOrderForestLayoutV1`

Use when the algorithm mostly wants globally ordered weighted edges plus a union-find style component view.

Files:

```text
manifest.json
edge.src.u32.bin
edge.dst.u32.bin
edge.weight.f32.bin
edge.order.u32.bin
```

Hot path:

```text
scan edges in weight order
union components if endpoints differ
emit accepted forest edges
```

Used by:
- Minimum Weight Spanning Tree
- Minimum Weight k-Spanning Tree

Why it fits:
- the winning representation is a sorted edge plane not adjacency lookup

### `FlowResidualLayoutV1`

Use when the algorithm needs mutable residual capacity state and reverse-arc jumps.

Files:

```text
manifest.json
residual.offsets.u64.bin
residual.head.u32.bin
residual.cap.f32.bin
residual.rev_arc.u32.bin
residual.cost.f32.bin         # optional
terminal.role.u8.bin
terminal.supply.f32.bin       # optional
terminal.demand.f32.bin       # optional
```

Hot path:

```text
active node -> residual arc scan
push or relabel
jump directly to reverse arc on update
```

Used by:
- Minimum Directed Steiner Tree
- Prize-collecting Steiner Tree
- Maximum Flow
- Minimum Cost Maximum Flow

Why it fits:
- flow workloads want mutable arc state and reverse arc identity
- generic relationship records are the wrong shape for repeated residual updates

### `FeatureMetricLayoutV1`

Use when the graph topology is secondary or ignored and the dominant operation is vector distance or nearest-neighbor refinement.

Files:

```text
manifest.json
node_table.bin
key_index.bin
features.row_offsets.u64.bin
features.values.f32.bin
features.rowmajor.f32.bin     # optional fixed width fast path
feature.norm.f32.bin
candidate.offsets.u64.bin     # optional ANN sidecar
candidate.peers.u32.bin       # optional ANN sidecar
```

Hot path:

```text
load feature row
compute metric against centroid candidate or ANN candidate set
update labels centroids or neighbor heap
```

Used by:
- K-Nearest Neighbors
- Filtered K-Nearest Neighbors
- K-Means Clustering
- HDBSCAN

Why it fits:
- these workloads want row-major numeric feature planes
- forcing them through topology-first storage creates pointless graph overhead

### `EmbeddingSampleLayoutV1`

Use when the algorithm samples neighborhoods or random walks to emit embeddings or model weights.

Files:

```text
manifest.json
node_table.bin
key_index.bin
fwd.offsets.u64.bin
fwd.peers.u32.bin
rev.offsets.u64.bin           # optional
rev.peers.u32.bin             # optional
feature.rowmajor.f32.bin      # optional
alias.jump.u32.bin            # optional
alias.prob.f32.bin            # optional
sample.seed.u64.bin
neg.alias.jump.u32.bin        # optional
neg.alias.prob.f32.bin        # optional
```

Hot path:

```text
sample neighborhood or walk path
gather feature rows or transition weights
emit embedding vector or training batch
```

Used by:
- FastRP
- GraphSAGE
- Node2Vec
- HashGNN

Why it fits:
- embedding systems want tensor-ready features plus cheap neighborhood sampling
- one runtime can reuse topology pages while swapping training sidecars

### `DagOrderLayoutV1`

Use when the graph is acyclic and the winning primitive is in-degree peeling then topological replay.

Files:

```text
manifest.json
dag.offsets.u64.bin
dag.peers.u32.bin
dag.weight.f32.bin            # optional
in_degree.u32.bin
topo.order.u32.bin            # optional persisted sidecar
```

Hot path:

```text
zero in-degree queue
emit topological order
replay edges in topo order for longest path dynamic programming
```

Used by:
- Topological Sort
- Longest Path

Why it fits:
- DAG workloads gain from precomputed in-degree and optional cached topo order

### `InfluenceMonteCarloLayoutV1`

Use when the dominant work is repeated stochastic propagation under an activation model.

Files:

```text
manifest.json
fwd.offsets.u64.bin
fwd.peers.u32.bin
activation.prob.f32.bin
rrset.offsets.u64.bin         # optional
rrset.nodes.u32.bin           # optional
seed_gain.cache.f32.bin       # optional
```

Hot path:

```text
seed candidate -> cascade simulation or RR set coverage update
reuse marginal gain cache
```

Used by:
- CELF

Why it fits:
- Monte Carlo influence is dominated by repeated stochastic frontier expansion and gain caching
- the real storage win is in reusable simulation sidecars not in a new generic graph base

## ROI Ranking

### P0: Build First

1. `AnchorDualCsrLayoutV1`
   - covers Degree Centrality, BFS, DFS, and Random Walk
   - lowest complexity and closest to the existing Knight Bus proof
   - strongest apples-to-apples story against generic property-graph traversal

2. `InboundPowerLayoutV1`
   - unlocks PageRank first and then Article Rank, Eigenvector, and HITS
   - very strong speedup potential because the hot loop becomes pure numeric gather over inbound slices

3. `ConnectivityLowlinkLayoutV1`
   - SCC and WCC are high-value baseline analytics
   - Articulation Points and Bridges then come almost for free once the family exists

4. `RelaxationFrontierLayoutV1`
   - start with Dijkstra Source-Target and Dijkstra Single-Source
   - then add A* and Delta-Stepping
   - this is the next clean proof after fixed-hop traversal

5. `OrderedWedgeLayoutV1`
   - start with Triangle Count
   - then Node Similarity and Common Neighbors family
   - very compelling for "generic graph store vs sorted-array intersection" demonstrations

### P1: Build After the Core

- `PartitionRefinementLayoutV1`
  - prioritize Louvain and Label Propagation first
  - Leiden follows once coarse-graph rebuild and refinement are stable
- `EdgeOrderForestLayoutV1`
  - MWST is a compact proof and k-MWST is incremental
- `PeelBucketLayoutV1`
  - K-Core is the best first proof in this family
  - K-1 Coloring is worthwhile after bucket discipline is stable

### P2: Specialized but Worth It

- `FlowResidualLayoutV1`
  - start with Maximum Flow
  - then Min-Cost Max-Flow
  - Steiner variants should be later because terminal semantics and output structure are more complex
- `DagOrderLayoutV1`
  - Topological Sort is easy and useful
  - Longest Path is a clean add-on
- `EmbeddingSampleLayoutV1`
  - FastRP is the best first proof because it is the least training-heavy of the embedding family

### P3: Explicit Skeptical Downgrades

- `FeatureMetricLayoutV1`
  - KNN is still interesting because ANN sidecars matter
  - K-Means and HDBSCAN are not strong "graph storage beats Neo4j" demonstrations because relationships are secondary or ignored
- `EmbeddingSampleLayoutV1` training-heavy members
  - GraphSAGE, Node2Vec, and HashGNN are more compute and training pipeline stories than storage-locality stories
- `InfluenceMonteCarloLayoutV1`
  - CELF is real and useful but the ROI is lower because Monte Carlo simulation dominates after the graph is loaded
- `RelaxationFrontierLayoutV1` at the far end
  - APSP should not be an early build target for a laptop-first runtime

### What To Prototype First

If only five proof formats are built, they should be:

1. `DegreeCentralityAnchorDualCsrSnapshotV1`
2. `BfsTraversalFrontierSnapshotV1`
3. `PageRankInboundPowerSnapshotV1`
4. `DijkstraSingleSourceHeapRelaxationSnapshotV1`
5. `TriangleCountOrderedWedgeSnapshotV1`

That set gives:

- one-hop and frontier replay
- iterative rank propagation
- weighted shortest path
- sorted-neighbor intersection

Together those prove that "storage should vibe with the algorithm" is a real systems thesis, not just a slogan.
