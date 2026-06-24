# Batch 07: Low-RAM Graph Priors, Representative GDS Kernels, And Storage Fit

Date: 2026-06-24

Assigned lanes:

- `Capability lane`
- `Architecture lane`
- `Rejection lane`

Assigned PRD outcomes:

- `Low-RAM graph priors`
- `Out-of-core graph execution`
- `CSR / sparse adjacency storage`
- `Memory-budgeted graph projection`
- `Representative GDS family feasibility`

Requirement IDs touched in this batch:

- `REQ-LEARN-008.0`
- `REQ-LEARN-009.0`
- `REQ-LEARN-015.0`
- `REQ-LEARN-024.0`
- `REQ-LEARN-025.0`
- `REQ-LEARN-026.0`
- `REQ-LEARN-044.0`
- `REQ-LEARN-045.0`
- `REQ-LEARN-046.0`
- `REQ-LEARN-047.0`
- `REQ-LEARN-048.0`

## Answer First

This batch started as a repo-ranking exercise and is now something more useful:
it connects low-RAM graph precedents to representative Neo4j GDS kernel
families.

The strongest current conclusions are:

1. `GraphChi` and `MiniGraph` are still the clearest single-node low-RAM
   precedents, but the reason is now source-backed runtime control, not README
   posture.
2. `Kuzu` is the cleanest local proof that a disk-first graph system can keep
   CSR as a durable storage shape rather than only an in-memory projection.
3. `GAPBS`, `Ligra`, and `sprs` are valuable, but mostly as canonical CSR /
   compressed-shape oracles and fixture scaffolding, not as full low-RAM server
   architectures.
4. `Neo4j GDS` representative families do not point toward thirteen persistent
   per-algorithm layouts. They point toward:
   - compact canonical topology,
   - typed sidecars,
   - explicit scratch/state accounting,
   - and selective spill or out-of-core execution for the hard families.
5. Cells are still not forced by the traced algorithm semantics alone.
   Representative family tracing strengthens the case for:
   - flat dual CSR plus sidecars as the first snapshot primitive,
   - optional cells for locality and bounded rebuild packaging later,
   - and separate treatment for pairwise-similarity and dense-embedding families.

Short architecture thesis after this batch:

```text
PageRank, BFS, and WCC fit the "flat CSR + sidecars + bounded scratch" story.
FastRP fits only if embeddings are treated as heavyweight sidecar/state artifacts.
NodeSimilarity is the first major family that actively pressures the architecture
toward selective spill, candidate pruning, or GraphBLAS-style alternatives.
```

## Scope

This batch answers two linked questions:

1. Which mirrored repos actually contain real low-RAM graph design moves in
   runtime code, not just README language?
2. When representative Neo4j GDS families are traced from public procedures to
   implementation and estimate classes, what storage/runtime shapes do they
   actually demand from v003?

This is not yet a complete per-procedure ledger for all GDS algorithms. It is a
representative-family pass meant to prevent the storage decision from being made
off topology alone.

## Graph-Tool Execution For This Batch

This batch explicitly used the two local graph-evidence skills required by the
learning spec:

- `/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/SKILL.md`
- `/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/SKILL.md`

The repo-scope truthcheck for these runs already lives in
`Reference-Shelf-Graph-Evidence-Ledger.md`. The table below records the
discovery queries actually used in this batch.

| repo | graph-tool status | query / analysis question | matched symbol | verified source implication |
| --- | --- | --- | --- | --- |
| `neo4j-gds-src` | `CBM query-ready`, `CGC low-yield` | `PageRank*`, `*MemoryEstimateDefinition*`, `CentralityAlgorithms*` | `PageRankStreamProc`, `PageRankMemoryEstimateDefinition`, `CentralityAlgorithms.java` | public GDS proc classes, implementation classes, and estimate definitions are structurally traceable even though CGC is zero-indexed on this repo in the current shelf pass |
| `graphchi-cpp-src` | `CBM query-ready`, `CGC low-yield` | `*engine*`, `*shard*`, `*memory*` | `graphchi_engine.hpp`, `slidingshard.hpp`, `memoryshard.hpp` | low-RAM claims are backed by explicit memory budget and streamed shard code |
| `minigraph-src` | `DualSemanticReady` | `buffer_size` | `buffer_size` in `minigraph_sys.h`, `load_component.h`, and multiple apps | bounded resident fragments are a runtime control, not just CLI decoration |
| `kuzu-src` | `CBM query-ready`, `CGC low-yield` | `*CSR*`, `*Adj*List*` | `csr_node_group.h`, `csr_chunked_node_group.h` | CSR is part of the storage/table layer, not only a benchmark view |
| `gapbs-src` | `DualSemanticReady` | `CSRGraph` | `CSRGraph` in `src/graph.h` | good in-memory CSR oracle and fixture baseline |
| `ligra-src` | `DualSemanticReady` | `compressedSymmetricVertex` | `compressedSymmetricVertex` in `ligra/compressedVertex.h` | compressed adjacency is a first-class execution representation |
| `sprs-src` | `DualSemanticReady` | `CsMat` | `CsMat` / `CsMatBase` | pure Rust compressed sparse matrix shape is available for fixtures and experiments |

## Evidence Ledger

| claim_id | source_path | symbol_or_query | sourced_fact | inference | speculation | PRD impact | skeptical note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `B07-001` | `gitrefrepo/neo4j-gds-src/proc/centrality/.../PageRankStreamProc.java:41-56`, `.../CentralityAlgorithms.java:366-387`, `.../PageRankComputation.java:35-114`, `.../PageRankMemoryEstimateDefinition.java:29-37` | `gds.pageRank.stream`, `PageRankAlgorithm`, `PageRankComputation` | PageRank has public stream + estimate entrypoints, routes through `CentralityAlgorithms`, executes a Pregel-style computation, and estimates memory through `Pregel.memoryEstimation(...)` with a double node value. | PageRank fits a canonical topology plus per-node numeric state model better than a custom persistent layout. | Later tuning might want special packing for hot vectors, but not a separate durable topology. | Supports flat CSR plus weight sidecar plus result sidecar; no separate PageRank layout is justified yet. | This says what the family needs now, not that PageRank will be cheap at 200M nodes. |
| `B07-002` | `gitrefrepo/neo4j-gds-src/proc/path-finding/.../BfsStreamProc.java:41-56`, `.../BfsBaseConfig.java:27-40`, `.../BFS.java:62-145`, `.../BfsMemoryEstimateDefinition.java:32-70` | `gds.bfs.stream`, `BFS`, `BfsMemoryEstimateDefinition` | BFS uses source/target node configs, allocates `traversedNodes`, `weights`, and a `visited` bitset, and its estimate includes per-thread `localNodes` / `chunks` ranges. | BFS is scratch-heavy but topology-light; the persistent need is adjacency, while the hard part is bounded frontier state. | Tile-local BFS may help locality later, but the family does not require cells for correctness. | Supports flat CSR plus bounded scratch planning; strengthens the case for explicit algorithm-state budgeting. | This is one traversal family only; weighted shortest-path families may pressure the design differently. |
| `B07-003` | `gitrefrepo/neo4j-gds-src/proc/community/.../WccStreamProc.java:41-56`, `.../WccBaseConfig.java:30-46`, `.../CommunityAlgorithms.java:439-450`, `.../Wcc.java:54-120`, `.../WccMemoryEstimateDefinition.java:27-40` | `gds.wcc.stream`, `Wcc`, `HugeAtomicDisjointSetStruct` | WCC exposes stream + estimate, accepts `seedProperty`, `relationshipWeightProperty`, and `consecutiveIds`, executes through `WccStub` / `Wcc`, and estimates memory mainly as a disjoint-set structure. | WCC needs optional property sidecars plus per-node community scratch, not a second topology format. | A future packed community workspace might help huge graphs, but the traced family does not justify a persistent WCC layout. | Supports flat/reverse CSR plus optional weight/seed sidecars and per-node result writeback. | Some community algorithms like Louvain/Leiden still need separate tracing; WCC is the easy member of the family. |
| `B07-004` | `gitrefrepo/neo4j-gds-src/proc/similarity/.../NodeSimilarityStreamProc.java:41-56`, `.../NodeSimilarityBaseConfig.java:35-110`, `.../SimilarityAlgorithms.java:196-236`, `.../NodeSimilarity.java:220-320`, `.../NodeSimilarityMemoryEstimateDefinition.java:36-108`, `.../NodeSimilarityWriteStep.java:37-84` | `gds.nodeSimilarity.stream`, `NodeSimilarity`, `TopKMap` | NodeSimilarity may run WCC first, materializes neighbor vectors and optional weight vectors, supports `topK` / `topN` / component filters, and can write relationship results. | This is the first representative family that clearly pressures the architecture toward pruning, spill, or sparse-kernel alternatives rather than pure flat-CSR iteration. | A GraphBLAS-style or candidate-blocking implementation may become preferable for the similarity family. | Supports the thesis that similarity is a `NeedsArchitectureSpike` family even if basic surface registration exists. | This is a representative trace, not proof that all similarity procedures share identical state shape. |
| `B07-005` | `gitrefrepo/neo4j-gds-src/proc/embeddings/.../FastRPStreamProc.java:41-56`, `.../FastRPBaseConfig.java:34-85`, `.../NodeEmbeddingAlgorithms.java:80-104`, `.../FastRP.java:55-130`, `.../FastRPMemoryEstimateDefinition.java:30-50`, `.../FastRPWriteStep.java:35-64` | `gds.fastRP.stream`, `FastRP`, `FastRPMemoryEstimateDefinition` | FastRP depends on embedding dimension, feature properties, optional relationship weights, random seed, and allocates `propertyVectors` plus three per-node embedding arrays before writing node-property embeddings. | Dense embedding families pressure result-sidecar design and strict-RAM rejection much more than topology design. | Cells may help packaging hot subsets later, but they are not the first-order answer; embedding sidecars and spill matter more. | Supports treating embeddings as heavyweight sidecar and result artifacts, not as a reason to invent a dedicated topology format. | FastRP is still friendlier than training-heavy GraphSAGE / Node2Vec families; those remain to be traced. |
| `B07-006` | `gitrefrepo/graphchi-cpp-src/src/engine/graphchi_engine.hpp:105-136`, `...:304-330`, `gitrefrepo/graphchi-cpp-src/src/shards/slidingshard.hpp:152-170` | `membudget_mb`, `determine_next_window`, `sliding_shard` | GraphChi has an explicit `membudget_mb`, computes the next window against a byte budget, and streams a shard that can only read one direction one chunk at a time. | GraphChi is a real runtime precedent for bounded graph windows rather than just a disk-backed slogan. | A strict-RAM execution profile for v003 could borrow GraphChi-style byte-budgeted window sizing without borrowing its whole API. | Strongly supports explicit-RAM global scan / window planning in v003. | GraphChi is not a property-graph database, so it cannot answer catalog or sidecar questions by itself. |
| `B07-007` | `gitrefrepo/minigraph-src/minigraph/minigraph_sys.h:42-92`, `gitrefrepo/minigraph-src/minigraph/components/load_component.h:57-105`, `gitrefrepo/minigraph-src/README.md:85-108` | `buffer_size`, `NativeSemaphore`, `binary CSR` | MiniGraph asserts `buffer_size >= 1`, uses it to size semaphores and queues, and documents binary CSR plus bounded in-memory fragments. | MiniGraph is a direct precedent for turning resident working set into a first-class user/runtime knob. | A future strict-RAM profile could expose a queue/window budget inspired by MiniGraph even if the rest of the engine differs. | Strongly supports bounded scratch and explicit out-of-core control in v003. | MiniGraph shows runtime control, but not Neo4j-compatible API or property semantics. |
| `B07-008` | `gitrefrepo/kuzu-src/src/include/storage/table/csr_node_group.h:21-29`, `...:81-99`, `...:165-179` | `NodeCSRIndex`, `CSRIndex`, `CSRNodeGroup` | Kuzu stores persistent data in CSR format, transient append data separately, and marks CSR as a node-group data format at the storage layer. | Disk-native CSR storage is compatible with a modern embedded graph system; CSR need not only be an in-memory projection. | v003 could adopt durable CSR snapshot packaging without becoming a Kuzu clone. | Strengthens the case for snapshot-topology files that are structurally first-class, not accidental exports. | Kuzu also has a full DB execution model that v003 is not required to copy. |
| `B07-009` | `gitrefrepo/gapbs-src/src/graph.h:98-180` | `CSRGraph` | GAPBS uses explicit CSR index and neighbor arrays, with separate in/out indices for directed graphs. | GAPBS is a strong oracle and fixture shape for flat dual-CSR semantics. | v003 parity harnesses can borrow GAPBS-like graph fixtures and expectations. | Supports fixture/oracle strategy for traversal and centrality kernels. | GAPBS is an in-memory benchmark library, not a low-RAM serving architecture. |
| `B07-010` | `gitrefrepo/ligra-src/ligra/compressedVertex.h:261-319` | `compressedSymmetricVertex` | Ligra defines compressed symmetric/asymmetric vertices and decodes neighborhoods lazily through decode helpers. | Compression can live in the execution representation without changing the public graph abstraction. | A later compact snapshot tier could experiment with compressed adjacency payloads behind the same logical API. | Supports optional compressed snapshot experiments after baseline flat CSR is proven. | Ligra is still fundamentally a shared-memory engine, not a disk-first system. |
| `B07-011` | `gitrefrepo/sprs-src/sprs/src/sparse.rs:14-50`, `gitrefrepo/sprs-src/sprs/src/lib.rs:183-200` | `CsMatBase`, `CsMat::new` | `sprs` treats compressed CSR/CSC matrices as core Rust types with sorted indices and direct owned/borrowed variants. | Rust-native sparse matrix fixtures and experiments are viable without inventing all sparse primitives from scratch. | v003 could use `sprs`-like shapes for harnesses or internal experiments without committing to it as production substrate. | Supports `REQ-LEARN-026.0` fixture/oracle scaffolding. | A fixture helper is not a production architecture by itself. |

## Representative GDS Family Trace

These rows are not the full GDS surface. They are the representative family
pass needed before claiming storage sufficiency.

| family | public modes seen in source | config surface | implementation path | estimate path | dominant runtime state | write / mutate target | v003 storage implication | current fit status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `PageRank / rank propagation` | `stream`, `stats`, `mutate`, `write`, `estimate` | `PageRankStreamConfig`, `PageRankStatsConfig`, `PageRankMutateConfig`, `PageRankWriteConfig` | `PageRankStreamProc` -> `CentralityAlgorithms.pageRank(...)` -> `PageRankAlgorithm` -> `PageRankComputation` | `PageRankMemoryEstimateDefinition` via `Pregel.memoryEstimation(...)` | per-node double value plus message passing over graph edges | mutate/write as node-property values via `PageRankWriteStep` | flat CSR or equivalent adjacency, optional weight sidecar, result sidecar for scores | `P1-friendly` on flat CSR plus sidecars |
| `BFS / frontier traversal` | `stream`, `stats`, `mutate`, `estimate` | `BfsBaseConfig`, `BfsStreamConfig`, source node, target nodes, optional `maxDepth` | `BfsStreamProc` -> `PathFindingAlgorithms.breadthFirstSearch(...)` -> `BreadthFirstSearch` -> `BFS` | `BfsMemoryEstimateDefinition` | visited bitset, traversed node array, weight/depth array, per-thread local node chunks | mutate path exists; no write proc found in the current public surface pass | canonical adjacency plus bounded frontier scratch, no persistent BFS layout | `P1-friendly` on flat CSR plus bounded scratch |
| `WCC / connectivity` | `stream`, `stats`, `mutate`, `write`, `estimate` | `WccBaseConfig` with optional `seedProperty`, `relationshipWeightProperty`, `threshold`, `consecutiveIds` | `WccStreamProc` -> `CommunityAlgorithms.wcc(...)` -> `WccStub` -> `Wcc` | `WccMemoryEstimateDefinition` | `HugeAtomicDisjointSetStruct` plus optional seeding and threshold logic | mutate/write as node-property values via `WccWriteStep` | flat or inverse-indexed adjacency, optional weight/seed sidecars, result sidecar / writeback | `P1-friendly` on flat/reverse CSR plus sidecars |
| `NodeSimilarity / pairwise similarity` | `stream`, `stats`, `mutate`, `write`, `estimate`; filtered variants too | `NodeSimilarityBaseConfig` with `topK`, `topN`, degree cutoffs, component filtering, optional weights | `NodeSimilarityStreamProc` -> `SimilarityAlgorithms.nodeSimilarity(...)` -> `NodeSimilarity` | `NodeSimilarityMemoryEstimateDefinition` | neighbor vectors, optional weight vectors, source/target filters, optional WCC prepass, `TopKMap` / `TopNList`, optional similarity graph | mutate/write create relationship-oriented similarity results via `NodeSimilarityWriteStep` | canonical adjacency is necessary but not sufficient; candidate pruning, spill, or sparse-kernel path is likely needed | `NeedsArchitectureSpike` |
| `FastRP / embedding propagation` | `stream`, `stats`, `mutate`, `write`, `estimate` | `FastRPBaseConfig` with embedding dimension, property ratio, feature properties, weights, seed | `FastRPStreamProc` -> `NodeEmbeddingAlgorithms.fastRP(...)` -> `FastRP` | `FastRPMemoryEstimateDefinition` | property vectors plus three per-node embedding arrays | mutate/write as node-property embeddings via `FastRPWriteStep` | topology plus feature-property sidecars plus embedding/result sidecars; strict RAM must account for dense matrices | `P2-but-feasible` with strong budget gates |

## Low-RAM Precedent Corrections After Runtime Inspection

The earlier draft was directionally right but too README-heavy. After reading the
actual runtime code, the more precise interpretation is:

### 1. GraphChi is a real memory-budgeted window engine

Runtime evidence:

- `gitrefrepo/graphchi-cpp-src/src/engine/graphchi_engine.hpp:105-136`
- `gitrefrepo/graphchi-cpp-src/src/engine/graphchi_engine.hpp:304-330`
- `gitrefrepo/graphchi-cpp-src/src/shards/slidingshard.hpp:152-170`

What survives source inspection:

- `membudget_mb` is an actual runtime setting.
- `determine_next_window(...)` computes window size against a byte budget.
- `sliding_shard` is explicitly a streamed one-direction, one-chunk-at-a-time
  shard abstraction.

v003 implication:

```text
GraphChi is not just "graph on disk".
It is a concrete precedent for strict-RAM window planning on top of a
canonical graph layout.
```

### 2. MiniGraph is a real bounded-working-set pipeline

Runtime evidence:

- `gitrefrepo/minigraph-src/minigraph/minigraph_sys.h:42-92`
- `gitrefrepo/minigraph-src/minigraph/components/load_component.h:57-105`
- `gitrefrepo/minigraph-src/README.md:85-108`

What survives source inspection:

- `buffer_size` is asserted, logged, and used to size semaphores and queues.
- the load component creates a semaphore from `buffer_size_` and drains work
  accordingly.
- binary CSR is still the durable graph shape.

v003 implication:

```text
MiniGraph is the clearest local precedent for making resident analytical
working-set size a first-class operator knob rather than an accidental outcome.
```

### 3. Kuzu keeps CSR at the storage/table layer

Runtime evidence:

- `gitrefrepo/kuzu-src/src/include/storage/table/csr_node_group.h:21-29`
- `gitrefrepo/kuzu-src/src/include/storage/table/csr_node_group.h:81-99`
- `gitrefrepo/kuzu-src/src/include/storage/table/csr_node_group.h:165-179`

What survives source inspection:

- Kuzu models node-CSR indices directly.
- it distinguishes persistent CSR-organized data from transient append data.
- `CSRNodeGroup` is a storage format, not just a temporary query structure.

v003 implication:

```text
Durable CSR snapshot packaging is a legitimate database-storage move,
not merely a benchmark or export trick.
```

### 4. GAPBS, Ligra, and sprs are baseline-shape oracles, not low-RAM servers

Runtime evidence:

- `gitrefrepo/gapbs-src/src/graph.h:98-180`
- `gitrefrepo/ligra-src/ligra/compressedVertex.h:261-319`
- `gitrefrepo/sprs-src/sprs/src/sparse.rs:14-50`

What survives source inspection:

- `GAPBS` gives a direct in/out CSR baseline for test and oracle work.
- `Ligra` gives compressed symmetric/asymmetric vertex shapes with decode
  helpers.
- `sprs` gives Rust-native CSR/CSC compressed matrix shapes.

v003 implication:

```text
These repos matter more for canonical shapes, fixtures, and compression ideas
than for end-to-end low-RAM product architecture.
```

## Architecture Fit Matrix

| family / capability | topology need | sidecar need | dominant state / scratch | likely artifact target | flat CSR only? | optional cells pressure | GraphBLAS pressure | recommendation |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `PageRank` | forward adjacency, optional weight-aware degree logic | relationship-weight sidecar, result property sidecar | per-node double rank state plus Pregel messaging | node-property result sidecar / writeback | almost | low | low / optional | keep on flat CSR plus sidecars |
| `BFS / traversal` | forward adjacency; reverse helpful for related families | little or none beyond source/target config metadata | visited bitset, traversed node list, depth/weight array, frontier chunks | stream rows or mutate-side scratch/result | no, needs scratch plan | medium for locality only | low | keep on flat CSR plus bounded scratch; cells later only if measured |
| `WCC` | undirected or inverse-indexed traversal semantics | seed property and optional weight sidecar | disjoint set per node | node-property result sidecar / writeback | almost | low | low | keep on flat/reverse CSR plus sidecars |
| `NodeSimilarity` | adjacency access for many node pairs | optional weight sidecar, component sidecar, result relationship artifact | vectors, weights, topK/topN, optional WCC, candidate explosion | relationship-result artifact or graph result | no | low-medium | medium-high | treat as spike family; design pruning/spill before claiming support |
| `FastRP` | canonical adjacency | feature-property sidecars, optional relationship-weight sidecar, embedding result sidecar | dense property vectors plus three per-node embedding arrays | node-embedding sidecar / writeback | no | low | medium | support only with explicit dimension-based RAM rejection |
| `General low-RAM global scan profile` | canonical adjacency stream | optional columnar sidecars | window buffers, spill, result scratch | streamed execution, not extra topology | no | low | optional by family | borrow GraphChi / MiniGraph control ideas |

## What This Batch Strengthens

### Stronger than the earlier draft

- The low-RAM shortlist is no longer justified mainly by README language.
- Representative GDS families now have source-backed proc/config/kernel/estimate
  traces.
- Similarity and embedding families are now visibly different from traversal and
  centrality families in storage pressure.

### Still intentionally unresolved

- This is not yet a full per-procedure kernel ledger for all of GDS.
- Louvain, Leiden, triangle count, Node2Vec, GraphSAGE, KNN, and model/pipeline
  families still need direct tracing before they can move out of
  `NeedsArchitectureSpike` or `ArtifactPartial`.
- This batch does not yet prove benchmark or observability discipline. That
  remains Batch 08.

## Requirement Impact

| requirement | after this batch | note |
| --- | --- | --- |
| `REQ-LEARN-024.0` | `ArtifactCovered` | low-RAM out-of-core graph systems are now supported by runtime code evidence, not only README summaries |
| `REQ-LEARN-025.0` | `ArtifactCovered` | GraphBLAS and sparse-matrix alternatives are now placed correctly as selective substrate options rather than universal answers |
| `REQ-LEARN-008.0` | stronger `ArtifactPartial` | estimator semantics now include representative PageRank, BFS, WCC, NodeSimilarity, and FastRP traces |
| `REQ-LEARN-009.0` | stronger `ArtifactPartial` | state-shape classification now has representative family evidence |
| `REQ-LEARN-015.0` | stronger `ArtifactPartial` | representative post-surface algorithm baselines now exist, but not yet the whole family set |
| `REQ-LEARN-026.0` | stronger `ArtifactPartial` | GAPBS, Ligra, and sprs now provide better fixture/oracle shape evidence |
| `REQ-LEARN-044.0` | stronger `ArtifactPartial` | proc-to-kernel tracing now exists for representative families |
| `REQ-LEARN-045.0` | stronger `ArtifactPartial` | kernel-derived storage implications now exist for representative families |
| `REQ-LEARN-046.0` | stronger `ArtifactPartial` | estimator definitions are now linked to real family state shapes |
| `REQ-LEARN-047.0` | stronger `ArtifactPartial` | family feasibility is more grounded, but not complete |
| `REQ-LEARN-048.0` | stronger `ArtifactPartial` | oracle direction is clearer, but batch does not yet define every family harness |

## Skeptical Review

| concern | why it is fair | current answer |
| --- | --- | --- |
| `GraphBLAS temptation` | Sparse algebra is elegant and could seduce us into redesigning everything around it. | The traced GDS families do not require a GraphBLAS-only architecture. Similarity is the first real pressure point; traversal/centrality are not. |
| `Cells everywhere` | Once locality enters the conversation, it is easy to over-prescribe cells. | The representative families still fit flat CSR plus sidecars first. Cells remain a packaging/locality optimization until measured otherwise. |
| `README bias` | Earlier drafts could have mistaken documentation tone for runtime truth. | This batch corrects that with runtime file evidence from GraphChi, MiniGraph, Kuzu, GAPBS, Ligra, and sprs. |
| `Representative-family overreach` | Five families are not the whole GDS surface. | Correct. This batch intentionally upgrades confidence without pretending complete family coverage. |
| `Memory-estimate optimism` | Having estimate classes does not guarantee real strict-RAM execution. | Correct. Neo4j GDS estimate definitions help classify state shape, not prove v003 execution policy. Strict-RAM remains a v003 design obligation. |

## Checkpoint Summary

What changed in the architecture conversation because of this batch:

1. The first architecture choice is still not `cells or no cells`.
   It is still:
   - canonical topology shape,
   - typed sidecars,
   - scratch/state budgeting,
   - publication semantics.
2. The strongest family split is now visible:
   - `PageRank`, `BFS`, `WCC`: good first-wave fits for flat CSR plus sidecars.
   - `FastRP`: feasible, but only with honest dense-state accounting.
   - `NodeSimilarity`: genuine architecture spike family.
3. The best low-RAM external borrow remains operational rather than cosmetic:
   - byte- or queue-bounded working sets,
   - streamed windows,
   - durable compact structure,
   - explicit refusal before memory blow-up.

Best next move after this batch:

```text
Trace one more hard family cluster:
  Louvain / Leiden / triangle / Node2Vec / KNN
then move to benchmark + observability discipline.
```

## References

- `gitrefrepo/neo4j-gds-src/proc/centrality/src/main/java/org/neo4j/gds/pagerank/PageRankStreamProc.java`
- `gitrefrepo/neo4j-gds-src/algo/src/main/java/org/neo4j/gds/pagerank/PageRankComputation.java`
- `gitrefrepo/neo4j-gds-src/algo/src/main/java/org/neo4j/gds/pagerank/PageRankMemoryEstimateDefinition.java`
- `gitrefrepo/neo4j-gds-src/applications/algorithms/centrality/src/main/java/org/neo4j/gds/applications/algorithms/centrality/CentralityAlgorithms.java`
- `gitrefrepo/neo4j-gds-src/proc/path-finding/src/main/java/org/neo4j/gds/paths/traverse/BfsStreamProc.java`
- `gitrefrepo/neo4j-gds-src/procedures/facade-api/configs/path-finding-configs/src/main/java/org/neo4j/gds/paths/traverse/BfsBaseConfig.java`
- `gitrefrepo/neo4j-gds-src/algo/src/main/java/org/neo4j/gds/paths/traverse/BFS.java`
- `gitrefrepo/neo4j-gds-src/algo/src/main/java/org/neo4j/gds/paths/traverse/BfsMemoryEstimateDefinition.java`
- `gitrefrepo/neo4j-gds-src/proc/community/src/main/java/org/neo4j/gds/wcc/WccStreamProc.java`
- `gitrefrepo/neo4j-gds-src/procedures/facade-api/configs/community-configs/src/main/java/org/neo4j/gds/wcc/WccBaseConfig.java`
- `gitrefrepo/neo4j-gds-src/algo/src/main/java/org/neo4j/gds/wcc/Wcc.java`
- `gitrefrepo/neo4j-gds-src/algo/src/main/java/org/neo4j/gds/wcc/WccMemoryEstimateDefinition.java`
- `gitrefrepo/neo4j-gds-src/proc/similarity/src/main/java/org/neo4j/gds/similarity/nodesim/NodeSimilarityStreamProc.java`
- `gitrefrepo/neo4j-gds-src/procedures/facade-api/configs/similarity-configs/src/main/java/org/neo4j/gds/similarity/nodesim/NodeSimilarityBaseConfig.java`
- `gitrefrepo/neo4j-gds-src/algo/src/main/java/org/neo4j/gds/similarity/nodesim/NodeSimilarity.java`
- `gitrefrepo/neo4j-gds-src/algo/src/main/java/org/neo4j/gds/similarity/nodesim/NodeSimilarityMemoryEstimateDefinition.java`
- `gitrefrepo/neo4j-gds-src/proc/embeddings/src/main/java/org/neo4j/gds/embeddings/fastrp/FastRPStreamProc.java`
- `gitrefrepo/neo4j-gds-src/procedures/facade-api/configs/node-embeddings-configs/src/main/java/org/neo4j/gds/embeddings/fastrp/FastRPBaseConfig.java`
- `gitrefrepo/neo4j-gds-src/algo/src/main/java/org/neo4j/gds/embeddings/fastrp/FastRP.java`
- `gitrefrepo/neo4j-gds-src/algo/src/main/java/org/neo4j/gds/embeddings/fastrp/FastRPMemoryEstimateDefinition.java`
- `gitrefrepo/graphchi-cpp-src/src/engine/graphchi_engine.hpp`
- `gitrefrepo/graphchi-cpp-src/src/shards/slidingshard.hpp`
- `gitrefrepo/minigraph-src/minigraph/minigraph_sys.h`
- `gitrefrepo/minigraph-src/minigraph/components/load_component.h`
- `gitrefrepo/minigraph-src/README.md`
- `gitrefrepo/kuzu-src/src/include/storage/table/csr_node_group.h`
- `gitrefrepo/gapbs-src/src/graph.h`
- `gitrefrepo/ligra-src/ligra/compressedVertex.h`
- `gitrefrepo/sprs-src/sprs/src/sparse.rs`
