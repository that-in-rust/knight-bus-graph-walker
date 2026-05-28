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
