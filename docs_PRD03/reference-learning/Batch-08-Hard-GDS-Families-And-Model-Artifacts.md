# Batch 08: Hard GDS Families, Model Artifacts, And Storage Fit

Date: 2026-06-24

Assigned lanes:

- `Capability lane`
- `Architecture lane`
- `Execution lane`
- `Rejection lane`

Assigned PRD outcomes:

- `Full GDS family diligence beyond representative easy kernels`
- `Mutate / write / model / pipeline semantics`
- `Strict-RAM architecture fit for hard families`
- `Artifact-plane and metadata-plane requirements`

Requirement IDs touched in this batch:

- `REQ-LEARN-008.0`
- `REQ-LEARN-009.0`
- `REQ-LEARN-010.0`
- `REQ-LEARN-015.0`
- `REQ-LEARN-026.0`
- `REQ-LEARN-044.0`
- `REQ-LEARN-045.0`
- `REQ-LEARN-046.0`
- `REQ-LEARN-047.0`
- `REQ-LEARN-048.0`

## Answer First

This batch answers the question that Batch 07 intentionally left open:
what happens when we stop tracing only friendly families like PageRank, BFS,
and WCC, and instead inspect the harder GDS surfaces that are more likely to
break a naive CSR-only plan?

The strongest conclusions are:

1. `Louvain` and `Leiden` do not justify dedicated persistent on-disk topology
   formats. They justify a runtime graph-workspace for aggregation,
   intermediate-community handling, and larger scratch budgets.
2. `TriangleCount` still fits canonical adjacency, but it specifically wants
   undirected projections, sorted/intersection-friendly neighborhoods, and
   degree-control gates. Its pressure is on execution strategy, not stored
   layout diversity.
3. `Node2Vec` is not primarily a topology problem. It is a random-walk corpus
   plus training-model problem. The dominant architectural need is spillable
   scratch and dense embedding artifacts, not a special graph file format.
4. `KNN` and `FilteredKnn` prove that not all “graph algorithms” are topology
   led. These families are primarily `node-property-plane` algorithms that emit
   relationship artifacts. This strongly strengthens the case for a typed
   property plane that is as first-class as CSR topology.
5. `PipelineCatalog` and `OpenModelCatalog` prove that full GDS compatibility
   requires durable thinking about `cataloged artifacts outside topology`:
   pipelines, models, mutate-side intermediate properties, and estimate
   composition.
6. The “cells” question stays open, but the hard-family evidence here still
   does not force cells as a first requirement. These families mostly force:
   - canonical topology,
   - typed property sidecars,
   - runtime graph workspaces,
   - result/model artifacts,
   - and hard RAM rejection or spill policies.

Short architecture thesis after this batch:

```text
Hard GDS families argue against "many durable graph formats" and in favor of:
one canonical graph topology substrate,
one strong typed property plane,
one runtime artifact/scratch plane,
and explicit model/pipeline catalogs.

Cells remain an optional packaging/locality decision,
not the first-order answer demanded by Louvain, Leiden, TriangleCount,
Node2Vec, KNN, or link-prediction pipeline semantics.
```

## Scope

This batch is deliberately narrower than “all GDS algorithms,” but deeper than
the representative-family pass in Batch 07.

It traces:

- `Louvain`
- `Leiden`
- `TriangleCount`
- `Node2Vec`
- `KNN`
- `FilteredKnn`
- `LinkPrediction pipeline + PipelineCatalog + OpenModelCatalog`

It also cross-checks these families against local prior repos that matter for
their execution shape:

- `gapbs-src`
- `lagraph-src`
- `graphblas-src`
- `snap-src`

## Graph-Tool Execution For This Batch

This batch explicitly used the two local graph-evidence skills required by the
learning spec:

- `/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/SKILL.md`
- `/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/SKILL.md`

The shelf-wide truthcheck remains authoritative for tool readiness:
`Reference-Shelf-Graph-Evidence-Ledger.md`.

This batch used the following repo-level discovery queries before narrowing to
direct file reads.

| repo | graph-tool status used in this batch | discovery query or probe | matched evidence | why it mattered |
| --- | --- | --- | --- | --- |
| `neo4j-gds-src` | `CBM query-ready`, `CGC low-yield` | `search_graph` over `Louvain`, `Leiden`, `TriangleCount`, `Node2Vec`, `FilteredKnn`, `Knn`, `PipelineCatalog`, `OpenModelCatalog` | class hits for all target families plus proc, write-step, and memory-estimate classes | proved the hard-family and artifact surface is structurally traceable even though CGC remains zero-indexed here |
| `gapbs-src` | `DualSemanticReady` | `cgc query "MATCH (f:Function) RETURN f.name LIMIT 10"` | rows included `BenchmarkKernel` | gave a second-tool check for the triangle-count oracle repo |
| `lagraph-src` | `CBM query-ready`, `CGC low-yield` | `rg` and CBM discovery for `LAGr_TriangleCount` | multiple method and presort variants | showed triangle count can vary by kernel method without changing stored graph shape |
| `snap-src` | `CBM query-ready`, `CGC low-yield` | CBM and `rg` for `CommunityCNM`, `KNNJaccard`, `node2vec` | direct community, KNN-graph, and node2vec symbols | gave a second implementation family for community, KNN-style graph output, and walk-plus-word2vec embeddings |
| `graphblas-src` | `CBM query-ready`, `CGC low-yield` | `rg` for `triangle count`, `BFS`, and semiring notes | explicit comments linking GraphBLAS semirings to triangle count and BFS | supports “GraphBLAS as optional execution substrate,” not “GraphBLAS as default storage substrate” |

## Evidence Ledger

| claim_id | source_path | symbol_or_query | sourced_fact | inference | speculation | PRD impact | skeptical note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `B08-001` | `gitrefrepo/neo4j-gds-src/proc/community/src/main/java/org/neo4j/gds/louvain/LouvainStreamProc.java:41-57`, `.../LouvainBaseConfig.java:30-80` | `gds.louvain.stream`, `LouvainBaseConfig` | Louvain exposes `stream` and `stream.estimate`; config includes `seedProperty`, `relationshipWeightProperty`, `tolerance`, `maxIterations`, `maxLevels`, and `includeIntermediateCommunities`. | Louvain is not a trivial one-pass family; it already encodes hierarchical and intermediate-result semantics in public config. | A future v003 may gate some knobs behind support tiers, but it cannot pretend the family is just “community id per node.” | Strengthens the need for runtime community workspace and result-sidecar semantics. | A config-rich family is not yet proof that a custom durable layout is needed. |
| `B08-002` | `gitrefrepo/neo4j-gds-src/applications/algorithms/community/src/main/java/org/neo4j/gds/applications/algorithms/community/CommunityAlgorithms.java:309-334`, `.../algo/src/main/java/org/neo4j/gds/louvain/LouvainMemoryEstimateDefinition.java:48-80`, `.../LouvainWriteStep.java:47-85` | `CommunityAlgorithms.louvain`, `LouvainMemoryEstimateDefinition`, `LouvainWriteStep` | GDS builds a `Louvain` runtime with `maxLevels` and `includeIntermediateCommunities`; its memory estimate explicitly says Louvain creates a new graph every iteration and sizes that subgraph with `CSRGraphStoreFactory`; write mode may emit intermediate communities or current dendrogram state. | Louvain pressures `runtime graph aggregation` and `hierarchical result handling`, not a second persistent topology family. | A future low-RAM implementation may need spillable aggregation or bounded contracted-graph workspaces. | Strong evidence for `runtime scratch + result sidecar`, not `persistent Louvain layout`. | If contracted graphs become too large, the family could still force a stronger runtime storage subsystem later. |
| `B08-003` | `gitrefrepo/neo4j-gds-src/proc/community/src/main/java/org/neo4j/gds/leiden/LeidenStreamProc.java:42-58`, `.../LeidenBaseConfig.java:30-76`, `.../CommunityAlgorithms.java:273-299` | `gds.leiden.stream`, `LeidenBaseConfig`, `CommunityAlgorithms.leiden` | Leiden exposes `stream` and `estimate`; config includes `gamma`, `theta`, `maxLevels`, `includeIntermediateCommunities`, random seed, and tolerance; runtime rejects non-undirected graphs. | Leiden is even more stateful and constrained than Louvain; orientation semantics and iterative refinement matter at the API boundary. | Future strict-RAM profiles may reject some Leiden runs earlier than Louvain. | Reinforces that projection orientation and runtime state gating are first-class compatibility concerns. | Rejecting directed projections does not yet prove how expensive a correct undirected rewrite path would be in v003. |
| `B08-004` | `gitrefrepo/neo4j-gds-src/algo/src/main/java/org/neo4j/gds/leiden/LeidenMemoryEstimateDefinition.java:37-61`, `.../LeidenWriteStep.java:47-88` | `LeidenMemoryEstimateDefinition`, `LeidenWriteStep` | Leiden estimates memory for multiple per-node arrays, seeded communities, local move phase, modularity computation, dendrogram manager, refinement phase, aggregation phase, and post-aggregation arrays; write mode may emit intermediate communities or final community properties. | Leiden needs `multiple heavy per-node work arrays + hierarchical outputs`; it is a strong runtime-workspace family, not a topology-format family. | A later v003 may need a distinct “contracted graph workspace” abstraction shared by Louvain and Leiden. | Supports runtime graph workspace and hierarchical result artifact design. | Memory estimates do not reveal I/O behavior; actual spill feasibility still needs a later implementation spike. |
| `B08-005` | `gitrefrepo/neo4j-gds-src/proc/community/src/main/java/org/neo4j/gds/triangle/TriangleCountStreamProc.java:42-57`, `.../TriangleCountBaseConfig.java:36-70`, `.../CommunityAlgorithms.java:405-425`, `.../IntersectingTriangleCountMemoryEstimateDefinition.java:27-34`, `.../IntersectingTriangleCount.java:42-129`, `.../TriangleCountWriteStep.java:44-63` | `gds.triangleCount.stream`, `TriangleCountBaseConfig`, `IntersectingTriangleCount` | Triangle count requires undirected relationship projections, accepts `maxDegree`, executes through `IntersectingTriangleCount.create(...)`, stores per-node triangle counts, and writes node properties from `localTriangles()`. | Triangle count wants sorted/intersection-friendly adjacency and degree gates; it does not ask for a dedicated durable layout. | A future strict-RAM plan might need multiple triangle strategies for high-degree graphs. | Strongly supports `canonical adjacency + degree-aware runtime strategy + node-result sidecar`. | The estimate shown here is light because it captures output arrays, not all transient intersection work; runtime behavior can still be harsher on skewed graphs. |
| `B08-006` | `gitrefrepo/gapbs-src/src/tc.cc:108-141` | `TCVerifier`, `BenchmarkKernel` | GAPBS verifies triangle count by intersecting sorted neighborhoods with `set_intersection`, divides by six, and requires an undirected graph. | GAPBS is a good correctness oracle for triangle-count parity and neighborhood-intersection semantics. | v003 parity fixtures can use GAPBS-style oracle logic for triangle families. | Strengthens `REQ-LEARN-026.0` fixture and oracle scaffolding. | GAPBS is a correctness oracle, not a product architecture precedent. |
| `B08-007` | `gitrefrepo/lagraph-src/src/algorithm/LAGr_TriangleCount.c:117-210`, `.../include/LAGraph.h:2521-2555`, `gitrefrepo/graphblas-src/README.md:9-18`, `.../Source/GB_control.h:1404-1415`, `...:1821-1822`, `...:2166-2167` | `LAGr_TriangleCount`, `LAGr_TriangleCount_Method`, GraphBLAS semiring comments | LAGraph exposes multiple triangle-count methods and presort modes on symmetric graphs; GraphBLAS source comments explicitly call out triangle count and BFS as LAGraph-relevant semiring use cases. | Triangle count is a family where multiple execution kernels can share the same graph storage substrate. | A future v003 could route triangle count to direct CSR intersection first and later add GraphBLAS-backed alternatives without changing snapshot format. | Supports “execution-plan diversity over storage-layout diversity.” | GraphBLAS elegance is not proof it wins holistic RAM on the target machine class. |
| `B08-008` | `gitrefrepo/neo4j-gds-src/proc/similarity/src/main/java/org/neo4j/gds/similarity/knn/KnnStreamProc.java:41-56`, `.../KnnBaseConfig.java:30-104`, `.../KnnMemoryEstimateDefinition.java:44-88`, `.../Knn.java:41-120`, `.../KnnWriteStep.java:66-86` | `gds.knn.stream`, `KnnBaseConfig`, `KnnMemoryEstimateDefinition`, `KnnWriteStep` | KNN public config is centered on `nodeProperties`, `topK`, `sampleRate`, perturbation, random joins, sampler type, and iterations; memory is dominated by top-k neighbor lists, old/new/reverse neighbor lists, and per-thread random-neighbor work; write mode emits relationship results. | KNN is fundamentally a `property-column + candidate-graph` family. Canonical graph topology is secondary, except when used for random-walk seeding or graph-shaped outputs. | v003 may eventually support a KNN-like family without touching canonical graph topology files at all, beyond node identity and optional seeding. | Very strong evidence for a first-class typed property plane and relationship-result artifact plane. | This family is broader than a single implementation; some variants might later benefit from ANN-specific indexes. |
| `B08-009` | `gitrefrepo/neo4j-gds-src/proc/similarity/src/main/java/org/neo4j/gds/similarity/filteredknn/FilteredKnnStreamProc.java:42-57`, `.../FilteredKnnBaseConfig.java:32-66`, `.../FilteredKnnMemoryEstimateDefinition.java:36-43`, `.../FilteredKnn.java:40-118`, `.../FilteredKnnWriteProc.java:43-58`, `.../FilteredKnnWriteStep.java:66-85` | `gds.knn.filtered.stream`, `FilteredKnnBaseConfig`, `FilteredKnn` | Filtered KNN adds `sourceNodeFilter` and `targetNodeFilter`, validates them against the graph store, wraps ordinary KNN, and reuses KNN memory estimation almost entirely; write mode still emits relationship results. | `FilteredKnn` proves filter semantics belong in metadata/property/config space, not in a second stored graph layout. | Future v003 can layer source/target filters over a canonical property-plane KNN implementation instead of inventing a “filtered graph format.” | Strengthens the argument for `property plane + filter metadata + result artifacts`. | Filter validation still depends on graph-schema and label/type semantics that v003 must preserve. |
| `B08-010` | `gitrefrepo/neo4j-gds-src/proc/embeddings/src/main/java/org/neo4j/gds/embeddings/node2vec/Node2VecStreamProc.java:42-57`, `.../Node2VecBaseConfig.java:30-83`, `.../Node2VecMemoryEstimateDefinition.java:39-76`, `.../Node2Vec.java:39-130`, `.../Node2VecWriteProc.java:43-58`, `.../Node2VecWriteStep.java:44-63` | `gds.node2vec.stream`, `Node2VecMemoryEstimateDefinition`, `Node2Vec` | Node2Vec config includes random-walk and training hyperparameters; memory estimate includes random walks, probability cache, and model embeddings; runtime builds compressed random walks before training a model; write mode persists float embeddings. | Node2Vec is a `walk corpus + trainable embedding model + result artifact` family. The canonical graph is only the substrate for generating walks. | A future strict-RAM v003 may need to reject large Node2Vec jobs or spill walk corpora and training state. | Strong evidence for a separate `artifact/scratch plane` and dense embedding result sidecars. | The current openGDS clone shows write semantics, but not a proof that this family is practical under the 50GB-on-8GB target. |
| `B08-011` | `gitrefrepo/snap-src/snap-adv/n2v.h:10-47`, `.../snap-adv/word2vec.cpp:5-27`, `.../snap-core/sim.cpp:80-149`, `.../snap-core/cmty.cpp:1449-1451` | `node2vec`, `word2vec`, `KNNJaccardParallel`, `CommunityCNM` | SNAP’s node2vec API also separates walk-generation parameters from embedding output and uses word2vec-derived negative-sampling code; SNAP KNN-Jaccard builds a graph with edge similarity; community CNM is exposed as a separate family. | The two-stage shape of GDS Node2Vec is not accidental; it matches a second implementation lineage. KNN-style families also naturally emit graph-shaped result artifacts. | v003 could use SNAP-like fixture cases for walk lengths, dimensions, and relationship-result expectations. | Strengthens the family-shape inference beyond a single codebase. | SNAP is not a Neo4j-compatible system, so it supports shape inference, not compatibility claims. |
| `B08-012` | `gitrefrepo/neo4j-gds-src/proc/machine-learning/src/main/java/org/neo4j/gds/ml/linkmodels/pipeline/LinkPredictionPipelineCreateProc.java:33-40`, `.../train/LinkPredictionPipelineTrainProc.java:40-55`, `.../predict/LinkPredictionPipelineStreamProc.java:41-56`, `.../predict/LinkPredictionPipelineMutateProc.java:41-56`, `.../pipeline/src/main/java/org/neo4j/gds/ml/pipeline/PipelineCatalog.java:35-140`, `.../open-model-catalog/src/main/java/org/neo4j/gds/core/model/OpenModelCatalog.java:40-149`, `.../pipeline/src/main/java/org/neo4j/gds/ml/pipeline/NodePropertyStep.java:110-137`, `.../pipeline/src/main/java/org/neo4j/gds/ml/pipeline/linkPipeline/train/LinkPredictionTrain.java:341-395`, `.../procedures/pipelines-facade/src/main/java/org/neo4j/gds/procedures/pipelines/PipelineRepository.java:36-78` | `gds.beta.pipeline.linkPrediction.*`, `PipelineCatalog`, `OpenModelCatalog`, `NodePropertyStep.estimate` | GDS exposes create/train/predict pipeline procedures; pipelines are cataloged per user in `PipelineCatalog`; models live in `OpenModelCatalog`; pipeline step estimation composes algorithm-factory memory estimation with a `ModelCatalog`; link-prediction training estimation includes cross-validation splitting and model-training candidate selection. | Full GDS compatibility requires an explicit `metadata and artifact plane` that is neither OLTP store nor canonical OLAP topology. Pipelines and models are first-class catalog entries with their own estimate and lifecycle semantics. | v003 may need a staged support boundary where pipeline and model surfaces are `registered first`, then backed by durable artifact storage later. | Strong evidence against any architecture that thinks “graph files alone” are sufficient for full GDS. | The openGDS clone keeps some catalogs in process-local maps; v003 still has to decide which of these semantics must become durable and which can remain ephemeral. |

## Hard-Family Trace Matrix

| family | public modes verified | dominant config shape | dominant runtime state | write / mutate target | first storage implication | current architecture fit |
| --- | --- | --- | --- | --- | --- | --- |
| `Louvain` | `stream`, `stats`, `mutate`, `write`, `estimate` | weights, seed property, max levels, intermediate communities, tolerance, iterations | modularity optimization workspace, aggregated subgraph per iteration, dendrogram state | node-property writeback or intermediate-community arrays | runtime graph-workspace + result-sidecar problem | `P2-feasible-with-graph-workspace` |
| `Leiden` | `stream`, `stats`, `mutate`, `write`, `estimate`, deprecated beta aliases | undirected projection, weights, seed property, gamma, theta, random seed, max levels | multiple per-node arrays, seeded communities, refinement, aggregation, dendrogram | node-property writeback or intermediate-community arrays | runtime graph-workspace + orientation discipline + hierarchical outputs | `P2-feasible-with-graph-workspace` |
| `TriangleCount` | `stream`, `stats`, `mutate`, `write`, `estimate` | undirected relationship projections, `maxDegree` | per-node triangle counts plus neighborhood intersection work | node-property writeback | sorted/intersection-friendly adjacency and degree gating | `P1-friendly-if-adjacency-sorted` |
| `Node2Vec` | `stream`, `mutate`, `write`, `estimate`, deprecated beta aliases | walk length, walks per node, window size, negative sampling, embedding dimension, learning rates | compressed random walks, probability caches, center/context embedding model | embedding property writeback | spillable scratch/model artifact plane is more important than format multiplicity | `NeedsStrictGateOrSpill` |
| `KNN` | `stream`, `stats`, `mutate`, `write`, `estimate` | node property specs, topK, sample rate, perturbation, random joins, sampler type | top-k neighbor lists, reverse-neighbor lists, per-thread random-neighbor buffers | relationship writeback | typed property plane is mandatory; topology is secondary | `PropertyPlaneRequired` |
| `FilteredKnn` | `stream`, `stats`, `mutate`, `write`, `estimate`, deprecated alpha aliases | KNN config plus source/target node filters | KNN state plus target/source filtering and optional seeding | relationship writeback | property plane plus graph-schema-aware filter semantics | `PropertyPlaneRequired` |
| `LinkPrediction pipeline + model catalog` | `create`, `train`, `predict.stream`, `predict.mutate`, `estimate`, plus catalog list/drop/exists from prior batches | pipeline identity, training configs, prediction configs, model and step composition | pipeline catalog entries, model catalog entries, cross-validation splits, trainer candidates, step-estimation composition | models, pipelines, mutate-side graph outputs, stats | separate artifact and metadata plane is mandatory | `MetadataAndArtifactPlaneRequired` |

## What The Prior Repos Add

### Triangle families

- `gapbs-src` shows a clean triangle-count correctness oracle using sorted
  neighbor intersection and divide-by-six normalization.
- `lagraph-src` shows that triangle count can legitimately be implemented by
  multiple matrix-kernel methods and presort policies on the same graph.
- `graphblas-src` source comments explicitly connect triangle count and BFS to
  GraphBLAS semiring families used by LAGraph.

Conclusion:

```text
Triangle count strengthens execution-plan plurality, not storage-layout plurality.
```

### Community families

- `snap-src` exposes `CommunityCNM`, which is a separate community-detection
  family but does not itself force any new durable storage format.

Conclusion:

```text
Community families are runtime-state heavy, but the evidence here still points
to workspace and result handling rather than per-family persistent topology.
```

### KNN and embedding families

- `snap-src` `KNNJaccardParallel` constructs a result graph with similarity edge
  attributes.
- `snap-src` node2vec and customized word2vec code show the same two-stage
  walk-plus-training pattern seen in Neo4j GDS.

Conclusion:

```text
The architecture must treat relationship-result artifacts and embedding/model
artifacts as first-class, not as incidental outputs of a CSR traversal engine.
```

## Architecture Fit Matrix

| family / capability | topology need | property-plane need | artifact or catalog need | dominant scratch pressure | cells required? | GraphBLAS pressure | recommendation |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `Louvain` | canonical adjacency | optional weight/seed sidecars | intermediate-community and final-community outputs | high due to aggregated subgraphs and modularity work | no | low | keep on canonical topology; add runtime graph workspace |
| `Leiden` | canonical undirected adjacency | optional weight/seed sidecars | intermediate-community and final-community outputs | very high due to multiple phases and arrays | no | low-medium | keep on canonical topology; add runtime graph workspace and strict orientation checks |
| `TriangleCount` | sorted undirected adjacency | little | node-property results | medium, driven by neighborhood intersection | no | medium | start with direct CSR intersection; keep GraphBLAS as optional later execution path |
| `Node2Vec` | canonical adjacency for walk generation | optional relationship weights | embedding result artifacts | very high from random walks and model training | no | low | do not invent a topology variant; build spill/gate logic around scratch and artifacts |
| `KNN` | node identity, optional graph seeding | mandatory | relationship result artifact | very high from topK and candidate lists | no | medium | treat as property-plane-first algorithm |
| `FilteredKnn` | graph/schema metadata for filter validation | mandatory | relationship result artifact | very high, same as KNN | no | medium | treat as KNN plus metadata/filter layer |
| `Pipeline and model surface` | graph handles only indirectly | often yes, via pipeline steps | mandatory catalogs and model artifacts | high and compositional | no | low | separate metadata plane from topology plane immediately |

## Storage Verdict After Hard-Family Tracing

The hard-family evidence further sharpens the likely v003 storage split.

### 1. Canonical topology remains central

Nothing in this batch proved the need for `13` or even `6` durable
per-algorithm graph formats.

What it proved instead:

- canonical graph adjacency is still the shared substrate;
- some families need undirected or reverse-oriented logical views;
- some families need sorted/intersection-friendly neighborhood access;
- some families create temporary contracted or aggregate graphs at runtime.

### 2. Property plane is not optional

`KNN` and `FilteredKnn` push this hardest, but the signal is broader:

- weight properties,
- seed properties,
- feature vectors,
- filter metadata,
- embedding outputs,
- and writeback targets

all live outside raw topology.

### 3. Artifact plane is not optional

The following are real architectural objects, not afterthoughts:

- node-property results,
- relationship-result graphs,
- intermediate communities,
- embeddings,
- training pipelines,
- trained models,
- and estimator outputs.

### 4. Cells remain optional

This batch still does not force cells for correctness or broad GDS coverage.

What cells could still help with later:

- locality-aware neighborhood scans,
- bounded rebuild packaging,
- bounded dirty-region publication,
- bounded delta overlays.

What this batch shows more strongly:

```text
The first-order missing pieces are property plane, artifact plane, and runtime
workspace discipline, not cell packaging.
```

## Skeptical Review

| concern | why it matters | current answer | remaining risk |
| --- | --- | --- | --- |
| “You still have not proven all GDS families fit.” | The goal is full compatibility, not a curated subset. | Correct. This batch only hardens the difficult-family cluster. | Full-family diligence is still incomplete until more ML, path, and operations families are similarly traced. |
| “Louvain and Leiden may still force a special storage layer.” | Contracted graphs can grow large and be rebuilt many times. | True, but the source points to `runtime-created graphs`, not a user-visible durable layout family. | A future implementation spike may prove that contracted-graph spill needs a named subsystem. |
| “KNN is barely a graph algorithm here.” | If true, topology-centric planning will under-design the property plane. | That is exactly the point this batch surfaces. | Full GDS support requires resisting the temptation to design only for edge traversal. |
| “PipelineCatalog and OpenModelCatalog are in-memory maps in openGDS.” | v003 still needs to decide what becomes durable. | Yes. The batch proves the semantic surface exists, not the final durability policy. | A separate artifact-lifecycle batch may still be needed if the PRD hardens full model durability. |
| “Cells might still be needed for bounded RAM.” | The user has repeatedly asked this. | Possible, but these hard families still do not make cells the first-order architectural answer. | Cells could still become attractive after measured locality and publication data exists. |

## Requirement Impact

| requirement | effect of this batch |
| --- | --- |
| `REQ-LEARN-008.0` | strengthened: memory-estimate semantics now include Louvain, Leiden, TriangleCount, Node2Vec, KNN, FilteredKnn, and link-prediction training composition. |
| `REQ-LEARN-009.0` | strengthened: harder family state shapes are now source-backed instead of inferred from names. |
| `REQ-LEARN-010.0` | strengthened substantially: mutate, write, pipeline, and model semantics now have direct source-backed evidence. |
| `REQ-LEARN-015.0` | strengthened: algorithm baselines now cover families that actively test the storage thesis. |
| `REQ-LEARN-026.0` | strengthened: GAPBS, LAGraph, SNAP, and GraphBLAS rows now give more concrete oracle and parity leads. |
| `REQ-LEARN-044.0` | strengthened: additional public procedures are now traced to kernel or runtime classes. |
| `REQ-LEARN-045.0` | strengthened: storage implications are now derived from Louvain, Leiden, TriangleCount, Node2Vec, KNN, FilteredKnn, and pipeline/model behavior. |
| `REQ-LEARN-046.0` | strengthened: estimator semantics are captured for hard families instead of only representative easy ones. |
| `REQ-LEARN-047.0` | strengthened but not complete: full-family feasibility still needs more families and final support-tier mapping. |
| `REQ-LEARN-048.0` | strengthened: oracle ideas now exist for triangle, KNN-result, and node2vec family parity, but the full harness plan is still not emitted as code. |

## Verification Log

Commands run for this batch included:

```bash
/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-gds-src
/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-gds-src
/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/gapbs-src
/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/gapbs-src
/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/graphblas-src
/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/graphblas-src
/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/lagraph-src
/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/lagraph-src
/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/snap-src
/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/snap-src
```

Representative follow-up probes:

```bash
CBM_CACHE_DIR=/tmp/codex-code-intel/codebase-memory/neo4j-gds-src-20260624-151217/cache \
  /Users/amuldotexe/.codex/tooling/code-intelligence/bin/codebase-memory-mcp \
  cli search_graph '{"project":"Users-amuldotexe-Desktop-personal-repos-lane-knight-bus-graph-walker-gitrefrepo-neo4j-gds-src","label":"Class","name_pattern":".*(Louvain|Leiden|TriangleCount|Node2Vec|FilteredKnn|Knn|LinkPredictionPipeline|OpenModelCatalog|PipelineCatalog).*"}'

HOME=/tmp/codex-code-intel/codegraphcontext/gapbs-src-20260624-151337/home \
  /Users/amuldotexe/.codex/tooling/code-intelligence/.venvs/codegraphcontext/bin/cgc \
  --database ladybugdb \
  --path /tmp/codex-code-intel/codegraphcontext/gapbs-src-20260624-151337/ladybugdb.sqlite \
  query "MATCH (f:Function) RETURN f.name LIMIT 10"
```

## Checkpoint Summary

This batch materially improves the implementation usefulness of the learning
program.

What is now clearer than before:

- full GDS pressure is not mainly “which graph layout should we persist?”
- it is “which families require topology, property columns, runtime workspaces,
  result artifacts, model artifacts, and estimate composition?”

What still remains open:

- the benchmark and observability lane;
- broader family coverage for the still-untraced GDS tails;
- a final support-tier mapping from `public ABI row` to
  `exact low-RAM`, `implemented later`, or `registered unsupported`.
