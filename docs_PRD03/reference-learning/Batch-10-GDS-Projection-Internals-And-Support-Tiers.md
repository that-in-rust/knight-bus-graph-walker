# Batch 10: GDS Projection Internals, Long-Tail Support Tiers, And Estimator Fit

Date: 2026-06-24

Assigned lanes:

- `Capability lane`
- `Architecture lane`
- `Memory-contract lane`
- `Feasibility lane`

Assigned PRD outcomes:

- `Projection and graph-store mechanics mapped to v003 artifacts`
- `Full-surface GDS family support-tier classification`
- `Estimator semantics translated into v003 memory formulas`
- `Stronger answer to "can all of GDS fit the CSR-centered architecture?"`

Requirement IDs touched in this batch:

- `REQ-LEARN-007.0`
- `REQ-LEARN-008.0`
- `REQ-LEARN-009.0`
- `REQ-LEARN-015.0`
- `REQ-LEARN-044.0`
- `REQ-LEARN-045.0`
- `REQ-LEARN-046.0`
- `REQ-LEARN-047.0`

## Answer First

This batch closes the largest remaining architecture gap from the earlier
reference-learning passes: we had the public GDS surface inventory, but we had
not yet turned that inventory into a credible answer for projection internals,
memory-estimator mechanics, and long-tail support tiers.

The strongest conclusions are:

1. Neo4j GDS does not treat a projected graph as "just CSR". It treats it as a
   cataloged graph store with:
   - user/database/name identity,
   - projection config,
   - dense id map,
   - orientation rules,
   - label/type/property schema,
   - memory estimate tree,
   - and graph-memory reporting hooks.
2. That does not weaken the v003 canonical-topology thesis. It sharpens it.
   The first durable topology can still be one canonical adjacency substrate,
   but it must sit beside:
   - a projection catalog,
   - a typed property plane,
   - a result/model artifact plane,
   - and runtime scratch / spill policies.
3. The GDS memory estimator is compositional, not magical. First-party code
   repeatedly builds estimates from a small vocabulary:
   - `perNode`,
   - `perNodeVector`,
   - `perGraphDimension`,
   - `perThread`,
   - and `max(...)` high-water-mark composition.
   That is directly portable into a Rust `estimate` discipline.
4. The full visible local GDS inventory is big enough that "full GDS support"
   must be tiered. The current inventory TSV exposes `575` visible rows, and
   all `575` are still marked `NeedsArchitectureSpike` in the machine-readable
   baseline. That is honest as a baseline, but no longer sufficient as the
   architecture answer.
5. From first principles, all major GDS family groups can fit a
   `canonical-topology + sidecars + artifacts + runtime-workspace` architecture
   better than they fit a "many durable per-algorithm graph formats"
   architecture.
6. Flat CSR alone is not enough. But the evidence still does not force
   `Tilehouse` as the only next step. What it forces is a broader substrate:
   canonical adjacency plus catalog, property plane, artifact plane, and
   bounded runtime workspaces. Cells remain an optional packaging/locality tool,
   not the first-order answer demanded by the long-tail GDS surface.

Short architecture thesis after this batch:

```text
The real GDS requirement is not "many stored graph layouts."
It is one canonical graph topology plus the systems around it:
projection catalog, typed property plane, memory-estimate discipline,
result/model artifacts, and strict runtime state budgeting.

That broader substrate can support the full GDS surface far better than
flat CSR alone, and it does so without conceding that each family needs its
own persistent layout.
```

## Scope

This batch focuses on the still-open first-party questions:

- `neo4j-gds-src` graph-store and projection mechanics
- first-party estimator composition and execution-path wiring
- long-tail family support tiers across the visible GDS surface
- benchmark-oracle support from:
  - `gapbs-src`
  - `snap-src`
  - `lagraph-src`
  - `graphblas-src`
- workflow/call-shape support from:
  - `neo4j-gds-client-src`
  - `graph-data-science-src`
  - `gds-agent-src`

This batch does not create implementation code. It upgrades the evidence base
so later implementation can honestly claim which surface areas are:

- `P0-RegisteredCompatible`
- `P1-ImplementedExactLowRam`
- `P2-ImplementedLater`
- `UnsupportedButRegistered`
- or still `NeedsArchitectureSpike`

## Graph-Tool Execution For This Batch

This batch explicitly uses the two local graph-evidence skills required by the
learning spec:

- `/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/SKILL.md`
- `/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/SKILL.md`

The shelf-wide truthcheck remains the control artifact for the full spec-named
repo set:

- `Reference-Shelf-Graph-Evidence-Ledger.md`
- `Reference-Shelf-Graph-Tool-Truthcheck.tsv`

That control ledger already covers the full current `71`-repo learning-spec
scope across the folders and sub-repos named by the spec. This batch therefore
uses:

- shelf-wide graph-tool truthcheck as the full-scope control plane; and
- fresh targeted reruns plus direct file reads for the repo families that most
  strongly affect `REQ-LEARN-007/008/009/015/044/045/046/047`.

| repo | batch-time graph-tool stance | evidence used here | why it mattered |
| --- | --- | --- | --- |
| `neo4j-gds-src` | fresh `CBM` query-ready, fresh `CGC` low-yield | fresh wrapper outputs plus direct source reads | first-party graph-store, estimator, executor, and algorithm-family truth |
| `gapbs-src` | fresh dual-tool signal | fresh CGC stats and function search plus direct reads | baseline oracle and state-shape support for path and centrality families |
| `graph-data-science-src` | fresh `CBM` ready, fresh `CGC` warning-low-trust | fresh wrapper outputs plus direct guide reads | user-visible projection and estimate workflow hints |
| `neo4j-gds-client-src` | shelf-truthcheck plus direct reads | direct client call-builder reads | user-workflow compatibility for graph projection and `.estimate` usage |
| `gds-agent-src` | shelf-truthcheck plus direct reads | direct MCP tool-definition reads | modern workflow edges around projection semantics and remote/session differences |
| `snap-src` | shelf truthcheck plus direct reads | direct source reads | second-lineage support for HITS, k-core, random walk, similarity, and node2vec shapes |
| `lagraph-src` | shelf truthcheck plus direct reads | direct source reads | GraphBLAS-family oracle and execution-shape support |
| `graphblas-src` | shelf truthcheck plus direct reads | direct source reads | sparse-linear-algebra execution alternative, not storage mandate |

Important truth rule:

```text
Repo-root graph-tool runs count as the graph-evidence pass for named subfolders.
Folder-specific claims still need direct `rg` plus source reads inside the
named paths. This batch follows that rule.
```

## Evidence Ledger

| claim_id | source_path | symbol_or_query | sourced_fact | inference | speculation | PRD impact | skeptical note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `B10-001` | `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java:50-125`, `...:187-218`, `...:249-267` | `GraphStoreCatalog.get`, `set`, `setDegreeDistribution` | GDS graph stores live in a static user-scoped catalog, are keyed by database plus graph name, can fail on ambiguous cross-user matches, emit `GraphStoreAddedEvent` with `MemoryUsage.sizeOf(graphStore)`, and can persist degree distribution metadata in the catalog. | A projected graph in GDS is not just bytes on disk or in memory; it is a named catalog object with metadata and memory reporting hooks. | v003 will likely need a snapshot/projection catalog layer even if the durable topology remains simple. | Strongly strengthens the need for a projection catalog and generation metadata plane. | The current GDS catalog is in-process and static; v003 still has to decide what becomes durable versus ephemeral. |
| `B10-002` | `gitrefrepo/neo4j-gds-src/config-api/src/main/java/org/neo4j/gds/config/GraphProjectConfig.java:33-107` | `GraphProjectConfig` | Graph projection config includes `graphName`, `readConcurrency`, optional `nodeCount`, optional `relationshipCount`, and `validateRelationships`; it also has `isFictitiousLoading()` when counts are supplied directly. | GDS projection semantics already distinguish real loading from estimate-only/fictitious loading. | v003 can adopt a similar split for `estimate` paths without forcing full projection construction. | Supports clean separation between estimate, build, and execute phases. | Fictitious loading helps estimates, but it does not reduce the burden of exact runtime accounting later. |
| `B10-003` | `gitrefrepo/neo4j-gds-src/graph-projection-api/src/main/java/org/neo4j/gds/NodeProjection.java:33-118`, `gitrefrepo/neo4j-gds-src/graph-projection-api/src/main/java/org/neo4j/gds/RelationshipProjection.java:37-169` | `NodeProjection`, `RelationshipProjection` | Node projections encode label plus property mappings; relationship projections encode type, orientation, aggregation, inverse index, and property mappings; undirected projections cannot also be inverse indexed; some aggregations require property mappings. | Projection semantics already exceed raw adjacency selection. Orientation, aggregation, and property-schema rules are first-class. | v003 will likely need logical-projection descriptors even if physical topology stays canonical. | Strengthens property-plane and projection-catalog requirements. | These rules prove schema richness, not yet the best physical encoding for every projection. |
| `B10-004` | `gitrefrepo/neo4j-gds-src/native-projection/src/main/java/org/neo4j/gds/projection/GraphDimensionsReader.java:62-130`, `...:168-177` | `GraphDimensionsReader.apply`, `loadPropertyTokens` | Native projection dimensions include node counts, highest possible node count, relationship upper bounds, label/type token mappings, and property tokens loaded from projection mappings. The code itself notes a TODO that relationship counting can double-count across distinct labels. | GDS estimates are dimension-driven approximations, not exact future RSS predictions. | v003 should expose estimate assumptions and approximation limits, not present estimates as perfect truth. | Helps shape an honest `estimate` contract with explicit approximation notes. | If v003 hides estimate assumptions, it may repeat the same ambiguity while claiming better RAM honesty. |
| `B10-005` | `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/api/GraphStoreFactory.java:27-76`, `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/api/CSRGraphStoreFactory.java:103-265` | `GraphStoreFactory`, `CSRGraphStoreFactory.getMemoryEstimation` | First-party GDS graph-store factories expose `build()`, `estimateMemoryUsageDuringLoading()`, and `estimateMemoryUsageAfterLoading()`. The CSR graph-store estimate composes node id map, node properties, relationship loading buffers, per-node offsets, degrees, relationship-property arrays, adjacency lists, adjacency-property storage, and optional inverse indexes. | The canonical graph-store estimate is already a compositional memory tree built from a small reusable vocabulary. | v003 can mirror this estimator style directly in Rust without copying Java structure one-for-one. | Strong first-party support for a compositional memory-estimation engine. | Estimator composition alone does not prove the chosen physical storage is minimal; it only proves what GDS currently accounts for. |
| `B10-006` | `gitrefrepo/neo4j-gds-src/triplet-graph-builder/src/main/java/org/neo4j/gds/projection/GraphImporter.java:81-86`, `...:112-160`, `...:163-218`, `...:255-283` | `graphImporterTask`, `update`, `result`, `newRelImporter` | GDS graph import is explicitly framed as `Graph aggregation`, then `Build graph store`. During import it loads nodes, attaches node and relationship properties, chooses NATURAL or UNDIRECTED orientation, optionally builds inverse indexes, validates requested rel-type semantics, and finally inserts the graph into `GraphStoreCatalog`. | Projection build is a real build/control workflow, not a direct serving path. | v003's Projection Build Store can legitimately own aggregation, dense-id remapping, projection validation, and publication staging. | Very strong support for Build Store as build/control plane, not user-serving store. | The first-party importer is in-memory oriented; v003 still has to adapt the workflow to published immutable snapshots. |
| `B10-007` | `gitrefrepo/neo4j-gds-src/executor/src/main/java/org/neo4j/gds/executor/ProcedureExecutor.java:69-130`, `...:166-201` | `ProcedureExecutor.compute`, `newAlgorithm` | Procedure execution preprocesses config, creates graph projection, validates memory estimation before loading, builds graph store and result store, validates graph/config compatibility, creates either a `Graph` or `GraphStore` algorithm, attaches estimated resource footprint to the progress tracker, and then executes. | GDS treats `estimate`, graph creation, algorithm factory selection, and execution as distinct phases wired together by the executor. | v003 should mirror this phase separation in its public procedure flow, even if the internal types differ. | Strong support for a phase-scoped execution contract and measured-vs-estimated memory reporting. | Executor shape tells us call flow, not which algorithms are affordable under the target RAM budget. |
| `B10-008` | `gitrefrepo/neo4j-gds-src/memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimations.java:225-575` | `Builder.perNodeVector`, `perNode`, `rangePerNode`, `perGraphDimension`, `perThread`, `max` | First-party GDS memory estimation is built from a compact set of compositional primitives that scale with node count, graph dimensions, vector size, concurrency, and high-water-mark alternatives. | Most GDS memory-estimate behavior can be translated into a reusable planner DSL rather than hard-coded per procedure. | v003 can expose a tree-like `estimate` result while internally using Rust closures or enum builders. | Strongly strengthens `REQ-LEARN-008.0` and `REQ-LEARN-046.0`. | A neat estimator DSL still needs honest handling of omitted classes like page cache, spill files, and OS-level residency. |
| `B10-009` | `gitrefrepo/neo4j-gds-src/proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphMemoryUsageProc.java:33-40`, `gitrefrepo/neo4j-gds-src/applications/graph-store-catalog/src/main/java/org/neo4j/gds/applications/graphstorecatalog/NativeProjectApplication.java:70-106` | `gds.internal.graph.sizeOf`, `estimate`, `estimateButFictitiously` | GDS exposes an internal graph size-of procedure via the graph catalog and has a projection-estimate path that can either use real dimensions or fictitious loading dimensions. | Memory sizing is part of the graph-catalog surface, not an internal afterthought. | v003 may eventually expose projection-size introspection and estimate-only commands as public diagnostics. | Strengthens the memory-contract surface and projection-catalog story. | Internal procedures do not automatically become user-facing compatibility requirements. |
| `B10-010` | `gitrefrepo/neo4j-gds-client-src/src/graphdatascience/query_runner/cypher_graph_constructor.py:202-227`, `gitrefrepo/gds-agent-src/mcp_server/src/mcp_server_neo4j_gds/graph_projection_specs.py:3-63`, `gitrefrepo/graph-data-science-src/documentation/graph-data-science.neo4j-browser-guide:707-980` | `gds.graph.project`, `gds.graph.project.remote`, `.estimate`, browser guide examples | Client and workflow layers assume named projected graphs, `.estimate` variants, standard versus Cypher projection modes, undirected relationship handling, graph catalog listing, and in-memory projected-graph lifecycle. | The projection/catalog workflow is part of what users experience as "GDS", not just the algorithm kernel. | v003 must preserve catalog-and-estimate ergonomics if it aims for deep GDS compatibility. | Strengthens `GDS as ABI`, not just `GDS as algorithm set`. | The workflow layers expose public expectations, but they do not fix the internal storage shape. |
| `B10-011` | `docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv`, local count query run on 2026-06-24 | family count and support-status count | The local inventory currently has `575` visible rows across `catalog`, `centrality`, `community`, `path-finding`, `similarity`, `embeddings`, `machine-learning`, `misc`, `pipeline-catalog`, `sysinfo`, `common`, and `test`, and all `575` rows still carry `NeedsArchitectureSpike` in the TSV baseline. | The row inventory exists, but it has not yet been transformed into architecture-ready support tiers at row level. | A later machine-readable refinement pass could annotate the TSV family-by-family without changing the original inventory role. | Justifies this batch's family-level support-tier matrix. | Family-level classification is still coarser than fully annotating all `575` rows individually. |
| `B10-012` | `gitrefrepo/neo4j-gds-src/algo/src/main/java/org/neo4j/gds/hits/HitsMemoryEstimateDefinition.java:29-41`, `.../betweenness/BetweennessCentralityMemoryEstimateDefinition.java:36-90`, `.../paths/bellmanford/BellmanFordMemoryEstimateDefinition.java:28-49`, `.../paths/delta/DeltaSteppingMemoryEstimateDefinition.java:30-63`, `.../traversal/RandomWalkMemoryEstimateDefinition.java:28-46`, `.../kcore/KCoreDecompositionMemoryEstimateDefinition.java:29-52`, `.../kmeans/KmeansMemoryEstimateDefinition.java:31-71`, `.../scaleproperties/ScalePropertiesMemoryEstimateDefinition.java:31-61`, `.../scc/SccMemoryEstimateDefinition.java:31-54` | long-tail memory estimate definitions | First-party long-tail families already expose diverse state shapes: dual per-node vectors for HITS, predecessor/sigma/delta stacks for betweenness, bucketed distance state for Delta-Stepping, walk buffers for RandomWalk, degree/core arrays for k-core, dense centroid matrices for KMeans, property-column transforms for ScaleProperties, and stack/index/visited state for SCC. | The long tail does not force many durable stored graph layouts; it forces diverse runtime-state and sidecar plans. | Some families may still need strict rejection or spill modes on 8GB-class machines. | Strongly supports canonical topology plus runtime state diversity. | Some families remain expensive enough that architectural fit does not imply practical first-release support. |
| `B10-013` | `gitrefrepo/gapbs-src/src/bc.cc:22-30`, `gitrefrepo/gapbs-src/src/sssp.cc:166-171`, `gitrefrepo/lagraph-src/src/algorithm/LAGr_Betweenness.c`, `gitrefrepo/lagraph-src/src/algorithm/LAGr_TriangleCount.c`, `gitrefrepo/snap-src/snap-core/centr.h`, `gitrefrepo/snap-src/snap-core/kcore.h`, `gitrefrepo/snap-src/examples/randwalk`, `gitrefrepo/graphblas-src/Doc/UserGuide/GrB_release.tex` | baseline oracle and algorithm-shape support | The external algorithm baselines offer concrete oracle or execution-shape precedents for betweenness, triangle count, shortest path, k-core, random walk, HITS, and GraphBLAS-family batch analytics. | These repos are good for oracle and state-shape support, not for defining public Neo4j/GDS semantics. | Batch 11 can turn these leads into a parity-matrix artifact without reopening the architecture question. | Strengthens `REQ-LEARN-015.0` while teeing up `REQ-LEARN-048.0`. | Baseline repos still do not substitute for procedure-mode and estimate-shape compatibility. |

## Projection And Graph-Store Mechanics Mapped To v003 Artifacts

| GDS concept | first-party evidence | v003 artifact mapping | why CSR alone is insufficient |
| --- | --- | --- | --- |
| named projected graph | `GraphStoreCatalog.get/set` | projection catalog entry pointing at a published snapshot generation and logical projection spec | raw CSR files do not know user, graph name, or lifecycle |
| dense id map | `CSRGraphStoreFactory` node id map estimate | dense-id map sidecar or generation metadata | topology traversal needs dense ids, but GDS also needs a named mapping contract |
| node projection with properties | `NodeProjection` | label/property selector plus property-plane bindings | adjacency alone does not encode projected node-property semantics |
| relationship projection orientation and aggregation | `RelationshipProjection` | logical orientation rules, aggregation policy, and optional inverse-index policy in projection metadata | canonical topology must support logical views beyond one raw edge direction |
| graph dimensions | `GraphDimensionsReader` | generation stats plus estimate-only dimensions view | estimate flow needs counts and token mappings before building graph data |
| graph aggregation/build step | `GraphImporter.graphImporterTask`, `update`, `result` | Projection Build Store responsibilities: ingest, normalize, validate, stage, publish | build-time aggregation is a workflow, not a serving layout |
| graph store memory size | `GraphMemoryUsageProc`, `GraphStoreCatalog.set` event memory usage | cataloged measured-size metadata and diagnostics | a snapshot file alone does not provide user-facing memory introspection |
| degree distribution metadata | `GraphStoreCatalog.setDegreeDistribution` | cached per-generation stats or sidecar metrics | many operations want graph metadata beyond raw edges |
| fictitious loading estimate | `GraphProjectConfig.isFictitiousLoading`, `estimateButFictitiously` | estimate-only mode using declared or sampled dimensions | exact build and estimate-only flows must stay distinct |

## Estimator Semantics And v003 Formula Skeleton

The first-party GDS estimator language is small enough to translate directly.

### First-party primitive vocabulary

| primitive | source evidence | meaning for v003 |
| --- | --- | --- |
| `perNode` | `MemoryEstimations.Builder.perNode` | a scalar or structure multiplied by `nodeCount` |
| `perNodeVector` | `Builder.perNodeVector` | vector or embedding state scaled by `nodeCount * dimension` |
| `rangePerNode` | `Builder.rangePerNode` | variable per-node state with lower/upper estimate |
| `perGraphDimension` | `Builder.perGraphDimension` | state sized from node count, rel upper bound, label/type counts, or other graph-level dimensions |
| `perThread` | `Builder.perThread` | concurrency-scaled scratch, queues, bins, tasks, or buffers |
| `max(...)` | `Builder.max` | high-water-mark composition where only the biggest concurrent phase counts |

### Projection estimate pseudocode

```text
projection_estimate =
  node_id_map()
  + sum(node_property_column(property) for property in projected_node_properties)
  + sum(
      max(
        relationship_loading_buffers(rel_projection),
        relationship_loaded_form(rel_projection)
      )
      for rel_projection in relationship_projections
    )

relationship_loading_buffers(rel_projection) =
  adjacency_loading_buffer(rel_projection)
  + per_node_offsets(rel_projection)
  + per_node_degrees(rel_projection)
  + sum(per_node_relationship_property_array(p) for p in rel_projection.properties)
  + optional_inverse_index_copy(rel_projection)

relationship_loaded_form(rel_projection) =
  adjacency_list(rel_projection)
  + sum(adjacency_property_storage(p) for p in rel_projection.properties)
  + optional_inverse_index_copy(rel_projection)
```

### Algorithm estimate pseudocode

```text
algorithm_estimate =
  algorithm_factory_estimate(graph_dimensions, concurrency)
  + result_sidecar_estimate(mode, result_shape)
  + optional_model_or_pipeline_estimate(config)
  + optional_delta_overlay_exposure(current_snapshot_generation)

algorithm_factory_estimate(...) is composed from:
  per_node(...)
  per_node_vector(...)
  per_graph_dimension(...)
  per_thread(...)
  max(...)
```

### v003 honesty rule

The first-party estimator logic is necessary, but not sufficient, for the v003
RAM promise. v003 must add omitted reality classes:

- `page_cache_or_mmap_exposure`
- `direct_io_buffer_bytes`
- `scratch_spill_bytes`
- `artifact_plane_bytes`
- `delta_overlay_bytes`
- `measured_runtime_rss`

This is the key difference between:

```text
GDS-style estimate tree
```

and

```text
Knight Bus v003 strict holistic RAM contract
```

## Long-Tail GDS Family Support-Tier Matrix

This matrix answers the user's practical question: if the architecture aims for
all of GDS, which surfaces fit the canonical-topology thesis cleanly, and which
ones mostly pressure sidecars, artifacts, or runtime scratch?

`Target support class` here means the architecture-fit target, not the current
implementation state of this repository.

| family group | visible local inventory signal | dominant graph access pattern | dominant state shape | dominant non-topology dependency | 50GB-on-8GB risk | target support class | architecture fit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `catalog` | `54` rows | metadata and graph-handle operations | graph names, counts, schema, memory trees, lifecycle metadata | projection catalog and generation catalog | low | `P0-RegisteredCompatible`, then `P1-ImplementedExactLowRam` | requires catalog, not extra graph layouts |
| `centrality` | `82` rows | offsets-only, frontier, or global iterative scans | degrees, distances, per-node vectors, predecessor stacks | weights, orientation, result sidecars | medium to very high depending on algorithm | mixed: easy kernels `P1`, expensive kernels `P2` or strict-gated | canonical topology fits; runtime-state diversity is the issue |
| `community` | `154` rows | frontier, neighborhood intersection, iterative label/community updates, contracted graph phases | community ids, queues, intersection buffers, aggregated graph workspaces | undirected views, result sidecars, workspace graphs | medium to very high | mixed: WCC/SCC/k-core `P1`, Louvain/Leiden/SLPA `P2` | canonical topology plus runtime workspace beats durable per-family layouts |
| `path-finding` | `92` rows | frontier, priority queue, repeated global scan, random walk | visited/distance arrays, priority queues, bins, walk buffers | weights, optional predecessor sidecars | low to very high | mixed: BFS/DFS/SSSP `P1`, all-pairs and heavy variants `P2` or strict-gated | topology fits; scratch/spill controls dominate |
| `similarity` | `44` rows | property-vector scans and candidate generation | topK heaps, candidate lists, filtered subsets | typed property plane and relationship-result artifacts | high to very high | `P2-ImplementedLater` for exact large-scale modes; some may begin as `UnsupportedButRegistered` | property plane is mandatory; topology is secondary |
| `embeddings` | `36` rows | random walk, propagation, neighborhood sampling, matrix-like feature transforms | embedding matrices, walk corpora, model weights, batches | artifact plane, model plane, typed property plane | very high | `P2-ImplementedLater` with strict gates; some training-heavy paths may start `UnsupportedButRegistered` | canonical topology is enough as substrate; artifact/workspace demands dominate |
| `machine-learning` | `46` rows | feature transforms, train/predict pipelines, model orchestration | model candidates, train/test splits, prediction buffers | model catalog, pipeline catalog, artifact plane | very high | `P0-RegisteredCompatible`, implementation staged through `P2` | not a topology problem; it is an artifact/catalog problem |
| `misc` | `40` rows | transforms, helper ops, property scans | scalar stats, transformed columns, lightweight graph rewrites | property plane, result plane | low to medium | many `P1`, some `P2` | mostly sidecar and procedure-surface work |
| `pipeline-catalog` | `6` rows | metadata and lifecycle | pipeline names, configs, model links | durable metadata plane | low | `P0-RegisteredCompatible`, then `P2` | requires catalog semantics, not new graph storage |
| `sysinfo` | `5` rows | reporting only | counters and state summaries | telemetry and memory introspection | low | `P0-RegisteredCompatible`, then `P1` | should ride the memory-contract work |
| `common` and `test` | `16` rows combined | support scaffolding | validation and helper semantics | registry, diagnostics, testing | low | `P0-RegisteredCompatible` | not storage-driving |

## Procedure-To-Kernel And Storage-Need Synthesis

The long tail is varied, but it still collapses into a small number of storage
and runtime needs.

| observed long-tail kernel behavior | example families | v003 need | durable new topology required? |
| --- | --- | --- | --- |
| read offsets or neighbors only | degree, WCC, SCC, k-core | canonical forward/reverse adjacency | no |
| iterate whole graph with per-node vectors | PageRank, HITS, eigenvector-like families | canonical global stream plus vector budgets | no |
| priority-queue or bucketed traversal | Dijkstra, Delta-Stepping, Bellman-Ford variants | bounded scratch, weight sidecars, spill gates | no |
| predecessor/sigma/delta stacks | betweenness, shortest-path variants | high-memory runtime state and concurrency-aware estimator | no |
| triangle or similarity intersections | TriangleCount, similarity overlaps | sorted/intersection-friendly adjacency or candidate lists | no durable new topology; maybe alternate execution kernels |
| property-only or property-first computation | KNN, FilteredKnn, ScaleProperties, KMeans | typed property plane and artifact/result plane | no |
| random walks and training | RandomWalk, Node2Vec, GraphSAGE-like surfaces | walk buffers, RNG determinism, model artifacts, spill/gate policy | no |
| hierarchical graph contraction | Louvain, Leiden | runtime graph-workspace and hierarchical result outputs | no durable new topology; yes runtime contracted graphs |
| pipelines and models | link prediction pipelines, model catalog | metadata plane and artifact plane | no |

## Full-Surface Feasibility Answer

### Can all of GDS fit "CSR architecture"?

If "CSR architecture" means:

```text
one pair of flat adjacency files and nothing else
```

then:

```text
No.
```

If "CSR architecture" means:

```text
canonical graph topology as forward/reverse adjacency
+ projection catalog
+ typed property plane
+ result/model artifact plane
+ bounded runtime scratch and spill
+ honest estimator and memory reporting
```

then:

```text
Yes, much more of the GDS surface fits than a naive reading suggests.
```

### What still does not become easy?

Even under the broader architecture, some families stay hard:

- all-pairs or repeated-source centralities
- large candidate-generation similarity families
- dense embedding and training surfaces
- pipeline/model catalog durability and writeback semantics

These are not arguments for many persistent graph layouts. They are arguments
for:

- stricter support tiers,
- runtime workspace gates,
- spillable execution profiles,
- and deterministic `UnsupportedButRegistered` behavior until a family is ready.

## Architecture Verdict After Batch 10

### What this batch strengthens

1. `GDS as ABI`
   - names, modes, configs, estimates, catalog, and lifecycle matter as much
     as kernels.
2. `Canonical topology plus support planes`
   - graph store semantics extend beyond adjacency, but they still do not force
     many durable per-algorithm formats.
3. `Estimator discipline`
   - a Rust planner can honestly mirror the first-party GDS estimation style
     while adding stricter RAM-accounting fields.
4. `Broader than flat CSR alone`
   - the correct contrast is no longer:
     - `flat CSR`
     - versus `Tilehouse`
   - it is:
     - `topology only`
     - versus `topology + sidecars + artifacts + catalog + workspace`

### What this batch does not prove

1. It does not prove every GDS family is practical at `50GB on 8GB` on day one.
2. It does not prove cells are unnecessary forever.
3. It does not prove GraphBLAS is irrelevant.
4. It does not remove the need for dedicated oracle and parity planning.

### Cells verdict after this batch

The long-tail GDS evidence still does not make cells the primary architectural
necessity.

What cells might later help with:

- bounded dirty-region rebuilds
- locality-aware neighborhood scans
- smaller publication units
- stricter update-local compaction

What the long-tail evidence primarily demands first:

- projection catalog
- typed property plane
- result/model artifact plane
- runtime graph workspaces
- strict estimator and memory telemetry

## Requirement Impact

| requirement | effect of this artifact |
| --- | --- |
| `REQ-LEARN-007.0` | satisfied for this batch scope: first-party graph-store, projection, graph-dimensions, importer, and catalog concepts are now mapped to v003 artifacts. |
| `REQ-LEARN-008.0` | satisfied for this batch scope: first-party estimator mechanics are traced through factory, executor, and builder primitives and translated into v003 pseudocode/formulas. |
| `REQ-LEARN-009.0` | satisfied for this batch scope: the remaining long-tail GDS families are now classified by access pattern, state shape, non-topology dependency, risk, and target support tier. |
| `REQ-LEARN-015.0` | satisfied for this batch scope: external baseline repos are now tied to oracle/state-shape support for the long-tail families, not just representative early kernels. |
| `REQ-LEARN-044.0` | satisfied for this batch scope: long-tail procedure families are now tied to estimate classes, workflow call shapes, and kernel/state evidence rather than only to procedure names. |
| `REQ-LEARN-045.0` | satisfied for this batch scope: storage needs are now derived from observed kernel behavior across the long-tail family groups. |
| `REQ-LEARN-046.0` | satisfied for this batch scope: algorithm-feasibility discussion now names estimator source patterns, dimensions, concurrency, and omitted memory classes. |
| `REQ-LEARN-047.0` | satisfied for this batch scope: every major family group now has a target support class and `50GB-on-8GB` risk characterization. |

## Verification Log

Commands used during this batch included:

```bash
python3 - <<'PY'
import csv, collections, pathlib
path = pathlib.Path('docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv')
rows = list(csv.DictReader(path.open(), delimiter='\t'))
print(len(rows))
print(collections.Counter(r['family'] for r in rows))
print(collections.Counter(r['support_status'] for r in rows))
PY

nl -ba gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/api/CSRGraphStoreFactory.java | sed -n '80,280p'
nl -ba gitrefrepo/neo4j-gds-src/triplet-graph-builder/src/main/java/org/neo4j/gds/projection/GraphImporter.java | sed -n '60,300p'
nl -ba gitrefrepo/neo4j-gds-src/executor/src/main/java/org/neo4j/gds/executor/ProcedureExecutor.java | sed -n '60,220p'
nl -ba gitrefrepo/neo4j-gds-src/memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimations.java | sed -n '220,620p'
nl -ba gitrefrepo/neo4j-gds-src/config-api/src/main/java/org/neo4j/gds/config/GraphProjectConfig.java | sed -n '30,140p'
nl -ba gitrefrepo/neo4j-gds-src/native-projection/src/main/java/org/neo4j/gds/projection/GraphDimensionsReader.java | sed -n '60,190p'
nl -ba gitrefrepo/neo4j-gds-src/graph-projection-api/src/main/java/org/neo4j/gds/NodeProjection.java | sed -n '30,120p'
nl -ba gitrefrepo/neo4j-gds-src/graph-projection-api/src/main/java/org/neo4j/gds/RelationshipProjection.java | sed -n '30,180p'
nl -ba gitrefrepo/neo4j-gds-client-src/src/graphdatascience/query_runner/cypher_graph_constructor.py | sed -n '190,235p'
nl -ba gitrefrepo/gds-agent-src/mcp_server/src/mcp_server_neo4j_gds/graph_projection_specs.py | sed -n '1,90p'
rg -n "gds\\.(hits|betweenness|closeness|bellmanFord|allShortestPaths\\.delta|randomWalk|scc|kcore|kmeans|scaleProperties)" gitrefrepo/neo4j-gds-src
```

## Checkpoint Summary

Assigned requirement IDs:

- `REQ-LEARN-007.0`
- `REQ-LEARN-008.0`
- `REQ-LEARN-009.0`
- `REQ-LEARN-015.0`
- `REQ-LEARN-044.0`
- `REQ-LEARN-045.0`
- `REQ-LEARN-046.0`
- `REQ-LEARN-047.0`

Most important sourced facts:

- `GraphStoreCatalog` proves projected graphs are catalog objects with memory
  metadata, not just adjacency bytes.
- `CSRGraphStoreFactory` proves projection memory estimation is compositional
  and already CSR-aware.
- `GraphImporter` proves projection build is a real aggregation/build workflow
  and therefore belongs in the Build Store/control plane.
- `ProcedureExecutor` proves estimate, graph creation, algorithm factory
  selection, and execution are separate public phases.
- The long-tail memory estimate definitions prove that most remaining GDS pain
  is runtime-state diversity, not durable topology diversity.

Architecture implications:

- `Adopt`:
  - canonical topology plus support planes
  - projection catalog
  - property plane
  - artifact/model plane
  - estimator DSL and measured-vs-estimated RAM split
- `Adapt`:
  - first-party in-memory graph-store lifecycle into published snapshot
    generations
- `Reject`:
  - many durable per-algorithm graph layouts as the default answer
- `Watch`:
  - cells for locality/update packaging
  - GraphBLAS for selected execution kernels
- `MissingEvidence`:
  - explicit oracle and parity matrix for the remaining family implementations

Unresolved risks:

- The family-level support tiers are stronger than the row-level TSV today.
  A later machine-readable refinement pass may still be useful.
- `50GB-on-8GB` remains a family-by-family practical question for the most
  state-heavy algorithms even though the architecture fit is now clearer.
- Batch 11 still needs to convert the oracle leads into an execution-ready
  parity plan.
