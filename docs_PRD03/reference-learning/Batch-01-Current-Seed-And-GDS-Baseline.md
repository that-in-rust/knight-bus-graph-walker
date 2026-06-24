# Batch 01: Current Seed And GDS Baseline

Date: 2026-06-24

Assigned lanes:

- `Surface lane`
- `Capability lane`
- `Architecture lane`

Assigned PRD outcomes:

- `Neo4j-shaped OLTP`
- `Published OLAP snapshots`
- `Complete GDS surface`
- `Strict holistic RAM`
- `Atomic publication`

Requirement IDs touched in this batch:

- `REQ-LEARN-001.0`
- `REQ-LEARN-006.0`
- `REQ-LEARN-007.0`
- `REQ-LEARN-008.0`
- `REQ-LEARN-030.0`
- `REQ-LEARN-031.0`
- `REQ-LEARN-032.0`
- `REQ-LEARN-034.0`
- `REQ-LEARN-035.0`
- `REQ-LEARN-036.0`
- `REQ-LEARN-037.0`
- `REQ-LEARN-038.0`
- `REQ-LEARN-040.0`
- `REQ-LEARN-041.0`
- `REQ-LEARN-042.0`
- `REQ-LEARN-043.0`
- `REQ-LEARN-044.0`
- `REQ-LEARN-045.0`
- `REQ-LEARN-046.0`
- `REQ-LEARN-049.0`
- `REQ-LEARN-050.0`
- `REQ-LEARN-051.0`
- `REQ-LEARN-052.0`

Batch status:

- This batch implements the spec by producing real study artifacts.
- This batch does not claim full GDS sufficiency.
- This batch is intentionally decision-first: current seed, OLTP boundary, GDS
  surface breadth, one fully traced representative algorithm, then option
  scoring.

## Clone Coverage Ledger

| local_repo | exists_now | upstream_hint | branch_or_head | study_role | required_or_optional | current_use | note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `.` | yes | current Knight Bus workspace | `ideation_20260525` | current seed | required | active study | source of truth for current CSR runtime, tests, and README claims |
| `gitrefrepo/neo4j-src` | yes | `neo4j/neo4j` | `release/5.26.0 @ c68156edf24` | compatibility oracle | required | active study | used for OLTP record-store and traversal-cursor boundary evidence |
| `gitrefrepo/neo4j-gds-src` | yes | `neo4j/graph-data-science` | `2.13 @ dc4417b3c1` | compatibility oracle | required | active study | used for GDS surface, catalog, estimator, and PageRank kernel evidence |
| `gitrefrepo/neo4j-docs-bolt-src` | yes | `neo4j/docs-bolt` | `1714723` | protocol oracle | required later | availability validated | not deeply read in this batch, but present for the next compatibility pass |

## Evidence Ledger

| claim_id | req_id | source_type | source_path | symbol_or_query | sourced_fact | inference | speculation | PRD impact | skeptical note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `CLAIM-B01-001` | `REQ-LEARN-032.0` | source | `README.md:13-17` | `v002 shape bullets` | Knight Bus `v002` explicitly keeps the runtime in an immutable dual-CSR plus `mmap` shape. | The current flat CSR plus `mmap` walker is the baseline OLAP primitive, not dead code. | Sidecars or cells may still be added later. | Published OLAP snapshots | Falsifier: a newer source path that abandons dual CSR or `mmap` as the default runtime shape. |
| `CLAIM-B01-002` | `REQ-LEARN-032.0` | source | `README.md:29-41` | `runtime RAM and latency claims` | The README reports lower runtime RSS than Neo4j on `1 MB`, `50 MB`, and `2 GB`, while noting Neo4j opens faster on the `2 GB` run. | Current moat is strongest on the steady-state walk path, not cold-open latency. | Larger graphs may narrow the RSS gap once algorithm state dominates. | Strict holistic RAM | Falsifier: rerun of the same benchmark corpus that reverses the RSS or latency ordering. |
| `CLAIM-B01-003` | `REQ-LEARN-032.0` | source | `src/snapshot.rs:23-29` | `SnapshotArtifactWriter` | Snapshot writing is abstracted behind `SnapshotArtifactWriter::write_snapshot_artifacts(...)`. | Future snapshot emitters can be additive instead of replacing the existing writer immediately. | A later architecture may add more writer traits around this one. | Published OLAP snapshots | Falsifier: future code removes this abstraction and hardcodes one storage writer. |
| `CLAIM-B01-004` | `REQ-LEARN-032.0` | source | `src/snapshot.rs:104-120` | `build_snapshot_manifest` | The current manifest is version `2` and sets `storage_mode` to `immutable_dual_csr`. | v003 should preserve v2 readability or intentionally version-bump with a migration story. | A generation catalog may later wrap the v2 manifest rather than replacing it. | Atomic publication | Falsifier: current runtime already ignores version or storage mode. |
| `CLAIM-B01-005` | `REQ-LEARN-032.0` | source | `src/runtime.rs:22-39` | `WalkQueryRuntime` | The current public runtime contract is walk-focused: neighbor query, family query, list all keys, and snapshot size. | Current Knight Bus is not yet a GDS surface; a separate GDS/catalog layer is still required. | A future graph-adjacency trait may sit under both walk and GDS runtimes. | Complete GDS surface | Falsifier: existing public API already exposes graph catalog, estimate, mutate, or write semantics. |
| `CLAIM-B01-006` | `REQ-LEARN-032.0` | source | `src/runtime.rs:41-77` | `MmapWalkRuntime` | The runtime maps forward/reverse offsets, peers, node table, strings, and key index with `memmap2::Mmap`. | Current RAM behavior is low explicit heap but OS-mediated page-cache residency. | Strict-RAM global algorithms may need a different execution profile than interactive walks. | Strict holistic RAM | Falsifier: runtime already uses explicit streaming or direct I/O instead of `mmap`. |
| `CLAIM-B01-007` | `REQ-LEARN-032.0` | source | `src/types.rs:147-184` | `QueryFamily` | The current query surface is limited to four traversal families: forward/backward one-hop and two-hop. | Existing correctness tests cover a narrow traversal ABI, not the full Neo4j/GDS surface. | Future compatibility layers may keep this path as an internal smoke oracle. | Complete GDS surface | Falsifier: another public source path exposes a broader query family surface today. |
| `CLAIM-B01-008` | `REQ-LEARN-032.0` | source | `src/types.rs:320-335` | `SnapshotManifest` | The manifest tracks node count, edge count, widths, key mode, storage mode, and file names, but not snapshot generation or watermark. | Generation catalog and watermark metadata are missing from the current open path. | A catalog file may wrap, not bloat, the manifest. | Atomic publication | Falsifier: another current file already carries generation, watermark, and active-pointer semantics. |
| `CLAIM-B01-009` | `REQ-LEARN-032.0` | source | `src/types.rs:419-457` | `BuildMemoryBudget` | Current build memory budgeting already exposes minimum bytes, default bytes, and spill buffer sizing. | v003 can extend an existing budgeting concept rather than inventing one from scratch. | Algorithm execution budgets can likely mirror the build budget shape. | Strict holistic RAM | Falsifier: build path does not actually honor or surface this budget in practice. |
| `CLAIM-B01-010` | `REQ-LEARN-032.0` | source | `tests/library_contract.rs:12-53` | `build_query_and_verify_round_trip_now` | The test suite proves snapshot build, query, and verify round-trip correctness on a fixed fixture corpus. | Current flat CSR is already a useful parity oracle for future storage experiments. | Additional GDS or catalog layers can re-use the same fixture discipline. | Published OLAP snapshots | Falsifier: tests only validate CLI plumbing and not actual query answers. |
| `CLAIM-B01-011` | `REQ-LEARN-032.0` | source | `tests/library_contract.rs:125-141` | `parity_uses_all_expected_families_now` | The test suite asserts each `QueryFamily` has at least one seed in the truth index. | The repo already treats family-level parity as contract surface. | This pattern can grow into algorithm-family parity tests later. | Published OLAP snapshots | Falsifier: parity is only implicit and not actually asserted in tests. |
| `CLAIM-B01-012` | `REQ-LEARN-040.0` | command | `/tmp/codex-code-intel/...` | `scan_current_repo_only.sh` smoke runs | Both local graph-index skills completed repo-only smoke scans and explicitly verified that indexed outputs did not mention `gitrefrepo/`. | The spec's tool-usage rules are feasible in this workspace. | Later study passes can use these tools for Knight Bus orientation without contaminating reference-shelf scope. | Strict holistic RAM | Falsifier: a follow-up scan includes `gitrefrepo/` results or requires committed tool artifacts. |
| `CLAIM-B01-013` | `REQ-LEARN-006.0` | source | `gitrefrepo/neo4j-gds-src/procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/GraphDataScienceProcedures.java:30-43` | `GraphDataScienceProcedures` | The root GDS surface explicitly exposes algorithms, graph catalog, model catalog, operations, pipelines, and deprecated-procedure metrics. | Full Neo4j GDS compatibility is much broader than algorithm kernels alone. | Additional facades may still exist deeper in the tree. | Complete GDS surface | Falsifier: public procedures bypass this facade and expose a materially different surface. |
| `CLAIM-B01-014` | `REQ-LEARN-006.0` | source | `gitrefrepo/neo4j-gds-src/procedures/algorithms-facade-api/src/main/java/org/neo4j/gds/procedures/algorithms/AlgorithmsProcedureFacade.java:34-87` | `AlgorithmsProcedureFacade` | The algorithm surface is split across centrality, community, machine learning, miscellaneous, embeddings, path finding, and similarity facades. | Architecture choice must survive multiple distinct algorithm state shapes, not just PageRank. | Additional deprecated aliases may increase visible surface even further. | Complete GDS surface | Falsifier: most families are dead code or hidden behind enterprise-only flags in this clone. |
| `CLAIM-B01-015` | `REQ-LEARN-006.0` | source | `gitrefrepo/neo4j-gds-src/procedures/graph-catalog-facade-api/src/main/java/org/neo4j/gds/procedures/catalog/GraphCatalogProcedureFacade.java:49-232` | `GraphCatalogProcedureFacade` | The graph catalog surface includes exists/list/drop, native and cypher project, subgraph/filter, sizeOf, property streaming, property writing, relationship streaming/writing, random-walk sampling, CSV/database export, and graph generation. | Topology-only CSR is insufficient; catalog metadata and semantic sidecars are mandatory. | Some procedures may later be intentionally `UnsupportedButRegistered`. | Complete GDS surface | Falsifier: most of these methods are never reachable as public procedures. |
| `CLAIM-B01-016` | `REQ-LEARN-006.0` | command | `gitrefrepo/neo4j-gds-src/proc` | `rg '@Procedure...' count` | The local GDS clone currently exposes `313` `gds.*` procedure annotations under `proc`, with family counts of `community 154`, `path-finding 92`, `centrality 82`, `machine-learning 46`, `similarity 44`, `misc 37`, `embeddings 36`, `catalog 53`, `pipeline-catalog 6`, and `sysinfo 3`. | Any architecture claim based on only one or two algorithms is structurally unsafe. | A generated inventory file should replace these shell counts in a later batch. | Complete GDS surface | Falsifier: the count double-counts deprecated aliases so badly that family breadth is misleading. |
| `CLAIM-B01-017` | `REQ-LEARN-006.0` | source | `gitrefrepo/neo4j-gds-src/proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphProjectProc.java:52-123` | `gds.graph.project*` | Public catalog procedures include `gds.graph.project`, `gds.graph.project.estimate`, deprecated `gds.graph.project.cypher`, and deprecated `gds.beta.graph.project.subgraph`. | Projection and estimate semantics are first-class ABI, not implementation detail. | Future v003 may support native projection before cypher projection, but cannot ignore the deprecated names. | Complete GDS surface | Falsifier: actual client workflows never call these procedures. |
| `CLAIM-B01-018` | `REQ-LEARN-006.0` | source | `gitrefrepo/neo4j-gds-src/proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphListProc.java:47-53` | `gds.graph.list` | `gds.graph.list` is a public procedure with default graph-name handling. | v003 needs named snapshot or projection catalog identity, not anonymous files. | Histogram/schema richness can still be staged later. | Atomic publication | Falsifier: graph listing could be emulated without a durable catalog identity. |
| `CLAIM-B01-019` | `REQ-LEARN-006.0` | source | `gitrefrepo/neo4j-gds-src/proc/catalog/src/main/java/org/neo4j/gds/model/catalog/ModelListProc.java:35-56` | `gds.model.list` | GDS exposes a public model catalog and deprecated aliases. | v003 needs model-artifact metadata if it claims ML or embedding compatibility. | Model storage may still be out of scope for P1 implementation. | Complete GDS surface | Falsifier: model procedures are detached from any supported client workflow. |
| `CLAIM-B01-020` | `REQ-LEARN-006.0` | source | `gitrefrepo/neo4j-gds-src/proc/pipeline-catalog/src/main/java/org/neo4j/gds/pipeline/catalog/PipelineListProc.java:35-58` | `gds.pipeline.list` | GDS exposes public pipeline-catalog procedures and deprecated aliases. | Full-surface compatibility reaches into pipeline metadata, not just algorithm execution. | P0 may register these as deterministic unsupported behavior before full support. | Complete GDS surface | Falsifier: pipelines are enterprise-only and not relevant to this clone. |
| `CLAIM-B01-021` | `REQ-LEARN-006.0` | source | `gitrefrepo/neo4j-gds-src/proc/misc/src/main/java/org/neo4j/gds/ListProgressProc.java:32-53` | `gds.listProgress` | GDS exposes operations/progress procedures outside algorithm families. | v003 needs operational telemetry procedures or explicit unsupported responses. | A minimal first implementation can likely back this from in-process job tracking. | Complete GDS surface | Falsifier: operations surface is optional for clients that matter. |
| `CLAIM-B01-022` | `REQ-LEARN-007.0` | source | `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java:50-125,187-214` | `GraphStoreCatalog` | GDS stores projected graphs in a static user-scoped catalog keyed by database and graph name, with ambiguity handling and memory-usage events on insert. | v003 needs explicit snapshot or projection identity scoped by user, database, and name. | A published-snapshot catalog can be simpler than GDS's in-memory catalog if it still preserves identity and ambiguity semantics. | Atomic publication | Falsifier: GDS clients never rely on user/database/name disambiguation behavior. |
| `CLAIM-B01-023` | `REQ-LEARN-007.0` | source | `gitrefrepo/neo4j-gds-src/core/src/main/java/org/neo4j/gds/core/compression/varlong/CompressedAdjacencyList.java:44-87,101-130` | `CompressedAdjacencyList` | GDS adjacency storage estimates memory for pages, per-node degrees, and per-node offsets, then stores adjacency as `pages`, `degrees`, and `offsets`. | Durable dual CSR plus sidecars is directionally close to the storage shape GDS already optimizes around, even though GDS keeps it in memory. | v003 may optionally add compressed peer storage later if benchmarks justify decode cost. | Strict holistic RAM | Falsifier: the GDS algorithms that matter most bypass this representation entirely. |
| `CLAIM-B01-024` | `REQ-LEARN-044.0` | source | `gitrefrepo/neo4j-gds-src/proc/centrality/src/main/java/org/neo4j/gds/pagerank/PageRankStreamProc.java:37-57` | `PageRankStreamProc` | `gds.pageRank.stream` and `.estimate` are public procedures that route through `facade.algorithms().centrality()`. | Representative algorithm tracing must start at public procedure classes, not internal kernels. | Similar tracing can later be repeated for WCC, Leiden, KNN, and Node2Vec. | Complete GDS surface | Falsifier: the public proc path is a thin alias and hides a materially different implementation route. |
| `CLAIM-B01-025` | `REQ-LEARN-044.0` | source | `gitrefrepo/neo4j-gds-src/procedures/facade-api/centrality-facade-api/src/main/java/org/neo4j/gds/procedures/algorithms/centrality/CentralityProcedureFacade.java:264-285` | `pageRank* methods` | The centrality facade exposes PageRank `mutate`, `stats`, `stream`, `write`, and estimate variants. | Proving only `stream` is insufficient for architecture sufficiency. | The same multi-mode pattern likely repeats across many families. | Complete GDS surface | Falsifier: write/mutate are shallow wrappers with no extra storage implication. |
| `CLAIM-B01-026` | `REQ-LEARN-044.0` | source | `gitrefrepo/neo4j-gds-src/procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/centrality/LocalCentralityProcedureFacade.java:1173-1199,1202-1230` | `pageRankStream/pageRankWrite` | Local procedure facade code parses `PageRankStreamConfig` or `PageRankWriteConfig` and dispatches either to stream-mode or write-mode business facades; estimates use `estimationModeBusinessFacade.pageRank(...)`. | Config parsing, mode routing, and estimate routing are separate concerns that v003 should preserve. | A generic procedure harness may be shared across algorithms later. | Complete GDS surface | Falsifier: deeper business facades erase the mode differences. |
| `CLAIM-B01-027` | `REQ-LEARN-044.0` | source | `gitrefrepo/neo4j-gds-src/applications/algorithms/centrality/src/main/java/org/neo4j/gds/applications/algorithms/centrality/CentralityAlgorithms.java:366-440` | `pageRank(...)` | CentralityAlgorithms builds a `PageRankComputation`, then a `PageRankAlgorithm`, and returns `pageRank.compute()`. | PageRank support depends on graph interface plus Pregel-like execution and per-node state, not just adjacency lookup. | Other centrality algorithms may share machinery but different state shapes. | Strict holistic RAM | Falsifier: a more direct non-Pregel path is used in production modes that matter. |
| `CLAIM-B01-028` | `REQ-LEARN-046.0` | source | `gitrefrepo/neo4j-gds-src/algo/src/main/java/org/neo4j/gds/pagerank/PageRankMemoryEstimateDefinition.java:29-37` | `memoryEstimation()` | PageRank estimate defers to `Pregel.memoryEstimation(...)` with one `DOUBLE` node property slot named `pagerank`. | v003 PageRank estimates must explicitly account for at least one per-node double state vector, plus runtime machinery around it. | Additional runtime buffers may still be required beyond this one declared property slot. | Strict holistic RAM | Falsifier: the estimator omits the dominant runtime memory class for real runs. |
| `CLAIM-B01-029` | `REQ-LEARN-046.0` | source | `gitrefrepo/neo4j-gds-src/algo/src/main/java/org/neo4j/gds/pagerank/PageRankAlgorithm.java:39-136` | `PageRankAlgorithm` | The algorithm runs a Pregel job, extracts scores as a `HugeDoubleArray`, optionally scales them, and returns `PageRankResult`. | Stream or write modes still carry at least one per-node double result vector in memory. | Strict-RAM mode may need spill or chunked result handling for larger graphs. | Strict holistic RAM | Falsifier: `HugeDoubleArray` is only a view over external storage and not resident state. |
| `CLAIM-B01-030` | `REQ-LEARN-046.0` | source | `gitrefrepo/neo4j-gds-src/algo/src/main/java/org/neo4j/gds/pagerank/PageRankComputation.java:35-114` | `PageRankComputation` | PageRank computation defines a Pregel schema with a `DOUBLE` node value, sends rank deltas to neighbors, can apply relationship weights, and exposes an estimate definition. | Minimum storage support for PageRank includes adjacency iteration, optional relationship weights, per-node double state, and message-passing style execution. | Exact physical orientation still needs a later graph-interface study. | Strict holistic RAM | Falsifier: physical graph access requires features missing from flat CSR plus sidecars. |
| `CLAIM-B01-031` | `REQ-LEARN-044.0` | source | `gitrefrepo/neo4j-gds-src/proc/centrality/src/main/java/org/neo4j/gds/pagerank/PageRankWriteProc.java:38-58` and `.../PageRankWriteStep.java:34-67` | `PageRankWriteProc` and `PageRankWriteStep` | Write mode exposes `gds.pageRank.write`, estimates separately, and persists node property values through `writeToDatabase.perform(...)`. | Supporting PageRank write mode requires either real OLTP writeback or an explicitly bounded alternate semantics, not just stream results. | A projected-sidecar mutate mode can probably land before OLTP writeback mode. | Complete GDS surface | Falsifier: write mode can be emulated without touching stored node properties or explicit result artifacts. |
| `CLAIM-B01-032` | `REQ-LEARN-001.0` | source | `gitrefrepo/neo4j-src/community/record-storage-engine/.../NodeRecordFormat.java:30-116`, `.../RecordRelationshipTraversalCursor.java:40-220`, `.../GraphDatabaseSettings.java:684-688` | `NodeRecordFormat`, `RecordRelationshipTraversalCursor`, `dense_node_threshold` | Neo4j OLTP uses compact fixed-size node records with dense-node state, relationship-chain vs relationship-group traversal, and a configurable dense-node threshold of `50`. | Neo4j-shaped OLTP storage is pointer-rich and correct for transactions, but it is not the target OLAP physical shape. | Later OLTP batches still need relationship/property record and lock/WAL evidence. | Neo4j-shaped OLTP | Falsifier: Neo4j OLAP/GDS actually reads directly from these record chains instead of projected graph stores. |

## Procedure-To-Kernel Ledger

| procedure_name | mode | config_type | result_type | estimate_path | algorithm_spec | implementation_class | graph_interfaces | topology_orientation | sidecar_inputs | dominant_state | mutate_write_target | oracle_test | storage_implication | ram_risk | source_paths |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `gds.pageRank.stream` | `stream` | `PageRankStreamConfig` | `CentralityStreamResult` | `PageRankStreamProc.estimate -> CentralityProcedureFacade.pageRankStreamEstimate -> LocalCentralityProcedureFacade.pageRankStreamEstimate -> estimationModeBusinessFacade.pageRank -> PageRankMemoryEstimateDefinition` | `CentralityAlgorithms.pageRank(...)` | `PageRankAlgorithm` plus `PageRankComputation` | `org.neo4j.gds.api.Graph` plus Pregel-style neighbor messaging | `MissingEvidence` for exact physical orientation in this batch; projected graph and optional relationship weighting are clearly involved | optional relationship weight property; source nodes; projected graph metadata | one per-node double rank state plus message flow; result extraction into `HugeDoubleArray` | none | `PageRankStreamProcTest` in local clone plus future flat-CSR parity graph | minimum viable support is flat global adjacency plus optional weight sidecar plus per-node state; strict-RAM mode likely needs explicit handling of O(V) doubles | `high` at 50GB-class scale until estimate object is made holistic | `PageRankStreamProc.java:41-57`; `CentralityProcedureFacade.java:273-278`; `LocalCentralityProcedureFacade.java:1173-1199`; `CentralityAlgorithms.java:366-440`; `PageRankMemoryEstimateDefinition.java:29-37`; `PageRankAlgorithm.java:39-136`; `PageRankComputation.java:35-114` |
| `gds.pageRank.write` | `write` | `PageRankWriteConfig` | `PageRankWriteResult` | `PageRankWriteProc.estimate -> CentralityProcedureFacade.pageRankWriteEstimate -> LocalCentralityProcedureFacade.pageRankWriteEstimate -> estimationModeBusinessFacade.pageRank -> PageRankMemoryEstimateDefinition` | `CentralityAlgorithms.pageRank(...)` plus write-mode business path | `PageRankAlgorithm` plus `PageRankWriteStep` | `Graph`, `GraphStore`, and `ResultStore` appear in the write step | `MissingEvidence` for exact physical orientation in this batch | optional relationship weight property; projected graph metadata; write-property config | PageRank state plus writeback result path | `writeToDatabase.perform(...)` writes node property values | `PageRankWriteProcTest` in local clone plus future writeback contract tests | write mode needs all stream-mode storage plus either true OLTP property writeback or a clearly documented deferred/result-sidecar alternative | `high` | `PageRankWriteProc.java:42-58`; `LocalCentralityProcedureFacade.java:1202-1230`; `PageRankWriteStep.java:34-67`; shared algorithm/estimate paths above |

## Architecture Fit Matrix

| capability | topology_need | sidecar_need | build_store_need | snapshot_catalog_need | algorithm_state | memory_plan | execution_strategy | support_status | falsifier |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `current.walk.forward_one/two` and `current.walk.backward_one/two` | flat dual CSR plus dense-key indirection | none | none for current seed | manifest only | small query-local hop expansion | `mmap` fast mode; page-cache mediated RSS | current `MmapWalkRuntime` | `P1-ImplementedExactLowRam` | current tests stop proving correct walk parity on fixture corpus |
| `gds.graph.project` | flat CSR alone is insufficient | node/relationship projection metadata, label/type/property sidecars | dense-id assignment, dictionaries, sorted runs, projection validation | named graph generation identity | build scratch rather than runtime-heavy state | projection estimate must include build scratch and semantic sidecars | build/control plane compile into a published snapshot | `NeedsArchitectureSpike` | a working implementation appears that serves native/cypher projection from the existing seven-file snapshot with no new artifacts |
| `gds.graph.list` | none directly | schema, histogram, size, and configuration metadata | optional counts/histograms | user/database/name scoped graph catalog | none | metadata-only | snapshot/projection catalog query | `P0-RegisteredCompatible` | user workflows do not depend on durable graph identity or ambiguity handling |
| `gds.pageRank.stream` | flat global adjacency is likely enough as a first primitive | optional relationship weight sidecar; degree metadata may be cached or derived | estimate statistics and projection metadata | named graph selection and watermark reporting | one or more per-node double vectors plus Pregel runtime state | no deterministic-RAM claim without accounting for O(V) doubles, scratch, and page cache | `mmap` for normal fast mode; explicit stream or reject/spill for strict mode | `NeedsArchitectureSpike` | traced kernel later proves that PageRank requires mutable adjacency packaging rather than flat CSR plus sidecars |
| `gds.pageRank.write` | same as PageRank stream | same as PageRank stream plus result/write-property semantics | writeback planning metadata | graph generation plus write target identity | same PageRank state plus write path | same as stream plus result/writeback accounting | algorithm compute plus OLTP writeback or explicitly documented alternative | `NeedsArchitectureSpike` | write mode can be made correct without an explicit write target or result artifact |
| `gds.model.list` | none | model-artifact metadata | model registration state | model catalog identity and versioning | none | metadata-only | model catalog query | `P0-RegisteredCompatible` | v003 intentionally excludes all model-facing surface from claimed compatibility |
| `gds.pipeline.list` | none | pipeline metadata | pipeline catalog state | pipeline catalog identity | none | metadata-only | pipeline catalog query | `P0-RegisteredCompatible` | pipeline catalog turns out to be dead surface for all target workflows |
| `gds.listProgress` | none | none | job metadata store | none beyond job identity | active progress rows only | operational telemetry memory only | operations facade over job tracker | `P0-RegisteredCompatible` | callers never rely on progress procedures in target workflows |

## Architecture Option Scorecard

The active option names come from `docs_PRD03/Arch-options.md`.

| option_id | option_summary | helps_prd_outcomes | known_blockers | required_capabilities | evidence_strength | dominant_ram_risks | current_status | next_falsifier |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `A` | Neo4j OLTP -> Build Store -> flat CSR | clean OLTP/OLAP split; simple published snapshot read path; preserves current seed | graph catalog, sidecars, and mutate/write semantics are not solved by topology alone | snapshot open/query, catalog identity, build reproducibility | `medium` | per-node algorithm state and build scratch can exceed heap-only intuition | `P0-RegisteredCompatible` | complete one more traced family besides PageRank and show it still fits without cells |
| `B` | Neo4j OLTP -> Build Store -> flat CSR plus sidecars | covers semantics outside topology: labels, types, weights, properties, results | sidecar cataloging, writeback, and holistic estimates still need explicit design | graph project/list, PageRank weighted modes, result sidecars | `medium` | sidecar duplication and page-cache blow-up if columns are over-materialized | `P0-RegisteredCompatible` | find a required GDS family that needs mutable topology instead of topology plus sidecars |
| `C` | Neo4j OLTP -> Build Store -> cellular CSR snapshots | may help locality, bounded rebuilds, and packaging | no first-party evidence yet that cells are required for the traced surface | locality-heavy workloads, bounded rebuild, optional local compaction | `low` | duplicated metadata, boundary overhead, cache churn if partitioning is poor | `NeedsArchitectureSpike` | show that flat global CSR plus sidecars cannot honestly meet a representative GDS family or locality budget |
| `D` | Neo4j OLTP -> Build Store -> hybrid flat plus cellular publication | preserves flat global scans while allowing locality packaging | duplicates topology unless tightly justified; ops complexity rises | same as `B` plus selective locality packaging | `low` | duplicate topology, duplicate page-cache competition, publication complexity | `NeedsArchitectureSpike` | prove cells matter for one high-value family without breaking global scan economics |
| `E` | Neo4j OLTP -> Build Store -> multi-generation snapshot catalog | required for watermarks, rollback, and atomic publication | active-pointer, retention, reader-pinning, and restart-recovery design still missing | named graph identity, generation swap, watermark reporting | `medium` | catalog metadata is small, but poor retention can silently multiply disk and cache pressure | `P0-RegisteredCompatible` | find a credible published-snapshot design that does not need generation or watermark semantics |

## PRD Outcome Traceability Dossier

| PRD outcome | supporting claims | current confidence | next experiment or evidence spike |
| --- | --- | --- | --- |
| `Neo4j-compatible API` | `CLAIM-B01-013` through `CLAIM-B01-021`, `CLAIM-B01-024` through `CLAIM-B01-031` | `medium` | generate a checked inventory artifact for all visible `gds.*` procedures and major Bolt/Cypher/driver touchpoints |
| `Neo4j-shaped OLTP` | `CLAIM-B01-032` | `medium` | add relationship/property record, WAL, lock, and Bolt/testkit evidence in the next OLTP batch |
| `Published OLAP snapshots` | `CLAIM-B01-001` through `CLAIM-B01-011` | `high` for current seed, `low` for full v003 | design the snapshot-generation catalog and reader-pinning contract |
| `Projection Build Store` | `CLAIM-B01-015`, `CLAIM-B01-017`, `CLAIM-B01-022`, option rows `A-B-E` | `low` | run the Build Store precedent batch against `apache-iggy`, `redb`, `fjall`, and `rocksdb` |
| `Complete GDS surface` | `CLAIM-B01-013` through `CLAIM-B01-021`, proc counts in `CLAIM-B01-016` | `medium` | generate the full surface inventory before any architecture sufficiency claim |
| `Strict holistic RAM` | `CLAIM-B01-002`, `CLAIM-B01-006`, `CLAIM-B01-009`, `CLAIM-B01-023`, `CLAIM-B01-028` through `CLAIM-B01-030` | `medium` | define a v003 estimate object that includes page-cache, direct buffers, scratch, sidecars, and result state |
| `Atomic publication` | `CLAIM-B01-004`, `CLAIM-B01-008`, `CLAIM-B01-018`, `CLAIM-B01-022`, option row `E` | `low` | create the generation-catalog batch and publication-state machine |

## Rejected-Alternative Note

Rejected for this batch:

- `Treat current flat CSR plus mmap as already sufficient for the full GDS surface.`

Why rejected:

- The current runtime surface is only four walk-query families.
- The local GDS clone exposes `313` public `gds.*` procedure annotations.
- Public surface already includes graph catalog, model catalog, pipeline
  catalog, and operations/progress APIs.
- Even one traced family, PageRank, already implies mode routing, estimates,
  optional relationship weights, per-node state, and writeback semantics beyond
  simple neighbor iteration.

What would overturn this rejection:

- Multiple fully traced representative families show that flat CSR plus a small
  fixed set of sidecars can support them all without additional storage layers
  or unbounded RAM.

## Skeptical Review

| challenge | response |
| --- | --- |
| Are you just renaming "read everything into memory" as `mmap`? | No. `CLAIM-B01-006` explicitly records that current runtime residency is OS-mediated page cache, not deterministic RAM. |
| Are you shrinking GDS to PageRank because it is convenient? | No. `CLAIM-B01-016` records surface breadth, and every option score remains non-final until full inventory exists. |
| Are you quietly serving OLAP from the Neo4j OLTP record store? | No. `CLAIM-B01-032` reinforces that Neo4j's record-store shape is the OLTP source-of-truth boundary, not the OLAP read primitive. |
| Are cells already proven necessary? | No. Scorecard rows `C` and `D` remain `NeedsArchitectureSpike`. |
| Are write and mutate semantics being ignored? | No. `CLAIM-B01-025`, `CLAIM-B01-026`, and `CLAIM-B01-031` explicitly show that PageRank has multi-mode semantics and a write step. |
| Is the Build Store accidentally becoming a serving layer? | No. This batch only infers Build Store responsibilities from projection and catalog needs; it does not place it on the read path. |

## Verification Commands Run

```bash
git rev-parse --abbrev-ref HEAD
git -C gitrefrepo/neo4j-gds-src rev-parse --short HEAD
git -C gitrefrepo/neo4j-src rev-parse --short HEAD
find docs_PRD03 -maxdepth 2 -type f | sort
find src tests -maxdepth 3 -type f | sort
rg -n "trait SnapshotArtifactWriter|struct SnapshotManifest|trait WalkQueryRuntime|enum QueryFamily|struct BuildMemoryBudget|storage_mode: \"immutable_dual_csr\"|memmap2::Mmap|write_snapshot_artifacts" src README.md tests
rg -n "@Procedure\\(value = \"gds\\." gitrefrepo/neo4j-gds-src/proc -g '*.java' | wc -l
find gitrefrepo/neo4j-gds-src/proc -maxdepth 1 -mindepth 1 -type d
rg -n "interface GraphDataScienceProcedures|class AlgorithmsProcedureFacade|interface GraphCatalogProcedureFacade|class PageRankStreamProc|class GraphStoreCatalog|class CompressedAdjacencyList|@Procedure\\(value = \"gds\\." gitrefrepo/neo4j-gds-src -g '*.java'
rg -n "class NodeRecordFormat|class RelationshipRecordFormat|class PropertyRecordFormat|class RecordRelationshipTraversalCursor|dense_node_threshold|denseNodeThreshold" gitrefrepo/neo4j-src/community -g '*.java'
/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker
/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/scripts/scan_current_repo_only.sh /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker
```

## Checkpoint: surface+capability+architecture / current-seed+gds-baseline / 2026-06-24

Assigned requirement IDs:

- `REQ-LEARN-001.0`
- `REQ-LEARN-006.0`
- `REQ-LEARN-007.0`
- `REQ-LEARN-008.0`
- `REQ-LEARN-030.0`
- `REQ-LEARN-031.0`
- `REQ-LEARN-032.0`
- `REQ-LEARN-034.0`
- `REQ-LEARN-035.0`
- `REQ-LEARN-036.0`
- `REQ-LEARN-037.0`
- `REQ-LEARN-038.0`
- `REQ-LEARN-040.0`
- `REQ-LEARN-041.0`
- `REQ-LEARN-042.0`
- `REQ-LEARN-043.0`
- `REQ-LEARN-044.0`
- `REQ-LEARN-045.0`
- `REQ-LEARN-046.0`
- `REQ-LEARN-049.0`
- `REQ-LEARN-050.0`
- `REQ-LEARN-051.0`
- `REQ-LEARN-052.0`

Evidence rows completed:

- `32`

Most important sourced facts:

- `src/snapshot.rs:23-120` and `src/runtime.rs:22-177` show the current seed is a real immutable dual-CSR snapshot with `mmap` open-path validation.
- `gitrefrepo/neo4j-gds-src/procedures/.../GraphDataScienceProcedures.java:30-43` and `GraphCatalogProcedureFacade.java:49-232` show the promised GDS surface is far broader than walk kernels.
- `gitrefrepo/neo4j-gds-src/proc/.../PageRankStreamProc.java`, `LocalCentralityProcedureFacade.java`, `CentralityAlgorithms.java`, `PageRankAlgorithm.java`, and `PageRankComputation.java` provide a complete representative procedure-to-kernel trace.

Architecture implications:

- `Adopt`: keep current flat dual CSR as the first OLAP read primitive and parity oracle.
- `Adapt`: add graph catalog, sidecars, estimate objects, and publication metadata around that primitive.
- `Reject`: claiming full GDS sufficiency from the current walk runtime.
- `Watch`: cells as a packaging evolution only if later evidence proves locality or bounded rebuild benefits.
- `MissingEvidence`: full GDS inventory, Build Store precedent study, snapshot-generation publication contract, and a second representative algorithm-family trace.

Unresolved risks:

- `Risk`: PageRank is only one family; another traced family may force new storage demands.
  `Falsifier`: a later WCC/KNN/Node2Vec trace shows topology or state needs that flat CSR plus sidecars cannot satisfy.
- `Risk`: graph catalog semantics may force more metadata richness than assumed here.
  `Falsifier`: graph-list/project tests in the GDS clone reveal mandatory schema or histogram semantics not covered by a lean snapshot catalog.
- `Risk`: holistic RAM may be dominated by algorithm state, not topology.
  `Falsifier`: a later estimator-derived 50GB-on-8GB study shows PageRank or similarity plans exceed credible budgets even with flat CSR.
