# Batch 06: Sidecars, Planner Inputs, And Compact Competitors

Date: 2026-06-24

Assigned lanes:

- `Capability lane`
- `Architecture lane`
- `Execution lane`
- `Rejection lane`

Assigned PRD outcomes:

- `Complete GDS surface`
- `Published OLAP snapshots`
- `Projection Build Store`
- `Strict holistic RAM`
- `Neo4j-compatible API`

Requirement IDs touched in this batch:

- `REQ-LEARN-012.0`
- `REQ-LEARN-013.0`
- `REQ-LEARN-014.0`
- `REQ-LEARN-019.0`
- `REQ-LEARN-034.0`
- `REQ-LEARN-035.0`
- `REQ-LEARN-036.0`
- `REQ-LEARN-037.0`
- `REQ-LEARN-038.0`
- `REQ-LEARN-040.0`
- `REQ-LEARN-041.0`
- `REQ-LEARN-049.0`
- `REQ-LEARN-050.0`
- `REQ-LEARN-051.0`
- `REQ-LEARN-053.0`

Batch status:

- This batch answers what must exist outside flat topology before v003 can
  honestly claim a GDS-capable OLAP path.
- This batch does not replace flat dual CSR as the topology oracle.
- This batch does reject three seductive mistakes:
  - turning DataFusion into the v003 product architecture;
  - duplicating full topology into a second generic column store;
  - treating in-memory or matrix-centric competitor designs as direct
    compatibility truth.

## Clone Coverage Ledger

| local_repo | exists_now | branch_or_head | study_role | required_or_optional | current_use | note |
| --- | --- | --- | --- | --- | --- | --- |
| `gitrefrepo/apache-arrow-rs-src` | yes | `2eeb805` | sidecar physical-layout precedent | required | active study | dictionary, null, fixed-list, and zero-copy IPC evidence |
| `gitrefrepo/apache-parquet-format-src` | yes | `662cdac` | cold-column and file-format precedent | required | active study | docs/spec-first; used for null, dictionary, and page-index semantics |
| `gitrefrepo/apache-datafusion-src` | yes | `f220077` | planner and spill-discipline precedent | required | active study | catalog snapshot, pushdown, explain, memory-pool, and disk-spill evidence |
| `gitrefrepo/ladybug-src` | yes | `7eab431` | compact embedded graph precedent | required | active study | immutable Parquet+CSR snapshots, morsel execution, WAL/checkpoint, and optimizer evidence |
| `gitrefrepo/kuzu-src` | yes | `89f0263` | compact embedded graph precedent | required | active study | disk-columnar plus CSR plus vectorized/factorized engine precedent |
| `gitrefrepo/memgraph-src` | yes | `8ea82dc` | compatibility and ops contrast | required | active study | Cypher-compatible in-memory system; useful mostly as RAM-first counterexample |
| `gitrefrepo/falkordb-src` | yes | `1a57217` | sparse-linear-algebra contrast | optional | active study | matrix substrate and vector/fulltext index-surface evidence |
| `gitrefrepo/age-src` | yes | `9960e9c` | transactional catalog and hybrid-query contrast | optional | active study | PostgreSQL extension precedent for graph catalog writes and hybrid SQL/Cypher |

## Graph Evidence Execution Ledger

This batch used the local Codex skills named by the user:

- `/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/SKILL.md`
- `/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/SKILL.md`

Whole-repo graph-tool coverage for `apache-arrow-rs-src`, `apache-datafusion-src`,
`ladybug-src`, `kuzu-src`, `memgraph-src`, `falkordb-src`, `age-src`, and the
docs-first `apache-parquet-format-src` is already recorded in
`docs_PRD03/reference-learning/Reference-Shelf-Graph-Evidence-Ledger.md`.

This batch additionally ran subpath-level dual-tool scans for the exact folders
named by `REQ-LEARN-012.0`, `REQ-LEARN-013.0`, and `REQ-LEARN-019.0`, with the
summary captured in:

- `/tmp/codex-code-intel/batch06-subpaths/batch06-subpaths-summary-20260624-133816.tsv`

| target | cbm_status | cgc_status | note |
| --- | --- | --- | --- |
| `gitrefrepo/apache-arrow-rs-src/arrow-array` | `ready` | `ready` | subpath evidence for dictionary and fixed-size list arrays |
| `gitrefrepo/apache-arrow-rs-src/arrow-buffer` | `ready` | `ready` | subpath evidence for null-buffer handling |
| `gitrefrepo/apache-arrow-rs-src/arrow-ipc` | `ready` | `ready` | subpath evidence for zero-copy and mmap-compatible reads |
| `gitrefrepo/apache-arrow-rs-src/parquet` | `ready` | `exit_1` | CBM useful; CGC low-yield on this subcrate, but whole-repo coverage exists |
| `gitrefrepo/apache-datafusion-src/datafusion/catalog` | `ready` | `ready` | catalog snapshot precedent |
| `gitrefrepo/apache-datafusion-src/datafusion/core` | `ready` | `timeout_180s` | whole-repo coverage exists; direct source reads used for this batch instead |
| `gitrefrepo/apache-datafusion-src/datafusion/datasource-parquet` | `ready` | `ready` | row-group and page pruning precedent |
| `gitrefrepo/apache-datafusion-src/datafusion/execution` | `ready` | `ready` | memory pool and disk spill precedent |
| `gitrefrepo/apache-datafusion-src/datafusion/expr` | `ready` | `ready` | table-source pushdown hooks |
| `gitrefrepo/apache-datafusion-src/datafusion/optimizer` | `ready` | `ready` | optimizer-rule sequencing precedent |
| `gitrefrepo/apache-datafusion-src/datafusion/physical-plan` | `ready` | `ready after timeouted index pass` | plan-explain precedent |
| `gitrefrepo/apache-datafusion-src/datafusion/session` | `ready` | `ready` | session-level planner registration context |
| `gitrefrepo/apache-datafusion-src/datafusion/sql` | `ready` | `ready` | scanned only as planner context, not as adoption target |
| `gitrefrepo/ladybug-src/src/storage` | `ready` | `ready` | WAL/checkpoint/storage evidence |
| `gitrefrepo/ladybug-src/src/transaction` | `ready` | `ready` | transaction and durability context |
| `gitrefrepo/ladybug-src/src/planner` | `ready` | `ready` | planner and stats context |
| `gitrefrepo/ladybug-src/src/optimizer` | `ready` | `ready` | filter/projection/factorization evidence |
| `gitrefrepo/ladybug-src/src/processor` | `ready` | `ready` | morsel and result-sidecar execution evidence |
| `gitrefrepo/ladybug-src/src/graph` | `ready` | `ready` | graph runtime context |

## Evidence Ledger

| claim_id | req_id | source_type | source_path | symbol_or_query | sourced_fact | inference | speculation | PRD impact | skeptical note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `CLAIM-B06-001` | `REQ-LEARN-012.0` | source | `gitrefrepo/apache-arrow-rs-src/arrow-array/src/array/dictionary_array.rs:162-170` | `DictionaryArray` | Arrow dictionary arrays are represented by a `keys` array plus a `values` array, with keys indexing the value dictionary. | Low-cardinality node labels, relationship types, enums, and repeated strings fit naturally as dictionary sidecars instead of repeated inline values. | v003 may later encode some ultra-hot low-cardinality fields more compactly than generic Arrow. | Complete GDS surface; Strict holistic RAM | Falsifier: target GDS procedures need those values materialized row-by-row often enough that dictionary indirection harms the RAM story more than it helps. |
| `CLAIM-B06-002` | `REQ-LEARN-012.0` | source | `gitrefrepo/apache-arrow-rs-src/arrow-array/src/array/fixed_size_list_array.rs:39-59,327-362` and `gitrefrepo/apache-arrow-rs-src/arrow-buffer/src/buffer/null.rs:34-64,113-132` | `FixedSizeListArray`, `NullBuffer` | Arrow fixed-size list arrays store each logical list in a child array with one constant element length, and null handling is explicit through a null buffer that can be expanded to child elements. | Fixed-dimension embeddings or feature vectors should be stored as fixed-size-list sidecars with explicit null accounting, not as variable-length lists. | v003 may want a custom tighter vector sidecar later, but its semantics should match this shape. | Complete GDS surface; Strict holistic RAM | Falsifier: most target vector-like properties are sparse or jagged enough that fixed-size layout wastes too much disk and page-cache space. |
| `CLAIM-B06-003` | `REQ-LEARN-012.0` | source | `gitrefrepo/apache-arrow-rs-src/arrow-ipc/src/reader.rs:750-757,946-952,1321-1324` | `read_record_batch`, `FileReader` docs | Arrow IPC reading remains zero-copy when alignment is already correct, and the reader docs explicitly cite mmap-based zero-copy examples. | Hot published sidecars and result artifacts can use mmap-friendly Arrow/IPC-style layouts for interactive reads without forcing a generic database engine. | v003 may choose a custom manifest around Arrow-compatible buffers rather than stock IPC files everywhere. | Published OLAP snapshots; Strict holistic RAM | Falsifier: integrity, schema evolution, or startup-time concerns make raw IPC packaging too brittle as a long-lived published artifact. |
| `CLAIM-B06-004` | `REQ-LEARN-012.0` | source | `gitrefrepo/apache-parquet-format-src/README.md:166-181,207-221`, `gitrefrepo/apache-parquet-format-src/Encodings.md:75-110`, `gitrefrepo/apache-parquet-format-src/src/main/thrift/parquet.thrift:284-291,918-945,1293-1302` | `Nulls`, `Column chunks`, `dictionary encoding`, `null_count` | Parquet encodes nullity through definition levels, stores dictionary pages first in column chunks, supports optional column/page indexes for skipping, and records null counts that readers must not infer when absent. | Cold property columns, large scalar sidecars, and bulk interchange artifacts should prefer Parquet-like storage because it offers compression, skipping, and statistics without requiring topology duplication. | v003 may only use a subset of Parquet features initially, especially if exact-RAM readers need simpler decode paths. | Published OLAP snapshots; Strict holistic RAM | Falsifier: decode complexity or page-cache pressure from Parquet readers outweighs the savings for the initial laptop-scale target. |
| `CLAIM-B06-005` | `REQ-LEARN-013.0` | source | `gitrefrepo/apache-datafusion-src/datafusion/catalog/src/catalog.rs:35-87`, `gitrefrepo/apache-datafusion-src/datafusion/expr/src/table_source.rs:94-123`, `gitrefrepo/apache-datafusion-src/datafusion/optimizer/src/optimizer.rs:255-319`, `gitrefrepo/apache-datafusion-src/datafusion/physical-plan/src/display.rs:116-174` | `CatalogProvider`, `TableSource::supports_filters_pushdown`, `Optimizer::new`, `DisplayableExecutionPlan` | DataFusion treats catalogs as layered metadata, recommends in-memory metadata snapshots for planning, exposes filter-pushdown introspection on table sources, orders optimizer rules carefully, and wraps execution plans for explain-style rendering. | v003 needs a graph projection catalog snapshot, explicit pushdown declarations for label/type/property filters, and explainable physical-plan selection, but not a generic SQL engine. | A future v003 diagnostics layer could expose plan explanations that look graph-native rather than relational. | Complete GDS surface; Neo4j-compatible API | Falsifier: a minimal GDS-only router proves good enough without pushdown introspection or plan explanation. |
| `CLAIM-B06-006` | `REQ-LEARN-013.0` | source | `gitrefrepo/apache-datafusion-src/datafusion/execution/src/memory_pool/mod.rs:74-108` and `gitrefrepo/apache-datafusion-src/datafusion/execution/src/disk_manager.rs:160-190` | `MemoryPool`, `DiskManager` | DataFusion forces stateful operators to reserve memory, then either spill or fail, and tracks spill files, bytes, and temp-directory limits explicitly. | v003 algorithms and snapshot build phases need explicit memory contracts and spill managers rather than silent allocator growth. | The first v003 implementation may encode these contracts more simply than DataFusion. | Strict holistic RAM; Projection Build Store | Falsifier: the chosen GDS family slice can be implemented without meaningful spill or intermediate state, making a structured memory contract unnecessary at first. |
| `CLAIM-B06-007` | `REQ-LEARN-019.0` | source | `gitrefrepo/ladybug-src/docs/icebug-disk.md:5-6,26-27,38,42-58` | `Icebug-Disk` | Ladybug documents a read-only graph storage format based on Parquet files, with node tables in `nodes_*.parquet`, relationship CSR in `indices_*.parquet` plus `indptr_*.parquet`, and an optional flat relationship layout in a single parquet file. | A published immutable graph snapshot made of topology plus sidecars is not a theoretical idea; it is an already-practiced graph shape close to the v003 direction. | v003 may keep flat dual CSR topology in dedicated binaries rather than adopt full Parquet topology files. | Published OLAP snapshots | Falsifier: Ladybug’s immutable format works for its workload mix but becomes too decode-heavy or compatibility-awkward for v003’s GDS path. |
| `CLAIM-B06-008` | `REQ-LEARN-019.0` | source | `gitrefrepo/ladybug-src/docs/index_build_recovery.md:5-27,45-73`, `gitrefrepo/ladybug-src/src/include/storage/wal/wal.h:17-70`, `gitrefrepo/ladybug-src/src/include/storage/checkpointer.h:31-108` | `Current recovery invariant`, `WAL`, `Checkpointer` | Ladybug requires committed valid indexes to have recoverable physical storage, uses WAL plus checkpoint machinery, separates checkpoint phases, tracks durable commit sequences, and explicitly calls for a global memory budget and private build buffers during parallel builds. | Build/publish machinery around OLAP sidecars matters as much as the sidecar format itself: recovery, checkpoint, watermark, and background memory must be first-class. | v003 may use simpler build-state machinery than Ladybug, but it cannot skip these correctness classes. | Projection Build Store; Published OLAP snapshots; Strict holistic RAM | Falsifier: v003 publishes only full rebuild artifacts from OLTP truth and can safely avoid any finer-grained WAL/checkpoint discipline. |
| `CLAIM-B06-009` | `REQ-LEARN-013.0` | source | `gitrefrepo/ladybug-src/docs/morsel_parallelism.md:5-6,54-74,94-143`, `gitrefrepo/ladybug-src/src/common/arrow/arrow_array_scan.cpp:12-16,31-43,155-240`, `gitrefrepo/ladybug-src/src/optimizer/filter_push_down_optimizer.cpp:37-90,93-156`, `gitrefrepo/ladybug-src/src/optimizer/projection_push_down_optimizer.cpp:34-121`, `gitrefrepo/ladybug-src/src/optimizer/factorization_rewriter.cpp:30-188`, `gitrefrepo/ladybug-src/src/processor/operator/arrow_result_collector.cpp:75-177` | `morsel parallelism`, `scanArrowArray*`, `FilterPushDownOptimizer`, `ProjectionPushDownOptimizer`, `FactorizationRewriter`, `CSRMetadata` | Ladybug combines fine-grained morsels for Arrow tables, vectorized Arrow scans, filter/projection pushdown, factorized execution rewriting, and even Arrow result-sidecar CSR reconstruction. | v003 can borrow execution ideas on top of published snapshots: vectorized sidecar scans, pushdown, factorization, and result artifacts can sit above flat CSR without replacing the base topology. | Some of these execution ideas may be too complex for the first milestone and may land later as execution profiles. | Complete GDS surface; Strict holistic RAM | Falsifier: direct GDS kernels over flat CSR plus minimal sidecars already hit the latency and RAM targets without these execution layers. |
| `CLAIM-B06-010` | `REQ-LEARN-014.0` | source | `gitrefrepo/kuzu-src/README.md:20-31`, `gitrefrepo/kuzu-src/src/transaction/transaction.cpp:33-37,55-84`, `gitrefrepo/kuzu-src/src/planner/join_order/cardinality_estimator.cpp:40-52` | `Kuzu features`, `Transaction::commit`, `CardinalityEstimator` | Kuzu advertises columnar disk-based storage, CSR adjacency indices, vectorized and factorized execution, serializable ACID transactions, local WAL handling, and planner cardinality from table stats. | Kuzu is the closest compact competitor precedent for a low-RAM graph engine that still respects query planning and durability. | v003 may remain more snapshot-centric than Kuzu’s live query engine. | Published OLAP snapshots; Strict holistic RAM | Falsifier: deeper Kuzu internals reveal hidden cache or executor state that makes it a poor RAM-first precedent after all. |
| `CLAIM-B06-011` | `REQ-LEARN-014.0` | source | `gitrefrepo/memgraph-src/README.md:36-80`, `gitrefrepo/memgraph-src/src/replication_handler/replication_handler.cpp:60-67,96-103,210-217` | `Memgraph features`, `RecoverReplication` | Memgraph positions itself as a high-performance in-memory graph database, Neo4j Cypher-compatible, with a large MAGE algorithm library, while its replication/durability path explicitly warns when snapshots and WAL are disabled and uses `InMemoryStorage` read-only accessors. | Memgraph is useful as a compatibility and algorithm-surface contrast, but it is not a direct precedent for a lowest-RAM OLAP architecture. | Its procedure surface and ecosystem expectations may still matter later for compatibility triage. | Neo4j-compatible API; Strict holistic RAM | Falsifier: a significant portion of the desired user base actually prioritizes in-memory low-latency profiles over the 50GB-on-8GB story. |
| `CLAIM-B06-012` | `REQ-LEARN-014.0` | source | `gitrefrepo/falkordb-src/README.md:41-55`, `gitrefrepo/falkordb-src/tests/flow/index_utils.py:23-95` | `Sparse Matrix Representation`, `CALL db.indexes()`, `CREATE ... VECTOR INDEX` | FalkorDB describes itself as a property graph database that uses sparse matrices and linear algebra for querying, while its test utilities show index-surface expectations including range, fulltext, and vector indexes over nodes and edges. | Sparse-linear-algebra execution is a real alternative for selected algorithm families, but FalkorDB’s substrate and index surface are not a reason to replace flat CSR globally. | A later GraphBLAS batch may adopt limited sparse-matrix execution for some similarity or linear-algebra-friendly families. | Complete GDS surface | Falsifier: later kernel tracing shows the target GDS families mostly align with matrix operations and are cheaper to implement that way. |
| `CLAIM-B06-013` | `REQ-LEARN-014.0` | source | `gitrefrepo/age-src/README.md:59-60,75-93,220-236`, `gitrefrepo/age-src/src/backend/commands/graph_commands.c:47-108` | `What is Apache AGE?`, `create_graph`, transactional note | AGE is a PostgreSQL extension that keeps relational and graph data in one storage world, supports SQL plus Cypher, and requires transaction commits for graph and label catalog writes to become visible across sessions. | AGE is a control-plane and catalog-discipline precedent, not a low-RAM OLAP storage precedent. | If v003 later adds richer metadata management around graphs, models, or labels, some catalog-publication ideas may still be worth borrowing. | Neo4j-compatible API; Projection Build Store | Falsifier: deeper AGE study shows a cleaner hybrid serving model that materially improves the PRD03 boundaries without blowing RAM. |

## Sidecar And Planner Recommendation

### 1. What to adopt

- `Adopt`: flat CSR or dual CSR remains the only persisted topology baseline.
- `Adopt`: typed sidecars for node properties, relationship properties, labels,
  relationship types, weights, results, and model artifacts.
- `Adopt`: dictionary sidecars for low-cardinality repeated values.
- `Adopt`: fixed-size-list layout for fixed-dimension vectors and embeddings.
- `Adopt`: explicit null accounting in every sidecar format.
- `Adopt`: a planner contract that declares filter pushdown, projected columns,
  execution plan class, memory estimate, and spill policy.

### 2. What to adapt carefully

- `Adapt`: Arrow-like mmap or zero-copy buffers for hot or interactive sidecars.
- `Adapt`: Parquet for cold or bulk sidecars where skipping, compression, and
  interchange matter more than zero-copy semantics.
- `Adapt`: Ladybug-style vectorized scans, morsels, factorization, and result
  sidecars only as execution layers, not as a new truth store.
- `Adapt`: Kuzu-style stats-aware planning and vectorized execution ideas where
  they do not blur the Neo4j compatibility boundary.

### 3. What to reject

- `Reject`: a second full topology store in generic columnar form.
- `Reject`: “use DataFusion” as a product answer.
- `Reject`: Memgraph-like in-memory-first posture as the default v003 OLAP path.
- `Reject`: a global sparse-matrix substrate before the algorithm-family
  evidence says it is needed.

## Architecture Fit Matrix

| capability | topology_need | sidecar_need | build_store_need | snapshot_catalog_need | algorithm_state | memory_plan | execution_strategy | support_status | falsifier |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `typed scalar properties` | flat CSR or dual CSR only for adjacency | typed scalar columns, nulls, dictionaries where useful | property dictionary and schema metadata | generation-scoped sidecar manifest | low | hot columns can mmap; cold columns can stream | direct column scan plus adjacency join | `P0-RegisteredCompatible` | procedures rarely use projected properties, making typed sidecars overkill |
| `label and relationship-type filters` | adjacency plus node/rel ids | bitsets or dictionary-backed categorical columns | build-time derived label/type facts | sidecar version tied to snapshot generation | low | explicit bitset or categorical-column bytes | pushdown before neighbor expansion where possible | `P0-RegisteredCompatible` | all useful filters can be compiled into separate topology shards more cheaply |
| `dense vector or embedding properties` | adjacency optional by algorithm | fixed-size-list sidecar, explicit nulls | dimension metadata and schema checks | generation and model/result manifest entries | medium to high | no eager full-materialization; stream or chunk | sidecar scan plus algorithm-specific state | `NeedsArchitectureSpike` | fixed-dimension vectors are not common enough to justify first-class layout now |
| `cold wide property planes` | none beyond id mapping | Parquet-like cold columns with stats and skipping | compression, encoding, and row-group metadata | generation-scoped column manifests | low to medium | page-cache and decode budget must be named | streamed scan or selective prune | `P0-RegisteredCompatible` | cold-column decode cost dominates enough to break laptop-scale behavior |
| `projection catalog and explainable planning` | none directly | projection selectors, property lists, orientation metadata | projection-build metadata and stats | named graph generations and plan metadata | low | metadata plus estimate objects | graph-native planner with explain output | `P0-RegisteredCompatible` | a tiny router without explain or pushdown contracts proves sufficient |
| `global algorithms with bounded memory` | flat global edge stream from CSR | optional weights/properties sidecars | optional spill files and temp accounting | generation pinning during execution | medium to very high | explicit state, spill, direct-buffer, and scratch budgets | global stream plus spill or fail-fast | `NeedsArchitectureSpike` | first shipped algorithm slice never needs spill or large vectors |
| `published immutable graph snapshot` | flat CSR or dual CSR topology | typed sidecars and manifests only, no duplicate full topology | publish/validate/recover pipeline | active generation pointer, rollback, retention | low at serve time | build-time scratch must be budgeted | build once, publish atomically, read many | `P0-RegisteredCompatible` | near-real-time OLAP freshness becomes mandatory and immutable publication is too stale |
| `competitor-inspired execution improvements` | keep flat CSR as oracle | optional result/model sidecars | only if build plane can publish them safely | generation-aware artifact registration | medium | budget each added artifact separately | opt-in later execution profiles | `P2-ImplementedLater` | later evidence shows these layers add complexity without real RAM wins |

## Compact-Competitor Scorecard

| option_id | option_summary | helps_prd_outcomes | known_blockers | evidence_strength | dominant_ram_risks | current_status | next_falsifier |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `CC-A` | Ladybug-style immutable Parquet plus CSR graph snapshots | published snapshots, sidecars, analytical scans, build/publish discipline | may be too columnar-heavy if copied literally | `high` | decode and page-cache behavior on cold scans | `AdoptInspiration` | compare direct CSR binaries versus Parquet-packaged topology on the same walk and PageRank cases |
| `CC-B` | Kuzu-style compact embedded graph engine ideas | low-RAM graph engine, CSR adjacency, vectorized/factorized execution, stats-aware planner | live engine complexity may exceed v003’s immediate need | `medium-high` | executor state and planner complexity | `AdoptInspiration` | deeper scan of storage-manager and buffer behavior reveals hidden resident-state costs |
| `CC-C` | Memgraph-like in-memory Cypher plus big algorithm catalog | compatibility and algorithm-surface awareness | conflicts with RAM-first premise | `high` against adoption | whole-graph resident memory | `RejectAsDefault` | product direction changes away from 50GB-on-8GB feasibility |
| `CC-D` | FalkorDB-style sparse-matrix substrate | selected linear-algebra-friendly families | poor fit for universal GDS and Neo4j-like storage story | `medium` | matrix materialization and extra substrate complexity | `NeedsAlgorithmBatch` | kernel tracing shows major target families map cleanly to sparse algebra with lower RAM |
| `CC-E` | AGE-style PostgreSQL extension hybrid | catalog/publication semantics and SQL/Cypher coexistence caution | not a RAM-first OLAP template and not a Neo4j OLTP clone | `medium` | relational storage overhead and serving-plane complexity | `RejectAsStorageTemplate` | control-plane lessons turn out essential enough to justify deeper adoption |

## PRD Outcome Traceability Dossier

| PRD outcome | supporting claims | current confidence | next experiment or evidence spike |
| --- | --- | --- | --- |
| `Complete GDS surface` | `CLAIM-B06-001` through `CLAIM-B06-006`, `CLAIM-B06-009`, `CLAIM-B06-012` | `medium` | trace more algorithm families from public modes to concrete sidecar and state needs in the algorithm-feasibility batch |
| `Published OLAP snapshots` | `CLAIM-B06-003`, `CLAIM-B06-004`, `CLAIM-B06-007`, `CLAIM-B06-008` | `medium-high` | compare hot Arrow-style sidecars versus cold Parquet sidecars under the same generation catalog |
| `Projection Build Store` | `CLAIM-B06-006`, `CLAIM-B06-008`, `CLAIM-B06-013` | `medium` | tie sidecar publication and recovery into the existing Build Store recommendation from Batch 03 |
| `Strict holistic RAM` | `CLAIM-B06-001` through `CLAIM-B06-006`, `CLAIM-B06-010`, `CLAIM-B06-011` | `medium` | add measured estimate formulas for sidecars, vectors, spill, and decode buffers in the observability batch |
| `Neo4j-compatible API` | `CLAIM-B06-005`, `CLAIM-B06-011`, `CLAIM-B06-013` | `medium` | ensure planner, sidecar, and artifact choices do not narrow the already-bounded Neo4j/GDS front surface |

## Rejected-Alternative Note

Rejected for this batch:

- `Store full OLAP topology twice: once as CSR and once as a generic columnar or relational table layer.`
- `Adopt DataFusion, Ladybug, Kuzu, Memgraph, FalkorDB, or AGE as the v003 architecture wholesale.`

Why rejected:

- The PRD is already leaning toward published immutable OLAP snapshots and a
  strict RAM contract, not a second fully general serving store.
- DataFusion’s strongest lessons here are planner interfaces and spill
  discipline, not product identity.
- Ladybug and Kuzu are useful because they show compact graph execution shapes,
  but they do not erase the Neo4j compatibility burden already documented in
  Batch 05.
- Memgraph, FalkorDB, and AGE each prove something real, but each also drifts
  away from at least one core v003 boundary: RAM-first, CSR-first topology,
  or Neo4j-shaped OLTP.

What would overturn this rejection:

- A later batch proves that one of these systems’ core physical shapes fits the
  full GDS family matrix better than flat CSR plus typed sidecars without
  violating the 50GB-on-8GB promise.

## Skeptical Review

| challenge | response |
| --- | --- |
| Aren’t Arrow and Parquet just two more formats to maintain? | Yes, which is why the recommendation is not “persist everything twice.” The narrower recommendation is hot-sidecar versus cold-sidecar packaging, both attached to one topology baseline. |
| Could Parquet page-cache behavior quietly destroy the RAM story? | Yes. That is why this batch treats Parquet as mainly for cold columns and requires later measured page-cache accounting before any strong claim. |
| Are you smuggling a query engine into the architecture through DataFusion ideas? | No. The adoption is limited to catalog snapshots, pushdown contracts, plan explanation, and spill discipline. |
| Is Ladybug proof that Parquet should also hold topology? | No. Ladybug proves immutable graph snapshots are workable; it does not force v003 to store topology in Parquet rather than its existing flat CSR-style binaries. |
| Is Kuzu close enough that we should just copy it? | Not yet. Kuzu is the strongest compact competitor precedent here, but v003 still has a different compatibility and publication burden. |
| Does the competitor study shrink the Neo4j/GDS surface to what is easy? | No. This batch is explicitly downstream of the public-surface inventory and compatibility boundary, and all storage claims remain subordinate to that front-door contract. |

## Verification Commands Run

```bash
git -C gitrefrepo/apache-arrow-rs-src rev-parse --short HEAD
git -C gitrefrepo/apache-parquet-format-src rev-parse --short HEAD
git -C gitrefrepo/apache-datafusion-src rev-parse --short HEAD
git -C gitrefrepo/ladybug-src rev-parse --short HEAD
git -C gitrefrepo/kuzu-src rev-parse --short HEAD
git -C gitrefrepo/memgraph-src rev-parse --short HEAD
git -C gitrefrepo/falkordb-src rev-parse --short HEAD
git -C gitrefrepo/age-src rev-parse --short HEAD
sed -n '1,120p' /tmp/codex-code-intel/batch06-subpaths/batch06-subpaths-summary-20260624-133816.tsv
nl -ba gitrefrepo/apache-arrow-rs-src/arrow-array/src/array/dictionary_array.rs | sed -n '156,176p'
nl -ba gitrefrepo/apache-arrow-rs-src/arrow-array/src/array/fixed_size_list_array.rs | sed -n '39,62p'
nl -ba gitrefrepo/apache-arrow-rs-src/arrow-array/src/array/fixed_size_list_array.rs | sed -n '324,362p'
nl -ba gitrefrepo/apache-arrow-rs-src/arrow-buffer/src/buffer/null.rs | sed -n '34,64p'
nl -ba gitrefrepo/apache-arrow-rs-src/arrow-buffer/src/buffer/null.rs | sed -n '110,132p'
nl -ba gitrefrepo/apache-arrow-rs-src/arrow-ipc/src/reader.rs | sed -n '748,758p'
nl -ba gitrefrepo/apache-arrow-rs-src/arrow-ipc/src/reader.rs | sed -n '944,953p'
nl -ba gitrefrepo/apache-arrow-rs-src/arrow-ipc/src/reader.rs | sed -n '1318,1326p'
nl -ba gitrefrepo/apache-parquet-format-src/README.md | sed -n '166,181p'
nl -ba gitrefrepo/apache-parquet-format-src/README.md | sed -n '207,221p'
nl -ba gitrefrepo/apache-parquet-format-src/Encodings.md | sed -n '75,110p'
nl -ba gitrefrepo/apache-parquet-format-src/src/main/thrift/parquet.thrift | sed -n '284,291p'
nl -ba gitrefrepo/apache-parquet-format-src/src/main/thrift/parquet.thrift | sed -n '918,945p'
nl -ba gitrefrepo/apache-parquet-format-src/src/main/thrift/parquet.thrift | sed -n '1293,1302p'
nl -ba gitrefrepo/apache-datafusion-src/datafusion/catalog/src/catalog.rs | sed -n '35,90p'
nl -ba gitrefrepo/apache-datafusion-src/datafusion/expr/src/table_source.rs | sed -n '94,123p'
nl -ba gitrefrepo/apache-datafusion-src/datafusion/optimizer/src/optimizer.rs | sed -n '74,90p'
nl -ba gitrefrepo/apache-datafusion-src/datafusion/optimizer/src/optimizer.rs | sed -n '255,320p'
nl -ba gitrefrepo/apache-datafusion-src/datafusion/physical-plan/src/display.rs | sed -n '116,174p'
nl -ba gitrefrepo/apache-datafusion-src/datafusion/execution/src/memory_pool/mod.rs | sed -n '74,108p'
nl -ba gitrefrepo/apache-datafusion-src/datafusion/execution/src/disk_manager.rs | sed -n '160,190p'
nl -ba gitrefrepo/apache-datafusion-src/datafusion/datasource-parquet/src/row_group_filter.rs | sed -n '248,327p'
nl -ba gitrefrepo/apache-datafusion-src/datafusion/datasource-parquet/src/page_filter.rs | sed -n '404,446p'
nl -ba gitrefrepo/ladybug-src/docs/icebug-disk.md | sed -n '1,90p'
nl -ba gitrefrepo/ladybug-src/docs/index_build_recovery.md | sed -n '1,90p'
nl -ba gitrefrepo/ladybug-src/docs/morsel_parallelism.md | sed -n '1,150p'
nl -ba gitrefrepo/ladybug-src/src/include/storage/wal/wal.h | sed -n '1,220p'
nl -ba gitrefrepo/ladybug-src/src/include/storage/checkpointer.h | sed -n '1,220p'
nl -ba gitrefrepo/ladybug-src/src/common/arrow/arrow_array_scan.cpp | sed -n '1,240p'
nl -ba gitrefrepo/ladybug-src/src/optimizer/filter_push_down_optimizer.cpp | sed -n '37,220p'
nl -ba gitrefrepo/ladybug-src/src/optimizer/projection_push_down_optimizer.cpp | sed -n '34,220p'
nl -ba gitrefrepo/ladybug-src/src/optimizer/factorization_rewriter.cpp | sed -n '30,188p'
nl -ba gitrefrepo/ladybug-src/src/processor/operator/arrow_result_collector.cpp | sed -n '75,177p'
nl -ba gitrefrepo/kuzu-src/README.md | sed -n '18,35p'
nl -ba gitrefrepo/kuzu-src/src/transaction/transaction.cpp | sed -n '30,90p'
nl -ba gitrefrepo/kuzu-src/src/planner/join_order/cardinality_estimator.cpp | sed -n '38,60p'
nl -ba gitrefrepo/memgraph-src/README.md | sed -n '36,82p'
nl -ba gitrefrepo/memgraph-src/src/replication_handler/replication_handler.cpp | sed -n '60,103p'
nl -ba gitrefrepo/memgraph-src/src/replication_handler/replication_handler.cpp | sed -n '210,220p'
nl -ba gitrefrepo/falkordb-src/README.md | sed -n '39,60p'
nl -ba gitrefrepo/falkordb-src/tests/flow/index_utils.py | sed -n '23,95p'
nl -ba gitrefrepo/age-src/README.md | sed -n '57,95p'
nl -ba gitrefrepo/age-src/README.md | sed -n '220,238p'
nl -ba gitrefrepo/age-src/src/backend/commands/graph_commands.c | sed -n '37,120p'
rg -n "apache-parquet-format-src|kuzu-src|memgraph-src|falkordb-src|age-src|apache-arrow-rs-src|apache-datafusion-src|ladybug-src" docs_PRD03/reference-learning/Reference-Shelf-Graph-Evidence-Ledger.md
```

## Checkpoint: capability+architecture+execution+rejection / sidecars-planner-compact-competitors / 2026-06-24

Assigned requirement IDs:

- `REQ-LEARN-012.0`
- `REQ-LEARN-013.0`
- `REQ-LEARN-014.0`
- `REQ-LEARN-019.0`
- `REQ-LEARN-034.0`
- `REQ-LEARN-035.0`
- `REQ-LEARN-036.0`
- `REQ-LEARN-037.0`
- `REQ-LEARN-038.0`
- `REQ-LEARN-040.0`
- `REQ-LEARN-041.0`
- `REQ-LEARN-049.0`
- `REQ-LEARN-050.0`
- `REQ-LEARN-051.0`
- `REQ-LEARN-053.0`

Evidence rows completed:

- `13`

Most important sourced facts:

- Arrow gives strong precedents for dictionary sidecars, fixed-dimension vector
  layout, explicit null buffers, and mmap-friendly zero-copy reads.
- Parquet gives strong precedents for cold-column encoding, null counts,
  dictionary pages, and page/row-group skipping.
- DataFusion gives strong precedents for metadata snapshots, pushdown
  declarations, plan explanation, and explicit spill contracts.
- Ladybug proves immutable Parquet+CSR graph snapshots, recovery-aware build
  publication, morsel execution, and Arrow result-sidecar handling.
- Kuzu is the closest compact competitor fit for RAM-first graph execution.
- Memgraph, FalkorDB, and AGE are useful contrasts, but not direct v003 storage
  templates.

Architecture implications:

- `Adopt`: flat CSR plus typed sidecars as the default physical direction.
- `Adopt`: hot-sidecar versus cold-sidecar distinction instead of one universal
  sidecar encoding.
- `Adapt`: pushdown, plan explanation, factorization, and morsel execution as
  execution layers above the snapshot substrate.
- `Reject`: full-topology duplication and wholesale engine adoption from
  competitors.
- `Watch`: page-cache behavior, decode costs, and spill accounting still need
  benchmark and observability proof.

Unresolved risks:

- `Risk`: Parquet cold columns may still create hidden decode and page-cache
  costs large enough to weaken the laptop-scale RAM story.
  `Falsifier`: sidecar observability and benchmark batch shows these costs
  clearly bounded under target workloads.
- `Risk`: vectorized/factorized execution layers may add more complexity than
  they are worth for the first milestone.
  `Falsifier`: a direct flat-CSR-plus-minimal-sidecars execution slice matches
  the required GDS families acceptably without them.
