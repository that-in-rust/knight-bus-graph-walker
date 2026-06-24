# Batch 09: Benchmarks, Observability, And Graph-Vector Market Watch

Date: 2026-06-24

Assigned lanes:

- `Benchmark lane`
- `Observability lane`
- `Memory-honesty lane`
- `Market-watch lane`

Assigned PRD outcomes:

- `Honest benchmark and RAM-claim discipline`
- `Measured-versus-estimated memory contract for v003`
- `Current Knight Bus benchmark truth-scoping`
- `Future graph-vector/full-text sidecar awareness without P0 drift`

Requirement IDs touched in this batch:

- `REQ-LEARN-023.0`
- `REQ-LEARN-027.0`
- `REQ-LEARN-029.0`

## Answer First

This batch answers a different question from the earlier storage batches:
not "which topology layout is elegant?", but "what would make a 50GB-on-8GB
claim credible, reproducible, and falsifiable?"

The strongest conclusions are:

1. The current Knight Bus `v002` benchmark is a useful `runtime-process-only`
   seed, but it is not yet the full v003 memory contract. Its own journal is
   honest about that.
2. LDBC Graphalytics and LDBC SNB both show that benchmark credibility is a
   contract made of `named workload + validation rule + repeat count + run
   steps + failure classes + reproducible command`, not just a fast number.
3. v003 must report `measured memory` and `estimated memory` separately. The
   measured side needs at least:
   - process RSS,
   - allocator allocated/active/resident bytes when available,
   - page-cache or mmap exposure,
   - direct-I/O buffer bytes,
   - scratch/spill bytes,
   - sidecar bytes,
   - delta bytes,
   - algorithm-state bytes,
   - result/model artifact bytes,
   - and the phase being measured.
4. `tracing`, `jemalloc`, `DuckDB`, `ClickHouse`, `Ladybug`, and `Qdrant`
   together show a practical observability stack for this:
   structured spans, allocator introspection, memory hard limits, page-cache
   awareness, spill-aware tests, and optional profiler/export hooks.
5. The graph-vector watchlist is real, but it still does not justify changing
   the core v003 thesis. `Qdrant`, `Helix`, `NornicDB`, `Tantivy`, and Neo4j's
   own vector-index DDL mostly argue for a future `artifact/index plane`, not
   for replacing canonical graph topology at P0.

Short architecture thesis after this batch:

```text
The benchmark moat is not "faster numbers."
It is honest, repeatable, workload-named, phase-scoped evidence for what
memory is consumed, by which layer, under which algorithm, with which
validation rule.

The storage thesis survives this batch:
keep one canonical OLAP topology path,
add stronger telemetry and accounting,
and keep graph-vector/full-text capabilities in a later artifact/index tier
unless PRD03 explicitly expands P0.
```

## Scope

This batch studies three things together because they interact tightly:

1. `Benchmark contracts`
   - `README.md`
   - `Final-Testing-Journal-v002.md`
   - `ldbc_graphalytics-src`
   - `ldbc_graphalytics_docs-src`
   - `ldbc_graphalytics_platforms_graphblas-src`
   - `ldbc_snb_interactive_v1_driver-src`
   - `ldbc_snb_interactive_v1_impls-src`
   - `ldbc_snb_interactive_v2_driver-src`
   - `ldbc_snb_interactive_v2_impls-src`
2. `Memory and telemetry precedents`
   - `tracing-src`
   - `jemalloc-src`
   - `duckdb-src`
   - `clickhouse-src`
   - `ladybug-src`
   - relevant `neo4j-gds-src` prior estimator traces from earlier batches
3. `Graph-vector/full-text market watch`
   - `qdrant-src`
   - `helix-db-src`
   - `nornicdb-src`
   - `tantivy-src`
   - vector-index paths in `neo4j-src`

## Graph-Tool Execution For This Batch

This batch explicitly uses the two local graph-evidence skills required by the
learning spec:

- `/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/SKILL.md`
- `/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/SKILL.md`

The controlling evidence for reference-repo graph-tool coverage remains:
`Reference-Shelf-Graph-Evidence-Ledger.md` plus the machine-readable companion
`Reference-Shelf-Graph-Tool-Truthcheck.tsv`.

This batch also ran fresh current-repo smoke scans to keep Knight Bus itself in
the same evidence discipline:

- `CBM`: `knight-bus-graph-walker-20260624-153939`
- `CGC`: `knight-bus-graph-walker-20260624-154001`

| repo or folder | graph-tool status used in this batch | run evidence | why it mattered |
| --- | --- | --- | --- |
| current Knight Bus repo | fresh `CBM` + fresh `CGC` | `/tmp/codex-code-intel/codebase-memory/knight-bus-graph-walker-20260624-153939`, `/tmp/codex-code-intel/codegraphcontext/knight-bus-graph-walker-20260624-154001` | anchored benchmark-contract claims in the current workspace |
| `ldbc_graphalytics-src` | `CbmSemanticReadyCgcLowYield` | truthcheck row `ldbc_graphalytics-src-20260624-125742` | proves the benchmark core repo is structurally queryable enough for follow-up navigation |
| `ldbc_graphalytics_docs-src` | `GraphToolLowYield` | truthcheck row `SkippedDocsOnly` | text-first spec repo; graph index would add little |
| `ldbc_graphalytics_platforms_graphblas-src` | `DualSemanticReady` | truthcheck rows `...125824` and `...125825` | useful for both structure and direct file reads |
| `ldbc_snb_interactive_v1_driver-src` | `CbmSemanticReadyCgcLowYield` | truthcheck row `...125843` | validation-generation paths are code-bearing |
| `ldbc_snb_interactive_v1_impls-src` | `CbmSemanticReadyCgcLowYield` | truthcheck row `...125912` | implementation workload side is code-bearing |
| `ldbc_snb_interactive_v2_driver-src` | `CbmSemanticReadyCgcLowYield` | truthcheck row `...130014` | benchmark and validation behavior live here |
| `ldbc_snb_interactive_v2_impls-src` | `CbmSemanticReadyCgcLowYield` | truthcheck row `...130043` | workload handler side is code-bearing |
| `tracing-src` | `DualSemanticReady` | truthcheck row `...121526` | strongest tracing/structured-event precedent in this batch |
| `jemalloc-src` | `CbmSemanticReadyCgcLowYield` | truthcheck row `...125522` | allocator counters and epoch refresh paths |
| `duckdb-src` | `CbmSemanticReadyCgcLowYield` | truthcheck row `...124119` | memory usage info and default allocator sizing |
| `clickhouse-src` | `NeedsRerun` | truthcheck rows `...150515` and `...150516` | still useful as direct-source precedent, but not reusable as graph evidence |
| `ladybug-src` | `CbmSemanticReadyCgcLowYield` | truthcheck row `...121259` | spill and memory-usage tests |
| `neo4j-gds-src` | `CbmSemanticReadyCgcLowYield` | truthcheck row `...120618` | earlier estimator traces remain usable as first-party memory-shape evidence |
| `helix-db-src` | `CbmSemanticReadyCgcLowYield` | truthcheck row `...125346` | graph-plus-vector DSL expectations |
| `nornicdb-src` | `CbmSemanticReadyCgcLowYield` | truthcheck row `...130630` | hybrid search and Neo4j CSV compatibility |
| `qdrant-src` | `CbmSemanticReadyCgcLowYield` | truthcheck row `...130806` | vector/payload/hybrid-search and heap-profiler expectations |
| `tantivy-src` | `CbmSemanticReadyCgcLowYield` | truthcheck row `...131342` | full-text/mmap/incremental-indexing precedent |

## Evidence Ledger

| claim_id | source_path | symbol_or_query | sourced_fact | inference | speculation | PRD impact | skeptical note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `B09-001` | `README.md:9-17`, `README.md:64-90` | `v002`, `Measurement Contract` | The current repo explicitly frames `v002` as measuring the Rust walker process and the Neo4j server process separately, and it keeps build and verify costs separate from query-time RAM. | The current benchmark is intentionally scoped and honest, but it is not yet a holistic whole-system OLAP memory proof. | v003 can preserve this separation while adding more breakdown fields instead of replacing the contract wholesale. | Strengthens the need for phase-scoped memory reporting. | A clean runtime-only benchmark can still mislead readers if later docs over-generalize it to whole-server memory. |
| `B09-002` | `Final-Testing-Journal-v002.md:11-27`, `Final-Testing-Journal-v002.md:68-79` | `Measurement Contract`, `Knight Bus Phase Costs`, `Honest Notes` | The journal says Rust RSS is `runtime_process_only`, Neo4j RSS is `server_process_only`, and the Rust build/verify/runtime values all come from `getrusage_self`. It also says runtime-only walker RSS is not the whole operating picture. | v003 should inherit the honesty, but add more sources than `getrusage_self` alone. | A later v003 benchmark may combine RSS with allocator and page-cache counters in one report. | Directly informs the measured-versus-estimated memory contract. | `getrusage_self` still leaves allocator internals and mmap/page-cache exposure opaque. |
| `B09-003` | `gitrefrepo/ldbc_graphalytics-src/README.md:8-19`, `gitrefrepo/ldbc_graphalytics_docs-src/tex/process.tex:8-23`, `gitrefrepo/ldbc_graphalytics_docs-src/tex/process.tex:66-90` | `Graphalytics`, benchmark composition, competition benchmark | Graphalytics defines a benchmark as experiments, jobs, and repeated runs, and the competition suite uses six core algorithms `BFS`, `WCC`, `PR`, `CDLP`, `LCC`, `SSSP` across scale classes `S` through `2XL+` with fixed timeouts and three executions per algorithm. | Credible benchmark claims need named workload, scale class, timeout discipline, and repetition count. | Knight Bus could eventually publish a Graphalytics-shaped subset for OLAP procedures that fit its surface. | Raises the standard for any future public benchmark claim. | Graphalytics is an external benchmark contract; it does not by itself prove product-market-fit relevance for every Neo4j/GDS workload. |
| `B09-004` | `gitrefrepo/ldbc_graphalytics_docs-src/tex/process.tex:103-141`, `gitrefrepo/ldbc_graphalytics_docs-src/tex/process.tex:148-184`, `gitrefrepo/ldbc_graphalytics_docs-src/tex/process.tex:197-209` | execution flow, run flow, failure indication, result fields | Graphalytics makes `verify-setup`, `format-graph`, `load-graph`, `execute-run`, `delete-graph`, then `prepare/startup/run/validate/finalize/terminate/archive` explicit, names failure classes, and specifies result-report fields. | v003 benchmark docs should name which phase a memory number belongs to and which validation gate passed before the number was recorded. | A future Knight Bus benchmark harness could emit Graphalytics-like failure codes for build/publish/execute phases. | Strongly supports benchmark-phase labeling and failure taxonomy. | Adopting the entire Graphalytics harness would be overkill if only the reporting discipline is needed. |
| `B09-005` | `gitrefrepo/ldbc_graphalytics_platforms_graphblas-src/README.md:11-18`, `gitrefrepo/ldbc_graphalytics_platforms_graphblas-src/README.md:67-103` | project structure, `init.sh`, `run-benchmark.sh` | The GraphBLAS driver uses Java driver code, C/LAGraph algorithms, shell scripts, dense vertex relabeling, Matrix Market conversion, and configuration-driven benchmark-size selection. | Benchmarking often includes format-conversion steps that must not be silently merged into runtime query claims. | Knight Bus may eventually benchmark both `published snapshot ready` and `build-to-query` scenarios separately. | Reinforces format/build/runtime phase separation. | This driver is benchmark packaging, not a recommendation that v003 should store Matrix Market files. |
| `B09-006` | `gitrefrepo/ldbc_snb_interactive_v2_driver-src/README.md:6-9`, `gitrefrepo/ldbc_snb_interactive_v2_driver-src/README.md:21-29`, `gitrefrepo/ldbc_snb_interactive_v2_driver-src/README.md:49-52`, `gitrefrepo/ldbc_snb_interactive_v2_driver-src/scripts/README.md:3-45` | cross-validation, `simpleworkload`, audited runs, data conversion | The Interactive v2 driver explicitly includes cross-validation and benchmark execution, ships a `simpleworkload`, documents audited-run artifacts, and converts parquet event streams with DuckDB-backed scripts. | Transactional or mixed-workload benchmark claims also need a stated data-generation and conversion path. | If v003 later studies mixed OLTP/OLAP publication lag, SNB-style workload generation may become useful. | Strengthens reproducible workload and audit expectations. | SNB Interactive is not a direct OLAP benchmark for GDS procedures. |
| `B09-007` | `gitrefrepo/ldbc_snb_interactive_v1_driver-src/src/main/java/org/ldbcouncil/snb/driver/validation/ValidationParamsGenerator.java:3-8`, `...:42-55`, `...:77-147`, `gitrefrepo/ldbc_snb_interactive_v2_driver-src/src/main/java/org/ldbcouncil/snb/driver/validation/DbValidationResult.java:29-56`, `...:101-105`, `...:194-247` | `ValidationParamsGenerator`, `DbValidationResult` | Validation-param generation requires a loaded DB, a validation filter, and time-mapped operations; it executes operation handlers through a `ResultReporter.SimpleResultReporter`, and the validation result is successful only if missing handlers, execution failures, and incorrect results are all empty. | A realistic compatibility benchmark must report not only speed but also handler coverage and exact result correctness. | Knight Bus could later adopt a similar pass/fail summary format for partial GDS support tiers. | Supports correctness-before-performance discipline for procedure benchmarking. | This validates transactional operation correctness, not graph-analytic numerical tolerance behavior. |
| `B09-008` | `gitrefrepo/tracing-src/tracing-flame/src/lib.rs:177-207`, `gitrefrepo/tracing-src/tracing-journald/src/lib.rs:74-81` | `FlameLayer`, journald `Layer` | `tracing-flame` records span open/close events as folded flamegraph stacks with nanosecond counts between events, and `tracing-journald` emits `CODE_LINE`, `CODE_FILE`, `TARGET`, `SPAN_NAME`, and prefixed user fields. | v003 can attach benchmark phase, algorithm, snapshot generation, and memory-scope fields to spans without inventing a custom profiler format. | A future telemetry layer could export both local flame stacks and structured journald records during benchmark runs. | Gives a practical tracing substrate for benchmark phases and publication steps. | Tracing coverage only helps if the code is actually instrumented at the right boundaries. |
| `B09-009` | `gitrefrepo/jemalloc-src/src/jemalloc.c:1915-1999`, `gitrefrepo/jemalloc-src/src/stats.c:1963-1979`, `gitrefrepo/jemalloc-src/src/stats.c:2153-2174` | `je_mallctl`, `je_mallctlbymib`, `je_malloc_stats_print`, `stats.allocated`, `stats.active`, `stats.resident`, `epoch` | jemalloc exposes control and stats APIs, including allocator counters for `allocated`, `active`, and `resident`, and refreshes them via `mallctl("epoch", ...)`. | If Knight Bus adopts jemalloc or similar allocator telemetry, it can report allocator truth separately from OS RSS. | A future v003 build could offer an optional `--memory-report allocator` mode gated by allocator availability. | Provides the clearest precedent for measured allocator-internal memory breakdowns. | Allocator resident bytes still do not fully equal end-to-end server memory or page-cache effects. |
| `B09-010` | `gitrefrepo/duckdb-src/src/main/client_data.cpp:144-145`, `gitrefrepo/duckdb-src/src/main/database.cpp:502-517`, `gitrefrepo/duckdb-src/src/main/settings/custom_settings.cpp:90-149` | `GetMemoryUsageInfo`, default `BlockAllocator`, allocator flush thresholds | DuckDB exposes `GetMemoryUsageInfo()`, sets default maximum memory when not configured, and sizes the default block allocator to `system_available_memory * 8 / 10`, with explicit allocator flush-threshold settings. | v003 should expose memory-usage info as a structured API rather than leaving RAM reporting to external shell tooling alone. | A future OLAP snapshot/catalog command may expose a `SHOW MEMORY`-style structured view. | Supports structured memory reporting and explicit memory-budget defaults. | DuckDB’s memory model is not a graph engine model, so only the reporting discipline transfers cleanly. |
| `B09-011` | `gitrefrepo/clickhouse-src/programs/server/Server.cpp:2264-2324`, `gitrefrepo/clickhouse-src/programs/local/LocalServer.cpp:870-930` | `max_server_memory_usage`, `total_memory_tracker`, `page cache`, `MemoryWorker` | ClickHouse computes a memory hard limit from available RAM and a ratio, sets tracker metrics, separately caps merges/mutations, connects page cache to the tracker, and documents that `MemoryWorker` updates RSS, resizes page cache, and purges jemalloc dirty pages under pressure. | Holistic RAM accounting needs both logical algorithm-state math and a server process that reacts to real RSS/page-cache pressure. | A future v003 exact-RAM mode may need its own background watcher for RSS and dirty-page behavior. | Strong precedent for memory limits plus page-cache awareness. | The truthcheck shows `clickhouse-src` is still graph-tool low-yield for reuse, so this batch should only use direct file reads from it. |
| `B09-012` | `gitrefrepo/ladybug-src/test/storage/buffer_manager_test.cpp:35-53`, `gitrefrepo/ladybug-src/test/storage/buffer_manager_test.cpp:62-113` | `getUsedMemory`, `TestSpillToDiskMemoryUsage` | Ladybug tests assert stable memory usage across identical queries and inspect spill-to-disk memory release through `getUsedMemory()` and `memoryFreed` / `memoryNowEvictable`. | v003 should have memory tests that assert not just latency and correctness, but also that repeated queries and spill paths stay within expected budgets. | A future parity suite could include repeated-query and forced-spill memory assertions for OLAP procedures. | Adds test-shape precedent for memory claims. | Test-only precedents do not prove that the production runtime exposes the same counters publicly. |
| `B09-013` | `gitrefrepo/qdrant-src/README.md:22-24`, `gitrefrepo/qdrant-src/README.md:130-163`, `gitrefrepo/qdrant-src/docs/DEVELOPMENT.md:231-317` | vector database features, hybrid search, quantization, tracing, Pyroscope, heap profiling | Qdrant positions vectors plus payload filters and hybrid search as first-class, supports dense/sparse/multivector search, advertises quantization and on-disk storage, and documents optional tracing plus continuous CPU/heap profiling via Pyroscope and jemalloc. | Graph-vector systems increasingly expect both vector + payload query planning and profiler-grade observability. | If v003 ever adds vector sidecars or GraphRAG-facing procedures, it will likely need optional profiler hooks and a separate index/artifact plane. | Useful market-watch signal, but not a reason to mutate P0 topology. | Qdrant is a vector DB, not a Neo4j-compatible graph engine. |
| `B09-014` | `gitrefrepo/helix-db-src/helix-cli/src/commands/chef.rs:179-185`, `gitrefrepo/helix-db-src/helix-cli/src/commands/chef.rs:305-319` | `vectorSearchNodesWith`, `textSearchNodesWith`, semantic search | Helix exposes vector search and BM25-style text search in a graph DSL and recommends generating embeddings server-side while storing a same-model `embedding` array on each node. | A future Neo4j rewrite may need to separate graph topology from vector/text sidecars rather than trying to bake them into adjacency. | Helix-like graph + vector APIs could inform a later P2 feature set or plugin boundary. | Strengthens the case for typed sidecars and artifact/index planes. | This is still far from a Neo4j compatibility requirement today. |
| `B09-015` | `gitrefrepo/nornicdb-src/pkg/nornicgrpc/gen/nornicdb_search_grpc.pb.go:29-37`, `...:62-70`, `gitrefrepo/nornicdb-src/pkg/adminimport/neo4j_csv.go:18-27`, `...:77-123`, `...:134-156` | `SearchText`, Neo4j CSV export/import | NornicDB exposes hybrid search that mixes vector and BM25 when embeddings are enabled, and it also has Neo4j-compatible CSV import/export and schema export helpers. | There is real market pressure to combine graph-adjacent search with Neo4j-shaped interchange formats. | If the PRD expands toward migration or GraphRAG, Nornic-like bridges become more relevant. | Keeps graph-vector watch connected to migration/interchange, not just search hype. | The existence of CSV import/export does not prove semantic Neo4j compatibility. |
| `B09-016` | `gitrefrepo/tantivy-src/README.md:11-18`, `gitrefrepo/tantivy-src/README.md:31-52`, `gitrefrepo/tantivy-src/README.md:134-144` | full-text engine, `BM25`, `Mmap directory`, immutable docs | Tantivy is a Rust full-text library with BM25, mmap-backed storage, fast fields, compressed document store, and immutable document updates via delete-and-reindex plus commit/reload. | If v003 adds a full-text sidecar or secondary search plane, immutable segment-style indexes are a more plausible fit than mutable graph-topology rewrites. | A later search/artifact tier might share lifecycle ideas with Tantivy-style commit/reload. | Supports later full-text sidecars without disturbing core OLAP topology. | Full-text relevance and graph traversal semantics are still different problem classes. |
| `B09-017` | `gitrefrepo/neo4j-src/community/cypher/front-end/ast/src/main/scala/org/neo4j/cypher/internal/ast/ShowIndexTypes.scala:49-52`, `gitrefrepo/neo4j-src/community/cypher/front-end/ast/src/main/scala/org/neo4j/cypher/internal/ast/CreateIndexTypes.scala:67-72` | `VectorIndexes`, `VectorCreateIndex` | Neo4j’s Cypher AST already includes vector index show/create types with node and relationship variants. | Full Neo4j compatibility eventually intersects vector-index DDL even if initial GDS storage work does not depend on it. | A later compatibility tier may need to parse or stub vector-index DDL before implementing vector execution. | Keeps the market-watch lane tied back to first-party Neo4j evolution. | The presence of AST types does not prove current user demand or immediate v003 necessity. |

## Benchmark Credibility Contract For v003

Every public benchmark or PMF-cost claim should include all of the following:

| field | why it is mandatory | minimum shape |
| --- | --- | --- |
| `workload_name` | prevents "generic graph benchmark" vagueness | `walk-corpus-v002`, `graphalytics-bfs-s`, `gds.pageRank.stream-fixture` |
| `algorithm_or_query_family` | ties cost to actual user-visible work | `BFS`, `PageRank`, `triangleCount`, `KNN`, `2-hop walk` |
| `dataset_identity` | avoids hidden regeneration or cherry-picked data | named corpus, snapshot generation, or public LDBC dataset |
| `scale_or_size_class` | makes cost comparable | bytes, node/edge count, or `S/M/L/XL/2XL+` |
| `validation_rule` | speed without correctness is meaningless | parity corpus, oracle output, or accepted numerical tolerance |
| `repeat_count` | reduces noise and benchmark theater | explicit `N`, e.g. `3`, `10`, or `fixed corpus rows` |
| `command_or_harness` | makes reruns possible | exact shell or script entry point |
| `phase_scope` | avoids mixing build and execute cost | `build`, `publish`, `load`, `run`, `validate`, `compact` |
| `memory_scope` | avoids "RSS" ambiguity | `runtime_process_only`, `server_process_only`, `whole_pipeline`, or explicit breakdown |
| `failure_mode_policy` | keeps timeouts and bad outputs visible | named fail codes or pass/fail conditions |
| `baseline_class` | prevents unfair comparisons | `Neo4j Cypher OLTP`, `Neo4j + GDS projection`, or another named baseline |

Rules:

- No claim counts as a serious RAM claim unless it names `phase_scope`.
- No claim counts as a serious throughput or latency claim unless it names
  `validation_rule`.
- No claim counts as reproducible unless it names a rerunnable command.
- No whole-system memory claim may be inferred from `runtime_process_only`
  results.

## Measured Versus Estimated Memory Contract

v003 should emit `estimated` and `measured` memory separately because they serve
different purposes.

### Estimated fields

These are planner- or procedure-facing:

| field | meaning |
| --- | --- |
| `topology_bytes` | canonical adjacency or logical projection bytes |
| `property_plane_bytes` | typed property or label/type sidecar bytes |
| `algorithm_state_bytes` | vectors, frontiers, heaps, counters, community arrays |
| `scratch_bytes` | temporary work arrays, tapes, contraction buffers |
| `spill_reserve_bytes` | disk-backed intermediate reserve or direct spill plan |
| `delta_overlay_bytes` | un-compacted freshness overlay bytes |
| `result_artifact_bytes` | mutate/write sidecar output bytes |
| `model_artifact_bytes` | embedding/model/pipeline artifact bytes |
| `direct_io_buffer_bytes` | explicit I/O buffers for strict-RAM plans |

### Measured fields

These are runtime-reporting fields:

| field | source precedent | why it matters |
| --- | --- | --- |
| `process_rss_bytes` | current Knight Bus journals, ClickHouse `MemoryWorker` | still the simplest external truth |
| `allocator_allocated_bytes` | jemalloc `stats.allocated` | shows active requested allocator memory |
| `allocator_active_bytes` | jemalloc `stats.active` | shows allocator page backing above allocated |
| `allocator_resident_bytes` | jemalloc `stats.resident` | shows allocator-resident footprint |
| `page_cache_bytes_explicit` | ClickHouse page-cache-aware tracking, DuckDB memory info | needed when the runtime owns or sizes page cache |
| `mmap_or_page_cache_exposure` | current mmap-based Knight Bus notes | needed when OS-resident pages are part of reality even if not allocated by Rust |
| `direct_io_buffer_bytes` | strict-RAM execution profile | shows what the runtime explicitly owns |
| `scratch_bytes_live` | Ladybug-style used-memory tests | catches compaction and contraction spikes |
| `sidecar_bytes_live` | columnar/property/index planes | prevents topology-only optimism |
| `delta_overlay_bytes_live` | freshness overlay | prevents hidden update-memory creep |
| `result_or_model_artifact_bytes_live` | KNN/Node2Vec/pipeline families | keeps non-topology outputs honest |
| `phase_name` | Graphalytics-style step naming | prevents cross-phase mixing |
| `measurement_source` | `getrusage_self`, allocator API, internal counters, OS probe | tells readers what the number actually means |

## Architecture Fit After This Batch

| question | answer after this batch | effect on storage thesis |
| --- | --- | --- |
| Is canonical topology weakened? | No. | The batch changes reporting discipline more than storage choice. |
| Is a typed sidecar plane strengthened? | Yes. | Vector/full-text/property planes remain out-of-topology and should stay that way. |
| Is a result/model artifact plane strengthened? | Yes. | Hard families and vector/full-text watch paths both reinforce it. |
| Do cells become mandatory because of this batch? | No. | Benchmarks and telemetry do not force cells; they force scope and budget honesty. |
| Does GraphBLAS become the default answer? | No. | GraphBLAS remains one execution substrate, not the benchmark-reporting substrate. |
| Does vector search become P0? | No. | Keep it in watchlist / later tier unless PRD03 expands. |

## P0 / P1 / P2 Outcome

| tier | what this batch says belongs there |
| --- | --- |
| `P0` | benchmark contract, phase labeling, memory-scoping rules, measured-versus-estimated reporting, strict validation naming |
| `P1` | richer allocator telemetry, background memory watcher behavior, sidecar- and artifact-plane counters in procedures and benchmark reports |
| `P2` | vector/full-text index plane, GraphRAG-facing hybrid search, vector-index DDL parity beyond parser or stub support |

## Requirement Impact

| requirement | effect of this artifact |
| --- | --- |
| `REQ-LEARN-023.0` | satisfied for this batch scope: Graphalytics and SNB benchmark contracts, validation steps, repeatability expectations, and reporting fields are now source-backed. |
| `REQ-LEARN-027.0` | satisfied for this batch scope: the observability lane now has allocator, tracing, page-cache, spill, and profiler precedents tied to a concrete v003 memory contract. |
| `REQ-LEARN-029.0` | satisfied for this batch scope: graph-vector/full-text watch findings are recorded, bounded, and explicitly kept out of P0 storage conclusions unless PRD03 changes. |

## Verification Log

Commands and checks used in this batch:

```bash
sed -n '1,220p' README.md
sed -n '1,220p' Final-Testing-Journal-v002.md
nl -ba gitrefrepo/ldbc_graphalytics-src/README.md | sed -n '1,80p'
nl -ba gitrefrepo/ldbc_graphalytics_docs-src/tex/process.tex | sed -n '1,220p'
nl -ba gitrefrepo/ldbc_graphalytics_platforms_graphblas-src/README.md | sed -n '1,140p'
nl -ba gitrefrepo/ldbc_snb_interactive_v2_driver-src/README.md | sed -n '1,120p'
nl -ba gitrefrepo/ldbc_snb_interactive_v2_driver-src/scripts/README.md | sed -n '1,120p'
nl -ba gitrefrepo/ldbc_snb_interactive_v1_driver-src/src/main/java/org/ldbcouncil/snb/driver/validation/ValidationParamsGenerator.java | sed -n '1,220p'
nl -ba gitrefrepo/ldbc_snb_interactive_v2_driver-src/src/main/java/org/ldbcouncil/snb/driver/validation/DbValidationResult.java | sed -n '1,260p'
nl -ba gitrefrepo/tracing-src/tracing-flame/src/lib.rs | sed -n '170,240p'
nl -ba gitrefrepo/tracing-src/tracing-journald/src/lib.rs | sed -n '70,110p'
nl -ba gitrefrepo/jemalloc-src/src/jemalloc.c | sed -n '1910,2005p'
nl -ba gitrefrepo/jemalloc-src/src/stats.c | sed -n '1960,1985p'
nl -ba gitrefrepo/jemalloc-src/src/stats.c | sed -n '2148,2175p'
nl -ba gitrefrepo/duckdb-src/src/main/client_data.cpp | sed -n '130,180p'
nl -ba gitrefrepo/duckdb-src/src/main/database.cpp | sed -n '500,535p'
nl -ba gitrefrepo/duckdb-src/src/main/settings/custom_settings.cpp | sed -n '90,170p'
nl -ba gitrefrepo/clickhouse-src/programs/server/Server.cpp | sed -n '2260,2325p'
nl -ba gitrefrepo/clickhouse-src/programs/local/LocalServer.cpp | sed -n '870,930p'
nl -ba gitrefrepo/ladybug-src/test/storage/buffer_manager_test.cpp | sed -n '1,120p'
nl -ba gitrefrepo/qdrant-src/README.md | sed -n '1,220p'
nl -ba gitrefrepo/qdrant-src/docs/DEVELOPMENT.md | sed -n '231,318p'
nl -ba gitrefrepo/helix-db-src/helix-cli/src/commands/chef.rs | sed -n '178,188p'
nl -ba gitrefrepo/helix-db-src/helix-cli/src/commands/chef.rs | sed -n '300,320p'
nl -ba gitrefrepo/nornicdb-src/pkg/nornicgrpc/gen/nornicdb_search_grpc.pb.go | sed -n '20,80p'
nl -ba gitrefrepo/nornicdb-src/pkg/adminimport/neo4j_csv.go | sed -n '1,160p'
nl -ba gitrefrepo/tantivy-src/README.md | sed -n '1,220p'
nl -ba gitrefrepo/neo4j-src/community/cypher/front-end/ast/src/main/scala/org/neo4j/cypher/internal/ast/ShowIndexTypes.scala | sed -n '40,60p'
nl -ba gitrefrepo/neo4j-src/community/cypher/front-end/ast/src/main/scala/org/neo4j/cypher/internal/ast/CreateIndexTypes.scala | sed -n '60,80p'
rg -n "^(ldbc_graphalytics-src|ldbc_graphalytics_docs-src|ldbc_graphalytics_platforms_graphblas-src|ldbc_snb_interactive_v1_driver-src|ldbc_snb_interactive_v1_impls-src|ldbc_snb_interactive_v2_driver-src|ldbc_snb_interactive_v2_impls-src|tracing-src|jemalloc-src|neo4j-gds-src|duckdb-src|clickhouse-src|ladybug-src|helix-db-src|nornicdb-src|qdrant-src|tantivy-src)\t" docs_PRD03/reference-learning/Reference-Shelf-Graph-Tool-Truthcheck.tsv
```

## Checkpoint Summary

What this batch completed:

- benchmark claims are now tied to named workload and validation discipline;
- the current Knight Bus `v002` memory scope is now explicitly bounded instead
  of being left as an implicit whole-system proxy;
- a concrete v003 measured-versus-estimated memory contract now exists;
- graph-vector/full-text market pressure is now documented without distorting
  the P0 storage thesis.

What this batch did not settle:

- the remaining long-tail GDS support-tier mapping;
- deeper first-party `neo4j-gds-src` graph-store/projection mechanics beyond
  the estimator and family traces already captured;
- the concrete fixture/parity matrix for all remaining algorithm families.

The active goal therefore remains incomplete, but it is now blocked by a much
smaller and more honest remainder: `support tiers`, `projection internals`,
and `oracle/parity completion`, not vague "more research".
