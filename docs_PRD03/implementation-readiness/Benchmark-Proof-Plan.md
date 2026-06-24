# Benchmark Proof Plan

This plan defines the proof needed before claiming v003 saves server cost or
makes 50GB-class graphs practical on 8GB-class machines.

## Systems Compared

| system | purpose |
| --- | --- |
| Neo4j Cypher | baseline transactional store and ad hoc Cypher traversal behavior |
| Neo4j GDS | fair OLAP baseline because it uses projected graph stores and GDS procedures |
| Knight Bus v003 | Rust OLTP plus published low-RAM OLAP snapshots and GDS-compatible surface |

## Benchmark Phases

| phase | metric | required output |
| --- | --- | --- |
| cold start | startup time, initial RSS, page faults | phase-scoped memory report |
| OLTP import/write | ingest time, WAL bytes, RSS, recovery correctness | OLTP correctness and receipt watermark |
| projection build | build time, scratch peak, spill bytes, page-cache effect | validation report and memory estimate delta |
| publication | publish latency, active-pointer atomicity, retained-generation bytes | generation state transition log |
| GDS estimate | estimate latency and estimated bytes | formula-book component breakdown |
| GDS execution | runtime, correctness, RSS, page cache, direct buffers, spill, artifact bytes | correctness oracle and measured memory report |
| mutation/writeback | mutate sidecar bytes, OLTP write counts, rollback behavior | catalog/artifact state and OLTP validation |
| cleanup | retention safety, disk reclaimed, reader-pin safety | no pinned reader broken |

## Workload Ladder

| tier | graph size | required procedures |
| --- | --- | --- |
| tiny oracle | less than 1MB | graph catalog, PageRank, BFS, WCC, SCC, triangle count, k-core, scaleProperties |
| medium regression | 1GB to 5GB | PageRank, BFS, WCC, Dijkstra, triangle count, nodeSimilarity sample |
| target proof | 50GB logical graph on 8GB machine | graph.project.estimate, projection build, PageRank, BFS, WCC, one hard-family rejection |
| stress | skewed high-degree graph | triangle count, BFS, PageRank, memory rejection and spill tests |

## Correctness Requirements

| surface | oracle |
| --- | --- |
| graph catalog | Neo4j GDS row shape and catalog lifecycle |
| PageRank | tiny numerical oracle plus Neo4j GDS comparison within tolerance |
| BFS/Dijkstra | path oracle and deterministic tie behavior where claimed |
| WCC/SCC/k-core | hand-labeled components/core fixture |
| triangle count | triangle/square/high-degree fixture with no double count |
| unsupported | stable known-unsupported error envelope |
| write/mutate | property or sidecar count equals row count and schema expectation |

## Memory Reporting Contract

Each run reports:

```text
memory_scope
estimated_total_bytes
estimated_heap_bytes
estimated_page_cache_policy
estimated_direct_buffer_bytes
estimated_topology_bytes
estimated_sidecar_bytes
estimated_result_artifact_bytes
estimated_model_artifact_bytes
estimated_scratch_bytes
estimated_spill_bytes
estimated_retained_generation_bytes
estimated_algorithm_state_bytes
measured_process_rss_bytes
measured_allocator_bytes
measured_page_faults
measured_spill_bytes
```

## Evidence Ledger

| claim_id | evidence_confidence | source_path | symbol_or_query | inference | falsifier |
| --- | --- | --- | --- | --- | --- |
| BENCH-001 | DirectSource | `docs_PRD03/reference-learning/Batch-09-Benchmarks-And-Observability.md:146-149` | current benchmark honesty and Graphalytics phases | v003 needs phase-scoped benchmark reporting | a single end-to-end wall-clock number proves enough |
| BENCH-002 | DirectSource | `docs_PRD03/reference-learning/Batch-09-Benchmarks-And-Observability.md:178-227` | memory_scope and measured fields | measured and estimated memory must be separate | RSS-only report is accepted as holistic proof |
| BENCH-003 | DirectSource | `docs_PRD03/prd-l1.md:68-87` | strict RAM and 50GB/8GB target | benchmark must include budget rejection, not just fast success | product stops claiming strict RAM |
| BENCH-004 | Inference | `docs_PRD03/reference-learning/GDS-Family-Oracle-Parity-Matrix.tsv` | oracle matrix | algorithm support should be gated by parity fixtures | implementation can ship without canary/oracle parity |

## Verification Commands

```bash
rg -n "Neo4j Cypher|Neo4j GDS|Knight Bus|cold|projection|publication|RSS|page cache|spill|correctness" docs_PRD03/implementation-readiness/Benchmark-Proof-Plan.md
rg -n "memory_scope|estimated|measured|Graphalytics" docs_PRD03/reference-learning/Batch-09-Benchmarks-And-Observability.md
```

