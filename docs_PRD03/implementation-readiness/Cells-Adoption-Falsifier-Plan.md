# Cells Adoption Falsifier Plan

Cells or Tilehouse are not a religion. They are a hypothesis: partitioned CSR
payloads may improve update locality, reader locality, compaction, and RAM
budgeting without breaking global-stream execution. This plan defines when to
adopt, postpone, or reject that hypothesis.

## PRD Plane

| plane | cells relevance |
| --- | --- |
| OLTP storage | no change; OLTP remains Neo4j-shaped |
| Projection Build Store | may use dirty-region metadata and cell-local rebuild jobs |
| OLAP snapshot storage | may package canonical CSR as bounded cells plus global stream adapter |

## Architecture Options

| option | shape | keep if | reject if |
| --- | --- | --- | --- |
| FlatDualCSR | one canonical immutable dual CSR generation | full-graph algorithms dominate and freshness lag is acceptable | update-local rebuilds become product-critical |
| FlatCSRPlusDeltaSidecars | flat generation plus bounded sidecar/delta overlays | small updates are rare and can wait for rebuild | overlay RAM or query merge cost grows unbounded |
| CellularCSR | per-cell dual CSR payloads plus boundary indexes and global stream | local queries, dirty-region rebuilds, and compaction budgets improve materially | boundary-edge ratio and metadata overhead erase locality benefits |
| BuildStoreOnlyIncremental | keep flat OLAP snapshot but make Build Store smarter | full rebuild can be made cheap enough | publish latency or scratch memory violates PRD target |

## Falsifier Thresholds

| metric | adopt cells if | postpone cells if | reject cells if | evidence |
| --- | --- | --- | --- | --- |
| boundary-edge ratio | p95 projection has boundary-edge ratio below 15% or query locality gain offsets it | ratio unknown | boundary-edge ratio above 35% on target graphs with no locality win | NeedsBenchmark |
| dirty-region rebuild | 10K OLTP changes rebuild less than 5% of topology bytes for common workloads | fewer than weekly updates | dirty rebuild touches more than 40% of topology for ordinary small updates | NeedsBenchmark |
| page-cache churn | local GDS/catalog queries fault 2x less data than flat mmap on target hardware | no local query workload | page-cache churn is equal or worse after metadata overhead | NeedsBenchmark |
| metadata overhead | passports, boundaries, and local maps stay below 5% of topology bytes | 5-15% with clear update win | metadata overhead exceeds 20% without update win | NeedsBenchmark |
| global stream regression | global PageRank/WCC stream slowdown below 10% versus flat CSR | 10-25% while update win is large | slowdown above 25% for P1 algorithms | NeedsBenchmark |
| compaction scratch | cell compaction peak scratch is below configured build budget | no compaction path yet | compaction still needs global scratch | NeedsBenchmark |

## Required Experiments

1. Build the same fixture as flat CSR and as candidate cell packaging.
2. Measure global PageRank, WCC, BFS, and triangle-count scan order.
3. Append 10, 10K, and 1M update receipts and measure rebuild bytes.
4. Measure page-cache residency or explicit-buffer reads for local queries.
5. Record boundary-edge ratio, opened cell count, metadata overhead, and stale-generation retention.

## Evidence Ledger

| claim_id | evidence_confidence | source_path | symbol_or_query | inference | falsifier |
| --- | --- | --- | --- | --- | --- |
| CELL-001 | DirectSource | `docs_PRD03/prd-l1.md:17-35` | OLAP snapshot W | cells cannot change OLTP boundary or query-time freshness model | PRD changes to live OLAP delta serving |
| CELL-002 | DirectSource | `docs_PRD03/reference-learning/Batch-01-Current-Seed-And-GDS-Baseline.md:71-74` | current flat mmap runtime and manifest | flat CSR remains oracle and global primitive | flat CSR fails baseline correctness |
| CELL-003 | Inference | `docs_PRD03/reference-learning/Architecture-Option-Scorecard.tsv` | architecture option scoring | cells are a falsifiable packaging choice, not default proof | benchmark thresholds reject cells |
| CELL-004 | NeedsBenchmark | this file | boundary-edge and metadata thresholds | adoption requires measured wins | experiments show no locality or compaction improvement |

## Verification Commands

```bash
rg -n "adopt|postpone|reject|boundary-edge|dirty-region|page-cache|metadata overhead" docs_PRD03/implementation-readiness/Cells-Adoption-Falsifier-Plan.md
rg -n "FlatDualCSR|Cellular|BuildStore" docs_PRD03/reference-learning/Architecture-Option-Scorecard.tsv docs_PRD03/reference-learning/Architecture-Fit-Matrix.tsv
```

