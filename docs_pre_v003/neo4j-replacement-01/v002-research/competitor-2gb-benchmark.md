# 2 GB Competitor Benchmark Matrix

This file is the tracked narrative view of the current `v003` 2 GB competitor study.

## Benchmark Contract

- fixed shared dataset: `artifacts/code_sparse_2gb`
- fixed query families: `forward_one`, `reverse_one`, `reverse_two`
- source of truth: CSV truth evaluator in `benchmarks/walk_hopper_v1`
- baseline: Knight Bus Rust
- machine: `macOS-14.6-arm64-arm-64bit-Mach-O` / `arm64` / `16.00 GB` RAM

## Dataset Shape

- raw bytes: `2.04 GB`
- nodes: `3997988`
- edges: `36294270`
- snapshot size: `490.4 MB`
- query corpus rows: `60`

## Knight Bus Baseline

| Engine | Status | Open | p50 | p95 | p99 | Mean | Runtime RSS | Version |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Knight Bus Rust | ok | 269.6 ms | 11.5 us | 84.3 us | 190.0 us | 26.8 us | 223.1 MB | snapshot-v2 |

## Competitor Status Matrix

| Backend | Query layer | Status | Same answers | Import | Open | p50 | p95 | p99 | Runtime RSS | Version | Reason |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| neo4j | cypher | ok | yes | 112.30 s | 60.9 ms | 1.29 s | 1.63 s | 1.78 s | 1.08 GB | 2026.03.1 |  |
| memgraph | cypher | unsupported | not checked | n/a | n/a | n/a | n/a | n/a | n/a | eb4f6a9 | Memgraph adapter not wired in the local v003 harness |
| kuzu | cypher | unsupported | not checked | n/a | n/a | n/a | n/a | n/a | n/a | 89f0263 | Kuzu adapter not wired in the local v003 harness |
| falkordb | opencypher | unsupported | not checked | n/a | n/a | n/a | n/a | n/a | n/a | 23e32b0 | FalkorDB adapter not wired in the local v003 harness |
| hugegraph | opencypher | unsupported | not checked | n/a | n/a | n/a | n/a | n/a | n/a | 836b348 | HugeGraph adapter not wired in the local v003 harness |
| apache-age | opencypher | unsupported | not checked | n/a | n/a | n/a | n/a | n/a | n/a | 774e781 | Apache AGE adapter not wired in the local v003 harness |
| janusgraph | gremlin | unsupported | not checked | n/a | n/a | n/a | n/a | n/a | n/a | 346f5a4 | JanusGraph adapter not wired in the local v003 harness |
| dgraph | dql | unsupported | not checked | n/a | n/a | n/a | n/a | n/a | n/a | 2da01c5 | Dgraph adapter not wired in the local v003 harness |

## Per-Backend Notes

### neo4j

- status: `ok`
- query language: `cypher`
- same answers: `yes`
- reason: `none`
- source note: `none`

### memgraph

- status: `unsupported`
- query language: `cypher`
- same answers: `not checked`
- reason: `Memgraph adapter not wired in the local v003 harness`
- source note: `v003 adapter placeholder: semantics mapped, local runtime integration not yet implemented`

### kuzu

- status: `unsupported`
- query language: `cypher`
- same answers: `not checked`
- reason: `Kuzu adapter not wired in the local v003 harness`
- source note: `v003 adapter placeholder: semantics mapped, local runtime integration not yet implemented`

### falkordb

- status: `unsupported`
- query language: `opencypher`
- same answers: `not checked`
- reason: `FalkorDB adapter not wired in the local v003 harness`
- source note: `v003 adapter placeholder: semantics mapped, local runtime integration not yet implemented`

### hugegraph

- status: `unsupported`
- query language: `opencypher`
- same answers: `not checked`
- reason: `HugeGraph adapter not wired in the local v003 harness`
- source note: `v003 adapter placeholder: semantics mapped, local runtime integration not yet implemented`

### apache-age

- status: `unsupported`
- query language: `opencypher`
- same answers: `not checked`
- reason: `Apache AGE adapter not wired in the local v003 harness`
- source note: `v003 adapter placeholder: semantics mapped, local runtime integration not yet implemented`

### janusgraph

- status: `unsupported`
- query language: `gremlin`
- same answers: `not checked`
- reason: `JanusGraph adapter not wired in the local v003 harness`
- source note: `v003 adapter placeholder: semantics mapped, local runtime integration not yet implemented`

### dgraph

- status: `unsupported`
- query language: `dql`
- same answers: `not checked`
- reason: `Dgraph adapter not wired in the local v003 harness`
- source note: `v003 adapter placeholder: semantics mapped, local runtime integration not yet implemented`

## Raw Artifacts

- machine-readable summary: `reports/v003_2gb_competitors/summary.json`
- per-backend raw reports: `reports/v003_2gb_competitors/<backend>/report.json`
- raw input dataset: `artifacts/code_sparse_2gb/...`
