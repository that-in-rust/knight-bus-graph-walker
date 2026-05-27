# v003 Research

This folder records the fixed `2 GB` competitor matrix for Knight Bus.

## What This Matrix Proves

- Knight Bus completed the fixed `2 GB` graph corpus on the current machine.
- Knight Bus query p99 on this corpus was `190.0 us` with runtime RSS `223.1 MB`.
- Live external backend results are currently available for `neo4j`.
- The remaining backends are still listed with honest `unsupported` status instead of being silently omitted: `memgraph`, `kuzu`, `falkordb`, `hugegraph`, `apache-age`, `janusgraph`, `dgraph`.

## Current Dataset Contract

- graph model: `code_sparse`
- raw dataset size: `2.04 GB`
- node count: `3997988`
- edge count: `36294270`
- query corpus size: `60`

## Current Backend Status

| Backend | Status | Same answers | p99 | Runtime RSS | Reason |
| --- | --- | --- | --- | --- | --- |
| Knight Bus Rust | ok | verifier | 190.0 us | 223.1 MB |  |
| neo4j | ok | yes | 1.78 s | 1.08 GB |  |
| memgraph | unsupported | not checked | n/a | n/a | Memgraph adapter not wired in the local v003 harness |
| kuzu | unsupported | not checked | n/a | n/a | Kuzu adapter not wired in the local v003 harness |
| falkordb | unsupported | not checked | n/a | n/a | FalkorDB adapter not wired in the local v003 harness |
| hugegraph | unsupported | not checked | n/a | n/a | HugeGraph adapter not wired in the local v003 harness |
| apache-age | unsupported | not checked | n/a | n/a | Apache AGE adapter not wired in the local v003 harness |
| janusgraph | unsupported | not checked | n/a | n/a | JanusGraph adapter not wired in the local v003 harness |
| dgraph | unsupported | not checked | n/a | n/a | Dgraph adapter not wired in the local v003 harness |

## Exact Records

- [Competitor Matrix](./competitor-2gb-benchmark.md)
- ignored raw reports: `reports/v003_2gb_competitors/...`
- ignored dataset inputs: `artifacts/code_sparse_2gb/...`
