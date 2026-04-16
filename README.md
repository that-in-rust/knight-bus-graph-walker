# Knight Bus Graph Walker v002

`v002` is the corrected benchmark release for Knight Bus Graph Walker.

The point of `v002` is simple:

- keep the Rust runtime in the immutable dual-CSR + mmap shape
- measure Rust walker memory as `runtime_process_only`
- measure Neo4j memory as `server_process_only`
- prove Rust correctness separately through `verify`
- reuse the fixed `1 MB`, `50 MB`, and `2 GB` datasets without regenerating them

## What v002 Says

Knight Bus Rust is dramatically faster on walk latency across all three datasets, and after fixing the measurement contract it also uses less runtime RSS than Neo4j on all three datasets.

The main honesty point in `v002` is that runtime RSS is no longer mixed together with CSV truth loading and parity machinery.

## Key Insights

Answer:

- Under the corrected `v002` runtime-only benchmark contract, Knight Bus Rust uses less runtime RAM than Neo4j on all three datasets and remains dramatically faster on traversal latency.

Why this matters:

- The memory story now measures the walker itself, not the old mixed process that also carried CSV truth loading and parity state.
- The result is not just "Rust is fast." The result is that the mmap + dual-CSR walker stays materially lighter at runtime while still answering the same fixed corpus correctly.

Evidence:

- `1 MB`: Knight Bus runtime RSS is `78.9x` lower than Neo4j, and mean traversal latency is `833.6x` faster.
- `50 MB`: Knight Bus runtime RSS is `42.5x` lower than Neo4j, and mean traversal latency is `6113.8x` faster.
- `2 GB`: Knight Bus runtime RSS is `4.5x` lower than Neo4j, and mean traversal latency is `127498.8x` faster.
- The one important counterpoint is startup: Neo4j still opens faster on the `2 GB` run, so Knight Bus wins the walk path much more strongly than the cold-open path.

## v002 Runtime Comparison

| Dataset | Query corpus | Rust status | Neo4j status | Rust open ms | Neo4j open ms | Rust p50 ms | Neo4j p50 ms | Rust p95 ms | Neo4j p95 ms | Rust p99 ms | Neo4j p99 ms | Rust mean ms | Neo4j mean ms | Rust RSS bytes | Neo4j RSS bytes | Neo4j import ms |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 MB | 18 | ok | ok | 0.258083 | 37.685375 | 0.00175 | 2.974563 | 0.018477 | 10.504973 | 0.02555 | 12.611896 | 0.005261 | 4.385152 | 6668288 | 525926400 | 3063.740125 |
| 50 MB | 60 | ok | ok | 4.32775 | 61.926542 | 0.002125 | 37.208291 | 0.020296 | 43.710169 | 0.0363 | 52.235973 | 0.006249 | 38.203163 | 14499840 | 616054784 | 5887.129583 |
| 2 GB | 60 | ok | ok | 189.978958 | 90.446458 | 0.004458 | 1096.492583 | 0.028146 | 1382.781209 | 0.044948 | 1514.533206 | 0.008815 | 1123.882205 | 234340352 | 1065615360 | 42080.808125 |

## Knight Bus Phase Costs

These are separate on purpose. They are not the same thing as runtime walker RSS.

| Dataset | Build peak RSS bytes | Verify peak RSS bytes | Runtime-only RSS bytes |
| --- | ---: | ---: | ---: |
| 1 MB | 10977280 | 11059200 | 6668288 |
| 50 MB | 75300864 | 107954176 | 14499840 |
| 2 GB | 235143168 | 409452544 | 234340352 |

## Measurement Contract

- Rust `bench-corpus` loads only the snapshot and fixed query corpus
- Rust correctness is enforced before timing with `knight-bus verify`
- Neo4j correctness is enforced on the same fixed shared corpus
- Rust RSS scope is `runtime_process_only`
- Neo4j RSS scope is `server_process_only`

## Main Records

- [Final-Testing-Journal-v002.md](./Final-Testing-Journal-v002.md)
- [journal-tests-202604-v002.md](./journal-tests-202604-v002.md)

## Release Links

- [v002 benchmark release](https://github.com/that-in-rust/knight-bus-graph-walker/releases/tag/v002)
- [v0.0.2 binary release](https://github.com/that-in-rust/knight-bus-graph-walker/releases/tag/v0.0.2)
