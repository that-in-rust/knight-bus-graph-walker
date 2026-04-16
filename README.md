# Knight Bus Graph Walker

`knight-bus-graph-walker` should prove one narrow claim:

> a persisted graph that is larger than RAM can still answer forward and backward neighborhood queries on a 16 GB laptop if the runtime is shaped like a graph walk engine instead of a general database.

The fastest honest build is:

- generate a large synthetic directed graph
- compile it into dual adjacency snapshots (`forward` + `reverse`)
- memory-map the snapshot from Rust
- expose a tiny HTTP API for `forward`, `backward`, and `hops`
- benchmark latency and resident memory against simpler baselines

This repo should not start as:

- a general graph database
- a query language project
- a graph visualization product
- a rank or PageRank engine
- a Wasm-first demo

The minimal proof should stay focused on walk-time traversal.

## Recommended MVP

Build one Rust binary with four subcommands:

- `generate` writes a synthetic graph dataset
- `build` compiles the dataset into a walk snapshot
- `serve` opens the snapshot and serves HTTP endpoints
- `bench` measures cold and hot lookup latency plus memory footprint

## Minimal API

- `GET /forward/:entity`
- `GET /backward/:entity`
- `GET /hops/:entity?hops=2&dir=backward`
- `GET /stats`

For the hackathon, synthetic node keys should be deterministic, for example `entity_000123456`, so key lookup can stay trivial and the benchmark focuses on graph walking, not fuzzy search.

## What Success Looks Like

- the snapshot is materially larger than RAM-friendly toy sizes
- the server answers one-hop and two-hop queries without loading the whole graph into heap memory
- RSS stays far below dataset size during query runs
- hot walk latency clearly beats raw edge scanning and indexed-table baselines
- the demo story is simple enough to explain in under two minutes

## v002 Corrected Benchmark Snapshot

These numbers come from the corrected `v002` fresh check:

- Knight Bus Rust RSS is `runtime_process_only`
- Neo4j RSS is `server_process_only`
- Rust correctness is enforced separately through `verify`
- the fixed `1 MB`, `50 MB`, and `2 GB` datasets were reused without regenerating them

### Runtime Comparison

| Dataset | Query corpus | Rust status | Neo4j status | Rust open ms | Neo4j open ms | Rust p50 ms | Neo4j p50 ms | Rust p95 ms | Neo4j p95 ms | Rust p99 ms | Neo4j p99 ms | Rust mean ms | Neo4j mean ms | Rust RSS bytes | Neo4j RSS bytes | Neo4j import ms |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 MB | 18 | ok | ok | 0.258083 | 37.685375 | 0.00175 | 2.974563 | 0.018477 | 10.504973 | 0.02555 | 12.611896 | 0.005261 | 4.385152 | 6668288 | 525926400 | 3063.740125 |
| 50 MB | 60 | ok | ok | 4.32775 | 61.926542 | 0.002125 | 37.208291 | 0.020296 | 43.710169 | 0.0363 | 52.235973 | 0.006249 | 38.203163 | 14499840 | 616054784 | 5887.129583 |
| 2 GB | 60 | ok | ok | 189.978958 | 90.446458 | 0.004458 | 1096.492583 | 0.028146 | 1382.781209 | 0.044948 | 1514.533206 | 0.008815 | 1123.882205 | 234340352 | 1065615360 | 42080.808125 |

### Knight Bus Phase Costs

| Dataset | Build peak RSS bytes | Verify peak RSS bytes | Runtime-only RSS bytes |
| --- | ---: | ---: | ---: |
| 1 MB | 10977280 | 11059200 | 6668288 |
| 50 MB | 75300864 | 107954176 | 14499840 |
| 2 GB | 235143168 | 409452544 | 234340352 |

See the full records in [Final-Testing-Journal-v002.md](./Final-Testing-Journal-v002.md) and [journal-tests-202604-v002.md](./journal-tests-202604-v002.md).

## Full Thesis

See [docs/KNIGHT_BUS_THESIS.md](./docs/KNIGHT_BUS_THESIS.md).

For the concrete storage contract that aligns the on-disk artifact to the runtime hot path, see [docs/STORAGE_RUNTIME_ALIGNMENT.md](./docs/STORAGE_RUNTIME_ALIGNMENT.md).
