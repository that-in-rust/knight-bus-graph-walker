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

## Full Thesis

See [docs/KNIGHT_BUS_THESIS.md](./docs/KNIGHT_BUS_THESIS.md).

For the concrete storage contract that aligns the on-disk artifact to the runtime hot path, see [docs/STORAGE_RUNTIME_ALIGNMENT.md](./docs/STORAGE_RUNTIME_ALIGNMENT.md).
