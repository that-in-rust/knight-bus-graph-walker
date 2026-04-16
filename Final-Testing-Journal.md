# Final Testing Journal

- timestamp: 2026-04-16 15:11:57 IST
- repo: /Users/neetipatni/Desktop/Codex202604/knight-bus-graph-walker
- platform: macOS-14.6-arm64-arm-64bit-Mach-O
- machine: arm64
- python: 3.13.7
- cargo: cargo 1.90.0 (840b83a10 2025-07-30)
- rustc: rustc 1.90.0 (1159e78c4 2025-09-14)

## Commands Used

- `./scripts/run_neo4j_smoke_ladder.sh`
- `cargo build --release --manifest-path ./Cargo.toml`
- `python benchmarks/walk_hopper_v1/bench_walk_vs_neo4j.py ... --knight-bus-bin ./target/release/knight-bus`
- `./target/release/knight-bus bench-corpus --snapshot ... --nodes-csv ... --edges-csv ... --corpus ... --report ...`

## Comparison Table

| Dataset | Metric | Knight Bus Rust | Neo4j | Winner | Delta |
| --- | --- | ---: | ---: | --- | --- |
| 1 MB | Status | ok | ok | tie | parity passed |
| 1 MB | Query corpus | 18 | 18 | tie | same workload |
| 1 MB | Open/start ms | 1.770833 | 31.858959 | Knight Bus Rust | ~18.0x faster |
| 1 MB | p50 hop ms | 0.001667 | 4.557187 | Knight Bus Rust | ~2734.6x faster |
| 1 MB | p95 hop ms | 0.018646 | 7.070831 | Knight Bus Rust | ~379.2x faster |
| 1 MB | p99 hop ms | 0.025692 | 9.375404 | Knight Bus Rust | ~364.9x faster |
| 1 MB | Mean hop ms | 0.005155 | 4.316184 | Knight Bus Rust | ~837.2x faster |
| 1 MB | RSS bytes | 12926976 | 74481664 | Knight Bus Rust | ~5.8x lower |
| 1 MB | Neo4j import ms | n/a | 3216.179334 | Neo4j | captured during rerun |
| 50 MB | Status | ok | ok | tie | parity passed |
| 50 MB | Query corpus | 60 | 60 | tie | same workload |
| 50 MB | Open/start ms | 21.318833 | 52.551167 | Knight Bus Rust | ~2.5x faster |
| 50 MB | p50 hop ms | 0.002063 | 25.29675 | Knight Bus Rust | ~12265.1x faster |
| 50 MB | p95 hop ms | 0.019935 | 50.787884 | Knight Bus Rust | ~2547.7x faster |
| 50 MB | p99 hop ms | 0.037531 | 55.434319 | Knight Bus Rust | ~1477.0x faster |
| 50 MB | Mean hop ms | 0.006136 | 31.163228 | Knight Bus Rust | ~5078.8x faster |
| 50 MB | RSS bytes | 388546560 | 75104256 | Neo4j | ~5.2x lower |
| 50 MB | Neo4j import ms | n/a | 5661.278292 | Neo4j | captured during rerun |

## Honest Caveats

- This is a `1 MB` and `50 MB` proof, not a `20 GB` proof.
- The shared workload is the selected corpus of `forward_one`, `reverse_one`, and `reverse_two` queries.
- Both engines are considered `ok` only if parity passed on the selected corpus.
- Neo4j import duration is listed only because it was captured during this rerun.

