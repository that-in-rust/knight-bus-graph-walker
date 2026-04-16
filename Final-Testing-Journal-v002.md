# Final Testing Journal v002

- timestamp: 2026-04-16 16:26:18 IST
- repo: /Users/neetipatni/Desktop/Codex202604/knight-bus-graph-walker
- platform: macOS-14.6-arm64-arm-64bit-Mach-O
- machine: arm64
- python: 3.13.7
- cargo: cargo 1.90.0 (840b83a10 2025-07-30)
- rustc: rustc 1.90.0 (1159e78c4 2025-09-14)

## Measurement Contract

- This record is the corrected runtime-only benchmark set for the fixed `1 MB`, `50 MB`, and `2 GB` fresh check.
- Knight Bus Rust RSS is `runtime_process_only` and comes from the standalone `bench-corpus` runtime process.
- Neo4j RSS is `server_process_only` and is sampled from the Neo4j JVM/server process, not the Python client.
- Rust correctness is enforced before timing through `knight-bus verify`; Neo4j correctness is enforced on the fixed shared corpus against Python truth answers.
- The archived `v001-learnings` files remain untouched as historical evidence; this v002 journal supersedes the old RSS interpretation only.

## Commands Used

- `./scripts/run_neo4j_fresh_check.sh`
- `cargo build --release --manifest-path ./Cargo.toml`
- `./target/release/knight-bus build --nodes-csv ... --edges-csv ... --output ...`
- `./target/release/knight-bus verify --snapshot ... --nodes-csv ... --edges-csv ...`
- `./target/release/knight-bus bench-corpus --snapshot ... --corpus ... --report ...`
- `python benchmarks/walk_hopper_v1/bench_walk_vs_neo4j.py --dataset ... --snapshot ... --corpus ... --report ...`

## Runtime Comparison

| Dataset | Metric | Knight Bus Rust | Neo4j | Winner | Delta |
| --- | --- | ---: | ---: | --- | --- |
| 1 MB | Status | ok | ok | tie | parity passed |
| 1 MB | Query corpus | 18 | 18 | tie | same workload |
| 1 MB | Open/start ms | 0.258083 | 37.685375 | Knight Bus Rust | ~146.0x faster |
| 1 MB | p50 hop ms | 0.00175 | 2.974563 | Knight Bus Rust | ~1699.8x faster |
| 1 MB | p95 hop ms | 0.018477 | 10.504973 | Knight Bus Rust | ~568.5x faster |
| 1 MB | p99 hop ms | 0.02555 | 12.611896 | Knight Bus Rust | ~493.6x faster |
| 1 MB | Mean hop ms | 0.005261 | 4.385152 | Knight Bus Rust | ~833.6x faster |
| 1 MB | RSS bytes | 6668288 | 525926400 | Knight Bus Rust | ~78.9x lower |
| 1 MB | Rust RSS scope | runtime_process_only | n/a | info | runtime-only |
| 1 MB | Neo4j RSS scope | n/a | server_process_only | info | server-only |
| 1 MB | Neo4j import ms | n/a | 3063.740125 | Neo4j | captured during rerun |
| 50 MB | Status | ok | ok | tie | parity passed |
| 50 MB | Query corpus | 60 | 60 | tie | same workload |
| 50 MB | Open/start ms | 4.32775 | 61.926542 | Knight Bus Rust | ~14.3x faster |
| 50 MB | p50 hop ms | 0.002125 | 37.208291 | Knight Bus Rust | ~17509.8x faster |
| 50 MB | p95 hop ms | 0.020296 | 43.710169 | Knight Bus Rust | ~2153.6x faster |
| 50 MB | p99 hop ms | 0.0363 | 52.235973 | Knight Bus Rust | ~1439.0x faster |
| 50 MB | Mean hop ms | 0.006249 | 38.203163 | Knight Bus Rust | ~6113.8x faster |
| 50 MB | RSS bytes | 14499840 | 616054784 | Knight Bus Rust | ~42.5x lower |
| 50 MB | Rust RSS scope | runtime_process_only | n/a | info | runtime-only |
| 50 MB | Neo4j RSS scope | n/a | server_process_only | info | server-only |
| 50 MB | Neo4j import ms | n/a | 5887.129583 | Neo4j | captured during rerun |
| 2 GB | Status | ok | ok | tie | parity passed |
| 2 GB | Query corpus | 60 | 60 | tie | same workload |
| 2 GB | Open/start ms | 189.978958 | 90.446458 | Neo4j | ~2.1x faster |
| 2 GB | p50 hop ms | 0.004458 | 1096.492583 | Knight Bus Rust | ~245960.7x faster |
| 2 GB | p95 hop ms | 0.028146 | 1382.781209 | Knight Bus Rust | ~49128.6x faster |
| 2 GB | p99 hop ms | 0.044948 | 1514.533206 | Knight Bus Rust | ~33695.6x faster |
| 2 GB | Mean hop ms | 0.008815 | 1123.882205 | Knight Bus Rust | ~127498.8x faster |
| 2 GB | RSS bytes | 234340352 | 1065615360 | Knight Bus Rust | ~4.5x lower |
| 2 GB | Rust RSS scope | runtime_process_only | n/a | info | runtime-only |
| 2 GB | Neo4j RSS scope | n/a | server_process_only | info | server-only |
| 2 GB | Neo4j import ms | n/a | 42080.808125 | Neo4j | captured during rerun |

## Knight Bus Phase Costs

| Dataset | Build peak RSS bytes | Verify peak RSS bytes | Runtime-only RSS bytes | Build RSS source | Verify RSS source | Runtime RSS source |
| --- | ---: | ---: | ---: | --- | --- | --- |
| 1 MB | 10977280 | 11059200 | 6668288 | getrusage_self | getrusage_self | getrusage_self |
| 50 MB | 75300864 | 107954176 | 14499840 | getrusage_self | getrusage_self | getrusage_self |
| 2 GB | 235143168 | 409452544 | 234340352 | getrusage_self | getrusage_self | getrusage_self |

## Honest Notes

- These journals use the three existing artifact datasets and their fixed `query_corpus.csv` files; no dataset regeneration is part of this rerun.
- Knight Bus build and verify costs are reported separately because runtime-only walker RSS is not the whole operating picture.
- Neo4j import duration is listed only because it was captured during the same fresh check.

