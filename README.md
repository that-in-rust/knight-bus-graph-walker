# What Knight Bus Is Proving

Knight Bus proves that a storage-specialized Rust walk runtime can answer the
same fixed graph corpus as Neo4j while keeping `p99` latency and runtime RAM
far lower on the tracked `1 MB`, `50 MB`, and `2 GB` datasets.

Neo4j still opens the `2 GB` run faster, so this README is strongest on the
walk path, not every cold-start path.

These numbers come from the current tracked benchmark record.
This repo itself was created in 5 hours for a Codex hackathon.

## What This Comparison Proves

This benchmark is proving three things at once:

- Knight Bus and Neo4j return the same answers on the tracked corpus.
- Knight Bus keeps tail latency dramatically lower on every tracked dataset.
- Knight Bus keeps runtime RAM materially lower while answering queries.

| Dataset | Same answers | Rust p99 | Neo4j p99 | p99 win | Rust runtime RAM | Neo4j runtime RAM | RAM win |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `1 MB` | `yes` | `26 µs` | `12.6 ms` | `494x` | `6.7 MB` | `525.9 MB` | `78.9x lower` |
| `50 MB` | `yes` | `36 µs` | `52.2 ms` | `1,439x` | `14.5 MB` | `616.1 MB` | `42.5x lower` |
| `2 GB` | `yes` | `45 µs` | `1.51 s` | `33,695x` | `234.3 MB` | `1.07 GB` | `4.5x lower` |

## Why This Comparison Is Fair

- same fixed shared datasets
- same fixed shared query corpus
- correctness checked before timing
- one Rust walker process measured against one Neo4j server process
- tracked datasets only: `1 MB`, `50 MB`, `2 GB`

## Tail Latency Is The Main Win

The percentile view makes the shape of the win easy to see.

| Dataset | Rust p50 | Neo4j p50 | Rust p95 | Neo4j p95 | Rust p99 | Neo4j p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `1 MB` | `1.8 µs` | `3.0 ms` | `18.5 µs` | `10.5 ms` | `26 µs` | `12.6 ms` |
| `50 MB` | `2.1 µs` | `37.2 ms` | `20.3 µs` | `43.7 ms` | `36 µs` | `52.2 ms` |
| `2 GB` | `4.5 µs` | `1.10 s` | `28.1 µs` | `1.38 s` | `45 µs` | `1.51 s` |

## Runtime RAM Is The Second Win

`RSS` here means the RAM the running process is holding onto while answering
queries.

The query-time RAM number is intentionally narrower than build and verify RAM.

| Dataset | Build peak RAM | Verify peak RAM | Query-time RAM |
| --- | ---: | ---: | ---: |
| `1 MB` | `11.0 MB` | `11.1 MB` | `6.7 MB` |
| `50 MB` | `75.3 MB` | `108.0 MB` | `14.5 MB` |
| `2 GB` | `235.1 MB` | `409.5 MB` | `234.3 MB` |

## Neo4j Still Wins One Startup Case

Startup is the visible caveat, and it only flips at the `2 GB` tier.

| Dataset | Rust open | Neo4j open | Winner |
| --- | ---: | ---: | --- |
| `1 MB` | `0.3 ms` | `37.7 ms` | `Rust` |
| `50 MB` | `4.3 ms` | `61.9 ms` | `Rust` |
| `2 GB` | `190.0 ms` | `90.4 ms` | `Neo4j` |

## Exact Records Stay Linked

Use the journals for exact raw values; the README keeps the numbers
human-readable on purpose.

- [Final-Testing-Journal-v002.md](./Final-Testing-Journal-v002.md)
- [journal-tests-202604-v002.md](./journal-tests-202604-v002.md)
- [v002 release](https://github.com/that-in-rust/knight-bus-graph-walker/releases/tag/v002)
- [v0.0.2 binary release](https://github.com/that-in-rust/knight-bus-graph-walker/releases/tag/v0.0.2)
