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

## Why The CSR-Style Walk Works

Knight Bus does the expensive graph organization work ahead of time. At query
time it does not go hunting through scattered edge records. It jumps straight
to a precompiled neighbor window by using sorted keys, dense ids, offsets, and
one contiguous peer slice.

```text
Level 1: The Essence

A graph hop is turned into:

    find key  -->  get dense id  -->  read one neighbor window


query(node_key)
     |
     v
+------------------+
| sorted key_index |
+------------------+
     |
     v
  dense_id
     |
     v
+-----------------------+
| start = offsets[id]   |
| end   = offsets[id+1] |
+-----------------------+
     |
     v
+------------------------------+
| peers[start .. end]          |
| one contiguous neighbor run  |
+------------------------------+
     |
     v
neighbor dense ids -> node keys


Level 2: Why It Feels Different

Generic graph engine                      Knight Bus walk path

+---------------------------+            +---------------------------+
| find node                 |            | find dense_id             |
| follow relationship idx   |            | read offsets[id]          |
| chase edge/node pointers  |            | read offsets[id + 1]      |
| gather scattered rows     |            | read peers[start..end]    |
+---------------------------+            +---------------------------+

left side  = discover links dynamically
right side = jump to a precompiled neighbor window


Level 3: Why Dual CSR Helps

forward walk                               reverse walk

node                                       node
 |                                          |
 v                                          v
forward_offsets[id..id+1]                  reverse_offsets[id..id+1]
 |                                          |
 v                                          v
forward_peers[start..end]                  reverse_peers[start..end]

No reverse scan over forward edges.
No rebuild of backlinks at query time.
```

Why this works in practice:

- dense ids turn each node into an array position instead of a hash-chase target
- offsets turn adjacency lookup into arithmetic
- peers are stored contiguously, so the hot read is a slice, not a scatter-gather
- forward and reverse CSR remove the need to derive the other direction on demand
- mmap lets the operating system page in only the touched parts of the snapshot

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

## Benchmark Code Stays Tracked

The repo keeps the benchmark machinery in Git and keeps the heavyweight outputs
out of Git.

- tracked: `benchmarks/walk_hopper_v1/...`, `scripts/...`, tests, and tiny
  committed fixtures under `benchmarks/walk_hopper_v1/fixtures/...`
- ignored: generated raw datasets under `artifacts/...`, benchmark reports
  under `reports/...`, local Neo4j state, and local virtualenv/runtime
  byproducts
- rule: the generator script is tracked, but generated `code_sparse_*` graph
  data is not

## Exact Records Stay Linked

Use the journals for exact raw values; the README keeps the numbers
human-readable on purpose.

- [Final-Testing-Journal-v002.md](./Final-Testing-Journal-v002.md)
- [journal-tests-202604-v002.md](./journal-tests-202604-v002.md)
- [v002 release](https://github.com/that-in-rust/knight-bus-graph-walker/releases/tag/v002)
- [v0.0.2 binary release](https://github.com/that-in-rust/knight-bus-graph-walker/releases/tag/v0.0.2)
