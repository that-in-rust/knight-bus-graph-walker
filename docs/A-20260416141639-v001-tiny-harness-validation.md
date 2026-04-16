# Knight Bus v001 Tiny Harness Validation

```text
Timestamp: 2026-04-16 14:16:39 IST
Repo: knight-bus-graph-walker
Target: v001 CLI-only Rust snapshot walker
Harness: parseltongue-rust-LLM-companion-v301/test-harness/rust-test-001
Status: working on the tiny harness
```

## What Was Rechecked

This validation pass rechecked the real `v001` implementation against the original tiny
Parseltongue harness, not only against the copied fixture tests inside this repo.

The benchmark unit was also upgraded from microseconds to nanoseconds before this run.

## Harness Inputs

- `interface_nodes.csv`: 40 lines including header
- `interface_edges.csv`: 68 lines including header
- effective graph size:
  - `39` nodes
  - `67` edges

## Commands Run

```bash
cargo test --all-targets
cargo build --release

target/release/knight-bus build \
  --nodes-csv /Users/neetipatni/Desktop/Codex202604/parseltongue-rust-LLM-companion-v301/test-harness/rust-test-001/interface_nodes.csv \
  --edges-csv /Users/neetipatni/Desktop/Codex202604/parseltongue-rust-LLM-companion-v301/test-harness/rust-test-001/interface_edges.csv \
  --output /tmp/knightbus-harness-ns/snapshot

target/release/knight-bus verify \
  --snapshot /tmp/knightbus-harness-ns/snapshot \
  --nodes-csv /Users/neetipatni/Desktop/Codex202604/parseltongue-rust-LLM-companion-v301/test-harness/rust-test-001/interface_nodes.csv \
  --edges-csv /Users/neetipatni/Desktop/Codex202604/parseltongue-rust-LLM-companion-v301/test-harness/rust-test-001/interface_edges.csv

target/release/knight-bus bench \
  --snapshot /tmp/knightbus-harness-ns/snapshot \
  --report /tmp/knightbus-harness-ns/report-run-1

target/release/knight-bus bench \
  --snapshot /tmp/knightbus-harness-ns/snapshot \
  --report /tmp/knightbus-harness-ns/report-run-2

target/release/knight-bus bench \
  --snapshot /tmp/knightbus-harness-ns/snapshot \
  --report /tmp/knightbus-harness-ns/report-run-3
```

## Build Result

The snapshot build succeeded.

- node count: `39`
- edge count: `67`
- snapshot size: `3508` bytes
- wall-clock build time: about `0.34s`

## Verify Result

Parity verification succeeded.

- total checked queries: `134`
- `forward_one`: `31`
- `backward_one`: `36`
- `forward_two`: `31`
- `backward_two`: `36`

This means the snapshot answers matched the CSV truth layer on the existing tiny harness.

## Nanosecond Benchmark Results

### Run 1

- `forward_one`: `p50 833 ns`, `p95 1084 ns`, `31` samples
- `backward_one`: `p50 667 ns`, `p95 875 ns`, `36` samples
- `forward_two`: `p50 1084 ns`, `p95 2541 ns`, `31` samples
- `backward_two`: `p50 1208 ns`, `p95 1875 ns`, `36` samples

### Run 2

- `forward_one`: `p50 833 ns`, `p95 1333 ns`, `31` samples
- `backward_one`: `p50 708 ns`, `p95 875 ns`, `36` samples
- `forward_two`: `p50 1417 ns`, `p95 2792 ns`, `31` samples
- `backward_two`: `p50 1125 ns`, `p95 1625 ns`, `36` samples

### Run 3

- `forward_one`: `p50 750 ns`, `p95 1125 ns`, `31` samples
- `backward_one`: `p50 667 ns`, `p95 875 ns`, `36` samples
- `forward_two`: `p50 1083 ns`, `p95 2709 ns`, `31` samples
- `backward_two`: `p50 1166 ns`, `p95 1708 ns`, `36` samples

## Stable Read Of The Tiny Harness

Across the three runs, the tiny harness is behaving like this:

- forward one-hop:
  - `p50` roughly `750-833 ns`
  - `p95` roughly `1084-1333 ns`
- backward one-hop:
  - `p50` roughly `667-708 ns`
  - `p95` roughly `875 ns`
- forward two-hop:
  - `p50` roughly `1083-1417 ns`
  - `p95` roughly `2541-2792 ns`
- backward two-hop:
  - `p50` roughly `1125-1208 ns`
  - `p95` roughly `1625-1875 ns`

For the tiny harness, the runtime is answering one-hop and two-hop queries in low-nanosecond to
low-microsecond territory inside the process.

## Memory Note

The built-in `peak_rss_bytes` field still looks wrong on macOS for this tiny benchmark.

The in-process benchmark reported values around `7.1-7.3 GB`, which are not believable for a
`3508` byte snapshot and a tiny query workload.

An OS-level sanity check using `/usr/bin/time -l` reported:

- maximum resident set size: about `7.0 MB`
- peak memory footprint: about `4.7 MB`

So:

- latency result: trusted
- parity result: trusted
- build result: trusted
- current built-in macOS RSS field: **not trusted yet**

## Working Or Not

For the tiny harness, `v001` is working.

Specifically:

- build works
- verify works
- query works
- benchmark works
- nanosecond hop measurements are now in place

The remaining known issue from this validation pass is only the macOS RSS reporting path.

## Bottom Line

`v001` passes the existing tiny harness honestly, and the measured hop latencies on that harness
are in the sub-microsecond to low-microsecond range, with nanosecond reporting now enabled.
