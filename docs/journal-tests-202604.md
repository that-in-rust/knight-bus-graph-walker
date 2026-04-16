# Knight Bus Test Journal 2026-04

This file is the singular append-only test journal for April 2026.

---

## 2026-04-16 14:16:39 IST — Tiny Harness Nanosecond Validation

### Scope

- repo: `knight-bus-graph-walker`
- target: `v001` CLI-only Rust snapshot walker
- harness: `parseltongue-rust-LLM-companion-v301/test-harness/rust-test-001`
- status: `working on the tiny harness`

### Inputs

- `interface_nodes.csv`: `39` effective nodes
- `interface_edges.csv`: `67` effective edges

### Commands Rechecked

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

### Build Truth

- snapshot build: passed
- node count: `39`
- edge count: `67`
- snapshot size: `3508` bytes
- wall-clock build time: about `0.34s`

### Verify Truth

- parity verification: passed
- total checked queries: `134`
- `forward_one`: `31`
- `backward_one`: `36`
- `forward_two`: `31`
- `backward_two`: `36`

### Nanosecond Hop Truth

#### Run 1

- `forward_one`: `p50 833 ns`, `p95 1084 ns`, `31` samples
- `backward_one`: `p50 667 ns`, `p95 875 ns`, `36` samples
- `forward_two`: `p50 1084 ns`, `p95 2541 ns`, `31` samples
- `backward_two`: `p50 1208 ns`, `p95 1875 ns`, `36` samples

#### Run 2

- `forward_one`: `p50 833 ns`, `p95 1333 ns`, `31` samples
- `backward_one`: `p50 708 ns`, `p95 875 ns`, `36` samples
- `forward_two`: `p50 1417 ns`, `p95 2792 ns`, `31` samples
- `backward_two`: `p50 1125 ns`, `p95 1625 ns`, `36` samples

#### Run 3

- `forward_one`: `p50 750 ns`, `p95 1125 ns`, `31` samples
- `backward_one`: `p50 667 ns`, `p95 875 ns`, `36` samples
- `forward_two`: `p50 1083 ns`, `p95 2709 ns`, `31` samples
- `backward_two`: `p50 1166 ns`, `p95 1708 ns`, `36` samples

### Honest Read

- tiny harness build result: trusted
- tiny harness parity result: trusted
- tiny harness latency result: trusted
- current built-in macOS `peak_rss_bytes`: not trusted yet

OS-level sanity check from `/usr/bin/time -l`:

- maximum resident set size: about `7.0 MB`
- peak memory footprint: about `4.7 MB`

### Verdict

`v001` is working on the existing tiny harness.

The remaining known issue from this pass is the macOS RSS reporting path, not the graph walk
correctness or nanosecond hop timing.
