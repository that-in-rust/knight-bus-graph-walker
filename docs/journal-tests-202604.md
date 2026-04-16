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

---

## 2026-04-16 14:45:02 IST — Three-Tier Dataset Validation

### Runner Defaults

- generator seed: `7`
- generator layer count: `64`
- generator degree palette: `6,8,10,12,14`
- preflight verify timeout: `300s`
- run root: `/var/folders/9g/583rs5gx46932lgqrh6_wq600000gn/T/knight-bus-three-tier-202604`

### Result Table

| tier | source | raw_csv_bytes | nodes | edges | snapshot_bytes | build_s | verify_status | checked_queries | forward_one ns p50/p95 | backward_one ns p50/p95 | forward_two ns p50/p95 | backward_two ns p50/p95 | peak_rss_bytes | peak_rss_source | verdict |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | --- | --- | --- | --- | ---: | --- | --- |
| tiny_checked_in_toy | `benchmarks/walk_hopper_v1/fixtures/tiny_graph` | 1052 | 8 | 9 | 919 | 0.641 | ok | 22 | 625/791 | 583/667 | 750/1000 | 792/958 | 6963200 | `getrusage_self` | correctness only, latency not representative |
| real_smoke_dataset | `/var/folders/9g/583rs5gx46932lgqrh6_wq600000gn/T/knight-bus-three-tier-202604/real_smoke_dataset/dataset` | 1142922 | 2085 | 18963 | 268934 | 0.020 | ok | 8178 | 1666/2708 | 625/1250 | 14334/20709 | 959/2875 | 7454720 | `getrusage_self` | representative smoke tier |
| planned_preflight_dataset | `/var/folders/9g/583rs5gx46932lgqrh6_wq600000gn/T/knight-bus-three-tier-202604/planned_preflight_dataset/dataset` | 55976085 | 102290 | 928620 | 13157673 | 0.948 | ok | 400920 | 1917/2792 | 750/1625 | 15958/24625 | 708/1125 | 28721152 | `getrusage_self` | preflight tier |

### Notes

- The smoke and preflight tiers use raw CSV size as the canonical target. The logged node and edge counts are the actual measured outputs of the deterministic generator.
- The tiny checked-in toy remains a correctness-only tier.
- `peak_rss_bytes` is now logged together with `peak_rss_source` so the measurement provenance is explicit.

---

## 2026-04-16 14:45:44 IST — Three-Tier Dataset Validation

### Runner Defaults

- generator seed: `7`
- generator layer count: `64`
- generator degree palette: `6,8,10,12,14`
- preflight verify timeout: `300s`
- run root: `/var/folders/9g/583rs5gx46932lgqrh6_wq600000gn/T/knight-bus-three-tier-202604`

### Result Table

| tier | source | raw_csv_bytes | nodes | edges | snapshot_bytes | build_s | verify_status | checked_queries | forward_one ns p50/p95 | backward_one ns p50/p95 | forward_two ns p50/p95 | backward_two ns p50/p95 | peak_rss_bytes | peak_rss_source | verdict |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | --- | --- | --- | --- | ---: | --- | --- |
| tiny_checked_in_toy | `benchmarks/walk_hopper_v1/fixtures/tiny_graph` | 1052 | 7 | 8 | 919 | 0.197 | ok | 22 | 917/1083 | 875/916 | 1167/1334 | 1000/1416 | 6799360 | `getrusage_self` | correctness only, latency not representative |
| real_smoke_dataset | `/var/folders/9g/583rs5gx46932lgqrh6_wq600000gn/T/knight-bus-three-tier-202604/real_smoke_dataset/dataset` | 1142922 | 2085 | 18963 | 268934 | 0.059 | ok | 8178 | 3834/21209 | 958/1750 | 14958/31791 | 917/3875 | 7520256 | `getrusage_self` | representative smoke tier |
| planned_preflight_dataset | `/var/folders/9g/583rs5gx46932lgqrh6_wq600000gn/T/knight-bus-three-tier-202604/planned_preflight_dataset/dataset` | 55976085 | 102290 | 928620 | 13157673 | 0.944 | ok | 400920 | 1750/2458 | 667/1000 | 13250/20292 | 708/1000 | 21725184 | `getrusage_self` | preflight tier |

### Notes

- The smoke and preflight tiers use raw CSV size as the canonical target. The logged node and edge counts are the actual measured outputs of the deterministic generator.
- The tiny checked-in toy remains a correctness-only tier.
- `peak_rss_bytes` is now logged together with `peak_rss_source` so the measurement provenance is explicit.
- This corrected entry supersedes the `2026-04-16 14:45:02 IST` ladder run for count accuracy; that earlier run had a tiny-tier `+1` row-count bug from trailing blank-line handling.
