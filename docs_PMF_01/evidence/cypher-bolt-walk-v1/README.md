# Cypher Bolt Walk v1 Evidence

Date: 2026-08-07

This directory preserves the release evidence for the exact compatibility
claim `knight-bus-neighborhood-walk-v1`: Neo4j Python driver `6.1.0`, direct
`bolt://`, read-only auto-commit, and the three bounded `DEPENDS_ON`
neighborhood-walk query shapes in PMF006.

## Verdict

The paired scale gate passed all four mandatory conditions.

| Measurement | Knight Bus | Neo4j 2026.07.0 | Neo4j / Knight Bus |
| --- | ---: | ---: | ---: |
| Warm Bolt p99, 180 samples | 3.970300 ms | 5.302670 ms | 1.335584x |
| Warm Bolt p50 | 0.274438 ms | 0.516938 ms | 1.883626x |
| Peak compatibility-stack RSS | 234,176,512 B | 374,046,720 B | 1.597285x |
| First successful query | 0.463666 ms | 1.656458 ms | 3.572524x |

The test corpus contained 3,997,988 nodes, 36,294,270 relationships, and 60
queries. One warm-up pass was discarded, then three measured passes produced
180 latency samples per engine. All ordered result sequences matched exactly.
The Neo4j plan was required to contain `NodeIndexSeek` before measurements were
admitted.

## Files

- `compatibility-receipt.json` is the machine-readable source of truth. Its
  SHA-256 is
  `1cedc9c89bdaf2869d8034e4daca2961992a0ead9237c0d950099ea7820af529`.
- `compatibility-summary.md` is the generated human-readable summary. Its
  SHA-256 is
  `bbd7a869bfca8594ced9cadf3e41e960d70532cb9fa7b3e4a917e42c195c20e8`.

## Verification Inventory

| Layer | Result |
| --- | --- |
| Rust compiler, planner, execution, profile, and property contracts | 14 passed |
| Official Neo4j driver protocol contracts | 8 passed |
| Neo4j Cypher DSL Parser differential contracts | 2 passed |
| Real Neo4j adversarial graph differential contracts | 2 passed |
| Benchmark harness unit contracts | 5 passed |
| Complete Rust workspace tests | 65 passed |
| Scale compatibility gate | passed, 4/4 conditions |

## Disclosures

- The receipt records Knight Bus readiness and first-query time separately.
  Neo4j server cold boot is marked unavailable because the comparator did not
  own the Homebrew service lifecycle.
- File-backed mmap residency is marked unavailable because this macOS build of
  `psutil.Process` does not expose `memory_maps`; process RSS and snapshot-file
  bytes remain measured separately.
- The execution kernel accepts a cancellation token and proves cancellation in
  Rust tests. Boltr `0.2.0` does not expose socket disconnect while its backend
  callback is running, so an abrupt transport disconnect cannot preempt an
  already-running callback. The finite deadline and row cap still bound cleanup,
  and connection-local state is dropped after disconnect detection.
- Basic authentication is suitable for this local compatibility proof. TLS,
  routing, writes, explicit transactions, bookmarks, impersonation, and
  alternate databases remain intentionally outside the profile.
- Repository-wide strict Clippy is currently blocked by unrelated concurrent
  warnings in `src/main.rs` and `tests/gds_registry_contract.rs`. Strict Clippy
  passes for the complete Cypher/Bolt production and contract-test scope.
