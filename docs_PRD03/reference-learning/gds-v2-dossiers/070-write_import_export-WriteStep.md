# 070 write_import_export WriteStep

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/WriteStep.java |
| lane | write_import_export |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 70 |
| line_count | 40 |
| fan_in / fan_out | 42 / 4 |

## Why This File Matters

This file is the narrow write boundary contract for algorithm machinery.

## Public Contract

- `WriteStep` is generic over result and metadata types (`RESULT_FROM_ALGORITHM`, `WRITE_METADATA`) and declares one `execute(...)` method (`27-39`).
- `execute` receives both `Graph` and `GraphStore`, and emits `WRITE_METADATA` from a `ResultStore`-aware mutation flow (`33-39`).
- No implementation defaults are present; all mutation strategies must implement this contract (`33-39`).

## Internal Mechanics

- This interface intentionally exposes only orchestration-level inputs and not execution internals.
- Metadata is separated from procedure output transport by using `WRITE_METADATA`.

## Memory and Storage Implications

- Write-side memory behavior is implemented in concrete classes, but this seam is where graph-store mutation is triggered (`33-38`).
- The presence of `ResultStore` allows optional stream/result-side recording without duplicating full result payloads.

## Snapshot And Catalog Implications

- Catalog validation and graph-resolution are expected before this boundary; this file only consumes already-bound `GraphStore` and `Graph`.

## Verification Oracles

1. **WHEN** a mutate mode writes results, **THEN** one `WriteStep.execute(...)` call path SHALL be used.
2. **WHEN** execution finishes, **THEN** `WRITE_METADATA` SHALL reflect mode-specific completion shape.
3. **WHEN** caller expects progress visibility, **THEN** `jobId` SHALL be supplied to write execution.

## Rust Rewrite Notes

- Model this as a generic trait interface for write-step policy isolation.
- Keep metadata types explicit and mode-specific.
- Preserve the same data inputs (`Graph`, `GraphStore`, `ResultStore`, `job_id`) as explicit function arguments.

## Dependencies Read Next

- Classes implementing `WriteStep` under algorithm families (community, centrality, path-finding, etc.).
- `AlgorithmMachinery` callsites that invoke write mode.

## Dependents As Tests

- Contract tests confirming every mutate entrypoint invokes a write step.
- Negative tests where write metadata is missing/invalid for write modes.

## Open Questions

- Should there be a common optional trait for write result counters vs. algorithm-specific metadata?
