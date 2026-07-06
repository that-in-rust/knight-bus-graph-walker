# 075 olap_algorithm StatsResultBuilder

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/StatsResultBuilder.java |
| lane | olap_algorithm |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 75 |
| line_count | 38 |
| fan_in / fan_out | 43 / 2 |

## Why This File Matters

Defines the stats-mode conversion seam between algorithm result and caller-facing output.

## Public Contract

- Generic trait-like interface with `build(Graph, Optional<RESULT_FROM_ALGORITHM>, AlgorithmProcessingTimings)` (`26-37`).
- Supports empty results through `Optional` with explicit comment (`31-32`).

## Internal Mechanics

- Contract is intentionally tiny, leaving schema construction to implementors.
- The separation of `AlgorithmProcessingTimings` keeps timing observable but detached from graph/result specifics.

## Memory and Storage Implications

- No inherent storage mutation in this interface.
- `Optional` allows cheap empty-result representation for empty graphs.

## Snapshot And Catalog Implications

- No catalog interactions; this seam is pure conversion logic.

## Verification Oracles

1. **WHEN** graph is empty, **THEN** caller may pass `Optional.empty()` and still produce valid output.
2. **WHEN** timings are present, **THEN** they SHALL be used in the result build path.
3. **WHEN** build returns caller result, **THEN** shape must match expected mode schema.

## Rust Rewrite Notes

- Model as a generic function object/trait.
- Keep empty-result behavior explicit (`Option<T>` + timing payload).

## Dependencies Read Next

- `AlgorithmProcessingTimings`
- Concrete stats result builders in algorithm families.

## Dependents As Tests

- Per-family builder tests for empty/non-empty result branches.
- Ensure timing propagation for stats mode.

## Open Questions

- Is there a generic way to enforce common stats fields in all builder outputs?
