# 21 olap_algorithm ResultBuilder

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/ResultBuilder.java |
| lane | olap_algorithm |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 21 |
| line_count | 49 |
| fan_in / fan_out | 109 / 2 |
| purpose | Capture algorithm execution skeleton: config, estimation, execution modes, result/write/mutate/stream artifacts. |
| read_prompt | Read this entire file as an algorithm execution contract. Extract lifecycle stages, required graph views, algorithm state, result artifacts, memory estimation hooks, mutate/write/stream/stats behavior, and verification cases. |

## Why This File Matters

- This is the abstraction boundary between algorithm execution engines and result rendering.
- Every OLAP façade call can flow through a `ResultBuilder` implementation to produce different output shapes while sharing algorithm output + timing + metadata.
- It is tiny but foundational for Rust dispatch architecture in procedure layers.

## Public Contract

- Generic interface:
  - `ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_OR_WRITE_METADATA>`
- Single required method:
  - `RESULT_TO_CALLER build(Graph graph, CONFIGURATION configuration, Optional<RESULT_FROM_ALGORITHM> result, AlgorithmProcessingTimings timings, Optional<MUTATE_OR_WRITE_METADATA> metadata)`
- Contract:
  - `result` empty when graph is empty
  - `metadata` empty if graph was empty / no metadata emitted
  - callers supply graph and parsed configuration

## Internal Mechanics

- No implementation details in this file itself.
- Intended to support:
  - output shape conversion (stream/stats/write),
  - runtime-injected dependencies via builder constructor (comment suggests in-layer/custom dependencies),
  - unified interface across algorithm modalities.
- The interface-level Javadoc indicates:
  - not every caller uses every argument,
  - implementers should accept selective use of arguments.

## Storage And Storage Behavior

- This file is memory-safe by design: no state, no fields.
- Most RAM effect comes from builder implementations and captured values of result/result metadata.
- A Rust port should keep this as a trait object or generic callback boundary to avoid allocations in non-empty-result code paths when possible.

## Verification Oracles

1. **WHEN** a procedure calls `build(...)` with `result=Optional.empty()`  
   **THEN** **SHALL** produce an empty/zero-output contract consistent with caller expectations.
2. **WHEN** a builder receives `metadata=Optional.empty()`  
   **THEN** **SHALL** not panic and shall render metadata-optional outputs.
3. **WHEN** the same algorithm returns non-empty result and timings  
   **THEN** **SHALL** convert configuration + timings to caller-visible payloads in a deterministic way.

## Rust Rewrite Notes

- **L1:** define `trait ResultBuilder` with associated types for configuration, raw result, caller result, metadata.
- **L2:** require deterministic conversion from `Option` for result/metadata.
- **L3:** keep this as the smallest shared interface between algorithm machinery and projection-facing outputs.

## Dependencies Read Next

- Concrete implementations in procedure result-builder classes (e.g., pathfinding/centrality/flow builders).
- `AlgorithmProcessingTimings` type.
- Procedure result wrapper types (`StandardModeResult`, `StandardStatsResult`, etc.).

## Dependents As Tests

- Each algorithm-specific builder should have property-based tests covering empty-result rendering.
- Procedure integration tests across stream/stats/write paths that exercise same underlying algorithms through shared timing/metadata flow.

## Open Questions

- Should metadata type be generalized beyond mutate/write into a separate enum to avoid generic type bloat?
- Should Rust use enum dispatch (`enum ResultBuilderKind`) or generic associated types for performance predictability?
