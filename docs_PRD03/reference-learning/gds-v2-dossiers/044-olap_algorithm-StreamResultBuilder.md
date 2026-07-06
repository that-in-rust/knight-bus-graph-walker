# 44 olap_algorithm StreamResultBuilder

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/StreamResultBuilder.java |
| lane | olap_algorithm |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 44 |
| line_count | 41 |
| fan_in / fan_out | 60 / 2 |

## Why This File Matters

This is the narrow seam between raw algorithm outputs and caller-facing result contracts. The interface drives how each algorithm mode shapes its return stream.

## Public Contract

- **Evidence:** Generic method signature `Stream<RESULT_TO_CALLER> build(Graph, GraphStore, Optional<RESULT_FROM_ALGORITHM>)` defines required context and optional algorithm output model (`29–40`).
- **Evidence:** The interface-level documentation explains it is injected and may use as much of gathered data as needed (`30–34`).
- **Inference:** Contract is intentionally minimal, with all behavior delegated to concrete builders.

## Internal Mechanics

- **Evidence:** No implementation code in interface; behavior purely from implementers, reducing coupling to algorithm internals (`29–40`).
- **Inference:** This is an open-ended adapter boundary where algorithm-specific renderers plug in.

## Memory And Storage Implications

- **Evidence:** Streaming return type (`Stream`) can be used to avoid list materialization of caller output (`36–40`).
- **Inference:** Implementations should choose lazy/iterator-like assembly to limit peak allocations.

## Snapshot And Catalog Implications

- **Evidence:** Input includes both `Graph` and `GraphStore`, so builders may use stored metadata if needed (`36–40`), but no direct catalog writes are mandated.
- **Inference:** This interface is compatible with stable snapshot IDs if result shaping reads graph metadata consistently.

## Verification Oracles

1. **WHEN** `build(...)` is called with empty `Optional`, **THEN** implementation **SHALL** still return a valid `Stream` (possibly empty).
2. **WHEN** graph output is optional and present, **THEN** implementation **SHALL** shape or map it into one or more caller-facing rows.
3. **WHEN** context includes `GraphStore`, **THEN** builder **SHALL** be free to read schema/metadata without mutating it.

## Rust Rewrite Notes

- **L1:** Core trait `StreamResultBuilder` with generic input/output types.
- **L2:** Return `impl Iterator` or stream-like abstraction to preserve lazy behavior.
- **L3:** Keep `Graph` and `GraphStore` references explicit in signature for mode-specific metadata reads.

## Dependencies Read Next

- Concrete builders implementing this interface under `applications/algorithms/*/`.
- Result/result-stream adapters in procedure-layer code.

## Dependents As Tests

- Result shaping tests that verify empty-result behavior and stream contract per algorithm mode.
- Tests for conversion of rich algorithm outputs into procedure output DTOs.

## Open Questions

- Should Rust use iterator semantics or buffered vector semantics for easier borrow-checking? Iterator preserves intent but affects API ergonomics.

## Coding Prompt Unlocked

Implement `StreamResultBuilder` in Rust as a trait over context+optional result:
1) keep optional-result handling explicit,
2) return lazy iterators,
3) add tests for empty optional and present result scenarios,
4) validate that builders remain serialization-safe across mode boundaries.
