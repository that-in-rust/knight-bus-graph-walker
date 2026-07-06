# 35 memory_estimator MemoryEstimateDefinition

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimateDefinition.java |
| lane | memory_estimator |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 35 |
| line_count | 25 |
| fan_in / fan_out | 69 / 1 |

## Why This File Matters

This file declares the marker contract every estimator definition implementation plugs into for memory-estimation-aware APIs.

## Public Contract

- **Evidence:** The interface has one method `MemoryEstimation memoryEstimation();` (`22-25`).
- **Inference:** Implementers are required to expose estimator metadata that can be passed to estimation templates and procedure surfaces.
- **Blocked:** No additional lifecycle or validation methods are defined here; behavior depends on callers and callers of caller.

## Internal Mechanics

- **Evidence:** No implementation logic exists in this file (`22-25` only).
- **Inference:** This is intentionally a composition point, not a policy point.

## Memory And Storage Implications

- **Evidence:** It does not encode fields or state, only a contract for retrieval of `MemoryEstimation`.
- **Inference:** Memory contract in Rust should preserve this as a trait object/reference boundary where each concrete estimator returns a rich estimate tree.

## Snapshot And Catalog Implications

- **Evidence:** This marker is frequently used by estimator-backed classes and is therefore part of RAM-behavior compatibility surface.
- **Inference:** Any class implementing this should be treated as part of compatibility-critical memory contract registry.

## Verification Oracles

1. **WHEN** a class implements `MemoryEstimateDefinition` **THEN** it **SHALL** provide a concrete `MemoryEstimation` via `memoryEstimation()`.
2. **WHEN** the returned estimator is invoked by estimate templates **THEN** the estimation path **SHALL** be deterministic for identical inputs.
3. **WHEN** estimation is invoked without implementation (default / missing implementation) **THEN** integration should fail loudly at wiring time (not silently return zero).

## Rust Rewrite Notes

- **L1:** trait `MemoryEstimateDefinition` with method `memory_estimation()`.
- **L2:** implementors return trait-object backed `MemoryEstimation` handles.
- **L3:** integrate with existing memory accounting engine to keep estimator registry typed and testable.

## Dependencies Read Next

- `memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimation.java`
- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmEstimationTemplate.java`
- sample implementations listed by dependency graph:
  - `algo/src/main/java/org/neo4j/gds/*/*MemoryEstimateDefinition.java`

## Dependents As Tests

- `procedures/algorithms-facade` estimate paths consuming estimator definitions.
- algorithm specs that assert estimation contract behavior (`ApproxMaxKCutMemoryEstimateDefinition`, `BetweennessCentrality...`).
- regression tests that assert memory estimate non-nullity and unit coverage.

## Open Questions

- Should we add a default “unsupported” implementation and hard error path for classes that should not be estimated?

## Coding Prompt Unlocked

Implement memory estimate definitions in Rust by:
1. adding a trait for `memory_estimation()`;
2. ensuring all estimation-capable classes implement it;
3. adding compile-time checks / marker assertions that every estimate entrypoint has an estimator.
