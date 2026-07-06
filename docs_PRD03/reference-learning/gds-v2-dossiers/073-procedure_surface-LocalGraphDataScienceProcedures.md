# 073 procedure_surface LocalGraphDataScienceProcedures

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | procedures/facade/src/main/java/org/neo4j/gds/procedures/LocalGraphDataScienceProcedures.java |
| lane | procedure_surface |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 73 |
| line_count | 285 |
| fan_in / fan_out | 4 / 42 |

## Why This File Matters

This class is the composition root for the public GDS procedure surface exposed to callers.

## Public Contract

- Implements `GraphDataScienceProcedures` and stores all top-level facade components (`68-75`, `241-259`).
- Static `create(...)` takes core dependencies across DB services, limits, metrics, defaults, catalogs, registries, and optional decorators (`100-125`).
- Builds `DatabaseGraphStoreEstimationService`, estimation and processing templates, and application facades before returning a built procedure aggregator (`130-235`).
- Exposes accessors for algorithms/graph catalog/model catalog/operations/pipelines (`241-259`).

## Internal Mechanics

- Optional decorators can override algorithm-processing and facade-specific behavior (`121-123`, `265-284`).
- `deprecatedProceduresMetricService` is part of construction state and part of returned API (`77`, `261-263`).

## Memory and Storage Implications

- Largest allocation is composition-time dependency graph.
- Runtime behavior should mostly be stable references into shared services.

## Snapshot And Catalog Implications

- Acts as the compatibility seam for all top-level procedure families; this is where surface stability is most likely checked by tests.

## Verification Oracles

1. **WHEN** decorators are empty, **THEN** default components SHALL be used.
2. **WHEN** decorators are present, **THEN** decoration function SHALL be applied.
3. **WHEN** accessors are called, **THEN** non-null facade references SHALL be returned.

## Rust Rewrite Notes

- Keep composition root explicit and deterministic.
- Preserve constructor argument breadth and optional decorator hooks.
- Provide thin accessor methods for procedure families.

## Dependencies Read Next

- `GraphDataScienceProceduresBuilder`
- `ApplicationsFacade.create(...)`
- `AlgorithmsProcedureFacadeFactory`, `GraphCatalogProcedureFacadeFactory`, `LocalPipelinesProcedureFacade`

## Dependents As Tests

- Integration verification that all facade getters return initialized dependencies.
- Decorator behavior tests for processing template replacement.

## Open Questions

- Are all decorator hooks required in stable Rust API or should they be gated behind feature flags?
