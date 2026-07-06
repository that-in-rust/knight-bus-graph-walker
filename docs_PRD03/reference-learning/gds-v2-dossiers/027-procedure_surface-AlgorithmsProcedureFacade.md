# 27 procedure_surface AlgorithmsProcedureFacade

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | procedures/algorithms-facade-api/src/main/java/org/neo4j/gds/procedures/algorithms/AlgorithmsProcedureFacade.java |
| lane | procedure_surface |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 27 |
| line_count | 88 |
| fan_in / fan_out | 83 / 7 |
| purpose | Capture procedure ABI: names, modes, columns, config parsing, errors, and unsupported behavior. |
| read_prompt | Read this entire file as a public GDS procedure surface contract. Extract procedure names, modes, columns, config parsing, errors, unsupported behavior, and verification fixtures. |

## Why This File Matters

- This file is the algorithm-domain facade container used by pipeline orchestration and probably many procedure calls.
- It has low line count but high architectural impact: it is the typed seam for accessing all algorithm procedure families.
- Preserving it faithfully avoids wiring mismatch between procedure classes and domain behavior in the rewrite.

## Public Contract

- Concrete class (not interface):
  - `AlgorithmsProcedureFacade` in `org.neo4j.gds.procedures.algorithms`.
- Internal references to family facades:
  - `CentralityProcedureFacade`
  - `CommunityProcedureFacade`
  - `MachineLearningProcedureFacade`
  - `MiscellaneousProcedureFacade`
  - `NodeEmbeddingsProcedureFacade`
  - `PathFindingProcedureFacade`
  - `SimilarityProcedureFacade`
- Constructor-injected immutable dependencies:
  - all seven facade objects above.
- Accessor methods:
  - `centrality()`
  - `community()`
  - `machineLearning()`
  - `miscellaneous()`
  - `nodeEmbeddings()`
  - `pathFinding()`
  - `similarity()`

## Internal Mechanics

- The class is primarily a constructor dependency container:
  - stores each family facade in final fields
  - returns each via straightforward getter
- `PipelineApplications` and other callers use this as a stable domain boundary to navigate algorithm families.
- No direct parsing, estimation, streaming, or business logic appears here; all behavior is delegated to member facades.

## Storage and Runtime Behavior

- No mutable fields and no complex runtime state.
- It behaves as an accessor graph root for algorithm procedure surfaces.
- Memory overhead is minimal (references only).

## Failure / Incompatibility Surfaces

- No direct failure logic in this file; incompatibility risk is purely linkage-based:
  - wrong facade wiring at construction time will make downstream calls silently route to incorrect behavior.
  - construction/DI mismatch can cause null-like behavior at call sites if called without valid facade implementations.

## Verification Oracles

1. **WHEN** an `AlgorithmsProcedureFacade` is instantiated  
   **THEN** **SHALL** hold all seven family facade fields and expose them via corresponding accessors.
2. **WHEN** caller requests any family accessor (for example `centrality()` or `pathFinding()`)  
   **THEN** **SHALL** return the same instance that was injected in constructor.
3. **WHEN** called in pipeline orchestration  
   **THEN** **SHALL** provide a stable boundary for selecting the required family facade for compute/estimation operations.

## Rust Rewrite Notes

- **L1:** model as a small immutable struct of typed family façade handles.
- **L2:** constructor injection must be explicit to preserve compile-time wiring guarantees.
- **L3:** keep facade accessors as zero-logic getters to minimize hidden behavior.
- This module is a good candidate for `#[derive(Clone)]`/`Arc`-based shared ownership so the same object is injected everywhere.

## Dependencies Read Next

- All procedure sub-facades listed above, especially their method signatures in:
  - `procedures/algorithms/*/` facade and mode handler classes.
- `PipelineApplications.create(...)` consumers that pass this facade into computation constructors.

## Dependents As Tests

- Any integration test that exercises multiple algorithm domains via unified pipeline paths should verify facade routing.
- Constructor/unit tests validating object graph wiring for all accessor methods.

## Open Questions

- Should Rust enforce non-nullability of all facade fields at construction time and fail fast if absent?
- Should accessors return borrowed references (`&T`) or shared handles (`Arc<T>`) to avoid clone overhead under concurrent procedure calls?
