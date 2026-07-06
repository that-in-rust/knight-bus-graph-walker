# 7 procedure_surface GraphDataScienceProcedures

## Source

**Evidence:** Full-source read performed as required by scope.


| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/GraphDataScienceProcedures.java |
| lane | procedure_surface |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 7 |
| line_count | 44 |
| fan_in / fan_out | 246 / 7 |
| purpose | Define the central procedure facade contract exposed to procedure classes. |
| read_prompt | Read this entire file as a public GDS procedure surface contract. Extract procedure names, modes, columns, config parsing, result shapes, side effects, errors, unsupported cases, and verification fixtures. |

## Why This File Matters

**Evidence:** This interface enumerates all major API surfaces (algorithms, graph catalog, model catalog, operations, pipelines, metrics) in one contract, making it a key compatibility boundary for rewrite and facade orchestration. [Lines 30-44]

## Public Contract

- `log()` returns a `Log` handle [Line 31].
- `algorithms()` returns `AlgorithmsProcedureFacade` [Line 33].
- `graphCatalog()` returns `GraphCatalogProcedureFacade` [Line 35].
- `modelCatalog()` returns `ModelCatalogProcedureFacade` [Line 37].
- `operations()` returns `OperationsProcedureFacade` [Line 39].
- `pipelines()` returns `PipelinesProcedureFacade` [Line 41].
- `deprecatedProcedures()` returns `DeprecatedProceduresMetricService` [Line 43].
- This is a dependency-injection style seam for procedure implementations to obtain subsystem-specific facades [Lines 30-44].

## Internal Mechanics

- **Evidence:** No implementation here, only typed accessors for downstream procedure modules [Lines 30-44].
- **Inference:** The real control flow and side effects are delegated to the object graph behind `facade` fields in calling procedure classes.
- **Inference:** This split isolates public procedure contracts from orchestration implementation and supports test doubles in rewrite verification.

## Memory And Storage Implications

- **Evidence:** Interface itself is allocation-light (pure method references) [Lines 30-44].
- **Inference:** In Rust rewrite, this should map to borrowed trait-object accessors to avoid cloning large service graphs.

## Snapshot And Catalog Implications

- **Evidence:** Graph/model/catalog/algorithm operations are explicit named modules and therefore map to separate mutable domains [Lines 33-41].
- **Inference:** This contract is central to ensuring OLAP/OLTP separation: each facade can enforce its own read/write contract while sharing the same procedural entrypoint.

## Verification Oracles

1. **WHEN** a mocked facade object is injected into a procedure test harness  
   **THEN** **SHALL** each accessor return the corresponding mock facade and no extra accessor side effects occur.

2. **WHEN** a procedure path calls `deprecatedProcedures()`  
   **THEN** **SHALL** a metrics logger call be possible without coupling to other facade methods.

3. **WHEN** a null facade implementation is provided in negative tests  
   **THEN** **SHALL** a startup/initialization contract test fail early rather than on first API invocation.

## Rust Rewrite Notes

- **L1:** `trait GraphDataScienceProcedures` with accessor methods (`log`, `algorithms`, etc.).
- **L2:** Return lightweight façade traits for each subsystem (e.g., `dyn AlgorithmsProcedureFacade`).
- **L3:** Keep this in a dedicated procedure-facade API crate to preserve the API seam boundary.
- **Compatibility caution:** Preserve method names for parity and easier mapping from Java procedure fixtures.

## Dependencies Read Next

| target_file | reason |
| --- | --- |
| procedures/algorithms-facade-api/src/main/java/org/neo4j/gds/procedures/algorithms/AlgorithmsProcedureFacade.java | direct typed sub-facade |
| procedures/graph-catalog-facade-api/src/main/java/org/neo4j/gds/procedures/catalog/GraphCatalogProcedureFacade.java | graph catalog integration |
| procedures/model-catalog-facade-api/src/main/java/org/neo4j/gds/procedures/modelcatalog/ModelCatalogProcedureFacade.java | model lifecycle facade |

## Dependents As Tests

| source_file | reason |
| --- | --- |
| proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphProjectProc.java | procedure class implementing `@Procedure` methods |
| proc/catalog/src/main/java/org/neo4j/gds/beta/generator/GraphGenerateProc.java | uses facade for catalog operations |
| alpha/alpha-proc/src/main/java/org/neo4j/gds/userlog/UserLogProc.java | metrics/procedures path usage |

## Open Questions

- Should the Rust facade trait expose explicit async signatures if some subsystems perform blocking calls?
- Can `GraphDataScienceProcedures` be split into narrower interfaces to support more granular mocking?

## Coding Prompt Unlocked

Implement a Rust façade trait layer for procedure services with explicit test doubles; add compile-time mocks to confirm every accessor route is called by procedure classes with the expected lifecycle.
