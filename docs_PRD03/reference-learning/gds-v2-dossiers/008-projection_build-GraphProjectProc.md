# 8 projection_build GraphProjectProc

## Source

**Evidence:** Full-source read performed as required by scope.


| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphProjectProc.java |
| lane | projection_build |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 8 |
| line_count | 124 |
| fan_in / fan_out | 244 / 7 |
| purpose | Capture the public graph projection procedure contract and deprecation behavior for project and estimate operations. |
| read_prompt | Read this entire file as a Projection Build Store contract. Extract the input shape, normalized facts, dense-id/label/type/property semantics, defaults, aggregation/orientation behavior, and validation rules. |

## Why This File Matters

**Evidence:** This procedure class is the primary Neo4j-side entrypoint for `gds.graph.project*` operations and therefore sits at the projection write-control boundary. [Lines 52-124]

## Public Contract

- `project(...)` procedure signature:
  - name = `"gds.graph.project"`, mode = `READ` [Lines 52-54]
  - params: `graphName`, `nodeProjection`, `relationshipProjection`, `configuration` [Lines 55-59]
  - delegates to `facade.graphCatalog().nativeProject(...)` [Lines 60-65]
- `projectEstimate(...)`:
  - name = `"gds.graph.project.estimate"` [Lines 68-70]
  - returns `MemoryEstimateResult` and delegates estimate call [Lines 75-76]
- `projectCypher(...)`:
  - deprecated procedure with warning log and delegation to `facade.graphCatalog().cypherProject(...)` [Lines 78-92]
- `projectCypherEstimate(...)`:
  - deprecated estimation path to `estimateCypherProject(...)` [Lines 94-107]
- `projectSubgraph(...)`:
  - internal+deprecated (`@Internal`, `@Deprecated(forRemoval = true)`, mode READ) [Lines 109-123]
  - logs warning, marks call with `deprecatedProcedures().called(...)`, delegates to `subGraphProject(...)` [Lines 120-123]

## Internal Mechanics

- **Evidence:** All methods are procedural dispatch wrappers around facade methods; this file contains no projection-state mutations itself [Lines 60,75,91,106,123].
- **Evidence:** `@Context` injects `GraphDataScienceProcedures facade` used by all handlers [Lines 42-43].
- **Inference:** This design pushes catalog-level validation and execution into another layer, likely to keep this class as a façade shell.
- **Evidence:** Deprecation warnings are emitted before delegation for deprecated calls [Lines 89-90,104-105].

## Memory And Storage Implications

- **Evidence:** Only estimate endpoint has explicit memory focus and returns `MemoryEstimateResult` [Lines 68-76, 97-107].
- **Inference:** The estimate functions are intended as preflight gates and are read-only pathways feeding projection workflows before catalog mutation.

## Snapshot And Catalog Implications

- **Evidence:** `project` and `estimate` are both procedure boundaries that likely mutate/validate catalog state through same façade [Lines 52-76].
- **Inference:** `project` includes `graphName` while `projectEstimate` does not, implying snapshot/mode distinction: estimates can be validated without graph creation.
- **Evidence:** Deprecated subgraph path is internal and may disappear; migration target is `gds.graph.filter` per description text [Lines 111,121].

## Verification Oracles

1. **WHEN** `project("g", nodeProj, relProj, config)` is invoked on a mock facade  
   **THEN** **SHALL** `nativeProject(...)` be called with matching arguments and the returned stream should contain projection execution results.

2. **WHEN** `projectEstimate(nodeProj, relProj, config)` is invoked  
   **THEN** **SHALL** forward the same projection inputs to `estimateNativeProject(...)`.

3. **WHEN** `projectCypher(...)` is invoked  
   **THEN** **SHALL** emit a warning log that mentions deprecation and still delegate to `cypherProject(...)`.

4. **WHEN** `projectSubgraph(...)` is invoked  
   **THEN** **SHALL** register a deprecated procedure call in `deprecatedProcedures().called(...)` and emit guidance warning.

## Rust Rewrite Notes

- **L1:** `GraphProjectProc` equivalent should expose explicit procedure handlers mapped to command structs (`Project`, `ProjectEstimate`, `ProjectCypher`, `ProjectSubgraphDeprecated`).
- **L2:** Keep deprecation metadata (`deprecated`, warning text) as explicit runtime behavior to preserve migration guidance.
- **L3:** `NativeProjectApplication`, `CypherProjectApplication` handlers likely belong in L3 (integration layer) with strict validation + catalog mutation hooks.
- **Safety note:** Preserve behavior where deprecated endpoints still succeed with warnings until removed.

## Dependencies Read Next

| target_file | reason |
| --- | --- |
| procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/GraphDataScienceProcedures.java | facade injection and dispatch |
| applications/graph-store-catalog/src/main/java/org/neo4j/gds/applications/graphstorecatalog/DefaultGraphCatalogApplications.java | likely backend catalog state transitions |
| applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmEstimationTemplate.java | memory-estimate consistency |
| memory-usage/src/main/java/org/neo4j/gds/applications/algorithms/machinery/MemoryEstimateResult.java | estimation shape contract |

## Dependents As Tests

| source_file | reason |
| --- | --- |
| doc-test/src/test/java/org/neo4j/gds/doc/GraphProjectDocTest.java | documents expected procedure contract |
| alpha/alpha-proc/src/integrationTest/java/org/neo4j/gds/EmptyGraphProcTest.java | integration path for projection behavior |
| algorithm-specifications/src/integrationTest/java/org/neo4j/gds/testproc/ProcedureFailTest.java | error and deprecation behavior probes |

## Open Questions

- Should `mode = READ` procedure annotations be preserved literally in Rust, or mapped to separate explicit read/write policy in handler traits?
- Should deprecated procedure wrappers be retained beyond planned compatibility windows in rewrite target?

## Coding Prompt Unlocked

Implement a Rust handler for `gds.graph.project` and `gds.graph.project.estimate` that forwards to a catalog facade and emits deprecation warnings for legacy paths; write integration tests for delegation, warning side-effects, and argument preservation.

