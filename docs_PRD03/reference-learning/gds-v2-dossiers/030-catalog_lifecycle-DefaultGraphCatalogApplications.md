# 30 catalog_lifecycle DefaultGraphCatalogApplications

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/graph-store-catalog/src/main/java/org/neo4j/gds/applications/graphstorecatalog/DefaultGraphCatalogApplications.java |
| lane | catalog_lifecycle |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 30 |
| line_count | 1160 |
| fan_in / fan_out | 2 / 74 |

## Why This File Matters

This file is the business-facade hub for catalog lifecycle and GDS projection/catalog operations. It contains the orchestration path for graph create/project/list/drop/size/query/write/export/mutate entry points that are still part of the OLAP-facing compatibility contract and must retain behavior in a Rust rewrite.

## Public Contract

- Implements `GraphCatalogApplications` and exposes core lifecycle entry points:
  - `graphExists`, `dropGraph`, `listGraphs`, `nativeProject`, `estimateNativeProject`, `cypherProject`, `estimateCypherProject`
  - `subGraphProject`, `sizeOf`, `dropNodeProperties`, `dropRelationships`, `dropGraphProperty`
  - `mutateNodeLabel`, `streamGraphProperty`, `streamNodeProperties`, `streamRelationshipProperties`, `streamRelationships`
  - `writeNodeProperties`, `writeRelationshipProperties`, `writeNodeLabel`, `writeRelationships`
  - `sampleRandomWalkWithRestarts`, `sampleCommonNeighbourAwareRandomWalk`, `estimateCommonNeighbourAwareRandomWalk`
  - `generateGraph`, `exportToCsv`, `exportToCsvEstimate`, `exportToDatabase`
- Static `create(...)` composes all child applications and returns a ready facade (`lines 179-270`).
- `ensureGraphNameValidAndUnknown` enforces uniqueness on project/create flows (`lines 1146-1152`).
- Private parse utility `parseGraphNameOrListOfGraphNames` enforces typed validation on list-or-single forms (`lines 1155-1159`).
- Export path uses `getGraphStoreAndValidateForExport` to enforce read/access+property-conflict checks before running (`lines 1081-1093`).

## Internal Mechanics

- Object graph is injected in one constructor and composed from specialized child application components (`lines 80-176`, `118-177`).
- Most public methods follow the same control style:
  1. validate graph name(s) via `GraphNameValidationService`
  2. parse and validate configuration
  3. fetch catalog entry from `GraphStoreCatalogService` when needed
  4. delegate execution to one child application
  5. return procedure-facing result DTO
- Error and metric boundaries:
  - `nativeProject` and `cypherProject` wrap execution with projection metric handles and mark failures (`lines 346-362`, `419-435`).
  - `subGraphProject` and sampling paths follow similar metric + exception propagation (`lines 499-511`, `1124-1142`).
- The class intentionally avoids embedding low-level IO/transaction logic and keeps orchestration at facade-level.

## Memory And Storage Implications

- The class itself is mostly pointer/reference storage and delegation; it has minimal per-call heap allocations beyond config validation/dependency locals.
- Memory-sensitive edges are indirect through child components:
  - `NativeProjectApplication` / `CypherProjectApplication` do graph materialization; this facade invokes them after strict parse/validation gates.
  - `write*` and `drop*` calls pass full graph stores/results through callers and child services.
- Validation behavior:
  - Graph property drops call `graphStoreValidationService.ensureGraphPropertyExists` and then `graphStore.removeGraphProperty(...)` (`lines 628-637`), so error paths are explicit.
  - Export path rejects conflicting properties and disallows read/write mismatches (`getGraphStoreAndValidateForExport`, `lines 1087-1093`).

## Snapshot And Catalog Implications

- Lifecycle split is visible:
  - create-style operations first call `ensureGraphNameValidAndUnknown` to guarantee no overwrite (`lines 1146-1152`).
  - read/modify operations validate graph exists before access (e.g., `sizeOf`, `drop*`, `mutateNodeLabel`, `write*`) (`lines 514-520`, `595-597`, `652-654`).
- Configuration and graph metadata flows through catalog service calls using `CatalogRequest` and user/database context (`lines 32-37`, `482-487`, `595-596`, `1120-1122`).
- Projection/stream/write surfaces use a consistent config-first approach, so the rewrite should preserve schema compatibility between estimate and execute paths.

## Verification Oracles

1. **WHEN** `graphNameAsString` is blank during `nativeProject(...)` **THEN** it **SHALL** fail during validation via `ensureGraphNameValidAndUnknown`.
2. **WHEN** `dropGraph` receives a known graph list with `failIfMissing=false` **THEN** it **SHALL** continue using `DropGraphApplication.compute` and only error when child layer rejects unsupported identifiers.
3. **WHEN** `sizeOf` is called for missing graph name **THEN** it **SHALL** throw `IllegalArgumentException("Graph '<name>' does not exist")`.
4. **WHEN** cypher/native project execution throws **THEN** projection metrics **SHALL** record `failed(e)` before rethrow.
5. **WHEN** export is requested with additional node properties that already exist **THEN** `graphStoreValidationService.ensureNodePropertiesNotExist` **SHALL** reject before writing.

## Rust Rewrite Notes

- `L2/L3` module split suggestion:
  - `graph_catalog::graph_catalog_applications` trait + `DefaultGraphCatalogApplications` coordinator struct.
  - Inject immutable sub-services (`project`, `drop`, `stream`, `write`, `export`, `sampling`, `metrics`).
- Keep each public method as explicit orchestration boundary with one validation module + one execution module.
- Preserve exact exception shape in error mapping for:
  - missing graph names,
  - duplicate/unknown graph names,
  - export property/conflict checks.
- `request_scoped_dependencies` should be passed through execution and logging context rather than reconstructed per method.

## Dependencies Read Next

| File | Why |
| --- | --- |
| `applications/graph-store-catalog/src/main/java/org/neo4j/gds/applications/graphstorecatalog/CatalogConfigurationService.java` | core config parsing and validation reused across all lifecycle calls |
| `core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalogService.java` | catalog existence/create/drop/query semantics |
| `applications/graph-store-catalog/src/main/java/org/neo4j/gds/applications/graphstorecatalog/GraphMemoryUsageApplication.java` | size/memory query endpoint |
| `applications/graph-store-catalog/src/main/java/org/neo4j/gds/applications/graphstorecatalog/CypherProjectApplication.java` | cypher projection orchestration |
| `applications/graph-store-catalog/src/main/java/org/neo4j/gds/applications/graphstorecatalog/NativeProjectApplication.java` | native projection orchestration |
| `procedures/facade-api/graphstore-catalog-api` | integration boundary for public procedure calls |

## Dependents As Tests

| Caller / Test | Type | Why relevant |
| --- | --- | --- |
| `applications/facade/src/main/java/org/neo4j/gds/applications/ApplicationsFacade.java` | facade wiring | entry-point aggregation |
| `applications/graph-store-catalog/.../DefaultGraphCatalogApplicationsBuilder.java` | factory | verifies builder + constructor wiring |
| Integration tests under `applications/graph-store-catalog` | tests | exercise create/project/drop/memory/read/export contracts |

## Open Questions

- Should projection metrics and task registries be decoupled from this coordinator in Rust so they can be enabled/disabled per integration boundary?
- Which failure modes must preserve checked vs runtime exception semantics for public procedure parity?

## Coding Prompt Unlocked

Given this dossier, implement the first smallest Rust artifact that mirrors `DefaultGraphCatalogApplications` contract as:

1) A `GraphCatalogApplications` trait with all public methods above,  
2) A `DefaultGraphCatalogApplications` struct with injected sub-services and validation-first orchestration,  
3) tests for `graphExists`, `sizeOf`, one create/project path, and one export validation path.
