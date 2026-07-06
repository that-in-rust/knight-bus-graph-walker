# 087 catalog_lifecycle GraphCatalogApplications

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/graph-store-catalog/src/main/java/org/neo4j/gds/applications/graphstorecatalog/GraphCatalogApplications.java |
| lane | catalog_lifecycle |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 87 |
| line_count | 296 |
| fan_in / fan_out | 9 / 31 |

## Why This File Matters

This interface is the comprehensive boundary for graph catalog lifecycle operations, streaming, filtering, mutation, and export across procedures.

## Public Contract

- Catalog query/mutation surface includes:
  - `graphExists`
  - `listGraphs`
  - `dropGraph`
  - `nativeProject`, `estimateNativeProject`
  - `cypherProject`, `estimateCypherProject`
  - `subGraphProject`
  - `sizeOf`
  - `dropNodeProperties`, `dropRelationships`, `dropGraphProperty`
  - `mutateNodeLabel`
- Read/stream operations:
  - `streamGraphProperty`
  - `streamNodeProperties`
  - `streamRelationshipProperties`
  - `streamRelationships`
- Write/export operations:
  - `writeNodeProperties`
  - `writeRelationshipProperties`
  - `writeNodeLabel`
  - `writeRelationships`
  - `sampleRandomWalkWithRestarts`, `sampleCommonNeighbourAwareRandomWalk`
  - `estimateCommonNeighbourAwareRandomWalk`
- Graph generation/export:
  - `generateGraph`
  - `exportToCsv`, `exportToCsvEstimate`
  - `exportToDatabase`

## Internal Mechanics

- Uses explicit dependency injection of exporters, task factories, termination flags and user/logging contexts in method signatures.
- Result types include strongly typed domain records (`FileExportResult`, `DatabaseExportResult`, sampling results etc.).
- Separation of concerns:
  - project estimate vs execute
  - mutation vs read streaming
  - single-node/relationship property writer paths

## Memory and Storage Implications

- This interface defines many mutable operations and thus marks a major side-effect boundary.
- Includes random-walk sampling estimate separation from execution (`estimateCommonNeighbourAwareRandomWalk`).
- Export and write methods need bounded buffers plus cancellation support through termination flags.

## Snapshot And Catalog Implications

- This is one of the most critical "catalog surface parity" contracts for rewrite: if changed, many tests will drift.
- In Rust, keep this as explicit trait with per-operation request/response structs and cancellation tokens.

## Verification Oracles

1. **WHEN** native/cypher estimation call is made, **THEN** execution-side method SHALL NOT be invoked.
2. **WHEN** graph is dropped, **THEN** deleted graph artifacts and entries SHALL disappear from catalog listing.
3. **WHEN** write methods receive unsupported termination state, **THEN** operation SHALL observe cancellation path via flag.
4. **WHEN** export methods are called, **THEN** response type SHALL match export channel (`csv` vs database).

## Rust Rewrite Notes

- Treat this as the canonical catalog boundary trait.
- Keep return types explicit and avoid untyped maps where possible.
- Implement generic stream producers with shared cancellation/token context.

## Dependencies Read Next

- `GraphStoreCatalogService`, `ModelCatalog`, and procedure-level facades that call these APIs
- Export builders and writer builders (`NodePropertyExporterBuilder`, `RelationshipExporterBuilder`)
- Termination and logging registries

## Dependents As Tests

- Contract tests per cluster:
  - catalog projection lifecycle
  - write/export pipeline
  - stream operations shape and cancellation behavior

## Open Questions

- Which side-effect paths should be made asynchronous in Rust for parity with task registry semantics?
- Should catalog operations be idempotent across missing resources in all layers?

