# 1019 verification_oracle GraphCatalogProcedureFacadeTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | procedures/graph-catalog-facade/src/test/java/org/neo4j/gds/procedures/catalog/GraphCatalogProcedureFacadeTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 477 |
| fan_in / fan_out | 0 / 29 |
| seed anchor | graph-projection-api/src/main/java/org/neo4j/gds/api/nodeproperties/ValueType.java |

## Why This File Matters

This is the catalog facade's optional-output contract. It proves `LocalGraphCatalogProcedureFacade` delegates request-scoped identity correctly and computes expensive/optional list fields only when procedure return columns ask for them.

## Public Contract

- `graphExists(graphName)` delegates to `GraphCatalogApplications.graphExists(user, databaseId, graphName)` using request-scoped user and database.
- `listGraphs` passes `displayDegreeDistribution` based on whether procedure return columns contain `degreeDistribution`.
- Memory fields are controlled by whether return columns contain `memoryUsage` or `sizeInBytes`.
- The seed `ValueType` defines CSV/Cypher/fallback value behavior for node/relationship property schemas.

## Fixture Graph Shape

- No real graph is loaded.
- `StubGraphStore` returns database `foo`, location `LOCAL`, empty schema, volatile timestamps, `nodeCount=42`, and `relationshipCount=87`.
- Most other `GraphStore` methods throw `UnsupportedOperationException("TODO")`, so tests only exercise DTO fields reachable from `GraphInfo.create`.
- The business facade is mocked and returns synthetic `GraphStoreCatalogEntry` values.

## Public Contract Evidence

- `shouldDetermineIfGraphExists` (`70`) verifies request-scoped delegation.
- `shouldListGraphsWithoutDegreeDistribution` (`97`) asserts omitted degree distribution.
- `shouldListGraphsWithDegreeDistribution` (`134`) asserts requested degree distribution.
- `shouldListGraphsWithoutMemoryUsage` (`183`) asserts memory fields are omitted by sentinel values.
- `shouldListGraphsWithMemoryUsage` (`223`) parameterizes over `memoryUsage` and `sizeInBytes` return columns.
- `StubGraphStore` (`261`) defines the minimal graph-store shape.

## Asserted Outputs And Errors

- If `degreeDistribution` is not requested, returned graph info has `degreeDistribution=null`.
- If `degreeDistribution` is requested, returned graph info contains exactly `{deg=117, ree=23, dist=512}`.
- If neither `memoryUsage` nor `sizeInBytes` is requested, returned graph info has `memoryUsage=""` and `sizeInBytes=-1`.
- If either memory return column is requested, both memory fields are populated: `memoryUsage="16 Bytes"` and `sizeInBytes=16`.
- Node and relationship counts come from `GraphStore.nodeCount()` and `GraphStore.relationshipCount()`.
- No thrown error path is asserted by this file.

## Memory And Storage Implications

- Optional return columns avoid `MemoryUsage.sizeOf(graphStore)` unless `memoryUsage` or `sizeInBytes` is requested.
- Sentinel no-memory values are wire-level compatibility values, not an internal absence type.
- A Rust rewrite should keep internal optional memory/histogram fields separate from serialized procedure output.

## Snapshot And Catalog Implications

- This test does not mutate `GraphStoreCatalog`.
- The catalog snapshot boundary here is a facade DTO projection from `GraphStoreCatalogEntry`.
- Return-column selection controls whether expensive fields are materialized.
- `ValueType` should be treated as a schema wire-format contract.

## Verification Oracles

1. **WHEN** `graphExists("some graph")` is called, **THEN** the facade SHALL delegate using request user `current user` and database `current database`.
2. **WHEN** `degreeDistribution` is not in return columns, **THEN** `listGraphs` SHALL ask the business facade with `displayDegreeDistribution=false` and return `degreeDistribution=null`.
3. **WHEN** `degreeDistribution` is requested, **THEN** `listGraphs` SHALL ask with `displayDegreeDistribution=true` and preserve the returned histogram map exactly.
4. **WHEN** neither `memoryUsage` nor `sizeInBytes` is requested, **THEN** results SHALL expose `memoryUsage=""` and `sizeInBytes=-1`.
5. **WHEN** either `memoryUsage` or `sizeInBytes` is requested, **THEN** both memory fields SHALL be populated from graph size.
6. **WHEN** translating graph info, **THEN** node and relationship counts SHALL come from `GraphStore.nodeCount()` and `GraphStore.relationshipCount()`.

## Rust Rewrite Notes

- Model return-column selection as a typed mask.
- Preserve wire sentinels if matching Neo4j output, but use internal `Option<MemorySize>` and `Option<HashMap<...>>` before serialization.
- Implement `ValueType` as a Rust enum with explicit CSV/Cypher/fallback methods.
- Check Java's `UNKNOWN.csvName()` and `fromCsvName` error-path behavior before copying supported-values rendering.

## Dependencies Read Next

- `LocalGraphCatalogProcedureFacade`
- `GraphInfo`
- `GraphInfoWithHistogram`
- `MemoryUsage.sizeOf`
- `Estimate.humanReadable`
- `GraphCatalogApplications`
- `GraphStore`
- `ValueType`

## Open Questions

- Should the Mockito default `returnColumns.contains(...) == false` be made explicit, as comments in the test suggest?
- Why does the memory-usage-positive test include a comment saying "do not want memory usage"?
- Is `ValueType.fromCsvName` intended to include `UNKNOWN` in supported-values rendering despite `UNKNOWN.csvName()` throwing?

## Coding Prompt Unlocked

Build Rust graph catalog facade tests around request-scoped delegation, return-column-driven optional fields, memory sentinels, degree-distribution inclusion, and schema value-type serialization.
