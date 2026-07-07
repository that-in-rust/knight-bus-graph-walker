# 1023 verification_oracle GraphProjectProcTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | proc/catalog/src/test/java/org/neo4j/gds/catalog/GraphProjectProcTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 1594 |
| fan_in / fan_out | 0 / 26 |
| seed anchor | proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphProjectProc.java |

## Why This File Matters

This is the broad public compatibility oracle for `gds.graph.project` and deprecated `gds.graph.project.cypher`: projection desugaring, result columns, catalog insertion/removal, property/default loading, aggregation semantics, invalid input errors, and failure cleanup.

## Public Contract

- `GraphProjectProc` exposes `gds.graph.project`, `gds.graph.project.estimate`, deprecated `gds.graph.project.cypher`, deprecated `gds.graph.project.cypher.estimate`, and deprecated/internal `gds.beta.graph.project.subgraph`.
- Procedure bodies delegate to `facade.graphCatalog()`, except subgraph also records deprecated usage and warns.
- Native projection returns `nodeProjection` and `relationshipProjection`.
- Cypher projection must not return native projection columns.
- Successful projection stores a graph in `GraphStoreCatalog`; failed projection must not.

## Fixture Graph Shape

- Default setup registers `GraphProjectProc` and `TestProc`.
- Base graph is `(:A {age: 2})-[:REL {weight: 55}]->(:A)`.
- Cleanup removes all loaded graphs after each test.
- Individual tests add shapes for wildcard labels/types, parallel relationships, missing properties, array properties, and relationship aggregation graphs.

## Public Contract Evidence

- `createNativeProjection` (`123`) checks default native output.
- `createCypherProjection` (`156`) and `createCypherProjectionWithParameters` (`198`) check Cypher output.
- `nodeProjectionVariants` (`269`) and variants source (`1406`) define node projection desugaring.
- `nodeProjectionWithProperties` (`290`) and variants source (`1426`) define node property projection shapes.
- `relationshipProjectionVariants` (`365`) and variants source (`1488`) define relationship projection desugaring.
- `relationshipProjectionOrientations` (`388`) checks orientation handling.
- `relationshipProjectionWithProperties` (`429`) checks relationship property projection.
- `relationshipProjectionPropertyPropagateAggregations` (`492`) and `relationshipProjectionPropertyAggregationsNativeVsCypher` (`601`) check aggregation semantics.
- `loadMultipleNodeProperties` (`797`) and `loadMultipleRelationshipProperties` (`827`) check multi-property loading.
- `failsOnEmptyProjection` (`1089`), `failsOnBothEmptyProjection` (`1101`), `failsOnInvalidPropertyKey` (`1117`), and `failsOnWriteQuery` (`1273`) check validation.
- `clearTasksOnFailure` (`1385`) checks task cleanup.

## Asserted Outputs And Errors

- Native default output includes graph name, normalized node/relationship projections, `nodeCount=2`, `relationshipCount=1`, and `projectMillis`.
- Cypher output includes graph name, node/relationship query text, counts, and `projectMillis`.
- Invalid relationship load errors contain `Failed to load a relationship because its target-node`.
- Exact validation messages cover empty projections, missing `id`/`source`/`target`, invalid labels/types, invalid aggregation/orientation, write queries, wrong default-value types, and invalid `UNDIRECTED + indexInverse`.
- Failure with unsupported string property leaves `TaskStore.isEmpty()` true.
- Native versus Cypher aggregation cases agree where asserted.

## Memory And Storage Implications

- Successful projections populate `GraphStoreCatalog`.
- Property projections materialize default values and validate scalar versus array defaults.
- Aggregation changes stored relationship property graphs, including `COUNT`, `SUM`, `MIN`, `MAX`, missing-property behavior, explicit `NaN`, and non-NaN default behavior.
- Saturated worker pool still must load graph and return counts.

## Snapshot And Catalog Implications

- Catalog state is global enough to require `GraphStoreCatalog.removeAllLoadedGraphs()` after each test.
- Tests inspect loaded graph unions and property-specific graphs through `GraphStoreCatalog.get(...)`.
- Failed projection must not create catalog entries.
- Task cleanup after projection failure is part of the procedure reliability contract.

## Verification Oracles

1. **WHEN** native projection receives graph name `name`, node label `A`, and relationship type `REL`, **THEN** it SHALL return normalized projection maps, `nodeCount=2`, `relationshipCount=1`, and store the graph in the catalog.
2. **WHEN** Cypher projection uses valid node and relationship queries, **THEN** it SHALL return query text plus counts, store the graph, and omit native projection columns.
3. **WHEN** projection config contains aliases, lists, wildcard labels/types, orientations, or `indexInverse`, **THEN** result projection maps SHALL preserve the exact desugared wire shape.
4. **WHEN** node or relationship properties include defaults, arrays, missing values, or aggregations, **THEN** the loaded graph SHALL expose the asserted property graph values exactly.
5. **WHEN** invalid config or invalid Cypher columns are supplied, **THEN** the procedure SHALL fail with the asserted user-facing error and SHALL NOT leave a graph in the catalog.
6. **WHEN** projection loading fails after task creation begins, **THEN** task cleanup SHALL leave `TaskStore.isEmpty()` true.

## Rust Rewrite Notes

- Replace Java `Object`/`Map` projection configs with typed enums plus a compatibility deserializer.
- Snapshot exact wire maps and exact error strings; these are public procedure behavior.
- Use RAII catalog cleanup in tests instead of global teardown-only cleanup.
- Treat `NaN`, array defaults, and `COUNT property:'*'` as explicit semantic cases.
- Keep deprecated Cypher behavior if compatibility requires it, including warnings and omitted projection columns.

## Dependencies Read Next

- `LocalGraphCatalogProcedureFacade`
- `GraphCatalogProcedureFacade`
- `GraphProjectNativeResult`
- `GraphProjectCypherResult`
- `GraphProjectFromStoreConfig`
- `GraphProjectFromCypherConfig`
- `GraphStoreCatalog`
- `RelationshipProjection`
- `PropertyMapping`
- `Aggregation`
- `Orientation`
- `BaseProcTest`

## Open Questions

- `invalidGraphNames()` is defined but unused here; is graph-name validation covered elsewhere?
- `loadGraphWithSaturatedThreadPool` still has a TODO; is this oracle strong enough for Rust parity?
- Should Rust preserve every deprecated Cypher projection behavior, or gate it behind a compatibility layer?

## Coding Prompt Unlocked

Build Rust graph projection procedure tests around native and Cypher projection compatibility, projection config desugaring, property/default/aggregation loading, catalog insertion failure cleanup, exact errors, and task cleanup.
