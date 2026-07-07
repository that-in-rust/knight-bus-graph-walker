# 1020 verification_oracle MemoryEstimationExecutorTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | executor/src/integrationTest/java/org/neo4j/gds/executor/MemoryEstimationExecutorTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 175 |
| fan_in / fan_out | 0 / 28 |
| seed anchor | proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphProjectProc.java |

## Why This File Matters

This integration test verifies memory estimation from two entry points: an existing named catalog graph and an explicit graph-dimension map. It also locks the exact invalid relationship-filter error text.

## Public Contract

- Setup opens a procedure transaction, registers `GraphProjectProc`, and constructs `MemoryEstimationExecutor` with `TestMutateSpec`, `ProcedureExecutorSpec`, execution context, and database transaction context.
- `computeEstimate(Object, Map)` dispatches map inputs through graph-project config parsing/loading and string inputs through catalog graph loading.
- `GraphProjectProc` provides the procedure entry points used to project empty fixture graphs in the test harness.

## Fixture Graph Shape

- Named-graph tests use `GdlGraphs.EMPTY_GRAPH_STORE`.
- The empty graph is projected through `gds.graph.project(...).loadEverything()`.
- Invalid relationship-filter case sees available relationship type `__ALL__`.
- Explicit-dimension test uses all node and relationship projections with:
  - `nodeCount=100_000_000`
  - `relationshipCount=20_000_000_000`
  - no node properties

## Public Contract Evidence

- `setup` (`68`) registers `GraphProjectProc` and builds `MemoryEstimationExecutor`.
- `tearDown` (`99`) closes the transaction.
- `testMemoryEstimate` (`104`) checks named empty graph estimation.
- `failOnMemoryEstimationWithInvalidRelationshipFilterOnExplicitGraphStore` (`128`) checks invalid relationship filter error text.
- `testMemoryEstimateOnExplicitDimensions` (`145`) checks explicit graph dimension estimation.

## Asserted Outputs And Errors

- Named empty graph estimate returns `nodeCount=0`.
- Named empty graph estimate has `bytesMin > 0`.
- Named empty graph estimate has `bytesMax >= bytesMin`.
- Named empty graph estimate has non-null `mapView` and non-empty `treeView`.
- Invalid relationship filter throws exactly: `Could not find the specified `relationshipTypes` of ['INVALID']. Available relationship types are ['__ALL__'].`
- Explicit dimensions preserve exact counts: `nodeCount=100_000_000`, `relationshipCount=20_000_000_000`.
- Explicit dimensions produce two map components.
- First map component is named `graph`.
- Graph component memory usage renders `[21 GiB ... 58 GiB]`.
- Explicit estimate has `bytesMin > nodeCount + relationshipCount`, `bytesMax >= bytesMin`, and non-empty `treeView`.

## Memory And Storage Implications

- Named graph estimates include algorithm memory for an already-loaded catalog graph.
- Explicit graph config includes graph loading memory, hence the asserted `graph` map component.
- `MemoryEstimateResult` exposes required memory, tree view, map view, min/max bytes, graph counts, and heap percentages.
- A Rust rewrite should separate raw byte ranges from rendered human-readable strings and snapshot both when compatibility matters.

## Snapshot And Catalog Implications

- Tests manually insert graph stores into `GraphStoreCatalog`.
- Projection is performed through registered `GraphProjectProc`.
- Cleanup is manual through `GraphStoreCatalog.removeAllLoadedGraphs()`.
- Cleanup is not inside `finally` in the Java test, so a failed assertion can leak catalog state inside the test JVM.

## Verification Oracles

1. **WHEN** setup runs, **THEN** `GraphProjectProc` SHALL be registered before Cypher projection is invoked.
2. **WHEN** estimating a named empty graph, **THEN** result rows SHALL report `nodeCount=0` and positive memory bounds.
3. **WHEN** named-graph config requests `relationshipTypes=['INVALID']`, **THEN** estimation SHALL fail with the exact `__ALL__` availability message.
4. **WHEN** graph dimensions are supplied as a map, **THEN** estimation SHALL not require a catalog graph name.
5. **WHEN** explicit dimensions are 100M nodes and 20B relationships, **THEN** result counts SHALL preserve those exact values.
6. **WHEN** explicit graph loading memory is estimated, **THEN** `mapView.components[0]` SHALL be the `graph` component with `[21 GiB ... 58 GiB]`.

## Rust Rewrite Notes

- Use an enum like `GraphInput::CatalogName(String) | GraphInput::ProjectConfig(GraphProjectConfig)` instead of Java `Object`.
- Make catalog cleanup RAII-based.
- Snapshot invalid-filter formatting exactly; this is user-visible procedure behavior.
- Represent memory ranges as typed `{ min, max }` and render strings separately.
- Preserve the distinction between estimating an already-loaded graph and estimating an explicit graph projection.

## Dependencies Read Next

- `MemoryEstimationExecutor`
- `GraphStoreFromCatalogLoader`
- `MemoryEstimationGraphConfigParser`
- `FictitiousGraphStoreLoader`
- `GraphDimensionFactory`
- `ElementTypeValidator`
- `MemoryEstimateResult`
- `GraphStoreCatalog`
- `GdsCypher`
- `TestAlgorithmFactory`

## Open Questions

- Why does the named-graph test call both `GraphStoreCatalog.set(...)` and `gds.graph.project(...).loadEverything()` for the same name?
- Should catalog cleanup move to `@AfterEach` or `try/finally` in the Java test and RAII in Rust?
- Is the rendered `[21 GiB ... 58 GiB]` string stable enough for Rust parity, or should raw bytes be the primary oracle?

## Coding Prompt Unlocked

Build Rust memory-estimation executor tests around catalog-name input, explicit graph-dimension input, invalid relationship filters, raw byte ranges, rendered memory strings, map/tree output, and projection-procedure setup.
