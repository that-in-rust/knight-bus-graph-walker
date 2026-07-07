# V029 Verification Oracle: CypherAggregationTest

Source: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/neo4j-gds-src/cypher-aggregation/src/integrationTest/java/org/neo4j/gds/projection/CypherAggregationTest.java`

Seed: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/neo4j-gds-src/graph-projection-api/src/main/java/org/neo4j/gds/api/DefaultValue.java`

## Why This File Matters

`CypherAggregationTest` is the broad integration oracle for Cypher aggregation projection through `gds.graph.project(...)`. It covers node and relationship ingestion, arbitrary IDs, schemas, labels, properties, config migration errors, catalog snapshots, write capability flags, alpha forwarding, and large/inverse ordering cases.

For the Rust rewrite, this file is not just "projection testing"; it is the compatibility contract for how Cypher-shaped rows become a graph store.

## Public Contract

- Setup registers `CypherAggregation`, `GraphDropProc`, `GraphListProc`, and `WccStreamProc`, then clears loaded graphs after each test.
- Node inputs may be Neo4j nodes or non-negative integer IDs.
- Node inputs reject negative IDs, relationships, paths, booleans, strings, maps, lists, nulls, and floats/doubles.
- Data config controls labels, node properties, relationship type/properties, undirected types, inverse-indexed types, and read concurrency.
- Graph existence and catalog retrieval via `GraphStoreCatalog` are part of the observable behavior.
- Query snapshots in `gds.graph.list` must preserve the executing query even when parameters are used.

## Fixture Graph Shape

- Static graph contains eight `:A` nodes, six `:B` nodes `a..f`, one `:DifferentLabel`, and one unlabeled node with `{foo: 42}`.
- `:A` fixture includes four `:A` to `:A` `REL` edges, one `:A` to `DifferentLabel` `REL`, and one `DISCONNECTED` edge.
- `:B` fixture has node properties `prop1`, optional `prop2`, optional double-array `prop3`, optional long-array `prop4`, plus four `REL` edges where one relationship lacks `prop`.
- Larger fixtures include Cora CSV rows, a 10,000-node random graph, and an inverse-id graph with 1000 extra `:A` nodes.

## Important Assertions

- `testMatch` projects only connected `:A` pairs: six nodes and four relationships.
- `testArbitraryIds` returns 50 nodes and 25 relationships, with deterministic mapped/original ID checks.
- `testOptionalMatch` preserves all eight `:A` nodes while retaining only four `:A` relationships.
- `testDifferentPropertySchemas` locks per-label and per-relationship-type property schemas: empty/`x`/`y`, empty/`weight`/`hq`.
- `testPropertiesOnEmptyNodes` verifies missing node property fallback by filtering out `DefaultValue.forLong().longValue()` and seeing only `42`.
- `testNodeProperties` locks value types `LONG`, `DOUBLE`, `DOUBLE_ARRAY`, `LONG_ARRAY` and exact projected values/defaults.
- Relationship property tests assert `Double.NaN` fallback for missing relationship properties and aggregation outputs for `avg`, `sum`, `max`, and `min`.
- Error oracles include exact messages for negative ID, invalid ID types, invalid labels/properties, split-map migration, unknown keys/suggestions, empty graph name, and invalid undirected/inverse relationship types.

## Memory And Storage Implications

Projection materializes named `GraphStoreCatalog` entries and schemas. Cora projects 2708 nodes and `rows.size()` relationships; the large graph projects 10,000 nodes.

Relationship property columns are optional and use `Double.NaN` as traversal fallback. Arbitrary-ID projections disable write-back capability, while Neo4j-node projections allow local database writes.

The relevant Rust design implication is that projection should build compact graph storage through typed inputs and typed defaults, not through unstructured `Object`-like maps that are interpreted late.

## Snapshot And Catalog Implications

Returned configuration snapshots include:

- `jobId`
- `logProgress`
- `query`
- `readConcurrency = 4`
- empty inverse relationship type lists unless configured
- empty undirected relationship type lists unless configured

`gds.graph.list` must preserve the projection query string even when params are supplied.

## Verification Oracles

1. WHEN projecting `MATCH (s:A)-[:REL]->(t:A)`, THEN the graph SHALL contain six nodes and four relationships.
2. WHEN projecting arbitrary integer IDs from `range(13,37)`, THEN the result SHALL report 50 nodes and 25 relationships and mapped IDs SHALL round-trip to original IDs.
3. WHEN node properties are missing, THEN typed defaults SHALL be used, including `DefaultValue.forLong().longValue()` for absent long values.
4. WHEN relationship property `prop` is absent, THEN traversal with default `Double.NaN` SHALL surface `NaN` rather than failing.
5. WHEN arbitrary IDs are used, THEN graph capabilities SHALL report `canWriteToLocalDatabase = false`; Neo4j node inputs SHALL report `true`.
6. WHEN invalid config shapes or legacy split maps are supplied, THEN exact migration/suggestion error messages SHALL be preserved.

## Rust Rewrite Notes

Use typed enums such as `ProjectedNodeInput::NeoNodeId` and `ProjectedNodeInput::ArbitraryId`, and reject invalid values before graph build.

Model `DefaultValue` as typed fallbacks rather than raw dynamic values. Catalog cleanup should be RAII-based. Error strings should be snapshot-tested because these are public procedure UX, not internal implementation details.

## Dependencies To Read Next

- `CypherAggregation`
- `GraphProjectFromCypherAggregationConfig`
- `GraphProjectFromCypherAggregation`
- `GraphStoreCatalog`
- `ValueType`
- `DefaultValueUtil`
- `GraphProjectProc`
- `GraphListProc`
- `WccStreamProc`

## Open Questions

- Should the Rust rewrite preserve the skipped final-loop behavior in `testArbitraryIds`, where the assertion query includes `37` but the verification loop uses `< 37`?
- Should alpha forwarding remain an explicit compatibility surface or become migration-only?
- Should exact config `jobId` shape be snapshot-tested beyond the Java matcher's `any(String.class)`?
