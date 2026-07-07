# 1021 verification_oracle DummyGraphStore

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | procedures/graph-catalog-facade/src/test/java/org/neo4j/gds/procedures/catalog/DummyGraphStore.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 399 |
| fan_in / fan_out | 2 / 26 |
| seed anchor | graph-projection-api/src/main/java/org/neo4j/gds/api/nodeproperties/ValueType.java |

## Why This File Matters

This is a minimal `GraphStore` test double for catalog DTO translation. It is intentionally "just enough" for `GraphInfo` and `GraphInfoWithHistogram` translation, so it defines the narrow graph-store surface needed to serialize catalog list/drop results.

## Public Contract

- Implements `GraphStore`.
- Returns database name `some database` and location `LOCAL`.
- Preserves constructor-provided creation and modification timestamps.
- Returns `nodeCount=2` and `relationshipCount=1`.
- Exposes schema with node label `A` and no properties.
- Exposes relationship type `REL`, directed, with no properties.
- Exposes old relationship schema map `{ REL: {} }`.
- Exposes no graph properties.
- Most other graph-store APIs fail fast with `UnsupportedOperationException("TODO")`.

## Fixture Graph Shape

- Logical two-node, one-directed-relationship graph.
- Node label: `A`.
- Relationship type: `REL`.
- Node, relationship, and graph property sets are empty.
- Database location is local.
- There is no usable adjacency API; `getGraph(...)` and `getUnion()` throw.
- `GraphInfo.create(...)` consumes only database info, counts, timestamps, and schema maps from this fixture.
- Density is `relationshipCount / (nodeCount * (nodeCount - 1))`, so `1 / (2 * 1) = 0.5`.

## Public Contract Evidence

- `databaseInfo` (`72`) returns database metadata.
- `schema` (`78`) returns the fixture schema.
- Node schema `entries` (`104`) exposes label `A`.
- Relationship schema `toMapOld` (`139`) exposes old-style `{REL={}}`.
- Relationship schema `entries` (`154`) exposes directed `REL`.
- `creationTime` (`193`) and `modificationTime` (`198`) preserve constructor values.
- `nodeCount` (`238`) returns `2`.
- `relationshipCount` (`302`) returns `1`.
- Seed `ValueType` defines enum variants, CSV/Cypher/fallback/visitor methods, array compatibility for `UNTYPED_ARRAY`, and CSV parsing behavior.

## Asserted Outputs And Errors

- `GraphInfoTest` asserts graph name `some graph`, database `some database`, density `0.5`, memory sentinel `memoryUsage=""`, and size sentinel `sizeInBytes=-1`.
- Timestamps are preserved.
- Counts are `nodeCount=2` and `relationshipCount=1`.
- Old schema is `{ nodes: {A: {}}, relationships: {REL: {}}, graphProperties: {} }`.
- New schema includes `REL.direction=DIRECTED`.
- `GraphInfoWithHistogramTest` asserts `databaseLocation=local`.
- Histogram wrapping preserves supplied degree-distribution keys `min`, `mean`, `max`, `p50`, `p75`, `p90`, `p95`, `p99`, and `p999`.
- Unsupported methods throw `UnsupportedOperationException("TODO")`.

## Memory And Storage Implications

- `GraphInfo.withoutMemoryUsage(...)` must emit `memoryUsage=""` and `sizeInBytes=-1`.
- This fixture is not a full graph storage object; `withMemoryUsage(...)` may touch unsupported methods through `MemoryUsage.sizeOf(graphStore)`.
- Treat it as a DTO translation fixture, not as an algorithm graph.

## Snapshot And Catalog Implications

- No catalog mutation occurs in this file.
- It proves catalog-list/drop output marshalling from `{ configuration, GraphStore }` to `GraphInfo`.
- Snapshot metadata includes database name/location, timestamps, schema, and counts.
- `ValueType` is the schema value-type wire-format anchor.

## Verification Oracles

1. **WHEN** `GraphInfo.withoutMemoryUsage(...)` receives this dummy store, **THEN** it SHALL return `memoryUsage=""` and `sizeInBytes=-1`.
2. **WHEN** counts are translated, **THEN** output SHALL expose `nodeCount=2`, `relationshipCount=1`, and `density=0.5`.
3. **WHEN** schema is translated, **THEN** old schema SHALL omit relationship orientation and new schema SHALL include `REL.direction=DIRECTED`.
4. **WHEN** histogram wrapping uses `computeGraphSize=false`, **THEN** it SHALL preserve the supplied degree-distribution map exactly and keep memory sentinels.
5. **WHEN** unsupported `GraphStore` surface is touched, **THEN** the fixture SHALL fail fast with `UnsupportedOperationException("TODO")`.

## Rust Rewrite Notes

- Use a narrow graph-store mock/fixture with explicit unsupported methods returning typed errors.
- Keep `GraphInfo` memory internally optional, but serialize Java-compatible sentinels where procedure output requires them.
- Implement `ValueType` as a closed enum with explicit CSV names, fallback values, and compatibility rules.
- Check whether Java's `UNKNOWN.csvName()` typo and `fromCsvName` error rendering are compatibility requirements.

## Dependencies Read Next

- `GraphInfo`
- `GraphInfoWithHistogram`
- `GraphStore`
- `GraphSchema`
- `ElementSchema`
- `MutableNodeSchemaEntry`
- `MutableRelationshipSchemaEntry`
- `DegreeDistribution`
- `MemoryUsage`
- `GraphProjectFromStoreConfig`
- `DefaultValue`

## Open Questions

- Should this fixture remain intentionally partial, or should Rust encode the reachable subset as a smaller trait?
- Should `withMemoryUsage(...)` have a separate full-store fixture?
- Is `ValueType.fromCsvName(...)` unsupported-value rendering bug-compatible behavior required?

## Coding Prompt Unlocked

Build Rust graph catalog DTO tests around a minimal graph-store fixture that proves counts, schema, density, timestamps, memory sentinels, histogram preservation, and unsupported-method behavior.
