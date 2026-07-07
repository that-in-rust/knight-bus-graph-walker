# 1013 verification_oracle GraphStoreToCsvExporterTest

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | io/csv/src/test/java/org/neo4j/gds/core/io/file/csv/GraphStoreToCsvExporterTest.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 814 |
| fan_in / fan_out | 0 / 34 |
| seed anchor | graph-projection-api/src/main/java/org/neo4j/gds/RelationshipType.java |

## Why This File Matters

This verifies CSV export as a durable graph-store snapshot: topology, labels, relationship types, properties, graph properties, schema metadata, database info, user sidecar, original IDs, graph capabilities, and concurrency behavior.

## Public Contract

- `GraphStoreToCsvExporter.create(...)` wires `GraphStore` plus `GraphStoreToFileExporterParameters` into CSV visitors for user info, graph info, schemas, mappings, capabilities, node data, relationship data, and graph properties.
- Parameters carry `exportName`, `username`, `defaultRelationshipType`, `concurrency`, and `batchSize`.
- `RelationshipType.ALL_RELATIONSHIPS` is the `__ALL__` sentinel; `RelationshipType.of(...)` and `RelationshipType.listOf(...)` are the type construction boundary.
- CSV assertions are order-insensitive for data rows and check expected files exist, not necessarily that no extra files exist.

## Fixture Graph Shape

- Main graph (`exportTopology`, line 141): undirected, persistent, four nodes: `a:A:B`, `b:A:B`, `c:A:C`, `d:B`.
- Node properties include longs and long arrays. Relationship properties include a long property name over 24 chars, plus `prop2`, `prop3`, and `prop4`.
- `REL1` has a self-loop plus `a-b` and `b-a`; undirected export doubles rows, including self-loop duplication.
- `REL2` has `b-c`, `c-d`, and `d-a`, also doubled by undirected export.
- Concurrent graph fixture (line 99) is natural orientation, four unlabeled nodes, six `REL1` relationships.
- No-property graph fixture (line 115) is reverse-oriented with labels `A/B/C` and `REL1..REL4`.
- Offset-ID fixture (line 723) uses original IDs `42` and `43`.

## Public Contract Evidence

- `exportTopology` (`141`) asserts topology file set, label mappings, node headers/data, and relationship headers/data.
- `shouldExportGraphProperties` (`247`) asserts graph property data and header export.
- `exportMultithreaded` (`301`) asserts sharded topology content under concurrency.
- `exportGraphPropertiesMultithreaded` (`378`) asserts a one-million-value graph property is split across four data files with no lost values.
- `exportSchemaAndDatabaseId` (`452`) asserts node schema, relationship schema, graph property schema, graph info, database info, and relationship counts.
- `exportUsername` (`649`) asserts `.userinfo` content.
- `exportSchemaWithoutProperties` (`677`) asserts schema-only export for graphs without properties.
- `shouldExportWithOffsetIds` (`722`) asserts original ID preservation.
- `shouldExportGraphCapabilities` (`776`) asserts `writeMode=LOCAL`.

## Asserted Outputs And Errors

- `label-mappings.csv` contains `label1,A`, `label2,B`, `label3,C`.
- Node CSVs preserve original IDs and encode missing property values as empty cells.
- Undirected relationships are emitted in both directions; the self-loop is duplicated.
- Graph property `graphProp` exports header `graphProp:long` and values `0`, `1`, `2`.
- Concurrent graph export preserves all four node IDs and six relationship pairs across shard files.
- One-million graph property export creates four data files under concurrency `4`; sorted exported values equal `0..999999`.
- Schema and database info preserve node schema, relationship schema, graph-property schema, node count, max original ID, and rel counts `REL1=6`, `REL2=6`.
- Username export writes `.userinfo` containing `UserA`.
- Capabilities export writes `writeMode` and `LOCAL`.
- No thrown error path is asserted by this file.

## Memory And Storage Implications

- Export writes a multi-file filesystem snapshot under `tempDir`.
- Graph-property export must stream or shard large values without loss under concurrency; test verification reads the shards back into memory, but production should not need to.
- Header writing must be concurrency-safe; the production exporter tracks header files across visitors.
- Relationship type and label mapping are part of reconstructable snapshot state, not only display metadata.

## Snapshot And Catalog Implications

- This file is primarily a filesystem snapshot oracle for graph-store state.
- Label/type mappings, schema files, graph info, capabilities, user info, node data, relationship data, and graph properties must be reconstructable.
- Graph properties are cleaned after each test by removing all graph property keys from the `GraphStore`.
- The `RelationshipType` seed is a value-object contract: equality and hashing are by identifier type and name.

## Verification Oracles

1. **WHEN** the main undirected graph is exported with `ALL_RELATIONSHIPS`, **THEN** the system SHALL create node, relationship, header, and label-mapping CSVs for all observed label/type partitions.
2. **WHEN** an undirected relationship is exported, **THEN** the system SHALL emit both directions and SHALL preserve the duplicated self-loop behavior asserted for `a -> a`.
3. **WHEN** graph property `graphProp` has values `0..N-1`, **THEN** the system SHALL export every value exactly once across one or more shard files.
4. **WHEN** schema metadata is exported, **THEN** the system SHALL include node, relationship, graph-property, graph-info, and capability files with value types, fallback values, property state, relationship counts, and write mode.
5. **WHEN** original node IDs are offset, **THEN** node and relationship CSV rows SHALL use original IDs.
6. **WHEN** `username` is non-empty, **THEN** the system SHALL write `.userinfo` with that exact username.

## Rust Rewrite Notes

- Model `RelationshipType` as a validated newtype with `Eq`/`Hash` by name and preserve the `__ALL__` sentinel.
- Keep schema/header ordering deterministic for snapshot tests.
- Preserve long property names and array serialization (`1;3;3;7` style) as compatibility behavior.
- Keep row assertions order-insensitive for parallel shards, but decide whether Rust compatibility should also assert that no extra files are produced.
- Stream graph-property export by shard; avoid collecting million-value graph properties except inside tests.

## Dependencies Read Next

- `GraphStoreToFileExporter`
- `CsvNodeVisitor`
- `CsvRelationshipVisitor`
- `CsvGraphPropertyVisitor`
- `CsvGraphInfoVisitor`
- `CsvNodeSchemaVisitor`
- `CsvRelationshipSchemaVisitor`
- `CsvGraphCapabilitiesWriter`
- `CsvToGraphStoreImporterIntegrationTest`

## Open Questions

- Is duplicated undirected self-loop export an intentional public compatibility contract or an artifact?
- Should Rust tests assert no extra files, unlike the existing `assertCsvFiles` helper?
- Relationship type mapping is wired but not directly asserted here; should importer tests define the stronger round-trip contract?

## Coding Prompt Unlocked

Build Rust CSV snapshot tests around graph-store export parity: topology files, label/type/schema files, graph properties, user info, original IDs, graph capabilities, and concurrency-safe sharding.
