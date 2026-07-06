# 079 write_import_export WriteToDatabase

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/WriteToDatabase.java |
| lane | write_import_export |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 79 |
| line_count | 96 |
| fan_in / fan_out | 30 / 13 |

## Why This File Matters

This class is the write execution boundary from algorithm outputs into Neo4j property storage (single-property and bulk-property variants).

## Public Contract

- Constructor injects:
  - `Log`
  - `RequestScopedDependencies`
  - `WriteContext`
- Two overloads of `perform(...)`:
  1. single-property path writing one `NodePropertyValues`
  2. multi-property path writing `Map<String, NodePropertyValues>`
- Return value is `NodePropertiesWritten` in both paths.
- Inputs include graph, `GraphStore`, `ResultStore`, write config (`WriteConfig`), write metadata config (`WritePropertyConfig` on single overload), label, job id, optional property map.

## Internal Mechanics

- Delegates to:
  - `Neo4jDatabaseNodePropertyWriter.writeNodeProperty(...)` for single property
  - `Neo4jDatabaseNodePropertyWriter.writeNodeProperties(...)` for map write
- Both delegate to shared dependencies:
  - `writeContext.nodePropertyExporterBuilder()`
  - `requestScopedDependencies.taskRegistryFactory()`
  - `requestScopedDependencies.terminationFlag()`
  - `writeConfiguration.resolveResultStore(resultStore)`
- Multi-property overload uses map keys for label-based writing and resolves result store each call.

## Memory and Storage Implications

- This boundary is storage-heavy by nature (actual I/O + transaction-backed writes), but this class mostly delegates execution.
- `writeConcurrency` directly influences parallel writer behavior.
- Result-store capture allows consumers to see write results without materializing extra payloads.

## Snapshot And Catalog Implications

- This is an irreversible side-effect boundary and should be kept outside immutable OLAP read mode.
- In Rust rewrite, preserve explicit `write` path methods separate from stream/write-result transformations.

## Verification Oracles

1. **WHEN** single-property mutate result is requested, **THEN** `writeNodeProperty(...)` SHALL be called with write concurrency and resolved result store.
2. **WHEN** multi-property payload is requested, **THEN** `writeNodeProperties(...)` SHALL be called with all mapped property values.
3. **WHEN** write mode is active but termination requested, **THEN** termination flag SHALL be passed to writer.
4. **WHEN** `writeProperty` is absent, **THEN** write methods may remain uninvoked by upper-layer controls.

## Rust Rewrite Notes

- Implement this as a dedicated write gateway service.
- Keep both one-property and multi-property pathways explicit for type clarity.
- Return typed write-metadata object for downstream observability.

## Dependencies Read Next

- `Neo4jDatabaseNodePropertyWriter`
- `WriteContext`, `WriteNodePropertyConfig`, `WriteConfig`
- `RequestScopedDependencies`

## Dependents As Tests

- One-shot property write test with configured `writeConcurrency`.
- Map write test validating all node properties are exported and observed in result metadata.

## Open Questions

- Should this class include batching/backpressure behavior directly, or stay as a thin delegate to writer layer?
- Do we need separate result metadata for partial failure vs full failure paths?

