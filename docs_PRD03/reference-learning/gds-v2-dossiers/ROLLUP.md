# GDS v2 Evidence Rollup

This file accumulates the cross-file conclusions from the complete-read queue.

Source queue:

```text
docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv
```

## Decisions Strengthened

- Projection primitives in `graph-projection-api` (`RelationshipType`, `NodeLabel`, `Orientation`) form a coherent immutable identity model: wildcard constants (`__ALL__`) are explicit sentinels and inversion/identity behavior is method-level and deterministic.
- `MemoryEstimation` is not a storage implementation; it is a behavioral contract for estimate composition and therefore should be rewritten as composable traits with safe arithmetic and explicit failure channels in Rust.
- `GraphProjectProc` confirms procedure surfaces are strict dispatch wrappers; catalog control logic remains in façade implementations, while procedure classes carry deprecation/compatibility behavior and delegation discipline.
- `GraphDataScienceProcedures` is the public façade seam; splitting it cleanly in Rust should be a first-order architecture boundary for testability and protocol stability.

## New Rust Module Candidates

- `memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimation.java`
  - `trait MemoryEstimation` with default `components()` and `times(...)`.
- `graph-projection-api/src/main/java/org/neo4j/gds/Orientation.java`
  - `enum Orientation` + `parse` helper with explicit error enum.
- `graph-projection-api/src/main/java/org/neo4j/gds/RelationshipType.java`
  - `enum`/`newtype RelationshipType` + wildcard constant + `project_all`.
- `graph-projection-api/src/main/java/org/neo4j/gds/NodeLabel.java`
  - `enum`/`newtype NodeLabel` + list parser helpers.
- `procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/GraphDataScienceProcedures.java`
  - `trait GraphDataScienceProcedures`.
- `proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphProjectProc.java`
  - facade-backed procedure command handlers with deprecation telemetry.
- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmProcessingTimings.java`
  - immutable `AlgorithmProcessingTimings` value type.
- `graph-projection-api/src/main/java/org/neo4j/gds/api/nodeproperties/ValueType.java`
  - `enum ValueType`, visitor trait, and parser returning explicit parse error type.

## Verification Specs Discovered

- Add focused tests for `times(...)` behavior of `MemoryEstimation` under neutral and scaled factors.
- Add parser tests for `Orientation.parse(...)` covering valid case-insensitive input, enum passthrough, and unsupported token diagnostics.
- Add façade contract tests for `GraphDataScienceProcedures` accessor wiring and `GraphProjectProc` delegation paths.
- Add `ValueType::from_csv_name` negative tests for unsupported types and unsupported `UNKNOWN`/`UNTYPED_ARRAY` csv conversions.

## Memory Accounting Terms

- `description`: human-facing estimator label.
- `components`: estimated memory subtree members.
- `estimate(...)`: returns `MemoryTree` from `GraphDimensions` + `Concurrency`.
- `factor`: multiplier from `times(long factor)`.
- `fallbackValue`: typed default for a value kind (`DefaultValue`).
- `cypherName` / `csvName`: external serialization names used by procedure/config interfaces.

## Projection/Catalog Invariants

- Procedure classes (`GraphProjectProc`) are intentionally thin dispatchers; catalog state transitions remain inside façade-backed `GraphDataScienceProcedures.graphCatalog()`.
- Estimate calls should be available before catalog mutation and should use the same projection input schema as write calls.
- Wildcard identifiers (`__ALL__`) in `RelationshipType` / `NodeLabel` imply broad reads that must be scoped by query context and permission policy before materialization.
- Deprecated projection APIs should preserve forward-compatible warning paths while being kept as explicit migration points.

## Unsupported Behavior Registry Candidates

- `Orientation.parse(null)` currently triggers `NullPointerException` through `input.getClass()` in unsupported-type branch.
- `ValueType.fromCsvName` rejects unknown CSV names with formatted exception, but has unsupported-csv behavior by design for `UNKNOWN` and some arrays.
- `ValueType.UNKNOWN.csvName()` and `ValueType.UNTYPED_ARRAY.csvName()` behavior are intentionally non-CSV-compatible and should be modeled explicitly in Rust to avoid silent coercion.
- `GraphProjectProc` deprecations still execute successfully with warnings; this migration behavior is an integration contract, not legacy noise to delete immediately.

## Next 10 Files

- 11 / `procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/community/LocalCommunityProcedureFacade.java` (procedure_surface)
- 12 / `core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java` (catalog_lifecycle)
- 13 / `procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/ProcedureConstants.java` (procedure_surface)
- 14 / `memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimations.java` (memory_estimator)
- 15 / `config-api/src/main/java/org/neo4j/gds/config/AlgoBaseConfig.java` (olap_algorithm)
- 16 / `memory-usage/src/main/java/org/neo4j/gds/mem/MemoryRange.java` (memory_estimator)
- 17 / `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/metadata/NodePropertiesWritten.java` (olap_algorithm)
- 18 / `memory-usage/src/main/java/org/neo4j/gds/mem/Estimate.java` (memory_estimator)
- 19 / `procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/centrality/LocalCentralityProcedureFacade.java` (procedure_surface)
- 20 / `graph-projection-api/src/main/java/org/neo4j/gds/core/Aggregation.java` (projection_build)
