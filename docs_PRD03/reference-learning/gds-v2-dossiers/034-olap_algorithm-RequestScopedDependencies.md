# 34 olap_algorithm RequestScopedDependencies

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/RequestScopedDependencies.java |
| lane | olap_algorithm |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 34 |
| line_count | 50 |
| fan_in / fan_out | 62 / 9 |

## Why This File Matters

This is the minimal request-scope dependency carrier for algorithm execution layers. It compresses a frequently changing constructor signature into one typed value object.

## Public Contract

- **Evidence:** File defines `public record RequestScopedDependencies(...)` with 8 fields (`37-46`):
  - `DatabaseId`
  - `GraphLoaderContext`
  - `TaskRegistryFactory`
  - `TaskStore`
  - `TerminationFlag`
  - `User`
  - `UserLogRegistryFactory`
  - `UserLogStore`
- **Evidence:** `@GenerateBuilder` annotation is present (`36`) and explicit `builder()` delegating to generated builder exists (`47-49`).
- **Inference:** All consumers can use generated setters and immutable snapshots per request.

## Internal Mechanics

- **Evidence:** The file is a pure carrier type; no methods beyond builder factory and no mutable members (`37-46`, `47-49`).
- **Evidence:** Comment explicitly states the purpose is to avoid constructor churn in dependent classes (`32-35`).
- **Inference:** This is part of architectural adaptation layer: adding/removing dependency fields should mostly avoid API breakage in many call sites.

## Memory And Storage Implications

- **Inference:** As a Java record, per-request allocations are small and allocation-light (single object reference tuple).
- **Evidence:** No internals perform I/O or caching; memory behavior is purely pass-through.

## Snapshot And Catalog Implications

- **Inference:** No catalog semantics here. It is a context transport object feeding algorithm and procedure execution graphs.
- **Blocked:** Verify if all algorithm factories consume this record consistently in all execution modes after rewrite.

## Verification Oracles

1. **WHEN** constructing via `RequestScopedDependencies.builder()` and calling build, **THEN** the produced object **SHALL** carry all supplied fields.
2. **WHEN** this record is passed to execution pathways, **THEN** request boundaries **SHALL** include termination + user + graph/task/log context in one object.
3. **WHEN** new dependency fields are introduced, **THEN** existing builder API should limit edit surface compared to constructor spread.

## Rust Rewrite Notes

- **L1:** use an immutable struct with `#[derive(Clone)]` and `Default`/builder-style constructor (manual or derive).
- **L1:** explicit request-scope constructor with all fields; include helper for ergonomic construction.
- **L2:** builder helper for evolving dependency lists (mirrors Java record builder intent).
- **L3:** keep this as a shared module boundary to reduce constructor churn in `CommunityAlgorithms`/`CentralityAlgorithms`/`Pipeline` modules.

## Dependencies Read Next

- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmMachinery.java` (primary consumer).
- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/centrality/...` (constructor patterns likely use this context).
- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/path-finding/...`.
- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/nodeembeddings/...`.
- Any builders using `RequestScopedDependenciesBuilder` output.

## Dependents As Tests

- Execution/facade tests that validate context propagation.
- Integration tests around algorithm procedure entry paths where request context fields influence progress/task/logging behavior.
- Any tests constructing algorithm machinery for user/scope-sensitive operations.

## Open Questions

- Should this be split into sub-records (execution context vs logging context) to improve ownership and testability, or remain a monolith for constructor stability?

## Coding Prompt Unlocked

Implement `RequestScopedDependencies` as:
1. an immutable request context struct with explicit fields;
2. a generated/builder constructor to keep site edits small;
3. tests asserting no field gets dropped and each execution path receives the scoped context unchanged.
