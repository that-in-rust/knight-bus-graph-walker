# 1024 verification_oracle ProcedureRunner

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | proc/test/src/main/java/org/neo4j/gds/ProcedureRunner.java |
| lane | verification_oracle |
| tier | T2_VERIFICATION_ORACLE_COMPLETE_READ |
| line_count | 198 |
| fan_in / fan_out | 2 / 25 |
| seed anchor | procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/GraphDataScienceProcedures.java |

## Why This File Matters

This is the legacy procedure test harness bridge. It reflectively instantiates `BaseProc` subclasses, injects Neo4j/GDS context fields, builds a local `GraphDataScienceProcedures` facade, and lets tests drive procedure methods without full procedure registration.

## Public Contract

- `instantiateProcedure(...)` requires a no-arg constructor and sets `BaseProc` context fields.
- Constructor failure becomes `RuntimeException("Could not instantiate Procedure Class ...")`.
- `applyOnProcedure(...)` derives `KernelTransaction`, creates a facade, instantiates the procedure, and invokes the consumer.
- `GraphDataScienceProcedures` exposes facade accessors: `log`, `algorithms`, `graphCatalog`, `modelCatalog`, `operations`, `pipelines`, and `deprecatedProcedures`.

## Fixture Shape

- No graph fixture is created by this file.
- Runner fixture is dependency shape:
  - database id
  - `GraphLoaderContext.NULL_CONTEXT`
  - provided task registry
  - `EmptyTaskStore`
  - `User(username, false)`
  - empty user-log registry/store
  - fresh `GraphStoreCatalogService`
  - fresh `OpenModelCatalog`
  - disabled memory guard and metrics
  - `MemoryTracker(Long.MAX_VALUE)`

## Public Contract Evidence

- `instantiateProcedure` (`59`) performs reflection and context injection.
- `applyOnProcedure` (`94`) derives transaction/facade and invokes the procedure consumer.
- `createGraphDataScienceProcedures` (`143`) builds the local facade.
- Seed interface methods expose `log`, `algorithms`, `graphCatalog`, `modelCatalog`, `operations`, `pipelines`, and `deprecatedProcedures`.
- `ProcedureRunnerTest.shouldValidateThatAllContextFieldsAreSet` checks coverage for `BaseProc` context fields.
- `ProcedureRunnerTest.shouldPassCorrectParameters` checks selected injected values.

## Asserted Outputs And Errors

- Reflective construction failure throws `RuntimeException` naming the procedure class.
- Companion test asserts every `@Context` field on `BaseProc` has a corresponding `instantiateProcedure` parameter type, excluding computed `KernelTransaction` and `ModelCatalog`.
- Companion test asserts `procedureTransaction`, `callContext`, `log`, `taskRegistryFactory`, and `username` are passed through exactly.

## Memory And Storage Implications

- Test facade disables memory enforcement via `MemoryGuard.DISABLED`.
- It uses `MemoryTracker(Long.MAX_VALUE, gdsLog)`.
- It uses fresh `GraphStoreCatalogService` and `OpenModelCatalog`.
- `EmptyTaskStore` means this harness does not preserve task-store side effects unless tests inject or inspect a different layer.

## Snapshot And Catalog Implications

- Catalog identity is request-scoped by database id and `User(username, false)`.
- `TestProcedureRunner` wraps calls in full-access transactions with empty username and empty task registry by default.
- This runner applies to `P extends BaseProc`; new-style procedures like `GraphProjectProc` use direct `@Context GraphDataScienceProcedures`.

## Verification Oracles

1. **WHEN** `instantiateProcedure(...)` is given a `BaseProc` subclass with a no-arg constructor, **THEN** it SHALL instantiate it and inject every required procedure context field.
2. **WHEN** reflective construction fails, **THEN** it SHALL throw a `RuntimeException` naming the procedure class.
3. **WHEN** `applyOnProcedure(...)` is called, **THEN** it SHALL derive the kernel transaction from the Neo4j transaction before creating the facade.
4. **WHEN** the local GDS facade is built, **THEN** request dependencies SHALL use the supplied username, database id, task registry, null graph loader, empty task/user-log stores, and non-admin user flag.
5. **WHEN** the local facade is built for tests, **THEN** memory guard and metrics SHALL be disabled and memory tracking SHALL use `Long.MAX_VALUE`.
6. **WHEN** the convenience `TestProcedureRunner` path is used, **THEN** it SHALL run in a full-access transaction with `ProcedureCallContext.EMPTY`, empty task registry, and empty username.

## Rust Rewrite Notes

- Prefer an explicit `ProcedureContext` struct over reflection and mutable public fields.
- Make unsupported test dependencies typed `Option` or fake services, not `null`.
- Represent disabled memory enforcement as `MemoryPolicy::Disabled`, not a max-value tracker.
- Separate legacy `BaseProc` harnesses from facade-injected procedure harnesses.
- Use RAII transaction/catalog cleanup around runner invocation.

## Dependencies Read Next

- `BaseProc`
- `ProcedureRunnerTest`
- `TestProcedureRunner`
- `LocalGraphDataScienceProcedures`
- `GraphDataScienceProceduresBuilder`
- `RequestScopedDependencies`
- `GraphCatalogProcedureFacadeFactory`
- `GraphStoreCatalogService`
- `MemoryGuard`
- `MemoryTracker`
- `ProcedureCallContextReturnColumns`

## Open Questions

- Should `ProcedureRunnerTest` assert the actual injected `graphDataScienceProcedures`, `metrics`, `databaseService`, `transaction`, and `userLogRegistryFactory`, not just parameter coverage/subset values?
- Which `LocalGraphDataScienceProcedures.create(...)` null arguments are intentionally unsupported in this harness?
- Should Rust tests avoid this top-level harness where the Java comment says granular local tests are preferred?

## Coding Prompt Unlocked

Build Rust procedure test harnesses around explicit procedure context construction, scoped transaction/facade creation, legacy/new-style procedure separation, disabled-memory test policy, and deterministic dependency injection.
