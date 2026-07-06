# 25 procedure_surface MutateStub

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | procedures/facade-api/algorithms-facade-common/src/main/java/org/neo4j/gds/procedures/algorithms/stubs/MutateStub.java |
| lane | procedure_surface |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 25 |
| line_count | 55 |
| fan_in / fan_out | 99 / 2 |
| purpose | Capture procedure ABI: names, modes, columns, config parsing, errors, and unsupported behavior. |
| read_prompt | Read this entire file as a public GDS procedure surface contract. Extract procedure names, modes, columns, config parsing, errors, unsupported behavior, and verification fixtures. |

## Why This File Matters

- This is the core mutate-semantics contract shared by procedure and pipeline code paths.
- It defines a single abstraction for parse, estimate, and execute in mutate mode, which is where rewrite parity risk is highest.
- It is low code volume but architecturally central because it normalizes behavior across many algorithm families.

## Public Contract

- Generic interface:
  - `MutateStub<CONFIGURATION, RESULT>`.
- Required methods:
  - `CONFIGURATION parseConfiguration(Map<String, Object> configuration)`
  - `MemoryEstimation getMemoryEstimation(String username, Map<String, Object> configuration)`
  - `Stream<MemoryEstimateResult> estimate(Object graphName, Map<String, Object> configuration)`
  - `Stream<RESULT> execute(String graphName, Map<String, Object> configuration)`
- Imports anchor return/result types:
  - `org.neo4j.gds.mem.MemoryEstimation`
  - `org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult`

## Internal Mechanics

- The interface documents usage intent through comments:
  - `parseConfiguration`: parse + implicit validation + defaults/limits.
  - `getMemoryEstimation`: delegated memory estimation with username context.
  - `estimate`: Neo4j procedure-style estimate endpoint behavior.
  - `execute`: validation + default application + limit checks + guardrails.
- No implementation details are present; all behavior comes from implementing stubs.

## Storage / Runtime Behavior

- `MemoryEstimation` and `Stream<MemoryEstimateResult>` indicate allocation-aware estimate contracts.
- `Stream<RESULT>` indicates per-request streamed execution results for mutate mode.
- The interface itself has no mutable state.

## Failure / Incompatibility Surfaces

- The API does not specify checked exceptions, so parser/estimate/execute failures can surface at runtime by implementation, likely as configuration validation errors.
- Any caller expecting consistent defaults/limits semantics must ensure implementations of all variants remain aligned.
- Divergent behavior across stub implementations can break pipeline/procedure parity because this interface is a shared contract.

## Verification Oracles

1. **WHEN** `parseConfiguration` is called with user config in mutate mode
   **THEN** **SHALL** perform implicit validation/default application and reject invalid settings.
2. **WHEN** `getMemoryEstimation` is called
   **THEN** **SHALL** return a `MemoryEstimation` that includes user-scoped context (`username`) and is safe for estimate mode boundaries.
3. **WHEN** `estimate` is called
   **THEN** **SHALL** return `Stream<MemoryEstimateResult>` compatible with Neo4j procedure estimate protocol.
4. **WHEN** `execute` is called for valid graph + config
   **THEN** **SHALL** return a stream of mutate-mode results under normal execution/limit semantics.
5. **WHEN** this contract is implemented by multiple concrete stubs
   **THEN** **SHALL** preserve method signatures exactly across all families to keep pipelines and procedures interchangeable.

## Rust Rewrite Notes

- **L1:** define a trait `MutateStub<Configuration, Result>` with the same four methods and explicit error type.
- **L2:** return `impl Iterator`-style/`Box<dyn Iterator>` for result streams and explicit `MemoryEstimation` model types.
- **L3:** keep parse + estimate + execute as pure contracts; avoid cross-family default drift by centralizing common parse/limits middleware.

## Dependencies Read Next

- `MutateStub` implementations under:
  - `procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/stubs/*`
  - `procedures/algorithms-facade/*/stubs/*`
- Pipeline code that uses mutate stubs directly.

## Dependents As Tests

- Procedure tests for mutate mode that assert config parse/estimate/execute behavior in one shared path.
- Pipeline tests that verify `getMemoryEstimation`/`estimate` consistency with procedure estimate semantics.

## Open Questions

- Should Rust return `Result<Stream<_>, Error>` for parse/execute to expose parsing and validation failures explicitly?
- What concrete error taxonomy should cover username-related policy (limits/privileges) consistently in both mutation and pipeline flows?
