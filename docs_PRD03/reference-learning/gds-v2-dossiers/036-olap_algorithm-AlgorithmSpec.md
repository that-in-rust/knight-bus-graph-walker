# 36 olap_algorithm AlgorithmSpec

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | executor/src/main/java/org/neo4j/gds/executor/AlgorithmSpec.java |
| lane | olap_algorithm |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 36 |
| line_count | 68 |
| fan_in / fan_out | 62 / 8 |

## Why This File Matters

This interface defines the generic contract for algorithm registration/execution in the procedure/executor pipeline.

## Public Contract

- **Evidence:** Generic declaration ties together algorithm type, result type, config type, procedure result, and factory type (`30-35`).
- **Evidence:** `name()` provides a stable string identity (`37`).
- **Evidence:** `algorithmFactory(ExecutionContext)` returns factory object for algorithm instantiation (`39-40`).
- **Evidence:** `newConfigFunction()` returns a parser/constructor function for algorithm config (`53`).
- **Evidence:** `computationResultConsumer()` returns the output adaptation handler (`55`).
- **Evidence:** `preProcessConfig(...)` default is no-op and explicitly allowed to mutate userInput (`49-51`).
- **Evidence:** `validationConfig(...)` defaults to `ValidationConfiguration.empty()` (`57-59`).
- **Evidence:** `createDefaultExecutorSpec()` defaults to `new ProcedureExecutorSpec<>()` (`61-63`).
- **Evidence:** `releaseProgressTask()` defaults to true (`65-67`), so pipeline can reclaim progress tasks by default.
- **Inference:** This is the schema-like center for algorithm surface behavior and should remain explicit in rewrite boundaries.

## Internal Mechanics

- **Evidence:** The interface is dependency-light and uses defaults to reduce boilerplate for high-volume algorithm definitions (`49-67`).
- **Inference:** Implementers inherit default executor and validation behavior while overriding where needed.
- **Evidence:** Generics encode compile-time consistency across algorithm-specific handler wiring.

## Memory And Storage Implications

- **Inference:** Memory-heavy parts are not in this interface but in implementation and outputs of factories/consumers.
- **Evidence:** Generic factory/consumer separation implies ephemeral execution object graphs per request rather than caching by interface.

## Snapshot And Catalog Implications

- **Inference:** This is a registry contract: algorithm registrations become discoverable and executable by procedure names (`name`) plus config parser (`newConfigFunction`).
- **Evidence:** Defaults encourage uniform behavior and reduce divergence between algorithm specs.

## Verification Oracles

1. **WHEN** a spec omits `validationConfig(...)` override **THEN** it **SHALL** inherit empty validation.
2. **WHEN** a spec omits `createDefaultExecutorSpec()` override **THEN** it **SHALL** get default `ProcedureExecutorSpec`.
3. **WHEN** a spec sets `releaseProgressTask()` to false **THEN** execution pipeline **SHALL** preserve/retain progress tasks.
4. **WHEN** `preProcessConfig` mutates user input **THEN** downstream parsing **SHALL** observe the mutated result.

## Rust Rewrite Notes

- **L1:** Generic trait `AlgorithmSpec<Algo, AlgoResult, Config, Result, Factory>` with associated types to preserve compile-time consistency.
- **L2:** default method implementations for config preprocessing, validation config, and executor spec construction.
- **L2:** trait composition around `new_config` + `consume_result` + `factory`.
- **L3:** registry entry type for procedure mode mapping and validation injection.

## Dependencies Read Next

- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery` registry-equivalent modules.
- `executor/src/main/java/org/neo4j/gds/executor/ProcedureExecutorSpec.java`
- `executor/src/main/java/org/neo4j/gds/executor/ComputationResultConsumer.java`
- `executor/src/main/java/org/neo4j/gds/executor/ExecutionContext.java`
- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/procedures/algorithms/configuration/NewConfigFunction.java`

## Dependents As Tests

- `algorithm-specifications/*` classes such as `*MutateSpec`, `*WriteSpec`, `*StreamSpec` (dozens).
- Procedure execution integration tests expecting validation and executor defaults.
- Any spec validation tests that assert no-op preprocessor paths and overridden config behavior.

## Open Questions

- Should validation/preprocess hooks be made mandatory per algorithm in Rust for stronger compile-time clarity, or keep them optional as defaults like Java?

## Coding Prompt Unlocked

Implement the `AlgorithmSpec` abstraction in Rust with:
1. generic trait bounds for algorithm/factory/result/config;
2. default hooks for validation and parser injection;
3. tests for default and overridden `release_progress_task` behavior;
4. at least one end-to-end spec registration test in the algorithm-spec suite.
