# 38 procedure_surface NewConfigFunction

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/machinery/src/main/java/org/neo4j/gds/procedures/algorithms/configuration/NewConfigFunction.java |
| lane | procedure_surface |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 38 |
| line_count | 30 |
| fan_in / fan_out | 66 / 2 |

## Why This File Matters

This interface is the procedural glue between raw procedure input (`Map`-backed `CypherMapWrapper`) and strongly-typed algorithm config instances.

## Public Contract

- **Evidence:** It defines a generic functional interface with a single method `CONFIG apply(String username, CypherMapWrapper config);` (`25-29`).
- **Inference:** Callers can pass lambdas/method references to bind each algorithm-specific config parser.
- **Evidence:** The generic bound ties to `AlgoBaseConfig` (`22`), so all returned configs share common base validation/settings contract.

## Internal Mechanics

- **Evidence:** No implementation exists; only abstraction contract (`25-29`).
- **Inference:** The interface is intended to be inlined via lambda, reducing boilerplate in spec classes.

## Memory And Storage Implications

- **Inference:** As a functional interface, runtime cost is in closure/lambda allocation and parser execution, not in container storage.
- **Evidence:** Parameter types are minimal and reused across algorithm suite, limiting adapter overhead.

## Snapshot And Catalog Implications

- **Evidence:** Every execution path using `AlgorithmSpec` pulls config through this function, so parser identity is part of compatibility surface.
- **Inference:** Config parser changes can alter accepted keys/defaults; parity tests must lock this edge.

## Verification Oracles

1. **WHEN** user input map is passed with valid keys **THEN** config parser **SHALL** return a valid `AlgoBaseConfig` subclass.
2. **WHEN** invalid keys/values are passed through parser **THEN** parser-specific exceptions **SHALL** be raised consistently.
3. **WHEN** `AlgorithmSpec` executes with no explicit config function override **THEN** compile-time generics still force a parse entry in implementation.

## Rust Rewrite Notes

- **L1:** define `type NewConfigFunction<Config> = Fn(&str, &CypherMapWrapper) -> Config`.
- **L2:** centralize parser wrappers into algorithm modules to preserve username-aware config semantics.
- **L3:** instrument parser errors at spec registration/dispatch layer for deterministic error shapes.

## Dependencies Read Next

- `config-api/src/main/java/org/neo4j/gds/config/AlgoBaseConfig.java`
- `core/src/main/java/org/neo4j/gds/core/CypherMapWrapper.java`
- `executor/src/main/java/org/neo4j/gds/executor/AlgorithmSpec.java`

## Dependents As Tests

- `algorithm-specifications` for every algorithm spec implementing parser dispatch.
- procedure tests that verify config parsing and error messages for edge keys.

## Open Questions

- Should parser be passed as owned closure vs trait object in Rust for performance vs dispatch flexibility?

## Coding Prompt Unlocked

Implement `NewConfigFunction` as typed function boundary in Rust:
1) username + config wrapper input,
2) deterministic typed parser mapping,
3) tests for at least two parser failure paths.
