# 39 procedure_surface GenericStub

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/stubs/GenericStub.java |
| lane | procedure_surface |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 39 |
| line_count | 121 |
| fan_in / fan_out | 59 / 9 |

## Why This File Matters

This class is the procedure-facing orchestration adapter for configuration parse + estimation + execution flows.  
It is frequently reused by mutate/write/stream paths and therefore highly relevant to rewrite correctness.

## Public Contract

- **Evidence:** Constructor injects `UserSpecificConfigurationParser` and `AlgorithmEstimationTemplate` (`40-46`).
- **Evidence:** `parseConfiguration` delegates parse with defaults/limits via parser (`51-59`) and is the entry point for execution path.
- **Evidence:** `getMemoryEstimation` explicitly parses with `parseConfigurationWithoutDefaultsAndLimits(...)` (`69-75`).
- **Evidence:** `estimate` composes:
  1) `getMemoryEstimation`, then
  2) `parseConfiguration`, then
  3) `algorithmEstimationTemplate.estimate(...)`,
  returning `Stream.of(memoryEstimateResult)` (`80-97`).
- **Evidence:** `execute` parses config with defaults/limits (`110-113`), then calls `executor.compute(...)`, returning `Stream.of(result)` (`115-118`).
- **Evidence:** Method comment notes "**NB: no configuration validation hook**" on `execute` (`100-101`).
- **Inference:** Parse in `execute` plus preconditions in facades/stubs may be where validation actually occurs.

## Internal Mechanics

- **Evidence:** Generic method signatures avoid duplication across algorithm/config/result families (`51-107`).
- **Evidence:** Each method has explicit type plumbing through `Map<String,Object>` → `CypherMapWrapper` → config type.
- **Inference:** Generic design is stable but needs clear parser and estimator wiring to avoid type erasure confusion in Rust equivalents.

## Memory And Storage Implications

- **Evidence:** Each call materializes temporary config objects and one `Stream` wrapper for output.
- **Inference:** Stream-of-singletons is chosen for interface parity rather than list-building.
- **Blocked:** No explicit memory cap or streaming backpressure in this layer.

## Snapshot And Catalog Implications

- **Inference:** Not a catalog object itself; this is an execution adapter.
- **Evidence:** Uses `GraphName.parse(graphNameAsString)` and `algorithmEstimationTemplate.estimate(...)` bridging user intent to graph-bound behavior (`109`, `90-94`).

## Verification Oracles

1. **WHEN** `estimate(...)` is called with a raw config map **THEN** it **SHALL** parse without defaults-and-limits first and return at least one `MemoryEstimateResult` stream element.
2. **WHEN** `execute(...)` receives config input **THEN** it **SHALL** parse configuration before calling executor.
3. **WHEN** parser throws for invalid input **THEN** execution estimation path **SHALL** fail fast through parser/executor exception propagation.
4. **WHEN** executor returns result **THEN** this class **SHALL** stream that result as a single-element stream.

## Rust Rewrite Notes

- **L1:** generic execution adapter trait/struct with `parse_configuration`, `get_memory_estimation`, `estimate`, `execute`.
- **L2:** preserve the two-step parse distinction: strict (without defaults) for estimation and full parse for execution.
- **L2:** return iterator/stream abstraction for single-result outputs.
- **L3:** keep type parameters/associated types to avoid cast-heavy runtime paths.

## Dependencies Read Next

- `procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/configuration/UserSpecificConfigurationParser.java`
- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmEstimationTemplate.java`
- `memory-usage/src/main/java/org/neo4j/gds/applications/algorithms/machinery/MemoryEstimateResult.java`
- `procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/AlgorithmHandle.java`
- `memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimation.java`
- any concrete stub impl under `procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/**/stubs`

## Dependents As Tests

- `procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/*/stubs/*MutateStub.java`
- integration tests under `proc/community/src/integrationTest` and `proc/machine-learning/src/test` that assert estimate/execute semantics.
- pipeline executor tests that route through procedure facade stubs.

## Open Questions

- Could estimation and execution share a single parse path safely, or does parser variance justify strict behavior split as-is?
- How strict should Rust single-element stream semantics be (Iterator vs Vec of len 1)?

## Coding Prompt Unlocked

Recreate this adaptation seam in Rust:
1) one `GenericStub` with parse/estimate/execute methods;
2) distinct parse modes for estimate vs execute;
3) one-element iterator outputs;
4) tests that assert both estimate and execute paths are covered and preserve parsing order.
