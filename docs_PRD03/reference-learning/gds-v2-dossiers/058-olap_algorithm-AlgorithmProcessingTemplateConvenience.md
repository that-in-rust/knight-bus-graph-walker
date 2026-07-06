# 58 olap_algorithm AlgorithmProcessingTemplateConvenience

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmProcessingTemplateConvenience.java |
| lane | olap_algorithm |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 58 |
| line_count | 238 |
| fan_in / fan_out | 37 / 13 |

## Why This File Matters

This class is a thin convenience facade over `AlgorithmProcessingTemplate` that fixes common mode defaults for write/mutate/stats/stream execution.

## Public Contract

- **Evidence:** Holds one dependency: `AlgorithmProcessingTemplate algorithmProcessingTemplate` (`32-37`).
- **Evidence:** Public methods cover all modes:
  - write (`processAlgorithmInWriteMode`, `processRegularAlgorithmInWriteMode`) (`42-66`),
  - mutate (`processAlgorithmInMutateMode`, `processRegularAlgorithmInMutateMode`) (`94-144`),
  - stats (`processRegularAlgorithmInStatsMode`, `processAlgorithmInStatsMode`) (`149-192`),
  - stream (`processAlgorithmInStreamMode`, `processRegularAlgorithmInStreamMode`) (`194-237`).
- **Evidence:** “Regular” forms reduce optional flags to `Optional.empty` for relationship-weight override, validation hooks, and ETL hooks (`69-78`, `71-79`, `121-143`, `146-192`, `226-237`).
- **Inference:** The class is a mode convenience layer, not a place for algorithm semantics.

## Internal Mechanics

- **Evidence:** Each regular variant calls the corresponding verbose variant passing `Optional.empty()` for omitted hooks/config (`71-86`, `132-137`, `226-236`).
- **Evidence:** Generic type parameters bind `CONFIGURATION extends AlgoBaseConfig`, `RESULT_TO_CALLER`, `RESULT_FROM_ALGORITHM`, and per-mode metadata/result-builder types (`42-53`, `71-79`, `94-105`, `123-130`).
- **Evidence:** Delegation is one line deep and consistent across all modes.
- **Inference:** It reduces repetitive wiring bugs by centralizing the defaulting pattern for options.

## Memory And Storage Implications

- **Evidence:** No collection creation beyond parameter forwarding; this class is allocation-light and side-effect minimal (`32-237`).
- **Inference:** For hot paths, this wrapper should stay thin in Rust to avoid introducing per-call overhead.
- **Blocked:** Template internals (`processAlgorithmFor*`) hold the heavyweight memory and execution behavior.

## Snapshot And Catalog Implications

- **Evidence:** No direct graph storage operations in this layer; all catalog/context concerns are passed as parameters to delegates (`42-64`, `94-141`, `149-167`, `194-205`).
- **Inference:** The class is a stable cross-cutting seam for all mode executions and suitable for macro-level verification around mode transitions.

## Verification Oracles

1. **WHEN** regular write mode is used, **THEN** relationship weight override and hook options **SHALL** be `Optional.empty`.
2. **WHEN** regular mutate mode is used, **THEN** metadata path **SHALL** use `MutateStep` and `processAlgorithmForMutate(...)`.
3. **WHEN** regular stats mode is used, **THEN** `StatsResultBuilder` path **SHALL** execute via `processAlgorithmForStats(...)` and include no write/mutate step.
4. **WHEN** regular stream mode is used, **THEN** method **SHALL** return `Stream<RESULT_TO_CALLER>` from `processAlgorithmForStream(...)`.
5. **WHEN** every mode is executed through this class, **THEN** callers **SHALL** only pass mode-specific defaults and not duplicate boilerplate optional wiring.

## Rust Rewrite Notes

- **L1:** Keep a dedicated convenience module that wraps core template operations with default option sets.
- **L2:** Preserve mode-specific generic signatures and allow explicit variants for full option injection.
- **L3:** Keep this layer allocation-light by avoiding wrappers that clone callbacks or allocate option objects on hot calls.

## Dependencies Read Next

- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmProcessingTemplate.java`
- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/Computation.java`
- `.../WriteStep.java`, `.../MutateStep.java`, `.../StatsResultBuilder.java`, `.../StreamResultBuilder.java`
- hook classes: `core/src/main/java/org/neo4j/gds/core/loading/PostLoadValidationHook.java`, `PostLoadETLHook.java`

## Dependents As Tests

- One test per mode per facade path confirming convenience method calls delegate to verbose mode with empty options.
- Generic compile-time tests on mode signature stability across `Label`-typed algorithm dispatch.
- Performance test for stream/write/mutate call-site overhead to ensure convenience remains zero-overhead.

## Open Questions

- Should the Rust version expose only regular methods and make full-option variants internal?
- Do we need builder-style overloads for relation weight override in public API for future use?
- How should hook option absence be represented in Rust (Option types vs explicit no-op defaults)?

## Coding Prompt Unlocked

Implement an `AlgorithmProcessingTemplateConvenience`-style Rust façade:
1) wrap the core template with one dependency,
2) implement verbose mode methods and regular convenience methods that forward defaults consistently,
3) verify by tests that option defaults are injected exactly once and mode dispatch remains explicit.
