# 56 olap_algorithm MutateStep

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/MutateStep.java |
| lane | olap_algorithm |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 56 |
| line_count | 36 |
| fan_in / fan_out | 49 / 2 |

## Why This File Matters

This is the mutate-step contract used by all algorithm-family write/mutate paths. It defines the only extension point for post-algorithm mutation side effects.

## Public Contract

- **Evidence:** Interface is generic over algorithm result and mutate metadata: `MutateStep<RESULT_FROM_ALGORITHM, MUTATE_METADATA>` (`25`).
- **Evidence:** Single method `execute(Graph, GraphStore, RESULT_FROM_ALGORITHM)` returns `MUTATE_METADATA` (`31-35`).
- **Evidence:** Javadoc states timing metadata belongs on outside layers, not inside `execute` (`26-30`).
- **Inference:** Implementations should focus solely on mutation and metadata reporting, with no timing/telemetry responsibility.

## Internal Mechanics

- **Evidence:** No methods other than `execute`, no fields, no static helpers (`20-36`).
- **Evidence:** `Graph` and `GraphStore` are inputs, indicating implementations can reason over both runtime graph view and stored graph state (`32-34`).
- **Inference:** This is intentionally minimal and highly testable—implementations are swappable and enforce consistent metadata return types.

## Memory And Storage Implications

- **Evidence:** Mutation metadata type is generic, so callsites define whether extra memory/reporting is returned (`31`).
- **Inference:** This abstraction is RAM-safe if implementations avoid capturing graph-wide state and return compact metadata objects.
- **Blocked:** Concrete memory cost sits in individual `WriteStep` implementations, not this interface.

## Snapshot And Catalog Implications

- **Evidence:** No catalog calls and no static/global state in this file (`20-36`).
- **Inference:** This seam is safe for catalog-parallelism boundaries and should remain a pure contract in rewrite.
- **Inference:** It is a stable boundary for algorithm-specific mutate outputs (`NodePropertiesWritten`, `HugeLongArray`, etc.) used upstream.

## Verification Oracles

1. **WHEN** any mutate algorithm writes result metadata, **THEN** implementation **SHALL** return that metadata via `MutateStep.execute(...)`.
2. **WHEN** mutation is executed via the interface, **THEN** no timing/telemetry logic **SHALL** be required by this method contract (per interface Javadoc intent `26-30`).
3. **WHEN** integrating mutate flows, **THEN** callsites **SHALL** preserve generic metadata typing to prevent erased-object errors.

## Rust Rewrite Notes

- **L1:** Represent as a trait with associated types for `ResultFromAlgorithm` and `MutateMetadata`.
- **L2:** Keep `execute(&self, graph: &Graph, graph_store: &GraphStore, result: &ResultFromAlgorithm) -> MutateMetadata` style with no timing concerns.
- **L3:** This trait should live in a machinery module used by all mutate mode paths to avoid duplication and preserve stable compile-time constraints.

## Dependencies Read Next

- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmProcessingTemplate.java`
- `applications/algorithms/centrality/src/main/java/org/neo4j/gds/applications/algorithms/centrality/*MutateStep.java`
- `applications/algorithms/community/src/main/java/org/neo4j/gds/applications/algorithms/community/*MutateStep.java`
- `applications/algorithms/*/src/main/java/org/neo4j/gds/applications/algorithms/*/mutate` families

## Dependents As Tests

- Trait conformance tests for one centrality and one community mutate-step impl.
- Mutation contract test that metadata type is propagated by callers.
- Compile-time surface test ensuring each algorithm mutate flow uses a strongly typed mutate step, not raw object.

## Open Questions

- Should Rust trait use `ResultToCaller` associated type or be fully generic at method-level?
- Is mutating step allowed to perform graph-store updates or only return metadata for a separate store writer?
- Should we enforce `!Send`/`!Sync` restrictions on mutators or permit cross-thread execution with explicit bounds?

## Coding Prompt Unlocked

Create a Rust `MutateStep` trait contract and replace callsites progressively:
1) define generic trait contract with explicit result and metadata types,
2) implement it for existing mutate steps,
3) ensure algorithm mutation pipeline passes metadata through the result builder unchanged,
4) add tests to keep timing/telemetry concerns out of the mutator contract.
