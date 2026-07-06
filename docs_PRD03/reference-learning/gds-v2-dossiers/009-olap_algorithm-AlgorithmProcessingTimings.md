# 9 olap_algorithm AlgorithmProcessingTimings

## Source

**Evidence:** Full-source read performed as required by scope.


| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmProcessingTimings.java |
| lane | olap_algorithm |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 9 |
| line_count | 35 |
| fan_in / fan_out | 211 / 0 |
| purpose | Capture a canonical per-run timing payload for algorithm lifecycle measurements. |
| read_prompt | Read this entire file as an algorithm execution contract. Extract lifecycle stages, required graph views, algorithm state, result artifacts, memory estimation hooks, mutate/write/stream/stats behavior, and verification cases. |

## Why This File Matters

**Evidence:** This class is a compact value object carrying algorithm-phase durations (`preProcessingMillis`, `computeMillis`, `sideEffectMillis`) and is referenced by rendering/result layers. [Lines 25-35]

## Public Contract

- Public fields:
  - `preProcessingMillis` [Lines 26-27]
  - `computeMillis` [Lines 27-28]
  - `sideEffectMillis` [Lines 28-29]
- Constructor:
  - package-private visibility (no `public`) [Lines 30-34]

## Internal Mechanics

- **Evidence:** The class is `public`, but object construction is package-restricted via default constructor visibility [Lines 25-35].
- **Evidence:** A comment indicates the class is intended as a finalised timing aggregate [Line 23].
- **Inference:** Consumers likely treat this as immutable output DTO for algorithm reporting and cannot freely instantiate from external packages.

## Memory And Storage Implications

- **Evidence:** Fixed three `long` fields, no collection/heap allocations in structure itself [Lines 26-34].
- **Inference:** Because fields are `long`, this object is stable for snapshot-friendly telemetry and low-overhead logging.
- **Inference:** Constructor package visibility hints at controlled allocator patterns and avoids constructing invalid states from arbitrary API consumers.

## Snapshot And Catalog Implications

- **Evidence:** The file has no catalog identifiers, so it acts as a reporting artifact, not lifecycle state holder [Lines 25-35].
- **Inference:** Timings are useful for auditability and could be persisted with job metadata to compare algorithm execution behavior across snapshots.

## Verification Oracles

1. **WHEN** an `AlgorithmProcessingTimings` instance is created with `(10, 20, 5)` inside `applications.algorithms.machinery` package tests  
   **THEN** **SHALL** expose those exact millis values on public fields.

2. **WHEN** this object is consumed by a result renderer test  
   **THEN** **SHALL** total wall clock equal `preProcessingMillis + computeMillis + sideEffectMillis`.

3. **WHEN** a downstream timing assertion requests struct fields directly  
   **THEN** **SHALL** require same-package test scope or helper constructor in Rust migration.

## Rust Rewrite Notes

- **L1:** `#[derive(Clone, Copy)] struct AlgorithmProcessingTimings { pre_processing_millis: u64, compute_millis: u64, side_effect_millis: u64 }`
- **L2:** Add module constructor as `new` method in same package/visibility boundary if strict crate parity is required.
- **L3:** Serialize timings to renderer layers; keep struct immutable once published.

## Dependencies Read Next

| target_file | reason |
| --- | --- |
| applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmProcessingTimingsBuilder.java | likely builder/aggregator |
| applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/ResultBuilder.java | timing consumer for result outputs |
| procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/... | execution mode output translation |

## Dependents As Tests

| source_file | reason |
| --- | --- |
| applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/MutateResultRenderer.java | renderer receives timing payloads |
| applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/StatsResultRenderer.java | specialized results builder |
| applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/WriteResultRenderer.java | execution-result path for write mode |

## Open Questions

- Should timings be monotonic/non-decreasing guaranteed at callsite?
- Should side-effect duration be nullable for no-side-effect modes, or remain always present as zero?

## Coding Prompt Unlocked

Add a Rust `AlgorithmProcessingTimings` value type and a timing-sum helper in result builder tests; assert immutable and copy-safe behavior with totals used in renderers.
