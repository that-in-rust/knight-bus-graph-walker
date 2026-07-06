# 45 olap_algorithm CommunityAlgorithmsMutateModeBusinessFacade

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/community/src/main/java/org/neo4j/gds/applications/algorithms/community/CommunityAlgorithmsMutateModeBusinessFacade.java |
| lane | olap_algorithm |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 45 |
| line_count | 321 |
| fan_in / fan_out | 14 / 48 |

## Why This File Matters

This class is the mutate-mode orchestration point for community algorithms. It binds estimation, execution, mutate-node-property step composition, and return-shaping in one seam.

## Public Contract

- **Evidence:** Constructor injects estimation facade, algorithm facade, processing template convenience, and mutate-node-property strategy (`70–86`).
- **Evidence:** Each public method follows the same template:
  1) construct domain-specific `MutateStep`,
  2) call `processRegularAlgorithmInMutateMode(...)`,
  3) pass graph-resolution, estimation function, execution lambda, result builder (`88–103`, `106–121`, ..., `304–320`).
- **Evidence:** For each community algorithm (approx max k-cut, k1Coloring, kCore, kMeans, etc.), it selects algorithm label constants from `AlgorithmLabel` and route-specific stub/result types (`56–69`, method bodies).
- **Evidence:** It supports stream/mutating output types via generic `<RESULT>` plus `ResultBuilder<Config, AlgorithmResult, RESULT, MutateMetadata>`.
- **Inference:** The consistency means adding/removing algorithms is mostly a registration and step-construction concern, not a control-flow rewrite.

## Internal Mechanics

- **Evidence:** `algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(...)` is the central dispatch pattern across all methods (`95–103`, `113–121`, `131–139`, etc.).
- **Evidence:** Mutate steps are algorithm-specific (`new *MutateStep(...)`) and injected with `mutateNodeProperty` + configuration (`93–94`, `111–112`, `129–130`, `147–148`, `165–166` ...).
- **Evidence:** Estimation is delegated via `estimation::<algorithm>` function references (direct lambda or method reference) separate from execution lambda (`99`, `117`, `135`, `153`, etc.).
- **Inference:** This is a textbook separation of concerns: estimate, execute, side-effect/writer, and output builder are decoupled but combined in one managed pipeline call.

## Memory And Storage Implications

- **Inference:** Mutate mode likely writes node properties and/or graph metadata; type annotations (`NodePropertiesWritten`, `NodePropertyValues`) show metadata-heavy payloads.
- **Evidence:** The builder carries result-type metadata to map domain outputs to response contracts and avoid unnecessary coupling (`28–29`, `28` etc).
- **Blocked:** Exact post-write storage costs of mutation steps are in per-step classes (`*MutateStep`) not visible in this file.

## Snapshot And Catalog Implications

- **Inference:** The façade assumes graph resolution occurs before this layer (`GraphName`, config), keeping catalog lookup outside this class (`88–90`, `106–110`).
- **Evidence:** The typed `graphName` argument is the cross-boundary handle for mutation routing; execution layer receives config+graph name and then delegates.

## Verification Oracles

1. **WHEN** a mutate method is invoked, **THEN** it **SHALL** create an algorithm-specific mutate step before processing (`93`, `111`, `129`...).
2. **WHEN** processing path enters template convenience, **THEN** it **SHALL** receive estimation supplier, execute lambda, and result builder as parameters (`95–103`, `149–156`, etc.).
3. **WHEN** `kMeans`/`leiden` mutate methods are used, **THEN** result metadata type **SHALL** match expected domain metadata signatures (`145–157`, `203–211`).

## Rust Rewrite Notes

- **L1:** `CommunityAlgorithmsMutateModeBusinessFacade` as orchestration service with one method per mutate use-case.
- **L2:** Central `process_regular_algorithm_in_mutate_mode` template taking `estimate_fn`, `execute_fn`, `mutate_step`, `result_builder`.
- **L2:** Generic result builder abstraction with compile-time result/metadata type constraints.
- **L3:** Injected step factories and execution convenience service to avoid cross-cutting duplication.

## Dependencies Read Next

- `applications/algorithms/community/CommunityAlgorithms.java`
- `applications/algorithms/community/CommunityAlgorithmsEstimationModeBusinessFacade.java`
- `applications/algorithms/machinery/AlgorithmProcessingTemplateConvenience.java`
- `applications/algorithms/machinery/MutateNodeProperty.java`

## Dependents As Tests

- Community mutate procedure tests for each algorithm.
- Metadata tests for `NodePropertiesWritten` and property-write side effects.
- Mutation contract tests that assert estimate/execution separation.

## Open Questions

- Should each mutate method remain public with duplicated structure, or be generated via macro/template pattern in Rust for readability?
- Do failure semantics for missing estimation/execution mapping need richer unified error envelopes in Rust?

## Coding Prompt Unlocked

Implement a Rust community mutate facade with:
1) per-algorithm methods and shared generic mutate pipeline,
2) pluggable mutate step factory,
3) explicit estimate and execute closures,
4) result metadata typing per algorithm,
5) tests that assert every mutate path hits the shared template and returns expected metadata shape.
