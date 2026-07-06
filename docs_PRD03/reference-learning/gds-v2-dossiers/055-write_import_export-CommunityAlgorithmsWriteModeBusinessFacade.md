# 55 write_import_export CommunityAlgorithmsWriteModeBusinessFacade

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/community/src/main/java/org/neo4j/gds/applications/algorithms/community/CommunityAlgorithmsWriteModeBusinessFacade.java |
| lane | write_import_export |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 55 |
| line_count | 340 |
| fan_in / fan_out | 2 / 50 |

## Why This File Matters

This class is the write-mode execution seam for community algorithms: it composes estimation, algorithm execution, write steps, result builders, and database-write plumbing.

## Public Contract

- **Evidence:** Object is created only through static `create(...)` that constructs `WriteToDatabase` with `Log`, `RequestScopedDependencies`, and `WriteContext` and injects it into the final instance (`89-104`).
- **Evidence:** For each supported write algorithm it creates algorithm-specific `WriteStep` (`new K1ColoringWriteStep(...)`, `new KCoreWriteStep(...)`, etc.) and calls `algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(...)` (`112-122`, `130-140`, `150-158`, `168-176`, `186-194`, `204-212`, `222-230`, `240-248`, `258-266`, `276-284`, `294-302`, `312-320`, `330-338`).
- **Evidence:** Each method ties the algorithm execution (`algorithms.*`) to a corresponding estimation provider (e.g., `estimationFacade::k1Coloring`) and pass-through result builder (`110-121`, `127-140`, `147-158`, etc.).
- **Inference:** The class is the canonical “write-mode policy” boundary for community algorithms.

## Internal Mechanics

- **Evidence:** It is final with only one mutable-free constructor and immutable fields (`71-87`, `99-104`).
- **Evidence:** It stores four collaborators: estimation facade, algorithm façade, template convenience, and write sink (`72-87`).
- **Evidence:** Generic methods return `<RESULT>` and delegate to a uniform pipeline; result-shape differences are carried by `ResultBuilder<Config, AlgoResult, RESULT, Metadata>`.
- **Inference:** Write behavior is standardized by dependency composition; specific algorithm differences are confined to configuration types and write-step implementations.

## Memory And Storage Implications

- **Evidence:** `WriteToDatabase` is shared across all write methods and carries database write context (`97-103`, `112-120`, etc.).
- **Inference:** This is the file to preserve carefully in rewrite because it defines when writes happen and which write step writes metadata and relationship/node updates.
- **Evidence:** Algorithm results are persisted through typed write metadata (`NodePropertiesWritten`, `HugeLongArray`, `Pair<NodePropertiesWritten, ...>`), making write payload size/type an explicit contract per algorithm (`129-139`, `164-175`, `178-175`, `304-319`).
- **Blocked:** Concrete write amplification and lock behavior are in each `WriteStep` and `WriteToDatabase` implementation.

## Snapshot And Catalog Implications

- **Evidence:** No catalog reads/writes are performed directly in this class; it consumes graph names/config from caller and delegates to machinery (`107-122`, etc.).
- **Inference:** Side effects are still write operations but centrally mediated by `WriteStep` and request-scoped dependencies.
- **Inference:** Invariant: estimation (`estimationFacade::*`) and mutation (`algorithms.*`) remain coupled but separated by typed orchestration call.

## Verification Oracles

1. **WHEN** `k1Coloring(...)` is called, **THEN** it **SHALL** create `K1ColoringWriteStep` and call `processRegularAlgorithmInWriteMode(...)` with label `AlgorithmLabel.K1Coloring` (`112-123`).
2. **WHEN** `kMeans(...)` is called, **THEN** estimator must be supplied via `() -> estimationFacade.kMeans(configuration)` and write step `KMeansWriteStep` must be passed (`143-157`).
3. **WHEN** `kCore(...)` is called, **THEN** result metadata type must be `NodePropertiesWritten` and write step `KCoreWriteStep` must be used (`125-141`).
4. **WHEN** `wcc(...)` is called, **THEN** method **SHALL** use `WccWriteConfig`, `WccWriteStep`, `estimationFacade.wcc`, and `algorithms.wcc(...)` (`305-320`).
5. **WHEN** `sllpa(...)` is called, **THEN** it **SHALL** use `SpeakerListenerLPAWriteConfig`, `SpeakerListenerLPAWriteStep(..., SLLPA)`, and `algorithms.speakerListenerLPA(...)` (`323-339`).

## Rust Rewrite Notes

- **L1:** Keep this as a pure orchestration struct with immutable dependencies and pure construction path (`create` pattern or constructor).
- **L2:** Preserve generic `<RESULT>` flow through template-driven write pipeline abstractions.
- **L2:** Make write step creation explicit in each method, but keep method signatures uniform for config/result metadata variance.
- **L3:** Represent write side effects as strategy objects (`WriteStep`) plus result builders to avoid mixing core algorithm and persistence concerns.

## Dependencies Read Next

- `applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmProcessingTemplateConvenience.java`
- `applications/algorithms/community/.../CommunityAlgorithmsEstimationModeBusinessFacade.java`
- `applications/algorithms/community/.../CommunityAlgorithms.java`
- all concrete `*WriteStep` classes and all `*WriteConfig` structs
- `org.neo4j.gds.applications.algorithms.machinery.WriteToDatabase`
- `org.neo4j.gds.applications.algorithms.machinery.ResultBuilder`

## Dependents As Tests

- Integration tests for each write method to assert write mode uses expected label and write-step type.
- Contract tests for estimation wiring: unsupported write algorithms should be unavailable unless deliberately added.
- Result-shape tests validating each method returns metadata type aligned with result builder contract.
- A DB-write side-effect test matrix across two algorithms to ensure `WriteToDatabase` lifecycle is single shared path in this façade.

## Open Questions

- Should Rust expose write modes as a generic `AlgorithmWriteMode` trait with generated wrappers or retain one wrapper method per algorithm for API parity?
- How much metadata should be persisted under each algorithm write result (`NodePropertiesWritten`, `Pair<...>`) in the rewritten Rust result contract?
- Should `create(...)` remain a named constructor vs direct constructor in public Rust API surface?

## Coding Prompt Unlocked

Implement `CommunityAlgorithmsWriteModeBusinessFacade` in Rust as:
1) a factory (`create`) that builds `WriteToDatabase`,
2) one method per write algorithm using method-local write-step creation,
3) call into an `AlgorithmProcessingTemplateConvenience`-like wrapper with config, estimation factory, computation closure, and result builder,
4) explicit tests that assert step wiring and label mapping for each algorithm.
