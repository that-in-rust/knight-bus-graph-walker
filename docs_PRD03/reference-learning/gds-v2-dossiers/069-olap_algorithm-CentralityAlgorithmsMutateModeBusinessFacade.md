# 069 olap_algorithm CentralityAlgorithmsMutateModeBusinessFacade

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/centrality/src/main/java/org/neo4j/gds/applications/algorithms/centrality/CentralityAlgorithmsMutateModeBusinessFacade.java |
| lane | olap_algorithm |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 69 |
| line_count | 288 |
| fan_in / fan_out | 9 / 38 |

## Why This File Matters

This is the mutate-mode API surface for centrality algorithms. It unifies estimation, execution, mutation step creation, result wrapping, and optional special hooks into a stable execution template.

## Public Contract

- **Evidence:** The class holds references to estimation/business/machine templates and mutation components (`64-68`).
- **Evidence:** Each method returns `<RESULT>` and accepts typed config + `ResultBuilder<...>` with `NodePropertiesWritten` as metadata (`85-89`, `103-107`, `122-126`, ...).
- **Evidence:** Normal algorithms call `processRegularAlgorithmInMutateMode(...)` with:
  - graphName,
  - config,
  - `AlgorithmLabel`,
  - estimation function (`estimation::...`),
  - algorithm function `(graph, __) -> algorithms...`,
  - mutate step,
  - result builder (`92-100`, `110-118`, `129-137`, ...).
- **Evidence:** HITS uses `processAlgorithmInMutateMode(...)` with ETL hook and explicit `Optional` wrappers (`266-284`) and no catalog view of estimation in regular pipeline call.
- **Inference:** A single shared execution path exists, with HITS requiring one special hook injection branch.

## Internal Mechanics

- **Evidence:** Mutate steps are algorithm-specific and created inline via `new <Algo>MutateStep(..., configuration)` (`90-90`, `108-109`, `127-128`, ...).
- **Evidence:** Centrality estimator functions are either direct method refs (`estimation::pageRank`, `estimation::harmonicCentrality`, etc.) or method lambdas for extra config transforms (`133`, `151`, etc.).
- **Evidence:** `processRegularAlgorithmInMutateMode` is reused for most algorithms, indicating the mutation pipeline contract is generic and strong.
- **Evidence:** `hits` adds `hitsHookGenerator.createETLHook(configuration)` and passes `Optional.of(List.of(hook))` to the special algorithm template (`272-279`).
- **Blocked:** Exact result shape for each algorithm is in mutate step/result classes, not this file.

## Memory and Storage Implications

- **Inference:** `MutateNodeProperty` is injected and reused across algorithms, centralizing property write policy (and avoiding duplicate memory/state logic).
- **Inference:** All mutate functions output `NodePropertiesWritten` to describe graph-side mutation impact, while algorithm payload is algorithm-specific and potentially large.
- **Inference:** HITS path may incur additional hook-related transient state via ETL hook.

## Snapshot And Catalog Implications

- **Inference:** This layer does not own catalog reads/writes directly; mutation goes through `MutateNodeProperty` and algorithm execution façade.
- **Inference:** `GraphName`, config, and termination hooks should be validated in lower layers.

## Verification Oracles

1. **WHEN** any standard centrality mutate method is called, **THEN** it SHALL use `processRegularAlgorithmInMutateMode` with the matching `AlgorithmLabel`.
2. **WHEN** page rank-style algorithms are invoked, **THEN** estimation function and execute function SHOULD align to the same algorithm pair (`estimation::...` and `algorithms...`).
3. **WHEN** HITS mutate is invoked, **THEN** a non-empty ETL hook list SHALL be passed through `Optional.of(List.of(hook))`.
4. **WHEN** `NodePropertiesWritten` is expected, **THEN** the mutate result builder output must retain typed metadata for downstream result formatting.

## Rust Rewrite Notes

- **L1:** Create one central mutate façade entrypoint that is generic over config, result type, algorithm label, estimation fn, run fn, mutate step, and result builder.
- **L1:** Preserve algorithm-specific step constructors as small adapter objects.
- **L2:** Handle HITS as dedicated branch using optional pre/post hooks before invoking generic algorithm processor.
- **L3:** Keep all methods generic (`fn <RESULT>`) to avoid duplicated orchestration code.

## Dependencies Read Next

- `CentralityAlgorithms` (execution layer)
- `CentralityAlgorithmsEstimationModeBusinessFacade`
- `AlgorithmProcessingTemplateConvenience`
- `MutateNodeProperty`
- `HitsHookGenerator`
- Centrality mutate steps under same package (`*MutateStep`)

## Dependents As Tests

- Contract tests for one path per centrality algorithm verifying:
  - matching algorithm label,
  - estimate function used,
  - mutate step type produced.
- HITS-focused test where hook list is asserted as present.
- Property-based test ensuring every method returns metadata object typed as `NodePropertiesWritten`.

## Open Questions

- Should HITS remain exceptional or be folded into shared pipeline with optional hooks?
- Are there algorithms missing in this class that should be surfaced for forward compatibility?
- Blocker: behavior of each `*MutateStep` needs follow-up dossiers for exact storage side effects.

