# 067 procedure_surface LocalNodeEmbeddingsProcedureFacade

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | procedures/algorithms-facade/src/main/java/org/neo4j/gds/procedures/algorithms/embeddings/LocalNodeEmbeddingsProcedureFacade.java |
| lane | procedure_surface |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 67 |
| line_count | 426 |
| fan_in / fan_out | 1 / 47 |

## Why This File Matters

This is the public facade for node-embedding procedure endpoints, converting user input into mode-specific business calls and wiring mutation/stream/write/estimate entry points.

## Public Contract

- **Evidence:** Class is `final` and implements `NodeEmbeddingsProcedureFacade`, exposing concrete node-embedding procedure routes (`56-56`, `98-426`).
- **Evidence:** Constructor is private and object creation uses static `create(...)`, assembling stubs and all 4 modes of `nodeEmbeddings` business facades (`72-142`).
- **Evidence:** Each supported operation follows a consistent parse-builder-execute pattern:
  - parse config (`configurationParser.parseConfiguration(...)`),
  - build a result builder (`new ...ResultBuilder...`),
  - delegate to mode facade with `GraphName.parse(graphName)`.
  - e.g. fastRP stream (`177-188`), graphSage stream/train/write (`235-335`, `238-334`), hashgnn stream (`344-367`), node2vec stream/write (`375-424`).
- **Evidence:** Estimate methods parse independently and return `Stream.of(result)` (`165-174`, `191-200`, `217-225`, `258-266`, `289-302`, `323-335`, `358-367`, `390-399`, `416-424`).
- **Evidence:** GraphSage and HashGNN write/stream use username-aware parsing via `GraphSage...Config.of(user.getUsername(), ...)` (`258-264`, `276-280`, `311-314`, `327-330`).
- **Inference:** Parsing, stubs, and mode dispatch is a stable compatibility boundary and should be preserved if procedure behavior is to remain stable.

## Internal Mechanics

- **Evidence:** `create(...)` injects mode-specific mutate stubs (`Local*MutateStub`) to support procedure-level mutation signatures and config reuse (`98-142`).
- **Evidence:** Mutate stub getters are one-line accessors mapping to constructor fields (`145-147`, `228-231`, `339-341`, `371-373`).
- **Evidence:** The class has no direct business execution logic; all algorithm semantics come from `statsModeBusinessFacade`, `streamModeBusinessFacade`, etc.
- **Blocked:** The exact result model fields and procedure annotations are outside this file and live in result types and interfaces.

## Memory And Storage Implications

- **Inference:** Allocation profile is mostly short-lived config/result-builder artifacts; no graph storage mutation occurs here.
- **Inference:** Streaming responses (`Stream<T>`) are used to avoid holding full result tables in memory for large outputs.
- **Inference:** `Stream.of(result)` for estimation paths is a compact one-row stream strategy.

## Snapshot And Catalog Implications

- **Inference:** This file maps request input to graph identity via `GraphName.parse(graphName)` and to user context via `user.getUsername()`, so these values must be validated before algorithm execution.

## Verification Oracles

1. **WHEN** fastRP graph name procedure is called, **THEN** config must be parsed with `FastRPStatsConfig::of`/`FastRPStreamConfig::of`/`FastRPWriteConfig::of`.
2. **WHEN** graphSage methods are invoked, **THEN** parser must include username in `GraphSage*Config.of(username, wrapper)`.
3. **WHEN** hashgnn or node2vec stream/write is called, **THEN** corresponding config-of parser and respective mode facade MUST be used.
4. **WHEN** estimation methods are called, **THEN** the result MUST be returned as a single-element stream.

## Rust Rewrite Notes

- **L1:** Preserve a single facade with:
  - injected mode facades (`estimate/stats/stream/train/write`),
  - parser service,
  - user context.
- **L2:** Keep each procedure path as parse → builder → delegate, returning iterables/streams as streaming abstractions.
- **L3:** Maintain username-aware config creation for methods that currently require user-bound config.
- **L3:** Keep local stub objects as lightweight adapters rather than duplicating business behavior.

## Dependencies Read Next

- `org.neo4j.gds.procedures.algorithms.embeddings.NodeEmbeddingsProcedureFacade`
- `NodeEmbeddingAlgorithms*ModeBusinessFacade` classes
- `org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser`
- Local mutate stubs under `procedures/algorithms/embeddings/stubs`

## Dependents As Tests

- Procedure-level integration tests per mode: `fastRP`, `graphSage`, `hashGnn`, `node2Vec` for stream/stats/write/estimate.
- Golden tests asserting stream cardinality for estimate methods.
- Snapshot tests for config parser shape with/without username on graphSage/train/write.

## Open Questions

- What policy controls mutually exclusive configuration fields between mutate and other modes?
- Should all estimate methods remain stream-returning for parity, or can they move to `single` semantics in Rust?
- Blocker: procedure metadata annotations and registration are external; this file alone does not guarantee endpoint exposure order.

