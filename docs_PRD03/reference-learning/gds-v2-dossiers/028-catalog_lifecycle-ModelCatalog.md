# 28 catalog_lifecycle ModelCatalog

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | model-catalog-api/src/main/java/org/neo4j/gds/core/model/ModelCatalog.java |
| lane | catalog_lifecycle |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 28 |
| line_count | 162 |
| fan_in / fan_out | 81 / 3 |
| purpose | Map graph identity, lifecycle, catalog scoping, snapshot ownership, and artifact state. |
| read_prompt | Read this entire file as a catalog/snapshot lifecycle contract. Extract identities, ownership boundaries, lifecycle transitions, concurrency assumptions, error cases, and what a Rust implementation must persist or reject. |

## Why This File Matters

- This is a foundational lifecycle interface for model persistence in GDS.
- It defines catalog operations at the identity level (user + model name + type/shape), plus side-effecting operations (`set`, `drop`, `store`).
- The file also exposes `EMPTY` as a sentinel instance, which affects how null-object behavior may be used across boundaries.

## Public Contract

- `ModelCatalog` interface in `org.neo4j.gds.core.model`.
- Listener management:
  - `registerListener(ModelCatalogListener)`
  - `unregisterListener(ModelCatalogListener)`
- Mutation and retrieval:
  - `set(Model<?, ?, ?> model)`
  - `<D, C extends ModelConfig, I extends CustomInfo> Model<D, C, I> get(username, modelName, dataClass, configClass, infoClass)`
  - `getUntypedOrThrow(username, modelName)`
  - `getUntyped(username, modelName)` (nullable)
  - `getAllModels()`
  - `modelCount()`
  - `exists(username, modelName)`
  - `dropOrThrow(username, modelName)`
  - `drop(username, modelName)` (nullable)
  - `Collection<Model<?, ?, ?>> list(username)`
  - `publish(username, modelName)`
  - `store(username, modelName, Path modelDir)`
- Lifecycle/system helpers:
  - `isEmpty()`
  - `removeAllLoadedModels()`
  - `verifyModelCanBeStored(username, modelName, modelType)`
- Embedded `EMPTY` implementation:
  - no-op registration/unregistration
  - `set` is no-op
  - nullable fallbacks (`null` for get/list/drop/store), false for bool checks, empty model count, and empty stream.

## Internal Mechanics

- The API is generic and intentionally untyped in many places (`Model<?, ?, ?>`) with generic helpers for type-checked retrieval.
- Return types include both throwing and nullable variants to support compatibility with both strict and optional workflows.
- `publish()` and `store()` indicate transition points between loaded model state and durable model artifact management.
- `verifyModelCanBeStored(...)` is explicit policy gate before persistence operations are attempted by callers.
- `EMPTY` is a deliberate fallback contract to avoid null checks at call sites in some branches.

## Storage and Runtime Behavior

- This is a contract for model catalog state management, not an implementation.
- Implementations must support:
  - multi-user scoping (`username`)
  - typed/unchecked model lookup semantics
  - transitions between loaded-memory and stored state (`store`, `publish`, `drop`).
- In Rust, this seam should model concurrency and ownership carefully because model objects can be expensive to materialize.

## Failure / Incompatibility Surfaces

- `getUntypedOrThrow`, `dropOrThrow`, and potentially `publish`/`store` are likely to fail when model is missing or invalid state exists.
- `drop`/`getUntyped` are intentionally nullable and may return no-value signals instead of throwing.
- `EMPTY` currently returns permissive defaults; callers may rely on this null-object behavior in non-production paths.
- Inference risk: generic type casts through `get(..., Class<...>)` can fail if not enforced with strict runtime checks.

## Verification Oracles

1. **WHEN** a model is registered with `set(...)` and then `exists(user, name)` is called  
   **THEN** **SHALL** return `true` for same username + name.
2. **WHEN** `get(username, modelName, dataClass, configClass, infoClass)` is called with mismatched typing  
   **THEN** **SHALL** surface a mismatch signal (exception or controlled error) rather than silently corrupting state.
3. **WHEN** `dropOrThrow` is called on missing model  
   **THEN** **SHALL** fail fast (throwing variant contract).
4. **WHEN** `drop` is called on missing model  
   **THEN** **SHALL** return nullable empty result per contract.
5. **WHEN** persistence policy disallows storage (`verifyModelCanBeStored`)  
   **THEN** **SHALL** reject training/model registration before storing side effects occur.

## Rust Rewrite Notes

- **L1:** define a `ModelCatalog` trait with explicit ownership and lifetime boundaries for model metadata and data payload.
- **L2:** encode both strict and optional variants as explicit enums to avoid nullable misuse.
- **L3:** implement an `EMPTY` sentinel/strategy as explicit object only if caller contracts depend on permissive defaults.
- Separate pure catalog operations from persistence operations (`publish`, `store`) to avoid accidental writes.

## Dependencies Read Next

- `org.neo4j.gds.core.model.Model`
- `org.neo4j.gds.model.ModelConfig`
- `ModelCatalogListener`
- Implementations in graph-store/pipeline/model modules and pipeline estimator code that calls `verifyModelCanBeStored`.

## Dependents As Tests

- Model lifecycle tests around registration, retrieval, publish/store, and drop semantics.
- Typing/validation tests for generic model fetch with malformed `ModelConfig`/data type combos.
- Persistence policy tests for `verifyModelCanBeStored` and `EMPTY` behavior.

## Open Questions

- Should null-object semantics (`EMPTY`) be preserved exactly, or should Rust prefer explicit `NoopCatalog` + feature flag to avoid silent no-op behavior?
- Is strict typing at API boundary desired for `get(...)` in Rust to reduce runtime casting risk?
