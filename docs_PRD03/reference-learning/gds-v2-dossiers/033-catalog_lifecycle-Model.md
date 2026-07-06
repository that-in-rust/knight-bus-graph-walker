# 33 catalog_lifecycle Model

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | model-catalog-api/src/main/java/org/neo4j/gds/core/model/Model.java |
| lane | catalog_lifecycle |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 33 |
| line_count | 131 |
| fan_in / fan_out | 65 / 6 |

## Why This File Matters

This interface is the canonical model metadata contract used for trained model artifacts and pipeline model APIs.  
**Evidence:** It is annotated with `@ValueClass` and exposes typed accessors for model identity, state, schema, config, and storage metadata.

## Public Contract

- **Evidence:** `Model` is an immutable-style interface with typed accessors for creator/name/algo type/graph schema/config/data/publication fields (`53-79`).
- **Evidence:** `static now()` chooses timezone from `GraphDatabaseSettings.db_temporal_timezone` and defaults to system timezone (`45-48`).
- **Evidence:** `creationTime()` defaults to `now()` when omitted (`70-73`), making model creation timestamp deterministic in the default path.
- **Evidence:** `loaded()` is derived from `data() != null` (`82-84`).
- **Evidence:** `stored()` is derived from `fileLocation().isPresent()` (`86-89`).
- **Evidence:** `isPublished()` is derived from `sharedWith().contains(ALL_USERS)` (`92-94`).
- **Evidence:** Two `of(...)` static factories build a `ImmutableModel` using builder semantics and fill creator/model name from `trainConfig` (`96-114`, `117-125`).
- **Evidence:** `CustomInfo` is constrained to `ToMapConvertible + Serializable` with optional trainer method hook (`128-130`).

## Internal Mechanics

- **Evidence:** The file declares constants `ALL_USERS="*"` and `PUBLIC_MODEL_SUFFIX="_public"` (`50-51`), both used as domain semantics for sharing.
- **Inference:** This interface is expected to generate immutable implementations via `@ValueClass` tooling at build-time; behavior depends on generated `ImmutableModel`.
- **Evidence:** Lifecycle flags are computed traits (default methods), not stored explicitly (`81-94`), which reduces boilerplate and increases derived consistency.
- **Evidence:** No mutating methods or setters exist, so the contract is intentionally immutable and thread-friendly.
- **Blocked:** No inline validation hooks are present here; all constraints are pushed to implementers and producer call sites.

## Memory And Storage Implications

- **Evidence:** The model holds at least metadata references for `graphSchema`, `customInfo`, `trainConfig`, and optional `data`.
- **Inference:** Memory pressure is likely dominated by `data()` payloads and schema/config graphs, while metadata objects are thin references.
- **Evidence:** `stored` and `loaded` are derived booleans, so callers can branch without extra bookkeeping.

## Snapshot And Catalog Implications

- **Evidence:** This contract is the backbone for cataloged model entries and model sharing behavior (`creator`, `name`, `sharedWith`, `fileLocation`).
- **Inference:** Snapshot persistence should preserve enough fields to faithfully round-trip the `ImmutableModel` representation because derived flags are recomputable.

## Verification Oracles

1. **WHEN** a `Model` is created via `Model.of(..., modelData = null, ...)` **THEN** `loaded()` **SHALL** return `false`.
2. **WHEN** a `Model` has `sharedWith = ["*"]` **THEN** `isPublished()` **SHALL** return `true`.
3. **WHEN** a `Model` has `fileLocation` present **THEN** `stored()` **SHALL** return `true`.

## Rust Rewrite Notes

- **L1:** model domain object with immutable `struct` and `Arc`-compatible owned references for heavy fields.
- **L1:** explicit value-object constructors mirroring `Model.of(...)` overloads.
- **L2:** derived methods (`loaded`, `stored`, `is_published`) from option checks to avoid duplicated flags.
- **L2:** timezone-aware `creation_time` default via config-backed `db_temporal_timezone` or system fallback.
- **L3:** trait contract for `CustomInfo: ToMapConvertible + Serialize` to preserve metadata extension points.

## Dependencies Read Next

- `model-catalog-api/src/main/java/org/neo4j/gds/core/model/ModelFactory.java` (if present in target branch) or equivalent builder usage sites.
- `model-catalog-api/src/main/java/org/neo4j/gds/core/model/ModelCatalog.java` (catalog-level lifecycle contracts).
- `model-catalog-api/src/main/java/org/neo4j/gds/model/ModelConfig.java` (model training config shape).
- `graph-schema-api/src/main/java/org/neo4j/gds/api/schema/GraphSchema.java`.
- `config-api/src/main/java/org/neo4j/gds/config/BaseConfig.java`.
- `ml/ml-api/src/main/java/org/neo4j/gds/ml/api/TrainingMethod.java`.

## Dependents As Tests

- `applications/algorithms/node-embeddings/src/main/java/org/neo4j/gds/applications/algorithms/embeddings/GraphSageModelCatalog.java` (model catalog usage)
- `algo/src/main/java/org/neo4j/gds/embeddings/graphsage/algo/*` (model producer/consumer)
- graph-sage training/prediction test specifications under `applications/algorithms/node-embeddings` and `algorithm-specifications`.
- Any tests asserting `Model.of(...)` and publication/filter behavior.

## Open Questions

- **Blocked:** Where should migration logic for older serialized model metadata live (catalog loader vs model factory)?
- **Inference:** If `sharedWith` is large, derived `isPublished` remains O(n); might need indexing only if this becomes a hotspot.

## Coding Prompt Unlocked

Implement `Model` in Rust as an immutable record + builder API:
1. include creator/name/algoType/graphSchema/trainConfig/customInfo/fileLocation;
2. include derived status methods (`loaded`, `stored`, `is_published`);
3. include timestamp defaulting logic with timezone fallback;
4. add unit tests for derived flags and all factory overload semantics.
