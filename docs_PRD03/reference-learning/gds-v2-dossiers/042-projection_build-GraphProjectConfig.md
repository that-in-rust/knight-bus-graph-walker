# 42 projection_build GraphProjectConfig

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | config-api/src/main/java/org/neo4j/gds/config/GraphProjectConfig.java |
| lane | projection_build |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 42 |
| line_count | 108 |
| fan_in / fan_out | 58 / 8 |

## Why This File Matters

This interface defines the shared contract for graph-project configs used by projection and projection-mode procedures. It combines defaults, parsing directives, and validation hooks.

## Public Contract

- **Evidence:** `GraphProjectConfig.emptyWithName(userName, graphName)` returns a concrete config builder with username+graphName (`55–59`).
- **Evidence:** `graphName()` is required and converted via `validateName` (`68–70`, `105–107`).
- **Evidence:** Defaults are explicit:
  - `readConcurrency` defaults to `ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY` (`72–76`),
  - node/relationship counts default `-1` (`78–86`),
  - `validateRelationships` defaults false (`93–96`).
- **Evidence:** `validateReadConcurrency()` delegates to `ConcurrencyValidatorService` with shared limitation constants (`98–103`).
- **Evidence:** `isFictitiousLoading()` marks a lightweight pseudo-state (`88–91`).
- **Evidence:** `asProcedureResultConfigurationField()` and `cleansed(...)` are explicitly ignored in procedure serialization (`35–39`, `36–47`).

## Internal Mechanics

- **Evidence:** Config annotations (`@Configuration.Parameter`, `@Configuration.Key`, `@Configuration.ConvertWith`, `@Configuration.ToMapValue`, `@Configuration.Ignore`, `@Configuration.Check`) indicate framework-managed mapping/validation pipeline.
- **Inference:** `cleansed(...)` provides a stable key-removal helper for procedure result maps and is important for result-shape parity under hidden/internal keys.
- **Blocked:** Annotation processing behavior (`@Configuration.*`) requires cross-module understanding; behavior is inferred from declaration only.

## Memory And Storage Implications

- **Evidence:** Most methods are immutable defaults/accessors with no internal mutable fields in the interface, minimizing memory footprint per config object (`62–70`, `74–91`).
- **Inference:** Memory behavior is dominated by config consumers that materialize maps in `cleansed(...)` and downstream project builders.

## Snapshot And Catalog Implications

- **Evidence:** `graphName` and username fields are central identity keys for catalog projection lookup (`55–60`, `63–70`).
- **Inference:** `isFictitiousLoading()` controls a path where graph dimensions are explicitly injected (`nodeCount`, `relationshipCount`) and may avoid full store resolution.

## Verification Oracles

1. **WHEN** graph name is invalid by whitespace checks, **THEN** `validateName` **SHALL** return validation result consistent with `StringIdentifierValidations`.
2. **WHEN** `readConcurrency` is omitted, **THEN** caller **SHALL** observe `ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY`.
3. **WHEN** `validateReadConcurrency()` is invoked, **THEN** it **SHALL** call shared concurrency validator with `ConcurrencyConfig.CONCURRENCY_LIMITATION`.
4. **WHEN** `cleansed(map, keysToIgnore)` receives keys to omit, **THEN** output **SHALL** remove exactly those keys.

## Rust Rewrite Notes

- **L1:** Config interface/trait with annotated-like metadata captured explicitly in Rust structs.
- **L2:** `read_concurrency`, `node_count`, and `relationship_count` as typed defaults.
- **L2:** `cleansed` helper for procedure payload filtering.
- **L3:** Separate parser/validator pipeline module to mimic `@Configuration.*` behavior with explicit check functions.

## Dependencies Read Next

- `config-api/src/main/java/org/neo4j/gds/config/ConcurrencyConfig.java`
- `config-api/src/main/java/org/neo4j/gds/config/BaseConfig.java`
- `config-api/src/main/java/org/neo4j/gds/config/JobIdConfig.java`
- `core/src/main/java/org/neo4j/gds/core/concurrency/ConcurrencyConfig.java`

## Dependents As Tests

- Projection config parser tests for project/unproject flows.
- Fictitious-load tests where node/relationship counts are passed manually.
- Validation tests for read concurrency limits and graph-name checks.

## Open Questions

- Should `isFictitiousLoading` remain a derived method in Rust or be normalized at parser boundary for simpler pattern matching?
- Do we preserve annotation-style metadata to keep compatibility with existing config reflection helpers, or replace with explicit schema files?

## Coding Prompt Unlocked

Port `GraphProjectConfig` semantics by implementing:
1) typed config trait with defaults,
2) parse/validate hooks (`validate_name`, `validate_read_concurrency`),
3) `fictitious_loading` helper based on dimension override sentinel,
4) procedure-shape filtering helper tested on maps and ignored keys.
