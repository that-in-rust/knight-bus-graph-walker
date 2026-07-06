# 64 write_import_export WritePropertyConfig

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | config-api/src/main/java/org/neo4j/gds/config/WritePropertyConfig.java |
| lane | write_import_export |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 64 |
| line_count | 39 |
| fan_out / fan_in | 39 / 4 |

## Why This File Matters

This interface defines the typed config contract for write-property names. It is tiny but high-impact for validation compatibility: write algorithms depend on this contract for user-facing input errors.

## Public Contract

- **Evidence:** Interface extends `ConcurrencyConfig` and `WriteConfig`, reusing existing write lifecycle behavior (`28`).
- **Evidence:** `WRITE_PROPERTY_KEY` constant defines canonical configuration key `"writeProperty"` (`30`).
- **Evidence:** `writeProperty()` is annotated with `@Configuration.Key(WRITE_PROPERTY_KEY)` and `@Configuration.ConvertWith(method = "validatePropertyName")` (`32-34`).
- **Evidence:** `validatePropertyName` delegates to `validateNoWhiteCharacter(emptyToNull(input), "writeProperty")` (`36-38`).
- **Inference:** The contract guarantees trimming/empty-to-null behavior and whitespace rejection in one place.

## Internal Mechanics

- **Evidence:** Validation is fully centralised in a static converter method, avoiding duplicate checks in call sites (`36-38`).
- **Inference:** Any rewrite should treat this as configuration-level policy, not procedure-level policy.

## Memory And Storage Implications

- **Evidence:** No runtime storage behavior; pure config validation interface (`20-39`).
- **Inference:** Near-zero RAM impact; only config object creation and validation path overhead.

## Snapshot And Catalog Implications

- **Inference:** No catalog access, no state mutations.
- **Inference:** Changing key semantics or default conversion here can silently break write-mode APIs.

## Verification Oracles

1. **WHEN** write configuration contains blank/empty `writeProperty`, **THEN** it SHALL normalize to `null`.
2. **WHEN** `writeProperty` contains whitespace, **THEN** validation SHALL reject with the same `validateNoWhiteCharacter` contract.
3. **WHEN** the config key is absent, **THEN** the generated config model shall follow inherited defaults from `WriteConfig`.
4. **WHEN** annotations are inspected, **THEN** the key must remain `writeProperty` and converter must be `validatePropertyName`.

## Rust Rewrite Notes

- **L1:** Represent this as a config trait/struct with a single `write_property: Option<String>`.
- **L1:** Keep key constant and annotation-equivalent metadata in serializer/deserializer layer.
- **L2:** Implement converter function `validate_property_name` with empty-to-null + whitespace validation.
- **L3:** Reuse error formatting utilities to preserve message shape.

## Dependencies Read Next

- `config-api/src/main/java/org/neo4j/gds/config/WriteConfig.java`
- `config-api/src/main/java/org/neo4j/gds/config/ConcurrencyConfig.java`
- `core/src/main/java/org/neo4j/gds/core/StringIdentifierValidations.java`

## Dependents As Tests

- Unit tests for config binding and conversion paths:
  - null/empty normalization,
  - whitespace rejection,
  - valid property key acceptance.
- Integration checks on algorithms that consume `WritePropertyConfig` to ensure unchanged behavior.

## Open Questions

- Should error message details be identical to Java converters or standardized in Rust config layer?
- Should empty-to-null occur before/after trimming (if trim policy changes in Rust)?
- Should write-property key be case-sensitive in future-proof schemas?

## Coding Prompt Unlocked

Implement `WritePropertyConfig` contract-equivalent in Rust:
1) typed config with constant `write_property` key,
2) converter that maps empty input to null and rejects whitespace,
3) centralize with existing write/concurrency config traits so dependent algorithms inherit correctly.
