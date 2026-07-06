# 078 procedure_surface PregelProcedureConfig

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | pregel/src/main/java/org/neo4j/gds/beta/pregel/PregelProcedureConfig.java |
| lane | procedure_surface |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 78 |
| line_count | 72 |
| fan_in / fan_out | 36 / 8 |

## Why This File Matters

This configuration interface is the mutable contract across stream/write/mutate usages of Pregel procedures and is the key boundary for mutation safety checks.

## Public Contract

- Implements:
  - `PregelConfig`
  - `WritePropertyConfig`
  - `MutateNodePropertyConfig`
- Default property names:
  - `writeProperty()` → `""`
  - `mutateProperty()` → `""`
- `validateGraphIsSuitableForWrite(...)` is a graph capability gate:
  - Skip when `writeProperty` is blank (non-write mode)
  - Otherwise assert graph can write to local or remote DB
  - Throw `IllegalArgumentException` if neither write mode is supported
- Static factory:
  - `static PregelProcedureConfig of(CypherMapWrapper userInput)`

## Internal Mechanics

- The default `@Configuration.GraphStoreValidationCheck` method is intentionally permissive for non-write modes.
- This interface intentionally allows one shared config class across multiple execution modes while gating destructive write behavior only when write mode is explicitly enabled.
- Validation hooks piggyback the config annotation machinery (`@Configuration`/`@Override`) rather than explicit caller checks.

## Memory and Storage Implications

- No direct allocations beyond config defaults and validation exceptions.
- It defines whether write-capable paths execute; therefore indirectly controls write I/O side effects and potential graph mutations.

## Snapshot And Catalog Implications

- Write/mutate behavior should be considered a capability contract, not just a parameter list.
- For compatible surfaces in Rust, treat this as a single config value object with mode-aware validation branches.

## Verification Oracles

1. **WHEN** `writeProperty` is blank, **THEN** write-capability validation SHALL be skipped.
2. **WHEN** graph capabilities are non-writable in both local and remote mode, **THEN** `validateGraphIsSuitableForWrite` SHALL throw `IllegalArgumentException`.
3. **WHEN** `of(userInput)` is called, **THEN** a `PregelProcedureConfigImpl` instance SHALL be produced.
4. **WHEN** unknown write/mutate defaults are passed, **THEN** defaults SHALL remain empty strings unless overwritten by parser.

## Rust Rewrite Notes

- Keep one shared config struct for multi-mode Pregel entrypoints.
- Encode the write-capability validation branch as a mode-dependent guard before execution.
- Preserve the interface-style composability (`WritePropertyConfig`, `MutateNodePropertyConfig`) as traits or trait bounds.

## Dependencies Read Next

- `PregelConfig`
- `WritePropertyConfig`
- `MutateNodePropertyConfig`
- `GraphStore` capability APIs
- `CypherMapWrapper`

## Dependents As Tests

- Contract test matrix for write / non-write modes.
- Regression test that read-only mode bypasses write-capability checks.

## Open Questions

- Do we need strict mode enum tagging for `writeProperty` vs `mutateProperty` in the Rust config layer, or keep shared optional semantics?

