# 13 procedure_surface ProcedureConstants

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/ProcedureConstants.java |
| lane | procedure_surface |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 13 |
| line_count | 26 |
| fan_in / fan_out | 161 / 0 |
| purpose | Central procedure-string constant for memory estimation procedure description output. |
| read_prompt | Read this entire file as a procedure contract constant. Extract compatibility guarantees and where this description value is consumed. |

## Why This File Matters

- It defines the canonical estimate procedure description text used for memory estimate entries.
- Single, stable constant implies strong coupling points for proc response docs and user-facing CLI/Procedure output.

## Public Contract

- `MEMORY_ESTIMATION_DESCRIPTION`
  - value: `"Returns an estimation of the memory consumption for that procedure."`
- Private constructor enforces utility-class semantics.

## Internal Mechanics

- No behavior logic beyond constant definition.
- This is effectively a named contract token intended for reuse across procedures and estimate documentation.

## Memory And Storage Implications

- No runtime RAM allocation semantics here; constant-only file.
- In rewrite, keep as shared symbol to avoid duplicated doc drift in multiple endpoints.

## Verification Oracles

1. **WHEN** a procedure returns memory-estimate metadata text
   **THEN** **SHALL** reuse a shared constant or equivalent to avoid drift.
2. **WHEN** tests validate estimate descriptions
   **THEN** **SHALL** assert exact text match with canonical value.

## Rust Rewrite Notes

- **L2:** represent as exported constant in a `procedure_constants` module.
- **L1:** include as typed constant used by all estimate-spec adapters and docs.
- **L3:** wire into generated procedure metadata registration to preserve output consistency.

## Dependencies Read Next

- Procedure registration paths where estimate descriptions are built.
- Procedure facade files that expose estimate docs and call-site descriptions.

## Dependents As Tests

- Any estimate contract tests asserting procedure output docs or annotation text.

## Open Questions

- Should this be an enum of all user-facing descriptions or a single constant module to avoid over-centralization?
- Does localized message support ever become required?

## Coding Prompt Unlocked

Add a shared Rust constant module:
- `const MEMORY_ESTIMATION_DESCRIPTION: &str = "...";`
- replace all literal estimate descriptions with this constant and add a test proving all estimate adapters share it.
