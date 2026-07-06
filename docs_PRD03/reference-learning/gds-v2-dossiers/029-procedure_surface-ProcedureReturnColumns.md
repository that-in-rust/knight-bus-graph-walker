# 29 procedure_surface ProcedureReturnColumns

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | neo4j-api/src/main/java/org/neo4j/gds/api/ProcedureReturnColumns.java |
| lane | procedure_surface |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 29 |
| line_count | 27 |
| fan_in / fan_out | 82 / 0 |
| purpose | Capture procedure ABI: names, modes, columns, config parsing, errors, and unsupported behavior. |
| read_prompt | Read this entire file as a public GDS procedure surface contract. Extract procedure names, modes, columns, result shapes, side effects, errors, and verification fixtures. |

## Why This File Matters

- This tiny interface controls whether optional output columns are included in result builders.
- It is read in many pipeline/procedure execution paths to gate expensive payload fields.
- Small surface, high fan-in impact: `PipelineApplications` and many stream builders rely on this switch.

## Public Contract

- Interface in `org.neo4j.gds.api`:
  - `boolean contains(String fieldName);`
- Static fallback:
  - `ProcedureReturnColumns EMPTY = fieldName -> false;`
- Semantics:
  - returns `true` if caller requested/expecting a result field by name
  - empty/default implementation returns false for all field names.

## Internal Mechanics

- Single method functional interface (effectively an SPI) suitable for lambda/backed implementations.
- `EMPTY` is a lambda constant representing deterministic negative membership.
- No parsing or mutation logic is present; this is purely a query interface.

## Storage and Runtime Behavior

- No internal mutable state.
- Very low RAM/CPU overhead and likely hot-path usage during result shape decisions.
- In a pipeline/procedure facade, checking `contains(...)` should be cheap and should not allocate.
## Failure / Incompatibility Surfaces

- No checked exceptions are exposed here.
- Null handling and field-name normalization are delegated to implementations.
- If `contains` is backed by case-sensitive or expensive set lookup, result-shape compatibility can drift.

## Verification Oracles

1. **WHEN** `ProcedureReturnColumns.EMPTY.contains(any)` is called  
   **THEN** **SHALL** return `false`.
2. **WHEN** a concrete implementation returns true for a field  
   **THEN** **SHALL** trigger the corresponding result-field branch in caller-specific result builders.
3. **WHEN** caller checks unknown/absent field names repeatedly  
   **THEN** **SHALL** keep behavior stable and side-effect free.

## Rust Rewrite Notes

- **L1:** model as a small trait with zero state:
  - `fn contains(&self, field_name: &str) -> bool;`
- **L2:** provide an `EMPTY` singleton that always returns false.
- **L3:** keep field-name handling explicit (case and null policy) and document expected semantics at boundaries where this gate is consumed.

## Dependencies Read Next

- `PipelineApplications` (`linkPrediction*`, `nodeClassification*`) result-flag routing.
- Result builders using `probabilityDistribution`, `path`, `route`, and similar flags.
- Procedure facade layers that inject configured return-column selectors.

## Dependents As Tests

- Result-shape tests across stream/mutate/write endpoints where requested columns change output structure.
- Consistency tests ensuring `EMPTY` remains false for all fields.
## Open Questions

- Should Rust normalize field names (case/locale/aliases) at the `contains` boundary or defer to call-site conventions?
- Should negative lookups short-circuit through bitset/smallvec optimization for frequent calls in tight loops?
