# 48 procedure_surface GdsCallable

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | executor/src/main/java/org/neo4j/gds/executor/GdsCallable.java |
| lane | procedure_surface |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 48 |
| line_count | 47 |
| fan_in / fan_out | 57 / 1 |

## Why This File Matters

This annotation defines the execution registration contract for all callable algorithm/mode classes surfaced to Cypher procedures.

## Public Contract

- **Evidence:** `@Target(ElementType.TYPE)` and `@Retention(RetentionPolicy.RUNTIME)` make this a type-level runtime annotation for discoverability (`35–37`).
- **Evidence:** Required attribute `name()` establishes procedure identity (`39`).
- **Evidence:** Optional aliases via `aliases()` with default empty array (`41`).
- **Evidence:** Required execution behavior indicator `executionMode()` (`43`), plus optional `description()` (`45`).
- **Evidence:** Class-level javadoc explicitly states callable is a unit of cypher procedure composition, not algorithm internals (`27–33`).

## Internal Mechanics

- **Evidence:** Default constructor requirement is documented (`33`) but not enforced by annotation metadata, implying framework/runtime validation (`33`).
- **Inference:** Discovery and registration likely scans runtime annotations for procedure injection.
- **Blocked:** Actual scanner behavior and validation timing are defined outside this file.

## Memory And Storage Implications

- **Evidence:** Annotation metadata is constant metadata, not runtime state (`27–47`).
- **Inference:** Zero-runtime per-call allocation from annotation itself; heavy implications are in registries that consume it.

## Snapshot And Catalog Implications

- **Inference:** Procedure identity is contractually tied to annotation metadata (`name`, `aliases`, `executionMode`), so any rewrite should treat this as a stable API boundary for compatibility snapshots.

## Verification Oracles

1. **WHEN** a procedure class is callable, **THEN** it **SHALL** be annotated with `@GdsCallable` including `name` and `executionMode`.
2. **WHEN** alternate procedure names are required, **THEN** `aliases()` **SHALL** enumerate them.
3. **WHEN** framework scans at runtime, **THEN** this annotation **SHALL** be retained at runtime (`RetentionPolicy.RUNTIME`).

## Rust Rewrite Notes

- **L1:** Derive a trait-like marker representation for Rust callable metadata.
- **L2:** Replace Java annotation usage with explicit registration struct (`CallableMeta`) and compile-time registration table.
- **L3:** Registry loader that maps callable name + aliases to execution handlers using metadata mode enums.

## Dependencies Read Next

- `executor/src/main/java/org/neo4j/gds/executor/ExecutionMode.java`
- `executor/src/main/java/org/neo4j/gds/executor/ExecutionContext.java`
- Procedure registration paths that scan and register `@GdsCallable`.

## Dependents As Tests

- Integration tests that assert callable discovery yields expected algorithm/mode names.
- Alias resolution tests for deprecated/compatibility callables.

## Open Questions

- Rust cannot preserve Java annotations; do we migrate to a generated static registry and maintain parity checks against `name`/`aliases` strings?

## Coding Prompt Unlocked

Create explicit callable registration in Rust with:
1) metadata type containing name, aliases, execution mode, and description,
2) startup registration loader,
3) tests for runtime metadata visibility and alias collision behavior.
