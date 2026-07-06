# 15 olap_algorithm AlgoBaseConfig

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | config-api/src/main/java/org/neo4j/gds/config/AlgoBaseConfig.java |
| lane | olap_algorithm |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 15 |
| line_count | 104 |
| fan_in / fan_out | 137 / 9 |
| purpose | Base algorithm config interface for default node/relationship selectors and graph validation hooks. |
| read_prompt | Read this entire file as the base algorithm contract for config defaults, selector resolution, and validation checks. |

## Why This File Matters

- Most algorithm configs inherit this interface (directly or via hierarchy), so defaults and validation hooks here are propagated broadly.
- Defines canonical defaults for `nodeLabels` and `relationshipTypes` selectors as `["*"]`-style wildcard (`ElementProjection.PROJECT_ALL`).
- Encapsulates common validation entry points for graph-store compatibility checks.

## Public Contract

- Inherited interfaces:
  - `BaseConfig`, `ConcurrencyConfig`, `JobIdConfig`.
- Constants:
  - `NODE_LABELS_KEY = "nodeLabels"`
  - `RELATIONSHIP_TYPES_KEY = "relationshipTypes"`
- Config keys/defaults:
  - `relationshipTypes()` defaults to `["__ALL__"]` (`ElementProjection.PROJECT_ALL`).
  - `nodeLabels()` defaults to same wildcard.
- Derived selectors:
  - `relationshipTypesFilter()` removes wildcard and maps strings to `RelationshipType`
  - `nodeLabelsFilter()` removes wildcard and maps strings to `NodeLabel`.
- Helper validators:
  - `projectAllRelationshipTypes()`
  - `internalRelationshipTypes(GraphStore)` delegates to `ElementTypeValidator.resolveTypes`.
  - `nodeLabelIdentifiers(GraphStore)` delegates to `ElementTypeValidator.resolve`.

## Internal Mechanics

- **Annotation-driven config contract:**
  - `@Configuration.Key` marks keys for serialization/parsing.
  - `@Configuration.Ignore` marks computed fields and internal checks.
  - `@Configuration.GraphStoreValidation` / `GraphStoreValidationCheck` mark validator hooks.
- **Validation lifecycle:**
  - `validateNodeLabels(...)` always checks selected labels with `ElementTypeValidator.validate`.
  - `validateRelationshipTypes(...)` always checks relationship types with `ElementTypeValidator.validateTypes`.
  - `graphStoreValidation(...)` is a no-op default extension point for overriding in concrete algorithms.

## Memory And Storage Implications

- This file is config-only and does not allocate heavy structures, but selector materialization (`Collectors.toSet`) impacts temporary allocation in validation paths.
- Correct filtering of wildcard vs explicit labels/types can materially affect downstream memory estimation and projection shape.

## Snapshot And Catalog Invariants

- `ProjectAll` sentinel is interpreted as "use all elements"; explicit types are computed against the `GraphStore` before algorithm execution.
- Any rewrite should preserve this sentinel/explicit split exactly to avoid catalog mismatch between estimated and actual algorithm runs.

## Verification Oracles

1. **WHEN** `relationshipTypes()` is omitted
   **THEN** **SHALL** return singleton list with `ElementProjection.PROJECT_ALL`.
2. **WHEN** relationship selector is explicit (`["KNOWS","LIKES"]`)
   **THEN** **SHALL** return `RelationshipType::of(...)` set and false for `projectAllRelationshipTypes()`.
3. **WHEN** invalid relationship labels are given
   **THEN** **SHALL** fail via `validateRelationshipTypes(...)`.
4. **WHEN** `nodeLabelIdentifiers(graphStore)` called with wildcard
   **THEN** **SHALL** resolve to wildcard-inclusive selection according to resolver rules.
5. **WHEN** custom config overrides `graphStoreValidation(...)`
   **THEN** **SHALL** execute override in addition to base validations.

## Rust Rewrite Notes

- **L1:** implement this as a trait shared by all algorithm config DTOs.
- **L2:** keep sentinel semantics explicit (`PROJECT_ALL`) and filter helpers that return empty vs non-empty lists.
- **L2:** split validation hook types:
  - per-config validation hook
  - graph validation hooks receiving resolved graph selections.
- **L3:** map annotation semantics (`Configuration.*`) to schema metadata for parser/runtime reflection.

## Dependencies Read Next

- `config-api/src/main/java/org/neo4j/gds/config/BaseConfig.java`
- `config-api/src/main/java/org/neo4j/gds/config/ConcurrencyConfig.java`, `JobIdConfig`
- `config-api/src/main/java/org/neo4j/gds/config/ElementTypeValidator.java`
- `ElementProjection`, `NodeLabel`, `RelationshipType` model classes.

## Dependents As Tests

- Algorithm config tests for defaults and input parsing.
- Procedure-level validation tests that assert invalid labels/types rejection before execution.

## Open Questions

- Should wildcard expansion be cached per graph for repeated configs?
- Which config-level validation should stay in base interface versus concrete algorithm-specific override?

## Coding Prompt Unlocked

Define a Rust shared config trait that provides:
- wildcard defaults for node/relationship selectors,
- validator hooks executed in a fixed order,
- and selector resolution functions against graph metadata.
