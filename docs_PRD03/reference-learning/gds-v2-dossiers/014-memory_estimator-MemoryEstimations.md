# 14 memory_estimator MemoryEstimations

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimations.java |
| lane | memory_estimator |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 14 |
| line_count | 860 |
| fan_in / fan_out | 139 / 7 |
| purpose | DSL factory for composing memory-estimation trees with compositional semantics, scaling transforms, and estimate-mode helpers. |
| read_prompt | Read this entire file as the MemoryEstimation DSL contract. Extract factory semantics, builder operations, component composition rules, and tree/estimation shape needed for a Rust port. |

## Why This File Matters

- Central source for how every algorithm memory estimate is assembled.
- Introduces a compositional tree model (`MemoryEstimation` + `MemoryTree`) used heavily by procedure estimation surfaces.
- Encodes semantics for aggregation, scaling, per-node/per-thread expansion, and delegation.

## Public Contract

- Static factory methods:
  - `empty()`
  - `maxEstimation(...)`
  - `delegateEstimation(...)`
  - `of(...)` overloads (`Class`, `(description, resident)`, `(description, range)`)
  - `setup(...)` overloads
  - `andThen(...)` overloads
  - `builder()`, `builder(description)`, `builder(Class)`, `builder(description, type)`
- `Builder` API:
  - field/scalar builders: `field`, `fixed`, `perNode`, `perNodeVector`, `perThread`, `rangePerNode`, `perGraphDimension`, `rangePerGraphDimension`
  - composition methods: `add`, `addComponentsOf`, `max`, `startField`, `endField`
  - `build()`
- Internal helper interfaces:
  - `MemoryRangeModifier`
  - `MemoryEstimationSetup`

## Internal Mechanics

- `MemoryEstimations` is a factory and also owns package-private test helpers:
  - `leafTree(...)`, `compositeTree(...)`.
- Factories mostly return immutable estimation objects:
  - `LeafEstimation`, `SetupEstimation`, `AndThenEstimation`, `CompositeEstimation`, `DelegateEstimation`, `MaxEstimation`.
- `AndThen` preserves component names and transforms `MemoryTree` via either `MemoryRangeModifier` or `UnaryOperator<MemoryRange>`.
- `CompositeTree` computes memory via `MemoryRange::add` across all components.
- `CompositeMaxTree` computes memory via `MemoryRange::max` for high-water-mark semantics.
- `Builder` carries parent pointer for hierarchical composition and `endField()` to attach child components.

## Memory And Storage Implications

- `MemoryEstimations` encodes explicit cost composition behavior:
  - additive scaling (`perNode`, `perThread`, `fixed`, `rangePerGraphDimension`)
  - multiplicative scaling via `times(...)` inside `AndThenTree`
  - max semantics where unknown upper-bound branch dominates.
- `times(count=0)` path may collapse ranges to empty and is an optimization/security behavior to validate.
- The DSL makes estimate tree explicit, which allows deterministic reporting and audit.

## Snapshot And Catalog Invariants

- Estimation is side-effect free and should be memoizable by config + graph dimensions + concurrency.
- The composition model is deterministic and should preserve the component tree shape for rendering and explainability.

## Verification Oracles

1. **WHEN** `builder(description).field("X", Foo.class).perNode("Y", MemoryRange.of(10)).build()` is executed
   **THEN** **SHALL** create a composite estimate with one `LeafTree("X", instance_size)` and one `LeafTree("Y", 10 * nodeCount)` component.
2. **WHEN** `andThen(estimation, UnaryOperator)` is called
   **THEN** **SHALL** preserve delegated component graph and apply output transform to memory usage at estimate time.
3. **WHEN** `times(0)` is encountered through `perThread`/`times(...)`
   **THEN** **SHALL** return zeroed `MemoryRange` (empty equivalent) in transformed tree.
4. **WHEN** `max(...)` is used with two estimates
   **THEN** **SHALL** return max component result, not sum.
5. **WHEN** `setup(...fn...)` returns another estimate
   **THEN** **SHALL** flatten evaluation to delegated estimate result before returning tree.

## Rust Rewrite Notes

- **L1:** define trait-like interfaces:
  - `MemoryEstimation { fn description(&self) -> &str; fn components(&self) -> Vec<&dyn MemoryEstimation>; fn estimate(&self, dims, conc) -> Box<dyn MemoryTree>; }`
  - `MemoryTree { fn description(&self); fn memory_usage(&self) -> MemoryRange; fn components(&self) -> Vec<&dyn MemoryTree>; }`
- **L2:** implement `Builder` with hierarchical state and parent stack (arena/Vec-based).
- **L2:** preserve max-vs-sum split (`CompositeTree`/`CompositeMaxTree`) exactly.
- **L3:** keep a narrow set of constructors and unit-test each DSL operator.

## Dependencies Read Next

- `memory-usage/src/main/java/org/neo4j/gds/mem/MemoryRange.java`
- `memory-usage/src/main/java/org/neo4j/gds/mem/MemoryEstimation.java`
- `memory-usage/src/main/java/org/neo4j/gds/mem/Estimate.java`
- all estimate-producing business façade code that currently calls factory methods.

## Dependents As Tests

- Algorithm `*MemoryEstimation` classes asserting estimate-tree shape.
- Procedure estimate endpoints that compare memory estimate component names and totals.

## Open Questions

- Should delegate nodes preserve original description or wrapper description on `to_tree` output in Rust?
- How strict should equality be for component ordering in composite trees for test stability?
- Should overflow checking happen at compose-time or estimate-time?

## Coding Prompt Unlocked

Build a Rust memory DSL matching this contract:
- `builder` with hierarchical fields,
- `Leaf/Setup/AndThen/Composite/Delegate/Max` types,
- `estimate()` output tree with deterministic `memory_usage` computation,
- and tests asserting max-vs-sum and scaling semantics.
