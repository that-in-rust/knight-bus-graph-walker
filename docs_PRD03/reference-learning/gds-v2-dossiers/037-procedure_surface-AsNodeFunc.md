# 37 procedure_surface AsNodeFunc

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | proc/common/src/main/java/org/neo4j/gds/functions/AsNodeFunc.java |
| lane | procedure_surface |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 37 |
| line_count | 57 |
| fan_in / fan_out | 68 / 1 |

## Why This File Matters

This file is a public procedure function surface for converting numeric IDs to `Node` objects, used in user-facing Cypher function calls.

## Public Contract

- **Evidence:** `@Context public KernelTransaction tx;` injects the transaction context (`36-40`).
- **Evidence:** `asNode(Number)` is annotated `@UserFunction("gds.util.asNode")` and maps one `nodeId` to single node via `getNodeById(tx, nodeId.longValue())` (`42-45`).
- **Evidence:** `asNodes(List<Number>)` is annotated `@UserFunction("gds.util.asNodes")` and maps each ID to node, filtering nulls (`48-55`).
- **Inference:** `asNodes` returns empty list when input list is empty or all IDs resolve to null (`49-55` via stream+filter+collect).

## Internal Mechanics

- **Evidence:** Uses `GraphDatabaseApiProxy.getNodeById` helper as a compatibility adapter (`34`).
- **Evidence:** Stream pipeline for batch function: map each ID and discard null values (`50-55`).
- **Inference:** Behavior intentionally ignores missing IDs for list form instead of preserving null slots.

## Memory And Storage Implications

- **Evidence:** Per-call allocations include stream objects and result list in `asNodes` only; `asNode` is constant memory.
- **Inference:** `asNodes` null-filtering can reduce list size vs input size, important for deterministic output size assumptions.

## Snapshot And Catalog Implications

- **Evidence:** Procedure signature is fixed via `gds.util.*`; this is a compatibility surface contract.
- **Inference:** Any rewrite should treat output cardinality difference of null filtering as part of accepted behavior.

## Verification Oracles

1. **WHEN** `gds.util.asNode(42)` maps to an existing node **THEN** it **SHALL** return the corresponding Node.
2. **WHEN** `gds.util.asNode(42)` maps to a missing node **THEN** it **SHALL** return `null`.
3. **WHEN** `gds.util.asNodes([id1, id2, ...])` contains missing IDs **THEN** the output **SHALL** omit missing nodes and return only present nodes.
4. **WHEN** `asNodes` receives an empty list **THEN** it **SHALL** return an empty list.

## Rust Rewrite Notes

- **L1:** user-function module exposing `as_node` and `as_nodes`.
- **L2:** one helper wrapper around kernel graph API for consistent Node lookup.
- **L3:** transaction-context handling abstraction (`KernelTransaction`) to keep procedure tests deterministic.
- **L3:** preserve stream semantics (filter missing) in `as_nodes`.

## Dependencies Read Next

- `compatibility/api/neo4j-kernel-adapter/src/main/java/org/neo4j/gds/compat/GraphDatabaseApiProxy.java`
- doc test callsites under `doc-test/src/test/java/org/neo4j/gds/doc/`
- procedure function registration layer for `proc/common`.

## Dependents As Tests

- `doc-test/src/test/java/org/neo4j/gds/doc/*` numerous integration docs use `gds.util.asNode` / `asNodes`.
- procedure/function registration integration checks for function signatures and description metadata.

## Open Questions

- Should missing IDs in `asNodes` remain filtered in all contexts, or should an alternate strict mode be offered for compatibility consumers needing positional output?

## Coding Prompt Unlocked

Implement a gds function module with:
1) `as_node(node_id)` resolving through DB adapter and returning Option<Node>;
2) `as_nodes(node_ids)` resolving and filtering nulls;
3) transaction-context injection tests for single and batched forms.
