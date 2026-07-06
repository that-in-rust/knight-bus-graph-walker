# 59 catalog_lifecycle GraphStoreCatalogService

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalogService.java |
| lane | catalog_lifecycle |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 59 |
| line_count | 228 |
| fan_in / fan_out | 33 / 17 |

## Why This File Matters

This service is the mutable-adapter boundary for catalog access and graph-store preparation. It is the primary “friendlier” entrypoint that turns the static `GraphStoreCatalog` helpers into testable, explicit orchestration without forcing every caller to pass a `CatalogRequest` directly.

## Public Contract

- **Evidence:** `graphExists(User, DatabaseId, GraphName)` delegates to `GraphStoreCatalog.exists` and performs no state change (`50-52`).
- **Evidence:** `removeGraph(CatalogRequest, GraphName, boolean)` passes a `result::set` callback into `GraphStoreCatalog.remove`, returning the removed entry (`54-67`).
- **Evidence:** `getGraphResources(...)` composes a full load-validation-pipeline by:
  - resolving `GraphStoreCatalogEntry`,
  - optional post-load graph store validation hooks (`89`),
  - resolving projected labels/types (`91-93`),
  - configuration validation (`95`),
  - optional ETL extraction hooks (`97`),
  - graph materialization (`99`),
  - optional post-load graph validation hooks (`101`),
  - returning `GraphResources` tuple (`103`).
- **Evidence:** `ensureGraphDoesNotExist(...)` and `ensureGraphExists(...)` are explicit guards that throw clear `IllegalArgumentException` messages for precondition failures (`164-171`, `179-187`).

## Internal Mechanics

- **Evidence:** `getGraphResources` uses `getGraphStoreCatalogEntry(...)` and then invokes config-driven filters, so projection filters are always re-derived from current configuration at call time (`85-93`, `148-157`).
- **Evidence:** `CatalogRequest.of(user, databaseId, config.usernameOverride())` centralizes username override behavior for catalog lookup (`154-156`).
- **Evidence:** degree-distribution helpers (`getDegreeDistribution` / `setDegreeDistribution`) are direct pass-throughs to catalog storage (`189-208`).
- **Inference:** Most behavior in this file is adapter logic; compatibility risk is in sequencing and validation invocation points, not low-level catalog storage internals.

## Memory And Storage Implications

- **Inference:** Memory churn here is mostly transient orchestration state; heavy storage mutation happens in catalog and graph objects reached through delegated calls.
- **Evidence:** `GraphResources` bundles `GraphStore`, `Graph`, and `resultStore` by reference with no cloning (`103`).
- **Evidence:** `update`/`mutate` behavior is delegated (e.g., `removeGraph`, `set`) to `GraphStoreCatalog`, so rewrite RAM cost depends on catalog implementation.

## Snapshot And Catalog Implications

- **Inference:** This file should be rewritten in Rust as a thin application service with dependency injection for catalog/validation hooks.
- **Inference:** This layer enforces *order-sensitive* graph loading invariants: hooks may assume validated graph store/graph states and must preserve same order.
- **Inference:** The `getGraphResources` path is where “load + validate + filter + materialize + validate again” semantics should be preserved to avoid correctness drift for OLAP algorithms.

## Verification Oracles

1. **WHEN** `graphExists` is called, **THEN** it SHALL return the same boolean outcome as `GraphStoreCatalog.exists` for the provided `user/databaseId/graphName`.
2. **WHEN** `getGraphResources` is called with optional validation hooks, **THEN** it SHALL execute hooks in this order: store validation → graph validation, with filtering and `graphStoreValidation` occurring before graph materialization.
3. **WHEN** `removeGraph` is called with `shouldFailIfMissing=false`, **THEN** call path SHALL still be attempted and callback-based extraction should be used.
4. **WHEN** `ensureGraphExists`/`ensureGraphDoesNotExist` precondition checks fail, **THEN** they SHALL throw `IllegalArgumentException` with the corresponding message form.

## Rust Rewrite Notes

- **L1:** Keep this as a pure service over injected traits: `GraphStoreCatalogTrait`, `GraphStoreCatalogEntryFactory`, and hook traits.
- **L2:** Preserve exact validation sequencing and optional hook application order as separate branches to keep behavior testable.
- **L3:** Keep graph-name extraction and `CatalogRequest` creation pure and explicit.
- **L3:** Keep degree-distribution getters/setters as thin pass-throughs to keep migration surface small.

## Dependencies Read Next

- `core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java`
- `core/src/main/java/org/neo4j/gds/core/loading/GraphResources.java`
- `core/src/main/java/org/neo4j/gds/core/loading/PostLoadValidationHook.java`
- `core/src/main/java/org/neo4j/gds/core/loading/PostLoadETLHook.java`

## Dependents As Tests

- Procedure and pipeline paths that resolve a graph before execution should assert hook order through a mocked catalog/hook implementation.
- `getGraphResources` should be covered by tests for:
  - successful path with both hooks present,
  - missing relationship property and validation exceptions,
  - node label/rtype filtering fallback behavior (`projectAll*` behavior via config).
- A regression test for `removeGraph(..., shouldFailIfMissing)` behavior should verify callback semantics.

## Open Questions

- Should `CatalogRequest` creation also include database-level auth context in Rust beyond `(user,databaseId,usernameOverride)`?
- Should hooks be typed as `fn(GraphStore)` + `fn(Graph)` or trait objects to preserve call-site readability?
- Should `getAllGraphStores` expose iterator/lazy semantics or an owned vector in Rust for ownership safety?

## Coding Prompt Unlocked

Implement `GraphStoreCatalogService` as a thin orchestration service:
1) inject catalog and hook dependencies,
2) expose `getGraphResources` with exact pre/post validation order,
3) keep helper methods side-effect-free except required catalog mutations,
4) model explicit preconditions as typed errors for missing/duplicate graphs.
