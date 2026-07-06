# 12 catalog_lifecycle GraphStoreCatalog

## Source

**Evidence:** Full-source read performed as required by scope.

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | core/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java |
| lane | catalog_lifecycle |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 12 |
| line_count | 463 |
| fan_in / fan_out | 145 / 17 |
| purpose | Static in-memory graph catalog with per-user and per-database partitioning, load/remove/get semantics, listeners, and degree-distribution metadata cache. |
| read_prompt | Read this entire file as a catalog lifecycle contract. Extract concurrency assumptions, conflict semantics, error behavior, event hooks, and data lifetime guarantees for Rust implementation. |

## Why This File Matters

- This is the canonical graph-store registry used by procedure and algorithm plumbing.
- It controls key lifecycle operations for load/unload/read, including:
  - resolving ambiguous graph names,
  - enforcing not-found behavior,
  - and eventing on graph add/remove.
- It centralizes catalog shape assumptions (user-scoped + database-scoped keys).

## Public Contract

- `registerGraphStoreAddedListener` / `unregisterGraphStoreAddedListener`
- `registerGraphStoreRemovedListener` / `unregisterGraphStoreRemovedListener`
- `setLog(Log)`
- `get(CatalogRequest, String)`:
  - first checks own user catalog with `restrictSearchToUsernameCatalog`.
  - if none, checks all users for exact username-scoped graph match.
  - throws on zero (`GraphNotFoundException`) or >1 matches (`IllegalArgumentException` with usernames).
- `set(GraphProjectConfig, GraphStore)`:
  - inserts catalog entry (including `GraphStore` + `GraphProjectConfig` + `EphemeralResultStore` wrapper),
  - publishes add listener events.
- `remove(CatalogRequest, String, Consumer<GraphStoreCatalogEntry>, boolean)`:
  - supports own user-first removal and cross-user fallback when not restricted.
- `exists`, `graphStoreCount`, `isEmpty`
- degree distribution helpers:
  - `getDegreeDistribution`, `setDegreeDistribution`
- bulk clear:
  - `removeAllLoadedGraphs()`, `removeAllLoadedGraphs(DatabaseId)`
- query helpers:
  - `getGraphStores(String)`, `getGraphStores(String, DatabaseId)`, `getAllGraphStores()`

## Internal Mechanics

- Catalog is held in `ConcurrentHashMap<String, UserCatalog> userCatalogs` keyed by username.
- Event listener collections are mutable `HashSet`s:
  - `graphStoreAddedEventListeners`
  - `graphStoreRemovedEventListeners`
- `UserCatalog` holds:
  - `graphsByName: Map<UserCatalogKey, GraphStoreCatalogEntry>`
  - `degreeDistributionByName: Map<UserCatalogKey, Map<String,Object>>`
- `UserCatalogKey` is `@ValueClass` with `graphName` + `databaseName`.
- Removal path notifies listeners after entry deletion.
- On ambiguous graph name across users, remove/get include readable usernames list via `StringJoining.joinVerbose`.

## Memory And Storage Implications

- This file is a static shared registry (process-level memory store) and therefore a major RAM-residency consideration.
- `GraphStoreCatalogEntry` stores `GraphStore` plus config and ephemeral result store wrapper: large stores persist by reference until removal.
- `ConcurrentHashMap` and map copies are retained in memory until graph removed or cleared.
- `MemoryUsage.sizeOf(graphStore)` is used for listener payload sizing, indicating memory accounting expectations during lifecycle transitions.

## Snapshot And Catalog Invariants

- The catalog stores both catalog metadata and materialized graph store reference; this is the canonical source of truth used by proc execution paths.
- Ambiguous name behavior must preserve deterministic errors and include user context.
- `TestOnly` overloads (`get(username,databaseName,graphName)` etc.) confirm internal testability hooks and should map to internal APIs in rewrite harness.

## Verification Oracles

1. **WHEN** `get(request, graphName)` finds exactly one foreign user graph and `restrictSearchToUsernameCatalog == false`
   **THEN** **SHALL** return that graph entry.
2. **WHEN** there are no matching graphs and `failOnMissing` semantics are active
   **THEN** **SHALL** throw `GraphNotFoundException` with the same selection key details.
3. **WHEN** two+ users own matching graph names
   **THEN** **SHALL** throw `IllegalArgumentException` with joined usernames.
4. **WHEN** `set(config, graphStore)` is called with duplicate `(database, graphName)` for same user
   **THEN** **SHALL** reject with state exception and avoid overwrite.
5. **WHEN** `remove` is called with consumer
   **THEN** **SHALL** invoke consumer for removed entry and emit remove event payload containing graph byte size.

## Rust Rewrite Notes

- **L1:** implement a thread-safe `GraphStoreCatalog` with nested per-user map + event hooks.
- **L2:** preserve fail-on-ambiguity semantics explicitly:
  - zero match => not found
  - one match => return
  - many => ambiguous with usernames list.
- **L2:** preserve two-step catalog keying (`UserCatalogKey` by `(databaseName, graphName)`).
- **L3:** event subsystem should carry byte-size metric for add/remove for observability parity.
- Use explicit `close`/drop semantics for graph entries to prevent catalog leaks.

## Dependencies Read Next

- `core-api/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalogEntry.java`
- `core-api/src/main/java/org/neo4j/gds/core/loading/CatalogRequest.java`
- `core-api/src/main/java/org/neo4j/gds/core/loading/GraphNotFoundException.java`
- `core-api/src/main/java/org/neo4j/gds/config/GraphProjectConfig.java`

## Dependents As Tests

- Procedures calling `GraphStoreCatalog.get` directly in graph lookup/validation tests.
- Graph drop and project lifecycle tests that assert ambiguity and cross-user behavior.
- Degree distribution metadata tests around project metadata caching and invalidation.

## Open Questions

- Is thread visibility of `HashSet` listeners sufficient, or should they be concurrent collections as well?
- Should user/database indexes be canonicalized (case sensitivity, locale) in Rust rewrite?
- Do `degreeDistribution` values require deep immutability or only ownership transfer semantics?

## Coding Prompt Unlocked

Create a Rust `GraphStoreCatalog` with:
- static per-user stores,
- deterministic get/remove/set semantics,
- ambiguity and not-found errors mirroring Java messages,
- add/remove event hooks with metric payload,
- and tests for 0/1/many-match lookup scenarios.
