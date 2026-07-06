# 1 catalog_lifecycle GraphStore

## Source

**Evidence:** Full-source read performed as required by scope.


| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | core-api/src/main/java/org/neo4j/gds/api/GraphStore.java |
| lane | catalog_lifecycle |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 1 |
| line_count | 238 |
| fan_in / fan_out | 453 / 18 |
| purpose | Map graph identity, lifecycle, catalog scoping, snapshot ownership, and artifact state. |
| read_prompt | Read this entire file as a catalog/snapshot lifecycle contract. Extract identities, ownership boundaries, lifecycle transitions, concurrency assumptions, error cases, and what a Rust implementation must persist or reject. |

## Why This File Matters

**Evidence:** `GraphStore` is the primary graph API boundary exposed to algorithms, projections, and procedures. It owns node/relation metadata, graph properties, and graph view accessors in one central interface. [core-api/src/main/java/org/neo4j/gds/api/GraphStore.java:48].

**Inference:** This file effectively defines compatibility rules for OLAP snapshots: everything above it is read/write via this interface (or equivalent Rust trait).

## Public Contract

- **Identity/lifecycle metadata:**
  - `databaseInfo()`, `creationTime()`, `modificationTime()`, `schema()`, `capabilities()` [Lines 50-59].
- **Node APIs:**
  - `nodeCount()`, `nodes()`, `nodePropertyKeys(...)`, `nodeProperty(String)`, `addNodeProperty`, `removeNodeProperty`, etc. [Lines 80-123].
- **Relationship APIs:**
  - `relationshipCount`, `relationshipTypes`, `relationshipPropertyKeys`, `addRelationshipType`, `addInverseIndex`, `deleteRelationships`. [Lines 128-173].
- **Graph access APIs:**
  - overloads of `getGraph(...)` and `getUnion()`; default overloads for convenience [Lines 174-238].
- **Graph properties:**
  - graph property registry methods `graphPropertyKeys`, `addGraphProperty`, `removeGraphProperty`, `graphProperty(...)`, `graphPropertyValues(...)` [Lines 62-69].
- **Implied immutability boundary:** defaults are provided for intersections of labels/properties and graph overload forwarding [Lines 105-118,174-188].

## Internal Mechanics

- **Evidence:** This is an interface only—no implementation logic appears in this file [Line 48 onward].
- **Evidence:** default helpers compute intersections for requested labels/property sets before returning collections [Lines 105-118,142-151], so empty selectors return empty lists and prevent null-like behavior.
- **Inference:** The implementer is responsible for enforcing consistency of mutating methods because setters exist without success/failure contract here.

## Memory And Storage Implications

- **Evidence:** No allocator fields or caps are specified here; memory behavior is delegated to downstream stores [No internal state fields].
- **Evidence:** `getGraph` returns a potentially rich `Graph` abstraction by filters but does not define copy/reference semantics [Lines 174-188].
- **Inference:** In Rust, this boundary should avoid eager materialization and preserve borrowed or arena-backed data to keep OLAP memory budgets stable.

## Snapshot And Catalog Implications

- **Evidence:** `databaseInfo`, `schema`, `creationTime`, `modificationTime` establish snapshot-level bookkeeping suitable for catalog validation and stale-cache checks [Lines 50-57,52,53].
- **Inference:** `addNodeLabel`, `addNodeProperty`, `addRelationshipType`, and delete APIs imply that `GraphStore` may be mutable, so versioned persistence or copy-on-write should be explicit before publication.
- **Evidence:** High fan-out references in queue imply this interface is central to lifecycle-dependent features (projection, algorithms, catalog tooling).

## Verification Oracles

1. **WHEN** a test graph store mock is created with labels `{Person}` and properties `{age: int}`
   **THEN** **SHALL** `nodePropertyKeys(NodeLabel.of("Person"))` return only existing property keys for that label and `nodePropertyKeys(Collections.emptyList())` returns empty list.

2. **WHEN** `getGraph(NodeLabel.of("Person"))` is invoked on a graph with no relationship request
   **THEN** **SHALL** return a node-only graph view and not include relationship columns.

3. **WHEN** `creationTime` is after `modificationTime` in snapshot fixture
   **THEN** **SHALL** a catalog validator reject the artifact before procedure execution.

4. **WHEN** implementation returns empty relationship type set and caller requests `addInverseIndex`
   **THEN** **SHALL** either no-op safely or return typed validation error (not silently corrupt data).

## Rust Rewrite Notes

- **L1:** define a `GraphStore` trait with immutable identity (`creation_time`, `modification_time`, `schema`) and accessor methods.
- **L1:** add value types `GraphIdentity`, `GraphSnapshotMeta`, `CatalogTimestamp`.
- **L2:** add borrowing-oriented `GraphStoreReadContract` methods that return borrowed `Graph`/`PropertyView` objects.
- **L3:** add adapter wrapper for existing procedure handlers and catalog services translating to/from this trait.
- **Lineage invariants:** preserve `mod_time >= creation_time` in constructors; expose this check in `GraphStoreValidator`.

## Dependencies Read Next

| target_file | reason |
| --- | --- |
| core-api/src/main/java/org/neo4j/gds/core/loading/GraphStoreCatalog.java | graph lifecycle and catalog storage behavior |
| core-api/src/main/java/org/neo4j/gds/api/Graph.java | graph view abstraction |
| core-api/src/main/java/org/neo4j/gds/api/IdMap.java | node id mapping contract |
| core-api/src/main/java/org/neo4j/gds/api/properties/graph/GraphProperty.java | property value object contract |
| core-api/src/main/java/org/neo4j/gds/api/CompositeRelationshipIterator.java | iterator contract requested by this interface |

## Dependents As Tests

| source_file | reason |
| --- | --- |
| algo-common/src/main/java/org/neo4j/gds/GraphStoreAlgorithmFactory.java | algorithm factory entrypoint using `GraphStore` |
| applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmFactory.java | algorithm orchestration using graph store metadata |
| proc/catalog/src/main/java/org/neo4j/gds/catalog/GraphDropProc.java | lifecycle behavior in catalog operations |
| procedures/procedures-facade-api/src/main/java/org/neo4j/gds/procedures/GraphDataScienceProcedures.java | facade exposure to stores |
| graph-store-catalog/src/main/java/org/neo4j/gds/applications/graphstorecatalog/DefaultGraphCatalogApplications.java | catalog interactions with graph stores |

## Open Questions

- Is `addNodeProperty` expected to create schema entries lazily or require pre-existing labels/types?
- Is graph mutation here protected by caller-level locks or should trait methods return `Arc<RwLock<_>>`-like guards?
- Should failed lookups emit `Option`/`Result` style errors or empty collections with logs?

## Coding Prompt Unlocked

Implement a Rust `GraphStore` contract test harness with a `MockGraphStore`:
- define `GraphStore` trait,
- enforce monotonic snapshot timestamps,
- verify `nodePropertyKeys(labels)` computes intersections,
- verify default-overload behavior for `get_graph(NodeLabel.of("Person"))` and relationship filtering.
