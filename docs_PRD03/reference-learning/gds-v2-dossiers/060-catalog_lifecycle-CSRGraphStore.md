# 60 catalog_lifecycle CSRGraphStore

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | core/src/main/java/org/neo4j/gds/core/loading/CSRGraphStore.java |
| lane | catalog_lifecycle |
| tier | T0_IMPLEMENTATION_COMPLETE_READ |
| priority | 60 |
| line_count | 755 |
| fan_in / fan_out | 10 / 40 |

## Why This File Matters

`CSRGraphStore` is the concrete graph-store implementation with the highest concentration of mutation, schema maintenance, graph projection, and validation behavior in this lane. It is one of the top RAM-sensitive compatibility surfaces for rewrite stability.

## Public Contract

- **Evidence:** `CSRGraphStore` implements `GraphStore` and owns lifecycle-bearing state (`nodes`, `relationships`, schema, properties, `modificationTime`) (`80-129`).
- **Evidence:** Node property operations:
  - `addNodeProperty` performs duplicate guard and schema update (`293-325`),
  - `removeNodeProperty` mutates store and schema (`331-342`).
- **Evidence:** Graph-property operations:
  - `addGraphProperty` rejects duplicates and updates schema map (`204-226`),
  - `removeGraphProperty` updates schema map (`230-243`).
- **Evidence:** Relationship operations:
  - `addRelationshipType` inserts new relationship data and schema (`430-437`),
  - `addInverseIndex` stores inverse topology and optional properties (`441-454`),
  - `deleteRelationships` returns `DeletionResult` with counts for relationships and property values (`456-474`).
- **Evidence:** Graph materialization:
  - `getGraph(Collection<NodeLabel>, Collection<RelationshipType>, Optional<String>)` branches to node-only vs typed-relationship graph generation (`482-493`),
  - `getUnion` materializes union graphs per relationship-property combinations (`496-512`).

## Internal Mechanics

- **Evidence:** All mutating methods call `updateGraphStore` which updates `modificationTime` after mutation (`587-590`).
- **Evidence:** Schema updates happen in-place through `MutableGraphSchema` and relation maps (node/relationship/graph property schema mutation code in `addNodeProperty`, `removeGraphProperty`, `addRelationshipType`) rather than immutable replacements.
- **Evidence:** `getFilteredIdMap` opts for node-label filtering only when necessary (`655-662`), a core optimization path to avoid unnecessary filtering.
- **Evidence:** `validateInput` enforces both relationship-type existence and property-key existence before graph materialization (`728-754`).
- **Inference:** The implementation is optimized for imperative in-place updates with synchronization around mutation hotspots.

## Memory And Storage Implications

- **Evidence:** Node/relationship property stores are replaced by builder-based immutable snapshots assigned into existing fields (`215-220`, `232-244`, `308-319`).
- **Evidence:** Large graph creation paths (`getGraph`, `createGraph`, `createNodeOnlyGraph`) allocate `HugeGraphBuilder` structures and filtered maps (`477-652`).
- **Inference:** Main RAM hotspots are graph/materialization paths; mutation operations also allocate temporary maps/lists during schema updates and property filtering.
- **Inference:** Synchronized mutation and mutable shared fields suggest careful ownership boundaries in Rust (interior mutability + locking for concurrent readers) to preserve correctness.

## Snapshot And Catalog Implications

- **Evidence:** `getGraph` returns graph projections based on computed filters and optional relationship property key (`481-493`), so projection behavior is part of query contract.
- **Evidence:** `getCompositeRelationshipIterator` validates unknown relationship types and missing property keys and throws informative exceptions (`519-546`).
- **Inference:** Rewrite must preserve error messaging style for compatibility (especially property/type mismatch cases), even if exception type changes.

## Verification Oracles

1. **WHEN** `addNodeProperty` sees an existing property key, **THEN** it SHALL throw `UnsupportedOperationException` with the same duplicate message shape.
2. **WHEN** `getGraph` is called with empty relationship types, **THEN** it SHALL return a node-only graph path (`createNodeOnlyGraph`).
3. **WHEN** unknown relationship types are requested in `getGraph`/`getCompositeRelationshipIterator`, **THEN** it SHALL raise `IllegalArgumentException` before graph materialization.
4. **WHEN** graph materialization uses filtered labels, **THEN** filtered properties and schema must remain aligned to filtered labels.
5. **WHEN** `deleteRelationships` is called, **THEN** result counts must include relationship and per-property element counts.

## Rust Rewrite Notes

- **L1:** Preserve stateful object boundaries for store lifecycle while isolating immutable reads from mutating writes.
- **L2:** Model updates through explicit mutation methods that perform validation before applying map/schema deltas.
- **L2:** Keep `updateGraphStore` as a single entry for all mutating methods to guarantee centralized timestamp updates.
- **L3:** Implement graph-materialization helpers as separable functions (`create_node_only_graph`, `create_graph_for_types`, `get_filtered_id_map`) for test granularity.

## Dependencies Read Next

- `org.neo4j.gds.api.GraphStore`
- `org.neo4j.gds.core.huge.HugeGraphBuilder`
- `org.neo4j.gds.core.huge.NodeFilteredGraph`
- `org.neo4j.gds.core.huge.UnionGraph`
- `core/src/main/java/org/neo4j/gds/core/loading/DeletionResult.java`
- `core/src/main/java/org/neo4j/gds/core/loading/RelationshipImportResult.java`

## Dependents As Tests

- Property mutation tests:
  - duplicate add/remove property behaviors for graph/node/relationship properties,
  - schema mutation correctness.
- `getGraph` property/key/rtype matrix tests:
  - node-only path vs relationship path,
  - composite iterator validation paths.
- Concurrency stress test around `updateGraphStore` timestamp and in-place schema mutations.

## Open Questions

- Could schema updates be made fully copy-on-write in Rust, or should mutable in-place updates stay for fidelity/performance?
- What level of locking should guard `updateGraphStore` in high-concurrency reads?
- Should `modificationTime` update on successful mutation only, and be monotonic across failed attempts?

## Coding Prompt Unlocked

Rewrite `CSRGraphStore` as an ownership-aware Rust graph store:
1) keep mutation paths centralized through one guarded updater,
2) preserve node/relationship/graph property schema consistency during add/remove,
3) preserve `getGraph` filtering semantics and validation ordering,
4) preserve error classes/messages for compatibility-sensitive failures.
