# 065 olap_algorithm MutateNodeProperty

## Source

| Field | Value |
| --- | --- |
| repo | neo4j-gds-src |
| file | applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/MutateNodeProperty.java |
| lane | olap_algorithm |
| tier | T1_IMPLEMENTATION_COMPLETE_READ |
| priority | 65 |
| line_count | 82 |
| fan_in / fan_out | 40 / 8 |

## Why This File Matters

This is the concrete mutation write adapter used by multiple algorithm families. It carries the contract boundary between algorithm results and graph-store updates.

## Public Contract

- **Evidence:** Class owns one dependency (`Log`) and has one behavioral surface: `mutateNodeProperties(...)` overloads that return `NodePropertiesWritten` (`34-81`).
- **Evidence:** It resolves target labels through `MutateNodePropertyConfig.nodeLabelIdentifiers(graphStore)` and writes to the configured `mutateProperty` (`47-53`).
- **Evidence:** Property values are optionally remapped for filtered views using `graph.asNodeFilteredGraph()` and `FilteredNodePropertyValues.OriginalToFilteredNodePropertyValues.create(...)` (`63-69`).
- **Evidence:** Final mutation side effect is `graphStore.addNodeProperty(new HashSet<>(labelsToUpdate), mutateProperty, maybeFilteredNodePropertyValues)` and then `NodePropertiesWritten(graph.nodeCount())` (`74-80`).
- **Inference:** Any rewrite must preserve both semantics: filtered graphs write only mapped values and return affected-node count as metadata.

## Internal Mechanics

- **Evidence:** Public overload with config delegates to label/value overload by extracting labels and property name (`47-53`).
- **Inference:** This establishes a thin façade and avoids duplicate addNodeProperty callsites.
- **Evidence:** Logging is explicit and in-band (`"Updating in-memory graph store"`), indicating mutation is intentionally visible (`72`).
- **Blocked:** No per-call rollback/transaction semantics are visible here; rollback behavior must be sourced from caller-level execution templates.

## Memory and Storage Implications

- **Inference:** Runtime memory impact is usually bounded to the `FilteredNodePropertyValues` wrapper and a temporary `HashSet` when labels are written (`63-69`, `75`).
- **Inference:** Write path is catalog-facing and mutates graph-store resident property arrays (`graphStore.addNodeProperty`).
- **Inference:** Return object (`NodePropertiesWritten`) carries lightweight completion metadata (`80`) and is suitable for low-overhead result builders.

## Snapshot And Catalog Implications

- **Evidence:** No catalog API is called directly, but the method performs irreversible in-memory state update (`74-80`).
- **Inference:** Invocation must remain behind a deterministic execution template that enforces mode correctness, access checks, and projection readiness.

## Verification Oracles

1. **WHEN** mutation is invoked with labels from config, **THEN** label IDs must be derived via `configuration.nodeLabelIdentifiers(graphStore)`.
2. **WHEN** graph is filtered, **THEN** `FilteredNodePropertyValues.OriginalToFilteredNodePropertyValues.create(...)` SHALL be used and the mapped values MUST be written to `graphStore`.
3. **WHEN** mutating a graph with `n` nodes, **THEN** returned `NodePropertiesWritten` SHALL contain `n` as the written-node estimate.
4. **WHEN** no filtered graph exists, **THEN** method SHALL fall back to raw `nodePropertyValues`.

## Rust Rewrite Notes

- **L1:** Create a small mutation service `MutateNodeProperty` with one method taking `(graph, graph_store, config, node_property_values)` and `(labels, mutate_property, values)`.
- **L1:** Keep label extraction in the config path to avoid repeating graph-store label policy across callers.
- **L2:** Expose filtered-value adapter that handles node-id remapping when graph is filtered.
- **L2:** Return a typed `NodePropertiesWritten { node_count: usize }` result.
- **L3:** Preserve explicit logging at mutation entry for parity with existing operational behavior.

## Dependencies Read Next

- `org.neo4j.gds.applications.algorithms.centrality.CentralityAlgorithmsMutateModeBusinessFacade`
- `org.neo4j.gds.applications.algorithms.machinery.MutateStep`
- `org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience`
- `org.neo4j.gds.config.MutateNodePropertyConfig`

## Dependents As Tests

- Integration tests around centrality/community mutate flows to assert same graphStore output and count reporting.
- Unit test for filtered-graph configuration:
  - graph has labels subset => filtered remapping path used.
  - graph has no filtered view => raw values path used.
- Verify `graphStore.addNodeProperty` is invoked with deduplicated label set.

## Open Questions

- Should mutation apply to multi-label updates as explicit set order or stable sorted order for deterministic snapshots?
- Should we add a metric around how many nodes actually changed vs `graph.nodeCount()`?
- Blocker: no direct metric of changed nodes in current file, so behavior contracts are count-based from existing Java shape.

