use std::collections::BTreeSet;

use knight_bus::{
    GraphProjectionCatalog, GraphProjectionMetadata, GraphProjectionSpec, KnightBusError,
    MemoryEstimate, ProjectionSelector, ProjectionSidecarKind, ProjectionSidecarNeed,
    PropertySelector, RelationshipOrientation,
};

fn sample_projection_spec_now(orientation: RelationshipOrientation) -> GraphProjectionSpec {
    GraphProjectionSpec::new(
        "roads".to_owned(),
        ProjectionSelector::named(["City", "Airport"]).expect("node selector"),
        ProjectionSelector::named(["ROUTE"]).expect("relationship selector"),
        orientation,
        PropertySelector::named(["pagerank", "community"]).expect("node properties"),
        PropertySelector::named(["weight"]).expect("relationship properties"),
    )
    .expect("projection spec")
}

fn sample_projection_metadata_now() -> GraphProjectionMetadata {
    GraphProjectionMetadata::new(
        "alice".to_owned(),
        "neo4j".to_owned(),
        42,
        12,
        34,
        MemoryEstimate::projection(8_192, 1_024, 256, 128, 64),
        1_717_000_000_000,
    )
}

#[test]
fn graph_projection_catalog_tracks_named_graph_lifecycle_now() {
    let mut catalog = GraphProjectionCatalog::new();
    let spec = sample_projection_spec_now(RelationshipOrientation::Natural);
    let metadata = sample_projection_metadata_now();

    let created = catalog
        .project(spec.clone(), metadata.clone())
        .expect("projection should be created")
        .clone();

    assert!(catalog.exists("roads"));
    assert_eq!(catalog.len(), 1);
    assert!(!catalog.is_empty());
    assert_eq!(catalog.size_of("roads").expect("graph size"), (12, 34));

    let listed = catalog.list();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].graph_name, "roads");
    assert_eq!(listed[0].owner_user, "alice");
    assert_eq!(listed[0].database_name, "neo4j");
    assert_eq!(listed[0].snapshot_generation, 42);
    assert_eq!(listed[0].projection_spec, spec);
    assert_eq!(listed[0].memory_estimate, metadata.memory_estimate);
    assert_eq!(created.logical_relationship_count(), 34);

    match catalog.project(spec, metadata) {
        Err(KnightBusError::DuplicateGraphProjection { graph_name }) => {
            assert_eq!(graph_name, "roads");
        }
        other => panic!("expected duplicate graph projection error, got {other:?}"),
    }

    let dropped = catalog.drop("roads").expect("graph should drop");
    assert_eq!(dropped.graph_name, "roads");
    assert!(!catalog.exists("roads"));
    assert!(catalog.is_empty());

    match catalog.drop("roads") {
        Err(KnightBusError::UnknownGraphProjection { graph_name }) => {
            assert_eq!(graph_name, "roads");
        }
        other => panic!("expected unknown graph projection error, got {other:?}"),
    }
}

#[test]
fn relationship_orientation_parses_and_inverts_now() {
    assert_eq!(
        "NATURAL"
            .parse::<RelationshipOrientation>()
            .expect("natural"),
        RelationshipOrientation::Natural
    );
    assert_eq!(
        "reverse"
            .parse::<RelationshipOrientation>()
            .expect("reverse"),
        RelationshipOrientation::Reverse
    );
    assert_eq!(
        "undirected"
            .parse::<RelationshipOrientation>()
            .expect("undirected"),
        RelationshipOrientation::Undirected
    );

    assert_eq!(
        RelationshipOrientation::Natural.inverse(),
        RelationshipOrientation::Reverse
    );
    assert_eq!(
        RelationshipOrientation::Reverse.inverse(),
        RelationshipOrientation::Natural
    );
    assert_eq!(
        RelationshipOrientation::Undirected.inverse(),
        RelationshipOrientation::Undirected
    );

    let undirected = GraphProjectionCatalog::new()
        .project(
            sample_projection_spec_now(RelationshipOrientation::Undirected),
            sample_projection_metadata_now(),
        )
        .expect("undirected projection")
        .clone();
    assert_eq!(undirected.base_relationship_count, 34);
    assert_eq!(undirected.logical_relationship_count(), 68);
}

#[test]
fn property_selectors_become_sidecar_needs_now() {
    let spec = sample_projection_spec_now(RelationshipOrientation::Natural);

    let needs = spec
        .required_sidecar_needs()
        .into_iter()
        .collect::<BTreeSet<_>>();

    let expected = BTreeSet::from([
        ProjectionSidecarNeed::new(ProjectionSidecarKind::NodeProperty, "community"),
        ProjectionSidecarNeed::new(ProjectionSidecarKind::NodeProperty, "pagerank"),
        ProjectionSidecarNeed::new(ProjectionSidecarKind::RelationshipProperty, "weight"),
    ]);

    assert_eq!(needs, expected);
}

#[test]
fn projection_memory_estimate_avoids_duplicate_topology_now() {
    let estimate = MemoryEstimate::projection(8_192, 1_024, 256, 128, 64);

    assert_eq!(estimate.topology_reference_bytes, 8_192);
    assert_eq!(estimate.sidecar_bytes, 1_024);
    assert_eq!(estimate.catalog_metadata_bytes, 256);
    assert_eq!(estimate.duplicate_topology_bytes, 0);
    assert_eq!(estimate.heap_bytes, 128);
    assert_eq!(estimate.page_cache_bytes, 64);
    assert_eq!(estimate.direct_io_buffer_bytes, 0);
    assert_eq!(estimate.algorithm_state_bytes, 0);
    assert_eq!(estimate.delta_overlay_bytes, 0);
    assert_eq!(estimate.scratch_bytes, 0);
    assert_eq!(estimate.required_bytes, 9_664);
}
