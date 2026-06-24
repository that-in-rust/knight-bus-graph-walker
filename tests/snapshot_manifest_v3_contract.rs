mod support;

use std::fs;

use knight_bus::{
    MmapWalkRuntime, SnapshotBuildOptions, SnapshotLogicalOrientation, SnapshotManifest,
    SnapshotSidecarCatalog, SnapshotSidecarEntry, SnapshotSidecarValueScope,
    build_snapshot_from_paths, build_snapshot_from_paths_with_options,
};

fn read_snapshot_manifest_now(snapshot_dir: &std::path::Path) -> SnapshotManifest {
    let manifest_path = snapshot_dir.join("manifest.json");
    let manifest_json = fs::read_to_string(&manifest_path).expect("manifest json");
    serde_json::from_str(&manifest_json).expect("manifest parses")
}

#[test]
fn snapshot_manifest_v3_round_trips_and_runtime_reads_v2_now() {
    let (_temp_dir, snapshot_dir) = support::temp_snapshot_dir();
    build_snapshot_from_paths(
        &support::valid_nodes_path(),
        &support::valid_edges_path(),
        &snapshot_dir,
    )
    .expect("snapshot builds");

    let manifest = read_snapshot_manifest_now(&snapshot_dir);
    assert_eq!(manifest.version, 3);

    let manifest_json = serde_json::to_string_pretty(&manifest).expect("manifest serializes");
    let round_tripped: SnapshotManifest =
        serde_json::from_str(&manifest_json).expect("manifest round-trips");
    assert_eq!(round_tripped, manifest);

    let manifest_path = snapshot_dir.join("manifest.json");
    let mut legacy_value = serde_json::to_value(&manifest).expect("manifest to value");
    let object = legacy_value
        .as_object_mut()
        .expect("manifest must serialize as an object");
    object.insert("version".to_owned(), serde_json::json!(2));
    object.remove("snapshot_generation");
    object.remove("source_tx_start");
    object.remove("source_tx_end");
    object.remove("built_at_epoch_millis");
    object.remove("logical_orientations");
    object.remove("sidecar_catalog");
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&legacy_value).expect("legacy json"),
    )
    .expect("legacy manifest written");

    let runtime = MmapWalkRuntime::open(&snapshot_dir).expect("v2 manifest still opens");
    assert_eq!(runtime.node_count(), 39);
}

#[test]
fn snapshot_manifest_records_generation_metadata_and_orientation_views_now() {
    let (_temp_dir, snapshot_dir) = support::temp_snapshot_dir();
    let options = SnapshotBuildOptions {
        snapshot_generation: Some(42),
        source_tx_start: Some(100),
        source_tx_end: Some(120),
        ..SnapshotBuildOptions::default()
    };

    build_snapshot_from_paths_with_options(
        &support::valid_nodes_path(),
        &support::valid_edges_path(),
        &snapshot_dir,
        &options,
    )
    .expect("snapshot builds");

    let manifest = read_snapshot_manifest_now(&snapshot_dir);
    assert_eq!(manifest.snapshot_generation, 42);
    assert_eq!(manifest.source_tx_start, Some(100));
    assert_eq!(manifest.source_tx_end, Some(120));
    assert!(manifest.built_at_epoch_millis > 0);
    assert_eq!(manifest.node_count, 39);
    assert_eq!(manifest.edge_count, 67);
    assert_eq!(
        manifest.logical_orientations,
        vec![
            SnapshotLogicalOrientation::Natural,
            SnapshotLogicalOrientation::Reverse,
            SnapshotLogicalOrientation::Undirected,
        ]
    );
    assert!(manifest.sidecar_catalog.label_sidecars.is_empty());
    assert!(
        manifest
            .sidecar_catalog
            .relationship_type_sidecars
            .is_empty()
    );
    assert!(manifest.sidecar_catalog.node_property_sidecars.is_empty());
    assert!(
        manifest
            .sidecar_catalog
            .relationship_property_sidecars
            .is_empty()
    );
    assert!(manifest.sidecar_catalog.weight_sidecars.is_empty());
    assert!(manifest.sidecar_catalog.feature_sidecars.is_empty());
    assert!(manifest.sidecar_catalog.result_sidecars.is_empty());
}

#[test]
fn snapshot_manifest_sidecar_catalog_round_trips_now() {
    let manifest = SnapshotManifest {
        version: 3,
        node_id_width: 32,
        adjacency_offset_width: 64,
        node_count: 2,
        edge_count: 1,
        key_mode: "sorted_key_index".to_owned(),
        storage_mode: knight_bus::SnapshotStorageMode::ImmutableDualCsr,
        snapshot_generation: 7,
        source_tx_start: Some(10),
        source_tx_end: Some(11),
        built_at_epoch_millis: 1_717_000_000_000,
        logical_orientations: vec![
            SnapshotLogicalOrientation::Natural,
            SnapshotLogicalOrientation::Reverse,
            SnapshotLogicalOrientation::Undirected,
        ],
        sidecar_catalog: SnapshotSidecarCatalog {
            label_sidecars: vec![SnapshotSidecarEntry::new(
                "labels.city".to_owned(),
                "label_sidecars/city.bin".to_owned(),
                SnapshotSidecarValueScope::Node,
            )],
            relationship_type_sidecars: vec![SnapshotSidecarEntry::new(
                "types.route".to_owned(),
                "reltype_sidecars/route.bin".to_owned(),
                SnapshotSidecarValueScope::Relationship,
            )],
            node_property_sidecars: vec![SnapshotSidecarEntry::new(
                "pagerank".to_owned(),
                "node_properties/pagerank.f64".to_owned(),
                SnapshotSidecarValueScope::Node,
            )],
            relationship_property_sidecars: vec![SnapshotSidecarEntry::new(
                "cost".to_owned(),
                "relationship_properties/cost.f64".to_owned(),
                SnapshotSidecarValueScope::Relationship,
            )],
            weight_sidecars: vec![SnapshotSidecarEntry::new(
                "weight".to_owned(),
                "weights/weight.f32".to_owned(),
                SnapshotSidecarValueScope::Relationship,
            )],
            feature_sidecars: vec![SnapshotSidecarEntry::new(
                "embedding_input".to_owned(),
                "features/embedding_input.f32".to_owned(),
                SnapshotSidecarValueScope::Node,
            )],
            result_sidecars: vec![SnapshotSidecarEntry::new(
                "community".to_owned(),
                "results/community.u64".to_owned(),
                SnapshotSidecarValueScope::Node,
            )],
        },
        forward_offsets: "forward.offsets.bin".to_owned(),
        forward_peers: "forward.peers.bin".to_owned(),
        reverse_offsets: "reverse.offsets.bin".to_owned(),
        reverse_peers: "reverse.peers.bin".to_owned(),
        node_table: "node_table.bin".to_owned(),
        strings: "strings.bin".to_owned(),
        key_index: "key_index.bin".to_owned(),
    };

    let manifest_json = serde_json::to_string_pretty(&manifest).expect("manifest serializes");
    let round_tripped: SnapshotManifest =
        serde_json::from_str(&manifest_json).expect("manifest parses");
    assert_eq!(round_tripped, manifest);
}

#[test]
fn snapshot_build_summary_reports_topology_sidecar_and_scratch_bytes_now() {
    let (_temp_dir, snapshot_dir) = support::temp_snapshot_dir();
    let build_summary = build_snapshot_from_paths(
        &support::valid_nodes_path(),
        &support::valid_edges_path(),
        &snapshot_dir,
    )
    .expect("snapshot builds");

    assert!(build_summary.topology_bytes > 0);
    assert_eq!(build_summary.sidecar_bytes, 0);
    assert!(build_summary.scratch_bytes > 0);
    assert_eq!(
        build_summary.snapshot_size_bytes,
        build_summary.topology_bytes + build_summary.sidecar_bytes
    );
}
