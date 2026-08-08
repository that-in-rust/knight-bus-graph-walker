use std::fs;

use knight_bus::{
    MmapWalkRuntime,
    bolt::{BoltCompatibilityStartupError, KnightBusBoltBackend},
    build_snapshot_from_paths,
};
use serde_json::json;
use tempfile::TempDir;

fn build_profile_snapshot_now() -> (TempDir, std::path::PathBuf) {
    let temp_dir = TempDir::new().expect("temporary graph directory");
    let nodes_path = temp_dir.path().join("nodes.csv");
    let edges_path = temp_dir.path().join("edges.csv");
    let snapshot_path = temp_dir.path().join("snapshot");
    fs::write(
        &nodes_path,
        "node_id,node_type,label,parent_id,file_path,span\nA,function,A,,A,\nB,function,B,,B,\n",
    )
    .expect("nodes fixture writes");
    fs::write(&edges_path, "from_id,edge_type,to_id\nA,DEPENDS_ON,B\n")
        .expect("edges fixture writes");
    build_snapshot_from_paths(&nodes_path, &edges_path, &snapshot_path)
        .expect("profile snapshot builds");
    (temp_dir, snapshot_path)
}

fn write_graph_profile_now(snapshot_path: &std::path::Path, relationship_type: &str) {
    let profile = json!({
        "schema_version": 1,
        "profile_version": "knight-bus-neighborhood-walk-v1",
        "node_label": "Entity",
        "start_node_id_property": "node_id",
        "result_node_id_property": "node_id",
        "relationship_type": relationship_type,
        "minimum_hops": 1,
        "maximum_hops": 2,
        "node_count": 2,
        "relationship_count": 1,
    });
    fs::write(
        snapshot_path.join("compatibility-profile.json"),
        serde_json::to_vec_pretty(&profile).expect("profile serializes"),
    )
    .expect("profile fixture writes");
}

#[test]
fn requires_exact_graph_profile_before_startup() {
    let (_temp_dir, snapshot_path) = build_profile_snapshot_now();

    let missing_runtime = MmapWalkRuntime::open(&snapshot_path).expect("snapshot opens");
    let missing = match KnightBusBoltBackend::new(missing_runtime) {
        Ok(_) => panic!("missing profile must fail startup"),
        Err(error) => error,
    };
    assert!(matches!(missing, BoltCompatibilityStartupError::Io { .. }));

    write_graph_profile_now(&snapshot_path, "KNOWS");
    let invalid_runtime = MmapWalkRuntime::open(&snapshot_path).expect("snapshot reopens");
    let invalid = match KnightBusBoltBackend::new(invalid_runtime) {
        Ok(_) => panic!("mismatched relationship type must fail startup"),
        Err(error) => error,
    };
    assert!(matches!(
        invalid,
        BoltCompatibilityStartupError::InvalidGraphProfile { .. }
    ));

    write_graph_profile_now(&snapshot_path, "DEPENDS_ON");
    let valid_runtime = MmapWalkRuntime::open(&snapshot_path).expect("snapshot reopens again");
    assert!(KnightBusBoltBackend::new(valid_runtime).is_ok());
}
