mod support;

use std::fs;

use knight_bus::{
    CsvTruthGraphSource, KnightBusError, MmapWalkRuntime, QueryFamily, TruthGraphIndex,
    TruthGraphSource, build_snapshot_from_paths, query_snapshot_from_path,
    verify_snapshot_against_paths,
};

#[test]
fn build_query_and_verify_round_trip_now() {
    let (_temp_dir, snapshot_dir) = support::temp_snapshot_dir();

    let build_summary = build_snapshot_from_paths(
        &support::valid_nodes_path(),
        &support::valid_edges_path(),
        &snapshot_dir,
    )
    .expect("snapshot builds");

    assert_eq!(build_summary.node_count, 39);
    assert_eq!(build_summary.edge_count, 67);

    let query_result = query_snapshot_from_path(
        &snapshot_dir,
        "fn:login_user_flow_now",
        knight_bus::WalkDirection::Forward,
        knight_bus::HopCount::Two,
    )
    .expect("query works");

    assert_eq!(
        query_result.neighbors,
        vec![
            "fn:check_password_match_now".to_owned(),
            "fn:clear_profile_cache_now".to_owned(),
            "fn:fetch_user_record_now".to_owned(),
            "fn:issue_login_token_now".to_owned(),
            "fn:read_profile_cache_now".to_owned(),
            "fn:save_session_record_now".to_owned(),
            "fn:write_audit_entry_now".to_owned(),
        ]
    );

    let verification_summary = verify_snapshot_against_paths(
        &snapshot_dir,
        &support::valid_nodes_path(),
        &support::valid_edges_path(),
    )
    .expect("verification works");

    assert!(verification_summary.total_checked_queries > 0);
}

#[test]
fn build_rejects_duplicate_node_ids_now() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let nodes_path = temp_dir.path().join("nodes.csv");
    let edges_path = temp_dir.path().join("edges.csv");
    let snapshot_dir = temp_dir.path().join("snapshot");

    fs::write(
        &nodes_path,
        "node_id,node_type,label,parent_id,file_path,span\nalpha,function,alpha,,,\nalpha,function,alpha,,,\n",
    )
    .expect("nodes written");
    fs::write(
        &edges_path,
        "from_id,edge_type,to_id\nalpha,depends_on,alpha\n",
    )
    .expect("edges written");

    let error = build_snapshot_from_paths(&nodes_path, &edges_path, &snapshot_dir)
        .expect_err("duplicate node ids must fail");

    assert!(matches!(error, KnightBusError::DuplicateNodeId { .. }));
}

#[test]
fn build_rejects_missing_edge_endpoints_now() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let nodes_path = temp_dir.path().join("nodes.csv");
    let edges_path = temp_dir.path().join("edges.csv");
    let snapshot_dir = temp_dir.path().join("snapshot");

    fs::write(
        &nodes_path,
        "node_id,node_type,label,parent_id,file_path,span\nalpha,function,alpha,,,\n",
    )
    .expect("nodes written");
    fs::write(
        &edges_path,
        "from_id,edge_type,to_id\nalpha,depends_on,beta\n",
    )
    .expect("edges written");

    let error = build_snapshot_from_paths(&nodes_path, &edges_path, &snapshot_dir)
        .expect_err("missing edge endpoint must fail");

    assert!(matches!(error, KnightBusError::MissingEdgeEndpoint { .. }));
}

#[test]
fn open_detects_truncated_offsets_now() {
    let (_temp_dir, snapshot_dir) = support::temp_snapshot_dir();
    build_snapshot_from_paths(
        &support::valid_nodes_path(),
        &support::valid_edges_path(),
        &snapshot_dir,
    )
    .expect("snapshot builds");

    let offsets_path = snapshot_dir.join("forward.offsets.bin");
    let file = fs::OpenOptions::new()
        .write(true)
        .open(&offsets_path)
        .expect("offsets file opens");
    file.set_len(8).expect("offsets truncated");

    let error = MmapWalkRuntime::open(&snapshot_dir).expect_err("truncated snapshot must fail");
    assert!(matches!(error, KnightBusError::SnapshotFileSize { .. }));
}

#[test]
fn parity_uses_all_expected_families_now() {
    let truth_rows =
        CsvTruthGraphSource::new(support::valid_nodes_path(), support::valid_edges_path())
            .load_truth_graph_rows()
            .expect("truth rows load");
    let truth_index = TruthGraphIndex::from_truth_graph_rows(&truth_rows);

    for family in QueryFamily::ALL {
        let seeds = truth_index.seed_keys_for_family(family);
        assert!(
            !seeds.is_empty(),
            "expected at least one seed for family {}",
            family.label()
        );
    }
}
