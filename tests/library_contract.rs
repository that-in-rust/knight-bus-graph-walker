mod support;

use std::fs;

use knight_bus::{
    CsvTruthGraphSource, KnightBusError, MmapWalkRuntime, QueryFamily, SnapshotPhase,
    TruthGraphIndex, TruthGraphSource, build_snapshot_from_paths, query_snapshot_from_path,
    run_corpus_benchmark_from_paths, run_snapshot_benchmark, verify_snapshot_against_paths,
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

#[test]
fn benchmark_report_records_peak_rss_source_now() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let snapshot_dir = temp_dir.path().join("snapshot");
    let report_dir = temp_dir.path().join("report");

    build_snapshot_from_paths(
        &support::valid_nodes_path(),
        &support::valid_edges_path(),
        &snapshot_dir,
    )
    .expect("snapshot builds");

    let benchmark_summary =
        run_snapshot_benchmark(&snapshot_dir, &report_dir).expect("benchmark works");

    assert!(benchmark_summary.report.peak_rss_bytes > 0);
    let report_json =
        serde_json::to_string(&benchmark_summary.report).expect("report serializes as json");
    assert!(report_json.contains("\"peak_rss_source\""));
}

#[test]
fn low_ram_build_and_verify_record_phase_peaks_now() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let snapshot_dir = temp_dir.path().join("snapshot");

    let build_summary = build_snapshot_from_paths(
        &support::valid_nodes_path(),
        &support::valid_edges_path(),
        &snapshot_dir,
    )
    .expect("snapshot builds");
    assert!(build_summary.peak_rss_bytes > 0);
    assert!(
        build_summary
            .phase_peaks
            .iter()
            .any(|phase_peak| phase_peak.phase == SnapshotPhase::BuildNodeRuns)
    );
    assert!(
        build_summary
            .phase_peaks
            .iter()
            .any(|phase_peak| phase_peak.phase == SnapshotPhase::EmitForwardCsr)
    );

    let verification_summary = verify_snapshot_against_paths(
        &snapshot_dir,
        &support::valid_nodes_path(),
        &support::valid_edges_path(),
    )
    .expect("verification works");
    assert!(verification_summary.checked_nodes > 0);
    assert!(verification_summary.checked_forward_edges > 0);
    assert!(verification_summary.checked_reverse_edges > 0);
    assert!(verification_summary.peak_rss_bytes > 0);
    assert!(
        verification_summary
            .phase_peaks
            .iter()
            .any(|phase_peak| phase_peak.phase == SnapshotPhase::VerifyForwardCsr)
    );
    assert!(
        verification_summary
            .phase_peaks
            .iter()
            .any(|phase_peak| phase_peak.phase == SnapshotPhase::QuerySmokeChecks)
    );
}

#[test]
fn corpus_benchmark_report_serializes_engine_measurement_now() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let snapshot_dir = temp_dir.path().join("snapshot");
    let report_path = temp_dir.path().join("corpus-report.json");

    build_snapshot_from_paths(
        &support::valid_nodes_path(),
        &support::valid_edges_path(),
        &snapshot_dir,
    )
    .expect("snapshot builds");

    let benchmark_summary = run_corpus_benchmark_from_paths(
        &snapshot_dir,
        &support::valid_nodes_path(),
        &support::valid_edges_path(),
        &support::valid_corpus_path(),
        &report_path,
    )
    .expect("corpus benchmark works");

    assert_eq!(benchmark_summary.measurement.engine_name, "knight_bus_rust");
    assert_eq!(benchmark_summary.measurement.status, "ok");
    assert_eq!(benchmark_summary.query_corpus_size, 3);
    assert_eq!(
        benchmark_summary.measurement.rss_scope,
        knight_bus::MeasurementRssScope::RuntimeProcessOnly
    );
    let report_json = fs::read_to_string(&report_path).expect("report file");
    assert!(report_json.contains("\"engine_name\": \"knight_bus_rust\""));
    assert!(report_json.contains("\"operation_count\""));
    assert!(report_json.contains("\"p99_ms\""));
    assert!(report_json.contains("\"rss_bytes\""));
    assert!(report_json.contains("\"rss_scope\": \"runtime_process_only\""));
    assert!(report_json.contains("\"rss_source\""));
}
