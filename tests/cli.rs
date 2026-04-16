mod support;

use std::fs;

use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn build_and_query_json_from_cli_now() {
    let (_temp_dir, snapshot_dir) = support::temp_snapshot_dir();

    Command::cargo_bin("knight-bus")
        .expect("binary exists")
        .args([
            "build",
            "--nodes-csv",
            support::valid_nodes_path().to_str().expect("utf8 path"),
            "--edges-csv",
            support::valid_edges_path().to_str().expect("utf8 path"),
            "--output",
            snapshot_dir.to_str().expect("utf8 path"),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("snapshot:"));

    Command::cargo_bin("knight-bus")
        .expect("binary exists")
        .args([
            "query",
            "--snapshot",
            snapshot_dir.to_str().expect("utf8 path"),
            "--entity",
            "fn:login_user_flow_now",
            "--dir",
            "forward",
            "--hops",
            "2",
            "--format",
            "json",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"entity\": \"fn:login_user_flow_now\"",
        ))
        .stdout(predicate::str::contains("\"neighbors\""));
}

#[test]
fn verify_cli_reports_success_now() {
    let (_temp_dir, snapshot_dir) = support::temp_snapshot_dir();

    Command::cargo_bin("knight-bus")
        .expect("binary exists")
        .args([
            "build",
            "--nodes-csv",
            support::valid_nodes_path().to_str().expect("utf8 path"),
            "--edges-csv",
            support::valid_edges_path().to_str().expect("utf8 path"),
            "--output",
            snapshot_dir.to_str().expect("utf8 path"),
        ])
        .assert()
        .success();

    Command::cargo_bin("knight-bus")
        .expect("binary exists")
        .args([
            "verify",
            "--snapshot",
            snapshot_dir.to_str().expect("utf8 path"),
            "--nodes-csv",
            support::valid_nodes_path().to_str().expect("utf8 path"),
            "--edges-csv",
            support::valid_edges_path().to_str().expect("utf8 path"),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("verification: ok"));
}

#[test]
fn query_rejects_invalid_hops_now() {
    let (_temp_dir, snapshot_dir) = support::temp_snapshot_dir();

    Command::cargo_bin("knight-bus")
        .expect("binary exists")
        .args([
            "build",
            "--nodes-csv",
            support::valid_nodes_path().to_str().expect("utf8 path"),
            "--edges-csv",
            support::valid_edges_path().to_str().expect("utf8 path"),
            "--output",
            snapshot_dir.to_str().expect("utf8 path"),
        ])
        .assert()
        .success();

    Command::cargo_bin("knight-bus")
        .expect("binary exists")
        .args([
            "query",
            "--snapshot",
            snapshot_dir.to_str().expect("utf8 path"),
            "--entity",
            "fn:login_user_flow_now",
            "--dir",
            "forward",
            "--hops",
            "3",
            "--format",
            "text",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid hop count"));
}

#[test]
fn bench_writes_report_now() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let snapshot_dir = temp_dir.path().join("snapshot");
    let report_dir = temp_dir.path().join("report");

    Command::cargo_bin("knight-bus")
        .expect("binary exists")
        .args([
            "build",
            "--nodes-csv",
            support::valid_nodes_path().to_str().expect("utf8 path"),
            "--edges-csv",
            support::valid_edges_path().to_str().expect("utf8 path"),
            "--output",
            snapshot_dir.to_str().expect("utf8 path"),
        ])
        .assert()
        .success();

    Command::cargo_bin("knight-bus")
        .expect("binary exists")
        .args([
            "bench",
            "--snapshot",
            snapshot_dir.to_str().expect("utf8 path"),
            "--report",
            report_dir.to_str().expect("utf8 path"),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("peak_rss_bytes"));

    let report_path = report_dir.join("bench-report.json");
    let report_bytes = fs::read(&report_path).expect("report exists");
    let report_json = String::from_utf8(report_bytes).expect("utf8 report");
    assert!(report_json.contains("\"snapshot_size_bytes\""));
    assert!(report_json.contains("\"peak_rss_bytes\""));
}
