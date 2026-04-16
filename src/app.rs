use std::{fs, path::Path};

use crate::{
    bench::{BenchmarkScenarioRunner, SnapshotBenchmarkRunner},
    error::KnightBusError,
    graph::normalize_truth_graph_data,
    parity::run_parity_verification,
    runtime::{MmapWalkRuntime, WalkQueryRuntime},
    snapshot::{FilesystemSnapshotWriter, SnapshotArtifactWriter},
    truth::{CsvTruthGraphSource, TruthGraphIndex, TruthGraphSource},
    types::{
        BenchmarkRunSummary, HopCount, NodeKey, QueryResult, SnapshotBuildSummary,
        VerificationSummary, WalkDirection,
    },
};

pub const BENCH_REPORT_FILE_NAME: &str = "bench-report.json";

pub fn build_snapshot_from_paths(
    nodes_path: &Path,
    edges_path: &Path,
    output_dir: &Path,
) -> Result<SnapshotBuildSummary, KnightBusError> {
    let truth_source = CsvTruthGraphSource::new(nodes_path, edges_path);
    let truth_graph = truth_source.load_truth_graph_rows()?;
    let normalized_graph = normalize_truth_graph_data(&truth_graph)?;
    FilesystemSnapshotWriter.write_snapshot_artifacts(&normalized_graph, output_dir)
}

pub fn verify_snapshot_against_paths(
    snapshot_dir: &Path,
    nodes_path: &Path,
    edges_path: &Path,
) -> Result<VerificationSummary, KnightBusError> {
    let truth_source = CsvTruthGraphSource::new(nodes_path, edges_path);
    let truth_graph = truth_source.load_truth_graph_rows()?;
    let truth_index = TruthGraphIndex::from_truth_graph_rows(&truth_graph);
    let runtime = MmapWalkRuntime::open(snapshot_dir)?;
    run_parity_verification(&truth_index, &runtime)
}

pub fn query_snapshot_from_path(
    snapshot_dir: &Path,
    entity: &str,
    direction: WalkDirection,
    hops: HopCount,
) -> Result<QueryResult, KnightBusError> {
    let runtime = MmapWalkRuntime::open(snapshot_dir)?;
    let entity_key = NodeKey::try_from(entity.to_owned())?;
    runtime.query_entity_neighbors(&entity_key, direction, hops)
}

pub fn run_snapshot_benchmark(
    snapshot_dir: &Path,
    report_dir: &Path,
) -> Result<BenchmarkRunSummary, KnightBusError> {
    fs::create_dir_all(report_dir).map_err(|source| KnightBusError::io(report_dir, source))?;
    let runtime = MmapWalkRuntime::open(snapshot_dir)?;
    let benchmark_report = SnapshotBenchmarkRunner::default().run_benchmark_scenarios(&runtime)?;
    let report_path = report_dir.join(BENCH_REPORT_FILE_NAME);
    let report_bytes = serde_json::to_vec_pretty(&benchmark_report)
        .map_err(|source| KnightBusError::json(&report_path, source))?;
    fs::write(&report_path, report_bytes)
        .map_err(|source| KnightBusError::io(&report_path, source))?;

    Ok(BenchmarkRunSummary {
        report_path,
        report: benchmark_report,
    })
}
