use std::{fs, path::Path, time::Instant};

use crate::{
    bench::{BenchmarkScenarioRunner, SnapshotBenchmarkRunner, SnapshotCorpusBenchmarkRunner},
    error::KnightBusError,
    low_ram::{build_snapshot_from_paths_low_ram, verify_snapshot_against_paths_low_ram},
    runtime::{MmapWalkRuntime, WalkQueryRuntime},
    truth::{CsvTruthGraphSource, TruthGraphIndex, TruthGraphSource},
    types::{
        BenchmarkRunSummary, CorpusBenchmarkRunSummary, HopCount, NodeKey, QueryResult,
        SnapshotBuildOptions, SnapshotBuildSummary, SnapshotVerifyOptions, VerificationSummary,
        WalkDirection,
    },
};

pub const BENCH_REPORT_FILE_NAME: &str = "bench-report.json";

pub fn build_snapshot_from_paths(
    nodes_path: &Path,
    edges_path: &Path,
    output_dir: &Path,
) -> Result<SnapshotBuildSummary, KnightBusError> {
    build_snapshot_from_paths_with_options(
        nodes_path,
        edges_path,
        output_dir,
        &SnapshotBuildOptions::default(),
    )
}

pub fn build_snapshot_from_paths_with_options(
    nodes_path: &Path,
    edges_path: &Path,
    output_dir: &Path,
    options: &SnapshotBuildOptions,
) -> Result<SnapshotBuildSummary, KnightBusError> {
    build_snapshot_from_paths_low_ram(nodes_path, edges_path, output_dir, options)
}

pub fn verify_snapshot_against_paths(
    snapshot_dir: &Path,
    nodes_path: &Path,
    edges_path: &Path,
) -> Result<VerificationSummary, KnightBusError> {
    verify_snapshot_against_paths_with_options(
        snapshot_dir,
        nodes_path,
        edges_path,
        &SnapshotVerifyOptions::default(),
    )
}

pub fn verify_snapshot_against_paths_with_options(
    snapshot_dir: &Path,
    nodes_path: &Path,
    edges_path: &Path,
    options: &SnapshotVerifyOptions,
) -> Result<VerificationSummary, KnightBusError> {
    verify_snapshot_against_paths_low_ram(snapshot_dir, nodes_path, edges_path, options)
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

pub fn run_corpus_benchmark_from_paths(
    snapshot_dir: &Path,
    nodes_path: &Path,
    edges_path: &Path,
    corpus_path: &Path,
    report_path: &Path,
) -> Result<CorpusBenchmarkRunSummary, KnightBusError> {
    if let Some(parent_dir) = report_path.parent() {
        fs::create_dir_all(parent_dir).map_err(|source| KnightBusError::io(parent_dir, source))?;
    }

    let truth_source = CsvTruthGraphSource::new(nodes_path, edges_path);
    let truth_graph = truth_source.load_truth_graph_rows()?;
    let truth_index = TruthGraphIndex::from_truth_graph_rows(&truth_graph);

    let started_at = Instant::now();
    let runtime = MmapWalkRuntime::open(snapshot_dir)?;
    let open_start_ms = started_at.elapsed().as_secs_f64() * 1_000.0;

    let outcome = SnapshotCorpusBenchmarkRunner::default().run_corpus_benchmark(
        &runtime,
        &truth_index,
        corpus_path,
        open_start_ms,
    )?;
    let report_bytes = serde_json::to_vec_pretty(&outcome.measurement)
        .map_err(|source| KnightBusError::json(report_path, source))?;
    fs::write(report_path, report_bytes)
        .map_err(|source| KnightBusError::io(report_path, source))?;

    Ok(CorpusBenchmarkRunSummary {
        report_path: report_path.to_path_buf(),
        measurement: outcome.measurement,
        query_corpus_size: outcome.query_corpus_size,
    })
}
