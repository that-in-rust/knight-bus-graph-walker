use std::{
    path::{Path, PathBuf},
    time::Instant,
};

use csv::{ReaderBuilder, StringRecord};
use sysinfo::{Pid, System};

use crate::{
    error::KnightBusError,
    parity::run_corpus_parity_verification,
    runtime::{MmapWalkRuntime, WalkQueryRuntime},
    truth::TruthGraphIndex,
    types::{
        BenchmarkFamilyReport, BenchmarkReport, CorpusFamily, CorpusQueryRow, EngineMeasurement,
        NodeKey, PeakRssSource, QueryFamily,
    },
};

const DEFAULT_SAMPLE_LIMIT: usize = 64;
const DEFAULT_WARMUP_PASSES: usize = 1;
const DEFAULT_MEASURE_PASSES: usize = 3;
const REQUIRED_CORPUS_HEADERS: [&str; 2] = ["family_name", "node_id"];

pub trait BenchmarkScenarioRunner {
    fn run_benchmark_scenarios(
        &self,
        runtime: &MmapWalkRuntime,
    ) -> Result<BenchmarkReport, KnightBusError>;
}

#[derive(Clone, Copy, Debug)]
pub struct SnapshotBenchmarkRunner {
    sample_limit: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct SnapshotCorpusBenchmarkRunner {
    warmup_passes: usize,
    measure_passes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PeakRssMeasurement {
    bytes: u64,
    source: PeakRssSource,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CorpusBenchmarkOutcome {
    pub measurement: EngineMeasurement,
    pub query_corpus_size: usize,
}

impl Default for SnapshotBenchmarkRunner {
    fn default() -> Self {
        Self {
            sample_limit: DEFAULT_SAMPLE_LIMIT,
        }
    }
}

impl Default for SnapshotCorpusBenchmarkRunner {
    fn default() -> Self {
        Self {
            warmup_passes: DEFAULT_WARMUP_PASSES,
            measure_passes: DEFAULT_MEASURE_PASSES,
        }
    }
}

impl BenchmarkScenarioRunner for SnapshotBenchmarkRunner {
    fn run_benchmark_scenarios(
        &self,
        runtime: &MmapWalkRuntime,
    ) -> Result<BenchmarkReport, KnightBusError> {
        let mut system = System::new_all();
        let process_id = Pid::from_u32(std::process::id());
        let mut sampled_peak_rss_bytes = current_process_rss_bytes(&mut system, process_id);
        let mut family_reports = Vec::new();

        for family in QueryFamily::ALL {
            let seed_keys = select_benchmark_seeds(runtime, family, self.sample_limit)?;
            let mut latencies_nanos = Vec::with_capacity(seed_keys.len());

            for seed_key in &seed_keys {
                let started_at = Instant::now();
                let _ = runtime.query_keys_for_family(seed_key, family)?;
                let elapsed = u64::try_from(started_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
                latencies_nanos.push(elapsed);
                sampled_peak_rss_bytes =
                    sampled_peak_rss_bytes.max(current_process_rss_bytes(&mut system, process_id));
            }

            family_reports.push(BenchmarkFamilyReport {
                family,
                sample_count: latencies_nanos.len(),
                p50_nanos: percentile_nanos(&latencies_nanos, 50),
                p95_nanos: percentile_nanos(&latencies_nanos, 95),
            });
        }

        let peak_rss_measurement = peak_rss_measurement_now(sampled_peak_rss_bytes);

        Ok(BenchmarkReport {
            snapshot_path: runtime.snapshot_dir().to_path_buf(),
            snapshot_size_bytes: runtime.snapshot_size_bytes(),
            peak_rss_bytes: peak_rss_measurement.bytes,
            peak_rss_source: peak_rss_measurement.source,
            families: family_reports,
        })
    }
}

impl SnapshotCorpusBenchmarkRunner {
    pub fn run_corpus_benchmark(
        &self,
        runtime: &MmapWalkRuntime,
        truth_index: &TruthGraphIndex,
        corpus_path: &Path,
        open_start_ms: f64,
    ) -> Result<CorpusBenchmarkOutcome, KnightBusError> {
        let corpus_rows = load_corpus_query_rows(corpus_path)?;
        run_corpus_parity_verification(truth_index, runtime, &corpus_rows)?;

        let mut system = System::new_all();
        let process_id = Pid::from_u32(std::process::id());
        let mut sampled_peak_rss_bytes = current_process_rss_bytes(&mut system, process_id);
        let mut latency_samples_ms = Vec::with_capacity(corpus_rows.len() * self.measure_passes);

        for _ in 0..self.warmup_passes {
            for row in &corpus_rows {
                let _ = query_runtime_for_corpus_row(runtime, row)?;
            }
        }

        for _ in 0..self.measure_passes {
            for row in &corpus_rows {
                let started_at = Instant::now();
                let _ = query_runtime_for_corpus_row(runtime, row)?;
                latency_samples_ms.push(started_at.elapsed().as_secs_f64() * 1_000.0);
                sampled_peak_rss_bytes =
                    sampled_peak_rss_bytes.max(current_process_rss_bytes(&mut system, process_id));
            }
        }

        let peak_rss_measurement = peak_rss_measurement_now(sampled_peak_rss_bytes);

        Ok(CorpusBenchmarkOutcome {
            measurement: EngineMeasurement {
                engine_name: "knight_bus_rust".to_owned(),
                status: "ok".to_owned(),
                reason: None,
                open_start_ms: Some(open_start_ms),
                operation_count: latency_samples_ms.len(),
                mean_ms: mean_value_ms(&latency_samples_ms),
                p50_ms: percentile_value_ms(&latency_samples_ms, 0.50),
                p95_ms: percentile_value_ms(&latency_samples_ms, 0.95),
                p99_ms: percentile_value_ms(&latency_samples_ms, 0.99),
                rss_bytes: Some(peak_rss_measurement.bytes),
                version: Some(format!("snapshot-v{}", runtime.manifest_version())),
                cold_run: false,
            },
            query_corpus_size: corpus_rows.len(),
        })
    }
}

fn select_benchmark_seeds(
    runtime: &MmapWalkRuntime,
    family: QueryFamily,
    sample_limit: usize,
) -> Result<Vec<NodeKey>, KnightBusError> {
    let mut seed_keys = Vec::new();

    for node_key in runtime.all_node_keys()? {
        if !runtime.query_keys_for_family(&node_key, family)?.is_empty() {
            seed_keys.push(node_key);
        }
        if seed_keys.len() >= sample_limit {
            break;
        }
    }

    Ok(seed_keys)
}

fn query_runtime_for_corpus_row(
    runtime: &MmapWalkRuntime,
    row: &CorpusQueryRow,
) -> Result<Vec<String>, KnightBusError> {
    Ok(runtime
        .query_entity_neighbors(&row.node_id, row.family.direction(), row.family.hops())?
        .neighbors)
}

fn load_corpus_query_rows(corpus_path: &Path) -> Result<Vec<CorpusQueryRow>, KnightBusError> {
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_path(corpus_path)
        .map_err(|source| KnightBusError::csv(corpus_path, source))?;
    let header_positions = resolve_corpus_header_positions(
        corpus_path,
        reader
            .headers()
            .map_err(|source| KnightBusError::csv(corpus_path, source))?,
    )?;

    let mut rows = Vec::new();

    for (row_index, record_result) in reader.records().enumerate() {
        let record = record_result.map_err(|source| KnightBusError::csv(corpus_path, source))?;
        let display_row_index = row_index + 2;

        let family = read_required_corpus_field(
            &record,
            header_positions["family_name"],
            corpus_path,
            display_row_index,
            "family_name",
        )?
        .parse::<CorpusFamily>()?;
        let node_id = NodeKey::parse_csv_field(
            read_required_corpus_field(
                &record,
                header_positions["node_id"],
                corpus_path,
                display_row_index,
                "node_id",
            )?,
            corpus_path,
            display_row_index,
            "node_id",
        )?;

        rows.push(CorpusQueryRow { family, node_id });
    }

    Ok(rows)
}

fn resolve_corpus_header_positions(
    corpus_path: &Path,
    header_record: &StringRecord,
) -> Result<std::collections::BTreeMap<&'static str, usize>, KnightBusError> {
    let mut positions = std::collections::BTreeMap::new();

    for required_header in REQUIRED_CORPUS_HEADERS {
        let position = header_record
            .iter()
            .position(|header| header == required_header)
            .ok_or_else(|| KnightBusError::MissingRequiredHeader {
                path: corpus_path.to_path_buf(),
                csv_kind: "corpus",
                header: required_header,
            })?;
        positions.insert(required_header, position);
    }

    Ok(positions)
}

fn read_required_corpus_field<'a>(
    record: &'a StringRecord,
    field_index: usize,
    corpus_path: &Path,
    row_index: usize,
    column: &'static str,
) -> Result<&'a str, KnightBusError> {
    record
        .get(field_index)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| KnightBusError::MissingRequiredField {
            path: PathBuf::from(corpus_path),
            row_index,
            column,
        })
}

fn mean_value_ms(samples: &[f64]) -> Option<f64> {
    (!samples.is_empty()).then(|| samples.iter().sum::<f64>() / samples.len() as f64)
}

fn percentile_value_ms(samples: &[f64], quantile: f64) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }

    let mut ordered = samples.to_vec();
    ordered.sort_by(|left, right| left.total_cmp(right));

    if ordered.len() == 1 {
        return ordered.first().copied();
    }

    let rank = (ordered.len() - 1) as f64 * quantile;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;

    if lower == upper {
        return ordered.get(lower).copied();
    }

    let fraction = rank - lower as f64;
    Some(ordered[lower] + ((ordered[upper] - ordered[lower]) * fraction))
}

fn percentile_nanos(latencies_nanos: &[u64], percentile: usize) -> u64 {
    if latencies_nanos.is_empty() {
        return 0;
    }

    let mut sorted_latencies = latencies_nanos.to_vec();
    sorted_latencies.sort_unstable();

    let index = ((sorted_latencies.len() - 1) * percentile) / 100;
    sorted_latencies[index]
}

pub(crate) fn current_process_rss_bytes(system: &mut System, process_id: Pid) -> u64 {
    system.refresh_process(process_id);
    system
        .process(process_id)
        .map(|process| process.memory())
        .unwrap_or(0)
}

fn peak_rss_measurement_now(sampled_peak_rss_bytes: u64) -> PeakRssMeasurement {
    match peak_rss_bytes_from_getrusage_now() {
        Some(os_peak_rss_bytes) if os_peak_rss_bytes > 0 => PeakRssMeasurement {
            bytes: os_peak_rss_bytes.max(sampled_peak_rss_bytes),
            source: PeakRssSource::GetrusageSelf,
        },
        _ => PeakRssMeasurement {
            bytes: sampled_peak_rss_bytes,
            source: PeakRssSource::SampledCurrentRssBytes,
        },
    }
}

#[cfg(unix)]
fn peak_rss_bytes_from_getrusage_now() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return None;
    }
    let usage = unsafe { usage.assume_init() };
    Some(normalize_ru_maxrss_bytes_now(usage.ru_maxrss))
}

#[cfg(not(unix))]
fn peak_rss_bytes_from_getrusage_now() -> Option<u64> {
    None
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn normalize_ru_maxrss_bytes_now(ru_maxrss: libc::c_long) -> u64 {
    u64::try_from(ru_maxrss.max(0))
        .unwrap_or(0)
        .saturating_mul(1024)
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn normalize_ru_maxrss_bytes_now(ru_maxrss: libc::c_long) -> u64 {
    u64::try_from(ru_maxrss.max(0)).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{
        CorpusFamily, SnapshotCorpusBenchmarkRunner, current_process_rss_bytes, percentile_value_ms,
    };
    use crate::{
        CsvTruthGraphSource, PeakRssSource, TruthGraphIndex, TruthGraphSource,
        build_snapshot_from_paths,
    };
    use sysinfo::{Pid, System};

    #[test]
    fn current_process_rss_bytes_uses_raw_sysinfo_units_now() {
        let mut system = System::new_all();
        let process_id = Pid::from_u32(std::process::id());
        system.refresh_process(process_id);
        let baseline_bytes = system
            .process(process_id)
            .map(|process| process.memory())
            .unwrap_or(0);

        let measured_bytes = current_process_rss_bytes(&mut system, process_id);
        let allowed_drift_bytes = 128 * 1024 * 1024;

        assert!(
            baseline_bytes.abs_diff(measured_bytes) <= allowed_drift_bytes,
            "expected sampled RSS to stay in sysinfo byte units: baseline={baseline_bytes} measured={measured_bytes}"
        );
    }

    #[test]
    fn peak_rss_source_serializes_now() {
        let serialized = serde_json::to_string(&PeakRssSource::GetrusageSelf).expect("json");
        assert_eq!(serialized, "\"getrusage_self\"");
    }

    #[test]
    fn corpus_family_maps_to_runtime_semantics_now() {
        assert_eq!(CorpusFamily::ForwardOne.direction().label(), "forward");
        assert_eq!(CorpusFamily::ReverseOne.direction().label(), "backward");
        assert_eq!(CorpusFamily::ReverseTwo.direction().label(), "backward");
        assert_eq!(CorpusFamily::ForwardOne.hops().get(), 1);
        assert_eq!(CorpusFamily::ReverseOne.hops().get(), 1);
        assert_eq!(CorpusFamily::ReverseTwo.hops().get(), 2);
    }

    #[test]
    fn percentile_value_ms_interpolates_now() {
        let samples = [1.0, 2.0, 10.0, 100.0];
        let value = percentile_value_ms(&samples, 0.95).expect("percentile");
        assert!(value > 80.0);
        assert!(value < 100.0);
    }

    #[test]
    fn corpus_benchmark_runs_against_tiny_fixture_now() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let snapshot_dir = temp_dir.path().join("snapshot");
        let nodes_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("valid")
            .join("interface_nodes.csv");
        let edges_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("valid")
            .join("interface_edges.csv");
        let corpus_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("valid")
            .join("corpus_query.csv");

        build_snapshot_from_paths(&nodes_path, &edges_path, &snapshot_dir)
            .expect("snapshot builds");
        let truth_rows = CsvTruthGraphSource::new(&nodes_path, &edges_path)
            .load_truth_graph_rows()
            .expect("truth rows load");
        let truth_index = TruthGraphIndex::from_truth_graph_rows(&truth_rows);
        let runtime = crate::MmapWalkRuntime::open(&snapshot_dir).expect("runtime opens");
        let outcome = SnapshotCorpusBenchmarkRunner::default()
            .run_corpus_benchmark(&runtime, &truth_index, &corpus_path, 1.0)
            .expect("corpus benchmark works");

        assert_eq!(outcome.measurement.engine_name, "knight_bus_rust");
        assert_eq!(outcome.measurement.status, "ok");
        assert!(outcome.measurement.operation_count > 0);
        assert!(outcome.measurement.p50_ms.is_some());
        assert!(outcome.measurement.p95_ms.is_some());
        assert!(outcome.measurement.p99_ms.is_some());
        assert!(outcome.measurement.rss_bytes.is_some());
        assert!(outcome.query_corpus_size > 0);
    }
}
