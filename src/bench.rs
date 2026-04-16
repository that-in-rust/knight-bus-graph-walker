use std::time::Instant;

use sysinfo::{Pid, System};

use crate::{
    error::KnightBusError,
    runtime::{MmapWalkRuntime, WalkQueryRuntime},
    types::{BenchmarkFamilyReport, BenchmarkReport, NodeKey, PeakRssSource, QueryFamily},
};

const DEFAULT_SAMPLE_LIMIT: usize = 64;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PeakRssMeasurement {
    bytes: u64,
    source: PeakRssSource,
}

impl Default for SnapshotBenchmarkRunner {
    fn default() -> Self {
        Self {
            sample_limit: DEFAULT_SAMPLE_LIMIT,
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

fn percentile_nanos(latencies_nanos: &[u64], percentile: usize) -> u64 {
    if latencies_nanos.is_empty() {
        return 0;
    }

    let mut sorted_latencies = latencies_nanos.to_vec();
    sorted_latencies.sort_unstable();

    let index = ((sorted_latencies.len() - 1) * percentile) / 100;
    sorted_latencies[index]
}

fn current_process_rss_bytes(system: &mut System, process_id: Pid) -> u64 {
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
    use super::{PeakRssSource, current_process_rss_bytes};
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
}
