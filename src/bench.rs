use std::time::Instant;

use sysinfo::{Pid, System};

use crate::{
    error::KnightBusError,
    runtime::{MmapWalkRuntime, WalkQueryRuntime},
    types::{BenchmarkFamilyReport, BenchmarkReport, NodeKey, QueryFamily},
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
        let mut peak_rss_bytes = current_process_rss_bytes(&mut system, process_id);
        let mut family_reports = Vec::new();

        for family in QueryFamily::ALL {
            let seed_keys = select_benchmark_seeds(runtime, family, self.sample_limit)?;
            let mut latencies_nanos = Vec::with_capacity(seed_keys.len());

            for seed_key in &seed_keys {
                let started_at = Instant::now();
                let _ = runtime.query_keys_for_family(seed_key, family)?;
                let elapsed = u64::try_from(started_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
                latencies_nanos.push(elapsed);
                peak_rss_bytes =
                    peak_rss_bytes.max(current_process_rss_bytes(&mut system, process_id));
            }

            family_reports.push(BenchmarkFamilyReport {
                family,
                sample_count: latencies_nanos.len(),
                p50_nanos: percentile_nanos(&latencies_nanos, 50),
                p95_nanos: percentile_nanos(&latencies_nanos, 95),
            });
        }

        Ok(BenchmarkReport {
            snapshot_path: runtime.snapshot_dir().to_path_buf(),
            snapshot_size_bytes: runtime.snapshot_size_bytes(),
            peak_rss_bytes,
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
        .map(|process| process.memory() * 1024)
        .unwrap_or(0)
}
