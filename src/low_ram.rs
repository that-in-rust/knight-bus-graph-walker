use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
};

use csv::{ReaderBuilder, StringRecord};
use memmap2::Mmap;
use sysinfo::{Pid, System};
use tempfile::{Builder as TempDirBuilder, TempDir};

use crate::{
    error::KnightBusError,
    runtime::{MmapWalkRuntime, WalkQueryRuntime},
    snapshot::{
        FORWARD_OFFSETS_FILE_NAME, FORWARD_PEERS_FILE_NAME, KEY_INDEX_FILE_NAME,
        MANIFEST_FILE_NAME, NODE_TABLE_FILE_NAME, REVERSE_OFFSETS_FILE_NAME,
        REVERSE_PEERS_FILE_NAME, STRINGS_FILE_NAME, compute_snapshot_size_bytes,
    },
    types::{
        BuildMemoryBudget, NodeKey, NodeRecord, PeakRssSource, PhasePeakReport, QueryFamily,
        SnapshotBuildOptions, SnapshotBuildSummary, SnapshotManifest, SnapshotPhase,
        SnapshotVerifyOptions, VerificationFamilySummary, VerificationSummary, WalkDirection,
    },
};

const REQUIRED_NODE_HEADERS: [&str; 6] = [
    "node_id",
    "node_type",
    "label",
    "parent_id",
    "file_path",
    "span",
];
const REQUIRED_EDGE_HEADERS: [&str; 3] = ["from_id", "edge_type", "to_id"];
const RUN_FILE_PREFIX: &str = "run";
const SAMPLE_INTERVAL_ROWS: usize = 1024;
const SMOKE_QUERY_LIMIT: usize = 4;

pub fn build_snapshot_from_paths_low_ram(
    nodes_path: &Path,
    edges_path: &Path,
    output_dir: &Path,
    options: &SnapshotBuildOptions,
) -> Result<SnapshotBuildSummary, KnightBusError> {
    fs::create_dir_all(output_dir).map_err(|source| KnightBusError::io(output_dir, source))?;
    let memory_budget = options.resolved_budget();
    let scratch_workspace = ScratchWorkspace::create_now(options.scratch_dir.as_deref())?;
    let mut peak_tracker = PhasePeakTracker::new_now();

    let node_run_paths = build_node_key_runs_now(
        nodes_path,
        scratch_workspace.root(),
        memory_budget,
        &mut peak_tracker,
    )?;
    let node_catalog =
        write_node_catalog_now(nodes_path, output_dir, &node_run_paths, &mut peak_tracker)?;
    let edge_run_paths = build_edge_source_runs_now(
        edges_path,
        scratch_workspace.root(),
        memory_budget,
        &mut peak_tracker,
    )?;
    let to_resolution_runs = resolve_from_keys_now(
        edges_path,
        output_dir,
        node_catalog.node_count,
        &edge_run_paths,
        scratch_workspace.root(),
        memory_budget,
        &mut peak_tracker,
    )?;
    let resolved_edge_runs = resolve_to_keys_now(
        edges_path,
        output_dir,
        node_catalog.node_count,
        &to_resolution_runs,
        scratch_workspace.root(),
        memory_budget,
        &mut peak_tracker,
    )?;
    let unique_edge_count = emit_forward_snapshot_now(
        output_dir,
        node_catalog.node_count,
        &resolved_edge_runs,
        scratch_workspace.root(),
        memory_budget,
        &mut peak_tracker,
    )?;
    emit_reverse_snapshot_now(
        output_dir,
        node_catalog.node_count,
        scratch_workspace.root(),
        &mut peak_tracker,
    )?;
    write_snapshot_manifest_now(output_dir, node_catalog.node_count, unique_edge_count)?;
    let manifest = read_snapshot_manifest_now(output_dir)?;
    let snapshot_size_bytes = compute_snapshot_size_bytes(output_dir, &manifest)?;
    let peak_rss_measurement = peak_tracker.finish_total_now();

    Ok(SnapshotBuildSummary {
        output_dir: output_dir.to_path_buf(),
        node_count: node_catalog.node_count,
        edge_count: unique_edge_count,
        snapshot_size_bytes,
        peak_rss_bytes: peak_rss_measurement.bytes,
        peak_rss_source: peak_rss_measurement.source,
        phase_peaks: peak_tracker.phase_peaks,
    })
}

pub fn verify_snapshot_against_paths_low_ram(
    snapshot_dir: &Path,
    nodes_path: &Path,
    edges_path: &Path,
    options: &SnapshotVerifyOptions,
) -> Result<VerificationSummary, KnightBusError> {
    let memory_budget = options.resolved_budget();
    let scratch_workspace = ScratchWorkspace::create_now(options.scratch_dir.as_deref())?;
    let mut peak_tracker = PhasePeakTracker::new_now();
    let runtime = validate_runtime_open_now(snapshot_dir, &mut peak_tracker)?;

    let node_run_paths = build_node_key_runs_now(
        nodes_path,
        scratch_workspace.root(),
        memory_budget,
        &mut peak_tracker,
    )?;
    let checked_nodes = verify_node_catalog_now(
        snapshot_dir,
        runtime.node_count(),
        &node_run_paths,
        &mut peak_tracker,
    )?;
    let edge_run_paths = build_edge_source_runs_now(
        edges_path,
        scratch_workspace.root(),
        memory_budget,
        &mut peak_tracker,
    )?;
    let to_resolution_runs = resolve_from_keys_now(
        edges_path,
        snapshot_dir,
        runtime.node_count(),
        &edge_run_paths,
        scratch_workspace.root(),
        memory_budget,
        &mut peak_tracker,
    )?;
    let resolved_edge_runs = resolve_to_keys_now(
        edges_path,
        snapshot_dir,
        runtime.node_count(),
        &to_resolution_runs,
        scratch_workspace.root(),
        memory_budget,
        &mut peak_tracker,
    )?;
    let checked_forward_edges = verify_forward_snapshot_now(
        snapshot_dir,
        runtime.node_count(),
        &resolved_edge_runs,
        scratch_workspace.root(),
        memory_budget,
        &mut peak_tracker,
    )?;
    let checked_reverse_edges = verify_reverse_snapshot_now(
        snapshot_dir,
        runtime.node_count(),
        scratch_workspace.root(),
        &mut peak_tracker,
    )?;
    let smoke_summary = run_query_smoke_checks_now(snapshot_dir, &mut peak_tracker)?;
    let peak_rss_measurement = peak_tracker.finish_total_now();

    Ok(VerificationSummary {
        total_checked_queries: smoke_summary
            .iter()
            .map(|family_summary| family_summary.checked_queries)
            .sum(),
        families: smoke_summary,
        checked_nodes,
        checked_forward_edges,
        checked_reverse_edges,
        peak_rss_bytes: peak_rss_measurement.bytes,
        peak_rss_source: peak_rss_measurement.source,
        phase_peaks: peak_tracker.phase_peaks,
    })
}

struct ScratchWorkspace {
    _temp_dir: TempDir,
    root: PathBuf,
}

impl ScratchWorkspace {
    fn create_now(scratch_dir: Option<&Path>) -> Result<Self, KnightBusError> {
        let temp_dir = match scratch_dir {
            Some(path) => {
                fs::create_dir_all(path).map_err(|source| KnightBusError::io(path, source))?;
                TempDirBuilder::new()
                    .prefix("knight-bus-low-ram-")
                    .tempdir_in(path)
                    .map_err(|source| KnightBusError::io(path, source))?
            }
            None => TempDirBuilder::new()
                .prefix("knight-bus-low-ram-")
                .tempdir()
                .map_err(|source| KnightBusError::io(std::env::temp_dir(), source))?,
        };
        let root = temp_dir.path().to_path_buf();
        Ok(Self {
            _temp_dir: temp_dir,
            root,
        })
    }

    fn root(&self) -> &Path {
        &self.root
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PeakRssMeasurement {
    bytes: u64,
    source: PeakRssSource,
}

struct PhasePeakTracker {
    system: System,
    process_id: Pid,
    sampled_peak_rss_bytes: u64,
    phase_peaks: Vec<PhasePeakReport>,
}

impl PhasePeakTracker {
    fn new_now() -> Self {
        let mut system = System::new_all();
        let process_id = Pid::from_u32(std::process::id());
        let sampled_peak_rss_bytes = current_process_rss_bytes_now(&mut system, process_id);
        Self {
            system,
            process_id,
            sampled_peak_rss_bytes,
            phase_peaks: Vec::new(),
        }
    }

    fn sample_now(&mut self) -> u64 {
        let current_rss_bytes = current_process_rss_bytes_now(&mut self.system, self.process_id);
        self.sampled_peak_rss_bytes = self.sampled_peak_rss_bytes.max(current_rss_bytes);
        current_rss_bytes
    }

    fn finish_phase_now(&mut self, phase: SnapshotPhase, phase_peak_rss_bytes: u64) {
        self.phase_peaks.push(PhasePeakReport {
            phase,
            peak_rss_bytes: phase_peak_rss_bytes,
            peak_rss_source: PeakRssSource::SampledCurrentRssBytes,
        });
    }

    fn finish_total_now(&self) -> PeakRssMeasurement {
        peak_rss_measurement_now(self.sampled_peak_rss_bytes)
    }
}

fn track_phase_now<T, F>(
    peak_tracker: &mut PhasePeakTracker,
    phase: SnapshotPhase,
    operation: F,
) -> Result<T, KnightBusError>
where
    F: FnOnce(&mut PhasePeakTracker, &mut u64) -> Result<T, KnightBusError>,
{
    let mut phase_peak_rss_bytes = peak_tracker.sample_now();
    let result = operation(peak_tracker, &mut phase_peak_rss_bytes)?;
    peak_tracker.finish_phase_now(phase, phase_peak_rss_bytes);
    Ok(result)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NodeCatalogSummary {
    node_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct NodeRunEntry {
    key: String,
    row_index: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EdgeSourceRunEntry {
    from_key: String,
    row_index: u64,
    to_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EdgeTargetRunEntry {
    to_key: String,
    row_index: u64,
    from_dense: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ResolvedEdgePair {
    from_dense: u32,
    to_dense: u32,
}

trait BinaryRunRecord: Ord + Sized {
    fn encode_to_now<W: Write>(&self, writer: &mut W, path: &Path) -> Result<(), KnightBusError>;
    fn decode_from_now<R: Read>(
        reader: &mut R,
        path: &Path,
    ) -> Result<Option<Self>, KnightBusError>;
    fn estimated_heap_bytes(&self) -> usize;
}

impl BinaryRunRecord for NodeRunEntry {
    fn encode_to_now<W: Write>(&self, writer: &mut W, path: &Path) -> Result<(), KnightBusError> {
        write_u64_le_now(writer, self.row_index, path)?;
        write_string_field_now(writer, &self.key, path)
    }

    fn decode_from_now<R: Read>(
        reader: &mut R,
        path: &Path,
    ) -> Result<Option<Self>, KnightBusError> {
        let Some(row_index) = read_optional_u64_le_now(reader, path)? else {
            return Ok(None);
        };
        let key = read_string_field_now(reader, path)?;
        Ok(Some(Self { key, row_index }))
    }

    fn estimated_heap_bytes(&self) -> usize {
        self.key.len() + 16
    }
}

impl BinaryRunRecord for EdgeSourceRunEntry {
    fn encode_to_now<W: Write>(&self, writer: &mut W, path: &Path) -> Result<(), KnightBusError> {
        write_u64_le_now(writer, self.row_index, path)?;
        write_string_field_now(writer, &self.from_key, path)?;
        write_string_field_now(writer, &self.to_key, path)
    }

    fn decode_from_now<R: Read>(
        reader: &mut R,
        path: &Path,
    ) -> Result<Option<Self>, KnightBusError> {
        let Some(row_index) = read_optional_u64_le_now(reader, path)? else {
            return Ok(None);
        };
        let from_key = read_string_field_now(reader, path)?;
        let to_key = read_string_field_now(reader, path)?;
        Ok(Some(Self {
            from_key,
            row_index,
            to_key,
        }))
    }

    fn estimated_heap_bytes(&self) -> usize {
        self.from_key.len() + self.to_key.len() + 32
    }
}

impl BinaryRunRecord for EdgeTargetRunEntry {
    fn encode_to_now<W: Write>(&self, writer: &mut W, path: &Path) -> Result<(), KnightBusError> {
        write_u64_le_now(writer, self.row_index, path)?;
        write_u32_le_now(writer, self.from_dense, path)?;
        write_string_field_now(writer, &self.to_key, path)
    }

    fn decode_from_now<R: Read>(
        reader: &mut R,
        path: &Path,
    ) -> Result<Option<Self>, KnightBusError> {
        let Some(row_index) = read_optional_u64_le_now(reader, path)? else {
            return Ok(None);
        };
        let from_dense = read_required_u32_le_now(reader, path)?;
        let to_key = read_string_field_now(reader, path)?;
        Ok(Some(Self {
            to_key,
            row_index,
            from_dense,
        }))
    }

    fn estimated_heap_bytes(&self) -> usize {
        self.to_key.len() + 24
    }
}

impl BinaryRunRecord for ResolvedEdgePair {
    fn encode_to_now<W: Write>(&self, writer: &mut W, path: &Path) -> Result<(), KnightBusError> {
        write_u32_le_now(writer, self.from_dense, path)?;
        write_u32_le_now(writer, self.to_dense, path)
    }

    fn decode_from_now<R: Read>(
        reader: &mut R,
        path: &Path,
    ) -> Result<Option<Self>, KnightBusError> {
        let Some(from_dense) = read_optional_u32_le_now(reader, path)? else {
            return Ok(None);
        };
        let to_dense = read_required_u32_le_now(reader, path)?;
        Ok(Some(Self {
            from_dense,
            to_dense,
        }))
    }

    fn estimated_heap_bytes(&self) -> usize {
        8
    }
}

struct RunReader<T> {
    path: PathBuf,
    reader: BufReader<File>,
    _marker: PhantomData<T>,
}

impl<T: BinaryRunRecord> RunReader<T> {
    fn open_now(path: &Path) -> Result<Self, KnightBusError> {
        let file = File::open(path).map_err(|source| KnightBusError::io(path, source))?;
        Ok(Self {
            path: path.to_path_buf(),
            reader: BufReader::new(file),
            _marker: PhantomData,
        })
    }

    fn next_record_now(&mut self) -> Result<Option<T>, KnightBusError> {
        T::decode_from_now(&mut self.reader, &self.path)
    }
}

#[derive(Debug)]
struct HeapItem<T> {
    value: T,
    run_index: usize,
}

impl<T: Ord> Ord for HeapItem<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value
            .cmp(&other.value)
            .then_with(|| self.run_index.cmp(&other.run_index))
    }
}

impl<T: Ord> PartialOrd for HeapItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Ord> PartialEq for HeapItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.run_index == other.run_index
    }
}

impl<T: Ord> Eq for HeapItem<T> {}

struct SortedRunMerger<T> {
    readers: Vec<RunReader<T>>,
    heap: BinaryHeap<Reverse<HeapItem<T>>>,
}

impl<T: BinaryRunRecord> SortedRunMerger<T> {
    fn open_now(run_paths: &[PathBuf]) -> Result<Self, KnightBusError> {
        let mut readers = Vec::with_capacity(run_paths.len());
        let mut heap = BinaryHeap::new();

        for run_path in run_paths {
            let mut reader = RunReader::<T>::open_now(run_path)?;
            let run_index = readers.len();
            if let Some(record) = reader.next_record_now()? {
                heap.push(Reverse(HeapItem {
                    value: record,
                    run_index,
                }));
            }
            readers.push(reader);
        }

        Ok(Self { readers, heap })
    }

    fn next_record_now(&mut self) -> Result<Option<T>, KnightBusError> {
        let Some(Reverse(item)) = self.heap.pop() else {
            return Ok(None);
        };

        if let Some(next_record) = self.readers[item.run_index].next_record_now()? {
            self.heap.push(Reverse(HeapItem {
                value: next_record,
                run_index: item.run_index,
            }));
        }

        Ok(Some(item.value))
    }
}

struct NodeCatalogStream {
    node_table: Mmap,
    strings: Mmap,
    dense_id_cursor: u32,
    node_count: u32,
}

impl NodeCatalogStream {
    fn open_now(snapshot_dir: &Path, node_count: u32) -> Result<Self, KnightBusError> {
        Ok(Self {
            node_table: map_file_read_only_now(snapshot_dir.join(NODE_TABLE_FILE_NAME))?,
            strings: map_file_read_only_now(snapshot_dir.join(STRINGS_FILE_NAME))?,
            dense_id_cursor: 0,
            node_count,
        })
    }

    fn next_entry_now(&mut self) -> Result<Option<(u32, String)>, KnightBusError> {
        if self.dense_id_cursor >= self.node_count {
            return Ok(None);
        }
        let dense_id = self.dense_id_cursor;
        self.dense_id_cursor += 1;
        let key = read_key_for_dense_id_now(&self.node_table, &self.strings, dense_id)?;
        Ok(Some((dense_id, key)))
    }
}

struct SnapshotEdgeStream {
    offsets: Mmap,
    peers: Mmap,
    node_count: u32,
    source_dense_id: u32,
    peer_index: u64,
}

impl SnapshotEdgeStream {
    fn open_now(
        snapshot_dir: &Path,
        direction: WalkDirection,
        node_count: u32,
    ) -> Result<Self, KnightBusError> {
        let (offsets_path, peers_path) = match direction {
            WalkDirection::Forward => (
                snapshot_dir.join(FORWARD_OFFSETS_FILE_NAME),
                snapshot_dir.join(FORWARD_PEERS_FILE_NAME),
            ),
            WalkDirection::Backward => (
                snapshot_dir.join(REVERSE_OFFSETS_FILE_NAME),
                snapshot_dir.join(REVERSE_PEERS_FILE_NAME),
            ),
        };
        Ok(Self {
            offsets: map_file_read_only_now(offsets_path)?,
            peers: map_file_read_only_now(peers_path)?,
            node_count,
            source_dense_id: 0,
            peer_index: 0,
        })
    }

    fn next_pair_now(&mut self) -> Option<ResolvedEdgePair> {
        while self.source_dense_id < self.node_count {
            let end_index =
                read_u64_from_bytes_now(&self.offsets, self.source_dense_id as usize + 1);
            if self.peer_index < end_index {
                let neighbor_dense_id =
                    read_u32_from_bytes_now(&self.peers, self.peer_index as usize);
                self.peer_index += 1;
                return Some(ResolvedEdgePair {
                    from_dense: self.source_dense_id,
                    to_dense: neighbor_dense_id,
                });
            }
            self.source_dense_id += 1;
            self.peer_index = read_u64_from_bytes_now(&self.offsets, self.source_dense_id as usize);
        }
        None
    }
}

fn build_node_key_runs_now(
    nodes_path: &Path,
    scratch_root: &Path,
    memory_budget: BuildMemoryBudget,
    peak_tracker: &mut PhasePeakTracker,
) -> Result<Vec<PathBuf>, KnightBusError> {
    track_phase_now(
        peak_tracker,
        SnapshotPhase::BuildNodeRuns,
        |peak_tracker, phase_peak_rss_bytes| {
            let mut reader = ReaderBuilder::new()
                .flexible(true)
                .from_path(nodes_path)
                .map_err(|source| KnightBusError::csv(nodes_path, source))?;
            let header_positions = resolve_header_positions_now(
                nodes_path,
                "nodes",
                reader
                    .headers()
                    .map_err(|source| KnightBusError::csv(nodes_path, source))?,
                &REQUIRED_NODE_HEADERS,
            )?;

            let mut buffer = Vec::new();
            let mut buffer_bytes = 0_usize;
            let mut run_paths = Vec::new();

            for (row_index, record_result) in reader.records().enumerate() {
                let record =
                    record_result.map_err(|source| KnightBusError::csv(nodes_path, source))?;
                let display_row_index = row_index + 2;
                let node_key = NodeKey::parse_csv_field(
                    read_required_field_now(
                        &record,
                        header_positions["node_id"],
                        nodes_path,
                        display_row_index,
                        "node_id",
                    )?,
                    nodes_path,
                    display_row_index,
                    "node_id",
                )?;
                let entry = NodeRunEntry {
                    key: node_key.into_string(),
                    row_index: display_row_index as u64,
                };
                buffer_bytes += entry.estimated_heap_bytes();
                buffer.push(entry);
                if buffer_bytes >= memory_budget.spill_buffer_bytes() {
                    spill_sorted_records_now(
                        &mut buffer,
                        scratch_root,
                        "node_keys",
                        &mut run_paths,
                    )?;
                    buffer_bytes = 0;
                    *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
                } else if display_row_index % SAMPLE_INTERVAL_ROWS == 0 {
                    *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
                }
            }

            spill_sorted_records_now(&mut buffer, scratch_root, "node_keys", &mut run_paths)?;
            *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
            Ok(run_paths)
        },
    )
}

fn write_node_catalog_now(
    nodes_path: &Path,
    output_dir: &Path,
    run_paths: &[PathBuf],
    peak_tracker: &mut PhasePeakTracker,
) -> Result<NodeCatalogSummary, KnightBusError> {
    track_phase_now(
        peak_tracker,
        SnapshotPhase::WriteNodeCatalog,
        |peak_tracker, phase_peak_rss_bytes| {
            let node_table_path = output_dir.join(NODE_TABLE_FILE_NAME);
            let strings_path = output_dir.join(STRINGS_FILE_NAME);
            let key_index_path = output_dir.join(KEY_INDEX_FILE_NAME);
            let mut node_table_writer = BufWriter::new(
                File::create(&node_table_path)
                    .map_err(|source| KnightBusError::io(&node_table_path, source))?,
            );
            let mut strings_writer = BufWriter::new(
                File::create(&strings_path)
                    .map_err(|source| KnightBusError::io(&strings_path, source))?,
            );
            let mut key_index_writer = BufWriter::new(
                File::create(&key_index_path)
                    .map_err(|source| KnightBusError::io(&key_index_path, source))?,
            );

            let mut merger = SortedRunMerger::<NodeRunEntry>::open_now(run_paths)?;
            let mut previous_key = None::<String>;
            let mut node_count = 0_u32;
            let mut string_offset = 0_u64;

            while let Some(entry) = merger.next_record_now()? {
                if previous_key.as_deref() == Some(entry.key.as_str()) {
                    return Err(KnightBusError::DuplicateNodeId {
                        path: nodes_path.to_path_buf(),
                        row_index: entry.row_index as usize,
                        node_id: entry.key,
                    });
                }

                let key_len = u32::try_from(entry.key.len()).map_err(|_| {
                    KnightBusError::SnapshotCorruption {
                        path: strings_path.clone(),
                        detail: "node key length exceeded u32 capacity".to_owned(),
                    }
                })?;
                strings_writer
                    .write_all(entry.key.as_bytes())
                    .map_err(|source| KnightBusError::io(&strings_path, source))?;
                node_table_writer
                    .write_all(
                        &NodeRecord {
                            key_offset: string_offset,
                            key_len,
                            flags: 0,
                        }
                        .encode_le(),
                    )
                    .map_err(|source| KnightBusError::io(&node_table_path, source))?;
                key_index_writer
                    .write_all(&node_count.to_le_bytes())
                    .map_err(|source| KnightBusError::io(&key_index_path, source))?;
                string_offset += u64::from(key_len);
                node_count =
                    node_count
                        .checked_add(1)
                        .ok_or(KnightBusError::NodeCountOverflow {
                            node_count: usize::MAX,
                        })?;
                previous_key = Some(entry.key);

                if (node_count as usize).is_multiple_of(SAMPLE_INTERVAL_ROWS) {
                    *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
                }
            }

            strings_writer
                .flush()
                .map_err(|source| KnightBusError::io(&strings_path, source))?;
            node_table_writer
                .flush()
                .map_err(|source| KnightBusError::io(&node_table_path, source))?;
            key_index_writer
                .flush()
                .map_err(|source| KnightBusError::io(&key_index_path, source))?;
            *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());

            Ok(NodeCatalogSummary { node_count })
        },
    )
}

fn build_edge_source_runs_now(
    edges_path: &Path,
    scratch_root: &Path,
    memory_budget: BuildMemoryBudget,
    peak_tracker: &mut PhasePeakTracker,
) -> Result<Vec<PathBuf>, KnightBusError> {
    track_phase_now(
        peak_tracker,
        SnapshotPhase::BuildEdgeRuns,
        |peak_tracker, phase_peak_rss_bytes| {
            let mut reader = ReaderBuilder::new()
                .flexible(true)
                .from_path(edges_path)
                .map_err(|source| KnightBusError::csv(edges_path, source))?;
            let header_positions = resolve_header_positions_now(
                edges_path,
                "edges",
                reader
                    .headers()
                    .map_err(|source| KnightBusError::csv(edges_path, source))?,
                &REQUIRED_EDGE_HEADERS,
            )?;

            let mut buffer = Vec::new();
            let mut buffer_bytes = 0_usize;
            let mut run_paths = Vec::new();

            for (row_index, record_result) in reader.records().enumerate() {
                let record =
                    record_result.map_err(|source| KnightBusError::csv(edges_path, source))?;
                let display_row_index = row_index + 2;
                let from_id = NodeKey::parse_csv_field(
                    read_required_field_now(
                        &record,
                        header_positions["from_id"],
                        edges_path,
                        display_row_index,
                        "from_id",
                    )?,
                    edges_path,
                    display_row_index,
                    "from_id",
                )?;
                let to_id = NodeKey::parse_csv_field(
                    read_required_field_now(
                        &record,
                        header_positions["to_id"],
                        edges_path,
                        display_row_index,
                        "to_id",
                    )?,
                    edges_path,
                    display_row_index,
                    "to_id",
                )?;
                let entry = EdgeSourceRunEntry {
                    from_key: from_id.into_string(),
                    row_index: display_row_index as u64,
                    to_key: to_id.into_string(),
                };
                buffer_bytes += entry.estimated_heap_bytes();
                buffer.push(entry);
                if buffer_bytes >= memory_budget.spill_buffer_bytes() {
                    spill_sorted_records_now(
                        &mut buffer,
                        scratch_root,
                        "edge_sources",
                        &mut run_paths,
                    )?;
                    buffer_bytes = 0;
                    *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
                } else if display_row_index % SAMPLE_INTERVAL_ROWS == 0 {
                    *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
                }
            }

            spill_sorted_records_now(&mut buffer, scratch_root, "edge_sources", &mut run_paths)?;
            *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
            Ok(run_paths)
        },
    )
}

fn resolve_from_keys_now(
    edges_path: &Path,
    snapshot_dir: &Path,
    node_count: u32,
    edge_run_paths: &[PathBuf],
    scratch_root: &Path,
    memory_budget: BuildMemoryBudget,
    peak_tracker: &mut PhasePeakTracker,
) -> Result<Vec<PathBuf>, KnightBusError> {
    track_phase_now(
        peak_tracker,
        SnapshotPhase::ResolveFromKeys,
        |peak_tracker, phase_peak_rss_bytes| {
            let mut merger = SortedRunMerger::<EdgeSourceRunEntry>::open_now(edge_run_paths)?;
            let mut node_catalog = NodeCatalogStream::open_now(snapshot_dir, node_count)?;
            let mut current_node = node_catalog.next_entry_now()?;
            let mut buffer = Vec::new();
            let mut buffer_bytes = 0_usize;
            let mut run_paths = Vec::new();
            let mut resolved_rows = 0_usize;

            while let Some(entry) = merger.next_record_now()? {
                loop {
                    match current_node.as_ref() {
                        Some((_, key)) if key.as_str() < entry.from_key.as_str() => {
                            current_node = node_catalog.next_entry_now()?;
                        }
                        _ => break,
                    }
                }

                let Some((from_dense, key)) = current_node.as_ref() else {
                    return Err(KnightBusError::MissingEdgeEndpoint {
                        path: edges_path.to_path_buf(),
                        row_index: entry.row_index as usize,
                        endpoint_role: "from",
                        node_id: entry.from_key,
                    });
                };
                if key.as_str() != entry.from_key.as_str() {
                    return Err(KnightBusError::MissingEdgeEndpoint {
                        path: edges_path.to_path_buf(),
                        row_index: entry.row_index as usize,
                        endpoint_role: "from",
                        node_id: entry.from_key,
                    });
                }

                let target_entry = EdgeTargetRunEntry {
                    to_key: entry.to_key,
                    row_index: entry.row_index,
                    from_dense: *from_dense,
                };
                buffer_bytes += target_entry.estimated_heap_bytes();
                buffer.push(target_entry);
                resolved_rows += 1;

                if buffer_bytes >= memory_budget.spill_buffer_bytes() {
                    spill_sorted_records_now(
                        &mut buffer,
                        scratch_root,
                        "edge_targets",
                        &mut run_paths,
                    )?;
                    buffer_bytes = 0;
                    *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
                } else if resolved_rows.is_multiple_of(SAMPLE_INTERVAL_ROWS) {
                    *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
                }
            }

            spill_sorted_records_now(&mut buffer, scratch_root, "edge_targets", &mut run_paths)?;
            *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
            Ok(run_paths)
        },
    )
}

fn resolve_to_keys_now(
    edges_path: &Path,
    snapshot_dir: &Path,
    node_count: u32,
    target_run_paths: &[PathBuf],
    scratch_root: &Path,
    memory_budget: BuildMemoryBudget,
    peak_tracker: &mut PhasePeakTracker,
) -> Result<Vec<PathBuf>, KnightBusError> {
    track_phase_now(
        peak_tracker,
        SnapshotPhase::ResolveToKeys,
        |peak_tracker, phase_peak_rss_bytes| {
            let mut merger = SortedRunMerger::<EdgeTargetRunEntry>::open_now(target_run_paths)?;
            let mut node_catalog = NodeCatalogStream::open_now(snapshot_dir, node_count)?;
            let mut current_node = node_catalog.next_entry_now()?;
            let mut buffer = Vec::new();
            let mut buffer_bytes = 0_usize;
            let mut run_paths = Vec::new();
            let mut resolved_rows = 0_usize;

            while let Some(entry) = merger.next_record_now()? {
                loop {
                    match current_node.as_ref() {
                        Some((_, key)) if key.as_str() < entry.to_key.as_str() => {
                            current_node = node_catalog.next_entry_now()?;
                        }
                        _ => break,
                    }
                }

                let Some((to_dense, key)) = current_node.as_ref() else {
                    return Err(KnightBusError::MissingEdgeEndpoint {
                        path: edges_path.to_path_buf(),
                        row_index: entry.row_index as usize,
                        endpoint_role: "to",
                        node_id: entry.to_key,
                    });
                };
                if key.as_str() != entry.to_key.as_str() {
                    return Err(KnightBusError::MissingEdgeEndpoint {
                        path: edges_path.to_path_buf(),
                        row_index: entry.row_index as usize,
                        endpoint_role: "to",
                        node_id: entry.to_key,
                    });
                }

                let edge_pair = ResolvedEdgePair {
                    from_dense: entry.from_dense,
                    to_dense: *to_dense,
                };
                buffer_bytes += edge_pair.estimated_heap_bytes();
                buffer.push(edge_pair);
                resolved_rows += 1;

                if buffer_bytes >= memory_budget.spill_buffer_bytes() {
                    spill_sorted_records_now(
                        &mut buffer,
                        scratch_root,
                        "resolved_edges",
                        &mut run_paths,
                    )?;
                    buffer_bytes = 0;
                    *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
                } else if resolved_rows.is_multiple_of(SAMPLE_INTERVAL_ROWS) {
                    *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
                }
            }

            spill_sorted_records_now(&mut buffer, scratch_root, "resolved_edges", &mut run_paths)?;
            *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
            Ok(run_paths)
        },
    )
}

fn emit_forward_snapshot_now(
    snapshot_dir: &Path,
    node_count: u32,
    resolved_edge_runs: &[PathBuf],
    scratch_root: &Path,
    memory_budget: BuildMemoryBudget,
    peak_tracker: &mut PhasePeakTracker,
) -> Result<u64, KnightBusError> {
    track_phase_now(
        peak_tracker,
        SnapshotPhase::EmitForwardCsr,
        |peak_tracker, phase_peak_rss_bytes| {
            let forward_offsets_path = snapshot_dir.join(FORWARD_OFFSETS_FILE_NAME);
            let forward_peers_path = snapshot_dir.join(FORWARD_PEERS_FILE_NAME);
            let mut offsets_writer = BufWriter::new(
                File::create(&forward_offsets_path)
                    .map_err(|source| KnightBusError::io(&forward_offsets_path, source))?,
            );
            let mut peers_writer = BufWriter::new(
                File::create(&forward_peers_path)
                    .map_err(|source| KnightBusError::io(&forward_peers_path, source))?,
            );
            let mut reverse_buffer = Vec::new();
            let mut reverse_buffer_bytes = 0_usize;
            let mut reverse_run_paths = Vec::new();
            let mut merger = SortedRunMerger::<ResolvedEdgePair>::open_now(resolved_edge_runs)?;
            let mut previous_pair = None::<ResolvedEdgePair>;
            let mut source_dense_id = 0_u32;
            let mut unique_edge_count = 0_u64;

            write_u64_le_now(&mut offsets_writer, 0, &forward_offsets_path)?;

            while let Some(edge_pair) = merger.next_record_now()? {
                if previous_pair == Some(edge_pair) {
                    continue;
                }
                while source_dense_id < edge_pair.from_dense {
                    write_u64_le_now(
                        &mut offsets_writer,
                        unique_edge_count,
                        &forward_offsets_path,
                    )?;
                    source_dense_id += 1;
                }
                write_u32_le_now(&mut peers_writer, edge_pair.to_dense, &forward_peers_path)?;
                unique_edge_count += 1;
                previous_pair = Some(edge_pair);

                let reverse_pair = ResolvedEdgePair {
                    from_dense: edge_pair.to_dense,
                    to_dense: edge_pair.from_dense,
                };
                reverse_buffer_bytes += reverse_pair.estimated_heap_bytes();
                reverse_buffer.push(reverse_pair);
                if reverse_buffer_bytes >= memory_budget.spill_buffer_bytes() {
                    spill_sorted_records_now(
                        &mut reverse_buffer,
                        scratch_root,
                        "reverse_edges",
                        &mut reverse_run_paths,
                    )?;
                    reverse_buffer_bytes = 0;
                }

                if (unique_edge_count as usize).is_multiple_of(SAMPLE_INTERVAL_ROWS) {
                    *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
                }
            }

            while source_dense_id < node_count {
                write_u64_le_now(
                    &mut offsets_writer,
                    unique_edge_count,
                    &forward_offsets_path,
                )?;
                source_dense_id += 1;
            }
            spill_sorted_records_now(
                &mut reverse_buffer,
                scratch_root,
                "reverse_edges",
                &mut reverse_run_paths,
            )?;
            offsets_writer
                .flush()
                .map_err(|source| KnightBusError::io(&forward_offsets_path, source))?;
            peers_writer
                .flush()
                .map_err(|source| KnightBusError::io(&forward_peers_path, source))?;
            write_reverse_run_manifest_now(scratch_root, &reverse_run_paths)?;
            *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());

            Ok(unique_edge_count)
        },
    )
}

fn emit_reverse_snapshot_now(
    snapshot_dir: &Path,
    node_count: u32,
    scratch_root: &Path,
    peak_tracker: &mut PhasePeakTracker,
) -> Result<(), KnightBusError> {
    track_phase_now(
        peak_tracker,
        SnapshotPhase::EmitReverseCsr,
        |peak_tracker, phase_peak_rss_bytes| {
            let reverse_run_paths = read_reverse_run_manifest_now(scratch_root)?;
            let reverse_offsets_path = snapshot_dir.join(REVERSE_OFFSETS_FILE_NAME);
            let reverse_peers_path = snapshot_dir.join(REVERSE_PEERS_FILE_NAME);
            let mut offsets_writer = BufWriter::new(
                File::create(&reverse_offsets_path)
                    .map_err(|source| KnightBusError::io(&reverse_offsets_path, source))?,
            );
            let mut peers_writer = BufWriter::new(
                File::create(&reverse_peers_path)
                    .map_err(|source| KnightBusError::io(&reverse_peers_path, source))?,
            );
            let mut merger = SortedRunMerger::<ResolvedEdgePair>::open_now(&reverse_run_paths)?;
            let mut source_dense_id = 0_u32;
            let mut edge_count = 0_u64;

            write_u64_le_now(&mut offsets_writer, 0, &reverse_offsets_path)?;
            while let Some(edge_pair) = merger.next_record_now()? {
                while source_dense_id < edge_pair.from_dense {
                    write_u64_le_now(&mut offsets_writer, edge_count, &reverse_offsets_path)?;
                    source_dense_id += 1;
                }
                write_u32_le_now(&mut peers_writer, edge_pair.to_dense, &reverse_peers_path)?;
                edge_count += 1;
                if (edge_count as usize).is_multiple_of(SAMPLE_INTERVAL_ROWS) {
                    *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
                }
            }
            while source_dense_id < node_count {
                write_u64_le_now(&mut offsets_writer, edge_count, &reverse_offsets_path)?;
                source_dense_id += 1;
            }
            offsets_writer
                .flush()
                .map_err(|source| KnightBusError::io(&reverse_offsets_path, source))?;
            peers_writer
                .flush()
                .map_err(|source| KnightBusError::io(&reverse_peers_path, source))?;
            *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
            Ok(())
        },
    )
}

fn validate_runtime_open_now(
    snapshot_dir: &Path,
    peak_tracker: &mut PhasePeakTracker,
) -> Result<MmapWalkRuntime, KnightBusError> {
    track_phase_now(
        peak_tracker,
        SnapshotPhase::ValidateOpenPath,
        |peak_tracker, phase_peak_rss_bytes| {
            let runtime = MmapWalkRuntime::open(snapshot_dir)?;
            *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
            Ok(runtime)
        },
    )
}

fn verify_node_catalog_now(
    snapshot_dir: &Path,
    node_count: u32,
    node_run_paths: &[PathBuf],
    peak_tracker: &mut PhasePeakTracker,
) -> Result<usize, KnightBusError> {
    track_phase_now(
        peak_tracker,
        SnapshotPhase::VerifyNodeCatalog,
        |peak_tracker, phase_peak_rss_bytes| {
            let mut merger = SortedRunMerger::<NodeRunEntry>::open_now(node_run_paths)?;
            let mut catalog = NodeCatalogStream::open_now(snapshot_dir, node_count)?;
            let mut previous_key = None::<String>;
            let mut checked_nodes = 0_usize;

            while let Some(entry) = merger.next_record_now()? {
                if previous_key.as_deref() == Some(entry.key.as_str()) {
                    return Err(KnightBusError::DuplicateNodeId {
                        path: snapshot_dir.to_path_buf(),
                        row_index: entry.row_index as usize,
                        node_id: entry.key,
                    });
                }
                let Some((dense_id, snapshot_key)) = catalog.next_entry_now()? else {
                    return Err(KnightBusError::StructuralMismatch {
                        detail: format!(
                            "snapshot is missing node `{}` at dense id {}",
                            entry.key, checked_nodes
                        ),
                    });
                };
                if snapshot_key != entry.key {
                    return Err(KnightBusError::StructuralMismatch {
                        detail: format!(
                            "node mismatch at dense id {}: snapshot=`{snapshot_key}`, csv=`{}`",
                            dense_id, entry.key
                        ),
                    });
                }
                previous_key = Some(entry.key);
                checked_nodes += 1;
                if checked_nodes.is_multiple_of(SAMPLE_INTERVAL_ROWS) {
                    *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
                }
            }

            if let Some((dense_id, snapshot_key)) = catalog.next_entry_now()? {
                return Err(KnightBusError::StructuralMismatch {
                    detail: format!(
                        "snapshot has extra node `{snapshot_key}` starting at dense id {dense_id}"
                    ),
                });
            }
            *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
            Ok(checked_nodes)
        },
    )
}

fn verify_forward_snapshot_now(
    snapshot_dir: &Path,
    node_count: u32,
    resolved_edge_runs: &[PathBuf],
    scratch_root: &Path,
    memory_budget: BuildMemoryBudget,
    peak_tracker: &mut PhasePeakTracker,
) -> Result<u64, KnightBusError> {
    track_phase_now(
        peak_tracker,
        SnapshotPhase::VerifyForwardCsr,
        |peak_tracker, phase_peak_rss_bytes| {
            let mut merger = SortedRunMerger::<ResolvedEdgePair>::open_now(resolved_edge_runs)?;
            let mut snapshot_edges =
                SnapshotEdgeStream::open_now(snapshot_dir, WalkDirection::Forward, node_count)?;
            let mut previous_pair = None::<ResolvedEdgePair>;
            let mut checked_edges = 0_u64;
            let mut reverse_buffer = Vec::new();
            let mut reverse_buffer_bytes = 0_usize;
            let mut reverse_run_paths = Vec::new();

            while let Some(edge_pair) = merger.next_record_now()? {
                if previous_pair == Some(edge_pair) {
                    continue;
                }
                let Some(snapshot_pair) = snapshot_edges.next_pair_now() else {
                    return Err(KnightBusError::StructuralMismatch {
                        detail: format!(
                            "snapshot forward CSR ended before csv edge {:?}",
                            edge_pair
                        ),
                    });
                };
                if snapshot_pair != edge_pair {
                    return Err(KnightBusError::StructuralMismatch {
                        detail: format!(
                            "forward CSR mismatch: snapshot={snapshot_pair:?}, csv={edge_pair:?}"
                        ),
                    });
                }
                previous_pair = Some(edge_pair);
                checked_edges += 1;

                let reverse_pair = ResolvedEdgePair {
                    from_dense: edge_pair.to_dense,
                    to_dense: edge_pair.from_dense,
                };
                reverse_buffer_bytes += reverse_pair.estimated_heap_bytes();
                reverse_buffer.push(reverse_pair);
                if reverse_buffer_bytes >= memory_budget.spill_buffer_bytes() {
                    spill_sorted_records_now(
                        &mut reverse_buffer,
                        scratch_root,
                        "verify_reverse_edges",
                        &mut reverse_run_paths,
                    )?;
                    reverse_buffer_bytes = 0;
                }

                if (checked_edges as usize).is_multiple_of(SAMPLE_INTERVAL_ROWS) {
                    *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
                }
            }

            if let Some(snapshot_pair) = snapshot_edges.next_pair_now() {
                return Err(KnightBusError::StructuralMismatch {
                    detail: format!(
                        "snapshot forward CSR has extra edge {:?} after csv stream completed",
                        snapshot_pair
                    ),
                });
            }
            spill_sorted_records_now(
                &mut reverse_buffer,
                scratch_root,
                "verify_reverse_edges",
                &mut reverse_run_paths,
            )?;
            write_reverse_run_manifest_now(scratch_root, &reverse_run_paths)?;
            *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
            Ok(checked_edges)
        },
    )
}

fn verify_reverse_snapshot_now(
    snapshot_dir: &Path,
    node_count: u32,
    scratch_root: &Path,
    peak_tracker: &mut PhasePeakTracker,
) -> Result<u64, KnightBusError> {
    track_phase_now(
        peak_tracker,
        SnapshotPhase::VerifyReverseCsr,
        |peak_tracker, phase_peak_rss_bytes| {
            let reverse_run_paths = read_reverse_run_manifest_now(scratch_root)?;
            let mut merger = SortedRunMerger::<ResolvedEdgePair>::open_now(&reverse_run_paths)?;
            let mut snapshot_edges =
                SnapshotEdgeStream::open_now(snapshot_dir, WalkDirection::Backward, node_count)?;
            let mut checked_edges = 0_u64;

            while let Some(edge_pair) = merger.next_record_now()? {
                let Some(snapshot_pair) = snapshot_edges.next_pair_now() else {
                    return Err(KnightBusError::StructuralMismatch {
                        detail: format!(
                            "snapshot reverse CSR ended before csv reverse edge {:?}",
                            edge_pair
                        ),
                    });
                };
                if snapshot_pair != edge_pair {
                    return Err(KnightBusError::StructuralMismatch {
                        detail: format!(
                            "reverse CSR mismatch: snapshot={snapshot_pair:?}, csv={edge_pair:?}"
                        ),
                    });
                }
                checked_edges += 1;
                if (checked_edges as usize).is_multiple_of(SAMPLE_INTERVAL_ROWS) {
                    *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
                }
            }

            if let Some(snapshot_pair) = snapshot_edges.next_pair_now() {
                return Err(KnightBusError::StructuralMismatch {
                    detail: format!(
                        "snapshot reverse CSR has extra edge {:?} after csv stream completed",
                        snapshot_pair
                    ),
                });
            }
            *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
            Ok(checked_edges)
        },
    )
}

fn run_query_smoke_checks_now(
    snapshot_dir: &Path,
    peak_tracker: &mut PhasePeakTracker,
) -> Result<Vec<VerificationFamilySummary>, KnightBusError> {
    track_phase_now(
        peak_tracker,
        SnapshotPhase::QuerySmokeChecks,
        |peak_tracker, phase_peak_rss_bytes| {
            let runtime = MmapWalkRuntime::open(snapshot_dir)?;
            let mut catalog = NodeCatalogStream::open_now(snapshot_dir, runtime.node_count())?;
            let mut seed_keys = Vec::new();
            while seed_keys.len() < SMOKE_QUERY_LIMIT {
                let Some((_, key)) = catalog.next_entry_now()? else {
                    break;
                };
                seed_keys.push(NodeKey::try_from(key)?);
            }

            let mut families = Vec::new();
            for family in QueryFamily::ALL {
                let mut checked_queries = 0_usize;
                for seed_key in &seed_keys {
                    let _ = runtime.query_keys_for_family(seed_key, family)?;
                    checked_queries += 1;
                }
                families.push(VerificationFamilySummary {
                    family,
                    checked_queries,
                });
            }
            *phase_peak_rss_bytes = (*phase_peak_rss_bytes).max(peak_tracker.sample_now());
            Ok(families)
        },
    )
}

fn spill_sorted_records_now<T: BinaryRunRecord>(
    buffer: &mut Vec<T>,
    scratch_root: &Path,
    prefix: &str,
    run_paths: &mut Vec<PathBuf>,
) -> Result<(), KnightBusError> {
    if buffer.is_empty() {
        return Ok(());
    }
    buffer.sort();
    let run_path = scratch_root.join(format!(
        "{RUN_FILE_PREFIX}-{prefix}-{}.bin",
        run_paths.len()
    ));
    let file = File::create(&run_path).map_err(|source| KnightBusError::io(&run_path, source))?;
    let mut writer = BufWriter::new(file);
    for record in buffer.iter() {
        record.encode_to_now(&mut writer, &run_path)?;
    }
    writer
        .flush()
        .map_err(|source| KnightBusError::io(&run_path, source))?;
    buffer.clear();
    run_paths.push(run_path);
    Ok(())
}

fn resolve_header_positions_now(
    csv_path: &Path,
    csv_kind: &'static str,
    header_record: &StringRecord,
    required_headers: &[&'static str],
) -> Result<std::collections::BTreeMap<&'static str, usize>, KnightBusError> {
    let mut positions = std::collections::BTreeMap::new();
    for required_header in required_headers {
        let position = header_record
            .iter()
            .position(|header| header == *required_header)
            .ok_or_else(|| KnightBusError::MissingRequiredHeader {
                path: csv_path.to_path_buf(),
                csv_kind,
                header: required_header,
            })?;
        positions.insert(*required_header, position);
    }
    Ok(positions)
}

fn read_required_field_now<'a>(
    record: &'a StringRecord,
    position: usize,
    csv_path: &Path,
    row_index: usize,
    column: &'static str,
) -> Result<&'a str, KnightBusError> {
    record
        .get(position)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| KnightBusError::MissingRequiredField {
            path: csv_path.to_path_buf(),
            row_index,
            column,
        })
}

fn write_snapshot_manifest_now(
    output_dir: &Path,
    node_count: u32,
    edge_count: u64,
) -> Result<(), KnightBusError> {
    let manifest = SnapshotManifest {
        version: 2,
        node_id_width: 32,
        adjacency_offset_width: 64,
        node_count,
        edge_count,
        key_mode: "sorted_key_index".to_owned(),
        storage_mode: "immutable_dual_csr".to_owned(),
        forward_offsets: FORWARD_OFFSETS_FILE_NAME.to_owned(),
        forward_peers: FORWARD_PEERS_FILE_NAME.to_owned(),
        reverse_offsets: REVERSE_OFFSETS_FILE_NAME.to_owned(),
        reverse_peers: REVERSE_PEERS_FILE_NAME.to_owned(),
        node_table: NODE_TABLE_FILE_NAME.to_owned(),
        strings: STRINGS_FILE_NAME.to_owned(),
        key_index: KEY_INDEX_FILE_NAME.to_owned(),
    };
    let manifest_path = output_dir.join(MANIFEST_FILE_NAME);
    let bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|source| KnightBusError::json(&manifest_path, source))?;
    fs::write(&manifest_path, bytes).map_err(|source| KnightBusError::io(&manifest_path, source))
}

fn read_snapshot_manifest_now(output_dir: &Path) -> Result<SnapshotManifest, KnightBusError> {
    let manifest_path = output_dir.join(MANIFEST_FILE_NAME);
    let bytes =
        fs::read(&manifest_path).map_err(|source| KnightBusError::io(&manifest_path, source))?;
    serde_json::from_slice(&bytes).map_err(|source| KnightBusError::json(&manifest_path, source))
}

fn write_reverse_run_manifest_now(
    scratch_root: &Path,
    reverse_run_paths: &[PathBuf],
) -> Result<(), KnightBusError> {
    let manifest_path = scratch_root.join("reverse-run-manifest.json");
    let manifest_bytes = serde_json::to_vec(reverse_run_paths)
        .map_err(|source| KnightBusError::json(&manifest_path, source))?;
    fs::write(&manifest_path, manifest_bytes)
        .map_err(|source| KnightBusError::io(&manifest_path, source))
}

fn read_reverse_run_manifest_now(scratch_root: &Path) -> Result<Vec<PathBuf>, KnightBusError> {
    let manifest_path = scratch_root.join("reverse-run-manifest.json");
    let manifest_bytes =
        fs::read(&manifest_path).map_err(|source| KnightBusError::io(&manifest_path, source))?;
    serde_json::from_slice(&manifest_bytes)
        .map_err(|source| KnightBusError::json(&manifest_path, source))
}

fn map_file_read_only_now(path: PathBuf) -> Result<Mmap, KnightBusError> {
    let file = File::open(&path).map_err(|source| KnightBusError::io(&path, source))?;
    unsafe { Mmap::map(&file) }.map_err(|source| KnightBusError::io(path, source))
}

fn read_key_for_dense_id_now(
    node_table: &Mmap,
    strings: &Mmap,
    dense_id: u32,
) -> Result<String, KnightBusError> {
    let start = dense_id as usize * NodeRecord::BYTE_LEN;
    let end = start + NodeRecord::BYTE_LEN;
    let node_record = NodeRecord::decode_le(&node_table[start..end]);
    let key_start = usize::try_from(node_record.key_offset).map_err(|_| {
        KnightBusError::StructuralMismatch {
            detail: "node key offset exceeded usize capacity".to_owned(),
        }
    })?;
    let key_end = key_start + node_record.key_len as usize;
    let bytes =
        strings
            .get(key_start..key_end)
            .ok_or_else(|| KnightBusError::StructuralMismatch {
                detail: format!("node key slice for dense id {dense_id} exceeds strings length"),
            })?;
    let key_str =
        std::str::from_utf8(bytes).map_err(|error| KnightBusError::StructuralMismatch {
            detail: format!("node key for dense id {dense_id} is not valid utf-8: {error}"),
        })?;
    Ok(key_str.to_owned())
}

fn write_u32_le_now<W: Write>(
    writer: &mut W,
    value: u32,
    path: &Path,
) -> Result<(), KnightBusError> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|source| KnightBusError::io(path, source))
}

fn write_u64_le_now<W: Write>(
    writer: &mut W,
    value: u64,
    path: &Path,
) -> Result<(), KnightBusError> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|source| KnightBusError::io(path, source))
}

fn write_string_field_now<W: Write>(
    writer: &mut W,
    value: &str,
    path: &Path,
) -> Result<(), KnightBusError> {
    let value_len = u32::try_from(value.len()).map_err(|_| KnightBusError::SnapshotCorruption {
        path: path.to_path_buf(),
        detail: "string field length exceeded u32 capacity".to_owned(),
    })?;
    write_u32_le_now(writer, value_len, path)?;
    writer
        .write_all(value.as_bytes())
        .map_err(|source| KnightBusError::io(path, source))
}

fn read_optional_u32_le_now<R: Read>(
    reader: &mut R,
    path: &Path,
) -> Result<Option<u32>, KnightBusError> {
    let Some(bytes) = read_optional_prefix_now::<R, 4>(reader, path)? else {
        return Ok(None);
    };
    Ok(Some(u32::from_le_bytes(bytes)))
}

fn read_required_u32_le_now<R: Read>(reader: &mut R, path: &Path) -> Result<u32, KnightBusError> {
    read_optional_u32_le_now(reader, path)?.ok_or_else(|| KnightBusError::StructuralMismatch {
        detail: format!("truncated u32 field in {}", path.display()),
    })
}

fn read_optional_u64_le_now<R: Read>(
    reader: &mut R,
    path: &Path,
) -> Result<Option<u64>, KnightBusError> {
    let Some(bytes) = read_optional_prefix_now::<R, 8>(reader, path)? else {
        return Ok(None);
    };
    Ok(Some(u64::from_le_bytes(bytes)))
}

fn read_string_field_now<R: Read>(reader: &mut R, path: &Path) -> Result<String, KnightBusError> {
    let length = read_required_u32_le_now(reader, path)? as usize;
    let mut bytes = vec![0_u8; length];
    reader
        .read_exact(&mut bytes)
        .map_err(|source| KnightBusError::io(path, source))?;
    String::from_utf8(bytes).map_err(|error| KnightBusError::StructuralMismatch {
        detail: format!("invalid utf-8 in {}: {error}", path.display()),
    })
}

fn read_optional_prefix_now<R: Read, const N: usize>(
    reader: &mut R,
    path: &Path,
) -> Result<Option<[u8; N]>, KnightBusError> {
    let mut first_byte = [0_u8; 1];
    match reader.read(&mut first_byte) {
        Ok(0) => Ok(None),
        Ok(1) => {
            let mut bytes = [0_u8; N];
            bytes[0] = first_byte[0];
            reader
                .read_exact(&mut bytes[1..])
                .map_err(|source| KnightBusError::io(path, source))?;
            Ok(Some(bytes))
        }
        Ok(_) => unreachable!(),
        Err(source) => Err(KnightBusError::io(path, source)),
    }
}

fn read_u64_from_bytes_now(mmap: &Mmap, index: usize) -> u64 {
    let start = index * 8;
    let end = start + 8;
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&mmap[start..end]);
    u64::from_le_bytes(bytes)
}

fn read_u32_from_bytes_now(mmap: &Mmap, index: usize) -> u32 {
    let start = index * 4;
    let end = start + 4;
    let mut bytes = [0_u8; 4];
    bytes.copy_from_slice(&mmap[start..end]);
    u32::from_le_bytes(bytes)
}

fn current_process_rss_bytes_now(system: &mut System, process_id: Pid) -> u64 {
    system.refresh_process(process_id);
    system
        .process(process_id)
        .map_or(0_u64, |process| process.memory())
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

#[cfg(target_os = "macos")]
fn peak_rss_bytes_from_getrusage_now() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return None;
    }
    let usage = unsafe { usage.assume_init() };
    u64::try_from(usage.ru_maxrss).ok()
}

#[cfg(all(unix, not(target_os = "macos")))]
fn peak_rss_bytes_from_getrusage_now() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return None;
    }
    let usage = unsafe { usage.assume_init() };
    u64::try_from(usage.ru_maxrss)
        .ok()
        .map(|kilobytes| kilobytes * 1024)
}

#[cfg(not(unix))]
fn peak_rss_bytes_from_getrusage_now() -> Option<u64> {
    None
}
