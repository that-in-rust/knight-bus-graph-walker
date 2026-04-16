use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use crate::{
    error::KnightBusError,
    types::{NodeRecord, NormalizedGraphData, SnapshotBuildSummary, SnapshotManifest},
};

pub const MANIFEST_FILE_NAME: &str = "manifest.json";
pub const NODE_TABLE_FILE_NAME: &str = "node_table.bin";
pub const STRINGS_FILE_NAME: &str = "strings.bin";
pub const FORWARD_OFFSETS_FILE_NAME: &str = "forward.offsets.bin";
pub const FORWARD_PEERS_FILE_NAME: &str = "forward.peers.bin";
pub const REVERSE_OFFSETS_FILE_NAME: &str = "reverse.offsets.bin";
pub const REVERSE_PEERS_FILE_NAME: &str = "reverse.peers.bin";
pub const KEY_INDEX_FILE_NAME: &str = "key_index.bin";

pub trait SnapshotArtifactWriter {
    fn write_snapshot_artifacts(
        &self,
        graph_data: &NormalizedGraphData,
        output_dir: &Path,
    ) -> Result<SnapshotBuildSummary, KnightBusError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct FilesystemSnapshotWriter;

impl SnapshotArtifactWriter for FilesystemSnapshotWriter {
    fn write_snapshot_artifacts(
        &self,
        graph_data: &NormalizedGraphData,
        output_dir: &Path,
    ) -> Result<SnapshotBuildSummary, KnightBusError> {
        fs::create_dir_all(output_dir).map_err(|source| KnightBusError::io(output_dir, source))?;

        let (node_records, strings_bytes) = build_node_records_and_strings(graph_data)?;
        let key_index = build_key_index_for_graph(graph_data);
        let manifest = build_snapshot_manifest(graph_data);

        write_manifest_file(output_dir, &manifest)?;
        write_node_records_file(output_dir, &node_records)?;
        write_bytes_file(output_dir.join(STRINGS_FILE_NAME), &strings_bytes)?;
        write_u64_values_file(
            output_dir.join(FORWARD_OFFSETS_FILE_NAME),
            &graph_data.forward_offsets,
        )?;
        write_u32_values_file(
            output_dir.join(FORWARD_PEERS_FILE_NAME),
            &graph_data.forward_peers,
        )?;
        write_u64_values_file(
            output_dir.join(REVERSE_OFFSETS_FILE_NAME),
            &graph_data.reverse_offsets,
        )?;
        write_u32_values_file(
            output_dir.join(REVERSE_PEERS_FILE_NAME),
            &graph_data.reverse_peers,
        )?;
        write_u32_values_file(output_dir.join(KEY_INDEX_FILE_NAME), &key_index)?;

        let snapshot_size_bytes = compute_snapshot_size_bytes(output_dir, &manifest)?;
        Ok(SnapshotBuildSummary {
            output_dir: output_dir.to_path_buf(),
            node_count: graph_data.node_count(),
            edge_count: graph_data.edge_count(),
            snapshot_size_bytes,
        })
    }
}

pub fn compute_snapshot_size_bytes(
    snapshot_dir: &Path,
    manifest: &SnapshotManifest,
) -> Result<u64, KnightBusError> {
    let tracked_paths = [
        snapshot_dir.join(MANIFEST_FILE_NAME),
        snapshot_dir.join(&manifest.node_table),
        snapshot_dir.join(&manifest.strings),
        snapshot_dir.join(&manifest.forward_offsets),
        snapshot_dir.join(&manifest.forward_peers),
        snapshot_dir.join(&manifest.reverse_offsets),
        snapshot_dir.join(&manifest.reverse_peers),
        snapshot_dir.join(&manifest.key_index),
    ];

    tracked_paths
        .into_iter()
        .try_fold(0_u64, |running_size, path| {
            let metadata =
                fs::metadata(&path).map_err(|source| KnightBusError::io(path, source))?;
            Ok(running_size + metadata.len())
        })
}

fn build_snapshot_manifest(graph_data: &NormalizedGraphData) -> SnapshotManifest {
    SnapshotManifest {
        version: 2,
        node_id_width: 32,
        adjacency_offset_width: 64,
        node_count: graph_data.node_count(),
        edge_count: graph_data.edge_count(),
        key_mode: "sorted_key_index".to_owned(),
        storage_mode: "immutable_dual_csr".to_owned(),
        forward_offsets: FORWARD_OFFSETS_FILE_NAME.to_owned(),
        forward_peers: FORWARD_PEERS_FILE_NAME.to_owned(),
        reverse_offsets: REVERSE_OFFSETS_FILE_NAME.to_owned(),
        reverse_peers: REVERSE_PEERS_FILE_NAME.to_owned(),
        node_table: NODE_TABLE_FILE_NAME.to_owned(),
        strings: STRINGS_FILE_NAME.to_owned(),
        key_index: KEY_INDEX_FILE_NAME.to_owned(),
    }
}

fn build_node_records_and_strings(
    graph_data: &NormalizedGraphData,
) -> Result<(Vec<NodeRecord>, Vec<u8>), KnightBusError> {
    let mut node_records = Vec::with_capacity(graph_data.node_keys.len());
    let mut strings_bytes = Vec::new();

    for node_key in &graph_data.node_keys {
        let key_bytes = node_key.as_str().as_bytes();
        let key_offset = strings_bytes.len() as u64;
        let key_len =
            u32::try_from(key_bytes.len()).map_err(|_| KnightBusError::SnapshotCorruption {
                path: PathBuf::from(STRINGS_FILE_NAME),
                detail: "node key length exceeded u32 capacity".to_owned(),
            })?;
        strings_bytes.extend_from_slice(key_bytes);
        node_records.push(NodeRecord {
            key_offset,
            key_len,
            flags: 0,
        });
    }

    Ok((node_records, strings_bytes))
}

fn build_key_index_for_graph(graph_data: &NormalizedGraphData) -> Vec<u32> {
    let mut key_index = (0..graph_data.node_count()).collect::<Vec<_>>();
    key_index.sort_by(|left, right| {
        graph_data.node_keys[*left as usize]
            .as_str()
            .cmp(graph_data.node_keys[*right as usize].as_str())
    });
    key_index
}

fn write_manifest_file(
    output_dir: &Path,
    manifest: &SnapshotManifest,
) -> Result<(), KnightBusError> {
    let manifest_path = output_dir.join(MANIFEST_FILE_NAME);
    let manifest_bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|source| KnightBusError::json(&manifest_path, source))?;
    write_bytes_file(manifest_path, &manifest_bytes)
}

fn write_node_records_file(
    output_dir: &Path,
    node_records: &[NodeRecord],
) -> Result<(), KnightBusError> {
    let node_table_path = output_dir.join(NODE_TABLE_FILE_NAME);
    let file = File::create(&node_table_path)
        .map_err(|source| KnightBusError::io(&node_table_path, source))?;
    let mut writer = BufWriter::new(file);

    for node_record in node_records.iter().copied() {
        writer
            .write_all(&node_record.encode_le())
            .map_err(|source| KnightBusError::io(&node_table_path, source))?;
    }
    writer
        .flush()
        .map_err(|source| KnightBusError::io(&node_table_path, source))
}

fn write_bytes_file(path: PathBuf, bytes: &[u8]) -> Result<(), KnightBusError> {
    fs::write(&path, bytes).map_err(|source| KnightBusError::io(path, source))
}

fn write_u64_values_file(path: PathBuf, values: &[u64]) -> Result<(), KnightBusError> {
    let file = File::create(&path).map_err(|source| KnightBusError::io(&path, source))?;
    let mut writer = BufWriter::new(file);

    for value in values {
        writer
            .write_all(&value.to_le_bytes())
            .map_err(|source| KnightBusError::io(&path, source))?;
    }
    writer
        .flush()
        .map_err(|source| KnightBusError::io(&path, source))
}

fn write_u32_values_file(path: PathBuf, values: &[u32]) -> Result<(), KnightBusError> {
    let file = File::create(&path).map_err(|source| KnightBusError::io(&path, source))?;
    let mut writer = BufWriter::new(file);

    for value in values {
        writer
            .write_all(&value.to_le_bytes())
            .map_err(|source| KnightBusError::io(&path, source))?;
    }
    writer
        .flush()
        .map_err(|source| KnightBusError::io(&path, source))
}
