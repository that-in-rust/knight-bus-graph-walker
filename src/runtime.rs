use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

use memmap2::Mmap;

use crate::{
    error::KnightBusError,
    graph::collect_neighbors_within_hops,
    snapshot::{
        FORWARD_OFFSETS_FILE_NAME, FORWARD_PEERS_FILE_NAME, KEY_INDEX_FILE_NAME,
        MANIFEST_FILE_NAME, NODE_TABLE_FILE_NAME, REVERSE_OFFSETS_FILE_NAME,
        REVERSE_PEERS_FILE_NAME, STRINGS_FILE_NAME, compute_snapshot_size_bytes,
    },
    types::{
        DenseNodeId, HopCount, NodeKey, NodeRecord, QueryFamily, QueryResult, SnapshotManifest,
        WalkDirection,
    },
};

pub trait WalkQueryRuntime {
    fn query_entity_neighbors(
        &self,
        entity_key: &NodeKey,
        direction: WalkDirection,
        hops: HopCount,
    ) -> Result<QueryResult, KnightBusError>;

    fn query_keys_for_family(
        &self,
        entity_key: &NodeKey,
        family: QueryFamily,
    ) -> Result<Vec<String>, KnightBusError>;

    fn all_node_keys(&self) -> Result<Vec<NodeKey>, KnightBusError>;

    fn snapshot_size_bytes(&self) -> u64;
}

#[derive(Debug)]
pub struct MmapWalkRuntime {
    snapshot_dir: PathBuf,
    manifest: SnapshotManifest,
    snapshot_size_bytes: u64,
    forward_offsets: Mmap,
    forward_peers: Mmap,
    reverse_offsets: Mmap,
    reverse_peers: Mmap,
    node_table: Mmap,
    strings: Mmap,
    key_index: Mmap,
}

impl MmapWalkRuntime {
    pub fn open(snapshot_dir: &Path) -> Result<Self, KnightBusError> {
        let manifest_path = snapshot_dir.join(MANIFEST_FILE_NAME);
        let manifest_bytes = fs::read(&manifest_path)
            .map_err(|source| KnightBusError::io(&manifest_path, source))?;
        let manifest: SnapshotManifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|source| KnightBusError::json(&manifest_path, source))?;

        let runtime = Self {
            snapshot_dir: snapshot_dir.to_path_buf(),
            snapshot_size_bytes: 0,
            forward_offsets: map_file_read_only(snapshot_dir.join(FORWARD_OFFSETS_FILE_NAME))?,
            forward_peers: map_file_read_only(snapshot_dir.join(FORWARD_PEERS_FILE_NAME))?,
            reverse_offsets: map_file_read_only(snapshot_dir.join(REVERSE_OFFSETS_FILE_NAME))?,
            reverse_peers: map_file_read_only(snapshot_dir.join(REVERSE_PEERS_FILE_NAME))?,
            node_table: map_file_read_only(snapshot_dir.join(NODE_TABLE_FILE_NAME))?,
            strings: map_file_read_only(snapshot_dir.join(STRINGS_FILE_NAME))?,
            key_index: map_file_read_only(snapshot_dir.join(KEY_INDEX_FILE_NAME))?,
            manifest,
        };

        runtime.validate_open_path()?;
        let snapshot_size_bytes = compute_snapshot_size_bytes(snapshot_dir, &runtime.manifest)?;

        Ok(Self {
            snapshot_size_bytes,
            ..runtime
        })
    }

    pub fn snapshot_dir(&self) -> &Path {
        &self.snapshot_dir
    }

    pub fn node_count(&self) -> u32 {
        self.manifest.node_count
    }

    pub fn resolve_dense_id(&self, entity_key: &NodeKey) -> Result<DenseNodeId, KnightBusError> {
        let mut low = 0_usize;
        let mut high = self.manifest.node_count as usize;
        let target = entity_key.as_str();

        while low < high {
            let middle = low + (high - low) / 2;
            let dense_id = self.read_key_index_value(middle)?;
            let middle_key = self.key_str_for_dense_id(dense_id)?;
            match middle_key.cmp(target) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(DenseNodeId::new(dense_id)),
            }
        }

        Err(KnightBusError::UnknownEntity {
            entity: entity_key.to_string(),
        })
    }

    pub fn key_for_dense_id(&self, dense_id: u32) -> Result<String, KnightBusError> {
        Ok(self.key_str_for_dense_id(dense_id)?.to_owned())
    }

    fn validate_open_path(&self) -> Result<(), KnightBusError> {
        if self.manifest.version != 2 {
            return Err(KnightBusError::SnapshotCorruption {
                path: self.snapshot_dir.join(MANIFEST_FILE_NAME),
                detail: format!("unsupported manifest version {}", self.manifest.version),
            });
        }
        if self.manifest.node_id_width != 32 || self.manifest.adjacency_offset_width != 64 {
            return Err(KnightBusError::SnapshotCorruption {
                path: self.snapshot_dir.join(MANIFEST_FILE_NAME),
                detail: "unexpected width fields in manifest".to_owned(),
            });
        }

        self.validate_mmap_size(
            &self.snapshot_dir.join(FORWARD_OFFSETS_FILE_NAME),
            &self.forward_offsets,
            (u64::from(self.manifest.node_count) + 1) * 8,
        )?;
        self.validate_mmap_size(
            &self.snapshot_dir.join(FORWARD_PEERS_FILE_NAME),
            &self.forward_peers,
            self.manifest.edge_count * 4,
        )?;
        self.validate_mmap_size(
            &self.snapshot_dir.join(REVERSE_OFFSETS_FILE_NAME),
            &self.reverse_offsets,
            (u64::from(self.manifest.node_count) + 1) * 8,
        )?;
        self.validate_mmap_size(
            &self.snapshot_dir.join(REVERSE_PEERS_FILE_NAME),
            &self.reverse_peers,
            self.manifest.edge_count * 4,
        )?;
        self.validate_mmap_size(
            &self.snapshot_dir.join(NODE_TABLE_FILE_NAME),
            &self.node_table,
            u64::from(self.manifest.node_count) * NodeRecord::BYTE_LEN as u64,
        )?;
        self.validate_mmap_size(
            &self.snapshot_dir.join(KEY_INDEX_FILE_NAME),
            &self.key_index,
            u64::from(self.manifest.node_count) * 4,
        )?;

        self.validate_offsets_mmap(
            &self.forward_offsets,
            &self.snapshot_dir.join(FORWARD_OFFSETS_FILE_NAME),
        )?;
        self.validate_offsets_mmap(
            &self.reverse_offsets,
            &self.snapshot_dir.join(REVERSE_OFFSETS_FILE_NAME),
        )?;
        self.validate_node_records()?;
        self.validate_key_index()?;
        Ok(())
    }

    fn validate_mmap_size(
        &self,
        path: &Path,
        mmap: &Mmap,
        expected_bytes: u64,
    ) -> Result<(), KnightBusError> {
        let actual_bytes = mmap.len() as u64;
        if actual_bytes != expected_bytes {
            return Err(KnightBusError::SnapshotFileSize {
                path: path.to_path_buf(),
                expected_bytes,
                actual_bytes,
            });
        }
        Ok(())
    }

    fn validate_offsets_mmap(&self, mmap: &Mmap, path: &Path) -> Result<(), KnightBusError> {
        let mut previous = 0_u64;
        let entry_count = self.manifest.node_count as usize + 1;

        for index in 0..entry_count {
            let current = read_u64_from_mmap(mmap, index);
            if current < previous {
                return Err(KnightBusError::SnapshotCorruption {
                    path: path.to_path_buf(),
                    detail: format!("offset {index} is smaller than previous offset"),
                });
            }
            if current > self.manifest.edge_count {
                return Err(KnightBusError::SnapshotCorruption {
                    path: path.to_path_buf(),
                    detail: format!("offset {index} exceeds edge count"),
                });
            }
            previous = current;
        }

        if previous != self.manifest.edge_count {
            return Err(KnightBusError::SnapshotCorruption {
                path: path.to_path_buf(),
                detail: "last offset does not equal edge count".to_owned(),
            });
        }

        Ok(())
    }

    fn validate_node_records(&self) -> Result<(), KnightBusError> {
        for dense_id in 0..self.manifest.node_count {
            let node_record = self.node_record_for_dense_id(dense_id)?;
            let start = usize::try_from(node_record.key_offset).map_err(|_| {
                KnightBusError::SnapshotCorruption {
                    path: self.snapshot_dir.join(NODE_TABLE_FILE_NAME),
                    detail: "node key offset exceeded usize capacity".to_owned(),
                }
            })?;
            let end = start + node_record.key_len as usize;
            if end > self.strings.len() {
                return Err(KnightBusError::SnapshotCorruption {
                    path: self.snapshot_dir.join(STRINGS_FILE_NAME),
                    detail: format!(
                        "node key slice for dense id {dense_id} exceeds strings length"
                    ),
                });
            }
            let _ = std::str::from_utf8(&self.strings[start..end]).map_err(|error| {
                KnightBusError::SnapshotCorruption {
                    path: self.snapshot_dir.join(STRINGS_FILE_NAME),
                    detail: format!("node key for dense id {dense_id} is not valid utf-8: {error}"),
                }
            })?;
        }
        Ok(())
    }

    fn validate_key_index(&self) -> Result<(), KnightBusError> {
        let mut previous_key = None::<String>;
        for index in 0..self.manifest.node_count as usize {
            let dense_id = self.read_key_index_value(index)?;
            if dense_id >= self.manifest.node_count {
                return Err(KnightBusError::SnapshotCorruption {
                    path: self.snapshot_dir.join(KEY_INDEX_FILE_NAME),
                    detail: format!(
                        "key index entry {index} references out-of-range dense id {dense_id}"
                    ),
                });
            }
            let key = self.key_str_for_dense_id(dense_id)?.to_owned();
            if let Some(previous_key_value) = previous_key.as_ref()
                && previous_key_value > &key
            {
                return Err(KnightBusError::SnapshotCorruption {
                    path: self.snapshot_dir.join(KEY_INDEX_FILE_NAME),
                    detail: format!("key index is not sorted at entry {index}"),
                });
            }
            previous_key = Some(key);
        }
        Ok(())
    }

    fn node_record_for_dense_id(&self, dense_id: u32) -> Result<NodeRecord, KnightBusError> {
        let start = dense_id as usize * NodeRecord::BYTE_LEN;
        let end = start + NodeRecord::BYTE_LEN;
        Ok(NodeRecord::decode_le(&self.node_table[start..end]))
    }

    fn key_str_for_dense_id(&self, dense_id: u32) -> Result<&str, KnightBusError> {
        let node_record = self.node_record_for_dense_id(dense_id)?;
        let start = usize::try_from(node_record.key_offset).map_err(|_| {
            KnightBusError::SnapshotCorruption {
                path: self.snapshot_dir.join(NODE_TABLE_FILE_NAME),
                detail: "node key offset exceeded usize capacity".to_owned(),
            }
        })?;
        let end = start + node_record.key_len as usize;
        std::str::from_utf8(&self.strings[start..end]).map_err(|error| {
            KnightBusError::SnapshotCorruption {
                path: self.snapshot_dir.join(STRINGS_FILE_NAME),
                detail: format!("node key for dense id {dense_id} is not valid utf-8: {error}"),
            }
        })
    }

    fn read_key_index_value(&self, index: usize) -> Result<u32, KnightBusError> {
        let start = index * 4;
        let end = start + 4;
        let bytes =
            self.key_index
                .get(start..end)
                .ok_or_else(|| KnightBusError::SnapshotCorruption {
                    path: self.snapshot_dir.join(KEY_INDEX_FILE_NAME),
                    detail: format!("key index entry {index} is truncated"),
                })?;
        let mut value_bytes = [0_u8; 4];
        value_bytes.copy_from_slice(bytes);
        Ok(u32::from_le_bytes(value_bytes))
    }

    fn read_neighbor_ids(&self, dense_id: DenseNodeId, direction: WalkDirection) -> Vec<u32> {
        let (offsets_mmap, peers_mmap) = match direction {
            WalkDirection::Forward => (&self.forward_offsets, &self.forward_peers),
            WalkDirection::Backward => (&self.reverse_offsets, &self.reverse_peers),
        };

        let start = read_u64_from_mmap(offsets_mmap, dense_id.get() as usize) as usize;
        let end = read_u64_from_mmap(offsets_mmap, dense_id.get() as usize + 1) as usize;
        (start..end)
            .map(|index| read_u32_from_mmap(peers_mmap, index))
            .collect()
    }
}

impl WalkQueryRuntime for MmapWalkRuntime {
    fn query_entity_neighbors(
        &self,
        entity_key: &NodeKey,
        direction: WalkDirection,
        hops: HopCount,
    ) -> Result<QueryResult, KnightBusError> {
        let dense_id = self.resolve_dense_id(entity_key)?;
        let mut neighbors =
            collect_neighbors_within_hops(dense_id.get(), hops, |current_dense_id| {
                self.read_neighbor_ids(DenseNodeId::new(current_dense_id), direction)
            })
            .into_iter()
            .map(|neighbor_dense_id| self.key_for_dense_id(neighbor_dense_id))
            .collect::<Result<Vec<_>, _>>()?;
        neighbors.sort();

        Ok(QueryResult {
            entity: entity_key.to_string(),
            dense_id: dense_id.get(),
            direction,
            hops: hops.get(),
            neighbors,
        })
    }

    fn query_keys_for_family(
        &self,
        entity_key: &NodeKey,
        family: QueryFamily,
    ) -> Result<Vec<String>, KnightBusError> {
        Ok(self
            .query_entity_neighbors(entity_key, family.direction(), family.hops())?
            .neighbors)
    }

    fn all_node_keys(&self) -> Result<Vec<NodeKey>, KnightBusError> {
        (0..self.manifest.node_count)
            .map(|dense_id| self.key_for_dense_id(dense_id)?.try_into())
            .collect()
    }

    fn snapshot_size_bytes(&self) -> u64 {
        self.snapshot_size_bytes
    }
}

fn map_file_read_only(path: PathBuf) -> Result<Mmap, KnightBusError> {
    let file = File::open(&path).map_err(|source| KnightBusError::io(&path, source))?;
    // SAFETY: The file is opened read-only, the mapping is read-only, and the
    // runtime stores the file-backed bytes without mutating them.
    unsafe { Mmap::map(&file) }.map_err(|source| KnightBusError::io(path, source))
}

fn read_u64_from_mmap(mmap: &Mmap, index: usize) -> u64 {
    let start = index * 8;
    let end = start + 8;
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&mmap[start..end]);
    u64::from_le_bytes(bytes)
}

fn read_u32_from_mmap(mmap: &Mmap, index: usize) -> u32 {
    let start = index * 4;
    let end = start + 4;
    let mut bytes = [0_u8; 4];
    bytes.copy_from_slice(&mmap[start..end]);
    u32::from_le_bytes(bytes)
}
