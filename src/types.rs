use std::{
    fmt,
    path::PathBuf,
    str::{self, FromStr},
};

use serde::{Deserialize, Serialize};

use crate::error::KnightBusError;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeKey(String);

impl NodeKey {
    pub fn parse_csv_field(
        value: &str,
        path: impl Into<PathBuf>,
        row_index: usize,
        column: &'static str,
    ) -> Result<Self, KnightBusError> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(KnightBusError::EmptyNodeKey {
                path: path.into(),
                row_index,
                column,
            });
        }
        Ok(Self(trimmed.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl TryFrom<String> for NodeKey {
    type Error = KnightBusError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(KnightBusError::EmptyNodeKey {
                path: PathBuf::from("<cli>"),
                row_index: 0,
                column: "entity",
            });
        }
        Ok(Self(trimmed.to_owned()))
    }
}

impl fmt::Display for NodeKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DenseNodeId(u32);

impl DenseNodeId {
    pub fn new(value: u32) -> Self {
        Self(value)
    }

    pub fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WalkDirection {
    Forward,
    Backward,
}

impl WalkDirection {
    pub fn label(self) -> &'static str {
        match self {
            Self::Forward => "forward",
            Self::Backward => "backward",
        }
    }
}

impl FromStr for WalkDirection {
    type Err = KnightBusError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "forward" => Ok(Self::Forward),
            "backward" => Ok(Self::Backward),
            other => Err(KnightBusError::InvalidWalkDirection {
                value: other.to_owned(),
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HopCount {
    One,
    Two,
}

impl HopCount {
    pub fn get(self) -> u8 {
        match self {
            Self::One => 1,
            Self::Two => 2,
        }
    }
}

impl TryFrom<u8> for HopCount {
    type Error = KnightBusError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::One),
            2 => Ok(Self::Two),
            _ => Err(KnightBusError::InvalidHopCount { value }),
        }
    }
}

impl FromStr for HopCount {
    type Err = KnightBusError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed = value
            .parse::<u8>()
            .map_err(|_| KnightBusError::InvalidHopCount { value: 0 })?;
        Self::try_from(parsed)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryFamily {
    ForwardOne,
    BackwardOne,
    ForwardTwo,
    BackwardTwo,
}

impl QueryFamily {
    pub const ALL: [Self; 4] = [
        Self::ForwardOne,
        Self::BackwardOne,
        Self::ForwardTwo,
        Self::BackwardTwo,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::ForwardOne => "forward_one",
            Self::BackwardOne => "backward_one",
            Self::ForwardTwo => "forward_two",
            Self::BackwardTwo => "backward_two",
        }
    }

    pub fn direction(self) -> WalkDirection {
        match self {
            Self::ForwardOne | Self::ForwardTwo => WalkDirection::Forward,
            Self::BackwardOne | Self::BackwardTwo => WalkDirection::Backward,
        }
    }

    pub fn hops(self) -> HopCount {
        match self {
            Self::ForwardOne | Self::BackwardOne => HopCount::One,
            Self::ForwardTwo | Self::BackwardTwo => HopCount::Two,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CsvNodeRow {
    pub node_id: NodeKey,
    pub node_type: String,
    pub label: String,
    pub parent_id: Option<String>,
    pub file_path: Option<String>,
    pub span: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CsvEdgeRow {
    pub from_id: NodeKey,
    pub edge_type: String,
    pub to_id: NodeKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedTruthGraph {
    pub nodes: Vec<CsvNodeRow>,
    pub edges: Vec<CsvEdgeRow>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NormalizedGraphData {
    pub node_keys: Vec<NodeKey>,
    pub forward_offsets: Vec<u64>,
    pub forward_peers: Vec<u32>,
    pub reverse_offsets: Vec<u64>,
    pub reverse_peers: Vec<u32>,
}

impl NormalizedGraphData {
    pub fn node_count(&self) -> u32 {
        self.node_keys.len() as u32
    }

    pub fn edge_count(&self) -> u64 {
        self.forward_peers.len() as u64
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeRecord {
    pub key_offset: u64,
    pub key_len: u32,
    pub flags: u32,
}

impl NodeRecord {
    pub const BYTE_LEN: usize = 16;

    pub fn encode_le(self) -> [u8; Self::BYTE_LEN] {
        let mut bytes = [0_u8; Self::BYTE_LEN];
        bytes[0..8].copy_from_slice(&self.key_offset.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.key_len.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.flags.to_le_bytes());
        bytes
    }

    pub fn decode_le(bytes: &[u8]) -> Self {
        let mut key_offset_bytes = [0_u8; 8];
        key_offset_bytes.copy_from_slice(&bytes[0..8]);

        let mut key_len_bytes = [0_u8; 4];
        key_len_bytes.copy_from_slice(&bytes[8..12]);

        let mut flags_bytes = [0_u8; 4];
        flags_bytes.copy_from_slice(&bytes[12..16]);

        Self {
            key_offset: u64::from_le_bytes(key_offset_bytes),
            key_len: u32::from_le_bytes(key_len_bytes),
            flags: u32::from_le_bytes(flags_bytes),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub version: u32,
    pub node_id_width: u32,
    pub adjacency_offset_width: u32,
    pub node_count: u32,
    pub edge_count: u64,
    pub key_mode: String,
    pub storage_mode: String,
    pub forward_offsets: String,
    pub forward_peers: String,
    pub reverse_offsets: String,
    pub reverse_peers: String,
    pub node_table: String,
    pub strings: String,
    pub key_index: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct QueryResult {
    pub entity: String,
    pub dense_id: u32,
    pub direction: WalkDirection,
    pub hops: u8,
    pub neighbors: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct VerificationFamilySummary {
    pub family: QueryFamily,
    pub checked_queries: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct VerificationSummary {
    pub total_checked_queries: usize,
    pub families: Vec<VerificationFamilySummary>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SnapshotBuildSummary {
    pub output_dir: PathBuf,
    pub node_count: u32,
    pub edge_count: u64,
    pub snapshot_size_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BenchmarkFamilyReport {
    pub family: QueryFamily,
    pub sample_count: usize,
    pub p50_nanos: u64,
    pub p95_nanos: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BenchmarkReport {
    pub snapshot_path: PathBuf,
    pub snapshot_size_bytes: u64,
    pub peak_rss_bytes: u64,
    pub families: Vec<BenchmarkFamilyReport>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BenchmarkRunSummary {
    pub report_path: PathBuf,
    pub report: BenchmarkReport,
}
