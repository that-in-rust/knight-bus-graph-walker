use std::{io, path::PathBuf};

use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum KnightBusError {
    #[error("failed to read file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to parse CSV {path}: {source}")]
    Csv {
        path: PathBuf,
        #[source]
        source: csv::Error,
    },
    #[error("failed to parse JSON {path}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("missing required {csv_kind} CSV header `{header}` in {path}")]
    MissingRequiredHeader {
        path: PathBuf,
        csv_kind: &'static str,
        header: &'static str,
    },
    #[error("missing required field `{column}` in {path} at row {row_index}")]
    MissingRequiredField {
        path: PathBuf,
        row_index: usize,
        column: &'static str,
    },
    #[error("empty node key in column `{column}` in {path} at row {row_index}")]
    EmptyNodeKey {
        path: PathBuf,
        row_index: usize,
        column: &'static str,
    },
    #[error("duplicate node_id `{node_id}` in {path} at row {row_index}")]
    DuplicateNodeId {
        path: PathBuf,
        row_index: usize,
        node_id: String,
    },
    #[error(
        "edge endpoint `{node_id}` for `{endpoint_role}` in {path} at row {row_index} does not exist in nodes.csv"
    )]
    MissingEdgeEndpoint {
        path: PathBuf,
        row_index: usize,
        endpoint_role: &'static str,
        node_id: String,
    },
    #[error("invalid hop count `{value}`; only 1 and 2 are supported")]
    InvalidHopCount { value: u8 },
    #[error("invalid walk direction `{value}`; expected `forward` or `backward`")]
    InvalidWalkDirection { value: String },
    #[error("unknown entity `{entity}`")]
    UnknownEntity { entity: String },
    #[error("node count {node_count} exceeds u32 capacity")]
    NodeCountOverflow { node_count: usize },
    #[error("peer count {peer_count} exceeds u32 capacity")]
    PeerCountOverflow { peer_count: usize },
    #[error(
        "snapshot file {path} has unexpected size: expected {expected_bytes} bytes, found {actual_bytes} bytes"
    )]
    SnapshotFileSize {
        path: PathBuf,
        expected_bytes: u64,
        actual_bytes: u64,
    },
    #[error("snapshot corruption in {path}: {detail}")]
    SnapshotCorruption { path: PathBuf, detail: String },
    #[error(
        "parity mismatch for family `{family}` seed `{entity}`: expected {expected:?}, actual {actual:?}"
    )]
    ParityMismatch {
        family: String,
        entity: String,
        expected: Vec<String>,
        actual: Vec<String>,
    },
}

impl KnightBusError {
    pub fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }

    pub fn csv(path: impl Into<PathBuf>, source: csv::Error) -> Self {
        Self::Csv {
            path: path.into(),
            source,
        }
    }

    pub fn json(path: impl Into<PathBuf>, source: serde_json::Error) -> Self {
        Self::Json {
            path: path.into(),
            source,
        }
    }
}
