use std::path::PathBuf;

use tempfile::TempDir;

pub fn fixture_path(relative_path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(relative_path)
}

pub fn valid_nodes_path() -> PathBuf {
    fixture_path("valid/interface_nodes.csv")
}

pub fn valid_edges_path() -> PathBuf {
    fixture_path("valid/interface_edges.csv")
}

pub fn valid_corpus_path() -> PathBuf {
    fixture_path("valid/corpus_query.csv")
}

pub fn temp_snapshot_dir() -> (TempDir, PathBuf) {
    let temp_dir = TempDir::new().expect("temp dir");
    let snapshot_dir = temp_dir.path().join("snapshot");
    (temp_dir, snapshot_dir)
}
