mod support;

use knight_bus::{
    CsvTruthGraphSource, DenseNodeId, GraphAdjacencyRuntime, HopCount, MmapWalkRuntime, NodeKey,
    QueryFamily, TruthGraphSource, WalkDirection, WalkQueryRuntime, build_snapshot_from_paths,
    normalize_truth_graph_data,
};

fn build_runtime_now() -> MmapWalkRuntime {
    let (_temp_dir, snapshot_dir) = support::temp_snapshot_dir();
    build_snapshot_from_paths(
        &support::valid_nodes_path(),
        &support::valid_edges_path(),
        &snapshot_dir,
    )
    .expect("snapshot builds");

    MmapWalkRuntime::open(&snapshot_dir).expect("runtime opens")
}

fn expected_edges_now(offsets: &[u64], peers: &[u32]) -> Vec<(u32, u32)> {
    let mut edges = Vec::new();

    for source_dense_id in 0..offsets.len().saturating_sub(1) {
        let start = offsets[source_dense_id] as usize;
        let end = offsets[source_dense_id + 1] as usize;
        for &target_dense_id in &peers[start..end] {
            edges.push((source_dense_id as u32, target_dense_id));
        }
    }

    edges
}

#[test]
fn mmap_runtime_neighbors_match_walk_query_runtime_now() {
    let runtime = build_runtime_now();
    let entity_key = NodeKey::try_from("fn:login_user_flow_now".to_owned()).expect("valid key");
    let dense_id = runtime.resolve_dense_id(&entity_key).expect("dense id");

    let expected_keys = runtime
        .query_entity_neighbors(&entity_key, WalkDirection::Forward, HopCount::One)
        .expect("one-hop query")
        .neighbors;

    let mut actual_keys = runtime
        .neighbors(dense_id, WalkDirection::Forward)
        .expect("neighbor cursor")
        .map(|neighbor_dense_id| {
            runtime
                .key_for_dense_id(neighbor_dense_id.get())
                .expect("key")
        })
        .collect::<Vec<_>>();
    actual_keys.sort();

    assert_eq!(actual_keys, expected_keys);
}

#[test]
fn mmap_runtime_global_edges_match_normalized_graph_now() {
    let runtime = build_runtime_now();
    let truth_rows =
        CsvTruthGraphSource::new(support::valid_nodes_path(), support::valid_edges_path())
            .load_truth_graph_rows()
            .expect("truth rows load");
    let normalized = normalize_truth_graph_data(&truth_rows).expect("normalized graph");

    assert_eq!(GraphAdjacencyRuntime::node_count(&runtime), 39);
    assert_eq!(
        GraphAdjacencyRuntime::relationship_count(&runtime),
        normalized.edge_count()
    );

    let forward_edges = runtime
        .global_edges(WalkDirection::Forward)
        .expect("forward edge cursor")
        .map(|(source, target)| (source.get(), target.get()))
        .collect::<Vec<_>>();
    let reverse_edges = runtime
        .global_edges(WalkDirection::Backward)
        .expect("reverse edge cursor")
        .map(|(source, target)| (source.get(), target.get()))
        .collect::<Vec<_>>();

    assert_eq!(
        forward_edges,
        expected_edges_now(&normalized.forward_offsets, &normalized.forward_peers)
    );
    assert_eq!(
        reverse_edges,
        expected_edges_now(&normalized.reverse_offsets, &normalized.reverse_peers)
    );
}

#[test]
fn mmap_runtime_neighbor_cursor_supports_reverse_one_hop_now() {
    let runtime = build_runtime_now();
    let entity_key = NodeKey::try_from("fn:issue_login_token_now".to_owned()).expect("valid key");
    let dense_id = runtime.resolve_dense_id(&entity_key).expect("dense id");

    let expected_keys = runtime
        .query_keys_for_family(&entity_key, QueryFamily::BackwardOne)
        .expect("backward one-hop keys");

    let mut actual_keys = runtime
        .neighbors(dense_id, WalkDirection::Backward)
        .expect("reverse neighbor cursor")
        .map(|neighbor_dense_id| {
            runtime
                .key_for_dense_id(neighbor_dense_id.get())
                .expect("key")
        })
        .collect::<Vec<_>>();
    actual_keys.sort();

    assert_eq!(actual_keys, expected_keys);
}

#[test]
fn mmap_runtime_neighbor_cursor_rejects_out_of_range_dense_ids_now() {
    let runtime = build_runtime_now();
    let invalid_dense_id = DenseNodeId::new(10_000);

    let error = runtime
        .neighbors(invalid_dense_id, WalkDirection::Forward)
        .expect_err("out-of-range dense ids must fail");

    assert!(
        matches!(
            error,
            knight_bus::KnightBusError::DenseNodeIdOutOfRange { .. }
        ),
        "expected dense-id bounds error, got {error:?}"
    );
}
