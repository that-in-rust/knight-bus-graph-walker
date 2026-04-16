use std::collections::{BTreeMap, BTreeSet};

use crate::{
    error::KnightBusError,
    types::{DenseNodeId, HopCount, NormalizedGraphData, ValidatedTruthGraph, WalkDirection},
};

pub fn normalize_truth_graph_data(
    truth_graph: &ValidatedTruthGraph,
) -> Result<NormalizedGraphData, KnightBusError> {
    if truth_graph.nodes.len() > u32::MAX as usize {
        return Err(KnightBusError::NodeCountOverflow {
            node_count: truth_graph.nodes.len(),
        });
    }

    let mut node_keys = truth_graph
        .nodes
        .iter()
        .map(|row| row.node_id.clone())
        .collect::<Vec<_>>();
    node_keys.sort();

    let key_to_dense = node_keys
        .iter()
        .enumerate()
        .map(|(index, key)| (key.clone(), index as u32))
        .collect::<BTreeMap<_, _>>();

    let deduped_edges = truth_graph
        .edges
        .iter()
        .map(|edge| {
            let from_id = *key_to_dense
                .get(&edge.from_id)
                .expect("validated truth graph must contain edge sources");
            let to_id = *key_to_dense
                .get(&edge.to_id)
                .expect("validated truth graph must contain edge destinations");
            (from_id, to_id)
        })
        .collect::<BTreeSet<_>>();

    let node_count = node_keys.len();
    let mut forward_lists = vec![Vec::<u32>::new(); node_count];
    let mut reverse_lists = vec![Vec::<u32>::new(); node_count];

    for (from_id, to_id) in deduped_edges {
        forward_lists[from_id as usize].push(to_id);
        reverse_lists[to_id as usize].push(from_id);
    }

    let (forward_offsets, forward_peers) = flatten_adjacency_lists_now(&forward_lists)?;
    let (reverse_offsets, reverse_peers) = flatten_adjacency_lists_now(&reverse_lists)?;

    Ok(NormalizedGraphData {
        node_keys,
        forward_offsets,
        forward_peers,
        reverse_offsets,
        reverse_peers,
    })
}

pub fn flatten_adjacency_lists_now(
    adjacency_lists: &[Vec<u32>],
) -> Result<(Vec<u64>, Vec<u32>), KnightBusError> {
    let mut offsets = Vec::with_capacity(adjacency_lists.len() + 1);
    let total_peer_count = adjacency_lists.iter().map(Vec::len).sum::<usize>();
    if total_peer_count > u32::MAX as usize {
        return Err(KnightBusError::PeerCountOverflow {
            peer_count: total_peer_count,
        });
    }
    let mut peers = Vec::with_capacity(total_peer_count);
    let mut running_offset = 0_u64;
    offsets.push(running_offset);

    for neighbor_ids in adjacency_lists {
        running_offset += neighbor_ids.len() as u64;
        peers.extend(neighbor_ids.iter().copied());
        offsets.push(running_offset);
    }

    Ok((offsets, peers))
}

pub fn collect_neighbors_within_hops<F>(
    seed_dense_id: u32,
    hops: HopCount,
    mut neighbors_for_id: F,
) -> Vec<u32>
where
    F: FnMut(u32) -> Vec<u32>,
{
    let mut deduped_ids = BTreeSet::new();

    for dense_id in neighbors_for_id(seed_dense_id) {
        if dense_id != seed_dense_id {
            deduped_ids.insert(dense_id);
        }
    }

    if hops == HopCount::Two {
        let one_hop_frontier = deduped_ids.iter().copied().collect::<Vec<_>>();
        for direct_neighbor_id in one_hop_frontier {
            for dense_id in neighbors_for_id(direct_neighbor_id) {
                if dense_id != seed_dense_id {
                    deduped_ids.insert(dense_id);
                }
            }
        }
    }

    deduped_ids.into_iter().collect()
}

pub fn query_normalized_graph(
    graph_data: &NormalizedGraphData,
    seed_dense_id: DenseNodeId,
    direction: WalkDirection,
    hops: HopCount,
) -> Vec<DenseNodeId> {
    let (offsets, peers) = match direction {
        WalkDirection::Forward => (&graph_data.forward_offsets, &graph_data.forward_peers),
        WalkDirection::Backward => (&graph_data.reverse_offsets, &graph_data.reverse_peers),
    };

    collect_neighbors_within_hops(seed_dense_id.get(), hops, |dense_id| {
        read_neighbor_slice_from_arrays(offsets, peers, dense_id)
    })
    .into_iter()
    .map(DenseNodeId::new)
    .collect()
}

fn read_neighbor_slice_from_arrays(offsets: &[u64], peers: &[u32], dense_id: u32) -> Vec<u32> {
    let start = offsets[dense_id as usize] as usize;
    let end = offsets[dense_id as usize + 1] as usize;
    peers[start..end].to_vec()
}

#[cfg(test)]
mod tests {
    use crate::types::{
        CsvEdgeRow, CsvNodeRow, DenseNodeId, HopCount, ValidatedTruthGraph, WalkDirection,
    };

    use super::{normalize_truth_graph_data, query_normalized_graph};

    fn fixture_truth_graph() -> ValidatedTruthGraph {
        ValidatedTruthGraph {
            nodes: vec![
                CsvNodeRow {
                    node_id: "alpha".to_owned().try_into().expect("valid key"),
                    node_type: "function".to_owned(),
                    label: "alpha".to_owned(),
                    parent_id: None,
                    file_path: None,
                    span: None,
                },
                CsvNodeRow {
                    node_id: "beta".to_owned().try_into().expect("valid key"),
                    node_type: "function".to_owned(),
                    label: "beta".to_owned(),
                    parent_id: None,
                    file_path: None,
                    span: None,
                },
                CsvNodeRow {
                    node_id: "gamma".to_owned().try_into().expect("valid key"),
                    node_type: "function".to_owned(),
                    label: "gamma".to_owned(),
                    parent_id: None,
                    file_path: None,
                    span: None,
                },
            ],
            edges: vec![
                CsvEdgeRow {
                    from_id: "alpha".to_owned().try_into().expect("valid key"),
                    edge_type: "depends_on".to_owned(),
                    to_id: "beta".to_owned().try_into().expect("valid key"),
                },
                CsvEdgeRow {
                    from_id: "beta".to_owned().try_into().expect("valid key"),
                    edge_type: "depends_on".to_owned(),
                    to_id: "gamma".to_owned().try_into().expect("valid key"),
                },
                CsvEdgeRow {
                    from_id: "alpha".to_owned().try_into().expect("valid key"),
                    edge_type: "depends_on".to_owned(),
                    to_id: "beta".to_owned().try_into().expect("valid key"),
                },
            ],
        }
    }

    #[test]
    fn normalize_truth_graph_data_deduplicates_edges() {
        let normalized =
            normalize_truth_graph_data(&fixture_truth_graph()).expect("graph normalizes");

        assert_eq!(normalized.node_count(), 3);
        assert_eq!(normalized.edge_count(), 2);
        assert_eq!(normalized.forward_peers, vec![1, 2]);
    }

    #[test]
    fn query_normalized_graph_uses_within_two_hops() {
        let normalized =
            normalize_truth_graph_data(&fixture_truth_graph()).expect("graph normalizes");
        let neighbors = query_normalized_graph(
            &normalized,
            DenseNodeId::new(0),
            WalkDirection::Forward,
            HopCount::Two,
        );

        assert_eq!(
            neighbors.into_iter().map(|id| id.get()).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }
}
