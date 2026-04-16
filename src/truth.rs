use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use csv::{ReaderBuilder, StringRecord};

use crate::{
    error::KnightBusError,
    types::{
        CsvEdgeRow, CsvNodeRow, HopCount, NodeKey, QueryFamily, ValidatedTruthGraph, WalkDirection,
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

pub trait TruthGraphSource {
    fn load_truth_graph_rows(&self) -> Result<ValidatedTruthGraph, KnightBusError>;
}

#[derive(Clone, Debug)]
pub struct CsvTruthGraphSource {
    nodes_path: PathBuf,
    edges_path: PathBuf,
}

impl CsvTruthGraphSource {
    pub fn new(nodes_path: impl Into<PathBuf>, edges_path: impl Into<PathBuf>) -> Self {
        Self {
            nodes_path: nodes_path.into(),
            edges_path: edges_path.into(),
        }
    }
}

impl TruthGraphSource for CsvTruthGraphSource {
    fn load_truth_graph_rows(&self) -> Result<ValidatedTruthGraph, KnightBusError> {
        let node_rows = load_node_rows_from_csv(&self.nodes_path)?;
        let edge_rows = load_edge_rows_from_csv(&self.edges_path, &node_rows)?;

        Ok(ValidatedTruthGraph {
            nodes: node_rows,
            edges: edge_rows,
        })
    }
}

#[derive(Clone, Debug)]
pub struct TruthGraphIndex {
    node_keys: Vec<NodeKey>,
    forward_map: BTreeMap<NodeKey, BTreeSet<NodeKey>>,
    reverse_map: BTreeMap<NodeKey, BTreeSet<NodeKey>>,
}

impl TruthGraphIndex {
    pub fn from_truth_graph_rows(truth_graph: &ValidatedTruthGraph) -> Self {
        let mut node_keys = truth_graph
            .nodes
            .iter()
            .map(|row| row.node_id.clone())
            .collect::<Vec<_>>();
        node_keys.sort();

        let mut forward_map = node_keys
            .iter()
            .cloned()
            .map(|key| (key, BTreeSet::new()))
            .collect::<BTreeMap<_, _>>();
        let mut reverse_map = node_keys
            .iter()
            .cloned()
            .map(|key| (key, BTreeSet::new()))
            .collect::<BTreeMap<_, _>>();

        for edge_row in &truth_graph.edges {
            forward_map
                .entry(edge_row.from_id.clone())
                .or_default()
                .insert(edge_row.to_id.clone());
            reverse_map
                .entry(edge_row.to_id.clone())
                .or_default()
                .insert(edge_row.from_id.clone());
        }

        Self {
            node_keys,
            forward_map,
            reverse_map,
        }
    }

    pub fn all_node_keys(&self) -> &[NodeKey] {
        &self.node_keys
    }

    pub fn seed_keys_for_family(&self, family: QueryFamily) -> Vec<NodeKey> {
        self.node_keys
            .iter()
            .filter_map(|node_key| {
                self.neighbors_within(node_key, family.direction(), family.hops())
                    .ok()
                    .filter(|neighbors| !neighbors.is_empty())
                    .map(|_| node_key.clone())
            })
            .collect()
    }

    pub fn neighbors_within(
        &self,
        entity_key: &NodeKey,
        direction: WalkDirection,
        hops: HopCount,
    ) -> Result<Vec<NodeKey>, KnightBusError> {
        let lookup_map = self.lookup_map_for_direction(direction);
        if !lookup_map.contains_key(entity_key) {
            return Err(KnightBusError::UnknownEntity {
                entity: entity_key.to_string(),
            });
        }

        let mut deduped_keys = lookup_map
            .get(entity_key)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|neighbor_key| neighbor_key != entity_key)
            .collect::<BTreeSet<_>>();

        if hops == HopCount::Two {
            let one_hop_neighbors = deduped_keys.iter().cloned().collect::<Vec<_>>();
            for direct_neighbor_key in one_hop_neighbors {
                if let Some(second_hop_keys) = lookup_map.get(&direct_neighbor_key) {
                    for second_hop_key in second_hop_keys {
                        if second_hop_key != entity_key {
                            deduped_keys.insert(second_hop_key.clone());
                        }
                    }
                }
            }
        }

        Ok(deduped_keys.into_iter().collect())
    }

    fn lookup_map_for_direction(
        &self,
        direction: WalkDirection,
    ) -> &BTreeMap<NodeKey, BTreeSet<NodeKey>> {
        match direction {
            WalkDirection::Forward => &self.forward_map,
            WalkDirection::Backward => &self.reverse_map,
        }
    }
}

fn load_node_rows_from_csv(nodes_path: &Path) -> Result<Vec<CsvNodeRow>, KnightBusError> {
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_path(nodes_path)
        .map_err(|source| KnightBusError::csv(nodes_path, source))?;
    let node_header_positions = resolve_header_positions(
        nodes_path,
        "nodes",
        reader
            .headers()
            .map_err(|source| KnightBusError::csv(nodes_path, source))?,
        &REQUIRED_NODE_HEADERS,
    )?;

    let mut seen_node_keys = BTreeSet::new();
    let mut node_rows = Vec::new();

    for (row_index, record_result) in reader.records().enumerate() {
        let record = record_result.map_err(|source| KnightBusError::csv(nodes_path, source))?;
        let display_row_index = row_index + 2;

        let node_id = NodeKey::parse_csv_field(
            read_required_field(
                &record,
                node_header_positions["node_id"],
                nodes_path,
                display_row_index,
                "node_id",
            )?,
            nodes_path,
            display_row_index,
            "node_id",
        )?;

        if !seen_node_keys.insert(node_id.clone()) {
            return Err(KnightBusError::DuplicateNodeId {
                path: nodes_path.to_path_buf(),
                row_index: display_row_index,
                node_id: node_id.to_string(),
            });
        }

        node_rows.push(CsvNodeRow {
            node_id,
            node_type: read_required_field(
                &record,
                node_header_positions["node_type"],
                nodes_path,
                display_row_index,
                "node_type",
            )?
            .to_owned(),
            label: read_required_field(
                &record,
                node_header_positions["label"],
                nodes_path,
                display_row_index,
                "label",
            )?
            .to_owned(),
            parent_id: read_optional_field(&record, node_header_positions["parent_id"]),
            file_path: read_optional_field(&record, node_header_positions["file_path"]),
            span: read_optional_field(&record, node_header_positions["span"]),
        });
    }

    Ok(node_rows)
}

fn load_edge_rows_from_csv(
    edges_path: &Path,
    node_rows: &[CsvNodeRow],
) -> Result<Vec<CsvEdgeRow>, KnightBusError> {
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_path(edges_path)
        .map_err(|source| KnightBusError::csv(edges_path, source))?;
    let edge_header_positions = resolve_header_positions(
        edges_path,
        "edges",
        reader
            .headers()
            .map_err(|source| KnightBusError::csv(edges_path, source))?,
        &REQUIRED_EDGE_HEADERS,
    )?;

    let known_node_keys = node_rows
        .iter()
        .map(|row| row.node_id.clone())
        .collect::<BTreeSet<_>>();
    let mut edge_rows = Vec::new();

    for (row_index, record_result) in reader.records().enumerate() {
        let record = record_result.map_err(|source| KnightBusError::csv(edges_path, source))?;
        let display_row_index = row_index + 2;

        let from_id = NodeKey::parse_csv_field(
            read_required_field(
                &record,
                edge_header_positions["from_id"],
                edges_path,
                display_row_index,
                "from_id",
            )?,
            edges_path,
            display_row_index,
            "from_id",
        )?;
        if !known_node_keys.contains(&from_id) {
            return Err(KnightBusError::MissingEdgeEndpoint {
                path: edges_path.to_path_buf(),
                row_index: display_row_index,
                endpoint_role: "from_id",
                node_id: from_id.to_string(),
            });
        }

        let to_id = NodeKey::parse_csv_field(
            read_required_field(
                &record,
                edge_header_positions["to_id"],
                edges_path,
                display_row_index,
                "to_id",
            )?,
            edges_path,
            display_row_index,
            "to_id",
        )?;
        if !known_node_keys.contains(&to_id) {
            return Err(KnightBusError::MissingEdgeEndpoint {
                path: edges_path.to_path_buf(),
                row_index: display_row_index,
                endpoint_role: "to_id",
                node_id: to_id.to_string(),
            });
        }

        edge_rows.push(CsvEdgeRow {
            from_id,
            edge_type: read_required_field(
                &record,
                edge_header_positions["edge_type"],
                edges_path,
                display_row_index,
                "edge_type",
            )?
            .to_owned(),
            to_id,
        });
    }

    Ok(edge_rows)
}

fn resolve_header_positions(
    csv_path: &Path,
    csv_kind: &'static str,
    headers: &StringRecord,
    required_headers: &[&'static str],
) -> Result<BTreeMap<&'static str, usize>, KnightBusError> {
    let mut header_positions = BTreeMap::new();

    for required_header in required_headers {
        let Some(position) = headers.iter().position(|header| header == *required_header) else {
            return Err(KnightBusError::MissingRequiredHeader {
                path: csv_path.to_path_buf(),
                csv_kind,
                header: required_header,
            });
        };
        header_positions.insert(*required_header, position);
    }

    Ok(header_positions)
}

fn read_required_field<'record>(
    record: &'record StringRecord,
    index: usize,
    csv_path: &Path,
    row_index: usize,
    column: &'static str,
) -> Result<&'record str, KnightBusError> {
    record
        .get(index)
        .ok_or_else(|| KnightBusError::MissingRequiredField {
            path: csv_path.to_path_buf(),
            row_index,
            column,
        })
}

fn read_optional_field(record: &StringRecord, index: usize) -> Option<String> {
    record
        .get(index)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use crate::error::KnightBusError;

    use super::{CsvTruthGraphSource, TruthGraphIndex, TruthGraphSource};

    #[test]
    fn csv_truth_graph_source_rejects_duplicate_nodes() {
        let temp_dir = TempDir::new().expect("temp dir");
        let nodes_path = temp_dir.path().join("nodes.csv");
        let edges_path = temp_dir.path().join("edges.csv");

        fs::write(
            &nodes_path,
            "node_id,node_type,label,parent_id,file_path,span\nalpha,function,alpha,,,\nalpha,function,alpha,,,\n",
        )
        .expect("nodes written");
        fs::write(
            &edges_path,
            "from_id,edge_type,to_id\nalpha,depends_on,alpha\n",
        )
        .expect("edges written");

        let error = CsvTruthGraphSource::new(&nodes_path, &edges_path)
            .load_truth_graph_rows()
            .expect_err("duplicate node ids must fail");

        assert!(matches!(error, KnightBusError::DuplicateNodeId { .. }));
    }

    #[test]
    fn truth_graph_index_within_two_hops_excludes_seed() {
        let temp_dir = TempDir::new().expect("temp dir");
        let nodes_path = temp_dir.path().join("nodes.csv");
        let edges_path = temp_dir.path().join("edges.csv");

        fs::write(
            &nodes_path,
            "node_id,node_type,label,parent_id,file_path,span\nalpha,function,alpha,,,\nbeta,function,beta,,,\ngamma,function,gamma,,,\n",
        )
        .expect("nodes written");
        fs::write(
            &edges_path,
            "from_id,edge_type,to_id\nalpha,depends_on,beta\nbeta,depends_on,gamma\ngamma,depends_on,alpha\n",
        )
        .expect("edges written");

        let truth_rows = CsvTruthGraphSource::new(&nodes_path, &edges_path)
            .load_truth_graph_rows()
            .expect("truth rows load");
        let truth_index = TruthGraphIndex::from_truth_graph_rows(&truth_rows);
        let neighbors = truth_index
            .neighbors_within(
                &"alpha".to_owned().try_into().expect("valid key"),
                crate::types::WalkDirection::Forward,
                crate::types::HopCount::Two,
            )
            .expect("neighbors resolve");

        assert_eq!(
            neighbors
                .into_iter()
                .map(|key| key.to_string())
                .collect::<Vec<_>>(),
            vec!["beta".to_owned(), "gamma".to_owned()]
        );
    }
}
