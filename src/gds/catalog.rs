use std::{collections::BTreeMap, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::error::KnightBusError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectionSelector {
    All,
    Named(Vec<String>),
}

impl ProjectionSelector {
    pub fn all() -> Self {
        Self::All
    }

    pub fn named<I, S>(names: I) -> Result<Self, KnightBusError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Ok(Self::Named(normalize_selector_values_now(
            "projection",
            names,
        )?))
    }

    pub fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }

    pub fn named_values(&self) -> Option<&[String]> {
        match self {
            Self::All => None,
            Self::Named(values) => Some(values.as_slice()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PropertySelector {
    None,
    All,
    Named(Vec<String>),
}

impl PropertySelector {
    pub fn none() -> Self {
        Self::None
    }

    pub fn all() -> Self {
        Self::All
    }

    pub fn named<I, S>(names: I) -> Result<Self, KnightBusError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Ok(Self::Named(normalize_selector_values_now(
            "property", names,
        )?))
    }

    pub fn named_values(&self) -> Option<&[String]> {
        match self {
            Self::Named(values) => Some(values.as_slice()),
            Self::None | Self::All => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum RelationshipOrientation {
    Natural,
    Reverse,
    Undirected,
}

impl RelationshipOrientation {
    pub fn label(self) -> &'static str {
        match self {
            Self::Natural => "NATURAL",
            Self::Reverse => "REVERSE",
            Self::Undirected => "UNDIRECTED",
        }
    }

    pub fn inverse(self) -> Self {
        match self {
            Self::Natural => Self::Reverse,
            Self::Reverse => Self::Natural,
            Self::Undirected => Self::Undirected,
        }
    }
}

impl FromStr for RelationshipOrientation {
    type Err = KnightBusError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "NATURAL" => Ok(Self::Natural),
            "REVERSE" => Ok(Self::Reverse),
            "UNDIRECTED" => Ok(Self::Undirected),
            other => Err(KnightBusError::InvalidRelationshipOrientation {
                value: other.to_owned(),
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProjectionSidecarKind {
    NodeProperty,
    RelationshipProperty,
}

impl ProjectionSidecarKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::NodeProperty => "node_property",
            Self::RelationshipProperty => "relationship_property",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProjectionSidecarNeed {
    pub kind: ProjectionSidecarKind,
    pub name: String,
}

impl ProjectionSidecarNeed {
    pub fn new(kind: ProjectionSidecarKind, name: impl Into<String>) -> Self {
        Self {
            kind,
            name: name.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphProjectionSpec {
    pub graph_name: String,
    pub node_projection: ProjectionSelector,
    pub relationship_projection: ProjectionSelector,
    pub orientation: RelationshipOrientation,
    pub node_properties: PropertySelector,
    pub relationship_properties: PropertySelector,
}

impl GraphProjectionSpec {
    pub fn new(
        graph_name: String,
        node_projection: ProjectionSelector,
        relationship_projection: ProjectionSelector,
        orientation: RelationshipOrientation,
        node_properties: PropertySelector,
        relationship_properties: PropertySelector,
    ) -> Result<Self, KnightBusError> {
        let graph_name = normalize_graph_name_now(graph_name)?;
        Ok(Self {
            graph_name,
            node_projection,
            relationship_projection,
            orientation,
            node_properties,
            relationship_properties,
        })
    }

    pub fn required_sidecar_needs(&self) -> Vec<ProjectionSidecarNeed> {
        let mut needs = Vec::new();

        if let PropertySelector::Named(values) = &self.node_properties {
            for value in values {
                needs.push(ProjectionSidecarNeed::new(
                    ProjectionSidecarKind::NodeProperty,
                    value.clone(),
                ));
            }
        }

        if let PropertySelector::Named(values) = &self.relationship_properties {
            for value in values {
                needs.push(ProjectionSidecarNeed::new(
                    ProjectionSidecarKind::RelationshipProperty,
                    value.clone(),
                ));
            }
        }

        needs
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryEstimate {
    pub required_bytes: u64,
    pub topology_reference_bytes: u64,
    pub duplicate_topology_bytes: u64,
    pub sidecar_bytes: u64,
    pub catalog_metadata_bytes: u64,
    pub heap_bytes: u64,
    pub page_cache_bytes: u64,
    pub direct_io_buffer_bytes: u64,
    pub algorithm_state_bytes: u64,
    pub delta_overlay_bytes: u64,
    pub scratch_bytes: u64,
}

impl MemoryEstimate {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        topology_reference_bytes: u64,
        duplicate_topology_bytes: u64,
        sidecar_bytes: u64,
        catalog_metadata_bytes: u64,
        heap_bytes: u64,
        page_cache_bytes: u64,
        direct_io_buffer_bytes: u64,
        algorithm_state_bytes: u64,
        delta_overlay_bytes: u64,
        scratch_bytes: u64,
    ) -> Self {
        let required_bytes = topology_reference_bytes
            .saturating_add(duplicate_topology_bytes)
            .saturating_add(sidecar_bytes)
            .saturating_add(catalog_metadata_bytes)
            .saturating_add(heap_bytes)
            .saturating_add(page_cache_bytes)
            .saturating_add(direct_io_buffer_bytes)
            .saturating_add(algorithm_state_bytes)
            .saturating_add(delta_overlay_bytes)
            .saturating_add(scratch_bytes);

        Self {
            required_bytes,
            topology_reference_bytes,
            duplicate_topology_bytes,
            sidecar_bytes,
            catalog_metadata_bytes,
            heap_bytes,
            page_cache_bytes,
            direct_io_buffer_bytes,
            algorithm_state_bytes,
            delta_overlay_bytes,
            scratch_bytes,
        }
    }

    pub fn projection(
        topology_reference_bytes: u64,
        sidecar_bytes: u64,
        catalog_metadata_bytes: u64,
        heap_bytes: u64,
        page_cache_bytes: u64,
    ) -> Self {
        Self::new(
            topology_reference_bytes,
            0,
            sidecar_bytes,
            catalog_metadata_bytes,
            heap_bytes,
            page_cache_bytes,
            0,
            0,
            0,
            0,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphProjectionMetadata {
    pub owner_user: String,
    pub database_name: String,
    pub snapshot_generation: u64,
    pub node_count: u64,
    pub relationship_count: u64,
    pub memory_estimate: MemoryEstimate,
    pub created_at_epoch_millis: u64,
}

impl GraphProjectionMetadata {
    pub fn new(
        owner_user: String,
        database_name: String,
        snapshot_generation: u64,
        node_count: u64,
        relationship_count: u64,
        memory_estimate: MemoryEstimate,
        created_at_epoch_millis: u64,
    ) -> Self {
        Self {
            owner_user,
            database_name,
            snapshot_generation,
            node_count,
            relationship_count,
            memory_estimate,
            created_at_epoch_millis,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphProjectionHandle {
    pub graph_name: String,
    pub owner_user: String,
    pub database_name: String,
    pub snapshot_generation: u64,
    pub node_count: u64,
    pub base_relationship_count: u64,
    pub projection_spec: GraphProjectionSpec,
    pub memory_estimate: MemoryEstimate,
    pub created_at_epoch_millis: u64,
    pub modified_at_epoch_millis: u64,
}

impl GraphProjectionHandle {
    pub fn logical_relationship_count(&self) -> u64 {
        match self.projection_spec.orientation {
            RelationshipOrientation::Natural | RelationshipOrientation::Reverse => {
                self.base_relationship_count
            }
            RelationshipOrientation::Undirected => self.base_relationship_count.saturating_mul(2),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct GraphProjectionCatalog {
    entries: BTreeMap<String, GraphProjectionHandle>,
}

impl GraphProjectionCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn exists(&self, graph_name: &str) -> bool {
        self.entries.contains_key(graph_name)
    }

    pub fn project(
        &mut self,
        spec: GraphProjectionSpec,
        metadata: GraphProjectionMetadata,
    ) -> Result<&GraphProjectionHandle, KnightBusError> {
        if self.entries.contains_key(spec.graph_name.as_str()) {
            return Err(KnightBusError::DuplicateGraphProjection {
                graph_name: spec.graph_name,
            });
        }

        let handle = GraphProjectionHandle {
            graph_name: spec.graph_name.clone(),
            owner_user: metadata.owner_user,
            database_name: metadata.database_name,
            snapshot_generation: metadata.snapshot_generation,
            node_count: metadata.node_count,
            base_relationship_count: metadata.relationship_count,
            projection_spec: spec,
            memory_estimate: metadata.memory_estimate,
            created_at_epoch_millis: metadata.created_at_epoch_millis,
            modified_at_epoch_millis: metadata.created_at_epoch_millis,
        };

        let graph_name = handle.graph_name.clone();
        self.entries.insert(graph_name.clone(), handle);
        Ok(self
            .entries
            .get(&graph_name)
            .expect("inserted handle exists"))
    }

    pub fn list(&self) -> Vec<&GraphProjectionHandle> {
        self.entries.values().collect()
    }

    pub fn get(&self, graph_name: &str) -> Result<&GraphProjectionHandle, KnightBusError> {
        self.entries
            .get(graph_name)
            .ok_or_else(|| KnightBusError::UnknownGraphProjection {
                graph_name: graph_name.to_owned(),
            })
    }

    pub fn size_of(&self, graph_name: &str) -> Result<(u64, u64), KnightBusError> {
        let handle = self.get(graph_name)?;
        Ok((handle.node_count, handle.base_relationship_count))
    }

    pub fn drop(&mut self, graph_name: &str) -> Result<GraphProjectionHandle, KnightBusError> {
        self.entries
            .remove(graph_name)
            .ok_or_else(|| KnightBusError::UnknownGraphProjection {
                graph_name: graph_name.to_owned(),
            })
    }
}

fn normalize_graph_name_now(graph_name: String) -> Result<String, KnightBusError> {
    let trimmed = graph_name.trim();
    if trimmed.is_empty() {
        return Err(KnightBusError::InvalidProjectionSelector {
            selector_kind: "graph_name",
            detail: "graph name must not be empty".to_owned(),
        });
    }
    Ok(trimmed.to_owned())
}

fn normalize_selector_values_now<I, S>(
    selector_kind: &'static str,
    values: I,
) -> Result<Vec<String>, KnightBusError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let normalized = values
        .into_iter()
        .map(|value| value.as_ref().trim().to_owned())
        .collect::<Vec<_>>();

    if normalized.is_empty() {
        return Err(KnightBusError::InvalidProjectionSelector {
            selector_kind,
            detail: "selector must include at least one value".to_owned(),
        });
    }

    if normalized.iter().any(|value| value.is_empty()) {
        return Err(KnightBusError::InvalidProjectionSelector {
            selector_kind,
            detail: "selector values must not be empty".to_owned(),
        });
    }

    Ok(normalized)
}
