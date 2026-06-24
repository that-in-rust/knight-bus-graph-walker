use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::{
    error::KnightBusError,
    gds::catalog::{
        GraphProjectionCatalog, GraphProjectionHandle, GraphProjectionMetadata, GraphProjectionSpec,
        MemoryEstimate,
    },
};

use super::{GdsEntryKind, GdsSupportStatus};

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(untagged)]
pub enum GdsExecutionValue {
    Null,
    Bool(bool),
    Integer(i64),
    Unsigned(u64),
    Float(f64),
    String(String),
    List(Vec<GdsExecutionValue>),
    Map(BTreeMap<String, GdsExecutionValue>),
}

impl GdsExecutionValue {
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Unsigned(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value.as_str()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct GdsExecutionRow {
    pub values: BTreeMap<String, GdsExecutionValue>,
}

impl GdsExecutionRow {
    pub fn new(values: BTreeMap<String, GdsExecutionValue>) -> Self {
        Self { values }
    }

    pub fn get(&self, key: &str) -> Option<&GdsExecutionValue> {
        self.values.get(key)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct GdsExecutionTable {
    pub columns: Vec<String>,
    pub rows: Vec<GdsExecutionRow>,
}

impl GdsExecutionTable {
    pub fn empty(columns: Vec<String>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum GdsExecutionResult {
    Table(GdsExecutionTable),
    Scalar(GdsExecutionValue),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GdsExecutionRequest {
    pub graph_name: Option<String>,
    pub fail_if_missing: bool,
    pub projection_spec: Option<GraphProjectionSpec>,
    pub projection_metadata: Option<GraphProjectionMetadata>,
    pub node_properties: Option<Vec<String>>,
    pub relationship_properties: Option<Vec<String>>,
    pub node_labels: Option<crate::gds::catalog::ProjectionSelector>,
    pub relationship_types: Option<crate::gds::catalog::ProjectionSelector>,
    pub list_node_labels: bool,
}

impl GdsExecutionRequest {
    pub fn graph_name_only(graph_name: impl Into<String>) -> Self {
        Self {
            graph_name: Some(graph_name.into()),
            fail_if_missing: true,
            projection_spec: None,
            projection_metadata: None,
            node_properties: None,
            relationship_properties: None,
            node_labels: None,
            relationship_types: None,
            list_node_labels: false,
        }
    }

    pub fn list_filter(graph_name: Option<String>) -> Self {
        Self {
            graph_name,
            fail_if_missing: true,
            projection_spec: None,
            projection_metadata: None,
            node_properties: None,
            relationship_properties: None,
            node_labels: None,
            relationship_types: None,
            list_node_labels: false,
        }
    }

    pub fn graph_project(
        projection_spec: GraphProjectionSpec,
        projection_metadata: GraphProjectionMetadata,
    ) -> Self {
        Self {
            graph_name: None,
            fail_if_missing: true,
            projection_spec: Some(projection_spec),
            projection_metadata: Some(projection_metadata),
            node_properties: None,
            relationship_properties: None,
            node_labels: None,
            relationship_types: None,
            list_node_labels: false,
        }
    }

    pub fn project_estimate(
        projection_spec: GraphProjectionSpec,
        projection_metadata: GraphProjectionMetadata,
    ) -> Self {
        Self {
            graph_name: None,
            fail_if_missing: true,
            projection_spec: Some(projection_spec),
            projection_metadata: Some(projection_metadata),
            node_properties: None,
            relationship_properties: None,
            node_labels: None,
            relationship_types: None,
            list_node_labels: false,
        }
    }

    pub fn node_properties_stream(
        graph_name: impl Into<String>,
        node_properties: Vec<String>,
        node_labels: crate::gds::catalog::ProjectionSelector,
    ) -> Self {
        Self {
            graph_name: Some(graph_name.into()),
            fail_if_missing: true,
            projection_spec: None,
            projection_metadata: None,
            node_properties: Some(node_properties),
            relationship_properties: None,
            node_labels: Some(node_labels),
            relationship_types: None,
            list_node_labels: false,
        }
    }

    pub fn relationship_properties_stream(
        graph_name: impl Into<String>,
        relationship_properties: Vec<String>,
        relationship_types: crate::gds::catalog::ProjectionSelector,
    ) -> Self {
        Self {
            graph_name: Some(graph_name.into()),
            fail_if_missing: true,
            projection_spec: None,
            projection_metadata: None,
            node_properties: None,
            relationship_properties: Some(relationship_properties),
            node_labels: None,
            relationship_types: Some(relationship_types),
            list_node_labels: false,
        }
    }

    pub fn with_fail_if_missing(mut self, fail_if_missing: bool) -> Self {
        self.fail_if_missing = fail_if_missing;
        self
    }

    pub fn with_list_node_labels(mut self, list_node_labels: bool) -> Self {
        self.list_node_labels = list_node_labels;
        self
    }
}

pub struct GdsExecutionContext<'a> {
    catalog: &'a mut GraphProjectionCatalog,
}

impl<'a> GdsExecutionContext<'a> {
    pub fn new(catalog: &'a mut GraphProjectionCatalog) -> Self {
        Self { catalog }
    }

    pub fn catalog(&self) -> &GraphProjectionCatalog {
        self.catalog
    }
}

pub fn built_in_gds_support_status_now(
    entry_kind: GdsEntryKind,
    name: &str,
) -> Option<GdsSupportStatus> {
    match (entry_kind, name) {
        (GdsEntryKind::Procedure, "gds.graph.project")
        | (GdsEntryKind::Procedure, "gds.graph.project.estimate")
        | (GdsEntryKind::Procedure, "gds.graph.nodeProperties.stream")
        | (GdsEntryKind::Procedure, "gds.graph.streamNodeProperties")
        | (GdsEntryKind::Procedure, "gds.graph.nodeProperty.stream")
        | (GdsEntryKind::Procedure, "gds.graph.streamNodeProperty")
        | (GdsEntryKind::Procedure, "gds.graph.exists")
        | (GdsEntryKind::Procedure, "gds.graph.list")
        | (GdsEntryKind::Procedure, "gds.graph.drop")
        | (GdsEntryKind::Procedure, "gds.graph.relationshipProperties.stream")
        | (GdsEntryKind::Procedure, "gds.graph.streamRelationshipProperties")
        | (GdsEntryKind::Procedure, "gds.graph.relationshipProperty.stream")
        | (GdsEntryKind::Procedure, "gds.graph.streamRelationshipProperty")
        | (GdsEntryKind::Procedure, "gds.internal.graph.sizeOf")
        | (GdsEntryKind::UserFunction, "gds.graph.exists") => {
            Some(GdsSupportStatus::P1ImplementedExactLowRam)
        }
        _ => None,
    }
}

pub fn execute_registered_gds_entry(
    context: &mut GdsExecutionContext<'_>,
    entry_kind: GdsEntryKind,
    name: &str,
    request: &GdsExecutionRequest,
) -> Result<GdsExecutionResult, KnightBusError> {
    match (entry_kind, name) {
        (GdsEntryKind::Procedure, "gds.graph.project") => {
            execute_graph_project_now(context, request)
        }
        (GdsEntryKind::Procedure, "gds.graph.project.estimate") => {
            execute_graph_project_estimate_now(context, request)
        }
        (GdsEntryKind::Procedure, "gds.graph.nodeProperties.stream")
        | (GdsEntryKind::Procedure, "gds.graph.streamNodeProperties") => {
            execute_graph_stream_node_properties_now(context, request)
        }
        (GdsEntryKind::Procedure, "gds.graph.nodeProperty.stream")
        | (GdsEntryKind::Procedure, "gds.graph.streamNodeProperty") => {
            execute_graph_stream_node_property_now(context, request)
        }
        (GdsEntryKind::Procedure, "gds.graph.exists") => execute_graph_exists_proc_now(context, request),
        (GdsEntryKind::UserFunction, "gds.graph.exists") => {
            execute_graph_exists_func_now(context, request)
        }
        (GdsEntryKind::Procedure, "gds.graph.list") => execute_graph_list_proc_now(context, request),
        (GdsEntryKind::Procedure, "gds.graph.drop") => execute_graph_drop_proc_now(context, request),
        (GdsEntryKind::Procedure, "gds.graph.relationshipProperties.stream")
        | (GdsEntryKind::Procedure, "gds.graph.streamRelationshipProperties") => {
            execute_graph_stream_relationship_properties_now(context, request)
        }
        (GdsEntryKind::Procedure, "gds.graph.relationshipProperty.stream")
        | (GdsEntryKind::Procedure, "gds.graph.streamRelationshipProperty") => {
            execute_graph_stream_relationship_property_now(context, request)
        }
        (GdsEntryKind::Procedure, "gds.internal.graph.sizeOf") => {
            execute_graph_size_of_proc_now(context, request)
        }
        _ => Err(KnightBusError::UnsupportedRegisteredGdsEntry {
            entry_kind: entry_kind.label().to_owned(),
            name: name.to_owned(),
            support_status: GdsSupportStatus::NeedsArchitectureSpike.label().to_owned(),
        }),
    }
}

pub fn execute_registered_gds_procedure(
    context: &mut GdsExecutionContext<'_>,
    name: &str,
    request: &GdsExecutionRequest,
) -> Result<GdsExecutionResult, KnightBusError> {
    execute_registered_gds_entry(context, GdsEntryKind::Procedure, name, request)
}

pub fn execute_registered_gds_user_function(
    context: &mut GdsExecutionContext<'_>,
    name: &str,
    request: &GdsExecutionRequest,
) -> Result<GdsExecutionResult, KnightBusError> {
    execute_registered_gds_entry(context, GdsEntryKind::UserFunction, name, request)
}

fn execute_graph_project_now(
    context: &mut GdsExecutionContext<'_>,
    request: &GdsExecutionRequest,
) -> Result<GdsExecutionResult, KnightBusError> {
    let (spec, metadata) = required_projection_inputs_now("gds.graph.project", request)?;

    let handle = context.catalog.project(spec, metadata)?.clone();
    let row = project_result_row_now(&handle);

    Ok(GdsExecutionResult::Table(GdsExecutionTable {
        columns: vec![
            "graphName".to_owned(),
            "nodeProjection".to_owned(),
            "relationshipProjection".to_owned(),
            "nodeCount".to_owned(),
            "relationshipCount".to_owned(),
            "projectMillis".to_owned(),
        ],
        rows: vec![row],
    }))
}

fn execute_graph_project_estimate_now(
    _context: &mut GdsExecutionContext<'_>,
    request: &GdsExecutionRequest,
) -> Result<GdsExecutionResult, KnightBusError> {
    let (_spec, metadata) = required_projection_inputs_now("gds.graph.project.estimate", request)?;
    let row = project_estimate_row_now(&metadata);

    Ok(GdsExecutionResult::Table(GdsExecutionTable {
        columns: vec![
            "requiredMemory".to_owned(),
            "treeView".to_owned(),
            "mapView".to_owned(),
            "bytesMin".to_owned(),
            "bytesMax".to_owned(),
            "nodeCount".to_owned(),
            "relationshipCount".to_owned(),
            "heapPercentageMin".to_owned(),
            "heapPercentageMax".to_owned(),
        ],
        rows: vec![row],
    }))
}

fn execute_graph_exists_proc_now(
    context: &mut GdsExecutionContext<'_>,
    request: &GdsExecutionRequest,
) -> Result<GdsExecutionResult, KnightBusError> {
    let graph_name = required_graph_name_now("gds.graph.exists", request)?;
    let exists = context.catalog.exists(graph_name);

    let mut row = BTreeMap::new();
    row.insert(
        "graphName".to_owned(),
        GdsExecutionValue::String(graph_name.to_owned()),
    );
    row.insert("exists".to_owned(), GdsExecutionValue::Bool(exists));

    Ok(GdsExecutionResult::Table(GdsExecutionTable {
        columns: vec!["graphName".to_owned(), "exists".to_owned()],
        rows: vec![GdsExecutionRow::new(row)],
    }))
}

fn execute_graph_stream_node_properties_now(
    context: &mut GdsExecutionContext<'_>,
    request: &GdsExecutionRequest,
) -> Result<GdsExecutionResult, KnightBusError> {
    let graph_name = required_graph_name_now("gds.graph.nodeProperties.stream", request)?;
    let property_names = required_node_properties_now("gds.graph.nodeProperties.stream", request)?;
    let node_labels = request
        .node_labels
        .clone()
        .unwrap_or(crate::gds::catalog::ProjectionSelector::All);

    let streamed_rows = context.catalog.stream_node_properties(
        graph_name,
        property_names,
        &node_labels,
        request.list_node_labels,
    )?;

    let rows = streamed_rows
        .into_iter()
        .map(|row| {
            let mut values = BTreeMap::new();
            values.insert("nodeId".to_owned(), GdsExecutionValue::Unsigned(row.node_id));
            values.insert(
                "nodeProperty".to_owned(),
                GdsExecutionValue::String(row.node_property),
            );
            values.insert(
                "propertyValue".to_owned(),
                json_value_to_execution_value_now(row.property_value),
            );
            values.insert(
                "nodeLabels".to_owned(),
                GdsExecutionValue::List(
                    row.node_labels
                        .into_iter()
                        .map(GdsExecutionValue::String)
                        .collect(),
                ),
            );
            GdsExecutionRow::new(values)
        })
        .collect::<Vec<_>>();

    Ok(GdsExecutionResult::Table(GdsExecutionTable {
        columns: vec![
            "nodeId".to_owned(),
            "nodeProperty".to_owned(),
            "propertyValue".to_owned(),
            "nodeLabels".to_owned(),
        ],
        rows,
    }))
}

fn execute_graph_stream_node_property_now(
    context: &mut GdsExecutionContext<'_>,
    request: &GdsExecutionRequest,
) -> Result<GdsExecutionResult, KnightBusError> {
    let graph_name = required_graph_name_now("gds.graph.nodeProperty.stream", request)?;
    let property_name = required_single_node_property_now("gds.graph.nodeProperty.stream", request)?;
    let node_labels = request
        .node_labels
        .clone()
        .unwrap_or(crate::gds::catalog::ProjectionSelector::All);

    let streamed_rows = context.catalog.stream_node_properties(
        graph_name,
        std::slice::from_ref(&property_name),
        &node_labels,
        request.list_node_labels,
    )?;

    let rows = streamed_rows
        .into_iter()
        .map(|row| {
            let mut values = BTreeMap::new();
            values.insert("nodeId".to_owned(), GdsExecutionValue::Unsigned(row.node_id));
            values.insert(
                "propertyValue".to_owned(),
                json_value_to_execution_value_now(row.property_value),
            );
            values.insert(
                "nodeLabels".to_owned(),
                GdsExecutionValue::List(
                    row.node_labels
                        .into_iter()
                        .map(GdsExecutionValue::String)
                        .collect(),
                ),
            );
            GdsExecutionRow::new(values)
        })
        .collect::<Vec<_>>();

    Ok(GdsExecutionResult::Table(GdsExecutionTable {
        columns: vec![
            "nodeId".to_owned(),
            "propertyValue".to_owned(),
            "nodeLabels".to_owned(),
        ],
        rows,
    }))
}

fn execute_graph_exists_func_now(
    context: &mut GdsExecutionContext<'_>,
    request: &GdsExecutionRequest,
) -> Result<GdsExecutionResult, KnightBusError> {
    let graph_name = required_graph_name_now("gds.graph.exists", request)?;
    Ok(GdsExecutionResult::Scalar(GdsExecutionValue::Bool(
        context.catalog.exists(graph_name),
    )))
}

fn execute_graph_stream_relationship_properties_now(
    context: &mut GdsExecutionContext<'_>,
    request: &GdsExecutionRequest,
) -> Result<GdsExecutionResult, KnightBusError> {
    let graph_name =
        required_graph_name_now("gds.graph.relationshipProperties.stream", request)?;
    let property_names = required_relationship_properties_now(
        "gds.graph.relationshipProperties.stream",
        request,
    )?;
    let relationship_types = request
        .relationship_types
        .clone()
        .unwrap_or(crate::gds::catalog::ProjectionSelector::All);

    let streamed_rows = context.catalog.stream_relationship_properties(
        graph_name,
        property_names,
        &relationship_types,
    )?;

    let rows = streamed_rows
        .into_iter()
        .map(|row| {
            let mut values = BTreeMap::new();
            values.insert(
                "sourceNodeId".to_owned(),
                GdsExecutionValue::Unsigned(row.source_node_id),
            );
            values.insert(
                "targetNodeId".to_owned(),
                GdsExecutionValue::Unsigned(row.target_node_id),
            );
            values.insert(
                "relationshipType".to_owned(),
                GdsExecutionValue::String(row.relationship_type),
            );
            values.insert(
                "relationshipProperty".to_owned(),
                GdsExecutionValue::String(row.relationship_property),
            );
            values.insert(
                "propertyValue".to_owned(),
                json_value_to_execution_value_now(row.property_value),
            );
            GdsExecutionRow::new(values)
        })
        .collect::<Vec<_>>();

    Ok(GdsExecutionResult::Table(GdsExecutionTable {
        columns: vec![
            "sourceNodeId".to_owned(),
            "targetNodeId".to_owned(),
            "relationshipType".to_owned(),
            "relationshipProperty".to_owned(),
            "propertyValue".to_owned(),
        ],
        rows,
    }))
}

fn execute_graph_stream_relationship_property_now(
    context: &mut GdsExecutionContext<'_>,
    request: &GdsExecutionRequest,
) -> Result<GdsExecutionResult, KnightBusError> {
    let graph_name =
        required_graph_name_now("gds.graph.relationshipProperty.stream", request)?;
    let property_name = required_single_relationship_property_now(
        "gds.graph.relationshipProperty.stream",
        request,
    )?;
    let relationship_types = request
        .relationship_types
        .clone()
        .unwrap_or(crate::gds::catalog::ProjectionSelector::All);

    let streamed_rows = context.catalog.stream_relationship_properties(
        graph_name,
        std::slice::from_ref(&property_name),
        &relationship_types,
    )?;

    let rows = streamed_rows
        .into_iter()
        .map(|row| {
            let mut values = BTreeMap::new();
            values.insert(
                "sourceNodeId".to_owned(),
                GdsExecutionValue::Unsigned(row.source_node_id),
            );
            values.insert(
                "targetNodeId".to_owned(),
                GdsExecutionValue::Unsigned(row.target_node_id),
            );
            values.insert(
                "relationshipType".to_owned(),
                GdsExecutionValue::String(row.relationship_type),
            );
            values.insert(
                "propertyValue".to_owned(),
                json_value_to_execution_value_now(row.property_value),
            );
            GdsExecutionRow::new(values)
        })
        .collect::<Vec<_>>();

    Ok(GdsExecutionResult::Table(GdsExecutionTable {
        columns: vec![
            "sourceNodeId".to_owned(),
            "targetNodeId".to_owned(),
            "relationshipType".to_owned(),
            "propertyValue".to_owned(),
        ],
        rows,
    }))
}

fn execute_graph_list_proc_now(
    context: &mut GdsExecutionContext<'_>,
    request: &GdsExecutionRequest,
) -> Result<GdsExecutionResult, KnightBusError> {
    let filter_name = request.graph_name.as_deref();

    let rows = context
        .catalog()
        .list()
        .into_iter()
        .filter(|handle| match filter_name {
            Some(graph_name) => handle.graph_name == graph_name,
            None => true,
        })
        .map(graph_info_row_now)
        .collect::<Vec<_>>();

    Ok(GdsExecutionResult::Table(GdsExecutionTable {
        columns: graph_info_columns_now(),
        rows,
    }))
}

fn execute_graph_drop_proc_now(
    context: &mut GdsExecutionContext<'_>,
    request: &GdsExecutionRequest,
) -> Result<GdsExecutionResult, KnightBusError> {
    let graph_name = required_graph_name_now("gds.graph.drop", request)?;
    match context.catalog.drop(graph_name) {
        Ok(handle) => Ok(GdsExecutionResult::Table(GdsExecutionTable {
            columns: graph_info_columns_now(),
            rows: vec![graph_info_row_now(&handle)],
        })),
        Err(KnightBusError::UnknownGraphProjection { .. }) if !request.fail_if_missing => {
            Ok(GdsExecutionResult::Table(GdsExecutionTable::empty(
                graph_info_columns_now(),
            )))
        }
        Err(error) => Err(error),
    }
}

fn execute_graph_size_of_proc_now(
    context: &mut GdsExecutionContext<'_>,
    request: &GdsExecutionRequest,
) -> Result<GdsExecutionResult, KnightBusError> {
    let graph_name = required_graph_name_now("gds.internal.graph.sizeOf", request)?;
    let handle = context.catalog.get(graph_name)?;
    let row = graph_memory_usage_row_now(handle);

    Ok(GdsExecutionResult::Table(GdsExecutionTable {
        columns: vec![
            "graphName".to_owned(),
            "memoryUsage".to_owned(),
            "sizeInBytes".to_owned(),
            "detailSizeInBytes".to_owned(),
            "nodeCount".to_owned(),
            "relationshipCount".to_owned(),
        ],
        rows: vec![row],
    }))
}

fn required_graph_name_now<'a>(
    procedure_name: &str,
    request: &'a GdsExecutionRequest,
) -> Result<&'a str, KnightBusError> {
    request
        .graph_name
        .as_deref()
        .ok_or_else(|| invalid_gds_invocation_now(procedure_name, "graph name is required"))
}

fn required_projection_inputs_now(
    procedure_name: &str,
    request: &GdsExecutionRequest,
) -> Result<(GraphProjectionSpec, GraphProjectionMetadata), KnightBusError> {
    let spec = request.projection_spec.clone().ok_or_else(|| {
        invalid_gds_invocation_now(procedure_name, "projection spec is required")
    })?;
    let metadata = request.projection_metadata.clone().ok_or_else(|| {
        invalid_gds_invocation_now(procedure_name, "projection metadata is required")
    })?;

    Ok((spec, metadata))
}

fn required_node_properties_now<'a>(
    procedure_name: &str,
    request: &'a GdsExecutionRequest,
) -> Result<&'a [String], KnightBusError> {
    request
        .node_properties
        .as_deref()
        .filter(|properties| !properties.is_empty())
        .ok_or_else(|| invalid_gds_invocation_now(procedure_name, "node properties are required"))
}

fn required_single_node_property_now(
    procedure_name: &str,
    request: &GdsExecutionRequest,
) -> Result<String, KnightBusError> {
    let property_names = required_node_properties_now(procedure_name, request)?;
    if property_names.len() != 1 {
        return Err(invalid_gds_invocation_now(
            procedure_name,
            "exactly one node property is required",
        ));
    }
    Ok(property_names[0].clone())
}

fn required_relationship_properties_now<'a>(
    procedure_name: &str,
    request: &'a GdsExecutionRequest,
) -> Result<&'a [String], KnightBusError> {
    request
        .relationship_properties
        .as_deref()
        .filter(|properties| !properties.is_empty())
        .ok_or_else(|| {
            invalid_gds_invocation_now(procedure_name, "relationship properties are required")
        })
}

fn required_single_relationship_property_now(
    procedure_name: &str,
    request: &GdsExecutionRequest,
) -> Result<String, KnightBusError> {
    let property_names = required_relationship_properties_now(procedure_name, request)?;
    if property_names.len() != 1 {
        return Err(invalid_gds_invocation_now(
            procedure_name,
            "exactly one relationship property is required",
        ));
    }
    Ok(property_names[0].clone())
}

fn invalid_gds_invocation_now(name: &str, detail: &str) -> KnightBusError {
    KnightBusError::InvalidGdsInvocation {
        name: name.to_owned(),
        detail: detail.to_owned(),
    }
}

fn json_value_to_execution_value_now(value: JsonValue) -> GdsExecutionValue {
    match value {
        JsonValue::Null => GdsExecutionValue::Null,
        JsonValue::Bool(value) => GdsExecutionValue::Bool(value),
        JsonValue::Number(number) => {
            if let Some(value) = number.as_u64() {
                GdsExecutionValue::Unsigned(value)
            } else if let Some(value) = number.as_i64() {
                GdsExecutionValue::Integer(value)
            } else if let Some(value) = number.as_f64() {
                GdsExecutionValue::Float(value)
            } else {
                GdsExecutionValue::String(number.to_string())
            }
        }
        JsonValue::String(value) => GdsExecutionValue::String(value),
        JsonValue::Array(values) => GdsExecutionValue::List(
            values
                .into_iter()
                .map(json_value_to_execution_value_now)
                .collect(),
        ),
        JsonValue::Object(map) => GdsExecutionValue::Map(
            map.into_iter()
                .map(|(key, value)| (key, json_value_to_execution_value_now(value)))
                .collect(),
        ),
    }
}

fn project_result_row_now(handle: &GraphProjectionHandle) -> GdsExecutionRow {
    let mut row = BTreeMap::new();
    row.insert(
        "graphName".to_owned(),
        GdsExecutionValue::String(handle.graph_name.clone()),
    );
    row.insert(
        "nodeProjection".to_owned(),
        selector_value_now(&handle.projection_spec.node_projection),
    );
    row.insert(
        "relationshipProjection".to_owned(),
        selector_value_now(&handle.projection_spec.relationship_projection),
    );
    row.insert(
        "nodeCount".to_owned(),
        GdsExecutionValue::Unsigned(handle.node_count),
    );
    row.insert(
        "relationshipCount".to_owned(),
        GdsExecutionValue::Unsigned(handle.logical_relationship_count()),
    );
    row.insert("projectMillis".to_owned(), GdsExecutionValue::Unsigned(0));
    GdsExecutionRow::new(row)
}

fn graph_info_columns_now() -> Vec<String> {
    vec![
        "graphName".to_owned(),
        "database".to_owned(),
        "databaseLocation".to_owned(),
        "memoryUsage".to_owned(),
        "sizeInBytes".to_owned(),
        "nodeCount".to_owned(),
        "relationshipCount".to_owned(),
        "configuration".to_owned(),
        "density".to_owned(),
        "creationTime".to_owned(),
        "modificationTime".to_owned(),
        "schema".to_owned(),
        "schemaWithOrientation".to_owned(),
        "degreeDistribution".to_owned(),
    ]
}

fn graph_info_row_now(handle: &GraphProjectionHandle) -> GdsExecutionRow {
    let mut row = BTreeMap::new();
    let relationship_count = handle.logical_relationship_count();
    let size_in_bytes = handle.memory_estimate.required_bytes;

    row.insert(
        "graphName".to_owned(),
        GdsExecutionValue::String(handle.graph_name.clone()),
    );
    row.insert(
        "database".to_owned(),
        GdsExecutionValue::String(handle.database_name.clone()),
    );
    row.insert(
        "databaseLocation".to_owned(),
        GdsExecutionValue::String("local".to_owned()),
    );
    row.insert(
        "memoryUsage".to_owned(),
        GdsExecutionValue::String(format_bytes_human_readable_now(size_in_bytes)),
    );
    row.insert("sizeInBytes".to_owned(), GdsExecutionValue::Unsigned(size_in_bytes));
    row.insert(
        "nodeCount".to_owned(),
        GdsExecutionValue::Unsigned(handle.node_count),
    );
    row.insert(
        "relationshipCount".to_owned(),
        GdsExecutionValue::Unsigned(relationship_count),
    );
    row.insert(
        "configuration".to_owned(),
        projection_configuration_value_now(handle),
    );
    row.insert(
        "density".to_owned(),
        GdsExecutionValue::Float(density_value_now(handle.node_count, relationship_count)),
    );
    row.insert(
        "creationTime".to_owned(),
        GdsExecutionValue::Unsigned(handle.created_at_epoch_millis),
    );
    row.insert(
        "modificationTime".to_owned(),
        GdsExecutionValue::Unsigned(handle.modified_at_epoch_millis),
    );
    row.insert("schema".to_owned(), projection_schema_value_now(handle, false));
    row.insert(
        "schemaWithOrientation".to_owned(),
        projection_schema_value_now(handle, true),
    );
    row.insert(
        "degreeDistribution".to_owned(),
        GdsExecutionValue::Map(BTreeMap::new()),
    );

    GdsExecutionRow::new(row)
}

fn graph_memory_usage_row_now(handle: &GraphProjectionHandle) -> GdsExecutionRow {
    let detail = memory_estimate_detail_map_now(&handle.memory_estimate);

    let mut row = BTreeMap::new();
    row.insert(
        "graphName".to_owned(),
        GdsExecutionValue::String(handle.graph_name.clone()),
    );
    row.insert(
        "memoryUsage".to_owned(),
        GdsExecutionValue::String(format_bytes_human_readable_now(
            handle.memory_estimate.required_bytes,
        )),
    );
    row.insert(
        "sizeInBytes".to_owned(),
        GdsExecutionValue::Unsigned(handle.memory_estimate.required_bytes),
    );
    row.insert("detailSizeInBytes".to_owned(), GdsExecutionValue::Map(detail));
    row.insert(
        "nodeCount".to_owned(),
        GdsExecutionValue::Unsigned(handle.node_count),
    );
    row.insert(
        "relationshipCount".to_owned(),
        GdsExecutionValue::Unsigned(handle.logical_relationship_count()),
    );
    GdsExecutionRow::new(row)
}

fn project_estimate_row_now(metadata: &GraphProjectionMetadata) -> GdsExecutionRow {
    let required_bytes = metadata.memory_estimate.required_bytes;
    let heap_fraction = heap_percentage_now(required_bytes);

    let mut row = BTreeMap::new();
    row.insert(
        "requiredMemory".to_owned(),
        GdsExecutionValue::String(format_bytes_human_readable_now(required_bytes)),
    );
    row.insert(
        "treeView".to_owned(),
        GdsExecutionValue::String(memory_estimate_tree_view_now(&metadata.memory_estimate)),
    );
    row.insert(
        "mapView".to_owned(),
        GdsExecutionValue::Map(memory_estimate_detail_map_now(&metadata.memory_estimate)),
    );
    row.insert("bytesMin".to_owned(), GdsExecutionValue::Unsigned(required_bytes));
    row.insert("bytesMax".to_owned(), GdsExecutionValue::Unsigned(required_bytes));
    row.insert(
        "nodeCount".to_owned(),
        GdsExecutionValue::Unsigned(metadata.node_count),
    );
    row.insert(
        "relationshipCount".to_owned(),
        GdsExecutionValue::Unsigned(metadata.relationship_count),
    );
    row.insert(
        "heapPercentageMin".to_owned(),
        GdsExecutionValue::Float(heap_fraction),
    );
    row.insert(
        "heapPercentageMax".to_owned(),
        GdsExecutionValue::Float(heap_fraction),
    );

    GdsExecutionRow::new(row)
}

fn memory_estimate_detail_map_now(
    estimate: &MemoryEstimate,
) -> BTreeMap<String, GdsExecutionValue> {
    let mut detail = BTreeMap::new();
    detail.insert(
        "topologyReferenceBytes".to_owned(),
        GdsExecutionValue::Unsigned(estimate.topology_reference_bytes),
    );
    detail.insert(
        "duplicateTopologyBytes".to_owned(),
        GdsExecutionValue::Unsigned(estimate.duplicate_topology_bytes),
    );
    detail.insert(
        "sidecarBytes".to_owned(),
        GdsExecutionValue::Unsigned(estimate.sidecar_bytes),
    );
    detail.insert(
        "catalogMetadataBytes".to_owned(),
        GdsExecutionValue::Unsigned(estimate.catalog_metadata_bytes),
    );
    detail.insert(
        "heapBytes".to_owned(),
        GdsExecutionValue::Unsigned(estimate.heap_bytes),
    );
    detail.insert(
        "pageCacheBytes".to_owned(),
        GdsExecutionValue::Unsigned(estimate.page_cache_bytes),
    );
    detail.insert(
        "directIoBufferBytes".to_owned(),
        GdsExecutionValue::Unsigned(estimate.direct_io_buffer_bytes),
    );
    detail.insert(
        "algorithmStateBytes".to_owned(),
        GdsExecutionValue::Unsigned(estimate.algorithm_state_bytes),
    );
    detail.insert(
        "deltaOverlayBytes".to_owned(),
        GdsExecutionValue::Unsigned(estimate.delta_overlay_bytes),
    );
    detail.insert(
        "scratchBytes".to_owned(),
        GdsExecutionValue::Unsigned(estimate.scratch_bytes),
    );
    detail
}

fn memory_estimate_tree_view_now(estimate: &MemoryEstimate) -> String {
    [
        format!(
            "requiredBytes: {}",
            format_bytes_human_readable_now(estimate.required_bytes)
        ),
        format!(
            "topologyReferenceBytes: {}",
            format_bytes_human_readable_now(estimate.topology_reference_bytes)
        ),
        format!(
            "duplicateTopologyBytes: {}",
            format_bytes_human_readable_now(estimate.duplicate_topology_bytes)
        ),
        format!(
            "sidecarBytes: {}",
            format_bytes_human_readable_now(estimate.sidecar_bytes)
        ),
        format!(
            "catalogMetadataBytes: {}",
            format_bytes_human_readable_now(estimate.catalog_metadata_bytes)
        ),
        format!(
            "heapBytes: {}",
            format_bytes_human_readable_now(estimate.heap_bytes)
        ),
        format!(
            "pageCacheBytes: {}",
            format_bytes_human_readable_now(estimate.page_cache_bytes)
        ),
        format!(
            "directIoBufferBytes: {}",
            format_bytes_human_readable_now(estimate.direct_io_buffer_bytes)
        ),
        format!(
            "algorithmStateBytes: {}",
            format_bytes_human_readable_now(estimate.algorithm_state_bytes)
        ),
        format!(
            "deltaOverlayBytes: {}",
            format_bytes_human_readable_now(estimate.delta_overlay_bytes)
        ),
        format!(
            "scratchBytes: {}",
            format_bytes_human_readable_now(estimate.scratch_bytes)
        ),
    ]
    .join("\n")
}

fn heap_percentage_now(required_bytes: u64) -> f64 {
    let heap_size = std::cmp::max(1u64, Runtime::max_memory_hint_now());
    (required_bytes as f64) / (heap_size as f64)
}

struct Runtime;

impl Runtime {
    fn max_memory_hint_now() -> u64 {
        // There is no JVM-style configured heap cap here yet, so use the current
        // process address-space hint exposed by Rust's stdlib environment.
        // This keeps the estimate deterministic for tests while reserving the
        // field shape expected by the GDS memory-estimate surface.
        #[cfg(target_pointer_width = "64")]
        {
            u64::MAX >> 16
        }

        #[cfg(not(target_pointer_width = "64"))]
        {
            usize::MAX as u64
        }
    }
}

fn selector_value_now(selector: &crate::gds::catalog::ProjectionSelector) -> GdsExecutionValue {
    match selector {
        crate::gds::catalog::ProjectionSelector::All => GdsExecutionValue::String("ALL".to_owned()),
        crate::gds::catalog::ProjectionSelector::Named(values) => GdsExecutionValue::List(
            values
                .iter()
                .cloned()
                .map(GdsExecutionValue::String)
                .collect(),
        ),
    }
}

fn property_selector_value_now(selector: &crate::gds::catalog::PropertySelector) -> GdsExecutionValue {
    match selector {
        crate::gds::catalog::PropertySelector::None => GdsExecutionValue::String("NONE".to_owned()),
        crate::gds::catalog::PropertySelector::All => GdsExecutionValue::String("ALL".to_owned()),
        crate::gds::catalog::PropertySelector::Named(values) => GdsExecutionValue::List(
            values
                .iter()
                .cloned()
                .map(GdsExecutionValue::String)
                .collect(),
        ),
    }
}

fn projection_configuration_value_now(handle: &GraphProjectionHandle) -> GdsExecutionValue {
    let mut configuration = BTreeMap::new();
    configuration.insert(
        "nodeProjection".to_owned(),
        selector_value_now(&handle.projection_spec.node_projection),
    );
    configuration.insert(
        "relationshipProjection".to_owned(),
        selector_value_now(&handle.projection_spec.relationship_projection),
    );
    configuration.insert(
        "orientation".to_owned(),
        GdsExecutionValue::String(handle.projection_spec.orientation.label().to_owned()),
    );
    configuration.insert(
        "nodeProperties".to_owned(),
        property_selector_value_now(&handle.projection_spec.node_properties),
    );
    configuration.insert(
        "relationshipProperties".to_owned(),
        property_selector_value_now(&handle.projection_spec.relationship_properties),
    );
    GdsExecutionValue::Map(configuration)
}

fn projection_schema_value_now(handle: &GraphProjectionHandle, include_orientation: bool) -> GdsExecutionValue {
    let mut schema = BTreeMap::new();
    schema.insert(
        "nodeLabels".to_owned(),
        selector_value_now(&handle.projection_spec.node_projection),
    );
    schema.insert(
        "relationshipTypes".to_owned(),
        selector_value_now(&handle.projection_spec.relationship_projection),
    );
    schema.insert(
        "nodeProperties".to_owned(),
        property_selector_value_now(&handle.projection_spec.node_properties),
    );
    schema.insert(
        "relationshipProperties".to_owned(),
        property_selector_value_now(&handle.projection_spec.relationship_properties),
    );
    if include_orientation {
        schema.insert(
            "orientation".to_owned(),
            GdsExecutionValue::String(handle.projection_spec.orientation.label().to_owned()),
        );
    }
    GdsExecutionValue::Map(schema)
}

fn density_value_now(node_count: u64, relationship_count: u64) -> f64 {
    if node_count <= 1 {
        return 0.0;
    }
    let denominator = (node_count as f64) * ((node_count - 1) as f64);
    (relationship_count as f64) / denominator
}

fn format_bytes_human_readable_now(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];

    let mut value = bytes as f64;
    let mut unit_index = 0usize;

    while value >= 1024.0 && unit_index < UNITS.len() - 1 {
        value /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{bytes} {}", UNITS[unit_index])
    } else {
        format!("{value:.1} {}", UNITS[unit_index])
    }
}
