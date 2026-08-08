use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use grafeo_adapters::query::cypher::{
    Clause, Direction, Expression, Pattern, ReturnItems, SortDirection, Statement, parse,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    error::KnightBusError,
    runtime::{GraphAdjacencyRuntime, MmapWalkRuntime},
    types::{HopCount, NodeKey, WalkDirection},
};

pub const NEIGHBORHOOD_WALK_PROFILE_VERSION: &str = "knight-bus-neighborhood-walk-v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CypherParameterValue {
    Null,
    Boolean(bool),
    Integer(i64),
    String(String),
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NeighborhoodProjection {
    NodeIdString,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NeighborhoodOrdering {
    NodeIdAscending,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledNeighborhoodWalkPlan {
    pub profile_version: String,
    pub start_node_id: String,
    pub direction: WalkDirection,
    pub minimum_hops: HopCount,
    pub maximum_hops: HopCount,
    pub relationship_type: String,
    pub projection: NeighborhoodProjection,
    pub distinct: bool,
    pub ordering: NeighborhoodOrdering,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectedBoltResultRecord {
    pub node_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NeighborhoodWalkResult {
    pub columns: Vec<String>,
    pub records: Vec<ProjectedBoltResultRecord>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NeighborhoodTerminationReason {
    DeadlineExceeded,
    ResultRowLimitExceeded,
    ClientCancelled,
}

#[derive(Clone, Debug)]
pub struct NeighborhoodExecutionLimits {
    pub deadline: Option<Instant>,
    pub maximum_result_rows: Option<usize>,
    pub cancellation_flag: Arc<AtomicBool>,
}

impl Default for NeighborhoodExecutionLimits {
    fn default() -> Self {
        Self {
            deadline: None,
            maximum_result_rows: None,
            cancellation_flag: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum CypherWalkError {
    #[error("invalid Cypher syntax: {message}")]
    Syntax { message: String },
    #[error("unsupported Cypher feature `{feature}`: {message}")]
    UnsupportedFeature { feature: String, message: String },
    #[error("invalid parameter `${name}`: {message}")]
    InvalidParameter { name: String, message: String },
    #[error("neighborhood walk execution failed: {message}")]
    Execution { message: String },
    #[error("neighborhood walk terminated: {reason:?}")]
    Terminated {
        reason: NeighborhoodTerminationReason,
    },
}

pub fn compile_neighborhood_walk_plan(
    query_text: &str,
    parameters: &BTreeMap<String, CypherParameterValue>,
) -> Result<CompiledNeighborhoodWalkPlan, CypherWalkError> {
    let statement = parse(query_text).map_err(|error| CypherWalkError::Syntax {
        message: error.to_string(),
    })?;
    let query = match statement {
        Statement::Query(query) => query,
        _ => {
            return Err(unsupported_feature(
                "statement",
                "only read queries are supported",
            ));
        }
    };
    for clause in &query.clauses {
        let unsupported = match clause {
            Clause::OptionalMatch(_) => Some(("optional_match", "OPTIONAL MATCH is not supported")),
            Clause::Where(_) => Some(("where_clause", "property predicates are not supported")),
            Clause::With(_) => Some(("with_clause", "WITH is not supported")),
            Clause::Unwind(_) => Some(("unwind_clause", "UNWIND is not supported")),
            Clause::Skip(_) => Some(("skip_clause", "SKIP is not supported")),
            Clause::Limit(_) => Some(("limit_clause", "Cypher LIMIT is not supported")),
            Clause::Create(_)
            | Clause::Merge(_)
            | Clause::Delete(_)
            | Clause::Set(_)
            | Clause::Remove(_)
            | Clause::ForEach(_)
            | Clause::LoadCsv(_) => Some(("write_clause", "write clauses are not supported")),
            Clause::Call(_) | Clause::CallSubquery(_) => Some((
                "call_clause",
                "procedure and subquery calls are not supported",
            )),
            Clause::Match(_) | Clause::Return(_) | Clause::OrderBy(_) => None,
        };
        if let Some((feature, message)) = unsupported {
            return Err(unsupported_feature(feature, message));
        }
    }
    if query.clauses.len() != 3 {
        return Err(unsupported_feature(
            "clause_shape",
            "expected exactly MATCH, RETURN, and ORDER BY",
        ));
    }

    let match_clause = match &query.clauses[0] {
        Clause::Match(clause) => clause,
        _ => return Err(unsupported_feature("first_clause", "expected MATCH")),
    };
    let return_clause = match &query.clauses[1] {
        Clause::Return(clause) => clause,
        _ => return Err(unsupported_feature("second_clause", "expected RETURN")),
    };
    let order_clause = match &query.clauses[2] {
        Clause::OrderBy(clause) => clause,
        _ => return Err(unsupported_feature("third_clause", "expected ORDER BY")),
    };

    if match_clause.patterns.len() != 1 {
        return Err(unsupported_feature(
            "match_pattern_count",
            "expected one path pattern",
        ));
    }
    let path = match &match_clause.patterns[0] {
        Pattern::Path(path) => path,
        _ => {
            return Err(unsupported_feature(
                "match_pattern",
                "expected one path pattern",
            ));
        }
    };
    if path.chain.len() != 1 {
        return Err(unsupported_feature(
            "relationship_chain",
            "expected exactly one relationship pattern",
        ));
    }
    let relationship = &path.chain[0];

    let start_variable = path
        .start
        .variable
        .as_deref()
        .ok_or_else(|| unsupported_feature("start_variable", "start node must be named"))?;
    let target_variable = relationship
        .target
        .variable
        .as_deref()
        .ok_or_else(|| unsupported_feature("target_variable", "target node must be named"))?;
    if !labels_match_graph_profile(&path.start.labels)
        || !labels_match_graph_profile(&relationship.target.labels)
    {
        return Err(unsupported_feature(
            "node_label",
            "only the optional Entity label is supported by the fixed graph profile",
        ));
    }
    if !relationship.target.properties.is_empty() {
        return Err(unsupported_feature(
            "target_property",
            "target node property matching is not supported",
        ));
    }
    if path.start.properties.len() != 1 {
        return Err(unsupported_feature(
            "start_property",
            "expected only node_id: $node_id on the start node",
        ));
    }
    let (property_name, property_value) = &path.start.properties[0];
    if property_name != "node_id"
        || !matches!(property_value, Expression::Parameter(name) if name == "node_id")
    {
        return Err(unsupported_feature(
            "start_property",
            "expected exactly node_id: $node_id",
        ));
    }

    if relationship.variable.is_some()
        || !relationship.properties.is_empty()
        || relationship.where_clause.is_some()
    {
        return Err(unsupported_feature(
            "relationship_expression",
            "relationship variables, properties, and predicates are not supported",
        ));
    }
    if relationship.types.len() != 1 || relationship.types[0] != "DEPENDS_ON" {
        return Err(unsupported_feature(
            "relationship_type",
            "expected exactly DEPENDS_ON",
        ));
    }

    let direction = match relationship.direction {
        Direction::Outgoing => WalkDirection::Forward,
        Direction::Incoming => WalkDirection::Backward,
        Direction::Undirected => {
            return Err(unsupported_feature(
                "relationship_direction",
                "undirected traversal is not supported",
            ));
        }
    };
    let (minimum_hops, maximum_hops) = match relationship.length {
        None => (HopCount::One, HopCount::One),
        Some(length) if length.min == Some(1) && length.max == Some(2) => {
            (HopCount::One, HopCount::Two)
        }
        Some(_) => {
            return Err(unsupported_feature(
                "hop_range",
                "only one hop or the bounded range 1..2 is supported",
            ));
        }
    };

    let return_items = match &return_clause.items {
        ReturnItems::Explicit(items) if items.len() == 1 => items,
        _ => {
            return Err(unsupported_feature(
                "return_shape",
                "expected one projected node_id column",
            ));
        }
    };
    let projection = &return_items[0];
    let projects_target_node_id = matches!(
        &projection.expression,
        Expression::PropertyAccess { base, property }
            if property == "node_id"
                && matches!(base.as_ref(), Expression::Variable(name) if name == target_variable)
    );
    if !projects_target_node_id || projection.alias.as_deref() != Some("node_id") {
        return Err(unsupported_feature(
            "return_shape",
            "expected target.node_id AS node_id",
        ));
    }

    if order_clause.items.len() != 1
        || order_clause.items[0].direction != SortDirection::Asc
        || !matches!(
            &order_clause.items[0].expression,
            Expression::Variable(name) if name == "node_id"
        )
    {
        return Err(unsupported_feature(
            "ordering",
            "expected ORDER BY node_id ASC",
        ));
    }
    if start_variable == target_variable {
        return Err(unsupported_feature(
            "variable_binding",
            "start and target variables must be distinct",
        ));
    }
    if maximum_hops == HopCount::Two
        && (direction != WalkDirection::Backward || !return_clause.distinct)
    {
        return Err(unsupported_feature(
            "walk_configuration",
            "the two-hop profile requires reverse traversal with DISTINCT",
        ));
    }
    if maximum_hops == HopCount::One && return_clause.distinct {
        return Err(unsupported_feature(
            "distinct",
            "DISTINCT is not part of either one-hop profile",
        ));
    }

    let start_node_id = match parameters.get("node_id") {
        Some(CypherParameterValue::String(value)) if !value.is_empty() => value.clone(),
        Some(CypherParameterValue::String(_)) => {
            return Err(invalid_parameter("node_id", "must not be empty"));
        }
        Some(_) => return Err(invalid_parameter("node_id", "must be a string")),
        None => return Err(invalid_parameter("node_id", "is required")),
    };

    Ok(CompiledNeighborhoodWalkPlan {
        profile_version: NEIGHBORHOOD_WALK_PROFILE_VERSION.to_owned(),
        start_node_id,
        direction,
        minimum_hops,
        maximum_hops,
        relationship_type: "DEPENDS_ON".to_owned(),
        projection: NeighborhoodProjection::NodeIdString,
        distinct: return_clause.distinct,
        ordering: NeighborhoodOrdering::NodeIdAscending,
    })
}

pub fn serialize_canonical_plan_bytes(plan: &CompiledNeighborhoodWalkPlan) -> Vec<u8> {
    let mut bytes = b"knight-bus-neighborhood-walk-plan-v1\0".to_vec();
    append_length_prefixed_bytes(&mut bytes, plan.profile_version.as_bytes());
    append_length_prefixed_bytes(&mut bytes, b"node_id");
    bytes.push(match plan.direction {
        WalkDirection::Forward => 1,
        WalkDirection::Backward => 2,
    });
    bytes.push(match plan.minimum_hops {
        HopCount::One => 1,
        HopCount::Two => 2,
    });
    bytes.push(match plan.maximum_hops {
        HopCount::One => 1,
        HopCount::Two => 2,
    });
    append_length_prefixed_bytes(&mut bytes, plan.relationship_type.as_bytes());
    bytes.push(match plan.projection {
        NeighborhoodProjection::NodeIdString => 1,
    });
    bytes.push(u8::from(plan.distinct));
    bytes.push(match plan.ordering {
        NeighborhoodOrdering::NodeIdAscending => 1,
    });
    bytes
}

pub fn hash_canonical_plan_bytes(plan: &CompiledNeighborhoodWalkPlan) -> String {
    let digest = Sha256::digest(serialize_canonical_plan_bytes(plan));
    format!("{digest:x}")
}

pub fn execute_neighborhood_walk_plan(
    runtime: &MmapWalkRuntime,
    plan: &CompiledNeighborhoodWalkPlan,
) -> Result<NeighborhoodWalkResult, CypherWalkError> {
    execute_neighborhood_walk_with_limits(runtime, plan, &NeighborhoodExecutionLimits::default())
}

pub fn execute_neighborhood_walk_with_limits(
    runtime: &MmapWalkRuntime,
    plan: &CompiledNeighborhoodWalkPlan,
    limits: &NeighborhoodExecutionLimits,
) -> Result<NeighborhoodWalkResult, CypherWalkError> {
    check_execution_termination_now(limits)?;
    if plan.start_node_id.trim() != plan.start_node_id {
        return Ok(empty_walk_result_now());
    }
    let start_key = NodeKey::try_from(plan.start_node_id.clone()).map_err(execution_error_now)?;
    let start_id = match runtime.resolve_dense_id(&start_key) {
        Ok(dense_id) => dense_id,
        Err(KnightBusError::UnknownEntity { .. }) => return Ok(empty_walk_result_now()),
        Err(error) => return Err(execution_error_now(error)),
    };

    let mut first_hop = Vec::new();
    for endpoint in runtime
        .neighbors(start_id, plan.direction)
        .map_err(execution_error_now)?
    {
        check_execution_termination_now(limits)?;
        first_hop.push(endpoint);
    }
    let mut endpoints = first_hop.clone();
    if plan.maximum_hops == HopCount::Two {
        for first_endpoint in first_hop {
            check_execution_termination_now(limits)?;
            for endpoint in runtime
                .neighbors(first_endpoint, plan.direction)
                .map_err(execution_error_now)?
            {
                check_execution_termination_now(limits)?;
                endpoints.push(endpoint);
            }
        }
    }

    let endpoint_ids = if plan.distinct {
        endpoints
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    } else {
        endpoints
    };
    if limits
        .maximum_result_rows
        .is_some_and(|maximum| endpoint_ids.len() > maximum)
    {
        return Err(CypherWalkError::Terminated {
            reason: NeighborhoodTerminationReason::ResultRowLimitExceeded,
        });
    }
    check_execution_termination_now(limits)?;
    let mut node_ids = endpoint_ids
        .into_iter()
        .map(|dense_id| {
            runtime
                .key_for_dense_id(dense_id.get())
                .map_err(execution_error_now)
        })
        .collect::<Result<Vec<_>, _>>()?;
    node_ids.sort();

    Ok(NeighborhoodWalkResult {
        columns: vec!["node_id".to_owned()],
        records: node_ids
            .into_iter()
            .map(|node_id| ProjectedBoltResultRecord { node_id })
            .collect(),
    })
}

fn empty_walk_result_now() -> NeighborhoodWalkResult {
    NeighborhoodWalkResult {
        columns: vec!["node_id".to_owned()],
        records: Vec::new(),
    }
}

fn execution_error_now(error: impl std::fmt::Display) -> CypherWalkError {
    CypherWalkError::Execution {
        message: error.to_string(),
    }
}

fn check_execution_termination_now(
    limits: &NeighborhoodExecutionLimits,
) -> Result<(), CypherWalkError> {
    if limits.cancellation_flag.load(Ordering::Acquire) {
        return Err(CypherWalkError::Terminated {
            reason: NeighborhoodTerminationReason::ClientCancelled,
        });
    }
    if limits
        .deadline
        .is_some_and(|deadline| Instant::now() >= deadline)
    {
        return Err(CypherWalkError::Terminated {
            reason: NeighborhoodTerminationReason::DeadlineExceeded,
        });
    }
    Ok(())
}

fn unsupported_feature(feature: &str, message: &str) -> CypherWalkError {
    CypherWalkError::UnsupportedFeature {
        feature: feature.to_owned(),
        message: message.to_owned(),
    }
}

fn invalid_parameter(name: &str, message: &str) -> CypherWalkError {
    CypherWalkError::InvalidParameter {
        name: name.to_owned(),
        message: message.to_owned(),
    }
}

fn labels_match_graph_profile(labels: &[String]) -> bool {
    labels.is_empty() || labels == ["Entity"]
}

fn append_length_prefixed_bytes(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&(value.len() as u64).to_le_bytes());
    target.extend_from_slice(value);
}
