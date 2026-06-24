use serde_json::json;

use knight_bus::{
    GdsExecutionContext, GdsExecutionRequest, GdsExecutionResult, GdsExecutionValue,
    GraphProjectionCatalog, GraphProjectionMetadata, GraphProjectionSpec, MemoryEstimate,
    ProjectedNodePropertyRow, ProjectedRelationshipPropertyRow, ProjectionSelector,
    PropertySelector, RelationshipOrientation,
    execute_registered_gds_procedure, execute_registered_gds_user_function,
};

fn sample_projection_spec_now(graph_name: &str) -> GraphProjectionSpec {
    GraphProjectionSpec::new(
        graph_name.to_owned(),
        ProjectionSelector::named(["City", "Airport"]).expect("node selector"),
        ProjectionSelector::named(["ROUTE"]).expect("relationship selector"),
        RelationshipOrientation::Natural,
        PropertySelector::named(["pagerank", "community"]).expect("node properties"),
        PropertySelector::named(["weight"]).expect("relationship properties"),
    )
    .expect("projection spec")
}

fn sample_projection_metadata_now(snapshot_generation: u64) -> GraphProjectionMetadata {
    GraphProjectionMetadata::new(
        "alice".to_owned(),
        "neo4j".to_owned(),
        snapshot_generation,
        12,
        34,
        MemoryEstimate::projection(8_192, 1_024, 256, 128, 64),
        1_717_000_000_000 + snapshot_generation,
    )
    .with_property_plane(
        vec![
            ProjectedNodePropertyRow::new(
                0,
                vec!["City".to_owned()],
                std::collections::BTreeMap::from([
                    ("pagerank".to_owned(), json!(0.25)),
                    ("community".to_owned(), json!(7)),
                ]),
            ),
            ProjectedNodePropertyRow::new(
                1,
                vec!["Airport".to_owned()],
                std::collections::BTreeMap::from([("pagerank".to_owned(), json!(0.75))]),
            ),
        ],
        vec![
            ProjectedRelationshipPropertyRow::new(
                0,
                1,
                "ROUTE",
                std::collections::BTreeMap::from([("weight".to_owned(), json!(1.5))]),
            ),
            ProjectedRelationshipPropertyRow::new(
                1,
                0,
                "ROUTE",
                std::collections::BTreeMap::from([("weight".to_owned(), json!(2.5))]),
            ),
        ],
    )
}

#[test]
fn gds_catalog_execution_round_trips_now() {
    let mut catalog = GraphProjectionCatalog::new();
    let mut context = GdsExecutionContext::new(&mut catalog);

    let estimate_result = execute_registered_gds_procedure(
        &mut context,
        "gds.graph.project.estimate",
        &GdsExecutionRequest::project_estimate(
            sample_projection_spec_now("roads"),
            sample_projection_metadata_now(42),
        ),
    )
    .expect("graph project estimate");

    match estimate_result {
        GdsExecutionResult::Table(table) => {
            assert_eq!(table.rows.len(), 1);
            assert_eq!(
                table.rows[0]
                    .get("bytesMin")
                    .and_then(GdsExecutionValue::as_u64),
                Some(9_664)
            );
            assert_eq!(
                table.rows[0]
                    .get("bytesMax")
                    .and_then(GdsExecutionValue::as_u64),
                Some(9_664)
            );
            assert_eq!(
                table.rows[0]
                    .get("nodeCount")
                    .and_then(GdsExecutionValue::as_u64),
                Some(12)
            );
        }
        other => panic!("expected table result, got {other:?}"),
    }

    assert_eq!(context.catalog().len(), 0, "estimate must not mutate catalog");

    let project_result = execute_registered_gds_procedure(
        &mut context,
        "gds.graph.project",
        &GdsExecutionRequest::graph_project(
            sample_projection_spec_now("roads"),
            sample_projection_metadata_now(42),
        ),
    )
    .expect("graph project");

    match project_result {
        GdsExecutionResult::Table(table) => {
            assert_eq!(table.rows.len(), 1);
            assert_eq!(
                table.rows[0]
                    .get("graphName")
                    .and_then(GdsExecutionValue::as_str),
                Some("roads")
            );
            assert_eq!(
                table.rows[0]
                    .get("relationshipCount")
                    .and_then(GdsExecutionValue::as_u64),
                Some(34)
            );
        }
        other => panic!("expected table result, got {other:?}"),
    }

    let exists_proc = execute_registered_gds_procedure(
        &mut context,
        "gds.graph.exists",
        &GdsExecutionRequest::graph_name_only("roads"),
    )
    .expect("graph exists proc");
    match exists_proc {
        GdsExecutionResult::Table(table) => {
            assert_eq!(table.rows.len(), 1);
            assert_eq!(
                table.rows[0].get("exists").and_then(GdsExecutionValue::as_bool),
                Some(true)
            );
        }
        other => panic!("expected table result, got {other:?}"),
    }

    let exists_func = execute_registered_gds_user_function(
        &mut context,
        "gds.graph.exists",
        &GdsExecutionRequest::graph_name_only("roads"),
    )
    .expect("graph exists function");
    assert_eq!(exists_func, GdsExecutionResult::Scalar(GdsExecutionValue::Bool(true)));

    let list_result = execute_registered_gds_procedure(
        &mut context,
        "gds.graph.list",
        &GdsExecutionRequest::list_filter(Some("roads".to_owned())),
    )
    .expect("graph list");
    match list_result {
        GdsExecutionResult::Table(table) => {
            assert_eq!(table.rows.len(), 1);
            assert_eq!(
                table.rows[0]
                    .get("database")
                    .and_then(GdsExecutionValue::as_str),
                Some("neo4j")
            );
            assert_eq!(
                table.rows[0]
                    .get("nodeCount")
                    .and_then(GdsExecutionValue::as_u64),
                Some(12)
            );
        }
        other => panic!("expected table result, got {other:?}"),
    }

    let size_of_result = execute_registered_gds_procedure(
        &mut context,
        "gds.internal.graph.sizeOf",
        &GdsExecutionRequest::graph_name_only("roads"),
    )
    .expect("graph sizeOf");
    match size_of_result {
        GdsExecutionResult::Table(table) => {
            assert_eq!(table.rows.len(), 1);
            assert_eq!(
                table.rows[0]
                    .get("graphName")
                    .and_then(GdsExecutionValue::as_str),
                Some("roads")
            );
            assert_eq!(
                table.rows[0]
                    .get("sizeInBytes")
                    .and_then(GdsExecutionValue::as_u64),
                Some(9_664)
            );
        }
        other => panic!("expected table result, got {other:?}"),
    }

    let drop_result = execute_registered_gds_procedure(
        &mut context,
        "gds.graph.drop",
        &GdsExecutionRequest::graph_name_only("roads"),
    )
    .expect("graph drop");
    match drop_result {
        GdsExecutionResult::Table(table) => {
            assert_eq!(table.rows.len(), 1);
            assert_eq!(
                table.rows[0]
                    .get("graphName")
                    .and_then(GdsExecutionValue::as_str),
                Some("roads")
            );
        }
        other => panic!("expected table result, got {other:?}"),
    }

    let exists_after_drop = execute_registered_gds_user_function(
        &mut context,
        "gds.graph.exists",
        &GdsExecutionRequest::graph_name_only("roads"),
    )
    .expect("graph exists function");
    assert_eq!(
        exists_after_drop,
        GdsExecutionResult::Scalar(GdsExecutionValue::Bool(false))
    );
}

#[test]
fn gds_property_stream_execution_uses_projected_property_plane_now() {
    let mut catalog = GraphProjectionCatalog::new();
    let mut context = GdsExecutionContext::new(&mut catalog);

    execute_registered_gds_procedure(
        &mut context,
        "gds.graph.project",
        &GdsExecutionRequest::graph_project(
            sample_projection_spec_now("roads"),
            sample_projection_metadata_now(42),
        ),
    )
    .expect("graph project");

    let node_rows = execute_registered_gds_procedure(
        &mut context,
        "gds.graph.nodeProperties.stream",
        &GdsExecutionRequest::node_properties_stream(
            "roads",
            vec!["pagerank".to_owned(), "community".to_owned()],
            ProjectionSelector::All,
        )
        .with_list_node_labels(true),
    )
    .expect("node properties stream");

    match node_rows {
        GdsExecutionResult::Table(table) => {
            assert_eq!(table.rows.len(), 3);
            assert_eq!(
                table.rows[0]
                    .get("nodeId")
                    .and_then(GdsExecutionValue::as_u64),
                Some(0)
            );
            assert_eq!(
                table.rows[0]
                    .get("nodeProperty")
                    .and_then(GdsExecutionValue::as_str),
                Some("community")
            );
        }
        other => panic!("expected table result, got {other:?}"),
    }

    let single_node_rows = execute_registered_gds_procedure(
        &mut context,
        "gds.graph.streamNodeProperty",
        &GdsExecutionRequest::node_properties_stream(
            "roads",
            vec!["community".to_owned()],
            ProjectionSelector::named(["City"]).expect("label selector"),
        )
        .with_list_node_labels(true),
    )
    .expect("single node property stream");

    match single_node_rows {
        GdsExecutionResult::Table(table) => {
            assert_eq!(table.rows.len(), 1);
            assert_eq!(
                table.rows[0]
                    .get("nodeLabels")
                    .and_then(|value| match value {
                        GdsExecutionValue::List(values) => Some(values.len() as u64),
                        _ => None,
                    }),
                Some(1)
            );
        }
        other => panic!("expected table result, got {other:?}"),
    }

    let relationship_rows = execute_registered_gds_procedure(
        &mut context,
        "gds.graph.relationshipProperties.stream",
        &GdsExecutionRequest::relationship_properties_stream(
            "roads",
            vec!["weight".to_owned()],
            ProjectionSelector::named(["ROUTE"]).expect("type selector"),
        ),
    )
    .expect("relationship properties stream");

    match relationship_rows {
        GdsExecutionResult::Table(table) => {
            assert_eq!(table.rows.len(), 2);
            assert_eq!(
                table.rows[0]
                    .get("relationshipType")
                    .and_then(GdsExecutionValue::as_str),
                Some("ROUTE")
            );
        }
        other => panic!("expected table result, got {other:?}"),
    }

    let single_relationship_rows = execute_registered_gds_procedure(
        &mut context,
        "gds.graph.streamRelationshipProperty",
        &GdsExecutionRequest::relationship_properties_stream(
            "roads",
            vec!["weight".to_owned()],
            ProjectionSelector::All,
        ),
    )
    .expect("single relationship property stream");

    match single_relationship_rows {
        GdsExecutionResult::Table(table) => {
            assert_eq!(table.rows.len(), 2);
            assert!(table.rows[0].get("relationshipType").is_some());
            assert!(table.rows[0].get("propertyValue").is_some());
        }
        other => panic!("expected table result, got {other:?}"),
    }
}

#[test]
fn gds_graph_drop_can_ignore_missing_graphs_now() {
    let mut catalog = GraphProjectionCatalog::new();
    let mut context = GdsExecutionContext::new(&mut catalog);

    let result = execute_registered_gds_procedure(
        &mut context,
        "gds.graph.drop",
        &GdsExecutionRequest::graph_name_only("missing").with_fail_if_missing(false),
    )
    .expect("drop without fail");

    match result {
        GdsExecutionResult::Table(table) => {
            assert!(table.rows.is_empty());
        }
        other => panic!("expected table result, got {other:?}"),
    }
}
