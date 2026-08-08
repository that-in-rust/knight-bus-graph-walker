use std::{
    collections::BTreeMap,
    fs,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use knight_bus::cypher::{
    CypherParameterValue, CypherWalkError, NeighborhoodExecutionLimits, NeighborhoodOrdering,
    NeighborhoodProjection, NeighborhoodTerminationReason, compile_neighborhood_walk_plan,
    execute_neighborhood_walk_plan, execute_neighborhood_walk_with_limits,
    hash_canonical_plan_bytes, serialize_canonical_plan_bytes,
};
use knight_bus::{HopCount, MmapWalkRuntime, WalkDirection, build_snapshot_from_paths};
use proptest::prelude::*;
use tempfile::TempDir;

const FORWARD_ONE_QUERY: &str = "MATCH (n {node_id: $node_id})-[:DEPENDS_ON]->(m) \
     RETURN m.node_id AS node_id ORDER BY node_id";
const REVERSE_ONE_QUERY: &str = "MATCH (n {node_id: $node_id})<-[:DEPENDS_ON]-(m) \
     RETURN m.node_id AS node_id ORDER BY node_id";
const REVERSE_TWO_QUERY: &str = "MATCH (n {node_id: $node_id})<-[:DEPENDS_ON*1..2]-(m) \
     RETURN DISTINCT m.node_id AS node_id ORDER BY node_id";

fn string_parameter_map(node_id: &str) -> BTreeMap<String, CypherParameterValue> {
    BTreeMap::from([(
        "node_id".to_owned(),
        CypherParameterValue::String(node_id.to_owned()),
    )])
}

fn build_adversarial_runtime_now() -> (TempDir, MmapWalkRuntime) {
    let temp_dir = TempDir::new().expect("temporary graph directory");
    let nodes_path = temp_dir.path().join("nodes.csv");
    let edges_path = temp_dir.path().join("edges.csv");
    let snapshot_path = temp_dir.path().join("snapshot");
    fs::write(
        &nodes_path,
        "node_id,node_type,label,parent_id,file_path,span\n\
         A,function,A,,A,\n\
         B,function,B,,B,\n\
         C,function,C,,C,\n\
         D,function,D,,D,\n\
         E,function,E,,E,\n\
         F,function,F,,F,\n\
         G,function,G,,G,\n",
    )
    .expect("nodes fixture writes");
    fs::write(
        &edges_path,
        "from_id,edge_type,to_id\n\
         A,DEPENDS_ON,A\n\
         A,DEPENDS_ON,E\n\
         B,DEPENDS_ON,A\n\
         C,DEPENDS_ON,B\n\
         C,DEPENDS_ON,D\n\
         D,DEPENDS_ON,A\n\
         F,DEPENDS_ON,G\n\
         G,DEPENDS_ON,F\n",
    )
    .expect("edges fixture writes");
    build_snapshot_from_paths(&nodes_path, &edges_path, &snapshot_path)
        .expect("adversarial snapshot builds");
    let runtime = MmapWalkRuntime::open(&snapshot_path).expect("adversarial snapshot opens");
    (temp_dir, runtime)
}

fn execute_query_node_ids_now(
    runtime: &MmapWalkRuntime,
    query: &str,
    node_id: &str,
) -> Vec<String> {
    let plan = compile_neighborhood_walk_plan(query, &string_parameter_map(node_id))
        .expect("fixture query compiles");
    execute_neighborhood_walk_plan(runtime, &plan)
        .expect("fixture query executes")
        .records
        .into_iter()
        .map(|record| record.node_id)
        .collect()
}

#[test]
fn compiles_three_supported_walks_exactly() {
    let parameters = string_parameter_map("src/runtime.rs");

    let forward = compile_neighborhood_walk_plan(FORWARD_ONE_QUERY, &parameters)
        .expect("canonical forward query should compile");
    assert_eq!(forward.profile_version, "knight-bus-neighborhood-walk-v1");
    assert_eq!(forward.start_node_id, "src/runtime.rs");
    assert_eq!(forward.direction, WalkDirection::Forward);
    assert_eq!(forward.minimum_hops, HopCount::One);
    assert_eq!(forward.maximum_hops, HopCount::One);
    assert_eq!(forward.relationship_type, "DEPENDS_ON");
    assert_eq!(forward.projection, NeighborhoodProjection::NodeIdString);
    assert_eq!(forward.ordering, NeighborhoodOrdering::NodeIdAscending);
    assert!(!forward.distinct);

    let reverse = compile_neighborhood_walk_plan(REVERSE_ONE_QUERY, &parameters)
        .expect("canonical reverse query should compile");
    assert_eq!(reverse.direction, WalkDirection::Backward);
    assert_eq!(reverse.minimum_hops, HopCount::One);
    assert_eq!(reverse.maximum_hops, HopCount::One);
    assert!(!reverse.distinct);

    let reverse_two = compile_neighborhood_walk_plan(REVERSE_TWO_QUERY, &parameters)
        .expect("canonical reverse one-to-two query should compile");
    assert_eq!(reverse_two.direction, WalkDirection::Backward);
    assert_eq!(reverse_two.minimum_hops, HopCount::One);
    assert_eq!(reverse_two.maximum_hops, HopCount::Two);
    assert!(reverse_two.distinct);
}

#[test]
fn canonicalizes_semantic_preserving_query_variants() {
    let parameters = string_parameter_map("src/runtime.rs");
    let canonical = compile_neighborhood_walk_plan(REVERSE_TWO_QUERY, &parameters)
        .expect("canonical query should compile");
    let variant = compile_neighborhood_walk_plan(
        "/* caller formatting */ match (seed {node_id:$node_id}) \
         <-[:DEPENDS_ON*1..2]-(endpoint) return distinct endpoint.node_id as node_id \
         order by node_id asc",
        &parameters,
    )
    .expect("semantic-preserving variant should compile");

    assert_eq!(canonical, variant);
    assert_eq!(
        serialize_canonical_plan_bytes(&canonical),
        serialize_canonical_plan_bytes(&variant)
    );
    assert_eq!(
        hash_canonical_plan_bytes(&canonical),
        hash_canonical_plan_bytes(&variant)
    );

    let labeled_variant = compile_neighborhood_walk_plan(
        "MATCH (seed:Entity {node_id: $node_id})<-[:DEPENDS_ON*1..2]-(endpoint:Entity) \
         RETURN DISTINCT endpoint.node_id AS node_id ORDER BY node_id ASC",
        &parameters,
    )
    .expect("optional Entity labels should preserve the fixed graph profile");
    assert_eq!(canonical, labeled_variant);
    assert_eq!(
        hash_canonical_plan_bytes(&canonical),
        hash_canonical_plan_bytes(&labeled_variant)
    );
}

#[test]
fn excludes_unrelated_parameters_from_plan_identity() {
    let baseline = string_parameter_map("src/runtime.rs");
    let mut with_extra = baseline.clone();
    with_extra.insert("unused".to_owned(), CypherParameterValue::Integer(42));

    let first = compile_neighborhood_walk_plan(FORWARD_ONE_QUERY, &baseline)
        .expect("baseline query should compile");
    let second = compile_neighborhood_walk_plan(FORWARD_ONE_QUERY, &with_extra)
        .expect("extra parameters should be ignored");

    assert_eq!(first, second);
}

#[test]
fn excludes_bound_value_from_canonical_plan_identity() {
    let first =
        compile_neighborhood_walk_plan(FORWARD_ONE_QUERY, &string_parameter_map("secret-A"))
            .expect("first parameter should bind");
    let second =
        compile_neighborhood_walk_plan(FORWARD_ONE_QUERY, &string_parameter_map("secret-B"))
            .expect("second parameter should bind");

    assert_ne!(first.start_node_id, second.start_node_id);
    assert_eq!(
        serialize_canonical_plan_bytes(&first),
        serialize_canonical_plan_bytes(&second)
    );
    assert_eq!(
        hash_canonical_plan_bytes(&first),
        hash_canonical_plan_bytes(&second)
    );
}

#[test]
fn rejects_missing_null_and_non_string_node_identifiers() {
    let missing = compile_neighborhood_walk_plan(FORWARD_ONE_QUERY, &BTreeMap::new())
        .expect_err("missing node_id should fail");
    assert!(matches!(
        missing,
        CypherWalkError::InvalidParameter { ref name, .. } if name == "node_id"
    ));

    for invalid in [
        CypherParameterValue::Null,
        CypherParameterValue::Integer(7),
        CypherParameterValue::Boolean(true),
    ] {
        let parameters = BTreeMap::from([("node_id".to_owned(), invalid)]);
        let error = compile_neighborhood_walk_plan(FORWARD_ONE_QUERY, &parameters)
            .expect_err("non-string node_id should fail");
        assert!(matches!(
            error,
            CypherWalkError::InvalidParameter { ref name, .. } if name == "node_id"
        ));
    }
}

#[test]
fn separates_syntax_from_unsupported_query_failures() {
    let parameters = string_parameter_map("src/runtime.rs");

    let syntax = compile_neighborhood_walk_plan("MATCH (n RETURN n", &parameters)
        .expect_err("malformed Cypher should fail");
    assert!(matches!(syntax, CypherWalkError::Syntax { .. }));

    let unsupported = compile_neighborhood_walk_plan(
        "MATCH (n {node_id: $node_id})-[:KNOWS]->(m) \
         RETURN m.node_id AS node_id ORDER BY node_id",
        &parameters,
    )
    .expect_err("valid unsupported relationship should fail");
    assert!(matches!(
        unsupported,
        CypherWalkError::UnsupportedFeature { ref feature, .. }
            if feature == "relationship_type"
    ));
}

#[test]
fn changes_plan_hash_when_execution_semantics_change() {
    let parameters = string_parameter_map("src/runtime.rs");
    let forward = compile_neighborhood_walk_plan(FORWARD_ONE_QUERY, &parameters)
        .expect("forward query should compile");
    let reverse = compile_neighborhood_walk_plan(REVERSE_ONE_QUERY, &parameters)
        .expect("reverse query should compile");

    assert_ne!(
        hash_canonical_plan_bytes(&forward),
        hash_canonical_plan_bytes(&reverse)
    );
}

#[test]
fn executes_self_loop_cycle_and_diamond_semantics() {
    let (_temp_dir, runtime) = build_adversarial_runtime_now();

    assert_eq!(
        execute_query_node_ids_now(&runtime, FORWARD_ONE_QUERY, "A"),
        vec!["A", "E"]
    );
    assert_eq!(
        execute_query_node_ids_now(&runtime, REVERSE_ONE_QUERY, "A"),
        vec!["A", "B", "D"]
    );
    assert_eq!(
        execute_query_node_ids_now(&runtime, REVERSE_TWO_QUERY, "A"),
        vec!["A", "B", "C", "D"]
    );
    assert_eq!(
        execute_query_node_ids_now(&runtime, REVERSE_TWO_QUERY, "F"),
        vec!["F", "G"]
    );
}

#[test]
fn returns_successful_empty_result_for_missing_seed() {
    let (_temp_dir, runtime) = build_adversarial_runtime_now();
    let plan =
        compile_neighborhood_walk_plan(REVERSE_TWO_QUERY, &string_parameter_map("missing-node"))
            .expect("missing seed still compiles");

    let result = execute_neighborhood_walk_plan(&runtime, &plan)
        .expect("missing seed is a successful empty query");

    assert_eq!(result.columns, vec!["node_id"]);
    assert!(result.records.is_empty());
}

#[test]
fn preserves_exact_parameter_value_during_lookup() {
    let (_temp_dir, runtime) = build_adversarial_runtime_now();
    let plan = compile_neighborhood_walk_plan(FORWARD_ONE_QUERY, &string_parameter_map(" A "))
        .expect("whitespace-bearing string is still a valid Cypher parameter");

    assert_eq!(plan.start_node_id, " A ");
    let result = execute_neighborhood_walk_plan(&runtime, &plan)
        .expect("an exact but absent node identifier returns an empty result");
    assert!(result.records.is_empty());
}

#[test]
fn enforces_deadline_row_limit_and_cancellation_without_poisoning_runtime() {
    let (_temp_dir, runtime) = build_adversarial_runtime_now();
    let plan = compile_neighborhood_walk_plan(FORWARD_ONE_QUERY, &string_parameter_map("A"))
        .expect("bounded query compiles");

    let expired = NeighborhoodExecutionLimits {
        deadline: Some(Instant::now() - Duration::from_millis(1)),
        maximum_result_rows: None,
        cancellation_flag: Arc::new(AtomicBool::new(false)),
    };
    let deadline_error = execute_neighborhood_walk_with_limits(&runtime, &plan, &expired)
        .expect_err("expired deadline should terminate");
    assert!(matches!(
        deadline_error,
        CypherWalkError::Terminated {
            reason: NeighborhoodTerminationReason::DeadlineExceeded
        }
    ));

    let row_limited = NeighborhoodExecutionLimits {
        deadline: None,
        maximum_result_rows: Some(1),
        cancellation_flag: Arc::new(AtomicBool::new(false)),
    };
    let row_error = execute_neighborhood_walk_with_limits(&runtime, &plan, &row_limited)
        .expect_err("two records should exceed a one-row limit");
    assert!(matches!(
        row_error,
        CypherWalkError::Terminated {
            reason: NeighborhoodTerminationReason::ResultRowLimitExceeded
        }
    ));

    let cancellation_flag = Arc::new(AtomicBool::new(true));
    let cancelled = NeighborhoodExecutionLimits {
        deadline: None,
        maximum_result_rows: None,
        cancellation_flag: cancellation_flag.clone(),
    };
    let cancellation_error = execute_neighborhood_walk_with_limits(&runtime, &plan, &cancelled)
        .expect_err("pre-cancelled query should terminate");
    assert!(matches!(
        cancellation_error,
        CypherWalkError::Terminated {
            reason: NeighborhoodTerminationReason::ClientCancelled
        }
    ));

    cancellation_flag.store(false, Ordering::Release);
    let recovered = execute_neighborhood_walk_plan(&runtime, &plan)
        .expect("shared runtime should remain usable after termination");
    assert_eq!(
        recovered
            .records
            .into_iter()
            .map(|record| record.node_id)
            .collect::<Vec<_>>(),
        vec!["A", "E"]
    );
}

#[test]
fn rejects_complete_valid_but_unsupported_cypher_matrix() {
    let parameters = string_parameter_map("A");
    let unsupported_queries = [
        (
            "OPTIONAL MATCH (n {node_id: $node_id})-[:DEPENDS_ON]->(m) \
             RETURN m.node_id AS node_id ORDER BY node_id",
            "optional_match",
        ),
        ("CREATE (n {node_id: $node_id})", "write_clause"),
        (
            "MATCH (n {node_id: $node_id})-[:DEPENDS_ON*]->(m) \
             RETURN DISTINCT m.node_id AS node_id ORDER BY node_id",
            "hop_range",
        ),
        (
            "MATCH (n {node_id: $node_id})<-[:DEPENDS_ON*1..3]-(m) \
             RETURN DISTINCT m.node_id AS node_id ORDER BY node_id",
            "hop_range",
        ),
        (
            "MATCH (n {node_id: $node_id})-[:DEPENDS_ON]->(m) WHERE m.kind = 'function' \
             RETURN m.node_id AS node_id ORDER BY node_id",
            "where_clause",
        ),
        (
            "MATCH (n {node_id: $node_id})-[:DEPENDS_ON]->(m) \
             RETURN m AS node_id ORDER BY node_id",
            "return_shape",
        ),
        (
            "MATCH (n {node_id: $node_id})-[:DEPENDS_ON]->(m) \
             RETURN m.node_id AS node_id ORDER BY node_id DESC",
            "ordering",
        ),
        (
            "MATCH (n {node_id: $node_id})-[:DEPENDS_ON]->(m) \
             RETURN m.node_id AS node_id",
            "clause_shape",
        ),
        (
            "MATCH (n:Other {node_id: $node_id})-[:DEPENDS_ON]->(m) \
             RETURN m.node_id AS node_id ORDER BY node_id",
            "node_label",
        ),
        (
            "MATCH (n {node_id: $node_id})-[:DEPENDS_ON]-(m) \
             RETURN m.node_id AS node_id ORDER BY node_id",
            "relationship_direction",
        ),
    ];

    for (query, expected_feature) in unsupported_queries {
        let error = compile_neighborhood_walk_plan(query, &parameters)
            .expect_err("matrix query should be rejected");
        assert!(
            matches!(
                error,
                CypherWalkError::UnsupportedFeature { ref feature, .. }
                    if feature == expected_feature
            ),
            "expected unsupported feature {expected_feature} for {query:?}, got {error:?}"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn arbitrary_cypher_inputs_return_without_panicking(
        query_characters in proptest::collection::vec(any::<char>(), 0..2048),
        node_characters in proptest::collection::vec(any::<char>(), 0..256),
    ) {
        let query = query_characters.into_iter().collect::<String>();
        let node_id = node_characters.into_iter().collect::<String>();
        let parameters = string_parameter_map(&node_id);

        let _outcome = compile_neighborhood_walk_plan(&query, &parameters);
    }
}
