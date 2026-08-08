use std::collections::BTreeMap;

use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::STANDARD};
use clap::Parser;
use knight_bus::cypher::{
    CypherParameterValue, CypherWalkError, compile_neighborhood_walk_plan,
    hash_canonical_plan_bytes,
};
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(name = "knight-bus-cypher-check")]
struct CypherCheckArguments {
    #[arg(long)]
    query_base64: String,
    #[arg(long)]
    node_id: String,
}

#[derive(Serialize)]
struct CypherCheckOutcome {
    outcome: &'static str,
    plan_hash: Option<String>,
    error_code: Option<&'static str>,
    error_message: Option<String>,
}

fn main() -> Result<()> {
    let arguments = CypherCheckArguments::parse();
    let query_bytes = STANDARD
        .decode(arguments.query_base64)
        .context("query-base64 is not valid base64")?;
    let query = String::from_utf8(query_bytes).context("decoded query is not UTF-8")?;
    let parameters = BTreeMap::from([(
        "node_id".to_owned(),
        CypherParameterValue::String(arguments.node_id),
    )]);
    let outcome = match compile_neighborhood_walk_plan(&query, &parameters) {
        Ok(plan) => CypherCheckOutcome {
            outcome: "accepted",
            plan_hash: Some(hash_canonical_plan_bytes(&plan)),
            error_code: None,
            error_message: None,
        },
        Err(error @ CypherWalkError::Syntax { .. }) => rejected_outcome_now("syntax", error),
        Err(error @ CypherWalkError::UnsupportedFeature { .. }) => {
            rejected_outcome_now("unsupported", error)
        }
        Err(error @ CypherWalkError::InvalidParameter { .. }) => {
            rejected_outcome_now("parameter", error)
        }
        Err(error @ CypherWalkError::Execution { .. }) => rejected_outcome_now("execution", error),
        Err(error @ CypherWalkError::Terminated { .. }) => {
            rejected_outcome_now("terminated", error)
        }
    };
    println!("{}", serde_json::to_string(&outcome)?);
    Ok(())
}

fn rejected_outcome_now(code: &'static str, error: CypherWalkError) -> CypherCheckOutcome {
    CypherCheckOutcome {
        outcome: code,
        plan_hash: None,
        error_code: Some(code),
        error_message: Some(error.to_string()),
    }
}
