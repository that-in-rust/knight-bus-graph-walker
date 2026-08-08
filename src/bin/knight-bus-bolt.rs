use std::{net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use boltr::server::BoltServer;
use clap::Parser;
use knight_bus::{
    MmapWalkRuntime,
    bolt::{
        DEFAULT_MAXIMUM_RESULT_ROWS, DEFAULT_QUERY_TIMEOUT, KnightBusBasicAuthValidator,
        KnightBusBoltBackend,
    },
};

#[derive(Debug, Parser)]
#[command(name = "knight-bus-bolt")]
struct BoltServerArguments {
    #[arg(long)]
    snapshot: PathBuf,
    #[arg(long, default_value = "127.0.0.1:7687")]
    bind: SocketAddr,
    #[arg(long)]
    username: String,
    #[arg(long)]
    password: String,
    #[arg(long, default_value_t = 256)]
    max_sessions: usize,
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    max_message_size: usize,
    #[arg(long, default_value_t = DEFAULT_QUERY_TIMEOUT.as_millis() as u64)]
    query_timeout_ms: u64,
    #[arg(long, default_value_t = DEFAULT_MAXIMUM_RESULT_ROWS)]
    max_result_rows: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = BoltServerArguments::parse();
    let runtime = MmapWalkRuntime::open(&arguments.snapshot)
        .with_context(|| format!("opening snapshot {}", arguments.snapshot.display()))?;
    let backend = KnightBusBoltBackend::new_with_execution_limits(
        runtime,
        Duration::from_millis(arguments.query_timeout_ms),
        arguments.max_result_rows,
    )
    .context("loading snapshot identity and execution limits")?;
    let auth = KnightBusBasicAuthValidator::new(arguments.username, arguments.password);

    eprintln!(
        "knight-bus-bolt starting profile=knight-bus-neighborhood-walk-v1 bind={}",
        arguments.bind
    );
    BoltServer::builder(backend)
        .auth(auth)
        .max_sessions(arguments.max_sessions)
        .max_message_size(arguments.max_message_size)
        .shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .serve(arguments.bind)
        .await
        .context("serving Bolt endpoint")
}
