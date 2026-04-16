use std::{path::PathBuf, process::ExitCode};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use knight_bus::{
    BENCH_REPORT_FILE_NAME, HopCount, WalkDirection, build_snapshot_from_paths,
    query_snapshot_from_path, run_corpus_benchmark_from_paths, run_snapshot_benchmark,
    verify_snapshot_against_paths,
};

#[derive(Parser, Debug)]
#[command(name = "knight-bus")]
#[command(about = "CSV-first graph snapshot builder and walker")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Build {
        #[arg(long)]
        nodes_csv: PathBuf,
        #[arg(long)]
        edges_csv: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    Verify {
        #[arg(long)]
        snapshot: PathBuf,
        #[arg(long)]
        nodes_csv: PathBuf,
        #[arg(long)]
        edges_csv: PathBuf,
    },
    Query {
        #[arg(long)]
        snapshot: PathBuf,
        #[arg(long)]
        entity: String,
        #[arg(long, value_parser = parse_walk_direction)]
        dir: WalkDirection,
        #[arg(long, value_parser = parse_hop_count)]
        hops: HopCount,
        #[arg(long, value_enum)]
        format: OutputFormat,
    },
    Bench {
        #[arg(long)]
        snapshot: PathBuf,
        #[arg(long)]
        report: PathBuf,
    },
    BenchCorpus {
        #[arg(long)]
        snapshot: PathBuf,
        #[arg(long)]
        nodes_csv: PathBuf,
        #[arg(long)]
        edges_csv: PathBuf,
        #[arg(long)]
        corpus: PathBuf,
        #[arg(long)]
        report: PathBuf,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    Json,
    Text,
}

fn main() -> ExitCode {
    match try_main() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error:#}");
            ExitCode::FAILURE
        }
    }
}

fn try_main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Build {
            nodes_csv,
            edges_csv,
            output,
        } => {
            let summary = build_snapshot_from_paths(&nodes_csv, &edges_csv, &output)
                .with_context(|| format!("failed to build snapshot at {}", output.display()))?;
            println!("snapshot: {}", summary.output_dir.display());
            println!("nodes: {}", summary.node_count);
            println!("edges: {}", summary.edge_count);
            println!("snapshot_size_bytes: {}", summary.snapshot_size_bytes);
        }
        Commands::Verify {
            snapshot,
            nodes_csv,
            edges_csv,
        } => {
            let summary = verify_snapshot_against_paths(&snapshot, &nodes_csv, &edges_csv)
                .with_context(|| format!("failed to verify snapshot at {}", snapshot.display()))?;
            println!("verification: ok");
            println!("checked_queries: {}", summary.total_checked_queries);
            for family_summary in summary.families {
                println!(
                    "{}: {}",
                    family_summary.family.label(),
                    family_summary.checked_queries
                );
            }
        }
        Commands::Query {
            snapshot,
            entity,
            dir,
            hops,
            format,
        } => {
            let query_result = query_snapshot_from_path(&snapshot, &entity, dir, hops)
                .with_context(|| format!("failed to query snapshot at {}", snapshot.display()))?;
            match format {
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&query_result)
                            .context("failed to render query result as json")?
                    );
                }
                OutputFormat::Text => {
                    println!("entity: {}", query_result.entity);
                    println!("dense_id: {}", query_result.dense_id);
                    println!("direction: {}", query_result.direction.label());
                    println!("hops: {}", query_result.hops);
                    println!("neighbors:");
                    for neighbor_key in query_result.neighbors {
                        println!("- {neighbor_key}");
                    }
                }
            }
        }
        Commands::Bench { snapshot, report } => {
            let summary = run_snapshot_benchmark(&snapshot, &report).with_context(|| {
                format!("failed to benchmark snapshot at {}", snapshot.display())
            })?;
            println!("report: {}", summary.report_path.display());
            println!(
                "snapshot_size_bytes: {}",
                summary.report.snapshot_size_bytes
            );
            println!("peak_rss_bytes: {}", summary.report.peak_rss_bytes);
            println!(
                "peak_rss_source: {}",
                summary.report.peak_rss_source.label()
            );
            for family_report in summary.report.families {
                println!(
                    "{} sample_count={} p50_nanos={} p95_nanos={}",
                    family_report.family.label(),
                    family_report.sample_count,
                    family_report.p50_nanos,
                    family_report.p95_nanos
                );
            }
            println!("report_file_name: {BENCH_REPORT_FILE_NAME}");
        }
        Commands::BenchCorpus {
            snapshot,
            nodes_csv,
            edges_csv,
            corpus,
            report,
        } => {
            let summary = run_corpus_benchmark_from_paths(
                &snapshot, &nodes_csv, &edges_csv, &corpus, &report,
            )
            .with_context(|| format!("failed to benchmark corpus at {}", snapshot.display()))?;
            println!("report: {}", summary.report_path.display());
            println!("status: {}", summary.measurement.status);
            println!("query_corpus_size: {}", summary.query_corpus_size);
            println!("operation_count: {}", summary.measurement.operation_count);
            if let Some(open_start_ms) = summary.measurement.open_start_ms {
                println!("open_start_ms: {open_start_ms}");
            }
            if let Some(p50_ms) = summary.measurement.p50_ms {
                println!("p50_ms: {p50_ms}");
            }
            if let Some(p95_ms) = summary.measurement.p95_ms {
                println!("p95_ms: {p95_ms}");
            }
            if let Some(p99_ms) = summary.measurement.p99_ms {
                println!("p99_ms: {p99_ms}");
            }
            if let Some(mean_ms) = summary.measurement.mean_ms {
                println!("mean_ms: {mean_ms}");
            }
            if let Some(rss_bytes) = summary.measurement.rss_bytes {
                println!("rss_bytes: {rss_bytes}");
            }
            if let Some(version) = &summary.measurement.version {
                println!("version: {version}");
            }
        }
    }
    Ok(())
}

fn parse_walk_direction(value: &str) -> Result<WalkDirection, String> {
    value
        .parse::<WalkDirection>()
        .map_err(|error| error.to_string())
}

fn parse_hop_count(value: &str) -> Result<HopCount, String> {
    value.parse::<HopCount>().map_err(|error| error.to_string())
}
