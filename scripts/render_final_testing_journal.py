#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import platform
import subprocess
from pathlib import Path
from typing import Any


STAGE_ROWS = [
    ("neo4j_smoke_1mb", "1 MB"),
    ("neo4j_preflight_50mb", "50 MB"),
]


def read_json_file_now(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def format_value_now(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def measurement_map_now(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {item["engine_name"]: item for item in payload["measurements"]}


def lower_is_better_delta_now(left: float | int | None, right: float | int | None, noun: str) -> str:
    if left in (None, 0) or right in (None, 0):
        return "n/a"
    if left == right:
        return "equal"
    faster = max(left, right) / min(left, right)
    return f"~{faster:.1f}x {noun}"


def winner_row_now(metric_name: str, rust_value: Any, neo4j_value: Any) -> tuple[str, str]:
    if metric_name == "Query corpus":
        return ("tie", "same workload")
    if metric_name == "Status":
        if rust_value == neo4j_value == "ok":
            return ("tie", "parity passed")
        if rust_value == neo4j_value:
            return ("tie", str(rust_value))
        if rust_value == "ok":
            return ("Knight Bus Rust", "only Rust completed")
        if neo4j_value == "ok":
            return ("Neo4j", "only Neo4j completed")
        return ("n/a", "both failed")
    if rust_value is None or neo4j_value is None:
        return ("n/a", "n/a")
    if float(rust_value) == float(neo4j_value):
        return ("tie", "equal")
    if metric_name == "RSS bytes":
        rust_wins = float(rust_value) < float(neo4j_value)
        return (
            "Knight Bus Rust" if rust_wins else "Neo4j",
            lower_is_better_delta_now(float(rust_value), float(neo4j_value), "lower"),
        )
    rust_wins = float(rust_value) < float(neo4j_value)
    return (
        "Knight Bus Rust" if rust_wins else "Neo4j",
        lower_is_better_delta_now(float(rust_value), float(neo4j_value), "faster"),
    )


def load_stage_now(reports_dir: Path, stage_name: str) -> dict[str, Any]:
    report_dir = reports_dir / stage_name
    payload = read_json_file_now(report_dir / "report.json")
    import_meta_path = report_dir / "import-meta.json"
    import_meta = read_json_file_now(import_meta_path) if import_meta_path.exists() else None
    return {
        "report_dir": report_dir,
        "payload": payload,
        "measurements": measurement_map_now(payload),
        "import_meta": import_meta,
    }


def snapshot_size_bytes_now(repo_root: Path, stage_name: str, payload: dict[str, Any]) -> int:
    snapshot_manifest = payload["snapshot_manifest"]
    if "snapshot_bytes" in snapshot_manifest:
        return int(snapshot_manifest["snapshot_bytes"])
    snapshot_dir = repo_root / "artifacts" / stage_name / "snapshot"
    return sum(entry.stat().st_size for entry in snapshot_dir.rglob("*") if entry.is_file())


def command_output_now(command: list[str]) -> str:
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    return (result.stdout or result.stderr).strip() or "unknown"


def build_final_journal_now(repo_root: Path, reports_dir: Path) -> str:
    timestamp = dt.datetime.now().astimezone()
    rendered_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S %Z")
    cargo_version = command_output_now(["cargo", "--version"])
    rustc_version = command_output_now(["rustc", "--version"])
    stage_payloads = [load_stage_now(reports_dir, stage_name) for stage_name, _ in STAGE_ROWS]

    lines = [
        "# Final Testing Journal",
        "",
        f"- timestamp: {rendered_timestamp}",
        f"- repo: {repo_root}",
        f"- platform: {platform.platform()}",
        f"- machine: {platform.machine()}",
        f"- python: {platform.python_version()}",
        f"- cargo: {cargo_version}",
        f"- rustc: {rustc_version}",
        "",
        "## Commands Used",
        "",
        "- `./scripts/run_neo4j_smoke_ladder.sh`",
        "- `cargo build --release --manifest-path ./Cargo.toml`",
        "- `python benchmarks/walk_hopper_v1/bench_walk_vs_neo4j.py ... --knight-bus-bin ./target/release/knight-bus`",
        "- `./target/release/knight-bus bench-corpus --snapshot ... --nodes-csv ... --edges-csv ... --corpus ... --report ...`",
        "",
        "## Comparison Table",
        "",
        "| Dataset | Metric | Knight Bus Rust | Neo4j | Winner | Delta |",
        "| --- | --- | ---: | ---: | --- | --- |",
    ]

    for (stage_name, dataset_label), stage in zip(STAGE_ROWS, stage_payloads, strict=True):
        rust_measurement = stage["measurements"]["knight_bus_rust"]
        neo4j_measurement = stage["measurements"]["neo4j"]
        query_corpus_size = stage["payload"]["query_corpus_size"]
        metric_rows = [
            ("Status", rust_measurement["status"], neo4j_measurement["status"]),
            ("Query corpus", query_corpus_size, query_corpus_size),
            ("Open/start ms", rust_measurement["open_start_ms"], neo4j_measurement["open_start_ms"]),
            ("p50 hop ms", rust_measurement["p50_ms"], neo4j_measurement["p50_ms"]),
            ("p95 hop ms", rust_measurement["p95_ms"], neo4j_measurement["p95_ms"]),
            ("p99 hop ms", rust_measurement["p99_ms"], neo4j_measurement["p99_ms"]),
            ("Mean hop ms", rust_measurement["mean_ms"], neo4j_measurement["mean_ms"]),
            ("RSS bytes", rust_measurement["rss_bytes"], neo4j_measurement["rss_bytes"]),
        ]
        for metric_name, rust_value, neo4j_value in metric_rows:
            winner, delta = winner_row_now(metric_name, rust_value, neo4j_value)
            lines.append(
                "| {dataset} | {metric} | {rust_value} | {neo4j_value} | {winner} | {delta} |".format(
                    dataset=dataset_label,
                    metric=metric_name,
                    rust_value=format_value_now(rust_value),
                    neo4j_value=format_value_now(neo4j_value),
                    winner=winner,
                    delta=delta,
                )
            )
        if stage["import_meta"] is not None:
            lines.append(
                f"| {dataset_label} | Neo4j import ms | n/a | "
                f"{format_value_now(stage['import_meta']['import_duration_ms'])} | Neo4j | captured during rerun |"
            )

    lines.extend(
        [
            "",
            "## Honest Caveats",
            "",
            "- This is a `1 MB` and `50 MB` proof, not a `20 GB` proof.",
            "- The shared workload is the selected corpus of `forward_one`, `reverse_one`, and `reverse_two` queries.",
            "- Both engines are considered `ok` only if parity passed on the selected corpus.",
            "- Neo4j import duration is listed only because it was captured during this rerun.",
            "",
        ]
    )
    return "\n".join(lines)


def append_truth_journal_now(repo_root: Path, reports_dir: Path) -> str:
    timestamp = dt.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    lines = [
        "",
        f"## {timestamp} - Rust vs Neo4j Fresh Rerun",
        "",
        "| stage | raw_csv_bytes | node_count | edge_count | snapshot_size_bytes | rust_status | neo4j_status | query_corpus_size | rust_p50_ms | rust_p95_ms | rust_p99_ms | rust_mean_ms | rust_rss_bytes | neo4j_p50_ms | neo4j_p95_ms | neo4j_p99_ms | neo4j_mean_ms | neo4j_rss_bytes | import_duration_ms | report_path |",
        "| --- | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]

    for stage_name, _dataset_label in STAGE_ROWS:
        stage = load_stage_now(reports_dir, stage_name)
        payload = stage["payload"]
        rust_measurement = stage["measurements"]["knight_bus_rust"]
        neo4j_measurement = stage["measurements"]["neo4j"]
        import_duration_ms = (
            stage["import_meta"]["import_duration_ms"] if stage["import_meta"] is not None else None
        )
        lines.append(
            "| {stage_name} | {raw_csv_bytes} | {node_count} | {edge_count} | {snapshot_size_bytes} | "
            "{rust_status} | {neo4j_status} | {query_corpus_size} | {rust_p50} | {rust_p95} | {rust_p99} | "
            "{rust_mean} | {rust_rss} | {neo4j_p50} | {neo4j_p95} | {neo4j_p99} | {neo4j_mean} | "
            "{neo4j_rss} | {import_duration_ms} | {report_path} |".format(
                stage_name=stage_name,
                raw_csv_bytes=payload["dataset_manifest"]["actual_raw_bytes"],
                node_count=payload["dataset_manifest"]["node_count"],
                edge_count=payload["dataset_manifest"]["edge_count"],
                snapshot_size_bytes=snapshot_size_bytes_now(repo_root, stage_name, payload),
                rust_status=rust_measurement["status"],
                neo4j_status=neo4j_measurement["status"],
                query_corpus_size=payload["query_corpus_size"],
                rust_p50=format_value_now(rust_measurement["p50_ms"]),
                rust_p95=format_value_now(rust_measurement["p95_ms"]),
                rust_p99=format_value_now(rust_measurement["p99_ms"]),
                rust_mean=format_value_now(rust_measurement["mean_ms"]),
                rust_rss=format_value_now(rust_measurement["rss_bytes"]),
                neo4j_p50=format_value_now(neo4j_measurement["p50_ms"]),
                neo4j_p95=format_value_now(neo4j_measurement["p95_ms"]),
                neo4j_p99=format_value_now(neo4j_measurement["p99_ms"]),
                neo4j_mean=format_value_now(neo4j_measurement["mean_ms"]),
                neo4j_rss=format_value_now(neo4j_measurement["rss_bytes"]),
                import_duration_ms=format_value_now(import_duration_ms),
                report_path=stage["report_dir"] / "report.json",
            )
        )

    lines.extend(
        [
            "",
            "- verdict: fresh rerun completed through `neo4j_smoke_1mb` and `neo4j_preflight_50mb` using the Rust binary for the Knight Bus side.",
            "- caveat: this ledger entry is a `1 MB` / `50 MB` benchmark comparison only.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Render the final Rust-vs-Neo4j testing journal.")
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--reports-dir", type=Path, required=True)
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    reports_dir = args.reports_dir.resolve()

    final_journal_path = repo_root / "Final-Testing-Journal.md"
    truth_journal_path = repo_root / "docs" / "journal-tests-202604.md"

    final_journal_path.write_text(
        build_final_journal_now(repo_root, reports_dir) + "\n",
        encoding="utf-8",
    )
    with truth_journal_path.open("a", encoding="utf-8") as handle:
        handle.write(append_truth_journal_now(repo_root, reports_dir))

    print(final_journal_path)
    print(truth_journal_path)


if __name__ == "__main__":
    main()
