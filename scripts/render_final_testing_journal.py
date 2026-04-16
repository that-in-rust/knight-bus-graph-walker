#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import platform
import subprocess
from pathlib import Path
from typing import Any


DEFAULT_STAGE_ROWS = [
    ("neo4j_smoke_1mb", "1 MB"),
    ("neo4j_preflight_50mb", "50 MB"),
]


def read_json_file_now(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_summary_lines_now(raw_text: str) -> dict[str, Any]:
    payload: dict[str, Any] = {"phase_peaks": []}
    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("phase_peak "):
            _prefix, phase_name, peak_value = line.split(maxsplit=2)
            payload["phase_peaks"].append(
                {
                    "phase": phase_name,
                    "peak_rss_bytes": int(peak_value),
                }
            )
            continue
        if ": " not in line:
            continue
        key, value = line.split(": ", 1)
        if value.isdigit():
            payload[key] = int(value)
            continue
        try:
            payload[key] = float(value)
            continue
        except ValueError:
            payload[key] = value
    return payload


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


def parse_stage_rows_now(raw_stage_rows: list[str] | None) -> list[tuple[str, str]]:
    if not raw_stage_rows:
        return list(DEFAULT_STAGE_ROWS)
    stage_rows: list[tuple[str, str]] = []
    for raw_stage_row in raw_stage_rows:
        if "=" not in raw_stage_row:
            raise ValueError(f"invalid --stage value: {raw_stage_row}")
        stage_name, dataset_label = raw_stage_row.split("=", 1)
        if not stage_name or not dataset_label:
            raise ValueError(f"invalid --stage value: {raw_stage_row}")
        stage_rows.append((stage_name, dataset_label))
    return stage_rows


def load_optional_summary_now(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return parse_summary_lines_now(path.read_text(encoding="utf-8"))


def load_stage_now(reports_dir: Path, stage_name: str) -> dict[str, Any]:
    report_dir = reports_dir / stage_name
    payload = read_json_file_now(report_dir / "report.json")
    import_meta_path = report_dir / "import-meta.json"
    import_meta = read_json_file_now(import_meta_path) if import_meta_path.exists() else None
    rust_build = load_optional_summary_now(report_dir / "rust-build.txt")
    rust_verify = payload.get("rust_verify") or load_optional_summary_now(report_dir / "rust-verify.txt")
    return {
        "report_dir": report_dir,
        "payload": payload,
        "measurements": measurement_map_now(payload),
        "import_meta": import_meta,
        "rust_build": rust_build,
        "rust_verify": rust_verify,
    }


def command_output_now(command: list[str]) -> str:
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    return (result.stdout or result.stderr).strip() or "unknown"


def build_final_journal_now(
    repo_root: Path,
    reports_dir: Path,
    stage_rows: list[tuple[str, str]],
) -> str:
    timestamp = dt.datetime.now().astimezone()
    rendered_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S %Z")
    cargo_version = command_output_now(["cargo", "--version"])
    rustc_version = command_output_now(["rustc", "--version"])
    stage_payloads = [load_stage_now(reports_dir, stage_name) for stage_name, _ in stage_rows]
    is_fresh_check = any(stage_name.startswith("freshcheck_") for stage_name, _ in stage_rows)
    runner_name = "./scripts/run_neo4j_fresh_check.sh" if is_fresh_check else "./scripts/run_neo4j_smoke_ladder.sh"

    lines = [
        "# Final Testing Journal v002" if is_fresh_check else "# Final Testing Journal",
        "",
        f"- timestamp: {rendered_timestamp}",
        f"- repo: {repo_root}",
        f"- platform: {platform.platform()}",
        f"- machine: {platform.machine()}",
        f"- python: {platform.python_version()}",
        f"- cargo: {cargo_version}",
        f"- rustc: {rustc_version}",
        "",
        "## Measurement Contract",
        "",
        "- This record is the corrected runtime-only benchmark set for the fixed `1 MB`, `50 MB`, and `2 GB` fresh check.",
        "- Knight Bus Rust RSS is `runtime_process_only` and comes from the standalone `bench-corpus` runtime process.",
        "- Neo4j RSS is `server_process_only` and is sampled from the Neo4j JVM/server process, not the Python client.",
        "- Rust correctness is enforced before timing through `knight-bus verify`; Neo4j correctness is enforced on the fixed shared corpus against Python truth answers.",
        "- The archived `v001-learnings` files remain untouched as historical evidence; this v002 journal supersedes the old RSS interpretation only.",
        "",
        "## Commands Used",
        "",
        f"- `{runner_name}`",
        "- `cargo build --release --manifest-path ./Cargo.toml`",
        "- `./target/release/knight-bus build --nodes-csv ... --edges-csv ... --output ...`",
        "- `./target/release/knight-bus verify --snapshot ... --nodes-csv ... --edges-csv ...`",
        "- `./target/release/knight-bus bench-corpus --snapshot ... --corpus ... --report ...`",
        "- `python benchmarks/walk_hopper_v1/bench_walk_vs_neo4j.py --dataset ... --snapshot ... --corpus ... --report ...`",
        "",
        "## Runtime Comparison",
        "",
        "| Dataset | Metric | Knight Bus Rust | Neo4j | Winner | Delta |",
        "| --- | --- | ---: | ---: | --- | --- |",
    ]

    for (stage_name, dataset_label), stage in zip(stage_rows, stage_payloads, strict=True):
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
        lines.append(
            f"| {dataset_label} | Rust RSS scope | {rust_measurement['rss_scope']} | n/a | info | runtime-only |"
        )
        lines.append(
            f"| {dataset_label} | Neo4j RSS scope | n/a | {neo4j_measurement['rss_scope']} | info | server-only |"
        )
        if stage["import_meta"] is not None:
            lines.append(
                f"| {dataset_label} | Neo4j import ms | n/a | "
                f"{format_value_now(stage['import_meta']['import_duration_ms'])} | Neo4j | captured during rerun |"
            )

    lines.extend(
        [
            "",
            "## Knight Bus Phase Costs",
            "",
            "| Dataset | Build peak RSS bytes | Verify peak RSS bytes | Runtime-only RSS bytes | Build RSS source | Verify RSS source | Runtime RSS source |",
            "| --- | ---: | ---: | ---: | --- | --- | --- |",
        ]
    )

    for (stage_name, dataset_label), stage in zip(stage_rows, stage_payloads, strict=True):
        rust_measurement = stage["measurements"]["knight_bus_rust"]
        rust_build = stage["rust_build"] or {}
        rust_verify = stage["rust_verify"] or {}
        lines.append(
            "| {dataset} | {build_peak} | {verify_peak} | {runtime_peak} | {build_source} | {verify_source} | {runtime_source} |".format(
                dataset=dataset_label,
                build_peak=format_value_now(rust_build.get("peak_rss_bytes")),
                verify_peak=format_value_now(rust_verify.get("peak_rss_bytes")),
                runtime_peak=format_value_now(rust_measurement.get("rss_bytes")),
                build_source=format_value_now(rust_build.get("peak_rss_source")),
                verify_source=format_value_now(rust_verify.get("peak_rss_source")),
                runtime_source=format_value_now(rust_measurement.get("rss_source")),
            )
        )

    lines.extend(
        [
            "",
            "## Honest Notes",
            "",
            "- These journals use the three existing artifact datasets and their fixed `query_corpus.csv` files; no dataset regeneration is part of this rerun.",
            "- Knight Bus build and verify costs are reported separately because runtime-only walker RSS is not the whole operating picture.",
            "- Neo4j import duration is listed only because it was captured during the same fresh check.",
            "",
        ]
    )
    return "\n".join(lines)


def build_truth_journal_now(
    repo_root: Path,
    reports_dir: Path,
    stage_rows: list[tuple[str, str]],
) -> str:
    timestamp = dt.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    lines = [
        "# journal-tests-202604-v002",
        "",
        f"- generated_at: {timestamp}",
        f"- repo: {repo_root}",
        "- note: this is the corrected runtime-only fresh-check ledger; `v001-learnings` remains archived history.",
        "",
        "| stage | dataset | raw_csv_bytes | node_count | edge_count | snapshot_size_bytes | rust_status | neo4j_status | query_corpus_size | rust_p50_ms | rust_p95_ms | rust_p99_ms | rust_mean_ms | rust_rss_bytes | rust_rss_scope | rust_rss_source | rust_verify_peak_rss | neo4j_p50_ms | neo4j_p95_ms | neo4j_p99_ms | neo4j_mean_ms | neo4j_rss_bytes | neo4j_rss_scope | neo4j_rss_source | import_duration_ms | report_path |",
        "| --- | --- | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | --- |",
    ]

    for stage_name, dataset_label in stage_rows:
        stage = load_stage_now(reports_dir, stage_name)
        payload = stage["payload"]
        rust_measurement = stage["measurements"]["knight_bus_rust"]
        neo4j_measurement = stage["measurements"]["neo4j"]
        rust_verify = stage["rust_verify"] or {}
        import_duration_ms = (
            stage["import_meta"]["import_duration_ms"] if stage["import_meta"] is not None else None
        )
        lines.append(
            "| {stage_name} | {dataset_label} | {raw_csv_bytes} | {node_count} | {edge_count} | {snapshot_size_bytes} | "
            "{rust_status} | {neo4j_status} | {query_corpus_size} | {rust_p50} | {rust_p95} | {rust_p99} | "
            "{rust_mean} | {rust_rss} | {rust_scope} | {rust_source} | {rust_verify_peak} | {neo4j_p50} | "
            "{neo4j_p95} | {neo4j_p99} | {neo4j_mean} | {neo4j_rss} | {neo4j_scope} | {neo4j_source} | "
            "{import_duration_ms} | {report_path} |".format(
                stage_name=stage_name,
                dataset_label=dataset_label,
                raw_csv_bytes=payload["dataset_manifest"]["actual_raw_bytes"],
                node_count=payload["dataset_manifest"]["node_count"],
                edge_count=payload["dataset_manifest"]["edge_count"],
                snapshot_size_bytes=payload.get("snapshot_size_bytes"),
                rust_status=rust_measurement["status"],
                neo4j_status=neo4j_measurement["status"],
                query_corpus_size=payload["query_corpus_size"],
                rust_p50=format_value_now(rust_measurement["p50_ms"]),
                rust_p95=format_value_now(rust_measurement["p95_ms"]),
                rust_p99=format_value_now(rust_measurement["p99_ms"]),
                rust_mean=format_value_now(rust_measurement["mean_ms"]),
                rust_rss=format_value_now(rust_measurement["rss_bytes"]),
                rust_scope=rust_measurement["rss_scope"],
                rust_source=rust_measurement["rss_source"],
                rust_verify_peak=format_value_now(rust_verify.get("peak_rss_bytes")),
                neo4j_p50=format_value_now(neo4j_measurement["p50_ms"]),
                neo4j_p95=format_value_now(neo4j_measurement["p95_ms"]),
                neo4j_p99=format_value_now(neo4j_measurement["p99_ms"]),
                neo4j_mean=format_value_now(neo4j_measurement["mean_ms"]),
                neo4j_rss=format_value_now(neo4j_measurement["rss_bytes"]),
                neo4j_scope=neo4j_measurement["rss_scope"],
                neo4j_source=neo4j_measurement["rss_source"],
                import_duration_ms=format_value_now(import_duration_ms),
                report_path=stage["report_dir"] / "report.json",
            )
        )

    lines.extend(
        [
            "",
            "- verdict: corrected fresh check completed against the three fixed datasets with runtime-only Rust RSS and server-process Neo4j RSS.",
            "- note: Knight Bus build and verify costs are intentionally recorded outside the runtime-only comparison headline.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Render the Rust-vs-Neo4j testing journals.")
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--reports-dir", type=Path, required=True)
    parser.add_argument("--final-journal", type=Path, default=None)
    parser.add_argument("--truth-journal", type=Path, default=None)
    parser.add_argument("--stage", action="append", default=None)
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    reports_dir = args.reports_dir.resolve()
    stage_rows = parse_stage_rows_now(args.stage)

    final_journal_path = (
        args.final_journal.resolve()
        if args.final_journal is not None
        else repo_root / "v001-learnings" / "Final-Testing-Journal.md"
    )
    truth_journal_path = (
        args.truth_journal.resolve()
        if args.truth_journal is not None
        else repo_root / "v001-learnings" / "journal-tests-202604.md"
    )

    final_journal_path.write_text(
        build_final_journal_now(repo_root, reports_dir, stage_rows) + "\n",
        encoding="utf-8",
    )

    truth_contents = build_truth_journal_now(repo_root, reports_dir, stage_rows)
    if args.truth_journal is None:
        with truth_journal_path.open("a", encoding="utf-8") as handle:
            handle.write("\n" + truth_contents + "\n")
    else:
        truth_journal_path.write_text(truth_contents + "\n", encoding="utf-8")

    print(final_journal_path)
    print(truth_journal_path)


if __name__ == "__main__":
    main()
