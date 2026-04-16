#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
RUN_ROOT = Path(tempfile.gettempdir()) / "knight-bus-three-tier-202604"
JOURNAL_PATH = REPO_ROOT / "docs" / "journal-tests-202604.md"
GENERATOR_PATH = REPO_ROOT / "benchmarks" / "walk_hopper_v1" / "generate_code_sparse_data.py"
BINARY_PATH = REPO_ROOT / "target" / "release" / "knight-bus"

DEFAULT_SEED = 7
DEFAULT_LAYER_COUNT = 64
DEFAULT_DEGREE_PALETTE = "6,8,10,12,14"
PREFLIGHT_VERIFY_TIMEOUT_SECONDS = 300


@dataclass(frozen=True)
class TierSpec:
    key: str
    label: str
    role: str
    dataset_kind: str
    nodes_csv: Path | None = None
    edges_csv: Path | None = None
    target_raw_bytes: int | None = None
    verify_timeout_seconds: int | None = None


def run_command_now(
    args: list[str],
    *,
    cwd: Path = REPO_ROOT,
    timeout_seconds: int | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
        timeout=timeout_seconds,
    )


def require_command_success_now(
    completed: subprocess.CompletedProcess[str],
    *,
    context: str,
) -> subprocess.CompletedProcess[str]:
    if completed.returncode == 0:
        return completed

    output = "\n".join(
        part
        for part in (
            f"stdout:\n{completed.stdout.strip()}" if completed.stdout.strip() else "",
            f"stderr:\n{completed.stderr.strip()}" if completed.stderr.strip() else "",
        )
        if part
    )
    raise RuntimeError(f"{context} failed with exit code {completed.returncode}\n{output}".strip())


def count_effective_rows_now(csv_path: Path) -> int:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        return sum(
            1
            for row in reader
            if any(cell.strip() for cell in row)
        )


def collect_dataset_summary_now(nodes_csv: Path, edges_csv: Path) -> dict[str, int]:
    return {
        "raw_csv_bytes": nodes_csv.stat().st_size + edges_csv.stat().st_size,
        "node_count": count_effective_rows_now(nodes_csv),
        "edge_count": count_effective_rows_now(edges_csv),
    }


def collect_directory_size_now(path: Path) -> int:
    return sum(entry.stat().st_size for entry in path.rglob("*") if entry.is_file())


def extract_cli_value_now(stdout: str, key: str) -> str | None:
    prefix = f"{key}: "
    for line in stdout.splitlines():
        if line.startswith(prefix):
            return line[len(prefix) :].strip()
    return None


def extract_verify_checked_queries_now(stdout: str) -> int | None:
    raw_value = extract_cli_value_now(stdout, "checked_queries")
    if raw_value is None:
        return None
    return int(raw_value)


def extract_family_latency_now(report: dict[str, Any], family_label: str) -> str:
    for family_report in report["families"]:
        if family_report["family"] == family_label:
            return f"{family_report['p50_nanos']}/{family_report['p95_nanos']}"
    return "n/a"


def generate_dataset_now(tier: TierSpec, dataset_dir: Path) -> tuple[Path, Path]:
    dataset_dir.mkdir(parents=True, exist_ok=True)
    completed = run_command_now(
        [
            sys.executable,
            str(GENERATOR_PATH),
            "--target-raw-bytes",
            str(tier.target_raw_bytes),
            "--seed",
            str(DEFAULT_SEED),
            "--layer-count",
            str(DEFAULT_LAYER_COUNT),
            "--degree-palette",
            DEFAULT_DEGREE_PALETTE,
            "--output",
            str(dataset_dir),
        ]
    )
    require_command_success_now(completed, context=f"{tier.label} generation")
    return dataset_dir / "nodes.csv", dataset_dir / "edges.csv"


def materialize_dataset_now(tier: TierSpec, tier_root: Path) -> tuple[Path, Path]:
    if tier.dataset_kind == "existing":
        assert tier.nodes_csv is not None
        assert tier.edges_csv is not None
        return tier.nodes_csv, tier.edges_csv
    return generate_dataset_now(tier, tier_root / "dataset")


def ensure_release_binary_now() -> None:
    completed = run_command_now(["cargo", "build", "--release"])
    require_command_success_now(completed, context="cargo build --release")


def run_tier_now(tier: TierSpec) -> dict[str, Any]:
    tier_root = RUN_ROOT / tier.key
    if tier_root.exists():
        shutil.rmtree(tier_root)
    tier_root.mkdir(parents=True, exist_ok=True)

    nodes_csv, edges_csv = materialize_dataset_now(tier, tier_root)
    dataset_summary = collect_dataset_summary_now(nodes_csv, edges_csv)
    snapshot_dir = tier_root / "snapshot"
    report_dir = tier_root / "report"

    build_started_at = time.perf_counter()
    build_completed = run_command_now(
        [
            str(BINARY_PATH),
            "build",
            "--nodes-csv",
            str(nodes_csv),
            "--edges-csv",
            str(edges_csv),
            "--output",
            str(snapshot_dir),
        ]
    )
    require_command_success_now(build_completed, context=f"{tier.label} build")
    build_duration_seconds = time.perf_counter() - build_started_at

    snapshot_size_bytes = collect_directory_size_now(snapshot_dir)

    verify_status = "ok"
    checked_queries: int | None = None
    verify_started_at = time.perf_counter()
    try:
        verify_completed = run_command_now(
            [
                str(BINARY_PATH),
                "verify",
                "--snapshot",
                str(snapshot_dir),
                "--nodes-csv",
                str(nodes_csv),
                "--edges-csv",
                str(edges_csv),
            ],
            timeout_seconds=tier.verify_timeout_seconds,
        )
        require_command_success_now(verify_completed, context=f"{tier.label} verify")
        checked_queries = extract_verify_checked_queries_now(verify_completed.stdout)
    except subprocess.TimeoutExpired:
        verify_status = f"timeout_{tier.verify_timeout_seconds}s"
    verify_duration_seconds = time.perf_counter() - verify_started_at

    bench_started_at = time.perf_counter()
    bench_completed = run_command_now(
        [
            str(BINARY_PATH),
            "bench",
            "--snapshot",
            str(snapshot_dir),
            "--report",
            str(report_dir),
        ]
    )
    require_command_success_now(bench_completed, context=f"{tier.label} bench")
    bench_duration_seconds = time.perf_counter() - bench_started_at

    bench_report = json.loads((report_dir / "bench-report.json").read_text(encoding="utf-8"))

    verdict = {
        "tiny_checked_in_toy": "correctness only, latency not representative",
        "real_smoke_dataset": "representative smoke tier",
        "planned_preflight_dataset": "preflight tier",
    }[tier.key]

    return {
        "tier_key": tier.key,
        "tier_label": tier.label,
        "role": tier.role,
        "source": (
            str(nodes_csv.parent.relative_to(REPO_ROOT))
            if nodes_csv.is_relative_to(REPO_ROOT)
            else str(nodes_csv.parent)
        ),
        "raw_csv_bytes": dataset_summary["raw_csv_bytes"],
        "node_count": dataset_summary["node_count"],
        "edge_count": dataset_summary["edge_count"],
        "snapshot_size_bytes": snapshot_size_bytes,
        "build_duration_seconds": build_duration_seconds,
        "verify_status": verify_status,
        "verify_duration_seconds": verify_duration_seconds,
        "checked_queries": checked_queries,
        "bench_duration_seconds": bench_duration_seconds,
        "forward_one_latency_ns": extract_family_latency_now(bench_report, "forward_one"),
        "backward_one_latency_ns": extract_family_latency_now(bench_report, "backward_one"),
        "forward_two_latency_ns": extract_family_latency_now(bench_report, "forward_two"),
        "backward_two_latency_ns": extract_family_latency_now(bench_report, "backward_two"),
        "peak_rss_bytes": bench_report["peak_rss_bytes"],
        "peak_rss_source": bench_report["peak_rss_source"],
        "verdict": verdict,
    }


def format_optional_int_now(value: int | None) -> str:
    return str(value) if value is not None else "n/a"


def append_journal_entry_now(results: list[dict[str, Any]]) -> None:
    timestamp = datetime.now().astimezone()
    timestamp_label = timestamp.strftime("%Y-%m-%d %H:%M:%S %Z")
    lines = [
        "",
        "---",
        "",
        f"## {timestamp_label} — Three-Tier Dataset Validation",
        "",
        "### Runner Defaults",
        "",
        f"- generator seed: `{DEFAULT_SEED}`",
        f"- generator layer count: `{DEFAULT_LAYER_COUNT}`",
        f"- generator degree palette: `{DEFAULT_DEGREE_PALETTE}`",
        f"- preflight verify timeout: `{PREFLIGHT_VERIFY_TIMEOUT_SECONDS}s`",
        f"- run root: `{RUN_ROOT}`",
        "",
        "### Result Table",
        "",
        "| tier | source | raw_csv_bytes | nodes | edges | snapshot_bytes | build_s | verify_status | checked_queries | forward_one ns p50/p95 | backward_one ns p50/p95 | forward_two ns p50/p95 | backward_two ns p50/p95 | peak_rss_bytes | peak_rss_source | verdict |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | --- | --- | --- | --- | ---: | --- | --- |",
    ]

    for result in results:
        lines.append(
            "| {tier_label} | `{source}` | {raw_csv_bytes} | {node_count} | {edge_count} | {snapshot_size_bytes} | {build_duration_seconds:.3f} | {verify_status} | {checked_queries} | {forward_one_latency_ns} | {backward_one_latency_ns} | {forward_two_latency_ns} | {backward_two_latency_ns} | {peak_rss_bytes} | `{peak_rss_source}` | {verdict} |".format(
                tier_label=result["tier_label"],
                source=result["source"],
                raw_csv_bytes=result["raw_csv_bytes"],
                node_count=result["node_count"],
                edge_count=result["edge_count"],
                snapshot_size_bytes=result["snapshot_size_bytes"],
                build_duration_seconds=result["build_duration_seconds"],
                verify_status=result["verify_status"],
                checked_queries=format_optional_int_now(result["checked_queries"]),
                forward_one_latency_ns=result["forward_one_latency_ns"],
                backward_one_latency_ns=result["backward_one_latency_ns"],
                forward_two_latency_ns=result["forward_two_latency_ns"],
                backward_two_latency_ns=result["backward_two_latency_ns"],
                peak_rss_bytes=result["peak_rss_bytes"],
                peak_rss_source=result["peak_rss_source"],
                verdict=result["verdict"],
            )
        )

    lines.extend(
        [
            "",
            "### Notes",
            "",
            "- The smoke and preflight tiers use raw CSV size as the canonical target. The logged node and edge counts are the actual measured outputs of the deterministic generator.",
            "- The tiny checked-in toy remains a correctness-only tier.",
            "- `peak_rss_bytes` is now logged together with `peak_rss_source` so the measurement provenance is explicit.",
        ]
    )

    with JOURNAL_PATH.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def print_results_now(results: list[dict[str, Any]]) -> None:
    print("Three-tier validation complete.")
    for result in results:
        print(
            f"- {result['tier_label']}: raw_csv_bytes={result['raw_csv_bytes']} "
            f"nodes={result['node_count']} edges={result['edge_count']} "
            f"snapshot_bytes={result['snapshot_size_bytes']} verify={result['verify_status']} "
            f"peak_rss_bytes={result['peak_rss_bytes']} source={result['peak_rss_source']}"
        )


def main() -> int:
    tiers = [
        TierSpec(
            key="tiny_checked_in_toy",
            label="tiny_checked_in_toy",
            role="correctness-only tier",
            dataset_kind="existing",
            nodes_csv=REPO_ROOT
            / "benchmarks"
            / "walk_hopper_v1"
            / "fixtures"
            / "tiny_graph"
            / "nodes.csv",
            edges_csv=REPO_ROOT
            / "benchmarks"
            / "walk_hopper_v1"
            / "fixtures"
            / "tiny_graph"
            / "edges.csv",
        ),
        TierSpec(
            key="real_smoke_dataset",
            label="real_smoke_dataset",
            role="representative smoke tier",
            dataset_kind="generated",
            target_raw_bytes=int(1.07 * 1024 * 1024),
        ),
        TierSpec(
            key="planned_preflight_dataset",
            label="planned_preflight_dataset",
            role="preflight tier",
            dataset_kind="generated",
            target_raw_bytes=int(52.4 * 1024 * 1024),
            verify_timeout_seconds=PREFLIGHT_VERIFY_TIMEOUT_SECONDS,
        ),
    ]

    ensure_release_binary_now()

    results: list[dict[str, Any]] = []
    for tier in tiers:
        results.append(run_tier_now(tier))

    append_journal_entry_now(results)
    print_results_now(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
