from __future__ import annotations

from pathlib import Path
from typing import Any


def format_bytes_human_now(raw_bytes: int | None) -> str:
    if raw_bytes is None:
        return "n/a"
    if raw_bytes >= 1024 * 1024 * 1024:
        return f"{raw_bytes / (1024 * 1024 * 1024):.2f} GB"
    if raw_bytes >= 1024 * 1024:
        return f"{raw_bytes / (1024 * 1024):.1f} MB"
    if raw_bytes >= 1024:
        return f"{raw_bytes / 1024:.1f} KB"
    return f"{raw_bytes} B"


def format_latency_human_now(raw_ms: float | None) -> str:
    if raw_ms is None:
        return "n/a"
    if raw_ms >= 1000.0:
        return f"{raw_ms / 1000.0:.2f} s"
    if raw_ms >= 1.0:
        return f"{raw_ms:.1f} ms"
    return f"{raw_ms * 1000.0:.1f} us"


def parity_text_now(entry: dict[str, Any]) -> str:
    if not entry.get("parity_checked"):
        return "not checked"
    if entry.get("parity_passed"):
        return "yes"
    return "no"


def render_v003_readme_now(summary_payload: dict[str, Any]) -> str:
    dataset_manifest = summary_payload["dataset_manifest"]
    baseline = summary_payload["baseline"]
    competitors = summary_payload["competitors"]
    ok_backends = [item for item in competitors if item["status"] == "ok"]
    unsupported_backends = [item["backend_name"] for item in competitors if item["status"] == "unsupported"]
    failed_backends = [item["backend_name"] for item in competitors if item["status"] == "failed"]

    lines = [
        "# v003 Research",
        "",
        "This folder records the fixed `2 GB` competitor matrix for Knight Bus.",
        "",
        "## What This Matrix Proves",
        "",
        "- Knight Bus completed the fixed `2 GB` graph corpus on the current machine.",
        f"- Knight Bus query p99 on this corpus was `{format_latency_human_now(baseline.get('p99_ms'))}` with runtime RSS `{format_bytes_human_now(baseline.get('rss_bytes'))}`.",
    ]
    if ok_backends:
        joined_ok = ", ".join(item["backend_name"] for item in ok_backends)
        lines.append(f"- Live external backend results are currently available for `{joined_ok}`.")
    else:
        lines.append("- No external competitor completed a live run in the current matrix yet.")
    if unsupported_backends:
        lines.append(
            "- The remaining backends are still listed with honest `unsupported` status instead of being silently omitted: "
            + ", ".join(f"`{name}`" for name in unsupported_backends)
            + "."
        )
    if failed_backends:
        lines.append(
            "- Some backends reached the harness but still failed during import, startup, or parity checks: "
            + ", ".join(f"`{name}`" for name in failed_backends)
            + "."
        )
    lines.extend(
        [
            "",
            "## Current Dataset Contract",
            "",
            f"- graph model: `{dataset_manifest.get('graph_model')}`",
            f"- raw dataset size: `{format_bytes_human_now(dataset_manifest.get('actual_raw_bytes'))}`",
            f"- node count: `{dataset_manifest.get('node_count')}`",
            f"- edge count: `{dataset_manifest.get('edge_count')}`",
            f"- query corpus size: `{summary_payload.get('query_corpus_size')}`",
            "",
            "## Current Backend Status",
            "",
            "| Backend | Status | Same answers | p99 | Runtime RSS | Reason |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    lines.append(
        "| Knight Bus Rust | {status} | verifier | {p99} | {rss} | {reason} |".format(
            status=baseline.get("status"),
            p99=format_latency_human_now(baseline.get("p99_ms")),
            rss=format_bytes_human_now(baseline.get("rss_bytes")),
            reason=baseline.get("reason") or "",
        )
    )
    for competitor in competitors:
        lines.append(
            "| {backend_name} | {status} | {parity} | {p99} | {rss} | {reason} |".format(
                backend_name=competitor["backend_name"],
                status=competitor["status"],
                parity=parity_text_now(competitor),
                p99=format_latency_human_now(competitor.get("p99_ms")),
                rss=format_bytes_human_now(competitor.get("rss_bytes")),
                reason=competitor.get("reason") or "",
            )
        )
    lines.extend(
        [
            "",
            "## Exact Records",
            "",
            "- [Competitor Matrix](./competitor-2gb-benchmark.md)",
            "- ignored raw reports: `reports/v003_2gb_competitors/...`",
            "- ignored dataset inputs: `artifacts/code_sparse_2gb/...`",
        ]
    )
    return "\n".join(lines) + "\n"


def render_competitor_matrix_now(summary_payload: dict[str, Any]) -> str:
    dataset_manifest = summary_payload["dataset_manifest"]
    environment = summary_payload["environment"]
    baseline = summary_payload["baseline"]
    competitors = summary_payload["competitors"]

    lines = [
        "# 2 GB Competitor Benchmark Matrix",
        "",
        "This file is the tracked narrative view of the current `v003` 2 GB competitor study.",
        "",
        "## Benchmark Contract",
        "",
        "- fixed shared dataset: `artifacts/code_sparse_2gb`",
        "- fixed query families: `forward_one`, `reverse_one`, `reverse_two`",
        "- source of truth: CSV truth evaluator in `benchmarks/walk_hopper_v1`",
        "- baseline: Knight Bus Rust",
        "- machine: `{platform}` / `{machine}` / `{ram}` RAM".format(
            platform=environment.get("platform"),
            machine=environment.get("machine"),
            ram=format_bytes_human_now(environment.get("ram_total_bytes")),
        ),
        "",
        "## Dataset Shape",
        "",
        f"- raw bytes: `{format_bytes_human_now(dataset_manifest.get('actual_raw_bytes'))}`",
        f"- nodes: `{dataset_manifest.get('node_count')}`",
        f"- edges: `{dataset_manifest.get('edge_count')}`",
        f"- snapshot size: `{format_bytes_human_now(summary_payload.get('snapshot_size_bytes'))}`",
        f"- query corpus rows: `{summary_payload.get('query_corpus_size')}`",
        "",
        "## Knight Bus Baseline",
        "",
        "| Engine | Status | Open | p50 | p95 | p99 | Mean | Runtime RSS | Version |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        (
            "| Knight Bus Rust | {status} | {open_ms} | {p50} | {p95} | {p99} | {mean_ms} | {rss} | {version} |".format(
                status=baseline.get("status"),
                open_ms=format_latency_human_now(baseline.get("open_start_ms")),
                p50=format_latency_human_now(baseline.get("p50_ms")),
                p95=format_latency_human_now(baseline.get("p95_ms")),
                p99=format_latency_human_now(baseline.get("p99_ms")),
                mean_ms=format_latency_human_now(baseline.get("mean_ms")),
                rss=format_bytes_human_now(baseline.get("rss_bytes")),
                version=baseline.get("version") or "",
            )
        ),
        "",
        "## Competitor Status Matrix",
        "",
        "| Backend | Query layer | Status | Same answers | Import | Open | p50 | p95 | p99 | Runtime RSS | Version | Reason |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for competitor in competitors:
        lines.append(
            "| {backend_name} | {query_language} | {status} | {parity} | {import_ms} | {open_ms} | {p50} | {p95} | {p99} | {rss} | {version} | {reason} |".format(
                backend_name=competitor["backend_name"],
                query_language=competitor["query_language"],
                status=competitor["status"],
                parity=parity_text_now(competitor),
                import_ms=format_latency_human_now(competitor.get("import_duration_ms")),
                open_ms=format_latency_human_now(competitor.get("open_start_ms")),
                p50=format_latency_human_now(competitor.get("p50_ms")),
                p95=format_latency_human_now(competitor.get("p95_ms")),
                p99=format_latency_human_now(competitor.get("p99_ms")),
                rss=format_bytes_human_now(competitor.get("rss_bytes")),
                version=competitor.get("version") or "",
                reason=competitor.get("reason") or "",
            )
        )
    lines.extend(
        [
            "",
            "## Per-Backend Notes",
            "",
        ]
    )
    for competitor in competitors:
        lines.extend(
            [
                f"### {competitor['backend_name']}",
                "",
                f"- status: `{competitor['status']}`",
                f"- query language: `{competitor['query_language']}`",
                f"- same answers: `{parity_text_now(competitor)}`",
                f"- reason: `{competitor.get('reason') or 'none'}`",
                f"- source note: `{competitor.get('source_note') or 'none'}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Raw Artifacts",
            "",
            "- machine-readable summary: `reports/v003_2gb_competitors/summary.json`",
            "- per-backend raw reports: `reports/v003_2gb_competitors/<backend>/report.json`",
            "- raw input dataset: `artifacts/code_sparse_2gb/...`",
        ]
    )
    return "\n".join(lines) + "\n"


def write_v003_markdown_bundle_now(summary_payload: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "README.md").write_text(render_v003_readme_now(summary_payload), encoding="utf-8")
    (output_dir / "competitor-2gb-benchmark.md").write_text(
        render_competitor_matrix_now(summary_payload),
        encoding="utf-8",
    )
