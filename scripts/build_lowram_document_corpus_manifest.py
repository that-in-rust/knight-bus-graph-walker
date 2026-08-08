#!/usr/bin/env python3
"""Build the frozen denominator for the PRD03-PRD06 architecture corpus."""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
from pathlib import Path
import sys


CORPUS_ROOTS = ("docs_PRD03", "docs_PRD04", "docs_PRD05", "docs_PRD06")
OUTPUT_SUBTREE = Path("docs_PRD04/reference-learning/lowram-architecture-corpus")


def assign_document_lane_now(relative_path: Path) -> tuple[str, str]:
    root_name = relative_path.parts[0]
    if root_name == "docs_PRD03":
        return "04-prd03", "agent-04"
    if root_name == "docs_PRD04":
        return "05-prd04", "agent-05"
    if root_name in {"docs_PRD05", "docs_PRD06"}:
        return "06-prd05-prd06", "agent-06"
    raise RuntimeError(f"unassigned document root: {relative_path}")


def classify_document_file_now(relative_path: Path, size_bytes: int) -> str:
    extension = relative_path.suffix.lower()
    if extension == ".md":
        return "semantic_text_candidate"
    if extension == ".txt":
        return "bulk_text_candidate" if size_bytes >= 1_000_000 else "semantic_text_candidate"
    if extension in {".tsv", ".dot"}:
        return "generated_structured_candidate"
    if extension == ".sqlite":
        return "structured_database_candidate"
    if extension == ".xlsx":
        return "structured_workbook_candidate"
    if extension in {".py", ".sh"}:
        return "source_script_candidate"
    return "unknown_candidate"


def hash_file_content_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source_file:
        while chunk := source_file.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def collect_document_corpus_rows(workspace_root: Path) -> list[dict[str, object]]:
    output_root = (workspace_root / OUTPUT_SUBTREE).resolve()
    rows: list[dict[str, object]] = []
    seen_paths: set[str] = set()
    for root_name in CORPUS_ROOTS:
        corpus_root = workspace_root / root_name
        if not corpus_root.is_dir():
            raise RuntimeError(f"missing corpus root: {corpus_root}")
        for path in corpus_root.rglob("*"):
            if not path.is_file():
                continue
            resolved_path = path.resolve()
            if resolved_path == output_root or output_root in resolved_path.parents:
                continue
            relative_path = path.relative_to(workspace_root)
            path_text = relative_path.as_posix()
            if path_text in seen_paths:
                raise RuntimeError(f"duplicate corpus path: {path_text}")
            seen_paths.add(path_text)
            size_bytes = path.stat().st_size
            lane, assigned_agent = assign_document_lane_now(relative_path)
            rows.append(
                {
                    "lane": lane,
                    "path": path_text,
                    "sha256": hash_file_content_sha256(path),
                    "bytes": size_bytes,
                    "extension": relative_path.suffix.lower().lstrip("."),
                    "default_file_class": classify_document_file_now(relative_path, size_bytes),
                    "assigned_agent": assigned_agent,
                }
            )
    return sorted(rows, key=lambda row: (str(row["lane"]), str(row["path"])))


def write_document_denominator_now(output_path: Path, rows: list[dict[str, object]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "lane", "path", "sha256", "bytes", "extension",
        "default_file_class", "assigned_agent",
    ]
    temporary_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
    with temporary_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary_path, output_path)


def parse_manifest_arguments_now() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create the initial frozen PRD03-PRD06 denominator."
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="explicitly replace an existing denominator with current filesystem hashes",
    )
    return parser.parse_args()


def run_document_manifest_now(refresh_existing: bool = False) -> int:
    workspace_root = Path(__file__).resolve().parents[1]
    output_path = workspace_root / OUTPUT_SUBTREE / "evidence" / "all-documents-denominator.tsv"
    if output_path.is_file() and not refresh_existing:
        print(
            f"preserved frozen denominator at {output_path}; "
            "pass --refresh only when intentionally starting a new evidence baseline"
        )
        return 0
    rows = collect_document_corpus_rows(workspace_root)
    write_document_denominator_now(output_path, rows)
    lane_counts: dict[str, int] = {}
    for row in rows:
        lane = str(row["lane"])
        lane_counts[lane] = lane_counts.get(lane, 0) + 1
    print(
        f"wrote {len(rows)} document rows "
        f"({sum(int(row['bytes']) for row in rows)} bytes) to {output_path}"
    )
    for lane, count in sorted(lane_counts.items()):
        print(f"{lane}: {count} files")
    return 0


if __name__ == "__main__":
    arguments = parse_manifest_arguments_now()
    sys.exit(run_document_manifest_now(refresh_existing=arguments.refresh))
