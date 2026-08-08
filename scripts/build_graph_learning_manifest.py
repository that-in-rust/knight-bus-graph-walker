#!/usr/bin/env python3
"""Freeze and balance the graph-learning corpus across three reading lanes."""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
from pathlib import Path
import sys


CORPUS_SUBTREE = Path("docs_PRD06/graph-learning")
EVIDENCE_SUBTREE = Path(
    "docs_PRD06/reference-learning/all-algorithm-lowram/evidence"
)
LANE_IDENTIFIERS = ("07", "08", "09")


def hash_file_content_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source_file:
        while chunk := source_file.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def count_logical_lines_now(path: Path) -> int:
    return len(path.read_text(encoding="utf-8").splitlines())


def collect_corpus_file_rows(workspace_root: Path) -> list[dict[str, object]]:
    corpus_root = workspace_root / CORPUS_SUBTREE
    if not corpus_root.is_dir():
        raise RuntimeError(f"missing graph-learning corpus: {corpus_root}")

    rows: list[dict[str, object]] = []
    for path in sorted(corpus_root.rglob("*")):
        if not path.is_file():
            continue
        relative_path = path.relative_to(workspace_root).as_posix()
        rows.append(
            {
                "path": relative_path,
                "sha256": hash_file_content_sha256(path),
                "bytes": path.stat().st_size,
                "line_count": count_logical_lines_now(path),
            }
        )
    return rows


def assign_balanced_lanes_now(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    lane_lines = {lane: 0 for lane in LANE_IDENTIFIERS}
    lane_files = {lane: 0 for lane in LANE_IDENTIFIERS}
    assignments: dict[str, str] = {}

    largest_first = sorted(
        rows,
        key=lambda row: (-int(row["line_count"]), str(row["path"])),
    )
    for row in largest_first:
        lane = min(
            LANE_IDENTIFIERS,
            key=lambda candidate: (
                lane_lines[candidate],
                lane_files[candidate],
                candidate,
            ),
        )
        path = str(row["path"])
        assignments[path] = lane
        lane_lines[lane] += int(row["line_count"])
        lane_files[lane] += 1

    assigned_rows: list[dict[str, object]] = []
    for source_index, row in enumerate(sorted(rows, key=lambda item: str(item["path"])), 1):
        lane = assignments[str(row["path"])]
        assigned_rows.append(
            {
                "lane": f"lane-{lane}",
                **row,
                "assigned_agent": f"agent-{lane}",
                "source_id": f"GLF-{source_index:04d}",
            }
        )
    return assigned_rows


def write_corpus_manifest_now(output_path: Path, rows: list[dict[str, object]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "lane",
        "path",
        "sha256",
        "bytes",
        "line_count",
        "assigned_agent",
        "source_id",
    ]
    temporary_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
    with temporary_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=fields,
            delimiter="\t",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary_path, output_path)


def parse_manifest_arguments_now() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Freeze graph-learning files and assign three balanced reading lanes."
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="replace the frozen denominator with the current corpus",
    )
    return parser.parse_args()


def run_graph_manifest_now(refresh_existing: bool = False) -> int:
    workspace_root = Path(__file__).resolve().parents[1]
    output_path = workspace_root / EVIDENCE_SUBTREE / "all-graph-learning-files.tsv"
    if output_path.is_file() and not refresh_existing:
        print(f"preserved frozen graph-learning denominator: {output_path}")
        return 0

    rows = assign_balanced_lanes_now(collect_corpus_file_rows(workspace_root))
    write_corpus_manifest_now(output_path, rows)
    print(
        f"wrote {len(rows)} files and "
        f"{sum(int(row['line_count']) for row in rows)} lines to {output_path}"
    )
    for lane in LANE_IDENTIFIERS:
        lane_rows = [row for row in rows if row["lane"] == f"lane-{lane}"]
        print(
            f"agent-{lane}: {len(lane_rows)} files, "
            f"{sum(int(row['line_count']) for row in lane_rows)} lines"
        )
    return 0


if __name__ == "__main__":
    arguments = parse_manifest_arguments_now()
    sys.exit(run_graph_manifest_now(refresh_existing=arguments.refresh))
