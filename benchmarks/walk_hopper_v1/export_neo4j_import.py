#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

if __package__ in (None, ""):
    import sys

    sys.path.append(str(Path(__file__).resolve().parents[2]))

from benchmarks.walk_hopper_v1.common import EDGE_TYPE_DEPENDS_ON, sha256_file_hex_now, write_json_file_now


def export_neo4j_import_files(dataset_dir: Path, output_dir: Path) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    nodes_path = dataset_dir / "nodes.csv"
    edges_path = dataset_dir / "edges.csv"
    neo4j_nodes_header_path = output_dir / "nodes.header.csv"
    neo4j_nodes_data_path = output_dir / "nodes.data.csv"
    neo4j_edges_header_path = output_dir / "relationships.header.csv"
    neo4j_edges_data_path = output_dir / "relationships.data.csv"

    with neo4j_nodes_header_path.open("w", encoding="utf-8", newline="") as handle:
        csv.writer(handle).writerow(
            ("node_id:ID", ":LABEL", "node_type", "label", "parent_id", "file_path", "span")
        )
    with neo4j_edges_header_path.open("w", encoding="utf-8", newline="") as handle:
        csv.writer(handle).writerow((":START_ID", ":END_ID", ":TYPE", "edge_type"))

    node_count = 0
    with nodes_path.open("r", encoding="utf-8", newline="") as source_handle, neo4j_nodes_data_path.open(
        "w", encoding="utf-8", newline=""
    ) as target_handle:
        reader = csv.DictReader(source_handle)
        writer = csv.writer(target_handle)
        for row in reader:
            node_count += 1
            writer.writerow(
                (
                    row["node_id"],
                    "Entity",
                    row["node_type"],
                    row["label"],
                    row["parent_id"],
                    row["file_path"],
                    row["span"],
                )
            )

    edge_count = 0
    with edges_path.open("r", encoding="utf-8", newline="") as source_handle, neo4j_edges_data_path.open(
        "w", encoding="utf-8", newline=""
    ) as target_handle:
        reader = csv.DictReader(source_handle)
        writer = csv.writer(target_handle)
        for row in reader:
            if row["edge_type"] != EDGE_TYPE_DEPENDS_ON:
                continue
            edge_count += 1
            writer.writerow((row["from_id"], row["to_id"], "DEPENDS_ON", row["edge_type"]))

    export_manifest = {
        "node_count": node_count,
        "edge_count": edge_count,
        "nodes_header_file": neo4j_nodes_header_path.name,
        "nodes_data_file": neo4j_nodes_data_path.name,
        "relationships_header_file": neo4j_edges_header_path.name,
        "relationships_data_file": neo4j_edges_data_path.name,
        "nodes_header_sha256": sha256_file_hex_now(neo4j_nodes_header_path),
        "nodes_data_sha256": sha256_file_hex_now(neo4j_nodes_data_path),
        "relationships_header_sha256": sha256_file_hex_now(neo4j_edges_header_path),
        "relationships_data_sha256": sha256_file_hex_now(neo4j_edges_data_path),
    }
    write_json_file_now(output_dir / "neo4j_export_manifest.json", export_manifest)
    return export_manifest


def build_arg_parser_now() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export canonical CSV files as Neo4j import-ready CSV.")
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main() -> None:
    args = build_arg_parser_now().parse_args()
    manifest = export_neo4j_import_files(args.dataset, args.output)
    print(f"Exported Neo4j import files at {args.output}")
    print(f"nodes={manifest['node_count']} relationships={manifest['edge_count']}")


if __name__ == "__main__":
    main()
