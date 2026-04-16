#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

if __package__ in (None, ""):
    import sys

    sys.path.append(str(Path(__file__).resolve().parents[2]))

from benchmarks.walk_hopper_v1.common import (
    DEFAULT_DEGREE_PALETTE,
    DEFAULT_LAYER_COUNT,
    GRAPH_MODEL_CODE_SPARSE,
    build_degree_summary_now,
    build_layer_ranges_now,
    choose_node_count_now,
    compute_edge_total_now,
    ensure_parent_directory_now,
    iter_node_targets_now,
    make_edge_row_now,
    make_node_row_now,
    measure_dataset_bytes_now,
    read_json_file_now,
    write_json_file_now,
)


def parse_degree_palette_now(raw_value: str) -> tuple[int, ...]:
    palette = tuple(int(part.strip()) for part in raw_value.split(",") if part.strip())
    if not palette:
        raise ValueError("degree palette must not be empty")
    if max(palette) <= 0:
        raise ValueError("degree palette values must be positive")
    return palette


def choose_target_bytes_now(args: argparse.Namespace) -> int:
    if args.target_raw_bytes is not None:
        return int(args.target_raw_bytes)
    if args.target_raw_mb is not None:
        return int(float(args.target_raw_mb) * 1024 * 1024)
    if args.target_raw_gb is not None:
        return int(float(args.target_raw_gb) * 1024 * 1024 * 1024)
    raise ValueError("one of --target-raw-bytes, --target-raw-mb, or --target-raw-gb is required")


def generate_sparse_code_graph(
    target_raw_bytes: int,
    seed: int,
    output_dir: Path,
    layer_count: int = DEFAULT_LAYER_COUNT,
    degree_palette: tuple[int, ...] = DEFAULT_DEGREE_PALETTE,
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    node_count, _, edge_count = choose_node_count_now(
        target_raw_bytes=target_raw_bytes,
        layer_count=layer_count,
        degree_palette=degree_palette,
        seed=seed,
    )
    layer_ranges = build_layer_ranges_now(node_count=node_count, layer_count=layer_count)

    nodes_path = output_dir / "nodes.csv"
    edges_path = output_dir / "edges.csv"

    with nodes_path.open("w", encoding="utf-8", newline="") as nodes_handle:
        node_writer = csv.writer(nodes_handle)
        node_writer.writerow(("node_id", "node_type", "label", "parent_id", "file_path", "span"))
        for layer_index, (layer_start, layer_end) in enumerate(layer_ranges):
            for node_index in range(layer_start, layer_end):
                node_writer.writerow(make_node_row_now(node_index=node_index, layer_index=layer_index))

    with edges_path.open("w", encoding="utf-8", newline="") as edges_handle:
        edge_writer = csv.writer(edges_handle)
        edge_writer.writerow(("from_id", "edge_type", "to_id"))
        for layer_index, (layer_start, layer_end) in enumerate(layer_ranges):
            for node_index in range(layer_start, layer_end):
                for target_index in iter_node_targets_now(
                    node_index=node_index,
                    layer_index=layer_index,
                    layer_ranges=layer_ranges,
                    degree_palette=degree_palette,
                    seed=seed,
                ):
                    edge_writer.writerow(make_edge_row_now(node_index, target_index))

    actual_raw_bytes = nodes_path.stat().st_size + edges_path.stat().st_size
    computed_total_bytes, computed_edge_count = measure_dataset_bytes_now(
        node_count=node_count,
        layer_count=layer_count,
        degree_palette=degree_palette,
        seed=seed,
    )
    manifest = {
        "graph_model": GRAPH_MODEL_CODE_SPARSE,
        "seed": seed,
        "target_raw_bytes": target_raw_bytes,
        "actual_raw_bytes": actual_raw_bytes,
        "computed_total_bytes": computed_total_bytes,
        "node_count": node_count,
        "edge_count": edge_count,
        "computed_edge_count": computed_edge_count,
        "layer_count": layer_count,
        "degree_palette": list(degree_palette),
        "degree_summary": build_degree_summary_now(
            node_count=node_count,
            edge_count=edge_count,
            degree_palette=degree_palette,
        ),
        "nodes_csv": "nodes.csv",
        "edges_csv": "edges.csv",
    }
    write_json_file_now(output_dir / "manifest.json", manifest)
    return manifest


def build_arg_parser_now() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a synthetic code-like sparse graph dataset.")
    parser.add_argument("--target-raw-gb", type=float, default=None)
    parser.add_argument("--target-raw-mb", type=float, default=None)
    parser.add_argument("--target-raw-bytes", type=int, default=None)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--layer-count", type=int, default=DEFAULT_LAYER_COUNT)
    parser.add_argument(
        "--degree-palette",
        type=str,
        default=",".join(str(value) for value in DEFAULT_DEGREE_PALETTE),
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main() -> None:
    parser = build_arg_parser_now()
    args = parser.parse_args()
    target_raw_bytes = choose_target_bytes_now(args)
    degree_palette = parse_degree_palette_now(args.degree_palette)
    manifest = generate_sparse_code_graph(
        target_raw_bytes=target_raw_bytes,
        seed=args.seed,
        output_dir=args.output,
        layer_count=args.layer_count,
        degree_palette=degree_palette,
    )
    print(f"Generated dataset at {args.output}")
    print(f"node_count={manifest['node_count']} edge_count={manifest['edge_count']}")
    print(f"actual_raw_bytes={manifest['actual_raw_bytes']}")


if __name__ == "__main__":
    main()

