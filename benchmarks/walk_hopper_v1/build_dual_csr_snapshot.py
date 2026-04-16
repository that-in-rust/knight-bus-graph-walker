#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

if __package__ in (None, ""):
    import sys

    sys.path.append(str(Path(__file__).resolve().parents[2]))

from benchmarks.walk_hopper_v1.common import (
    EDGE_TYPE_DEPENDS_ON,
    GRAPH_MODEL_CODE_SPARSE,
    NODE_ID_PREFIX,
    NODE_ID_WIDTH,
    collect_directory_size_now,
    parse_dense_node_index_now,
    read_json_file_now,
    write_json_file_now,
)


def load_numpy_module_now():
    try:
        import numpy as np
    except ImportError as exc:  # pragma: no cover - exercised only without numpy installed
        raise RuntimeError("numpy is required to build the dual CSR snapshot") from exc
    return np


def count_node_rows_now(nodes_path: Path) -> int:
    with nodes_path.open("r", encoding="utf-8", newline="") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def build_dual_csr_snapshot(
    dataset_dir: Path,
    output_dir: Path,
    edge_type: str = EDGE_TYPE_DEPENDS_ON,
) -> dict[str, object]:
    np = load_numpy_module_now()
    dataset_manifest_path = dataset_dir / "manifest.json"
    dataset_manifest = read_json_file_now(dataset_manifest_path) if dataset_manifest_path.exists() else {}
    nodes_path = dataset_dir / "nodes.csv"
    edges_path = dataset_dir / "edges.csv"
    node_count = int(dataset_manifest.get("node_count", count_node_rows_now(nodes_path)))
    seed = int(dataset_manifest.get("seed", 0))
    layer_count = int(dataset_manifest.get("layer_count", 0))
    degree_palette = list(dataset_manifest.get("degree_palette", []))

    output_dir.mkdir(parents=True, exist_ok=True)

    forward_counts = np.zeros(node_count, dtype=np.uint64)
    reverse_counts = np.zeros(node_count, dtype=np.uint64)

    with edges_path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["edge_type"] != edge_type:
                continue
            source_id = parse_dense_node_index_now(row["from_id"])
            target_id = parse_dense_node_index_now(row["to_id"])
            forward_counts[source_id] += 1
            reverse_counts[target_id] += 1

    edge_count = int(forward_counts.sum())
    forward_offsets = np.memmap(
        output_dir / "forward.offsets.bin",
        dtype=np.uint64,
        mode="w+",
        shape=(node_count + 1,),
    )
    reverse_offsets = np.memmap(
        output_dir / "reverse.offsets.bin",
        dtype=np.uint64,
        mode="w+",
        shape=(node_count + 1,),
    )
    forward_offsets[0] = 0
    reverse_offsets[0] = 0
    forward_offsets[1:] = np.cumsum(forward_counts, dtype=np.uint64)
    reverse_offsets[1:] = np.cumsum(reverse_counts, dtype=np.uint64)

    forward_peers = np.memmap(
        output_dir / "forward.peers.bin",
        dtype=np.uint32,
        mode="w+",
        shape=(edge_count,),
    )
    reverse_peers = np.memmap(
        output_dir / "reverse.peers.bin",
        dtype=np.uint32,
        mode="w+",
        shape=(edge_count,),
    )
    forward_cursor = np.array(forward_offsets[:-1], dtype=np.uint64, copy=True)
    reverse_cursor = np.array(reverse_offsets[:-1], dtype=np.uint64, copy=True)

    with edges_path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["edge_type"] != edge_type:
                continue
            source_id = parse_dense_node_index_now(row["from_id"])
            target_id = parse_dense_node_index_now(row["to_id"])
            forward_index = int(forward_cursor[source_id])
            reverse_index = int(reverse_cursor[target_id])
            forward_peers[forward_index] = target_id
            reverse_peers[reverse_index] = source_id
            forward_cursor[source_id] += 1
            reverse_cursor[target_id] += 1

    forward_offsets.flush()
    reverse_offsets.flush()
    forward_peers.flush()
    reverse_peers.flush()

    manifest = {
        "version": 1,
        "graph_model": GRAPH_MODEL_CODE_SPARSE,
        "seed": seed,
        "node_count": node_count,
        "edge_count": edge_count,
        "layer_count": layer_count,
        "degree_palette": degree_palette,
        "edge_type": edge_type,
        "node_id_prefix": NODE_ID_PREFIX,
        "node_id_width": NODE_ID_WIDTH,
        "forward_offsets": "forward.offsets.bin",
        "forward_peers": "forward.peers.bin",
        "reverse_offsets": "reverse.offsets.bin",
        "reverse_peers": "reverse.peers.bin",
        "snapshot_bytes": collect_directory_size_now(output_dir),
    }
    write_json_file_now(output_dir / "manifest.json", manifest)
    return manifest


def build_arg_parser_now() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a dual-CSR snapshot from canonical CSV files.")
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--edge-type", type=str, default=EDGE_TYPE_DEPENDS_ON)
    return parser


def main() -> None:
    args = build_arg_parser_now().parse_args()
    manifest = build_dual_csr_snapshot(
        dataset_dir=args.dataset,
        output_dir=args.output,
        edge_type=args.edge_type,
    )
    print(f"Built snapshot at {args.output}")
    print(f"node_count={manifest['node_count']} edge_count={manifest['edge_count']}")


if __name__ == "__main__":
    main()

