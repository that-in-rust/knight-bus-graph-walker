#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    import sys

    sys.path.append(str(Path(__file__).resolve().parents[2]))

from benchmarks.walk_hopper_v1.common import (
    format_dense_node_key_now,
    parse_dense_node_index_now,
    percentile_value_now,
    read_json_file_now,
    write_json_file_now,
)


def load_numpy_module_now():
    try:
        import numpy as np
    except ImportError as exc:  # pragma: no cover - exercised only without numpy installed
        raise RuntimeError("numpy is required to query the WALK Hopper snapshot") from exc
    return np


@dataclass(frozen=True)
class SnapshotGraph:
    snapshot_dir: Path
    manifest: dict[str, Any]
    forward_offsets: Any
    forward_peers: Any
    reverse_offsets: Any
    reverse_peers: Any


def load_snapshot_graph_now(snapshot_dir: Path) -> SnapshotGraph:
    np = load_numpy_module_now()
    manifest = read_json_file_now(snapshot_dir / "manifest.json")
    node_count = int(manifest["node_count"])
    edge_count = int(manifest["edge_count"])
    return SnapshotGraph(
        snapshot_dir=snapshot_dir,
        manifest=manifest,
        forward_offsets=np.memmap(
            snapshot_dir / manifest["forward_offsets"], dtype=np.uint64, mode="r", shape=(node_count + 1,)
        ),
        forward_peers=np.memmap(
            snapshot_dir / manifest["forward_peers"], dtype=np.uint32, mode="r", shape=(edge_count,)
        ),
        reverse_offsets=np.memmap(
            snapshot_dir / manifest["reverse_offsets"], dtype=np.uint64, mode="r", shape=(node_count + 1,)
        ),
        reverse_peers=np.memmap(
            snapshot_dir / manifest["reverse_peers"], dtype=np.uint32, mode="r", shape=(edge_count,)
        ),
    )


def query_snapshot_ids_now(snapshot_graph: SnapshotGraph, family_name: str, node_index: int) -> list[int]:
    if family_name == "forward_one":
        start = int(snapshot_graph.forward_offsets[node_index])
        end = int(snapshot_graph.forward_offsets[node_index + 1])
        return sorted(int(peer) for peer in snapshot_graph.forward_peers[start:end])
    if family_name == "reverse_one":
        start = int(snapshot_graph.reverse_offsets[node_index])
        end = int(snapshot_graph.reverse_offsets[node_index + 1])
        return sorted(int(peer) for peer in snapshot_graph.reverse_peers[start:end])
    if family_name == "reverse_two":
        direct_parents = query_snapshot_ids_now(snapshot_graph, "reverse_one", node_index)
        seen_ids = set(direct_parents)
        for parent_id in direct_parents:
            seen_ids.update(query_snapshot_ids_now(snapshot_graph, "reverse_one", parent_id))
        seen_ids.discard(node_index)
        return sorted(seen_ids)
    raise ValueError(f"Unsupported query family: {family_name}")


def query_snapshot_family_now(
    snapshot_graph: SnapshotGraph,
    family_name: str,
    node_id: str,
) -> list[str]:
    node_index = parse_dense_node_index_now(node_id)
    return [format_dense_node_key_now(peer_id) for peer_id in query_snapshot_ids_now(snapshot_graph, family_name, node_index)]


def load_query_corpus_now(corpus_path: Path) -> list[dict[str, str]]:
    with corpus_path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def select_evenly_spaced_now(items: list[int], limit: int) -> list[int]:
    if limit <= 0 or not items:
        return []
    if limit >= len(items):
        return items
    chosen: list[int] = []
    for item_index in range(limit):
        position = round((item_index * (len(items) - 1)) / max(limit - 1, 1))
        chosen.append(items[position])
    return list(dict.fromkeys(chosen))


def build_bucket_rows_now(
    ordered_candidates: list[int],
    degree_lookup: Any,
    family_name: str,
    direction: str,
    hops: int,
    per_family: int,
) -> list[dict[str, str]]:
    if not ordered_candidates:
        return []
    if per_family <= 0:
        per_family = len(ordered_candidates)
    total = len(ordered_candidates)
    first_cut = max(total // 3, 1)
    second_cut = max((2 * total) // 3, first_cut + 1)
    buckets = {
        "low": ordered_candidates[:first_cut],
        "medium": ordered_candidates[first_cut:second_cut],
        "high": ordered_candidates[second_cut:],
    }
    if not buckets["medium"]:
        buckets["medium"] = buckets["low"][:]
    if not buckets["high"]:
        buckets["high"] = buckets["medium"][:]
    base_quota = per_family // 3
    extra = per_family % 3
    bucket_rows: list[dict[str, str]] = []
    bucket_names = ("low", "medium", "high")
    for bucket_index, bucket_name in enumerate(bucket_names):
        desired_count = base_quota + (1 if bucket_index < extra else 0)
        for node_index in select_evenly_spaced_now(buckets[bucket_name], desired_count):
            bucket_rows.append(
                {
                    "family_name": family_name,
                    "node_id": format_dense_node_key_now(node_index),
                    "hops": str(hops),
                    "direction": direction,
                    "degree_bucket": bucket_name,
                }
            )
    return bucket_rows


def build_query_corpus_now(
    snapshot_dir: Path,
    output_path: Path,
    per_family: int = 200,
) -> list[dict[str, str]]:
    np = load_numpy_module_now()
    snapshot_graph = load_snapshot_graph_now(snapshot_dir)
    forward_degree = snapshot_graph.forward_offsets[1:] - snapshot_graph.forward_offsets[:-1]
    reverse_degree = snapshot_graph.reverse_offsets[1:] - snapshot_graph.reverse_offsets[:-1]

    forward_candidates = [int(node_id) for node_id in np.nonzero(forward_degree)[0]]
    reverse_candidates = [int(node_id) for node_id in np.nonzero(reverse_degree)[0]]
    reverse_two_candidates: list[int] = []
    for node_id in reverse_candidates:
        start = int(snapshot_graph.reverse_offsets[node_id])
        end = int(snapshot_graph.reverse_offsets[node_id + 1])
        parents = snapshot_graph.reverse_peers[start:end]
        if any(int(reverse_degree[int(parent_id)]) > 0 for parent_id in parents):
            reverse_two_candidates.append(node_id)

    rows: list[dict[str, str]] = []
    rows.extend(
        build_bucket_rows_now(
            ordered_candidates=sorted(forward_candidates, key=lambda node_id: (int(forward_degree[node_id]), node_id)),
            degree_lookup=forward_degree,
            family_name="forward_one",
            direction="forward",
            hops=1,
            per_family=per_family,
        )
    )
    rows.extend(
        build_bucket_rows_now(
            ordered_candidates=sorted(reverse_candidates, key=lambda node_id: (int(reverse_degree[node_id]), node_id)),
            degree_lookup=reverse_degree,
            family_name="reverse_one",
            direction="reverse",
            hops=1,
            per_family=per_family,
        )
    )
    rows.extend(
        build_bucket_rows_now(
            ordered_candidates=sorted(
                reverse_two_candidates, key=lambda node_id: (int(reverse_degree[node_id]), node_id)
            ),
            degree_lookup=reverse_degree,
            family_name="reverse_two",
            direction="reverse",
            hops=2,
            per_family=per_family,
        )
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("family_name", "node_id", "hops", "direction", "degree_bucket"),
        )
        writer.writeheader()
        writer.writerows(rows)
    return rows


def collect_truth_answers_now(
    dataset_dir: Path,
    query_rows: list[dict[str, str]],
) -> dict[tuple[str, str], list[str]]:
    edges_path = dataset_dir / "edges.csv"
    forward_keys = {row["node_id"] for row in query_rows if row["family_name"] == "forward_one"}
    reverse_one_keys = {row["node_id"] for row in query_rows if row["family_name"] == "reverse_one"}
    reverse_two_keys = {row["node_id"] for row in query_rows if row["family_name"] == "reverse_two"}

    forward_answers: dict[str, set[str]] = {node_id: set() for node_id in forward_keys}
    reverse_direct: dict[str, set[str]] = {
        node_id: set() for node_id in reverse_one_keys | reverse_two_keys
    }

    with edges_path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            source_id = row["from_id"]
            target_id = row["to_id"]
            if source_id in forward_answers:
                forward_answers[source_id].add(target_id)
            if target_id in reverse_direct:
                reverse_direct[target_id].add(source_id)

    answers: dict[tuple[str, str], list[str]] = {}
    for node_id in sorted(forward_keys):
        answers[("forward_one", node_id)] = sorted(forward_answers[node_id])
    for node_id in sorted(reverse_one_keys):
        answers[("reverse_one", node_id)] = sorted(reverse_direct[node_id])

    reverse_two_answers: dict[str, set[str]] = {
        node_id: set(reverse_direct[node_id]) for node_id in reverse_two_keys
    }
    direct_parent_to_seeds: dict[str, set[str]] = defaultdict(set)
    for seed_id in reverse_two_keys:
        for parent_id in reverse_direct[seed_id]:
            direct_parent_to_seeds[parent_id].add(seed_id)

    if direct_parent_to_seeds:
        with edges_path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                source_id = row["from_id"]
                target_id = row["to_id"]
                if target_id not in direct_parent_to_seeds:
                    continue
                for seed_id in direct_parent_to_seeds[target_id]:
                    reverse_two_answers[seed_id].add(source_id)

    for node_id in sorted(reverse_two_keys):
        reverse_two_answers[node_id].discard(node_id)
        answers[("reverse_two", node_id)] = sorted(reverse_two_answers[node_id])
    return answers


def build_arg_parser_now() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Query a WALK Hopper snapshot.")
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--family", type=str, choices=("forward_one", "reverse_one", "reverse_two"), required=True)
    parser.add_argument("--node-id", type=str, required=True)
    parser.add_argument("--format", type=str, default="json", choices=("json", "text"))
    return parser


def main() -> None:
    args = build_arg_parser_now().parse_args()
    snapshot_graph = load_snapshot_graph_now(args.snapshot)
    answers = query_snapshot_family_now(snapshot_graph, args.family, args.node_id)
    if args.format == "json":
        print(json.dumps({"family_name": args.family, "node_id": args.node_id, "answers": answers}, indent=2))
    else:
        for answer in answers:
            print(answer)


if __name__ == "__main__":
    main()

