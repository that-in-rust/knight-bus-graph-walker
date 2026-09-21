"""Count a standard batched-tree control on frozen exact trajectories.

This is a logical repair model, not an implemented faster community solver.
The source solver is replayed only to recover and verify its original trace.
"""

import argparse
import gzip
import hashlib
import json
from fractions import Fraction as F
from pathlib import Path

import probe_native_community_gate as native
from probe_threshold_event_solver import run_threshold_event_solver


def count_batched_repair_nodes(n, ranks):
    if type(n) is not int or n <= 0:
        raise ValueError("positive integer vertex count required")
    if any(type(rank) is not int or not 0 <= rank < n for rank in ranks):
        raise ValueError("invalid affected rank")
    if len(set(ranks)) != len(ranks):
        raise ValueError("affected ranks must be distinct")
    size = 1 << (n - 1).bit_length()
    frontier = {size + rank for rank in ranks}
    widths = []
    while frontier:
        widths.append(len(frontier))
        frontier = {node // 2 for node in frontier if node > 1}
    return {"point_node_writes": len(ranks) * size.bit_length(),
            "batched_node_writes": sum(widths), "level_widths": widths,
            "max_level_entries": max(widths, default=0)}


def load_frozen_native_sources(receipt, here):
    graphs = {}
    for source in receipt["sources"]:
        path = here / "data" / source["source_file"]
        if hashlib.sha256(path.read_bytes()).hexdigest() != source["source_sha256"]:
            raise AssertionError("native source changed")
        if source["source_file"].endswith(".gz"):
            with gzip.open(path, "rt", encoding="ascii") as stream:
                adjacency, counts = native.read_undirected_graph_source(stream)
            if counts != source["normalization"]:
                raise AssertionError("normalization changed")
            graph = {u: {v: 1 for v in sorted(row)} for u, row in sorted(adjacency.items())}
            labels = None
        else:
            fixture = json.loads(path.read_text())
            graph = {u: {} for u, _ in fixture["clubs"]}
            for u, v, weight in fixture["edges"]:
                graph[u][v] = graph[v][u] = weight if source["projection"] == "weighted" else 1
            labels = {u: "P" if club == "Mr. Hi" else "Q" for u, club in fixture["clubs"]}
        graphs[source["dataset"]] = graph, labels
    return graphs


def run_native_repair_accounting(output):
    here = Path(__file__).resolve().parent
    receipt_path = here.parent / "evidence/community-native-gate-20260921/receipt.json"
    receipt = json.loads(receipt_path.read_text())
    for name, expected in receipt["source_sha256"].items():
        if hashlib.sha256((here / name).read_bytes()).hexdigest() != expected:
            raise AssertionError(f"frozen source changed: {name}")
    graphs = load_frozen_native_sources(receipt, here)
    rows = []
    for old in receipt["rows"]:
        graph, supplied = graphs[old["dataset"]]
        labels = supplied if supplied is not None else native.construct_native_initial_partition(graph, old["initial_mode"])[0]
        result = run_threshold_event_solver(graph, labels, F(old["gamma"]), max_moves=old["event_budget"])
        trace_hash = native.digest_canonical_native_value(result.trace)
        labels_hash = native.digest_canonical_native_value(sorted(result.labels.items()))
        if (trace_hash, labels_hash, result.status, result.sweeps) != (
                old["two_label_trace_sha256"], old["two_label_labels_sha256"],
                old["two_label_status"], old["two_label_sweeps"]):
            raise AssertionError(f"frozen trajectory changed: {old['case']}")
        if result.metrics != old["tree_metrics"]:
            raise AssertionError("frozen metric mismatch")
        ranks = {u: i for i, u in enumerate(sorted(graph))}
        point, batched, peak = 0, 0, 0
        for _, u, _, _ in result.trace:
            affected = [ranks[u]] + [ranks[v] for v in graph[u]]
            counts = count_batched_repair_nodes(len(graph), affected)
            point += counts["point_node_writes"]
            batched += counts["batched_node_writes"]
            peak = max(peak, counts["max_level_entries"])
        if point != result.metrics["threshold_update_nodes"]:
            raise AssertionError("point-path accounting does not reconstruct frozen receipt")
        size = 1 << (len(graph) - 1).bit_length()
        row = {"case": old["case"], "status": result.status, "moves": len(result.trace),
               "trace_sha256": trace_hash, "point_node_writes": point,
               "batched_node_writes": batched, "peak_level_entries": peak,
               "tree_build_nodes": 2 * size - 1,
               "search_nodes": result.metrics["search_tree_nodes"],
               "cached_scalar_visits": old["cached_scalar_metrics"]["scalar_visits"],
               "neighbor_affinity_updates": result.metrics["neighbor_affinity_updates"]}
        rows.append(row)
        print(json.dumps(row), flush=True)
    payload = {"scope": "Standard batched ancestor-union work model, not a new solver or RAM/time measurement",
               "parent_receipt_sha256": hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
               "source_sha256": {name: hashlib.sha256((here / name).read_bytes()).hexdigest()
                                 for name in (Path(__file__).name, "test_threshold_repair_accounting.py")},
               "rows": rows}
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as destination:
        json.dump(payload, destination, indent=2)
        destination.write("\n")
    print(f"Retained {len(rows)} cases at {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    run_native_repair_accounting(parser.parse_args().output)
