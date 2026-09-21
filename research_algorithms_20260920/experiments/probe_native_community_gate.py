"""Predeclared native topology gate. No timing, RSS or selected-success runs."""

import argparse
import gzip
import hashlib
import inspect
import json
from collections import Counter, deque
from fractions import Fraction as F
from pathlib import Path

from bench_bounded_seed_similarity import read_undirected_graph_source
from probe_cached_community_controls import run_cached_scalar_solver, run_general_scalar_solver
from probe_threshold_event_solver import run_threshold_event_solver
from test_single_flip_trace_verifier import compute_direct_partition_objective


def digest_canonical_native_value(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def prepare_karate_reference_fixture(path):
    if path.exists():
        return json.loads(path.read_text())
    import networkx as nx
    if nx.__version__ != "3.6.1":
        raise ValueError("fixture generation requires networkx3.6.1")
    graph = nx.karate_club_graph()
    payload = {"source_url": "https://networkx.org/documentation/stable/reference/generated/networkx.generators.social.karate_club_graph.html",
               "networkx_version": nx.__version__,
               "source_module_sha256": hashlib.sha256(Path(inspect.getsourcefile(nx.generators.social)).read_bytes()).hexdigest(),
               "edges": sorted((min(u, v), max(u, v), data["weight"]) for u, v, data in graph.edges(data=True)),
               "clubs": sorted((u, data["club"]) for u, data in graph.nodes(data=True))}
    if len(payload["edges"]) != 78 or len(payload["clubs"]) != 34:
        raise AssertionError("unexpected karate source size")
    with path.open("x") as stream:
        json.dump(payload, stream, indent=2)
        stream.write("\n")
    return payload


def construct_native_initial_partition(graph, mode):
    vertices = sorted(graph)
    scanned = 0
    if mode == "id_half":
        ordered = vertices
    elif mode == "bfs_half":
        ordered, seen = [], set()
        for root in vertices:
            if root in seen:
                continue
            queue = deque([root])
            seen.add(root)
            while queue:
                u = queue.popleft()
                ordered.append(u)
                for v in sorted(graph[u]):
                    scanned += 1
                    if v not in seen:
                        seen.add(v)
                        queue.append(v)
    else:
        raise ValueError("unknown initial partition mode")
    side = set(ordered[:len(ordered) // 2])
    return {u: "P" if u in side else "Q" for u in vertices}, {"ordered_vertices": len(ordered),
                                                              "neighbor_records_scanned": scanned}


def compare_complete_native_case(name, graph, initial, gamma):
    budget = 20 * len(graph)
    scalar = run_cached_scalar_solver(graph, initial, gamma, max_moves=budget)
    tree = run_threshold_event_solver(graph, initial, gamma, max_moves=budget)
    fields = lambda result: (result.status, result.trace, result.labels, result.sweeps, result.next_visit)
    if fields(scalar) != fields(tree):
        raise AssertionError(f"two-label mismatch:{name}")
    cold = run_general_scalar_solver(graph, initial, gamma, max_moves=budget)
    suffix = None
    if tree.status == "complete":
        if fields(tree) != fields(cold):
            raise AssertionError(f"complete mismatch:{name}")
        combined_trace, final_labels, final_status, final_sweeps = tree.trace, tree.labels, tree.status, tree.sweeps
    else:
        suffix = run_general_scalar_solver(graph, tree.labels, gamma, max_moves=budget - len(tree.trace),
                                           start_visit=tree.next_visit)
        combined_trace = tree.trace + suffix.trace
        final_labels, final_status, final_sweeps = suffix.labels, suffix.status, suffix.sweeps
        if (combined_trace, final_labels, final_status, final_sweeps, suffix.next_visit) != (
                cold.trace, cold.labels, cold.status, cold.sweeps, cold.next_visit):
            raise AssertionError(f"resumed mismatch:{name}")
    before = compute_direct_partition_objective(graph, initial, gamma)
    after = compute_direct_partition_objective(graph, final_labels, gamma)
    total = sum(sum(row.values()) for row in graph.values())
    if after - before != 2 * sum((gain for _, _, _, gain in combined_trace), F(0)) / total ** 2:
        raise AssertionError(f"objective telescoping mismatch:{name}")
    return {"case": name, "gamma": str(gamma), "event_budget": budget,
            "initial_labels_sha256": digest_canonical_native_value(sorted(initial.items())),
            "two_label_status": tree.status, "two_label_moves": len(tree.trace),
            "two_label_sweeps": tree.sweeps, "pending_visit": tree.next_visit,
            "two_label_trace_sha256": digest_canonical_native_value(tree.trace),
            "two_label_labels_sha256": digest_canonical_native_value(sorted(tree.labels.items())),
            "tree_metrics": tree.metrics, "cached_scalar_metrics": scalar.metrics,
            "fallback_metrics": suffix.metrics if suffix is not None else None,
            "general_cold_metrics": cold.metrics, "complete_status": final_status,
            "complete_moves": len(combined_trace), "complete_sweeps": final_sweeps,
            "complete_communities": len(set(final_labels.values())),
            "final_labels_sha256": digest_canonical_native_value(sorted(final_labels.items())),
            "complete_trace_sha256": digest_canonical_native_value(combined_trace),
            "initial_modularity": str(before), "final_modularity": str(after)}


def run_native_community_study(output):
    here = Path(__file__).resolve().parent
    karate_path = here / "data/karate-club-networkx-3.6.1.json"
    karate = prepare_karate_reference_fixture(karate_path)
    sources = [
        ("grqc", "ca-GrQc.txt.gz", "a254442cdf5d684712578b630c2e0d7543518ab154ef2341cabb607572ce7230"),
        ("facebook", "facebook_combined.txt.gz", "125e84db872eeba443d270c70315c256b0af43a502fcfe51f50621166ad035d7")]
    graphs, source_rows = [], []
    for name, filename, expected in sources:
        path = here / "data" / filename
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != expected:
            raise AssertionError(f"retained source changed:{name}")
        with gzip.open(path, "rt", encoding="ascii") as stream:
            adjacency, information = read_undirected_graph_source(stream)
        graph = {u: {v: 1 for v in sorted(row)} for u, row in sorted(adjacency.items())}
        source_rows.append({"dataset": name, "source_sha256": digest, "source_file": filename,
                            "normalization": information})
        graphs.append((name, graph, None))
    for mode in ("weighted", "unit"):
        graph = {u: {} for u, _ in karate["clubs"]}
        for u, v, weight in karate["edges"]:
            graph[u][v] = graph[v][u] = weight if mode == "weighted" else 1
        labels = {u: "P" if club == "Mr. Hi" else "Q" for u, club in karate["clubs"]}
        name = "karate_" + mode
        source_rows.append({"dataset": name, "source_sha256": hashlib.sha256(karate_path.read_bytes()).hexdigest(),
                            "source_file": karate_path.name, "projection": mode,
                            "normalization": {"nodes": len(graph), "edges": sum(map(len, graph.values())) // 2}})
        graphs.append((name, graph, labels))
    rows, counts = [], Counter()
    for name, graph, supplied in graphs:
        modes = ("club_split",) if supplied else ("id_half", "bfs_half")
        for mode in modes:
            labels, preparation = (supplied, {"supplied_label_records": len(supplied)}) if supplied else construct_native_initial_partition(graph, mode)
            for gamma in (F(1, 2), F(1), F(3, 2)):
                case = f"{name}/{mode}/gamma{gamma}"
                row = compare_complete_native_case(case, graph, labels, gamma)
                row.update(dataset=name, initial_mode=mode, initial_preparation=preparation)
                rows.append(row)
                counts["cases"] += 1
                counts["two_label_" + row["two_label_status"]] += 1
                counts["workflow_" + row["complete_status"]] += 1
                print(json.dumps({key: row[key] for key in ("case", "two_label_status", "two_label_moves", "complete_status", "complete_moves", "complete_communities")}), flush=True)
    names = ("probe_native_community_gate.py", "probe_cached_community_controls.py", "test_cached_community_controls.py",
             "probe_threshold_event_solver.py", "probe_threshold_trace_control.py", "probe_single_flip_trace_verifier.py",
             "bench_bounded_seed_similarity.py", "test_single_flip_trace_verifier.py")
    payload = {"scope": "Native topology,declared constructed or observed two-way starts;logical work only,no timings/RSS",
               "source_sha256": {name: hashlib.sha256((here / name).read_bytes()).hexdigest() for name in names},
               "sources": source_rows, "counts": counts, "rows": rows}
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as destination:
        json.dump(payload, destination, indent=2)
        destination.write("\n")
    print(json.dumps({"counts": counts, "receipt": str(output)}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    run_native_community_study(parser.parse_args().output)
