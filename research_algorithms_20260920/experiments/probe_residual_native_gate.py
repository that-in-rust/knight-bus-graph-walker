"""Frozen native eligibility study. Resident oracle, no memory/timing claims."""

import argparse
from collections import Counter
from fractions import Fraction as F
import gzip
import hashlib
import heapq
import json
from pathlib import Path

from bench_bounded_seed_similarity import read_undirected_graph_source
from probe_residual_degree_envelope import (
    build_bounded_quotient_summary,
    score_bounded_quotient_partition,
)


def run_sparse_move_sweep(degrees, retained, total_mass, gamma):
    """One ordinary move sweep; original degrees can exceed retained degrees."""
    size = len(degrees)
    if total_mass <= 0 or gamma <= 0 or any(value < 0 for value in degrees):
        raise ValueError("positive mass/resolution and nonnegative degrees required")
    adjacency = [{} for _ in range(size)]
    for (left, right), weight in retained.items():
        if not (0 <= left < right < size) or weight <= 0:
            raise ValueError("canonical positive non-loop pairs required")
        adjacency[left][right] = adjacency[right][left] = weight
    labels = list(range(size))
    volumes = dict(enumerate(degrees))
    counts = {node: 1 for node in range(size)}
    zero = {node for node in range(size) if not degrees[node]}
    zero_heap = list(zero)
    heapq.heapify(zero_heap)
    metrics = {"visits": size, "adjacency_entries": 2 * len(retained), "candidate_scores": 0, "moves": 0}
    for node in range(size):
        source, degree = labels[node], degrees[node]
        affinity = {}
        for other, weight in adjacency[node].items():
            label = labels[other]
            affinity[label] = affinity.get(label, F()) + weight
        while zero_heap and zero_heap[0] not in zero:
            heapq.heappop(zero_heap)
        choices = set(affinity) | {source, size + node}
        if zero_heap:
            choices.add(zero_heap[0])
        scores = {label: 2 * total_mass * affinity.get(label, 0) - gamma * degree *
                  (volumes.get(label, 0) - (degree if label == source else 0)) for label in choices}
        metrics["candidate_scores"] += len(scores)
        best = min(scores, key=lambda label: (-scores[label], label))
        if scores[best] <= scores[source]:
            continue
        volumes[source] -= degree
        counts[source] -= 1
        if not counts[source]:
            del counts[source]
            del volumes[source]
            zero.discard(source)
        elif not volumes[source]:
            zero.add(source)
            heapq.heappush(zero_heap, source)
        counts[best] = counts.get(best, 0) + 1
        volumes[best] = volumes.get(best, 0) + degree
        zero.discard(best)
        labels[node] = best
        metrics["moves"] += 1
    return tuple(labels), metrics


def build_single_sweep_partition(graph, gamma):
    vertices = sorted(graph)
    index = {node: i for i, node in enumerate(vertices)}
    degrees = [F(sum(graph[node].values())) for node in vertices]
    pairs = {(index[u], index[v]): F(w) for u in vertices for v, w in graph[u].items() if u < v and w}
    labels, metrics = run_sparse_move_sweep(degrees, pairs, sum(degrees) / 2, gamma)
    return dict(zip(vertices, labels)), metrics


def aggregate_exact_quotient_edges(graph, labels):
    occupied = {label: i for i, label in enumerate(sorted(set(labels.values())))}
    base = {node: occupied[labels[node]] for node in sorted(graph)}
    stream, exact = [], {}
    for left in sorted(graph):
        for right, weight in sorted(graph[left].items()):
            if left >= right:
                continue
            u, v = sorted((base[left], base[right]))
            stream.append((u, v, F(weight)))
            exact[u, v] = exact.get((u, v), F()) + weight
    return base, stream, exact


def create_summary_candidate_family(summary, gamma):
    degrees, retained = summary.original_degrees, summary.retained
    size, mass = len(degrees), sum(degrees) / 2
    if mass <= 0 or gamma <= 0:
        raise ValueError("positive mass and resolution required")
    ranked = []
    for (left, right), weight in retained.items():
        gain = 2 * mass * weight - gamma * degrees[left] * degrees[right]
        if gain > 0:
            ranked.append((-gain, left, right))
    labels, used = list(range(size)), set()
    for _, left, right in sorted(ranked):
        if left not in used and right not in used:
            labels[right] = left
            used.update((left, right))
    sweep, sweep_metrics = run_sparse_move_sweep(degrees, retained, mass, gamma)
    candidates = [("baseline", tuple(range(size))), ("matching", tuple(labels)), ("sweep", sweep)]
    return candidates, {"matching_candidates": len(ranked), "matching_merges": len(used) // 2, "sweep": sweep_metrics}


def evaluate_candidate_score_bounds(summary, labels, gamma):
    degrees, residual = summary.original_degrees, summary.residual_degrees
    if len(labels) != len(degrees):
        raise ValueError("one label per coarse node required")
    mass, remaining = sum(degrees) / 2, sum(residual) / 2
    lower, upper = score_bounded_quotient_partition(summary, labels, gamma)
    groups, volumes = {}, {}
    for label, degree, rest in zip(labels, degrees, residual):
        subtotal, largest = groups.get(label, (F(), F()))
        groups[label] = subtotal + rest, max(largest, rest)
        volumes[label] = volumes.get(label, F()) + degree
    known = summary.loop_mass + sum(w for (u, v), w in summary.retained.items() if labels[u] == labels[v])
    penalty = gamma * sum((value / (2 * mass)) ** 2 for value in volumes.values())
    known_lower = known / mass - penalty
    mass_low, mass_high = F(), remaining
    if len(groups) == len(labels):
        mass_high = F()
    if len(groups) == 1:
        mass_low = mass_high = remaining
    group_high = sum(min(total / 2, total - largest) for total, largest in groups.values())
    return {"known_lower": known_lower, "mass_lower": known_lower + mass_low / mass,
            "mass_upper": known_lower + mass_high / mass, "group_lower": lower,
            "group_upper": known_lower + group_high / mass,
            "degree_lower": lower, "degree_upper": upper}


def select_certified_candidate_winner(summary, candidates, gamma, bound):
    if bound not in ("known_lower", "degree_lower"):
        raise ValueError("unsupported selection bound")
    scores = [evaluate_candidate_score_bounds(summary, labels, gamma)[bound] for _, labels in candidates]
    return candidates[max(range(len(scores)), key=lambda i: scores[i])][0]


def evaluate_exact_quotient_score(exact, labels, gamma):
    mass, internal, degrees = F(), F(), [F() for _ in labels]
    for (left, right), weight in exact.items():
        mass += weight
        degrees[left] += weight
        degrees[right] += weight
        if labels[left] == labels[right]:
            internal += weight
    volumes = {}
    for label, degree in zip(labels, degrees):
        volumes[label] = volumes.get(label, F()) + degree
    return internal / mass - gamma * sum((value / (2 * mass)) ** 2 for value in volumes.values())


def digest_canonical_study_value(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def load_pinned_native_sources():
    here = Path(__file__).resolve().parent
    old = here.parent / "evidence/community-native-gate-20260921/receipt.json"
    manifest = json.loads(old.read_text())["sources"]
    graphs = []
    for source in manifest:
        path = here / "data" / source["source_file"]
        if hashlib.sha256(path.read_bytes()).hexdigest() != source["source_sha256"]:
            raise AssertionError("native source hash mismatch")
        if path.suffix == ".gz":
            with gzip.open(path, "rt", encoding="ascii") as stream:
                adjacency, normalization = read_undirected_graph_source(stream)
            if normalization != source["normalization"]:
                raise AssertionError("native normalization changed")
            graph = {u: {v: 1 for v in sorted(row)} for u, row in sorted(adjacency.items())}
        else:
            fixture = json.loads(path.read_text())
            graph = {u: {} for u, _ in fixture["clubs"]}
            for u, v, weight in fixture["edges"]:
                graph[u][v] = graph[v][u] = weight if source["projection"] == "weighted" else 1
        graphs.append((source["dataset"], graph))
    return graphs, manifest, old


def measure_residual_candidate_rows(summary, candidates, exact, gamma, operational):
    result = []
    for name, labels in candidates:
        bounds = evaluate_candidate_score_bounds(summary, labels, gamma)
        actual = evaluate_exact_quotient_score(exact, labels, gamma)
        if not bounds["degree_lower"] <= actual <= bounds["degree_upper"]:
            raise AssertionError("original score outside residual envelope")
        if not (bounds["degree_upper"] <= bounds["group_upper"] <= bounds["mass_upper"] and
                bounds["degree_lower"] >= bounds["mass_lower"] >= bounds["known_lower"]):
            raise AssertionError("control containment failed")
        result.append({"name": name, "operational": name in operational, "labels_sha256": digest_canonical_study_value(labels),
                       "communities": len(set(labels)), "actual": actual, **bounds,
                       "width": bounds["degree_upper"] - bounds["degree_lower"]})
    return result


def run_residual_native_study(output):
    here = Path(__file__).resolve().parent
    graphs, sources, old_receipt = load_pinned_native_sources()
    rows, partitions, counts = [], [], Counter()
    for dataset, graph in graphs:
        for gamma in (F(1, 2), F(1), F(3, 2)):
            labels, preparation = build_single_sweep_partition(graph, gamma)
            base, stream, exact = aggregate_exact_quotient_edges(graph, labels)
            size = len(set(base.values()))
            q = sum(u != v for u, v in exact)
            full = build_bounded_quotient_summary(size, iter(stream), q)
            oracle, oracle_metrics = create_summary_candidate_family(full, gamma)
            oracle = [("exact_" + name, part) for name, part in oracle if name != "baseline"]
            base_score = evaluate_exact_quotient_score(exact, tuple(range(size)), gamma)
            partition = {"dataset": dataset, "gamma": gamma, "vertices": len(graph), "edges": len(stream),
                         "coarse_nodes": size, "coarse_pairs": q, "coarse_loops": sum(u == v for u, v in exact),
                         "base_labels_sha256": digest_canonical_study_value(sorted(base.items())),
                         "quotient_sha256": digest_canonical_study_value(sorted(exact.items())),
                         "base_score": base_score, "preparation": preparation, "exact_candidate_work": oracle_metrics}
            partitions.append(partition)
            budgets = sorted({0, (q + 99) // 100, (q + 19) // 20, (q + 4) // 5, (q + 1) // 2, q})
            for order in ("forward", "reverse"):
                for budget in budgets:
                    summary = build_bounded_quotient_summary(size, iter(stream) if order == "forward" else reversed(stream), budget)
                    candidates, candidate_work = create_summary_candidate_family(summary, gamma)
                    winner = select_certified_candidate_winner(summary, candidates, gamma, "degree_lower")
                    control = select_certified_candidate_winner(summary, candidates, gamma, "known_lower")
                    candidate_rows = measure_residual_candidate_rows(summary, candidates + oracle, exact, gamma,
                                                                     {name for name, _ in candidates})
                    chosen = next(row for row in candidate_rows if row["name"] == winner)
                    mass = sum(summary.original_degrees) / 2
                    row = {"dataset": dataset, "gamma": gamma, "order": order, "budget": budget,
                           "coarse_nodes": size, "coarse_pairs": q, "peak_counters": summary.peak_counters,
                           "final_counters": len(summary.retained), "retained_mass": sum(summary.retained.values(), F()),
                           "residual_mass": sum(summary.residual_degrees) / 2, "original_mass": mass,
                           "winner": winner, "known_only_winner": control,
                           "actual_gain": chosen["actual"] - base_score,
                           "certified_gain": chosen["degree_lower"] - base_score,
                           "selected_width": chosen["width"],
                           "finite_operational_regret": max(r["degree_upper"] for r in candidate_rows if r["operational"]) - chosen["degree_lower"],
                           "observed_five_candidate_gap": max(r["actual"] for r in candidate_rows) - chosen["actual"],
                           "candidate_work": candidate_work, "candidates": candidate_rows}
                    if row["certified_gain"] < 0 or row["actual_gain"] < row["certified_gain"]:
                        raise AssertionError("baseline protection failed")
                    if budget == q and any(r["width"] for r in candidate_rows):
                        raise AssertionError("full-budget quotient must be exact")
                    rows.append(row)
                    counts["rows"] += 1
                    counts["positive_certified_gain"] += row["certified_gain"] > 0
                    counts["winner_changed_by_degree"] += winner != control
                    counts["candidate_scores"] += len(candidate_rows)
                    counts["lower_improved_candidates"] += sum(r["degree_lower"] > r["known_lower"] for r in candidate_rows)
                    counts["coupled_upper_improved_candidates"] += sum(r["degree_upper"] < r["group_upper"] for r in candidate_rows)
            print(json.dumps({"dataset": dataset, "gamma": str(gamma), "coarse_nodes": size, "coarse_pairs": q,
                              "base_score": str(base_score), "completed_rows": len(rows)}), flush=True)
    names = ("probe_residual_native_gate.py", "test_residual_native_gate.py", "probe_residual_degree_envelope.py",
             "test_residual_degree_envelope.py", "bench_bounded_seed_similarity.py")
    payload = {"scope": "Native semantic eligibility; resident graph/oracle, no timings/RSS/end-to-end cap",
               "protocol_pre_run_sha256": "30156b553433cf72ebe29023f56ab2a3c937b37fb5b707570ee9d777a85831fb",
               "prior_native_receipt_sha256": hashlib.sha256(old_receipt.read_bytes()).hexdigest(),
               "source_sha256": {name: hashlib.sha256((here / name).read_bytes()).hexdigest() for name in names},
               "sources": sources, "partitions": partitions, "counts": counts, "rows": rows}
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as destination:
        json.dump(payload, destination, indent=2, default=str)
        destination.write("\n")
    print(json.dumps({"counts": counts, "receipt": str(output)}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    arguments = parser.parse_args()
    run_residual_native_study(arguments.output)
