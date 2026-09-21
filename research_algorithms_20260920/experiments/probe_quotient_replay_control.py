"""Exact one-scan rescoring of fixed candidate partitions, a standard control."""

import argparse
from collections import Counter
from fractions import Fraction as F
import hashlib
import json
from pathlib import Path

from probe_residual_degree_envelope import build_bounded_quotient_summary
from probe_residual_native_gate import (
    aggregate_exact_quotient_edges,
    build_single_sweep_partition,
    create_summary_candidate_family,
    digest_canonical_study_value,
    load_pinned_native_sources,
)


def score_streamed_candidate_family(node_count, edge_stream, candidates, gamma):
    if type(node_count) is not int or node_count < 0 or gamma <= 0 or not candidates:
        raise ValueError("nonnegative node count, candidates and positive resolution required")
    names = [name for name, _ in candidates]
    if len(set(names)) != len(names) or any(len(labels) != node_count for _, labels in candidates):
        raise ValueError("unique candidate names and full label arrays required")
    degrees, internal, mass = [F() for _ in range(node_count)], [F() for _ in candidates], F()
    records = 0
    for left, right, raw_weight in edge_stream:
        if type(left) is not int or type(right) is not int or not (0 <= left < node_count and 0 <= right < node_count):
            raise ValueError("edge endpoint outside declared graph")
        weight = F(raw_weight)
        if weight < 0:
            raise ValueError("nonnegative weights required")
        degrees[left] += weight
        degrees[right] += weight
        mass += weight
        records += 1
        for i, (_, labels) in enumerate(candidates):
            if labels[left] == labels[right]:
                internal[i] += weight
    if not mass:
        raise ValueError("edgeless scoring convention not declared")
    scores, peak_groups = {}, 0
    for i, (name, labels) in enumerate(candidates):
        volumes = {}
        for label, degree in zip(labels, degrees):
            volumes[label] = volumes.get(label, F()) + degree
        peak_groups = max(peak_groups, len(volumes))
        scores[name] = internal[i] / mass - F(gamma) * sum((value / (2 * mass)) ** 2 for value in volumes.values())
    return scores, {"edge_records": records, "label_pair_checks": records * len(candidates),
                    "retained_edge_records": 0, "degree_records": node_count,
                    "supplied_label_records": node_count * len(candidates), "peak_volume_records": peak_groups}


def run_native_replay_control(output):
    here = Path(__file__).resolve().parent
    old_path = here.parent / "evidence/community-residual-native-20260921/receipt.json"
    old_bytes = old_path.read_bytes()
    old_hash = hashlib.sha256(old_bytes).hexdigest()
    if old_hash != "53dcc226a8b176760a1ccf7ec2d10f2572976feb5e6a4a6cdf9efda93d060b64":
        raise AssertionError("frozen native receipt changed")
    old = json.loads(old_bytes)
    for name, expected in old["source_sha256"].items():
        if hashlib.sha256((here / name).read_bytes()).hexdigest() != expected:
            raise AssertionError("frozen native source changed")
    graphs, sources, _ = load_pinned_native_sources()
    rows, counts = [], Counter()
    for dataset, graph in graphs:
        for gamma in (F(1, 2), F(1), F(3, 2)):
            initial, _ = build_single_sweep_partition(graph, gamma)
            base, stream, exact = aggregate_exact_quotient_edges(graph, initial)
            size = len(set(base.values()))
            original = next(p for p in old["partitions"] if p["dataset"] == dataset and F(p["gamma"]) == gamma)
            if digest_canonical_study_value(sorted(base.items())) != original["base_labels_sha256"]:
                raise AssertionError("initial partition changed")
            q = original["coarse_pairs"]
            full = build_bounded_quotient_summary(size, iter(stream), q)
            oracle, _ = create_summary_candidate_family(full, gamma)
            oracle = [("exact_" + name, labels) for name, labels in oracle if name != "baseline"]
            for previous in old["rows"]:
                if previous["dataset"] != dataset or F(previous["gamma"]) != gamma:
                    continue
                sketch = build_bounded_quotient_summary(size, iter(stream) if previous["order"] == "forward" else reversed(stream), previous["budget"])
                candidates, _ = create_summary_candidate_family(sketch, gamma)
                all_candidates = candidates + oracle
                for (name, labels), frozen in zip(all_candidates, previous["candidates"]):
                    if name != frozen["name"] or digest_canonical_study_value(labels) != frozen["labels_sha256"]:
                        raise AssertionError("candidate family changed")
                scores, metrics = score_streamed_candidate_family(size, iter(stream), all_candidates, gamma)
                for frozen in previous["candidates"]:
                    if scores[frozen["name"]] != F(frozen["actual"]):
                        raise AssertionError("streamed score differs from quotient oracle")
                winner = max((name for name, _ in candidates), key=scores.get)
                previous_winner = previous["winner"]
                improvement = scores[winner] - scores[previous_winner]
                rows.append({"dataset": dataset, "gamma": str(gamma), "order": previous["order"], "budget": previous["budget"],
                             "previous_winner": previous_winner, "replay_winner": winner,
                             "actual_score_improvement": str(improvement), "exact_scores": {k: str(v) for k, v in scores.items()},
                             "metrics": metrics})
                counts["rows"] += 1
                counts["exact_scores_checked"] += len(scores)
                counts["edge_records"] += metrics["edge_records"]
                counts["label_pair_checks"] += metrics["label_pair_checks"]
                counts["winner_name_changes"] += winner != previous_winner
                counts["strict_quality_improvements"] += improvement > 0
            print(json.dumps({"dataset": dataset, "gamma": str(gamma), "completed_rows": len(rows)}), flush=True)
    names = ("probe_quotient_replay_control.py", "test_quotient_replay_control.py")
    payload = {"scope": "Exact fixed-candidate score replay; resident fixture/label generator; no physical I/O benchmark",
               "frozen_native_receipt_sha256": old_hash,
               "source_sha256": {name: hashlib.sha256((here / name).read_bytes()).hexdigest() for name in names},
               "sources": sources, "counts": counts, "rows": rows}
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as destination:
        json.dump(payload, destination, indent=2)
        destination.write("\n")
    print(json.dumps({"counts": counts, "receipt": str(output)}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    arguments = parser.parse_args()
    run_native_replay_control(arguments.output)
