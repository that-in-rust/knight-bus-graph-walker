"""Primary pair-cap certificate on the frozen native candidate family."""

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


def bound_capped_residual_internal(residual_degrees, labels, cap):
    """Caller certifies a loopless residual with these degrees and pair cap.

    Necessary metadata checks are not a complete capped-feasibility solver.
    """
    degrees, cap = tuple(F(value) for value in residual_degrees), F(cap)
    size, mass = len(degrees), sum(degrees, F()) / 2
    if len(labels) != size or cap < 0 or any(value < 0 for value in degrees):
        raise ValueError("nonnegative degrees/cap and one label per node required")
    if max(degrees, default=F()) > min(mass, max(0, size - 1) * cap):
        raise ValueError("necessary degree or pair-cap condition failed")
    groups, total_clipped = {}, F()
    for degree, label in zip(degrees, labels):
        clipped = min(degree, cap)
        state = groups.setdefault(label, [F(), F(), F(), F()])
        state[0] += degree
        state[1] += clipped
        total_clipped += clipped
    for degree, label in zip(degrees, labels):
        state = groups[label]
        state[2] += max(F(), degree - state[1] + min(degree, cap))
        state[3] += min(degree, total_clipped - state[1])
    alphas = [state[2] for state in groups.values()]
    bounds = [min(2 * mass - state[0], state[3]) for state in groups.values()]
    alpha_sum, bound_sum = sum(alphas, F()), sum(bounds, F())
    crossing_min = max(max(alphas, default=F()), alpha_sum / 2)
    crossing_max = min(mass, bound_sum / 2, bound_sum - max(bounds, default=F()))
    lower, upper = mass - crossing_max, mass - crossing_min
    if not F() <= lower <= upper <= mass:
        raise ValueError("inconsistent capped residual metadata")
    return lower, upper


def run_cancellation_native_study(output):
    here = Path(__file__).resolve().parent
    old_path = here.parent / "evidence/community-residual-native-20260921/receipt.json"
    old_bytes = old_path.read_bytes()
    old_hash = hashlib.sha256(old_bytes).hexdigest()
    if old_hash != "53dcc226a8b176760a1ccf7ec2d10f2572976feb5e6a4a6cdf9efda93d060b64":
        raise AssertionError("native receipt changed")
    old = json.loads(old_bytes)
    for name, expected in old["source_sha256"].items():
        if hashlib.sha256((here / name).read_bytes()).hexdigest() != expected:
            raise AssertionError("native source changed")
    doc_hashes = {"Communities-Cancellation-Native-Gate.md": "45704ebdd45b785b4aeec8b21e646d60810077b538ecb5bd200e924b8f9024c6",
                  "Communities-Cancellation-Cap-Review.md": "838ab697f829c117313ea9c6bf7562dbaceb27f8e36a945d8a0a12ffb2aa0646"}
    for name, expected in doc_hashes.items():
        if hashlib.sha256((here.parent / name).read_bytes()).hexdigest() != expected:
            raise AssertionError("cap protocol/review changed")
    graphs, sources, _ = load_pinned_native_sources()
    rows, counts = [], Counter()
    for dataset, graph in graphs:
        for gamma in (F(1, 2), F(1), F(3, 2)):
            initial, _ = build_single_sweep_partition(graph, gamma)
            base, stream, exact = aggregate_exact_quotient_edges(graph, initial)
            size = len(set(base.values()))
            partition = next(p for p in old["partitions"] if p["dataset"] == dataset and F(p["gamma"]) == gamma)
            if digest_canonical_study_value(sorted(base.items())) != partition["base_labels_sha256"]:
                raise AssertionError("base partition changed")
            full = build_bounded_quotient_summary(size, iter(stream), partition["coarse_pairs"])
            oracle, _ = create_summary_candidate_family(full, gamma)
            oracle = [("exact_" + name, labels) for name, labels in oracle if name != "baseline"]
            for previous in old["rows"]:
                if previous["dataset"] != dataset or F(previous["gamma"]) != gamma:
                    continue
                budget = previous["budget"]
                summary = build_bounded_quotient_summary(size, iter(stream) if previous["order"] == "forward" else reversed(stream), budget)
                operational, _ = create_summary_candidate_family(summary, gamma)
                all_candidates = operational + oracle
                mass, residual_mass = sum(summary.original_degrees) / 2, sum(summary.residual_degrees) / 2
                cap = residual_mass / (budget + 1) if budget else None
                candidate_rows = []
                for (name, labels), frozen in zip(all_candidates, previous["candidates"]):
                    if name != frozen["name"] or digest_canonical_study_value(labels) != frozen["labels_sha256"]:
                        raise AssertionError("candidate changed")
                    old_lower, old_upper = F(frozen["degree_lower"]), F(frozen["degree_upper"])
                    if cap is None:
                        lower, upper = old_lower, old_upper
                    else:
                        lo, hi = bound_capped_residual_internal(summary.residual_degrees, labels, cap)
                        lower, upper = F(frozen["known_lower"]) + lo / mass, F(frozen["known_lower"]) + hi / mass
                    actual = F(frozen["actual"])
                    if not old_lower <= lower <= actual <= upper <= old_upper:
                        raise AssertionError("capped score containment/refinement failed")
                    candidate_rows.append({"name": name, "operational": frozen["operational"], "actual": actual,
                                           "lower": lower, "upper": upper, "width": upper - lower,
                                           "previous_width": old_upper - old_lower,
                                           "lower_improvement": lower - old_lower, "upper_improvement": old_upper - upper,
                                           "labels_sha256": frozen["labels_sha256"]})
                operational_rows = [r for r in candidate_rows if r["operational"]]
                chosen = max(operational_rows, key=lambda row: row["lower"])
                old_chosen = next(row for row in operational_rows if row["name"] == previous["winner"])
                base_score = F(partition["base_score"])
                row = {"dataset": dataset, "gamma": gamma, "order": previous["order"], "budget": budget,
                       "coarse_nodes": size, "coarse_pairs": partition["coarse_pairs"], "cap": cap,
                       "winner": chosen["name"], "previous_winner": previous["winner"],
                       "actual_quality_change": chosen["actual"] - old_chosen["actual"],
                       "certified_gain": chosen["lower"] - base_score,
                       "previous_certified_gain": F(previous["certified_gain"]),
                       "selected_width": chosen["width"],
                       "finite_operational_regret": max(r["upper"] for r in operational_rows) - chosen["lower"],
                       "previous_finite_regret": F(previous["finite_operational_regret"]),
                       "candidates": candidate_rows}
                rows.append(row)
                counts["rows"] += 1
                counts["candidate_scores"] += len(candidate_rows)
                counts["winner_changes"] += row["winner"] != row["previous_winner"]
                counts["actual_quality_improvements"] += row["actual_quality_change"] > 0
                counts["actual_quality_losses"] += row["actual_quality_change"] < 0
                counts["regret_certificate_improvements"] += row["finite_operational_regret"] < row["previous_finite_regret"]
                for candidate in candidate_rows:
                    counts["lower_improved_candidates"] += candidate["lower_improvement"] > 0
                    counts["upper_improved_candidates"] += candidate["upper_improvement"] > 0
                    if candidate["operational"]:
                        counts["operational_lower_improved"] += candidate["lower_improvement"] > 0
                        counts["operational_upper_improved"] += candidate["upper_improvement"] > 0
            print(json.dumps({"dataset": dataset, "gamma": str(gamma), "completed_rows": len(rows)}), flush=True)
    names = ("probe_cancellation_cap_gate.py", "test_cancellation_cap_gate.py")
    payload = {"scope": "Fixed native candidates with stronger certificate only; no physical benchmark",
               "frozen_native_receipt_sha256": old_hash, "document_sha256": doc_hashes,
               "source_sha256": {name: hashlib.sha256((here / name).read_bytes()).hexdigest() for name in names},
               "sources": sources, "counts": counts, "rows": rows}
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
    run_cancellation_native_study(arguments.output)
