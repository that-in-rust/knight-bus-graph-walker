"""Joint capped residual comparisons. Logical certificates, not performance."""

import argparse
from collections import Counter
from fractions import Fraction as F
import hashlib
import json
from pathlib import Path

from probe_cancellation_cap_gate import bound_capped_residual_internal
from probe_residual_degree_envelope import build_bounded_quotient_summary
from probe_residual_native_gate import (
    aggregate_exact_quotient_edges,
    build_single_sweep_partition,
    create_summary_candidate_family,
    digest_canonical_study_value,
    evaluate_candidate_score_bounds,
    load_pinned_native_sources,
)


def compare_partition_refinement_flags(labels_a, labels_b):
    if len(labels_a) != len(labels_b):
        raise ValueError("partitions require the same domain")
    targets_a, targets_b = {}, {}
    a_refines, b_refines = True, True
    for a, b in zip(labels_a, labels_b):
        if a in targets_a and targets_a[a] != b:
            a_refines = False
        if b in targets_b and targets_b[b] != a:
            b_refines = False
        targets_a[a], targets_b[b] = b, a
    return a_refines, b_refines


def bound_joint_residual_difference(residual_degrees, labels_a, labels_b, cap):
    """Caller certifies feasible loopless degrees and a residual pair cap."""
    degrees, cap = tuple(F(value) for value in residual_degrees), F(cap)
    size, mass = len(degrees), sum(degrees, F()) / 2
    if len(labels_a) != size or len(labels_b) != size or cap < 0 or any(r < 0 for r in degrees):
        raise ValueError("nonnegative degrees/cap and two full partitions required")
    if max(degrees, default=F()) > min(mass, max(0, size - 1) * cap):
        raise ValueError("necessary degree or pair-cap condition failed")
    by_a, by_b, cells, total = {}, {}, {}, F()
    for degree, a, b in zip(degrees, labels_a, labels_b):
        clipped = min(degree, cap)
        by_a[a] = by_a.get(a, F()) + clipped
        by_b[b] = by_b.get(b, F()) + clipped
        cells[a, b] = cells.get((a, b), F()) + clipped
        total += clipped
    upper, reverse_upper = F(), F()
    for degree, a, b in zip(degrees, labels_a, labels_b):
        positive = by_a[a] - cells[a, b]
        negative = by_b[b] - cells[a, b]
        neutral = total - min(degree, cap) - positive - negative
        upper += min(degree, positive) - max(F(), degree - positive - neutral)
        reverse_upper += min(degree, negative) - max(F(), degree - negative - neutral)
    lower, upper = -reverse_upper / 2, upper / 2
    if lower > upper:
        raise ValueError("inconsistent signed residual metadata")
    return lower, upper


def bound_original_partition_difference(summary, labels_a, labels_b, gamma, pair_cap):
    """None means no sketch cap; generic M is valid for the joint bound."""
    bounds_a = evaluate_candidate_score_bounds(summary, labels_a, gamma)
    bounds_b = evaluate_candidate_score_bounds(summary, labels_b, gamma)
    mass, residual_mass = sum(summary.original_degrees) / 2, sum(summary.residual_degrees) / 2
    known = bounds_a["known_lower"] - bounds_b["known_lower"]
    if pair_cap is None:
        lower_a, upper_a = bounds_a["degree_lower"], bounds_a["degree_upper"]
        lower_b, upper_b = bounds_b["degree_lower"], bounds_b["degree_upper"]
        cap = residual_mass
    else:
        cap = F(pair_cap)
        lo_a, hi_a = bound_capped_residual_internal(summary.residual_degrees, labels_a, cap)
        lo_b, hi_b = bound_capped_residual_internal(summary.residual_degrees, labels_b, cap)
        lower_a, upper_a = bounds_a["known_lower"] + lo_a / mass, bounds_a["known_lower"] + hi_a / mass
        lower_b, upper_b = bounds_b["known_lower"] + lo_b / mass, bounds_b["known_lower"] + hi_b / mass
    independent_lower, independent_upper = lower_a - upper_b, upper_a - lower_b
    lower, upper = independent_lower, independent_upper
    a_refines, b_refines = compare_partition_refinement_flags(labels_a, labels_b)
    if a_refines:
        upper = min(upper, known)
    if b_refines:
        lower = max(lower, known)
    joint_lo, joint_hi = bound_joint_residual_difference(summary.residual_degrees, labels_a, labels_b, cap)
    joint_lower, joint_upper = max(lower, known + joint_lo / mass), min(upper, known + joint_hi / mass)
    if joint_lower > joint_upper:
        raise ValueError("inconsistent original-score difference interval")
    return {"independent_lower": independent_lower, "independent_upper": independent_upper,
            "control_lower": lower, "control_upper": upper, "joint_lower": joint_lower,
            "joint_upper": joint_upper, "a_refines_b": a_refines, "b_refines_a": b_refines,
            "intersection_records": len(set(zip(labels_a, labels_b)))}


def run_partition_difference_study(output):
    here = Path(__file__).resolve().parent
    pins = {"community-residual-native-20260921": "53dcc226a8b176760a1ccf7ec2d10f2572976feb5e6a4a6cdf9efda93d060b64",
            "community-cancellation-native-20260921": "19c522eb9167748e8acf18b7bda3255cdde7d6d44d4b7287d6fe33c7a40dd655"}
    receipts = {}
    for directory, expected in pins.items():
        path = here.parent / "evidence" / directory / "receipt.json"
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != expected:
            raise AssertionError("prior receipt changed")
        receipts[directory] = json.loads(raw)
        for name, digest in receipts[directory]["source_sha256"].items():
            if hashlib.sha256((here / name).read_bytes()).hexdigest() != digest:
                raise AssertionError("prior source changed")
    old, capped = (receipts[name] for name in pins)
    protocol = here.parent / "Communities-Partition-Difference-Gate.md"
    protocol_hash = hashlib.sha256(protocol.read_bytes()).hexdigest()
    if protocol_hash != "cb5c73cc46db5962b1f6e0accda1f9a2a079fb30f477c61f7fb0dc41d11aa2ed":
        raise AssertionError("joint protocol changed")
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
            for previous, cap_previous in zip(old["rows"], capped["rows"]):
                if previous["dataset"] != dataset or F(previous["gamma"]) != gamma:
                    continue
                key = (dataset, str(gamma), previous["order"], previous["budget"])
                if key != (cap_previous["dataset"], cap_previous["gamma"], cap_previous["order"], cap_previous["budget"]):
                    raise AssertionError("prior rows not aligned")
                budget = previous["budget"]
                summary = build_bounded_quotient_summary(size, iter(stream) if previous["order"] == "forward" else reversed(stream), budget)
                operational, _ = create_summary_candidate_family(summary, gamma)
                all_candidates = operational + oracle
                if len(all_candidates) != len(previous["candidates"]) or len(all_candidates) != len(cap_previous["candidates"]):
                    raise AssertionError("candidate count changed")
                actuals, frozen_caps = {}, {}
                for (name, labels), frozen, cap_frozen in zip(all_candidates, previous["candidates"], cap_previous["candidates"]):
                    if name != frozen["name"] or name != cap_frozen["name"] or digest_canonical_study_value(labels) != frozen["labels_sha256"]:
                        raise AssertionError("candidate changed")
                    actuals[name] = F(frozen["actual"])
                    frozen_caps[name] = cap_frozen
                residual_mass = sum(summary.residual_degrees) / 2
                pair_cap = residual_mass / (budget + 1) if budget else None
                names = {name for name, _ in operational}
                pairs = []
                for name_a, labels_a in all_candidates:
                    for name_b, labels_b in all_candidates:
                        if name_a == name_b:
                            continue
                        bounds = bound_original_partition_difference(summary, labels_a, labels_b, gamma, pair_cap)
                        actual = actuals[name_a] - actuals[name_b]
                        if not bounds["control_lower"] <= bounds["joint_lower"] <= actual <= bounds["joint_upper"] <= bounds["control_upper"]:
                            raise AssertionError("joint difference containment failed")
                        expected_lo = F(frozen_caps[name_a]["lower"]) - F(frozen_caps[name_b]["upper"])
                        expected_hi = F(frozen_caps[name_a]["upper"]) - F(frozen_caps[name_b]["lower"])
                        if (bounds["independent_lower"], bounds["independent_upper"]) != (expected_lo, expected_hi):
                            raise AssertionError("independent cap comparator changed")
                        is_operational = name_a in names and name_b in names
                        crossing = not bounds["a_refines_b"] and not bounds["b_refines_a"]
                        improved = bounds["joint_lower"] > bounds["control_lower"] or bounds["joint_upper"] < bounds["control_upper"]
                        pairs.append({"a": name_a, "b": name_b, "actual": actual, "operational": is_operational, "crossing": crossing, **bounds})
                        counts["pairs"] += 1
                        counts["pair_intervals_improved"] += improved
                        if is_operational:
                            counts["operational_pairs"] += 1
                            counts["operational_pair_improvements"] += improved
                        if crossing:
                            counts["crossing_pairs"] += 1
                            counts["crossing_pair_improvements"] += improved
                            counts["operational_crossing_improvements"] += improved and is_operational
                selected_pairs = [p for p in pairs if p["operational"] and p["b"] == previous["winner"]]
                regrets = {kind: max([F()] + [p[kind + "_upper"] for p in selected_pairs]) for kind in ("independent", "control", "joint")}
                actual_regret = max(actuals[name] for name in names) - actuals[previous["winner"]]
                if not actual_regret <= regrets["joint"] <= regrets["control"] <= regrets["independent"]:
                    raise AssertionError("finite-family regret certificate failed")
                rows.append({"dataset": dataset, "gamma": gamma, "order": previous["order"], "budget": budget,
                             "coarse_nodes": size, "coarse_pairs": partition["coarse_pairs"], "winner": previous["winner"],
                             "actual_regret": actual_regret, "regrets": regrets, "pairs": pairs})
                counts["rows"] += 1
                for kind in regrets:
                    counts[kind + "_proves_best"] += regrets[kind] == 0
                counts["joint_regret_improvements"] += regrets["joint"] < regrets["control"]
            print(json.dumps({"dataset": dataset, "gamma": str(gamma), "completed_rows": len(rows)}), flush=True)
    names = ("probe_partition_difference_gate.py", "test_partition_difference_gate.py")
    payload = {"scope": "Frozen native candidate differences; no new candidate production or physical benchmark",
               "prior_receipt_sha256": pins, "protocol_sha256": protocol_hash,
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
    run_partition_difference_study(arguments.output)
