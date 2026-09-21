"""Separate elementary signed controls from sketch-specific information."""

import argparse
from collections import Counter
from fractions import Fraction as F
import hashlib
import json
from pathlib import Path

from probe_partition_difference_gate import bound_joint_residual_difference
from probe_residual_degree_envelope import build_bounded_quotient_summary
from probe_residual_native_gate import (
    aggregate_exact_quotient_edges, build_single_sweep_partition,
    create_summary_candidate_family, digest_canonical_study_value,
    evaluate_candidate_score_bounds, load_pinned_native_sources,
)


def bound_signed_residual_controls(residual_degrees, labels_a, labels_b, cap):
    degrees, cap = tuple(map(F, residual_degrees)), F(cap)
    row = bound_joint_residual_difference(degrees, labels_a, labels_b, cap)
    by_a, by_b, cells = {}, {}, {}
    count_a, count_b, count_cells = Counter(labels_a), Counter(labels_b), Counter(zip(labels_a, labels_b))
    clipped = tuple(min(r, cap) for r in degrees)
    for t, a, b in zip(clipped, labels_a, labels_b):
        by_a[a] = by_a.get(a, F()) + t
        by_b[b] = by_b.get(b, F()) + t
        cells[a, b] = cells.get((a, b), F()) + t
    total = sum(clipped, F())
    positive, negative, forced_positive, forced_negative = [], [], [], []
    support_positive, support_negative = [], []
    for r, t, a, b in zip(degrees, clipped, labels_a, labels_b):
        p, n = by_a[a] - cells[a, b], by_b[b] - cells[a, b]
        z = total - t - p - n
        if r > p + n + z:
            raise ValueError("degree exceeds all capped neighbor capacity")
        positive.append(min(r, p))
        negative.append(min(r, n))
        forced_negative.append(max(F(), r - p - z))
        forced_positive.append(max(F(), r - n - z))
        if count_a[a] > count_cells[a, b]:
            support_positive.append(r)
        if count_b[b] > count_cells[a, b]:
            support_negative.append(r)
    pos, neg = sum(positive, F()), sum(negative, F())
    upper = min(pos / 2, pos - max(positive, default=F())) - max(sum(forced_negative, F()) / 2, max(forced_negative, default=F()))
    reverse = min(neg / 2, neg - max(negative, default=F())) - max(sum(forced_positive, F()) / 2, max(forced_positive, default=F()))
    sp, sn = sum(support_positive, F()), sum(support_negative, F())
    support = (-min(sn / 2, sn - max(support_negative, default=F())),
               min(sp / 2, sp - max(support_positive, default=F())))
    return {"support": support, "sign": (-neg / 2, pos / 2), "row": row, "endpoint": (-reverse, upper)}


def run_signed_control_study(output):
    here = Path(__file__).resolve().parent
    pins = {"community-residual-native-20260921": "53dcc226a8b176760a1ccf7ec2d10f2572976feb5e6a4a6cdf9efda93d060b64",
            "community-partition-difference-20260921": "b0683779648e2c72a630690e80047fcc815d76cc4b4af36c4a38401afa5870b1"}
    receipts = {}
    for directory, expected in pins.items():
        raw = (here.parent / "evidence" / directory / "receipt.json").read_bytes()
        if hashlib.sha256(raw).hexdigest() != expected:
            raise AssertionError("prior receipt changed")
        receipts[directory] = json.loads(raw)
        for name, digest in receipts[directory]["source_sha256"].items():
            if hashlib.sha256((here / name).read_bytes()).hexdigest() != digest:
                raise AssertionError("prior source changed")
    old, joint = (receipts[name] for name in pins)
    protocol_hash = hashlib.sha256((here.parent / "Communities-Signed-Control-Gate.md").read_bytes()).hexdigest()
    if protocol_hash != "c7301b3eda0a3f118023ee8b20f35785d19481b69c7ce6ba4dbce48a131a50bb":
        raise AssertionError("ablation protocol changed")
    tiers = ("support", "sign_generic", "row_generic", "endpoint_generic", "sign_capped", "row_capped", "endpoint_capped")
    counts, rows = {tier: Counter() for tier in tiers}, []
    graphs, sources, _ = load_pinned_native_sources()
    for dataset, graph in graphs:
        for gamma in (F(1, 2), F(1), F(3, 2)):
            initial, _ = build_single_sweep_partition(graph, gamma)
            base, stream, _ = aggregate_exact_quotient_edges(graph, initial)
            size = len(set(base.values()))
            partition = next(p for p in old["partitions"] if p["dataset"] == dataset and F(p["gamma"]) == gamma)
            if digest_canonical_study_value(sorted(base.items())) != partition["base_labels_sha256"]:
                raise AssertionError("base partition changed")
            full = build_bounded_quotient_summary(size, iter(stream), partition["coarse_pairs"])
            oracle, _ = create_summary_candidate_family(full, gamma)
            oracle = [("exact_" + name, labels) for name, labels in oracle if name != "baseline"]
            for previous, frozen in zip(old["rows"], joint["rows"]):
                if previous["dataset"] != dataset or F(previous["gamma"]) != gamma:
                    continue
                keys = ("dataset", "gamma", "order", "budget")
                if any(previous[key] != frozen[key] for key in keys):
                    raise AssertionError("row ordering changed")
                budget = previous["budget"]
                summary = build_bounded_quotient_summary(size, iter(stream) if previous["order"] == "forward" else reversed(stream), budget)
                operational, _ = create_summary_candidate_family(summary, gamma)
                candidates = operational + oracle
                for (name, labels), cached in zip(candidates, previous["candidates"]):
                    if name != cached["name"] or digest_canonical_study_value(labels) != cached["labels_sha256"]:
                        raise AssertionError("candidate changed")
                mass, residual_mass = sum(summary.original_degrees) / 2, sum(summary.residual_degrees) / 2
                cap = residual_mass / (budget + 1) if budget else residual_mass
                labels_by_name = dict(candidates)
                known = {name: evaluate_candidate_score_bounds(summary, labels, gamma)["known_lower"] for name, labels in candidates}
                pairs, regrets = [], {tier: F() for tier in tiers}
                for pair in frozen["pairs"]:
                    a, b = pair["a"], pair["b"]
                    generic = bound_signed_residual_controls(summary.residual_degrees, labels_by_name[a], labels_by_name[b], residual_mass)
                    capped = bound_signed_residual_controls(summary.residual_degrees, labels_by_name[a], labels_by_name[b], cap)
                    residual = {"support": generic["support"],
                                **{name + "_generic": generic[name] for name in ("sign", "row", "endpoint")},
                                **{name + "_capped": capped[name] for name in ("sign", "row", "endpoint")}}
                    offset, actual = known[a] - known[b], F(pair["actual"])
                    control = F(pair["control_lower"]), F(pair["control_upper"])
                    intervals = {}
                    for tier, (low, high) in residual.items():
                        interval = max(control[0], offset + low / mass), min(control[1], offset + high / mass)
                        if not interval[0] <= actual <= interval[1]:
                            raise AssertionError("signed ablation excludes actual difference")
                        intervals[tier] = interval
                        improved = interval[0] > control[0] or interval[1] < control[1]
                        counts[tier]["pairs"] += 1
                        counts[tier]["improved_pairs"] += improved
                        counts[tier]["operational_improvements"] += improved and pair["operational"]
                        counts[tier]["crossing_improvements"] += improved and pair["crossing"]
                        if pair["operational"] and b == previous["winner"]:
                            regrets[tier] = max(regrets[tier], interval[1])
                    if intervals["row_capped"] != (F(pair["joint_lower"]), F(pair["joint_upper"])):
                        raise AssertionError("frozen row certificate not reproduced")
                    for suffix in ("generic", "capped"):
                        lo, hi = intervals["row_" + suffix]
                        elo, ehi = intervals["endpoint_" + suffix]
                        if not lo <= elo <= ehi <= hi:
                            raise AssertionError("endpoint control lost dominance")
                    pairs.append({"a": a, "b": b, "actual": actual, "operational": pair["operational"],
                                  "crossing": pair["crossing"], "intervals": intervals})
                for tier, regret in regrets.items():
                    counts[tier]["cases"] += 1
                    counts[tier]["proves_best"] += regret == 0
                rows.append({**{key: previous[key] for key in keys}, "regrets": regrets, "pairs": pairs})
            print(json.dumps({"dataset": dataset, "gamma": str(gamma), "rows": len(rows)}), flush=True)
    names = ("probe_signed_control_ablation.py", "test_signed_control_ablation.py")
    payload = {"scope": "Signed comparison attribution; common marginal control unchanged; not execution or performance",
               "protocol_sha256": protocol_hash, "prior_receipt_sha256": pins,
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
    run_signed_control_study(parser.parse_args().output)
