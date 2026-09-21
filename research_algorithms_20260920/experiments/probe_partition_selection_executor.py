"""Execute exact finite-family selection by certificate or one source replay."""

import argparse
from collections import Counter
from fractions import Fraction as F
import hashlib
import json
from pathlib import Path

from probe_cancellation_cap_gate import bound_capped_residual_internal
from probe_partition_difference_gate import bound_original_partition_difference, compare_partition_refinement_flags
from probe_quotient_replay_control import score_streamed_candidate_family
from probe_residual_degree_envelope import build_bounded_quotient_summary
from probe_residual_native_gate import (
    aggregate_exact_quotient_edges,
    build_single_sweep_partition,
    create_summary_candidate_family,
    digest_canonical_study_value,
    evaluate_candidate_score_bounds,
    load_pinned_native_sources,
)


def select_certified_partition_family(summary, candidates, gamma, pair_cap, replay_factory, *, mode="joint", require_exact_score=False):
    """Summary and replay factory must belong to the same immutable snapshot."""
    if mode not in ("independent", "refinement", "joint") or not candidates:
        raise ValueError("supported mode and nonempty candidate family required")
    names = [name for name, _ in candidates]
    if len(set(names)) != len(names) or any(len(labels) != len(summary.original_degrees) for _, labels in candidates):
        raise ValueError("unique names and one candidate label per node required")
    mass = sum(summary.original_degrees) / 2
    intervals, known = {}, {}
    for name, labels in candidates:
        bounds = evaluate_candidate_score_bounds(summary, labels, gamma)
        known[name] = bounds["known_lower"]
        if pair_cap is None:
            intervals[name] = bounds["degree_lower"], bounds["degree_upper"]
        else:
            low, high = bound_capped_residual_internal(summary.residual_degrees, labels, pair_cap)
            intervals[name] = known[name] + low / mass, known[name] + high / mass
    preferred = max(names, key=lambda name: intervals[name][0])
    label_map = dict(candidates)
    lower, upper = intervals[preferred]
    force_score_replay = require_exact_score and lower != upper
    certified = not force_score_replay
    comparisons = core_calls = 0
    if certified:
        for name, labels in candidates:
            if name == preferred:
                continue
            comparisons += 1
            difference_upper = intervals[name][1] - lower
            if difference_upper > 0 and mode != "independent":
                refines, _ = compare_partition_refinement_flags(labels, label_map[preferred])
                if refines:
                    difference_upper = min(difference_upper, known[name] - known[preferred])
            if difference_upper > 0 and mode == "joint":
                core_calls += 1
                difference_upper = bound_original_partition_difference(summary, labels, label_map[preferred], gamma, pair_cap)["joint_upper"]
            if difference_upper > 0:
                certified = False
                break
    if certified:
        return {"winner": preferred, "labels": label_map[preferred], "mode": "certified", "score_lower": lower,
                "score_upper": upper, "comparisons": comparisons, "joint_core_calls": core_calls,
                "replay_records": 0, "replay_label_checks": 0}
    scores, metrics = score_streamed_candidate_family(len(summary.original_degrees), replay_factory(), candidates, gamma)
    best = max(scores.values())
    winner = preferred if scores[preferred] == best else next(name for name in names if scores[name] == best)
    return {"winner": winner, "labels": label_map[winner], "mode": "replayed", "score_lower": best,
            "score_upper": best, "comparisons": comparisons, "joint_core_calls": core_calls,
            "replay_records": metrics["edge_records"], "replay_label_checks": metrics["label_pair_checks"]}


def run_native_selection_study(output):
    here = Path(__file__).resolve().parent
    pins = {"community-residual-native-20260921": "53dcc226a8b176760a1ccf7ec2d10f2572976feb5e6a4a6cdf9efda93d060b64",
            "community-cancellation-native-20260921": "19c522eb9167748e8acf18b7bda3255cdde7d6d44d4b7287d6fe33c7a40dd655",
            "community-partition-difference-20260921": "b0683779648e2c72a630690e80047fcc815d76cc4b4af36c4a38401afa5870b1",
            "community-quotient-replay-20260921": "e9afffd7fa898d4d638972074b251603734b5a71fdecac703e4e74e41e6ced6c"}
    receipts = {}
    for directory, expected in pins.items():
        raw = (here.parent / "evidence" / directory / "receipt.json").read_bytes()
        if hashlib.sha256(raw).hexdigest() != expected:
            raise AssertionError("prior receipt changed")
        receipts[directory] = json.loads(raw)
        for name, digest in receipts[directory]["source_sha256"].items():
            if hashlib.sha256((here / name).read_bytes()).hexdigest() != digest:
                raise AssertionError("prior source changed")
    old = receipts["community-residual-native-20260921"]
    protocol_hash = hashlib.sha256((here.parent / "Communities-Certify-Replay-Gate.md").read_bytes()).hexdigest()
    if protocol_hash != "9c8b09382770e2bdb3c08ba75a797cbaefd9b23dfb92f7cd3df93dbc8f8b8d3f":
        raise AssertionError("execution protocol changed")
    graphs, sources, _ = load_pinned_native_sources()
    rows, counts = [], {name: Counter() for name in ("independent", "refinement", "joint", "joint_exact_score")}
    for dataset, graph in graphs:
        for gamma in (F(1, 2), F(1), F(3, 2)):
            initial, _ = build_single_sweep_partition(graph, gamma)
            base, stream, _ = aggregate_exact_quotient_edges(graph, initial)
            size = len(set(base.values()))
            partition = next(p for p in old["partitions"] if p["dataset"] == dataset and F(p["gamma"]) == gamma)
            if digest_canonical_study_value(sorted(base.items())) != partition["base_labels_sha256"]:
                raise AssertionError("base partition changed")
            for previous in old["rows"]:
                if previous["dataset"] != dataset or F(previous["gamma"]) != gamma:
                    continue
                budget = previous["budget"]
                summary = build_bounded_quotient_summary(size, iter(stream) if previous["order"] == "forward" else reversed(stream), budget)
                candidates, _ = create_summary_candidate_family(summary, gamma)
                frozen = {c["name"]: c for c in previous["candidates"] if c["operational"]}
                for name, labels in candidates:
                    if digest_canonical_study_value(labels) != frozen[name]["labels_sha256"]:
                        raise AssertionError("candidate changed")
                cap = sum(summary.residual_degrees) / (2 * (budget + 1)) if budget else None
                for policy in counts:
                    observed = Counter()
                    def iterate_counted_source_records():
                        for record in stream:
                            observed["records"] += 1
                            yield record
                    def open_counted_source_stream():
                        observed["opens"] += 1
                        return iterate_counted_source_records()
                    mode = "joint" if policy == "joint_exact_score" else policy
                    result = select_certified_partition_family(summary, candidates, gamma, cap, open_counted_source_stream,
                                                              mode=mode, require_exact_score=policy == "joint_exact_score")
                    actual = F(frozen[result["winner"]]["actual"])
                    if actual != max(F(c["actual"]) for c in frozen.values()):
                        raise AssertionError("selected partition not exact best in family")
                    if not result["score_lower"] <= actual <= result["score_upper"]:
                        raise AssertionError("selected score not in reported interval")
                    if policy == "joint_exact_score" and (result["score_lower"] != actual or result["score_upper"] != actual):
                        raise AssertionError("exact scalar output contract failed")
                    if observed["records"] != result["replay_records"] or observed["opens"] != (result["mode"] == "replayed"):
                        raise AssertionError("source access accounting failed")
                    labels_digest = digest_canonical_study_value(result.pop("labels"))
                    if labels_digest != frozen[result["winner"]]["labels_sha256"]:
                        raise AssertionError("complete selected labels changed")
                    rows.append({"dataset": dataset, "gamma": gamma, "order": previous["order"], "budget": budget,
                                 "policy": policy, "source_opens": observed["opens"], "labels_sha256": labels_digest,
                                 "actual_score": actual, **result})
                    current = counts[policy]
                    current["runs"] += 1
                    current[result["mode"]] += 1
                    current["source_opens"] += observed["opens"]
                    for key in ("replay_records", "replay_label_checks", "comparisons", "joint_core_calls"):
                        current[key] += result[key]
            print(json.dumps({"dataset": dataset, "gamma": str(gamma), "completed_runs": len(rows)}), flush=True)
    names = ("probe_partition_selection_executor.py", "test_partition_selection_executor.py")
    payload = {"scope": "Executed source iterator access and complete finite-family selection; resident fixtures, no physical benchmark",
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
    run_native_selection_study(arguments.output)
