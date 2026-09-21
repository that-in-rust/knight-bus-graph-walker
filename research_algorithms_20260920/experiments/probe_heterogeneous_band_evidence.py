"""Fixed-grid admission and exact-output study; retain conservative refusals."""

import argparse
import hashlib
import json
from collections import Counter
from fractions import Fraction
from pathlib import Path

import probe_certified_warm_communities as uniform
import probe_heterogeneous_band_communities as candidate
import test_certified_warm_communities as fixtures
import test_certified_warm_review_contracts as reviewed
from test_heterogeneous_band_communities import perturb_mover_incident_weights


def digest_canonical_evidence_value(value):
    return hashlib.sha256(json.dumps(value, default=str, sort_keys=True).encode()).hexdigest()


def collect_heterogeneous_case_evidence(name, fixture, baseline):
    graph, p, q, initial, gamma = fixture
    oracle_moves, oracle_labels, oracle_sweeps = fixtures.run_expanded_warm_oracle(graph, initial, gamma)
    oracle_skeleton = ([(s, u, token) for s, u, token, _ in oracle_moves], oracle_labels, oracle_sweeps)
    try:
        uniform.certify_warm_graph_state(*fixture)
    except ValueError as error:
        old = str(error)
    else:
        old = "admitted"
    edges = [(u, v, str(w)) for u in sorted(graph) for v, w in sorted(graph[u].items()) if u < v]
    row = {"case": name, "vertices": len(graph), "edges": len(edges),
           "input_sha256": digest_canonical_evidence_value([edges, sorted(initial.items()), str(gamma)]),
           "uniform_certificate": old, "same_schedule_as_unperturbed": oracle_skeleton == baseline,
           "oracle_labels_sha256": digest_canonical_evidence_value(sorted(oracle_labels.items())),
           "oracle_sweeps": oracle_sweeps,
           "oracle_full_moves": [[s, u, token, str(gain)] for s, u, token, gain in oracle_moves]}
    try:
        prepared = candidate.certify_heterogeneous_band_graph(*fixture)
    except ValueError as error:
        row.update(admitted=False, refusal=str(error))
        return row
    low, high, moves = [], [], []
    result = candidate.run_heterogeneous_band_streams(prepared.header, iter(prepared.low), iter(prepared.high),
                                                      low.append, high.append, moves.append)
    labels = list(uniform.merge_certified_label_streams(iter(prepared.anchors), iter(low), iter(high)))
    if (moves != oracle_moves or labels != sorted(oracle_labels.items()) or result["sweeps"] != oracle_sweeps):
        raise AssertionError(f"unsafe admission/incorrect output: {name}")
    degrees = {u: sum(graph[u].values()) for u in graph}
    actual_dq = sum(degrees[u] for u in graph if u not in p | q and oracle_labels[u] == "Q")
    if actual_dq != result["mover_q_degree"]:
        raise AssertionError("final Q degree differs from expanded source")
    row.update(admitted=True, execution=result, output_labels_checked=len(labels),
               mover_configurations_checked=prepared.header.mover_configurations,
               minimum_mover_margin=str(prepared.header.minimum_mover_margin),
               minimum_anchor_margin=str(prepared.header.minimum_anchor_margin),
               distinct_mover_degrees=len({degrees[u] for u in graph if u not in p | q}))
    return row


def run_heterogeneous_admission_study(output):
    cases = []
    scales = (Fraction(1, 10000), Fraction(1, 1000), Fraction(1, 100),
              Fraction(1, 20), Fraction(1, 10), Fraction(3, 20))
    for b in (16, 32):
        for seed in range(10):
            base = fixtures.make_sparse_warm_fixture(b, seed)
            trace, labels, sweeps = fixtures.run_expanded_warm_oracle(base[0], base[3], base[4])
            skeleton = ([(s, u, token) for s, u, token, _ in trace], labels, sweeps)
            for scale in scales:
                fixture = perturb_mover_incident_weights(fixtures.make_sparse_warm_fixture(b, seed), seed, scale)
                cases.append(collect_heterogeneous_case_evidence(f"b{b}/seed{seed}/scale{scale}", fixture, skeleton))
    for k in range(15):
        base = reviewed.make_recentered_warm_fixture(k, 109)
        trace, labels, sweeps = fixtures.run_expanded_warm_oracle(base[0], base[3], base[4])
        skeleton = ([(s, u, token) for s, u, token, _ in trace], labels, sweeps)
        for scale in (Fraction(1, 10000), Fraction(1, 10)):
            fixture = perturb_mover_incident_weights(reviewed.make_recentered_warm_fixture(k, 109), 31 + k, scale)
            cases.append(collect_heterogeneous_case_evidence(f"prefix{k}/scale{scale}", fixture, skeleton))
    groups = {}
    for row in cases:
        scale = row["case"].split("/scale")[-1]
        counts = groups.setdefault(scale, Counter())
        counts["cases"] += 1
        counts["admitted"] += row["admitted"]
        counts["refused_same_schedule"] += not row["admitted"] and row["same_schedule_as_unperturbed"]
        counts["refused_changed_schedule"] += not row["admitted"] and not row["same_schedule_as_unperturbed"]
    here = Path(__file__).resolve().parent
    names = ("probe_heterogeneous_band_communities.py", "test_heterogeneous_band_communities.py",
             "probe_heterogeneous_band_evidence.py", "test_certified_warm_communities.py",
             "test_certified_warm_review_contracts.py", "probe_certified_warm_communities.py")
    payload = {"scope": "Constructed fixed-grid admission study; exact expanded oracle; no timings/RSS",
               "source_sha256": {name: hashlib.sha256((here / name).read_bytes()).hexdigest() for name in names},
               "cases": len(cases), "admitted": sum(row["admitted"] for row in cases),
               "uniform_admitted": sum(row["uniform_certificate"] == "admitted" for row in cases),
               "complete_admitted_labels_checked": sum(row.get("output_labels_checked", 0) for row in cases),
               "accepted_moves_checked": sum(row.get("execution", {}).get("moves", 0) for row in cases),
               "by_scale": groups, "rows": cases}
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, default=str) + "\n")
    print(json.dumps({key: value for key, value in payload.items() if key != "rows"}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    run_heterogeneous_admission_study(parser.parse_args().output)
