"""Retain complete-output logical evidence, not timing or RSS measurements."""

import argparse
import hashlib
import json
from fractions import Fraction
from pathlib import Path

from probe_certified_warm_communities import (
    certify_warm_graph_state, merge_certified_label_streams, run_certified_warm_streams,
)
from test_certified_warm_communities import (
    OnceOnlyRecordIterator, add_undirected_weight_edge, make_even_crossing_fixture,
    make_sparse_warm_fixture, run_expanded_warm_oracle,
)


def hash_canonical_json_value(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def collect_certified_fixture_evidence(name, fixture):
    graph, p, q, initial, gamma = fixture
    prepared = certify_warm_graph_state(*fixture)
    low, high, moves = [], [], []
    lo, hi = OnceOnlyRecordIterator(prepared.low), OnceOnlyRecordIterator(prepared.high)
    result = run_certified_warm_streams(prepared.header, lo, hi, low.append, high.append, moves.append)
    labels = list(merge_certified_label_streams(iter(prepared.anchors), iter(low), iter(high)))
    expected_moves, expected_labels, expected_sweeps = run_expanded_warm_oracle(graph, initial, gamma)
    if (moves != expected_moves or labels != sorted(expected_labels.items())
            or result["sweeps"] != expected_sweeps or lo.read + hi.read != prepared.header.b):
        raise AssertionError(f"complete trace/output mismatch: {name}")
    edge_rows = [(u, v, str(w)) for u in sorted(graph)
                 for v, w in sorted(graph[u].items()) if u < v]
    degrees = {u: sum(row.values()) for u, row in graph.items()}
    header = prepared.header
    return {"fixture": name, "vertices": len(graph), "edges": len(edge_rows),
            "input_sha256": hash_canonical_json_value([edge_rows, sorted(initial.items()), str(gamma)]),
            "anchor_degree_values_p": len({degrees[u] for u in p}),
            "anchor_degree_values_q": len({degrees[u] for u in q}),
            "cross_anchor_edges": sum(1 for u in p for v in graph[u] if v in q),
            "output_labels_checked": len(labels), "output_sha256": hash_canonical_json_value(labels),
            "full_move_trace": [[sweep, u, token, str(gain)] for sweep, u, token, gain in moves],
            "minimum_anchor_margin": str(header.min_anchor_margin),
            "minimum_fresh_margin": str(header.min_fresh_margin),
            "minimum_band_margin": str(min(header.a - abs(header.f0),
                                            2 * header.h - abs(header.f0) - header.a)),
            "execution": result}


def run_certified_evidence_study(output):
    rows = []
    for b in (16, 24, 32, 64):
        for seed in range(12):
            for variant in ("sparse", "irregular", "cross-anchor"):
                fixture = make_sparse_warm_fixture(b, seed, perturb=variant != "sparse")
                if variant == "cross-anchor":
                    graph, p, q = fixture[:3]
                    add_undirected_weight_edge(graph, min(p), max(q), Fraction(seed + 1, 100))
                rows.append(collect_certified_fixture_evidence(f"{variant}/b{b}/seed{seed}", fixture))
    for seed in range(4):
        rows.append(collect_certified_fixture_evidence(
            f"dense/b16/seed{seed}", make_sparse_warm_fixture(seed=seed, dense=True)))
    for crossing in (False, True):
        rows.append(collect_certified_fixture_evidence(
            f"symmetric/even-crossing-{crossing}", make_even_crossing_fixture(crossing)))
    here = Path(__file__).resolve().parent
    payload = {
        "scope": "Constructed rational graphs; supplied warm states; full exact trace/output; no timing/RSS",
        "source_sha256": {name: hashlib.sha256((here / name).read_bytes()).hexdigest() for name in (
            "probe_certified_warm_communities.py", "test_certified_warm_communities.py",
            "probe_certified_warm_evidence.py")},
        "cases": len(rows), "complete_labels_checked": sum(row["vertices"] for row in rows),
        "accepted_moves_checked": sum(row["execution"]["moves"] for row in rows),
        "cases_with_irregular_anchors": sum(row["anchor_degree_values_p"] > 1 or
                                            row["anchor_degree_values_q"] > 1 for row in rows),
        "cases_with_cross_anchor_edges": sum(row["cross_anchor_edges"] > 0 for row in rows),
        "rows": rows,
    }
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n")
    print(json.dumps({key: value for key, value in payload.items() if key != "rows"}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    run_certified_evidence_study(parser.parse_args().output)
