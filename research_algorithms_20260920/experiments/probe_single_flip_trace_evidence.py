"""Replay the frozen 150 sources; proposals are generated without an oracle."""

import argparse
import hashlib
import json
from collections import Counter
from fractions import Fraction as F
from pathlib import Path

import probe_single_flip_trace_verifier as offline
import probe_threshold_trace_control as chronological
import test_certified_warm_communities as fixtures
import test_certified_warm_review_contracts as reviewed
from test_heterogeneous_band_communities import perturb_mover_incident_weights


def digest_canonical_evidence_value(value):
    return hashlib.sha256(json.dumps(value, default=str, sort_keys=True).encode()).hexdigest()


def generate_matching_trace_proposal(graph, p, q, initial):
    y = sorted(set(graph) - set(p) - set(q))
    k = sum(initial[u] == "Q" for u in y)
    if any(initial[u] != ("Q" if i < k else "P") for i, u in enumerate(y)):
        raise ValueError("producer requires an initial Q prefix")
    ranks = {u: i for i, u in enumerate(y)}
    partners = []
    scanned = 0
    for u in y:
        neighbors = [v for v in graph[u] if v in ranks]
        scanned += len(graph[u])
        if len(neighbors) != 1:
            raise ValueError("producer requires a mover matching")
        partners.append(ranks[neighbors[0]])
    if any(partners[v] != u or v == u for u, v in enumerate(partners)):
        raise ValueError("producer requires a reciprocal loopless matching")
    crossing = sum(partners[i] >= k for i in range(k))
    cursors, ends, proposal, consumed = [0, k], (k, len(y)), [], 0
    while len(proposal) < crossing:
        j = len(proposal)
        side = 1 if (j // 2) % 2 == 0 else 0
        rank = cursors[side]
        if rank >= ends[side]:
            raise AssertionError("matching frontier exhausted")
        mate = partners[rank]
        cursors[side] += 1
        consumed += 1
        if (rank < k) != (mate < k) and mate >= cursors[1 - side]:
            proposal.append((1 + (j + 2) // 4, y[rank], "Q" if side else "P"))
    terminal = 2 + (crossing + 1) // 4 if crossing else 1
    return proposal, terminal, {"mover_records": len(y), "mover_records_consumed": consumed,
                                "mover_adjacency_records_scanned": scanned,
                                "crossing_pairs": crossing, "proposed_moves": len(proposal)}


def reconstruct_retained_source_fixture(case):
    prefix, scale_text = case.split("/scale")
    scale = F(scale_text)
    if prefix.startswith("prefix"):
        k = int(prefix.removeprefix("prefix"))
        return perturb_mover_incident_weights(reviewed.make_recentered_warm_fixture(k, 109), 31 + k, scale)
    b_text, seed_text = prefix.split("/")
    b, seed = int(b_text[1:]), int(seed_text.removeprefix("seed"))
    return perturb_mover_incident_weights(fixtures.make_sparse_warm_fixture(b, seed), seed, scale)


def run_fixed_trace_study(output):
    root = Path(__file__).resolve().parent.parent
    retained_path = root / "evidence/community-heterogeneous-band-20260921/receipt.json"
    retained_bytes = retained_path.read_bytes()
    if hashlib.sha256(retained_bytes).hexdigest() != "18eb630eb9ccb919af13d502c066384001d3b27453ba1276cb13b54b89fc5bb7":
        raise AssertionError("frozen input cohort receipt differs")
    retained = json.loads(retained_bytes)
    here = root / "experiments"
    for name, expected in retained["source_sha256"].items():
        if hashlib.sha256((here / name).read_bytes()).hexdigest() != expected:
            raise AssertionError(f"frozen source changed: {name}")
    rows, counts, sums = [], Counter(), {"offline": Counter(), "chronological": Counter(), "producer": Counter()}
    for previous in retained["rows"]:
        graph, p, q, initial, gamma = reconstruct_retained_source_fixture(previous["case"])
        edges = [(u, v, str(w)) for u in sorted(graph) for v, w in sorted(graph[u].items()) if u < v]
        input_hash = digest_canonical_evidence_value([edges, sorted(initial.items()), str(gamma)])
        if input_hash != previous["input_sha256"]:
            raise AssertionError("reconstructed input differs")
        proposal, terminal, generation = generate_matching_trace_proposal(graph, p, q, initial)
        oracle, labels, stop = fixtures.run_expanded_warm_oracle(graph, initial, gamma)
        if [[s, u, d, str(g)] for s, u, d, g in oracle] != previous["oracle_full_moves"]:
            raise AssertionError("replayed oracle differs from retained oracle")
        expected = proposal == [row[:3] for row in oracle] and terminal == stop
        if expected != previous["same_schedule_as_unperturbed"]:
            raise AssertionError("source-generated proposal differs from retained baseline schedule")
        reports = {"offline": offline.verify_single_flip_trace(graph, initial, gamma, proposal, terminal),
                   "chronological": chronological.verify_single_flip_trace(graph, initial, gamma, proposal, terminal)}
        for name, result in reports.items():
            if result.accepted != expected:
                raise AssertionError(f"incorrect acceptance: {name}/{previous['case']}")
            if result.accepted and (list(result.trace) != oracle or result.labels != labels):
                raise AssertionError(f"incorrect complete output: {name}/{previous['case']}")
        category = "retained_admission" if previous["admitted"] else "recovered_refusal" if expected else "changed_rejection"
        counts[category] += 1
        counts["cases"] += 1
        counts["admitted"] += expected
        if expected:
            counts["labels_checked_per_verifier"] += len(labels)
            counts["moves_checked_per_verifier"] += len(oracle)
            for name, result in reports.items():
                sums[name].update(result.metrics)
            sums["producer"].update(generation)
        rows.append({"case": previous["case"], "input_sha256": input_hash, "category": category,
                     "proposal": proposal, "terminal_sweep": terminal, "producer": generation,
                     "expected_accepted": expected, "oracle_labels_sha256": previous["oracle_labels_sha256"],
                     "oracle_moves_sha256": digest_canonical_evidence_value(previous["oracle_full_moves"]),
                     "verifiers": {name: {"accepted": result.accepted, "reason": result.reason,
                                          "metrics": result.metrics,
                                          "trace_sha256": digest_canonical_evidence_value(result.trace) if result.accepted else None}
                                   for name, result in reports.items()}})
    names = ("probe_single_flip_trace_verifier.py", "probe_threshold_trace_control.py", "probe_single_flip_trace_evidence.py",
             "test_single_flip_trace_verifier.py", "test_threshold_trace_control.py")
    payload = {"scope": "Fixed constructed cohort; exact supplied-trace certification; no wall/RSS measurements",
               "prior_receipt_sha256": hashlib.sha256(retained_bytes).hexdigest(),
               "source_sha256": {name: hashlib.sha256((here / name).read_bytes()).hexdigest() for name in names},
               "counts": counts, "admitted_logical_work_sums": sums, "rows": rows}
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as destination:
        json.dump(payload, destination, indent=2, default=str)
        destination.write("\n")
    print(json.dumps({key: value for key, value in payload.items() if key != "rows"}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    run_fixed_trace_study(parser.parse_args().output)
