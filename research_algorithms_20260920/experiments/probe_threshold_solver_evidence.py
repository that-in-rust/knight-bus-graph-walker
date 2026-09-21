"""Fixed-cohort proposal-free solver evidence; all handoffs are retained."""

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path

from probe_single_flip_trace_evidence import digest_canonical_evidence_value, reconstruct_retained_source_fixture
from probe_threshold_event_solver import run_threshold_event_solver
import test_certified_warm_communities as fixtures


def run_fixed_solver_study(output):
    root = Path(__file__).resolve().parent.parent
    path = root / "evidence/community-heterogeneous-band-20260921/receipt.json"
    data = path.read_bytes()
    assert hashlib.sha256(data).hexdigest() == "18eb630eb9ccb919af13d502c066384001d3b27453ba1276cb13b54b89fc5bb7"
    previous = json.loads(data)
    here = root / "experiments"
    verifier_path = root / "evidence/community-single-flip-20260921/receipt.json"
    verifier_bytes = verifier_path.read_bytes()
    assert hashlib.sha256(verifier_bytes).hexdigest() == "5ab0af9c2b41c84648e9de53455090071cecad612892c39a9f81b1711b0838be"
    verifier = json.loads(verifier_bytes)
    for source in (previous, verifier):
        for name, expected in source["source_sha256"].items():
            assert hashlib.sha256((here / name).read_bytes()).hexdigest() == expected, name
    rows, counts, metrics = [], Counter(), Counter()
    for prior in previous["rows"]:
        graph, _, _, initial, gamma = reconstruct_retained_source_fixture(prior["case"])
        edges = [(u, v, str(w)) for u in sorted(graph) for v, w in sorted(graph[u].items()) if u < v]
        assert digest_canonical_evidence_value([edges, sorted(initial.items()), str(gamma)]) == prior["input_sha256"]
        moves, final, stop = fixtures.run_expanded_warm_oracle(graph, initial, gamma)
        assert [[s, u, d, str(g)] for s, u, d, g in moves] == prior["oracle_full_moves"]
        prefix, labels, status, pending = [], dict(initial), "complete", None
        for row in moves:
            sweep, u, destination, _ = row
            if destination not in ("P", "Q"):
                status, pending = "fresh_destination", (sweep, u)
                break
            if sum(label == labels[u] for label in labels.values()) == 1:
                status, pending = "empty_community", (sweep, u)
                break
            labels[u] = destination
            prefix.append(row)
        result = run_threshold_event_solver(graph, initial, gamma, max_moves=100 * len(graph))
        assert (result.status, list(result.trace), result.labels, result.next_visit) == (status, prefix, labels, pending)
        counts["cases"] += 1
        counts[result.status] += 1
        if result.status == "complete":
            assert (result.labels, result.sweeps) == (final, stop)
            counts["complete_labels_checked"] += len(final)
            counts["complete_moves_checked"] += len(moves)
            counts["complete_repeated_mover_cases"] += len({row[1] for row in moves}) < len(moves)
            counts["complete_changed_schedule_cases"] += not prior["same_schedule_as_unperturbed"]
            counts["cached_scalar_visit_model"] += len(graph) * stop
            metrics.update(result.metrics)
        rows.append({"case": prior["case"], "input_sha256": prior["input_sha256"],
                     "status": result.status, "sweeps": result.sweeps, "next_visit": result.next_visit,
                     "changed_schedule": not prior["same_schedule_as_unperturbed"],
                     "labels_sha256": digest_canonical_evidence_value(sorted(result.labels.items())),
                     "trace": [[s, u, d, str(g)] for s, u, d, g in result.trace], "metrics": result.metrics})
    names = ("probe_threshold_event_solver.py", "test_threshold_event_solver.py", "probe_threshold_solver_evidence.py")
    payload = {"scope": "Constructed fixed150; proposal-free exact two-label solver or explicit handoff; no timings/RSS",
               "prior_receipt_sha256": hashlib.sha256(data).hexdigest(),
               "verifier_receipt_sha256": hashlib.sha256(verifier_bytes).hexdigest(),
               "source_sha256": {name: hashlib.sha256((here / name).read_bytes()).hexdigest() for name in names},
               "counts": counts, "complete_logical_work_sums": metrics, "rows": rows}
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as destination:
        json.dump(payload, destination, indent=2)
        destination.write("\n")
    print(json.dumps({key: value for key, value in payload.items() if key != "rows"}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    run_fixed_solver_study(parser.parse_args().output)
