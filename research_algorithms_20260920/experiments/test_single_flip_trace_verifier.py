"""Exact trace certification against expanded visits and objective differences."""

import importlib
import itertools
import random
import unittest
from fractions import Fraction as F


def load_single_flip_candidate():
    try:
        return importlib.import_module("probe_single_flip_trace_verifier")
    except ModuleNotFoundError as error:
        raise AssertionError("single-flip trace verifier is not implemented") from error


def compute_direct_partition_objective(graph, labels, gamma):
    degrees = {u: sum(row.values(), F(0)) for u, row in graph.items()}
    total = sum(degrees.values())
    internal = sum(w for u, row in graph.items() for v, w in row.items()
                   if labels[u] == labels[v])
    volumes = {}
    for u, label in labels.items():
        volumes[label] = volumes.get(label, F(0)) + degrees[u]
    return internal / total - gamma * sum(v * v for v in volumes.values()) / total ** 2


def replay_direct_objective_trace(graph, initial, gamma):
    labels, trace = dict(initial), []
    total = sum(sum(row.values(), F(0)) for row in graph.values())
    for sweep in range(1, 100):
        changed = False
        for u in sorted(graph):
            before = compute_direct_partition_objective(graph, labels, gamma)
            choices = []
            for destination in sorted(set(labels.values()) | {f"fresh:{sweep}:{u}"}):
                moved = dict(labels, **{})
                moved[u] = destination
                delta = compute_direct_partition_objective(graph, moved, gamma) - before
                choices.append((delta, destination))
            gain, destination = min(choices, key=lambda pair: (-pair[0], pair[1]))
            if gain > 0:
                labels[u] = destination
                trace.append((sweep, u, destination, gain * total ** 2 / 2))
                changed = True
        if not changed:
            return trace, labels, sweep
    raise AssertionError("tiny objective oracle did not terminate")


def build_weighted_graph_fixture(n, edges):
    graph = {u: {} for u in range(n)}
    for u, v, w in edges:
        graph[u][v] = graph[v][u] = F(w)
    return graph


class SingleFlipTraceVerificationTests(unittest.TestCase):
    def load_trace_verification_candidate(self):
        return load_single_flip_candidate()

    def test_exact_sampled_volume_extrema(self):
        candidate = load_single_flip_candidate()
        rng = random.Random(317)
        checks = 0
        for n in range(1, 8):
            for _ in range(24):
                sweeps = rng.randrange(1, 5)
                points = sorted(rng.sample(list(itertools.product(range(1, sweeps), range(n))),
                                           rng.randrange((sweeps - 1) * n + 1)))
                events = [(s, r, F(rng.randrange(-7, 8), 3)) for s, r in points]
                index = candidate.build_sampled_volume_index(n, F(17), events, sweeps)
                value, grid, lookup = F(17), {}, {(s, r): d for s, r, d in events}
                for s in range(1, sweeps + 1):
                    for r in range(n):
                        grid[s, r] = value
                        value += lookup.get((s, r), 0)
                for r in range(n):
                    for lo in range(1, sweeps + 1):
                        for hi in range(lo, sweeps + 1):
                            expected = [grid[s, r] for s in range(lo, hi + 1)]
                            self.assertEqual(index.query_sampled_volume_extrema(r, lo, hi),
                                             (min(expected), max(expected)))
                            checks += 1
                self.assertLessEqual(index.piece_count, len(events) + sweeps)
                self.assertLessEqual(index.entry_count, index.piece_count * 2 * n.bit_length())
        self.assertGreater(checks, 3000)

    def test_seeded_objective_oracle_parity(self):
        candidate = self.load_trace_verification_candidate()
        rng = random.Random(55013)
        accepted, unsupported, corruptions = 0, 0, 0
        for _ in range(140):
            n = rng.randrange(3, 8)
            edges = [(u, v, F(rng.randrange(1, 8), 3)) for u in range(n)
                     for v in range(u + 1, n) if rng.random() < .5]
            if not edges:
                edges = [(0, 1, F(1))]
            graph = build_weighted_graph_fixture(n, edges)
            initial = {u: "P" if u == 0 or (u != n - 1 and rng.randrange(2)) else "Q" for u in graph}
            gamma = rng.choice((F(1, 4), F(1), F(3, 2), F(2)))
            trace, final, sweeps = replay_direct_objective_trace(graph, initial, gamma)
            proposal = [row[:3] for row in trace]
            labels, structurally_valid, seen = dict(initial), True, set()
            for s, u, destination in proposal:
                structurally_valid &= u not in seen and destination == ("Q" if initial[u] == "P" else "P")
                seen.add(u)
                labels[u] = destination
                structurally_valid &= set(labels.values()) == {"P", "Q"}
            report = candidate.verify_single_flip_trace(graph, initial, gamma, proposal, sweeps)
            self.assertEqual(report.accepted, structurally_valid, (graph, initial, proposal, report.reason))
            if structurally_valid:
                accepted += 1
                self.assertEqual(list(report.trace), trace)
                self.assertEqual(report.labels, final)
                if proposal:
                    corrupted = proposal[1:]
                    self.assertFalse(candidate.verify_single_flip_trace(graph, initial, gamma, corrupted, sweeps).accepted)
                    corruptions += 1
            else:
                unsupported += 1
        self.assertGreater(accepted, 10)
        self.assertGreater(unsupported, 10)
        self.assertGreater(corruptions, 5)

    def test_exhaustive_three_vertex_proposals(self):
        candidate = self.load_trace_verification_candidate()
        pairs = [(0, 1), (0, 2), (1, 2)]
        checks = 0
        for mask in range(1, 8):
            graph = build_weighted_graph_fixture(3, [(u, v, 1) for i, (u, v) in enumerate(pairs) if mask >> i & 1])
            for partition in range(1, 7):
                initial = {u: "Q" if partition >> u & 1 else "P" for u in graph}
                for gamma in (F(1, 2), F(1), F(2)):
                    trace, _, stop = replay_direct_objective_trace(graph, initial, gamma)
                    expected = [row[:3] for row in trace]
                    for assignments in itertools.product(range(4), repeat=3):
                        proposal = sorted((s, u, "P" if initial[u] == "Q" else "Q")
                                          for u, s in enumerate(assignments) if s)
                        terminal = max(assignments) + 1
                        labels, nonempty = dict(initial), True
                        for _, u, d in proposal:
                            labels[u] = d
                            nonempty &= set(labels.values()) == {"P", "Q"}
                        verdict = candidate.verify_single_flip_trace(graph, initial, gamma, proposal, terminal)
                        self.assertEqual(verdict.accepted, nonempty and expected == proposal and stop == terminal,
                                         (mask, partition, gamma, proposal, verdict.reason))
                        checks += 1
        self.assertEqual(checks, 8064)

    def test_invalid_graph_and_proposal(self):
        candidate = self.load_trace_verification_candidate()
        graph = build_weighted_graph_fixture(4, [(0, 1, 1), (2, 3, 1)])
        initial = {0: "P", 1: "P", 2: "Q", 3: "Q"}
        self.assertTrue(candidate.verify_single_flip_trace(graph, initial, F(1), [], 1).accepted)
        for proposal, terminal in (([], 2), ([(2, 0, "Q")], 3), ([(1, 0, "P")], 2),
                                   ([(1, 0, "Q"), (2, 0, "P")], 3), ([(1, 8, "Q")], 2),
                                   ([(1, 0, "fresh")], 2), ([(1, 1, "Q"), (1, 0, "Q")], 2)):
            self.assertFalse(candidate.verify_single_flip_trace(graph, initial, F(1), proposal, terminal).accepted)
        for invalid in ({0: {0: F(1)}, 1: {}}, {0: {1: F(1)}, 1: {}},
                        {0: {1: F(-1)}, 1: {0: F(-1)}}, {0: {}, 1: {}}):
            with self.assertRaises(ValueError):
                candidate.verify_single_flip_trace(invalid, {0: "P", 1: "Q"}, F(1), [], 1)

    def test_terminal_and_fresh_refusals(self):
        candidate = self.load_trace_verification_candidate()
        graph = build_weighted_graph_fixture(4, [(0, 1, 1), (2, 3, 1)])
        initial = {0: "P", 1: "P", 2: "Q", 3: "Q"}
        report = candidate.verify_single_flip_trace(graph, initial, F(10), [], 1)
        self.assertFalse(report.accepted)
        self.assertIn("profitable skipped", report.reason)
        graph = build_weighted_graph_fixture(4, [(0, 2, 2), (1, 3, 2)])
        self.assertFalse(candidate.verify_single_flip_trace(graph, initial, F(1), [], 1).accepted)

    def test_matching_producer_without_oracle(self):
        try:
            producer = importlib.import_module("probe_single_flip_trace_evidence")
        except ModuleNotFoundError as error:
            self.fail(f"matching proposal producer is absent: {error}")
        import test_certified_warm_communities as fixtures
        import test_certified_warm_review_contracts as reviewed
        sources = [fixtures.make_sparse_warm_fixture(b, seed) for b in (16, 32) for seed in range(4)]
        sources.extend(reviewed.make_recentered_warm_fixture(k, 109) for k in range(15))
        for graph, p, q, initial, gamma in sources:
            proposal, sweeps, metrics = producer.generate_matching_trace_proposal(graph, p, q, initial)
            expected, _, stop = fixtures.run_expanded_warm_oracle(graph, initial, gamma)
            self.assertEqual((proposal, sweeps), ([row[:3] for row in expected], stop))
            self.assertLessEqual(metrics["mover_records_consumed"], len(graph) - len(p) - len(q))


if __name__ == "__main__":
    unittest.main()
