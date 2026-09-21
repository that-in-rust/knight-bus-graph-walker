"""Exact event discovery, including explicit handoff outside two labels."""

import importlib
import random
import unittest
from fractions import Fraction as F

import probe_threshold_trace_control as control
import test_single_flip_trace_verifier as oracle


def load_threshold_solver_module():
    try:
        return importlib.import_module("probe_threshold_event_solver")
    except ModuleNotFoundError as error:
        raise AssertionError("proposal-free threshold solver is absent") from error


class ThresholdEventSolverTests(unittest.TestCase):
    def test_first_violation_matches_scan(self):
        solver = load_threshold_solver_module()
        rng = random.Random(711)
        for n in range(1, 25):
            for _ in range(20):
                bounds = [(F(rng.randrange(-10, 20), 3), None) if rng.randrange(2)
                          else (None, F(rng.randrange(-10, 20), 5)) for _ in range(n)]
                tree = control.RankStayThresholdTree(bounds)
                volume = F(rng.randrange(-5, 15), 3)
                for first in range(n + 1):
                    expected = next((i for i in range(first, n)
                                     if (bounds[i][0] is not None and volume < bounds[i][0])
                                     or (bounds[i][1] is not None and volume > bounds[i][1])), None)
                    found, visited = solver.find_first_threshold_violation(tree, first, volume)
                    self.assertEqual(found, expected)
                    self.assertLessEqual(visited, 4 * n.bit_length() + 1)

    def test_seeded_full_objective_traces(self):
        solver = load_threshold_solver_module()
        rng = random.Random(9097)
        complete, fresh, empty, repeated = 0, 0, 0, 0
        for _ in range(260):
            n = rng.randrange(3, 9)
            edges = [(u, v, F(rng.randrange(1, 10), 3)) for u in range(n)
                     for v in range(u + 1, n) if rng.random() < .5]
            if not edges:
                edges = [(0, 1, F(1))]
            graph = oracle.build_weighted_graph_fixture(n, edges)
            initial = {u: "P" if u == 0 or (u != n - 1 and rng.randrange(2)) else "Q" for u in graph}
            gamma = rng.choice((F(1, 4), F(1, 2), F(1), F(3, 2), F(3)))
            trace, final, stop = oracle.replay_direct_objective_trace(graph, initial, gamma)
            prefix, labels, expected_status, pending = [], dict(initial), "complete", None
            for row in trace:
                s, u, destination, _ = row
                if destination not in ("P", "Q"):
                    expected_status, pending = "fresh_destination", (s, u)
                    break
                if sum(label == labels[u] for label in labels.values()) == 1:
                    expected_status, pending = "empty_community", (s, u)
                    break
                labels[u] = destination
                prefix.append(row)
            result = solver.run_threshold_event_solver(graph, initial, gamma, max_moves=500)
            self.assertEqual((result.status, list(result.trace), result.labels, result.next_visit),
                             (expected_status, prefix, labels, pending))
            if result.status == "complete":
                complete += 1
                self.assertEqual((result.labels, result.sweeps), (final, stop))
                self.assertEqual(result.metrics["covered_visits"], n * stop)
            elif result.status == "fresh_destination":
                fresh += 1
            else:
                empty += 1
            repeated += len({row[1] for row in prefix}) < len(prefix)
        self.assertGreater(complete, 20)
        self.assertGreater(fresh, 20)
        self.assertGreater(empty, 10)
        self.assertGreater(repeated, 0)

    def test_move_budget_preserves_prefix(self):
        solver = load_threshold_solver_module()
        import test_certified_warm_communities as fixtures
        graph, _, _, initial, gamma = fixtures.make_sparse_warm_fixture(32, 6)
        trace, final, stop = fixtures.run_expanded_warm_oracle(graph, initial, gamma)
        self.assertGreater(len(trace), 2)
        for budget in (0, 1, len(trace) - 1, len(trace)):
            result = solver.run_threshold_event_solver(graph, initial, gamma, max_moves=budget)
            self.assertEqual(list(result.trace), trace[:budget])
            if budget < len(trace):
                self.assertEqual(result.status, "move_budget")
                self.assertEqual(result.next_visit, trace[budget][:2])
            else:
                self.assertEqual((result.status, result.labels, result.sweeps), ("complete", final, stop))
        for invalid in (-1, True, F(1, 2)):
            with self.assertRaises(ValueError):
                solver.run_threshold_event_solver(graph, initial, gamma, max_moves=invalid)

    def test_wide_identifiers_and_isolates(self):
        solver = load_threshold_solver_module()
        graph = oracle.build_weighted_graph_fixture(5, [(0, 1, 1), (2, 3, 1)])
        initial = {u: "P" if u < 2 else "Q" for u in graph}
        ids = {u: 2 ** 512 + u * 11 for u in graph}
        translated = {ids[u]: {ids[v]: w for v, w in row.items()} for u, row in graph.items()}
        labels = {ids[u]: label for u, label in initial.items()}
        result = solver.run_threshold_event_solver(translated, labels, F(1), max_moves=0)
        self.assertEqual((result.status, result.labels, result.sweeps), ("complete", labels, 1))


if __name__ == "__main__":
    unittest.main()
