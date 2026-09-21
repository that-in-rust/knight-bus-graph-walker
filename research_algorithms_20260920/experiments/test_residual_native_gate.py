"""Native gate contracts; independent direct objective checks, not RAM tests."""

from fractions import Fraction as F
from itertools import combinations
import importlib
import random
import unittest

from probe_residual_degree_envelope import build_bounded_quotient_summary
from test_residual_degree_envelope import evaluate_original_modularity_direct


def replay_dense_objective_sweep(size, edges, gamma):
    labels = list(range(size))
    for node in range(size):
        old = evaluate_original_modularity_direct(size, edges, labels, gamma)
        options = []
        for target in sorted(set(labels) | {size + node}):
            changed = list(labels)
            changed[node] = target
            options.append((evaluate_original_modularity_direct(size, edges, changed, gamma), target))
        score, target = min(options, key=lambda value: (-value[0], value[1]))
        if score > old:
            labels[node] = target
    return tuple(labels)


class ResidualNativeGateTests(unittest.TestCase):
    def setUp(self):
        self.subject = importlib.import_module("probe_residual_native_gate")

    def test_sweep_matches_objective(self):
        randomizer = random.Random(91041)
        for case in range(90):
            size = 3 + case % 4
            edges = [(u, v, randomizer.randrange(4)) for u, v in combinations(range(size), 2)]
            edges.append((0, 0, 1 + case % 3))
            degrees = [sum(w * ((u == i) + (v == i)) for u, v, w in edges) for i in range(size)]
            pairs = {(u, v): F(w) for u, v, w in edges if u != v and w}
            gamma = (F(1, 2), F(1), F(3, 2))[case % 3]
            labels, metrics = self.subject.run_sparse_move_sweep(degrees, pairs, sum(w for _, _, w in edges), gamma)
            self.assertEqual(labels, replay_dense_objective_sweep(size, edges, gamma))
            self.assertEqual(metrics["visits"], size)

    def test_quotient_preserves_objective(self):
        graph = {0: {1: 3, 2: 1}, 1: {0: 3, 3: 2}, 2: {0: 1, 3: 4}, 3: {1: 2, 2: 4}, 4: {}}
        labels = {0: 17, 1: 17, 2: 21, 3: 32, 4: 33}
        base, stream, exact = self.subject.aggregate_exact_quotient_edges(graph, labels)
        self.assertEqual(base, {0: 0, 1: 0, 2: 1, 3: 2, 4: 3})
        self.assertEqual(sum(w for _, _, w in stream), 10)
        for coarse in ((0, 1, 2, 3), (0, 0, 1, 2), (0, 0, 0, 0)):
            lifted = [coarse[base[i]] for i in range(5)]
            original = [(u, v, w) for u in graph for v, w in graph[u].items() if u < v]
            q = evaluate_original_modularity_direct(5, original, lifted, F(1))
            contracted = [(u, v, w) for (u, v), w in exact.items()]
            self.assertEqual(q, evaluate_original_modularity_direct(4, contracted, coarse, F(1)))

    def test_candidates_preserve_baseline(self):
        randomizer = random.Random(91042)
        for case in range(120):
            size = 3 + case % 6
            edges = [(u, v, randomizer.randrange(6)) for u, v in combinations(range(size), 2)]
            edges += [(0, 0, 1)]
            summary = build_bounded_quotient_summary(size, iter(edges), case % 9)
            gamma = (F(1, 2), F(1), F(3, 2))[case % 3]
            candidates, _ = self.subject.create_summary_candidate_family(summary, gamma)
            base = evaluate_original_modularity_direct(size, edges, tuple(range(size)), gamma)
            for _, labels in candidates:
                bounds = self.subject.evaluate_candidate_score_bounds(summary, labels, gamma)
                actual = evaluate_original_modularity_direct(size, edges, labels, gamma)
                self.assertGreaterEqual(bounds["known_lower"], base)
                self.assertLessEqual(bounds["degree_lower"], actual)
                self.assertGreaterEqual(bounds["degree_upper"], actual)
                self.assertLessEqual(bounds["degree_upper"], bounds["group_upper"])
                self.assertLessEqual(bounds["group_upper"], bounds["mass_upper"])
                self.assertGreaterEqual(bounds["degree_lower"], bounds["mass_lower"])

    def test_full_budget_exactness(self):
        edges = [(0, 1, 4), (1, 2, 3), (2, 3, 2), (0, 0, 5)]
        summary = build_bounded_quotient_summary(4, iter(edges), 3)
        candidates, _ = self.subject.create_summary_candidate_family(summary, F(1))
        for _, labels in candidates:
            bounds = self.subject.evaluate_candidate_score_bounds(summary, labels, F(1))
            self.assertEqual(bounds["degree_lower"], bounds["degree_upper"])

    def test_degree_adds_information(self):
        summary = build_bounded_quotient_summary(6, ((0, j, 1) for j in range(1, 6)), 0)
        bounds = self.subject.evaluate_candidate_score_bounds(summary, (0, 0, 1, 1, 1, 1), F(1))
        self.assertGreater(bounds["degree_lower"], bounds["known_lower"])
        self.assertEqual(bounds["degree_lower"], bounds["degree_upper"])
        self.assertLess(bounds["degree_upper"], bounds["group_upper"])

    def test_baseline_ties_preserved(self):
        summary = build_bounded_quotient_summary(3, [(0, 1, 1), (1, 2, 1)], 0)
        candidates, _ = self.subject.create_summary_candidate_family(summary, F(1))
        winner = self.subject.select_certified_candidate_winner(summary, candidates, F(1), "degree_lower")
        self.assertEqual(winner, "baseline")

    def test_production_candidates_independent(self):
        a = [(u, v, 1) for u, v in combinations(range(8), 2) if (u < 4) == (v < 4)]
        b = [(u, v, 1) for u in range(4) for v in range(4, 8) if u != v - 4]
        sa = build_bounded_quotient_summary(8, iter(a), 0)
        sb = build_bounded_quotient_summary(8, iter(b), 0)
        self.assertEqual(sa, sb)
        self.assertEqual(self.subject.create_summary_candidate_family(sa, F(1)),
                         self.subject.create_summary_candidate_family(sb, F(1)))


if __name__ == "__main__":
    unittest.main()
