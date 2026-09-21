"""Independently supplied mathematical witnesses against the lead solver."""

import unittest
from fractions import Fraction as F

from probe_threshold_event_solver import run_threshold_event_solver
from test_single_flip_trace_verifier import build_weighted_graph_fixture


def run_review_source_fixture(n, edges, vector, gamma, budget=100):
    graph = build_weighted_graph_fixture(n, edges)
    labels = {u: "Q" if bit else "P" for u, bit in enumerate(vector)}
    result = run_threshold_event_solver(graph, labels, gamma, max_moves=budget)
    total = sum(sum(row.values()) for row in graph.values())
    actual = tuple((s, u, dest, 2 * gain / total**2) for s, u, dest, gain in result.trace)
    return result, actual


class ThresholdSolverReviewContracts(unittest.TestCase):
    def test_repeated_movers_exceed_singlepass(self):
        result, actual = run_review_source_fixture(5, [(0, 3, 1), (2, 3, 1), (3, 4, 5)],
                                                   (0, 1, 0, 1, 0), F(3, 4))
        self.assertEqual(actual, ((1, 0, "Q", F(53, 392)), (1, 2, "Q", F(47, 392)),
                                  (1, 3, "P", F(15, 56)), (2, 0, "P", F(23, 392)),
                                  (2, 2, "P", F(17, 392))))
        self.assertEqual((result.status, result.sweeps), ("complete", 3))
        self.assertEqual(result.metrics["neighbor_affinity_updates"], 7)
        self.assertGreater(result.metrics["neighbor_affinity_updates"], 2 * 3)

    def test_global_volume_activates_nonneighbor(self):
        result, actual = run_review_source_fixture(5, [(0, 3, 2), (1, 2, 1)],
                                                   (0, 1, 1, 1, 0), F(4, 3))
        self.assertEqual(actual[:2], ((1, 0, "Q", F(2, 27)), (1, 1, "P", F(1, 27))))
        self.assertEqual(result.status, "complete")

    def test_last_rank_requires_terminal_sweep(self):
        result, actual = run_review_source_fixture(6, [(0, 3, 6), (0, 4, 6), (1, 2, 6),
                                                       (1, 5, 1), (3, 4, 5)],
                                                   (0, 1, 1, 0, 0, 0), F(2), budget=1)
        self.assertEqual(actual, ((1, 5, "Q", F(5, 64)),))
        self.assertEqual((result.status, result.sweeps, result.next_visit), ("complete", 2, None))

    def test_handoffs_precede_budget_refusal(self):
        fresh, trace = run_review_source_fixture(4, [(0, 1, 1), (0, 2, 1), (1, 3, 1), (2, 3, 1)],
                                                 (0, 0, 1, 0), F(3), budget=0)
        self.assertEqual((fresh.status, fresh.next_visit, trace), ("fresh_destination", (1, 0), ()))
        empty, trace = run_review_source_fixture(2, [(0, 1, 1)], (0, 1), F(1), budget=0)
        self.assertEqual((empty.status, empty.next_visit, trace), ("empty_community", (1, 0), ()))


if __name__ == "__main__":
    unittest.main()
