"""Explicit boundary witnesses from the separate mathematical verifier review."""

import unittest
from fractions import Fraction as F

import probe_single_flip_trace_verifier as offline
import probe_threshold_trace_control as chronological
import probe_threshold_event_solver as solver
from test_single_flip_trace_verifier import build_weighted_graph_fixture, replay_direct_objective_trace


class SingleFlipReviewContracts(unittest.TestCase):
    def test_positive_tie_isolated_community(self):
        graph = build_weighted_graph_fixture(3, [(1, 2, F(1, 2))])
        initial = {0: "P", 1: "Q", 2: "Q"}
        for verifier in (offline, chronological):
            result = verifier.verify_single_flip_trace(graph, initial, F(3), [(1, 1, "P")], 2)
            self.assertTrue(result.accepted)
            self.assertEqual(result.trace, ((1, 1, "P", F(1, 4)),))
        result = solver.run_threshold_event_solver(graph, initial, F(3), max_moves=4)
        self.assertEqual((result.status, result.trace), ("complete", ((1, 1, "P", F(1, 4)),)))

    def test_terminal_sweep_requires_revisit(self):
        graph = build_weighted_graph_fixture(6, [(1, 2, 1), (1, 3, F(1, 2)),
                                                 (2, 3, F(3, 2)), (2, 4, 1)])
        initial = dict(enumerate("QPPQQP"))
        expected, labels, stop = replay_direct_objective_trace(graph, initial, F(1, 2))
        self.assertEqual(([row[:3] for row in expected], stop), ([(1, 2, "Q"), (2, 1, "Q")], 3))
        for verifier in (offline, chronological):
            self.assertFalse(verifier.verify_single_flip_trace(graph, initial, F(1, 2), [(1, 2, "Q")], 2).accepted)
            result = verifier.verify_single_flip_trace(graph, initial, F(1, 2), [row[:3] for row in expected], stop)
            self.assertEqual((result.accepted, list(result.trace), result.labels), (True, expected, labels))
        result = solver.run_threshold_event_solver(graph, initial, F(1, 2), max_moves=4)
        self.assertEqual((result.status, list(result.trace), result.sweeps), ("complete", expected, stop))

    def test_tiny_rational_boundary_signs(self):
        graph = build_weighted_graph_fixture(3, [(1, 2, F(1, 2))])
        initial = dict(enumerate("PPQ"))
        for delta in (-F(1, 10 ** 60), F(0), F(1, 10 ** 60)):
            gamma = 2 + delta
            expected, labels, stop = replay_direct_objective_trace(graph, initial, gamma)
            self.assertEqual(len(expected), int(delta < 0))
            for verifier in (offline, chronological):
                result = verifier.verify_single_flip_trace(graph, initial, gamma, [row[:3] for row in expected], stop)
                self.assertEqual((result.accepted, list(result.trace), result.labels), (True, expected, labels))
            result = solver.run_threshold_event_solver(graph, initial, gamma, max_moves=4)
            self.assertEqual((result.status, list(result.trace), result.labels), ("complete", expected, labels))

    def test_fresh_destination_is_not_suppressed(self):
        graph = build_weighted_graph_fixture(3, [(0, 2, F(1, 2)), (1, 2, F(1, 2))])
        initial = dict(enumerate("PPQ"))
        for verifier in (offline, chronological):
            self.assertFalse(verifier.verify_single_flip_trace(graph, initial, F(3), [(1, 0, "Q")], 2).accepted)
        result = solver.run_threshold_event_solver(graph, initial, F(3), max_moves=4)
        self.assertEqual((result.status, result.trace, result.labels, result.next_visit),
                         ("fresh_destination", (), initial, (1, 0)))


if __name__ == "__main__":
    unittest.main()
