"""Independent objective parity and full handoff completion contracts."""

import importlib
import random
import unittest
from fractions import Fraction as F

import probe_threshold_event_solver as candidate
import test_single_flip_trace_verifier as oracle


def load_cached_control_module():
    try:
        return importlib.import_module("probe_cached_community_controls")
    except ModuleNotFoundError as error:
        raise AssertionError("cached scalar community controls are absent") from error


class CachedCommunityControlTests(unittest.TestCase):
    def test_seeded_complete_and_resume(self):
        control = load_cached_control_module()
        rng = random.Random(90612)
        resumed, complete = 0, 0
        for _ in range(120):
            n = rng.randrange(3, 8)
            edges = [(u, v, F(rng.randrange(1, 7), 3)) for u in range(n)
                     for v in range(u + 1, n) if rng.random() < .5] or [(0, 1, F(1))]
            graph = oracle.build_weighted_graph_fixture(n, edges)
            initial = {u: "P" if u == 0 or (u != n - 1 and rng.randrange(2)) else "Q" for u in graph}
            gamma = rng.choice((F(1, 2), F(1), F(3)))
            expected, labels, stop = oracle.replay_direct_objective_trace(graph, initial, gamma)
            general = control.run_general_scalar_solver(graph, initial, gamma, max_moves=300)
            self.assertEqual((general.status, list(general.trace), general.labels, general.sweeps),
                             ("complete", expected, labels, stop))
            scalar = control.run_cached_scalar_solver(graph, initial, gamma, max_moves=300)
            tree = candidate.run_threshold_event_solver(graph, initial, gamma, max_moves=300)
            self.assertEqual((scalar.status, scalar.trace, scalar.labels, scalar.sweeps, scalar.next_visit),
                             (tree.status, tree.trace, tree.labels, tree.sweeps, tree.next_visit))
            if tree.status != "complete":
                suffix = control.run_general_scalar_solver(graph, tree.labels, gamma, max_moves=300,
                                                          start_visit=tree.next_visit)
                self.assertEqual(list(tree.trace + suffix.trace), expected)
                self.assertEqual((suffix.status, suffix.labels, suffix.sweeps), ("complete", labels, stop))
                resumed += 1
            else:
                complete += 1
        self.assertGreater(resumed, 20)
        self.assertGreater(complete, 10)

    def test_zero_volume_tie_and_budget(self):
        control = load_cached_control_module()
        graph = oracle.build_weighted_graph_fixture(4, [(1, 2, F(1, 2))])
        initial = {0: "P", 1: "Q", 2: "Q", 3: "R"}
        expected, labels, stop = oracle.replay_direct_objective_trace(graph, initial, F(3))
        actual = control.run_general_scalar_solver(graph, initial, F(3), max_moves=20)
        self.assertEqual((list(actual.trace), actual.labels, actual.sweeps), (expected, labels, stop))
        self.assertEqual(actual.trace[0][2], "P")
        paused = control.run_general_scalar_solver(graph, initial, F(3), max_moves=0)
        self.assertEqual((paused.status, paused.trace, paused.labels, paused.next_visit),
                         ("move_budget", (), initial, (1, 1)))
        resumed = control.run_general_scalar_solver(graph, paused.labels, F(3), max_moves=20,
                                                   start_visit=paused.next_visit)
        self.assertEqual(list(resumed.trace), expected)

    def test_strict_resume_and_source_checks(self):
        control = load_cached_control_module()
        graph = oracle.build_weighted_graph_fixture(4, [(0, 1, 1), (2, 3, 1)])
        initial = dict(enumerate("PPQQ"))
        for pending in ((0, 0), (1, 10), (1, 0)):
            with self.assertRaises(ValueError):
                control.run_general_scalar_solver(graph, initial, F(1), max_moves=10, start_visit=pending)
        for budget in (-1, True, F(1, 2)):
            with self.assertRaises(ValueError):
                control.run_general_scalar_solver(graph, initial, F(1), max_moves=budget)


if __name__ == "__main__":
    unittest.main()
