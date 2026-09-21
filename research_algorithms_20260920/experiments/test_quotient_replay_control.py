"""Exact replay is an ordinary necessary comparator, not a new algorithm."""

from fractions import Fraction as F
import importlib
import random
import unittest

from test_residual_degree_envelope import evaluate_original_modularity_direct


class QuotientReplayControlTests(unittest.TestCase):
    def setUp(self):
        self.subject = importlib.import_module("probe_quotient_replay_control")

    def test_single_pass_scores(self):
        events = [(0, 0, 3), (0, 1, 2), (0, 1, 4), (1, 2, F(1, 2))]
        candidates = [("base", (0, 1, 2, 3)), ("pair", (0, 0, 1, 2)), ("all", (0, 0, 0, 0))]
        stream = iter(events)
        scores, metrics = self.subject.score_streamed_candidate_family(4, stream, candidates, F(3, 2))
        self.assertEqual(list(stream), [])
        self.assertEqual(metrics["edge_records"], 4)
        self.assertEqual(metrics["label_pair_checks"], 12)
        self.assertEqual(metrics["retained_edge_records"], 0)
        self.assertEqual(metrics["supplied_label_records"], 12)
        for name, labels in candidates:
            self.assertEqual(scores[name], evaluate_original_modularity_direct(4, events, labels, F(3, 2)))

    def test_seeded_exact_scores(self):
        randomizer = random.Random(91043)
        for case in range(100):
            size = 2 + case % 7
            events = [(randomizer.randrange(size), randomizer.randrange(size), F(randomizer.randrange(6), 3)) for _ in range(25)]
            candidates = [(str(j), tuple(randomizer.randrange(size) for _ in range(size))) for j in range(5)]
            scores, _ = self.subject.score_streamed_candidate_family(size, iter(events), candidates, F(1))
            for name, labels in candidates:
                self.assertEqual(scores[name], evaluate_original_modularity_direct(size, events, labels, F(1)))

    def test_invalid_inputs_rejected(self):
        score = self.subject.score_streamed_candidate_family
        for edges, candidates, gamma in [([(0, 2, 1)], [("a", (0, 1))], 1),
                                          ([(0, 1, -1)], [("a", (0, 1))], 1),
                                          ([], [("a", (0, 1))], 1),
                                          ([(0, 1, 1)], [("a", (0,))], 1),
                                          ([(0, 1, 1)], [("a", (0, 1)), ("a", (1, 1))], 1),
                                          ([(0, 1, 1)], [("a", (0, 1))], 0)]:
            with self.assertRaises(ValueError):
                score(2, iter(edges), candidates, gamma)


if __name__ == "__main__":
    unittest.main()
