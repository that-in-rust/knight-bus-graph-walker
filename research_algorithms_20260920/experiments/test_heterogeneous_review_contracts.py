"""Review-driven implementation regressions; separate frozen study preserved."""

import unittest
from fractions import Fraction
from types import SimpleNamespace

import probe_certified_warm_communities as uniform
import probe_heterogeneous_band_communities as candidate
import test_certified_warm_communities as fixtures
import test_heterogeneous_band_communities as heterogeneous


def construct_four_anchor_family(b, epsilon, reverse):
    p, q = {-9, 4}, {-1, 12}
    y = [u for u in range(3 * b) if u not in p | q][:b]
    graph = {u: {} for u in p | q | set(y)}
    for anchors in (p, q):
        u, v = sorted(anchors)
        fixtures.add_undirected_weight_edge(graph, u, v, Fraction(3 * b + 1, 4))
    for i, u in enumerate(y):
        for v in p | q:
            extra = epsilon * (i + 1) if v == min(p) else epsilon * (2 * i + 1) if v == max(q) else 0
            fixtures.add_undirected_weight_edge(graph, u, v, Fraction(b, 2) + extra)
    pairs = list(zip(y[:b // 2], reversed(y[b // 2:]))) if reverse else list(zip(y[::2], y[1::2]))
    for j, (u, v) in enumerate(pairs):
        fixtures.add_undirected_weight_edge(graph, u, v, 1 + epsilon * (j + 1))
    labels = {u: "Q" if u in q else "P" for u in graph}
    labels.update({u: "Q" if i < b // 2 - 1 else "P" for i, u in enumerate(y)})
    return graph, p, q, labels, Fraction(1, 2)


class HeterogeneousReviewContracts(unittest.TestCase):
    def test_independent_small_family_parity(self):
        for b in (4, 6, 8):
            for epsilon in (Fraction(0), Fraction(1, 1000), Fraction(-1, 1000)):
                for reverse in (False, True):
                    fixture = construct_four_anchor_family(b, epsilon, reverse)
                    heterogeneous.compare_heterogeneous_complete_output(fixture, True)
                    if epsilon:
                        with self.assertRaisesRegex(ValueError, "uniform"):
                            uniform.certify_warm_graph_state(*fixture)

    def test_fixed_mate_degree_contribution(self):
        mover = SimpleNamespace(degree=Fraction(1), alpha=Fraction(5, 8),
                                beta=Fraction(1, 8), matching_weight=Fraction(1, 4))
        self.assertEqual(candidate.bound_conditioned_mover_gains(40, 1, mover, 19, 19, False, True),
                         (-9, -9, -5))
        self.assertEqual(candidate.bound_conditioned_mover_gains(40, 1, mover, 10, 10, False, True),
                         (9, 9, 4))
        graph = {u: {} for u in range(8)}
        for u, v, weight in ((0, 4, Fraction(5, 8)), (0, 6, Fraction(1, 8)),
                              (0, 1, Fraction(1, 4)), (1, 4, Fraction(35, 4)),
                              (2, 4, 1), (3, 4, 2), (2, 3, 1),
                              (4, 5, Fraction(21, 16)), (6, 7, Fraction(79, 16))):
            fixtures.add_undirected_weight_edge(graph, u, v, weight)
        labels = {u: "Q" if u in (1, 6, 7) else "P" for u in graph}
        degrees = {u: sum(row.values()) for u, row in graph.items()}
        self.assertEqual(sum(degrees.values()), 40)
        self.assertEqual(sum(degrees[u] for u in (6, 7)), 10)
        self.assertEqual(tuple(degrees[u] for u in range(4)), (1, 9, 2, 3))
        before = fixtures.compute_expanded_modularity_value(graph, labels, Fraction(1))
        labels[0] = "Q"
        after = fixtures.compute_expanded_modularity_value(graph, labels, Fraction(1))
        self.assertEqual(after - before, Fraction(-18, 1600))

    def test_variable_width_identifier_parity(self):
        graph, p, q, labels, gamma = construct_four_anchor_family(8, Fraction(1, 1000), True)
        offset = 2 ** 512
        fixture = ({u + offset: {v + offset: w for v, w in row.items()} for u, row in graph.items()},
                   {u + offset for u in p}, {u + offset for u in q},
                   {u + offset: label for u, label in labels.items()}, gamma)
        heterogeneous.compare_heterogeneous_complete_output(fixture, True)


if __name__ == "__main__":
    unittest.main()
