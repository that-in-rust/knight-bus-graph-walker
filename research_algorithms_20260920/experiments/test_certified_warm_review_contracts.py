"""Lead regressions prompted by the separate warm-state theorem review."""

import unittest
from fractions import Fraction

import probe_certified_warm_communities as candidate
import test_certified_warm_communities as fixtures


def make_recentered_warm_fixture(k, seed=0, lower_boundary=False):
    graph, p, q, labels, gamma = fixtures.make_sparse_warm_fixture(seed=seed)
    y = sorted(set(graph) - p - q)
    degrees = {u: sum(row.values()) for u, row in graph.items()}
    total, degree = sum(degrees.values()), degrees[y[0]]
    alpha = sum(w for v, w in graph[y[0]].items() if v in p)
    beta = sum(w for v, w in graph[y[0]].items() if v in q)
    ap, aq = sum(degrees[u] for u in p), sum(degrees[u] for u in q)
    f0 = total * (beta - alpha) - gamma * degree * (aq - ap - degree * len(y)) - 2 * gamma * degree ** 2 * (k + 1)
    target = -(total - gamma * degree ** 2) if lower_boundary else Fraction(0)
    delta = (target - f0) / (2 * gamma * degree)
    for anchors, sign in ((p, 1), (q, -1)):
        edges = [(u, v) for u in anchors for v in graph[u] if v in anchors and u < v]
        for u, v in edges:
            graph[u][v] += sign * delta / (2 * len(edges))
            graph[v][u] = graph[u][v]
            if graph[u][v] <= 0:
                raise AssertionError("recentered fixture lost positivity")
    labels.update({u: "Q" if j < k else "P" for j, u in enumerate(y)})
    return graph, p, q, labels, gamma


class CertifiedWarmReviewContracts(unittest.TestCase):
    def test_all_prefix_positions_parity(self):
        checker = fixtures.CertifiedWarmCommunityTests()
        for k in range(15):
            for seed in (7, 109):
                with self.subTest(k=k, seed=seed):
                    checker.assert_complete_trace_parity(make_recentered_warm_fixture(k, seed), k in (0, 14))

    def test_lower_band_equality_refusal(self):
        with self.assertRaisesRegex(ValueError, "three-state band"):
            candidate.certify_warm_graph_state(*make_recentered_warm_fixture(2, 19, True))

    def test_isolated_anchor_singleton_refusal(self):
        graph = {u: {} for u in range(-2, 8)}
        for u, v in ((0, 3), (1, 4), (2, 5), (6, 7)):
            fixtures.add_undirected_weight_edge(graph, u, v, 1)
        labels = {u: "Q" if u == -1 or 0 <= u < 3 else "P" for u in graph}
        with self.assertRaisesRegex(ValueError, "fresh-community"):
            candidate.certify_warm_graph_state(graph, {-2}, {-1}, labels, Fraction(3))

    def test_unbounded_identifier_width_parity(self):
        graph, p, q, labels, gamma = fixtures.make_sparse_warm_fixture(seed=3)
        offset = 2 ** 512
        translated = ({u + offset: {v + offset: w for v, w in row.items()} for u, row in graph.items()},
                      {u + offset for u in p}, {u + offset for u in q},
                      {u + offset: token for u, token in labels.items()}, gamma)
        fixtures.CertifiedWarmCommunityTests().assert_complete_trace_parity(translated)


if __name__ == "__main__":
    unittest.main()
