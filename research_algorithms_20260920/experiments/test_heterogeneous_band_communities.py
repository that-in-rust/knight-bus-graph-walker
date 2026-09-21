"""Verification-first tests for unequal-degree community stream admission."""

import importlib
import random
import unittest
from fractions import Fraction
from itertools import combinations, product
from types import SimpleNamespace

import probe_certified_warm_communities as uniform
import test_certified_warm_communities as fixtures
import test_certified_warm_review_contracts as reviewed


def load_heterogeneous_band_module():
    try:
        return importlib.import_module("probe_heterogeneous_band_communities")
    except ModuleNotFoundError as error:
        raise AssertionError("heterogeneous band module not implemented") from error


def perturb_mover_incident_weights(fixture, seed, scale=Fraction(1, 10000)):
    graph, p, q, labels, gamma = fixture
    rng = random.Random(seed)
    y = set(graph) - p - q
    for u in sorted(graph):
        for v in sorted(graph[u]):
            if u < v and (u in y or v in y):
                weight = graph[u][v] + rng.randint(-5, 5) * scale
                if weight <= 0:
                    raise ValueError("fixture perturbation would produce nonpositive weight")
                graph[u][v] = graph[v][u] = weight
    return fixture


def compare_heterogeneous_complete_output(fixture, objective=False):
    module = load_heterogeneous_band_module()
    graph, p, q, initial, gamma = fixture
    prepared = module.certify_heterogeneous_band_graph(*fixture)
    low, high, trace = [], [], []
    ls, hs = fixtures.OnceOnlyRecordIterator(prepared.low), fixtures.OnceOnlyRecordIterator(prepared.high)
    result = module.run_heterogeneous_band_streams(prepared.header, ls, hs,
                                                  low.append, high.append, trace.append)
    labels = list(uniform.merge_certified_label_streams(iter(prepared.anchors), iter(low), iter(high)))
    expected_moves, expected_labels, expected_sweeps = fixtures.run_expanded_warm_oracle(
        graph, initial, gamma, objective)
    if (trace != expected_moves or labels != sorted(expected_labels.items())
            or result["sweeps"] != expected_sweeps):
        raise AssertionError("heterogeneous complete trace/output does not match expanded oracle")
    degrees = {u: sum(row.values()) for u, row in graph.items()}
    actual_dq = sum(degrees[u] for u in graph if u not in p | q and expected_labels[u] == "Q")
    if result["mover_q_degree"] != actual_dq or ls.read + hs.read != prepared.header.b:
        raise AssertionError("volume accounting or one-pass record count mismatch")
    return prepared, result, trace, labels


class HeterogeneousBandCommunityTests(unittest.TestCase):
    def test_exhaustive_pair_degree_extrema(self):
        module = load_heterogeneous_band_module()
        checks = 0
        for b in range(2, 7):
            for values in product((1, 2, 4), repeat=b):
                order = module.MoverDegreeOrderStatistics(values)
                for u, v in combinations(range(b), 2):
                    remaining = [values[j] for j in range(b) if j not in (u, v)]
                    for count in range(b - 1):
                        sums = [sum(selected) for selected in combinations(remaining, count)]
                        self.assertEqual(order.sum_excluding_pair_bounds(count, u, v), (min(sums), max(sums)))
                        checks += 1
        self.assertEqual(checks, 66024)

    def test_mover_bounds_match_subsets(self):
        module = load_heterogeneous_band_module()
        rng = random.Random(876)
        for _ in range(50):
            values = [Fraction(rng.randrange(3, 12)) for _ in range(6)]
            order = module.MoverDegreeOrderStatistics(values)
            u, v = rng.sample(range(6), 2)
            mover = SimpleNamespace(degree=values[u], alpha=(values[u] - 1) / 3,
                                    beta=2 * (values[u] - 1) / 3, matching_weight=Fraction(1))
            total, gamma, aq = 60 + sum(values), Fraction(1, 2), Fraction(20)
            remaining = [values[j] for j in range(6) if j not in (u, v)]
            for in_q, mate_q, r in product((False, True), (False, True), range(7)):
                count = r - in_q - mate_q
                if not 0 <= count <= 4:
                    continue
                lo, hi = order.sum_excluding_pair_bounds(count, u, v)
                base = aq + in_q * values[u] + mate_q * values[v]
                bound = module.bound_conditioned_mover_gains(total, gamma, mover, base + lo, base + hi, in_q, mate_q)
                switches, singles = [], []
                for selected in combinations(remaining, count):
                    qvol = base + sum(selected)
                    own = mover.beta if in_q else mover.alpha
                    other = mover.alpha if in_q else mover.beta
                    source, destination = (qvol, total - qvol) if in_q else (total - qvol, qvol)
                    if in_q == mate_q:
                        own += mover.matching_weight
                    else:
                        other += mover.matching_weight
                    switches.append(total * (other - own) - gamma * mover.degree * (destination - source + mover.degree))
                    singles.append(-total * own + gamma * mover.degree * (source - mover.degree))
                self.assertEqual(bound, (min(switches), max(switches), max(singles)))

    def test_unequal_weight_complete_parity(self):
        for b, seed in product((16, 24), range(6)):
            fixture = perturb_mover_incident_weights(fixtures.make_sparse_warm_fixture(b, seed, perturb=True), seed)
            graph, p, q = fixture[:3]
            self.assertGreater(len({sum(graph[u].values()) for u in graph if u not in p | q}), 1)
            with self.assertRaisesRegex(ValueError, "uniform"):
                uniform.certify_warm_graph_state(*fixture)
            compare_heterogeneous_complete_output(fixture, seed == 0)

    def test_all_prefix_positions_parity(self):
        for k in range(15):
            fixture = perturb_mover_incident_weights(reviewed.make_recentered_warm_fixture(k, 109), 31 + k)
            compare_heterogeneous_complete_output(fixture, k in (0, 14))

    def test_uniform_and_even_parity(self):
        for fixture in (fixtures.make_sparse_warm_fixture(dense=True),
                        fixtures.make_even_crossing_fixture(False), fixtures.make_even_crossing_fixture(True)):
            compare_heterogeneous_complete_output(fixture, True)

    def test_unsafe_sign_and_singleton(self):
        module = load_heterogeneous_band_module()
        graph, p, q, initial, gamma = fixtures.make_sparse_warm_fixture()
        high = max(set(graph) - p - q)
        fixtures.add_undirected_weight_edge(graph, high, min(p), 1000)
        with self.assertRaises(ValueError):
            module.certify_heterogeneous_band_graph(graph, p, q, initial, gamma)
        graph = {u: {} for u in range(-2, 8)}
        for u, v in ((0, 3), (1, 4), (2, 5), (6, 7)):
            fixtures.add_undirected_weight_edge(graph, u, v, 1)
        initial = {u: "Q" if u == -1 or 0 <= u < 3 else "P" for u in graph}
        with self.assertRaisesRegex(ValueError, "fresh"):
            module.certify_heterogeneous_band_graph(graph, {-2}, {-1}, initial, Fraction(3))

    def test_invalid_inputs_are_refused(self):
        module = load_heterogeneous_band_module()
        for kind in ("loop", "asymmetric", "matching", "prefix", "partition", "float"):
            graph, p, q, initial, gamma = fixtures.make_sparse_warm_fixture()
            y = sorted(set(graph) - p - q)
            if kind == "loop":
                graph[y[0]][y[0]] = Fraction(1)
            elif kind == "asymmetric":
                del graph[y[0]][next(iter(graph[y[0]]))]
            elif kind == "matching":
                v = next(v for v in y[1:] if v not in graph[y[0]])
                fixtures.add_undirected_weight_edge(graph, y[0], v, 1)
            elif kind == "prefix":
                initial[y[0]], initial[y[-1]] = "P", "Q"
            elif kind == "partition":
                q.add(min(p))
            else:
                gamma = 0.5
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                module.certify_heterogeneous_band_graph(graph, p, q, initial, gamma)

    def test_truncated_and_trailing_streams(self):
        module = load_heterogeneous_band_module()
        prepared = module.certify_heterogeneous_band_graph(*perturb_mover_incident_weights(
            fixtures.make_sparse_warm_fixture(), 43))
        for low, high in ((prepared.low[:-1], prepared.high),
                          (prepared.low, prepared.high + prepared.high[:1])):
            with self.assertRaises(ValueError):
                module.run_heterogeneous_band_streams(prepared.header, iter(low), iter(high),
                                                     lambda row: None, lambda row: None, lambda row: None)


if __name__ == "__main__":
    unittest.main()
