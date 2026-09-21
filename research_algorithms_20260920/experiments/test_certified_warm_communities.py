"""Independent expanded-graph oracle for certified warm-state streams."""

import importlib
import random
import unittest
from fractions import Fraction
from itertools import combinations


def load_certified_warm_module():
    try:
        return importlib.import_module("probe_certified_warm_communities")
    except ModuleNotFoundError as error:
        raise AssertionError("certified warm-state module not implemented") from error


def add_undirected_weight_edge(graph, u, v, weight):
    weight = Fraction(weight)
    graph[u][v] = graph[u].get(v, Fraction(0)) + weight
    graph[v][u] = graph[v].get(u, Fraction(0)) + weight


def make_sparse_warm_fixture(b=16, seed=0, perturb=False, dense=False):
    rng = random.Random(seed)
    identifiers = list(range(4 * b))
    rng.shuffle(identifiers)
    p = sorted(identifiers[:2 * b])
    q = sorted(identifiers[2 * b:3 * b])
    y = sorted(identifiers[3 * b:])
    graph = {u: {} for u in identifiers}
    for anchors in (p, q):
        if dense:
            for u, v in combinations(anchors, 2):
                add_undirected_weight_edge(graph, u, v, 11)
        else:
            cycle = rng.sample(anchors, len(anchors))
            for j, u in enumerate(cycle):
                add_undirected_weight_edge(
                    graph, u, cycle[(j + 1) % len(cycle)],
                    Fraction(11 * (len(cycle) - 1), 2))
    pp, qq = rng.sample(p, len(p)), rng.sample(q, len(q))
    for j, u in enumerate(y):
        if dense:
            for v in p:
                add_undirected_weight_edge(graph, u, v, 3)
            for v in q:
                add_undirected_weight_edge(graph, u, v, 4)
        else:
            for v in pp[2 * j:2 * j + 2]:
                add_undirected_weight_edge(graph, u, v, 3 * b)
            add_undirected_weight_edge(graph, u, qq[j], 4 * b)
    match = rng.sample(y, b)
    for u, v in zip(match[::2], match[1::2]):
        add_undirected_weight_edge(graph, u, v, 1)
    if perturb:
        add_undirected_weight_edge(graph, p[0], p[1], Fraction(1, 100))
    labels = {u: "P" for u in p}
    labels.update({u: "Q" for u in q})
    labels.update({u: "Q" if j < 3 * b // 4 - 1 else "P"
                   for j, u in enumerate(y)})
    return graph, set(p), set(q), labels, Fraction(1, 2)


def make_even_crossing_fixture(crossing):
    p, q, y = range(6), range(6, 12), range(12, 18)
    graph = {u: {} for u in range(18)}
    for anchors in (p, q):
        for u, v in combinations(anchors, 2):
            add_undirected_weight_edge(graph, u, v, Fraction(7, 10))
    for u in y:
        for v in range(12):
            add_undirected_weight_edge(graph, u, v, Fraction(2, 3))
    pairs = ((12, 14), (13, 15), (16, 17)) if crossing else (
        (12, 13), (14, 15), (16, 17))
    for u, v in pairs:
        add_undirected_weight_edge(graph, u, v, 1)
    labels = {u: "P" if u < 6 or u >= 14 else "Q" for u in graph}
    return graph, set(p), set(q), labels, Fraction(8, 9)


def compute_expanded_modularity_value(graph, labels, gamma):
    degrees = {u: sum(row.values(), Fraction(0)) for u, row in graph.items()}
    total = sum(degrees.values())
    volumes = {}
    for u, label in labels.items():
        volumes[label] = volumes.get(label, Fraction(0)) + degrees[u]
    internal = sum(weight for u, row in graph.items() for v, weight in row.items()
                   if labels[u] == labels[v])
    return internal / total - gamma * sum(v * v for v in volumes.values()) / total ** 2


def run_expanded_warm_oracle(graph, initial, gamma, check_objective=False):
    labels = dict(initial)
    degrees = {u: sum(row.values(), Fraction(0)) for u, row in graph.items()}
    total = sum(degrees.values())
    trace = []
    for sweep in range(1, len(graph) ** 2 + 1):
        changed = False
        for u in sorted(graph):
            before = labels[u]
            volumes, affinity = {}, {}
            for v, token in labels.items():
                if v != u:
                    volumes[token] = volumes.get(token, Fraction(0)) + degrees[v]
                    affinity[token] = affinity.get(token, Fraction(0)) + graph[u].get(v, 0)
            fresh = f"fresh:{sweep}:{u}"
            scores = {token: total * affinity.get(token, 0) -
                      gamma * degrees[u] * volumes.get(token, 0)
                      for token in set(labels.values()) | {fresh}}
            best = min(scores, key=lambda token: (-scores[token], token))
            gain = scores[best] - scores[before]
            if gain > 0:
                old_quality = compute_expanded_modularity_value(graph, labels, gamma) if check_objective else None
                labels[u] = best
                if check_objective:
                    new_quality = compute_expanded_modularity_value(graph, labels, gamma)
                    if new_quality - old_quality != 2 * gain / total ** 2:
                        raise AssertionError("oracle gain disagrees with expanded modularity")
                trace.append((sweep, u, best, gain))
                changed = True
        if not changed:
            return trace, labels, sweep
    raise AssertionError("oracle did not terminate")


class OnceOnlyRecordIterator:
    def __init__(self, records):
        self.records = iter(records)
        self.read = 0

    def __iter__(self):
        return self

    def __next__(self):
        value = next(self.records)
        self.read += 1
        return value


class CertifiedWarmCommunityTests(unittest.TestCase):
    def assert_complete_trace_parity(self, fixture, check_objective=False):
        module = load_certified_warm_module()
        graph, p, q, labels, gamma = fixture
        prepared = module.certify_warm_graph_state(graph, p, q, labels, gamma)
        low, high, trace = [], [], []
        ls = OnceOnlyRecordIterator(prepared.low)
        hs = OnceOnlyRecordIterator(prepared.high)
        result = module.run_certified_warm_streams(prepared.header, ls, hs,
                                                 low.append, high.append, trace.append)
        actual = dict(prepared.anchors + tuple(low) + tuple(high))
        expected = run_expanded_warm_oracle(graph, labels, gamma, check_objective)
        self.assertEqual((trace, actual, result["sweeps"]), expected)
        self.assertEqual(ls.read + hs.read, len(graph) - len(p) - len(q))
        self.assertEqual(len(actual), len(graph))
        self.assertEqual(result["moves"], prepared.header.crossing)
        self.assertEqual(result["mover_records_read"], prepared.header.b)
        self.assertEqual(result["movers_final_q"], sum(actual[u] == "Q" for u in graph if u not in p | q))
        return prepared, result

    def test_sparse_dense_trace_parity(self):
        for b in (16, 24, 32):
            for seed in range(5):
                with self.subTest(b=b, seed=seed):
                    fixture = make_sparse_warm_fixture(b, seed)
                    self.assertEqual(sum(map(len, fixture[0].values())) // 2, 13 * b // 2)
                    self.assert_complete_trace_parity(fixture, seed == 0)
        self.assert_complete_trace_parity(make_sparse_warm_fixture(dense=True), True)

    def test_irregular_anchor_trace_parity(self):
        for seed in range(10):
            fixture = make_sparse_warm_fixture(seed=seed, perturb=True)
            graph, p = fixture[:2]
            self.assertGreater(len({sum(graph[u].values()) for u in p}), 1)
            self.assert_complete_trace_parity(fixture)

    def test_even_and_empty_crossings(self):
        for crossing in (False, True):
            prepared, result = self.assert_complete_trace_parity(
                make_even_crossing_fixture(crossing), True)
            self.assertEqual(prepared.header.crossing, 2 if crossing else 0)
            self.assertEqual(result["sweeps"], 2 if crossing else 1)

    def test_graph_contract_refusal_cases(self):
        module = load_certified_warm_module()
        for mutation in ("loop", "asymmetric", "weight", "affinity", "matching",
                         "labels", "band", "partition"):
            graph, p, q, labels, gamma = make_sparse_warm_fixture()
            y = sorted(set(graph) - p - q)
            if mutation == "loop":
                graph[y[0]][y[0]] = Fraction(1)
            elif mutation == "asymmetric":
                del graph[next(iter(p))][next(iter(graph[next(iter(p))]))]
            elif mutation == "weight":
                u = next(iter(p))
                v = next(iter(graph[u]))
                graph[u][v] = graph[v][u] = Fraction(-1)
            elif mutation == "affinity":
                add_undirected_weight_edge(graph, y[0], next(iter(p)), 1)
            elif mutation == "matching":
                v = next(v for v in y[1:] if v not in graph[y[0]])
                add_undirected_weight_edge(graph, y[0], v, 1)
            elif mutation == "labels":
                labels[y[0]], labels[y[-1]] = "P", "Q"
            elif mutation == "band":
                gamma = Fraction(3)
            else:
                q.add(next(iter(p)))
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                module.certify_warm_graph_state(graph, p, q, labels, gamma)

    def test_stream_length_refusal_cases(self):
        module = load_certified_warm_module()
        prepared = module.certify_warm_graph_state(*make_sparse_warm_fixture())
        for low, high in ((prepared.low[:-1], prepared.high),
                          (prepared.low, prepared.high + prepared.high[:1])):
            with self.assertRaises(ValueError):
                module.run_certified_warm_streams(prepared.header, iter(low), iter(high),
                                                 lambda row: None, lambda row: None,
                                                 lambda row: None)

    def test_unsafe_anchor_certificate_refusal(self):
        module = load_certified_warm_module()
        graph, p, q, labels, gamma = make_sparse_warm_fixture()
        u = min(p)
        removed = sum(w for v, w in graph[u].items() if v in p)
        for v in list(graph[u]):
            if v in p:
                del graph[u][v]
                del graph[v][u]
        v, z = sorted(p - {u})[:2]
        add_undirected_weight_edge(graph, v, z, removed)
        with self.assertRaisesRegex(ValueError, "anchor stability"):
            module.certify_warm_graph_state(graph, p, q, labels, gamma)

    def test_fresh_destination_certificate_refusal(self):
        module = load_certified_warm_module()
        graph, p, q, labels, gamma = make_even_crossing_fixture(True)
        for anchors in (p, q):
            for u, v in combinations(anchors, 2):
                add_undirected_weight_edge(graph, u, v, 1)
        total = sum(sum(row.values()) for row in graph.values())
        gamma = total / (2 * 9 ** 2)
        with self.assertRaisesRegex(ValueError, "fresh-community"):
            module.certify_warm_graph_state(graph, p, q, labels, gamma)

    def test_cross_anchor_edges_parity(self):
        for seed in range(10):
            graph, p, q, labels, gamma = make_sparse_warm_fixture(seed=seed, perturb=True)
            add_undirected_weight_edge(graph, min(p), max(q), Fraction(seed + 1, 100))
            self.assert_complete_trace_parity((graph, p, q, labels, gamma), seed == 0)

    def test_scalar_band_boundaries_rejected(self):
        module = load_certified_warm_module()
        fixture = make_even_crossing_fixture(True)
        graph, p, q, labels, gamma = fixture
        total = sum(sum(row.values()) for row in graph.values())
        for gamma in (total / 9 ** 2, total / (3 * 9 ** 2)):
            with self.assertRaisesRegex(ValueError, "three-state band"):
                module.certify_warm_graph_state(graph, p, q, labels, gamma)

    def test_full_sorted_label_stream(self):
        module = load_certified_warm_module()
        fixture = make_sparse_warm_fixture(seed=19, perturb=True)
        prepared = module.certify_warm_graph_state(*fixture)
        low, high = [], []
        module.run_certified_warm_streams(prepared.header, iter(prepared.low), iter(prepared.high),
                                         low.append, high.append, lambda row: None)
        expected = run_expanded_warm_oracle(fixture[0], fixture[3], fixture[4])[1]
        self.assertEqual(list(module.merge_certified_label_streams(
            OnceOnlyRecordIterator(prepared.anchors), OnceOnlyRecordIterator(low),
            OnceOnlyRecordIterator(high))), sorted(expected.items()))


if __name__ == "__main__":
    unittest.main()
