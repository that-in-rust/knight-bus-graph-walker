"""Exact finite falsifiers for degree-only omitted-edge score envelopes."""

from fractions import Fraction
from itertools import combinations, product
import random
import unittest


def enumerate_canonical_partition_labels(size):
    if size == 0:
        yield ()
        return
    for suffix in product(range(size), repeat=size - 1):
        labels = (0,) + suffix
        if all(labels[i] <= 1 + max(labels[:i]) for i in range(1, size)):
            yield labels


def evaluate_original_modularity_direct(size, edges, labels, gamma):
    mass = sum((weight for _, _, weight in edges), Fraction())
    degrees = [Fraction() for _ in range(size)]
    internal = Fraction()
    for left, right, weight in edges:
        degrees[left] += weight
        degrees[right] += weight
        if labels[left] == labels[right]:
            internal += weight
    volumes = {}
    for node, label in enumerate(labels):
        volumes[label] = volumes.get(label, Fraction()) + degrees[node]
    return internal / mass - gamma * sum(v * v for v in volumes.values()) / (4 * mass * mass)


class ResidualDegreeEnvelopeContracts(unittest.TestCase):
    def test_degrees_hide_quality(self):
        from probe_residual_degree_envelope import build_bounded_quotient_summary, score_bounded_quotient_partition

        cliques = tuple((u, v, Fraction(1)) for u, v in combinations(range(8), 2) if u // 4 == v // 4)
        crossing = tuple((u, v, Fraction(1)) for u in range(4) for v in range(4, 8) if v != u + 4)
        labels = (0, 0, 0, 0, 1, 1, 1, 1)
        first = build_bounded_quotient_summary(8, iter(cliques), 0)
        second = build_bounded_quotient_summary(8, iter(crossing), 0)
        self.assertEqual(first, second)
        self.assertEqual(score_bounded_quotient_partition(first, labels, Fraction(1)), (Fraction(-1, 2), Fraction(1, 2)))
        self.assertEqual(evaluate_original_modularity_direct(8, cliques, labels, Fraction(1)), Fraction(1, 2))
        self.assertEqual(evaluate_original_modularity_direct(8, crossing, labels, Fraction(1)), Fraction(-1, 2))

    def test_sparse_normalization_fails(self):
        from probe_residual_degree_envelope import bound_original_partition_score

        labels = (0, 0, 1, 1, 1, 1)
        bounds = bound_original_partition_score((5, 1, 1, 1, 1, 1), (4, 0, 1, 1, 1, 1), 1, labels, Fraction(1))
        self.assertEqual(bounds, (Fraction(-8, 25), Fraction(-8, 25)))
        wrong = evaluate_original_modularity_direct(6, ((0, 1, Fraction(1)),), labels, Fraction(1))
        self.assertEqual(wrong, 0)
        self.assertNotEqual(wrong, bounds[0])

    def test_summary_respects_budget(self):
        from probe_residual_degree_envelope import build_bounded_quotient_summary

        rng = random.Random(20260921)
        for case in range(180):
            size = 2 + case % 7
            stream = [(rng.randrange(size), rng.randrange(size), Fraction(rng.randrange(1, 9), 3)) for _ in range(30)]
            budget = case % 6
            summary = build_bounded_quotient_summary(size, iter(stream), budget)
            totals = {}
            for left, right, weight in stream:
                if left != right:
                    key = tuple(sorted((left, right)))
                    totals[key] = totals.get(key, 0) + weight
            self.assertLessEqual(len(summary.retained), budget)
            self.assertLessEqual(summary.peak_counters, budget)
            self.assertEqual(summary.input_records, 30)
            for key, weight in summary.retained.items():
                self.assertGreater(weight, 0)
                self.assertLessEqual(weight, totals[key])
            expected = [Fraction() for _ in range(size)]
            for (left, right), weight in totals.items():
                weight -= summary.retained.get((left, right), 0)
                expected[left] += weight
                expected[right] += weight
            self.assertEqual(summary.residual_degrees, tuple(expected))

    def test_scores_contain_original(self):
        from probe_residual_degree_envelope import build_bounded_quotient_summary, score_bounded_quotient_partition

        rng = random.Random(4104)
        comparisons = 0
        for case in range(120):
            size = 2 + case % 6
            edges = [(rng.randrange(size), rng.randrange(size), Fraction(rng.randrange(1, 8), 2)) for _ in range(24)]
            summary = build_bounded_quotient_summary(size, iter(edges), case % 5)
            for _ in range(12):
                labels = tuple(rng.randrange(size) for _ in range(size))
                gamma = Fraction(1 + case % 5, 3)
                lower, upper = score_bounded_quotient_partition(summary, labels, gamma)
                actual = evaluate_original_modularity_direct(size, edges, labels, gamma)
                self.assertLessEqual(lower, actual)
                self.assertLessEqual(actual, upper)
                comparisons += 1
        self.assertEqual(comparisons, 1440)

    def test_full_budget_exact(self):
        from probe_residual_degree_envelope import build_bounded_quotient_summary, score_bounded_quotient_partition

        edges = ((0, 0, Fraction(3)), (0, 1, Fraction(2)), (1, 2, Fraction(5)), (2, 0, Fraction(1)))
        summary = build_bounded_quotient_summary(4, iter(edges), 3)
        self.assertEqual(summary.residual_degrees, (0, 0, 0, 0))
        self.assertEqual(summary.loop_mass, 3)
        for labels in enumerate_canonical_partition_labels(4):
            actual = evaluate_original_modularity_direct(4, edges, labels, Fraction(1))
            self.assertEqual(score_bounded_quotient_partition(summary, labels, Fraction(1)), (actual, actual))

    def test_invalid_summary_inputs(self):
        from probe_residual_degree_envelope import build_bounded_quotient_summary

        for nodes, stream, budget in ((2, (), -1), (-1, (), 0), (2, ((0, 2, 1),), 1), (2, ((0, 1, -1),), 1)):
            with self.assertRaises(ValueError):
                build_bounded_quotient_summary(nodes, iter(stream), budget)

    def test_hub_forces_boundary(self):
        from probe_residual_degree_envelope import bound_residual_internal_mass

        # The degree-five hub forces every edge; counting group mass alone misses this.
        self.assertEqual(
            bound_residual_internal_mass((5, 1, 1, 1, 1, 1), (0, 0, 1, 1, 1, 1)),
            (Fraction(1), Fraction(1)),
        )

    def test_uniform_degrees_ambiguity(self):
        from probe_residual_degree_envelope import bound_residual_internal_mass

        self.assertEqual(bound_residual_internal_mass((1, 1, 1, 1), (0, 0, 1, 1)), (0, 2))
        self.assertEqual(bound_residual_internal_mass((1, 1, 1, 1), (0, 0, 0, 0)), (2, 2))
        self.assertEqual(bound_residual_internal_mass((1, 1, 1, 1), (0, 1, 2, 3)), (0, 0))

    def test_weighted_graphs_containment(self):
        from probe_residual_degree_envelope import bound_residual_internal_mass

        pairs = tuple(combinations(range(4), 2))
        partitions = tuple(enumerate_canonical_partition_labels(4))
        checks = 0
        for weights in product((0, 1, 2), repeat=len(pairs)):
            degrees = [0] * 4
            for (left, right), weight in zip(pairs, weights):
                degrees[left] += weight
                degrees[right] += weight
            for labels in partitions:
                actual = sum(w for (u, v), w in zip(pairs, weights) if labels[u] == labels[v])
                lower, upper = bound_residual_internal_mass(degrees, labels)
                self.assertLessEqual(lower, actual)
                self.assertLessEqual(actual, upper)
                checks += 1
        self.assertEqual(checks, 10935)

    def test_preserve_original_degrees(self):
        from probe_residual_degree_envelope import bound_original_partition_score

        # Retain one hub edge, omit the other four, but keep all original degrees.
        edges = tuple((0, node, Fraction(1)) for node in range(1, 6))
        degrees = (5, 1, 1, 1, 1, 1)
        residual = (4, 0, 1, 1, 1, 1)
        for labels in enumerate_canonical_partition_labels(6):
            known = Fraction(labels[0] == labels[1])
            for gamma in (Fraction(1, 2), Fraction(1), Fraction(3, 2)):
                lower, upper = bound_original_partition_score(degrees, residual, known, labels, gamma)
                actual = evaluate_original_modularity_direct(6, edges, labels, gamma)
                self.assertEqual(lower, actual)
                self.assertEqual(upper, actual)

    def test_invalid_degrees_rejected(self):
        from probe_residual_degree_envelope import bound_residual_internal_mass, bound_original_partition_score

        for degrees, labels in (((3, 1), (0, 1)), ((-1, 1), (0, 1)), ((1, 1), (0,))):
            with self.assertRaises(ValueError):
                bound_residual_internal_mass(degrees, labels)
        with self.assertRaises(ValueError):
            bound_original_partition_score((1, 1), (2, 2), 0, (0, 1), Fraction(1))
        with self.assertRaises(ValueError):
            bound_original_partition_score((1, 1), (0, 0), 2, (0, 1), Fraction(1))
        with self.assertRaises(ValueError):
            bound_original_partition_score((0, 0), (0, 0), 0, (0, 1), Fraction(1))


if __name__ == "__main__":
    unittest.main()
