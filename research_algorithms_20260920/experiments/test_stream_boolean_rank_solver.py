"""Separate expanded rational oracle; fixture storage is not solver memory."""

from collections import defaultdict
from fractions import Fraction
import importlib
import math
import random
import unittest


class ResidentBooleanSourceFixture:
    snapshot_id = "boolean-fixture-v1"

    def __init__(self, rows, factor_count):
        self.rows = [(v, tuple(sorted(set(groups))), float(weight)) for v, groups, weight in rows]
        self.factor_count, self.vertex_count, self.calls = factor_count, len(rows), 0
        sizes = [sum(f in groups for _, groups, _ in self.rows) for f in range(factor_count)]
        signatures = defaultdict(list)
        for vertex, groups, weight in self.rows:
            signatures[groups].append((vertex, weight))
        self.classes, self.by_class = [], {}
        self.total_weight = sum((Fraction.from_float(weight) for _, _, weight in self.rows), Fraction())
        self.isolate_weight = Fraction()
        self.counts = [0] * factor_count
        for index, groups in enumerate(sorted(signatures)):
            vertices = sorted(signatures[groups])
            h = len(vertices)
            degree = sum(sizes[f] for f in groups) - (h if len(groups) == 2 else 0) - 1 if groups else 0
            weight = sum((Fraction.from_float(w) for _, w in vertices), Fraction())
            self.classes.append((index, groups, h, degree, weight))
            self.by_class[index] = vertices
            if degree:
                for f in groups:
                    self.counts[f] += h
            else:
                self.isolate_weight += weight
        self.class_count = len(self.classes)
        self.active_class_count = sum(record[3] > 0 for record in self.classes)

    def iterate_class_records(self):
        self.calls += 1
        return iter(self.classes)

    def iterate_class_vertex_rows(self, class_id):
        self.calls += 1
        return iter(self.by_class[class_id])

    def iterate_active_factor_counts(self):
        self.calls += 1
        return iter(enumerate(self.counts))


def compute_expanded_rational_pagerank(rows, alpha):
    n = len(rows)
    if not n:
        return {}
    alpha = Fraction.from_float(alpha)
    weights = [Fraction.from_float(float(row[2])) for row in rows]
    total = sum(weights)
    preferences = [weight / total for weight in weights]
    adjacent = [[i != j and bool(set(rows[i][1]) & set(rows[j][1])) for j in range(n)] for i in range(n)]
    degree = [sum(row) for row in adjacent]
    matrix = [[Fraction(i == j) - alpha * (Fraction(adjacent[i][j], degree[j]) if degree[j] else preferences[i])
               for j in range(n)] + [(1 - alpha) * preferences[i]] for i in range(n)]
    for pivot in range(n):
        selected = next(i for i in range(pivot, n) if matrix[i][pivot])
        matrix[pivot], matrix[selected] = matrix[selected], matrix[pivot]
        scale = matrix[pivot][pivot]
        matrix[pivot] = [value / scale for value in matrix[pivot]]
        for i in range(n):
            if i != pivot:
                scale = matrix[i][pivot]
                matrix[i] = [left - scale * right for left, right in zip(matrix[i], matrix[pivot])]
    return {row[0]: matrix[i][-1] for i, row in enumerate(rows)}


class StreamBooleanRankSolverTests(unittest.TestCase):
    def test_centered_highalpha_comparator(self):
        module = self.load_boolean_solver_module()
        rows = [(0, (0,), 1.0), (1, (0,), 1.0)]
        source = ResidentBooleanSourceFixture(rows, 1)
        state = module.solve_boolean_rank_state(source, alpha=math.nextafter(1.0, 0.0),
            method="class-cg", max_factor_slots=1, max_class_slots=1)
        self.assertEqual(dict(module.iterate_boolean_rank_output(source, state)), {0: 0.5, 1: 0.5})

    def load_boolean_solver_module(self):
        try:
            return importlib.import_module("stream_boolean_rank_solver")
        except ModuleNotFoundError as error:
            if error.name == "stream_boolean_rank_solver":
                self.fail("streamed Boolean solver is absent")
            raise

    def compare_complete_boolean_output(self, rows, factors, alpha=0.85):
        module = self.load_boolean_solver_module()
        expected = compute_expanded_rational_pagerank(rows, alpha)
        states = []
        for method in ("factor-cg", "class-cg"):
            source = ResidentBooleanSourceFixture(rows, factors)
            state = module.solve_boolean_rank_state(source, alpha=alpha, method=method,
                max_factor_slots=factors, max_class_slots=source.active_class_count,
                tolerance=1e-13, maximum_iterations=1000)
            output = dict(module.iterate_boolean_rank_output(source, state))
            self.assertEqual(set(output), set(expected))
            self.assertTrue(all(math.isfinite(value) and value >= 0 for value in output.values()))
            error = sum(abs(Fraction.from_float(output[v]) - exact) for v, exact in expected.items())
            self.assertLess(float(error), 1e-10, (rows, method, alpha, error))
            self.assertEqual(state["metrics"]["dimension"], factors if method == "factor-cg" else source.active_class_count)
            self.assertEqual(len(state["factor_scores"]), factors)
            self.assertEqual(state["class_scores"] is None, method == "factor-cg")
            states.append(state)
        return states

    def test_seeded_boolean_full_outputs(self):
        rng = random.Random(20260924)
        for _ in range(100):
            factors, count = rng.randrange(7), rng.randrange(1, 8)
            rows = [(v, rng.sample(range(factors), rng.randrange(min(2, factors) + 1)),
                     rng.choice((0.0, 0.125, 1.0, 3.0))) for v in rng.sample(range(1000), count)]
            rows[0] = (rows[0][0], rows[0][1], 1.0)
            for alpha in (0.0, 0.85, 0.999):
                self.compare_complete_boolean_output(rows, factors, alpha)

    def test_boolean_overlap_and_preferences(self):
        self.compare_complete_boolean_output([(0, (0, 1), 9.0), (1, (0, 1), 1.0),
                                             (2, (0,), 2.0), (3, (1,), 3.0), (9, (), 4.0)], 2)

    def test_quadratic_pair_class_count(self):
        rows = [(100 * a + b, (a, b), float(a + b + 1)) for a in range(8) for b in range(a + 1, 8)]
        module = self.load_boolean_solver_module()
        source = ResidentBooleanSourceFixture(rows, 8)
        self.assertEqual(source.active_class_count, 28)
        state = module.solve_boolean_rank_state(source, alpha=0.85, method="factor-cg",
                                               max_factor_slots=8, max_class_slots=0)
        self.assertEqual(state["metrics"]["dimension"], 8)
        self.assertIsNone(state["class_scores"])
        self.assertEqual(len(list(module.iterate_boolean_rank_output(source, state))), 28)

    def test_single_class_smaller_control(self):
        states = self.compare_complete_boolean_output([(0, (0, 1), 1.0), (1, (0, 1), 0.0)], 2)
        self.assertEqual(states[0]["metrics"]["dimension"], 2)
        self.assertEqual(states[1]["metrics"]["dimension"], 1)

    def test_tiny_alpha_stable_lift(self):
        rows = [(0, (0, 1), 1.0), (1, (0, 1), 0.0)]
        module = self.load_boolean_solver_module()
        states = self.compare_complete_boolean_output(rows, 2, 2.0**-60)
        for state in states:
            values = dict(module.iterate_boolean_rank_output(ResidentBooleanSourceFixture(rows, 2), state))
            self.assertGreater(values[1], 0)
            self.assertLess(abs(values[1] / (2.0**-60) - 1), 1e-12)

    def test_isolates_zero_active_preference(self):
        self.compare_complete_boolean_output([(0, (), 3.0), (1, (0,), 0.0), (2, (0,), 0.0)], 2)
        self.compare_complete_boolean_output([(0, (0, 1), 1.0), (9, (), 0.25)], 3)
        self.compare_complete_boolean_output([], 0)

    def test_preallocation_and_bad_options(self):
        module = self.load_boolean_solver_module()
        source = ResidentBooleanSourceFixture([(0, (0, 1), 1.0), (1, (0, 1), 1.0)], 2)
        for options in (dict(max_factor_slots=1), dict(alpha=1.0), dict(alpha=float("nan")),
                        dict(tolerance=0), dict(maximum_iterations=-1), dict(method="unknown"),
                        dict(method="class-cg", max_class_slots=0)):
            args = dict(alpha=0.85, method="factor-cg", max_factor_slots=2, max_class_slots=1)
            args.update(options)
            before = source.calls
            with self.assertRaises(ValueError):
                module.solve_boolean_rank_state(source, **args)
            self.assertEqual(source.calls, before)

    def test_snapshot_binding_and_iteration_stop(self):
        module = self.load_boolean_solver_module()
        source = ResidentBooleanSourceFixture([(0, (0, 1), 1.0), (1, (0, 1), 0.0)], 2)
        state = module.solve_boolean_rank_state(source, alpha=0.5, method="factor-cg",
            max_factor_slots=2, max_class_slots=0, maximum_iterations=0)
        self.assertFalse(state["metrics"]["converged"])
        with self.assertRaisesRegex(ValueError, "negative"):
            list(module.iterate_boolean_rank_output(source, state))
        source.snapshot_id = "different-source"
        with self.assertRaisesRegex(ValueError, "snapshot"):
            list(module.iterate_boolean_rank_output(source, state))


if __name__ == "__main__":
    unittest.main()
