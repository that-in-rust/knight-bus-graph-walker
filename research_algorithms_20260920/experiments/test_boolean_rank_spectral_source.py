"""Validate the exact topology premise before using a spectral shortcut."""

from fractions import Fraction
import importlib
from itertools import combinations
import unittest

from test_stream_boolean_rank_solver import ResidentBooleanSourceFixture


class BooleanRankSpectralSourceTests(unittest.TestCase):
    def load_spectral_source_module(self):
        try:
            return importlib.import_module("boolean_rank_spectral_source")
        except ModuleNotFoundError:
            self.fail("canonical spectral eligibility module is absent")

    def build_uniform_source_fixture(self, factors=4, height=2):
        rows = [(i, pair, float(i + 1)) for i, pair in enumerate(
            pair for pair in combinations(range(factors), 2) for _ in range(height))]
        return ResidentBooleanSourceFixture(rows, factors)

    def test_valid_uniform_pair_source(self):
        module = self.load_spectral_source_module()
        for factors in (4, 5, 9):
            for height in (1, 2, 7):
                source = self.build_uniform_source_fixture(factors, height)
                result = module.validate_boolean_spectral_source(source, max_factor_slots=factors)
                self.assertEqual(result["vertices"], height * factors * (factors - 1) // 2)
                self.assertEqual(result["degree"], height * (2 * factors - 3) - 1)
                self.assertEqual(result["spectral_upper"], Fraction(height * (factors - 3) - 1, result["degree"]))
                self.assertEqual(result["snapshot_id"], source.snapshot_id)

    def test_reservation_before_source_scan(self):
        module = self.load_spectral_source_module()
        source = self.build_uniform_source_fixture()
        for capacity in (3, -1, True, 4.0):
            with self.assertRaises(ValueError):
                module.validate_boolean_spectral_source(source, max_factor_slots=capacity)
            self.assertEqual(source.calls, 0)

    def test_invalid_graph_premises_refused(self):
        module = self.load_spectral_source_module()
        for mutation in ("missing", "height", "degree", "order", "singleton", "weight", "vertices"):
            source = self.build_uniform_source_fixture()
            if mutation == "missing":
                source.classes.pop()
            elif mutation == "vertices":
                source.vertex_count += 1
            else:
                index = {"height":2, "degree":3, "order":0, "singleton":1, "weight":4}[mutation]
                row = list(source.classes[0])
                row[index] = {"height":3, "degree":999, "order":1,
                              "singleton":(0,), "weight":Fraction(0)}[mutation]
                source.classes[0] = tuple(row)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                module.validate_boolean_spectral_source(source, max_factor_slots=4)

    def test_ineligible_domain_refused(self):
        module = self.load_spectral_source_module()
        for rows, factors in (([], 4), ([(0, (), 1.)], 4),
                              ([(0, (0,), 1.), (1, (0,), 1.)], 4),
                              ([(0, pair, 1.) for pair in combinations(range(3), 2)], 3)):
            source = ResidentBooleanSourceFixture(rows, factors)
            with self.assertRaises(ValueError):
                module.validate_boolean_spectral_source(source, max_factor_slots=factors)


if __name__ == "__main__":
    unittest.main()
