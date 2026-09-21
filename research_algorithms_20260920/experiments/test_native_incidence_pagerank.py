"""Independent expanded-operator checks for the native incidence experiment."""

import importlib.util
import itertools
from fractions import Fraction
from pathlib import Path
import struct
import tempfile
import unittest

MODULE = Path(__file__).with_name("probe_native_incidence_pagerank.py")


def solve_expanded_fraction_oracle(n, groups, source):
    adjacency = [[0] * n for _ in range(n)]
    for group in groups:
        for i in group:
            for j in group:
                if i != j:
                    adjacency[i][j] += 1
    degrees = [sum(row[j] for row in adjacency) for j in range(n)]
    p = [Fraction(1, n) if source is None else Fraction(i == source)
         for i in range(n)]
    alpha = Fraction(17, 20)
    rows = [[Fraction(i == j) - alpha * (
        Fraction(adjacency[i][j], degrees[j]) if degrees[j] else p[i]
    ) for j in range(n)] + [(1-alpha)*p[i]] for i in range(n)]
    for j in range(n):
        pivot = next(i for i in range(j, n) if rows[i][j])
        rows[j], rows[pivot] = rows[pivot], rows[j]
        divisor = rows[j][j]
        rows[j] = [value/divisor for value in rows[j]]
        for i in range(n):
            if i != j:
                multiplier = rows[i][j]
                rows[i] = [left-multiplier*right
                           for left, right in zip(rows[i], rows[j])]
    return [row[-1] for row in rows]


class NativeIncidenceTests(unittest.TestCase):
    def load_required_probe_module(self):
        self.assertTrue(MODULE.exists(), "native incidence probe is not implemented")
        spec = importlib.util.spec_from_file_location("native_probe", MODULE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_exhaustive_original_operator_answers(self):
        probe = self.load_required_probe_module()
        subsets = [list(group) for size in range(1, 4)
                   for group in itertools.combinations(range(3), size)]
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "rows.bin"
            output = Path(folder) / "answer.bin"
            for mask in range(128):
                groups = [group for j, group in enumerate(subsets) if mask >> j & 1]
                probe.build_incidence_row_store(path, 3, groups)
                for source in (None, 0):
                    oracle = solve_expanded_fraction_oracle(3, groups, source)
                    for method in probe.METHODS:
                        result = probe.run_incidence_rank_solver(path, output, method, source, 1e-10)
                        answer = struct.unpack("<3d", output.read_bytes())
                        actual = sum(abs(Fraction.from_float(x)-y)
                                     for x, y in zip(answer, oracle))
                        bound = Fraction(result["certificate"]["l1_error_upper"])
                        self.assertLessEqual(actual, bound, (mask, source, method))
                        self.assertLessEqual(bound, Fraction("1e-10"))
                        self.assertTrue(result["certificate"]["accepted"])

    def test_certificate_rejects_wrong_output(self):
        probe = self.load_required_probe_module()
        with tempfile.TemporaryDirectory() as folder:
            path, output = Path(folder)/"rows", Path(folder)/"answer"
            probe.build_incidence_row_store(path, 4, [[0, 1], [1, 2], [0]])
            output.write_bytes(struct.pack("<4d", 0.0, 0.0, 0.0, 0.0))
            result = probe.certify_published_rank_output(path, output, None, 1e-10)
            self.assertFalse(result["accepted"])
            self.assertGreaterEqual(Fraction(result["l1_error_upper"]), 1)
            oracle = solve_expanded_fraction_oracle(4, [[0, 1], [1, 2], [0]], 3)
            output.write_bytes(struct.pack("<4d", *(float(x) for x in oracle)))
            result = probe.certify_published_rank_output(path, output, 3, 1e-10)
            self.assertTrue(result["accepted"])
            output.write_bytes(output.read_bytes()[:-1])
            with self.assertRaises(ValueError):
                probe.certify_published_rank_output(path, output, 3, 1e-10)

    def test_builder_preserves_isolate_universe(self):
        probe = self.load_required_probe_module()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"rows"
            meta = probe.build_incidence_row_store(path, 5, [[0], [], [1, 2], [1, 2, 3]])
            self.assertEqual(meta["factors"], 2)
            self.assertEqual(meta["memberships"], 5)
            self.assertEqual(list(probe.iter_incidence_row_records(path)),
                             [(0, ()), (3, (0, 1)), (3, (0, 1)), (2, (1,)), (0, ())])
            with self.assertRaises(ValueError):
                probe.build_incidence_row_store(path, 2, [[0, 0]])
            with self.assertRaises(ValueError):
                probe.build_incidence_row_store(path, 2, [[2]])

    def test_signed_rounded_residual_enclosures(self):
        probe = self.load_required_probe_module()
        groups = [[0, 1, 2], [0, 1], [1, 3]]
        with tempfile.TemporaryDirectory() as folder:
            path, output = Path(folder)/"rows", Path(folder)/"answer"
            probe.build_incidence_row_store(path, 5, groups)
            for source in (None, 4):
                oracle = solve_expanded_fraction_oracle(5, groups, source)
                for scale in (2.0**-1000, 1.0, 2.0**100):
                    answer = [scale, -scale, scale/3, -scale/7, scale/11]
                    output.write_bytes(struct.pack("<5d", *answer))
                    result = probe.certify_published_rank_output(path, output, source, 1e-10)
                    actual = sum(abs(Fraction.from_float(x)-y) for x, y in zip(answer, oracle))
                    self.assertLessEqual(actual, Fraction(result["l1_error_upper"]))

    def test_large_groups_bound_cancellation(self):
        probe = self.load_required_probe_module()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"rows"
            meta = probe.build_incidence_row_store(path, 8, [list(range(5)), list(range(3, 8))])
            self.assertEqual(meta["minimum_group_size"], 5)
            self.assertLessEqual(meta["maximum_q_over_d"], 0.25)
            self.assertLessEqual(meta["beta_upper"], 17/20*5/(4+17/20)+1e-15)

    def test_dense_admission_precedes_allocation(self):
        probe = self.load_required_probe_module()
        with tempfile.TemporaryDirectory() as folder:
            path, output = Path(folder)/"rows", Path(folder)/"answer"
            probe.build_incidence_row_store(path, 2, [[0, 1]]*1025)
            with self.assertRaisesRegex(ValueError, "dense factor cap"):
                probe.run_incidence_rank_solver(path, output, "dense", None, 1e-10)
            self.assertFalse(output.exists())

    def test_source_parser_rejects_duplicate_memberships(self):
        probe = self.load_required_probe_module()
        with tempfile.TemporaryDirectory() as folder:
            source, path = Path(folder)/"u.data", Path(folder)/"rows"
            source.write_text("1\t1\t5\t12\n1\t2\t3\t13\n2\t2\t4\t15\n")
            meta = probe.build_movielens_source_store(source, path)
            self.assertEqual((meta["vertices"], meta["factors"], meta["memberships"]), (2, 1, 2))
            source.write_text("1\t1\t5\t12\n1\t1\t3\t13\n")
            with self.assertRaisesRegex(ValueError, "duplicate"):
                probe.build_movielens_source_store(source, path)

    def test_strong_controls_charge_work(self):
        probe = self.load_required_probe_module()
        self.assertIn("filepower", probe.METHODS)
        self.assertIn("vertexcg", probe.METHODS)
        with tempfile.TemporaryDirectory() as folder:
            path, output = Path(folder)/"rows", Path(folder)/"answer"
            probe.build_incidence_row_store(path, 5, [[0, 1, 2], [1, 3]])
            result = probe.run_incidence_rank_solver(path, output, "filepower", None, 1e-10)
            steps = result["iterations_or_checks"]
            self.assertEqual(result["iterative_score_read_bytes"], 40*steps)
            self.assertEqual(result["logical_write_bytes"], 40*(steps+1))
            self.assertEqual(len(output.read_bytes()), 40)
            self.assertEqual(sorted(p.name for p in Path(folder).iterdir()), ["answer", "rows"])


if __name__ == "__main__":
    unittest.main()
