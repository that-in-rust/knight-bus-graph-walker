"""Original-operator and exact-mass checks for the new correction."""

import importlib.util
import itertools
from fractions import Fraction as F
from pathlib import Path
import random
import struct
import tempfile
import unittest

import probe_native_incidence_pagerank as base
from test_native_incidence_pagerank import solve_expanded_fraction_oracle

MODULE = Path(__file__).with_name("probe_mass_projected_pagerank.py")


class MassProjectedTests(unittest.TestCase):
    def load_required_projected_module(self):
        self.assertTrue(MODULE.exists(), "mass-projected probe is not implemented")
        spec = importlib.util.spec_from_file_location("mass_probe", MODULE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_projected_complete_original_answers(self):
        probe = self.load_required_projected_module()
        subsets = [list(g) for size in range(1, 4) for g in itertools.combinations(range(3), size)]
        cases = [(3, [g for j, g in enumerate(subsets) if mask >> j & 1]) for mask in range(128)]
        rng = random.Random(920621)
        for _ in range(96):
            n = rng.randrange(4, 8)
            cases.append((n, [[i for i in range(n) if rng.random() < .45]
                              for _ in range(rng.randrange(1, 10))]))
        with tempfile.TemporaryDirectory() as folder:
            rows, meta, output = [Path(folder)/name for name in ("rows", "meta", "answer")]
            for n, groups in cases:
                base.build_incidence_row_store(rows, n, groups)
                built = probe.build_factor_correction_records(rows, meta)
                for source in (None, n-1):
                    result = probe.run_projected_rank_solver(rows, meta, output, source, 1e-10, collect_trace=True)
                    answer = struct.unpack(f"<{n}d", output.read_bytes())
                    oracle = solve_expanded_fraction_oracle(n, groups, source)
                    actual = sum(abs(F.from_float(x)-y) for x, y in zip(answer, oracle))
                    self.assertLessEqual(actual, F(result["certificate"]["l1_error_upper"]))
                    self.assertTrue(result["certificate"]["accepted"])
                    self.assertTrue(all(abs(t["candidate_mass"]-1) < 4e-14 for t in result["trace"]))
                    if built["factors"]:
                        self.assertEqual(result["metadata_scans"], 2*result["checks"]+1)
                    self.assertEqual(result["float_iterate_payload_bytes"], 16*built["factors"])
                    for method in ("relaxed", "normalized", "projected_relaxed"):
                        if method == "projected_relaxed":
                            result = probe.run_projected_rank_solver(rows, meta, output, source, 1e-10, relaxed=True)
                        else:
                            result = probe.run_reduced_control_solver(rows, output, source, 1e-10, method)
                        values = struct.unpack(f"<{n}d", output.read_bytes())
                        error = sum(abs(F.from_float(x)-y) for x,y in zip(values,oracle))
                        self.assertLessEqual(error, F(result["certificate"]["l1_error_upper"]))
                        self.assertTrue(result["certificate"]["accepted"])

    def test_exact_projection_energy_identity(self):
        self.load_required_projected_module()
        a = F(17, 20)
        subsets = [list(g) for size in (2, 3) for g in itertools.combinations(range(3), size)]
        for mask in range(1, 16):
            groups = [g for j, g in enumerate(subsets) if mask >> j & 1]
            n, f = 4, len(groups)
            memberships = [[j for j, group in enumerate(groups) if i in group] for i in range(n)]
            q = list(map(len, memberships))
            d = [sum(len(groups[j])-1 for j in ids) for ids in memberships]
            s = list(map(len, groups))
            c = [[F(j == k)-a*sum((1/(F(d[i])+a*q[i]) for i in range(n)
                  if j in memberships[i] and k in memberships[i]), F()) for k in range(f)] for j in range(f)]
            u = [sum(c[j][k]*s[k] for k in range(f)) for j in range(f)]
            self.assertEqual(u, [(1-a)*sum((F(d[i])/(d[i]+a*q[i]) for i in groups[j]), F()) for j in range(f)])
            e_scalar = sum(s[j]*u[j] for j in range(f))
            beta = max(a*(d[i]+q[i])/(d[i]+a*q[i]) for i in range(n) if d[i])
            for source in (None, 0, 3):
                p = [F(1,n) if source is None else F(i == source) for i in range(n)]
                pz = sum(p[i] for i in range(n) if not d[i])
                b = [(1-a)/(1-a*pz)*value for value in p]
                g = [sum((b[i]/(d[i]+a*q[i]) for i in groups[j]), F()) for j in range(f)]
                h = [F((-1)**j*(j+2), 7) for j in range(f)]
                r = [g[j]-sum(c[j][k]*h[k] for k in range(f)) for j in range(f)]
                mass = sum(F(d[i])*(b[i]+a*sum(h[j] for j in memberships[i]))/(d[i]+a*q[i])
                           for i in range(n) if d[i])
                self.assertEqual((1-pz)/(1-a*pz)-mass, a/(1-a)*sum(s[j]*r[j] for j in range(f)))
                offset = (sum(s[j]*g[j] for j in range(f))-sum(u[j]*h[j] for j in range(f)))/e_scalar
                projected = [h[j]+s[j]*offset for j in range(f)]
                self.assertEqual(sum(s[j]*(g[j]-sum(c[j][k]*projected[k] for k in range(f))) for j in range(f)), 0)
                error = [F(j+1,5) for j in range(f)]
                temp = [error[j]-sum(c[j][k]*error[k] for k in range(f)) for j in range(f)]
                corrected = [temp[j]-s[j]*sum(u[k]*temp[k] for k in range(f))/e_scalar for j in range(f)]
                energy_before = sum(error[j]*c[j][k]*error[k] for j in range(f) for k in range(f))
                energy_after = sum(corrected[j]*c[j][k]*corrected[k] for j in range(f) for k in range(f))
                self.assertLessEqual(energy_after, beta*beta*energy_before)

    def test_dense_reuse_all_queries(self):
        probe = self.load_required_projected_module()
        cases = [(5, [[0,1,2], [1,3], [4]]), (3, [[0], [2]]), (4, [[0,1], [2,3]])]
        with tempfile.TemporaryDirectory() as folder:
            rows = Path(folder)/"rows"
            for n, groups in cases:
                base.build_incidence_row_store(rows, n, groups)
                result = probe.run_reused_dense_session(rows, Path(folder), [None, 0, n-1], 1e-10)
                self.assertEqual(result["factorizations"], int(any(len(g) > 1 for g in groups)))
                self.assertEqual(len(result["queries"]), 3)
                for j, source in enumerate((None, 0, n-1)):
                    values = struct.unpack(f"<{n}d", (Path(folder)/f"dense-{j}.bin").read_bytes())
                    oracle = solve_expanded_fraction_oracle(n, groups, source)
                    error = sum(abs(F.from_float(x)-y) for x,y in zip(values,oracle))
                    self.assertLessEqual(error, F(result["queries"][j]["certificate"]["l1_error_upper"]))
                if not any(len(g)>1 for g in groups):
                    self.assertEqual(result["dense_matrix_payload_bytes"], 0)

    def test_metadata_binding_and_validation(self):
        probe = self.load_required_projected_module()
        with tempfile.TemporaryDirectory() as folder:
            rows, other, meta, output = [Path(folder)/name for name in ("rows", "other", "meta", "answer")]
            base.build_incidence_row_store(rows, 3, [[0,1]])
            base.build_incidence_row_store(other, 3, [[1,2]])
            probe.build_factor_correction_records(rows, meta)
            with self.assertRaisesRegex(ValueError, "source identity"):
                probe.run_projected_rank_solver(other, meta, output, None, 1e-10)
            self.assertFalse(output.exists())
            original = meta.read_bytes()
            meta.write_bytes(original[:-1])
            with self.assertRaises(ValueError):
                probe.run_projected_rank_solver(rows, meta, output, None, 1e-10)
            meta.write_bytes(original)
            with self.assertRaisesRegex(ValueError, "iteration budget"):
                probe.run_projected_rank_solver(rows, meta, output, None, 1e-10, maximum_checks=0)
            self.assertFalse(output.exists())

    def test_dense_cap_before_allocation(self):
        probe = self.load_required_projected_module()
        with tempfile.TemporaryDirectory() as folder:
            rows = Path(folder)/"rows"
            base.build_incidence_row_store(rows, 2, [[0,1]]*1025)
            with self.assertRaisesRegex(ValueError, "dense factor cap"):
                probe.run_reused_dense_session(rows, Path(folder), [None], 1e-10)
            self.assertEqual(list(Path(folder).iterdir()), [rows])

    def test_required_cheap_solver_controls(self):
        probe = self.load_required_projected_module()
        self.assertTrue(hasattr(probe, "run_reduced_control_solver"), "cheap controls missing")
        self.assertIn("projected_relaxed", probe.MODES)
        with tempfile.TemporaryDirectory() as folder:
            rows, meta, output = [Path(folder)/name for name in ("rows", "meta", "answer")]
            for n, groups in ((5, [[0,1],[2,3,4]]), (4, [[0,1,2],[1,2]]), (3, [[0],[1],[2]])):
                base.build_incidence_row_store(rows, n, groups)
                probe.build_factor_correction_records(rows, meta)
                for source in (None, 0, n-1):
                    oracle = solve_expanded_fraction_oracle(n, groups, source)
                    for method in ("relaxed", "normalized", "projected_relaxed"):
                        if method == "projected_relaxed":
                            result = probe.run_projected_rank_solver(rows, meta, output, source, 1e-10, relaxed=True)
                        else:
                            result = probe.run_reduced_control_solver(rows, output, source, 1e-10, method)
                        values = struct.unpack(f"<{n}d", output.read_bytes())
                        error = sum(abs(F.from_float(x)-y) for x,y in zip(values,oracle))
                        self.assertLessEqual(error,F(result["certificate"]["l1_error_upper"]))
                        self.assertTrue(result["certificate"]["accepted"])

    def test_exact_isolate_personalization_mass(self):
        probe = self.load_required_projected_module()
        with tempfile.TemporaryDirectory() as folder:
            rows, output = Path(folder)/"rows", Path(folder)/"answer"
            base.build_incidence_row_store(rows, 7, [])
            result = probe.run_reduced_control_solver(rows, output, None, 1e-10, "normalized")
            self.assertTrue(result["certificate"]["accepted"])


if __name__ == "__main__":
    unittest.main()
