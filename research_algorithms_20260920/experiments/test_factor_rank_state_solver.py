"""Actual-state parity and ownership contracts for the A01 CG extraction."""

from array import array
from contextlib import contextmanager
from fractions import Fraction
import hashlib
import importlib.util
import itertools
import math
from pathlib import Path
import struct
import sys
import tempfile
import unittest
from unittest.mock import patch
import weakref

EXPERIMENTS = Path(__file__).resolve().parent
MODULE = EXPERIMENTS / "factor_rank_state_solver.py"
with patch.object(sys, "path", [str(EXPERIMENTS), *sys.path]):
    import probe_native_incidence_pagerank as base
    from test_native_incidence_pagerank import solve_expanded_fraction_oracle


def load_required_solver_module():
    spec = importlib.util.spec_from_file_location("factor_state_solver", MODULE)
    module = importlib.util.module_from_spec(spec)
    with patch.object(sys, "path", [str(EXPERIMENTS), *sys.path]):
        spec.loader.exec_module(module)
    return module


def capture_frozen_solver_state(path, output, source, epsilon, maximum_iterations=20000):
    captured = []
    previous = sys.getprofile()

    def record_frozen_return_state(frame, event, result):
        if event == "return" and frame.f_code is base.run_incidence_rank_solver.__code__:
            if result is not None:
                captured.append(array("d", frame.f_locals["h"]))

    try:
        sys.setprofile(record_frozen_return_state)
        metrics = base.run_incidence_rank_solver(
            path, output, "cg", source, epsilon, maximum_iterations)
    finally:
        sys.setprofile(previous)
    return captured[0], metrics


def reconstruct_original_float_bytes(path, h, source):
    n, _, _ = base.read_incidence_store_header(path)
    rows = list(base.iter_incidence_row_records(path))
    pz = sum(degree == 0 for degree, _ in rows)/n if source is None else float(rows[source][0] == 0)
    bscale = (1-base.ALPHA)/(1-base.ALPHA*pz)
    output = bytearray()
    for i, (degree, ids) in enumerate(rows):
        b = bscale*base.select_personalization_node_value(i, n, source)
        value = degree*(b+base.ALPHA*math.fsum(h[j] for j in ids))/(degree+base.ALPHA*len(ids)) if degree else b
        output.extend(base.DOUBLE.pack(value))
    return bytes(output)


@contextmanager
def track_solver_array_ownership(module):
    references, peaks = [], []
    original_zero = base.create_zero_float_vector

    def record_allocated_array_reference(value):
        references.append(weakref.ref(value))
        peaks.append(sum(len(item())*item().itemsize for item in references if item() is not None))
        return value

    def create_tracked_zero_vector(count):
        return record_allocated_array_reference(original_zero(count))

    def create_tracked_array_copy(*args):
        return record_allocated_array_reference(array(*args))

    with patch.object(base, "create_zero_float_vector", create_tracked_zero_vector), \
            patch.object(module, "array", create_tracked_array_copy):
        yield references, peaks


class FactorStateSolverTests(unittest.TestCase):
    def load_existing_solver_module(self):
        self.assertTrue(MODULE.exists(), "actual CG factor-state exporter is not implemented")
        return load_required_solver_module()

    def assert_complete_frozen_parity(self, module, path, output, n, groups, source, epsilon=1e-10):
        expected_h, frozen = capture_frozen_solver_state(path, output, source, epsilon)
        h, metrics = module.solve_incidence_factor_state(path, source, epsilon)
        self.assertIsInstance(h, array)
        self.assertEqual(h.typecode, "d")
        self.assertEqual(h.tobytes(), expected_h.tobytes())
        actual_bytes = reconstruct_original_float_bytes(path, h, source)
        self.assertEqual(actual_bytes, output.read_bytes())
        self.assertEqual(hashlib.sha256(actual_bytes).hexdigest(), frozen["output_sha256"])
        canonical_h = b"".join(base.DOUBLE.pack(value) for value in h)
        self.assertEqual(metrics["state_sha256"], hashlib.sha256(canonical_h).hexdigest())
        self.assertEqual(metrics["state_bytes"], 8*len(h))
        self.assertEqual(metrics["iterations_or_checks"], frozen["iterations_or_checks"])
        self.assertEqual(metrics["screen_bound_uncertified"], frozen["screen_bound_uncertified"])
        scans = metrics["noncertificate_row_scans"]
        self.assertEqual(scans, frozen["noncertificate_row_scans"]-1)
        self.assertEqual(scans, metrics["iterations_or_checks"]+2)
        z = base.read_incidence_store_header(path)[2]
        self.assertEqual(metrics["noncertificate_membership_visits"], scans*z)
        self.assertEqual(metrics["operator_membership_visits"], 2*(scans-1)*z)
        self.assertIn("total_ms", metrics)
        self.assertGreaterEqual(metrics["total_ms"], 0)
        self.assertTrue(math.isfinite(metrics["total_ms"]))
        self.assertEqual(metrics["row_scans"], scans)
        self.assertEqual(metrics["logical_read_bytes"], base.HEADER.size+scans*path.stat().st_size)
        self.assertEqual(metrics["selected_array_phase_peak_bytes"], 6*8*len(h))
        self.assertNotIn("accepted", metrics)
        self.assertNotIn("certificate", metrics)
        self.assertEqual([key for key in metrics if key.endswith(("_ms", "_seconds"))], ["total_ms"])
        answer = struct.unpack(f"<{n}d", actual_bytes)
        oracle = solve_expanded_fraction_oracle(n, groups, source)
        error = sum(abs(Fraction.from_float(value)-exact) for value, exact in zip(answer, oracle))
        self.assertLessEqual(error, Fraction(frozen["certificate"]["l1_error_upper"]))
        self.assertLessEqual(error, Fraction(str(epsilon)))

    def test_exhaustive_tiny_state_parity(self):
        module = self.load_existing_solver_module()
        subsets = [list(group) for size in range(1, 4)
                   for group in itertools.combinations(range(3), size)]
        with tempfile.TemporaryDirectory() as folder:
            path, output = Path(folder)/"rows", Path(folder)/"answer"
            for mask in range(128):
                groups = [group for j, group in enumerate(subsets) if mask >> j & 1]
                base.build_incidence_row_store(path, 3, groups)
                for source in (None, 0, 1, 2):
                    with self.subTest(mask=mask, source=source):
                        self.assert_complete_frozen_parity(module, path, output, 3, groups, source)

    def test_overlap_duplicates_isolate_parity(self):
        module = self.load_existing_solver_module()
        groups = [[0, 1, 2], [0, 1], [0, 1], [1, 3], [4], []]
        with tempfile.TemporaryDirectory() as folder:
            path, output = Path(folder)/"rows", Path(folder)/"answer"
            base.build_incidence_row_store(path, 5, groups)
            for source in (None, 0, 4):
                self.assert_complete_frozen_parity(module, path, output, 5, groups, source)

    def test_frozen_fixture_hash_receipts(self):
        module = self.load_existing_solver_module()
        groups = [[0, 1, 2], [0, 1], [0, 1], [1, 3], [4], []]
        expected = (
            (None, 3, "8fd6732fc8071ccc42266fb96fbe6c95d8072896db3d6b019e0caac9b5c3b295",
             "540f98241019b541ea63a79b471f2eff21781e109d708a830991b8eefc916618"),
            (0, 3, "ea225c6b08d6a5da8bce49468d082cc45c41efad89140b502a8db3796816d153",
             "d64dbd0f41ade5f652872cd89f03b528da5a2ffc0189b26853266f6ba2489c69"),
            (4, 0, "66687aadf862bd776c8fc18b8e9f8e20089714856ee233b3902a591d0d5f2925",
             "6841360c725e8cfcad4fab032784e071ee0bbf981e301e3982780c9661c34094"),
        )
        with tempfile.TemporaryDirectory() as folder:
            path, output = Path(folder)/"rows", Path(folder)/"answer"
            base.build_incidence_row_store(path, 5, groups)
            for source, iterations, state_hash, output_hash in expected:
                frozen_h, frozen = capture_frozen_solver_state(path, output, source, 1e-10)
                h, metrics = module.solve_incidence_factor_state(path, source, 1e-10, iterations)
                self.assertEqual(h.tobytes(), frozen_h.tobytes())
                self.assertEqual(metrics["iterations_or_checks"], iterations)
                self.assertEqual(metrics["state_sha256"], state_hash)
                self.assertEqual(frozen["output_sha256"], output_hash)
                self.assertEqual(hashlib.sha256(reconstruct_original_float_bytes(path, h, source)).hexdigest(), output_hash)

    def test_zero_screen_not_certificate(self):
        module = self.load_existing_solver_module()
        with tempfile.TemporaryDirectory() as folder:
            path, output = Path(folder)/"rows", Path(folder)/"answer"
            base.build_incidence_row_store(path, 2, [[0, 1]])
            h, metrics = module.solve_incidence_factor_state(path, None, 1e-30)
            self.assertEqual(metrics["screen_bound_uncertified"], 0)
            output.write_bytes(reconstruct_original_float_bytes(path, h, None))
            certificate = base.certify_published_rank_output(path, output, None, 1e-30)
            self.assertFalse(certificate["accepted"])
            oracle = solve_expanded_fraction_oracle(2, [[0, 1]], None)
            error = sum(abs(Fraction.from_float(value)-exact)
                        for value, exact in zip(struct.unpack("<2d", output.read_bytes()), oracle))
            self.assertGreater(error, Fraction("1e-30"))
            self.assertNotIn("accepted", metrics)
            self.assertNotIn("certificate", metrics)

    def test_initial_state_not_remapped(self):
        module = self.load_existing_solver_module()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"rows"
            base.build_incidence_row_store(path, 5, [[0, 1, 2], [0, 1], [1, 3]])
            expected = base.create_zero_float_vector(3)
            for i, (degree, ids) in enumerate(base.iter_incidence_row_records(path)):
                if degree:
                    for j in ids:
                        expected[j] += (1/5)/degree
            h, metrics = module.solve_incidence_factor_state(path, None, 100.0, maximum_iterations=0)
            self.assertEqual(h.tobytes(), expected.tobytes())
            self.assertEqual(metrics["iterations_or_checks"], 0)
            scores = struct.unpack("<5d", reconstruct_original_float_bytes(path, h, None))
            mapped = base.create_zero_float_vector(3)
            for value, (degree, ids) in zip(scores, base.iter_incidence_row_records(path)):
                if degree:
                    for j in ids:
                        mapped[j] += value/degree
            self.assertNotEqual(h.tobytes(), mapped.tobytes())

    def test_zero_factors_zero_budget(self):
        module = self.load_existing_solver_module()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"rows"
            base.build_incidence_row_store(path, 4, [[0], [], [3]])
            for source in (None, 0, 3):
                h, metrics = module.solve_incidence_factor_state(path, source, 1e-10, 0, 0)
                self.assertEqual(h, array("d"))
                self.assertEqual(metrics["state_sha256"], hashlib.sha256(b"").hexdigest())
                self.assertEqual(metrics["selected_array_phase_peak_bytes"], 0)
                self.assertEqual(metrics["iterations_or_checks"], 0)
                self.assertEqual(metrics["screen_bound_uncertified"], 0)
                self.assertEqual(metrics["noncertificate_row_scans"], 2)
                self.assertEqual(metrics["noncertificate_membership_visits"], 0)

    def test_parameters_refuse_before_allocation(self):
        module = self.load_existing_solver_module()
        invalid = [{"epsilon": value} for value in (0, -1, math.nan, math.inf, -math.inf, True, "1e-10", 10**1000)]
        invalid += [{"source": value} for value in (-1, 3, True, 1.0, math.nan, math.inf)]
        invalid += [{"maximum_iterations": value} for value in (-1, 1.0, True, math.nan, math.inf)]
        invalid += [{"maximum_factor_bytes": value} for value in (-1, 1.0, True, math.nan, math.inf, 47)]
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"rows"
            base.build_incidence_row_store(path, 3, [[0, 1]])
            for override in invalid:
                arguments = dict(source=None, epsilon=1e-10)
                arguments.update(override)
                with self.subTest(override=override), \
                        patch.object(base, "create_zero_float_vector", side_effect=AssertionError("allocated before admission")), \
                        patch.object(module, "array", side_effect=AssertionError("allocated before admission")), \
                        patch.object(base, "iter_incidence_row_records", side_effect=AssertionError("scanned before admission")):
                    with self.assertRaises(ValueError):
                        module.solve_incidence_factor_state(path, **arguments)

    def test_selected_array_budget_boundary(self):
        module = self.load_existing_solver_module()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"rows"
            base.build_incidence_row_store(path, 5, [[0, 1, 2], [0, 1], [1, 3]])
            original = path.read_bytes()
            with track_solver_array_ownership(module) as (references, peaks):
                h, metrics = module.solve_incidence_factor_state(path, None, 1e-10, maximum_factor_bytes=144)
            self.assertEqual(max(peaks), 144)
            self.assertEqual(metrics["selected_array_phase_peak_bytes"], 144)
            self.assertEqual(sum(item() is not None for item in references), 1)
            self.assertTrue(any(item() is h for item in references))
            self.assertEqual(sorted(item.name for item in Path(folder).iterdir()), ["rows"])
            self.assertEqual(path.read_bytes(), original)
            first = h.tobytes()
            another, _ = module.solve_incidence_factor_state(path, None, 1e-10)
            another[0] = -1
            self.assertEqual(h.tobytes(), first)

    def test_huge_header_budget_refusal(self):
        module = self.load_existing_solver_module()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"rows"
            path.write_bytes(base.HEADER.pack(base.MAGIC, 2, 2**32-1, 0))
            with patch.object(base, "create_zero_float_vector", side_effect=AssertionError("allocated")), \
                    patch.object(module, "array", side_effect=AssertionError("allocated")), \
                    patch.object(base, "iter_incidence_row_records", side_effect=AssertionError("scanned")):
                with self.assertRaisesRegex(ValueError, "budget exceeded before allocation"):
                    module.solve_incidence_factor_state(path, None, 1e-10)

    def test_iteration_budget_refuses_state(self):
        module = self.load_existing_solver_module()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"rows"
            base.build_incidence_row_store(path, 5, [[0, 1, 2], [0, 1], [1, 3]])
            with self.assertRaisesRegex(ValueError, "CG iteration budget exhausted"):
                module.solve_incidence_factor_state(path, None, 1e-10, maximum_iterations=0)

    def test_exhaustion_precedes_next_step(self):
        module = self.load_existing_solver_module()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"rows"
            base.build_incidence_row_store(path, 5, [[0, 1, 2], [0, 1], [1, 3]])
            for limit in (0, 1, 2):
                with self.subTest(limit=limit), patch.object(base, "iter_incidence_row_records", wraps=base.iter_incidence_row_records) as rows:
                    with self.assertRaisesRegex(ValueError, "CG iteration budget exhausted"):
                        module.solve_incidence_factor_state(path, None, 1e-10, maximum_iterations=limit)
                    self.assertEqual(rows.call_count, limit+2, "started a CG step beyond the budget")

    def test_bad_curvature_refuses_state(self):
        module = self.load_existing_solver_module()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"rows"
            # Deliberately violate upstream degree validation to exercise CG's guard.
            path.write_bytes(base.HEADER.pack(base.MAGIC, 2, 2, 4)
                             + (base.ROW.pack(1, 2)+struct.pack("<2I", 0, 1))*2)
            with self.assertRaisesRegex(ValueError, "CG lost positive curvature"):
                module.solve_incidence_factor_state(path, None, 1e-10)

    def test_nonfinite_work_refuses_state(self):
        module = self.load_existing_solver_module()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"rows"
            base.build_incidence_row_store(path, 2, [[0, 1]])
            with patch.object(base, "ALPHA", math.nan):
                with self.assertRaisesRegex(ValueError, "nonfinite"):
                    module.solve_incidence_factor_state(path, None, 1e-10)

    def test_malformed_source_refuses_state(self):
        module = self.load_existing_solver_module()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"rows"
            base.build_incidence_row_store(path, 2, [[0, 1]])
            original = path.read_bytes()
            for damaged in (original[:-1], original+b"x", original[:base.HEADER.size]):
                path.write_bytes(damaged)
                with self.assertRaises(ValueError):
                    module.solve_incidence_factor_state(path, None, 1e-10)

    def test_cancellation_releases_owned_arrays(self):
        module = self.load_existing_solver_module()
        original_rows = base.iter_incidence_row_records
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"rows"
            base.build_incidence_row_store(path, 5, [[0, 1, 2], [0, 1], [1, 3]])
            for interruption in (1, 2, 3):
                calls, closed = [], []
                retained = None

                def interrupt_selected_row_scan(store):
                    calls.append(1)
                    iterator = original_rows(store)
                    try:
                        for row in iterator:
                            yield row
                            if len(calls) == interruption:
                                raise KeyboardInterrupt("cancelled CG")
                    finally:
                        iterator.close()
                        closed.append(1)

                with self.subTest(scan=interruption), track_solver_array_ownership(module) as (references, _), \
                        patch.object(base, "iter_incidence_row_records", interrupt_selected_row_scan):
                    try:
                        module.solve_incidence_factor_state(path, None, 1e-10)
                    except KeyboardInterrupt as error:
                        retained = error
                    self.assertIsNotNone(retained)
                    self.assertEqual(len(closed), interruption)
                    self.assertTrue(all(item() is None for item in references), "traceback retained CG arrays")

    def test_refusal_releases_owned_arrays(self):
        module = self.load_existing_solver_module()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"rows"
            base.build_incidence_row_store(path, 5, [[0, 1, 2], [0, 1], [1, 3]])
            retained = None
            with track_solver_array_ownership(module) as (references, _):
                try:
                    module.solve_incidence_factor_state(path, None, 1e-10, maximum_iterations=0)
                except ValueError as error:
                    retained = error
            self.assertIsNotNone(retained)
            self.assertTrue(all(item() is None for item in references), "traceback retained refused state")

    def test_hashing_owns_only_state(self):
        module = self.load_existing_solver_module()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"rows"
            base.build_incidence_row_store(path, 5, [[0, 1, 2], [0, 1], [1, 3]])
            with track_solver_array_ownership(module) as (references, _):
                def interrupt_after_helper_return():
                    self.assertEqual(sum(item() is not None for item in references), 1)
                    raise KeyboardInterrupt("cancelled while hashing actual state")

                retained = None
                with patch.object(module.hashlib, "sha256", interrupt_after_helper_return):
                    try:
                        module.solve_incidence_factor_state(path, None, 1e-10)
                    except KeyboardInterrupt as error:
                        retained = error
                self.assertIsNotNone(retained)
                self.assertTrue(all(item() is None for item in references))

    def test_timer_includes_helper_hashing(self):
        module = self.load_existing_solver_module()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"rows"
            base.build_incidence_row_store(path, 2, [[0, 1]])
            clock = [10.0]
            original_solve = module._run_incidence_cg_state
            original_hash = module.hashlib.sha256

            def advance_clock_during_solving(*args):
                result = original_solve(*args)
                clock[0] += 2
                return result

            def advance_clock_during_hashing():
                clock[0] += 3
                return original_hash()

            with patch.object(module.time, "perf_counter", lambda: clock[0]), \
                    patch.object(module, "_run_incidence_cg_state", advance_clock_during_solving), \
                    patch.object(module.hashlib, "sha256", advance_clock_during_hashing):
                _, metrics = module.solve_incidence_factor_state(path, None, 1e-10)
            self.assertEqual(metrics["total_ms"], 5000)


if __name__ == "__main__":
    unittest.main()
