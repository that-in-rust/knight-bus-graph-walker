"""Tiny exact-oracle tests for the ordinary direct binary64 control."""

from array import array
from decimal import Decimal, localcontext
from fractions import Fraction
import hashlib
import importlib.util
import itertools
import math
from pathlib import Path
import platform
import random
import shutil
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import probe_native_incidence_pagerank as base
import streaming_rank_publication_certificate as publication
from test_native_incidence_pagerank import solve_expanded_fraction_oracle

MODULE = Path(__file__).with_name("binary64_rank_residual_certificate.py")


def compute_expanded_exact_residual(n, groups, source, scores):
    adjacency = [[0]*n for _ in range(n)]
    for group in groups:
        for i in group:
            for j in group:
                if i != j:
                    adjacency[i][j] += 1
    degrees = [sum(row) for row in adjacency]
    x = list(map(Fraction.from_float, scores))
    p = [Fraction(1, n) if source is None else Fraction(i == source) for i in range(n)]
    residual = []
    for i in range(n):
        incoming = sum((x[j]*(Fraction(adjacency[i][j], degrees[j]) if degrees[j] else p[i])
                        for j in range(n)), Fraction())
        residual.append(Fraction(3, 20)*p[i]+Fraction(17, 20)*incoming-x[i])
    return sum(map(abs, residual), Fraction())


class Binary64DirectCertificateTests(unittest.TestCase):
    def load_required_certificate_module(self):
        self.assertTrue(MODULE.exists(), "binary64 direct certificate is not implemented")
        spec = importlib.util.spec_from_file_location("binary64_control", MODULE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def assert_complete_oracle_enclosure(self, module, folder, n, groups, source, scores,
                                         epsilon=1e-10, precision=50):
        store, output = Path(folder)/"rows", Path(folder)/"scores"
        base.build_incidence_row_store(store, n, groups)
        output.write_bytes(struct.pack(f"<{n}d", *scores))
        before = (store.read_bytes(), output.read_bytes())
        result = module.certify_binary64_rank_output(store, output, source, epsilon, precision)
        actual_scores = struct.unpack(f"<{n}d", output.read_bytes())
        oracle = solve_expanded_fraction_oracle(n, groups, source)
        error = sum((abs(Fraction.from_float(x)-y) for x, y in zip(actual_scores, oracle)), Fraction())
        residual = compute_expanded_exact_residual(n, groups, source, actual_scores)
        residual_bound, error_bound = map(Fraction, (result["residual_l1_upper"], result["l1_error_upper"]))
        self.assertLessEqual(residual, residual_bound, (n, groups, source, scores, result))
        self.assertLessEqual(error, error_bound)
        self.assertGreaterEqual(error_bound, residual_bound*Fraction(20, 3))
        self.assertEqual(result["accepted"], error_bound <= Fraction(str(epsilon)))
        self.assertEqual(result["row_scans"], 2)
        self.assertEqual(result["output_scans"], 2)
        self.assertEqual(result["membership_visits"], 2*sum(len(g) for g in groups if len(g) >= 2))
        self.assertEqual(result["retained_factor_payload_bytes"], 16*sum(len(g) >= 2 for g in groups))
        self.assertEqual(result["retained_decimal_vector_values"], 0)
        self.assertEqual(result["logical_read_bytes"], 2*len(before[0])+2*len(before[1]))
        self.assertEqual(result["logical_write_bytes"], 0)
        self.assertEqual(result["source_sha256"], hashlib.sha256(before[0]).hexdigest())
        self.assertEqual(result["output_sha256"], hashlib.sha256(before[1]).hexdigest())
        self.assertEqual(result["certificate_code_sha256"], hashlib.sha256(MODULE.read_bytes()).hexdigest())
        self.assertEqual(result["damping_exact"], "17/20")
        self.assertEqual(before, (store.read_bytes(), output.read_bytes()))
        return result

    def test_complete_tiny_graph_oracles(self):
        module = self.load_required_certificate_module()
        factors = [[0, 1], [0, 2], [1, 2], [0, 1, 2]]
        with tempfile.TemporaryDirectory() as folder:
            for mask in range(16):
                groups = [g for j, g in enumerate(factors) if mask >> j & 1]
                for source in (None, 0, 2):
                    scores = list(map(float, solve_expanded_fraction_oracle(3, groups, source)))
                    result = self.assert_complete_oracle_enclosure(module, folder, 3, groups, source, scores)
                    self.assertTrue(result["accepted"])

    def test_random_perturbed_output_oracles(self):
        module = self.load_required_certificate_module()
        rng = random.Random(2026092101)
        with tempfile.TemporaryDirectory() as folder:
            for _ in range(36):
                n = rng.randrange(2, 7)
                groups = [rng.sample(range(n), rng.randrange(n+1)) for _ in range(rng.randrange(8))]
                source = rng.choice([None, 0, n-1])
                scores = list(map(float, solve_expanded_fraction_oracle(n, groups, source)))
                scale = rng.choice([1e-12, 0.01, 1.0, 1e100])
                scores = [x+scale*rng.uniform(-2, 2) for x in scores]
                self.assert_complete_oracle_enclosure(module, folder, n, groups, source, scores)

    def test_signed_cancellation_extreme_scales(self):
        module = self.load_required_certificate_module()
        groups = [[0, 1, 2], [1, 2, 3], [0, 3], [0, 3]]
        tiny = float.fromhex("0x0.0000000000001p-1022")
        with tempfile.TemporaryDirectory() as folder:
            for source in (None, 0, 4):
                for scale in (tiny, sys.float_info.min, 1.0, 2.0**53, 1e300):
                    scores = [scale, -scale, math.nextafter(scale, math.inf), -scale, -0.0]
                    self.assert_complete_oracle_enclosure(module, folder, 5, groups, source, scores)
            self.assert_complete_oracle_enclosure(module, folder, 3, [[0, 1, 2]], None,
                                                 [2.0**53, 1.0, -(2.0**53)])

    def test_zero_and_isolate_outputs(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as folder:
            for n in (1, 5):
                for source in (None, 0, n-1):
                    scores = list(map(float, solve_expanded_fraction_oracle(n, [], source)))
                    self.assertTrue(self.assert_complete_oracle_enclosure(
                        module, folder, n, [], source, scores)["accepted"])
                    result = self.assert_complete_oracle_enclosure(module, folder, n, [], source, [0.0]*n)
                    self.assertFalse(result["accepted"])
                    self.assertGreaterEqual(Fraction(result["l1_error_upper"]), 1)

    def test_integer_denominator_exact_enclosures(self):
        module = self.load_required_certificate_module()
        tiny = float.fromhex("0x0.0000000000001p-1022")
        for denominator in (1, 3, 2**32-1, 2**53-1, 2**53+1, 2**53+3, 2**64-1):
            dl, dh = module.enclose_integer_binary64_value(denominator)
            self.assertLessEqual(Fraction(dl), denominator)
            self.assertGreaterEqual(Fraction(dh), denominator)
            for value in (-1e300, -1.0, -tiny, -0.0, tiny, 1.0, 1e300):
                low, high = module.divide_interval_positive_bounds(value, value, dl, dh)
                exact = Fraction.from_float(value)/denominator
                self.assertLessEqual(Fraction(low), exact)
                self.assertGreaterEqual(Fraction(high), exact)
        with self.assertRaises(ValueError):
            module.enclose_integer_binary64_value(2**2000)

    def test_signed_interval_kernel_enclosures(self):
        module = self.load_required_certificate_module()
        tiny = float.fromhex("0x0.0000000000001p-1022")
        intervals = [(-3.0, -2.0), (-tiny, tiny), (-2.0, 3.0), (0.0, 0.0), (tiny, tiny), (2.0, 3.0)]
        for lo, hi in intervals:
            for pl, ph in ((0.0, tiny), (0.15, math.nextafter(0.15, math.inf)), (2.0, 3.0)):
                low, high = module.multiply_interval_positive_bounds(lo, hi, pl, ph)
                exacts = [Fraction(a)*Fraction(b) for a, b in itertools.product((lo, hi), (pl, ph))]
                self.assertLessEqual(Fraction(low), min(exacts))
                self.assertGreaterEqual(Fraction(high), max(exacts))
            for dl, dh in ((1.0, 3.0), (2.0, 2.0)):
                low, high = module.divide_interval_positive_bounds(lo, hi, dl, dh)
                exacts = [Fraction(a)/Fraction(b) for a, b in itertools.product((lo, hi), (dl, dh))]
                self.assertLessEqual(Fraction(low), min(exacts))
                self.assertGreaterEqual(Fraction(high), max(exacts))

    def test_nonfinite_and_overflow_refusal(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as folder:
            store, output = Path(folder)/"rows", Path(folder)/"scores"
            base.build_incidence_row_store(store, 2, [[0, 1]])
            for scores in ([math.nan, 0], [math.inf, 0], [-math.inf, 0],
                           [sys.float_info.max, sys.float_info.max], [1e308, -1e308]):
                encoded = struct.pack("<2d", *scores)
                output.write_bytes(encoded)
                with self.assertRaisesRegex(ValueError, "finite|overflow"):
                    module.certify_binary64_rank_output(store, output, None, 1e-10)
                self.assertEqual(output.read_bytes(), encoded)

    def test_invalid_certificate_parameter_refusal(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as folder:
            store, output = Path(folder)/"rows", Path(folder)/"scores"
            base.build_incidence_row_store(store, 2, [[0, 1]])
            output.write_bytes(struct.pack("<2d", 0.5, 0.5))
            for epsilon in (0, -1, math.nan, math.inf, -math.inf, True, "1e-10", Decimal("NaN")):
                with self.assertRaises(ValueError):
                    module.certify_binary64_rank_output(store, output, None, epsilon)
            for precision in (0, 19, 20.0, True):
                with self.assertRaises(ValueError):
                    module.certify_binary64_rank_output(store, output, None, 1e-10, precision)
            for source in (-1, 2, 0.0, True, "0"):
                with self.assertRaises(ValueError):
                    module.certify_binary64_rank_output(store, output, source, 1e-10)

    def test_malformed_source_framing_refused(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as folder:
            store, output = Path(folder)/"rows", Path(folder)/"scores"
            base.build_incidence_row_store(store, 3, [[0, 1], [0, 1, 2]])
            good = store.read_bytes()
            output.write_bytes(struct.pack("<3d", 0.3, 0.3, 0.4))
            header = base.HEADER.size
            cases = [good[:10], b"BADMAGIC"+good[8:], good[:-1], good+b"x",
                     base.HEADER.pack(base.MAGIC, 0, 2, 5)+good[header:],
                     base.HEADER.pack(base.MAGIC, 3, 2**32, 5)+good[header:],
                     base.HEADER.pack(base.MAGIC, 3, 2, 6)+good[header:],
                     good[:header]+base.ROW.pack(0, 2)+good[header+base.ROW.size:],
                     good[:header]+base.ROW.pack(3, 3)+good[header+base.ROW.size:],
                     good[:header+base.ROW.size]+struct.pack("<II", 0, 0)+good[header+base.ROW.size+8:],
                     good[:header+base.ROW.size]+struct.pack("<II", 1, 0)+good[header+base.ROW.size+8:],
                     good[:header+base.ROW.size]+struct.pack("<II", 0, 2)+good[header+base.ROW.size+8:]]
            for malformed in cases:
                store.write_bytes(malformed)
                with self.subTest(malformed=malformed.hex()):
                    with self.assertRaises(ValueError):
                        module.certify_binary64_rank_output(store, output, None, 1e-10)

    def test_malformed_output_framing_refused(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as folder:
            store, output = Path(folder)/"rows", Path(folder)/"scores"
            base.build_incidence_row_store(store, 2, [[0, 1]])
            good = struct.pack("<2d", 0.5, 0.5)
            for raw in (b"", good[:-1], good+b"x", good+struct.pack("<d", 0)):
                output.write_bytes(raw)
                with self.assertRaises(ValueError):
                    module.certify_binary64_rank_output(store, output, None, 1e-10)

    def test_changed_inputs_between_scans(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as folder:
            store, output = Path(folder)/"rows", Path(folder)/"scores"
            base.build_incidence_row_store(store, 3, [[0, 1], [1, 2]])
            source_bytes = store.read_bytes()
            score_bytes = struct.pack("<3d", 0.25, 0.5, 0.25)
            real_open = Path.open
            for target in (store, output):
                for change in ("finite", "truncate", "trailing", "nonfinite"):
                    store.write_bytes(source_bytes)
                    output.write_bytes(score_bytes)
                    opens = 0

                    def replace_input_second_pass(path, *args, **kwargs):
                        nonlocal opens
                        if path == output and args and args[0] == "rb":
                            opens += 1
                            if opens == 2:
                                raw = score_bytes if target == output else source_bytes
                                if change == "truncate":
                                    raw = raw[:-1]
                                elif change == "trailing":
                                    raw += b"x"
                                elif target == output:
                                    raw = struct.pack("<d", math.nan if change == "nonfinite" else 0.1)+raw[8:]
                                else:
                                    offset = base.HEADER.size
                                    raw = raw[:offset]+struct.pack("<Q", 2)+raw[offset+8:]
                                with real_open(target, "wb") as handle:
                                    handle.write(raw)
                        return real_open(path, *args, **kwargs)

                    with patch.object(Path, "open", replace_input_second_pass):
                        with self.assertRaises(ValueError):
                            module.certify_binary64_rank_output(store, output, None, 1e-10)

    def test_only_two_factor_arrays(self):
        module = self.load_required_certificate_module()
        allocations = []

        def record_endpoint_array_allocation(*args, **kwargs):
            value = array(*args, **kwargs)
            allocations.append(value)
            return value

        with tempfile.TemporaryDirectory() as folder:
            with patch.object(module, "array", record_endpoint_array_allocation):
                self.assert_complete_oracle_enclosure(module, folder, 3, [[0, 1], [1, 2]], None,
                                                     [0.25, 0.5, 0.25])
        self.assertEqual([(a.typecode, a.itemsize, len(a)) for a in allocations], [("d", 8, 2), ("d", 8, 2)])

    def test_decimal_context_doesnt_leak(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as folder, localcontext() as context:
            context.prec = 3
            context.clear_flags()
            before = context.copy()
            for precision in (20, 50, 80):
                result = self.assert_complete_oracle_enclosure(module, folder, 3, [[0, 1]], None,
                                                             [0.25, 0.5, 0.25], precision=precision)
                self.assertEqual(result["decimal_precision"], precision)
            self.assertEqual(context.prec, before.prec)
            self.assertEqual(context.flags, before.flags)

    def test_tolerance_changes_only_acceptance(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as folder:
            store, output = Path(folder)/"rows", Path(folder)/"scores"
            base.build_incidence_row_store(store, 2, [[0, 1]])
            output.write_bytes(struct.pack("<2d", 0.5, 0.5))
            first = module.certify_binary64_rank_output(store, output, None, 1e-10)
            bound = Decimal(first["l1_error_upper"])
            with localcontext() as context:
                context.prec = 80
                smaller = context.next_minus(bound)
            for epsilon, accepted in ((bound, True), (smaller, False)):
                result = module.certify_binary64_rank_output(store, output, None, epsilon)
                self.assertEqual(result["accepted"], accepted)
                self.assertEqual(result["l1_error_upper"], first["l1_error_upper"])
                self.assertEqual(result["residual_l1_upper"], first["residual_l1_upper"])

    def test_manifest_and_publication_compatibility(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as folder:
            store, metadata, output = (Path(folder)/name for name in ("rows", "manifest", "scores"))
            base.build_incidence_row_store(store, 3, [[0, 1]])
            manifest = publication.build_certificate_source_manifest(store, metadata)
            oracle = solve_expanded_fraction_oracle(3, [[0, 1]], None)
            published = publication.publish_streaming_rank_certificate(
                store, metadata, manifest, array("d", [float(oracle[0]+oracle[1])]), output, None, 1e-10)
            self.assertTrue(published["accepted"])
            direct = module.certify_binary64_rank_output(store, output, None, 1e-10)
            frozen = base.certify_published_rank_output(store, output, None, 1e-10)
            self.assertTrue(direct["accepted"])
            self.assertTrue(frozen["accepted"])
            self.assertEqual(direct["output_sha256"], published["output_sha256"])

    def test_live_rounding_modes_refused(self):
        self.load_required_certificate_module()
        machine = platform.machine().lower()
        if machine in ("arm64", "aarch64") and sys.platform in ("darwin", "linux"):
            modes = [0x400000, 0x800000, 0xc00000]
        elif machine in ("x86_64", "amd64") and sys.platform in ("darwin", "linux"):
            modes = [0x400, 0x800, 0xc00]
        else:
            self.skipTest("no known fenv constants for this platform")
        program = f'''
import ctypes
import binary64_rank_residual_certificate as module
lib = ctypes.CDLL(None)
old = lib.fegetround()
module.validate_binary64_runtime_contract()
try:
    for mode in {modes!r}:
        assert lib.fesetround(mode) == 0
        try:
            module.validate_binary64_runtime_contract()
        except ValueError as error:
            assert 'round' in str(error)
        else:
            raise AssertionError('non-nearest mode admitted')
finally:
    assert lib.fesetround(old) == 0
'''
        result = subprocess.run([sys.executable, "-B", "-c", program], cwd=MODULE.parent,
                                capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_live_gradual_underflow_required(self):
        self.load_required_certificate_module()
        compiler = shutil.which("cc")
        if compiler is None or platform.machine().lower() not in ("arm64", "aarch64", "x86_64", "amd64"):
            self.skipTest("underflow child requires cc and ARM64 or x86-64")
        program = '''
import ctypes, pathlib, subprocess, tempfile
import binary64_rank_residual_certificate as module
code = r"""
void enable_flush_mode(unsigned mode) {
#if defined(__aarch64__) || defined(__arm64__)
    unsigned long fpcr;
    __asm__ volatile("mrs %0, fpcr" : "=r"(fpcr));
    fpcr |= 1UL << 24;
    __asm__ volatile("msr fpcr, %0" : : "r"(fpcr));
#elif defined(__x86_64__)
    unsigned csr;
    __asm__ volatile("stmxcsr %0" : "=m"(csr));
    csr &= ~((1U << 6) | (1U << 15));
    csr |= mode ? (1U << 6) : (1U << 15);
    __asm__ volatile("ldmxcsr %0" : : "m"(csr));
#endif
}
"""
with tempfile.TemporaryDirectory() as folder:
    source = pathlib.Path(folder)/'flush.c'
    library = pathlib.Path(folder)/'flush.so'
    source.write_text(code)
    subprocess.run(['cc', '-shared', '-fPIC', str(source), '-o', str(library)], check=True,
                   capture_output=True, timeout=15)
    lib = ctypes.CDLL(str(library))
    module.validate_binary64_runtime_contract()
    for mode in (0, 1):
        lib.enable_flush_mode(mode)
        try:
            module.validate_binary64_runtime_contract()
        except ValueError as error:
            assert 'underflow' in str(error)
        else:
            raise AssertionError('flush-to-zero/denormals-are-zero admitted')
'''
        result = subprocess.run([sys.executable, "-B", "-c", program], cwd=MODULE.parent,
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
