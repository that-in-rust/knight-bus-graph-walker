"""Exact accuracy/frontier witnesses; no numerical-summation novelty claim."""

from array import array
from decimal import Context, Decimal, ROUND_FLOOR, ROUND_CEILING
from fractions import Fraction
import importlib.util
import itertools
import math
from pathlib import Path
import platform
import random
import struct
import subprocess
import sys
import tempfile
import unittest

import probe_native_incidence_pagerank as base
import streaming_rank_publication_certificate as frozen
import precision_placed_rank_certificate as placed
from test_streaming_rank_certificate import StreamingCertificateTests, derive_exact_factor_state

MODULE = Path(__file__).with_name("accuracy_frontier_rank_certificate.py")


class AccuracyFrontierCertificateTests(StreamingCertificateTests):
    def load_required_certificate_module(self):
        self.assertTrue(MODULE.exists(), "accuracy-frontier publisher is not implemented")
        spec = importlib.util.spec_from_file_location("accuracy_certificate", MODULE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_exact_accumulator_mixed_values(self):
        module = self.load_required_certificate_module()
        tiny = float.fromhex("0x0.0000000000001p-1022")
        cases = [[], [0.0], [tiny, -tiny, tiny], [1.0, 2**-53, -1.0],
                 [sys.float_info.max, -sys.float_info.max, tiny], [sys.float_info.max]*2]
        rng = random.Random(921881)
        for count in (1, 2, 3, 9, 64):
            for _ in range(20):
                bits = [(rng.randrange(2) << 63) | (rng.randrange(2047) << 52) | rng.getrandbits(52)
                        for _ in range(count)]
                cases.append([struct.unpack("<d", struct.pack("<Q", b))[0] for b in bits])
        for values in cases:
            exact = sum(map(Fraction, values), Fraction())
            coefficient = module.accumulate_exact_dyadic_row(array("d", values), range(len(values)))
            self.assertEqual(Fraction(coefficient, 1 << 1074), exact)
            self.assertLessEqual(abs(coefficient).bit_length(), 2098+max(0, (len(values)-1).bit_length()))
            self.assertEqual(coefficient, module.accumulate_exact_dyadic_row(array("d", values), reversed(range(len(values)))))
            for precision in (20, 50, 100):
                lower, upper = Context(prec=precision, rounding=ROUND_FLOOR), Context(prec=precision, rounding=ROUND_CEILING)
                lo, hi = module.enclose_exact_dyadic_row(array("d", values), range(len(values)), lower, upper)
                self.assertLessEqual(Fraction(lo), exact)
                self.assertGreaterEqual(Fraction(hi), exact)
        for value in (math.inf, -math.inf, math.nan):
            with self.assertRaises(ValueError):
                module.accumulate_exact_dyadic_row(array("d", [value]), range(1))

    def test_midpoint_removes_duplicate_score_bias(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as directory:
            store, meta, out = [Path(directory)/p for p in ("rows", "meta", "out")]
            for q in (2, 4, 16, 64):
                base.build_incidence_row_store(store, 2, [[0, 1]]*q)
                manifest = module.build_certificate_source_manifest(store, meta)
                h = array("d", [1/q]*q)
                low = module.publish_accuracy_frontier_certificate(
                    store, meta, manifest, h, out, None, 1e-10, summation_bound="rump", row_sum="rump", representative="lower")
                midpoint = module.publish_accuracy_frontier_certificate(
                    store, meta, manifest, h, out, None, 1e-10, summation_bound="rump", row_sum="rump", representative="midpoint")
                self.assertEqual(struct.unpack("<2d", out.read_bytes()), (0.5, 0.5))
                self.assertLess(Fraction(midpoint["l1_error_upper"]), Fraction(low["l1_error_upper"]))
                self.assertGreater(Fraction(midpoint["l1_error_upper"]), 0)
                exact = module.publish_accuracy_frontier_certificate(
                    store, meta, manifest, h, out, None, 2e-15, summation_bound="rump", row_sum="exact", representative="midpoint")
                self.assertTrue(exact["accepted"], exact)
                self.assertEqual(struct.unpack("<2d", out.read_bytes()), (0.5, 0.5))

    def test_all_variants_exact_complete_outputs(self):
        module = self.load_required_certificate_module()
        subsets = [[0, 1], [0, 2], [1, 2], [0, 1, 2]]
        with tempfile.TemporaryDirectory() as directory:
            store, meta, out = [Path(directory)/p for p in ("rows", "meta", "out")]
            for mask in range(16):
                groups = [g for j, g in enumerate(subsets) if mask >> j & 1]
                base.build_incidence_row_store(store, 4, groups)
                manifest = module.build_certificate_source_manifest(store, meta)
                for source in (None, 0, 3):
                    oracle, h = derive_exact_factor_state(4, groups, source)
                    before = h.tobytes()
                    for row_sum, representative in itertools.product(("decimal", "rump", "exact"), ("lower", "midpoint")):
                        result = module.publish_accuracy_frontier_certificate(
                            store, meta, manifest, h, out, source, 1e-10, summation_bound="rump",
                            row_sum=row_sum, representative=representative)
                        self.assertTrue(result["accepted"])
                        scores = struct.unpack("<4d", out.read_bytes())
                        error = sum(abs(Fraction(x)-y) for x, y in zip(scores, oracle))
                        self.assertLessEqual(error, Fraction(result["l1_error_upper"]))
                        self.assertTrue(base.certify_published_rank_output(store, out, source, 1e-10)["accepted"])
                        self.assertEqual(h.tobytes(), before)

    def test_existing_paths_reproduce_exactly(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as directory:
            store, meta, out = [Path(directory)/p for p in ("rows", "meta", "out")]
            base.build_incidence_row_store(store, 5, [[0, 1, 2], [1, 2, 3], [0, 3]])
            manifest = module.build_certificate_source_manifest(store, meta)
            for source in (None, 0, 4):
                _, h = derive_exact_factor_state(5, [[0, 1, 2], [1, 2, 3], [0, 3]], source)
                for row_sum, original in (("decimal", frozen), ("rump", placed)):
                    a = original.publish_streaming_rank_certificate(store, meta, manifest, h, out, source, 1e-10, summation_bound="rump")
                    b = module.publish_accuracy_frontier_certificate(store, meta, manifest, h, out, source, 1e-10,
                                                                     summation_bound="rump", row_sum=row_sum, representative="lower")
                    for key in ("output_sha256", "l1_error_upper", "row_error_upper", "publication_error_upper", "scatter_error_upper"):
                        self.assertEqual(a[key], b[key], (key, a, b))
                    if row_sum == "decimal":
                        excluded = {"preparation_ms", "publication_ms", "total_ms", "scope"}
                        for key in a.keys()-excluded:
                            self.assertIn(key, b)
                            self.assertEqual(a[key], b[key], key)

    def test_work_counter_numeric_domains(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as directory:
            store, meta, out = [Path(directory)/p for p in ("rows", "meta", "out")]
            base.build_incidence_row_store(store, 2, [[0, 1]]*2)
            manifest = module.build_certificate_source_manifest(store, meta)
            for mode in ("decimal", "rump", "exact"):
                result = module.publish_accuracy_frontier_certificate(
                    store, meta, manifest, array("d", [0.5, 0.5]), out, None, 1e-10,
                    row_sum=mode, representative="midpoint", summation_bound="rump")
                self.assertTrue(result["accepted"])
                self.assertEqual(result["decimal_membership_additions"], 8 if mode == "decimal" else 0)
                self.assertEqual(result["exact_integer_membership_accumulations"], 4 if mode == "exact" else 0)
                self.assertIn("Decimal", result["high_precision_membership_counter_scope"])

    def test_exact_cancellation_can_be_admitted(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as directory:
            store, meta, out = [Path(directory)/p for p in ("rows", "meta", "out")]
            base.build_incidence_row_store(store, 3, [[0, 1], [0, 1]])
            manifest = module.build_certificate_source_manifest(store, meta)
            h = array("d", [2**-100, -2**-100])
            for row_sum in ("rump", "exact"):
                result = module.publish_accuracy_frontier_certificate(store, meta, manifest, h, out, 2, 1e-20,
                    precision=100, summation_bound="rump", row_sum=row_sum, representative="midpoint")
                self.assertTrue(result["accepted"])
                self.assertEqual(struct.unpack("<3d", out.read_bytes()), (0.0, 0.0, 1.0))

    def test_new_entrypoint_live_modes_refused(self):
        self.load_required_certificate_module()
        if (sys.platform, platform.machine()) != ("darwin", "arm64"):
            self.skipTest("Darwin arm64 fenv layout only")
        program = '''
import ctypes, struct, tempfile
from pathlib import Path
from array import array
import accuracy_frontier_rank_certificate as module
import probe_native_incidence_pagerank as base
class Environment(ctypes.Structure):
    _fields_ = [('fpsr', ctypes.c_ulonglong), ('fpcr', ctypes.c_ulonglong)]
lib = ctypes.CDLL(None)
lib.fegetenv.argtypes = lib.fesetenv.argtypes = [ctypes.POINTER(Environment)]
old = Environment()
assert lib.fegetenv(ctypes.byref(old)) == 0
with tempfile.TemporaryDirectory() as directory:
    store, meta, output = [Path(directory)/s for s in ('rows','meta','out')]
    base.build_incidence_row_store(store, 2, [[0,1]])
    manifest = module.build_certificate_source_manifest(store, meta)
    output.write_bytes(b'prior')
    try:
        for mask in (0x400000, 0x800000, 0xc00000, 0x1000000):
            changed = Environment(old.fpsr, old.fpcr | mask)
            assert lib.fesetenv(ctypes.byref(changed)) == 0
            if mask == 0x1000000:
                tiny = struct.unpack('<d', struct.pack('<Q', 1))[0]
                assert struct.pack('<d', tiny+tiny) == struct.pack('<Q', 0)
            for row_sum in ('decimal','rump','exact'):
                try:
                    module.publish_accuracy_frontier_certificate(store,meta,manifest,array('d',[1]),output,None,1e-10,
                                                                 row_sum=row_sum,representative='midpoint')
                except ValueError as error:
                    assert ('underflow' if mask == 0x1000000 else 'round') in str(error)
                else:
                    raise AssertionError('new publisher admitted invalid live mode')
            assert lib.fesetenv(ctypes.byref(old)) == 0
            assert output.read_bytes() == b'prior'
    finally:
        assert lib.fesetenv(ctypes.byref(old)) == 0
'''
        result = subprocess.run([sys.executable, "-B", "-c", program], cwd=MODULE.parent,
                                capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
