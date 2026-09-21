"""Exact tests for moving certified row sums out of per-membership Decimal work."""

from array import array
from decimal import Context, Decimal, ROUND_CEILING, ROUND_FLOOR
from fractions import Fraction
import importlib.util
import math
from pathlib import Path
import platform
import random
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import probe_native_incidence_pagerank as base
import streaming_rank_publication_certificate as frozen
from test_streaming_rank_certificate import StreamingCertificateTests, derive_exact_factor_state

MODULE = Path(__file__).with_name("precision_placed_rank_certificate.py")


class PrecisionPlacedCertificateTests(StreamingCertificateTests):
    def load_required_certificate_module(self):
        self.assertTrue(MODULE.exists(), "precision-placed publisher is not implemented")
        before = dict(vars(frozen))
        spec = importlib.util.spec_from_file_location("precision_certificate", MODULE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertEqual(before.keys(), vars(frozen).keys())
        self.assertTrue(all(vars(frozen)[key] is value for key, value in before.items()))
        self.assertIsNot(module._publisher, frozen)
        self.assertIs(module._publisher.publish_streaming_rank_certificate.__globals__, vars(module._publisher))
        self.assertIs(module._publisher.enclose_reconstructed_row_value, module.enclose_reconstructed_row_value)
        return module

    def test_signed_sum_exact_enclosure(self):
        module = self.load_required_certificate_module()
        lower = Context(prec=50, rounding=ROUND_FLOOR)
        upper = Context(prec=50, rounding=ROUND_CEILING)
        tiny = float.fromhex("0x0.0000000000001p-1022")
        cases = [[], [0.0], [-0.0], [tiny], [tiny, tiny, -tiny],
                 [1.0, 2**-53, -1.0], [1.0, -1.0, tiny],
                 [sys.float_info.min, -math.nextafter(sys.float_info.min, math.inf)],
                 [1.0]+[2**-53]*8, [2**900, 1.0, -2**900]]
        rng = random.Random(921731)
        for n in (1, 2, 3, 9, 31, 257):
            for exponent in (-1074, -1022, -50, 0, 600):
                cases.append([math.ldexp(rng.randrange(-7, 8), exponent) for _ in range(n)])
        module.validate_gradual_binary64_environment()
        for values in cases:
            exact = sum(map(Fraction.from_float, values), Fraction())
            lo, hi = module.enclose_binary64_signed_sum(array("d", values), range(len(values)), lower, upper)
            self.assertLessEqual(Fraction(lo), exact, values)
            self.assertGreaterEqual(Fraction(hi), exact, values)
        with self.assertRaisesRegex(ValueError, "overflow"):
            module.enclose_binary64_signed_sum(array("d", [sys.float_info.max]*2), range(2), lower, upper)
        for invalid in (math.nan, math.inf, -math.inf):
            with self.assertRaises(ValueError):
                module.enclose_binary64_signed_sum(array("d", [invalid]), range(1), lower, upper)

    def test_reconstruction_exact_rational_enclosure(self):
        module = self.load_required_certificate_module()
        lower = Context(prec=20, rounding=ROUND_FLOOR)
        upper = Context(prec=20, rounding=ROUND_CEILING)
        for values in ([1.0, 2**-53, -1.0], [0.1, 0.2, 0.3], [0.0], [2**500, -2**500, 1.0]):
            for degree in (1, 7, 2**53+3, 2**64-1):
                h = array("d", values)
                lo, hi = module.enclose_reconstructed_row_value(h, range(len(h)), degree,
                                                               Decimal("0.15"), Decimal("0.15"), lower, upper)
                exact = (3+17*sum(map(Fraction, h)))/(20*degree+17*len(h))
                self.assertLessEqual(Fraction(lo), exact)
                self.assertGreaterEqual(Fraction(hi), exact)

    def test_declared_precision_work_counters(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as directory:
            store, meta, output = [Path(directory)/s for s in ("rows", "meta", "out")]
            groups = [[0, 1, 2], [1, 2], [0, 1, 2]]
            base.build_incidence_row_store(store, 4, groups)
            manifest = module.build_certificate_source_manifest(store, meta)
            _, h = derive_exact_factor_state(4, groups, None)
            original = frozen.enclose_reconstructed_row_value
            result = module.publish_streaming_rank_certificate(store, meta, manifest, h, output, None, 1e-10)
            self.assertTrue(result["accepted"])
            self.assertEqual(result["precision_schedule"], "binary64-row-sums/scalar-decimal-ledger")
            self.assertEqual(result["high_precision_membership_operations"], 0)
            self.assertEqual(result["binary64_row_additions"], 2*manifest["memberships"])
            self.assertEqual(result["high_precision_row_enclosures"], 3)
            self.assertIs(frozen.enclose_reconstructed_row_value, original)

    def test_runtime_underflow_guard_is_used(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as directory:
            store, meta, output = [Path(directory)/s for s in ("rows", "meta", "out")]
            base.build_incidence_row_store(store, 2, [[0, 1]])
            manifest = module.build_certificate_source_manifest(store, meta)
            with patch.object(module, "validate_gradual_binary64_environment", side_effect=ValueError("underflow mode")):
                with self.assertRaisesRegex(ValueError, "underflow"):
                    module.publish_streaming_rank_certificate(store, meta, manifest, array("d", [1.0]), output, None, 1e-10)
            self.assertFalse(output.exists())

    def test_mixed_exponents_generate_roundoff(self):
        module = self.load_required_certificate_module()
        rng = random.Random(20260921)
        nonzero = 0
        for count in (2, 3, 9, 31, 257):
            for _ in range(30):
                bits = [(rng.randrange(2) << 63) | (rng.randrange(1924) << 52) | rng.getrandbits(52)
                        for _ in range(count)]
                values = [struct.unpack("<d", struct.pack("<Q", x))[0] for x in bits]
                exact, rounded = sum(map(Fraction, values)), 0.0
                for value in values:
                    rounded += value
                nonzero += Fraction(rounded) != exact
                for precision in (20, 50, 100):
                    lower = Context(prec=precision, rounding=ROUND_FLOOR)
                    upper = Context(prec=precision, rounding=ROUND_CEILING)
                    lo, hi = module.enclose_binary64_signed_sum(array("d", values), range(count), lower, upper)
                    self.assertLessEqual(Fraction(lo), exact)
                    self.assertGreaterEqual(Fraction(hi), exact)
        self.assertGreater(nonzero, 120, "random fixtures did not exercise actual rounding")

    def test_extra_precision_cannot_remove_floor(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as directory:
            store, meta, output = [Path(directory)/s for s in ("rows", "meta", "out")]
            base.build_incidence_row_store(store, 2, [[0, 1], [0, 1]])
            manifest = module.build_certificate_source_manifest(store, meta)
            h = array("d", [0.5, 0.5])
            accepted = frozen.publish_streaming_rank_certificate(
                store, meta, manifest, h, output, None, 2e-15, summation_bound="rump")
            self.assertTrue(accepted["accepted"])
            prior = output.read_bytes()
            self.assertEqual(struct.unpack("<2d", prior), (0.5, 0.5))
            for precision in (20, 50, 100):
                refused = module.publish_streaming_rank_certificate(
                    store, meta, manifest, h, output, None, 2e-15,
                    precision=precision, summation_bound="rump")
                self.assertFalse(refused["accepted"])
                self.assertEqual(output.read_bytes(), prior)
                self.assertNotEqual(refused["candidate_output_sha256"], accepted["output_sha256"])

    def test_new_publisher_live_modes_refused(self):
        self.load_required_certificate_module()
        if (sys.platform, platform.machine()) != ("darwin", "arm64"):
            self.skipTest("live fenv_t layout verified on Darwin arm64")
        program = '''
import ctypes, struct, tempfile
from pathlib import Path
from array import array
import precision_placed_rank_certificate as module
import probe_native_incidence_pagerank as base
class Environment(ctypes.Structure):
    _fields_ = [('fpsr', ctypes.c_ulonglong), ('fpcr', ctypes.c_ulonglong)]
lib = ctypes.CDLL(None)
lib.fegetenv.argtypes = lib.fesetenv.argtypes = [ctypes.POINTER(Environment)]
old = Environment()
assert lib.fegetenv(ctypes.byref(old)) == 0
module.validate_gradual_binary64_environment()
with tempfile.TemporaryDirectory() as directory:
    store, meta, output = [Path(directory)/s for s in ('rows', 'meta', 'out')]
    base.build_incidence_row_store(store, 2, [[0,1]])
    manifest = module.build_certificate_source_manifest(store, meta)
    output.write_bytes(b'prior-output')
    try:
        for mask in (0x400000, 0x800000, 0xc00000, 0x1000000):
            changed = Environment(old.fpsr, old.fpcr | mask)
            assert lib.fesetenv(ctypes.byref(changed)) == 0
            if mask == 0x1000000:
                tiny = struct.unpack('<d', struct.pack('<Q', 1))[0]
                assert struct.pack('<d', tiny+tiny) == struct.pack('<Q', 0), 'FZ not actually active'
            try:
                module.publish_streaming_rank_certificate(store, meta, manifest, array('d', [1.0]), output, None, 1e-10)
            except ValueError as error:
                assert ('underflow' if mask == 0x1000000 else 'round') in str(error)
            else:
                raise AssertionError('new publisher admitted unsupported live mode')
            assert lib.fesetenv(ctypes.byref(old)) == 0
            assert output.read_bytes() == b'prior-output'
    finally:
        assert lib.fesetenv(ctypes.byref(old)) == 0
'''
        result = subprocess.run([sys.executable, "-B", "-c", program], cwd=MODULE.parent,
                                capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
