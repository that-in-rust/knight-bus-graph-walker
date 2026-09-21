"""Exact complete-output checks for the scalar-ledger publication proposal."""

from array import array
from fractions import Fraction
import hashlib
import importlib.util
import itertools
import math
import platform
from pathlib import Path
import random
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import probe_native_incidence_pagerank as base
from test_native_incidence_pagerank import solve_expanded_fraction_oracle

MODULE = Path(__file__).with_name("streaming_rank_publication_certificate.py")


def derive_exact_factor_state(n, groups, source):
    retained = [group for group in groups if len(group) >= 2]
    degrees = [sum(len(group)-1 for group in retained if i in group) for i in range(n)]
    oracle = solve_expanded_fraction_oracle(n, groups, source)
    h = array("d", [float(sum((oracle[i]/degrees[i] for i in group), Fraction()))
                    for group in retained])
    return oracle, h


class StreamingCertificateTests(unittest.TestCase):
    def load_required_certificate_module(self):
        self.assertTrue(MODULE.exists(), "streaming publication certificate is not implemented")
        spec = importlib.util.spec_from_file_location("stream_certificate", MODULE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_certificate_bounds_complete_outputs(self):
        module = self.load_required_certificate_module()
        subsets = [list(group) for size in range(1, 4)
                   for group in itertools.combinations(range(3), size)]
        rng = random.Random(920731)
        cases = [(3, [group for j, group in enumerate(subsets) if mask >> j & 1])
                 for mask in range(128)]
        cases += [(n, [rng.sample(range(n), rng.randrange(1, n+1))
                       for _ in range(rng.randrange(1, 10))])
                  for n in range(4, 8) for _ in range(8)]
        with tempfile.TemporaryDirectory() as folder:
            store, metadata, output = [Path(folder)/name for name in ("rows", "metadata", "answer")]
            for n, groups in cases:
                base.build_incidence_row_store(store, n, groups)
                manifest = module.build_certificate_source_manifest(store, metadata)
                for source in (None, 0, n-1):
                    oracle, h = derive_exact_factor_state(n, groups, source)
                    before = h.tobytes()
                    result = module.publish_streaming_rank_certificate(
                        store, metadata, manifest, h, output, source, 1e-10)
                    self.assertTrue(result["accepted"], (n, groups, source, result))
                    answer = struct.unpack(f"<{n}d", output.read_bytes())
                    actual = sum(abs(Fraction.from_float(x)-y) for x, y in zip(answer, oracle))
                    self.assertLessEqual(actual, Fraction(result["l1_error_upper"]))
                    self.assertLessEqual(Fraction(result["l1_error_upper"]), Fraction("1e-10"))
                    self.assertTrue(base.certify_published_rank_output(store, output, source, 1e-10)["accepted"])
                    self.assertEqual(before, h.tobytes())
                    self.assertEqual(result["output_sha256"], hashlib.sha256(output.read_bytes()).hexdigest())
                    self.assertEqual(result["publication_row_scans"], 1)
                    self.assertEqual(result["retained_decimal_vector_values"], 0)
                    self.assertEqual(result["factor_state_payload_bytes"], 16*len(h))
                    strong = module.publish_streaming_rank_certificate(
                        store, metadata, manifest, h, output, source, 1e-10, summation_bound="rump")
                    self.assertTrue(strong["accepted"])
                    self.assertEqual(strong["output_sha256"], result["output_sha256"])
                    self.assertLessEqual(actual, Fraction(strong["l1_error_upper"]))
                    self.assertLessEqual(Fraction(strong["l1_error_upper"]), Fraction(result["l1_error_upper"]))

    def test_arbitrary_states_preserve_bound(self):
        module = self.load_required_certificate_module()
        groups = [[0, 1, 2], [1, 2, 3], [0, 3], [0, 3]]
        with tempfile.TemporaryDirectory() as folder:
            store, metadata, output = [Path(folder)/name for name in ("rows", "metadata", "answer")]
            base.build_incidence_row_store(store, 5, groups)
            manifest = module.build_certificate_source_manifest(store, metadata)
            for source in (None, 4):
                oracle = solve_expanded_fraction_oracle(5, groups, source)
                for values in ([0, 0, 0, 0], [1, 2, 3, 4], [1, 1, -0.25, 0.5]):
                    h = array("d", values)
                    result = module.publish_streaming_rank_certificate(
                        store, metadata, manifest, h, output, source, 1000)
                    self.assertTrue(result["accepted"])
                    answer = struct.unpack("<5d", output.read_bytes())
                    actual = sum(abs(Fraction.from_float(x)-y) for x, y in zip(answer, oracle))
                    self.assertLessEqual(actual, Fraction(result["l1_error_upper"]))
            output.write_bytes(struct.pack("<5d", *([0.0]*5)))
            self.assertFalse(base.certify_published_rank_output(store, output, None, 1e-10)["accepted"])

    def test_refusal_preserves_existing_output(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as folder:
            store, metadata, output = [Path(folder)/name for name in ("rows", "metadata", "answer")]
            base.build_incidence_row_store(store, 4, [[0, 1], [1, 2]])
            manifest = module.build_certificate_source_manifest(store, metadata)
            output.write_bytes(b"existing-user-output")
            result = module.publish_streaming_rank_certificate(
                store, metadata, manifest, array("d", [0, 0]), output, None, 1e-30)
            self.assertFalse(result["accepted"])
            self.assertEqual(output.read_bytes(), b"existing-user-output")
            self.assertEqual(sorted(p.name for p in Path(folder).iterdir()), ["answer", "metadata", "rows"])
            for values in ([math.nan, 1], [math.inf, 1], [-100, -100]):
                with self.assertRaises(ValueError):
                    module.publish_streaming_rank_certificate(
                        store, metadata, manifest, array("d", values), output, None, 1e-10)
                self.assertEqual(output.read_bytes(), b"existing-user-output")
            with patch.object(base, "create_zero_float_vector", side_effect=AssertionError("allocation too early")):
                with self.assertRaisesRegex(ValueError, "payload budget"):
                    module.publish_streaming_rank_certificate(
                        store, metadata, manifest, array("d", [0, 0]), output, None, 1e-10,
                        maximum_factor_bytes=31)

    def test_manifest_validates_source_degrees(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as folder:
            store, metadata, output = [Path(folder)/name for name in ("rows", "metadata", "answer")]
            base.build_incidence_row_store(store, 3, [[0, 1], [1, 2]])
            manifest = module.build_certificate_source_manifest(store, metadata)
            raw = store.read_bytes()
            store.write_bytes(raw[:base.HEADER.size]+struct.pack("<Q", 2)+raw[base.HEADER.size+8:])
            with self.assertRaisesRegex(ValueError, "identity"):
                module.publish_streaming_rank_certificate(
                    store, metadata, manifest, array("d", [0, 0]), output, None, 1e-10)
            with self.assertRaisesRegex(ValueError, "degree"):
                module.build_certificate_source_manifest(store, metadata)
            store.write_bytes(raw)
            saved = metadata.read_bytes()
            metadata.write_bytes(saved[:-1])
            with self.assertRaisesRegex(ValueError, "identity|length"):
                module.publish_streaming_rank_certificate(
                    store, metadata, manifest, array("d", [0, 0]), output, None, 1e-10)
            self.assertFalse(output.exists())

    def test_normal_positive_arithmetic_admission(self):
        module = self.load_required_certificate_module()
        for bad in (-1.0, math.nan, math.inf, float.fromhex("0x0.0000000000001p-1022")):
            with self.assertRaises(ValueError):
                module.validate_positive_scatter_value(bad)
        for good in (0.0, sys.float_info.min, 1.0, sys.float_info.max):
            module.validate_positive_scatter_value(good)
        with self.assertRaisesRegex(ValueError, "gamma"):
            module.bound_positive_scatter_error(2**52, 1.0, 50)
        bound = module.bound_positive_scatter_error(3, 1.0, 50)
        u = Fraction(1, 2**53)
        gamma = 3*u/(1-3*u)
        self.assertGreaterEqual(Fraction(bound), 3*gamma/(1-gamma))

    def test_rump_control_exact_bounds(self):
        module = self.load_required_certificate_module()
        rng = random.Random(920732)
        for size in (2, 3, 9, 31):
            for exponent in (-1000, -40, 0, 400):
                values = [math.ldexp(rng.randrange(1, 32), exponent) for _ in range(size)]
                rounded = 0.0
                for value in values:
                    rounded += value
                actual = abs(Fraction.from_float(rounded)-sum(map(Fraction.from_float, values)))
                bound = module.bound_rump_scatter_error(size, rounded, 50)
                self.assertLessEqual(size*actual, Fraction(bound))
                self.assertLessEqual(Fraction(bound), Fraction(module.bound_positive_scatter_error(size, rounded, 50)))
        self.assertGreater(Fraction(module.bound_rump_scatter_error(2**52, 1.0, 50)), 0)
        size = 9
        values = [1.0]+[2**-53]*(size-1)
        rounded = 0.0
        for value in values:
            rounded += value
        actual = size*abs(Fraction(rounded)-sum(map(Fraction, values)))
        self.assertEqual(actual, Fraction(module.bound_rump_scatter_error(size, rounded, 50)))

    def test_stronger_bound_changes_admission(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as folder:
            store, metadata, output = [Path(folder)/name for name in ("rows", "metadata", "answer")]
            base.build_incidence_row_store(store, 2, [[0, 1]])
            manifest = module.build_certificate_source_manifest(store, metadata)
            h = array("d", [1.0])
            conservative = module.publish_streaming_rank_certificate(
                store, metadata, manifest, h, output, None, 2e-15)
            self.assertFalse(conservative["accepted"])
            self.assertFalse(output.exists())
            stronger = module.publish_streaming_rank_certificate(
                store, metadata, manifest, h, output, None, 2e-15, summation_bound="rump")
            self.assertTrue(stronger["accepted"])
            self.assertEqual(struct.unpack("<2d", output.read_bytes()), (0.5, 0.5))
            self.assertEqual(stronger["output_sha256"], conservative["candidate_output_sha256"])

    def test_live_rounding_mode_refused(self):
        self.load_required_certificate_module()
        if (sys.platform, platform.machine()) != ("darwin", "arm64"):
            self.skipTest("Darwin arm64 FE_UPWARD witness only")
        program = '''
import ctypes, tempfile
from pathlib import Path
from array import array
import probe_native_incidence_pagerank as base
import streaming_rank_publication_certificate as module
with tempfile.TemporaryDirectory() as folder:
    store, meta, output = [Path(folder)/n for n in ('rows','meta','output')]
    base.build_incidence_row_store(store, 2, [[0,1]])
    manifest = module.build_certificate_source_manifest(store, meta)
    lib = ctypes.CDLL(None)
    old = lib.fegetround()
    try:
        assert lib.fesetround(0x400000) == 0
        try:
            module.publish_streaming_rank_certificate(store,meta,manifest,array('d',[1.0]),output,None,1e-10)
        except ValueError as error:
            assert 'round' in str(error)
        else:
            raise AssertionError('changed runtime rounding was admitted')
        assert not output.exists()
    finally:
        assert lib.fesetround(old) == 0
'''
        result = subprocess.run([sys.executable, "-c", program], cwd=MODULE.parent, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_receipt_io_precedes_commit(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as folder:
            store, metadata, output = [Path(folder)/name for name in ("rows", "metadata", "answer")]
            base.build_incidence_row_store(store, 2, [[0, 1]])
            manifest = module.build_certificate_source_manifest(store, metadata)
            output.write_bytes(b"prior")
            real_stat = Path.stat

            def reject_postcommit_source_stat(path, *args, **kwargs):
                if path == store and output.read_bytes() != b"prior":
                    raise OSError("postcommit source stat")
                return real_stat(path, *args, **kwargs)

            with patch.object(Path, "stat", reject_postcommit_source_stat):
                result = module.publish_streaming_rank_certificate(
                    store, metadata, manifest, array("d", [1.0]), output, None, 1e-10)
            self.assertTrue(result["accepted"])

    def test_encoder_error_is_charged(self):
        module = self.load_required_certificate_module()
        with tempfile.TemporaryDirectory() as folder:
            store, metadata, output = [Path(folder)/name for name in ("rows", "metadata", "answer")]
            base.build_incidence_row_store(store, 1, [])
            manifest = module.build_certificate_source_manifest(store, metadata)
            output.write_bytes(b"prior")
            real_codec = base.DOUBLE

            class WrongOutputCodec:
                def pack(self, value):
                    return real_codec.pack(0.0)

                def unpack(self, encoded):
                    return real_codec.unpack(encoded)

            with patch.object(base, "DOUBLE", WrongOutputCodec()):
                result = module.publish_streaming_rank_certificate(
                    store, metadata, manifest, array("d"), output, None, 1e-10)
            self.assertFalse(result["accepted"])
            self.assertGreaterEqual(Fraction(result["l1_error_upper"]), 1)
            self.assertEqual(output.read_bytes(), b"prior")


if __name__ == "__main__":
    unittest.main()
