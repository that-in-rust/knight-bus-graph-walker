"""Original-ID native publication options and no-partial-output failures."""

from fractions import Fraction
import importlib
import math
from pathlib import Path
import struct
import tempfile
import unittest

import test_boolean_rank_native_gap_source as native_tests
from test_stream_boolean_rank_solver import compute_expanded_rational_pagerank,ResidentBooleanSourceFixture


class NativeRankPublicationTests(unittest.TestCase):
    def load_native_pipeline_module(self):
        try:
            return importlib.import_module("boolean_rank_native_pipeline")
        except ModuleNotFoundError as error:
            if error.name=="boolean_rank_native_pipeline":
                self.fail("native publication pipeline is absent")
            raise

    def build_publication_test_source(self):
        return native_tests.NativeBooleanGapTests().build_native_pair_fixture(4,
            [(0,1,1),(0,3,2),(1,2,3),(2,3,1)])

    def publish_native_test_result(self,module,source,path,**changes):
        arguments=dict(alpha=0.85,epsilon=1e-10,method="factor-cg",certificate_method="native-gap",
                       max_factor_slots=4,max_class_slots=source.active_class_count)
        arguments.update(changes)
        return module.publish_native_rank_result(source,path,**arguments)

    def test_four_original_publication_combinations(self):
        module=self.load_native_pipeline_module()
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for method in ("factor-cg","class-cg"):
                for certificate in ("generic","native-gap"):
                    rows,source=self.build_publication_test_source()
                    path=root/f"{method}-{certificate}"
                    receipt=self.publish_native_test_result(module,source,path,
                        method=method,certificate_method=certificate)
                    truth=compute_expanded_rational_pagerank(rows,0.85)
                    actual=dict(struct.iter_unpack("<Qd",path.read_bytes()))
                    error=sum(abs(Fraction(actual[v])-value) for v,value in truth.items())
                    self.assertEqual(set(actual),set(truth))
                    self.assertLessEqual(error,Fraction(receipt["certificate"]["l1_error_upper"]))
                    self.assertTrue(receipt["accepted"])
                    self.assertEqual(receipt["output_sha256"],receipt["certificate"]["output_sha256"])
                    self.assertEqual(receipt["certificate_method"],certificate)
            self.assertEqual(len(list(root.iterdir())),4)

    def test_near_one_gap_admission(self):
        module=self.load_native_pipeline_module()
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for method in ("factor-cg","class-cg"):
                _,source=self.build_publication_test_source()
                receipt=self.publish_native_test_result(module,source,root/method,
                    alpha=math.nextafter(1.,0.),method=method)
                self.assertTrue(receipt["accepted"])
            self.assertEqual(len(list(root.iterdir())),2)

    def test_refusal_keeps_no_result(self):
        module=self.load_native_pipeline_module()
        with tempfile.TemporaryDirectory() as directory:
            for certificate in ("generic","native-gap"):
                _,source=self.build_publication_test_source()
                with self.assertRaises(module.NativeRankCertificateRefusal) as caught:
                    self.publish_native_test_result(module,source,Path(directory)/"out",
                        epsilon=1e-30,certificate_method=certificate)
                self.assertFalse(caught.exception.receipt["accepted"])
                self.assertEqual(list(Path(directory).iterdir()),[])

    def test_unsupported_source_precedes_solving(self):
        module=self.load_native_pipeline_module()
        source=ResidentBooleanSourceFixture([(0,(0,),1.),(1,(0,1),1.)],4)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                self.publish_native_test_result(module,source,Path(directory)/"out")
            self.assertEqual(list(Path(directory).iterdir()),[])

    def test_existing_and_invalid_options(self):
        module=self.load_native_pipeline_module()
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"out"
            path.write_bytes(b"preserve")
            _,source=self.build_publication_test_source()
            with self.assertRaises(FileExistsError):
                self.publish_native_test_result(module,source,path)
            self.assertEqual(source.calls,0)
            self.assertEqual(path.read_bytes(),b"preserve")
            for changes in ({"certificate_method":"unknown"},{"epsilon":0.},{"precision":1}):
                with self.assertRaises(ValueError):
                    self.publish_native_test_result(module,source,Path(directory)/"new",**changes)
                self.assertEqual(source.calls,0)


if __name__=="__main__":
    unittest.main()
