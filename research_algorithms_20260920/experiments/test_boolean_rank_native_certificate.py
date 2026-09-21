"""Degree-weighted original-output bounds on missing/unequal pair sources."""

from decimal import localcontext, Inexact
from fractions import Fraction
import importlib
from pathlib import Path
import struct
import tempfile
import math
import os
import random
import unittest

import test_boolean_rank_native_gap_source as native_tests
from test_stream_boolean_rank_solver import compute_expanded_rational_pagerank


class NativeBooleanCertificateTests(unittest.TestCase):
    def load_native_certificate_module(self):
        try:
            return importlib.import_module("boolean_rank_native_certificate")
        except ModuleNotFoundError as error:
            if error.name == "boolean_rank_native_certificate":
                self.fail("weighted native original-byte certificate is absent")
            raise

    def build_native_test_fixture(self):
        return native_tests.NativeBooleanGapTests().build_native_pair_fixture(4,
            [(0,1,1),(0,3,2),(1,2,3),(2,3,1)])

    def write_native_score_file(self, path, source, scores):
        with path.open("wb") as stream:
            for cid,_,_,_,_ in source.iterate_class_records():
                for vertex,_ in source.iterate_class_vertex_rows(cid):
                    stream.write(struct.pack("<Qd",vertex,scores[vertex]))

    def test_weighted_variance_encloses_truth(self):
        module=self.load_native_certificate_module()
        rows,source=self.build_native_test_fixture()
        degree={v:sum(u!=v and bool(set(g)&set(groups)) for u,g,_ in rows) for v,groups,_ in rows}
        volume=sum(degree.values())
        total=sum(Fraction(w) for _,_,w in rows)
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"scores"
            for alpha in (0.,0.85,math.nextafter(1.,0.)):
                truth=compute_expanded_rational_pagerank(rows,alpha)
                for perturbation in (0.,0.125):
                    scores={v:float(value) for v,value in truth.items()}
                    scores[rows[0][0]]+=perturbation
                    self.write_native_score_file(path,source,scores)
                    a=Fraction(alpha)
                    residual=[]
                    for v,groups,w in rows:
                        incoming=sum(Fraction(scores[u]) / degree[u] for u,g,_ in rows
                                     if u!=v and bool(set(g)&set(groups)))
                        residual.append((v,(1-a)*Fraction(w)/total+a*incoming-Fraction(scores[v])))
                    exact_variance=sum(r*r*Fraction(volume,degree[v]) for v,r in residual)-sum(r for _,r in residual)**2
                    error=sum(abs(Fraction(scores[v])-truth[v]) for v in scores)
                    for precision in (7,60):
                        report=module.certify_native_boolean_output(source,path,alpha=alpha,
                            epsilon=1e-10,precision=precision,max_factor_slots=4,scratch_dir=directory)
                        self.assertGreaterEqual(Fraction(report["variance_upper"]),exact_variance)
                        self.assertGreaterEqual(Fraction(report["l1_error_upper"]),error)
                        self.assertEqual(report["volume"],volume)
                        self.assertEqual(report["nrows"],len(rows))
                        self.assertEqual(report["scratch_records"],source.class_count)
                        if precision==60:
                            self.assertEqual(report["accepted"],perturbation==0.)
                        self.assertEqual(list(Path(directory).iterdir()),[path])

    def test_mass_and_tolerance_refusal(self):
        module=self.load_native_certificate_module()
        rows,source=self.build_native_test_fixture()
        truth=compute_expanded_rational_pagerank(rows,0.85)
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"half"
            self.write_native_score_file(path,source,{v:float(value)/2 for v,value in truth.items()})
            report=module.certify_native_boolean_output(source,path,alpha=0.85,
                epsilon=0.01,max_factor_slots=4,scratch_dir=directory)
            self.assertFalse(report["accepted"])
            self.assertGreaterEqual(Fraction(report["l1_error_upper"]),Fraction(49,100))

    def test_sqlite_failures_and_context(self):
        module=self.load_native_certificate_module()
        from boolean_rank_sqlite_source import build_boolean_rank_source,SqliteBooleanRankSource
        rows,_=self.build_native_test_fixture()
        truth=compute_expanded_rational_pagerank(rows,0.85)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            build_boolean_rank_source(root/"source.sqlite",rows,factor_count=4)
            with SqliteBooleanRankSource(root/"source.sqlite") as source:
                path=root/"scores"
                self.write_native_score_file(path,source,{v:float(value) for v,value in truth.items()})
                valid=path.read_bytes()
                baseline=module.certify_native_boolean_output(source,path,alpha=0.85,epsilon=1e-10,max_factor_slots=4)
                with localcontext() as context:
                    context.prec=1
                    context.traps[Inexact]=True
                    report=module.certify_native_boolean_output(source,path,alpha=0.85,epsilon=1e-10,max_factor_slots=4)
                self.assertEqual(report["l1_error_upper"],baseline["l1_error_upper"])
                for broken in (valid[:-1],struct.pack("<Qd",999,0.1)+valid[16:],
                               struct.pack("<Qd",rows[0][0],float("nan"))+valid[16:]):
                    path.write_bytes(broken)
                    with self.assertRaises(ValueError):
                        module.certify_native_boolean_output(source,path,alpha=0.85,
                            epsilon=1e-10,max_factor_slots=4,scratch_dir=root)
                    self.assertEqual(source.events["active_cursors"],0)
                    self.assertEqual(len(list(root.iterdir())),2)

    def test_reservation_precedes_output_open(self):
        module=self.load_native_certificate_module()
        _,source=self.build_native_test_fixture()
        with self.assertRaises(ValueError):
            module.certify_native_boolean_output(source,"absent",alpha=0.85,epsilon=1e-10,max_factor_slots=3)
        self.assertEqual(source.calls,0)

    def test_seeded_original_answer_comparisons(self):
        module=self.load_native_certificate_module()
        rng=random.Random(20260923)
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"scores"
            for case in range(24):
                factors=3+case%3
                edges=[(a,b,rng.randrange(1,3)) for a in range(factors) for b in range(a+1,factors)
                       if b==a+1 or rng.randrange(2)]
                rows,source=native_tests.NativeBooleanGapTests().build_native_pair_fixture(factors,edges)
                for alpha in (0.,0.85,math.nextafter(1.,0.)):
                    truth=compute_expanded_rational_pagerank(rows,alpha)
                    for perturbation in (0.,2.**-20):
                        scores={v:float(value) for v,value in truth.items()}
                        scores[rows[0][0]]+=perturbation
                        self.write_native_score_file(path,source,scores)
                        report=module.certify_native_boolean_output(source,path,alpha=alpha,
                            epsilon=1e-10,max_factor_slots=factors,scratch_dir=directory)
                        error=sum(abs(Fraction(scores[v])-truth[v]) for v in scores)
                        self.assertGreaterEqual(Fraction(report["l1_error_upper"]),error)
                        self.assertEqual(report["accepted"],perturbation==0.)

    def test_consumed_prefix_write_rejected(self):
        module=self.load_native_certificate_module()
        rows,base=self.build_native_test_fixture()
        truth=compute_expanded_rational_pagerank(rows,0.85)
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"scores"
            self.write_native_score_file(path,base,{v:float(value) for v,value in truth.items()})
            class MutatingSource(type(base)):
                scans=0
                def iterate_class_records(self):
                    self.scans+=1
                    return super().iterate_class_records()
                def iterate_class_vertex_rows(self,cid):
                    for row in super().iterate_class_vertex_rows(cid):
                        yield row
                        if self.scans==4 and cid==0:
                            with path.open("r+b",buffering=0) as writer:
                                writer.seek(8)
                                writer.write(struct.pack("<d",0.5))
                            stamp=path.stat()
                            os.utime(path,ns=(stamp.st_atime_ns,stamp.st_mtime_ns+1_000_000_000))
            source=MutatingSource(rows,4)
            with self.assertRaisesRegex(ValueError,"changed"):
                module.certify_native_boolean_output(source,path,alpha=0.85,
                    epsilon=1e-10,max_factor_slots=4,scratch_dir=directory)
            self.assertEqual(list(Path(directory).iterdir()),[path])


if __name__=="__main__":
    unittest.main()
