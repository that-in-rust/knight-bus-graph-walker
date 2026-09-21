"""Original-answer checks for stationary and clique direct controls."""

from decimal import Inexact, localcontext
from fractions import Fraction
import importlib
import math
from pathlib import Path
import random
import struct
import tempfile
import unittest
from unittest.mock import patch

import test_boolean_rank_native_gap_source as native_tests
from test_stream_boolean_rank_solver import compute_expanded_rational_pagerank, ResidentBooleanSourceFixture
from boolean_rank_sqlite_source import build_boolean_rank_source, SqliteBooleanRankSource


class NativeDirectControlTests(unittest.TestCase):
    def load_native_direct_module(self):
        try:
            return importlib.import_module("boolean_rank_native_direct")
        except ModuleNotFoundError as error:
            if error.name == "boolean_rank_native_direct":
                self.fail("native stationary/clique direct control is absent")
            raise

    def build_native_direct_fixture(self, edges=None, factors=4):
        return native_tests.NativeBooleanGapTests().build_native_pair_fixture(factors,
            edges or [(0,1,1),(0,3,2),(1,2,3),(2,3,1)])

    def write_native_direct_values(self, path, source, values):
        with path.open("wb") as stream:
            for cid, _, _, _, _ in source.iterate_class_records():
                for vertex, _ in source.iterate_class_vertex_rows(cid):
                    stream.write(struct.pack("<Qd", vertex, values[vertex]))

    def test_stationary_bound_encloses_original(self):
        module = self.load_native_direct_module()
        rng = random.Random(20260924)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"scores"
            for sample in range(18):
                factors = 3+sample%3
                edges = [(a,b,rng.randrange(1,3)) for a in range(factors) for b in range(a+1,factors)
                         if b==a+1 or rng.randrange(2)]
                rows, source = self.build_native_direct_fixture(edges, factors)
                degrees = {v: sum(u!=v and bool(set(g)&set(groups)) for u,g,_ in rows)
                           for v,groups,_ in rows}
                volume = sum(degrees.values())
                total = sum(Fraction(w) for _,_,w in rows)
                exact_k = sum((Fraction(w)/total)**2*Fraction(volume,degrees[v]) for v,_,w in rows)-1
                for alpha in (0.,0.85,math.nextafter(1.,0.)):
                    truth = compute_expanded_rational_pagerank(rows,alpha)
                    for perturbation in (0.,0.125):
                        values = {v: float(Fraction(d,volume)) for v,d in degrees.items()}
                        values[rows[0][0]] += perturbation
                        self.write_native_direct_values(path, source, values)
                        report = module.certify_native_direct_output(source,path,alpha=alpha,epsilon=1e-10,
                            method="stationary",max_factor_slots=factors)
                        error = sum(abs(Fraction(values[v])-truth[v]) for v in values)
                        self.assertGreaterEqual(Fraction(report["l1_error_upper"]),error)
                        self.assertGreaterEqual(Fraction(report["personalization_moment_upper"]),exact_k)
                        self.assertEqual(report["output_read_bytes"],16*len(rows))
                        self.assertEqual(report["scratch_bytes"],0)
                        if alpha > .999:
                            self.assertEqual(report["accepted"],perturbation==0.)

    def test_stationary_personalization_and_bipartite(self):
        module = self.load_native_direct_module()
        for edges,factors in (([(0,1,1),(1,2,1)],3), ([(0,1,2),(1,2,1),(2,3,2)],4)):
            rows,_ = self.build_native_direct_fixture(edges,factors)
            degrees = {v: sum(u!=v and bool(set(g)&set(groups)) for u,g,_ in rows)
                       for v,groups,_ in rows}
            rows = [(v,g,float(degrees[v])) for v,g,_ in rows]
            source = ResidentBooleanSourceFixture(rows,factors)
            with tempfile.TemporaryDirectory() as directory:
                for alpha in (0.,.85,math.nextafter(1.,0.)):
                    receipt = module.publish_native_direct_result(source,Path(directory)/str(alpha),
                        alpha=alpha,epsilon=1e-10,method="stationary",max_factor_slots=factors)
                    self.assertTrue(receipt["accepted"])

    def test_clique_formula_and_perturbation(self):
        module = self.load_native_direct_module()
        for edges,factors in (([(0,1,2),(0,2,1),(0,3,2)],4), ([(0,1,2),(0,2,1),(1,2,2)],3)):
            rows,source = self.build_native_direct_fixture(edges,factors)
            with tempfile.TemporaryDirectory() as directory:
                path=Path(directory)/"out"
                for alpha in (0.,.85,math.nextafter(1.,0.)):
                    truth=compute_expanded_rational_pagerank(rows,alpha)
                    for perturbation in (0.,0.125):
                        values={v:float(x) for v,x in truth.items()}
                        values[rows[0][0]]+=perturbation
                        self.write_native_direct_values(path,source,values)
                        report=module.certify_native_direct_output(source,path,alpha=alpha,epsilon=1e-10,
                            method="clique-direct",max_factor_slots=factors)
                        error=sum(abs(Fraction(values[v])-truth[v]) for v in values)
                        self.assertGreaterEqual(Fraction(report["l1_error_upper"]),error)
                        self.assertEqual(report["accepted"],perturbation==0.)

    def test_sqlite_publication_and_context(self):
        module=self.load_native_direct_module()
        rows,_=self.build_native_direct_fixture([(0,1,2),(0,2,1),(0,3,2)],4)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            build_boolean_rank_source(root/"source.sqlite",rows,factor_count=4)
            with SqliteBooleanRankSource(root/"source.sqlite") as source:
                for method in ("stationary","clique-direct"):
                    with localcontext() as context:
                        context.prec=1
                        context.traps[Inexact]=True
                        receipt=module.publish_native_direct_result(source,root/method,
                            alpha=math.nextafter(1.,0.),epsilon=1e-10,method=method,max_factor_slots=4)
                    self.assertTrue(receipt["accepted"])
                    self.assertEqual(receipt["output_sha256"],receipt["certificate"]["output_sha256"])
                    self.assertEqual(source.events["active_cursors"],0)
                self.assertEqual(len(list(root.iterdir())),3)

    def test_limits_and_nonclique_refusal(self):
        module=self.load_native_direct_module()
        _,source=self.build_native_direct_fixture()
        with self.assertRaises(ValueError):
            module.certify_native_direct_output(source,"absent",alpha=.85,epsilon=1e-10,
                method="stationary",max_factor_slots=3)
        self.assertEqual(source.calls,0)
        with tempfile.TemporaryDirectory() as directory:
            for method,epsilon in (("clique-direct",1e-10),("stationary",1e-30)):
                with self.assertRaises(ValueError):
                    module.publish_native_direct_result(source,Path(directory)/"out",
                        alpha=.85,epsilon=epsilon,method=method,max_factor_slots=4)
                self.assertEqual(list(Path(directory).iterdir()),[])

    def test_bad_bytes_and_existing_destination(self):
        module=self.load_native_direct_module()
        _,source=self.build_native_direct_fixture()
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"out"
            module.publish_native_direct_result(source,path,alpha=math.nextafter(1.,0.),epsilon=1e-10,
                method="stationary",max_factor_slots=4)
            valid=path.read_bytes()
            with self.assertRaises(FileExistsError):
                module.publish_native_direct_result(source,path,alpha=.85,epsilon=1e-10,
                    method="stationary",max_factor_slots=4)
            self.assertEqual(path.read_bytes(),valid)
            for broken in (valid[:-1],struct.pack("<Qd",999,.1)+valid[16:],
                           struct.pack("<Qd",100,float("nan"))+valid[16:]):
                path.write_bytes(broken)
                with self.assertRaises(ValueError):
                    module.certify_native_direct_output(source,path,alpha=.85,epsilon=1e-10,
                        method="stationary",max_factor_slots=4)

    def test_consumed_prefix_mutation_detected(self):
        module=self.load_native_direct_module()
        _,source=self.build_native_direct_fixture()
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"out"
            module.publish_native_direct_result(source,path,alpha=math.nextafter(1.,0.),epsilon=1e-10,
                method="stationary",max_factor_slots=4)
            original=source.iterate_class_vertex_rows
            opens=0
            def mutate_after_first_class(cid):
                nonlocal opens
                opens+=1
                if opens==2:
                    with path.open("r+b",buffering=0) as stream:
                        stream.seek(8)
                        stream.write(struct.pack("<d",.5))
                return original(cid)
            with patch.object(source,"iterate_class_vertex_rows",mutate_after_first_class):
                with self.assertRaisesRegex(ValueError,"changed"):
                    module.certify_native_direct_output(source,path,alpha=.85,epsilon=1e-10,
                        method="stationary",max_factor_slots=4)

    def test_cancelled_write_releases_source(self):
        module=self.load_native_direct_module()
        rows,_=self.build_native_direct_fixture()
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            build_boolean_rank_source(root/"source.sqlite",rows,factor_count=4)
            with SqliteBooleanRankSource(root/"source.sqlite") as source:
                original=source.iterate_class_vertex_rows
                def cancel_inside_vertex_iterator(cid):
                    values=original(cid)
                    try:
                        yield next(values)
                        raise KeyboardInterrupt("cancel direct candidate")
                    finally:
                        values.close()
                with patch.object(source,"iterate_class_vertex_rows",cancel_inside_vertex_iterator):
                    with self.assertRaises(KeyboardInterrupt):
                        module.publish_native_direct_result(source,root/"out",alpha=.85,epsilon=1e-10,
                            method="stationary",max_factor_slots=4)
                self.assertEqual(source.events["active_cursors"],0)
            self.assertEqual(list(root.iterdir()),[root/"source.sqlite"])


if __name__=="__main__":
    unittest.main()
