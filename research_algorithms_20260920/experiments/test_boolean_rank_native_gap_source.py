"""Native missing-pair spectral admission and paid source lifetimes."""

from fractions import Fraction
import importlib
from itertools import combinations
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from test_stream_boolean_rank_solver import ResidentBooleanSourceFixture


def check_rational_positive_semidefinite(matrix):
    work = [[Fraction(x) for x in row] for row in matrix]
    for k in range(len(work)):
        if work[k][k] < 0:
            return False
        if not work[k][k]:
            if any(work[k][j] for j in range(k+1, len(work))):
                return False
            continue
        for i in range(k+1, len(work)):
            for j in range(i, len(work)):
                work[i][j] -= work[i][k]*work[k][j]/work[k][k]
                work[j][i] = work[i][j]
    return True


class NativeBooleanGapTests(unittest.TestCase):
    def load_native_gap_module(self):
        try:
            return importlib.import_module("boolean_rank_native_gap_source")
        except ModuleNotFoundError as error:
            if error.name == "boolean_rank_native_gap_source":
                self.fail("native pair-source gap validator is absent")
            raise

    def build_native_pair_fixture(self, factors, edges):
        groups = [(a,b) for a,b,h in edges for _ in range(h)]
        rows = [(100+i, pair, float(i%7+1)) for i,pair in enumerate(groups)]
        return rows, ResidentBooleanSourceFixture(rows, factors)

    def test_missing_pairs_and_multiplicities(self):
        module = self.load_native_gap_module()
        fixtures = [(4, [(0,1,1),(1,2,2),(2,3,3)]),
                    (5, [(0,i,i) for i in range(1,5)]),
                    (4, [(0,1,3),(0,3,1),(1,2,2),(2,3,1)]),
                    (4, [(a,b,1+(a+b)%3) for a,b in combinations(range(4),2)])]
        for factors, edges in fixtures:
            with self.subTest(edges=edges):
                rows, source = self.build_native_pair_fixture(factors, edges)
                report = module.validate_native_boolean_source(source, max_factor_slots=factors)
                delta = report["gap_lower"]
                adjacency = [[int(i!=j and bool(set(a[1])&set(b[1])))
                              for j,b in enumerate(rows)] for i,a in enumerate(rows)]
                degrees = list(map(sum, adjacency))
                volume, n = sum(degrees), len(rows)
                matrix = [[Fraction((degrees[i] if i==j else 0)-adjacency[i][j])
                    - delta*(Fraction(degrees[i] if i==j else 0)-Fraction(degrees[i]*degrees[j],volume))
                    for j in range(n)] for i in range(n)]
                self.assertGreater(delta, 0)
                self.assertLessEqual(delta, 1)
                self.assertTrue(check_rational_positive_semidefinite(matrix))
                self.assertEqual(report["volume"], volume)
                self.assertEqual(report["vertices"], n)
                self.assertEqual(report["validation_class_rows"], 2*len(edges))
                self.assertEqual(report["validation_class_passes"], 2)
                self.assertEqual(report["tree_edges"], factors-1)
                self.assertEqual(report["tree_arcs"], 2*(factors-1))
                self.assertEqual(report["original_vertex_reads"], 0)

    def test_uniform_bound_recovers_exact_gap(self):
        module = self.load_native_gap_module()
        for factors, height in ((4,1),(4,3),(8,2)):
            _, source = self.build_native_pair_fixture(factors,
                [(a,b,height) for a,b in combinations(range(factors),2)])
            report = module.validate_native_boolean_source(source, max_factor_slots=factors)
            self.assertTrue(report["complete_pairs"])
            self.assertEqual(report["gap_lower"], Fraction(height*factors,height*(2*factors-3)-1))
            self.assertEqual(report["gap_lower"], max(report["tree_gap_lower"],report["all_pair_gap_lower"]))

    def test_leaf_groups_do_not_weaken_comparison(self):
        module = self.load_native_gap_module()
        _, source = self.build_native_pair_fixture(6, [(0, leaf, 1) for leaf in range(1, 6)])
        report = module.validate_native_boolean_source(source, max_factor_slots=6)
        self.assertEqual(report["gap_lower"], Fraction(25, 26))
        self.assertEqual(report["unrefined_tree_gap_lower"], Fraction(5, 26))
        self.assertEqual(report["min_mediating_population"], 5)
        self.assertEqual(report["mediating_group_count"], 1)
        self.assertEqual(report["class_incidence_counter_entries"], 6)

    def test_limits_fail_before_scans(self):
        module = self.load_native_gap_module()
        for cap in (2,-1,True,3.0):
            _, source = self.build_native_pair_fixture(3, [(0,1,1),(1,2,1)])
            with self.assertRaises(ValueError):
                module.validate_native_boolean_source(source, max_factor_slots=cap)
            self.assertEqual(source.calls, 0)

    def test_declared_counts_require_integers(self):
        module = self.load_native_gap_module()
        _, source = self.build_native_pair_fixture(3, [(0,1,1),(1,2,1)])
        source.active_class_count = 2.0
        with self.assertRaises(ValueError):
            module.validate_native_boolean_source(source, max_factor_slots=3)
        self.assertEqual(source.calls, 0)

    def test_disconnected_and_invalid_domains(self):
        module = self.load_native_gap_module()
        fixtures = [(4, [(0,(0,1),1.),(1,(2,3),1.)]),
                    (4, [(0,(0,1),1.),(1,(1,2),1.)]),
                    (3, [(0,(0,),1.),(1,(1,2),1.)]),
                    (3, [(0,(),1.),(1,(0,1),1.),(2,(1,2),1.)])]
        for factors, rows in fixtures:
            with self.subTest(rows=rows):
                source = ResidentBooleanSourceFixture(rows, factors)
                with self.assertRaises(ValueError):
                    module.validate_native_boolean_source(source, max_factor_slots=factors)

    def test_degree_and_order_validation(self):
        module = self.load_native_gap_module()
        for mutation in ("degree","height","repeat","reverse","group","weight","count"):
            with self.subTest(mutation=mutation):
                _, source = self.build_native_pair_fixture(4, [(0,1,1),(1,2,2),(2,3,1)])
                record = list(source.classes[0])
                if mutation=="degree": record[3]+=1
                elif mutation=="height": record[2]=True
                elif mutation=="group": record[1]=(0,0)
                elif mutation=="weight": record[4]=-Fraction(1)
                source.classes[0]=tuple(record)
                if mutation=="repeat": source.classes[1]=source.classes[0]
                elif mutation=="reverse": source.classes.reverse()
                elif mutation=="count": source.vertex_count+=1
                with self.assertRaises(ValueError):
                    module.validate_native_boolean_source(source, max_factor_slots=4)

    def test_both_class_scans_pinned(self):
        module = self.load_native_gap_module()
        rows,_ = self.build_native_pair_fixture(4,[(0,1,1),(1,2,2),(2,3,1)])
        class MutatingSource(ResidentBooleanSourceFixture):
            scans=0
            def iterate_class_records(self):
                self.scans+=1
                if self.scans==2:
                    record=list(self.classes[0])
                    record[4]+=Fraction(1)
                    self.classes[0]=tuple(record)
                return super().iterate_class_records()
        with self.assertRaisesRegex(ValueError,"changed|inconsistent"):
            module.validate_native_boolean_source(MutatingSource(rows,4),max_factor_slots=4)

    def test_sqlite_failures_release_cursors(self):
        module = self.load_native_gap_module()
        from boolean_rank_sqlite_source import build_boolean_rank_source, SqliteBooleanRankSource
        rows,_ = self.build_native_pair_fixture(4,[(0,1,1),(1,2,2),(2,3,1)])
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"source.sqlite"
            build_boolean_rank_source(path,rows,factor_count=4)
            with SqliteBooleanRankSource(path) as source:
                original=source.iterate_class_records
                for failing_scan in (1,2):
                    opens=0
                    def fail_inside_class_iterator():
                        nonlocal opens
                        opens+=1
                        records=original()
                        try:
                            for row in records:
                                yield row
                                if opens==failing_scan:
                                    raise KeyboardInterrupt("cancel source validation")
                        finally:
                            records.close()
                    with patch.object(source,"iterate_class_records",fail_inside_class_iterator):
                        with self.assertRaises(KeyboardInterrupt):
                            module.validate_native_boolean_source(source,max_factor_slots=4)
                    self.assertEqual(source.events["active_cursors"],0)
                report=module.validate_native_boolean_source(source,max_factor_slots=4)
                self.assertGreater(report["gap_lower"],0)
                self.assertEqual(source.events["active_cursors"],0)


if __name__=="__main__":
    unittest.main()
