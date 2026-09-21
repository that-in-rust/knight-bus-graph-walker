"""Finite exact multicut graph/original-path oracles, not resource measurements."""

import importlib
from itertools import product
import json
import random
import sys
import unittest


COUNTS = dict(queries=0, finite_paths=0, cut_views=0)


class MulticutPathPartitionTests(unittest.TestCase):
    def load_multicut_path_module(self):
        try:
            return importlib.import_module("multicut_path_partition_probe")
        except ModuleNotFoundError as error:
            if error.name == "multicut_path_partition_probe":
                self.fail("multicut sparse-partition reference probe is absent")
            raise

    def verify_every_source_target(self,module,base,cuts):
        for source in range(len(base["parent"])):
            view=module.prepare_multicut_source_view(base,cuts,source)
            finite=module.verify_multicut_original_paths(base,view)
            COUNTS["queries"]+=1
            COUNTS["finite_paths"]+=finite
        COUNTS["cut_views"]+=1

    def test_nested_revival_reported_once(self):
        module=self.load_multicut_path_module()
        base=module.build_multicut_path_fixture([-1,0,1,2,3],[(0,4,1)]*7)
        cuts={1,2,3,4}
        bands,revived=module.report_multicut_revived_records(base,cuts)
        repeated=sum(module.original_forest_ancestor_test(base,c,e[2]) and base["depth"][e[1]]<base["depth"][c]
                     for c in cuts for e in base["omitted"])
        self.assertEqual(repeated,28)
        self.assertEqual(len(revived),7)
        self.assertLessEqual(len(bands),2*len(cuts))
        self.verify_every_source_target(module,base,cuts)

    def test_deepest_cut_and_strictness(self):
        module=self.load_multicut_path_module()
        base=module.build_multicut_path_fixture([-1,0,1,2],[(1,3,1),(1,2,1)])
        _,revived=module.report_multicut_revived_records(base,{1,3})
        self.assertEqual([(e[1],e[2]) for e in revived],[(1,3)])
        self.assertFalse(base["depth"][1]<base["depth"][1])
        self.verify_every_source_target(module,base,{1,3})

    def test_revived_tail_changes_owner(self):
        module=self.load_multicut_path_module()
        base=module.build_multicut_path_fixture([-1,0,1,2],[(1,3,1)])
        view=module.prepare_multicut_source_view(base,{1,3},0)
        edge=view["revived"][0]
        self.assertEqual(base["owner"][edge[1]],0)
        self.assertEqual(list(view["revival_rows"]),[1])
        self.verify_every_source_target(module,base,{1,3})

    def test_parallel_identity_and_gate_cut(self):
        module=self.load_multicut_path_module()
        base=module.build_multicut_path_fixture([-1,0,1,-1],[(0,1,0),(3,1,2),(2,0,0)])
        view=module.prepare_multicut_source_view(base,{1,2},0)
        self.assertIn(1,base["gates"])
        self.assertEqual(view["deleted_ledger_count"],1)
        self.assertTrue(any(e[1:3]==(0,1) for e in view["revived"]))
        self.verify_every_source_target(module,base,{1,2})

    def test_all_small_cut_subsets(self):
        module=self.load_multicut_path_module()
        choices=[(0,2,0),(0,3,2),(1,3,0),(3,0,0),(2,1,1),(0,1,0),(3,3,0)]
        for parent in ([-1,0,1,2],[-1,0,0,1]):
            for positive in (False,True):
                for flags in product((False,True),repeat=len(choices)):
                    base=module.build_multicut_path_fixture(parent,[e for e,yes in zip(choices,flags) if yes],positive)
                    raw_before=base["raw"]
                    for cut_flags in product((False,True),repeat=3):
                        cuts={v for v,yes in zip(range(1,4),cut_flags) if yes}
                        self.verify_every_source_target(module,base,cuts)
                    self.assertEqual(base["raw"],raw_before)

    def test_seeded_forests_and_cut_sets(self):
        module=self.load_multicut_path_module()
        rng=random.Random(20260925)
        for sample in range(80):
            n=rng.randrange(3,10)
            parent=[-1]+[rng.randrange(-1,v) for v in range(1,n)]
            extras=[(rng.randrange(n),rng.randrange(n),rng.randrange(4)) for _ in range(2*n)]
            base=module.build_multicut_path_fixture(parent,extras,bool(sample%2))
            children=[v for v,p in enumerate(parent) if p>=0]
            for _ in range(8):
                cuts={v for v in children if rng.randrange(2)}
                self.verify_every_source_target(module,base,cuts)

    def test_invalid_cuts_and_sources(self):
        module=self.load_multicut_path_module()
        base=module.build_multicut_path_fixture([-1,0,1],[])
        for cuts,source in (({0},0),({3},0),({True},0),({1},3),({1},True)):
            with self.assertRaises(ValueError):
                module.prepare_multicut_source_view(base,cuts,source)


if __name__=="__main__":
    result=unittest.main(exit=False).result
    print(json.dumps(COUNTS,sort_keys=True))
    sys.exit(not result.wasSuccessful())
