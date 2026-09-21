"""Decision-level integration checks; not a physical-memory benchmark."""

import importlib
from itertools import product
import random
import unittest
from unittest.mock import patch

from test_bounded_seed_similarity import compute_finite_similarity_oracle


class LargeGroupSimilarityTests(unittest.TestCase):
    def load_large_group_module(self):
        try:
            return importlib.import_module("profile_large_group_similarity")
        except ModuleNotFoundError as error:
            if error.name == "profile_large_group_similarity":
                self.fail("large-group workload profiler is absent")
            raise

    def test_separating_block_and_refusal(self):
        module = self.load_large_group_module()
        query = {0,4,1,5,2}
        targets = [(0,query|{77}),(1,query|{78}),
                   (10,{0,4,8,1,2}),(11,{12,5,9,13,2})]
        source = module.prepare_large_group_source(targets,4)
        expected = compute_finite_similarity_oracle(targets,query,None,2)
        rows,base = module.run_large_group_query(source,query,None,2,"interval")
        self.assertEqual(rows,expected)
        self.assertEqual(base["body_targets"],4)
        for mode in ("modal_branch","modal_dual"):
            rows,stats = module.run_large_group_query(source,query,None,2,mode)
            self.assertEqual(rows,expected)
            self.assertEqual(stats["body_targets"],2)
            self.assertEqual(stats["modal_evaluations"],1)
            for options in ({"state_cap":0},{"work_cap":0},{"query_work_cap":0}):
                rows,stats = module.run_large_group_query(source,query,None,2,mode,**options)
                self.assertEqual(rows,expected)
                self.assertEqual(stats["body_targets"],4)
                self.assertEqual(stats["modal_budget_refusals"],1)

    def test_tau_and_invalid_metadata(self):
        module = self.load_large_group_module()
        query = {0,4,1,5,2}
        targets = [(0,query|{77}),(1,query|{78}),(10,{0,4,8,1,2}),(11,{12,5,9,13,2})]
        source = module.prepare_large_group_source(targets,4,query_limit=1)
        with patch.object(module,"compute_compressed_threshold_bound",side_effect=AssertionError("tau bypass")):
            rows,stats = module.run_large_group_query(source,query,None,2,"modal_dual")
        self.assertEqual(rows,compute_finite_similarity_oracle(targets,query,None,2))
        self.assertEqual(stats["modal_tau_refusals"],1)
        source = module.prepare_large_group_source(targets,4)
        with patch.object(module,"compute_compressed_threshold_bound",side_effect=ValueError("no complete pair realizes the metadata")):
            with self.assertRaisesRegex(ValueError,"no complete pair"):
                module.run_large_group_query(source,query,None,2,"modal_dual")

    def test_cumulative_reservation_and_peak(self):
        module = self.load_large_group_module()
        query = {0,4,1,5,2}
        pair = ({0,4,8,1,2},{12,5,9,13,2})
        targets = [(0,query|{77}),(1,query|{78})]+[(10+j,pair[j%2]) for j in range(6)]
        source = module.prepare_large_group_source(targets,4)
        for mode in ("modal_branch","modal_dual"):
            _,full = module.run_large_group_query(source,query,None,2,mode)
            one_call = full["solver_reserved_work"]//3
            rows,paid = module.run_large_group_query(source,query,None,2,mode,query_work_cap=one_call)
            self.assertEqual(rows,compute_finite_similarity_oracle(targets,query,None,2))
            self.assertEqual(paid["modal_evaluations"],1)
            self.assertEqual(paid["modal_budget_refusals"],2)
            self.assertEqual(paid["solver_reserved_work"],one_call)
            self.assertEqual(paid["solver_states_peak"],full["solver_states_peak"])

    def test_small_complete_result_matrix(self):
        module = self.load_large_group_module()
        subsets = (set(),{0},{1},{0,1})
        for values in product(subsets,repeat=3):
            targets = list(zip((-3,2,9),values))
            source = module.prepare_large_group_source(targets,2)
            for query,sid,k in product(subsets,(None,-3),(0,1,2,5)):
                expected = compute_finite_similarity_oracle(targets,query|{99},sid,k)
                for mode in ("union","interval","modal_branch","modal_dual","posting_merge"):
                    rows,_ = module.run_large_group_query(source,query|{99},sid,k,mode)
                    self.assertEqual(rows,expected)

    def test_seeded_large_group_queries(self):
        module = self.load_large_group_module()
        rng = random.Random(20260921)
        for groups in (4,16,64):
            for _ in range(20):
                targets = [(v,{x for x in range(80) if rng.randrange(5)==0}) for v in range(9)]
                source = module.prepare_large_group_source(targets,groups)
                query = {x for x in range(84) if rng.randrange(4)==0}
                expected = compute_finite_similarity_oracle(targets,query,3,4)
                for mode in ("union","interval","modal_branch","modal_dual","posting_merge"):
                    rows,stats = module.run_large_group_query(source,query,3,4,mode)
                    self.assertEqual(rows,expected)
                    self.assertLessEqual(stats["solver_reserved_work"],10**7)

    def test_stable_order_and_input(self):
        module = self.load_large_group_module()
        targets = [(9,{1}),(2,{1}),(5,set()),(4,set())]
        source = module.prepare_large_group_source(targets,4)
        for query in ({1},set()):
            for mode in ("union","interval","modal_branch","modal_dual","posting_merge"):
                rows,_ = module.run_large_group_query(source,query,2,3,mode)
                self.assertEqual(rows,compute_finite_similarity_oracle(targets,query,2,3))
        for options in ({"k":True},{"k":-1},{"state_cap":True},{"work_cap":-1},{"mode":"unknown"}):
            args = dict(sid=None,k=2,mode="modal_dual") | options
            with self.assertRaises(ValueError):
                module.run_large_group_query(source,{1},**args)
        with self.assertRaises(ValueError):
            module.prepare_large_group_source([(1,{0}),(1,{2})],4)


if __name__ == "__main__":
    unittest.main()
