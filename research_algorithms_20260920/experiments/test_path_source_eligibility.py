"""Exact finite gates for unit-hop and reachability source profiling."""

import importlib
from itertools import product
import json
from pathlib import Path
import random
import tempfile
import unittest


COUNTS = dict(unit_graphs=0, reference_queries=0, target_checks=0)


class PathSourceEligibilityTests(unittest.TestCase):
    def load_path_eligibility_module(self):
        try:
            return importlib.import_module("path_source_eligibility")
        except ModuleNotFoundError as error:
            if error.name == "path_source_eligibility":
                self.fail("path source eligibility implementation is absent")
            raise

    def test_cycle_and_shortcut_barrier(self):
        module = self.load_path_eligibility_module()
        cycle = module.prepare_source_forest_profile(4, [(0,1),(1,2),(2,0),(2,3)], "unit")
        self.assertEqual(cycle["q"], 1)
        self.assertEqual(cycle["degree_one_cycles"], 1)
        self.assertEqual(cycle["D"], 0)
        shortcut = module.prepare_source_forest_profile(3, [(0,1),(1,2),(0,2)], "unit")
        self.assertEqual(shortcut["q"], 2)
        self.assertEqual(shortcut["D"], 0)
        reachable = module.prepare_source_forest_profile(3, [(0,1),(1,2),(0,2)], "reachability")
        self.assertEqual(reachable["D"], 1)

    def test_all_four_vertex_digraphs(self):
        module = self.load_path_eligibility_module()
        options = [(u,v) for u in range(4) for v in range(4) if u != v]
        for flags in product((False,True), repeat=len(options)):
            edges = [edge for edge, keep in zip(options,flags) if keep]
            view = module.prepare_source_forest_profile(4, edges, "unit")
            self.assertEqual(view["q"], 4-view["indegree_one"]+view["degree_one_cycles"])
            self.assertEqual(view["D"], 0)
            self.assertTrue(all(1+view["h"][u]-view["h"][v] >= 0 for u,v in edges))
            COUNTS["unit_graphs"] += 1

    def test_minimum_all_small_forests(self):
        module = self.load_path_eligibility_module()
        n = 3
        options = [(u,v) for u in range(n) for v in range(n) if u != v]
        for flags in product((False,True), repeat=len(options)):
            edges = [edge for edge, keep in zip(options,flags) if keep]
            choices = [[-1]+[u for u,v in edges if v == vertex] for vertex in range(n)]
            best = n
            for parent in product(*choices):
                acyclic = True
                for vertex in range(n):
                    seen = set()
                    while vertex != -1 and vertex not in seen:
                        seen.add(vertex)
                        vertex = parent[vertex]
                    if vertex != -1:
                        acyclic = False
                        break
                if acyclic:
                    gates = {v for v,p in enumerate(parent) if p == -1}
                    gates.update(v for u,v in edges if parent[v] != u)
                    best = min(best,len(gates))
            self.assertEqual(module.prepare_source_forest_profile(n,edges,"unit")["q"],best)

    def test_every_small_cut_query(self):
        module = self.load_path_eligibility_module()
        n = 3
        options = [(u,v) for u in range(n) for v in range(n) if u != v]
        for flags in product((False,True), repeat=len(options)):
            edges = [edge for edge,keep in zip(options,flags) if keep]
            for mode in ("unit","reachability"):
                base = module.prepare_source_forest_profile(n,edges,mode)
                children = [v for v,p in enumerate(base["parent"]) if p != -1]
                for selected in product((False,True),repeat=len(children)):
                    cuts = {v for v,yes in zip(children,selected) if yes}
                    for source in range(n):
                        receipt = module.verify_source_cut_query(base,cuts,source,all_paths=True)
                        self.assertEqual(receipt["target_checks"],n)
                        COUNTS["reference_queries"] += 1
                        COUNTS["target_checks"] += n

    def test_seeded_cut_path_queries(self):
        module = self.load_path_eligibility_module()
        rng = random.Random(20260921)
        for _ in range(80):
            n = rng.randrange(1,22)
            edges = sorted({(rng.randrange(n),rng.randrange(n)) for _ in range(3*n)})
            edges = [(u,v) for u,v in edges if u != v]
            for mode in ("unit","reachability"):
                base = module.prepare_source_forest_profile(n,edges,mode)
                cuts = {v for v,p in enumerate(base["parent"]) if p != -1 and rng.randrange(2)}
                for source in sorted({0,n-1,rng.randrange(n)}):
                    receipt = module.verify_source_cut_query(base,cuts,source,all_paths=True)
                    COUNTS["reference_queries"] += 1
                    COUNTS["target_checks"] += receipt["target_checks"]

    def test_tsv_identity_and_normalization(self):
        module = self.load_path_eligibility_module()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/"family-file-nodes.tsv").write_text("repo\tfile\na\tx\na\ty\nb\tx\n",encoding="utf-8")
            (root/"family-file-edges.tsv").write_text("repo\tsource_file\ttarget_file\na\tx\ty\na\tx\ty\na\tx\tx\n",encoding="utf-8")
            nodes,edges,receipt = module.load_dependency_source_snapshot(root)
            self.assertEqual(len(nodes),3)
            self.assertEqual(len(edges),1)
            self.assertEqual(receipt["duplicate_edges_removed"],1)
            self.assertEqual(receipt["loops_removed"],1)
            (root/"family-file-edges.tsv").write_text("repo\tsource_file\ttarget_file\na\tx\tz\n",encoding="utf-8")
            with self.assertRaises(ValueError):
                module.load_dependency_source_snapshot(root)

    def test_invalid_query_contract_inputs(self):
        module = self.load_path_eligibility_module()
        for n,edges,mode in ((-1,[],"unit"),(2,[(0,0)],"unit"),(2,[(0,1),(0,1)],"unit"),(2,[(0,2)],"unit"),(2,[(0,True)],"unit"),(2,[],"weighted")):
            with self.assertRaises(ValueError):
                module.prepare_source_forest_profile(n,edges,mode)
        base = module.prepare_source_forest_profile(2,[(0,1)],"unit")
        for cuts,source in (({0},0),({True},0),({1},True),({1},2)):
            with self.assertRaises(ValueError):
                module.verify_source_cut_query(base,cuts,source)


if __name__ == "__main__":
    result = unittest.main(exit=False).result
    print(json.dumps(COUNTS,sort_keys=True))
    raise SystemExit(not result.wasSuccessful())
