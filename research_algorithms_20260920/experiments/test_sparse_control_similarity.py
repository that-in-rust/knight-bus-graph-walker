"""Compact-only top-k integration must preserve the strongest cheap control."""

import importlib
import importlib.util
import itertools
import unittest

from profile_large_group_similarity import prepare_large_group_source, run_large_group_query
from test_bounded_seed_similarity import compute_finite_similarity_oracle


class SparseControlSimilarityTests(unittest.TestCase):
    def load_required_sparse_module(self):
        name = "probe_sparse_control_similarity"
        self.assertIsNotNone(importlib.util.find_spec(name), "sparse control implementation missing")
        return importlib.import_module(name)

    def test_dense_free_topk(self):
        module = self.load_required_sparse_module()
        targets = [(7, {0, 1, 3}), (1, {2, 3}), (-3, {0, 2}), (9, set()), (8, {100})]
        dense = prepare_large_group_source(targets, 64, query_limit=100)
        compact = module.prepare_compact_source_view(dense)
        self.assertNotIn("target_posts", compact)
        for block in compact["blocks"]:
            self.assertNotIn("caps", block)
            self.assertNotIn("populations", block)
        for query, sid, k in itertools.product((set(), {0, 2, 3}, {100}, {999}), (None, -3), (0, 1, 8)):
            expected, paid = run_large_group_query(dense, query, sid, k, "interval")
            actual, stats = module.run_sparse_interval_query(compact, query, sid, k)
            self.assertEqual(actual, expected)
            for key in ("blocks_seen", "block_memberships", "body_targets", "body_memberships", "zero_rows"):
                self.assertEqual(stats.get(key, 0), paid.get(key, 0))
            self.assertEqual(stats["dense_histogram_cells"], 0)
            self.assertLessEqual(stats["sparse_pairs_peak"], min(64, len(query)))

    def test_complete_small_topk(self):
        module = self.load_required_sparse_module()
        subsets = [set(i for i in range(2) if mask & (1 << i)) for mask in range(4)]
        checked = 0
        for groups, rows in itertools.product((1, 4), itertools.product(subsets, repeat=3)):
            targets = list(zip((-3, 2, 7), rows))
            dense = prepare_large_group_source(targets, groups, query_limit=2)
            compact = module.prepare_compact_source_view(dense)
            for query, sid, k in itertools.product(subsets, (None, -3), (0, 1, 5)):
                actual, stats = module.run_sparse_interval_query(compact, query, sid, k)
                expected, paid = run_large_group_query(dense, query, sid, k, "interval")
                self.assertEqual(actual, expected)
                self.assertEqual(actual, compute_finite_similarity_oracle(targets, query, sid, k))
                self.assertEqual(stats.get("body_memberships", 0), paid.get("body_memberships", 0))
                checked += 1
        self.assertEqual(checked, 3072)

    def test_refusal_preserves_answers(self):
        module = self.load_required_sparse_module()
        targets = [(0, {0, 1, 2, 77}), (1, {4, 5}), (2, {0, 1, 3}), (3, {0, 2, 3})]
        query = {0, 1, 2}
        for limit, cap in ((0, 4096), (10, 0), (10, 4096)):
            compact = module.prepare_compact_source_view(prepare_large_group_source(targets, 4, query_limit=limit))
            actual, stats = module.run_sparse_interval_query(compact, query, None, 1, record_cap=cap)
            self.assertEqual(actual, compute_finite_similarity_oracle(targets, query, None, 1))
            if limit == 0:
                self.assertGreater(stats["profile_refusals"], 0)
            if cap == 0:
                self.assertGreater(stats["record_refusals"], 0)
        for kwargs in ({"k": -1}, {"k": True}, {"k": 1, "record_cap": -1}):
            with self.assertRaises(ValueError):
                module.run_sparse_interval_query(compact, query, None, **kwargs)
        with self.assertRaises(ValueError):
            module.run_sparse_interval_query(compact, {True}, None, 1)


if __name__ == "__main__":
    unittest.main()
