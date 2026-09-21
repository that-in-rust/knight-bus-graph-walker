"""Structural batched-repair control; no physical timing claims."""

import importlib
import random
import unittest


def load_repair_accounting_module():
    try:
        return importlib.import_module("probe_threshold_repair_accounting")
    except ModuleNotFoundError as error:
        raise AssertionError("batched-repair accounting control is absent") from error


class ThresholdRepairAccountingTests(unittest.TestCase):
    def test_union_matches_explicit_paths(self):
        module = load_repair_accounting_module()
        rng = random.Random(712091)
        for n in range(1, 66):
            size = 1 << (n - 1).bit_length()
            for _ in range(12):
                ranks = rng.sample(range(n), rng.randrange(n + 1))
                paths = set()
                for rank in ranks:
                    node = size + rank
                    while node:
                        paths.add(node)
                        node //= 2
                result = module.count_batched_repair_nodes(n, ranks)
                self.assertEqual(result["batched_node_writes"], len(paths))
                self.assertEqual(result["point_node_writes"], len(ranks) * size.bit_length())
                self.assertLessEqual(result["batched_node_writes"], result["point_node_writes"])
                self.assertLessEqual(result["max_level_entries"], len(ranks))
                self.assertEqual(sum(result["level_widths"]), len(paths))

    def test_dense_and_sparse_boundaries(self):
        module = load_repair_accounting_module()
        full = module.count_batched_repair_nodes(64, list(range(64)))
        self.assertEqual((full["point_node_writes"], full["batched_node_writes"]), (448, 127))
        one = module.count_batched_repair_nodes(65, [64])
        self.assertEqual((one["point_node_writes"], one["batched_node_writes"]), (8, 8))
        empty = module.count_batched_repair_nodes(65, [])
        self.assertEqual((empty["batched_node_writes"], empty["max_level_entries"]), (0, 0))

    def test_invalid_batches_are_rejected(self):
        module = load_repair_accounting_module()
        for n, ranks in ((0, []), (-1, []), (True, []), (3, [True]),
                         (3, [0, 0]), (3, [-1]), (3, [3]), (3, [1.0])):
            with self.assertRaises(ValueError):
                module.count_batched_repair_nodes(n, ranks)


if __name__ == "__main__":
    unittest.main()
