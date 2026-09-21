"""Exact modal query signatures; established default coding, model-specific preservation."""

import random
import unittest
from fractions import Fraction

import probe_exception_core_similarity as candidate
from probe_additive_core_similarity import build_additive_orientation_core, compute_additive_core_bound
from test_additive_core_similarity import construct_actual_pair_metadata
from test_static_core_similarity import SinglePassPairStream


class ModalCoreSimilarityTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(callable(getattr(candidate, "compile_modal_capacity_core", None)),
                        "modal core compiler missing")

    def test_uniform_signature_pruning(self):
        for groups in (8, 16, 32, 64, 128, 256):
            caps = [0] * groups + [3] * groups
            for node in range(groups - 1, 0, -1):
                caps[node] = caps[2 * node] + caps[2 * node + 1]
            core = candidate.compile_modal_capacity_core(caps, [4] * groups, query_limit=4)
            self.assertEqual(core["component_defaults"], ((1, 3),))
            self.assertEqual(core["exceptions"], ())
            stream = SinglePassPairStream([(groups - 1, 4)])
            value = candidate.compute_exception_core_bound(core, 4, groups, stream)
            self.assertEqual(value, Fraction(3, 3 * groups + 1))
            self.assertLess(value, Fraction(4, groups))
            self.assertEqual(stream.iterations, 1)

    def test_balanced_strict_defaults(self):
        groups, big = 16, 10**12
        caps, population = [0] * groups + [big] * groups, [3 * big // 2] * groups
        caps[groups + 2:groups + 4], population[2:4] = [3, 3], [4, 4]
        for node in range(groups - 1, 0, -1):
            caps[node] = caps[2 * node] + caps[2 * node + 1] - 2 * (node == groups // 2 + 1)
        core = candidate.compile_modal_capacity_core(caps, population, query_limit=2)
        self.assertEqual((core["strict_nodes"], len(core["exceptions"])), (1, 0))
        self.assertEqual(candidate.compute_exception_core_bound(core, 4, caps[1], [(2, 2), (3, 2)]),
                         Fraction(3, caps[1] + 1))

    def test_sparse_empty_groups(self):
        groups = 8
        population = [1, 0, 0, 0, 0, 0, 0, 1]
        caps = [0] * groups + population
        for node in range(groups - 1, 0, -1):
            caps[node] = caps[2 * node] + caps[2 * node + 1]
        core = candidate.compile_modal_capacity_core(caps, population, query_limit=2)
        self.assertEqual(core["component_defaults"], ((0, 0),))
        self.assertEqual(core["exception_keys"], (0, 7))
        self.assertEqual(candidate.compute_exception_core_bound(core, 1, 0, [(7, 1)]), Fraction(1, 2))
        with self.assertRaisesRegex(ValueError, "population"):
            candidate.compute_exception_core_bound(core, 1, 0, [(3, 1)])

    def test_modal_frontier_parity(self):
        rng, checked = random.Random(93022), 0
        for groups in (1, 2, 4, 8):
            for _ in range(20):
                rows = [set(j for j in range(40) if rng.randrange(2)) for _ in range(2)]
                _, minimum, caps, populations = construct_actual_pair_metadata(rows, groups)
                for limit in (1, 2, 4, 8):
                    core = candidate.compile_modal_capacity_core(caps, populations, query_limit=limit)
                    fixed = candidate.compile_exception_capacity_core(caps, populations, query_limit=limit)
                    self.assertLessEqual(len(core["exceptions"]), len(fixed["exceptions"]))
                    counts = [rng.randrange(min(h, limit) + 1) for h in populations]
                    pairs = [(j, q) for j, q in enumerate(counts) if q]
                    size = sum(counts) + rng.randrange(3)
                    self.assertEqual(candidate.prepare_exception_query_core(core, size, iter(pairs)),
                                     build_additive_orientation_core(caps, counts, populations))
                    self.assertEqual(candidate.compute_exception_core_bound(core, size, minimum, iter(pairs)),
                                     compute_additive_core_bound(size, minimum, caps, counts, populations))
                    checked += 1
        self.assertEqual(checked, 320)

    def test_single_leaf_admission(self):
        checked = 0
        for capacity in range(6):
            for lower in range(capacity + 1):
                for limit in range(6):
                    core = candidate.compile_modal_capacity_core([0, capacity], [lower + capacity], query_limit=limit)
                    self.assertEqual(core["component_defaults"], ((min(lower, limit), min(capacity, limit)),))
                    for query in range(limit + 2):
                        pairs = [(0, query)] if query else []
                        if query > limit or query > lower + capacity:
                            with self.assertRaises(ValueError):
                                candidate.compute_exception_core_bound(core, query, lower, pairs)
                        else:
                            self.assertEqual(candidate.compute_exception_core_bound(core, query, lower, pairs),
                                             compute_additive_core_bound(query, lower, [0, capacity], [query],
                                                                         [lower + capacity]))
                        checked += 1
        self.assertEqual(checked, 567)

    def test_signature_behavior_equivalence(self):
        checked = 0
        for limit in range(9):
            by_signature, by_behavior = {}, {}
            for capacity in range(9):
                for lower in range(capacity + 1):
                    signature = min(lower, limit), min(capacity, limit)
                    behavior = tuple(None if q > lower + capacity else (min(q, lower), min(q, capacity))
                                     for q in range(limit + 1))
                    self.assertEqual(by_signature.setdefault(signature, behavior), behavior)
                    self.assertEqual(by_behavior.setdefault(behavior, signature), signature)
                    checked += 1
        self.assertEqual(checked, 405)


if __name__ == "__main__":
    unittest.main()
