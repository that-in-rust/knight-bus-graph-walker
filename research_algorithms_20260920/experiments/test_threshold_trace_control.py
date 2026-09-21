"""The equally informed chronological control inherits all semantic cases."""

import importlib
import random
import unittest
from fractions import Fraction as F

import test_single_flip_trace_verifier as common


class ThresholdTraceControlTests(common.SingleFlipTraceVerificationTests):
    def load_trace_verification_candidate(self):
        try:
            return importlib.import_module("probe_threshold_trace_control")
        except ModuleNotFoundError as error:
            raise AssertionError("chronological threshold control is not implemented") from error

    def test_threshold_predicates_match_gains(self):
        control = self.load_trace_verification_candidate()
        import probe_single_flip_trace_verifier as offline
        for total in (F(8), F(21, 2)):
            for degree in (F(0), F(1, 3), F(2)):
                for affinity in (F(0), degree / 2, degree):
                    for gamma in (F(1, 3), F(1), F(3)):
                        for label in ("P", "Q"):
                            lower, upper = control.compute_vertex_stay_thresholds(total, gamma, degree, affinity, label)
                            samples = [F(z, 3) for z in range(-3, 42)]
                            for boundary in (lower, upper):
                                if boundary is not None:
                                    samples.extend((boundary - F(1, 100), boundary, boundary + F(1, 100)))
                            for volume in samples:
                                gains = offline.evaluate_exact_visit_gains(total, gamma, degree, affinity, volume, label)
                                safe = (lower is None or volume >= lower) and (upper is None or volume <= upper)
                                self.assertEqual(safe, max(gains) <= 0)

    def test_updated_rank_range_thresholds(self):
        control = self.load_trace_verification_candidate()
        rng = random.Random(571)
        for n in range(1, 17):
            values = [(None, None)] * n
            tree = control.RankStayThresholdTree(values)
            for _ in range(60):
                rank = rng.randrange(n)
                pair = (F(rng.randrange(-20, 20), 3), None) if rng.randrange(2) else (None, F(rng.randrange(-20, 20), 7))
                values[rank] = pair
                tree.update_vertex_stay_threshold(rank, pair)
                lo, hi = sorted((rng.randrange(n), rng.randrange(n)))
                lower = [a for a, _ in values[lo:hi + 1] if a is not None]
                upper = [b for _, b in values[lo:hi + 1] if b is not None]
                self.assertEqual(tree.query_rank_interval_thresholds(lo, hi),
                                 (max(lower) if lower else None, min(upper) if upper else None))


if __name__ == "__main__":
    unittest.main()
