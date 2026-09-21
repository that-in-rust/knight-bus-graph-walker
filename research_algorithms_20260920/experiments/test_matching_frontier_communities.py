"""Exact scalar and single-pass contracts for the matching-defect family."""

from importlib import import_module
from itertools import permutations
from pathlib import Path
import random
import struct
import tempfile
import unittest


def load_required_frontier_module():
    try:
        return import_module("probe_matching_frontier_communities")
    except ModuleNotFoundError as error:
        if error.name != "probe_matching_frontier_communities":
            raise
        raise AssertionError("matching frontier implementation is absent") from error


def build_seeded_matching_mates(b, seed):
    ids = list(range(b))
    random.Random(seed).shuffle(ids)
    mates = [0] * b
    for left, right in zip(ids[::2], ids[1::2]):
        mates[left], mates[right] = right, left
    return mates


def evaluate_expanded_scalar_reference(mates):
    b, weights = len(mates), ((11, 3, 0), (3, 0, 4), (0, 4, 11))
    n = 4 * b
    kinds = [0] * (2 * b) + [1] * b + [2] * b
    adjacency = [[0] * n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            if i != j:
                adjacency[i][j] = weights[kinds[i]][kinds[j]]
                if 2 * b <= i < 3 * b and j == 2 * b + mates[i - 2 * b]:
                    adjacency[i][j] += 1
    degrees = list(map(sum, adjacency))
    mass = sum(degrees)
    labels, trace, sweep, fresh = list(range(n)), [], 0, n
    while True:
        sweep += 1
        before = len(trace)
        for vertex in range(n):
            scores = {label: 0 for label in labels}
            for other in range(n):
                if other != vertex:
                    scores[labels[other]] += 2 * mass * adjacency[vertex][other] - degrees[vertex] * degrees[other]
            source = scores[labels[vertex]]
            scores[fresh] = 0
            destination = min(scores, key=lambda label: (-scores[label], label))
            if scores[destination] > source:
                labels[vertex] = destination
                trace.append((sweep, vertex, destination))
                if destination == fresh:
                    fresh += 1
        if len(trace) == before:
            return labels, trace, sweep
        if sweep > n:
            raise AssertionError("reference did not converge in its family bound")


def evaluate_reduced_scalar_reference(mates):
    b = len(mates)
    k, r0 = 3 * b // 4 - 1, 3 * b // 4
    h, d_scale = (10 * b + 1) ** 2, 150 * b * b - 64 * b
    t = 150 * b ** 3 + 73 * b * b - 10 * b
    labels, trace, r, sweep = [True] * k + [False] * (b - k), [], k, 2
    cursor = k
    while True:
        before = len(trace)
        for vertex in range(cursor, b):
            d = t - 2 * h * r
            source_q, mate_q = labels[vertex], labels[mates[vertex]]
            gap = (-d if source_q else d) - h + d_scale * (1 if source_q != mate_q else -1)
            if gap > 0:
                labels[vertex] = not source_q
                r += -1 if source_q else 1
                trace.append((sweep, 2 * b + vertex, 1 if source_q else 3 * b + 1))
        if len(trace) == before and cursor == 0:
            assert r == r0
            return labels, trace, sweep
        cursor = 0
        sweep += 1


class SinglePassMateStream:
    def __init__(self, values):
        self.values = iter(values)
        self.started = False

    def __iter__(self):
        if self.started:
            raise AssertionError("mate stream was restarted")
        self.started = True
        return self.values


class MatchingFrontierContracts(unittest.TestCase):
    def collect_streamed_family_results(self, mates):
        module = load_required_frontier_module()
        b, low, high, moves = len(mates), [], [], []
        k = 3 * b // 4 - 1
        c = sum(mate >= k for mate in mates[:k])
        receipt = module.run_matching_frontier_streams(
            b, c, SinglePassMateStream(mates[:k]), SinglePassMateStream(mates[k:]),
            low.append, high.append, lambda *event: moves.append(event),
        )
        return low + high, moves, receipt

    def test_expanded_scalar_equivalence(self):
        for b, seed in [(16, seed) for seed in range(8)] + [(24, 71), (32, 73)]:
            mates = build_seeded_matching_mates(b, seed)
            expected, trace, sweeps = evaluate_expanded_scalar_reference(mates)
            labels, actual, receipt = self.collect_streamed_family_results(mates)
            self.assertEqual([1] * (2 * b) + labels + [3 * b + 1] * b, expected)
            self.assertEqual(actual, trace)
            self.assertEqual(receipt["sweeps"], sweeps)
            self.assertEqual(receipt["mate_records_read"], b)

    def test_crossing_permutation_equivalence(self):
        b, k, cases = 24, 17, 0
        for permutation in permutations(range(7)):
            mates = [-1] * b
            for left in range(0, 10, 2):
                mates[left], mates[left + 1] = left + 1, left
            for rank, partner in enumerate(permutation):
                left, right = 10 + rank, k + partner
                mates[left], mates[right] = right, left
            expected, trace, sweeps = evaluate_reduced_scalar_reference(mates)
            labels, actual, receipt = self.collect_streamed_family_results(mates)
            self.assertEqual(labels, [3 * b + 1 if flag else 1 for flag in expected])
            self.assertEqual(actual[4 * b - 2 + k:], trace)
            self.assertEqual(receipt["sweeps"], sweeps)
            self.assertEqual(receipt["completion_moves"], 7)
            cases += 1
        self.assertEqual(cases, 5040)

    def test_seeded_fragmented_output(self):
        cases = 0
        for b in (16, 24, 32, 64, 128, 256):
            for seed in range(25):
                mates = build_seeded_matching_mates(b, seed)
                expected, trace, sweeps = evaluate_reduced_scalar_reference(mates)
                labels, actual, receipt = self.collect_streamed_family_results(mates)
                k = 3 * b // 4 - 1
                self.assertEqual(labels, [3 * b + 1 if flag else 1 for flag in expected])
                self.assertEqual(actual[4 * b - 2 + k:], trace)
                self.assertEqual(receipt["sweeps"], sweeps)
                self.assertEqual(receipt["label_records_written"], b)
                cases += 1
        self.assertEqual(cases, 150)

    def test_packed_streaming_files(self):
        module = load_required_frontier_module()
        b, word = 128, struct.Struct("<Q")
        mates = build_seeded_matching_mates(b, 919)
        labels, moves, _ = self.collect_streamed_family_results(mates)
        k = 3 * b // 4 - 1
        c = sum(mate >= k for mate in mates[:k])
        with tempfile.TemporaryDirectory() as directory:
            source, destination = Path(directory) / "mates.bin", Path(directory) / "result"
            with source.open("wb") as handle:
                for mate in mates:
                    handle.write(word.pack(mate))
            receipt = module.run_matching_frontier_files(source, destination, b, c, buffer_bytes=16)
            full = [item[0] for item in struct.iter_unpack("<Q", (destination / "labels.bin").read_bytes())]
            trace = list(struct.iter_unpack("<QQQ", (destination / "moves.bin").read_bytes()))
            self.assertEqual(full, [1] * (2 * b) + labels + [3 * b + 1] * b)
            self.assertEqual(trace, moves)
            self.assertEqual(receipt["source_bytes_read"], 8 * b)
            self.assertEqual(receipt["retained_output_bytes"], 32 * b + 24 * len(moves))
            self.assertFalse((destination / "low.bin").exists())
            self.assertFalse((destination / "high.bin").exists())

    def test_invalid_contract_refusal(self):
        module = load_required_frontier_module()
        for b, c in ((8, 1), (17, 1), (True, 1), (16, 2), (16, 7), (16, True)):
            with self.assertRaises(ValueError):
                module.run_matching_frontier_streams(b, c, iter(()), iter(()), lambda x: None, lambda x: None, lambda *x: None)
        mates = build_seeded_matching_mates(16, 4)
        k, c = 11, sum(mate >= 11 for mate in mates[:11])
        for low, high in ((mates[:k - 1], mates[k:]), (mates[:k] + [0], mates[k:]), ([-1] + mates[1:k], mates[k:])):
            with self.assertRaises(ValueError):
                module.run_matching_frontier_streams(16, c, iter(low), iter(high), lambda x: None, lambda x: None, lambda *x: None)

    def test_counts_hide_identity(self):
        outputs = []
        for permutation in (range(7), reversed(range(7))):
            mates = [-1] * 24
            for left in range(0, 10, 2):
                mates[left], mates[left + 1] = left + 1, left
            for rank, partner in enumerate(permutation):
                left, right = 10 + rank, 17 + partner
                mates[left], mates[right] = right, left
            labels, moves, receipt = self.collect_streamed_family_results(mates)
            outputs.append((labels, moves, receipt))
        self.assertEqual(outputs[0][2], outputs[1][2])
        self.assertEqual(outputs[0][0].count(73), outputs[1][0].count(73))
        self.assertNotEqual(outputs[0][0], outputs[1][0])
        self.assertNotEqual(outputs[0][1], outputs[1][1])


if __name__ == "__main__":
    unittest.main()
