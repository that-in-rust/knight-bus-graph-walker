"""Full-output oracles using the unchanged runwise file/index reference."""

import ast
import importlib.util
import itertools
import random
import re
import tempfile
import types
import unittest
from fractions import Fraction
from functools import partial
from pathlib import Path

if importlib.util.find_spec("probe_bounded_seed_similarity"):
    import probe_bounded_seed_similarity as candidate
else:
    candidate = types.SimpleNamespace()


def load_runwise_reference_module(sort_capacity=4, sort_fanin=2):
    if type(sort_capacity) is not int or sort_capacity < 1 or type(sort_fanin) is not int or sort_fanin < 2:
        raise ValueError("sort capacity must be positive and fan-in at least two")
    path = Path(__file__).resolve().parents[1] / "Similarity-Runwise-Selection.md"
    blocks = re.findall(r"^```python\n(.*?)^```", path.read_text(), re.M | re.S)
    assert len(blocks) == 1
    tree = ast.parse(blocks[0], filename=str(path))
    stop = next(i for i, node in enumerate(tree.body) if isinstance(node, ast.With))
    namespace = {"__name__": "runwise_reference"}
    exec(compile(ast.Module(body=tree.body[:stop], type_ignores=[]), str(path), "exec"), namespace)
    namespace["sort_finite_record_stream"] = partial(namespace["sort_finite_record_stream"],
                                                     capacity=sort_capacity, fanin=sort_fanin)
    return types.SimpleNamespace(**namespace)


def compute_finite_similarity_oracle(targets, values, sid, k):
    source, rows = set(values), []
    for node, raw in targets:
        if node != sid:
            target = set(raw)
            overlap, union = len(source & target), len(source | target)
            score = Fraction(overlap, union) if overlap else Fraction(0)
            rows.append((score, node, len(source), len(target), overlap))
    rows.sort(key=lambda row: (-row[0], row[1]))
    return [row[1:] for row in rows[:k]]


class BoundedSeedTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.reference = load_runwise_reference_module()

    def require_bounded_selector_exists(self):
        solve = getattr(candidate, "select_bounded_runwise_answers", None)
        self.assertTrue(callable(solve), "select_bounded_runwise_answers")
        return solve

    def check_complete_similarity_answer(self, snapshot, targets, values, sid, k, **kwargs):
        solve = self.require_bounded_selector_exists()
        source = snapshot.root / "source"
        self.reference.write_normalized_source_file(source, values)
        rows, metrics = solve(self.reference, snapshot, source, sid, k, **kwargs)
        expected = compute_finite_similarity_oracle(targets, values, sid, k)
        self.assertEqual(rows, expected)
        count = len(expected)
        self.assertLessEqual(metrics["heap_peak"], max(0, 2 * count - 1))
        self.assertLessEqual(metrics["seed_peak"], count)
        self.assertLessEqual(metrics["child_rmq_calls"], max(0, 2 * (count - 1)))
        self.assertEqual(metrics["rmq_calls"], metrics["seed_rmq_calls"] + metrics["child_rmq_calls"])
        self.assertFalse(list(snapshot.root.glob("query-*")))
        return rows, metrics

    def test_fragmentation_one_slot(self):
        self.require_bounded_selector_exists()
        with tempfile.TemporaryDirectory() as directory:
            for n in (64, 256, 1024):
                targets = [(2 * n - j, ({0} if j % 2 == 0 else set()) | {2000 + j}) for j in range(n)]
                root = Path(directory) / str(n)
                snapshot = self.reference.Snapshot(self.reference.write_normalized_target_fixture(root, targets))
                _, metrics = self.check_complete_similarity_answer(snapshot, targets, {0}, None, 1, heap_cap=1)
                self.assertEqual((metrics["R"], metrics["seed_rmq_calls"], metrics["heap_peak"]), (n, n, 1))
                with self.assertRaisesRegex(AssertionError, "heap admission"):
                    self.reference.select_runwise_exact_answers(snapshot, root / "source", None, 1, heap_cap=1)

    def test_exhaustive_complete_answers(self):
        self.require_bounded_selector_exists()
        subsets = (set(), {0}, {1}, {0, 1})
        with tempfile.TemporaryDirectory() as directory:
            for case, sets in enumerate(itertools.product(subsets, repeat=3)):
                targets = list(zip((9, -3, 2), sets))
                snapshot = self.reference.Snapshot(self.reference.write_normalized_target_fixture(Path(directory) / str(case), targets))
                for values, sid, k in itertools.product(subsets, (None, -3), (0, 1, 2, 5)):
                    self.check_complete_similarity_answer(snapshot, targets, values, sid, k)

    def test_random_complete_answers(self):
        self.require_bounded_selector_exists()
        rng = random.Random(920405)
        with tempfile.TemporaryDirectory() as directory:
            for case in range(36):
                ids = list(range(-5, case % 23 - 5))
                rng.shuffle(ids)
                targets = [(node, {f for f in range(12) if rng.randrange(4) == 0}) for node in ids]
                snapshot = self.reference.Snapshot(self.reference.write_normalized_target_fixture(Path(directory) / str(case), targets))
                for query in range(8):
                    values = {f for f in range(15) if rng.randrange(3) == 0}
                    sid = rng.choice(ids) if ids and query % 2 else None
                    self.check_complete_similarity_answer(snapshot, targets, values, sid, (0, 1, 3, 50)[query % 4])

    def test_single_seed_descendants(self):
        self.require_bounded_selector_exists()
        targets = [(node, {0}) for node in (90, 1, 80, 2, 70, 3, 60, 4)] + [(0, set())]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = self.reference.Snapshot(self.reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            rows, metrics = self.check_complete_similarity_answer(snapshot, targets, {0}, None, 4, heap_cap=7)
            self.assertEqual([row[0] for row in rows], [1, 2, 3, 4])
            self.assertGreater(metrics["child_rmq_calls"], 0)

    def test_ties_zero_self(self):
        self.require_bounded_selector_exists()
        with tempfile.TemporaryDirectory() as directory:
            cases = [([(9, {0}), (1, {0, 1, 2, 3})], {0, 1}, None, 1),
                     ([(9, {2}), (1, set()), (-3, {1})], {7}, None, 2),
                     ([(0, {0}), (9, {0, 1}), (2, {0, 2})], {0}, 0, 2)]
            for case, (targets, values, sid, k) in enumerate(cases):
                snapshot = self.reference.Snapshot(self.reference.write_normalized_target_fixture(Path(directory) / str(case), targets))
                self.check_complete_similarity_answer(snapshot, targets, values, sid, k)

    def test_refusals_clean_output(self):
        solve = self.require_bounded_selector_exists()
        targets = [(9, {0}), (1, {0, 1}), (2, set())]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = self.reference.Snapshot(self.reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            source = snapshot.root / "source"
            self.reference.write_normalized_source_file(source, {0})
            for options in ({"heap_cap": 0}, {"output_cap": 0}, {"disk_cap": 0}, {"source_epoch": 99}):
                with self.subTest(options=options), self.assertRaises(AssertionError):
                    solve(self.reference, snapshot, source, None, 2, **options)
                self.assertFalse(list(snapshot.root.glob("query-*")))
            with self.assertRaisesRegex(OSError, "sink"):
                solve(self.reference, snapshot, source, None, 2, fail_after=1)
            self.assertFalse(list(snapshot.root.glob("query-*")))
            snapshot.tree_token = (99,)
            with self.assertRaises(AssertionError):
                solve(self.reference, snapshot, source, None, 2)

    def test_streaming_scan_control(self):
        solve = getattr(candidate, "scan_exact_similarity_targets", None)
        self.assertTrue(callable(solve), "scan_exact_similarity_targets")
        targets = [(9, {0, 2}), (-3, {1}), (2, set()), (7, {0, 1})]
        with tempfile.TemporaryDirectory() as directory:
            snapshot = self.reference.Snapshot(self.reference.write_normalized_target_fixture(Path(directory) / "index", targets))
            source = snapshot.root / "source"
            for values, sid, k in itertools.product((set(), {0}, {0, 1}), (None, 9), (0, 1, 9)):
                self.reference.write_normalized_source_file(source, values)
                rows, metrics = solve(self.reference, snapshot, source, sid, k)
                self.assertEqual(rows, compute_finite_similarity_oracle(targets, values, sid, k))
                self.assertLessEqual(metrics["heap_peak"], min(k, len(targets)))
                self.assertEqual(metrics["source_peak"], len(values) if k else 0)
                self.assertFalse(list(snapshot.root.glob("query-*")))

    def test_public_graph_normalization(self):
        module = __import__("bench_bounded_seed_similarity") if importlib.util.find_spec("bench_bounded_seed_similarity") else types.SimpleNamespace()
        parse = getattr(module, "read_undirected_graph_source", None)
        self.assertTrue(callable(parse), "read_undirected_graph_source")
        graph, metrics = parse(iter(["# graph\n", "1 1\n", "2 3\n", "3 2\n", "2 3\n", "\n"]))
        self.assertEqual(graph, {1: set(), 2: {3}, 3: {2}})
        self.assertEqual(metrics, {"raw_rows": 4, "self_loop_rows": 1, "duplicate_rows": 2,
                                   "nodes": 3, "edges": 1, "memberships": 2})


if __name__ == "__main__":
    unittest.main()
