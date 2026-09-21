"""Keep a pinned SQLite source reusable after refused or cancelled work."""

from pathlib import Path
import sqlite3
import struct
import tempfile
import unittest
import weakref
from unittest.mock import patch

from boolean_rank_sqlite_source import build_boolean_rank_source, SqliteBooleanRankSource
from boolean_rank_output_certificate import certify_boolean_rank_output
from boolean_rank_pipeline import publish_boolean_rank_result
from stream_boolean_rank_solver import solve_boolean_rank_state, iterate_boolean_rank_output


ROWS = [(0, (0,), 1.0), (1, (0,), 3.0), (2, (1,), 1.0), (3, (1,), 2.0)]


class BooleanRankFailureLifecycleTests(unittest.TestCase):
    def test_solver_vectors_released(self):
        references = []

        def capture_solver_vector_references(*args, **kwargs):
            state = solve_boolean_rank_state(*args, **kwargs)
            references.extend(weakref.ref(state[key]) for key in ("factor_scores", "class_scores"))
            return state

        def verify_released_solver_vectors(*args, **kwargs):
            self.assertTrue(all(reference() is None for reference in references))
            return certify_boolean_rank_output(*args, **kwargs)

        with tempfile.TemporaryDirectory() as directory:
            path, output = Path(directory) / "source.sqlite", Path(directory) / "result.ranks"
            build_boolean_rank_source(path, ROWS, factor_count=2)
            with SqliteBooleanRankSource(path) as source:
                with patch("boolean_rank_pipeline.solve_boolean_rank_state", capture_solver_vector_references), \
                        patch("boolean_rank_output_certificate.certify_boolean_rank_output", verify_released_solver_vectors):
                    publish_boolean_rank_result(source, output, alpha=0.85, epsilon=1e-10,
                        method="class-cg", max_factor_slots=2, max_class_slots=2)

    def test_negative_identifier_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            for identifier in (-1, -2**63):
                with self.subTest(identifier=identifier):
                    path = Path(directory) / str(identifier)
                    with self.assertRaisesRegex(ValueError, "nonnegative"):
                        build_boolean_rank_source(path, [(identifier, (), 1.0)], factor_count=0)
                    self.assertFalse(path.exists())

    def test_cancelled_lift_closes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite"
            build_boolean_rank_source(path, ROWS, factor_count=2)
            with SqliteBooleanRankSource(path) as source:
                state = solve_boolean_rank_state(source, alpha=0.85, method="factor-cg",
                                                 max_factor_slots=2, max_class_slots=0)
                for _ in range(12):
                    values = iterate_boolean_rank_output(source, state)
                    next(values)
                    values.close()
                    self.assertEqual(source.events["active_cursors"], 0)

    def test_malformed_certificate_closes(self):
        with tempfile.TemporaryDirectory() as directory:
            path, output = Path(directory) / "source.sqlite", Path(directory) / "bad.ranks"
            build_boolean_rank_source(path, ROWS, factor_count=2)
            output.write_bytes(b"".join(struct.pack("<Qd", identifier + 10, 0.25)
                                        for identifier, _, _ in ROWS))
            with SqliteBooleanRankSource(path) as source:
                for _ in range(12):
                    with self.assertRaisesRegex(ValueError, "original ID"):
                        certify_boolean_rank_output(source, output, alpha=0.85, epsilon=1e-10,
                                                     max_factor_slots=2, scratch_dir=directory)
                    self.assertEqual(source.events["active_cursors"], 0)
                self.assertEqual(len(list(Path(directory).iterdir())), 2)

    def test_failed_writer_closes(self):
        with tempfile.TemporaryDirectory() as directory:
            path, output = Path(directory) / "source.sqlite", Path(directory) / "result.ranks"
            build_boolean_rank_source(path, ROWS, factor_count=2)
            with SqliteBooleanRankSource(path) as source:
                with patch("boolean_rank_pipeline.struct.pack", side_effect=OSError("injected writer")):
                    with self.assertRaisesRegex(OSError, "injected writer"):
                        publish_boolean_rank_result(source, output, alpha=0.85, epsilon=1e-10,
                            method="factor-cg", max_factor_slots=2, max_class_slots=0)
                self.assertEqual(source.events["active_cursors"], 0)
                self.assertFalse(output.exists())
                self.assertEqual(len(list(Path(directory).iterdir())), 1)

    def test_numerical_refusal_closes(self):
        with tempfile.TemporaryDirectory() as directory:
            for case, statement in enumerate((
                    "UPDATE classes SET h=9007199254740993 WHERE class_id=0",
                    "UPDATE factors SET active_member_count=9007199254740993 WHERE factor_id=0")):
                path = Path(directory) / f"{case}.sqlite"
                build_boolean_rank_source(path, ROWS, factor_count=2)
                connection = sqlite3.connect(path)
                connection.execute(statement)
                connection.commit()
                connection.close()
                with SqliteBooleanRankSource(path) as source:
                    with self.assertRaises(ValueError):
                        solve_boolean_rank_state(source, alpha=0.85, method="factor-cg",
                                                 max_factor_slots=2, max_class_slots=0)
                    self.assertEqual(source.events["active_cursors"], 0)


if __name__ == "__main__":
    unittest.main()
