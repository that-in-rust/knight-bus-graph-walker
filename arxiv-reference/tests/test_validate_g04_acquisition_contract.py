#!/usr/bin/env python3
"""RED-first tests for bounded G04 acquisition and deterministic parsing."""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import io
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from xml.sax.saxutils import escape

from pypdf import PdfWriter


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_ROOT = REPOSITORY_ROOT / "arxiv-reference"
PIPELINE_PATH = REFERENCE_ROOT / "tools" / "g04_acquisition_pipeline.py"
LEDGER_PATH = REFERENCE_ROOT / "sources" / "download-ledger.tsv"
VALIDATOR_PATH = REFERENCE_ROOT / "tools" / "validate_arxiv_corpus_contract.py"

spec = importlib.util.spec_from_file_location("g04_acquisition_pipeline", PIPELINE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("cannot load G04 acquisition pipeline")
pipeline = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = pipeline
spec.loader.exec_module(pipeline)

validator_spec = importlib.util.spec_from_file_location(
    "g04_corpus_validator", VALIDATOR_PATH
)
if validator_spec is None or validator_spec.loader is None:
    raise RuntimeError("cannot load G04 corpus validator")
validator = importlib.util.module_from_spec(validator_spec)
sys.modules[validator_spec.name] = validator
validator_spec.loader.exec_module(validator)


def create_valid_pdf_bytes() -> bytes:
    """Create one deterministic blank-page PDF fixture in memory."""

    output = io.BytesIO()
    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    writer.write(output)
    return output.getvalue()


def create_terminal_ledger_row(rank: int, paper_id: str) -> dict[str, str]:
    """Create one complete acquired-row fixture."""

    source_checksum = "A" * 64
    extracted_checksum = "B" * 64
    trace_checksum = "C" * 64
    return {
        "request_id": f"REQ-G04-{rank:04d}",
        "goal_id": "G04",
        "queue_rank": str(rank),
        "paper_id": paper_id,
        "source_service": "ARXIV",
        "retrieval_uri": "https://arxiv.org/pdf/2401.00001",
        "accessed_at_utc": "2026-08-11T00:00:00Z",
        "response_status": "HTTP_200",
        "media_type": "application/pdf",
        "content_length_bytes": "100",
        "source_checksum": source_checksum,
        "local_path": f"sources/papers/{paper_id}.pdf",
        "license_uri": "https://creativecommons.org/licenses/by/4.0/",
        "license_state": "LICENSE_PERMISSIVE_VERIFIED",
        "acquisition_status": "ACQUIRED",
        "attempt_count": "1",
        "retry_events": "NONE",
        "rate_limit_events": "NONE",
        "policy_url": "https://info.arxiv.org/help/api/tou.html",
        "policy_checked_date": "2026-08-11",
        "cache_status": "MISS",
        "trace_path": f"cache/g04/traces/{paper_id}.json",
        "trace_checksum": trace_checksum,
        "parser_name": "pypdf",
        "parser_version": "6.14.2",
        "parser_options": "layout;page-separator=LF-FF-LF",
        "page_count": "1",
        "extracted_path": f"cache/g04/text/{paper_id}.txt",
        "extracted_checksum": extracted_checksum,
        "parse_status": "PARSED",
        "terminal_reason": "ACQUIRED_AND_PARSED",
    }


def create_arxiv_metadata_feed(queue: list[dict[str, object]]) -> bytes:
    """Create one exact Atom fixture for all queue arXiv identities."""

    entries: list[str] = []
    for record in queue:
        arxiv_id = str(record["arxiv_id"])
        if arxiv_id == "UNKNOWN":
            continue
        version = str(record["canonical_version"])
        versioned_id = arxiv_id + (version if version.startswith("v") else "v1")
        title = escape(str(record["title"]))
        pdf_url = "https://arxiv.org/pdf/" + versioned_id
        entries.append(
            """<entry>
<id>https://arxiv.org/abs/{versioned_id}</id>
<updated>2026-08-11T00:00:00Z</updated>
<published>2026-08-11T00:00:00Z</published>
<title>{title}</title>
<summary>Mechanical fixture only.</summary>
<author><name>Fixture Author</name></author>
<link href="https://arxiv.org/abs/{versioned_id}" rel="alternate" type="text/html"/>
<link title="pdf" href="{pdf_url}" rel="related" type="application/pdf"/>
<arxiv:license>http://arxiv.org/licenses/nonexclusive-distrib/1.0/</arxiv:license>
</entry>""".format(versioned_id=versioned_id, title=title, pdf_url=pdf_url)
        )
    return (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<feed xmlns=\"http://www.w3.org/2005/Atom\" "
        "xmlns:arxiv=\"http://arxiv.org/schemas/atom\">\n"
        + "\n".join(entries)
        + "\n</feed>\n"
    ).encode("utf-8")


def create_openalex_metadata_payload(queue: list[dict[str, object]]) -> bytes:
    """Create exact DOI fixture rows with no acceptable full-text locations."""

    results = [
        {
            "doi": "https://doi.org/" + str(record["doi"]),
            "title": record["title"],
            "best_oa_location": None,
            "locations": [],
        }
        for record in queue
        if record["arxiv_id"] == "UNKNOWN" and record["doi"] != "UNKNOWN"
    ]
    return json.dumps({"results": results}, sort_keys=True).encode("utf-8")


def copy_reference_fixture_tree() -> tuple[tempfile.TemporaryDirectory[str], Path]:
    """Copy the corpus contract without ignored acquisition artifacts."""

    temporary_directory = tempfile.TemporaryDirectory()
    copied_root = Path(temporary_directory.name) / "arxiv-reference"
    shutil.copytree(
        REFERENCE_ROOT,
        copied_root,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "cache", "papers"),
    )
    queue = pipeline.derive_exact_queue_records(copied_root)
    manifest_path = copied_root / "sources" / "paper-manifest.tsv"
    manifest_rows = pipeline.read_tsv_rows_exact(manifest_path)
    pipeline.write_tsv_rows_atomic(
        manifest_path,
        tuple(manifest_rows[0]),
        pipeline.project_manifest_before_g04(manifest_rows, queue),
    )
    pipeline.write_tsv_rows_atomic(
        copied_root / "sources" / "download-ledger.tsv",
        pipeline.DOWNLOAD_LEDGER_FIELDS,
        [],
    )
    report_path = copied_root / "sources" / "G04-acquisition-parsing-report.md"
    if report_path.exists():
        report_path.unlink()
    return temporary_directory, copied_root


class ValidateG04AcquisitionContractTests(unittest.TestCase):
    def test_exact_queue_derivation(self) -> None:
        queue = pipeline.derive_exact_queue_records(REFERENCE_ROOT)

        self.assertEqual(len(queue), 50)
        self.assertEqual(len({record["paper_id"] for record in queue}), 50)
        self.assertEqual([record["queue_rank"] for record in queue], list(range(1, 51)))
        self.assertTrue(all(record["basis"] == "G02_SEED" for record in queue[:25]))
        self.assertTrue(all(record["basis"] == "G03_ACQUIRE" for record in queue[25:]))

    def test_duplicate_queue_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            copied_root = Path(directory) / "arxiv-reference"
            shutil.copytree(REFERENCE_ROOT, copied_root, ignore=shutil.ignore_patterns("cache"))
            report_path = copied_root / "sources" / "G03-citation-ancestry-report.md"
            report = report_path.read_text(encoding="utf-8")
            report = report.replace("PAPER-2101.12631", "PAPER-HASH-0232e71ded2b5c43")
            report_path.write_text(report, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "unique|duplicate"):
                pipeline.derive_exact_queue_records(copied_root)

    def test_canonical_filename_generation(self) -> None:
        self.assertEqual(
            pipeline.build_canonical_paper_filename("PAPER-1905.04264"),
            "PAPER-1905.04264.pdf",
        )
        self.assertEqual(
            pipeline.build_canonical_paper_filename("PAPER-HASH-0123456789abcdef"),
            "PAPER-HASH-0123456789abcdef.pdf",
        )

    def test_safe_local_paths(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            accepted = pipeline.validate_safe_local_path(
                root, "sources/papers/PAPER-1905.04264.pdf"
            )
            self.assertTrue(accepted.is_relative_to(root))
            for value in (
                "../escape.pdf",
                "sources/papers/../../escape.pdf",
                "/tmp/escape.pdf",
                "sources/papers/PAPER%2fescape.pdf",
                "sources/papers/PAPER\\escape.pdf",
            ):
                with self.subTest(value=value):
                    with self.assertRaises(ValueError):
                        pipeline.validate_safe_local_path(root, value)

    def test_download_header_exact(self) -> None:
        self.assertEqual(
            LEDGER_PATH.read_text(encoding="utf-8").splitlines()[0],
            pipeline.DOWNLOAD_LEDGER_HEADER,
        )

    def test_terminal_ledger_provenance(self) -> None:
        queue = pipeline.derive_exact_queue_records(REFERENCE_ROOT)
        rows = [
            create_terminal_ledger_row(record["queue_rank"], record["paper_id"])
            for record in queue
        ]
        self.assertEqual(pipeline.validate_download_ledger_rows(rows, queue), [])

        rows[-1]["trace_checksum"] = "NOT_AVAILABLE"
        errors = pipeline.validate_download_ledger_rows(rows, queue)
        self.assertTrue(any("trace_checksum" in error for error in errors))

        rows = [
            create_terminal_ledger_row(record["queue_rank"], record["paper_id"])
            for record in queue
        ]
        rows[-1].update(
            {
                "acquisition_status": "AUTHORIZATION_FAILED",
                "parse_status": "NOT_APPLICABLE",
                "local_path": "NOT_ACQUIRED",
                "source_checksum": "NOT_AVAILABLE",
                "content_length_bytes": "0",
                "trace_path": "NOT_AVAILABLE",
                "trace_checksum": "NOT_AVAILABLE",
                "parser_name": "NOT_APPLICABLE",
                "parser_version": "NOT_APPLICABLE",
                "parser_options": "NOT_APPLICABLE",
                "page_count": "0",
                "extracted_path": "NOT_AVAILABLE",
                "extracted_checksum": "NOT_AVAILABLE",
                "source_pdf_checksum": "NOT_AVAILABLE",
                "terminal_reason": "HTTP_403",
            }
        )
        errors = pipeline.validate_download_ledger_rows(rows, queue)
        self.assertTrue(any("attempted request requires trace_path" in error for error in errors))
        self.assertTrue(any("trace_checksum" in error for error in errors))

    def test_exact_license_state(self) -> None:
        self.assertEqual(
            pipeline.classify_license_state_uri(
                "https://creativecommons.org/licenses/by/4.0/", acquired=True
            ),
            "LICENSE_PERMISSIVE_VERIFIED",
        )
        self.assertEqual(
            pipeline.classify_license_state_uri(
                "http://arxiv.org/licenses/nonexclusive-distrib/1.0/", acquired=True
            ),
            "LICENSE_RESTRICTED_OR_CONDITIONAL",
        )
        self.assertEqual(
            pipeline.classify_license_state_uri("NOT_DISCOVERED", acquired=True),
            "LICENSE_UNKNOWN",
        )
        self.assertEqual(
            pipeline.classify_license_state_uri("NOT_DISCOVERED", acquired=False),
            "LICENSE_UNAVAILABLE",
        )

    def test_invalid_payload_rejection(self) -> None:
        html = b"<html><body>authentication required</body></html>"
        with self.assertRaisesRegex(ValueError, "PDF|HTML"):
            pipeline.validate_pdf_payload_bytes(html, "text/html", 100 * 1024 * 1024)

    def test_truncated_payload_rejection(self) -> None:
        payload = create_valid_pdf_bytes()
        with self.assertRaisesRegex(ValueError, "truncated|EOF|PDF"):
            pipeline.validate_pdf_payload_bytes(
                payload[:-16], "application/pdf", 100 * 1024 * 1024
            )

    def test_deterministic_text_extraction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pdf_path = root / "paper.pdf"
            first_path = root / "first.txt"
            second_path = root / "second.txt"
            pdf_path.write_bytes(create_valid_pdf_bytes())

            first = pipeline.extract_pdf_text_deterministic(pdf_path, first_path)
            second = pipeline.extract_pdf_text_deterministic(pdf_path, second_path)

            self.assertEqual(first_path.read_bytes(), second_path.read_bytes())
            self.assertEqual(first["extracted_checksum"], second["extracted_checksum"])
            self.assertEqual(first["page_count"], 1)
            self.assertEqual(first["parser_version"], "6.14.2")

    def test_valid_cache_reuse(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pdf_path = root / "paper.pdf"
            text_path = root / "paper.txt"
            pdf_path.write_bytes(create_valid_pdf_bytes())
            text_path.write_text("fixture\n", encoding="utf-8")
            pdf_checksum = hashlib.sha256(pdf_path.read_bytes()).hexdigest().upper()
            text_checksum = hashlib.sha256(text_path.read_bytes()).hexdigest().upper()

            self.assertTrue(
                pipeline.verify_cached_artifact_checksums(
                    pdf_path, pdf_checksum, text_path, text_checksum
                )
            )

    def test_corrupt_cache_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pdf_path = root / "paper.pdf"
            text_path = root / "paper.txt"
            pdf_path.write_bytes(create_valid_pdf_bytes())
            text_path.write_text("fixture\n", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "checksum"):
                pipeline.verify_cached_artifact_checksums(
                    pdf_path, "0" * 64, text_path, "1" * 64
                )

    def test_retry_budget_enforcement(self) -> None:
        attempts: list[int] = []

        def fail_then_succeed(attempt: int) -> tuple[int, bytes, dict[str, str]]:
            attempts.append(attempt)
            if attempt < 3:
                raise pipeline.RetryableRequestError("fixture transport failure")
            return 200, create_valid_pdf_bytes(), {"content-type": "application/pdf"}

        result = pipeline.execute_retry_operation_bounded(
            fail_then_succeed, sleep_function=lambda _seconds: None, maximum_attempts=3
        )
        self.assertEqual(result[0], 200)
        self.assertEqual(attempts, [1, 2, 3])

    def test_service_stop_enforcement(self) -> None:
        attempts: list[int] = []

        def authorization_failure(attempt: int) -> tuple[int, bytes, dict[str, str]]:
            attempts.append(attempt)
            raise pipeline.ServiceStopError("HTTP 403")

        with self.assertRaisesRegex(pipeline.ServiceStopError, "403"):
            pipeline.execute_retry_operation_bounded(
                authorization_failure,
                sleep_function=lambda _seconds: None,
                maximum_attempts=3,
            )
        self.assertEqual(attempts, [1])

    def test_http_status_persists_host_stop(self) -> None:
        request = {
            "service": "PUBLISHER",
            "url": "https://publisher.example/paper.pdf",
            "maximum_bytes": 1024,
        }
        state: dict[str, object] = {"campaign_requests": 0, "invocation_requests": 0}
        calls = {"count": 0}

        def forbidden_response(_request: dict[str, object]) -> dict[str, object]:
            calls["count"] += 1
            return {
                "status_code": 403,
                "body": b"",
                "headers": {"content-type": "text/html"},
                "final_url": request["url"],
            }

        first, first_attempts = pipeline.perform_bounded_request_operation(
            request,
            state,
            forbidden_response,
            lambda _seconds: None,
            lambda: "2026-08-11T00:00:00Z",
        )
        second, second_attempts = pipeline.perform_bounded_request_operation(
            request,
            state,
            forbidden_response,
            lambda _seconds: None,
            lambda: "2026-08-11T00:00:01Z",
        )

        self.assertEqual(first["status_code"], 403)
        self.assertEqual(len(first_attempts), 1)
        self.assertEqual(second["status_code"], -1)
        self.assertEqual(second_attempts, [])
        self.assertEqual(calls["count"], 1)

        rate_state: dict[str, object] = {"campaign_requests": 0, "invocation_requests": 0}
        rate_calls = {"count": 0}

        def limited_response(_request: dict[str, object]) -> dict[str, object]:
            rate_calls["count"] += 1
            return {
                "status_code": 429,
                "body": b"",
                "headers": {"retry-after": "0"},
                "final_url": request["url"],
            }

        exhausted, exhausted_attempts = pipeline.perform_bounded_request_operation(
            request,
            rate_state,
            limited_response,
            lambda _seconds: None,
            lambda: "2026-08-11T00:00:02Z",
        )
        stopped, stopped_attempts = pipeline.perform_bounded_request_operation(
            request,
            rate_state,
            limited_response,
            lambda _seconds: None,
            lambda: "2026-08-11T00:00:03Z",
        )
        self.assertEqual(exhausted["status_code"], 429)
        self.assertEqual(len(exhausted_attempts), 3)
        self.assertEqual(stopped["status_code"], -1)
        self.assertEqual(stopped_attempts, [])
        self.assertEqual(rate_calls["count"], 3)
        self.assertEqual(
            pipeline.derive_stopped_request_hosts(
                [
                    {
                        "retrieval_uri": request["url"],
                        "acquisition_status": "AUTHORIZATION_FAILED",
                    },
                    {
                        "retrieval_uri": "https://limited.example/paper.pdf",
                        "acquisition_status": "RATE_LIMITED",
                    },
                ]
            ),
            ["limited.example", "publisher.example"],
        )

    def test_retry_after_http_date_honored(self) -> None:
        request = {
            "service": "OPENALEX",
            "url": "https://api.openalex.org/works",
            "maximum_bytes": 1024,
        }
        responses = iter(
            [
                {
                    "status_code": 429,
                    "body": b"",
                    "headers": {"retry-after": "Tue, 11 Aug 2026 00:01:00 GMT"},
                    "final_url": request["url"],
                },
                {
                    "status_code": 200,
                    "body": b"{}",
                    "headers": {"content-type": "application/json"},
                    "final_url": request["url"],
                },
            ]
        )
        sleeps: list[float] = []
        response, attempts = pipeline.perform_bounded_request_operation(
            request,
            {"campaign_requests": 0, "invocation_requests": 0},
            lambda _request: next(responses),
            sleeps.append,
            lambda: "2026-08-11T00:00:00Z",
        )

        self.assertEqual(response["status_code"], 200)
        self.assertEqual(len(attempts), 2)
        self.assertIn(60.0, sleeps)

    def test_arxiv_request_compilation(self) -> None:
        request = pipeline.build_arxiv_metadata_request(["1905.04264", "2401.01019"])
        self.assertEqual(request["service"], "ARXIV")
        self.assertIn("id_list=1905.04264%2C2401.01019", request["url"])
        self.assertNotIn("search_query", request["url"])

    def test_openalex_request_compilation(self) -> None:
        request = pipeline.build_openalex_location_request(
            ["10.1145/3331446", "10.4230/LIPIcs.ICDT.2018.19"]
        )
        self.assertEqual(request["service"], "OPENALEX")
        self.assertIn("doi:", request["parameters"]["filter"])
        self.assertNotIn("search", request["parameters"])
        self.assertEqual(request["parameters"]["per_page"], "100")

    def test_selected_manifest_gate(self) -> None:
        queue = pipeline.derive_exact_queue_records(REFERENCE_ROOT)
        with (REFERENCE_ROOT / "sources" / "paper-manifest.tsv").open(
            encoding="utf-8", newline=""
        ) as handle:
            manifest = list(csv.DictReader(handle, delimiter="\t"))
        updated = pipeline.mark_selected_papers_deep(manifest, queue)
        queue_ids = {record["paper_id"] for record in queue}
        selected = [row for row in updated if row["paper_id"] in queue_ids]

        self.assertEqual(len(selected), 50)
        self.assertTrue(all(row["selection_status"] == "DEEP_READ" for row in selected))
        self.assertTrue(
            all(
                row["selection_status"] != "DEEP_READ"
                for row in updated
                if row["paper_id"] not in queue_ids
            )
        )

    def test_actual_manifest_selection(self) -> None:
        queue = pipeline.derive_exact_queue_records(REFERENCE_ROOT)
        with (REFERENCE_ROOT / "sources" / "paper-manifest.tsv").open(
            encoding="utf-8", newline=""
        ) as handle:
            manifest = list(csv.DictReader(handle, delimiter="\t"))
        ledger = pipeline.read_tsv_rows_exact(LEDGER_PATH)

        self.assertEqual(len(ledger), 50)
        self.assertEqual(
            pipeline.validate_g04_manifest_rows(
                manifest,
                ledger,
                queue,
                require_complete=True,
                allow_read_complete=True,
            ),
            [],
        )

    def test_full_validator_acceptance(self) -> None:
        result = subprocess.run(
            [sys.executable, str(VALIDATOR_PATH), "--root", str(REFERENCE_ROOT)],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_completion_requires_review_artifact(self) -> None:
        self.assertIn(
            "governance/reviews/G04-adversarial-review.md",
            validator.G04_OPTIONAL_FILE_PATHS,
        )
        complete_status = "- Completion state: `COMPLETE`\n"
        incomplete_review = "- Fifth-pass verdict: `PENDING`\n"
        self.assertTrue(
            validator.validate_g04_review_clearance(
                complete_status, incomplete_review
            )
        )
        cleared_review = "\n".join(
            (
                "- Fifth-pass verdict: `CLEARED`",
                "**Unresolved findings: P0=0, P1=0, P2=0.**",
                "G04 is **CLEARED**.",
            )
        )
        self.assertEqual(
            validator.validate_g04_review_clearance(
                complete_status, cleared_review
            ),
            [],
        )

    def test_journal_header_matches_latest_checkpoint(self) -> None:
        journal = (REFERENCE_ROOT / "journals" / "G04-progress.md").read_text(
            encoding="utf-8"
        )
        self.assertEqual(validator.validate_goal_journal_shape(journal, "G04"), [])

        phase_line = next(
            line for line in journal.splitlines() if line.startswith("- Current Phase: ")
        )
        replacement_phase = "Red" if not phase_line.endswith("Red") else "Green"
        stale_phase = journal.replace(
            phase_line, "- Current Phase: " + replacement_phase, 1
        )
        self.assertTrue(
            any(
                "header Current Phase does not match latest session" in error
                for error in validator.validate_goal_journal_shape(stale_phase, "G04")
            )
        )

        updated_line = next(
            line for line in journal.splitlines() if line.startswith("- Updated: ")
        )
        stale_time = journal.replace(updated_line, "- Updated: 2000-01-01 00:00:00Z", 1)
        self.assertTrue(
            any(
                "header Updated does not match latest session" in error
                for error in validator.validate_goal_journal_shape(stale_time, "G04")
            )
        )

    def test_preexisting_worktree_isolated(self) -> None:
        packet = (REFERENCE_ROOT / "governance" / "G04-goal-packet.md").read_text(
            encoding="utf-8"
        )
        self.assertFalse(
            validator.is_g04_owned_worktree_path(
                "arxiv-reference/sources/G03-citation-ancestry-report.md"
            )
        )
        self.assertTrue(
            validator.is_g04_owned_worktree_path(
                "arxiv-reference/sources/download-ledger.tsv"
            )
        )
        g04_scope_errors = validator.validate_g04_worktree_scope(REFERENCE_ROOT, packet)
        self.assertTrue(g04_scope_errors)
        self.assertTrue(
            all("outside G04 ownership" in error for error in g04_scope_errors),
            g04_scope_errors,
        )
        self.assertEqual(validator.validate_g05_worktree_scope(REFERENCE_ROOT), [])

        undisclosed = packet.replace("- `AGENTS.md`", "- `MISSING.md`", 1)
        self.assertTrue(
            any(
                "AGENTS.md" in error and "not declared" in error
                for error in validator.validate_g04_worktree_scope(
                    REFERENCE_ROOT, undisclosed
                )
            )
        )

    def test_nonqueue_acquisition_rejection(self) -> None:
        queue = pipeline.derive_exact_queue_records(REFERENCE_ROOT)
        rows = [
            create_terminal_ledger_row(record["queue_rank"], record["paper_id"])
            for record in queue
        ]
        rows[-1]["paper_id"] = "PAPER-HASH-0000000000000000"

        errors = pipeline.validate_download_ledger_rows(rows, queue)
        self.assertTrue(any("queue" in error or "paper_id" in error for error in errors))

    def test_unavailable_identity_preservation(self) -> None:
        queue = pipeline.derive_exact_queue_records(REFERENCE_ROOT)
        row = pipeline.build_unavailable_ledger_row(
            queue[0],
            acquisition_status="UNAVAILABLE",
            terminal_reason="NO_ACCEPTABLE_DIRECT_SOURCE",
        )

        self.assertEqual(row["paper_id"], queue[0]["paper_id"])
        self.assertEqual(row["license_state"], "LICENSE_UNAVAILABLE")
        self.assertEqual(row["local_path"], "NOT_ACQUIRED")
        self.assertEqual(row["parse_status"], "NOT_APPLICABLE")

    def test_semantic_boundary_enforcement(self) -> None:
        queue = pipeline.derive_exact_queue_records(REFERENCE_ROOT)
        rows = [
            create_terminal_ledger_row(record["queue_rank"], record["paper_id"])
            for record in queue
        ]
        rows[0]["parse_status"] = "READ_COMPLETE"
        errors = pipeline.validate_download_ledger_rows(rows, queue)
        self.assertTrue(any("parse_status" in error for error in errors))

    def test_trace_checksum_validation(self) -> None:
        trace = {
            "paper_id": "PAPER-1905.04264",
            "attempts": [{"attempt": 1, "status": "HTTP_200"}],
        }
        first = pipeline.serialize_request_trace_bytes(trace)
        second = pipeline.serialize_request_trace_bytes(json.loads(first))
        self.assertEqual(first, second)
        self.assertEqual(
            hashlib.sha256(first).hexdigest().upper(),
            hashlib.sha256(second).hexdigest().upper(),
        )

    def test_arxiv_metadata_identity_parsing(self) -> None:
        queue = pipeline.derive_exact_queue_records(REFERENCE_ROOT)
        parsed = pipeline.parse_arxiv_metadata_entries(
            create_arxiv_metadata_feed(queue), queue
        )

        expected_ids = {
            str(record["paper_id"])
            for record in queue
            if record["arxiv_id"] != "UNKNOWN"
        }
        self.assertEqual(set(parsed), expected_ids)
        self.assertTrue(
            all(record["pdf_url"].startswith("https://arxiv.org/pdf/") for record in parsed.values())
        )
        self.assertTrue(
            all(record["license_uri"].startswith("http://arxiv.org/licenses/") for record in parsed.values())
        )
        self.assertTrue(all(record["title_match"] == "TRUE" for record in parsed.values()))

    def test_arxiv_strong_identity_drift(self) -> None:
        queue = pipeline.derive_exact_queue_records(REFERENCE_ROOT)
        payload = create_arxiv_metadata_feed(queue)
        original_title = escape(str(queue[0]["title"])).encode("utf-8")
        payload = payload.replace(original_title, b"Official title variant", 1)

        parsed = pipeline.parse_arxiv_metadata_entries(payload, queue)

        self.assertEqual(parsed[str(queue[0]["paper_id"])]["title_match"], "FALSE")
        self.assertEqual(
            parsed[str(queue[0]["paper_id"])]["observed_title"],
            "Official title variant",
        )

    def test_openalex_official_location_filtering(self) -> None:
        queue = pipeline.derive_exact_queue_records(REFERENCE_ROOT)
        doi_records = [record for record in queue if record["arxiv_id"] == "UNKNOWN" and record["doi"] != "UNKNOWN"]
        accepted = next(record for record in doi_records if record["doi"] == "10.1145/3564593")
        rejected = next(record for record in doi_records if record["paper_id"] != accepted["paper_id"])
        payload = json.dumps(
            {
                "results": [
                    {
                        "doi": "https://doi.org/" + str(accepted["doi"]),
                        "title": accepted["title"],
                        "best_oa_location": {
                            "is_oa": True,
                            "pdf_url": "https://dl.acm.org/doi/pdf/10.1145/3564593",
                            "landing_page_url": "https://doi.org/10.1145/3564593",
                            "source": {"type": "journal", "display_name": "Fixture Journal"},
                        },
                        "locations": [],
                    },
                    {
                        "doi": "https://doi.org/" + str(rejected["doi"]),
                        "title": rejected["title"],
                        "best_oa_location": {
                            "is_oa": True,
                            "pdf_url": "https://repository.example/paper.pdf",
                            "landing_page_url": "https://repository.example/paper",
                            "source": {"type": "repository", "display_name": "Fixture Repository"},
                        },
                        "locations": [],
                    },
                ]
            },
            sort_keys=True,
        ).encode("utf-8")

        parsed = pipeline.parse_openalex_location_entries(payload, queue)
        self.assertEqual(set(parsed), {accepted["paper_id"]})
        self.assertEqual(parsed[str(accepted["paper_id"])]["source_service"], "PUBLISHER")

        mislabeled = json.loads(payload)
        mislabeled["results"] = [mislabeled["results"][0]]
        mislabeled["results"][0]["best_oa_location"]["pdf_url"] = (
            "https://repository.example/paper.pdf"
        )
        parsed = pipeline.parse_openalex_location_entries(
            json.dumps(mislabeled, sort_keys=True).encode("utf-8"), queue
        )
        self.assertEqual(parsed, {})

    def test_openalex_duplicate_doi_conflict(self) -> None:
        queue = pipeline.derive_exact_queue_records(REFERENCE_ROOT)
        record = next(
            value
            for value in queue
            if value["arxiv_id"] == "UNKNOWN" and value["doi"] != "UNKNOWN"
        )
        payload = json.dumps(
            {
                "results": [
                    {
                        "doi": "https://doi.org/" + str(record["doi"]),
                        "title": record["title"],
                        "best_oa_location": None,
                        "locations": [],
                    },
                    {
                        "doi": "https://doi.org/" + str(record["doi"]),
                        "title": "Conflicting duplicate title",
                        "best_oa_location": {
                            "is_oa": True,
                            "pdf_url": "https://repository.example/wrong.pdf",
                            "source": {"type": "repository"},
                        },
                        "locations": [],
                    },
                ]
            }
        ).encode("utf-8")

        self.assertEqual(pipeline.parse_openalex_location_entries(payload, queue), {})

    def test_mocked_campaign_terminal_outputs(self) -> None:
        temporary_directory, copied_root = copy_reference_fixture_tree()
        self.addCleanup(temporary_directory.cleanup)
        queue = pipeline.derive_exact_queue_records(copied_root)
        atom_payload = create_arxiv_metadata_feed(queue)
        openalex_payload = create_openalex_metadata_payload(queue)
        pdf_payload = create_valid_pdf_bytes()
        requests: list[str] = []

        def request_fixture_transport(request: dict[str, object]) -> dict[str, object]:
            url = str(request["url"])
            requests.append(url)
            if "export.arxiv.org/api/query" in url:
                body = atom_payload
                media_type = "application/atom+xml"
            elif "api.openalex.org/works" in url:
                body = openalex_payload
                media_type = "application/json"
            elif url.startswith("https://arxiv.org/pdf/"):
                body = pdf_payload
                media_type = "application/pdf"
            else:
                raise AssertionError("unexpected request " + url)
            return {
                "status_code": 200,
                "body": body,
                "headers": {"content-type": media_type, "content-length": str(len(body))},
                "final_url": url,
                "redirects": [],
            }

        result = pipeline.execute_g04_acquisition_campaign(
            copied_root,
            allow_network=True,
            request_function=request_fixture_transport,
            sleep_function=lambda _seconds: None,
            clock_function=lambda: "2026-08-11T00:00:00Z",
        )

        self.assertEqual(result["queue_size"], 50)
        self.assertEqual(result["terminal_rows"], 50)
        self.assertEqual(result["acquired"], 31)
        self.assertEqual(result["parsed"], 31)
        self.assertEqual(result["unavailable"], 19)
        self.assertEqual(result["external_requests"], 33)
        self.assertEqual(len(requests), 33)

        ledger = pipeline.read_tsv_rows_exact(copied_root / "sources" / "download-ledger.tsv")
        manifest = pipeline.read_tsv_rows_exact(copied_root / "sources" / "paper-manifest.tsv")
        self.assertEqual(pipeline.validate_download_ledger_rows(ledger, queue), [])
        self.assertEqual(
            pipeline.validate_g04_manifest_rows(manifest, ledger, queue, require_complete=True),
            [],
        )
        self.assertEqual(pipeline.validate_local_artifact_records(copied_root, ledger), [])
        self.assertTrue((copied_root / "sources" / "G04-acquisition-parsing-report.md").is_file())

    def test_offline_campaign_byte_replay(self) -> None:
        temporary_directory, copied_root = copy_reference_fixture_tree()
        self.addCleanup(temporary_directory.cleanup)
        queue = pipeline.derive_exact_queue_records(copied_root)
        atom_payload = create_arxiv_metadata_feed(queue)
        openalex_payload = create_openalex_metadata_payload(queue)
        pdf_payload = create_valid_pdf_bytes()

        def request_fixture_transport(request: dict[str, object]) -> dict[str, object]:
            url = str(request["url"])
            if "export.arxiv.org/api/query" in url:
                return {"status_code": 200, "body": atom_payload, "headers": {"content-type": "application/atom+xml"}, "final_url": url, "redirects": []}
            if "api.openalex.org/works" in url:
                return {"status_code": 200, "body": openalex_payload, "headers": {"content-type": "application/json"}, "final_url": url, "redirects": []}
            if url.startswith("https://arxiv.org/pdf/"):
                return {"status_code": 200, "body": pdf_payload, "headers": {"content-type": "application/pdf"}, "final_url": url, "redirects": []}
            raise AssertionError("unexpected request " + url)

        pipeline.execute_g04_acquisition_campaign(
            copied_root,
            allow_network=True,
            request_function=request_fixture_transport,
            sleep_function=lambda _seconds: None,
            clock_function=lambda: "2026-08-11T00:00:00Z",
        )
        output_paths = (
            "sources/download-ledger.tsv",
            "sources/paper-manifest.tsv",
            "sources/G04-acquisition-parsing-report.md",
        )
        before = {path: (copied_root / path).read_bytes() for path in output_paths}

        def reject_network_request(_request: dict[str, object]) -> dict[str, object]:
            raise AssertionError("offline replay attempted network")

        result = pipeline.execute_g04_acquisition_campaign(
            copied_root,
            allow_network=False,
            request_function=reject_network_request,
            sleep_function=lambda _seconds: None,
            clock_function=lambda: "2099-01-01T00:00:00Z",
        )

        self.assertEqual(result["external_requests"], 0)
        self.assertEqual(result["cache_hits"], 31)
        self.assertEqual(
            {path: (copied_root / path).read_bytes() for path in output_paths},
            before,
        )

    def test_requested_failure_trace_integrity(self) -> None:
        queue = pipeline.derive_exact_queue_records(REFERENCE_ROOT)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            trace_relative = "cache/g04/traces/{0}.json".format(queue[0]["paper_id"])
            trace_path = root / trace_relative
            trace_path.parent.mkdir(parents=True)
            request_url = "https://arxiv.org/pdf/2511.07886v1"
            trace_bytes = pipeline.serialize_request_trace_bytes(
                {
                    "goal_id": "G04",
                    "paper_id": queue[0]["paper_id"],
                    "source_service": "ARXIV",
                    "request_url": request_url,
                    "attempts": [
                        {
                            "attempt": 1,
                            "accessed_at_utc": "2026-08-11T00:00:00Z",
                            "requested_uri": request_url,
                            "response_status": "HTTP_403",
                            "final_uri": request_url,
                            "redirects": [],
                            "response_headers": {"content-type": "text/html"},
                            "payload_checksum": hashlib.sha256(b"").hexdigest().upper(),
                        }
                    ],
                }
            )
            trace_path.write_bytes(trace_bytes)
            row = pipeline.build_requested_failure_row(
                queue[0],
                {
                    "source_service": "ARXIV",
                    "retrieval_uri": request_url,
                    "policy_url": pipeline.ARXIV_POLICY_URL,
                },
                {
                    "status_code": 403,
                    "body": b"",
                    "headers": {"content-type": "text/html"},
                    "final_url": request_url,
                },
                [{"accessed_at_utc": "2026-08-11T00:00:00Z", "response_status": "HTTP_403"}],
                trace_relative,
                hashlib.sha256(trace_bytes).hexdigest().upper(),
                acquisition_status="AUTHORIZATION_FAILED",
                terminal_reason="HTTP_403",
            )

            self.assertEqual(pipeline.validate_local_artifact_records(root, [row]), [])
            unbound_trace = json.loads(trace_bytes)
            unbound_trace["attempts"][-1]["final_uri"] = (
                "https://unbound.example/paper.pdf"
            )
            self.assertTrue(
                any(
                    "final URI does not match ledger" in error
                    for error in pipeline.validate_paper_request_trace(row, unbound_trace)
                )
            )
            trace_path.unlink()
            self.assertTrue(
                any(
                    "trace_path" in error and "missing" in error
                    for error in pipeline.validate_local_artifact_records(root, [row])
                )
            )

    def test_malformed_request_trace_rejected(self) -> None:
        queue = pipeline.derive_exact_queue_records(REFERENCE_ROOT)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            trace_relative = "cache/g04/traces/{0}.json".format(queue[0]["paper_id"])
            trace_path = root / trace_relative
            trace_path.parent.mkdir(parents=True)
            trace_bytes = b"not-json\n"
            trace_path.write_bytes(trace_bytes)
            row = pipeline.build_requested_failure_row(
                queue[0],
                {
                    "source_service": "ARXIV",
                    "retrieval_uri": "https://arxiv.org/pdf/2511.07886v1",
                    "policy_url": pipeline.ARXIV_POLICY_URL,
                },
                {
                    "status_code": 403,
                    "body": b"",
                    "headers": {"content-type": "text/html"},
                    "final_url": "https://arxiv.org/pdf/2511.07886v1",
                },
                [{"accessed_at_utc": "2026-08-11T00:00:00Z"}],
                trace_relative,
                hashlib.sha256(trace_bytes).hexdigest().upper(),
                acquisition_status="AUTHORIZATION_FAILED",
                terminal_reason="HTTP_403",
            )
            errors = pipeline.validate_local_artifact_records(root, [row])
            self.assertTrue(any("trace JSON is malformed" in error for error in errors))

    def test_metadata_attempt_global_cap_rejected(self) -> None:
        temporary_directory, root = copy_reference_fixture_tree()
        with temporary_directory:
            metadata_root = root / "cache" / "g04" / "metadata"
            metadata_root.mkdir(parents=True)
            body = b"{}"
            body_checksum = hashlib.sha256(body).hexdigest().upper()
            (metadata_root / "probe.body").write_bytes(body)
            attempts = [
                {
                    "attempt": attempt,
                    "accessed_at_utc": "2026-08-11T00:00:0{0}Z".format(attempt),
                    "requested_uri": "https://api.example/metadata",
                    "response_status": "HTTP_200",
                    "final_uri": "https://api.example/metadata",
                    "redirects": [],
                    "response_headers": {},
                    "payload_checksum": body_checksum,
                }
                for attempt in (1, 2)
            ]
            trace = {
                "goal_id": "G04",
                "cache_name": "probe",
                "request_url": "https://api.example/metadata",
                "final_url": "https://api.example/metadata",
                "redirects": [],
                "response_headers": {},
                "response_checksum": body_checksum,
                "attempts": attempts,
            }
            (metadata_root / "probe.trace.json").write_bytes(
                pipeline.serialize_request_trace_bytes(trace)
            )
            errors = pipeline.validate_campaign_request_evidence(
                root, [{"attempt_count": "219"}]
            )
            self.assertTrue(any("220" in error and "cap" in error for error in errors))

    def test_metadata_trace_identity_binding(self) -> None:
        queue = pipeline.derive_exact_queue_records(REFERENCE_ROOT)
        metadata_root = REFERENCE_ROOT / "cache" / "g04" / "metadata"
        trace = json.loads(
            (metadata_root / "openalex-exact-dois.trace.json").read_text(
                encoding="utf-8"
            )
        )
        body = (metadata_root / "openalex-exact-dois.body").read_bytes()
        self.assertEqual(
            pipeline.validate_metadata_cache_record(
                "openalex-exact-dois", trace, body, queue
            ),
            [],
        )

        unbound = json.loads(json.dumps(trace))
        unbound["request_url"] = "https://unbound.example/metadata"
        for attempt in unbound["attempts"]:
            attempt["requested_uri"] = unbound["request_url"]
        self.assertTrue(
            any(
                "frozen request" in error
                for error in pipeline.validate_metadata_cache_record(
                    "openalex-exact-dois", unbound, body, queue
                )
            )
        )

        self.assertTrue(
            any(
                "identity set" in error
                for error in pipeline.validate_metadata_cache_record(
                    "openalex-exact-dois", trace, b'{"results":[]}', queue
                )
            )
        )

        four_attempts = json.loads(json.dumps(trace))
        fourth = dict(four_attempts["attempts"][-1])
        fourth["attempt"] = 4
        four_attempts["attempts"] = [dict(fourth, attempt=index) for index in range(1, 5)]
        self.assertTrue(
            any(
                "three-attempt ceiling" in error
                for error in pipeline.validate_metadata_cache_record(
                    "openalex-exact-dois", four_attempts, body, queue
                )
            )
        )

    def test_request_route_binding(self) -> None:
        queue = pipeline.derive_exact_queue_records(REFERENCE_ROOT)
        request_url = "https://arxiv.org/pdf/2511.07886v1"
        final_url = "https://export.arxiv.org/pdf/2511.07886v1"
        trace = {
            "goal_id": "G04",
            "paper_id": queue[0]["paper_id"],
            "source_service": "ARXIV",
            "request_url": request_url,
            "attempts": [
                {
                    "attempt": 1,
                    "accessed_at_utc": "2026-08-11T00:00:00Z",
                    "requested_uri": request_url,
                    "response_status": "HTTP_403",
                    "final_uri": final_url,
                    "redirects": [
                        {
                            "status_code": 302,
                            "from": request_url,
                            "to": final_url,
                        }
                    ],
                    "response_headers": {"content-type": "text/html"},
                    "payload_checksum": hashlib.sha256(b"").hexdigest().upper(),
                }
            ],
        }
        row = pipeline.build_requested_failure_row(
            queue[0],
            {
                "source_service": "ARXIV",
                "retrieval_uri": request_url,
                "policy_url": pipeline.ARXIV_POLICY_URL,
            },
            {
                "status_code": 403,
                "body": b"",
                "headers": {"content-type": "text/html"},
                "final_url": final_url,
            },
            trace["attempts"],
            "cache/g04/traces/probe.json",
            "0" * 64,
            acquisition_status="AUTHORIZATION_FAILED",
            terminal_reason="HTTP_403",
        )

        self.assertEqual(
            pipeline.validate_paper_request_trace(
                row, trace, expected_request_url=request_url
            ),
            [],
        )

        unbound_request = json.loads(json.dumps(trace))
        unbound_request["request_url"] = "https://unbound.example/paper.pdf"
        unbound_request["attempts"][0]["requested_uri"] = unbound_request[
            "request_url"
        ]
        self.assertTrue(
            any(
                "frozen source" in error or "redirect route" in error
                for error in pipeline.validate_paper_request_trace(
                    row, unbound_request, expected_request_url=request_url
                )
            )
        )

        broken_redirect = json.loads(json.dumps(trace))
        broken_redirect["attempts"][0]["redirects"][0]["from"] = (
            "https://unbound.example/paper.pdf"
        )
        self.assertTrue(
            any(
                "redirect route" in error
                for error in pipeline.validate_paper_request_trace(
                    row, broken_redirect, expected_request_url=request_url
                )
            )
        )

        metadata_root = REFERENCE_ROOT / "cache" / "g04" / "metadata"
        metadata_trace = json.loads(
            (metadata_root / "openalex-exact-dois.trace.json").read_text(
                encoding="utf-8"
            )
        )
        metadata_body = (metadata_root / "openalex-exact-dois.body").read_bytes()
        unbound_final = json.loads(json.dumps(metadata_trace))
        unbound_final["final_url"] = "https://unbound.example/metadata"
        unbound_final["attempts"][-1]["final_uri"] = unbound_final["final_url"]
        self.assertTrue(
            any(
                "authorized service host" in error or "redirect route" in error
                for error in pipeline.validate_metadata_cache_record(
                    "openalex-exact-dois", unbound_final, metadata_body, queue
                )
            )
        )

    def test_redirect_ceiling_replay(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            copied_root = Path(directory) / "arxiv-reference"
            shutil.copytree(
                REFERENCE_ROOT,
                copied_root,
                ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
            )
            ledger_path = copied_root / "sources" / "download-ledger.tsv"
            ledger_rows = pipeline.read_tsv_rows_exact(ledger_path)
            paper_row = next(
                row for row in ledger_rows if row["trace_path"] != "NOT_AVAILABLE"
            )
            paper_trace_path = copied_root / paper_row["trace_path"]
            original_paper_trace = paper_trace_path.read_bytes()
            paper_trace = json.loads(original_paper_trace)
            paper_request = paper_trace["request_url"]
            route_urls = [
                "https://route-{0}.example/paper.pdf".format(index)
                for index in range(1, 6)
            ]
            route_urls.append(paper_request)
            paper_trace["attempts"][-1]["redirects"] = [
                {"status_code": 302, "from": source, "to": target}
                for source, target in zip(
                    [paper_request] + route_urls[:-1], route_urls
                )
            ]
            mutated_paper_trace = pipeline.serialize_request_trace_bytes(paper_trace)
            paper_trace_path.write_bytes(mutated_paper_trace)
            paper_row["trace_checksum"] = hashlib.sha256(
                mutated_paper_trace
            ).hexdigest().upper()
            pipeline.write_tsv_rows_atomic(
                ledger_path, pipeline.DOWNLOAD_LEDGER_FIELDS, ledger_rows
            )

            with self.assertRaisesRegex(ValueError, "five-redirect ceiling"):
                pipeline.execute_g04_acquisition_campaign(
                    copied_root,
                    allow_network=False,
                    request_function=lambda _request: self.fail(
                        "paper replay attempted network"
                    ),
                )

            paper_trace_path.write_bytes(original_paper_trace)
            paper_row["trace_checksum"] = hashlib.sha256(
                original_paper_trace
            ).hexdigest().upper()
            pipeline.write_tsv_rows_atomic(
                ledger_path, pipeline.DOWNLOAD_LEDGER_FIELDS, ledger_rows
            )

            metadata_trace_path = (
                copied_root
                / "cache"
                / "g04"
                / "metadata"
                / "openalex-exact-dois.trace.json"
            )
            metadata_trace = json.loads(
                metadata_trace_path.read_text(encoding="utf-8")
            )
            metadata_request = metadata_trace["request_url"]
            metadata_urls = [
                "https://route-{0}.example/metadata".format(index)
                for index in range(1, 6)
            ]
            metadata_urls.append(metadata_request)
            six_redirects = [
                {"status_code": 302, "from": source, "to": target}
                for source, target in zip(
                    [metadata_request] + metadata_urls[:-1], metadata_urls
                )
            ]
            metadata_trace["redirects"] = six_redirects
            metadata_trace["attempts"][-1]["redirects"] = six_redirects
            metadata_trace_path.write_bytes(
                pipeline.serialize_request_trace_bytes(metadata_trace)
            )

            with self.assertRaisesRegex(ValueError, "five-redirect ceiling"):
                pipeline.execute_g04_acquisition_campaign(
                    copied_root,
                    allow_network=False,
                    request_function=lambda _request: self.fail(
                        "metadata replay attempted network"
                    ),
                )

    def test_actual_terminal_corpus_integrity(self) -> None:
        queue = pipeline.derive_exact_queue_records(REFERENCE_ROOT)
        ledger = pipeline.read_tsv_rows_exact(LEDGER_PATH)
        manifest = pipeline.read_tsv_rows_exact(
            REFERENCE_ROOT / "sources" / "paper-manifest.tsv"
        )

        self.assertEqual(len(ledger), 50)
        self.assertEqual(
            sum(row["acquisition_status"] == "ACQUIRED" for row in ledger), 34
        )
        self.assertEqual(sum(row["parse_status"] == "PARSED" for row in ledger), 34)
        self.assertEqual(
            sum(row["acquisition_status"] != "ACQUIRED" for row in ledger), 16
        )
        self.assertEqual(pipeline.validate_download_ledger_rows(ledger, queue), [])
        self.assertEqual(pipeline.validate_local_artifact_records(REFERENCE_ROOT, ledger), [])
        self.assertEqual(
            pipeline.validate_g04_manifest_rows(
                manifest,
                ledger,
                queue,
                require_complete=True,
                allow_read_complete=True,
            ),
            [],
        )
        self.assertEqual(
            sum(row["selection_status"] == "READ_COMPLETE" for row in manifest),
            25,
        )
        report = (
            REFERENCE_ROOT / "sources" / "G04-acquisition-parsing-report.md"
        ).read_text(encoding="utf-8")
        self.assertIn("## Preserved Terminal Limits", report)
        self.assertIn("15 identities have no acceptable direct source", report)
        self.assertIn("1 attempted publisher retrieval ended in authorization failure", report)
        self.assertIn("34 acquired papers remain `LICENSE_UNKNOWN`", report)

    def test_actual_offline_byte_replay(self) -> None:
        output_paths = (
            LEDGER_PATH,
            REFERENCE_ROOT / "sources" / "paper-manifest.tsv",
            REFERENCE_ROOT / "sources" / "G04-acquisition-parsing-report.md",
        )
        before = {path: path.read_bytes() for path in output_paths}

        def reject_network_request(_request: dict[str, object]) -> dict[str, object]:
            raise AssertionError("actual offline replay attempted network")

        result = pipeline.execute_g04_acquisition_campaign(
            REFERENCE_ROOT,
            allow_network=False,
            request_function=reject_network_request,
        )

        self.assertEqual(result["external_requests"], 0)
        self.assertEqual(result["cache_hits"], 34)
        self.assertEqual({path: path.read_bytes() for path in output_paths}, before)

    def test_local_corpus_ignore_policy(self) -> None:
        ledger = pipeline.read_tsv_rows_exact(LEDGER_PATH)
        local_paths = sorted(
            {
                row[field]
                for row in ledger
                for field in ("local_path", "trace_path", "extracted_path")
                if row[field] not in {"NOT_ACQUIRED", "NOT_AVAILABLE"}
            }
        )
        self.assertTrue(local_paths)
        ignored = subprocess.run(
            ["git", "check-ignore", "--stdin"],
            cwd=REPOSITORY_ROOT,
            check=False,
            input="\n".join("arxiv-reference/" + path for path in local_paths) + "\n",
            capture_output=True,
            text=True,
        )
        self.assertEqual(ignored.returncode, 0)
        self.assertEqual(len(ignored.stdout.splitlines()), len(local_paths))
        tracked = subprocess.run(
            ["git", "ls-files", "--", "arxiv-reference/sources/papers", "arxiv-reference/cache/g04"],
            cwd=REPOSITORY_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(tracked.stdout.strip(), "")


if __name__ == "__main__":
    unittest.main()
