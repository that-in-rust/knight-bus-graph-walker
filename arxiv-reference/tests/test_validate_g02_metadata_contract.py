#!/usr/bin/env python3
"""RED-first tests for bounded G02 metadata discovery."""

from __future__ import annotations

import csv
import importlib.util
import re
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_ROOT = REPOSITORY_ROOT / "arxiv-reference"
FIXTURE_ROOT = REFERENCE_ROOT / "tests" / "fixtures" / "g02"
PIPELINE_PATH = REFERENCE_ROOT / "tools" / "g02_metadata_pipeline.py"
MANIFEST_PATH = REFERENCE_ROOT / "sources" / "paper-manifest.tsv"
REQUEST_LEDGER_PATH = REFERENCE_ROOT / "sources" / "metadata-request-ledger.tsv"
SCREENING_REPORT_PATH = REFERENCE_ROOT / "sources" / "G02-metadata-screening-report.md"
QUERY_LEDGER_HEADER = (
    "query_id\tarchitecture_question_ids\tsource_term_ids\tservice\tquery_text\t"
    "categories\tdate_from\tdate_to\texclusions\texecuted_at\tresult_count\t"
    "response_checksum\tstatus"
)
TAXONOMY_HEADER = (
    "term_id\tterm\tterm_type\tarchitecture_question_ids\tsource_repo_paths\t"
    "synonyms\thistorical_terms\tadjacent_domain_terms\texclusion_terms\tnotes"
)

spec = importlib.util.spec_from_file_location("g02_metadata_pipeline", PIPELINE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("cannot load G02 metadata pipeline")
pipeline = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = pipeline
spec.loader.exec_module(pipeline)


class ValidateG02MetadataContractTests(unittest.TestCase):
    def test_g02_owned_outputs_exist(self) -> None:
        self.assertTrue(MANIFEST_PATH.is_file())
        self.assertTrue(REQUEST_LEDGER_PATH.is_file())
        self.assertTrue(SCREENING_REPORT_PATH.is_file())

    def test_screening_report_has_closure_contract(self) -> None:
        report = SCREENING_REPORT_PATH.read_text(encoding="utf-8")
        required_sections = (
            "## Executive Result",
            "## Campaign Accounting",
            "## Coverage By Architecture Question",
            "## Exploration Quotas",
            "## Ranking Limitations",
            "## Recommended G03 Seed Set",
            "## Unresolved Coverage Gaps",
            "## Scope Boundary",
        )
        for section in required_sections:
            self.assertIn(section, report)
        self.assertIn("metadata-screening judgments", report)
        self.assertIn("not `SOURCE_CLAIM`s", report)
        self.assertIn("G03 remains `NOT_STARTED`", report)

        seed_section = report.split("## Recommended G03 Seed Set", 1)[1]
        seed_section = seed_section.split("\n## ", 1)[0]
        seed_ids = re.findall(r"`(PAPER-[^`]+)`", seed_section)
        self.assertEqual(len(seed_ids), 25)
        self.assertEqual(len(seed_ids), len(set(seed_ids)))

        with MANIFEST_PATH.open(encoding="utf-8") as handle:
            manifest_rows = list(csv.DictReader(handle, delimiter="\t"))
        manifest_by_id = {row["paper_id"]: row for row in manifest_rows}
        manifest_ids = set(manifest_by_id)
        self.assertTrue(set(seed_ids) <= manifest_ids)
        self.assertTrue(
            all(manifest_by_id[paper_id]["selection_status"] == "METADATA_ONLY" for paper_id in seed_ids)
        )

        def lane_for_seed(paper_id: str) -> str:
            minimum_query = min(
                int(value.removeprefix("QRY-"))
                for value in manifest_by_id[paper_id]["discovery_query_ids"].split("|")
            )
            if minimum_query <= 6:
                return "A"
            if minimum_query <= 14:
                return "B"
            if minimum_query <= 20:
                return "C"
            return "D"

        self.assertEqual(Counter(lane_for_seed(paper_id) for paper_id in seed_ids), Counter({"A": 8, "B": 8, "C": 5, "D": 4}))
        seed_questions = {
            question
            for paper_id in seed_ids
            for question in manifest_by_id[paper_id]["architecture_question_ids"].split("|")
        }
        self.assertEqual(seed_questions, {f"AQ-{index:03d}" for index in range(1, 13)})

        contradictory_section = report.split("### Explicit Contradictory-Looking Set", 1)[1]
        contradictory_section = contradictory_section.split("\n## ", 1)[0]
        contradictory_ids = re.findall(r"`(PAPER-[^`]+)`", contradictory_section)
        self.assertEqual(len(contradictory_ids), 26)
        self.assertEqual(len(contradictory_ids), len(set(contradictory_ids)))
        self.assertTrue(set(contradictory_ids) <= manifest_ids)

    def test_active_g03_lifecycle_preserves_verified_g02(self) -> None:
        status = (REFERENCE_ROOT / "governance" / "campaign-status.md").read_text()
        self.assertIn("- Active goal: `G03`", status)
        self.assertIn("- G02 state: `COMPLETE_VERIFIED`", status)

    def test_basic_metadata_fixture_parses(self) -> None:
        records = pipeline.parse_arxiv_metadata_feed(
            (FIXTURE_ROOT / "arxiv-basic.xml").read_bytes()
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["arxiv_id"], "2401.00001v2")
        self.assertEqual(records[0]["doi"], "10.1000/example.1")

    def test_empty_and_malformed_metadata_are_distinct(self) -> None:
        self.assertEqual(
            pipeline.parse_arxiv_metadata_feed(
                (FIXTURE_ROOT / "arxiv-empty.xml").read_bytes()
            ),
            [],
        )
        with self.assertRaises(ValueError):
            pipeline.parse_arxiv_metadata_feed(
                (FIXTURE_ROOT / "arxiv-malformed.xml").read_bytes()
            )

    def test_date_bucket_rejects_out_of_range_metadata(self) -> None:
        records = pipeline.parse_arxiv_metadata_feed(
            (FIXTURE_ROOT / "arxiv-basic.xml").read_bytes()
        )
        self.assertEqual(pipeline.validate_variant_date_records(records, "ALL"), [])
        errors = pipeline.validate_variant_date_records(records, "PRE2001")
        self.assertTrue(any("outside PRE2001" in error for error in errors))

    def test_versions_and_doi_aliases_reconcile(self) -> None:
        records = pipeline.parse_arxiv_metadata_feed(
            (FIXTURE_ROOT / "arxiv-duplicates.xml").read_bytes()
        )
        canonical = pipeline.reconcile_canonical_paper_identities(records)
        self.assertEqual(len(canonical), 1)
        self.assertEqual(canonical[0]["canonical_version"], "v2")
        self.assertEqual(canonical[0]["versions"], ["v1", "v2"])

    def test_normalized_title_collision_stays_ambiguous(self) -> None:
        records = pipeline.parse_arxiv_metadata_feed(
            (FIXTURE_ROOT / "arxiv-title-collision.xml").read_bytes()
        )
        canonical = pipeline.reconcile_canonical_paper_identities(records)
        self.assertEqual(len(canonical), 2)
        self.assertTrue(all(row["identity_state"] == "AMBIGUOUS" for row in canonical))

    def test_request_cap_and_checksum_fail_closed(self) -> None:
        rows = [
            {
                "request_id": f"REQ-G02-{index:04d}",
                "goal_id": "G02",
                "query_id": "QRY-001",
                "variant_id": f"QRY-001-ALL-{index}",
                "service": "arXiv",
                "response_checksum": "" if index == 1 else "a" * 64,
                "attempt": "1",
                "cache_status": "MISS",
                "terminal_state": "COMPLETE",
            }
            for index in range(1, 202)
        ]
        errors = pipeline.validate_request_provenance_rows(rows)
        self.assertTrue(any("200" in error and "cap" in error for error in errors))
        self.assertTrue(any("checksum" in error for error in errors))

    def test_retry_and_completed_request_safety_fail_closed(self) -> None:
        rows = [
            {"request_id":"REQ-G02-0001","goal_id":"G02","query_id":"QRY-001","variant_id":"QRY-001-ALL","service":"arXiv","response_checksum":"a"*64,"attempt":"4","cache_status":"MISS","terminal_state":"COMPLETE"},
            {"request_id":"REQ-G02-0002","goal_id":"G02","query_id":"QRY-001","variant_id":"QRY-001-ALL","service":"arXiv","response_checksum":"b"*64,"attempt":"1","cache_status":"MISS","terminal_state":"COMPLETE"},
        ]
        errors = pipeline.validate_request_provenance_rows(rows)
        self.assertTrue(any("attempt" in error for error in errors))
        self.assertTrue(any("completed variant" in error for error in errors))

    def test_successful_retry_chain_is_valid(self) -> None:
        rows = [
            {"request_id":"REQ-G02-0001","goal_id":"G02","query_id":"QRY-001","variant_id":"QRY-001-ALL","service":"arXiv","response_checksum":"a"*64,"response_status":"429","attempt":"1","cache_status":"MISS","retry_events":"attempt-1:429","terminal_state":"FAILED"},
            {"request_id":"REQ-G02-0002","goal_id":"G02","query_id":"QRY-001","variant_id":"QRY-001-ALL","service":"arXiv","response_checksum":"b"*64,"response_status":"200","attempt":"2","cache_status":"MISS","retry_events":"attempt-1:429","terminal_state":"COMPLETE"},
        ]
        errors = pipeline.validate_request_provenance_rows(rows)
        self.assertFalse(any("unsafe retry" in error for error in errors), errors)

    def test_request_budget_is_checked_per_attempt(self) -> None:
        class FakeResponse:
            status = 429
            headers = {"Retry-After": "0"}

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self) -> bytes:
                return b"rate limited"

        with tempfile.TemporaryDirectory() as temporary_directory:
            reference_root = Path(temporary_directory) / "arxiv-reference"
            request_ledger = reference_root / "sources" / "metadata-request-ledger.tsv"
            with mock.patch.object(pipeline.urllib.request, "urlopen", return_value=FakeResponse()) as urlopen:
                with mock.patch.object(pipeline.time, "sleep"):
                    with self.assertRaisesRegex(RuntimeError, "request cap"):
                        pipeline.fetch_arxiv_variant_payload(
                            reference_root,
                            "QRY-001",
                            "QRY-001-ALL",
                            "all:PageRank",
                            request_ledger,
                            1,
                            0.0,
                            remaining_http_requests=1,
                        )
            self.assertEqual(urlopen.call_count, 1)

    def test_cached_response_and_query_aggregates_verify(self) -> None:
        request_rows = pipeline.read_tsv_rows(
            REQUEST_LEDGER_PATH, pipeline.REQUEST_LEDGER_HEADER
        )
        query_rows = pipeline.read_tsv_rows(
            REFERENCE_ROOT / "governance" / "query-ledger.tsv", QUERY_LEDGER_HEADER
        )
        self.assertEqual(
            pipeline.validate_cached_response_provenance(REFERENCE_ROOT, request_rows),
            [],
        )
        self.assertEqual(
            pipeline.validate_query_aggregate_provenance(query_rows, request_rows),
            [],
        )

        bad_requests = [dict(row) for row in request_rows]
        bad_requests[0]["response_checksum"] = "0" * 64
        self.assertTrue(
            any("cached response checksum" in error for error in pipeline.validate_cached_response_provenance(REFERENCE_ROOT, bad_requests))
        )
        bad_queries = [dict(row) for row in query_rows]
        bad_queries[0]["result_count"] = "999"
        self.assertTrue(
            any("aggregate result_count" in error for error in pipeline.validate_query_aggregate_provenance(bad_queries, request_rows))
        )

    def test_cache_rejects_unreferenced_or_forbidden_files(self) -> None:
        payload = (FIXTURE_ROOT / "arxiv-basic.xml").read_bytes()
        with tempfile.TemporaryDirectory() as temporary_directory:
            reference_root = Path(temporary_directory) / "arxiv-reference"
            cache_directory = reference_root / "cache" / "g02" / "arxiv"
            cache_directory.mkdir(parents=True)
            cache_file = cache_directory / "QRY-001-ALL.xml"
            cache_file.write_bytes(payload)
            row = {
                "request_id":"REQ-G02-0001","goal_id":"G02","query_id":"QRY-001","variant_id":"QRY-001-ALL","service":"arXiv","response_checksum":pipeline.checksum_response_payload(payload),"response_status":"200","result_count":"1","attempt":"1","cache_status":"MISS","retry_events":"NONE","cache_path":"arxiv-reference/cache/g02/arxiv/QRY-001-ALL.xml","terminal_state":"COMPLETE"
            }
            self.assertEqual(
                pipeline.validate_cached_response_provenance(reference_root, [row]),
                [],
            )
            (cache_directory / "hidden-full-text.pdf").write_bytes(b"%PDF-1.7")
            errors = pipeline.validate_cached_response_provenance(reference_root, [row])
            self.assertTrue(any("unreferenced cache file" in error for error in errors), errors)
            self.assertTrue(any("forbidden cache content" in error for error in errors), errors)

    def test_manifest_rejects_schema_provenance_and_source_claims(self) -> None:
        row = {
            "paper_id":"PAPER-2401.00001","arxiv_id":"2401.00001","doi":"UNKNOWN","title":"Example","authors":"A","published_date":"2024-01-01","updated_date":"2024-01-02","categories":"cs.DS","abstract_url":"https://arxiv.org/abs/2401.00001","pdf_url":"https://arxiv.org/pdf/2401.00001","license_uri":"UNKNOWN","canonical_version":"v1","discovery_query_ids":"QRY-999","architecture_question_ids":"AQ-999","relevance_score":"101","score_breakdown":"MR=20","selection_status":"METADATA_ONLY","evidence_grade":"D_THEORETICAL_OR_INCOMPLETE","code_urls":"UNKNOWN","local_path":"NOT_ACQUIRED","sha256":"NOT_ACQUIRED","notes":"SOURCE_CLAIM=the title proves it"
        }
        errors = pipeline.validate_metadata_manifest_rows(
            [row], {"QRY-001"}, {"AQ-001"}
        )
        self.assertTrue(any("query" in error for error in errors))
        self.assertTrue(any("question" in error for error in errors))
        self.assertTrue(any("SOURCE_CLAIM" in error for error in errors))
        self.assertTrue(any("score" in error for error in errors))

    def test_metadata_score_is_bounded_and_transparent(self) -> None:
        record = pipeline.parse_arxiv_metadata_feed(
            (FIXTURE_ROOT / "arxiv-basic.xml").read_bytes()
        )[0]
        score, breakdown = pipeline.build_metadata_screen_score(record)
        self.assertGreaterEqual(score, 0)
        self.assertLessEqual(score, 100)
        self.assertRegex(
            breakdown,
            r"^MR=\d+;RR=\d+;IS=\d+;BS=\d+;TR=\d+;FL=\d+;ND=\d+$",
        )

    def test_query_translation_uses_algorithm_and_mechanism_anchors(self) -> None:
        query_rows = pipeline.read_tsv_rows(
            REFERENCE_ROOT / "governance" / "query-ledger.tsv", QUERY_LEDGER_HEADER
        )
        taxonomy_rows = pipeline.read_tsv_rows(
            REFERENCE_ROOT / "governance" / "keyword-taxonomy.tsv", TAXONOMY_HEADER
        )
        query = pipeline.build_arxiv_query_variant(
            query_rows[2],
            {row["term_id"]: row for row in taxonomy_rows},
            None,
        )
        self.assertIn("all:PageRank", query)
        self.assertIn("all:CSR", query)
        self.assertNotIn('all:"PageRank power iteration"', query)

        dated_query = pipeline.build_arxiv_query_variant(
            query_rows[2],
            {row["term_id"]: row for row in taxonomy_rows},
            ("197001010000", "200012312359"),
        )
        self.assertIn(" ANDNOT ", dated_query)
        self.assertNotIn(" AND NOT ", dated_query)
        self.assertLess(dated_query.index("submittedDate:"), dated_query.index("ANDNOT"))

    def test_no_pdf_fixture_or_tracked_output_exists(self) -> None:
        self.assertFalse(any(FIXTURE_ROOT.rglob("*.pdf")))
        if MANIFEST_PATH.exists():
            with MANIFEST_PATH.open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle, delimiter="\t"))
            self.assertTrue(all(row["local_path"] == "NOT_ACQUIRED" for row in rows))


if __name__ == "__main__":
    unittest.main()
