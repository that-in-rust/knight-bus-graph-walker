#!/usr/bin/env python3
"""RED-first tests for bounded G03 citation archaeology."""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import io
import re
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_ROOT = REPOSITORY_ROOT / "arxiv-reference"
FIXTURE_ROOT = REFERENCE_ROOT / "tests" / "fixtures" / "g03"
PIPELINE_PATH = REFERENCE_ROOT / "tools" / "g03_citation_pipeline.py"
G04_PIPELINE_PATH = REFERENCE_ROOT / "tools" / "g04_acquisition_pipeline.py"
REPORT_PATH = REFERENCE_ROOT / "sources" / "G02-metadata-screening-report.md"
MANIFEST_PATH = REFERENCE_ROOT / "sources" / "paper-manifest.tsv"
EDGE_PATH = REFERENCE_ROOT / "sources" / "citation-edges.tsv"
REQUEST_PATH = REFERENCE_ROOT / "sources" / "citation-request-ledger.tsv"
PREFLIGHT_PATH = REFERENCE_ROOT / "governance" / "g03-service-preflight.md"
FINAL_REPORT_PATH = REFERENCE_ROOT / "sources" / "G03-citation-ancestry-report.md"
STOP_PATH = REFERENCE_ROOT / "sources" / "citation-stops.tsv"
SCREENING_PATH = REFERENCE_ROOT / "sources" / "citation-screening-ledger.tsv"

spec = importlib.util.spec_from_file_location("g03_citation_pipeline", PIPELINE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("cannot load G03 citation pipeline")
pipeline = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = pipeline
spec.loader.exec_module(pipeline)

g04_spec = importlib.util.spec_from_file_location("g04_acquisition_pipeline_for_g03", G04_PIPELINE_PATH)
if g04_spec is None or g04_spec.loader is None:
    raise RuntimeError("cannot load G04 acquisition pipeline")
g04_pipeline = importlib.util.module_from_spec(g04_spec)
sys.modules[g04_spec.name] = g04_pipeline
g04_spec.loader.exec_module(g04_pipeline)


class ValidateG03CitationContractTests(unittest.TestCase):
    def test_mocked_campaign_traverses_all_seeds_and_writes_bounded_outputs(self) -> None:
        with MANIFEST_PATH.open(encoding="utf-8") as handle:
            baseline_rows = list(csv.DictReader(handle, delimiter="\t"))
        baseline_rows = baseline_rows[: pipeline.EXPECTED_G02_MANIFEST_COUNT]
        baseline_by_id = {row["paper_id"]: row for row in baseline_rows}

        def synthetic_record(
            openalex_id: str,
            title: str,
            published_date: str,
            *,
            paper_id: str = "",
            arxiv_id: str = "UNKNOWN",
            authors: list[str] | None = None,
            references: list[str] | None = None,
        ) -> dict[str, object]:
            record: dict[str, object] = {
                "openalex_id": "https://openalex.org/" + openalex_id,
                "doi": "UNKNOWN",
                "title": title,
                "authors": authors or ["Fixture Author"],
                "published_date": published_date,
                "updated_date": published_date,
                "type": "article",
                "arxiv_id": arxiv_id,
                "arxiv_version": "UNKNOWN",
                "referenced_works": references or [],
                "cited_by_count": 1,
                "source_urls": ["https://openalex.org/" + openalex_id],
                "is_retracted": False,
                "identity_state": "CANONICAL",
            }
            record["paper_id"] = paper_id or pipeline.stable_citation_paper_id(record)
            return record

        def synthetic_s2_record(
            semantic_scholar_id: str,
            title: str,
            published_date: str,
            *,
            paper_id: str = "",
            arxiv_id: str = "UNKNOWN",
            authors: list[str] | None = None,
        ) -> dict[str, object]:
            record: dict[str, object] = {
                "openalex_id": "UNKNOWN",
                "semantic_scholar_id": semantic_scholar_id,
                "provider_id": "S2:" + semantic_scholar_id,
                "provider_name": "SemanticScholar",
                "doi": "UNKNOWN",
                "title": title,
                "authors": authors or ["Fixture Author"],
                "published_date": published_date,
                "updated_date": "UNKNOWN",
                "type": "paper",
                "arxiv_id": arxiv_id,
                "arxiv_version": "UNKNOWN",
                "referenced_works": [],
                "cited_by_count": 1,
                "reference_count": 1,
                "source_urls": [
                    "https://www.semanticscholar.org/paper/{0}?utm_source=api".format(
                        semantic_scholar_id
                    )
                ],
                "is_retracted": False,
                "identity_state": "CANONICAL",
            }
            record["paper_id"] = paper_id or pipeline.stable_citation_paper_id(record)
            return record

        def fake_campaign_page(
            _reference_root: Path,
            _ledger_path: Path,
            _preflight_text: str,
            operation: str,
            identifier: str,
            seed_paper_id: str,
            _traversal_paper_id: str,
            depth: int,
            _direction: str,
            _allow_network: bool,
        ) -> list[dict[str, object]]:
            if operation == "SEED_RESOLUTION":
                if seed_paper_id == "PAPER-2511.07886":
                    return []
                seed = baseline_by_id[seed_paper_id]
                arxiv_id = seed_paper_id.removeprefix("PAPER-")
                suffix = arxiv_id.replace(".", "")
                return [
                    synthetic_record(
                        "WSEED" + suffix,
                        seed["title"],
                        seed["published_date"],
                        paper_id=seed_paper_id,
                        arxiv_id=arxiv_id,
                        authors=seed["authors"].split("|"),
                        references=["https://openalex.org/WBACK1" + suffix],
                    )
                ]
            if operation == "BATCH_WORKS":
                return [
                    synthetic_record(
                        openalex_id,
                        "External Memory PageRank Evaluation",
                        "2000-01-01",
                        references=["https://openalex.org/WDEPTH2" + openalex_id[-8:]],
                    )
                    for openalex_id in identifier.split("|")
                ]
            if operation == "FORWARD_CITATIONS":
                suffix = identifier.rsplit("/", 1)[-1]
                if depth == 0:
                    seed_title = baseline_by_id[seed_paper_id]["title"]
                    title = "Evaluation of {0} with compressed storage".format(seed_title)
                else:
                    title = "PageRank refinement with external memory"
                return [
                    synthetic_record(
                        "WFWD{0}{1}".format(depth, suffix),
                        title,
                        "2026-01-01",
                    )
                ]
            self.fail("unexpected mocked operation: " + operation)

        def fake_s2_campaign_page(
            _reference_root: Path,
            _ledger_path: Path,
            _preflight_text: str,
            operation: str,
            _identifier: str,
            seed_paper_id: str,
            traversal_paper_id: str,
            depth: int,
            _direction: str,
            _allow_network: bool,
        ) -> list[dict[str, object]]:
            if operation == "SEED_RESOLUTION_BATCH":
                rows: list[dict[str, object]] = []
                for seed_id, seed in baseline_by_id.items():
                    if seed_id not in pipeline.extract_g03_seed_ids(
                        REPORT_PATH.read_text(encoding="utf-8")
                    ):
                        continue
                    rows.append(
                        synthetic_s2_record(
                            "S2-SEED-" + seed_id.removeprefix("PAPER-").replace(".", ""),
                            seed["title"],
                            seed["published_date"],
                            paper_id=seed_id,
                            arxiv_id=seed_id.removeprefix("PAPER-"),
                            authors=seed["authors"].split("|"),
                        )
                    )
                return rows
            suffix = traversal_paper_id.replace("PAPER-", "").replace(".", "")
            if operation == "BACKWARD_REFERENCES":
                if depth == 0 and seed_paper_id == "PAPER-2401.01019":
                    payload_error = getattr(
                        pipeline, "CitationPayloadRejected", RuntimeError
                    )
                    raise payload_error("fixture selected payload was rejected")
                return [
                    synthetic_s2_record(
                        "S2-BACK-{0}-{1}".format(depth, suffix),
                        "External Memory PageRank Foundations",
                        "1999-01-01",
                    )
                ]
            if operation == "FORWARD_CITATIONS":
                if depth == 0 and seed_paper_id == "PAPER-1602.02864":
                    raise pipeline.CitationRateLimitExhausted(
                        "fixture branch exhausted its rate-limit attempts"
                    )
                return [
                    synthetic_s2_record(
                        "S2-FWD-{0}-{1}".format(depth, suffix),
                        "PageRank evaluation with compressed storage",
                        "2026-01-01",
                    )
                ]
            self.fail("unexpected mocked S2 operation: " + operation)

        with tempfile.TemporaryDirectory() as temporary_directory:
            reference_root = Path(temporary_directory) / "arxiv-reference"
            for relative_path in (
                "sources/G02-metadata-screening-report.md",
                "sources/paper-manifest.tsv",
                "governance/keyword-taxonomy.tsv",
                "governance/g03-service-preflight.md",
            ):
                source = REFERENCE_ROOT / relative_path
                destination = reference_root / relative_path
                destination.parent.mkdir(parents=True, exist_ok=True)
                if relative_path == "sources/paper-manifest.tsv":
                    pipeline.write_tsv_rows(
                        destination, pipeline.MANIFEST_HEADER, baseline_rows
                    )
                else:
                    shutil.copy2(source, destination)
            (reference_root / "sources" / "citation-request-ledger.tsv").write_text(
                pipeline.REQUEST_HEADER + "\n", encoding="utf-8"
            )
            (reference_root / "sources" / "citation-edges.tsv").write_text(
                pipeline.EDGE_HEADER + "\n", encoding="utf-8"
            )
            (reference_root / "sources" / "citation-screening-ledger.tsv").write_text(
                pipeline.SCREENING_HEADER + "\n", encoding="utf-8"
            )

            def fake_reviewed_g04_queue(_screening_path: Path) -> list[str]:
                generated_rows = pipeline.read_tsv_rows(
                    reference_root / "sources" / "paper-manifest.tsv",
                    pipeline.MANIFEST_HEADER,
                )
                return [
                    row["paper_id"]
                    for row in generated_rows
                    if row["discovery_query_ids"] == "NOT_APPLICABLE"
                    and "IDENTITY_STATE=CANONICAL" in row["notes"]
                ][:25]

            with mock.patch.object(
                pipeline, "_fetch_campaign_page", side_effect=fake_campaign_page
            ) as campaign_page, mock.patch.object(
                pipeline, "_fetch_s2_campaign_page", side_effect=fake_s2_campaign_page
            ) as s2_campaign_page, mock.patch.object(
                pipeline, "validate_edge_cache_provenance", return_value=[]
            ), mock.patch.object(
                pipeline, "validate_screening_rows", return_value=[]
            ), mock.patch.object(
                pipeline, "load_reviewed_g04_queue", side_effect=fake_reviewed_g04_queue
            ), mock.patch.object(
                pipeline, "build_screening_ledger_rows", return_value=[]
            ):
                result = pipeline.execute_g03_citation_campaign(
                    reference_root, allow_network=False
                )

            self.assertEqual(result["seeds"], 25)
            self.assertLessEqual(result["new_identities"], 250)
            self.assertGreater(result["edges"], 0)
            self.assertEqual(result["semantic_scholar_seeds"], 25)
            self.assertEqual(
                sum(
                    call.args[3] == "SEED_RESOLUTION_BATCH"
                    for call in s2_campaign_page.call_args_list
                ),
                1,
            )
            self.assertEqual(
                sum(
                    call.args[3] in {"BACKWARD_REFERENCES", "FORWARD_CITATIONS"}
                    and call.args[7] == 0
                    for call in s2_campaign_page.call_args_list
                ),
                50,
            )
            self.assertLessEqual(
                sum(call.args[7] == 1 for call in s2_campaign_page.call_args_list),
                pipeline.MAX_S2_DEPTH2_EXPANSIONS,
            )
            self.assertEqual(
                sum(
                    call.args[3] == "SEED_RESOLUTION"
                    for call in campaign_page.call_args_list
                ),
                25,
            )
            for call in campaign_page.call_args_list:
                if call.args[3] != "SEED_RESOLUTION":
                    continue
                seed_id = call.args[5]
                canonical_version = baseline_by_id[seed_id]["canonical_version"]
                self.assertEqual(
                    call.args[4], seed_id + "|" + seed_id + canonical_version
                )
            report = (
                reference_root / "sources" / "G03-citation-ancestry-report.md"
            ).read_text(encoding="utf-8")
            self.assertIn("Exact G04 set size:", report)
            self.assertIn("Papers read | 0 | 0", report)
            self.assertIn("S2_RATE_LIMIT_ATTEMPTS_EXHAUSTED", report)
            self.assertIn("S2_SELECTED_PAYLOAD_REJECTED", report)
            self.assertIn(
                "OpenAlex and Semantic Scholar provider relations", report
            )
            self.assertIn("Semantic Scholar page limit is 75", report)
            self.assertIn("Depth-2 expansion attempts", report)
            self.assertIn("Post-Traversal Screening Review", report)
            self.assertIn("Constraint And Negative-Result Signals", report)
            self.assertIn("Survey And Review Signals", report)
            self.assertIn("Displayed", report)
            self.assertIn("Provider-backed CITES edges", report)

            first_outputs = {
                relative_path: (reference_root / relative_path).read_bytes()
                for relative_path in (
                    "sources/paper-manifest.tsv",
                    "sources/citation-edges.tsv",
                    "sources/G03-citation-ancestry-report.md",
                )
            }
            with mock.patch.object(
                pipeline, "_fetch_campaign_page", side_effect=fake_campaign_page
            ), mock.patch.object(
                pipeline, "_fetch_s2_campaign_page", side_effect=fake_s2_campaign_page
            ), mock.patch.object(
                pipeline, "validate_edge_cache_provenance", return_value=[]
            ), mock.patch.object(
                pipeline, "validate_screening_rows", return_value=[]
            ), mock.patch.object(
                pipeline, "load_reviewed_g04_queue", side_effect=fake_reviewed_g04_queue
            ), mock.patch.object(
                pipeline, "build_screening_ledger_rows", return_value=[]
            ):
                replay_result = pipeline.execute_g03_citation_campaign(
                    reference_root, allow_network=False
                )
            self.assertEqual(replay_result, result)
            for relative_path, expected in first_outputs.items():
                self.assertEqual((reference_root / relative_path).read_bytes(), expected)
            written_manifest = pipeline.read_tsv_rows(
                reference_root / "sources" / "paper-manifest.tsv",
                pipeline.MANIFEST_HEADER,
            )
            original_multi_value = next(
                row for row in baseline_rows if "|" in row["discovery_query_ids"]
            )
            written_by_id = {row["paper_id"]: row for row in written_manifest}
            self.assertEqual(
                written_by_id[original_multi_value["paper_id"]]["discovery_query_ids"],
                original_multi_value["discovery_query_ids"],
            )
            self.assertIn(
                "OPENALEX_RESOLUTION=UNAVAILABLE",
                written_by_id["PAPER-2511.07886"]["notes"],
            )

    def test_service_preflight_authorizes_exactly_openalex_metadata(self) -> None:
        preflight = PREFLIGHT_PATH.read_text(encoding="utf-8")
        self.assertEqual(pipeline.validate_g03_network_preflight(preflight), [])
        self.assertIn(
            "Exactly two citation services are authorized: OpenAlex and Semantic Scholar",
            preflight,
        )
        self.assertIn("Hard ceiling | 90", preflight)
        self.assertIn("Semantic Scholar | `AUTHORIZED_METADATA_ONLY`", preflight)

    def test_openalex_request_compiler_uses_exact_selected_fields(self) -> None:
        parameters = pipeline.build_openalex_request_parameters(
            "FORWARD_CITATIONS", "W100"
        )
        self.assertEqual(parameters["filter"], "cites:W100")
        self.assertEqual(parameters["per_page"], "100")
        self.assertEqual(parameters["select"], pipeline.OPENALEX_SELECT_FIELDS)
        forbidden = {"search", "abstract", "content", "cursor", "api_key"}
        self.assertFalse(forbidden & set(parameters))
        self.assertNotIn("abstract", parameters["select"])
        self.assertNotIn("primary_location", parameters["select"])
        exact_seed_parameters = pipeline.build_openalex_request_parameters(
            "SEED_RESOLUTION",
            "PAPER-2401.00001|PAPER-2401.00001v2",
        )
        self.assertEqual(
            exact_seed_parameters["filter"],
            "locations.landing_page_url:https://arxiv.org/abs/2401.00001|"
            "https://arxiv.org/abs/2401.00001v2",
        )

    def test_s2_request_compiler_uses_exact_ids_and_selected_fields(self) -> None:
        seed_parameters = pipeline.build_s2_request_parameters(
            "SEED_RESOLUTION_BATCH",
            "PAPER-2401.00001|PAPER-2401.00002",
        )
        self.assertEqual(
            seed_parameters["ids"],
            "ARXIV:2401.00001|ARXIV:2401.00002",
        )
        self.assertEqual(seed_parameters["fields"], pipeline.S2_SELECTED_FIELDS)
        self.assertNotIn("limit", seed_parameters)

        for operation in ("BACKWARD_REFERENCES", "FORWARD_CITATIONS"):
            parameters = pipeline.build_s2_request_parameters(
                operation, "649def34f8be52c8b66281af98ae884c09aef38b"
            )
            self.assertEqual(parameters["limit"], "75")
            self.assertEqual(parameters["offset"], "0")
            self.assertEqual(parameters["fields"], pipeline.S2_SELECTED_FIELDS)
        forbidden = {"abstract", "tldr", "openAccessPdf", "embedding", "contexts"}
        self.assertFalse(forbidden & set(pipeline.S2_SELECTED_FIELDS.split(",")))

    def test_s2_parser_rejects_content_and_preserves_exact_identities(self) -> None:
        batch_payload = (FIXTURE_ROOT / "s2-seed-batch.json").read_bytes()
        records = pipeline.parse_s2_work_payload(
            batch_payload, "SEED_RESOLUTION_BATCH"
        )
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["arxiv_id"], "2401.00001")
        self.assertEqual(records[0]["semantic_scholar_id"], "S2-SEED-1")
        self.assertEqual(records[0]["paper_id"], "PAPER-2401.00001")
        self.assertEqual(records[0]["provider_name"], "SemanticScholar")
        self.assertEqual(records[0]["openalex_id"], "UNKNOWN")
        resolved, unavailable = pipeline.resolve_s2_seed_identities(
            ["PAPER-2401.00001", "PAPER-2401.00002", "PAPER-2401.00003"],
            records,
        )
        self.assertEqual(set(resolved), {"PAPER-2401.00001", "PAPER-2401.00002"})
        self.assertEqual(unavailable, {"PAPER-2401.00003"})

        forbidden = b'{"paperId":"x","title":"x","abstract":"forbidden"}'
        with self.assertRaisesRegex(ValueError, "forbidden"):
            pipeline.parse_s2_work_payload(forbidden, "SEED_RESOLUTION_BATCH")

    def test_s2_exact_identity_records_bibliographic_variants(self) -> None:
        seed_row = {
            "paper_id": "PAPER-1407.6755",
            "title": "Dynamic Set Intersection",
            "authors": "Tsvi Kopelowitz|Seth Pettie|Ely Porat",
            "published_date": "2014-07-24",
        }
        record = {
            "arxiv_id": "1407.6755",
            "title": "Word-packing Algorithms for Dynamic Connectivity and Dynamic Sets",
            "authors": ["T. Kopelowitz", "Seth Pettie", "E. Porat"],
            "published_date": "2014-07-24",
        }
        variants = pipeline.audit_s2_seed_bibliography(
            "PAPER-1407.6755", seed_row, record
        )
        self.assertEqual(variants, ["title mismatch"])

        conflicting = dict(record)
        conflicting["published_date"] = "2021-01-01"
        with self.assertRaisesRegex(ValueError, "independent anchors"):
            pipeline.audit_s2_seed_bibliography(
                "PAPER-1407.6755", seed_row, conflicting
            )
        wrong_identity = dict(record)
        wrong_identity["arxiv_id"] = "1407.00001"
        with self.assertRaisesRegex(ValueError, "exact arXiv"):
            pipeline.audit_s2_seed_bibliography(
                "PAPER-1407.6755", seed_row, wrong_identity
            )

    def test_s2_reference_and_citation_payloads_preserve_direction(self) -> None:
        raw_references = (FIXTURE_ROOT / "s2-references.json").read_bytes()
        with self.assertRaisesRegex(ValueError, "forbidden"):
            pipeline.parse_s2_work_payload(raw_references, "BACKWARD_REFERENCES")
        sanitized_references = pipeline.sanitize_s2_metadata_payload(
            raw_references, "BACKWARD_REFERENCES"
        )
        self.assertNotIn(b"citingPaperInfo", sanitized_references)
        self.assertNotIn(b"openAccessPdf", sanitized_references)
        references = pipeline.parse_s2_work_payload(
            sanitized_references, "BACKWARD_REFERENCES"
        )
        citations = pipeline.parse_s2_work_payload(
            (FIXTURE_ROOT / "s2-citations.json").read_bytes(),
            "FORWARD_CITATIONS",
        )
        self.assertEqual(references[0]["semantic_scholar_id"], "S2-REFERENCE-1")
        self.assertTrue(
            str(references[1]["semantic_scholar_id"]).startswith("UNAVAILABLE:")
        )
        self.assertEqual(references[1]["identity_state"], "UNAVAILABLE_PROVIDER_ID")
        self.assertTrue(str(references[1]["paper_id"]).startswith("PAPER-HASH-"))
        self.assertEqual(citations[0]["semantic_scholar_id"], "S2-CITATION-1")

        target = {
            "paper_id": "PAPER-2401.00001",
            "title": "Bounded Graph Processing",
            "provider_name": "SemanticScholar",
        }
        backward = pipeline.build_provider_citation_edges(
            references[0]["paper_id"],
            target,
            "2026-08-11T00:00:00Z",
            target_title=references[0]["title"],
        )
        forward = pipeline.build_provider_citation_edges(
            target["paper_id"],
            citations[0],
            "2026-08-11T00:00:00Z",
            target_title=target["title"],
        )
        self.assertEqual(
            (backward[0]["source_paper_id"], backward[0]["target_paper_id"]),
            ("PAPER-2401.00001", references[0]["paper_id"]),
        )
        self.assertEqual(
            (forward[0]["source_paper_id"], forward[0]["target_paper_id"]),
            (citations[0]["paper_id"], "PAPER-2401.00001"),
        )
        self.assertEqual(backward[0]["discovery_source"], "SEMANTIC_SCHOLAR_API")
        self.assertEqual(forward[0]["discovery_source"], "SEMANTIC_SCHOLAR_API")

    def test_s2_request_is_ledgered_cached_and_replayed(self) -> None:
        payload = (FIXTURE_ROOT / "s2-references.json").read_bytes()

        class FakeResponse:
            status = 200
            headers = {}

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self) -> bytes:
                return payload

        with tempfile.TemporaryDirectory() as temporary_directory:
            reference_root = Path(temporary_directory) / "arxiv-reference"
            ledger_path = reference_root / "sources" / "citation-request-ledger.tsv"
            preflight = PREFLIGHT_PATH.read_text(encoding="utf-8")
            with mock.patch.object(
                pipeline.urllib.request, "urlopen", return_value=FakeResponse()
            ) as urlopen:
                first = pipeline.fetch_s2_metadata_page(
                    reference_root=reference_root,
                    ledger_path=ledger_path,
                    preflight_text=preflight,
                    operation="BACKWARD_REFERENCES",
                    identifier="S2-SEED-1",
                    seed_paper_id="PAPER-2401.00001",
                    traversal_paper_id="PAPER-2401.00001",
                    depth=0,
                    direction="BACKWARD",
                    allow_network=True,
                    remaining_http_requests=1,
                    minimum_delay_seconds=0.0,
                )
                second = pipeline.fetch_s2_metadata_page(
                    reference_root=reference_root,
                    ledger_path=ledger_path,
                    preflight_text=preflight,
                    operation="BACKWARD_REFERENCES",
                    identifier="S2-SEED-1",
                    seed_paper_id="PAPER-2401.00001",
                    traversal_paper_id="PAPER-2401.00001",
                    depth=0,
                    direction="BACKWARD",
                    allow_network=False,
                    remaining_http_requests=0,
                    minimum_delay_seconds=0.0,
                )
            self.assertEqual(first, second)
            self.assertEqual(urlopen.call_count, 1)
            request = urlopen.call_args.args[0]
            self.assertEqual(request.method, "GET")
            self.assertIn("/paper/S2-SEED-1/references?", request.full_url)
            self.assertNotIn("abstract", request.full_url)
            rows = pipeline.read_tsv_rows(ledger_path, pipeline.REQUEST_HEADER)
            self.assertEqual(rows[0]["service"], "SemanticScholar")
            self.assertEqual(
                rows[0]["response_checksum"], hashlib.sha256(payload).hexdigest()
            )
            cached_payload = (
                reference_root
                / rows[0]["cache_path"].removeprefix("arxiv-reference/")
            ).read_bytes()
            self.assertEqual(
                rows[0]["cache_checksum"],
                hashlib.sha256(cached_payload).hexdigest(),
            )
            self.assertNotEqual(
                rows[0]["response_checksum"], rows[0]["cache_checksum"]
            )
            self.assertNotIn(b"citingPaperInfo", cached_payload)
            self.assertNotIn(b"openAccessPdf", cached_payload)
            self.assertEqual(
                rows[0]["cache_path"],
                "arxiv-reference/cache/g03/semantic-scholar/REQ-G03-0001.json",
            )
            self.assertEqual(
                pipeline.validate_citation_request_rows(rows), []
            )
            self.assertEqual(
                pipeline.validate_g03_cache_provenance(reference_root, rows), []
            )

    def test_s2_rate_limit_exhaustion_is_typed_and_persistent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            reference_root = Path(temporary_directory) / "arxiv-reference"
            ledger_path = reference_root / "sources" / "citation-request-ledger.tsv"
            preflight = PREFLIGHT_PATH.read_text(encoding="utf-8")
            rate_error = pipeline.urllib.error.HTTPError(
                "https://api.semanticscholar.org/graph/v1/paper/S2-SEED/citations",
                429,
                "Too Many Requests",
                {},
                io.BytesIO(b'{"error":"rate_limited"}'),
            )
            for _attempt in range(2):
                with mock.patch.object(
                    pipeline.urllib.request, "urlopen", side_effect=rate_error
                ), self.assertRaisesRegex(RuntimeError, "status 429"):
                    pipeline.fetch_s2_metadata_page(
                        reference_root=reference_root,
                        ledger_path=ledger_path,
                        preflight_text=preflight,
                        operation="FORWARD_CITATIONS",
                        identifier="S2-SEED",
                        seed_paper_id="PAPER-2401.00001",
                        traversal_paper_id="PAPER-2401.00001",
                        depth=0,
                        direction="FORWARD",
                        allow_network=True,
                        remaining_http_requests=1,
                        minimum_delay_seconds=0.0,
                    )
                rate_error.fp = io.BytesIO(b'{"error":"rate_limited"}')
            with mock.patch.object(
                pipeline.urllib.request, "urlopen", side_effect=rate_error
            ), self.assertRaises(pipeline.CitationRateLimitExhausted):
                pipeline.fetch_s2_metadata_page(
                    reference_root=reference_root,
                    ledger_path=ledger_path,
                    preflight_text=preflight,
                    operation="FORWARD_CITATIONS",
                    identifier="S2-SEED",
                    seed_paper_id="PAPER-2401.00001",
                    traversal_paper_id="PAPER-2401.00001",
                    depth=0,
                    direction="FORWARD",
                    allow_network=True,
                    remaining_http_requests=1,
                    minimum_delay_seconds=0.0,
                )
            rows = pipeline.read_tsv_rows(ledger_path, pipeline.REQUEST_HEADER)
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[-1]["terminal_state"], "RATE_LIMITED")
            self.assertEqual(pipeline.validate_citation_request_rows(rows), [])

    def test_s2_rejected_payload_is_typed_and_replayed_without_network(self) -> None:
        class RejectedResponse:
            status = 200
            headers: dict[str, str] = {}

            def __enter__(self) -> "RejectedResponse":
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def read(self) -> bytes:
                return b'{"data":{"error":"invalid citation envelope"}}'

        with tempfile.TemporaryDirectory() as temporary_directory:
            reference_root = Path(temporary_directory) / "arxiv-reference"
            ledger_path = reference_root / "sources" / "citation-request-ledger.tsv"
            preflight = PREFLIGHT_PATH.read_text(encoding="utf-8")
            arguments = dict(
                reference_root=reference_root,
                ledger_path=ledger_path,
                preflight_text=preflight,
                operation="BACKWARD_REFERENCES",
                identifier="S2-SEED",
                seed_paper_id="PAPER-2401.00001",
                traversal_paper_id="PAPER-2401.00001",
                depth=0,
                direction="BACKWARD",
                remaining_http_requests=1,
                minimum_delay_seconds=0.0,
            )
            with mock.patch.object(
                pipeline.urllib.request, "urlopen", return_value=RejectedResponse()
            ), self.assertRaisesRegex(RuntimeError, "selected metadata response is invalid"):
                pipeline.fetch_s2_metadata_page(allow_network=True, **arguments)

            rows = pipeline.read_tsv_rows(ledger_path, pipeline.REQUEST_HEADER)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["terminal_state"], "PAYLOAD_REJECTED")
            self.assertEqual(pipeline.validate_citation_request_rows(rows), [])
            cache_path = reference_root.parent / rows[0]["cache_path"]
            self.assertEqual(
                pipeline.validate_g03_cache_provenance(reference_root, rows), []
            )
            self.assertIn(b'"error":"rejected_provider_payload"', cache_path.read_bytes())

            with self.assertRaisesRegex(RuntimeError, "payload was previously rejected"):
                pipeline.fetch_s2_metadata_page(allow_network=False, **arguments)
            self.assertEqual(
                len(pipeline.read_tsv_rows(ledger_path, pipeline.REQUEST_HEADER)), 1
            )

    def test_network_request_is_preflighted_ledgered_and_cache_resumable(self) -> None:
        payload = (FIXTURE_ROOT / "openalex-citations.json").read_bytes()

        class FakeResponse:
            status = 200
            headers = {
                "X-RateLimit-Remaining": "99",
                "X-RateLimit-Reset": "3600",
                "X-RateLimit-Credits-Used": "1",
            }

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self) -> bytes:
                return payload

            def geturl(self) -> str:
                return "https://api.openalex.org/works"

        with tempfile.TemporaryDirectory() as temporary_directory:
            reference_root = Path(temporary_directory) / "arxiv-reference"
            ledger_path = reference_root / "sources" / "citation-request-ledger.tsv"
            with mock.patch.object(
                pipeline.urllib.request, "urlopen", return_value=FakeResponse()
            ) as urlopen:
                first = pipeline.fetch_openalex_metadata_page(
                    reference_root=reference_root,
                    ledger_path=ledger_path,
                    preflight_text=PREFLIGHT_PATH.read_text(encoding="utf-8"),
                    operation="FORWARD_CITATIONS",
                    identifier="W100",
                    seed_paper_id="PAPER-2401.00001",
                    traversal_paper_id="PAPER-2401.00001",
                    depth=0,
                    direction="FORWARD",
                    allow_network=True,
                    remaining_http_requests=1,
                    minimum_delay_seconds=0.0,
                )
                second = pipeline.fetch_openalex_metadata_page(
                    reference_root=reference_root,
                    ledger_path=ledger_path,
                    preflight_text=PREFLIGHT_PATH.read_text(encoding="utf-8"),
                    operation="FORWARD_CITATIONS",
                    identifier="W100",
                    seed_paper_id="PAPER-2401.00001",
                    traversal_paper_id="PAPER-2401.00001",
                    depth=0,
                    direction="FORWARD",
                    allow_network=True,
                    remaining_http_requests=0,
                    minimum_delay_seconds=0.0,
                )
            self.assertEqual(len(first), 2)
            self.assertEqual(first, second)
            self.assertEqual(urlopen.call_count, 1)
            request_url = urlopen.call_args.args[0].full_url
            self.assertNotIn("api_key", request_url)
            rows = pipeline.read_tsv_rows(ledger_path, pipeline.REQUEST_HEADER)
            self.assertEqual(len(rows), 1)
            self.assertEqual(
                pipeline.validate_g03_cache_provenance(reference_root, rows), []
            )
            provenance_manifest = [
                {
                    "paper_id": "PAPER-2401.00001",
                    "notes": "OPENALEX_ID=https://openalex.org/W100",
                },
                *[
                    {
                        "paper_id": str(record["paper_id"]),
                        "notes": "OPENALEX_ID=" + str(record["openalex_id"]),
                    }
                    for record in first
                ],
            ]
            provenance_edges = pipeline.build_provider_citation_edges(
                "PAPER-2401.00001",
                first[0],
                "2026-08-11T00:00:00Z",
                target_title="Bounded Graph Processing",
            )
            self.assertEqual(
                pipeline.validate_edge_cache_provenance(
                    reference_root,
                    rows,
                    provenance_edges,
                    provenance_manifest,
                ),
                [],
            )
            fabricated_edges = [dict(row) for row in provenance_edges]
            fabricated_edges[0]["target_paper_id"] = first[1]["paper_id"]
            self.assertTrue(
                any(
                    "not established" in error
                    for error in pipeline.validate_edge_cache_provenance(
                        reference_root,
                        rows,
                        fabricated_edges,
                        provenance_manifest,
                    )
                )
            )

            with self.assertRaisesRegex(RuntimeError, "preflight"):
                pipeline.fetch_openalex_metadata_page(
                    reference_root=reference_root,
                    ledger_path=reference_root / "sources" / "other.tsv",
                    preflight_text="NOT AUTHORIZED",
                    operation="FORWARD_CITATIONS",
                    identifier="W100",
                    seed_paper_id="PAPER-2401.00001",
                    traversal_paper_id="PAPER-2401.00001",
                    depth=0,
                    direction="FORWARD",
                    allow_network=True,
                    remaining_http_requests=1,
                    minimum_delay_seconds=0.0,
                )

    def test_retry_budget_persists_across_process_resume(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            reference_root = Path(temporary_directory) / "arxiv-reference"
            ledger_path = reference_root / "sources" / "citation-request-ledger.tsv"
            preflight = PREFLIGHT_PATH.read_text(encoding="utf-8")
            with mock.patch.object(
                pipeline.urllib.request,
                "urlopen",
                side_effect=pipeline.urllib.error.URLError("fixture transport failure"),
            ) as first_urlopen, mock.patch.object(pipeline.time, "sleep"):
                with self.assertRaisesRegex(RuntimeError, "stopped"):
                    pipeline.fetch_openalex_metadata_page(
                        reference_root=reference_root,
                        ledger_path=ledger_path,
                        preflight_text=preflight,
                        operation="FORWARD_CITATIONS",
                        identifier="W100",
                        seed_paper_id="PAPER-2401.00001",
                        traversal_paper_id="PAPER-2401.00001",
                        depth=0,
                        direction="FORWARD",
                        allow_network=True,
                        remaining_http_requests=3,
                        minimum_delay_seconds=0.0,
                    )
            self.assertEqual(first_urlopen.call_count, 3)
            retry_rows = pipeline.read_tsv_rows(ledger_path, pipeline.REQUEST_HEADER)
            self.assertEqual(len(retry_rows), 3)
            self.assertEqual(pipeline.validate_citation_request_rows(retry_rows), [])
            broken_retry_rows = [dict(row) for row in retry_rows]
            broken_retry_rows[1]["attempt"] = "1"
            self.assertTrue(
                any(
                    "attempt sequence" in error
                    for error in pipeline.validate_citation_request_rows(
                        broken_retry_rows
                    )
                )
            )

            with mock.patch.object(
                pipeline.urllib.request,
                "urlopen",
                side_effect=AssertionError("resume must not exceed three attempts"),
            ) as resumed_urlopen:
                with self.assertRaisesRegex(RuntimeError, "attempt cap"):
                    pipeline.fetch_openalex_metadata_page(
                        reference_root=reference_root,
                        ledger_path=ledger_path,
                        preflight_text=preflight,
                        operation="FORWARD_CITATIONS",
                        identifier="W100",
                        seed_paper_id="PAPER-2401.00001",
                        traversal_paper_id="PAPER-2401.00001",
                        depth=0,
                        direction="FORWARD",
                        allow_network=True,
                        remaining_http_requests=3,
                        minimum_delay_seconds=0.0,
                    )
            resumed_urlopen.assert_not_called()

    def test_g03_cache_is_checksummed_referenced_and_content_safe(self) -> None:
        payload = (FIXTURE_ROOT / "openalex-citations.json").read_bytes()
        with tempfile.TemporaryDirectory() as temporary_directory:
            reference_root = Path(temporary_directory) / "arxiv-reference"
            cache_directory = reference_root / "cache" / "g03" / "openalex"
            cache_directory.mkdir(parents=True)
            cache_path = cache_directory / "REQ-G03-0001.json"
            cache_path.write_bytes(payload)
            row = {
                "request_id": "REQ-G03-0001",
                "goal_id": "G03",
                "seed_paper_id": "PAPER-2401.00001",
                "traversal_paper_id": "PAPER-2401.00001",
                "depth": "0",
                "direction": "FORWARD",
                "service": "OpenAlex",
                "response_checksum": hashlib.sha256(payload).hexdigest(),
                "cache_checksum": hashlib.sha256(payload).hexdigest(),
                "response_status": "200",
                "result_count": "2",
                "cache_status": "MISS",
                "attempt": "1",
                "retry_events": "NONE",
                "cache_path": "arxiv-reference/cache/g03/openalex/REQ-G03-0001.json",
                "terminal_state": "COMPLETE",
            }
            self.assertEqual(
                pipeline.validate_g03_cache_provenance(reference_root, [row]), []
            )
            (cache_directory / "unreferenced.json").write_text("{}", encoding="utf-8")
            errors = pipeline.validate_g03_cache_provenance(reference_root, [row])
            self.assertTrue(any("unreferenced" in error for error in errors), errors)
            (cache_directory / "paper.pdf").write_bytes(b"%PDF-1.7")
            errors = pipeline.validate_g03_cache_provenance(reference_root, [row])
            self.assertTrue(any("forbidden cache" in error for error in errors), errors)

    def test_final_report_has_required_decision_handoff(self) -> None:
        self.assertTrue(FINAL_REPORT_PATH.is_file())
        report = FINAL_REPORT_PATH.read_text(encoding="utf-8")
        for heading in (
            "## Executive Result",
            "## Campaign Accounting",
            "## Foundational Branches",
            "## Implementation And Evaluation Branches",
            "## Contradictory Branches",
            "## Stopped Branches",
            "## Architecture-Question Decision Impact",
            "## Coverage Gaps",
            "## Exact Recommended G04 Acquisition Set",
            "## Scope Boundary",
        ):
            self.assertIn(heading, report)

    def test_active_g03_lifecycle_preserves_verified_g02(self) -> None:
        status = (REFERENCE_ROOT / "governance" / "campaign-status.md").read_text()
        self.assertIn("- Active goal: `G05`", status)
        self.assertIn("- G02 state: `COMPLETE_VERIFIED`", status)
        self.assertIn("- G03 state: `COMPLETE_VERIFIED_CLEARED`", status)
        self.assertIn("- G04 state: `COMPLETE_VERIFIED_CLEARED`", status)
        self.assertIn("- Journal: `arxiv-reference/journals/G05-progress.md`", status)

    def test_exactly_twenty_five_frozen_seeds_are_extracted(self) -> None:
        seeds = pipeline.extract_g03_seed_ids(REPORT_PATH.read_text(encoding="utf-8"))
        self.assertEqual(len(seeds), 25)
        self.assertEqual(len(seeds), len(set(seeds)))
        self.assertEqual(seeds[0], "PAPER-2511.07886")
        self.assertEqual(seeds[-1], "PAPER-2401.01019")

    def test_openalex_fixture_omits_abstract_and_resolves_arxiv_seed(self) -> None:
        records = pipeline.parse_openalex_work_payload(
            (FIXTURE_ROOT / "openalex-seed-work.json").read_bytes()
        )
        self.assertEqual(len(records), 1)
        self.assertNotIn("abstract", records[0])
        self.assertEqual(records[0]["arxiv_id"], "2401.00001")
        self.assertEqual(
            pipeline.resolve_seed_openalex_identity("PAPER-2401.00001", records),
            "https://openalex.org/W100",
        )

    def test_empty_and_malformed_responses_are_distinct(self) -> None:
        empty_records = pipeline.parse_openalex_work_payload(
            (FIXTURE_ROOT / "openalex-empty.json").read_bytes()
        )
        self.assertEqual(empty_records, [])
        self.assertIsNone(
            pipeline.resolve_seed_openalex_identity(
                "PAPER-2401.00001", empty_records
            )
        )
        with self.assertRaises(ValueError):
            pipeline.parse_openalex_work_payload(
                (FIXTURE_ROOT / "openalex-malformed.json").read_bytes()
            )

    def test_title_collision_with_conflicting_authors_and_dates_stays_ambiguous(self) -> None:
        records = pipeline.parse_openalex_work_payload(
            (FIXTURE_ROOT / "openalex-title-collision.json").read_bytes()
        )
        identities = pipeline.reconcile_citation_identities(records, [])
        self.assertEqual(len(identities), 2)
        self.assertEqual(len({row["paper_id"] for row in identities}), 2)
        self.assertTrue(all(row["identity_state"] == "AMBIGUOUS" for row in identities))

    def test_conflicting_strong_identifiers_never_reuse_either_existing_identity(self) -> None:
        existing = [
            {
                "paper_id": "PAPER-2401.00001",
                "arxiv_id": "2401.00001",
                "doi": "10.1000/arxiv-anchor",
            },
            {
                "paper_id": "PAPER-HASH-doi-anchor",
                "arxiv_id": "UNKNOWN",
                "doi": "10.1000/doi-anchor",
            },
        ]
        record = {
            "paper_id": "PAPER-2401.00001",
            "provider_id": "S2:conflicting-anchor",
            "semantic_scholar_id": "conflicting-anchor",
            "arxiv_id": "2401.00001",
            "doi": "10.1000/doi-anchor",
            "title": "Conflicting strong identifier fixture",
            "authors": ["Fixture Author"],
            "published_date": "2024-01-01",
        }

        identities = pipeline.reconcile_citation_identities([record], existing)

        self.assertEqual(len(identities), 1)
        self.assertEqual(identities[0]["identity_state"], "AMBIGUOUS")
        self.assertNotIn(
            identities[0]["paper_id"],
            {"PAPER-2401.00001", "PAPER-HASH-doi-anchor"},
        )
        self.assertEqual(
            set(identities[0]["conflicting_identity_ids"]),
            {"PAPER-2401.00001", "PAPER-HASH-doi-anchor"},
        )

        reconciled_map = pipeline._reconcile_record_map([record], existing)
        reconciled = reconciled_map["S2:conflicting-anchor"]
        observation = {
            "paper_id": reconciled["paper_id"],
            "seed_paper_id": "PAPER-2401.00001",
            "parent_paper_id": "PAPER-2401.00001",
            "depth": 1,
            "direction": "FORWARD",
            "decision_score": 80,
            "score_breakdown": "ALG=40;MECH=25;ROLE=15;AGE=0;FALS=0",
            "architecture_question_ids": ["AQ-001"],
        }
        final_manifest = pipeline._update_manifest_rows(
            existing,
            {},
            [],
            {str(reconciled["paper_id"]): reconciled},
            [observation],
        )
        ambiguous_row = next(
            row for row in final_manifest if row["paper_id"] == reconciled["paper_id"]
        )
        self.assertIn(
            "CONFLICTING_IDENTITY_IDS=PAPER-2401.00001|PAPER-HASH-doi-anchor",
            ambiguous_row["notes"],
        )

    def test_citation_direction_is_citing_to_cited(self) -> None:
        citing = pipeline.parse_openalex_work_payload(
            (FIXTURE_ROOT / "openalex-citations.json").read_bytes()
        )[0]
        edges = pipeline.build_provider_citation_edges(
            "PAPER-2401.00001",
            citing,
            "2026-08-11T00:00:00Z",
            target_title="Bounded Graph Processing",
        )
        citation = next(row for row in edges if row["edge_type"] == "CITES")
        self.assertEqual(citation["source_paper_id"], citing["paper_id"])
        self.assertEqual(citation["target_paper_id"], "PAPER-2401.00001")

    def test_semantic_edges_are_metadata_inferences_beside_cites(self) -> None:
        citing = pipeline.parse_openalex_work_payload(
            (FIXTURE_ROOT / "openalex-citations.json").read_bytes()
        )[0]
        edges = pipeline.build_provider_citation_edges(
            "PAPER-2401.00001",
            citing,
            "2026-08-11T00:00:00Z",
            target_title="Bounded Graph Processing",
        )
        self.assertEqual({row["edge_type"] for row in edges}, {"CITES", "EVALUATES"})
        semantic = next(row for row in edges if row["edge_type"] == "EVALUATES")
        self.assertTrue(semantic["discovery_source"].endswith("_METADATA_SCREEN"))
        self.assertTrue(semantic["relevance_reason"].startswith("DERIVED_INFERENCE:"))
        self.assertNotIn("SOURCE_CLAIM", semantic["relevance_reason"])

    def test_semantic_edge_accepts_distinctive_target_prefix_before_subtitle(self) -> None:
        citing = {
            "paper_id": "PAPER-HASH-30d2a0e04327c2df",
            "title": "A CUDA implementation of the pagerank pipeline benchmark",
            "provider_name": "SemanticScholar",
        }
        edges = pipeline.build_provider_citation_edges(
            "PAPER-1603.01876",
            citing,
            "2026-08-11T00:00:00Z",
            target_title=(
                "PageRank Pipeline Benchmark: Proposal for a Holistic System "
                "Benchmark for Big-Data Platforms"
            ),
        )
        self.assertEqual({row["edge_type"] for row in edges}, {"CITES", "IMPLEMENTS"})
        semantic = next(row for row in edges if row["edge_type"] == "IMPLEMENTS")
        self.assertIn("TARGET_TITLE_ANCHORED", semantic["relevance_reason"])

    def test_reviewed_g04_queue_is_unique_and_excludes_screening_false_positives(self) -> None:
        reviewed = pipeline.load_reviewed_g04_queue(SCREENING_PATH)
        self.assertEqual(len(reviewed), 25)
        self.assertEqual(len(set(reviewed)), 25)
        self.assertNotIn("PAPER-2608.07254", reviewed)
        self.assertNotIn("PAPER-HASH-5f08934040ef557c", reviewed)
        self.assertNotIn("PAPER-HASH-6d247d25d59aa0a2", reviewed)
        self.assertNotIn("PAPER-HASH-b3b01325cae00d33", reviewed)
        self.assertNotIn("PAPER-HASH-3cd083d5fa09c1f7", reviewed)
        self.assertNotIn("PAPER-HASH-5be22f3d593ea876", reviewed)

    def test_screening_ledger_covers_every_retained_ancestry_identity(self) -> None:
        screening_rows = pipeline.read_tsv_rows(
            SCREENING_PATH, pipeline.SCREENING_HEADER
        )
        with MANIFEST_PATH.open(encoding="utf-8") as handle:
            manifest_rows = list(csv.DictReader(handle, delimiter="\t"))
        ancestry_ids = {
            row["paper_id"]
            for row in manifest_rows
            if int(pipeline._parse_notes_map(row["notes"]).get("CITATION_DEPTH", "0")) >= 1
        }
        new_ancestry_ids = {
            row["paper_id"]
            for row in manifest_rows
            if row["discovery_query_ids"] == "NOT_APPLICABLE"
            and int(pipeline._parse_notes_map(row["notes"]).get("CITATION_DEPTH", "0")) >= 1
        }

        self.assertEqual(
            {row["candidate_paper_id"] for row in screening_rows}, ancestry_ids
        )
        self.assertEqual(len(ancestry_ids), 137)
        self.assertEqual(len(screening_rows), len(ancestry_ids))
        self.assertTrue(
            {
                row["candidate_paper_id"]
                for row in screening_rows
                if row["disposition"] == "ACQUIRE"
            }.issubset(new_ancestry_ids)
        )
        self.assertTrue(
            all(row["primary_lane"] in {"G03-LANE-A", "G03-LANE-B", "G03-LANE-C"} for row in screening_rows)
        )
        self.assertTrue(all(row["audit_lane_id"] == "G03-LANE-D" for row in screening_rows))
        self.assertTrue(all(re.fullmatch(r"[0-9a-f]{64}", row["result_checksum"]) for row in screening_rows))

    def test_screening_ledger_rebuilds_from_lane_results(self) -> None:
        manifest_rows = pipeline.read_tsv_rows(
            MANIFEST_PATH, pipeline.MANIFEST_HEADER
        )
        rebuilt_rows = pipeline.build_screening_ledger_rows(
            manifest_rows, REFERENCE_ROOT
        )
        persisted_rows = pipeline.read_tsv_rows(
            SCREENING_PATH, pipeline.SCREENING_HEADER
        )

        self.assertEqual(rebuilt_rows, persisted_rows)
        self.assertEqual(len(rebuilt_rows), 137)
        self.assertEqual(
            sum(row["disposition"] == "ACQUIRE" for row in rebuilt_rows), 25
        )

    def test_populated_cache_replay_is_network_free_and_byte_identical(self) -> None:
        generated_paths = (
            "sources/paper-manifest.tsv",
            "sources/citation-request-ledger.tsv",
            "sources/citation-edges.tsv",
            "sources/citation-stops.tsv",
            "sources/citation-screening-ledger.tsv",
            "sources/G03-citation-ancestry-report.md",
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            copied_root = Path(temporary_directory) / "arxiv-reference"
            shutil.copytree(
                REFERENCE_ROOT,
                copied_root,
                ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
            )
            copied_manifest_path = copied_root / "sources" / "paper-manifest.tsv"
            copied_manifest_rows = g04_pipeline.read_tsv_rows_exact(
                copied_manifest_path
            )
            copied_queue = g04_pipeline.derive_exact_queue_records(copied_root)
            g04_pipeline.write_tsv_rows_atomic(
                copied_manifest_path,
                tuple(copied_manifest_rows[0]),
                g04_pipeline.project_manifest_before_g04(
                    copied_manifest_rows, copied_queue
                ),
            )
            before = {
                relative_path: (copied_root / relative_path).read_bytes()
                for relative_path in generated_paths
            }

            with mock.patch.object(
                pipeline.urllib.request,
                "urlopen",
                side_effect=AssertionError("populated-cache replay attempted network"),
            ):
                result = pipeline.execute_g03_citation_campaign(
                    copied_root, allow_network=False
                )

            self.assertEqual(
                result,
                {
                    "seeds": 25,
                    "requests": 83,
                    "raw_observations": 1389,
                    "baseline_identities": 262,
                    "final_identities": 377,
                    "new_identities": 115,
                    "edges": 159,
                    "stops": 1251,
                    "unavailable_seeds": 0,
                    "semantic_scholar_seeds": 25,
                },
            )
            for relative_path, expected_bytes in before.items():
                self.assertEqual(
                    (copied_root / relative_path).read_bytes(), expected_bytes
                )

    def test_stop_ledger_persists_every_exact_stopped_observation(self) -> None:
        stop_rows = pipeline.read_tsv_rows(STOP_PATH, pipeline.STOP_HEADER)
        self.assertTrue(stop_rows)
        self.assertEqual(len(stop_rows), len({row["stop_id"] for row in stop_rows}))
        self.assertTrue(
            all(
                row["candidate_identity"]
                and row["seed_paper_id"]
                and row["parent_paper_id"]
                and row["direction"] in {"SEED_RESOLUTION", "BACKWARD", "FORWARD"}
                and row["reason"]
                for row in stop_rows
            )
        )
        report = FINAL_REPORT_PATH.read_text(encoding="utf-8")
        declared = re.search(r"Exact stopped observations: \*\*(\d+)\*\*", report)
        self.assertIsNotNone(declared)
        self.assertEqual(int(declared.group(1)), len(stop_rows))
        self.assertEqual(pipeline.validate_citation_stop_rows(stop_rows), [])
        control_reasons = {
            "REQUEST_RETRY_RESERVE",
            "S2_RATE_LIMIT_ATTEMPTS_EXHAUSTED",
            "S2_SELECTED_PAYLOAD_REJECTED",
            "S2_PROVIDER_ID_UNAVAILABLE",
        }
        control_rows = [row for row in stop_rows if row["reason"] in control_reasons]
        self.assertEqual(len(control_rows), 47)
        self.assertTrue(
            all(
                row["provider_name"] in {"OpenAlex", "SemanticScholar"}
                and row["provider_id"] not in {"", "UNKNOWN"}
                and row["architecture_question_ids"] not in {"", "UNKNOWN"}
                for row in control_rows
            )
        )

        tampered = [dict(row) for row in stop_rows]
        tampered[0]["candidate_identity"] += "-tampered"
        self.assertTrue(
            any(
                "content-derived stop_id mismatch" in error
                for error in pipeline.validate_citation_stop_rows(tampered)
            )
        )

    def test_retained_new_identities_respect_each_seed_direction_quota(self) -> None:
        with MANIFEST_PATH.open(encoding="utf-8") as handle:
            manifest_rows = list(csv.DictReader(handle, delimiter="\t"))
        counts: dict[tuple[str, str], set[str]] = {}
        for row in manifest_rows:
            if row["discovery_query_ids"] != "NOT_APPLICABLE":
                continue
            notes = dict(
                clause.split("=", 1)
                for clause in row["notes"].split(";")
                if "=" in clause
            )
            seeds = notes.get("ANCESTRY_SEEDS", "").split("|")
            directions = notes.get("ANCESTRY_DIRECTIONS", "").split("|")
            for seed_id in filter(None, seeds):
                for direction in filter(None, directions):
                    counts.setdefault((seed_id, direction), set()).add(row["paper_id"])
        for (seed_id, direction), identities in counts.items():
            self.assertLessEqual(
                len(identities),
                3,
                f"{seed_id} {direction} retained {len(identities)} new identities",
            )

    def test_relation_selection_applies_quota_after_global_identity_reconciliation(self) -> None:
        candidates = []
        for seed_id in ("PAPER-SEED-A", "PAPER-SEED-B"):
            for index, paper_id in enumerate(
                ("PAPER-SHARED", f"{seed_id}-2", f"{seed_id}-3", f"{seed_id}-4")
            ):
                candidates.append(
                    {
                        "paper_id": paper_id,
                        "seed_paper_id": seed_id,
                        "parent_paper_id": seed_id,
                        "depth": 1,
                        "direction": "BACKWARD",
                        "decision_score": 100 - index,
                        "published_date": f"200{index}-01-01",
                    }
                )

        selected, stops = pipeline.select_bounded_relations(candidates, 20)

        selected_by_key: dict[tuple[str, str], set[str]] = {}
        for row in selected:
            key = (row["seed_paper_id"], row["direction"])
            selected_by_key.setdefault(key, set()).add(row["paper_id"])
        self.assertEqual(set(selected_by_key), {("PAPER-SEED-A", "BACKWARD"), ("PAPER-SEED-B", "BACKWARD")})
        self.assertTrue(all(len(values) == 3 for values in selected_by_key.values()))
        self.assertEqual(sum(row["paper_id"] == "PAPER-SHARED" for row in selected), 2)
        self.assertEqual(
            sum(row["reason"] == "PER_SEED_DIRECTION_QUOTA" for row in stops), 2
        )

    def test_bounded_selection_rejects_depth_three_and_caps_new_identities(self) -> None:
        candidates = [
            {
                "paper_id": f"PAPER-HASH-{index:016x}",
                "seed_paper_id": "PAPER-2401.00001",
                "depth": 3 if index == 0 else 2,
                "direction": "BACKWARD",
                "decision_score": 100 - (index % 100),
                "published_date": "2000-01-01",
            }
            for index in range(300)
        ]
        selected, stops = pipeline.select_bounded_candidates(candidates, 1)
        self.assertLessEqual(len(selected), 1)
        self.assertTrue(all(row["depth"] <= 2 for row in selected))
        self.assertTrue(any(row["reason"] == "MAX_DEPTH_EXCEEDED" for row in stops))
        self.assertTrue(any(row["reason"] == "GLOBAL_IDENTITY_CAP" for row in stops))

    def test_decision_score_uses_frozen_taxonomy_and_aq_links(self) -> None:
        taxonomy = pipeline.read_tsv_rows(
            REFERENCE_ROOT / "governance" / "keyword-taxonomy.tsv",
            pipeline.TAXONOMY_HEADER,
        )
        record = {
            "title": "An Evaluation of External-Memory PageRank",
            "published_date": "2008-01-01",
        }
        seed = {
            "paper_id": "PAPER-2511.07886",
            "title": "ACGraph",
            "published_date": "2025-11-11",
            "architecture_question_ids": "AQ-002|AQ-008",
        }
        score, breakdown, question_ids = pipeline.score_candidate_decision(
            record, seed, taxonomy, "BACKWARD"
        )
        self.assertGreaterEqual(score, 65)
        self.assertIn("AQ-002", question_ids)
        self.assertIn("AQ-008", question_ids)
        self.assertRegex(breakdown, r"^ALG=\d+;MECH=\d+;ROLE=\d+;AGE=\d+;FALS=\d+$")

    def test_existing_manifest_identity_does_not_consume_new_identity_cap(self) -> None:
        candidates = [
            {
                "paper_id": "PAPER-EXISTING",
                "seed_paper_id": "PAPER-SEED",
                "depth": 1,
                "direction": "BACKWARD",
                "decision_score": 90,
                "published_date": "2000-01-01",
            },
            {
                "paper_id": "PAPER-NEW",
                "seed_paper_id": "PAPER-SEED",
                "depth": 1,
                "direction": "BACKWARD",
                "decision_score": 80,
                "published_date": "2001-01-01",
            },
        ]
        selected, stops = pipeline.select_bounded_candidates(
            candidates, 0, existing_identity_ids={"PAPER-EXISTING"}
        )
        self.assertEqual([row["paper_id"] for row in selected], ["PAPER-EXISTING"])
        self.assertTrue(any(row["paper_id"] == "PAPER-NEW" and row["reason"] == "GLOBAL_IDENTITY_CAP" for row in stops))

    def test_request_provenance_fails_closed_on_caps_depth_and_checksum(self) -> None:
        rows = [
            {
                "request_id": f"REQ-G03-{index:04d}",
                "goal_id": "G03",
                "seed_paper_id": "PAPER-2401.00001",
                "traversal_paper_id": "PAPER-2401.00001",
                "depth": "3" if index == 1 else "1",
                "direction": "BACKWARD",
                "service": "OpenAlex",
                "response_checksum": "" if index == 2 else "a" * 64,
                "cache_status": "MISS",
                "attempt": "1",
                "terminal_state": "COMPLETE",
            }
            for index in range(1, 302)
        ]
        errors = pipeline.validate_citation_request_rows(rows)
        self.assertTrue(any("request cap" in error for error in errors))
        self.assertTrue(any("depth" in error for error in errors))
        self.assertTrue(any("checksum" in error for error in errors))

    def test_edge_validator_rejects_orphans_semantic_without_cites_and_claims(self) -> None:
        rows = [{
            "source_paper_id": "PAPER-HASH-1111111111111111",
            "target_paper_id": "PAPER-2401.00001",
            "edge_type": "EVALUATES",
            "discovery_source": "OPENALEX_METADATA_SCREEN",
            "relevance_reason": "SOURCE_CLAIM=proves faster",
            "verified_at": "2026-08-11T00:00:00Z",
        }]
        errors = pipeline.validate_citation_edge_contract(
            rows, {"PAPER-2401.00001"}
        )
        self.assertTrue(any("endpoint" in error for error in errors))
        self.assertTrue(any("CITES" in error for error in errors))
        self.assertTrue(any("SOURCE_CLAIM" in error for error in errors))

    def test_g03_owned_ledgers_have_exact_headers(self) -> None:
        self.assertEqual(EDGE_PATH.read_text(encoding="utf-8").splitlines()[0], pipeline.EDGE_HEADER)
        self.assertEqual(REQUEST_PATH.read_text(encoding="utf-8").splitlines()[0], pipeline.REQUEST_HEADER)
        self.assertIn("\tresponse_checksum\tcache_checksum\t", pipeline.REQUEST_HEADER)

    def test_manifest_remains_metadata_only_and_unacquired(self) -> None:
        with MANIFEST_PATH.open(encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle, delimiter="\t"))
        rows = g04_pipeline.project_manifest_before_g04(
            rows,
            g04_pipeline.derive_exact_queue_records(REFERENCE_ROOT),
        )
        self.assertTrue(all(row["selection_status"] in {"METADATA_ONLY", "UNAVAILABLE"} for row in rows))
        self.assertTrue(all(row["local_path"] == "NOT_ACQUIRED" for row in rows))
        self.assertTrue(all(row["sha256"] == "NOT_ACQUIRED" for row in rows))

    def test_no_pdf_archive_or_full_text_fixture_exists(self) -> None:
        forbidden = re.compile(r"\.(pdf|zip|tar|gz|zst|epub|html?)$", re.IGNORECASE)
        self.assertFalse(any(forbidden.search(path.name) for path in FIXTURE_ROOT.rglob("*") if path.is_file()))


if __name__ == "__main__":
    unittest.main()
