#!/usr/bin/env python3
"""Bounded, resumable G02 metadata discovery and screening pipeline."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, MutableMapping, Sequence


REQUEST_LEDGER_HEADER = (
    "request_id\tgoal_id\tquery_id\tvariant_id\tservice\toperation\t"
    "normalized_query\tparameters\trequested_at_utc\tpage_cursor\t"
    "response_status\tresult_count\tresponse_checksum\tclient_version\t"
    "cache_status\tattempt\tretry_events\trate_limit_events\tpolicy_url\t"
    "policy_checked_date\tcache_path\tterminal_state"
)
MANIFEST_HEADER = (
    "paper_id\tarxiv_id\tdoi\ttitle\tauthors\tpublished_date\tupdated_date\t"
    "categories\tabstract_url\tpdf_url\tlicense_uri\tcanonical_version\t"
    "discovery_query_ids\tarchitecture_question_ids\trelevance_score\t"
    "score_breakdown\tselection_status\tevidence_grade\tcode_urls\t"
    "local_path\tsha256\tnotes"
)
QUERY_LEDGER_HEADER = (
    "query_id\tarchitecture_question_ids\tsource_term_ids\tservice\tquery_text\t"
    "categories\tdate_from\tdate_to\texclusions\texecuted_at\tresult_count\t"
    "response_checksum\tstatus"
)
TAXONOMY_HEADER = (
    "term_id\tterm\tterm_type\tarchitecture_question_ids\tsource_repo_paths\t"
    "synonyms\thistorical_terms\tadjacent_domain_terms\texclusion_terms\tnotes"
)

ARXIV_ENDPOINT = "https://export.arxiv.org/api/query"
ARXIV_POLICY_URL = "https://info.arxiv.org/help/api/tou.html"
ARXIV_MANUAL_URL = "https://info.arxiv.org/help/api/user-manual.html"
CLIENT_VERSION = "KnightBusArxivPatternFoundry/0.1"
POLICY_CHECKED_DATE = "2026-08-11"
MAX_HTTP_REQUESTS = 200
MAX_LOGICAL_VARIANTS = 125
MAX_RAW_RECORDS = 5000
MAX_CANONICAL_CANDIDATES = 2000
MAX_RESULTS_PER_VARIANT = 15
MAX_ATTEMPTS = 3
MIN_ARXIV_INTERVAL_SECONDS = 3.1

VARIANT_BUCKETS = (
    ("ALL", None),
    ("PRE2001", ("197001010000", "200012312359")),
    ("2001_2010", ("200101010000", "201012312359")),
    ("2011_2020", ("201101010000", "202012312359")),
    ("2021_CURRENT", ("202101010000", "202608112359")),
)

REQUIRED_SUCCESSFUL_BUCKETS = {"ALL", "PRE2001", "2001_2010"}
DATE_BUCKET_LIMITS = {
    "PRE2001": ("1970-01-01", "2000-12-31"),
    "2001_2010": ("2001-01-01", "2010-12-31"),
    "2011_2020": ("2011-01-01", "2020-12-31"),
    "2021_CURRENT": ("2021-01-01", "2026-08-11"),
}

ATOM = "{http://www.w3.org/2005/Atom}"
ARXIV = "{http://arxiv.org/schemas/atom}"


def normalize_inline_text(value: str) -> str:
    """Collapse metadata whitespace and protect TSV boundaries."""

    return re.sub(r"\s+", " ", value or "").strip().replace("\t", " ").replace("\n", " ")


def normalize_doi_identity(value: str) -> str:
    """Normalize a DOI alias for deterministic identity comparison."""

    normalized = normalize_inline_text(value).casefold()
    normalized = re.sub(r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", "", normalized)
    return normalized.rstrip(".")


def normalize_title_identity(value: str) -> str:
    """Normalize a title for collision detection, never merge authorization."""

    value = unicodedata.normalize("NFKC", value).casefold()
    value = "".join(character if character.isalnum() else " " for character in value)
    return re.sub(r"\s+", " ", value).strip()


def split_arxiv_version(identifier: str) -> tuple[str, str]:
    """Split an arXiv identifier into stable base identity and version."""

    identifier = identifier.rsplit("/abs/", 1)[-1].strip()
    match = re.fullmatch(r"(.+?)(v\d+)?", identifier)
    if not match:
        return identifier, "v1"
    return match.group(1), match.group(2) or "v1"


def text_from_element(parent: ET.Element, qualified_name: str) -> str:
    """Read and normalize one optional XML element."""

    element = parent.find(qualified_name)
    return normalize_inline_text(element.text if element is not None and element.text else "")


def parse_arxiv_metadata_feed(payload: bytes) -> list[dict[str, object]]:
    """Parse one cached arXiv Atom response without reading full text."""

    try:
        root = ET.fromstring(payload)
    except ET.ParseError as error:
        raise ValueError("malformed arXiv Atom response") from error

    records: list[dict[str, object]] = []
    for entry in root.findall(ATOM + "entry"):
        identifier = text_from_element(entry, ATOM + "id")
        base_id, version = split_arxiv_version(identifier)
        title = text_from_element(entry, ATOM + "title")
        if not identifier or not title:
            raise ValueError("arXiv entry requires id and title")

        authors = [
            text_from_element(author, ATOM + "name")
            for author in entry.findall(ATOM + "author")
        ]
        authors = [author for author in authors if author]
        categories = sorted(
            {
                category.attrib.get("term", "").strip()
                for category in entry.findall(ATOM + "category")
                if category.attrib.get("term", "").strip()
            }
        )
        source_urls: list[str] = []
        abstract_url = ""
        pdf_url = ""
        for link in entry.findall(ATOM + "link"):
            href = link.attrib.get("href", "").strip()
            if not href:
                continue
            source_urls.append(href)
            if link.attrib.get("rel") == "alternate":
                abstract_url = href
            if link.attrib.get("title") == "pdf" or link.attrib.get("type") == "application/pdf":
                pdf_url = href

        if not abstract_url:
            abstract_url = "https://arxiv.org/abs/" + base_id
        if not pdf_url:
            pdf_url = "https://arxiv.org/pdf/" + base_id
        records.append(
            {
                "arxiv_id": base_id + version,
                "arxiv_base_id": base_id,
                "canonical_version": version,
                "doi": normalize_doi_identity(text_from_element(entry, ARXIV + "doi")),
                "title": title,
                "authors": authors,
                "published_date": text_from_element(entry, ATOM + "published")[:10],
                "updated_date": text_from_element(entry, ATOM + "updated")[:10],
                "categories": categories,
                "abstract_url": abstract_url,
                "pdf_url": pdf_url,
                "license_uri": text_from_element(entry, ARXIV + "license"),
                "summary": text_from_element(entry, ATOM + "summary"),
                "comment": text_from_element(entry, ARXIV + "comment"),
                "journal_reference": text_from_element(entry, ARXIV + "journal_ref"),
                "source_urls": sorted(set(source_urls + [abstract_url, pdf_url])),
            }
        )
    return records


def validate_variant_date_records(
    records: Sequence[Mapping[str, object]], variant_suffix: str
) -> list[str]:
    """Fail closed when a date-bucket response contains out-of-range records."""

    if variant_suffix == "ALL":
        return []
    if variant_suffix not in DATE_BUCKET_LIMITS:
        return [f"unknown date bucket {variant_suffix}"]
    lower_bound, upper_bound = DATE_BUCKET_LIMITS[variant_suffix]
    errors: list[str] = []
    for record in records:
        published_date = str(record.get("published_date", ""))
        if not published_date or published_date < lower_bound or published_date > upper_bound:
            errors.append(
                f"{record.get('arxiv_base_id', 'UNKNOWN')} published {published_date or 'UNKNOWN'} outside {variant_suffix}"
            )
    return errors


def version_number(value: object) -> int:
    """Convert an arXiv version token into a sortable integer."""

    match = re.fullmatch(r"v(\d+)", str(value))
    return int(match.group(1)) if match else 0


def stable_paper_identifier(record: Mapping[str, object]) -> str:
    """Build the frozen modern, legacy, or metadata-hash paper ID."""

    base_id = str(record.get("arxiv_base_id", "")).strip()
    if base_id and re.fullmatch(r"\d{4}\.\d{4,5}", base_id):
        return "PAPER-" + base_id
    if base_id:
        digest = hashlib.sha256(base_id.casefold().encode("utf-8")).hexdigest()[:16]
        return "PAPER-LEGACY-" + digest
    identity = normalize_doi_identity(str(record.get("doi", "")))
    if not identity:
        identity = "|".join(
            (
                normalize_title_identity(str(record.get("title", ""))),
                "|".join(str(value).casefold() for value in record.get("authors", [])),
                str(record.get("published_date", "")),
            )
        )
    return "PAPER-HASH-" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]


def merge_record_group(records: Sequence[Mapping[str, object]]) -> dict[str, object]:
    """Merge one deterministic identity group while preserving every alias."""

    canonical = dict(max(records, key=lambda row: (version_number(row.get("canonical_version")), str(row.get("updated_date", "")))))
    canonical["paper_id"] = stable_paper_identifier(canonical)
    canonical["versions"] = sorted(
        {str(row.get("canonical_version", "v1")) for row in records},
        key=version_number,
    )
    canonical["aliases"] = sorted(
        {
            alias
            for row in records
            for alias in (str(row.get("arxiv_id", "")), normalize_doi_identity(str(row.get("doi", ""))))
            if alias
        }
    )
    canonical["source_urls"] = sorted(
        {str(url) for row in records for url in row.get("source_urls", []) if str(url)}
    )
    canonical["identity_state"] = "CANONICAL"
    return canonical


def reconcile_canonical_paper_identities(
    records: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Reconcile versions and aliases without unsafe ambiguous merges."""

    identity_groups: MutableMapping[str, list[Mapping[str, object]]] = defaultdict(list)
    for index, record in enumerate(records):
        base_id = str(record.get("arxiv_base_id", "")).strip()
        doi = normalize_doi_identity(str(record.get("doi", "")))
        identity_key = "arxiv:" + base_id if base_id else ("doi:" + doi if doi else f"record:{index}")
        identity_groups[identity_key].append(record)

    canonical = [merge_record_group(group) for group in identity_groups.values()]
    title_groups: MutableMapping[str, list[dict[str, object]]] = defaultdict(list)
    for row in canonical:
        title_groups[normalize_title_identity(str(row.get("title", "")))].append(row)
    for title_group in title_groups.values():
        if len(title_group) < 2:
            continue
        author_sets = {tuple(str(author).casefold() for author in row.get("authors", [])) for row in title_group}
        published_dates = {str(row.get("published_date", "")) for row in title_group}
        if len(author_sets) > 1 or len(published_dates) > 1:
            for row in title_group:
                row["identity_state"] = "AMBIGUOUS"
    return sorted(canonical, key=lambda row: str(row["paper_id"]))


def is_sha256_checksum(value: str) -> bool:
    """Return whether a string is one lowercase SHA-256 digest."""

    return bool(re.fullmatch(r"[0-9a-f]{64}", value or ""))


def is_retryable_response_status(status: str) -> bool:
    """Return whether one recorded response may authorize another attempt."""

    return status in {"408", "429", "TRANSPORT_ERROR"} or (
        status.isdigit() and int(status) >= 500
    )


def validate_request_provenance_rows(
    rows: Sequence[Mapping[str, str]],
) -> list[str]:
    """Validate request caps, checksums, retries, and completed-request safety."""

    errors: list[str] = []
    external_rows = [row for row in rows if row.get("cache_status") == "MISS"]
    if len(external_rows) > MAX_HTTP_REQUESTS:
        errors.append(f"external request cap 200 exceeded: {len(external_rows)}")
    variant_ids = {row.get("variant_id", "") for row in rows if row.get("variant_id")}
    if len(variant_ids) > MAX_LOGICAL_VARIANTS:
        errors.append(f"logical variant cap 125 exceeded: {len(variant_ids)}")
    request_ids: set[str] = set()
    completed_variants: set[str] = set()
    for index, row in enumerate(rows, start=2):
        prefix = f"metadata-request-ledger.tsv: row {index}"
        request_id = row.get("request_id", "")
        if request_id in request_ids:
            errors.append(f"{prefix} duplicate request_id {request_id}")
        request_ids.add(request_id)
        if row.get("goal_id") != "G02":
            errors.append(f"{prefix} goal_id must be G02")
        try:
            attempt = int(row.get("attempt", "0"))
        except ValueError:
            attempt = 0
        if attempt < 1 or attempt > MAX_ATTEMPTS:
            errors.append(f"{prefix} attempt must be 1-3")
        if not is_sha256_checksum(row.get("response_checksum", "")):
            errors.append(f"{prefix} requires response checksum")
        variant_id = row.get("variant_id", "")
        if row.get("terminal_state") == "COMPLETE":
            if variant_id in completed_variants:
                errors.append(f"{prefix} repeated completed variant {variant_id}")
            completed_variants.add(variant_id)
        retry_events = row.get("retry_events", "")
        if retry_events not in ("", "NONE", "NOT_APPLICABLE"):
            for event in retry_events.split("|"):
                match = re.fullmatch(r"attempt-(\d+):(.+)", event)
                if not match:
                    errors.append(f"{prefix} malformed retry event {event}")
                    continue
                event_attempt = int(match.group(1))
                event_status = match.group(2)
                maximum_event_attempt = attempt - 1 if row.get("terminal_state") == "COMPLETE" else attempt
                if event_attempt < 1 or event_attempt > maximum_event_attempt:
                    errors.append(f"{prefix} retry event has invalid attempt {event_attempt}")
                if not is_retryable_response_status(event_status):
                    errors.append(f"{prefix} unsafe retry for status {event_status}")
    return sorted(set(errors))


def validate_cached_response_provenance(
    reference_root: Path,
    rows: Sequence[Mapping[str, str]],
) -> list[str]:
    """Verify every cached body and reject unreferenced or full-text cache files."""

    errors: list[str] = []
    cache_root = (reference_root / "cache" / "g02").resolve()
    referenced_paths: set[Path] = set()
    forbidden_suffixes = {
        ".7z", ".doc", ".docx", ".epub", ".gz", ".html", ".md",
        ".pdf", ".tar", ".tex", ".txt", ".xz", ".zip",
    }
    forbidden_signatures = (b"%PDF-", b"PK\x03\x04", b"\x1f\x8b")

    for index, row in enumerate(rows, start=2):
        prefix = f"metadata-request-ledger.tsv: row {index}"
        recorded_path = row.get("cache_path", "")
        relative_path = Path(recorded_path)
        if not recorded_path or relative_path.is_absolute():
            errors.append(f"{prefix} cache_path must be repository-relative")
            continue
        cache_path = (reference_root.parent / relative_path).resolve()
        try:
            cache_path.relative_to(cache_root)
        except ValueError:
            errors.append(f"{prefix} cache_path escapes cache/g02")
            continue
        referenced_paths.add(cache_path)
        if not cache_path.is_file() or cache_path.is_symlink():
            errors.append(f"{prefix} cached response is missing or not a regular file")
            continue
        payload = cache_path.read_bytes()
        if checksum_response_payload(payload) != row.get("response_checksum", ""):
            errors.append(f"{prefix} cached response checksum mismatch")
        if cache_path.suffix.casefold() in forbidden_suffixes or payload.startswith(forbidden_signatures):
            errors.append(f"{prefix} forbidden cache content")
        if row.get("response_status") == "200":
            try:
                records = parse_arxiv_metadata_feed(payload)
            except ValueError as error:
                errors.append(f"{prefix} cached Atom response is malformed: {error}")
                continue
            if str(len(records)) != row.get("result_count", ""):
                errors.append(f"{prefix} cached response result_count mismatch")
            if row.get("terminal_state") == "COMPLETE":
                variant_id = row.get("variant_id", "")
                suffix = next(
                    (
                        bucket
                        for bucket in ("PRE2001", "2001_2010", "2011_2020", "2021_CURRENT")
                        if variant_id.endswith("-" + bucket)
                    ),
                    "ALL",
                )
                for date_error in validate_variant_date_records(records, suffix):
                    errors.append(f"{prefix} {date_error}")

    if cache_root.is_dir():
        for cache_path in sorted(cache_root.rglob("*")):
            if cache_path.is_dir() and not cache_path.is_symlink():
                continue
            resolved_path = cache_path.resolve()
            payload_prefix = b""
            if cache_path.is_file() and not cache_path.is_symlink():
                payload_prefix = cache_path.read_bytes()[:8]
            if resolved_path not in referenced_paths:
                errors.append(
                    "{0}: unreferenced cache file".format(
                        resolved_path.relative_to(reference_root.resolve()).as_posix()
                    )
                )
            if (
                cache_path.suffix.casefold() in forbidden_suffixes
                or payload_prefix.startswith(forbidden_signatures)
            ):
                errors.append(
                    "{0}: forbidden cache content".format(
                        resolved_path.relative_to(reference_root.resolve()).as_posix()
                    )
                )
    return sorted(set(errors))


def validate_query_aggregate_provenance(
    query_rows: Sequence[Mapping[str, str]],
    request_rows: Sequence[Mapping[str, str]],
) -> list[str]:
    """Recompute canonical query counts, timestamps, and response digests."""

    errors: list[str] = []
    rows_by_query: MutableMapping[str, list[Mapping[str, str]]] = defaultdict(list)
    for request_row in request_rows:
        if request_row.get("terminal_state") == "COMPLETE":
            rows_by_query[request_row.get("query_id", "")].append(request_row)
    for index, query_row in enumerate(query_rows, start=2):
        if query_row.get("status") != "EXECUTED":
            continue
        prefix = f"query-ledger.tsv: row {index}"
        complete_rows = rows_by_query.get(query_row.get("query_id", ""), [])
        expected_variants = {
            f"{query_row.get('query_id', '')}-{suffix}"
            for suffix in REQUIRED_SUCCESSFUL_BUCKETS
        }
        if {row.get("variant_id", "") for row in complete_rows} != expected_variants:
            errors.append(f"{prefix} aggregate variants mismatch")
            continue
        expected_count = sum(int(row.get("result_count", "0")) for row in complete_rows)
        if query_row.get("result_count") != str(expected_count):
            errors.append(f"{prefix} aggregate result_count mismatch")
        expected_time = max(row.get("requested_at_utc", "") for row in complete_rows)
        if query_row.get("executed_at") != expected_time:
            errors.append(f"{prefix} aggregate executed_at mismatch")
        ordered_checksums = "\n".join(
            row.get("response_checksum", "")
            for row in sorted(complete_rows, key=lambda row: row.get("variant_id", ""))
        )
        expected_checksum = hashlib.sha256(ordered_checksums.encode("ascii")).hexdigest()
        if query_row.get("response_checksum") != expected_checksum:
            errors.append(f"{prefix} aggregate response_checksum mismatch")
    return sorted(set(errors))


SCORE_KEYS = ("MR", "RR", "IS", "BS", "TR", "FL", "ND")
SCORE_LIMITS = {"MR": 20, "RR": 20, "IS": 15, "BS": 15, "TR": 15, "FL": 10, "ND": 5}


def parse_score_breakdown(value: str) -> dict[str, int] | None:
    """Parse the frozen G02 score serialization."""

    parts = value.split(";")
    if len(parts) != len(SCORE_KEYS):
        return None
    parsed: dict[str, int] = {}
    for expected_key, part in zip(SCORE_KEYS, parts):
        match = re.fullmatch(r"([A-Z]{2})=(\d+)", part)
        if not match or match.group(1) != expected_key:
            return None
        parsed[expected_key] = int(match.group(2))
    return parsed


def split_pipe_values(value: str) -> set[str]:
    """Split one frozen pipe-delimited field."""

    return {part.strip() for part in value.split("|") if part.strip()}


def validate_metadata_manifest_rows(
    rows: Sequence[Mapping[str, str]],
    valid_query_ids: set[str],
    valid_question_ids: set[str],
) -> list[str]:
    """Validate G02 metadata-only manifest and AQ/QRY provenance."""

    errors: list[str] = []
    if len(rows) > MAX_CANONICAL_CANDIDATES:
        errors.append(f"canonical candidate cap 2000 exceeded: {len(rows)}")
    paper_ids: set[str] = set()
    arxiv_ids: set[str] = set()
    dois: set[str] = set()
    expected_fields = MANIFEST_HEADER.split("\t")
    for index, row in enumerate(rows, start=2):
        prefix = f"paper-manifest.tsv: row {index}"
        for field in expected_fields:
            if not str(row.get(field, "")).strip():
                errors.append(f"{prefix} requires {field}")
        paper_id = row.get("paper_id", "")
        if paper_id in paper_ids:
            errors.append(f"{prefix} duplicate paper_id {paper_id}")
        paper_ids.add(paper_id)
        arxiv_id = split_arxiv_version(row.get("arxiv_id", ""))[0]
        if arxiv_id not in {"", "UNKNOWN"}:
            if arxiv_id in arxiv_ids:
                errors.append(f"{prefix} duplicate canonical arXiv identity {arxiv_id}")
            arxiv_ids.add(arxiv_id)
        doi = normalize_doi_identity(row.get("doi", ""))
        if doi not in {"", "unknown"}:
            if doi in dois:
                errors.append(f"{prefix} duplicate DOI identity {doi}")
            dois.add(doi)

        query_ids = split_pipe_values(row.get("discovery_query_ids", ""))
        question_ids = split_pipe_values(row.get("architecture_question_ids", ""))
        if not query_ids or not query_ids <= valid_query_ids:
            errors.append(f"{prefix} has invalid discovery query provenance")
        if not question_ids or not question_ids <= valid_question_ids:
            errors.append(f"{prefix} has invalid architecture question provenance")
        if row.get("selection_status") != "METADATA_ONLY":
            errors.append(f"{prefix} selection_status must be METADATA_ONLY")
        if row.get("evidence_grade") != "D_THEORETICAL_OR_INCOMPLETE":
            errors.append(f"{prefix} evidence_grade must remain metadata placeholder")
        if row.get("local_path") != "NOT_ACQUIRED" or row.get("sha256") != "NOT_ACQUIRED":
            errors.append(f"{prefix} metadata-only row cannot acquire full text")
        if "SOURCE_CLAIM" in " ".join(str(value) for value in row.values()):
            errors.append(f"{prefix} metadata-only row cannot produce SOURCE_CLAIM")

        breakdown = parse_score_breakdown(row.get("score_breakdown", ""))
        try:
            score = int(row.get("relevance_score", "-1"))
        except ValueError:
            score = -1
        if breakdown is None:
            errors.append(f"{prefix} has invalid score breakdown")
        else:
            if any(breakdown[key] > SCORE_LIMITS[key] for key in SCORE_KEYS):
                errors.append(f"{prefix} score component exceeds its cap")
            if score != sum(breakdown.values()) or score < 0 or score > 100:
                errors.append(f"{prefix} relevance score does not match breakdown")
        required_note_keys = (
            "ALIASES=", "VERSIONS=", "SOURCE_URLS=", "DISCOVERY_ERAS=",
            "NEIGHBORING_DOMAIN=", "PRE_ARXIV_ANCESTRY=", "IDENTITY_STATE=",
        )
        for marker in required_note_keys:
            if marker not in row.get("notes", ""):
                errors.append(f"{prefix} notes require {marker[:-1]}")
    return sorted(set(errors))


def lexical_component_score(text: str, vocabulary: Iterable[str], weight: int, cap: int) -> int:
    """Score unique case-folded metadata vocabulary hits."""

    hits = sum(1 for term in vocabulary if term in text)
    return min(cap, hits * weight)


def build_metadata_screen_score(record: Mapping[str, object]) -> tuple[int, str]:
    """Build the transparent 0-100 metadata-screening score."""

    text = " ".join(
        str(record.get(field, ""))
        for field in ("title", "summary", "comment", "journal_reference", "categories")
    ).casefold()
    values = {
        "MR": lexical_component_score(text, ("pagerank", "breadth-first", "bfs", "connected component", "triangle", "louvain", "leiden", "similarity", "nearest neighbor", "knn", "hnsw", "graph processing", "graph algorithm"), 4, 20),
        "RR": lexical_component_score(text, ("memory", "ram", "external-memory", "out-of-core", "semi-external", "streaming", "bounded", "buffer", "spill", "disk", "i/o", "cache", "numa", "page fault"), 3, 20),
        "IS": lexical_component_score(text, ("implementation", "prototype", "system", "engine", "framework", "library", "code", "open source"), 3, 15),
        "BS": lexical_component_score(text, ("benchmark", "evaluation", "experiment", "performance", "latency", "throughput", "dataset", "measurement"), 3, 15),
        "TR": lexical_component_score(text, ("layout", "storage", "index", "partition", "compression", "scheduling", "format", "preprocessing", "materialized", "deterministic"), 3, 15),
        "FL": lexical_component_score(text, ("limitation", "failure", "tradeoff", "drawback", "overhead", "regression", "bottleneck", "worst case"), 2, 10),
        "ND": lexical_component_score(text, ("operating system", "database", "information retrieval", "real-time", "embedded", "external sort", "vector search"), 1, 5),
    }
    breakdown = ";".join(f"{key}={values[key]}" for key in SCORE_KEYS)
    return sum(values.values()), breakdown


def read_tsv_rows(path: Path, expected_header: str) -> list[dict[str, str]]:
    """Read a frozen-header TSV file."""

    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != expected_header:
        raise ValueError(f"invalid TSV header: {path}")
    return list(csv.DictReader(lines[1:], fieldnames=expected_header.split("\t"), delimiter="\t"))


def write_tsv_rows(path: Path, header: str, rows: Sequence[Mapping[str, str]]) -> None:
    """Atomically write deterministic TSV rows."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    with temporary_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header.split("\t"), delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    temporary_path.replace(path)


def quote_arxiv_phrase(value: str) -> str:
    """Encode one repository term as an arXiv all-field phrase."""

    value = normalize_inline_text(value).replace('"', "")
    return f'all:"{value}"'


def quote_arxiv_anchor(value: str) -> str:
    """Encode one recognizable query anchor as a token or phrase."""

    value = normalize_inline_text(value).replace('"', "")
    if re.fullmatch(r"[A-Za-z0-9]+", value):
        return "all:" + value
    return quote_arxiv_phrase(value)


def select_algorithm_query_anchors(term: Mapping[str, str]) -> list[str]:
    """Select conservative algorithm names already present in one taxonomy row."""

    vocabulary = " ".join((term.get("term", ""), term.get("synonyms", ""))).casefold()
    known_anchors = (
        ("pagerank", ("PageRank",)),
        ("breadth-first", ("BFS", "breadth-first search")),
        ("bounded bfs", ("BFS", "bounded reachability")),
        ("weakly connected", ("WCC", "connected components")),
        ("afforest", ("Afforest",)),
        ("triangle", ("triangle counting",)),
        ("set intersection", ("set intersection",)),
        ("louvain", ("Louvain",)),
        ("leiden", ("Leiden",)),
        ("nodesimilarity", ("NodeSimilarity", "node similarity")),
        ("property-vector knn", ("kNN", "nearest neighbor")),
    )
    anchors: list[str] = []
    for marker, candidates in known_anchors:
        if marker in vocabulary:
            anchors.extend(candidates)
    if anchors:
        return list(dict.fromkeys(anchors))
    tokens = re.findall(r"[A-Za-z][A-Za-z0-9-]{2,}", term.get("term", ""))
    return tokens[:3]


def select_mechanism_query_anchors(term: Mapping[str, str]) -> list[str]:
    """Select linked mechanism tokens without introducing outside vocabulary."""

    source = " ".join((term.get("term", ""), term.get("synonyms", "").replace("|", " ")))
    stop_tokens = {
        "algorithm", "algorithms", "graph", "graphs", "only", "with", "from",
        "state", "custom", "shared", "common", "query", "profile", "artifact",
    }
    anchors: list[str] = []
    for token in re.findall(r"[A-Za-z][A-Za-z0-9-]{2,}", source):
        if token.casefold() in stop_tokens:
            continue
        anchors.append(token)
    return list(dict.fromkeys(anchors))[:16]


def build_arxiv_query_variant(
    query_row: Mapping[str, str],
    terms_by_id: Mapping[str, Mapping[str, str]],
    date_range: tuple[str, str] | None,
) -> str:
    """Translate one G01 family into bounded arXiv syntax without semantic expansion."""

    algorithm_phrases: list[str] = []
    mechanism_phrases: list[str] = []
    for term_id in query_row["source_term_ids"].split("|"):
        term = terms_by_id[term_id]
        if term["term_type"] == "ALGORITHM":
            algorithm_phrases.extend(
                quote_arxiv_anchor(anchor)
                for anchor in select_algorithm_query_anchors(term)
            )
        else:
            mechanism_phrases.extend(
                quote_arxiv_anchor(anchor)
                for anchor in select_mechanism_query_anchors(term)
            )
    if not algorithm_phrases or not mechanism_phrases:
        raise ValueError(f"{query_row['query_id']} is not a compound query family")
    query_parts = [
        "(" + " OR ".join(dict.fromkeys(algorithm_phrases)) + ")",
        "(" + " OR ".join(dict.fromkeys(mechanism_phrases)) + ")",
    ]
    categories = [category for category in query_row["categories"].split("|") if category]
    if categories:
        query_parts.append("(" + " OR ".join(f"cat:{category}" for category in categories) + ")")
    if date_range:
        query_parts.append(f"submittedDate:[{date_range[0]} TO {date_range[1]}]")
    query = " AND ".join(query_parts)
    for exclusion in query_row["exclusions"].split("|"):
        if exclusion:
            query += " ANDNOT " + quote_arxiv_phrase(exclusion)
    return query


def utc_timestamp_now() -> str:
    """Return one reproducible UTC timestamp encoding."""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def checksum_response_payload(payload: bytes) -> str:
    """Hash one exact cached response body."""

    return hashlib.sha256(payload).hexdigest()


def encode_request_parameters(parameters: Mapping[str, str]) -> str:
    """Serialize non-secret request parameters in sorted order."""

    return "&".join(
        f"{urllib.parse.quote(key, safe='')}={urllib.parse.quote(str(value), safe='')}"
        for key, value in sorted(parameters.items())
    )


def append_request_ledger_row(path: Path, row: Mapping[str, str]) -> None:
    """Checkpoint one external operation at a request boundary."""

    existing = read_tsv_rows(path, REQUEST_LEDGER_HEADER) if path.exists() else []
    write_tsv_rows(path, REQUEST_LEDGER_HEADER, [*existing, row])


def make_request_ledger_row(
    request_number: int,
    query_id: str,
    variant_id: str,
    normalized_query: str,
    parameters: Mapping[str, str],
    requested_at: str,
    response_status: str,
    result_count: int,
    checksum: str,
    cache_status: str,
    attempt: int,
    retry_events: str,
    rate_limit_events: str,
    cache_path: str,
    terminal_state: str,
) -> dict[str, str]:
    """Create one complete frozen-schema request provenance row."""

    return {
        "request_id": f"REQ-G02-{request_number:04d}",
        "goal_id": "G02",
        "query_id": query_id,
        "variant_id": variant_id,
        "service": "arXiv",
        "operation": "legacy_api_query_metadata",
        "normalized_query": normalized_query.replace("\t", " "),
        "parameters": encode_request_parameters(parameters),
        "requested_at_utc": requested_at,
        "page_cursor": "0",
        "response_status": response_status,
        "result_count": str(result_count),
        "response_checksum": checksum,
        "client_version": CLIENT_VERSION,
        "cache_status": cache_status,
        "attempt": str(attempt),
        "retry_events": retry_events,
        "rate_limit_events": rate_limit_events,
        "policy_url": ARXIV_POLICY_URL,
        "policy_checked_date": POLICY_CHECKED_DATE,
        "cache_path": cache_path,
        "terminal_state": terminal_state,
    }


def fetch_arxiv_variant_payload(
    root: Path,
    query_id: str,
    variant_id: str,
    normalized_query: str,
    request_ledger_path: Path,
    request_number: int,
    last_request_time: float,
    remaining_http_requests: int = MAX_HTTP_REQUESTS,
) -> tuple[bytes, int, float, int]:
    """Fetch one variant with cache-first, bounded, respectful retry behavior."""

    cache_directory = root / "cache" / "g02" / "arxiv"
    cache_directory.mkdir(parents=True, exist_ok=True)
    cache_path = cache_directory / f"{variant_id}.xml"
    relative_cache_path = cache_path.relative_to(root.parent).as_posix()
    parameters = {
        "search_query": normalized_query,
        "start": "0",
        "max_results": str(MAX_RESULTS_PER_VARIANT),
        "sortBy": "relevance",
        "sortOrder": "descending",
    }
    if cache_path.is_file():
        payload = cache_path.read_bytes()
        count = len(parse_arxiv_metadata_feed(payload))
        row = make_request_ledger_row(
            request_number, query_id, variant_id, normalized_query, parameters,
            utc_timestamp_now(), "200", count, checksum_response_payload(payload),
            "HIT", 1, "NONE", "NONE", relative_cache_path, "COMPLETE",
        )
        append_request_ledger_row(request_ledger_path, row)
        return payload, request_number + 1, last_request_time, 0

    external_requests = 0
    retry_notes: list[str] = []
    for attempt in range(1, MAX_ATTEMPTS + 1):
        if external_requests >= remaining_http_requests:
            raise RuntimeError(f"external request cap reached during retries for {variant_id}")
        delay = MIN_ARXIV_INTERVAL_SECONDS - (time.monotonic() - last_request_time)
        if delay > 0:
            time.sleep(delay)
        requested_at = utc_timestamp_now()
        request = urllib.request.Request(
            ARXIV_ENDPOINT + "?" + urllib.parse.urlencode(parameters),
            headers={"User-Agent": CLIENT_VERSION, "Accept": "application/atom+xml"},
            method="GET",
        )
        status = "TRANSPORT_ERROR"
        payload = b""
        retry_after = "NONE"
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                status = str(response.status)
                payload = response.read()
                retry_after = response.headers.get("Retry-After", "NONE")
        except urllib.error.HTTPError as error:
            status = str(error.code)
            payload = error.read()
            retry_after = error.headers.get("Retry-After", "NONE")
        except (urllib.error.URLError, TimeoutError) as error:
            payload = str(error).encode("utf-8", errors="replace")
        last_request_time = time.monotonic()
        external_requests += 1
        checksum = checksum_response_payload(payload)

        if status == "200":
            cache_path.write_bytes(payload)
            records = parse_arxiv_metadata_feed(payload)
            row = make_request_ledger_row(
                request_number, query_id, variant_id, normalized_query, parameters,
                requested_at, status, len(records), checksum, "MISS", attempt,
                "|".join(retry_notes) or "NONE", "NONE", relative_cache_path, "COMPLETE",
            )
            append_request_ledger_row(request_ledger_path, row)
            return payload, request_number + 1, last_request_time, external_requests

        error_cache_path = cache_directory / f"{variant_id}.attempt-{attempt}.error"
        error_cache_path.write_bytes(payload)
        error_relative_path = error_cache_path.relative_to(root.parent).as_posix()
        retryable = is_retryable_response_status(status)
        terminal_state = "RATE_LIMITED" if status == "429" and attempt == MAX_ATTEMPTS else "FAILED"
        retry_event = f"attempt-{attempt}:{status}"
        row = make_request_ledger_row(
            request_number, query_id, variant_id, normalized_query, parameters,
            requested_at, status, 0, checksum, "MISS", attempt,
            retry_event if retryable else "NONE",
            f"Retry-After={retry_after}" if status == "429" else "NONE",
            error_relative_path, terminal_state,
        )
        append_request_ledger_row(request_ledger_path, row)
        request_number += 1
        if not retryable or status in {"401", "403"}:
            raise RuntimeError(f"non-retryable arXiv response {status} for {variant_id}")
        retry_notes.append(retry_event)
        if attempt < MAX_ATTEMPTS:
            if retry_after.isdigit():
                time.sleep(min(60, int(retry_after)))
            else:
                time.sleep(min(30.0, (2 ** (attempt - 1)) + random.uniform(0.1, 0.5)))
    raise RuntimeError(f"arXiv retries exhausted for {variant_id}")


def serialize_manifest_notes(record: Mapping[str, object]) -> str:
    """Serialize required identity and exploration metadata notes."""

    published_date = str(record.get("published_date", ""))
    pre_arxiv = "YES_G03_REQUIRED" if published_date and published_date < "1991-01-01" else "NO"
    neighboring = "YES" if int(str(record.get("score_components", {}).get("ND", 0))) > 0 else "NO"
    note_values = {
        "ALIASES": "|".join(str(value).replace("|", "%7C") for value in record.get("aliases", [])) or "NONE",
        "DISCOVERY_ERAS": "|".join(sorted(record.get("discovery_eras", set()))) or "UNKNOWN",
        "IDENTITY_STATE": str(record.get("identity_state", "CANONICAL")),
        "NEIGHBORING_DOMAIN": neighboring,
        "PRE_ARXIV_ANCESTRY": pre_arxiv,
        "SOURCE_URLS": "|".join(str(value).replace("|", "%7C") for value in record.get("source_urls", [])) or "UNKNOWN",
        "VERSIONS": "|".join(str(value) for value in record.get("versions", [])) or str(record.get("canonical_version", "v1")),
    }
    return ";".join(f"{key}={note_values[key]}" for key in sorted(note_values))


def build_manifest_row(record: Mapping[str, object]) -> dict[str, str]:
    """Convert one reconciled metadata record into the frozen manifest schema."""

    score, breakdown = build_metadata_screen_score(record)
    score_components = parse_score_breakdown(breakdown) or {}
    enriched = dict(record)
    enriched["score_components"] = score_components
    return {
        "paper_id": str(record["paper_id"]),
        "arxiv_id": str(record.get("arxiv_base_id", "UNKNOWN")) or "UNKNOWN",
        "doi": str(record.get("doi", "UNKNOWN")) or "UNKNOWN",
        "title": normalize_inline_text(str(record.get("title", "UNKNOWN"))).replace("|", "%7C"),
        "authors": "|".join(str(author).replace("|", "%7C") for author in record.get("authors", [])) or "UNKNOWN",
        "published_date": str(record.get("published_date", "UNKNOWN")) or "UNKNOWN",
        "updated_date": str(record.get("updated_date", "UNKNOWN")) or "UNKNOWN",
        "categories": "|".join(str(category) for category in record.get("categories", [])) or "UNKNOWN",
        "abstract_url": str(record.get("abstract_url", "UNKNOWN")) or "UNKNOWN",
        "pdf_url": str(record.get("pdf_url", "UNKNOWN")) or "UNKNOWN",
        "license_uri": str(record.get("license_uri", "UNKNOWN")) or "UNKNOWN",
        "canonical_version": str(record.get("canonical_version", "v1")),
        "discovery_query_ids": "|".join(sorted(record.get("discovery_query_ids", set()))) or "UNKNOWN",
        "architecture_question_ids": "|".join(sorted(record.get("architecture_question_ids", set()))) or "UNKNOWN",
        "relevance_score": str(score),
        "score_breakdown": breakdown,
        "selection_status": "METADATA_ONLY",
        "evidence_grade": "D_THEORETICAL_OR_INCOMPLETE",
        "code_urls": "UNKNOWN",
        "local_path": "NOT_ACQUIRED",
        "sha256": "NOT_ACQUIRED",
        "notes": serialize_manifest_notes(enriched),
    }


def update_query_execution_rows(
    query_rows: Sequence[Mapping[str, str]],
    request_rows: Sequence[Mapping[str, str]],
) -> list[dict[str, str]]:
    """Aggregate terminal variant provenance back into the 25 query families."""

    rows_by_query: MutableMapping[str, list[Mapping[str, str]]] = defaultdict(list)
    for row in request_rows:
        rows_by_query[row.get("query_id", "")].append(row)
    updated: list[dict[str, str]] = []
    for source_row in query_rows:
        row = dict(source_row)
        operation_rows = rows_by_query.get(row["query_id"], [])
        complete_rows = [item for item in operation_rows if item.get("terminal_state") == "COMPLETE"]
        completed_variants = {item["variant_id"] for item in complete_rows}
        expected_variants = {
            f"{row['query_id']}-{suffix}" for suffix in REQUIRED_SUCCESSFUL_BUCKETS
        }
        if completed_variants == expected_variants:
            row["status"] = "EXECUTED"
            row["executed_at"] = max(item["requested_at_utc"] for item in complete_rows)
            row["result_count"] = str(sum(int(item["result_count"]) for item in complete_rows))
            ordered_checksums = "\n".join(
                item["response_checksum"] for item in sorted(complete_rows, key=lambda item: item["variant_id"])
            )
            row["response_checksum"] = hashlib.sha256(ordered_checksums.encode("ascii")).hexdigest()
        elif any(item.get("terminal_state") == "RATE_LIMITED" for item in operation_rows):
            row["status"] = "RATE_LIMITED"
        elif operation_rows and not complete_rows:
            row["status"] = "FAILED"
        updated.append(row)
    return updated


def execute_g02_discovery_campaign(root: Path, allow_network: bool) -> int:
    """Execute or resume the bounded G02 discovery campaign."""

    if not allow_network:
        raise RuntimeError("network execution requires --allow-network")
    preflight_path = root / "governance" / "g02-service-preflight.md"
    if not preflight_path.is_file() or "arXiv decision: `AUTHORIZED`" not in preflight_path.read_text(encoding="utf-8"):
        raise RuntimeError("arXiv service preflight is not authorized")

    query_path = root / "governance" / "query-ledger.tsv"
    taxonomy_path = root / "governance" / "keyword-taxonomy.tsv"
    request_path = root / "sources" / "metadata-request-ledger.tsv"
    manifest_path = root / "sources" / "paper-manifest.tsv"
    query_rows = read_tsv_rows(query_path, QUERY_LEDGER_HEADER)
    taxonomy_rows = read_tsv_rows(taxonomy_path, TAXONOMY_HEADER)
    if len(query_rows) != 25:
        raise RuntimeError("G02 requires exactly 25 G01 query families")
    terms_by_id = {row["term_id"]: row for row in taxonomy_rows}
    if not request_path.exists():
        write_tsv_rows(request_path, REQUEST_LEDGER_HEADER, [])

    request_rows = read_tsv_rows(request_path, REQUEST_LEDGER_HEADER)
    errors = validate_request_provenance_rows(request_rows)
    if errors:
        raise RuntimeError("invalid existing request ledger: " + "; ".join(errors))
    completed_variants = {
        row["variant_id"] for row in request_rows if row.get("terminal_state") == "COMPLETE"
    }
    intentionally_unretried_variants = {
        row["variant_id"]
        for row in request_rows
        if row.get("operation")
        == "legacy_api_query_metadata_invalidated_date_filter_not_retried_request_cap"
    }
    request_number = len(request_rows) + 1
    external_requests = len([row for row in request_rows if row.get("cache_status") == "MISS"])
    raw_records: list[dict[str, object]] = []
    last_request_time = 0.0

    for query_row in query_rows:
        for suffix, date_range in VARIANT_BUCKETS:
            variant_id = f"{query_row['query_id']}-{suffix}"
            normalized_query = build_arxiv_query_variant(query_row, terms_by_id, date_range)
            if variant_id in intentionally_unretried_variants:
                continue
            if variant_id in completed_variants:
                cache_path = root / "cache" / "g02" / "arxiv" / f"{variant_id}.xml"
                if not cache_path.is_file():
                    raise RuntimeError(f"completed variant lacks cache: {variant_id}")
                payload = cache_path.read_bytes()
            else:
                if external_requests >= MAX_HTTP_REQUESTS:
                    raise RuntimeError("external request cap reached")
                payload, request_number, last_request_time, request_delta = fetch_arxiv_variant_payload(
                    root, query_row["query_id"], variant_id, normalized_query,
                    request_path, request_number, last_request_time,
                    remaining_http_requests=MAX_HTTP_REQUESTS - external_requests,
                )
                external_requests += request_delta
            records = parse_arxiv_metadata_feed(payload)
            date_errors = validate_variant_date_records(records, suffix)
            if date_errors:
                raise RuntimeError("invalid date-bucket response: " + "; ".join(date_errors[:5]))
            if len(raw_records) + len(records) > MAX_RAW_RECORDS:
                raise RuntimeError("raw metadata record cap reached")
            for record in records:
                record["discovery_query_ids"] = {query_row["query_id"]}
                record["architecture_question_ids"] = set(query_row["architecture_question_ids"].split("|"))
                record["discovery_eras"] = {suffix}
                raw_records.append(record)

    canonical = reconcile_canonical_paper_identities(raw_records)
    if len(canonical) > MAX_CANONICAL_CANDIDATES:
        raise RuntimeError("canonical candidate cap reached")
    # Reconciliation groups version identities; aggregate discovery provenance separately.
    provenance_by_base: MutableMapping[str, dict[str, set[str]]] = defaultdict(lambda: {"queries": set(), "questions": set(), "eras": set()})
    for record in raw_records:
        key = str(record.get("arxiv_base_id", ""))
        provenance_by_base[key]["queries"].update(record.get("discovery_query_ids", set()))
        provenance_by_base[key]["questions"].update(record.get("architecture_question_ids", set()))
        provenance_by_base[key]["eras"].update(record.get("discovery_eras", set()))
    for record in canonical:
        provenance = provenance_by_base[str(record.get("arxiv_base_id", ""))]
        record["discovery_query_ids"] = provenance["queries"]
        record["architecture_question_ids"] = provenance["questions"]
        record["discovery_eras"] = provenance["eras"]
    manifest_rows = [build_manifest_row(record) for record in canonical]
    manifest_rows.sort(key=lambda row: (-int(row["relevance_score"]), row["paper_id"]))
    write_tsv_rows(manifest_path, MANIFEST_HEADER, manifest_rows)

    request_rows = read_tsv_rows(request_path, REQUEST_LEDGER_HEADER)
    write_tsv_rows(query_path, QUERY_LEDGER_HEADER, update_query_execution_rows(query_rows, request_rows))
    return 0


def build_argument_parser() -> argparse.ArgumentParser:
    """Build the explicit G02 command-line boundary."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--allow-network", action="store_true")
    return parser


def main() -> int:
    """Run the G02 metadata campaign CLI."""

    arguments = build_argument_parser().parse_args()
    try:
        return execute_g02_discovery_campaign(arguments.root, arguments.allow_network)
    except (OSError, RuntimeError, ValueError) as error:
        print(f"G02 FAILED: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
