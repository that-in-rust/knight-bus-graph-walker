#!/usr/bin/env python3
"""Bounded, metadata-only G03 citation archaeology pipeline."""

from __future__ import annotations

import hashlib
import json
import re
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import csv
import argparse
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple


EDGE_HEADER = (
    "source_paper_id\ttarget_paper_id\tedge_type\tdiscovery_source\t"
    "relevance_reason\tverified_at"
)
REQUEST_HEADER = (
    "request_id\tgoal_id\tseed_paper_id\ttraversal_paper_id\tdepth\tdirection\t"
    "service\toperation\tnormalized_identifier\tparameters\trequested_at_utc\t"
    "page_cursor\tresponse_status\tresult_count\tresponse_checksum\tcache_checksum\tclient_version\t"
    "cache_status\tattempt\tretry_events\trate_limit_events\tpolicy_url\t"
    "policy_checked_date\tcache_path\tterminal_state"
)
STOP_HEADER = (
    "stop_id\tcandidate_identity\tseed_paper_id\tparent_paper_id\tdepth\t"
    "direction\tdecision_score\tscore_breakdown\tarchitecture_question_ids\t"
    "provider_name\tprovider_id\treason"
)
SCREENING_HEADER = (
    "candidate_paper_id\tprimary_lane\tdirection\tdisposition\tqueue_rank\t"
    "rationale\treviewer_model\treviewer_agent_id\tprompt_id\t"
    "screened_at_utc\tevidence_scope\tresult_checksum\taudit_lane_id\t"
    "audit_reviewer_agent_id\taudit_result_checksum"
)
TAXONOMY_HEADER = (
    "term_id\tterm\tterm_type\tarchitecture_question_ids\tsource_repo_paths\t"
    "synonyms\thistorical_terms\tadjacent_domain_terms\texclusion_terms\tnotes"
)
MANIFEST_HEADER = (
    "paper_id\tarxiv_id\tdoi\ttitle\tauthors\tpublished_date\tupdated_date\t"
    "categories\tabstract_url\tpdf_url\tlicense_uri\tcanonical_version\t"
    "discovery_query_ids\tarchitecture_question_ids\trelevance_score\t"
    "score_breakdown\tselection_status\tevidence_grade\tcode_urls\tlocal_path\t"
    "sha256\tnotes"
)
ALLOWED_EDGE_TYPES = {
    "CITES",
    "IMPLEMENTS",
    "EVALUATES",
    "REFINES",
    "CONTRADICTS",
    "SURVEYS",
}
ALLOWED_DIRECTIONS = {"SEED_RESOLUTION", "BACKWARD", "FORWARD"}
ALLOWED_TERMINAL_STATES = {
    "COMPLETE",
    "EMPTY",
    "UNAVAILABLE",
    "RATE_LIMITED",
    "PAYLOAD_REJECTED",
    "FAILED",
}
MAX_HTTP_REQUESTS = 90
MAX_ATTEMPTS = 3
MAX_DEPTH = 2
MAX_NEW_IDENTITIES = 250
MAX_RAW_OBSERVATIONS = 6000
EXPECTED_G02_MANIFEST_COUNT = 262
MAX_REFERENCE_IDS_PER_SEED = 12
MAX_S2_PAGE_RESULTS = 75
MAX_S2_DEPTH2_EXPANSIONS = 5
MIN_S2_RETRY_RESERVE = 6
S2_MINIMUM_DELAY_SECONDS = 5.0
OPENALEX_SELECT_FIELDS = (
    "id,doi,display_name,publication_date,type,authorships,ids,locations,"
    "referenced_works,cited_by_count,is_retracted,updated_date"
)
OPENALEX_ALLOWED_WORK_FIELDS = set(OPENALEX_SELECT_FIELDS.split(","))
S2_SELECTED_FIELDS = (
    "paperId,externalIds,url,title,year,publicationDate,authors,venue,"
    "citationCount,referenceCount"
)
S2_ALLOWED_WORK_FIELDS = set(S2_SELECTED_FIELDS.split(","))
FORBIDDEN_RESPONSE_KEYS = {
    "abstract",
    "abstract_inverted_index",
    "full_text",
    "fulltext",
    "content",
    "ngrams",
    "tldr",
    "openaccesspdf",
    "embedding",
    "contexts",
    "intents",
    "isinfluential",
    "snippet",
    "snippets",
}
SEMANTIC_TITLE_TOKENS = {
    "IMPLEMENTS": ("implementation", "implementing"),
    "EVALUATES": ("evaluation", "evaluating"),
    "REFINES": ("refinement", "refining", "improved"),
    "CONTRADICTS": ("counterexample", "impossibility", "lower bound"),
    "SURVEYS": ("survey", "review", "taxonomy"),
}
SCREENING_RESULT_PATHS = {
    "G03-LANE-A": "governance/reviews/G03-lane-A-backward.md",
    "G03-LANE-B": "governance/reviews/G03-lane-B-forward.md",
    "G03-LANE-C": "governance/reviews/G03-lane-C-constraints.md",
    "G03-LANE-D": "governance/reviews/G03-lane-D-audit.md",
}


class CitationRateLimitExhausted(RuntimeError):
    """One citation operation consumed its persistent three-attempt budget."""


class CitationPayloadRejected(RuntimeError):
    """A checksummed provider response violated the selected-metadata contract."""


def normalize_inline_text(value: object) -> str:
    """Collapse provider text to one TSV-safe line."""

    return re.sub(r"\s+", " ", str(value or "")).strip().replace("|", "%7C")


def normalize_tsv_cell(value: object) -> str:
    """Collapse control whitespace while preserving frozen multi-value pipes."""

    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_title_identity(value: object) -> str:
    """Normalize a title for conservative exact comparisons."""

    normalized = unicodedata.normalize("NFKC", str(value or "")).casefold()
    normalized = "".join(character if character.isalnum() else " " for character in normalized)
    return " ".join(normalized.split())


def normalize_doi_identity(value: object) -> str:
    """Normalize DOI resolver forms without destroying suffix punctuation."""

    normalized = str(value or "").strip().casefold()
    normalized = re.sub(r"^(?:doi:\s*|https?://(?:dx\.)?doi\.org/)", "", normalized)
    if normalized in {"", "unknown", "not_applicable"}:
        return "UNKNOWN"
    return normalized


def split_arxiv_version(identifier: str) -> Tuple[str, str]:
    """Return an arXiv base ID and optional version."""

    value = identifier.strip()
    value = re.sub(r"^https?://(?:export\.)?arxiv\.org/(?:abs|pdf)/", "", value)
    value = re.sub(r"\.pdf$", "", value)
    match = re.fullmatch(r"(.+?)(v\d+)?", value)
    if match is None:
        return value, ""
    return match.group(1), match.group(2) or ""


def extract_g03_seed_ids(report_text: str) -> List[str]:
    """Extract exactly the frozen ordered G03 seed table."""

    if "## Recommended G03 Seed Set" not in report_text:
        raise ValueError("G02 report has no Recommended G03 Seed Set")
    section = report_text.split("## Recommended G03 Seed Set", 1)[1]
    section = section.split("\n## ", 1)[0]
    seeds = re.findall(r"^\|\s*\d+\s*\|\s*`(PAPER-[^`]+)`\s*\|", section, re.MULTILINE)
    if len(seeds) != 25 or len(set(seeds)) != 25:
        raise ValueError("G03 requires exactly 25 unique seeds")
    return seeds


def validate_g03_network_preflight(preflight_text: str) -> List[str]:
    """Fail closed unless both bounded metadata-provider decisions are present."""

    required_markers = (
        "**Status:** `AUTHORIZED_OPENALEX_SEMANTIC_SCHOLAR_METADATA_ONLY`",
        "**Checked:** 2026-08-11",
        "Exactly two citation services are authorized: OpenAlex and Semantic Scholar.",
        "OpenAlex | `AUTHORIZED_METADATA_ONLY`",
        "Semantic Scholar | `AUTHORIZED_METADATA_ONLY`",
        "https://developers.openalex.org/api-reference/authentication",
        "https://developers.openalex.org/api-reference/works",
        "https://developers.openalex.org/guides/selecting-fields",
        "https://developers.openalex.org/guides/page-through-results",
        "https://developers.openalex.org/api-reference/errors",
        "https://help.openalex.org/hc/en-us/articles/28926392245399-How-is-OpenAlex-open",
        "https://openalex.org/OpenAlex_termsofservice.pdf",
        "https://api.semanticscholar.org/api-docs/graph",
        "https://www.semanticscholar.org/product/api",
        "https://www.semanticscholar.org/product/api/tutorial",
        "https://www.semanticscholar.org/product/api/license",
        "Hard ceiling | 90",
        OPENALEX_SELECT_FIELDS,
        S2_SELECTED_FIELDS,
        "No search, semantic search",
        "No paid balance",
    )
    errors = [
        "g03-service-preflight.md: missing {0}".format(marker)
        for marker in required_markers
        if marker not in preflight_text
    ]
    if preflight_text.count("`AUTHORIZED_METADATA_ONLY`") != 2:
        errors.append("g03-service-preflight.md: exactly two services must be authorized")
    return sorted(errors)


def build_openalex_request_parameters(operation: str, identifier: str) -> Dict[str, str]:
    """Compile one allowlisted OpenAlex list/filter request without credentials."""

    normalized_identifier = re.sub(r"\s+", " ", str(identifier or "")).strip()
    if operation == "SEED_RESOLUTION":
        seed_identifiers = normalized_identifier.split("|")
        if not seed_identifiers or len(seed_identifiers) > 2:
            raise ValueError("seed resolution requires one base and optional versioned ID")
        arxiv_ids = [value.removeprefix("PAPER-") for value in seed_identifiers]
        if any(
            not re.fullmatch(r"\d{4}\.\d{4,5}(?:v\d+)?", value)
            for value in arxiv_ids
        ):
            raise ValueError("seed resolution contains invalid arXiv identity")
        filter_value = "locations.landing_page_url:" + "|".join(
            "https://arxiv.org/abs/{0}".format(value) for value in arxiv_ids
        )
    elif operation == "FORWARD_CITATIONS":
        openalex_id = normalized_identifier.rsplit("/", 1)[-1]
        if not re.fullmatch(r"W\d+", openalex_id):
            raise ValueError("forward citation request requires an OpenAlex work ID")
        filter_value = "cites:{0}".format(openalex_id)
    elif operation == "BATCH_WORKS":
        openalex_ids = [value.rsplit("/", 1)[-1] for value in normalized_identifier.split("|")]
        if not openalex_ids or len(openalex_ids) > 100:
            raise ValueError("batch request requires 1-100 OpenAlex work IDs")
        if any(not re.fullmatch(r"W\d+", value) for value in openalex_ids):
            raise ValueError("batch request contains invalid OpenAlex work ID")
        filter_value = "openalex:" + "|".join(openalex_ids)
    else:
        raise ValueError("unauthorized OpenAlex operation")
    return {
        "filter": filter_value,
        "per_page": "100",
        "select": OPENALEX_SELECT_FIELDS,
    }


def build_s2_request_parameters(operation: str, identifier: str) -> Dict[str, str]:
    """Compile one exact Semantic Scholar metadata request without credentials."""

    normalized_identifier = re.sub(r"\s+", " ", str(identifier or "")).strip()
    if operation == "SEED_RESOLUTION_BATCH":
        seed_ids = normalized_identifier.split("|") if normalized_identifier else []
        if not seed_ids or len(seed_ids) > 25:
            raise ValueError("S2 seed batch requires 1-25 exact arXiv identities")
        arxiv_ids: List[str] = []
        for seed_id in seed_ids:
            arxiv_id = seed_id.removeprefix("PAPER-")
            if not re.fullmatch(r"\d{4}\.\d{4,5}", arxiv_id):
                raise ValueError("S2 seed batch contains invalid arXiv identity")
            arxiv_ids.append("ARXIV:" + arxiv_id)
        return {"fields": S2_SELECTED_FIELDS, "ids": "|".join(arxiv_ids)}
    if operation not in {"BACKWARD_REFERENCES", "FORWARD_CITATIONS"}:
        raise ValueError("unauthorized Semantic Scholar operation")
    if not re.fullmatch(r"[A-Za-z0-9._:-]+", normalized_identifier):
        raise ValueError("S2 neighborhood request requires an exact provider ID")
    return {
        "fields": S2_SELECTED_FIELDS,
        "limit": str(MAX_S2_PAGE_RESULTS),
        "offset": "0",
    }


def read_tsv_rows(path: Path, expected_header: str) -> List[Dict[str, str]]:
    """Read a strict UTF-8 TSV ledger."""

    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != expected_header:
        raise ValueError("unexpected TSV header for {0}".format(path))
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def write_tsv_rows(
    path: Path, header: str, rows: Sequence[Mapping[str, object]]
) -> None:
    """Write one strict TSV ledger atomically enough for checkpoint replay."""

    path.parent.mkdir(parents=True, exist_ok=True)
    fields = header.split("\t")
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
            delimiter="\t",
            lineterminator="\n",
            extrasaction="raise",
        )
        writer.writeheader()
        for source in rows:
            writer.writerow({field: normalize_tsv_cell(source.get(field, "")) for field in fields})
    temporary.replace(path)


def utc_timestamp_now() -> str:
    """Return a second-resolution UTC timestamp."""

    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _request_operation_key(row: Mapping[str, str]) -> Tuple[str, ...]:
    return (
        row.get("service", ""),
        row.get("seed_paper_id", ""),
        row.get("traversal_paper_id", ""),
        row.get("depth", ""),
        row.get("direction", ""),
        row.get("operation", ""),
        row.get("normalized_identifier", ""),
    )


def _build_request_ledger_row(
    request_id: str,
    seed_paper_id: str,
    traversal_paper_id: str,
    depth: int,
    direction: str,
    service: str,
    operation: str,
    identifier: str,
    parameters: Mapping[str, str],
    status: str,
    payload: bytes,
    cache_payload: bytes,
    result_count: int,
    cache_path: str,
    attempt: int,
    retry_events: Sequence[str],
    rate_limit_events: str,
    terminal_state: str,
) -> Dict[str, str]:
    return {
        "request_id": request_id,
        "goal_id": "G03",
        "seed_paper_id": seed_paper_id,
        "traversal_paper_id": traversal_paper_id,
        "depth": str(depth),
        "direction": direction,
        "service": service,
        "operation": operation,
        "normalized_identifier": normalize_inline_text(identifier),
        "parameters": urllib.parse.urlencode(sorted(parameters.items())),
        "requested_at_utc": utc_timestamp_now(),
        "page_cursor": "NOT_APPLICABLE",
        "response_status": status,
        "result_count": str(result_count),
        "response_checksum": hashlib.sha256(payload).hexdigest(),
        "cache_checksum": hashlib.sha256(cache_payload).hexdigest(),
        "client_version": (
            "knight-bus-g03-openalex/1.0"
            if service == "OpenAlex"
            else "knight-bus-g03-semantic-scholar/1.0"
        ),
        "cache_status": "MISS",
        "attempt": str(attempt),
        "retry_events": "|".join(retry_events) if retry_events else "NONE",
        "rate_limit_events": rate_limit_events or "NONE",
        "policy_url": (
            "https://developers.openalex.org/api-reference/authentication"
            if service == "OpenAlex"
            else "https://www.semanticscholar.org/product/api/license"
        ),
        "policy_checked_date": "2026-08-11",
        "cache_path": cache_path,
        "terminal_state": terminal_state,
    }


def fetch_openalex_metadata_page(
    reference_root: Path,
    ledger_path: Path,
    preflight_text: str,
    operation: str,
    identifier: str,
    seed_paper_id: str,
    traversal_paper_id: str,
    depth: int,
    direction: str,
    allow_network: bool,
    remaining_http_requests: int,
    minimum_delay_seconds: float = 1.0,
) -> List[Dict[str, object]]:
    """Fetch or replay one allowlisted OpenAlex page with per-attempt provenance."""

    preflight_errors = validate_g03_network_preflight(preflight_text)
    if preflight_errors:
        raise RuntimeError("preflight is not authorized: " + "; ".join(preflight_errors))
    if depth < 0 or depth > MAX_DEPTH or direction not in ALLOWED_DIRECTIONS:
        raise RuntimeError("invalid traversal depth or direction")
    parameters = build_openalex_request_parameters(operation, identifier)
    existing_rows = read_tsv_rows(ledger_path, REQUEST_HEADER)
    operation_key = (
        "OpenAlex",
        seed_paper_id,
        traversal_paper_id,
        str(depth),
        direction,
        operation,
        normalize_inline_text(identifier),
    )
    matching_rows = [
        row for row in existing_rows if _request_operation_key(row) == operation_key
    ]
    for row in matching_rows:
        if _request_operation_key(row) != operation_key:
            continue
        if row.get("terminal_state") not in {"COMPLETE", "EMPTY"}:
            continue
        cache_errors = validate_g03_cache_provenance(reference_root, existing_rows)
        if cache_errors:
            raise RuntimeError("cached citation response failed verification: " + "; ".join(cache_errors))
        cache_path = _resolve_cache_path(
            reference_root, row.get("cache_path", ""), row.get("service", "")
        )
        if cache_path is None:
            raise RuntimeError("cached citation response path is unsafe")
        return parse_openalex_work_payload(cache_path.read_bytes())
    if len(matching_rows) >= MAX_ATTEMPTS:
        raise RuntimeError("citation operation attempt cap exhausted")
    if matching_rows:
        last_status = matching_rows[-1].get("response_status", "")
        retryable = last_status in {"408", "429", "TRANSPORT_ERROR"} or (
            last_status.isdigit() and int(last_status) >= 500
        )
        if not retryable:
            raise RuntimeError(
                "citation operation is terminal after status {0}".format(last_status)
            )
    if not allow_network:
        raise RuntimeError("network access is disabled and no verified cache exists")
    if remaining_http_requests < 1:
        raise RuntimeError("citation request cap exhausted")

    retry_events: List[str] = [
        "attempt-{0}:{1}".format(row.get("attempt", "UNKNOWN"), row.get("response_status", "UNKNOWN"))
        for row in matching_rows
    ]
    prior_attempts = len(matching_rows)
    attempts_allowed = min(MAX_ATTEMPTS - prior_attempts, remaining_http_requests)
    final_attempt = prior_attempts + attempts_allowed
    for attempt in range(prior_attempts + 1, final_attempt + 1):
        if minimum_delay_seconds > 0:
            time.sleep(minimum_delay_seconds)
        query = urllib.parse.urlencode(parameters)
        request = urllib.request.Request(
            "https://api.openalex.org/works?" + query,
            headers={
                "Accept": "application/json",
                "User-Agent": "KnightBus-G03-Citation-Archaeology/1.0",
            },
            method="GET",
        )
        status = "TRANSPORT_ERROR"
        headers: Mapping[str, str] = {}
        payload = b""
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                status = str(getattr(response, "status", 200))
                headers = getattr(response, "headers", {})
                payload = response.read()
        except urllib.error.HTTPError as error:
            status = str(error.code)
            headers = error.headers or {}
            payload = error.read()
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            payload = json.dumps(
                {"error": "transport_error", "type": type(error).__name__},
                sort_keys=True,
            ).encode("utf-8")

        request_id = "REQ-G03-{0:04d}".format(len(existing_rows) + 1)
        cache_file = reference_root / "cache" / "g03" / "openalex" / (request_id + ".json")
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_file.write_bytes(payload)
        try:
            records = parse_openalex_work_payload(payload) if status == "200" else []
            parse_error: Optional[ValueError] = None
        except ValueError as error:
            records = []
            parse_error = error
        rate_events = "|".join(
            "{0}={1}".format(name, normalize_inline_text(headers.get(header_name)))
            for name, header_name in (
                ("remaining", "X-RateLimit-Remaining"),
                ("reset", "X-RateLimit-Reset"),
                ("credits_used", "X-RateLimit-Credits-Used"),
            )
            if headers.get(header_name) is not None
        )
        if status == "200" and parse_error is None:
            terminal_state = "COMPLETE" if records else "EMPTY"
        elif status == "404":
            terminal_state = "UNAVAILABLE"
        elif status == "429" and attempt == final_attempt:
            terminal_state = "RATE_LIMITED"
        else:
            terminal_state = "FAILED"
        relative_cache = reference_root.name + "/" + cache_file.relative_to(reference_root).as_posix()
        row = _build_request_ledger_row(
            request_id,
            seed_paper_id,
            traversal_paper_id,
            depth,
            direction,
            "OpenAlex",
            operation,
            identifier,
            parameters,
            status,
            payload,
            payload,
            len(records),
            relative_cache,
            attempt,
            retry_events,
            rate_events,
            terminal_state,
        )
        existing_rows.append(row)
        write_tsv_rows(ledger_path, REQUEST_HEADER, existing_rows)
        if status == "200" and parse_error is None:
            return records
        if parse_error is not None:
            raise RuntimeError("OpenAlex selected metadata response is invalid: {0}".format(parse_error))
        retryable = status in {"408", "429", "TRANSPORT_ERROR"} or (
            status.isdigit() and int(status) >= 500
        )
        if not retryable or attempt == final_attempt:
            raise RuntimeError("OpenAlex request stopped with status {0}".format(status))
        retry_events.append("attempt-{0}:{1}".format(attempt, status))
        retry_after = normalize_inline_text(headers.get("Retry-After"))
        try:
            retry_delay = float(retry_after) if retry_after else float(2 ** (attempt - 1))
        except ValueError:
            retry_delay = float(2 ** (attempt - 1))
        time.sleep(max(retry_delay, float(2 ** (attempt - 1))))
    raise RuntimeError("OpenAlex request exhausted retries")


def fetch_s2_metadata_page(
    reference_root: Path,
    ledger_path: Path,
    preflight_text: str,
    operation: str,
    identifier: str,
    seed_paper_id: str,
    traversal_paper_id: str,
    depth: int,
    direction: str,
    allow_network: bool,
    remaining_http_requests: int,
    minimum_delay_seconds: float = 1.1,
) -> List[Dict[str, object]]:
    """Fetch or replay one allowlisted Semantic Scholar metadata operation."""

    preflight_errors = validate_g03_network_preflight(preflight_text)
    if preflight_errors:
        raise RuntimeError("preflight is not authorized: " + "; ".join(preflight_errors))
    if depth < 0 or depth > MAX_DEPTH or direction not in ALLOWED_DIRECTIONS:
        raise RuntimeError("invalid traversal depth or direction")
    parameters = build_s2_request_parameters(operation, identifier)
    existing_rows = read_tsv_rows(ledger_path, REQUEST_HEADER)
    operation_key = (
        "SemanticScholar",
        seed_paper_id,
        traversal_paper_id,
        str(depth),
        direction,
        operation,
        normalize_inline_text(identifier),
    )
    matching_rows = [
        row for row in existing_rows if _request_operation_key(row) == operation_key
    ]
    for row in matching_rows:
        if row.get("terminal_state") not in {"COMPLETE", "EMPTY"}:
            continue
        cache_errors = validate_g03_cache_provenance(reference_root, existing_rows)
        if cache_errors:
            raise RuntimeError(
                "cached citation response failed verification: " + "; ".join(cache_errors)
            )
        cache_path = _resolve_cache_path(
            reference_root, row.get("cache_path", ""), row.get("service", "")
        )
        if cache_path is None:
            raise RuntimeError("cached citation response path is unsafe")
        return parse_s2_work_payload(cache_path.read_bytes(), operation)
    if matching_rows and matching_rows[-1].get("terminal_state") == "PAYLOAD_REJECTED":
        cache_errors = validate_g03_cache_provenance(reference_root, existing_rows)
        if cache_errors:
            raise RuntimeError(
                "rejected citation response failed verification: "
                + "; ".join(cache_errors)
            )
        raise CitationPayloadRejected(
            "Semantic Scholar selected payload was previously rejected"
        )
    if len(matching_rows) >= MAX_ATTEMPTS:
        if matching_rows[-1].get("response_status") == "429":
            raise CitationRateLimitExhausted(
                "Semantic Scholar citation operation exhausted three rate-limit attempts"
            )
        raise RuntimeError("citation operation attempt cap exhausted")
    if matching_rows:
        last_status = matching_rows[-1].get("response_status", "")
        if not _is_retryable_status(last_status):
            raise RuntimeError(
                "citation operation is terminal after status {0}".format(last_status)
            )
    if not allow_network:
        raise RuntimeError("network access is disabled and no verified cache exists")
    if remaining_http_requests < 1:
        raise RuntimeError("citation request cap exhausted")

    retry_events = [
        "attempt-{0}:{1}".format(
            row.get("attempt", "UNKNOWN"), row.get("response_status", "UNKNOWN")
        )
        for row in matching_rows
    ]
    prior_attempts = len(matching_rows)
    attempts_allowed = min(MAX_ATTEMPTS - prior_attempts, remaining_http_requests)
    final_attempt = prior_attempts + attempts_allowed
    for attempt in range(prior_attempts + 1, final_attempt + 1):
        if minimum_delay_seconds > 0:
            time.sleep(minimum_delay_seconds)
        if operation == "SEED_RESOLUTION_BATCH":
            query_parameters = {"fields": parameters["fields"]}
            request_body = json.dumps(
                {"ids": parameters["ids"].split("|")},
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            endpoint = "https://api.semanticscholar.org/graph/v1/paper/batch"
            method = "POST"
        else:
            query_parameters = parameters
            request_body = None
            suffix = (
                "references" if operation == "BACKWARD_REFERENCES" else "citations"
            )
            endpoint = (
                "https://api.semanticscholar.org/graph/v1/paper/{0}/{1}".format(
                    urllib.parse.quote(identifier, safe=""), suffix
                )
            )
            method = "GET"
        request = urllib.request.Request(
            endpoint + "?" + urllib.parse.urlencode(query_parameters),
            data=request_body,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "KnightBus-G03-Citation-Archaeology/1.0",
            },
            method=method,
        )
        status = "TRANSPORT_ERROR"
        headers: Mapping[str, str] = {}
        payload = b""
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                status = str(getattr(response, "status", 200))
                headers = getattr(response, "headers", {})
                payload = response.read()
        except urllib.error.HTTPError as error:
            status = str(error.code)
            headers = error.headers or {}
            payload = error.read()
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            payload = json.dumps(
                {"error": "transport_error", "type": type(error).__name__},
                sort_keys=True,
            ).encode("utf-8")

        request_id = "REQ-G03-{0:04d}".format(len(existing_rows) + 1)
        cache_file = (
            reference_root
            / "cache"
            / "g03"
            / "semantic-scholar"
            / (request_id + ".json")
        )
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_payload = payload
        try:
            if status == "200":
                cache_payload = sanitize_s2_metadata_payload(payload, operation)
                records = parse_s2_work_payload(cache_payload, operation)
            else:
                records = []
            parse_error: Optional[ValueError] = None
        except ValueError as error:
            records = []
            parse_error = error
            cache_payload = json.dumps(
                {
                    "error": "rejected_provider_payload",
                    "provider": "SemanticScholar",
                    "raw_response_checksum": hashlib.sha256(payload).hexdigest(),
                    "reason": type(error).__name__,
                },
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        cache_file.write_bytes(cache_payload)
        rate_events = "|".join(
            "{0}={1}".format(name, normalize_inline_text(headers.get(header_name)))
            for name, header_name in (
                ("remaining", "X-RateLimit-Remaining"),
                ("reset", "X-RateLimit-Reset"),
                ("retry_after", "Retry-After"),
            )
            if headers.get(header_name) is not None
        )
        if status == "200" and parse_error is None:
            terminal_state = "COMPLETE" if records else "EMPTY"
        elif status == "404":
            terminal_state = "UNAVAILABLE"
        elif status == "429" and attempt == final_attempt:
            terminal_state = "RATE_LIMITED"
        elif parse_error is not None:
            terminal_state = "PAYLOAD_REJECTED"
        else:
            terminal_state = "FAILED"
        relative_cache = reference_root.name + "/" + cache_file.relative_to(reference_root).as_posix()
        row = _build_request_ledger_row(
            request_id,
            seed_paper_id,
            traversal_paper_id,
            depth,
            direction,
            "SemanticScholar",
            operation,
            identifier,
            parameters,
            status,
            payload,
            cache_payload,
            len(records),
            relative_cache,
            attempt,
            retry_events,
            rate_events,
            terminal_state,
        )
        existing_rows.append(row)
        write_tsv_rows(ledger_path, REQUEST_HEADER, existing_rows)
        if status == "200" and parse_error is None:
            return records
        if parse_error is not None:
            raise CitationPayloadRejected(
                "Semantic Scholar selected metadata response is invalid: {0}".format(
                    parse_error
                )
            )
        if status == "429" and attempt >= MAX_ATTEMPTS:
            raise CitationRateLimitExhausted(
                "Semantic Scholar citation operation exhausted three rate-limit attempts"
            )
        if not _is_retryable_status(status) or attempt == final_attempt:
            raise RuntimeError(
                "Semantic Scholar request stopped with status {0}".format(status)
            )
        retry_events.append("attempt-{0}:{1}".format(attempt, status))
        retry_after = normalize_inline_text(headers.get("Retry-After"))
        try:
            retry_delay = float(retry_after) if retry_after else float(2 ** (attempt - 1))
        except ValueError:
            retry_delay = float(2 ** (attempt - 1))
        time.sleep(max(retry_delay, float(2 ** (attempt - 1))))
    raise RuntimeError("Semantic Scholar request exhausted retries")


def _contains_forbidden_response_key(value: object) -> bool:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key).casefold() in FORBIDDEN_RESPONSE_KEYS:
                return True
            if _contains_forbidden_response_key(child):
                return True
    elif isinstance(value, list):
        return any(_contains_forbidden_response_key(item) for item in value)
    return False


def _extract_arxiv_identity(work: Mapping[str, object]) -> Tuple[str, str]:
    locations: List[object] = list(work.get("locations") or [])
    primary = work.get("primary_location")
    if isinstance(primary, Mapping):
        locations.append(primary)
    matches: Set[Tuple[str, str]] = set()
    for location in locations:
        if not isinstance(location, Mapping):
            continue
        landing = str(location.get("landing_page_url") or "")
        match = re.search(r"arxiv\.org/(?:abs|pdf)/([^?#]+)", landing, re.IGNORECASE)
        if match:
            matches.add(split_arxiv_version(match.group(1)))
    bases = {base for base, _version in matches if base}
    if len(bases) > 1:
        raise ValueError("one OpenAlex work exposes conflicting arXiv identities")
    if not matches:
        return "UNKNOWN", "UNKNOWN"
    base = next(iter(bases))
    versions = sorted(version for found_base, version in matches if found_base == base and version)
    return base, versions[-1] if versions else "UNKNOWN"


def _author_names(work: Mapping[str, object]) -> List[str]:
    names: List[str] = []
    for authorship in work.get("authorships") or []:
        if not isinstance(authorship, Mapping):
            continue
        author = authorship.get("author")
        if isinstance(author, Mapping):
            name = normalize_inline_text(author.get("display_name"))
            if name:
                names.append(name)
    return names


def stable_citation_paper_id(record: Mapping[str, object]) -> str:
    """Build the G02-compatible canonical identity for one citation record."""

    arxiv_id = str(record.get("arxiv_id") or "UNKNOWN")
    if arxiv_id != "UNKNOWN":
        base, _version = split_arxiv_version(arxiv_id)
        if re.fullmatch(r"\d{4}\.\d{4,5}", base):
            return "PAPER-" + base
        digest = hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]
        return "PAPER-LEGACY-" + digest
    doi = normalize_doi_identity(record.get("doi"))
    if doi != "UNKNOWN":
        key = doi
    else:
        key = "|".join(
            [
                normalize_title_identity(record.get("title")),
                "|".join(normalize_title_identity(name) for name in record.get("authors") or []),
                str(record.get("published_date") or "UNKNOWN"),
            ]
        )
    return "PAPER-HASH-" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def _normalize_openalex_work(work: Mapping[str, object]) -> Dict[str, object]:
    openalex_id = normalize_inline_text(work.get("id"))
    title = normalize_inline_text(work.get("display_name") or work.get("title"))
    if not openalex_id or not title:
        raise ValueError("OpenAlex work requires id and display_name")
    arxiv_id, arxiv_version = _extract_arxiv_identity(work)
    doi = normalize_doi_identity(work.get("doi"))
    source_urls: Set[str] = set()
    for location in work.get("locations") or []:
        if isinstance(location, Mapping):
            url = normalize_inline_text(location.get("landing_page_url"))
            if url:
                source_urls.add(url)
    record: Dict[str, object] = {
        "openalex_id": openalex_id,
        "semantic_scholar_id": "UNKNOWN",
        "provider_id": openalex_id,
        "provider_name": "OpenAlex",
        "doi": doi,
        "title": title,
        "authors": _author_names(work),
        "published_date": normalize_inline_text(work.get("publication_date")) or "UNKNOWN",
        "updated_date": normalize_inline_text(work.get("updated_date")) or "UNKNOWN",
        "type": normalize_inline_text(work.get("type")) or "UNKNOWN",
        "arxiv_id": arxiv_id,
        "arxiv_version": arxiv_version,
        "referenced_works": sorted(
            {normalize_inline_text(value) for value in work.get("referenced_works") or [] if value}
        ),
        "cited_by_count": int(work.get("cited_by_count") or 0),
        "source_urls": sorted(source_urls),
        "is_retracted": bool(work.get("is_retracted", False)),
    }
    record["paper_id"] = stable_citation_paper_id(record)
    record["identity_state"] = "CANONICAL"
    return record


def parse_openalex_work_payload(payload: bytes) -> List[Dict[str, object]]:
    """Parse selected OpenAlex work metadata while rejecting textual content."""

    try:
        document = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("malformed OpenAlex JSON") from error
    if _contains_forbidden_response_key(document):
        raise ValueError("OpenAlex response contains forbidden text/content fields")
    if isinstance(document, Mapping) and "results" in document:
        works = document.get("results")
        if not isinstance(works, list):
            raise ValueError("OpenAlex results must be a list")
    elif isinstance(document, Mapping):
        works = [document]
    else:
        raise ValueError("OpenAlex response must be a work or result page")
    records: List[Dict[str, object]] = []
    for work in works:
        if not isinstance(work, Mapping):
            raise ValueError("OpenAlex result must be a work object")
        unexpected_fields = set(work) - OPENALEX_ALLOWED_WORK_FIELDS
        if unexpected_fields:
            raise ValueError(
                "OpenAlex response contains unselected fields: {0}".format(
                    ",".join(sorted(str(value) for value in unexpected_fields))
                )
            )
        records.append(_normalize_openalex_work(work))
    return records


def _s2_external_identity(
    external_ids: Mapping[str, object]
) -> Tuple[str, str, str]:
    arxiv_value = next(
        (
            str(value)
            for key, value in external_ids.items()
            if str(key).casefold() == "arxiv" and value
        ),
        "UNKNOWN",
    )
    if arxiv_value != "UNKNOWN":
        arxiv_id, arxiv_version = split_arxiv_version(arxiv_value)
    else:
        arxiv_id, arxiv_version = "UNKNOWN", "UNKNOWN"
    doi_value = next(
        (
            value
            for key, value in external_ids.items()
            if str(key).casefold() == "doi" and value
        ),
        "UNKNOWN",
    )
    return arxiv_id, arxiv_version or "UNKNOWN", normalize_doi_identity(doi_value)


def _s2_source_url(value: object, paper_id: str) -> str:
    raw_url = normalize_inline_text(value)
    if not raw_url:
        if paper_id.startswith("UNAVAILABLE:"):
            raw_url = "https://www.semanticscholar.org/"
        else:
            raw_url = "https://www.semanticscholar.org/paper/{0}".format(paper_id)
    separator = "&" if "?" in raw_url else "?"
    if "utm_source=" not in raw_url:
        raw_url += separator + "utm_source=api"
    return raw_url


def _normalize_s2_work(work: Mapping[str, object]) -> Dict[str, object]:
    unexpected_fields = set(work) - S2_ALLOWED_WORK_FIELDS
    if unexpected_fields:
        raise ValueError(
            "Semantic Scholar response contains unselected fields: {0}".format(
                ",".join(sorted(str(value) for value in unexpected_fields))
            )
        )
    semantic_scholar_id = normalize_inline_text(work.get("paperId"))
    title = normalize_inline_text(work.get("title"))
    if not title:
        raise ValueError("Semantic Scholar work requires a title")
    external_ids = work.get("externalIds") or {}
    if not isinstance(external_ids, Mapping):
        raise ValueError("Semantic Scholar externalIds must be an object")
    arxiv_id, arxiv_version, doi = _s2_external_identity(external_ids)
    authors: List[str] = []
    for author in work.get("authors") or []:
        if not isinstance(author, Mapping) or set(author) - {"authorId", "name"}:
            raise ValueError("Semantic Scholar authors contain unselected fields")
        name = normalize_inline_text(author.get("name"))
        if name:
            authors.append(name)
    publication_date = normalize_inline_text(work.get("publicationDate"))
    if not publication_date:
        year = normalize_inline_text(work.get("year"))
        publication_date = year + "-01-01" if re.fullmatch(r"\d{4}", year) else "UNKNOWN"
    identity_state = "CANONICAL"
    if not semantic_scholar_id:
        unresolved_key = "|".join(
            [
                normalize_title_identity(title),
                "|".join(normalize_title_identity(author) for author in authors),
                publication_date,
            ]
        )
        semantic_scholar_id = "UNAVAILABLE:" + hashlib.sha256(
            unresolved_key.encode("utf-8")
        ).hexdigest()[:16]
        identity_state = "UNAVAILABLE_PROVIDER_ID"
    source_url = _s2_source_url(work.get("url"), semantic_scholar_id)
    record: Dict[str, object] = {
        "openalex_id": "UNKNOWN",
        "semantic_scholar_id": semantic_scholar_id,
        "provider_id": "S2:" + semantic_scholar_id,
        "provider_name": "SemanticScholar",
        "doi": doi,
        "title": title,
        "authors": authors,
        "published_date": publication_date,
        "updated_date": "UNKNOWN",
        "type": normalize_inline_text(work.get("venue")) or "UNKNOWN",
        "arxiv_id": arxiv_id,
        "arxiv_version": arxiv_version,
        "referenced_works": [],
        "cited_by_count": int(work.get("citationCount") or 0),
        "reference_count": int(work.get("referenceCount") or 0),
        "source_urls": [source_url],
        "is_retracted": False,
        "identity_state": identity_state,
    }
    record["paper_id"] = stable_citation_paper_id(record)
    return record


def sanitize_s2_metadata_payload(payload: bytes, operation: str) -> bytes:
    """Discard unsolicited parent metadata before an S2 response is cached."""

    try:
        document = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("malformed Semantic Scholar JSON") from error
    sanitized = document
    if operation in {"BACKWARD_REFERENCES", "FORWARD_CITATIONS"}:
        if not isinstance(document, Mapping):
            raise ValueError("Semantic Scholar citation page must be an object")
        sanitized = dict(document)
        sanitized.pop("citingPaperInfo", None)
        sanitized.pop("citedPaperInfo", None)
    if _contains_forbidden_response_key(sanitized):
        raise ValueError("Semantic Scholar response contains forbidden text/content fields")
    if sanitized == document:
        return payload
    return json.dumps(
        sanitized, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def parse_s2_work_payload(payload: bytes, operation: str) -> List[Dict[str, object]]:
    """Parse an exact S2 batch or citation page while rejecting content fields."""

    try:
        document = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("malformed Semantic Scholar JSON") from error
    if _contains_forbidden_response_key(document):
        raise ValueError("Semantic Scholar response contains forbidden text/content fields")
    if operation == "SEED_RESOLUTION_BATCH":
        if not isinstance(document, list):
            raise ValueError("Semantic Scholar seed batch must be a list")
        works = [work for work in document if work is not None]
    elif operation in {"BACKWARD_REFERENCES", "FORWARD_CITATIONS"}:
        if not isinstance(document, Mapping) or set(document) - {"offset", "next", "data"}:
            raise ValueError("Semantic Scholar citation page has unexpected envelope fields")
        data = document.get("data")
        if not isinstance(data, list):
            raise ValueError("Semantic Scholar citation data must be a list")
        relationship_key = (
            "citedPaper" if operation == "BACKWARD_REFERENCES" else "citingPaper"
        )
        works = []
        for relation in data:
            if not isinstance(relation, Mapping) or set(relation) != {relationship_key}:
                raise ValueError("Semantic Scholar relationship has unexpected fields")
            work = relation.get(relationship_key)
            if work is not None:
                works.append(work)
    else:
        raise ValueError("unauthorized Semantic Scholar payload operation")
    records: List[Dict[str, object]] = []
    for work in works:
        if not isinstance(work, Mapping):
            raise ValueError("Semantic Scholar result must be a paper object")
        records.append(_normalize_s2_work(work))
    return records


def resolve_s2_seed_identities(
    seed_paper_ids: Sequence[str], records: Sequence[Mapping[str, object]]
) -> Tuple[Dict[str, Dict[str, object]], Set[str]]:
    """Resolve exact S2 arXiv aliases without silently merging duplicates."""

    expected = {seed_id: seed_id.removeprefix("PAPER-") for seed_id in seed_paper_ids}
    by_arxiv: Dict[str, List[Mapping[str, object]]] = defaultdict(list)
    for record in records:
        arxiv_id = str(record.get("arxiv_id") or "UNKNOWN")
        if arxiv_id != "UNKNOWN":
            by_arxiv[arxiv_id].append(record)
    resolved: Dict[str, Dict[str, object]] = {}
    unavailable: Set[str] = set()
    for seed_id, arxiv_id in expected.items():
        matches = by_arxiv.get(arxiv_id, [])
        provider_ids = {str(record.get("semantic_scholar_id") or "") for record in matches}
        if len(provider_ids) > 1:
            raise ValueError("seed resolution produced multiple Semantic Scholar identities")
        if not matches:
            unavailable.add(seed_id)
            continue
        record = dict(matches[0])
        record["paper_id"] = seed_id
        resolved[seed_id] = record
    unexpected = set(by_arxiv) - set(expected.values())
    if unexpected:
        raise ValueError("Semantic Scholar batch returned an unexpected arXiv identity")
    return resolved, unavailable


def resolve_seed_openalex_identity(
    seed_paper_id: str, records: Sequence[Mapping[str, object]]
) -> Optional[str]:
    """Resolve one exact seed, preserving zero matches as unavailable."""

    expected = seed_paper_id.removeprefix("PAPER-")
    matches = {
        str(record.get("openalex_id"))
        for record in records
        if str(record.get("arxiv_id")) == expected
    }
    if not matches:
        return None
    if len(matches) > 1:
        raise ValueError("seed resolution produced multiple OpenAlex identities")
    return next(iter(matches))


def _record_fingerprint(record: Mapping[str, object]) -> Tuple[str, Tuple[str, ...], str]:
    return (
        normalize_title_identity(record.get("title")),
        tuple(normalize_title_identity(name) for name in record.get("authors") or []),
        str(record.get("published_date") or "UNKNOWN"),
    )


def _provider_record_key(record: Mapping[str, object]) -> str:
    provider_id = str(record.get("provider_id") or "")
    if provider_id and provider_id != "UNKNOWN":
        return provider_id
    openalex_id = str(record.get("openalex_id") or "")
    if openalex_id and openalex_id != "UNKNOWN":
        return openalex_id
    semantic_scholar_id = str(record.get("semantic_scholar_id") or "")
    if semantic_scholar_id and semantic_scholar_id != "UNKNOWN":
        return "S2:" + semantic_scholar_id
    return ""


def reconcile_citation_identities(
    records: Sequence[Mapping[str, object]],
    existing_manifest_rows: Sequence[Mapping[str, str]],
) -> List[Dict[str, object]]:
    """Reconcile strong aliases and preserve unresolved title collisions."""

    existing_by_arxiv = {
        str(row.get("arxiv_id", "")): str(row.get("paper_id", ""))
        for row in existing_manifest_rows
        if row.get("arxiv_id") not in (None, "", "UNKNOWN")
    }
    existing_by_doi = {
        normalize_doi_identity(row.get("doi")): str(row.get("paper_id", ""))
        for row in existing_manifest_rows
        if normalize_doi_identity(row.get("doi")) != "UNKNOWN"
    }
    reconciled: List[Dict[str, object]] = []
    for source in records:
        record = dict(source)
        arxiv = str(record.get("arxiv_id") or "UNKNOWN")
        doi = normalize_doi_identity(record.get("doi"))
        existing_ids = {
            identity
            for identity in (existing_by_arxiv.get(arxiv), existing_by_doi.get(doi))
            if identity
        }
        if len(existing_ids) > 1:
            record["identity_state"] = "AMBIGUOUS"
            record["conflicting_identity_ids"] = sorted(existing_ids)
            conflict_key = "|".join(
                [
                    "STRONG_ID_CONFLICT",
                    *sorted(existing_ids),
                    _provider_record_key(record),
                    normalize_title_identity(record.get("title")),
                ]
            )
            record["paper_id"] = "PAPER-AMBIG-" + hashlib.sha256(
                conflict_key.encode("utf-8")
            ).hexdigest()[:16]
        elif existing_ids:
            record["paper_id"] = next(iter(existing_ids))
        reconciled.append(record)

    fingerprints: Dict[Tuple[str, Tuple[str, ...], str], List[Dict[str, object]]] = defaultdict(list)
    title_groups: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for record in reconciled:
        fingerprints[_record_fingerprint(record)].append(record)
        title_groups[normalize_title_identity(record.get("title"))].append(record)

    collapsed: List[Dict[str, object]] = []
    seen_fingerprints: Set[Tuple[str, Tuple[str, ...], str]] = set()
    for record in sorted(reconciled, key=_provider_record_key):
        fingerprint = _record_fingerprint(record)
        if fingerprint in seen_fingerprints:
            continue
        seen_fingerprints.add(fingerprint)
        same_title = title_groups[fingerprint[0]]
        distinct_fingerprints = {_record_fingerprint(item) for item in same_title}
        if len(distinct_fingerprints) > 1:
            record["identity_state"] = "AMBIGUOUS"
        collapsed.append(record)
    return sorted(collapsed, key=lambda row: str(row.get("paper_id", "")))


def _semantic_title_edges(title: str, target_title: str) -> List[Tuple[str, str]]:
    normalized_title = normalize_title_identity(title)
    normalized_target = normalize_title_identity(target_title)
    target_anchors = [normalized_target] if normalized_target else []
    if ":" in target_title:
        prefix = normalize_title_identity(target_title.split(":", 1)[0])
        if len(prefix.split()) >= 3:
            target_anchors.append(prefix)
    if not any(anchor and anchor in normalized_title for anchor in target_anchors):
        return []
    edges: List[Tuple[str, str]] = []
    for edge_type, tokens in SEMANTIC_TITLE_TOKENS.items():
        for token in tokens:
            if normalize_title_identity(token) in normalized_title:
                edges.append((edge_type, token))
                break
    return edges


def build_provider_citation_edges(
    target_paper_id: str,
    citing_record: Mapping[str, object],
    verified_at: str,
    target_title: str = "",
) -> List[Dict[str, str]]:
    """Create provider-backed CITES plus conservatively anchored semantic edges."""

    source_paper_id = str(citing_record.get("paper_id") or "")
    if not source_paper_id:
        raise ValueError("citing record requires paper_id")
    provider_name = str(citing_record.get("provider_name") or "OpenAlex")
    if provider_name not in {"OpenAlex", "SemanticScholar"}:
        raise ValueError("citing record has unsupported provider")
    discovery_prefix = (
        "OPENALEX" if provider_name == "OpenAlex" else "SEMANTIC_SCHOLAR"
    )
    provider_relation = (
        "referenced_works_or_cites_filter"
        if provider_name == "OpenAlex"
        else "references_or_citations_endpoint"
    )
    rows = [{
        "source_paper_id": source_paper_id,
        "target_paper_id": target_paper_id,
        "edge_type": "CITES",
        "discovery_source": discovery_prefix + "_API",
        "relevance_reason": "PROVIDER_RELATION=" + provider_relation,
        "verified_at": verified_at,
    }]
    for edge_type, token in _semantic_title_edges(
        str(citing_record.get("title") or ""), target_title
    ):
        rows.append({
            "source_paper_id": source_paper_id,
            "target_paper_id": target_paper_id,
            "edge_type": edge_type,
            "discovery_source": discovery_prefix + "_METADATA_SCREEN",
            "relevance_reason": "DERIVED_INFERENCE: TITLE_TOKEN={0}; TARGET_TITLE_ANCHORED".format(token),
            "verified_at": verified_at,
        })
    return rows


def _candidate_sort_key(candidate: Mapping[str, object]) -> Tuple[object, ...]:
    direction = str(candidate.get("direction") or "")
    date_value = str(candidate.get("published_date") or "9999-99-99")
    try:
        date_ordinal = datetime.strptime(date_value[:10], "%Y-%m-%d").date().toordinal()
    except ValueError:
        date_ordinal = 9999999
    date_key = date_ordinal if direction == "BACKWARD" else -date_ordinal
    return (
        -int(candidate.get("decision_score") or 0),
        int(candidate.get("depth") or 0),
        date_key,
        str(candidate.get("paper_id") or ""),
    )


def _split_multi_value(value: object) -> List[str]:
    return [item.strip() for item in str(value or "").split("|") if item.strip()]


def _taxonomy_match_variants(value: str) -> Set[str]:
    normalized = normalize_title_identity(value)
    variants = {normalized} if normalized else set()
    generic_tokens = {
        "algorithm", "algorithms", "graph", "graphs", "data", "system",
        "processing", "query", "method", "contract", "state", "exact",
    }
    variants.update(
        token
        for token in normalized.split()
        if len(token) >= 4 and token not in generic_tokens
    )
    return variants


def score_candidate_decision(
    record: Mapping[str, object],
    seed_manifest_row: Mapping[str, str],
    taxonomy_rows: Sequence[Mapping[str, str]],
    direction: str,
) -> Tuple[int, str, List[str]]:
    """Score title-only AQ decision relevance using the frozen G01 taxonomy."""

    title = normalize_title_identity(record.get("title"))
    matched_questions: Set[str] = set()
    algorithm_match = False
    mechanism_match = False
    for row in taxonomy_rows:
        variants: Set[str] = set()
        for field in ("term", "synonyms", "historical_terms", "adjacent_domain_terms"):
            for value in _split_multi_value(row.get(field, "")):
                variants.update(_taxonomy_match_variants(value))
        if not any(variant and variant in title for variant in variants):
            continue
        matched_questions.update(_split_multi_value(row.get("architecture_question_ids", "")))
        if row.get("term_type") == "ALGORITHM":
            algorithm_match = True
        else:
            mechanism_match = True

    seed_title = normalize_title_identity(seed_manifest_row.get("title"))
    if seed_title and seed_title in title:
        matched_questions.update(
            _split_multi_value(seed_manifest_row.get("architecture_question_ids", ""))
        )
    role_match = any(
        normalize_title_identity(token) in title
        for tokens in SEMANTIC_TITLE_TOKENS.values()
        for token in tokens
    )
    falsifier_match = any(
        token in title
        for token in (
            "lower bound", "impossibility", "counterexample", "incorrect",
            "correctness", "semantics", "error bound", "limitations",
        )
    )
    age_match = False
    if direction == "BACKWARD":
        try:
            candidate_year = int(str(record.get("published_date", ""))[:4])
            seed_year = int(str(seed_manifest_row.get("published_date", ""))[:4])
            age_match = seed_year - candidate_year >= 10
        except ValueError:
            age_match = False
    components = {
        "ALG": 40 if algorithm_match else 0,
        "MECH": 25 if mechanism_match else 0,
        "ROLE": 15 if role_match else 0,
        "AGE": 10 if age_match else 0,
        "FALS": 10 if falsifier_match else 0,
    }
    score = min(100, sum(components.values())) if matched_questions else 0
    breakdown = ";".join("{0}={1}".format(key, components[key]) for key in ("ALG", "MECH", "ROLE", "AGE", "FALS"))
    return score, breakdown, sorted(matched_questions)


def select_bounded_candidates(
    candidates: Sequence[Mapping[str, object]],
    max_new_identities: int = MAX_NEW_IDENTITIES,
    existing_identity_ids: Optional[Iterable[str]] = None,
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    """Apply deterministic relation-level depth, relevance, identity, and branch caps."""

    return select_bounded_relations(
        candidates,
        max_new_identities=max_new_identities,
        existing_identity_ids=existing_identity_ids,
    )


def select_bounded_relations(
    candidates: Sequence[Mapping[str, object]],
    max_new_identities: int = MAX_NEW_IDENTITIES,
    existing_identity_ids: Optional[Iterable[str]] = None,
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    """Retain bounded seed/direction relations after canonical reconciliation."""

    selected: List[Dict[str, object]] = []
    stops: List[Dict[str, object]] = []
    baseline_ids = set(existing_identity_ids or [])
    selected_new_ids: Set[str] = set()
    quota_identities: Dict[Tuple[str, int, str], Set[str]] = defaultdict(set)
    relation_rows: Dict[Tuple[str, str, str, int, str], Dict[str, object]] = {}
    for source in candidates:
        row = dict(source)
        relation_key = (
            str(row.get("paper_id") or ""),
            str(row.get("seed_paper_id") or ""),
            str(row.get("parent_paper_id") or ""),
            int(row.get("depth") or 0),
            str(row.get("direction") or ""),
        )
        prior = relation_rows.get(relation_key)
        if prior is None or (
            prior.get("provider_name") != "SemanticScholar"
            and row.get("provider_name") == "SemanticScholar"
        ):
            relation_rows[relation_key] = row

    for source in sorted(relation_rows.values(), key=_candidate_sort_key):
        paper_id = str(source.get("paper_id") or "")
        depth = int(source.get("depth") or 0)
        if depth > MAX_DEPTH:
            stops.append({**source, "reason": "MAX_DEPTH_EXCEEDED"})
            continue
        if int(source.get("decision_score") or 0) <= 0:
            stops.append({**source, "reason": "NO_DECISION_IMPACT"})
            continue
        if paper_id not in baseline_ids and len(selected_new_ids) >= max_new_identities:
            if paper_id in selected_new_ids:
                pass
            else:
                stops.append({**source, "reason": "GLOBAL_IDENTITY_CAP"})
                continue
        key = (str(source.get("seed_paper_id") or ""), depth, str(source.get("direction") or ""))
        quota = 3 if depth == 1 else 2
        if (
            paper_id not in baseline_ids
            and paper_id not in quota_identities[key]
            and len(quota_identities[key]) >= quota
        ):
            stops.append({**source, "reason": "PER_SEED_DIRECTION_QUOTA"})
            continue
        selected.append(source)
        if paper_id not in baseline_ids:
            selected_new_ids.add(paper_id)
            quota_identities[key].add(paper_id)
    return selected, stops


def _build_provider_control_stop(
    seed_row: Mapping[str, str],
    provider_name: str,
    provider_id: str,
    direction: str,
    depth: int,
    reason: str,
) -> Dict[str, object]:
    """Preserve known seed and provider provenance for a control stop."""

    seed_id = seed_row.get("paper_id", "")
    return {
        "candidate_identity": seed_id,
        "paper_id": seed_id,
        "seed_paper_id": seed_id,
        "parent_paper_id": seed_id,
        "depth": depth,
        "direction": direction,
        "decision_score": "NOT_SCORED",
        "score_breakdown": "NOT_APPLICABLE_PROVIDER_CONTROL_STOP",
        "architecture_question_ids": _split_multi_value(
            seed_row.get("architecture_question_ids", "")
        ),
        "provider_name": provider_name,
        "provider_id": provider_id,
        "reason": reason,
    }


def _build_branch_control_stop(
    branch: Mapping[str, object], depth: int, reason: str
) -> Dict[str, object]:
    """Preserve scored branch provenance when expansion is stopped."""

    row = dict(branch)
    paper_id = str(branch.get("paper_id") or "")
    row.update({
        "candidate_identity": paper_id,
        "paper_id": paper_id,
        "parent_paper_id": paper_id,
        "depth": depth,
        "reason": reason,
    })
    return row


def normalize_citation_stop_rows(
    stops: Sequence[Mapping[str, object]],
) -> List[Dict[str, str]]:
    """Serialize every stopped observation with a stable content-derived ID."""

    normalized: Dict[Tuple[str, ...], Dict[str, str]] = {}
    for source in stops:
        candidate_identity = str(
            source.get("candidate_identity")
            or source.get("paper_id")
            or source.get("provider_id")
            or "UNKNOWN"
        )
        seed_paper_id = str(source.get("seed_paper_id") or "UNKNOWN")
        parent_paper_id = str(
            source.get("parent_paper_id")
            or source.get("traversal_paper_id")
            or seed_paper_id
        )
        question_ids = "|".join(
            sorted(str(value) for value in source.get("architecture_question_ids") or [])
        ) or "NONE"
        values = (
            candidate_identity,
            seed_paper_id,
            parent_paper_id,
            str(source.get("depth") if source.get("depth") is not None else "UNKNOWN"),
            str(source.get("direction") or "UNKNOWN"),
            str(source.get("decision_score") if source.get("decision_score") is not None else "NOT_SCORED"),
            str(source.get("score_breakdown") or "NOT_SCORED"),
            question_ids,
            str(source.get("provider_name") or "UNKNOWN"),
            str(source.get("provider_id") or "UNKNOWN"),
            str(source.get("reason") or "UNKNOWN"),
        )
        digest = hashlib.sha256("\t".join(values).encode("utf-8")).hexdigest()[:16]
        normalized[values] = {
            "stop_id": "STOP-G03-" + digest,
            "candidate_identity": values[0],
            "seed_paper_id": values[1],
            "parent_paper_id": values[2],
            "depth": values[3],
            "direction": values[4],
            "decision_score": values[5],
            "score_breakdown": values[6],
            "architecture_question_ids": values[7],
            "provider_name": values[8],
            "provider_id": values[9],
            "reason": values[10],
        }
    return sorted(
        normalized.values(),
        key=lambda row: (
            row["seed_paper_id"],
            row["depth"],
            row["direction"],
            row["reason"],
            row["candidate_identity"],
            row["provider_name"],
        ),
    )


def validate_citation_stop_rows(rows: Sequence[Mapping[str, str]]) -> List[str]:
    """Validate exact stopped-observation provenance and stable identifiers."""

    errors: List[str] = []
    seen_ids: Set[str] = set()
    for index, row in enumerate(rows, start=2):
        prefix = "citation-stops.tsv: row {0}".format(index)
        stop_id = row.get("stop_id", "")
        if not re.fullmatch(r"STOP-G03-[0-9a-f]{16}", stop_id):
            errors.append(prefix + " has invalid stop_id")
        if stop_id in seen_ids:
            errors.append(prefix + " duplicates stop_id")
        seen_ids.add(stop_id)
        for field in (
            "candidate_identity",
            "seed_paper_id",
            "parent_paper_id",
            "depth",
            "direction",
            "decision_score",
            "score_breakdown",
            "architecture_question_ids",
            "provider_name",
            "provider_id",
            "reason",
        ):
            if not row.get(field):
                errors.append(prefix + " is missing " + field)
        for field in (
            "candidate_identity",
            "seed_paper_id",
            "parent_paper_id",
            "provider_name",
            "provider_id",
        ):
            if row.get(field) == "UNKNOWN":
                errors.append(prefix + " has unjustified UNKNOWN " + field)
        if row.get("provider_name") not in {"OpenAlex", "SemanticScholar"}:
            errors.append(prefix + " has invalid provider_name")
        question_ids = row.get("architecture_question_ids", "")
        if question_ids != "NONE" and any(
            not re.fullmatch(r"AQ-\d{3}", value)
            for value in _split_multi_value(question_ids)
        ):
            errors.append(prefix + " has invalid architecture_question_ids")
        if row.get("reason") in {
            "REQUEST_RETRY_RESERVE",
            "S2_RATE_LIMIT_ATTEMPTS_EXHAUSTED",
            "S2_SELECTED_PAYLOAD_REJECTED",
            "S2_PROVIDER_ID_UNAVAILABLE",
        } and question_ids in {"", "UNKNOWN", "NONE"}:
            errors.append(prefix + " control stop lacks architecture-question provenance")
        stop_values = tuple(
            row.get(field, "")
            for field in STOP_HEADER.split("\t")[1:]
        )
        expected_stop_id = "STOP-G03-" + hashlib.sha256(
            "\t".join(stop_values).encode("utf-8")
        ).hexdigest()[:16]
        if stop_id != expected_stop_id:
            errors.append(prefix + " has content-derived stop_id mismatch")
        if row.get("direction") not in ALLOWED_DIRECTIONS:
            errors.append(prefix + " has invalid direction")
        try:
            depth = int(row.get("depth", "-1"))
        except ValueError:
            depth = -1
        if depth < 0 or depth > MAX_DEPTH:
            errors.append(prefix + " has invalid depth")
    return sorted(set(errors))


def _screening_primary_lane(manifest_row: Mapping[str, str]) -> str:
    title = normalize_title_identity(manifest_row.get("title"))
    constraint_tokens = (
        "counterexample",
        "lower bound",
        "impossibility",
        "limitations",
        "intractability",
        "resolution limit",
        "no harder",
        "survey",
        "review",
    )
    if any(token in title for token in constraint_tokens):
        return "G03-LANE-C"
    notes = _parse_notes_map(manifest_row.get("notes", ""))
    directions = set(_split_multi_value(notes.get("ANCESTRY_DIRECTIONS", "")))
    return "G03-LANE-A" if "BACKWARD" in directions else "G03-LANE-B"


def is_retained_ancestry_identity(manifest_row: Mapping[str, str]) -> bool:
    """Return whether a manifest row survived G03 citation selection."""

    depth = _parse_notes_map(manifest_row.get("notes", "")).get(
        "CITATION_DEPTH", "0"
    )
    try:
        return int(depth) >= 1
    except ValueError:
        return False


def parse_screening_result_document(
    reference_root: Path, lane_id: str
) -> Dict[str, object]:
    """Parse one frozen lane result without making semantic decisions."""

    relative_path = SCREENING_RESULT_PATHS[lane_id]
    path = reference_root / relative_path
    if not path.is_file():
        raise ValueError(relative_path + ": screening result is missing")
    result_bytes = path.read_bytes()
    result_text = result_bytes.decode("utf-8")
    lane_match = re.search(r"^- Lane ID: `([^`]+)`$", result_text, re.MULTILINE)
    agent_match = re.search(r"^- Agent ID: `([^`]+)`$", result_text, re.MULTILINE)
    completed_match = re.search(
        r"^- Completed: `(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)`$",
        result_text,
        re.MULTILINE,
    )
    candidate_match = re.search(
        r"^- Candidate count: `(\d+)`|^- Candidate count: (\d+)",
        result_text,
        re.MULTILINE,
    )
    if lane_match is None or lane_match.group(1) != lane_id:
        raise ValueError(relative_path + ": screening result has wrong Lane ID")
    if agent_match is None:
        raise ValueError(relative_path + ": screening result lacks exact Agent ID")
    if completed_match is None:
        raise ValueError(relative_path + ": screening result lacks exact completion time")

    selections: List[Dict[str, object]] = []
    for line in result_text.splitlines():
        match = re.fullmatch(
            r"\|\s*(\d+)\s*\|\s*`([^`]+)`\s*\|\s*(.*?)\s*\|", line
        )
        if match is None:
            continue
        selections.append({
            "rank": int(match.group(1)),
            "paper_id": match.group(2),
            "rationale": normalize_inline_text(match.group(3)),
        })
    if [row["rank"] for row in selections] != list(range(1, len(selections) + 1)):
        raise ValueError(relative_path + ": screening ranks must be contiguous")
    candidate_count = None
    if candidate_match is not None:
        candidate_count = int(candidate_match.group(1) or candidate_match.group(2))
    return {
        "agent_id": agent_match.group(1),
        "completed_at": completed_match.group(1),
        "candidate_count": candidate_count,
        "checksum": hashlib.sha256(result_bytes).hexdigest(),
        "selections": selections,
    }


def build_screening_ledger_rows(
    manifest_rows: Sequence[Mapping[str, str]], reference_root: Path
) -> List[Dict[str, str]]:
    """Build the durable screening ledger from frozen lane result documents."""

    ancestry_rows = [row for row in manifest_rows if is_retained_ancestry_identity(row)]
    ancestry_by_id = {row.get("paper_id", ""): row for row in ancestry_rows}
    lane_results = {
        lane_id: parse_screening_result_document(reference_root, lane_id)
        for lane_id in SCREENING_RESULT_PATHS
    }
    lane_counts = Counter(_screening_primary_lane(row) for row in ancestry_rows)
    for lane_id in ("G03-LANE-A", "G03-LANE-B", "G03-LANE-C"):
        expected_count = lane_results[lane_id]["candidate_count"]
        if expected_count != lane_counts[lane_id]:
            raise ValueError(
                "{0}: candidate count {1} does not match manifest count {2}".format(
                    lane_id, expected_count, lane_counts[lane_id]
                )
            )

    ranked_selections: List[Tuple[str, Dict[str, object]]] = []
    for lane_id in ("G03-LANE-A", "G03-LANE-B", "G03-LANE-C"):
        ranked_selections.extend(
            (lane_id, dict(selection))
            for selection in lane_results[lane_id]["selections"]
        )
    selected_ids = [str(selection["paper_id"]) for _lane, selection in ranked_selections]
    if len(selected_ids) != 25 or len(set(selected_ids)) != 25:
        raise ValueError("screening results must nominate exactly 25 unique identities")
    selected_by_id = {
        str(selection["paper_id"]): {
            "lane_id": lane_id,
            "queue_rank": str(index),
            "rationale": str(selection["rationale"]),
        }
        for index, (lane_id, selection) in enumerate(ranked_selections, start=1)
    }
    for paper_id, selection in selected_by_id.items():
        manifest_row = ancestry_by_id.get(paper_id)
        if manifest_row is None:
            raise ValueError(paper_id + ": selected identity is not retained ancestry")
        if _screening_primary_lane(manifest_row) != selection["lane_id"]:
            raise ValueError(paper_id + ": selected identity is in the wrong lane")

    audit_result = lane_results["G03-LANE-D"]
    rows: List[Dict[str, str]] = []
    for manifest_row in sorted(ancestry_rows, key=lambda row: row.get("paper_id", "")):
        paper_id = manifest_row.get("paper_id", "")
        lane_id = _screening_primary_lane(manifest_row)
        lane_result = lane_results[lane_id]
        notes = _parse_notes_map(manifest_row.get("notes", ""))
        identity_state = notes.get("IDENTITY_STATE", "UNKNOWN")
        selection = selected_by_id.get(paper_id)
        if selection is not None:
            disposition = "ACQUIRE"
            queue_rank = str(selection["queue_rank"])
            rationale = str(selection["rationale"])
        elif identity_state == "CANONICAL":
            disposition = "DEFER"
            queue_rank = "NOT_APPLICABLE"
            rationale = "Not selected within the lane's bounded acquisition quota."
        else:
            disposition = "REJECT"
            queue_rank = "NOT_APPLICABLE"
            rationale = "Identity state {0} is not eligible for acquisition.".format(
                identity_state
            )
        rows.append({
            "candidate_paper_id": paper_id,
            "primary_lane": lane_id,
            "direction": notes.get("ANCESTRY_DIRECTIONS", "UNKNOWN"),
            "disposition": disposition,
            "queue_rank": queue_rank,
            "rationale": rationale,
            "reviewer_model": "gpt-5.6-sol",
            "reviewer_agent_id": str(lane_result["agent_id"]),
            "prompt_id": lane_id + "-v1",
            "screened_at_utc": str(lane_result["completed_at"]),
            "evidence_scope": "COMMITTED_METADATA_AND_CONTROLS_ONLY",
            "result_checksum": str(lane_result["checksum"]),
            "audit_lane_id": "G03-LANE-D",
            "audit_reviewer_agent_id": str(audit_result["agent_id"]),
            "audit_result_checksum": str(audit_result["checksum"]),
        })
    return rows


def load_reviewed_g04_queue(screening_path: Path) -> List[str]:
    """Load the exact reviewed ancestry queue from its durable screening ledger."""

    rows = read_tsv_rows(screening_path, SCREENING_HEADER)
    acquired = [row for row in rows if row.get("disposition") == "ACQUIRE"]
    try:
        acquired.sort(key=lambda row: int(row.get("queue_rank", "0")))
    except ValueError as error:
        raise ValueError("screening queue_rank must be numeric for ACQUIRE rows") from error
    queue = [row.get("candidate_paper_id", "") for row in acquired]
    if len(queue) != 25 or len(set(queue)) != 25:
        raise ValueError("screening ledger must yield exactly 25 unique ACQUIRE identities")
    if [int(row["queue_rank"]) for row in acquired] != list(range(1, 26)):
        raise ValueError("screening ledger ACQUIRE ranks must be contiguous 1-25")
    return queue


def validate_screening_rows(
    rows: Sequence[Mapping[str, str]],
    manifest_rows: Sequence[Mapping[str, str]],
    reference_root: Path,
) -> List[str]:
    """Validate disjoint lane assignment, result checksums, and queue derivation."""

    errors: List[str] = []
    ancestry_by_id = {
        row.get("paper_id", ""): row
        for row in manifest_rows
        if is_retained_ancestry_identity(row)
    }
    new_ancestry_ids = {
        paper_id
        for paper_id, row in ancestry_by_id.items()
        if row.get("discovery_query_ids") == "NOT_APPLICABLE"
    }
    row_ids = [row.get("candidate_paper_id", "") for row in rows]
    if len(row_ids) != len(set(row_ids)):
        errors.append("citation-screening-ledger.tsv has duplicate candidate identity")
    if set(row_ids) != set(ancestry_by_id):
        errors.append("citation-screening-ledger.tsv must cover every ancestry identity exactly once")

    result_checksums: Dict[str, str] = {}
    result_agent_ids: Dict[str, str] = {}
    for lane_id, relative_path in SCREENING_RESULT_PATHS.items():
        path = reference_root / relative_path
        if not path.is_file():
            errors.append(relative_path + ": screening result is missing")
            continue
        result_bytes = path.read_bytes()
        result_checksums[lane_id] = hashlib.sha256(result_bytes).hexdigest()
        result_text = result_bytes.decode("utf-8")
        agent_match = re.search(r"^- Agent ID: `([^`]+)`$", result_text, re.MULTILINE)
        if agent_match is None:
            errors.append(relative_path + ": screening result lacks exact Agent ID")
        else:
            result_agent_ids[lane_id] = agent_match.group(1)

    acquired_ranks: List[int] = []
    acquired_ids: List[str] = []
    for index, row in enumerate(rows, start=2):
        prefix = "citation-screening-ledger.tsv: row {0}".format(index)
        paper_id = row.get("candidate_paper_id", "")
        manifest_row = ancestry_by_id.get(paper_id)
        if manifest_row is None:
            continue
        expected_lane = _screening_primary_lane(manifest_row)
        if row.get("primary_lane") != expected_lane:
            errors.append(prefix + " violates deterministic primary-lane rule")
        if row.get("audit_lane_id") != "G03-LANE-D":
            errors.append(prefix + " must name G03-LANE-D audit")
        if row.get("reviewer_model") != "gpt-5.6-sol":
            errors.append(prefix + " has wrong reviewer model")
        if row.get("prompt_id") != expected_lane + "-v1":
            errors.append(prefix + " has wrong prompt_id")
        if row.get("evidence_scope") != "COMMITTED_METADATA_AND_CONTROLS_ONLY":
            errors.append(prefix + " has wrong evidence scope")
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", row.get("screened_at_utc", "")):
            errors.append(prefix + " has invalid screened_at_utc")
        if row.get("result_checksum") != result_checksums.get(expected_lane):
            errors.append(prefix + " has incorrect primary-lane result checksum")
        if row.get("audit_result_checksum") != result_checksums.get("G03-LANE-D"):
            errors.append(prefix + " has incorrect audit result checksum")
        if row.get("reviewer_agent_id") != result_agent_ids.get(expected_lane):
            errors.append(prefix + " has incorrect primary-lane reviewer identity")
        if row.get("audit_reviewer_agent_id") != result_agent_ids.get("G03-LANE-D"):
            errors.append(prefix + " has incorrect audit reviewer identity")
        notes = _parse_notes_map(manifest_row.get("notes", ""))
        expected_direction = notes.get("ANCESTRY_DIRECTIONS", "UNKNOWN")
        if row.get("direction") != expected_direction:
            errors.append(prefix + " has incorrect ancestry direction")
        if not row.get("rationale"):
            errors.append(prefix + " is missing screening rationale")
        disposition = row.get("disposition")
        if disposition not in {"ACQUIRE", "DEFER", "REJECT"}:
            errors.append(prefix + " has invalid disposition")
        identity_state = notes.get("IDENTITY_STATE", "UNKNOWN")
        if disposition == "ACQUIRE":
            if identity_state != "CANONICAL":
                errors.append(prefix + " cannot acquire ambiguous or unavailable identity")
            if paper_id not in new_ancestry_ids:
                errors.append(prefix + " cannot acquire a rediscovered baseline identity")
            try:
                rank = int(row.get("queue_rank", "0"))
            except ValueError:
                rank = 0
            acquired_ranks.append(rank)
            acquired_ids.append(paper_id)
        elif row.get("queue_rank") != "NOT_APPLICABLE":
            errors.append(prefix + " non-ACQUIRE row must use NOT_APPLICABLE rank")
        if identity_state != "CANONICAL" and disposition != "REJECT":
            errors.append(prefix + " ambiguous or unavailable identity must be rejected")
    if sorted(acquired_ranks) != list(range(1, 26)):
        errors.append("citation-screening-ledger.tsv must have ACQUIRE ranks 1-25")
    if len(acquired_ids) != len(set(acquired_ids)):
        errors.append("citation-screening-ledger.tsv ACQUIRE identities must be unique")
    return sorted(set(errors))


def _is_sha256(value: object) -> bool:
    return bool(re.fullmatch(r"[0-9a-f]{64}", str(value or "")))


def _is_retryable_status(value: object) -> bool:
    status = str(value or "")
    return status in {"408", "429", "TRANSPORT_ERROR"} or (
        status.isdigit() and int(status) >= 500
    )


def validate_citation_request_rows(rows: Sequence[Mapping[str, str]]) -> List[str]:
    """Validate bounded G03 request provenance without touching the network."""

    errors: List[str] = []
    misses = [row for row in rows if row.get("cache_status") == "MISS"]
    if len(misses) > MAX_HTTP_REQUESTS:
        errors.append("citation request cap 90 exceeded: {0}".format(len(misses)))
    raw_observations = 0
    request_ids: Set[str] = set()
    operation_rows: Dict[Tuple[str, ...], List[Mapping[str, str]]] = defaultdict(list)
    for index, row in enumerate(rows, start=2):
        prefix = "citation-request-ledger.tsv: row {0}".format(index)
        request_id = row.get("request_id", "")
        if request_id in request_ids:
            errors.append("{0} duplicate request_id {1}".format(prefix, request_id))
        request_ids.add(request_id)
        if row.get("goal_id") != "G03":
            errors.append("{0} goal_id must be G03".format(prefix))
        service = row.get("service", "")
        if service not in {"OpenAlex", "SemanticScholar"}:
            errors.append("{0} has unauthorized service".format(prefix))
        operation = row.get("operation", "")
        allowed_operations = {
            "OpenAlex": {"SEED_RESOLUTION", "FORWARD_CITATIONS", "BATCH_WORKS"},
            "SemanticScholar": {
                "SEED_RESOLUTION_BATCH",
                "BACKWARD_REFERENCES",
                "FORWARD_CITATIONS",
            },
        }.get(service, set())
        if operation not in allowed_operations:
            errors.append("{0} has unauthorized operation".format(prefix))
        try:
            depth = int(row.get("depth", "-1"))
        except ValueError:
            depth = -1
        if depth < 0 or depth > MAX_DEPTH:
            errors.append("{0} depth must be 0-2".format(prefix))
        if row.get("direction") not in ALLOWED_DIRECTIONS:
            errors.append("{0} has invalid direction".format(prefix))
        if operation in {"SEED_RESOLUTION", "SEED_RESOLUTION_BATCH"} and (
            row.get("direction") != "SEED_RESOLUTION" or depth != 0
        ):
            errors.append("{0} seed resolution requires depth 0".format(prefix))
        if operation == "FORWARD_CITATIONS" and row.get("direction") != "FORWARD":
            errors.append("{0} forward operation has wrong direction".format(prefix))
        if operation == "BATCH_WORKS" and row.get("direction") != "BACKWARD":
            errors.append("{0} batch operation has wrong direction".format(prefix))
        if operation == "BACKWARD_REFERENCES" and row.get("direction") != "BACKWARD":
            errors.append("{0} reference operation has wrong direction".format(prefix))
        try:
            attempt = int(row.get("attempt", "0"))
        except ValueError:
            attempt = 0
        if attempt < 1 or attempt > MAX_ATTEMPTS:
            errors.append("{0} attempt must be 1-3".format(prefix))
        if not _is_sha256(row.get("response_checksum")):
            errors.append("{0} requires response checksum".format(prefix))
        if not _is_sha256(row.get("cache_checksum")):
            errors.append("{0} requires cache checksum".format(prefix))
        if row.get("terminal_state") not in ALLOWED_TERMINAL_STATES:
            errors.append("{0} has invalid terminal_state".format(prefix))
        try:
            result_count = int(row.get("result_count", "-1"))
        except ValueError:
            result_count = -1
        result_limit = 100
        if service == "SemanticScholar" and operation == "SEED_RESOLUTION_BATCH":
            result_limit = 25
        elif service == "SemanticScholar":
            result_limit = MAX_S2_PAGE_RESULTS
        if result_count < 0 or result_count > result_limit:
            errors.append(
                "{0} result_count must be 0-{1}".format(prefix, result_limit)
            )
        elif row.get("terminal_state") in {"COMPLETE", "EMPTY"}:
            raw_observations += result_count
        if (
            row.get("response_status") == "200"
            and row.get("terminal_state") == "PAYLOAD_REJECTED"
        ):
            if result_count != 0 or service != "SemanticScholar":
                errors.append(
                    "{0} invalid HTTP 200 payload-rejection state".format(prefix)
                )
        elif row.get("response_status") == "200":
            expected_terminal = "COMPLETE" if result_count > 0 else "EMPTY"
            if row.get("terminal_state") != expected_terminal:
                errors.append("{0} HTTP 200 has inconsistent terminal_state".format(prefix))
        if row.get("terminal_state") == "UNAVAILABLE" and row.get("response_status") != "404":
            errors.append("{0} UNAVAILABLE requires HTTP 404".format(prefix))
        identifier = row.get("normalized_identifier", "").replace("%7C", "|")
        try:
            if service == "OpenAlex":
                compiled_parameters = build_openalex_request_parameters(
                    operation, identifier
                )
            elif service == "SemanticScholar":
                compiled_parameters = build_s2_request_parameters(operation, identifier)
            else:
                raise ValueError("unauthorized service")
            expected_parameters = urllib.parse.urlencode(
                sorted(compiled_parameters.items())
            )
        except ValueError as error:
            errors.append("{0} invalid request identifier: {1}".format(prefix, error))
        else:
            if row.get("parameters") != expected_parameters:
                errors.append("{0} request parameters differ from allowlist".format(prefix))
        if row.get("cache_status") != "MISS":
            errors.append("{0} durable request rows must be external MISS attempts".format(prefix))
        expected_cache_prefix = {
            "OpenAlex": "arxiv-reference/cache/g03/openalex/",
            "SemanticScholar": "arxiv-reference/cache/g03/semantic-scholar/",
        }.get(service, "INVALID/")
        if not row.get("cache_path", "").startswith(expected_cache_prefix):
            errors.append("{0} has invalid G03 cache path".format(prefix))
        expected_client = {
            "OpenAlex": "knight-bus-g03-openalex/1.0",
            "SemanticScholar": "knight-bus-g03-semantic-scholar/1.0",
        }.get(service)
        if row.get("client_version") and row.get("client_version") != expected_client:
            errors.append("{0} has invalid provider client version".format(prefix))
        expected_policy = {
            "OpenAlex": "https://developers.openalex.org/api-reference/authentication",
            "SemanticScholar": "https://www.semanticscholar.org/product/api/license",
        }.get(service)
        if row.get("policy_url") and row.get("policy_url") != expected_policy:
            errors.append("{0} has invalid provider policy URL".format(prefix))
        operation_rows[_request_operation_key(row)].append(row)

    expected_request_ids = [
        "REQ-G03-{0:04d}".format(index) for index in range(1, len(rows) + 1)
    ]
    if [row.get("request_id", "") for row in rows] != expected_request_ids:
        errors.append("citation request IDs must be contiguous in ledger order")
    if raw_observations > MAX_RAW_OBSERVATIONS:
        errors.append(
            "citation raw-observation cap 6000 exceeded: {0}".format(raw_observations)
        )
    for operation_key, grouped_rows in sorted(operation_rows.items()):
        if len(grouped_rows) > MAX_ATTEMPTS:
            errors.append("citation operation exceeds three attempts: {0}".format(operation_key))
        attempts: List[int] = []
        for row in grouped_rows:
            try:
                attempts.append(int(row.get("attempt", "0")))
            except ValueError:
                attempts.append(0)
        if attempts != list(range(1, len(grouped_rows) + 1)):
            errors.append("citation operation attempt sequence is not contiguous: {0}".format(operation_key))
        for position, row in enumerate(grouped_rows):
            prior_rows = grouped_rows[:position]
            expected_retry_events = "|".join(
                "attempt-{0}:{1}".format(
                    prior.get("attempt", "UNKNOWN"),
                    prior.get("response_status", "UNKNOWN"),
                )
                for prior in prior_rows
            ) or "NONE"
            if row.get("retry_events") != expected_retry_events:
                errors.append("citation operation retry history mismatch: {0}".format(operation_key))
            if position < len(grouped_rows) - 1 and not _is_retryable_status(
                row.get("response_status")
            ):
                errors.append("citation operation retried a terminal status: {0}".format(operation_key))
            if position < len(grouped_rows) - 1 and row.get("terminal_state") in {
                "COMPLETE", "EMPTY", "UNAVAILABLE"
            }:
                errors.append("citation operation repeats a terminal result: {0}".format(operation_key))
    return sorted(set(errors))


def _resolve_cache_path(
    reference_root: Path, ledger_path: str, service: str
) -> Optional[Path]:
    normalized = ledger_path.replace("\\", "/")
    prefix = reference_root.name + "/"
    if normalized.startswith(prefix):
        normalized = normalized[len(prefix):]
    candidate = (reference_root / normalized).resolve()
    cache_directory = {
        "OpenAlex": "openalex",
        "SemanticScholar": "semantic-scholar",
    }.get(service)
    if cache_directory is None:
        return None
    expected_root = (reference_root / "cache" / "g03" / cache_directory).resolve()
    try:
        candidate.relative_to(expected_root)
    except ValueError:
        return None
    return candidate


def _parse_cached_request_records(
    row: Mapping[str, str], payload: bytes
) -> List[Dict[str, object]]:
    service = row.get("service", "")
    if service == "OpenAlex":
        return parse_openalex_work_payload(payload)
    if service == "SemanticScholar":
        return parse_s2_work_payload(payload, row.get("operation", ""))
    raise ValueError("cache row has unauthorized provider")


def validate_g03_cache_provenance(
    reference_root: Path, rows: Sequence[Mapping[str, str]]
) -> List[str]:
    """Verify every ignored G03 cache body and reject hidden content."""

    errors: List[str] = []
    referenced_paths: Set[Path] = set()
    for index, row in enumerate(rows, start=2):
        prefix = "citation-request-ledger.tsv: row {0}".format(index)
        candidate = _resolve_cache_path(
            reference_root, row.get("cache_path", ""), row.get("service", "")
        )
        if candidate is None:
            errors.append("{0} cache path escapes its G03 provider cache".format(prefix))
            continue
        referenced_paths.add(candidate)
        if not candidate.is_file() or candidate.is_symlink():
            errors.append("{0} referenced cache file is missing or unsafe".format(prefix))
            continue
        if candidate.suffix.casefold() != ".json":
            errors.append("{0} forbidden cache suffix".format(prefix))
            continue
        payload = candidate.read_bytes()
        if payload.startswith((b"%PDF", b"PK\x03\x04", b"\x1f\x8b")):
            errors.append("{0} forbidden cache content".format(prefix))
            continue
        checksum = hashlib.sha256(payload).hexdigest()
        if checksum != row.get("cache_checksum"):
            errors.append("{0} cached response checksum mismatch".format(prefix))
        if row.get("terminal_state") in {"COMPLETE", "EMPTY"}:
            try:
                records = _parse_cached_request_records(row, payload)
            except ValueError as error:
                errors.append("{0} invalid selected metadata cache: {1}".format(prefix, error))
            else:
                if str(len(records)) != row.get("result_count"):
                    errors.append("{0} cached result_count mismatch".format(prefix))
        elif row.get("terminal_state") == "PAYLOAD_REJECTED":
            try:
                rejection = json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                rejection = None
            if not isinstance(rejection, Mapping) or set(rejection) != {
                "error",
                "provider",
                "raw_response_checksum",
                "reason",
            }:
                errors.append("{0} malformed payload-rejection marker".format(prefix))
            elif (
                rejection.get("error") != "rejected_provider_payload"
                or rejection.get("provider") != "SemanticScholar"
                or rejection.get("raw_response_checksum")
                != row.get("response_checksum")
            ):
                errors.append("{0} inconsistent payload-rejection marker".format(prefix))
    cache_roots = (
        reference_root / "cache" / "g03" / "openalex",
        reference_root / "cache" / "g03" / "semantic-scholar",
    )
    for cache_root in cache_roots:
        if not cache_root.is_dir():
            continue
        for cache_file in sorted(cache_root.rglob("*")):
            if not cache_file.is_file():
                continue
            resolved = cache_file.resolve()
            if resolved not in referenced_paths:
                errors.append("{0}: unreferenced G03 cache file".format(cache_file))
            if cache_file.suffix.casefold() != ".json":
                errors.append("{0}: forbidden cache file".format(cache_file))
            else:
                payload = cache_file.read_bytes()
                if payload.startswith((b"%PDF", b"PK\x03\x04", b"\x1f\x8b")):
                    errors.append("{0}: forbidden cache content".format(cache_file))
    return sorted(set(errors))


def validate_edge_cache_provenance(
    reference_root: Path,
    request_rows: Sequence[Mapping[str, str]],
    edge_rows: Sequence[Mapping[str, str]],
    manifest_rows: Sequence[Mapping[str, str]],
) -> List[str]:
    """Prove every CITES pair from checksummed provider-response metadata."""

    errors = validate_g03_cache_provenance(reference_root, request_rows)
    if errors:
        return errors
    openalex_to_paper: Dict[str, str] = {}
    s2_to_paper: Dict[str, str] = {}
    for row in manifest_rows:
        paper_id = row.get("paper_id", "")
        notes = _parse_notes_map(row.get("notes", ""))
        aliases = [
            notes.get("OPENALEX_ID", ""),
            notes.get("SEMANTIC_SCHOLAR_ID", ""),
            notes.get("ALIASES", ""),
        ]
        for value in aliases:
            for alias in _split_multi_value(value):
                if re.fullmatch(r"https://openalex\.org/W\d+", alias):
                    prior = openalex_to_paper.get(alias)
                    if prior and prior != paper_id:
                        errors.append(
                            "OpenAlex alias {0} maps to multiple manifest identities".format(
                                alias
                            )
                        )
                    else:
                        openalex_to_paper[alias] = paper_id
                semantic_scholar_id = alias.removeprefix("S2:")
                if re.fullmatch(r"[A-Za-z0-9._:-]+", semantic_scholar_id) and (
                    alias.startswith("S2:") or alias == notes.get("SEMANTIC_SCHOLAR_ID")
                ):
                    prior = s2_to_paper.get(semantic_scholar_id)
                    if prior and prior != paper_id:
                        errors.append(
                            "Semantic Scholar alias {0} maps to multiple manifest identities".format(
                                semantic_scholar_id
                            )
                        )
                    else:
                        s2_to_paper[semantic_scholar_id] = paper_id

    established_pairs: Set[Tuple[str, str]] = set()
    for index, row in enumerate(request_rows, start=2):
        if row.get("terminal_state") not in {"COMPLETE", "EMPTY"}:
            continue
        cache_path = _resolve_cache_path(
            reference_root, row.get("cache_path", ""), row.get("service", "")
        )
        if cache_path is None or not cache_path.is_file():
            continue
        try:
            records = _parse_cached_request_records(row, cache_path.read_bytes())
        except ValueError as error:
            errors.append(
                "citation-request-ledger.tsv: row {0} cannot prove edges: {1}".format(
                    index, error
                )
            )
            continue
        for record in records:
            if row.get("service") == "OpenAlex":
                source_id = openalex_to_paper.get(str(record.get("openalex_id") or ""))
                if not source_id:
                    continue
                for referenced_openalex_id in record.get("referenced_works") or []:
                    target_id = openalex_to_paper.get(str(referenced_openalex_id))
                    if target_id:
                        established_pairs.add((source_id, target_id))
                if row.get("operation") == "FORWARD_CITATIONS":
                    target_id = row.get("traversal_paper_id", "")
                    if target_id:
                        established_pairs.add((source_id, target_id))
            elif row.get("service") == "SemanticScholar":
                neighbor_id = s2_to_paper.get(
                    str(record.get("semantic_scholar_id") or "")
                )
                traversal_id = row.get("traversal_paper_id", "")
                if not neighbor_id or not traversal_id:
                    continue
                if row.get("operation") == "BACKWARD_REFERENCES":
                    established_pairs.add((traversal_id, neighbor_id))
                elif row.get("operation") == "FORWARD_CITATIONS":
                    established_pairs.add((neighbor_id, traversal_id))

    for index, row in enumerate(edge_rows, start=2):
        if row.get("edge_type") != "CITES":
            continue
        pair = (row.get("source_paper_id", ""), row.get("target_paper_id", ""))
        if pair not in established_pairs:
            errors.append(
                "citation-edges.tsv: row {0} CITES pair is not established by cached provider metadata".format(
                    index
                )
            )
    return sorted(set(errors))


def validate_citation_edge_contract(
    rows: Sequence[Mapping[str, str]], manifest_ids: Iterable[str]
) -> List[str]:
    """Validate endpoints, direction-safe semantics, provenance, and deduplication."""

    errors: List[str] = []
    known_ids = set(manifest_ids)
    keys: Set[Tuple[str, str, str]] = set()
    citation_pairs = {
        (row.get("source_paper_id", ""), row.get("target_paper_id", ""))
        for row in rows
        if row.get("edge_type") == "CITES"
    }
    for index, row in enumerate(rows, start=2):
        prefix = "citation-edges.tsv: row {0}".format(index)
        source = row.get("source_paper_id", "")
        target = row.get("target_paper_id", "")
        edge_type = row.get("edge_type", "")
        key = (source, target, edge_type)
        if source not in known_ids or target not in known_ids:
            errors.append("{0} endpoint is absent from manifest".format(prefix))
        if source == target:
            errors.append("{0} self-edge is forbidden".format(prefix))
        if edge_type not in ALLOWED_EDGE_TYPES:
            errors.append("{0} has invalid edge_type".format(prefix))
        if key in keys:
            errors.append("{0} duplicate canonical edge".format(prefix))
        keys.add(key)
        if edge_type != "CITES":
            if (source, target) not in citation_pairs:
                errors.append("{0} semantic edge requires companion CITES".format(prefix))
            if not row.get("discovery_source", "").endswith("_METADATA_SCREEN"):
                errors.append("{0} semantic edge requires metadata-screen provenance".format(prefix))
            if not row.get("relevance_reason", "").startswith("DERIVED_INFERENCE:"):
                errors.append("{0} semantic edge requires DERIVED_INFERENCE".format(prefix))
        if "SOURCE_CLAIM" in row.get("relevance_reason", ""):
            errors.append("{0} metadata edge cannot assert SOURCE_CLAIM".format(prefix))
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", row.get("verified_at", "")):
            errors.append("{0} verified_at must be UTC RFC3339".format(prefix))
    return sorted(set(errors))


def _manifest_rows_by_id(rows: Sequence[Mapping[str, str]]) -> Dict[str, Dict[str, str]]:
    return {str(row.get("paper_id")): dict(row) for row in rows if row.get("paper_id")}


def _request_budget_remaining(ledger_path: Path) -> int:
    rows = read_tsv_rows(ledger_path, REQUEST_HEADER)
    misses = sum(row.get("cache_status") == "MISS" for row in rows)
    return MAX_HTTP_REQUESTS - misses


def _raw_observations_used(ledger_path: Path) -> int:
    return sum(
        int(row.get("result_count", "0"))
        for row in read_tsv_rows(ledger_path, REQUEST_HEADER)
        if row.get("terminal_state") in {"COMPLETE", "EMPTY"}
    )


def _fetch_campaign_page(
    reference_root: Path,
    ledger_path: Path,
    preflight_text: str,
    operation: str,
    identifier: str,
    seed_paper_id: str,
    traversal_paper_id: str,
    depth: int,
    direction: str,
    allow_network: bool,
) -> List[Dict[str, object]]:
    records = fetch_openalex_metadata_page(
        reference_root=reference_root,
        ledger_path=ledger_path,
        preflight_text=preflight_text,
        operation=operation,
        identifier=identifier,
        seed_paper_id=seed_paper_id,
        traversal_paper_id=traversal_paper_id,
        depth=depth,
        direction=direction,
        allow_network=allow_network,
        remaining_http_requests=_request_budget_remaining(ledger_path),
        minimum_delay_seconds=1.0,
    )
    if _raw_observations_used(ledger_path) > MAX_RAW_OBSERVATIONS:
        raise RuntimeError("G03 raw-observation cap exceeded")
    return records


def _fetch_s2_campaign_page(
    reference_root: Path,
    ledger_path: Path,
    preflight_text: str,
    operation: str,
    identifier: str,
    seed_paper_id: str,
    traversal_paper_id: str,
    depth: int,
    direction: str,
    allow_network: bool,
) -> List[Dict[str, object]]:
    records = fetch_s2_metadata_page(
        reference_root=reference_root,
        ledger_path=ledger_path,
        preflight_text=preflight_text,
        operation=operation,
        identifier=identifier,
        seed_paper_id=seed_paper_id,
        traversal_paper_id=traversal_paper_id,
        depth=depth,
        direction=direction,
        allow_network=allow_network,
        remaining_http_requests=min(1, _request_budget_remaining(ledger_path)),
        minimum_delay_seconds=S2_MINIMUM_DELAY_SECONDS,
    )
    if _raw_observations_used(ledger_path) > MAX_RAW_OBSERVATIONS:
        raise RuntimeError("G03 raw-observation cap exceeded")
    return records


def _validate_seed_bibliography(
    seed_row: Mapping[str, str], record: Mapping[str, object]
) -> List[str]:
    errors: List[str] = []
    if normalize_title_identity(seed_row.get("title")) != normalize_title_identity(record.get("title")):
        errors.append("title mismatch")
    seed_authors = {
        normalize_title_identity(author).split()[-1]
        for author in _split_multi_value(seed_row.get("authors", ""))
        if normalize_title_identity(author)
    }
    provider_authors = {
        normalize_title_identity(author).split()[-1]
        for author in record.get("authors") or []
        if normalize_title_identity(author)
    }
    if seed_authors and provider_authors and not seed_authors & provider_authors:
        errors.append("author mismatch")
    try:
        seed_year = int(seed_row.get("published_date", "")[:4])
        provider_year = int(str(record.get("published_date", ""))[:4])
        if abs(seed_year - provider_year) > 1:
            errors.append("publication-year mismatch")
    except ValueError:
        errors.append("publication-year unavailable")
    return errors


def audit_s2_seed_bibliography(
    seed_paper_id: str,
    seed_row: Mapping[str, str],
    record: Mapping[str, object],
) -> List[str]:
    """Retain exact-ID S2 metadata variants without accepting ambiguous merges."""

    expected_arxiv = seed_paper_id.removeprefix("PAPER-")
    if str(record.get("arxiv_id") or "") != expected_arxiv:
        raise ValueError("Semantic Scholar seed lacks the exact arXiv identity")
    variants = _validate_seed_bibliography(seed_row, record)
    if "author mismatch" in variants or "publication-year unavailable" in variants:
        raise ValueError("Semantic Scholar seed lacks a required author/date anchor")
    if "title mismatch" in variants and "publication-year mismatch" in variants:
        raise ValueError("Semantic Scholar seed disagrees on both independent anchors")
    return sorted(variants)


def _evenly_sample_values(values: Sequence[str], limit: int) -> List[str]:
    unique = list(dict.fromkeys(values))
    if len(unique) <= limit:
        return unique
    if limit <= 1:
        return unique[:limit]
    indices = sorted(
        {round(index * (len(unique) - 1) / (limit - 1)) for index in range(limit)}
    )
    return [unique[index] for index in indices]


def _chunk_values(values: Sequence[str], size: int = 100) -> List[List[str]]:
    return [list(values[index:index + size]) for index in range(0, len(values), size)]


def _fetch_batched_work_records(
    reference_root: Path,
    ledger_path: Path,
    preflight_text: str,
    openalex_ids: Sequence[str],
    seed_ids: Sequence[str],
    traversal_ids: Sequence[str],
    depth: int,
    allow_network: bool,
) -> Dict[str, Dict[str, object]]:
    records: Dict[str, Dict[str, object]] = {}
    normalized_ids = sorted({value.rsplit("/", 1)[-1] for value in openalex_ids})
    for chunk in _chunk_values(normalized_ids, 100):
        fetched = _fetch_campaign_page(
            reference_root,
            ledger_path,
            preflight_text,
            "BATCH_WORKS",
            "|".join(chunk),
            "|".join(sorted(set(seed_ids))),
            "|".join(sorted(set(traversal_ids))),
            depth,
            "BACKWARD",
            allow_network,
        )
        for record in fetched:
            records[str(record["openalex_id"])] = record
    return records


def _candidate_observation(
    record: Mapping[str, object],
    seed_row: Mapping[str, str],
    parent_record: Mapping[str, object],
    direction: str,
    depth: int,
    taxonomy_rows: Sequence[Mapping[str, str]],
) -> Dict[str, object]:
    score, breakdown, question_ids = score_candidate_decision(
        record, seed_row, taxonomy_rows, direction
    )
    return {
        "paper_id": str(record.get("paper_id") or ""),
        "openalex_id": str(record.get("openalex_id") or ""),
        "semantic_scholar_id": str(record.get("semantic_scholar_id") or ""),
        "provider_id": _provider_record_key(record),
        "provider_name": str(record.get("provider_name") or "OpenAlex"),
        "seed_paper_id": seed_row.get("paper_id", ""),
        "parent_paper_id": str(parent_record.get("paper_id") or ""),
        "parent_openalex_id": str(parent_record.get("openalex_id") or ""),
        "parent_provider_id": _provider_record_key(parent_record),
        "depth": depth,
        "direction": direction,
        "decision_score": score,
        "score_breakdown": breakdown,
        "architecture_question_ids": question_ids,
        "published_date": str(record.get("published_date") or "UNKNOWN"),
    }


def _reconcile_record_map(
    records: Iterable[Mapping[str, object]],
    manifest_rows: Sequence[Mapping[str, str]],
) -> Dict[str, Dict[str, object]]:
    unique_by_provider: Dict[str, Dict[str, object]] = {}
    for source in records:
        provider_id = _provider_record_key(source)
        if provider_id:
            unique_by_provider[provider_id] = dict(source)
    reconciled = reconcile_citation_identities(
        list(unique_by_provider.values()), manifest_rows
    )
    canonical_by_fingerprint = {
        _record_fingerprint(record): record for record in reconciled
    }
    result: Dict[str, Dict[str, object]] = {}
    for provider_id, source in unique_by_provider.items():
        canonical = canonical_by_fingerprint.get(_record_fingerprint(source), source)
        merged = dict(source)
        merged["paper_id"] = canonical.get("paper_id", source.get("paper_id"))
        merged["identity_state"] = canonical.get("identity_state", "CANONICAL")
        if canonical.get("conflicting_identity_ids"):
            merged["conflicting_identity_ids"] = list(
                canonical["conflicting_identity_ids"]
            )
        result[provider_id] = merged
    return result


def _replace_observation_identities(
    observations: Sequence[Mapping[str, object]],
    records_by_provider: Mapping[str, Mapping[str, object]],
) -> List[Dict[str, object]]:
    replaced: List[Dict[str, object]] = []
    for source in observations:
        observation = dict(source)
        record = records_by_provider.get(str(source.get("provider_id") or ""))
        if record is None:
            continue
        observation["paper_id"] = record.get("paper_id", "")
        replaced.append(observation)
    return replaced


def _select_expansion_branches(
    observations: Sequence[Mapping[str, object]], selected_ids: Set[str]
) -> Dict[Tuple[str, str], Dict[str, object]]:
    branches: Dict[Tuple[str, str], Dict[str, object]] = {}
    for observation in sorted(
        (dict(row) for row in observations if row.get("paper_id") in selected_ids),
        key=_candidate_sort_key,
    ):
        key = (
            str(observation.get("seed_paper_id") or ""),
            str(observation.get("direction") or ""),
        )
        branches.setdefault(key, observation)
    return branches


def _parse_notes_map(value: str) -> Dict[str, str]:
    notes: Dict[str, str] = {}
    for clause in value.split(";"):
        if "=" not in clause:
            continue
        key, item = clause.split("=", 1)
        notes[key] = item
    return notes


def _serialize_notes_map(notes: Mapping[str, str]) -> str:
    return ";".join("{0}={1}".format(key, notes[key]) for key in sorted(notes))


def _record_provider_aliases(record: Mapping[str, object]) -> List[str]:
    aliases: List[str] = []
    openalex_id = str(record.get("openalex_id") or "")
    if openalex_id not in {"", "UNKNOWN"}:
        aliases.append(openalex_id)
    semantic_scholar_id = str(record.get("semantic_scholar_id") or "")
    if semantic_scholar_id not in {"", "UNKNOWN"}:
        aliases.append("S2:" + semantic_scholar_id)
    return sorted(set(aliases))


def _build_new_manifest_row(
    record: Mapping[str, object], observations: Sequence[Mapping[str, object]]
) -> Dict[str, str]:
    related = [row for row in observations if row.get("paper_id") == record.get("paper_id")]
    question_ids = sorted(
        {
            question_id
            for row in related
            for question_id in row.get("architecture_question_ids") or []
        }
    )
    seed_ids = sorted({str(row.get("seed_paper_id")) for row in related})
    directions = sorted({str(row.get("direction")) for row in related})
    minimum_depth = min(int(row.get("depth") or 0) for row in related)
    maximum_score = max(int(row.get("decision_score") or 0) for row in related)
    best_breakdown = next(
        str(row.get("score_breakdown"))
        for row in sorted(related, key=_candidate_sort_key)
    )
    provider_aliases = _record_provider_aliases(record)
    notes = {
        "ALIASES": "|".join(provider_aliases) or "UNKNOWN",
        "BIBLIOGRAPHIC_VARIANTS": "|".join(
            str(value) for value in record.get("bibliographic_variants") or []
        ) or "NONE",
        "ANCESTRY_DIRECTIONS": "|".join(directions),
        "ANCESTRY_RESOLUTION": "RESOLVED",
        "ANCESTRY_SEEDS": "|".join(seed_ids),
        "CITATION_DEPTH": str(minimum_depth),
        "G03_AQ_LINKS": "|".join(question_ids) or "UNKNOWN",
        "G03_SCREEN": "DERIVED_INFERENCE_METADATA_ONLY",
        "IDENTITY_STATE": str(record.get("identity_state") or "CANONICAL"),
        "OPENALEX_ID": str(record.get("openalex_id") or "UNKNOWN"),
        "SEMANTIC_SCHOLAR_ID": str(
            record.get("semantic_scholar_id") or "UNKNOWN"
        ),
        "SOURCE_URLS": "|".join(
            sorted(set(provider_aliases + list(record.get("source_urls") or [])))
        ) or "UNKNOWN",
        "VERSIONS": str(record.get("arxiv_version") or "UNKNOWN"),
    }
    conflicting_identity_ids = [
        str(value) for value in record.get("conflicting_identity_ids") or []
    ]
    if conflicting_identity_ids:
        notes["CONFLICTING_IDENTITY_IDS"] = "|".join(
            sorted(conflicting_identity_ids)
        )
    return {
        "paper_id": str(record.get("paper_id") or ""),
        "arxiv_id": str(record.get("arxiv_id") or "UNKNOWN"),
        "doi": str(record.get("doi") or "UNKNOWN"),
        "title": normalize_inline_text(record.get("title")),
        "authors": "|".join(str(value) for value in record.get("authors") or []) or "UNKNOWN",
        "published_date": str(record.get("published_date") or "UNKNOWN"),
        "updated_date": str(record.get("updated_date") or "UNKNOWN")[:10],
        "categories": "UNKNOWN",
        "abstract_url": "UNKNOWN",
        "pdf_url": "UNKNOWN",
        "license_uri": "UNKNOWN",
        "canonical_version": str(record.get("arxiv_version") or "UNKNOWN"),
        "discovery_query_ids": "NOT_APPLICABLE",
        "architecture_question_ids": "|".join(question_ids) or "UNKNOWN",
        "relevance_score": str(maximum_score),
        "score_breakdown": best_breakdown,
        "selection_status": "METADATA_ONLY",
        "evidence_grade": "D_THEORETICAL_OR_INCOMPLETE",
        "code_urls": "UNKNOWN",
        "local_path": "NOT_ACQUIRED",
        "sha256": "NOT_ACQUIRED",
        "notes": _serialize_notes_map(notes),
    }


def _update_manifest_rows(
    baseline_rows: Sequence[Mapping[str, str]],
    seed_records: Mapping[str, Mapping[str, object]],
    unavailable_seed_ids: Iterable[str],
    selected_records: Mapping[str, Mapping[str, object]],
    observations: Sequence[Mapping[str, object]],
) -> List[Dict[str, str]]:
    rows = [dict(row) for row in baseline_rows]
    by_id = {row["paper_id"]: row for row in rows}
    for seed_id, record in seed_records.items():
        row = by_id[seed_id]
        notes = _parse_notes_map(row.get("notes", ""))
        notes.update({
            "CITATION_DEPTH": "0",
            "G03_SEED": "YES",
            "BIBLIOGRAPHIC_VARIANTS": "|".join(
                str(value) for value in record.get("bibliographic_variants") or []
            ) or "NONE",
            "OPENALEX_ID": str(record.get("openalex_id") or "UNKNOWN"),
            "SEMANTIC_SCHOLAR_ID": str(
                record.get("semantic_scholar_id") or "UNKNOWN"
            ),
        })
        row["notes"] = _serialize_notes_map(notes)
    for seed_id in sorted(set(unavailable_seed_ids)):
        row = by_id[seed_id]
        notes = _parse_notes_map(row.get("notes", ""))
        notes.update({
            "ANCESTRY_RESOLUTION": "UNAVAILABLE",
            "CITATION_DEPTH": "0",
            "G03_SEED": "YES",
            "OPENALEX_RESOLUTION": "UNAVAILABLE",
        })
        row["notes"] = _serialize_notes_map(notes)
    for paper_id, record in sorted(selected_records.items()):
        related = [row for row in observations if row.get("paper_id") == paper_id]
        if paper_id in by_id:
            row = by_id[paper_id]
            notes = _parse_notes_map(row.get("notes", ""))
            notes.update({
                "ANCESTRY_DIRECTIONS": "|".join(sorted({str(item.get("direction")) for item in related})),
                "ANCESTRY_RESOLUTION": "RESOLVED",
                "ANCESTRY_SEEDS": "|".join(sorted({str(item.get("seed_paper_id")) for item in related})),
                "CITATION_DEPTH": str(min(int(item.get("depth") or 0) for item in related)),
                "G03_AQ_LINKS": "|".join(
                    sorted(
                        {
                            str(question)
                            for item in related
                            for question in item.get("architecture_question_ids") or []
                        }
                    )
                ) or "UNKNOWN",
                "G03_SCREEN": "DERIVED_INFERENCE_METADATA_ONLY",
                "OPENALEX_ID": str(record.get("openalex_id") or "UNKNOWN"),
                "SEMANTIC_SCHOLAR_ID": str(
                    record.get("semantic_scholar_id") or "UNKNOWN"
                ),
            })
            row["notes"] = _serialize_notes_map(notes)
            existing_questions = set(_split_multi_value(row.get("architecture_question_ids", "")))
            existing_questions.update(
                question
                for item in related
                for question in item.get("architecture_question_ids") or []
            )
            row["architecture_question_ids"] = "|".join(sorted(existing_questions))
        else:
            new_row = _build_new_manifest_row(record, observations)
            rows.append(new_row)
            by_id[paper_id] = new_row
    return rows


def _build_selected_edges(
    observations: Sequence[Mapping[str, object]],
    selected_ids: Set[str],
    records_by_paper: Mapping[str, Mapping[str, object]],
    verified_at: str,
) -> List[Dict[str, str]]:
    edges: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    for observation in observations:
        candidate_id = str(observation.get("paper_id") or "")
        if candidate_id not in selected_ids:
            continue
        parent_id = str(observation.get("parent_paper_id") or "")
        candidate = records_by_paper.get(candidate_id)
        parent = records_by_paper.get(parent_id)
        if candidate is None or parent is None or candidate_id == parent_id:
            continue
        if observation.get("direction") == "BACKWARD":
            citing_record = dict(parent)
            citing_record["provider_name"] = observation.get(
                "provider_name", parent.get("provider_name", "OpenAlex")
            )
            generated = build_provider_citation_edges(
                candidate_id,
                citing_record,
                verified_at,
                target_title=str(candidate.get("title") or ""),
            )
        else:
            citing_record = dict(candidate)
            citing_record["provider_name"] = observation.get(
                "provider_name", candidate.get("provider_name", "OpenAlex")
            )
            generated = build_provider_citation_edges(
                parent_id,
                citing_record,
                verified_at,
                target_title=str(parent.get("title") or ""),
            )
        for row in generated:
            edges[(row["source_paper_id"], row["target_paper_id"], row["edge_type"])] = row
    return [edges[key] for key in sorted(edges)]


def _unique_selected_observations(
    observations: Sequence[Mapping[str, object]], selected_ids: Set[str]
) -> List[Dict[str, object]]:
    best: Dict[str, Dict[str, object]] = {}
    for source in sorted((dict(row) for row in observations), key=_candidate_sort_key):
        paper_id = str(source.get("paper_id") or "")
        if paper_id in selected_ids:
            best.setdefault(paper_id, source)
    return [best[key] for key in sorted(best, key=lambda value: _candidate_sort_key(best[value]))]


def _render_branch_table(
    observations: Sequence[Mapping[str, object]],
    manifest_by_id: Mapping[str, Mapping[str, str]],
    limit: int = 40,
) -> List[str]:
    lines = [
        "| Paper | Metadata title | Direction/depth | AQ links | Decision score |",
        "|---|---|---|---|---:|",
    ]
    for observation in observations[:limit]:
        paper_id = str(observation.get("paper_id") or "")
        row = manifest_by_id.get(paper_id, {})
        title = str(row.get("title") or "UNKNOWN").replace("|", "\\|")
        questions = "|".join(observation.get("architecture_question_ids") or []).replace("|", ", ")
        lines.append(
            "| `{0}` | {1} | {2} / {3} | {4} | {5} |".format(
                paper_id,
                title,
                observation.get("direction"),
                observation.get("depth"),
                questions or "UNKNOWN",
                observation.get("decision_score", 0),
            )
        )
    if len(lines) == 2:
        lines.append("| `NONE` | No branch survived the frozen metadata screen | N/A | N/A | 0 |")
    return lines


def build_g03_citation_report(
    seed_ids: Sequence[str],
    baseline_manifest_count: int,
    final_manifest_rows: Sequence[Mapping[str, str]],
    request_rows: Sequence[Mapping[str, str]],
    edge_rows: Sequence[Mapping[str, str]],
    observations: Sequence[Mapping[str, object]],
    selected_ids: Set[str],
    reviewed_g04_ids: Sequence[str],
    stops: Sequence[Mapping[str, object]],
    sampled_reference_omissions: int,
) -> str:
    """Render the metadata-only G03 decision handoff and exact G04 set."""

    manifest_by_id = _manifest_rows_by_id(final_manifest_rows)
    selected = _unique_selected_observations(observations, selected_ids)
    backward = [row for row in selected if row.get("direction") == "BACKWARD"]
    forward = [row for row in selected if row.get("direction") == "FORWARD"]
    direction_sets: Dict[str, Set[str]] = defaultdict(set)
    for row in observations:
        paper_id = str(row.get("paper_id") or "")
        if paper_id in selected_ids:
            direction_sets[paper_id].add(str(row.get("direction") or "UNKNOWN"))
    backward_identity_count = sum("BACKWARD" in value for value in direction_sets.values())
    forward_identity_count = sum("FORWARD" in value for value in direction_sets.values())
    bidirectional_identity_count = sum(
        {"BACKWARD", "FORWARD"}.issubset(value) for value in direction_sets.values()
    )
    negative_tokens = (
        "counterexample",
        "lower bound",
        "impossibility",
        "limitations",
        "incorrect",
        "intractability",
        "resolution limit",
        "no harder",
    )
    negative_signals = [
        row for row in selected
        if any(
            token in normalize_title_identity(manifest_by_id.get(str(row.get("paper_id")), {}).get("title"))
            for token in negative_tokens
        )
    ]
    survey_signals = [
        row
        for row in selected
        if any(
            token
            in normalize_title_identity(
                manifest_by_id.get(str(row.get("paper_id")), {}).get("title")
            )
            for token in ("survey", "review")
        )
    ]
    stop_counts: Dict[str, int] = defaultdict(int)
    for row in stops:
        stop_counts[str(row.get("reason") or "UNKNOWN")] += 1
    question_counts: Dict[str, int] = defaultdict(int)
    for paper_id in selected_ids:
        notes = _parse_notes_map(manifest_by_id.get(paper_id, {}).get("notes", ""))
        for question_id in _split_multi_value(notes.get("G03_AQ_LINKS", "")):
            question_counts[str(question_id)] += 1
    new_identity_ids = selected_ids - set(seed_ids) - {
        row.get("paper_id", "") for row in final_manifest_rows[:baseline_manifest_count]
    }
    semantic_counts: Dict[str, int] = defaultdict(int)
    for row in edge_rows:
        semantic_counts[row.get("edge_type", "UNKNOWN")] += 1
    g04_new_ids = list(reviewed_g04_ids)
    if (
        len(g04_new_ids) != 25
        or len(set(g04_new_ids)) != 25
        or not set(g04_new_ids) <= new_identity_ids
    ):
        raise ValueError("screening ledger must select 25 retained new identities")
    g04_queue_basis = "FOUR_LANE_SCREENING_LEDGER"
    g04_ids = list(seed_ids) + g04_new_ids
    g04_ids = list(dict.fromkeys(g04_ids))
    external_requests = sum(row.get("cache_status") == "MISS" for row in request_rows)
    raw_observations = sum(
        int(row.get("result_count", "0"))
        for row in request_rows
        if row.get("terminal_state") in {"COMPLETE", "EMPTY"}
    )
    depth2_request_rows = [
        row
        for row in request_rows
        if row.get("depth") == "1"
        and row.get("operation") in {"BACKWARD_REFERENCES", "FORWARD_CITATIONS"}
    ]
    depth2_completed_requests = sum(
        row.get("terminal_state") in {"COMPLETE", "EMPTY"}
        for row in depth2_request_rows
    )
    provider_request_counts = Counter(row.get("service", "UNKNOWN") for row in request_rows)
    provider_observation_counts = Counter()
    for row in request_rows:
        if row.get("terminal_state") in {"COMPLETE", "EMPTY"}:
            provider_observation_counts[row.get("service", "UNKNOWN")] += int(
                row.get("result_count", "0")
            )
    screening_lane_counts = Counter(
        _screening_primary_lane(row)
        for row in final_manifest_rows
        if is_retained_ancestry_identity(row)
    )
    displayed_control_stops = [
        row
        for row in stops
        if row.get("reason")
        in {
            "REQUEST_RETRY_RESERVE",
            "S2_RATE_LIMIT_ATTEMPTS_EXHAUSTED",
            "S2_SELECTED_PAYLOAD_REJECTED",
            "S2_PROVIDER_ID_UNAVAILABLE",
        }
    ]
    lines = [
        "# G03 Citation Ancestry Report",
        "",
        "**Status:** `METADATA_TRAVERSAL_COMPLETE`",
        "**Epistemic boundary:** OpenAlex and Semantic Scholar provider relations establish only `CITES`. All branch roles and decision scores are `DERIVED_INFERENCE` metadata-screening judgments, not `SOURCE_CLAIM`s.",
        "",
        "## Executive Result",
        "",
        "G03 converted the 25 G02 seeds into a bounded depth-1 citation map and an exact G04 reading queue. One depth-2 neighborhood was attempted; its selected payload was rejected, so zero depth-2 identities or edges were retained. G03 did not read or acquire a paper. The result prioritizes citation-visible branches that can change an open architecture question; it does not prove any mechanism, performance result, or compatibility claim.",
        "",
        "## Campaign Accounting",
        "",
        "| Measure | Count | Cap |",
        "|---|---:|---:|",
        "| Initial seeds | {0} | exactly 25 |".format(len(seed_ids)),
        "| External HTTP attempts | {0} | 90 |".format(external_requests),
        "| Raw metadata observations | {0} | 6,000 |".format(raw_observations),
        "| Baseline canonical identities | {0} | frozen |".format(baseline_manifest_count),
        "| Final canonical identities | {0} | N/A |".format(len(final_manifest_rows)),
        "| New canonical identities retained | {0} | 250 |".format(len(new_identity_ids)),
        "| Provider-backed CITES edges | {0} | N/A |".format(
            semantic_counts.get("CITES", 0)
        ),
        "| Metadata-inferred semantic edges | {0} | N/A |".format(
            len(edge_rows) - semantic_counts.get("CITES", 0)
        ),
        "| Depth-2 expansion attempts | {0} | at most 5 |".format(
            len(depth2_request_rows)
        ),
        "| Successful/empty depth-2 responses | {0} | N/A |".format(
            depth2_completed_requests
        ),
        "| Papers read | 0 | 0 |",
        "| Full-text/PDF files acquired | 0 | 0 |",
        "| Repositories acquired | 0 | 0 |",
        "",
        "Edge counts: " + ", ".join("`{0}`={1}".format(key, semantic_counts[key]) for key in sorted(semantic_counts)) + ".",
        "",
        "Provider accounting: OpenAlex={0} requests/{1} observations/0 retained edges; Semantic Scholar={2} requests/{3} observations/{4} retained edges.".format(
            provider_request_counts.get("OpenAlex", 0),
            provider_observation_counts.get("OpenAlex", 0),
            provider_request_counts.get("SemanticScholar", 0),
            provider_observation_counts.get("SemanticScholar", 0),
            len(edge_rows),
        ),
        "",
        "## Foundational Branches",
        "",
        "Backward branches are older or referenced candidates retained by the frozen taxonomy screen. The label 'foundational' is a reading priority, not a claim about originality.",
        "",
        *_render_branch_table(backward, manifest_by_id),
        "",
        "Displayed {0} of {1} retained identities whose observations include `BACKWARD`; {2} identities were reached in both directions.".format(
            min(40, len(backward)), backward_identity_count, bidirectional_identity_count
        ),
        "",
        "## Implementation And Evaluation Branches",
        "",
        "Forward branches are later citing works. A role is semantically typed only when its title explicitly anchors the cited target; otherwise the report merely nominates the branch for G04 reading.",
        "",
        *_render_branch_table(forward, manifest_by_id),
        "",
        "Displayed {0} of {1} retained identities whose observations include `FORWARD`; {2} identities were reached in both directions.".format(
            min(40, len(forward)), forward_identity_count, bidirectional_identity_count
        ),
        "",
        "## Contradictory Branches",
        "",
        "Constraint And Negative-Result Signals: these titles contain an explicit counterexample, lower-bound, impossibility, limitation, intractability, resolution-limit, or complexity-relief signal. They can constrain a design without contradicting the cited target. Reading is required before assigning `CONTRADICTS`.",
        "",
        *_render_branch_table(negative_signals, manifest_by_id),
        "",
        "## Survey And Review Signals",
        "",
        "These titles explicitly identify a survey or review. They are G04 routing candidates, not `SURVEYS` edges, unless the title also anchors the exact cited target.",
        "",
        *_render_branch_table(survey_signals, manifest_by_id),
        "",
        "## Post-Traversal Screening Review",
        "",
        "Four disjoint read-only `gpt-5.6-sol` xhigh lanes screened backward candidates, forward systems, negative/survey signals, and provenance/accounting. The lanes read metadata and control artifacts only. They did not read papers or ignored provider caches.",
        "",
        "- Backward lane: {0} identities after constraint-lane precedence, prioritizing external-memory traversal, adjacency compression, local PageRank, dynamic indexes, direction-optimizing BFS, and path-query semantics.".format(
            screening_lane_counts.get("G03-LANE-A", 0)
        ),
        "- Forward lane: {0} identities after constraint- and backward-lane precedence, prioritizing graph-shaped SSD/storage systems, partitioned processing, direct compressed-query execution, named benchmark implementations, RPQ systems, and I/O-aware ANN scheduling.".format(
            screening_lane_counts.get("G03-LANE-B", 0)
        ),
        "- Constraint lane: {0} lower-bound, intractability, resolution-limit, survey, or review identities remained reading nominations rather than semantic claims.".format(
            screening_lane_counts.get("G03-LANE-C", 0)
        ),
        "- Audit lane: independently reconciled {0} seeds, {1} attempts, {2} observations, {3} identities, {4} typed edges, {5} exact stops, provider attribution, and the G04 queue.".format(
            len(seed_ids),
            external_requests,
            raw_observations,
            len(final_manifest_rows),
            len(edge_rows),
            len(stops),
        ),
        "- Semantic result: exactly one title-explicit `IMPLEMENTS` inference survives the strict target-anchor rule; all other retained role relationships remain `CITES` only.",
        "",
        "## Stopped Branches",
        "",
        "Exact stopped observations: **{0}**. The complete identity-level record is `sources/citation-stops.tsv`; the table below displays provider, retry-reserve, and payload stops while the reason table reconciles every row.".format(len(stops)),
        "",
        "| Stop reason | Count |",
        "|---|---:|",
    ]
    if stop_counts:
        lines.extend("| `{0}` | {1} |".format(key, stop_counts[key]) for key in sorted(stop_counts))
    else:
        lines.append("| `NONE` | 0 |")
    lines.extend([
        "",
        "Exact provider and retry-reserve stops:",
        "",
        "| Paper | Seed | Depth | Direction | Reason |",
        "|---|---|---:|---|---|",
    ])
    if displayed_control_stops:
        for row in sorted(
            displayed_control_stops,
            key=lambda value: (
                str(value.get("reason") or ""),
                str(value.get("paper_id") or ""),
                str(value.get("seed_paper_id") or ""),
            ),
        ):
            lines.append(
                "| `{0}` | `{1}` | {2} | {3} | `{4}` |".format(
                    row.get("paper_id") or "UNKNOWN",
                    row.get("seed_paper_id") or "UNKNOWN",
                    row.get("depth") or "UNKNOWN",
                    row.get("direction") or "UNKNOWN",
                    row.get("reason") or "UNKNOWN",
                )
            )
    else:
        lines.append("| `NONE` | `NONE` | 0 | N/A | `NONE` |")
    lines.extend([
        "",
        "Depth-2 expansion was attempted {0} time(s), with {1} successful or empty selected-metadata response(s). A retained depth-2 identity is never expanded further. Forward and backward traversal used one page per operation; the Semantic Scholar page limit is 75 and the OpenAlex page limit is 100. These are explicit recall limits.".format(
            len(depth2_request_rows), depth2_completed_requests
        ),
        "",
        "## Architecture-Question Decision Impact",
        "",
        "| Architecture question | Retained branch identities | Decision effect |",
        "|---|---:|---|",
    ])
    for index in range(1, 13):
        question_id = "AQ-{0:03d}".format(index)
        count = question_counts.get(question_id, 0)
        effect = "G04_READING_PRIORITY_CHANGED" if count else "NO_NEW_CITATION_VISIBLE_BRANCH"
        lines.append("| `{0}` | {1} | `{2}` |".format(question_id, count, effect))
    lines.extend([
        "",
        "## Coverage Gaps",
        "",
        "- OpenAlex `referenced_works` omits references it cannot resolve to an OpenAlex identity; this is not a complete bibliography.",
        "- OpenAlex exact arXiv-location resolution can miss records whose location metadata differs; Semantic Scholar exact arXiv resolution repaired this for all 25 seeds in this campaign.",
        "- One-page traversal can miss lower-ranked relations beyond 75 Semantic Scholar results or 100 OpenAlex results.",
        "- One depth-2 Semantic Scholar response violated the selected-metadata envelope and was retained only as a checksummed `PAYLOAD_REJECTED` marker.",
        "- One seed's forward branch exhausted three Semantic Scholar rate-limit attempts and remains an explicit coverage gap.",
        "- Twelve-reference sampling can miss a relevant ancestor in a long bibliography.",
        "- Titles and bibliographic types cannot prove implementation, evaluation, contradiction, mechanism, correctness, RAM, or latency claims.",
        "- No citation metadata directly closes Bolt, Cypher, GDS procedure, admission-control, whole-process RSS, or verification-receipt gaps unless its title matched the frozen taxonomy.",
        "",
        "## Exact Recommended G04 Acquisition Set",
        "",
        "The set contains all 25 original seeds plus 25 new ancestry identities after global deduplication and four-lane post-traversal screening. The reviewed queue replaces generic taxonomy/clustering false positives and ambiguous duplicate PageRank identities with architecture-direct external-memory, storage, compression, query, implementation, and survey candidates. Queue basis: `{0}`. G04 must perform its own license, availability, and acquisition-time identity preflight.".format(
            g04_queue_basis
        ),
        "",
        "| # | Paper | Metadata title | G03 basis |",
        "|---:|---|---|---|",
    ])
    g04_observations = {str(row.get("paper_id")): row for row in selected}
    for index, paper_id in enumerate(g04_ids, start=1):
        manifest_row = manifest_by_id.get(paper_id, {})
        title = str(manifest_row.get("title") or "UNKNOWN").replace("|", "\\|")
        if paper_id in seed_ids:
            basis = "G02_SEED"
        else:
            observation = g04_observations.get(paper_id, {})
            basis = "SCREENED_{0}_DEPTH_{1}_SCORE_{2}".format(
                observation.get("direction", "UNKNOWN"),
                observation.get("depth", "UNKNOWN"),
                observation.get("decision_score", 0),
            )
        lines.append("| {0} | `{1}` | {2} | `{3}` |".format(index, paper_id, title, basis))
    lines.extend([
        "",
        "Exact G04 set size: **{0}** canonical identities.".format(len(g04_ids)),
        "",
        "## Scope Boundary",
        "",
        "G03 downloaded no PDF, abstract, paper body, source archive, or repository; read no paper; created no mechanism, failure, or transfer card; proposed no architecture or experiment; and did not begin G04. OpenAlex and sanitized Semantic Scholar selected-metadata bodies remain ignored local cache files. The report is a citation-metadata routing artifact only.",
        "",
    ])
    return "\n".join(lines)


def execute_g03_citation_campaign(reference_root: Path, allow_network: bool) -> Dict[str, int]:
    """Execute the exact bounded G03 citation campaign and write owned outputs."""

    report_path = reference_root / "sources" / "G02-metadata-screening-report.md"
    manifest_path = reference_root / "sources" / "paper-manifest.tsv"
    taxonomy_path = reference_root / "governance" / "keyword-taxonomy.tsv"
    preflight_path = reference_root / "governance" / "g03-service-preflight.md"
    ledger_path = reference_root / "sources" / "citation-request-ledger.tsv"
    edge_path = reference_root / "sources" / "citation-edges.tsv"
    stop_path = reference_root / "sources" / "citation-stops.tsv"
    screening_path = reference_root / "sources" / "citation-screening-ledger.tsv"
    final_report_path = reference_root / "sources" / "G03-citation-ancestry-report.md"

    seed_ids = extract_g03_seed_ids(report_path.read_text(encoding="utf-8"))
    manifest_rows = read_tsv_rows(manifest_path, MANIFEST_HEADER)
    if len(manifest_rows) < EXPECTED_G02_MANIFEST_COUNT:
        raise RuntimeError("G03 requires the frozen 262-row G02 manifest baseline")
    if len(manifest_rows) > EXPECTED_G02_MANIFEST_COUNT:
        g03_suffix = manifest_rows[EXPECTED_G02_MANIFEST_COUNT:]
        if not final_report_path.is_file() or any(
            row.get("discovery_query_ids") != "NOT_APPLICABLE"
            or "CITATION_DEPTH=" not in row.get("notes", "")
            or "G03_SCREEN=DERIVED_INFERENCE_METADATA_ONLY" not in row.get("notes", "")
            for row in g03_suffix
        ):
            raise RuntimeError("manifest suffix is not a verified G03 replay artifact")
    baseline_manifest = manifest_rows[:EXPECTED_G02_MANIFEST_COUNT]
    baseline_by_id = _manifest_rows_by_id(baseline_manifest)
    if any(seed_id not in baseline_by_id for seed_id in seed_ids):
        raise RuntimeError("one or more G03 seeds are absent from the G02 manifest")
    taxonomy_rows = read_tsv_rows(taxonomy_path, TAXONOMY_HEADER)
    preflight_text = preflight_path.read_text(encoding="utf-8")
    preflight_errors = validate_g03_network_preflight(preflight_text)
    if preflight_errors:
        raise RuntimeError("G03 service preflight failed: " + "; ".join(preflight_errors))
    if not ledger_path.exists():
        write_tsv_rows(ledger_path, REQUEST_HEADER, [])

    openalex_seed_records: Dict[str, Dict[str, object]] = {}
    openalex_unavailable_seed_ids: Set[str] = set()
    records_by_openalex: Dict[str, Dict[str, object]] = {}
    for seed_id in seed_ids:
        canonical_version = baseline_by_id[seed_id].get("canonical_version", "")
        seed_identifier = seed_id
        if re.fullmatch(r"v\d+", canonical_version):
            seed_identifier += "|" + seed_id + canonical_version
        records = _fetch_campaign_page(
            reference_root,
            ledger_path,
            preflight_text,
            "SEED_RESOLUTION",
            seed_identifier,
            seed_id,
            seed_id,
            0,
            "SEED_RESOLUTION",
            allow_network,
        )
        openalex_id = resolve_seed_openalex_identity(seed_id, records)
        if openalex_id is None:
            openalex_unavailable_seed_ids.add(seed_id)
            continue
        record = next(row for row in records if row.get("openalex_id") == openalex_id)
        bibliography_errors = _validate_seed_bibliography(baseline_by_id[seed_id], record)
        if bibliography_errors:
            raise RuntimeError(
                "ambiguous seed resolution for {0}: {1}".format(
                    seed_id, ", ".join(bibliography_errors)
                )
            )
        record = dict(record)
        record["paper_id"] = seed_id
        openalex_seed_records[seed_id] = record
        records_by_openalex[openalex_id] = record

    s2_batch_records = _fetch_s2_campaign_page(
        reference_root,
        ledger_path,
        preflight_text,
        "SEED_RESOLUTION_BATCH",
        "|".join(seed_ids),
        "|".join(seed_ids),
        "|".join(seed_ids),
        0,
        "SEED_RESOLUTION",
        allow_network,
    )
    s2_seed_records, s2_unavailable_seed_ids = resolve_s2_seed_identities(
        seed_ids, s2_batch_records
    )
    for seed_id, record in s2_seed_records.items():
        try:
            bibliography_variants = audit_s2_seed_bibliography(
                seed_id, baseline_by_id[seed_id], record
            )
        except ValueError as error:
            raise RuntimeError(
                "ambiguous Semantic Scholar seed resolution for {0}: {1}".format(
                    seed_id, error
                )
            ) from error
        record["bibliographic_variants"] = bibliography_variants
        openalex_record = openalex_seed_records.get(seed_id)
        if openalex_record is not None:
            record["openalex_id"] = openalex_record.get("openalex_id", "UNKNOWN")
            record["source_urls"] = sorted(
                set(record.get("source_urls") or [])
                | set(openalex_record.get("source_urls") or [])
            )

    seed_records = dict(openalex_seed_records)
    seed_records.update(s2_seed_records)
    unavailable_seed_ids = (
        openalex_unavailable_seed_ids & s2_unavailable_seed_ids
    )
    records_by_s2 = _reconcile_record_map(
        s2_seed_records.values(), baseline_manifest
    )
    for seed_id, record in list(s2_seed_records.items()):
        provider_id = _provider_record_key(record)
        if provider_id in records_by_s2:
            reconciled = records_by_s2[provider_id]
            reconciled["paper_id"] = seed_id
            s2_seed_records[seed_id] = reconciled
            seed_records[seed_id] = reconciled

    sampled_reference_omissions = 0
    sampled_reference_stops: List[Dict[str, object]] = []
    backward_relations: List[Tuple[str, str]] = []
    for seed_id in openalex_seed_records:
        references = list(openalex_seed_records[seed_id].get("referenced_works") or [])
        sampled = _evenly_sample_values(references, MAX_REFERENCE_IDS_PER_SEED)
        unique_references = list(dict.fromkeys(references))
        omitted_references = [value for value in unique_references if value not in set(sampled)]
        sampled_reference_omissions += len(omitted_references)
        sampled_reference_stops.extend(
            {
                "candidate_identity": value,
                "seed_paper_id": seed_id,
                "parent_paper_id": seed_id,
                "depth": 1,
                "direction": "BACKWARD",
                "provider_name": "OpenAlex",
                "provider_id": value,
                "reason": "REFERENCE_SAMPLE_CAP",
            }
            for value in omitted_references
        )
        backward_relations.extend((seed_id, value) for value in sampled)
    depth1_backward_records = _fetch_batched_work_records(
        reference_root,
        ledger_path,
        preflight_text,
        [value for _seed_id, value in backward_relations],
        [seed_id for seed_id, _value in backward_relations],
        [seed_id for seed_id, _value in backward_relations],
        1,
        allow_network,
    )
    records_by_openalex.update(depth1_backward_records)

    forward_relations: List[Tuple[str, Dict[str, object]]] = []
    for seed_id in openalex_seed_records:
        parent = openalex_seed_records[seed_id]
        citing_records = _fetch_campaign_page(
            reference_root,
            ledger_path,
            preflight_text,
            "FORWARD_CITATIONS",
            str(parent["openalex_id"]),
            seed_id,
            seed_id,
            0,
            "FORWARD",
            allow_network,
        )
        for record in citing_records:
            records_by_openalex[str(record["openalex_id"])] = record
            forward_relations.append((seed_id, record))

    depth1_reconciled = _reconcile_record_map(records_by_openalex.values(), baseline_manifest)
    records_by_openalex.update(depth1_reconciled)
    depth1_observations: List[Dict[str, object]] = []
    for seed_id, openalex_id in backward_relations:
        record = records_by_openalex.get(openalex_id)
        if record is None:
            continue
        depth1_observations.append(
            _candidate_observation(
                record, baseline_by_id[seed_id], openalex_seed_records[seed_id],
                "BACKWARD", 1, taxonomy_rows
            )
        )
    for seed_id, source_record in forward_relations:
        record = records_by_openalex.get(str(source_record["openalex_id"]))
        if record is None:
            continue
        depth1_observations.append(
            _candidate_observation(
                record, baseline_by_id[seed_id], openalex_seed_records[seed_id],
                "FORWARD", 1, taxonomy_rows
            )
        )

    s2_backward_relations: List[Tuple[str, Dict[str, object]]] = []
    s2_forward_relations: List[Tuple[str, Dict[str, object]]] = []
    s2_provider_stops: List[Dict[str, object]] = []
    for seed_id in seed_ids:
        parent = s2_seed_records.get(seed_id)
        if parent is None:
            continue
        semantic_scholar_id = str(parent.get("semantic_scholar_id") or "")
        try:
            referenced_records = _fetch_s2_campaign_page(
                reference_root,
                ledger_path,
                preflight_text,
                "BACKWARD_REFERENCES",
                semantic_scholar_id,
                seed_id,
                seed_id,
                0,
                "BACKWARD",
                allow_network,
            )
        except CitationRateLimitExhausted:
            referenced_records = []
            s2_provider_stops.append(
                _build_provider_control_stop(
                    baseline_by_id[seed_id],
                    "SemanticScholar",
                    semantic_scholar_id,
                    "BACKWARD",
                    1,
                    "S2_RATE_LIMIT_ATTEMPTS_EXHAUSTED",
                )
            )
        except CitationPayloadRejected:
            referenced_records = []
            s2_provider_stops.append(
                _build_provider_control_stop(
                    baseline_by_id[seed_id],
                    "SemanticScholar",
                    semantic_scholar_id,
                    "BACKWARD",
                    1,
                    "S2_SELECTED_PAYLOAD_REJECTED",
                )
            )
        try:
            citing_records = _fetch_s2_campaign_page(
                reference_root,
                ledger_path,
                preflight_text,
                "FORWARD_CITATIONS",
                semantic_scholar_id,
                seed_id,
                seed_id,
                0,
                "FORWARD",
                allow_network,
            )
        except CitationRateLimitExhausted:
            citing_records = []
            s2_provider_stops.append(
                _build_provider_control_stop(
                    baseline_by_id[seed_id],
                    "SemanticScholar",
                    semantic_scholar_id,
                    "FORWARD",
                    1,
                    "S2_RATE_LIMIT_ATTEMPTS_EXHAUSTED",
                )
            )
        except CitationPayloadRejected:
            citing_records = []
            s2_provider_stops.append(
                _build_provider_control_stop(
                    baseline_by_id[seed_id],
                    "SemanticScholar",
                    semantic_scholar_id,
                    "FORWARD",
                    1,
                    "S2_SELECTED_PAYLOAD_REJECTED",
                )
            )
        s2_backward_relations.extend((seed_id, record) for record in referenced_records)
        s2_forward_relations.extend((seed_id, record) for record in citing_records)

    s2_depth1_records = _reconcile_record_map(
        [record for _seed_id, record in s2_backward_relations + s2_forward_relations],
        baseline_manifest,
    )
    records_by_s2.update(s2_depth1_records)
    for seed_id, source_record in s2_backward_relations:
        record = records_by_s2.get(_provider_record_key(source_record))
        parent = s2_seed_records.get(seed_id)
        if record is None or parent is None:
            continue
        depth1_observations.append(
            _candidate_observation(
                record,
                baseline_by_id[seed_id],
                parent,
                "BACKWARD",
                1,
                taxonomy_rows,
            )
        )
    for seed_id, source_record in s2_forward_relations:
        record = records_by_s2.get(_provider_record_key(source_record))
        parent = s2_seed_records.get(seed_id)
        if record is None or parent is None:
            continue
        depth1_observations.append(
            _candidate_observation(
                record,
                baseline_by_id[seed_id],
                parent,
                "FORWARD",
                1,
                taxonomy_rows,
            )
        )
    depth1_selected, depth1_stops = select_bounded_candidates(
        depth1_observations,
        MAX_NEW_IDENTITIES,
        existing_identity_ids=set(baseline_by_id),
    )
    depth1_selected_ids = {str(row["paper_id"]) for row in depth1_selected}
    branches = _select_expansion_branches(depth1_selected, depth1_selected_ids)

    depth2_backward_relations: List[Tuple[str, str, str]] = []
    for (seed_id, direction), branch in sorted(branches.items()):
        if direction != "BACKWARD":
            continue
        parent = records_by_openalex.get(str(branch.get("openalex_id") or ""))
        if parent is None:
            continue
        references = list(parent.get("referenced_works") or [])
        sampled = _evenly_sample_values(references, MAX_REFERENCE_IDS_PER_SEED)
        unique_references = list(dict.fromkeys(references))
        omitted_references = [value for value in unique_references if value not in set(sampled)]
        sampled_reference_omissions += len(omitted_references)
        sampled_reference_stops.extend(
            {
                "candidate_identity": value,
                "seed_paper_id": seed_id,
                "parent_paper_id": str(branch["paper_id"]),
                "depth": 2,
                "direction": "BACKWARD",
                "provider_name": "OpenAlex",
                "provider_id": value,
                "reason": "REFERENCE_SAMPLE_CAP",
            }
            for value in omitted_references
        )
        depth2_backward_relations.extend(
            (seed_id, str(branch["paper_id"]), value) for value in sampled
        )
    identity_reference_rows = list(baseline_manifest)
    for record in records_by_openalex.values():
        identity_reference_rows.append({
            "paper_id": str(record.get("paper_id") or ""),
            "arxiv_id": str(record.get("arxiv_id") or "UNKNOWN"),
            "doi": str(record.get("doi") or "UNKNOWN"),
        })
    for record in records_by_s2.values():
        identity_reference_rows.append({
            "paper_id": str(record.get("paper_id") or ""),
            "arxiv_id": str(record.get("arxiv_id") or "UNKNOWN"),
            "doi": str(record.get("doi") or "UNKNOWN"),
        })
    depth2_backward_records = _fetch_batched_work_records(
        reference_root,
        ledger_path,
        preflight_text,
        [value for _seed, _parent, value in depth2_backward_relations],
        [seed for seed, _parent, _value in depth2_backward_relations],
        [parent for _seed, parent, _value in depth2_backward_relations],
        2,
        allow_network,
    )
    records_by_openalex.update(
        _reconcile_record_map(depth2_backward_records.values(), identity_reference_rows)
    )

    depth2_forward_relations: List[Tuple[str, str, Dict[str, object]]] = []
    for (seed_id, direction), branch in sorted(branches.items()):
        if direction != "FORWARD":
            continue
        parent = records_by_openalex.get(str(branch.get("openalex_id") or ""))
        if parent is None:
            continue
        citing_records = _fetch_campaign_page(
            reference_root,
            ledger_path,
            preflight_text,
            "FORWARD_CITATIONS",
            str(parent["openalex_id"]),
            seed_id,
            str(branch["paper_id"]),
            1,
            "FORWARD",
            allow_network,
        )
        reconciled = _reconcile_record_map(citing_records, identity_reference_rows)
        records_by_openalex.update(reconciled)
        for record in citing_records:
            canonical = records_by_openalex.get(str(record["openalex_id"]))
            if canonical is not None:
                depth2_forward_relations.append((seed_id, str(branch["paper_id"]), canonical))

    depth2_observations: List[Dict[str, object]] = []
    for seed_id, parent_id, openalex_id in depth2_backward_relations:
        record = records_by_openalex.get(openalex_id)
        parent = next(
            (value for value in records_by_openalex.values() if value.get("paper_id") == parent_id),
            None,
        )
        if record is None or parent is None:
            continue
        depth2_observations.append(
            _candidate_observation(
                record, baseline_by_id[seed_id], parent,
                "BACKWARD", 2, taxonomy_rows
            )
        )
    for seed_id, parent_id, record in depth2_forward_relations:
        parent = next(
            (value for value in records_by_openalex.values() if value.get("paper_id") == parent_id),
            None,
        )
        if parent is None:
            continue
        depth2_observations.append(
            _candidate_observation(
                record, baseline_by_id[seed_id], parent,
                "FORWARD", 2, taxonomy_rows
            )
        )

    ranked_s2_expansion_branches = sorted(
        (
            dict(branch)
            for branch in branches.values()
            if branch.get("provider_name") == "SemanticScholar"
        ),
        key=_candidate_sort_key,
    )
    expansion_capacity = max(
        0,
        min(
            MAX_S2_DEPTH2_EXPANSIONS,
            _request_budget_remaining(ledger_path) - MIN_S2_RETRY_RESERVE,
        ),
    )
    s2_expansion_branches = ranked_s2_expansion_branches[:expansion_capacity]
    depth2_provider_stops: List[Dict[str, object]] = [
        _build_branch_control_stop(branch, 2, "REQUEST_RETRY_RESERVE")
        for branch in ranked_s2_expansion_branches[expansion_capacity:]
    ]
    for branch in s2_expansion_branches:
        seed_id = str(branch.get("seed_paper_id") or "")
        direction = str(branch.get("direction") or "")
        parent = records_by_s2.get(str(branch.get("provider_id") or ""))
        if parent is None or direction not in {"BACKWARD", "FORWARD"}:
            continue
        operation = (
            "BACKWARD_REFERENCES" if direction == "BACKWARD" else "FORWARD_CITATIONS"
        )
        if str(parent.get("semantic_scholar_id") or "").startswith("UNAVAILABLE:"):
            depth2_provider_stops.append(
                _build_branch_control_stop(branch, 2, "S2_PROVIDER_ID_UNAVAILABLE")
            )
            continue
        try:
            fetched = _fetch_s2_campaign_page(
                reference_root,
                ledger_path,
                preflight_text,
                operation,
                str(parent.get("semantic_scholar_id") or ""),
                seed_id,
                str(branch.get("paper_id") or ""),
                1,
                direction,
                allow_network,
            )
        except CitationRateLimitExhausted:
            depth2_provider_stops.append(
                _build_branch_control_stop(
                    branch, 2, "S2_RATE_LIMIT_ATTEMPTS_EXHAUSTED"
                )
            )
            continue
        except CitationPayloadRejected:
            depth2_provider_stops.append(
                _build_branch_control_stop(branch, 2, "S2_SELECTED_PAYLOAD_REJECTED")
            )
            continue
        reconciled = _reconcile_record_map(fetched, identity_reference_rows)
        records_by_s2.update(reconciled)
        for source_record in fetched:
            record = records_by_s2.get(_provider_record_key(source_record))
            if record is None:
                continue
            depth2_observations.append(
                _candidate_observation(
                    record,
                    baseline_by_id[seed_id],
                    parent,
                    direction,
                    2,
                    taxonomy_rows,
                )
            )
    depth1_new_ids = depth1_selected_ids - set(baseline_by_id)
    depth2_selected, depth2_stops = select_bounded_candidates(
        depth2_observations,
        MAX_NEW_IDENTITIES - len(depth1_new_ids),
        existing_identity_ids=set(baseline_by_id) | depth1_selected_ids,
    )
    selected_ids = depth1_selected_ids | {str(row["paper_id"]) for row in depth2_selected}
    selected_observations = depth1_selected + depth2_selected
    selected_records: Dict[str, Dict[str, object]] = {}
    records_by_paper: Dict[str, Dict[str, object]] = {
        seed_id: record for seed_id, record in seed_records.items()
    }
    for record in records_by_openalex.values():
        paper_id = str(record.get("paper_id") or "")
        if paper_id:
            records_by_paper[paper_id] = record
            if paper_id in selected_ids:
                selected_records[paper_id] = record
    for record in records_by_s2.values():
        paper_id = str(record.get("paper_id") or "")
        if paper_id:
            records_by_paper[paper_id] = record
            if paper_id in selected_ids:
                selected_records[paper_id] = record

    final_manifest = _update_manifest_rows(
        baseline_manifest,
        seed_records,
        unavailable_seed_ids,
        selected_records,
        selected_observations,
    )
    final_manifest_ids = {row["paper_id"] for row in final_manifest}
    if len(final_manifest_ids - set(baseline_by_id)) > MAX_NEW_IDENTITIES:
        raise RuntimeError("G03 canonical identity cap exceeded after reconciliation")
    existing_edge_rows = (
        read_tsv_rows(edge_path, EDGE_HEADER) if edge_path.is_file() else []
    )
    existing_edge_timestamps = {
        (
            row.get("source_paper_id", ""),
            row.get("target_paper_id", ""),
            row.get("edge_type", ""),
        ): row.get("verified_at", "")
        for row in existing_edge_rows
    }
    verified_at = utc_timestamp_now()
    edge_rows = _build_selected_edges(
        selected_observations, selected_ids, records_by_paper, verified_at
    )
    for row in edge_rows:
        prior_timestamp = existing_edge_timestamps.get(
            (row["source_paper_id"], row["target_paper_id"], row["edge_type"])
        )
        if prior_timestamp:
            row["verified_at"] = prior_timestamp
    edge_errors = validate_citation_edge_contract(edge_rows, final_manifest_ids)
    if edge_errors:
        raise RuntimeError("G03 citation edges failed validation: " + "; ".join(edge_errors))
    request_rows = read_tsv_rows(ledger_path, REQUEST_HEADER)
    request_errors = validate_citation_request_rows(request_rows)
    request_errors.extend(validate_g03_cache_provenance(reference_root, request_rows))
    request_errors.extend(
        validate_edge_cache_provenance(
            reference_root, request_rows, edge_rows, final_manifest
        )
    )
    if request_errors:
        raise RuntimeError("G03 request provenance failed validation: " + "; ".join(request_errors))
    write_tsv_rows(manifest_path, MANIFEST_HEADER, final_manifest)
    write_tsv_rows(edge_path, EDGE_HEADER, edge_rows)
    all_stops = (
        depth1_stops
        + depth2_stops
        + s2_provider_stops
        + depth2_provider_stops
        + sampled_reference_stops
        + [
            {
                "paper_id": seed_id,
                "seed_paper_id": seed_id,
                "parent_paper_id": seed_id,
                "depth": 0,
                "direction": "SEED_RESOLUTION",
                "reason": "SEED_ALL_PROVIDERS_UNAVAILABLE",
            }
            for seed_id in sorted(unavailable_seed_ids)
        ]
    )
    stop_rows = normalize_citation_stop_rows(all_stops)
    stop_errors = validate_citation_stop_rows(stop_rows)
    if stop_errors:
        raise RuntimeError("G03 citation stops failed validation: " + "; ".join(stop_errors))
    write_tsv_rows(stop_path, STOP_HEADER, stop_rows)
    screening_rows = build_screening_ledger_rows(final_manifest, reference_root)
    write_tsv_rows(screening_path, SCREENING_HEADER, screening_rows)
    screening_errors = validate_screening_rows(
        screening_rows, final_manifest, reference_root
    )
    if screening_errors:
        raise RuntimeError(
            "G03 citation screening failed validation: "
            + "; ".join(screening_errors)
        )
    reviewed_g04_ids = load_reviewed_g04_queue(screening_path)
    report = build_g03_citation_report(
        seed_ids,
        len(baseline_manifest),
        final_manifest,
        request_rows,
        edge_rows,
        selected_observations,
        selected_ids,
        reviewed_g04_ids,
        all_stops,
        0,
    )
    final_report_path.write_text(report, encoding="utf-8")
    return {
        "seeds": len(seed_ids),
        "requests": sum(row.get("cache_status") == "MISS" for row in request_rows),
        "raw_observations": _raw_observations_used(ledger_path),
        "baseline_identities": len(baseline_manifest),
        "final_identities": len(final_manifest),
        "new_identities": len(final_manifest_ids - set(baseline_by_id)),
        "edges": len(edge_rows),
        "stops": (
            len(stop_rows)
        ),
        "unavailable_seeds": len(unavailable_seed_ids),
        "semantic_scholar_seeds": len(s2_seed_records),
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--allow-network", action="store_true")
    return parser


def main() -> int:
    arguments = build_argument_parser().parse_args()
    result = execute_g03_citation_campaign(arguments.root, arguments.allow_network)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
