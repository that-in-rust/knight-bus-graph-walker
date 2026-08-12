#!/usr/bin/env python3
"""Bounded G04 full-text acquisition and deterministic parsing pipeline."""

from __future__ import annotations

import argparse
import csv
import email.utils
import hashlib
import io
import json
import re
import time
import unicodedata
import urllib.parse
import xml.etree.ElementTree as ElementTree
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Mapping, MutableMapping, Sequence, Tuple

import pypdf
import requests
from pypdf import PdfReader


EXPECTED_QUEUE_SIZE = 50
EXPECTED_SEED_SIZE = 25
EXPECTED_ACQUIRE_SIZE = 25
MAXIMUM_HTTP_ATTEMPTS = 220
MAXIMUM_PAPER_ATTEMPTS = 5
MAXIMUM_RETRY_ATTEMPTS = 3
MAXIMUM_PDF_BYTES = 100 * 1024 * 1024
MAXIMUM_TOTAL_PDF_BYTES = 5 * 1024 * 1024 * 1024
PINNED_PARSER_VERSION = "6.14.2"
PDF_PAGE_SEPARATOR = "\n\f\n"
MAXIMUM_METADATA_BYTES = 16 * 1024 * 1024
MAXIMUM_REDIRECTS = 5
CONNECT_TIMEOUT_SECONDS = 20.0
READ_TIMEOUT_SECONDS = 120.0
G04_USER_AGENT = (
    "KnightBusArxivPatternFoundry/0.1 "
    "(+https://github.com/amuldotexe/knight-bus-graph-walker)"
)
ARXIV_POLICY_URL = "https://info.arxiv.org/help/api/tou.html"
OPENALEX_POLICY_URL = "https://developers.openalex.org/api-reference/authentication"
PUBLISHER_POLICY_URL = "SOURCE_SPECIFIC_PUBLIC_ENDPOINT"

DOWNLOAD_LEDGER_HEADER = (
    "request_id\tgoal_id\tqueue_rank\tpaper_id\tsource_service\tretrieval_uri\t"
    "accessed_at_utc\tresponse_status\tmedia_type\tcontent_length_bytes\t"
    "source_checksum\tlocal_path\tlicense_uri\tlicense_state\t"
    "acquisition_status\tattempt_count\tretry_events\trate_limit_events\t"
    "policy_url\tpolicy_checked_date\tcache_status\ttrace_path\ttrace_checksum\t"
    "parser_name\tparser_version\tparser_options\tpage_count\textracted_path\t"
    "extracted_checksum\tparse_status\tterminal_reason"
)

DOWNLOAD_LEDGER_FIELDS = tuple(DOWNLOAD_LEDGER_HEADER.split("\t"))
ALLOWED_SOURCE_SERVICES = {"ARXIV", "PUBLISHER", "NONE"}
ALLOWED_LICENSE_STATES = {
    "LICENSE_PERMISSIVE_VERIFIED",
    "LICENSE_RESTRICTED_OR_CONDITIONAL",
    "LICENSE_UNKNOWN",
    "LICENSE_UNAVAILABLE",
}
ALLOWED_ACQUISITION_STATUSES = {
    "ACQUIRED",
    "UNAVAILABLE",
    "LICENSE_BLOCKED",
    "RATE_LIMITED",
    "AUTHORIZATION_FAILED",
    "NOT_FOUND",
    "PAYLOAD_REJECTED",
    "FAILED",
    "SERVICE_STOPPED",
}
ALLOWED_PARSE_STATUSES = {"PARSED", "PARSE_FAILED", "NOT_APPLICABLE"}
ALLOWED_CACHE_STATUSES = {"MISS", "HIT", "NOT_APPLICABLE"}
CHECKSUM_PATTERN = re.compile(r"[A-F0-9]{64}")
PAPER_ID_PATTERN = re.compile(r"PAPER-[A-Za-z0-9][A-Za-z0-9.-]*")
MODERN_ARXIV_PATTERN = re.compile(r"\d{4}\.\d{4,5}")
APPROVED_PUBLISHER_HOSTS_BY_DOI_PREFIX = {
    "10.1007/": {"link.springer.com"},
    "10.1080/": {
        "internetmathematicsjournal.com",
        "www.internetmathematicsjournal.com",
        "www.tandfonline.com",
    },
    "10.1109/": {"ieeexplore.ieee.org"},
    "10.1137/": {"epubs.siam.org"},
    "10.1145/": {"dl.acm.org"},
    "10.3233/": {"content.iospress.com"},
    "10.4230/": {"drops.dagstuhl.de"},
}
AUTHORIZED_METADATA_HOSTS_BY_CACHE = {
    "arxiv-exact-identities": {"export.arxiv.org"},
    "openalex-exact-dois": {"api.openalex.org"},
}


class RetryableRequestError(RuntimeError):
    """Represent one transport or retryable HTTP failure."""


class ServiceStopError(RuntimeError):
    """Represent a non-retryable service-wide stop condition."""


def normalize_title_identity(value: object) -> str:
    """Normalize a title for exact conservative comparison."""

    normalized = unicodedata.normalize("NFKC", str(value or "")).casefold()
    normalized = "".join(
        character if character.isalnum() else " " for character in normalized
    )
    return " ".join(normalized.split())


def read_tsv_rows_exact(path: Path) -> List[Dict[str, str]]:
    """Read one UTF-8 TSV into ordered row dictionaries."""

    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def extract_markdown_table_ids(text: str, heading: str) -> List[str]:
    """Extract paper IDs from one ranked Markdown table."""

    marker = "## " + heading
    if marker not in text:
        raise ValueError("missing Markdown section: " + heading)
    section = text.split(marker, 1)[1].split("\n## ", 1)[0]
    return re.findall(
        r"^\|\s*\d+\s*\|\s*`(PAPER-[^`]+)`\s*\|",
        section,
        re.MULTILINE,
    )


def derive_exact_queue_records(reference_root: Path) -> List[Dict[str, object]]:
    """Derive and cross-check the exact ordered G04 queue."""

    g02_report = (
        reference_root / "sources" / "G02-metadata-screening-report.md"
    ).read_text(encoding="utf-8")
    g03_report = (
        reference_root / "sources" / "G03-citation-ancestry-report.md"
    ).read_text(encoding="utf-8")
    screening_rows = read_tsv_rows_exact(
        reference_root / "sources" / "citation-screening-ledger.tsv"
    )
    manifest_rows = read_tsv_rows_exact(
        reference_root / "sources" / "paper-manifest.tsv"
    )

    seed_ids = extract_markdown_table_ids(g02_report, "Recommended G03 Seed Set")
    report_ids = extract_markdown_table_ids(
        g03_report, "Exact Recommended G04 Acquisition Set"
    )
    acquire_rows = [
        row for row in screening_rows if row.get("disposition") == "ACQUIRE"
    ]
    try:
        acquire_rows.sort(key=lambda row: int(row["queue_rank"]))
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("G03 ACQUIRE queue contains an invalid rank") from error
    acquire_ranks = [int(row["queue_rank"]) for row in acquire_rows]
    acquire_ids = [row["candidate_paper_id"] for row in acquire_rows]

    if len(seed_ids) != EXPECTED_SEED_SIZE or len(set(seed_ids)) != EXPECTED_SEED_SIZE:
        raise ValueError("G04 requires exactly 25 unique ordered G02 seeds")
    if acquire_ranks != list(range(1, EXPECTED_ACQUIRE_SIZE + 1)):
        raise ValueError("G04 requires contiguous G03 ACQUIRE ranks 1 through 25")
    if len(report_ids) != EXPECTED_QUEUE_SIZE:
        raise ValueError("G04 report queue must contain exactly 50 identities")
    if len(set(report_ids)) != EXPECTED_QUEUE_SIZE:
        raise ValueError("G04 queue must contain 50 unique identities; duplicate found")
    expected_ids = seed_ids + acquire_ids
    if report_ids != expected_ids:
        raise ValueError("G04 report queue differs from seeds plus ACQUIRE ranks")

    manifest_by_id = {row["paper_id"]: row for row in manifest_rows}
    if len(manifest_by_id) != len(manifest_rows):
        raise ValueError("paper manifest contains duplicate canonical identities")
    missing_ids = sorted(set(report_ids) - set(manifest_by_id))
    if missing_ids:
        raise ValueError("G04 queue identities missing from manifest: " + ",".join(missing_ids))

    queue: List[Dict[str, object]] = []
    for index, paper_id in enumerate(report_ids, start=1):
        manifest = manifest_by_id[paper_id]
        queue.append(
            {
                "queue_rank": index,
                "paper_id": paper_id,
                "basis": "G02_SEED" if index <= EXPECTED_SEED_SIZE else "G03_ACQUIRE",
                "title": manifest["title"],
                "arxiv_id": manifest["arxiv_id"],
                "doi": manifest["doi"],
                "pdf_url": manifest["pdf_url"],
                "license_uri": manifest["license_uri"],
                "canonical_version": manifest["canonical_version"],
            }
        )
    return queue


def build_canonical_paper_filename(paper_id: str) -> str:
    """Build the only allowed PDF filename for an identity."""

    if PAPER_ID_PATTERN.fullmatch(paper_id) is None:
        raise ValueError("invalid canonical paper ID for filename")
    return paper_id + ".pdf"


def validate_safe_local_path(root: Path, relative_path: str) -> Path:
    """Resolve a relative artifact path without allowing escape."""

    if not relative_path or "%" in relative_path or "\\" in relative_path:
        raise ValueError("unsafe encoded or backslash path")
    if any(ord(character) < 32 for character in relative_path):
        raise ValueError("unsafe control byte in path")
    candidate_relative = Path(relative_path)
    if candidate_relative.is_absolute() or ".." in candidate_relative.parts:
        raise ValueError("path escapes declared root")
    resolved_root = root.resolve()
    current = resolved_root
    for part in candidate_relative.parts:
        current = current / part
        if current.exists() and current.is_symlink():
            raise ValueError("symlink path component is forbidden")
    resolved = (resolved_root / candidate_relative).resolve()
    if not resolved.is_relative_to(resolved_root):
        raise ValueError("path escapes declared root")
    return resolved


def classify_license_state_uri(license_uri: str, acquired: bool) -> str:
    """Classify a discovered content-license URI conservatively."""

    normalized = str(license_uri or "").strip().casefold()
    if not acquired:
        return "LICENSE_UNAVAILABLE"
    if normalized in {"", "unknown", "not_discovered", "not_available"}:
        return "LICENSE_UNKNOWN"
    permissive_markers = (
        "creativecommons.org/publicdomain/zero/",
        "creativecommons.org/licenses/by/",
        "creativecommons.org/licenses/by-sa/",
    )
    if any(marker in normalized for marker in permissive_markers):
        return "LICENSE_PERMISSIVE_VERIFIED"
    return "LICENSE_RESTRICTED_OR_CONDITIONAL"


def validate_pdf_payload_bytes(payload: bytes, media_type: str, maximum_bytes: int) -> int:
    """Validate one bounded PDF payload and return its page count."""

    if not payload:
        raise ValueError("empty PDF payload")
    if len(payload) > maximum_bytes:
        raise ValueError("PDF payload exceeds byte cap")
    lowered_type = str(media_type or "").casefold()
    if "html" in lowered_type or payload.lstrip().startswith((b"<html", b"<!DOCTYPE")):
        raise ValueError("HTML response is not a PDF")
    if b"%PDF-" not in payload[:1024]:
        raise ValueError("missing PDF signature")
    if b"%%EOF" not in payload[-4096:]:
        raise ValueError("truncated PDF payload has no EOF marker")
    try:
        reader = PdfReader(io.BytesIO(payload), strict=True)
        if reader.is_encrypted:
            try:
                unlocked = reader.decrypt("")
            except Exception as error:
                raise ValueError("encrypted PDF cannot be parsed") from error
            if not unlocked:
                raise ValueError("encrypted PDF cannot be parsed")
        page_count = len(reader.pages)
    except ValueError:
        raise
    except Exception as error:
        raise ValueError("malformed PDF payload") from error
    if page_count < 1:
        raise ValueError("PDF contains no pages")
    return page_count


def normalize_extracted_page_text(text: object) -> str:
    """Normalize only mechanical line-ending and whitespace differences."""

    normalized = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    normalized = normalized.replace("\x00", "")
    lines = [line.rstrip(" \t") for line in normalized.split("\n")]
    return "\n".join(lines).rstrip("\n") + "\n"


def extract_pdf_text_deterministic(pdf_path: Path, output_path: Path) -> Dict[str, object]:
    """Extract page-ordered text with pinned deterministic formatting."""

    if pypdf.__version__ != PINNED_PARSER_VERSION:
        raise RuntimeError(
            "pypdf version mismatch: expected {0}, got {1}".format(
                PINNED_PARSER_VERSION, pypdf.__version__
            )
        )
    payload = pdf_path.read_bytes()
    page_count = validate_pdf_payload_bytes(payload, "application/pdf", MAXIMUM_PDF_BYTES)
    reader = PdfReader(io.BytesIO(payload), strict=True)
    pages = []
    for page in reader.pages:
        page_text = "" if "/Contents" not in page else page.extract_text(extraction_mode="layout")
        pages.append(normalize_extracted_page_text(page_text))
    output_bytes = PDF_PAGE_SEPARATOR.join(pages).encode("utf-8")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(output_path.suffix + ".part")
    temporary_path.write_bytes(output_bytes)
    temporary_path.replace(output_path)
    return {
        "parser_name": "pypdf",
        "parser_version": pypdf.__version__,
        "parser_options": "layout;page-separator=LF-FF-LF",
        "page_count": page_count,
        "extracted_checksum": hashlib.sha256(output_bytes).hexdigest().upper(),
    }


def verify_cached_artifact_checksums(
    pdf_path: Path,
    expected_pdf_checksum: str,
    text_path: Path,
    expected_text_checksum: str,
) -> bool:
    """Verify both local artifact checksums before cache reuse."""

    if not pdf_path.is_file() or not text_path.is_file():
        raise ValueError("cached artifact is missing")
    actual_pdf = hashlib.sha256(pdf_path.read_bytes()).hexdigest().upper()
    actual_text = hashlib.sha256(text_path.read_bytes()).hexdigest().upper()
    if actual_pdf != expected_pdf_checksum.upper():
        raise ValueError("PDF cache checksum mismatch")
    if actual_text != expected_text_checksum.upper():
        raise ValueError("text cache checksum mismatch")
    return True


def execute_retry_operation_bounded(
    operation: Callable[[int], Tuple[int, bytes, Dict[str, str]]],
    *,
    sleep_function: Callable[[float], None] = time.sleep,
    maximum_attempts: int = MAXIMUM_RETRY_ATTEMPTS,
) -> Tuple[int, bytes, Dict[str, str]]:
    """Execute one retry chain without crossing its frozen bound."""

    if maximum_attempts < 1 or maximum_attempts > MAXIMUM_RETRY_ATTEMPTS:
        raise ValueError("retry maximum must be between one and three")
    last_error: RetryableRequestError | None = None
    for attempt in range(1, maximum_attempts + 1):
        try:
            return operation(attempt)
        except ServiceStopError:
            raise
        except RetryableRequestError as error:
            last_error = error
            if attempt == maximum_attempts:
                break
            sleep_function(3.1 * (2 ** (attempt - 1)))
    if last_error is None:
        raise RuntimeError("retry operation ended without result")
    raise last_error


def build_arxiv_metadata_request(arxiv_ids: Sequence[str]) -> Dict[str, object]:
    """Compile one exact arXiv ID-list metadata request."""

    identities = [str(identifier).strip() for identifier in arxiv_ids]
    if not identities or len(identities) > EXPECTED_QUEUE_SIZE:
        raise ValueError("arXiv request requires 1-50 exact identities")
    if any(MODERN_ARXIV_PATTERN.fullmatch(identifier) is None for identifier in identities):
        raise ValueError("arXiv request contains an invalid modern identity")
    parameters = {"id_list": ",".join(identities), "max_results": str(len(identities))}
    return {
        "service": "ARXIV",
        "url": "https://export.arxiv.org/api/query?" + urllib.parse.urlencode(parameters),
        "parameters": parameters,
    }


def build_openalex_location_request(dois: Sequence[str]) -> Dict[str, object]:
    """Compile one exact DOI location-metadata request."""

    normalized_dois: List[str] = []
    for value in dois:
        normalized = str(value or "").strip().casefold()
        normalized = re.sub(r"^(?:doi:\s*|https?://(?:dx\.)?doi\.org/)", "", normalized)
        if not normalized.startswith("10.") or "/" not in normalized:
            raise ValueError("OpenAlex request contains an invalid DOI")
        normalized_dois.append(normalized)
    if not normalized_dois or len(normalized_dois) > EXPECTED_QUEUE_SIZE:
        raise ValueError("OpenAlex request requires 1-50 exact DOI values")
    parameters = {
        "filter": "doi:" + "|".join("https://doi.org/" + doi for doi in normalized_dois),
        "per_page": "100",
        "select": "id,doi,title,authorships,publication_date,best_oa_location,locations",
    }
    return {
        "service": "OPENALEX",
        "url": "https://api.openalex.org/works?" + urllib.parse.urlencode(parameters),
        "parameters": parameters,
    }


def read_current_utc_timestamp() -> str:
    """Return one second-resolution RFC 3339 UTC timestamp."""

    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_retry_after_delay_seconds(value: object, accessed_at_utc: str) -> float:
    """Parse HTTP Retry-After delay-seconds or HTTP-date deterministically."""

    normalized = str(value or "").strip()
    if normalized.isdigit():
        return float(normalized)
    try:
        retry_at = email.utils.parsedate_to_datetime(normalized)
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        accessed_at = datetime.strptime(
            accessed_at_utc, "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return max(
        0.0,
        (retry_at.astimezone(timezone.utc) - accessed_at).total_seconds(),
    )


def normalize_doi_identity_value(value: object) -> str:
    """Normalize a DOI without broadening identity matching."""

    normalized = str(value or "").strip().casefold()
    return re.sub(r"^(?:doi:\s*|https?://(?:dx\.)?doi\.org/)", "", normalized)


def write_atomic_binary_file(path: Path, payload: bytes) -> None:
    """Write bytes through a same-directory atomic replacement."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".part")
    temporary_path.write_bytes(payload)
    temporary_path.replace(path)


def normalize_http_response_record(
    response: Mapping[str, object], maximum_bytes: int
) -> Dict[str, object]:
    """Validate an injected or real transport response record."""

    try:
        status_code = int(response["status_code"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("transport response lacks a valid status code") from error
    body = response.get("body", b"")
    if not isinstance(body, bytes):
        raise ValueError("transport response body must be bytes")
    if len(body) > maximum_bytes:
        raise ValueError("transport response exceeds byte cap")
    raw_headers = response.get("headers", {})
    if not isinstance(raw_headers, Mapping):
        raise ValueError("transport response headers must be a mapping")
    headers = {
        str(key).casefold(): str(value)
        for key, value in raw_headers.items()
        if str(key).casefold()
        in {"content-type", "content-length", "retry-after", "etag", "last-modified"}
    }
    final_url = str(response.get("final_url", ""))
    if final_url and urllib.parse.urlsplit(final_url).scheme != "https":
        raise ServiceStopError("transport returned a non-HTTPS final URI")
    redirects = response.get("redirects", [])
    if not isinstance(redirects, list) or len(redirects) > MAXIMUM_REDIRECTS:
        raise ServiceStopError("transport redirect chain exceeds policy")
    return {
        "status_code": status_code,
        "body": body,
        "headers": headers,
        "final_url": final_url,
        "redirects": redirects,
    }


def execute_bounded_http_request(request: Mapping[str, object]) -> Dict[str, object]:
    """Execute one HTTPS operation with manual bounded redirects."""

    original_url = str(request.get("url", ""))
    if urllib.parse.urlsplit(original_url).scheme != "https":
        raise ServiceStopError("only HTTPS requests are authorized")
    maximum_bytes = int(request.get("maximum_bytes", MAXIMUM_METADATA_BYTES))
    request_headers = {
        "User-Agent": G04_USER_AGENT,
        "Accept": str(request.get("accept", "*/*")),
    }
    current_url = original_url
    redirects: List[Dict[str, object]] = []
    try:
        with requests.Session() as session:
            for redirect_index in range(MAXIMUM_REDIRECTS + 1):
                response = session.get(
                    current_url,
                    headers=request_headers,
                    allow_redirects=False,
                    stream=True,
                    timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
                )
                if response.status_code in {301, 302, 303, 307, 308}:
                    location = response.headers.get("location", "")
                    response.close()
                    if redirect_index >= MAXIMUM_REDIRECTS or not location:
                        raise ServiceStopError("redirect chain is incomplete or too long")
                    next_url = urllib.parse.urljoin(current_url, location)
                    if urllib.parse.urlsplit(next_url).scheme != "https":
                        raise ServiceStopError("redirect attempted a non-HTTPS URI")
                    redirects.append(
                        {
                            "status_code": response.status_code,
                            "from": current_url,
                            "to": next_url,
                        }
                    )
                    current_url = next_url
                    continue

                body = bytearray()
                for chunk in response.iter_content(chunk_size=64 * 1024):
                    if not chunk:
                        continue
                    body.extend(chunk)
                    if len(body) > maximum_bytes:
                        response.close()
                        raise ValueError("transport response exceeds byte cap")
                result = {
                    "status_code": response.status_code,
                    "body": bytes(body),
                    "headers": dict(response.headers),
                    "final_url": current_url,
                    "redirects": redirects,
                }
                response.close()
                return normalize_http_response_record(result, maximum_bytes)
    except ServiceStopError:
        raise
    except ValueError:
        raise
    except requests.RequestException as error:
        raise RetryableRequestError(str(error)) from error
    raise ServiceStopError("redirect loop ended without a terminal response")


def perform_bounded_request_operation(
    request: Mapping[str, object],
    state: MutableMapping[str, object],
    request_function: Callable[[Mapping[str, object]], Mapping[str, object]],
    sleep_function: Callable[[float], None],
    clock_function: Callable[[], str],
) -> Tuple[Dict[str, object], List[Dict[str, object]]]:
    """Perform one retry chain while preserving every attempt."""

    attempts: List[Dict[str, object]] = []
    service = str(request.get("service", "PUBLISHER"))
    request_host = (urllib.parse.urlsplit(str(request.get("url", ""))).hostname or "").casefold()
    stopped_hosts = state.setdefault("stopped_hosts", [])
    if not isinstance(stopped_hosts, list):
        raise ValueError("stopped_hosts state must be a list")
    if request_host and request_host in stopped_hosts:
        return (
            {
                "status_code": -1,
                "body": b"",
                "headers": {},
                "final_url": str(request["url"]),
                "redirects": [],
            },
            attempts,
        )
    cadence = 1.1 if service == "OPENALEX" else 3.1
    maximum_bytes = int(request.get("maximum_bytes", MAXIMUM_METADATA_BYTES))
    for attempt_number in range(1, MAXIMUM_RETRY_ATTEMPTS + 1):
        if int(state.get("campaign_requests", 0)) >= MAXIMUM_HTTP_ATTEMPTS:
            raise ServiceStopError("global G04 HTTP attempt cap exhausted")
        if state.get("last_request_service") is not None:
            sleep_function(cadence)
        state["last_request_service"] = service
        state["campaign_requests"] = int(state.get("campaign_requests", 0)) + 1
        state["invocation_requests"] = int(state.get("invocation_requests", 0)) + 1
        accessed_at = clock_function()
        try:
            raw_response = request_function(request)
            response = normalize_http_response_record(raw_response, maximum_bytes)
            status_code = int(response["status_code"])
            body = bytes(response["body"])
            attempts.append(
                {
                    "attempt": attempt_number,
                    "accessed_at_utc": accessed_at,
                    "requested_uri": str(request["url"]),
                    "response_status": "HTTP_{0}".format(status_code),
                    "final_uri": str(response.get("final_url") or request["url"]),
                    "redirects": response.get("redirects", []),
                    "response_headers": response.get("headers", {}),
                    "payload_checksum": hashlib.sha256(body).hexdigest().upper(),
                }
            )
        except ServiceStopError as error:
            attempts.append(
                {
                    "attempt": attempt_number,
                    "accessed_at_utc": accessed_at,
                    "requested_uri": str(request["url"]),
                    "response_status": "SERVICE_STOPPED",
                    "final_uri": str(request["url"]),
                    "redirects": [],
                    "response_headers": {},
                    "payload_checksum": "NOT_AVAILABLE",
                    "error": str(error),
                }
            )
            return (
                {
                    "status_code": -1,
                    "body": b"",
                    "headers": {},
                    "final_url": str(request["url"]),
                    "redirects": [],
                },
                attempts,
            )
        except (RetryableRequestError, requests.RequestException) as error:
            attempts.append(
                {
                    "attempt": attempt_number,
                    "accessed_at_utc": accessed_at,
                    "requested_uri": str(request["url"]),
                    "response_status": "TRANSPORT_ERROR",
                    "final_uri": str(request["url"]),
                    "redirects": [],
                    "response_headers": {},
                    "payload_checksum": "NOT_AVAILABLE",
                    "error": str(error),
                }
            )
            if attempt_number == MAXIMUM_RETRY_ATTEMPTS:
                return (
                    {
                        "status_code": 0,
                        "body": b"",
                        "headers": {},
                        "final_url": str(request["url"]),
                        "redirects": [],
                    },
                    attempts,
                )
            sleep_function(3.1 * (2 ** (attempt_number - 1)))
            continue

        if status_code not in {408, 429} and status_code < 500:
            if status_code in {401, 403} and request_host not in stopped_hosts:
                stopped_hosts.append(request_host)
            return response, attempts
        if attempt_number == MAXIMUM_RETRY_ATTEMPTS:
            if status_code == 429 and request_host not in stopped_hosts:
                stopped_hosts.append(request_host)
            return response, attempts
        retry_after = response.get("headers", {}).get("retry-after", "0")
        retry_seconds = parse_retry_after_delay_seconds(retry_after, accessed_at)
        sleep_function(max(3.1 * (2 ** (attempt_number - 1)), retry_seconds))
    raise RuntimeError("bounded request operation ended without a response")


def parse_arxiv_metadata_entries(
    payload: bytes, queue: Sequence[Mapping[str, object]]
) -> Dict[str, Dict[str, str]]:
    """Parse exact arXiv entries and reject identity drift."""

    expected_by_arxiv = {
        str(record["arxiv_id"]): record
        for record in queue
        if str(record.get("arxiv_id", "UNKNOWN")) != "UNKNOWN"
    }
    try:
        root = ElementTree.fromstring(payload)
    except ElementTree.ParseError as error:
        raise ValueError("malformed arXiv Atom metadata") from error
    atom_namespace = "{http://www.w3.org/2005/Atom}"
    arxiv_namespace = "{http://arxiv.org/schemas/atom}"
    parsed: Dict[str, Dict[str, str]] = {}
    for entry in root.findall(atom_namespace + "entry"):
        entry_uri = (entry.findtext(atom_namespace + "id") or "").strip()
        identity_match = re.search(r"/abs/(\d{4}\.\d{4,5})(v\d+)?$", entry_uri)
        if identity_match is None:
            raise ValueError("arXiv entry has an unsupported identity URI")
        arxiv_id = identity_match.group(1)
        version = identity_match.group(2) or "UNKNOWN"
        if arxiv_id not in expected_by_arxiv:
            raise ValueError("arXiv metadata returned a non-queue identity")
        record = expected_by_arxiv[arxiv_id]
        paper_id = str(record["paper_id"])
        if paper_id in parsed:
            raise ValueError("arXiv metadata returned a duplicate identity")
        title = " ".join((entry.findtext(atom_namespace + "title") or "").split())
        title_match = normalize_title_identity(title) == normalize_title_identity(
            record["title"]
        )
        expected_version = str(record.get("canonical_version", "UNKNOWN"))
        if expected_version != "UNKNOWN" and version != expected_version:
            raise ValueError("arXiv canonical version changed after queue freeze")
        pdf_links = [
            link.get("href", "")
            for link in entry.findall(atom_namespace + "link")
            if link.get("title") == "pdf" or link.get("type") == "application/pdf"
        ]
        if len(pdf_links) != 1:
            raise ValueError("arXiv entry must expose exactly one PDF link")
        pdf_url = pdf_links[0].replace("http://", "https://", 1)
        parsed_url = urllib.parse.urlsplit(pdf_url)
        if parsed_url.scheme != "https" or parsed_url.hostname not in {
            "arxiv.org",
            "export.arxiv.org",
        }:
            raise ValueError("arXiv PDF link is not an official HTTPS URI")
        license_uri = (
            entry.findtext(arxiv_namespace + "license") or "NOT_DISCOVERED"
        ).strip()
        parsed[paper_id] = {
            "paper_id": paper_id,
            "source_service": "ARXIV",
            "pdf_url": pdf_url,
            "retrieval_uri": pdf_url,
            "license_uri": license_uri or "NOT_DISCOVERED",
            "policy_url": ARXIV_POLICY_URL,
            "canonical_version": version,
            "title_match": "TRUE" if title_match else "FALSE",
            "observed_title": title,
        }
    return parsed


def select_official_pdf_location(
    locations: Sequence[Mapping[str, object]],
    doi: str,
) -> Mapping[str, object] | None:
    """Select one deterministic publisher or proceedings PDF location."""

    normalized_doi = normalize_doi_identity_value(doi)
    approved_hosts: set[str] = set()
    for prefix, hosts in APPROVED_PUBLISHER_HOSTS_BY_DOI_PREFIX.items():
        if normalized_doi.startswith(prefix):
            approved_hosts.update(hosts)
    accepted: List[Mapping[str, object]] = []
    for location in locations:
        source = location.get("source")
        if not isinstance(source, Mapping):
            continue
        source_type = str(source.get("type", "")).strip().casefold()
        if source_type not in {"journal", "conference", "book series"}:
            continue
        if location.get("is_oa") is not True:
            continue
        pdf_url = str(location.get("pdf_url") or "")
        parsed_url = urllib.parse.urlsplit(pdf_url)
        if parsed_url.scheme != "https" or not parsed_url.hostname:
            continue
        if parsed_url.hostname.casefold() not in approved_hosts:
            continue
        accepted.append(location)
    if not accepted:
        return None
    return sorted(accepted, key=lambda value: str(value.get("pdf_url", "")))[0]


def parse_openalex_location_entries(
    payload: bytes, queue: Sequence[Mapping[str, object]]
) -> Dict[str, Dict[str, str]]:
    """Parse exact DOI records and retain official direct PDFs only."""

    expected_by_doi = {
        normalize_doi_identity_value(record["doi"]): record
        for record in queue
        if str(record.get("arxiv_id", "UNKNOWN")) == "UNKNOWN"
        and str(record.get("doi", "UNKNOWN")) != "UNKNOWN"
    }
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("malformed OpenAlex location metadata") from error
    if not isinstance(document, Mapping) or not isinstance(document.get("results"), list):
        raise ValueError("OpenAlex payload lacks a results list")
    works_by_doi: Dict[str, List[Mapping[str, object]]] = {}
    for work in document["results"]:
        if not isinstance(work, Mapping):
            raise ValueError("OpenAlex result is not an object")
        doi = normalize_doi_identity_value(work.get("doi"))
        if doi not in expected_by_doi:
            raise ValueError("OpenAlex returned a non-queue DOI")
        works_by_doi.setdefault(doi, []).append(work)

    parsed: Dict[str, Dict[str, str]] = {}
    for doi, works in works_by_doi.items():
        record = expected_by_doi[doi]
        matching_works = [
            work
            for work in works
            if normalize_title_identity(work.get("title"))
            == normalize_title_identity(record["title"])
        ]
        if not matching_works:
            raise ValueError("OpenAlex DOI title does not match canonical identity")
        locations: List[Mapping[str, object]] = []
        for work in matching_works:
            best_location = work.get("best_oa_location")
            if isinstance(best_location, Mapping) and best_location not in locations:
                locations.append(best_location)
            for location in work.get("locations", []):
                if isinstance(location, Mapping) and location not in locations:
                    locations.append(location)
        selected = select_official_pdf_location(locations, doi)
        if selected is None:
            continue
        license_value = str(selected.get("license") or "NOT_DISCOVERED")
        if not license_value.startswith(("http://", "https://")):
            license_value = "NOT_DISCOVERED"
        paper_id = str(record["paper_id"])
        parsed[paper_id] = {
            "paper_id": paper_id,
            "source_service": "PUBLISHER",
            "retrieval_uri": str(selected["pdf_url"]),
            "license_uri": license_value,
            "policy_url": PUBLISHER_POLICY_URL,
            "canonical_version": str(record.get("canonical_version", "UNKNOWN")),
        }
    return parsed


def load_cached_metadata_response(
    body_path: Path, trace_path: Path, request: Mapping[str, object]
) -> Dict[str, object] | None:
    """Load one checksummed metadata response or fail closed."""

    if not body_path.exists() and not trace_path.exists():
        return None
    if not body_path.is_file() or not trace_path.is_file():
        raise ValueError("metadata cache body and trace must both exist")
    try:
        trace = json.loads(trace_path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("metadata cache trace is malformed") from error
    if not isinstance(trace, Mapping):
        raise ValueError("metadata cache trace must be an object")
    body = body_path.read_bytes()
    checksum = hashlib.sha256(body).hexdigest().upper()
    if trace.get("response_checksum") != checksum:
        raise ValueError("metadata cache response checksum mismatch")
    if trace.get("request_url") != str(request["url"]):
        raise ValueError("metadata cache request URI mismatch")
    return {
        "status_code": 200,
        "body": body,
        "headers": trace.get("response_headers", {}),
        "final_url": trace.get("final_url", str(request["url"])),
        "redirects": trace.get("redirects", []),
        "cache_hit": True,
        "attempts": trace.get("attempts", []),
    }


def request_metadata_batch_cached(
    reference_root: Path,
    cache_name: str,
    request: Mapping[str, object],
    state: MutableMapping[str, object],
    *,
    allow_network: bool,
    request_function: Callable[[Mapping[str, object]], Mapping[str, object]],
    sleep_function: Callable[[float], None],
    clock_function: Callable[[], str],
) -> Dict[str, object]:
    """Reuse or request one exact metadata batch deterministically."""

    cache_root = reference_root / "cache" / "g04" / "metadata"
    body_path = cache_root / (cache_name + ".body")
    trace_path = cache_root / (cache_name + ".trace.json")
    cached = load_cached_metadata_response(body_path, trace_path, request)
    if cached is not None:
        state["metadata_cache_hits"] = int(state.get("metadata_cache_hits", 0)) + 1
        return cached
    if not allow_network:
        raise RuntimeError("network-disabled replay lacks completed metadata cache")

    response, attempts = perform_bounded_request_operation(
        request, state, request_function, sleep_function, clock_function
    )
    response["attempts"] = attempts
    response["cache_hit"] = False
    if int(response["status_code"]) == 200:
        body = bytes(response["body"])
        trace = {
            "goal_id": "G04",
            "cache_name": cache_name,
            "request_url": str(request["url"]),
            "final_url": str(response.get("final_url") or request["url"]),
            "redirects": response.get("redirects", []),
            "response_headers": response.get("headers", {}),
            "response_checksum": hashlib.sha256(body).hexdigest().upper(),
            "attempts": attempts,
        }
        write_atomic_binary_file(body_path, body)
        write_atomic_binary_file(trace_path, serialize_request_trace_bytes(trace))
    return response


def classify_http_terminal_status(status_code: int) -> str:
    """Map one exhausted HTTP result to a terminal acquisition status."""

    if status_code == -1:
        return "SERVICE_STOPPED"
    if status_code == 0:
        return "FAILED"
    if status_code in {401, 403}:
        return "AUTHORIZATION_FAILED"
    if status_code == 404:
        return "NOT_FOUND"
    if status_code == 429:
        return "RATE_LIMITED"
    return "FAILED"


def derive_stopped_request_hosts(
    ledger_rows: Sequence[Mapping[str, str]],
) -> List[str]:
    """Reconstruct durable host stops from terminal ledger evidence."""

    stopped_hosts = {
        (urllib.parse.urlsplit(str(row.get("retrieval_uri", ""))).hostname or "").casefold()
        for row in ledger_rows
        if row.get("acquisition_status") in {"AUTHORIZATION_FAILED", "RATE_LIMITED"}
    }
    return sorted(host for host in stopped_hosts if host)


def resolve_queue_source_records(
    reference_root: Path,
    queue: Sequence[Mapping[str, object]],
    state: MutableMapping[str, object],
    *,
    allow_network: bool,
    request_function: Callable[[Mapping[str, object]], Mapping[str, object]],
    sleep_function: Callable[[float], None],
    clock_function: Callable[[], str],
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Tuple[str, str]]]:
    """Resolve only official exact-identity full-text sources."""

    sources: Dict[str, Dict[str, str]] = {}
    terminal_reasons: Dict[str, Tuple[str, str]] = {}
    arxiv_records = [record for record in queue if record["arxiv_id"] != "UNKNOWN"]
    if arxiv_records:
        compiled = build_arxiv_metadata_request(
            [str(record["arxiv_id"]) for record in arxiv_records]
        )
        request = {
            **compiled,
            "accept": "application/atom+xml",
            "maximum_bytes": MAXIMUM_METADATA_BYTES,
        }
        response = request_metadata_batch_cached(
            reference_root,
            "arxiv-exact-identities",
            request,
            state,
            allow_network=allow_network,
            request_function=request_function,
            sleep_function=sleep_function,
            clock_function=clock_function,
        )
        status_code = int(response["status_code"])
        if status_code == 200:
            sources.update(parse_arxiv_metadata_entries(bytes(response["body"]), queue))
            for record in arxiv_records:
                paper_id = str(record["paper_id"])
                if paper_id not in sources:
                    terminal_reasons[paper_id] = (
                        "UNAVAILABLE",
                        "NO_VERIFIED_ARXIV_METADATA_ENTRY",
                    )
        else:
            terminal_status = classify_http_terminal_status(status_code)
            for record in arxiv_records:
                terminal_reasons[str(record["paper_id"])] = (
                    terminal_status,
                    "ARXIV_METADATA_{0}".format(
                        "TRANSPORT_ERROR" if status_code == 0 else "HTTP_" + str(status_code)
                    ),
                )

    doi_records = [
        record
        for record in queue
        if record["arxiv_id"] == "UNKNOWN" and record["doi"] != "UNKNOWN"
    ]
    if doi_records:
        compiled = build_openalex_location_request(
            [str(record["doi"]) for record in doi_records]
        )
        request = {
            **compiled,
            "accept": "application/json",
            "maximum_bytes": MAXIMUM_METADATA_BYTES,
        }
        response = request_metadata_batch_cached(
            reference_root,
            "openalex-exact-dois",
            request,
            state,
            allow_network=allow_network,
            request_function=request_function,
            sleep_function=sleep_function,
            clock_function=clock_function,
        )
        status_code = int(response["status_code"])
        if status_code == 200:
            sources.update(
                parse_openalex_location_entries(bytes(response["body"]), queue)
            )
            for record in doi_records:
                paper_id = str(record["paper_id"])
                if paper_id not in sources:
                    terminal_reasons[paper_id] = (
                        "UNAVAILABLE",
                        "NO_ACCEPTABLE_DIRECT_PUBLISHER_SOURCE",
                    )
        else:
            terminal_status = classify_http_terminal_status(status_code)
            for record in doi_records:
                terminal_reasons[str(record["paper_id"])] = (
                    terminal_status,
                    "OPENALEX_METADATA_{0}".format(
                        "TRANSPORT_ERROR" if status_code == 0 else "HTTP_" + str(status_code)
                    ),
                )

    for record in queue:
        paper_id = str(record["paper_id"])
        if paper_id not in sources and paper_id not in terminal_reasons:
            terminal_reasons[paper_id] = (
                "UNAVAILABLE",
                "NO_ARXIV_OR_DOI_SOURCE_IDENTITY",
            )
    return sources, terminal_reasons


def normalize_terminal_reason_token(value: object) -> str:
    """Normalize one terminal reason into a stable TSV-safe token."""

    normalized = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "UNKNOWN")).strip("_")
    return normalized.upper() or "UNKNOWN"


def build_requested_failure_row(
    queue_record: Mapping[str, object],
    source_record: Mapping[str, str],
    response: Mapping[str, object],
    attempts: Sequence[Mapping[str, object]],
    trace_path: str,
    trace_checksum: str,
    *,
    acquisition_status: str,
    terminal_reason: str,
) -> Dict[str, str]:
    """Build one requested but non-acquired terminal row."""

    rank = int(queue_record["queue_rank"])
    status_code = int(response.get("status_code", 0))
    return {
        "request_id": "REQ-G04-{0:04d}".format(rank),
        "goal_id": "G04",
        "queue_rank": str(rank),
        "paper_id": str(queue_record["paper_id"]),
        "source_service": str(source_record["source_service"]),
        "retrieval_uri": str(response.get("final_url") or source_record["retrieval_uri"]),
        "accessed_at_utc": str(attempts[0]["accessed_at_utc"]) if attempts else "NOT_REQUESTED",
        "response_status": (
            "SERVICE_STOPPED"
            if status_code == -1
            else "TRANSPORT_ERROR"
            if status_code == 0
            else "HTTP_{0}".format(status_code)
        ),
        "media_type": str(response.get("headers", {}).get("content-type", "NOT_AVAILABLE")),
        "content_length_bytes": str(len(bytes(response.get("body", b"")))),
        "source_checksum": "NOT_AVAILABLE",
        "local_path": "NOT_ACQUIRED",
        "license_uri": "NOT_DISCOVERED",
        "license_state": "LICENSE_UNAVAILABLE",
        "acquisition_status": acquisition_status,
        "attempt_count": str(len(attempts)),
        "retry_events": "NONE" if len(attempts) <= 1 else "RETRY_COUNT={0}".format(len(attempts) - 1),
        "rate_limit_events": "RATE_LIMIT_COUNT={0}".format(
            sum(attempt.get("response_status") == "HTTP_429" for attempt in attempts)
        ),
        "policy_url": str(source_record["policy_url"]),
        "policy_checked_date": "2026-08-11",
        "cache_status": "MISS",
        "trace_path": trace_path,
        "trace_checksum": trace_checksum,
        "parser_name": "NOT_APPLICABLE",
        "parser_version": "NOT_APPLICABLE",
        "parser_options": "NOT_APPLICABLE",
        "page_count": "0",
        "extracted_path": "NOT_ACQUIRED",
        "extracted_checksum": "NOT_AVAILABLE",
        "parse_status": "NOT_APPLICABLE",
        "terminal_reason": normalize_terminal_reason_token(terminal_reason),
    }


def acquire_single_paper_source(
    reference_root: Path,
    queue_record: Mapping[str, object],
    source_record: Mapping[str, str],
    state: MutableMapping[str, object],
    *,
    request_function: Callable[[Mapping[str, object]], Mapping[str, object]],
    sleep_function: Callable[[float], None],
    clock_function: Callable[[], str],
) -> Dict[str, str]:
    """Acquire, validate, and mechanically parse one approved PDF."""

    paper_id = str(queue_record["paper_id"])
    source_service = str(source_record["source_service"])
    request = {
        "service": source_service,
        "url": str(source_record["retrieval_uri"]),
        "accept": "application/pdf",
        "maximum_bytes": MAXIMUM_PDF_BYTES,
    }
    response, attempts = perform_bounded_request_operation(
        request, state, request_function, sleep_function, clock_function
    )
    trace_relative = "cache/g04/traces/{0}.json".format(paper_id)
    trace = {
        "goal_id": "G04",
        "paper_id": paper_id,
        "source_service": source_service,
        "request_url": str(source_record["retrieval_uri"]),
        "attempts": attempts,
    }
    trace_bytes = serialize_request_trace_bytes(trace)
    trace_checksum = hashlib.sha256(trace_bytes).hexdigest().upper()
    trace_path = validate_safe_local_path(reference_root, trace_relative)
    write_atomic_binary_file(trace_path, trace_bytes)

    status_code = int(response["status_code"])
    if status_code != 200:
        acquisition_status = classify_http_terminal_status(status_code)
        return build_requested_failure_row(
            queue_record,
            source_record,
            response,
            attempts,
            trace_relative,
            trace_checksum,
            acquisition_status=acquisition_status,
            terminal_reason=(
                "TRANSPORT_ERROR"
                if status_code == 0
                else "SERVICE_POLICY_STOP"
                if status_code == -1
                else "HTTP_{0}".format(status_code)
            ),
        )

    payload = bytes(response["body"])
    media_type = str(response.get("headers", {}).get("content-type", "application/octet-stream"))
    try:
        page_count = validate_pdf_payload_bytes(payload, media_type, MAXIMUM_PDF_BYTES)
    except ValueError as error:
        return build_requested_failure_row(
            queue_record,
            source_record,
            response,
            attempts,
            trace_relative,
            trace_checksum,
            acquisition_status="PAYLOAD_REJECTED",
            terminal_reason="PDF_REJECTED_{0}".format(error),
        )

    pdf_relative = "sources/papers/" + build_canonical_paper_filename(paper_id)
    text_relative = "cache/g04/text/{0}.txt".format(paper_id)
    pdf_path = validate_safe_local_path(reference_root, pdf_relative)
    text_path = validate_safe_local_path(reference_root, text_relative)
    source_checksum = hashlib.sha256(payload).hexdigest().upper()
    write_atomic_binary_file(pdf_path, payload)

    parse_status = "PARSED"
    terminal_reason = (
        "ACQUIRED_AND_PARSED_WITH_METADATA_TITLE_VARIANT"
        if source_record.get("title_match") == "FALSE"
        else "ACQUIRED_AND_PARSED"
    )
    parser_record: Dict[str, object]
    try:
        parser_record = extract_pdf_text_deterministic(pdf_path, text_path)
    except Exception as error:
        parse_status = "PARSE_FAILED"
        terminal_reason = "ACQUIRED_PARSE_FAILED_{0}{1}".format(
            type(error).__name__,
            "_WITH_METADATA_TITLE_VARIANT"
            if source_record.get("title_match") == "FALSE"
            else "",
        )
        parser_record = {
            "parser_name": "pypdf",
            "parser_version": pypdf.__version__,
            "parser_options": "layout;page-separator=LF-FF-LF",
            "page_count": page_count,
            "extracted_checksum": "NOT_AVAILABLE",
        }
        if text_path.exists():
            text_path.unlink()

    license_uri = str(source_record.get("license_uri") or "NOT_DISCOVERED")
    return {
        "request_id": "REQ-G04-{0:04d}".format(int(queue_record["queue_rank"])),
        "goal_id": "G04",
        "queue_rank": str(queue_record["queue_rank"]),
        "paper_id": paper_id,
        "source_service": source_service,
        "retrieval_uri": str(response.get("final_url") or source_record["retrieval_uri"]),
        "accessed_at_utc": str(attempts[0]["accessed_at_utc"]),
        "response_status": "HTTP_200",
        "media_type": media_type,
        "content_length_bytes": str(len(payload)),
        "source_checksum": source_checksum,
        "local_path": pdf_relative,
        "license_uri": license_uri,
        "license_state": classify_license_state_uri(license_uri, acquired=True),
        "acquisition_status": "ACQUIRED",
        "attempt_count": str(len(attempts)),
        "retry_events": "NONE" if len(attempts) <= 1 else "RETRY_COUNT={0}".format(len(attempts) - 1),
        "rate_limit_events": "RATE_LIMIT_COUNT={0}".format(
            sum(attempt.get("response_status") == "HTTP_429" for attempt in attempts)
        ),
        "policy_url": str(source_record["policy_url"]),
        "policy_checked_date": "2026-08-11",
        "cache_status": "MISS",
        "trace_path": trace_relative,
        "trace_checksum": trace_checksum,
        "parser_name": str(parser_record["parser_name"]),
        "parser_version": str(parser_record["parser_version"]),
        "parser_options": str(parser_record["parser_options"]),
        "page_count": str(parser_record["page_count"]),
        "extracted_path": text_relative if parse_status == "PARSED" else "NOT_ACQUIRED",
        "extracted_checksum": str(parser_record["extracted_checksum"]),
        "parse_status": parse_status,
        "terminal_reason": terminal_reason,
    }


def validate_partial_terminal_rows(
    rows: Sequence[Mapping[str, str]], queue: Sequence[Mapping[str, object]]
) -> List[str]:
    """Validate a resumable strict subset of terminal rows."""

    errors: List[str] = []
    queue_by_rank = {int(record["queue_rank"]): str(record["paper_id"]) for record in queue}
    seen_ranks: set[int] = set()
    seen_papers: set[str] = set()
    for row_number, row in enumerate(rows, start=1):
        prefix = "partial download-ledger row {0}: ".format(row_number)
        if tuple(row) != DOWNLOAD_LEDGER_FIELDS:
            errors.append(prefix + "fields differ from frozen header")
            continue
        try:
            rank = int(row["queue_rank"])
            attempts = int(row["attempt_count"])
        except ValueError:
            errors.append(prefix + "integer field is invalid")
            continue
        if rank in seen_ranks or row["paper_id"] in seen_papers:
            errors.append(prefix + "duplicate terminal identity")
        seen_ranks.add(rank)
        seen_papers.add(row["paper_id"])
        if queue_by_rank.get(rank) != row["paper_id"]:
            errors.append(prefix + "identity differs from exact queue")
        if attempts < 0 or attempts > MAXIMUM_PAPER_ATTEMPTS:
            errors.append(prefix + "attempt count exceeds cap")
        if row["acquisition_status"] not in ALLOWED_ACQUISITION_STATUSES:
            errors.append(prefix + "invalid acquisition status")
        if row["license_state"] not in ALLOWED_LICENSE_STATES:
            errors.append(prefix + "invalid license state")
        if row["parse_status"] not in ALLOWED_PARSE_STATUSES:
            errors.append(prefix + "invalid parse status")
    return sorted(set(errors))


def mark_selected_papers_deep(
    manifest_rows: Sequence[Mapping[str, str]],
    queue: Sequence[Mapping[str, object]],
) -> List[Dict[str, str]]:
    """Mark only exact selected manifest identities for deep reading."""

    queue_ids = {str(record["paper_id"]) for record in queue}
    if len(queue_ids) != EXPECTED_QUEUE_SIZE:
        raise ValueError("selection gate requires exactly 50 queue identities")
    manifest_ids = {row["paper_id"] for row in manifest_rows}
    missing = sorted(queue_ids - manifest_ids)
    if missing:
        raise ValueError("selected identities missing from manifest: " + ",".join(missing))
    updated: List[Dict[str, str]] = []
    for source_row in manifest_rows:
        row = dict(source_row)
        if row["paper_id"] in queue_ids:
            if row["selection_status"] == "REJECTED":
                raise ValueError("rejected manifest identity cannot enter acquisition")
            row["selection_status"] = "DEEP_READ"
        updated.append(row)
    return updated


def project_manifest_before_g04(
    manifest_rows: Sequence[Mapping[str, str]],
    queue: Sequence[Mapping[str, object]],
) -> List[Dict[str, str]]:
    """Project G04-owned fields back to their verified G03 state."""

    queue_ids = {str(record["paper_id"]) for record in queue}
    projected: List[Dict[str, str]] = []
    for source_row in manifest_rows:
        row = dict(source_row)
        if row.get("paper_id") in queue_ids:
            note_clauses = [clause for clause in row.get("notes", "").split(";") if clause]
            original_pdf = next(
                (
                    clause.split("=", 1)[1]
                    for clause in note_clauses
                    if clause.startswith("G04_ORIGINAL_PDF_URL=")
                ),
                None,
            )
            original_license = next(
                (
                    clause.split("=", 1)[1]
                    for clause in note_clauses
                    if clause.startswith("G04_ORIGINAL_LICENSE_URI=")
                ),
                None,
            )
            if original_pdf is not None:
                row["pdf_url"] = original_pdf
            if original_license is not None:
                row["license_uri"] = original_license
            row["selection_status"] = "METADATA_ONLY"
            row["local_path"] = "NOT_ACQUIRED"
            row["sha256"] = "NOT_ACQUIRED"
            clauses = [
                clause
                for clause in note_clauses
                if clause
                and not clause.startswith("G04_")
                and clause not in ALLOWED_LICENSE_STATES
            ]
            row["notes"] = ";".join(clauses)
        projected.append(row)
    return projected


def update_g04_manifest_rows(
    manifest_rows: Sequence[Mapping[str, str]],
    ledger_rows: Sequence[Mapping[str, str]],
    queue: Sequence[Mapping[str, object]],
) -> List[Dict[str, str]]:
    """Apply terminal G04 state while preserving reversible G03 fields."""

    queue_ids = {str(record["paper_id"]) for record in queue}
    ledger_by_id = {row["paper_id"]: row for row in ledger_rows}
    if set(ledger_by_id) != queue_ids:
        raise ValueError("manifest update requires one terminal row for every queue identity")
    updated: List[Dict[str, str]] = []
    for source_row in manifest_rows:
        row = dict(source_row)
        paper_id = row["paper_id"]
        if paper_id not in queue_ids:
            updated.append(row)
            continue
        ledger = ledger_by_id[paper_id]
        base_notes = [
            clause
            for clause in row.get("notes", "").split(";")
            if clause
            and not clause.startswith("G04_")
            and clause not in ALLOWED_LICENSE_STATES
        ]
        original_pdf = row.get("pdf_url", "UNKNOWN")
        original_license = row.get("license_uri", "UNKNOWN")
        base_notes.extend(
            (
                "G04_ORIGINAL_PDF_URL=" + original_pdf,
                "G04_ORIGINAL_LICENSE_URI=" + original_license,
                ledger["license_state"],
                "G04_ACQUISITION_STATUS=" + ledger["acquisition_status"],
                "G04_PARSE_STATUS=" + ledger["parse_status"],
            )
        )
        acquired = ledger["acquisition_status"] == "ACQUIRED"
        row["selection_status"] = "DEEP_READ" if acquired else "UNAVAILABLE"
        row["local_path"] = ledger["local_path"] if acquired else "NOT_ACQUIRED"
        row["sha256"] = ledger["source_checksum"] if acquired else "NOT_ACQUIRED"
        if acquired:
            row["pdf_url"] = ledger["retrieval_uri"]
            if ledger["license_uri"] != "NOT_DISCOVERED":
                row["license_uri"] = ledger["license_uri"]
        row["notes"] = ";".join(base_notes)
        updated.append(row)
    return updated


def validate_g04_manifest_rows(
    manifest_rows: Sequence[Mapping[str, str]],
    ledger_rows: Sequence[Mapping[str, str]],
    queue: Sequence[Mapping[str, object]],
    *,
    require_complete: bool,
    allow_read_complete: bool = False,
) -> List[str]:
    """Validate G04 state and optionally preserve later semantic completion."""

    errors: List[str] = []
    queue_ids = {str(record["paper_id"]) for record in queue}
    manifest_by_id = {row.get("paper_id", ""): row for row in manifest_rows}
    if len(queue_ids) != EXPECTED_QUEUE_SIZE:
        errors.append("G04 manifest gate requires exactly 50 queue identities")
    missing_ids = sorted(queue_ids - set(manifest_by_id))
    if missing_ids:
        errors.append("G04 manifest is missing queue identities: " + ",".join(missing_ids))
    if not allow_read_complete and any(
        row.get("selection_status") == "READ_COMPLETE" for row in manifest_rows
    ):
        errors.append("G04 cannot mark any manifest row READ_COMPLETE")
    for row in manifest_rows:
        if row.get("paper_id") not in queue_ids and row.get("selection_status") == "DEEP_READ":
            errors.append("non-queue manifest identity cannot be DEEP_READ")

    if require_complete and len(ledger_rows) != EXPECTED_QUEUE_SIZE:
        errors.append("completed G04 requires exactly 50 terminal ledger rows")
    if ledger_rows:
        errors.extend(validate_download_ledger_rows(ledger_rows, queue))
        ledger_by_id = {row.get("paper_id", ""): row for row in ledger_rows}
        for paper_id in sorted(queue_ids):
            manifest = manifest_by_id.get(paper_id)
            ledger = ledger_by_id.get(paper_id)
            if manifest is None or ledger is None:
                continue
            acquired = ledger.get("acquisition_status") == "ACQUIRED"
            expected_status = "DEEP_READ" if acquired else "UNAVAILABLE"
            accepted_statuses = {expected_status}
            if allow_read_complete and acquired:
                accepted_statuses.add("READ_COMPLETE")
            if manifest.get("selection_status") not in accepted_statuses:
                errors.append(
                    "G04 manifest {0} status does not match terminal acquisition".format(
                        paper_id
                    )
                )
            expected_path = ledger.get("local_path", "") if acquired else "NOT_ACQUIRED"
            expected_checksum = (
                ledger.get("source_checksum", "") if acquired else "NOT_ACQUIRED"
            )
            if manifest.get("local_path") != expected_path:
                errors.append("G04 manifest {0} local_path mismatch".format(paper_id))
            if manifest.get("sha256") != expected_checksum:
                errors.append("G04 manifest {0} SHA-256 mismatch".format(paper_id))
            license_tokens = [
                token
                for token in ALLOWED_LICENSE_STATES
                if token in manifest.get("notes", "").split(";")
            ]
            if license_tokens != [ledger.get("license_state")]:
                errors.append("G04 manifest {0} license state mismatch".format(paper_id))
            status_marker = "G04_ACQUISITION_STATUS=" + ledger.get(
                "acquisition_status", ""
            )
            if status_marker not in manifest.get("notes", "").split(";"):
                errors.append("G04 manifest {0} lacks acquisition marker".format(paper_id))
    else:
        for paper_id in sorted(queue_ids):
            manifest = manifest_by_id.get(paper_id)
            if manifest is None:
                continue
            if manifest.get("selection_status") != "DEEP_READ":
                errors.append("G04 selected manifest identity must be DEEP_READ: " + paper_id)
            if manifest.get("local_path") != "NOT_ACQUIRED":
                errors.append("pre-acquisition G04 row cannot have local content: " + paper_id)
            if manifest.get("sha256") != "NOT_ACQUIRED":
                errors.append("pre-acquisition G04 row cannot have a checksum: " + paper_id)
    return sorted(set(errors))


def validate_local_artifact_records(
    reference_root: Path,
    ledger_rows: Sequence[Mapping[str, str]],
) -> List[str]:
    """Validate every acquired PDF, trace, and parsed-text checksum."""

    errors: List[str] = []
    expected_request_urls: Dict[str, str] = {}
    metadata_root = reference_root / "cache" / "g04" / "metadata"
    metadata_evidence_exists = any(metadata_root.glob("*.body"))
    if metadata_evidence_exists:
        try:
            expected_request_urls = derive_frozen_paper_request_urls(reference_root)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            errors.append("frozen paper source evidence is invalid: " + str(error))
    for row in ledger_rows:
        paper_id = row.get("paper_id", "UNKNOWN")
        artifact_fields: List[Tuple[str, str]] = []
        if row.get("trace_path") not in {"", "NOT_AVAILABLE"}:
            artifact_fields.append(("trace_path", "trace_checksum"))
        if row.get("acquisition_status") == "ACQUIRED":
            artifact_fields.append(("local_path", "source_checksum"))
            if row.get("parse_status") == "PARSED":
                artifact_fields.append(("extracted_path", "extracted_checksum"))
        for path_field, checksum_field in artifact_fields:
            try:
                artifact_path = validate_safe_local_path(
                    reference_root, row.get(path_field, "")
                )
            except ValueError as error:
                errors.append("{0} {1}: {2}".format(paper_id, path_field, error))
                continue
            if not artifact_path.is_file() or artifact_path.is_symlink():
                errors.append("{0} {1}: local artifact is missing".format(paper_id, path_field))
                continue
            artifact_bytes = artifact_path.read_bytes()
            actual_checksum = hashlib.sha256(artifact_bytes).hexdigest().upper()
            if actual_checksum != row.get(checksum_field, ""):
                errors.append("{0} {1}: checksum mismatch".format(paper_id, path_field))
            if path_field == "trace_path":
                try:
                    trace = json.loads(artifact_bytes)
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    errors.append(
                        "{0} trace JSON is malformed: {1}".format(paper_id, error)
                    )
                    continue
                if not isinstance(trace, Mapping):
                    errors.append("{0} trace must be an object".format(paper_id))
                    continue
                expected_request_url = expected_request_urls.get(paper_id)
                if metadata_evidence_exists and expected_request_url is None:
                    errors.append(
                        "{0} trace has no frozen paper source evidence".format(paper_id)
                    )
                errors.extend(
                    validate_paper_request_trace(
                        row,
                        trace,
                        expected_request_url=expected_request_url,
                    )
                )
    errors.extend(validate_campaign_request_evidence(reference_root, ledger_rows))
    return sorted(set(errors))


def build_unavailable_ledger_row(
    queue_record: Mapping[str, object],
    *,
    acquisition_status: str,
    terminal_reason: str,
) -> Dict[str, str]:
    """Build one explicit terminal unavailable ledger row."""

    if acquisition_status not in ALLOWED_ACQUISITION_STATUSES - {"ACQUIRED"}:
        raise ValueError("unavailable row requires a non-acquired status")
    rank = int(queue_record["queue_rank"])
    paper_id = str(queue_record["paper_id"])
    return {
        "request_id": "REQ-G04-{0:04d}".format(rank),
        "goal_id": "G04",
        "queue_rank": str(rank),
        "paper_id": paper_id,
        "source_service": "NONE",
        "retrieval_uri": "NOT_AVAILABLE",
        "accessed_at_utc": "NOT_REQUESTED",
        "response_status": "NOT_REQUESTED",
        "media_type": "NOT_AVAILABLE",
        "content_length_bytes": "0",
        "source_checksum": "NOT_AVAILABLE",
        "local_path": "NOT_ACQUIRED",
        "license_uri": "NOT_DISCOVERED",
        "license_state": "LICENSE_UNAVAILABLE",
        "acquisition_status": acquisition_status,
        "attempt_count": "0",
        "retry_events": "NONE",
        "rate_limit_events": "NONE",
        "policy_url": "NOT_APPLICABLE",
        "policy_checked_date": "2026-08-11",
        "cache_status": "NOT_APPLICABLE",
        "trace_path": "NOT_AVAILABLE",
        "trace_checksum": "NOT_AVAILABLE",
        "parser_name": "NOT_APPLICABLE",
        "parser_version": "NOT_APPLICABLE",
        "parser_options": "NOT_APPLICABLE",
        "page_count": "0",
        "extracted_path": "NOT_ACQUIRED",
        "extracted_checksum": "NOT_AVAILABLE",
        "parse_status": "NOT_APPLICABLE",
        "terminal_reason": terminal_reason,
    }


def serialize_request_trace_bytes(trace: Mapping[str, object]) -> bytes:
    """Serialize one request trace into canonical JSON bytes."""

    return (
        json.dumps(trace, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def validate_trace_attempt_records(
    trace: Mapping[str, object],
    label: str,
) -> Tuple[int, List[str]]:
    """Validate one request trace's attempt sequence and mechanical fields."""

    errors: List[str] = []
    request_url = trace.get("request_url")
    if not isinstance(request_url, str) or urllib.parse.urlsplit(request_url).scheme != "https":
        errors.append(label + ": request_url must be HTTPS")
        request_url = ""
    attempts = trace.get("attempts")
    if not isinstance(attempts, list):
        return 0, [label + ": attempts must be a list"]
    if len(attempts) > MAXIMUM_RETRY_ATTEMPTS:
        errors.append(label + ": attempts exceed the three-attempt ceiling")
    for expected_attempt, attempt in enumerate(attempts, start=1):
        attempt_label = "{0} attempt {1}".format(label, expected_attempt)
        if not isinstance(attempt, Mapping):
            errors.append(attempt_label + ": record must be an object")
            continue
        if attempt.get("attempt") != expected_attempt:
            errors.append(attempt_label + ": attempt number is not contiguous")
        accessed_at = attempt.get("accessed_at_utc")
        if not isinstance(accessed_at, str) or re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", accessed_at
        ) is None:
            errors.append(attempt_label + ": accessed_at_utc is invalid")
        if attempt.get("requested_uri") != request_url:
            errors.append(attempt_label + ": requested_uri does not match request_url")
        response_status = attempt.get("response_status")
        if not isinstance(response_status, str) or re.fullmatch(
            r"HTTP_\d{3}|TRANSPORT_ERROR|SERVICE_STOPPED", response_status
        ) is None:
            errors.append(attempt_label + ": response_status is invalid")
        if not isinstance(attempt.get("final_uri"), str):
            errors.append(attempt_label + ": final_uri is missing")
        if not isinstance(attempt.get("redirects"), list):
            errors.append(attempt_label + ": redirects must be a list")
        else:
            errors.extend(
                validate_https_redirect_route(
                    str(attempt.get("requested_uri", "")),
                    str(attempt.get("final_uri", "")),
                    attempt["redirects"],
                    attempt_label,
                )
            )
        if not isinstance(attempt.get("response_headers"), Mapping):
            errors.append(attempt_label + ": response_headers must be an object")
        payload_checksum = attempt.get("payload_checksum")
        if payload_checksum != "NOT_AVAILABLE" and (
            not isinstance(payload_checksum, str)
            or CHECKSUM_PATTERN.fullmatch(payload_checksum) is None
        ):
            errors.append(attempt_label + ": payload_checksum is invalid")
    return len(attempts), errors


def validate_https_redirect_route(
    request_url: str,
    final_url: str,
    redirects: Sequence[object],
    label: str,
) -> List[str]:
    """Prove one contiguous HTTPS request-to-terminal redirect route."""

    errors: List[str] = []
    if urllib.parse.urlsplit(request_url).scheme != "https":
        errors.append(label + ": redirect route request must be HTTPS")
    if urllib.parse.urlsplit(final_url).scheme != "https":
        errors.append(label + ": redirect route terminal must be HTTPS")
    if len(redirects) > MAXIMUM_REDIRECTS:
        errors.append(label + ": redirect route exceeds five-redirect ceiling")
    expected_from = request_url
    for redirect_index, redirect in enumerate(redirects, start=1):
        redirect_label = "{0} redirect {1}".format(label, redirect_index)
        if not isinstance(redirect, Mapping):
            errors.append(redirect_label + ": redirect route record must be an object")
            continue
        status_code = redirect.get("status_code")
        if status_code not in {301, 302, 303, 307, 308}:
            errors.append(redirect_label + ": redirect route status is invalid")
        from_url = str(redirect.get("from", ""))
        to_url = str(redirect.get("to", ""))
        if from_url != expected_from:
            errors.append(redirect_label + ": redirect route is not contiguous")
        if urllib.parse.urlsplit(from_url).scheme != "https":
            errors.append(redirect_label + ": redirect route source must be HTTPS")
        if urllib.parse.urlsplit(to_url).scheme != "https":
            errors.append(redirect_label + ": redirect route target must be HTTPS")
        expected_from = to_url
    if expected_from != final_url:
        errors.append(label + ": redirect route does not reach terminal URI")
    return sorted(set(errors))


def validate_paper_request_trace(
    row: Mapping[str, str],
    trace: Mapping[str, object],
    *,
    expected_request_url: str | None = None,
) -> List[str]:
    """Bind one paper request trace to its terminal ledger row."""

    paper_id = row.get("paper_id", "UNKNOWN")
    label = "{0} trace".format(paper_id)
    errors: List[str] = []
    expected_values = (
        ("goal_id", "G04"),
        ("paper_id", paper_id),
        ("source_service", row.get("source_service")),
    )
    for field, expected in expected_values:
        if trace.get(field) != expected:
            errors.append("{0}: {1} does not match ledger".format(label, field))
    if expected_request_url is not None and trace.get("request_url") != expected_request_url:
        errors.append(label + ": request URL does not match frozen source evidence")
    attempt_count, attempt_errors = validate_trace_attempt_records(trace, label)
    errors.extend(attempt_errors)
    try:
        ledger_attempt_count = int(row.get("attempt_count", "-1"))
    except ValueError:
        ledger_attempt_count = -1
    if attempt_count != ledger_attempt_count:
        errors.append(label + ": attempt count does not match ledger")
    attempts = trace.get("attempts")
    if isinstance(attempts, list) and attempts and isinstance(attempts[-1], Mapping):
        if attempts[-1].get("response_status") != row.get("response_status"):
            errors.append(label + ": terminal response does not match ledger")
        if attempts[-1].get("final_uri") != row.get("retrieval_uri"):
            errors.append(label + ": final URI does not match ledger")
        if attempts[0].get("accessed_at_utc") != row.get("accessed_at_utc"):
            errors.append(label + ": access timestamp does not match ledger")
        source_checksum = row.get("source_checksum")
        if (
            row.get("acquisition_status") == "ACQUIRED"
            and attempts[-1].get("payload_checksum") != source_checksum
        ):
            errors.append(label + ": payload checksum does not match ledger")
    return sorted(set(errors))


def validate_metadata_cache_record(
    cache_name: str,
    trace: Mapping[str, object],
    body: bytes,
    queue: Sequence[Mapping[str, object]],
) -> List[str]:
    """Bind one metadata cache record to its frozen request and queue identities."""

    label = "metadata trace " + cache_name
    errors: List[str] = []
    if cache_name == "arxiv-exact-identities":
        records = [record for record in queue if record.get("arxiv_id") != "UNKNOWN"]
        expected_request = build_arxiv_metadata_request(
            [str(record["arxiv_id"]) for record in records]
        )
        expected_identities = {str(record["paper_id"]) for record in records}
        try:
            observed_identities = set(parse_arxiv_metadata_entries(body, queue))
        except ValueError as error:
            errors.append(label + ": response identity validation failed: " + str(error))
            observed_identities = set()
    elif cache_name == "openalex-exact-dois":
        records = [
            record
            for record in queue
            if record.get("arxiv_id") == "UNKNOWN" and record.get("doi") != "UNKNOWN"
        ]
        expected_request = build_openalex_location_request(
            [str(record["doi"]) for record in records]
        )
        expected_identities = {
            normalize_doi_identity_value(record["doi"]) for record in records
        }
        try:
            document = json.loads(body)
            results = document.get("results") if isinstance(document, Mapping) else None
            if not isinstance(results, list):
                raise ValueError("OpenAlex payload lacks a results list")
            observed_identities = {
                normalize_doi_identity_value(result.get("doi"))
                for result in results
                if isinstance(result, Mapping)
            }
            parse_openalex_location_entries(body, queue)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            errors.append(label + ": response identity validation failed: " + str(error))
            observed_identities = set()
    else:
        return [label + ": cache name is not part of the frozen G04 request set"]

    if trace.get("request_url") != expected_request["url"]:
        errors.append(label + ": request_url does not match frozen request")
    if observed_identities != expected_identities:
        errors.append(label + ": response identity set does not match exact queue")
    attempt_count, attempt_errors = validate_trace_attempt_records(trace, label)
    errors.extend(attempt_errors)
    body_checksum = hashlib.sha256(body).hexdigest().upper()
    if trace.get("response_checksum") != body_checksum:
        errors.append(label + ": response checksum does not match body")
    attempts = trace.get("attempts")
    if isinstance(attempts, list) and attempts and isinstance(attempts[-1], Mapping):
        if trace.get("final_url") != attempts[-1].get("final_uri"):
            errors.append(label + ": final URL does not match terminal attempt")
        if trace.get("redirects") != attempts[-1].get("redirects"):
            errors.append(label + ": redirects do not match terminal attempt")
        final_host = (
            urllib.parse.urlsplit(str(trace.get("final_url", ""))).hostname or ""
        ).casefold()
        if final_host not in AUTHORIZED_METADATA_HOSTS_BY_CACHE[cache_name]:
            errors.append(label + ": final URL is not an authorized service host")
        if attempts[-1].get("response_status") == "HTTP_200" and (
            attempts[-1].get("payload_checksum") != body_checksum
        ):
            errors.append(label + ": terminal payload checksum does not match body")
    if attempt_count < 1:
        errors.append(label + ": completed metadata cache requires an attempt")
    return sorted(set(errors))


def derive_frozen_paper_request_urls(reference_root: Path) -> Dict[str, str]:
    """Recover exact paper request URLs from checksummed metadata bodies."""

    metadata_root = reference_root / "cache" / "g04" / "metadata"
    if not metadata_root.is_dir():
        return {}
    queue = derive_exact_queue_records(reference_root)
    source_records: Dict[str, Dict[str, str]] = {}
    arxiv_body = metadata_root / "arxiv-exact-identities.body"
    if arxiv_body.is_file():
        source_records.update(parse_arxiv_metadata_entries(arxiv_body.read_bytes(), queue))
    openalex_body = metadata_root / "openalex-exact-dois.body"
    if openalex_body.is_file():
        source_records.update(
            parse_openalex_location_entries(openalex_body.read_bytes(), queue)
        )
    return {
        paper_id: str(source_record["retrieval_uri"])
        for paper_id, source_record in source_records.items()
    }


def inspect_cached_metadata_request_evidence(
    reference_root: Path,
) -> Tuple[int, List[str]]:
    """Validate cached metadata bodies and traces and count real attempts."""

    metadata_root = reference_root / "cache" / "g04" / "metadata"
    if not metadata_root.exists():
        return 0, []
    errors: List[str] = []
    total_attempts = 0
    trace_names: set[str] = set()
    for trace_path in sorted(metadata_root.glob("*.trace.json")):
        cache_name = trace_path.name[: -len(".trace.json")]
        trace_names.add(cache_name)
        label = "metadata trace " + cache_name
        try:
            trace = json.loads(trace_path.read_text(encoding="utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            errors.append(label + ": trace JSON is malformed: " + str(error))
            continue
        if not isinstance(trace, Mapping):
            errors.append(label + ": trace must be an object")
            continue
        if trace.get("goal_id") != "G04":
            errors.append(label + ": goal_id must be G04")
        if trace.get("cache_name") != cache_name:
            errors.append(label + ": cache_name does not match path")
        body_path = metadata_root / (cache_name + ".body")
        if not body_path.is_file() or body_path.is_symlink():
            errors.append(label + ": cached body is missing")
            continue
        queue = derive_exact_queue_records(reference_root)
        total_attempts += len(trace.get("attempts", [])) if isinstance(trace.get("attempts"), list) else 0
        errors.extend(
            validate_metadata_cache_record(
                cache_name, trace, body_path.read_bytes(), queue
            )
        )
    for body_path in sorted(metadata_root.glob("*.body")):
        if body_path.name[: -len(".body")] not in trace_names:
            errors.append("metadata body {0}: trace is missing".format(body_path.name))
    return total_attempts, sorted(set(errors))


def validate_campaign_request_evidence(
    reference_root: Path,
    ledger_rows: Sequence[Mapping[str, str]],
) -> List[str]:
    """Validate metadata evidence and the aggregate G04 request ceiling."""

    metadata_attempts, errors = inspect_cached_metadata_request_evidence(reference_root)
    paper_attempts = 0
    for row in ledger_rows:
        try:
            paper_attempts += int(row.get("attempt_count", "0"))
        except ValueError:
            errors.append("download ledger attempt_count is invalid")
    if paper_attempts + metadata_attempts > MAXIMUM_HTTP_ATTEMPTS:
        errors.append("campaign request evidence exceeds the 220-attempt global cap")
    return sorted(set(errors))


def validate_download_ledger_rows(
    rows: Sequence[Mapping[str, str]],
    queue: Sequence[Mapping[str, object]],
) -> List[str]:
    """Validate exact terminal G04 ledger rows against the queue."""

    errors: List[str] = []
    queue_by_rank = {int(record["queue_rank"]): str(record["paper_id"]) for record in queue}
    if len(queue_by_rank) != EXPECTED_QUEUE_SIZE:
        errors.append("queue must contain exactly 50 unique ranks")
    if len(rows) != EXPECTED_QUEUE_SIZE:
        errors.append("download ledger must contain exactly 50 terminal rows")
    seen_ranks: set[int] = set()
    seen_papers: set[str] = set()
    total_attempts = 0
    total_bytes = 0
    for row_number, row in enumerate(rows, start=1):
        prefix = "download-ledger row {0}: ".format(row_number)
        missing_fields = [field for field in DOWNLOAD_LEDGER_FIELDS if field not in row]
        if missing_fields:
            errors.append(prefix + "missing fields " + ",".join(missing_fields))
            continue
        try:
            rank = int(row["queue_rank"])
            attempts = int(row["attempt_count"])
            content_bytes = int(row["content_length_bytes"])
            page_count = int(row["page_count"])
        except ValueError:
            errors.append(prefix + "integer field is invalid")
            continue
        total_attempts += attempts
        total_bytes += max(content_bytes, 0)
        if rank in seen_ranks:
            errors.append(prefix + "duplicate queue rank")
        seen_ranks.add(rank)
        if row["paper_id"] in seen_papers:
            errors.append(prefix + "duplicate paper_id")
        seen_papers.add(row["paper_id"])
        if queue_by_rank.get(rank) != row["paper_id"]:
            errors.append(prefix + "paper_id does not match exact queue rank")
        if row["request_id"] != "REQ-G04-{0:04d}".format(rank):
            errors.append(prefix + "request_id does not match queue rank")
        if row["goal_id"] != "G04":
            errors.append(prefix + "goal_id must be G04")
        if row["source_service"] not in ALLOWED_SOURCE_SERVICES:
            errors.append(prefix + "source_service is invalid")
        if row["license_state"] not in ALLOWED_LICENSE_STATES:
            errors.append(prefix + "license_state is invalid")
        if row["acquisition_status"] not in ALLOWED_ACQUISITION_STATUSES:
            errors.append(prefix + "acquisition_status is invalid")
        if row["parse_status"] not in ALLOWED_PARSE_STATUSES:
            errors.append(prefix + "parse_status is invalid")
        if row["cache_status"] not in ALLOWED_CACHE_STATUSES:
            errors.append(prefix + "cache_status is invalid")
        if attempts < 0 or attempts > MAXIMUM_PAPER_ATTEMPTS:
            errors.append(prefix + "attempt_count exceeds per-paper cap")
        if attempts > 0:
            if row["trace_path"] == "NOT_AVAILABLE":
                errors.append(prefix + "attempted request requires trace_path")
            if CHECKSUM_PATTERN.fullmatch(row["trace_checksum"]) is None:
                errors.append(prefix + "trace_checksum must be an uppercase SHA-256")
        if content_bytes < 0 or content_bytes > MAXIMUM_PDF_BYTES:
            errors.append(prefix + "content_length_bytes exceeds per-PDF cap")
        acquired = row["acquisition_status"] == "ACQUIRED"
        if acquired:
            for field in ("source_checksum", "trace_checksum"):
                if CHECKSUM_PATTERN.fullmatch(row[field]) is None:
                    errors.append(prefix + field + " must be an uppercase SHA-256")
            if row["parse_status"] == "PARSED":
                if CHECKSUM_PATTERN.fullmatch(row["extracted_checksum"]) is None:
                    errors.append(prefix + "extracted_checksum must be an uppercase SHA-256")
                if page_count < 1:
                    errors.append(prefix + "parsed PDF requires at least one page")
            elif row["parse_status"] != "PARSE_FAILED":
                errors.append(prefix + "acquired PDF requires PARSED or PARSE_FAILED")
            if row["local_path"] == "NOT_ACQUIRED":
                errors.append(prefix + "acquired PDF requires a local_path")
        else:
            if row["parse_status"] != "NOT_APPLICABLE":
                errors.append(prefix + "unavailable acquisition requires NOT_APPLICABLE parse_status")
            if page_count != 0:
                errors.append(prefix + "unavailable acquisition requires zero pages")
            if row["local_path"] != "NOT_ACQUIRED":
                errors.append(prefix + "unavailable acquisition must not have local content")
    if seen_ranks != set(range(1, EXPECTED_QUEUE_SIZE + 1)):
        errors.append("download ledger ranks must be contiguous 1 through 50")
    if total_attempts > MAXIMUM_HTTP_ATTEMPTS:
        errors.append("download ledger exceeds global request cap")
    if total_bytes > MAXIMUM_TOTAL_PDF_BYTES:
        errors.append("download ledger exceeds total local PDF byte cap")
    return sorted(set(errors))


def write_tsv_rows_atomic(
    path: Path,
    fieldnames: Sequence[str],
    rows: Sequence[Mapping[str, str]],
) -> None:
    """Write deterministic TSV rows through an atomic temporary path."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".part")
    with temporary_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
            delimiter="\t",
            lineterminator="\n",
            extrasaction="raise",
        )
        writer.writeheader()
        writer.writerows(rows)
    temporary_path.replace(path)


def count_cached_metadata_attempts(reference_root: Path) -> int:
    """Count preserved shared-metadata attempts for cap reconciliation."""

    total, errors = inspect_cached_metadata_request_evidence(reference_root)
    if errors:
        raise ValueError("; ".join(errors))
    return total


def render_g04_acquisition_report(
    queue: Sequence[Mapping[str, object]],
    ledger_rows: Sequence[Mapping[str, str]],
    campaign_requests: int,
) -> bytes:
    """Render the deterministic mechanical G04 completion report."""

    acquired = [row for row in ledger_rows if row["acquisition_status"] == "ACQUIRED"]
    parsed = [row for row in acquired if row["parse_status"] == "PARSED"]
    unavailable = [row for row in ledger_rows if row["acquisition_status"] != "ACQUIRED"]
    queue_by_id = {str(record["paper_id"]): record for record in queue}
    lines = [
        "# G04 Acquisition And Parsing Report",
        "",
        "## Executive Result",
        "",
        "G04 terminally reconciled the exact 50-paper G03 queue without semantic reading.",
        "Every successful full-text acquisition was validated, checksummed, mechanically",
        "parsed with the pinned parser, and kept in ignored local storage.",
        "",
        "## Campaign Accounting",
        "",
        "| Measure | Count |",
        "|---|---:|",
        "| Exact queue identities | {0} |".format(len(queue)),
        "| Terminal ledger rows | {0} |".format(len(ledger_rows)),
        "| External HTTP attempts including shared metadata | {0} |".format(campaign_requests),
        "| Successfully acquired PDFs | {0} |".format(len(acquired)),
        "| Successfully parsed texts | {0} |".format(len(parsed)),
        "| Acquired but parse-failed PDFs | {0} |".format(len(acquired) - len(parsed)),
        "| Unavailable or rejected identities | {0} |".format(len(unavailable)),
        "| Total accepted PDF bytes | {0} |".format(
            sum(int(row["content_length_bytes"]) for row in acquired)
        ),
        "",
        "## Exact Terminal Results",
        "",
        "| Rank | Paper | Acquisition | Parse | License | Terminal reason |",
        "|---:|---|---|---|---|---|",
    ]
    for row in ledger_rows:
        lines.append(
            "| {rank} | `{paper}` | `{acquisition}` | `{parse}` | `{license}` | `{reason}` |".format(
                rank=row["queue_rank"],
                paper=row["paper_id"],
                acquisition=row["acquisition_status"],
                parse=row["parse_status"],
                license=row["license_state"],
                reason=row["terminal_reason"],
            )
        )
    lines.extend(
        (
            "",
            "## Exact G05-Eligible Parsed Subset",
            "",
            "Only the following acquired-and-parsed identities are eligible for later",
            "G05 semantic reading. This list makes no mechanism or architecture claim.",
            "",
            "| Queue rank | Paper | Title | PDF SHA-256 | Text SHA-256 |",
            "|---:|---|---|---|---|",
        )
    )
    for row in parsed:
        record = queue_by_id[row["paper_id"]]
        title = str(record["title"]).replace("|", "\\|")
        lines.append(
            "| {rank} | `{paper}` | {title} | `{pdf}` | `{text}` |".format(
                rank=row["queue_rank"],
                paper=row["paper_id"],
                title=title,
                pdf=row["source_checksum"],
                text=row["extracted_checksum"],
            )
        )
    no_direct_source_count = sum(
        row["acquisition_status"] == "UNAVAILABLE" for row in ledger_rows
    )
    authorization_failure_count = sum(
        row["acquisition_status"] == "AUTHORIZATION_FAILED" for row in ledger_rows
    )
    unknown_license_count = sum(
        row["license_state"] == "LICENSE_UNKNOWN" for row in acquired
    )
    title_variant_ids = [
        row["paper_id"]
        for row in ledger_rows
        if "METADATA_TITLE_VARIANT" in row["terminal_reason"]
    ]
    lines.extend(
        (
            "",
            "## Preserved Terminal Limits",
            "",
            "- {0} identities have no acceptable direct source; none was silently substituted.".format(
                no_direct_source_count
            ),
            "- {0} attempted publisher retrieval ended in authorization failure; its HTTP trace is preserved.".format(
                authorization_failure_count
            ),
            "- {0} acquired papers remain `LICENSE_UNKNOWN`; no license URI was fabricated.".format(
                unknown_license_count
            ),
            "- Exact-ID metadata title variants: {0}.".format(
                ", ".join("`{0}`".format(paper_id) for paper_id in title_variant_ids)
                if title_variant_ids
                else "none"
            ),
            "",
            "## Reproducibility",
            "",
            "- Queue identities are derived from the G02 seed and G03 screening ledgers.",
            "- PDF, trace, and extracted-text paths are checksum-linked in the terminal ledger.",
            "- Parser: `pypdf=={0}` with `layout;page-separator=LF-FF-LF`.".format(
                PINNED_PARSER_VERSION
            ),
            "- Network-disabled replay validates local evidence and regenerates this report,",
            "  the manifest, and the terminal ledger without changing their bytes.",
            "",
            "## Scope Boundary",
            "",
            "G04 performed acquisition, validation, and mechanical text extraction only.",
            "It did not summarize papers, interpret claims, create evidence cards, propose",
            "architectures, design experiments, acquire repositories, or begin G05.",
            "",
        )
    )
    return "\n".join(lines).encode("utf-8")


def execute_g04_acquisition_campaign(
    reference_root: Path,
    *,
    allow_network: bool,
    request_function: Callable[[Mapping[str, object]], Mapping[str, object]] | None = None,
    sleep_function: Callable[[float], None] = time.sleep,
    clock_function: Callable[[], str] = read_current_utc_timestamp,
) -> Dict[str, int]:
    """Execute or replay the exact bounded G04 acquisition campaign."""

    reference_root = reference_root.resolve()
    queue = derive_exact_queue_records(reference_root)
    ledger_path = reference_root / "sources" / "download-ledger.tsv"
    manifest_path = reference_root / "sources" / "paper-manifest.tsv"
    report_path = reference_root / "sources" / "G04-acquisition-parsing-report.md"
    manifest_rows = read_tsv_rows_exact(manifest_path)
    if not manifest_rows:
        raise ValueError("paper manifest cannot be empty")
    read_complete_ids = {
        row.get("paper_id", "")
        for row in manifest_rows
        if row.get("selection_status") == "READ_COMPLETE"
    }
    manifest_fields = tuple(manifest_rows[0])
    ledger_rows = read_tsv_rows_exact(ledger_path) if ledger_path.is_file() else []
    partial_errors = validate_partial_terminal_rows(ledger_rows, queue)
    if partial_errors:
        raise ValueError("; ".join(partial_errors))
    local_errors = validate_local_artifact_records(reference_root, ledger_rows)
    if local_errors:
        raise ValueError("; ".join(local_errors))

    existing_by_id = {row["paper_id"]: row for row in ledger_rows}
    existing_attempts = sum(int(row["attempt_count"]) for row in ledger_rows)
    metadata_attempts = count_cached_metadata_attempts(reference_root)
    if existing_attempts + metadata_attempts > MAXIMUM_HTTP_ATTEMPTS:
        raise ValueError("campaign request evidence exceeds the 220-attempt global cap")
    state: MutableMapping[str, object] = {
        "campaign_requests": existing_attempts + metadata_attempts,
        "invocation_requests": 0,
        "metadata_cache_hits": 0,
        "stopped_hosts": derive_stopped_request_hosts(ledger_rows),
    }
    cache_hits = sum(
        row["acquisition_status"] == "ACQUIRED" for row in ledger_rows
    )

    if len(ledger_rows) < EXPECTED_QUEUE_SIZE:
        if not allow_network:
            raise RuntimeError("network-disabled replay requires 50 terminal ledger rows")
        base_manifest = project_manifest_before_g04(manifest_rows, queue)
        write_tsv_rows_atomic(
            manifest_path,
            manifest_fields,
            mark_selected_papers_deep(base_manifest, queue),
        )
        transport = request_function or execute_bounded_http_request
        sources, terminal_reasons = resolve_queue_source_records(
            reference_root,
            queue,
            state,
            allow_network=True,
            request_function=transport,
            sleep_function=sleep_function,
            clock_function=clock_function,
        )
        rows_by_id = dict(existing_by_id)
        for record in queue:
            paper_id = str(record["paper_id"])
            if paper_id in rows_by_id:
                continue
            if paper_id not in sources:
                status, reason = terminal_reasons[paper_id]
                row = build_unavailable_ledger_row(
                    record,
                    acquisition_status=status,
                    terminal_reason=reason,
                )
            else:
                try:
                    row = acquire_single_paper_source(
                        reference_root,
                        record,
                        sources[paper_id],
                        state,
                        request_function=transport,
                        sleep_function=sleep_function,
                        clock_function=clock_function,
                    )
                except ServiceStopError as error:
                    row = build_unavailable_ledger_row(
                        record,
                        acquisition_status="SERVICE_STOPPED",
                        terminal_reason=normalize_terminal_reason_token(error),
                    )
            rows_by_id[paper_id] = row
            ledger_rows = [
                rows_by_id[str(queue_record["paper_id"])]
                for queue_record in queue
                if str(queue_record["paper_id"]) in rows_by_id
            ]
            write_tsv_rows_atomic(
                ledger_path, DOWNLOAD_LEDGER_FIELDS, ledger_rows
            )

    if len(ledger_rows) != EXPECTED_QUEUE_SIZE:
        ledger_rows = read_tsv_rows_exact(ledger_path)
    ledger_errors = validate_download_ledger_rows(ledger_rows, queue)
    if ledger_errors:
        raise ValueError("; ".join(ledger_errors))
    local_errors = validate_local_artifact_records(reference_root, ledger_rows)
    if local_errors:
        raise ValueError("; ".join(local_errors))

    base_manifest = project_manifest_before_g04(manifest_rows, queue)
    completed_manifest = update_g04_manifest_rows(base_manifest, ledger_rows, queue)
    for row in completed_manifest:
        if (
            row.get("paper_id") in read_complete_ids
            and row.get("selection_status") == "DEEP_READ"
        ):
            row["selection_status"] = "READ_COMPLETE"
    manifest_errors = validate_g04_manifest_rows(
        completed_manifest,
        ledger_rows,
        queue,
        require_complete=True,
        allow_read_complete=bool(read_complete_ids),
    )
    if manifest_errors:
        raise ValueError("; ".join(manifest_errors))
    write_tsv_rows_atomic(ledger_path, DOWNLOAD_LEDGER_FIELDS, ledger_rows)
    write_tsv_rows_atomic(manifest_path, manifest_fields, completed_manifest)
    campaign_requests = sum(int(row["attempt_count"]) for row in ledger_rows) + count_cached_metadata_attempts(reference_root)
    write_atomic_binary_file(
        report_path,
        render_g04_acquisition_report(queue, ledger_rows, campaign_requests),
    )

    acquired = sum(row["acquisition_status"] == "ACQUIRED" for row in ledger_rows)
    parsed = sum(row["parse_status"] == "PARSED" for row in ledger_rows)
    return {
        "queue_size": len(queue),
        "terminal_rows": len(ledger_rows),
        "acquired": acquired,
        "parsed": parsed,
        "unavailable": len(ledger_rows) - acquired,
        "external_requests": int(state["invocation_requests"]),
        "campaign_requests": campaign_requests,
        "cache_hits": cache_hits,
    }


def main() -> int:
    """Run the bounded campaign or a network-disabled replay."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("arxiv-reference"))
    parser.add_argument("--allow-network", action="store_true")
    arguments = parser.parse_args()
    result = execute_g04_acquisition_campaign(
        arguments.root, allow_network=arguments.allow_network
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
