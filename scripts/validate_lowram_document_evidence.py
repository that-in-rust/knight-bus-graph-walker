#!/usr/bin/env python3
"""Validate exhaustive PRD03-PRD06 architecture-corpus evidence."""

from __future__ import annotations

import csv
from pathlib import Path
import sys


ALLOWED_COVERAGE_STATUSES = {
    "semantic_read",
    "structured_queried",
    "generated_classified",
    "binary_inspected",
    "superseded_classified",
}

EXPECTED_AGENT_FILES = {
    "agent-04": "agent-04-prd03-files.tsv",
    "agent-05": "agent-05-prd04-files.tsv",
    "agent-06": "agent-06-prd05-prd06-files.tsv",
}

EXPECTED_EVIDENCE_PREFIXES = {
    "agent-04": "A04-",
    "agent-05": "A05-",
    "agent-06": "A06-",
}

REQUIRED_AGENT_FIELDS = {
    "lane",
    "path",
    "sha256",
    "bytes",
    "extension",
    "file_class",
    "coverage_status",
    "relevance",
    "architecture_option_ids",
    "evidence_id",
}


class DocumentEvidenceError(RuntimeError):
    """Raised when the document-corpus proof is incomplete or inconsistent."""


def load_document_tsv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.is_file():
        raise DocumentEvidenceError(f"missing evidence file: {path}")
    with path.open("r", encoding="utf-8", newline="") as source_file:
        reader = csv.DictReader(source_file, delimiter="\t")
        return reader.fieldnames or [], list(reader)


def index_document_rows_now(
    rows: list[dict[str, str]],
    source_name: str,
) -> dict[str, dict[str, str]]:
    indexed: dict[str, dict[str, str]] = {}
    duplicates: list[str] = []
    for row in rows:
        path = row.get("path", "")
        if path in indexed:
            duplicates.append(path)
        indexed[path] = row
    if duplicates:
        raise DocumentEvidenceError(
            f"{source_name} has {len(duplicates)} duplicate paths; examples={duplicates[:10]}"
        )
    return indexed


def validate_document_headers_now(headers: list[str], source_name: str) -> None:
    missing_fields = sorted(REQUIRED_AGENT_FIELDS - set(headers))
    if missing_fields:
        raise DocumentEvidenceError(f"{source_name} is missing fields: {missing_fields}")


def validate_document_row_now(row: dict[str, str], assigned_agent: str) -> None:
    path = row.get("path", "")
    coverage_status = row.get("coverage_status", "")
    if coverage_status not in ALLOWED_COVERAGE_STATUSES:
        raise DocumentEvidenceError(f"{path}: illegal coverage_status={coverage_status!r}")
    try:
        relevance = int(row.get("relevance", ""))
    except ValueError as error:
        raise DocumentEvidenceError(f"{path}: relevance must be an integer") from error
    if not 1 <= relevance <= 100:
        raise DocumentEvidenceError(f"{path}: relevance must be between 1 and 100")
    if relevance >= 80 and coverage_status not in {"semantic_read", "structured_queried"}:
        raise DocumentEvidenceError(
            f"{path}: relevance {relevance} requires semantic_read or structured_queried"
        )
    evidence_id = row.get("evidence_id", "")
    if not evidence_id.startswith(EXPECTED_EVIDENCE_PREFIXES[assigned_agent]):
        raise DocumentEvidenceError(
            f"{path}: evidence_id {evidence_id!r} does not match {assigned_agent}"
        )
    if not row.get("file_class", "").strip():
        raise DocumentEvidenceError(f"{path}: file_class is empty")
    if not row.get("architecture_option_ids", "").strip():
        raise DocumentEvidenceError(f"{path}: architecture_option_ids is empty; use NONE when irrelevant")


def validate_document_classification_now(
    denominator: dict[str, str],
    evidence: dict[str, str],
) -> None:
    path = denominator["path"]
    default_class = denominator["default_file_class"]
    evidence_class = evidence["file_class"]
    status = evidence["coverage_status"]
    if default_class == "semantic_text_candidate":
        if evidence_class.startswith("generated_") or evidence_class == "tool_diagnostic_output":
            allowed_statuses = {"semantic_read", "structured_queried", "generated_classified"}
        else:
            allowed_statuses = {"semantic_read", "structured_queried", "superseded_classified"}
        if status not in allowed_statuses:
            raise DocumentEvidenceError(
                f"{path}: semantic text class {evidence_class!r} has incompatible status {status}"
            )
    if default_class == "bulk_text_candidate" and status not in {
        "semantic_read", "structured_queried", "generated_classified"
    }:
        raise DocumentEvidenceError(f"{path}: bulk text has incompatible status {status}")
    if default_class == "generated_structured_candidate" and status not in {
        "semantic_read", "structured_queried", "generated_classified"
    }:
        raise DocumentEvidenceError(f"{path}: generated structured file has incompatible status {status}")
    if default_class in {"structured_database_candidate", "structured_workbook_candidate"} and status not in {
        "structured_queried", "binary_inspected"
    }:
        raise DocumentEvidenceError(f"{path}: structured binary has incompatible status {status}")
    if default_class == "source_script_candidate" and status not in {
        "semantic_read", "structured_queried"
    }:
        raise DocumentEvidenceError(f"{path}: source script has incompatible status {status}")


def validate_document_union_now(
    denominator_rows: list[dict[str, str]],
    evidence_rows: list[dict[str, str]],
) -> None:
    denominator_index = index_document_rows_now(denominator_rows, "document denominator")
    evidence_index = index_document_rows_now(evidence_rows, "document evidence union")
    denominator_paths = set(denominator_index)
    evidence_paths = set(evidence_index)
    missing_paths = sorted(denominator_paths - evidence_paths)
    unexpected_paths = sorted(evidence_paths - denominator_paths)
    if missing_paths or unexpected_paths:
        raise DocumentEvidenceError(
            f"coverage mismatch: missing={len(missing_paths)} {missing_paths[:10]}; "
            f"unexpected={len(unexpected_paths)} {unexpected_paths[:10]}"
        )
    for path, denominator in denominator_index.items():
        evidence = evidence_index[path]
        for field_name in ("lane", "sha256", "bytes", "extension"):
            if evidence.get(field_name, "") != denominator[field_name]:
                raise DocumentEvidenceError(
                    f"{path}: {field_name} mismatch; expected={denominator[field_name]!r}, "
                    f"actual={evidence.get(field_name, '')!r}"
                )
        validate_document_classification_now(denominator, evidence)


def print_document_coverage_summary(evidence_rows: list[dict[str, str]]) -> None:
    status_counts: dict[str, int] = {}
    agent_counts: dict[str, int] = {}
    for row in evidence_rows:
        status = row["coverage_status"]
        status_counts[status] = status_counts.get(status, 0) + 1
        agent = row["_assigned_agent"]
        agent_counts[agent] = agent_counts.get(agent, 0) + 1
    print(f"PASS: reconciled {len(evidence_rows)} document evidence rows")
    for agent, count in sorted(agent_counts.items()):
        print(f"{agent}: {count} rows")
    for status, count in sorted(status_counts.items()):
        print(f"{status}: {count} rows")


def run_document_validation_now() -> int:
    workspace_root = Path(__file__).resolve().parents[1]
    evidence_root = (
        workspace_root
        / "docs_PRD04"
        / "reference-learning"
        / "lowram-architecture-corpus"
        / "evidence"
    )
    denominator_headers, denominator_rows = load_document_tsv_rows(
        evidence_root / "all-documents-denominator.tsv"
    )
    required_denominator_fields = {
        "lane", "path", "sha256", "bytes", "extension",
        "default_file_class", "assigned_agent",
    }
    missing_denominator_fields = sorted(required_denominator_fields - set(denominator_headers))
    if missing_denominator_fields:
        raise DocumentEvidenceError(
            f"denominator is missing fields: {missing_denominator_fields}"
        )
    denominator_assignments = {
        row["path"]: row["assigned_agent"] for row in denominator_rows
    }

    evidence_rows: list[dict[str, str]] = []
    evidence_ids: set[str] = set()
    for assigned_agent, filename in EXPECTED_AGENT_FILES.items():
        headers, rows = load_document_tsv_rows(evidence_root / filename)
        validate_document_headers_now(headers, filename)
        for row in rows:
            validate_document_row_now(row, assigned_agent)
            expected_agent = denominator_assignments.get(row["path"])
            if expected_agent is not None and expected_agent != assigned_agent:
                raise DocumentEvidenceError(
                    f"{row['path']}: belongs to {expected_agent}, found in {assigned_agent}"
                )
            evidence_id = row["evidence_id"]
            if evidence_id in evidence_ids:
                raise DocumentEvidenceError(f"duplicate document evidence ID: {evidence_id}")
            evidence_ids.add(evidence_id)
            row["_assigned_agent"] = assigned_agent
        evidence_rows.extend(rows)

    validate_document_union_now(denominator_rows, evidence_rows)
    print_document_coverage_summary(evidence_rows)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(run_document_validation_now())
    except DocumentEvidenceError as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
