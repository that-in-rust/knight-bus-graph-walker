#!/usr/bin/env python3
"""Reconcile Neo4j-family evidence rows against the immutable file denominator."""

from __future__ import annotations

import csv
from pathlib import Path
import re
import sys


ALLOWED_COVERAGE_STATUSES = {
    "graph_indexed",
    "direct_read",
    "generated_classified",
    "noncode_classified",
    "binary_classified",
}

EXPECTED_AGENT_FILES = {
    "agent-01": "agent-01-files.tsv",
    "agent-02": "agent-02-files.tsv",
    "agent-03": "agent-03-files.tsv",
}

REQUIRED_AGENT_FIELDS = {
    "repo",
    "path",
    "git_blob",
    "bytes",
    "extension",
    "file_class",
    "coverage_status",
    "relevance",
    "evidence_id",
}

FOUNDER_CRITICAL_PATTERN = re.compile(
    r"(?:memory(?:estimate|estimation|usage)|working.?set|admission|planner|"
    r"bolt.?protocol|cypher.?parser|testkit|tck|pagerank|shortest.?path|"
    r"node.?similarity|louvain|leiden|triangle|fastrp|weakly.?connected|wcc)",
    re.IGNORECASE,
)


class EvidenceValidationError(RuntimeError):
    """Raised when the evidence union cannot prove complete coverage."""


def load_tsv_rows_checked(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.is_file():
        raise EvidenceValidationError(f"missing evidence file: {path}")
    with path.open("r", encoding="utf-8", newline="") as source_file:
        reader = csv.DictReader(source_file, delimiter="\t")
        headers = reader.fieldnames or []
        rows = list(reader)
    return headers, rows


def index_rows_by_identity(rows: list[dict[str, str]], source_name: str) -> dict[tuple[str, str], dict[str, str]]:
    indexed: dict[tuple[str, str], dict[str, str]] = {}
    duplicates: list[tuple[str, str]] = []
    for row in rows:
        key = (row.get("repo", ""), row.get("path", ""))
        if key in indexed:
            duplicates.append(key)
        indexed[key] = row
    if duplicates:
        examples = ", ".join(f"{repo}/{path}" for repo, path in duplicates[:10])
        raise EvidenceValidationError(
            f"{source_name} has {len(duplicates)} duplicate repo/path keys; examples: {examples}"
        )
    return indexed


def validate_agent_headers_now(headers: list[str], source_name: str) -> None:
    missing_fields = sorted(REQUIRED_AGENT_FIELDS - set(headers))
    if missing_fields:
        raise EvidenceValidationError(f"{source_name} is missing fields: {missing_fields}")


def validate_agent_row_contract(row: dict[str, str], assigned_agent: str) -> None:
    identity = f"{row.get('repo', '')}/{row.get('path', '')}"
    coverage_status = row.get("coverage_status", "")
    if coverage_status not in ALLOWED_COVERAGE_STATUSES:
        raise EvidenceValidationError(f"{identity}: illegal coverage_status={coverage_status!r}")
    try:
        relevance = int(row.get("relevance", ""))
    except ValueError as error:
        raise EvidenceValidationError(f"{identity}: relevance must be an integer") from error
    if not 1 <= relevance <= 100:
        raise EvidenceValidationError(f"{identity}: relevance must be between 1 and 100")
    if not row.get("evidence_id", "").strip():
        raise EvidenceValidationError(f"{identity}: evidence_id is empty")
    if not row.get("file_class", "").strip():
        raise EvidenceValidationError(f"{identity}: file_class is empty")
    if relevance >= 80 and coverage_status != "direct_read":
        raise EvidenceValidationError(
            f"{identity}: relevance {relevance} requires direct_read, found {coverage_status} ({assigned_agent})"
        )


def validate_classification_pairing_now(denominator: dict[str, str], evidence: dict[str, str]) -> None:
    identity = f"{denominator['repo']}/{denominator['path']}"
    default_class = denominator["default_file_class"]
    coverage_status = evidence["coverage_status"]
    if default_class == "source_candidate" and coverage_status not in {"graph_indexed", "direct_read"}:
        raise EvidenceValidationError(
            f"{identity}: source candidate must be graph_indexed or direct_read, found {coverage_status}"
        )
    if default_class == "binary_classified" and coverage_status not in {"binary_classified", "direct_read"}:
        raise EvidenceValidationError(
            f"{identity}: binary candidate must be binary_classified or direct_read, found {coverage_status}"
        )
    if default_class == "generated_classified" and coverage_status not in {
        "generated_classified", "graph_indexed", "direct_read"
    }:
        raise EvidenceValidationError(
            f"{identity}: generated candidate has incompatible status {coverage_status}"
        )


def validate_denominator_coverage_now(
    denominator_rows: list[dict[str, str]],
    evidence_rows: list[dict[str, str]],
) -> None:
    denominator_index = index_rows_by_identity(denominator_rows, "denominator")
    evidence_index = index_rows_by_identity(evidence_rows, "evidence union")
    denominator_keys = set(denominator_index)
    evidence_keys = set(evidence_index)
    missing_keys = sorted(denominator_keys - evidence_keys)
    unexpected_keys = sorted(evidence_keys - denominator_keys)
    if missing_keys or unexpected_keys:
        missing_examples = [f"{repo}/{path}" for repo, path in missing_keys[:10]]
        unexpected_examples = [f"{repo}/{path}" for repo, path in unexpected_keys[:10]]
        raise EvidenceValidationError(
            "coverage mismatch: "
            f"missing={len(missing_keys)} {missing_examples}; "
            f"unexpected={len(unexpected_keys)} {unexpected_examples}"
        )

    for key, denominator in denominator_index.items():
        evidence = evidence_index[key]
        identity = f"{key[0]}/{key[1]}"
        for field_name in ("git_blob", "bytes", "extension"):
            if evidence.get(field_name, "") != denominator[field_name]:
                raise EvidenceValidationError(
                    f"{identity}: {field_name} mismatch; "
                    f"expected={denominator[field_name]!r}, actual={evidence.get(field_name, '')!r}"
                )
        if evidence.get("coverage_status") != "direct_read" and FOUNDER_CRITICAL_PATTERN.search(key[1]):
            if denominator["default_file_class"] == "source_candidate":
                raise EvidenceValidationError(
                    f"{identity}: founder-critical source path requires direct_read"
                )
        validate_classification_pairing_now(denominator, evidence)


def print_coverage_summary_now(evidence_rows: list[dict[str, str]]) -> None:
    status_counts: dict[str, int] = {}
    agent_counts: dict[str, int] = {}
    direct_bytes = 0
    for row in evidence_rows:
        status = row["coverage_status"]
        status_counts[status] = status_counts.get(status, 0) + 1
        if status == "direct_read":
            direct_bytes += int(row["bytes"])
        agent_name = row["_assigned_agent"]
        agent_counts[agent_name] = agent_counts.get(agent_name, 0) + 1
    print(f"PASS: reconciled {len(evidence_rows)} evidence rows")
    for agent_name, count in sorted(agent_counts.items()):
        print(f"{agent_name}: {count} rows")
    for status, count in sorted(status_counts.items()):
        print(f"{status}: {count} rows")
    print(f"direct_read_bytes: {direct_bytes}")


def run_evidence_validation_now() -> int:
    workspace_root = Path(__file__).resolve().parents[1]
    evidence_root = (
        workspace_root
        / "docs_PRD04"
        / "reference-learning"
        / "neo4j-compat-lowram"
        / "evidence"
    )
    denominator_headers, denominator_rows = load_tsv_rows_checked(
        evidence_root / "all-files-denominator.tsv"
    )
    required_denominator_fields = {
        "repo", "path", "git_blob", "bytes", "extension",
        "default_file_class", "assigned_agent",
    }
    missing_denominator_fields = sorted(required_denominator_fields - set(denominator_headers))
    if missing_denominator_fields:
        raise EvidenceValidationError(
            f"denominator is missing fields: {missing_denominator_fields}"
        )

    denominator_assignments = {
        (row["repo"], row["path"]): row["assigned_agent"] for row in denominator_rows
    }
    evidence_rows: list[dict[str, str]] = []
    seen_evidence_ids: set[str] = set()
    for assigned_agent, filename in EXPECTED_AGENT_FILES.items():
        headers, agent_rows = load_tsv_rows_checked(evidence_root / filename)
        validate_agent_headers_now(headers, filename)
        for row in agent_rows:
            validate_agent_row_contract(row, assigned_agent)
            identity = (row["repo"], row["path"])
            denominator_agent = denominator_assignments.get(identity)
            if denominator_agent is not None and denominator_agent != assigned_agent:
                raise EvidenceValidationError(
                    f"{identity[0]}/{identity[1]} belongs to {denominator_agent}, found in {assigned_agent}"
                )
            evidence_id = row["evidence_id"]
            if evidence_id in seen_evidence_ids:
                raise EvidenceValidationError(f"duplicate evidence_id: {evidence_id}")
            seen_evidence_ids.add(evidence_id)
            row["_assigned_agent"] = assigned_agent
        evidence_rows.extend(agent_rows)

    validate_denominator_coverage_now(denominator_rows, evidence_rows)
    print_coverage_summary_now(evidence_rows)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(run_evidence_validation_now())
    except EvidenceValidationError as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
