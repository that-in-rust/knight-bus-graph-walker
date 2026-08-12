#!/usr/bin/env python3
"""Deterministic G06 counterexample-card and coverage validation."""

from __future__ import annotations

import csv
import json
import hashlib
import re
from pathlib import Path
from typing import Dict, List, Mapping, Sequence, Set, Tuple


FAILURE_CARD_FIELDS = frozenset(
    {
        "failure_id",
        "name",
        "epistemic_label",
        "failure_basis",
        "source_paper_ids",
        "source_pointers",
        "broken_assumption",
        "triggering_workload",
        "observable_symptom",
        "breakpoint_equation",
        "affected_pattern_ids",
        "affected_architecture_ids",
        "adversarial_fixture",
        "expected_failure_signal",
        "repair_options",
        "confidence_rationale",
    }
)
CLAIM_OBJECT_FIELDS = frozenset(
    {
        "claim_type",
        "text",
        "source_pointer_ids",
        "premises",
        "assumptions",
        "uncertainty",
    }
)
SOURCE_POINTER_FIELDS = frozenset(
    {"pointer_id", "paper_id", "page", "locator_type", "locator_value", "claim_scope"}
)
BREAKPOINT_FIELDS = frozenset(
    {
        "claim_type",
        "expression",
        "variables",
        "numeric_constants",
        "source_pointer_ids",
        "premises",
        "assumptions",
        "uncertainty",
        "measurement_needed",
    }
)
BREAKPOINT_VARIABLE_FIELDS = frozenset({"symbol", "definition", "units"})
BREAKPOINT_CONSTANT_FIELDS = frozenset(
    {"literal", "units", "source_pointer_ids", "premises", "assumptions", "uncertainty"}
)
ADVERSARIAL_FIXTURE_FIELDS = frozenset(
    {
        "claim_type",
        "fixture_name",
        "fixture_kind",
        "graph_shape",
        "graph_scale",
        "workload",
        "controlled_variables",
        "varied_variables",
        "independent_oracle",
        "expected_observation",
        "source_pointer_ids",
        "premises",
        "assumptions",
        "uncertainty",
    }
)
FAILURE_BASES = {
    "SOURCE_REPORTED",
    "SOURCE_SUPPORTED_DERIVATION",
    "ANALYTICAL_COUNTEREXAMPLE",
}
LOCATOR_TYPES = {
    "SECTION",
    "FIGURE",
    "TABLE",
    "THEOREM",
    "LEMMA",
    "ALGORITHM",
    "EQUATION",
    "APPENDIX",
    "PARAGRAPH",
}
FIXTURE_KINDS = {"GRAPH", "EXECUTION_PROFILE", "GRAPH_AND_EXECUTION"}
REPAIR_CLASSES = {
    "ADD_ADMISSION_GUARD",
    "ADD_RESOURCE_BOUND",
    "ADD_FALLBACK_PATH",
    "SPECIALIZE_WORKLOAD",
    "CHANGE_SCHEDULE",
    "CHANGE_REPRESENTATION",
    "MEASURE_UNKNOWN",
}
ADVERSARIAL_PLAN_FIELDS = (
    "subject_type",
    "subject_rank",
    "lane_id",
    "lane_position",
    "subject_id",
    "source_paper_ids",
    "reader_agent_id",
    "reviewer_agent_id",
    "inspection_status",
    "terminal_disposition",
    "failure_ids",
    "evidence_gap",
    "measurement_needed",
    "reading_coverage",
    "result_checksum",
)
PAPER_DISPOSITIONS = {"NEGATIVE_EVIDENCE_EXTRACTED", "NO_NEGATIVE_EVIDENCE"}
PATTERN_DISPOSITIONS = {
    "SOURCE_FAILURE_LINKED",
    "ANALYTICAL_TEST_LINKED",
    "EXPLICIT_EVIDENCE_GAP",
}
EVIDENCE_CONFLICT_FIELDS = (
    "conflict_id",
    "left_evidence_type",
    "left_evidence_id",
    "right_evidence_type",
    "right_evidence_id",
    "conflict_type",
    "affected_pattern_ids",
    "claim_scope",
    "rationale",
    "epistemic_label",
    "source_paper_ids",
    "source_pointer_ids",
    "resolution_state",
)
CONFLICT_TYPES = {
    "CONDITION_REVERSAL",
    "BENCHMARK_DISAGREEMENT",
    "ASSUMPTION_MISMATCH",
    "BOUND_CONTRADICTION",
    "APPLICABILITY_DISAGREEMENT",
}
REVIEWED_SEMANTIC_MERGES = (
    (
        "FAIL-BINARY-QUANTIZATION-GEOMETRY-COLLAPSE",
        "FAIL-INCOMPATIBLE-GEOMETRY-COLLAPSES-RECALL",
    ),
    (
        "FAIL-REORDERING-PREPROCESSING-DOMINATES-TRAVERSAL",
        "FAIL-FULL-REORDER-DOMINATES-TRAVERSAL",
    ),
)
LANE_DOSSIER_FIELDS = frozenset(
    {
        "schema_version",
        "lane_id",
        "paper_results",
        "pattern_results",
        "failure_cards",
        "conflict_candidates",
        "coverage_audit",
        "lane_self_review",
    }
)
LANE_PAPER_RESULT_FIELDS = frozenset(
    {
        "subject_id",
        "page_count",
        "page_audit",
        "terminal_disposition",
        "proposed_failure_ids",
        "evidence_gap",
        "measurement_needed",
        "negative_evidence_notes",
    }
)
LANE_PATTERN_RESULT_FIELDS = frozenset(
    {
        "subject_id",
        "source_paper_ids",
        "terminal_disposition",
        "proposed_failure_ids",
        "evidence_gap",
        "measurement_needed",
        "required_assumption",
        "smallest_violating_workload",
        "triggering_graph_property",
        "unexpected_resource_term",
        "observable_symptom",
        "source_reported_breakpoint",
        "symbolic_breakpoint",
        "unknowns",
        "minimal_fixture",
        "independent_oracle",
        "failure_effect",
        "related_mechanisms",
    }
)
LANE_COVERAGE_FIELDS = frozenset(
    {
        "assigned_paper_ids",
        "completed_paper_ids",
        "assigned_pattern_ids",
        "completed_pattern_ids",
        "pages_expected",
        "pages_inspected",
        "missing_subject_ids",
        "duplicate_subject_ids",
        "network_requests",
        "repository_edits",
    }
)
LANE_SELF_REVIEW_FIELDS = frozenset(
    {
        "schema_valid_json",
        "all_subjects_terminal",
        "all_source_pointers_rechecked",
        "no_unsupported_numeric_breakpoints",
        "no_later_goal_artifacts",
        "known_uncertainties",
    }
)
G06_REPORT_HEADINGS = (
    "## Executive Result",
    "## Frozen Corpus And Scope",
    "## Negative Evidence Accounting",
    "## Failure Taxonomy",
    "## Pattern Coverage",
    "## Evidence Conflicts",
    "## A007 Decision Yield",
    "## Explicit Unknowns",
    "## Scope Boundary",
    "## Verification Handoff",
)
FROZEN_INPUTS = {
    "manifest": (377, "ac6dd076cf65b3ec8e6addc45b90111cb0ab4f14fe44f71d4c6e1cda4b8f3bfc"),
    "g05_plan": (25, "b8e942272218ecee670b97fdea601c802a2705505bef352b0c644a5d00f53c3f"),
    "pattern_edges": (47, "df677bdaca319de644d2f89ef6025bebd52ddac16d2c44dbe27fd3619719855e"),
    "mechanism_cards": (67, "1fb0b8e4e63a09c764cf8b5ff6b4de4c113e8fa843c12c296eed459d5f1a82d9"),
    "metadata_requests": (191, "29ab0c268a7e07931832cc43aff917cacb289058239df443e06f7de44cfa1718"),
    "citation_requests": (83, "da8a5ebaa536c2fc221a85fe48e537a319fcfac8142bacea05181317a9a223d7"),
    "download_requests": (50, "b5249dbbfed3b272fe01e9b6b4bb18eb41488470e2a69e9e89fa9918b3e2f337"),
}


def normalize_duplicate_signature_text(value: object) -> str:
    """Normalize one semantic field for duplicate-signature comparison."""

    return " ".join(str(value).casefold().split())


def split_pipe_values_sorted(value: object) -> List[str]:
    """Return sorted unique non-empty values from one pipe-delimited field."""

    return sorted({item for item in str(value).split("|") if item})


def read_tsv_records_exact(path: Path) -> List[Dict[str, str]]:
    """Read one UTF-8 TSV file with its exact committed header."""

    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def hash_file_bytes_sha256(path: Path) -> str:
    """Return lowercase SHA-256 for one regular file."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_failure_card_file(path: Path) -> Dict[str, object]:
    """Parse exactly one canonical JSON envelope from a failure-card file."""

    text = path.read_text(encoding="utf-8")
    matches = re.findall(r"```json\s*\n(.*?)\n```", text, flags=re.DOTALL)
    if len(matches) != 1:
        raise ValueError("failure card must contain exactly one fenced json envelope")
    payload = json.loads(matches[0])
    if not isinstance(payload, dict):
        raise ValueError("failure card envelope must be a JSON object")
    if payload.get("failure_id") != path.stem:
        raise ValueError("failure_id does not match the failure-card filename")
    return payload


def parse_mechanism_card_file(path: Path) -> Dict[str, object]:
    """Parse one already-validated G05 mechanism-card envelope."""

    text = path.read_text(encoding="utf-8")
    matches = re.findall(r"```json\s*\n(.*?)\n```", text, flags=re.DOTALL)
    if len(matches) != 1:
        raise ValueError("mechanism card must contain exactly one fenced json envelope")
    payload = json.loads(matches[0])
    if not isinstance(payload, dict) or payload.get("pattern_id") != path.stem:
        raise ValueError("mechanism card identity does not match its filename")
    return payload


def parse_lane_dossier_file(path: Path) -> Dict[str, object]:
    """Parse one UTF-8 JSON lane dossier and require an object envelope."""

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("lane dossier must be a JSON object")
    return payload


def derive_g06_corpus_records(reference_root: Path) -> Dict[str, object]:
    """Derive the complete frozen G05 input snapshot used by G06."""

    plan_path = reference_root / "governance" / "g05-reading-plan.tsv"
    manifest_path = reference_root / "sources" / "paper-manifest.tsv"
    edge_path = reference_root / "evidence" / "pattern-edges.tsv"
    plan_rows = read_tsv_records_exact(plan_path)
    manifest_rows = read_tsv_records_exact(manifest_path)
    edge_rows = read_tsv_records_exact(edge_path)
    page_counts: Dict[str, int] = {}
    source_hashes: Dict[str, Tuple[str, str]] = {}
    integrity_errors: List[str] = []
    for row in plan_rows:
        paper_id = row.get("paper_id", "")
        try:
            page_counts[paper_id] = int(row.get("page_count", ""))
        except ValueError:
            integrity_errors.append("g05 plan: invalid page_count for " + paper_id)
            continue
        pdf_path = reference_root / row.get("pdf_path", "")
        text_path = reference_root / row.get("text_path", "")
        expected_pdf = row.get("pdf_sha256", "").lower()
        expected_text = row.get("text_sha256", "").lower()
        actual_pdf = hash_file_bytes_sha256(pdf_path) if pdf_path.is_file() else "MISSING"
        actual_text = hash_file_bytes_sha256(text_path) if text_path.is_file() else "MISSING"
        if actual_pdf != expected_pdf:
            integrity_errors.append("g05 source PDF checksum drift for " + paper_id)
        if actual_text != expected_text:
            integrity_errors.append("g05 extracted text checksum drift for " + paper_id)
        source_hashes[paper_id] = (expected_pdf.upper(), expected_text.upper())
    card_paths = sorted(
        (reference_root / "evidence" / "mechanism-cards").glob("PAT-*.md"),
        key=lambda path: path.name,
    )
    pattern_cards = {
        path.stem: parse_mechanism_card_file(path)
        for path in card_paths
    }
    pattern_source_papers = {
        pattern_id: list(map(str, card.get("source_paper_ids", [])))
        for pattern_id, card in pattern_cards.items()
    }
    card_hash_lines = []
    for path in card_paths:
        repository_relative = "arxiv-reference/" + path.relative_to(reference_root).as_posix()
        card_hash_lines.append(hash_file_bytes_sha256(path) + "  " + repository_relative + "\n")
    card_aggregate = hashlib.sha256("".join(card_hash_lines).encode("utf-8")).hexdigest()
    request_paths = {
        "metadata": reference_root / "sources" / "metadata-request-ledger.tsv",
        "citation": reference_root / "sources" / "citation-request-ledger.tsv",
        "download": reference_root / "sources" / "download-ledger.tsv",
    }
    request_rows = {name: read_tsv_records_exact(path) for name, path in request_paths.items()}
    g06_request_count = sum(
        row.get("goal_id") == "G06"
        for rows in request_rows.values()
        for row in rows
    )
    return {
        "paper_count": len(plan_rows),
        "page_count": sum(page_counts.values()),
        "pattern_count": len(pattern_cards),
        "pattern_edge_count": len(edge_rows),
        "manifest_row_count": len(manifest_rows),
        "read_complete_count": sum(
            row.get("selection_status") == "READ_COMPLETE" for row in manifest_rows
        ),
        "paper_page_counts": page_counts,
        "paper_source_hashes": source_hashes,
        "pattern_cards": pattern_cards,
        "pattern_source_papers": pattern_source_papers,
        "integrity_errors": integrity_errors,
        "g06_request_count": g06_request_count,
        "input_hashes": {
            "manifest": hash_file_bytes_sha256(manifest_path),
            "g05_plan": hash_file_bytes_sha256(plan_path),
            "pattern_edges": hash_file_bytes_sha256(edge_path),
            "mechanism_cards": card_aggregate,
            "metadata_requests": hash_file_bytes_sha256(request_paths["metadata"]),
            "citation_requests": hash_file_bytes_sha256(request_paths["citation"]),
            "download_requests": hash_file_bytes_sha256(request_paths["download"]),
        },
        "request_row_counts": {
            name: len(rows) for name, rows in request_rows.items()
        },
    }


def validate_frozen_input_snapshot(snapshot: Mapping[str, object]) -> List[str]:
    """Compare one derived corpus snapshot with the frozen G06 entry state."""

    errors: List[str] = list(map(str, snapshot.get("integrity_errors", [])))
    if snapshot.get("manifest_row_count") != FROZEN_INPUTS["manifest"][0]:
        errors.append("frozen input: paper identities changed after G06 entry")
    if snapshot.get("paper_count") != 25 or snapshot.get("read_complete_count") != 25:
        errors.append("frozen input: selected READ_COMPLETE paper set is not exactly 25")
    if snapshot.get("page_count") != 427:
        errors.append("frozen input: selected page coverage changed")
    if snapshot.get("pattern_count") != 67:
        errors.append("frozen input: mechanism card set changed")
    if snapshot.get("pattern_edge_count") != 47:
        errors.append("frozen input: pattern edge set changed")
    input_hashes = snapshot.get("input_hashes", {})
    if not isinstance(input_hashes, Mapping):
        input_hashes = {}
    for name, (_, expected_hash) in FROZEN_INPUTS.items():
        if input_hashes.get(name) != expected_hash:
            errors.append("frozen input: SHA-256 drift for " + name)
    request_counts = snapshot.get("request_row_counts", {})
    if not isinstance(request_counts, Mapping):
        request_counts = {}
    expected_request_counts = {"metadata": 191, "citation": 83, "download": 50}
    for name, expected_count in expected_request_counts.items():
        if request_counts.get(name) != expected_count:
            errors.append("frozen input: external request count changed for " + name)
    if snapshot.get("g06_request_count") != 0:
        errors.append("frozen input: G06 external request row is forbidden")
    return sorted(set(errors))


def validate_g06_entry_corpus(reference_root: Path) -> List[str]:
    """Validate lifecycle markers and the complete frozen G05 input corpus."""

    errors = validate_frozen_input_snapshot(derive_g06_corpus_records(reference_root))
    status_text = (reference_root / "governance" / "campaign-status.md").read_text(
        encoding="utf-8"
    )
    review_text = (
        reference_root / "governance" / "reviews" / "G05-adversarial-review.md"
    ).read_text(encoding="utf-8")
    preserved_marker = "- G05 state: `COMPLETE_VERIFIED_CLEARED`"
    if preserved_marker not in status_text:
        errors.append("G05 entry lifecycle marker is missing: " + preserved_marker)
    for marker in (
        "- Final verdict: `CLEARED`",
        "**Unresolved findings: P0=0, P1=0, P2=0.**",
    ):
        if marker not in review_text:
            errors.append("G05 review clearance marker is missing: " + marker)
    return sorted(set(errors))


def validate_later_artifacts_absent(reference_root: Path) -> List[str]:
    """Reject G07, G08, and G09 instances while G06 is active."""

    forbidden_roots = (
        reference_root / "evidence" / "constraint-transfer-cards",
        reference_root / "synthesis" / "architecture-genomes",
        reference_root / "synthesis" / "architecture-candidates",
        reference_root / "synthesis" / "experiments",
    )
    forbidden_files = (
        reference_root / "synthesis" / "pareto-archive.tsv",
        reference_root / "synthesis" / "architecture-decision-atlas.md",
        reference_root / "synthesis" / "experiment-backlog.md",
    )
    errors: List[str] = []
    for directory in forbidden_roots:
        if directory.is_dir():
            for path in directory.rglob("*"):
                if path.is_file():
                    errors.append(
                        "scope: later-goal artifact is forbidden during G06: "
                        + path.relative_to(reference_root).as_posix()
                    )
    for path in forbidden_files:
        if path.is_file():
            errors.append(
                "scope: later-goal artifact is forbidden during G06: "
                + path.relative_to(reference_root).as_posix()
            )
    return sorted(set(errors))


def derive_initial_adversarial_rows(reference_root: Path) -> List[Dict[str, str]]:
    """Derive the deterministic 25-paper plus 67-pattern PENDING plan."""

    g05_rows = read_tsv_records_exact(
        reference_root / "governance" / "g05-reading-plan.tsv"
    )
    snapshot = derive_g06_corpus_records(reference_root)
    rows: List[Dict[str, str]] = []
    for row in g05_rows:
        batch_match = re.fullmatch(r"G05-BATCH-([1-5])", row.get("batch_id", ""))
        if batch_match is None:
            raise ValueError("G05 batch ID cannot be mapped to a G06 lane")
        values = {
            "subject_type": "PAPER",
            "subject_rank": row.get("selection_rank", ""),
            "lane_id": "G06-LANE-" + batch_match.group(1),
            "lane_position": row.get("batch_position", ""),
            "subject_id": row.get("paper_id", ""),
            "source_paper_ids": row.get("paper_id", ""),
            "reader_agent_id": "PENDING",
            "reviewer_agent_id": "PENDING",
            "inspection_status": "PENDING",
            "terminal_disposition": "PENDING",
            "failure_ids": "",
            "evidence_gap": "",
            "measurement_needed": "",
            "reading_coverage": "PENDING",
            "result_checksum": "PENDING",
        }
        rows.append({field: values[field] for field in ADVERSARIAL_PLAN_FIELDS})
    pattern_sources = snapshot["pattern_source_papers"]
    if not isinstance(pattern_sources, Mapping):
        raise ValueError("derived pattern source mapping is invalid")
    for rank, pattern_id in enumerate(sorted(pattern_sources), start=1):
        values = {
            "subject_type": "PATTERN",
            "subject_rank": str(rank),
            "lane_id": "G06-LANE-{0}".format(((rank - 1) % 5) + 1),
            "lane_position": str(((rank - 1) // 5) + 1),
            "subject_id": pattern_id,
            "source_paper_ids": "|".join(
                sorted(set(map(str, pattern_sources[pattern_id])))
            ),
            "reader_agent_id": "PENDING",
            "reviewer_agent_id": "PENDING",
            "inspection_status": "PENDING",
            "terminal_disposition": "PENDING",
            "failure_ids": "",
            "evidence_gap": "",
            "measurement_needed": "",
            "reading_coverage": "PENDING",
            "result_checksum": "PENDING",
        }
        rows.append({field: values[field] for field in ADVERSARIAL_PLAN_FIELDS})
    return rows


def write_adversarial_plan_tsv(path: Path, rows: Sequence[Mapping[str, str]]) -> None:
    """Write the frozen adversarial plan with byte-deterministic TSV encoding."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=ADVERSARIAL_PLAN_FIELDS,
            delimiter="\t",
            lineterminator="\n",
            extrasaction="raise",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in ADVERSARIAL_PLAN_FIELDS})


def write_failure_card_markdown(path: Path, card: Mapping[str, object]) -> None:
    """Write one canonical failure card as deterministic Markdown plus JSON."""

    failure_id = str(card.get("failure_id", ""))
    if path.stem != failure_id:
        raise ValueError("failure-card path does not match failure_id")
    name = str(card.get("name", "")).strip()
    if not name:
        raise ValueError("failure-card name is required")
    payload = json.dumps(card, sort_keys=True, indent=2, ensure_ascii=True)
    text = "# {0}\n\n```json\n{1}\n```\n".format(name, payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)


def validate_claim_object_fields(
    value: object,
    field_name: str,
    pointer_ids: Set[str],
) -> List[str]:
    """Validate one claim-granular epistemic object."""

    if not isinstance(value, Mapping):
        return [field_name + ": must be an object"]
    errors: List[str] = []
    fields = set(value)
    if fields != CLAIM_OBJECT_FIELDS:
        errors.append(
            field_name
            + ": fields must be exactly "
            + ",".join(sorted(CLAIM_OBJECT_FIELDS))
        )
    claim_type = value.get("claim_type")
    if claim_type not in {"SOURCE_CLAIM", "DERIVED_INFERENCE"}:
        errors.append(field_name + ": invalid claim_type")
    if not str(value.get("text", "")).strip():
        errors.append(field_name + ": text must be non-empty")
    source_pointer_ids = value.get("source_pointer_ids")
    if not isinstance(source_pointer_ids, list):
        errors.append(field_name + ": source_pointer_ids must be a list")
        source_pointer_ids = []
    unresolved = sorted(set(map(str, source_pointer_ids)) - pointer_ids)
    if unresolved:
        errors.append(field_name + ": unknown source pointer IDs " + "|".join(unresolved))
    premises = value.get("premises")
    assumptions = value.get("assumptions")
    uncertainty = str(value.get("uncertainty", "")).strip()
    if not isinstance(premises, list) or not isinstance(assumptions, list):
        errors.append(field_name + ": premises and assumptions must be lists")
    elif claim_type == "SOURCE_CLAIM":
        if not source_pointer_ids:
            errors.append(field_name + ": SOURCE_CLAIM requires a source pointer")
        if premises or assumptions:
            errors.append(field_name + ": SOURCE_CLAIM cannot contain derived premises")
    elif claim_type == "DERIVED_INFERENCE":
        if not premises or not assumptions or not uncertainty:
            errors.append(
                field_name
                + ": DERIVED_INFERENCE requires premises, assumptions, and uncertainty"
            )
    return errors


def validate_source_pointer_records(
    pointers: object,
    source_paper_ids: Set[str],
    paper_page_counts: Mapping[str, int],
) -> Tuple[List[str], Set[str]]:
    """Validate page-bounded source pointers and return their IDs."""

    if not isinstance(pointers, list) or not pointers:
        return ["source_pointers: at least one source pointer is required"], set()
    errors: List[str] = []
    pointer_ids: Set[str] = set()
    for index, pointer in enumerate(pointers, start=1):
        label = "source_pointers[{0}]".format(index)
        if not isinstance(pointer, Mapping):
            errors.append(label + ": must be an object")
            continue
        fields = set(pointer)
        if fields not in {SOURCE_POINTER_FIELDS, SOURCE_POINTER_FIELDS | {"short_quote"}}:
            errors.append(label + ": invalid fields")
        pointer_id = str(pointer.get("pointer_id", ""))
        if not re.fullmatch(r"FP-\d{3}", pointer_id):
            errors.append(label + ": pointer_id must match FP-NNN")
        if pointer_id in pointer_ids:
            errors.append(label + ": duplicate pointer_id " + pointer_id)
        pointer_ids.add(pointer_id)
        paper_id = str(pointer.get("paper_id", ""))
        if paper_id not in source_paper_ids:
            errors.append(label + ": paper_id is outside source_paper_ids")
        if paper_id not in paper_page_counts:
            errors.append(label + ": paper_id is outside the frozen G06 corpus")
        page = pointer.get("page")
        if not isinstance(page, int) or page < 1:
            errors.append(label + ": page must be a positive integer")
        elif paper_id in paper_page_counts and page > paper_page_counts[paper_id]:
            errors.append(label + ": page exceeds the frozen page count")
        if pointer.get("locator_type") not in LOCATOR_TYPES:
            errors.append(label + ": invalid locator_type")
        locator_value = str(pointer.get("locator_value", ""))
        if re.search(r"\b(?:abstract|title)\b", locator_value, flags=re.IGNORECASE):
            errors.append(label + ": abstract or title locators are forbidden")
        for field in ("locator_value", "claim_scope"):
            if not str(pointer.get(field, "")).strip():
                errors.append(label + ": " + field + " must be non-empty")
        quote = str(pointer.get("short_quote", ""))
        if quote and (len(quote.split()) > 25 or len(quote) > 200):
            errors.append(label + ": short_quote exceeds the bounded quotation limit")
    return errors, pointer_ids


def validate_breakpoint_equation_fields(
    value: object,
    pointer_ids: Set[str],
) -> List[str]:
    """Validate symbolic, sourced, derived, or explicitly unknown breakpoints."""

    if not isinstance(value, Mapping):
        return ["breakpoint_equation: must be an object"]
    errors: List[str] = []
    if set(value) != BREAKPOINT_FIELDS:
        errors.append("breakpoint_equation: invalid fields")
    expression = str(value.get("expression", "")).strip()
    if not expression:
        errors.append("breakpoint_equation: expression must be non-empty")
    variables = value.get("variables")
    if not isinstance(variables, list) or not variables:
        errors.append("breakpoint_equation: variables must be a non-empty list")
    else:
        for variable in variables:
            if not isinstance(variable, Mapping) or set(variable) != BREAKPOINT_VARIABLE_FIELDS:
                errors.append("breakpoint_equation: invalid variable fields")
                continue
            if any(not str(variable.get(field, "")).strip() for field in BREAKPOINT_VARIABLE_FIELDS):
                errors.append("breakpoint_equation: variable fields must be non-empty")
    constants = value.get("numeric_constants")
    if not isinstance(constants, list):
        errors.append("breakpoint_equation: numeric_constants must be a list")
        constants = []
    declared_literals: List[str] = []
    for constant in constants:
        if not isinstance(constant, Mapping) or set(constant) != BREAKPOINT_CONSTANT_FIELDS:
            errors.append("breakpoint_equation: invalid numeric constant fields")
            continue
        literal = str(constant.get("literal", "")).strip()
        if not literal:
            errors.append("breakpoint_equation: numeric constant literal is empty")
        declared_literals.append(literal)
        constant_pointers = constant.get("source_pointer_ids")
        premises = constant.get("premises")
        assumptions = constant.get("assumptions")
        uncertainty = str(constant.get("uncertainty", "")).strip()
        if not isinstance(constant_pointers, list):
            errors.append("breakpoint_equation: constant pointers must be a list")
        elif set(map(str, constant_pointers)) - pointer_ids:
            errors.append("breakpoint_equation: numeric constant has unknown pointer")
        if constant_pointers:
            if premises or assumptions:
                errors.append("breakpoint_equation: sourced constant cannot mix derived premises")
        elif not premises or not assumptions or not uncertainty:
            errors.append("breakpoint_equation: derived constant lacks provenance")
    expression_literals = re.findall(r"(?<![A-Za-z_])\d+(?:\.\d+)?(?![A-Za-z_])", expression)
    if sorted(expression_literals) != sorted(declared_literals):
        errors.append("breakpoint_equation: every numeric literal requires one support record")
    claim_stub = {
        "claim_type": value.get("claim_type"),
        "text": expression,
        "source_pointer_ids": value.get("source_pointer_ids"),
        "premises": value.get("premises"),
        "assumptions": value.get("assumptions"),
        "uncertainty": value.get("uncertainty"),
    }
    errors.extend(validate_claim_object_fields(claim_stub, "breakpoint_equation", pointer_ids))
    if expression == "UNKNOWN" and not str(value.get("measurement_needed", "")).strip():
        errors.append("breakpoint_equation: UNKNOWN requires measurement_needed")
    return errors


def validate_adversarial_fixture_fields(
    value: object,
    pointer_ids: Set[str],
) -> List[str]:
    """Validate one controller-derived minimal adversarial fixture."""

    if not isinstance(value, Mapping):
        return ["adversarial_fixture: must be an object"]
    errors: List[str] = []
    if set(value) != ADVERSARIAL_FIXTURE_FIELDS:
        errors.append("adversarial_fixture: invalid fields")
    if value.get("claim_type") != "DERIVED_INFERENCE":
        errors.append("adversarial_fixture: claim_type must be DERIVED_INFERENCE")
    if value.get("fixture_kind") not in FIXTURE_KINDS:
        errors.append("adversarial_fixture: invalid fixture_kind")
    for field in (
        "fixture_name",
        "graph_shape",
        "graph_scale",
        "workload",
        "independent_oracle",
        "expected_observation",
        "uncertainty",
    ):
        if not str(value.get(field, "")).strip():
            errors.append("adversarial_fixture: " + field + " must be non-empty")
    for field in ("controlled_variables", "varied_variables", "premises", "assumptions"):
        field_value = value.get(field)
        if not isinstance(field_value, list) or not field_value:
            errors.append("adversarial_fixture: " + field + " must be a non-empty list")
    source_pointer_ids = value.get("source_pointer_ids")
    if not isinstance(source_pointer_ids, list):
        errors.append("adversarial_fixture: source_pointer_ids must be a list")
    elif set(map(str, source_pointer_ids)) - pointer_ids:
        errors.append("adversarial_fixture: unknown source pointer")
    return errors


def validate_repair_option_records(value: object) -> List[str]:
    """Validate non-decisional response classes for one failure."""

    if not isinstance(value, list) or not value:
        return ["repair_options: must be a non-empty list"]
    errors: List[str] = []
    for option in value:
        if not isinstance(option, Mapping) or set(option) != {"repair_class", "description"}:
            errors.append("repair_options: invalid option fields")
            continue
        if option.get("repair_class") not in REPAIR_CLASSES:
            errors.append("repair_options: invalid repair_class")
        if not str(option.get("description", "")).strip():
            errors.append("repair_options: description must be non-empty")
    return errors


def build_failure_duplicate_signature(card: Mapping[str, object]) -> Tuple[object, ...]:
    """Build the frozen semantic duplicate signature for one failure."""

    def claim_text(field: str) -> str:
        value = card.get(field)
        if not isinstance(value, Mapping):
            return ""
        return normalize_duplicate_signature_text(value.get("text", ""))

    breakpoint = card.get("breakpoint_equation")
    expression = breakpoint.get("expression", "") if isinstance(breakpoint, Mapping) else ""
    patterns = card.get("affected_pattern_ids")
    pattern_tuple = tuple(sorted(map(str, patterns))) if isinstance(patterns, list) else ()
    return (
        pattern_tuple,
        claim_text("broken_assumption"),
        claim_text("triggering_workload"),
        claim_text("observable_symptom"),
        normalize_duplicate_signature_text(expression),
    )


def validate_failure_card_record(
    card: Mapping[str, object],
    paper_page_counts: Mapping[str, int],
    known_pattern_ids: Set[str],
) -> List[str]:
    """Validate one G06 failure card against its frozen corpus."""

    errors: List[str] = []
    fields = set(card)
    unknown = sorted(fields - FAILURE_CARD_FIELDS)
    missing = sorted(FAILURE_CARD_FIELDS - fields)
    if unknown:
        errors.append("failure card: unknown top-level fields " + "|".join(unknown))
    if missing:
        errors.append("failure card: missing top-level fields " + "|".join(missing))
    failure_id = str(card.get("failure_id", ""))
    if not re.fullmatch(r"FAIL-(?:[A-Z0-9]+-){3}[A-Z0-9]+", failure_id):
        errors.append("failure_id: must use FAIL-<FOUR-WORD-SLUG>")
    if not str(card.get("name", "")).strip():
        errors.append("name: must be non-empty")
    basis = card.get("failure_basis")
    label = card.get("epistemic_label")
    if basis not in FAILURE_BASES:
        errors.append("failure_basis: invalid value")
    if basis == "SOURCE_REPORTED" and label != "SOURCE_CLAIM":
        errors.append("epistemic_label: SOURCE_REPORTED requires SOURCE_CLAIM")
    if basis in {"SOURCE_SUPPORTED_DERIVATION", "ANALYTICAL_COUNTEREXAMPLE"} and label != "DERIVED_INFERENCE":
        errors.append("epistemic_label: derived failure basis requires DERIVED_INFERENCE")
    source_papers = card.get("source_paper_ids")
    if not isinstance(source_papers, list) or not source_papers:
        errors.append("source_paper_ids: must be a non-empty list")
        source_papers = []
    elif source_papers != sorted(set(map(str, source_papers))):
        errors.append("source_paper_ids: must be sorted and unique")
    source_paper_set = set(map(str, source_papers))
    unknown_papers = sorted(source_paper_set - set(paper_page_counts))
    if unknown_papers:
        errors.append("source_paper_ids: unknown frozen papers " + "|".join(unknown_papers))
    pointer_errors, pointer_ids = validate_source_pointer_records(
        card.get("source_pointers"), source_paper_set, paper_page_counts
    )
    errors.extend(pointer_errors)
    for field in (
        "broken_assumption",
        "triggering_workload",
        "observable_symptom",
        "expected_failure_signal",
        "confidence_rationale",
    ):
        errors.extend(validate_claim_object_fields(card.get(field), field, pointer_ids))
    confidence = card.get("confidence_rationale")
    if not isinstance(confidence, Mapping) or confidence.get("claim_type") != "DERIVED_INFERENCE":
        errors.append("confidence_rationale: must be DERIVED_INFERENCE")
    errors.extend(validate_breakpoint_equation_fields(card.get("breakpoint_equation"), pointer_ids))
    patterns = card.get("affected_pattern_ids")
    if not isinstance(patterns, list) or not patterns:
        errors.append("affected_pattern_ids: must be a non-empty list")
        patterns = []
    elif patterns != sorted(set(map(str, patterns))):
        errors.append("affected_pattern_ids: must be sorted and unique")
    unknown_patterns = sorted(set(map(str, patterns)) - known_pattern_ids)
    if unknown_patterns:
        errors.append("affected_pattern_ids: unknown pattern IDs " + "|".join(unknown_patterns))
    if card.get("affected_architecture_ids") != []:
        errors.append("affected_architecture_ids: must remain empty before G08")
    errors.extend(validate_adversarial_fixture_fields(card.get("adversarial_fixture"), pointer_ids))
    errors.extend(validate_repair_option_records(card.get("repair_options")))
    serialized = json.dumps(card, sort_keys=True, ensure_ascii=True)
    if re.search(r"\b(?:ARCH|XFER|EXP)-[A-Z0-9-]+", serialized):
        errors.append("failure card: later-goal identifier is forbidden")
    return sorted(set(errors))


def validate_failure_card_collection(
    cards: Sequence[Mapping[str, object]],
    paper_page_counts: Mapping[str, int],
    known_pattern_ids: Set[str],
) -> List[str]:
    """Validate cards, identities, and semantic duplicate signatures."""

    errors: List[str] = []
    seen_ids: Set[str] = set()
    seen_signatures: Dict[Tuple[object, ...], str] = {}
    for card in cards:
        errors.extend(validate_failure_card_record(card, paper_page_counts, known_pattern_ids))
        failure_id = str(card.get("failure_id", ""))
        if failure_id in seen_ids:
            errors.append("failure collection: duplicate failure_id " + failure_id)
        seen_ids.add(failure_id)
        signature = build_failure_duplicate_signature(card)
        previous = seen_signatures.get(signature)
        if previous is not None:
            errors.append(
                "failure collection: duplicate failure signature "
                + previous
                + " and "
                + failure_id
            )
        else:
            seen_signatures[signature] = failure_id
    return sorted(set(errors))


def validate_reviewed_semantic_merge_resolution(
    failure_cards: Mapping[str, Mapping[str, object]],
    plan_rows: Sequence[Mapping[str, str]],
    report_text: str,
) -> List[str]:
    """Require every human-reviewed duplicate alias to resolve canonically."""

    errors: List[str] = []
    plan_failure_ids = {
        failure_id
        for row in plan_rows
        for failure_id in split_pipe_values_sorted(row.get("failure_ids", ""))
    }
    for retired_id, canonical_id in REVIEWED_SEMANTIC_MERGES:
        if retired_id in failure_cards:
            errors.append("semantic merge: retired failure card still exists " + retired_id)
        if canonical_id not in failure_cards:
            errors.append("semantic merge: canonical failure card is missing " + canonical_id)
        if retired_id in plan_failure_ids:
            errors.append("semantic merge: plan still references retired failure " + retired_id)
        marker = "| `{0}` | `{1}` |".format(retired_id, canonical_id)
        if marker not in report_text:
            errors.append("semantic merge: report alias is missing " + retired_id)
    return sorted(set(errors))


def validate_adversarial_plan_rows(
    rows: Sequence[Mapping[str, str]],
    paper_page_counts: Mapping[str, int],
    pattern_source_papers: Mapping[str, Sequence[str]],
    failure_cards: Mapping[str, Mapping[str, object]],
    require_complete: bool,
) -> List[str]:
    """Validate exact paper/pattern coverage and bidirectional evidence links."""

    errors: List[str] = []
    expected_papers = list(paper_page_counts)
    expected_patterns = sorted(pattern_source_papers)
    expected_subjects = {
        "PAPER": set(expected_papers),
        "PATTERN": set(expected_patterns),
    }
    seen_subjects: Dict[str, Set[str]] = {"PAPER": set(), "PATTERN": set()}
    linked_failure_ids: Set[str] = set()
    pattern_failure_ids: Dict[str, Set[str]] = {}
    paper_rows_complete: Set[str] = set()
    for row_index, row in enumerate(rows, start=1):
        label = "adversarial plan row {0}".format(row_index)
        if tuple(row) != ADVERSARIAL_PLAN_FIELDS:
            errors.append(label + ": header fields do not match the frozen plan schema")
        subject_type = row.get("subject_type", "")
        if subject_type not in expected_subjects:
            errors.append(label + ": invalid subject_type")
            continue
        subject_id = row.get("subject_id", "")
        if subject_id not in expected_subjects[subject_type]:
            errors.append(label + ": unknown " + subject_type.lower() + " subject " + subject_id)
        if subject_id in seen_subjects[subject_type]:
            errors.append(label + ": duplicate subject " + subject_id)
        seen_subjects[subject_type].add(subject_id)
        try:
            rank = int(row.get("subject_rank", ""))
            lane_position = int(row.get("lane_position", ""))
        except ValueError:
            errors.append(label + ": rank and lane_position must be integers")
            continue
        expected_order = expected_papers if subject_type == "PAPER" else expected_patterns
        if 1 <= rank <= len(expected_order) and subject_id != expected_order[rank - 1]:
            errors.append(label + ": subject_rank does not match deterministic ordering")
        lane_id = row.get("lane_id", "")
        if lane_id not in {"G06-LANE-{0}".format(number) for number in range(1, 6)}:
            errors.append(label + ": invalid lane_id")
        if subject_type == "PATTERN" and rank >= 1:
            expected_lane = "G06-LANE-{0}".format(((rank - 1) % 5) + 1)
            expected_position = ((rank - 1) // 5) + 1
            if lane_id != expected_lane or lane_position != expected_position:
                errors.append(label + ": pattern lane assignment is not deterministic")
        source_papers = split_pipe_values_sorted(row.get("source_paper_ids", ""))
        expected_sources = (
            [subject_id]
            if subject_type == "PAPER"
            else sorted(set(map(str, pattern_source_papers.get(subject_id, []))))
        )
        if source_papers != expected_sources:
            errors.append(label + ": source_paper_ids do not match the frozen subject")
        status = row.get("inspection_status", "")
        disposition = row.get("terminal_disposition", "")
        failure_ids = split_pipe_values_sorted(row.get("failure_ids", ""))
        gap = row.get("evidence_gap", "").strip()
        measurement = row.get("measurement_needed", "").strip()
        if require_complete:
            if status != "COMPLETE":
                errors.append(label + ": inspection_status must be COMPLETE")
            if row.get("reader_agent_id") in {"", "PENDING"}:
                errors.append(label + ": reader_agent_id is not terminal")
            if row.get("reviewer_agent_id") in {"", "PENDING"}:
                errors.append(label + ": reviewer_agent_id is not terminal")
            if subject_type == "PAPER" and disposition not in PAPER_DISPOSITIONS:
                errors.append(label + ": missing terminal disposition for paper")
            if subject_type == "PATTERN" and disposition not in PATTERN_DISPOSITIONS:
                errors.append(label + ": missing terminal disposition for pattern")
            if not re.fullmatch(r"[A-F0-9]{64}", row.get("result_checksum", "")):
                errors.append(label + ": result_checksum must be uppercase SHA-256")
        elif status not in {"PENDING", "INSPECTING", "COMPLETE"}:
            errors.append(label + ": invalid inspection_status")
        linked_disposition = disposition in {
            "NEGATIVE_EVIDENCE_EXTRACTED",
            "SOURCE_FAILURE_LINKED",
            "ANALYTICAL_TEST_LINKED",
        }
        if linked_disposition:
            if not failure_ids:
                errors.append(label + ": linked disposition requires failure_ids")
            if gap or measurement:
                errors.append(label + ": linked disposition cannot contain gap fields")
        if disposition in {"NO_NEGATIVE_EVIDENCE", "EXPLICIT_EVIDENCE_GAP"}:
            if failure_ids:
                errors.append(label + ": gap disposition cannot contain failure_ids")
            if not gap or not measurement:
                errors.append(label + ": gap disposition requires evidence and measurement gaps")
        for failure_id in failure_ids:
            failure = failure_cards.get(failure_id)
            if failure is None:
                errors.append(label + ": unknown failure_id " + failure_id)
                continue
            linked_failure_ids.add(failure_id)
            if subject_type == "PATTERN" and subject_id not in failure.get(
                "affected_pattern_ids", []
            ):
                errors.append(label + ": failure " + failure_id + " does not affect pattern " + subject_id)
            if subject_type == "PAPER" and subject_id not in failure.get(
                "source_paper_ids", []
            ):
                errors.append(label + ": failure " + failure_id + " is not sourced by paper " + subject_id)
        if subject_type == "PATTERN":
            pattern_failure_ids[subject_id] = set(failure_ids)
            linked_bases = {
                str(failure_cards[failure_id].get("failure_basis", ""))
                for failure_id in failure_ids
                if failure_id in failure_cards
            }
            disposition_matches_basis = (
                disposition == "SOURCE_FAILURE_LINKED"
                and "SOURCE_REPORTED" in linked_bases
            ) or (
                disposition == "ANALYTICAL_TEST_LINKED"
                and bool(linked_bases)
                and "SOURCE_REPORTED" not in linked_bases
            )
            if disposition in {"SOURCE_FAILURE_LINKED", "ANALYTICAL_TEST_LINKED"} and not disposition_matches_basis:
                errors.append(label + ": disposition does not match linked failure bases")
        coverage = row.get("reading_coverage", "")
        if subject_type == "PAPER" and require_complete:
            expected_coverage = "ALL_PAGES:1-{0}".format(paper_page_counts.get(subject_id, 0))
            if coverage != expected_coverage:
                errors.append(label + ": incomplete paper reading coverage")
            else:
                paper_rows_complete.add(subject_id)
        elif subject_type == "PATTERN" and require_complete:
            expected_coverage = "PAPER_ROWS:" + "|".join(expected_sources)
            if coverage != expected_coverage:
                errors.append(label + ": pattern reading coverage does not bind paper rows")
    for subject_type, expected in expected_subjects.items():
        missing = sorted(expected - seen_subjects[subject_type])
        extra = sorted(seen_subjects[subject_type] - expected)
        if missing:
            errors.append("adversarial plan: missing {0} subjects {1}".format(subject_type, "|".join(missing)))
        if extra:
            errors.append("adversarial plan: extra {0} subjects {1}".format(subject_type, "|".join(extra)))
    if require_complete:
        missing_paper_rows = sorted(set(expected_papers) - paper_rows_complete)
        if missing_paper_rows:
            errors.append("adversarial plan: papers lack complete-page rows " + "|".join(missing_paper_rows))
        orphan_failures = sorted(set(failure_cards) - linked_failure_ids)
        if orphan_failures:
            errors.append("adversarial plan: orphan failure cards " + "|".join(orphan_failures))
        for failure_id, failure in sorted(failure_cards.items()):
            for pattern_id in sorted(map(str, failure.get("affected_pattern_ids", []))):
                if failure_id not in pattern_failure_ids.get(pattern_id, set()):
                    errors.append(
                        "adversarial plan: missing inverse pattern link "
                        + pattern_id
                        + " -> "
                        + failure_id
                    )
    return sorted(set(errors))


def validate_evidence_conflict_rows(
    rows: Sequence[Mapping[str, str]],
    mechanism_cards: Mapping[str, Mapping[str, object]],
    failure_cards: Mapping[str, Mapping[str, object]],
    known_paper_ids: Set[str],
) -> List[str]:
    """Validate typed, two-sided, pointer-qualified evidence conflicts."""

    errors: List[str] = []
    artifact_maps = {
        "MECHANISM_CARD": mechanism_cards,
        "FAILURE_CARD": failure_cards,
    }
    seen_ids: Set[str] = set()
    seen_signatures: Set[Tuple[object, ...]] = set()
    for row_index, row in enumerate(rows, start=1):
        label = "evidence conflict row {0}".format(row_index)
        if tuple(row) != EVIDENCE_CONFLICT_FIELDS:
            errors.append(label + ": fields do not match the frozen conflict schema")
        conflict_id = row.get("conflict_id", "")
        if not re.fullmatch(r"ECONFLICT-\d{4}", conflict_id):
            errors.append(label + ": conflict_id must match ECONFLICT-NNNN")
        if conflict_id in seen_ids:
            errors.append(label + ": duplicate conflict_id " + conflict_id)
        seen_ids.add(conflict_id)
        endpoint_values: List[Tuple[str, str]] = []
        for side in ("left", "right"):
            evidence_type = row.get(side + "_evidence_type", "")
            evidence_id = row.get(side + "_evidence_id", "")
            if evidence_type not in artifact_maps:
                errors.append(label + ": invalid " + side + " evidence type")
            if not evidence_id:
                errors.append(label + ": " + side + " evidence ID is required")
            elif evidence_type in artifact_maps and evidence_id not in artifact_maps[evidence_type]:
                errors.append(label + ": unknown " + side + " evidence ID " + evidence_id)
            endpoint_values.append((evidence_type, evidence_id))
        if endpoint_values[0] == endpoint_values[1]:
            errors.append(label + ": conflict sides must differ")
        if row.get("conflict_type") not in CONFLICT_TYPES:
            errors.append(label + ": invalid conflict_type")
        affected_patterns = split_pipe_values_sorted(row.get("affected_pattern_ids", ""))
        if not affected_patterns:
            errors.append(label + ": affected_pattern_ids must be non-empty")
        unknown_patterns = sorted(set(affected_patterns) - set(mechanism_cards))
        if unknown_patterns:
            errors.append(label + ": unknown affected pattern IDs " + "|".join(unknown_patterns))
        for field in ("claim_scope", "rationale"):
            if not row.get(field, "").strip():
                errors.append(label + ": " + field + " must be non-empty")
        epistemic_label = row.get("epistemic_label", "")
        if epistemic_label not in {"SOURCE_CLAIM", "DERIVED_INFERENCE"}:
            errors.append(label + ": invalid epistemic_label")
        if epistemic_label == "DERIVED_INFERENCE":
            rationale = row.get("rationale", "")
            if not all(token in rationale for token in ("premises=", "assumptions=", "uncertainty=")):
                errors.append(label + ": derived rationale lacks premises, assumptions, or uncertainty")
        if row.get("resolution_state") not in {"OPEN", "CONDITIONALLY_RECONCILED"}:
            errors.append(label + ": invalid resolution_state")
        declared_papers = split_pipe_values_sorted(row.get("source_paper_ids", ""))
        if not declared_papers or set(declared_papers) - known_paper_ids:
            errors.append(label + ": source_paper_ids are empty or outside the frozen corpus")
        reached_papers: Set[str] = set()
        pointer_values = split_pipe_values_sorted(row.get("source_pointer_ids", ""))
        if not pointer_values:
            errors.append(label + ": source_pointer_ids must be non-empty")
        endpoint_ids = {value[1] for value in endpoint_values if value[1]}
        for qualified_pointer in pointer_values:
            if "#" not in qualified_pointer:
                errors.append(label + ": malformed qualified source pointer")
                continue
            artifact_id, pointer_id = qualified_pointer.split("#", 1)
            if artifact_id not in endpoint_ids:
                errors.append(label + ": pointer artifact is not a conflict endpoint")
                continue
            artifact = mechanism_cards.get(artifact_id) or failure_cards.get(artifact_id)
            if artifact is None:
                continue
            pointer_map = {
                str(pointer.get("pointer_id", "")): str(pointer.get("paper_id", ""))
                for pointer in artifact.get("source_pointers", [])
                if isinstance(pointer, Mapping)
            }
            if pointer_id not in pointer_map:
                errors.append(label + ": unknown qualified source pointer " + qualified_pointer)
            else:
                reached_papers.add(pointer_map[pointer_id])
        if declared_papers != sorted(reached_papers):
            errors.append(label + ": source_paper_ids do not match qualified pointers")
        signature = (
            tuple(sorted(endpoint_values)),
            row.get("conflict_type", ""),
            normalize_duplicate_signature_text(row.get("claim_scope", "")),
        )
        if signature in seen_signatures:
            errors.append(label + ": duplicate or inverse conflict")
        seen_signatures.add(signature)
    return sorted(set(errors))


def validate_lane_dossier_record(
    dossier: Mapping[str, object],
    assigned_rows: Sequence[Mapping[str, str]],
    paper_page_counts: Mapping[str, int],
    known_pattern_ids: Set[str],
) -> List[str]:
    """Validate one read-only semantic lane before canonical integration."""

    errors: List[str] = []
    if set(dossier) != LANE_DOSSIER_FIELDS:
        errors.append("lane dossier: top-level fields do not match the frozen intake schema")
    if dossier.get("schema_version") != "G06-LANE-DOSSIER-V1":
        errors.append("lane dossier: invalid schema_version")
    lane_id = str(dossier.get("lane_id", ""))
    if not re.fullmatch(r"G06-LANE-[1-5]", lane_id):
        errors.append("lane dossier: invalid lane_id")
    if any(row.get("lane_id") != lane_id for row in assigned_rows):
        errors.append("lane dossier: assigned rows cross lane ownership")

    expected_papers = {
        row.get("subject_id", "")
        for row in assigned_rows
        if row.get("subject_type") == "PAPER"
    }
    expected_patterns = {
        row.get("subject_id", "")
        for row in assigned_rows
        if row.get("subject_type") == "PATTERN"
    }
    expected_sources = {
        row.get("subject_id", ""): split_pipe_values_sorted(
            row.get("source_paper_ids", "")
        )
        for row in assigned_rows
        if row.get("subject_type") == "PATTERN"
    }

    failure_records = dossier.get("failure_cards")
    if not isinstance(failure_records, list):
        errors.append("lane dossier: failure_cards must be a list")
        failure_records = []
    typed_failure_records = [
        record for record in failure_records if isinstance(record, Mapping)
    ]
    if len(typed_failure_records) != len(failure_records):
        errors.append("lane dossier: each failure card must be an object")
    failure_cards = {
        str(card.get("failure_id", "")): card for card in typed_failure_records
    }
    if len(failure_cards) != len(typed_failure_records):
        errors.append("lane dossier: failure card IDs must be non-empty and unique")
    errors.extend(
        validate_failure_card_collection(
            typed_failure_records, paper_page_counts, known_pattern_ids
        )
    )

    linked_failure_ids: Set[str] = set()
    paper_results = dossier.get("paper_results")
    if not isinstance(paper_results, list):
        errors.append("lane dossier: paper_results must be a list")
        paper_results = []
    paper_ids: List[str] = []
    inspected_pages = 0
    for index, result in enumerate(paper_results, start=1):
        label = "lane paper result {0}".format(index)
        if not isinstance(result, Mapping):
            errors.append(label + ": must be an object")
            continue
        if set(result) != LANE_PAPER_RESULT_FIELDS:
            errors.append(label + ": fields do not match the intake schema")
        paper_id = str(result.get("subject_id", ""))
        paper_ids.append(paper_id)
        expected_count = paper_page_counts.get(paper_id)
        if expected_count is None or paper_id not in expected_papers:
            errors.append(label + ": paper is outside lane ownership")
            expected_count = 0
        if result.get("page_count") != expected_count:
            errors.append(label + ": page_count does not match the frozen paper")
        page_audit = result.get("page_audit")
        audited_pages: List[int] = []
        if not isinstance(page_audit, list):
            errors.append(label + ": page_audit must be a list")
            page_audit = []
        for page_record in page_audit:
            if not isinstance(page_record, Mapping) or set(page_record) != {
                "page",
                "disposition",
                "evidence_keys",
            }:
                errors.append(label + ": invalid page_audit record")
                continue
            page = page_record.get("page")
            if not isinstance(page, int):
                errors.append(label + ": audited page must be an integer")
                continue
            audited_pages.append(page)
            if page_record.get("disposition") not in {
                "NEGATIVE_EVIDENCE_FOUND",
                "NO_RELEVANT_NEGATIVE_EVIDENCE",
            }:
                errors.append(label + ": invalid page disposition")
            evidence_keys = page_record.get("evidence_keys")
            if not isinstance(evidence_keys, list):
                errors.append(label + ": evidence_keys must be a list")
        if audited_pages != list(range(1, int(expected_count or 0) + 1)):
            errors.append(label + ": page_audit is not exact complete-page coverage")
        inspected_pages += len(audited_pages)
        disposition = result.get("terminal_disposition")
        failure_ids = result.get("proposed_failure_ids")
        if not isinstance(failure_ids, list):
            errors.append(label + ": proposed_failure_ids must be a list")
            failure_ids = []
        failure_ids = list(map(str, failure_ids))
        linked_failure_ids.update(failure_ids)
        gap = str(result.get("evidence_gap", "")).strip()
        measurement = str(result.get("measurement_needed", "")).strip()
        if disposition == "NEGATIVE_EVIDENCE_EXTRACTED":
            if not failure_ids or gap or measurement:
                errors.append(label + ": extracted evidence requires linked failures only")
        elif disposition == "NO_NEGATIVE_EVIDENCE":
            if failure_ids or not gap or not measurement:
                errors.append(label + ": no-evidence disposition requires explicit gaps")
        else:
            errors.append(label + ": invalid terminal disposition")
        for failure_id in failure_ids:
            card = failure_cards.get(failure_id)
            if card is None:
                errors.append(label + ": unresolved failure ID " + failure_id)
            elif paper_id not in card.get("source_paper_ids", []):
                errors.append(label + ": linked failure is not sourced by the paper")

    pattern_results = dossier.get("pattern_results")
    if not isinstance(pattern_results, list):
        errors.append("lane dossier: pattern_results must be a list")
        pattern_results = []
    pattern_ids: List[str] = []
    for index, result in enumerate(pattern_results, start=1):
        label = "lane pattern result {0}".format(index)
        if not isinstance(result, Mapping):
            errors.append(label + ": must be an object")
            continue
        if set(result) != LANE_PATTERN_RESULT_FIELDS:
            errors.append(label + ": fields do not match the intake schema")
        pattern_id = str(result.get("subject_id", ""))
        pattern_ids.append(pattern_id)
        if pattern_id not in expected_patterns:
            errors.append(label + ": pattern is outside lane ownership")
        source_papers = result.get("source_paper_ids")
        if not isinstance(source_papers, list) or sorted(set(map(str, source_papers))) != expected_sources.get(
            pattern_id, []
        ):
            errors.append(label + ": source_paper_ids do not match the plan")
        for field in (
            "required_assumption",
            "smallest_violating_workload",
            "triggering_graph_property",
            "unexpected_resource_term",
            "observable_symptom",
            "source_reported_breakpoint",
            "symbolic_breakpoint",
            "unknowns",
            "minimal_fixture",
            "independent_oracle",
        ):
            if not str(result.get(field, "")).strip():
                errors.append(label + ": " + field + " must be non-empty")
        if result.get("failure_effect") not in {"INVALIDATES", "SPECIALIZES", "LIMITS"}:
            errors.append(label + ": invalid failure_effect")
        if not isinstance(result.get("related_mechanisms"), list):
            errors.append(label + ": related_mechanisms must be a list")
        failure_ids = result.get("proposed_failure_ids")
        if not isinstance(failure_ids, list):
            errors.append(label + ": proposed_failure_ids must be a list")
            failure_ids = []
        failure_ids = list(map(str, failure_ids))
        linked_failure_ids.update(failure_ids)
        disposition = result.get("terminal_disposition")
        gap = str(result.get("evidence_gap", "")).strip()
        measurement = str(result.get("measurement_needed", "")).strip()
        if disposition in {"SOURCE_FAILURE_LINKED", "ANALYTICAL_TEST_LINKED"}:
            if not failure_ids or gap or measurement:
                errors.append(label + ": linked disposition requires failure IDs only")
        elif disposition == "EXPLICIT_EVIDENCE_GAP":
            if failure_ids or not gap or not measurement:
                errors.append(label + ": evidence gap requires gap and measurement text")
        else:
            errors.append(label + ": invalid terminal disposition")
        for failure_id in failure_ids:
            card = failure_cards.get(failure_id)
            if card is None:
                errors.append(label + ": unresolved failure ID " + failure_id)
            elif pattern_id not in card.get("affected_pattern_ids", []):
                errors.append(label + ": linked failure does not affect the pattern")

    if set(paper_ids) != expected_papers or len(paper_ids) != len(expected_papers):
        errors.append("lane dossier: paper results do not exactly cover owned papers")
    if set(pattern_ids) != expected_patterns or len(pattern_ids) != len(expected_patterns):
        errors.append("lane dossier: pattern results do not exactly cover owned patterns")
    orphan_failures = sorted(set(failure_cards) - linked_failure_ids)
    if orphan_failures:
        errors.append("lane dossier: orphan failure cards " + "|".join(orphan_failures))

    coverage = dossier.get("coverage_audit")
    if not isinstance(coverage, Mapping) or set(coverage) != LANE_COVERAGE_FIELDS:
        errors.append("lane dossier: invalid coverage_audit fields")
    else:
        exact_lists = {
            "assigned_paper_ids": sorted(expected_papers),
            "completed_paper_ids": sorted(expected_papers),
            "assigned_pattern_ids": sorted(expected_patterns),
            "completed_pattern_ids": sorted(expected_patterns),
            "missing_subject_ids": [],
            "duplicate_subject_ids": [],
        }
        for field, expected in exact_lists.items():
            if coverage.get(field) != expected:
                errors.append("lane dossier: coverage_audit " + field + " is not exact")
        expected_pages = sum(paper_page_counts.get(paper_id, 0) for paper_id in expected_papers)
        if coverage.get("pages_expected") != expected_pages:
            errors.append("lane dossier: pages_expected does not match ownership")
        if coverage.get("pages_inspected") != inspected_pages:
            errors.append("lane dossier: pages_inspected does not match page audit")
        if coverage.get("network_requests") != 0 or coverage.get("repository_edits") != 0:
            errors.append("lane dossier: network requests and repository edits must be zero")

    self_review = dossier.get("lane_self_review")
    if not isinstance(self_review, Mapping) or set(self_review) != LANE_SELF_REVIEW_FIELDS:
        errors.append("lane dossier: invalid lane_self_review fields")
    else:
        for field in LANE_SELF_REVIEW_FIELDS - {"known_uncertainties"}:
            if self_review.get(field) is not True:
                errors.append("lane dossier: self-review gate is not true: " + field)
        if not isinstance(self_review.get("known_uncertainties"), list):
            errors.append("lane dossier: known_uncertainties must be a list")
    if not isinstance(dossier.get("conflict_candidates"), list):
        errors.append("lane dossier: conflict_candidates must be a list")
    return sorted(set(errors))


def apply_lane_dossier_to_plan_rows(
    rows: Sequence[Mapping[str, str]],
    dossier: Mapping[str, object],
    reader_agent_id: str,
) -> List[Dict[str, str]]:
    """Map one already-validated lane dossier onto its owned plan rows."""

    lane_id = str(dossier.get("lane_id", ""))
    paper_results = dossier.get("paper_results", [])
    pattern_results = dossier.get("pattern_results", [])
    if not isinstance(paper_results, list) or not isinstance(pattern_results, list):
        raise ValueError("lane dossier results must be lists")
    result_by_subject: Dict[Tuple[str, str], Mapping[str, object]] = {}
    for subject_type, results in (("PAPER", paper_results), ("PATTERN", pattern_results)):
        for result in results:
            if not isinstance(result, Mapping):
                raise ValueError("lane dossier result must be an object")
            key = (subject_type, str(result.get("subject_id", "")))
            if key in result_by_subject:
                raise ValueError("lane dossier contains a duplicate subject")
            result_by_subject[key] = result

    updated_rows: List[Dict[str, str]] = []
    used_keys: Set[Tuple[str, str]] = set()
    for row in rows:
        updated = {field: str(row.get(field, "")) for field in ADVERSARIAL_PLAN_FIELDS}
        if row.get("lane_id") != lane_id:
            updated_rows.append(updated)
            continue
        key = (row.get("subject_type", ""), row.get("subject_id", ""))
        result = result_by_subject.get(key)
        if result is None:
            raise ValueError("lane dossier omits owned subject " + key[1])
        used_keys.add(key)
        failure_ids = result.get("proposed_failure_ids", [])
        if not isinstance(failure_ids, list):
            raise ValueError("lane result proposed_failure_ids must be a list")
        updated.update(
            {
                "reader_agent_id": reader_agent_id,
                "reviewer_agent_id": "PENDING",
                "inspection_status": "COMPLETE",
                "terminal_disposition": str(result.get("terminal_disposition", "")),
                "failure_ids": "|".join(sorted(set(map(str, failure_ids)))),
                "evidence_gap": str(result.get("evidence_gap", "")),
                "measurement_needed": str(result.get("measurement_needed", "")),
                "result_checksum": "PENDING",
            }
        )
        if key[0] == "PAPER":
            updated["reading_coverage"] = "ALL_PAGES:1-{0}".format(
                result.get("page_count", "")
            )
        else:
            updated["reading_coverage"] = "PAPER_ROWS:" + "|".join(
                split_pipe_values_sorted(row.get("source_paper_ids", ""))
            )
        updated_rows.append(updated)
    unused_keys = sorted(set(result_by_subject) - used_keys)
    if unused_keys:
        raise ValueError(
            "lane dossier contains subjects outside the plan: "
            + "|".join(subject_id for _, subject_id in unused_keys)
        )
    return updated_rows


def validate_lane_dossier_collection(
    dossiers: Sequence[Mapping[str, object]],
    plan_rows: Sequence[Mapping[str, str]],
    paper_page_counts: Mapping[str, int],
    known_pattern_ids: Set[str],
) -> List[str]:
    """Validate exact lane coverage and cross-lane failure uniqueness."""

    errors: List[str] = []
    expected_lanes = {row.get("lane_id", "") for row in plan_rows}
    lane_counts: Dict[str, int] = {}
    lane_failure_records: List[Tuple[str, Mapping[str, object]]] = []
    for dossier in dossiers:
        lane_id = str(dossier.get("lane_id", ""))
        lane_counts[lane_id] = lane_counts.get(lane_id, 0) + 1
        assigned_rows = [row for row in plan_rows if row.get("lane_id") == lane_id]
        errors.extend(
            "{0}: {1}".format(lane_id or "UNKNOWN-LANE", error)
            for error in validate_lane_dossier_record(
                dossier, assigned_rows, paper_page_counts, known_pattern_ids
            )
        )
        cards = dossier.get("failure_cards", [])
        if not isinstance(cards, list):
            continue
        for card in cards:
            if not isinstance(card, Mapping):
                continue
            lane_failure_records.append((lane_id, card))
    for lane_id in sorted(expected_lanes | set(lane_counts)):
        count = lane_counts.get(lane_id, 0)
        if count == 0:
            errors.append("lane collection: missing lane " + lane_id)
        elif count > 1:
            errors.append("lane collection: duplicate lane " + lane_id)
        if lane_id not in expected_lanes:
            errors.append("lane collection: unexpected lane " + lane_id)
    _, merge_errors = merge_failure_card_records(lane_failure_records)
    errors.extend(merge_errors)
    return sorted(set(errors))


def merge_failure_card_records(
    lane_cards: Sequence[Tuple[str, Mapping[str, object]]],
) -> Tuple[Dict[str, Mapping[str, object]], List[str]]:
    """Merge byte-equivalent rediscoveries and reject ID or signature conflicts."""

    merged: Dict[str, Mapping[str, object]] = {}
    canonical_bytes: Dict[str, str] = {}
    owning_lanes: Dict[str, str] = {}
    signatures: Dict[Tuple[object, ...], str] = {}
    errors: List[str] = []
    for lane_id, card in lane_cards:
        failure_id = str(card.get("failure_id", ""))
        serialized = json.dumps(card, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        if failure_id in merged:
            if canonical_bytes[failure_id] != serialized:
                errors.append(
                    "lane collection: conflicting duplicate failure_id {0} in {1} and {2}".format(
                        failure_id, owning_lanes[failure_id], lane_id
                    )
                )
            continue
        signature = build_failure_duplicate_signature(card)
        previous_failure = signatures.get(signature)
        if previous_failure is not None:
            errors.append(
                "lane collection: duplicate failure signature {0} and {1}".format(
                    previous_failure, failure_id
                )
            )
            continue
        merged[failure_id] = card
        canonical_bytes[failure_id] = serialized
        owning_lanes[failure_id] = lane_id
        signatures[signature] = failure_id
    return merged, sorted(set(errors))


def integrate_lane_dossier_records(
    dossiers: Sequence[Mapping[str, object]],
    plan_rows: Sequence[Mapping[str, str]],
    paper_page_counts: Mapping[str, int],
    known_pattern_ids: Set[str],
    reader_agent_ids: Mapping[str, str],
) -> Tuple[List[Dict[str, str]], Dict[str, Mapping[str, object]], List[object]]:
    """Integrate only a completely valid dossier collection before review."""

    errors = validate_lane_dossier_collection(
        dossiers, plan_rows, paper_page_counts, known_pattern_ids
    )
    if errors:
        raise ValueError("invalid lane dossier collection: " + "; ".join(errors))
    updated_rows = [
        {field: str(row.get(field, "")) for field in ADVERSARIAL_PLAN_FIELDS}
        for row in plan_rows
    ]
    lane_failure_records: List[Tuple[str, Mapping[str, object]]] = []
    conflict_candidates: List[object] = []
    for dossier in sorted(dossiers, key=lambda item: str(item.get("lane_id", ""))):
        lane_id = str(dossier.get("lane_id", ""))
        reader_agent_id = str(reader_agent_ids.get(lane_id, ""))
        if not reader_agent_id or reader_agent_id == "PENDING":
            raise ValueError("missing terminal reader agent ID for " + lane_id)
        updated_rows = apply_lane_dossier_to_plan_rows(
            updated_rows, dossier, reader_agent_id
        )
        failure_records = dossier.get("failure_cards", [])
        if not isinstance(failure_records, list):
            raise ValueError("failure_cards must be a list")
        for card in failure_records:
            if not isinstance(card, Mapping):
                raise ValueError("failure card must be an object")
            lane_failure_records.append((lane_id, card))
        conflicts = dossier.get("conflict_candidates", [])
        if not isinstance(conflicts, list):
            raise ValueError("conflict_candidates must be a list")
        conflict_candidates.extend(conflicts)
    cards, merge_errors = merge_failure_card_records(lane_failure_records)
    if merge_errors:
        raise ValueError("invalid lane failure merge: " + "; ".join(merge_errors))
    return updated_rows, cards, conflict_candidates


def validate_counterexample_report(
    report_text: str,
    plan_rows: Sequence[Mapping[str, str]],
    failure_cards: Mapping[str, Mapping[str, object]],
    conflict_rows: Sequence[Mapping[str, str]],
    paper_page_counts: Mapping[str, int],
) -> List[str]:
    """Validate exact accounting and scope of the terminal G06 report."""

    errors: List[str] = []
    lines = report_text.splitlines()
    for heading in G06_REPORT_HEADINGS:
        count = lines.count(heading)
        if count != 1:
            errors.append(
                "G06 report: expected one heading {0}, found {1}".format(
                    heading, count
                )
            )
    paper_rows = [row for row in plan_rows if row.get("subject_type") == "PAPER"]
    pattern_rows = [row for row in plan_rows if row.get("subject_type") == "PATTERN"]
    counts = {
        "Papers inspected": len(paper_rows),
        "Pages inspected": sum(
            paper_page_counts.get(row.get("subject_id", ""), 0) for row in paper_rows
        ),
        "Patterns disposed": len(pattern_rows),
        "Failure cards": len(failure_cards),
        "Evidence conflicts": len(conflict_rows),
        "Explicit evidence gaps": sum(
            row.get("terminal_disposition") == "EXPLICIT_EVIDENCE_GAP"
            for row in pattern_rows
        ),
    }
    for label, count in counts.items():
        marker = "{0}: {1}".format(label, count)
        if marker not in report_text:
            errors.append("G06 report: missing exact accounting marker " + marker)
    if not conflict_rows and "No qualifying two-sided evidence conflict was found." not in report_text:
        errors.append("G06 report: empty conflict ledger lacks explicit disclosure")
    forbidden_patterns = (
        r"\b(?:ARCH|XFER|EXP)-[A-Z0-9-]+",
        r"\b(?:improved|reduced|lowered)\s+(?:RAM|RSS|latency)\b",
        r"\b(?:reproduced|benchmarked|implemented)\s+(?:by|in)\s+(?:G06|this campaign)\b",
    )
    if any(re.search(pattern, report_text, flags=re.IGNORECASE) for pattern in forbidden_patterns):
        errors.append("G06 report: later-goal or measured-performance claim is forbidden")
    return sorted(set(errors))


def calculate_adversarial_result_checksum(
    row: Mapping[str, str],
    failure_cards: Mapping[str, Mapping[str, object]],
    source_hashes: Mapping[str, Tuple[str, str]],
) -> str:
    """Bind one terminal disposition to source and linked-card bytes."""

    row_payload = {
        field: row.get(field, "")
        for field in ADVERSARIAL_PLAN_FIELDS
        if field != "result_checksum"
    }
    paper_ids = split_pipe_values_sorted(row.get("source_paper_ids", ""))
    failure_ids = split_pipe_values_sorted(row.get("failure_ids", ""))
    payload = {
        "row": row_payload,
        "source_hashes": {
            paper_id: list(source_hashes.get(paper_id, ("MISSING", "MISSING")))
            for paper_id in paper_ids
        },
        "failure_cards": {
            failure_id: failure_cards.get(failure_id, {"missing": True})
            for failure_id in failure_ids
        },
    }
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(canonical).hexdigest().upper()


def validate_adversarial_result_checksums(
    rows: Sequence[Mapping[str, str]],
    failure_cards: Mapping[str, Mapping[str, object]],
    source_hashes: Mapping[str, Tuple[str, str]],
) -> List[str]:
    """Recompute and compare every terminal adversarial result checksum."""

    errors: List[str] = []
    for row_index, row in enumerate(rows, start=1):
        expected = calculate_adversarial_result_checksum(row, failure_cards, source_hashes)
        actual = row.get("result_checksum", "")
        if actual != expected:
            errors.append(
                "adversarial plan row {0}: result_checksum mismatch for {1}".format(
                    row_index, row.get("subject_id", "UNKNOWN")
                )
            )
    return errors


def finalize_adversarial_plan_rows(
    rows: Sequence[Mapping[str, str]],
    failure_cards: Mapping[str, Mapping[str, object]],
    source_hashes: Mapping[str, Tuple[str, str]],
    reviewer_agent_id: str,
) -> List[Dict[str, str]]:
    """Bind one independent reviewer and canonical checksum to every terminal row."""

    if not reviewer_agent_id or reviewer_agent_id == "PENDING":
        raise ValueError("terminal reviewer_agent_id is required")
    finalized: List[Dict[str, str]] = []
    for row in rows:
        updated = {field: str(row.get(field, "")) for field in ADVERSARIAL_PLAN_FIELDS}
        if updated["inspection_status"] != "COMPLETE":
            raise ValueError("cannot finalize an incomplete adversarial-plan row")
        if updated["terminal_disposition"] == "PENDING":
            raise ValueError("cannot finalize a pending adversarial disposition")
        updated["reviewer_agent_id"] = reviewer_agent_id
        updated["result_checksum"] = calculate_adversarial_result_checksum(
            updated, failure_cards, source_hashes
        )
        finalized.append(updated)
    return finalized
