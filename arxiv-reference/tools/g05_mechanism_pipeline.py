#!/usr/bin/env python3
"""Deterministic G05 selection and mechanism-contract validation."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Sequence, Set


READING_PLAN_FIELDS = (
    "selection_rank",
    "batch_id",
    "batch_position",
    "paper_id",
    "g04_queue_rank",
    "relevance_score",
    "page_count",
    "pdf_path",
    "pdf_sha256",
    "text_path",
    "text_sha256",
    "architecture_question_ids",
    "selection_basis",
    "reader_agent_id",
    "reviewer_agent_id",
    "reading_status",
    "terminal_outcome",
    "card_ids",
    "reading_coverage",
    "no_mechanism_rationale",
    "result_checksum",
)

MECHANISM_CARD_FIELDS = frozenset(
    {
        "pattern_id",
        "name",
        "epistemic_label",
        "source_paper_ids",
        "source_pointers",
        "source_domain",
        "problem",
        "invariant",
        "mechanism",
        "data_arrangement",
        "access_schedule",
        "resident_state",
        "streamed_state",
        "recomputed_state",
        "resource_model",
        "works_when",
        "fails_when",
        "unknown_when",
        "knight_bus_algorithm_families",
        "a007_consequence",
        "falsifying_test",
        "falsifying_experiment_id",
        "evidence_grade",
        "confidence_rationale",
        "related_pattern_ids",
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
    {
        "pointer_id",
        "paper_id",
        "page",
        "locator_type",
        "locator_value",
        "claim_scope",
    }
)

RESOURCE_TERM_FIELDS = frozenset(
    {
        "status",
        "expression",
        "source_pointer_ids",
        "premises",
        "assumptions",
        "uncertainty",
        "measurement_needed",
    }
)

RESOURCE_MODEL_FIELDS = frozenset(
    {"ram", "io", "preprocessing", "persistent_storage", "temporary_storage"}
)

FALSIFYING_TEST_FIELDS = frozenset(
    {"fixture", "independent_oracle", "controlled_variables", "failure_signal", "scope"}
)

PATTERN_EDGE_FIELDS = (
    "edge_id",
    "source_pattern_id",
    "target_pattern_id",
    "relationship_type",
    "rationale",
    "epistemic_label",
    "source_paper_ids",
    "source_pointer_ids",
)

PATTERN_RELATIONSHIP_TYPES = {
    "SHARES_MECHANISM_WITH",
    "COMPLEMENTS",
    "CONTRADICTS",
    "SUBSUMES",
}

SYMMETRIC_RELATIONSHIP_TYPES = {
    "SHARES_MECHANISM_WITH",
    "COMPLEMENTS",
    "CONTRADICTS",
}

TERMINAL_PAPER_OUTCOMES = {"MECHANISM_EXTRACTED", "NO_MECHANISM"}
READING_STATUS_VALUES = {"PENDING", "READING", "COMPLETE"}
RESOURCE_STATUS_VALUES = {"SOURCED", "DERIVED", "UNKNOWN"}
CLAIM_TYPE_VALUES = {"SOURCE_CLAIM", "DERIVED_INFERENCE"}
LOCATOR_TYPE_VALUES = {
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
EVIDENCE_GRADE_VALUES = {
    "A_REPRODUCED",
    "B_CODE_BACKED",
    "C_PAPER_BENCHMARK",
    "D_THEORETICAL_OR_INCOMPLETE",
    "E_CONTRADICTED",
}
PATTERN_ID_PATTERN = re.compile(r"^PAT-(?:[A-Z0-9]+-){3}[A-Z0-9]+$")
SHA256_PATTERN = re.compile(r"^[0-9A-Fa-f]{64}$")


def read_tsv_records_exact(path: Path) -> List[Dict[str, str]]:
    """Read one UTF-8 TSV file as dictionaries."""

    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def hash_file_bytes_sha256(path: Path) -> str:
    """Hash one local file without interpreting its contents."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest().upper()


def split_pipe_values_sorted(value: object) -> List[str]:
    """Normalize a pipe-delimited field into sorted values."""

    return sorted({part.strip() for part in str(value or "").split("|") if part.strip()})


def derive_eligible_paper_records(reference_root: Path) -> List[Dict[str, object]]:
    """Derive the exact checksum-linked G04-eligible paper inventory."""

    manifest_rows = read_tsv_records_exact(reference_root / "sources" / "paper-manifest.tsv")
    ledger_rows = read_tsv_records_exact(reference_root / "sources" / "download-ledger.tsv")
    manifest_by_id = {row["paper_id"]: row for row in manifest_rows}
    eligible: List[Dict[str, object]] = []
    for ledger in ledger_rows:
        if ledger.get("acquisition_status") != "ACQUIRED" or ledger.get("parse_status") != "PARSED":
            continue
        paper_id = ledger.get("paper_id", "")
        manifest = manifest_by_id.get(paper_id)
        if manifest is None or manifest.get("selection_status") not in {"DEEP_READ", "READ_COMPLETE"}:
            continue
        if manifest.get("local_path") != ledger.get("local_path"):
            continue
        if manifest.get("sha256", "").upper() != ledger.get("source_checksum", "").upper():
            continue
        eligible.append(
            {
                "paper_id": paper_id,
                "g04_queue_rank": int(ledger["queue_rank"]),
                "relevance_score": int(manifest["relevance_score"]),
                "page_count": int(ledger["page_count"]),
                "pdf_path": ledger["local_path"],
                "pdf_sha256": ledger["source_checksum"].upper(),
                "text_path": ledger["extracted_path"],
                "text_sha256": ledger["extracted_checksum"].upper(),
                "architecture_question_ids": manifest["architecture_question_ids"],
                "selection_status": manifest["selection_status"],
                "title": manifest["title"],
            }
        )
    return sorted(eligible, key=lambda row: int(row["g04_queue_rank"]))


def derive_selected_paper_records(reference_root: Path) -> List[Dict[str, object]]:
    """Select the deterministic top 25 G04-eligible papers."""

    eligible = derive_eligible_paper_records(reference_root)
    ranked = sorted(
        eligible,
        key=lambda row: (
            -int(row["relevance_score"]),
            int(row["g04_queue_rank"]),
            str(row["paper_id"]),
        ),
    )
    return ranked[:25]


def validate_selected_paper_records(
    selected_rows: Sequence[Mapping[str, object]], reference_root: Path
) -> List[str]:
    """Validate G05 selection membership, uniqueness, and order."""

    errors: List[str] = []
    eligible_ids = {
        str(row["paper_id"]) for row in derive_eligible_paper_records(reference_root)
    }
    selected_ids = [str(row.get("paper_id", "")) for row in selected_rows]
    if len(selected_rows) != 25:
        errors.append("selection must contain exactly 25 papers")
    if len(selected_ids) != len(set(selected_ids)):
        errors.append("selection contains duplicate paper identities")
    for paper_id in selected_ids:
        if paper_id not in eligible_ids:
            errors.append("selection contains ineligible paper {0}".format(paper_id))
    expected_ids = [
        str(row["paper_id"]) for row in derive_selected_paper_records(reference_root)
    ]
    if selected_ids != expected_ids:
        errors.append("selection does not match deterministic eligible ordering")
    return sorted(set(errors))


def validate_reading_plan_rows(
    rows: Sequence[Mapping[str, str]], reference_root: Path
) -> List[str]:
    """Validate frozen reading-plan provenance and local checksums."""

    errors: List[str] = []
    selected = derive_selected_paper_records(reference_root)
    errors.extend(validate_selected_paper_records(selected, reference_root))
    if len(rows) != 25:
        errors.append("reading plan must contain exactly 25 rows")
        return sorted(set(errors))

    for index, (row, expected) in enumerate(zip(rows, selected), start=1):
        prefix = "g05-reading-plan.tsv row {0}".format(index + 1)
        expected_values = {
            "selection_rank": str(index),
            "batch_id": "G05-BATCH-{0}".format(((index - 1) % 5) + 1),
            "batch_position": str(((index - 1) // 5) + 1),
            "paper_id": str(expected["paper_id"]),
            "g04_queue_rank": str(expected["g04_queue_rank"]),
            "relevance_score": str(expected["relevance_score"]),
            "page_count": str(expected["page_count"]),
            "pdf_path": str(expected["pdf_path"]),
            "pdf_sha256": str(expected["pdf_sha256"]),
            "text_path": str(expected["text_path"]),
            "text_sha256": str(expected["text_sha256"]),
            "architecture_question_ids": str(expected["architecture_question_ids"]),
            "selection_basis": "SCORE_DESC_QUEUE_ASC_PAPER_ID_ASC",
        }
        for field_name, expected_value in expected_values.items():
            if row.get(field_name) != expected_value:
                errors.append("{0}: {1} mismatch".format(prefix, field_name))
        if row.get("reading_status") not in READING_STATUS_VALUES:
            errors.append("{0}: invalid reading_status".format(prefix))
        if row.get("terminal_outcome") not in TERMINAL_PAPER_OUTCOMES | {"PENDING"}:
            errors.append("{0}: invalid terminal_outcome".format(prefix))
        for path_field, checksum_field in (
            ("pdf_path", "pdf_sha256"),
            ("text_path", "text_sha256"),
        ):
            local_path = reference_root / row.get(path_field, "")
            if not local_path.is_file():
                errors.append("{0}: missing local {1}".format(prefix, path_field))
            elif hash_file_bytes_sha256(local_path) != row.get(checksum_field, "").upper():
                errors.append("{0}: {1} checksum mismatch".format(prefix, path_field))

    batches = Counter(row.get("batch_id", "") for row in rows)
    if batches != Counter({"G05-BATCH-{0}".format(value): 5 for value in range(1, 6)}):
        errors.append("reading plan must contain five disjoint five-paper batches")
    return sorted(set(errors))


def validate_completed_reading_rows(rows: Sequence[Mapping[str, str]]) -> List[str]:
    """Validate terminal outcomes for all 25 selected papers."""

    errors: List[str] = []
    if len(rows) != 25:
        errors.append("completed reading plan must contain exactly 25 rows")
    paper_ids = [row.get("paper_id", "") for row in rows]
    if len(paper_ids) != len(set(paper_ids)):
        errors.append("completed reading plan contains duplicate papers")
    for index, row in enumerate(rows, start=2):
        prefix = "g05-reading-plan.tsv row {0}".format(index)
        outcome = row.get("terminal_outcome", "")
        if row.get("reading_status") != "COMPLETE" or outcome not in TERMINAL_PAPER_OUTCOMES:
            errors.append("{0}: terminal extraction outcome is required".format(prefix))
            continue
        expected_coverage = "ALL_PAGES:1-{0}".format(row.get("page_count", ""))
        if row.get("reading_coverage") != expected_coverage:
            errors.append("{0}: complete all-page reading coverage is required".format(prefix))
        if row.get("reader_agent_id") in {"", "UNASSIGNED"}:
            errors.append("{0}: reader_agent_id is required".format(prefix))
        if row.get("reviewer_agent_id") in {"", "UNASSIGNED"}:
            errors.append("{0}: reviewer_agent_id is required".format(prefix))
        if not SHA256_PATTERN.fullmatch(row.get("result_checksum", "")):
            errors.append("{0}: result_checksum must be SHA-256".format(prefix))
        if outcome == "MECHANISM_EXTRACTED":
            if row.get("card_ids") in {"", "NONE"}:
                errors.append("{0}: extracted outcome requires card_ids".format(prefix))
            if row.get("no_mechanism_rationale") != "NOT_APPLICABLE":
                errors.append("{0}: extracted outcome requires NOT_APPLICABLE rationale".format(prefix))
        elif outcome == "NO_MECHANISM":
            if row.get("card_ids") != "NONE":
                errors.append("{0}: NO_MECHANISM forbids card_ids".format(prefix))
            rationale = row.get("no_mechanism_rationale", "").strip()
            if rationale in {"", "NOT_APPLICABLE", "NONE"} or len(rationale.split()) < 6:
                errors.append("{0}: NO_MECHANISM requires substantive rationale".format(prefix))
    return sorted(set(errors))


def validate_claim_object_fields(
    claim: object, pointer_ids: Set[str], display_name: str
) -> List[str]:
    """Validate one atomic source or derived claim object."""

    if not isinstance(claim, Mapping):
        return ["{0}: claim must be a mapping".format(display_name)]
    errors: List[str] = []
    if set(claim) != CLAIM_OBJECT_FIELDS:
        errors.append("{0}: claim fields must match the frozen envelope".format(display_name))
    claim_type = str(claim.get("claim_type", ""))
    if claim_type not in CLAIM_TYPE_VALUES:
        errors.append("{0}: invalid claim_type".format(display_name))
    if not str(claim.get("text", "")).strip():
        errors.append("{0}: claim text is required".format(display_name))
    source_ids = claim.get("source_pointer_ids")
    if not isinstance(source_ids, list) or any(value not in pointer_ids for value in source_ids):
        errors.append("{0}: source_pointer_ids must resolve".format(display_name))
    premises = claim.get("premises")
    assumptions = claim.get("assumptions")
    uncertainty = str(claim.get("uncertainty", "")).strip()
    if claim_type == "SOURCE_CLAIM" and (not source_ids or premises != [] or assumptions != []):
        errors.append("{0}: SOURCE_CLAIM requires pointers and no derived premises".format(display_name))
    if claim_type == "DERIVED_INFERENCE":
        if not isinstance(premises, list) or not premises:
            errors.append("{0}: DERIVED_INFERENCE requires premises".format(display_name))
        if not isinstance(assumptions, list) or not assumptions:
            errors.append("{0}: DERIVED_INFERENCE requires assumptions".format(display_name))
        if not uncertainty:
            errors.append("{0}: DERIVED_INFERENCE requires uncertainty".format(display_name))
    if not uncertainty:
        errors.append("{0}: uncertainty is required".format(display_name))
    return errors


def validate_resource_term_fields(
    term: object, pointer_ids: Set[str], display_name: str
) -> List[str]:
    """Validate one sourced, derived, or unknown resource term."""

    if not isinstance(term, Mapping):
        return ["{0}: resource term must be a mapping".format(display_name)]
    errors: List[str] = []
    if set(term) != RESOURCE_TERM_FIELDS:
        errors.append("{0}: resource fields must match the frozen envelope".format(display_name))
    status = str(term.get("status", ""))
    expression = str(term.get("expression", "")).strip()
    source_ids = term.get("source_pointer_ids")
    premises = term.get("premises")
    assumptions = term.get("assumptions")
    uncertainty = str(term.get("uncertainty", "")).strip()
    measurement = str(term.get("measurement_needed", "")).strip()
    if status not in RESOURCE_STATUS_VALUES:
        errors.append("{0}: invalid resource status".format(display_name))
    if not isinstance(source_ids, list) or any(value not in pointer_ids for value in source_ids):
        errors.append("{0}: source_pointer_ids must resolve".format(display_name))
    if status == "SOURCED" and (not source_ids or not expression or expression == "UNKNOWN"):
        errors.append("{0}: SOURCED resource requires expression and pointers".format(display_name))
    elif status == "DERIVED":
        if not expression or expression == "UNKNOWN":
            errors.append("{0}: DERIVED resource requires expression".format(display_name))
        if not isinstance(premises, list) or not premises:
            errors.append("{0}: DERIVED resource requires premises".format(display_name))
        if not isinstance(assumptions, list) or not assumptions:
            errors.append("{0}: DERIVED resource requires assumptions".format(display_name))
        if not uncertainty:
            errors.append("{0}: DERIVED resource requires uncertainty".format(display_name))
    elif status == "UNKNOWN":
        if expression != "UNKNOWN" or source_ids != [] or premises != []:
            errors.append("{0}: UNKNOWN resource must use the canonical UNKNOWN notation".format(display_name))
        if not uncertainty or not measurement:
            errors.append("{0}: UNKNOWN resource requires uncertainty and measurement".format(display_name))
    return errors


def validate_mechanism_card_record(
    card: Mapping[str, object], paper_page_counts: Mapping[str, int]
) -> List[str]:
    """Validate one complete G05 mechanism-card payload."""

    errors: List[str] = []
    if set(card) != MECHANISM_CARD_FIELDS:
        errors.append("mechanism card fields must match the frozen envelope")
    pattern_id = str(card.get("pattern_id", ""))
    if not PATTERN_ID_PATTERN.fullmatch(pattern_id):
        errors.append("mechanism card pattern_id must use a four-word slug")
    if not str(card.get("name", "")).strip():
        errors.append("mechanism card name is required")
    if card.get("epistemic_label") != "SOURCE_CLAIM":
        errors.append("G05 mechanism card must use SOURCE_CLAIM")
    source_paper_ids = card.get("source_paper_ids")
    if not isinstance(source_paper_ids, list) or not source_paper_ids:
        errors.append("mechanism card requires source_paper_ids")
        source_paper_ids = []
    if any(paper_id not in paper_page_counts for paper_id in source_paper_ids):
        errors.append("mechanism card source paper is not READ_COMPLETE eligible")

    pointer_ids: Set[str] = set()
    pointers = card.get("source_pointers")
    if not isinstance(pointers, list) or not pointers:
        errors.append("mechanism card requires source_pointers")
        pointers = []
    for index, pointer in enumerate(pointers, start=1):
        display = "source_pointers[{0}]".format(index)
        if not isinstance(pointer, Mapping):
            errors.append("{0}: pointer must be a mapping".format(display))
            continue
        allowed_fields = SOURCE_POINTER_FIELDS | {"short_quote"}
        if set(pointer) < SOURCE_POINTER_FIELDS or not set(pointer) <= allowed_fields:
            errors.append("{0}: pointer fields must match the frozen envelope".format(display))
        pointer_id = str(pointer.get("pointer_id", ""))
        if not re.fullmatch(r"SP-[0-9]{3}", pointer_id) or pointer_id in pointer_ids:
            errors.append("{0}: pointer_id must be unique SP-NNN".format(display))
        pointer_ids.add(pointer_id)
        paper_id = str(pointer.get("paper_id", ""))
        if paper_id not in source_paper_ids or paper_id not in paper_page_counts:
            errors.append("{0}: paper_id must resolve to card sources".format(display))
        page = pointer.get("page")
        if not isinstance(page, int) or page < 1 or page > paper_page_counts.get(paper_id, 0):
            errors.append("{0}: page must be within the G04 PDF".format(display))
        if pointer.get("locator_type") not in LOCATOR_TYPE_VALUES:
            errors.append("{0}: invalid locator_type".format(display))
        for field_name in ("locator_value", "claim_scope"):
            if not str(pointer.get(field_name, "")).strip():
                errors.append("{0}: {1} is required".format(display, field_name))
        if "short_quote" in pointer:
            quote = str(pointer.get("short_quote", ""))
            if len(quote.split()) > 25 or len(quote) > 200:
                errors.append("{0}: short_quote exceeds the quotation limit".format(display))

    for field_name in (
        "problem",
        "invariant",
        "mechanism",
        "data_arrangement",
        "access_schedule",
        "resident_state",
        "streamed_state",
        "recomputed_state",
        "a007_consequence",
        "confidence_rationale",
    ):
        errors.extend(
            validate_claim_object_fields(card.get(field_name), pointer_ids, field_name)
        )
    confidence_rationale = card.get("confidence_rationale")
    if (
        isinstance(confidence_rationale, Mapping)
        and confidence_rationale.get("claim_type") != "DERIVED_INFERENCE"
    ):
        errors.append(
            "confidence_rationale: extractor appraisal must be DERIVED_INFERENCE"
        )
    for field_name in ("works_when", "fails_when", "unknown_when"):
        values = card.get(field_name)
        if not isinstance(values, list) or not values:
            errors.append("{0}: at least one claim is required".format(field_name))
            continue
        for index, value in enumerate(values, start=1):
            errors.extend(
                validate_claim_object_fields(
                    value, pointer_ids, "{0}[{1}]".format(field_name, index)
                )
            )

    resource_model = card.get("resource_model")
    if not isinstance(resource_model, Mapping) or set(resource_model) != RESOURCE_MODEL_FIELDS:
        errors.append("resource_model fields must match the frozen envelope")
    else:
        for field_name in sorted(RESOURCE_MODEL_FIELDS):
            errors.extend(
                validate_resource_term_fields(
                    resource_model.get(field_name), pointer_ids, "resource_model." + field_name
                )
            )

    algorithm_families = card.get("knight_bus_algorithm_families")
    if not isinstance(algorithm_families, list) or not algorithm_families:
        errors.append("knight_bus_algorithm_families requires at least one value")
    related = card.get("related_pattern_ids")
    if not isinstance(related, list) or any(not PATTERN_ID_PATTERN.fullmatch(str(value)) for value in related):
        errors.append("related_pattern_ids must contain only pattern IDs")
    if card.get("evidence_grade") not in EVIDENCE_GRADE_VALUES:
        errors.append("mechanism card has invalid evidence_grade")

    falsifying_test = card.get("falsifying_test")
    if not isinstance(falsifying_test, Mapping) or set(falsifying_test) != FALSIFYING_TEST_FIELDS:
        errors.append("falsifying_test fields must match the frozen envelope")
    else:
        for field_name in FALSIFYING_TEST_FIELDS - {"controlled_variables"}:
            if not str(falsifying_test.get(field_name, "")).strip():
                errors.append("falsifying_test.{0} is required".format(field_name))
        controlled = falsifying_test.get("controlled_variables")
        if not isinstance(controlled, list) or not controlled:
            errors.append("falsifying_test.controlled_variables is required")
    expected_reservation = "RESERVED-G09-FOR-" + pattern_id
    if card.get("falsifying_experiment_id") != expected_reservation:
        errors.append("falsifying_experiment_id must use the G09 reservation lifecycle")
    return sorted(set(errors))


def validate_mechanism_card_collection(cards: Sequence[Mapping[str, object]]) -> List[str]:
    """Reject duplicate IDs and duplicate mechanism payloads."""

    errors: List[str] = []
    pattern_ids = [str(card.get("pattern_id", "")) for card in cards]
    for pattern_id, count in Counter(pattern_ids).items():
        if count > 1:
            errors.append("duplicate mechanism card ID {0}".format(pattern_id))
    fingerprints: Dict[str, str] = {}
    for card in cards:
        duplicate_scope = {
            field_name: card.get(field_name)
            for field_name in ("source_paper_ids", "problem", "invariant", "mechanism", "data_arrangement", "access_schedule")
        }
        fingerprint = hashlib.sha256(
            json.dumps(duplicate_scope, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        if fingerprint in fingerprints:
            errors.append(
                "duplicate mechanism content between {0} and {1}".format(
                    fingerprints[fingerprint], card.get("pattern_id", "")
                )
            )
        else:
            fingerprints[fingerprint] = str(card.get("pattern_id", ""))
    return sorted(set(errors))


def parse_mechanism_card_file(path: Path) -> Dict[str, object]:
    """Parse exactly one canonical fenced JSON card payload."""

    text = path.read_text(encoding="utf-8")
    matches = re.findall(r"```json\s*\n(.*?)\n```", text, flags=re.DOTALL)
    if len(matches) != 1:
        raise ValueError("mechanism card must contain exactly one fenced json object")
    try:
        payload = json.loads(matches[0])
    except json.JSONDecodeError as error:
        raise ValueError("mechanism card JSON is invalid") from error
    if not isinstance(payload, dict):
        raise ValueError("mechanism card JSON must be an object")
    if payload.get("pattern_id") != path.stem:
        raise ValueError("mechanism card pattern_id must match filename")
    return payload


def parse_batch_dossier_cards(path: Path) -> List[Dict[str, object]]:
    """Parse every canonical mechanism-card JSON block in one batch dossier."""

    text = path.read_text(encoding="utf-8")
    payloads: List[Dict[str, object]] = []
    for block_number, block in enumerate(
        re.findall(r"```json\s*\n(.*?)\n```", text, flags=re.DOTALL), start=1
    ):
        try:
            payload = json.loads(block)
        except json.JSONDecodeError as error:
            raise ValueError(
                "batch dossier JSON block {0} is invalid".format(block_number)
            ) from error
        if not isinstance(payload, dict) or not PATTERN_ID_PATTERN.fullmatch(
            str(payload.get("pattern_id", ""))
        ):
            raise ValueError(
                "batch dossier JSON block {0} is not a mechanism card".format(
                    block_number
                )
            )
        payloads.append(payload)
    return payloads


def validate_pattern_edge_rows(
    rows: Sequence[Mapping[str, str]], pattern_ids: Set[str]
) -> List[str]:
    """Validate typed pattern edges and canonical symmetry."""

    errors: List[str] = []
    edge_ids: Set[str] = set()
    canonical_keys: Set[tuple[str, str, str, str, str]] = set()
    for index, row in enumerate(rows, start=2):
        prefix = "pattern-edges.tsv row {0}".format(index)
        if set(row) != set(PATTERN_EDGE_FIELDS):
            errors.append("{0}: fields must match the frozen header".format(prefix))
        edge_id = row.get("edge_id", "")
        if not re.fullmatch(r"PEDGE-[0-9]{4}", edge_id) or edge_id in edge_ids:
            errors.append("{0}: edge_id must be unique PEDGE-NNNN".format(prefix))
        edge_ids.add(edge_id)
        source = row.get("source_pattern_id", "")
        target = row.get("target_pattern_id", "")
        relationship = row.get("relationship_type", "")
        if source not in pattern_ids or target not in pattern_ids:
            errors.append("{0}: edge endpoints must resolve".format(prefix))
        if source == target:
            errors.append("{0}: self-edge is forbidden".format(prefix))
        if relationship not in PATTERN_RELATIONSHIP_TYPES:
            errors.append("{0}: invalid relationship_type".format(prefix))
        if relationship in SYMMETRIC_RELATIONSHIP_TYPES and source >= target:
            errors.append("{0}: symmetric edge endpoints are not canonical".format(prefix))
        if not str(row.get("rationale", "")).strip():
            errors.append("{0}: rationale is required".format(prefix))
        epistemic = row.get("epistemic_label", "")
        if epistemic not in CLAIM_TYPE_VALUES:
            errors.append("{0}: invalid epistemic_label".format(prefix))
        if epistemic == "DERIVED_INFERENCE":
            rationale = row.get("rationale", "")
            if not all(marker in rationale for marker in ("premises=", "assumptions=", "uncertainty=")):
                errors.append("{0}: derived rationale requires premises, assumptions, and uncertainty".format(prefix))
        key = (
            min(source, target) if relationship in SYMMETRIC_RELATIONSHIP_TYPES else source,
            max(source, target) if relationship in SYMMETRIC_RELATIONSHIP_TYPES else target,
            relationship,
            row.get("source_paper_ids", ""),
            row.get("source_pointer_ids", ""),
        )
        if key in canonical_keys:
            errors.append("{0}: duplicate pattern edge".format(prefix))
        canonical_keys.add(key)
    return sorted(set(errors))


def calculate_reading_result_checksum(
    row: Mapping[str, str], cards_by_id: Mapping[str, Mapping[str, object]]
) -> str:
    """Bind one terminal reading row to its canonical mechanism payloads."""

    card_ids = split_pipe_values_sorted(row.get("card_ids", ""))
    if card_ids == ["NONE"]:
        card_ids = []
    payload = {
        "paper_id": row.get("paper_id", ""),
        "reader_agent_id": row.get("reader_agent_id", ""),
        "reviewer_agent_id": row.get("reviewer_agent_id", ""),
        "terminal_outcome": row.get("terminal_outcome", ""),
        "card_ids": card_ids,
        "reading_coverage": row.get("reading_coverage", ""),
        "no_mechanism_rationale": row.get("no_mechanism_rationale", ""),
        "card_payloads": [cards_by_id[card_id] for card_id in card_ids],
    }
    canonical = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest().upper()


def validate_reading_result_checksums(
    rows: Sequence[Mapping[str, str]],
    cards_by_id: Mapping[str, Mapping[str, object]],
) -> List[str]:
    """Verify every terminal row against its canonical result checksum."""

    errors: List[str] = []
    for index, row in enumerate(rows, start=2):
        if row.get("reading_status") != "COMPLETE":
            continue
        try:
            expected = calculate_reading_result_checksum(row, cards_by_id)
        except KeyError as error:
            errors.append(
                "g05-reading-plan.tsv row {0}: result_checksum references missing card {1}".format(
                    index, error.args[0]
                )
            )
            continue
        if row.get("result_checksum", "").upper() != expected:
            errors.append(
                "g05-reading-plan.tsv row {0}: result_checksum mismatch".format(index)
            )
    return sorted(set(errors))


def validate_output_crosslinks_complete(
    cards: Sequence[Mapping[str, object]],
    edge_rows: Sequence[Mapping[str, str]],
    plan_rows: Sequence[Mapping[str, str]],
) -> List[str]:
    """Validate card, edge, pointer, and terminal-plan foreign keys."""

    errors: List[str] = []
    cards_by_id = {str(card.get("pattern_id", "")): card for card in cards}
    terminal_plan = {
        row.get("paper_id", ""): row
        for row in plan_rows
        if row.get("reading_status") == "COMPLETE"
        and row.get("terminal_outcome") in TERMINAL_PAPER_OUTCOMES
    }
    referenced_cards: Set[str] = set()
    for paper_id, row in terminal_plan.items():
        card_ids = split_pipe_values_sorted(row.get("card_ids", ""))
        if row.get("terminal_outcome") == "NO_MECHANISM":
            card_ids = [] if card_ids == ["NONE"] else card_ids
        for card_id in card_ids:
            card = cards_by_id.get(card_id)
            if card is None:
                errors.append("{0}: terminal plan references missing card {1}".format(paper_id, card_id))
                continue
            referenced_cards.add(card_id)
            if paper_id not in card.get("source_paper_ids", []):
                errors.append("{0}: card {1} does not cite its plan paper".format(paper_id, card_id))

    for pattern_id, card in cards_by_id.items():
        if pattern_id not in referenced_cards:
            errors.append("orphan mechanism card is not referenced by any terminal paper: " + pattern_id)
        for paper_id in card.get("source_paper_ids", []):
            plan = terminal_plan.get(str(paper_id))
            if plan is None or plan.get("terminal_outcome") != "MECHANISM_EXTRACTED":
                errors.append("{0}: source paper is not terminal MECHANISM_EXTRACTED".format(pattern_id))
            elif pattern_id not in split_pipe_values_sorted(plan.get("card_ids", "")):
                errors.append("{0}: source paper plan does not link back to card".format(pattern_id))

    qualified_pointers: Dict[str, str] = {}
    for pattern_id, card in cards_by_id.items():
        for pointer in card.get("source_pointers", []):
            if not isinstance(pointer, Mapping):
                continue
            pointer_id = str(pointer.get("pointer_id", ""))
            qualified_pointers[pattern_id + "#" + pointer_id] = str(
                pointer.get("paper_id", "")
            )

    adjacency: Dict[str, Set[str]] = {pattern_id: set() for pattern_id in cards_by_id}
    for index, edge in enumerate(edge_rows, start=2):
        prefix = "pattern-edges.tsv row {0}".format(index)
        source = edge.get("source_pattern_id", "")
        target = edge.get("target_pattern_id", "")
        if source in adjacency and target in adjacency:
            adjacency[source].add(target)
            adjacency[target].add(source)
        qualified_ids = split_pipe_values_sorted(edge.get("source_pointer_ids", ""))
        endpoint_ids = {source, target}
        pointer_papers: Set[str] = set()
        for qualified_id in qualified_ids:
            if qualified_id not in qualified_pointers:
                errors.append("{0}: source pointer does not resolve: {1}".format(prefix, qualified_id))
                continue
            pointer_pattern_id = qualified_id.rsplit("#", 1)[0]
            if pointer_pattern_id not in endpoint_ids:
                errors.append("{0}: source pointer must belong to an edge endpoint".format(prefix))
            pointer_papers.add(qualified_pointers[qualified_id])
        declared_papers = set(split_pipe_values_sorted(edge.get("source_paper_ids", "")))
        if pointer_papers != declared_papers:
            errors.append("{0}: source_paper_ids do not match qualified pointers".format(prefix))

    for pattern_id, card in cards_by_id.items():
        related = card.get("related_pattern_ids")
        expected = sorted(adjacency.get(pattern_id, set()))
        if related != expected:
            errors.append("{0}: related_pattern_ids do not match typed edges".format(pattern_id))
    return sorted(set(errors))


def validate_read_complete_transitions(
    manifest_rows: Sequence[Mapping[str, str]], plan_rows: Sequence[Mapping[str, str]]
) -> List[str]:
    """Bind manifest READ_COMPLETE state to terminal extraction."""

    errors: List[str] = []
    plan_by_id = {row.get("paper_id", ""): row for row in plan_rows}
    for row in manifest_rows:
        paper_id = row.get("paper_id", "")
        plan = plan_by_id.get(paper_id)
        if plan is None:
            continue
        terminal = (
            plan.get("reading_status") == "COMPLETE"
            and plan.get("terminal_outcome") in TERMINAL_PAPER_OUTCOMES
        )
        status = row.get("selection_status")
        if status == "READ_COMPLETE" and not terminal:
            errors.append("{0}: premature READ_COMPLETE before terminal extraction".format(paper_id))
        if terminal and status != "READ_COMPLETE":
            errors.append("{0}: terminal extraction requires READ_COMPLETE".format(paper_id))
        if not terminal and status != "DEEP_READ":
            errors.append("{0}: nonterminal selected paper must remain DEEP_READ".format(paper_id))
    return sorted(set(errors))


def validate_later_artifacts_absent(reference_root: Path) -> List[str]:
    """Reject G06 through G09 artifacts while G05 is active."""

    errors: List[str] = []
    forbidden_prefixes = (
        ("evidence/failure-cards", "G06 failure-card"),
        ("evidence/constraint-transfer-cards", "G07 constraint-transfer"),
        ("synthesis/architecture-genomes", "G08 architecture-genome"),
        ("synthesis/architecture-candidates", "G08 architecture-candidate"),
        ("synthesis/experiments", "G09 experiment"),
    )
    for relative_prefix, label in forbidden_prefixes:
        path = reference_root / relative_prefix
        if path.exists() and any(candidate.is_file() for candidate in path.rglob("*")):
            errors.append("{0} artifact is forbidden while G05 is active".format(label))
    for relative_path in (
        "evidence/evidence-conflicts.tsv",
        "synthesis/pareto-archive.tsv",
        "synthesis/architecture-decision-atlas.md",
        "synthesis/experiment-backlog.md",
    ):
        if (reference_root / relative_path).exists():
            errors.append("later-goal artifact is forbidden while G05 is active: " + relative_path)
    return sorted(set(errors))
