#!/usr/bin/env python3
"""Deterministic validation helpers for G07 constraint transfers."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter
from pathlib import Path
from typing import Dict, List, Mapping, Sequence, Set


TRANSFER_ID_PATTERN = re.compile(r"^XFER-(?:[A-Z0-9]+-){3}[A-Z0-9]+$")
CHECKSUM_PATTERN = re.compile(r"^[A-F0-9]{64}$")
PLAN_HEADER = (
    "selection_rank",
    "lane_id",
    "lane_position",
    "pattern_id",
    "source_paper_ids",
    "source_domain",
    "selection_score",
    "selection_basis",
    "reader_agent_id",
    "reviewer_agent_id",
    "inspection_status",
    "terminal_disposition",
    "transfer_ids",
    "evidence_gap",
    "measurement_needed",
    "result_checksum",
)
ALLOWED_DISPOSITIONS = {
    "TRANSFER_CREATED",
    "NO_SURVIVING_INVARIANT",
    "MODERN_COST_REVERSAL_INVALIDATES",
    "INSUFFICIENT_EVIDENCE",
    "DUPLICATE_TRANSFER_MERGED",
}
ALLOWED_INSPECTION_STATUSES = {
    "FROZEN",
    "READ_COMPLETE",
    "NORMALIZED",
    "CHALLENGED",
    "COMPLETE",
}
PROFILE_FIELDS = {
    "constrained_resource",
    "access_medium",
    "predictability_requirement",
    "data_mutability",
    "communication_model",
    "original_hardware_operating_assumptions",
}
RESOURCE_FIELDS = {"ram", "io", "preprocessing", "storage", "concurrency"}
CARD_FIELDS = {
    "transfer_id",
    "name",
    "epistemic_label",
    "source_pattern_ids",
    "original_domain",
    "original_constraint_profile",
    "original_constraints",
    "original_cost_model",
    "surviving_invariant",
    "reversed_assumptions",
    "modern_knight_bus_constraints",
    "proposed_transfer",
    "modern_resource_model",
    "unknown_measurement_constants",
    "g06_challenges",
    "analogy_failure_modes",
    "target_algorithm_families",
    "smallest_falsifier",
    "falsifying_experiment_id",
}


def split_pipe_values_sorted(value: str) -> List[str]:
    """Parse one canonical sorted pipe-separated set."""

    values = [item for item in value.split("|") if item]
    return values if values == sorted(set(values)) else []


def parse_json_markdown_envelope(path: Path) -> Dict[str, object]:
    """Parse exactly one JSON fence from a Markdown artifact."""

    text = path.read_text(encoding="utf-8")
    matches = re.findall(r"```json\n(.*?)\n```", text, flags=re.DOTALL)
    if len(matches) != 1:
        raise ValueError("card must contain exactly one fenced JSON object")
    payload = json.loads(matches[0])
    if not isinstance(payload, dict):
        raise ValueError("card JSON payload must be an object")
    return payload


def parse_transfer_card_file(path: Path) -> Dict[str, object]:
    """Parse a canonical transfer card and enforce filename identity."""

    payload = parse_json_markdown_envelope(path)
    if payload.get("transfer_id") != path.stem:
        raise ValueError("transfer_id must match the card filename")
    return payload


def write_transfer_card_markdown(path: Path, card: Mapping[str, object]) -> None:
    """Write one deterministic Markdown plus JSON transfer card."""

    path.parent.mkdir(parents=True, exist_ok=True)
    title = str(card.get("name", "Constraint Transfer"))
    body = "# {0}\n\n```json\n{1}\n```\n".format(
        title, json.dumps(card, indent=2, sort_keys=True, ensure_ascii=True)
    )
    path.write_text(body, encoding="utf-8")


def validate_claim_object_fields(value: object, label: str, allowed_types: Set[str]) -> List[str]:
    """Validate one claim-granular object."""

    errors: List[str] = []
    if not isinstance(value, Mapping):
        return [label + ": must be a claim object"]
    claim_type = value.get("claim_type")
    if claim_type not in allowed_types:
        errors.append(label + ": invalid claim_type")
    if not isinstance(value.get("text"), str) or not str(value.get("text", "")).strip():
        errors.append(label + ": text must be non-empty")
    if not isinstance(value.get("assumptions"), list):
        errors.append(label + ": assumptions must be a list")
    if not isinstance(value.get("uncertainty"), str) or not str(value.get("uncertainty", "")).strip():
        errors.append(label + ": uncertainty must be non-empty")
    if claim_type == "SOURCE_CLAIM":
        if not isinstance(value.get("source_pattern_ids"), list) or not value.get("source_pattern_ids"):
            errors.append(label + ": source claim needs source_pattern_ids")
        if not isinstance(value.get("source_pointer_ids"), list) or not value.get("source_pointer_ids"):
            errors.append(label + ": source claim needs source_pointer_ids")
    elif claim_type in {"DERIVED_INFERENCE", "SPECULATIVE_TRANSFER"}:
        if not isinstance(value.get("premises"), list) or not value.get("premises"):
            errors.append(label + ": derived or transfer claim needs premises")
    return errors


def validate_resource_model_fields(value: object, label: str) -> List[str]:
    """Validate one symbolic modern-resource term."""

    errors: List[str] = []
    if not isinstance(value, Mapping):
        return [label + ": must be a resource model object"]
    if value.get("claim_type") != "DERIVED_INFERENCE":
        errors.append(label + ": claim_type must be DERIVED_INFERENCE")
    expression = str(value.get("expression", "")).strip()
    if (
        not expression
        or expression == "UNKNOWN"
        or not re.search(r"[A-Za-z_]", expression)
        or not re.search(r"[=+*/-]", expression)
        or re.search(r"\b\d+(?:\.\d+)?x\b", expression, flags=re.IGNORECASE)
    ):
        errors.append(label + ": expression must be a symbolic modern cost, not a historical ratio")
    for field in ("variables", "unknown_constants", "assumptions"):
        if not isinstance(value.get(field), list) or not value.get(field):
            errors.append(label + ": " + field + " must be a non-empty list")
    for field in ("measurement_needed", "uncertainty"):
        if not isinstance(value.get(field), str) or not str(value.get(field, "")).strip():
            errors.append(label + ": " + field + " must be non-empty")
    return errors


def validate_transfer_card_record(
    card: Mapping[str, object],
    known_pattern_ids: Set[str],
    known_failure_ids: Set[str],
    pattern_failure_ids: Mapping[str, Set[str]],
) -> List[str]:
    """Validate one canonical G07 transfer record."""

    errors: List[str] = []
    missing = sorted(CARD_FIELDS - set(card))
    if missing:
        errors.append("transfer card: missing fields " + "|".join(missing))
        return errors

    transfer_id = str(card.get("transfer_id", ""))
    if not TRANSFER_ID_PATTERN.fullmatch(transfer_id):
        errors.append("transfer_id: must use exactly four slug words")
    if not str(card.get("name", "")).strip():
        errors.append("name: must be non-empty")
    if card.get("epistemic_label") != "SPECULATIVE_TRANSFER":
        errors.append("epistemic_label: must be SPECULATIVE_TRANSFER")

    source_patterns = card.get("source_pattern_ids")
    if not isinstance(source_patterns, list) or not source_patterns:
        errors.append("source_pattern_ids: must be a non-empty list")
        source_pattern_set: Set[str] = set()
    else:
        source_pattern_set = set(map(str, source_patterns))
        if list(map(str, source_patterns)) != sorted(source_pattern_set):
            errors.append("source_pattern_ids: must be sorted and unique")
        unknown = sorted(source_pattern_set - known_pattern_ids)
        if unknown:
            errors.append("source_pattern_ids: unknown patterns " + "|".join(unknown))

    if not str(card.get("original_domain", "")).strip():
        errors.append("original_domain: must be non-empty")
    profile = card.get("original_constraint_profile")
    if not isinstance(profile, Mapping) or set(profile) != PROFILE_FIELDS:
        errors.append("original_constraint_profile: must contain the six frozen fields")
    else:
        for field in sorted(PROFILE_FIELDS):
            errors.extend(validate_claim_object_fields(profile[field], "original_constraint_profile." + field, {"SOURCE_CLAIM", "DERIVED_INFERENCE"}))

    original_constraints = card.get("original_constraints")
    if not isinstance(original_constraints, list) or not original_constraints:
        errors.append("original_constraints: must be non-empty")
    else:
        for index, item in enumerate(original_constraints):
            errors.extend(validate_claim_object_fields(item, "original_constraints[{0}]".format(index), {"SOURCE_CLAIM", "DERIVED_INFERENCE"}))

    errors.extend(validate_claim_object_fields(card.get("original_cost_model"), "original_cost_model", {"SOURCE_CLAIM", "DERIVED_INFERENCE"}))
    errors.extend(validate_claim_object_fields(card.get("surviving_invariant"), "surviving_invariant", {"SOURCE_CLAIM", "DERIVED_INFERENCE"}))
    for field, allowed in (
        ("reversed_assumptions", {"DERIVED_INFERENCE"}),
        ("modern_knight_bus_constraints", {"SPECULATIVE_TRANSFER"}),
        (
            "analogy_failure_modes",
            {"SOURCE_CLAIM", "DERIVED_INFERENCE", "SPECULATIVE_TRANSFER"},
        ),
    ):
        values = card.get(field)
        if not isinstance(values, list) or not values:
            errors.append(field + ": must be non-empty")
        else:
            for index, item in enumerate(values):
                errors.extend(validate_claim_object_fields(item, "{0}[{1}]".format(field, index), allowed))
    errors.extend(validate_claim_object_fields(card.get("proposed_transfer"), "proposed_transfer", {"SPECULATIVE_TRANSFER"}))

    model = card.get("modern_resource_model")
    if not isinstance(model, Mapping) or set(model) != RESOURCE_FIELDS:
        errors.append("modern_resource_model: must contain RAM, I/O, preprocessing, storage, and concurrency")
    else:
        for field in sorted(RESOURCE_FIELDS):
            errors.extend(validate_resource_model_fields(model[field], "modern_resource_model." + field))

    constants = card.get("unknown_measurement_constants")
    if not isinstance(constants, list) or not constants or list(map(str, constants)) != sorted(set(map(str, constants))):
        errors.append("unknown_measurement_constants: must be a sorted non-empty set")
    algorithms = card.get("target_algorithm_families")
    if not isinstance(algorithms, list) or not algorithms:
        errors.append("target_algorithm_families: must be non-empty")

    challenges = card.get("g06_challenges")
    answered: Set[str] = set()
    if not isinstance(challenges, list):
        errors.append("g06_challenges: must be a list")
    else:
        for index, challenge in enumerate(challenges):
            label = "g06_challenges[{0}]".format(index)
            if not isinstance(challenge, Mapping):
                errors.append(label + ": must be an object")
                continue
            failure_id = str(challenge.get("failure_id", ""))
            if failure_id not in known_failure_ids:
                errors.append(label + ": unknown failure_id")
            else:
                answered.add(failure_id)
            if not isinstance(challenge.get("applies"), bool):
                errors.append(label + ": applies must be boolean")
            if not str(challenge.get("response", "")).strip():
                errors.append(label + ": response must be non-empty")
    expected_failures: Set[str] = set()
    for pattern_id in source_pattern_set:
        expected_failures.update(pattern_failure_ids.get(pattern_id, set()))
    missing_failures = sorted(expected_failures - answered)
    if missing_failures:
        errors.append("G06 challenges are unanswered: " + "|".join(missing_failures))

    falsifier = card.get("smallest_falsifier")
    falsifier_fields = {"fixture", "controlled_variables", "independent_oracle", "failure_signal"}
    if not isinstance(falsifier, Mapping) or set(falsifier) != falsifier_fields:
        errors.append("smallest_falsifier: must contain the four frozen fields")
    else:
        if not isinstance(falsifier.get("controlled_variables"), list) or not falsifier.get("controlled_variables"):
            errors.append("smallest_falsifier.controlled_variables: must be non-empty")
        for field in ("fixture", "independent_oracle", "failure_signal"):
            if not str(falsifier.get(field, "")).strip():
                errors.append("smallest_falsifier." + field + ": must be non-empty")
    expected_falsifier = "RESERVED-G09-FOR-" + transfer_id
    if card.get("falsifying_experiment_id") != expected_falsifier:
        errors.append("falsifying_experiment_id: must match the reserved G09 lifecycle")
    return errors


def validate_transfer_card_collection(
    cards: Sequence[Mapping[str, object]],
    known_pattern_ids: Set[str],
    known_failure_ids: Set[str],
    pattern_failure_ids: Mapping[str, Set[str]],
) -> List[str]:
    """Validate IDs and records across all canonical cards."""

    errors: List[str] = []
    ids = [str(card.get("transfer_id", "")) for card in cards]
    for transfer_id, count in Counter(ids).items():
        if count != 1:
            errors.append("duplicate transfer_id " + transfer_id)
    if len(cards) > 20:
        errors.append("transfer card count exceeds the G07 maximum of 20")
    for card in cards:
        prefix = str(card.get("transfer_id", "")) + ": "
        errors.extend(prefix + error for error in validate_transfer_card_record(card, known_pattern_ids, known_failure_ids, pattern_failure_ids))
    return errors


def validate_transfer_plan_rows(
    rows: Sequence[Mapping[str, str]],
    known_pattern_ids: Set[str],
    require_complete: bool,
) -> List[str]:
    """Validate the frozen four-by-five plan and terminal lifecycle."""

    errors: List[str] = []
    if len(rows) != 20:
        errors.append("g07 plan must contain exactly 20 mechanisms")
    pattern_ids = [row.get("pattern_id", "") for row in rows]
    if len(set(pattern_ids)) != len(pattern_ids):
        errors.append("g07 plan pattern IDs must be unique")
    unknown = sorted(set(pattern_ids) - known_pattern_ids)
    if unknown:
        errors.append("g07 plan contains unknown patterns " + "|".join(unknown))
    lanes = Counter(row.get("lane_id", "") for row in rows)
    if lanes != Counter({"G07-LANE-1": 5, "G07-LANE-2": 5, "G07-LANE-3": 5, "G07-LANE-4": 5}):
        errors.append("g07 plan must contain four disjoint lanes of five")
    domains = {row.get("source_domain", "").strip() for row in rows if row.get("source_domain", "").strip()}
    if len(domains) < 3:
        errors.append("g07 plan must cover at least three source domains")
    if domains and all("graph" in domain.lower() for domain in domains):
        errors.append("g07 plan must include a non-graph source domain")
    for index, row in enumerate(rows, start=1):
        label = "g07 plan row {0}".format(index)
        if set(row) != set(PLAN_HEADER):
            errors.append(label + ": fields do not match the frozen header")
        if row.get("selection_rank") != str(index):
            errors.append(label + ": selection_rank must be contiguous")
        try:
            score = int(row.get("selection_score", ""))
            if score < 1 or score > 100:
                raise ValueError
        except ValueError:
            errors.append(label + ": selection_score must be 1..100")
        if row.get("inspection_status") not in ALLOWED_INSPECTION_STATUSES:
            errors.append(label + ": invalid inspection_status")
        if require_complete:
            if row.get("inspection_status") != "COMPLETE":
                errors.append(label + ": inspection_status must be COMPLETE")
            disposition = row.get("terminal_disposition", "")
            if disposition not in ALLOWED_DISPOSITIONS:
                errors.append(label + ": invalid terminal_disposition")
            transfer_ids = split_pipe_values_sorted(row.get("transfer_ids", ""))
            if disposition in {"TRANSFER_CREATED", "DUPLICATE_TRANSFER_MERGED"} and not transfer_ids:
                errors.append(label + ": transfer disposition needs transfer_ids")
            if disposition not in {"TRANSFER_CREATED", "DUPLICATE_TRANSFER_MERGED"} and row.get("transfer_ids", ""):
                errors.append(label + ": rejection disposition cannot link a transfer")
            if disposition not in {"TRANSFER_CREATED", "DUPLICATE_TRANSFER_MERGED"} and not row.get("evidence_gap", "").strip():
                errors.append(label + ": rejection disposition needs evidence_gap")
            if row.get("reader_agent_id") in {"", "UNASSIGNED"}:
                errors.append(label + ": reader_agent_id must be assigned")
            if row.get("reviewer_agent_id") in {"", "UNASSIGNED", row.get("reader_agent_id")}:
                errors.append(label + ": reviewer must be independently assigned")
            if not CHECKSUM_PATTERN.fullmatch(row.get("result_checksum", "")):
                errors.append(label + ": result_checksum must be uppercase SHA-256")
    return errors


def calculate_transfer_result_checksum(
    row: Mapping[str, str],
    cards_by_id: Mapping[str, Mapping[str, object]],
    mechanism_hashes: Mapping[str, str],
    failure_hashes: Mapping[str, str],
    pattern_failure_ids: Mapping[str, Set[str]],
) -> str:
    """Calculate one stable terminal-plan result checksum."""

    row_payload = {key: row.get(key, "") for key in PLAN_HEADER if key != "result_checksum"}
    transfer_ids = split_pipe_values_sorted(row.get("transfer_ids", ""))
    relevant_failure_ids = pattern_failure_ids.get(row.get("pattern_id", ""), set())
    payload = {
        "row": row_payload,
        "transfers": {transfer_id: cards_by_id.get(transfer_id, {"missing": True}) for transfer_id in transfer_ids},
        "mechanism_hash": mechanism_hashes.get(row.get("pattern_id", ""), "MISSING"),
        "failure_hashes": {
            failure_id: failure_hashes.get(failure_id, "MISSING")
            for failure_id in sorted(relevant_failure_ids)
        },
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest().upper()


def validate_transfer_plan_crosslinks(
    rows: Sequence[Mapping[str, str]],
    cards_by_id: Mapping[str, Mapping[str, object]],
    require_complete: bool,
) -> List[str]:
    """Validate plan-to-card ownership and reject orphan transfers."""

    errors: List[str] = []
    selected_patterns = {row.get("pattern_id", "") for row in rows}
    referenced_transfers: Set[str] = set()
    for index, row in enumerate(rows, start=1):
        transfer_ids = split_pipe_values_sorted(row.get("transfer_ids", ""))
        if row.get("transfer_ids", "") and not transfer_ids:
            errors.append("g07 plan row {0}: transfer_ids must be sorted and unique".format(index))
            continue
        for transfer_id in transfer_ids:
            card = cards_by_id.get(transfer_id)
            if card is None:
                errors.append("g07 plan row {0}: unknown transfer_id {1}".format(index, transfer_id))
                continue
            referenced_transfers.add(transfer_id)
            if row.get("pattern_id", "") not in set(map(str, card.get("source_pattern_ids", []))):
                errors.append("g07 plan row {0}: transfer does not name its source pattern".format(index))
    for transfer_id, card in cards_by_id.items():
        unknown_patterns = sorted(set(map(str, card.get("source_pattern_ids", []))) - selected_patterns)
        if unknown_patterns:
            errors.append(transfer_id + ": source patterns are outside the frozen G07 set " + "|".join(unknown_patterns))
    if require_complete:
        orphaned = sorted(set(cards_by_id) - referenced_transfers)
        if orphaned:
            errors.append("orphan transfer cards are not referenced by the terminal plan: " + "|".join(orphaned))
    return errors


def validate_transfer_result_checksums(
    rows: Sequence[Mapping[str, str]],
    cards_by_id: Mapping[str, Mapping[str, object]],
    mechanism_hashes: Mapping[str, str],
    failure_hashes: Mapping[str, str],
    pattern_failure_ids: Mapping[str, Set[str]],
) -> List[str]:
    """Recompute every terminal plan-row checksum."""

    errors: List[str] = []
    for index, row in enumerate(rows, start=1):
        expected = calculate_transfer_result_checksum(
            row,
            cards_by_id,
            mechanism_hashes,
            failure_hashes,
            pattern_failure_ids,
        )
        if row.get("result_checksum", "") != expected:
            errors.append("g07 plan row {0}: result_checksum mismatch".format(index))
    return errors


def validate_lane_dossier_record(
    dossier: Mapping[str, object],
    expected_lane_id: str,
    expected_pattern_ids: Set[str],
    known_pattern_ids: Set[str],
    known_failure_ids: Set[str],
    pattern_failure_ids: Mapping[str, Set[str]],
) -> List[str]:
    """Validate one read-only five-mechanism lane dossier."""

    errors: List[str] = []
    if dossier.get("lane_id") != expected_lane_id:
        errors.append("lane dossier: lane_id does not match assignment")
    assigned = dossier.get("assigned_pattern_ids")
    if not isinstance(assigned, list) or set(map(str, assigned)) != expected_pattern_ids or len(assigned) != len(expected_pattern_ids):
        errors.append("lane dossier: assigned_pattern_ids do not match the frozen lane")
    evaluations = dossier.get("evaluations")
    if not isinstance(evaluations, list):
        return errors + ["lane dossier: evaluations must be a list"]
    evaluation_ids = [str(item.get("pattern_id", "")) for item in evaluations if isinstance(item, Mapping)]
    if set(evaluation_ids) != expected_pattern_ids or len(evaluation_ids) != len(expected_pattern_ids):
        errors.append("lane dossier: evaluations must cover each assigned pattern exactly once")
    for index, evaluation in enumerate(evaluations):
        label = "lane dossier evaluation {0}".format(index + 1)
        if not isinstance(evaluation, Mapping):
            errors.append(label + ": must be an object")
            continue
        pattern_id = str(evaluation.get("pattern_id", ""))
        linked = evaluation.get("linked_failure_ids")
        if not isinstance(linked, list) or list(map(str, linked)) != sorted(pattern_failure_ids.get(pattern_id, set())):
            errors.append(label + ": linked_failure_ids do not equal the complete G06 set")
        disposition = evaluation.get("recommended_disposition")
        if disposition not in ALLOWED_DISPOSITIONS:
            errors.append(label + ": invalid recommended_disposition")
        if not str(evaluation.get("rationale", "")).strip():
            errors.append(label + ": rationale must be non-empty")
        transfer_card = evaluation.get("transfer_card")
        if disposition in {"TRANSFER_CREATED", "DUPLICATE_TRANSFER_MERGED"}:
            if not isinstance(transfer_card, Mapping):
                errors.append(label + ": transfer disposition needs a complete transfer_card")
            else:
                errors.extend(
                    label + ": " + error
                    for error in validate_transfer_card_record(
                        transfer_card,
                        known_pattern_ids,
                        known_failure_ids,
                        pattern_failure_ids,
                    )
                )
        elif transfer_card is not None:
            errors.append(label + ": rejection disposition must set transfer_card to null")
    return errors


def hash_file_content_exact(path: Path) -> str:
    """Hash one canonical input file as uppercase SHA-256."""

    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def validate_constraint_transfer_report(
    report_text: str,
    rows: Sequence[Mapping[str, str]],
    cards_by_id: Mapping[str, Mapping[str, object]],
    require_complete: bool,
) -> List[str]:
    """Validate G07's decision report coverage and eight required answers."""

    errors: List[str] = []
    required_headings = (
        "# G07 Constraint Transfer Report",
        "## Executive Decision",
        "## Corpus And Method",
        "## Transfer Dispositions",
        "## 1. Historical Constraints Still Relevant",
        "## 2. Invariants That Survive",
        "## 3. Assumptions Reversed By Modern Hardware",
        "## 4. Transfers For RAM And Predictability",
        "## 5. Attractive Transfers Killed By G06",
        "## 6. Constants G09 Must Measure",
        "## 7. Architecture Vocabulary For G08",
        "## 8. Explicit Non-Transfers",
        "## G08 Recommendation",
    )
    for heading in required_headings:
        if report_text.count(heading) != 1:
            errors.append("G07 report must contain heading exactly once: " + heading)
    count_markers = (
        "- Frozen mechanisms: {0}".format(len(rows)),
        "- Terminal dispositions: {0}".format(
            sum(bool(row.get("terminal_disposition", "")) for row in rows)
        ),
        "- Canonical transfer cards: {0}".format(len(cards_by_id)),
        "- External requests: 0",
    )
    for marker in count_markers:
        if marker not in report_text:
            errors.append("G07 report missing count marker " + marker)
    for row in rows:
        pattern_id = row.get("pattern_id", "")
        if pattern_id and pattern_id not in report_text:
            errors.append("G07 report omits selected pattern " + pattern_id)
    for transfer_id in cards_by_id:
        if transfer_id not in report_text:
            errors.append("G07 report omits canonical transfer " + transfer_id)
    if require_complete and "Recommendation: `PROCEED_TO_G08`" not in report_text:
        errors.append("G07 report must recommend PROCEED_TO_G08 at cleared completion")
    return errors


def read_transfer_plan_rows(path: Path) -> List[Dict[str, str]]:
    """Read the canonical G07 TSV plan."""

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if tuple(reader.fieldnames or ()) != PLAN_HEADER:
            raise ValueError("g07 plan header does not match the frozen schema")
        return [dict(row) for row in reader]
