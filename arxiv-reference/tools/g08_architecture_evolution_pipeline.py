#!/usr/bin/env python3
"""Deterministic integrity checks for the G08 Architecture Evolution Arena."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, List, Mapping, Sequence, Set


CANDIDATE_ID_PATTERN = re.compile(r"^ARCH-G08-\d{3}$")
CHECKSUM_PATTERN = re.compile(r"^[A-F0-9]{64}$")
ALLOWED_NICHES = {
    "LOWEST_RAM",
    "LOWEST_TAIL_LATENCY",
    "HIGHEST_PREDICTABILITY",
    "LOWEST_PREPARATION_STORAGE",
    "LOWEST_ADOPTION_FRICTION",
    "UNCONVENTIONAL_COMPOSABLE",
    "BASELINE",
}
NICHE_BUDGET = {
    "LOWEST_RAM": 8,
    "LOWEST_TAIL_LATENCY": 8,
    "HIGHEST_PREDICTABILITY": 8,
    "LOWEST_PREPARATION_STORAGE": 8,
    "LOWEST_ADOPTION_FRICTION": 8,
    "UNCONVENTIONAL_COMPOSABLE": 8,
    "BASELINE": 2,
}
ALLOWED_DISPOSITIONS = {
    "PARETO_SURVIVOR",
    "SPECIALIZED_SURVIVOR",
    "REPAIR_REQUIRED",
    "DEFER_TO_CALIBRATION",
    "REJECTED_BY_COUNTEREXAMPLE",
    "REJECTED_BY_COMPOSITION",
    "DUPLICATE_MERGED",
}
STAGE_ORDER = {
    "RAW_GENERATED": 0,
    "SCHEMA_COMPLETE": 1,
    "LINEAGE_REVIEW": 2,
    "SYMBOLIC_RESOURCE_REVIEW": 3,
    "INVARIANT_COMPOSITION_REVIEW": 4,
    "G06_COUNTEREXAMPLE_REVIEW": 5,
    "A007_ENFORCEABILITY_REVIEW": 6,
    "PREPARATION_STORAGE_REVIEW": 7,
    "QUALITATIVE_PARETO_PLACEMENT": 8,
    "INDEPENDENT_ADVERSARIAL_REVIEW": 9,
}
PRIORITY_WORKLOADS = {
    "DEPENDENCY_SECURITY_ACCESS_PATH",
    "BFS_REACHABILITY_SHORTEST_UNWEIGHTED",
    "PAGERANK_ITERATIVE_SPARSE_LINEAR_ALGEBRA",
    "WCC_CONNECTIVITY",
    "LOUVAIN_LEIDEN_COMMUNITY",
    "NODE_SIMILARITY_VECTOR_KNN",
    "MULTI_ALGORITHM_SHARED_ARTIFACT",
}
RAM_TERMS = {
    "topology",
    "algorithm_state",
    "frontier_active_set",
    "scratch",
    "output",
    "conversion",
    "page_cache_or_direct_io",
    "runtime_overhead",
    "spill",
    "safety_margin",
    "temporary_artifact_coexistence",
}
RESOURCE_TERMS = {"io", "preprocessing", "storage", "recomputation", "concurrency"}
PARETO_AXES = {
    "peak_ram",
    "tail_latency_risk",
    "predictability",
    "preprocessing_cost",
    "persistent_storage_amplification",
    "temporary_storage_peak",
    "exactness_determinism",
    "operational_complexity",
    "neo4j_adoption_friction",
    "calibration_debt",
}
CARD_FIELDS = {
    "candidate_id",
    "name",
    "epistemic_label",
    "primary_niche",
    "secondary_niches",
    "parents",
    "generation_lineage",
    "inherited_transfer_ids",
    "target_workload_contract",
    "genome",
    "minimum_resident_kernel",
    "resource_model",
    "state_multiplicity",
    "page_cache_direct_io_policy",
    "preparation_model",
    "fallback_ladder",
    "correctness_and_determinism",
    "compatibility_boundary",
    "crossover_guards",
    "composition_review",
    "receipt_fields",
    "estimator_feedback",
    "linked_g06_failure_ids",
    "g06_challenge_responses",
    "adversarial_review",
    "loses_when",
    "smallest_g09_falsifier",
    "qualitative_pareto",
    "highest_completed_stage",
    "terminal_disposition",
    "disposition_reason",
}
PLAN_HEADER = (
    "candidate_rank",
    "candidate_id",
    "lane_id",
    "lane_position",
    "primary_niche",
    "generator_identity",
    "reviewer_identity",
    "highest_completed_stage",
    "terminal_disposition",
    "inherited_transfer_ids",
    "linked_g06_failure_ids",
    "result_checksum",
)
PARETO_HEADER = (
    "pareto_order",
    "candidate_id",
    "primary_niche",
    "survival_class",
    "robust_without_calibration",
    "requires_g09",
    "narrow_shape_only",
    "neo4j_surface_fit",
    "dependency_wedge_fit",
    "symbolic_dominance",
    "never_combine_with",
)


def parse_json_markdown_envelope(path: Path) -> Dict[str, object]:
    """Parse exactly one fenced JSON record from Markdown."""

    text = path.read_text(encoding="utf-8")
    matches = re.findall(r"```json\n(.*?)\n```", text, flags=re.DOTALL)
    if len(matches) != 1:
        raise ValueError("artifact must contain exactly one fenced JSON object")
    payload = json.loads(matches[0])
    if not isinstance(payload, dict):
        raise ValueError("artifact JSON must be an object")
    return payload


def parse_candidate_card_file(path: Path) -> Dict[str, object]:
    """Parse one candidate card and bind its ID to its filename."""

    payload = parse_json_markdown_envelope(path)
    if payload.get("candidate_id") != path.stem:
        raise ValueError("candidate_id must match candidate filename")
    return payload


def write_candidate_card_markdown(path: Path, card: Mapping[str, object]) -> None:
    """Write a deterministic Markdown plus JSON candidate card."""

    path.parent.mkdir(parents=True, exist_ok=True)
    body = "# {0}\n\n```json\n{1}\n```\n".format(
        str(card.get("name", "Architecture Candidate")),
        json.dumps(card, indent=2, sort_keys=True, ensure_ascii=True),
    )
    path.write_text(body, encoding="utf-8")


def load_tsv(path: Path) -> List[Dict[str, str]]:
    """Load a tab-separated ledger."""

    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def nonempty_string(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def sorted_unique_strings(value: object, allow_empty: bool = False) -> bool:
    if not isinstance(value, list):
        return False
    values = list(map(str, value))
    return (allow_empty or bool(values)) and values == sorted(set(values))


def validate_symbolic_term(value: object, label: str) -> List[str]:
    """Reject hidden zeros, historical ratios, and uncalibrated constants."""

    errors: List[str] = []
    if not isinstance(value, Mapping):
        return [label + ": must be a symbolic-resource object"]
    expression = str(value.get("expression", "")).strip()
    if (
        not expression
        or expression == "UNKNOWN"
        or not re.search(r"[A-Za-z_]", expression)
        or not re.search(r"[=+*/-]", expression)
        or re.search(r"\b\d+(?:\.\d+)?x\b", expression, flags=re.IGNORECASE)
    ):
        errors.append(label + ": expression must be symbolic and unit-interpretable")
    for field in ("variables", "unknown_constants", "assumptions", "double_counting_risks"):
        if not isinstance(value.get(field), list) or not value.get(field):
            errors.append(label + ": " + field + " must be a non-empty list")
    for field in ("measurement_needed", "uncertainty"):
        if not nonempty_string(value.get(field)):
            errors.append(label + ": " + field + " must be non-empty")
    return errors


def validate_required_object(
    value: object,
    label: str,
    required: Set[str],
    list_fields: Set[str] = set(),
) -> List[str]:
    errors: List[str] = []
    if not isinstance(value, Mapping):
        return [label + ": must be an object"]
    missing = sorted(required - set(value))
    if missing:
        errors.append(label + ": missing fields " + "|".join(missing))
    for field in sorted(required & set(value)):
        if field in list_fields:
            if not isinstance(value.get(field), list) or not value.get(field):
                errors.append(label + "." + field + ": must be a non-empty list")
        elif not nonempty_string(value.get(field)) and not isinstance(value.get(field), bool):
            errors.append(label + "." + field + ": must be non-empty")
    return errors


def validate_candidate_record(
    card: Mapping[str, object],
    known_transfer_ids: Set[str],
    known_failure_ids: Set[str],
    closure: bool,
) -> List[str]:
    """Validate one G08 architecture candidate."""

    errors: List[str] = []
    candidate_id = str(card.get("candidate_id", ""))
    missing = sorted(CARD_FIELDS - set(card))
    if missing:
        return [candidate_id + ": missing fields " + "|".join(missing)]
    if not CANDIDATE_ID_PATTERN.fullmatch(candidate_id):
        errors.append("candidate_id: expected ARCH-G08-NNN")
    if not nonempty_string(card.get("name")):
        errors.append("name: must be non-empty")
    if card.get("epistemic_label") != "SPECULATIVE_TRANSFER":
        errors.append("epistemic_label: must remain SPECULATIVE_TRANSFER")
    niche = str(card.get("primary_niche", ""))
    if niche not in ALLOWED_NICHES:
        errors.append("primary_niche: invalid niche")
    secondary = card.get("secondary_niches")
    if not isinstance(secondary, list) or list(map(str, secondary)) != sorted(set(map(str, secondary))):
        errors.append("secondary_niches: must be a sorted unique list")
    elif any(str(item) not in ALLOWED_NICHES - {"BASELINE"} for item in secondary):
        errors.append("secondary_niches: contains invalid niche")

    parents = card.get("parents")
    if not isinstance(parents, list) or list(map(str, parents)) != sorted(set(map(str, parents))):
        errors.append("parents: must be a sorted unique list")
    elif candidate_id in set(map(str, parents)) or any(not CANDIDATE_ID_PATTERN.fullmatch(str(item)) for item in parents):
        errors.append("parents: must contain valid non-self candidate IDs")

    lineage_required = {
        "lane_id",
        "lane_position",
        "generator_identity",
        "prompt_id",
        "generated_at",
        "added_transfer_ids",
        "removed_transfer_ids",
        "intended_changed_behavior",
    }
    lineage = card.get("generation_lineage")
    if not isinstance(lineage, Mapping) or not lineage_required.issubset(lineage):
        errors.append("generation_lineage: incomplete traceable variation record")
    else:
        if not isinstance(lineage.get("lane_position"), int) or int(lineage.get("lane_position", 0)) < 1:
            errors.append("generation_lineage.lane_position: must be a positive integer")
        for field in ("lane_id", "generator_identity", "prompt_id", "generated_at", "intended_changed_behavior"):
            if not nonempty_string(lineage.get(field)):
                errors.append("generation_lineage." + field + ": must be non-empty")
        for field in ("added_transfer_ids", "removed_transfer_ids"):
            if not sorted_unique_strings(lineage.get(field), allow_empty=True):
                errors.append("generation_lineage." + field + ": must be sorted and unique")

    transfers = card.get("inherited_transfer_ids")
    if not sorted_unique_strings(transfers, allow_empty=niche == "BASELINE"):
        errors.append("inherited_transfer_ids: must be sorted, unique, and non-empty outside baselines")
        transfer_set: Set[str] = set()
    else:
        transfer_set = set(map(str, transfers))
        unknown = sorted(transfer_set - known_transfer_ids)
        if unknown:
            errors.append("inherited_transfer_ids: unknown " + "|".join(unknown))

    errors.extend(
        validate_required_object(
            card.get("target_workload_contract"),
            "target_workload_contract",
            {
                "artifact",
                "algorithm_family",
                "algorithm_semantics",
                "exactness_requirement",
                "ram_ceiling",
                "storage_allowance",
                "deadline_model",
                "output_bound",
                "a007_decision_strengthened",
                "first_wedge",
            },
        )
    )
    workload = card.get("target_workload_contract")
    if isinstance(workload, Mapping) and workload.get("algorithm_family") not in PRIORITY_WORKLOADS:
        errors.append("target_workload_contract.algorithm_family: unsupported priority family")

    errors.extend(
        validate_required_object(
            card.get("genome"),
            "genome",
            {
                "topology_layout",
                "prepared_artifact_variants",
                "ordering_and_identifier_representation",
                "algorithm_state_placement",
                "scheduling_and_concurrency",
                "overflow_behavior",
                "exactness",
                "admission_model",
                "receipt_model",
                "compatibility_boundary",
            },
            {"prepared_artifact_variants"},
        )
    )
    errors.extend(
        validate_required_object(
            card.get("minimum_resident_kernel"),
            "minimum_resident_kernel",
            {"admission_unit", "expression", "refusal_condition"},
        )
    )

    resources = card.get("resource_model")
    if not isinstance(resources, Mapping):
        errors.append("resource_model: must be an object")
    else:
        equation = str(resources.get("peak_ram_equation", ""))
        if not equation or not all("RAM_" + term in equation for term in RAM_TERMS):
            errors.append("resource_model.peak_ram_equation: must include every required RAM term")
        ram_terms = resources.get("ram_terms")
        if not isinstance(ram_terms, Mapping):
            errors.append("resource_model.ram_terms: must be an object")
        else:
            for term in sorted(RAM_TERMS):
                if term not in ram_terms:
                    errors.append("resource_model.ram_terms." + term + ": missing")
                else:
                    errors.extend(validate_symbolic_term(ram_terms[term], "resource_model.ram_terms." + term))
        for term in sorted(RESOURCE_TERMS):
            if term not in resources:
                errors.append("resource_model." + term + ": missing")
            else:
                errors.extend(validate_symbolic_term(resources[term], "resource_model." + term))

    errors.extend(
        validate_required_object(
            card.get("state_multiplicity"),
            "state_multiplicity",
            {"workers", "queries", "partitions", "stages", "io_depth"},
        )
    )
    errors.extend(
        validate_required_object(
            card.get("page_cache_direct_io_policy"),
            "page_cache_direct_io_policy",
            {"mode", "equation", "guard"},
        )
    )
    errors.extend(
        validate_required_object(
            card.get("preparation_model"),
            "preparation_model",
            {
                "artifact_build_phases",
                "build_peak_ram",
                "build_io",
                "persistent_bytes",
                "temporary_bytes",
                "freshness_model",
                "amortization_assumptions",
                "shared_layout_baseline_comparison",
            },
            {"artifact_build_phases", "amortization_assumptions"},
        )
    )
    ladder = card.get("fallback_ladder")
    if not isinstance(ladder, list) or not ladder or ladder[-1] != "REFUSE":
        errors.append("fallback_ladder: must be non-empty and terminate in REFUSE")
    errors.extend(
        validate_required_object(
            card.get("correctness_and_determinism"),
            "correctness_and_determinism",
            {
                "exactness",
                "numerical_tolerance",
                "seed_policy",
                "ordering_guarantee",
                "nondeterministic_sources",
                "oracle_strategy",
                "refusal_rule",
            },
            {"nondeterministic_sources"},
        )
    )
    errors.extend(
        validate_required_object(
            card.get("compatibility_boundary"),
            "compatibility_boundary",
            {"neo4j", "cypher", "bolt", "gds", "unsupported_behavior"},
        )
    )

    guards = card.get("crossover_guards")
    if not isinstance(guards, list) or not guards:
        errors.append("crossover_guards: must be non-empty")
    else:
        for index, guard in enumerate(guards):
            errors.extend(
                validate_required_object(
                    guard,
                    "crossover_guards[{0}]".format(index),
                    {"guard", "unknown_constants", "switch_action"},
                    {"unknown_constants"},
                )
            )
    errors.extend(
        validate_required_object(
            card.get("composition_review"),
            "composition_review",
            {
                "invariant_compatibility",
                "memory_term_overlap",
                "prepared_artifact_coexistence",
                "access_pattern_conflicts",
                "fallback_composition",
                "exactness_determinism_composition",
                "preparation_rationality",
            },
        )
    )
    if not sorted_unique_strings(card.get("receipt_fields")):
        errors.append("receipt_fields: must be a sorted non-empty set")
    errors.extend(
        validate_required_object(
            card.get("estimator_feedback"),
            "estimator_feedback",
            {"error_equation", "calibration_action", "safety_action"},
        )
    )

    failure_ids = card.get("linked_g06_failure_ids")
    if not sorted_unique_strings(failure_ids, allow_empty=False):
        errors.append("linked_g06_failure_ids: must be a sorted non-empty set after challenge")
        failure_set: Set[str] = set()
    else:
        failure_set = set(map(str, failure_ids))
        unknown_failures = sorted(failure_set - known_failure_ids)
        if unknown_failures:
            errors.append("linked_g06_failure_ids: unknown " + "|".join(unknown_failures))
    responses = card.get("g06_challenge_responses")
    answered: Set[str] = set()
    if not isinstance(responses, list) or not responses:
        errors.append("g06_challenge_responses: must be non-empty after challenge")
    else:
        for index, response in enumerate(responses):
            if not isinstance(response, Mapping):
                errors.append("g06_challenge_responses[{0}]: must be an object".format(index))
                continue
            failure_id = str(response.get("failure_id", ""))
            answered.add(failure_id)
            if failure_id not in known_failure_ids:
                errors.append("g06_challenge_responses[{0}]: unknown failure".format(index))
            if not isinstance(response.get("applies"), bool) or not nonempty_string(response.get("response")):
                errors.append("g06_challenge_responses[{0}]: needs applies and response".format(index))
    if failure_set != answered:
        errors.append("G06 challenge responses must cover exactly linked_g06_failure_ids")

    adversarial = card.get("adversarial_review")
    errors.extend(
        validate_required_object(
            adversarial,
            "adversarial_review",
            {"loaded_after_raw_portfolio_freeze", "raw_portfolio_freeze_id", "reviewer_identity", "challenged_at", "failure_selection_basis"},
        )
    )
    if isinstance(adversarial, Mapping) and adversarial.get("loaded_after_raw_portfolio_freeze") is not True:
        errors.append("adversarial_review: failures must load after raw portfolio freeze")

    if not isinstance(card.get("loses_when"), list) or not card.get("loses_when"):
        errors.append("loses_when: must be non-empty")
    errors.extend(
        validate_required_object(
            card.get("smallest_g09_falsifier"),
            "smallest_g09_falsifier",
            {"experiment_id", "fixture", "baseline", "oracle", "metrics", "modeled_expectation", "acceptance_thresholds", "disconfirming_result"},
            {"metrics"},
        )
    )
    falsifier = card.get("smallest_g09_falsifier")
    if isinstance(falsifier, Mapping) and falsifier.get("experiment_id") != "RESERVED-G09-FOR-" + candidate_id:
        errors.append("smallest_g09_falsifier.experiment_id: must match candidate lifecycle")

    pareto = card.get("qualitative_pareto")
    if not isinstance(pareto, Mapping) or set(pareto) != PARETO_AXES:
        errors.append("qualitative_pareto: must contain all ten explicit axes")
    elif any(not nonempty_string(pareto.get(axis)) for axis in PARETO_AXES):
        errors.append("qualitative_pareto: every axis must be non-empty")

    stage = str(card.get("highest_completed_stage", ""))
    if stage not in STAGE_ORDER:
        errors.append("highest_completed_stage: invalid stage")
    disposition = str(card.get("terminal_disposition", ""))
    if closure and disposition not in ALLOWED_DISPOSITIONS:
        errors.append("terminal_disposition: missing or invalid at closure")
    if closure and stage != "INDEPENDENT_ADVERSARIAL_REVIEW":
        errors.append("highest_completed_stage: closure requires independent adversarial review")
    if closure and not nonempty_string(card.get("disposition_reason")):
        errors.append("disposition_reason: required at closure")

    serialized = json.dumps(card, sort_keys=True)
    if re.search(r"\b\d+(?:\.\d+)?x\s+(?:faster|lower|less)\b", serialized, flags=re.IGNORECASE):
        errors.append("unsupported numeric performance delta is prohibited in G08")
    return errors


def validate_candidate_collection(
    cards: Sequence[Mapping[str, object]],
    known_transfer_ids: Set[str],
    known_failure_ids: Set[str],
    closure: bool,
) -> List[str]:
    """Validate all 50 candidates and portfolio-level diversity."""

    errors: List[str] = []
    ids = [str(card.get("candidate_id", "")) for card in cards]
    for candidate_id, count in Counter(ids).items():
        if count != 1:
            errors.append("duplicate candidate_id " + candidate_id)
    if closure and len(cards) != 50:
        errors.append("candidate count must be exactly 50 at closure")
    if closure:
        niches = Counter(str(card.get("primary_niche", "")) for card in cards)
        if niches != Counter(NICHE_BUDGET):
            errors.append("primary niche counts must equal the frozen 8x6 plus 2 baseline budget")
        workloads = {
            str(card.get("target_workload_contract", {}).get("algorithm_family", ""))
            for card in cards
            if isinstance(card.get("target_workload_contract"), Mapping)
        }
        missing_workloads = sorted(PRIORITY_WORKLOADS - workloads)
        if missing_workloads:
            errors.append("priority workload coverage missing " + "|".join(missing_workloads))
        first_wedge = sum(
            1
            for card in cards
            if isinstance(card.get("target_workload_contract"), Mapping)
            and card["target_workload_contract"].get("first_wedge") is True
            and card["target_workload_contract"].get("algorithm_family") == "DEPENDENCY_SECURITY_ACCESS_PATH"
        )
        if first_wedge < 1:
            errors.append("dependency/security/access-path first wedge is absent")
    for card in cards:
        prefix = str(card.get("candidate_id", "")) + ": "
        errors.extend(prefix + error for error in validate_candidate_record(card, known_transfer_ids, known_failure_ids, closure))
    return errors


def validate_candidate_plan_rows(rows: Sequence[Mapping[str, str]], closure: bool) -> List[str]:
    """Validate the exact 50-row candidate accounting ledger."""

    errors: List[str] = []
    if len(rows) != 50:
        errors.append("candidate plan must contain exactly 50 rows")
    ids = [row.get("candidate_id", "") for row in rows]
    if len(ids) != len(set(ids)):
        errors.append("candidate plan IDs must be unique")
    expected_ranks = [str(index) for index in range(1, 51)]
    if [row.get("candidate_rank", "") for row in rows] != expected_ranks:
        errors.append("candidate plan ranks must be contiguous 1..50")
    for index, row in enumerate(rows, 1):
        if tuple(row.keys()) != PLAN_HEADER:
            errors.append("candidate plan row {0}: invalid header".format(index))
            continue
        if not CANDIDATE_ID_PATTERN.fullmatch(row.get("candidate_id", "")):
            errors.append("candidate plan row {0}: invalid candidate ID".format(index))
        if row.get("primary_niche") not in ALLOWED_NICHES:
            errors.append("candidate plan row {0}: invalid niche".format(index))
        if closure:
            if row.get("terminal_disposition") not in ALLOWED_DISPOSITIONS:
                errors.append("candidate plan row {0}: invalid disposition".format(index))
            if row.get("highest_completed_stage") != "INDEPENDENT_ADVERSARIAL_REVIEW":
                errors.append("candidate plan row {0}: incomplete review stage".format(index))
            if not row.get("generator_identity") or not row.get("reviewer_identity"):
                errors.append("candidate plan row {0}: missing generator or reviewer".format(index))
            if not CHECKSUM_PATTERN.fullmatch(row.get("result_checksum", "")):
                errors.append("candidate plan row {0}: missing checksum".format(index))
    if closure and Counter(row.get("primary_niche", "") for row in rows) != Counter(NICHE_BUDGET):
        errors.append("candidate plan niche budget is incorrect")
    return errors


def validate_pareto_rows(rows: Sequence[Mapping[str, str]], candidate_ids: Set[str]) -> List[str]:
    """Validate the bounded non-scalar Pareto archive."""

    errors: List[str] = []
    if not 12 <= len(rows) <= 18:
        errors.append("Pareto archive must retain 12-18 candidates")
    ids = [row.get("candidate_id", "") for row in rows]
    if len(ids) != len(set(ids)):
        errors.append("Pareto archive candidate IDs must be unique")
    if set(ids) - candidate_ids:
        errors.append("Pareto archive references unknown candidates")
    for index, row in enumerate(rows, 1):
        if tuple(row.keys()) != PARETO_HEADER:
            errors.append("Pareto row {0}: invalid header".format(index))
            continue
        if row.get("pareto_order") != str(index):
            errors.append("Pareto row {0}: non-contiguous order".format(index))
        if row.get("survival_class") not in {"PARETO_SURVIVOR", "SPECIALIZED_SURVIVOR"}:
            errors.append("Pareto row {0}: invalid survival class".format(index))
        if row.get("robust_without_calibration") not in {"YES", "NO", "PARTIAL"}:
            errors.append("Pareto row {0}: invalid calibration robustness".format(index))
        if row.get("requires_g09") not in {"YES", "NO"}:
            errors.append("Pareto row {0}: invalid G09 flag".format(index))
        if row.get("narrow_shape_only") not in {"YES", "NO"}:
            errors.append("Pareto row {0}: invalid narrow-shape flag".format(index))
        if row.get("symbolic_dominance") not in {"NON_COMPARABLE", "NICHE_NON_DOMINATED", "SPECIALIZED_NON_DOMINATED"}:
            errors.append("Pareto row {0}: invalid symbolic dominance".format(index))
        for field in PARETO_HEADER:
            if not row.get(field):
                errors.append("Pareto row {0}: blank {1}".format(index, field))
    return errors


def canonical_candidate_checksum(row: Mapping[str, str], card: Mapping[str, object]) -> str:
    """Hash one normalized plan row plus canonical candidate JSON."""

    row_payload = {key: row.get(key, "") for key in PLAN_HEADER if key != "result_checksum"}
    payload = json.dumps(
        {"plan_row": row_payload, "candidate": card},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest().upper()


def known_card_ids(directory: Path, field: str) -> Set[str]:
    """Load IDs from one Markdown-plus-JSON card directory."""

    ids: Set[str] = set()
    for path in sorted(directory.glob("*.md")):
        payload = parse_json_markdown_envelope(path)
        value = payload.get(field)
        if isinstance(value, str) and value:
            ids.add(value)
    return ids


def validate_repository(repo_root: Path, closure: bool) -> List[str]:
    """Validate the repository's complete G08 artifact set."""

    base = repo_root / "arxiv-reference"
    candidate_dir = base / "synthesis" / "architecture-candidates"
    transfer_ids = known_card_ids(base / "evidence" / "constraint-transfer-cards", "transfer_id")
    failure_ids = known_card_ids(base / "evidence" / "failure-cards", "failure_id")
    cards: List[Dict[str, object]] = []
    errors: List[str] = []
    for path in sorted(candidate_dir.glob("ARCH-G08-*.md")):
        try:
            cards.append(parse_candidate_card_file(path))
        except (ValueError, json.JSONDecodeError) as error:
            errors.append(path.name + ": " + str(error))
    errors.extend(validate_candidate_collection(cards, transfer_ids, failure_ids, closure))

    plan_path = base / "governance" / "g08-candidate-plan.tsv"
    if not plan_path.exists():
        errors.append("g08-candidate-plan.tsv: missing")
        rows: List[Dict[str, str]] = []
    else:
        rows = load_tsv(plan_path)
        errors.extend(validate_candidate_plan_rows(rows, closure))
        card_by_id = {str(card.get("candidate_id", "")): card for card in cards}
        if closure:
            for row in rows:
                card = card_by_id.get(row.get("candidate_id", ""))
                if card is None:
                    errors.append("candidate plan references missing card " + row.get("candidate_id", ""))
                    continue
                expected = canonical_candidate_checksum(row, card)
                if row.get("result_checksum") != expected:
                    errors.append(row.get("candidate_id", "") + ": result checksum mismatch")

    pareto_path = base / "synthesis" / "g08-pareto-archive.tsv"
    if closure:
        if not pareto_path.exists():
            errors.append("g08-pareto-archive.tsv: missing")
        else:
            errors.extend(validate_pareto_rows(load_tsv(pareto_path), {str(card.get("candidate_id", "")) for card in cards}))
        for required in (
            base / "governance" / "G08-goal-packet.md",
            base / "governance" / "g08-architecture-evolution-contract.md",
            base / "sources" / "G08-architecture-evolution-report.md",
            base / "governance" / "reviews" / "G08-adversarial-review.md",
            base / "journals" / "G08-progress.md",
        ):
            if not required.exists():
                errors.append(str(required.relative_to(repo_root)) + ": missing")
        review_path = base / "governance" / "reviews" / "G08-adversarial-review.md"
        if review_path.exists():
            review = review_path.read_text(encoding="utf-8")
            for marker in ("P0: 0", "P1: 0", "P2: 0", "Verdict: CLEARED"):
                if marker not in review:
                    errors.append("G08 review missing closure marker " + marker)
    return errors


def finalize_plan_checksums(repo_root: Path) -> None:
    """Populate deterministic checksums after all terminal fields are frozen."""

    base = repo_root / "arxiv-reference"
    plan_path = base / "governance" / "g08-candidate-plan.tsv"
    rows = load_tsv(plan_path)
    cards = {
        path.stem: parse_candidate_card_file(path)
        for path in sorted((base / "synthesis" / "architecture-candidates").glob("ARCH-G08-*.md"))
    }
    for row in rows:
        card = cards[row["candidate_id"]]
        row["result_checksum"] = canonical_candidate_checksum(row, card)
    with plan_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PLAN_HEADER, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--closure", action="store_true")
    parser.add_argument("--finalize-plan-checksums", action="store_true")
    args = parser.parse_args()
    if args.finalize_plan_checksums:
        finalize_plan_checksums(args.repo_root.resolve())
    errors = validate_repository(args.repo_root.resolve(), args.closure)
    if errors:
        for error in errors:
            print("FAIL:", error)
        return 1
    print("PASS: G08 architecture-evolution contract")
    return 0


if __name__ == "__main__":
    sys.exit(main())
