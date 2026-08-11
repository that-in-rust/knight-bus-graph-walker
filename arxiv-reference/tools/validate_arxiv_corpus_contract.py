#!/usr/bin/env python3
"""Validate the deterministic arXiv Pattern Foundry corpus contract."""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import marshal
import re
import subprocess
import sys
import types
from collections import Counter
from pathlib import Path, PurePosixPath
from typing import Dict, Iterable, List, Mapping, MutableSequence, Optional, Sequence, Tuple


ALLOWED_EPISTEMIC_LABELS = {
    "SOURCE_CLAIM",
    "DERIVED_INFERENCE",
    "SPECULATIVE_TRANSFER",
}

ALLOWED_TERM_TYPES = {
    "ALGORITHM",
    "LAYOUT",
    "STATE",
    "SCHEDULING",
    "IO",
    "PREDICTABILITY",
    "CORRECTNESS",
    "HARDWARE",
    "PRODUCT_CONTRACT",
}

ALLOWED_QUERY_STATUSES = {
    "PLANNED",
    "EXECUTED",
    "RATE_LIMITED",
    "FAILED",
    "SUPERSEDED",
}

ALLOWED_PAPER_STATUSES = {
    "METADATA_ONLY",
    "DEEP_READ",
    "READ_COMPLETE",
    "REJECTED",
    "UNAVAILABLE",
}

ALLOWED_CITATION_EDGE_TYPES = {
    "CITES",
    "IMPLEMENTS",
    "EVALUATES",
    "REFINES",
    "CONTRADICTS",
    "SURVEYS",
}

ALLOWED_LICENSE_STATES = {
    "LICENSE_PERMISSIVE_VERIFIED",
    "LICENSE_RESTRICTED_OR_CONDITIONAL",
    "LICENSE_UNKNOWN",
    "LICENSE_UNAVAILABLE",
}

PUBLIC_VALIDATOR_FUNCTIONS = (
    "validate_source_query_terms",
    "deduplicate_paper_manifest_entries",
    "validate_mechanism_card_fields",
    "validate_failure_card_fields",
    "validate_transfer_card_invariants",
    "score_architecture_candidate_niches",
    "verify_download_license_policy",
    "audit_requirement_test_links",
)

REQUIRED_CONTROL_PATHS = (
    ".gitignore",
    "Arxiv-Pattern-Foundry-SOP.md",
    "README.md",
    "governance/G00-goal-packet.md",
    "governance/G00-generation-ledger.md",
    "governance/artifact-schema-contracts.md",
    "governance/campaign-status.md",
    "governance/claim-evidence-policy.md",
    "governance/source-service-policy.md",
    "journals/G00-progress.md",
    "tests/test_validate_arxiv_corpus_contract.py",
    "tools/validate_arxiv_corpus_contract.py",
)

EXPECTED_TSV_HEADERS = {
    "governance/keyword-taxonomy.tsv": (
        "term_id\tterm\tterm_type\tarchitecture_question_ids\tsource_repo_paths\t"
        "synonyms\thistorical_terms\tadjacent_domain_terms\texclusion_terms\tnotes"
    ),
    "governance/query-ledger.tsv": (
        "query_id\tarchitecture_question_ids\tsource_term_ids\tservice\tquery_text\t"
        "categories\tdate_from\tdate_to\texclusions\texecuted_at\tresult_count\t"
        "response_checksum\tstatus"
    ),
    "sources/paper-manifest.tsv": (
        "paper_id\tarxiv_id\tdoi\ttitle\tauthors\tpublished_date\tupdated_date\t"
        "categories\tabstract_url\tpdf_url\tlicense_uri\tcanonical_version\t"
        "discovery_query_ids\tarchitecture_question_ids\trelevance_score\t"
        "score_breakdown\tselection_status\tevidence_grade\tcode_urls\tlocal_path\t"
        "sha256\tnotes"
    ),
    "sources/citation-edges.tsv": (
        "source_paper_id\ttarget_paper_id\tedge_type\tdiscovery_source\t"
        "relevance_reason\tverified_at"
    ),
    "sources/metadata-request-ledger.tsv": (
        "request_id\tgoal_id\tquery_id\tvariant_id\tservice\toperation\t"
        "normalized_query\tparameters\trequested_at_utc\tpage_cursor\t"
        "response_status\tresult_count\tresponse_checksum\tclient_version\t"
        "cache_status\tattempt\tretry_events\trate_limit_events\tpolicy_url\t"
        "policy_checked_date\tcache_path\tterminal_state"
    ),
    "sources/citation-request-ledger.tsv": (
        "request_id\tgoal_id\tseed_paper_id\ttraversal_paper_id\tdepth\t"
        "direction\tservice\toperation\tnormalized_identifier\tparameters\t"
        "requested_at_utc\tpage_cursor\tresponse_status\tresult_count\t"
        "response_checksum\tcache_checksum\tclient_version\tcache_status\tattempt\t"
        "retry_events\trate_limit_events\tpolicy_url\tpolicy_checked_date\t"
        "cache_path\tterminal_state"
    ),
}

G00_ALLOWED_FILE_PATHS = frozenset(REQUIRED_CONTROL_PATHS) | frozenset(
    EXPECTED_TSV_HEADERS
)

G01_REQUIRED_FILE_PATHS = (
    "governance/architecture-question-ledger.md",
    "governance/keyword-taxonomy.tsv",
    "governance/query-ledger.tsv",
    "journals/G01-progress.md",
    "tests/test_validate_g01_discovery_contract.py",
)

G01_ALLOWED_FILE_PATHS = frozenset(REQUIRED_CONTROL_PATHS) | frozenset(
    G01_REQUIRED_FILE_PATHS
)

G02_REQUIRED_FILE_PATHS = (
    "governance/architecture-question-ledger.md",
    "governance/keyword-taxonomy.tsv",
    "governance/query-ledger.tsv",
    "governance/g02-metadata-contract.md",
    "governance/g02-service-preflight.md",
    "journals/G01-progress.md",
    "journals/G02-progress.md",
    "sources/G02-metadata-screening-report.md",
    "sources/metadata-request-ledger.tsv",
    "sources/paper-manifest.tsv",
    "tests/test_validate_g01_discovery_contract.py",
    "tests/test_validate_g02_metadata_contract.py",
    "tools/g02_metadata_pipeline.py",
)

G02_FIXTURE_FILE_PATHS = (
    "tests/fixtures/g02/arxiv-basic.xml",
    "tests/fixtures/g02/arxiv-duplicates.xml",
    "tests/fixtures/g02/arxiv-empty.xml",
    "tests/fixtures/g02/arxiv-malformed.xml",
    "tests/fixtures/g02/arxiv-title-collision.xml",
    "tests/fixtures/g02/http-429.json",
    "tests/fixtures/g02/interrupted-pagination.json",
)

G02_ALLOWED_FILE_PATHS = (
    frozenset(REQUIRED_CONTROL_PATHS)
    | frozenset(G02_REQUIRED_FILE_PATHS)
    | frozenset(G02_FIXTURE_FILE_PATHS)
)

G03_REQUIRED_FILE_PATHS = (
    *G02_REQUIRED_FILE_PATHS,
    "governance/G03-goal-packet.md",
    "governance/g03-citation-contract.md",
    "governance/g03-service-preflight.md",
    "journals/G03-progress.md",
    "sources/citation-edges.tsv",
    "sources/citation-request-ledger.tsv",
    "tests/test_validate_g03_citation_contract.py",
    "tools/g03_citation_pipeline.py",
)

G03_FIXTURE_FILE_PATHS = (
    *G02_FIXTURE_FILE_PATHS,
    "tests/fixtures/g03/http-429.json",
    "tests/fixtures/g03/openalex-citations.json",
    "tests/fixtures/g03/openalex-empty.json",
    "tests/fixtures/g03/openalex-malformed.json",
    "tests/fixtures/g03/openalex-seed-work.json",
    "tests/fixtures/g03/openalex-title-collision.json",
    "tests/fixtures/g03/s2-citations.json",
    "tests/fixtures/g03/s2-references.json",
    "tests/fixtures/g03/s2-seed-batch.json",
)

G03_OPTIONAL_FILE_PATHS = (
    "sources/G03-citation-ancestry-report.md",
)

G03_ALLOWED_FILE_PATHS = (
    frozenset(REQUIRED_CONTROL_PATHS)
    | frozenset(G03_REQUIRED_FILE_PATHS)
    | frozenset(G03_FIXTURE_FILE_PATHS)
    | frozenset(G03_OPTIONAL_FILE_PATHS)
)

ALLOWED_CACHE_MODULES = {
    "tests": "test_validate_arxiv_corpus_contract",
    "tools": "validate_arxiv_corpus_contract",
}

ALLOWED_TEST_CACHE_MODULES = {
    "test_validate_arxiv_corpus_contract",
    "test_validate_g01_discovery_contract",
    "test_validate_g02_metadata_contract",
    "test_validate_g03_citation_contract",
}

ALLOWED_TOOL_CACHE_MODULES = {
    "validate_arxiv_corpus_contract",
    "g02_metadata_pipeline",
    "g03_citation_pipeline",
}

G01_QUESTION_FIELDS = (
    "family_slug",
    "decision",
    "product_consequence",
    "candidate_options",
    "known_evidence",
    "missing_evidence",
    "falsifier",
    "status",
    "owner_goal",
)

G01_REQUIRED_QUESTION_FAMILIES = {
    "algorithm-specific-layouts",
    "pagerank",
    "bfs",
    "wcc",
    "triangles",
    "communities",
    "similarity-knn",
    "bounded-ram-external-memory",
    "preprocessing-repeated-latency",
    "deterministic-ram-tail-latency",
    "neo4j-cypher-gds-compatibility",
    "correctness-verification-receipts",
}

G01_MAX_QUESTIONS = 12
G01_MAX_TERMS = 200
G01_MAX_QUERIES = 25
G01_NULL_SENTINEL = "NOT_EXECUTED"
G01_QUERY_GENERIC_TOKENS = {
    "algorithm",
    "algorithms",
    "bounded",
    "data",
    "exact",
    "graph",
    "graphs",
    "memory",
    "processing",
    "query",
    "ram",
    "state",
}

GIT_COMMAND_TIMEOUT_SECONDS = 30

G00_GENERATION_WRITERS = {
    "Planck": "019fec71-4b28-7bd1-8333-67af2c159524",
    "Raman": "019fec71-4ab0-7de3-9bd2-8bad97b8a06f",
    "Zeno": "019fec71-4a11-7f11-9940-41b66d9ec811",
    "Sartre": "019fec71-4bb7-7bf3-b041-293bfdf52bce",
    "Hypatia": "019fec84-86fa-7ff3-89fd-102f29e2c19f",
    "James": "019fec84-87ef-73a0-aa31-37b6ece9b0a9",
    "Euclid": "019fec84-8783-74c2-91b2-5ce1e61457dc",
    "Jason": "019fec84-8865-7542-9123-2c4210be9a5e",
    "Anscombe": "019fec9e-2394-7a62-9df1-7e31d3cd2d29",
    "Curie": "019fec9e-2314-72d3-ae46-f89d86aff681",
    "Newton": "019fec9e-252a-7f32-bed5-cda901506a16",
    "Ptolemy": "019fecbd-acf7-7e62-a28f-e3b2b8cab527",
    "Avicenna": "019fecbd-af95-7920-b1f9-39643eeb2048",
    "Plato": "019fecbd-b271-7aa3-8e3e-bc08fbd0a71e",
}

G00_PROMPT_SECTION_HEADINGS = (
    "### Initial Lane A Body: Planck",
    "### Initial Lane B Body: Raman",
    "### Initial Lane C Body: Zeno",
    "### Initial Lane D Body: Sartre",
    "### Repair Lane V Body: Hypatia",
    "### Repair Lane G Body: James",
    "### Repair Lane S Body: Euclid",
    "### Repair Lane I Body: Jason",
    "### Final Repair Lane: Anscombe",
    "### Final Repair Lane: Curie",
    "### Final Repair Lane: Newton",
    "### Integrity Repair Lane: Ptolemy",
    "### Integrity Repair Lane: Avicenna",
    "### Integrity Repair Lane: Plato",
)

G00_CHECKSUM_OUTPUT_PATHS = frozenset(
    {
        "Markdown-Value-Index.md",
        "arxiv-reference/.gitignore",
        "arxiv-reference/README.md",
        "arxiv-reference/governance/G00-goal-packet.md",
        "arxiv-reference/governance/artifact-schema-contracts.md",
        "arxiv-reference/governance/campaign-status.md",
        "arxiv-reference/governance/claim-evidence-policy.md",
        "arxiv-reference/governance/source-service-policy.md",
        "arxiv-reference/tests/test_validate_arxiv_corpus_contract.py",
        "arxiv-reference/tools/validate_arxiv_corpus_contract.py",
    }
)

TSV_PRIMARY_ID_FIELDS = {
    "governance/keyword-taxonomy.tsv": "term_id",
    "governance/query-ledger.tsv": "query_id",
    "sources/paper-manifest.tsv": "paper_id",
    "sources/metadata-request-ledger.tsv": "request_id",
    "sources/citation-request-ledger.tsv": "request_id",
}

GOAL_PACKET_FIELDS = (
    "Goal ID",
    "Objective",
    "A007 uncertainty reduced",
    "Inputs",
    "Owned outputs",
    "Batch caps",
    "Excluded work",
    "Entry tests",
    "Exit tests",
    "Stop conditions",
    "Journal",
)

MECHANISM_CARD_FIELDS = (
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
    "falsifying_experiment_id",
    "evidence_grade",
    "confidence_rationale",
    "related_pattern_ids",
)

FAILURE_CARD_FIELDS = (
    "failure_id",
    "name",
    "epistemic_label",
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
)

TRANSFER_CARD_FIELDS = (
    "transfer_id",
    "name",
    "epistemic_label",
    "source_pattern_ids",
    "original_domain",
    "original_constraints",
    "original_cost_model",
    "surviving_invariant",
    "reversed_assumptions",
    "modern_knight_bus_constraints",
    "proposed_transfer",
    "modern_resource_model",
    "analogy_failure_modes",
    "target_algorithm_families",
    "falsifying_experiment_id",
)

SCHEMA_SECTION_FIELDS = {
    "### 5.1 Architecture Question": (
        "question_id",
        "decision",
        "product_consequence",
        "candidate_options",
        "known_evidence",
        "missing_evidence",
        "falsifier",
        "status",
        "owner_goal",
    ),
    "### 5.2 Mechanism Card": MECHANISM_CARD_FIELDS
    + ("ram", "io", "preprocessing", "persistent_storage", "temporary_storage"),
    "### 5.3 Failure Card": FAILURE_CARD_FIELDS,
    "### 5.4 Constraint-Transfer Card": TRANSFER_CARD_FIELDS
    + ("ram", "io", "preprocessing", "storage", "concurrency"),
    "### 5.5 Architecture Candidate": (
        "architecture_id",
        "name",
        "epistemic_label",
        "architecture_question_ids",
        "parent_architecture_ids",
        "mechanism_pattern_ids",
        "failure_card_ids",
        "constraint_transfer_ids",
        "workload_contract",
        "genome",
        "resource_model",
        "preparation_model",
        "storage_amplification",
        "correctness_contract",
        "determinism_contract",
        "failure_boundaries",
        "fallback_response",
        "pareto_niches",
        "highest_evaluator_stage",
        "falsifying_experiment_id",
        "artifact",
        "algorithm_family",
        "exactness",
        "ram_ceiling_bytes",
        "storage_allowance_bytes",
        "deadline_model",
        "output_bound",
        "topology_layout",
        "ordering",
        "state_placement",
        "scheduling",
        "overflow_behavior",
        "admission_model",
        "receipt_model",
        "compatibility_boundary",
        "topology",
        "algorithm_state",
        "frontier_or_active_set",
        "scratch",
        "output",
        "conversion",
        "page_cache_or_direct_io",
        "runtime_overhead",
        "spill",
        "safety_margin",
    ),
    "### 5.6 Experiment Packet": (
        "experiment_id",
        "architecture_id",
        "hypothesis",
        "fixture_ids",
        "holdout_fixture_ids",
        "baseline",
        "independent_oracle",
        "controlled_variables",
        "measured_metrics",
        "acceptance_thresholds",
        "disconfirming_result",
        "modeled_expectation",
        "required_implementation_scope",
    ),
}

SCHEMA_REQUIRED_HEADINGS = (
    "# Artifact Schema Contracts",
    "## 1. Scope And Epistemic Discipline",
    "## 2. Stable Identifier Contract",
    "## 3. Exact TSV Headers",
    "## 4. Controlled Values",
    "## 5. Required Logical Schemas",
    "## 6. Completed And DRAFT Artifacts",
    "## 7. Empty-Corpus Semantics",
    "## 8. Cross-Link And Claim Rules",
    "## 9. Validator Behavior",
    "## 10. Deferred Schemas And Freeze Owners",
    "## 11. G00 Acceptance Contract",
)

G00_ARTIFACT_SCHEMA_CONTRACT_SHA256 = (
    "d674367ef1966e1bc7453ed9abac544a9897e0c5e0e9e8cd174b258d2398da56"
)


def normalize_field_text_value(value: object) -> str:
    """Return a stable, whitespace-normalized textual field value."""

    if value is None:
        return ""
    return " ".join(str(value).split())


def is_blank_field_value(value: object) -> bool:
    """Recognize absent scalar and empty collection values."""

    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, set, dict)):
        return not value
    return False


def validate_source_query_terms(terms: object) -> List[str]:
    """Validate taxonomy terms used to construct source queries.

    SOURCE_CLAIM provenance starts at this boundary: each term must identify the
    repository path and architecture question that caused it to be collected.
    """

    errors: List[str] = []
    if terms is None:
        records: List[object] = []
    elif isinstance(terms, Mapping):
        records = [terms]
    elif isinstance(terms, (str, bytes)):
        return ["keyword terms: expected a mapping or iterable of mappings"]
    else:
        try:
            records = list(terms)  # type: ignore[arg-type]
        except TypeError:
            return ["keyword terms: expected a mapping or iterable of mappings"]
    seen_term_ids = set()

    for index, term in enumerate(records, start=1):
        prefix = "keyword term row {0}".format(index)
        if not isinstance(term, Mapping):
            errors.append("{0}: expected a mapping".format(prefix))
            continue
        term_id = normalize_field_text_value(term.get("term_id"))
        if not term_id:
            errors.append("{0}: term_id is required".format(prefix))
        elif term_id in seen_term_ids:
            errors.append("{0}: duplicate term_id {1}".format(prefix, term_id))
        else:
            seen_term_ids.add(term_id)

        for field_name in ("term", "architecture_question_ids", "source_repo_paths"):
            if is_blank_field_value(term.get(field_name)):
                errors.append("{0}: {1} is required".format(prefix, field_name))

        term_type = normalize_field_text_value(term.get("term_type"))
        if term_type not in ALLOWED_TERM_TYPES:
            errors.append("{0}: invalid term_type {1!r}".format(prefix, term_type))

    return sorted(errors)


def deduplicate_paper_manifest_entries(
    entries: Iterable[Mapping[str, object]],
    duplicate_paper_ids: Optional[MutableSequence[str]] = None,
) -> List[Dict[str, object]]:
    """Remove exact duplicate rows and report exact duplicate primary IDs.

    G00 deliberately does not normalize titles, DOI forms, arXiv versions, or
    merge precedence. G02 owns those policies before its first manifest merge.
    """

    unique_entries: List[Dict[str, object]] = []
    paper_id_counts: Counter[str] = Counter()

    for raw_entry in entries:
        entry = dict(raw_entry)
        raw_paper_id = entry.get("paper_id")
        paper_id = "" if raw_paper_id is None else str(raw_paper_id)
        if paper_id:
            paper_id_counts[paper_id] += 1

        if not any(entry == known_entry for known_entry in unique_entries):
            unique_entries.append(entry)

    if duplicate_paper_ids is not None:
        duplicate_paper_ids.extend(
            sorted(paper_id for paper_id, count in paper_id_counts.items() if count > 1)
        )

    return unique_entries


def validate_card_required_fields(
    card: Mapping[str, object],
    required_fields: Sequence[str],
    card_kind: str,
    empty_list_fields: Sequence[str] = (),
) -> List[str]:
    """Validate common completed-card fields and epistemic labels."""

    errors: List[str] = []
    is_draft = normalize_field_text_value(card.get("status")).upper() == "DRAFT"

    for field_name in required_fields:
        if field_name not in card:
            errors.append("{0}: missing field {1}".format(card_kind, field_name))
        elif (
            not is_draft
            and is_blank_field_value(card.get(field_name))
            and not (
                field_name in empty_list_fields
                and isinstance(card.get(field_name), (list, tuple, set))
            )
        ):
            errors.append("{0}: blank field {1}".format(card_kind, field_name))

    epistemic_label = normalize_field_text_value(card.get("epistemic_label"))
    if epistemic_label and epistemic_label not in ALLOWED_EPISTEMIC_LABELS:
        errors.append("{0}: invalid epistemic_label {1!r}".format(card_kind, epistemic_label))
    if epistemic_label == "SOURCE_CLAIM":
        if is_blank_field_value(card.get("source_paper_ids")):
            errors.append("{0}: SOURCE_CLAIM requires source_paper_ids".format(card_kind))
        if is_blank_field_value(card.get("source_pointers")):
            errors.append("{0}: SOURCE_CLAIM requires source_pointers".format(card_kind))

    return errors


def validate_nested_model_fields(
    card: Mapping[str, object], model_name: str, required_fields: Sequence[str], card_kind: str
) -> List[str]:
    """Validate one nested resource-model mapping."""

    model = card.get(model_name)
    if not isinstance(model, Mapping):
        return ["{0}: {1} must be a mapping".format(card_kind, model_name)]

    is_draft = normalize_field_text_value(card.get("status")).upper() == "DRAFT"
    errors = []
    for field_name in required_fields:
        if field_name not in model:
            errors.append("{0}: {1}.{2} is missing".format(card_kind, model_name, field_name))
        elif not is_draft and is_blank_field_value(model.get(field_name)):
            errors.append("{0}: {1}.{2} is blank".format(card_kind, model_name, field_name))
    return errors


def validate_mechanism_card_fields(card: Mapping[str, object]) -> List[str]:
    """Validate a completed mechanism card and SOURCE_CLAIM provenance."""

    errors = validate_card_required_fields(
        card,
        MECHANISM_CARD_FIELDS,
        "mechanism card",
        ("source_paper_ids", "source_pointers", "related_pattern_ids"),
    )
    errors.extend(
        validate_nested_model_fields(
            card,
            "resource_model",
            ("ram", "io", "preprocessing", "persistent_storage", "temporary_storage"),
            "mechanism card",
        )
    )
    return sorted(set(errors))


def validate_failure_card_fields(card: Mapping[str, object]) -> List[str]:
    """Validate a failure card, including its adversarial failure signal."""

    return sorted(
        set(
            validate_card_required_fields(
                card,
                FAILURE_CARD_FIELDS,
                "failure card",
                (
                    "source_paper_ids",
                    "source_pointers",
                    "affected_pattern_ids",
                    "affected_architecture_ids",
                ),
            )
        )
    )


def validate_transfer_card_invariants(card: Mapping[str, object]) -> List[str]:
    """Validate a SPECULATIVE_TRANSFER without presenting it as source evidence."""

    errors = validate_card_required_fields(card, TRANSFER_CARD_FIELDS, "transfer card")
    epistemic_label = normalize_field_text_value(card.get("epistemic_label"))
    if epistemic_label and epistemic_label != "SPECULATIVE_TRANSFER":
        errors.append(
            "transfer card: epistemic_label must be SPECULATIVE_TRANSFER, not {0!r}".format(
                epistemic_label
            )
        )
    errors.extend(
        validate_nested_model_fields(
            card,
            "modern_resource_model",
            ("ram", "io", "preprocessing", "storage", "concurrency"),
            "transfer card",
        )
    )
    return sorted(set(errors))


def score_architecture_candidate_niches(
    candidates: Iterable[Mapping[str, object]],
) -> List[Tuple[str, int]]:
    """Report per-niche candidate coverage without ranking candidate quality."""

    niche_counts: Counter[str] = Counter()

    for candidate in candidates:
        raw_niches = candidate.get("pareto_niches", [])
        if isinstance(raw_niches, str):
            normalized_niche = normalize_field_text_value(raw_niches)
            niches = {normalized_niche} if normalized_niche else set()
        elif isinstance(raw_niches, Iterable) and not isinstance(raw_niches, Mapping):
            niches = {
                normalize_field_text_value(niche)
                for niche in raw_niches
                if normalize_field_text_value(niche)
            }
        else:
            niches = set()
        niche_counts.update(niches)

    return sorted(niche_counts.items())


def normalize_local_paper_path(local_path: str) -> str:
    """Normalize a manifest local path without touching the filesystem."""

    normalized = local_path.replace("\\", "/")
    return str(PurePosixPath(normalized))


def verify_download_license_policy(
    manifest_entries: Iterable[Mapping[str, object]], ignore_text: str
) -> List[str]:
    """Validate ignored full text, acquisition status, checksums, and license state."""

    errors = validate_ignore_policy_rules(ignore_text)
    sha256_pattern = re.compile(r"^[0-9a-fA-F]{64}$")

    for index, entry in enumerate(manifest_entries, start=1):
        local_path = normalize_field_text_value(entry.get("local_path"))
        if not local_path:
            continue

        paper_id = normalize_field_text_value(entry.get("paper_id")) or "row {0}".format(index)
        if local_path == "NOT_ACQUIRED":
            selection_status = normalize_field_text_value(entry.get("selection_status"))
            sha256 = normalize_field_text_value(entry.get("sha256"))
            if selection_status != "METADATA_ONLY":
                errors.append(
                    "paper manifest {0}: NOT_ACQUIRED requires METADATA_ONLY".format(
                        paper_id
                    )
                )
            if sha256 != "NOT_ACQUIRED":
                errors.append(
                    "paper manifest {0}: NOT_ACQUIRED path requires NOT_ACQUIRED sha256".format(
                        paper_id
                    )
                )
            continue

        portable_path = local_path.replace("\\", "/")
        parsed_path = PurePosixPath(portable_path)
        normalized_path = normalize_local_paper_path(local_path)
        if (
            parsed_path.is_absolute()
            or ".." in parsed_path.parts
            or len(parsed_path.parts) < 3
            or parsed_path.parts[:2] != ("sources", "papers")
        ):
            errors.append(
                "paper manifest {0}: local_path must be a safe relative path under "
                "sources/papers/".format(paper_id)
            )

        selection_status = normalize_field_text_value(entry.get("selection_status"))
        if selection_status not in {"DEEP_READ", "READ_COMPLETE"}:
            errors.append(
                "paper manifest {0}: local full text requires DEEP_READ or READ_COMPLETE".format(
                    paper_id
                )
            )

        notes = normalize_field_text_value(entry.get("notes"))
        license_tokens = re.findall(r"(?<![A-Z0-9_])LICENSE_[A-Z0-9_]+(?![A-Z0-9_])", notes)
        if len(license_tokens) != 1:
            errors.append(
                "paper manifest {0}: acquired full text requires exactly one "
                "explicit license state in notes".format(
                    paper_id
                )
            )
        elif license_tokens[0] not in ALLOWED_LICENSE_STATES:
            errors.append(
                "paper manifest {0}: unsupported license state {1}".format(
                    paper_id, license_tokens[0]
                )
            )
        elif license_tokens[0] == "LICENSE_PERMISSIVE_VERIFIED":
            license_uri = normalize_field_text_value(entry.get("license_uri"))
            if not license_uri or license_uri.upper() in {
                "UNKNOWN",
                "UNAVAILABLE",
                "NONE",
                "NOT_APPLICABLE",
            }:
                errors.append(
                    "paper manifest {0}: LICENSE_PERMISSIVE_VERIFIED requires "
                    "a discovered license URI".format(
                        paper_id
                    )
                )

        sha256 = normalize_field_text_value(entry.get("sha256"))
        if not sha256_pattern.fullmatch(sha256):
            errors.append(
                "paper manifest {0}: acquired full text requires a 64-character sha256".format(
                    paper_id
                )
            )

    return sorted(set(errors))


def audit_requirement_test_links(sop_text: str) -> List[str]:
    """Audit the SOP's 49 one-to-one REQ-to-TEST traceability links."""

    errors: List[str] = []
    requirement_ids = re.findall(
        r"^###\s+(REQ-[A-Z]+-\d{3}\.\d+):", sop_text, flags=re.MULTILINE
    )
    matrix_links = re.findall(
        r"^\|\s*(REQ-[A-Z]+-\d{3}\.\d+)\s*\|\s*(TEST-[A-Z]+-\d{3})\s*\|",
        sop_text,
        flags=re.MULTILINE,
    )
    matrix_requirement_ids = [requirement_id for requirement_id, _ in matrix_links]
    matrix_test_ids = [test_id for _, test_id in matrix_links]

    if len(requirement_ids) != 49:
        errors.append("SOP: expected 49 requirement definitions, found {0}".format(len(requirement_ids)))
    if len(matrix_links) != 49:
        errors.append("SOP: expected 49 requirement-to-test links, found {0}".format(len(matrix_links)))

    for requirement_id, count in sorted(Counter(requirement_ids).items()):
        if count != 1:
            errors.append("SOP: requirement defined {0} times: {1}".format(count, requirement_id))
    for requirement_id, count in sorted(Counter(matrix_requirement_ids).items()):
        if count != 1:
            errors.append("SOP: requirement linked {0} times: {1}".format(count, requirement_id))
    for test_id, count in sorted(Counter(matrix_test_ids).items()):
        if count != 1:
            errors.append("SOP: test ID linked {0} times: {1}".format(count, test_id))

    missing_links = sorted(set(requirement_ids) - set(matrix_requirement_ids))
    extra_links = sorted(set(matrix_requirement_ids) - set(requirement_ids))
    for requirement_id in missing_links:
        errors.append("SOP: requirement has no test link: {0}".format(requirement_id))
    for requirement_id in extra_links:
        errors.append("SOP: test matrix references undefined requirement: {0}".format(requirement_id))

    return sorted(set(errors))


def read_text_file_safely(path: Path, display_path: str) -> Tuple[str, List[str]]:
    """Read one UTF-8 contract file and return deterministic diagnostics."""

    try:
        return path.read_text(encoding="utf-8"), []
    except OSError as error:
        return "", ["{0}: cannot read file: {1}".format(display_path, error.strerror or error)]
    except UnicodeError:
        return "", ["{0}: file is not valid UTF-8".format(display_path)]


def is_regular_file_path(path: Path) -> bool:
    """Return whether a path is a regular file and not a symbolic link."""

    try:
        return not path.is_symlink() and path.is_file()
    except OSError:
        return False


def is_path_beneath_root(path: Path, root: Path) -> bool:
    """Return whether a resolved path remains beneath a resolved root."""

    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=True))
    except (OSError, ValueError):
        return False
    return True


def has_symlink_path_component(path: Path, root: Path) -> bool:
    """Return whether any component beneath root is a symbolic link."""

    try:
        relative_path = path.relative_to(root)
    except ValueError:
        return True

    current_path = root
    for path_part in relative_path.parts:
        current_path = current_path / path_part
        try:
            if current_path.is_symlink():
                return True
        except OSError:
            return True
    return False


def extract_markdown_heading_text(text: str, heading: str) -> str:
    """Extract one Markdown heading body through the next peer heading."""

    lines = text.splitlines()
    try:
        start_index = lines.index(heading) + 1
    except ValueError:
        return ""

    heading_level = len(heading) - len(heading.lstrip("#"))
    section_lines = []
    for line in lines[start_index:]:
        match = re.match(r"^(#+)\s+", line)
        if match and len(match.group(1)) <= heading_level:
            break
        section_lines.append(line)
    return "\n".join(section_lines)


def validate_artifact_schema_contract(schema_text: str) -> List[str]:
    """Validate the semantic minimum of the authoritative G00 schema contract."""

    display_path = "governance/artifact-schema-contracts.md"
    errors: List[str] = []
    schema_checksum = hashlib.sha256(schema_text.encode("utf-8")).hexdigest()
    if schema_checksum != G00_ARTIFACT_SCHEMA_CONTRACT_SHA256:
        errors.append(
            "{0}: SHA-256 contract mismatch; expected {1}, found {2}".format(
                display_path,
                G00_ARTIFACT_SCHEMA_CONTRACT_SHA256,
                schema_checksum,
            )
        )
    lines = schema_text.splitlines()

    for heading in SCHEMA_REQUIRED_HEADINGS:
        heading_count = lines.count(heading)
        if heading_count != 1:
            errors.append(
                "{0}: expected one schema heading {1!r}, found {2}".format(
                    display_path, heading, heading_count
                )
            )

    metadata_patterns = (
        r"^\*\*Status:\*\*\s+G00 schema definition$",
        r"^\*\*Authority:\*\*\s+`arxiv-reference/Arxiv-Pattern-Foundry-SOP\.md` version 0\.1$",
        r"^\*\*Instance count created by this document:\*\*\s+zero$",
    )
    for pattern in metadata_patterns:
        if not re.search(pattern, schema_text, flags=re.MULTILINE):
            errors.append("{0}: missing required G00 schema metadata".format(display_path))

    for relative_path, expected_header in sorted(EXPECTED_TSV_HEADERS.items()):
        if schema_text.count(expected_header) != 1:
            errors.append(
                "{0}: must contain the exact {1} header once".format(
                    display_path, relative_path
                )
            )

    for heading, required_fields in SCHEMA_SECTION_FIELDS.items():
        section_text = extract_markdown_heading_text(schema_text, heading)
        if not section_text:
            continue
        for field_name in required_fields:
            if not re.search(
                r"(?<![A-Za-z0-9_]){0}(?![A-Za-z0-9_])".format(
                    re.escape(field_name)
                ),
                section_text,
            ):
                errors.append(
                    "{0}: {1} omits required field {2}".format(
                        display_path, heading, field_name
                    )
                )

    controlled_values = (
        tuple(sorted(ALLOWED_EPISTEMIC_LABELS))
        + tuple(sorted(ALLOWED_TERM_TYPES))
        + tuple(sorted(ALLOWED_QUERY_STATUSES))
        + tuple(sorted(ALLOWED_PAPER_STATUSES))
        + tuple(sorted(ALLOWED_CITATION_EDGE_TYPES))
        + tuple(sorted(ALLOWED_LICENSE_STATES))
        + (
            "OPEN",
            "EVIDENCE_COLLECTING",
            "EXPERIMENT_READY",
            "DECIDED",
            "REJECTED",
            "A_REPRODUCED",
            "B_CODE_BACKED",
            "C_PAPER_BENCHMARK",
            "D_THEORETICAL_OR_INCOMPLETE",
            "E_CONTRADICTED",
            "REPAIR",
            "SPECIALIZE",
            "DEFER",
            "DRAFT",
            "SCHEMA_ONLY",
        )
    )
    controlled_section = extract_markdown_heading_text(
        schema_text, "## 4. Controlled Values"
    )
    for controlled_value in controlled_values:
        if not re.search(
            r"(?<![A-Za-z0-9_]){0}(?![A-Za-z0-9_])".format(
                re.escape(controlled_value)
            ),
            controlled_section,
        ):
            errors.append(
                "{0}: controlled values omit {1}".format(display_path, controlled_value)
            )

    validator_section = extract_markdown_heading_text(
        schema_text, "## 9. Validator Behavior"
    )
    for function_name in PUBLIC_VALIDATOR_FUNCTIONS:
        if not re.search(
            r"(?<![A-Za-z0-9_]){0}(?![A-Za-z0-9_])".format(
                re.escape(function_name)
            ),
            validator_section,
        ):
            errors.append(
                "{0}: validator behavior omits public function {1}".format(
                    display_path, function_name
                )
            )

    normalized_schema = normalize_field_text_value(schema_text)
    semantic_markers = (
        "A later-goal path MAY be absent during G00.",
        "G00 SHALL NOT create any of the following:",
        "Every required list key SHALL be present.",
        "a list SHALL be non-empty only when the SOP or this contract states "
        "explicit non-empty semantics for that list.",
        "the header followed by zero data rows is valid.",
        "SHALL NOT rank candidate quality",
        "prohibit tracked or staged PDFs",
        "G00 only rejects exact duplicate IDs and exact duplicate rows or records",
        "G02 SHALL freeze those details before the first manifest merge.",
        "exactly one explicit `LICENSE_*` state in `notes`",
    )
    for marker in semantic_markers:
        if marker not in normalized_schema:
            errors.append(
                "{0}: missing semantic contract marker {1!r}".format(
                    display_path, marker
                )
            )

    deferred_section = extract_markdown_heading_text(
        schema_text, "## 10. Deferred Schemas And Freeze Owners"
    )
    deferred_owners = (
        ("sources/download-ledger.tsv", "G04"),
        ("evidence/evidence-conflicts.tsv", "G06"),
        ("synthesis/pareto-archive.tsv", "G08"),
    )
    for deferred_path, owner_goal in deferred_owners:
        matching_lines = [
            line
            for line in deferred_section.splitlines()
            if deferred_path in line and owner_goal in line
        ]
        if len(matching_lines) != 1:
            errors.append(
                "{0}: deferred schema must assign {1} to {2}".format(
                    display_path, deferred_path, owner_goal
                )
            )

    return sorted(set(errors))


def read_active_goal_identifier(root: Path) -> Tuple[Optional[str], List[str]]:
    """Read the campaign's single active goal identifier."""

    relative_path = "governance/campaign-status.md"
    status_path = root / relative_path
    if not is_regular_file_path(status_path):
        return None, []

    status_text, errors = read_text_file_safely(status_path, relative_path)
    if errors:
        return None, errors
    matches = re.findall(
        r"^- Active goal:\s*`?(G\d{2})`?\s*$", status_text, flags=re.MULTILINE
    )
    if len(matches) != 1:
        return None, [
            "{0}: expected exactly one Active goal identifier".format(relative_path)
        ]
    return matches[0], []


def validate_g00_empty_artifacts(root: Path) -> List[str]:
    """Allow only the complete G00 control surface while G00 remains active."""

    errors: List[str] = []

    try:
        corpus_paths = sorted(
            root.rglob("*"),
            key=lambda path: path.relative_to(root).as_posix(),
        )
    except OSError as error:
        return ["root: cannot inspect G00 corpus: {0}".format(error.strerror or error)]

    for corpus_path in corpus_paths:
        relative_path = corpus_path.relative_to(root)
        relative_path_text = relative_path.as_posix()
        if corpus_path.is_dir() and not corpus_path.is_symlink():
            continue
        if relative_path_text in G00_ALLOWED_FILE_PATHS and is_regular_file_path(
            corpus_path
        ):
            continue
        if is_allowed_python_cache(root, relative_path):
            continue
        errors.append(
            "{0}: file is not allowed while active goal is G00".format(
                relative_path_text
            )
        )

    for relative_path in sorted(EXPECTED_TSV_HEADERS):
        tsv_path = root / relative_path
        if not is_regular_file_path(tsv_path):
            continue
        rows, row_errors = read_tsv_file_rows(tsv_path, relative_path)
        if row_errors:
            continue
        for row_number, _row in enumerate(rows, start=2):
            errors.append(
                "{0}: row {1} is not allowed while active goal is G00".format(
                    relative_path, row_number
                )
            )

    return sorted(set(errors))


def is_allowed_python_cache(root: Path, relative_path: Path) -> bool:
    """Allow only interpreter-created bytecode in owned tool cache directories."""

    if len(relative_path.parts) != 3 or relative_path.parts[1] != "__pycache__":
        return False

    module_name = ALLOWED_CACHE_MODULES.get(relative_path.parts[0])
    if relative_path.parts[0] == "tests":
        module_name = next(
            (
                candidate
                for candidate in sorted(ALLOWED_TEST_CACHE_MODULES)
                if relative_path.name.startswith(candidate + ".")
            ),
            None,
        )
    elif relative_path.parts[0] == "tools":
        module_name = next(
            (
                candidate
                for candidate in sorted(ALLOWED_TOOL_CACHE_MODULES)
                if relative_path.name.startswith(candidate + ".")
            ),
            None,
        )
    cache_tag = sys.implementation.cache_tag
    if not module_name or not cache_tag:
        return False

    cache_name_pattern = re.compile(
        r"^{0}\.{1}(?:\.opt-[12])?\.pyc$".format(
            re.escape(module_name), re.escape(cache_tag)
        )
    )
    if not cache_name_pattern.fullmatch(relative_path.name):
        return False

    cache_path = root / relative_path
    if not is_regular_file_path(cache_path):
        return False
    try:
        cache_payload = cache_path.read_bytes()
    except OSError:
        return False
    if len(cache_payload) < 16 or cache_payload[:4] != importlib.util.MAGIC_NUMBER:
        return False

    bytecode_flags = int.from_bytes(cache_payload[4:8], byteorder="little")
    if bytecode_flags & ~0x03:
        return False
    try:
        code_object = marshal.loads(cache_payload[16:])
    except (EOFError, TypeError, ValueError):
        return False
    return isinstance(code_object, types.CodeType)


def validate_git_tracked_pdfs(root: Path) -> List[str]:
    """Reject Git tracked or staged PDFs beneath the explicit corpus root."""

    try:
        worktree_result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=str(root),
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=GIT_COMMAND_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return ["git: timed out while locating the worktree for tracked PDF inspection"]
    except OSError as error:
        return ["git: cannot inspect tracked PDFs: {0}".format(error.strerror or error)]

    if worktree_result.returncode != 0:
        return []

    worktree_path = Path(
        worktree_result.stdout.decode("utf-8", errors="surrogateescape").strip()
    )
    try:
        root_relative_path = root.resolve(strict=True).relative_to(
            worktree_path.resolve(strict=True)
        )
    except (OSError, ValueError):
        return ["git: arxiv-reference root is outside the discovered worktree"]

    root_pathspec = root_relative_path.as_posix() or "."
    try:
        tracked_result = subprocess.run(
            [
                "git",
                "-C",
                str(worktree_path),
                "ls-files",
                "--cached",
                "-z",
                "--",
                root_pathspec,
            ],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=GIT_COMMAND_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return ["git: timed out while listing tracked PDFs under arxiv-reference"]
    except OSError as error:
        return ["git: cannot list tracked PDFs: {0}".format(error.strerror or error)]
    if tracked_result.returncode != 0:
        return [
            "git: cannot list tracked PDFs under arxiv-reference (exit {0})".format(
                tracked_result.returncode
            )
        ]

    tracked_paths = []
    for encoded_path in tracked_result.stdout.split(b"\0"):
        if not encoded_path:
            continue
        tracked_path = encoded_path.decode("utf-8", errors="surrogateescape")
        if not tracked_path.casefold().endswith(".pdf"):
            continue
        try:
            relative_path = PurePosixPath(tracked_path).relative_to(
                PurePosixPath(root_pathspec)
            )
        except ValueError:
            continue
        tracked_paths.append(relative_path.as_posix())

    return [
        "{0}: PDF is tracked or staged by Git".format(path)
        for path in sorted(tracked_paths)
    ]


def validate_required_control_files(root: Path) -> List[str]:
    """Require every control file owned by the minimum G00 scaffold."""

    errors = []
    for relative_path in REQUIRED_CONTROL_PATHS:
        control_path = root / relative_path
        if control_path.is_symlink() or (control_path.exists() and not control_path.is_file()):
            errors.append(
                "{0}: required G00 control file must be a regular non-symlink file".format(
                    relative_path
                )
            )
            continue
        if not control_path.exists():
            errors.append("{0}: required G00 control file is missing".format(relative_path))
            continue
        try:
            if control_path.stat().st_size == 0:
                errors.append("{0}: required G00 control file is empty".format(relative_path))
        except OSError as error:
            errors.append(
                "{0}: cannot inspect file: {1}".format(relative_path, error.strerror or error)
            )
    return sorted(errors)


def validate_ignore_policy_rules(ignore_text: str) -> List[str]:
    """Validate the local full-text and PDF ignore policy."""

    errors = []
    policy_lines = {
        line.strip()
        for line in ignore_text.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    for required_rule in ("sources/papers/", "*.pdf"):
        if required_rule not in policy_lines:
            errors.append(".gitignore: missing required rule {0}".format(required_rule))

    for policy_line in sorted(policy_lines):
        if not policy_line.startswith("!"):
            continue
        unignored_path = policy_line[1:].lstrip("/")
        if unignored_path.casefold().endswith(".pdf") or unignored_path.startswith(
            "sources/papers/"
        ):
            errors.append(".gitignore: full text must not be unignored: {0}".format(policy_line))

    return errors


def extract_markdown_section_text(text: str, heading: str) -> str:
    """Extract one level-two Markdown section by exact heading."""

    marker = "## " + heading
    lines = text.splitlines()
    try:
        start_index = lines.index(marker) + 1
    except ValueError:
        return ""

    section_lines = []
    for line in lines[start_index:]:
        if line.startswith("## "):
            break
        section_lines.append(line)
    return "\n".join(section_lines)


def normalize_markdown_path_value(value: str) -> str:
    """Extract a path from Markdown inline code or plain sentence text."""

    inline_path_match = re.search(r"`([^`]+)`", value)
    if inline_path_match:
        return inline_path_match.group(1)
    return value.rstrip(".").strip()


def validate_goal_packet_shape(
    packet_text: str, display_path: str, expected_goal: str = "G00"
) -> List[str]:
    """Validate one exact, bounded Goal Packet field set."""

    errors: List[str] = []
    packet_fields: Dict[str, str] = {}
    field_counts: Counter[str] = Counter()
    for line in packet_text.splitlines():
        match = re.match(r"^- ([^:]+):\s*(.*)$", line)
        if match:
            field_name = match.group(1).strip()
            field_counts[field_name] += 1
            packet_fields[field_name] = match.group(2).strip()

    for field_name in GOAL_PACKET_FIELDS:
        if not packet_fields.get(field_name):
            errors.append("{0}: Goal Packet missing {1}".format(display_path, field_name))
        if field_counts[field_name] > 1:
            errors.append("{0}: Goal Packet repeats {1}".format(display_path, field_name))

    if packet_fields.get("Goal ID") != expected_goal:
        errors.append(
            "{0}: Goal Packet must select exactly {1}".format(
                display_path, expected_goal
            )
        )
    journal_path = normalize_markdown_path_value(packet_fields.get("Journal", ""))
    expected_journal_path = "arxiv-reference/journals/{0}-progress.md".format(
        expected_goal
    )
    if journal_path and journal_path != expected_journal_path:
        errors.append("{0}: Goal Packet Journal path is invalid".format(display_path))

    return sorted(set(errors))


def validate_generation_ledger_shape(
    ledger_text: str,
    root: Optional[Path] = None,
    verify_current_checksums: bool = True,
) -> List[str]:
    """Validate writer, prompt, time-bound, and checksum generation evidence."""

    display_path = "governance/G00-generation-ledger.md"
    errors: List[str] = []
    required_markers = (
        "# G00 Generation Ledger",
        "## Generation Environment",
        "## Writer Registry",
        "## Read-Only Reviewers",
        "## Checkpoint Time Bounds",
        "gpt-5.6-sol",
        "xhigh",
        "priority",
        "## Prompt Reconstruction",
        "## Artifact Checksum Snapshot",
        "## Reproducibility Limits",
    )
    for marker in required_markers:
        if marker not in ledger_text:
            errors.append(
                "{0}: missing required generation marker {1!r}".format(
                    display_path, marker
                )
            )

    for writer_name, agent_id in sorted(G00_GENERATION_WRITERS.items()):
        if writer_name not in ledger_text or agent_id not in ledger_text:
            errors.append(
                "{0}: missing writer registry evidence for {1}".format(
                    display_path, writer_name
                )
            )

    for prompt_heading in G00_PROMPT_SECTION_HEADINGS:
        if ledger_text.count(prompt_heading) != 1:
            errors.append(
                "{0}: expected one prompt section {1!r}".format(
                    display_path, prompt_heading
                )
            )

    time_bound_rows = re.findall(
        r"^\| (Initial|Repair|Final repair|Integrity repair) \| `([0-9TZ: -]+)` \| "
        r"`([0-9TZ: -]+)` \|",
        ledger_text,
        flags=re.MULTILINE,
    )
    if len(time_bound_rows) != 4:
        errors.append(
            "{0}: expected four bounded checkpoint time rows".format(display_path)
        )

    checksum_rows = re.findall(
        r"^\| `([^`]+)` \| `([0-9a-f]{64})` \| ([^|]+) \|$",
        ledger_text,
        flags=re.MULTILINE,
    )
    checksum_paths = [path for path, _checksum, _history in checksum_rows]
    if len(checksum_rows) != len(G00_CHECKSUM_OUTPUT_PATHS):
        errors.append(
            "{0}: expected {1} output checksum rows, found {2}".format(
                display_path, len(G00_CHECKSUM_OUTPUT_PATHS), len(checksum_rows)
            )
        )
    if set(checksum_paths) != G00_CHECKSUM_OUTPUT_PATHS:
        errors.append("{0}: output checksum path set is incomplete".format(display_path))
    if len(checksum_paths) != len(set(checksum_paths)):
        errors.append("{0}: output checksum paths must be unique".format(display_path))

    if root is not None:
        repository_root = root.parent
        for relative_path, expected_checksum, _history in checksum_rows:
            if relative_path not in G00_CHECKSUM_OUTPUT_PATHS:
                errors.append(
                    "{0}: checksum path is not an allowed G00 output: {1}".format(
                        display_path, relative_path
                    )
                )
                continue

            output_path = repository_root / relative_path
            if not is_path_beneath_root(output_path, repository_root):
                errors.append(
                    "{0}: checksum path escapes the repository: {1}".format(
                        display_path, relative_path
                    )
                )
                continue
            if has_symlink_path_component(output_path, repository_root):
                errors.append(
                    "{0}: checksum target must be a regular non-symlink file: {1}".format(
                        display_path, relative_path
                    )
                )
                continue
            if not is_regular_file_path(output_path):
                if relative_path == "Markdown-Value-Index.md" and not output_path.exists():
                    continue
                if not output_path.exists():
                    errors.append(
                        "{0}: checksum path is missing: {1}".format(
                            display_path, relative_path
                        )
                    )
                else:
                    errors.append(
                        "{0}: checksum target must be a regular non-symlink file: {1}".format(
                            display_path, relative_path
                        )
                    )
                continue
            if not verify_current_checksums:
                continue
            try:
                actual_checksum = hashlib.sha256(output_path.read_bytes()).hexdigest()
            except OSError as error:
                errors.append(
                    "{0}: cannot read checksum target {1}: {2}".format(
                        display_path, relative_path, error.strerror or error
                    )
                )
                continue
            if actual_checksum != expected_checksum:
                errors.append(
                    "{0}: checksum mismatch for {1}".format(
                        display_path, relative_path
                    )
                )

    return sorted(set(errors))


def extract_checkpoint_field_text(session_text: str, field_name: str) -> Optional[str]:
    """Extract one inline or block checkpoint field value."""

    heading_match = re.search(
        r"^#{{4,6}}\s+{0}:?[ \t]*(.*)$".format(re.escape(field_name)),
        session_text,
        flags=re.MULTILINE,
    )
    if not heading_match:
        return None
    inline_value = heading_match.group(1).strip()
    if inline_value:
        return inline_value

    remaining_text = session_text[heading_match.end() :]
    next_heading = re.search(r"^#{3,6}\s+", remaining_text, flags=re.MULTILINE)
    field_body = remaining_text[: next_heading.start()] if next_heading else remaining_text
    return field_body.strip()


def validate_goal_journal_shape(
    journal_text: str, goal_id: str = "G00"
) -> List[str]:
    """Validate one goal packet and resumable TDD journal shape."""

    errors: List[str] = []
    display_path = "journals/{0}-progress.md".format(goal_id)
    if not journal_text.startswith("# TDD Progress Journal\n"):
        errors.append("{0}: expected TDD Progress Journal title".format(display_path))

    for metadata_name in ("Task", "Created", "Updated", "Current Phase", "Status"):
        if not re.search(
            r"^- {0}:\s*\S.+$".format(re.escape(metadata_name)),
            journal_text,
            flags=re.MULTILINE,
        ):
            errors.append(
                "{0}: missing non-empty metadata {1}".format(
                    display_path, metadata_name
                )
            )

    packet_text = extract_markdown_section_text(journal_text, "Goal Packet")
    if not packet_text:
        errors.append("{0}: missing Goal Packet section".format(display_path))
    else:
        errors.extend(
            validate_goal_packet_shape(packet_text, display_path, expected_goal=goal_id)
        )

    sessions_text = extract_markdown_section_text(journal_text, "Sessions")
    if not sessions_text:
        errors.append("{0}: missing Sessions section".format(display_path))
    elif not re.search(r"^### Session:\s*\S.+$", sessions_text, flags=re.MULTILINE):
        errors.append(
            "{0}: Sessions must contain a timestamped session".format(display_path)
        )

    checkpoint_fields = (
        "Current Phase",
        "Tests Written",
        "Implementation Progress",
        "Current Focus",
        "Next Steps",
        "Context Notes",
        "Performance/Metrics",
    )
    allowed_phases = {"Stub", "Red", "Green", "Refactor", "Verify"}
    session_starts = list(
        re.finditer(r"^### Session:\s*\S.+$", sessions_text, flags=re.MULTILINE)
    )
    for session_index, session_start in enumerate(session_starts, start=1):
        session_end = (
            session_starts[session_index].start()
            if session_index < len(session_starts)
            else len(sessions_text)
        )
        session_text = sessions_text[session_start.end() : session_end]
        for field_name in checkpoint_fields:
            field_value = extract_checkpoint_field_text(session_text, field_name)
            if field_value is None:
                errors.append(
                    "{0}: session {1} missing {2}".format(
                        display_path, session_index, field_name
                    )
                )
            elif not field_value:
                errors.append(
                    "{0}: session {1} has empty {2}".format(
                        display_path, session_index, field_name
                    )
                )

        phase = extract_checkpoint_field_text(session_text, "Current Phase")
        if phase and phase not in allowed_phases:
            errors.append(
                "{0}: session {1} has invalid Current Phase {2!r}".format(
                    display_path, session_index, phase
                )
            )

    return sorted(set(errors))


def read_tsv_file_rows(path: Path, relative_path: str) -> Tuple[List[Dict[str, str]], List[str]]:
    """Read a header-validated TSV file into row dictionaries."""

    text, errors = read_text_file_safely(path, relative_path)
    if errors:
        return [], errors

    lines = text.splitlines()
    expected_header = EXPECTED_TSV_HEADERS[relative_path]
    if not lines or lines[0] != expected_header:
        return [], ["{0}: header does not match the SOP contract".format(relative_path)]

    expected_fields = expected_header.split("\t")
    rows: List[Dict[str, str]] = []
    reader = csv.reader(lines[1:], delimiter="\t", strict=True)
    try:
        for line_number, values in enumerate(reader, start=2):
            if not values or all(not value for value in values):
                continue
            if len(values) != len(expected_fields):
                errors.append(
                    "{0}: row {1} has {2} columns; expected {3}".format(
                        relative_path, line_number, len(values), len(expected_fields)
                    )
                )
                continue
            rows.append(dict(zip(expected_fields, values)))
    except csv.Error as error:
        errors.append("{0}: invalid TSV data: {1}".format(relative_path, error))

    return rows, sorted(errors)


def validate_query_ledger_rows(rows: Sequence[Mapping[str, str]]) -> List[str]:
    """Validate traceability and closed statuses for query ledger rows."""

    errors = []
    for index, row in enumerate(rows, start=2):
        prefix = "governance/query-ledger.tsv: row {0}".format(index)
        for field_name in ("query_id", "architecture_question_ids", "source_term_ids"):
            if not row.get(field_name, "").strip():
                errors.append("{0} requires {1}".format(prefix, field_name))
        status = row.get("status", "").strip()
        if status not in ALLOWED_QUERY_STATUSES:
            errors.append("{0} has invalid status {1!r}".format(prefix, status))
        if status == "EXECUTED":
            for field_name in (
                "service",
                "query_text",
                "categories",
                "date_from",
                "date_to",
                "exclusions",
                "executed_at",
                "result_count",
                "response_checksum",
            ):
                if not row.get(field_name, "").strip():
                    errors.append(
                        "{0} EXECUTED row requires {1}".format(prefix, field_name)
                    )
    return sorted(errors)


def validate_paper_manifest_rows(rows: Sequence[Mapping[str, str]]) -> List[str]:
    """Validate canonical paper IDs and selection statuses."""

    errors = []
    for index, row in enumerate(rows, start=2):
        prefix = "sources/paper-manifest.tsv: row {0}".format(index)
        if not row.get("paper_id", "").strip():
            errors.append("{0} requires paper_id".format(prefix))
        status = row.get("selection_status", "").strip()
        if status not in ALLOWED_PAPER_STATUSES:
            errors.append("{0} has invalid selection_status {1!r}".format(prefix, status))
    return sorted(errors)


def validate_citation_edge_rows(rows: Sequence[Mapping[str, str]]) -> List[str]:
    """Validate citation endpoints, provenance, and closed edge types."""

    errors = []
    for index, row in enumerate(rows, start=2):
        prefix = "sources/citation-edges.tsv: row {0}".format(index)
        for field_name in (
            "source_paper_id",
            "target_paper_id",
            "discovery_source",
            "relevance_reason",
            "verified_at",
        ):
            if not row.get(field_name, "").strip():
                errors.append("{0} requires {1}".format(prefix, field_name))
        edge_type = row.get("edge_type", "").strip()
        if edge_type not in ALLOWED_CITATION_EDGE_TYPES:
            errors.append("{0} has invalid edge_type {1!r}".format(prefix, edge_type))
    return sorted(errors)


def load_g02_pipeline_module() -> object:
    """Load the sibling G02 pipeline without relying on process import paths."""

    pipeline_path = Path(__file__).with_name("g02_metadata_pipeline.py")
    specification = importlib.util.spec_from_file_location(
        "arxiv_g02_metadata_pipeline", pipeline_path
    )
    if specification is None or specification.loader is None:
        raise RuntimeError("cannot load G02 metadata pipeline")
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


def load_g03_pipeline_module() -> object:
    """Load the sibling G03 pipeline without relying on process import paths."""

    pipeline_path = Path(__file__).with_name("g03_citation_pipeline.py")
    specification = importlib.util.spec_from_file_location(
        "arxiv_g03_citation_pipeline", pipeline_path
    )
    if specification is None or specification.loader is None:
        raise RuntimeError("cannot load G03 citation pipeline")
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


def validate_optional_tsv_files(root: Path, ignore_text: str) -> List[str]:
    """Validate optional corpus ledgers when their first row appears."""

    errors: List[str] = []
    for relative_path in sorted(EXPECTED_TSV_HEADERS):
        tsv_path = root / relative_path
        if not tsv_path.exists():
            continue
        if not is_regular_file_path(tsv_path):
            errors.append("{0}: expected a regular non-symlink file".format(relative_path))
            continue

        rows, row_errors = read_tsv_file_rows(tsv_path, relative_path)
        errors.extend(row_errors)
        if row_errors:
            continue

        expected_fields = EXPECTED_TSV_HEADERS[relative_path].split("\t")
        row_counts = Counter(
            tuple(row.get(field_name, "") for field_name in expected_fields)
            for row in rows
        )
        if any(count > 1 for count in row_counts.values()):
            errors.append("{0}: contains an exact duplicate row".format(relative_path))

        primary_id_field = TSV_PRIMARY_ID_FIELDS.get(relative_path)
        if primary_id_field:
            primary_id_counts = Counter(
                row.get(primary_id_field, "")
                for row in rows
                if row.get(primary_id_field, "")
            )
            for primary_id, count in sorted(primary_id_counts.items()):
                if count > 1:
                    errors.append(
                        "{0}: duplicate {1} {2}".format(
                            relative_path, primary_id_field, primary_id
                        )
                    )

        if relative_path == "governance/keyword-taxonomy.tsv":
            errors.extend(validate_source_query_terms(rows))
        elif relative_path == "governance/query-ledger.tsv":
            errors.extend(validate_query_ledger_rows(rows))
        elif relative_path == "sources/paper-manifest.tsv":
            errors.extend(validate_paper_manifest_rows(rows))
            errors.extend(verify_download_license_policy(rows, ignore_text))
        elif relative_path == "sources/citation-edges.tsv":
            errors.extend(validate_citation_edge_rows(rows))
        elif relative_path == "sources/metadata-request-ledger.tsv":
            try:
                g02_pipeline = load_g02_pipeline_module()
                errors.extend(g02_pipeline.validate_request_provenance_rows(rows))
            except (OSError, RuntimeError) as error:
                errors.append("cannot load G02 request validator: {0}".format(error))
        elif relative_path == "sources/citation-request-ledger.tsv":
            try:
                g03_pipeline = load_g03_pipeline_module()
                errors.extend(g03_pipeline.validate_citation_request_rows(rows))
            except (OSError, RuntimeError) as error:
                errors.append("cannot load G03 request validator: {0}".format(error))

    return sorted(set(errors))


def split_g01_multi_value(value: str) -> List[str]:
    """Split one G01 pipe-delimited field into nonblank values."""

    return [part.strip() for part in value.split("|") if part.strip()]


def tokenize_g01_query_vocabulary(value: str) -> set[str]:
    """Return meaningful lowercase tokens for G01 term-to-query overlap."""

    return {
        token
        for token in re.findall(r"[a-z0-9]+", value.casefold())
        if len(token) >= 3 and token not in G01_QUERY_GENERIC_TOKENS
    }


def parse_g01_architecture_questions(
    ledger_text: str,
) -> List[Dict[str, str]]:
    """Parse the machine-readable single-line fields in the G01 question ledger."""

    question_pattern = re.compile(
        r"^## (AQ-\d{3}): ([^\n]+)\n(?P<body>.*?)(?=^## AQ-\d{3}:|\Z)",
        flags=re.MULTILINE | re.DOTALL,
    )
    records: List[Dict[str, str]] = []
    for match in question_pattern.finditer(ledger_text):
        record = {
            "question_id": match.group(1),
            "title": match.group(2).strip(),
        }
        for line in match.group("body").splitlines():
            field_match = re.match(r"^- ([a-z_]+):\s*(.+)$", line)
            if field_match:
                record[field_match.group(1)] = field_match.group(2).strip()
        records.append(record)
    return records


def validate_g01_question_ledger(
    ledger_text: str, repository_root: Path
) -> Tuple[List[str], List[Dict[str, str]]]:
    """Validate the bounded G01 question set and its repository evidence paths."""

    display_path = "governance/architecture-question-ledger.md"
    errors: List[str] = []
    encoding_markers = (
        "## Encoding Contract",
        "UTF-8",
        "LF",
        "TAB",
        "`|`",
        "`%7C`",
        "`NOT_EXECUTED`",
        "`AQ-NNN`",
        "`TERM-NNN`",
        "`QRY-NNN`",
    )
    for marker in encoding_markers:
        if marker not in ledger_text:
            errors.append("{0}: missing encoding marker {1}".format(display_path, marker))
    if "## Encoding Contract" in ledger_text and "## AQ-001:" in ledger_text:
        if ledger_text.index("## Encoding Contract") > ledger_text.index("## AQ-001:"):
            errors.append("{0}: encoding contract must precede question rows".format(display_path))

    questions = parse_g01_architecture_questions(ledger_text)
    if len(questions) != G01_MAX_QUESTIONS:
        errors.append(
            "{0}: expected exactly {1} architecture questions, found {2}".format(
                display_path, G01_MAX_QUESTIONS, len(questions)
            )
        )
    expected_ids = ["AQ-{0:03d}".format(index) for index in range(1, len(questions) + 1)]
    actual_ids = [question.get("question_id", "") for question in questions]
    if actual_ids != expected_ids:
        errors.append("{0}: architecture question IDs must be contiguous".format(display_path))

    family_slugs = set()
    for question in questions:
        question_id = question.get("question_id", "UNKNOWN")
        for field_name in G01_QUESTION_FIELDS:
            if not question.get(field_name, "").strip():
                errors.append(
                    "{0}: {1} requires {2}".format(
                        display_path, question_id, field_name
                    )
                )
        family_slugs.add(question.get("family_slug", ""))
        if question.get("status") != "OPEN":
            errors.append("{0}: {1} status must be OPEN".format(display_path, question_id))
        if question.get("owner_goal") != "G01":
            errors.append("{0}: {1} owner_goal must be G01".format(display_path, question_id))

        evidence_paths = re.findall(r"`([^`]+)`", question.get("known_evidence", ""))
        if not evidence_paths:
            errors.append("{0}: {1} requires repository evidence paths".format(display_path, question_id))
        for evidence_path in evidence_paths:
            if "://" in evidence_path or Path(evidence_path).is_absolute():
                errors.append(
                    "{0}: {1} evidence path must be repository-relative: {2}".format(
                        display_path, question_id, evidence_path
                    )
                )
                continue
            candidate_path = repository_root / evidence_path
            if not is_path_beneath_root(candidate_path, repository_root) or not is_regular_file_path(
                candidate_path
            ):
                errors.append(
                    "{0}: {1} evidence path does not exist: {2}".format(
                        display_path, question_id, evidence_path
                    )
                )

    if family_slugs != G01_REQUIRED_QUESTION_FAMILIES:
        errors.append("{0}: required question-family set is incomplete".format(display_path))
    return sorted(set(errors)), questions


def validate_g01_discovery_rows(
    taxonomy_rows: Sequence[Mapping[str, str]],
    query_rows: Sequence[Mapping[str, str]],
    questions: Sequence[Mapping[str, str]],
    repository_root: Path,
) -> List[str]:
    """Validate G01 taxonomy and planned-query traceability as one closed batch."""

    errors: List[str] = []
    question_ids = {question.get("question_id", "") for question in questions}

    if not taxonomy_rows or len(taxonomy_rows) > G01_MAX_TERMS:
        errors.append(
            "governance/keyword-taxonomy.tsv: expected 1-{0} rows, found {1}".format(
                G01_MAX_TERMS, len(taxonomy_rows)
            )
        )
    expected_term_ids = [
        "TERM-{0:03d}".format(index) for index in range(1, len(taxonomy_rows) + 1)
    ]
    actual_term_ids = [row.get("term_id", "") for row in taxonomy_rows]
    if actual_term_ids != expected_term_ids:
        errors.append("governance/keyword-taxonomy.tsv: term IDs must be contiguous")

    term_ids = set(actual_term_ids)
    term_rows_by_id = {
        row.get("term_id", ""): row for row in taxonomy_rows if row.get("term_id", "")
    }
    term_questions: Dict[str, set[str]] = {}
    normalized_terms: Counter[str] = Counter()
    taxonomy_question_coverage: set[str] = set()
    taxonomy_fields = EXPECTED_TSV_HEADERS["governance/keyword-taxonomy.tsv"].split("\t")
    for index, row in enumerate(taxonomy_rows, start=2):
        prefix = "governance/keyword-taxonomy.tsv: row {0}".format(index)
        for field_name in taxonomy_fields:
            if not row.get(field_name, "").strip():
                errors.append("{0} requires {1}".format(prefix, field_name))
        normalized_terms[row.get("term", "").strip().casefold()] += 1
        linked_questions = set(split_g01_multi_value(row.get("architecture_question_ids", "")))
        if not linked_questions or not linked_questions <= question_ids:
            errors.append("{0} has invalid architecture_question_ids".format(prefix))
        taxonomy_question_coverage.update(linked_questions)
        term_questions[row.get("term_id", "")] = linked_questions
        for source_path in split_g01_multi_value(row.get("source_repo_paths", "")):
            candidate_path = repository_root / source_path
            if Path(source_path).is_absolute() or not is_path_beneath_root(
                candidate_path, repository_root
            ) or not is_regular_file_path(candidate_path):
                errors.append("{0} has invalid source_repo_path {1}".format(prefix, source_path))
    if any(count > 1 for count in normalized_terms.values()):
        errors.append("governance/keyword-taxonomy.tsv: term text must be unique")
    if taxonomy_question_coverage != question_ids:
        errors.append("governance/keyword-taxonomy.tsv: every question requires source terms")

    if len(query_rows) < len(question_ids) or len(query_rows) > G01_MAX_QUERIES:
        errors.append(
            "governance/query-ledger.tsv: expected {0}-{1} rows, found {2}".format(
                len(question_ids), G01_MAX_QUERIES, len(query_rows)
            )
        )
    expected_query_ids = [
        "QRY-{0:03d}".format(index) for index in range(1, len(query_rows) + 1)
    ]
    actual_query_ids = [row.get("query_id", "") for row in query_rows]
    if actual_query_ids != expected_query_ids:
        errors.append("governance/query-ledger.tsv: query IDs must be contiguous")

    query_fields = EXPECTED_TSV_HEADERS["governance/query-ledger.tsv"].split("\t")
    query_question_coverage: set[str] = set()
    normalized_query_texts: Counter[str] = Counter()
    for index, row in enumerate(query_rows, start=2):
        prefix = "governance/query-ledger.tsv: row {0}".format(index)
        for field_name in query_fields:
            if not row.get(field_name, "").strip():
                errors.append("{0} requires {1}".format(prefix, field_name))
        linked_questions = set(split_g01_multi_value(row.get("architecture_question_ids", "")))
        linked_terms = set(split_g01_multi_value(row.get("source_term_ids", "")))
        normalized_query_texts[row.get("query_text", "").strip().casefold()] += 1
        if not linked_questions or not linked_questions <= question_ids:
            errors.append("{0} has invalid architecture_question_ids".format(prefix))
        if len(linked_terms) < 2 or not linked_terms <= term_ids:
            errors.append("{0} requires at least two valid source_term_ids".format(prefix))
        linked_term_types = {
            term_rows_by_id.get(term_id, {}).get("term_type", "")
            for term_id in linked_terms
        }
        if "ALGORITHM" not in linked_term_types or not (
            linked_term_types - {"", "ALGORITHM"}
        ):
            errors.append(
                "{0} must combine an ALGORITHM term with a mechanism or resource term".format(
                    prefix
                )
            )
        for term_id in linked_terms:
            if term_questions.get(term_id, set()).isdisjoint(linked_questions):
                errors.append(
                    "{0} source term {1} is not linked to a query question".format(
                        prefix, term_id
                    )
                )
        query_question_coverage.update(linked_questions)
        if row.get("service") != "arXiv":
            errors.append("{0} service must be arXiv".format(prefix))
        if row.get("status") != "PLANNED":
            errors.append("{0} status must remain PLANNED in G01".format(prefix))
        for field_name in ("executed_at", "result_count", "response_checksum"):
            if row.get(field_name) != G01_NULL_SENTINEL:
                errors.append(
                    "{0} {1} must be {2}".format(
                        prefix, field_name, G01_NULL_SENTINEL
                    )
                )
        query_words = row.get("query_text", "").split()
        if len(query_words) < 4 or row.get("query_text", "").strip().casefold() == "graph":
            errors.append("{0} query_text is too generic".format(prefix))
        query_tokens = tokenize_g01_query_vocabulary(row.get("query_text", ""))
        matched_term_ids = {
            term_id
            for term_id in linked_terms
            if query_tokens
            & tokenize_g01_query_vocabulary(
                term_rows_by_id.get(term_id, {}).get("term", "")
            )
        }
        if len(matched_term_ids) < 2:
            errors.append(
                "{0} query_text must overlap at least two linked taxonomy terms".format(
                    prefix
                )
            )
    if query_question_coverage != question_ids:
        errors.append("governance/query-ledger.tsv: every question requires a planned query")
    if any(count > 1 for count in normalized_query_texts.values()):
        errors.append("governance/query-ledger.tsv: query_text must be unique")
    return sorted(set(errors))


def validate_g01_allowed_artifacts(root: Path) -> List[str]:
    """Reject all files outside the complete G00 plus G01 control surface."""

    errors: List[str] = []
    for relative_path in G01_REQUIRED_FILE_PATHS:
        required_path = root / relative_path
        if not is_regular_file_path(required_path):
            errors.append("{0}: required G01 file is missing".format(relative_path))

    try:
        corpus_paths = sorted(root.rglob("*"), key=lambda path: path.relative_to(root).as_posix())
    except OSError as error:
        return ["root: cannot inspect G01 corpus: {0}".format(error.strerror or error)]
    for corpus_path in corpus_paths:
        relative_path = corpus_path.relative_to(root)
        relative_path_text = relative_path.as_posix()
        if corpus_path.is_dir() and not corpus_path.is_symlink():
            continue
        if relative_path_text in G01_ALLOWED_FILE_PATHS and is_regular_file_path(corpus_path):
            continue
        if is_allowed_python_cache(root, relative_path):
            continue
        errors.append("{0}: file is not allowed while active goal is G01".format(relative_path_text))
    return sorted(set(errors))


def validate_g01_no_research_boundary(texts: Mapping[str, str]) -> List[str]:
    """Reject locators or records that would imply G02 literature activity."""

    errors: List[str] = []
    forbidden_patterns = (
        (re.compile(r"https?://", flags=re.IGNORECASE), "URL"),
        (re.compile(r"\barXiv:\d{4}\.\d{4,5}\b", flags=re.IGNORECASE), "arXiv identifier"),
        (re.compile(r"\b\d{4}\.\d{4,5}(?:v\d+)?\b"), "arXiv-like identifier"),
    )
    for display_path, text_value in sorted(texts.items()):
        for pattern, label in forbidden_patterns:
            if pattern.search(text_value):
                errors.append("{0}: G01 must not contain a {1}".format(display_path, label))
    return errors


def validate_g01_campaign_status(
    status_text: str,
    question_count: int,
    term_count: int,
    query_count: int,
) -> List[str]:
    """Validate G01 closure markers and exact discovery-only artifact counts."""

    display_path = "governance/campaign-status.md"
    errors: List[str] = []
    required_markers = (
        "- Active goal: `G01`",
        "- Goal state: `COMPLETE`",
        "- Completion state: `COMPLETE`",
        "- Validation state: `VERIFIED`",
        "- Journal: `arxiv-reference/journals/G01-progress.md`",
        "- Recommended next goal: `G02`",
        "- G02 state: `NOT_STARTED`",
    )
    for marker in required_markers:
        if marker not in status_text:
            errors.append("{0}: missing G01 marker {1}".format(display_path, marker))

    expected_count_rows = (
        ("Architecture questions", question_count),
        ("Taxonomy terms", term_count),
        ("Planned query families", query_count),
    )
    for label, expected_count in expected_count_rows:
        row_pattern = re.compile(
            r"^\|\s*{0}\s*\|\s*{1}\s*\|$".format(
                re.escape(label), expected_count
            ),
            flags=re.MULTILINE,
        )
        if not row_pattern.search(status_text):
            errors.append(
                "{0}: missing exact {1} count {2}".format(
                    display_path, label, expected_count
                )
            )

    zero_count_labels = (
        "External queries executed",
        "Canonical paper records",
        "Papers screened",
        "Papers read",
        "Full-text files downloaded",
        "Mechanism cards",
        "Failure cards",
        "Constraint-transfer cards",
        "Evidence conflicts",
        "Architecture genomes",
        "Architecture candidates",
        "Candidates changed",
        "Experiments created",
    )
    for label in zero_count_labels:
        row_pattern = re.compile(
            r"^\|\s*{0}\s*\|\s*0\s*\|$".format(re.escape(label)),
            flags=re.MULTILINE,
        )
        if not row_pattern.search(status_text):
            errors.append("{0}: {1} must remain zero in G01".format(display_path, label))
    return sorted(set(errors))


def validate_g02_allowed_artifacts(root: Path) -> List[str]:
    """Allow only G00-G02 controls, fixtures, and ignored G02 response caches."""

    errors: List[str] = []
    for relative_path in G02_REQUIRED_FILE_PATHS + G02_FIXTURE_FILE_PATHS:
        required_path = root / relative_path
        if not is_regular_file_path(required_path):
            errors.append("{0}: required G02 file is missing".format(relative_path))
    try:
        corpus_paths = sorted(root.rglob("*"), key=lambda path: path.relative_to(root).as_posix())
    except OSError as error:
        return ["root: cannot inspect G02 corpus: {0}".format(error.strerror or error)]
    for corpus_path in corpus_paths:
        relative_path = corpus_path.relative_to(root)
        relative_path_text = relative_path.as_posix()
        if corpus_path.is_dir() and not corpus_path.is_symlink():
            continue
        if relative_path_text in G02_ALLOWED_FILE_PATHS and is_regular_file_path(corpus_path):
            continue
        if relative_path.parts[:2] == ("cache", "g02") and is_regular_file_path(corpus_path):
            continue
        if is_allowed_python_cache(root, relative_path):
            continue
        errors.append("{0}: file is not allowed while active goal is G02".format(relative_path_text))
    return sorted(set(errors))


def validate_g02_query_rows(
    query_rows: Sequence[Mapping[str, str]],
    taxonomy_rows: Sequence[Mapping[str, str]],
    questions: Sequence[Mapping[str, str]],
    require_terminal: bool,
) -> List[str]:
    """Validate the exact G01 family set as a bounded G02 execution ledger."""

    errors: List[str] = []
    if len(query_rows) != 25:
        errors.append("governance/query-ledger.tsv: G02 requires exactly 25 query families")
    expected_ids = ["QRY-{0:03d}".format(index) for index in range(1, len(query_rows) + 1)]
    if [row.get("query_id", "") for row in query_rows] != expected_ids:
        errors.append("governance/query-ledger.tsv: G02 query IDs must remain contiguous")
    question_ids = {row.get("question_id", "") for row in questions}
    term_rows = {row.get("term_id", ""): row for row in taxonomy_rows}
    covered_questions: set[str] = set()
    for index, row in enumerate(query_rows, start=2):
        prefix = "governance/query-ledger.tsv: row {0}".format(index)
        linked_questions = set(split_g01_multi_value(row.get("architecture_question_ids", "")))
        linked_terms = set(split_g01_multi_value(row.get("source_term_ids", "")))
        if not linked_questions or not linked_questions <= question_ids:
            errors.append("{0} has invalid architecture_question_ids".format(prefix))
        if not linked_terms or not linked_terms <= set(term_rows):
            errors.append("{0} has invalid source_term_ids".format(prefix))
        linked_types = {term_rows[term_id].get("term_type", "") for term_id in linked_terms if term_id in term_rows}
        if "ALGORITHM" not in linked_types or not linked_types - {"ALGORITHM"}:
            errors.append("{0} must remain an algorithm-plus-mechanism family".format(prefix))
        covered_questions.update(linked_questions)
        status = row.get("status", "")
        if status not in ALLOWED_QUERY_STATUSES:
            errors.append("{0} has invalid status {1!r}".format(prefix, status))
        if require_terminal and status == "PLANNED":
            errors.append("{0} must have terminal G02 status".format(prefix))
        if status == "EXECUTED":
            for field_name in ("executed_at", "result_count", "response_checksum"):
                value = row.get(field_name, "")
                if value in ("", "NOT_EXECUTED"):
                    errors.append("{0} EXECUTED row requires {1}".format(prefix, field_name))
            if not row.get("result_count", "").isdigit():
                errors.append("{0} EXECUTED result_count must be an integer".format(prefix))
            if not re.fullmatch(r"[0-9a-f]{64}", row.get("response_checksum", "")):
                errors.append("{0} EXECUTED response_checksum must be SHA-256".format(prefix))
    if covered_questions != question_ids:
        errors.append("governance/query-ledger.tsv: G02 must retain coverage of all architecture questions")
    return sorted(set(errors))


def validate_g02_service_preflight(preflight_text: str) -> List[str]:
    """Validate explicit authorization and non-substitution service boundaries."""

    errors: List[str] = []
    required_markers = (
        "**Checked:** 2026-08-11",
        "arXiv decision: `AUTHORIZED`",
        "https://info.arxiv.org/help/api/tou.html",
        "https://info.arxiv.org/help/api/user-manual.html",
        "one request",
        "three seconds",
        "one connection",
        "Crossref decision: `NOT_USED_NOT_AUTHORIZED`",
        "OpenAlex decision: `NOT_USED_NOT_AUTHORIZED`",
        "downloads no PDF",
    )
    for marker in required_markers:
        if marker not in preflight_text:
            errors.append("governance/g02-service-preflight.md: missing {0}".format(marker))
    return errors


def validate_g02_campaign_status(
    status_text: str,
    query_rows: Sequence[Mapping[str, str]],
    request_rows: Sequence[Mapping[str, str]],
    manifest_rows: Sequence[Mapping[str, str]],
) -> List[str]:
    """Validate active or completed G02 lifecycle and exact visible counts."""

    errors: List[str] = []
    required_markers = (
        "- Active goal: `G02`",
        "- Journal: `arxiv-reference/journals/G02-progress.md`",
        "- G03 state: `NOT_STARTED`",
    )
    for marker in required_markers:
        if marker not in status_text:
            errors.append("governance/campaign-status.md: missing G02 marker {0}".format(marker))
    is_complete = "- Completion state: `COMPLETE`" in status_text
    if is_complete:
        if "- Validation state: `VERIFIED`" not in status_text:
            errors.append("governance/campaign-status.md: completed G02 must be VERIFIED")
        if any(row.get("status") == "PLANNED" for row in query_rows):
            errors.append("governance/campaign-status.md: completed G02 has planned queries")
        exact_counts = (
            ("Query families executed", sum(row.get("status") == "EXECUTED" for row in query_rows)),
            ("External HTTP requests", sum(row.get("cache_status") == "MISS" for row in request_rows)),
            ("Cache hits", sum(row.get("cache_status") == "HIT" for row in request_rows)),
            ("Raw metadata records", sum(int(row.get("result_count", "0")) for row in request_rows if row.get("terminal_state") == "COMPLETE")),
            ("Canonical paper records", len(manifest_rows)),
        )
        for label, expected_count in exact_counts:
            if not re.search(r"^\|\s*{0}\s*\|\s*{1}\s*\|$".format(re.escape(label), expected_count), status_text, flags=re.MULTILINE):
                errors.append("governance/campaign-status.md: missing exact {0} count {1}".format(label, expected_count))
    return sorted(set(errors))


def validate_g02_cache_git_boundary(root: Path) -> List[str]:
    """Reject tracked or staged ignored response bodies."""

    try:
        result = subprocess.run(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard", "--", "cache/g02"],
            cwd=str(root),
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=GIT_COMMAND_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return ["git: cannot inspect G02 cache boundary: {0}".format(error)]
    if result.returncode != 0:
        return []
    listed = result.stdout.decode("utf-8", errors="replace").splitlines()
    tracked = [path for path in listed if path.startswith("cache/g02/")]
    return ["{0}: G02 response cache must remain ignored".format(path) for path in tracked]


def validate_g03_allowed_artifacts(root: Path, require_report: bool) -> List[str]:
    """Allow only G00-G03 controls, fixtures, reports, and ignored caches."""

    errors: List[str] = []
    required_paths = list(G03_REQUIRED_FILE_PATHS) + list(G03_FIXTURE_FILE_PATHS)
    if require_report:
        required_paths.extend(G03_OPTIONAL_FILE_PATHS)
    for relative_path in required_paths:
        if not is_regular_file_path(root / relative_path):
            errors.append("{0}: required G03 file is missing".format(relative_path))
    try:
        corpus_paths = sorted(
            root.rglob("*"), key=lambda path: path.relative_to(root).as_posix()
        )
    except OSError as error:
        return ["root: cannot inspect G03 corpus: {0}".format(error.strerror or error)]
    for corpus_path in corpus_paths:
        relative_path = corpus_path.relative_to(root)
        relative_text = relative_path.as_posix()
        if corpus_path.is_dir() and not corpus_path.is_symlink():
            continue
        if relative_text in G03_ALLOWED_FILE_PATHS and is_regular_file_path(corpus_path):
            continue
        if relative_path.parts[:2] in (("cache", "g02"), ("cache", "g03")):
            if is_regular_file_path(corpus_path):
                continue
        if is_allowed_python_cache(root, relative_path):
            continue
        errors.append(
            "{0}: file is not allowed while active goal is G03".format(relative_text)
        )
    return sorted(set(errors))


def validate_g03_manifest_rows(
    rows: Sequence[Mapping[str, str]],
    valid_query_ids: set[str],
    valid_question_ids: set[str],
    seed_ids: Sequence[str],
    require_complete: bool,
) -> List[str]:
    """Validate the preserved G02 baseline plus bounded G03 ancestry rows."""

    errors: List[str] = []
    baseline_rows = [
        row for row in rows if row.get("discovery_query_ids") != "NOT_APPLICABLE"
    ]
    ancestry_rows = [
        row for row in rows if row.get("discovery_query_ids") == "NOT_APPLICABLE"
    ]
    if len(baseline_rows) != 262:
        errors.append(
            "sources/paper-manifest.tsv: G03 must preserve exactly 262 G02 identities"
        )
    if len(ancestry_rows) > 250:
        errors.append(
            "sources/paper-manifest.tsv: G03 new canonical identity cap 250 exceeded"
        )
    try:
        g02_pipeline = load_g02_pipeline_module()
        errors.extend(
            g02_pipeline.validate_metadata_manifest_rows(
                baseline_rows, valid_query_ids, valid_question_ids
            )
        )
    except (OSError, RuntimeError) as error:
        errors.append("cannot load preserved G02 manifest validator: {0}".format(error))

    score_pattern = re.compile(
        r"^ALG=(\d+);MECH=(\d+);ROLE=(\d+);AGE=(\d+);FALS=(\d+)$"
    )
    required_note_keys = {
        "ALIASES",
        "ANCESTRY_DIRECTIONS",
        "ANCESTRY_RESOLUTION",
        "ANCESTRY_SEEDS",
        "CITATION_DEPTH",
        "G03_SCREEN",
        "IDENTITY_STATE",
        "OPENALEX_ID",
        "SOURCE_URLS",
        "VERSIONS",
    }
    for index, row in enumerate(ancestry_rows, start=2 + len(baseline_rows)):
        prefix = "sources/paper-manifest.tsv: G03 row {0}".format(index)
        if row.get("selection_status") not in {"METADATA_ONLY", "UNAVAILABLE"}:
            errors.append("{0} must remain metadata-only".format(prefix))
        if row.get("local_path") != "NOT_ACQUIRED" or row.get("sha256") != "NOT_ACQUIRED":
            errors.append("{0} cannot acquire full text".format(prefix))
        if "SOURCE_CLAIM" in " ".join(str(value) for value in row.values()):
            errors.append("{0} cannot assert SOURCE_CLAIM".format(prefix))
        question_ids = set(split_g01_multi_value(row.get("architecture_question_ids", "")))
        if not question_ids or not question_ids <= valid_question_ids:
            errors.append("{0} has invalid architecture-question provenance".format(prefix))
        score_match = score_pattern.fullmatch(row.get("score_breakdown", ""))
        try:
            relevance_score = int(row.get("relevance_score", "-1"))
        except ValueError:
            relevance_score = -1
        if not score_match:
            errors.append("{0} has invalid G03 score breakdown".format(prefix))
        elif relevance_score != sum(int(value) for value in score_match.groups()):
            errors.append("{0} relevance score does not match G03 breakdown".format(prefix))
        notes = {
            clause.split("=", 1)[0]
            for clause in row.get("notes", "").split(";")
            if "=" in clause
        }
        if not required_note_keys <= notes:
            errors.append("{0} is missing G03 ancestry notes".format(prefix))

    if require_complete:
        by_id = {row.get("paper_id", ""): row for row in rows}
        if len(seed_ids) != 25 or len(set(seed_ids)) != 25:
            errors.append("sources/G02-metadata-screening-report.md: expected 25 seeds")
        for seed_id in seed_ids:
            seed_row = by_id.get(seed_id)
            if seed_row is None:
                errors.append("sources/paper-manifest.tsv: missing G03 seed " + seed_id)
                continue
            notes = seed_row.get("notes", "")
            if "G03_SEED=YES" not in notes or "CITATION_DEPTH=0" not in notes:
                errors.append(
                    "sources/paper-manifest.tsv: seed {0} lacks depth-0 provenance".format(
                        seed_id
                    )
                )
    return sorted(set(errors))


def validate_g03_report(
    report_text: str,
    seed_ids: Sequence[str],
    manifest_ids: set[str],
) -> List[str]:
    """Validate the exact metadata-only G03 decision handoff."""

    errors: List[str] = []
    required_headings = (
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
    )
    for heading in required_headings:
        if heading not in report_text:
            errors.append("sources/G03-citation-ancestry-report.md: missing " + heading)
    if "Papers read | 0 | 0" not in report_text:
        errors.append("sources/G03-citation-ancestry-report.md: papers read must be zero")
    if "Full-text/PDF files acquired | 0 | 0" not in report_text:
        errors.append("sources/G03-citation-ancestry-report.md: full text must be zero")
    acquisition_section = extract_markdown_section_text(
        report_text, "Exact Recommended G04 Acquisition Set"
    )
    acquisition_ids = re.findall(r"`(PAPER-[^`]+)`", acquisition_section)
    if len(acquisition_ids) < 25 or len(acquisition_ids) > 50:
        errors.append("sources/G03-citation-ancestry-report.md: G04 set must contain 25-50 identities")
    if len(acquisition_ids) != len(set(acquisition_ids)):
        errors.append("sources/G03-citation-ancestry-report.md: G04 set has duplicate identities")
    if not set(seed_ids) <= set(acquisition_ids):
        errors.append("sources/G03-citation-ancestry-report.md: G04 set omits a G03 seed")
    if not set(acquisition_ids) <= manifest_ids:
        errors.append("sources/G03-citation-ancestry-report.md: G04 set has unknown identity")
    declared_size = re.search(r"Exact G04 set size: \*\*(\d+)\*\*", report_text)
    if not declared_size or int(declared_size.group(1)) != len(acquisition_ids):
        errors.append("sources/G03-citation-ancestry-report.md: exact G04 size mismatch")
    return sorted(set(errors))


def validate_g03_campaign_status(
    status_text: str,
    request_rows: Sequence[Mapping[str, str]],
    manifest_rows: Sequence[Mapping[str, str]],
    edge_rows: Sequence[Mapping[str, str]],
) -> List[str]:
    """Validate active and completed G03 lifecycle markers and counts."""

    errors: List[str] = []
    required_markers = (
        "- Active goal: `G03`",
        "- G02 state: `COMPLETE_VERIFIED`",
        "- Journal: `arxiv-reference/journals/G03-progress.md`",
        "exactly 25 seeds",
        "citation depth 2",
        "250 new canonical identities",
        "90 HTTP attempts",
        "6,000 raw metadata observations",
    )
    for marker in required_markers:
        if marker not in status_text:
            errors.append("governance/campaign-status.md: missing G03 marker " + marker)
    is_complete = "- Completion state: `COMPLETE`" in status_text
    if is_complete:
        for marker in (
            "- Goal state: `COMPLETE`",
            "- Validation state: `VERIFIED`",
            "- Recommended next goal: `G04`",
        ):
            if marker not in status_text:
                errors.append("governance/campaign-status.md: missing G03 closure marker " + marker)
        counts = (
            ("External citation HTTP attempts", sum(row.get("cache_status") == "MISS" for row in request_rows)),
            ("Citation metadata observations", sum(int(row.get("result_count", "0")) for row in request_rows if row.get("terminal_state") in {"COMPLETE", "EMPTY"})),
            ("Final canonical paper records", len(manifest_rows)),
            ("New G03 canonical identities", max(0, len(manifest_rows) - 262)),
            ("Citation and semantic edges", len(edge_rows)),
        )
        for label, count in counts:
            if not re.search(
                r"^\|\s*{0}\s*\|\s*{1}\s*\|$".format(re.escape(label), count),
                status_text,
                flags=re.MULTILINE,
            ):
                errors.append(
                    "governance/campaign-status.md: missing exact {0} count {1}".format(
                        label, count
                    )
                )
    elif "- Completion state: `IN_PROGRESS`" not in status_text:
        errors.append("governance/campaign-status.md: G03 must be IN_PROGRESS or COMPLETE")
    return sorted(set(errors))


def validate_g03_cache_git_boundary(root: Path) -> List[str]:
    """Reject tracked or staged ignored G03 metadata response bodies."""

    try:
        result = subprocess.run(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard", "--", "cache/g03"],
            cwd=str(root),
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=GIT_COMMAND_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return ["git: cannot inspect G03 cache boundary: {0}".format(error)]
    if result.returncode != 0:
        return []
    tracked = [
        path
        for path in result.stdout.decode("utf-8", errors="replace").splitlines()
        if path.startswith("cache/g03/")
    ]
    return ["{0}: G03 response cache must remain ignored".format(path) for path in tracked]


def run_corpus_contract_checks(root: Path) -> List[str]:
    """Run the active G00, G01, or G02 corpus contract without side effects."""

    if not root.is_dir():
        return ["root: directory does not exist: {0}".format(root)]

    errors = validate_required_control_files(root)
    active_goal, active_goal_errors = read_active_goal_identifier(root)
    errors.extend(active_goal_errors)

    ignore_text = ""
    ignore_path = root / ".gitignore"
    if is_regular_file_path(ignore_path):
        ignore_text, read_errors = read_text_file_safely(ignore_path, ".gitignore")
        errors.extend(read_errors)
        if not read_errors:
            errors.extend(validate_ignore_policy_rules(ignore_text))

    journal_path = root / "journals/G00-progress.md"
    if is_regular_file_path(journal_path):
        journal_text, read_errors = read_text_file_safely(
            journal_path, "journals/G00-progress.md"
        )
        errors.extend(read_errors)
        if not read_errors:
            errors.extend(validate_goal_journal_shape(journal_text))

    goal_packet_path = root / "governance/G00-goal-packet.md"
    if is_regular_file_path(goal_packet_path):
        goal_packet_text, read_errors = read_text_file_safely(
            goal_packet_path, "governance/G00-goal-packet.md"
        )
        errors.extend(read_errors)
        if not read_errors:
            errors.extend(
                validate_goal_packet_shape(
                    goal_packet_text, "governance/G00-goal-packet.md"
                )
            )

    generation_ledger_path = root / "governance/G00-generation-ledger.md"
    if is_regular_file_path(generation_ledger_path):
        generation_ledger_text, read_errors = read_text_file_safely(
            generation_ledger_path, "governance/G00-generation-ledger.md"
        )
        errors.extend(read_errors)
        if not read_errors:
            errors.extend(
                validate_generation_ledger_shape(
                    generation_ledger_text,
                    root,
                    verify_current_checksums=active_goal == "G00",
                )
            )

    schema_path = root / "governance/artifact-schema-contracts.md"
    if is_regular_file_path(schema_path):
        schema_text, read_errors = read_text_file_safely(
            schema_path, "governance/artifact-schema-contracts.md"
        )
        errors.extend(read_errors)
        if not read_errors:
            errors.extend(validate_artifact_schema_contract(schema_text))

    sop_path = root / "Arxiv-Pattern-Foundry-SOP.md"
    if is_regular_file_path(sop_path):
        sop_text, read_errors = read_text_file_safely(sop_path, "Arxiv-Pattern-Foundry-SOP.md")
        errors.extend(read_errors)
        if not read_errors:
            errors.extend(audit_requirement_test_links(sop_text))

    errors.extend(validate_optional_tsv_files(root, ignore_text))
    if active_goal == "G00":
        errors.extend(validate_g00_empty_artifacts(root))
    elif active_goal == "G01":
        g01_texts: Dict[str, str] = {}

        g01_journal_path = root / "journals/G01-progress.md"
        if is_regular_file_path(g01_journal_path):
            g01_journal_text, read_errors = read_text_file_safely(
                g01_journal_path, "journals/G01-progress.md"
            )
            errors.extend(read_errors)
            if not read_errors:
                g01_texts["journals/G01-progress.md"] = g01_journal_text
                errors.extend(validate_goal_journal_shape(g01_journal_text, "G01"))

        question_path = root / "governance/architecture-question-ledger.md"
        questions: List[Dict[str, str]] = []
        if is_regular_file_path(question_path):
            question_text, read_errors = read_text_file_safely(
                question_path, "governance/architecture-question-ledger.md"
            )
            errors.extend(read_errors)
            if not read_errors:
                g01_texts["governance/architecture-question-ledger.md"] = question_text
                question_errors, questions = validate_g01_question_ledger(
                    question_text, root.parent
                )
                errors.extend(question_errors)

        taxonomy_path = root / "governance/keyword-taxonomy.tsv"
        taxonomy_rows: List[Dict[str, str]] = []
        if is_regular_file_path(taxonomy_path):
            taxonomy_text, read_errors = read_text_file_safely(
                taxonomy_path, "governance/keyword-taxonomy.tsv"
            )
            errors.extend(read_errors)
            if not read_errors:
                g01_texts["governance/keyword-taxonomy.tsv"] = taxonomy_text
                taxonomy_rows, row_errors = read_tsv_file_rows(
                    taxonomy_path, "governance/keyword-taxonomy.tsv"
                )
                errors.extend(row_errors)

        query_path = root / "governance/query-ledger.tsv"
        query_rows: List[Dict[str, str]] = []
        if is_regular_file_path(query_path):
            query_text, read_errors = read_text_file_safely(
                query_path, "governance/query-ledger.tsv"
            )
            errors.extend(read_errors)
            if not read_errors:
                g01_texts["governance/query-ledger.tsv"] = query_text
                query_rows, row_errors = read_tsv_file_rows(
                    query_path, "governance/query-ledger.tsv"
                )
                errors.extend(row_errors)

        errors.extend(
            validate_g01_discovery_rows(
                taxonomy_rows, query_rows, questions, root.parent
            )
        )

        status_path = root / "governance/campaign-status.md"
        if is_regular_file_path(status_path):
            status_text, read_errors = read_text_file_safely(
                status_path, "governance/campaign-status.md"
            )
            errors.extend(read_errors)
            if not read_errors:
                g01_texts["governance/campaign-status.md"] = status_text
                errors.extend(
                    validate_g01_campaign_status(
                        status_text,
                        len(questions),
                        len(taxonomy_rows),
                        len(query_rows),
                    )
                )

        errors.extend(validate_g01_no_research_boundary(g01_texts))
        errors.extend(validate_g01_allowed_artifacts(root))
    elif active_goal == "G02":
        g02_journal_path = root / "journals/G02-progress.md"
        if is_regular_file_path(g02_journal_path):
            journal_text, read_errors = read_text_file_safely(
                g02_journal_path, "journals/G02-progress.md"
            )
            errors.extend(read_errors)
            if not read_errors:
                errors.extend(validate_goal_journal_shape(journal_text, "G02"))

        question_path = root / "governance/architecture-question-ledger.md"
        questions: List[Dict[str, str]] = []
        if is_regular_file_path(question_path):
            question_text, read_errors = read_text_file_safely(
                question_path, "governance/architecture-question-ledger.md"
            )
            errors.extend(read_errors)
            if not read_errors:
                question_errors, questions = validate_g01_question_ledger(
                    question_text, root.parent
                )
                errors.extend(question_errors)

        taxonomy_path = root / "governance/keyword-taxonomy.tsv"
        taxonomy_rows: List[Dict[str, str]] = []
        if is_regular_file_path(taxonomy_path):
            taxonomy_rows, row_errors = read_tsv_file_rows(
                taxonomy_path, "governance/keyword-taxonomy.tsv"
            )
            errors.extend(row_errors)

        query_path = root / "governance/query-ledger.tsv"
        query_rows: List[Dict[str, str]] = []
        if is_regular_file_path(query_path):
            query_rows, row_errors = read_tsv_file_rows(
                query_path, "governance/query-ledger.tsv"
            )
            errors.extend(row_errors)

        request_path = root / "sources/metadata-request-ledger.tsv"
        request_rows: List[Dict[str, str]] = []
        if is_regular_file_path(request_path):
            request_rows, row_errors = read_tsv_file_rows(
                request_path, "sources/metadata-request-ledger.tsv"
            )
            errors.extend(row_errors)

        manifest_path = root / "sources/paper-manifest.tsv"
        manifest_rows: List[Dict[str, str]] = []
        if is_regular_file_path(manifest_path):
            manifest_rows, row_errors = read_tsv_file_rows(
                manifest_path, "sources/paper-manifest.tsv"
            )
            errors.extend(row_errors)

        status_text = ""
        status_path = root / "governance/campaign-status.md"
        if is_regular_file_path(status_path):
            status_text, read_errors = read_text_file_safely(
                status_path, "governance/campaign-status.md"
            )
            errors.extend(read_errors)
        require_terminal = "- Completion state: `COMPLETE`" in status_text
        errors.extend(
            validate_g02_query_rows(
                query_rows, taxonomy_rows, questions, require_terminal
            )
        )

        preflight_path = root / "governance/g02-service-preflight.md"
        if is_regular_file_path(preflight_path):
            preflight_text, read_errors = read_text_file_safely(
                preflight_path, "governance/g02-service-preflight.md"
            )
            errors.extend(read_errors)
            if not read_errors:
                errors.extend(validate_g02_service_preflight(preflight_text))

        try:
            g02_pipeline = load_g02_pipeline_module()
            errors.extend(g02_pipeline.validate_request_provenance_rows(request_rows))
            errors.extend(
                g02_pipeline.validate_metadata_manifest_rows(
                    manifest_rows,
                    {row.get("query_id", "") for row in query_rows},
                    {row.get("question_id", "") for row in questions},
                )
            )
            errors.extend(
                g02_pipeline.validate_cached_response_provenance(
                    root, request_rows
                )
            )
            errors.extend(
                g02_pipeline.validate_query_aggregate_provenance(
                    query_rows, request_rows
                )
            )
        except (OSError, RuntimeError) as error:
            errors.append("cannot load G02 metadata validators: {0}".format(error))

        errors.extend(
            validate_g02_campaign_status(
                status_text, query_rows, request_rows, manifest_rows
            )
        )
        errors.extend(validate_g02_allowed_artifacts(root))
        errors.extend(validate_g02_cache_git_boundary(root))
    elif active_goal == "G03":
        status_text = ""
        status_path = root / "governance/campaign-status.md"
        if is_regular_file_path(status_path):
            status_text, read_errors = read_text_file_safely(
                status_path, "governance/campaign-status.md"
            )
            errors.extend(read_errors)
        require_complete = "- Completion state: `COMPLETE`" in status_text

        g03_journal_path = root / "journals/G03-progress.md"
        if is_regular_file_path(g03_journal_path):
            journal_text, read_errors = read_text_file_safely(
                g03_journal_path, "journals/G03-progress.md"
            )
            errors.extend(read_errors)
            if not read_errors:
                errors.extend(validate_goal_journal_shape(journal_text, "G03"))

        g03_packet_path = root / "governance/G03-goal-packet.md"
        if is_regular_file_path(g03_packet_path):
            packet_text, read_errors = read_text_file_safely(
                g03_packet_path, "governance/G03-goal-packet.md"
            )
            errors.extend(read_errors)
            if not read_errors:
                errors.extend(
                    validate_goal_packet_shape(
                        packet_text,
                        "governance/G03-goal-packet.md",
                        expected_goal="G03",
                    )
                )

        questions: List[Dict[str, str]] = []
        question_path = root / "governance/architecture-question-ledger.md"
        if is_regular_file_path(question_path):
            question_text, read_errors = read_text_file_safely(
                question_path, "governance/architecture-question-ledger.md"
            )
            errors.extend(read_errors)
            if not read_errors:
                question_errors, questions = validate_g01_question_ledger(
                    question_text, root.parent
                )
                errors.extend(question_errors)

        taxonomy_rows: List[Dict[str, str]] = []
        taxonomy_path = root / "governance/keyword-taxonomy.tsv"
        if is_regular_file_path(taxonomy_path):
            taxonomy_rows, row_errors = read_tsv_file_rows(
                taxonomy_path, "governance/keyword-taxonomy.tsv"
            )
            errors.extend(row_errors)

        query_rows: List[Dict[str, str]] = []
        query_path = root / "governance/query-ledger.tsv"
        if is_regular_file_path(query_path):
            query_rows, row_errors = read_tsv_file_rows(
                query_path, "governance/query-ledger.tsv"
            )
            errors.extend(row_errors)
        errors.extend(validate_g02_query_rows(query_rows, taxonomy_rows, questions, True))

        metadata_request_rows: List[Dict[str, str]] = []
        metadata_request_path = root / "sources/metadata-request-ledger.tsv"
        if is_regular_file_path(metadata_request_path):
            metadata_request_rows, row_errors = read_tsv_file_rows(
                metadata_request_path, "sources/metadata-request-ledger.tsv"
            )
            errors.extend(row_errors)

        manifest_rows: List[Dict[str, str]] = []
        manifest_path = root / "sources/paper-manifest.tsv"
        if is_regular_file_path(manifest_path):
            manifest_rows, row_errors = read_tsv_file_rows(
                manifest_path, "sources/paper-manifest.tsv"
            )
            errors.extend(row_errors)

        citation_request_rows: List[Dict[str, str]] = []
        citation_request_path = root / "sources/citation-request-ledger.tsv"
        if is_regular_file_path(citation_request_path):
            citation_request_rows, row_errors = read_tsv_file_rows(
                citation_request_path, "sources/citation-request-ledger.tsv"
            )
            errors.extend(row_errors)

        edge_rows: List[Dict[str, str]] = []
        edge_path = root / "sources/citation-edges.tsv"
        if is_regular_file_path(edge_path):
            edge_rows, row_errors = read_tsv_file_rows(
                edge_path, "sources/citation-edges.tsv"
            )
            errors.extend(row_errors)

        seed_ids: List[str] = []
        screening_report_path = root / "sources/G02-metadata-screening-report.md"
        try:
            g02_pipeline = load_g02_pipeline_module()
            errors.extend(g02_pipeline.validate_request_provenance_rows(metadata_request_rows))
            errors.extend(g02_pipeline.validate_cached_response_provenance(root, metadata_request_rows))
            errors.extend(g02_pipeline.validate_query_aggregate_provenance(query_rows, metadata_request_rows))
        except (OSError, RuntimeError) as error:
            errors.append("cannot load preserved G02 provenance validators: {0}".format(error))

        try:
            g03_pipeline = load_g03_pipeline_module()
            if is_regular_file_path(screening_report_path):
                screening_text, read_errors = read_text_file_safely(
                    screening_report_path, "sources/G02-metadata-screening-report.md"
                )
                errors.extend(read_errors)
                if not read_errors:
                    seed_ids = g03_pipeline.extract_g03_seed_ids(screening_text)
            preflight_path = root / "governance/g03-service-preflight.md"
            if is_regular_file_path(preflight_path):
                preflight_text, read_errors = read_text_file_safely(
                    preflight_path, "governance/g03-service-preflight.md"
                )
                errors.extend(read_errors)
                if not read_errors:
                    errors.extend(g03_pipeline.validate_g03_network_preflight(preflight_text))
            errors.extend(g03_pipeline.validate_citation_request_rows(citation_request_rows))
            errors.extend(g03_pipeline.validate_g03_cache_provenance(root, citation_request_rows))
            errors.extend(
                g03_pipeline.validate_citation_edge_contract(
                    edge_rows, {row.get("paper_id", "") for row in manifest_rows}
                )
            )
            errors.extend(
                g03_pipeline.validate_edge_cache_provenance(
                    root, citation_request_rows, edge_rows, manifest_rows
                )
            )
        except (OSError, RuntimeError, ValueError) as error:
            errors.append("cannot load G03 citation validators: {0}".format(error))

        errors.extend(
            validate_g03_manifest_rows(
                manifest_rows,
                {row.get("query_id", "") for row in query_rows},
                {row.get("question_id", "") for row in questions},
                seed_ids,
                require_complete,
            )
        )
        if require_complete:
            report_path = root / "sources/G03-citation-ancestry-report.md"
            if is_regular_file_path(report_path):
                report_text, read_errors = read_text_file_safely(
                    report_path, "sources/G03-citation-ancestry-report.md"
                )
                errors.extend(read_errors)
                if not read_errors:
                    errors.extend(
                        validate_g03_report(
                            report_text,
                            seed_ids,
                            {row.get("paper_id", "") for row in manifest_rows},
                        )
                    )
        errors.extend(
            validate_g03_campaign_status(
                status_text, citation_request_rows, manifest_rows, edge_rows
            )
        )
        errors.extend(validate_g03_allowed_artifacts(root, require_complete))
        errors.extend(validate_g02_cache_git_boundary(root))
        errors.extend(validate_g03_cache_git_boundary(root))
    elif active_goal is not None:
        errors.append(
            "governance/campaign-status.md: active goal {0} is not supported by "
            "this validator".format(active_goal)
        )
    errors.extend(validate_git_tracked_pdfs(root))
    return sorted(set(errors))


def build_validator_argument_parser() -> argparse.ArgumentParser:
    """Build the dependency-free corpus validator CLI parser."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path, help="path to arxiv-reference")
    return parser


def print_contract_check_results(errors: Sequence[str]) -> int:
    """Print deterministic PASS or FAIL diagnostics and return an exit code."""

    if errors:
        print("FAIL arxiv corpus contract")
        for error in sorted(set(errors)):
            print("FAIL " + error)
        return 1

    print("PASS arxiv corpus contract")
    return 0


def run_corpus_validator_cli() -> int:
    """Parse CLI arguments and validate one corpus root."""

    arguments = build_validator_argument_parser().parse_args()
    errors = run_corpus_contract_checks(arguments.root)
    return print_contract_check_results(errors)


if __name__ == "__main__":
    raise SystemExit(run_corpus_validator_cli())
