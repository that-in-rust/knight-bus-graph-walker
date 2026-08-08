#!/usr/bin/env python3
"""Validate the algorithm-by-algorithm low-RAM architecture option atlas."""

from __future__ import annotations

import csv
from pathlib import Path
import re
import sys


OPTION_HEADING_PATTERN = re.compile(
    r"^### (ARCH-([A-Z0-9]+)-(\d{3})):\s+(.+)$",
    re.MULTILINE,
)

FIELD_PATTERN = re.compile(r"^\*\*([^*]+):\*\*\s*(.+)$", re.MULTILINE)
UPSTREAM_EVIDENCE_PATTERN = re.compile(r"\bA0[123]-\d{6}\b")
DOCUMENT_EVIDENCE_PATTERN = re.compile(r"\bA0[456]-\d{6}\b")
DOCUMENT_LANE_PATTERN = re.compile(r"\b(A0[456])-\d{6}\b")
WORKING_SET_SYMBOL_PATTERN = re.compile(
    r"\b(?:n|m|m_u|p|t|d|k|f|c|a|B_ram|B_os|B_io|B_out|B_tmp)\b"
)

REQUIRED_FIELDS = {
    "Algorithms",
    "Plan classes",
    "Decision",
    "Storage layout",
    "Working-set model",
    "Latency/I/O tradeoff",
    "Predictability mechanism",
    "Applicability",
    "Refusal condition",
    "Verification",
    "Evidence",
}

REQUIRED_FAMILIES = {
    "PATH",
    "WCC",
    "PAGERANK",
    "NODESIM",
    "KNN",
    "LOUVAIN",
    "LEIDEN",
    "TRIANGLE",
    "FASTRP",
}

ALLOWED_DECISIONS = {"choose", "experiment", "reject", "defer"}
ALLOWED_PLAN_CLASSES = {"fit", "spill", "approximate", "refuse", "hybrid"}


class ArchitectureAtlasError(RuntimeError):
    """Raised when the architecture atlas lacks executable decision evidence."""


def extract_architecture_option_blocks(atlas: str) -> dict[str, tuple[str, dict[str, str]]]:
    matches = list(OPTION_HEADING_PATTERN.finditer(atlas))
    if not matches:
        raise ArchitectureAtlasError("no ARCH option headings found")
    options: dict[str, tuple[str, dict[str, str]]] = {}
    for index, match in enumerate(matches):
        option_id = match.group(1)
        if option_id in options:
            raise ArchitectureAtlasError(f"duplicate architecture option ID: {option_id}")
        block_end = matches[index + 1].start() if index + 1 < len(matches) else len(atlas)
        fields = {
            field_match.group(1).strip(): field_match.group(2).strip()
            for field_match in FIELD_PATTERN.finditer(atlas[match.end():block_end])
        }
        options[option_id] = (match.group(4).strip(), fields)
    return options


def validate_option_field_contracts(
    options: dict[str, tuple[str, dict[str, str]]],
) -> None:
    failures: list[str] = []
    for option_id, (title, fields) in options.items():
        option_family = option_id.split("-")[1]
        if option_family not in REQUIRED_FAMILIES:
            failures.append(f"{option_id} has unknown family {option_family!r}")
        missing_fields = sorted(REQUIRED_FIELDS - set(fields))
        if missing_fields:
            failures.append(f"{option_id} missing fields {missing_fields}")
            continue
        decision = fields["Decision"].lower()
        if decision not in ALLOWED_DECISIONS:
            failures.append(f"{option_id} illegal decision {decision!r}")
        plan_classes = {
            item.strip().lower() for item in fields["Plan classes"].split(",") if item.strip()
        }
        illegal_plan_classes = sorted(plan_classes - ALLOWED_PLAN_CLASSES)
        if illegal_plan_classes or not plan_classes:
            failures.append(f"{option_id} illegal plan classes {illegal_plan_classes}")
        algorithms = {
            item.strip().upper() for item in fields["Algorithms"].split(",") if item.strip()
        }
        if not algorithms:
            failures.append(f"{option_id} has no algorithm families")
        elif option_family not in algorithms:
            failures.append(
                f"{option_id} heading family is absent from Algorithms field {sorted(algorithms)}"
            )
        option_text = " ".join([title, *fields.values()])
        if re.search(r"\b(?:TODO|TBD|PLACEHOLDER|FIXME)\b", option_text, re.IGNORECASE):
            failures.append(f"{option_id} contains a placeholder")
        if decision in {"choose", "experiment"}:
            if not UPSTREAM_EVIDENCE_PATTERN.search(fields["Evidence"]):
                failures.append(f"{option_id} lacks upstream code evidence")
            if not DOCUMENT_EVIDENCE_PATTERN.search(fields["Evidence"]):
                failures.append(f"{option_id} lacks PRD03-PRD06 document evidence")
            if len(fields["Working-set model"]) < 20:
                failures.append(f"{option_id} working-set model is too weak")
            elif "=" not in fields["Working-set model"]:
                failures.append(f"{option_id} working-set model lacks an equation")
            elif not WORKING_SET_SYMBOL_PATTERN.search(fields["Working-set model"]):
                failures.append(f"{option_id} working-set model lacks shared symbols")
            if len(fields["Refusal condition"]) < 20:
                failures.append(f"{option_id} refusal condition is too weak")
            if len(fields["Verification"]) < 20:
                failures.append(f"{option_id} verification is too weak")
    if failures:
        raise ArchitectureAtlasError("option contract failures:\n- " + "\n- ".join(failures))


def validate_family_option_coverage(
    options: dict[str, tuple[str, dict[str, str]]],
) -> None:
    family_options: dict[str, list[dict[str, str]]] = {
        family: [] for family in REQUIRED_FAMILIES
    }
    for _, (_, fields) in options.items():
        algorithms = {
            item.strip().upper() for item in fields.get("Algorithms", "").split(",") if item.strip()
        }
        for family in algorithms & REQUIRED_FAMILIES:
            family_options[family].append(fields)

    failures: list[str] = []
    for family, option_fields in sorted(family_options.items()):
        if len(option_fields) < 3:
            failures.append(f"{family} has {len(option_fields)} options; requires at least 3")
            continue
        retained_fields = [
            fields for fields in option_fields if fields.get("Decision", "").lower() in {"choose", "experiment"}
        ]
        chosen_fields = [
            fields for fields in option_fields if fields.get("Decision", "").lower() == "choose"
        ]
        if not chosen_fields:
            failures.append(f"{family} lacks a chosen default or bounded profile")
        retained_classes = {
            plan_class.strip().lower()
            for fields in retained_fields
            for plan_class in fields.get("Plan classes", "").split(",")
            if plan_class.strip()
        }
        if not ({"fit", "hybrid"} & retained_classes):
            failures.append(f"{family} lacks a retained fit-capable option")
        if not ({"spill", "hybrid"} & retained_classes):
            failures.append(f"{family} lacks a retained spill-capable option")
    if failures:
        raise ArchitectureAtlasError("family coverage failures:\n- " + "\n- ".join(failures))


def load_all_evidence_identifiers(workspace_root: Path) -> tuple[set[str], set[str]]:
    upstream_root = (
        workspace_root
        / "docs_PRD04"
        / "reference-learning"
        / "neo4j-compat-lowram"
        / "evidence"
    )
    document_root = (
        workspace_root
        / "docs_PRD04"
        / "reference-learning"
        / "lowram-architecture-corpus"
        / "evidence"
    )
    upstream_identifiers: set[str] = set()
    document_identifiers: set[str] = set()
    for agent_number in (1, 2, 3):
        path = upstream_root / f"agent-{agent_number:02d}-files.tsv"
        with path.open("r", encoding="utf-8", newline="") as source_file:
            upstream_identifiers.update(
                row["evidence_id"] for row in csv.DictReader(source_file, delimiter="\t")
            )
    document_files = (
        "agent-04-prd03-files.tsv",
        "agent-05-prd04-files.tsv",
        "agent-06-prd05-prd06-files.tsv",
    )
    for filename in document_files:
        path = document_root / filename
        if not path.is_file():
            raise ArchitectureAtlasError(f"missing document evidence ledger: {path}")
        with path.open("r", encoding="utf-8", newline="") as source_file:
            document_identifiers.update(
                row["evidence_id"] for row in csv.DictReader(source_file, delimiter="\t")
            )
    return upstream_identifiers, document_identifiers


def validate_option_evidence_ids(
    atlas: str,
    workspace_root: Path,
) -> tuple[int, int]:
    cited_upstream = set(UPSTREAM_EVIDENCE_PATTERN.findall(atlas))
    cited_documents = set(DOCUMENT_EVIDENCE_PATTERN.findall(atlas))
    known_upstream, known_documents = load_all_evidence_identifiers(workspace_root)
    missing_upstream = sorted(cited_upstream - known_upstream)
    missing_documents = sorted(cited_documents - known_documents)
    if missing_upstream or missing_documents:
        raise ArchitectureAtlasError(
            f"unknown evidence IDs; upstream={missing_upstream}, documents={missing_documents}"
        )
    return len(cited_upstream), len(cited_documents)


def validate_atlas_document_spine(atlas: str) -> None:
    failures: list[str] = []
    for required_text in (
        "A007-spc-founder-interview-prep-v7.md",
        "fit",
        "spill",
        "approximate",
        "refuse",
    ):
        if required_text not in atlas:
            failures.append(f"atlas does not mention binding term {required_text!r}")
    cited_lanes = set(DOCUMENT_LANE_PATTERN.findall(atlas))
    missing_lanes = {"A04", "A05", "A06"} - cited_lanes
    if missing_lanes:
        failures.append(f"atlas lacks citations from document lanes {sorted(missing_lanes)}")
    if failures:
        raise ArchitectureAtlasError("atlas spine failures:\n- " + "\n- ".join(failures))


def run_architecture_atlas_validation() -> int:
    workspace_root = Path(__file__).resolve().parents[1]
    atlas_path = (
        workspace_root
        / "docs_PRD04"
        / "reference-learning"
        / "lowram-architecture-corpus"
        / "LowRAM-Algorithm-Architecture-Decision-Atlas.md"
    )
    if not atlas_path.is_file():
        raise ArchitectureAtlasError(f"missing architecture atlas: {atlas_path}")
    atlas = atlas_path.read_text(encoding="utf-8")
    options = extract_architecture_option_blocks(atlas)
    validate_option_field_contracts(options)
    validate_family_option_coverage(options)
    validate_atlas_document_spine(atlas)
    upstream_count, document_count = validate_option_evidence_ids(atlas, workspace_root)
    print(
        f"PASS: {len(options)} architecture options, {len(REQUIRED_FAMILIES)} families, "
        f"{upstream_count} upstream citations, {document_count} document citations"
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(run_architecture_atlas_validation())
    except ArchitectureAtlasError as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
