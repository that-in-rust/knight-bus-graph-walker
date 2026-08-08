#!/usr/bin/env python3
"""Lint the founder-gated mega spec as an executable requirements artifact."""

from __future__ import annotations

import csv
from pathlib import Path
import re
import sys


REQUIREMENT_HEADING_PATTERN = re.compile(
    r"^### (REQ-([A-Z]+)-(\d{3})\.(\d+)):\s+(.+)$",
    re.MULTILINE,
)

TEST_ROW_PATTERN = re.compile(
    r"^\| (TEST-[A-Z]+-\d{3}) \| ([^|]+) \|",
    re.MULTILINE,
)

REFERENCE_PATTERN = re.compile(r"([A-Z]+)-(\d{3})(?:\.\.(\d{3}))?")
EVIDENCE_ID_PATTERN = re.compile(r"\bA0[123]-\d{6}\b")
LOCAL_EVIDENCE_ID_PATTERN = re.compile(r"\b(?:CG-MAIN|KW-CURRENT)-\d{3}\b")

REQUIRED_SECTION_HEADINGS = [
    "# 1. Executable Requirements",
    "# 2. Test Matrix",
    "# 3. TDD Plan",
    "# 4. Quality Gates",
    "# 5. Open Questions",
]


class SpecValidationError(RuntimeError):
    """Raised when the mega spec violates its executable contract."""


def validate_section_order_now(specification: str) -> None:
    positions: list[int] = []
    for heading in REQUIRED_SECTION_HEADINGS:
        position = specification.find(heading)
        if position < 0:
            raise SpecValidationError(f"missing required section: {heading}")
        positions.append(position)
    if positions != sorted(positions) or len(set(positions)) != len(positions):
        raise SpecValidationError("required sections are not in executable-spec order")


def extract_requirement_blocks_now(specification: str) -> dict[str, str]:
    matches = list(REQUIREMENT_HEADING_PATTERN.finditer(specification))
    if not matches:
        raise SpecValidationError("no executable requirements found")
    blocks: dict[str, str] = {}
    for index, match in enumerate(matches):
        requirement_id = match.group(1)
        if requirement_id in blocks:
            raise SpecValidationError(f"duplicate requirement ID: {requirement_id}")
        block_end = matches[index + 1].start() if index + 1 < len(matches) else specification.find(
            "# 2. Test Matrix",
            match.end(),
        )
        if block_end < 0:
            raise SpecValidationError(f"cannot find end of requirement block: {requirement_id}")
        blocks[requirement_id] = specification[match.end():block_end]
    return blocks


def validate_requirement_contracts_now(requirement_blocks: dict[str, str]) -> None:
    failures: list[str] = []
    negative_contract_pattern = re.compile(
        r"(?:SHALL\s+NOT|\b(?:fail|refus|reject|unsupported|kill|defer|cancel|"
        r"terminat|absent|unknown|unavailable|invalid|corrupt|missing|overflow|"
        r"exceed|mismatch|violation|incompatible|without|zero|only|block)\w*\b)",
        re.IGNORECASE,
    )
    for requirement_id, block in requirement_blocks.items():
        missing_terms = [term for term in ("**WHEN**", "**THEN**", "SHALL") if term not in block]
        if missing_terms:
            failures.append(f"{requirement_id} missing {','.join(missing_terms)}")
        if not negative_contract_pattern.search(block):
            failures.append(f"{requirement_id} lacks observable failure/refusal behavior")
    if failures:
        raise SpecValidationError("requirement contract failures:\n- " + "\n- ".join(failures))


def expand_requirement_references_now(reference_text: str) -> set[str]:
    expanded: set[str] = set()
    for match in REFERENCE_PATTERN.finditer(reference_text):
        domain = match.group(1)
        start = int(match.group(2))
        end = int(match.group(3)) if match.group(3) else start
        if end < start:
            raise SpecValidationError(f"descending requirement range: {match.group(0)}")
        for number in range(start, end + 1):
            expanded.add(f"REQ-{domain}-{number:03d}")
    return expanded


def extract_test_mappings_now(specification: str) -> dict[str, set[str]]:
    mappings: dict[str, set[str]] = {}
    for match in TEST_ROW_PATTERN.finditer(specification):
        test_id = match.group(1)
        if test_id in mappings:
            raise SpecValidationError(f"duplicate test ID: {test_id}")
        references = expand_requirement_references_now(match.group(2))
        if not references:
            raise SpecValidationError(f"{test_id} has no parseable requirement references")
        mappings[test_id] = references
    if not mappings:
        raise SpecValidationError("no test matrix mappings found")
    return mappings


def validate_test_coverage_now(
    requirement_blocks: dict[str, str],
    test_mappings: dict[str, set[str]],
) -> None:
    actual_requirements = {requirement_id.rsplit(".", 1)[0] for requirement_id in requirement_blocks}
    mapped_requirements = set().union(*test_mappings.values())
    missing_mappings = sorted(actual_requirements - mapped_requirements)
    unknown_mappings = sorted(mapped_requirements - actual_requirements)
    if missing_mappings or unknown_mappings:
        raise SpecValidationError(
            f"test mapping mismatch; missing={missing_mappings}, unknown={unknown_mappings}"
        )


def load_evidence_identifiers_now(workspace_root: Path) -> set[str]:
    evidence_root = (
        workspace_root
        / "docs_PRD04"
        / "reference-learning"
        / "neo4j-compat-lowram"
        / "evidence"
    )
    identifiers: set[str] = set()
    for agent_number in (1, 2, 3):
        path = evidence_root / f"agent-{agent_number:02d}-files.tsv"
        if not path.is_file():
            raise SpecValidationError(f"missing evidence ledger: {path}")
        with path.open("r", encoding="utf-8", newline="") as source_file:
            for row in csv.DictReader(source_file, delimiter="\t"):
                evidence_id = row.get("evidence_id", "")
                if evidence_id in identifiers:
                    raise SpecValidationError(f"duplicate reconciled evidence ID: {evidence_id}")
                identifiers.add(evidence_id)
    return identifiers


def validate_evidence_references_now(specification: str, workspace_root: Path) -> int:
    cited_identifiers = set(EVIDENCE_ID_PATTERN.findall(specification))
    if not cited_identifiers:
        raise SpecValidationError("mega spec contains no file-level evidence citations")
    known_identifiers = load_evidence_identifiers_now(workspace_root)
    missing_identifiers = sorted(cited_identifiers - known_identifiers)
    if missing_identifiers:
        raise SpecValidationError(f"unknown file-level evidence citations: {missing_identifiers}")
    return len(cited_identifiers)


def validate_local_references_now(specification: str, workspace_root: Path) -> int:
    cited_identifiers = set(LOCAL_EVIDENCE_ID_PATTERN.findall(specification))
    evidence_paths = [
        workspace_root
        / "docs_PRD04"
        / "reference-learning"
        / "neo4j-compat-lowram"
        / "main-codegraph-evidence.md",
        workspace_root
        / "docs_PRD04"
        / "reference-learning"
        / "neo4j-compat-lowram"
        / "current-implementation-gap-ledger.md",
    ]
    evidence_text = "\n".join(path.read_text(encoding="utf-8") for path in evidence_paths)
    known_identifiers = set(LOCAL_EVIDENCE_ID_PATTERN.findall(evidence_text))
    missing_identifiers = sorted(cited_identifiers - known_identifiers)
    if missing_identifiers:
        raise SpecValidationError(f"unknown local evidence citations: {missing_identifiers}")
    return len(cited_identifiers)


def run_spec_validation_now() -> int:
    workspace_root = Path(__file__).resolve().parents[1]
    specification_path = workspace_root / "docs_PRD04" / "Neo4j-Compatibility-LowRAM-Mega-Spec.md"
    specification = specification_path.read_text(encoding="utf-8")
    validate_section_order_now(specification)
    requirement_blocks = extract_requirement_blocks_now(specification)
    validate_requirement_contracts_now(requirement_blocks)
    test_mappings = extract_test_mappings_now(specification)
    validate_test_coverage_now(requirement_blocks, test_mappings)
    evidence_reference_count = validate_evidence_references_now(specification, workspace_root)
    local_reference_count = validate_local_references_now(specification, workspace_root)
    print(
        f"PASS: {len(requirement_blocks)} requirements, "
        f"{len(test_mappings)} tests, 100% requirement-to-test coverage, "
        f"{evidence_reference_count} verified file-level citations, "
        f"{local_reference_count} verified local citations"
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(run_spec_validation_now())
    except SpecValidationError as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
