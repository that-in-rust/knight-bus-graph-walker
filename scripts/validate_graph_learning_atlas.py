#!/usr/bin/env python3
"""Validate line-complete evidence and per-algorithm low-RAM coverage."""

from __future__ import annotations

import csv
from pathlib import Path
import re
import sys


EVIDENCE_SUBTREE = Path(
    "docs_PRD06/reference-learning/all-algorithm-lowram/evidence"
)
ATLAS_SUBTREE = Path("docs_PRD06/LowRAM-All-Algorithm-Architecture-Atlas.md")
AGENT_IDENTIFIERS = ("07", "08", "09")

READING_FIELDS = {
    "lane",
    "path",
    "sha256",
    "bytes",
    "line_count",
    "read_span",
    "coverage_status",
    "algorithms_mentioned",
    "patterns_mentioned",
    "evidence_id",
}
OCCURRENCE_FIELDS = {
    "canonical_hint",
    "raw_name",
    "path",
    "line_start",
    "line_end",
    "context_kind",
    "evidence_id",
}
CANONICAL_FIELDS = {
    "algorithm_id",
    "canonical_name",
    "category",
    "aliases",
    "occurrence_ids",
    "architecture_option_ids",
}
ALLOWED_CONTEXT_KINDS = {
    "algorithm",
    "algorithm_family",
    "storage_algorithm",
    "supporting_structure",
    "protocol",
    "verification_method",
}
ALGORITHM_HEADING = re.compile(r"^### (ALG-\d{3}):\s+(.+)$", re.MULTILINE)
OPTION_HEADING = re.compile(r"^#### (ALG-\d{3}-A\d+):\s+(.+)$", re.MULTILINE)
FIELD_PATTERN = re.compile(r"^\*\*([^*]+):\*\*\s*(.+)$", re.MULTILINE)
REQUIRED_OPTION_FIELDS = {
    "Mode",
    "Storage layout",
    "Memory equation",
    "Budget decision",
    "Latency and I/O",
    "Correctness",
    "Verification",
    "Best for",
    "Reject when",
    "Evidence",
}
ALLOWED_MODES = {"fit", "spill", "approximate", "hybrid"}


class GraphLearningAtlasError(RuntimeError):
    """Raised when the graph-learning research proof is incomplete."""


def load_tsv_rows_now(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.is_file():
        raise GraphLearningAtlasError(f"missing evidence file: {path}")
    with path.open("r", encoding="utf-8", newline="") as source_file:
        reader = csv.DictReader(source_file, delimiter="\t")
        return reader.fieldnames or [], list(reader)


def index_unique_rows_now(
    rows: list[dict[str, str]],
    key_name: str,
    source_name: str,
) -> dict[str, dict[str, str]]:
    indexed: dict[str, dict[str, str]] = {}
    for row in rows:
        key = row.get(key_name, "")
        if not key:
            raise GraphLearningAtlasError(f"{source_name}: empty {key_name}")
        if key in indexed:
            raise GraphLearningAtlasError(f"{source_name}: duplicate {key_name}={key}")
        indexed[key] = row
    return indexed


def validate_reading_ledgers_now(
    denominator_rows: list[dict[str, str]],
    evidence_root: Path,
) -> tuple[list[dict[str, str]], dict[str, dict[str, str]]]:
    denominator = index_unique_rows_now(
        denominator_rows, "path", "graph-learning denominator"
    )
    reading_rows: list[dict[str, str]] = []
    evidence_ids: set[str] = set()
    for agent in AGENT_IDENTIFIERS:
        filename = f"agent-{agent}-files.tsv"
        headers, rows = load_tsv_rows_now(evidence_root / filename)
        missing = sorted(READING_FIELDS - set(headers))
        if missing:
            raise GraphLearningAtlasError(f"{filename}: missing fields {missing}")
        for row in rows:
            path = row["path"]
            expected = denominator.get(path)
            if expected is None:
                raise GraphLearningAtlasError(f"{filename}: unexpected path {path}")
            if expected["assigned_agent"] != f"agent-{agent}":
                raise GraphLearningAtlasError(
                    f"{path}: assigned to {expected['assigned_agent']}, found in agent-{agent}"
                )
            for field in ("lane", "sha256", "bytes", "line_count"):
                if row[field] != expected[field]:
                    raise GraphLearningAtlasError(
                        f"{path}: {field} mismatch, expected {expected[field]!r}, got {row[field]!r}"
                    )
            line_count = int(row["line_count"])
            expected_span = "EMPTY" if line_count == 0 else f"1-{line_count}"
            if row["read_span"] != expected_span or row["coverage_status"] != "line_read":
                raise GraphLearningAtlasError(
                    f"{path}: expected line_read span {expected_span}, got "
                    f"{row['coverage_status']} {row['read_span']}"
                )
            evidence_id = row["evidence_id"]
            if not evidence_id.startswith(f"A{agent}-F"):
                raise GraphLearningAtlasError(f"{path}: invalid evidence ID {evidence_id}")
            if evidence_id in evidence_ids:
                raise GraphLearningAtlasError(f"duplicate file evidence ID {evidence_id}")
            evidence_ids.add(evidence_id)
            reading_rows.append(row)

    reading = index_unique_rows_now(reading_rows, "path", "reading ledger union")
    missing_paths = sorted(set(denominator) - set(reading))
    if missing_paths:
        raise GraphLearningAtlasError(
            f"reading ledgers omit {len(missing_paths)} paths; examples={missing_paths[:10]}"
        )
    return reading_rows, denominator


def validate_occurrence_ledgers_now(
    denominator: dict[str, dict[str, str]],
    evidence_root: Path,
) -> list[dict[str, str]]:
    occurrences: list[dict[str, str]] = []
    occurrence_ids: set[str] = set()
    for agent in AGENT_IDENTIFIERS:
        filename = f"agent-{agent}-algorithm-occurrences.tsv"
        headers, rows = load_tsv_rows_now(evidence_root / filename)
        missing = sorted(OCCURRENCE_FIELDS - set(headers))
        if missing:
            raise GraphLearningAtlasError(f"{filename}: missing fields {missing}")
        for row in rows:
            path = row["path"]
            expected = denominator.get(path)
            if expected is None or expected["assigned_agent"] != f"agent-{agent}":
                raise GraphLearningAtlasError(f"{filename}: invalid owned path {path}")
            try:
                line_start = int(row["line_start"])
                line_end = int(row["line_end"])
            except ValueError as error:
                raise GraphLearningAtlasError(
                    f"{filename}: non-integer span for {row['evidence_id']}"
                ) from error
            if not 1 <= line_start <= line_end <= int(expected["line_count"]):
                raise GraphLearningAtlasError(
                    f"{row['evidence_id']}: span {line_start}-{line_end} outside {path}"
                )
            if row["context_kind"] not in ALLOWED_CONTEXT_KINDS:
                raise GraphLearningAtlasError(
                    f"{row['evidence_id']}: invalid context_kind {row['context_kind']}"
                )
            occurrence_id = row["evidence_id"]
            if not occurrence_id.startswith(f"A{agent}-O"):
                raise GraphLearningAtlasError(f"invalid occurrence ID {occurrence_id}")
            if occurrence_id in occurrence_ids:
                raise GraphLearningAtlasError(f"duplicate occurrence ID {occurrence_id}")
            occurrence_ids.add(occurrence_id)
            occurrences.append(row)
    if not occurrences:
        raise GraphLearningAtlasError("algorithm occurrence union is empty")
    return occurrences


def parse_atlas_options_now(atlas: str) -> dict[str, dict[str, dict[str, str]]]:
    algorithm_matches = list(ALGORITHM_HEADING.finditer(atlas))
    if not algorithm_matches:
        raise GraphLearningAtlasError("atlas has no canonical algorithm headings")
    parsed: dict[str, dict[str, dict[str, str]]] = {}
    for index, algorithm_match in enumerate(algorithm_matches):
        algorithm_id = algorithm_match.group(1)
        block_end = (
            algorithm_matches[index + 1].start()
            if index + 1 < len(algorithm_matches)
            else len(atlas)
        )
        block = atlas[algorithm_match.end():block_end]
        option_matches = list(OPTION_HEADING.finditer(block))
        options: dict[str, dict[str, str]] = {}
        for option_index, option_match in enumerate(option_matches):
            option_id = option_match.group(1)
            if not option_id.startswith(f"{algorithm_id}-A"):
                raise GraphLearningAtlasError(
                    f"{algorithm_id}: foreign option heading {option_id}"
                )
            option_end = (
                option_matches[option_index + 1].start()
                if option_index + 1 < len(option_matches)
                else len(block)
            )
            fields = {
                match.group(1).strip(): match.group(2).strip()
                for match in FIELD_PATTERN.finditer(block[option_match.end():option_end])
            }
            options[option_id] = fields
        parsed[algorithm_id] = options
    return parsed


def validate_canonical_atlas_now(
    occurrences: list[dict[str, str]],
    evidence_root: Path,
    atlas_path: Path,
) -> tuple[int, int]:
    headers, canonical_rows = load_tsv_rows_now(evidence_root / "canonical-algorithms.tsv")
    missing = sorted(CANONICAL_FIELDS - set(headers))
    if missing:
        raise GraphLearningAtlasError(f"canonical-algorithms.tsv: missing fields {missing}")
    canonicals = index_unique_rows_now(
        canonical_rows, "algorithm_id", "canonical algorithm ledger"
    )
    occurrence_ids = {row["evidence_id"] for row in occurrences}
    claimed_occurrences: set[str] = set()
    for algorithm_id, row in canonicals.items():
        if not re.fullmatch(r"ALG-\d{3}", algorithm_id):
            raise GraphLearningAtlasError(f"invalid canonical algorithm ID {algorithm_id}")
        cited = {item for item in row["occurrence_ids"].split(",") if item}
        unknown = cited - occurrence_ids
        if unknown:
            raise GraphLearningAtlasError(f"{algorithm_id}: unknown occurrences {sorted(unknown)}")
        duplicate_claims = claimed_occurrences & cited
        if duplicate_claims:
            raise GraphLearningAtlasError(
                f"{algorithm_id}: occurrences claimed by multiple algorithms {sorted(duplicate_claims)}"
            )
        claimed_occurrences.update(cited)
    algorithm_occurrences = {
        row["evidence_id"]
        for row in occurrences
        if row["context_kind"] in {"algorithm", "algorithm_family", "storage_algorithm"}
    }
    unclaimed = sorted(algorithm_occurrences - claimed_occurrences)
    if unclaimed:
        raise GraphLearningAtlasError(
            f"canonical ledger omits {len(unclaimed)} algorithm occurrences; examples={unclaimed[:10]}"
        )

    if not atlas_path.is_file():
        raise GraphLearningAtlasError(f"missing atlas: {atlas_path}")
    atlas = atlas_path.read_text(encoding="utf-8")
    parsed = parse_atlas_options_now(atlas)
    if set(parsed) != set(canonicals):
        raise GraphLearningAtlasError(
            f"atlas/canonical mismatch: missing={sorted(set(canonicals) - set(parsed))}; "
            f"unexpected={sorted(set(parsed) - set(canonicals))}"
        )

    option_count = 0
    for algorithm_id, options in parsed.items():
        if len(options) < 3:
            raise GraphLearningAtlasError(
                f"{algorithm_id}: has {len(options)} architectures; requires at least 3"
            )
        expected_options = {
            item for item in canonicals[algorithm_id]["architecture_option_ids"].split(",") if item
        }
        if set(options) != expected_options:
            raise GraphLearningAtlasError(
                f"{algorithm_id}: option ledger mismatch; expected={sorted(expected_options)}, "
                f"actual={sorted(options)}"
            )
        modes: set[str] = set()
        for option_id, fields in options.items():
            missing_fields = sorted(REQUIRED_OPTION_FIELDS - set(fields))
            if missing_fields:
                raise GraphLearningAtlasError(f"{option_id}: missing fields {missing_fields}")
            mode = fields["Mode"].lower()
            if mode not in ALLOWED_MODES:
                raise GraphLearningAtlasError(f"{option_id}: invalid mode {mode}")
            modes.add(mode)
            if "=" not in fields["Memory equation"]:
                raise GraphLearningAtlasError(f"{option_id}: memory equation lacks '='")
            cited = set(re.findall(r"A(?:07|08|09)-O\d{4}", fields["Evidence"]))
            if not cited or not cited <= occurrence_ids:
                raise GraphLearningAtlasError(f"{option_id}: missing or unknown occurrence evidence")
        if not ({"spill", "hybrid"} & modes):
            raise GraphLearningAtlasError(f"{algorithm_id}: lacks a bounded spill/hybrid design")
        option_count += len(options)

    for required_text in (
        "A007-spc-founder-interview-prep-v7.md",
        "fit",
        "spill",
        "approximate",
        "refuse",
        "+",
        "-->",
    ):
        if required_text not in atlas:
            raise GraphLearningAtlasError(f"atlas lacks required spine token {required_text!r}")
    return len(canonicals), option_count


def run_graph_atlas_validation() -> int:
    workspace_root = Path(__file__).resolve().parents[1]
    evidence_root = workspace_root / EVIDENCE_SUBTREE
    headers, denominator_rows = load_tsv_rows_now(
        evidence_root / "all-graph-learning-files.tsv"
    )
    required_denominator = {
        "lane", "path", "sha256", "bytes", "line_count", "assigned_agent", "source_id"
    }
    missing = sorted(required_denominator - set(headers))
    if missing:
        raise GraphLearningAtlasError(f"denominator missing fields {missing}")

    reading_rows, denominator = validate_reading_ledgers_now(
        denominator_rows, evidence_root
    )
    occurrences = validate_occurrence_ledgers_now(denominator, evidence_root)
    algorithm_count, option_count = validate_canonical_atlas_now(
        occurrences, evidence_root, workspace_root / ATLAS_SUBTREE
    )
    print(
        f"PASS: {len(reading_rows)} files, "
        f"{sum(int(row['line_count']) for row in reading_rows)} lines, "
        f"{len(occurrences)} occurrences, {algorithm_count} canonical algorithms, "
        f"{option_count} architectures"
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(run_graph_atlas_validation())
    except GraphLearningAtlasError as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
