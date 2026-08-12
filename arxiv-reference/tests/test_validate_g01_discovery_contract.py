from __future__ import annotations

import csv
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_ROOT = REPOSITORY_ROOT / "arxiv-reference"
VALIDATOR_PATH = REFERENCE_ROOT / "tools" / "validate_arxiv_corpus_contract.py"

QUESTION_LEDGER_PATH = REFERENCE_ROOT / "governance" / "architecture-question-ledger.md"
TAXONOMY_PATH = REFERENCE_ROOT / "governance" / "keyword-taxonomy.tsv"
QUERY_LEDGER_PATH = REFERENCE_ROOT / "governance" / "query-ledger.tsv"
STATUS_PATH = REFERENCE_ROOT / "governance" / "campaign-status.md"
JOURNAL_PATH = REFERENCE_ROOT / "journals" / "G01-progress.md"

TAXONOMY_HEADER = (
    "term_id\tterm\tterm_type\tarchitecture_question_ids\tsource_repo_paths\t"
    "synonyms\thistorical_terms\tadjacent_domain_terms\texclusion_terms\tnotes"
)
QUERY_LEDGER_HEADER = (
    "query_id\tarchitecture_question_ids\tsource_term_ids\tservice\tquery_text\t"
    "categories\tdate_from\tdate_to\texclusions\texecuted_at\tresult_count\t"
    "response_checksum\tstatus"
)
QUESTION_FIELDS = (
    "decision",
    "product_consequence",
    "candidate_options",
    "known_evidence",
    "missing_evidence",
    "falsifier",
    "status",
    "owner_goal",
)
REQUIRED_QUESTION_FAMILIES = {
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
FORBIDDEN_G01_PATHS = (
    "sources/paper-manifest.tsv",
    "sources/citation-edges.tsv",
    "sources/papers",
    "evidence/mechanism-cards",
    "evidence/failure-cards",
    "evidence/constraint-transfer-cards",
    "synthesis/architecture-candidates",
    "synthesis/experiments",
)


def read_tsv_rows(path: Path, expected_header: str) -> list[dict[str, str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        raise AssertionError(f"{path} is empty")
    if lines[0] != expected_header:
        raise AssertionError(f"{path} has the wrong header")
    reader = csv.DictReader(lines[1:], fieldnames=expected_header.split("\t"), delimiter="\t")
    return list(reader)


def split_links(value: str) -> set[str]:
    return {part.strip() for part in value.split("|") if part.strip()}


def parse_question_records(text: str) -> list[dict[str, str]]:
    matches = list(
        re.finditer(
            r"^## (AQ-\d{3}): ([^\n]+)\n(?P<body>.*?)(?=^## AQ-\d{3}:|\Z)",
            text,
            flags=re.MULTILINE | re.DOTALL,
        )
    )
    records: list[dict[str, str]] = []
    for match in matches:
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


def copy_g01_contract_to_temp() -> tuple[tempfile.TemporaryDirectory[str], Path]:
    temporary_directory = tempfile.TemporaryDirectory()
    temporary_root = Path(temporary_directory.name)
    copied_reference_root = temporary_root / "arxiv-reference"
    shutil.copytree(
        REFERENCE_ROOT,
        copied_reference_root,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )

    g02_paths = (
        "governance/g02-metadata-contract.md",
        "governance/g02-service-preflight.md",
        "journals/G02-progress.md",
        "sources/metadata-request-ledger.tsv",
        "sources/paper-manifest.tsv",
        "sources/G02-metadata-screening-report.md",
        "tests/fixtures/g02",
        "tests/test_validate_g02_metadata_contract.py",
        "tools/g02_metadata_pipeline.py",
        "cache/g02",
        "governance/G03-goal-packet.md",
        "governance/g03-citation-contract.md",
        "governance/g03-service-preflight.md",
        "governance/g03-screening-prompts.md",
        "governance/reviews",
        "journals/G03-progress.md",
        "sources/citation-edges.tsv",
        "sources/citation-request-ledger.tsv",
        "sources/citation-stops.tsv",
        "sources/citation-screening-ledger.tsv",
        "sources/G03-citation-ancestry-report.md",
        "tests/fixtures/g03",
        "tests/test_validate_g03_citation_contract.py",
        "tools/g03_citation_pipeline.py",
        "cache/g03",
        "governance/G04-goal-packet.md",
        "governance/g04-acquisition-contract.md",
        "governance/g04-service-preflight.md",
        "journals/G04-progress.md",
        "requirements-g04.txt",
        "sources/download-ledger.tsv",
        "sources/G04-acquisition-parsing-report.md",
        "sources/papers",
        "tests/fixtures/g04",
        "tests/test_validate_g04_acquisition_contract.py",
        "tools/g04_acquisition_pipeline.py",
        "cache/g04",
        "governance/G05-goal-packet.md",
        "governance/g05-mechanism-extraction-contract.md",
        "governance/g05-reading-plan.tsv",
        "journals/G05-progress.md",
        "sources/G05-mechanism-extraction-report.md",
        "evidence/mechanism-cards",
        "evidence/pattern-edges.tsv",
        "tests/test_validate_g05_mechanism_contract.py",
        "tools/g05_mechanism_pipeline.py",
    )
    for relative_path in g02_paths:
        candidate = copied_reference_root / relative_path
        if candidate.is_dir():
            shutil.rmtree(candidate)
        elif candidate.exists():
            candidate.unlink()

    copied_query_path = copied_reference_root / "governance" / "query-ledger.tsv"
    copied_query_rows = read_tsv_rows(copied_query_path, QUERY_LEDGER_HEADER)
    for row in copied_query_rows:
        row["executed_at"] = "NOT_EXECUTED"
        row["result_count"] = "NOT_EXECUTED"
        row["response_checksum"] = "NOT_EXECUTED"
        row["status"] = "PLANNED"
    with copied_query_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=QUERY_LEDGER_HEADER.split("\t"),
            delimiter="\t",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(copied_query_rows)

    copied_status_path = copied_reference_root / "governance" / "campaign-status.md"
    copied_status_path.write_text(
        """# Arxiv Pattern Foundry Campaign Status

- Active goal: `G01`
- Goal state: `COMPLETE`
- Completion state: `COMPLETE`
- Validation state: `VERIFIED`
- Journal: `arxiv-reference/journals/G01-progress.md`

| Measure | Count |
|---|---:|
| Architecture questions | 12 |
| Taxonomy terms | 109 |
| Planned query families | 25 |
| External queries executed | 0 |
| Canonical paper records | 0 |
| Papers screened | 0 |
| Papers read | 0 |
| Full-text files downloaded | 0 |
| Mechanism cards | 0 |
| Failure cards | 0 |
| Constraint-transfer cards | 0 |
| Evidence conflicts | 0 |
| Architecture genomes | 0 |
| Architecture candidates | 0 |
| Candidates changed | 0 |
| Experiments created | 0 |

- Recommended next goal: `G02`
- G02 state: `NOT_STARTED`
""",
        encoding="utf-8",
    )

    evidence_paths: set[str] = set()
    for record in parse_question_records(QUESTION_LEDGER_PATH.read_text(encoding="utf-8")):
        evidence_paths.update(re.findall(r"`([^`]+)`", record.get("known_evidence", "")))
    for row in read_tsv_rows(TAXONOMY_PATH, TAXONOMY_HEADER):
        evidence_paths.update(split_links(row["source_repo_paths"]))

    for relative_path in sorted(evidence_paths):
        source_path = REPOSITORY_ROOT / relative_path
        target_path = temporary_root / relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target_path)
    return temporary_directory, copied_reference_root


def run_validator_with_root(reference_root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(VALIDATOR_PATH), "--root", str(reference_root)],
        cwd=REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )


class ValidateG01DiscoveryContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        required_paths = (
            QUESTION_LEDGER_PATH,
            TAXONOMY_PATH,
            QUERY_LEDGER_PATH,
            STATUS_PATH,
            JOURNAL_PATH,
        )
        missing_paths = [str(path) for path in required_paths if not path.is_file()]
        if missing_paths:
            raise AssertionError("missing G01 artifacts: " + ", ".join(missing_paths))

        cls.question_text = QUESTION_LEDGER_PATH.read_text(encoding="utf-8")
        cls.questions = parse_question_records(cls.question_text)
        cls.taxonomy_rows = read_tsv_rows(TAXONOMY_PATH, TAXONOMY_HEADER)
        cls.query_rows = read_tsv_rows(QUERY_LEDGER_PATH, QUERY_LEDGER_HEADER)
        if re.search(
            r"^- Active goal: `G0[2345]`$",
            STATUS_PATH.read_text(encoding="utf-8"),
            flags=re.MULTILINE,
        ):
            cls.query_rows = [dict(row) for row in cls.query_rows]
            for row in cls.query_rows:
                row["executed_at"] = "NOT_EXECUTED"
                row["result_count"] = "NOT_EXECUTED"
                row["response_checksum"] = "NOT_EXECUTED"
                row["status"] = "PLANNED"
        cls.question_ids = {row["question_id"] for row in cls.questions}
        cls.term_ids = {row["term_id"] for row in cls.taxonomy_rows}
        cls.term_rows_by_id = {row["term_id"]: row for row in cls.taxonomy_rows}

    def test_campaign_preserves_verified_g01_closure(self) -> None:
        status_text = STATUS_PATH.read_text(encoding="utf-8")
        self.assertEqual(
            re.findall(r"^- Active goal:\s*`(G\d{2})`$", status_text, flags=re.MULTILINE),
            ["G05"],
        )
        self.assertIn("G01 remains complete and verified", status_text)
        self.assertIn("12 open architecture questions", status_text)
        self.assertIn("25 planned query families", status_text)

    def test_encoding_contract_is_frozen_before_questions(self) -> None:
        encoding_index = self.question_text.index("## Encoding Contract")
        first_question_index = self.question_text.index("## AQ-001:")
        self.assertLess(encoding_index, first_question_index)
        required_markers = (
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
        for marker in required_markers:
            with self.subTest(marker=marker):
                self.assertIn(marker, self.question_text)

    def test_exactly_twelve_complete_open_questions_exist(self) -> None:
        self.assertEqual(len(self.questions), 12)
        self.assertEqual(
            [row["question_id"] for row in self.questions],
            [f"AQ-{index:03d}" for index in range(1, 13)],
        )
        self.assertEqual(
            {row.get("family_slug") for row in self.questions},
            REQUIRED_QUESTION_FAMILIES,
        )
        for record in self.questions:
            with self.subTest(question_id=record["question_id"]):
                for field_name in QUESTION_FIELDS:
                    self.assertTrue(record.get(field_name), field_name)
                self.assertEqual(record["status"], "OPEN")
                self.assertEqual(record["owner_goal"], "G01")
                self.assertIn("`", record["known_evidence"])

    def test_taxonomy_rows_are_bounded_complete_and_traceable(self) -> None:
        self.assertGreaterEqual(len(self.taxonomy_rows), 1)
        self.assertLessEqual(len(self.taxonomy_rows), 200)
        self.assertEqual(len(self.term_ids), len(self.taxonomy_rows))
        self.assertEqual(
            [row["term_id"] for row in self.taxonomy_rows],
            [f"TERM-{index:03d}" for index in range(1, len(self.taxonomy_rows) + 1)],
        )
        self.assertEqual(
            len({row["term"].casefold() for row in self.taxonomy_rows}),
            len(self.taxonomy_rows),
        )
        for row in self.taxonomy_rows:
            with self.subTest(term_id=row["term_id"]):
                self.assertTrue(all(value.strip() for value in row.values()))
                self.assertIn(row["term_type"], ALLOWED_TERM_TYPES)
                self.assertTrue(split_links(row["architecture_question_ids"]) <= self.question_ids)
                for source_path in split_links(row["source_repo_paths"]):
                    self.assertFalse(Path(source_path).is_absolute())
                    self.assertTrue((REPOSITORY_ROOT / source_path).is_file(), source_path)

    def test_queries_are_planned_complete_compound_and_traceable(self) -> None:
        self.assertGreaterEqual(len(self.query_rows), 12)
        self.assertLessEqual(len(self.query_rows), 25)
        self.assertEqual(
            [row["query_id"] for row in self.query_rows],
            [f"QRY-{index:03d}" for index in range(1, len(self.query_rows) + 1)],
        )
        linked_questions: set[str] = set()
        for row in self.query_rows:
            with self.subTest(query_id=row["query_id"]):
                self.assertTrue(all(value.strip() for value in row.values()))
                self.assertEqual(row["service"], "arXiv")
                self.assertEqual(row["status"], "PLANNED")
                self.assertEqual(row["executed_at"], "NOT_EXECUTED")
                self.assertEqual(row["result_count"], "NOT_EXECUTED")
                self.assertEqual(row["response_checksum"], "NOT_EXECUTED")
                question_links = split_links(row["architecture_question_ids"])
                term_links = split_links(row["source_term_ids"])
                self.assertTrue(question_links)
                self.assertTrue(question_links <= self.question_ids)
                self.assertGreaterEqual(len(term_links), 2)
                self.assertTrue(term_links <= self.term_ids)
                linked_term_types = {
                    self.term_rows_by_id[term_id]["term_type"] for term_id in term_links
                }
                self.assertIn("ALGORITHM", linked_term_types)
                self.assertTrue(linked_term_types - {"ALGORITHM"})
                self.assertGreaterEqual(len(row["query_text"].split()), 4)
                self.assertNotEqual(row["query_text"].strip().casefold(), "graph")
                linked_questions.update(question_links)
        self.assertEqual(linked_questions, self.question_ids)

    def test_g01_contains_no_research_records_or_external_locators(self) -> None:
        temporary_directory, copied_root = copy_g01_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        for relative_path in FORBIDDEN_G01_PATHS:
            with self.subTest(relative_path=relative_path):
                self.assertFalse((copied_root / relative_path).exists())

        owned_text = "\n".join(
            (
                self.question_text,
                TAXONOMY_PATH.read_text(encoding="utf-8"),
                QUERY_LEDGER_PATH.read_text(encoding="utf-8"),
                JOURNAL_PATH.read_text(encoding="utf-8"),
                STATUS_PATH.read_text(encoding="utf-8"),
            )
        )
        self.assertNotRegex(owned_text, r"https?://")
        self.assertNotRegex(owned_text, r"\barXiv:\d{4}\.\d{4,5}\b")
        self.assertNotRegex(owned_text, r"\b\d{4}\.\d{4,5}(?:v\d+)?\b")

    def test_full_corpus_validator_accepts_g01(self) -> None:
        temporary_directory, copied_root = copy_g01_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        result = run_validator_with_root(copied_root)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("PASS", result.stdout)

    def test_invalid_taxonomy_question_link_fails(self) -> None:
        temporary_directory, copied_root = copy_g01_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        taxonomy_path = copied_root / "governance" / "keyword-taxonomy.tsv"
        taxonomy_text = taxonomy_path.read_text(encoding="utf-8")
        taxonomy_path.write_text(
            taxonomy_text.replace("\tAQ-001\t", "\tAQ-999\t", 1),
            encoding="utf-8",
        )

        result = run_validator_with_root(copied_root)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid architecture_question_ids", result.stdout + result.stderr)

    def test_executed_query_row_fails_g01_boundary(self) -> None:
        temporary_directory, copied_root = copy_g01_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        query_path = copied_root / "governance" / "query-ledger.tsv"
        query_text = query_path.read_text(encoding="utf-8")
        query_path.write_text(
            query_text.replace("\tPLANNED\n", "\tEXECUTED\n", 1),
            encoding="utf-8",
        )

        result = run_validator_with_root(copied_root)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("status must remain PLANNED in G01", result.stdout + result.stderr)

    def test_vocabulary_free_query_fails_g01_boundary(self) -> None:
        temporary_directory, copied_root = copy_g01_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        query_path = copied_root / "governance" / "query-ledger.tsv"
        query_text = query_path.read_text(encoding="utf-8")
        query_path.write_text(
            query_text.replace(
                "algorithm shaped storage immutable canonical generation physical view portfolio artifact memory working set",
                "alpha beta gamma delta",
                1,
            ),
            encoding="utf-8",
        )

        result = run_validator_with_root(copied_root)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "query_text must overlap at least two linked taxonomy terms",
            result.stdout + result.stderr,
        )

    def test_query_without_algorithm_term_fails_g01_boundary(self) -> None:
        temporary_directory, copied_root = copy_g01_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        query_path = copied_root / "governance" / "query-ledger.tsv"
        query_text = query_path.read_text(encoding="utf-8")
        query_path.write_text(
            query_text.replace(
                "TERM-001|TERM-002|TERM-003|TERM-004|TERM-009",
                "TERM-001|TERM-002|TERM-003|TERM-004",
                1,
            ),
            encoding="utf-8",
        )

        result = run_validator_with_root(copied_root)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "must combine an ALGORITHM term with a mechanism or resource term",
            result.stdout + result.stderr,
        )

    def test_empty_discovery_ledgers_fail_even_with_matching_status_counts(self) -> None:
        cases = (
            ("keyword-taxonomy.tsv", TAXONOMY_HEADER, "Taxonomy terms", "expected 1-200 rows"),
            ("query-ledger.tsv", QUERY_LEDGER_HEADER, "Planned query families", "expected 12-25 rows"),
        )
        for file_name, header, status_label, expected_error in cases:
            with self.subTest(file_name=file_name):
                temporary_directory, copied_root = copy_g01_contract_to_temp()
                try:
                    ledger_path = copied_root / "governance" / file_name
                    ledger_path.write_text(header + "\n", encoding="utf-8")
                    status_path = copied_root / "governance" / "campaign-status.md"
                    status_text = status_path.read_text(encoding="utf-8")
                    status_text = re.sub(
                        rf"(^\| {re.escape(status_label)} \| )\d+( \|$)",
                        r"\g<1>0\g<2>",
                        status_text,
                        flags=re.MULTILINE,
                    )
                    status_path.write_text(status_text, encoding="utf-8")

                    result = run_validator_with_root(copied_root)

                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(expected_error, result.stdout + result.stderr)
                finally:
                    temporary_directory.cleanup()

    def test_external_locator_fails_g01_boundary(self) -> None:
        temporary_directory, copied_root = copy_g01_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        ledger_path = copied_root / "governance" / "architecture-question-ledger.md"
        ledger_path.write_text(
            ledger_path.read_text(encoding="utf-8") + "\nExternal locator: http://example.invalid\n",
            encoding="utf-8",
        )

        result = run_validator_with_root(copied_root)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("G01 must not contain a URL", result.stdout + result.stderr)

    def test_paper_manifest_fails_g01_boundary(self) -> None:
        temporary_directory, copied_root = copy_g01_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        manifest_path = copied_root / "sources" / "paper-manifest.tsv"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text("paper_id\n", encoding="utf-8")

        result = run_validator_with_root(copied_root)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("file is not allowed while active goal is G01", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
