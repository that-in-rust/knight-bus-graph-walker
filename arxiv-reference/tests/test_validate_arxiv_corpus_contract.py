from __future__ import annotations

import importlib.util
import hashlib
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

QUERY_LEDGER_HEADER = (
    "query_id\tarchitecture_question_ids\tsource_term_ids\tservice\tquery_text\t"
    "categories\tdate_from\tdate_to\texclusions\texecuted_at\tresult_count\t"
    "response_checksum\tstatus"
)
PAPER_MANIFEST_HEADER = (
    "paper_id\tarxiv_id\tdoi\ttitle\tauthors\tpublished_date\tupdated_date\t"
    "categories\tabstract_url\tpdf_url\tlicense_uri\tcanonical_version\t"
    "discovery_query_ids\tarchitecture_question_ids\trelevance_score\t"
    "score_breakdown\tselection_status\tevidence_grade\tcode_urls\tlocal_path\t"
    "sha256\tnotes"
)


def run_validator_with_root(reference_root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(VALIDATOR_PATH), "--root", str(reference_root)],
        cwd=REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )


def copy_contract_to_temp() -> tuple[tempfile.TemporaryDirectory[str], Path]:
    temporary_directory = tempfile.TemporaryDirectory()
    copied_root = Path(temporary_directory.name) / "arxiv-reference"
    shutil.copytree(
        REFERENCE_ROOT,
        copied_root,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    return temporary_directory, copied_root


def load_validator_test_module() -> object:
    specification = importlib.util.spec_from_file_location("arxiv_validator", VALIDATOR_PATH)
    if specification is None or specification.loader is None:
        raise AssertionError("validator module could not be loaded")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def refresh_copied_output_checksum(copied_root: Path, repository_path: str) -> None:
    output_path = copied_root.parent / repository_path
    output_checksum = hashlib.sha256(output_path.read_bytes()).hexdigest()
    ledger_path = copied_root / "governance" / "G00-generation-ledger.md"
    ledger_text = ledger_path.read_text()
    row_pattern = re.compile(
        r"(^\| `"
        + re.escape(repository_path)
        + r"` \| `)[0-9a-f]{64}(` \|[^\n]+$)",
        flags=re.MULTILINE,
    )
    refreshed_text, replacement_count = row_pattern.subn(
        r"\g<1>" + output_checksum + r"\g<2>", ledger_text
    )
    if replacement_count != 1:
        raise AssertionError("expected one checksum row for " + repository_path)
    ledger_path.write_text(refreshed_text)


class ValidateArxivCorpusContractTests(unittest.TestCase):
    def test_empty_corpus_passes(self) -> None:
        result = run_validator_with_root(REFERENCE_ROOT)

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("PASS", result.stdout)

    def test_public_functions_exist(self) -> None:
        self.assertTrue(VALIDATOR_PATH.exists(), str(VALIDATOR_PATH))
        module = load_validator_test_module()

        required_functions = {
            "validate_source_query_terms",
            "deduplicate_paper_manifest_entries",
            "validate_mechanism_card_fields",
            "validate_failure_card_fields",
            "validate_transfer_card_invariants",
            "score_architecture_candidate_niches",
            "verify_download_license_policy",
            "audit_requirement_test_links",
        }
        missing_functions = sorted(name for name in required_functions if not callable(getattr(module, name, None)))

        self.assertEqual(missing_functions, [])

    def test_missing_schema_fails(self) -> None:
        source_schema_path = REFERENCE_ROOT / "governance" / "artifact-schema-contracts.md"
        self.assertTrue(source_schema_path.exists(), str(source_schema_path))
        temporary_directory, copied_root = copy_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        schema_path = copied_root / "governance" / "artifact-schema-contracts.md"
        schema_path.unlink()

        result = run_validator_with_root(copied_root)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("artifact-schema-contracts.md", result.stdout + result.stderr)

    def test_malformed_schema_fails(self) -> None:
        temporary_directory, copied_root = copy_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        schema_path = copied_root / "governance" / "artifact-schema-contracts.md"
        schema_path.write_text("# Malformed But Nonempty\n")

        result = run_validator_with_root(copied_root)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("artifact-schema-contracts.md", result.stdout + result.stderr)

    def test_schema_corruption_fails(self) -> None:
        temporary_directory, copied_root = copy_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        schema_path = copied_root / "governance" / "artifact-schema-contracts.md"
        schema_text = schema_path.read_text()
        corrupted_text = schema_text.replace(
            "No required field may be blank in a completed artifact.",
            "Every required field may be blank in a completed artifact.",
        )
        self.assertNotEqual(corrupted_text, schema_text)
        schema_path.write_text(corrupted_text)

        result = run_validator_with_root(copied_root)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("artifact-schema-contracts.md", result.stdout + result.stderr)

    def test_future_artifact_fails(self) -> None:
        temporary_directory, copied_root = copy_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        card_path = copied_root / "evidence" / "mechanism-cards" / "PAT-NOT-G00-DATA-YET.md"
        card_path.parent.mkdir(parents=True, exist_ok=True)
        card_path.write_text("pattern_id: PAT-NOT-G00-DATA-YET\n")

        result = run_validator_with_root(copied_root)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PAT-NOT-G00-DATA-YET.md", result.stdout + result.stderr)

    def test_unexpected_artifacts_fail(self) -> None:
        unexpected_paths = (
            "evidence/pattern-relationships.tsv",
            "sources/source-aliases.tsv",
            "synthesis/candidate-notes.md",
        )
        for relative_path in unexpected_paths:
            with self.subTest(relative_path=relative_path):
                temporary_directory, copied_root = copy_contract_to_temp()
                try:
                    artifact_path = copied_root / relative_path
                    artifact_path.parent.mkdir(parents=True, exist_ok=True)
                    artifact_path.write_text("unauthorized G00 record\n")

                    result = run_validator_with_root(copied_root)

                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(relative_path, result.stdout + result.stderr)
                finally:
                    temporary_directory.cleanup()

    def test_invalid_header_fails(self) -> None:
        temporary_directory, copied_root = copy_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        ledger_path = copied_root / "governance" / "query-ledger.tsv"
        ledger_path.parent.mkdir(parents=True, exist_ok=True)
        ledger_path.write_text(QUERY_LEDGER_HEADER.replace("query_id", "wrong_id") + "\n")

        result = run_validator_with_root(copied_root)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("query-ledger.tsv", result.stdout + result.stderr)

    def test_duplicate_paper_fails(self) -> None:
        temporary_directory, copied_root = copy_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        manifest_path = copied_root / "sources" / "paper-manifest.tsv"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        row = [""] * len(PAPER_MANIFEST_HEADER.split("\t"))
        row[0] = "PAPER-DUPLICATE"
        row[3] = "Duplicate fixture"
        row[16] = "METADATA_ONLY"
        duplicate_row = "\t".join(row)
        manifest_path.write_text(
            PAPER_MANIFEST_HEADER + "\n" + duplicate_row + "\n" + duplicate_row + "\n"
        )

        result = run_validator_with_root(copied_root)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PAPER-DUPLICATE", result.stdout + result.stderr)

    def test_acquired_license_fails(self) -> None:
        temporary_directory, copied_root = copy_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        manifest_path = copied_root / "sources" / "paper-manifest.tsv"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        row = [""] * len(PAPER_MANIFEST_HEADER.split("\t"))
        row[0] = "PAPER-UNLICENSED"
        row[3] = "License-state fixture"
        row[10] = "UNKNOWN"
        row[16] = "DEEP_READ"
        row[19] = "sources/papers/PAPER-UNLICENSED.pdf"
        row[20] = "0" * 64
        manifest_path.write_text(PAPER_MANIFEST_HEADER + "\n" + "\t".join(row) + "\n")

        result = run_validator_with_root(copied_root)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PAPER-UNLICENSED", result.stdout + result.stderr)
        self.assertIn("license state", (result.stdout + result.stderr).lower())

    def test_pdf_ignore_exists(self) -> None:
        ignore_path = REFERENCE_ROOT / ".gitignore"
        self.assertTrue(ignore_path.exists(), str(ignore_path))
        ignore_text = ignore_path.read_text()

        self.assertIn("sources/papers/", ignore_text)
        self.assertIn("*.pdf", ignore_text)

    def test_tracked_pdf_fails(self) -> None:
        temporary_directory, copied_root = copy_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        repository_root = copied_root.parent
        pdf_path = copied_root / "sources" / "papers" / "tracked.pdf"
        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        pdf_path.write_bytes(b"not a real PDF")
        subprocess.run(
            ["git", "init", "--quiet", str(repository_root)], check=True, timeout=30
        )
        subprocess.run(
            ["git", "-C", str(repository_root), "add", "-f", str(pdf_path)],
            check=True,
        )

        result = run_validator_with_root(copied_root)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "sources/papers/tracked.pdf: PDF is tracked or staged by Git",
            result.stdout + result.stderr,
        )

    def test_generation_ledger_exists(self) -> None:
        ledger_path = REFERENCE_ROOT / "governance" / "G00-generation-ledger.md"
        self.assertTrue(ledger_path.exists(), str(ledger_path))
        ledger_text = ledger_path.read_text()

        self.assertIn("gpt-5.6-sol", ledger_text)
        self.assertIn("xhigh", ledger_text)
        self.assertIn("priority", ledger_text)
        self.assertIn("Prompt Reconstruction", ledger_text)
        self.assertRegex(ledger_text, r"[0-9a-f]{64}")

    def test_integrity_repair_provenance_exists(self) -> None:
        ledger_text = (
            REFERENCE_ROOT / "governance" / "G00-generation-ledger.md"
        ).read_text()
        expected_writers = {
            "Ptolemy": "019fecbd-acf7-7e62-a28f-e3b2b8cab527",
            "Avicenna": "019fecbd-af95-7920-b1f9-39643eeb2048",
            "Plato": "019fecbd-b271-7aa3-8e3e-bc08fbd0a71e",
        }
        for writer_name, agent_id in expected_writers.items():
            with self.subTest(writer_name=writer_name):
                self.assertIn(writer_name, ledger_text)
                self.assertIn(agent_id, ledger_text)
                self.assertIn("### Integrity Repair Lane: " + writer_name, ledger_text)

    def test_malformed_ledger_fails(self) -> None:
        temporary_directory, copied_root = copy_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        ledger_path = copied_root / "governance" / "G00-generation-ledger.md"
        ledger_path.write_text(
            "# G00 Generation Ledger\n\n"
            "gpt-5.6-sol xhigh priority\n\n"
            "## Prompt Reconstruction\n\n"
            + "0" * 64
            + "\n"
        )

        result = run_validator_with_root(copied_root)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("G00-generation-ledger.md", result.stdout + result.stderr)

    def test_cache_suffix_escape_fails(self) -> None:
        unexpected_paths = (
            "governance/future-record.pyc",
            "sources/future-record.pyo",
            "evidence/future-record.pyd",
            "retrieval/future-record.pyc",
            "synthesis/future-record.pyc",
            "prompts/future-record.pyc",
            "journals/future-record.pyc",
        )
        for relative_path in unexpected_paths:
            with self.subTest(relative_path=relative_path):
                temporary_directory, copied_root = copy_contract_to_temp()
                try:
                    artifact_path = copied_root / relative_path
                    artifact_path.parent.mkdir(parents=True, exist_ok=True)
                    artifact_path.write_bytes(b"unauthorized G00 record")

                    result = run_validator_with_root(copied_root)

                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(relative_path, result.stdout + result.stderr)
                finally:
                    temporary_directory.cleanup()

    def test_active_goal_bypass_fails(self) -> None:
        temporary_directory, copied_root = copy_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        status_path = copied_root / "governance" / "campaign-status.md"
        status_text = status_path.read_text()
        active_goals = re.findall(
            r"^- Active goal: `(G\d{2})`$", status_text, flags=re.MULTILINE
        )
        self.assertEqual(active_goals, ["G06"])
        status_path.write_text(
            status_text.replace("- Active goal: `G06`", "- Active goal: `G99`")
        )
        future_path = copied_root / "evidence" / "future-record.md"
        future_path.parent.mkdir(parents=True, exist_ok=True)
        future_path.write_text("unauthorized later-goal record\n")

        result = run_validator_with_root(copied_root)
        output = result.stdout + result.stderr

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("active goal G99 is not supported", output)

    def test_in_progress_g03_completion_candidate_requires_exact_fifty_queue(self) -> None:
        temporary_directory, copied_root = copy_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        report_path = copied_root / "sources" / "G03-citation-ancestry-report.md"
        report = report_path.read_text(encoding="utf-8")
        section_start = report.index("## Exact Recommended G04 Acquisition Set")
        scope_start = report.index("## Scope Boundary", section_start)
        prefix, section, suffix = (
            report[:section_start],
            report[section_start:scope_start],
            report[scope_start:],
        )
        rows = re.findall(r"^\| \d+ \| `PAPER-[^\n]+$", section, flags=re.MULTILINE)
        self.assertEqual(len(rows), 50)
        section = section.replace(rows[-1] + "\n", "").replace(
            "Exact G04 set size: **50**", "Exact G04 set size: **49**"
        )
        report_path.write_text(prefix + section + suffix, encoding="utf-8")

        result = run_validator_with_root(copied_root)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("G04 set must contain exactly 50 identities", result.stdout)

    def test_g03_report_rejects_deferred_screening_substitution(self) -> None:
        temporary_directory, copied_root = copy_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        report_path = copied_root / "sources" / "G03-citation-ancestry-report.md"
        report = report_path.read_text(encoding="utf-8")
        section_start = report.index("## Exact Recommended G04 Acquisition Set")
        scope_start = report.index("## Scope Boundary", section_start)
        section = report[section_start:scope_start]
        self.assertIn("PAPER-2101.12631", section)
        self.assertNotIn("PAPER-0709.2938", section)
        section = section.replace("PAPER-2101.12631", "PAPER-0709.2938")
        report_path.write_text(
            report[:section_start] + section + report[scope_start:],
            encoding="utf-8",
        )

        result = run_validator_with_root(copied_root)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ancestry half must equal screening ACQUIRE ranks", result.stdout)

    def test_g03_report_rejects_acquire_rank_reordering(self) -> None:
        temporary_directory, copied_root = copy_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        report_path = copied_root / "sources" / "G03-citation-ancestry-report.md"
        report = report_path.read_text(encoding="utf-8")
        section_start = report.index("## Exact Recommended G04 Acquisition Set")
        scope_start = report.index("## Scope Boundary", section_start)
        section = report[section_start:scope_start]
        first = "PAPER-HASH-9b43309b046b4742"
        second = "PAPER-HASH-2d35f96d423f4ddb"
        self.assertIn(first, section)
        self.assertIn(second, section)
        section = section.replace(first, "PAPER-SWAP-PLACEHOLDER", 1)
        section = section.replace(second, first, 1)
        section = section.replace("PAPER-SWAP-PLACEHOLDER", second, 1)
        report_path.write_text(
            report[:section_start] + section + report[scope_start:],
            encoding="utf-8",
        )

        result = run_validator_with_root(copied_root)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ancestry half must equal screening ACQUIRE ranks", result.stdout)

    def test_control_symlink_fails(self) -> None:
        temporary_directory, copied_root = copy_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        status_path = copied_root / "governance" / "campaign-status.md"
        external_status_path = copied_root.parent / "external-campaign-status.md"
        external_status_path.write_bytes(status_path.read_bytes())
        status_path.unlink()
        status_path.symlink_to(external_status_path)

        result = run_validator_with_root(copied_root)
        output = result.stdout + result.stderr

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("required G00 control file must be a regular non-symlink file", output)

    def test_manifest_path_traversal_fails(self) -> None:
        module = load_validator_test_module()
        ignore_text = (REFERENCE_ROOT / ".gitignore").read_text()
        invalid_paths = (
            "../../sources/papers/escape.pdf",
            "sources/papers/../../outside.pdf",
            "/sources/papers/absolute.pdf",
        )
        for local_path in invalid_paths:
            with self.subTest(local_path=local_path):
                errors = module.verify_download_license_policy(
                    [
                        {
                            "paper_id": "PAPER-PATH-ESCAPE",
                            "local_path": local_path,
                            "selection_status": "DEEP_READ",
                            "notes": "LICENSE_UNKNOWN",
                            "sha256": "0" * 64,
                        }
                    ],
                    ignore_text,
                )

                self.assertTrue(
                    any(
                        "local_path must be a safe relative path under sources/papers/"
                        in error
                        for error in errors
                    ),
                    errors,
                )

    def test_ledger_path_escape_fails(self) -> None:
        temporary_directory, copied_root = copy_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        external_path = copied_root.parent / "outside-ledger-target.md"
        external_path.write_text("outside corpus\n")
        external_checksum = hashlib.sha256(external_path.read_bytes()).hexdigest()
        ledger_path = copied_root / "governance" / "G00-generation-ledger.md"
        ledger_text = ledger_path.read_text()
        escaped_row = "| `{0}` | `{1}` | path escape probe |".format(
            external_path, external_checksum
        )
        ledger_text, replacement_count = re.subn(
            r"^\| `Markdown-Value-Index\.md` \| `[0-9a-f]{64}` \|[^\n]+$",
            escaped_row,
            ledger_text,
            count=1,
            flags=re.MULTILINE,
        )
        self.assertEqual(replacement_count, 1)
        ledger_path.write_text(ledger_text)

        result = run_validator_with_root(copied_root)
        output = result.stdout + result.stderr

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("checksum path is not an allowed G00 output", output)

    def test_tracked_pdf_outside_papers_fails(self) -> None:
        temporary_directory, copied_root = copy_contract_to_temp()
        self.addCleanup(temporary_directory.cleanup)
        repository_root = copied_root.parent
        pdf_path = copied_root / "evidence" / "tracked-outside.pdf"
        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        pdf_path.write_bytes(b"not a real PDF")
        subprocess.run(
            ["git", "init", "--quiet", str(repository_root)], check=True, timeout=30
        )
        subprocess.run(
            ["git", "-C", str(repository_root), "add", "-f", str(pdf_path)],
            check=True,
            timeout=30,
        )

        result = run_validator_with_root(copied_root)
        output = result.stdout + result.stderr

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("evidence/tracked-outside.pdf: PDF is tracked or staged by Git", output)

    def test_cache_payload_fails(self) -> None:
        cache_tag = sys.implementation.cache_tag
        self.assertIsNotNone(cache_tag)
        for relative_path in (
            "tests/__pycache__/test_validate_arxiv_corpus_contract.{0}.pyc".format(
                cache_tag
            ),
            "tools/__pycache__/validate_arxiv_corpus_contract.{0}.pyc".format(
                cache_tag
            ),
        ):
            with self.subTest(relative_path=relative_path):
                temporary_directory, copied_root = copy_contract_to_temp()
                try:
                    cache_path = copied_root / relative_path
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    cache_path.write_bytes(b"not Python bytecode")

                    result = run_validator_with_root(copied_root)
                    output = result.stdout + result.stderr

                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(relative_path, output)
                finally:
                    temporary_directory.cleanup()

    def test_traceability_links_pass(self) -> None:
        sop_text = (REFERENCE_ROOT / "Arxiv-Pattern-Foundry-SOP.md").read_text()
        requirement_ids = {
            line.split(":", 1)[0].removeprefix("### ")
            for line in sop_text.splitlines()
            if line.startswith("### REQ-")
        }
        matrix_requirement_ids = {
            line.split("|")[1].strip()
            for line in sop_text.splitlines()
            if line.startswith("| REQ-")
        }
        test_ids = {
            line.split("|")[2].strip()
            for line in sop_text.splitlines()
            if line.startswith("| REQ-")
        }

        self.assertEqual(len(requirement_ids), 49)
        self.assertEqual(requirement_ids, matrix_requirement_ids)
        self.assertEqual(len(test_ids), 49)


if __name__ == "__main__":
    unittest.main()
