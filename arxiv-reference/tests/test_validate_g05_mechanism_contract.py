#!/usr/bin/env python3
"""RED-first tests for bounded G05 mechanism extraction."""

from __future__ import annotations

import copy
import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_ROOT = REPOSITORY_ROOT / "arxiv-reference"
PIPELINE_PATH = REFERENCE_ROOT / "tools" / "g05_mechanism_pipeline.py"
VALIDATOR_PATH = REFERENCE_ROOT / "tools" / "validate_arxiv_corpus_contract.py"


def load_g05_pipeline_module():
    """Load the G05 implementation after proving its path exists."""

    if not PIPELINE_PATH.is_file():
        raise AssertionError("G05 mechanism pipeline is missing")
    spec = importlib.util.spec_from_file_location("g05_mechanism_pipeline", PIPELINE_PATH)
    if spec is None or spec.loader is None:
        raise AssertionError("G05 mechanism pipeline cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_corpus_validator_module():
    """Load the shared corpus validator for G05 integration checks."""

    spec = importlib.util.spec_from_file_location("g05_corpus_validator", VALIDATOR_PATH)
    if spec is None or spec.loader is None:
        raise AssertionError("corpus validator cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_plan_rows_exact() -> list[dict[str, str]]:
    """Read the frozen G05 reading plan."""

    path = REFERENCE_ROOT / "governance" / "g05-reading-plan.tsv"
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def create_valid_card_fixture() -> dict[str, object]:
    """Create one complete source-grounded mechanism fixture."""

    source_claim = {
        "claim_type": "SOURCE_CLAIM",
        "text": "The source groups active state so irrelevant state is not resident.",
        "source_pointer_ids": ["SP-001"],
        "premises": [],
        "assumptions": [],
        "uncertainty": "The bound applies only to the source workload.",
    }
    unknown_resource = {
        "status": "UNKNOWN",
        "expression": "UNKNOWN",
        "source_pointer_ids": [],
        "premises": [],
        "assumptions": [],
        "uncertainty": "The paper does not bound this whole-process term.",
        "measurement_needed": "Measure the term on the declared fixture.",
    }
    return {
        "pattern_id": "PAT-SELECT-ACTIVE-PARTITIONS-ONLY",
        "name": "Select Active Partitions Only",
        "epistemic_label": "SOURCE_CLAIM",
        "source_paper_ids": ["PAPER-1806.08092"],
        "source_pointers": [
            {
                "pointer_id": "SP-001",
                "paper_id": "PAPER-1806.08092",
                "page": 4,
                "locator_type": "SECTION",
                "locator_value": "Section 3.1, partition activation",
                "claim_scope": "Partition activation and retained state",
            }
        ],
        "source_domain": "external-memory graph processing",
        "problem": copy.deepcopy(source_claim),
        "invariant": copy.deepcopy(source_claim),
        "mechanism": copy.deepcopy(source_claim),
        "data_arrangement": copy.deepcopy(source_claim),
        "access_schedule": copy.deepcopy(source_claim),
        "resident_state": copy.deepcopy(source_claim),
        "streamed_state": copy.deepcopy(source_claim),
        "recomputed_state": copy.deepcopy(source_claim),
        "resource_model": {
            "ram": copy.deepcopy(unknown_resource),
            "io": copy.deepcopy(unknown_resource),
            "preprocessing": copy.deepcopy(unknown_resource),
            "persistent_storage": copy.deepcopy(unknown_resource),
            "temporary_storage": copy.deepcopy(unknown_resource),
        },
        "works_when": [copy.deepcopy(source_claim)],
        "fails_when": [copy.deepcopy(source_claim)],
        "unknown_when": [copy.deepcopy(source_claim)],
        "knight_bus_algorithm_families": ["PAGERANK"],
        "a007_consequence": {
            "claim_type": "DERIVED_INFERENCE",
            "text": "Admission must account for active partitions rather than total topology alone.",
            "source_pointer_ids": ["SP-001"],
            "premises": ["PAT-SELECT-ACTIVE-PARTITIONS-ONLY:mechanism"],
            "assumptions": ["Knight Bus can predict the active partition set."],
            "uncertainty": "Prediction error is not bounded by the paper.",
        },
        "falsifying_test": {
            "fixture": "A graph whose active vertices touch every partition",
            "independent_oracle": "A full in-memory traversal",
            "controlled_variables": ["vertex count", "edge count", "partition count"],
            "failure_signal": "Peak RSS or I/O loses the claimed active-partition bound",
            "scope": "Analytical test description only; no G09 experiment exists",
        },
        "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-SELECT-ACTIVE-PARTITIONS-ONLY",
        "evidence_grade": "C_PAPER_BENCHMARK",
        "confidence_rationale": {
            "claim_type": "DERIVED_INFERENCE",
            "text": "The source describes the mechanism, but G05 did not reproduce it.",
            "source_pointer_ids": ["SP-001"],
            "premises": ["The cited section describes partition activation."],
            "assumptions": ["The paper accurately reports the evaluated mechanism."],
            "uncertainty": "No campaign reproduction or code inspection occurred.",
        },
        "related_pattern_ids": [],
    }


class ValidateG05MechanismContractTests(unittest.TestCase):
    def test_full_validator_supports_g05(self) -> None:
        validator = load_corpus_validator_module()
        errors = validator.run_corpus_contract_checks(REFERENCE_ROOT)
        self.assertFalse(
            any("active goal G05 is not supported" in error for error in errors),
            errors,
        )
        self.assertFalse(
            any("outside G04 ownership" in error for error in errors),
            errors,
        )

    def test_selects_twentyfive_unique(self) -> None:
        pipeline = load_g05_pipeline_module()
        selected = pipeline.derive_selected_paper_records(REFERENCE_ROOT)
        self.assertEqual(len(selected), 25)
        self.assertEqual(len({row["paper_id"] for row in selected}), 25)

    def test_rejects_ineligible_papers(self) -> None:
        pipeline = load_g05_pipeline_module()
        selected = pipeline.derive_selected_paper_records(REFERENCE_ROOT)
        changed = [dict(row) for row in selected]
        changed[-1]["paper_id"] = "PAPER-NOT-G04-ELIGIBLE"
        errors = pipeline.validate_selected_paper_records(changed, REFERENCE_ROOT)
        self.assertTrue(any("eligible" in error for error in errors))

    def test_assigns_terminal_outcomes(self) -> None:
        pipeline = load_g05_pipeline_module()
        rows = read_plan_rows_exact()
        self.assertEqual(pipeline.validate_completed_reading_rows(rows), [])
        nonterminal = [dict(row) for row in rows]
        nonterminal[0].update(
            {
                "reading_status": "PENDING",
                "terminal_outcome": "PENDING",
                "card_ids": "NONE",
                "reading_coverage": "PENDING",
                "result_checksum": "PENDING",
            }
        )
        errors = pipeline.validate_completed_reading_rows(nonterminal)
        self.assertTrue(any("terminal" in error for error in errors))
        completed = []
        for row in rows:
            current = dict(row)
            current.update(
                {
                    "reader_agent_id": "AGENT-READER",
                    "reviewer_agent_id": "AGENT-REVIEWER",
                    "reading_status": "COMPLETE",
                    "terminal_outcome": "NO_MECHANISM",
                    "card_ids": "NONE",
                    "reading_coverage": "ALL_PAGES:1-" + row["page_count"],
                    "no_mechanism_rationale": "Complete reading found no reusable data or execution mechanism.",
                    "result_checksum": "A" * 64,
                }
            )
            completed.append(current)
        self.assertEqual(pipeline.validate_completed_reading_rows(completed), [])

    def test_requires_precise_pointers(self) -> None:
        pipeline = load_g05_pipeline_module()
        card = create_valid_card_fixture()
        self.assertEqual(pipeline.validate_mechanism_card_record(card, {"PAPER-1806.08092": 28}), [])
        del card["source_pointers"][0]["page"]
        errors = pipeline.validate_mechanism_card_record(card, {"PAPER-1806.08092": 28})
        self.assertTrue(any("page" in error for error in errors))

    def test_requires_derived_confidence(self) -> None:
        pipeline = load_g05_pipeline_module()
        card = create_valid_card_fixture()
        card["confidence_rationale"] = copy.deepcopy(card["mechanism"])
        errors = pipeline.validate_mechanism_card_record(
            card, {"PAPER-1806.08092": 28}
        )
        self.assertTrue(any("confidence_rationale" in error for error in errors))

    def test_limits_pointer_quotations(self) -> None:
        pipeline = load_g05_pipeline_module()
        card = create_valid_card_fixture()
        card["source_pointers"][0]["short_quote"] = " ".join(["word"] * 26)
        errors = pipeline.validate_mechanism_card_record(card, {"PAPER-1806.08092": 28})
        self.assertTrue(any("quote" in error for error in errors))

    def test_parses_canonical_envelope(self) -> None:
        pipeline = load_g05_pipeline_module()
        card = create_valid_card_fixture()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / (card["pattern_id"] + ".md")
            payload = json.dumps(card, indent=2, sort_keys=True)
            path.write_text("# " + card["name"] + "\n\n```json\n" + payload + "\n```\n", encoding="utf-8")
            self.assertEqual(pipeline.parse_mechanism_card_file(path), card)
            path.write_text("```json\n{}\n```\n```json\n{}\n```\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "exactly one"):
                pipeline.parse_mechanism_card_file(path)

    def test_parses_batch_dossier_cards(self) -> None:
        pipeline = load_g05_pipeline_module()
        first = create_valid_card_fixture()
        second = copy.deepcopy(first)
        second["pattern_id"] = "PAT-STREAM-COLD-EDGES-SEQUENTIALLY"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "batch.md"
            path.write_text(
                "# Batch\n\n```json\n"
                + json.dumps(first, sort_keys=True)
                + "\n```\n\n```json\n"
                + json.dumps(second, sort_keys=True)
                + "\n```\n",
                encoding="utf-8",
            )
            self.assertEqual(
                [card["pattern_id"] for card in pipeline.parse_batch_dossier_cards(path)],
                [first["pattern_id"], second["pattern_id"]],
            )

    def test_classifies_resource_terms(self) -> None:
        pipeline = load_g05_pipeline_module()
        card = create_valid_card_fixture()
        card["resource_model"]["ram"]["expression"] = "0"
        errors = pipeline.validate_mechanism_card_record(card, {"PAPER-1806.08092": 28})
        self.assertTrue(any("UNKNOWN" in error or "resource" in error for error in errors))

    def test_rejects_duplicate_cards(self) -> None:
        pipeline = load_g05_pipeline_module()
        card = create_valid_card_fixture()
        errors = pipeline.validate_mechanism_card_collection([card, copy.deepcopy(card)])
        self.assertTrue(any("duplicate" in error for error in errors))

    def test_rejects_invalid_edges(self) -> None:
        pipeline = load_g05_pipeline_module()
        card = create_valid_card_fixture()
        second = copy.deepcopy(card)
        second["pattern_id"] = "PAT-STREAM-COLD-EDGES-SEQUENTIALLY"
        edge = {
            "edge_id": "PEDGE-0001",
            "source_pattern_id": second["pattern_id"],
            "target_pattern_id": card["pattern_id"],
            "relationship_type": "SHARES_MECHANISM_WITH",
            "rationale": "DERIVED_INFERENCE[premises=both stream cold state;assumptions=layouts are comparable;uncertainty=not benchmarked together]",
            "epistemic_label": "DERIVED_INFERENCE",
            "source_paper_ids": "PAPER-1806.08092",
            "source_pointer_ids": "SP-001",
        }
        errors = pipeline.validate_pattern_edge_rows([edge], {card["pattern_id"], second["pattern_id"]})
        self.assertTrue(any("canonical" in error for error in errors))

    def test_binds_output_crosslinks(self) -> None:
        pipeline = load_g05_pipeline_module()
        first = create_valid_card_fixture()
        second = copy.deepcopy(first)
        second["pattern_id"] = "PAT-STREAM-COLD-EDGES-SEQUENTIALLY"
        first["related_pattern_ids"] = [second["pattern_id"]]
        second["related_pattern_ids"] = [first["pattern_id"]]
        edge = {
            "edge_id": "PEDGE-0001",
            "source_pattern_id": first["pattern_id"],
            "target_pattern_id": second["pattern_id"],
            "relationship_type": "COMPLEMENTS",
            "rationale": "DERIVED_INFERENCE[premises=active selection and cold streaming;assumptions=the states are separable;uncertainty=not co-benchmarked]",
            "epistemic_label": "DERIVED_INFERENCE",
            "source_paper_ids": "PAPER-1806.08092",
            "source_pointer_ids": first["pattern_id"] + "#SP-001",
        }
        plan_row = {
            "paper_id": "PAPER-1806.08092",
            "reading_status": "COMPLETE",
            "terminal_outcome": "MECHANISM_EXTRACTED",
            "card_ids": first["pattern_id"] + "|" + second["pattern_id"],
        }
        self.assertEqual(
            pipeline.validate_output_crosslinks_complete(
                [first, second], [edge], [plan_row]
            ),
            [],
        )
        second["related_pattern_ids"] = []
        errors = pipeline.validate_output_crosslinks_complete(
            [first, second], [edge], [plan_row]
        )
        self.assertTrue(any("related_pattern_ids" in error for error in errors))

    def test_binds_result_checksum(self) -> None:
        pipeline = load_g05_pipeline_module()
        card = create_valid_card_fixture()
        row = {
            "paper_id": "PAPER-1806.08092",
            "reader_agent_id": "AGENT-READER",
            "reviewer_agent_id": "AGENT-REVIEWER",
            "reading_status": "COMPLETE",
            "terminal_outcome": "MECHANISM_EXTRACTED",
            "card_ids": card["pattern_id"],
            "reading_coverage": "ALL_PAGES:1-28",
            "no_mechanism_rationale": "NOT_APPLICABLE",
        }
        first = pipeline.calculate_reading_result_checksum(row, {card["pattern_id"]: card})
        second = pipeline.calculate_reading_result_checksum(row, {card["pattern_id"]: card})
        self.assertEqual(first, second)
        self.assertRegex(first, r"^[0-9A-F]{64}$")
        changed = copy.deepcopy(card)
        changed["mechanism"]["text"] = "Changed mechanism claim."
        changed_hash = pipeline.calculate_reading_result_checksum(
            row, {changed["pattern_id"]: changed}
        )
        self.assertNotEqual(first, changed_hash)
        row["result_checksum"] = first
        self.assertEqual(
            pipeline.validate_reading_result_checksums(
                [row], {card["pattern_id"]: card}
            ),
            [],
        )
        row["result_checksum"] = "0" * 64
        errors = pipeline.validate_reading_result_checksums(
            [row], {card["pattern_id"]: card}
        )
        self.assertTrue(any("result_checksum" in error for error in errors))

    def test_blocks_premature_completion(self) -> None:
        pipeline = load_g05_pipeline_module()
        manifest_rows = pipeline.read_tsv_records_exact(REFERENCE_ROOT / "sources" / "paper-manifest.tsv")
        plan_rows = read_plan_rows_exact()
        selected_id = plan_rows[0]["paper_id"]
        plan_rows[0].update(
            {
                "reading_status": "PENDING",
                "terminal_outcome": "PENDING",
                "card_ids": "NONE",
                "reading_coverage": "PENDING",
                "result_checksum": "PENDING",
            }
        )
        for row in manifest_rows:
            if row["paper_id"] == selected_id:
                row["selection_status"] = "READ_COMPLETE"
                break
        errors = pipeline.validate_read_complete_transitions(manifest_rows, plan_rows)
        self.assertTrue(any("terminal" in error or "premature" in error for error in errors))

    def test_blocks_later_artifacts(self) -> None:
        pipeline = load_g05_pipeline_module()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            forbidden = root / "evidence" / "failure-cards" / "FAIL-LATER-GOAL-LEAKS-HERE.md"
            forbidden.parent.mkdir(parents=True)
            forbidden.write_text("forbidden\n", encoding="utf-8")
            errors = pipeline.validate_later_artifacts_absent(root)
        self.assertTrue(any("G06" in error or "failure" in error for error in errors))

    def test_freezes_contract_schemas(self) -> None:
        pipeline = load_g05_pipeline_module()
        self.assertEqual(len(pipeline.READING_PLAN_FIELDS), 21)
        self.assertEqual(
            pipeline.PATTERN_RELATIONSHIP_TYPES,
            {"SHARES_MECHANISM_WITH", "COMPLEMENTS", "CONTRADICTS", "SUBSUMES"},
        )
        self.assertEqual(pipeline.TERMINAL_PAPER_OUTCOMES, {"MECHANISM_EXTRACTED", "NO_MECHANISM"})

    def test_preserves_excluded_deepread(self) -> None:
        pipeline = load_g05_pipeline_module()
        selected = {row["paper_id"] for row in pipeline.derive_selected_paper_records(REFERENCE_ROOT)}
        eligible = pipeline.derive_eligible_paper_records(REFERENCE_ROOT)
        excluded = [row for row in eligible if row["paper_id"] not in selected]
        self.assertEqual(len(excluded), 9)
        self.assertTrue(all(row["selection_status"] == "DEEP_READ" for row in excluded))

    def test_validates_plan_checksums(self) -> None:
        pipeline = load_g05_pipeline_module()
        rows = read_plan_rows_exact()
        self.assertEqual(pipeline.validate_reading_plan_rows(rows, REFERENCE_ROOT), [])
        rows[0]["text_sha256"] = "0" * 64
        errors = pipeline.validate_reading_plan_rows(rows, REFERENCE_ROOT)
        self.assertTrue(any("checksum" in error or "SHA" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
