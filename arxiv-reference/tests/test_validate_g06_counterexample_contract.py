#!/usr/bin/env python3
"""RED-first tests for bounded G06 counterexample extraction."""

from __future__ import annotations

import copy
import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_ROOT = REPOSITORY_ROOT / "arxiv-reference"
PIPELINE_PATH = REFERENCE_ROOT / "tools" / "g06_counterexample_pipeline.py"
VALIDATOR_PATH = REFERENCE_ROOT / "tools" / "validate_arxiv_corpus_contract.py"


def load_g06_pipeline_module():
    """Load the G06 pipeline after its availability RED has been satisfied."""

    spec = importlib.util.spec_from_file_location(
        "g06_counterexample_pipeline", PIPELINE_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load G06 counterexample pipeline")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_corpus_validator_module():
    """Load the shared corpus validator for lifecycle integration tests."""

    spec = importlib.util.spec_from_file_location("g06_corpus_validator", VALIDATOR_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load shared corpus validator")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def create_valid_failure_fixture() -> dict[str, object]:
    """Create one minimal source-reported failure-card fixture."""

    source_claim = {
        "claim_type": "SOURCE_CLAIM",
        "text": "Packed operations lose their stated advantage when fields exceed the word-capacity condition.",
        "source_pointer_ids": ["FP-001"],
        "premises": [],
        "assumptions": [],
        "uncertainty": "The source gives an analytical boundary rather than a measured crossover.",
    }
    return {
        "failure_id": "FAIL-WIDE-FIELDS-ERASE-PACKING",
        "name": "Wide Fields Erase Packing",
        "epistemic_label": "SOURCE_CLAIM",
        "failure_basis": "SOURCE_REPORTED",
        "source_paper_ids": ["PAPER-0708.3259"],
        "source_pointers": [
            {
                "pointer_id": "FP-001",
                "paper_id": "PAPER-0708.3259",
                "page": 10,
                "locator_type": "SECTION",
                "locator_value": "Section 3.1 and Lemma 3",
                "claim_scope": "Word-capacity condition for packed field operations.",
            }
        ],
        "broken_assumption": dict(source_claim),
        "triggering_workload": dict(source_claim),
        "observable_symptom": dict(source_claim),
        "breakpoint_equation": {
            "claim_type": "DERIVED_INFERENCE",
            "expression": "UNKNOWN",
            "variables": [
                {
                    "symbol": "field_bits",
                    "definition": "bits required by one packed field",
                    "units": "bits",
                }
            ],
            "numeric_constants": [],
            "source_pointer_ids": ["FP-001"],
            "premises": ["The source conditions packed operations on word capacity."],
            "assumptions": ["Implementation packing follows the source model."],
            "uncertainty": "No measured numeric crossover is reported.",
            "measurement_needed": "Measure operation time while increasing field_bits on fixed hardware.",
        },
        "affected_pattern_ids": ["PAT-BALANCE-BUCKETED-PACKED-SETS"],
        "affected_architecture_ids": [],
        "adversarial_fixture": {
            "claim_type": "DERIVED_INFERENCE",
            "fixture_name": "Packed width boundary",
            "fixture_kind": "GRAPH",
            "graph_shape": "Two equal-cardinality adjacency sets with widening identifiers.",
            "graph_scale": "Symbolic cardinality n with increasing field_bits.",
            "workload": "Repeated exact intersection of the two adjacency sets.",
            "controlled_variables": ["Set cardinality and overlap ratio."],
            "varied_variables": ["Identifier field width."],
            "independent_oracle": "Standard-library exact set intersection.",
            "expected_observation": "Correct output with packing speedup disappearing at an unknown width.",
            "source_pointer_ids": ["FP-001"],
            "premises": ["The source states a word-capacity condition."],
            "assumptions": ["The fixture isolates field width from set cardinality."],
            "uncertainty": "The performance crossover must be measured.",
        },
        "expected_failure_signal": dict(source_claim),
        "repair_options": [
            {
                "repair_class": "ADD_ADMISSION_GUARD",
                "description": "Refuse the packed kernel when identifiers exceed its verified width envelope.",
            }
        ],
        "confidence_rationale": {
            "claim_type": "DERIVED_INFERENCE",
            "text": "The qualitative boundary is sourced but its system crossover is unknown.",
            "source_pointer_ids": ["FP-001"],
            "premises": ["The source gives the packed-word precondition."],
            "assumptions": ["The cited condition applies to the represented adjacency IDs."],
            "uncertainty": "No G06 benchmark or implementation inspection occurred.",
        },
    }


def create_valid_plan_fixture() -> list[dict[str, str]]:
    """Create one paper row and one linked pattern row."""

    failure_id = "FAIL-WIDE-FIELDS-ERASE-PACKING"
    return [
        {
            "subject_type": "PAPER",
            "subject_rank": "1",
            "lane_id": "G06-LANE-1",
            "lane_position": "1",
            "subject_id": "PAPER-0708.3259",
            "source_paper_ids": "PAPER-0708.3259",
            "reader_agent_id": "reader-agent",
            "reviewer_agent_id": "reviewer-agent",
            "inspection_status": "COMPLETE",
            "terminal_disposition": "NEGATIVE_EVIDENCE_EXTRACTED",
            "failure_ids": failure_id,
            "evidence_gap": "",
            "measurement_needed": "",
            "reading_coverage": "ALL_PAGES:1-16",
            "result_checksum": "A" * 64,
        },
        {
            "subject_type": "PATTERN",
            "subject_rank": "1",
            "lane_id": "G06-LANE-1",
            "lane_position": "1",
            "subject_id": "PAT-BALANCE-BUCKETED-PACKED-SETS",
            "source_paper_ids": "PAPER-0708.3259",
            "reader_agent_id": "reader-agent",
            "reviewer_agent_id": "reviewer-agent",
            "inspection_status": "COMPLETE",
            "terminal_disposition": "SOURCE_FAILURE_LINKED",
            "failure_ids": failure_id,
            "evidence_gap": "",
            "measurement_needed": "",
            "reading_coverage": "PAPER_ROWS:PAPER-0708.3259",
            "result_checksum": "B" * 64,
        },
    ]


def create_valid_conflict_fixture() -> dict[str, str]:
    """Create one two-sided mechanism-versus-failure conflict row."""

    return {
        "conflict_id": "ECONFLICT-0001",
        "left_evidence_type": "MECHANISM_CARD",
        "left_evidence_id": "PAT-BALANCE-BUCKETED-PACKED-SETS",
        "right_evidence_type": "FAILURE_CARD",
        "right_evidence_id": "FAIL-WIDE-FIELDS-ERASE-PACKING",
        "conflict_type": "APPLICABILITY_DISAGREEMENT",
        "affected_pattern_ids": "PAT-BALANCE-BUCKETED-PACKED-SETS",
        "claim_scope": "Whether packed fields remain useful beyond the word-capacity envelope.",
        "rationale": "DERIVED_INFERENCE[premises=the mechanism requires packed-word capacity and the failure records its loss;assumptions=both claims use the same field-width definition;uncertainty=the measured crossover remains unknown]",
        "epistemic_label": "DERIVED_INFERENCE",
        "source_paper_ids": "PAPER-0708.3259",
        "source_pointer_ids": "FAIL-WIDE-FIELDS-ERASE-PACKING#FP-001|PAT-BALANCE-BUCKETED-PACKED-SETS#SP-001",
        "resolution_state": "OPEN",
    }


def create_valid_lane_dossier_fixture() -> dict[str, object]:
    """Create one two-subject semantic lane result for intake validation."""

    failure_id = "FAIL-WIDE-FIELDS-ERASE-PACKING"
    pattern_id = "PAT-BALANCE-BUCKETED-PACKED-SETS"
    paper_id = "PAPER-0708.3259"
    return {
        "schema_version": "G06-LANE-DOSSIER-V1",
        "lane_id": "G06-LANE-1",
        "paper_results": [
            {
                "subject_id": paper_id,
                "page_count": 16,
                "page_audit": [
                    {
                        "page": page,
                        "disposition": "NEGATIVE_EVIDENCE_FOUND"
                        if page == 10
                        else "NO_RELEVANT_NEGATIVE_EVIDENCE",
                        "evidence_keys": [failure_id] if page == 10 else [],
                    }
                    for page in range(1, 17)
                ],
                "terminal_disposition": "NEGATIVE_EVIDENCE_EXTRACTED",
                "proposed_failure_ids": [failure_id],
                "evidence_gap": "",
                "measurement_needed": "",
                "negative_evidence_notes": [
                    "Page 10 states the packed-word capacity condition."
                ],
            }
        ],
        "pattern_results": [
            {
                "subject_id": pattern_id,
                "source_paper_ids": [paper_id],
                "terminal_disposition": "SOURCE_FAILURE_LINKED",
                "proposed_failure_ids": [failure_id],
                "evidence_gap": "",
                "measurement_needed": "",
                "required_assumption": "Fields remain packable within the word model.",
                "smallest_violating_workload": "Two sets whose fields exceed that capacity.",
                "triggering_graph_property": "Identifiers require wider fields.",
                "unexpected_resource_term": "Multiword packing work.",
                "observable_symptom": "The stated packing advantage disappears.",
                "source_reported_breakpoint": "No measured crossover is reported.",
                "symbolic_breakpoint": "field_bits exceeds verified packed capacity.",
                "unknowns": "The machine-specific crossover is unknown.",
                "minimal_fixture": "Fixed-size sets with increasing identifier width.",
                "independent_oracle": "Exact standard-library set intersection.",
                "failure_effect": "SPECIALIZES",
                "related_mechanisms": [],
            }
        ],
        "failure_cards": [create_valid_failure_fixture()],
        "conflict_candidates": [],
        "coverage_audit": {
            "assigned_paper_ids": [paper_id],
            "completed_paper_ids": [paper_id],
            "assigned_pattern_ids": [pattern_id],
            "completed_pattern_ids": [pattern_id],
            "pages_expected": 16,
            "pages_inspected": 16,
            "missing_subject_ids": [],
            "duplicate_subject_ids": [],
            "network_requests": 0,
            "repository_edits": 0,
        },
        "lane_self_review": {
            "schema_valid_json": True,
            "all_subjects_terminal": True,
            "all_source_pointers_rechecked": True,
            "no_unsupported_numeric_breakpoints": True,
            "no_later_goal_artifacts": True,
            "known_uncertainties": ["Machine-specific crossover requires measurement."],
        },
    }


class ValidateG06CounterexampleContractTests(unittest.TestCase):
    """Exercise the frozen G06 failure and adversarial-coverage contracts."""

    def test_pipeline_module_exists(self) -> None:
        """REQ-G06-STUB: production validation begins only after observed RED."""

        self.assertTrue(
            PIPELINE_PATH.is_file(),
            "G06 counterexample pipeline is absent before GREEN implementation",
        )

    def test_valid_lane_dossier_passes_intake(self) -> None:
        """REQ-G06-PLAN-001: one complete owned lane dossier passes intake."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "validate_lane_dossier_record"),
            "semantic lane dossier validation is not implemented",
        )
        errors = pipeline.validate_lane_dossier_record(
            create_valid_lane_dossier_fixture(),
            create_valid_plan_fixture(),
            {"PAPER-0708.3259": 16},
            {"PAT-BALANCE-BUCKETED-PACKED-SETS"},
        )
        self.assertEqual(errors, [])

    def test_lane_dossier_requires_exact_subject_ownership(self) -> None:
        """REQ-G06-PLAN-001: a lane cannot omit or steal a mechanism."""

        pipeline = load_g06_pipeline_module()
        dossier = create_valid_lane_dossier_fixture()
        dossier["pattern_results"] = []
        errors = pipeline.validate_lane_dossier_record(
            dossier,
            create_valid_plan_fixture(),
            {"PAPER-0708.3259": 16},
            {"PAT-BALANCE-BUCKETED-PACKED-SETS"},
        )
        self.assertTrue(any("exactly cover owned patterns" in error for error in errors), errors)

    def test_lane_dossier_requires_every_page_once(self) -> None:
        """REQ-G06-PLAN-001: claimed full-paper reading binds every page."""

        pipeline = load_g06_pipeline_module()
        dossier = create_valid_lane_dossier_fixture()
        dossier["paper_results"][0]["page_audit"].pop()
        dossier["coverage_audit"]["pages_inspected"] -= 1
        errors = pipeline.validate_lane_dossier_record(
            dossier,
            create_valid_plan_fixture(),
            {"PAPER-0708.3259": 16},
            {"PAT-BALANCE-BUCKETED-PACKED-SETS"},
        )
        self.assertTrue(any("complete-page coverage" in error for error in errors), errors)

    def test_lane_dossier_rejects_unresolved_failure_link(self) -> None:
        """REQ-G06-LINK-001: dossier failure links resolve before integration."""

        pipeline = load_g06_pipeline_module()
        dossier = create_valid_lane_dossier_fixture()
        dossier["pattern_results"][0]["proposed_failure_ids"] = [
            "FAIL-MISSING-CARD-CANNOT-RESOLVE"
        ]
        errors = pipeline.validate_lane_dossier_record(
            dossier,
            create_valid_plan_fixture(),
            {"PAPER-0708.3259": 16},
            {"PAT-BALANCE-BUCKETED-PACKED-SETS"},
        )
        self.assertTrue(any("unresolved failure ID" in error for error in errors), errors)

    def test_lane_dossier_rejects_external_or_repo_activity(self) -> None:
        """REQ-G06-SCOPE-001: semantic lanes remain read-only and offline."""

        pipeline = load_g06_pipeline_module()
        dossier = create_valid_lane_dossier_fixture()
        dossier["coverage_audit"]["network_requests"] = 1
        errors = pipeline.validate_lane_dossier_record(
            dossier,
            create_valid_plan_fixture(),
            {"PAPER-0708.3259": 16},
            {"PAT-BALANCE-BUCKETED-PACKED-SETS"},
        )
        self.assertTrue(any("must be zero" in error for error in errors), errors)

    def test_lane_dossier_updates_plan_deterministically(self) -> None:
        """REQ-G06-PLAN-001: validated lane results map to terminal plan rows."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "apply_lane_dossier_to_plan_rows"),
            "lane-to-plan integration is not implemented",
        )
        rows = pipeline.apply_lane_dossier_to_plan_rows(
            create_valid_plan_fixture(),
            create_valid_lane_dossier_fixture(),
            "reader-agent-001",
        )
        self.assertEqual([row["inspection_status"] for row in rows], ["COMPLETE", "COMPLETE"])
        self.assertEqual(
            [row["reading_coverage"] for row in rows],
            ["ALL_PAGES:1-16", "PAPER_ROWS:PAPER-0708.3259"],
        )
        self.assertTrue(all(row["reader_agent_id"] == "reader-agent-001" for row in rows))
        self.assertTrue(all(row["reviewer_agent_id"] == "PENDING" for row in rows))
        self.assertTrue(all(row["result_checksum"] == "PENDING" for row in rows))

    def test_failure_card_writer_is_byte_deterministic(self) -> None:
        """REQ-G06-CARD-001: canonical cards serialize reproducibly."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "write_failure_card_markdown"),
            "canonical failure-card serialization is not implemented",
        )
        card = create_valid_failure_fixture()
        with tempfile.TemporaryDirectory() as temporary_directory:
            first = Path(temporary_directory) / "first" / (card["failure_id"] + ".md")
            second = Path(temporary_directory) / "second" / (card["failure_id"] + ".md")
            pipeline.write_failure_card_markdown(first, card)
            pipeline.write_failure_card_markdown(second, card)
            self.assertEqual(first.read_bytes(), second.read_bytes())
            self.assertEqual(pipeline.parse_failure_card_file(first), card)

    def test_finalized_plan_binds_reviewer_and_checksums(self) -> None:
        """REQ-G06-CHK-001: final review identity and evidence bytes are bound."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "finalize_adversarial_plan_rows"),
            "terminal plan finalization is not implemented",
        )
        card = create_valid_failure_fixture()
        cards = {card["failure_id"]: card}
        rows = pipeline.apply_lane_dossier_to_plan_rows(
            create_valid_plan_fixture(),
            create_valid_lane_dossier_fixture(),
            "reader-agent-001",
        )
        source_hashes = {"PAPER-0708.3259": ("A" * 64, "B" * 64)}
        finalized = pipeline.finalize_adversarial_plan_rows(
            rows, cards, source_hashes, "reviewer-agent-001"
        )
        self.assertTrue(
            all(row["reviewer_agent_id"] == "reviewer-agent-001" for row in finalized)
        )
        self.assertEqual(
            pipeline.validate_adversarial_result_checksums(
                finalized, cards, source_hashes
            ),
            [],
        )

    def test_lane_collection_rejects_duplicate_ownership(self) -> None:
        """REQ-G06-PLAN-001: each deterministic lane appears exactly once."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "validate_lane_dossier_collection"),
            "multi-lane dossier validation is not implemented",
        )
        dossier = create_valid_lane_dossier_fixture()
        errors = pipeline.validate_lane_dossier_collection(
            [dossier, copy.deepcopy(dossier)],
            create_valid_plan_fixture(),
            {"PAPER-0708.3259": 16},
            {"PAT-BALANCE-BUCKETED-PACKED-SETS"},
        )
        self.assertTrue(any("duplicate lane" in error for error in errors), errors)

    def test_cross_lane_exact_failure_duplicates_merge(self) -> None:
        """REQ-G06-DUP-001: identical rediscoveries retain one canonical card."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "merge_failure_card_records"),
            "cross-lane failure-card merge is not implemented",
        )
        card = create_valid_failure_fixture()
        merged, errors = pipeline.merge_failure_card_records(
            [("G06-LANE-1", card), ("G06-LANE-2", copy.deepcopy(card))]
        )
        self.assertEqual(errors, [])
        self.assertEqual(set(merged), {card["failure_id"]})

    def test_cross_lane_conflicting_failure_id_rejected(self) -> None:
        """REQ-G06-DUP-001: one ID cannot hide materially different failures."""

        pipeline = load_g06_pipeline_module()
        first = create_valid_failure_fixture()
        second = copy.deepcopy(first)
        second["triggering_workload"]["text"] = "A materially different trigger."
        _, errors = pipeline.merge_failure_card_records(
            [("G06-LANE-1", first), ("G06-LANE-2", second)]
        )
        self.assertTrue(any("conflicting duplicate failure_id" in error for error in errors), errors)

    def test_lane_dossier_file_requires_json_object(self) -> None:
        """REQ-G06-PLAN-001: controller parses one bounded JSON dossier."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "parse_lane_dossier_file"),
            "lane dossier file parsing is not implemented",
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "lane.json"
            path.write_text(
                json.dumps(create_valid_lane_dossier_fixture()), encoding="utf-8"
            )
            self.assertEqual(
                pipeline.parse_lane_dossier_file(path),
                create_valid_lane_dossier_fixture(),
            )
            path.write_text("[]", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "JSON object"):
                pipeline.parse_lane_dossier_file(path)

    def test_validated_lane_integration_preserves_review_boundary(self) -> None:
        """REQ-G06-CHK-001: controller integrates evidence before review binding."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "integrate_lane_dossier_records"),
            "validated multi-lane integration is not implemented",
        )
        rows, cards, conflicts = pipeline.integrate_lane_dossier_records(
            [create_valid_lane_dossier_fixture()],
            create_valid_plan_fixture(),
            {"PAPER-0708.3259": 16},
            {"PAT-BALANCE-BUCKETED-PACKED-SETS"},
            {"G06-LANE-1": "reader-agent-001"},
        )
        self.assertEqual(set(cards), {"FAIL-WIDE-FIELDS-ERASE-PACKING"})
        self.assertEqual(conflicts, [])
        self.assertTrue(all(row["inspection_status"] == "COMPLETE" for row in rows))
        self.assertTrue(all(row["reviewer_agent_id"] == "PENDING" for row in rows))
        self.assertTrue(all(row["result_checksum"] == "PENDING" for row in rows))

    def test_counterexample_report_requires_auditable_handoff(self) -> None:
        """REQ-G06-REPORT-001: report binds counts, scope, and decision yield."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "validate_counterexample_report"),
            "G06 report validation is not implemented",
        )
        headings = "\n\n".join(
            heading + "\n\nEvidence."
            for heading in pipeline.G06_REPORT_HEADINGS
        )
        report = (
            "# G06 Counterexample Report\n\n"
            + headings
            + "\n\nPapers inspected: 1\nPages inspected: 16\nPatterns disposed: 1\n"
            + "Failure cards: 1\nEvidence conflicts: 0\nExplicit evidence gaps: 0\n"
            + "No qualifying two-sided evidence conflict was found.\n"
        )
        self.assertEqual(
            pipeline.validate_counterexample_report(
                report,
                create_valid_plan_fixture(),
                {"FAIL-WIDE-FIELDS-ERASE-PACKING": create_valid_failure_fixture()},
                [],
                {"PAPER-0708.3259": 16},
            ),
            [],
        )
        errors = pipeline.validate_counterexample_report(
            report + "\nARCH-FORBIDDEN-0001 improved RAM by 50 percent.\n",
            create_valid_plan_fixture(),
            {"FAIL-WIDE-FIELDS-ERASE-PACKING": create_valid_failure_fixture()},
            [],
            {"PAPER-0708.3259": 16},
        )
        self.assertTrue(any("later-goal or measured-performance" in error for error in errors), errors)

    def test_canonical_review_repairs_are_preserved(self) -> None:
        """REQ-G06-REV-002: reviewed relabels and semantic merges stay fixed."""

        pipeline = load_g06_pipeline_module()
        snapshot = pipeline.derive_g06_corpus_records(REFERENCE_ROOT)
        failure_directory = REFERENCE_ROOT / "evidence" / "failure-cards"
        cards = {
            path.stem: pipeline.parse_failure_card_file(path)
            for path in sorted(failure_directory.glob("FAIL-*.md"))
        }
        self.assertEqual(len(cards), 79)
        semantic_merges = {
            "FAIL-BINARY-QUANTIZATION-GEOMETRY-COLLAPSE":
                "FAIL-INCOMPATIBLE-GEOMETRY-COLLAPSES-RECALL",
            "FAIL-REORDERING-PREPROCESSING-DOMINATES-TRAVERSAL":
                "FAIL-FULL-REORDER-DOMINATES-TRAVERSAL",
        }
        for retired_id, canonical_id in semantic_merges.items():
            self.assertNotIn(retired_id, cards)
            self.assertIn(canonical_id, cards)

        source_supported = {
            "FAIL-COMPRESSED-FILTER-DROPS-NEIGHBOR",
            "FAIL-DENSE-COLUMN-EXCEEDS-MEMORY",
            "FAIL-EARLY-STOPPING-WEAKENS-GUARANTEES",
            "FAIL-FIXED-ITERATIONS-MISS-CONVERGENCE",
            "FAIL-PROGRESSIVE-BEAM-IO-EXPANSION",
            "FAIL-RESIDUAL-PRUNING-BREAKS-EXACTNESS",
            "FAIL-STAGE-MATERIALIZATION-EXCEEDS-MEMORY",
            "FAIL-STATIC-ENTRY-CACHE-SHIFT",
            "FAIL-SYMMETRIC-SETS-REVERSE-COST",
            "FAIL-WIDE-FIELDS-ERASE-PACKING",
        }
        for failure_id in source_supported:
            self.assertEqual(cards[failure_id]["failure_basis"], "SOURCE_SUPPORTED_DERIVATION")
            self.assertEqual(cards[failure_id]["epistemic_label"], "DERIVED_INFERENCE")
        frontier = cards["FAIL-FRONTIER-HEURISTIC-MISSELECTS-DIRECTION"]
        self.assertEqual(frontier["failure_basis"], "ANALYTICAL_COUNTEREXAMPLE")
        self.assertEqual(frontier["epistemic_label"], "DERIVED_INFERENCE")

        aging_query = cards["FAIL-AGING-SUSPENDS-STREAM-QUERIES"]
        aging_pages = {pointer["page"] for pointer in aging_query["source_pointers"]}
        self.assertTrue({12, 13}.issubset(aging_pages))
        aging_pointer_ids = {
            pointer["pointer_id"]
            for pointer in aging_query["source_pointers"]
            if pointer["page"] in {12, 13}
        }
        for field_name in (
            "triggering_workload",
            "observable_symptom",
            "expected_failure_signal",
        ):
            self.assertTrue(
                aging_pointer_ids.intersection(
                    aging_query[field_name]["source_pointer_ids"]
                )
            )

        bounded_frontier = cards["FAIL-BOUNDED-FRONTIER-MISSES-NEAREST"]
        trigger = bounded_frontier["triggering_workload"]
        self.assertEqual(trigger["claim_type"], "DERIVED_INFERENCE")
        self.assertTrue(trigger["premises"])
        self.assertTrue(trigger["assumptions"])
        self.assertTrue(trigger["uncertainty"])

        with (REFERENCE_ROOT / "governance" / "g06-adversarial-plan.tsv").open(
            encoding="utf-8", newline=""
        ) as handle:
            plan_rows = list(csv.DictReader(handle, delimiter="\t"))
        serialized_plan = json.dumps(plan_rows, sort_keys=True)
        for retired_id in semantic_merges:
            self.assertNotIn(retired_id, serialized_plan)
        self.assertEqual(
            pipeline.validate_failure_card_collection(
                list(cards.values()),
                snapshot["paper_page_counts"],
                set(snapshot["pattern_cards"]),
            ),
            [],
        )

    def test_reviewed_semantic_merge_aliases_are_auditable(self) -> None:
        """REQ-G06-DUP-002: retired lane IDs resolve to canonical cards."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "validate_reviewed_semantic_merge_resolution"),
            "reviewed semantic merge validation is not implemented",
        )
        failure_directory = REFERENCE_ROOT / "evidence" / "failure-cards"
        cards = {
            path.stem: pipeline.parse_failure_card_file(path)
            for path in sorted(failure_directory.glob("FAIL-*.md"))
        }
        with (REFERENCE_ROOT / "governance" / "g06-adversarial-plan.tsv").open(
            encoding="utf-8", newline=""
        ) as handle:
            plan_rows = list(csv.DictReader(handle, delimiter="\t"))
        report = (REFERENCE_ROOT / "sources" / "G06-counterexample-report.md").read_text(
            encoding="utf-8"
        )
        self.assertEqual(
            pipeline.validate_reviewed_semantic_merge_resolution(
                cards, plan_rows, report
            ),
            [],
        )

    def test_full_validator_supports_g06(self) -> None:
        """REQ-G06-SCOPE-001: the shared validator recognizes active G06."""

        validator = load_corpus_validator_module()
        self.assertEqual(validator.run_corpus_contract_checks(REFERENCE_ROOT), [])

    def test_full_validator_routes_g06_report_validation(self) -> None:
        """REQ-G06-REPORT-001: shared validation includes the G06 report."""

        validator = load_corpus_validator_module()
        pipeline = load_g06_pipeline_module()
        sentinel = "sources/G06-counterexample-report.md: sentinel report failure"
        with mock.patch.object(
            pipeline, "validate_counterexample_report", return_value=[sentinel]
        ) as report_validator:
            with mock.patch.object(
                validator, "load_g06_pipeline_module", return_value=pipeline
            ):
                errors = validator.run_corpus_contract_checks(REFERENCE_ROOT)

        self.assertIn(sentinel, errors)
        report_validator.assert_called_once()

    def test_failure_envelope_is_canonical(self) -> None:
        """REQ-G06-CARD-001: one complete canonical card validates."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "validate_failure_card_record"),
            "valid failure-card validation is not implemented",
        )
        errors = pipeline.validate_failure_card_record(
            create_valid_failure_fixture(),
            {
                "PAPER-0708.3259": 16,
            },
            {"PAT-BALANCE-BUCKETED-PACKED-SETS"},
        )
        self.assertEqual(errors, [])

    def test_unknown_failure_field_rejected(self) -> None:
        """REQ-G06-CARD-001: unknown envelope fields fail closed."""

        pipeline = load_g06_pipeline_module()
        card = create_valid_failure_fixture()
        card["architecture_recommendation"] = "Use this design"
        errors = pipeline.validate_failure_card_record(
            card, {"PAPER-0708.3259": 16}, {"PAT-BALANCE-BUCKETED-PACKED-SETS"}
        )
        self.assertTrue(any("unknown top-level" in error for error in errors), errors)

    def test_source_failure_requires_pointer(self) -> None:
        """REQ-G06-PTR-001: source failures need exact local pointers."""

        pipeline = load_g06_pipeline_module()
        card = create_valid_failure_fixture()
        card["source_pointers"] = []
        errors = pipeline.validate_failure_card_record(
            card, {"PAPER-0708.3259": 16}, {"PAT-BALANCE-BUCKETED-PACKED-SETS"}
        )
        self.assertTrue(any("source pointer" in error for error in errors), errors)

    def test_abstract_source_pointer_is_rejected(self) -> None:
        """REQ-G06-PTR-001: abstracts and titles are not evidence locators."""

        pipeline = load_g06_pipeline_module()
        card = create_valid_failure_fixture()
        card["source_pointers"][0]["locator_value"] = "Abstract, applicability boundary"
        errors = pipeline.validate_failure_card_record(
            card, {"PAPER-0708.3259": 16}, {"PAT-BALANCE-BUCKETED-PACKED-SETS"}
        )
        self.assertTrue(any("abstract or title" in error for error in errors), errors)

    def test_derived_failure_stays_unmeasured(self) -> None:
        """REQ-G06-EPI-001: analytical cards cannot masquerade as source reports."""

        pipeline = load_g06_pipeline_module()
        card = create_valid_failure_fixture()
        card["failure_basis"] = "ANALYTICAL_COUNTEREXAMPLE"
        errors = pipeline.validate_failure_card_record(
            card, {"PAPER-0708.3259": 16}, {"PAT-BALANCE-BUCKETED-PACKED-SETS"}
        )
        self.assertTrue(any("DERIVED_INFERENCE" in error for error in errors), errors)

    def test_failure_requires_affected_pattern(self) -> None:
        """REQ-G06-LINK-001: every failure attacks at least one mechanism."""

        pipeline = load_g06_pipeline_module()
        card = create_valid_failure_fixture()
        card["affected_pattern_ids"] = []
        errors = pipeline.validate_failure_card_record(
            card, {"PAPER-0708.3259": 16}, {"PAT-BALANCE-BUCKETED-PACKED-SETS"}
        )
        self.assertTrue(any("affected_pattern_ids" in error for error in errors), errors)

    def test_failure_requires_known_pattern(self) -> None:
        """REQ-G06-LINK-001: affected pattern foreign keys resolve."""

        pipeline = load_g06_pipeline_module()
        card = create_valid_failure_fixture()
        card["affected_pattern_ids"] = ["PAT-NOT-A-REAL-PATTERN"]
        errors = pipeline.validate_failure_card_record(
            card, {"PAPER-0708.3259": 16}, {"PAT-BALANCE-BUCKETED-PACKED-SETS"}
        )
        self.assertTrue(any("unknown pattern" in error for error in errors), errors)

    def test_numeric_breakpoint_requires_support(self) -> None:
        """REQ-G06-BREAK-001: numeric literals require explicit provenance."""

        pipeline = load_g06_pipeline_module()
        card = create_valid_failure_fixture()
        card["breakpoint_equation"]["expression"] = "field_bits > 10"
        errors = pipeline.validate_failure_card_record(
            card, {"PAPER-0708.3259": 16}, {"PAT-BALANCE-BUCKETED-PACKED-SETS"}
        )
        self.assertTrue(any("numeric literal" in error for error in errors), errors)

    def test_fixture_requires_independent_oracle(self) -> None:
        """REQ-G06-FIX-001: a fixture needs an independent correctness oracle."""

        pipeline = load_g06_pipeline_module()
        card = create_valid_failure_fixture()
        card["adversarial_fixture"]["independent_oracle"] = ""
        errors = pipeline.validate_failure_card_record(
            card, {"PAPER-0708.3259": 16}, {"PAT-BALANCE-BUCKETED-PACKED-SETS"}
        )
        self.assertTrue(any("independent_oracle" in error for error in errors), errors)

    def test_failure_signal_is_observable(self) -> None:
        """REQ-G06-FIX-001: the expected failure signal cannot be empty."""

        pipeline = load_g06_pipeline_module()
        card = create_valid_failure_fixture()
        card["expected_failure_signal"]["text"] = ""
        errors = pipeline.validate_failure_card_record(
            card, {"PAPER-0708.3259": 16}, {"PAT-BALANCE-BUCKETED-PACKED-SETS"}
        )
        self.assertTrue(any("expected_failure_signal" in error for error in errors), errors)

    def test_repair_options_avoid_decision(self) -> None:
        """REQ-G06-REPAIR-001: G06 repair options cannot choose rejection."""

        pipeline = load_g06_pipeline_module()
        card = create_valid_failure_fixture()
        card["repair_options"] = [
            {"repair_class": "REJECT", "description": "Reject the architecture."}
        ]
        errors = pipeline.validate_failure_card_record(
            card, {"PAPER-0708.3259": 16}, {"PAT-BALANCE-BUCKETED-PACKED-SETS"}
        )
        self.assertTrue(any("repair_class" in error for error in errors), errors)

    def test_duplicate_failures_ignore_names(self) -> None:
        """REQ-G06-DUP-001: renaming an equal failure does not hide duplication."""

        pipeline = load_g06_pipeline_module()
        first = create_valid_failure_fixture()
        second = copy.deepcopy(first)
        second["failure_id"] = "FAIL-PACKING-WIDTH-BREAKS-SPEED"
        second["name"] = "Packing Width Breaks Speed"
        self.assertTrue(
            hasattr(pipeline, "validate_failure_card_collection"),
            "duplicate-signature validation is not implemented",
        )
        errors = pipeline.validate_failure_card_collection(
            [first, second],
            {"PAPER-0708.3259": 16},
            {"PAT-BALANCE-BUCKETED-PACKED-SETS"},
        )
        self.assertTrue(any("duplicate failure signature" in error for error in errors), errors)

    def test_plan_covers_all_subjects(self) -> None:
        """REQ-G06-PLAN-001: exact paper and pattern coverage validates."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "validate_adversarial_plan_rows"),
            "adversarial-plan validation is not implemented",
        )
        errors = pipeline.validate_adversarial_plan_rows(
            create_valid_plan_fixture(),
            {"PAPER-0708.3259": 16},
            {"PAT-BALANCE-BUCKETED-PACKED-SETS": ["PAPER-0708.3259"]},
            {
                "FAIL-WIDE-FIELDS-ERASE-PACKING": create_valid_failure_fixture(),
            },
            True,
        )
        self.assertEqual(errors, [])

    def test_missing_disposition_is_rejected(self) -> None:
        """REQ-G06-PLAN-001: every completed pattern needs a terminal disposition."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "validate_adversarial_plan_rows"),
            "adversarial-plan validation is not implemented",
        )
        rows = create_valid_plan_fixture()
        rows[1]["terminal_disposition"] = "PENDING"
        errors = pipeline.validate_adversarial_plan_rows(
            rows,
            {"PAPER-0708.3259": 16},
            {"PAT-BALANCE-BUCKETED-PACKED-SETS": ["PAPER-0708.3259"]},
            {"FAIL-WIDE-FIELDS-ERASE-PACKING": create_valid_failure_fixture()},
            True,
        )
        self.assertTrue(any("terminal disposition" in error for error in errors), errors)

    def test_plan_links_are_bidirectional(self) -> None:
        """REQ-G06-LINK-001: plan and failure-card pattern links agree."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "validate_adversarial_plan_rows"),
            "adversarial-plan validation is not implemented",
        )
        failure = create_valid_failure_fixture()
        failure["affected_pattern_ids"] = ["PAT-PROBE-SMALLEST-SET-FIRST"]
        errors = pipeline.validate_adversarial_plan_rows(
            create_valid_plan_fixture(),
            {"PAPER-0708.3259": 16},
            {"PAT-BALANCE-BUCKETED-PACKED-SETS": ["PAPER-0708.3259"]},
            {"FAIL-WIDE-FIELDS-ERASE-PACKING": failure},
            True,
        )
        self.assertTrue(any("does not affect pattern" in error for error in errors), errors)

    def test_plan_rejects_missing_card_to_pattern_inverse_link(self) -> None:
        """REQ-G06-LINK-001: every affected pattern links back to the card."""

        pipeline = load_g06_pipeline_module()
        rows = create_valid_plan_fixture()
        second_pattern = "PAT-PROBE-SMALLEST-SET-FIRST"
        second_row = copy.deepcopy(rows[1])
        second_row.update(
            {
                "subject_rank": "2",
                "lane_id": "G06-LANE-2",
                "subject_id": second_pattern,
                "terminal_disposition": "EXPLICIT_EVIDENCE_GAP",
                "failure_ids": "",
                "evidence_gap": "No linked evidence was declared.",
                "measurement_needed": "Check whether an affected card exists.",
                "result_checksum": "C" * 64,
            }
        )
        rows.append(second_row)
        failure = create_valid_failure_fixture()
        failure["affected_pattern_ids"] = sorted(
            ["PAT-BALANCE-BUCKETED-PACKED-SETS", second_pattern]
        )
        errors = pipeline.validate_adversarial_plan_rows(
            rows,
            {"PAPER-0708.3259": 16},
            {
                "PAT-BALANCE-BUCKETED-PACKED-SETS": ["PAPER-0708.3259"],
                second_pattern: ["PAPER-0708.3259"],
            },
            {"FAIL-WIDE-FIELDS-ERASE-PACKING": failure},
            True,
        )
        self.assertTrue(any("missing inverse pattern link" in error for error in errors), errors)

    def test_pattern_disposition_must_match_failure_basis(self) -> None:
        """REQ-G06-LINK-001: source and analytical dispositions are honest."""

        pipeline = load_g06_pipeline_module()
        rows = create_valid_plan_fixture()
        failure = create_valid_failure_fixture()
        failure["failure_basis"] = "SOURCE_SUPPORTED_DERIVATION"
        failure["epistemic_label"] = "DERIVED_INFERENCE"
        errors = pipeline.validate_adversarial_plan_rows(
            rows,
            {"PAPER-0708.3259": 16},
            {"PAT-BALANCE-BUCKETED-PACKED-SETS": ["PAPER-0708.3259"]},
            {"FAIL-WIDE-FIELDS-ERASE-PACKING": failure},
            True,
        )
        self.assertTrue(any("does not match linked failure bases" in error for error in errors), errors)

    def test_valid_conflict_row_passes(self) -> None:
        """REQ-G06-CONFLICT-001: a complete two-sided conflict validates."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "validate_evidence_conflict_rows"),
            "evidence-conflict validation is not implemented",
        )
        mechanism = {
            "source_paper_ids": ["PAPER-0708.3259"],
            "source_pointers": [
                {"pointer_id": "SP-001", "paper_id": "PAPER-0708.3259"}
            ],
        }
        errors = pipeline.validate_evidence_conflict_rows(
            [create_valid_conflict_fixture()],
            {"PAT-BALANCE-BUCKETED-PACKED-SETS": mechanism},
            {"FAIL-WIDE-FIELDS-ERASE-PACKING": create_valid_failure_fixture()},
            {"PAPER-0708.3259"},
        )
        self.assertEqual(errors, [])

    def test_conflict_requires_both_sides(self) -> None:
        """REQ-G06-CONFLICT-001: an absent right side fails closed."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "validate_evidence_conflict_rows"),
            "evidence-conflict validation is not implemented",
        )
        conflict = create_valid_conflict_fixture()
        conflict["right_evidence_id"] = ""
        mechanism = {
            "source_paper_ids": ["PAPER-0708.3259"],
            "source_pointers": [
                {"pointer_id": "SP-001", "paper_id": "PAPER-0708.3259"}
            ],
        }
        errors = pipeline.validate_evidence_conflict_rows(
            [conflict],
            {"PAT-BALANCE-BUCKETED-PACKED-SETS": mechanism},
            {"FAIL-WIDE-FIELDS-ERASE-PACKING": create_valid_failure_fixture()},
            {"PAPER-0708.3259"},
        )
        self.assertTrue(any("right evidence" in error for error in errors), errors)

    def test_terminal_checksum_binds_evidence(self) -> None:
        """REQ-G06-CHK-001: linked-card changes invalidate terminal checksums."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "calculate_adversarial_result_checksum"),
            "terminal checksum calculation is not implemented",
        )
        self.assertTrue(
            hasattr(pipeline, "validate_adversarial_result_checksums"),
            "terminal checksum validation is not implemented",
        )
        rows = create_valid_plan_fixture()
        cards = {"FAIL-WIDE-FIELDS-ERASE-PACKING": create_valid_failure_fixture()}
        source_hashes = {"PAPER-0708.3259": ("C" * 64, "D" * 64)}
        for row in rows:
            row["result_checksum"] = pipeline.calculate_adversarial_result_checksum(
                row, cards, source_hashes
            )
        self.assertEqual(
            pipeline.validate_adversarial_result_checksums(rows, cards, source_hashes),
            [],
        )
        cards["FAIL-WIDE-FIELDS-ERASE-PACKING"]["expected_failure_signal"][
            "text"
        ] = "changed after binding"
        errors = pipeline.validate_adversarial_result_checksums(
            rows, cards, source_hashes
        )
        self.assertTrue(any("result_checksum mismatch" in error for error in errors), errors)

    def test_failure_card_file_parses(self) -> None:
        """REQ-G06-CARD-001: one fenced JSON envelope parses from Markdown."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "parse_failure_card_file"),
            "failure-card Markdown parsing is not implemented",
        )
        card = create_valid_failure_fixture()
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / (card["failure_id"] + ".md")
            path.write_text(
                "# " + card["name"] + "\n\n```json\n" + json.dumps(card) + "\n```\n",
                encoding="utf-8",
            )
            self.assertEqual(pipeline.parse_failure_card_file(path), card)

    def test_entry_corpus_remains_frozen(self) -> None:
        """REQ-G06-ENTRY-001: actual G05 inputs retain their exact entry state."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "derive_g06_corpus_records"),
            "G06 corpus derivation is not implemented",
        )
        self.assertTrue(
            hasattr(pipeline, "validate_g06_entry_corpus"),
            "G06 entry validation is not implemented",
        )
        snapshot = pipeline.derive_g06_corpus_records(REFERENCE_ROOT)
        self.assertEqual(snapshot["paper_count"], 25)
        self.assertEqual(snapshot["page_count"], 427)
        self.assertEqual(snapshot["pattern_count"], 67)
        self.assertEqual(snapshot["pattern_edge_count"], 47)
        self.assertEqual(pipeline.validate_g06_entry_corpus(REFERENCE_ROOT), [])

    def test_external_or_new_paper_rejected(self) -> None:
        """REQ-G06-SCOPE-001: identity or request growth fails the frozen snapshot."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "validate_frozen_input_snapshot"),
            "frozen-input validation is not implemented",
        )
        snapshot = pipeline.derive_g06_corpus_records(REFERENCE_ROOT)
        snapshot["manifest_row_count"] += 1
        snapshot["request_row_counts"]["metadata"] += 1
        errors = pipeline.validate_frozen_input_snapshot(snapshot)
        self.assertTrue(any("paper identities" in error for error in errors), errors)
        self.assertTrue(any("external request" in error for error in errors), errors)

    def test_later_goal_artifacts_rejected(self) -> None:
        """REQ-G06-SCOPE-001: G07 through G09 artifacts remain forbidden."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "validate_later_artifacts_absent"),
            "later-goal artifact validation is not implemented",
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            later_directory = root / "evidence" / "constraint-transfer-cards"
            later_directory.mkdir(parents=True)
            (later_directory / "XFER-BOUND-ACTIVE-WORKING-SET.md").write_text(
                "later goal", encoding="utf-8"
            )
            errors = pipeline.validate_later_artifacts_absent(root)
        self.assertTrue(any("later-goal artifact" in error for error in errors), errors)

    def test_initial_plan_is_deterministic(self) -> None:
        """REQ-G06-PLAN-001: the frozen 25-plus-67 plan is byte deterministic."""

        pipeline = load_g06_pipeline_module()
        self.assertTrue(
            hasattr(pipeline, "derive_initial_adversarial_rows"),
            "initial adversarial-plan derivation is not implemented",
        )
        self.assertTrue(
            hasattr(pipeline, "write_adversarial_plan_tsv"),
            "adversarial-plan serialization is not implemented",
        )
        rows = pipeline.derive_initial_adversarial_rows(REFERENCE_ROOT)
        self.assertEqual(len(rows), 92)
        self.assertEqual(sum(row["subject_type"] == "PAPER" for row in rows), 25)
        self.assertEqual(sum(row["subject_type"] == "PATTERN" for row in rows), 67)
        self.assertTrue(all(row["inspection_status"] == "PENDING" for row in rows))
        self.assertTrue(all(row["terminal_disposition"] == "PENDING" for row in rows))
        snapshot = pipeline.derive_g06_corpus_records(REFERENCE_ROOT)
        self.assertEqual(
            pipeline.validate_adversarial_plan_rows(
                rows,
                snapshot["paper_page_counts"],
                snapshot["pattern_source_papers"],
                {},
                False,
            ),
            [],
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            first = Path(temporary_directory) / "first.tsv"
            second = Path(temporary_directory) / "second.tsv"
            pipeline.write_adversarial_plan_tsv(first, rows)
            pipeline.write_adversarial_plan_tsv(second, rows)
            self.assertEqual(first.read_bytes(), second.read_bytes())

    def test_completion_requires_clear_review(self) -> None:
        """REQ-G06-REV-001: completion fails without zero-severity clearance."""

        validator = load_corpus_validator_module()
        completed_status = "\n".join(
            (
                "- Goal state: `COMPLETE`",
                "- Completion state: `COMPLETE`",
                "- Validation state: `VERIFIED`",
                "- Review state: `CLEARED`",
            )
        )
        errors = validator.validate_g06_review_clearance(completed_status, "")
        self.assertTrue(any("Final verdict" in error for error in errors), errors)
        self.assertTrue(any("P0=0, P1=0, P2=0" in error for error in errors), errors)
        self.assertEqual(
            validator.validate_g06_review_clearance(
                completed_status,
                "\n".join(
                    (
                        "- Final verdict: `CLEARED`",
                        "**Unresolved findings: P0=0, P1=0, P2=0.**",
                        "G06 is **CLEARED**.",
                    )
                ),
            ),
            [],
        )


if __name__ == "__main__":
    unittest.main()
