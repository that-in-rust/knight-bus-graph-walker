#!/usr/bin/env python3
"""Focused contract tests for the G07 constraint-transfer pipeline."""

from __future__ import annotations

import copy
import sys
import tempfile
import unittest
from pathlib import Path


TOOLS_DIR = Path(__file__).resolve().parents[1] / "tools"
sys.path.insert(0, str(TOOLS_DIR))

from g07_constraint_transfer_pipeline import (  # noqa: E402
    parse_transfer_card_file,
    validate_transfer_card_record,
    validate_transfer_plan_rows,
    write_transfer_card_markdown,
)


def claim(claim_type: str, text: str) -> dict[str, object]:
    value: dict[str, object] = {
        "claim_type": claim_type,
        "text": text,
        "assumptions": [],
        "uncertainty": "Bounded test fixture",
    }
    if claim_type == "SOURCE_CLAIM":
        value["source_pattern_ids"] = ["PAT-BOUND-SEARCH-CANDIDATE-FRONTIER"]
        value["source_pointer_ids"] = ["SP-001"]
    else:
        value["premises"] = ["PAT-BOUND-SEARCH-CANDIDATE-FRONTIER"]
    return value


def resource(expression: str) -> dict[str, object]:
    return {
        "claim_type": "DERIVED_INFERENCE",
        "expression": expression,
        "variables": [{"symbol": "n", "definition": "nodes", "units": "nodes"}],
        "unknown_constants": ["c_runtime"],
        "measurement_needed": "Calibrate c_runtime on the G09 fixture matrix.",
        "assumptions": ["The selected representation is used."],
        "uncertainty": "No G09 measurement exists.",
    }


def valid_card() -> dict[str, object]:
    source = claim("SOURCE_CLAIM", "Candidate count is explicitly bounded.")
    derived = claim("DERIVED_INFERENCE", "The bound remains operational on modern storage.")
    transfer = claim("SPECULATIVE_TRANSFER", "Expose the candidate cap in admission and receipts.")
    return {
        "transfer_id": "XFER-BOUND-SEARCH-FRONTIER-STATE",
        "name": "Bound Search Frontier State",
        "epistemic_label": "SPECULATIVE_TRANSFER",
        "source_pattern_ids": ["PAT-BOUND-SEARCH-CANDIDATE-FRONTIER"],
        "original_domain": "graph approximate-nearest-neighbor search",
        "original_constraint_profile": {
            "constrained_resource": source,
            "access_medium": source,
            "predictability_requirement": source,
            "data_mutability": source,
            "communication_model": source,
            "original_hardware_operating_assumptions": source,
        },
        "original_constraints": [source],
        "original_cost_model": source,
        "surviving_invariant": source,
        "reversed_assumptions": [derived],
        "modern_knight_bus_constraints": [transfer],
        "proposed_transfer": transfer,
        "modern_resource_model": {
            "ram": resource("RAM_peak = c_runtime + c_candidate * candidate_cap"),
            "io": resource("IO_bytes = pages_read * page_bytes"),
            "preprocessing": resource("T_prepare = records_scanned / scan_rate"),
            "storage": resource("Storage_bytes = graph_bytes + index_bytes"),
            "concurrency": resource("In_flight = worker_count * buffers_per_worker"),
        },
        "unknown_measurement_constants": ["c_candidate", "c_runtime", "scan_rate"],
        "g06_challenges": [
            {
                "failure_id": "FAIL-BOUNDED-FRONTIER-MISSES-NEAREST",
                "applies": True,
                "response": "Narrow to an explicit recall contract and exact fallback.",
            }
        ],
        "analogy_failure_modes": [transfer],
        "target_algorithm_families": ["NODESIMILARITY_KNN"],
        "smallest_falsifier": {
            "fixture": "A tiny graph where the nearest result requires exceeding the cap.",
            "controlled_variables": ["candidate_cap", "graph", "query"],
            "independent_oracle": "Exhaustive exact nearest-neighbor result.",
            "failure_signal": "The declared contract is violated or memory exceeds its bound.",
        },
        "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-BOUND-SEARCH-FRONTIER-STATE",
    }


class G07ConstraintTransferContractTests(unittest.TestCase):
    def test_valid_transfer_card_passes(self) -> None:
        self.assertEqual(
            [],
            validate_transfer_card_record(
                valid_card(),
                {"PAT-BOUND-SEARCH-CANDIDATE-FRONTIER"},
                {"FAIL-BOUNDED-FRONTIER-MISSES-NEAREST"},
                {"PAT-BOUND-SEARCH-CANDIDATE-FRONTIER": {"FAIL-BOUNDED-FRONTIER-MISSES-NEAREST"}},
            ),
        )

    def test_missing_surviving_invariant_fails(self) -> None:
        card = valid_card()
        card["surviving_invariant"] = {}
        errors = validate_transfer_card_record(
            card,
            {"PAT-BOUND-SEARCH-CANDIDATE-FRONTIER"},
            {"FAIL-BOUNDED-FRONTIER-MISSES-NEAREST"},
            {"PAT-BOUND-SEARCH-CANDIDATE-FRONTIER": {"FAIL-BOUNDED-FRONTIER-MISSES-NEAREST"}},
        )
        self.assertTrue(any("surviving_invariant" in error for error in errors))

    def test_unsymbolic_resource_model_fails(self) -> None:
        card = valid_card()
        card["modern_resource_model"]["ram"]["expression"] = "2.4x faster"
        errors = validate_transfer_card_record(
            card,
            {"PAT-BOUND-SEARCH-CANDIDATE-FRONTIER"},
            {"FAIL-BOUNDED-FRONTIER-MISSES-NEAREST"},
            {"PAT-BOUND-SEARCH-CANDIDATE-FRONTIER": {"FAIL-BOUNDED-FRONTIER-MISSES-NEAREST"}},
        )
        self.assertTrue(any("symbolic" in error for error in errors))

    def test_unanswered_g06_failure_fails(self) -> None:
        card = valid_card()
        card["g06_challenges"] = []
        errors = validate_transfer_card_record(
            card,
            {"PAT-BOUND-SEARCH-CANDIDATE-FRONTIER"},
            {"FAIL-BOUNDED-FRONTIER-MISSES-NEAREST"},
            {"PAT-BOUND-SEARCH-CANDIDATE-FRONTIER": {"FAIL-BOUNDED-FRONTIER-MISSES-NEAREST"}},
        )
        self.assertTrue(any("G06" in error for error in errors))

    def test_four_lanes_of_five_are_required(self) -> None:
        rows = []
        for rank in range(1, 21):
            rows.append(
                {
                    "selection_rank": str(rank),
                    "lane_id": "G07-LANE-" + str(((rank - 1) // 5) + 1),
                    "lane_position": str(((rank - 1) % 5) + 1),
                    "pattern_id": "PAT-TEST-PATTERN-NUMBER-" + str(rank),
                    "source_paper_ids": "PAPER-TEST",
                    "source_domain": (
                        "sparse-linear-algebra" if rank == 1 else "graph-domain-" + str(rank % 3)
                    ),
                    "selection_score": "90",
                    "selection_basis": "A007_BOUND+TEST",
                    "reader_agent_id": "UNASSIGNED",
                    "reviewer_agent_id": "UNASSIGNED",
                    "inspection_status": "FROZEN",
                    "terminal_disposition": "",
                    "transfer_ids": "",
                    "evidence_gap": "",
                    "measurement_needed": "",
                    "result_checksum": "",
                }
            )
        self.assertEqual([], validate_transfer_plan_rows(rows, {row["pattern_id"] for row in rows}, False))
        rows.pop()
        self.assertTrue(validate_transfer_plan_rows(rows, {row["pattern_id"] for row in rows}, False))

    def test_card_markdown_round_trips(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "XFER-BOUND-SEARCH-FRONTIER-STATE.md"
            write_transfer_card_markdown(path, valid_card())
            self.assertEqual(valid_card(), parse_transfer_card_file(path))

    def test_reserved_falsifier_matches_transfer(self) -> None:
        card = valid_card()
        card["falsifying_experiment_id"] = "RESERVED-G09-FOR-XFER-WRONG-REFERENCE-HERE-NOW"
        errors = validate_transfer_card_record(
            card,
            {"PAT-BOUND-SEARCH-CANDIDATE-FRONTIER"},
            {"FAIL-BOUNDED-FRONTIER-MISSES-NEAREST"},
            {"PAT-BOUND-SEARCH-CANDIDATE-FRONTIER": {"FAIL-BOUNDED-FRONTIER-MISSES-NEAREST"}},
        )
        self.assertTrue(any("falsifying_experiment_id" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
