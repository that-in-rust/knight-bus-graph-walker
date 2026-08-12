#!/usr/bin/env python3
"""Focused contract tests for the G08 architecture-evolution arena."""

from __future__ import annotations

import copy
import sys
import tempfile
import unittest
from pathlib import Path


TOOLS_DIR = Path(__file__).resolve().parents[1] / "tools"
sys.path.insert(0, str(TOOLS_DIR))

from g08_architecture_evolution_pipeline import (  # noqa: E402
    parse_candidate_card_file,
    validate_candidate_collection,
    validate_candidate_record,
    validate_candidate_plan_rows,
    validate_pareto_rows,
    write_candidate_card_markdown,
)


def symbolic(expression: str) -> dict[str, object]:
    return {
        "expression": expression,
        "variables": [{"symbol": "N", "definition": "node count", "units": "nodes"}],
        "unknown_constants": ["c_runtime"],
        "assumptions": ["The admitted artifact variant is used."],
        "double_counting_risks": ["Runtime reserve may overlap allocator reserve."],
        "measurement_needed": "Calibrate c_runtime in G09.",
        "uncertainty": "No G09 measurement exists.",
    }


def valid_candidate(candidate_id: str = "ARCH-G08-001") -> dict[str, object]:
    ram_terms = {
        key: symbolic(f"RAM_{key} = c_{key} * N")
        for key in (
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
        )
    }
    return {
        "candidate_id": candidate_id,
        "name": "Bounded Frontier Stream Capsule",
        "epistemic_label": "SPECULATIVE_TRANSFER",
        "primary_niche": "LOWEST_RAM",
        "secondary_niches": ["HIGHEST_PREDICTABILITY"],
        "parents": [],
        "generation_lineage": {
            "lane_id": "G08-LANE-RAM",
            "lane_position": 1,
            "generator_identity": "TEST-FIXTURE",
            "prompt_id": "G08-DIVERGENT-V1",
            "generated_at": "2026-08-12T00:00:00Z",
            "added_transfer_ids": ["XFER-BOUND-SEARCH-FRONTIER-STATE"],
            "removed_transfer_ids": [],
            "intended_changed_behavior": "Bound the live traversal frontier before execution.",
        },
        "inherited_transfer_ids": ["XFER-BOUND-SEARCH-FRONTIER-STATE"],
        "target_workload_contract": {
            "artifact": "sorted adjacency blocks plus bounded frontier runs",
            "algorithm_family": "DEPENDENCY_SECURITY_ACCESS_PATH",
            "algorithm_semantics": "Exact directed reachability with deterministic path witness ordering.",
            "exactness_requirement": "EXACT",
            "ram_ceiling": "B_user",
            "storage_allowance": "S_user",
            "deadline_model": "D_user or no deadline",
            "output_bound": "O_user",
            "a007_decision_strengthened": "Admit, stream, spill, reference, or refuse before traversal.",
            "first_wedge": True,
        },
        "genome": {
            "topology_layout": "Sorted immutable adjacency blocks.",
            "prepared_artifact_variants": ["forward-only", "forward-plus-reverse"],
            "ordering_and_identifier_representation": "Dense local IDs with stable external-ID map.",
            "algorithm_state_placement": "Visited bitmap resident; overflow frontier external.",
            "scheduling_and_concurrency": "Deterministic level barriers with admitted worker count.",
            "overflow_behavior": "Spill frontier runs, then reference fallback, then refuse.",
            "exactness": "Exact within declared direction and path semantics.",
            "admission_model": "Admit only when the minimum resident kernel plus reserve fits B_user.",
            "receipt_model": "Report admitted and observed state counts and bytes.",
            "compatibility_boundary": "Bolt/Cypher adapter accepts only the declared traversal subset.",
        },
        "minimum_resident_kernel": {
            "admission_unit": "one visited bitmap plus one frontier block per worker",
            "expression": "RAM_kernel = RAM_visited + W * RAM_frontier_block + RAM_runtime",
            "refusal_condition": "RAM_kernel + RAM_safety_margin > B_user",
        },
        "resource_model": {
            "peak_ram_equation": "RAM_peak = RAM_topology + RAM_algorithm_state + RAM_frontier_active_set + RAM_scratch + RAM_output + RAM_conversion + RAM_page_cache_or_direct_io + RAM_runtime_overhead + RAM_spill + RAM_safety_margin + RAM_temporary_artifact_coexistence",
            "ram_terms": ram_terms,
            "io": symbolic("IO_total = IO_topology + IO_frontier_spill + IO_output"),
            "preprocessing": symbolic("T_prepare = T_scan + T_sort + T_emit + T_verify"),
            "storage": symbolic("Storage_total = S_topology + S_id_map + S_manifest"),
            "recomputation": symbolic("Work_recompute = passes_replayed * edges_replayed"),
            "concurrency": symbolic("RAM_concurrency = W * RAM_worker_private + Q * RAM_query_private"),
        },
        "state_multiplicity": {
            "workers": "W * RAM_worker_private",
            "queries": "Q * RAM_query_private",
            "partitions": "P_active * RAM_partition_state",
            "stages": "max_stage(RAM_stage_i), not sum unless stages coexist",
            "io_depth": "D_io * RAM_io_buffer",
        },
        "page_cache_direct_io_policy": {
            "mode": "DIRECT_IO_OR_EXPLICIT_PAGE_CACHE_ACCOUNTING",
            "equation": "RAM_io_accounted = RAM_direct_buffers + RAM_kernel_io + RAM_page_cache_budget",
            "guard": "Never count mmap bytes as free and never assume page-cache residency is zero.",
        },
        "preparation_model": {
            "artifact_build_phases": ["scan", "sort", "emit", "verify"],
            "build_peak_ram": "RAM_build_peak = max(RAM_scan, RAM_sort, RAM_emit, RAM_verify) + RAM_runtime",
            "build_io": "IO_build = IO_scan + IO_runs + IO_merge + IO_emit + IO_verify",
            "persistent_bytes": "S_persistent = S_topology + S_id_map + S_manifest",
            "temporary_bytes": "S_temporary_peak = S_old + S_new + S_runs + S_journal",
            "freshness_model": "Immutable epoch with atomic manifest swap.",
            "amortization_assumptions": ["At least Q_amortized admitted queries reuse the artifact."],
            "shared_layout_baseline_comparison": "Specialization adds frontier spill metadata but may omit reverse topology.",
        },
        "fallback_ladder": ["FIT", "LOWER_CONCURRENCY", "STREAM", "SPILL", "REFERENCE", "REFUSE"],
        "correctness_and_determinism": {
            "exactness": "EXACT",
            "numerical_tolerance": "NOT_APPLICABLE",
            "seed_policy": "NOT_APPLICABLE",
            "ordering_guarantee": "Stable level then external-ID order.",
            "nondeterministic_sources": ["I/O completion order, removed by barrier and stable merge."],
            "oracle_strategy": "Compare result set and canonical witness against a reference traversal.",
            "refusal_rule": "Refuse when exact semantics cannot fit or spill within B_user and S_user.",
        },
        "compatibility_boundary": {
            "neo4j": "Input and output semantics only; no internal store compatibility.",
            "cypher": "Bounded MATCH path subset compiled to the capsule.",
            "bolt": "Request/record/error framing for the supported subset.",
            "gds": "Procedure facade only for the declared algorithm and estimate/run modes.",
            "unsupported_behavior": "Return stable unsupported-surface error before execution.",
        },
        "crossover_guards": [
            {
                "guard": "Use spill only when predicted spill bytes fit S_user.",
                "unknown_constants": ["c_runtime", "spill_write_amplification"],
                "switch_action": "REFERENCE_OR_REFUSE",
            }
        ],
        "composition_review": {
            "invariant_compatibility": "Frontier capping does not alter exact result membership when spill is lossless.",
            "memory_term_overlap": "Worker frontier blocks are counted once in RAM_frontier_active_set, not again in scratch.",
            "prepared_artifact_coexistence": "Forward-only and dual-direction variants are separate manifests.",
            "access_pattern_conflicts": "Sorted IDs support sequential block access; random witness lookup may lose locality.",
            "fallback_composition": "A guard failure skips spill and selects reference or refusal.",
            "exactness_determinism_composition": "Lossless spill plus stable merge preserves exact deterministic output.",
            "preparation_rationality": "Reject specialization when Q_amortized is below the build-cost crossover.",
        },
        "receipt_fields": sorted([
            "plan_id",
            "budget_bytes",
            "predicted_peak_bytes",
            "observed_peak_rss_bytes",
            "spill_bytes",
            "fallback_taken",
            "oracle_status",
            "estimator_error_bytes",
        ]),
        "estimator_feedback": {
            "error_equation": "E_ram = RAM_observed_peak - RAM_predicted_peak",
            "calibration_action": "Update constants only from versioned G09 measurements.",
            "safety_action": "Increase reserve or refuse when upper error quantile exceeds policy.",
        },
        "linked_g06_failure_ids": ["FAIL-BOUNDED-FRONTIER-MISSES-NEAREST"],
        "g06_challenge_responses": [
            {
                "failure_id": "FAIL-BOUNDED-FRONTIER-MISSES-NEAREST",
                "applies": False,
                "response": "This candidate is exact traversal, not capped nearest-neighbor recall; retain as a non-applicable boundary check.",
            }
        ],
        "adversarial_review": {
            "loaded_after_raw_portfolio_freeze": True,
            "raw_portfolio_freeze_id": "G08-RAW-50-SHA256-TEST",
            "reviewer_identity": "TEST-CHALLENGER",
            "challenged_at": "2026-08-12T00:01:00Z",
            "failure_selection_basis": "Linked G07 transfer failures plus family-level G06 counterexamples.",
        },
        "loses_when": ["The graph changes faster than an immutable epoch can be rebuilt.", "The entire graph and state fit comfortably in the shared baseline."],
        "smallest_g09_falsifier": {
            "experiment_id": "RESERVED-G09-FOR-ARCH-G08-001",
            "fixture": "A layered graph whose frontier exceeds the resident cap.",
            "baseline": "Conservative Knight Bus reference traversal.",
            "oracle": "Canonical exact visited set and witness path.",
            "metrics": ["whole-process peak RSS", "spill bytes", "wall time", "result digest"],
            "modeled_expectation": "Observed counts remain within admitted symbolic terms after calibration.",
            "acceptance_thresholds": "To be set by G09 before execution; no threshold is claimed in G08.",
            "disconfirming_result": "Wrong result, unmodeled state, or an observed term outside the calibrated bound.",
        },
        "qualitative_pareto": {
            "peak_ram": "LOW_IF_SPILL_GUARD_HOLDS",
            "tail_latency_risk": "MEDIUM_TO_HIGH_FROM_EXTERNAL_FRONTIER",
            "predictability": "HIGH_AFTER_CALIBRATION",
            "preprocessing_cost": "MEDIUM",
            "persistent_storage_amplification": "LOW_TO_MEDIUM",
            "temporary_storage_peak": "MEDIUM",
            "exactness_determinism": "EXACT_DETERMINISTIC",
            "operational_complexity": "MEDIUM",
            "neo4j_adoption_friction": "LOW_FOR_SUPPORTED_SUBSET",
            "calibration_debt": "MEDIUM",
        },
        "highest_completed_stage": "INDEPENDENT_ADVERSARIAL_REVIEW",
        "terminal_disposition": "PARETO_SURVIVOR",
        "disposition_reason": "Retained as a bounded exact access-path capsule pending G09 calibration.",
    }


class G08ArchitectureEvolutionContractTests(unittest.TestCase):
    def test_valid_candidate_passes(self) -> None:
        self.assertEqual(
            [],
            validate_candidate_record(
                valid_candidate(),
                {"XFER-BOUND-SEARCH-FRONTIER-STATE"},
                {"FAIL-BOUNDED-FRONTIER-MISSES-NEAREST"},
                closure=True,
            ),
        )

    def test_unknown_ram_term_cannot_disappear(self) -> None:
        card = valid_candidate()
        del card["resource_model"]["ram_terms"]["runtime_overhead"]
        errors = validate_candidate_record(
            card,
            {"XFER-BOUND-SEARCH-FRONTIER-STATE"},
            {"FAIL-BOUNDED-FRONTIER-MISSES-NEAREST"},
            closure=True,
        )
        self.assertTrue(any("runtime_overhead" in error for error in errors))

    def test_candidate_markdown_round_trips(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ARCH-G08-001.md"
            write_candidate_card_markdown(path, valid_candidate())
            self.assertEqual(valid_candidate(), parse_candidate_card_file(path))

    def test_exact_fifty_niche_budget_is_required(self) -> None:
        niches = (
            ["LOWEST_RAM"] * 8
            + ["LOWEST_TAIL_LATENCY"] * 8
            + ["HIGHEST_PREDICTABILITY"] * 8
            + ["LOWEST_PREPARATION_STORAGE"] * 8
            + ["LOWEST_ADOPTION_FRICTION"] * 8
            + ["UNCONVENTIONAL_COMPOSABLE"] * 8
            + ["BASELINE"] * 2
        )
        cards = []
        for index, niche in enumerate(niches, 1):
            card = valid_candidate(f"ARCH-G08-{index:03d}")
            card["primary_niche"] = niche
            card["generation_lineage"]["lane_position"] = ((index - 1) % 8) + 1
            card["smallest_g09_falsifier"]["experiment_id"] = f"RESERVED-G09-FOR-ARCH-G08-{index:03d}"
            families = sorted([
                "DEPENDENCY_SECURITY_ACCESS_PATH",
                "BFS_REACHABILITY_SHORTEST_UNWEIGHTED",
                "PAGERANK_ITERATIVE_SPARSE_LINEAR_ALGEBRA",
                "WCC_CONNECTIVITY",
                "LOUVAIN_LEIDEN_COMMUNITY",
                "NODE_SIMILARITY_VECTOR_KNN",
                "MULTI_ALGORITHM_SHARED_ARTIFACT",
            ])
            family = families[(index - 1) % len(families)]
            card["target_workload_contract"]["algorithm_family"] = family
            card["target_workload_contract"]["first_wedge"] = family == "DEPENDENCY_SECURITY_ACCESS_PATH"
            cards.append(card)
        self.assertEqual(
            [],
            validate_candidate_collection(
                cards,
                {"XFER-BOUND-SEARCH-FRONTIER-STATE"},
                {"FAIL-BOUNDED-FRONTIER-MISSES-NEAREST"},
                closure=True,
            ),
        )
        cards.pop()
        self.assertTrue(
            validate_candidate_collection(
                cards,
                {"XFER-BOUND-SEARCH-FRONTIER-STATE"},
                {"FAIL-BOUNDED-FRONTIER-MISSES-NEAREST"},
                closure=True,
            )
        )

    def test_plan_and_pareto_bounds_are_enforced(self) -> None:
        niches = (
            ["LOWEST_RAM"] * 8
            + ["LOWEST_TAIL_LATENCY"] * 8
            + ["HIGHEST_PREDICTABILITY"] * 8
            + ["LOWEST_PREPARATION_STORAGE"] * 8
            + ["LOWEST_ADOPTION_FRICTION"] * 8
            + ["UNCONVENTIONAL_COMPOSABLE"] * 8
            + ["BASELINE"] * 2
        )
        rows = []
        for index in range(1, 51):
            rows.append(
                {
                    "candidate_rank": str(index),
                    "candidate_id": f"ARCH-G08-{index:03d}",
                    "lane_id": "G08-LANE-TEST",
                    "lane_position": str(index),
                    "primary_niche": niches[index - 1],
                    "generator_identity": "TEST",
                    "reviewer_identity": "TEST-REVIEWER",
                    "highest_completed_stage": "INDEPENDENT_ADVERSARIAL_REVIEW",
                    "terminal_disposition": "PARETO_SURVIVOR" if index <= 12 else "REPAIR_REQUIRED",
                    "inherited_transfer_ids": "XFER-BOUND-SEARCH-FRONTIER-STATE",
                    "linked_g06_failure_ids": "FAIL-BOUNDED-FRONTIER-MISSES-NEAREST",
                    "result_checksum": "A" * 64,
                }
            )
        self.assertEqual([], validate_candidate_plan_rows(rows, closure=True))
        pareto = [
            {
                "pareto_order": str(index),
                "candidate_id": f"ARCH-G08-{index:03d}",
                "primary_niche": "LOWEST_RAM",
                "survival_class": "PARETO_SURVIVOR",
                "robust_without_calibration": "NO",
                "requires_g09": "YES",
                "narrow_shape_only": "NO",
                "neo4j_surface_fit": "SUPPORTED_SUBSET",
                "dependency_wedge_fit": "HIGH",
                "symbolic_dominance": "NON_COMPARABLE",
                "never_combine_with": "NONE_IDENTIFIED",
            }
            for index in range(1, 13)
        ]
        self.assertEqual([], validate_pareto_rows(pareto, {row["candidate_id"] for row in rows}))
        self.assertTrue(validate_pareto_rows(pareto[:11], {row["candidate_id"] for row in rows}))


if __name__ == "__main__":
    unittest.main()
