# Query Heavy Overlay Crossover

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The small fixture preserves the compared visibility and recall semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "logical dataset",
      "query set",
      "recall target",
      "storage device",
      "merge trigger"
    ],
    "expected_observation": "Record the point where base-plus-overlay query cost exceeds direct-maintenance query cost while results remain equivalent.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "Query-dominant overlay run",
    "graph_scale": "Minimal graph whose query path can touch both base and delta.",
    "graph_shape": "A stable disk proximity graph with a query-visible memory delta.",
    "independent_oracle": "A freshly rebuilt index over the complete logical dataset at each visibility checkpoint.",
    "premises": [
      "The source reports higher in-place sustained query throughput and lower out-of-place update latency."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "A small fixture may not reproduce storage locality or merge behavior of the source systems.",
    "varied_variables": [
      "query-to-update ratio",
      "delta occupancy",
      "consolidation overlap"
    ],
    "workload": "Interleave a sparse update stream with a dominant exact-oracle query stream across one forced consolidation."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-BATCH-UPDATES-BEFORE-CONSOLIDATION"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Query semantics and recall are held equivalent across strategies."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "C_overlay_read + C_merge_share > C_direct_read",
    "measurement_needed": "Measure matched-recall query latency, overlay probes, consolidation overlap, and update rate across the target read/write mix.",
    "numeric_constants": [],
    "premises": [
      "The source reports a query-throughput reversal and read/write-ratio guidance."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The source does not supply a portable cost decomposition or threshold.",
    "variables": [
      {
        "definition": "Per-query cost of consulting base plus overlay",
        "symbol": "C_overlay_read",
        "units": "time per query"
      },
      {
        "definition": "Consolidation interference attributed to each query",
        "symbol": "C_merge_share",
        "units": "time per query"
      },
      {
        "definition": "Per-query cost under direct in-place maintenance",
        "symbol": "C_direct_read",
        "units": "time per query"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-003"
    ],
    "text": "Out-of-place accumulation is beneficial only when cheaper foreground updates repay the extra multi-component query and consolidation costs.",
    "uncertainty": "The source does not give a universal crossover."
  },
  "confidence_rationale": {
    "assumptions": [
      "The paper implementation matches its described update paths."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The mechanism and comparative result are explicit in the source."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Confidence is high that the source reports this qualitative reversal and moderate that the symbolic cost split captures its cause.",
    "uncertainty": "No independent reproduction, code inspection, recovery test, or target-system calibration was performed."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "The fixture holds correctness, recall, and hardware constant."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports a workload-dependent strategy reversal."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Matched-result queries through base plus overlay take longer or sustain less throughput than direct maintenance once overlay and merge costs dominate.",
    "uncertainty": "The exact crossover is target-specific and unmeasured here."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-QUERY-HEAVY-OVERLAY-CROSSOVER",
  "name": "Query Heavy Overlay Crossover",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "The in-place strategy sustains higher search throughput than the out-of-place strategy on the evaluated workloads.",
    "uncertainty": "The magnitude is not generalized beyond the evaluated fixtures."
  },
  "repair_options": [
    {
      "description": "Admit out-of-place updates only for a measured read/write region and bounded delta occupancy.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Bound delta, merge scratch, and old/new index coexistence before accepting updates.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Retain a direct-maintenance path when overlay query cost exceeds the declared budget.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Calibrate the target-system crossover without importing source benchmark ratios.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-2603.01779"
  ],
  "source_pointers": [
    {
      "claim_scope": "Memory overlay, dual-component query state, and costly consolidation.",
      "locator_type": "SECTION",
      "locator_value": "Section 3.5, Out-of-Place Update",
      "page": 6,
      "paper_id": "PAPER-2603.01779",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "In-place versus out-of-place query throughput, update latency, and merge comparison.",
      "locator_type": "FIGURE",
      "locator_value": "Figures 15-16 and Section 4.3.5",
      "page": 11,
      "paper_id": "PAPER-2603.01779",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Read/write-ratio guidance for selecting update technique.",
      "locator_type": "SECTION",
      "locator_value": "Section 5.1 item 5",
      "page": 12,
      "paper_id": "PAPER-2603.01779",
      "pointer_id": "FP-003"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002",
      "FP-003"
    ],
    "text": "A query-heavy workload repeatedly consults the base plus update component while updates are too sparse to amortize that read cost.",
    "uncertainty": "The reported comparison is fixture-specific."
  }
}
```
