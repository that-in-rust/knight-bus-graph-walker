# Early Iterations Pay Checks

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The fixture preserves a valid monotone BFS finality predicate."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph",
      "row order",
      "chunk height",
      "BFS root",
      "semiring",
      "thread schedule"
    ],
    "expected_observation": "Results match, no chunks skip, and the checking path records additional work or time.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "No-final-chunk frontier",
    "graph_scale": "Minimal chunked graph that keeps all chunks active through an early phase.",
    "graph_shape": "A high-diameter traversal whose frontier leaves at least one unsettled row in every chunk.",
    "independent_oracle": "The identical kernel with chunk skipping disabled.",
    "premises": [
      "The source reports this early-iteration and graph-family boundary."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "Small-run timing may be dominated by measurement noise; operation counts remain observable.",
    "varied_variables": [
      "frontier placement",
      "iteration"
    ],
    "workload": "Run identical BFS sparse kernels with and without finality checks through the no-skip phase."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-SKIP-FINALIZED-VECTOR-CHUNKS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "The no-skip oracle uses the same representation and semiring."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "C_saved_chunks <= C_finality_checks",
    "measurement_needed": "Measure finalized-chunk fraction, check time, skipped edge work, row ordering, and traversal iteration.",
    "numeric_constants": [],
    "premises": [
      "The source reports early checks without skips and graph families with little gain."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The paper does not provide a device-independent crossover.",
    "variables": [
      {
        "definition": "Cost of edge kernels avoided for finalized chunks",
        "symbol": "C_saved_chunks",
        "units": "time"
      },
      {
        "definition": "Cost of testing finality across visited chunks",
        "symbol": "C_finality_checks",
        "units": "time"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Finality checks pay off only when complete chunks become safely final early enough to avoid more edge work than the checks cost.",
    "uncertainty": "No universal skip-density threshold is supplied."
  },
  "confidence_rationale": {
    "assumptions": [
      "The implementation follows the published skip predicate."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source defines the check and discusses its early overhead and weak graph cases."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Confidence is high in the source-reported no-skip early phase and moderate in any target timing effect.",
    "uncertainty": "No independent rerun or generated-code inspection was performed."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "The fixture distributes active rows across every chunk."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source states that early iterations can have no finalized chunks."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "All results match the no-skip oracle, but every visited chunk executes its finality check and no edge kernel is avoided.",
    "uncertainty": "The wall-time penalty depends on hardware and chunk layout."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-EARLY-ITERATIONS-PAY-CHECKS",
  "name": "Early Iterations Pay Checks",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002",
      "FP-003"
    ],
    "text": "The optimized path pays checking overhead and shows little or no improvement while doing the same edge work.",
    "uncertainty": "The overhead is architecture- and ordering-dependent."
  },
  "repair_options": [
    {
      "description": "Enable chunk checks only after observed finality density can repay them.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Reorder rows or defer checking to concentrate finalized rows into complete chunks.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Use the no-skip kernel during phases with no observed finalized chunks.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Calibrate check-versus-saved-work crossover per architecture and graph family.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-2010.09913"
  ],
  "source_pointers": [
    {
      "claim_scope": "Per-chunk finality check and skip condition.",
      "locator_type": "SECTION",
      "locator_value": "Section III-C, SlimWork",
      "page": 7,
      "paper_id": "PAPER-2010.09913",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Early-iteration checking overhead before chunks finalize.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 5(d) and Section IV-A4",
      "page": 9,
      "paper_id": "PAPER-2010.09913",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "High-diameter and low-degree graph boundary.",
      "locator_type": "FIGURE",
      "locator_value": "Figures 7-8 and Sections IV-C-IV-E",
      "page": 10,
      "paper_id": "PAPER-2010.09913",
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
    "text": "Early iterations with no finalized chunks, especially on high-diameter low-degree graphs, execute checks without skipping the sparse kernel.",
    "uncertainty": "The source describes little improvement rather than a universal slowdown."
  }
}
```
