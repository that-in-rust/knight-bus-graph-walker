# Recent Samples Miss Shifts

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The scheduler is deterministic for an identical history."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "retained history",
      "conditioning bucket",
      "utilization setting"
    ],
    "expected_observation": "The same admitted budget passes historical validation but exceeds the future-window allowance only after the shift.",
    "fixture_kind": "EXECUTION_PROFILE",
    "fixture_name": "identical-history-divergent-next-window",
    "graph_scale": "Use the smallest retained history accepted by the estimator and one subsequent decision.",
    "graph_shape": "No graph topology is required; use identical search-stall histories followed by different next windows.",
    "independent_oracle": "Exhaustively evaluate actual overrun using the known synthetic next-window duration.",
    "premises": [
      "The estimator uses recent samples."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "Production shifts may be less abrupt than the fixture.",
    "varied_variables": [
      "next-window duration"
    ],
    "workload": "Solve one budget from the shared history, then run it against a stable and an abruptly shortened next window."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-BOUND-OVERRUN-FROM-SAMPLES"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "The future distribution may differ from the sample distribution."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "future_overrun > admitted_overrun_budget",
    "measurement_needed": "Replay abrupt shifts and measure actual foreground overrun against the admitted budget.",
    "numeric_constants": [],
    "premises": [
      "The source constrains empirical expected overrun from recent samples."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "No finite-sample or drift bound is provided.",
    "variables": [
      {
        "definition": "Background execution extending beyond the actual future idle window.",
        "symbol": "future_overrun",
        "units": "time"
      },
      {
        "definition": "Overrun permitted by the history-derived admission decision.",
        "symbol": "admitted_overrun_budget",
        "units": "time"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Recent conditioned idle samples represent the upcoming short execution window well enough to budget background work.",
    "uncertainty": "The source presents short-window stability empirically, not as a guarantee."
  },
  "confidence_rationale": {
    "assumptions": [
      "No hidden predictor uses future information."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source budget is derived from recent samples."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The indistinguishable-history construction is exact for any history-only estimator; only its production frequency is unknown.",
    "uncertainty": "The source does not quantify adversarial drift."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "The measured timer and scheduler share a consistent clock."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The sample rule does not constrain an unseen shifted window."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Observed foreground overrun exceeds the admitted overrun budget after a history-preserving distribution shift.",
    "uncertainty": "Timer noise must be separated from the shift effect."
  },
  "failure_basis": "ANALYTICAL_COUNTEREXAMPLE",
  "failure_id": "FAIL-RECENT-SAMPLES-MISS-SHIFTS",
  "name": "Recent Samples Miss Shifts",
  "observable_symptom": {
    "assumptions": [
      "No independent hard deadline preempts the background task."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Equal estimator histories produce equal admitted budgets.",
      "The next window in the shifted execution is shorter than the admitted work."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Both executions receive the same budget, so the shifted execution overruns foreground work despite satisfying the historical empirical constraint.",
    "uncertainty": "The exact tail impact depends on preemption granularity."
  },
  "repair_options": [
    {
      "description": "Add an independent hard preemption guard below the sample-derived budget.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Detect drift by conditioning and recency, and suspend sample-based admission until the history is representative again.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-2605.19335"
  ],
  "source_pointers": [
    {
      "claim_scope": "Recent samples define an empirical expected-overrun budget and search-specific conditioning.",
      "locator_type": "SECTION",
      "locator_value": "Section 4.3, Overrun-Bounded Time Budgeting",
      "page": 6,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Per-hop overhead is evaluated against a configured target and described as best-effort.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 7 and Section 6.3",
      "page": 9,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "An abrupt change can occur after the final observed sample."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The estimator is a function of retained recent samples."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Two executions share the same retained history but the next idle-window distribution changes abruptly in only one execution.",
    "uncertainty": "The probability of such a shift is workload-specific."
  }
}
```
