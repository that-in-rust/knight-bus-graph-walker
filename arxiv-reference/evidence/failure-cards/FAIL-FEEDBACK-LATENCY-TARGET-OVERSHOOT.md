# Feedback Latency Target Overshoot

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The replay does not claim production latency performance."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Controller rules",
      "Latency target",
      "Observation statistic",
      "Utilization step policy"
    ],
    "expected_observation": "Every state transition matches the oracle, and target violations remain visible rather than being mislabeled as a hard guarantee.",
    "fixture_kind": "EXECUTION_PROFILE",
    "fixture_name": "Feedback drift and overshoot",
    "graph_scale": "Symbolic finite observation sequence covering stable, spiking, drifting, and infeasible phases.",
    "graph_shape": "A fixed disk-resident search graph with a deterministic latency-observation trace.",
    "independent_oracle": "A hand-evaluated state-transition and target-comparison trace from the source procedure.",
    "premises": [
      "The source defines the controller and best-effort limitation."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "Exact implementation cadence and history length are not reported.",
    "varied_variables": [
      "Observation sequence",
      "Baseline drift",
      "Feasible utilization"
    ],
    "workload": "Replay observations through recording, feasibility-search, steady, disable, and rebaseline states."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-TUNE-IDLE-WINDOW-UTILIZATION"
  ],
  "breakpoint_equation": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "expression": "observed_latency > baseline_latency * allowed_ratio",
    "measurement_needed": "Replay deterministic latency traces through the controller and measure overshoot duration and magnitude.",
    "numeric_constants": [],
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The source explicitly characterizes the target as best effort rather than hard per sample.",
    "variables": [
      {
        "definition": "selected measured foreground-latency statistic",
        "symbol": "observed_latency",
        "units": "time"
      },
      {
        "definition": "recorded no-update foreground-latency statistic",
        "symbol": "baseline_latency",
        "units": "time"
      },
      {
        "definition": "configured multiplicative latency allowance",
        "symbol": "allowed_ratio",
        "units": "ratio"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "A phase-level feedback controller cannot be treated as a hard per-request latency bound.",
    "uncertainty": "Control cadence and observed statistic affect overshoot."
  },
  "confidence_rationale": {
    "assumptions": [
      "The replay follows the published transition rules."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports best-effort semantics and noisy high-quantile feedback."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The limitation and disable behavior are explicit source claims; deterministic replay is the verification fixture, not a performance benchmark.",
    "uncertainty": "Code-level controller details were not inspected."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The observed statistic crosses the configured ratio before correction or no feasible utilization remains.",
    "uncertainty": "The source allows phase-level best-effort violations."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-FEEDBACK-LATENCY-TARGET-OVERSHOOT",
  "name": "Feedback Latency Target Overshoot",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Observed latency temporarily or phase-averagely exceeds the target, or the controller disables co-execution.",
    "uncertainty": "Violation magnitude depends on workload phase."
  },
  "repair_options": [
    {
      "description": "Advertise only a best-effort phase-level objective and disable co-execution when no feasible ratio exists.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Rebaseline after sustained drift and reduce utilization after violations.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Measure controller cadence and tail-statistic noise on the target runtime.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-2605.19335"
  ],
  "source_pointers": [
    {
      "claim_scope": "Feasibility search, steady adjustment, disable, and rebaseline transitions.",
      "locator_type": "SECTION",
      "locator_value": "Section 4.4, Feedback loop",
      "page": 7,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Best-effort phase-level target and transient violations.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Sections 6.2-6.3",
      "page": 9,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Sparse and noisy high-quantile feedback.",
      "locator_type": "TABLE",
      "locator_value": "Table 5 and Appendix A, Experiment 9",
      "page": 15,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "FP-003"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Short latency fluctuations, baseline drift, noisy tail statistics, or absence of any feasible nonzero utilization invalidate the current control setting.",
    "uncertainty": "The source does not generalize a stability region."
  }
}
```
