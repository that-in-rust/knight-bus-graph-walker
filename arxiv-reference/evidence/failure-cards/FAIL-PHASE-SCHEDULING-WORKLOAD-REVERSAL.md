# Phase Scheduling Workload Reversal

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The fixture exposes all three source-described phases."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Graph",
      "Source vertex",
      "Thread count",
      "Direction-switch policy"
    ],
    "expected_observation": "Distances match while at least one phase loses its scheduling advantage.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "Three phase frontier reversal",
    "graph_scale": "Symbolic graph order and degree skew with fixed source vertex.",
    "graph_shape": "A power-law graph with a tiny high-degree frontier, a dense bottom-up frontier, and a sparse tail.",
    "independent_oracle": "Sequential BFS distances plus per-level edge-work accounting.",
    "premises": [
      "The source identifies phase-dependent work units and partition overhead."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The smallest degree sequence that induces all phases is unknown.",
    "varied_variables": [
      "Partition factor",
      "Frontier phase"
    ],
    "workload": "Run level-synchronous BFS under phase-specific and reference schedules."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Timing uses identical BFS semantics and graph layout."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "T_phase_schedule >= T_reference_schedule OR imbalance_phase >= imbalance_limit",
    "measurement_needed": "Measure per-worker useful work, steals, and elapsed time by BFS level.",
    "numeric_constants": [],
    "premises": [
      "The source reports both high-degree imbalance and too-fine partition overhead."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "No source-reported universal crossover exists.",
    "variables": [
      {
        "definition": "elapsed time under phase-specific scheduling",
        "symbol": "T_phase_schedule",
        "units": "time"
      },
      {
        "definition": "elapsed time under the comparison schedule",
        "symbol": "T_reference_schedule",
        "units": "time"
      },
      {
        "definition": "largest worker-work deviation in the phase",
        "symbol": "imbalance_phase",
        "units": "work units"
      },
      {
        "definition": "admitted worker-work deviation",
        "symbol": "imbalance_limit",
        "units": "work units"
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
    "text": "A phase-specific scheduler assumes its selected work unit balances useful work without introducing dominant coordination overhead.",
    "uncertainty": "The source supplies empirical tuning rather than a universal phase detector."
  },
  "confidence_rationale": {
    "assumptions": [
      "The local scheduler preserves the source work-unit definitions."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports both forms of scheduling reversal."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The failure modes are source-reported, but the admission boundary remains workload and machine dependent.",
    "uncertainty": "No deterministic contention bound is available."
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
    "text": "Per-level worker imbalance or work-stealing overhead erases the intended scheduling gain.",
    "uncertainty": "The source does not define a universal threshold."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-PHASE-SCHEDULING-WORKLOAD-REVERSAL",
  "name": "Phase Scheduling Workload Reversal",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Workers remain imbalanced or scheduling overhead consumes the intended parallel gain.",
    "uncertainty": "The paper does not report a portable imbalance limit."
  },
  "repair_options": [
    {
      "description": "Switch work unit by observed frontier shape and cap partition count.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Use the simpler reference schedule when phase telemetry rejects specialization.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Calibrate the partition and imbalance envelope per platform.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-2012.10026"
  ],
  "source_pointers": [
    {
      "claim_scope": "High-degree top-down edge-block scheduling.",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 4 and Section 3.3 Phase 1",
      "page": 5,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Too-fine bottom-up partition overhead and scale-sensitive tuning.",
      "locator_type": "SECTION",
      "locator_value": "Section 3.6, Parameter Tuning",
      "page": 8,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "A tiny frontier containing a few high-degree vertices defeats vertex-only splitting, while too many small bottom-up partitions amplify work stealing.",
    "uncertainty": "The graph-size and thread-count crossover is platform dependent."
  }
}
```
