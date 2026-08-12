# Active Edge Prediction Miss

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Alternation isolates temporal prediction error."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Graph",
      "Page layout",
      "Active-set cardinality",
      "Superstep count"
    ],
    "expected_observation": "Results match while useful logged edges vanish and relog lifecycle time crosses direct loading.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "Alternating active edge sets",
    "graph_scale": "Symbolic fixed graph and page layout over repeated supersteps.",
    "graph_shape": "Two disjoint vertex sets alternate activity so recent history predicts the wrong next set.",
    "independent_oracle": "Direct CSR execution with exact active vertices and page-read tracing.",
    "premises": [
      "The source predictor uses recent active history."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The source does not evaluate a fully anti-correlated sequence.",
    "varied_variables": [
      "Overlap between consecutive active sets"
    ],
    "workload": "Run history-based relogging and direct CSR page loading over the same active sequence."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-RELOG-PREDICTED-ACTIVE-EDGES"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "The comparison includes edge-log write and read work."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "useful_logged_edges <= wasted_logged_edges OR T_edge_log >= T_direct_pages",
    "measurement_needed": "Trace predicted, logged, next-step-used edges, bytes written, and direct page reads by superstep.",
    "numeric_constants": [],
    "premises": [
      "The source reports replication and lower prediction accuracy for quickly converging workloads."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The source does not publish a useful-to-wasted crossover.",
    "variables": [
      {
        "definition": "logged edges read usefully in the next superstep",
        "symbol": "useful_logged_edges",
        "units": "edges"
      },
      {
        "definition": "logged edges not used in the next superstep",
        "symbol": "wasted_logged_edges",
        "units": "edges"
      },
      {
        "definition": "time for prediction, replication, writing, and reading the edge log",
        "symbol": "T_edge_log",
        "units": "time"
      },
      {
        "definition": "time for direct original-page reads without relogging",
        "symbol": "T_direct_pages",
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
      "FP-002",
      "FP-003"
    ],
    "text": "Edge relogging assumes recent activity predicts next-superstep activity and enough sparse original pages will be avoided to repay replicated writes.",
    "uncertainty": "Prediction behavior depends on convergence and page utilization."
  },
  "confidence_rationale": {
    "assumptions": [
      "The local trace includes all relog lifecycle work."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source states predictor and replication behavior."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Lower prediction accuracy is source-reported; the anti-correlated fixture and end-to-end breakpoint are derived.",
    "uncertainty": "No reproduction or code inspection occurred."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "The direct oracle uses identical active sets."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Relogging replicates graph data based on prediction."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Logged-edge usefulness or total time fails the guard while vertex results remain exact.",
    "uncertainty": "The terminal crossover requires measurement."
  },
  "failure_basis": "SOURCE_SUPPORTED_DERIVATION",
  "failure_id": "FAIL-ACTIVE-EDGE-PREDICTION-MISS",
  "name": "Active Edge Prediction Miss",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002",
      "FP-003"
    ],
    "text": "Few useful pages are predicted, reducing the opportunity to offset edge-log replication and write work.",
    "uncertainty": "The source does not report a total lifecycle crossover."
  },
  "repair_options": [
    {
      "description": "Require recent predictor precision and page-savings estimates before relogging.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Read original CSR pages when the predictor guard fails.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Cap edge-log bytes and write bandwidth per superstep.",
      "repair_class": "ADD_RESOURCE_BOUND"
    }
  ],
  "source_paper_ids": [
    "PAPER-1905.04264"
  ],
  "source_pointers": [
    {
      "claim_scope": "Page read amplification and edge-log objective.",
      "locator_type": "SECTION",
      "locator_value": "Section IV.C",
      "page": 4,
      "paper_id": "PAPER-1905.04264",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "History predictor, utilization threshold, and graph-data replication.",
      "locator_type": "SECTION",
      "locator_value": "Section V.C continuation",
      "page": 7,
      "paper_id": "PAPER-1905.04264",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Lower prediction accuracy for quickly converging workloads.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 9 and prediction-accuracy paragraph",
      "page": 10,
      "paper_id": "PAPER-1905.04264",
      "pointer_id": "FP-003"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-003"
    ],
    "text": "A quickly converging or anti-correlated active set offers few inefficient pages and weak next-step predictability.",
    "uncertainty": "The source reports lower accuracy but not an adversarial sequence."
  }
}
```
