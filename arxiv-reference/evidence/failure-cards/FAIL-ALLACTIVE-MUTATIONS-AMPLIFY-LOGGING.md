# Allactive Mutations Amplify Logging

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The fixture holds mutation semantics constant between paths."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph",
      "interval boundaries",
      "mutation set",
      "storage device",
      "memory budget"
    ],
    "expected_observation": "Record equivalent final topology together with logging, sorting, merge, and direct-update costs.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "All-active structural mutation",
    "graph_scale": "Minimal graph containing multiple destination intervals and observable deletions.",
    "graph_shape": "A CSR graph whose structural-update phase activates every vertex and routes deletions to destination intervals.",
    "independent_oracle": "A direct structural-update interpreter that applies every mutation without the destination-log path.",
    "premises": [
      "The source identifies this all-active structural-update workload as pathological."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Small fixtures may underrepresent SSD write and sorting behavior.",
    "varied_variables": [
      "active-set coverage",
      "message multiplicity"
    ],
    "workload": "Execute one all-active mutation pass through destination logs and through a direct-mutation oracle."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-PARTITION-UPDATES-BY-DESTINATION"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Both paths preserve the same structural updates."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "C_log + C_sort + C_merge > C_direct_mutation",
    "measurement_needed": "Measure log bytes, sort/group time, merge time, direct mutation time, and active-set coverage.",
    "numeric_constants": [],
    "premises": [
      "The source reports a direct-update advantage on an all-active one-iteration mutation workload."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "No general decomposition or portable coefficient is supplied.",
    "variables": [
      {
        "definition": "Cost of appending and persisting destination logs",
        "symbol": "C_log",
        "units": "time"
      },
      {
        "definition": "Cost of loading, sorting, and grouping interval logs",
        "symbol": "C_sort",
        "units": "time"
      },
      {
        "definition": "Cost of applying structural updates to CSR",
        "symbol": "C_merge",
        "units": "time"
      },
      {
        "definition": "Cost of direct shard mutation",
        "symbol": "C_direct_mutation",
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
    "text": "Destination logging pays when selective activity or repeated processing amortizes append, sort, and delayed mutation costs.",
    "uncertainty": "The source does not state a universal activity threshold."
  },
  "confidence_rationale": {
    "assumptions": [
      "The benchmark implementations preserve comparable mutation semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source explains both the logging path and the direct-update comparison."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Confidence is high in the source-reported pathological workload and moderate in the symbolic cost decomposition.",
    "uncertainty": "No independent reproduction or destination-skew stress test was performed."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "The direct oracle applies the same mutations."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports this qualitative reversal."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The log path preserves topology but takes longer or moves more temporary data than direct mutation when all work is active and unamortized.",
    "uncertainty": "The crossover scale and device response require measurement."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-ALLACTIVE-MUTATIONS-AMPLIFY-LOGGING",
  "name": "Allactive Mutations Amplify Logging",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "The partitioned logging implementation runs slower than GraphChi on the evaluated all-active structural-deletion workload.",
    "uncertainty": "The ratio is implementation- and fixture-specific."
  },
  "repair_options": [
    {
      "description": "Route all-active one-pass mutations away from the log path after measured detection.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Retain direct structural mutation for workloads without logging amortization.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Fuse or bypass interval processing when the active set covers the graph.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Calibrate activity, skew, and mutation-volume crossover on the target storage stack.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-1905.04264"
  ],
  "source_pointers": [
    {
      "claim_scope": "Destination-interval logging and in-memory grouping schedule.",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 1 and Section IV-B",
      "page": 4,
      "paper_id": "PAPER-1905.04264",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "All-active one-iteration structural-update case underperforming direct shard mutation.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 8(a), K-core discussion",
      "page": 10,
      "paper_id": "PAPER-1905.04264",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "K-core activates all vertices in its structural-update iteration, logs deletions, and later applies them to CSR while the comparison path updates shard delete state directly.",
    "uncertainty": "This is the evaluated pathological case."
  }
}
```
