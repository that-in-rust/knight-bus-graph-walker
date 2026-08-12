# In-memory sort exceeds memory budget

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The external oracle can operate within a separately enforced bounded buffer."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Edge encoding",
      "Sort implementation",
      "Input order",
      "Storage device"
    ],
    "expected_observation": "The in-memory path breaches or rejects the budget while the external path preserves the edge multiset",
    "fixture_kind": "EXECUTION_PROFILE",
    "fixture_name": "edge-sort budget overflow",
    "graph_scale": "Symbolic edge count whose sort workspace crosses the configured budget",
    "graph_shape": "A persisted edge list with arbitrary valid topology",
    "independent_oracle": "External merge sort with exact edge multiset comparison",
    "premises": [
      "The source names out-of-core sorting as the fit-boundary response."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "The exact byte crossover is implementation-specific.",
    "varied_variables": [
      "Edge count",
      "Memory budget"
    ],
    "workload": "Execute only the read-sort-rewrite stage under a hard memory cap"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-MATERIALIZE-EACH-PIPELINE-STAGE"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Peak sort residency is measured consistently with the configured budget."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "edge_array_bytes > memory_budget_bytes",
    "measurement_needed": "Measure peak resident bytes for edge data and sort workspace.",
    "numeric_constants": [],
    "premises": [
      "The source explicitly separates in-memory and out-of-core sorting by fit."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "Allocator and implementation overhead can increase actual peak bytes.",
    "variables": [
      {
        "definition": "Peak resident bytes for the edge arrays and sorting workspace",
        "symbol": "edge_array_bytes",
        "units": "bytes"
      },
      {
        "definition": "Available memory budget for the stage",
        "symbol": "memory_budget_bytes",
        "units": "bytes"
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
    "text": "The pipeline's in-memory sorting path requires the edge arrays to fit available RAM.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "The proposed fixture preserves the source mechanism while varying only the stated trigger."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The cited pages define the mechanism and its reported or analytically exposed boundary."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The failure condition is bounded by the cited mechanism premise and negative result; the proposed fixture isolates the same condition without asserting an unsourced numeric threshold.",
    "uncertainty": "The precise crossover remains graph-, implementation-, and hardware-dependent unless the source states otherwise."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "Memory instrumentation captures the entire process tree."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source states the fit boundary."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Peak resident bytes exceed the cap or admission rejects the in-memory sort, while the external oracle emits the same edge multiset.",
    "uncertainty": "Transient allocator overhead may require conservative measurement."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-INMEMORY-SORT-EXCEEDS-BUDGET",
  "name": "In-memory sort exceeds memory budget",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The in-memory stage cannot be admitted and requires an out-of-core sort path.",
    "uncertainty": "NONE"
  },
  "repair_options": [
    {
      "description": "Expose and enforce a budget for the resource term that triggers the failure.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Use a correctness-preserving fallback when the optimized path's assumptions fail.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-1603.01876"
  ],
  "source_pointers": [
    {
      "claim_scope": "The in-memory sort boundary and out-of-core alternative.",
      "locator_type": "SECTION",
      "locator_value": "Sections IV.B-IV.D",
      "page": 5,
      "paper_id": "PAPER-1603.01876",
      "pointer_id": "FP-001"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Materialize and sort an edge array whose resident bytes exceed the allowed memory budget.",
    "uncertainty": "NONE"
  }
}
```
