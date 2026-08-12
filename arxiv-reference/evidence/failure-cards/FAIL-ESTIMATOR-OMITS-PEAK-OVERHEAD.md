# Estimator omits peak runtime overhead

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The implementation has at least one unmodeled positive resident term."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Graph encoding",
      "Query batch",
      "Budget accounting method",
      "Allocator"
    ],
    "expected_observation": "The model admits the configuration while independent peak accounting exceeds the budget",
    "fixture_kind": "EXECUTION_PROFILE",
    "fixture_name": "near-budget hidden-overhead admission",
    "graph_scale": "A scale adjusted so the estimate approaches but does not exceed the budget",
    "graph_shape": "Any valid ANN graph accepted by the placement model",
    "independent_oracle": "Operating-system process peak plus allocator-level accounting reconciled against mapped files",
    "premises": [
      "The source provides the admission equation but not error bars."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "This fixture is analytical and may not fail if the implementation adds conservative headroom.",
    "varied_variables": [
      "Dataset scale",
      "Budget headroom"
    ],
    "workload": "Build and query after major-in-memory admission while recording all resident mappings and allocations"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-PLACE-SCALE-GROWING-STATE"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Actual peak includes every resident allocation and mapping charged to the process."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "estimated_peak_bytes <= memory_budget_bytes < actual_peak_bytes",
    "measurement_needed": "Measure estimator components and process-wide peak residency on the target implementation.",
    "numeric_constants": [],
    "premises": [
      "The source gate compares a decomposed estimate with a budget."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "The magnitude of omitted overhead is unknown.",
    "variables": [
      {
        "definition": "Bytes predicted by the placement model",
        "symbol": "estimated_peak_bytes",
        "units": "bytes"
      },
      {
        "definition": "Configured peak-memory budget",
        "symbol": "memory_budget_bytes",
        "units": "bytes"
      },
      {
        "definition": "Measured process peak including runtime overhead",
        "symbol": "actual_peak_bytes",
        "units": "bytes"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "Actual implementations may add resident terms not represented in the decomposition."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source admits major-in-memory placement when its decomposed estimate fits the budget."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The admission decision assumes the estimated footprint upper-bounds actual peak residency, but the source provides no estimator error bars for allocator, alignment, or transient query overhead.",
    "uncertainty": "The omitted terms and their magnitude are implementation-dependent."
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
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "The independent meter captures all charged resident state."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The model is used as a hard admission guard."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The admission predicate is true while independent process-wide accounting records a budget breach.",
    "uncertainty": "A non-failure would bound, rather than prove absence of, omitted overhead."
  },
  "failure_basis": "ANALYTICAL_COUNTEREXAMPLE",
  "failure_id": "FAIL-ESTIMATOR-OMITS-PEAK-OVERHEAD",
  "name": "Estimator omits peak runtime overhead",
  "observable_symptom": {
    "assumptions": [
      "The memory meter includes allocator and transient buffers."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Estimated fit controls placement."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Admission succeeds, then measured peak resident memory exceeds the configured budget.",
    "uncertainty": "The source does not report this failure; it is an analytical safety test."
  },
  "repair_options": [
    {
      "description": "Expose and enforce a budget for the resource term that triggers the failure.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Reject or reroute workloads once the symbolic failure predicate is observed.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Measure the unresolved crossover on the target graph and machine before admission.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-2603.01779"
  ],
  "source_pointers": [
    {
      "claim_scope": "Pre-search memory-footprint decomposition and budget-gated placement decision.",
      "locator_type": "EQUATION",
      "locator_value": "Equations 1-2 and Section 5.1",
      "page": 12,
      "paper_id": "PAPER-2603.01779",
      "pointer_id": "FP-001"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "At least one positive runtime-resident term is absent from the estimate."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Admission is based on the source footprint equation."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Choose a case whose declared estimate fits the memory budget but whose measured runtime overhead pushes actual peak residency above it.",
    "uncertainty": "No source measurement establishes the smallest omitted term."
  }
}
```
