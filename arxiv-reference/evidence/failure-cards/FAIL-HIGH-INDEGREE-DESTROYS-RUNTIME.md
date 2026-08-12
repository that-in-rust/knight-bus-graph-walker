# High Indegree Destroys Runtime

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The hub can be given a large residual without changing the algorithm."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "teleport parameter",
      "tolerance",
      "source vertex",
      "queue policy"
    ],
    "expected_observation": "Push count remains modest while scanned reverse-edge work crosses its admission bound",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "high indegree residual hub",
    "graph_scale": "Small graph with tunable hub in-degree",
    "graph_shape": "Directed graph where a high-residual vertex has many incoming neighbors",
    "independent_oracle": "Dense contribution computation plus explicit reverse-edge scan counter",
    "premises": [
      "The source makes push cost degree-proportional."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "Exact pushed order must be recorded.",
    "varied_variables": [
      "hub in-degree",
      "residual placement"
    ],
    "workload": "Run local pushback to the same approximation tolerance"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-PUSHBACK-LARGE-RESIDUALS-LOCALLY"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Instrumentation counts every scanned reverse edge."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "sum_indegree(pushed_sequence) > admitted_reverse_edge_work",
    "measurement_needed": "Count pushes and scanned reverse edges separately under fixed accuracy.",
    "numeric_constants": [],
    "premises": [
      "Push work scans reverse neighbors.",
      "The paper identifies total pushed in-degree as the runtime term."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The pushed sequence depends on graph and tolerance.",
    "variables": [
      {
        "definition": "sum of in-degrees over the actual pushed sequence",
        "symbol": "sum_indegree",
        "units": "edges"
      },
      {
        "definition": "ordered vertices selected by residual priority",
        "symbol": "pushed_sequence",
        "units": "vertices"
      },
      {
        "definition": "declared reverse-edge work budget",
        "symbol": "admitted_reverse_edge_work",
        "units": "edges"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "Reverse adjacency access cost is proportional to in-degree."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Each push scans in-neighbors.",
      "The theorem bounds pushes while runtime sums pushed in-degrees."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "A bound on push count is not a runtime bound unless the total reverse-neighbor volume of selected vertices is also bounded.",
    "uncertainty": "Queue and storage effects can add further cost."
  },
  "confidence_rationale": {
    "assumptions": [
      "The frozen paper statement applies to the reviewed algorithm."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "All cited pages and the full paper were rechecked."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The source explicitly separates push-count and summed-in-degree runtime and names the latter as an open limitation.",
    "uncertainty": "No target implementation was benchmarked."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "Accuracy and queue semantics are fixed."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Pushes scan all incoming neighbors."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Measured reverse-edge work or runtime exceeds the admitted bound even though push count does not.",
    "uncertainty": "The fixture establishes scaling, not a portable wall-clock threshold."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-HIGH-INDEGREE-DESTROYS-RUNTIME",
  "name": "High Indegree Destroys Runtime",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002",
      "FP-003"
    ],
    "text": "Actual work grows with the sum of in-degrees in the pushed sequence, so the push-count estimate understates runtime.",
    "uncertainty": "Storage locality and duplicate pushes can change constants."
  },
  "repair_options": [
    {
      "description": "Admit total reverse-edge work rather than push count alone.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Reject or reroute queries whose likely pushed vertices have excessive in-degree.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Provide degree-aware compressed or streamed reverse adjacency for expensive pushes.",
      "repair_class": "CHANGE_REPRESENTATION"
    }
  ],
  "source_paper_ids": [
    "PAPER-HASH-c2a6a5317d82ac28"
  ],
  "source_pointers": [
    {
      "claim_scope": "Push operation proportional to reverse-neighbor count",
      "locator_type": "SECTION",
      "locator_value": "Section 3.2, opening paragraphs",
      "page": 8,
      "paper_id": "PAPER-HASH-c2a6a5317d82ac28",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Runtime proportional to summed in-degrees of pushed vertices",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 2 and proof of Theorem 3.2",
      "page": 11,
      "paper_id": "PAPER-HASH-c2a6a5317d82ac28",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Explicit unresolved runtime dependence",
      "locator_type": "SECTION",
      "locator_value": "Section 6.1, improving dependency on in-degrees",
      "page": 20,
      "paper_id": "PAPER-HASH-c2a6a5317d82ac28",
      "pointer_id": "FP-003"
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
    "text": "Residual selection repeatedly chooses vertices whose in-degree is large even though the number of pushes remains within its theorem bound.",
    "uncertainty": "The paper does not give a universal graph family threshold."
  }
}
```
