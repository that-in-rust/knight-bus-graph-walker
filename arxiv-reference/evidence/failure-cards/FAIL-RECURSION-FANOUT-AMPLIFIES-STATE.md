# Recursion Fanout Amplifies State

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Nonoverlapping branches expose the maximum distinct expansion without changing semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "teleport parameter",
      "samples per vertex",
      "endpoint encoding",
      "cache policy"
    ],
    "expected_observation": "Peak query state and database accesses grow with expanded vertices and cross the admitted budget",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "recursive high fanout tree",
    "graph_scale": "Small layered graph with adjustable branching and depth",
    "graph_shape": "Directed layered graph with broad nonoverlapping out-neighborhoods from the personalization source",
    "independent_oracle": "Exact personalized ranking on the small graph plus direct counts of distinct sample sets",
    "premises": [
      "The source equation expands over every out-neighbor."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "This is an analytical fixture, not source-reported performance.",
    "varied_variables": [
      "branching",
      "recursion depth"
    ],
    "workload": "Refine endpoint samples recursively while holding sample count per vertex fixed"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-REFINE-ENDPOINT-SAMPLES-RECURSIVELY"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Loaded samples or equivalent accumulator entries remain live until aggregation."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "expanded_vertices(depth, branching) * samples_per_vertex * endpoint_width > query_state_budget",
    "measurement_needed": "Measure distinct sample-set reads, peak live endpoint state, and precision by recursion depth.",
    "numeric_constants": [],
    "premises": [
      "Each expanded vertex contributes a stored endpoint sample set."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Deduplication, streaming aggregation, and cache reuse are unspecified.",
    "variables": [
      {
        "definition": "distinct vertices whose sample sets are loaded",
        "symbol": "expanded_vertices",
        "units": "vertices"
      },
      {
        "definition": "recursive refinement depth",
        "symbol": "depth",
        "units": "levels"
      },
      {
        "definition": "effective out-neighbor fanout after overlap",
        "symbol": "branching",
        "units": "vertices per level"
      },
      {
        "definition": "stored endpoints loaded per vertex",
        "symbol": "samples_per_vertex",
        "units": "samples per vertex"
      },
      {
        "definition": "bytes per stored endpoint",
        "symbol": "endpoint_width",
        "units": "bytes per sample"
      },
      {
        "definition": "admitted query-time endpoint and accumulator state",
        "symbol": "query_state_budget",
        "units": "bytes"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "Sample sets are not perfectly shared or deduplicated across all branches."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "One recursion level accesses every out-neighbor sample set.",
      "Further recursion repeats the decomposition."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Recursive sample refinement assumes neighborhood fanout and accumulated endpoint state fit the query I/O and memory budgets.",
    "uncertainty": "The paper reports one-level benefits but not a general multi-level resource bound."
  },
  "confidence_rationale": {
    "assumptions": [
      "The fixture minimizes neighborhood overlap."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source gives the one-level access and sample expansion."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The fanout counterexample follows directly from the sourced recursion, but the paper does not report the derived peak-memory failure.",
    "uncertainty": "Implementation-specific sharing can change the measured growth."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "The fixture controls overlap and cache state."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The recursive implementation follows the sourced decomposition."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Distinct reads or peak live state exceed the admitted query budget before the oracle precision target is met.",
    "uncertainty": "A streaming implementation may reduce live state while retaining I/O growth."
  },
  "failure_basis": "ANALYTICAL_COUNTEREXAMPLE",
  "failure_id": "FAIL-RECURSION-FANOUT-AMPLIFIES-STATE",
  "name": "Recursion Fanout Amplifies State",
  "observable_symptom": {
    "assumptions": [
      "The oracle uses the same personalization semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Recursion expands the queried vertex set and sample pool."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Database accesses, endpoint samples combined, or temporary accumulator state exceed the admitted query budget before the desired precision is reached.",
    "uncertainty": "The source does not report multi-level peak memory."
  },
  "repair_options": [
    {
      "description": "Cap recursive expansion by distinct sample reads and peak state.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Reject deeper refinement for high effective fanout.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Stream and aggregate child samples to bound live state.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Use direct iteration when exact low-probability values are required.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-HASH-0232e71ded2b5c43"
  ],
  "source_pointers": [
    {
      "claim_scope": "One recursion level uses neighbor sample sets and degree-proportional database accesses",
      "locator_type": "EQUATION",
      "locator_value": "Section 2.3 recursive PPV equation",
      "page": 12,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Approximation and recursion continuation caveats",
      "locator_type": "SECTION",
      "locator_value": "Section 2.3 continuation",
      "page": 13,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Approximation scope and unresolved method combination",
      "locator_type": "SECTION",
      "locator_value": "Section 6, Conclusions and Open Problems",
      "page": 22,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "FP-003"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "The index contains fixed endpoint samples for each encountered vertex."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source equation expands through out-neighbors."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Query a high-out-degree source whose neighbors also have broad, weakly overlapping out-neighborhoods, then request additional recursive refinement.",
    "uncertainty": "Caching and overlap can reduce realized fanout."
  }
}
```
