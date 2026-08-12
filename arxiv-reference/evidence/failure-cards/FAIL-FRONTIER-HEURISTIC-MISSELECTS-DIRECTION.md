# Frontier Heuristic Misselects Direction

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Equal cardinality isolates properties not captured by the heuristic."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "vertex count",
      "edge count",
      "frontier cardinality",
      "source",
      "thread count"
    ],
    "expected_observation": "The heuristic selects the direction with larger measured work or latency while all outputs remain exact",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "equal frontier unequal work",
    "graph_scale": "Small graphs sufficient to force opposite cheaper directions at one level",
    "graph_shape": "Two unweighted graphs with equal frontier cardinality but sharply different frontier edge volume and unvisited-neighbor hit position",
    "independent_oracle": "Serial exact BFS plus fixed-direction per-level work counters",
    "premises": [
      "The source calls the criterion simplified and reports graph/platform dependence."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003",
      "FP-004"
    ],
    "uncertainty": "Parallel scheduling may add noise to elapsed time, so operation counts are primary.",
    "varied_variables": [
      "frontier degree distribution",
      "unvisited-neighbor order"
    ],
    "workload": "At the selected level, run top-down, bottom-up, and the cardinality heuristic"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-SWITCH-TRAVERSAL-BY-FRONTIER"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Runs share graph, source, frontier state, and exact output."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "bottom_up_scan_work + conversion_work > top_down_frontier_edge_work",
    "measurement_needed": "Count examined vertices, examined edges, conversion work, and elapsed time per level.",
    "numeric_constants": [],
    "premises": [
      "The two directions have different work surfaces and conversion is required on switches."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003",
      "FP-004"
    ],
    "uncertainty": "Hardware costs and edge ordering affect each term.",
    "variables": [
      {
        "definition": "unvisited-vertex and incoming-neighbor work in bottom-up mode",
        "symbol": "bottom_up_scan_work",
        "units": "operations"
      },
      {
        "definition": "array-to-bitmap or bitmap-to-array frontier work",
        "symbol": "conversion_work",
        "units": "operations"
      },
      {
        "definition": "outgoing-edge work from the current frontier",
        "symbol": "top_down_frontier_edge_work",
        "units": "operations"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "Both directions use the same exact BFS semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source uses a simplified cardinality criterion.",
      "It bypasses switching for large-diameter graphs and reports graph/platform dependence."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003",
      "FP-004"
    ],
    "text": "Frontier cardinality alone predicts the cheaper BFS direction only when it also tracks examined-edge work, conversion cost, and platform behavior.",
    "uncertainty": "The paper does not isolate conversion cost."
  },
  "confidence_rationale": {
    "assumptions": [
      "The source implementation uses the described criterion."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "All cited pages were rechecked."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003",
      "FP-004"
    ],
    "text": "The source defines the simplified criterion and reports that its observed misclassification did not significantly affect performance; the equal-cardinality workload is therefore an analytical test of omitted work terms, not a reported regression.",
    "uncertainty": "The fixture has not been executed and no universal threshold is inferred."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "Both fixed directions return the oracle distances."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Direction choice is based on the tested heuristic."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The chosen direction performs more work and takes longer than the alternate fixed direction for the same BFS level.",
    "uncertainty": "One failure level does not imply the hybrid loses end to end."
  },
  "failure_basis": "ANALYTICAL_COUNTEREXAMPLE",
  "failure_id": "FAIL-FRONTIER-HEURISTIC-MISSELECTS-DIRECTION",
  "name": "Frontier Heuristic Misselects Direction",
  "observable_symptom": {
    "assumptions": [
      "The heuristic chooses bottom-up for the constructed state and both directions preserve exact BFS output."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Bottom-up and top-down expose different work surfaces and switching requires representation conversion."
    ],
    "source_pointer_ids": [
      "FP-002",
      "FP-003",
      "FP-004"
    ],
    "text": "Selecting bottom-up at the wrong frontier shape adds full-vertex scanning and representation conversion without enough edge-work reduction.",
    "uncertainty": "The source did not observe a significant performance penalty from its reported misclassification; this symptom remains an unexecuted adversarial prediction."
  },
  "repair_options": [
    {
      "description": "Choose direction from estimated edge work and conversion cost, not frontier cardinality alone.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Disable bottom-up for graph classes lacking a validated benefit.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Calibrate the decision model per hardware and graph family.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-2503.00430"
  ],
  "source_pointers": [
    {
      "claim_scope": "Frontier-size direction criterion",
      "locator_type": "SECTION",
      "locator_value": "Section 2, BFS-Hybrid",
      "page": 2,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Direction conversion and level-synchronous execution",
      "locator_type": "ALGORITHM",
      "locator_value": "Listing 2",
      "page": 3,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Large-diameter top-down-only path and platform dependence",
      "locator_type": "SECTION",
      "locator_value": "Section 3, Evaluation",
      "page": 4,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "FP-003"
    },
    {
      "claim_scope": "Graph- and architecture-dependent boundary",
      "locator_type": "SECTION",
      "locator_value": "Section 4, Conclusion",
      "page": 5,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "FP-004"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "Equal frontier cardinality can coexist with sharply different incident-edge and bottom-up scan work."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source uses frontier cardinality as a simplified direction criterion."
    ],
    "source_pointer_ids": [
      "FP-003"
    ],
    "text": "Construct a frontier state where cardinality favors one direction while measured edge, scan, and conversion work favor the other.",
    "uncertainty": "The source-specific heuristic did not show a significant penalty in its reported cases, so this workload requires execution."
  }
}
```
