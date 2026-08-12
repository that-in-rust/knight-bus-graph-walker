# Bounded frontier misses nearest result

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The synthetic graph can make the bridge candidate the first evicted required route."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Distances",
      "Start vertices",
      "Neighbor ordering",
      "Recall target"
    ],
    "expected_observation": "Below the graph-dependent required capacity, the bounded search omits an exact nearest neighbor",
    "fixture_kind": "GRAPH",
    "fixture_name": "narrow bridge nearest-neighbor trap",
    "graph_scale": "Smallest graph that forces one required bridge candidate to be evicted",
    "graph_shape": "Two candidate basins joined through a route that is temporarily farther than competing dead ends",
    "independent_oracle": "Exhaustive distance scan over all vectors",
    "premises": [
      "The source defines bounded farthest-candidate eviction."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "This is a constructed fixture, not a source benchmark.",
    "varied_variables": [
      "Candidate capacity",
      "Number of competing dead ends"
    ],
    "workload": "Run bounded best-first graph search for a query whose exact neighbor lies beyond the bridge"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-BOUND-SEARCH-CANDIDATE-FRONTIER"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "A missed route can be exposed by comparison with exact nearest neighbors."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "candidate_capacity < required_capacity(query, graph, recall_target)",
    "measurement_needed": "Sweep candidate capacity and compare returned neighbors with exhaustive exact search.",
    "numeric_constants": [],
    "premises": [
      "The source reports dataset-dependent recall ceilings and defines eviction at capacity."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "No universal required capacity is reported.",
    "variables": [
      {
        "definition": "Maximum retained search candidates",
        "symbol": "candidate_capacity",
        "units": "candidates"
      },
      {
        "definition": "Minimum capacity needed by a query and graph to attain target recall",
        "symbol": "required_capacity",
        "units": "candidates"
      },
      {
        "definition": "Requested recall",
        "symbol": "recall_target",
        "units": "fraction"
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
    "text": "A fixed candidate capacity is not a universal guarantee of target recall because some graph and dataset combinations reach an accuracy ceiling.",
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
      "FP-001",
      "FP-002"
    ],
    "text": "The failure condition is bounded by the cited mechanism premise and negative result; the proposed fixture isolates the same condition without asserting an unsourced numeric threshold.",
    "uncertainty": "The precise crossover remains graph-, implementation-, and hardware-dependent unless the source states otherwise."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "The exhaustive scan is an independent oracle."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports recall ceilings and defines eviction."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The bounded search returns a different neighbor set from exhaustive exact search at the requested recall.",
    "uncertainty": "The smallest failing capacity depends on graph geometry."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-BOUNDED-FRONTIER-MISSES-NEAREST",
  "name": "Bounded frontier misses nearest result",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Increasing or bounding the candidate set does not reach the requested recall, while path length and I/O may remain high.",
    "uncertainty": "NONE"
  },
  "repair_options": [
    {
      "description": "Reject or reroute workloads once the symbolic failure predicate is observed.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Use a correctness-preserving fallback when the optimized path's assumptions fail.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Measure the unresolved crossover on the target graph and machine before admission.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-2101.12631"
  ],
  "source_pointers": [
    {
      "claim_scope": "Recall ceilings, candidate capacity, cache pressure, and path-length I/O.",
      "locator_type": "SECTION",
      "locator_value": "Section 5.3, Candidate Set Size and Query Path Length",
      "page": 10,
      "paper_id": "PAPER-2101.12631",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Exact bounded-frontier insertion and farthest-candidate eviction rule.",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 1",
      "page": 20,
      "paper_id": "PAPER-2101.12631",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "The graph can make a temporarily farther bridge candidate necessary to reach the exact nearest-neighbor region."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports dataset-dependent recall ceilings and defines bounded farthest-candidate eviction."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Search a graph whose route to a true nearest neighbor requires retaining more viable candidates than the configured frontier permits.",
    "uncertainty": "The source does not attribute its measured ceiling to this constructed route condition; the fixture must establish that causal bridge."
  }
}
```
