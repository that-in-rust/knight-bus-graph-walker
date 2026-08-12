# Switch Traversal By Frontier

- Pattern ID: `PAT-SWITCH-TRAVERSAL-BY-FRONTIER`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus executes exact level-synchronous BFS and can access the neighbor orientation required by bottom-up traversal."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-SWITCH-TRAVERSAL-BY-FRONTIER:resident_state",
      "PAT-SWITCH-TRAVERSAL-BY-FRONTIER:temporary_storage",
      "PAT-SWITCH-TRAVERSAL-BY-FRONTIER:unknown_when"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "An A007 feasibility estimate for exact BFS should admit both direction-specific frontier representations before treating per-level direction switching as an optional work-reduction choice.",
    "uncertainty": "The paper provides no storage-backed execution evidence, incoming-neighbor storage cost, or portable switch threshold."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "At each level, inspect current frontier cardinality, execute one traversal direction in parallel, synchronize, update depth and frontier state, and convert frontier representation when direction changes.",
    "uncertainty": "The source does not isolate representation-conversion or barrier cost."
  },
  "confidence_rationale": {
    "assumptions": [
      "The plotted BFS-Hybrid implementation follows Listing 2 and uses the stated baseline consistently."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Listing 2 specifies the per-level direction decision and frontier conversion.",
      "Section 3 and Figures 2-3 report source benchmark evidence for BFS-Hybrid."
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-004"
    ],
    "text": "Confidence is moderate-low because Listing 2 makes the direction switch concrete and the paper benchmarks a hybrid variant on two platforms, but it does not isolate conversion cost or reproduce the result in this campaign.",
    "uncertainty": "No independent reproduction, code inspection, conversion-cost ablation, or storage-backed test was performed."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Adjacency is held in row-pointer and column arrays; top-down uses a frontier array, bottom-up uses current and next frontier bitmaps, and the distance array remains the visited-state test in this variant.",
    "uncertainty": "Listing 2 omits complete buffer-capacity and byte-layout details."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-005"
      ],
      "text": "For graphs classified as large-diameter, the evaluated implementation bypasses direction switching and uses a separate top-down-only path.",
      "uncertainty": "The paper does not isolate the cost of mistakenly enabling bottom-up traversal on every large-diameter graph."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-SWITCH-TRAVERSAL-BY-FRONTIER",
  "falsifying_test": {
    "controlled_variables": [
      "source vertex",
      "thread count",
      "distance-based visited test",
      "CSR layout",
      "frontier buffer capacities",
      "switch threshold"
    ],
    "failure_signal": "The hybrid traversal returns a distance different from the oracle or performs more examined-edge work and longer elapsed time than both fixed directions on the broad-frontier fixture",
    "fixture": "Two unweighted graphs with equal vertex and edge counts but contrasting frontier profiles, one dominated by narrow frontiers and one containing a broad middle frontier",
    "independent_oracle": "A serial exact BFS distance vector plus fixed-direction top-down and bottom-up reference runs",
    "scope": "Smallest direction-switching test description only; bitmap visited-state and deferred sorted writes are disabled, and no G09 experiment packet exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Each iteration preserves one BFS depth: top-down expands the current frontier, bottom-up accepts an unvisited vertex after finding a current-frontier neighbor, and both produce vertices for the same next depth.",
    "uncertainty": "Correctness is stated for unweighted, level-synchronous BFS with barriers between levels."
  },
  "knight_bus_algorithm_families": [
    "BFS",
    "UNWEIGHTED_SHORTEST_PATHS",
    "REACHABILITY",
    "BETWEENNESS_CENTRALITY"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Choose top-down traversal for a small current frontier and bottom-up traversal for a large current frontier, using a threshold equal to five percent of total vertices in the reported implementation.",
    "uncertainty": "The paper presents the threshold as its simplified implementation choice, not as a universal optimum."
  },
  "name": "Switch Traversal By Frontier",
  "pattern_id": "PAT-SWITCH-TRAVERSAL-BY-FRONTIER",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Top-down BFS can traverse unnecessary edges when a frontier is large, while bottom-up BFS can avoid that work by scanning unvisited vertices for a current-frontier neighbor.",
    "uncertainty": "The source studies level-synchronous in-memory BFS on shared-memory multicore systems."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "The next frontier cardinality and direction decision are recomputed once per BFS level; frontier representation is regenerated when the direction changes.",
    "uncertainty": "The source does not quantify conversion work separately."
  },
  "related_pattern_ids": [
    "PAT-SORT-THEN-WRITE-DISTANCES",
    "PAT-TRACK-VISITED-WITH-BITMAPS"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "The graph arrays, distance array, frontier array, current and next frontier bitmaps, frontier counters, depth, and direction flag remain in shared memory during traversal.",
    "uncertainty": "Peak whole-process memory and allocation capacities are not reported."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "For any storage-backed implementation, measure cold-cache graph reads and spill writes by traversal direction.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The source evaluates in-memory execution and reports no storage-I/O model."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Specify retained graph and result formats and measure their bytes independently.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper specifies no persistent graph or result format for this mechanism."
    },
    "preprocessing": {
      "assumptions": [
        "Graph cardinality is available from graph metadata rather than requiring a separate graph scan."
      ],
      "expression": "Obtain vertex cardinality and initialize distance and frontier state before the first level",
      "measurement_needed": "Measure metadata acquisition, frontier initialization, and distance initialization outside traversal time.",
      "premises": [
        "The switch compares frontier cardinality with total vertex cardinality and Listing 2 begins from initialized BFS state."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "DERIVED",
      "uncertainty": "Initialization time and metadata acquisition are not reported separately."
    },
    "ram": {
      "assumptions": [
        "The implementation retains the representations shown in Listing 2 for the duration of traversal."
      ],
      "expression": "CSR storage plus distance-array storage plus array and bitmap frontier storage plus thread-local frontier buffers",
      "measurement_needed": "Measure peak RSS and allocated bytes for every frontier representation at the maximum frontier.",
      "premises": [
        "Listing 2 uses graph arrays, a distance array, array and bitmap frontier forms, and parallel local frontier state."
      ],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "DERIVED",
      "uncertainty": "Allocation capacities, bitmap padding, and simultaneous frontier-form lifetimes are implementation-dependent."
    },
    "temporary_storage": {
      "assumptions": [
        "These traversal-only structures can be released after BFS completes."
      ],
      "expression": "Current and next frontier arrays and bitmaps plus thread-local frontier capacity",
      "measurement_needed": "Record allocated and occupied frontier capacity at each BFS level.",
      "premises": [
        "Listing 2 switches between array and bitmap frontier forms and performs parallel frontier construction."
      ],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "DERIVED",
      "uncertainty": "Peak frontier cardinality and over-allocation factors depend on graph and implementation."
    }
  },
  "source_domain": "shared-memory multicore breadth-first search",
  "source_paper_ids": [
    "PAPER-2503.00430"
  ],
  "source_pointers": [
    {
      "claim_scope": "Top-down and bottom-up work selection, simplified frontier-size criterion, and five-percent switch threshold",
      "locator_type": "SECTION",
      "locator_value": "Section 2, BFS-Hybrid",
      "page": 2,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Level-synchronous direction decision and conversion between array and bitmap frontier forms",
      "locator_type": "ALGORITHM",
      "locator_value": "Listing 2",
      "page": 3,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Graph classification, separate top-down path for large-diameter graphs, and hybrid benchmark interpretation",
      "locator_type": "SECTION",
      "locator_value": "Section 3, Evaluation",
      "page": 4,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Reported BFS-Hybrid performance across graph classes and two multicore platforms",
      "locator_type": "FIGURE",
      "locator_value": "Figures 2 and 3",
      "page": 4,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Evaluated graph sizes, diameters, and average degrees",
      "locator_type": "TABLE",
      "locator_value": "Table 1",
      "page": 5,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "Graph- and architecture-dependent optimization boundary and average-degree heuristic conclusion",
      "locator_type": "SECTION",
      "locator_value": "Section 4, Conclusion",
      "page": 5,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "SP-006"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Top-down streams adjacency lists of current-frontier vertices; bottom-up streams unvisited vertices and their neighbors until a current-frontier neighbor is found.",
    "uncertainty": "Streaming denotes memory traversal rather than storage I/O."
  },
  "unknown_when": [
    {
      "assumptions": [
        "Threshold quality depends on frontier evolution, conversion cost, memory hierarchy, and thread synchronization."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source replaces edge-count criteria with one fixed frontier-size threshold.",
        "The evaluation reports graph- and architecture-dependent outcomes and an average-degree classifier with exceptions."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003",
        "SP-006"
      ],
      "text": "The best switch threshold and the point at which direction-conversion overhead cancels edge-traversal savings remain unknown for a new graph and machine.",
      "uncertainty": "No threshold sweep or workload-independent selection rule is reported."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-004",
        "SP-005"
      ],
      "text": "Small-diameter graphs create large frontiers for which the evaluated hybrid traversal improves over conventional BFS on both reported platforms.",
      "uncertainty": "The reported benefit varies by graph and platform, and the plotted hybrid variant includes implementation details beyond the abstract direction choice."
    }
  ]
}
```
