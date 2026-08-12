# Shrink Visited Partition Bounds

- Pattern ID: `PAT-SHRINK-VISITED-PARTITION-BOUNDS`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus uses contiguous partitions and exact visited state.",
      "Its receipt can report actual retained range per iteration."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source safely removes only fully visited boundary runs.",
      "The source observes strong shrink after RCM and little shrink without it."
    ],
    "source_pointer_ids": [
      "SP-036",
      "SP-037"
    ],
    "text": "Knight Bus could retain compact per-partition active bounds across bottom-up iterations and charge only the bounded scan ranges, but the quote must fall back to full-partition work when locality does not materialize.",
    "uncertainty": "Admission cannot assume shrink before observing workload locality."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-036"
    ],
    "text": "Each work-stealing partition is shrunk immediately before its bottom-up scan and again before the second degree-ordered neighbor pass.",
    "uncertainty": "The source does not isolate shrink-check overhead."
  },
  "confidence_rationale": {
    "assumptions": [
      "The cited paper and pointers accurately represent the evaluated mechanism."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The cited source pointers describe or evaluate the mechanism.",
      "G05 did not independently reproduce the source result or inspect implementation code."
    ],
    "source_pointer_ids": [
      "SP-036",
      "SP-037",
      "SP-038"
    ],
    "text": "The source specifies the boundary operation and reports partition-size trajectories across graph scales, but does not isolate its cost or provide an independent reproduction.",
    "uncertainty": "Grade C is limited to the reported reordered bottom-up BFS setting."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-036",
      "SP-037"
    ],
    "text": "Vertices are held in contiguous partitions over an RCM-reordered identifier space with a visited bitmap.",
    "uncertainty": "Other orderings may not cluster visited vertices sufficiently."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-037"
      ],
      "text": "The mechanism removes little work when visited vertices are scattered through partition interiors, which the source observes for unreordered larger Kronecker graphs.",
      "uncertainty": "Boundary checks still add work even when no shrink occurs."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-SHRINK-VISITED-PARTITION-BOUNDS",
  "falsifying_test": {
    "controlled_variables": [
      "partition count",
      "partition sizes",
      "frontier bitmap",
      "thread count",
      "graph edge count"
    ],
    "failure_signal": "Shrinking omits an unvisited candidate, changes the next frontier, or claims a reduced scan when active bounds do not contract",
    "fixture": "Two equal-size bottom-up BFS fixtures with identical frontier cardinalities but boundary-clustered versus interior-scattered visited vertices",
    "independent_oracle": "Full-partition bottom-up BFS with identical visited and next-frontier outputs",
    "scope": "Smallest correctness and locality falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-036"
    ],
    "text": "Removing only a contiguous prefix and suffix whose vertices are all visited preserves every still-unvisited candidate inside the partition.",
    "uncertainty": "Interior visited vertices remain and are filtered by the visited bitmap."
  },
  "knight_bus_algorithm_families": [
    "BFS",
    "WCC",
    "BOTTOM_UP_TRAVERSAL",
    "LEVEL_SYNCHRONOUS_TRAVERSAL"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-036"
    ],
    "text": "Before each bottom-up scan, advance the lower bound and retreat the upper bound past already visited vertices; repeat the shrink before later subpasses and levels.",
    "uncertainty": "Savings depend on visited vertices clustering at partition boundaries."
  },
  "name": "Shrink Visited Partition Bounds",
  "pattern_id": "PAT-SHRINK-VISITED-PARTITION-BOUNDS",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-036"
    ],
    "text": "Bottom-up BFS repeatedly scans unvisited candidates, including partition boundary regions whose vertices were already visited in earlier bottom-up levels.",
    "uncertainty": "The source focuses on contiguous partitions after RCM ordering."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-036"
    ],
    "text": "Partition bounds are recomputed from current visited state before consecutive bottom-up scans.",
    "uncertainty": "No asymptotic bound for repeated boundary checks is stated."
  },
  "related_pattern_ids": [
    "PAT-RELABEL-VERTICES-FOR-LOCALITY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-036"
    ],
    "text": "The visited bitmap and two mutable bounds per partition are sufficient to identify the retained scan range.",
    "uncertainty": "The broader BFS state remains resident but is not caused by this mechanism."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure memory bytes scanned and mapped-page faults with and without shrinking.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The source reports in-memory workload reduction, not device I/O."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure serialized partition metadata if bounds persist between runs.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "No persistent metadata size is reported."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "The reported stable shrink behavior depends on prior RCM reordering and partition creation.",
      "measurement_needed": "Measure and report ordering and partition preparation separately.",
      "premises": [],
      "source_pointer_ids": [
        "SP-036",
        "SP-037"
      ],
      "status": "SOURCED",
      "uncertainty": "The card does not include RCM's substantial cost as zero."
    },
    "ram": {
      "assumptions": [],
      "expression": "A visited bitmap plus mutable lower and upper bounds for each existing bottom-up partition; the graph and frontier state are shared with BFS.",
      "measurement_needed": "Measure partition-bound metadata and whole-phase RSS.",
      "premises": [],
      "source_pointer_ids": [
        "SP-036"
      ],
      "status": "SOURCED",
      "uncertainty": "Incremental bytes are not measured separately."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "Two changing bounds per active partition are maintained during bottom-up levels.",
      "measurement_needed": "Measure temporary partition and scheduler bytes per level.",
      "premises": [],
      "source_pointer_ids": [
        "SP-036"
      ],
      "status": "SOURCED",
      "uncertainty": "Allocator and work-stealing queue bytes are not isolated."
    }
  },
  "source_domain": "bottom-up breadth-first search",
  "source_paper_ids": [
    "PAPER-2012.10026"
  ],
  "source_pointers": [
    {
      "claim_scope": "Repeated boundary shrink and locality precondition",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 6 lines 10 and 18 plus shrinking-partitions paragraph",
      "page": 7,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "SP-036"
    },
    {
      "claim_scope": "Partition-size evolution across bottom-up levels",
      "locator_type": "FIGURE",
      "locator_value": "Figure 3",
      "page": 8,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "SP-037"
    },
    {
      "claim_scope": "Workload reduction between consecutive bottom-up steps",
      "locator_type": "SECTION",
      "locator_value": "Section 5, conclusion",
      "page": 10,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "SP-038"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-036",
      "SP-037"
    ],
    "text": "Only vertices within the current partition bounds are scanned by the bottom-up kernel.",
    "uncertainty": "Interior visited vertices are still read for the visited check."
  },
  "unknown_when": [
    {
      "assumptions": [
        "No uncited section of the fully read paper resolves the named boundary."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The cited source pointers delimit the mechanism, evaluated conditions, or stated analysis."
      ],
      "source_pointer_ids": [
        "SP-036",
        "SP-037"
      ],
      "text": "The paper does not isolate shrink overhead, prove a worst-case scan bound, or test the mechanism without RCM on diverse directed graphs.",
      "uncertainty": "The mechanism has no source-backed universal benefit."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-036",
        "SP-037"
      ],
      "text": "Visited vertices become spatially clustered at the ends of contiguous partitions across consecutive bottom-up levels, as in the reordered fixtures.",
      "uncertainty": "The amount of shrink varies by level and graph order."
    }
  ]
}
```
