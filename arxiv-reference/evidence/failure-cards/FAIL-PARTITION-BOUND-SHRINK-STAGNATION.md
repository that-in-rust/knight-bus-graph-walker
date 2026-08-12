# Partition Bound Shrink Stagnation

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Visited positions can be controlled independently of count."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Partition length",
      "Visited count",
      "Frontier bitmap",
      "Neighbor data"
    ],
    "expected_observation": "Outputs match; interior scatter crosses the time guard while boundary clustering reduces work.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "Interior visited scatter",
    "graph_scale": "Symbolic partition length and visited count.",
    "graph_shape": "Two equal partitions with the same visited count placed at boundaries versus evenly through interiors.",
    "independent_oracle": "Full-range bottom-up scan with identical reachability updates.",
    "premises": [
      "The source ties shrink benefit to boundary locality."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The minimal realizable BFS graph producing each bitmap is unknown.",
    "varied_variables": [
      "Visited-position distribution"
    ],
    "workload": "Execute one bottom-up scan with repeated bound shrink and one full scan."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-SHRINK-VISITED-PARTITION-BOUNDS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Both paths execute identical bottom-up visitation semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "T_shrink + T_bounded_scan >= T_full_scan",
    "measurement_needed": "Measure boundary movement, skipped vertices, and scan time under controlled visited-position layouts.",
    "numeric_constants": [],
    "premises": [
      "The source reports little contraction for an adverse ordering."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "No universal dispersion crossover is reported.",
    "variables": [
      {
        "definition": "time spent advancing and retreating bounds",
        "symbol": "T_shrink",
        "units": "time"
      },
      {
        "definition": "time scanning the remaining bounded range",
        "symbol": "T_bounded_scan",
        "units": "time"
      },
      {
        "definition": "time scanning the unshrunk partition",
        "symbol": "T_full_scan",
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
    "text": "Boundary shrinking assumes visited vertices accumulate near the low or high ends of each partition.",
    "uncertainty": "The locality depends on graph ordering and traversal phase."
  },
  "confidence_rationale": {
    "assumptions": [
      "The oracle preserves the same bottom-up semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source gives algorithm and adverse partition evolution."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The stagnation workload is source-observed; the minimal controlled bitmap fixture is derived.",
    "uncertainty": "No local timing measurement was performed."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Bounds fail to contract enough to reduce total scan time.",
    "uncertainty": "The source reports the qualitative case but not its timing crossover."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-PARTITION-BOUND-SHRINK-STAGNATION",
  "name": "Partition Bound Shrink Stagnation",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Partition bounds contract little, so repeated shrink checks remove little scan work.",
    "uncertainty": "The source does not isolate shrink overhead."
  },
  "repair_options": [
    {
      "description": "Skip shrinking when recent boundary contraction does not repay checks.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Use full or bitmap-guided scans for interior-scattered visited state.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Relabel or partition vertices only when its separate cost is justified.",
      "repair_class": "CHANGE_REPRESENTATION"
    }
  ],
  "source_paper_ids": [
    "PAPER-2012.10026"
  ],
  "source_pointers": [
    {
      "claim_scope": "Repeated boundary shrink and locality premise.",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 6 and shrinking-partitions paragraph",
      "page": 7,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Partition-size evolution and unreordered workload behavior.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 3",
      "page": 8,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Workload-reduction scope.",
      "locator_type": "SECTION",
      "locator_value": "Section 5, Conclusion",
      "page": 10,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "FP-003"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "Visited vertices remain scattered through partition interiors, as observed for an unreordered larger Kronecker workload.",
    "uncertainty": "The source does not provide a formal dispersion statistic."
  }
}
```
