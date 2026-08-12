# Bitmap overhead outweighs locality benefit

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The variants differ only in visited-state representation."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Graph",
      "BFS sources",
      "Thread count",
      "Direction policy",
      "Hardware"
    ],
    "expected_observation": "Both optimized variants match the oracle, but bitmap elapsed time or traffic exceeds conventional BFS",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "large-diameter bitmap reversal",
    "graph_scale": "Symbolic graph large enough that bitmap residency and access are measurable",
    "graph_shape": "A sparse large-diameter graph with small frontiers",
    "independent_oracle": "Exact BFS distance or reachability vector from a serial queue implementation",
    "premises": [
      "The source reports the regression on some evaluated graphs and hardware."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The regression is not universal and may require platform-specific reproduction.",
    "varied_variables": [
      "Diameter",
      "Average degree",
      "Bitmap cache residency"
    ],
    "workload": "Run conventional and bitmap visited-state BFS from identical sources"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-TRACK-VISITED-WITH-BITMAPS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Controlled variants isolate visited-state representation."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "bitmap_overhead_time > avoided_random_access_time",
    "measurement_needed": "Compare bitmap and conventional variants while measuring cache behavior, memory traffic, and elapsed time.",
    "numeric_constants": [],
    "premises": [
      "The source reports opposite behavior across graph and hardware conditions."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The source does not give a portable cache or sparsity threshold.",
    "variables": [
      {
        "definition": "Time attributable to bitmap allocation, access, and maintenance",
        "symbol": "bitmap_overhead_time",
        "units": "time"
      },
      {
        "definition": "Time saved by avoiding distance-array reads",
        "symbol": "avoided_random_access_time",
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
    "text": "On some large-diameter graphs and hardware, the extra visited bitmap costs more than the random distance-array accesses it avoids.",
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
      "Benchmark controls cache state and thread placement."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports the same symptom."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Outputs match the independent BFS oracle while the bitmap variant records worse elapsed time or memory traffic than the conventional variant.",
    "uncertainty": "System noise can obscure a small crossover."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-BITMAP-OVERHEAD-OUTWEIGHS-LOCALITY",
  "name": "Bitmap overhead outweighs locality benefit",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Bitmap BFS falls behind the conventional distance-array baseline despite preserving reachability results.",
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
    "PAPER-2503.00430"
  ],
  "source_pointers": [
    {
      "claim_scope": "Visited bitmap regresses below baseline for some large-diameter graphs on the evaluated AMD system.",
      "locator_type": "SECTION",
      "locator_value": "Section 3, Evaluation",
      "page": 4,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Bitmap benefit depends on graph characteristics and hardware architecture.",
      "locator_type": "SECTION",
      "locator_value": "Section 4, Conclusion",
      "page": 5,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "FP-002"
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
    "text": "Run bitmap-based visited tracking on a sparse or large-diameter graph whose frontier accesses provide too little locality benefit for the extra bitmap traffic.",
    "uncertainty": "NONE"
  }
}
```
