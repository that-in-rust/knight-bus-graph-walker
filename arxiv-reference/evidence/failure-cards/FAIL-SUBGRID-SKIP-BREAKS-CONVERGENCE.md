# Subgrid Skip Breaks Convergence

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The unsafe predicate is intentionally adversarial and not attributed to the authors."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph",
      "partitioning",
      "update rule",
      "initial state"
    ],
    "expected_observation": "Selective execution reports convergence while the delayed vertex remains incorrectly unchanged",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "late activating subgrid",
    "graph_scale": "Small multi-subgrid path with one delayed dependency",
    "graph_shape": "Partitioned directed path whose final segment lies in a subgrid activated only after an earlier segment updates state",
    "independent_oracle": "Exhaustive subgrid scan to the algorithm-defined fixed point",
    "premises": [
      "The source exposes user-controlled subgrid skipping."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "Framework duplicate and convergence detection behavior must be declared.",
    "varied_variables": [
      "skip predicate"
    ],
    "workload": "Run a convergence algorithm with permanent inactive-first skip and with exhaustive scheduling"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-REUSE-LOADED-SUBGRIDS-SELECTIVELY"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "The selected algorithm requires the omitted dependency."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "required_late_subgrid_skipped = correctness_failure",
    "measurement_needed": "Compare output and convergence trace against exhaustive scheduling.",
    "numeric_constants": [],
    "premises": [
      "The scheduler can skip at subgrid granularity."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "No numeric breakpoint is applicable to this correctness counterexample.",
    "variables": [
      {
        "definition": "whether an omitted subgrid contains a later-required dependency",
        "symbol": "required_late_subgrid_skipped",
        "units": "boolean"
      },
      {
        "definition": "whether output differs from the exhaustive oracle",
        "symbol": "correctness_failure",
        "units": "boolean"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "The user scheduler, rather than the framework, establishes safety."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The scheduler may suppress a whole subgrid.",
      "The source limits this freedom to suitable asynchronous workloads."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Selective skipping is correct only if a skipped subgrid cannot contain an edge needed by the current or a later convergence step.",
    "uncertainty": "The paper does not formalize a general safe-skip predicate."
  },
  "confidence_rationale": {
    "assumptions": [
      "The adversarial scheduler is possible under the exposed predicate."
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
    "text": "The source establishes scheduler power and workload limits; the incorrect fixed point is a pure analytical counterexample to an unsafe scheduler.",
    "uncertainty": "No claim is made that the source implementation uses this unsafe policy."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "Both runs use identical update semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The omitted subgrid is necessary for the oracle result."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Output differs from exhaustive execution or convergence is declared before the delayed dependency is processed.",
    "uncertainty": "This invalidates an unsafe policy, not all selective reuse policies."
  },
  "failure_basis": "ANALYTICAL_COUNTEREXAMPLE",
  "failure_id": "FAIL-SUBGRID-SKIP-BREAKS-CONVERGENCE",
  "name": "Subgrid Skip Breaks Convergence",
  "observable_symptom": {
    "assumptions": [
      "The independent oracle scans every required subgrid until convergence."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "A required later edge is never processed."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The run terminates with a fixed point or reachability result different from exhaustive subgrid traversal.",
    "uncertainty": "Choice of algorithm determines the smallest graph."
  },
  "repair_options": [
    {
      "description": "Require a proved conservative safe-skip predicate for the selected algorithm.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Rescan all subgrids before declaring convergence.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Limit selective skip to algorithms and schedules with demonstrated monotone safety.",
      "repair_class": "SPECIALIZE_WORKLOAD"
    }
  ],
  "source_paper_ids": [
    "PAPER-HASH-b12240577b20eaad"
  ],
  "source_pointers": [
    {
      "claim_scope": "User-controlled subgrid skip and reuse capability",
      "locator_type": "SECTION",
      "locator_value": "Section 2.1 opening",
      "page": 4,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Scheduler predicate controls whether each subgrid loads",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 1, grid streamer",
      "page": 5,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Skip and reprocess applicability to asynchronous workloads",
      "locator_type": "SECTION",
      "locator_value": "Section 5.1, Asynchronous systems",
      "page": 18,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "FP-003"
    },
    {
      "claim_scope": "Source-stated algorithm-coverage limitations",
      "locator_type": "SECTION",
      "locator_value": "Section 6, Conclusion",
      "page": 20,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "FP-004"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "Later vertex-state changes can make previously unneeded edges relevant."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The scheduler returns the load decision for each subgrid."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Use a graph where a subgrid is inactive initially but receives a later dependency; apply a scheduler that permanently skips it after the first inactive observation.",
    "uncertainty": "The source does not claim this scheduler is safe."
  }
}
```
