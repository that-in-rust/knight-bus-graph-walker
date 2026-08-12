# Premature Subgrid Reads Stale

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The fixture makes the omitted prerequisite's value change observable."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph",
      "partitioning",
      "orientation",
      "initial values",
      "arithmetic",
      "thread schedule"
    ],
    "expected_observation": "The malformed schedule consumes an older input version and differs from the synchronous result or requires recomputation.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "Missing subgrid prerequisite",
    "graph_scale": "Minimal partitioned topology with one observable prerequisite chain and integration step.",
    "graph_shape": "A directed block grid with a cross-shard value dependency feeding a future-value subgrid.",
    "independent_oracle": "A fully synchronous full-graph iteration with identical arithmetic.",
    "premises": [
      "The source explicitly retains shard and integration prerequisites for correctness."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "A coincidentally unchanged prerequisite value would not expose the failure.",
    "varied_variables": [
      "dependency metadata",
      "integration completion order"
    ],
    "workload": "Execute a correct dependency schedule and a schedule with the required integration delayed past dependent-task release."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-SCHEDULE-SUBGRIDS-BY-DEPENDENCY"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "At least one missing prerequisite changes a consumed value."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "exists task: start(task) < complete(prerequisites(task))",
    "measurement_needed": "Trace task release, prerequisite completion, consumed versions, and synchronous-oracle outputs.",
    "numeric_constants": [],
    "premises": [
      "The source requires prerequisites to resolve before eligible subgrid execution."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Equal-value coincidences can mask an invalid schedule on a particular input.",
    "variables": [
      {
        "definition": "A future-value subgrid computation",
        "symbol": "task",
        "units": "task"
      },
      {
        "definition": "All subgrid and integration tasks that determine the task input",
        "symbol": "prerequisites",
        "units": "set of tasks"
      },
      {
        "definition": "Task start time",
        "symbol": "start",
        "units": "time"
      },
      {
        "definition": "Latest prerequisite completion time",
        "symbol": "complete",
        "units": "time"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "Reading before a required integration can observe stale state."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source states that predecessor shard values must be determined and defines prerequisite completion rules."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Every future-value subgrid must wait for all subgrid and integration prerequisites that can change its inputs.",
    "uncertainty": "The source gives operational rules rather than a general formal proof."
  },
  "confidence_rationale": {
    "assumptions": [
      "A task reading a changed value before its producer completes can diverge from synchronous semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source says predecessor values and integration must complete before dependent work."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The counterexample follows directly from the source's prerequisite rule, while general dependency construction remains unproved.",
    "uncertainty": "No malformed-schedule experiment or implementation inspection was performed."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "The fixture's delayed integration changes a consumed value."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The sourced schedule requires prerequisite completion."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "A task starts before a value-changing prerequisite completes and its output differs from the synchronous oracle or is later invalidated.",
    "uncertainty": "This is an analytical counterexample, not a source-reported failure run."
  },
  "failure_basis": "ANALYTICAL_COUNTEREXAMPLE",
  "failure_id": "FAIL-PREMATURE-SUBGRID-READS-STALE",
  "name": "Premature Subgrid Reads Stale",
  "observable_symptom": {
    "assumptions": [
      "The independent synchronous oracle uses the same arithmetic."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source retains dependencies to preserve future-value correctness."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The relaxed execution reads stale values and differs from the fully synchronous iteration, or it must recompute the prematurely executed subgrid.",
    "uncertainty": "Some graphs may coincidentally produce equal values despite the stale read."
  },
  "repair_options": [
    {
      "description": "Release a subgrid only after versioned prerequisite and integration completion checks.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Restore the coarser shard barrier when dependency completeness is uncertain.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Use the synchronous iteration for algorithms without a verified future-value dependency graph.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Audit dependency construction and stale-read incidence across target algorithms.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-HASH-b12240577b20eaad"
  ],
  "source_pointers": [
    {
      "claim_scope": "Primary-secondary alternation and shard-barrier correctness dependency.",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 2 and following paragraphs",
      "page": 6,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Subgrid and integration prerequisites for future-value execution.",
      "locator_type": "SECTION",
      "locator_value": "Section 2.2 continuation",
      "page": 7,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "The omitted dependency carries a value used by the released task."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The sourced schedule relaxes barriers only after prerequisite analysis."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "A scheduler omits or prematurely marks a cross-shard prerequisite complete, releasing a dependent subgrid before integration updates its input.",
    "uncertainty": "This malformed schedule is constructed analytically and not source-measured."
  }
}
```
