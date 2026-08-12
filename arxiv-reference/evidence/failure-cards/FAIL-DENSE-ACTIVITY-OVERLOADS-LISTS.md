# Dense Activity Overloads Lists

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "One message per pair is sufficient to activate each bin."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "partition count",
      "message payload",
      "thread count",
      "bin representation"
    ],
    "expected_observation": "Both methods visit every bin while the hierarchy adds list state and maintenance work",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "all bins active",
    "graph_scale": "Small complete partition-pair matrix with minimal messages per bin",
    "graph_shape": "Partitioned directed graph containing an active edge from every source partition to every destination partition",
    "independent_oracle": "Exhaustive bin scan recording exactly the same messages",
    "premises": [
      "The source hierarchy enumerates all active partition pairs."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "This is an analytical dense-case bound, not a source benchmark.",
    "varied_variables": [
      "activity density"
    ],
    "workload": "Execute one scatter and gather iteration with hierarchical activity lists and exhaustive probing"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-SKIP-INACTIVE-BINS-HIERARCHICALLY"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "The fixture makes every partition pair nonempty."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "active_partition_pairs = source_partitions * destination_partitions",
    "measurement_needed": "Measure list entries, insertion attempts, contention, allocated bytes, and gather work against exhaustive probing.",
    "numeric_constants": [],
    "premises": [
      "The hierarchy records nonempty bins."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-003"
    ],
    "uncertainty": "The equality is structural; runtime overhead still requires measurement.",
    "variables": [
      {
        "definition": "nonempty source-destination bins in the iteration",
        "symbol": "active_partition_pairs",
        "units": "partition pairs"
      },
      {
        "definition": "source partition count",
        "symbol": "source_partitions",
        "units": "partitions"
      },
      {
        "definition": "destination partition count",
        "symbol": "destination_partitions",
        "units": "partitions"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "Each active pair requires representable list state."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The hierarchy stores active destinations and source bins.",
      "Its stated benefit is avoiding empty-bin probes."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The active-list hierarchy saves work only when active partition pairs are sparse enough that list construction and traversal cost less than exhaustive bin probing.",
    "uncertainty": "Duplicate suppression and contention costs are unspecified."
  },
  "confidence_rationale": {
    "assumptions": [
      "The fixture activates all pairs."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Every active pair must appear in the hierarchy."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The dense counterexample is derived from the sourced list cardinality; the paper reports sparse-case benefits but no isolated dense overhead.",
    "uncertainty": "Contention and allocation constants remain unknown."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "Both methods gather identical messages."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Every bin is active."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-003"
    ],
    "text": "List-assisted execution performs no fewer bin visits and consumes additional metadata work or bytes.",
    "uncertainty": "Optimized list iteration may still improve locality despite equal cardinality."
  },
  "failure_basis": "ANALYTICAL_COUNTEREXAMPLE",
  "failure_id": "FAIL-DENSE-ACTIVITY-OVERLOADS-LISTS",
  "name": "Dense Activity Overloads Lists",
  "observable_symptom": {
    "assumptions": [
      "Exhaustive scanning remains the comparison baseline."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "No bin is empty in the fixture."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-003"
    ],
    "text": "The hierarchy approaches exhaustive pair cardinality, provides no skip benefit, and adds list maintenance, contention, or allocation overhead.",
    "uncertainty": "The source does not isolate list overhead."
  },
  "repair_options": [
    {
      "description": "Estimate active-pair density before selecting the hierarchy.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Switch to exhaustive dense scanning when activity approaches full coverage.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Bound activity-list capacity and duplicate insertion work.",
      "repair_class": "ADD_RESOURCE_BOUND"
    }
  ],
  "source_paper_ids": [
    "PAPER-1806.08092"
  ],
  "source_pointers": [
    {
      "claim_scope": "Activity hierarchy and quadratic all-bin probe problem",
      "locator_type": "SECTION",
      "locator_value": "Section 3.1.3, two-level Active List",
      "page": 8,
      "paper_id": "PAPER-1806.08092",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Observed role of lists in skipping partitions without active vertices",
      "locator_type": "PARAGRAPH",
      "locator_value": "Figure 7 setup discussion",
      "page": 20,
      "paper_id": "PAPER-1806.08092",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Work-efficiency rationale as partition count grows",
      "locator_type": "SECTION",
      "locator_value": "Section 7, cache-capped partition discussion",
      "page": 22,
      "paper_id": "PAPER-1806.08092",
      "pointer_id": "FP-003"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "The partitioned graph can place at least one active message in every bin."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source list contains every nonempty partition pair."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Activate every source-destination partition pair in one iteration while holding the message payload per pair minimal.",
    "uncertainty": "Concurrent insertion behavior is implementation-specific."
  }
}
```
