# Large Frontiers Overwhelm Sorting

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The fixture can grow frontier width independently."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph depth",
      "visited bitmap",
      "thread count",
      "distance type"
    ],
    "expected_observation": "At sufficient width, sort time or temporary bytes dominate the saved distance-write locality.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "wide-shuffled-bfs-frontier",
    "graph_scale": "Increase one-level frontier width under a fixed memory and thread configuration.",
    "graph_shape": "A root connected to a wide next level whose vertex identifiers are adversarially interleaved across the distance array.",
    "independent_oracle": "Exact serial BFS supplies distances; hardware counters compare only resource behavior among exact runs.",
    "premises": [
      "The source condition explicitly requires a small local frontier."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "No universal frontier threshold exists across machines.",
    "varied_variables": [
      "frontier width",
      "identifier disorder"
    ],
    "workload": "Run direct distance writes and sort-then-write with identical bitmap discovery."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-SORT-THEN-WRITE-DISTANCES"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Both terms are measured for the same frontier and thread count."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "sort_and_buffer_cost >= ordered_write_locality_savings",
    "measurement_needed": "Sweep frontier width and identifier disorder while measuring temporary bytes, sort time, and distance-write cache misses.",
    "numeric_constants": [],
    "premises": [
      "The mechanism adds sorting to improve write locality."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The source does not report an enabled-extension crossover.",
    "variables": [
      {
        "definition": "CPU and temporary-memory cost of retaining and sorting discovered identifiers.",
        "symbol": "sort_and_buffer_cost",
        "units": "work units"
      },
      {
        "definition": "Avoided distance-array cache and memory cost from ordered writes.",
        "symbol": "ordered_write_locality_savings",
        "units": "work units"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "The local discovery frontier remains small enough that sorting and buffering cost less than the locality gained by ordered distance writes.",
    "uncertainty": "The source states this as a conditional expectation."
  },
  "confidence_rationale": {
    "assumptions": [
      "Direct writes remain exact."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Sorting and buffering grow with discovered identifiers."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The source states the small-frontier precondition; the wide-frontier crossover is analytically inevitable but numerically unmeasured.",
    "uncertainty": "Actual cache savings may delay the crossover."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "Measurements isolate the optional sorting extension."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Both variants share discovery semantics."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Sort-then-write returns exact distances but uses no less peak temporary memory or elapsed work than direct writes on the wide frontier.",
    "uncertainty": "Reported paper benchmarks do not establish whether the extension was enabled."
  },
  "failure_basis": "ANALYTICAL_COUNTEREXAMPLE",
  "failure_id": "FAIL-LARGE-FRONTIERS-OVERWHELM-SORTING",
  "name": "Large Frontiers Overwhelm Sorting",
  "observable_symptom": {
    "assumptions": [
      "The control writes distances directly without that sort."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Sorting has nonzero work and requires retaining identifiers."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Temporary frontier memory and sorting work exceed the cache-miss savings of deferred ordered writes.",
    "uncertainty": "The cache and allocator crossover is platform-dependent."
  },
  "repair_options": [
    {
      "description": "Enable deferred sorting only below a measured per-thread frontier and disorder envelope.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Flush bounded sorted runs or bucket writes by distance-array region instead of retaining the full local frontier.",
      "repair_class": "CHANGE_SCHEDULE"
    }
  ],
  "source_paper_ids": [
    "PAPER-2503.00430"
  ],
  "source_pointers": [
    {
      "claim_scope": "Local discovered vertices may be sorted before deferred distance writes.",
      "locator_type": "SECTION",
      "locator_value": "Section 2, BFS-VisitedBitmap optional extension",
      "page": 2,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "The source expects benefit when the bitmap fits cache and the local frontier is small.",
      "locator_type": "ALGORITHM",
      "locator_value": "Listing 3 and continuation",
      "page": 3,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "The graph exposes a level with large fan-out and distance writes are not already costly enough to offset sorting."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The extension appends every discovered vertex and sorts the local frontier."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "A breadth-first level discovers a wide, poorly ordered frontier whose identifiers must all be retained and sorted.",
    "uncertainty": "Thread partitioning changes each local frontier size."
  }
}
```
