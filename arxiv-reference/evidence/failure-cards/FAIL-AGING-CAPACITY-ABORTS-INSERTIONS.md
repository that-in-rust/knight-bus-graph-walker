# Aging Capacity Aborts Insertions

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The fixture obeys the same finite-capacity model."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "pipeline capacities",
      "arrival order",
      "query schedule",
      "aging policy"
    ],
    "expected_observation": "An insertion fails or query availability is suspended when temporary plus retained state exhausts capacity",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "aging capacity exhaustion",
    "graph_scale": "Small stream configured so retained and incoming state approach the declared capacity",
    "graph_shape": "Finite edge stream whose retained connectivity edges overlap with sustained arrivals during aging",
    "independent_oracle": "Offline dynamic-connectivity oracle plus exact slot accounting",
    "premises": [
      "The source explicitly defines failure on exhausted placement."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The smallest concrete stream depends on configured capacities and is not assigned a numeric size.",
    "varied_variables": [
      "arrival intensity",
      "retained-edge fraction"
    ],
    "workload": "Insert edges continuously while triggering aging and connectivity maintenance"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-RECYCLE-SURVIVORS-DURING-AGING"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "All capacity classes use the same slot accounting."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "incoming_edge_state + aging_duplicate_state + survivor_state > free_pipeline_capacity",
    "measurement_needed": "Trace occupancy by state class and record the first rejected insertion.",
    "numeric_constants": [],
    "premises": [
      "Aging can retain and temporarily duplicate edge state.",
      "The implementation fails when an edge cannot be placed."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The paper formulas depend on estimated stream parameters.",
    "variables": [
      {
        "definition": "state required by newly arriving edges",
        "symbol": "incoming_edge_state",
        "units": "edge slots"
      },
      {
        "definition": "temporary duplicate edge state created by aging",
        "symbol": "aging_duplicate_state",
        "units": "edge slots"
      },
      {
        "definition": "retained connectivity-supporting state",
        "symbol": "survivor_state",
        "units": "edge slots"
      },
      {
        "definition": "currently available stream-pipeline storage",
        "symbol": "free_pipeline_capacity",
        "units": "edge slots"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "Arrival and aging schedules obey the model."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source can hold an edge twice during aging and explicitly fails when capacity is exhausted."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Stream aging remains live only while temporary duplication, survivor recycling, and incoming edges fit the finite pipeline capacity under the declared schedule.",
    "uncertainty": "Capacity planning uses estimated parameters and unmodeled data-dependent effects remain."
  },
  "confidence_rationale": {
    "assumptions": [
      "The implementation follows the described finite-capacity pipeline."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Every page of the frozen paper was inspected."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The source explicitly identifies the capacity failure and aging slowdown, while the exact target stream breakpoint remains configuration dependent.",
    "uncertainty": "No numeric capacity threshold is invented."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "Instrumentation records every occupied slot."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports explicit failure under capacity exhaustion."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The first rejected insertion or unsettled jeopardy edge occurs while the offline oracle remains well-defined.",
    "uncertainty": "A different capacity schedule may avoid the failure."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-AGING-CAPACITY-ABORTS-INSERTIONS",
  "name": "Aging Capacity Aborts Insertions",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The system returns failure for an insertion or jeopardy settlement, and ingestion slows during aging.",
    "uncertainty": "The source prototype and parameter regime limit generalization."
  },
  "repair_options": [
    {
      "description": "Reserve capacity for incoming, survivor, and temporary aging state separately.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Throttle or reject ingestion before the no-settlement state.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Move aging to lower-intensity periods as suggested by the source discussion.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Spill retained or duplicate state to a slower overflow path.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-2112.00098"
  ],
  "source_pointers": [
    {
      "claim_scope": "Explicit failure when a jeopardy edge cannot be settled",
      "locator_type": "SECTION",
      "locator_value": "Section 3.3, aging jeopardy handling",
      "page": 12,
      "paper_id": "PAPER-2112.00098",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Insertion failure when system capacity is exhausted",
      "locator_type": "SECTION",
      "locator_value": "Section 4, implementation capacity behavior",
      "page": 16,
      "paper_id": "PAPER-2112.00098",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Aging-induced ingestion slowdown and workload dependence",
      "locator_type": "SECTION",
      "locator_value": "Section 5, aging performance discussion",
      "page": 20,
      "paper_id": "PAPER-2112.00098",
      "pointer_id": "FP-003"
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
    "text": "Sustain insertion while aging retains or duplicates enough edges that a jeopardy edge cannot settle before free capacity is exhausted.",
    "uncertainty": "The exact triggering stream depends on system parameters and graph history."
  }
}
```
