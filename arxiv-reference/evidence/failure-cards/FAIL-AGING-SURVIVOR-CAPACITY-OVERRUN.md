# Aging Survivor Capacity Overrun

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The simulator implements the same slot and tick semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "System capacity",
      "Processor count",
      "Arrival sequence",
      "Aging predicate"
    ],
    "expected_observation": "Below a sufficient source condition, a jeopardy edge can leave the tail and produce FAIL; admitted configurations preserve every edge.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "Aging capacity boundary",
    "graph_scale": "Symbolic system capacity and processor count with starting free space and payload bandwidth varied around the source conditions.",
    "graph_shape": "A full streaming connectivity state with a controlled survivor fraction and uninterrupted unique arrivals.",
    "independent_oracle": "A finite event-by-event simulator of edge slots, survivor classification, arrivals, and tail exits.",
    "premises": [
      "The source defines the jeopardy condition and sufficient inequalities."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The sufficient boundary need not be necessary for every edge order.",
    "varied_variables": [
      "Starting free space",
      "Survivor fraction",
      "Payload bandwidth"
    ],
    "workload": "Issue an aging predicate while continuing to ingest primary-slot edges."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-RECYCLE-SURVIVORS-DURING-AGING"
  ],
  "breakpoint_equation": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "expression": "free_space_available < free_space_required OR bandwidth_expansion < bandwidth_required",
    "measurement_needed": "Validate the source inequality and jeopardy trace with exact finite parameters before admission.",
    "numeric_constants": [],
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The source conditions are sufficient and rely on estimated workload fractions.",
    "variables": [
      {
        "definition": "unoccupied system edge slots when aging begins",
        "symbol": "free_space_available",
        "units": "edge slots"
      },
      {
        "definition": "source-derived sufficient open-space requirement",
        "symbol": "free_space_required",
        "units": "edge slots"
      },
      {
        "definition": "payload capacity per circulating bundle under the source definition",
        "symbol": "bandwidth_expansion",
        "units": "slots per bundle"
      },
      {
        "definition": "source-derived sufficient payload expansion for the declared survivor, downtime, uniqueness, and processor parameters",
        "symbol": "bandwidth_required",
        "units": "slots per bundle"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Survivor recycling assumes aging starts with enough open storage and payload bandwidth to absorb survivors and uninterrupted new arrivals.",
    "uncertainty": "The theorem depends on estimated survivor and uniqueness fractions."
  },
  "confidence_rationale": {
    "assumptions": [
      "The local interpretation preserves source variables and event order."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source proves correctness under stated conditions and defines jeopardy failure."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The fail signal and sufficient conditions are theorem-backed source claims; necessity outside that envelope remains uncertain.",
    "uncertainty": "No local simulator was executed in G06."
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
    "text": "The system emits its defined FAIL signal when a jeopardy edge cannot settle before leaving the full tail.",
    "uncertainty": "Failure depends on edge order when sufficient conditions are not met."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-AGING-SURVIVOR-CAPACITY-OVERRUN",
  "name": "Aging Survivor Capacity Overrun",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "A jeopardy edge exits the full tail and the system raises its defined FAIL condition.",
    "uncertainty": "The theorem provides sufficient conditions rather than a measured failure distribution."
  },
  "repair_options": [
    {
      "description": "Start aging only while the source-derived open-space and bandwidth conditions hold.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Reserve explicit survivor and arrival capacity during aging.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Throttle or redirect ingestion when the aging admission guard cannot be satisfied.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-2112.00098"
  ],
  "source_pointers": [
    {
      "claim_scope": "Jeopardy edge and explicit FAIL condition.",
      "locator_type": "THEOREM",
      "locator_value": "Figure 8 and Property 1",
      "page": 12,
      "paper_id": "PAPER-2112.00098",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Reserved-space and bandwidth conditions for successful aging.",
      "locator_type": "THEOREM",
      "locator_value": "Lemma 3 and Theorem 4",
      "page": 13,
      "paper_id": "PAPER-2112.00098",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Conditional indefinite operation across repeated aging.",
      "locator_type": "THEOREM",
      "locator_value": "Theorem 5",
      "page": 14,
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
    "text": "Aging starts too late, retains too many stored edges, or uses insufficient bundle bandwidth.",
    "uncertainty": "Input-stream uniqueness and survivor fractions must be estimated."
  }
}
```
