# Iobound Updates Empty Stalls

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The fixture can independently separate ready CPU work from update-side I/O wait."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "search trace",
      "update semantics",
      "storage device",
      "queue policy",
      "checkpoint policy"
    ],
    "expected_observation": "Record useful update progress, idle CPU time, search latency, and update-side I/O contention.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "Storage-wait update pairing",
    "graph_scale": "Minimal search and update operations exposing overlapping storage waits.",
    "graph_shape": "A disk search trace paired with an update whose next required state is also on storage.",
    "independent_oracle": "The same query/update sequence executed without co-execution, with final index state checked independently.",
    "premises": [
      "The source derives benefit from complementary search I/O stalls and CPU-bound update pruning."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Storage caching can change which update phases are ready in a given run.",
    "varied_variables": [
      "fraction of update time blocked on I/O",
      "ready CPU slice duration"
    ],
    "workload": "Run the update scheduler during fixed search stalls while varying whether update CPU work is immediately ready."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-CHECKPOINT-PRUNING-BETWEEN-STALLS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Update progress is useful only when ready CPU work exceeds scheduling and checkpoint cost."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "T_useful_update <= T_schedule + T_checkpoint",
    "measurement_needed": "Measure ready CPU slices, update I/O waits, scheduler cost, checkpoint cost, and search interference.",
    "numeric_constants": [],
    "premises": [
      "The source limits the mechanism to compute-bound update work and identifies an I/O-bound counter-domain."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The source does not quantify a portable useful-work threshold.",
    "variables": [
      {
        "definition": "Ready CPU time usefully spent on update work during a stall",
        "symbol": "T_useful_update",
        "units": "time"
      },
      {
        "definition": "Queue and dispatch overhead",
        "symbol": "T_schedule",
        "units": "time"
      },
      {
        "definition": "Checkpoint, yield, and resume overhead",
        "symbol": "T_checkpoint",
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
    "text": "The co-execution opportunity depends on update work being CPU-bound and ready while search waits for storage.",
    "uncertainty": "The source boundary is qualitative."
  },
  "confidence_rationale": {
    "assumptions": [
      "The cited source accurately characterizes the implementations it discusses."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The paper explicitly contrasts graph-ANNS pruning with I/O-bound SPANN updates."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Confidence is high in the source's compute-bound scope and moderate in the proposed symbolic crossover.",
    "uncertainty": "No independent backend trace or isolated negative experiment was performed."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "Correctness and queue policy are held constant."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source states that I/O-bound updates leave limited co-execution opportunity."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Useful update progress does not exceed dispatch and checkpoint cost, or update I/O contends with foreground search without filling idle CPU time.",
    "uncertainty": "The measured crossover depends on device and cache state."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-IOBOUND-UPDATES-EMPTY-STALLS",
  "name": "Iobound Updates Empty Stalls",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "The source characterizes the available co-execution benefit for such update designs as limited.",
    "uncertainty": "No isolated threshold or negative benchmark is reported."
  },
  "repair_options": [
    {
      "description": "Restrict the mechanism to update phases with measured ready CPU work.",
      "repair_class": "SPECIALIZE_WORKLOAD"
    },
    {
      "description": "Decline co-execution when update-side I/O wait or contention exceeds a measured limit.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Schedule a different compute-ready maintenance task during the stall.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Measure phase-level readiness and overhead before enabling the scheduler.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-2605.19335"
  ],
  "source_pointers": [
    {
      "claim_scope": "CPU-intensive update pruning is paired with search-side I/O stalls.",
      "locator_type": "SECTION",
      "locator_value": "Sections 4.1-4.2",
      "page": 4,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "I/O-bound SPANN updates leave limited co-execution opportunity.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 7 update-system boundary",
      "page": 12,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "An I/O-bound update path cannot provide enough ready CPU work to fill search-side I/O stalls.",
    "uncertainty": "This is a scope boundary rather than an impossibility proof."
  }
}
```
