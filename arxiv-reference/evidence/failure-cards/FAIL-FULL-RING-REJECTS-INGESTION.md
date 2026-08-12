# Full ring rejects continued ingestion

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Disabling aging isolates the space boundary."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Ring capacity",
      "Processor count",
      "Edge order",
      "Aging disabled"
    ],
    "expected_observation": "All slots become occupied and the next required retained edge is rejected or cannot make progress",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "insertion-only full-ring stream",
    "graph_scale": "Symbolic stream length sufficient to fill the configured ring",
    "graph_shape": "An undirected stream that continually introduces retained non-tree edges",
    "independent_oracle": "An unbounded reference edge log plus offline connectivity computation",
    "premises": [
      "The source states normal mode has finite storage and aging handles exhaustion."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The fixture does not model a production aging policy.",
    "varied_variables": [
      "Number of retained insertions",
      "Input rate"
    ],
    "workload": "Ingest edges with aging disabled and issue connectivity checks against a reference union-find"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-PACK-CONNECTIVITY-STATE-PREFIX"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "No aging operation frees a slot during the fixture."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "occupied_edge_slots >= total_edge_slots",
    "measurement_needed": "Count occupied slots and record whether the next connectivity-preserving insertion is admitted.",
    "numeric_constants": [],
    "premises": [
      "The source defines finite capacity and normal-mode retention."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "The source's aging schedule, not a universal stream duration, determines time to full.",
    "variables": [
      {
        "definition": "Ring slots occupied by retained edges",
        "symbol": "occupied_edge_slots",
        "units": "edge slots"
      },
      {
        "definition": "Total finite edge capacity across processors",
        "symbol": "total_edge_slots",
        "units": "edge slots"
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
    "text": "Normal ingestion cannot retain another edge when all processors' edge slots are full; timely aging is required to restore space.",
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
      "The test observes all processors and distinguishes backpressure from data loss."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source identifies full capacity as a normal-ingestion failure."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The admission result or progress counter shows no capacity for the next retained edge while the reference log accepts it.",
    "uncertainty": "The time to failure depends on retained-edge density."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-FULL-RING-REJECTS-INGESTION",
  "name": "Full ring rejects continued ingestion",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The next retained edge cannot be admitted in normal mode, or ingestion throughput collapses before the stream rate is sustained.",
    "uncertainty": "NONE"
  },
  "repair_options": [
    {
      "description": "Reject or reroute workloads once the symbolic failure predicate is observed.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Expose and enforce a budget for the resource term that triggers the failure.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Choose a schedule that avoids the reported work, contention, or locality reversal.",
      "repair_class": "CHANGE_SCHEDULE"
    }
  ],
  "source_paper_ids": [
    "PAPER-2112.00098"
  ],
  "source_pointers": [
    {
      "claim_scope": "Normal-mode insertion failure when finite edge storage is full.",
      "locator_type": "SECTION",
      "locator_value": "Section 6, jeopardy condition and full-capacity failure path",
      "page": 12,
      "paper_id": "PAPER-2112.00098",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Data-dependent throughput and builder-transition costs.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 9 and Section 9.3",
      "page": 19,
      "paper_id": "PAPER-2112.00098",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Submit insertion-only connectivity updates without aging until every ring edge slot is occupied.",
    "uncertainty": "NONE"
  }
}
```
