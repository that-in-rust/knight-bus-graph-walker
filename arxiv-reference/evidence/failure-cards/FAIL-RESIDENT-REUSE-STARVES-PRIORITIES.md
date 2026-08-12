# Resident Reuse Starves Priorities

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The priority function gives the delayed block strict precedence."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph",
      "block layout",
      "priority function",
      "buffer capacity",
      "thread count"
    ],
    "expected_observation": "Resident-first execution performs extra updates or delays the better frontier enough to exceed the avoided-read benefit",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "resident suboptimal path",
    "graph_scale": "Small block-partitioned graph that deterministically reactivates one block",
    "graph_shape": "Graph with a long reactivating resident path and a disk-resident block holding a globally better frontier",
    "independent_oracle": "Strict global-priority scheduler with exact algorithm output",
    "premises": [
      "The source identifies exactly this scheduling concern."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Performance conversion between I/O and compute must be measured.",
    "varied_variables": [
      "reuse cap",
      "resident path length"
    ],
    "workload": "Run resident-first scheduling with and without a reuse cap"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-PRIORITIZE-RESIDENT-ACTIVE-BLOCKS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Costs are converted with a declared measurement model."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "resident_reuse_work + priority_delay_cost > avoided_read_cost",
    "measurement_needed": "Trace reuse streaks, delayed block priorities, redundant updates, and avoided reads.",
    "numeric_constants": [],
    "premises": [
      "Resident blocks are selected ahead of disk-resident blocks.",
      "The source identifies redundant work and priority disruption."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "No universal forced-eviction threshold is reported.",
    "variables": [
      {
        "definition": "work from consecutive processing of resident blocks",
        "symbol": "resident_reuse_work",
        "units": "operations"
      },
      {
        "definition": "cost from delaying globally preferred blocks",
        "symbol": "priority_delay_cost",
        "units": "operations"
      },
      {
        "definition": "work-equivalent benefit of avoided block reads",
        "symbol": "avoided_read_cost",
        "units": "operations"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "Priority order materially affects convergence work."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The cached queue dominates retrieval.",
      "The source warns that consecutive reuse can disrupt priority order."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Resident-first reuse assumes locality savings exceed the redundant work and priority delay caused by repeatedly reactivating one loaded block.",
    "uncertainty": "The source disables its mitigation by default because evaluated cases did not show a significant effect."
  },
  "confidence_rationale": {
    "assumptions": [
      "The derived fixture creates the source-described priority inversion."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The warning and early-stop rule were rechecked."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The paper explicitly identifies the failure mechanism and an optional mitigation but reports no significant effect in its evaluated workloads.",
    "uncertainty": "Its target-system prevalence is unknown."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "The selected algorithm is correct under both schedules."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Resident reuse can change execution order without changing the fixed point."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Redundant updates or completion latency exceed the strict-priority baseline while correctness remains unchanged.",
    "uncertainty": "Some algorithms may not exhibit priority-sensitive work inflation."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-RESIDENT-REUSE-STARVES-PRIORITIES",
  "name": "Resident Reuse Starves Priorities",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "Redundant updates increase and global priority order is disrupted until forced eviction or natural deactivation.",
    "uncertainty": "The frequency and severity are unreported."
  },
  "repair_options": [
    {
      "description": "Cap consecutive resident reuse and return blocks to global priority order.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Enable resident-first reuse only for algorithms tolerant of relaxed priority.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Calibrate reuse streaks against redundant work and avoided reads.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-2511.07886"
  ],
  "source_pointers": [
    {
      "claim_scope": "Resident-first dual-queue scheduling",
      "locator_type": "SECTION",
      "locator_value": "Section 4.2, Worklist and Buffer Pool",
      "page": 10,
      "paper_id": "PAPER-2511.07886",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Priority disruption, redundant work, and forced-eviction mitigation",
      "locator_type": "SECTION",
      "locator_value": "Section 4.5, Early-stop",
      "page": 12,
      "paper_id": "PAPER-2511.07886",
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
    "text": "Repeated activation of a resident block can form a suboptimal path while higher-priority disk blocks wait.",
    "uncertainty": "The source presents this as a concern and mitigation boundary, not a measured regression in its experiments."
  }
}
```
