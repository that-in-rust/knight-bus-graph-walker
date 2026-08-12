# Interleaving Loses Exclusive Ownership

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The test harness can force the conflicting schedule."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph and weights",
      "partition map",
      "initial distances"
    ],
    "expected_observation": "A violating schedule emits state that differs from the oracle or requires a repair iteration absent under exclusive ownership.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "two-writer-partition-relaxation",
    "graph_scale": "Use one destination partition and the minimum sources needed to create competing updates.",
    "graph_shape": "A small weighted path with two incoming relaxations targeting vertices in the same destination partition.",
    "independent_oracle": "Sequential Bellman-Ford relaxation to a fixed point.",
    "premises": [
      "The source states ownership and visibility prerequisites."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Hardware memory models may require different forcing mechanisms.",
    "varied_variables": [
      "worker interleaving",
      "message visibility delay"
    ],
    "workload": "Schedule two workers to gather and scatter the partition with a controlled visibility delay."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-INTERLEAVE-PARTITION-UPDATES-SAFELY"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "No atomic merge makes multiple writers equivalent to one owner."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "concurrent_partition_writers > exclusive_owner_capacity",
    "measurement_needed": "Instrument ownership and message visibility while enumerating conflicting schedules.",
    "numeric_constants": [],
    "premises": [
      "The source requires exclusive ownership."
    ],
    "source_pointer_ids": [
      "FP-002"
    ],
    "uncertainty": "Visibility failure can trigger the same symptom without multiple writers.",
    "variables": [
      {
        "definition": "Workers concurrently able to mutate or scatter one partition.",
        "symbol": "concurrent_partition_writers",
        "units": "workers"
      },
      {
        "definition": "Concurrent writers permitted by the ownership invariant.",
        "symbol": "exclusive_owner_capacity",
        "units": "workers"
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
    "text": "Only the exclusive owner gathers and scatters a partition, and already-written messages are visible before its scatter.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "The fixture can force their violation."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Exclusive ownership and shared-memory visibility are source-stated."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The counterexample negates prerequisites explicitly frozen in the mechanism card.",
    "uncertainty": "The severity ranges from extra work to incorrect output by algorithm."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "All non-scheduling inputs are deterministic."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The fixture creates competing partition updates."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "A schedule violating ownership or visibility produces a stale/lost update, nondeterministic output, or extra convergence work relative to the oracle.",
    "uncertainty": "Eventual convergence may mask intermediate corruption unless traces are checked."
  },
  "failure_basis": "ANALYTICAL_COUNTEREXAMPLE",
  "failure_id": "FAIL-INTERLEAVING-LOSES-EXCLUSIVE-OWNERSHIP",
  "name": "Interleaving Loses Exclusive Ownership",
  "observable_symptom": {
    "assumptions": [
      "The algorithm does not provide conflict resolution for the violating schedule."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Concurrent mutation or stale visibility breaks the state transition assumed by interleaving."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The emitted frontier contains a lost, duplicated, or stale relaxation and can diverge from a sequential exact oracle.",
    "uncertainty": "Some monotone algorithms may eventually self-correct despite extra iterations."
  },
  "repair_options": [
    {
      "description": "Enforce single-owner gather-before-scatter scheduling for each partition.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Use interleaving only in shared-memory algorithms whose update operator and visibility semantics tolerate asynchronous relaxation.",
      "repair_class": "SPECIALIZE_WORKLOAD"
    }
  ],
  "source_paper_ids": [
    "PAPER-1806.08092"
  ],
  "source_pointers": [
    {
      "claim_scope": "Within-iteration updates are consumed before scattering a partition.",
      "locator_type": "SECTION",
      "locator_value": "Section 3.3, interleaved scatter-gather",
      "page": 9,
      "paper_id": "PAPER-1806.08092",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Safety depends on exclusive partition ownership and shared-memory visibility.",
      "locator_type": "SECTION",
      "locator_value": "Section 3.3 numbered prerequisites",
      "page": 10,
      "paper_id": "PAPER-1806.08092",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "The execution environment violates at least one stated prerequisite."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The mechanism reads refreshed partition state and immediately emits from it."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Two workers mutate or scatter the same partition concurrently, or a remote write remains invisible when the owner gathers.",
    "uncertainty": "The exact race schedule is platform-specific."
  }
}
```
