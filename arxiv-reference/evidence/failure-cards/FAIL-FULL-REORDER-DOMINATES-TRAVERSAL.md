# Full reorder dominates traversal savings

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The original BFS and inverse-permuted RCM output are semantically equivalent."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Graph",
      "BFS source",
      "Thread count",
      "Output",
      "Cache state"
    ],
    "expected_observation": "Per-run RCM BFS is faster but total preparation plus traversals exceeds original BFS total",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "single-use RCM amortization failure",
    "graph_scale": "Symbolic graph large enough for measurable preparation",
    "graph_shape": "A static graph accepted by both original and RCM CSR pipelines",
    "independent_oracle": "Original-ID BFS distance vector after inverse permutation",
    "premises": [
      "The source reports this preparation boundary."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The exact amortization count is not transferred from the source hardware.",
    "varied_variables": [
      "Run count",
      "Partial versus full reorder"
    ],
    "workload": "Compare end-to-end original BFS with prepare-once then a small number of RCM BFS runs"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-RELABEL-VERTICES-FOR-LOCALITY"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Runs reuse the same reordered static graph."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "preparation_time > traversal_savings_per_run * run_count",
    "measurement_needed": "Measure full preparation and controlled baseline/reordered traversal times over increasing reuse counts.",
    "numeric_constants": [],
    "premises": [
      "The source separately reports preparation and traversal time."
    ],
    "source_pointer_ids": [
      "FP-002"
    ],
    "uncertainty": "The break-even run count is graph- and machine-dependent.",
    "variables": [
      {
        "definition": "One-time full relabeling and CSR rebuild time",
        "symbol": "preparation_time",
        "units": "time"
      },
      {
        "definition": "Baseline traversal time minus reordered traversal time",
        "symbol": "traversal_savings_per_run",
        "units": "time per run"
      },
      {
        "definition": "Traversals amortizing the preparation",
        "symbol": "run_count",
        "units": "runs"
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
    "text": "Full RCM preprocessing can cost more than the traversal it accelerates and also changes the load distribution that the scheduler must handle.",
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
      "Timing includes all required permutation and rebuild work."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports RCM preparation can dominate BFS."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Distance outputs agree, but end-to-end reordered elapsed time exceeds baseline for the tested reuse count.",
    "uncertainty": "Warm-cache policy can alter traversal savings."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-FULL-REORDER-DOMINATES-TRAVERSAL",
  "name": "Full reorder dominates traversal savings",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "End-to-end elapsed time is worse than unreordered BFS even when each reordered traversal is faster.",
    "uncertainty": "NONE"
  },
  "repair_options": [
    {
      "description": "Reject or reroute workloads once the symbolic failure predicate is observed.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Limit use of the mechanism to workloads satisfying its sourced applicability conditions.",
      "repair_class": "SPECIALIZE_WORKLOAD"
    },
    {
      "description": "Measure the unresolved crossover on the target graph and machine before admission.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-2012.10026"
  ],
  "source_pointers": [
    {
      "claim_scope": "RCM improves locality but induces load imbalance requiring scheduling changes.",
      "locator_type": "SECTION",
      "locator_value": "Section 1",
      "page": 1,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Full RCM preparation cost can be much greater than one BFS traversal and partial RCM is proposed.",
      "locator_type": "SECTION",
      "locator_value": "Section 4.6 and Table 4",
      "page": 10,
      "paper_id": "PAPER-2012.10026",
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
    "text": "Pay full graph reordering for only one or a few BFS traversals.",
    "uncertainty": "NONE"
  }
}
```
