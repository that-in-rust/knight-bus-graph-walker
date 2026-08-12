# Thread vectors exceed memory budget

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Both variants use equivalent modularity semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Graph",
      "Partition",
      "Community initialization",
      "Numeric type"
    ],
    "expected_observation": "The vector variant breaches or rejects the cap while the fallback preserves the partition result",
    "fixture_kind": "EXECUTION_PROFILE",
    "fixture_name": "worker-vector budget crossing",
    "graph_scale": "Symbolic vertex count near the per-worker scratch budget",
    "graph_shape": "A fixed community-detection graph",
    "independent_oracle": "Lower-memory PLM-star output plus process-wide peak-memory accounting",
    "premises": [
      "The source supplies a lower-memory variant for this boundary."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "Scheduling and convergence order may require a partition-quality tolerance rather than byte-identical labels.",
    "varied_variables": [
      "Worker count",
      "Vertex count",
      "Memory cap"
    ],
    "workload": "Run local recomputation with increasing worker counts under a hard memory cap"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-RECOMPUTE-NEIGHBOR-WEIGHTS-LOCALLY"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Private vectors are fully allocated for each worker."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "worker_count * vertex_count * scratch_bytes_per_vertex > memory_budget_bytes",
    "measurement_needed": "Measure private-vector allocation and process-wide peak memory while varying workers and vertices.",
    "numeric_constants": [],
    "premises": [
      "The source states memory scales with workers and vertices."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "Implementation metadata and allocator overhead are not isolated.",
    "variables": [
      {
        "definition": "Concurrent workers with private vectors",
        "symbol": "worker_count",
        "units": "workers"
      },
      {
        "definition": "Vertices addressed by each private vector",
        "symbol": "vertex_count",
        "units": "vertices"
      },
      {
        "definition": "Private scratch bytes reserved per vertex",
        "symbol": "scratch_bytes_per_vertex",
        "units": "bytes per vertex"
      },
      {
        "definition": "Allowed memory for the workload",
        "symbol": "memory_budget_bytes",
        "units": "bytes"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The faster local-recomputation variant is unsuitable under tight memory because thread-private vectors scale with both worker count and vertex count.",
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
      "FP-001"
    ],
    "text": "The failure condition is bounded by the cited mechanism premise and negative result; the proposed fixture isolates the same condition without asserting an unsourced numeric threshold.",
    "uncertainty": "The precise crossover remains graph-, implementation-, and hardware-dependent unless the source states otherwise."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "Memory accounting includes every worker allocation."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports the per-thread-vector memory trade-off."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Peak memory crosses the cap for the vector variant while the fallback remains admitted and reaches an equivalent modularity result.",
    "uncertainty": "Exact community labels can differ despite equivalent partitions."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-THREAD-VECTORS-EXCEED-BUDGET",
  "name": "Thread vectors exceed memory budget",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Peak memory exceeds the budget, requiring the lower-memory fallback instead of the faster vector variant.",
    "uncertainty": "NONE"
  },
  "repair_options": [
    {
      "description": "Expose and enforce a budget for the resource term that triggers the failure.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Use a correctness-preserving fallback when the optimized path's assumptions fail.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-1304.4453"
  ],
  "source_pointers": [
    {
      "claim_scope": "Per-thread vectors provide speed at memory proportional to workers and vertices; PLM-star is the lower-memory fallback.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section III.B, PLM Implementation",
      "page": 5,
      "paper_id": "PAPER-1304.4453",
      "pointer_id": "FP-001"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Increase workers or vertices until the aggregate thread-private scratch state exceeds the allowed memory budget.",
    "uncertainty": "NONE"
  }
}
```
