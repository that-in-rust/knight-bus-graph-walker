# Partial Coarse Graph Amplification

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The fixture maximizes duplicate coarse keys without changing the expected coarse graph."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Fine graph",
      "Community assignment",
      "Edge weights"
    ],
    "expected_observation": "Coarse weights match while partial bytes exceed the cap or elapsed time crosses the serial oracle.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "Overlapping coarse contributions",
    "graph_scale": "Symbolic fine-edge count, coarse-pair count, and worker count.",
    "graph_shape": "Fine edges are divided among workers but map repeatedly to the same coarse node pairs.",
    "independent_oracle": "Single-thread map-and-sum keyed by coarse endpoint pair.",
    "premises": [
      "The source builds per-thread partial coarse graphs then merges them."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The smallest overlap level that reverses scaling is unknown.",
    "varied_variables": [
      "Worker count",
      "Cross-worker coarse-pair overlap"
    ],
    "workload": "Build and merge thread-local coarsenings, then compare with exact serial aggregation."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-MERGE-THREAD-LOCAL-COARSENINGS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "All partial graphs coexist at the measured peak and both paths construct identical coarse weights."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "bytes_partial_sum + bytes_merge_scratch > ram_cap OR T_parallel_coarsen >= T_serial_coarsen",
    "measurement_needed": "Measure retained bytes, peak RSS, partial-edge duplication, and merge time versus worker count.",
    "numeric_constants": [],
    "premises": [
      "The source materializes per-thread partial graphs and observes weaker phase scaling."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The source does not report the peak-live-set crossover.",
    "variables": [
      {
        "definition": "sum of simultaneously live thread-local partial coarse graph bytes",
        "symbol": "bytes_partial_sum",
        "units": "bytes"
      },
      {
        "definition": "temporary bytes used to merge partial coarse graphs",
        "symbol": "bytes_merge_scratch",
        "units": "bytes"
      },
      {
        "definition": "admitted peak RAM for coarsening",
        "symbol": "ram_cap",
        "units": "bytes"
      },
      {
        "definition": "elapsed partial-build and merge time",
        "symbol": "T_parallel_coarsen",
        "units": "time"
      },
      {
        "definition": "elapsed exact reference coarsening time",
        "symbol": "T_serial_coarsen",
        "units": "time"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "Peak memory includes all simultaneously live partial graphs and merge scratch."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source creates one partial coarse graph per thread and reports weaker coarsening scaling."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Parallel coarsening assumes thread-local partial graphs and their merge remain smaller and faster than the serial coarse aggregation they replace.",
    "uncertainty": "The source does not provide a complete peak-byte equation."
  },
  "confidence_rationale": {
    "assumptions": [
      "The local implementation uses materially similar per-thread partial graphs."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source states the dataflow and reports phase scaling."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The weak scaling is sourced; the explicit memory-amplification fixture and breakpoint are derived.",
    "uncertainty": "No source peak-RSS decomposition is available."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "Memory and time accounting include merge scratch."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Per-thread materialization introduces a worker-dependent live set."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Peak partial-graph bytes exceed the admitted cap or parallel coarsening loses to exact serial aggregation.",
    "uncertainty": "The boundary requires implementation measurement."
  },
  "failure_basis": "SOURCE_SUPPORTED_DERIVATION",
  "failure_id": "FAIL-PARTIAL-COARSE-GRAPH-AMPLIFICATION",
  "name": "Partial Coarse Graph Amplification",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Coarsening scales less strongly than movement and refinement, with merge and partial-graph work limiting parallel benefit.",
    "uncertainty": "The source does not isolate merge time or peak RSS."
  },
  "repair_options": [
    {
      "description": "Bound aggregate partial-graph bytes before launching workers.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Use partitioned or serial aggregation when the bound cannot be met.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Stream worker contributions into bounded merge runs instead of retaining all partial graphs.",
      "repair_class": "CHANGE_REPRESENTATION"
    }
  ],
  "source_paper_ids": [
    "PAPER-1304.4453"
  ],
  "source_pointers": [
    {
      "claim_scope": "Per-thread partial coarse graphs and merge.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section III.B, Parallel graph coarsening",
      "page": 5,
      "paper_id": "PAPER-1304.4453",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Weaker scaling of the coarsening phase.",
      "locator_type": "FIGURE",
      "locator_value": "Figures 3-4 and Section V.C",
      "page": 8,
      "paper_id": "PAPER-1304.4453",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "The adversarial graph maps each worker edge slice to many of the same coarse pairs."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Thread-local partial graphs duplicate coarse destinations before aggregation."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Many workers each materialize large overlapping coarse adjacency contributions before merge.",
    "uncertainty": "The maximum duplication factor is implementation dependent."
  }
}
```
