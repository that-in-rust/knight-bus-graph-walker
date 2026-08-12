# Dense Column Exceeds Memory

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The implementation exposes a measurable admission budget and does not silently page resident state."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "sparse encoding",
      "matrix dimensions",
      "dense element width",
      "thread count",
      "buffer policy",
      "storage device"
    ],
    "expected_observation": "Admission refuses below the measured resident minimum; any accepted run must remain within its declared peak and match the result oracle.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "Oversized dense column",
    "graph_scale": "Symbolic matrix dimensions chosen around the measured resident-state budget rather than an invented threshold.",
    "graph_shape": "A sparse matrix with a dense operand whose narrowest complete-column partition exceeds admitted RAM after buffers.",
    "independent_oracle": "An in-memory multiplication result for correctness and independent allocator/RSS plus device-byte counters for resources.",
    "premises": [
      "The source explicitly requires one complete dense column and buffers in memory."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Operating-system caching and allocator behavior can blur the practical RSS boundary.",
    "varied_variables": [
      "available RAM"
    ],
    "workload": "Attempt admission and execution while varying available RAM across the measured complete-column-plus-buffer requirement."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-STREAM-SPARSE-KEEP-DENSE"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "The target implementation requires the same complete-column and buffer invariant."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "M_available < M_dense_column + M_thread_buffers",
    "measurement_needed": "Measure all resident dense state, thread buffers, I/O metadata, allocator overhead, and peak RSS before admission.",
    "numeric_constants": [],
    "premises": [
      "The source gives the minimum resident-state expression for semi-external multiplication."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "Allocator, queue, alignment, and runtime metadata raise the practical minimum beyond the source terms.",
    "variables": [
      {
        "definition": "RAM admitted to the multiplication",
        "symbol": "M_available",
        "units": "bytes"
      },
      {
        "definition": "Bytes for one complete dense input column",
        "symbol": "M_dense_column",
        "units": "bytes"
      },
      {
        "definition": "Aggregate required sparse-input and output buffers",
        "symbol": "M_thread_buffers",
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
    "text": "Semi-external execution requires one complete dense input column and required per-thread buffers to reside in RAM.",
    "uncertainty": "Runtime and allocator overhead are outside the source minimum."
  },
  "confidence_rationale": {
    "assumptions": [
      "The target implementation uses the same dense-column partition model."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source provides an explicit resident-state expression and measured narrow-partition degradation."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Confidence is high in the source minimum-memory premise and moderate in its translation to whole-process admission.",
    "uncertainty": "No independent reproduction or whole-process allocator calibration occurred."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "The target mechanism preserves the source's complete-column invariant."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source minimum excludes execution below that resident-state requirement."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The schedule cannot allocate its required complete dense column and buffers within admitted RAM, or an unguarded run exceeds the declared bound.",
    "uncertainty": "A fully external fallback is a different mechanism and is not tested by this card."
  },
  "failure_basis": "SOURCE_SUPPORTED_DERIVATION",
  "failure_id": "FAIL-DENSE-COLUMN-EXCEEDS-MEMORY",
  "name": "Dense Column Exceeds Memory",
  "observable_symptom": {
    "assumptions": [
      "The target mechanism preserves the complete-column invariant and has no hidden fully external fallback."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source gives a minimum complete-column resident-state expression and reports narrow-partition overhead."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The mechanism cannot form its required resident dense partition; when only narrow valid partitions fit, repeated sparse passes and locality loss increase cost.",
    "uncertainty": "The hard failure and performance degradation are distinct boundaries."
  },
  "repair_options": [
    {
      "description": "Refuse the semi-external plan when measured complete-column and buffer state does not fit.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Include runtime, allocator, queue, and alignment headroom above the analytical minimum.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Select a separately verified fully external plan when the complete-column invariant fails.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Reduce thread-buffer multiplicity or serialize work if that preserves correctness and fits the budget.",
      "repair_class": "CHANGE_SCHEDULE"
    }
  ],
  "source_paper_ids": [
    "PAPER-1602.02864"
  ],
  "source_pointers": [
    {
      "claim_scope": "Minimum RAM for one dense column and thread buffers plus repeated-pass I/O model.",
      "locator_type": "EQUATION",
      "locator_value": "Section 3.6 minimum-memory expression",
      "page": 6,
      "paper_id": "PAPER-1602.02864",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Locality loss and overhead from narrow vertical dense partitions.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 11 and Section 5.3",
      "page": 10,
      "paper_id": "PAPER-1602.02864",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "The target schedule requires the same complete dense column and buffers."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source states the minimum resident state for its semi-external schedule."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "If RAM cannot hold that minimum resident state, the paper's semi-external schedule is not applicable.",
    "uncertainty": "A different fully external algorithm may still exist."
  }
}
```
