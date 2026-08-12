# Synchronous Barriers Defeat Pipeline

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Instrumentation does not alter scheduling order."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph",
      "algorithm",
      "block layout",
      "thread count",
      "I/O depth"
    ],
    "expected_observation": "I/O remains busy or completed while workers accumulate barrier idle time and total overlap fails to offset stalls",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "barrier dominated iterations",
    "graph_scale": "Small graph sufficient to create repeatable phase imbalance",
    "graph_shape": "Partitioned graph with skewed per-partition work under a globally synchronous algorithm",
    "independent_oracle": "Sequentially consistent synchronous result plus per-worker timeline",
    "premises": [
      "The source preserves barriers for correctness and reports remaining stalls."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The fixture is derived and not the source benchmark.",
    "varied_variables": [
      "partition skew",
      "barrier frequency"
    ],
    "workload": "Run synchronous iterations with asynchronous I/O enabled"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-PIPELINE-ASYNC-IO-COMPUTE"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Correctness requires the declared global barrier."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "barrier_idle_time + serial_phase_time > overlap_savings",
    "measurement_needed": "Measure worker idle time, barrier time, I/O wait, and overlapped compute per iteration.",
    "numeric_constants": [],
    "premises": [
      "Barriers stop progress even when I/O submission is nonblocking."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The source does not isolate all equation terms.",
    "variables": [
      {
        "definition": "worker time lost waiting at global barriers",
        "symbol": "barrier_idle_time",
        "units": "time"
      },
      {
        "definition": "time in nonoverlappable iteration work",
        "symbol": "serial_phase_time",
        "units": "time"
      },
      {
        "definition": "time hidden by asynchronous I/O overlap",
        "symbol": "overlap_savings",
        "units": "time"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "The algorithm cannot relax its barrier semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The system exposes a synchronous mode for correctness.",
      "The evaluation states that mode remains stall-bound."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Asynchronous I/O overlap removes blocking only where the algorithm permits useful work between completions; global barriers retain synchronization stalls.",
    "uncertainty": "The magnitude depends on imbalance and storage latency."
  },
  "confidence_rationale": {
    "assumptions": [
      "The synchronous interface is required for the selected workload."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Both source pages were rechecked."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The source explicitly states the semantic boundary and observed residual stalls; the smallest target crossover is unreported.",
    "uncertainty": "No independent trace was collected."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "All compared runs enforce identical synchronization semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The pipeline cannot execute next-iteration work before the barrier."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Barrier and idle counters dominate saved I/O wait while output remains correct.",
    "uncertainty": "Instrumentation overhead must be measured."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-SYNCHRONOUS-BARRIERS-DEFEAT-PIPELINE",
  "name": "Synchronous Barriers Defeat Pipeline",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "The asynchronous I/O pipeline improves loading throughput but does not remove per-iteration synchronization stalls.",
    "uncertainty": "Runtime impact varies with phase imbalance."
  },
  "repair_options": [
    {
      "description": "Use unrestricted asynchronous execution only for algorithms with compatible convergence semantics.",
      "repair_class": "SPECIALIZE_WORKLOAD"
    },
    {
      "description": "Balance synchronous phase work while retaining barriers.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Expose a separate synchronous plan with honest stall accounting.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-2511.07886"
  ],
  "source_pointers": [
    {
      "claim_scope": "Algorithms requiring global barriers and explicit synchronous fallback",
      "locator_type": "SECTION",
      "locator_value": "Section 4.3, Synchronous Execution",
      "page": 11,
      "paper_id": "PAPER-2511.07886",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Synchronization stalls remaining under asynchronous I/O",
      "locator_type": "SECTION",
      "locator_value": "Section 6.4, Synchronous Execution",
      "page": 21,
      "paper_id": "PAPER-2511.07886",
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
    "text": "A graph algorithm requiring global synchronization must use the synchronous interface rather than the unrestricted asynchronous schedule.",
    "uncertainty": "The source gives MIS as its case study rather than an exhaustive algorithm taxonomy."
  }
}
```
