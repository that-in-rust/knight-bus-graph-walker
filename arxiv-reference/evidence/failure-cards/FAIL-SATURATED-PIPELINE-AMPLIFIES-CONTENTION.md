# Saturated Pipeline Amplifies Contention

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The fixture can hold candidate ordering and recall constant across schedules."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph index",
      "query set",
      "recall target",
      "beam policy",
      "storage device",
      "cache state"
    ],
    "expected_observation": "At saturation, replenishment increases speculative I/O or latency without improving matched-recall throughput.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "Saturated completion pipeline",
    "graph_scale": "Minimal frontier with staggered completions and continuously eligible replacements.",
    "graph_shape": "A disk ANN graph with a frontier containing both useful and read-but-unexplored candidates.",
    "independent_oracle": "Timestamped batch-barrier request/completion trace with identical result checks.",
    "premises": [
      "The source reports this concurrency reversal and device-exclusive recommendation."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "Storage firmware and scheduler behavior may move the crossover.",
    "varied_variables": [
      "query concurrency",
      "queue depth",
      "worker count"
    ],
    "workload": "Compare batch-barrier and per-completion replenishment below and at device saturation with matched recall."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-REPLENISH-IO-EACH-COMPLETION"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Matched recall and candidate semantics are maintained."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "I_speculative + I_contention > I_overlap_saved",
    "measurement_needed": "Measure read-but-unexplored requests, queue occupancy, device utilization, completion skew, and matched-recall latency.",
    "numeric_constants": [],
    "premises": [
      "The source reports counterproductive speculative reads under saturated concurrency."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "The source does not isolate each term or give a portable saturation coefficient.",
    "variables": [
      {
        "definition": "Cost of read-but-unexplored replacement requests",
        "symbol": "I_speculative",
        "units": "time-equivalent I/O cost"
      },
      {
        "definition": "Additional delay from device and queue contention",
        "symbol": "I_contention",
        "units": "time"
      },
      {
        "definition": "Idle or batch-delay time removed by replenishment",
        "symbol": "I_overlap_saved",
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
      "FP-002",
      "FP-003"
    ],
    "text": "Immediate replenishment helps only when the device has useful unsaturated capacity and replacement requests are sufficiently likely to be explored.",
    "uncertainty": "No device-independent threshold is supplied."
  },
  "confidence_rationale": {
    "assumptions": [
      "The complete Pipeline's behavior is relevant to the replenishment submechanism."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source describes immediate replenishment and reports Pipeline's speculative-I/O harm."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Confidence is high in the source's qualitative concurrency reversal and moderate in attributing it specifically to replenishment.",
    "uncertainty": "The source does not provide a replenishment-only ablation, and no independent rerun occurred."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "The experiment isolates schedule while preserving search semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports Pipeline as counterproductive under high concurrency."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "At matched recall, per-completion replenishment performs more read-but-unexplored I/O and has worse latency or throughput than the batch-barrier oracle once storage is saturated.",
    "uncertainty": "The isolated replenishment share of the full Pipeline effect is unknown."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-SATURATED-PIPELINE-AMPLIFIES-CONTENTION",
  "name": "Saturated Pipeline Amplifies Contention",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002",
      "FP-003"
    ],
    "text": "Pipeline issues more speculative reads and can reduce overall performance relative to the baseline under high concurrency.",
    "uncertainty": "The source evaluates the complete Pipeline rather than replenishment in isolation."
  },
  "repair_options": [
    {
      "description": "Disable immediate replenishment when measured device or queue saturation is already high.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Restrict continuous replenishment to device-exclusive or otherwise demonstrated regimes.",
      "repair_class": "SPECIALIZE_WORKLOAD"
    },
    {
      "description": "Cap speculative width or combine replenishment with a candidate-usefulness gate.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Calibrate queue-depth and concurrency crossover on each target device.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "Immediate request replenishment and continuous I/O pipeline.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 9(b) and Section 4.3.2",
      "page": 6,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Speculative reads add I/O and make Pipeline counterproductive.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Finding 5 and Pipeline discussion",
      "page": 9,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Concurrency boundary and device-exclusive recommendation.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Finding 9",
      "page": 11,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-003"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002",
      "FP-003"
    ],
    "text": "Concurrent queries already saturate the SSD while early replacement requests widen the frontier before candidate order is final.",
    "uncertainty": "The exact saturation point depends on hardware and workload."
  }
}
```
