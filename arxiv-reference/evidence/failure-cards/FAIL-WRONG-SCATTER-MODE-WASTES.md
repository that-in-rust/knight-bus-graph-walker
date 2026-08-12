# Wrong scatter mode wastes work

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Forced execution exposes both valid alternatives."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Partitions",
      "Values",
      "Aggregation semantics",
      "Hardware",
      "Bandwidth measurement method"
    ],
    "expected_observation": "Outputs agree while the wrong mode processes more bytes or takes longer",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "sparse-dense scatter reversal",
    "graph_scale": "Symbolic partitions spanning sparse to dense activity",
    "graph_shape": "A partitioned graph with independently controllable active vertices per partition",
    "independent_oracle": "Element-wise equality of gathered updates plus forced-mode time and byte counters",
    "premises": [
      "The source reports mode-specific sparse and dense costs."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The model crossover is machine-specific.",
    "varied_variables": [
      "Active-edge density",
      "Active-vertex density"
    ],
    "workload": "Execute source-centric and partition-centric scatter for the same active sets"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-SELECT-PARTITION-SCATTER-MODE"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Both modes implement the same scatter semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "predicted_time(chosen_mode) > actual_time(alternative_mode)",
    "measurement_needed": "Force both modes per partition across controlled active-edge densities and record bytes and time.",
    "numeric_constants": [],
    "premises": [
      "The source defines opposite sparse and dense mode costs."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The exact activity crossover depends on bandwidth and message representation.",
    "variables": [
      {
        "definition": "Model-predicted scatter time",
        "symbol": "predicted_time",
        "units": "time"
      },
      {
        "definition": "Scatter mode chosen by the model or forced policy",
        "symbol": "chosen_mode",
        "units": "mode"
      },
      {
        "definition": "Measured scatter time",
        "symbol": "actual_time",
        "units": "time"
      },
      {
        "definition": "Other valid scatter mode",
        "symbol": "alternative_mode",
        "units": "mode"
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
    "text": "Partition-centric mode wastes work on very sparse frontiers, while source-centric mode can lose bandwidth on dense frontiers through bin switching.",
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
      "The fixture isolates scatter from unrelated phases."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports wrong-mode boundaries."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The chosen or forced mode has higher measured scatter time than the alternative at equal output.",
    "uncertainty": "Noise near the crossover requires repeated controlled runs."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-WRONG-SCATTER-MODE-WASTES",
  "name": "Wrong scatter mode wastes work",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Scatter time and processed bytes exceed the alternative mode even though graph results remain equal.",
    "uncertainty": "NONE"
  },
  "repair_options": [
    {
      "description": "Choose a schedule that avoids the reported work, contention, or locality reversal.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Measure the unresolved crossover on the target graph and machine before admission.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-1806.08092"
  ],
  "source_pointers": [
    {
      "claim_scope": "Source-centric active-only work versus partition-centric sequential inactive work.",
      "locator_type": "SECTION",
      "locator_value": "Section 3.1.2",
      "page": 7,
      "paper_id": "PAPER-1806.08092",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Per-iteration comparison of source-centric, partition-centric, and modeled dual-mode selection.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 7",
      "page": 21,
      "paper_id": "PAPER-1806.08092",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Force partition-centric scatter on sparse activity or source-centric scatter on dense activity instead of selecting per partition.",
    "uncertainty": "NONE"
  }
}
```
