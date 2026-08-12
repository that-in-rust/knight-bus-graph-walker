# Late Settlement Erases Pruning

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The constructed order can suppress that reduction while preserving correctness."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph",
      "edge order",
      "memory capacity",
      "neighbor order",
      "BFS root"
    ],
    "expected_observation": "Record equal final forests while pruning overhead equals or exceeds saved scan and reducer work.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "Late-settling BFS stream",
    "graph_scale": "Minimal graph and edge order that delays settled-prefix advancement without changing the BFS oracle.",
    "graph_shape": "A directed traversal with candidate parent alternatives ordered to remain unresolved until late passes.",
    "independent_oracle": "Deterministic in-memory BFS plus an otherwise identical semi-external run with pruning disabled.",
    "premises": [
      "The source's advantage depends on early state and edge reduction."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Small graphs may not reproduce cache and disk-stream costs.",
    "varied_variables": [
      "settlement timing",
      "fraction of edges surviving reduction"
    ],
    "workload": "Run pruned and unpruned semi-external BFS with identical edge order and memory capacity."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-PRUNE-SETTLED-SEARCH-STATE"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "All correctness behavior remains equivalent to the unpruned oracle."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "C_saved_scan <= C_prune + C_rewrite",
    "measurement_needed": "Measure per-iteration settled vertices, removed edges, scan bytes, reducer calls, rewrite bytes, and reconstruction time.",
    "numeric_constants": [],
    "premises": [
      "The sourced mechanism trades pruning work for reduced later scans."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "No source coefficient predicts pruning yield or rewrite cost.",
    "variables": [
      {
        "definition": "Cost avoided through reduced scans and reducer calls",
        "symbol": "C_saved_scan",
        "units": "time"
      },
      {
        "definition": "Threshold and resident-state pruning cost",
        "symbol": "C_prune",
        "units": "time"
      },
      {
        "definition": "Reduced-stream materialization and reconstruction cost",
        "symbol": "C_rewrite",
        "units": "time"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "Those savings are negligible when little state is prunable before termination."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source explains benefit through fewer resident tree edges, reducer calls, and scanned edges."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Pruning must remove enough settled state and scan work early enough to repay threshold, rewrite, and reconstruction costs.",
    "uncertainty": "The source does not isolate a no-pruning crossover."
  },
  "confidence_rationale": {
    "assumptions": [
      "Positive pruning and rewrite overhead remains when reductions are negligible."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The paper explains efficiency through reduced resident and scanned state."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The no-savings case follows from the sourced cost mechanism, but its prevalence and crossover are unknown.",
    "uncertainty": "No isolated ablation or independent reproduction was available."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "The fixture yields little early removable state."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source mechanism adds pruning and reconstruction to reduce later work."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The final BFS forest remains correct, but scan/reducer savings do not exceed pruning and rewrite overhead.",
    "uncertainty": "This is an analytical performance counterexample, not a source benchmark."
  },
  "failure_basis": "ANALYTICAL_COUNTEREXAMPLE",
  "failure_id": "FAIL-LATE-SETTLEMENT-ERASES-PRUNING",
  "name": "Late Settlement Erases Pruning",
  "observable_symptom": {
    "assumptions": [
      "Resource counters separate saved work from pruning overhead."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source attributes efficiency to those reductions."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Reduced scan bytes and reducer calls do not repay pruning bookkeeping and edge-stream rewrite cost.",
    "uncertainty": "The exact runtime crossover depends on storage and implementation."
  },
  "repair_options": [
    {
      "description": "Materialize reduced streams only after measured pruning yield exceeds an admitted threshold.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Continue scanning the original stream when reduction is not paying back.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Delay or batch pruning until enough settled state accumulates.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Measure pruning yield and rewrite cost online because no pre-run estimator is sourced.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-2507.12925"
  ],
  "source_pointers": [
    {
      "claim_scope": "Pruning reduces resident tree state and scanned/reduced work when vertices settle.",
      "locator_type": "SECTION",
      "locator_value": "Section 5, Overview",
      "page": 9,
      "paper_id": "PAPER-2507.12925",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Threshold advancement, reduced edge stream, and reconstruction work.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 5.1, vPrune and ErPrune",
      "page": 12,
      "paper_id": "PAPER-2507.12925",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "The constructed order preserves candidate alternatives until late iterations."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source pruning rules depend on settled breadth-first thresholds."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "A graph and edge order delay parent finality so the settled prefix advances late and the reduced edge stream remains near the original.",
    "uncertainty": "This workload is not source-measured."
  }
}
```
