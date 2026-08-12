# Long walks multiply full edge scans

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The direct simulator does not require scanning the full graph per step."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Graph",
      "Sample count",
      "Stopping rule",
      "Storage layout"
    ],
    "expected_observation": "The scan pipeline exceeds the full-scan budget before all endpoints finish",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "persistent unfinished endpoint walks",
    "graph_scale": "Symbolic path length exceeding the configured scan budget",
    "graph_shape": "A directed path or low-termination region that permits long sampled walks",
    "independent_oracle": "Direct per-walk simulator that records exact endpoints and steps",
    "premises": [
      "The source identifies long unfinished paths as the repeated-scan boundary."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "Occurrence frequency is probabilistic and must be reported rather than assumed.",
    "varied_variables": [
      "Path length",
      "Random seed",
      "Stopping probability"
    ],
    "workload": "Generate independent endpoint fingerprints with reuse and truncation disabled until the budget is crossed"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-STORE-RANDOM-WALK-ENDPOINTS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Each unfinished pass requires another full edge scan in the stated construction."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "path_length > affordable_full_scans",
    "measurement_needed": "Record unfinished paths and full edge scans across seeds and stopping probabilities.",
    "numeric_constants": [],
    "premises": [
      "The source explains shrinking unfinished paths and long-path completion."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "Walk length is random and no universal affordable scan count is reported.",
    "variables": [
      {
        "definition": "Edge-scan passes needed to complete a sampled walk",
        "symbol": "path_length",
        "units": "scan passes"
      },
      {
        "definition": "Maximum full graph scans allowed by the construction budget",
        "symbol": "affordable_full_scans",
        "units": "scan passes"
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
    "text": "Uncontrolled long fingerprint paths can force too many full edge scans unless the construction reuses completed paths or truncates with acknowledged approximation.",
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
      "Instrumentation counts complete scans and unfinished paths exactly."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source ties each construction round to an edge scan."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The full-edge-scan counter exceeds the declared budget while unfinished endpoint paths remain.",
    "uncertainty": "A particular random seed may not produce the long path."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-LONG-WALKS-MULTIPLY-SCANS",
  "name": "Long walks multiply full edge scans",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Construction I/O grows with repeated full edge scans, or truncation/reuse changes the exact independent-walk construction.",
    "uncertainty": "NONE"
  },
  "repair_options": [
    {
      "description": "Expose and enforce a budget for the resource term that triggers the failure.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Choose a schedule that avoids the reported work, contention, or locality reversal.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Use a correctness-preserving fallback when the optimized path's assumptions fail.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-HASH-0232e71ded2b5c43"
  ],
  "source_pointers": [
    {
      "claim_scope": "Long fingerprint paths require repeated edge scans unless completed by reuse or explicit truncation.",
      "locator_type": "SECTION",
      "locator_value": "Section 2.1",
      "page": 10,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
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
    "text": "Construct endpoint samples whose geometric walks remain unfinished across many scan passes.",
    "uncertainty": "NONE"
  }
}
```
