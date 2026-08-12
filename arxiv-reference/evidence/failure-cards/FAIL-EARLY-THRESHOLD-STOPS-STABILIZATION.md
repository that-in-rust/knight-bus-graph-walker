# Early Threshold Stops Stabilization

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "A deterministic schedule removes race-induced output differences."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph",
      "initial labels",
      "tie-breaking",
      "worker schedule"
    ],
    "expected_observation": "The nonzero-threshold run terminates with a label or quality difference that the zero-threshold run resolves.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "small-consequential-label-residual",
    "graph_scale": "Use the smallest graph where the residual count is below the configured threshold yet changes the fixed point.",
    "graph_shape": "Two dense label regions joined through a small high-degree residual whose late changes propagate locally.",
    "independent_oracle": "The zero-threshold run under the same deterministic schedule is the independent fixed-point oracle.",
    "premises": [
      "The source explicitly stops early and shows small late residuals."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Constructing the mathematically smallest graph requires search.",
    "varied_variables": [
      "termination threshold"
    ],
    "workload": "Run PLP with identical order and schedule under nonzero and zero update thresholds."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-REACTIVATE-CHANGED-NEIGHBORS-ONLY"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Consequential updates can be fewer than that threshold."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "remaining_consequential_updates <= configured_stop_threshold",
    "measurement_needed": "Run identical schedules to zero updates and compare every intermediate threshold result.",
    "numeric_constants": [],
    "premises": [
      "The source terminates on an updated-count threshold."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "No universal quality-preserving threshold is reported.",
    "variables": [
      {
        "definition": "Pending label changes that can alter the chosen output under the fixed schedule.",
        "symbol": "remaining_consequential_updates",
        "units": "updates"
      },
      {
        "definition": "Updated-count threshold that terminates iteration.",
        "symbol": "configured_stop_threshold",
        "units": "updates"
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
    "text": "Stopping when the updated count falls below a nonzero threshold preserves sufficient output quality for the workload.",
    "uncertainty": "The source reports this as an empirical time-quality trade-off."
  },
  "confidence_rationale": {
    "assumptions": [
      "The fixture fixes asynchronous order."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The stop rule permits unfinished vertices."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Early termination is sourced and residual consequence is analytically possible; magnitude is graph-dependent.",
    "uncertainty": "The paper reports negligible degradation on preliminary cases, not a universal bound."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "The zero-threshold control is allowed to stabilize."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The fixture has a consequential residual."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "At termination, at least one oracle-consequential update remains and final labels differ from the zero-threshold control.",
    "uncertainty": "Quality significance is metric-dependent."
  },
  "failure_basis": "SOURCE_SUPPORTED_DERIVATION",
  "failure_id": "FAIL-EARLY-THRESHOLD-STOPS-STABILIZATION",
  "name": "Early Threshold Stops Stabilization",
  "observable_symptom": {
    "assumptions": [
      "Tie-breaking and schedule are held fixed."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The run terminates with pending consequential updates."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The thresholded result differs from the zero-threshold fixed-point oracle in labels or quality.",
    "uncertainty": "Multiple valid label-propagation fixed points complicate semantic comparison."
  },
  "repair_options": [
    {
      "description": "Treat the nonzero threshold as an approximate mode with an explicit quality contract.",
      "repair_class": "SPECIALIZE_WORKLOAD"
    },
    {
      "description": "Continue to a zero-update fixed point when deterministic exact stabilization is requested.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-1304.4453"
  ],
  "source_pointers": [
    {
      "claim_scope": "Neighbor reactivation is exact locally, but termination uses a nonzero update threshold to trade residual work for time.",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 1 and PLP implementation discussion",
      "page": 4,
      "paper_id": "PAPER-1304.4453",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Late iterations retain a small active and updated residual.",
      "locator_type": "FIGURE",
      "locator_value": "Figures 12-13",
      "page": 15,
      "paper_id": "PAPER-1304.4453",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "A graph can concentrate consequential changes in a small high-degree residual."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source stops before all vertices necessarily stabilize."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "A small residual set remains below the stop threshold but contains vertices whose later label changes alter the final partition.",
    "uncertainty": "The source does not bound quality loss for every graph."
  }
}
```
