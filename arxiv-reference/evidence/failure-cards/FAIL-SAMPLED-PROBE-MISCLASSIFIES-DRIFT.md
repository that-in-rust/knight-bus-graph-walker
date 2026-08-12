# Sampled Probe Misclassifies Drift

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The two populations reproduce compatible and incompatible ranking behavior."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "vector count",
      "dimension",
      "build parameters",
      "query budget"
    ],
    "expected_observation": "The sample passes its heuristic while deployed BQ topology falls below the declared recall or efficiency acceptance criterion.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "compatible-sample-incompatible-deployment",
    "graph_scale": "Use the smallest balanced populations that produce distinct BQ-versus-float rankings.",
    "graph_shape": "Two vector populations: one cosine-compatible and one quantization-incompatible, each with exact float neighbors.",
    "independent_oracle": "Brute-force float ranking and a float-topology index provide exact and structural controls.",
    "premises": [
      "The source reports distribution-dependent compatibility and proposes sampling."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "Synthetic mixtures may not capture production drift.",
    "varied_variables": [
      "deployment mixture",
      "sample mixture"
    ],
    "workload": "Build the probe sample from the compatible population, then evaluate topology on increasingly incompatible deployment mixtures."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-PROBE-QUANTIZED-TOPOLOGY-COMPATIBILITY"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "A measurable divergence statistic can be selected."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "deployment_geometry_divergence > probe_representativeness_margin",
    "measurement_needed": "Measure probe classification and deployed exact recall under controlled mixture shift.",
    "numeric_constants": [],
    "premises": [
      "The source shows distribution, not dimension, controls compatibility."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "uncertainty": "No source-proven margin exists.",
    "variables": [
      {
        "definition": "Difference between sampled and deployed ranking/geometry distributions.",
        "symbol": "deployment_geometry_divergence",
        "units": "distribution divergence"
      },
      {
        "definition": "Maximum divergence under which the probe remains predictive.",
        "symbol": "probe_representativeness_margin",
        "units": "distribution divergence"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "The sampled ranking-overlap statistic represents the geometry of the deployed embedding distribution.",
    "uncertainty": "The source calls the decision a practical heuristic rather than a guarantee."
  },
  "confidence_rationale": {
    "assumptions": [
      "Deployment may differ from that sample."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The probe observes only its sample."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Distribution dependence and heuristic status are sourced; misclassification under nonrepresentative sampling is an analytical consequence.",
    "uncertainty": "Production drift frequency and the best divergence metric are unknown."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "The acceptance criterion is declared before the run."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The sample and deployment mixtures differ."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "Probe admission disagrees with the deployed float-oracle acceptance result after controlled distribution shift.",
    "uncertainty": "The source does not define a universal target recall."
  },
  "failure_basis": "ANALYTICAL_COUNTEREXAMPLE",
  "failure_id": "FAIL-SAMPLED-PROBE-MISCLASSIFIES-DRIFT",
  "name": "Sampled Probe Misclassifies Drift",
  "observable_symptom": {
    "assumptions": [
      "No runtime guard rechecks the mixture."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Probe admission depends on sample overlap.",
      "Compatibility differs between the sampled and deployed subpopulations."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The probe admits BQ-native topology although deployed recall or navigation efficiency collapses relative to float topology.",
    "uncertainty": "Recall impact depends on the unseen distribution."
  },
  "repair_options": [
    {
      "description": "Stratify the probe by modality and distribution segment, and require each admitted segment to pass.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Continuously monitor overlap or exact-recall sentinels and rebuild with float topology after drift.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-2605.02171"
  ],
  "source_pointers": [
    {
      "claim_scope": "Compatibility varies continuously by embedding distribution and can collapse on incompatible geometry.",
      "locator_type": "SECTION",
      "locator_value": "Section 5.6, cross-dataset analysis",
      "page": 8,
      "paper_id": "PAPER-2605.02171",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "A sampled BQ-versus-float ranking-overlap probe is proposed as a heuristic.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Practical compatibility test",
      "page": 11,
      "paper_id": "PAPER-2605.02171",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "The method is specialized and the probe is recommended before deployment.",
      "locator_type": "SECTION",
      "locator_value": "Section 8, Scope and deployment",
      "page": 12,
      "paper_id": "PAPER-2605.02171",
      "pointer_id": "FP-003"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "Sampling or later drift changes mixture weights between probe and deployment."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports strong distribution dependence."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003"
    ],
    "text": "The probe sample contains a compatible subpopulation while deployment traffic shifts toward an incompatible geometric subpopulation.",
    "uncertainty": "The source does not provide a drift-distance bound."
  }
}
```
