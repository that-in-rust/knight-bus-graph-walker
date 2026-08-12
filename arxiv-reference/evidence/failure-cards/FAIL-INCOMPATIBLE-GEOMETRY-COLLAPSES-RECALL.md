# Incompatible Geometry Collapses Recall

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The two corpora expose distinguishable geometry while other parameters remain fixed."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "corpus size",
      "dimension",
      "graph degree",
      "construction beam",
      "search beam",
      "rerank count"
    ],
    "expected_observation": "Record candidate recall before reranking and final recall; incompatible geometry omits exact neighbors before reranking.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "Geometry compatibility contrast",
    "graph_scale": "Small corpora large enough to contain nontrivial local navigation paths and exact neighbors.",
    "graph_shape": "Two fixed vector corpora converted to identically parameterized navigation graphs: one cosine-native and one Euclidean or structureless.",
    "independent_oracle": "Brute-force full-precision similarity ranking.",
    "premises": [
      "The source reports distribution-driven recall differences and explains the local-navigation requirement."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "A small synthetic control may not represent every production distribution shift.",
    "varied_variables": [
      "embedding geometry"
    ],
    "workload": "Build and query both graphs at matched construction degree, beam, candidate count, and rerank count."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-NAVIGATE-BINARY-RERANK-EXACTLY"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "A useful gate would need to predict candidate-set reachability before exact reranking."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "UNKNOWN",
    "measurement_needed": "Measure quantized path preservation, candidate recall before reranking, exact recall after reranking, and beam cost on the target embedding distribution.",
    "numeric_constants": [],
    "premises": [
      "The source reports a geometry-dependent compatibility gradient and severe incompatible cases."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "No universal distribution statistic or numeric cutoff is source-established.",
    "variables": [
      {
        "definition": "Candidate-set recall before exact reranking on the target embedding distribution.",
        "symbol": "R_candidate",
        "units": "dimensionless fraction"
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
    "text": "Binary navigation assumes sign direction and relative magnitude retain enough local neighborhood signal to reach the exact-neighbor region.",
    "uncertainty": "A rigorous real-data navigability guarantee remains open."
  },
  "confidence_rationale": {
    "assumptions": [
      "The reported implementation and exact oracle are correct."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source includes explicit negative datasets and a mechanism-level analysis."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Confidence is high in the source-reported applicability boundary and moderate in any transfer to a new embedding family.",
    "uncertainty": "No independent reproduction, code inspection, or production distribution-shift test occurred."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "The exact oracle uses the same similarity definition as final reranking."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports geometry-driven recall collapse and states reranking only orders retained candidates."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Exact neighbors are absent from the retained candidate set, so full-precision reranking cannot restore them and final recall falls.",
    "uncertainty": "The failure magnitude and useful-beam limit are fixture-specific."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-INCOMPATIBLE-GEOMETRY-COLLAPSES-RECALL",
  "name": "Incompatible Geometry Collapses Recall",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The source reports near-collapsed recall on incompatible distributions even though exact reranking is retained.",
    "uncertainty": "Wider search may recover recall at potentially impractical cost."
  },
  "repair_options": [
    {
      "description": "Require a target-distribution compatibility probe before selecting binary-native topology.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Use a float or alternative quantized navigation metric when compatibility is not demonstrated.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Limit the mechanism to embedding families with measured candidate reachability.",
      "repair_class": "SPECIALIZE_WORKLOAD"
    },
    {
      "description": "Calibrate a target-specific compatibility statistic without claiming a universal cutoff.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-2605.02171"
  ],
  "source_pointers": [
    {
      "claim_scope": "Cross-dataset applicability boundary and low recall on incompatible distributions.",
      "locator_type": "TABLE",
      "locator_value": "Table 11 and Section 5.6",
      "page": 8,
      "paper_id": "PAPER-2605.02171",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Directionality assumption, impossible-triangle discussion, and exact-rerank limitation.",
      "locator_type": "SECTION",
      "locator_value": "Section 6, Analysis",
      "page": 10,
      "paper_id": "PAPER-2605.02171",
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
    "text": "Euclidean-native features or structureless random vectors violate the directionality assumption used by the sign-magnitude metric.",
    "uncertainty": "Compatibility is a continuous gradient for intermediate distributions."
  }
}
```
