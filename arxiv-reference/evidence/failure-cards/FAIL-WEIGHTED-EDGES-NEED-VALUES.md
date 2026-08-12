# Weighted Edges Need Values

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The fixture semiring makes unequal values observable."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "topology",
      "vertex ordering",
      "chunk layout",
      "semiring",
      "padding marker"
    ],
    "expected_observation": "The value-free result differs from the explicit-value result while unweighted control results match.",
    "fixture_kind": "GRAPH",
    "fixture_name": "Observable unequal weights",
    "graph_scale": "Minimal weighted topology whose output depends on distinguishing edge values.",
    "graph_shape": "Equal topology with adjacencies carrying unequal operative values.",
    "independent_oracle": "CSR or equivalent sparse execution retaining explicit edge values.",
    "premises": [
      "The source scopes value inference to unweighted adjacency matrices."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "The smallest failing topology depends on the selected semiring.",
    "varied_variables": [
      "edge values"
    ],
    "workload": "Execute the same algebraic traversal with explicit values and with topology-derived unit values."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-INFER-UNWEIGHTED-EDGE-VALUES"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "The selected operation observes per-edge values."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "exists e: inferred_value(e) != required_value(e)",
    "measurement_needed": "Compare the value-free kernel against an explicit-value oracle over representative weighted domains.",
    "numeric_constants": [],
    "premises": [
      "The source removes values only because unweighted adjacency values are redundant."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "Alternative exact encodings for restricted weighted domains are not excluded.",
    "variables": [
      {
        "definition": "An adjacency entry",
        "symbol": "e",
        "units": "edge"
      },
      {
        "definition": "Value regenerated from topology and padding marker",
        "symbol": "inferred_value",
        "units": "semiring value"
      },
      {
        "definition": "Value required by the graph operation",
        "symbol": "required_value",
        "units": "semiring value"
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
    "text": "The value-free format assumes every real adjacency has an implicit unit value distinguishable from padding.",
    "uncertainty": "A different restricted generated-value scheme is outside the source."
  },
  "confidence_rationale": {
    "assumptions": [
      "The extracted page accurately maps the mechanism boundary."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source directly states why values can be synthesized and when they cannot be omitted."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Confidence is high because the source's storage reduction and its unweighted restriction are explicit.",
    "uncertainty": "No independent kernel reproduction was performed."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "The oracle and mechanism use identical topology and arithmetic except value storage."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source states that weighted values cannot be omitted under the mechanism."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "At least one output differs from the explicit-value oracle when unequal edge values affect the operation.",
    "uncertainty": "A semiring that ignores weights would not expose this counterexample."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-WEIGHTED-EDGES-NEED-VALUES",
  "name": "Weighted Edges Need Values",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Omitting the value array is not valid for the weighted operation described by the source boundary.",
    "uncertainty": "The source states the scope rather than reporting a wrong-result benchmark."
  },
  "repair_options": [
    {
      "description": "Admit only unweighted operations with a proved generated value.",
      "repair_class": "SPECIALIZE_WORKLOAD"
    },
    {
      "description": "Retain explicit values or use a separately verified weighted-value encoding.",
      "repair_class": "CHANGE_REPRESENTATION"
    },
    {
      "description": "Reject value elision when the selected operation observes edge attributes.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Use an explicit-value sparse kernel for weighted graphs.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-2010.09913"
  ],
  "source_pointers": [
    {
      "claim_scope": "Value-array removal for unweighted graphs and its weighted-edge boundary.",
      "locator_type": "SECTION",
      "locator_value": "Section III-B, Reducing Storage Complexity with SlimSell",
      "page": 6,
      "paper_id": "PAPER-2010.09913",
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
    "text": "Distinct edge weights or other operative per-edge values make the stored value array semantically necessary.",
    "uncertainty": "Some weighted domains may admit another representation not studied here."
  }
}
```
