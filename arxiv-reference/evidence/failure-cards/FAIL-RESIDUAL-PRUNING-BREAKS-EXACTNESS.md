# Residual Pruning Breaks Exactness

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The minimal graph assigns nonzero, noncancelling contribution to the omitted branch."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph",
      "seed",
      "decay",
      "total depth",
      "stage depths",
      "numeric arithmetic"
    ],
    "expected_observation": "Full staged execution matches the oracle while the omitted-residual run differs at a downstream vertex.",
    "fixture_kind": "GRAPH",
    "fixture_name": "Omitted residual branch",
    "graph_scale": "Minimal topology with independently observable residual continuations.",
    "graph_shape": "A seed reaches distinct downstream regions through separate residual-bearing intermediate vertices.",
    "independent_oracle": "Direct single-stage diffusion using the same arithmetic and total depth.",
    "premises": [
      "The source gives the linear decomposition identity and describes residual selection as approximate."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Different arithmetic order can create small differences even when all residuals are retained.",
    "varied_variables": [
      "retained residual support"
    ],
    "workload": "Run direct diffusion, full staged diffusion, and staged diffusion with one required residual continuation omitted."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-DECOMPOSE-DIFFUSION-INTO-STAGES"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "The diffusion operator and arithmetic are otherwise identical."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "S_processed != support(r)",
    "measurement_needed": "Compare direct diffusion and staged execution while enumerating retained residual support.",
    "numeric_constants": [],
    "premises": [
      "The source's exact linear identity sums all nonzero residual components."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "Some omitted contributions may cancel or fall below an application tolerance, but exact equality is not guaranteed.",
    "variables": [
      {
        "definition": "Residual seeds whose later-stage contributions are computed",
        "symbol": "S_processed",
        "units": "set of vertices"
      },
      {
        "definition": "First-stage residual vector",
        "symbol": "r",
        "units": "score vector"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "A required omitted residual has a nonzero, noncancelling downstream contribution."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source's exact linear decomposition sums all nonzero residual continuations."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Exact staged diffusion requires combining every nonzero residual-seeded continuation under the linear decomposition identity.",
    "uncertainty": "Finite arithmetic may introduce additional differences."
  },
  "confidence_rationale": {
    "assumptions": [
      "The paper's algebra applies to the selected diffusion operator."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source equations define the preserved quantity."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Confidence is high because the source states both the exact linear identity and the precision trade-off from selecting residuals.",
    "uncertainty": "No independent implementation reproduction or floating-point sensitivity study was performed."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "The omitted residual affects an independently observable downstream score."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Exact source decomposition includes every nonzero residual component."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "At least one output score differs from direct diffusion when a nonzero, noncancelling residual continuation is omitted.",
    "uncertainty": "The difference magnitude is not predicted without the fixture's weights and decay."
  },
  "failure_basis": "SOURCE_SUPPORTED_DERIVATION",
  "failure_id": "FAIL-RESIDUAL-PRUNING-BREAKS-EXACTNESS",
  "name": "Residual Pruning Breaks Exactness",
  "observable_symptom": {
    "assumptions": [
      "The omitted residual contributes nonzero score under otherwise identical arithmetic."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source gives the exact decomposition identity and evaluates approximate residual selection."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The staged result loses precision relative to full diffusion when required residual components are not processed.",
    "uncertainty": "The source evaluates an approximate selection policy rather than every possible omission."
  },
  "repair_options": [
    {
      "description": "Label residual selection as approximate and require an explicit error contract.",
      "repair_class": "SPECIALIZE_WORKLOAD"
    },
    {
      "description": "Require complete residual support when exact results are requested.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Use direct or full staged diffusion when residual completeness cannot be maintained.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Bound residual count, overlap, and serial/concurrent subgraph state before exact admission.",
      "repair_class": "ADD_RESOURCE_BOUND"
    }
  ],
  "source_paper_ids": [
    "PAPER-2104.09616"
  ],
  "source_pointers": [
    {
      "claim_scope": "Linearity identity, residual decomposition, and precision loss when residual seeds are omitted.",
      "locator_type": "EQUATION",
      "locator_value": "Equations 6-8 and Sections IV-B-IV-C",
      "page": 3,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Evaluated precision-latency boundary for selected residual processing.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 7 and Section VII",
      "page": 6,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "At least one selected-out residual is nonzero and noncancelling."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source identity includes every nonzero residual continuation for exact equality."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Selecting only a subset of residual seeds omits diffusion contributions and trades precision for lower latency.",
    "uncertainty": "The size of the error is workload-specific."
  }
}
```
