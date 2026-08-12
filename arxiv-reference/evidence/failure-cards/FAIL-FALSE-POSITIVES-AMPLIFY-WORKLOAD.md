# False Positives Amplify Workload

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Controlled injection preserves the no-false-negative invariant."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph topology",
      "query vectors",
      "exact valid set",
      "search budget"
    ],
    "expected_observation": "At sufficient false-positive work the speculative run performs no less total measured work than the exact-filter control.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "false-positive-selectivity-sweep",
    "graph_scale": "Hold graph size fixed and vary approximate false-positive rate over the same query set.",
    "graph_shape": "A fixed navigable graph with exact attributes arranged so invalid candidates remain near the search path.",
    "independent_oracle": "Exhaustive exact predicate evaluation identifies every valid candidate and supports exact resource counters.",
    "premises": [
      "The source reports false-positive exploration and incomplete cost modeling."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The practical crossover is device- and cache-dependent.",
    "varied_variables": [
      "approximate false-positive decisions",
      "attribute co-location"
    ],
    "workload": "Compare speculative filtering with exact in-filtering under cold and warm page-cache controls."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-EXPLORE-SUPERSET-VERIFY-RESULTS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Costs are measured on the same query and storage state."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "false_positive_traversal_cost + deferred_verification_cost >= avoided_exact_filter_cost",
    "measurement_needed": "Measure page reads, distance evaluations, and exact checks for speculative and exact-filter controls.",
    "numeric_constants": [],
    "premises": [
      "The mechanism exchanges exact filter reads for speculative work."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "No universal conversion between I/O and compute work is source-provided.",
    "variables": [
      {
        "definition": "Traversal and record-read cost caused by invalid admitted candidates.",
        "symbol": "false_positive_traversal_cost",
        "units": "work units"
      },
      {
        "definition": "Exact checking cost paid after candidate selection.",
        "symbol": "deferred_verification_cost",
        "units": "work units"
      },
      {
        "definition": "Exact attribute-access cost avoided during exploration.",
        "symbol": "avoided_exact_filter_cost",
        "units": "work units"
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
    "text": "Avoided exact attribute reads outweigh the extra traversal and reranking work admitted by false positives.",
    "uncertainty": "The balance depends on selectivity, layout, and search behavior."
  },
  "confidence_rationale": {
    "assumptions": [
      "The fixture isolates selector error from graph changes."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "False positives necessarily add candidate work."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The source reports false-positive work and model limitations; the exact resource crossover remains a measurement question.",
    "uncertainty": "Cache and clustered-validity effects are not analytically bounded."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "A declared weighting or separate Pareto comparison is used rather than hiding resource trade-offs."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The comparison uses identical result correctness and search budget."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Speculative traversal performs at least as many weighted resource units as exact filtering while returning the same verified result.",
    "uncertainty": "No single scalar weighting is universally valid."
  },
  "failure_basis": "SOURCE_SUPPORTED_DERIVATION",
  "failure_id": "FAIL-FALSE-POSITIVES-AMPLIFY-WORKLOAD",
  "name": "False Positives Amplify Workload",
  "observable_symptom": {
    "assumptions": [
      "The baseline can avoid those candidates with exact filtering."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "False-positive candidates consume traversal and verification resources."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Extra candidate expansions, record reads, and final verification work equal or exceed the exact-filter baseline.",
    "uncertainty": "Cache effects and early termination can move the crossover."
  },
  "repair_options": [
    {
      "description": "Disable speculative filtering when measured false-positive work exceeds the admitted envelope.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Increase selector fidelity or colocate exact attributes to reduce speculative and deferred work.",
      "repair_class": "CHANGE_REPRESENTATION"
    }
  ],
  "source_paper_ids": [
    "PAPER-2605.17992"
  ],
  "source_pointers": [
    {
      "claim_scope": "False-positive traversal is permitted and exact checks are deferred.",
      "locator_type": "SECTION",
      "locator_value": "Section 3, Speculative Filtering",
      "page": 4,
      "paper_id": "PAPER-2605.17992",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Measured false-positive work and cost-model limitations are reported.",
      "locator_type": "SECTION",
      "locator_value": "Section 5.4, False-positive exploration rate",
      "page": 11,
      "paper_id": "PAPER-2605.17992",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "The approximate selector admits a high false-positive population or attribute co-location does not provide the expected page savings.",
    "uncertainty": "NONE"
  }
}
```
