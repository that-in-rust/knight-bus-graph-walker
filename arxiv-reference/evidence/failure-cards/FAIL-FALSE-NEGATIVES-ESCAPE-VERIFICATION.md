# False Negatives Escape Verification

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The search has no alternate path to the rejected candidate."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "graph and vector distances",
      "exact attributes",
      "search budget"
    ],
    "expected_observation": "The injected run omits the oracle result even though final verification itself is exact.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "single-valid-filter-false-negative",
    "graph_scale": "Use the smallest path containing a start, the rejected valid item, and a returned distractor.",
    "graph_shape": "A query graph path whose only exact nearest valid result is behind one selector-rejected valid bridge or candidate.",
    "independent_oracle": "Exhaustive exact attribute filtering followed by exact distance ordering.",
    "premises": [
      "The source correctness invariant is no false negatives."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "A richer graph may offer alternate paths.",
    "varied_variables": [
      "one approximate membership decision"
    ],
    "workload": "Run speculative traversal once with an exact selector and once with the single injected false negative."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-EXPLORE-SUPERSET-VERIFY-RESULTS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Exactness tolerance permits no omitted valid result."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "false_negative_count > exactness_tolerance",
    "measurement_needed": "Inject controlled selector false negatives and compare results with exhaustive exact filtering.",
    "numeric_constants": [],
    "premises": [
      "The source requires no false negatives for sound superset exploration."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "No probability model for selector errors is supplied.",
    "variables": [
      {
        "definition": "Valid candidates rejected by the approximate selector before exploration.",
        "symbol": "false_negative_count",
        "units": "candidates"
      },
      {
        "definition": "Allowed rejected-valid-candidate count for exact result preservation.",
        "symbol": "exactness_tolerance",
        "units": "candidates"
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
    "text": "The approximate selector rejects only invalid vectors and never rejects a valid vector.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "There is no recovery path over rejected candidates."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source explicitly places exact verification after superset exploration."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "This is a direct logical counterexample to the sourced no-false-negative invariant.",
    "uncertainty": "Production filter implementations still require separate fault measurement."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "All other randomness is fixed."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Verification cannot examine an unvisited candidate."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The verified output differs from exhaustive exact filtering only when the false-negative decision is injected.",
    "uncertainty": "Approximate-nearest search may require a recall-aware oracle as well as attribute exactness."
  },
  "failure_basis": "ANALYTICAL_COUNTEREXAMPLE",
  "failure_id": "FAIL-FALSE-NEGATIVES-ESCAPE-VERIFICATION",
  "name": "False Negatives Escape Verification",
  "observable_symptom": {
    "assumptions": [
      "No independent exact fallback scans rejected candidates."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The candidate is removed before exploration and exact checking."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Exact post-verification returns a result set that omits the valid candidate because verification cannot recover an unvisited item.",
    "uncertainty": "Result impact depends on whether the omitted candidate belongs in the requested top set."
  },
  "repair_options": [
    {
      "description": "Admit only approximate filters with a proved or exhaustively validated no-false-negative property for the supported predicate.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Route unsupported predicates to exact prefiltering or a complete exact scan.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-2605.17992"
  ],
  "source_pointers": [
    {
      "claim_scope": "Correctness relies on an approximate selector with no false negatives and final exact verification.",
      "locator_type": "SECTION",
      "locator_value": "Section 3, Speculative Filtering",
      "page": 4,
      "paper_id": "PAPER-2605.17992",
      "pointer_id": "FP-001"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "The approximate selector is allowed to make at least one false-negative error."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Final verification operates only on explored candidates."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "A valid nearest candidate is a false negative of the approximate selector before graph exploration reaches it.",
    "uncertainty": "The source mechanism is designed to exclude this condition."
  }
}
```
