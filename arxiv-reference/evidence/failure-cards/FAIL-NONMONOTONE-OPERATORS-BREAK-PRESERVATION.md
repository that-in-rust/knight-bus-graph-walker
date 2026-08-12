# Nonmonotone Operators Break Preservation

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The algorithm is naively extended to difference."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "input sets",
      "expression tree"
    ],
    "expected_observation": "The exact output element is absent from the candidate-refined result under the colliding hash.",
    "fixture_kind": "EXECUTION_PROFILE",
    "fixture_name": "difference-expression-hash-collision",
    "graph_scale": "Use the smallest input sets containing distinct elements mapped to the same hash value.",
    "graph_shape": "Treat sets as a bipartite element-membership graph with one included element and one excluded colliding element.",
    "independent_oracle": "Direct exact set-difference evaluation over original elements.",
    "premises": [
      "The source uses hash-image candidates and states monotonicity."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The paper does not claim support for this extension.",
    "varied_variables": [
      "hash function collision"
    ],
    "workload": "Evaluate an inclusion-minus-exclusion expression with the sourced candidate pipeline."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-REFINE-HASHED-CANDIDATES-EXACTLY"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Exact refinement only sees retained candidates."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "nonmonotone_collision_count > preservation_tolerance",
    "measurement_needed": "Enumerate small set expressions and hashes, comparing candidate preservation with exact evaluation.",
    "numeric_constants": [],
    "premises": [
      "The source proof depends on monotonicity and allows hash false positives."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The source does not analyze subtractive operators.",
    "variables": [
      {
        "definition": "Hash collisions whose presence in a subtractive input removes a true output hash.",
        "symbol": "nonmonotone_collision_count",
        "units": "collisions"
      },
      {
        "definition": "Such collisions permitted while retaining every true output candidate.",
        "symbol": "preservation_tolerance",
        "units": "collisions"
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
    "text": "Adding an element to any input cannot remove an element from the set expression result.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "The same reduction is applied to a nonmonotone expression."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Universal hashing permits collisions."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The source explicitly bases preservation on monotonicity; the collision counterexample shows why the assumption is necessary.",
    "uncertainty": "Alternative nonmonotone algorithms are outside the source scope."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "No separate exact scan recovers discarded inputs."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The chosen hash collision is controlled."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The refined result differs from direct exact evaluation by a false negative caused by the nonmonotone collision.",
    "uncertainty": "This falsifies an unsupported extension, not the sourced monotone algorithm."
  },
  "failure_basis": "ANALYTICAL_COUNTEREXAMPLE",
  "failure_id": "FAIL-NONMONOTONE-OPERATORS-BREAK-PRESERVATION",
  "name": "Nonmonotone Operators Break Preservation",
  "observable_symptom": {
    "assumptions": [
      "The colliding excluded-set element cancels the included-set hash."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The candidate stage retains only elements whose hash appears in the approximate expression result."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "A true output element is removed from the approximate candidate image and cannot be recovered by exact refinement.",
    "uncertainty": "A specialized two-sided error representation could avoid the loss."
  },
  "repair_options": [
    {
      "description": "Restrict the mechanism contract to monotone union-intersection expressions.",
      "repair_class": "SPECIALIZE_WORKLOAD"
    },
    {
      "description": "Use a two-sided-error or operator-specific candidate representation before supporting difference.",
      "repair_class": "CHANGE_REPRESENTATION"
    }
  ],
  "source_paper_ids": [
    "PAPER-0708.3259"
  ],
  "source_pointers": [
    {
      "claim_scope": "Hash images form approximate candidates followed by exact refinement.",
      "locator_type": "SECTION",
      "locator_value": "Section 1.3, Technical overview",
      "page": 6,
      "paper_id": "PAPER-0708.3259",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "The preservation proof requires a monotone union-intersection expression.",
      "locator_type": "SECTION",
      "locator_value": "Section 2.2, The general case",
      "page": 7,
      "paper_id": "PAPER-0708.3259",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "The implementation applies the monotone three-stage rule unchanged."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Hash images can contain false-positive collisions.",
      "Nonmonotone operators can turn an added approximate element into a removal."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The expression contains set difference or another nonmonotone operator and two unequal elements collide under the hash image.",
    "uncertainty": "Collision frequency depends on the hash range."
  }
}
```
