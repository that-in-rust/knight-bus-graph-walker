# Bounded Table Drops Candidates

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The fixture preserves source scoring semantics while changing candidate order across stages."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "requested result cardinality",
      "score representation",
      "stage order",
      "tie rule"
    ],
    "expected_observation": "A candidate discarded by bounded aggregation appears in the complete final ranking or ranking error exceeds the admitted budget",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "late candidate promotion",
    "graph_scale": "Small deterministic graph sufficient to exceed the bounded candidate table",
    "graph_shape": "Staged graph decomposition whose separate score vectors promote different candidates",
    "independent_oracle": "Complete global-vector accumulation with deterministic tie-breaking",
    "premises": [
      "The source aggregates every stage into a bounded table."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Replacement policy is not reported and must be declared by the implementation.",
    "varied_variables": [
      "table multiplier",
      "late contribution magnitude"
    ],
    "workload": "Aggregate staged score vectors into the bounded table, then request the final ranking"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-BOUND-GLOBAL-SCORE-TABLE"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "The full-vector result is the comparison oracle."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "precision_loss(c, graph, query, k) > precision_budget",
    "measurement_needed": "Sweep capacity and compare final rankings with complete accumulation.",
    "numeric_constants": [],
    "premises": [
      "The table retains only bounded intermediate candidates.",
      "The source reports capacity-sensitive precision."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "No source establishes a portable loss function.",
    "variables": [
      {
        "definition": "table-capacity multiplier",
        "symbol": "c",
        "units": "dimensionless"
      },
      {
        "definition": "input graph and score distribution",
        "symbol": "graph",
        "units": "graph"
      },
      {
        "definition": "personalization query",
        "symbol": "query",
        "units": "query"
      },
      {
        "definition": "requested result cardinality",
        "symbol": "k",
        "units": "entries"
      },
      {
        "definition": "admitted ranking-error budget",
        "symbol": "precision_budget",
        "units": "dimensionless"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "Later subgraph contributions can change global candidate order."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source bounds the table and reports precision sensitivity when its multiplier is smaller."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "A fixed candidate table preserves the requested ranking only while discarded intermediate candidates cannot later enter the final result.",
    "uncertainty": "The source does not specify replacement and tie behavior."
  },
  "confidence_rationale": {
    "assumptions": [
      "The local frozen paper is the reviewed version."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The paper reports the bounded table and empirical sensitivity."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The failure is directly supported as capacity-sensitive precision loss, but its graph-dependent crossover remains unknown.",
    "uncertainty": "No independent reproduction or implementation inspection was performed."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "Both paths use identical stage scores and tie rules."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The complete accumulator retains every candidate."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Bounded and complete aggregation disagree on the final ranking or exceed the declared precision budget.",
    "uncertainty": "Approximation metrics must be selected before execution."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-BOUNDED-TABLE-DROPS-CANDIDATES",
  "name": "Bounded Table Drops Candidates",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "Smaller tested table settings produce materially larger final ranking precision loss.",
    "uncertainty": "The source does not report a universal safe capacity."
  },
  "repair_options": [
    {
      "description": "Reject capacity settings lacking an evidenced precision budget.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Use complete accumulation when bounded-table validation fails.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Expose table capacity and measured ranking error as separate admitted quantities.",
      "repair_class": "ADD_RESOURCE_BOUND"
    }
  ],
  "source_paper_ids": [
    "PAPER-2104.09616"
  ],
  "source_pointers": [
    {
      "claim_scope": "Fixed global score-table capacity and deferred final transfer",
      "locator_type": "SECTION",
      "locator_value": "Section V-B, Data Transfer Reduction",
      "page": 4,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Empirical precision sensitivity to the table multiplier",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section V-B final paragraph",
      "page": 4,
      "paper_id": "PAPER-2104.09616",
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
    "text": "The source evaluates staged personalized ranking with a fixed score table whose precision depends on its capacity setting.",
    "uncertainty": "The tested sensitivity is not portable to unseen graphs or query distributions."
  }
}
```
