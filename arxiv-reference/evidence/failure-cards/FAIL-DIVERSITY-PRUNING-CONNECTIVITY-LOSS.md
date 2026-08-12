# Diversity Pruning Connectivity Loss

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The sole bridge is globally necessary and local pruning is deterministic."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Cluster sizes",
      "Degree cap",
      "Entry vertices",
      "Queries"
    ],
    "expected_observation": "The un-repaired graph loses reachability or exact recall while the repaired graph preserves it.",
    "fixture_kind": "GRAPH",
    "fixture_name": "Diversity bridge deletion",
    "graph_scale": "Symbolic two-cluster graph with fixed degree cap.",
    "graph_shape": "Two dense vector clusters joined by one candidate edge that appears locally directionally redundant.",
    "independent_oracle": "Unpruned graph reachability plus brute-force nearest-neighbor results.",
    "premises": [
      "The source states that explicit connectivity assurance improves search."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003",
      "FP-004"
    ],
    "uncertainty": "A source-authored bridge instance is unavailable.",
    "varied_variables": [
      "Bridge geometry",
      "Pruning parameter"
    ],
    "workload": "Apply the diversity rule with and without a connectivity repair, then search across clusters."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-PRUNE-NEIGHBORS-BY-DIVERSITY"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Reachability from admitted entries is required for correctness of the search surface."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "reachable_vertices_pruned < reachable_vertices_oracle",
    "measurement_needed": "Construct and enumerate bridge cases, then compare reachability and exact recall.",
    "numeric_constants": [],
    "premises": [
      "The source treats pruning and connectivity as distinct components and reports performance differences."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003",
      "FP-004"
    ],
    "uncertainty": "The minimum bridge geometry satisfying the pruning predicate is not source-reported.",
    "variables": [
      {
        "definition": "vertices reachable from admitted entries after diversity pruning",
        "symbol": "reachable_vertices_pruned",
        "units": "vertices"
      },
      {
        "definition": "vertices required reachable by the unpruned or connectivity-repaired graph",
        "symbol": "reachable_vertices_oracle",
        "units": "vertices"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003",
      "FP-004"
    ],
    "text": "Local directional diversity pruning does not by itself guarantee that every graph component remains reachable from search entries.",
    "uncertainty": "Connectivity depends on construction strategy and dataset."
  },
  "confidence_rationale": {
    "assumptions": [
      "The target search requires global entry reachability."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source evaluates connectivity as separate from neighbor selection."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002",
      "FP-003",
      "FP-004"
    ],
    "text": "The need for connectivity assurance is sourced; the sole-bridge counterexample is a derived minimal witness.",
    "uncertainty": "No local implementation test was run."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "The oracle and pruned graph use the same entries and queries."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Connectivity is a separate required component in the source."
    ],
    "source_pointer_ids": [
      "FP-002",
      "FP-003"
    ],
    "text": "A required vertex becomes unreachable or its exact neighbor cannot be returned after pruning.",
    "uncertainty": "The fixture is analytical rather than source-measured."
  },
  "failure_basis": "SOURCE_SUPPORTED_DERIVATION",
  "failure_id": "FAIL-DIVERSITY-PRUNING-CONNECTIVITY-LOSS",
  "name": "Diversity Pruning Connectivity Loss",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002",
      "FP-003",
      "FP-004"
    ],
    "text": "The pruned graph has unreachable components and worse search performance than a graph with explicit connectivity assurance.",
    "uncertainty": "Recall impact depends on query and entry distribution."
  },
  "repair_options": [
    {
      "description": "Reject a pruned graph that fails entry-rooted reachability checks.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Add a connectivity repair such as a spanning or DFS-derived bridge set.",
      "repair_class": "CHANGE_REPRESENTATION"
    },
    {
      "description": "Retain necessary unpruned bridge edges when repair cannot prove reachability.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-2101.12631"
  ],
  "source_pointers": [
    {
      "claim_scope": "Distance-and-direction diversity pruning.",
      "locator_type": "SECTION",
      "locator_value": "Section 4.1, C3 Neighbor Selection",
      "page": 7,
      "paper_id": "PAPER-2101.12631",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Dataset-sensitive connectivity and disconnected components.",
      "locator_type": "SECTION",
      "locator_value": "Section 5.2, Connectivity",
      "page": 9,
      "paper_id": "PAPER-2101.12631",
      "pointer_id": "FP-002"
    },
    {
      "claim_scope": "Search advantage of explicit connectivity assurance.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 10(e), connectivity component evaluation",
      "page": 11,
      "paper_id": "PAPER-2101.12631",
      "pointer_id": "FP-003"
    },
    {
      "claim_scope": "Need to combine diversity, degree control, and connectivity.",
      "locator_type": "SECTION",
      "locator_value": "Section 6, Guidelines",
      "page": 12,
      "paper_id": "PAPER-2101.12631",
      "pointer_id": "FP-004"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "A bridge can satisfy the local rejection rule while remaining globally necessary."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source separates neighbor diversity from connectivity assurance."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "A locally redundant-looking edge is the sole global bridge between two vector clusters and is removed by the diversity rule.",
    "uncertainty": "The source does not provide this minimal bridge fixture."
  }
}
```
