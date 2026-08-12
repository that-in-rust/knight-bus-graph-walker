# Graph Residency Adds Passes

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "A test implementation exposes both successor-access modes."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Graph topology",
      "ordering schedule",
      "initial node order"
    ],
    "expected_observation": "Crossing the graph-memory budget activates additional passes or fails admission while preserving the same logical ordering task.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "resident-versus-nonresident-ordering",
    "graph_scale": "Increase graph representation size across a fixed graph-memory budget without changing topology.",
    "graph_shape": "A fixed directed graph presented through resident and deliberately memory-capped successor access.",
    "independent_oracle": "Compare final permutation validity and measured graph passes against a simple exact resident implementation.",
    "premises": [
      "The source names graph residency and a nonresident asymptotic cost."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "Constants and practical crossover depend on representation and storage.",
    "varied_variables": [
      "graph-memory budget",
      "successor-access mode"
    ],
    "workload": "Run the same multiresolution ordering schedule in resident and nonresident modes."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-COMPOSE-MULTIRESOLUTION-CLUSTER-ORDERS"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "The implementation cannot use a different low-memory successor oracle with lower cost."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "graph_storage_bytes > graph_memory_budget_bytes",
    "measurement_needed": "Measure peak graph bytes and pass count while crossing the available graph-memory budget.",
    "numeric_constants": [],
    "premises": [
      "The source requires graph space for its stated memory profile and names a higher-cost nonresident alternative."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "The source does not give a byte-level crossover for a specific implementation.",
    "variables": [
      {
        "definition": "Bytes required by the graph representation used during ordering.",
        "symbol": "graph_storage_bytes",
        "units": "bytes"
      },
      {
        "definition": "Bytes available to retain that graph representation.",
        "symbol": "graph_memory_budget_bytes",
        "units": "bytes"
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
    "text": "The reported memory-efficient ordering procedure assumes space for the graph in addition to its label arrays.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "The extracted page maps correctly to the cited section and footnote."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source explicitly states both resident state and nonresident cost."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The residency premise and nonresident cost are source-reported, while the exact byte crossover is implementation-dependent.",
    "uncertainty": "No local reproduction establishes constants."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "The test holds topology and ordering schedule fixed."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports a higher asymptotic cost when graph residency is avoided."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The nonresident run requires additional graph passes or cannot execute under the same memory cap.",
    "uncertainty": "Wall-clock impact depends on the successor-access implementation."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-GRAPH-RESIDENCY-ADDS-PASSES",
  "name": "Graph Residency Adds Passes",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The source states that avoiding graph residency changes the cost to O(n log n).",
    "uncertainty": "NONE"
  },
  "repair_options": [
    {
      "description": "Reject the resident procedure when graph plus label state exceeds the declared memory budget.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Provide an explicitly measured nonresident successor-access path and report its added passes.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-1011.5425"
  ],
  "source_pointers": [
    {
      "claim_scope": "LLP resident state includes the graph plus label arrays; avoiding graph residency changes the cost to O(n log n).",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 9 scalability discussion and footnote 21",
      "page": 9,
      "paper_id": "PAPER-1011.5425",
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
    "text": "The graph representation cannot remain in main memory during repeated clustering and ordering passes.",
    "uncertainty": "NONE"
  }
}
```
