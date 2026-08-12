# Page Shuffle Exceeds Memory

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The fixture uses the same construction strategy."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "record encoding",
      "build threads",
      "memory budget"
    ],
    "expected_observation": "Construction exceeds the memory budget or refuses admission before producing the permutation.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "forward-reverse-build-pressure",
    "graph_scale": "Increase graph scale until the declared build-memory budget is crossed.",
    "graph_shape": "A directed graph whose forward and reverse forms have the same fixed topology but increasing encoded size.",
    "independent_oracle": "A streaming external reorder that emits an equivalent vertex permutation is the correctness oracle.",
    "premises": [
      "The source reports the simultaneous representations and an observed inability to build at scale."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "A different implementation could reduce the peak.",
    "varied_variables": [
      "node count",
      "edge count"
    ],
    "workload": "Run only PageShuffle construction with peak-memory component accounting."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-COLOCATE-NEIGHBORS-WITHIN-PAGES"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "The build does not stream one representation out before the other is needed."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "graph_bytes + reverse_graph_bytes + shuffle_workspace_bytes > build_memory_budget_bytes",
    "measurement_needed": "Measure component-wise peak resident bytes during construction across graph scales.",
    "numeric_constants": [],
    "premises": [
      "The source identifies simultaneous graph and reverse-graph residency."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Allocator and implementation overhead are not isolated.",
    "variables": [
      {
        "definition": "Resident bytes for the forward graph during shuffle.",
        "symbol": "graph_bytes",
        "units": "bytes"
      },
      {
        "definition": "Resident bytes for the reverse graph during shuffle.",
        "symbol": "reverse_graph_bytes",
        "units": "bytes"
      },
      {
        "definition": "Additional ordering and mapping workspace.",
        "symbol": "shuffle_workspace_bytes",
        "units": "bytes"
      },
      {
        "definition": "Memory budget for offline construction.",
        "symbol": "build_memory_budget_bytes",
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
    "text": "The offline shuffle can retain both the graph and its reverse representation during construction.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "Peak RSS is the relevant budget signal."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The cited pages identify build structures and measured overhead."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The source directly reports the memory limitation; the symbolic sum avoids inventing a numeric crossover.",
    "uncertainty": "No per-component byte trace is source-reported."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The PageShuffle build exceeds available memory before completing the largest construction.",
    "uncertainty": "NONE"
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-PAGE-SHUFFLE-EXCEEDS-MEMORY",
  "name": "Page Shuffle Exceeds Memory",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The source reports that PageShuffle construction can exceed memory and prevents the largest reported build.",
    "uncertainty": "NONE"
  },
  "repair_options": [
    {
      "description": "Account for forward graph, reverse graph, mappings, and workspace before construction admission.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Use an external-memory or partitioned shuffle when the resident build estimate exceeds budget.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "PageShuffle construction retains the graph and reverse graph and can exceed available memory.",
      "locator_type": "SECTION",
      "locator_value": "Finding 6, construction overhead",
      "page": 9,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Measured build overhead and complementarity are reported for the evaluated configurations.",
      "locator_type": "TABLE",
      "locator_value": "Table 6 and Finding 8",
      "page": 10,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The combined construction representations exceed available build memory at larger scale.",
    "uncertainty": "NONE"
  }
}
```
