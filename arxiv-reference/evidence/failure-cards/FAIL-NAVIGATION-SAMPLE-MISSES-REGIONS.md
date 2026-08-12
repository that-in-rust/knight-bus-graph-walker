# Navigation Sample Misses Regions

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Clusters make entry-region coverage observable without changing search semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "disk graph",
      "vectors",
      "queries",
      "recall target",
      "beam policy"
    ],
    "expected_observation": "The sampled entry path misses the useful cluster or saves fewer disk reads than its resident and search costs justify",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "uncovered query region",
    "graph_scale": "Small clustered index whose coverage can be enumerated",
    "graph_shape": "Proximity graph with separated clusters and a query region absent from a small resident sample",
    "independent_oracle": "Exhaustive best entry-point search over all graph regions plus matched-recall disk trace",
    "premises": [
      "The source identifies coverage and oversampling trade-offs."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Approximate-search tie behavior must be fixed.",
    "varied_variables": [
      "sample membership",
      "sample size"
    ],
    "workload": "Search from resident-sample entry points, then continue on disk"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-NAVIGATE-MEMORY-BEFORE-DISK"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Matched recall and query distribution are fixed."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "navigation_ram(sample) + navigation_time(sample) + disk_search_cost(sample) > admitted_query_cost",
    "measurement_needed": "Measure RAM, entry-search time, disk reads, and recall separately over sample policies.",
    "numeric_constants": [],
    "premises": [
      "Sampling trades resident cost for better disk entry points."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The terms use different units and require separately declared admission limits.",
    "variables": [
      {
        "definition": "resident navigation sample policy",
        "symbol": "sample",
        "units": "policy"
      },
      {
        "definition": "resident sampled-graph memory",
        "symbol": "navigation_ram",
        "units": "bytes"
      },
      {
        "definition": "in-memory entry search time",
        "symbol": "navigation_time",
        "units": "time"
      },
      {
        "definition": "downstream disk-search cost",
        "symbol": "disk_search_cost",
        "units": "operations"
      },
      {
        "definition": "declared multi-resource query budget",
        "symbol": "admitted_query_cost",
        "units": "budget"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "The disk graph remains the correctness target at matched recall."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source states under-sampling and over-sampling costs."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "A resident sampled graph provides useful entry points only if it covers query-relevant regions without consuming more RAM and search time than it saves on disk.",
    "uncertainty": "Coverage is query-distribution dependent."
  },
  "confidence_rationale": {
    "assumptions": [
      "The resident sample is the only changed entry mechanism."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source pages were rechecked."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The source directly states both undersampling and oversampling boundaries, while target-specific coverage remains unknown.",
    "uncertainty": "No independent search benchmark was run."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "Queries and downstream search policy are identical."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Sample membership changes entry coverage."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Recall falls, disk work rises, or resident navigation cost crosses its admitted limit.",
    "uncertainty": "No single scalar combines RAM and latency without a declared policy."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-NAVIGATION-SAMPLE-MISSES-REGIONS",
  "name": "Navigation Sample Misses Regions",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Matched-recall disk work or total latency worsens, or the resident sample exceeds its RAM allocation.",
    "uncertainty": "The source reports trade-offs rather than a universal failure threshold."
  },
  "repair_options": [
    {
      "description": "Validate sample coverage against the intended query distribution.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Bound resident sample bytes and entry-search time separately.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Use additional deterministic entry points when coverage diagnostics fail.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "Sampled navigation graph and under-versus-over sampling trade-off",
      "locator_type": "SECTION",
      "locator_value": "Section 4.1.3, Hierarchical Graphs",
      "page": 5,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Diminishing returns and RAM allocation trade-off",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 6.3 and Finding 7",
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
      "FP-001",
      "FP-002"
    ],
    "text": "Insufficient sampling can miss useful entry regions, whereas excessive sampling increases resident memory and in-memory search time with diminishing returns.",
    "uncertainty": "The source does not give a universal sample ratio."
  }
}
```
