# Static Entry Cache Shift

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The two query sets isolate access distribution."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Graph index",
      "Entry point",
      "Cache capacity",
      "Recall target",
      "Query count"
    ],
    "expected_observation": "The shifted query set has no read reduction while cache bytes remain resident.",
    "fixture_kind": "GRAPH",
    "fixture_name": "Entry cache distribution shift",
    "graph_scale": "Symbolic fixed graph and cache capacity.",
    "graph_shape": "A disk graph with two query regions sharing no entry-near traversal prefix.",
    "independent_oracle": "The identical deterministic search with the cache disabled and page reads traced.",
    "premises": [
      "The source calls the entry policy static and locality sensitive."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The exact path concentration required for benefit is unknown.",
    "varied_variables": [
      "Query-region distribution"
    ],
    "workload": "Run one query sequence near the cached entry, then one sequence that avoids it."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-CACHE-ENTRY-NEARBY-VERTICES"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Both runs use the same graph, queries, recall target, and storage trace method."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "storage_reads_cached >= storage_reads_uncached",
    "measurement_needed": "Trace cache hits, misses, and physical reads under controlled query-distribution shifts.",
    "numeric_constants": [],
    "premises": [
      "The source describes a static policy and graph-quality-sensitive benefit."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The shift magnitude needed to reverse benefit is unknown.",
    "variables": [
      {
        "definition": "physical storage reads with the static entry cache",
        "symbol": "storage_reads_cached",
        "units": "reads per query"
      },
      {
        "definition": "physical storage reads with the cache disabled",
        "symbol": "storage_reads_uncached",
        "units": "reads per query"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "Future query paths may shift away from the entry-near region."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source describes a static cache of the entry neighborhood and reports locality-sensitive benefit."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The entry-neighborhood cache assumes future traversal paths remain concentrated near a stable entry point.",
    "uncertainty": "The paper does not quantify distribution-shift tolerance."
  },
  "confidence_rationale": {
    "assumptions": [
      "Physical reads are measured at matched search semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source contrasts static and adaptive alternatives."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The static policy and its entry-near locality premise are source-stated; failure under a shifted query distribution is a derived test question.",
    "uncertainty": "No independent source code or benchmark reproduction was used."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "Physical reads are traced under identical graph, query, and recall semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source describes the policy as static and sensitive to entry-near path concentration."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Cached execution performs no fewer physical reads than the uncached oracle for the shifted query set.",
    "uncertainty": "The source does not publish this exact adversarial trace."
  },
  "failure_basis": "SOURCE_SUPPORTED_DERIVATION",
  "failure_id": "FAIL-STATIC-ENTRY-CACHE-SHIFT",
  "name": "Static Entry Cache Shift",
  "observable_symptom": {
    "assumptions": [
      "Resident cache bytes have no compensating benefit when the traced workload records no cache read savings."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source attributes the cache benefit to entry-near path concentration."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The static cache consumes resident memory without reducing early-hop storage reads.",
    "uncertainty": "Whole-process memory and physical read counts are not fully reported."
  },
  "repair_options": [
    {
      "description": "Require a measured entry-neighborhood hit rate before reserving cache bytes.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Disable or replace the static cache when the hit-rate guard fails.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Use a bounded adaptive cache when query distributions are not stable.",
      "repair_class": "CHANGE_REPRESENTATION"
    }
  ],
  "source_paper_ids": [
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "Static SSSP cache policy and adaptation limitation.",
      "locator_type": "SECTION",
      "locator_value": "Section 4.1.2, Cache Management",
      "page": 5,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Early-hop I/O benefit and graph-quality sensitivity.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Finding 3 and Cache discussion",
      "page": 8,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "A shifted query distribution can avoid the cached entry neighborhood while preserving the same search semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source caches vertices near one static entry point."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Queries whose paths avoid the cached entry neighborhood or whose access distribution shifts violate that locality assumption.",
    "uncertainty": "The source does not give a minimum hit-rate requirement."
  }
}
```
