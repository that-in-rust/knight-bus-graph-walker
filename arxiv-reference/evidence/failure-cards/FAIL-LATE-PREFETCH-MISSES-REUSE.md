# Late Prefetch Misses Reuse

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The platform exposes sufficiently precise cache or demand-miss observations."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "target addresses",
      "search result",
      "initial cache state",
      "prefetch trigger order"
    ],
    "expected_observation": "Observe unchanged or higher demand misses when arrival is late or the target is re-evicted.",
    "fixture_kind": "EXECUTION_PROFILE",
    "fixture_name": "Late or re-evicted search state",
    "graph_scale": "Minimal deterministic search step with observable target cache lines.",
    "graph_shape": "No graph topology change; fixed addresses represent the query vector, distance table, and visited metadata.",
    "independent_oracle": "Address and cache-event traces plus the exact result from the same step with prefetch disabled.",
    "premises": [
      "The source defines targets and trigger points but no timing guarantee."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "Hardware prefetch, replacement policy, and counters may confound causal attribution.",
    "varied_variables": [
      "request-to-use delay",
      "conflicting working-set size"
    ],
    "workload": "Displace the targets, issue the source-defined prefetches, and vary delay and conflicting accesses before first use."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-PREFETCH-DISPLACED-SEARCH-STATE"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "Arrival no earlier than use cannot avoid that use's demand miss."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "T_prefetch_arrival >= T_first_use",
    "measurement_needed": "Measure request issue, cache fill, first-use timestamps, demand misses, and memory traffic per target structure.",
    "numeric_constants": [],
    "premises": [
      "The sourced mechanism prefetches state to make it warm before resumed computation."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "The source does not define arrival timing or cache residency guarantees.",
    "variables": [
      {
        "definition": "Time at which requested data becomes usable in the cache hierarchy",
        "symbol": "T_prefetch_arrival",
        "units": "time"
      },
      {
        "definition": "Time resumed search first consumes the target data",
        "symbol": "T_first_use",
        "units": "time"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [
      "A prefetch that arrives after use or is evicted before use cannot satisfy that premise."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source's benefit depends on named search structures being warm before distance computation resumes."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The requested lines must arrive before resumed search uses them and remain resident until that use.",
    "uncertainty": "Processor, cache, and request timing are not specified by the source."
  },
  "confidence_rationale": {
    "assumptions": [
      "Late arrival or pre-use eviction cannot warm data for that use."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source names the structures and intended timing."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The counterexample follows directly from the sourced warm-before-use premise, but its frequency is unknown.",
    "uncertainty": "No isolated source ablation, hardware trace, or target-platform measurement establishes prevalence."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "The fixture isolates target lines and preserves search semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "A cache restoration mechanism succeeds only if residency changes before use."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The source-defined requests occur, but target demand misses are not reduced before first use and total memory traffic does not improve.",
    "uncertainty": "The signal is not a source-reported measurement."
  },
  "failure_basis": "ANALYTICAL_COUNTEREXAMPLE",
  "failure_id": "FAIL-LATE-PREFETCH-MISSES-REUSE",
  "name": "Late Prefetch Misses Reuse",
  "observable_symptom": {
    "assumptions": [
      "Demand-miss and memory-traffic counters accurately observe the target lines."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The mechanism is intended to alter cache state rather than search semantics."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Demand misses are not reduced and memory traffic may increase even though exact search results remain unchanged.",
    "uncertainty": "Counter behavior and cache replacement policy are platform-specific."
  },
  "repair_options": [
    {
      "description": "Enable requests only when measured request-to-use distance and cache pressure can make them timely.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Move the trigger earlier or reduce intervening conflicting work.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Disable cache restoration when counters show no avoided misses.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Measure target-specific arrival, eviction, and bandwidth effects.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-2605.19335"
  ],
  "source_pointers": [
    {
      "claim_scope": "Named prefetch targets, trigger points, and intended cache-warming premise.",
      "locator_type": "SECTION",
      "locator_value": "Section 4.4, Cache prefetching",
      "page": 7,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "FP-001"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "The target processor permits delayed or nonbinding prefetch behavior."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source issues prefetches after update execution and after I/O completion."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "A short request-to-use interval or intervening conflicting working set makes the prefetch late or causes re-eviction.",
    "uncertainty": "This workload is analytically constructed and not source-measured."
  }
}
```
