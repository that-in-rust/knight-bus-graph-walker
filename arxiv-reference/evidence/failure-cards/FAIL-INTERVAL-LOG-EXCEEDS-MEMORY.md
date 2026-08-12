# Interval Log Exceeds Memory

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The graph engine exposes per-interval log size and spill counters."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Partition boundaries",
      "message width",
      "host-memory budget"
    ],
    "expected_observation": "The hot interval crosses its memory budget and triggers spill, external sort, or admission failure.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "destination-hotspot-update-log",
    "graph_scale": "Grow active edges while holding destination interval width and host-memory budget fixed.",
    "graph_shape": "Many active source vertices send updates into one compact destination interval.",
    "independent_oracle": "An external stable sort grouped by destination vertex provides the exact update multiset and result.",
    "premises": [
      "Destination partitioning is the sourced grouping mechanism."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "The smallest triggering skew depends on message encoding.",
    "varied_variables": [
      "fraction of updates targeting the hot interval",
      "active-edge count"
    ],
    "workload": "Execute one superstep and collect per-interval update-log bytes."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-PARTITION-UPDATES-BY-DESTINATION"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "No compression or repartition reduces the interval before loading."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "largest_interval_log_bytes > interval_log_memory_budget_bytes",
    "measurement_needed": "Measure the largest interval log and spill behavior under controlled destination skew.",
    "numeric_constants": [],
    "premises": [
      "The current interval log must be loaded for in-memory grouping."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "The source gives no universal skew breakpoint.",
    "variables": [
      {
        "definition": "Bytes in the most heavily targeted destination interval log.",
        "symbol": "largest_interval_log_bytes",
        "units": "bytes"
      },
      {
        "definition": "Host-memory budget for one interval log.",
        "symbol": "interval_log_memory_budget_bytes",
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
    "text": "The destination-partitioned design relies on the currently processed interval update log fitting host memory.",
    "uncertainty": "The source says this is typical rather than universal."
  },
  "confidence_rationale": {
    "assumptions": [
      "Destination skew can be controlled independently in the fixture."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Update volume may scale with edge volume and logs are destination-partitioned."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The source establishes the fit assumption and overflow consequence; concentration into one interval is an analytical workload construction.",
    "uncertainty": "No source benchmark isolates adversarial interval skew."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "Instrumentation accounts for all interval-log buffers."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source connects oversized logs to SSD storage and external sorting."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Peak interval-log bytes exceed the declared budget or spill/external-sort counters become nonzero.",
    "uncertainty": "Allocator overhead may move the measured crossover."
  },
  "failure_basis": "ANALYTICAL_COUNTEREXAMPLE",
  "failure_id": "FAIL-INTERVAL-LOG-EXCEEDS-MEMORY",
  "name": "Interval Log Exceeds Memory",
  "observable_symptom": {
    "assumptions": [
      "The implementation has no finer emergency repartition before processing."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source ties memory overflow of logs to SSD residence and external sorting."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The interval log spills or requires external sorting, restoring the cost the multi-log arrangement was intended to avoid.",
    "uncertainty": "A spill implementation may degrade rather than fail."
  },
  "repair_options": [
    {
      "description": "Split or flush an interval before its log exceeds the declared memory budget.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Use a bounded streaming grouping representation for a hot destination interval.",
      "repair_class": "CHANGE_REPRESENTATION"
    }
  ],
  "source_paper_ids": [
    "PAPER-1905.04264"
  ],
  "source_pointers": [
    {
      "claim_scope": "A global update log can exceed host memory; the multi-log design relies on an interval log typically fitting host memory.",
      "locator_type": "SECTION",
      "locator_value": "Section IV.A-B, update-log and multi-log discussion",
      "page": 4,
      "paper_id": "PAPER-1905.04264",
      "pointer_id": "FP-001"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "An adversarial graph and active set can concentrate destinations inside one interval."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source allows update volume proportional to edge volume and partitions logs by destination interval."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Updates are destination-skewed so a single interval receives nearly the whole superstep log.",
    "uncertainty": "The paper does not report the worst observed interval skew."
  }
}
```
