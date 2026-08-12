# Cache Partition Spill Reversal

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Working-set bytes include all partition-local mutable state."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Graph",
      "Labeling",
      "Algorithm",
      "Thread count",
      "Iteration count"
    ],
    "expected_observation": "Results match while the oversized partition incurs more cache misses, DRAM traffic, or time than the admitted baseline.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "Partition cache spill",
    "graph_scale": "Symbolic graph with partition size swept around measured cache availability.",
    "graph_shape": "One fixed graph and labeling processed with partition working sets below and above available private cache.",
    "independent_oracle": "Straightforward exact push or pull implementation with hardware counters and identical iteration semantics.",
    "premises": [
      "The source reports the cache-spill reversal."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Effective cache availability must be measured.",
    "varied_variables": [
      "Partition working-set bytes",
      "Concurrent cache pressure"
    ],
    "workload": "Run exact partitioned updates for a fixed number of iterations."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-STREAM-PARTITIONED-UPDATES-SEQUENTIALLY"
  ],
  "breakpoint_equation": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "expression": "partition_working_set_bytes > private_cache_available_bytes",
    "measurement_needed": "Measure cache occupancy, misses, DRAM bytes, and elapsed time while sweeping partition size.",
    "numeric_constants": [],
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "Available cache capacity and associativity are machine and co-runner dependent.",
    "variables": [
      {
        "definition": "mutable vertex state and partition-local metadata touched during ownership",
        "symbol": "partition_working_set_bytes",
        "units": "bytes"
      },
      {
        "definition": "cache capacity available to the worker after concurrent pressure",
        "symbol": "private_cache_available_bytes",
        "units": "bytes"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Sequential partitioned updates assume the active partition working set fits the intended cache while preserving enough locality to amortize bin and layout work.",
    "uncertainty": "Concurrent cache pressure changes the available capacity."
  },
  "confidence_rationale": {
    "assumptions": [
      "The implementation measures the complete working set."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source links partition size, cache misses, DRAM traffic, and time."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The cache-capacity reversal is directly source-reported; the precise available-cache admission measurement is local.",
    "uncertainty": "No local hardware-counter run occurred."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "Partition spill produces the source-described cache-miss and main-memory-traffic increase.",
    "uncertainty": "The exact machine crossover is not portable."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-CACHE-PARTITION-SPILL-REVERSAL",
  "name": "Cache Partition Spill Reversal",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "Cache misses increase main-memory traffic and can increase execution time despite better compression.",
    "uncertainty": "The source reports a platform-specific design-space sweep."
  },
  "repair_options": [
    {
      "description": "Size partitions from measured available cache, not nominal capacity alone.",
      "repair_class": "ADD_RESOURCE_BOUND"
    },
    {
      "description": "Reduce partition size or active ownership when cache pressure rises.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Use a non-partitioned or smaller-partition path when the cache-fit guard fails.",
      "repair_class": "ADD_FALLBACK_PATH"
    }
  ],
  "source_paper_ids": [
    "PAPER-1709.07122"
  ],
  "source_pointers": [
    {
      "claim_scope": "Partition-local transposed layout and cacheability premise.",
      "locator_type": "ALGORITHM",
      "locator_value": "Section 3.3 and Algorithm 3",
      "page": 6,
      "paper_id": "PAPER-1709.07122",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Cache-capacity spill, memory-traffic increase, and execution-time trade-off.",
      "locator_type": "FIGURE",
      "locator_value": "Figures 11-13 and Section 5.3.2",
      "page": 11,
      "paper_id": "PAPER-1709.07122",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-002"
    ],
    "text": "The partition working set exceeds cache capacity or its vertex accesses have poor locality.",
    "uncertainty": "The source breakpoint is hardware-specific."
  }
}
```
