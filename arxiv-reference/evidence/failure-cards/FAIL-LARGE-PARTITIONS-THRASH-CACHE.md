# Large partitions thrash effective cache

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "Hardware counters adequately represent cache and DRAM behavior."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Graph",
      "Update function",
      "Thread placement",
      "Vertex ordering",
      "Hardware"
    ],
    "expected_observation": "Above effective cache, misses, DRAM traffic, or time rise even if compression improves",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "partition cache-capacity crossover",
    "graph_scale": "Symbolic graph supporting partitions below and above effective cache",
    "graph_shape": "A partitionable graph with random node accesses inside each partition",
    "independent_oracle": "Hardware cache-miss and DRAM counters plus exact output comparison",
    "premises": [
      "The source directly reports this reversal."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "The exact partition size is platform-specific.",
    "varied_variables": [
      "Partition size",
      "Effective cache allocation"
    ],
    "workload": "Run the same partition-centric update with increasing partition working sets"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-SELECT-PARTITION-SCATTER-MODE"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "The target mode's bandwidth estimate is sensitive to the resulting memory hierarchy behavior."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "partition_working_set_bytes > effective_cache_bytes",
    "measurement_needed": "Sweep partition size while measuring cache misses, DRAM bytes, compression, and elapsed scatter time.",
    "numeric_constants": [],
    "premises": [
      "The source reports a cache-capacity reversal as partition size grows."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "uncertainty": "Effective cache depends on sharing and access pattern, not nominal capacity alone.",
    "variables": [
      {
        "definition": "Random-access node state touched within one partition",
        "symbol": "partition_working_set_bytes",
        "units": "bytes"
      },
      {
        "definition": "Cache capacity effectively available to that state",
        "symbol": "effective_cache_bytes",
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
    "text": "Increasing partition size improves compression only until the partition working set exceeds effective cache capacity, after which cache misses and execution time rise.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "The proposed fixture preserves the source mechanism while varying only the stated trigger."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The cited pages define the mechanism and its reported or analytically exposed boundary."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "The failure condition is bounded by the cited mechanism premise and negative result; the proposed fixture isolates the same condition without asserting an unsourced numeric threshold.",
    "uncertainty": "The precise crossover remains graph-, implementation-, and hardware-dependent unless the source states otherwise."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "Runs control thread placement and cache state."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports the crossover."
    ],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "A larger partition preserves output and improves or maintains compression but produces worse cache, DRAM, or elapsed-time measurements.",
    "uncertainty": "Counter attribution may be noisy on shared systems."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-LARGE-PARTITIONS-THRASH-CACHE",
  "name": "Large partitions thrash effective cache",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "DRAM traffic and execution time increase despite improved compression or reduced nominal communication volume.",
    "uncertainty": "NONE"
  },
  "repair_options": [
    {
      "description": "Reject or reroute workloads once the symbolic failure predicate is observed.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Choose a schedule that avoids the reported work, contention, or locality reversal.",
      "repair_class": "CHANGE_SCHEDULE"
    },
    {
      "description": "Measure the unresolved crossover on the target graph and machine before admission.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-1709.07122"
  ],
  "source_pointers": [
    {
      "claim_scope": "Large partitions exceed effective cache, increase cache misses and DRAM traffic, and can increase execution time.",
      "locator_type": "FIGURE",
      "locator_value": "Figures 12-13 and Section 5.3.2",
      "page": 11,
      "paper_id": "PAPER-1709.07122",
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
    "text": "Select a partition size whose random-access working set is larger than the effective cache available to the processing threads.",
    "uncertainty": "NONE"
  }
}
```
