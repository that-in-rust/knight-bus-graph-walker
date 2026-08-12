# Tight memory increases storage runtime

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The workload reuses the displaced state enough for storage access to be observable."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "Graph",
      "Algorithm",
      "Storage device",
      "Thread count",
      "Output oracle"
    ],
    "expected_observation": "Outputs remain equal while storage traffic and runtime increase below the fit boundary",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "memory-cap storage crossover",
    "graph_scale": "Symbolic scale with a working set that can straddle the cap",
    "graph_shape": "A fixed graph whose reusable state can be placed in memory or storage",
    "independent_oracle": "Exact output comparison plus independent storage-byte and elapsed-time counters",
    "premises": [
      "The sources report memory sensitivity and all-in-disk trade-offs."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The fixture does not predict a universal slowdown ratio.",
    "varied_variables": [
      "Memory cap",
      "Resident placement"
    ],
    "workload": "Run identical queries or graph iterations under descending memory caps"
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-PLACE-SCALE-GROWING-STATE"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "The comparison holds graph, algorithm, and storage device constant."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "resident_working_set_bytes > memory_budget_bytes",
    "measurement_needed": "Measure peak residency, storage bytes, and elapsed time while sweeping a hard memory cap.",
    "numeric_constants": [],
    "premises": [
      "Both sources report the memory-versus-storage runtime trade-off."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The exact fit point varies by representation, cache policy, and device.",
    "variables": [
      {
        "definition": "State that would be reused from memory during the workload",
        "symbol": "resident_working_set_bytes",
        "units": "bytes"
      },
      {
        "definition": "Memory available to resident graph state",
        "symbol": "memory_budget_bytes",
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
    "text": "Moving scale-growing state out of memory preserves a lower memory footprint but can increase execution time through additional storage access.",
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
      "FP-001",
      "FP-002"
    ],
    "text": "The failure condition is bounded by the cited mechanism premise and negative result; the proposed fixture isolates the same condition without asserting an unsourced numeric threshold.",
    "uncertainty": "The precise crossover remains graph-, implementation-, and hardware-dependent unless the source states otherwise."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "expected_failure_signal": {
    "assumptions": [
      "Runs are otherwise controlled and warmed consistently."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports this trade-off in two storage-backed graph contexts."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The low-memory run preserves output but records more storage traffic and longer runtime than the fit-resident run.",
    "uncertainty": "Device caching and operating-system readahead can change the magnitude."
  },
  "failure_basis": "SOURCE_REPORTED",
  "failure_id": "FAIL-TIGHT-MEMORY-INCREASES-RUNTIME",
  "name": "Tight memory increases storage runtime",
  "observable_symptom": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Execution time increases as the working set spills from memory, with improvement plateauing only after the useful working set fits.",
    "uncertainty": "NONE"
  },
  "repair_options": [
    {
      "description": "Limit use of the mechanism to workloads satisfying its sourced applicability conditions.",
      "repair_class": "SPECIALIZE_WORKLOAD"
    },
    {
      "description": "Use a correctness-preserving fallback when the optimized path's assumptions fail.",
      "repair_class": "ADD_FALLBACK_PATH"
    },
    {
      "description": "Measure the unresolved crossover on the target graph and machine before admission.",
      "repair_class": "MEASURE_UNKNOWN"
    }
  ],
  "source_paper_ids": [
    "PAPER-2603.01779",
    "PAPER-HASH-b12240577b20eaad"
  ],
  "source_pointers": [
    {
      "claim_scope": "All-in-disk placement trades lower memory for additional storage access.",
      "locator_type": "SECTION",
      "locator_value": "Section 3.1 and Table 2",
      "page": 4,
      "paper_id": "PAPER-2603.01779",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "Execution time rises under tighter memory and plateaus when the working set fits.",
      "locator_type": "TABLE",
      "locator_value": "Table 4 and memory-restriction discussion",
      "page": 16,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
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
    "text": "Run a storage-backed workload with a memory budget below the resident working set while keeping the graph and algorithm fixed.",
    "uncertainty": "NONE"
  }
}
```
