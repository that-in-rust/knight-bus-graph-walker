# Balance Phase Specific Workloads

- Pattern ID: `PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus can observe or predict frontier phase.",
      "Its runtime supports bounded work stealing or an equivalent scheduler."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source shows different units of work balance different BFS phases.",
      "The source identifies too-fine partitioning as overhead."
    ],
    "source_pointer_ids": [
      "SP-031",
      "SP-032",
      "SP-034"
    ],
    "text": "Knight Bus could quote phase-specific frontier and partition state rather than one worst-case scheduling rule, provided it also caps partition count and records the phase switch in the receipt.",
    "uncertainty": "The source does not provide an admission-time phase forecast or deterministic contention bound."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-031",
      "SP-032",
      "SP-034"
    ],
    "text": "The schedule changes with frontier phase: edge blocks are statically split early, bottom-up partitions are stolen dynamically in the middle, and vertices are statically divided late.",
    "uncertainty": "The source does not derive a universal phase detector."
  },
  "confidence_rationale": {
    "assumptions": [
      "The cited paper and pointers accurately represent the evaluated mechanism."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The cited source pointers describe or evaluate the mechanism.",
      "G05 did not independently reproduce the source result or inspect implementation code."
    ],
    "source_pointer_ids": [
      "SP-031",
      "SP-034",
      "SP-035"
    ],
    "text": "The source provides pseudocode, thread-work distributions, tuning observations, and scaling results, but this campaign did not reproduce or inspect the implementation.",
    "uncertainty": "Grade C is limited to the reported graph families and shared-memory platform."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-031",
      "SP-032"
    ],
    "text": "Top-down phases use CSR frontier ranges, while bottom-up phases use cache-line-aligned visited, current-frontier, and next-frontier bitmaps plus contiguous vertex partitions.",
    "uncertainty": "The bitmap layout assumes shared-memory access."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-031",
        "SP-034"
      ],
      "text": "Too many small bottom-up partitions cause frequent work-stealing overhead, while a vertex-only top-down split remains imbalanced when few frontier vertices have very high degree.",
      "uncertainty": "The paper gives empirical scale-dependent guidance rather than a universal threshold."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS",
  "falsifying_test": {
    "controlled_variables": [
      "graph order",
      "source vertex",
      "thread count",
      "direction-switch thresholds",
      "partition factor"
    ],
    "failure_signal": "The phase schedule changes reachability or distance, leaves persistent severe imbalance, or exceeds the capped partition and temporary-state budget",
    "fixture": "A power-law graph whose BFS levels include a tiny high-degree frontier, a dense bottom-up frontier, and a low-degree tail",
    "independent_oracle": "Sequential level-synchronous BFS distances and predecessors normalized for tie freedom",
    "scope": "Smallest correctness and scheduling-bound falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-031",
      "SP-032"
    ],
    "text": "Each current-level work item is assigned exactly within the phase's partitioning scheme, while atomic or disjoint bitmap updates preserve BFS visitation and predecessor semantics.",
    "uncertainty": "The paper's correctness argument is operational rather than a formal theorem."
  },
  "knight_bus_algorithm_families": [
    "BFS",
    "SHORTEST_PATHS_UNWEIGHTED",
    "WCC",
    "LEVEL_SYNCHRONOUS_TRAVERSAL"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-031",
      "SP-032",
      "SP-033"
    ],
    "text": "Split high-degree top-down adjacency ranges evenly across threads, use many descending-size bottom-up partitions with dynamic work stealing, and return to simple vertex scheduling for the low-degree shrinking tail.",
    "uncertainty": "Phase switches and partition factor are tuned heuristically."
  },
  "name": "Balance Phase Specific Workloads",
  "pattern_id": "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-031",
      "SP-032"
    ],
    "text": "Power-law frontiers change shape across BFS levels, so assigning frontier vertices uniformly can leave threads idle or concentrate high-degree work on a few threads.",
    "uncertainty": "The three-phase characterization is tied to the evaluated hybrid BFS workloads."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-032",
      "SP-033"
    ],
    "text": "Partition assignments are regenerated for bottom-up levels, while next-frontier and visited bitmaps are updated level by level.",
    "uncertainty": "Partition construction cost is not isolated."
  },
  "related_pattern_ids": [
    "PAT-RELABEL-VERTICES-FOR-LOCALITY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-031",
      "SP-032"
    ],
    "text": "CSR, predecessor state, visited state, frontier state, partition descriptors, and a shared next-partition counter remain resident during traversal.",
    "uncertainty": "Thread-local frontier-copy state is described but not byte-counted."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure memory bytes and storage I/O separately if applied to a mapped or disk-backed graph.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Only in-memory cache behavior is evaluated."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure any persisted phase parameters or partition metadata.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "No persistent scheduler metadata size is reported."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure schedule-calibration and partition-setup time apart from graph reordering.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The scheduling card depends on reordered degree structure but does not isolate that separate preparation term."
    },
    "ram": {
      "assumptions": [],
      "expression": "CSR and predecessor arrays plus three vertex-length bitmaps in bottom-up mode, partition descriptors, and per-thread traversal state.",
      "measurement_needed": "Measure retained and peak RSS by BFS phase.",
      "premises": [],
      "source_pointer_ids": [
        "SP-032"
      ],
      "status": "SOURCED",
      "uncertainty": "Exact bytes depend on vertex identifiers, predecessor width, and implementation."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "Bottom-up execution creates lambda times thread-count partitions and uses a shared next-partition counter; top-down execution uses frontier and thread-local work state.",
      "measurement_needed": "Measure peak partition, queue, and frontier scratch bytes.",
      "premises": [],
      "source_pointer_ids": [
        "SP-031",
        "SP-032",
        "SP-034"
      ],
      "status": "SOURCED",
      "uncertainty": "Descriptor and thread-local byte sizes are not provided."
    }
  },
  "source_domain": "multicore hybrid breadth-first search",
  "source_paper_ids": [
    "PAPER-2012.10026"
  ],
  "source_pointers": [
    {
      "claim_scope": "Top-down edge-block distribution",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 4 and Section 3.3 Phase 1",
      "page": 5,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "SP-031"
    },
    {
      "claim_scope": "Bitmap state, descending partitions, work stealing, and late top-down phase",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 5 and Section 3.3 Phases 2-3",
      "page": 6,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "SP-032"
    },
    {
      "claim_scope": "Bottom-up scheduling with workload reduction",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 6",
      "page": 7,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "SP-033"
    },
    {
      "claim_scope": "Partition-factor overhead and scale-sensitive tuning",
      "locator_type": "SECTION",
      "locator_value": "Section 3.6, Parameter Tuning",
      "page": 8,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "SP-034"
    },
    {
      "claim_scope": "Measured strong scaling",
      "locator_type": "TABLE",
      "locator_value": "Table 3",
      "page": 9,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "SP-035"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-031",
      "SP-032",
      "SP-033"
    ],
    "text": "Threads scan assigned adjacency ranges in top-down phases and assigned contiguous vertex partitions in bottom-up phases.",
    "uncertainty": "This is memory traffic, not external-storage streaming."
  },
  "unknown_when": [
    {
      "assumptions": [
        "No uncited section of the fully read paper resolves the named boundary."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The cited source pointers delimit the mechanism, evaluated conditions, or stated analysis."
      ],
      "source_pointer_ids": [
        "SP-032",
        "SP-034"
      ],
      "text": "The paper does not evaluate deterministic scheduling, NUMA placement, directed graphs, or a hard upper bound on work-stealing contention.",
      "uncertainty": "These unknowns matter for an enforceable parallel resource contract."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-031",
        "SP-032",
        "SP-035"
      ],
      "text": "Frontiers exhibit the source's small-high-degree, large-bottom-up, and small-low-degree phases, and the shared-memory runtime supports atomic bitmap updates and work stealing.",
      "uncertainty": "Benefit depends on thread count and degree distribution."
    }
  ]
}
```
