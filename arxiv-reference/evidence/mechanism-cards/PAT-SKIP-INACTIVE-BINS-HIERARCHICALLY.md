# Skip Inactive Bins Hierarchically

- Pattern ID: `PAT-SKIP-INACTIVE-BINS-HIERARCHICALLY`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `D_THEORETICAL_OR_INCOMPLETE`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus partitions communication into identifiable source/destination channels",
      "The planner has a conservative estimate of active partition-pair density"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-SKIP-INACTIVE-BINS-HIERARCHICALLY: unfiltered gather may probe every partition pair",
      "PAT-SKIP-INACTIVE-BINS-HIERARCHICALLY: the hierarchy stores only active destinations and bins"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "For sparse-frontier Knight Bus jobs, admission should estimate active partition-pair metadata and schedule only nonempty channels; it must still reserve a dense-case upper bound or refuse when the activity pattern cannot be bounded.",
    "uncertainty": "The source does not provide list-byte coefficients, contention costs, or out-of-core activity metadata behavior."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Insert activity metadata when a scatter message first targets a bin; during gather, schedule only listed destination partitions and scan only their listed source bins.",
    "uncertainty": "The source does not specify contention behavior for concurrent list insertion."
  },
  "confidence_rationale": {
    "assumptions": [
      "The implementation inserts each active channel without expensive contention or duplication"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Section 3.1.3 specifies the two-level lists",
      "Later discussion attributes skipped inactive partitions to the list"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-003"
    ],
    "text": "Confidence is moderate-low because the source clearly specifies the hierarchy and its avoided quadratic probe pattern, but provides no isolated benchmark or byte accounting for this component.",
    "uncertainty": "No campaign reproduction, code inspection, or isolated ablation exists."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "A top-level gPartList identifies active destination columns of the partition-bin matrix, while binPartList entries identify active source rows within each selected column.",
    "uncertainty": "Concrete list representation and duplicate suppression are implementation-specific."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "D_THEORETICAL_OR_INCOMPLETE",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "text": "Without activity lists, cache-capped partitioning can make all-bin probing theoretically inefficient as partition count scales with graph size.",
      "uncertainty": "This is the broken-assumption boundary the mechanism repairs."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-SKIP-INACTIVE-BINS-HIERARCHICALLY",
  "falsifying_test": {
    "controlled_variables": [
      "partition count",
      "active partition-pair count",
      "message count",
      "thread count",
      "list representation"
    ],
    "failure_signal": "The hierarchy misses or duplicates a nonempty bin, or sparse-case list work does not remain below exhaustive probing under the declared representation",
    "fixture": "A partitioned graph with one frontier activating a single partition pair and another activating every partition pair",
    "independent_oracle": "A direct exhaustive bin scan that records the same gathered messages",
    "scope": "Smallest activity-index correctness/work falsifier only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Every nonempty destination partition is listed globally, and every nonempty source bin for that destination is listed locally, so gather visits all and only communication channels that received at least one message.",
    "uncertainty": "Correctness requires duplicate handling or idempotent insertion semantics that the paper does not detail."
  },
  "knight_bus_algorithm_families": [
    "BFS_SHORTEST_PATHS",
    "WCC_CONNECTED_COMPONENTS",
    "SEEDED_RANDOM_WALK"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Populate a global active-destination partition list and one active-source-bin list per destination during scatter, then use the global list as the parallel gather task queue and the local list to enumerate bins.",
    "uncertainty": "List maintenance overhead is not isolated experimentally."
  },
  "name": "Skip Inactive Bins Hierarchically",
  "pattern_id": "PAT-SKIP-INACTIVE-BINS-HIERARCHICALLY",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "If gather probes every partition-pair bin without knowing which bins received messages, it performs at least quadratic work in the partition count even when useful work is very small.",
    "uncertainty": "The practical impact depends on partition count and frontier sparsity."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Both hierarchy levels are rebuilt from the current iteration's scatter activity, and gather produces the partition/frontier list for the next iteration.",
    "uncertainty": "Reset and allocation policy are not described."
  },
  "related_pattern_ids": [
    "PAT-STREAM-PARTITIONED-UPDATES-SEQUENTIALLY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "The current global active-partition list and per-destination active-bin lists remain resident for the iteration.",
    "uncertainty": "Peak list capacity in bytes is not reported."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Compare bin probes and message bytes with and without the hierarchy at matched frontiers.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper establishes avoided empty-bin probes but does not give an isolated byte or operation count for the hierarchical lists."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Verify whether any activity metadata is persisted across runs and measure it if present.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The mechanism is described as iteration state; no persistent-storage consequence is bounded."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "No graph rewrite is specified for this mechanism; activity lists are populated during each scatter phase.",
      "measurement_needed": "Measure one-time list allocation and initialization separately from per-iteration rebuild cost.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "SOURCED",
      "uncertainty": "Framework initialization of list capacity is not described."
    },
    "ram": {
      "assumptions": [
        "Each active partition and active bin is represented once per iteration"
      ],
      "expression": "Resident list state is proportional to the active destination partitions plus their listed nonempty source bins for the current iteration.",
      "measurement_needed": "Measure list capacity, unique entries, duplicate attempts, and allocator bytes per iteration.",
      "premises": [
        "gPartList stores active destination partitions",
        "binPartList stores active source bins for each destination"
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "DERIVED",
      "uncertainty": "Entry width, duplicate suppression, allocator overhead, and concurrent construction are unspecified."
    },
    "temporary_storage": {
      "assumptions": [
        "Lists are not retained beyond the iteration except for allocated capacity"
      ],
      "expression": "The temporary hierarchy contains the current gPartList and all current binPartList entries until gather completes.",
      "measurement_needed": "Measure peak logical and allocated bytes across sparse, medium, and dense frontier phases.",
      "premises": [
        "Both list levels are built from current scatter messages",
        "Gather consumes these lists before constructing next-iteration activity"
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "DERIVED",
      "uncertainty": "Peak byte size depends on active partition-pair density and representation."
    }
  },
  "source_domain": "sparse-frontier partitioned graph scheduling",
  "source_paper_ids": [
    "PAPER-1806.08092"
  ],
  "source_pointers": [
    {
      "claim_scope": "Quadratic empty-bin probing problem and global/local active-list mechanism",
      "locator_type": "SECTION",
      "locator_value": "Section 3.1.3",
      "page": 8,
      "paper_id": "PAPER-1806.08092",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Two-level list preventing scatter of partitions with no active vertices in PC mode",
      "locator_type": "FIGURE",
      "locator_value": "Figure 7 setup discussion, final paragraph",
      "page": 20,
      "paper_id": "PAPER-1806.08092",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Work-efficiency rationale when partition count grows with graph size",
      "locator_type": "SECTION",
      "locator_value": "Section 7, paragraph on cache-capped partitions and two-level Active List",
      "page": 22,
      "paper_id": "PAPER-1806.08092",
      "pointer_id": "SP-003"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Gather streams messages only from bins named by the hierarchical activity lists.",
    "uncertainty": "Message-byte savings are not isolated from other GPOP optimizations."
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
        "SP-001"
      ],
      "text": "The source does not isolate behavior when nearly every partition pair is active and list insertion becomes dense or contentious.",
      "uncertainty": "Dense-list overhead may approach or exceed a direct scan but is not measured separately."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "Only a small subset of partition pairs receives messages, so enumerating nonempty channels avoids many empty probes.",
      "uncertainty": "No source threshold defines when sparsity outweighs list-maintenance overhead."
    }
  ]
}
```
