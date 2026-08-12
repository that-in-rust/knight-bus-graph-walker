# Stream Partitioned Updates Sequentially

- Pattern ID: `PAT-STREAM-PARTITIONED-UPDATES-SEQUENTIALLY`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus can build or import a stable partitioned connectivity layout",
      "The admission planner knows vertex-state width, partition count, thread count, frontier profile, and expected iterations"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-STREAM-PARTITIONED-UPDATES-SEQUENTIALLY: correctness and locality depend on cache-fit partitions and disjoint bin ownership",
      "PAT-STREAM-PARTITIONED-UPDATES-SEQUENTIALLY: PNG and destination-ID streams add preprocessing and persistent state"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-005",
      "SP-006"
    ],
    "text": "A Knight Bus partition-centric plan would need admission terms for cache-resident vertex state, persistent connectivity layout, update-bin capacity, frontier density, and preprocessing amortization; input graph bytes alone cannot represent its working set.",
    "uncertainty": "The source does not quantify whole-process RSS, out-of-core bin behavior, or Knight Bus topology distributions."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003",
      "SP-005"
    ],
    "text": "Scatter one source partition at a time in destination-partition order, then gather all incoming bins for one destination partition while its vertex state is cached; reuse destination-ID streams across iterations and write only changing update values.",
    "uncertainty": "This schedule is most efficient when enough vertices are active to justify partition-centric traversal."
  },
  "confidence_rationale": {
    "assumptions": [
      "The reported implementations preserve the described ownership and layout rules"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "GPOP and PCPM independently document cache-fit partition processing and sequential bins",
      "PCPM supplies explicit analytical and benchmark support"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-005",
      "SP-006",
      "SP-007"
    ],
    "text": "Confidence is moderate because two assigned papers describe the same partition/bin mechanism, one supplies an analytical communication model and both report source benchmarks, but no implementation was inspected or rerun in this campaign.",
    "uncertainty": "Evidence remains source-reported and is strongest for shared-memory PageRank-like workloads."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-004",
      "SP-005"
    ],
    "text": "Vertex IDs map to disjoint cache-sized partitions; partition-pair bins separate update values from memoized destination IDs; the PNG representation transposes compressed node-to-partition links per source partition so updates to each destination bin are contiguous.",
    "uncertainty": "More sophisticated partitioning can improve locality but adds preprocessing not isolated by this card."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-008"
      ],
      "text": "A partition exceeds cache capacity, causing random vertex-state accesses to spill to slower cache levels or DRAM and reversing the locality gain.",
      "uncertainty": "The reversal point depends on the hardware and concurrent cache pressure."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-003"
      ],
      "text": "Very sparse frontiers make scanning all vertices or out-edges of a partition redundant; GPOP therefore uses a separate source-centric mode for this case.",
      "uncertainty": "The crossover depends on modeled bandwidth and active-edge density."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-STREAM-PARTITIONED-UPDATES-SEQUENTIALLY",
  "falsifying_test": {
    "controlled_variables": [
      "graph and labeling",
      "partition size",
      "vertex-state width",
      "thread count",
      "iteration count",
      "frontier density"
    ],
    "failure_signal": "Results diverge from the exact oracle, disjoint ownership is violated, or partitioned execution fails to reduce random accesses when the stated cache-fit and locality conditions hold",
    "fixture": "One graph whose source neighborhoods concentrate within partitions and one relabeled graph whose neighbors are dispersed, each run at partition sizes below and above private-cache capacity",
    "independent_oracle": "A straightforward exact push or pull implementation with identical iteration semantics",
    "scope": "Smallest correctness/locality falsifier only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-004"
    ],
    "text": "While one worker owns a partition, that partition's mutable vertex state remains cache-resident and all messages it emits or consumes use disjoint bin regions, so updates require neither locks nor atomic operations.",
    "uncertainty": "The invariant depends on choosing a partition that fits the intended private cache and preserving exclusive ownership."
  },
  "knight_bus_algorithm_families": [
    "BFS_SHORTEST_PATHS",
    "WCC_CONNECTED_COMPONENTS",
    "PAGERANK_CENTRALITY",
    "SEEDED_RANDOM_WALK",
    "SPARSE_MATRIX_VECTOR"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003",
      "SP-004",
      "SP-005"
    ],
    "text": "Partition vertices into cacheable ranges, aggregate a source vertex's update once per neighboring partition, stream partition-grouped updates through fixed bins, and gather one destination partition at a time under exclusive ownership.",
    "uncertainty": "GPOP generalizes the mechanism to dynamic frontiers, while the PCPM benchmark is centered on PageRank."
  },
  "name": "Stream Partitioned Updates Sequentially",
  "pattern_id": "PAT-STREAM-PARTITIONED-UPDATES-SEQUENTIALLY",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-004"
    ],
    "text": "Fine-grained graph traversal causes irregular vertex-state accesses, redundant per-edge update propagation, poor cache-line use, and synchronization when multiple threads update the same destinations.",
    "uncertainty": "The bottleneck magnitude depends on graph locality, frontier density, and memory hierarchy."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-004"
    ],
    "text": "Only iteration-dependent update values and resulting vertex values are regenerated; destination-ID placement and PNG connectivity are reused.",
    "uncertainty": "Algorithms with changing topology would invalidate the reused connectivity layout."
  },
  "related_pattern_ids": [
    "PAT-INTERLEAVE-PARTITION-UPDATES-SAFELY",
    "PAT-PARTITION-UPDATES-BY-DESTINATION",
    "PAT-SELECT-PARTITION-SCATTER-MODE",
    "PAT-SKIP-INACTIVE-BINS-HIERARCHICALLY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-004"
    ],
    "text": "One partition's vertex values or partial sums, bin insertion metadata, and worker-local scheduling state are resident during processing.",
    "uncertainty": "The papers do not report peak whole-process RSS including every bin and framework allocation."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "For PCPM PageRank, Equation 5 gives per-iteration DRAM traffic as m * (di * (1 + 1/r) + 2dv/r) + k^2 * di + 2n * dv.",
      "measurement_needed": "Measure DRAM bytes, cache misses, and persistent-device bytes separately for each target algorithm and frontier profile.",
      "premises": [],
      "source_pointer_ids": [
        "SP-006"
      ],
      "status": "SOURCED",
      "uncertainty": "The expression is for the paper's PageRank/PNG model and variable definitions; it is not a storage-device I/O bound or a general formula for every GPOP algorithm."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "Persistent auxiliary state includes compressed node-to-partition PNG edges, per-partition offset arrays, and memoized destination-ID streams; update values remain iteration-dependent.",
      "measurement_needed": "Measure original graph, PNG, offsets, destination IDs, and filesystem metadata as separate persistent byte totals.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "The papers give structural terms but no complete byte total including original graph retention and metadata."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "For each partition, scan outgoing edges to count compressed node-to-partition links and compute offsets, then scan again to fill the transposed PNG; destination-ID bins are written once for reuse.",
      "measurement_needed": "Measure two-pass layout-build wall time, bytes read/written, and amortization across the expected iteration count.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "Preprocessing time varies with input layout, partition count, and parallel construction implementation."
    },
    "ram": {
      "assumptions": [],
      "expression": "Each partition's vertex data must fit the largest private cache; GPOP also chooses at least four partitions per worker thread for load balancing.",
      "measurement_needed": "Measure whole-process peak RSS and cache occupancy for the declared partition count, thread count, vertex-state width, and bin capacities.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "This is a partition-local condition, not a bound on total RSS or all bins."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak transient bytes by phase for bins, frontier lists, partial sums, and PNG construction scratch.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Neither paper provides a complete peak-byte bound for update bins, partial sums, frontier state, and preprocessing scratch together."
    }
  },
  "source_domain": "shared-memory iterative graph processing and sparse matrix-vector multiplication",
  "source_paper_ids": [
    "PAPER-1709.07122",
    "PAPER-1806.08092"
  ],
  "source_pointers": [
    {
      "claim_scope": "Partition bins, exclusive gather ownership, cache reuse, and sequential message access",
      "locator_type": "SECTION",
      "locator_value": "Section 3, scatter/gather description and Figure 2",
      "page": 5,
      "paper_id": "PAPER-1806.08092",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Cache-sized index partitioning and load-balance condition",
      "locator_type": "SECTION",
      "locator_value": "Section 3.1.1",
      "page": 6,
      "paper_id": "PAPER-1806.08092",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Partition-centric write order, one-time neighbor-ID storage, and lock-free gather",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 3 and Section 3.1.2 continuation",
      "page": 8,
      "paper_id": "PAPER-1806.08092",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Fixed disjoint bin offsets, partition-local vertex state, destination-ID reuse, and node-to-partition aggregation",
      "locator_type": "SECTION",
      "locator_value": "Sections 3.1-3.2",
      "page": 4,
      "paper_id": "PAPER-1709.07122",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Partition-wise transposed PNG layout, two-pass construction, cacheability, and sequential bin streaming",
      "locator_type": "ALGORITHM",
      "locator_value": "Section 3.3 and Algorithm 3",
      "page": 6,
      "paper_id": "PAPER-1709.07122",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "PCPM communication expression, locality condition, and random-access bound",
      "locator_type": "EQUATION",
      "locator_value": "Equations 5, 7, and 10 with Section 4 discussion",
      "page": 8,
      "paper_id": "PAPER-1709.07122",
      "pointer_id": "SP-006"
    },
    {
      "claim_scope": "Source PageRank benchmark for runtime, DRAM traffic, bandwidth, and PNG compression",
      "locator_type": "FIGURE",
      "locator_value": "Figures 7-10 and Table 6",
      "page": 10,
      "paper_id": "PAPER-1709.07122",
      "pointer_id": "SP-007"
    },
    {
      "claim_scope": "Partition-size trade-off between compression and cache misses",
      "locator_type": "FIGURE",
      "locator_value": "Figures 11-13 and Section 5.3.2",
      "page": 11,
      "paper_id": "PAPER-1709.07122",
      "pointer_id": "SP-008"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-005"
    ],
    "text": "Graph/PNG entries, update values, memoized destination identifiers, and gathered messages are consumed in partition-contiguous streams.",
    "uncertainty": "Actual transfer granularity depends on cache-line, bin-buffer, and graph-encoding choices."
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
        "SP-003",
        "SP-008"
      ],
      "text": "The sources do not establish performance under mutable topology, nonuniform large vertex payloads, storage-backed execution, or NUMA-aware placement.",
      "uncertainty": "These environments change the cacheability and ownership premises."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003",
        "SP-008"
      ],
      "text": "Partitions fit the intended private cache and iterative traversal repeatedly reuses fixed connectivity while updating vertex values.",
      "uncertainty": "The optimal partition size is platform-dependent."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-004",
        "SP-006",
        "SP-007"
      ],
      "text": "Graph locality or neighbor concentration allows multiple edges from one source to the same destination partition to share one propagated value.",
      "uncertainty": "Compression ratio depends on topology, labeling, and partition size."
    }
  ]
}
```
