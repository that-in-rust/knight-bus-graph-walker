# Probe Split Navigation Vectors

```json
{
  "analogy_failure_modes": [
    {
      "assumptions": [
        "The transfer remains scoped to NODESIMILARITY_KNN."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS knight_bus_algorithm_families",
        "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS a007_consequence"
      ],
      "text": "The analogy fails outside ANN-like two-stage navigation and exact vector refinement because generic graph algorithms may need topology and values together with no sound pruning oracle.",
      "uncertainty": "No source evidence establishes this mechanism for generic graph values."
    },
    {
      "assumptions": [
        "A coupled exact fallback is retained."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "FAIL-DECOUPLED-LAYOUT-DOUBLES-READS triggering_workload",
        "FAIL-DECOUPLED-LAYOUT-DOUBLES-READS breakpoint_equation"
      ],
      "text": "The analogy fails when weak pruning, compact vectors, or favorable coupled locality makes separate navigation and vector reads exceed the matched-recall coupled path.",
      "uncertainty": "The crossover is target-specific."
    }
  ],
  "epistemic_label": "SPECULATIVE_TRANSFER",
  "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-PROBE-SPLIT-NAVIGATION-VECTORS",
  "g06_challenges": [
    {
      "applies": true,
      "failure_id": "FAIL-DECOUPLED-LAYOUT-DOUBLES-READS",
      "response": "Applies. Layout admission requires a matched-recall, declared-cache probe of navigation and surviving vector reads; when split reads exceed the coupled equivalent, the transfer selects coupled or mixed placement rather than admitting the split layout."
    }
  ],
  "modern_knight_bus_constraints": [
    {
      "assumptions": [
        "The probe itself has a declared bounded sample and cost."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "FAIL-DECOUPLED-LAYOUT-DOUBLES-READS repair_options",
        "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS a007_consequence"
      ],
      "text": "Before selecting a split layout, admission must run or consume a bounded matched-recall probe that estimates navigation reads, surviving vector reads, packing, cache state, and complete in-flight memory; otherwise it keeps a coupled or mixed exact fallback.",
      "uncertainty": "Probe representativeness under query drift is unknown."
    },
    {
      "assumptions": [
        "The exact oracle and recall target are versioned in the manifest."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "A007 receipt contract",
        "FAIL-DECOUPLED-LAYOUT-DOUBLES-READS expected_failure_signal"
      ],
      "text": "The receipt must separate navigation-block reads, vector-block reads, prune decisions, exact refinements, cache hits, matched-recall verification, in-flight buffers, and selected layout.",
      "uncertainty": "Read count may not predict latency under concurrency."
    }
  ],
  "modern_resource_model": {
    "concurrency": {
      "assumptions": [
        "Read queues have enforced maxima.",
        "Shared cache bytes are accounted outside per-worker state."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Concurrency_state_bytes = W_workers*(B_nav_buffer_bytes + B_vector_buffer_bytes + Q_navigation_reads*P_navigation_block_bytes + Q_vector_reads*P_vector_block_bytes + B_worker_queue_bytes) + c_scheduler_state_bytes",
      "measurement_needed": "Measure every worker's navigation buffer, vector buffer, navigation and vector read reservations, queue, completion, and scheduler allocations while sweeping bounded concurrency.",
      "uncertainty": "Device queueing and asynchronous library state may add bytes or contention not visible in read counts.",
      "unknown_constants": [
        "c_scheduler_state_bytes"
      ],
      "variables": [
        {
          "definition": "bounded in-flight query state",
          "symbol": "Concurrency_state_bytes",
          "units": "bytes"
        },
        {
          "definition": "query workers",
          "symbol": "W_workers",
          "units": "workers"
        },
        {
          "definition": "per-worker navigation buffer",
          "symbol": "B_nav_buffer_bytes",
          "units": "bytes per worker"
        },
        {
          "definition": "per-worker vector buffer",
          "symbol": "B_vector_buffer_bytes",
          "units": "bytes per worker"
        },
        {
          "definition": "in-flight navigation reads per worker",
          "symbol": "Q_navigation_reads",
          "units": "reads per worker"
        },
        {
          "definition": "navigation read reservation",
          "symbol": "P_navigation_block_bytes",
          "units": "bytes per read"
        },
        {
          "definition": "in-flight vector reads per worker",
          "symbol": "Q_vector_reads",
          "units": "reads per worker"
        },
        {
          "definition": "vector read reservation",
          "symbol": "P_vector_block_bytes",
          "units": "bytes per read"
        },
        {
          "definition": "candidate and completion queue bytes",
          "symbol": "B_worker_queue_bytes",
          "units": "bytes per worker"
        }
      ]
    },
    "io": {
      "assumptions": [
        "Runs are matched for recall and cache state.",
        "Every exact refinement resolves the correct raw vector."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "IO_total_bytes = c_physical_read_amplification * (R_navigation_blocks * P_navigation_block_bytes + R_vector_blocks * P_vector_block_bytes)",
      "measurement_needed": "Measure separate logical and physical block reads over dimension, page size, recall, pruning, cache, and query-distribution sweeps.",
      "uncertainty": "Pruning survival, cache reuse, and device read amplification are workload dependent.",
      "unknown_constants": [
        "c_physical_read_amplification"
      ],
      "variables": [
        {
          "definition": "physical query I/O",
          "symbol": "IO_total_bytes",
          "units": "bytes"
        },
        {
          "definition": "navigation blocks read",
          "symbol": "R_navigation_blocks",
          "units": "blocks"
        },
        {
          "definition": "bytes per navigation block",
          "symbol": "P_navigation_block_bytes",
          "units": "bytes per block"
        },
        {
          "definition": "vector blocks read after pruning",
          "symbol": "R_vector_blocks",
          "units": "blocks"
        },
        {
          "definition": "bytes per vector block",
          "symbol": "P_vector_block_bytes",
          "units": "bytes per block"
        }
      ]
    },
    "preprocessing": {
      "assumptions": [
        "All layouts use equivalent graph and vector content.",
        "Existing-index conversion cost is included when applicable."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "T_prepare = c_layout_write_time_per_byte * (B_navigation_bytes + B_vector_bytes) + c_pruning_build_time_per_vector * N_vectors",
      "measurement_needed": "Measure coupled, split, and mixed layout build time, rewritten bytes, scratch, and pruning-index construction separately.",
      "uncertainty": "Construction overhead caused solely by separation is not isolated in the source.",
      "unknown_constants": [
        "c_layout_write_time_per_byte",
        "c_pruning_build_time_per_vector"
      ],
      "variables": [
        {
          "definition": "layout and pruning preparation time",
          "symbol": "T_prepare",
          "units": "time"
        },
        {
          "definition": "navigation payload written",
          "symbol": "B_navigation_bytes",
          "units": "bytes"
        },
        {
          "definition": "vector payload written",
          "symbol": "B_vector_bytes",
          "units": "bytes"
        },
        {
          "definition": "indexed vectors",
          "symbol": "N_vectors",
          "units": "vectors"
        }
      ]
    },
    "ram": {
      "assumptions": [
        "Navigation and vector buffers may coexist.",
        "Worker and queue caps are enforced."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "RAM_peak_bytes = B_search_state_bytes + B_pruning_state_bytes + B_cache_charge_bytes + W_workers*(B_nav_buffer_bytes + B_vector_buffer_bytes + Q_navigation_reads*P_navigation_block_bytes + Q_vector_reads*P_vector_block_bytes + B_worker_queue_bytes) + c_scheduler_state_bytes + c_allocator_overhead_bytes",
      "measurement_needed": "Measure shared search/pruning/cache state and every worker's navigation buffer, vector buffer, navigation reads, vector reads, queue, scheduler state, allocator state, and aggregate RSS at matched recall.",
      "uncertainty": "Allocator and cache accounting depend on the runtime and operating system.",
      "unknown_constants": [
        "c_allocator_overhead_bytes",
        "c_scheduler_state_bytes"
      ],
      "variables": [
        {
          "definition": "maximum charged resident bytes",
          "symbol": "RAM_peak_bytes",
          "units": "bytes"
        },
        {
          "definition": "candidate and result queue bytes",
          "symbol": "B_search_state_bytes",
          "units": "bytes"
        },
        {
          "definition": "compressed scoring and pruning bytes",
          "symbol": "B_pruning_state_bytes",
          "units": "bytes"
        },
        {
          "definition": "charged cached index and vector blocks",
          "symbol": "B_cache_charge_bytes",
          "units": "bytes"
        },
        {
          "definition": "concurrent query workers",
          "symbol": "W_workers",
          "units": "workers"
        },
        {
          "definition": "per-worker navigation buffer",
          "symbol": "B_nav_buffer_bytes",
          "units": "bytes per worker"
        },
        {
          "definition": "per-worker vector buffer",
          "symbol": "B_vector_buffer_bytes",
          "units": "bytes per worker"
        },
        {
          "definition": "bounded navigation reads per worker",
          "symbol": "Q_navigation_reads",
          "units": "reads per worker"
        },
        {
          "definition": "reservation per navigation read",
          "symbol": "P_navigation_block_bytes",
          "units": "bytes per read"
        },
        {
          "definition": "bounded vector reads per worker",
          "symbol": "Q_vector_reads",
          "units": "reads per worker"
        },
        {
          "definition": "reservation per vector read",
          "symbol": "P_vector_block_bytes",
          "units": "bytes per read"
        },
        {
          "definition": "candidate and completion queue bytes per worker, excluding B_search_state_bytes",
          "symbol": "B_worker_queue_bytes",
          "units": "bytes per worker"
        }
      ]
    },
    "storage": {
      "assumptions": [
        "The same graph and vector payload are represented.",
        "Replication is counted explicitly if a mixed layout uses it."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Storage_total_bytes = B_navigation_bytes + B_vector_bytes + B_pruning_bytes + B_alignment_waste_bytes + c_filesystem_overhead_bytes",
      "measurement_needed": "Measure all files, directories, alignment waste, and pruning metadata for each candidate layout.",
      "uncertainty": "Block packing and filesystem overhead vary with dimension and implementation.",
      "unknown_constants": [
        "c_filesystem_overhead_bytes"
      ],
      "variables": [
        {
          "definition": "complete index storage",
          "symbol": "Storage_total_bytes",
          "units": "bytes"
        },
        {
          "definition": "navigation block bytes",
          "symbol": "B_navigation_bytes",
          "units": "bytes"
        },
        {
          "definition": "raw vector block bytes",
          "symbol": "B_vector_bytes",
          "units": "bytes"
        },
        {
          "definition": "compressed scoring or pruning metadata",
          "symbol": "B_pruning_bytes",
          "units": "bytes"
        },
        {
          "definition": "unused block and alignment capacity",
          "symbol": "B_alignment_waste_bytes",
          "units": "bytes"
        }
      ]
    }
  },
  "name": "Probe Split Navigation Vectors",
  "original_constraint_profile": {
    "access_medium": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "Compact adjacency lists occupy disk index blocks, raw vectors occupy separate disk data blocks, and query queues plus fetched blocks reside in memory.",
      "uncertainty": "Cache placement and asynchronous execution can alter the effective access path."
    },
    "communication_model": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS"
      ],
      "source_pointer_ids": [
        "SP-002"
      ],
      "text": "Query execution fetches navigation blocks during expansion and fetches separate vector blocks only for candidates surviving pruning and requiring exact refinement.",
      "uncertainty": "Queueing, cache hits, and device parallelism can reorder or overlap these transfers."
    },
    "constrained_resource": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002",
        "SP-003",
        "SP-004"
      ],
      "text": "The source mechanism trades disk-block utilization and vector-read avoidance against the risk of separate navigation and vector reads.",
      "uncertainty": "The crossover depends on dimension, page packing, recall target, and pruning quality."
    },
    "data_mutability": {
      "assumptions": [
        "No publication-year inference or unstated source assumption is used."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The extracted decoupled-layout mechanism does not establish update consistency or rewrite cost for adjacency and vector blocks."
      ],
      "text": "UNKNOWN",
      "uncertainty": "The broader source discusses updates, but the mechanism card does not bind this layout invariant to one update protocol."
    },
    "original_hardware_operating_assumptions": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002",
        "SP-003"
      ],
      "text": "The mechanism assumes page- or block-addressable disk storage, memory-resident search state, and a pruning representation capable of screening candidates before raw-vector fetch.",
      "uncertainty": "Concrete page size, vector width, cache state, and device behavior are source-configuration conditions."
    },
    "predictability_requirement": {
      "assumptions": [
        "No publication-year inference or unstated source assumption is used."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The mechanism card states no source-side hard-budget or completion-predictability requirement for global-layout selection."
      ],
      "text": "UNKNOWN",
      "uncertainty": "The source provides an empirical decision guide rather than an enforceable admission guarantee."
    }
  },
  "original_constraints": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS"
      ],
      "source_pointer_ids": [
        "SP-002"
      ],
      "text": "Decoupling is useful only when compact navigation packing and avoided vector reads outweigh the loss of coupled one-fetch locality.",
      "uncertainty": "Pruning quality controls whether the second block class is avoided."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS"
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-003",
        "SP-004"
      ],
      "text": "Coupled and decoupled layouts must be compared at matched search semantics, including recall target and exact vector refinement.",
      "uncertainty": "The source crossover is evaluation-specific."
    }
  ],
  "original_cost_model": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-004"
    ],
    "text": "A decoupled search reads navigation blocks for expansion and separate vector blocks for unpruned candidates; the useful regime packs more navigation entries per block and skips enough vector blocks to reduce total I/O.",
    "uncertainty": "The source does not give a universal storage, RAM, or construction equation for decoupling alone."
  },
  "original_domain": "disk block layout for high-dimensional graph ANN",
  "proposed_transfer": {
    "assumptions": [
      "Matched-recall comparison is feasible for the admitted query sample."
    ],
    "claim_type": "SPECULATIVE_TRANSFER",
    "premises": [
      "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS invariant",
      "FAIL-DECOUPLED-LAYOUT-DOUBLES-READS breakpoint_equation",
      "A007 fit/spill/approximate/refuse contract"
    ],
    "text": "Expose coupled, split, or mixed navigation/vector placement as a guarded ANN layout branch selected by a target-specific packing and pruning probe, with exact vector refinement preserved and a coupled fallback when separate reads do not satisfy the declared crossover.",
    "uncertainty": "The transfer is limited to ANN-like navigation and does not establish applicability to BFS, WCC, PageRank, or generic property access."
  },
  "reversed_assumptions": [
    {
      "assumptions": [
        "Coupled and decoupled probes use the same graph, vectors, and query set."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS unknown_when",
        "FAIL-DECOUPLED-LAYOUT-DOUBLES-READS breakpoint_equation"
      ],
      "text": "Source-reported dimension guidance is not imported as a modern threshold; layout selection is instead conditioned on target page size, vector width, graph degree, recall contract, cache state, and observed pruning selectivity.",
      "uncertainty": "No portable crossover has been measured."
    },
    {
      "assumptions": [
        "The admitted recall and exact-refinement contract is explicit."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS a007_consequence",
        "A007 full-working-set requirement"
      ],
      "text": "Raw input bytes no longer stand in for the admitted working set because A007 must separately account for navigation blocks, vector blocks, pruning state, search queues, cache charge, and in-flight reads.",
      "uncertainty": "Peak query-state and cache-attribution constants remain unknown."
    }
  ],
  "smallest_falsifier": {
    "controlled_variables": [
      "block_size",
      "cache_state",
      "candidate_policy",
      "graph",
      "queries",
      "recall_target",
      "vectors",
      "worker count",
      "navigation-read cap",
      "vector-read cap"
    ],
    "failure_signal": "The split-layout guard admits a run that loses required exact refinement, violates the recall contract, exceeds the declared memory bound, or performs more navigation-plus-vector reads than the coupled fallback.",
    "fixture": "One deterministic proximity graph and vector set encoded in coupled and split layouts, with low and high vector widths and controllable pruning selectivity.",
    "independent_oracle": "Exact nearest-neighbor sets plus matched-recall coupled traces and attributed navigation-read, vector-read, per-worker queue, cache, and aggregate-RSS traces."
  },
  "source_pattern_ids": [
    "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS"
  ],
  "surviving_invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS"
    ],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Navigation topology can be stored independently from full vectors while preserving candidate expansion, provided every candidate requiring exact scoring can still resolve and fetch its corresponding vector.",
    "uncertainty": "The invariant preserves search access, not recall or I/O benefit by itself."
  },
  "target_algorithm_families": [
    "NODESIMILARITY_KNN"
  ],
  "transfer_id": "XFER-PROBE-SPLIT-NAVIGATION-VECTORS",
  "unknown_measurement_constants": [
    "c_allocator_overhead_bytes",
    "c_filesystem_overhead_bytes",
    "c_layout_write_time_per_byte",
    "c_physical_read_amplification",
    "c_pruning_build_time_per_vector",
    "c_scheduler_state_bytes"
  ]
}
```
