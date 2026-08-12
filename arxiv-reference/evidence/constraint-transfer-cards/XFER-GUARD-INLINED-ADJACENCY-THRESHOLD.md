# Guard Inlined Adjacency Threshold

```json
{
  "analogy_failure_modes": [
    {
      "assumptions": [
        "Topology mutation is observable."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-INLINE-LOW-DEGREE-ADJACENCIES unknown_when",
        "The proposed transfer relies on exact reconstruction metadata."
      ],
      "text": "The analogy fails when the graph mutates during the representation epoch because degree bands, virtual boundaries, offsets, and identifier maps may become stale.",
      "uncertainty": "No incremental-maintenance invariant or cost is established."
    },
    {
      "assumptions": [
        "A non-inlined exact representation is available or the run is refused."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "FAIL-INLINE-THRESHOLD-INFLATES-MEMORY breakpoint_equation",
        "PAT-INLINE-LOW-DEGREE-ADJACENCIES fails_when"
      ],
      "text": "The analogy fails when low-degree mass, metadata discontinuities, fragmentation, or algorithm state causes every threshold to exceed the hard RAM budget.",
      "uncertainty": "Portable byte coefficients are unmeasured."
    }
  ],
  "epistemic_label": "SPECULATIVE_TRANSFER",
  "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-GUARD-INLINED-ADJACENCY-THRESHOLD",
  "g06_challenges": [
    {
      "applies": true,
      "failure_id": "FAIL-INLINE-THRESHOLD-INFLATES-MEMORY",
      "response": "Applies. The transfer computes metadata and mini-adjacency bytes separately for every admitted threshold, includes algorithm and allocator state in peak RAM, and falls back to non-inlined block storage or refuses when no threshold remains within budget."
    }
  ],
  "modern_knight_bus_constraints": [
    {
      "assumptions": [
        "A degree histogram and representation byte widths are available."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "FAIL-INLINE-THRESHOLD-INFLATES-MEMORY repair_options",
        "PAT-INLINE-LOW-DEGREE-ADJACENCIES a007_consequence"
      ],
      "text": "Admission must sweep candidate thresholds symbolically, reject any threshold whose complete RAM expression exceeds budget, and retain a non-inlined large-list representation when no guarded threshold survives.",
      "uncertainty": "Fragmentation and allocator constants are not known before calibration."
    },
    {
      "assumptions": [
        "The receipt records the exact representation version."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "A007 receipt contract",
        "FAIL-INLINE-THRESHOLD-INFLATES-MEMORY expected_failure_signal"
      ],
      "text": "The receipt must expose the selected threshold, degree histogram checksum, mini-adjacency bytes, metadata bytes, fragmentation, large-list block reads, algorithm-state bytes, and peak charged memory.",
      "uncertainty": "Attribution of shared cache pages may be approximate."
    }
  ],
  "modern_resource_model": {
    "concurrency": {
      "assumptions": [
        "Worker and request caps are enforced.",
        "Shared buffers are charged once and separately."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Concurrency_state_bytes = W_workers * (Q_blocks_per_worker * P_block_bytes + B_frontier_worker_bytes) + c_scheduler_state_bytes",
      "measurement_needed": "Measure worker-local buffers, queue reservations, synchronization metadata, and scheduler allocation.",
      "uncertainty": "Asynchronous block execution may add library and queue state outside explicit worker buffers.",
      "unknown_constants": [
        "c_scheduler_state_bytes"
      ],
      "variables": [
        {
          "definition": "worker and in-flight block state",
          "symbol": "Concurrency_state_bytes",
          "units": "bytes"
        },
        {
          "definition": "workers",
          "symbol": "W_workers",
          "units": "workers"
        },
        {
          "definition": "bounded in-flight block requests per worker",
          "symbol": "Q_blocks_per_worker",
          "units": "requests per worker"
        },
        {
          "definition": "buffer reservation per block request",
          "symbol": "P_block_bytes",
          "units": "bytes per request"
        },
        {
          "definition": "per-worker active-state buffer",
          "symbol": "B_frontier_worker_bytes",
          "units": "bytes per worker"
        }
      ]
    },
    "io": {
      "assumptions": [
        "Mini-list accesses require no storage-block read.",
        "Cache state is declared."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "IO_total_bytes = c_physical_read_amplification * SUM_d_gt_theta(A_degree[d] * K_blocks_per_adjacency[d] * P_block_bytes)",
      "measurement_needed": "Count activated degree classes, logical block requests, physical bytes, cache hits, and read amplification per algorithm.",
      "uncertainty": "Future activation by degree class cannot be inferred from the static degree histogram alone.",
      "unknown_constants": [
        "c_physical_read_amplification"
      ],
      "variables": [
        {
          "definition": "physical large-list read bytes",
          "symbol": "IO_total_bytes",
          "units": "bytes"
        },
        {
          "definition": "inline degree threshold",
          "symbol": "theta",
          "units": "degree"
        },
        {
          "definition": "degree-class index",
          "symbol": "d",
          "units": "degree"
        },
        {
          "definition": "activated vertices indexed by degree",
          "symbol": "A_degree",
          "units": "vertices"
        },
        {
          "definition": "blocks touched per activated adjacency list",
          "symbol": "K_blocks_per_adjacency",
          "units": "blocks per vertex"
        },
        {
          "definition": "storage block bytes",
          "symbol": "P_block_bytes",
          "units": "bytes per block"
        }
      ]
    },
    "preprocessing": {
      "assumptions": [
        "The source graph artifact is held constant.",
        "Preparation includes all candidate-threshold-dependent rewrites."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "T_prepare = c_partition_time_per_edge * M_edges + c_reorder_time_per_vertex_log_vertex * N_vertices * log2(N_vertices + 1) + c_virtual_time_per_boundary * V_virtual + c_idmap_time_per_vertex * N_vertices",
      "measurement_needed": "Measure partitioning, reordering, virtual insertion, degree-band construction, and identifier-map writing separately.",
      "uncertainty": "Parallel partitioning, external sort, and fragmentation can alter preparation coefficients.",
      "unknown_constants": [
        "c_idmap_time_per_vertex",
        "c_partition_time_per_edge",
        "c_reorder_time_per_vertex_log_vertex",
        "c_virtual_time_per_boundary"
      ],
      "variables": [
        {
          "definition": "layout preparation time",
          "symbol": "T_prepare",
          "units": "time"
        },
        {
          "definition": "graph edges",
          "symbol": "M_edges",
          "units": "edges"
        },
        {
          "definition": "graph vertices",
          "symbol": "N_vertices",
          "units": "vertices"
        },
        {
          "definition": "inserted virtual boundaries",
          "symbol": "V_virtual",
          "units": "virtual vertices"
        }
      ]
    },
    "ram": {
      "assumptions": [
        "Every inlined adjacency identifier is counted.",
        "Virtual and degree-band metadata is included in B_metadata_bytes(theta)."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "RAM_peak_bytes = B_fixed_bytes + B_metadata_bytes(theta) + b_id_bytes*SUM_d_le_theta(H_degree[d]*d) + B_algorithm_state_bytes + B_large_block_cache_peak + W_workers*(Q_blocks_per_worker*P_block_bytes + B_frontier_worker_bytes) + c_scheduler_state_bytes + c_allocator_overhead_bytes",
      "measurement_needed": "Measure fixed, metadata, mini-adjacency, algorithm, large-block resident pages, detailed per-worker frontier/request buffers, scheduler state, allocator, and charged-cache components for each threshold.",
      "uncertainty": "Alignment and metadata-layout changes can make B_metadata_bytes discontinuous in theta.",
      "unknown_constants": [
        "c_allocator_overhead_bytes",
        "c_scheduler_state_bytes"
      ],
      "variables": [
        {
          "definition": "Large adjacency pages physically resident at peak, excluding worker request buffers",
          "symbol": "B_large_block_cache_peak",
          "units": "bytes"
        },
        {
          "definition": "maximum charged resident bytes",
          "symbol": "RAM_peak_bytes",
          "units": "bytes"
        },
        {
          "definition": "index, vertex state, and fixed layout bytes",
          "symbol": "B_fixed_bytes",
          "units": "bytes"
        },
        {
          "definition": "threshold-dependent virtual, block, and degree-band metadata",
          "symbol": "B_metadata_bytes",
          "units": "bytes"
        },
        {
          "definition": "candidate inline degree threshold",
          "symbol": "theta",
          "units": "degree"
        },
        {
          "definition": "degree-class index",
          "symbol": "d",
          "units": "degree"
        },
        {
          "definition": "vertex counts indexed by degree",
          "symbol": "H_degree",
          "units": "vertices"
        },
        {
          "definition": "bytes per stored adjacent identifier",
          "symbol": "b_id_bytes",
          "units": "bytes per identifier"
        },
        {
          "definition": "algorithm-specific resident state",
          "symbol": "B_algorithm_state_bytes",
          "units": "bytes"
        },
        {
          "definition": "concurrent workers",
          "symbol": "W_workers",
          "units": "workers"
        },
        {
          "definition": "per-worker frontier state excluding block request buffers",
          "symbol": "B_frontier_worker_bytes",
          "units": "bytes per worker"
        },
        {
          "definition": "bounded in-flight large-block requests per worker",
          "symbol": "Q_blocks_per_worker",
          "units": "requests per worker"
        },
        {
          "definition": "buffer reservation per in-flight block request",
          "symbol": "P_block_bytes",
          "units": "bytes per request"
        }
      ]
    },
    "storage": {
      "assumptions": [
        "Inlined adjacency bytes are counted in RAM, not silently omitted from total footprint.",
        "Identifier-map retention policy is declared."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Storage_total_bytes = B_large_blocks_bytes(theta) + B_id_map_bytes + B_layout_metadata_bytes + c_block_fragmentation_bytes + c_filesystem_overhead_bytes",
      "measurement_needed": "Measure block payload, unused block capacity, identifier map, metadata, and filesystem allocation per threshold.",
      "uncertainty": "Block packing and filesystem allocation are graph- and implementation-dependent.",
      "unknown_constants": [
        "c_block_fragmentation_bytes",
        "c_filesystem_overhead_bytes"
      ],
      "variables": [
        {
          "definition": "complete persistent layout bytes",
          "symbol": "Storage_total_bytes",
          "units": "bytes"
        },
        {
          "definition": "inline degree threshold",
          "symbol": "theta",
          "units": "degree"
        },
        {
          "definition": "non-inlined adjacency block payload",
          "symbol": "B_large_blocks_bytes",
          "units": "bytes"
        },
        {
          "definition": "old-to-new identifier relation",
          "symbol": "B_id_map_bytes",
          "units": "bytes"
        },
        {
          "definition": "block, virtual-boundary, and degree-band metadata",
          "symbol": "B_layout_metadata_bytes",
          "units": "bytes"
        }
      ]
    }
  },
  "name": "Guard Inlined Adjacency Threshold",
  "original_constraint_profile": {
    "access_medium": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-INLINE-LOW-DEGREE-ADJACENCIES"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "text": "The index, vertex state, low-degree mini-adjacency data, degree-band boundaries, and block metadata reside in RAM while larger adjacency lists remain in SSD blocks.",
      "uncertainty": "Actual block residency and buffer-pool behavior vary during execution."
    },
    "communication_model": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-INLINE-LOW-DEGREE-ADJACENCIES"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "Mini adjacency is accessed directly in RAM, while large adjacency is transferred through the block-centric SSD execution path.",
      "uncertainty": "The extracted storage mechanism does not isolate synchronization or distributed exchange."
    },
    "constrained_resource": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-INLINE-LOW-DEGREE-ADJACENCIES"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003",
        "SP-004",
        "SP-005"
      ],
      "text": "The source mechanism trades resident metadata and low-degree adjacency bytes against SSD block reads for larger adjacency lists.",
      "uncertainty": "The threshold balance is layout-, graph-, and algorithm-dependent."
    },
    "data_mutability": {
      "assumptions": [
        "No publication-year inference or unstated source assumption is used."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The mechanism card describes a preprocessed reordered graph and does not establish online topology-update correctness or maintenance cost."
      ],
      "text": "UNKNOWN",
      "uncertainty": "Virtual-vertex, degree-band, and old-to-new identifier structures may require rebuild after mutation."
    },
    "original_hardware_operating_assumptions": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-INLINE-LOW-DEGREE-ADJACENCIES"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002",
        "SP-003"
      ],
      "text": "The mechanism assumes a hybrid RAM and SSD hierarchy, bounded storage blocks, compact identifiers, and a marker convention that keeps inserted virtual vertices unreachable.",
      "uncertainty": "Concrete block size, metadata alignment, and memory hierarchy costs are source-implementation conditions."
    },
    "predictability_requirement": {
      "assumptions": [
        "No publication-year inference or unstated source assumption is used."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The mechanism card states no source-side hard-budget or completion-predictability contract for choosing the degree threshold."
      ],
      "text": "UNKNOWN",
      "uncertainty": "The source reports threshold sensitivity but not a portable admission equation."
    }
  },
  "original_constraints": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-INLINE-LOW-DEGREE-ADJACENCIES"
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "text": "Vertex reordering, virtual boundaries, and degree-band boundaries must preserve exact degree and adjacency reconstruction for both large and mini vertices.",
      "uncertainty": "Virtual vertices must remain unreachable for correctness."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-INLINE-LOW-DEGREE-ADJACENCIES"
      ],
      "source_pointer_ids": [
        "SP-005"
      ],
      "text": "Thresholds below the compact-metadata regime enlarge metadata, while larger thresholds eventually make resident mini-adjacency data dominate memory.",
      "uncertainty": "The source-selected balance is not portable."
    }
  ],
  "original_cost_model": {
    "assumptions": [
      "The degree histogram is available before threshold selection."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-INLINE-LOW-DEGREE-ADJACENCIES resource_model.ram",
      "PAT-INLINE-LOW-DEGREE-ADJACENCIES resource_model.io",
      "FAIL-INLINE-THRESHOLD-INFLATES-MEMORY breakpoint_equation"
    ],
    "text": "For threshold theta, resident cost consists of fixed index and vertex state, threshold-dependent metadata, degree-band boundaries, and identifiers for all inlined adjacencies; I/O consists of activated large-list block reads plus fragmentation.",
    "uncertainty": "Portable byte widths, allocator overhead, activation distribution, and preprocessing high-water are unknown."
  },
  "original_domain": "hybrid in-memory and SSD graph storage",
  "proposed_transfer": {
    "assumptions": [
      "Topology is immutable during the admitted representation epoch."
    ],
    "claim_type": "SPECULATIVE_TRANSFER",
    "premises": [
      "PAT-INLINE-LOW-DEGREE-ADJACENCIES invariant",
      "FAIL-INLINE-THRESHOLD-INFLATES-MEMORY breakpoint_equation",
      "A007 fit/spill/refuse contract"
    ],
    "text": "Use degree-banded adjacency inlining only as a histogram-driven placement branch: derive a threshold-specific RAM and I/O bound, preserve exact adjacency reconstruction, fall back to block-resident lists when the bound fails, and receipt both sides of the threshold trade-off.",
    "uncertainty": "The transfer does not establish one threshold, one metadata encoding, or lower runtime."
  },
  "reversed_assumptions": [
    {
      "assumptions": [
        "The graph image is static for the admitted run."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "PAT-INLINE-LOW-DEGREE-ADJACENCIES fails_when",
        "FAIL-INLINE-THRESHOLD-INFLATES-MEMORY breakpoint_equation"
      ],
      "text": "A source-default degree threshold is no longer accepted; threshold selection is recalculated from the admitted graph's degree histogram, byte widths, block layout, algorithm state, and hard RAM budget.",
      "uncertainty": "Future activation by degree class remains workload dependent."
    },
    {
      "assumptions": [
        "Correctness is compared to the original adjacency image."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "PAT-INLINE-LOW-DEGREE-ADJACENCIES a007_consequence",
        "A007 full-working-set requirement"
      ],
      "text": "Avoided block reads are no longer assumed to justify inlining unless the estimate includes the complete resident mini-data, metadata, allocator, buffer, and algorithm-state terms.",
      "uncertainty": "Physical read savings and peak RSS coefficients require measurement."
    }
  ],
  "smallest_falsifier": {
    "controlled_variables": [
      "algorithm",
      "block_size",
      "degree_histogram",
      "thread_count",
      "vertex_ordering",
      "cache state",
      "in-flight block request cap"
    ],
    "failure_signal": "Any threshold chosen by the guard reconstructs an incorrect degree or adjacency, reaches a virtual vertex, or exceeds the declared complete RAM bound.",
    "fixture": "A small graph with controllable counts from degree zero through above-threshold degrees, forced block fragmentation, and exact original adjacency lists, built across a threshold sweep.",
    "independent_oracle": "Original adjacency lists and exact traversal output, plus large-block resident-page, per-worker request/frontier, scheduler, allocation, and whole-process charged-memory counters."
  },
  "source_pattern_ids": [
    "PAT-INLINE-LOW-DEGREE-ADJACENCIES"
  ],
  "surviving_invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-INLINE-LOW-DEGREE-ADJACENCIES"
    ],
    "source_pointer_ids": [
      "SP-003"
    ],
    "text": "A reordered graph can recover large-vertex degree from consecutive offsets and recover mini-vertex degree and mini-data offset from contiguous degree bands without an explicit per-vertex degree field.",
    "uncertainty": "Correctness depends on exact reordering metadata and unreachable virtual vertices."
  },
  "target_algorithm_families": [
    "BFS_SHORTEST_PATHS",
    "K_CORE",
    "PAGERANK_CENTRALITY",
    "PERSONALIZED_PAGERANK",
    "WCC_CONNECTED_COMPONENTS"
  ],
  "transfer_id": "XFER-GUARD-INLINED-ADJACENCY-THRESHOLD",
  "unknown_measurement_constants": [
    "c_allocator_overhead_bytes",
    "c_block_fragmentation_bytes",
    "c_filesystem_overhead_bytes",
    "c_idmap_time_per_vertex",
    "c_partition_time_per_edge",
    "c_physical_read_amplification",
    "c_reorder_time_per_vertex_log_vertex",
    "c_scheduler_state_bytes",
    "c_virtual_time_per_boundary"
  ]
}
```
