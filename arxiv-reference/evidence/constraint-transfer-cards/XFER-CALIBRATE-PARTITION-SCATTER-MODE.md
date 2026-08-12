# Calibrate Partition Scatter Mode

```json
{
  "analogy_failure_modes": [
    {
      "assumptions": [
        "Knight Bus exposes two semantically equivalent scatter paths."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-SELECT-PARTITION-SCATTER-MODE:invariant"
      ],
      "text": "The analogy fails if the target cannot provide both an active-only path and a partition-streaming path with identical graph-result semantics.",
      "uncertainty": "No target dual-path implementation exists in G07."
    },
    {
      "assumptions": [
        "The target may be storage-backed rather than DRAM-only."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-SELECT-PARTITION-SCATTER-MODE:unknown_when"
      ],
      "text": "The analogy fails if storage latency, page faults, decompression, or write amplification changes the mode ordering so the source DRAM model is no longer predictive.",
      "uncertainty": "The source equations do not model secondary-storage I/O."
    },
    {
      "assumptions": [
        "Both access paths and their metadata fit the declared preparation and storage budgets."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-SELECT-PARTITION-SCATTER-MODE:resource_model",
        "FAIL-LARGE-PARTITIONS-THRASH-CACHE"
      ],
      "text": "The analogy fails if dual-layout build or retention cost exceeds the budget or a partition's random-access state exceeds effective cache without a bounded split or fallback.",
      "uncertainty": "Dual-layout bytes and effective cache are unmeasured."
    }
  ],
  "epistemic_label": "SPECULATIVE_TRANSFER",
  "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-CALIBRATE-PARTITION-SCATTER-MODE",
  "g06_challenges": [
    {
      "applies": true,
      "failure_id": "FAIL-LARGE-PARTITIONS-THRASH-CACHE",
      "response": "Treat effective_cache_bytes as a measured deployment constant, bound each partition_working_set_bytes, and split the partition or fall back to the active-only path when the cache envelope is exceeded. Receipt partition working set, cache misses or proxy counters, and selected mode."
    },
    {
      "applies": true,
      "failure_id": "FAIL-WRONG-SCATTER-MODE-WASTES",
      "response": "Calibrate both forced modes on controlled target fixtures, attach uncertainty to each predicted cost, choose a mode only when the decision is inside the calibrated envelope, and otherwise use a conservative single-mode fallback or refuse. Receipt predicted and observed bytes and time without presenting them as preexisting measurements."
    }
  ],
  "modern_knight_bus_constraints": [
    {
      "assumptions": [
        "Both target modes preserve identical update semantics."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-SELECT-PARTITION-SCATTER-MODE:a007_consequence"
      ],
      "text": "Admission must account for both retained access paths, active frontier state, per-partition counters, bin or stream buffers, output state, and the larger transient requirement of either forced mode.",
      "uncertainty": "Target dual-layout and transient byte coefficients are unmeasured."
    },
    {
      "assumptions": [
        "Partition activity and working-set estimates can be refreshed each iteration."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "FAIL-LARGE-PARTITIONS-THRASH-CACHE:repair_options",
        "FAIL-WRONG-SCATTER-MODE-WASTES:repair_options"
      ],
      "text": "The selector must use target-calibrated cache and storage coefficients, cap partition working sets, and retain forced active-only, forced partition-streaming, and refusal paths when uncertainty is too large.",
      "uncertainty": "No acceptable model-error envelope is defined in G07."
    },
    {
      "assumptions": [
        "Per-partition decisions and counters can be included in a bounded receipt."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "docs_PRD04/A007-spc-founder-interview-prep-v7.md:during execution",
        "PAT-SELECT-PARTITION-SCATTER-MODE:a007_consequence"
      ],
      "text": "The receipt must record active vertices and edges, partition working set, selected mode, modeled and observed bytes, fallback decisions, cache or page-fault indicators, and output checksum.",
      "uncertainty": "Counter availability and attribution vary by platform."
    }
  ],
  "modern_resource_model": {
    "concurrency": {
      "assumptions": [
        "Worker, bin, and per-partition mode state are capped by the plan."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Concurrent_state_bytes = W * b_worker_state + Q_bins * b_bin_state + P_active * b_mode_state + c_bin_runtime",
      "measurement_needed": "Measure worker-local state, nonempty bin state, active partition counters, and runtime queue bytes at each iteration.",
      "uncertainty": "Bin representation and scheduling determine c_bin_runtime.",
      "unknown_constants": [
        "c_bin_runtime"
      ],
      "variables": [
        {
          "definition": "Active partitions with a mode decision",
          "symbol": "P_active",
          "units": "partitions"
        },
        {
          "definition": "Live bin or stream descriptors",
          "symbol": "Q_bins",
          "units": "descriptors"
        },
        {
          "definition": "Worker count",
          "symbol": "W",
          "units": "workers"
        },
        {
          "definition": "Bytes per live bin descriptor",
          "symbol": "b_bin_state",
          "units": "bytes per descriptor"
        },
        {
          "definition": "Bytes of model state per active partition",
          "symbol": "b_mode_state",
          "units": "bytes per partition"
        },
        {
          "definition": "Bytes of local state per worker",
          "symbol": "b_worker_state",
          "units": "bytes per worker"
        }
      ]
    },
    "io": {
      "assumptions": [
        "x_sc_p and x_pc_p are mutually exclusive mode indicators for partition p."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "IO_bytes = sum_p(x_sc_p * E_active_p * b_sc_record * c_sc_read_amplification + x_pc_p * E_total_p * b_pc_record * c_pc_read_amplification) + S_mode_spill",
      "measurement_needed": "Force both modes per partition and record logical records, physical bytes, page faults, spill, and elapsed scatter time across activity densities.",
      "uncertainty": "Target storage, cache, compression, and message encoding determine both amplification constants.",
      "unknown_constants": [
        "c_pc_read_amplification",
        "c_sc_read_amplification"
      ],
      "variables": [
        {
          "definition": "Active edges in partition p",
          "symbol": "E_active_p",
          "units": "edges"
        },
        {
          "definition": "Total edges in partition p",
          "symbol": "E_total_p",
          "units": "edges"
        },
        {
          "definition": "Spill bytes generated by the selected mode",
          "symbol": "S_mode_spill",
          "units": "bytes"
        },
        {
          "definition": "Encoded bytes per partition-streaming record",
          "symbol": "b_pc_record",
          "units": "bytes per record"
        },
        {
          "definition": "Encoded bytes per active-only record",
          "symbol": "b_sc_record",
          "units": "bytes per record"
        },
        {
          "definition": "Partition-streaming mode indicator for partition p",
          "symbol": "x_pc_p",
          "units": "dimensionless"
        },
        {
          "definition": "Active-only mode indicator for partition p",
          "symbol": "x_sc_p",
          "units": "dimensionless"
        }
      ]
    },
    "preprocessing": {
      "assumptions": [
        "Both access paths are built only when their combined budget is admitted."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "T_pre = (N_v + N_e) / c_dual_layout_build_rate + P_total / c_mode_calibration_rate",
      "measurement_needed": "Measure incremental dual-layout build time, per-partition statistics time, and calibration time separately.",
      "uncertainty": "Input order, representation, and calibration fixture determine both rates.",
      "unknown_constants": [
        "c_dual_layout_build_rate",
        "c_mode_calibration_rate"
      ],
      "variables": [
        {
          "definition": "Input edge count",
          "symbol": "N_e",
          "units": "edges"
        },
        {
          "definition": "Input vertex count",
          "symbol": "N_v",
          "units": "vertices"
        },
        {
          "definition": "Partition count",
          "symbol": "P_total",
          "units": "partitions"
        }
      ]
    },
    "ram": {
      "assumptions": [
        "Only one mode executes per partition, and admission sums each active partition's larger transient mode requirement."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "RAM_peak = R_fixed + R_frontier + sum_{p=1}^{P_active}max(R_sc_transient_p, R_pc_transient_p) + P_active*b_mode_state + Q_bins*b_bin_state + W*b_worker_state + c_bin_runtime + c_allocator_ram + c_runtime_ram",
      "measurement_needed": "Measure per-partition transients, live bin/stream descriptors, bin runtime, and whole-process RSS for forced and crossed-cost mixed-mode executions while attributing every component.",
      "uncertainty": "Dual-layout residency, page cache, and transient bin state are unmeasured.",
      "unknown_constants": [
        "c_allocator_ram",
        "c_bin_runtime",
        "c_runtime_ram"
      ],
      "variables": [
        {
          "definition": "Live bin or stream descriptors across admitted partitions",
          "symbol": "Q_bins",
          "units": "descriptors"
        },
        {
          "definition": "Active partitions with a mode decision",
          "symbol": "P_active",
          "units": "partitions"
        },
        {
          "definition": "Partition-streaming transient bytes for active partition p",
          "symbol": "R_pc_transient_p",
          "units": "bytes per partition"
        },
        {
          "definition": "Active frontier bytes",
          "symbol": "R_frontier",
          "units": "bytes"
        },
        {
          "definition": "Fixed graph and algorithm state",
          "symbol": "R_fixed",
          "units": "bytes"
        },
        {
          "definition": "Active-only transient bytes for active partition p",
          "symbol": "R_sc_transient_p",
          "units": "bytes per partition"
        },
        {
          "definition": "Worker count",
          "symbol": "W",
          "units": "workers"
        },
        {
          "definition": "Bytes of model state per active partition",
          "symbol": "b_mode_state",
          "units": "bytes per partition"
        },
        {
          "definition": "Bytes per live bin or stream descriptor, disjoint from partition transients",
          "symbol": "b_bin_state",
          "units": "bytes per descriptor"
        },
        {
          "definition": "Bytes of local state per worker",
          "symbol": "b_worker_state",
          "units": "bytes per worker"
        }
      ]
    },
    "storage": {
      "assumptions": [
        "The active-only and partition-streaming layouts are retained together only when admitted.",
        "S_model_trace is shared across retained layout generations and is not multiplied by G_layout_retained."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Storage_peak_bytes = G_layout_retained*(S_sc_layout_generation + S_pc_layout_generation + P_total*b_partition_offset + B_generation_metadata) + B_temporary_generation_peak + S_model_trace + c_layout_storage",
      "measurement_needed": "Measure every retained layout generation, offsets, per-generation metadata, shared model trace, incremental temporary build/replacement files, filesystem allocation high-water, and old-generation deletion timing.",
      "uncertainty": "The source does not total dual-layout storage amplification.",
      "unknown_constants": [
        "c_layout_storage"
      ],
      "variables": [
        {
          "definition": "Per-generation manifests, checksums, and filesystem metadata excluding c_layout_storage",
          "symbol": "B_generation_metadata",
          "units": "bytes per generation"
        },
        {
          "definition": "Incremental build or replacement bytes beyond all retained layout generations",
          "symbol": "B_temporary_generation_peak",
          "units": "bytes"
        },
        {
          "definition": "Dual-layout generations simultaneously retained at storage peak",
          "symbol": "G_layout_retained",
          "units": "generations"
        },
        {
          "definition": "Partition count",
          "symbol": "P_total",
          "units": "partitions"
        },
        {
          "definition": "Retained model trace bytes",
          "symbol": "S_model_trace",
          "units": "bytes"
        },
        {
          "definition": "Partition-streaming layout bytes in one retained generation",
          "symbol": "S_pc_layout_generation",
          "units": "bytes per generation"
        },
        {
          "definition": "Active-only layout bytes in one retained generation",
          "symbol": "S_sc_layout_generation",
          "units": "bytes per generation"
        },
        {
          "definition": "Persistent offset bytes per partition",
          "symbol": "b_partition_offset",
          "units": "bytes per partition"
        }
      ]
    }
  },
  "name": "Calibrate Partition Scatter Mode",
  "original_constraint_profile": {
    "access_medium": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-SELECT-PARTITION-SCATTER-MODE"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "The active-only path reads active CSR adjacency and switches destination bins, while the partition-streaming path scans a destination-grouped partition-node layout sequentially in DRAM.",
      "uncertainty": "The source does not model secondary-storage I/O."
    },
    "communication_model": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-SELECT-PARTITION-SCATTER-MODE"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002",
        "SP-003"
      ],
      "text": "One thread scatters each source partition using one of two equivalent modes, gather consumes destination-grouped messages, and mode selection is independent per partition and iteration.",
      "uncertainty": "The source assumes shared-memory visibility and exclusive bin ownership."
    },
    "constrained_resource": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-SELECT-PARTITION-SCATTER-MODE"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "text": "The mechanism trades active-edge work and bin switching against inactive-edge work and sequential memory bandwidth, while maintaining frontier, bin, and per-partition model state.",
      "uncertainty": "Peak RAM and dual-layout storage are not totaled."
    },
    "data_mutability": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-SELECT-PARTITION-SCATTER-MODE"
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "text": "Partition layouts and destination identifiers are reusable, while active counts, vertex values, frontiers, messages, and mode choices change each iteration.",
      "uncertainty": "Dynamic topology invalidation is not costed."
    },
    "original_hardware_operating_assumptions": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-SELECT-PARTITION-SCATTER-MODE"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "text": "The source assumes a shared-memory multicore system where partition vertex state and bin insertion points can use cache and both scatter modes are modeled by DRAM communication divided by calibrated mode bandwidth.",
      "uncertainty": "Cache sharing, NUMA, and storage-backed access are not represented by the source equations."
    },
    "predictability_requirement": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-SELECT-PARTITION-SCATTER-MODE"
      ],
      "source_pointer_ids": [
        "SP-003",
        "SP-005"
      ],
      "text": "The selector predicts per-partition mode cost from active vertices, active and total edges, aggregation, encoded widths, and configured mode bandwidths.",
      "uncertainty": "Aggregation and bandwidth calibration error are not bounded."
    }
  },
  "original_constraints": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-SELECT-PARTITION-SCATTER-MODE"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "Active-only scatter minimizes work for sparse activity but switches destination bins; partition-streaming scatter performs more work but writes destination-grouped streams sequentially.",
      "uncertainty": "Relative cost depends on activity and calibrated bandwidth."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-SELECT-PARTITION-SCATTER-MODE"
      ],
      "source_pointer_ids": [
        "SP-003"
      ],
      "text": "A mode decision is recomputed for every partition and iteration and selects partition-streaming only when its modeled time is no larger than active-only time under the source model.",
      "uncertainty": "The guarantee is model-relative, not a universal runtime guarantee."
    }
  ],
  "original_cost_model": {
    "assumptions": [
      "Both modes implement identical scatter semantics and can be forced independently."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-SELECT-PARTITION-SCATTER-MODE:resource_model.io",
      "PAT-SELECT-PARTITION-SCATTER-MODE:mechanism"
    ],
    "text": "For partition p, C_original = min(B_sc_p / k_sc_bandwidth, B_pc_p / k_pc_bandwidth), where B_sc_p and B_pc_p are symbolic communication volumes from active and total partition work. Unknown bandwidth and aggregation constants require forced-mode byte and elapsed-time measurements per partition.",
    "uncertainty": "The source model covers DRAM communication and approximates aggregation; it omits whole-process RAM, persistent storage, and external I/O."
  },
  "original_domain": "frontier-sensitive shared-memory graph communication",
  "proposed_transfer": {
    "assumptions": [
      "Knight Bus can expose two exact scatter paths and collect per-partition activity and working-set estimates."
    ],
    "claim_type": "SPECULATIVE_TRANSFER",
    "premises": [
      "PAT-SELECT-PARTITION-SCATTER-MODE:invariant",
      "FAIL-LARGE-PARTITIONS-THRASH-CACHE:repair_options",
      "FAIL-WRONG-SCATTER-MODE-WASTES:repair_options"
    ],
    "text": "Offer a per-partition mode-selection branch that recalculates active-only and partition-streaming cost from target RAM, cache, storage, preprocessing, and concurrency terms; reserve both paths' retained state and the larger transient state; cap partition working sets; and fall back to one forced exact mode or refuse when calibration uncertainty is excessive.",
    "uncertainty": "No target dual layout, selector, or calibrated crossover is selected or measured in G07."
  },
  "reversed_assumptions": [
    {
      "assumptions": [
        "Knight Bus may execute on mapped or disk-backed graph data."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "PAT-SELECT-PARTITION-SCATTER-MODE:unknown_when"
      ],
      "text": "DRAM communication divided by a source bandwidth ratio is no longer a sufficient cost model; physical reads, page faults, decompression, writes, and spill become target terms.",
      "uncertainty": "No target storage-mode coefficients exist."
    },
    {
      "assumptions": [
        "Effective cache is shared and workload-dependent."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "FAIL-LARGE-PARTITIONS-THRASH-CACHE"
      ],
      "text": "Larger partitions cannot be assumed to improve the partition-streaming path monotonically; partition working set must be bounded against measured effective cache.",
      "uncertainty": "Effective cache depends on access pattern, sharing, and thread placement."
    },
    {
      "assumptions": [
        "Dual layouts impose retained and preparation costs."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "PAT-SELECT-PARTITION-SCATTER-MODE:resource_model.persistent_storage",
        "PAT-SELECT-PARTITION-SCATTER-MODE:resource_model.preprocessing"
      ],
      "text": "The second access path is not free; its build time, retained bytes, invalidation, and rebuild costs must be admitted before mode selection is available.",
      "uncertainty": "The source does not isolate these costs."
    }
  ],
  "smallest_falsifier": {
    "controlled_variables": [
      "active-edge density",
      "cache state",
      "graph layout",
      "partition size",
      "thread count",
      "value and index widths",
      "simultaneously active mixed-mode partition count",
      "retained layout generations",
      "replacement cleanup policy",
      "live bin descriptor cap"
    ],
    "failure_signal": "Forced-mode outputs differ, the selector chooses a mode outside its prediction envelope, the summed crossed-cost mixed-mode peak exceeds admission without fallback, or versioned dual-layout build/replacement allocation exceeds Storage_peak_bytes before old-generation deletion.",
    "fixture": "One small graph with at least two active partitions whose mode costs cross, plus a versioned dual-layout replacement while the old generation remains retained and sparse/dense controls around the cache envelope.",
    "independent_oracle": "Element-wise exact updates; forced/mixed transient, live-bin, and RSS traces; and filesystem allocation high-water with generation creation/deletion timing."
  },
  "source_pattern_ids": [
    "PAT-SELECT-PARTITION-SCATTER-MODE"
  ],
  "surviving_invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-SELECT-PARTITION-SCATTER-MODE"
    ],
    "source_pointer_ids": [
      "SP-003"
    ],
    "text": "For each partition and iteration, choose between two semantically equivalent scatter paths from partition-local work and access-cost estimates rather than forcing one mode globally.",
    "uncertainty": "The source's modeled ordering depends on approximated aggregation and configured bandwidths."
  },
  "target_algorithm_families": [
    "BFS_SHORTEST_PATHS",
    "PAGERANK_CENTRALITY",
    "SEEDED_RANDOM_WALK",
    "WCC_CONNECTED_COMPONENTS"
  ],
  "transfer_id": "XFER-CALIBRATE-PARTITION-SCATTER-MODE",
  "unknown_measurement_constants": [
    "c_allocator_ram",
    "c_bin_runtime",
    "c_dual_layout_build_rate",
    "c_layout_storage",
    "c_mode_calibration_rate",
    "c_pc_read_amplification",
    "c_runtime_ram",
    "c_sc_read_amplification"
  ]
}
```
