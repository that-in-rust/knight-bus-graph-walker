# Guard Compressed ID Streams

```json
{
  "analogy_failure_modes": [
    {
      "assumptions": [
        "Topology mutation is observable before admission."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-COMPRESS-SORTED-ID-STREAMS unknown_when",
        "The proposed transfer requires immutable representation epochs."
      ],
      "text": "The analogy fails when topology updates invalidate sorted merged streams faster than rebuild cost can be amortized.",
      "uncertainty": "No update-frequency crossover is available."
    },
    {
      "assumptions": [
        "Exact decode continues to hold."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "FAIL-XOR-STREAM-COMPRESSION-CROSSOVER breakpoint_equation",
        "PAT-COMPRESS-SORTED-ID-STREAMS resource_model"
      ],
      "text": "The analogy fails when favorable logical compression is offset by decode CPU, physical read amplification, or preprocessing high-water storage.",
      "uncertainty": "All crossover constants remain unmeasured."
    }
  ],
  "epistemic_label": "SPECULATIVE_TRANSFER",
  "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-GUARD-COMPRESSED-ID-STREAMS",
  "g06_challenges": [
    {
      "applies": true,
      "failure_id": "FAIL-XOR-STREAM-COMPRESSION-CROSSOVER",
      "response": "Applies. Admission samples both orientation streams under the target identifier order, includes expected scans and peak temporary bytes, and selects the exact plain-stream fallback or refuses the compression branch when the byte or amortized-lifecycle crossover is not satisfied."
    }
  ],
  "modern_knight_bus_constraints": [
    {
      "assumptions": [
        "A plain exact stream remains an available fallback."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-COMPRESS-SORTED-ID-STREAMS a007_consequence",
        "FAIL-XOR-STREAM-COMPRESSION-CROSSOVER repair_options"
      ],
      "text": "Admission must estimate both orientations, optional weights, decode buffers, temporary merge coexistence, physical read amplification, and expected scan reuse before selecting compression.",
      "uncertainty": "Sampling error and codec coefficients require G09 measurement."
    },
    {
      "assumptions": [
        "Receipt counters are scoped to the admitted run and engine version."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "A007 receipt fields",
        "FAIL-XOR-STREAM-COMPRESSION-CROSSOVER expected_failure_signal"
      ],
      "text": "The receipt must record the selected representation, encoded bytes, weight bytes read or avoided, decode volume, preprocessing high-water storage, physical I/O, and whether the admission crossover remained satisfied.",
      "uncertainty": "Some operating systems expose cache and mapped-page accounting only approximately."
    }
  ],
  "modern_resource_model": {
    "concurrency": {
      "assumptions": [
        "The runtime enforces the declared worker and queue caps.",
        "Buffers are not silently shared outside the accounting boundary."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Concurrency_state_bytes = W_workers * (B_decode_worker_bytes + Q_reads_per_worker * P_read_bytes) + c_scheduler_state_bytes",
      "measurement_needed": "Measure per-worker buffers, in-flight read reservations, and scheduler allocations at each concurrency setting.",
      "uncertainty": "Runtime queueing and library state may add unmodeled concurrency bytes.",
      "unknown_constants": [
        "c_scheduler_state_bytes"
      ],
      "variables": [
        {
          "definition": "bounded worker and in-flight read state",
          "symbol": "Concurrency_state_bytes",
          "units": "bytes"
        },
        {
          "definition": "decode workers",
          "symbol": "W_workers",
          "units": "workers"
        },
        {
          "definition": "per-worker codec and output buffer",
          "symbol": "B_decode_worker_bytes",
          "units": "bytes per worker"
        },
        {
          "definition": "maximum in-flight reads per worker",
          "symbol": "Q_reads_per_worker",
          "units": "reads per worker"
        },
        {
          "definition": "bytes reserved per in-flight read",
          "symbol": "P_read_bytes",
          "units": "bytes per read"
        }
      ]
    },
    "io": {
      "assumptions": [
        "Both representations encode the same edge multiset.",
        "Cache state is declared in the run manifest."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "IO_total_bytes = c_physical_read_amplification * I_scans * (B_ids_compressed_bytes + Q_weighted * B_weights_bytes) + B_prepare_read_bytes + B_prepare_write_bytes",
      "measurement_needed": "Trace logical and physical bytes by orientation, scan, cache state, and weight use.",
      "uncertainty": "Filesystem, page-cache, and device behavior can make physical bytes exceed logical bytes.",
      "unknown_constants": [
        "c_physical_read_amplification"
      ],
      "variables": [
        {
          "definition": "physical lifecycle I/O",
          "symbol": "IO_total_bytes",
          "units": "bytes"
        },
        {
          "definition": "post-build graph scans",
          "symbol": "I_scans",
          "units": "scans"
        },
        {
          "definition": "complete compressed identifier bytes per scan",
          "symbol": "B_ids_compressed_bytes",
          "units": "bytes per scan"
        },
        {
          "definition": "one when the admitted algorithm consumes weights, otherwise zero",
          "symbol": "Q_weighted",
          "units": "dimensionless"
        },
        {
          "definition": "weight-stream bytes per scan",
          "symbol": "B_weights_bytes",
          "units": "bytes per scan"
        },
        {
          "definition": "preprocessing read bytes",
          "symbol": "B_prepare_read_bytes",
          "units": "bytes"
        },
        {
          "definition": "preprocessing write bytes",
          "symbol": "B_prepare_write_bytes",
          "units": "bytes"
        }
      ]
    },
    "preprocessing": {
      "assumptions": [
        "Input generation cost is accounted separately if the source artifact is not already partitioned.",
        "The logarithmic sort term is a planning model, not a measured claim."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "T_prepare = c_sort_time_per_edge_log_edge * M_edges * log2(M_edges + 1) + c_encode_time_per_edge * M_edges + c_merge_time_per_edge_orientation * M_edges * O_orientations",
      "measurement_needed": "Measure sort, encode, and each orientation merge independently for candidate codecs and identifier orders.",
      "uncertainty": "External sorting, mmap faults, and parallel scheduling can change the modeled coefficients.",
      "unknown_constants": [
        "c_encode_time_per_edge",
        "c_merge_time_per_edge_orientation",
        "c_sort_time_per_edge_log_edge"
      ],
      "variables": [
        {
          "definition": "representation build time",
          "symbol": "T_prepare",
          "units": "time"
        },
        {
          "definition": "edges encoded",
          "symbol": "M_edges",
          "units": "edges"
        },
        {
          "definition": "retained scan orientations",
          "symbol": "O_orientations",
          "units": "orientations"
        }
      ]
    },
    "ram": {
      "assumptions": [
        "Compressed and decoded buffers can coexist.",
        "Mapped pages charged to the run are included.",
        "Shared B_decode_bytes excludes every per-worker codec/output buffer and in-flight read reservation."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "RAM_peak_bytes = B_decode_bytes + B_metadata_bytes + W_workers*(B_decode_worker_bytes + Q_reads_per_worker*P_read_bytes) + c_scheduler_state_bytes + B_cache_charge_bytes + c_allocator_overhead_bytes",
      "measurement_needed": "Measure shared decode, metadata, every per-worker codec/output buffer and read reservation, scheduler state, cache charge, allocator state, and aggregate RSS or cgroup charge across worker counts, queue caps, and subgrid sizes.",
      "uncertainty": "Allocator, page-cache, and worker-buffer coefficients are target-runtime dependent.",
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
          "definition": "decoded identifier buffer bytes",
          "symbol": "B_decode_bytes",
          "units": "bytes"
        },
        {
          "definition": "subgrid and orientation metadata bytes",
          "symbol": "B_metadata_bytes",
          "units": "bytes"
        },
        {
          "definition": "concurrent decode workers",
          "symbol": "W_workers",
          "units": "workers"
        },
        {
          "definition": "per-worker codec and output buffer, excluding shared B_decode_bytes",
          "symbol": "B_decode_worker_bytes",
          "units": "bytes per worker"
        },
        {
          "definition": "maximum in-flight reads per worker",
          "symbol": "Q_reads_per_worker",
          "units": "reads per worker"
        },
        {
          "definition": "bytes reserved per in-flight read",
          "symbol": "P_read_bytes",
          "units": "bytes per read"
        },
        {
          "definition": "charged mapped or page-cache bytes",
          "symbol": "B_cache_charge_bytes",
          "units": "bytes"
        }
      ]
    },
    "storage": {
      "assumptions": [
        "Input retention policy is declared.",
        "Both required orientations are counted before admission."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Storage_peak_bytes = B_input_bytes + B_block_files_bytes + B_live_merge_output_bytes + B_weights_bytes + B_metadata_bytes + c_filesystem_overhead_bytes",
      "measurement_needed": "Trace allocated file bytes and deletion timing throughout preparation and finalization.",
      "uncertainty": "Sparse-file allocation, alignment, journaling, and deletion timing are implementation dependent.",
      "unknown_constants": [
        "c_filesystem_overhead_bytes"
      ],
      "variables": [
        {
          "definition": "maximum live persistent plus temporary storage",
          "symbol": "Storage_peak_bytes",
          "units": "bytes"
        },
        {
          "definition": "retained source artifact bytes",
          "symbol": "B_input_bytes",
          "units": "bytes"
        },
        {
          "definition": "live partition-block bytes",
          "symbol": "B_block_files_bytes",
          "units": "bytes"
        },
        {
          "definition": "live compressed orientation output bytes",
          "symbol": "B_live_merge_output_bytes",
          "units": "bytes"
        },
        {
          "definition": "retained optional weight bytes",
          "symbol": "B_weights_bytes",
          "units": "bytes"
        },
        {
          "definition": "orientation and subgrid metadata bytes",
          "symbol": "B_metadata_bytes",
          "units": "bytes"
        }
      ]
    }
  },
  "name": "Guard Compressed ID Streams",
  "original_constraint_profile": {
    "access_medium": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-COMPRESS-SORTED-ID-STREAMS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "text": "Partition blocks and merged orientation streams are file-backed; preprocessing maps blocks for sorting and execution batch-decodes compressed subgrids into memory.",
      "uncertainty": "Physical page residency and filesystem read amplification are not bounded by the source."
    },
    "communication_model": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-COMPRESS-SORTED-ID-STREAMS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "text": "Compressed identifier words and only requested weight streams move from file-backed storage to an in-memory decoder and graph streamer.",
      "uncertainty": "The extracted mechanism does not quantify inter-thread coordination or distributed communication."
    },
    "constrained_resource": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-COMPRESS-SORTED-ID-STREAMS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003",
        "SP-004"
      ],
      "text": "The source mechanism reduces secondary-storage traffic and retained edge-stream bytes while preserving repeated iterative scans.",
      "uncertainty": "The mechanism card does not provide whole-process RSS or peak temporary-storage measurements."
    },
    "data_mutability": {
      "assumptions": [
        "No publication-year inference or unstated source assumption is used."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The extracted mechanism preprocesses and merges graph topology but does not establish topology-update semantics for the compressed orientations."
      ],
      "text": "UNKNOWN",
      "uncertainty": "Separate weight access is described, but topology mutation and rebuild consistency remain unspecified."
    },
    "original_hardware_operating_assumptions": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-COMPRESS-SORTED-ID-STREAMS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002",
        "SP-003"
      ],
      "text": "The mechanism assumes file-backed blocks, virtual-memory mapping for in-place sorting, and selector-described 64-bit packed words that support batch decoding.",
      "uncertainty": "The source-specific machine and device costs are not portable constants."
    },
    "predictability_requirement": {
      "assumptions": [
        "No publication-year inference or unstated source assumption is used."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The mechanism card states no source-side hard-budget, latency, or completion-predictability requirement."
      ],
      "text": "UNKNOWN",
      "uncertainty": "The source reports a mechanism and evaluations, not an enforceable admission contract."
    }
  },
  "original_constraints": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-COMPRESS-SORTED-ID-STREAMS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "text": "Both source- and destination-oriented scans require orientation-specific merged streams, and optional weights remain separate so unweighted scans can omit them.",
      "uncertainty": "Retaining both orientations duplicates some compressed graph storage."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-COMPRESS-SORTED-ID-STREAMS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-004",
        "SP-006"
      ],
      "text": "Compression is useful only when sorted identifiers have enough locality and repeated scans amortize sorting, encoding, merging, and repeated decoding.",
      "uncertainty": "The source does not state a portable locality or scan-count crossover."
    }
  ],
  "original_cost_model": {
    "assumptions": [
      "All lifecycle phases, rather than only final compressed bytes, are in scope."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-COMPRESS-SORTED-ID-STREAMS resource_model.preprocessing and resource_model.persistent_storage",
      "PAT-COMPRESS-SORTED-ID-STREAMS a007_consequence"
    ],
    "text": "The source lifecycle contains block generation, sorting, encoding, two orientation merges, final compressed streams, optional weights, repeated decode work, and temporary coexistence of input blocks with merge output.",
    "uncertainty": "Peak RSS, complete filesystem bytes, deletion timing, and codec-specific CPU coefficients remain unknown."
  },
  "original_domain": "lossless graph edge-stream compression for iterative out-of-core processing",
  "proposed_transfer": {
    "assumptions": [
      "The graph stream is immutable for the admitted representation lifetime."
    ],
    "claim_type": "SPECULATIVE_TRANSFER",
    "premises": [
      "PAT-COMPRESS-SORTED-ID-STREAMS invariant",
      "FAIL-XOR-STREAM-COMPRESSION-CROSSOVER breakpoint_equation",
      "A007 fit/spill/approximate/refuse contract"
    ],
    "text": "Treat lossless sorted-ID compression as a guarded storage branch: sample target identifier locality, model full lifecycle resources and reuse, choose an exact codec or plain-stream fallback before execution, and preserve byte-exact decode verification.",
    "uncertainty": "The transfer does not establish a winning codec, threshold, or performance benefit."
  },
  "reversed_assumptions": [
    {
      "assumptions": [
        "Decoded graph semantics are held constant across representations."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "PAT-COMPRESS-SORTED-ID-STREAMS fails_when",
        "FAIL-XOR-STREAM-COMPRESSION-CROSSOVER breakpoint_equation"
      ],
      "text": "The source codec and identifier order are no longer assumed cost-favorable; codec choice, locality, physical read amplification, and expected reuse are target-specific decision inputs.",
      "uncertainty": "No target-machine crossover has been measured."
    },
    {
      "assumptions": [
        "The hard budget applies to the complete execution lifecycle."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "PAT-COMPRESS-SORTED-ID-STREAMS a007_consequence",
        "A007 before-execution and after-execution receipt contract"
      ],
      "text": "Final compressed size is no longer treated as the resource boundary because A007 requires full-working-set admission and a post-run receipt.",
      "uncertainty": "The relative contribution of page cache, allocator state, and temporary files is unmeasured."
    }
  ],
  "smallest_falsifier": {
    "controlled_variables": [
      "cache_state",
      "codec",
      "edge_weights",
      "filesystem",
      "graph_topology",
      "orientation_count",
      "thread_count",
      "in-flight reads per worker"
    ],
    "failure_signal": "The guard selects compression even though decode differs from the oracle, peak charged RAM or storage exceeds the declared bound, or the measured lifecycle byte/time crossover favors the plain stream.",
    "fixture": "One fixed graph emitted under a locality-preserving and a locality-destroying identifier permutation, encoded in both orientations with the candidate codec and an exact plain stream while scan count is varied.",
    "independent_oracle": "Byte-exact edge and weight multisets from independently sorted plain orientation streams, plus component-attributed shared decode, per-worker decode/read, scheduler, cache, allocator, aggregate charged-memory, allocated-storage, and lifecycle time/byte traces."
  },
  "source_pattern_ids": [
    "PAT-COMPRESS-SORTED-ID-STREAMS"
  ],
  "surviving_invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-COMPRESS-SORTED-ID-STREAMS"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Sorting can expose adjacent identifier locality; storing the first identifier and losslessly encoding reversible adjacent transforms preserves the exact ordered identifier stream.",
    "uncertainty": "The invariant guarantees losslessness, not favorable compression or amortized cost."
  },
  "target_algorithm_families": [
    "BFS_SHORTEST_PATHS",
    "COMMUNITY_DETECTION",
    "PAGERANK_CENTRALITY",
    "SPARSE_MATRIX_VECTOR",
    "WCC_CONNECTED_COMPONENTS"
  ],
  "transfer_id": "XFER-GUARD-COMPRESSED-ID-STREAMS",
  "unknown_measurement_constants": [
    "c_allocator_overhead_bytes",
    "c_encode_time_per_edge",
    "c_filesystem_overhead_bytes",
    "c_merge_time_per_edge_orientation",
    "c_physical_read_amplification",
    "c_scheduler_state_bytes",
    "c_sort_time_per_edge_log_edge"
  ]
}
```
