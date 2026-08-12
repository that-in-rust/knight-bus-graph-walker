# Guard Sparse Row Packing

```json
{
  "analogy_failure_modes": [
    {
      "assumptions": [
        "Both formats encode identical coordinates and values."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "FAIL-SPARSE-ROW-ADVANTAGE-VANISHES triggering_workload",
        "FAIL-SPARSE-ROW-ADVANTAGE-VANISHES breakpoint_equation"
      ],
      "text": "The analogy fails when tiles are sufficiently occupied or the reference format is already compact, leaving too little empty-row metadata for packing to remove.",
      "uncertainty": "The occupancy crossover is implementation-specific."
    },
    {
      "assumptions": [
        "Unsafe marker or width combinations are rejected before encoding."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-PACK-NONEMPTY-SPARSE-ROWS invariant",
        "PAT-PACK-NONEMPTY-SPARSE-ROWS unknown_when"
      ],
      "text": "The analogy fails when identifier widths or marker conventions cannot encode the tile safely, or when updates invalidate packed row occupancy before conversion is amortized.",
      "uncertainty": "No incremental-update representation is established."
    }
  ],
  "epistemic_label": "SPECULATIVE_TRANSFER",
  "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-GUARD-SPARSE-ROW-PACKING",
  "g06_challenges": [
    {
      "applies": true,
      "failure_id": "FAIL-SPARSE-ROW-ADVANTAGE-VANISHES",
      "response": "Applies. Admission compares complete packed and reference bytes and amortized conversion over expected scans for the target tiling; it retains the exact reference format or refuses conversion when clustering, row occupancy, metadata, or reuse fails the crossover."
    }
  ],
  "modern_knight_bus_constraints": [
    {
      "assumptions": [
        "An exact reference sparse representation is available."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-PACK-NONEMPTY-SPARSE-ROWS a007_consequence",
        "FAIL-SPARSE-ROW-ADVANTAGE-VANISHES repair_options"
      ],
      "text": "Admission must count nonempty rows and nonzeros per candidate tiling, estimate complete packed and reference representations with modern byte widths, include conversion and dense working set, and choose the packed branch only when its guarded byte and reuse conditions survive.",
      "uncertainty": "Device and decoder crossovers require G09 measurement."
    },
    {
      "assumptions": [
        "Every decoded coordinate is verifiable against the source artifact."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "A007 receipt contract",
        "FAIL-SPARSE-ROW-ADVANTAGE-VANISHES expected_failure_signal"
      ],
      "text": "The receipt must record tiling, nnr, nnz, row and column widths, value width, container bytes, alignment, conversion bytes, scan count, physical I/O, decoder high-water, dense working set, and selected representation.",
      "uncertainty": "Physical I/O and cache behavior can diverge from logical payload bytes."
    }
  ],
  "modern_resource_model": {
    "concurrency": {
      "assumptions": [
        "The runtime enforces worker and tile-queue caps.",
        "Dense shared operands are counted outside per-worker state."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Concurrency_state_bytes = W_workers * (Q_tiles_per_worker * P_tile_buffer_bytes + B_decoder_worker_bytes) + c_scheduler_state_bytes",
      "measurement_needed": "Measure worker buffers, queued tile reservations, synchronization state, and scheduler allocations.",
      "uncertainty": "I/O polling, buffer pools, and runtime queues can add concurrency state.",
      "unknown_constants": [
        "c_scheduler_state_bytes"
      ],
      "variables": [
        {
          "definition": "worker and in-flight tile state",
          "symbol": "Concurrency_state_bytes",
          "units": "bytes"
        },
        {
          "definition": "tile workers",
          "symbol": "W_workers",
          "units": "workers"
        },
        {
          "definition": "bounded queued tiles per worker",
          "symbol": "Q_tiles_per_worker",
          "units": "tiles per worker"
        },
        {
          "definition": "buffer reservation per tile",
          "symbol": "P_tile_buffer_bytes",
          "units": "bytes per tile"
        },
        {
          "definition": "per-worker row and COO decoder state",
          "symbol": "B_decoder_worker_bytes",
          "units": "bytes per worker"
        }
      ]
    },
    "io": {
      "assumptions": [
        "Each encoded payload byte is logically read once per complete scan.",
        "Both formats encode identical values and coordinates."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "IO_total_bytes = c_physical_read_amplification * I_scans * (b_row_bytes * N_nonempty_rows + (b_column_bytes + b_value_bytes) * N_nonzeros + B_container_bytes)",
      "measurement_needed": "Measure logical payload, complete-file, and physical device bytes for packed and reference formats over declared cache states.",
      "uncertainty": "Read amplification, prefetch, and filesystem alignment can dominate a small logical byte difference.",
      "unknown_constants": [
        "c_physical_read_amplification"
      ],
      "variables": [
        {
          "definition": "physical sparse-stream read bytes",
          "symbol": "IO_total_bytes",
          "units": "bytes"
        },
        {
          "definition": "complete sparse scans",
          "symbol": "I_scans",
          "units": "scans"
        },
        {
          "definition": "bytes per stored row header",
          "symbol": "b_row_bytes",
          "units": "bytes per row header"
        },
        {
          "definition": "nonempty rows summed across admitted tiles",
          "symbol": "N_nonempty_rows",
          "units": "rows"
        },
        {
          "definition": "bytes per stored column identifier",
          "symbol": "b_column_bytes",
          "units": "bytes per nonzero"
        },
        {
          "definition": "bytes per stored nonzero value",
          "symbol": "b_value_bytes",
          "units": "bytes per nonzero"
        },
        {
          "definition": "stored nonzeros",
          "symbol": "N_nonzeros",
          "units": "nonzeros"
        },
        {
          "definition": "tile directory, header, and alignment bytes",
          "symbol": "B_container_bytes",
          "units": "bytes per representation"
        }
      ]
    },
    "preprocessing": {
      "assumptions": [
        "The source representation is scanned sequentially where possible.",
        "Output write bytes are recorded separately in the receipt."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "T_prepare = c_source_scan_time_per_byte * B_reference_bytes + c_encode_time_per_nonzero * N_nonzeros + c_directory_time_per_tile * N_tiles",
      "measurement_needed": "Measure source read, encoding, directory construction, output write, and scratch high-water separately.",
      "uncertainty": "Conversion can become nonsequential or require additional scratch in a different container implementation.",
      "unknown_constants": [
        "c_directory_time_per_tile",
        "c_encode_time_per_nonzero",
        "c_source_scan_time_per_byte"
      ],
      "variables": [
        {
          "definition": "format conversion time",
          "symbol": "T_prepare",
          "units": "time"
        },
        {
          "definition": "source sparse representation bytes read",
          "symbol": "B_reference_bytes",
          "units": "bytes"
        },
        {
          "definition": "nonzeros encoded",
          "symbol": "N_nonzeros",
          "units": "nonzeros"
        },
        {
          "definition": "tiles emitted",
          "symbol": "N_tiles",
          "units": "tiles"
        }
      ]
    },
    "ram": {
      "assumptions": [
        "Dense operands and output state are included in the admitted working set.",
        "Worker count is bounded.",
        "Shared B_decoder_state_bytes excludes every per-worker decoder and tile reservation."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "RAM_peak_bytes = B_sparse_tile_bytes + B_decoder_state_bytes + B_dense_working_set_bytes + B_algorithm_state_bytes + W_workers*(Q_tiles_per_worker*P_tile_buffer_bytes + B_decoder_worker_bytes) + c_scheduler_state_bytes + c_allocator_overhead_bytes",
      "measurement_needed": "Measure sparse tile, shared decoder, dense operand, algorithm, every per-worker tile reservation and decoder, scheduler, allocator, charged-cache, and aggregate memory high-water separately.",
      "uncertainty": "The source does not isolate decoder RAM from broader SEM-SpMM buffers.",
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
          "definition": "resident encoded sparse tiles",
          "symbol": "B_sparse_tile_bytes",
          "units": "bytes"
        },
        {
          "definition": "row, column, and COO decode state",
          "symbol": "B_decoder_state_bytes",
          "units": "bytes"
        },
        {
          "definition": "resident dense input and output partitions",
          "symbol": "B_dense_working_set_bytes",
          "units": "bytes"
        },
        {
          "definition": "algorithm-specific vectors and output state",
          "symbol": "B_algorithm_state_bytes",
          "units": "bytes"
        },
        {
          "definition": "tile workers",
          "symbol": "W_workers",
          "units": "workers"
        },
        {
          "definition": "bounded queued tiles per worker",
          "symbol": "Q_tiles_per_worker",
          "units": "tiles per worker"
        },
        {
          "definition": "buffer reservation per queued tile",
          "symbol": "P_tile_buffer_bytes",
          "units": "bytes per tile"
        },
        {
          "definition": "per-worker row and COO decoder state, excluding shared B_decoder_state_bytes",
          "symbol": "B_decoder_worker_bytes",
          "units": "bytes per worker"
        }
      ]
    },
    "storage": {
      "assumptions": [
        "Marker-bit constraints are satisfied for chosen widths.",
        "Single-entry COO payload is included in the same coordinate count.",
        "The exact reference representation remains retained until the packed artifact is verified and is the exact fallback source."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "STORAGE_peak_bytes = B_reference_retained + G_packed_retained*(b_row_bytes*N_nonempty_rows + (b_column_bytes + b_value_bytes)*N_nonzeros + B_tile_directory_bytes + B_alignment_bytes) + B_conversion_generation_peak + c_filesystem_overhead_bytes",
      "measurement_needed": "Measure retained exact reference, every retained packed generation, directories/alignment, incremental conversion files, allocation high-water, and creation/deletion timing.",
      "uncertainty": "Container design and alignment can erase payload-level savings.",
      "unknown_constants": [
        "c_filesystem_overhead_bytes"
      ],
      "variables": [
        {
          "definition": "maximum allocated storage while reference, packed, and conversion generations coexist",
          "symbol": "STORAGE_peak_bytes",
          "units": "bytes"
        },
        {
          "definition": "incremental packed conversion/replacement bytes beyond retained reference and packed generations",
          "symbol": "B_conversion_generation_peak",
          "units": "bytes"
        },
        {
          "definition": "retained exact reference bytes used by fallback",
          "symbol": "B_reference_retained",
          "units": "bytes"
        },
        {
          "definition": "packed generations simultaneously retained at storage peak",
          "symbol": "G_packed_retained",
          "units": "generations"
        },
        {
          "definition": "row-header width",
          "symbol": "b_row_bytes",
          "units": "bytes per row header"
        },
        {
          "definition": "nonempty rows across tiles",
          "symbol": "N_nonempty_rows",
          "units": "rows"
        },
        {
          "definition": "column identifier width",
          "symbol": "b_column_bytes",
          "units": "bytes per nonzero"
        },
        {
          "definition": "value width",
          "symbol": "b_value_bytes",
          "units": "bytes per nonzero"
        },
        {
          "definition": "nonzeros",
          "symbol": "N_nonzeros",
          "units": "nonzeros"
        },
        {
          "definition": "tile directory and container headers",
          "symbol": "B_tile_directory_bytes",
          "units": "bytes"
        },
        {
          "definition": "padding and alignment bytes",
          "symbol": "B_alignment_bytes",
          "units": "bytes"
        }
      ]
    }
  },
  "name": "Guard Sparse Row Packing",
  "original_constraint_profile": {
    "access_medium": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-PACK-NONEMPTY-SPARSE-ROWS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002",
        "SP-004"
      ],
      "text": "The encoded sparse matrix is stored and sequentially scanned from SSD while current sparse tiles, decoder state, and selected dense-matrix partitions are processed in memory.",
      "uncertainty": "The mechanism card does not isolate all dense-operand residency from the larger SEM-SpMM execution."
    },
    "communication_model": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-PACK-NONEMPTY-SPARSE-ROWS"
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-004"
      ],
      "text": "Rows and columns are decoded from a sequential row-major tile stream, with empty rows omitted and single-entry rows consumed from an adjacent COO suffix.",
      "uncertainty": "The extracted mechanism does not specify distributed communication."
    },
    "constrained_resource": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-PACK-NONEMPTY-SPARSE-ROWS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002",
        "SP-003"
      ],
      "text": "The source mechanism reduces sparse tile payload and SSD traffic by storing row headers only for nonempty tile rows.",
      "uncertainty": "Complete container, alignment, decoder, and dense-operand costs are outside the displayed payload expression."
    },
    "data_mutability": {
      "assumptions": [
        "No publication-year inference or unstated source assumption is used."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The mechanism card describes conversion from a sparse source representation but does not establish online update semantics for packed tiles."
      ],
      "text": "UNKNOWN",
      "uncertainty": "Mutation may change row occupancy and require tile rewrite or rebuild."
    },
    "original_hardware_operating_assumptions": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-PACK-NONEMPTY-SPARSE-ROWS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "The mechanism assumes semi-external SSD storage, in-memory tile and dense-operand processing, bounded tile identifiers, and a reserved marker bit distinguishing row headers from columns.",
      "uncertainty": "Concrete tile dimensions, identifier widths, cache size, and SSD behavior are source conditions."
    },
    "predictability_requirement": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-PACK-NONEMPTY-SPARSE-ROWS"
      ],
      "source_pointer_ids": [
        "SP-003",
        "SP-006"
      ],
      "text": "Under the source encoding assumptions, payload bytes are determined from nonempty-row count, nonzero count, and value width, and conversion performs one sequential input read and one sequential output write.",
      "uncertainty": "The payload equation excludes outer container and alignment overhead."
    }
  },
  "original_constraints": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-PACK-NONEMPTY-SPARSE-ROWS"
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-003",
        "SP-004"
      ],
      "text": "Every nonempty tile row has one row header followed by columns, empty rows have no header, and single-entry rows may use an exact adjacent COO representation.",
      "uncertainty": "The marker convention constrains usable identifier width."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-PACK-NONEMPTY-SPARSE-ROWS"
      ],
      "source_pointer_ids": [
        "SP-005",
        "SP-006"
      ],
      "text": "Conversion and row metadata are useful only when enough empty-row overhead is removed and enough scans amortize the conversion.",
      "uncertainty": "The source reports reduced benefit on clustered data, not a universal reversal threshold."
    }
  ],
  "original_cost_model": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-PACK-NONEMPTY-SPARSE-ROWS"
    ],
    "source_pointer_ids": [
      "SP-003",
      "SP-006"
    ],
    "text": "The source tile payload is S_SCSR = 2*nnr + (2+c)*nnz bytes under its two-byte row and column identifiers, and CSR-to-SCSR conversion is linear with one sequential source read and one sequential destination write.",
    "uncertainty": "Outer tile directories, alignment, decoder RAM, and conversion scratch are not included in the payload expression."
  },
  "original_domain": "compact tiled sparse-matrix encoding for graph-shaped SpMM operands",
  "proposed_transfer": {
    "assumptions": [
      "The coordinate and value oracle is exact."
    ],
    "claim_type": "SPECULATIVE_TRANSFER",
    "premises": [
      "PAT-PACK-NONEMPTY-SPARSE-ROWS invariant",
      "FAIL-SPARSE-ROW-ADVANTAGE-VANISHES breakpoint_equation",
      "A007 fit/spill/refuse contract"
    ],
    "text": "Use nonempty-row packing as an exact, tiling-specific storage branch selected from a complete symbolic comparison against a reference sparse format, with conversion/reuse accounting and a reference fallback when row occupancy or lifecycle cost removes the advantage.",
    "uncertainty": "The transfer does not select a tile size, marker width, sparse format, or performance winner."
  },
  "reversed_assumptions": [
    {
      "assumptions": [
        "Both candidate formats encode identical coordinates and values."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "PAT-PACK-NONEMPTY-SPARSE-ROWS resource_model",
        "FAIL-SPARSE-ROW-ADVANTAGE-VANISHES breakpoint_equation"
      ],
      "text": "The source's fixed identifier widths and source comparison are no longer assumed; modern admission uses explicit row-ID, column-ID, value, directory, alignment, decoder, and reference-format terms.",
      "uncertainty": "Reference-format and decoder coefficients remain implementation-specific."
    },
    {
      "assumptions": [
        "The source artifact's lifecycle and expected reuse are known to admission."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "PAT-PACK-NONEMPTY-SPARSE-ROWS works_when",
        "FAIL-SPARSE-ROW-ADVANTAGE-VANISHES triggering_workload"
      ],
      "text": "One-time conversion is no longer presumed amortized; expected scan count, update invalidation, and full preparation storage are declared before the packed branch is selected.",
      "uncertainty": "No portable reuse crossover has been measured."
    }
  ],
  "smallest_falsifier": {
    "controlled_variables": [
      "identifier_width",
      "scan_count",
      "tile_dimensions",
      "value_width",
      "worker_count",
      "tile queue cap per worker",
      "retained packed generations",
      "conversion cleanup policy"
    ],
    "failure_signal": "Decoded triples differ, an empty row consumes a header, packed RAM exceeds admission, conversion allocation exceeds STORAGE_peak_bytes before cleanup, or the exact fallback becomes unavailable.",
    "fixture": "A tile family with fixed dimensions and nonzero count but variable nonempty-row occupancy, including empty, single-entry, and multi-entry rows, encoded in packed and exact reference formats.",
    "independent_oracle": "Canonical triples, byte-counted reference and packed codecs, component-attributed shared/worker decoder, queued-tile, scheduler, cache, aggregate charged-memory counters, and filesystem allocation plus generation creation/deletion traces through conversion."
  },
  "source_pattern_ids": [
    "PAT-PACK-NONEMPTY-SPARSE-ROWS"
  ],
  "surviving_invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-PACK-NONEMPTY-SPARSE-ROWS"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "A sparse tile can preserve exact coordinates while omitting all empty-row headers, provided each stored row header and following columns are unambiguously decoded.",
    "uncertainty": "Packing benefit depends on row occupancy and representation widths, but coordinate correctness does not."
  },
  "target_algorithm_families": [
    "FASTRP_EMBEDDINGS",
    "GRAPH_LINEAR_ALGEBRA",
    "SPARSE_MATRIX_DENSE_MULTIPLICATION"
  ],
  "transfer_id": "XFER-GUARD-SPARSE-ROW-PACKING",
  "unknown_measurement_constants": [
    "c_allocator_overhead_bytes",
    "c_directory_time_per_tile",
    "c_encode_time_per_nonzero",
    "c_filesystem_overhead_bytes",
    "c_physical_read_amplification",
    "c_scheduler_state_bytes",
    "c_source_scan_time_per_byte"
  ]
}
```
