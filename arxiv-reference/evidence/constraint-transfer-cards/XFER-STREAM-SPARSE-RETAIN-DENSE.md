# Stream Sparse Retain Dense

```json
{
  "analogy_failure_modes": [
    {
      "assumptions": [
        "The target plan preserves the source complete-column invariant."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "PAT-STREAM-SPARSE-KEEP-DENSE source pointer SP-004 defines the complete-column plus thread-buffer minimum.",
        "FAIL-DENSE-COLUMN-EXCEEDS-MEMORY identifies failure below that minimum."
      ],
      "text": "If RAM cannot hold one complete dense column and required buffers, the source semi-external schedule is not applicable.",
      "uncertainty": "A different fully external mechanism may remain possible."
    },
    {
      "assumptions": [
        "The target plan claims this source invariant."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "The surviving invariant and source partitioning use complete columns."
      ],
      "text": "Arbitrary row fragments of the dense operand are not a valid substitute for complete-column residency unless a separately proven algorithm changes the multiplication schedule.",
      "uncertainty": "Other decompositions require independent evidence."
    },
    {
      "assumptions": [
        "The product has a declared I/O or runtime envelope in addition to RAM."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source reports additional sparse passes and narrow-partition degradation.",
        "G06 separates hard fit failure from cost degradation."
      ],
      "text": "A narrow partition that technically fits can still amplify repeated sparse scans and lose locality, so fit alone does not establish a useful plan.",
      "uncertainty": "The useful crossover is device- and workload-specific."
    }
  ],
  "epistemic_label": "SPECULATIVE_TRANSFER",
  "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-STREAM-SPARSE-RETAIN-DENSE",
  "g06_challenges": [
    {
      "applies": true,
      "failure_id": "FAIL-DENSE-COLUMN-EXCEEDS-MEMORY",
      "response": "Treat one complete dense column plus output, worker, I/O, allocator, queue, runtime, and concurrency state as the minimum admission unit; reduce bounded concurrency where valid, otherwise route to a separately verified fully external plan or refuse."
    }
  ],
  "modern_knight_bus_constraints": [
    {
      "assumptions": [
        "Dense dimensions, element width, thread count, and buffer policy are known before execution."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "The source invariant requires a complete dense column.",
        "G06 identifies inability to form that partition as a hard boundary."
      ],
      "text": "Admit the semi-external plan only when measured available RAM covers one complete dense column, required output window, worker buffers, asynchronous descriptors, queues, allocator overhead, runtime state, and declared concurrent work.",
      "uncertainty": "Operating-system page and cache accounting require calibration."
    },
    {
      "assumptions": [
        "Any fully external fallback has its own resource and correctness evidence."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "G06 distinguishes a fully external fallback from the source mechanism.",
        "A007 permits fit, spill, approximate, or refuse branches."
      ],
      "text": "When the complete-column admission unit does not fit, select a separately verified fully external algorithm or refuse; reducing thread multiplicity is allowed only if its smaller buffer term preserves correctness and the declared plan.",
      "uncertainty": "This card does not define that fallback."
    },
    {
      "assumptions": [
        "The runtime exposes phase and device counters."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "The transfer trades resident dense state against repeated sparse scans."
      ],
      "text": "The receipt must report resident dense columns, sparse pass count, sparse bytes, dense input and output bytes, output writes, worker and I/O buffer peaks, concurrency, result checksum, and refusal or fallback reason.",
      "uncertainty": "Counter availability varies by operating environment."
    }
  ],
  "modern_resource_model": {
    "concurrency": {
      "assumptions": [
        "Thread and queue bounds are fixed at admission and cannot grow without a plan revision."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "CONCURRENCY_peak_bytes = C_threads*(B_thread_buffer + b_async_descriptor) + Q_tasks*c_work_queue_entry",
      "measurement_needed": "Measure per-thread buffers, asynchronous descriptors, queue entries, in-flight output, storage queue depth, and peak aggregate RSS.",
      "uncertainty": "I/O batching and runtime task representation may make state non-linear.",
      "unknown_constants": [
        "b_async_descriptor",
        "c_work_queue_entry"
      ],
      "variables": [
        "B_thread_buffer: per-thread sparse and output buffer bytes",
        "C_threads: admitted worker count",
        "Q_tasks: maximum queued tile-row tasks"
      ]
    },
    "io": {
      "assumptions": [
        "Each dense partition requires a logically complete sparse pass and cache reuse is represented by the measured sparse-read fraction."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "IO_total_bytes = P_dense_partitions*(rho_sparse_read_fraction*E_sparse + B_dense_partition_read + B_output_write)",
      "measurement_needed": "Measure sparse, dense-input, output, metadata, readahead, and cache bytes separately for each partition count and device state.",
      "uncertainty": "Cache eviction, direct I/O, output residency, and fused iteration reuse can change traffic.",
      "unknown_constants": [
        "rho_sparse_read_fraction"
      ],
      "variables": [
        "B_dense_partition_read: bytes read for one dense input partition",
        "B_output_write: bytes written for one output partition",
        "E_sparse: encoded sparse operand bytes",
        "P_dense_partitions: complete-column partition count"
      ]
    },
    "preprocessing": {
      "assumptions": [
        "Any specialized sparse layout is charged to the workload or an explicit amortization horizon."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "PREP_work_units = E_sparse*c_sparse_format_convert + T_tiles*c_tile_index_build",
      "measurement_needed": "Measure source-format reads, target-format writes, conversion time, tile-index construction, peak preparation RSS, and reuse count.",
      "uncertainty": "Existing artifact format, graph updates, and validation requirements alter preparation.",
      "unknown_constants": [
        "c_sparse_format_convert",
        "c_tile_index_build"
      ],
      "variables": [
        "E_sparse: input sparse bytes converted or validated",
        "PREP_work_units: implementation-defined preparation work units",
        "T_tiles: sparse tile count"
      ]
    },
    "ram": {
      "assumptions": [
        "All columns in an admitted partition are complete and element width is fixed for the operation."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "RAM_peak_bytes = N_rows*b_dense_element*K_resident_columns + C_threads*(B_thread_buffer + b_async_descriptor) + Q_tasks*c_work_queue_entry + B_output_window + B_sparse_output_cache_peak + B_runtime_state + b_allocator_headroom",
      "measurement_needed": "Measure peak RSS and disjoint component allocations around the complete-column boundary for each dense column, thread buffer, asynchronous descriptor, queued task, output window, sparse/output page-cache high-water, runtime state, and allocator policy.",
      "uncertainty": "Runtime, mapped pages, queues, alignment, and allocator behavior can raise the peak.",
      "unknown_constants": [
        "b_allocator_headroom",
        "b_async_descriptor",
        "c_work_queue_entry",
        "b_dense_element"
      ],
      "variables": [
        "B_output_window: resident output rows or partition buffer in bytes",
        "B_runtime_state: executor and I/O runtime state excluding descriptors, work-queue entries, and allocator headroom",
        "B_sparse_output_cache_peak: sparse-input and output pages physically resident at peak, excluding explicit thread and output buffers",
        "B_thread_buffer: per-thread sparse-input and output buffer bytes",
        "C_threads: admitted worker count",
        "K_resident_columns: complete dense columns resident together",
        "N_rows: dense operand row count",
        "Q_tasks: maximum queued tile-row tasks"
      ]
    },
    "storage": {
      "assumptions": [
        "Input and output retention policies are fixed before storage admission."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "STORAGE_peak_bytes = E_sparse + B_dense_input_external + B_dense_output_external + B_sparse_index + H_retained*b_storage_metadata + B_temporary_generations_peak + B_receipt",
      "measurement_needed": "Measure sparse encoding, tile index, external dense operands, output, temporary generations, manifests, and retained receipts.",
      "uncertainty": "Checkpointing, compression, versioning, and mutable snapshot retention are unspecified.",
      "unknown_constants": [
        "b_storage_metadata"
      ],
      "variables": [
        "B_dense_input_external: persisted dense input bytes not resident",
        "B_dense_output_external: persisted or spilled dense output bytes",
        "B_receipt: retained checksum and execution-receipt payload bytes excluding per-generation manifests",
        "B_sparse_index: tile directory and offset bytes",
        "B_temporary_generations_peak: incremental bytes for temporary conversion, checkpoint, or replacement generations beyond all retained baseline components",
        "E_sparse: encoded sparse operand bytes",
        "H_retained: retained durable generations whose per-generation manifests and metadata remain live",
        "b_storage_metadata: measured manifest, generation, and filesystem metadata bytes per retained generation, excluding B_receipt"
      ]
    }
  },
  "name": "Stream Sparse Retain Dense",
  "original_constraint_profile": {
    "access_medium": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-STREAM-SPARSE-KEEP-DENSE"
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "text": "The sparse matrix is tiled and streamed from SSD, complete-column dense partitions are loaded into RAM, and dense output may be resident or streamed to SSD.",
      "uncertainty": "Device and buffering behavior affect physical I/O."
    },
    "communication_model": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-STREAM-SPARSE-KEEP-DENSE"
      ],
      "source_pointer_ids": [
        "SP-003"
      ],
      "text": "Workers dynamically claim contiguous tile-row tasks, issue asynchronous sparse reads, compute against the resident dense partition, buffer local output, and asynchronously merge nearby writes.",
      "uncertainty": "The exact interleaving and queue overhead depend on the runtime."
    },
    "constrained_resource": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-STREAM-SPARSE-KEEP-DENSE"
      ],
      "source_pointer_ids": [
        "SP-004"
      ],
      "text": "RAM must hold at least one complete input-dense column and required per-thread sparse-input and output buffers while the sparse matrix remains external.",
      "uncertainty": "The source minimum excludes some allocator, queue, descriptor, and runtime overhead."
    },
    "data_mutability": {
      "assumptions": [
        "No consistency model is inferred from the storage medium."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The mechanism card treats the sparse matrix and dense operands as inputs to a multiplication and does not define concurrent mutation semantics."
      ],
      "text": "UNKNOWN",
      "uncertainty": "Mutable graph snapshots may require rebuilding sparse encodings or snapshot isolation."
    },
    "original_hardware_operating_assumptions": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-STREAM-SPARSE-KEEP-DENSE"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002",
        "SP-003"
      ],
      "text": "The source operating model is a multicore NUMA machine with RAM sufficient for a complete dense column and buffers plus SSD-resident sparse data and asynchronous I/O.",
      "uncertainty": "No Knight Bus device coefficient is inferred from the source's publication year or benchmark machine."
    },
    "predictability_requirement": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-STREAM-SPARSE-KEEP-DENSE"
      ],
      "source_pointer_ids": [
        "SP-004"
      ],
      "text": "The source provides a symbolic minimum resident-state expression and relates the number of resident dense partitions to repeated sparse-input passes under stated capacity assumptions.",
      "uncertainty": "The analytical I/O expression omits some dense-input, output, runtime, and device effects."
    }
  },
  "original_constraints": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-STREAM-SPARSE-KEEP-DENSE"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "The sparse graph matrix and associated dense matrices may not fit together in RAM.",
      "uncertainty": "The source targets SSD-backed multicore execution."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-STREAM-SPARSE-KEEP-DENSE"
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-004"
      ],
      "text": "Vertical partitions must contain complete dense columns, and each additional partition requires another sparse pass.",
      "uncertainty": "Output residency and sparse cache reuse vary by application and capacity."
    }
  ],
  "original_cost_model": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-STREAM-SPARSE-KEEP-DENSE"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-004"
    ],
    "text": "Minimum RAM is one complete dense column plus per-thread sparse and output buffers; repeated sparse-read volume grows with the number of dense column partitions, while persistent state includes the sparse matrix and any external dense operands.",
    "uncertainty": "The source model does not include all whole-process overhead or every dense-input and output byte."
  },
  "original_domain": "semi-external-memory sparse-matrix dense-matrix multiplication for graph analytics",
  "proposed_transfer": {
    "assumptions": [
      "The target computation can be represented as a sparse external operand multiplied by complete dense column partitions."
    ],
    "claim_type": "SPECULATIVE_TRANSFER",
    "premises": [
      "The source defines complete-column dense residency and sparse streaming.",
      "G06 provides the hard fit boundary.",
      "A007 supports algorithm-specific admission and receipts."
    ],
    "text": "For FastRP-style sparse-dense multiplication, PageRank SpMV, and iterative graph linear algebra, make one complete dense column partition the minimum admission unit, stream sparse tiles through bounded worker buffers, choose the maximum admitted dense-column group only from a target-specific symbolic resource model, and refuse or select a separately verified fully external plan when the minimum unit cannot fit.",
    "uncertainty": "Fused operators, mutable graphs, non-SSD devices, and different runtimes require separate falsification."
  },
  "reversed_assumptions": [
    {
      "assumptions": [
        "The implementation exposes component and peak-RSS measurements."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "FAIL-DENSE-COLUMN-EXCEEDS-MEMORY identifies omitted practical overhead above the source terms.",
        "A007 requires whole-process enforcement."
      ],
      "text": "The analytical complete-column minimum is treated as a lower bound that must be augmented by measured allocator, runtime, queue, descriptor, mapped-page, and concurrency headroom before Knight Bus admission.",
      "uncertainty": "Headroom coefficients are runtime- and machine-specific."
    },
    {
      "assumptions": [
        "The target plan retains the complete-column invariant."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The mechanism depends on a streaming external sparse operand.",
        "A007 requires modern I/O accounting rather than historical benchmark ratios."
      ],
      "text": "SSD-specific source scheduling is not assumed to be optimal on the target device; sparse-pass bytes, dense transfers, output writes, and asynchronous queueing are measured separately.",
      "uncertainty": "Storage behavior may reverse the preferred buffer and partition choices."
    }
  ],
  "smallest_falsifier": {
    "controlled_variables": [
      "buffer policy",
      "cache policy",
      "dense element width",
      "resident dense-column count",
      "sparse encoding",
      "storage device",
      "thread count",
      "task-queue cap",
      "page-cache policy"
    ],
    "failure_signal": "Admission accepts below the measured minimum, an accepted run exceeds declared peak RAM, output differs from the oracle, or sparse-pass and dense-output traffic are omitted from the receipt.",
    "fixture": "A sparse matrix and two-column dense operand sized so admitted RAM crosses the measured one-complete-column plus output, worker, descriptor, queue, allocator, and runtime requirement.",
    "independent_oracle": "An in-memory multiplication result plus component-attributed thread-buffer, descriptor, task-queue, page-cache, runtime, allocator, aggregate-RSS, and device-byte counters."
  },
  "source_pattern_ids": [
    "PAT-STREAM-SPARSE-KEEP-DENSE"
  ],
  "surviving_invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-STREAM-SPARSE-KEEP-DENSE"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-004"
    ],
    "text": "At least one complete input-dense column partition and required worker buffers remain resident while sparse tile rows are streamed; each output region is completed locally before its asynchronous write.",
    "uncertainty": "A plan below the complete-column minimum is a different fully external mechanism."
  },
  "target_algorithm_families": [
    "FASTRP_SPMM",
    "ITERATIVE_GRAPH_LINEAR_ALGEBRA",
    "PAGERANK_SPMV"
  ],
  "transfer_id": "XFER-STREAM-SPARSE-RETAIN-DENSE",
  "unknown_measurement_constants": [
    "b_allocator_headroom",
    "b_async_descriptor",
    "b_dense_element",
    "b_storage_metadata",
    "c_sparse_format_convert",
    "c_tile_index_build",
    "c_work_queue_entry",
    "rho_sparse_read_fraction"
  ]
}
```
