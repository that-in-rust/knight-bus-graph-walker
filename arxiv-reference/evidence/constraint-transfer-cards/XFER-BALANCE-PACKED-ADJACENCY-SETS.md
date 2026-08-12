# Balance Packed Adjacency Sets

```json
{
  "analogy_failure_modes": [
    {
      "assumptions": [
        "Both implementations use the same exact set semantics."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "FAIL-WIDE-FIELDS-ERASE-PACKING keeps packed and scalar outputs equal while challenging work."
      ],
      "text": "Exact packed semantics can survive even when wide fields or sparse bucket occupancy erase the intended work advantage, so correctness is not evidence that the optimized branch should be selected.",
      "uncertainty": "The crossover is unmeasured."
    },
    {
      "assumptions": [
        "Knight Bus artifacts may expose wider or non-integer identifiers."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "The source representation operates on fixed-width integer values."
      ],
      "text": "Opaque, variable-width, or unstable graph identifiers do not fit the prefix-and-suffix invariant unless an exact stable integer remapping is included and charged.",
      "uncertainty": "Remapping storage and update cost are target-specific."
    },
    {
      "assumptions": [
        "A system implementation introduces those resources."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "The mechanism card marks device I/O and durable layout unknown."
      ],
      "text": "The word-RAM operation bound does not establish favorable page I/O, SIMD behavior, cache locality, concurrent scratch, or persistent-storage amplification.",
      "uncertainty": "Only target measurements can resolve them."
    }
  ],
  "epistemic_label": "SPECULATIVE_TRANSFER",
  "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-BALANCE-PACKED-ADJACENCY-SETS",
  "g06_challenges": [
    {
      "applies": true,
      "failure_id": "FAIL-WIDE-FIELDS-ERASE-PACKING",
      "response": "Gate the packed path on exact field-capacity checks and measured directory, payload, conversion, scratch, and scalar-baseline work; preserve a scalar exact fallback and refuse any plan whose resource crossover is unresolved under the declared budget."
    }
  ],
  "modern_knight_bus_constraints": [
    {
      "assumptions": [
        "Identifier width, set cardinalities, bucket occupancy, and word width are available before kernel selection."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "The source invariant requires exact bucket and suffix encoding.",
        "G06 shows packing benefit can reverse without breaking correctness."
      ],
      "text": "Admit packed adjacency intersection only when integer encoding is exact, field capacity satisfies the implementation's word constraints, and measured directory, payload, conversion, and output work remain inside the declared resource envelope; otherwise use scalar exact intersection or refusal.",
      "uncertainty": "The target crossover remains a measurement constant."
    },
    {
      "assumptions": [
        "The implementation materializes converted operands or equivalent scratch."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "The source conversion and operation schedule can overlap inputs, converted forms, and result state.",
        "A007 requires temporary-overlap accounting."
      ],
      "text": "Reserve operand directories, packed payloads, converted operands, result payload, compaction scratch, page windows, worker state, and concurrent intersections before execution; do not count the compact payload alone as peak RAM.",
      "uncertainty": "In-place conversion and degree-aware scheduling may reduce overlap but require proof."
    },
    {
      "assumptions": [
        "The kernel exposes representation and operation counters."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "The G06 failure boundary depends on field width and directory-versus-payload work."
      ],
      "text": "The receipt must report identifier and suffix widths, bucket count, nonempty buckets, directory and payload bytes, conversion bytes, packed and scalar work counters, peak scratch, output cardinality, checksum, and selected fallback.",
      "uncertainty": "Elapsed time is not predicted by this transfer."
    }
  ],
  "modern_resource_model": {
    "concurrency": {
      "assumptions": [
        "Concurrency admission bounds both operations and workers before buffers are allocated."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "CONCURRENCY_peak_bytes = C_intersections*(B_operand_window + B_result_window) + C_workers*B_worker_scratch + c_scheduler_metadata*(C_intersections + C_workers)",
      "measurement_needed": "Measure pinned operand pages, result buffers, private compaction scratch, queue metadata, skew, and peak RSS across concurrent intersections.",
      "uncertainty": "Shared operands and bucket-level parallelism may reduce or amplify aggregate state.",
      "unknown_constants": [
        "c_scheduler_metadata"
      ],
      "variables": [
        "B_operand_window: directory and payload bytes pinned per active intersection",
        "B_result_window: result bytes buffered per active intersection",
        "B_worker_scratch: per-worker merge and compaction scratch bytes",
        "C_intersections: concurrent set operations",
        "C_workers: workers assigned across operations"
      ]
    },
    "io": {
      "assumptions": [
        "A persistent or mapped implementation preserves bucket-wise exact access."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "IO_total_bytes = Q_directory_pages*P_page + rho_payload_page_read_fraction*Q_payload_pages*P_page + Q_conversion_write_pages*P_page",
      "measurement_needed": "Measure cold and warm directory, payload, conversion, result, cache, and readahead bytes for each operand shape and storage layout.",
      "uncertainty": "The source provides no block layout, so every physical-I/O coefficient is target-specific.",
      "unknown_constants": [
        "rho_payload_page_read_fraction"
      ],
      "variables": [
        "P_page: target storage page bytes",
        "Q_conversion_write_pages: pages written for converted or rebalanced sets",
        "Q_directory_pages: bucket-directory pages read",
        "Q_payload_pages: packed-payload pages logically visited"
      ]
    },
    "preprocessing": {
      "assumptions": [
        "Conversion cost is charged per operation unless a valid reusable representation is retained."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "PREP_work_units = (N_left + N_right)*c_bucket_convert + U_buckets*c_directory_probe",
      "measurement_needed": "Measure identifier remapping, bucket conversion, compaction, bytes copied, time, peak scratch, update invalidation, and reuse count.",
      "uncertainty": "Mutation and asymmetric operand sizes determine amortization.",
      "unknown_constants": [
        "c_bucket_convert",
        "c_directory_probe"
      ],
      "variables": [
        "N_left: left operand cardinality",
        "N_right: right operand cardinality",
        "PREP_work_units: implementation-defined conversion work units",
        "U_buckets: bucket entries examined or created"
      ]
    },
    "ram": {
      "assumptions": [
        "The result bound and conversion schedule are declared before execution."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "RAM_peak_bytes = B_shared_representation + C_intersections*(B_operand_window + B_result_window) + C_workers*B_worker_scratch + B_conversion_scratch + c_scheduler_metadata*(C_intersections + C_workers)",
      "measurement_needed": "Measure shared packed representation, per-intersection operand/result windows, per-worker scratch, conversion scratch, scheduler metadata, and aggregate RSS across field widths and occupancy distributions.",
      "uncertainty": "In-place reuse, alignment, test bits, and allocator slabs change physical bytes.",
      "unknown_constants": [
        "c_scheduler_metadata"
      ],
      "variables": [
        "B_operand_window: disjoint directory and payload bytes pinned per active intersection",
        "B_result_window: disjoint result bytes buffered per active intersection",
        "B_shared_representation: packed-set representation bytes shared across active intersections",
        "B_conversion_scratch: peak repartition, merge, mask, and compaction scratch bytes",
        "B_worker_scratch: disjoint merge and compaction bytes per worker",
        "C_intersections: simultaneously active set intersections",
        "C_workers: workers assigned across active intersections"
      ]
    },
    "storage": {
      "assumptions": [
        "The same identifier encoding and bucket parameters are recoverable from durable metadata."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "STORAGE_peak_bytes = B_source_sets_retained + G_packed_retained*(U_buckets*b_directory_entry + N_items*(F_suffix + b_field_overhead)/B_bits_per_byte + H_sets*b_persistent_header) + B_conversion_generation_peak",
      "measurement_needed": "Measure retained exact source/scalar fallback, every retained packed generation, conversion/replacement allocation high-water, and generation creation/deletion timing.",
      "uncertainty": "No source serialization or durability protocol exists.",
      "unknown_constants": [
        "b_directory_entry",
        "b_field_overhead",
        "b_persistent_header"
      ],
      "variables": [
        "B_conversion_generation_peak: incremental conversion or replacement bytes beyond retained source and packed generations",
        "B_source_sets_retained: retained exact source or scalar-fallback bytes shared across packed generations",
        "B_bits_per_byte: fixed bit-to-byte unit conversion",
        "F_suffix: encoded suffix bits per item",
        "G_packed_retained: packed generations simultaneously retained at storage peak",
        "H_sets: persisted set-record count",
        "N_items: total persisted adjacency identifiers",
        "U_buckets: total persisted directory entries"
      ]
    }
  },
  "name": "Balance Packed Adjacency Sets",
  "original_constraint_profile": {
    "access_medium": {
      "assumptions": [
        "No storage tier is inferred from the source date."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source defines word-RAM packed arrays and bucket directories but no durable serialization, page layout, or device I/O model."
      ],
      "text": "UNKNOWN",
      "uncertainty": "A Knight Bus adaptation may retain, map, or stream bucket data."
    },
    "communication_model": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BALANCE-BUCKETED-PACKED-SETS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "text": "The operation converts both operands to a shared bucket parameter, processes corresponding buckets with packed merge, masking, duplicate handling, and compaction, then rebalances the exact result.",
      "uncertainty": "The source does not specify parallel or device-level communication."
    },
    "constrained_resource": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BALANCE-BUCKETED-PACKED-SETS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "The representation balances bucket-directory space against packed suffix payload and conditions word-parallel operations on field capacity relative to the machine word.",
      "uncertainty": "Allocator, cache, persistent layout, and graph-kernel constants are outside the analytical model."
    },
    "data_mutability": {
      "assumptions": [
        "Operand sets are immutable during one operation."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The mechanism card defines conversions and set operations but no concurrent update, snapshot, or incremental rebalance protocol."
      ],
      "text": "UNKNOWN",
      "uncertainty": "Mutable adjacency may make conversion and rebalancing recurrent."
    },
    "original_hardware_operating_assumptions": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BALANCE-BUCKETED-PACKED-SETS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "The analytical mechanism assumes fixed-width machine words that can hold multiple encoded fields and support packed merge, masking, and compaction operations.",
      "uncertainty": "No cache, SIMD, allocator, or persistent-storage behavior is established."
    },
    "predictability_requirement": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BALANCE-BUCKETED-PACKED-SETS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002",
        "SP-003"
      ],
      "text": "Exact union and intersection follow from matching disjoint prefix buckets and exact packed suffix operations, while operation bounds depend on field width, word width, and the chosen bucket balance.",
      "uncertainty": "The source supplies asymptotic operations rather than a target-machine crossover."
    }
  },
  "original_constraints": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BALANCE-BUCKETED-PACKED-SETS"
      ],
      "source_pointer_ids": [
        "SP-002"
      ],
      "text": "A naive small-universe set representation can spend too much space on bucket pointers or per-element fields and lose packed-word utility.",
      "uncertainty": "The source states analytical conditions rather than a measured hardware breakpoint."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BALANCE-BUCKETED-PACKED-SETS"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "Elements must be integer encodings whose suffix fields satisfy the word-capacity condition and whose corresponding buckets can be processed independently.",
      "uncertainty": "Graph identifier remapping and degree skew are not evaluated."
    }
  ],
  "original_cost_model": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-BALANCE-BUCKETED-PACKED-SETS"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-003"
    ],
    "text": "Resident space is bucket-directory words plus packed suffix fields; conversion repartitions and compacts operands before per-bucket operations; operation work depends symbolically on set cardinality, suffix field width, and machine word width.",
    "uncertainty": "Persistent storage, external I/O, allocator overhead, SIMD realization, and peak temporary overlap are not supplied."
  },
  "original_domain": "word-parallel compact set representation",
  "proposed_transfer": {
    "assumptions": [
      "Adjacency identifiers have an exact stable integer encoding and the algorithm's set semantics are exact union or intersection."
    ],
    "claim_type": "SPECULATIVE_TRANSFER",
    "premises": [
      "The source provides exact balanced bucketed set operations.",
      "G06 preserves correctness while challenging the resource advantage.",
      "A007 can select a kernel branch and receipt its representation terms."
    ],
    "text": "For triangle, clustering-coefficient, and node-similarity kernels over sorted integer adjacency sets, partition identifiers by stable prefixes, pack exact suffixes, convert both operands to a measured shared balance point, perform exact bucket-wise intersection, and fall back to scalar exact set operations when field capacity, directory work, conversion work, or peak scratch violates the admitted envelope.",
    "uncertainty": "Graph degree skew, persistent block layout, cache behavior, SIMD realization, and reusable conversion are unmeasured."
  },
  "reversed_assumptions": [
    {
      "assumptions": [
        "A scalar exact implementation provides a correctness and work baseline."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "FAIL-WIDE-FIELDS-ERASE-PACKING identifies a symbolic reversal when fields are wide or directory work dominates.",
        "The source reports no hardware-level crossover."
      ],
      "text": "The analytical packing benefit is not assumed to survive modern identifiers or hardware; encoded field width, fields per item, directory work, payload work, conversion work, and scalar fallback work are measured on the target implementation.",
      "uncertainty": "The crossover depends on graph, encoding, implementation, and machine."
    },
    {
      "assumptions": [
        "Knight Bus operands may arrive with different cardinalities or bucket parameters."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source algorithm converts operands to a common balance point and may rebalance the result."
      ],
      "text": "Bucket conversion is treated as explicit preprocessing or per-operation temporary work rather than a free representation change.",
      "uncertainty": "Reuse and mutation determine whether conversion can be amortized."
    }
  ],
  "smallest_falsifier": {
    "controlled_variables": [
      "bucket parameter",
      "bucket occupancy distribution",
      "concurrency",
      "encoded field width",
      "identifier width",
      "operation sequence",
      "word width",
      "worker count",
      "retained packed generations"
    ],
    "failure_signal": "Packed output differs, conversion loses an element, aggregate concurrent RSS exceeds the composed shared/per-intersection/per-worker bound, or conversion/replacement allocation exceeds STORAGE_peak_bytes before generation cleanup.",
    "fixture": "Two exact adjacency-set pairs with fixed cardinalities and contents, swept across suffix field widths and bucket occupancy distributions, including a scalar exact implementation and packed conversion plus intersection.",
    "independent_oracle": "Scalar exact intersections with element equality, aggregate concurrent RSS attribution, and filesystem allocation plus generation creation/deletion traces through conversion."
  },
  "source_pattern_ids": [
    "PAT-BALANCE-BUCKETED-PACKED-SETS"
  ],
  "surviving_invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-BALANCE-BUCKETED-PACKED-SETS"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-003"
    ],
    "text": "Bucket prefixes partition the integer universe into disjoint ranges and each bucket stores exact suffixes, so converting operands to a shared parameter and applying exact packed operations bucket by bucket reconstructs exact union or intersection.",
    "uncertainty": "The invariant establishes exact set semantics, not a modern speed or memory advantage."
  },
  "target_algorithm_families": [
    "CLUSTERING_COEFFICIENT",
    "NODE_SIMILARITY_SET_INTERSECTION",
    "TRIANGLE_COUNTING"
  ],
  "transfer_id": "XFER-BALANCE-PACKED-ADJACENCY-SETS",
  "unknown_measurement_constants": [
    "b_directory_entry",
    "b_field_overhead",
    "b_persistent_header",
    "c_bucket_convert",
    "c_directory_probe",
    "c_scheduler_metadata",
    "rho_payload_page_read_fraction"
  ]
}
```
