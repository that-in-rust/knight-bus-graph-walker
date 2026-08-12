# Elide Derivable Edge Values

```json
{
  "analogy_failure_modes": [
    {
      "assumptions": [
        "The explicit-value oracle observes the same arithmetic."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "FAIL-WEIGHTED-EDGES-NEED-VALUES breakpoint_equation",
        "PAT-INFER-UNWEIGHTED-EDGE-VALUES fails_when"
      ],
      "text": "The analogy fails whenever at least one operative edge value differs from the value regenerated from topology and the selected algebra.",
      "uncertainty": "Other exact restricted-value encodings are outside this transfer."
    },
    {
      "assumptions": [
        "Marker and complete-working-set checks run before execution."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-INFER-UNWEIGHTED-EDGE-VALUES invariant",
        "PAT-INFER-UNWEIGHTED-EDGE-VALUES a007_consequence"
      ],
      "text": "The analogy fails when the padding marker collides with a valid identifier or when padding, metadata, algorithm vectors, or kernel state erase the admitted resource fit.",
      "uncertainty": "Target container and kernel constants are unmeasured."
    }
  ],
  "epistemic_label": "SPECULATIVE_TRANSFER",
  "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-ELIDE-DERIVABLE-EDGE-VALUES",
  "g06_challenges": [
    {
      "applies": true,
      "failure_id": "FAIL-WEIGHTED-EDGES-NEED-VALUES",
      "response": "Applies. The transfer requires a semantic proof that the selected operation cannot observe distinct per-edge values; weighted or attribute-sensitive jobs retain explicit values through an exact fallback or are refused for the value-elision branch."
    }
  ],
  "modern_knight_bus_constraints": [
    {
      "assumptions": [
        "Edge properties used by the algorithm are declared."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "FAIL-WEIGHTED-EDGES-NEED-VALUES repair_options",
        "PAT-INFER-UNWEIGHTED-EDGE-VALUES a007_consequence"
      ],
      "text": "Admission must emit a machine-checkable semantic proof that edge presence and the selected operation determine every operative edge value; otherwise it retains explicit values or refuses the value-elision branch.",
      "uncertainty": "The proof system and supported semiring registry are not selected in G07."
    },
    {
      "assumptions": [
        "The representation and algorithm versions are fixed in the manifest."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "A007 receipt contract",
        "FAIL-WEIGHTED-EDGES-NEED-VALUES expected_failure_signal"
      ],
      "text": "The receipt must record the value-elision proof identifier, marker domain, edge/value schema checksum, padding, metadata, algorithm vectors, explicit-value fallback state, and differential-oracle status.",
      "uncertainty": "Kernel register and spill state may require lower-level counters."
    }
  ],
  "modern_resource_model": {
    "concurrency": {
      "assumptions": [
        "Worker count is bounded.",
        "Shared algorithm vectors are accounted separately in RAM."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Concurrency_state_bytes = W_workers * (B_column_vector_bytes + B_marker_mask_bytes + B_synthesized_value_vector_bytes + B_gather_state_bytes) + c_scheduler_state_bytes",
      "measurement_needed": "Inspect generated kernels or measure worker-local spills, masks, gathers, queues, and scheduler state.",
      "uncertainty": "Compiler vectorization, register spills, and device execution models vary.",
      "unknown_constants": [
        "c_scheduler_state_bytes"
      ],
      "variables": [
        {
          "definition": "worker-local vectorized kernel state",
          "symbol": "Concurrency_state_bytes",
          "units": "bytes"
        },
        {
          "definition": "kernel workers",
          "symbol": "W_workers",
          "units": "workers"
        },
        {
          "definition": "loaded column vector state",
          "symbol": "B_column_vector_bytes",
          "units": "bytes per worker"
        },
        {
          "definition": "padding comparison mask state",
          "symbol": "B_marker_mask_bytes",
          "units": "bytes per worker"
        },
        {
          "definition": "generated semiring values",
          "symbol": "B_synthesized_value_vector_bytes",
          "units": "bytes per worker"
        },
        {
          "definition": "frontier gather state",
          "symbol": "B_gather_state_bytes",
          "units": "bytes per worker"
        }
      ]
    },
    "io": {
      "assumptions": [
        "External storage is a speculative Knight Bus placement, not a source result.",
        "Both branches execute the same algebra."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "IO_total_bytes = c_physical_read_amplification * I_scans * (B_topology_bytes + B_padding_bytes + B_chunk_metadata_bytes + Q_explicit_values * B_edge_values_bytes)",
      "measurement_needed": "Measure logical memory traffic and external physical I/O separately for both branches and declared cache states.",
      "uncertainty": "The source evaluates memory-resident kernels and provides no external-I/O bound.",
      "unknown_constants": [
        "c_physical_read_amplification"
      ],
      "variables": [
        {
          "definition": "physical representation scan bytes",
          "symbol": "IO_total_bytes",
          "units": "bytes"
        },
        {
          "definition": "complete representation scans",
          "symbol": "I_scans",
          "units": "scans"
        },
        {
          "definition": "topology bytes per scan",
          "symbol": "B_topology_bytes",
          "units": "bytes per scan"
        },
        {
          "definition": "padding bytes per scan",
          "symbol": "B_padding_bytes",
          "units": "bytes per scan"
        },
        {
          "definition": "chunk metadata bytes per scan",
          "symbol": "B_chunk_metadata_bytes",
          "units": "bytes per scan"
        },
        {
          "definition": "explicit-value branch selector",
          "symbol": "Q_explicit_values",
          "units": "dimensionless"
        },
        {
          "definition": "value bytes per scan",
          "symbol": "B_edge_values_bytes",
          "units": "bytes per scan"
        }
      ]
    },
    "preprocessing": {
      "assumptions": [
        "Semantic validation inspects every operative edge property required by the algorithm.",
        "The logarithmic sort term is a planning model."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "T_prepare = c_chunk_build_time_per_edge * M_edges + c_sort_time_per_vertex_log_vertex * N_vertices * log2(N_vertices + 1) + c_semantic_validation_time_per_edge * M_edges",
      "measurement_needed": "Measure chunk construction, optional row sorting, marker validation, schema proof, and branch-specific output writes.",
      "uncertainty": "Sorting scope, schema-validation implementation, and update history change preparation cost.",
      "unknown_constants": [
        "c_chunk_build_time_per_edge",
        "c_semantic_validation_time_per_edge",
        "c_sort_time_per_vertex_log_vertex"
      ],
      "variables": [
        {
          "definition": "format build plus semantic validation time",
          "symbol": "T_prepare",
          "units": "time"
        },
        {
          "definition": "edges represented",
          "symbol": "M_edges",
          "units": "edges"
        },
        {
          "definition": "vertices represented",
          "symbol": "N_vertices",
          "units": "vertices"
        }
      ]
    },
    "ram": {
      "assumptions": [
        "Q_explicit_values is derived from the semantic guard.",
        "Algorithm vectors are never omitted from admission.",
        "Shared B_algorithm_state_bytes excludes every per-worker column, marker, synthesized-value, and gather component."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "RAM_peak_bytes = B_topology_bytes + B_padding_bytes + B_chunk_metadata_bytes + B_algorithm_state_bytes + Q_explicit_values*B_edge_values_bytes + W_workers*(B_column_vector_bytes + B_marker_mask_bytes + B_synthesized_value_vector_bytes + B_gather_state_bytes) + c_scheduler_state_bytes + c_allocator_overhead_bytes + c_kernel_spill_bytes",
      "measurement_needed": "Measure every shared and per-worker resident component, scheduler state, allocator overhead, generated-kernel spills, and aggregate charged memory for both value-free and explicit-value branches.",
      "uncertainty": "Compiler register allocation and runtime libraries can add unmodeled state.",
      "unknown_constants": [
        "c_allocator_overhead_bytes",
        "c_kernel_spill_bytes",
        "c_scheduler_state_bytes"
      ],
      "variables": [
        {
          "definition": "maximum charged resident bytes",
          "symbol": "RAM_peak_bytes",
          "units": "bytes"
        },
        {
          "definition": "stored edge identifiers or topology bytes",
          "symbol": "B_topology_bytes",
          "units": "bytes"
        },
        {
          "definition": "padding-marker cells",
          "symbol": "B_padding_bytes",
          "units": "bytes"
        },
        {
          "definition": "chunk starts and lengths",
          "symbol": "B_chunk_metadata_bytes",
          "units": "bytes"
        },
        {
          "definition": "frontier, output, filter, parent, and related vectors",
          "symbol": "B_algorithm_state_bytes",
          "units": "bytes"
        },
        {
          "definition": "zero only when semantic proof permits elision, otherwise one",
          "symbol": "Q_explicit_values",
          "units": "dimensionless"
        },
        {
          "definition": "explicit edge-value array bytes",
          "symbol": "B_edge_values_bytes",
          "units": "bytes"
        },
        {
          "definition": "kernel workers",
          "symbol": "W_workers",
          "units": "workers"
        },
        {
          "definition": "loaded column vector state per worker",
          "symbol": "B_column_vector_bytes",
          "units": "bytes per worker"
        },
        {
          "definition": "padding comparison mask state per worker",
          "symbol": "B_marker_mask_bytes",
          "units": "bytes per worker"
        },
        {
          "definition": "generated semiring value state per worker",
          "symbol": "B_synthesized_value_vector_bytes",
          "units": "bytes per worker"
        },
        {
          "definition": "frontier gather state per worker",
          "symbol": "B_gather_state_bytes",
          "units": "bytes per worker"
        }
      ]
    },
    "storage": {
      "assumptions": [
        "The marker domain is disjoint from valid identifiers.",
        "Q_explicit_values is one whenever proof fails."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Storage_total_bytes = B_topology_bytes + B_padding_bytes + B_chunk_metadata_bytes + Q_explicit_values * B_edge_values_bytes + c_container_overhead_bytes",
      "measurement_needed": "Measure serialized topology, padding, metadata, optional values, alignment, headers, and filesystem allocation.",
      "uncertainty": "Container and alignment overhead can reduce the material effect of value elision.",
      "unknown_constants": [
        "c_container_overhead_bytes"
      ],
      "variables": [
        {
          "definition": "complete persistent representation bytes",
          "symbol": "Storage_total_bytes",
          "units": "bytes"
        },
        {
          "definition": "stored topology bytes",
          "symbol": "B_topology_bytes",
          "units": "bytes"
        },
        {
          "definition": "stored padding bytes",
          "symbol": "B_padding_bytes",
          "units": "bytes"
        },
        {
          "definition": "chunk starts, lengths, and headers",
          "symbol": "B_chunk_metadata_bytes",
          "units": "bytes"
        },
        {
          "definition": "explicit-value branch selector",
          "symbol": "Q_explicit_values",
          "units": "dimensionless"
        },
        {
          "definition": "explicit edge-value bytes",
          "symbol": "B_edge_values_bytes",
          "units": "bytes"
        }
      ]
    }
  },
  "name": "Elide Derivable Edge Values",
  "original_constraint_profile": {
    "access_medium": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-INFER-UNWEIGHTED-EDGE-VALUES"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "The evaluated kernel streams chunked column entries from memory into vector operations and synthesizes edge values and padding behavior in registers.",
      "uncertainty": "External-storage I/O is not established by the source."
    },
    "communication_model": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-INFER-UNWEIGHTED-EDGE-VALUES"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "Column identifiers are loaded from memory, compared against a padding marker, and combined with gathered frontier entries inside a local vectorized sparse kernel.",
      "uncertainty": "Distributed communication and external I/O are outside the evaluated mechanism."
    },
    "constrained_resource": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-INFER-UNWEIGHTED-EDGE-VALUES"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002",
        "SP-003"
      ],
      "text": "The source mechanism reduces memory footprint and memory transfer by omitting a per-edge value array whose entries are determined by unweighted edge presence.",
      "uncertainty": "Padding, chunk metadata, frontier vectors, and runtime state remain."
    },
    "data_mutability": {
      "assumptions": [
        "No publication-year inference or unstated source assumption is used."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The mechanism card does not establish update semantics for the chunked representation, padding, or sorted row groups."
      ],
      "text": "UNKNOWN",
      "uncertainty": "Topology changes may alter chunk padding and preprocessing cost."
    },
    "original_hardware_operating_assumptions": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-INFER-UNWEIGHTED-EDGE-VALUES"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002",
        "SP-005"
      ],
      "text": "The mechanism assumes vector or SIMD execution, a reserved noncolliding padding marker, and a sparse layout whose value stream can be regenerated for the selected semiring.",
      "uncertainty": "Vector width, register allocation, gather cost, and spill behavior are architecture-dependent."
    },
    "predictability_requirement": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-INFER-UNWEIGHTED-EDGE-VALUES"
      ],
      "source_pointer_ids": [
        "SP-003"
      ],
      "text": "The source provides a symbolic representation-size expression in terms of edges, vertices, chunk height, and padding cells.",
      "uncertainty": "Concrete bytes still require cell width, edge convention, padding, and all algorithm vectors."
    }
  },
  "original_constraints": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-INFER-UNWEIGHTED-EDGE-VALUES"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "Every real adjacency must have the exact implicit value generated by the selected semiring, and the reserved marker must denote padding rather than a valid vertex.",
      "uncertainty": "A marker collision or value-sensitive operation breaks correctness."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-INFER-UNWEIGHTED-EDGE-VALUES"
      ],
      "source_pointer_ids": [
        "SP-003",
        "SP-004"
      ],
      "text": "Padding, chunk starts, chunk lengths, preprocessing, frontier vectors, and algorithm-specific filter or parent vectors remain part of the lifecycle cost.",
      "uncertainty": "The source representation expression is not a whole-process estimator."
    }
  ],
  "original_cost_model": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-INFER-UNWEIGHTED-EDGE-VALUES"
    ],
    "source_pointer_ids": [
      "SP-003"
    ],
    "text": "The source SlimSell representation uses 2m + 2n/C + P cells in its notation while omitting the per-entry value array; BFS vectors and runtime overhead are excluded.",
    "uncertainty": "Cell width, graph-edge convention, padding P, and complete process state are required for bytes."
  },
  "original_domain": "SIMD-oriented algebraic breadth-first search",
  "proposed_transfer": {
    "assumptions": [
      "An explicit-value exact representation and oracle are available."
    ],
    "claim_type": "SPECULATIVE_TRANSFER",
    "premises": [
      "PAT-INFER-UNWEIGHTED-EDGE-VALUES invariant",
      "FAIL-WEIGHTED-EDGES-NEED-VALUES breakpoint_equation",
      "A007 fit/spill/refuse contract"
    ],
    "text": "Make per-edge value elision a semantics-gated representation branch: omit only fields that the admitted algorithm can regenerate exactly from retained topology and metadata, prove marker noncollision, and fall back to an explicit-value kernel whenever any operative edge value is not derivable.",
    "uncertainty": "The transfer does not generalize to arbitrary weighted graphs or establish a faster kernel."
  },
  "reversed_assumptions": [
    {
      "assumptions": [
        "The schema and algorithm contract are available before representation selection."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "PAT-INFER-UNWEIGHTED-EDGE-VALUES invariant",
        "FAIL-WEIGHTED-EDGES-NEED-VALUES breakpoint_equation"
      ],
      "text": "Unweighted graph metadata alone is no longer accepted as sufficient proof; the admitted algorithm, semiring, edge schema, property projection, and marker domain must jointly prove that every omitted value is exactly regenerable.",
      "uncertainty": "Restricted weighted domains may admit other exact encodings not covered here."
    },
    {
      "assumptions": [
        "The runtime can select between value-free and explicit-value exact kernels."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "PAT-INFER-UNWEIGHTED-EDGE-VALUES a007_consequence",
        "PAT-INFER-UNWEIGHTED-EDGE-VALUES resource_model.io",
        "A007 full-working-set requirement"
      ],
      "text": "The source's in-memory transfer reduction is not imported as external-I/O or whole-process savings; modern admission includes topology, padding, metadata, algorithm vectors, kernel state, preprocessing, and explicit-value fallback.",
      "uncertainty": "External read behavior and target kernel coefficients remain unknown."
    }
  ],
  "smallest_falsifier": {
    "controlled_variables": [
      "chunk_height",
      "padding_marker",
      "semiring",
      "topology",
      "vertex_ordering",
      "worker count",
      "kernel spill policy"
    ],
    "failure_signal": "The semantic guard permits elision when the explicit-value result differs, a padding marker is accepted as an edge, or the value-free branch exceeds its declared complete resource bound.",
    "fixture": "The same minimal topology represented once with unequal operative edge values and once as an unweighted control, including at least one padding marker in the chunk layout.",
    "independent_oracle": "An explicit-value CSR or equivalent sparse kernel using identical topology and arithmetic, plus component-attributed shared vectors, per-worker column/mask/synthesized/gather state, scheduler, spill, allocator, and aggregate charged-memory counters."
  },
  "source_pattern_ids": [
    "PAT-INFER-UNWEIGHTED-EDGE-VALUES"
  ],
  "surviving_invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-INFER-UNWEIGHTED-EDGE-VALUES"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A stored edge-value field is exactly redundant when every real adjacency's operative value is deterministically implied by retained topology and the selected algebra, while padding remains distinguishable by a noncolliding marker.",
    "uncertainty": "The invariant is semantic and does not apply to distinct operative edge values."
  },
  "target_algorithm_families": [
    "BFS_SHORTEST_PATHS",
    "UNWEIGHTED_GRAPHBLAS_TRAVERSAL"
  ],
  "transfer_id": "XFER-ELIDE-DERIVABLE-EDGE-VALUES",
  "unknown_measurement_constants": [
    "c_allocator_overhead_bytes",
    "c_chunk_build_time_per_edge",
    "c_container_overhead_bytes",
    "c_kernel_spill_bytes",
    "c_physical_read_amplification",
    "c_scheduler_state_bytes",
    "c_semantic_validation_time_per_edge",
    "c_sort_time_per_vertex_log_vertex"
  ]
}
```
