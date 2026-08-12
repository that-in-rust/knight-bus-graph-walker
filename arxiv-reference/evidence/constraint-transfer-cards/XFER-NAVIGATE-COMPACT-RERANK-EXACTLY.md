# Navigate Compact Rerank Exactly

```json
{
  "analogy_failure_modes": [
    {
      "assumptions": [
        "The target compact metric retains the source mechanism's sign and relative-magnitude dependency."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "PAT-NAVIGATE-BINARY-RERANK-EXACTLY source pointers SP-003 and SP-004 report the geometry-dependent applicability boundary and exact-rerank limitation."
      ],
      "text": "Euclidean-native or structureless distributions can lack the sign-direction and relative-magnitude signal needed for compact navigation, leaving exact neighbors outside the rerank set.",
      "uncertainty": "Intermediate and multimodal distributions form a compatibility gradient."
    },
    {
      "assumptions": [
        "No independent complete scan augments the candidate set."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "Reranking operates only on the candidate set.",
        "G06 records recall collapse despite exact reranking."
      ],
      "text": "Exact reranking is not an exact-nearest-neighbor guarantee when compact navigation fails to retain the true neighbor.",
      "uncertainty": "A wider beam may improve reachability but can violate the admitted resource envelope."
    },
    {
      "assumptions": [
        "No separate invariant has been established for other graph families."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "The source mechanism and failure are both metric-geometry specific."
      ],
      "text": "Compact vector signatures are not analogous to compressed graph state for non-vector algorithms because their invariant depends on embedding geometry and a full-precision similarity oracle.",
      "uncertainty": "Other compact-guidance mechanisms require independent transfer cards."
    }
  ],
  "epistemic_label": "SPECULATIVE_TRANSFER",
  "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-NAVIGATE-COMPACT-RERANK-EXACTLY",
  "g06_challenges": [
    {
      "applies": true,
      "failure_id": "FAIL-INCOMPATIBLE-GEOMETRY-COLLAPSES-RECALL",
      "response": "Require a target-distribution compatibility probe and explicit candidate-recall contract before compact-native topology is admitted; monitor drift and route incompatible or unproven distributions to float or alternative quantized navigation, or refuse."
    }
  ],
  "modern_knight_bus_constraints": [
    {
      "assumptions": [
        "The exact similarity oracle and target quality threshold are supplied outside this transfer."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "The surviving invariant is candidate-region reachability.",
        "G06 retains no portable geometry cutoff."
      ],
      "text": "Select compact-native navigation only after a target-distribution probe measures candidate recall before reranking, exact recall after reranking, path preservation, and candidate cost under the requested quality contract.",
      "uncertainty": "A sampled probe may itself require drift monitoring and later falsification."
    },
    {
      "assumptions": [
        "A separately verified fallback is available if this transfer is admitted."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "A007 requires full-working-set admission and a bounded branch.",
        "G06 recommends a fallback for incompatible geometry."
      ],
      "text": "Reserve hot signature and adjacency RAM, private beam and visited state, cold-vector storage, exact-rerank reads, build scratch, and concurrent-query buffers before execution; use a float or alternative quantized navigation fallback or refuse when the compact plan misses its envelope.",
      "uncertainty": "Fallback resource coefficients are outside this card."
    },
    {
      "assumptions": [
        "Quality evaluation is permitted on the verification fixture or audited sample."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "Approximate guidance and exact reranking have different failure boundaries."
      ],
      "text": "The receipt must report the compact metric and encoding, probe identity, candidate budget, pre-rerank candidate recall where an oracle is available, exact-vector pages, final quality, peak hot state, and any fallback or refusal.",
      "uncertainty": "Production queries without ground truth may expose only proxy and resource counters."
    }
  ],
  "modern_resource_model": {
    "concurrency": {
      "assumptions": [
        "Each query reserves its worst admitted private state before execution."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "CONCURRENCY_peak_bytes = C_queries*(B_beam + B_visited + B_query_private) + C_cold_reads*B_io_request + c_scheduler_metadata*(C_queries + C_cold_reads)",
      "measurement_needed": "Measure private beam, visited, rerank, queue, and I/O descriptor state and storage contention at each admitted concurrency.",
      "uncertainty": "Shared visited structures, batched reads, and cache reuse may alter additivity.",
      "unknown_constants": [
        "c_scheduler_metadata"
      ],
      "variables": [
        "B_beam: private compact-navigation beam bytes per admitted query",
        "B_io_request: bytes per in-flight cold-vector request",
        "B_query_private: private exact-vector rerank and queue bytes per admitted query",
        "B_visited: private compact-navigation visited-state bytes per admitted query",
        "C_cold_reads: in-flight cold reads",
        "C_queries: concurrent admitted queries"
      ]
    },
    "io": {
      "assumptions": [
        "Compact navigation itself performs no original-vector reads."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "IO_query_bytes = K_rerank*rho_candidate_page_uniqueness*P_vector_record + Q_metadata_pages*P_metadata_page",
      "measurement_needed": "Measure unique cold pages, bytes read, cache hits, readahead, and candidate co-residency for each rerank budget and concurrency.",
      "uncertainty": "Page sharing and cache state can make bytes non-linear in candidate count.",
      "unknown_constants": [
        "rho_candidate_page_uniqueness"
      ],
      "variables": [
        "K_rerank: retained candidates scored with original vectors",
        "P_metadata_page: bytes per cold metadata page",
        "P_vector_record: bytes per full-precision vector record or containing page",
        "Q_metadata_pages: cold metadata pages read during finalization"
      ]
    },
    "preprocessing": {
      "assumptions": [
        "Probe and index-build costs are charged to an explicit reuse horizon and rerun after material distribution drift."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "PREP_work_units = N_vectors*D_dimensions*c_bq_encode + N_vectors*c_graph_construct + S_probe*D_dimensions*c_geometry_probe",
      "measurement_needed": "Measure encoding, graph construction, probe duration, peak build RSS, and bytes written on the target embedding distribution.",
      "uncertainty": "Construction concurrency, update strategy, and representative probe size are unknown.",
      "unknown_constants": [
        "c_bq_encode",
        "c_geometry_probe",
        "c_graph_construct"
      ],
      "variables": [
        "D_dimensions: vector dimension count",
        "N_vectors: indexed vector count",
        "PREP_work_units: implementation-defined build and probe work units",
        "S_probe: vectors or queries in the compatibility probe"
      ]
    },
    "ram": {
      "assumptions": [
        "Exact vectors are counted as cold mapped or page-cache state when physically resident at peak."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "RAM_peak_bytes = N_vectors*(D_dimensions*b_bq_per_dimension + R_degree*b_adjacency_entry) + B_exact_vector_cache_peak + C_queries*(B_beam + B_visited + B_query_private) + C_cold_reads*B_io_request + c_scheduler_metadata*(C_queries + C_cold_reads) + b_runtime_overhead",
      "measurement_needed": "Measure compact-index RSS, exact-vector resident-page high-water, private query state, in-flight cold-read descriptors/buffers, scheduler metadata, and runtime across cache, concurrency, and queue-depth controls.",
      "uncertainty": "Visited representation, graph metadata, page cache, and runtime overhead are implementation-specific.",
      "unknown_constants": [
        "b_adjacency_entry",
        "b_bq_per_dimension",
        "b_runtime_overhead",
        "c_scheduler_metadata"
      ],
      "variables": [
        "B_beam: active compact-distance beam bytes per admitted query",
        "B_exact_vector_cache_peak: shared exact-vector pages physically resident at peak, excluding compact hot-index and per-query rerank buffers",
        "B_io_request: disjoint descriptor and buffer bytes per in-flight exact-vector request",
        "B_query_private: per-query rerank and queue bytes",
        "B_visited: visited-state bytes per admitted query",
        "C_queries: concurrent admitted queries",
        "C_cold_reads: in-flight exact-vector reads across admitted queries",
        "D_dimensions: vector dimension count",
        "N_vectors: indexed vector count",
        "R_degree: stored adjacency entries per vector"
      ]
    },
    "storage": {
      "assumptions": [
        "Original vectors are retained because exact reranking depends on them.",
        "The exact-vector payload is shared across compact-index generations and is not multiplied by G_compact_retained."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "STORAGE_peak_bytes = N_vectors*D_dimensions*b_float_value + G_compact_retained*(N_vectors*D_dimensions*b_bq_per_dimension + N_vectors*R_degree*b_adjacency_entry + B_generation_metadata) + B_temporary_generation_peak + B_shared_index_metadata",
      "measurement_needed": "Measure shared exact vectors, every retained compact-signature and adjacency generation, per-generation metadata, incremental temporary build/replacement files, shared metadata, allocation high-water, and old-generation deletion timing separately.",
      "uncertainty": "Compression, identifier width, durability, and update duplication are unspecified.",
      "unknown_constants": [
        "b_adjacency_entry",
        "b_bq_per_dimension",
        "b_float_value"
      ],
      "variables": [
        "B_generation_metadata: headers, offsets, manifest, and checksum bytes for one retained compact-index generation",
        "B_shared_index_metadata: durable bytes shared across compact-index generations",
        "B_temporary_generation_peak: incremental build or replacement bytes live beyond all retained compact-index generations",
        "D_dimensions: vector dimension count",
        "G_compact_retained: compact-index generations simultaneously retained at peak",
        "N_vectors: indexed vector count",
        "R_degree: stored adjacency entries per vector"
      ]
    }
  },
  "name": "Navigate Compact Rerank Exactly",
  "original_constraint_profile": {
    "access_medium": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-NAVIGATE-BINARY-RERANK-EXACTLY"
      ],
      "source_pointer_ids": [
        "SP-002"
      ],
      "text": "Two-bit signatures and adjacency are hot, while original vectors form a cold array that may be memory-mapped from SSD and is accessed during reranking.",
      "uncertainty": "Cold storage, cache policy, and candidate-page locality determine actual I/O."
    },
    "communication_model": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-NAVIGATE-BINARY-RERANK-EXACTLY"
      ],
      "source_pointer_ids": [
        "SP-002"
      ],
      "text": "Each query quantizes once, performs beam navigation over hot signatures and adjacency, then reads and scores a retained cold candidate set.",
      "uncertainty": "Cross-query scheduling and distributed communication are not specified."
    },
    "constrained_resource": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-NAVIGATE-BINARY-RERANK-EXACTLY"
      ],
      "source_pointer_ids": [
        "SP-002"
      ],
      "text": "The mechanism removes full-precision vectors from graph-navigation hot state and accesses them only for exact reranking of retained candidates.",
      "uncertainty": "Whole-process hot RAM and cold-page behavior remain workload-specific."
    },
    "data_mutability": {
      "assumptions": [
        "No mutation semantics are inferred from the paper date or index family."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The inspected card describes batch construction and query execution but does not establish an online mutation or consistency protocol."
      ],
      "text": "UNKNOWN",
      "uncertainty": "Updates may require coordinated signature, topology, and exact-vector refresh."
    },
    "original_hardware_operating_assumptions": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-NAVIGATE-BINARY-RERANK-EXACTLY"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "The mechanism assumes a CPU bitwise navigation path over compact signatures and a separable cold tier for full-precision vectors.",
      "uncertainty": "No modern Knight Bus throughput or memory assumption is inferred from publication year."
    },
    "predictability_requirement": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-NAVIGATE-BINARY-RERANK-EXACTLY"
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-004"
      ],
      "text": "Compact navigation must preserve enough local improving paths to reach an exact-neighbor candidate region; exact reranking restores order only among retained candidates.",
      "uncertainty": "A rigorous real-data navigability guarantee is not established."
    }
  },
  "original_constraints": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-NAVIGATE-BINARY-RERANK-EXACTLY"
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-004"
      ],
      "text": "Keeping original vectors in the navigation path increases hot-state pressure, while compact distances alone are insufficient for final ranking.",
      "uncertainty": "The hot-state and ranking tradeoff depends on vector dimension, graph layout, and metric."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-NAVIGATE-BINARY-RERANK-EXACTLY"
      ],
      "source_pointer_ids": [
        "SP-003",
        "SP-004"
      ],
      "text": "Applicability depends on embedding geometry because sign and relative magnitude must retain local neighborhood signal.",
      "uncertainty": "Compatibility forms a distribution-dependent gradient rather than a universal class boundary."
    }
  ],
  "original_cost_model": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-NAVIGATE-BINARY-RERANK-EXACTLY"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Hot state consists of compact signatures, adjacency, metadata, visited state, and beam state; preprocessing encodes vectors and constructs topology in the compact metric; cold reads are restricted to full-precision vectors retained for reranking.",
    "uncertainty": "Build scratch, concurrent-query state, durable overhead, and cold-page locality are not bounded as one portable model."
  },
  "original_domain": "quantized graph-based approximate nearest-neighbor search",
  "proposed_transfer": {
    "assumptions": [
      "Knight Bus has or will expose a vector-similarity family with an exact full-precision oracle."
    ],
    "claim_type": "SPECULATIVE_TRANSFER",
    "premises": [
      "The source demonstrates a two-stage compact-navigation and exact-rerank mechanism.",
      "G06 localizes its failure to target geometry and missing candidate reachability.",
      "A007 can branch before execution and receipt the selected approximation contract."
    ],
    "text": "For vector node-similarity and kNN workloads, permit a compact signature plus adjacency hot graph to guide bounded candidate discovery and read original vectors only for exact reranking, but admit that plan only for a measured compatible embedding distribution and retain a verified alternative-metric or float-navigation fallback.",
    "uncertainty": "No transfer is supported for BFS, WCC, PageRank, community detection, or non-vector graph semantics."
  },
  "reversed_assumptions": [
    {
      "assumptions": [
        "A representative target-distribution sample and exact oracle can be supplied."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "FAIL-INCOMPATIBLE-GEOMETRY-COLLAPSES-RECALL records source-reported failure on incompatible distributions.",
        "Exact reranking cannot restore a neighbor absent from the candidate set."
      ],
      "text": "Embedding compatibility is promoted from a source-specific observation to an explicit pre-admission unknown that must be measured on the target model and monitored for drift.",
      "uncertainty": "No universal compatibility statistic or cutoff is established."
    },
    {
      "assumptions": [
        "The implementation exposes candidate-page counters."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source separates hot navigation from cold reranking.",
        "A007 requires I/O and whole-process receipt terms."
      ],
      "text": "The cold tier is modeled by measured page access and cache residency rather than assumed to be costless because it is outside navigation.",
      "uncertainty": "Candidate layout and shared page reuse are target-specific."
    }
  ],
  "smallest_falsifier": {
    "controlled_variables": [
      "candidate rerank count",
      "construction beam",
      "corpus size",
      "dimension",
      "graph degree",
      "retained compact-index generations",
      "replacement cleanup policy",
      "concurrent query count",
      "in-flight cold-read count",
      "query set",
      "search beam"
    ],
    "failure_signal": "The compatibility gate admits a corpus whose exact neighbors are absent from retained candidates, compact navigation reads original vectors before reranking, exact-vector resident pages or aggregate execution exceed the RAM envelope, or build/replacement allocated-file high-water exceeds STORAGE_peak_bytes before old-generation deletion.",
    "fixture": "Matched compatible and incompatible small vector corpora converted to compact-navigation graphs, followed by a versioned compact-index replacement while the shared exact-vector payload and old compact generation remain present.",
    "independent_oracle": "Brute-force ranking; attributed query, request, scheduler, resident-page, and peak-RSS counters; and allocation high-water plus generation timing through replacement."
  },
  "source_pattern_ids": [
    "PAT-NAVIGATE-BINARY-RERANK-EXACTLY"
  ],
  "surviving_invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-NAVIGATE-BINARY-RERANK-EXACTLY"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-004"
    ],
    "text": "The compact navigation metric need not preserve global exact ranking, but it must preserve enough local improving paths to place exact neighbors in the retained candidate set; original vectors then determine exact order within that set.",
    "uncertainty": "Candidate reachability is empirical on realistic target distributions."
  },
  "target_algorithm_families": [
    "NODE_SIMILARITY",
    "VECTOR_KNN"
  ],
  "transfer_id": "XFER-NAVIGATE-COMPACT-RERANK-EXACTLY",
  "unknown_measurement_constants": [
    "b_adjacency_entry",
    "b_bq_per_dimension",
    "b_float_value",
    "b_runtime_overhead",
    "c_bq_encode",
    "c_geometry_probe",
    "c_graph_construct",
    "c_scheduler_metadata",
    "rho_candidate_page_uniqueness"
  ]
}
```
