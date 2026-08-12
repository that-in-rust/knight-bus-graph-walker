# Bound Search Frontier State

```json
{
  "analogy_failure_modes": [
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-BOUND-SEARCH-CANDIDATE-FRONTIER",
        "FAIL-BOUNDED-FRONTIER-MISSES-NEAREST"
      ],
      "text": "Reject the analogy if an implementation keeps an unbounded duplicate or visited structure outside the frontier, or if a fixed cap is presented as a universal recall guarantee.",
      "uncertainty": "Hidden allocations and graph-dependent reachability of nearest regions require falsification."
    },
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "The source invariant is specific to graph-based approximate nearest-neighbor routing."
      ],
      "text": "Do not apply this transfer to BFS, WCC, or community queues merely because they also have frontiers; their correctness and finality invariants differ.",
      "uncertainty": "A separate mechanism card is required for each other algorithm family."
    }
  ],
  "epistemic_label": "SPECULATIVE_TRANSFER",
  "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-BOUND-SEARCH-FRONTIER-STATE",
  "g06_challenges": [
    {
      "applies": true,
      "failure_id": "FAIL-BOUNDED-FRONTIER-MISSES-NEAREST",
      "response": "Narrow the transfer to a candidate-state bound, require a declared recall envelope for approximation, include an exhaustive-oracle calibration need, and use an exact fallback or refusal when required_capacity(query, graph, recall_target) is not bounded by candidate_cap."
    }
  ],
  "modern_knight_bus_constraints": [
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "A007 requires total-working-set estimation and enforcement.",
        "The source invariant bounds only candidate entries."
      ],
      "text": "Admission must include candidate, result, visited, worker, index, vector, cache, and I/O-buffer terms under the declared hard budget.",
      "uncertainty": "Allocator and storage coefficients require target measurement."
    },
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "FAIL-BOUNDED-FRONTIER-MISSES-NEAREST",
        "The source reports dataset-dependent recall ceilings."
      ],
      "text": "An approximate branch must declare its recall contract and calibration scope; an exact request must use an exact fallback or refuse when the cap cannot preserve the requested semantics.",
      "uncertainty": "No portable candidate-cap threshold is available."
    }
  ],
  "modern_resource_model": {
    "concurrency": {
      "assumptions": [
        "Each worker has an independent bounded frontier.",
        "Shared index bytes are not multiplied unless measurement shows replication."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Concurrent_state_bytes = worker_count*(b_candidate_entry*candidate_cap + b_result_entry*result_cap + b_visited_entry*visited_cap + b_worker_buffer + b_worker_state) + Q_io*B_io_request",
      "measurement_needed": "Measure worker-local frontier capacity, request buffers, shared-state contention, and aggregate RSS while varying admitted workers.",
      "uncertainty": "Worker-local versus shared allocation and storage queueing are unmeasured.",
      "unknown_constants": [
        "b_candidate_entry",
        "b_result_entry",
        "b_visited_entry",
        "b_worker_buffer",
        "b_worker_state"
      ],
      "variables": [
        {
          "definition": "Concurrently admitted search workers",
          "symbol": "worker_count",
          "units": "workers"
        },
        {
          "definition": "Maximum retained candidates per worker",
          "symbol": "candidate_cap",
          "units": "candidates"
        },
        {
          "definition": "Maximum retained result entries per worker",
          "symbol": "result_cap",
          "units": "results"
        },
        {
          "definition": "Maximum explicitly tracked visited vertices per worker",
          "symbol": "visited_cap",
          "units": "vertices"
        },
        {
          "definition": "In-flight index or vector storage requests",
          "symbol": "Q_io",
          "units": "requests"
        },
        {
          "definition": "Bytes reserved per in-flight direct-I/O or request buffer",
          "symbol": "B_io_request",
          "units": "bytes per request"
        }
      ]
    },
    "io": {
      "assumptions": [
        "External index or vector pages are read through one declared storage path.",
        "Cold-cache and warm-cache receipts are distinguished."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "IO_bytes = a_physical_read * requested_pages * page_bytes + b_spill_record * spill_records",
      "measurement_needed": "Measure logical requests, physical reads, page size, cache state, and spill-record bytes for each admitted layout.",
      "uncertainty": "Path length and physical read amplification remain graph-, query-, layout-, and cache-dependent.",
      "unknown_constants": [
        "a_physical_read",
        "b_spill_record"
      ],
      "variables": [
        {
          "definition": "Logical index and vector pages requested by the search",
          "symbol": "requested_pages",
          "units": "pages"
        },
        {
          "definition": "Bytes in one storage page",
          "symbol": "page_bytes",
          "units": "bytes per page"
        },
        {
          "definition": "Candidate or trace records written to spill storage",
          "symbol": "spill_records",
          "units": "records"
        }
      ]
    },
    "preprocessing": {
      "assumptions": [
        "Calibration uses a declared query distribution and exact oracle.",
        "No source benchmark ratio is imported."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "T_prepare_ns = t_index_probe * probe_items + t_query_calibration * calibration_queries",
      "measurement_needed": "Time index probes and any query-distribution calibration separately from query execution.",
      "uncertainty": "A representative calibration set and its stability under index drift are unknown.",
      "unknown_constants": [
        "t_index_probe",
        "t_query_calibration"
      ],
      "variables": [
        {
          "definition": "Index records inspected before admission",
          "symbol": "probe_items",
          "units": "records"
        },
        {
          "definition": "Representative queries used to establish an allowed recall envelope",
          "symbol": "calibration_queries",
          "units": "queries"
        }
      ]
    },
    "ram": {
      "assumptions": [
        "Candidate, result, and visited structures have explicit admission caps.",
        "The graph index and vector payload are accounted outside this algorithm-state term."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "RAM_peak_bytes = b_fixed_runtime + B_index_vector_cache_peak + Q_io*B_io_request + worker_count*(b_candidate_entry*candidate_cap + b_result_entry*result_cap + b_visited_entry*visited_cap + b_worker_buffer + b_worker_state)",
      "measurement_needed": "Instrument candidate, result, visited, per-worker, shared index/vector resident-page, in-flight direct-I/O/request-buffer, allocator, and runtime high-water bytes under cold and warm cache states.",
      "uncertainty": "Allocator capacity, duplicate suppression, and hidden visited-state widths are not yet calibrated.",
      "unknown_constants": [
        "b_candidate_entry",
        "b_fixed_runtime",
        "b_result_entry",
        "b_visited_entry",
        "b_worker_buffer",
        "b_worker_state"
      ],
      "variables": [
        {
          "definition": "Shared index and exact-vector pages physically resident at peak, excluding per-worker buffers",
          "symbol": "B_index_vector_cache_peak",
          "units": "bytes"
        },
        {
          "definition": "Bytes reserved for one in-flight direct-I/O or request buffer",
          "symbol": "B_io_request",
          "units": "bytes per request"
        },
        {
          "definition": "In-flight index or vector storage requests",
          "symbol": "Q_io",
          "units": "requests"
        },
        {
          "definition": "Maximum retained search candidates per worker",
          "symbol": "candidate_cap",
          "units": "candidates"
        },
        {
          "definition": "Maximum retained result entries per worker",
          "symbol": "result_cap",
          "units": "results"
        },
        {
          "definition": "Maximum explicitly tracked visited vertices per worker",
          "symbol": "visited_cap",
          "units": "vertices"
        },
        {
          "definition": "Concurrently admitted search workers",
          "symbol": "worker_count",
          "units": "workers"
        }
      ]
    },
    "storage": {
      "assumptions": [
        "The search frontier itself is temporary.",
        "Exact vectors remain available when exact verification or fallback requires them."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Storage_peak_bytes = graph_bytes + index_bytes + vector_bytes + a_storage_overlap * temporary_index_bytes",
      "measurement_needed": "Measure retained artifacts and peak old/new index generation overlap.",
      "uncertainty": "Index-generation overlap and retention policy are implementation choices.",
      "unknown_constants": [
        "a_storage_overlap"
      ],
      "variables": [
        {
          "definition": "Persistent graph topology bytes",
          "symbol": "graph_bytes",
          "units": "bytes"
        },
        {
          "definition": "Persistent search-index bytes",
          "symbol": "index_bytes",
          "units": "bytes"
        },
        {
          "definition": "Persistent exact vector payload bytes",
          "symbol": "vector_bytes",
          "units": "bytes"
        },
        {
          "definition": "Temporary bytes during index preparation or replacement",
          "symbol": "temporary_index_bytes",
          "units": "bytes"
        }
      ]
    }
  },
  "name": "Bound Search Frontier State",
  "original_constraint_profile": {
    "access_medium": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BOUND-SEARCH-CANDIDATE-FRONTIER"
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-005"
      ],
      "text": "The core search repeatedly expands an in-memory candidate set; when original vectors are external, visited path length controls external accesses.",
      "uncertainty": "The source does not provide portable bytes per path step."
    },
    "communication_model": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BOUND-SEARCH-CANDIDATE-FRONTIER"
      ],
      "source_pointer_ids": [
        "SP-005"
      ],
      "text": "The source pseudocode uses iterative candidate expansion, distance evaluation, eviction, and result updates within one search.",
      "uncertainty": "Parallel scheduling and distributed communication are outside the pseudocode."
    },
    "constrained_resource": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BOUND-SEARCH-CANDIDATE-FRONTIER"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002",
        "SP-003"
      ],
      "text": "The mechanism constrains frequently accessed candidate-frontier entries with a configured capacity c while index, result, query, vector, and visited state remain additional resources.",
      "uncertainty": "The source does not normalize entry widths or whole-process overhead."
    },
    "data_mutability": {
      "assumptions": [
        "Publication year is not used to infer mutability."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The mechanism card describes query-time search over an existing graph index.",
        "Its unknown boundary does not establish behavior during index mutation."
      ],
      "text": "UNKNOWN",
      "uncertainty": "The inspected evidence does not specify a concurrent index-update model."
    },
    "original_hardware_operating_assumptions": {
      "assumptions": [
        "Publication year is not treated as hardware evidence."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source mechanism card distinguishes in-memory core search from externally stored vectors.",
        "It reports implementation-specific memory and path observations without a portable hardware contract."
      ],
      "text": "UNKNOWN",
      "uncertainty": "Exact cache, storage device, allocator, and worker assumptions are not frozen by the source mechanism card."
    },
    "predictability_requirement": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BOUND-SEARCH-CANDIDATE-FRONTIER"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "Configured capacity deterministically bounds candidate count, but target recall and query path length remain graph- and dataset-dependent.",
      "uncertainty": "No universal capacity establishes recall, runtime, or I/O."
    }
  },
  "original_constraints": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BOUND-SEARCH-CANDIDATE-FRONTIER"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-005"
      ],
      "text": "Candidate set C is trimmed to configured capacity c by removing the farthest candidate after expansion.",
      "uncertainty": "The bound excludes the graph index, vector payloads, result set, and visited-state implementation."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-BOUND-SEARCH-CANDIDATE-FRONTIER"
      ],
      "source_pointer_ids": [
        "SP-002"
      ],
      "text": "Candidate capacity should remain small enough for frequent access, while required capacity and path length vary by graph, dataset, algorithm, and recall target.",
      "uncertainty": "The source supplies no universal admissible value."
    }
  ],
  "original_cost_model": {
    "assumptions": [
      "The expression is a symbolic restatement, not a source benchmark ratio."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source card bounds |C| by c.",
      "The source card identifies index, result, query, distance, and visited state as additional terms.",
      "The source links external accesses to path length."
    ],
    "text": "RAM_original_bytes = B_index + b_candidate * c + B_result + B_query_vector + B_visited; external I/O follows visited_path_pages rather than c alone.",
    "uncertainty": "Byte coefficients, duplicate state, and path-page amplification are unknown."
  },
  "original_domain": "graph-based approximate nearest-neighbor search",
  "proposed_transfer": {
    "assumptions": [
      "The selected search implementation has explicit result and visited bounds.",
      "A later goal chooses among eligible plans."
    ],
    "claim_type": "SPECULATIVE_TRANSFER",
    "premises": [
      "PAT-BOUND-SEARCH-CANDIDATE-FRONTIER",
      "FAIL-BOUNDED-FRONTIER-MISSES-NEAREST",
      "A007 fit/spill/approximate/refuse and receipt contract"
    ],
    "text": "Expose candidate_cap as a countable admission variable for Knight Bus node-similarity or kNN search, enforce it during execution, and receipt its high-water count together with visited state and page traffic. Admit the bounded approximate path only inside a graph-, index-, query-distribution-, and recall-scoped envelope; otherwise select an exact fallback or refuse.",
    "uncertainty": "This transfer does not establish recall, latency, or I/O from candidate_cap alone."
  },
  "reversed_assumptions": [
    {
      "assumptions": [
        "No final architecture is selected."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "A007 requires a full-working-set contract.",
        "The source card separates bounded frontier state from graph index and externally stored vectors."
      ],
      "text": "The candidate frontier need not imply that the complete index and vector payload are resident, but any page cache, direct-I/O buffers, spill state, and visited structure must be included in the modern budget.",
      "uncertainty": "The final storage path and cache policy remain a later design decision."
    },
    {
      "assumptions": [],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source proves the candidate-count cap.",
        "FAIL-BOUNDED-FRONTIER-MISSES-NEAREST shows that fixed capacity is not a universal target-recall guarantee."
      ],
      "text": "A configured frontier cap is usable as an admission term only after separating memory predictability from recall predictability.",
      "uncertainty": "The graph- and query-dependent required capacity is unmeasured."
    }
  ],
  "smallest_falsifier": {
    "controlled_variables": [
      "candidate_cap",
      "distance_function",
      "graph_index",
      "query",
      "result_cap",
      "seed_set",
      "visited_cap",
      "worker_count",
      "cache state",
      "simultaneous storage reads"
    ],
    "failure_signal": "Any worker structure exceeds its cap, resident index/vector pages or request buffers make aggregate RSS exceed the full peak, or an admitted recall contract is violated.",
    "fixture": "A small two-basin vector graph whose exact nearest neighbor is reachable only through a temporarily farther bridge candidate, exercised by one and multiple simultaneous instrumented bounded searches.",
    "independent_oracle": "Exhaustive exact search plus per-worker state, resident-page, in-flight request-buffer, aggregate-RSS, and physical-read traces."
  },
  "source_pattern_ids": [
    "PAT-BOUND-SEARCH-CANDIDATE-FRONTIER"
  ],
  "surviving_invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-BOUND-SEARCH-CANDIDATE-FRONTIER"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-005"
    ],
    "text": "The candidate frontier never exceeds configured capacity c because overfill triggers farthest-candidate eviction.",
    "uncertainty": "This is a state-count invariant, not a recall or total-memory guarantee."
  },
  "target_algorithm_families": [
    "APPROXIMATE_NEAREST_NEIGHBOR_SEARCH",
    "NODE_SIMILARITY_KNN"
  ],
  "transfer_id": "XFER-BOUND-SEARCH-FRONTIER-STATE",
  "unknown_measurement_constants": [
    "a_physical_read",
    "a_storage_overlap",
    "b_candidate_entry",
    "b_fixed_runtime",
    "b_result_entry",
    "b_spill_record",
    "b_visited_entry",
    "b_worker_buffer",
    "b_worker_state",
    "t_index_probe",
    "t_query_calibration"
  ]
}
```
