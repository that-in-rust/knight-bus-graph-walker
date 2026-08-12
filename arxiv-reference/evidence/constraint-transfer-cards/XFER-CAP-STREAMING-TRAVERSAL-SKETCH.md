# Cap Streaming Traversal Sketch

```json
{
  "analogy_failure_modes": [
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH"
      ],
      "text": "Reject the transfer for dynamic inputs, artifacts that cannot support the required sequential adjacency schedule, or traversal semantics lacking the source threshold correctness argument.",
      "uncertainty": "No evidence here establishes those variants."
    },
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "FAIL-LONG-PATHS-REPEAT-SCANS"
      ],
      "text": "Reject any claim that fixed sketch RAM alone bounds job cost; long paths and adversarial edge order can preserve the cap while repeated scans grow.",
      "uncertainty": "Attained worst-case scan growth remains to be falsified."
    }
  ],
  "epistemic_label": "SPECULATIVE_TRANSFER",
  "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-CAP-STREAMING-TRAVERSAL-SKETCH",
  "g06_challenges": [
    {
      "applies": true,
      "failure_id": "FAIL-LONG-PATHS-REPEAT-SCANS",
      "response": "Add an admission term for scan_iterations and current_ER bytes, reject or route to a different exact traversal when the path/correction envelope is not bounded, and receipt scans, reducer invocations, physical bytes, and the observed sketch high-water separately."
    }
  ],
  "modern_knight_bus_constraints": [
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH",
        "A007 total-working-set contract"
      ],
      "text": "Admission must bind K, n, all byte widths, runtime overhead, preparation scratch, stream generations, and worker multiplicity under the hard budget.",
      "uncertainty": "Byte coefficients require target measurement."
    },
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH",
        "FAIL-LONG-PATHS-REPEAT-SCANS"
      ],
      "text": "A separate work and I/O envelope must bound scan iterations and reduced-stream bytes; high-diameter or slowly correcting fixtures require a fallback or refusal even when RAM fits.",
      "uncertainty": "The relationship between artifact features and attained scans is uncalibrated."
    }
  ],
  "modern_resource_model": {
    "concurrency": {
      "assumptions": [
        "Concurrency does not change threshold, parent, or neighbor-order semantics.",
        "Worker caps are fixed before admission."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Concurrent_state_bytes = worker_count * (b_worker_edge * worker_edge_cap + b_worker_queue * worker_queue_cap) + b_sync_state * synchronization_points",
      "measurement_needed": "Measure worker-local edge and queue buffers, synchronization records, and aggregate RSS under the declared ordering semantics.",
      "uncertainty": "The source algorithm is not a portable parallel-concurrency model.",
      "unknown_constants": [
        "b_sync_state",
        "b_worker_edge",
        "b_worker_queue"
      ],
      "variables": [
        {
          "definition": "Concurrent admitted workers",
          "symbol": "worker_count",
          "units": "workers"
        },
        {
          "definition": "Maximum buffered edges per worker",
          "symbol": "worker_edge_cap",
          "units": "edges per worker"
        },
        {
          "definition": "Maximum queued nodes per worker",
          "symbol": "worker_queue_cap",
          "units": "nodes per worker"
        },
        {
          "definition": "Live coordination records for ordered reduction",
          "symbol": "synchronization_points",
          "units": "records"
        }
      ]
    },
    "io": {
      "assumptions": [
        "The current edge stream is sequentially scannable.",
        "The admitted work envelope includes a bound on scan iterations or a refusal condition."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "IO_bytes = a_scan_physical * sum_ER_scan_bytes + a_rewrite_physical * sum_ER_rewrite_bytes",
      "measurement_needed": "Record scan count, current ER bytes per scan, physical reads, rewrite bytes, cache state, and reducer invocations.",
      "uncertainty": "LLSP-dependent scan count and physical amplification remain graph-, order-, and device-dependent.",
      "unknown_constants": [
        "a_rewrite_physical",
        "a_scan_physical"
      ],
      "variables": [
        {
          "definition": "Logical bytes scanned across all outer iterations",
          "symbol": "sum_ER_scan_bytes",
          "units": "bytes"
        },
        {
          "definition": "Logical bytes written across reduced-stream generations",
          "symbol": "sum_ER_rewrite_bytes",
          "units": "bytes"
        }
      ]
    },
    "preprocessing": {
      "assumptions": [
        "The static input can be rewritten into the required adjacency-list order.",
        "Preparation is charged before execution admission completes."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "T_prepare_ns = t_segment_edge * m + t_initialize_node * n + t_prepare_write * prepared_edge_records",
      "measurement_needed": "Measure the initial graph scan, adjacency segmentation, node-array initialization, and preparation scratch.",
      "uncertainty": "Preparation write amplification and retained scratch are unknown.",
      "unknown_constants": [
        "t_initialize_node",
        "t_prepare_write",
        "t_segment_edge"
      ],
      "variables": [
        {
          "definition": "Input graph edge count",
          "symbol": "m",
          "units": "edges"
        },
        {
          "definition": "Input graph node count",
          "symbol": "n",
          "units": "nodes"
        },
        {
          "definition": "Edge records written into adjacency-list segments",
          "symbol": "prepared_edge_records",
          "units": "records"
        }
      ]
    },
    "ram": {
      "assumptions": [
        "The source threshold and reduction logic is preserved.",
        "Every worker buffer, live synchronization record, and charged stream page is explicit."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "RAM_peak_bytes = b_runtime + (K+1)*n*b_edge_slot + n*b_node_attribute + n*b_order_entry + worker_count*(b_worker_edge*worker_edge_cap + b_worker_queue*worker_queue_cap) + b_sync_state*synchronization_points + B_stream_cache_peak",
      "measurement_needed": "Measure sketch, node/order arrays, detailed worker edge/queue buffers, synchronization records, stream page-cache or direct-I/O residency, runtime, and RSS at each K and worker count.",
      "uncertainty": "Integer widths, array metadata, runtime overhead, and worker duplication are not calibrated.",
      "unknown_constants": [
        "b_edge_slot",
        "b_node_attribute",
        "b_order_entry",
        "b_runtime",
        "b_sync_state",
        "b_worker_edge",
        "b_worker_queue"
      ],
      "variables": [
        {
          "definition": "Stream pages or direct-I/O buffers physically resident at peak, excluding worker buffers",
          "symbol": "B_stream_cache_peak",
          "units": "bytes"
        },
        {
          "definition": "Configurable admitted edge slots per node beyond the resident tree portion",
          "symbol": "K",
          "units": "edge slots per node"
        },
        {
          "definition": "Graph node count",
          "symbol": "n",
          "units": "nodes"
        },
        {
          "definition": "Concurrent scan or reduction workers",
          "symbol": "worker_count",
          "units": "workers"
        },
        {
          "definition": "Buffered edges per worker",
          "symbol": "worker_edge_cap",
          "units": "edges per worker"
        },
        {
          "definition": "Queued nodes per worker",
          "symbol": "worker_queue_cap",
          "units": "nodes per worker"
        },
        {
          "definition": "Live ordered-reduction coordination records",
          "symbol": "synchronization_points",
          "units": "records"
        }
      ]
    },
    "storage": {
      "assumptions": [
        "Old and new stream generations coexist until replacement is verified.",
        "Output and checkpoint retention are explicit."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Storage_peak_bytes = input_graph_bytes + current_ER_bytes + ET_bytes + a_generation_overlap * next_ER_bytes + output_tree_bytes",
      "measurement_needed": "Measure peak current/next stream coexistence, retained input, tree-edge state, output, and cleanup timing.",
      "uncertainty": "The source does not specify crash-consistent generation management.",
      "unknown_constants": [
        "a_generation_overlap"
      ],
      "variables": [
        {
          "definition": "Retained input graph bytes",
          "symbol": "input_graph_bytes",
          "units": "bytes"
        },
        {
          "definition": "Current reduced edge-stream bytes",
          "symbol": "current_ER_bytes",
          "units": "bytes"
        },
        {
          "definition": "Disk-resident tree-edge bytes",
          "symbol": "ET_bytes",
          "units": "bytes"
        },
        {
          "definition": "Next edge-stream generation bytes",
          "symbol": "next_ER_bytes",
          "units": "bytes"
        },
        {
          "definition": "Final tree artifact bytes",
          "symbol": "output_tree_bytes",
          "units": "bytes"
        }
      ]
    }
  },
  "name": "Cap Streaming Traversal Sketch",
  "original_constraint_profile": {
    "access_medium": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH"
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "text": "A static directed graph is prepared into adjacency-list edge segments and later processed by repeated sequential scans of a reduced disk-resident edge stream.",
      "uncertainty": "Physical device traffic and page-cache residency are not separated."
    },
    "communication_model": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH"
      ],
      "source_pointer_ids": [
        "SP-003",
        "SP-004"
      ],
      "text": "Sequential edge scans feed a bounded resident sketch; when the cap fills, an in-memory reducer rebuilds the partial tree and resets the edge batch.",
      "uncertainty": "Distributed execution and asynchronous stream partitioning are not specified."
    },
    "constrained_resource": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003",
        "SP-004"
      ],
      "text": "The mechanism fixes resident edge and tree sketch capacity to (K+1)n edge slots while retaining two node attributes, a breadth-first order array, thresholds, and reducer queue indices.",
      "uncertainty": "The cap excludes complete process, runtime, and operating-system memory."
    },
    "data_mutability": {
      "assumptions": [],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The mechanism card states that the formal problem and correctness proof assume one static disk-resident graph.",
        "The card does not establish dynamic updates."
      ],
      "text": "The source proof and preparation model require one static directed graph during execution.",
      "uncertainty": "Snapshot and concurrent-update behavior are outside the evidence."
    },
    "original_hardware_operating_assumptions": {
      "assumptions": [
        "Only explicit medium and access-order claims are retained."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source card establishes disk-resident sequential streams, contiguous resident arrays, and source benchmarks on selected HDD/SSD systems.",
        "It does not freeze one portable device, cache, runtime, or controller profile for transfer."
      ],
      "text": "UNKNOWN",
      "uncertainty": "Exact hardware assumptions are intentionally not inferred from publication year."
    },
    "predictability_requirement": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH"
      ],
      "source_pointer_ids": [
        "SP-003",
        "SP-005"
      ],
      "text": "The named sketch count is fixed by K and n, while exact execution may require up to the source LLSP(G)-dependent outer-iteration allowance.",
      "uncertainty": "A fixed RAM cap does not give a small or portable work bound."
    }
  },
  "original_constraints": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH"
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "text": "Threshold F[i] rejects edges outside the current reduction region, admits qualifying edges only until the fixed sketch cap is reached, then invokes EP-Reduce.",
      "uncertainty": "The threshold logic is specific to the source V-BFS characterization."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH"
      ],
      "source_pointer_ids": [
        "SP-005"
      ],
      "text": "Correctness is stated with at most LLSP(G) outer iterations, so repeated scan work remains graph-dependent.",
      "uncertainty": "The source cost paragraph has a preserved time/I/O label ambiguity."
    }
  ],
  "original_cost_model": {
    "assumptions": [
      "The expression preserves source structure without importing historical ratios."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source card states the (K+1)n edge-slot cap and two node attributes.",
      "The source card identifies the order array, repeated scans, and reduced-stream rewrites."
    ],
    "text": "RAM_original_bytes = (K + 1) * n * b_edge_slot + 2 * n * b_node_attribute + n * b_order_entry + B_runtime; IO_original_bytes = initial_scan_bytes + sum(current_ER_scan_bytes) + sum(rewrite_bytes).",
    "uncertainty": "Whole-process coefficients, stream-generation overlap, and attained scan count are unknown."
  },
  "original_domain": "semi-external exact breadth-first search on directed graphs",
  "proposed_transfer": {
    "assumptions": [
      "The source threshold semantics apply unchanged.",
      "A later goal selects any concrete external-BFS realization."
    ],
    "claim_type": "SPECULATIVE_TRANSFER",
    "premises": [
      "PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH",
      "FAIL-LONG-PATHS-REPEAT-SCANS",
      "A007 hard-budget and refusal contract"
    ],
    "text": "For eligible exact static BFS-style jobs, expose K and n as a fixed resident-sketch admission term, require a sequentially scannable prepared artifact, and enforce the sketch cap at runtime. Separately admit a scan and rewrite envelope based on an explicit path/correction bound; monitor scan count and stream bytes, then fall back or refuse when the work envelope cannot be established.",
    "uncertainty": "The transfer preserves a RAM-count invariant but does not establish acceptable runtime or I/O."
  },
  "reversed_assumptions": [
    {
      "assumptions": [],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "A007 requires full-working-set and I/O accounting.",
        "FAIL-LONG-PATHS-REPEAT-SCANS preserves bounded sketch memory while work grows."
      ],
      "text": "A bounded resident sketch is useful only when the modern contract also prices repeated scans, rewrites, page cache, direct-I/O buffers, runtime state, and preparation.",
      "uncertainty": "Target storage coefficients and scan-count predictors are unmeasured."
    },
    {
      "assumptions": [],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source mechanism depends on sequential disk streams.",
        "The source does not supply portable device coefficients."
      ],
      "text": "Sequential-scan affordability cannot be assumed from the source environment; it must be an admitted target-machine and artifact property.",
      "uncertainty": "No hardware comparison or modern performance claim is made."
    }
  ],
  "smallest_falsifier": {
    "controlled_variables": [
      "K",
      "edge_order",
      "integer_width",
      "neighbor_order",
      "node_count",
      "path_length",
      "segment_size",
      "worker count",
      "cache state",
      "synchronization high-water"
    ],
    "failure_signal": "The returned tree differs from the oracle, resident sketch slots exceed the cap, unmodeled process state breaks the RAM budget, or scans or bytes exceed the admitted work envelope.",
    "fixture": "A degree-controlled directed path with backward distractor edges in favorable and adversarial stream orders, using the smallest admitted K.",
    "independent_oracle": "Deterministic in-memory BFS plus edge-admission, detailed worker buffers, synchronization, resident-page, RSS, scan, rewrite, and physical-I/O traces."
  },
  "source_pattern_ids": [
    "PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH"
  ],
  "surviving_invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "Under the source threshold and reduction rules, resident E plus the resident tree portion never exceeds the configured (K+1)n edge-slot cap.",
    "uncertainty": "The invariant is algorithm-specific and excludes unreported process memory."
  },
  "target_algorithm_families": [
    "BFS",
    "CONNECTED_COMPONENTS",
    "REACHABILITY",
    "UNWEIGHTED_SHORTEST_PATHS"
  ],
  "transfer_id": "XFER-CAP-STREAMING-TRAVERSAL-SKETCH",
  "unknown_measurement_constants": [
    "a_generation_overlap",
    "a_rewrite_physical",
    "a_scan_physical",
    "b_edge_slot",
    "b_node_attribute",
    "b_order_entry",
    "b_runtime",
    "b_sync_state",
    "b_worker_edge",
    "b_worker_queue",
    "t_initialize_node",
    "t_prepare_write",
    "t_segment_edge"
  ]
}
```
