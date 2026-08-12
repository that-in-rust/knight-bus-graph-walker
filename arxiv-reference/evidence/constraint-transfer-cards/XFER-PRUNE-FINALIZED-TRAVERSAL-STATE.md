# Prune Finalized Traversal State

```json
{
  "analogy_failure_modes": [
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-PRUNE-SETTLED-SEARCH-STATE"
      ],
      "text": "Reject the transfer when parent or traversal state is not monotone-final, when graph mutations can invalidate settlement, or when reconstruction cannot be independently checked.",
      "uncertainty": "Finality and snapshot semantics are algorithm-specific."
    },
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "FAIL-LATE-SETTLEMENT-ERASES-PRUNING"
      ],
      "text": "Do not claim a resource win when late settlement leaves the reduced stream near its original size; correctness may survive while the scheduling advantage disappears.",
      "uncertainty": "The payback crossover is machine- and workload-dependent."
    }
  ],
  "epistemic_label": "SPECULATIVE_TRANSFER",
  "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-PRUNE-FINALIZED-TRAVERSAL-STATE",
  "g06_challenges": [
    {
      "applies": true,
      "failure_id": "FAIL-LATE-SETTLEMENT-ERASES-PRUNING",
      "response": "Assume zero pruning benefit at admission, measure settlement timing and reduced bytes online, materialize Enext only when modeled future scan savings exceed pruning plus rewrite plus reconstruction cost, and retain the current-stream fallback when the guard is not met."
    }
  ],
  "modern_knight_bus_constraints": [
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-PRUNE-SETTLED-SEARCH-STATE",
        "A007 verification receipt"
      ],
      "text": "Exact release requires an algorithm-specific finality proof, snapshot-stable input, persisted parent information, and an independent reconstruction oracle.",
      "uncertainty": "Knight Bus reconstruction and durability coefficients are not measured."
    },
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-PRUNE-SETTLED-SEARCH-STATE",
        "FAIL-LATE-SETTLEMENT-ERASES-PRUNING"
      ],
      "text": "Admission uses the unpruned upper bound; execution may materialize a reduced stream only when observed saved future scan work exceeds pruning, rewrite, overlap, and reconstruction cost, otherwise it continues with the current stream.",
      "uncertainty": "No portable payback threshold exists."
    }
  ],
  "modern_resource_model": {
    "concurrency": {
      "assumptions": [
        "Concurrency preserves the declared BFS parent and neighbor-order semantics.",
        "Per-worker buffers are capped before admission."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Concurrent_state_bytes = worker_count * (b_scan_buffer * scan_buffer_edges + b_reduce_queue * reduce_queue_nodes) + b_merge_state * worker_count",
      "measurement_needed": "Measure worker-local buffers, queues, merge metadata, synchronization, and deterministic parent-selection state.",
      "uncertainty": "Parallel finality and merge overhead are not established by the source mechanism.",
      "unknown_constants": [
        "b_merge_state",
        "b_reduce_queue",
        "b_scan_buffer"
      ],
      "variables": [
        {
          "definition": "Concurrent scan or reducer workers",
          "symbol": "worker_count",
          "units": "workers"
        },
        {
          "definition": "Buffered edge entries per worker",
          "symbol": "scan_buffer_edges",
          "units": "edges per worker"
        },
        {
          "definition": "Reducer queue nodes per worker",
          "symbol": "reduce_queue_nodes",
          "units": "nodes per worker"
        }
      ]
    },
    "io": {
      "assumptions": [
        "Current ER scans are sequential.",
        "The original stream remains available for fallback until replacement is committed."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "IO_bytes = a_scan_physical * sum_ER_scan_bytes + ET_write_bytes + reset_read_bytes + a_rewrite_overlap * Enext_write_bytes",
      "measurement_needed": "Trace logical and physical scan bytes, finalized-edge writes, reduced-stream rewrites, and Reset reads per iteration.",
      "uncertainty": "Cache effects, rewrite overlap, and future scan savings are runtime-dependent.",
      "unknown_constants": [
        "a_rewrite_overlap",
        "a_scan_physical"
      ],
      "variables": [
        {
          "definition": "Logical bytes across all current reduced-stream scans",
          "symbol": "sum_ER_scan_bytes",
          "units": "bytes"
        },
        {
          "definition": "Bytes written for finalized tree edges",
          "symbol": "ET_write_bytes",
          "units": "bytes"
        },
        {
          "definition": "Bytes read during final reconstruction",
          "symbol": "reset_read_bytes",
          "units": "bytes"
        },
        {
          "definition": "Bytes written while materializing reduced streams",
          "symbol": "Enext_write_bytes",
          "units": "bytes"
        }
      ]
    },
    "preprocessing": {
      "assumptions": [
        "The graph is static during preparation.",
        "Input edge records can be scanned and rewritten into the required stream form."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "T_prepare_ns = t_degree_classify * n + t_initial_edge_scan * m + t_stream_write * m",
      "measurement_needed": "Measure initial degree classification, edge scan, reduced-stream construction, and split-tree initialization.",
      "uncertainty": "Preparation scratch and storage-device coefficients are not calibrated.",
      "unknown_constants": [
        "t_degree_classify",
        "t_initial_edge_scan",
        "t_stream_write"
      ],
      "variables": [
        {
          "definition": "Graph node count",
          "symbol": "n",
          "units": "nodes"
        },
        {
          "definition": "Graph edge count",
          "symbol": "m",
          "units": "edges"
        }
      ]
    },
    "ram": {
      "assumptions": [
        "Admission assumes no pruning benefit unless a pre-run proof supplies a smaller U_tree_max.",
        "Settled edges are released only after exact finality is established.",
        "Shared node, tree, edge-batch, and pruning state excludes every per-worker scan buffer, reduce queue, and merge state.",
        "Stream pages physically resident in the operating-system cache are charged separately from userspace edge batches."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "RAM_peak_bytes = b_runtime + B_stream_page_cache_peak + b_node_state*n + b_tree_edge*U_tree_max + b_edge_batch*E_batch_cap + b_prune_meta*n + worker_count*(b_scan_buffer*scan_buffer_edges + b_reduce_queue*reduce_queue_nodes + b_merge_state)",
      "measurement_needed": "Measure aggregate whole-process or cgroup memory and separately attribute stream-page cache residency, node arrays, unsettled tree, edge batch, pruning metadata, runtime, and every worker's scan buffer, reduce queue, and merge state.",
      "uncertainty": "Per-node widths, container capacity, and runtime overhead are uncalibrated.",
      "unknown_constants": [
        "b_edge_batch",
        "b_node_state",
        "b_prune_meta",
        "b_reduce_queue",
        "b_runtime",
        "b_scan_buffer",
        "b_tree_edge",
        "b_merge_state"
      ],
      "variables": [
        {
          "definition": "Current and candidate reduced-stream pages physically resident at peak, excluding userspace edge batches",
          "symbol": "B_stream_page_cache_peak",
          "units": "bytes"
        },
        {
          "definition": "Graph node count",
          "symbol": "n",
          "units": "nodes"
        },
        {
          "definition": "Maximum unsettled resident tree edges",
          "symbol": "U_tree_max",
          "units": "edges"
        },
        {
          "definition": "Maximum admitted edge-batch entries",
          "symbol": "E_batch_cap",
          "units": "edges"
        },
        {
          "definition": "Concurrent reducer or scan workers",
          "symbol": "worker_count",
          "units": "workers"
        },
        {
          "definition": "Buffered edge entries per worker",
          "symbol": "scan_buffer_edges",
          "units": "edges per worker"
        },
        {
          "definition": "Reducer queue nodes per worker",
          "symbol": "reduce_queue_nodes",
          "units": "nodes per worker"
        }
      ]
    },
    "storage": {
      "assumptions": [
        "Old and new generations coexist until replacement is verified.",
        "Crash-consistency metadata is counted if present."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Storage_peak_bytes = input_graph_bytes + current_ER_bytes + ET_bytes + output_tree_bytes + a_generation_overlap * next_ER_bytes",
      "measurement_needed": "Measure retained input, current and next stream generations, finalized edges, and output-tree coexistence through cleanup.",
      "uncertainty": "Generation lifetime and durability semantics are not specified by the source.",
      "unknown_constants": [
        "a_generation_overlap"
      ],
      "variables": [
        {
          "definition": "Retained input artifact bytes",
          "symbol": "input_graph_bytes",
          "units": "bytes"
        },
        {
          "definition": "Current edge-stream generation bytes",
          "symbol": "current_ER_bytes",
          "units": "bytes"
        },
        {
          "definition": "Disk-resident finalized tree-edge bytes",
          "symbol": "ET_bytes",
          "units": "bytes"
        },
        {
          "definition": "Final reconstructed tree bytes",
          "symbol": "output_tree_bytes",
          "units": "bytes"
        },
        {
          "definition": "Candidate next edge-stream generation bytes",
          "symbol": "next_ER_bytes",
          "units": "bytes"
        }
      ]
    }
  },
  "name": "Prune Finalized Traversal State",
  "original_constraint_profile": {
    "access_medium": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-PRUNE-SETTLED-SEARCH-STATE"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "text": "The graph and finalized tree edges are disk-resident streams while the unsettled tree portion, admitted edge batch, node arrays, order, and thresholds remain in memory.",
      "uncertainty": "Physical-byte reduction depends on when reduced streams are materialized."
    },
    "communication_model": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-PRUNE-SETTLED-SEARCH-STATE"
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "text": "State moves between sequential edge streams, resident reducer state, disk-resident finalized parent edges, and a final Reset reconstruction.",
      "uncertainty": "Distributed coordination and crash recovery are outside the source mechanism."
    },
    "constrained_resource": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-PRUNE-SETTLED-SEARCH-STATE"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "The mechanism frees resident spanning-tree edge slots by moving settled parent edges to disk and may shrink later edge streams while retaining global per-node arrays.",
      "uncertainty": "Pruning yield is not predictable before execution."
    },
    "data_mutability": {
      "assumptions": [],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source mechanism processes a disk-resident graph and reconstructs one final BFS tree.",
        "No update protocol appears in the mechanism card."
      ],
      "text": "The evidence is scoped to one static directed graph during the BFS run.",
      "uncertainty": "Concurrent graph mutation and snapshot semantics are not established."
    },
    "original_hardware_operating_assumptions": {
      "assumptions": [
        "Publication year is not hardware evidence."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source mechanism card establishes semi-external disk streams, resident arrays, and sequential scans.",
        "It does not freeze a portable cache, device, allocator, or durability profile for the invariant."
      ],
      "text": "UNKNOWN",
      "uncertainty": "The precise original operating assumptions beyond the stated semi-external model are not required and are not inferred from publication year."
    },
    "predictability_requirement": {
      "assumptions": [
        "Admission uses a conservative no-pruning upper bound unless artifact analysis proves otherwise."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source invariant preserves final parent placement through ET or Reset.",
        "The source unknown boundary leaves per-iteration pruning yield unresolved."
      ],
      "text": "Correctness requires proof of settlement before state release; resource admission cannot assume a pruning benefit because the source supplies no pre-run yield estimator.",
      "uncertainty": "Execution-time settlement and reduced-stream size remain workload-dependent."
    }
  },
  "original_constraints": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-PRUNE-SETTLED-SEARCH-STATE"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "text": "Only tree edges whose final position is settled may leave resident state; later work scans the current reduced edge stream and final output is reconstructed from retained pieces.",
      "uncertainty": "The trigger depends on source-specific BFS-order thresholds."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-PRUNE-SETTLED-SEARCH-STATE"
      ],
      "source_pointer_ids": [
        "SP-004"
      ],
      "text": "Zero-in-degree and zero-out-degree nodes may be removed and later restored under the source correctness rule.",
      "uncertainty": "The rule is specific to the source's total-BFS tree ordering."
    }
  ],
  "original_cost_model": {
    "assumptions": [
      "No source benchmark ratio is used."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source card retains two node attributes, order metadata, unsettled tree edges, and an admitted edge batch.",
      "The source card names sequential scans, finalized-edge writes, optional stream rewrites, and Reset."
    ],
    "text": "RAM_original_bytes = b_node * n + b_unsettled_edge * U_tree + b_batch_edge * E_batch + B_order_meta; IO_original_bytes = sum(current_ER_scan_bytes) + ET_write_bytes + Enext_write_bytes + Reset_read_bytes.",
    "uncertainty": "Pruning yield, rewrite overlap, and storage coefficients are unknown."
  },
  "original_domain": "semi-external exact breadth-first search state reduction",
  "proposed_transfer": {
    "assumptions": [
      "Input graph state is snapshot-stable.",
      "A later goal chooses the storage representation."
    ],
    "claim_type": "SPECULATIVE_TRANSFER",
    "premises": [
      "PAT-PRUNE-SETTLED-SEARCH-STATE",
      "FAIL-LATE-SETTLEMENT-ERASES-PRUNING",
      "A007 hard-budget and verification contract"
    ],
    "text": "For eligible exact BFS or reachability plans, separate finalized parent state from the resident unsettled traversal state and release only proof-finalized entries. Quote the unpruned resource ceiling before execution, monitor settlement and stream-reduction yield, conditionally materialize a smaller stream under an online symbolic payback guard, and retain an unpruned fallback plus final reconstruction receipt.",
    "uncertainty": "The transfer does not promise pre-run savings and is invalid without a correctness-preserving finality rule."
  },
  "reversed_assumptions": [
    {
      "assumptions": [],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source has no pre-run pruning-yield estimator.",
        "FAIL-LATE-SETTLEMENT-ERASES-PRUNING constructs a low-yield execution."
      ],
      "text": "Modern admission must not count runtime pruning as guaranteed headroom; pruning can improve the receipt only after settlement is observed.",
      "uncertainty": "Some artifacts may permit static proof of trivial-degree removals, but that proof is not supplied here."
    },
    {
      "assumptions": [],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source mechanism trades pruning work for later scan reduction.",
        "FAIL-LATE-SETTLEMENT-ERASES-PRUNING states C_saved_scan <= C_prune + C_rewrite as the failure boundary."
      ],
      "text": "Reduced-stream materialization becomes a guarded runtime choice because rewrite and reconstruction can outweigh future scan savings.",
      "uncertainty": "The crossover coefficients require online measurement."
    }
  ],
  "smallest_falsifier": {
    "controlled_variables": [
      "BFS_root",
      "edge_order",
      "memory_capacity",
      "neighbor_order",
      "pruning_guard",
      "reconstruction_order",
      "worker count",
      "scan-buffer cap",
      "reduce-queue cap",
      "stream cache or direct-I/O mode"
    ],
    "failure_signal": "A pruned parent later changes, final reconstruction differs from the oracle, resident state exceeds the admitted no-pruning bound, or the guard rewrites when saved scan cost does not exceed pruning and rewrite cost.",
    "fixture": "A small directed graph with zero-degree nodes, an early-settled subtree, a late-settling component, and an edge order that delays parent finality.",
    "independent_oracle": "Deterministic in-memory BFS plus an otherwise identical unpruned semi-external execution with component-attributed worker scan/reduce/merge state, stream resident pages, edge batches, aggregate charged memory, scan, rewrite, and reconstruction counters."
  },
  "source_pattern_ids": [
    "PAT-PRUNE-SETTLED-SEARCH-STATE"
  ],
  "surviving_invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-PRUNE-SETTLED-SEARCH-STATE"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-004"
    ],
    "text": "A node or parent edge may leave resident traversal state only after its final position is preserved in disk-resident state or can be reconstructed exactly; the final tree still covers the original nodes.",
    "uncertainty": "Finality relies on the source BFS-order and zero-degree rules."
  },
  "target_algorithm_families": [
    "BFS",
    "CONNECTED_COMPONENTS",
    "REACHABILITY",
    "UNWEIGHTED_SHORTEST_PATHS"
  ],
  "transfer_id": "XFER-PRUNE-FINALIZED-TRAVERSAL-STATE",
  "unknown_measurement_constants": [
    "a_generation_overlap",
    "a_rewrite_overlap",
    "a_scan_physical",
    "b_edge_batch",
    "b_merge_state",
    "b_node_state",
    "b_prune_meta",
    "b_reduce_queue",
    "b_runtime",
    "b_scan_buffer",
    "b_tree_edge",
    "t_degree_classify",
    "t_initial_edge_scan",
    "t_stream_write"
  ]
}
```
