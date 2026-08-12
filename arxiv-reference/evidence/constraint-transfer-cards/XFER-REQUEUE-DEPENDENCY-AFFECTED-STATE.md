# Requeue Dependency Affected State

```json
{
  "analogy_failure_modes": [
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY"
      ],
      "text": "Reject the transfer when an update can alter move opportunities outside the declared neighborhood dependency closure.",
      "uncertainty": "Directed objectives, nonlocal statistics, and graph mutations are outside the source proof."
    },
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "FAIL-DENSE-MOVES-SATURATE-QUEUE"
      ],
      "text": "Reject any strict work-saving claim on dense accepted-move sequences where the queue repeatedly covers nearly all eligible nodes.",
      "uncertainty": "The algorithm may remain correct even when the scheduling advantage vanishes."
    }
  ],
  "epistemic_label": "SPECULATIVE_TRANSFER",
  "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-REQUEUE-DEPENDENCY-AFFECTED-STATE",
  "g06_challenges": [
    {
      "applies": true,
      "failure_id": "FAIL-DENSE-MOVES-SATURATE-QUEUE",
      "response": "Reserve a full n-entry queue, measure distinct requeued nodes and neighbor visits per accepted move, and switch to a single deduplicated full sweep when modeled queue and adjacency work no longer improves on the sweep fallback."
    }
  ],
  "modern_knight_bus_constraints": [
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY",
        "A007 full-working-set contract"
      ],
      "text": "Admission reserves a full n-entry queue and membership state unless a smaller dependency closure is proved, and it separately models adjacency traffic, partition state, community statistics, and workers.",
      "uncertainty": "Queue, statistic, and storage coefficients require measurement."
    },
    {
      "assumptions": [],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY",
        "FAIL-DENSE-MOVES-SATURATE-QUEUE"
      ],
      "text": "Execution monitors distinct affected-neighborhood coverage and switches to one deduplicated full sweep when repeated queue refill is no longer cheaper, while preserving the same quality, tie, and ordering contract.",
      "uncertainty": "The crossover is target-implementation and workload dependent."
    }
  ],
  "modern_resource_model": {
    "concurrency": {
      "assumptions": [
        "Parallel scheduling defines conflict, ordering, and tie semantics.",
        "A serial worker is the default when equivalence is not proved."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Concurrent_state_bytes = worker_count * (b_worker_queue * worker_queue_cap + b_worker_delta * delta_buffer_cap) + b_sync_membership * n",
      "measurement_needed": "Measure per-worker queues and delta buffers, base membership separately from incremental synchronization metadata, retries, and aggregate RSS.",
      "uncertainty": "The source fast local move rule does not establish concurrent equivalence.",
      "unknown_constants": [
        "b_sync_membership",
        "b_worker_delta",
        "b_worker_queue"
      ],
      "variables": [
        {
          "definition": "Concurrent local-move workers",
          "symbol": "worker_count",
          "units": "workers"
        },
        {
          "definition": "Maximum queued nodes per worker",
          "symbol": "worker_queue_cap",
          "units": "nodes per worker"
        },
        {
          "definition": "Maximum pending quality-delta records per worker",
          "symbol": "delta_buffer_cap",
          "units": "records per worker"
        },
        {
          "definition": "Graph node count covered by incremental synchronization metadata beyond the base membership representation",
          "symbol": "n",
          "units": "nodes"
        }
      ]
    },
    "io": {
      "assumptions": [
        "The chosen storage path can retrieve a popped node's adjacency.",
        "Spill order preserves declared move semantics if spill is enabled."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "IO_bytes = a_adjacency_read * edge_record_bytes * neighbor_visits + b_queue_spill * spilled_queue_entries",
      "measurement_needed": "Trace adjacency records, logical and physical reads, cache state, queue spill writes, and repeated visits by node.",
      "uncertainty": "The source provides no disk-backed I/O model, and locality under queue order is unknown.",
      "unknown_constants": [
        "a_adjacency_read",
        "b_queue_spill"
      ],
      "variables": [
        {
          "definition": "Bytes per adjacency record",
          "symbol": "edge_record_bytes",
          "units": "bytes per edge"
        },
        {
          "definition": "Adjacency records visited across all queue pops and requeues",
          "symbol": "neighbor_visits",
          "units": "edge visits"
        },
        {
          "definition": "Queue entries written to spill storage",
          "symbol": "spilled_queue_entries",
          "units": "entries"
        }
      ]
    },
    "preprocessing": {
      "assumptions": [
        "The initial node order and tie rules are recorded.",
        "Initialization is charged to the admitted job."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "T_prepare_ns = t_queue_init * n + t_partition_init * n + t_stat_init * m",
      "measurement_needed": "Measure randomized or deterministic queue initialization, partition initialization, and community-statistic construction.",
      "uncertainty": "Statistic initialization work depends on the quality function and representation.",
      "unknown_constants": [
        "t_partition_init",
        "t_queue_init",
        "t_stat_init"
      ],
      "variables": [
        {
          "definition": "Graph node count",
          "symbol": "n",
          "units": "nodes"
        },
        {
          "definition": "Graph edge count used to initialize community statistics",
          "symbol": "m",
          "units": "edges"
        }
      ]
    },
    "ram": {
      "assumptions": [
        "Duplicate queued entries are suppressed.",
        "Admission reserves queue_cap equal to n unless a smaller dependency bound is proved."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "RAM_peak_bytes = graph_partition_bytes + b_queue_entry*queue_cap + b_membership*n + b_community_stat*community_count + worker_count*(b_worker_queue*worker_queue_cap + b_worker_delta*delta_buffer_cap) + b_sync_membership*n",
      "measurement_needed": "Measure the shared queue, base membership, community statistics, graph representation, every private worker queue and delta buffer, synchronization membership, and aggregate peak RSS under the admitted caps.",
      "uncertainty": "Container growth, partition layout, and community-statistic widths are unknown.",
      "unknown_constants": [
        "b_community_stat",
        "b_membership",
        "b_queue_entry",
        "b_sync_membership",
        "b_worker_delta",
        "b_worker_queue"
      ],
      "variables": [
        {
          "definition": "Resident graph topology and partition-layout bytes excluding the node-to-community assignment charged by b_membership*n",
          "symbol": "graph_partition_bytes",
          "units": "bytes"
        },
        {
          "definition": "Maximum distinct queued nodes",
          "symbol": "queue_cap",
          "units": "nodes"
        },
        {
          "definition": "Graph node count",
          "symbol": "n",
          "units": "nodes"
        },
        {
          "definition": "Current community count",
          "symbol": "community_count",
          "units": "communities"
        },
        {
          "definition": "Concurrent local-move workers",
          "symbol": "worker_count",
          "units": "workers"
        },
        {
          "definition": "Maximum queued nodes in each private worker queue, excluding the shared queue_cap entries",
          "symbol": "worker_queue_cap",
          "units": "nodes per worker"
        },
        {
          "definition": "Maximum pending quality-delta records in each worker buffer",
          "symbol": "delta_buffer_cap",
          "units": "records per worker"
        }
      ]
    },
    "storage": {
      "assumptions": [
        "Checkpoint semantics are explicit if queue state is durable.",
        "No durable queue is assumed when queue_checkpoint_bytes is zero by plan."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "Storage_peak_bytes = graph_bytes + partition_bytes + a_checkpoint_overlap * checkpoint_bytes + queue_checkpoint_bytes",
      "measurement_needed": "Measure retained graph and partition artifacts, checkpoint generations, queue state, and cleanup overlap.",
      "uncertainty": "The source does not specify checkpoint or recovery behavior.",
      "unknown_constants": [
        "a_checkpoint_overlap"
      ],
      "variables": [
        {
          "definition": "Persistent graph artifact bytes",
          "symbol": "graph_bytes",
          "units": "bytes"
        },
        {
          "definition": "Persistent or retained partition bytes",
          "symbol": "partition_bytes",
          "units": "bytes"
        },
        {
          "definition": "One generated state checkpoint",
          "symbol": "checkpoint_bytes",
          "units": "bytes"
        },
        {
          "definition": "Retained queue and membership checkpoint bytes",
          "symbol": "queue_checkpoint_bytes",
          "units": "bytes"
        }
      ]
    }
  },
  "name": "Requeue Dependency Affected State",
  "original_constraint_profile": {
    "access_medium": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "Each queue pop evaluates neighboring communities and, after an accepted move, traverses affected neighbors for requeue.",
      "uncertainty": "Disk-backed adjacency behavior is not specified."
    },
    "communication_model": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "One in-process work queue coordinates local node moves, queue membership, partition state, and community statistics.",
      "uncertainty": "Parallel or distributed queue ownership is not specified."
    },
    "constrained_resource": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "The mechanism avoids repeated global sweeps by retaining a duplicate-suppressed queue of nodes whose local move opportunities may have changed.",
      "uncertainty": "The queue may still hold all nodes and revisit them multiple times."
    },
    "data_mutability": {
      "assumptions": [],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source mechanism card explicitly leaves directed quality functions and graph mutations outside its established scope."
      ],
      "text": "The evidence is scoped to a static undirected graph during the local-moving phase.",
      "uncertainty": "Dynamic graph and snapshot semantics are not established."
    },
    "original_hardware_operating_assumptions": {
      "assumptions": [
        "Publication year is not used to infer hardware."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source card evaluates an in-memory implementation and provides no external-memory resource model.",
        "The operational invariant is a dependency and queue rule rather than a hardware property."
      ],
      "text": "UNKNOWN",
      "uncertainty": "Exact machine, cache, allocator, and concurrency assumptions are not frozen."
    },
    "predictability_requirement": {
      "assumptions": [],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source queue starts with all nodes and suppresses duplicate queued entries.",
        "The source card does not bound revisit count."
      ],
      "text": "Queue capacity can be bounded by n under one-entry-per-node membership, but total pops, neighbor visits, and requeues are workload-dependent.",
      "uncertainty": "Accepted move sequence and affected-neighborhood coverage are quality-function dependent."
    }
  },
  "original_constraints": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "All nodes are initially eligible; after an accepted move, only neighbors outside the new community and not already queued are added for reevaluation.",
      "uncertainty": "Correctness is tied to the source local quality-delta dependency."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY"
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "text": "Queue coverage participates in the source local-move and connectivity reasoning.",
      "uncertainty": "The source does not isolate a queue-scheduler performance ablation."
    }
  ],
  "original_cost_model": {
    "assumptions": [
      "No source runtime ratio is reused."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source queue is initialized with all nodes and suppresses duplicates.",
      "Each popped node evaluates neighbors and accepted moves can requeue affected neighbors."
    ],
    "text": "RAM_original_bytes = B_graph_partition + b_queue_entry * n + b_membership * n + B_community_stats; Work_original = sum(queue_pops + neighbor_visits + requeues).",
    "uncertainty": "Queue container widths, revisit count, adjacency traffic, and community statistics are unknown."
  },
  "original_domain": "incremental local-move scheduling for community detection",
  "proposed_transfer": {
    "assumptions": [
      "The local dependency rule is proved for the target kernel.",
      "A later goal selects the concrete scheduler."
    ],
    "claim_type": "SPECULATIVE_TRANSFER",
    "premises": [
      "PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY",
      "FAIL-DENSE-MOVES-SATURATE-QUEUE",
      "A007 bounded-plan and receipt contract"
    ],
    "text": "For local community-move, label-propagation, or active-set kernels with a proved dependency closure, maintain a duplicate-suppressed affected-node queue instead of unconditional global rescans. Quote full queue capacity at admission, receipt queue high-water, pops, requeues, and adjacency bytes, and fall back to a deduplicated sweep when observed affected coverage approaches global work.",
    "uncertainty": "The transfer is invalid for nonlocal dependencies, undefined parallel ordering, or mutable graphs without snapshot semantics."
  },
  "reversed_assumptions": [
    {
      "assumptions": [],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source provides an in-memory queue rule and no storage-I/O model.",
        "A007 requires I/O and temporary-state accounting."
      ],
      "text": "A modern disk-backed or bounded runner cannot assume queue order has good locality; adjacency traffic and queue spill must be explicit resource terms.",
      "uncertainty": "The best adjacency layout and spill policy are later design choices."
    },
    {
      "assumptions": [],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source preserves only local dependency coverage.",
        "FAIL-DENSE-MOVES-SATURATE-QUEUE exposes near-global requeue coverage."
      ],
      "text": "Changed-neighborhood scheduling is a conditional work-suppression rule, not a strict work bound; dense accepted moves can approach global sweeps.",
      "uncertainty": "No source ratio defines the switching point."
    }
  ],
  "smallest_falsifier": {
    "controlled_variables": [
      "initial_partition",
      "move_order",
      "quality_function",
      "random_seed",
      "resolution_parameter",
      "tie_rules",
      "worker count",
      "worker queue cap",
      "worker delta-buffer cap",
      "synchronization representation"
    ],
    "failure_signal": "The queue misses an improving move, reaches a different stable partition under identical choices, contains duplicate live entries, exceeds its admitted state bound, or fails to switch when queue work exceeds the modeled sweep work.",
    "fixture": "A small sparse community graph with a known local affected set and a degree-controlled dense-cut graph where accepted moves touch nearly all eligible nodes.",
    "independent_oracle": "A deterministic full-sweep local-move implementation using the same quality function, choices, and ordering, plus component-attributed shared-queue, private-worker-queue, delta-buffer, synchronization-membership, adjacency, and aggregate-RSS traces."
  },
  "source_pattern_ids": [
    "PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY"
  ],
  "surviving_invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "For the source local quality dependency, after moving a node, reevaluation need only be reintroduced for affected neighbors outside the node's new community, with duplicate queued entries suppressed.",
    "uncertainty": "The invariant does not apply when a move changes nonlocal opportunities."
  },
  "target_algorithm_families": [
    "INCREMENTAL_ACTIVE_SET_ALGORITHMS",
    "LABEL_PROPAGATION",
    "LEIDEN_COMMUNITY_DETECTION",
    "LOUVAIN_COMMUNITY_DETECTION"
  ],
  "transfer_id": "XFER-REQUEUE-DEPENDENCY-AFFECTED-STATE",
  "unknown_measurement_constants": [
    "a_adjacency_read",
    "a_checkpoint_overlap",
    "b_community_stat",
    "b_membership",
    "b_queue_entry",
    "b_queue_spill",
    "b_sync_membership",
    "b_worker_delta",
    "b_worker_queue",
    "t_partition_init",
    "t_queue_init",
    "t_stat_init"
  ]
}
```
