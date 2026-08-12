# Revisit Changed Neighborhoods Only

- Pattern ID: `PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus exposes a local update whose dependency region is exactly neighboring vertices.",
      "Duplicate queue entries can be suppressed without changing update semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY:invariant",
      "PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY:ram",
      "PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY:fails_when"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A Knight Bus local-optimization scheduler could retain an explicit affected-node queue instead of rescanning all vertices, but admission must reserve a full n-entry queue and treat repeated requeue count and adjacency traffic as workload-dependent unknowns.",
    "uncertainty": "The analogy is not established for unrelated Knight Bus algorithms, and queue revisit counts have not been measured."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Initialize the queue with all nodes in random order, repeatedly remove the front node, evaluate and possibly apply one move, append newly affected neighbors at the rear, and stop when the queue is empty.",
    "uncertainty": "A node can be revisited multiple times after different neighboring moves."
  },
  "confidence_rationale": {
    "assumptions": [
      "The benchmark implementation uses the specified fast local move queue."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Algorithm A.2 specifies queue transitions.",
      "Lemma 2 reasons about queue coverage.",
      "Section IV reports source runtime evidence for the combined Leiden algorithm."
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-004"
    ],
    "text": "Confidence is moderate because the queue rule is explicit and participates in the source's proofs and benchmarks, but its isolated performance contribution and external-memory resources were not measured or reproduced here.",
    "uncertainty": "No isolated ablation, code inspection, independent reproduction, or disk-backed test was performed."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "State consists of the current partition, graph adjacency, a queue of node identifiers, queue-membership tracking, and community statistics sufficient to evaluate quality deltas.",
    "uncertainty": "Queue-membership representation and community-statistic layout are not specified in pseudocode."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [
        "Move and enqueue overhead remains comparable to a full-sweep node evaluation."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The mechanism saves work only by omitting unaffected nodes from later evaluations."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "text": "The scheduling advantage collapses when most accepted moves affect broad neighborhoods and repeatedly refill the queue with nearly all nodes.",
      "uncertainty": "The paper does not isolate a measured queue-saturation breakpoint."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY",
  "falsifying_test": {
    "controlled_variables": [
      "initial partition",
      "node order",
      "random seed",
      "quality function",
      "resolution parameter"
    ],
    "failure_signal": "The queue implementation reaches a different locally stable quality/partition under identical choices, misses an improving move found by the oracle, or holds duplicate entries beyond the one-per-node invariant",
    "fixture": "A small undirected graph where one community move affects a known subset of neighbors, plus a dense case where every move can requeue almost all nodes",
    "independent_oracle": "A full-sweep local-move implementation using the same quality function, tie rules, and random seed",
    "scope": "Smallest mechanism test description only; no G09 experiment packet exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "All nodes are initially eligible, and after moving a node only its neighbors outside the node's new community that are not already queued are added for reevaluation.",
    "uncertainty": "Correctness is tied to the source's local quality-delta dependency."
  },
  "knight_bus_algorithm_families": [
    "LEIDEN_COMMUNITY_DETECTION",
    "LOUVAIN_COMMUNITY_DETECTION",
    "LABEL_PROPAGATION",
    "INCREMENTAL_ACTIVE_SET_ALGORITHMS"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Replace repeated global node sweeps with a work queue: pop one node, take its best strictly improving move, and enqueue only neighbors whose move opportunities may have changed until the queue is empty.",
    "uncertainty": "The source randomizes initial queue order and does not claim a deterministic move sequence."
  },
  "name": "Revisit Changed Neighborhoods Only",
  "pattern_id": "PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Repeatedly sweeping every node after local community changes wastes work on nodes whose neighborhood assignments have not changed.",
    "uncertainty": "The source frames wasted work inside Louvain-style local moving."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A queued node's best community and quality delta are recomputed when it is popped, while its eligibility is reintroduced only by a relevant neighboring move.",
    "uncertainty": "The source does not describe caching quality deltas between queue visits."
  },
  "related_pattern_ids": [
    "PAT-REACTIVATE-CHANGED-NEIGHBORS-ONLY",
    "PAT-REFINE-COMMUNITIES-BEFORE-AGGREGATION"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "The current graph, node-to-community assignments, queue and membership state, and quality-delta statistics remain resident during local moving.",
    "uncertainty": "The source does not quantify bytes or queue allocation policy."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "For a disk-backed adaptation, measure adjacency bytes read per queue visit and any spill writes.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper evaluates an in-memory implementation and gives no storage-I/O model."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Define checkpoint semantics and measure retained partition and queue state.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "No durable queue, checkpoint, or partition-storage contract is specified."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "initialize the work queue with all nodes in randomized order and initialize partition/community statistics",
      "measurement_needed": "Measure queue/statistic initialization wall time and bytes touched.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "Initialization time and statistic-building cost are not reported separately."
    },
    "ram": {
      "assumptions": [
        "The queue holds at most one entry per node and membership uses one bit per node."
      ],
      "expression": "graph_and_partition_state + n * queue_entry_bytes + n * queue_membership_bits + community_statistic_bytes",
      "measurement_needed": "Measure peak queue length, capacity, membership bytes, and total RSS.",
      "premises": [
        "The queue is initialized with every node and avoids duplicate queued entries."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "DERIVED",
      "uncertainty": "Queue container overhead, graph representation, and community statistics are not bounded."
    },
    "temporary_storage": {
      "assumptions": [
        "Queue and membership state are released after the local-moving phase."
      ],
      "expression": "queue_capacity_bytes + queue_membership_bytes",
      "measurement_needed": "Measure maximum queue capacity and membership allocation during the hardest local-moving phase.",
      "premises": [
        "The fast local move procedure requires a queue and suppresses duplicate queued nodes."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "DERIVED",
      "uncertainty": "Container capacity growth and identifier width are implementation-dependent."
    }
  },
  "source_domain": "incremental local-move scheduling for community detection",
  "source_paper_ids": [
    "PAPER-1810.08473"
  ],
  "source_pointers": [
    {
      "claim_scope": "Queue initialization, move test, affected-neighbor requeue, and termination",
      "locator_type": "SECTION",
      "locator_value": "Section III, fast local move procedure",
      "page": 5,
      "paper_id": "PAPER-1810.08473",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Exact changed-neighborhood queue algorithm",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm A.2, MoveNodesFast, lines 13-24",
      "page": 15,
      "paper_id": "PAPER-1810.08473",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Queue-state property used by the connectivity proof",
      "locator_type": "LEMMA",
      "locator_value": "Lemma 2 and surrounding notation",
      "page": 18,
      "paper_id": "PAPER-1810.08473",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Source runtime scaling on benchmark and empirical networks",
      "locator_type": "FIGURE",
      "locator_value": "Figures 7 and 8",
      "page": 9,
      "paper_id": "PAPER-1810.08473",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Source conclusion that fast local move contributes to Leiden runtime",
      "locator_type": "SECTION",
      "locator_value": "Section V, Discussion",
      "page": 10,
      "paper_id": "PAPER-1810.08473",
      "pointer_id": "SP-005"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "For each popped node, the procedure traverses its neighboring communities to select a move and traverses neighbors again as needed to enqueue affected nodes.",
    "uncertainty": "Algorithm A.2 does not specify disk-backed adjacency access."
  },
  "unknown_when": [
    {
      "assumptions": [
        "No uncited section of the fully read paper resolves the named boundary."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The cited source pointers delimit the mechanism, evaluated conditions, or stated analysis."
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "text": "The source does not establish the same queue dependency rule for directed quality functions, external-memory adjacency, or graph mutations during local moving.",
      "uncertainty": "The formal graph model is static and undirected."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-002",
        "SP-005"
      ],
      "text": "A node move changes move opportunities only in its local neighborhood, so most nodes need not be revisited after each change.",
      "uncertainty": "The benchmark speedup combines this scheduler with other Leiden changes."
    }
  ]
}
```
