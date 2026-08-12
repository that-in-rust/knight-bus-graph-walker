# Recompute Neighbor Weights Locally

- Pattern ID: `PAT-RECOMPUTE-NEIGHBOR-WEIGHTS-LOCALLY`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus implements the same local-move state."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source states the memory overhead and lower-memory alternative.",
      "The source reports recomputation as faster than maintained maps in its implementation."
    ],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "A007 can price two explicit plans: a faster per-worker scratch plan with worker-count times vertex-count RAM, and a lower-memory plan with more map or synchronization overhead, refusing the former when the declared budget cannot hold its scratch.",
    "uncertainty": "The speed crossover and whole-process coefficients require measurement."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "On each local-move evaluation, scan the vertex neighborhood, aggregate edge weights by current community into thread-local scratch, evaluate the best move, and discard or reset the scratch entries.",
    "uncertainty": "Scratch reset details are not reported."
  },
  "confidence_rationale": {
    "assumptions": [
      "The benchmarked implementation matches the text."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The paper describes both the discarded and current implementations."
    ],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "The implementation tradeoff is explicitly reported, including a memory formula and fallback, but source code and performance were not checked in this campaign.",
    "uncertainty": "Only source-reported performance supports the choice."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Global community volumes are shared; each worker may own a vertex-sized aggregation vector instead of every vertex owning a lock-protected map.",
    "uncertainty": "NONE"
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "The faster variant becomes unsuitable under tight memory because its per-thread vectors scale with both workers and vertices.",
      "uncertainty": "The source offers PLM* as the lower-memory fallback rather than a universal breakpoint."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-RECOMPUTE-NEIGHBOR-WEIGHTS-LOCALLY",
  "falsifying_test": {
    "controlled_variables": [
      "Graph, labels, move order, worker count, scratch representation, and synchronization strategy."
    ],
    "failure_signal": "Any selected move differs from the oracle for the same observed state or scratch allocation exceeds the declared worker-times-vertex bound.",
    "fixture": "A small weighted graph with vertices adjacent to several communities, tested under one and multiple workers.",
    "independent_oracle": "A sequential implementation that recomputes exact modularity gain from current labels.",
    "scope": "Local aggregation correctness and memory-plan accounting."
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "The modularity-gain calculation needs the current neighborhood aggregation and community volumes, not a permanently maintained per-vertex community map.",
    "uncertainty": "Concurrent moves may still make observations stale."
  },
  "knight_bus_algorithm_families": [
    {
      "assumptions": [
        "Knight Bus exposes neighborhood scans and thread-private reusable scratch."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source applies local recomputation to Louvain modularity gains."
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "Louvain and Leiden local-move community detection are direct matches; other iterative algorithms may use the recompute-versus-retain trade only with different state definitions.",
      "uncertainty": "Transfer to PageRank, WCC, or BFS is not source-established."
    }
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Recompute weights to neighboring communities whenever a vertex is evaluated, persist only community volumes, and use thread-private vectors for fast aggregation when memory permits.",
    "uncertainty": "The lower-memory PLM* variant remains available when the per-thread vectors are too costly."
  },
  "name": "Recompute Neighbor Weights Locally",
  "pattern_id": "PAT-RECOMPUTE-NEIGHBOR-WEIGHTS-LOCALLY",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Maintaining and locking a mutable per-vertex map of weights to neighboring communities creates map-operation and synchronization overhead.",
    "uncertainty": "NONE"
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Neighbor-community weight aggregation is recomputed for each vertex evaluation rather than incrementally maintained across moves.",
    "uncertainty": "NONE"
  },
  "related_pattern_ids": [
    "PAT-MERGE-THREAD-LOCAL-COARSENINGS",
    "PAT-REACTIVATE-CHANGED-NEIGHBORS-ONLY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Community volumes and, in the faster variant, one vertex-sized vector per worker remain resident.",
    "uncertainty": "The source states asymptotic memory but not bytes."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure adjacency bytes fetched and cache misses per local move.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Storage I/O is outside the shared-memory evaluation."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure only if checkpointing these values is required.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "No persistent representation is defined for the scratch or volumes."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure initialization and per-iteration vector reset work.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Scratch initialization and community-volume initialization costs are not separated."
    },
    "ram": {
      "assumptions": [],
      "expression": "Shared community volumes plus one vertex-sized aggregation vector per worker in the faster variant; the source states O(worker-count times vertex-count) memory.",
      "measurement_needed": "Measure per-thread scratch bytes and peak RSS by thread and vertex count.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "SOURCED",
      "uncertainty": "Element width, clearing strategy, and allocator overhead are not reported."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "Thread-private aggregation vectors replace per-node mutable maps in the faster implementation.",
      "measurement_needed": "Measure touched entries, clearing bandwidth, and per-thread peak scratch.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "SOURCED",
      "uncertainty": "The vectors are resident across evaluations but logically serve as reusable scratch."
    }
  },
  "source_domain": "parallel Louvain local-move evaluation",
  "source_paper_ids": [
    "PAPER-1304.4453"
  ],
  "source_pointers": [
    {
      "claim_scope": "Replace lock-protected per-node maps with local recomputation; retain community volumes; per-thread vector speed/memory tradeoff.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section III.B, PLM Implementation",
      "page": 5,
      "paper_id": "PAPER-1304.4453",
      "pointer_id": "SP-001"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "The current vertex adjacency list is scanned to reconstruct neighboring-community weights.",
    "uncertainty": "The source assumes shared-memory adjacency."
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
        "SP-001"
      ],
      "text": "The source does not isolate when adjacency rescanning becomes more expensive than maintaining a compact synchronized summary.",
      "uncertainty": "Crossover depends on degree, community count, cache, and thread count."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "Local adjacency scans and thread-private aggregation cost less than synchronized map maintenance, and memory can hold the per-thread vectors.",
      "uncertainty": "The source reports an average speed improvement on its implementation."
    }
  ]
}
```
