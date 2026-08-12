# Reactivate Changed Neighbors Only

- Pattern ID: `PAT-REACTIVATE-CHANGED-NEIGHBORS-ONLY`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "The algorithm exposes a sound reactivation rule."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source restricts scans to active vertices.",
      "The source shows work concentrated in early iterations."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A007 can quote per-iteration work from the active subgraph rather than all edges, while carrying resident active-state bytes and a worst-case fallback to a full scan when all vertices reactivate.",
    "uncertainty": "The active-set trajectory cannot be known exactly before execution."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Iterate only active nonisolated vertices in parallel, compute dominant neighbor labels, update shared labels, reactivate changed neighbors, and stop when updates fall below the configured threshold.",
    "uncertainty": "Guided scheduling is recommended for degree skew."
  },
  "confidence_rationale": {
    "assumptions": [
      "The published implementation follows Algorithm 1."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source provides pseudocode and empirical iteration traces."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "The active-set algorithm and iteration figures are explicit, but asynchronous behavior and thresholds were not reproduced.",
    "uncertainty": "No code inspection or independent execution occurred."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "A shared label array is indexed by consecutive vertex IDs; an active-set representation and update count accompany the adjacency array.",
    "uncertainty": "The concrete active-set encoding is not specified."
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
      "text": "A nonzero update threshold can stop before all vertices stabilize, trading residual quality for time.",
      "uncertainty": "The source reports negligible quality effect in preliminary experiments, not a guarantee."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-REACTIVATE-CHANGED-NEIGHBORS-ONLY",
  "falsifying_test": {
    "controlled_variables": [
      "Graph, initial labels, tie breaking, update threshold, thread count, and scheduling order."
    ],
    "failure_signal": "A sleeping vertex should change under the oracle but is not reactivated, or output differs when the threshold is zero.",
    "fixture": "A small graph with a stable region, a changing boundary, an oscillating bipartite component, and a full-reactivation case.",
    "independent_oracle": "Synchronous full-vertex iterations to the declared convergence criterion.",
    "scope": "Dirty-state correctness and conservative fallback."
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "A vertex whose neighborhood labels are unchanged cannot select a different dominant label; a changed vertex reactivates its neighbors before their state can become stale.",
    "uncertainty": "Stopping below a nonzero update threshold deliberately relaxes full convergence."
  },
  "knight_bus_algorithm_families": [
    {
      "assumptions": [
        "The target kernel has an explicit local dirty-state rule."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source sleeps vertices whose neighbor labels are unchanged."
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "Label propagation, WCC-style frontier convergence, and PageRank-like iterative kernels are candidate Knight Bus families only when neighbor changes fully determine reactivation.",
      "uncertainty": "PageRank residual propagation and WCC semantics differ and require separate correctness tests."
    }
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Maintain an active set, remove vertices that retain their dominant label, and reactivate the neighbors of every vertex whose label changes.",
    "uncertainty": "Parallel asynchronous reads may see old or new labels."
  },
  "name": "Reactivate Changed Neighbors Only",
  "pattern_id": "PAT-REACTIVATE-CHANGED-NEIGHBORS-ONLY",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Repeatedly rescanning vertices whose neighborhood labels did not change wastes most late-iteration work, especially when only a small residual set remains active.",
    "uncertainty": "The size of the residual depends on graph structure."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "A vertex's neighborhood label weights are recomputed only while that vertex is active.",
    "uncertainty": "NONE"
  },
  "related_pattern_ids": [
    "PAT-RECOMPUTE-NEIGHBOR-WEIGHTS-LOCALLY",
    "PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "The adjacency array, shared label array, active flags or set, and update counter remain resident.",
    "uncertainty": "Whole-process memory is not itemized."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure adjacency bytes read per active iteration under a declared storage tier.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The implementation is shared-memory and does not model storage I/O."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure durable adjacency and optional label-checkpoint bytes.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "No persistent graph or checkpoint format is specified."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Initialize each vertex with a unique label and mark all vertices active before iteration.",
      "measurement_needed": "Measure initialization and graph-load time separately.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "SOURCED",
      "uncertainty": "Graph loading and adjacency construction are not included."
    },
    "ram": {
      "assumptions": [],
      "expression": "Adjacency array plus one label per vertex, active-state tracking, and parallel iteration state.",
      "measurement_needed": "Measure peak RSS and bytes per vertex for labels and active tracking.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "SOURCED",
      "uncertainty": "Bytes for active tracking and OpenMP runtime are not reported."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Instrument peak per-thread scratch and active-update queues.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Per-thread neighborhood weight scratch and active-set mutation buffers are not quantified."
    }
  },
  "source_domain": "shared-memory iterative community detection",
  "source_paper_ids": [
    "PAPER-1304.4453"
  ],
  "source_pointers": [
    {
      "claim_scope": "Active/inactive labels, neighbor reactivation, update threshold, asynchronous updates, and guided scheduling.",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 1 and PLP implementation paragraphs",
      "page": 4,
      "paper_id": "PAPER-1304.4453",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Active and updated labels and per-iteration time over PLP execution.",
      "locator_type": "FIGURE",
      "locator_value": "Figures 12-13",
      "page": 15,
      "paper_id": "PAPER-1304.4453",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Observed early-iteration concentration of work and graph-structure-dependent quality.",
      "locator_type": "SECTION",
      "locator_value": "Section V.A, Parallel Label Propagation",
      "page": 7,
      "paper_id": "PAPER-1304.4453",
      "pointer_id": "SP-003"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Only adjacency lists of currently active vertices are scanned in an iteration.",
    "uncertainty": "The paper evaluates in-memory adjacency arrays rather than disk streaming."
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
      "text": "Convergence iteration count is not mathematically bounded and depends more on graph structure than size.",
      "uncertainty": "Adversarial oscillation behavior is not fully characterized."
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
      "text": "The iterative rule is local and vertex state can change only after a neighbor changes, allowing stable vertices to sleep.",
      "uncertainty": "NONE"
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "text": "Many vertices stabilize after early iterations, leaving a much smaller active residual.",
      "uncertainty": "Observed on the evaluated graph set."
    }
  ]
}
```
