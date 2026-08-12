# Prune Settled Search State

- Pattern ID: `PAT-PRUNE-SETTLED-SEARCH-STATE`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus exposes exact BFS tree state and can persist pruned parent edges.",
      "Admission favors conservative bounds over optimistic runtime pruning."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-PRUNE-SETTLED-SEARCH-STATE:mechanism",
      "PAT-PRUNE-SETTLED-SEARCH-STATE:unknown_when",
      "PAT-PRUNE-SETTLED-SEARCH-STATE:io"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-003",
      "SP-005"
    ],
    "text": "A Knight Bus receipt could report released resident tree slots and reduced edge-stream bytes by iteration, while admission must conservatively assume no pruning unless artifact analysis can prove zero-degree or settled-state reductions before execution.",
    "uncertainty": "The source offers no pre-run pruning-yield model, and Knight Bus reconstruction/storage coefficients are unmeasured."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "After each ER pass, advance FR/FC/FCC, move settled parent edges from T-bar to ET, decide whether to build or adopt Enext, and exclude settled prefixes from later scans before final Reset.",
    "uncertainty": "How often a smaller Enext is materialized depends on graph reduction progress."
  },
  "confidence_rationale": {
    "assumptions": [
      "The source implementation persists and reconstructs pruned state as described."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Algorithm 3 defines vPrune, ErPrune, and Reset transitions.",
      "Theorem 5.4 addresses correctness with removed trivial-degree nodes.",
      "Section 6.1 attributes benchmark benefits partly to pruning."
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-004",
      "SP-005"
    ],
    "text": "Confidence is moderate because split state, pruning, reconstruction, correctness, and source benchmark explanations are present, but pruning yield is not predictable before the run and no independent reproduction was performed.",
    "uncertainty": "No code inspection, reproduction, crash-recovery review, or isolated pruning ablation occurred."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-003"
    ],
    "text": "Tree state is split into resident T-bar and disk-resident ET; edge work is split into current ER and candidate Enext; zero-in-degree and zero-out-degree edges are held separately for final Reset.",
    "uncertainty": "The source does not give a crash-consistent on-disk format for these sets."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [
        "Rewrite and bookkeeping overhead remains nonzero when reduction is negligible."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "Pruning reduces reducer calls and scan bytes only by removing resident tree edges, vertices, or ER edges."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "text": "The pruning advantage disappears when no nodes settle early, no trivial-degree nodes can be removed, and Enext remains nearly as large as ER, although the underlying BFS procedure may remain correct.",
      "uncertainty": "The source does not report an isolated no-pruning ablation."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-PRUNE-SETTLED-SEARCH-STATE",
  "falsifying_test": {
    "controlled_variables": [
      "neighbor order",
      "edge-stream order",
      "pruning thresholds",
      "K",
      "Reset ordering rules"
    ],
    "failure_signal": "Any node or parent edge is missing or misplaced after Reset, or a pruned edge later proves necessary to match the oracle",
    "fixture": "A disconnected directed graph containing one zero-in-degree node, one zero-out-degree node, one early-settled BFS subtree, and one component that settles only at the end",
    "independent_oracle": "A deterministic in-memory BFS forest over the unreduced graph",
    "scope": "Smallest mechanism test description only; no G09 experiment packet exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-004"
    ],
    "text": "A node or tree edge is removed from resident search state only after its final position can be preserved in disk-resident ET or reconstructed by Reset; the returned tree still spans every original node.",
    "uncertainty": "The formal pruning rules rely on the paper's BFS-order thresholds and zero-degree cases."
  },
  "knight_bus_algorithm_families": [
    "BFS",
    "UNWEIGHTED_SHORTEST_PATHS",
    "REACHABILITY",
    "CONNECTED_COMPONENTS"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-003"
    ],
    "text": "Represent the evolving BFS tree as a resident portion plus disk-resident parent edges, advance a settled breadth-first prefix, prune its vertices and irrelevant edges from subsequent work, materialize a smaller Enext edge stream when worthwhile, and reconstruct the full result at termination.",
    "uncertainty": "The trigger for materializing Enext includes a source parameter and is not shown to be universally optimal."
  },
  "name": "Prune Settled Search State",
  "pattern_id": "PAT-PRUNE-SETTLED-SEARCH-STATE",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Keeping the complete evolving spanning tree resident consumes edge slots that could otherwise hold a larger scanned-edge batch and forces more in-memory reducer invocations.",
    "uncertainty": "The argument is specific to the paper's fixed-capacity semi-external sketch."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "The unsettled resident tree is rebuilt by EP-Reduce, reduced edge state may be rewritten as Enext, and the complete final tree is reconstructed from T-bar, ET, Ei, and Eo by Reset.",
    "uncertainty": "The paper does not quantify reconstruction cost separately."
  },
  "related_pattern_ids": [
    "PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Only the unsettled tree portion, current admitted edge batch, per-node attributes, BON order, and pruning thresholds remain resident; settled tree edges move to ET.",
    "uncertainty": "Per-node arrays remain resident even when a node's tree edge is pruned."
  },
  "resource_model": {
    "io": {
      "assumptions": [
        "Each current ER is scanned sequentially once per outer iteration."
      ],
      "expression": "sum_iteration_bytes(current_ER) + ET_write_bytes + final_reconstruction_reads",
      "measurement_needed": "Record physical bytes for ER scans, ET writes, Enext rewrites, and Reset.",
      "premises": [
        "The source scans a reduced ER, writes pruned tree edges to ET, and reads retained pieces for Reset."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "status": "DERIVED",
      "uncertainty": "The reduction ratio, ET layout, page-cache effects, and reconstruction traffic are not bounded."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "current_ER + disk_resident_ET + Ei + Eo + retained_input_segments",
      "measurement_needed": "Measure retained successful-run storage and peak generation overlap.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "The paper does not state whether prior ER generations and input segments coexist through completion."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "initial scan identifies zero-in-degree and zero-out-degree cases, builds initial ER, and initializes split tree state",
      "measurement_needed": "Measure initial degree classification, ER construction, and ET initialization costs.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "The source does not isolate this cost from EP-BFS initialization."
    },
    "ram": {
      "assumptions": [],
      "expression": "unsettled_Tbar_edge_slots + current_E_edge_slots + 2 * n * node_attribute_bytes + BON_and_pruning_metadata_bytes",
      "measurement_needed": "Measure peak RSS and released sketch slots as the settled prefix advances.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "Pruning releases resident edge slots but not the paper's global per-node arrays."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak temporary bytes while ER and Enext coexist and during final Reset.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The source names Enext and rewrite state but does not specify peak temporary-storage lifetime or cleanup."
    }
  },
  "source_domain": "semi-external exact breadth-first search state reduction",
  "source_paper_ids": [
    "PAPER-2507.12925"
  ],
  "source_pointers": [
    {
      "claim_scope": "Splitting T into resident T-bar and disk-resident ET, removing fixed nodes, and shrinking the scanned graph",
      "locator_type": "SECTION",
      "locator_value": "Section 5, Overview, first and final paragraphs",
      "page": 9,
      "paper_id": "PAPER-2507.12925",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Threshold advancement, vertex pruning, edge-stream pruning, and final Reset",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 3, lines 25-30",
      "page": 10,
      "paper_id": "PAPER-2507.12925",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Conditions and reconstruction behavior for pruned vertices, tree edges, and reduced edge streams",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 5.1, paragraphs explaining vPrune, ErPrune, Enext, and Reset",
      "page": 12,
      "paper_id": "PAPER-2507.12925",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Correctness when nodes with zero in-degree or zero out-degree are removed and later restored",
      "locator_type": "THEOREM",
      "locator_value": "Theorem 5.4, proof part (2)",
      "page": 13,
      "paper_id": "PAPER-2507.12925",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Observed benefit from zero-degree pruning and reduced resident sketch on real graphs",
      "locator_type": "TABLE",
      "locator_value": "Table 2 discussion in Section 6.1",
      "page": 15,
      "paper_id": "PAPER-2507.12925",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "Source explanation that partial-tree residency and edge reduction improve scaling",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 6.7, final paragraph",
      "page": 19,
      "paper_id": "PAPER-2507.12925",
      "pointer_id": "SP-006"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "Later iterations stream the current reduced edge list rather than the entire original graph, while pruned tree edges remain in disk-resident ET until reconstruction.",
    "uncertainty": "Actual physical-byte reduction depends on how much of ER is removed and when Enext is adopted."
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
        "SP-001",
        "SP-003"
      ],
      "text": "The source does not provide a pre-run estimator for how many vertices or edges will become prunable in each iteration.",
      "uncertainty": "Pruning yield is observed only as execution advances."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-004",
        "SP-005"
      ],
      "text": "The graph exposes zero-degree nodes, a growing settled breadth-first prefix, or edge regions that can be removed from later scans without changing the final BFS tree.",
      "uncertainty": "The magnitude of benefit is graph-structure dependent."
    }
  ]
}
```
