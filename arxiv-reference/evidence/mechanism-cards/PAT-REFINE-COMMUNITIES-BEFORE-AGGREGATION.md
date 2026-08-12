# Refine Communities Before Aggregation

- Pattern ID: `PAT-REFINE-COMMUNITIES-BEFORE-AGGREGATION`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus implements Leiden semantics for an undirected graph and supported quality function.",
      "Its receipt can report iteration stage and the strongest guarantee actually reached."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-REFINE-COMMUNITIES-BEFORE-AGGREGATION:invariant",
      "PAT-REFINE-COMMUNITIES-BEFORE-AGGREGATION:temporary_storage",
      "PAT-REFINE-COMMUNITIES-BEFORE-AGGREGATION:fails_when"
    ],
    "source_pointer_ids": [
      "SP-004",
      "SP-005",
      "SP-008"
    ],
    "text": "A Knight Bus community-detection admission and receipt should separate the one-iteration connectivity guarantee from stable and asymptotic guarantees, reserve simultaneous refined-partition and aggregate-graph state, and expose a bounded-iteration stop as a weaker result rather than claiming subset optimality.",
    "uncertainty": "Peak aggregate-state bytes, iteration count, and directed-graph applicability are not bounded by the paper for Knight Bus."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-005"
    ],
    "text": "Run fast local moves, refine each coarse community, aggregate by refined communities, preserve coarse assignments in the aggregate graph, and repeat levels and outer iterations until no further improvement is available.",
    "uncertainty": "A stable iteration need not be asymptotically final because stochastic refinement can enable later improvements."
  },
  "confidence_rationale": {
    "assumptions": [
      "The implementation used in the benchmarks conforms to Algorithm A.2."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Algorithm A.2 specifies refinement and aggregation.",
      "Theorems 4 and 5 establish per-iteration separation and connectivity.",
      "Section IV reports source benchmark evidence."
    ],
    "source_pointer_ids": [
      "SP-005",
      "SP-006",
      "SP-007"
    ],
    "text": "Confidence is high within grade C because the refinement is given in pseudocode, connectivity is proved, and the paper reports benchmark evidence, but this campaign did not inspect code or reproduce results and resource bounds are largely absent.",
    "uncertainty": "No independent reproduction, code inspection, directed-graph proof, or whole-process resource measurement occurred."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-005"
    ],
    "text": "Each level retains a current graph, a coarse partition P, a refined partition P_refined initialized to singletons within P, and an aggregate multigraph whose nodes are refined communities but whose initial assignments follow P.",
    "uncertainty": "The source's mathematical model treats aggregate edges as a multiset and does not specify a byte-level layout."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-004",
        "SP-008"
      ],
      "text": "Stopping after one stable iteration does not establish the asymptotic subset-optimality guarantee, and difficult empirical networks can require many additional iterations before stability.",
      "uncertainty": "Failure here means the stronger convergence claim is unavailable, not that per-iteration connectivity is lost."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-REFINE-COMMUNITIES-BEFORE-AGGREGATION",
  "falsifying_test": {
    "controlled_variables": [
      "node visitation order",
      "resolution parameter",
      "quality function",
      "random seed",
      "iteration limit"
    ],
    "failure_signal": "A completed Leiden iteration returns a disconnected community, decreases the quality function through refinement, or claims an asymptotic guarantee before the required convergence condition",
    "fixture": "The smallest bridge-node graph shaped like Figure 2, where moving the bridge disconnects a coarse Louvain community",
    "independent_oracle": "An induced-subgraph connectivity checker plus direct recomputation of the selected quality function for every reported partition",
    "scope": "Smallest mechanism test description only; no G09 experiment packet exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-005"
    ],
    "text": "Refinement occurs only inside each coarse community, and a singleton node or refined community is merged only when the required internal connectivity and non-decreasing quality conditions hold.",
    "uncertainty": "The formal guarantees require a positive refinement-randomness parameter."
  },
  "knight_bus_algorithm_families": [
    "LEIDEN_COMMUNITY_DETECTION",
    "LOUVAIN_COMMUNITY_DETECTION",
    "MODULARITY_OPTIMIZATION",
    "CPM_COMMUNITY_DETECTION"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-005"
    ],
    "text": "After local moving, split coarse communities through constrained randomized merges, aggregate the graph using the refined partition, and initialize aggregate-node communities from the original coarse partition before repeating.",
    "uncertainty": "The mechanism is stochastic and the exact partition can vary by seed."
  },
  "name": "Refine Communities Before Aggregation",
  "pattern_id": "PAT-REFINE-COMMUNITIES-BEFORE-AGGREGATION",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Louvain can move a bridge node out of a community and then aggregate the disconnected remainder into an indivisible supernode, producing arbitrarily badly connected communities.",
    "uncertainty": "The source analyzes modularity and CPM community optimization."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-005"
    ],
    "text": "P_refined is rebuilt from singleton communities for each refinement phase, and a new aggregate graph and its coarse-derived initial partition are built at each hierarchy level.",
    "uncertainty": "The source does not report rebuild cost separately from iteration runtime."
  },
  "related_pattern_ids": [
    "PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-005"
    ],
    "text": "The current base or aggregate graph, coarse and refined community assignments, node-move queue, connectivity/quality statistics, and aggregation mappings are resident during an iteration.",
    "uncertainty": "The paper does not report memory attributed to these structures."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "For an external adaptation, measure graph-level reads, aggregate writes, and spill traffic.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "No storage-I/O model is provided for the in-memory implementation."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Define checkpoint and result retention, then measure graph, partition, and hierarchy bytes.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not specify retained partitions, aggregate levels, checkpoints, or storage amplification."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure graph conversion and initial community-statistic construction on the named fixture.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Input conversion, undirected symmetrization, and quality-statistic initialization are not bounded separately."
    },
    "ram": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak RSS by hierarchy level, including base graph, refined partition, aggregate graph, and move queue.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper gives benchmark machine memory but no algorithm or whole-process peak-RAM bound."
    },
    "temporary_storage": {
      "assumptions": [
        "The implementation materializes rather than lazily views these structures."
      ],
      "expression": "P_refined_assignment_bytes + aggregate_graph_bytes + aggregation_map_bytes + move_queue_bytes",
      "measurement_needed": "Measure peak simultaneously live refined, aggregate, mapping, and queue allocations.",
      "premises": [
        "Algorithm A.2 materializes a refined partition and then an aggregate graph while preserving coarse assignments."
      ],
      "source_pointer_ids": [
        "SP-003",
        "SP-005"
      ],
      "status": "DERIVED",
      "uncertainty": "Aggregate edge multiplicity, representation width, and generation overlap are not bounded."
    }
  },
  "source_domain": "hierarchical community detection on undirected networks",
  "source_paper_ids": [
    "PAPER-1810.08473"
  ],
  "source_pointers": [
    {
      "claim_scope": "Bridge-node move that leaves a Louvain community disconnected",
      "locator_type": "FIGURE",
      "locator_value": "Figure 2 and Section II.A",
      "page": 2,
      "paper_id": "PAPER-1810.08473",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Three Leiden phases and motivation for a refinement phase",
      "locator_type": "SECTION",
      "locator_value": "Section III, first paragraph",
      "page": 3,
      "paper_id": "PAPER-1810.08473",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Refined partition drives aggregation while coarse partition initializes aggregate assignments",
      "locator_type": "FIGURE",
      "locator_value": "Figure 3 and Section III refinement paragraphs",
      "page": 4,
      "paper_id": "PAPER-1810.08473",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Per-iteration, stable-iteration, and asymptotic guarantees",
      "locator_type": "TABLE",
      "locator_value": "Table I and Section III.A",
      "page": 5,
      "paper_id": "PAPER-1810.08473",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Exact refine, aggregate, constrained merge, and initialization sequence",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm A.2, Leiden algorithm",
      "page": 15,
      "paper_id": "PAPER-1810.08473",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "Per-iteration gamma-separation and gamma-connectivity guarantees",
      "locator_type": "THEOREM",
      "locator_value": "Theorem 4 and Theorem 5 beginning",
      "page": 19,
      "paper_id": "PAPER-1810.08473",
      "pointer_id": "SP-006"
    },
    {
      "claim_scope": "Source runtime and partition-quality benchmark results",
      "locator_type": "FIGURE",
      "locator_value": "Figures 7 and 8, Sections IV.B-IV.C",
      "page": 9,
      "paper_id": "PAPER-1810.08473",
      "pointer_id": "SP-007"
    },
    {
      "claim_scope": "Many iterations may be needed for stability and later iterations can continue improving",
      "locator_type": "FIGURE",
      "locator_value": "Figure 10 and Section V discussion",
      "page": 10,
      "paper_id": "PAPER-1810.08473",
      "pointer_id": "SP-008"
    },
    {
      "claim_scope": "Open problem of an a-priori minimal-quality bound",
      "locator_type": "PARAGRAPH",
      "locator_value": "Appendix E, final paragraph",
      "page": 25,
      "paper_id": "PAPER-1810.08473",
      "pointer_id": "SP-009"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-005"
    ],
    "text": "Local move and refinement traverse neighbors in the current graph level, and aggregation traverses current edges to form weighted multi-edges between refined communities.",
    "uncertainty": "Algorithm A.2 is in-memory pseudocode and does not define external-memory streaming."
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
        "SP-009"
      ],
      "text": "The paper leaves an a-priori lower bound on the quality of an arbitrary uniformly gamma-dense partition open and does not establish the same guarantees for directed graphs.",
      "uncertainty": "The mathematical setup in Appendix A assumes undirected graphs."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-004",
        "SP-005",
        "SP-006"
      ],
      "text": "The input is an undirected network optimized with modularity or CPM, refinement randomness is positive, and connected community output is required at every completed iteration.",
      "uncertainty": "The exact quality and runtime remain graph- and seed-dependent."
    }
  ]
}
```
