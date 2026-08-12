# Prune Neighbors By Diversity

- Pattern ID: `PAT-PRUNE-NEIGHBORS-BY-DIVERSITY`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus exposes graph-ANNS index construction and search as an admitted workload family.",
      "The selected pruning rule and candidate capacities are known before execution."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source ties index size and search memory to retained degree and candidate-set size.",
      "The source evaluates connectivity as an independent performance-relevant component."
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-004"
    ],
    "text": "A Knight Bus admission estimate for this family should include retained edge count, maximum and average out-degree, construction candidate count, search candidate capacity, and a connectivity condition instead of deriving RAM from input vectors alone.",
    "uncertainty": "The source does not provide a whole-process estimator or enforceable memory coefficient."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-005"
    ],
    "text": "Candidate neighbors are considered in increasing distance order and tested against neighbors already retained for the same vertex.",
    "uncertainty": "The exact comparison schedule varies for angle-maximizing implementations."
  },
  "confidence_rationale": {
    "assumptions": [
      "The cited paper and pointers accurately represent the evaluated mechanism."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The cited source pointers describe or evaluate the mechanism.",
      "G05 did not independently reproduce the source result or inspect implementation code."
    ],
    "source_pointer_ids": [
      "SP-003",
      "SP-005"
    ],
    "text": "The paper combines an analytical directional-separation argument with component-controlled benchmarks on multiple real and synthetic datasets, but this campaign did not reproduce them.",
    "uncertainty": "The paper's implementations and benchmark measurements were not independently inspected or rerun."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Each vertex has a candidate-neighbor set and a smaller retained adjacency list whose entries encode both proximity and directional coverage.",
    "uncertainty": "Candidate-set construction differs across the compared algorithms."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-004"
      ],
      "text": "Pruning without a separate connectivity guarantee can leave unreachable components and degrade search performance.",
      "uncertainty": "The failure threshold depends on graph construction and data geometry."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-PRUNE-NEIGHBORS-BY-DIVERSITY",
  "falsifying_test": {
    "controlled_variables": [
      "vector count",
      "dimension",
      "candidate count",
      "retained degree",
      "search candidate capacity",
      "distance function"
    ],
    "failure_signal": "Diversity pruning fails to reduce retained edges or search memory, or loses connectivity or recall at the controlled search budget",
    "fixture": "A small clustered vector dataset whose unpruned exact neighbor graph is connected and whose candidate sets contain many same-direction neighbors",
    "independent_oracle": "Exhaustive nearest-neighbor results plus a graph connectivity check",
    "scope": "Smallest mechanism-level falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-005"
    ],
    "text": "Retained neighbors must be close to the vertex while also satisfying a directional-separation rule; the analyzed NSG rule implies at least sixty degrees between retained neighbor directions.",
    "uncertainty": "The angular lemma is specific to the analyzed Euclidean rule and is not a universal bound for every diversity heuristic."
  },
  "knight_bus_algorithm_families": [
    "APPROXIMATE_NEAREST_NEIGHBOR_SEARCH"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-005"
    ],
    "text": "Visit candidate neighbors from nearest to farthest and reject a candidate when an already retained neighbor makes its direction redundant, producing an approximation to a relative-neighborhood graph.",
    "uncertainty": "Different algorithms implement the diversity test with distance, angle, or relaxed parameters."
  },
  "name": "Prune Neighbors By Diversity",
  "pattern_id": "PAT-PRUNE-NEIGHBORS-BY-DIVERSITY",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Keeping many nearby neighbors that point in similar directions adds redundant distance evaluations and enlarges the graph index.",
    "uncertainty": "The observation is bounded to graph-based ANNS indexes studied by the paper."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-005"
    ],
    "text": "Construction recomputes candidate-to-vertex and candidate-to-retained-neighbor distance or angle tests while selecting each adjacency list.",
    "uncertainty": "The paper reports different construction frameworks, so the total count is algorithm-dependent."
  },
  "related_pattern_ids": [
    "PAT-BOUND-SEARCH-CANDIDATE-FRONTIER"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "The resulting graph index retains the pruned adjacency lists; index size is primarily coupled to retained average out-degree.",
    "uncertainty": "Whole-process allocator and vector-storage overheads are not isolated."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure bytes read per query with the pruned index stored on the target medium.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The principal evaluation is in memory and does not bound bytes transferred for this mechanism."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "Persistent index storage scales with retained adjacency entries and any auxiliary seed index.",
      "measurement_needed": "Measure serialized index bytes, retained edge count, and auxiliary-index bytes separately.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "Serialization metadata and vector payload size are not normalized by the source."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Preprocessing performs candidate acquisition followed by per-vertex diversity pruning; its cost depends on candidate count and the selected pairwise tests.",
      "measurement_needed": "Measure build time and distance-test count for the exact pruning rule.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "The source compares several implementations rather than one universal cost."
    },
    "ram": {
      "assumptions": [],
      "expression": "RAM includes retained adjacency entries plus the search candidate set; reducing out-degree and candidate-set size reduces index and search memory.",
      "measurement_needed": "Measure peak whole-process RSS while varying retained degree and candidate capacity on a fixed fixture.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "The source does not give a portable whole-process coefficient."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "Construction temporarily holds a candidate set and the growing retained-neighbor set for each processed vertex.",
      "measurement_needed": "Measure peak temporary bytes as build parallelism and candidate count vary.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "SOURCED",
      "uncertainty": "Peak parallel construction multiplicity is not bounded."
    }
  },
  "source_domain": "graph-based approximate nearest-neighbor search",
  "source_paper_ids": [
    "PAPER-2101.12631"
  ],
  "source_pointers": [
    {
      "claim_scope": "Distance-and-direction neighbor selection mechanisms",
      "locator_type": "SECTION",
      "locator_value": "Section 4.1, C3 Neighbor Selection",
      "page": 7,
      "paper_id": "PAPER-2101.12631",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Pruning, degree, and index-size consequences",
      "locator_type": "SECTION",
      "locator_value": "Section 5.2, Index Size and Average Out-degree",
      "page": 9,
      "paper_id": "PAPER-2101.12631",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Controlled comparison of neighbor-selection variants",
      "locator_type": "FIGURE",
      "locator_value": "Figure 10(c), neighbor-selection component evaluation",
      "page": 11,
      "paper_id": "PAPER-2101.12631",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Degree, direction diversity, connectivity, and memory guidance",
      "locator_type": "SECTION",
      "locator_value": "Section 6, Guidelines",
      "page": 12,
      "paper_id": "PAPER-2101.12631",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Directional-separation invariant and DPG approximation argument",
      "locator_type": "LEMMA",
      "locator_value": "Lemma 7.1 and Appendix C",
      "page": 17,
      "paper_id": "PAPER-2101.12631",
      "pointer_id": "SP-005"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-004"
    ],
    "text": "The paper evaluates core algorithms in main memory and leaves SSD, GPU, and distributed execution outside its primary scope.",
    "uncertainty": "Streaming behavior is therefore not established by this source."
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
        "SP-004"
      ],
      "text": "The source does not establish the rule for structured filters, dynamic updates, SSD execution, or graph algorithms other than ANNS.",
      "uncertainty": "These settings are explicitly outside or beyond the paper's evaluated scope."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "text": "The mechanism works when a small set of directionally distributed neighbors retains useful routes while redundant nearby directions can be removed.",
      "uncertainty": "Effect size varies substantially with dataset characteristics."
    }
  ]
}
```
