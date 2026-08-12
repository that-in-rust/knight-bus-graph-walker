# Inline Low Degree Adjacencies

- Pattern ID: `PAT-INLINE-LOW-DEGREE-ADJACENCIES`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus receives a degree histogram before admission.",
      "The graph is static for the duration of preprocessing and execution."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source assigns adjacency placement by a degree threshold.",
      "The source reports opposing metadata and mini-data memory effects as the threshold changes."
    ],
    "source_pointer_ids": [
      "SP-003",
      "SP-005"
    ],
    "text": "Knight Bus could quote this layout by separating mini-adjacency RAM from large-adjacency SSD bytes and by choosing or refusing a degree threshold from the declared degree histogram and algorithm access family.",
    "uncertainty": "The source does not predict which degrees a future algorithm run will activate or supply a portable threshold model."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003"
    ],
    "text": "A mini vertex's degree and mini-data offset are computed from its reordered ID and theta-id boundary, while a large vertex follows its block offset to SSD data.",
    "uncertainty": "The source does not discuss concurrent graph updates to the reordered layout."
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
    "text": "The source specifies the ordering equations and reports memory/runtime sensitivity to the degree threshold, but this campaign did not inspect or reproduce the implementation.",
    "uncertainty": "Benchmark effects remain source-reported and layout-specific."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003"
    ],
    "text": "Large vertices and virtual boundaries are ordered by disk offset; mini vertices follow in descending-degree contiguous groups; theta-id records the first ID for each mini degree.",
    "uncertainty": "The paper uses a small fixed degree threshold in its implementation."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-005"
      ],
      "text": "Thresholds below the supported compact-metadata range increase per-block metadata, while larger thresholds eventually make the resident mini-data array dominate memory.",
      "uncertainty": "The observed optimum is specific to the paper's metadata layout and tested graph."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-INLINE-LOW-DEGREE-ADJACENCIES",
  "falsifying_test": {
    "controlled_variables": [
      "degree threshold",
      "block size",
      "vertex ordering",
      "virtual-vertex placement",
      "edge list"
    ],
    "failure_signal": "Any reordered vertex returns an incorrect degree or adjacency, a virtual vertex becomes reachable, or resident mini-data exceeds the declared threshold-based size",
    "fixture": "A tiny graph containing degree-zero through degree-above-threshold vertices plus one forced block-fragment boundary",
    "independent_oracle": "Original adjacency lists and degrees before reordering",
    "scope": "Smallest mechanism-level falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003"
    ],
    "text": "After reordering, large and virtual vertices preserve degree as the difference between consecutive offsets, while low-degree vertices occupy contiguous degree bands identified by theta-id boundaries.",
    "uncertainty": "Virtual vertices must remain unreachable to preserve algorithm correctness."
  },
  "knight_bus_algorithm_families": [
    "BFS",
    "WEAKLY_CONNECTED_COMPONENTS",
    "PERSONALIZED_PAGERANK",
    "PAGERANK",
    "K_CORE"
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
    "text": "Classify vertices by a degree threshold, keep low-degree adjacency lists in a compact in-memory mini-data array, leave larger lists in SSD blocks, and reorder plus insert virtual vertices so degrees and offsets need not be stored explicitly.",
    "uncertainty": "The selected threshold trades memory against I/O and is not universal."
  },
  "name": "Inline Low Degree Adjacencies",
  "pattern_id": "PAT-INLINE-LOW-DEGREE-ADJACENCIES",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Block partitioning breaks the CSR offset-difference degree invariant, while fetching disk blocks for tiny adjacency lists wastes I/O and storing explicit degree fields inflates RAM.",
    "uncertainty": "The problem is formulated for the paper's block-centric semi-external layout."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003"
    ],
    "text": "Mini-vertex degree and mini-data offset are derived algebraically at access time instead of being retained per vertex.",
    "uncertainty": "The arithmetic cost is constant time but hardware-dependent."
  },
  "related_pattern_ids": [
    "PAT-PRIORITIZE-RESIDENT-ACTIVE-BLOCKS"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003",
      "SP-004"
    ],
    "text": "The index array, vertex state, mini-data adjacency array, theta-id boundaries, and block metadata reside in memory.",
    "uncertainty": "Whole-process state also includes algorithm-specific vertex values."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Mini-vertex adjacency accesses use RAM; large-vertex adjacency accesses read SSD blocks, so I/O depends on activated vertices above the threshold and block locality.",
      "measurement_needed": "Count mini hits, large-list block reads, and bytes per algorithm.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-002",
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "The paper does not give a symbolic activation-degree distribution."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "Persistent storage includes large adjacency blocks and the vertex-ID relation used at initialization and termination; low-degree adjacency payload is moved to resident mini-data.",
      "measurement_needed": "Measure block payload, fragmentation, and ID-map storage separately.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "Block fragmentation and ID-map compression are workload-specific."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Preprocessing partitions adjacency lists, reorders large and mini vertices, inserts virtual vertices, creates theta-id, and writes an SSD-resident old-to-new ID relation.",
      "measurement_needed": "Measure preprocessing time, peak preprocessing memory, and ID-map bytes.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "Update and rebuild costs after graph mutation are not evaluated."
    },
    "ram": {
      "assumptions": [],
      "expression": "RAM = index array + mini-data adjacency entries + theta-id boundaries + vertex state + block metadata; explicit per-vertex degree fields are eliminated.",
      "measurement_needed": "Measure each resident component and peak RSS for candidate thresholds.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-003",
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "The optimum threshold depends on metadata width and degree distribution."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak preprocessing scratch storage for the admitted graph.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not isolate peak temporary storage for sorting, virtual insertion, and mini-data construction."
    }
  },
  "source_domain": "hybrid in-memory and SSD graph storage",
  "source_paper_ids": [
    "PAPER-2511.07886"
  ],
  "source_pointers": [
    {
      "claim_scope": "Mini-edge lists, index arrays, and virtual vertices",
      "locator_type": "FIGURE",
      "locator_value": "Figure 7 and Section 5, Hybrid Storage Architecture",
      "page": 15,
      "paper_id": "PAPER-2511.07886",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Locality-window partitioning and degree-based vertex classes",
      "locator_type": "SECTION",
      "locator_value": "Sections 5.1-5.2",
      "page": 16,
      "paper_id": "PAPER-2511.07886",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Theta-id boundaries and algebraic degree/offset recovery",
      "locator_type": "EQUATION",
      "locator_value": "Equation 3 and Section 5.2, Mini Edge List Optimization",
      "page": 17,
      "paper_id": "PAPER-2511.07886",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Measured memory trade-off and metadata overhead",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 6.2, memory-footprint discussion",
      "page": 20,
      "paper_id": "PAPER-2511.07886",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Threshold-dependent runtime and memory boundary",
      "locator_type": "FIGURE",
      "locator_value": "Figure 15, degree-threshold sensitivity",
      "page": 22,
      "paper_id": "PAPER-2511.07886",
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
    "text": "Adjacency lists above the threshold remain in partitioned SSD blocks and are loaded through the block execution path; mini lists avoid those block reads.",
    "uncertainty": "Actual block savings depend on which degree classes the algorithm activates."
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
      "text": "The paper does not establish maintenance cost or correctness under online vertex and edge updates to the reordered graph.",
      "uncertainty": "The evaluated storage image is preprocessed and static."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-005"
      ],
      "text": "The mechanism works when many activated vertices have small degrees and the chosen threshold balances metadata cost against avoided block access.",
      "uncertainty": "Algorithm access patterns can favor a different partitioner or threshold."
    }
  ]
}
```
