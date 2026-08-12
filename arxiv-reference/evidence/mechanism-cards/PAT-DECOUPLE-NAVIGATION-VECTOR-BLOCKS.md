# Decouple Navigation Vector Blocks

- Pattern ID: `PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus has an ANN-like navigation/refinement algorithm with a conservative pruning oracle",
      "Page size, vector width, graph degree, and recall target are known before execution"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS: decoupling packs navigation separately but can require two block classes",
      "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS: pruning determines whether value-block reads are avoided"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-004"
    ],
    "text": "For Knight Bus similarity search, admission would need separate navigation-block and value-block estimates, a pruning-rate uncertainty term, and a page-packing check before choosing decoupled storage; a raw input-size estimate would miss both the two-read risk and skipped value reads.",
    "uncertainty": "The source does not establish this mechanism for general BFS/WCC/PageRank values or quantify estimator error."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Fetch compact navigation blocks during graph expansion, score/prune their candidates, and fetch raw-vector data blocks only for survivors requiring exact distance evaluation.",
    "uncertainty": "Cache hits and asynchronous execution can further alter ordering and latency."
  },
  "confidence_rationale": {
    "assumptions": [
      "Compared layouts use equivalent graph/search semantics at matched recall"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Section 3.2.1 defines coupled and decoupled access",
      "Section 4.3.2 reports source measurements across dimension and recall"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-004"
    ],
    "text": "Confidence is moderate because the layout mechanism and two-read trade-off are explicit and the source evaluates dimension/recall sensitivity, but the crossover is fixture-specific and no reproduction or code inspection occurred.",
    "uncertainty": "Evidence is source-reported and specific to disk ANN."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Index blocks contain many compact adjacency lists; data blocks contain raw vectors independently, allowing each component to use a packing policy suited to its size.",
    "uncertainty": "The source evaluates representative layouts rather than one canonical file format."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "text": "Low-dimensional vectors and strong coupled-block locality allow one fetch to serve both navigation and exact scoring, or pruning is too weak to offset separate reads.",
      "uncertainty": "Some low-dimensional packing points can still favor decoupling because of block-utilization discontinuities."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS",
  "falsifying_test": {
    "controlled_variables": [
      "graph topology",
      "vector values",
      "dimension",
      "page size",
      "recall target",
      "pruning representation",
      "cache state"
    ],
    "failure_signal": "Decoupling does not improve useful navigation packing or fails to reduce total block reads in the declared high-dimension/pruning regime",
    "fixture": "The same proximity graph encoded in coupled and decoupled layouts at one low and one high vector dimension",
    "independent_oracle": "Exact nearest-neighbor sets plus block-level traces at matched recall and page size",
    "scope": "Smallest layout/pruning falsifier only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Navigation adjacency remains densely packed in index blocks while full vectors occupy separate data blocks, and data blocks are read only for candidates that survive the refinement/pruning decision.",
    "uncertainty": "Effective I/O reduction depends on pruning quality."
  },
  "knight_bus_algorithm_families": [
    "NODESIMILARITY_KNN"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Store adjacency lists and raw vectors in separate index and data blocks; traverse compact index blocks first, then use approximate-distance pruning to avoid data-block reads for candidates that cannot improve the result.",
    "uncertainty": "A weak approximate representation can fail to prune enough vectors and expose the two-read penalty."
  },
  "name": "Decouple Navigation Vector Blocks",
  "pattern_id": "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "When raw vectors are very large, coupling each vector with its adjacency list can reduce a block to one vector, fragment navigation metadata, and waste I/O locality.",
    "uncertainty": "The dimensional point at which fragmentation appears depends on page size, data width, and graph degree."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Approximate candidate scores and pruning decisions are recomputed during traversal; exact distances are computed only after selected raw-vector blocks arrive.",
    "uncertainty": "Pruning accuracy depends on the compressed-vector representation."
  },
  "related_pattern_ids": [
    "PAT-PLACE-SCALE-GROWING-STATE",
    "PAT-PREFETCH-CANDIDATE-BLOCKS-ASYNCHRONOUSLY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "The query retains candidate/result queues, compressed scoring state, and currently fetched navigation/data blocks, while full adjacency and vectors remain disk-resident.",
    "uncertainty": "The paper does not isolate layout-specific peak RAM."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Decoupling can require separate index-block and data-block reads for a traversal step, but compact adjacency packing and pruning can skip most data blocks at high dimension.",
      "measurement_needed": "Measure index/data block reads, useful bytes, prune rate, and QPS at matched recall across dimensions and page sizes.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003",
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "The net effect reverses with dimension, page packing, recall target, and pruning accuracy."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "Adjacency lists are stored in index blocks and raw vectors in separate data blocks; method-specific auxiliary files and pruning state remain additional.",
      "measurement_needed": "Measure index blocks, data blocks, pruning metadata, alignment waste, and filesystem overhead separately.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "The paper does not give a universal storage-amplification expression for decoupling alone."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure layout-build time, data rewrite bytes, and pruning-index construction for matched coupled/decoupled indexes.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The evaluation does not isolate construction overhead caused solely by separating index and vector files."
    },
    "ram": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak RSS and resident navigation/data block buffers for coupled and decoupled layouts at matched search settings.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The source does not provide a layout-only RAM equation separated from caching and search state."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure query in-flight buffers and construction scratch for both layouts.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Peak temporary buffers for concurrent index/data reads and construction are not bounded."
    }
  },
  "source_domain": "disk block layout for high-dimensional graph ANN",
  "source_paper_ids": [
    "PAPER-2603.01779"
  ],
  "source_pointers": [
    {
      "claim_scope": "Global coupled/decoupled layout distinction",
      "locator_type": "FIGURE",
      "locator_value": "Figure 4 and Section 3.2 opening paragraphs",
      "page": 4,
      "paper_id": "PAPER-2603.01779",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Coupled high-dimension fragmentation and decoupled navigation/refinement with pruning",
      "locator_type": "SECTION",
      "locator_value": "Section 3.2.1",
      "page": 5,
      "paper_id": "PAPER-2603.01779",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Dimension- and recall-sensitive coupled/decoupled benchmark",
      "locator_type": "FIGURE",
      "locator_value": "Figure 11 and Section 4.3.2 Global Layout Evaluation",
      "page": 9,
      "paper_id": "PAPER-2603.01779",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Source crossover finding and block-utilization effects",
      "locator_type": "SECTION",
      "locator_value": "Section 4.3.2, Global Layout Findings",
      "page": 10,
      "paper_id": "PAPER-2603.01779",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Dimension-guided layout recommendation",
      "locator_type": "SECTION",
      "locator_value": "Section 5.1 item 2",
      "page": 12,
      "paper_id": "PAPER-2603.01779",
      "pointer_id": "SP-005"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Navigation blocks are streamed for expansion and selected vector blocks are streamed separately for exact refinement.",
    "uncertainty": "A candidate can require two block classes instead of one coupled fetch."
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
        "SP-004",
        "SP-005"
      ],
      "text": "The best crossover is unknown for other page sizes, vector data types, storage devices, graph degrees, compressed-score quality, and non-ANN graph traversals.",
      "uncertainty": "The source guide's dimension values are empirical for its evaluated configuration."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003",
        "SP-004"
      ],
      "text": "Raw vectors are large relative to pages, many adjacency entries can share an index block, and pruning removes enough candidates to avoid most vector-block reads.",
      "uncertainty": "The source observes this regime at high dimension and higher recall, not as a universal threshold."
    }
  ]
}
```
