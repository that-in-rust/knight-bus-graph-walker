# Navigate Binary Rerank Exactly

- Pattern ID: `PAT-NAVIGATE-BINARY-RERANK-EXACTLY`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "The compatibility probe is run on the target embedding model."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source removes float vectors from navigation state.",
      "Exact vectors are accessed only for retained candidates.",
      "The source documents severe incompatible-distribution cases."
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "A007 can quote hot signature-plus-adjacency RAM separately from cold exact-vector storage and bound exact-vector reads by the admitted candidate budget, but only after a geometry compatibility gate.",
    "uncertainty": "Candidate-page locality and whole-process RSS remain fixture-specific."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Quantize the query once, traverse and maintain the beam with bitwise distances, then fetch and exactly score only the retained float candidates.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "The source implementation matches its described data path."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The paper reports both compatible and collapse workloads."
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "The mechanism, ablations, and negative datasets are unusually explicit, but all measured claims remain source-reported.",
    "uncertainty": "No code inspection or reproduction occurred."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Two-bit signatures and adjacency lists form the hot structure; full-precision vectors form a cold array that may be memory-mapped from SSD.",
    "uncertainty": "Cold-path behavior depends on candidate count and storage caching."
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
      "text": "Euclidean-native or structureless distributions can collapse recall because sign-based directionality carries little discriminative neighborhood information.",
      "uncertainty": "Wider search can improve recall but may make the plan inefficient."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-NAVIGATE-BINARY-RERANK-EXACTLY",
  "falsifying_test": {
    "controlled_variables": [
      "Corpus, graph degree, construction beam, search beam, query set, and rerank count."
    ],
    "failure_signal": "Quantized navigation cannot reach the oracle neighborhood on the compatible fixture or accesses float vectors before reranking.",
    "fixture": "One cosine-native contrastive embedding sample and one Euclidean or structureless control, each with exact nearest-neighbor ground truth.",
    "independent_oracle": "Brute-force float similarity ranking.",
    "scope": "Two-stage correctness and access separation; source performance is not independently reproduced."
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-004"
    ],
    "text": "Binary distances need only preserve enough local improving paths to reach a candidate neighborhood; exact float reranking restores final ordering within that candidate set.",
    "uncertainty": "A rigorous real-data navigability guarantee remains open."
  },
  "knight_bus_algorithm_families": [
    {
      "assumptions": [
        "Knight Bus includes a vector-similarity or kNN family."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source mechanism is an ANN graph index and query procedure."
      ],
      "source_pointer_ids": [
        "SP-002"
      ],
      "text": "Node-similarity and kNN graph workloads are the direct Knight Bus family match; other graph-analytics families are not established by this source.",
      "uncertainty": "No evidence supports transfer to BFS, WCC, PageRank, or community detection."
    }
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Encode each vector with sign and magnitude-strength bits, use the resulting symmetric bitwise distance for edge selection and beam navigation, and access original vectors only to rerank final candidates exactly.",
    "uncertainty": "Applicability depends on embedding geometry."
  },
  "name": "Navigate Binary Rerank Exactly",
  "pattern_id": "PAT-NAVIGATE-BINARY-RERANK-EXACTLY",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-004"
    ],
    "text": "Keeping full-precision vectors in the graph-navigation hot path raises memory and cache pressure, while coarse binary distances are not accurate enough for final ranking.",
    "uncertainty": "NONE"
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Exact cosine scores and final candidate order are recomputed from original vectors after approximate navigation.",
    "uncertainty": "NONE"
  },
  "related_pattern_ids": [
    "PAT-EXPLORE-SUPERSET-VERIFY-RESULTS",
    "PAT-FILTER-COMPRESSED-RERANK-EXACTLY",
    "PAT-PROBE-QUANTIZED-TOPOLOGY-COMPATIBILITY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "BQ signatures, graph adjacency, node metadata, visited state, and the active beam remain hot.",
    "uncertainty": "Measured component sizes are fixture-specific."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Navigation does not read original vectors; cold reads are limited to the final candidate vectors used for exact reranking.",
      "measurement_needed": "Measure cold-cache bytes and random reads for reranking candidates.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "The claim is bounded to the described layout and candidate beam."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "Persistent state includes original float vectors, two-bit signatures, adjacency lists, and node metadata.",
      "measurement_needed": "Measure bytes for each persisted component including alignment and indexes.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "Durable file-format overhead is not isolated."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Compute per-vector sign and magnitude-strength bits and construct the Vamana topology in the quantized metric; no learned codebook or rotation is required.",
      "measurement_needed": "Measure encoding and graph-construction time and peak build RSS.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "Graph-build time remains workload- and hardware-dependent."
    },
    "ram": {
      "assumptions": [],
      "expression": "Hot RAM is two-bit signatures plus adjacency, metadata, visited state, and beam state; original float vectors are excluded from the navigation hot path.",
      "measurement_needed": "Measure peak RSS by hot components and page-cache residency for the declared corpus.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "Whole-process RSS beyond the reported component table is not generalized."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure build scratch, private visited sets, beam state, and rerank buffers.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Peak temporary memory during concurrent graph construction and exact reranking is not bounded as a single term."
    }
  },
  "source_domain": "quantized graph-based approximate nearest-neighbor search",
  "source_paper_ids": [
    "PAPER-2605.02171"
  ],
  "source_pointers": [
    {
      "claim_scope": "Per-vector two-bit sign-magnitude encoding and bitwise symmetric distance.",
      "locator_type": "SECTION",
      "locator_value": "Section 3.1 and Table 1",
      "page": 3,
      "paper_id": "PAPER-2605.02171",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "BQ-native Vamana construction, BQ beam navigation, exact float reranking, and hot/cold state separation.",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 1 and Sections 3.2-4.2",
      "page": 4,
      "paper_id": "PAPER-2605.02171",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Encoding ablation and cross-dataset applicability boundary.",
      "locator_type": "TABLE",
      "locator_value": "Tables 10-11 and Section 5.6",
      "page": 8,
      "paper_id": "PAPER-2605.02171",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Local navigation versus global ranking, two-stage division of labor, and geometry-dependent failure boundary.",
      "locator_type": "SECTION",
      "locator_value": "Section 6, Analysis",
      "page": 10,
      "paper_id": "PAPER-2605.02171",
      "pointer_id": "SP-004"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Full-precision vectors are fetched only for final candidate reranking and can reside in a memory-mapped cold tier.",
    "uncertainty": "The paper benchmarks one NVMe-backed cold-path setup."
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
        "SP-003",
        "SP-004"
      ],
      "text": "A rigorous navigability guarantee under realistic data distributions remains open, and multimodal mixtures occupy an intermediate regime.",
      "uncertainty": "Distribution shift may change compatibility."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-004"
      ],
      "text": "Cosine-native contrastive embeddings have angular geometry whose signs and magnitude-strength bits preserve useful local navigation signal.",
      "uncertainty": "Compatibility is empirical rather than universally guaranteed."
    }
  ]
}
```
