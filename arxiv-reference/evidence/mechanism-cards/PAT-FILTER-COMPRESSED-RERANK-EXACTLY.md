# Filter Compressed Rerank Exactly

- Pattern ID: `PAT-FILTER-COMPRESSED-RERANK-EXACTLY`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "The Knight Bus workload permits approximate candidate filtering before exact result verification.",
      "Compressed guidance codes fit within the declared resident-memory budget."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source keeps compressed vectors in memory for candidate ranking.",
      "The source retains full-precision vectors on storage for precise refinement."
    ],
    "source_pointer_ids": [
      "SP-025",
      "SP-026",
      "SP-028"
    ],
    "text": "For approximate candidate-search workloads, Knight Bus could separate a bounded resident guidance representation from streamed full-precision verification state so initial ranking does not fetch every full record.",
    "uncertainty": "The paper studies approximate nearest-neighbor search and does not establish correctness for exact BFS, WCC, PageRank, or other graph algorithms."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-026",
      "SP-028"
    ],
    "text": "Rank candidates with memory-resident PQ coordinates, expand the candidate set to offset quantization loss, and fetch full-precision vectors from storage only for precise final reranking.",
    "uncertainty": "The paper does not provide a dataset-independent candidate-expansion rule."
  },
  "confidence_rationale": {
    "assumptions": [
      "The paper's reported baseline and memory-budget study exercise the described PQ path.",
      "Absence of campaign reproduction limits confidence to source-reported behavior."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Section 4.1.1 specifies resident PQ filtering and disk refinement.",
      "Section 6 identifies PQ as the evaluated baseline and Figure 15 varies PQ memory."
    ],
    "source_pointer_ids": [
      "SP-026",
      "SP-029",
      "SP-030"
    ],
    "text": "The mechanism has paper-level analytical and benchmark support, but this campaign neither reproduced the results nor inspected an implementation.",
    "uncertainty": "Confidence is bounded to the paper's DiskANN topology, datasets, hardware, recall metric, and PQ configurations."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-025",
      "SP-026",
      "SP-028"
    ],
    "text": "Compressed PQ coordinates for the dataset reside in memory, while page-aligned disk records retain full-precision vectors and neighbor information for refinement.",
    "uncertainty": "Code width, codebook representation, and exact record packing depend on the configured index."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-028",
        "SP-030"
      ],
      "text": "Quantization loses precision, so insufficient candidate expansion or insufficient PQ coordinate memory can reduce search accuracy before exact reranking.",
      "uncertainty": "The source does not state one failure threshold that transfers across datasets and recall targets."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-FILTER-COMPRESSED-RERANK-EXACTLY",
  "falsifying_test": {
    "controlled_variables": [
      "graph index",
      "query set",
      "recall target",
      "candidate-list size",
      "beam width",
      "cache state",
      "concurrency"
    ],
    "failure_signal": "The PQ-guided run performs no fewer full-vector page reads than the full-precision-ranking oracle at matched recall, or exact reranking cannot recover the oracle top-k from the filtered candidate set",
    "fixture": "A small page-aligned proximity graph with full vectors, deterministic PQ codes, and queries whose exact top-k set is known",
    "independent_oracle": "Full-precision distance ranking over every candidate followed by the same graph-expansion and top-k rules",
    "scope": "Smallest mechanism-level falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-026",
      "SP-028"
    ],
    "text": "Approximate in-memory distances guide candidate selection, while full-precision disk vectors remain the refinement authority for final top-k results.",
    "uncertainty": "The final result remains approximate because filtering can exclude a true neighbor before refinement."
  },
  "knight_bus_algorithm_families": [
    "APPROXIMATE_NEAREST_NEIGHBOR",
    "BEST_FIRST_GRAPH_SEARCH",
    "BOUNDED_PATH_SEARCH"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-026",
      "SP-027",
      "SP-028"
    ],
    "text": "Compress vectors into memory-resident Product Quantization coordinates, use those codes for approximate distance filtering, and rerank only promising candidates with full-precision vectors read from disk.",
    "uncertainty": "The paper selects classic PQ as representative and does not compare all quantization families."
  },
  "name": "Filter Compressed Rerank Exactly",
  "pattern_id": "PAT-FILTER-COMPRESSED-RERANK-EXACTLY",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-025",
      "SP-026"
    ],
    "text": "Reading full-precision vectors for initial candidate-distance ranking consumes disk I/O and makes a large vector collection difficult to search within a practical RAM budget.",
    "uncertainty": "The analysis assumes the paper's disk-resident Vamana-based search model."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-026",
      "SP-028"
    ],
    "text": "Queries compute approximate distances from PQ coordinates during filtering and precise distances from fetched full vectors during reranking.",
    "uncertainty": "The paper does not isolate the compute cost of either distance stage."
  },
  "related_pattern_ids": [
    "PAT-NAVIGATE-BINARY-RERANK-EXACTLY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-025",
      "SP-026",
      "SP-030"
    ],
    "text": "PQ coordinates for all indexed vectors and their quantization support state remain memory resident during query processing.",
    "uncertainty": "The paper reports configured memory budgets but not a portable whole-process RAM equation."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "The paper models optimized page reads as O(H divided by (OR(G) times n_p)) because memory-resident PQ distances remove the average-degree factor associated with reading full vectors during initial ranking.",
      "measurement_needed": "Measure full-vector and topology page reads per query separately at matched recall.",
      "premises": [],
      "source_pointer_ids": [
        "SP-026",
        "SP-027"
      ],
      "status": "SOURCED",
      "uncertainty": "The model excludes cache and other optimizations and does not guarantee ideal page locality."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "Page-aligned disk records retain full-precision vectors and neighbor information used for expansion and exact reranking.",
      "measurement_needed": "Measure full-vector, topology, alignment-padding, and any serialized PQ-code bytes independently.",
      "premises": [],
      "source_pointer_ids": [
        "SP-025",
        "SP-028"
      ],
      "status": "SOURCED",
      "uncertainty": "The card does not infer a storage coefficient absent from the source."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure PQ training, code generation, and serialization time plus peak build RSS for the named fixture.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not isolate PQ training and encoding as a preprocessing term."
    },
    "ram": {
      "assumptions": [],
      "expression": "RAM retains compressed coordinates for every indexed vector; increasing PQ coordinate memory improves recall but the compressed representation can still be substantial at scale.",
      "measurement_needed": "Measure retained PQ-code and codebook bytes separately from cache, graph, and query state.",
      "premises": [],
      "source_pointer_ids": [
        "SP-026",
        "SP-030"
      ],
      "status": "SOURCED",
      "uncertainty": "Compression ratio and accuracy depend on vector dimension and PQ configuration."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "Query-local temporary state includes the expanded approximate candidate set and the subset retained for full-precision reranking.",
      "measurement_needed": "Measure peak candidate-list and reranking-buffer bytes as the expansion setting varies.",
      "premises": [],
      "source_pointer_ids": [
        "SP-028"
      ],
      "status": "SOURCED",
      "uncertainty": "The source describes expansion but gives no closed temporary-space bound."
    }
  },
  "source_domain": "disk-resident graph approximate-nearest-neighbor search",
  "source_paper_ids": [
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "Three-layer placement of full vectors, PQ guidance, and cache",
      "locator_type": "SECTION",
      "locator_value": "Section 2.2, DiskANN overview and architecture",
      "page": 3,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-025"
    },
    {
      "claim_scope": "Two-stage PQ filtering, memory placement, precision trade-off, and Figure 3 example",
      "locator_type": "SECTION",
      "locator_value": "Section 4.1.1, Product Quantization (PQ)",
      "page": 4,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-026"
    },
    {
      "claim_scope": "PQ-adjusted page-read complexity",
      "locator_type": "EQUATION",
      "locator_value": "Equation 2",
      "page": 4,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-027"
    },
    {
      "claim_scope": "Candidate expansion, exact reranking, and DiskANN record colocation",
      "locator_type": "SECTION",
      "locator_value": "Section 4.1.1 continuation",
      "page": 5,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-028"
    },
    {
      "claim_scope": "PQ baseline and individual-optimization evaluation design",
      "locator_type": "SECTION",
      "locator_value": "Section 6 and Section 6.1 opening paragraphs",
      "page": 8,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-029"
    },
    {
      "claim_scope": "PQ memory allocation effect on recall and throughput",
      "locator_type": "FIGURE",
      "locator_value": "Figure 15 and Section 6.3, Memory Budget Analysis",
      "page": 10,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-030"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-025",
      "SP-026",
      "SP-028"
    ],
    "text": "Full-precision candidate vectors and neighbor records are fetched from page-aligned SSD records only when graph expansion or precise reranking requires them.",
    "uncertainty": "The query-dependent read set varies with graph path, beam settings, and recall target."
  },
  "unknown_when": [
    {
      "assumptions": [
        "The reviewed sections are the paper's complete treatment of PQ preprocessing.",
        "No unreported implementation detail supplies a portable construction bound."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source describes PQ query placement and evaluates memory allocation.",
        "The source does not isolate codebook training or encoding costs."
      ],
      "source_pointer_ids": [
        "SP-026",
        "SP-029",
        "SP-030"
      ],
      "text": "Construction cost and the smallest PQ budget that preserves a requested recall remain unknown from this paper.",
      "uncertainty": "Those boundaries may depend on implementation, codebook training, vector distribution, and target recall."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-026",
        "SP-028",
        "SP-030"
      ],
      "text": "The mechanism applies when memory can retain compressed coordinates and exact disk reranking can verify an expanded shortlist of promising candidates.",
      "uncertainty": "Memory allocation and shortlist expansion must be tuned to dataset and recall target."
    }
  ]
}
```
