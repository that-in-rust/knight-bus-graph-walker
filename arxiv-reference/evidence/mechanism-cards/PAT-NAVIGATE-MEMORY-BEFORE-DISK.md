# Navigate Memory Before Disk

- Pattern ID: `PAT-NAVIGATE-MEMORY-BEFORE-DISK`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "The Knight Bus algorithm has a query-local navigation phase that can use sampled entry points.",
      "Sampling does not violate the algorithm's required correctness semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports that better entry points shorten disk traversal paths.",
      "The source separates resident sampled topology from disk-resident full records."
    ],
    "source_pointer_ids": [
      "SP-008",
      "SP-010",
      "SP-011"
    ],
    "text": "Knight Bus could treat a bounded resident navigation sample as separately admissible state whose purpose is to reduce later streamed topology reads, rather than requiring the complete graph to be resident.",
    "uncertainty": "The source is ANN-specific and does not prove that sampling preserves exact BFS, WCC, or PageRank semantics."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-008"
    ],
    "text": "Each query performs coarse navigation in memory before issuing disk reads from the resulting high-quality entry points.",
    "uncertainty": "The paper does not define a hard upper bound on disk hops."
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
      "SP-009",
      "SP-010",
      "SP-011"
    ],
    "text": "The paper implements the sampled navigator in a common DiskANN baseline and reports ablations on four public datasets, but the campaign did not reproduce or inspect the code and the study excludes billion scale.",
    "uncertainty": "Grade C is limited to the reported hardware, datasets, recall targets, and implementation."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-007",
      "SP-008",
      "SP-011"
    ],
    "text": "A sampled topology-only navigation graph is resident in memory, while the full graph records and vectors remain in page-aligned SSD storage.",
    "uncertainty": "The sampled graph's bytes vary with sampling ratio and dataset."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-008",
        "SP-011"
      ],
      "text": "Insufficient sampling fails to cover useful entry regions, while excessive sampling increases memory and in-memory search time with diminishing returns.",
      "uncertainty": "The source does not give a dataset-independent crossover point."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-NAVIGATE-MEMORY-BEFORE-DISK",
  "falsifying_test": {
    "controlled_variables": [
      "graph index",
      "query set",
      "recall target",
      "beam width",
      "cache state",
      "concurrency"
    ],
    "failure_signal": "The sampled navigator does not reduce disk pages at matched recall, or its resident and search overhead exceeds the saved disk work",
    "fixture": "A disk-resident proximity graph with one query family clustered away from the baseline entry point and one adversarial family outside the navigation sample",
    "independent_oracle": "The same disk search initialized from an exhaustive best entry point chosen from all vertices",
    "scope": "Smallest source-mechanism falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-007",
      "SP-008"
    ],
    "text": "The memory stage supplies promising entry vertices, while exact full vectors and graph records remain available on disk for final navigation and refinement.",
    "uncertainty": "Search remains approximate and is evaluated at matched recall."
  },
  "knight_bus_algorithm_families": [
    "BEST_FIRST_GRAPH_SEARCH",
    "APPROXIMATE_NEAREST_NEIGHBOR",
    "BOUNDED_PATH_SEARCH"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-008",
      "SP-010"
    ],
    "text": "Sample vertices into a small memory-resident navigation graph, search that graph first, and use the discovered region as entry points for precise disk-based traversal.",
    "uncertainty": "The evaluation uses random sampling; other sampling strategies are discussed but not evaluated here."
  },
  "name": "Navigate Memory Before Disk",
  "pattern_id": "PAT-NAVIGATE-MEMORY-BEFORE-DISK",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-007",
      "SP-008"
    ],
    "text": "Disk traversal from a fixed or poor entry point takes a longer convergence path and incurs more independent page reads.",
    "uncertainty": "The source studies vector proximity graphs under DiskANN's logical topology."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-007"
    ],
    "text": "Approximate in-memory distances guide the search, and exact distances are evaluated from full disk-resident vectors for refinement.",
    "uncertainty": "Distance-computation cost is not the dominant term in the evaluated I/O-bound regime."
  },
  "related_pattern_ids": [
    "PAT-COLOCATE-NEIGHBORS-WITHIN-PAGES",
    "PAT-GROW-BEAM-WIDTH-PROGRESSIVELY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-007",
      "SP-011"
    ],
    "text": "Compressed navigation coordinates and the sampled navigation topology occupy memory during search.",
    "uncertainty": "Whole-process RSS includes additional queues, caches, and runtime state not isolated by the card."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Memory navigation reduces disk traversal hops by improving entry-point quality; full vectors and graph records are still fetched for disk refinement.",
      "measurement_needed": "Measure pages read per query at matched recall and fixed concurrency.",
      "premises": [],
      "source_pointer_ids": [
        "SP-008",
        "SP-010"
      ],
      "status": "SOURCED",
      "uncertainty": "I/O reduction is workload- and recall-dependent."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "Full-precision vectors and graph neighbor records remain in the disk-resident index.",
      "measurement_needed": "Measure index bytes and any serialized navigation-graph bytes separately.",
      "premises": [],
      "source_pointer_ids": [
        "SP-007"
      ],
      "status": "SOURCED",
      "uncertainty": "The incremental persistent bytes attributable only to navigation are not isolated."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Sample vertices and construct an in-memory navigation graph before query service.",
      "measurement_needed": "Measure navigation-graph build time and peak build RSS for the declared sample ratio.",
      "premises": [],
      "source_pointer_ids": [
        "SP-008",
        "SP-009"
      ],
      "status": "SOURCED",
      "uncertainty": "Construction time is configuration-dependent and not symbolically bounded."
    },
    "ram": {
      "assumptions": [],
      "expression": "Memory-resident compressed coordinates for the dataset plus a sampled topology-only navigation graph; higher sampling consumes more memory and eventually has diminishing throughput returns.",
      "measurement_needed": "Measure retained RSS by separating compressed coordinates, navigation topology, cache, and query state.",
      "premises": [],
      "source_pointer_ids": [
        "SP-007",
        "SP-008",
        "SP-011"
      ],
      "status": "SOURCED",
      "uncertainty": "The paper reports selected configurations, not a universal byte formula."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak temporary allocations during build and query execution.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Temporary query and build storage is not reported as a separate term."
    }
  },
  "source_domain": "disk-resident graph approximate-nearest-neighbor search",
  "source_paper_ids": [
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "Memory guidance and disk refinement arrangement",
      "locator_type": "SECTION",
      "locator_value": "Section 2.2, DiskANN overview and architecture",
      "page": 3,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-007"
    },
    {
      "claim_scope": "Sampled navigation graph, entry points, and sampling trade-off",
      "locator_type": "SECTION",
      "locator_value": "Section 4.1.3, Hierarchical Graphs (MemGraph)",
      "page": 5,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-008"
    },
    {
      "claim_scope": "Evaluation scale, memory budget, and deferred billion-scale study",
      "locator_type": "SECTION",
      "locator_value": "Section 5.1, scope of evaluation",
      "page": 7,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-009"
    },
    {
      "claim_scope": "Measured path shortening and standalone gains",
      "locator_type": "PARAGRAPH",
      "locator_value": "Finding 3 and preceding MemGraph paragraph",
      "page": 8,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-010"
    },
    {
      "claim_scope": "Allocation trade-off between PQ coordinates and sampled navigation",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 6.3 and Finding 7",
      "page": 10,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-011"
    },
    {
      "claim_scope": "Complementarity with page layout and adaptive width",
      "locator_type": "PARAGRAPH",
      "locator_value": "Finding 10 and full-combination discussion",
      "page": 11,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-012"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-007",
      "SP-008"
    ],
    "text": "Disk-resident full vectors and neighbor records are fetched for candidates selected after memory navigation.",
    "uncertainty": "The exact read set depends on query, recall target, and beam parameters."
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
      "text": "The study defers billion-scale evaluation and does not establish behavior when navigation construction itself exceeds the available build memory.",
      "uncertainty": "The reported 100M-scale I/O regime may not preserve all trade-offs at larger scale."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-008",
        "SP-010",
        "SP-011"
      ],
      "text": "A modest memory budget can hold a representative navigation sample whose entry points are closer to query targets than the fixed baseline entry point.",
      "uncertainty": "Benefit varies by graph quality and recall target."
    }
  ]
}
```
