# Colocate Neighbors Within Pages

- Pattern ID: `PAT-COLOCATE-NEIGHBORS-WITHIN-PAGES`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus uses fixed-size pages or blocks with more than one graph record.",
      "Its traversal can consume useful co-located records without changing correctness."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source links page reads to page utility.",
      "The source reports substantial build memory and time for page shuffle.",
      "The source finds page shuffle weak alone and complementary with full-page search."
    ],
    "source_pointer_ids": [
      "SP-013",
      "SP-015",
      "SP-016"
    ],
    "text": "Knight Bus could admit a page-reordered artifact only when the quote includes both online page utility and the offline reverse-graph, mapping, and shuffle peak rather than treating layout as free.",
    "uncertainty": "The ANN page-read model may not predict BFS, WCC, or PageRank access paths."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-013",
      "SP-014"
    ],
    "text": "Best-first traversal fetches pages by candidate expansion, and co-location raises the expected useful neighbors supplied by each fetch.",
    "uncertainty": "The source model excludes several other optimizations and uses an expected overlap ratio."
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
      "SP-013",
      "SP-015",
      "SP-016"
    ],
    "text": "The paper supplies a page-read model, a common implementation, construction-resource measurements, and ablations showing both weak standalone behavior and synergy; this campaign did not reproduce the results or inspect code.",
    "uncertainty": "Grade C is bounded to the source's SSD, datasets, layout algorithm, and accuracy conditions."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-014",
      "SP-015"
    ],
    "text": "Page-aligned records are permuted so graph-neighbor records share physical pages, with an ID-to-page mapping retained for lookup.",
    "uncertainty": "Page occupancy depends on record dimension and page size."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-015",
        "SP-016"
      ],
      "text": "The mechanism provides little locality benefit when each page holds only one record, and construction can exceed available memory because it retains the graph and reverse graph.",
      "uncertainty": "The one-record page boundary is discussed later for high-dimensional data; exact memory failure thresholds vary."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-COLOCATE-NEIGHBORS-WITHIN-PAGES",
  "falsifying_test": {
    "controlled_variables": [
      "logical graph",
      "record format",
      "query set",
      "cache state",
      "search parameters"
    ],
    "failure_signal": "The shuffled artifact changes graph answers, fails to improve useful records per page, or its preparation peak violates the declared build budget",
    "fixture": "The same graph encoded once in vertex-ID order and once with neighbor co-location, tested with one-record and multi-record page sizes",
    "independent_oracle": "Logical-edge equality plus identical search answers at the same accuracy target",
    "scope": "Smallest layout and resource falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-014"
    ],
    "text": "Logical graph topology is preserved while physical record order is changed to place more neighbors on the same storage page.",
    "uncertainty": "The optimization objective is heuristic because optimal placement is computationally hard."
  },
  "knight_bus_algorithm_families": [
    "BEST_FIRST_GRAPH_SEARCH",
    "BOUNDED_PATH_SEARCH",
    "APPROXIMATE_NEAREST_NEIGHBOR"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-013",
      "SP-014"
    ],
    "text": "Offline page shuffle reorders vertex records to increase page-level neighbor overlap, allowing one fetched page to serve multiple candidate expansions.",
    "uncertainty": "Higher one-hop overlap does not by itself guarantee a shorter multi-hop search path."
  },
  "name": "Colocate Neighbors Within Pages",
  "pattern_id": "PAT-COLOCATE-NEIGHBORS-WITHIN-PAGES",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-013",
      "SP-014"
    ],
    "text": "Vertex-ID order scatters graph neighbors across pages, so a page fetch often supplies few vertices useful to the traversal.",
    "uncertainty": "Usefulness is defined for proximity-graph navigation."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-014",
      "SP-015"
    ],
    "text": "The logical graph is not recomputed at query time; the source changes physical placement offline and translates identifiers through the retained mapping.",
    "uncertainty": "Update and reshuffle behavior is outside the evaluated scope."
  },
  "related_pattern_ids": [
    "PAT-NAVIGATE-MEMORY-BEFORE-DISK",
    "PAT-SEARCH-EVERY-FETCHED-RECORD"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-015"
    ],
    "text": "The online index retains an in-memory mapping between record identifiers and shuffled page identifiers.",
    "uncertainty": "Mapping bytes are reported only through aggregate auxiliary-memory measurements."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Expected page reads decrease as page-level neighbor overlap and records per page increase, but page shuffle alone produced only limited I/O reduction in the reported ablation.",
      "measurement_needed": "Measure unique pages, useful records per page, and read amplification at matched accuracy.",
      "premises": [],
      "source_pointer_ids": [
        "SP-013",
        "SP-015"
      ],
      "status": "SOURCED",
      "uncertainty": "The analytical model is simplified and the measured effect is dataset-dependent."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "The evaluated shuffled disk index retains the baseline graph records in reordered pages; Table 6 reports unchanged aggregate disk index size for the tested combinations.",
      "measurement_needed": "Measure final index bytes, page slack, and serialized mapping bytes.",
      "premises": [],
      "source_pointer_ids": [
        "SP-016"
      ],
      "status": "SOURCED",
      "uncertainty": "This does not establish unchanged storage for every record size, page size, or implementation."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "An iterative offline shuffle solves a hard layout objective and adds substantial build time in the evaluated system.",
      "measurement_needed": "Record shuffle wall time, passes, and peak memory for the declared graph.",
      "premises": [],
      "source_pointer_ids": [
        "SP-015",
        "SP-016"
      ],
      "status": "SOURCED",
      "uncertainty": "No general asymptotic or wall-time bound is established."
    },
    "ram": {
      "assumptions": [],
      "expression": "Offline shuffle loads the full graph and reverse graph, while online search retains an ID-to-page mapping.",
      "measurement_needed": "Measure peak build RSS and retained mapping RSS separately.",
      "premises": [],
      "source_pointer_ids": [
        "SP-015",
        "SP-016"
      ],
      "status": "SOURCED",
      "uncertainty": "Peak bytes depend on graph scale and record representation."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure scratch files and temporary allocation bytes during every shuffle pass.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper reports peak build memory but not a separate temporary-storage term."
    }
  },
  "source_domain": "page-oriented disk-resident graph indexing",
  "source_paper_ids": [
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "Page reads as a function of path length, degree, page occupancy, and overlap",
      "locator_type": "EQUATION",
      "locator_value": "Equation 1 and intuition paragraph",
      "page": 4,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-013"
    },
    {
      "claim_scope": "Neighbor co-location and overlap-ratio mechanism",
      "locator_type": "SECTION",
      "locator_value": "Section 4.2.1, Page Shuffle",
      "page": 5,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-014"
    },
    {
      "claim_scope": "Weak standalone benefit and construction resource costs",
      "locator_type": "PARAGRAPH",
      "locator_value": "Finding 4 and Finding 6",
      "page": 9,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-015"
    },
    {
      "claim_scope": "Complementarity with page search and measured build overhead",
      "locator_type": "PARAGRAPH",
      "locator_value": "Finding 8 and Table 6",
      "page": 10,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-016"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-014"
    ],
    "text": "Reordered disk pages containing full vertex records and neighbor data are fetched during graph traversal.",
    "uncertainty": "Cold-cache and warm-cache page traffic are not both reported."
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
        "SP-015"
      ],
      "text": "The paper does not evaluate online updates, out-of-memory shuffling, or billion-scale construction.",
      "uncertainty": "Static 100M-scale results do not establish maintenance cost for changing graphs."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-014",
        "SP-016"
      ],
      "text": "Pages can hold multiple related records and the traversal is able to exploit co-located candidates, especially when page search evaluates the full fetched page.",
      "uncertainty": "Standalone page shuffle is weaker than the paired mechanism."
    }
  ]
}
```
