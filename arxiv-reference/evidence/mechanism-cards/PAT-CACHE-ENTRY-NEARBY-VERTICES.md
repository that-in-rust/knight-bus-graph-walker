# Cache Entry Nearby Vertices

- Pattern ID: `PAT-CACHE-ENTRY-NEARBY-VERTICES`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus exposes a stable entry region for repeated graph traversals.",
      "Cached records can be admitted within a declared resident-memory cap without changing algorithm semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source observes higher access probability near graph-search entry points.",
      "The source preloads a fixed-hop SSSP neighborhood and reports reduced early-hop I/O when access is concentrated there."
    ],
    "source_pointer_ids": [
      "SP-031",
      "SP-033",
      "SP-034"
    ],
    "text": "Knight Bus could reserve a bounded resident tier for records near stable traversal entry points, while requiring a miss path to the authoritative streamed graph for accesses outside that tier.",
    "uncertainty": "The paper does not show that entry-local access concentration holds for Knight Bus BFS, WCC, PageRank, or adversarial source distributions."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-031"
    ],
    "text": "Before query service, run SSSP from the search entry point and preload records within a configured hop radius; queries consume those resident records and read storage on misses.",
    "uncertainty": "The source does not specify a universal radius-selection rule."
  },
  "confidence_rationale": {
    "assumptions": [
      "The reported Cache curves correspond to the SSSP policy described in Section 4.1.2.",
      "No campaign reproduction or code inspection is available."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source specifies SSSP, frequency-based, and LRU alternatives and evaluates SSSP.",
      "The source reports standalone behavior on four datasets and identifies graph-quality sensitivity."
    ],
    "source_pointer_ids": [
      "SP-031",
      "SP-032",
      "SP-033",
      "SP-034"
    ],
    "text": "The SSSP cache has source-level mechanism and benchmark support, but confidence remains limited to the paper's reported implementation and test conditions.",
    "uncertainty": "The campaign did not reproduce cache-hit rates, page reads, or throughput."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-031",
      "SP-032"
    ],
    "text": "Records within a fixed SSSP hop neighborhood of the entry point are copied into a memory-resident cache while the complete page-aligned graph index remains on SSD.",
    "uncertainty": "The exact cached record contents and replacement representation are not isolated."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-031",
        "SP-034"
      ],
      "text": "A static entry-centered cache adapts poorly when query patterns or access distributions vary and is less useful when early-hop paths are not concentrated near the selected entry point.",
      "uncertainty": "The source does not quantify the distribution shift at which another cache policy becomes preferable."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-CACHE-ENTRY-NEARBY-VERTICES",
  "falsifying_test": {
    "controlled_variables": [
      "graph index",
      "entry point",
      "cache capacity",
      "query count",
      "beam width",
      "recall target",
      "concurrency"
    ],
    "failure_signal": "The fixed-hop cache exceeds its capacity, changes search results, or fails to reduce storage reads relative to an empty cache for entry-concentrated queries",
    "fixture": "A small disk graph with one query set whose traversals share an entry-near prefix and a second set whose paths avoid that prefix",
    "independent_oracle": "The same deterministic search over the complete disk index with cache disabled and page reads traced",
    "scope": "Smallest mechanism-level falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-031"
    ],
    "text": "Cache membership is determined by graph-hop distance from the designated entry point, and a miss remains serviceable from the complete disk-resident index.",
    "uncertainty": "The source calls the policy static and does not describe dynamic invalidation."
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
      "SP-031",
      "SP-033"
    ],
    "text": "Compute a Single-Source Shortest Path neighborhood around the graph-search entry point and retain records within a configured hop distance so common early traversal accesses avoid SSD reads.",
    "uncertainty": "The paper evaluates SSSP caching rather than its frequency-based and LRU alternatives."
  },
  "name": "Cache Entry Nearby Vertices",
  "pattern_id": "PAT-CACHE-ENTRY-NEARBY-VERTICES",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-031"
    ],
    "text": "Unpredictable best-first traversal causes cache misses and repeated disk reads, even though vertices near a common search entry point can have higher access probability.",
    "uncertainty": "The observed locality depends on graph quality and query distribution."
  },
  "recomputed_state": {
    "assumptions": [
      "The paper's static designation means cache membership is not recomputed per query.",
      "Ordinary query candidate scoring remains outside the cache-selection mechanism."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source preloads a fixed-hop SSSP neighborhood.",
      "The source contrasts that policy with dynamically tracked frequency caching."
    ],
    "source_pointer_ids": [
      "SP-031"
    ],
    "text": "The SSSP cache policy introduces no per-query cache-membership recomputation; membership changes only if the entry point, hop radius, or preload is rebuilt.",
    "uncertainty": "The implementation's refresh lifecycle is not described."
  },
  "related_pattern_ids": [],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-031",
      "SP-032"
    ],
    "text": "The configured entry-near record subset remains in memory alongside the baseline PQ state during query service.",
    "uncertainty": "Whole-process RAM includes other index and runtime structures not attributable to this cache."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Cache hits on entry-near records avoid their SSD reads; the paper reports that the policy accelerates early-hop I/O and that benefit depends on graph quality and path concentration.",
      "measurement_needed": "Measure cache hits, misses, and physical page reads per query at matched recall.",
      "premises": [],
      "source_pointer_ids": [
        "SP-031",
        "SP-034"
      ],
      "status": "SOURCED",
      "uncertainty": "The paper does not provide a portable miss-rate formula."
    },
    "persistent_storage": {
      "assumptions": [
        "Cached records are copies or resident views of records already present in the disk index.",
        "The cache is rebuilt rather than persisted as a separate durable index."
      ],
      "expression": "The authoritative full graph index remains on SSD; the mechanism need not add a distinct persistent structure if the entry-near cache is rebuilt at service start.",
      "measurement_needed": "Inspect deployment artifacts and measure any persisted cache image separately.",
      "premises": [
        "The source describes preloading records from the graph around the entry point.",
        "The source does not identify a separate durable cache artifact."
      ],
      "source_pointer_ids": [
        "SP-031"
      ],
      "status": "DERIVED",
      "uncertainty": "An implementation may choose to serialize the cache even though the paper does not describe doing so."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Run SSSP from the designated search entry point, select vertices within the configured hop radius, and preload their records before queries.",
      "measurement_needed": "Measure SSSP time, preload bytes, and peak RSS for each radius and cache fraction.",
      "premises": [],
      "source_pointer_ids": [
        "SP-031",
        "SP-032"
      ],
      "status": "SOURCED",
      "uncertainty": "The paper does not report the preload duration as a separate benchmark."
    },
    "ram": {
      "assumptions": [],
      "expression": "RAM retains the configured entry-near cache; the evaluated setup caches a fixed fraction of records in addition to baseline resident index state.",
      "measurement_needed": "Measure retained cache bytes by record component and allocator overhead.",
      "premises": [],
      "source_pointer_ids": [
        "SP-031",
        "SP-032"
      ],
      "status": "SOURCED",
      "uncertainty": "The source gives a configured fraction but no universal bytes-per-cached-record formula."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak SSSP frontier, distance-state, and preload-buffer bytes while constructing the cache.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not isolate temporary storage used by SSSP cache construction."
    }
  },
  "source_domain": "disk-resident graph approximate-nearest-neighbor search",
  "source_paper_ids": [
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "SSSP cache selection, alternatives, and static-policy limitation",
      "locator_type": "SECTION",
      "locator_value": "Section 4.1.2, Cache Management (Cache)",
      "page": 5,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-031"
    },
    {
      "claim_scope": "Evaluated SSSP cache fraction and query-processing parameters",
      "locator_type": "SECTION",
      "locator_value": "Section 5.1, Implementation and parameters",
      "page": 7,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-032"
    },
    {
      "claim_scope": "Cache as a standalone PQ-baseline ablation",
      "locator_type": "SECTION",
      "locator_value": "Section 6.1, individual optimization evaluation",
      "page": 8,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-033"
    },
    {
      "claim_scope": "Early-hop I/O effect and graph-quality sensitivity",
      "locator_type": "PARAGRAPH",
      "locator_value": "Finding 3 and preceding Cache paragraph",
      "page": 8,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-034"
    },
    {
      "claim_scope": "Exclusion from combination study because of static entry-point dependency",
      "locator_type": "SECTION",
      "locator_value": "Section 7.1.1, Combination Design",
      "page": 10,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-035"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-031"
    ],
    "text": "Graph records outside the entry-near cache continue to be fetched from the complete SSD index when traversal misses the resident subset.",
    "uncertainty": "The miss sequence depends on query distribution and graph traversal."
  },
  "unknown_when": [
    {
      "assumptions": [
        "Only the SSSP policy receives experimental treatment in the reviewed paper.",
        "The cited alternatives require their own implementation and tuning."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source lists frequency-based and LRU cache policies.",
        "The source explicitly focuses its evaluation on SSSP caching."
      ],
      "source_pointer_ids": [
        "SP-031",
        "SP-033"
      ],
      "text": "The relative crossover among SSSP, frequency-based, and LRU caching remains unknown from this evaluation.",
      "uncertainty": "A different query distribution or refresh policy could reverse the reported preference."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-031",
        "SP-034"
      ],
      "text": "The cache is useful when repeated searches share an entry region and graph structure concentrates early-hop accesses within the preloaded SSSP neighborhood.",
      "uncertainty": "The benefit is dataset- and graph-quality-dependent."
    }
  ]
}
```
