# Track Visited With Bitmaps

- Pattern ID: `PAT-TRACK-VISITED-WITH-BITMAPS`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus executes exact BFS with an in-memory or memory-mapped visited-state structure."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-TRACK-VISITED-WITH-BITMAPS:ram",
      "PAT-TRACK-VISITED-WITH-BITMAPS:works_when",
      "PAT-TRACK-VISITED-WITH-BITMAPS:fails_when"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-003",
      "SP-005"
    ],
    "text": "An A007 feasibility estimate should account for a full visited bitmap separately from the distance array and treat its locality benefit as workload- and machine-dependent rather than guaranteed.",
    "uncertainty": "The source provides no disk-backed bitmap behavior, cache-capacity breakpoint, or concurrency-cost model."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "During neighbor examination, test the destination visited bit; when it is clear, write the distance, append the vertex to the next frontier, and set the bit. Bottom-up traversal applies the same state test before scanning neighbors.",
    "uncertainty": "The paper does not report atomicity, false-sharing, or bitmap-word contention costs."
  },
  "confidence_rationale": {
    "assumptions": [
      "The benchmarked BFS-VisitedBitmap variant implements the state transitions described in Listing 3."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Section 2 and Listing 3 specify bitmap-based visited checks.",
      "Section 3 and Figures 2-3 report BFS-VisitedBitmap outcomes on two platforms."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-003",
      "SP-004"
    ],
    "text": "Confidence is moderate-low because the source gives a concrete bitmap kernel and reports graph- and platform-dependent benchmarks, but synchronization details are incomplete and this campaign did not inspect code or reproduce results.",
    "uncertainty": "No independent reproduction, code inspection, bitmap-contention analysis, or isolated memory measurement was performed."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Visited membership occupies a vertex-indexed bitmap separate from the vertex-indexed distance array; existing frontier arrays or bitmaps and graph row-pointer and column arrays remain unchanged.",
    "uncertainty": "Bitmap word size, padding, and update granularity are not specified."
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
        "SP-004",
        "SP-005"
      ],
      "text": "On some large-diameter graphs on the evaluated AMD system, bitmap memory overhead does not offset reduced random distance accesses and performance falls below the conventional baseline.",
      "uncertainty": "The regression is platform- and graph-dependent rather than universal."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-TRACK-VISITED-WITH-BITMAPS",
  "falsifying_test": {
    "controlled_variables": [
      "source vertex",
      "thread count",
      "traversal direction schedule",
      "CSR layout",
      "frontier representation",
      "distance word width"
    ],
    "failure_signal": "The bitmap run returns a different distance vector or fails to reduce distance-array discovery reads and cache misses on the cache-resident fixture while adding the predicted bitmap state",
    "fixture": "One small-diameter unweighted graph whose visited bitmap fits in the measured last-level cache and one large-diameter sparse graph with the same vertex cardinality",
    "independent_oracle": "A serial exact BFS distance vector and a parallel distance-array-tested BFS using the same traversal direction",
    "scope": "Smallest visited-bitmap test description only; sorted deferred writes are disabled, and no G09 experiment packet exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A clear visited bit denotes an undiscovered vertex; when traversal accepts that vertex, it records the next-level distance and sets the visited bit before later levels test it again.",
    "uncertainty": "Listing 3 shows logical state transitions but does not fully specify synchronization for concurrent updates to the same bitmap word."
  },
  "knight_bus_algorithm_families": [
    "BFS",
    "UNWEIGHTED_SHORTEST_PATHS",
    "REACHABILITY",
    "BETWEENNESS_CENTRALITY"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Maintain visited membership in a bitmap and consult that bitmap instead of the distance array in both top-down and bottom-up discovery checks, updating distance only after a vertex is found unvisited.",
    "uncertainty": "This card excludes Listing 3 lines 27-41, which are captured separately as sorted deferred distance writes."
  },
  "name": "Track Visited With Bitmaps",
  "pattern_id": "PAT-TRACK-VISITED-WITH-BITMAPS",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Using a multi-byte distance entry only to test whether a vertex has been visited causes random distance-array reads and cache misses.",
    "uncertainty": "The source discusses its own integer distance representation and multicore cache hierarchy."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Visited bits change monotonically from clear to set as vertices are discovered, while next-frontier membership and distances are produced level by level.",
    "uncertainty": "Duplicate concurrent discoveries and bitmap-word synchronization are not specified in the listing."
  },
  "related_pattern_ids": [
    "PAT-SORT-THEN-WRITE-DISTANCES",
    "PAT-SWITCH-TRAVERSAL-BY-FRONTIER"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "The visited bitmap resides with the graph, distance array, and frontier state for the complete BFS traversal.",
    "uncertainty": "The source does not report peak RSS or bitmap allocation overhead."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "For a storage-backed adaptation, measure graph reads, bitmap paging, and spill traffic.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper evaluates an in-memory bitmap and gives no storage-I/O result."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "If visited state is persisted, define its lifecycle and measure encoded bytes and write amplification.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Visited state is traversal-local and the paper defines no persistent bitmap artifact."
    },
    "preprocessing": {
      "assumptions": [
        "Initialization is performed once per BFS query and no prior bitmap state can be reused without clearing."
      ],
      "expression": "Allocate and clear the visited bitmap and mark the BFS source before traversal",
      "measurement_needed": "Measure bitmap allocation and clearing separately from traversal.",
      "premises": [
        "Listing 3 assumes an initialized visited bitmap whose bits represent prior discovery."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "DERIVED",
      "uncertainty": "Initialization method and time are not reported."
    },
    "ram": {
      "assumptions": [
        "Each vertex contributes one logical membership bit and implementation padding is separately measured."
      ],
      "expression": "Baseline in-memory BFS state plus one visited bit per vertex and bitmap alignment or padding",
      "measurement_needed": "Measure allocated bitmap bytes and peak RSS relative to the distance-tested baseline.",
      "premises": [
        "The mechanism adds a vertex-indexed visited bitmap while retaining the distance array and frontier state."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "DERIVED",
      "uncertainty": "Physical bitmap size depends on word packing, alignment, allocator overhead, and any thread-safe representation."
    },
    "temporary_storage": {
      "assumptions": [
        "The bitmap can be released or reused after the query completes."
      ],
      "expression": "One traversal-local visited bitmap in addition to the existing frontier and distance state",
      "measurement_needed": "Measure all bitmap-related allocations, including replicas and synchronization metadata.",
      "premises": [
        "The bitmap is introduced solely to track visited vertices during BFS."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "DERIVED",
      "uncertainty": "Thread-local replicas or synchronization metadata are not described."
    }
  },
  "source_domain": "shared-memory multicore breadth-first search",
  "source_paper_ids": [
    "PAPER-2503.00430"
  ],
  "source_pointers": [
    {
      "claim_scope": "Separate visited bitmap replacing distance-array reads for visited-state checks",
      "locator_type": "SECTION",
      "locator_value": "Section 2, BFS-VisitedBitmap",
      "page": 2,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Bitmap visited-state checks and updates in top-down and bottom-up traversal",
      "locator_type": "ALGORITHM",
      "locator_value": "Listing 3, lines 1-26",
      "page": 3,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Visited-bitmap behavior on small- and large-diameter graphs across two platforms",
      "locator_type": "SECTION",
      "locator_value": "Section 3, Evaluation",
      "page": 4,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Reported BFS-VisitedBitmap performance relative to conventional BFS",
      "locator_type": "FIGURE",
      "locator_value": "Figures 2 and 3",
      "page": 4,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Bitmap locality benefit and graph- and architecture-dependent degradation boundary",
      "locator_type": "SECTION",
      "locator_value": "Section 4, Conclusion",
      "page": 5,
      "paper_id": "PAPER-2503.00430",
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
    "text": "Adjacency entries and visited bitmap locations are read during discovery; distance entries are touched only for vertices accepted as newly unvisited in this base bitmap variant.",
    "uncertainty": "The paper does not provide memory-traffic counters for each structure."
  },
  "unknown_when": [
    {
      "assumptions": [
        "Cache capacity, bitmap contention, random distance latency, and graph degree jointly determine the crossover."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source conditions locality benefits on bitmap cache residency.",
        "The evaluation reports opposite large-diameter behavior across platforms and a sparse-graph exception."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003",
        "SP-005"
      ],
      "text": "The cache-capacity and graph-sparsity breakpoint at which the extra bitmap becomes beneficial is unknown on untested machines and graphs.",
      "uncertainty": "No independent cache-size sweep, contention measurement, or portable crossover model is reported."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-003",
        "SP-004",
        "SP-005"
      ],
      "text": "The bitmap is favorable when it fits in cache and random distance-array checks are costly; the reported variant improves consistently on small-diameter graphs across both evaluated platforms.",
      "uncertainty": "The source benchmark does not isolate cache residency from the other BFS-VisitedBitmap implementation details."
    }
  ]
}
```
