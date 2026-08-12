# Sort Then Write Distances

- Pattern ID: `PAT-SORT-THEN-WRITE-DISTANCES`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `D_THEORETICAL_OR_INCOMPLETE`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus has bitmap-based visited checks and can batch distance writes until a level-local discovery set is sorted."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-SORT-THEN-WRITE-DISTANCES:resident_state",
      "PAT-SORT-THEN-WRITE-DISTANCES:temporary_storage",
      "PAT-SORT-THEN-WRITE-DISTANCES:works_when"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-003"
    ],
    "text": "An A007 feasibility estimate should treat sorted deferred distance writes as an optional locality optimization that requires bounded per-thread discovery buffers and sorting workspace, not as a reduction in the required distance or visited state.",
    "uncertainty": "The source provides no storage-backed result, buffer bound, isolated benchmark, or concurrent-flush semantics."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Scan current-frontier adjacency and update visited bits, complete the discovery loop, sort the accumulated local vertex identifiers, then traverse that sorted list to write distance values in order.",
    "uncertainty": "The source does not define batching across local-frontier flushes or multiple threads."
  },
  "confidence_rationale": {
    "assumptions": [
      "The pseudocode is representative of the intended implementation and the optional extension was not independently evaluated in the paper."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Section 2 and Listing 3 specify bitmap-only discovery followed by sorting and ordered distance writes.",
      "The accompanying paragraph states only a conditional locality benefit."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-003"
    ],
    "text": "Confidence is low because the source supplies explicit pseudocode and a conditional locality rationale but no isolated benchmark, buffer bound, concurrency specification, code inspection, or campaign reproduction.",
    "uncertainty": "No independent reproduction, code inspection, benchmark attribution, sort-cost measurement, or parallel correctness audit was performed."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "A thread-local frontier stores newly discovered vertex identifiers; the visited bitmap records discovery immediately, while the vertex-indexed distance array remains untouched until the local frontier is sorted.",
    "uncertainty": "The paper does not state local-frontier capacity, sorting algorithm, or sorting workspace."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "D_THEORETICAL_OR_INCOMPLETE",
  "fails_when": [
    {
      "assumptions": [
        "Sorting cost grows with retained frontier size and ordered writes offer little gain when the original write order is already local."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The mechanism sorts every retained local discovery frontier before writing distances.",
        "The source conditions its benefit on a small local frontier and a cache-resident visited bitmap."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002",
        "SP-003"
      ],
      "text": "Large local frontiers or already-local distance writes can make sorting cost and retained-buffer memory outweigh the locality benefit.",
      "uncertainty": "The paper reports no crossover size, sort-cost measurement, or adverse case for this extension."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-SORT-THEN-WRITE-DISTANCES",
  "falsifying_test": {
    "controlled_variables": [
      "current frontier",
      "adjacency order",
      "visited bitmap state",
      "thread count",
      "local frontier capacity",
      "distance word width"
    ],
    "failure_signal": "Deferred execution changes any distance or visited result, loses a discovered vertex, exceeds the reserved local-buffer or sort workspace, or fails to make the distance-write address order more monotone on the shuffled fixture",
    "fixture": "One level-synchronous top-down BFS level that discovers a small deliberately shuffled set of vertex identifiers from a fixed current frontier",
    "independent_oracle": "Immediate distance writes checked against a serial exact BFS distance vector and an address-trace comparison",
    "scope": "Smallest sorted deferred-write test description only; direction switching is fixed and no G09 experiment packet exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "All vertices accepted during one level receive the same next-depth value, so their distance writes may be delayed until after discovery without changing exact BFS distances.",
    "uncertainty": "The source relies on level-synchronous unweighted BFS and a visited bitmap that prevents later rediscovery."
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
    "text": "During top-down discovery, consult and update only the visited bitmap, append newly discovered vertex identifiers to a local frontier, sort that frontier after the loop, and then write the shared next-depth value to distance entries in sorted identifier order.",
    "uncertainty": "Listing 3 demonstrates the extension for the top-down phase and does not specify an equivalent bottom-up batching procedure."
  },
  "name": "Sort Then Write Distances",
  "pattern_id": "PAT-SORT-THEN-WRITE-DISTANCES",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Bitmap visited checks can remove distance-array reads from discovery tests, but writing each newly discovered vertex distance immediately still produces random distance-array accesses.",
    "uncertainty": "The source presents this as an optional extension of BFS-VisitedBitmap rather than a separately benchmarked variant."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A sorted order is recomputed for each local discovery frontier before its distance writes are committed.",
    "uncertainty": "The batching frequency and whether buffers are reused are not specified."
  },
  "related_pattern_ids": [
    "PAT-SWITCH-TRAVERSAL-BY-FRONTIER",
    "PAT-TRACK-VISITED-WITH-BITMAPS"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "The graph, visited bitmap, distance array, current frontier, and per-thread local discovery frontier remain memory-resident while the deferred writes are pending.",
    "uncertainty": "The source gives no peak-memory measurement for the local buffers or sort implementation."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "For a storage-backed distance array, measure read and write bytes, request count, and locality before and after sorting.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The optional extension is evaluated only as in-memory code and has no storage-I/O model."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "If batches are persisted or spilled, specify their format and measure retained and temporary bytes.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper defines no persistent artifact for sorted local frontiers or deferred writes."
    },
    "preprocessing": {
      "assumptions": [
        "The graph and bitmap-enabled BFS state are already initialized before this mechanism runs."
      ],
      "expression": "No graph-specific reordering is required; sorting occurs on each discovered local frontier during traversal",
      "measurement_needed": "Measure one-time buffer allocation separately from per-level sorting.",
      "premises": [
        "Listing 3 sorts only the local frontier produced by the current discovery loop."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "DERIVED",
      "uncertainty": "The source does not separately label setup cost or exclude hidden allocator setup."
    },
    "ram": {
      "assumptions": [
        "Sorting executes in memory and may require implementation-specific auxiliary workspace."
      ],
      "expression": "Bitmap-enabled BFS state plus capacity for each thread-local discovery frontier and any sorting workspace",
      "measurement_needed": "Measure peak local-frontier and sort-workspace allocation per thread and for the whole process.",
      "premises": [
        "Listing 3 accumulates discovered vertex identifiers locally and sorts them before distance writes."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "DERIVED",
      "uncertainty": "Local frontier capacity, number of simultaneous buffers, and sorting workspace are unspecified."
    },
    "temporary_storage": {
      "assumptions": [
        "Buffers and sorting workspace are traversal-local and releasable or reusable after each batch."
      ],
      "expression": "Sum of thread-local discovered-vertex capacities plus implementation-specific sorting workspace",
      "measurement_needed": "Record maximum discovered identifiers retained per thread and auxiliary sort bytes.",
      "premises": [
        "The mechanism retains discovered identifiers until sorting and ordered distance writes complete."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "DERIVED",
      "uncertainty": "Peak batch size and sort allocation strategy are not stated."
    }
  },
  "source_domain": "shared-memory multicore breadth-first search",
  "source_paper_ids": [
    "PAPER-2503.00430"
  ],
  "source_pointers": [
    {
      "claim_scope": "Deferring distance-array writes, sorting the local frontier, and writing distances in order",
      "locator_type": "SECTION",
      "locator_value": "Section 2, BFS-VisitedBitmap, optional extension",
      "page": 2,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Bitmap-only discovery loop followed by local-frontier sorting and ordered distance writes",
      "locator_type": "ALGORITHM",
      "locator_value": "Listing 3, lines 27-41",
      "page": 3,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Conditional locality claim when the visited bitmap fits in cache and the local frontier is small",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 2 continuation immediately below Listing 2",
      "page": 3,
      "paper_id": "PAPER-2503.00430",
      "pointer_id": "SP-003"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "The discovery phase streams adjacency and visited-state accesses; the commit phase streams the sorted local frontier and writes distance entries in vertex-identifier order.",
    "uncertainty": "The source does not provide measured cache-line or memory-traffic counts."
  },
  "unknown_when": [
    {
      "assumptions": [
        "A separate benchmark or implementation inspection is required to attribute measured effects specifically to sorted deferred writes."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The optional extension is described in Section 2 and Listing 3.",
        "No separate variant name, ablation, or concurrency schedule is attached to the extension."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002",
        "SP-003"
      ],
      "text": "The source does not establish whether the optional extension was enabled in the reported BFS-VisitedBitmap benchmark or how it behaves under concurrent local-frontier flushes.",
      "uncertainty": "Benchmark inclusion, thread interaction, sorting implementation, and effect size remain unverified."
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
      "text": "The paper expects better cache utilization and fewer memory accesses when the visited bitmap fits in cache and the local discovery frontier is small.",
      "uncertainty": "This is a conditional source claim without a separately reported ablation."
    }
  ]
}
```
