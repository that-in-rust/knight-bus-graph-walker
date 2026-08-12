# Place Scale Growing State

- Pattern ID: `PAT-PLACE-SCALE-GROWING-STATE`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus can identify every scale-growing structure for a selected algorithm",
      "A disk-resident alternative with declared I/O behavior exists for the same semantics"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-PLACE-SCALE-GROWING-STATE: compressed vectors, adjacency, and raw vectors scale with dataset size",
      "PAT-PLACE-SCALE-GROWING-STATE: source placement is gated by a full component sum rather than raw input size"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-005"
    ],
    "text": "Knight Bus should make scale-growing-state placement an admission decision: sum algorithm-specific resident structures and choose memory-resident or disk-resident state before execution, with refusal when neither the resident budget nor the disk/I/O plan is bounded.",
    "uncertainty": "ANN component equations do not directly transfer to BFS, WCC, PageRank, or other Knight Bus families, and estimator error is not reported."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "During best-first traversal, major-in-disk scores neighbors from memory before fetching selected full blocks; all-in-disk fetches traversal and compressed-scoring data from disk along the search path.",
    "uncertainty": "Actual accesses depend on cache hits, beam width, layout, and recall target."
  },
  "confidence_rationale": {
    "assumptions": [
      "Compared implementations expose equivalent recall targets and accurately reported memory totals"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Section 5.1 gives a budget-gated memory decomposition",
      "Sections 4.2-4.3 report source benchmarks for placement variants"
    ],
    "source_pointer_ids": [
      "SP-003",
      "SP-004",
      "SP-005"
    ],
    "text": "Confidence is moderate because the source supplies a component memory model and measured resource/performance tables across diverse ANN datasets, but the campaign did not reproduce the systems or inspect their code.",
    "uncertainty": "No independent rerun, code inspection, estimator-error study, or non-ANN validation was performed."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Major-in-disk stores compressed vectors and selected auxiliary structures in memory while placing adjacency lists and raw vectors on disk; all-in-disk places all scale-growing structures on disk and may colocate compressed vectors with adjacency to share a fetch.",
    "uncertainty": "Individual methods vary in entries, cache, and layout details."
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
        "SP-004",
        "SP-005"
      ],
      "text": "A major-in-disk estimate exceeds the memory budget because its compressed vectors and navigation/cache structures grow with dataset scale.",
      "uncertainty": "The source guide treats this as a placement rejection, not an attempt to spill during the query."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-PLACE-SCALE-GROWING-STATE",
  "falsifying_test": {
    "controlled_variables": [
      "dataset",
      "vector dimension",
      "graph degree",
      "PQ configuration",
      "cache budget",
      "beam width",
      "recall target"
    ],
    "failure_signal": "The estimator admits major-in-disk above the hard memory limit, or selects all-in-disk despite a fitting estimate without the predicted resource trade-off",
    "fixture": "One fixed ANN graph and query set executed with a memory budget just below and just above the measured compressed-vector placement threshold",
    "independent_oracle": "Measured component bytes and peak RSS from instrumented major-in-disk and all-in-disk implementations at matched recall",
    "scope": "Smallest placement/admission falsifier only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-005"
    ],
    "text": "A major-in-disk plan is selected only when the summed navigation, quantization, cache, working-memory, and auxiliary footprint fits budget B; otherwise every scale-growing component is placed on disk and memory is limited to controllable auxiliary state.",
    "uncertainty": "The paper's component equations are ANN-specific and its auxiliary constant is empirical."
  },
  "knight_bus_algorithm_families": [
    "NODESIMILARITY_KNN"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-005"
    ],
    "text": "Estimate the full memory footprint before search; keep compressed vectors in memory with adjacency and raw vectors on disk when the estimate fits, otherwise move compressed vectors to disk as well and retain only entries, cache, and bounded working structures.",
    "uncertainty": "The source's decision guide does not provide estimator error bars."
  },
  "name": "Place Scale Growing State",
  "pattern_id": "PAT-PLACE-SCALE-GROWING-STATE",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Compressed vectors, adjacency lists, and raw vectors all grow with dataset size, so retaining compressed vectors in memory can improve traversal but eventually violates strict memory budgets.",
    "uncertainty": "The relative size of each component depends on vector dimension, quantization, graph degree, and cache policy."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Candidate priorities and exact result distances are recomputed during each query from compressed estimates and fetched raw vectors; scale-growing index state is retained rather than rebuilt per query.",
    "uncertainty": "Index update behavior belongs to a separate mechanism."
  },
  "related_pattern_ids": [
    "PAT-BATCH-UPDATES-BEFORE-CONSOLIDATION",
    "PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-005"
    ],
    "text": "Resident state includes navigation entries, the candidate and result queues, cache/buffer, working memory, and, only for major-in-disk, dataset-scale compressed vectors.",
    "uncertainty": "Allocator and implementation overhead are not represented in the paper's symbolic terms."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Major-in-disk uses memory-resident compressed vectors to reduce disk accesses; all-in-disk typically incurs more disk I/O because traversal and scoring state are both disk-resident.",
      "measurement_needed": "Measure mean and tail block reads, bytes, cache hits, and latency for both placements at matched recall.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "The paper reports workload-specific QPS and does not provide one universal I/O formula across methods."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "Both strategies persist adjacency lists and raw vectors; all-in-disk also persists compressed vectors, while method-specific indexes and replicated data determine total disk use.",
      "measurement_needed": "Measure component-level persistent bytes and storage amplification for the selected method and dataset.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "Table 4 totals are dataset- and method-specific and cannot be generalized as a fixed amplification factor."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Build matched major-in-disk and all-in-disk indexes from the same graph and isolate placement-specific construction time and bytes.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The study reports method-level construction time but does not isolate the incremental preprocessing cost of changing only the placement strategy."
    },
    "ram": {
      "assumptions": [],
      "expression": "M_total is the sum of navigation-graph, PQ, cache, working-memory, and auxiliary terms; select major-in-disk only when M_total is no greater than budget B, otherwise select all-in-disk.",
      "measurement_needed": "Measure peak RSS and estimator error for each placement on fixed N, dimension, degree, PQ, cache, and query settings.",
      "premises": [],
      "source_pointer_ids": [
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "The equations use ANN-specific N, dimension, data width, degree, PQ chunks, and cached-hot-vector terms; estimator error and allocator overhead are absent."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak query scratch, asynchronous buffers, construction scratch, and temporary disk bytes by placement.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper's memory model includes working and auxiliary memory but does not separately bound temporary disk or query spill state."
    }
  },
  "source_domain": "disk-resident graph approximate nearest-neighbor search",
  "source_paper_ids": [
    "PAPER-2603.01779"
  ],
  "source_pointers": [
    {
      "claim_scope": "Representative memory/disk split and disk-resident ANN traversal state",
      "locator_type": "FIGURE",
      "locator_value": "Figure 3 and Section 2 final paragraphs",
      "page": 3,
      "paper_id": "PAPER-2603.01779",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Major-in-disk and all-in-disk definitions, placement rationale, and application boundaries",
      "locator_type": "SECTION",
      "locator_value": "Section 3.1 and Table 2",
      "page": 4,
      "paper_id": "PAPER-2603.01779",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Source QPS/recall and memory-overhead comparison between major-in-disk and all-in-disk variants",
      "locator_type": "SECTION",
      "locator_value": "Section 4.2.1, final four paragraphs",
      "page": 7,
      "paper_id": "PAPER-2603.01779",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Measured disk and memory footprints and storage-strategy findings",
      "locator_type": "TABLE",
      "locator_value": "Table 4 and Section 4.3.1",
      "page": 9,
      "paper_id": "PAPER-2603.01779",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Memory-footprint decomposition and budget-gated placement decision",
      "locator_type": "EQUATION",
      "locator_value": "Equations 1-2 and Section 5.1 item 1",
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
      "SP-001",
      "SP-002"
    ],
    "text": "Adjacency lists and full vectors are streamed in disk blocks for both strategies; all-in-disk also streams compressed vectors needed for approximate scoring.",
    "uncertainty": "Block utilization and bytes per useful vector vary with layout and dimension."
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
        "SP-005"
      ],
      "text": "The study does not establish placement thresholds for storage hierarchies beyond memory and local SSD or for graph workloads outside ANN traversal.",
      "uncertainty": "Cloud object storage and multi-layer placement are listed as future work."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-005"
      ],
      "text": "Major-in-disk works when the computed full memory footprint fits the declared budget and query throughput justifies retaining compressed vectors in memory.",
      "uncertainty": "The source benchmark's performance ratios are not universal."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-005"
      ],
      "text": "All-in-disk works when memory is tight or dataset-scale compressed vectors cannot fit, and lower throughput is acceptable.",
      "uncertainty": "Efficiency still depends on careful colocation, caching, and execution overlap."
    }
  ]
}
```
