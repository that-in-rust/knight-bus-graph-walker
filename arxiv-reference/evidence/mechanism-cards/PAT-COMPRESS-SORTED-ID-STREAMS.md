# Compress Sorted ID Streams

- Pattern ID: `PAT-COMPRESS-SORTED-ID-STREAMS`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus requires both source- and destination-oriented scans for the admitted workload set",
      "The runtime can measure or conservatively estimate graph-specific compression before committing final storage"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-COMPRESS-SORTED-ID-STREAMS: final execution uses two compressed orientations and optional separate weights",
      "PAT-COMPRESS-SORTED-ID-STREAMS: preprocessing creates block files before compression/merge and removes them only afterward"
    ],
    "source_pointer_ids": [
      "SP-003",
      "SP-004"
    ],
    "text": "Knight Bus should quote compressed persistent bytes, both required orientations, optional weights, decode buffers, and peak preprocessing amplification separately; a hard storage budget must cover coexistence of input blocks and merge outputs rather than assuming final compressed size alone.",
    "uncertainty": "The paper does not report peak temporary storage, physical RSS under mmap, or compression estimator error."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "Preprocessing maps and sorts each block, compresses it, and merges blocks in source and destination orientations; execution reads a bounded compressed subgrid, batch-decodes IDs, and reads weights only when requested.",
    "uncertainty": "mmap sorting can fault arbitrary pages when a block exceeds RAM."
  },
  "confidence_rationale": {
    "assumptions": [
      "The implementation uses the described reversible transform and reports complete compared graph-file sizes"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Sections 3.2-3.3 specify sorting, XOR, Simple-8b, merging, and separate weights",
      "Sections 4.4-4.5 report source preprocessing and storage measurements"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-003",
      "SP-004",
      "SP-005"
    ],
    "text": "Confidence is moderate because the lossless pipeline is precisely described and source preprocessing/file-size measurements are reported, but component effects, peak resources, and implementation correctness were not independently verified.",
    "uncertainty": "No code inspection, decode round-trip reproduction, or isolated compression ablation occurred."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "Each oriented merged graph file contains compressed source and destination ID streams grouped by block/subgrid metadata; weight files are separate so unweighted algorithms do not load them.",
    "uncertainty": "Two orientations duplicate compressed graph storage when both are retained."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-004"
      ],
      "text": "ID order provides poor adjacent XOR compressibility or preprocessing/decode work is not amortized over enough scans.",
      "uncertainty": "The source does not report an explicit compression or iteration crossover."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-COMPRESS-SORTED-ID-STREAMS",
  "falsifying_test": {
    "controlled_variables": [
      "vertex-ID order",
      "partition count",
      "subgrid size",
      "weight presence and width",
      "thread count",
      "filesystem"
    ],
    "failure_signal": "Decoded oriented streams differ from the oracle, unweighted execution reads weight bytes, or peak live preprocessing storage exceeds the declared coexistence accounting",
    "fixture": "A small graph with repeated and nonlocal sorted IDs, plus optional edge weights, encoded into both orientations",
    "independent_oracle": "Byte-for-byte original edge multiset and weights sorted independently for each orientation",
    "scope": "Smallest losslessness/selective-I/O/storage-accounting falsifier only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Sorting makes adjacent IDs similar, XOR of adjacent IDs is reversible from the first explicit ID, and Simple-8b packs the resulting unsigned integers losslessly into selector-described 64-bit words.",
    "uncertainty": "Compression ratio depends on sorted-ID locality."
  },
  "knight_bus_algorithm_families": [
    "BFS_SHORTEST_PATHS",
    "WCC_CONNECTED_COMPONENTS",
    "PAGERANK_CENTRALITY",
    "SPARSE_MATRIX_VECTOR",
    "COMMUNITY_DETECTION"
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
    "text": "Within each block, sort edges by source then destination, split source and destination IDs into separate streams, store the first ID and XOR each adjacent pair, encode transformed values with Simple-8b, and keep edge weights in separate optional files.",
    "uncertainty": "The source later chooses which ID stream to compress based on encoded lengths, and concrete details may differ by orientation."
  },
  "name": "Compress Sorted ID Streams",
  "pattern_id": "PAT-COMPRESS-SORTED-ID-STREAMS",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "Out-of-core graph algorithms decode the same edge identifiers in many iterations, so the representation must reduce bytes transferred without making repeated decompression too expensive; interleaving source, destination, and unused weights wastes locality and I/O.",
    "uncertainty": "The best codec depends on ID order and decode hardware."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-006"
    ],
    "text": "Edge IDs are decoded from the compressed stream on every processing pass rather than stored persistently in expanded form.",
    "uncertainty": "Repeated decode cost is traded for lower I/O and is not isolated as CPU cycles in the evaluation."
  },
  "related_pattern_ids": [
    "PAT-REUSE-LOADED-SUBGRIDS-SELECTIVELY",
    "PAT-SCHEDULE-SUBGRIDS-BY-DEPENDENCY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "Execution holds compressed input, a decoded edge-ID buffer, subgrid metadata, and optional mapped weights; preprocessing maps a block for in-place sorting.",
    "uncertainty": "Virtual mapping does not bound physical RSS, and the paper does not report peak process memory."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Execution reads compressed source/destination ID streams and omits separate weights for workloads that do not use them; source Table 7 reports compressed merged-file sizes for four datasets.",
      "measurement_needed": "Record physical bytes read, decompressed edges, weight bytes avoided, and device time per iteration.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-004",
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "File-size reduction is source-measured against GridGraph and does not equal a universal runtime I/O reduction."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "Two orientation-specific compressed merged graph files are produced; separate optional weight files and subgrid metadata are also retained, while intermediate block files may be deleted after merging.",
      "measurement_needed": "Measure both oriented files, metadata, weights, maps, and any retained input or temporary blocks.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "Table 7 compares one orientation because the two graph files have equal size and does not include every metadata/weight file."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Generate block files, sort each block by source and destination IDs using mmap/in-place qsort, XOR-transform and Simple-8b encode separate ID streams, then merge every block into both orientations.",
      "measurement_needed": "Measure generation, sort, encode, and each orientation merge separately with bytes read/written.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-003",
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "Reported preprocessing time combines generation, merging, and optional reindexing and is hardware/dataset-specific."
    },
    "ram": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak RSS and major faults during preprocessing and execution by block/subgrid size and thread count.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not bound physical RSS for mmap sorting, compressed/decoded buffers, metadata, and worker state."
    },
    "temporary_storage": {
      "assumptions": [
        "The implementation retains input block files until both required merges complete"
      ],
      "expression": "During preprocessing, p by p uncompressed block files coexist with compressed merge output until block files are removed; mmap sort and merge buffers add further temporary state.",
      "measurement_needed": "Trace live temporary files and mapped/buffer bytes throughout preprocessing to identify peak amplification.",
      "premises": [
        "Partitioning first generates p by p block files",
        "Each block is compressed and merged before previous partition files are removable",
        "Both source and destination orientations are produced"
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "status": "DERIVED",
      "uncertainty": "Scheduling, deletion timing, filesystem allocation, and mmap paging make peak bytes unknown."
    }
  },
  "source_domain": "lossless graph edge-stream compression for iterative out-of-core processing",
  "source_paper_ids": [
    "PAPER-HASH-b12240577b20eaad"
  ],
  "source_pointers": [
    {
      "claim_scope": "Block partition files, independent source/destination streams, adjacent XOR transform, Simple-8b, and separate weights",
      "locator_type": "SECTION",
      "locator_value": "Sections 3.2-3.3, first three compression paragraphs",
      "page": 9,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Simple-8b 64-bit selector packing modes",
      "locator_type": "TABLE",
      "locator_value": "Table 1",
      "page": 10,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Sorting order, mmap in-place sort, orientation-specific merging, subgrid size, optional weight access, and temporary block deletion",
      "locator_type": "SECTION",
      "locator_value": "Section 3.3 continuation",
      "page": 11,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Source preprocessing-time and compressed-file-size measurements",
      "locator_type": "TABLE",
      "locator_value": "Tables 6-7 and Sections 4.4-4.5",
      "page": 17,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Source statement that produced files were at least half smaller than GridGraph on evaluated datasets",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 4.5, first paragraph",
      "page": 18,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "Compression contribution to out-of-core I/O reduction",
      "locator_type": "SECTION",
      "locator_value": "Section 6, first paragraph",
      "page": 20,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-006"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "Compressed ID words and only requested weight streams move from storage to memory; decoded edge IDs feed the graph streamer.",
    "uncertainty": "Read amplification from page cache and alignment is not reported."
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
        "SP-006"
      ],
      "text": "The source does not isolate compression from reindexing, scheduling, and streamer effects or evaluate alternative codecs, mutable graphs, and higher-core platforms.",
      "uncertainty": "The paper explicitly requests component ablation and broader platform evaluation."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-004",
        "SP-006"
      ],
      "text": "Sorted source or destination IDs have local similarity and the graph is scanned repeatedly enough for reduced I/O to amortize sorting, encoding, and repeated decoding.",
      "uncertainty": "Compression ratio and amortization vary by topology and iteration count."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "text": "Some algorithms do not consume edge weights, so separate weight files prevent unnecessary weight I/O.",
      "uncertainty": "Weighted algorithms still pay the separate weight-stream cost."
    }
  ]
}
```
