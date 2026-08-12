# Schedule Subgrids By Dependency

- Pattern ID: `PAT-SCHEDULE-SUBGRIDS-BY-DEPENDENCY`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus can derive a correctness-preserving dependency DAG for the selected synchronous algorithm",
      "Partitioning/reindexing fixes region sizes before admission"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-SCHEDULE-SUBGRIDS-BY-DEPENDENCY: only prerequisite-complete subgrids may compute future values",
      "PAT-SCHEDULE-SUBGRIDS-BY-DEPENDENCY: secondary-phase I/O depends on the region not covered in primary processing and optional pinning"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-004"
    ],
    "text": "A Knight Bus future-value plan would need to quote primary and secondary region bytes separately, include dependency and integration state, and bound optional pinned blocks; it should refuse the plan when the algorithm lacks a proven dependency schedule or the secondary region cannot be bounded.",
    "uncertainty": "The paper does not provide a formal schedule proof, isolated component costs, or whole-process memory bound."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "In primary phases, release subgrid tasks according to topological-style prerequisites and trigger integration after a shard's subgrids finish; in alternating secondary phases, stream only blocks without precomputed future values.",
    "uncertainty": "Dynamic worker ordering may vary while respecting dependencies."
  },
  "confidence_rationale": {
    "assumptions": [
      "The evaluated implementation enforces the stated prerequisites"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Algorithm 2 and Section 2.2 specify the schedule",
      "Section 4 reports source PageRank and resource-sensitivity results"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-005",
      "SP-006",
      "SP-007",
      "SP-008"
    ],
    "text": "Confidence is moderate because the primary/secondary schedule and prerequisites are explicit and PageRank, memory, and thread results are reported, but contributions are not ablated and the campaign did not reproduce or inspect code.",
    "uncertainty": "No formal schedule verification, independent rerun, or component ablation was performed."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "A p by p block grid is oriented by destination or source; blocks on one side of the diagonal form the primary future-value region, the other side forms the secondary region, and each block is divided into dependency-scheduled subgrids.",
    "uncertainty": "Reindexing changes region sizes but not correctness."
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
      "text": "The algorithm lacks usable future-value dependencies, or a misconstructed prerequisite schedule releases a subgrid before its required shard and integration work complete.",
      "uncertainty": "KedaGraph routes other workload classes to different streamers."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-SCHEDULE-SUBGRIDS-BY-DEPENDENCY",
  "falsifying_test": {
    "controlled_variables": [
      "partitioning",
      "orientation",
      "reindexing",
      "subgrid dependency graph",
      "pin budget",
      "thread schedule"
    ],
    "failure_signal": "A dependency-respecting execution differs from the synchronous oracle, reads primary-region blocks again in the promised secondary-only phase without a declared reason, or exceeds the pinned-memory cap",
    "fixture": "A small partitioned directed graph with a hand-derived dependency chain across primary, diagonal, and secondary blocks for several PageRank iterations",
    "independent_oracle": "A conventional fully synchronous full-graph PageRank iteration using the same arithmetic and iteration count",
    "scope": "Smallest dependency/future-value falsifier only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "A primary-phase subgrid that computes future values executes only after every subgrid and integration task in its prerequisite shard is complete; secondary-region blocks are processed without such prerequisites in the alternating phase.",
    "uncertainty": "The source presents operational dependency rules rather than a general formal correctness proof."
  },
  "knight_bus_algorithm_families": [
    "PAGERANK_CENTRALITY"
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
    "text": "Split oriented blocks into a future-value primary region and a secondary region, schedule primary subgrids as soon as shard prerequisites resolve, integrate completed shards, and alternate with a phase that reads only the secondary region.",
    "uncertainty": "Correct orientation and dependency construction are algorithm-specific."
  },
  "name": "Schedule Subgrids By Dependency",
  "pattern_id": "PAT-SCHEDULE-SUBGRIDS-BY-DEPENDENCY",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Synchronous iterative algorithms repeatedly scan the entire disk graph, while coarse shard barriers leave workers waiting even when a subgrid's actual prerequisites have already completed.",
    "uncertainty": "The opportunity depends on the dependency orientation and amount of future-value-eligible work."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-003"
    ],
    "text": "Future vertex values and shard integration results are computed during the primary phase so the next secondary phase can omit the already-covered region.",
    "uncertainty": "Only algorithms with suitable dependencies can use future-value computation."
  },
  "related_pattern_ids": [
    "PAT-COMPRESS-SORTED-ID-STREAMS"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-007"
    ],
    "text": "Vertex values, subgrid dependency counters, integration state, worker buffers, and any pinned secondary/diagonal blocks are resident during execution.",
    "uncertainty": "The amount pinned depends on the provided cache-memory budget and is not expressed as a closed bound."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Each primary phase processes both regions to compute future values, while the alternating secondary phase reads only blocks where future values were not computed; pinning eligible blocks can avoid redundant reloads.",
      "measurement_needed": "Measure primary/secondary bytes, pinned-hit bytes, region sizes, and I/O per logical PageRank iteration.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "No closed byte formula accounts for region size, compression, and pinning together."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure oriented streams, subgrid offsets, dependency metadata, and retained reindex map separately.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The source does not isolate persistent bytes for dependency metadata and dual regions from the compressed oriented graph files."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Optionally compute and sort vertices by in-degree/out-degree ratio, then partition, compress, orient, and split blocks into dependency-scheduled subgrids.",
      "measurement_needed": "Measure reindexing, orientation, dependency metadata, and shared compression/layout time separately.",
      "premises": [],
      "source_pointer_ids": [
        "SP-004",
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "The paper reports total preprocessing components elsewhere but does not isolate dependency-graph construction from shared KedaGraph preparation."
    },
    "ram": {
      "assumptions": [],
      "expression": "Optional reindexing requires in-degree and out-degree metadata totaling 8 bytes per vertex; runtime also accepts a cache-memory budget used to pin eligible blocks, but total RSS is not bounded.",
      "measurement_needed": "Measure peak RSS with reindexing on/off and by pin budget, partition count, vertex-state width, and thread count.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "Dependency metadata, vertex values, worker buffers, and allocator overhead are not included in the 8-byte term."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure phase-level peak temporary RAM and disk scratch with different partition/thread counts.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Concurrent subgrid buffers, integration state, and preprocessing sort scratch are not bounded."
    }
  },
  "source_domain": "synchronous iterative out-of-core graph processing",
  "source_paper_ids": [
    "PAPER-HASH-b12240577b20eaad"
  ],
  "source_pointers": [
    {
      "claim_scope": "Primary/secondary block split and future-value computation purpose",
      "locator_type": "SECTION",
      "locator_value": "Section 2.2 and Figure 2",
      "page": 5,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Alternating primary/secondary phases and LUMOS shard-barrier dependency",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 2 and following paragraphs",
      "page": 6,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Topological-style subgrid prerequisites, integration trigger, secondary reads, and diagonal pinning",
      "locator_type": "SECTION",
      "locator_value": "Section 2.2 continuation",
      "page": 7,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "In/out-degree-ratio reindexing, 8 bytes per vertex, and disable/correctness boundary",
      "locator_type": "SECTION",
      "locator_value": "Section 3.1",
      "page": 8,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "PageRank fixture, iteration count, memory limit, and enabled reindexing",
      "locator_type": "SECTION",
      "locator_value": "Section 4.2, primary-secondary evaluation setup",
      "page": 14,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "Source primary-secondary PageRank execution results",
      "locator_type": "TABLE",
      "locator_value": "Table 3, PageRank rows",
      "page": 15,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-006"
    },
    {
      "claim_scope": "Memory-budget and thread-count sensitivity for PageRank",
      "locator_type": "TABLE",
      "locator_value": "Tables 4-5 and Section 4.3",
      "page": 16,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-007"
    },
    {
      "claim_scope": "Grafu distinction, relaxed subgrid barriers, memory pinning, and source-stated limitations",
      "locator_type": "SECTION",
      "locator_value": "Section 5.4 final paragraph and Section 6",
      "page": 20,
      "paper_id": "PAPER-HASH-b12240577b20eaad",
      "pointer_id": "SP-008"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "Compressed primary-region subgrids are streamed in primary phases and only secondary-region subgrids are streamed in secondary phases, except blocks retained in memory.",
    "uncertainty": "Actual bytes depend on reindexing, compression, and pinning."
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
        "SP-008"
      ],
      "text": "The source does not establish gains beyond PageRank, across higher-core/larger-memory platforms, or after isolating reindexing, scheduling, pinning, and compression contributions.",
      "uncertainty": "These are explicit future-work limitations."
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
        "SP-005"
      ],
      "text": "The algorithm is synchronous and iterative, its edge dependencies permit future-value computation, and enough work lies in the primary region to make alternating secondary-only reads beneficial.",
      "uncertainty": "The source evaluates this mechanism using fixed-iteration PageRank."
    }
  ]
}
```
