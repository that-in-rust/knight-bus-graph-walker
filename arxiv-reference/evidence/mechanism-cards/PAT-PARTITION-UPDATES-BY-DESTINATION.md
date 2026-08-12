# Partition Updates By Destination

- Pattern ID: `PAT-PARTITION-UPDATES-BY-DESTINATION`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus executes an algorithm in message-producing stages or supersteps.",
      "Messages can be routed to a stable destination interval before the next stage."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-PARTITION-UPDATES-BY-DESTINATION:invariant",
      "PAT-PARTITION-UPDATES-BY-DESTINATION:temporary_storage"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "For staged Knight Bus algorithms, admission could bound sort/group RAM by the largest destination-interval update log rather than the global update stream, but it must estimate message fanout, payload width, and destination skew and reserve temporary SSD bytes for all interval logs.",
    "uncertainty": "Knight Bus message distributions and SSD behavior have not been measured, and a skewed interval may violate the intended in-memory sort bound."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-004"
    ],
    "text": "For each superstep, load and optionally fuse interval logs until sort memory is full, sort/group by destination, process active vertices, append their outbound messages to destination logs, and flush full pages sequentially.",
    "uncertainty": "The degree of interval fusion and log parallelism depends on current message counts and SSD channels."
  },
  "confidence_rationale": {
    "assumptions": [
      "The source implementation follows the described interval and buffer policies."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Algorithm 1 and Sections V.A-V.B define the update path.",
      "Section VIII reports source benchmark evidence across multiple application families."
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-005",
      "SP-006"
    ],
    "text": "Confidence is moderate because the paper specifies the routing and memory-sizing mechanism and benchmarks several mergeable and non-mergeable workloads, but no independent run or code inspection occurred in this campaign.",
    "uncertainty": "Whole-process resource accounting, crash recovery, and performance on current storage devices were not independently checked."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-005"
    ],
    "text": "Static adjacency is stored in CSR, vertices are divided into contiguous intervals, and each interval owns an update log containing destination identifier plus the unmerged message payload.",
    "uncertainty": "Structural mutation is handled through additional buffered per-interval updates rather than direct CSR insertion."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-007"
      ],
      "text": "When all vertices are active in a one-iteration structural-update workload, logging and later CSR mutation can underperform direct shard updates.",
      "uncertainty": "The reported boundary is the source's K-core case on its evaluated implementation."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-PARTITION-UPDATES-BY-DESTINATION",
  "falsifying_test": {
    "controlled_variables": [
      "vertex interval boundaries",
      "message payload width",
      "message multiplicity",
      "sort memory budget",
      "SSD page size"
    ],
    "failure_signal": "Any message is lost or merged incorrectly, the active set differs from the oracle, or one interval requires more sort memory than admission reserved",
    "fixture": "A small CSR graph whose one superstep emits individually distinguishable messages to one heavily skewed destination interval and several lightly loaded intervals",
    "independent_oracle": "An in-memory vertex-centric interpreter that preserves every message and next-active vertex",
    "scope": "Smallest mechanism test description only; no G09 experiment packet exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "Every message is appended to the log indexed by its destination vertex interval, and an interval is sized so its update log can normally be loaded, sorted, and grouped within the assigned memory budget.",
    "uncertainty": "The sizing rule conservatively assumes one incoming-edge update, but exceptional update volume can still challenge the budget."
  },
  "knight_bus_algorithm_families": [
    "BFS",
    "PAGERANK",
    "COMMUNITY_DETECTION",
    "GRAPH_COLORING",
    "MAXIMAL_INDEPENDENT_SET",
    "RANDOM_WALK"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-004"
    ],
    "text": "Partition vertices into contiguous destination intervals, maintain one append-only update log per interval, load only the scheduled interval logs, group messages by destination in memory, and extract the next active set from destinations that received messages.",
    "uncertainty": "The source implementation depends on its vertex-interval map and SSD-aware runtime."
  },
  "name": "Partition Updates By Destination",
  "pattern_id": "PAT-PARTITION-UPDATES-BY-DESTINATION",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A single external update log can require an external sort, while direct CSR updates produce random accesses and shard formats load inactive graph data.",
    "uncertainty": "The problem is framed for bulk-synchronous vertex-centric processing on SSD-backed graphs."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-004"
    ],
    "text": "The active-vertex set is reconstructed each superstep from destination identifiers in the sorted interval logs, and interval log-size counters are refreshed to guide fusion.",
    "uncertainty": "The paper does not isolate the CPU cost of active-set reconstruction from sorting."
  },
  "related_pattern_ids": [
    "PAT-RELOG-PREDICTED-ACTIVE-EDGES",
    "PAT-STREAM-PARTITIONED-UPDATES-SEQUENTIALLY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-004"
    ],
    "text": "Resident state includes the current interval's update records, sort/group workspace, a top-page buffer and mapping entry for each interval log, CSR row-pointer buffers, active-vertex state, and application vertex values.",
    "uncertainty": "The paper reports configured fractions and examples rather than a complete whole-process RSS equation."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "sequential_update_log_writes + relevant_interval_log_reads + CSR_pages_containing_active_vertex_data",
      "measurement_needed": "Record physical bytes read and written per superstep, including write amplification.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "The source reports application page-access behavior but no universal byte coefficient for the symbolic terms."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "CSR_row_pointer + CSR_column_index + optional_value_vector + buffered_structural_update_state",
      "measurement_needed": "Measure all retained CSR and structural-update files after preprocessing and after mutation batches.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "The paper does not provide total storage amplification relative to the input artifact."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "build CSR and statically partition contiguous vertices using conservative incoming-update capacity",
      "measurement_needed": "Measure CSR conversion and interval-planning wall time and write volume.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "One-time construction time and bytes written are not quantified separately."
    },
    "ram": {
      "assumptions": [],
      "expression": "sort_group_budget + one_page_buffer_per_interval + interval_map + CSR_row_buffers + active_and_application_state",
      "measurement_needed": "Measure peak whole-process RSS and each named buffer class at the declared interval count.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "Runtime, allocator, OS cache, and thread-stack memory are not fully accounted by the source."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "sum_over_superstep_messages(destination_id_bytes + message_payload_bytes), partitioned across interval logs",
      "measurement_needed": "Measure peak aggregate interval-log bytes and cleanup lag for the exact message schema.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "The source notes worst-case update count can be proportional to edges but does not bound application payload size or retained generations."
    }
  },
  "source_domain": "SSD-backed vertex-centric graph processing",
  "source_paper_ids": [
    "PAPER-1905.04264"
  ],
  "source_pointers": [
    {
      "claim_scope": "CSR selective adjacency access and sequential logging instead of random edge updates",
      "locator_type": "SECTION",
      "locator_value": "Section III and Section IV.A",
      "page": 3,
      "paper_id": "PAPER-1905.04264",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Destination-interval log routing, bounded interval sorting, and superstep schedule",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 1 and Section IV.B",
      "page": 4,
      "paper_id": "PAPER-1905.04264",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Conservative interval sizing, dynamic interval fusion, and page-buffered logs",
      "locator_type": "SECTION",
      "locator_value": "Section V.A.1-V.A.3",
      "page": 5,
      "paper_id": "PAPER-1905.04264",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "SSD-channel striping, host buffers, log loading, sorting, active-set extraction, and active CSR page loading",
      "locator_type": "SECTION",
      "locator_value": "Section V.A.3 and Section V.B",
      "page": 6,
      "paper_id": "PAPER-1905.04264",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Preservation of individual messages, optional combining, structural updates, and vertex-centric semantics",
      "locator_type": "SECTION",
      "locator_value": "Section V.D-V.F",
      "page": 7,
      "paper_id": "PAPER-1905.04264",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "Application performance as active sets and update logs change across supersteps",
      "locator_type": "FIGURE",
      "locator_value": "Figures 5-7 and Section VIII",
      "page": 9,
      "paper_id": "PAPER-1905.04264",
      "pointer_id": "SP-006"
    },
    {
      "claim_scope": "Pathological all-active structural-update boundary",
      "locator_type": "FIGURE",
      "locator_value": "Figure 8(a), K-core discussion",
      "page": 10,
      "paper_id": "PAPER-1905.04264",
      "pointer_id": "SP-007"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-004"
    ],
    "text": "Update-log pages are appended to SSD and later read by destination interval; CSR row pointers and only adjacency pages containing active vertices are fetched for processing.",
    "uncertainty": "Physical read amplification still depends on SSD page occupancy and active-vertex distribution."
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
        "SP-002",
        "SP-003"
      ],
      "text": "The source does not establish behavior when one skewed destination interval receives more update bytes than the conservative in-memory sorting allocation.",
      "uncertainty": "Overflow policy beyond normal interval sizing is not fully specified."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-005",
        "SP-006"
      ],
      "text": "Active vertices or messages become sparse across supersteps and each destination interval's updates fit the assigned sort/group memory, including workloads whose messages cannot be combined.",
      "uncertainty": "Performance depends on activity distribution, not only active count."
    }
  ]
}
```
