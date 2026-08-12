# Prioritize Resident Active Blocks

- Pattern ID: `PAT-PRIORITIZE-RESIDENT-ACTIVE-BLOCKS`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus can classify algorithm ordering requirements before execution.",
      "Block size, metadata size, and pool capacity are declared in the execution plan."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source fixes the resident state categories and buffer pool.",
      "The source shows that scheduling order changes block reuse and edge work."
    ],
    "source_pointer_ids": [
      "SP-003",
      "SP-005",
      "SP-006"
    ],
    "text": "A Knight Bus admission contract for this schedule should reserve vertex state, block metadata, queues, and a fixed block pool, then treat block-read count and repeated work as workload-dependent quantities with measured receipts rather than RAM-only consequences.",
    "uncertainty": "The paper does not provide a pre-run predictor for reactivation order or total repeated I/O."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003"
    ],
    "text": "Executors pull batches only from the cached queue; submissions update active vertices by assigned block; finished reactivated blocks return to the cached queue and inactive blocks release their buffers.",
    "uncertainty": "Concurrent queue ordering among equal-priority blocks is not specified."
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
      "SP-002",
      "SP-003",
      "SP-006"
    ],
    "text": "The state machine and queue schedule are described precisely and evaluated across several graph algorithms and datasets, but the campaign did not inspect or reproduce the implementation.",
    "uncertainty": "Benchmark results remain source-reported."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-005"
    ],
    "text": "Edges are divided into disk blocks; vertex state and block metadata stay in memory; each block stores a local active-frontier set, priority, state, and optional resident-data pointer.",
    "uncertainty": "The source implementation fixes block size to its SSD design."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-004"
      ],
      "text": "Algorithms whose correctness requires a global barrier cannot use the asynchronous schedule directly and must select the synchronous interface, which retains synchronization stalls.",
      "uncertainty": "The paper demonstrates this boundary with maximal independent set rather than every barrier-dependent algorithm."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-005"
      ],
      "text": "Repeatedly reusing one block can disrupt global priority order and cause redundant computation; the implementation offers a forced-eviction threshold for this case.",
      "uncertainty": "The paper reports little effect in its experiments and disables the safeguard by default."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-PRIORITIZE-RESIDENT-ACTIVE-BLOCKS",
  "falsifying_test": {
    "controlled_variables": [
      "block assignment",
      "buffer-pool capacity",
      "vertex priority function",
      "worker count",
      "activation sequence"
    ],
    "failure_signal": "An activated vertex is lost, a nonresident block is executed before an eligible resident block contrary to the rule, state exceeds the pool bound, or output differs from the sequential oracle",
    "fixture": "A two-block graph where processing the first block reactivates itself and activates a higher-priority vertex in the second block",
    "independent_oracle": "A sequential implementation of the same algorithm plus a logged block-state transition trace",
    "scope": "Smallest mechanism-level falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Every activated vertex belongs to one assigned block, and each active block occupies exactly one state among uncached, cached, processing, or reactivated until its work becomes inactive.",
    "uncertainty": "Large adjacency lists may span consecutive disk blocks and are handled by the storage layer."
  },
  "knight_bus_algorithm_families": [
    "BFS",
    "WEAKLY_CONNECTED_COMPONENTS",
    "PERSONALIZED_PAGERANK",
    "PAGERANK",
    "K_CORE"
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
    "text": "Schedule memory-resident active blocks before disk-resident blocks, requeue a block immediately when it is reactivated during processing, and priority-order unloaded blocks using priorities aggregated from their active vertices.",
    "uncertainty": "The aggregation function and vertex priority are application-defined."
  },
  "name": "Prioritize Resident Active Blocks",
  "pattern_id": "PAT-PRIORITIZE-RESIDENT-ACTIVE-BLOCKS",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-006"
    ],
    "text": "Iteration barriers and vertex-granular scheduling separate accesses to the same disk block, waste fetched adjacency data, and stall I/O workers.",
    "uncertainty": "The measured effect is bounded to the tested SSD systems and graph algorithms."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "Block priority is recomputed from changing active-vertex priorities, and a reactivated block can repeat edge processing until no new activation remains.",
    "uncertainty": "The paper includes an optional consecutive-reuse threshold because repeated processing can become suboptimal."
  },
  "related_pattern_ids": [
    "PAT-INLINE-LOW-DEGREE-ADJACENCIES",
    "PAT-PIPELINE-ASYNC-IO-COMPUTE"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003",
      "SP-005"
    ],
    "text": "Vertex state, edge offsets, block metadata, active-frontier metadata, the dual work queues, and a fixed buffer pool are resident.",
    "uncertainty": "User algorithm state is additional and workload-specific."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Disk reads are incurred for uncached active blocks; resident reactivation can reuse a loaded block without another read, while post-eviction reactivation can reload it.",
      "measurement_needed": "Record unique and repeated block reads, bytes, and block residency intervals.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003",
        "SP-006"
      ],
      "status": "SOURCED",
      "uncertainty": "Future activation order prevents a static exact block-read count in the paper."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "Persistent storage includes the partitioned edge blocks and graph index information needed to locate adjacency lists.",
      "measurement_needed": "Measure edge payload, block fragmentation, and index bytes separately.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "Storage amplification from fragmentation depends on the partitioner and graph degree distribution."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Preprocessing partitions adjacency data into blocks, assigns vertices to blocks, and builds per-block metadata.",
      "measurement_needed": "Measure partition time, metadata-build time, and output bytes on the admitted graph.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "Partitioning cost depends on the selected storage policy."
    },
    "ram": {
      "assumptions": [],
      "expression": "RAM = vertex and algorithm state + block metadata + work queues + fixed buffer-pool capacity + executor-local activation buffers.",
      "measurement_needed": "Measure peak RSS and separate vertex, metadata, queue, and buffer-pool bytes.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "Application state and allocator overhead are not expressed by a portable coefficient."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "Temporary storage includes executor-local activation buffers, in-flight task batches, and resident block buffers bounded by the configured pool.",
      "measurement_needed": "Measure maximum queued blocks, activation entries, and in-flight task bytes.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "Queue and batch cardinalities are not given as a closed bound."
    }
  },
  "source_domain": "SSD-based semi-external graph processing",
  "source_paper_ids": [
    "PAPER-2511.07886"
  ],
  "source_pointers": [
    {
      "claim_scope": "Block scheduling unit, aggregated priority, and relaxed-order semantics",
      "locator_type": "SECTION",
      "locator_value": "Section 4.1, Block-Centric Asynchronous Execution Model",
      "page": 8,
      "paper_id": "PAPER-2511.07886",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Executor, worklist, buffer architecture, and block state discussion",
      "locator_type": "FIGURE",
      "locator_value": "Figure 5 and Sections 4.1-4.2",
      "page": 9,
      "paper_id": "PAPER-2511.07886",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Dual queues, resident-first retrieval, reactivation, and reclamation",
      "locator_type": "SECTION",
      "locator_value": "Section 4.2, Worklist and Buffer Pool",
      "page": 10,
      "paper_id": "PAPER-2511.07886",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Synchronous fallback, consistency, and correctness boundary",
      "locator_type": "SECTION",
      "locator_value": "Sections 4.3-4.4",
      "page": 11,
      "paper_id": "PAPER-2511.07886",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Metadata, preload, and consecutive-reuse early-stop rule",
      "locator_type": "SECTION",
      "locator_value": "Section 4.5, Block Management",
      "page": 12,
      "paper_id": "PAPER-2511.07886",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "Read inflation, work inflation, and I/O-throughput benchmark discussion",
      "locator_type": "SECTION",
      "locator_value": "Section 6.3 and following discussion",
      "page": 21,
      "paper_id": "PAPER-2511.07886",
      "pointer_id": "SP-006"
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
    "text": "Adjacency blocks for uncached active work are loaded from SSD into the fixed buffer pool and reclaimed when their blocks become inactive.",
    "uncertainty": "A block may be reloaded if it is reactivated after eviction."
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
        "SP-004"
      ],
      "text": "Automatic selection between synchronous and asynchronous modes is outside the paper's scope.",
      "uncertainty": "Users must currently select the mode through distinct APIs."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-004"
      ],
      "text": "The mechanism applies when vertex state fits in memory, edges do not, and the graph algorithm converges correctly under relaxed sequential orderings.",
      "uncertainty": "Priority quality affects practical work but not the stated sequential-consistency condition."
    }
  ]
}
```
