# Pipeline Async IO Compute

- Pattern ID: `PAT-PIPELINE-ASYNC-IO-COMPUTE`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus can probe or declare target-device queue characteristics.",
      "The workload exposes enough concurrent block requests and compute to overlap."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source bounds resident block capacity through a fixed pool.",
      "The source shows that asynchronous overlap improves utilization but synchronous barriers remain."
    ],
    "source_pointer_ids": [
      "SP-003",
      "SP-004",
      "SP-005"
    ],
    "text": "Knight Bus should admit this mechanism with explicit pool capacity and I/O queue limits, while treating sustained throughput as a device- and frontier-conditioned receipt rather than as a guaranteed consequence of asynchronous submission.",
    "uncertainty": "The paper does not provide a portable pre-run utilization model."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003"
    ],
    "text": "A task pull first collects completed reads, moves their blocks to the cached queue, allocates free buffers, submits more high-priority reads, and immediately returns available cached work.",
    "uncertainty": "Batch size and submission cadence adapt implicitly to the compute/I/O balance."
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
      "SP-003",
      "SP-004"
    ],
    "text": "The source specifies the nonblocking preload schedule and reports device-throughput traces, but this campaign did not reproduce or code-inspect the pipeline.",
    "uncertainty": "The reported throughput is hardware- and workload-specific."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "Disk blocks, a preallocated block buffer pool, cached and uncached work queues, and asynchronous completion records form the pipeline stages.",
    "uncertainty": "Queue implementation details are system-specific."
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
      "text": "Synchronous algorithms still incur iteration barriers; asynchronous I/O improves throughput but does not remove synchronization stalls in synchronous mode.",
      "uncertainty": "The source evaluates this boundary with maximal independent set."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-PIPELINE-ASYNC-IO-COMPUTE",
  "falsifying_test": {
    "controlled_variables": [
      "block size",
      "pool capacity",
      "I/O queue depth",
      "worker count",
      "per-block compute duration"
    ],
    "failure_signal": "Executors block waiting for submitted I/O despite available independent work, pool occupancy exceeds its bound, or no I/O/compute interval overlaps",
    "fixture": "Two independent active disk blocks and one resident compute task with a buffer pool large enough for one preload",
    "independent_oracle": "A timestamped trace of I/O submissions, completions, executor activity, and buffer occupancy",
    "scope": "Smallest mechanism-level falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003"
    ],
    "text": "Executors never wait synchronously for a submitted block read; completed reads enter the cached queue, and new reads are issued only when buffer capacity is available.",
    "uncertainty": "Kernel and device queues can still impose backpressure."
  },
  "knight_bus_algorithm_families": [
    "BFS",
    "WEAKLY_CONNECTED_COMPONENTS",
    "PERSONALIZED_PAGERANK",
    "PAGERANK",
    "K_CORE",
    "MAXIMAL_INDEPENDENT_SET"
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
    "text": "Use asynchronous I/O submission/completion rings to preload priority blocks while executors process already resident tasks, combining I/O and computation on adaptive executor threads.",
    "uncertainty": "The implementation uses io_uring; portability to other interfaces is not evaluated."
  },
  "name": "Pipeline Async IO Compute",
  "pattern_id": "PAT-PIPELINE-ASYNC-IO-COMPUTE",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-004"
    ],
    "text": "Blocking disk reads idle CPU workers, while barrier-separated execution leaves the SSD intermittently underutilized.",
    "uncertainty": "The observations are for modern SSD execution in the tested systems."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003"
    ],
    "text": "Each pull recomputes which uncached priority blocks can be admitted to the current free-buffer and I/O-queue capacity.",
    "uncertainty": "The scheduling overhead is not isolated as a symbolic term."
  },
  "related_pattern_ids": [
    "PAT-PIPELINE-ASYNCHRONOUS-DISK-READS",
    "PAT-PRIORITIZE-RESIDENT-ACTIVE-BLOCKS"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "The buffer pool, work queues, block metadata, and completion/submission rings remain resident alongside vertex state.",
    "uncertainty": "Ring and kernel memory are not included in a source-level whole-process formula."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "I/O consists of asynchronous block reads for uncached active work; overlap changes elapsed time and utilization but does not by itself eliminate required bytes.",
      "measurement_needed": "Record submitted/completed reads, bytes, queue depth, and overlap duration.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "Read count is controlled by the separate reuse and scheduling mechanisms."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "The pipeline reads the graph's persistent adjacency blocks; it introduces no separately identified persistent artifact.",
      "measurement_needed": "Confirm that pipeline metadata is rebuilt rather than persisted.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "Persistent queue or trace storage is not described."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Separate pipeline initialization and io_uring registration time from graph preprocessing.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The asynchronous pipeline itself does not have an isolated preprocessing term in the paper."
    },
    "ram": {
      "assumptions": [],
      "expression": "RAM attributable to the pipeline includes the fixed block buffer pool, cached/uncached queues, block metadata, and asynchronous I/O ring state.",
      "measurement_needed": "Measure userspace and kernel memory while varying pool and queue depth.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003",
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "Kernel-side ring memory and allocator overhead are not quantified."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "Temporary state includes in-flight I/O descriptors, completed-I/O records, task batches, and occupied buffer slots bounded by configured queue and pool capacities.",
      "measurement_needed": "Measure peak in-flight descriptors and task-batch bytes.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "The source does not state a closed descriptor-count formula."
    }
  },
  "source_domain": "SSD-based semi-external graph processing",
  "source_paper_ids": [
    "PAPER-2511.07886"
  ],
  "source_pointers": [
    {
      "claim_scope": "io_uring mechanism and compute/I/O overlap",
      "locator_type": "SECTION",
      "locator_value": "Section 2.1, Asynchronous I/O",
      "page": 5,
      "paper_id": "PAPER-2511.07886",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Executor, worklist, asynchronous I/O, and buffer-pool dataflow",
      "locator_type": "FIGURE",
      "locator_value": "Figure 5, ACGraph architecture",
      "page": 9,
      "paper_id": "PAPER-2511.07886",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Nonblocking completion, submission, and adaptive request generation",
      "locator_type": "SECTION",
      "locator_value": "Section 4.5, Preload",
      "page": 12,
      "paper_id": "PAPER-2511.07886",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "I/O-throughput discussion and synchronous-mode boundary",
      "locator_type": "SECTION",
      "locator_value": "Sections 6.3-6.4",
      "page": 21,
      "paper_id": "PAPER-2511.07886",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Performance sensitivity to resident buffer capacity",
      "locator_type": "FIGURE",
      "locator_value": "Figure 14, buffer-pool sensitivity",
      "page": 22,
      "paper_id": "PAPER-2511.07886",
      "pointer_id": "SP-005"
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
    "text": "Active adjacency blocks are read asynchronously from SSD into free buffer slots and passed to executors after completion.",
    "uncertainty": "Write traffic is not the main path described for the graph algorithms."
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
        "SP-001"
      ],
      "text": "The paper does not establish performance on HDDs, remote object storage, or SSDs with substantially different queue and random-read behavior.",
      "uncertainty": "The design is explicitly optimized for modern SSDs."
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
      "text": "The pipeline works when enough independent block reads and executor work exist to overlap latency and drive a modern SSD concurrently.",
      "uncertainty": "Saturation depends on device, graph, frontier, and worker count."
    }
  ]
}
```
