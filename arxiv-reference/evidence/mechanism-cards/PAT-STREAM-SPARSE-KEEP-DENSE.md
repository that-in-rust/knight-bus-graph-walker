# Stream Sparse Keep Dense

- Pattern ID: `PAT-STREAM-SPARSE-KEEP-DENSE`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus can identify a graph-linear-algebra workload with a sparse external operand and dense column operand.",
      "The target implementation exposes element width, thread count, buffer size, and encoded sparse bytes."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source requires complete dense columns in each resident vertical partition.",
      "The source relates partition capacity M' to repeated sparse-input reads."
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-004"
    ],
    "text": "Knight Bus can treat complete dense-column partitions as an admission unit and estimate sparse-pass count before execution, while reporting SSD traffic separately from resident and temporary bytes.",
    "uncertainty": "The source does not establish that the same estimator remains accurate for non-SSD devices, fused operators, or runtimes with different buffering."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-004"
    ],
    "text": "Workers dynamically claim contiguous tile-row tasks, issue asynchronous sparse reads, compute when reads complete, and asynchronously merge nearby output writes; another sparse pass is required for each dense column partition.",
    "uncertainty": "The exact interleaving depends on I/O completion and runtime scheduling."
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
      "SP-004",
      "SP-005"
    ],
    "text": "The placement rule is defined in the method, Algorithm 1 shows its execution, Section 3.6 supplies symbolic resource expressions, and the evaluation varies resident columns; this campaign did not reproduce or inspect code.",
    "uncertainty": "Reported performance is specific to the paper's graphs and SSD platform."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "The sparse matrix is tiled and SSD-resident; each in-memory dense vertical partition contains complete columns in row-major order and is horizontally striped across NUMA nodes after loading.",
    "uncertainty": "Tile-size selection is discussed separately and is not a universal constant."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-005",
        "SP-006"
      ],
      "text": "Very narrow vertical partitions incur repeated sparse passes and lose dense-operand locality; the measured overhead is largest when only small partitions fit.",
      "uncertainty": "The source reports degradation rather than semantic failure."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-004"
      ],
      "text": "The semi-external premise fails when RAM cannot hold one complete dense column together with required thread buffers.",
      "uncertainty": "A different fully external algorithm may still be possible but is not this mechanism."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-STREAM-SPARSE-KEEP-DENSE",
  "falsifying_test": {
    "controlled_variables": [
      "sparse encoding",
      "dense element width",
      "thread count",
      "buffer size",
      "resident dense-column count",
      "storage device"
    ],
    "failure_signal": "The result differs from the oracle, peak RAM exceeds the declared resident-plus-buffer model, or sparse-input reads do not increase with the required extra pass under the equation's assumptions",
    "fixture": "A sparse matrix and two-column dense operand sized so RAM can hold exactly one dense column plus declared thread buffers",
    "independent_oracle": "An in-memory multiplication result and device-level byte counters",
    "scope": "Smallest mechanism-level falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "At least one complete input-dense column partition is resident in RAM while the sparse matrix is streamed from SSDs; each output region is completed in local buffers before its asynchronous write.",
    "uncertainty": "Whether the complete output matrix is resident or streamed is application- and capacity-dependent."
  },
  "knight_bus_algorithm_families": [
    "SPARSE_MATRIX_DENSE_MULTIPLICATION",
    "ITERATIVE_GRAPH_LINEAR_ALGEBRA"
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
    "text": "Partition the dense operand by complete columns that fit RAM, stream horizontal sparse tile rows asynchronously, multiply them against the resident partition, merge intermediate results in thread-local buffers, and write each output region at most once.",
    "uncertainty": "The paper's concrete scheduling and I/O implementation is tied to its FlashX environment."
  },
  "name": "Stream Sparse Keep Dense",
  "pattern_id": "PAT-STREAM-SPARSE-KEEP-DENSE",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A sparse graph matrix and its associated dense matrices may not fit together in RAM, while random or repeated SSD access can dominate sparse-matrix multiplication.",
    "uncertainty": "The source studies multicore machines with SSDs rather than every external-memory device."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-004"
    ],
    "text": "The multiplication does not deliberately recompute sparse products; repeated sparse scans arise because separate dense-column partitions require separate passes.",
    "uncertainty": "Application-level iterative recomputation is outside this mechanism's single-call scope."
  },
  "related_pattern_ids": [
    "PAT-PACK-NONEMPTY-SPARSE-ROWS"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-004"
    ],
    "text": "RAM holds M' bytes of complete input-dense columns plus per-thread sparse-input and output buffers; optional remaining memory may cache sparse data.",
    "uncertainty": "The output matrix may also be resident when sufficiently small."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "IO_in = (n*c*p/M') * [E - (M - M')] bytes of sparse-input reads",
      "measurement_needed": "Measure total device reads and writes, including dense partitions and output, for the target fixture.",
      "premises": [],
      "source_pointer_ids": [
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "The expression assumes M' < M, n*c*p is divisible by M', E > M, and M-M' caches sparse data; it does not include all dense-input or output traffic."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "E bytes for the SSD-resident sparse matrix, plus any SSD-resident input and output dense matrices",
      "measurement_needed": "Measure encoded sparse bytes and external dense-operand bytes separately.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "The paper denotes sparse storage by E but does not give one symbolic total for all persistent dense operands."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Linear-time CSR-to-SCSR conversion with one sequential CSR read and one sequential SCSR write",
      "measurement_needed": "Measure conversion wall time and bytes for the target input format and storage device.",
      "premises": [],
      "source_pointer_ids": [
        "SP-007"
      ],
      "status": "SOURCED",
      "uncertainty": "The source reports conversion as a one-time cost amortized by repeated multiplication."
    },
    "ram": {
      "assumptions": [],
      "expression": "n*c + t*epsilon bytes minimum",
      "measurement_needed": "Measure allocator, runtime, queue, and alignment overhead beyond the stated minimum.",
      "premises": [],
      "source_pointer_ids": [
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "n is the dense-matrix row count, c is element bytes, t is thread count, and epsilon is each thread's sparse/output buffer size."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "t*epsilon bytes for per-thread sparse-input and output buffers within the stated minimum",
      "measurement_needed": "Measure peak temporary allocation including queues and asynchronous-I/O metadata.",
      "premises": [],
      "source_pointer_ids": [
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "Queue metadata and runtime I/O descriptors are not included in epsilon explicitly."
    }
  },
  "source_domain": "Semi-external-memory sparse-matrix dense-matrix multiplication for graph analytics",
  "source_paper_ids": [
    "PAPER-1602.02864"
  ],
  "source_pointers": [
    {
      "claim_scope": "The sparse matrix remains on SSDs, dense columns remain in memory, threads stream sparse partitions, and local buffers accumulate intermediate results.",
      "locator_type": "SECTION",
      "locator_value": "1 Introduction, semi-external-memory definition and execution summary",
      "page": 2,
      "paper_id": "PAPER-1602.02864",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "The input dense matrix, or a complete-column vertical partition of it, is resident while the sparse matrix is external; output may be resident or streamed.",
      "locator_type": "SECTION",
      "locator_value": "3.1 Semi-external memory",
      "page": 3,
      "paper_id": "PAPER-1602.02864",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Tile rows are read asynchronously, multiplied against the resident dense partition, buffered locally, and output rows are written asynchronously.",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 1, Parallel execution of sparse matrix dense matrix multiplication",
      "page": 5,
      "paper_id": "PAPER-1602.02864",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Minimum memory is n*c + t*epsilon, and the sparse-input read volume is (n*c*p/M')*[E-(M-M')] under the stated divisibility and capacity assumptions.",
      "locator_type": "EQUATION",
      "locator_value": "Section 3.6, minimum-memory and IO_in expressions",
      "page": 6,
      "paper_id": "PAPER-1602.02864",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Vertical dense partitions allow both sparse and dense matrices to exceed memory; retaining more dense columns improves measured performance.",
      "locator_type": "SECTION",
      "locator_value": "5.3 SEM-SpMM with a large input dense matrix and Figure 10",
      "page": 9,
      "paper_id": "PAPER-1602.02864",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "Small vertical partitions lose data locality, while sparse-matrix and dense-input/output I/O contribute additional overhead.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 11 and Section 5.3 overhead discussion",
      "page": 10,
      "paper_id": "PAPER-1602.02864",
      "pointer_id": "SP-006"
    },
    {
      "claim_scope": "CSR-to-SCSR conversion is linear, reads CSR once, writes SCSR once, and is amortized by iterative applications.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 5.4, format-conversion paragraph following Table 2",
      "page": 11,
      "paper_id": "PAPER-1602.02864",
      "pointer_id": "SP-007"
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
    "text": "Sparse tile rows are read from SSDs, and output dense rows are streamed to SSDs when the output is not retained in memory.",
    "uncertainty": "Input-dense partitions are also read from SSDs when the full dense matrix is external."
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
      "text": "The paper does not provide a device-independent threshold at which retaining another dense column is better than caching the same bytes of sparse data once runtime overheads are included.",
      "uncertainty": "The analytical expression favors maximizing M' under its assumptions, while measured bottlenecks can shift."
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
      "text": "The method applies when RAM can hold at least one complete input-dense column and per-thread buffers while the sparse matrix remains external.",
      "uncertainty": "Performance still depends on SSD bandwidth, graph structure, and dense-column count."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-004",
        "SP-005"
      ],
      "text": "Keeping more dense columns resident reduces sparse passes and can move execution from I/O-bound toward CPU-bound behavior.",
      "uncertainty": "The transition point is hardware- and workload-dependent."
    }
  ]
}
```
