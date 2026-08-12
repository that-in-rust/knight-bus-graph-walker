# Pipeline Asynchronous Disk Reads

- Pattern ID: `PAT-PIPELINE-ASYNCHRONOUS-DISK-READS`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `D_THEORETICAL_OR_INCOMPLETE`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus exposes independent distance computation while storage reads are outstanding.",
      "Its asynchronous interface can preserve the algorithm's candidate-ordering and result semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source identifies alternating I/O and computation as a cause of SSD idle time.",
      "The source's first Pipeline variant executes distance calculations and disk reads simultaneously."
    ],
    "source_pointer_ids": [
      "SP-042",
      "SP-043"
    ],
    "text": "Knight Bus could use bounded asynchronous page reads only where query-local distance work can overlap their latency, while treating any throughput benefit as conditioned on a measured overlap window.",
    "uncertainty": "The paper does not establish an isolated overlap-only benefit for Knight Bus workloads or provide a portable admission threshold."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-043"
    ],
    "text": "Submit disk reads asynchronously and perform candidate distance calculations while those reads are outstanding so storage and computation execute simultaneously.",
    "uncertainty": "The source does not specify the overlap-only request batch size or synchronization implementation."
  },
  "confidence_rationale": {
    "assumptions": [
      "The paper's full Pipeline implementation includes the overlap variant described in Section 4.3.2.",
      "A combined Pipeline benchmark cannot isolate the contribution of overlap alone."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Section 4.3.2 and Figure 9(a) specify asynchronous I/O-compute overlap.",
      "The evaluation reports only the complete two-optimization Pipeline configuration."
    ],
    "source_pointer_ids": [
      "SP-043",
      "SP-045",
      "SP-046"
    ],
    "text": "The overlap mechanism is source-specified, but its empirical effect is not independently ablated and this campaign did not reproduce or inspect it.",
    "uncertainty": "Evidence is incomplete for overlap alone and is bounded to the paper's DiskANN setting and asynchronous I/O implementation."
  },
  "data_arrangement": {
    "assumptions": [
      "Asynchronous overlap requires request descriptors and page buffers in memory.",
      "The overlap variant does not alter the source's page-aligned disk-record layout."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Figure 9(a) depicts disk reads and compute on concurrent timelines.",
      "The source classifies Pipeline as a search-algorithm optimization rather than a disk-layout optimization."
    ],
    "source_pointer_ids": [
      "SP-043",
      "SP-047"
    ],
    "text": "The unchanged disk-resident candidate records are paired with query-local in-flight read descriptors and buffers that permit distance work on available candidates during outstanding reads.",
    "uncertainty": "Descriptor ownership, buffer layout, and queue representation are not specified."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "D_THEORETICAL_OR_INCOMPLETE",
  "fails_when": [
    {
      "assumptions": [
        "Overlap can hide latency only when useful compute and I/O intervals coexist.",
        "Neither interval is assumed to create additional independent work."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source attributes sequential idle time to alternating I/O and computation.",
        "The source's remedy is their simultaneous execution."
      ],
      "source_pointer_ids": [
        "SP-043"
      ],
      "text": "The overlap variant cannot hide latency when distance work finishes before reads remain outstanding, reads finish before useful compute begins, or dependencies force the two phases to serialize.",
      "uncertainty": "The source does not measure these overlap-window failure points separately."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-PIPELINE-ASYNCHRONOUS-DISK-READS",
  "falsifying_test": {
    "controlled_variables": [
      "graph index",
      "query set",
      "recall target",
      "read batch",
      "I/O queue depth",
      "distance-computation work",
      "cache state"
    ],
    "failure_signal": "No distance-computation interval overlaps an outstanding disk-read interval, result ordering differs from the sequential oracle, or elapsed query time does not decrease when the controlled fixture provides a positive overlap window",
    "fixture": "A deterministic proximity-search batch with two candidate-page reads and independent distance work on an already available candidate",
    "independent_oracle": "The same candidate batch executed with sequential reads and distance calculations, plus a timestamped trace of submissions, completions, compute intervals, and result order",
    "scope": "Smallest overlap-only mechanism falsifier description; no continuous queue replenishment and no G09 experiment exist"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-043"
    ],
    "text": "At least one disk read may remain outstanding while the query performs distance calculations on candidate data already available to compute.",
    "uncertainty": "Simultaneous activity does not guarantee that either resource remains continuously busy."
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
      "SP-043"
    ],
    "text": "Overlap asynchronous disk reads with distance calculations so the processor can evaluate available candidates while storage fetches other candidate records.",
    "uncertainty": "The paper presents this as the first Pipeline optimization and does not independently benchmark it."
  },
  "name": "Pipeline Asynchronous Disk Reads",
  "pattern_id": "PAT-PIPELINE-ASYNCHRONOUS-DISK-READS",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-042",
      "SP-043"
    ],
    "text": "Sequentially alternating disk I/O and distance computation leaves the SSD idle during compute phases and underuses available bandwidth.",
    "uncertainty": "The degree of underutilization depends on the device, computation cost, and concurrent workload."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-043"
    ],
    "text": "Distance scores and candidate priorities are computed from available records while other candidate records are being fetched asynchronously.",
    "uncertainty": "The source does not isolate scheduling overhead for this computation."
  },
  "related_pattern_ids": [
    "PAT-PIPELINE-ASYNC-IO-COMPUTE",
    "PAT-REPLENISH-IO-EACH-COMPLETION"
  ],
  "resident_state": {
    "assumptions": [
      "Outstanding asynchronous reads require bounded request metadata and destination buffers.",
      "Baseline PQ guidance and the candidate frontier remain resident as in the evaluated DiskANN search."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source runs distance computation concurrently with asynchronous reads.",
      "The evaluation uses a common asynchronous I/O engine and direct I/O."
    ],
    "source_pointer_ids": [
      "SP-043",
      "SP-044"
    ],
    "text": "Resident query state includes the candidate frontier, baseline PQ guidance, and bounded descriptors and buffers for the current asynchronous read batch.",
    "uncertainty": "The paper does not report overlap-specific resident bytes."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Asynchronous overlap permits disk reads and distance computation to proceed simultaneously and is intended to improve bandwidth utilization; overlap alone does not claim to change the candidate-page set.",
      "measurement_needed": "Trace page reads, bytes, outstanding-read intervals, compute intervals, bandwidth, and elapsed latency for overlap enabled and disabled on the same candidate batches.",
      "premises": [],
      "source_pointer_ids": [
        "SP-042",
        "SP-043"
      ],
      "status": "SOURCED",
      "uncertainty": "The paper does not report an overlap-only ablation or a portable utilization gain."
    },
    "persistent_storage": {
      "assumptions": [
        "Asynchronous overlap changes query execution but not record encoding.",
        "No durable scheduler trace is required."
      ],
      "expression": "The overlap variant reuses the existing disk-resident graph index without requiring an additional persistent index artifact.",
      "measurement_needed": "Confirm index-byte identity between synchronous and overlap-only builds and inspect for persisted runtime metadata.",
      "premises": [
        "The source classifies Pipeline under search-algorithm optimization.",
        "The source states that query-time techniques add no index-construction work over the baseline."
      ],
      "source_pointer_ids": [
        "SP-043",
        "SP-047"
      ],
      "status": "DERIVED",
      "uncertainty": "An implementation could persist tuning state even though the paper does not describe it."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Pipeline is treated as a query-time optimization among methods that incur no extra index-construction overhead over the baseline.",
      "measurement_needed": "Measure asynchronous-engine initialization separately from index construction and query execution.",
      "premises": [],
      "source_pointer_ids": [
        "SP-047"
      ],
      "status": "SOURCED",
      "uncertainty": "The paper does not isolate initialization for the overlap-only variant."
    },
    "ram": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure candidate-frontier, asynchronous-request, completion, page-buffer, and I/O-engine memory for the overlap-only configuration.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Overlap-specific RAM is not reported separately from the baseline and complete Pipeline runtime."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak in-flight descriptors and page buffers for one asynchronous read batch.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not state an overlap-only bound for transient requests or buffers."
    }
  },
  "source_domain": "disk-resident graph approximate-nearest-neighbor search",
  "source_paper_ids": [
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "Pipeline bandwidth benefit and I/O-waste cost classification",
      "locator_type": "TABLE",
      "locator_value": "Table 1, Pipeline row",
      "page": 4,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-042"
    },
    {
      "claim_scope": "First Pipeline optimization: asynchronous disk-read and distance-computation overlap",
      "locator_type": "FIGURE",
      "locator_value": "Figure 9(a) and Section 4.3.2 first optimization",
      "page": 6,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-043"
    },
    {
      "claim_scope": "Asynchronous I/O engine, direct I/O, worker count, and fairness controls",
      "locator_type": "SECTION",
      "locator_value": "Section 5.1, Implementation and parameters",
      "page": 7,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-044"
    },
    {
      "claim_scope": "Complete Pipeline configuration as a standalone PQ-baseline ablation",
      "locator_type": "SECTION",
      "locator_value": "Section 6 and Section 6.1 opening paragraphs",
      "page": 8,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-045"
    },
    {
      "claim_scope": "Complete Pipeline configuration's aggregate disk and performance behavior",
      "locator_type": "PARAGRAPH",
      "locator_value": "Finding 5, preceding Pipeline paragraph, and Table 5",
      "page": 9,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-046"
    },
    {
      "claim_scope": "Query-time techniques add no baseline index-construction overhead",
      "locator_type": "SECTION",
      "locator_value": "Section 6.2, Graph Index Construction Overhead",
      "page": 9,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-047"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-043"
    ],
    "text": "Candidate graph records are fetched asynchronously from SSD while the query computes distances using candidate data already available.",
    "uncertainty": "The read set remains query- and frontier-dependent."
  },
  "unknown_when": [
    {
      "assumptions": [
        "The paper's reported Pipeline curves combine both progressive optimizations.",
        "No unreported overlap-only ablation is available in the reviewed local source."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "Section 4.3.2 distinguishes overlap as the first optimization.",
        "Sections 6 and 7 evaluate Pipeline only as the complete configuration."
      ],
      "source_pointer_ids": [
        "SP-043",
        "SP-045",
        "SP-046"
      ],
      "text": "The overlap-only effect on page reads, bandwidth, latency, throughput, and recall remains unknown from this paper.",
      "uncertainty": "The complete Pipeline results cannot identify how much behavior belongs to overlap alone."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-043"
      ],
      "text": "The overlap variant applies when distance calculations and disk reads can execute simultaneously, allowing useful computation during storage latency.",
      "uncertainty": "The source does not quantify the minimum useful overlap interval."
    }
  ]
}
```
