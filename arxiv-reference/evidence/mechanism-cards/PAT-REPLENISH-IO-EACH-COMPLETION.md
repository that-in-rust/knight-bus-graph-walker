# Replenish IO Each Completion

- Pattern ID: `PAT-REPLENISH-IO-EACH-COMPLETION`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus can process individual I/O completions without a global batch barrier.",
      "Its scheduler can bound speculative work and observe whether concurrent queries already saturate storage."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source immediately issues another request after each individual completion to remove batch delays.",
      "The source reports that this Pipeline behavior can widen the frontier through speculative reads under concurrency."
    ],
    "source_pointer_ids": [
      "SP-049",
      "SP-052",
      "SP-055"
    ],
    "text": "Knight Bus could use completion-triggered I/O replenishment only as a bounded scheduler variant whose speculative-read budget and device-exclusivity condition are explicit.",
    "uncertainty": "The paper does not provide a portable saturation threshold, cancellation policy, or Knight Bus correctness argument."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-049"
    ],
    "text": "When any individual disk operation completes, immediately issue another eligible I/O request instead of waiting for all operations in the current batch to finish.",
    "uncertainty": "The source does not specify request-selection, queue-depth, or cancellation rules."
  },
  "confidence_rationale": {
    "assumptions": [
      "The paper's complete Pipeline implementation includes the continuous replenishment variant specified in Section 4.3.2.",
      "The paper's causal discussion of speculative reads primarily concerns requests issued before candidate confirmation."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Section 4.3.2 and Figure 9(b) define immediate per-completion request issuance.",
      "The paper benchmarks the complete Pipeline configuration and attributes its adverse behavior to speculative reads under high concurrency."
    ],
    "source_pointer_ids": [
      "SP-049",
      "SP-051",
      "SP-052",
      "SP-055"
    ],
    "text": "The mechanism has a precise source description and paper-benchmark evidence for the complete Pipeline configuration, but no isolated replenishment-only ablation or campaign reproduction.",
    "uncertainty": "Grade C is limited to the paper's aggregate Pipeline results, SSD, worker configuration, datasets, and recall targets."
  },
  "data_arrangement": {
    "assumptions": [
      "Completion-triggered scheduling requires individually addressable completion records and pending candidates.",
      "The mechanism changes query scheduling rather than the page-aligned graph layout."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reacts to completion of individual disk operations.",
      "The source depicts a continuous I/O timeline over the existing graph search."
    ],
    "source_pointer_ids": [
      "SP-049"
    ],
    "text": "The unchanged disk index is paired with a pending candidate-request set, individually tracked in-flight reads, and completion records that can trigger one-for-one queue replenishment.",
    "uncertainty": "The source does not define descriptor representation, ownership, or buffer allocation."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-052",
        "SP-055"
      ],
      "text": "When concurrent queries already saturate the SSD, early replacement requests widen the exploration frontier, add speculative reads and contention, and can reduce end-to-end performance.",
      "uncertainty": "The source does not give a device-independent concurrency or saturation threshold."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-REPLENISH-IO-EACH-COMPLETION",
  "falsifying_test": {
    "controlled_variables": [
      "graph index",
      "query set",
      "recall target",
      "beam width",
      "I/O queue depth",
      "worker count",
      "cache state"
    ],
    "failure_signal": "A completed operation does not trigger an eligible replacement until the batch drains, storage becomes idle despite pending eligible requests, or speculative reads and latency exceed the batch-barrier oracle in the source-claimed device-exclusive case",
    "fixture": "A deterministic search frontier with a multi-request batch whose reads complete at staggered times and whose pending frontier remains nonempty",
    "independent_oracle": "Timestamped request and completion traces from the same search using a batch barrier, with identical candidate ordering and result checks",
    "scope": "Smallest completion-replenishment mechanism falsifier description only; asynchronous overlap is held constant and no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-049"
    ],
    "text": "An individual completed I/O operation can release capacity for an immediate replacement request without waiting for the rest of its request batch.",
    "uncertainty": "Near-continuous activity still depends on an eligible pending request and available queue capacity."
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
      "SP-049"
    ],
    "text": "Remove batch processing delays by issuing a new disk request immediately whenever one individual I/O operation completes, maintaining a continuously replenished storage pipeline.",
    "uncertainty": "Requests can be issued before candidate ordering is final and may therefore become speculative."
  },
  "name": "Replenish IO Each Completion",
  "pattern_id": "PAT-REPLENISH-IO-EACH-COMPLETION",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-049"
    ],
    "text": "Batch-oriented search delays new disk requests until earlier requests finish as a group, creating storage-idle gaps even when pending candidates remain.",
    "uncertainty": "The magnitude of batch delay depends on completion skew and queue occupancy."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-049",
      "SP-052"
    ],
    "text": "Each individual completion exposes a queue slot and causes the scheduler to select and issue another pending candidate read, potentially before all candidate priorities are confirmed.",
    "uncertainty": "The source does not specify the eligibility function or stale-request cancellation behavior."
  },
  "related_pattern_ids": [
    "PAT-PIPELINE-ASYNCHRONOUS-DISK-READS"
  ],
  "resident_state": {
    "assumptions": [
      "Immediate replenishment requires a pending-request set and per-operation completion tracking.",
      "In-flight records require bounded destination buffers until consumed or discarded."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source issues replacement requests on individual completions.",
      "The source reports speculative reads when requests precede candidate confirmation."
    ],
    "source_pointer_ids": [
      "SP-049",
      "SP-052"
    ],
    "text": "Resident scheduler state includes pending candidates, individually tracked in-flight requests, completion metadata, page buffers, and enough priority state to choose each replacement read.",
    "uncertainty": "The paper does not quantify these replenishment-specific bytes."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Immediate replacement of each completed operation is intended to maintain near-continuous disk activity and high bandwidth utilization, but the evaluated complete Pipeline can increase read operations through speculation and reduce performance when concurrency already saturates storage.",
      "measurement_needed": "Measure queue occupancy, idle intervals, submitted and completed reads, read-but-unexplored pages, bandwidth, IOPS, and latency at fixed recall and concurrency.",
      "premises": [],
      "source_pointer_ids": [
        "SP-049",
        "SP-052",
        "SP-055"
      ],
      "status": "SOURCED",
      "uncertainty": "The benchmark combines replenishment with overlap and does not isolate the replenishment-only effect."
    },
    "persistent_storage": {
      "assumptions": [
        "Completion-triggered replenishment changes query scheduling but not index encoding.",
        "Pending and completion state is rebuilt at runtime rather than persisted."
      ],
      "expression": "The mechanism reuses the existing disk-resident graph index and does not require a separate persistent index artifact.",
      "measurement_needed": "Confirm index-byte identity between batch-barrier and completion-replenished configurations and inspect for persisted tuning state.",
      "premises": [
        "The source classifies Pipeline as a search-algorithm optimization.",
        "The source states that query-time techniques add no index-construction work over the baseline."
      ],
      "source_pointer_ids": [
        "SP-049",
        "SP-053"
      ],
      "status": "DERIVED",
      "uncertainty": "An implementation could persist scheduler tuning even though the paper does not describe it."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Pipeline is treated as a query-time optimization among methods that incur no extra index-construction overhead over the baseline.",
      "measurement_needed": "Measure queue and asynchronous-engine initialization separately from index construction and query execution.",
      "premises": [],
      "source_pointer_ids": [
        "SP-053"
      ],
      "status": "SOURCED",
      "uncertainty": "The paper does not isolate replenishment scheduler initialization."
    },
    "ram": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure pending-candidate, in-flight-request, completion, page-buffer, and priority-state memory across queue depths.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Replenishment-specific RAM is not reported separately from the complete Pipeline runtime."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak pending, in-flight, completed, speculative, and cancellation-state bytes per query.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not state a queue-depth or transient-storage bound for continuous replenishment."
    }
  },
  "source_domain": "disk-resident graph approximate-nearest-neighbor search",
  "source_paper_ids": [
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "Second Pipeline optimization: immediate request issuance on individual completion and removal of batch delay",
      "locator_type": "FIGURE",
      "locator_value": "Figure 9(b) and Section 4.3.2 second optimization",
      "page": 6,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-049"
    },
    {
      "claim_scope": "Asynchronous I/O engine, direct I/O, worker count, and fairness controls",
      "locator_type": "SECTION",
      "locator_value": "Section 5.1, Implementation and parameters",
      "page": 7,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-050"
    },
    {
      "claim_scope": "Complete Pipeline configuration as a standalone PQ-baseline ablation",
      "locator_type": "SECTION",
      "locator_value": "Section 6 and Section 6.1 opening paragraphs",
      "page": 8,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-051"
    },
    {
      "claim_scope": "Speculative reads, added I/O, aggregate disk metrics, and counterproductive behavior",
      "locator_type": "PARAGRAPH",
      "locator_value": "Finding 5, preceding Pipeline paragraph, and Table 5",
      "page": 9,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-052"
    },
    {
      "claim_scope": "Query-time techniques add no baseline index-construction overhead",
      "locator_type": "SECTION",
      "locator_value": "Section 6.2, Graph Index Construction Overhead",
      "page": 9,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-053"
    },
    {
      "claim_scope": "Complete Pipe plus DynamicWidth combination definition",
      "locator_type": "SECTION",
      "locator_value": "Section 7.1.1, Combination Design",
      "page": 10,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-054"
    },
    {
      "claim_scope": "Speculative-read concurrency boundary and device-exclusive recommendation",
      "locator_type": "PARAGRAPH",
      "locator_value": "Finding 9 and preceding Pipeline plus DynamicWidth paragraph",
      "page": 11,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-055"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-049",
      "SP-052"
    ],
    "text": "Candidate graph pages are continuously requested from SSD as individual operations complete, including pages issued before candidate confirmation that may never be explored.",
    "uncertainty": "The speculative fraction depends on frontier order, queue depth, completion order, and concurrency."
  },
  "unknown_when": [
    {
      "assumptions": [
        "The reviewed benchmark evaluates the full two-optimization Pipeline rather than replenishment alone.",
        "No portable scheduler threshold is supplied elsewhere in the local paper."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source reports harm when high concurrency already saturates storage.",
        "The source recommends Pipeline only for single-thread, device-exclusive cases."
      ],
      "source_pointer_ids": [
        "SP-052",
        "SP-055"
      ],
      "text": "The isolated replenishment effect and the exact queue depth, worker count, completion skew, and device-utilization threshold at which it changes from beneficial to harmful remain unknown.",
      "uncertainty": "The crossover can shift with device behavior, computation cost, candidate ordering, and speculative-read cancellation."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-049",
        "SP-055"
      ],
      "text": "Completion-triggered replenishment is intended to keep storage active when pending candidates exist and is recommended only for single-thread, device-exclusive cases with spare device capacity.",
      "uncertainty": "Speculative-read cost must remain below the benefit of removing batch idle gaps."
    }
  ]
}
```
