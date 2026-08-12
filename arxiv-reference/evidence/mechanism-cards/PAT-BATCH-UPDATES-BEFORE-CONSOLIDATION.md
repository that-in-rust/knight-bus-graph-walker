# Batch Updates Before Consolidation

- Pattern ID: `PAT-BATCH-UPDATES-BEFORE-CONSOLIDATION`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus supports a query-visible delta over a stable disk index",
      "The runtime can bound update rate and schedule consolidation before the delta cap"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-BATCH-UPDATES-BEFORE-CONSOLIDATION: updates remain in a memory component until batched merge",
      "PAT-BATCH-UPDATES-BEFORE-CONSOLIDATION: out-of-place updates reduce foreground latency but add multi-component and consolidation costs"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A dynamic Knight Bus similarity index would need admission limits for outstanding delta bytes, dual-component query state, merge scratch, and coexistence of old/new persistent indexes; it should trigger consolidation or refuse updates before those terms exceed budget.",
    "uncertainty": "The paper does not bound crash recovery, merge amplification, or peak simultaneous persistent storage."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Foreground writes append to or modify the memory component; queries consult both components; background work periodically writes a consolidated disk index.",
    "uncertainty": "Merge trigger and scheduling policy are not unified across evaluated methods."
  },
  "confidence_rationale": {
    "assumptions": [
      "Compared methods preserve equivalent freshness and recall settings for the reported measurements"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Section 3.5 defines memory-first update consolidation",
      "Section 4.3.5 reports source update/query measurements"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-003"
    ],
    "text": "Confidence is moderate because the source defines the out-of-place mechanism and compares query throughput, update latency, and merge time, but it leaves peak resource terms and recovery semantics unbounded and was not reproduced here.",
    "uncertainty": "No independent rerun, implementation inspection, failure injection, or recovery analysis occurred."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "A stable disk-resident base graph coexists with a memory-resident update component or overlay and merge/compaction state.",
    "uncertainty": "The paper surveys more than one concrete overlay representation."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "text": "Query throughput dominates and the extra overlay lookup plus consolidation state costs more than direct in-place maintenance; the source measured higher sustained query throughput for in-place updates.",
      "uncertainty": "The measured ratio is fixture-specific and does not establish all query-heavy workloads."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-BATCH-UPDATES-BEFORE-CONSOLIDATION",
  "falsifying_test": {
    "controlled_variables": [
      "update sequence",
      "query set",
      "delta cap",
      "merge trigger",
      "storage device",
      "query/update thread counts"
    ],
    "failure_signal": "Queries miss visible updates, delta or merge state exceeds declared caps, or consolidation changes the logical result beyond the ANN recall contract",
    "fixture": "A small disk proximity graph receiving an interleaved sequence of insertions, deletions, and exact-oracle queries through one forced merge",
    "independent_oracle": "A freshly rebuilt index from the complete logical dataset after each update checkpoint",
    "scope": "Smallest update-visibility/resource-bound falsifier only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "New updates remain query-visible in a memory-resident delta or overlay until a background merge integrates them into the disk-resident base index.",
    "uncertainty": "The exact freshness and deletion semantics vary among out-of-place implementations."
  },
  "knight_bus_algorithm_families": [
    "NODESIMILARITY_KNN"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Absorb many small insertions and deletions into memory, serve queries across base plus delta, and periodically batch-merge or compact the accumulated updates into the disk graph.",
    "uncertainty": "Background consolidation can be costly and introduces a multi-component index state."
  },
  "name": "Batch Updates Before Consolidation",
  "pattern_id": "PAT-BATCH-UPDATES-BEFORE-CONSOLIDATION",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Applying every graph update directly to disk can touch multiple random pages and interfere with foreground search, producing high update latency.",
    "uncertainty": "Direct-update cost depends on graph-maintenance scope and storage behavior."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Graph connectivity and disk layout affected by accumulated updates are rebuilt or compacted during merge rather than after every update.",
    "uncertainty": "Merge implementation and deletion repair differ across systems."
  },
  "related_pattern_ids": [
    "PAT-PLACE-SCALE-GROWING-STATE"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "The update delta or overlay, its searchable graph/index state, query working state, and merge bookkeeping remain resident until consolidation.",
    "uncertainty": "The maximum delta size is policy-dependent and not bounded in the paper."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Out-of-place updates amortize small changes through batched disk writes and isolate foreground reads, but incur background merge/compaction I/O.",
      "measurement_needed": "Measure foreground and background bytes, write amplification, merge bandwidth, and query interference across update rates.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "The source reports latency and merge-time comparisons rather than a general byte-amplification formula."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure base, delta/log, new merged index, tombstones, and rollback/checkpoint bytes over a full merge.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "During consolidation, old base, new base, logs, and overlay snapshots may coexist, but the paper does not bound this peak."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure initial base build and incremental delta-index initialization separately.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Initial base-index construction is not separated from the update mechanism in a reusable expression."
    },
    "ram": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure delta/overlay bytes versus outstanding updates and enforce a declared merge or refusal threshold.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not bound the maximum memory-resident delta or overlay before consolidation."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak RAM and disk scratch while queries and consolidation overlap.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Merge scratch and concurrent-query buffers are not bounded."
    }
  },
  "source_domain": "dynamic disk-resident graph ANN indexes",
  "source_paper_ids": [
    "PAPER-2603.01779"
  ],
  "source_pointers": [
    {
      "claim_scope": "In-memory accumulation, LSM-style merge/overlay, benefits, and multi-component cost",
      "locator_type": "SECTION",
      "locator_value": "Section 3.5, Out-of-Place Update",
      "page": 6,
      "paper_id": "PAPER-2603.01779",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Source throughput/update-latency/merge comparison of in-place and out-of-place updates",
      "locator_type": "FIGURE",
      "locator_value": "Figures 15-16 and Section 4.3.5",
      "page": 11,
      "paper_id": "PAPER-2603.01779",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Read/write-ratio update-technique guidance",
      "locator_type": "SECTION",
      "locator_value": "Section 5.1 item 5 and Figure 17",
      "page": 12,
      "paper_id": "PAPER-2603.01779",
      "pointer_id": "SP-003"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Base graph pages are read for queries while accumulated updates are eventually streamed or rewritten into a merged disk graph during consolidation.",
    "uncertainty": "Write amplification is not expressed as one source-wide formula."
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
        "SP-001",
        "SP-003"
      ],
      "text": "The source does not establish a universal merge trigger, bounded stale/delta size, crash-recovery cost, or read/write crossover.",
      "uncertainty": "These controls are system- and workload-specific."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "text": "The workload is write-heavy or balanced enough that lower foreground update latency outweighs multi-component queries and background consolidation.",
      "uncertainty": "The source's final wording varies between balanced/read-heavy guidance and measured update-heavy benefit; the strongest supported boundary is lower update latency under update pressure."
    }
  ]
}
```
