# Prefetch Displaced Search State

- Pattern ID: `PAT-PREFETCH-DISPLACED-SEARCH-STATE`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus can instrument cache and memory-hierarchy effects without changing the foreground schedule materially."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source specifies three cache targets and two trigger points.",
      "The source evaluates the component as a separately enabled cumulative ablation stage."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "A007 should account for cache restoration as its own optional admission component, naming target bytes and trigger points while requiring measured cache occupancy, memory traffic, and useful-request ratio before crediting latency headroom.",
    "uncertainty": "The paper does not provide transferable byte, bandwidth, or cache-residency coefficients."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Prefetch the three search structures immediately after an update execution and repeat the prefetch after the outstanding search I/O completes, before resumed distance computation consumes them.",
    "uncertainty": "Available lead time differs between cache hits, short stalls, and longer device completions."
  },
  "confidence_rationale": {
    "assumptions": [
      "The evaluated implementation's final ablation step corresponds to the described cache requests."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Section 4.4 describes an independently controllable schedule and Figure 9 reports a distinct cumulative enablement step."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-003"
    ],
    "text": "The source identifies concrete cache targets and trigger points and separately enables the mechanism in Figure 9, but this campaign did not inspect instructions, reproduce the ablation, or measure isolated cache and bandwidth effects.",
    "uncertainty": "No independent implementation, hardware-counter, or reproduction evidence is available."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "The prefetched set consists of three existing search structures: the current query vector, its product-quantization distance lookup table, and visited-set metadata.",
    "uncertainty": "Object layout, cache-line coverage, and metadata size are not reported."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [
        "A request that does not change residency before use cannot avoid the corresponding demand miss."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source benefit depends on making named search structures warm before distance computation resumes."
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "The mechanism provides no useful cache restoration when target lines are already resident, are evicted again before use, or arrive after resumed search consumes them.",
      "uncertainty": "The source does not report a timing or cache-pressure breakpoint for these cases."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-PREFETCH-DISPLACED-SEARCH-STATE",
  "falsifying_test": {
    "controlled_variables": [
      "Target addresses and sizes",
      "Update working set",
      "Initial cache state",
      "Trigger order",
      "Delay from request to use",
      "Search input and exact result oracle"
    ],
    "failure_signal": "Either trigger omits a named target, the prefetched addresses differ from the declared search state, the exact search result changes, or controlled cold-target demand misses are not reduced before use.",
    "fixture": "A deterministic search step with fixed query-vector, distance-table, and visited-set addresses, preceded by an update slice that displaces those lines from a controlled cache state.",
    "independent_oracle": "An instrumented trigger-and-address trace paired with the exact search result from the same step executed without cache-state restoration.",
    "scope": "Target selection, trigger placement, semantic neutrality, and cache-restoration behavior only; no G09 experiment is created."
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "The query vector, product-quantization distance lookup table, and visited-set metadata are prefetched after each update slice and again after I/O completion so they can be warm before search distance computation resumes.",
    "uncertainty": "A prefetch request does not guarantee residency at the later use point on every processor."
  },
  "knight_bus_algorithm_families": [
    {
      "assumptions": [
        "A Knight Bus kernel exposes stable addresses for its next foreground working set and predictable resumption boundaries."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source restores a named search working set before foreground distance computation resumes."
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "Disk-resident graph approximate-nearest-neighbor search and other iterative graph kernels with a small reusable foreground working set separated by background compute slices are candidate Knight Bus families.",
      "uncertainty": "Only disk-based graph approximate-nearest-neighbor search is source-evaluated."
    }
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Issue cache prefetches for the query vector, distance lookup table, and visited-set metadata at both post-update and post-I/O boundaries, restoring search-critical working state before distance calculations restart.",
    "uncertainty": "The source does not specify instruction selection, cache level, or prefetch distance."
  },
  "name": "Prefetch Displaced Search State",
  "pattern_id": "PAT-PREFETCH-DISPLACED-SEARCH-STATE",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Executing update computation during a search stall can displace search-critical data from cache, increasing resumed search latency and forcing the co-execution controller to admit less update work.",
    "uncertainty": "The amount of displacement depends on update working sets, cache hierarchy, and backend behavior."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Search-critical cache warmth is re-established by repeating the same targeted requests after update execution and after I/O completion.",
    "uncertainty": "Cache replacement between request and use can undo the intended state."
  },
  "related_pattern_ids": [
    "PAT-TUNE-IDLE-WINDOW-UTILIZATION"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "The query vector, distance lookup table, and visited-set metadata remain the search-side working state whose cache residency is restored at co-execution boundaries.",
    "uncertainty": "The source does not quantify how many cache lines remain resident until use."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure device reads, last-level-cache misses, memory-controller bytes, and useful versus unused requests with identical query and update traces.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not isolate storage-device or memory-hierarchy traffic caused by the cache requests."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Verify whether the implementation persists any target-region metadata and measure its durable bytes.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "No persistent format or durable metadata attributable to the mechanism is specified."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Determine whether target addresses require setup beyond existing search initialization and measure that setup if present.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "No separate preprocessing phase for discovering or registering target address ranges is specified."
    },
    "ram": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure target bytes, occupied cache lines, and whole-process RSS with this mechanism independently disabled and enabled.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not isolate incremental RAM or cache-capacity occupancy attributable to the mechanism."
    },
    "temporary_storage": {
      "assumptions": [
        "At least some requested lines enter a finite processor cache before use."
      ],
      "expression": "Temporary state consists of cache occupancy for lines from the query vector, distance lookup table, and visited-set metadata until those lines are consumed or evicted.",
      "measurement_needed": "Measure target cache-line count, residency at use, eviction rate, and occupancy lifetime.",
      "premises": [
        "The source requests the three structures in advance to make them warm before resumed computation."
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "DERIVED",
      "uncertainty": "Cache level, replacement, line count, and overlap with already-resident state are unknown."
    }
  },
  "source_domain": "CPU-cache recovery after update work inside disk-based graph-search I/O stalls",
  "source_paper_ids": [
    "PAPER-2605.19335"
  ],
  "source_pointers": [
    {
      "claim_scope": "Prefetch targets, two trigger points, intended cache-warming effect, and interaction with update throughput.",
      "locator_type": "SECTION",
      "locator_value": "Section 4.4, Cache prefetching",
      "page": 7,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Reported tail-latency reductions attributed by the authors to cache warming after co-execution episodes.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 6.2, search-latency discussion",
      "page": 9,
      "paper_id": "PAPER-2605.19335",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Separately enabled cumulative ablation stage and its reported latency-throughput effect after adaptive tuning.",
      "locator_type": "FIGURE",
      "locator_value": "Figure 9 and Section 6.4, +Prefetch stage",
      "page": 10,
      "paper_id": "PAPER-2605.19335",
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
    "text": "Cache lines belonging to the three target structures are requested through the memory hierarchy at each post-update and post-I/O trigger.",
    "uncertainty": "The source does not report memory-bandwidth traffic or useful-versus-wasted requests."
  },
  "unknown_when": [
    {
      "assumptions": [
        "Shared caches or memory channels may be contested by work outside the evaluated process."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source names the cache targets and reports a cumulative performance effect but does not report isolated bandwidth, cache-occupancy, or multi-tenant measurements."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "text": "Behavior is unknown when repeated cache requests compete with other tenants or consume enough memory bandwidth and cache capacity to displace equally valuable foreground state.",
      "uncertainty": "No source-established contention boundary is available."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-003"
      ],
      "text": "In the evaluated cumulative ablation, enabling the cache-warming component after adaptive tuning recovered update speed while keeping search latency near the baseline.",
      "uncertainty": "The result is source-reported for one cumulative configuration and does not isolate every hardware counter."
    }
  ]
}
```
