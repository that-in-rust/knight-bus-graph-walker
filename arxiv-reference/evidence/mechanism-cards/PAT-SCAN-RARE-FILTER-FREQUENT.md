# Scan Rare Filter Frequent

- Pattern ID: `PAT-SCAN-RARE-FILTER-FREQUENT`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Label counts are available before execution."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source separates rare exact postings from frequent approximate membership.",
      "The source reports nonzero memory for probabilistic filters."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A007 can quote rare-posting page reads, resident Bloom bytes, merged-ID peak bytes, and deferred verification separately instead of treating every predicate branch as an exact scan.",
    "uncertainty": "Attribute correlation and merged-list peaks require measurement."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Fetch rare postings first, merge their IDs, then check candidate IDs against Bloom filters for frequent labels and defer exact constraints to final verification.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "The reported implementation matches the described selector behavior."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source explains both the hybrid algorithm and filter memory measurements."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "The data structures and measured memory are reported clearly, but thresholds and gains remain workload-specific and unreproduced.",
    "uncertainty": "No code inspection or rerun occurred."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Sorted per-label inverted indexes live on SSD; label offsets and counts plus one lightweight Bloom filter per vector live in memory.",
    "uncertainty": "NONE"
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
        "SP-002"
      ],
      "text": "Bloom collisions become costly when vectors contain many labels or when rare postings do not reduce the candidate set.",
      "uncertainty": "The failure is a performance reversal, not loss of exact final validity."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-SCAN-RARE-FILTER-FREQUENT",
  "falsifying_test": {
    "controlled_variables": [
      "Posting sizes, label correlation, Bloom bits, Boolean operator, and cache state."
    ],
    "failure_signal": "Any exact match is excluded from the superset or frequent-label posting pages are read during the approximate in-filter stage.",
    "fixture": "A labeled graph with one rare label, one frequent label, correlated-label and independent-label variants, and deliberate Bloom collisions.",
    "independent_oracle": "Exact Boolean evaluation over original label sets.",
    "scope": "Superset correctness and intended access separation."
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "The merged rare-label IDs and Bloom checks form a superset of exact matches; skipped frequent constraints are enforced during final verification.",
    "uncertainty": "NONE"
  },
  "knight_bus_algorithm_families": [
    {
      "assumptions": [
        "Knight Bus vertices carry exact categorical labels and expose final verification."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source uses rare exact postings and frequent approximate tests to construct a safe superset."
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "Predicate-filtered path traversal and filtered node-similarity or kNN workloads with categorical labels are plausible Knight Bus matches.",
      "uncertainty": "Graph-traversal benefit outside ANNS remains unmeasured."
    }
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Read only low-selectivity label postings, merge them in memory according to the Boolean operator, and use per-vector Bloom filters for remaining high-selectivity labels.",
    "uncertainty": "Bloom collisions create false positives."
  },
  "name": "Scan Rare Filter Frequent",
  "pattern_id": "PAT-SCAN-RARE-FILTER-FREQUENT",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Scanning every queried label index wastes I/O for frequent labels, while relying only on Bloom filters increases collisions for label-rich vectors.",
    "uncertainty": "NONE"
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Exact frequent-label membership is recomputed from original attributes during final candidate verification.",
    "uncertainty": "NONE"
  },
  "related_pattern_ids": [
    "PAT-ROUTE-FILTERS-BY-COST"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Label counts, posting offsets, per-vector Bloom filters, and the current merged rare-label ID list are resident.",
    "uncertainty": "The merged-list peak depends on selectivity."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "SSD reads are limited to selected rare postings in the approximate pass; frequent-label checks use memory-resident Bloom filters.",
      "measurement_needed": "Measure posting pages, verification pages, and false-positive-induced graph reads separately.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "SOURCED",
      "uncertainty": "Exact verification still reads final attributes and Bloom false positives add work."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "Persistent state includes sorted per-label vector-ID postings and exact row attributes used later for verification.",
      "measurement_needed": "Measure posting and exact-attribute bytes on disk.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "SOURCED",
      "uncertainty": "Compression and duplication amplification are workload-dependent."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Build sorted inverted postings, store label offsets and counts, and construct a Bloom filter for each vector.",
      "measurement_needed": "Measure construction and mutation cost per posting and Bloom filter.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "SOURCED",
      "uncertainty": "Build and update times are not reported."
    },
    "ram": {
      "assumptions": [],
      "expression": "Per-vector Bloom filters plus label counts, offsets, and the query-specific merged rare-label list.",
      "measurement_needed": "Measure peak filter and merged-posting bytes for the declared label distribution.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "The paper reports filter sizes for its datasets, not a general merged-list bound."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure query-specific merge buffers at rare-label selectivity extremes.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The peak merged-ID list and Boolean merge scratch are not bounded."
    }
  },
  "source_domain": "hybrid SSD and memory label filtering",
  "source_paper_ids": [
    "PAPER-2605.17992"
  ],
  "source_pointers": [
    {
      "claim_scope": "Rare-label SSD index scans, in-memory ID merge, Bloom fallback for frequent labels, and deferred exact checks.",
      "locator_type": "SECTION",
      "locator_value": "Section 4.3.1, Label Filtering",
      "page": 7,
      "paper_id": "PAPER-2605.17992",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Measured memory usage of per-vector probabilistic label filters.",
      "locator_type": "TABLE",
      "locator_value": "Table 3 and Section 5.4, Memory usage",
      "page": 12,
      "paper_id": "PAPER-2605.17992",
      "pointer_id": "SP-002"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Only selected rare-label posting ranges are fetched from SSD for the approximate pass.",
    "uncertainty": "Posting compression and cache state are not specified."
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
      "text": "The method assumes label-independence for selectivity and precision estimates, which may not hold for correlated attributes.",
      "uncertainty": "Correlation-aware estimation is not evaluated."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "Label frequencies are skewed enough that rare postings sharply reduce the candidate superset while frequent postings would be expensive to scan.",
      "uncertainty": "The source uses selectivity heuristics rather than a universal cutoff."
    }
  ]
}
```
