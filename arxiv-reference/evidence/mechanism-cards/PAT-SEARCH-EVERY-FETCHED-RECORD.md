# Search Every Fetched Record

- Pattern ID: `PAT-SEARCH-EVERY-FETCHED-RECORD`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus block records can be evaluated without changing result semantics.",
      "The runtime can cap or estimate candidate growth."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source exploits records already transferred by a page read.",
      "The source observes both synergy and compute-induced degradation."
    ],
    "source_pointer_ids": [
      "SP-017",
      "SP-018",
      "SP-020"
    ],
    "text": "A Knight Bus streamed-block plan could quote extra per-block computation in exchange for avoiding separate fetches, but only when block occupancy and candidate amplification are included in admission.",
    "uncertainty": "No source evidence covers exact BFS or WCC block-wide candidate evaluation."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-017"
    ],
    "text": "Page evaluation occurs immediately after a traversal read and before the search continues from the resulting candidate queue.",
    "uncertainty": "The source does not specify a hard cap on candidates accepted per page."
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
      "SP-018",
      "SP-019",
      "SP-020"
    ],
    "text": "The paper describes and ablates page search, including negative standalone results and a positive paired result; this campaign did not reproduce the implementation.",
    "uncertainty": "Grade C is limited to the source's ANN workload and SSD setup."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-017",
      "SP-019",
      "SP-020"
    ],
    "text": "The mechanism consumes fixed-size pages containing multiple graph records and is most effective when page layout places useful candidates together.",
    "uncertainty": "It does not require shuffle, but the paper finds the paired arrangement stronger."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-018",
        "SP-020"
      ],
      "text": "The mechanism loses its locality benefit when a page contains one record, and can reduce throughput when evaluating all records adds enough computation to idle the SSD.",
      "uncertainty": "The crossover depends on record type, page size, and hardware."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-SEARCH-EVERY-FETCHED-RECORD",
  "falsifying_test": {
    "controlled_variables": [
      "query set",
      "logical graph",
      "page bytes",
      "beam parameters",
      "cache state",
      "concurrency"
    ],
    "failure_signal": "Full-page evaluation changes answers, increases pages read, or increases total latency and peak queue state beyond the declared contract",
    "fixture": "A page-backed search index with identical logical records arranged into pages containing one, several unrelated, or several useful candidates",
    "independent_oracle": "Baseline best-first search at the same recall target",
    "scope": "Smallest schedule and resource falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-017"
    ],
    "text": "Every additional candidate evaluated by page search comes from a page already fetched for normal traversal, so candidate expansion adds no page solely for that record.",
    "uncertainty": "Later expansion of a newly discovered candidate may still cause additional reads."
  },
  "knight_bus_algorithm_families": [
    "BEST_FIRST_GRAPH_SEARCH",
    "APPROXIMATE_NEAREST_NEIGHBOR",
    "BOUNDED_PATH_SEARCH"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-017",
      "SP-019"
    ],
    "text": "After fetching a page, compute distances for every record on it and insert promising co-located records into the search candidate set.",
    "uncertainty": "Effectiveness depends on page occupancy and whether co-located records are useful."
  },
  "name": "Search Every Fetched Record",
  "pattern_id": "PAT-SEARCH-EVERY-FETCHED-RECORD",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-017"
    ],
    "text": "A graph search can pay the latency of a page read while evaluating only the requested neighbor record and ignoring other records already delivered in that page.",
    "uncertainty": "The source assumes distance computation is much cheaper than SSD access."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-017",
      "SP-018"
    ],
    "text": "Distances are computed for co-resident records that baseline traversal would have fetched but not evaluated.",
    "uncertainty": "Extra computation can dominate when many inexpensive records share a page."
  },
  "related_pattern_ids": [
    "PAT-COLOCATE-NEIGHBORS-WITHIN-PAGES"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-017"
    ],
    "text": "The normal best-first candidate queue is enlarged with useful records evaluated from fetched pages.",
    "uncertainty": "Incremental queue memory is not reported separately."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Evaluate all records in an already fetched page without a proportional read for each additional candidate; standalone evaluation left page-read counts nearly unchanged in the reported setup.",
      "measurement_needed": "Measure pages read, records evaluated, and useful candidates accepted at matched recall.",
      "premises": [],
      "source_pointer_ids": [
        "SP-017",
        "SP-018"
      ],
      "status": "SOURCED",
      "uncertainty": "Subsequent traversal from added candidates can alter later I/O."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Compare persisted index bytes with page search disabled and enabled.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "No separate persistent-storage term is reported for page search alone."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Page search is a query-time optimization and adds no separate index-construction stage in the source's construction-overhead comparison.",
      "measurement_needed": "Verify build artifacts are byte-identical when page search is enabled without shuffle.",
      "premises": [],
      "source_pointer_ids": [
        "SP-018"
      ],
      "status": "SOURCED",
      "uncertainty": "This excludes optional page shuffle, which has its own preparation cost."
    },
    "ram": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak queue size and RSS with page search on and off.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The incremental candidate-queue RAM for full-page evaluation is not separated."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure temporary candidate and distance-buffer bytes per query.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Per-query scratch storage is not isolated."
    }
  },
  "source_domain": "disk-page graph search scheduling",
  "source_paper_ids": [
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "Full-page candidate evaluation mechanism",
      "locator_type": "SECTION",
      "locator_value": "Section 4.3.3, Page Search",
      "page": 6,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-017"
    },
    {
      "claim_scope": "Standalone compute overhead and unchanged I/O count",
      "locator_type": "PARAGRAPH",
      "locator_value": "Finding 4 and PageSearch discussion",
      "page": 9,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-018"
    },
    {
      "claim_scope": "Complementarity with page shuffle",
      "locator_type": "PARAGRAPH",
      "locator_value": "Finding 8",
      "page": 10,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-019"
    },
    {
      "claim_scope": "Page-size and one-record-per-page boundary",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 7.3 and Finding 12",
      "page": 12,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-020"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-017"
    ],
    "text": "Every record in each fetched disk page becomes available for distance evaluation.",
    "uncertainty": "Record count per page changes with vector dimension and page size."
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
        "SP-017",
        "SP-020"
      ],
      "text": "The paper does not bound queue amplification or determine when page-wide evaluation is beneficial for non-ANN graph kernels.",
      "uncertainty": "The mechanism may trade I/O for unbounded candidate state outside the evaluated search."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-017",
        "SP-019"
      ],
      "text": "A page contains multiple useful records and computation on those records is cheap relative to the page-read latency, especially after locality-aware shuffle.",
      "uncertainty": "The source evaluates vector-distance work, not arbitrary per-record graph kernels."
    }
  ]
}
```
