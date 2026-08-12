# Route Filters By Cost

- Pattern ID: `PAT-ROUTE-FILTERS-BY-COST`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus has calibrated plan-specific estimators."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source chooses among plans using explicit I/O and compute estimates.",
      "The source observes model error from early termination and distribution."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A007 can treat plan choice as an admission-time comparison of estimated resident bytes, page reads, and compute, while exposing model assumptions and estimate-versus-actual error rather than presenting the chosen plan as guaranteed cheapest.",
    "uncertainty": "A cost-minimizing route does not itself enforce a hard resource ceiling."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Before each query, compute mode-specific costs, choose speculative pre-filtering, speculative in-filtering, or post-filtering, then execute only the selected plan.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "Reported measurements are representative only of their declared fixtures."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source supplies an explicit cost table and compares estimated with actual I/O."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "The planner and its benchmarked errors are documented, but workload assumptions are material and no reproduction or code inspection occurred.",
    "uncertainty": "Generality beyond filtered ANNS remains unknown."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Selectors expose selectivity and precision estimates backed by counts, histograms, quantiles, or probabilistic-filter rates; the router consumes those summaries.",
    "uncertainty": "Summary accuracy depends on data distribution."
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
      "text": "Clustered valid vectors and early termination can make modeled graph I/O materially differ from actual I/O and therefore misroute a query.",
      "uncertainty": "The source reports both over- and under-estimation rather than a single direction."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-ROUTE-FILTERS-BY-COST",
  "falsifying_test": {
    "controlled_variables": [
      "Index, query, cache state, concurrency, cost weights, selectivity summaries, and target result count."
    ],
    "failure_signal": "The router repeatedly chooses a plan whose measured cost is not minimal or whose estimate error changes sign without being surfaced.",
    "fixture": "Queries spanning low, moderate, and high selectivity over both uniform and clustered valid vertices, with all exact plans executable on the same frozen index.",
    "independent_oracle": "Measured lowest-cost exact plan for each query under the same cold-cache and thread settings.",
    "scope": "Planner ordering and estimate reporting, not a G09 benchmark packet."
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Every routed strategy still performs exact verification before returning results; routing changes cost, not the validity contract.",
    "uncertainty": "NONE"
  },
  "knight_bus_algorithm_families": [
    {
      "assumptions": [
        "Knight Bus exposes exact alternative plans for a named algorithm and workload."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source routes among semantically equivalent filtering plans using resource estimates."
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "BFS or bounded path search, WCC, PageRank, node similarity or kNN, and community detection could use this routing form only where each family has multiple exact plans with separately estimable state and I/O.",
      "uncertainty": "The source does not validate routing for iterative graph analytics."
    }
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Estimate candidate expansion from query selectivity and approximate-filter precision, estimate SSD pages and distance computations for each filtering mode, combine them with configurable weights, and choose the lowest estimated cost.",
    "uncertainty": "The model assumes distribution properties that can cause over- or under-estimation."
  },
  "name": "Route Filters By Cost",
  "pattern_id": "PAT-ROUTE-FILTERS-BY-COST",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "No single filtering mode is efficient across query selectivity, approximate-filter precision, SSD I/O, and compute cost.",
    "uncertainty": "NONE"
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Mode costs are recomputed per query from current query attributes, selectivity, precision, and system parameters.",
    "uncertainty": "Weights are configured rather than learned online in the described model."
  },
  "related_pattern_ids": [
    "PAT-SCAN-RARE-FILTER-FREQUENT"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Selectivity summaries, false-positive estimates, graph and record-size parameters, and configurable I/O/compute weights remain resident.",
    "uncertainty": "The router-state byte count is not isolated."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Each candidate plan estimates SSD pages for attribute-index scans, graph records, and reranking records, then weights them against compute.",
      "measurement_needed": "Log estimated versus actual page reads by plan and query.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "SOURCED",
      "uncertainty": "The source documents estimation error from early termination and nonuniform data."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure durable bytes for persisted statistics and model parameters.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The incremental persistent bytes attributable only to the router are not stated."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Build or maintain counts, histograms, quantiles, and probabilistic-filter statistics used to estimate selectivity and precision.",
      "measurement_needed": "Measure summary-build and refresh work for mutable attributes.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "SOURCED",
      "uncertainty": "Refresh cost under updates is not evaluated."
    },
    "ram": {
      "assumptions": [],
      "expression": "Compact selector statistics and mode parameters are resident in addition to the underlying index state.",
      "measurement_needed": "Measure bytes for all counts, histograms, quantiles, and per-selector estimates.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "SOURCED",
      "uncertainty": "Router-state bytes are not reported separately."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure planner allocations per query and per composed selector.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Per-query planning scratch is not quantified."
    }
  },
  "source_domain": "cost-based filtered vector-search routing",
  "source_paper_ids": [
    "PAPER-2605.17992"
  ],
  "source_pointers": [
    {
      "claim_scope": "Selectivity- and precision-scaled I/O and compute estimates for pre-, in-, and post-filtering.",
      "locator_type": "TABLE",
      "locator_value": "Table 1 and Section 4.2, Cost Estimation",
      "page": 6,
      "paper_id": "PAPER-2605.17992",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Measured estimation error, early termination effects, and missing distribution awareness.",
      "locator_type": "FIGURE",
      "locator_value": "Figures 10-11 and Section 5.4, Cost estimation accuracy",
      "page": 11,
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
    "text": "Only the SSD records and attribute indexes required by the selected filtering mode are read.",
    "uncertainty": "Actual reads can differ because of early termination and data clustering."
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
        "SP-002"
      ],
      "text": "A distribution-aware model that captures early termination is left for future work.",
      "uncertainty": "No validated replacement model is supplied."
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
      "text": "Selectivity and precision summaries approximate the query workload closely enough that relative mode costs are ordered correctly.",
      "uncertainty": "Uniform valid-vector distribution is an explicit modeling assumption."
    }
  ]
}
```
