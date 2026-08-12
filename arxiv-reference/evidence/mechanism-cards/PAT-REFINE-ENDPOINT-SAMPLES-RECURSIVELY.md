# Refine Endpoint Samples Recursively

- Pattern ID: `PAT-REFINE-ENDPOINT-SAMPLES-RECURSIVELY`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus offers endpoint-sampled personalized ranking",
      "A pre-run planner can bound or conservatively cap source fan-out and recursion depth"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-REFINE-ENDPOINT-SAMPLES-RECURSIVELY: each refinement level fetches endpoint groups for an expanded neighborhood",
      "PAT-REFINE-ENDPOINT-SAMPLES-RECURSIVELY: the source benchmark shows a precision versus query-work trade-off"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-005"
    ],
    "text": "Knight Bus would need to admit recursive sample refinement against a declared fan-out and recursion-depth budget, accounting separately for index reads and temporary aggregation rather than treating the base endpoint index as the complete working set.",
    "uncertainty": "The source does not bound tail latency, duplicate-page effects, or peak accumulator state on Knight Bus artifacts."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Begin with a source lookup, then fan out to endpoint groups for its out-neighbors; one recursion level uses the neighbor sample pool, while deeper levels repeat this fan-out.",
    "uncertainty": "The paper gives a database-access trade-off but not a tail-latency model for high-degree or multi-level queries."
  },
  "confidence_rationale": {
    "assumptions": [
      "The source benchmark isolates recursion sufficiently for its stated comparison"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Section 2.3 defines the query mechanism",
      "Figure 3 reports the source's one-level refinement result"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-004",
      "SP-005"
    ],
    "text": "Confidence is moderate: the refinement follows a stated decomposition equation and has a source benchmark, but only one recursion level and one web-graph evaluation are reported and no reproduction was performed here.",
    "uncertainty": "No code inspection, independent rerun, or non-web workload evaluation occurred in this campaign."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "The mechanism reuses the per-vertex endpoint groups of the base index and the graph's out-neighbor lists rather than materializing a larger dense vector index.",
    "uncertainty": "The physical neighborhood and endpoint encodings are unspecified."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "text": "The expanded neighborhood requires too many database accesses for the query latency budget or the caller requires exact low-probability values.",
      "uncertainty": "The paper describes the trade-off but does not publish a universal latency cutoff."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-REFINE-ENDPOINT-SAMPLES-RECURSIVELY",
  "falsifying_test": {
    "controlled_variables": [
      "N",
      "teleportation parameter",
      "source out-degree",
      "recursion depth",
      "index layout",
      "cache state"
    ],
    "failure_signal": "One-level refinement fails to improve oracle ranking agreement while consuming the predicted neighbor accesses, or exceeds the declared fan-out memory/I/O cap",
    "fixture": "A directed graph with one low-degree source and one high-degree source whose exact personalized rankings are known",
    "independent_oracle": "Exact dense personalized PageRank and a no-recursion endpoint-sampling baseline",
    "scope": "Smallest query-refinement falsifier only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "A source's personalized vector decomposes into its teleportation mass plus the average personalized vectors of its out-neighbors, scaled by the continuation probability.",
    "uncertainty": "The equation assumes the paper's PageRank transition and dangling-node treatment."
  },
  "knight_bus_algorithm_families": [
    "PAGERANK_CENTRALITY",
    "SEEDED_RANDOM_WALK"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "At query time, load the source's out-neighborhood and the endpoint samples stored for those neighbors, then combine their empirical distributions through the recursive personalized-PageRank equation; repeat to additional levels when more samples are needed.",
    "uncertainty": "Each added level expands the accessed neighborhood and query work."
  },
  "name": "Refine Endpoint Samples Recursively",
  "pattern_id": "PAT-REFINE-ENDPOINT-SAMPLES-RECURSIVELY",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "A fixed N-endpoint index gives at most N nonzero empirical destinations for one source and can be too coarse for sources with large neighborhoods.",
    "uncertainty": "The severity depends on neighborhood size and the target ranking depth."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "The refined approximate personalized vector is recomputed from the larger neighbor-derived sample pool for each query.",
    "uncertainty": "The recomputation remains a Monte Carlo approximation."
  },
  "related_pattern_ids": [
    "PAT-STORE-RANDOM-WALK-ENDPOINTS"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "The query holds the source neighborhood, fetched endpoint samples, and an accumulator for the combined empirical distribution.",
    "uncertainty": "Peak accumulator cardinality is not bounded by the paper."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "A basic query uses one database access; one recursive neighbor level is described as using |O(u)| database accesses to draw from |O(u)| * N samples.",
      "measurement_needed": "Measure unique index pages, bytes read, seeks, and latency by source degree and recursion depth.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "The source counts accesses, not bytes, and deeper recursion can revisit or expand overlapping neighborhoods."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "The shared base index remains N * V endpoint identifiers; recursive refinement consumes neighboring groups from that index.",
      "measurement_needed": "Measure endpoint-index plus graph-neighborhood bytes under the target encoding.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "Graph-neighborhood storage and index metadata are not included in the N * V statement."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Reuse the base preprocessing that stores N endpoint fingerprints for each vertex; no separate refined vectors are precomputed.",
      "measurement_needed": "Record the incremental preprocessing and metadata needed to expose ordered out-neighbor lists.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "The graph neighborhood index needed for query fan-out is not costed separately."
    },
    "ram": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak query RSS by source degree, recursion depth, N, and requested top-list size.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not bound the query accumulator, fetched-neighborhood, or whole-process RAM for recursive refinement."
    },
    "temporary_storage": {
      "assumptions": [
        "The implementation aggregates samples in temporary query state instead of streaming directly into a bounded top-k sketch"
      ],
      "expression": "Temporary query state grows with the fetched endpoint sample pool and the distinct destinations accumulated at the selected recursion depth.",
      "measurement_needed": "Measure peak fetched-sample and accumulator bytes by degree, overlap, N, and recursion depth.",
      "premises": [
        "One recursion level combines endpoint groups for every out-neighbor",
        "An empirical vector can contain one distinct destination per observed endpoint"
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "status": "DERIVED",
      "uncertainty": "Neighbor overlap, duplicate endpoints, accumulator representation, and deeper recursion make the peak unknown."
    }
  },
  "source_domain": "query-time refinement of sampled personalized PageRank indexes",
  "source_paper_ids": [
    "PAPER-HASH-0232e71ded2b5c43"
  ],
  "source_pointers": [
    {
      "claim_scope": "N endpoint samples stored for each source vertex",
      "locator_type": "SECTION",
      "locator_value": "Section 2, fingerprint index paragraphs",
      "page": 8,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Neighbor-vector decomposition, one-level sample amplification, and database-access trade-off",
      "locator_type": "EQUATION",
      "locator_value": "Section 2.3 recursive PPV equation and following three paragraphs",
      "page": 12,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Recursion/truncation continuation and sparse empirical-vector caveat",
      "locator_type": "SECTION",
      "locator_value": "Section 2.3 continuation",
      "page": 13,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Eighty-million-page fixture and one thousand random personalization nodes",
      "locator_type": "SECTION",
      "locator_value": "Section 5, experiment fixture paragraphs",
      "page": 18,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "One-level recursion reducing the fingerprint count needed for a given ranking precision in the source benchmark",
      "locator_type": "FIGURE",
      "locator_value": "Figure 3 and Section 5.2 paragraph 2",
      "page": 20,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "Evaluation scope and approximation conclusion",
      "locator_type": "SECTION",
      "locator_value": "Section 6",
      "page": 22,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "SP-006"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Endpoint groups for neighboring vertices are fetched from the index as the recursive query expands.",
    "uncertainty": "The source describes database accesses, not byte-level streaming."
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
        "SP-002",
        "SP-006"
      ],
      "text": "The source does not establish the best recursion depth or stopping policy for arbitrary degree distributions, storage devices, and latency objectives.",
      "uncertainty": "This policy remains workload- and implementation-dependent."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "text": "A high-degree personalization source needs more effective samples than its own fixed endpoint group provides and extra query-time accesses are acceptable.",
      "uncertainty": "The source does not define a degree threshold for enabling refinement."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-005"
      ],
      "text": "One-level neighbor recursion can exchange additional query work for better ranking precision at a fixed base-index size.",
      "uncertainty": "The observed improvement is limited to the source's web-graph benchmark and settings."
    }
  ]
}
```
