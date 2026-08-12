# Store Random Walk Endpoints

- Pattern ID: `PAT-STORE-RANDOM-WALK-ENDPOINTS`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus exposes a sampled personalized-ranking family with the same geometric-walk semantics",
      "The runtime can predeclare N, c, identifier width, and the sorter strategy"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-STORE-RANDOM-WALK-ENDPOINTS: the source stores N endpoints per vertex and maintains a shrinking active Paths array",
      "PAT-STORE-RANDOM-WALK-ENDPOINTS: ranking error depends on N and score separation"
    ],
    "source_pointer_ids": [
      "SP-003",
      "SP-005",
      "SP-007"
    ],
    "text": "A Knight Bus admission record for sampled personalized ranking would need to budget endpoint-index bytes, active-path temporary state, sort-and-scan I/O, and an explicit approximation target rather than using topology size alone.",
    "uncertainty": "The paper does not provide whole-process RSS, byte-level I/O, or calibration on Knight Bus graph families."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-004",
      "SP-006"
    ],
    "text": "During external-memory construction, sort active path pairs by current endpoint and scan the source-sorted edge stream to advance every path in bulk; during a basic query, fetch the source's endpoint group once and aggregate frequencies.",
    "uncertainty": "Long paths require the paper's reuse or truncation strategy to avoid an excessive number of edge scans."
  },
  "confidence_rationale": {
    "assumptions": [
      "The reported implementation follows the described sampling and truncation procedures"
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Theorem 1.2 supplies the endpoint-distribution invariant",
      "Sections 2.1 and 5 report resource analysis and source benchmarks"
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-005",
      "SP-008",
      "SP-009"
    ],
    "text": "Confidence is moderate because the endpoint invariant and storage/I/O model are analytical and the source benchmarks ranking quality on a large web graph, but this campaign did not inspect code or reproduce the result.",
    "uncertainty": "No independent reproduction or implementation inspection was performed."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-004"
    ],
    "text": "The persistent index groups N endpoint fingerprints by each of V source vertices; construction keeps partial walks as pairs of start vertex and current endpoint.",
    "uncertainty": "Identifier width and physical encoding are not fixed by the paper."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-007"
      ],
      "text": "Exact full personalized vectors or reliable ordering among very low and near-tied scores is required.",
      "uncertainty": "Failure means the compact randomized index cannot establish the demanded exactness, not that every returned high-rank entry is wrong."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-005"
      ],
      "text": "Uncontrolled long fingerprint paths force too many full edge scans unless completed by reuse or truncated with an explicit approximation.",
      "uncertainty": "The frequency of this condition depends on c, N, and V."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-STORE-RANDOM-WALK-ENDPOINTS",
  "falsifying_test": {
    "controlled_variables": [
      "graph topology",
      "teleportation parameter c",
      "fingerprints per vertex N",
      "random seed",
      "identifier width",
      "walk truncation or reuse policy"
    ],
    "failure_signal": "Endpoint frequencies do not converge toward oracle probabilities as N rises, or measured index/path state exceeds the declared symbolic accounting under fixed encoding",
    "fixture": "A small directed graph with analytically computable personalized PageRank, plus a second graph containing near-tied low-rank vertices and long geometric walks",
    "independent_oracle": "Dense exact personalized PageRank computed independently at higher numerical precision",
    "scope": "Smallest semantic and resource-accounting falsifier only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "For a walk starting at vertex u whose length is geometrically distributed by the teleportation parameter, the probability of ending at v equals the personalized PageRank value from u to v.",
    "uncertainty": "This invariant uses the paper's normalized transition matrix and teleportation model."
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
      "SP-003"
    ],
    "text": "Generate N independent geometrically stopped random walks from every vertex, store only their endpoint vertex identifiers, and estimate a source's personalized vector from the empirical endpoint distribution.",
    "uncertainty": "The mechanism is randomized and does not preserve exact low-probability entries."
  },
  "name": "Store Random Walk Endpoints",
  "pattern_id": "PAT-STORE-RANDOM-WALK-ENDPOINTS",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Serving arbitrary personalized PageRank queries by running graph-wide power iteration at query time is infeasible for an online service, while storing exact vectors for every source can require quadratic space.",
    "uncertainty": "The infeasibility statement is scoped to the paper's online web-ranking setting and exact full personalization."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-006"
    ],
    "text": "The approximate personalized vector and ranking are recomputed at query time from endpoint frequencies instead of being stored as dense per-source vectors.",
    "uncertainty": "Only vertices observed as endpoints receive nonzero empirical mass unless refinement or truncation correction is used."
  },
  "related_pattern_ids": [
    "PAT-REFINE-ENDPOINT-SAMPLES-RECURSIVELY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-004",
      "SP-005",
      "SP-006"
    ],
    "text": "Construction retains the available external-sort memory M and the current active path records; a basic query retains the selected endpoint records and their empirical-frequency accumulator.",
    "uncertainty": "The paper does not report peak whole-process resident memory for either phase."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "External-sort I/O is bounded by (1/c) * N * V * log_M(NV) plus the required edge scans in the paper's model.",
      "measurement_needed": "Record bytes read and written, sort passes, and edge scans for declared N, V, c, M, and storage device.",
      "premises": [],
      "source_pointer_ids": [
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "The bound is an external-memory operation count and omits device-specific transfer sizes and the exact number of late edge scans."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "N * V endpoint identifiers, grouped as N fingerprints for every vertex.",
      "measurement_needed": "Measure final index bytes including identifiers, group offsets, metadata, and filesystem overhead.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-009"
      ],
      "status": "SOURCED",
      "uncertainty": "Byte size depends on identifier encoding and index metadata that the paper does not quantify."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Start N paths per vertex, repeatedly sort active path pairs by current endpoint, scan edges to advance them, and collect completed endpoints.",
      "measurement_needed": "Measure preprocessing wall time, CPU time, and sort/scan counts on the target graph format.",
      "premises": [],
      "source_pointer_ids": [
        "SP-004",
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "Wall time and CPU cost depend on the sorter, graph encoding, c, and long-path strategy."
    },
    "ram": {
      "assumptions": [],
      "expression": "The external-memory variant assumes constant random-access memory with M denoting available main memory for sorting; peak whole-process RSS is not reported.",
      "measurement_needed": "Measure peak builder and query RSS for a fixed graph, N, c, identifier width, and sorter implementation.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "The constant and implementation buffers are unspecified."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "After k construction rounds the expected active Paths array contains (1-c)^k * N * V start/current-endpoint pairs.",
      "measurement_needed": "Measure peak active-path bytes and external-sort run-file bytes under the declared stopping strategy.",
      "premises": [],
      "source_pointer_ids": [
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "This is an expected record count, not a peak byte bound; sorter run files are not separately quantified."
    }
  },
  "source_domain": "personalized web ranking and external-memory Monte Carlo indexing",
  "source_paper_ids": [
    "PAPER-HASH-0232e71ded2b5c43"
  ],
  "source_pointers": [
    {
      "claim_scope": "Online full-personalization problem, compact endpoint database, and approximation trade-off",
      "locator_type": "SECTION",
      "locator_value": "Section 1, paragraphs 2-5",
      "page": 2,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Endpoint distribution of a geometrically stopped walk equals an individual personalized PageRank vector",
      "locator_type": "THEOREM",
      "locator_value": "Theorem 1.2",
      "page": 7,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "N independent endpoint fingerprints per vertex, empirical distribution, and N times V index size",
      "locator_type": "SECTION",
      "locator_value": "Section 2, Definitions 2.1-2.2 and following paragraphs",
      "page": 8,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "External-memory sort-and-scan generation of partial fingerprint paths",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 1 and Section 2.1",
      "page": 9,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "External-sort I/O bound, expected shrinking Paths array, edge scans, and long-path completion",
      "locator_type": "SECTION",
      "locator_value": "Section 2.1, first three paragraphs",
      "page": 10,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "One-access query reconstruction from endpoint frequencies",
      "locator_type": "SECTION",
      "locator_value": "Section 2.3, first paragraph",
      "page": 12,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "SP-006"
    },
    {
      "claim_scope": "Ranking-order convergence discussion for separated scores and weaker behavior for low ranks",
      "locator_type": "PARAGRAPH",
      "locator_value": "Discussion following Theorems 3.1-3.2",
      "page": 14,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "SP-007"
    },
    {
      "claim_scope": "Source benchmark on fingerprint count and one-level recursive evaluation",
      "locator_type": "FIGURE",
      "locator_value": "Figures 2-3 and Section 5.2",
      "page": 20,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "SP-008"
    },
    {
      "claim_scope": "Linear index conclusion, approximation scope, and evaluated fingerprint setting",
      "locator_type": "SECTION",
      "locator_value": "Section 6",
      "page": 22,
      "paper_id": "PAPER-HASH-0232e71ded2b5c43",
      "pointer_id": "SP-009"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-004",
      "SP-006"
    ],
    "text": "The external-memory builder streams graph edges alongside endpoint-sorted active paths, and completed endpoint records are later streamed or fetched from the index for query aggregation.",
    "uncertainty": "The source counts database scans and total I/O rather than device-specific bytes."
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
        "SP-007"
      ],
      "text": "Worst-case lower bounds do not determine whether one particular graph has exploitable structure that permits a smaller exact representation.",
      "uncertainty": "Graph-specific exact compressibility remains outside the theorem's scope."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-007",
        "SP-008"
      ],
      "text": "The requested result tolerates randomized approximation and focuses on ranking entries whose personalized scores are sufficiently separated.",
      "uncertainty": "Required N depends on the requested error probability and score separation."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-006"
      ],
      "text": "Many arbitrary personalization sources must be served from one precomputed index without graph-wide power iteration at query time.",
      "uncertainty": "The source evaluates web-ranking queries rather than general graph analytics."
    }
  ]
}
```
