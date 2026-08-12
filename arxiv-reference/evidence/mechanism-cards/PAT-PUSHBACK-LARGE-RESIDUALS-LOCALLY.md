# Pushback Large Residuals Locally

- Pattern ID: `PAT-PUSHBACK-LARGE-RESIDUALS-LOCALLY`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `D_THEORETICAL_OR_INCOMPLETE`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus exposes reverse-neighbor access and can estimate or cap encountered in-degree volume.",
      "The requested result accepts the paper's additive approximation semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source bounds the number of pushes from the error threshold.",
      "The source states that actual work is the sum of in-degrees of pushed vertices."
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-004",
      "SP-006"
    ],
    "text": "A Knight Bus admission model for target-local PageRank could budget threshold-qualified pushes and encountered reverse-neighbor volume separately instead of charging for a full all-vertex iteration.",
    "uncertainty": "The paper does not establish a storage-device I/O bound or predict the pushed set before execution."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003"
    ],
    "text": "An eligibility queue starts with the target, removes one threshold-qualified vertex per step, and enqueues in-neighbors whose updated residual crosses the threshold.",
    "uncertainty": "Queue order affects practical locality but not the stated invariant."
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
      "SP-004",
      "SP-006"
    ],
    "text": "The paper proves the invariant, error condition, push bound, and in-degree runtime dependency, but reports no implementation benchmark or whole-process resource measurement.",
    "uncertainty": "Evidence is analytical and incomplete for systems-resource claims."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "The algorithm maintains vertex-indexed settled and residual vectors and requires access from each pushed vertex to its in-neighbors.",
    "uncertainty": "The paper does not prescribe a concrete sparse-map or reverse-adjacency encoding."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "D_THEORETICAL_OR_INCOMPLETE",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-004",
        "SP-006"
      ],
      "text": "The push-count bound does not provide a bounded runtime when selected vertices have very large in-degrees, because each push costs time proportional to that in-degree.",
      "uncertainty": "This is a failure of the stronger runtime bound, not of the approximation invariant."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-PUSHBACK-LARGE-RESIDUALS-LOCALLY",
  "falsifying_test": {
    "controlled_variables": [
      "vertex count",
      "edge count",
      "teleportation parameter",
      "error threshold",
      "target PageRank"
    ],
    "failure_signal": "The local result violates the additive error bound, or a push-only admission estimate predicts equivalent work despite materially different enumerated reverse-neighbor volume",
    "fixture": "Two equal-size directed graphs with the same target and error threshold but sharply different in-degrees on every threshold-qualified residual vertex",
    "independent_oracle": "A dense contribution-vector solve using the paper's PageRank equations",
    "scope": "Smallest correctness-and-resource falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "At every push, the settled vector plus the contribution vector induced by the residual equals the target's exact contribution vector.",
    "uncertainty": "The invariant depends on the paper's PageRank transition and teleportation definitions."
  },
  "knight_bus_algorithm_families": [
    "PAGERANK",
    "PERSONALIZED_PAGERANK",
    "TARGET_LOCAL_CENTRALITY"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-004"
    ],
    "text": "Select a vertex whose residual meets the error threshold, move an alpha fraction into settled mass, and distribute the remainder backward to its in-neighbors until no eligible residual remains or a settled-mass cap is reached.",
    "uncertainty": "The paper permits any eligible selection order and mentions a priority queue only as a heuristic."
  },
  "name": "Pushback Large Residuals Locally",
  "pattern_id": "PAT-PUSHBACK-LARGE-RESIDUALS-LOCALLY",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Computing contributors to one target by constructing personalized PageRank vectors for many possible sources wastes work outside the target-local region.",
    "uncertainty": "The source studies PageRank contributions on directed graphs, not arbitrary traversal state."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-004"
    ],
    "text": "The unexplained contribution is not recomputed from complete walks; it remains represented by the residual term and is refined incrementally by later pushes.",
    "uncertainty": "The source does not compare incremental residual maintenance with explicit recomputation in measured runs."
  },
  "related_pattern_ids": [],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-005"
    ],
    "text": "Settled values, residual values, and the queue of currently eligible vertices are maintained during the local computation.",
    "uncertainty": "Whole-process byte usage is not reported."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure bytes and random reads used to enumerate in-neighbors under cold and warm cache.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not model device I/O or cache-line traffic."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure graph and reverse-index bytes retained after preparation.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "No persistent representation size is specified."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure time and bytes required to prepare reverse-neighbor access for the fixture.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The cost of constructing or retaining reverse adjacency is not bounded."
    },
    "ram": {
      "assumptions": [],
      "expression": "Vertex-indexed settled and residual state plus an eligibility queue; the output support is bounded by the number of pushback operations, but whole-process bytes are not given.",
      "measurement_needed": "Measure peak whole-process RSS and touched-vertex state on a declared target and error threshold.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "Residual-map and queue encodings are unspecified."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak temporary allocations separately from retained graph storage.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper bounds pushes and support, not allocator or queue scratch bytes."
    }
  },
  "source_domain": "local PageRank contribution analysis",
  "source_paper_ids": [
    "PAPER-HASH-c2a6a5317d82ac28"
  ],
  "source_pointers": [
    {
      "claim_scope": "Local contribution computation and pushback cost model",
      "locator_type": "SECTION",
      "locator_value": "Section 3.2, opening paragraphs",
      "page": 8,
      "paper_id": "PAPER-HASH-c2a6a5317d82ac28",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Approximation alternatives, push bound, and residual invariant",
      "locator_type": "THEOREM",
      "locator_value": "Theorem 3.2 and Lemma 3.3",
      "page": 9,
      "paper_id": "PAPER-HASH-c2a6a5317d82ac28",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Reverse-neighbor push operation and residual-threshold queue",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 1 and following queue paragraph",
      "page": 10,
      "paper_id": "PAPER-HASH-c2a6a5317d82ac28",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Termination, error condition, and degree-proportional operation cost",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 2 and proof of Theorem 3.2",
      "page": 11,
      "paper_id": "PAPER-HASH-c2a6a5317d82ac28",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Support bound and modified residual retention",
      "locator_type": "SECTION",
      "locator_value": "Section 3.3, support of the approximate vector",
      "page": 12,
      "paper_id": "PAPER-HASH-c2a6a5317d82ac28",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "Runtime limitation from the in-degrees of pushed vertices",
      "locator_type": "SECTION",
      "locator_value": "Section 6.1, improving dependency on in-degrees",
      "page": 20,
      "paper_id": "PAPER-HASH-c2a6a5317d82ac28",
      "pointer_id": "SP-006"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "Each push enumerates the selected vertex's in-neighbor list and updates those neighbors' residual entries.",
    "uncertainty": "The source analyzes neighbor enumeration as computation and does not characterize storage-device streaming."
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
        "SP-003",
        "SP-006"
      ],
      "text": "The paper leaves practical RAM, device I/O, reverse-index construction cost, and the effect of queue ordering unmeasured.",
      "uncertainty": "These omissions prevent a whole-process resource guarantee."
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
      "text": "The target, teleportation parameter, and absolute error threshold are defined, and in-neighbors can be enumerated for every pushed vertex.",
      "uncertainty": "Practical efficiency still depends on the encountered in-degree distribution."
    }
  ]
}
```
