# Bound Search Candidate Frontier

- Pattern ID: `PAT-BOUND-SEARCH-CANDIDATE-FRONTIER`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus knows c and candidate-entry width before execution.",
      "The implementation does not allocate an unbounded duplicate or visited structure outside C."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source algorithm enforces |C| <= c.",
      "The source observes dataset-dependent recall ceilings and path lengths."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Knight Bus can expose candidate capacity as an admission-time auxiliary-state bound, but must keep recall and path-I/O outcomes conditional rather than promising them from c alone.",
    "uncertainty": "The paper does not supply an end-to-end memory estimator or a recall guarantee for arbitrary fixtures."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-005"
    ],
    "text": "Each step selects the nearest candidate, visits its adjacency list, trims farthest candidates, and updates the result set when a nearer neighbor is found.",
    "uncertainty": "Parallel scheduling is outside the pseudocode."
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
      "SP-001",
      "SP-003",
      "SP-005"
    ],
    "text": "The frontier bound is explicit in a formal definition and pseudocode, and the paper reports candidate size, path length, and memory across many datasets; no campaign reproduction was performed.",
    "uncertainty": "Implementation-specific hidden allocations were not code-inspected."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-005"
    ],
    "text": "Search maintains a bounded candidate set C ordered by query distance and a fixed-size result set R.",
    "uncertainty": "The paper abstracts away the concrete heap or array representation."
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
      "text": "Some compared graph/search combinations reach a recall ceiling even when the candidate set is enlarged, especially on harder datasets.",
      "uncertainty": "The paper does not identify one universal capacity threshold."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-BOUND-SEARCH-CANDIDATE-FRONTIER",
  "falsifying_test": {
    "controlled_variables": [
      "candidate capacity c",
      "graph index",
      "seed set",
      "query",
      "distance function"
    ],
    "failure_signal": "The candidate structure exceeds c entries, or the claimed bounded state depends on an uncounted structure, or recall is presented as guaranteed despite the oracle counterexample",
    "fixture": "A small ANNS graph with two branches where the true nearest neighbor is reached only if an initially distant candidate survives",
    "independent_oracle": "Exhaustive nearest-neighbor search and an instrumented candidate-set trace",
    "scope": "Smallest mechanism-level falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-005"
    ],
    "text": "The candidate set never exceeds configured capacity c because the farthest candidate is removed whenever an expansion overfills it.",
    "uncertainty": "The bound covers candidate entries, not the graph index or vector payloads."
  },
  "knight_bus_algorithm_families": [
    "APPROXIMATE_NEAREST_NEIGHBOR_SEARCH"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-005"
    ],
    "text": "Expand the nearest unvisited candidate, insert its neighbors, and repeatedly evict the farthest candidate until the frontier returns to capacity c.",
    "uncertainty": "Termination and result-set policies differ among algorithms."
  },
  "name": "Bound Search Candidate Frontier",
  "pattern_id": "PAT-BOUND-SEARCH-CANDIDATE-FRONTIER",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-004"
    ],
    "text": "Best-first graph search can accumulate a large candidate frontier whose frequent access raises cache and memory pressure.",
    "uncertainty": "The pressure depends on graph quality, dataset hardness, and target recall."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-005"
    ],
    "text": "Search repeatedly evaluates distances from the query to newly encountered vertices and updates nearest/farthest orderings.",
    "uncertainty": "Distance-evaluation reuse is implementation-specific."
  },
  "related_pattern_ids": [
    "PAT-PRUNE-NEIGHBORS-BY-DIVERSITY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "The graph index, bounded candidate frontier, result set, query, and distance values are active search state.",
    "uncertainty": "Whole-process state beyond the measured implementations is not itemized."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "For externally stored vectors, external accesses track query path length; candidate capacity alone does not bound path length.",
      "measurement_needed": "Measure block reads and bytes read per visited path vertex on the target layout.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "The paper reports path length but not portable byte traffic."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Confirm whether the implementation persists search buffers or only the graph index.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The candidate frontier is temporary and the paper does not assign persistent storage to this mechanism."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure any queue-allocation or capacity-tuning preprocessing separately from index construction.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The frontier bound is a search rule and the paper does not isolate preprocessing attributable to it."
    },
    "ram": {
      "assumptions": [],
      "expression": "RAM_search includes the graph index, a candidate set with |C| <= c, a fixed result set, and vector/query state.",
      "measurement_needed": "Measure peak RSS and candidate bytes for the selected index and c.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "Entry widths and whole-process overhead are not normalized."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "Temporary search state includes at most c candidate entries and the fixed-size result set R.",
      "measurement_needed": "Measure all temporary search allocations, including visited-state storage.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "Visited markers and implementation queue overhead are not included in the abstract algorithm."
    }
  },
  "source_domain": "graph-based approximate nearest-neighbor search",
  "source_paper_ids": [
    "PAPER-2101.12631"
  ],
  "source_pointers": [
    {
      "claim_scope": "Bounded candidate-set routing rule",
      "locator_type": "SECTION",
      "locator_value": "Section 4.2, Definition 4.7 Best First Search",
      "page": 8,
      "paper_id": "PAPER-2101.12631",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Candidate capacity, cache pressure, accuracy ceiling, and I/O proxy",
      "locator_type": "SECTION",
      "locator_value": "Section 5.3, Candidate Set Size and Query Path Length",
      "page": 10,
      "paper_id": "PAPER-2101.12631",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Measured candidate sizes, path lengths, and peak search memory",
      "locator_type": "TABLE",
      "locator_value": "Table 5",
      "page": 11,
      "paper_id": "PAPER-2101.12631",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Guidance to reduce candidate set size and memory overhead",
      "locator_type": "SECTION",
      "locator_value": "Section 6, Guidelines",
      "page": 12,
      "paper_id": "PAPER-2101.12631",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Exact candidate eviction and result-update schedule",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 1, BFS(G,q,c,S-hat)",
      "page": 20,
      "paper_id": "PAPER-2101.12631",
      "pointer_id": "SP-005"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "When original vectors are external, visited path length determines the number of external accesses, while the main study otherwise evaluates in-memory core algorithms.",
    "uncertainty": "The source does not give bytes per path step."
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
        "SP-004"
      ],
      "text": "The paper does not establish whether a fixed candidate capacity gives deterministic recall, runtime, or I/O bounds on unseen data distributions.",
      "uncertainty": "Dataset-contingent performance is a central finding of the source."
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
      "text": "The mechanism works when the configured capacity reaches the requested recall before the search enters an accuracy ceiling.",
      "uncertainty": "Required capacity differs by graph and dataset."
    }
  ]
}
```
