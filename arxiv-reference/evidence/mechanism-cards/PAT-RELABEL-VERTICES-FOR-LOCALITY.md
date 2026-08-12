# Relabel Vertices For Locality

- Pattern ID: `PAT-RELABEL-VERTICES-FOR-LOCALITY`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus can persist and reuse a permutation-aware artifact.",
      "Its ID translation remains correct for outputs and queries."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source reports fewer cache references and faster reused BFS on reordered graphs.",
      "The source reports that full-RCM time exceeds one BFS run on its fixtures.",
      "The source proposes partial RCM to trade preparation for locality."
    ],
    "source_pointer_ids": [
      "SP-029",
      "SP-030"
    ],
    "text": "Knight Bus could expose a prepared locality ordering as an artifact variant only when its quote includes peak preparation state and an amortization horizon, with refusal or partial ordering when full RCM exceeds the build budget.",
    "uncertainty": "The source does not quantify out-of-core preparation or updated-graph maintenance."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-027"
    ],
    "text": "RCM explores each connected component level by level, beginning from a minimum-degree unvisited vertex and labeling unvisited neighbors in increasing degree order.",
    "uncertainty": "The paper chooses a reproducible minimum-degree seed instead of a pseudo-peripheral heuristic."
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
      "SP-027",
      "SP-029",
      "SP-030"
    ],
    "text": "The paper gives the algorithm, cache and scaling ablations, and full versus partial preparation measurements on named fixtures, but this campaign did not reproduce the work or inspect code.",
    "uncertainty": "Grade C is bounded to the source's ARMv8 platform and graph set."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-027"
    ],
    "text": "The graph is stored as CSR arrays after applying a permutation derived from non-isolated vertices and degree-sorted neighbor lists.",
    "uncertainty": "The paper stores each undirected edge in both directions."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-025",
        "SP-030"
      ],
      "text": "For one or few traversals, full RCM preparation can dominate BFS time, and the reordered degree distribution introduces load imbalance unless scheduling is changed.",
      "uncertainty": "This is a cost and scheduling boundary, not a topology-correctness failure."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-RELABEL-VERTICES-FOR-LOCALITY",
  "falsifying_test": {
    "controlled_variables": [
      "logical graph",
      "BFS sources",
      "thread count",
      "CSR value widths",
      "cache state"
    ],
    "failure_signal": "Relabeling changes answers, exceeds the declared preparation peak, or fails to amortize within the quoted traversal count",
    "fixture": "One static undirected graph used for a single traversal and for repeated traversals, encoded in original, full-RCM, and partial-RCM orders",
    "independent_oracle": "Original-ID BFS predecessor and distance results from the unpermuted graph",
    "scope": "Smallest correctness, peak, and amortization falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-025",
      "SP-027"
    ],
    "text": "Relabeling changes vertex identifiers and physical order while preserving graph topology; isolated vertices are appended outside the non-isolated reordered region.",
    "uncertainty": "Downstream outputs must translate identifiers consistently."
  },
  "knight_bus_algorithm_families": [
    "BFS",
    "SHORTEST_PATHS_UNWEIGHTED",
    "WCC",
    "TRAVERSAL_BASED_GRAPH_ALGORITHMS"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-026",
      "SP-027"
    ],
    "text": "Traverse connected components in RCM order, visiting lower-degree neighbors first, reverse the resulting permutation, and rebuild the CSR under the new identifiers.",
    "uncertainty": "RCM is a bandwidth-reduction heuristic, not an optimal ordering algorithm."
  },
  "name": "Relabel Vertices For Locality",
  "pattern_id": "PAT-RELABEL-VERTICES-FOR-LOCALITY",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-025",
      "SP-026",
      "SP-029"
    ],
    "text": "Random vertex identifiers make BFS adjacency accesses spatially irregular and produce many cache references and misses.",
    "uncertainty": "The evaluated graphs are undirected power-law and selected real-world graphs."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-027",
      "SP-030"
    ],
    "text": "The permutation and reordered CSR are prepared once and reused across BFS runs; partial RCM recomputes ordering for only a selected proportion of vertices.",
    "uncertainty": "Amortization depends on how many traversals reuse the artifact."
  },
  "related_pattern_ids": [
    "PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS",
    "PAT-MATERIALIZE-EACH-PIPELINE-STAGE",
    "PAT-SHRINK-VISITED-PARTITION-BOUNDS"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-027"
    ],
    "text": "CSR, the permutation, visited state, sorted non-isolated vertices, and degree-sorted neighbor access are used during preparation.",
    "uncertainty": "Peak bytes are reported only as part of the platform and graph-scale feasibility, not per structure."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure artifact read/write bytes if reordering is applied out of core.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The study is in-memory and reports cache references rather than storage-device I/O."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure retained CSR, permutation, and optional original-ID mapping bytes.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not report serialized original-versus-reordered artifact sizes."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Full RCM sorts degree information, traverses every connected component, builds a permutation, and can cost much more time than one BFS; partial RCM reduces this preparation proportionally in the source's proposal.",
      "measurement_needed": "Measure preparation wall time and the traversal count needed to amortize it.",
      "premises": [],
      "source_pointer_ids": [
        "SP-027",
        "SP-030"
      ],
      "status": "SOURCED",
      "uncertainty": "Amortized benefit depends on graph reuse and selected partial ratio."
    },
    "ram": {
      "assumptions": [],
      "expression": "CSR plus permutation, visited, non-isolated-vertex, and degree-sorted-neighbor preparation state; the source platform holds the complete graph in memory.",
      "measurement_needed": "Measure peak build RSS and retained reordered-CSR bytes per vertex and edge.",
      "premises": [],
      "source_pointer_ids": [
        "SP-027"
      ],
      "status": "SOURCED",
      "uncertainty": "Per-structure peak bytes are not separated."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "Preparation uses a permutation, visited flags, a sorted non-isolated-vertex list, and degree-sorted neighbor data.",
      "measurement_needed": "Measure peak and releasable temporary bytes across preparation phases.",
      "premises": [],
      "source_pointer_ids": [
        "SP-027"
      ],
      "status": "SOURCED",
      "uncertainty": "The source does not identify which structures can be released after rebuilding CSR."
    }
  },
  "source_domain": "shared-memory parallel breadth-first search",
  "source_paper_ids": [
    "PAPER-2012.10026"
  ],
  "source_pointers": [
    {
      "claim_scope": "Topology preservation, locality motivation, isolated vertices, and induced load imbalance",
      "locator_type": "SECTION",
      "locator_value": "Section 1, final introductory paragraphs",
      "page": 1,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "SP-025"
    },
    {
      "claim_scope": "Bandwidth reduction and heuristic reordering",
      "locator_type": "SECTION",
      "locator_value": "Section 2.3, Reverse Cuthill-Mckee Algorithm",
      "page": 3,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "SP-026"
    },
    {
      "claim_scope": "RCM data arrangement, access order, permutation, and isolated-vertex handling",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 3 and Section 3.2",
      "page": 4,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "SP-027"
    },
    {
      "claim_scope": "RCM locality enabling contiguous partition shrink",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 3.4, shrinking partitions paragraph",
      "page": 7,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "SP-028"
    },
    {
      "claim_scope": "Cache-reference and cache-miss ablations",
      "locator_type": "TABLE",
      "locator_value": "Table 1 and Figure 4",
      "page": 9,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "SP-029"
    },
    {
      "claim_scope": "Full-RCM preparation cost and partial-RCM alternative",
      "locator_type": "SECTION",
      "locator_value": "Section 4.6 and Table 4",
      "page": 10,
      "paper_id": "PAPER-2012.10026",
      "pointer_id": "SP-030"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-025",
      "SP-029"
    ],
    "text": "BFS subsequently scans adjacency ranges from the reordered CSR, where related vertices are physically closer.",
    "uncertainty": "The study measures memory-cache behavior rather than device I/O."
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
        "SP-027",
        "SP-030"
      ],
      "text": "The paper does not evaluate graph updates, out-of-core reordering, directed graphs, or a hard-memory preparation cap.",
      "uncertainty": "Those omissions limit direct admission guarantees."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-027",
        "SP-029",
        "SP-030"
      ],
      "text": "The graph fits the in-memory CSR workflow, topology is static across enough traversals to amortize preparation, and reduced matrix bandwidth aligns BFS accesses spatially.",
      "uncertainty": "The best partial-RCM ratio varies by graph."
    }
  ]
}
```
