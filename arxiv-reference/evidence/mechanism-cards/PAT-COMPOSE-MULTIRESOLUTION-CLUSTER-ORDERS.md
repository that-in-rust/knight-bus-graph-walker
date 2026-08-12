# Compose Multiresolution Cluster Orders

- Pattern ID: `PAT-COMPOSE-MULTIRESOLUTION-CLUSTER-ORDERS`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus accepts an offline static-graph reorder before algorithm execution.",
      "The production adjacency encoder rewards locality and similarity in a way comparable to BV."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-COMPOSE-MULTIRESOLUTION-CLUSTER-ORDERS:resident_state",
      "PAT-COMPOSE-MULTIRESOLUTION-CLUSTER-ORDERS:persistent_storage"
    ],
    "source_pointer_ids": [
      "SP-005",
      "SP-007"
    ],
    "text": "A Knight Bus admission estimate could treat an LLP-like reorder as optional topology preparation that may lower persistent adjacency bytes, but it must charge the graph-resident preprocessing state and cannot assume the paper's compression ratio for a new graph or encoder.",
    "uncertainty": "Compression yield, preparation time, and physical I/O remain unmeasured for Knight Bus artifacts."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-004",
      "SP-006"
    ],
    "text": "Each base labeling is produced by a small number of graph passes; composition then scans node labels and the current permutation, and the randomized update work can be decomposed across cores.",
    "uncertainty": "The exact pass count depends on stopping criteria and the selected resolution set."
  },
  "confidence_rationale": {
    "assumptions": [
      "The reported experimental configuration was applied consistently across compared orderings."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The ordering invariant is explicit in Section 6.",
      "Tables 7 and 8 report source benchmark evidence under randomized initial order."
    ],
    "source_pointer_ids": [
      "SP-003",
      "SP-007"
    ],
    "text": "Confidence is moderate because the paper defines the ordering rule precisely and benchmarks randomized input orders, but this campaign neither reran the implementation nor inspected its code.",
    "uncertainty": "No independent reproduction or code inspection was performed in G05."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-004",
      "SP-007"
    ],
    "text": "The arrangement consists of the graph, a current node permutation, and one cluster label per node for each precomputed resolution; the final permutation is consumed by BV adjacency encoding.",
    "uncertainty": "The source evaluates BV encoding and does not establish identical gains for unrelated encoders."
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
        "SP-004"
      ],
      "text": "Using one repeated resolution loses the benefit of multiresolution composition, and the source provides no global optimality guarantee for the resulting order.",
      "uncertainty": "Failure means loss of the reported ordering advantage, not incorrect graph decoding."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-COMPOSE-MULTIRESOLUTION-CLUSTER-ORDERS",
  "falsifying_test": {
    "controlled_variables": [
      "graph edges",
      "resolution schedule",
      "label-propagation stopping rule",
      "encoder settings",
      "thread count"
    ],
    "failure_signal": "Decoded adjacency differs, or the final order remains materially dependent on initial numbering and does not improve compressed bytes under the fixed encoder",
    "fixture": "Two isomorphic static graphs with identical edges but independently randomized initial node identifiers, including one low-locality social graph",
    "independent_oracle": "Decode both outputs and compare every successor list with the original graph while a fixed unmodified BV-style encoding provides the compression baseline",
    "scope": "Smallest mechanism test description only; no G09 experiment packet exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003"
    ],
    "text": "At every composition step, nodes assigned the same cluster retain their previous relative order, while different clusters inherit the previous order of their leader labels.",
    "uncertainty": "The invariant describes the LLP ordering rule, not an optimality guarantee."
  },
  "knight_bus_algorithm_families": [
    "BFS",
    "PAGERANK",
    "CONNECTED_COMPONENTS",
    "COMMUNITY_DETECTION",
    "TRIANGLE_COUNTING"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-004"
    ],
    "text": "Run label propagation at multiple resolution values and repeatedly compose the resulting clusterings into one node order instead of committing to one resolution or one clustering.",
    "uncertainty": "Resolution values are sampled from the paper's finite schedule and the final order remains heuristic."
  },
  "name": "Compose Multiresolution Cluster Orders",
  "pattern_id": "PAT-COMPOSE-MULTIRESOLUTION-CLUSTER-ORDERS",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Compression methods that exploit adjacency locality and similarity are highly sensitive to node numbering, while non-web social graphs lack a natural URL-derived order.",
    "uncertainty": "The problem statement is scoped to static compressed adjacency structures evaluated by the paper."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-004"
    ],
    "text": "A base APM labeling is computed once for each selected resolution and reused across later randomized composition steps.",
    "uncertainty": "Stopping criteria can still change the cost of producing each base labeling."
  },
  "related_pattern_ids": [],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-005"
    ],
    "text": "The implementation retains graph storage and three integer arrays per node while constructing the order.",
    "uncertainty": "The paper states integer counts rather than whole-process byte accounting."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure cold-cache bytes read and written for each base labeling and final encoding.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper reports linear passes but does not bound physical bytes read or written."
    },
    "persistent_storage": {
      "assumptions": [
        "The graph is static and the encoder metadata is accounted separately."
      ],
      "expression": "compressed_graph_bytes = m * achieved_bits_per_link / 8 + encoder_metadata_bytes",
      "measurement_needed": "Measure final graph and metadata file sizes with the exact production encoder settings.",
      "premises": [
        "Tables 7 and 8 report achieved bits per link after LLP ordering under fixed BV settings."
      ],
      "source_pointer_ids": [
        "SP-007"
      ],
      "status": "DERIVED",
      "uncertainty": "Achieved bits per link is dataset- and encoder-dependent; metadata coefficients are not reported."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "one APM labeling per selected resolution plus linear-time ordering combinations and final BV encoding",
      "measurement_needed": "Record pass count and wall time by resolution on the declared fixture.",
      "premises": [],
      "source_pointer_ids": [
        "SP-004",
        "SP-006"
      ],
      "status": "SOURCED",
      "uncertainty": "The source does not give one closed-form wall-time bound because convergence and the resolution schedule vary."
    },
    "ram": {
      "assumptions": [],
      "expression": "graph_storage + 3 * n * integer_word_bytes",
      "measurement_needed": "Measure peak whole-process RSS, including graph representation and parallel runtime state.",
      "premises": [],
      "source_pointer_ids": [
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "Allocator, runtime, thread, and page-cache memory are not included by the source statement."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak temporary disk usage and transient allocation during labeling and encoding.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The source does not separate temporary files or transient buffers from the stated resident arrays."
    }
  },
  "source_domain": "compressed static web and social graph representations",
  "source_paper_ids": [
    "PAPER-1011.5425"
  ],
  "source_pointers": [
    {
      "claim_scope": "Initial-numbering sensitivity and the coordinate-free ordering objective",
      "locator_type": "SECTION",
      "locator_value": "Section 2, coordinate-free orderings; Section 3, contribution list",
      "page": 3,
      "paper_id": "PAPER-1011.5425",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "LLP composes APM clusterings produced at different resolution levels",
      "locator_type": "SECTION",
      "locator_value": "Section 6, first three paragraphs",
      "page": 6,
      "paper_id": "PAPER-1011.5425",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Stable order within a cluster and leader-order placement across clusters",
      "locator_type": "EQUATION",
      "locator_value": "Section 6, definition of the ordering pi_(k+1)",
      "page": 7,
      "paper_id": "PAPER-1011.5425",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Random multiresolution schedule, reuse of base labelings, and parallel task decomposition",
      "locator_type": "SECTION",
      "locator_value": "Section 6, gamma sampling and labeling reuse; Section 7",
      "page": 7,
      "paper_id": "PAPER-1011.5425",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Resident 3n-integer state, graph residency, and scalability conditions",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section 9, scalability and memory paragraph following Table 5",
      "page": 9,
      "paper_id": "PAPER-1011.5425",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "Few linear graph passes and compressed random-access objective",
      "locator_type": "SECTION",
      "locator_value": "Section 10, Conclusions and Future Work",
      "page": 10,
      "paper_id": "PAPER-1011.5425",
      "pointer_id": "SP-006"
    },
    {
      "claim_scope": "Compression after randomized input order and locality/similarity comparison",
      "locator_type": "TABLE",
      "locator_value": "Tables 7 and 8",
      "page": 13,
      "paper_id": "PAPER-1011.5425",
      "pointer_id": "SP-007"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-004",
      "SP-006"
    ],
    "text": "Graph adjacency is scanned during base-clustering passes, while cluster-combination work is linear in the node count rather than the arc count.",
    "uncertainty": "The source does not specify a storage-device streaming protocol."
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
        "SP-006"
      ],
      "text": "Compression behavior when fast successor access is not required is left for future investigation.",
      "uncertainty": "The paper gives only partial observations for maximum-compression settings."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-006",
        "SP-007"
      ],
      "text": "The graph is static, label propagation can make a few linear passes, and the downstream encoder benefits from node locality and adjacency-list similarity.",
      "uncertainty": "The evaluated graph families are web and social graphs."
    }
  ]
}
```
