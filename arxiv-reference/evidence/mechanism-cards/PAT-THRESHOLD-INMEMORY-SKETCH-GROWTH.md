# Threshold Inmemory Sketch Growth

- Pattern ID: `PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus implements the same exact total-BFS semantics and V-BFS threshold logic.",
      "Its artifact can be scanned sequentially in the required adjacency order."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH:invariant",
      "PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH:ram",
      "PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH:io"
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-005",
      "SP-006"
    ],
    "text": "A Knight Bus exact-BFS admission model could expose K as a sketch-capacity control and reject a run when the fixed sketch plus two per-node attributes and runtime overhead exceed RAM, while reporting that sequential scan count remains data-dependent up to the source's LLSP(G) bound.",
    "uncertainty": "Whole-process coefficients, observed scan count, and the applicability of total-BFS semantics to Knight Bus workloads are not established."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-005"
    ],
    "text": "Perform one preparation scan, then repeatedly scan ER sequentially; threshold each edge, invoke EP-Reduce whenever the sketch fills, update frontier thresholds, and continue until no further reduction is required.",
    "uncertainty": "The number of reducer invocations within one scan depends on n, K, and admitted-edge density."
  },
  "confidence_rationale": {
    "assumptions": [
      "The benchmark implementation conforms to Algorithm 3."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Algorithm 3 fixes the sketch-cap transition.",
      "Theorem 5.4 states correctness and the iteration allowance.",
      "Figure 12 reports source benchmark behavior across K values."
    ],
    "source_pointer_ids": [
      "SP-003",
      "SP-005",
      "SP-006"
    ],
    "text": "Confidence is moderate because the algorithm, cap, proof, and memory-sensitivity benchmarks are explicit, but the source has a page-13 cost-label ambiguity and G05 did not reproduce or inspect the implementation.",
    "uncertainty": "No independent reproduction, code inspection, or whole-process accounting was performed."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-003"
    ],
    "text": "The graph is prepared as adjacency-list edge segments; resident sketch edges occupy fixed arrays with each node's outgoing sketch neighbors contiguous, while per-node B and P attributes encode current breadth-first position and parent.",
    "uncertainty": "Integer widths and complete array metadata are implementation choices."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [
        "A long simple path and adversarial edge order can realize repeated late corrections."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "Theorem 5.4 permits up to LLSP(G) outer iterations.",
        "The reducer is invoked whenever the fixed sketch fills."
      ],
      "source_pointer_ids": [
        "SP-005"
      ],
      "text": "The mechanism loses its practical scan bound on graphs whose V-BFS corrections force close to the source's LLSP(G) worst-case number of iterations and admit many edges before each reduction.",
      "uncertainty": "The paper proves a worst-case allowance but does not exhibit a measured worst-case fixture."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH",
  "falsifying_test": {
    "controlled_variables": [
      "node count",
      "edge order",
      "K",
      "integer width",
      "neighbor ordering"
    ],
    "failure_signal": "The returned parent/order differs from the oracle, resident sketch slots exceed the configured cap, or outer scans exceed the stated LLSP(G) allowance",
    "fixture": "A directed path with backward distractor edges stored in an adversarial order, run with the smallest K that admits the fixed sketch arrays",
    "independent_oracle": "A conventional in-memory BFS forest with deterministic neighbor order",
    "scope": "Smallest mechanism test description only; no G09 experiment packet exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "The resident edge list E plus resident tree portion never exceeds the configured (K+1)n edge-slot cap, and every admitted edge either satisfies the current threshold region or is a V-BFS edge that lowers the threshold.",
    "uncertainty": "The cap covers the named sketch arrays, not unreported whole-process overhead."
  },
  "knight_bus_algorithm_families": [
    "BFS",
    "UNWEIGHTED_SHORTEST_PATHS",
    "REACHABILITY",
    "CONNECTED_COMPONENTS"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003",
      "SP-004"
    ],
    "text": "Scan the reduced edge stream, use F[i] and current breadth-first positions to reject edges that cannot affect the present reduction region, admit qualifying edges until the fixed sketch cap is reached, then run EP-Reduce and reset the edge batch.",
    "uncertainty": "The threshold logic is specific to the paper's V-BFS characterization."
  },
  "name": "Threshold Inmemory Sketch Growth",
  "pattern_id": "PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Semi-external BFS must process a disk-resident directed graph while retaining only a linear-size sketch, and repeatedly enlarging and reducing that sketch can dominate CPU and I/O cost.",
    "uncertainty": "The paper computes a total BFS forest/order, not only one-source reachability."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-004"
    ],
    "text": "EP-Reduce rebuilds the resident breadth-first tree portion and its BON order from the current bounded sketch; F[i], FR, FC, and FCC are recomputed as iterations advance.",
    "uncertainty": "The source does not isolate recomputation CPU from edge-admission CPU."
  },
  "related_pattern_ids": [
    "PAT-PRUNE-SETTLED-SEARCH-STATE"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003",
      "SP-004"
    ],
    "text": "Resident state includes the bounded E and partial-tree edge arrays, two attributes per node, the BON breadth-first-order array, threshold variables, and temporary reducer queue indices.",
    "uncertainty": "The paper's minimum-memory statement omits JVM and operating-system overhead."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "one initial graph scan plus repeated sequential scans of reduced ER, with at most LLSP(G) outer iterations",
      "measurement_needed": "Measure physical sequential bytes read and written per outer iteration and compare with logical ER size.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-005"
      ],
      "status": "SOURCED",
      "uncertainty": "The page-13 prose orders the displayed time and I/O expressions ambiguously; this card therefore does not relabel those formulas."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "adjacency-list input segments + current reduced ER + disk-resident tree-edge state ET",
      "measurement_needed": "Measure retained and peak on-disk bytes across edge-stream rewrites.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "The source does not provide a peak storage-amplification bound when old and new reduced streams coexist."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "scan G, divide edges into adjacency-list segments, initialize the forest and node attributes, and build initial ER",
      "measurement_needed": "Measure initial scan time, bytes written, and peak preparation scratch.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "The paper does not isolate preparation wall time or write amplification from total execution."
    },
    "ram": {
      "assumptions": [],
      "expression": "(K + 1) * n * edge_slot_bytes + 2 * n * node_attribute_bytes + BON_and_array_metadata_bytes",
      "measurement_needed": "Measure peak process RSS and separately attribute sketch arrays, BON, runtime, and page cache.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-003",
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "The source's MMSR counts logical attributes and sketch capacity, not complete JVM RSS."
    },
    "temporary_storage": {
      "assumptions": [
        "ER rewrite is materialized rather than updated in place."
      ],
      "expression": "current_Enext_bytes + rewrite_staging_bytes + reducer_transient_bytes",
      "measurement_needed": "Measure peak temporary disk and transient heap during one ER reduction cycle.",
      "premises": [
        "Algorithm 3 constructs Enext while scanning ER and repeatedly resets the resident edge batch."
      ],
      "source_pointer_ids": [
        "SP-003"
      ],
      "status": "DERIVED",
      "uncertainty": "The source does not state file-generation overlap or reclamation timing."
    }
  },
  "source_domain": "semi-external exact breadth-first search on directed graphs",
  "source_paper_ids": [
    "PAPER-2507.12925"
  ],
  "source_pointers": [
    {
      "claim_scope": "Two per-node attributes, bounded sketch, and three EP-BFS cost classes",
      "locator_type": "SECTION",
      "locator_value": "Section 5, Overview",
      "page": 8,
      "paper_id": "PAPER-2507.12925",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Threshold F[i], edge-admission cases, contiguous sketch neighborhoods, and reduced edge stream",
      "locator_type": "FIGURE",
      "locator_value": "Figure 5 and surrounding Section 5 paragraphs",
      "page": 9,
      "paper_id": "PAPER-2507.12925",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Initial adjacency-list preparation, iterative threshold filtering, fixed sketch cap, and reducer invocation",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm 3, lines 1-24",
      "page": 10,
      "paper_id": "PAPER-2507.12925",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Queue-based reconstruction of the bounded in-memory tree sketch",
      "locator_type": "ALGORITHM",
      "locator_value": "Procedure 1, EP-Reduce",
      "page": 11,
      "paper_id": "PAPER-2507.12925",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Correctness and at-most-LLSP(G) iteration statement",
      "locator_type": "THEOREM",
      "locator_value": "Theorem 5.4 and following cost paragraph",
      "page": 13,
      "paper_id": "PAPER-2507.12925",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "Source benchmark behavior as K varies, including small available sketch capacity",
      "locator_type": "FIGURE",
      "locator_value": "Figure 12 and Section 6.8",
      "page": 20,
      "paper_id": "PAPER-2507.12925",
      "pointer_id": "SP-006"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "The initial graph segments and later reduced edge stream ER are read sequentially; edges not admitted to the current sketch remain disk-resident for later scans.",
    "uncertainty": "The source does not report OS cache residency separately from physical device traffic."
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
        "SP-005"
      ],
      "text": "The analysis does not establish behavior for dynamic graph updates during a run or for an input that cannot be rewritten into the required adjacency-list segments.",
      "uncertainty": "The formal problem and correctness proof assume one static disk-resident graph."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-006"
      ],
      "text": "The graph is static and directed, sequential scans are affordable, and a linear-size sketch with configurable K fits even though the full edge set does not.",
      "uncertainty": "The experiments cover the paper's Java implementation and selected HDD/SSD systems."
    }
  ]
}
```
