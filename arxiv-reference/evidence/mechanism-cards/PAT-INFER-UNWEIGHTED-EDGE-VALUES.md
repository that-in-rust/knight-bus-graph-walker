# Infer Unweighted Edge Values

- Pattern ID: `PAT-INFER-UNWEIGHTED-EDGE-VALUES`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus validates unweighted semantics and a noncolliding marker before execution.",
      "The estimator receives measured or exact padding P for the chosen C and sorting scope."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source removes the value array only because edge presence determines its entries.",
      "The source storage expression retains padding and chunk metadata."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "For admitted unweighted algebraic traversals, Knight Bus can subtract stored edge-value bytes only after verifying that the selected semiring can regenerate every edge value and that padding P and all dense vectors remain in the quote.",
    "uncertainty": "The source does not provide a whole-process byte estimator or external-I/O model."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "For each chunk column, vector-load column entries, derive an edge/padding mask, synthesize values, gather frontier entries, and execute the selected semiring operation.",
    "uncertainty": "Gather cost and vector width vary by architecture."
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
      "SP-003",
      "SP-004"
    ],
    "text": "The mechanism has explicit kernel pseudocode, storage equations, and cross-architecture benchmarks, but this campaign did not reproduce the implementation.",
    "uncertainty": "The source's measured speed and storage effects remain unverified in this campaign."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Column IDs and padding markers remain in SIMD-height, column-major chunks, accompanied by chunk offsets and lengths but no per-entry value array.",
    "uncertainty": "Padding amount depends on row-degree grouping and sorting scope."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "The value array cannot be omitted when distinct edge weights or other per-edge numeric values affect the matrix operation.",
      "uncertainty": "Some restricted weighted domains might support a different dictionary or generated-value scheme, which the paper does not study."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-INFER-UNWEIGHTED-EDGE-VALUES",
  "falsifying_test": {
    "controlled_variables": [
      "vertex ordering",
      "SIMD chunk height C",
      "sorting scope",
      "padding marker",
      "BFS root",
      "semiring"
    ],
    "failure_signal": "The value-free kernel produces a different frontier or output, accepts a padding entry as an edge, or stores more cells than the declared expression",
    "fixture": "A tiny unweighted graph whose chunk layout contains both real edges and padding markers",
    "independent_oracle": "CSR BFS using explicit unit values",
    "scope": "Smallest mechanism-level falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A non-marker column entry denotes an edge with implicit unit value, while the reserved marker denotes chunk padding rather than an edge.",
    "uncertainty": "The marker must not collide with valid vertex identifiers."
  },
  "knight_bus_algorithm_families": [
    "BFS",
    "UNWEIGHTED_GRAPHBLAS_TRAVERSAL"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Remove the value array, load only column entries, compare them to the padding marker with a vector instruction, and synthesize the semiring value vector in registers.",
    "uncertainty": "The shown synthesized values target the paper's BFS semirings."
  },
  "name": "Infer Unweighted Edge Values",
  "pattern_id": "PAT-INFER-UNWEIGHTED-EDGE-VALUES",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "General sparse-matrix formats store a value for every adjacency entry even though an unweighted graph needs only edge presence, duplicating information already represented by column indices.",
    "uncertainty": "The redundancy argument is specific to unweighted adjacency matrices."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Unit or semiring-identity values and padding sentinels are regenerated from the marker comparison for every processed vector chunk.",
    "uncertainty": "The compute-versus-bandwidth trade-off is architecture-dependent."
  },
  "related_pattern_ids": [
    "PAT-SKIP-FINALIZED-VECTOR-CHUNKS"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "The chunked column array, chunk offsets and lengths, frontier/output vectors, and semiring-specific filter or parent vectors reside in memory during BFS.",
    "uncertainty": "Whole-process runtime and library state are not included in the format expression."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure memory bytes and external bytes separately on the target execution medium.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper demonstrates reduced memory transfer but does not bound external-storage I/O."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "Serialized representation retains column IDs, P padding markers, chunk starts, and chunk lengths while omitting all stored edge values.",
      "measurement_needed": "Measure serialized bytes including headers, alignment, and chunk metadata.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "The paper reports cells rather than a file-format byte layout."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Preprocessing builds the chunk layout and may sort rows within scope sigma; sorting and build cost are one-time and amortized over repeated BFS runs in the evaluation.",
      "measurement_needed": "Measure format-build and sorting time and divide by the expected query count.",
      "premises": [],
      "source_pointer_ids": [
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "Amortization depends on the number of queries and graph reuse."
    },
    "ram": {
      "assumptions": [],
      "expression": "SlimSell representation size = 2m + 2n/C + P cells in the paper's notation, excluding BFS vectors and runtime overhead.",
      "measurement_needed": "Record cell width, m, n, C, P, all BFS vectors, and peak process RSS.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "Cell width, graph-edge convention, and padding P must be supplied for a concrete byte estimate."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "Kernel-local temporary state includes one vector of column entries, one marker mask, one synthesized-value vector, and gathered frontier values per active SIMD operation.",
      "measurement_needed": "Inspect generated kernel resource use or measure spills on the target architecture.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "Compiler register allocation and spill behavior are not bounded."
    }
  },
  "source_domain": "SIMD-oriented algebraic breadth-first search",
  "source_paper_ids": [
    "PAPER-2010.09913"
  ],
  "source_pointers": [
    {
      "claim_scope": "Value-array elimination for unweighted adjacency matrices",
      "locator_type": "SECTION",
      "locator_value": "Section III-B, Reducing Storage Complexity with SlimSell",
      "page": 6,
      "paper_id": "PAPER-2010.09913",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Vectorized derivation of values from column IDs and padding markers",
      "locator_type": "ALGORITHM",
      "locator_value": "Listing 6, SlimSell kernel",
      "page": 6,
      "paper_id": "PAPER-2010.09913",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Storage expressions and padding condition",
      "locator_type": "TABLE",
      "locator_value": "Table III and Equation 3",
      "page": 7,
      "paper_id": "PAPER-2010.09913",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Preprocessing amortization and storage evaluation",
      "locator_type": "SECTION",
      "locator_value": "Sections IV-D and IV-E",
      "page": 10,
      "paper_id": "PAPER-2010.09913",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Mechanism applicability beyond Sell-C-sigma",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section V, paragraph on applicability to other sparse formats",
      "page": 11,
      "paper_id": "PAPER-2010.09913",
      "pointer_id": "SP-005"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "The matrix kernel streams chunked column entries from memory and avoids the corresponding stream of value-array entries.",
    "uncertainty": "The paper evaluates memory-resident CPU/GPU data rather than external storage."
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
        "SP-005"
      ],
      "text": "The paper leaves distributed-memory and external-storage variants as future work and does not establish their communication or I/O behavior.",
      "uncertainty": "Only in-memory CPU, manycore, and GPU implementations are benchmarked."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-005"
      ],
      "text": "The mechanism works for unweighted graphs whose sparse-matrix values are determined entirely by edge presence and whose vertex IDs reserve a distinct padding marker.",
      "uncertainty": "Semiring-specific synthesis still has to preserve the algorithm's algebra."
    }
  ]
}
```
