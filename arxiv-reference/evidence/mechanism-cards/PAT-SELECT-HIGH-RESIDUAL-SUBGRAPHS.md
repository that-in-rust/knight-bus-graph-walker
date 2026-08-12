# Select High Residual Subgraphs

- Pattern ID: `PAT-SELECT-HIGH-RESIDUAL-SUBGRAPHS`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus exposes the selected count or cutoff and can record processed residual seeds.",
      "Approximate local PPR is an explicitly permitted algorithm mode."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source orders candidate later-stage seeds by residual score.",
      "The source reports fixture-specific precision and latency curves rather than a deterministic error bound."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-003",
      "SP-004"
    ],
    "text": "A Knight Bus residual-guided PPR run can bound selected later-stage count or residual cutoff as a declared approximation control, but its receipt must report the selected scope and must not promise top-k precision from prefix length alone.",
    "uncertainty": "Admission-time precision remains unknown for graph and seed distributions outside the evaluated source fixtures."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-006"
    ],
    "text": "Select next-stage seeds in descending residual order, prepare each selected seed's bounded subgraph, execute its diffusion, and add its contribution to the accumulated result.",
    "uncertainty": "The paper does not specify a stable tie rule or an adaptive stopping certificate."
  },
  "confidence_rationale": {
    "assumptions": [
      "The local PDF and extracted text accurately represent the evaluated paper version.",
      "No independent campaign reproduction or code inspection has occurred."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source states descending residual selection and gives the additive decomposition.",
      "The source reports precision-selection and precision-latency measurements."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-003",
      "SP-004"
    ],
    "text": "The structural ordering rule and additive contributions are explicit in Section IV-D and Equation 8, while precision behavior is supported only by the paper's benchmark curves; this campaign did not independently reproduce the result or inspect implementation code.",
    "uncertainty": "Selection implementation details and generalization beyond the reported fixtures remain unverified."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-005"
    ],
    "text": "First-stage vertices carry residual scores; each selected vertex identifies a bounded-depth later-stage subgraph whose local diffusion contribution is accumulated into the query result.",
    "uncertainty": "The source does not define a deduplicated representation for overlapping selected subgraphs."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-004"
      ],
      "text": "In the reported experiments, processing more next-stage nodes increased work and reduced speedup; CPU cases could become slower than the baseline when higher measured precision was sought.",
      "uncertainty": "This is a measured workload boundary, not a universal crossover point."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-SELECT-HIGH-RESIDUAL-SUBGRAPHS",
  "falsifying_test": {
    "controlled_variables": [
      "seed vertex",
      "stage depths",
      "decay factor",
      "residual scores",
      "selected prefix length",
      "numeric precision"
    ],
    "failure_signal": "Selected seeds are not processed in descending residual order, a selected seed's contribution is not additive against the Equation 8 oracle, or execution processes more seeds than the declared prefix",
    "fixture": "A small graph whose first stage yields three distinct residual-bearing vertices and bounded later-stage subgraphs",
    "independent_oracle": "Exact Equation 8 contribution vectors computed separately for all three residual seeds using the same arithmetic",
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
    "text": "Candidate next-stage vertices are ordered by descending residual score, and each processed vertex contributes an additive residual-seeded diffusion term in Equation 8.",
    "uncertainty": "The source does not specify a tie rule for vertices with equal residual scores."
  },
  "knight_bus_algorithm_families": [
    "PERSONALIZED_PAGERANK",
    "APPROXIMATE_LOCAL_GRAPH_DIFFUSION"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-003"
    ],
    "text": "Rank residual-bearing first-stage vertices by residual score and execute later-stage subgraph diffusion only for a configured high-residual prefix, omitting the remaining additive terms.",
    "uncertainty": "Omitting residual-seeded terms makes the result approximate, and the paper evaluates the resulting precision empirically."
  },
  "name": "Select High Residual Subgraphs",
  "pattern_id": "PAT-SELECT-HIGH-RESIDUAL-SUBGRAPHS",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Exact evaluation of Equation 8 requires a later-stage diffusion for every nonzero first-stage residual vertex, which can increase computation latency.",
    "uncertainty": "The cost depends on the number and topology of residual-seeded subgraphs."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-006"
    ],
    "text": "Each selected residual vertex initiates a separate bounded-depth subgraph preparation and diffusion contribution.",
    "uncertainty": "Overlapping topology can be revisited across selected seeds, but the paper does not quantify that repetition."
  },
  "related_pattern_ids": [
    "PAT-DECOMPOSE-DIFFUSION-INTO-STAGES"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-005"
    ],
    "text": "The residual scores and the currently processed selected subgraph's local accumulated and residual score state are retained during later-stage evaluation.",
    "uncertainty": "The paper does not isolate host-side residual-ordering memory."
  },
  "resource_model": {
    "io": {
      "assumptions": [
        "Each selected subgraph is communicated once.",
        "No cross-seed topology cache suppresses repeated bytes."
      ],
      "expression": "Selected-subgraph communication is the sum of payload bytes for processed residual seeds; exact bytes and overlap coefficient UNKNOWN",
      "measurement_needed": "Record selected-seed count, unique topology, repeated topology, and host-device bytes.",
      "premises": [
        "Only selected residual seeds trigger later-stage subgraph computation.",
        "The source implementation prepares subgraphs on the CPU and communicates them to the FPGA."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-006"
      ],
      "status": "DERIVED",
      "uncertainty": "Selected subgraphs may overlap, and the implementation's transfer encoding is not specified."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Determine whether residual rankings or extracted subgraphs are persisted and measure their encoded bytes.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not specify a persistent residual ranking or selected-subgraph cache for this mechanism."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure residual selection time, ordering scratch, BFS extraction time, and duplicate topology by seed.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not specify the algorithm, complexity, or scratch space used to produce descending residual order or reuse BFS extraction across selected seeds."
    },
    "ram": {
      "assumptions": [
        "Selected subgraphs execute one at a time.",
        "Completed subgraph-local state is released before the next selected seed."
      ],
      "expression": "Under sequential selected-seed processing: residual-order state plus the largest active selected subgraph and its local score state; exact bytes UNKNOWN",
      "measurement_needed": "Measure host RSS and accelerator resident bytes by selected-seed concurrency and maximum active subgraph.",
      "premises": [
        "Equation 8 separates later-stage work into residual-seeded contributions.",
        "Figure 3 identifies local accumulated and residual state for a diffusion."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-002",
        "SP-005"
      ],
      "status": "DERIVED",
      "uncertainty": "Concurrent execution, residual-order representation, host graph state, and allocator overhead can raise peak RAM."
    },
    "temporary_storage": {
      "assumptions": [
        "Selected subgraphs execute sequentially.",
        "The ordering workspace remains live while selected seeds are processed."
      ],
      "expression": "Residual ordering workspace plus one active selected subgraph's accumulated and residual score state under sequential execution; exact bytes UNKNOWN",
      "measurement_needed": "Measure peak selection scratch and subgraph-local temporary bytes separately.",
      "premises": [
        "Selection requires residual-bearing vertices to be considered in descending score order.",
        "Each active diffusion maintains local accumulated and residual scores."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-005"
      ],
      "status": "DERIVED",
      "uncertainty": "The source does not identify the selection data structure or temporary allocation policy."
    }
  },
  "source_domain": "Approximate local personalized PageRank by residual-guided staged graph diffusion",
  "source_paper_ids": [
    "PAPER-2104.09616"
  ],
  "source_pointers": [
    {
      "claim_scope": "Residual sparsity observation and descending-residual next-stage selection rule",
      "locator_type": "SECTION",
      "locator_value": "Section IV-D, Sparsity of the PPR Vector",
      "page": 3,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Additive decomposition into the first-stage term and residual-seeded later-stage contributions",
      "locator_type": "EQUATION",
      "locator_value": "Equation 8",
      "page": 3,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Measured residual distribution and selection-ratio versus top-k precision on G1 through G3 over sampled runs",
      "locator_type": "FIGURE",
      "locator_value": "Figure 6 and Section VI-C",
      "page": 5,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Measured precision-latency behavior across six evaluated graphs and sampled seed nodes",
      "locator_type": "FIGURE",
      "locator_value": "Figure 7 and continuation of Section VI-C",
      "page": 6,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Accumulated and residual score state within staged graph diffusion",
      "locator_type": "FIGURE",
      "locator_value": "Figure 3",
      "page": 4,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "SP-005"
    },
    {
      "claim_scope": "CPU preparation of bounded subgraphs by BFS and CPU-FPGA communication",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section V opening implementation paragraph before Section V-A",
      "page": 4,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "SP-006"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-006"
    ],
    "text": "The host prepares and communicates only the selected residual-seeded subgraphs for later-stage accelerator diffusion; omitted residual seeds do not trigger those subgraph computations.",
    "uncertainty": "Exact transfer bytes depend on selected topology and any implementation reuse."
  },
  "unknown_when": [
    {
      "assumptions": [
        "A deterministic guarantee would require an explicit theorem, bound, or certified stopping rule in the inspected paper."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "Section IV-D motivates residual ordering intuitively.",
        "Figures 6 and 7 report sampled empirical precision curves."
      ],
      "source_pointer_ids": [
        "SP-001",
        "SP-003",
        "SP-004"
      ],
      "text": "No deterministic error bound in the inspected source maps a residual cutoff or prefix length to top-k precision for an unseen graph and seed.",
      "uncertainty": "A guarantee may exist outside this assigned source, but it is not evidence for this card."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-004"
      ],
      "text": "On the paper's evaluated graphs and sampled seeds, residual scores were concentrated and processing a small selected fraction produced a useful measured top-k precision and latency trade-off.",
      "uncertainty": "The reported behavior is empirical and bounded to the paper's fixtures, parameters, and implementation."
    }
  ]
}
```
