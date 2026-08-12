# Decompose Diffusion Into Stages

- Pattern ID: `PAT-DECOMPOSE-DIFFUSION-INTO-STAGES`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus can bound the vertex and edge count of every admitted stage subgraph before loading it.",
      "Stage execution is serial unless concurrency is separately budgeted."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source proves a linear residual decomposition across smaller stage subgraphs.",
      "The source states that stage decomposition alone retains full-subgraph memory."
    ],
    "source_pointer_ids": [
      "SP-002",
      "SP-003"
    ],
    "text": "Knight Bus could quote peak stage memory from the largest admitted subgraph while separately quoting cumulative extraction and transfer work; it must not treat stage decomposition alone as a memory bound unless the linear residual decomposition is present.",
    "uncertainty": "The source does not supply a pre-run bound for overlap, total subgraph count, or host memory."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Extract the first bounded-depth subgraph, compute its diffusion and residuals, then extract and process later-stage subgraphs one residual seed at a time or in parallel before summing their contributions.",
    "uncertainty": "Parallel later-stage execution is identified but left for future experiments."
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
      "SP-005"
    ],
    "text": "The paper derives the decomposition algebraically and reports CPU/FPGA memory and latency results, but this campaign did not reproduce the arithmetic or hardware implementation.",
    "uncertainty": "Source benchmarks use a fixed two-stage configuration and selected fixtures."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-003"
    ],
    "text": "Each stage operates on a bounded-depth subgraph with local accumulated and residual score tables; later-stage subgraphs are keyed by residual-bearing vertices from the preceding stage.",
    "uncertainty": "Subgraph overlap and deduplication are not modeled."
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
      "text": "Stage decomposition without linear decomposition does not reduce the memory needed for the later diffusion because that stage still depends on the full L-hop subgraph.",
      "uncertainty": "The failure is the paper's stated motivation for splitting residual vectors by vertex."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-DECOMPOSE-DIFFUSION-INTO-STAGES",
  "falsifying_test": {
    "controlled_variables": [
      "walk decay",
      "total depth L",
      "stage depths",
      "seed vertex",
      "numeric precision"
    ],
    "failure_signal": "The sum of all staged residual contributions differs from the direct diffusion or peak resident subgraph exceeds the declared largest-stage bound under serial execution",
    "fixture": "A small undirected graph with a two-stage diffusion depth and residual mass on two first-stage vertices",
    "independent_oracle": "Direct single-stage diffusion using the same arithmetic",
    "scope": "Smallest mechanism-level falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Linearity of graph diffusion preserves the full L-step result when accumulated first-stage scores are combined with every nonzero residual-seeded later-stage diffusion according to Equation 8.",
    "uncertainty": "Exact preservation requires including every required residual component; pruning residual seeds introduces approximation."
  },
  "knight_bus_algorithm_families": [
    "PERSONALIZED_PAGERANK",
    "LOCAL_GRAPH_DIFFUSION"
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
    "text": "Split the diffusion depth into shorter stages, retain accumulated and residual score vectors, and use linear decomposition to execute later stages as separate residual-seeded diffusions on smaller extracted subgraphs.",
    "uncertainty": "The paper evaluates a two-stage instance and states that the decomposition extends to more terms."
  },
  "name": "Decompose Diffusion Into Stages",
  "pattern_id": "PAT-DECOMPOSE-DIFFUSION-INTO-STAGES",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Extracting and retaining the full L-hop subgraph for a local PPR query can exceed on-chip memory and incur high preparation latency.",
    "uncertainty": "The paper targets bounded-length local PPR on memory-constrained CPU/FPGA platforms."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Later-stage subgraphs and diffusion contributions are regenerated from residual-seed vertices, then aggregated into the final score result.",
    "uncertainty": "Repeated extraction may revisit overlapping topology."
  },
  "related_pattern_ids": [
    "PAT-BOUND-GLOBAL-SCORE-TABLE",
    "PAT-SELECT-HIGH-RESIDUAL-SUBGRAPHS"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-003",
      "SP-004"
    ],
    "text": "The currently processed subgraph, its adjacency table, local accumulated and residual scores, and bounded global top-score state are resident on the accelerator.",
    "uncertainty": "Host-side graph and control memory are additional."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Transfer volume is the sum of prepared stage-subgraph payloads and returned aggregate results; decomposition lowers per-stage payload but may repeat overlapping topology.",
      "measurement_needed": "Measure host-to-device and device-to-host bytes by stage and deduplicate overlap analytically.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "The source does not provide a symbolic overlap coefficient or host-storage byte count."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure original graph storage and determine whether extracted subgraphs are transient or persisted.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not specify a canonical persistent graph or subgraph artifact size for this mechanism."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Preprocessing performs bounded-depth BFS extraction and neighbor-list reorganization for the first stage and each processed residual seed.",
      "measurement_needed": "Record BFS extraction time, nodes, edges, and duplicate topology per stage.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-003"
      ],
      "status": "SOURCED",
      "uncertainty": "Extraction latency grows with selected subgraph sizes and overlap."
    },
    "ram": {
      "assumptions": [],
      "expression": "Peak accelerator RAM is proportional to the currently processed bounded-depth subgraph and its local score tables rather than the complete L-hop subgraph when linear decomposition is applied.",
      "measurement_needed": "Measure peak host RSS and accelerator memory separately for each stage and concurrency level.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002",
        "SP-003",
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "Host memory, concurrent subgraphs, and implementation metadata are not included in one portable expression."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "Temporary stage state includes one subgraph table, accumulated scores, residual scores, and stage contribution buffers.",
      "measurement_needed": "Measure temporary bytes at each permitted stage concurrency.",
      "premises": [],
      "source_pointer_ids": [
        "SP-003",
        "SP-004"
      ],
      "status": "SOURCED",
      "uncertainty": "Peak multiplicity depends on whether residual stages execute serially or concurrently."
    }
  },
  "source_domain": "local personalized PageRank diffusion",
  "source_paper_ids": [
    "PAPER-2104.09616"
  ],
  "source_pointers": [
    {
      "claim_scope": "Stage/subgraph execution model and memory objective",
      "locator_type": "SECTION",
      "locator_value": "Section IV-A, Overall Idea",
      "page": 2,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Stage and linear decomposition identity",
      "locator_type": "EQUATION",
      "locator_value": "Equations 6-8 and Sections IV-B-IV-C",
      "page": 3,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Accumulated/residual state and staged subgraph memory",
      "locator_type": "FIGURE",
      "locator_value": "Figure 3",
      "page": 4,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "SP-003"
    },
    {
      "claim_scope": "Subgraph memory model and reported memory evaluation",
      "locator_type": "TABLE",
      "locator_value": "Table II and Section VI-B",
      "page": 5,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "SP-004"
    },
    {
      "claim_scope": "Precision-latency boundary and evaluated two-stage configuration",
      "locator_type": "FIGURE",
      "locator_value": "Figure 7 and Section VII",
      "page": 6,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "SP-005"
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
    "text": "The host prepares bounded-depth subgraphs and transfers them to the accelerator for one stage computation rather than transferring the entire L-hop subgraph at once.",
    "uncertainty": "Total transfer volume depends on the number and overlap of stage subgraphs."
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
      "text": "The paper does not benchmark more than two stages or concurrent residual-subgraph execution.",
      "uncertainty": "The algebra extends, but scheduling, overlap, and resource behavior remain unmeasured."
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
      "text": "The exact decomposition works for the paper's linear graph-diffusion formulation when each selected stage subgraph fits the target memory and all required residual contributions are included.",
      "uncertainty": "The evaluated implementation uses a simple undirected graph and bounded walk length."
    }
  ]
}
```
