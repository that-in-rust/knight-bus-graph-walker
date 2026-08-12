# Materialize Each Pipeline Stage

- Pattern ID: `PAT-MATERIALIZE-EACH-PIPELINE-STAGE`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus executes or prepares graphs through comparable pipeline phases.",
      "Stage barriers or equivalent accounting can attribute resource peaks without changing required semantics."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source defines explicit materialized stage boundaries.",
      "The source's serial results expose different I/O and memory effects by stage.",
      "The source distinguishes in-memory and out-of-core sort based on fit."
    ],
    "source_pointer_ids": [
      "SP-055",
      "SP-056",
      "SP-058"
    ],
    "text": "Knight Bus admission and receipts could be phase-complete: quote and report ingest, sort, graph preparation, retained artifacts, and iterative algorithm state separately instead of treating the analytics loop as the whole job.",
    "uncertainty": "The benchmark proposal does not define hard ceilings, overlapping-stage accounting, or a complete correctness oracle."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-055",
      "SP-056"
    ],
    "text": "The schedule is a strict sequence of generate/write, read/sort/write, read/filter/build, and repeated sparse matrix-vector updates.",
    "uncertainty": "Parallel implementations add decomposition, aggregation, and broadcast behavior."
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
      "SP-057",
      "SP-058",
      "SP-059"
    ],
    "text": "The paper defines each kernel mathematically and reports serial implementations and stage measurements, but it is a proposal with unresolved validation choices and this campaign did not reproduce the results.",
    "uncertainty": "Grade C supports the reported benchmark observations, not a complete production contract."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-055",
      "SP-056"
    ],
    "text": "Edges are materialized as text pairs, then sorted by source vertex; graph preparation creates a sparse adjacency matrix, degree vectors, filtered columns, and normalized rows; PageRank uses an N-element rank vector.",
    "uncertainty": "File count and implementation language are free parameters."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-056",
        "SP-057"
      ],
      "text": "An in-memory sort cannot be used when edge arrays do not fit RAM, and the proposed fixed-iteration PageRank with omitted dangling-node correction is not a general production convergence or correctness contract.",
      "uncertainty": "The source frames these as benchmark choices and open refinements."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-MATERIALIZE-EACH-PIPELINE-STAGE",
  "falsifying_test": {
    "controlled_variables": [
      "graph generator seed",
      "edge format",
      "stage barriers",
      "iteration count",
      "damping factor",
      "cache state"
    ],
    "failure_signal": "A stage consumes the wrong predecessor artifact, a checksum or rank vector differs from the oracle, or measured stage peaks and I/O cannot be reconciled with the phase receipt",
    "fixture": "A small graph whose unsorted edges, sorted edges, prepared adjacency, and fixed-iteration PageRank vector can each be independently checksummed",
    "independent_oracle": "A reference sort, sparse-matrix builder, and dense eigenvector or independently implemented fixed-iteration PageRank check",
    "scope": "Smallest pipeline-accounting and correctness falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-055"
    ],
    "text": "Each mathematically defined kernel completes before the next begins, and its explicit output is the next kernel's input.",
    "uncertainty": "The barrier is a benchmark-design choice and may differ from overlapped production pipelines."
  },
  "knight_bus_algorithm_families": [
    "PAGERANK",
    "ITERATIVE_SPARSE_MATRIX_VECTOR",
    "GRAPH_INGEST_PIPELINES",
    "CENTRALITY"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-055",
      "SP-056"
    ],
    "text": "Generate and persist graph edges, read-sort-rewrite them, read and prepare a normalized sparse adjacency structure, then run a fixed PageRank iteration count, timing each complete stage independently.",
    "uncertainty": "The proposed PageRank kernel intentionally omits some dangling-node handling."
  },
  "name": "Materialize Each Pipeline Stage",
  "pattern_id": "PAT-MATERIALIZE-EACH-PIPELINE-STAGE",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-055",
      "SP-056"
    ],
    "text": "Timing only the iterative graph kernel hides the file generation, read, sort, rewrite, sparse-matrix construction, filtering, and communication stages present in real data systems.",
    "uncertainty": "The paper proposes a benchmark rather than a production execution contract."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-056"
    ],
    "text": "PageRank repeatedly recomputes the rank vector from the normalized sparse adjacency for a fixed number of iterations chosen to make timing less data-dependent.",
    "uncertainty": "Fixed iterations are not a convergence guarantee for a production answer."
  },
  "related_pattern_ids": [
    "PAT-RELABEL-VERTICES-FOR-LOCALITY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-055",
      "SP-056",
      "SP-057"
    ],
    "text": "The active stage retains either sortable edge arrays, the sparse adjacency and degree state, or the adjacency plus rank and damping vectors.",
    "uncertainty": "Peak overlap between input and output structures is implementation-dependent."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Generation writes edge files; sort reads and rewrites all edges; graph preparation reads sorted edges; measured stage rates reflect storage and cache behavior.",
      "measurement_needed": "Measure physical bytes read and written with cold-cache indicators per stage.",
      "premises": [],
      "source_pointer_ids": [
        "SP-055",
        "SP-058"
      ],
      "status": "SOURCED",
      "uncertainty": "Exact bytes depend on text encoding, file count, sort strategy, and caching."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "Kernel 0 persists unsorted edge text and Kernel 1 persists a sorted edge-text materialization for the next stage.",
      "measurement_needed": "Measure unsorted, sorted, and retained-final artifact bytes plus file-system overhead.",
      "premises": [],
      "source_pointer_ids": [
        "SP-055"
      ],
      "status": "SOURCED",
      "uncertainty": "The paper does not provide a general storage-amplification formula or require retention of both versions after completion."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "Before PageRank, the pipeline sorts edges, builds a sparse adjacency matrix, computes degrees, filters selected columns, and normalizes rows; every stage is timed.",
      "measurement_needed": "Record wall time, CPU time, and peak state for each preparation kernel.",
      "premises": [],
      "source_pointer_ids": [
        "SP-055",
        "SP-056"
      ],
      "status": "SOURCED",
      "uncertainty": "The benchmark's filters are specific and do not represent every production preparation pipeline."
    },
    "ram": {
      "assumptions": [],
      "expression": "The benchmark suggests choosing a graph whose edge data consume about one quarter of available RAM; stage memory includes sortable edge arrays, sparse adjacency, degree vectors, and rank vectors, with in-memory versus out-of-core sort selected by fit.",
      "measurement_needed": "Measure peak RSS and retained bytes separately for every stage.",
      "premises": [],
      "source_pointer_ids": [
        "SP-055",
        "SP-056",
        "SP-057"
      ],
      "status": "SOURCED",
      "uncertainty": "The one-quarter target is a benchmark sizing choice, not a peak-RSS proof."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak scratch files and temporary allocations for every kernel.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "External-sort runs, sparse-builder scratch, and intermediate rank buffers are not separated into one temporary-storage term."
    }
  },
  "source_domain": "holistic big-data PageRank benchmarking",
  "source_paper_ids": [
    "PAPER-1603.01876"
  ],
  "source_pointers": [
    {
      "claim_scope": "Four-stage pipeline, full-stage barriers, file formats, RAM target, and sort",
      "locator_type": "SECTION",
      "locator_value": "Section IV and Sections IV.A-IV.B",
      "page": 4,
      "paper_id": "PAPER-1603.01876",
      "pointer_id": "SP-055"
    },
    {
      "claim_scope": "Out-of-core boundary, graph preparation, communication, and fixed PageRank iterations",
      "locator_type": "SECTION",
      "locator_value": "Sections IV.B-IV.D",
      "page": 5,
      "paper_id": "PAPER-1603.01876",
      "pointer_id": "SP-056"
    },
    {
      "claim_scope": "Correctness oracle, omitted dangling term, fixtures, hardware, and file system",
      "locator_type": "SECTION",
      "locator_value": "Section IV serial implementations and results",
      "page": 6,
      "paper_id": "PAPER-1603.01876",
      "pointer_id": "SP-057"
    },
    {
      "claim_scope": "Stage-level serial measurements and bottleneck interpretations",
      "locator_type": "FIGURE",
      "locator_value": "Figures 4-7 and accompanying paragraphs",
      "page": 7,
      "paper_id": "PAPER-1603.01876",
      "pointer_id": "SP-058"
    },
    {
      "claim_scope": "Unresolved correctness outputs, generator, dangling nodes, and future parallel models",
      "locator_type": "SECTION",
      "locator_value": "Section V, Summary and Next Steps",
      "page": 8,
      "paper_id": "PAPER-1603.01876",
      "pointer_id": "SP-059"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-055",
      "SP-058"
    ],
    "text": "Edge files are written by generation, read and rewritten by sorting, and read again by graph preparation.",
    "uncertainty": "The serial runs use a Lustre file system and may receive storage-cache effects."
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
        "SP-059"
      ],
      "text": "The paper leaves generator determinism, final validation outputs, dangling-node treatment, local-versus-shared storage, and parallel performance models unresolved.",
      "uncertainty": "The proposal is incomplete for a canonical production workload receipt."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-055",
        "SP-057",
        "SP-058"
      ],
      "text": "Implementations preserve the mathematical stage inputs and outputs, expose stage timings, and scale graph size enough to exercise storage, memory, or communication limits.",
      "uncertainty": "The serial results do not establish parallel behavior."
    }
  ]
}
```
