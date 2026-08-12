# Merge Thread Local Coarsenings

- Pattern ID: `PAT-MERGE-THREAD-LOCAL-COARSENINGS`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus uses materialized thread-local coarsening."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source materializes one partial graph per thread.",
      "The coarsening phase scales less strongly than other phases."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A007 must include the simultaneous peak of all partial coarse graphs and the final coarse graph; parallel speedup alone cannot justify admission when that temporary amplification exceeds the budget.",
    "uncertainty": "Peak bytes depend on community structure and duplicate aggregation."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Workers scan disjoint edge portions and aggregate locally; a second parallel phase iterates coarse vertices and merges corresponding adjacencies from all partial graphs.",
    "uncertainty": "NONE"
  },
  "confidence_rationale": {
    "assumptions": [
      "The implementation follows the stated partial-graph merge."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source describes the mechanism and evaluates coarsening as a phase."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "The two-phase coarsening schedule is explicit and phase scaling is reported, but temporary amplification and implementation details were not independently inspected.",
    "uncertainty": "No reproduction or code-backed grade is available."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "A fine-to-coarse vertex map accompanies one partial coarse adjacency structure per worker and one final coarse graph.",
    "uncertainty": "Partial-graph container details are unspecified."
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
      "text": "The merge and memory footprint can dominate when partial graphs are large; the source observes coarsening scaling less strongly than move and refinement phases.",
      "uncertainty": "No universal memory breakpoint is given."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-MERGE-THREAD-LOCAL-COARSENINGS",
  "falsifying_test": {
    "controlled_variables": [
      "Fine graph, community map, edge partition, worker count, and merge order."
    ],
    "failure_signal": "Any coarse edge or self-loop weight differs from the oracle, or peak partial-graph storage is omitted from the receipt.",
    "fixture": "A small weighted graph whose fine edges map many times to the same coarse edge, partitioned across several worker ranges.",
    "independent_oracle": "Single-threaded coarse graph built by exact aggregation of every fine edge.",
    "scope": "Coarsening correctness and temporary-state accounting."
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Each original edge contributes exactly once to one thread's partial coarse adjacency, and merging partial adjacencies by coarse node preserves total inter-community and self-loop weights.",
    "uncertainty": "NONE"
  },
  "knight_bus_algorithm_families": [
    {
      "assumptions": [
        "Knight Bus retains exact coarse-edge weights and fine-to-coarse lineage."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source coarsens communities into supernodes."
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "Louvain and Leiden multilevel community detection are direct Knight Bus matches; graph condensation before other algorithms is plausible but not established.",
      "uncertainty": "Transfer to BFS, WCC, PageRank, or similarity requires separate semantics."
    }
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Partition original edges among workers, let each worker build its own partial coarse graph, then process coarse nodes in parallel and merge their partial adjacency contributions.",
    "uncertainty": "The final merge still limits scaling in the reported implementation."
  },
  "name": "Merge Thread Local Coarsenings",
  "pattern_id": "PAT-MERGE-THREAD-LOCAL-COARSENINGS",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Serial graph coarsening becomes a bottleneck after local-move parallelization.",
    "uncertainty": "NONE"
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Coarse edge weights and self-loops are recomputed by summing contributions from fine edges across partial graphs.",
    "uncertainty": "NONE"
  },
  "related_pattern_ids": [
    "PAT-RECOMPUTE-NEIGHBOR-WEIGHTS-LOCALLY"
  ],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "The fine graph, vertex map, all thread-local partial graphs, and final coarse graph overlap during coarsening.",
    "uncertainty": "Peak overlap duration is not quantified."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure bytes read and written if fine or coarse adjacencies spill to storage.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The implementation and evaluation are shared-memory; storage traffic is not modeled."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure persistent bytes if coarse levels are retained.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "No durable multilevel graph encoding is specified."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "A fine-to-coarse vertex mapping is produced from the current community solution before edge aggregation.",
      "measurement_needed": "Measure map construction and community-ID compaction work.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "SOURCED",
      "uncertainty": "Mapping construction cost is not isolated."
    },
    "ram": {
      "assumptions": [
        "Partial graphs remain live until their coarse-node contributions are merged."
      ],
      "expression": "Fine graph and vertex map plus all per-worker partial coarse adjacencies and the final coarse graph coexist during the merge.",
      "measurement_needed": "Measure peak RSS and bytes per partial graph throughout coarsening.",
      "premises": [
        "The source creates one partial coarse graph per thread and then combines them."
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "DERIVED",
      "uncertainty": "The source does not state aggregate bytes or release schedule."
    },
    "temporary_storage": {
      "assumptions": [
        "They are materialized rather than streamed directly into the final graph."
      ],
      "expression": "Temporary storage includes one partial coarse graph per worker until merged.",
      "measurement_needed": "Measure peak sum of partial adjacency capacities and merge buffers.",
      "premises": [
        "The source explicitly constructs per-thread coarse graphs."
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "status": "DERIVED",
      "uncertainty": "Sparsity and duplicate-edge aggregation determine amplification."
    }
  },
  "source_domain": "parallel multilevel graph coarsening",
  "source_paper_ids": [
    "PAPER-1304.4453"
  ],
  "source_pointers": [
    {
      "claim_scope": "Per-thread partial coarse graphs and parallel adjacency merge.",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section III.B, Parallel graph coarsening",
      "page": 5,
      "paper_id": "PAPER-1304.4453",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Measured phase scaling and the coarsening phase's weaker parallel benefit.",
      "locator_type": "FIGURE",
      "locator_value": "Figures 3-4 and Section V.C",
      "page": 8,
      "paper_id": "PAPER-1304.4453",
      "pointer_id": "SP-002"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001"
    ],
    "text": "Each worker scans its assigned original-edge range, followed by a scan of partial adjacencies for each coarse vertex.",
    "uncertainty": "The source assumes in-memory graph storage."
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
        "SP-001"
      ],
      "text": "The source does not quantify storage amplification from duplicate coarse edges across thread-local partial graphs.",
      "uncertainty": "Workload structure controls duplication."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "Fine edges can be partitioned independently and enough memory exists for worker-local partial graphs.",
      "uncertainty": "NONE"
    }
  ]
}
```
