# Bound Global Score Table

- Pattern ID: `PAT-BOUND-GLOBAL-SCORE-TABLE`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `C_PAPER_BENCHMARK`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus can expose k, c, entry representation, and transfer counters for the run.",
      "The graph-scoring operation permits bounded approximate aggregation."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source fixes the on-chip global table at c*k entries.",
      "The source defers CPU result transfer until the final top-k output.",
      "The source reports empirical precision sensitivity to c."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-003"
    ],
    "text": "A Knight Bus top-k graph-scoring run can declare c*k as a bounded result-aggregation entry budget and report one final top-k transfer, but must leave byte width, update scratch, and precision as separately evidenced terms.",
    "uncertainty": "The source does not establish a portable byte bound or precision guarantee for other hardware and workloads."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "After each subgraph diffusion, add its output scores into the on-chip global table; defer device-to-host result transfer until all subgraph diffusions finish, then emit the top-k nodes.",
    "uncertainty": "The source does not give pseudocode for concurrent table updates or conflict resolution."
  },
  "confidence_rationale": {
    "assumptions": [
      "The local PDF and extracted text accurately represent the evaluated paper version.",
      "No independent campaign reproduction or source-code inspection has occurred."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source describes the table's capacity, placement, aggregation role, and transfer boundary.",
      "The source reports an empirical precision sensitivity to table multiplier c."
    ],
    "source_pointer_ids": [
      "SP-001",
      "SP-002",
      "SP-003"
    ],
    "text": "Section V-B directly specifies the c*k BRAM table and final-only top-k transfer, and reports sensitivity to c; this campaign did not independently reproduce the hardware behavior or inspect implementation code.",
    "uncertainty": "The table update policy, exact byte cost, concurrency behavior, and cross-workload precision remain unverified."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "A BRAM-resident global table holds node identifiers and PPR scores for at most the current top c*k nodes, separate from each processing element's local accumulated and residual score tables.",
    "uncertainty": "Per-entry width and table metadata are not reported."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "C_PAPER_BENCHMARK",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-003"
      ],
      "text": "The source reports materially larger top-k precision loss for its tested smaller c settings.",
      "uncertainty": "The reported threshold is empirical and must not be generalized to unseen graphs or query distributions."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-BOUND-GLOBAL-SCORE-TABLE",
  "falsifying_test": {
    "controlled_variables": [
      "k",
      "c",
      "subgraph score vectors",
      "node-score representation",
      "update order",
      "tie rule"
    ],
    "failure_signal": "The global table exceeds c*k resident entries, a full intermediate subgraph vector is transferred to the CPU, or final output contains other than k node-score results",
    "fixture": "Two sequential subgraph score vectors whose union contains more than c*k candidate nodes and whose final result requests top-k",
    "independent_oracle": "An instrumented bounded-table reference that counts resident entries and host-device result transfers, plus full-vector accumulation for observing approximation",
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
    "text": "The FPGA-resident global score table has fixed capacity c*k node-score entries, accumulates subgraph results on chip, and sends only the final top-k ranking nodes to the CPU after all diffusions.",
    "uncertainty": "The source does not specify the table's replacement, tie-breaking, or update data structure."
  },
  "knight_bus_algorithm_families": [
    "PERSONALIZED_PAGERANK",
    "TOP_K_GRAPH_SCORING",
    "ACCELERATOR_ASSISTED_GRAPH_DIFFUSION"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-001",
      "SP-002"
    ],
    "text": "Maintain a fixed c*k global top-score table in FPGA BRAM, aggregate each completed subgraph diffusion into that bounded table without returning its full vector to the CPU, and transfer only the final top-k result.",
    "uncertainty": "Bounding the table can discard candidates that would be retained by a complete global vector, so precision depends on c."
  },
  "name": "Bound Global Score Table",
  "pattern_id": "PAT-BOUND-GLOBAL-SCORE-TABLE",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Returning every subgraph diffusion's output vector to the CPU for summation increases transfer latency, while retaining the complete global score vector requires memory proportional to the full related subgraph.",
    "uncertainty": "The paper frames this problem for its hybrid CPU-FPGA local-PPR implementation."
  },
  "recomputed_state": {
    "assumptions": [
      "The implementation does not replay a subgraph after its contribution is aggregated."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Section V-B states that subgraph outputs are aggregated into the FPGA-resident global table.",
      "The final top-k is returned after all diffusions."
    ],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "The described table consumes each processed subgraph contribution when produced and does not require replay solely to reconstruct the final top-k output.",
    "uncertainty": "The paper does not explicitly describe recovery after table eviction, conflict, or hardware fault."
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
      "SP-002"
    ],
    "text": "The global score table remains resident in FPGA BRAM throughout all subgraph diffusions for one query.",
    "uncertainty": "The paper specifies entry count but not complete byte occupancy."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Suppress one full device-to-host output-vector transfer after each subgraph diffusion; transfer one final top-k node-score result after all diffusions",
      "measurement_needed": "Measure intermediate transfers, final result bytes, and protocol overhead per query.",
      "premises": [],
      "source_pointer_ids": [
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "The source does not provide byte widths, protocol overhead, or a total transfer equation."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Confirm whether any table snapshot is persisted and measure it if present.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The source describes volatile FPGA BRAM state and does not specify a persistent score-table artifact."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure per-query initialization and any synthesis-time or configuration-time table setup separately.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not isolate table initialization, compilation, or setup cost for this mechanism."
    },
    "ram": {
      "assumptions": [],
      "expression": "c*k node-score entries in FPGA BRAM; bytes per entry and table metadata UNKNOWN",
      "measurement_needed": "Measure synthesized BRAM bits and runtime occupancy for the global table separately from local score tables.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "The source supplies capacity in entries but not node-ID width, score width for this table, alignment, or update metadata."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "c*k node-score entries of query-lifetime global score state; update scratch UNKNOWN",
      "measurement_needed": "Measure peak table-update scratch and arbitration state in addition to the fixed entries.",
      "premises": [],
      "source_pointer_ids": [
        "SP-001",
        "SP-002"
      ],
      "status": "SOURCED",
      "uncertainty": "The fixed table is explicit, but temporary comparison, merge, and conflict-resolution state is not."
    }
  },
  "source_domain": "FPGA-resident top-k score aggregation for staged personalized PageRank",
  "source_paper_ids": [
    "PAPER-2104.09616"
  ],
  "source_pointers": [
    {
      "claim_scope": "CPU-FPGA dataflow and FPGA-resident global table storing scores for the top c*k nodes",
      "locator_type": "FIGURE",
      "locator_value": "Figure 4 and its surrounding Section V implementation description",
      "page": 4,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "SP-001"
    },
    {
      "claim_scope": "Fixed c*k BRAM global score table, per-subgraph aggregation, suppressed intermediate CPU transfers, and final top-k transfer",
      "locator_type": "SECTION",
      "locator_value": "Section V-B, Data Transfer Reduction",
      "page": 4,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "SP-002"
    },
    {
      "claim_scope": "Source-reported empirical precision sensitivity to c and the final implementation's selected c value",
      "locator_type": "PARAGRAPH",
      "locator_value": "Section V-B final paragraph",
      "page": 4,
      "paper_id": "PAPER-2104.09616",
      "pointer_id": "SP-003"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-002"
    ],
    "text": "Intermediate per-subgraph output vectors remain on the FPGA side for aggregation; only the final top-k node-score result is sent to the CPU.",
    "uncertainty": "Protocol framing and transfer bytes per returned node are not reported."
  },
  "unknown_when": [
    {
      "assumptions": [
        "A deterministic bound or complete update policy would require an explicit statement, algorithm, or proof in the inspected paper."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "Section V-B specifies fixed capacity and final top-k transfer.",
        "Its precision discussion is an empirical sensitivity report."
      ],
      "source_pointer_ids": [
        "SP-002",
        "SP-003"
      ],
      "text": "No deterministic bound in the inspected source maps c to top-k error, and the exact replacement and tie policy for maintaining the top c*k entries is unspecified.",
      "uncertainty": "Implementation details outside this source may define the policy, but they are not evidence for this card."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-003"
      ],
      "text": "In the source's reported sensitivity experiment, larger tested c settings had lower measured precision loss; the final FPGA implementation used a fixed c of 10.",
      "uncertainty": "The paragraph does not provide a complete per-graph fixture breakdown for this sensitivity result."
    }
  ]
}
```
