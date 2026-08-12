# Colocate Neighbor Codes Ondisk

- Pattern ID: `PAT-COLOCATE-NEIGHBOR-CODES-ONDISK`
- Epistemic label: `SOURCE_CLAIM`
- Evidence grade: `D_THEORETICAL_OR_INCOMPLETE`

```json
{
  "a007_consequence": {
    "assumptions": [
      "Knight Bus must serve more indexes than its RAM budget can keep resident.",
      "Its query path can consume neighbor guidance codes from the same streamed record as topology."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The source moves full vectors and PQ codes to SSD and duplicates neighboring PQ codes beside each node record.",
      "The source reports a memory and cold-switching benefit but warns of storage expansion and read amplification."
    ],
    "source_pointer_ids": [
      "SP-036",
      "SP-038",
      "SP-039",
      "SP-041"
    ],
    "text": "For memory-constrained multi-index retrieval, Knight Bus could trade a global resident guidance table for self-contained streamed records that carry each node's topology and neighbor guidance codes.",
    "uncertainty": "The paper does not evaluate this layout in its testbed, and no evidence establishes that the storage and read-amplification trade is favorable for Knight Bus graph workloads."
  },
  "access_schedule": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-038",
      "SP-039"
    ],
    "text": "Fetch a node's SSD record to obtain its full vector, neighbor identifiers, and the PQ codes needed to score those neighbors before following later records.",
    "uncertainty": "The paper does not provide a complete request schedule or queueing policy for AiSAQ."
  },
  "confidence_rationale": {
    "assumptions": [
      "The survey's description of AiSAQ accurately represents the cited system.",
      "Lack of an in-paper implementation or benchmark prevents empirical validation in this campaign."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Section 4.2.2 and Figure 7 specify the layout and intended memory benefit.",
      "The authors explicitly exclude All-in-Storage from their experimental and combination evaluations."
    ],
    "source_pointer_ids": [
      "SP-039",
      "SP-040"
    ],
    "text": "This card has a source-described mechanism and stated trade-offs but no benchmark evidence from the reviewed paper, so its support is incomplete.",
    "uncertainty": "The campaign did not inspect the cited AiSAQ implementation or reproduce its memory, storage, switching, or search behavior."
  },
  "data_arrangement": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-038",
      "SP-039"
    ],
    "text": "Each storage record colocates the node's full-precision vector and neighbor identifiers with PQ codes for those neighboring vertices, while the global full-vector and PQ collections reside on SSD.",
    "uncertainty": "The paper does not state the replication factor or exact byte layout for arbitrary graph degree."
  },
  "epistemic_label": "SOURCE_CLAIM",
  "evidence_grade": "D_THEORETICAL_OR_INCOMPLETE",
  "fails_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-036",
        "SP-040"
      ],
      "text": "The layout is unattractive when the objective is I/O efficiency under a substantial memory budget because moving and replicating PQ guidance on storage increases the on-disk footprint and can amplify reads.",
      "uncertainty": "The paper does not quantify the crossover between saved RAM and added storage traffic."
    }
  ],
  "falsifying_experiment_id": "RESERVED-G09-FOR-PAT-COLOCATE-NEIGHBOR-CODES-ONDISK",
  "falsifying_test": {
    "controlled_variables": [
      "logical proximity graph",
      "PQ code width",
      "record alignment",
      "query set",
      "recall target",
      "cache state",
      "concurrency"
    ],
    "failure_signal": "The on-storage layout requires an unbounded resident PQ table for correct traversal, cannot score a record's declared neighbors from fetched contents, or fails to reduce resident index bytes relative to the in-memory-PQ baseline",
    "fixture": "A tiny proximity graph whose SSD records contain each node vector, neighbor list, and copied neighbor PQ codes, plus an equivalent baseline with a global resident PQ table",
    "independent_oracle": "Byte accounting and deterministic candidate scores from the equivalent global-PQ baseline using identical vectors and codes",
    "scope": "Smallest mechanism-level falsifier description only; no G09 experiment exists"
  },
  "invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-039"
    ],
    "text": "A fetched node record carries the neighbor identifiers and corresponding neighbor PQ codes needed for the next approximate scoring step without a dataset-wide resident PQ array.",
    "uncertainty": "The source does not specify how updates preserve copied-code consistency."
  },
  "knight_bus_algorithm_families": [
    "APPROXIMATE_NEAREST_NEIGHBOR",
    "BEST_FIRST_GRAPH_SEARCH",
    "MULTI_INDEX_RETRIEVAL"
  ],
  "mechanism": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-038",
      "SP-039"
    ],
    "text": "Move full vectors and PQ codes from RAM to SSD, then duplicate each node's neighbors' PQ codes beside its vector and neighbor list so one record fetch supplies local navigation guidance.",
    "uncertainty": "The paper presents AiSAQ as the representative design and does not evaluate alternative all-storage encodings."
  },
  "name": "Colocate Neighbor Codes Ondisk",
  "pattern_id": "PAT-COLOCATE-NEIGHBOR-CODES-ONDISK",
  "problem": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-036",
      "SP-039"
    ],
    "text": "A global memory-resident PQ representation can consume too much RAM for billion-scale or multi-index deployments and can make rapid switching among indexes impractical.",
    "uncertainty": "The source frames this problem for disk-based ANN and retrieval-augmented-generation index switching."
  },
  "recomputed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-039"
    ],
    "text": "After each record fetch, the query computes approximate distances from the colocated neighbor PQ codes and can use the stored full vector for precise distance work.",
    "uncertainty": "The paper does not isolate compute overhead or exact reranking order for this layout."
  },
  "related_pattern_ids": [],
  "resident_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-039"
    ],
    "text": "Only a small in-memory index-management and query working set is required instead of the dataset-wide full-vector and PQ arrays.",
    "uncertainty": "The paper reports a qualitative scale reduction but does not enumerate every resident component."
  },
  "resource_model": {
    "io": {
      "assumptions": [],
      "expression": "Moving PQ guidance to SSD risks read amplification because query navigation must obtain codes from storage rather than a global resident array.",
      "measurement_needed": "Measure page reads, bytes read, and read amplification per query at matched recall against a resident-PQ baseline.",
      "premises": [],
      "source_pointer_ids": [
        "SP-036",
        "SP-040"
      ],
      "status": "SOURCED",
      "uncertainty": "The reviewed paper does not benchmark the magnitude of amplification."
    },
    "persistent_storage": {
      "assumptions": [],
      "expression": "SSD stores full vectors, node PQ codes, neighbor identifiers, and replicated PQ codes for neighboring vertices, producing on-disk expansion relative to the baseline record layout.",
      "measurement_needed": "Measure total index bytes and separate full vectors, topology, PQ codes, replicated neighbor codes, and padding.",
      "premises": [],
      "source_pointer_ids": [
        "SP-039",
        "SP-040"
      ],
      "status": "SOURCED",
      "uncertainty": "Expansion depends on graph degree, code width, and packing and is not quantified in this paper's evaluation."
    },
    "preprocessing": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure code generation, neighbor-code replication, record packing, build duration, and peak build RSS.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "The paper does not evaluate or separately bound All-in-Storage construction work."
    },
    "ram": {
      "assumptions": [],
      "expression": "The design moves dataset-wide full vectors and PQ codes to SSD and is reported to reduce memory requirements from gigabyte scale to megabyte scale for billion-scale data.",
      "measurement_needed": "Measure retained userspace and kernel RSS, separating query state, metadata, buffers, and any residual code cache.",
      "premises": [],
      "source_pointer_ids": [
        "SP-039"
      ],
      "status": "SOURCED",
      "uncertainty": "The source gives an order-of-scale claim rather than a parameterized whole-process formula."
    },
    "temporary_storage": {
      "assumptions": [],
      "expression": "UNKNOWN",
      "measurement_needed": "Measure peak query buffers, in-flight record pages, candidate state, and build-time replication buffers.",
      "premises": [],
      "source_pointer_ids": [],
      "status": "UNKNOWN",
      "uncertainty": "Temporary storage is not reported separately for the All-in-Storage layout."
    }
  },
  "source_domain": "memory-constrained disk-resident graph approximate-nearest-neighbor search",
  "source_paper_ids": [
    "PAPER-2602.21514"
  ],
  "source_pointers": [
    {
      "claim_scope": "Storage-centric placement and read-amplification risk",
      "locator_type": "SECTION",
      "locator_value": "Section 2.2, Representative disk-based systems",
      "page": 3,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-036"
    },
    {
      "claim_scope": "All-in-Storage cost classification",
      "locator_type": "TABLE",
      "locator_value": "Table 1, All-in-Storage row",
      "page": 4,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-037"
    },
    {
      "claim_scope": "Placement of full vectors and PQ codes on SSD",
      "locator_type": "SECTION",
      "locator_value": "Section 4.2.2, All-in-Storage Layout opening",
      "page": 5,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-038"
    },
    {
      "claim_scope": "AiSAQ record contents, RAM reduction, multi-index switching, and evaluation exclusion",
      "locator_type": "SECTION",
      "locator_value": "Section 4.2.2, All-in-Storage Layout (AiS), and Figure 7",
      "page": 6,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-039"
    },
    {
      "claim_scope": "Combination-space exclusion for storage expansion and read amplification",
      "locator_type": "SECTION",
      "locator_value": "Section 7.1.1, Combination Design",
      "page": 10,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-040"
    },
    {
      "claim_scope": "Decision boundary for tight memory and rapid cold start",
      "locator_type": "FIGURE",
      "locator_value": "Figure 24 and Section 8 conclusion",
      "page": 12,
      "paper_id": "PAPER-2602.21514",
      "pointer_id": "SP-041"
    }
  ],
  "streamed_state": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "SP-038",
      "SP-039"
    ],
    "text": "Query processing streams self-contained SSD records carrying full vectors, neighbor identifiers, and neighbor PQ codes.",
    "uncertainty": "Record-fetch reuse and cache policy are not specified for this technique."
  },
  "unknown_when": [
    {
      "assumptions": [
        "The reviewed paper's evaluation exclusion means no in-paper measurements can validate the stated AiS trade-offs.",
        "The cited external system is outside this campaign's inspected evidence."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "Section 4.2.2 explicitly excludes AiSAQ from the experimental evaluation.",
        "Section 7.1.1 excludes AiS from combinations because of expected storage and read costs."
      ],
      "source_pointer_ids": [
        "SP-039",
        "SP-040"
      ],
      "text": "The measured memory, storage, read-amplification, latency, and recall trade-offs remain unknown from this paper.",
      "uncertainty": "The source description may not capture implementation-specific buffering, caching, or record compression."
    }
  ],
  "works_when": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "premises": [],
      "source_pointer_ids": [
        "SP-039",
        "SP-041"
      ],
      "text": "The layout is intended for tight-memory deployments that need fast cold starts or rapid switching among multiple large indexes and can accept added storage traffic.",
      "uncertainty": "The reviewed paper states this boundary but does not benchmark it."
    }
  ]
}
```
