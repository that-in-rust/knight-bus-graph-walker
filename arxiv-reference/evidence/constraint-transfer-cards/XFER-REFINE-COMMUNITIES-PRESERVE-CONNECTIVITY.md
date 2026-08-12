# Refine Communities Preserve Connectivity

```json
{
  "analogy_failure_modes": [
    {
      "assumptions": [
        "Aggregation loses the ability to split a coarse remainder represented as one node."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "The source specifically refines before constructing the aggregate graph."
      ],
      "text": "Applying refinement after coarse communities have already become indivisible aggregate nodes does not preserve the source invariant.",
      "uncertainty": "A different reversible aggregate representation would need independent proof."
    },
    {
      "assumptions": [
        "The cap can occur before the relevant convergence predicate."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "G06 and the source distinguish guarantee stages."
      ],
      "text": "A fixed iteration cap is a resource bound, not a stable or asymptotic convergence witness.",
      "uncertainty": "Per-completed-iteration connectivity may still hold."
    },
    {
      "assumptions": [
        "No additional source is being imported."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The mechanism card records directed applicability and an a-priori quality bound as unknown."
      ],
      "text": "The undirected modularity or CPM proof does not establish equivalent guarantees for directed graphs, mutable snapshots, or unrelated clustering objectives.",
      "uncertainty": "Separate mechanisms may exist but are outside this transfer."
    }
  ],
  "epistemic_label": "SPECULATIVE_TRANSFER",
  "falsifying_experiment_id": "RESERVED-G09-FOR-XFER-REFINE-COMMUNITIES-PRESERVE-CONNECTIVITY",
  "g06_challenges": [
    {
      "applies": true,
      "failure_id": "FAIL-EARLY-STOPPING-WEAKENS-GUARANTEES",
      "response": "Separate per-completed-iteration connectivity from stable and asymptotic claims; record the exact stop and convergence witnesses, return an explicitly early-stopped result when the budget expires, and never advertise a stronger tier before its predicate is observed."
    }
  ],
  "modern_knight_bus_constraints": [
    {
      "assumptions": [
        "Knight Bus can inspect graph direction and selected quality function before execution."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "The source guarantees are proved in an undirected setup.",
        "The mechanism card explicitly leaves directed applicability unknown."
      ],
      "text": "Limit the transfer to fixed undirected graph snapshots using supported modularity or CPM semantics and a positive refinement-randomness parameter; directed or dynamically mutating inputs require separate evidence or refusal.",
      "uncertainty": "Weighted and multigraph normalization details still require target specification."
    },
    {
      "assumptions": [
        "The implementation exposes level cardinalities and representation widths."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "The source algorithm materializes refinement before aggregate construction.",
        "A007 requires temporary-overlap accounting."
      ],
      "text": "Reserve current graph, coarse and refined assignments, quality state, move queue, aggregate mapping, aggregate edges, output, worker state, and any overlapping hierarchy generation before each level; spill, reduce bounded concurrency, or refuse when the peak cannot be established.",
      "uncertainty": "Aggregate edge multiplicity and reclamation timing require measurement."
    },
    {
      "assumptions": [
        "The last reported partition completed the refinement and aggregation iteration required by the per-iteration guarantee."
      ],
      "claim_type": "SPECULATIVE_TRANSFER",
      "premises": [
        "G06 shows early stopping invalidates stronger guarantee claims but can preserve per-iteration connectivity."
      ],
      "text": "If a resource or iteration budget stops execution, return an explicitly early-stopped connected partition and only the per-completed-iteration guarantees actually witnessed; advertise stable or asymptotic properties only after their convergence predicates are recorded.",
      "uncertainty": "Application acceptance of an early-stopped partition is a product contract input."
    }
  ],
  "modern_resource_model": {
    "concurrency": {
      "assumptions": [
        "Worker and queue limits are fixed before the level starts."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "CONCURRENCY_peak_bytes = C_workers*(B_move_buffer + B_refine_buffer) + Q_moves*b_queue_entry + c_scheduler_metadata*C_workers",
      "measurement_needed": "Measure private worker vectors, queued moves, aggregate contributions, synchronization metadata, and peak RSS while varying worker count and community skew.",
      "uncertainty": "Partition skew and stochastic merge opportunities can create uneven private state.",
      "unknown_constants": [
        "b_queue_entry",
        "c_scheduler_metadata"
      ],
      "variables": [
        "B_move_buffer: per-worker local-move state bytes",
        "B_refine_buffer: per-worker refinement and aggregate contribution bytes",
        "C_workers: admitted worker count",
        "Q_moves: maximum queued nodes or communities"
      ]
    },
    "io": {
      "assumptions": [
        "Any external or spill adaptation preserves the exact refinement and aggregation semantics."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "IO_total_bytes = rho_external_level_fraction*L_levels*(Q_graph_pages*P_page + Q_aggregate_write_pages*P_page + Q_checkpoint_pages*P_page)",
      "measurement_needed": "Measure graph reads, aggregate writes, checkpoint traffic, spill traffic, cache state, and page reuse for every level and iteration.",
      "uncertainty": "The source supplies no external-memory schedule; the external-level fraction is entirely target-specific.",
      "unknown_constants": [
        "rho_external_level_fraction"
      ],
      "variables": [
        "L_levels: hierarchy levels processed",
        "P_page: target storage page bytes",
        "Q_aggregate_write_pages: aggregate pages written per level",
        "Q_checkpoint_pages: checkpoint pages written per level",
        "Q_graph_pages: current-level graph pages read per level"
      ]
    },
    "preprocessing": {
      "assumptions": [
        "The input artifact is converted to the exact graph and weight semantics used by the quality oracle."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "PREP_work_units = N_nodes*c_initialization + E_edges*c_quality_state_initialize",
      "measurement_needed": "Measure snapshot validation, undirected normalization, quality-statistic initialization, initial partition construction, time, bytes, and peak RSS.",
      "uncertainty": "Symmetrization, self-loop policy, weight representation, and preexisting partitions alter preparation.",
      "unknown_constants": [
        "c_initialization",
        "c_quality_state_initialize"
      ],
      "variables": [
        "E_edges: input edge count after required normalization",
        "N_nodes: input node count",
        "PREP_work_units: implementation-defined conversion and initialization work units"
      ]
    },
    "ram": {
      "assumptions": [
        "Materialized structures remain live until the aggregate level is committed or the prior level is reclaimed."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "RAM_peak_bytes = B_current_graph + N_level*(b_coarse_assignment + b_refined_assignment + b_mapping_entry + b_quality_node) + C_level*b_quality_community + E_aggregate*b_aggregate_edge + C_workers*(B_move_buffer + B_refine_buffer) + Q_moves*b_queue_entry + c_scheduler_metadata*C_workers + b_runtime_overhead",
      "measurement_needed": "Measure peak RSS by hierarchy level and phase, including overlapping current and aggregate graphs, both partitions, mappings, node-level and community-level quality state, per-worker move and refinement buffers, the move-queue high-water mark, scheduler metadata, connectivity metadata, and runtime overhead.",
      "uncertainty": "Lazy views, edge deduplication, multiedge representation, and allocator behavior may change peak overlap.",
      "unknown_constants": [
        "b_aggregate_edge",
        "b_coarse_assignment",
        "b_mapping_entry",
        "b_quality_community",
        "b_quality_node",
        "b_queue_entry",
        "b_refined_assignment",
        "b_runtime_overhead",
        "c_scheduler_metadata"
      ],
      "variables": [
        "B_move_buffer: disjoint local-move state bytes per admitted worker",
        "B_refine_buffer: disjoint refinement and aggregate-contribution bytes per admitted worker",
        "B_current_graph: bytes for the current base or aggregate graph",
        "C_level: communities represented by quality accumulators in the current hierarchy level",
        "C_workers: admitted concurrent workers",
        "E_aggregate: materialized aggregate edge entries at peak",
        "N_level: nodes in the current hierarchy level",
        "Q_moves: maximum live move-queue entries across all workers, excluding private buffers",
        "b_quality_community: disjoint quality-accumulator bytes per current-level community",
        "b_quality_node: disjoint node-level quality and connectivity-state bytes not contained in B_current_graph"
      ]
    },
    "storage": {
      "assumptions": [
        "Hierarchy retention and checkpoint policy are declared before admission."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "expression": "STORAGE_peak_bytes = B_input_graph + sum_{h=1}^{H_retained}(N_h*b_community_assignment + E_h*b_aggregate_edge + b_storage_metadata) + B_temporary_generations_peak + B_checkpoint + B_receipt",
      "measurement_needed": "Measure retained graph, partitions, aggregate levels, checkpoints, temporary generations, manifests, and receipts under the chosen retention policy.",
      "uncertainty": "Aggregate sizes vary by graph, seed, resolution, and iteration.",
      "unknown_constants": [
        "b_aggregate_edge",
        "b_community_assignment",
        "b_storage_metadata"
      ],
      "variables": [
        "B_checkpoint: persisted recovery state bytes",
        "B_input_graph: durable input snapshot bytes",
        "B_receipt: retained partition checksum and receipt payload bytes excluding per-level manifests",
        "B_temporary_generations_peak: incremental aggregate or checkpoint-generation bytes live beyond every retained baseline term",
        "E_h: aggregate edge entries retained at hierarchy level h",
        "H_retained: retained hierarchy generations",
        "N_h: nodes retained at hierarchy level h",
        "b_storage_metadata: manifest, generation, and filesystem metadata bytes for retained level h, excluding B_receipt"
      ]
    }
  },
  "name": "Refine Communities Preserve Connectivity",
  "original_constraint_profile": {
    "access_medium": {
      "assumptions": [
        "No external storage behavior is inferred from the publication context."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source presents in-memory pseudocode and benchmarks but does not define an external-memory layout or device I/O model."
      ],
      "text": "UNKNOWN",
      "uncertainty": "A Knight Bus adaptation may retain or stream different hierarchy levels."
    },
    "communication_model": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-REFINE-COMMUNITIES-BEFORE-AGGREGATION"
      ],
      "source_pointer_ids": [
        "SP-003",
        "SP-005"
      ],
      "text": "Each level performs local node moves, constrained refinement within coarse communities, and edge traversal to construct a weighted aggregate multigraph before repeating.",
      "uncertainty": "The source does not specify distributed communication or external streaming."
    },
    "constrained_resource": {
      "assumptions": [
        "The implementation materializes the structures named by Algorithm A.2."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The mechanism card specifies these live structures and marks most byte-level resource terms unknown."
      ],
      "text": "The source constrains semantic loss during hierarchical aggregation but does not provide a whole-process memory or I/O bound for the simultaneously live graph, coarse partition, refined partition, mapping, queue, and aggregate graph.",
      "uncertainty": "Representation width and generation overlap are not source-bounded."
    },
    "data_mutability": {
      "assumptions": [
        "The input is treated as a fixed snapshot for a run."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The source algorithm operates on a graph supplied to an iterative partitioning procedure and does not define concurrent graph mutation semantics."
      ],
      "text": "UNKNOWN",
      "uncertainty": "Dynamic-graph guarantees require separate evidence."
    },
    "original_hardware_operating_assumptions": {
      "assumptions": [
        "Benchmark-machine capacity is not converted into an algorithmic hardware assumption."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The mechanism card reports in-memory pseudocode and no portable byte-level storage or concurrency model."
      ],
      "text": "UNKNOWN",
      "uncertainty": "Target memory hierarchy, worker model, and storage placement remain open."
    },
    "predictability_requirement": {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-REFINE-COMMUNITIES-BEFORE-AGGREGATION"
      ],
      "source_pointer_ids": [
        "SP-004",
        "SP-008"
      ],
      "text": "The source distinguishes guarantees after each completed iteration, after a stable iteration, and asymptotically; later guarantees require their corresponding convergence conditions.",
      "uncertainty": "No universal iteration bound predicts stable or asymptotic completion."
    }
  },
  "original_constraints": [
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-REFINE-COMMUNITIES-BEFORE-AGGREGATION"
      ],
      "source_pointer_ids": [
        "SP-001"
      ],
      "text": "Aggregating a coarse community after a bridge node moves can make a disconnected remainder indivisible at later hierarchy levels.",
      "uncertainty": "The source analyzes undirected modularity and CPM community optimization."
    },
    {
      "assumptions": [],
      "claim_type": "SOURCE_CLAIM",
      "source_pattern_ids": [
        "PAT-REFINE-COMMUNITIES-BEFORE-AGGREGATION"
      ],
      "source_pointer_ids": [
        "SP-003",
        "SP-005"
      ],
      "text": "Refinement must occur inside each coarse community before aggregation, and the refined partition and coarse assignments play different roles in constructing and initializing the aggregate graph.",
      "uncertainty": "The exact partition is stochastic and can vary by seed."
    }
  ],
  "original_cost_model": {
    "assumptions": [
      "A materialized implementation may overlap old and new hierarchy generations."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The mechanism card's resident and temporary-state fields enumerate these structures.",
      "The card marks RAM, I/O, preprocessing, and persistent storage unknown."
    ],
    "text": "The source algorithm requires a current graph, coarse and refined assignments, move queue, connectivity and quality statistics, aggregation mapping, and newly built aggregate graph, but it supplies no whole-process RAM, external I/O, preprocessing, or persistent-storage formula.",
    "uncertainty": "Edge multiplicity, identifier width, lazy views, and reclamation schedule determine actual cost."
  },
  "original_domain": "hierarchical community detection on undirected networks",
  "proposed_transfer": {
    "assumptions": [
      "The run uses an undirected fixed snapshot and supported quality semantics."
    ],
    "claim_type": "SPECULATIVE_TRANSFER",
    "premises": [
      "The source refinement preserves connectivity conditions before aggregation.",
      "G06 limits stronger guarantees under early stopping.",
      "A007 requires bounded execution and verifiable receipts."
    ],
    "text": "For bounded Leiden-style community detection, perform constrained refinement inside coarse communities before constructing each aggregate graph, reserve both semantic partitions and aggregate-build state, and make the stop condition and resulting guarantee tier part of admission and the final receipt rather than treating a fixed iteration budget as convergence.",
    "uncertainty": "Resource coefficients, convergence iteration, stochastic variability, and any external-memory schedule are unmeasured."
  },
  "reversed_assumptions": [
    {
      "assumptions": [
        "The runtime can record partition changes and the exact convergence predicate."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "FAIL-EARLY-STOPPING-WEAKENS-GUARANTEES separates per-iteration, stable, and asymptotic properties.",
        "The source reports difficult networks reaching stability after different numbers of iterations."
      ],
      "text": "Iteration count is not treated as a predictable constant; Knight Bus admission reserves a bounded iteration or work envelope and the receipt states only the guarantee reached at the observed stop witness.",
      "uncertainty": "No source-supported pre-run convergence-iteration bound exists."
    },
    {
      "assumptions": [
        "A target implementation chooses and exposes a concrete representation."
      ],
      "claim_type": "DERIVED_INFERENCE",
      "premises": [
        "The mechanism card leaves external I/O and most byte coefficients unknown.",
        "A007 requires full-working-set and temporary-storage terms."
      ],
      "text": "The source's in-memory presentation is not carried forward as a storage decision; every simultaneously live hierarchy structure and any spill or checkpoint traffic is modeled explicitly.",
      "uncertainty": "This transfer does not select that representation."
    }
  ],
  "smallest_falsifier": {
    "controlled_variables": [
      "iteration limit",
      "node order",
      "quality function",
      "quality-state representation",
      "random seed",
      "refinement randomness",
      "resolution",
      "worker count",
      "move-queue high-water cap"
    ],
    "failure_signal": "A completed iteration returns a disconnected community, refinement decreases the selected quality function, the runtime claims a stable or asymptotic tier before its witness, or attributed node/community quality-state and hierarchy peak exceed the admitted resource model.",
    "fixture": "The smallest undirected bridge-node graph where moving a bridge disconnects a coarse community, executed with controlled seeds both through one completed refinement iteration and through an imposed early stop before a stronger convergence witness.",
    "independent_oracle": "Induced-subgraph connectivity checking, direct quality recomputation for each partition, an unrestricted run evaluated against the claimed convergence predicate, and component-attributed quality-state, worker-buffer, move-queue, scheduler, and aggregate-RSS traces across level, community cardinality, and admitted concurrency."
  },
  "source_pattern_ids": [
    "PAT-REFINE-COMMUNITIES-BEFORE-AGGREGATION"
  ],
  "surviving_invariant": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "source_pattern_ids": [
      "PAT-REFINE-COMMUNITIES-BEFORE-AGGREGATION"
    ],
    "source_pointer_ids": [
      "SP-003",
      "SP-005",
      "SP-006"
    ],
    "text": "Refinement is confined to each coarse community, and singleton nodes or refined communities merge only under required internal-connectivity and non-decreasing-quality conditions before the refined partition drives aggregation.",
    "uncertainty": "Formal guarantees require an undirected graph, the source quality setup, and a positive refinement-randomness parameter."
  },
  "target_algorithm_families": [
    "CPM_COMMUNITY_DETECTION",
    "LEIDEN_COMMUNITY_DETECTION",
    "MODULARITY_OPTIMIZATION"
  ],
  "transfer_id": "XFER-REFINE-COMMUNITIES-PRESERVE-CONNECTIVITY",
  "unknown_measurement_constants": [
    "b_aggregate_edge",
    "b_coarse_assignment",
    "b_community_assignment",
    "b_mapping_entry",
    "b_quality_community",
    "b_quality_node",
    "b_queue_entry",
    "b_refined_assignment",
    "b_runtime_overhead",
    "b_storage_metadata",
    "c_initialization",
    "c_quality_state_initialize",
    "c_scheduler_metadata",
    "rho_external_level_fraction"
  ]
}
```
