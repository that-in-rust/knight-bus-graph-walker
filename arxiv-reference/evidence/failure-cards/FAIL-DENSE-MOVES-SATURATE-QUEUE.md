# Dense Moves Saturate Queue

```json
{
  "adversarial_fixture": {
    "assumptions": [
      "The fixture produces accepted broad-neighborhood moves."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "controlled_variables": [
      "initial partition",
      "quality function",
      "move order"
    ],
    "expected_observation": "Queue insertions and pops converge toward full-sweep work as affected neighborhoods become global.",
    "fixture_kind": "GRAPH_AND_EXECUTION",
    "fixture_name": "dense-cut-community-moves",
    "graph_scale": "Increase boundary-vertex degree while holding node count and quality function fixed.",
    "graph_shape": "Dense communities connected so an accepted boundary move touches most currently movable nodes.",
    "independent_oracle": "A full-sweep implementation recomputes every node opportunity after each accepted move.",
    "premises": [
      "The source queue rule is neighborhood-based."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "The smallest accepting graph depends on the quality function.",
    "varied_variables": [
      "boundary degree",
      "accepted move sequence"
    ],
    "workload": "Run fast local move and a deterministic full-sweep control from the same initial partition."
  },
  "affected_architecture_ids": [],
  "affected_pattern_ids": [
    "PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY"
  ],
  "breakpoint_equation": {
    "assumptions": [
      "A broad neighborhood makes most nodes affected."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "expression": "requeued_nodes_per_move approaches graph_node_count",
    "measurement_needed": "Trace queue insertions, pops, and neighborhood coverage on degree-controlled graphs.",
    "numeric_constants": [],
    "premises": [
      "The mechanism saves work only by avoiding unaffected nodes."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "uncertainty": "No source ratio defines when queue scheduling loses.",
    "variables": [
      {
        "definition": "Distinct neighbors newly queued after an accepted move.",
        "symbol": "requeued_nodes_per_move",
        "units": "nodes per move"
      },
      {
        "definition": "Nodes eligible for local moving.",
        "symbol": "graph_node_count",
        "units": "nodes"
      }
    ]
  },
  "broken_assumption": {
    "assumptions": [],
    "claim_type": "SOURCE_CLAIM",
    "premises": [],
    "source_pointer_ids": [
      "FP-001"
    ],
    "text": "Accepted node moves affect a small enough neighborhood that most nodes need not be revisited.",
    "uncertainty": "The source gives the local dependency rule but no universal queue-size bound."
  },
  "confidence_rationale": {
    "assumptions": [
      "Dense neighborhoods can be nearly global."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Move effects are local to graph neighborhoods."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The queue-collapse construction follows from the explicit requeue rule; source benchmarks do not bound dense worst cases.",
    "uncertainty": "Whether such moves are accepted must be established per quality function."
  },
  "epistemic_label": "DERIVED_INFERENCE",
  "expected_failure_signal": {
    "assumptions": [
      "Queue bookkeeping is measured separately."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The same move semantics and tie-breaking are used."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Distinct queue pops and neighbor scans provide no material reduction relative to the full-sweep oracle under the dense trigger.",
    "uncertainty": "No numeric materiality threshold is asserted here."
  },
  "failure_basis": "ANALYTICAL_COUNTEREXAMPLE",
  "failure_id": "FAIL-DENSE-MOVES-SATURATE-QUEUE",
  "name": "Dense Moves Saturate Queue",
  "observable_symptom": {
    "assumptions": [
      "Membership checks do not eliminate future requeues after nodes are popped."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "Dense affected neighborhoods refill the queue broadly."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "The queue repeatedly approaches a global sweep, eliminating the intended scheduling advantage and increasing queue traffic.",
    "uncertainty": "The algorithm may still converge faster for quality reasons."
  },
  "repair_options": [
    {
      "description": "Switch away from queue scheduling when observed affected-neighborhood coverage approaches a global sweep.",
      "repair_class": "ADD_ADMISSION_GUARD"
    },
    {
      "description": "Batch broad moves and perform one deduplicated sweep rather than repeated queue refills.",
      "repair_class": "CHANGE_SCHEDULE"
    }
  ],
  "source_paper_ids": [
    "PAPER-1810.08473"
  ],
  "source_pointers": [
    {
      "claim_scope": "Only neighbors affected by an accepted move are requeued.",
      "locator_type": "SECTION",
      "locator_value": "Section III, fast local move procedure",
      "page": 5,
      "paper_id": "PAPER-1810.08473",
      "pointer_id": "FP-001"
    },
    {
      "claim_scope": "The work queue and affected-neighbor requeue rule are explicit.",
      "locator_type": "ALGORITHM",
      "locator_value": "Algorithm A.2, MoveNodesFast",
      "page": 15,
      "paper_id": "PAPER-1810.08473",
      "pointer_id": "FP-002"
    }
  ],
  "triggering_workload": {
    "assumptions": [
      "The graph contains vertices whose neighborhoods cover most movable nodes."
    ],
    "claim_type": "DERIVED_INFERENCE",
    "premises": [
      "The sourced rule enqueues affected neighbors after a move."
    ],
    "source_pointer_ids": [
      "FP-001",
      "FP-002"
    ],
    "text": "Accepted moves occur at high-degree vertices or across dense cuts, and each move requeues nearly every node not already queued.",
    "uncertainty": "Move acceptance depends on quality-function state."
  }
}
```
