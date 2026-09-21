# Proposal-Free Threshold Community Solver

Date: 2026-09-21. A04 research follow-on. Status: implemented resident Python
prototype with exact small-objective tests, a pinned150-source study and a
fixed18-case native-topology gate including complete fallback.
The separate [trace-verifier review](Communities-Single-Flip-Independent-Review.md)
does NOT review this solver or its implementation. No physical-RAM, latency,
customer-workload prevalence or publication-priority claim is made. The
separate [solver mathematical review](Communities-Threshold-Solver-Independent-Review.md)
is now complete and lead-replayed; it does not audit the implementation.

## What Changed

The [single-flip verifiers](Communities-Single-Flip-Trace-Verification.md)
can check a supplied proposed run without replaying all its unchanged visits.
Their stronger chronological control revealed that the proposal is unnecessary
while the run has two nonempty communities: the same tree can FIND the next
profitable vertex in the remaining visit order.

This removes both the matching restriction on proposal generation and the
single-flip restriction on execution. It does not solve arbitrary-community
Louvain. When a third community must be created or one of the two empties,
the prototype returns the exact prefix and pending visit before that action.

## Contract

- Simple loopless undirected graph, original integer IDs, positive rational
  stored weights, positive total degree T and positive rational gamma.
- Supplied initial labels P/Q, both nonempty. Isolates are permitted.
- Increasing-ID visits, repeated full sweeps. Choose the best strictly
  positive move among the opposite community and a fresh singleton. Stay at
  zero; a positive tie favors the existing opposite community.
- Return `complete` only after certifying an entire no-move sweep. Output
  includes exact accepted gains, moves, final labels and terminal sweep.
- Otherwise return `fresh_destination`, `empty_community` or `move_budget`,
  plus the verified prefix, current labels and pending `(sweep, original ID)`.
  These statuses are not completed clustering results or lower-quality answers.
- `max_moves` bounds accepted events, not bytes, build time or elapsed time.
  It is checked before the next supported positive move. An already complete
  no-move state can succeed at budget zero.

No source partition discovery is included. A two-way warm partition might
come from a previous snapshot, a coarse split or another routine, but useful
sources and the cost of obtaining that partition still need measurement.

## Algorithm

Use the exact stay thresholds from the verifier manuscript. At current
Q volume z, each P vertex is safe iff `z>=L_u`, and each Q vertex is safe
iff `z<=U_u`. The rank tree stores interval max(L) and min(U).

```text
remaining sweep ranks
        |
        v
aggregate interval safe? -- yes --> skip entire interval
        |
        no
        v
descend left before right
        |
        v
first profitable vertex
        |
        +--> fresh/empty/budget --> exact prefix + pending visit
        |
        v
apply best positive move
update label, scalar volume and neighbor thresholds
continue immediately after that rank
```

Depth-first left-first search ignores ranks already visited in this sweep.
A wholly safe tree interval contains no profitable vertex. For an unsafe
interval, recursively search its left child before its right child. Padded
leaves impose no constraint. The first returned real leaf is the earliest
profitable remaining visit. If none exists, the remaining suffix contains
only stays; either start the next sweep after a changed sweep, or terminate
after a no-move sweep.

Already visited neighbors are updated too. Their new thresholds cannot affect
the current suffix search, but must affect their next sweep visits. Nonneighbor
thresholds need not be rewritten when z changes: z is supplied to every range
check. This separates local affinity changes from the global degree-volume
effect, without a neighbor-only frontier assumption.

## Correctness Argument

1. The two exact gain inequalities give necessary and sufficient stay
   conditions at every leaf, including fresh moves and exact ties.
2. A tree aggregate is safe iff all represented leaves are safe. Pruning a
   safe interval cannot hide a positive action. Ignoring ranks before the
   cursor exactly implements the prescribed visit order.
3. Left-first descent therefore returns the next real action of serial
   replay. Intervening visits are all stays and change no state.
4. A supported switch updates precisely its label, Q volume and neighbor
   affinities. This preserves the tree invariant after each accepted action.
5. Induction identifies the entire returned prefix with serial replay.
   No profitable suffix means the skipped suffix is correct. A complete
   no-move sweep gives the same termination condition as replay.
6. Fresh/empty handoff occurs BEFORE changing the incompatible state. A
   general executor resumes at that positive pending visit using source,
   labels, gamma, visit order and remaining event allowance. The now-implemented
   [general scalar fallback](Communities-Native-Workload-Gate.md#fallback-is-part-of-the-algorithm)
   reconstructs its caches and preserves the prefix. This is not a generic
   checkpoint: an arbitrary stay-position resume still needs the prior changed
   bit, and a complete output still needs the retained prefix.

Repeated movers do not invalidate these invariants. They do invalidate the
old `J<=2M` single-flip bound. Strictly increasing exact modularity prevents
revisiting the identical labeled partition while execution stays in its
two-label domain, but this supplies only an exponential worst-case state
bound, not a practical deadline. The event budget remains explicit.

The independent solver challenge establishes the first-event search bound
and handoff sufficiency under these assumptions. Its standalone checker was
read and replayed by the lead:4,496 exact cases,10,981 budget cuts,15,190 resume
comparisons,36,158 gain identities and48,870 rank-search queries pass. It
exposes five faulty solver variants plus explicit counterexamples to
neighbor-only activation, gain-priority order and generic changed-bit loss.
Four of its fixed witnesses are now regression tests against the actual
candidate. This remains a mathematical review plus finite implementation
tests, not a machine-checked proof or a general fallback code audit.

## Cost And Low-RAM Relevance

Let f be accepted events, S observed sweeps and
`J=sum_over_events adjacency_record_degree(mover)` including repetitions.
With a ranked source, initialization is O(n+M). Sorting arbitrary IDs adds
O(n log n) comparisons. Each first-violation search visits O(log n) tree
nodes: whole safe subtrees are pruned; only the suffix boundary and the path
to the first violating leaf can require descent. Each accepted action updates
one mover and its neighbors, each with O(log n) tree work.

```text
Cached scalar baseline: O(n+M+n*S+J) operations after ranking
Threshold solver:       O(n+M+(f+S+J)*log n) operations after ranking
Resident source/state:  O(n+M+f) records including retained trace/output
```

The tree adds O(n) records. This is a work-elimination algorithm, NOT yet a
RAM-elimination algorithm. On dense graphs, many accepted high-degree moves
can make its logarithmic update factor worse than cached scalar execution.
On short runs, tree construction and rational division can dominate avoided
visits. Exact integer widths, Python overhead, source validation and complete
output remain charged. Replacing exact comparisons with a tolerance would
change the contract near zero.

The possible low-RAM next step is external adjacency with bounded edge buffers
and a measured resident threshold-state budget. That is unimplemented and
would still pay random/batched neighbor updates, file cache, sorting and
recovery. The current all-resident source cannot establish the product's
4 GB process/job cap or50 GB prepared-storage constraint.

## Fixed150 Result

The [retained receipt](evidence/community-threshold-solver-20260921/receipt.json)
reuses every input in the heterogeneous study, verifies source hashes and
replays the old expanded oracle. No matching partition, HHLL producer or
proposed trace is supplied to the solver, only graph/initial labels/gamma.
The test oracle is run separately for verification and is not an input to
the solver.

| Item | Exact retained result |
| --- | ---: |
| Cases attempted | 150 |
| Complete exact outputs | 150 |
| Previously changed matching schedules handled | 64 |
| Cases with a vertex moving more than once | 19 |
| Final original-ID labels checked | 13,440 |
| Accepted moves/gains checked | 813 |
| Logical cached-scalar visit model | 49,024 |
| Event searches | 1,327 |
| Search-tree nodes examined | 10,598 |
| Neighbor-affinity updates | 3,252 |
| Threshold updates / tree nodes written | 4,065 /31,005 |
| Sum of allocated threshold pair slots across jobs | 26,880 |

Searches include suffix/terminal checks, not just accepted events. The tree
also has build cost: comparing only1,327 to49,024 would misrepresent the work.
Tree reads/writes, divisions and scalar gain evaluations are different units,
so their ratio is not a runtime prediction. All cases are engineered warm
sources; there is no measured native-source success rate. None of these150
cases triggered a handoff; separate randomized tests exercise handoffs.

Four new tests initially failed for the absent module and now pass. They
cover first-violation selection against direct scans,260 seeded small exact
objective traces including repeated movers and handoffs, budget-prefix
preservation, isolates and513-bit IDs. The combined suite has49 passing tests.

Four later review-driven boundary regressions bring the combined total to53:
positive existing/fresh ties with isolates, genuine terminal revisits,
1/10^60 sign margins and explicit fresh-community handoff. They strengthen
finite checks without substituting for an independent solver review.

Reproduce the focused solver and review-driven checks from `experiments/`:

```sh
/Users/amuldotexe/.local/bin/python3.11 -B -W error -m unittest test_threshold_event_solver test_single_flip_review_contracts -v
```

The retained full study is generated by `probe_threshold_solver_evidence.py
--output <new-receipt-path>`. It refuses to overwrite an existing receipt and
checks all pinned upstream inputs and sources first.

Receipt SHA-256:
`e0e4b5c3c9f0ef79f582196bfe86fb59732569e134d9ba4150ef1c5a6508b1a4`.

## Why This Is Not Yet A Paper Claim

Local moving, deterministic visit order and range aggregation are established
ideas; see the [targeted primary-source comparison](Communities-Single-Flip-Trace-Verification.md#10-targeted-prior-art-check).
The candidate delta is exact event discovery under globally changing
modularity volume while preserving the prescribed serial trajectory. We have
not established whether this domain-specific combination already appears in
the literature or an optimized partition-refinement implementation.

The original next gates were independent search/handoff review, a retained
native matrix, competent cached/full-fallback controls, then physical costs
and a useful scientific separation. The first three now have evidence below;
they should not be rerun as if unfinished. Meaningful customer workloads,
competitive physical costs and contribution priority are still unproved.
Any later timings require serial fresh workers without concurrent research
load, matched source preparation and complete output. Retain adverse results.

The seven-family innovation objective remains active. This implementation
removes a real proposal-generation restriction but does not close the
whole-workflow RAM or scientific novelty requirements.

## Native Gate Changes The Next Step

The [fixed native gate](Communities-Native-Workload-Gate.md) is complete.
Twelve of18 cases finish without handoff; six gamma3/2 cases need a fresh
community before any accepted specialized move. Full resumed output matches
cold general execution on all18, including17,754 events and55,890 labels.
The binary starting partitions on the large graphs are constructed probes;
the Karate split is observed. This is not measured customer prevalence.

The exact native logical cost is unfavorable to an unqualified speed claim:
232,878 cached scalar visits compare to138,690 search nodes and1,358,363
point-update node writes, plus tree construction and other costs. A standard
batched ancestor-union model reduces writes to676,661, but does not establish
a time or RAM improvement. The model is not an implemented faster solver.

The combined community suite now passes63 tests. Frozen original sources and
receipts remain unchanged. Do not repeat the completed independent-search
gate or replace the six handoffs with more favorable starts. Next establish
a useful binary-refinement use case, a materially cheaper exact certificate
representation, or a sparse general-label reduction. Any representation must
beat the strong batched/cached controls with paid preparation, fallback and
output, and preserve globally activated nonneighbors. Physical benchmarking
is premature until that mechanism and workload are defensible.
