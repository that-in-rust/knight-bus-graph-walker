# Paths: Unit-Hop Eligibility Before Buying Indexes

Date: 2026-09-21. A03 source/theorem gate completed: independent proof review,
finite oracles and retained real-snapshot profiling. No performance, physical
RAM or publication-priority claim.

## Decision

**Do not build the full geometric multicut index for this unit-hop source on
the expectation of a large RAM reduction.** The best prepared gate count within
this representation retains 81.75% of the full family in the forward direction
and 86.20% in reverse. It also retains 97.03%/97.75% of original simple edges
in the ledger. The more expensive omitted-edge revival index has nothing to
report here: D=r=0. None of these logical ratios is a physical RAM percentage.

The useful positive result is an exact eligibility formula and anchor-free
constructor, with a separate reviewed cut-root state refinement. They prevent
an inappropriate storage choice and simplify eligible unit-hop executions.
They are not a newly invented generic contraction operation or seven paper-
ready innovations. The normalized graph itself is a real file-dependency
artifact, not a representative sample of all graph customers.

## Question And Plan

The previous multicut result is correct under a paid prepared-forest contract.
This study asks whether a useful existing dependency source satisfies that
contract with enough eliminated query states to justify the implementation.
Expert lenses are query semantics, graph reduction, full-cost engineering and
skeptical source/provenance review; they are not imaginary independent experts.

Three options have different meanings: ordinary BFS over the normalized graph;
an exact unit-hop forest quotient; or a zero-cost quotient preserving only
reachability. The third is not a faster implementation of the second's query.
We profile both forward dependencies and reverse impact, without mixing them.

The concrete source is the existing Clarity file-level snapshot under
`docs_PRD03/reference-learning/neo4j-family-dependency-graphs/`. It contains
structural file imports/references, not complete runtime calls, security policy,
or a live Neo4j customer workload. Existing extractor omissions stay explicit.

Execution steps for this bounded approved research gate:

1. Write failing finite tests for the proposed forest constructor, unit-hop
   barrier, identity normalization, cuts and original shortest-path semantics.
2. Implement a resident reference profiler, not a production low-RAM engine.
3. Test exact all-target answers on finite graphs before reading the snapshot.
4. Record source hashes, normalized duplicate/loop policy, q/n, A/m, D and
   actual revivals r. Keep isolates and repository-qualified file identities.
5. Compare whole family plus Neo4j and GDS subgraphs in both directions.
   Preserve source/cut selection and verify sampled source-to-all-target results.
6. Integrate a disjoint independent theorem review and record a decisive next
   action. No timing while that review runs, no automatic index-building bet.

## Reviewed Unit-Hop Barrier

For a simple loopless directed graph with every original weight one, an
original-ancestor forest path from u to v has length L and, by tightness,
`h(v)-h(u)=L`. A direct edge u->v and feasible h require L<=1. If u!=v,
L>=1. For L=1, the simple edge is the selected forest identity itself.
Therefore there are no omitted nonforest arcs: D=0, hence r=0 for any batch
of selected-forest deletions. This says nothing about general weighted or
zero-cost reachability graphs, parallel raw identities, or arbitrary indexes.

## Reviewed Optimal State Count

Let n1 count vertices of indegree exactly one. Let c1 count directed cycles
whose vertices all have indegree one. For this forest-quotient representation,
the proved optimum is `q_min=n-n1+c1`.

Every non-gate must have its unique incoming edge selected. A directed cycle
of such edges must lose at least one selected edge, yielding a gate. Conversely,
select every edge whose head has indegree one, break one per cycle, and root
all remaining components. Set h to forest depth. Any unselected edge enters
an indegree-not-one root or a broken cycle root, so its reduced cost is
`1+h(tail)>=0`. All selected edges are tight. Exactly n-n1+c1 roots/gates
remain. This construction needs no anchor SSSP.

This is an elementary degree-one elimination specialization and a bound on
THIS representation, not a new generic shortest-path lower bound. Optimizing
over a fixed supplied potential, prescribed extra gates, general elimination
schemes or source-specific reached state is a different problem.

### Ledger And Cut Refinements

In the attaining forest all old gates are roots. Every selected edge enters
an indegree-one nongate, so there are no cross-capsule forest ledger entries.
Consequently `A=m-n1+c1=m-n+q`. If average directed degree is m/n, the fraction
removed from the ledger is `(1-q/n)/(m/n)`. A small state reduction can therefore
translate into a still smaller edge-record reduction. This is a ledger-size
identity, not a lower bound on head aggregation or query work.

After k selected-edge deletions, the standard entry-closed representation has
q+k old/new roots (before optional source promotion), and still no revivals.
Every new cut root has lost its **only** original incoming edge. It is
unreachable from any source other than itself. Hence at most q+1 entry states
can actually be reached, independent of k. A specialized query can avoid
allocating solve state for those unreachable roots, retaining only sparse cut
descriptors and source-specific tail restrictions. A source inside a detached
fragment must still be promoted; seeding its old root invents paths.

The current resident profiler materializes all q+k roots and does not implement
that low-state scheduling option. The [independent review](Paths-Unit-Hop-Eligibility-Review.md)
supports the refinement and identifies its paid lookup/witness requirements.
Exact unit cost equals original hop count; shortest paths here are full-graph
minimum-hop paths, unlike general weighted domination that can discard a
shorter-hop equal-cost shortcut.

## Source Contract And Provenance

Inputs are the existing [node TSV](../docs_PRD03/reference-learning/neo4j-family-dependency-graphs/family-file-nodes.tsv)
and [edge TSV](../docs_PRD03/reference-learning/neo4j-family-dependency-graphs/family-file-edges.tsv),
interpreted according to the [original extractor notes](../docs_PRD03/reference-learning/neo4j-family-dependency-graphs/README.md).
Node identity is the pair `(repository,file path)`, never a bare basename.
All 23,972 declared nodes, including isolates, remain. The loader rejects
unknown endpoints, duplicate node identities and schema changes. Edges within
a repository are normalized to simple directed pairs; six self-loops are
removed and zero duplicate nonloop rows are found. This leaves 147,294 edges
from 147,300 raw rows. There are no inferred cross-repository dependencies.

Removing positive self-loops does not change vertex hop distances. Static
pair normalization would not preserve deletion of individual parallel raw
identities; this source has no such duplicated pairs, and the study's failure
contract is explicitly deletion of normalized selected edges. Neither input
schema carries relationship types, costs, permissions or runtime call truth.
Repository-local generated/partial extractor coverage must not be mistaken
for the complete executable behavior of Neo4j.

Source hashes, checked before and after parsing:

```text
family-file-nodes.tsv
0ba39215498a05df12747e0819c348b8723c2b17bd0bb5a4d8a25040b1aa40c8
family-file-edges.tsv
84065c3f87961c60889b4bbf5ac1a774aa911ec103e942c2d1a05c1b2f5164da
```

The forward interpretation is source file -> referenced file, following the
stored source/target columns. Reverse impact traverses target -> referencing
file. The forest, indegree and potential are rebuilt for each direction.
For reachability only, all edges receive zero weight and h=0; sorted outgoing
DFS selects the forest. The latter forest is a deterministic heuristic, not
a proved optimal reachability representation. No custom weights were invented
to manufacture compression.

## Observed Eligibility

q counts old entry states; A counts retained old ledger records; D counts
omitted nonforest records. All other metadata and original input remain paid.

| Snapshot / direction | n | m | Optimal unit-hop q | State slots removed | Unit-hop A | Ledger records retained |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Family / forward | 23,972 | 147,294 | 19,596 | 18.25% | 142,918 | 97.03% |
| Family / reverse | 23,972 | 147,294 | 20,663 | 13.80% | 143,985 | 97.75% |
| Neo4j / forward | 10,738 | 85,273 | 8,987 | 16.31% | 83,522 | 97.95% |
| Neo4j / reverse | 10,738 | 85,273 | 9,594 | 10.65% | 84,129 | 98.66% |
| GDS / forward | 4,921 | 30,609 | 4,049 | 17.72% | 29,737 | 97.15% |
| GDS / reverse | 4,921 | 30,609 | 4,413 | 10.32% | 30,101 | 98.34% |

All six unit-hop profiles have D=0. Family forward has n1=4,394 and c1=18;
reverse has n1=3,330 and c1=21. These produce the displayed optimum exactly.
Maximum forest depth is five forward and eight reverse; Neo4j and GDS unit
profiles have maximum depth four. The study did not infer these from timing.

| Snapshot / direction | DFS reachability q | State slots removed | A | D | Revivals for selected 32-cut batch |
| --- | ---: | ---: | ---: | ---: | ---: |
| Family / forward | 20,253 | 15.51% | 140,303 | 3,272 | 19 |
| Family / reverse | 20,724 | 13.55% | 135,557 | 8,489 | 43 |
| Neo4j / forward | 9,113 | 15.13% | 81,927 | 1,721 | 21 |
| Neo4j / reverse | 9,515 | 11.39% | 79,303 | 4,747 | 121 |
| GDS / forward | 4,130 | 16.07% | 29,453 | 365 | 7 |
| GDS / reverse | 4,449 | 9.59% | 29,282 | 855 | 30 |

Switching the query to reachability and using this DFS heuristic does not
rescue the state gate. This is not a bound on the best possible zero-cost
forest, SCC-based engine, reachability labeling or other representation.
The cut batches are deterministic examples, not an estimated production
failure distribution or a worst-case-r experiment.

## Executed Verification And Receipt

The [resident reference](experiments/path_source_eligibility.py) was added
after the first narrow test failed for its absent module. All seven
[lead tests](experiments/test_path_source_eligibility.py) then passed with
warnings as errors: 4,096 four-vertex digraphs, 1,500 source/cut queries and
8,683 target comparisons. Tests independently enumerate small forest choices
for the q minimum, check nonnegative reduced costs, original paths, cycles,
cuts, identity normalization and invalid input. These are finite checks.

The separate [independent checker](Paths-Unit-Hop-Eligibility-Review.md) was
read and replayed without importing project code. It passed 4,166 graphs,
77,180 admissible forests, 12,990 cut cases, 51,783 source queries and 206,681
target checks. It verified 133,883 complete lifted paths; the other 72,798
target results were unreachable. This is theorem review, not a code audit.

The retained [real-source receipt](evidence/path-source-eligibility-20260921/receipt.json)
contains twelve profiles: three datasets, two directions and two query
meanings. For each, it selects four source IDs by a fixed rule: first ID,
last ID, maximum outgoing degree and maximum selected-forest depth, with
ID tie-breaking. IDs and file names are retained. Batches of size 0, 1, 8
and 32 are selected at evenly spaced positions in the forest-child preorder;
these are separate sets against the same epoch, not an incremental deletion
sequence or nested cut-size progression.

All 192 queries passed full original-target distance/reachability comparisons
against ordinary BFS: 2,536,384 target checks, of which 115,713 were reachable.
For finite outputs, 1,720 sampled full paths also passed original-edge,
continuity, cut-exclusion, endpoint and simplicity checks; unit-hop paths also
matched shortest hop count. Most target checks are unreachable, and the four
source rules are coverage choices rather than customer workload frequencies.
No full serialized output or time-to-result benchmark is claimed.

Receipt SHA-256:

```text
bcdd4f3e04490861a2104af5e8828fdedeb504b72e713e4809ba4156f1533a29
```

The receipt pins both input hashes and both lead module/test hashes. The lead
code was unchanged after the recorded run. Reproduce with a fresh output
directory; the writer refuses to overwrite an existing receipt:

```sh
python3 -B -W error research_algorithms_20260920/experiments/test_path_source_eligibility.py
python3 -B -W error research_algorithms_20260920/experiments/path_source_eligibility.py \
  --source-dir docs_PRD03/reference-learning/neo4j-family-dependency-graphs \
  --output-dir /tmp/knightbus-path-source-replay
awk '/^```python$/{p=1;next} p && /^```$/{exit} p{print}' research_algorithms_20260920/Paths-Unit-Hop-Eligibility-Review.md | python3 -B -W error -
```

The prototype sorts/materializes edges, owners, adjacency and quotient rows;
it uses resident dictionaries/sets and a duplicate-entry heap. The constructor's
linear-time mathematical possibility is not a claim that this whole profiler,
including its stable sorting and repeated all-target oracle, runs in linear
time or bounded physical RAM. No timing experiment ran alongside the reviewer.

## Prior Art And Rubber-Duck Corrections

The bounded independent primary review inspected directed degree-one
elimination in [Dietzfelbinger and Jaberi](https://arxiv.org/abs/1412.1639),
and weighted shortcut/path unpacking in
[Contraction Hierarchies](https://ai.dmi.unibas.ch/research/reading_group/geisberger-et-al-wea2008.pdf).
The former preserves a different property; the latter is a general shortest-
path contraction framework. Neither is falsely quoted as the exact gate-count
formula, and neither licenses claiming its underlying operation as new.
The inspected portions and unresolved priority boundary are in the review.

| Tempting interpretation | Correction |
| --- | --- |
| Few forest cuts imply little query state | Old q can already be close to n; here it is. r is zero in the unit contract, but q still dominates. |
| A different anchor/potential will dramatically lower q | The attained optimum ranges over every admissible F,h in this quotient. It cannot beat the degree-count bound here. |
| 18.25% fewer states means 18.25% less process RAM | Immutable metadata, ledger/index storage, runtime, output and per-state widths remain. No byte measurement exists. |
| Compressed shortest paths can still use ordinary unit-edge BFS | Macro paths span multiple edges and carry their full costs. A generic weighted solver may use more per-state memory than the original BFS. |
| Reachability is the same cheaper workload | It drops shortest-hop answers. Even that weaker query with this DFS choice retains most states. |
| 2.5 million checks establish a customer win | Most are unreachable targets in twelve related profiles. They establish tested semantics, not demand, representativeness or performance. |
| This disproves a low-RAM graph engine | It limits one exact representation on one snapshot. External BFS, other contractions and native structures remain valid alternatives. |

## Next Research Decision

Park full A03 index construction for this unit-hop file graph. Preserve the
reviewed multicut theorem for weighted/zero-cost domains with demonstrated
eligibility; do not invent edge weights, reinterpret critical-path longest
paths as shortest paths, or choose a tree merely to obtain a favorable plot.
The next useful A03 implementation requires either an actually eligible
source or a representation that escapes the proved single-forest gate bound,
compared against equally capable elimination/reachability engines.

For the seven-family goal, the next bounded investigation is the existing
[A05 compressed dual](Similarity-Compressed-Dual-Core.md) on the fixed public
workload at larger G. Its theorem is already independently reviewed, but
large-G exact score opportunities were not evaluated by the
[prior profile](Similarity-Exception-Workload-Profile.md). Determine whether
those bounds avoid enough body work beyond the stronger interval/DAAT controls
to repay query preparation and metadata evaluation. Do not run more A03
tiny-graph checks or rename this eligibility corollary as a new graph solver.
