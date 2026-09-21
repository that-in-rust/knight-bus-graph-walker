# Community Solver Native Workload Gate

Date: 2026-09-21. Fixed design declared before native results; results added
below after the run. This follows the
[proposal-free solver](Communities-Proposal-Free-Threshold-Solver.md), not a
claim that the seven-family research goal is complete.

## Question And Fixed Design

Does exact event discovery help on real graph topology, and does its restricted
two-community state survive useful starts? A truthful answer must retain
fresh/empty-community handoffs and compare to a competent cached scalar
baseline, not objective recomputation.

Fixed matrix: two retained SNAP graphs (GrQc and Facebook combined), two
deterministic constructed starts (first half of increasing IDs; first half
of a deterministic BFS forest), and gamma in {1/2,1,3/2}:12 cases. Add Zachary's
karate graph with its supplied club split, both native integer edge weights
and unit-edge projection, at the same three gammas:6 cases. Total18.
Do not filter this matrix based on which cases complete or look favorable.

The large starts are stress/structural probes, NOT measured production warm
starts. The small supplied club labels are an observed split, not proof that
modularity refinement is the appropriate business objective or that these
results generalize to large workloads. The source papers' graph purposes
do not establish a customer distribution for our engine.

## Planned Controls And Evidence

1. Implement a two-label cached scalar baseline: cached weighted degrees,
   Q affinities and Q volume, visiting every rank. It shares input validation
   but not threshold tests or event search. Verify against direct objective
   differences on small sources and against all retained150 source outputs.
2. Implement an exact general-label scalar fallback with cached community
   volumes and adjacency-based affinities, including zero-volume existing
   communities as eligible tie winners. Validate cold execution and resume
   from a positive pending visit against an objective oracle.
3. Run each native case with a declared event budget, compare every exact
   accepted gain, label, stop/pending visit and handoff state. Run fallback
   for handoffs; charge work already spent before fallback. No suppressed
   fresh moves and no claiming a prefix is a completed result.
4. Retain input hashes, source versions, initial-label hashes, projection
   rules, every status and logical construction/search/update/output counts.
   The data parser, validation, rank ordering and fallback reconstruction
   costs are part of the workflow even if not all are instrumented yet.
5. No timings or RSS claims in this gate. Independent review can run during
   correctness studies, but not during later serial physical measurements.

## Primary Data Sources

- [SNAP GrQc](https://snap.stanford.edu/data/ca-GrQc.html) describes an
  undirected scientific coauthorship graph with5,242 nodes and14,496 edges.
  Parse the retained compressed edge list, canonicalize reciprocal/duplicate
  pairs, remove self-loops explicitly and retain their endpoint vertices.
- [SNAP Facebook](https://snap.stanford.edu/data/ego-Facebook.html) describes
  the anonymized combined ego-network graph with4,039 nodes and88,234 edges.
  The combined edge file does not supply a two-way initial partition.
- [NetworkX karate data documentation](https://networkx.org/documentation/stable/reference/generated/networkx.generators.social.karate_club_graph.html)
  describes observed club labels and interaction-context edge weights from
  Zachary's1977 study. Pin NetworkX3.6.1 for fixture generation and retain the
  normalized34-node weighted graph and labels as JSON, with content hash.

Sources inspected2026-09-21. Counts in the final receipt must come from the
actual parsed sources; a normalization discrepancy must be explained, not
silently forced to match the web page.

## Decision Gate

If most cases leave the two-label state quickly, do not promote the current
kernel as a general native-community accelerator. Either justify a useful
two-way refinement niche or develop a genuinely broader representation. If
tree updates dominate avoided visits, report the negative cost evidence and
do not infer speed from skipped visits. Native correctness is not physical
RAM validation or scientific novelty evidence.

## Retained Native Results

The [native receipt](evidence/community-native-gate-20260921/receipt.json)
exists, all eight recorded source-code hashes match, and its driver process
is terminal. The continuation recovered the completed receipt rather than
restarting a process whose handle had fallen out of conversation context.

| Source/start | Gamma | Two-label status | Prefix / complete moves | Full sweeps | Final communities |
| --- | ---: | --- | ---: | ---: | ---: |
| GrQc / ID half | 1/2 | Complete | 2,221 / 2,221 | 8 | 2 |
| GrQc / ID half | 1 | Complete | 2,440 / 2,440 | 14 | 2 |
| GrQc / ID half | 3/2 | Fresh handoff | 0 / 4,522 | 6 | 294 |
| GrQc / BFS half | 1/2 | Complete | 619 / 619 | 7 | 2 |
| GrQc / BFS half | 1 | Complete | 747 / 747 | 6 | 2 |
| GrQc / BFS half | 3/2 | Fresh handoff | 0 / 3,212 | 10 | 143 |
| Facebook / ID half | 1/2 | Complete | 121 / 121 | 3 | 2 |
| Facebook / ID half | 1 | Complete | 121 / 121 | 3 | 2 |
| Facebook / ID half | 3/2 | Fresh handoff | 0 / 1,686 | 9 | 10 |
| Facebook / BFS half | 1/2 | Complete | 347 / 347 | 3 | 2 |
| Facebook / BFS half | 1 | Complete | 328 / 328 | 3 | 2 |
| Facebook / BFS half | 3/2 | Fresh handoff | 0 / 1,370 | 11 | 7 |
| Karate weighted / observed | 1/2 | Complete | 1 / 1 | 2 | 2 |
| Karate weighted / observed | 1 | Complete | 1 / 1 | 2 | 2 |
| Karate weighted / observed | 3/2 | Fresh handoff | 0 / 7 | 3 | 3 |
| Karate unit / observed | 1/2 | Complete | 2 / 2 | 2 | 2 |
| Karate unit / observed | 1 | Complete | 2 / 2 | 2 | 2 |
| Karate unit / observed | 3/2 | Fresh handoff | 0 / 7 | 3 | 3 |

All18 workflows complete with identical accepted moves, exact gains, final
labels, and terminal sweeps to cold general-label scalar execution. This
checks17,754 accepted events and55,890 final label values across jobs. Twelve
finish in the specialized state; six require fallback. All six leave BEFORE
any accepted specialized move. They are genuine failures of the two-label
domain, not failures of arithmetic correctness and not approximate answers.
The apparent66.7% specialized completion fraction is only this designed matrix,
not a customer adoption rate or a confidence interval on native usefulness.

The driver independently recomputes initial/final modularity and checks that
the difference equals the sum of all scaled gains times2/T^2. Large-graph
trajectory comparison is against a separately implemented cached scalar
executor; direct objective recomputation at every visit is used only in
small tests. Neither test oracle is an input to the specialized solver.

GrQc normalizes to5,242 vertices and14,484 simple edges, not14,496. Its file
contains28,980 rows:12 self-loop rows are removed and14,484 duplicate/reciprocal
rows collapse. Endpoint vertices remain. Facebook normalizes to4,039 vertices
and88,234 edges without removed rows. Karate has34 vertices and78 edges in
both projections. Input normalization is explicit because changing it changes
the modularity objective.

The planned constructed-cohort comparator check was also executed: the new
cached scalar control reconstructs all150 source fixtures, checks each input
digest, and matches every retained trace, label hash and terminal sweep from
the frozen solver receipt. Totals are13,440 labels,813 gains,49,024 scalar
visits and3,252 neighbor updates. This validates the former visit model with
an actual cached implementation; it is not another native workload or timing.

## Fallback Is Part Of The Algorithm

`probe_cached_community_controls.py` implements both the strong cached
two-label scalar control and a general-label fallback. The latter caches
community volumes but computes a visited vertex's affinities from adjacency.
It evaluates neighboring communities, the current community, the fresh token,
and the lexicographically least existing zero-volume community. The last
candidate matters: an isolated nonempty community can beat a fresh token on
an exact positive tie. Omitting it changes the prescribed trajectory.

For positive-degree vertices, an absent positive-volume destination has score
strictly below the fresh score zero; zero-degree vertices have no strict
positive gain. These facts justify omitting other nonneighbor destinations.
Fresh tokens are `fresh:{sweep}:{ID}`; token collisions are rejected. Existing
ties are lexicographic, so numeric-looking suffixes are not numerically sorted.
This is a declared research policy, not a claim of Neo4j/GDS compatibility.

Resume reconstructs degree and volume caches, starts at the exact positive
pending `(sweep,ID)`, and receives the remaining event budget. It must either
accept that pending action or re-halt at an exhausted budget. The independent
[solver review](Communities-Threshold-Solver-Independent-Review.md) explains
why that positive action makes the previous sweep's changed flag redundant;
an arbitrary mid-sweep checkpoint would still need it. Prefix and suffix
events must be joined for complete output. This is not a durable checkpoint
or crash-recovery implementation.

Full cold general execution makes397,066 scalar visits. The six fallbacks
make164,194 visits after the specialized attempts. Repeated pending visits,
reconstructed caches, source validation, tree construction, input sorting and
the output log all remain costs. The two-label cached prefixes make232,878
visits, whereas the tree's covered-visits counter totals232,872: the six
profitable pending visits are inspected but not counted as covered by the
tree. Do not silently equate these counters.

## Stronger Standard Repair Control

The frozen solver rewrites each affected leaf-to-root path separately.
Before attributing its repair cost to the mathematical reduction, compare
the ordinary alternative: set all changed leaves for one accepted move,
then recompute each ancestor ONCE bottom-up before the next search.

For k distinct affected ranks in a padded tree of N=2^h leaves, write count is
the union of their ancestor paths. At level j from the leaves it is at most
`min(k,N/2^j)`. Summing gives `O(k*(1+log(N/k)))` node writes, versus
`k*(h+1)` for independent point updates. This is standard batch aggregation,
not proposed scientific novelty. Exact set construction, sorting/hashing,
threshold arithmetic and temporary storage are additional work. The Python
counter uses sets; its node counts are deterministic, not a worst-case hash
table time guarantee.

The [structural receipt](evidence/community-batched-repair-20260921/receipt.json)
replays ALL18 frozen specialized trajectories, checks their hashes, labels,
statuses, sweeps and every previous metric, and reconstructs the old point
write count exactly. It models the batched union; it does not execute a new
batched community solver or measure latency/RSS. Three tests cover780 seeded
rank batches plus dense, singleton, empty, duplicate and invalid-rank cases.

| Charged structural unit, summed over18 jobs | Count |
| --- | ---: |
| Specialized accepted moves | 6,950 |
| Neighbor-affinity updates | 94,582 |
| First-event searches / search nodes | 7,011 / 138,690 |
| Old point-update node writes | 1,358,363 |
| Standard batched ancestor-union writes | 676,661 |
| Initial tree nodes, excluding unused array slot zero | 148,206 |
| Cached scalar visits, including pending visits | 232,878 |
| Largest single-level temporary set | 793 entries |

Batched writes are50.19% fewer. This is NOT50.19% less time, RAM, total work,
or traffic: original affinities and thresholds still need updating, and the
tree, graph, outputs and batched temporary records remain. Even without
charging build or batch discovery, search plus batched writes sum to815,351
tree-node operations, versus232,878 scalar visits of a different cost. These
units cannot prove a runtime loss either, but they invalidate a speed argument
based only on7,011 searches replacing232,878 visits.

The small observed Karate cases are especially instructive: only one or two
moves and two sweeps occur, but each builds127 meaningful tree nodes. The
Facebook ID-half cases need only121 moves yet touch9,693 neighbor records
each. Native graph degree and short convergence both matter.

## Research Decision And Next Experiment

Correctness and explicit fallback pass this gate. A broad fast/low-RAM claim
does not. Keep all six handoffs and the full-cost counters in every summary.
Do not increase favorable matching examples or quote only terminal searches.

Three approaches remain, with different failure risks:

1. **Useful two-way refinement.** Obtain a real task requiring refinement of
   a supplied binary partition, at its justified resolution parameter. Charge
   partition creation and compare full builds, output and scalar/batched
   controls. Existing cases do not establish that such a customer exists.
2. **Selective certificate maintenance.** Investigate block/lazy or compressed
   certificates that remove more state or repair work, while certifying
   globally activated nonneighbors and arbitrary rank suffixes. A correct
   bound plus an adversarial workload is required before a new implementation.
   Compare against the standard batch bound above, not only frozen point paths.
3. **General-label exact execution.** Derive a representation that survives
   fresh creation and empty communities. Current labels at gamma3/2 produce
   up to294 communities; storing an n-by-community gain table is disallowed
   as an unpriced low-RAM solution. Sparse candidate storage and existing
   general local-moving engines are compulsory controls.

An ordinary engineering improvement is to postpone tree construction until
after checking whether the first positive action is supported. It would avoid
tree build in these six zero-prefix handoffs, but also charges preliminary
scalar work on successes. This is a candidate dispatcher, not implemented or
claimed as a novel algorithm. Tune neither gamma nor initial labels to hide
handoffs. Physical measurements wait for an actual competitive implementation
and a meaningful workload, not just a smaller modeled counter.

## Keep The Original Product Objective

The user asked for useful graph algorithms with lower RAM and predictable
processing, not universal preservation of this particular serial trajectory.
Exact replay remains a valuable compatibility and falsification track, but
must not become a self-imposed substitute for the seven-family objective.
The native gate therefore creates TWO research tracks, without retroactively
changing any old test or calling a different answer equivalent:

- **Exact trajectory:** preserve objective, initialization, visit order, ties,
  every accepted action and final labels. Compare sparse/general certificates,
  cached scalar visits and standard batched trees under the same semantics.
- **Quality/resource frontier:** declare a potentially different algorithm;
  assess original-graph modularity, community connectivity, stability and
  downstream utility, alongside full RAM/disk/build/run/output costs. Never
  label an observed modularity ratio an approximation theorem or a guarantee
  against the unknown optimum. A pass/event budget may return an explicitly
  unfinished heuristic result, not falsely claim convergence or exact replay.

The new literature check supplies a concrete comparator for the second
track. [CluStRE, SEA2025](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.SEA.2025.11)
combines streaming clustering, a quotient representation and multi-stage
refinement. The publisher abstract reports memory/quality/speed trade-offs;
these are the authors' experiments, not independently reproduced Knight Bus
measurements. Its [official implementation](https://github.com/KaHIP/CluStRE)
describes light, restreaming, quotient-optimization and combined modes. Inspect
its preparation and retained quotient growth before claiming a hard job cap.
Publisher abstract and official README were inspected2026-09-21; full paper
and implementation have NOT yet been audited in this workstream.

[Weighted sketches for community detection](https://arxiv.org/html/2411.02268v1),
Sections4.1 and4.4, are already credited in the heterogeneous manuscript and
were revisited here. They replace per-thread candidate maps with weighted
Misra-Gries summaries, then rescan selected candidates; full reported space
still includes vertices and edges. Candidate truncation is not an exact
best-move certificate. Thus neither sketching candidates nor ignoring input
graph storage is a new low-RAM contribution. Use the pinned arXiv version,
not its visible placeholder conference metadata.

[Yun, Lelarge and Proutiere, NIPS2014](https://proceedings.neurips.cc/paper_files/paper/2014/hash/c308206b8de48a9af6add45ee4c0f93d-Abstract.html)
study streaming recovery under a specific sparse community model, including
different memory contracts for retaining versus emitting assignments. The
inspected abstract does not establish exact arbitrary-input modularity replay
or our whole-process resource cap. It does establish that streaming and
sublinear-state community research predate this project.

**Next decisive action:** read CluStRE's full method and source lifetime
before designing another community representation. Extract input, quotient,
assignment, refinement and output state bounds. Compare these to our strict
contract and identify a concrete unbounded term to remove or certify. Only
then propose a new storage/scheduling theorem or implementation. That focused
comparison is preferable to benchmarking another renamed range-tree variant.
Keep the already reviewed exact solver as a reusable oracle track, not as the
only acceptable product shape. No published comparator is declared equivalent
to our exact reduction merely because it also uses less memory.

## Reproduction And Pins

The native driver uses the retained Karate JSON; NetworkX is required only
if regenerating that fixture. The generated JSON pins NetworkX3.6.1 and the
source-module digest. Native receipts refuse overwrite. From repository root:

```sh
/Users/amuldotexe/.local/bin/python3.11 -B -W error research_algorithms_20260920/experiments/probe_native_community_gate.py --output /tmp/community-native-new-receipt.json
/Users/amuldotexe/.local/bin/python3.11 -B -W error research_algorithms_20260920/experiments/probe_threshold_repair_accounting.py --output /tmp/community-repair-new-receipt.json
```

Native receipt SHA-256:
`03760b10c0e7758d9e65dc46f74de030c74f021c7ce11eb01b74c4eaaabbbf7d`.
Structural repair receipt SHA-256:
`33d3505889a0ebb414992f3faa2415841e126a9e17451c1022884e860d55d64c`.
The second pins the first, which pins the source files and all data inputs.
Neither receipt is a measurement of the product's process/job RAM contract.
