# Residual-Degree Quotient Envelopes

Date: 2026-09-21. A04 quality/resource research track.

Status: exact-rational prototype and 11 lead tests pass. The independent
mathematical/prior-art challenge is complete and its checker is lead-replayed.
No physical RAM measurement,
native-workload advantage, globally novel theorem, or publication readiness is
claimed. The builder uses a known weighted frequent-item sketch; the contribution
under investigation is the resource/quality behavior of the resulting
original-objective envelope, not renaming that sketch.

Native follow-up: [140 fixed cases](Communities-Residual-Native-Results.md)
now show zero operational lower-bound gains or winner changes attributable to
the degree addition. The [pair-cap extension](Communities-Cancellation-Native-Results.md)
uses already implied sketch information and improves four finite-family
selection certificates, but no selected partition. Both are evidence updates,
not changes to this document's frozen prototype or its original proof.

## Why This Direction

The [source audit](Communities-CluStRE-Source-Audit.md) identifies a quotient
pair map that can remain edge-sized despite streaming original adjacency.
The earlier exact serial solver preserves decisions but adds an O(n) tree and
substantial update work. This note explores a different allowed contract:

> Keep a user-budgeted number of coarse edge counters, preserve the original
> modularity normalization, and certify what the omitted edges could change.

The output is a partition score interval, not a promise to reproduce GDS,
Louvain, Leiden, CluStRE, or a global optimum. Optimizer quality and resource
limits remain distinct questions.

### Alternatives

1. Exact external quotient aggregation: strongest simple exact baseline. It
   pays for external label joins, sort/coalesce, and optimizer I/O, but does not
   need to discard quality information.
2. All-cuts sparsification with original degrees: a known uniform approximation
   route. An epsilon cut approximation gives at most epsilon additive modularity
   error when original degrees and original total mass are kept.
3. Retained edge underestimates plus a residual-degree envelope: implemented
   logical candidate below. It has deterministic data-dependent bounds, can be
   very sharp on some residuals, and can be completely uninformative on others.

No selection among these is justified by the present small semantic probe.

## Exact Input Contract

Freeze an initial partition P0 of a nonnegative rational weighted undirected
input graph. The K base blocks become coarse nodes with stable IDs. A candidate
partition of coarse nodes represents a coarsening of P0; it cannot split a base
block. Original-ID expansion is a separate paid scan/join through P0.

The builder receives a once-per-undirected-edge contribution stream on these
coarse IDs. Duplicate pair contributions are permitted and summed logically.
Coarse self-loops represent original edges already internal to a base block;
they are counted once as edge mass and twice in weighted degree. Zero weights
are allowed; negative weights and invalid IDs are rejected.

Define W as original total undirected edge mass, d_i as original coarse degree,
and gamma > 0. For W > 0 the target is

```text
Q_G(P) = internal_G(P)/W - gamma * sum_C (volume_G(C)/(2W))^2.
```

An edgeless input needs a separately declared product convention; the current
score probe rejects W=0 rather than dividing by zero. Numerical results here
use exact rational arithmetic. Fixed-width overflow checks and floating-point
outward rounding are not implemented.

## Representation And Builder

```text
Frozen quotient contributions, consumed once
                       |
          +------------+-------------+
          |                          |
          v                          v
  Exact degree totals         At most b edge counters
  Exact self-loop mass        Weighted frequent items
          |                          |
          +------------+-------------+
                       |
                       v
       Residual degree r_i = d_i - retained degree_i
                       |
                       v
      Candidate labels -> certified score interval
```

All loop mass is retained exactly. Let h_ij be the final non-loop counter for
pair ij, with zero for untracked pairs. Use a weighted Misra-Gries/Frequent
control so that 0 <= h_ij <= w_ij. The residual graph R=G-H therefore has
nonnegative weights and no loops. Counter weights are underestimates, not
necessarily the full weights of selected original edges. Residual edges may
share a pair with a retained partial edge.

For each incoming positive non-loop contribution (key,w):

```text
if key tracked: add w
else if fewer than b counters: insert (key,w)
else:
    delta = min(w, minimum counter)
    subtract delta from every tracked counter; remove zeros
    if w > delta: insert (key,w-delta)
```

For b=0, retain no non-loop edge counters. Accumulate exact original degrees
and loop mass regardless. At the end subtract retained non-loop degrees from
exact non-loop degrees to obtain r. The underestimation invariant follows by
induction: updates never add more than observed weight, and cancellation only
decreases counters. It guarantees an actual nonnegative residual completion.

This is established sketch machinery. See the primary
[frequent-items paper](https://conferences.sigcomm.org/imc/2017/papers/imc17-final255.pdf)
and [counter-based algorithm survey](https://dimacs.rutgers.edu/~graham/pubs/papers/freqcacm.pdf).
The prototype directly scans counters on cancellation; it does not implement a
faster offset/heap variant or claim a new frequent-items algorithm.

## The Residual Envelope

For a proposed partition P and residual degrees r_i, define

```text
R     = sum_i r_i                  M = R/2
s_C   = sum_{i in C} r_i           a_C = max_{i in C} r_i
ell_C = max(0, 2*a_C-s_C)

L(P) = max(0, max_C s_C-M)
U(P) = M - max(max_C ell_C, (sum_C ell_C)/2).
```

Empty maxima are zero. The actual residual internal edge mass obeys
`L(P) <= internal_R(P) <= U(P)`. The proposed stronger statement is that both
endpoints are achievable over **all loopless, nonnegative rational weighted
completions with these degrees and unrestricted pair support**. The independent
[review](Communities-Residual-Degree-Review.md) confirms tightness with a
constructive proof; the lead read it and replayed its separate exact LP-vertex
enumerator. The bounds and their derivation are supplied below.

The feasibility condition is `max_i r_i <= M`, including the all-zero case.
An actual loopless residual satisfies it. Arbitrary supplied degree metadata
must be checked rather than trusted.

### Lower Bound

A community with residual volume s_C can send at most R-s_C weight outside.
Its internal edge mass is at least `(s_C-(R-s_C))/2 = s_C-M`. At most one
community exceeds M, so the lower bound is L.

For attainability, if every s_C <= M, realize all residual weight between
communities. A loopless weighted graph on community volumes exists because no
volume exceeds half the total; distribute its endpoints among member vertices
to match their degrees. This gives internal mass zero.

If a community D has s_D > M, send every other community's degree to D.
Internal degree remaining in D totals `2*(s_D-M)`. It can be distributed among
D's vertices with each internal degree <= s_D-M, since the largest vertex
degree is <= M. Realize those internal degrees within D. The resulting internal
mass is exactly s_D-M. The realizability condition is justified below.

### Upper Bound

For a community C, its largest residual-degree vertex can match at most
s_C-a_C of its degree internally. Therefore at least ell_C residual degree
must leave C. Let x_C be the eventual external degree of C. Necessarily

```text
ell_C <= x_C <= s_C,
max_C x_C <= (sum_C x_C)/2.
```

The smallest possible cross-edge mass is therefore at least
`max(max_C ell_C, sum_C ell_C/2)`. Subtract from M to get U.

For attainability, start x_C=ell_C. If these values satisfy the half-total
condition, keep them. Otherwise exactly one community D dominates. Keep
x_D=ell_D and increase other x_C values until their sum is ell_D. Their total
capacity suffices because

```text
ell_D = 2*a_D-s_D <= 2*M-s_D = sum_{C != D} s_C.
```

Each community's desired internal degree total is T_C=s_C-x_C. The condition
x_C>=ell_C gives `T_C <= 2*(s_C-a_C)`. Choose internal vertex degrees t_i with
`0<=t_i<=min(r_i,T_C/2)` and sum T_C; the displayed condition and T_C<=s_C
make this possible. These degrees admit a loopless weighted realization.
The remaining vertex degrees sum to x_C. Realize the community-level external
degrees x_C and distribute each community's external endpoints among its
vertices. This constructs the upper endpoint.

The elementary weighted realization fact used here is: nonnegative rational
degrees z admit a loopless graph with unrestricted rational edge weights iff
`max(z) <= sum(z)/2`. One construction places consecutive intervals of lengths
z_i on a circle of circumference sum(z) and pairs points half a circumference
apart. No interval overlaps itself under that shift. Interval intersections
give rational undirected pair weights with the requested degrees. This is a
fractional-degree construction, not the simple unweighted graphical-sequence
criterion.

### What Tight Means Here

The information model matters. Integer-only weights, forbidden pairs, known
original support, or a known number of residual edges can tighten the interval.
Ignoring those extra constraints keeps the envelope safe but may make it loose.
Do not claim an information-theoretic lower bound for every possible synopsis.
The claim concerns only the relaxed degree-only completion class.

## Original-Objective Certificate

Let J(P) be the exactly retained internal mass: all retained loops plus h_ij
for pairs placed together. Compute original community volumes from d, not r
or h. Then

```text
tax(P) = gamma * sum_C (sum_{i in C} d_i/(2W))^2
Qlo(P) = (J(P)+L(P))/W - tax(P)
Qhi(P) = (J(P)+U(P))/W - tax(P).
```

These bound the original graph's score for every candidate partition, including
candidates selected adaptively after inspecting the summary. This is a
deterministic uniform validity statement, not a union bound over a random
sample or a guarantee that every interval is narrow.

An optimizer may retain the candidate with the largest lower bound. If the
base singleton partition of coarse nodes is included, its residual internal
mass is exactly zero and its score interval is exact. Therefore the selected
candidate's original modularity is at least that baseline score. A stronger
improvement claim requires a strictly higher lower bound.

For a finite considered candidate family F, returning P_best gives the
certificate `max_{P in F} Qhi(P) - Qlo(P_best)` on regret within F. This is
not regret against partitions never considered, unrestricted vertex splits,
or a global modularity optimum. The current prototype implements the builder
and scoring, not a complete optimizer or this selection workflow.

## Strong Controls

### Mass-Only Residual Control

The immediate control is `0 <= internal_R(P) <= M`. The new envelope cannot
be wider. A group-volume-only control strengthens the lower endpoint to L
but misses the forced external degree from a single dominant vertex. The
upper endpoint above also enforces that external degree must be matched at
both ends, rather than independently bounding each community.

### Cut-Sparsifier Control

For H satisfying an all-cuts `(1 +/- epsilon)` guarantee for G, summing cluster
boundary cuts and dividing by two also bounds the multiway cut. With original
W and original degree tax, define `Qhat=1-cut_H(P)/W-tax_G(P)`. Then
`abs(Qhat-Q_G)<=epsilon*cut_G(P)/W<=epsilon`. An eta-optimal solution of this
surrogate is within eta+2*epsilon of the original optimum **over the same
search space**, if that optimization guarantee is actually available. A
heuristic does not inherit eta=0. The same reasoning appears as an elementary
control in the independent [CluStRE paper audit](Communities-CluStRE-Paper-Audit.md).

Graph sparsification, quotient reduction and linear-work multilevel execution
already have substantial prior art, including
[ESA2025 coarse-edge sparsification](https://arxiv.org/html/2504.17615v1).
Our formula must survive comparison with those representations and with
ordinary fractional degree-completion bounds. A distinct application alone
is not enough for a novelty claim.

### Closer Sketch And Degree-Realization Work

[Sahu's weighted-sketch community paper](https://arxiv.org/html/2411.02268v1),
section4.1, already uses weighted MG not only for per-vertex candidate tallies
but also to create a reduced supernode graph during aggregation. Our global
canonical-pair budget differs from its per-supernode sketches; that distinction
alone does not establish novelty. The candidate addition here is an explicit
original-degree score certificate and its resource/quality consequences.

The independent review connects the degree-completion proof to
[fractional perfect b-matching](https://arxiv.org/abs/1301.7356) and the adjacent
[fixed-partition graph-realization literature](https://arxiv.org/abs/1508.00542).
Treat our closed form as a complete-support specialization of classical
degree-constrained realization, with an application to residual modularity.
Failure to locate these exact formulas in the bounded search is not proof
of priority. Additional algorithmic or experimental separation is required.

## Resource Ledger

For K coarse nodes, T contribution records and counter budget b:

- Builder: O(K+b) logical records; no full q-pair quotient dictionary. The
  current simple cancellation loop costs O(T*b+K+b) rational operations in the
  worst case for b>=1, and O(T+K) for b=0. Dictionary lookup is runtime-dependent.
- Scoring one candidate: O(K+b) operations and O(number of candidate communities)
  accumulator records, in addition to the retained summary and candidate labels.
- Candidate generation, original-to-coarse mapping, output lifting, input
  normalization and full original-data retention are not included in those
  small-kernel bounds. They remain mandatory workflow charges.
- Exact rational numerators/denominators can grow. Record counts are not a
  byte cap; the prototype makes no 5 GB or 10 GB promise.
- Python object overhead, tuple-copy peaks and counter-key snapshots remain
  real allocations. A fixed b bounds counter cardinality, not process RSS.
- If K does not fit, this representation alone does not solve the problem.
  External arrays or a different coarse-node representation would be needed.

## Falsification Evidence

Implementation: `experiments/probe_residual_degree_envelope.py`.
Tests: `experiments/test_residual_degree_envelope.py`.
Pinned hashes, seeds and replay counts are retained in the
[semantic receipt](evidence/community-residual-degree-20260921/receipt.json).

Five absent-module failures were observed before the envelope implementation.
Four absent-function failures were observed before adding the bounded builder.
All 11 tests now pass using:

```sh
cd research_algorithms_20260920/experiments
/Users/amuldotexe/.local/bin/python3.11 -B -W error -m unittest test_residual_degree_envelope -q
```

Coverage includes 10,935 residual containment checks (all 729 four-node edge
weight assignments in {0,1,2}, all15 partitions); 180 seeded one-pass weighted
stream summaries with b=0..5; 1,440 full original-score interval comparisons;
609 exact hub score comparisons (203 partitions, three resolutions); loops,
isolates, invalid metadata, and a full-counter-budget exactness case. These
are semantic checks, not scale or timing experiments.

The separate review enumerates basic feasible completions with exact rational
linear algebra, not the lead implementation. Lead replay on Python3.11 exits0:
363 degree vectors,293 feasible,12,073 exhaustive endpoint pairs plus109
rational endpoint pairs, giving24,364 endpoint comparisons. Two restricted-
support cases and15 integer perfect matchings demonstrate the stated limits.
Six degree-one vertices in two triples allow upper mass3 with half-weight
triangles but only2 with integer edge weights. Hence rational tightness must
not be relabeled integer tightness. The combined current community suite
passes74 tests; this review does not audit the sketch implementation or physical
resource accounting.

### Positive Witness And Its Limitation

Residual degrees `(5,1,1,1,1,1)` force a star. For labels `(0,0,1,1,1,1)`, the
envelope gives internal mass exactly1, versus mass-only `[0,5]` and the weaker
independent-per-community upper bound3. This is a genuine tighter certificate.
However, a star has only K-1 edges already. It does not demonstrate asymptotic
storage savings against a competent sparse adjacency baseline.

### Indistinguishable-Input Obstruction

Take eight vertices, split into two groups of four. Graph A is two disjoint
K4 cliques. Graph B is K4,4 minus a perfect matching. Both have degree3 at every
vertex, 12 unit edges, no loops, and exactly the same b=0 summary. For the
declared two-group partition and gamma=1:

```text
Graph A: internal=12, Q=+1/2
Graph B: internal= 0, Q=-1/2
Envelope for both: [-1/2, +1/2].
```

No certificate using only this shared summary can distinguish them. The bound
is not merely weak because of poor constants: the summary has discarded the
relevant information. This falsifies a universal high-quality, arbitrarily
small-b claim, including on a graph with perfectly separated communities.

### Normalization Trap

A six-node star with one retained unit edge has original modularity -8/25
for `(0,0,1,1,1,1)`. Recomputing modularity on only the retained edge gives0.
The test preserves this difference. A good sparse-graph score is not necessarily
a good score for the original graph.

## Next Decision Gate

Before optimizing the sketch or claiming a paper contribution:

1. Independent proof/prior-art review is complete at its declared scope;
   preserve its rational-completion restriction and classical-theory attribution.
2. Freeze native initial partitions and candidate-generation policy. Include
   small and large K, hub-heavy and balanced-degree residuals, and a declared
   b schedule. Retain every uninformative interval and baseline-only outcome.
3. Compare exact original modularity, lower-bound-admitted improvements, interval
   widths and complete workflow cost against mass-only, group-only, exact
   external aggregation and a competent cut-sparsifier control.
4. Continue only if the degree envelope or a precisely identified added summary
   has useful quality/resource separation. Otherwise preserve this as a strong
   control and an impossibility example, not a renamed new clustering algorithm.

The seven-family goal remains open. The concrete advance here is a bounded
quotient construction, a derived certificate with explicit assumptions, and
a testable obstruction that identifies when additional structural information
is necessary.
