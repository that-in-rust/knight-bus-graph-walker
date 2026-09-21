# Periodic Refinement As An Exact Triangle Memory Dial

Date: 2026-09-20. Exact derivation and finite reference implemented; ten focused tests pass. No physical-memory or global-novelty claim. This document retains the scoped research implementation plan; no production graph engine is being changed.

## Problem And Proposed Design

The hybrid triangle algorithm removes all-full class enumeration but its exact convolution reference needs workspace proportional to L, the number of coordinates per fibre. Periodic partial masks are also treated as arbitrary partial masks and can be expanded repeatedly by interval joins.

Proposed operation: verify a supplied common mask period p dividing L, then change coordinates from g to `(g mod p, floor(g/p))`. The graph gains p times as many base vertices, while each new fibre has only h=L/p coordinates. Every admitted mask becomes full on its refined fibre. Keep exact displacement carries: an ordinary unlabelled quotient would lose triangle closure information.

Three plans must be compared: direct cheapest-pair joins (tiny numerical scratch but potentially large work), exact convolution at the original or refined scale, and conventional blocked convolution. The refinement is an exact representation option, not automatically the best plan. Its purpose is to expose a measurable base-storage/workspace trade-off and to absorb periodic partial masks without pointwise root expansion.

## Verification-First Plan

1. Create `experiments/test_periodic_triangle_refinement.py` with explicit missing-function failures. Check invalid periods, all degrees/local counts, wrap carries, independently expanded random graphs and a large repetition count without full output materialization.
2. Create `experiments/probe_periodic_triangle_refinement.py`. Validate the original integer Boolean profile; certify period using canonical mask shifting, compile only residues in [0,p), and count full refined classes. Return bp residue rows and a separate lazy full-output iterator.
3. Exercise both exact pair enumeration and the existing carry-free integer convolution reference. Count compiled classes, refined vertices, actual base-neighbor probes, pair probes and maximum convolution length; do not label these RSS or time.
4. Derive the memory/work frontier, include compiler/input/output and blocked-convolution controls, and record failure cases. Do not call a coordinate change globally novel.
5. Integrate the independent review of the pre-existing mixed hybrid separately. Its conclusions do not automatically review this refinement.

## Current Status

All five planned research steps have supporting material below. The [separate mixed-hybrid review](Triangles-Hybrid-Independent-Review.md) is complete and integrated into its manuscript; this refinement was excluded from that review and has not been independently reviewed. The ordinary blocked-convolution control narrows its value: changing coordinates is not automatically a better memory dial than blocking the existing arithmetic.

## Exact Source Contract

Use the same simple undirected cross-base graph as the masked-matching manuscript: vertices `(a,g)`, `0<=a<b`, `g in Z_L`; class `(a,c,d)` contains edges `(a,g)--(c,g+d mod L)` for selected roots g. There are no same-base edges, loops, multiplicities or approximate membership tests.

Two source encodings are admitted:

- Expanded interval masks give all valid root intervals in [0,L), as before.
- Native periodic masks give a positive integer q dividing L and canonical Boolean intervals U in [0,q). A root belongs exactly when `g mod q` belongs to U. Each class may have a different q. Empty masks are absent after normalization.

The second encoding is essential. Requiring the caller to enumerate every repeated interval would spend the space and scan time that the representation is intended to avoid. The finite implementation accepts an explicit `mask_periods` mapping with exactly the class keys; without it, q=L. It rejects fractional/bool dimensions and identifiers, invalid q, overlapping intervals and invalid displacements. This is a source semantics declaration, not a claim that arbitrary CSV automatically has this structure.

A candidate refinement p must be a positive divisor of L and leave every mask unchanged under translation by p. Test the latter by rotating and comparing canonical interval lists in each mask's native q-coordinate, not by scanning L roots. A sufficient choice is a common multiple of all q values dividing L; the implemented check also accepts smaller p when a declared q is not the mask's minimal period. No factorization of L, period discovery or free arbitrary relabeling is assumed.

## Coordinate Compiler And Proof

Set h=L/p. Every old vertex has a unique representation

```text
old vertex:     (a, g)
                     g = i + p*j
refined vertex: ((a, i), j),  0<=i<p, 0<=j<h.
```

For each class `(a,c,d)` and each admitted residue `i in [0,p)`, emit a full refined class

```text
base source = a*p + i
base target = c*p + ((i+d) mod p)
displacement = floor((i+d)/p) mod h.
```

It represents every j in [0,h). The compiler enumerates only admitted residues in the first p coordinates, using the native pattern and enough copies to cover that prefix. It does not expand the remaining h repetitions.

**Isomorphism theorem.** The compiler is an exact bijection of actual vertices and actual edges, including modular wraparound.

**Proof.** Write `i+d=i'+p*s`. Then `(i+p*j+d) mod L = i'+p*((j+s) mod h)`, which is precisely the emitted edge's destination. Period invariance makes membership of every root `i+p*j` equal to membership of i. Conversely, a refined source residue, target residue and shift recover `d=(p*s+i'-i) mod L` uniquely. Thus different canonical classes cannot accidentally merge. Original base ordering a<c also orders every refined endpoint correctly. The compiler changes coordinates, not adjacency.

Omitting the `i` in the carry is unsound. A retained mutation changes the carry to `floor(d/p)`: for L=6, p=2 and triangle shifts (5,3,2), the mutated solver returns zero triangles instead of six. This is an executed mutation, not an illustrative warning.

**Compressed local-answer theorem.** Every vertex in a refined fibre has the same degree and local triangle count. For each refined base triangle, exact shift closure modulo h gives multiplicity k; add k to its three residue counters and h*k to the global total.

**Proof.** Translation of all j coordinates by one is an automorphism. More directly, any closing shift triple yields precisely one triangle through each vertex in each of its three fibres, and h actual triangles in total. Non-closing triples yield none. The degree contribution of one full refined class is one at each endpoint. Summing over unique ordered base triangles and classes gives exact local counts and degrees. The complete answer is recovered by looking up residue `(a,g mod p)` for each original vertex.

This is not triangle counting on an ordinary unlabelled quotient. The displacement labels and closure test are indispensable. At p=L, h=1 the representation degenerates to the expanded graph; at p=1 it recovers an admitted all-full voltage representation.

## Exact Resource Frontier

Let R be native input runs, M be nonempty classes and E the number of actual graph edges, calculated from the source as `sum_classes (L/q)*|U|`. Let F_p be the number of emitted full refined classes. For every admitted p,

```text
F_p = E*p/L, exactly.
residue output rows = b*p.
remaining fibre length = L/p.
```

The identity follows because every refined class represents exactly h distinct edges. It permits an exact count reservation before compiling every candidate p. It is not an approximate cardinality estimator. Counter widths and the actual retained record layout still have to be specified before converting counts to bytes.

The reference's lifetime includes normalized source runs plus emitted classes during construction, then class dictionaries, pair indexes, base adjacency, degree/local arrays and returned residue tuples. Its logical state is

```text
O(R + M + F_p + b*p) + W_backend(L/p).
```

`W_backend` includes arithmetic scratch, not just input/output digits. For the pair backend it excludes a dense length-h buffer and uses the already retained shift membership sets. For the Python-integer convolution reference, digit width grows with log h; integer multiplication's temporary allocation is not enforced by our code. This formula is not a measured RSS bound or a physical 4 GB admission implementation.

Suppose a future packed backend has a justified conservative reservation `beta*h`, and compiled graph/counter storage has a justified linear reservation `alpha*p`. With fixed costs S, the provisional reservation is

```text
S + alpha*p + beta*L/p.
```

Its continuous minimum is at `sqrt(beta*L/alpha)` when alpha>0, but only certified divisors p are admissible. Check candidate reservations with exact arithmetic. This is a planner formula conditional on a real backend/layout bound; the Python probe does not implement those byte constants or prove that the continuous optimum is attainable. Non-monotone bit widths can be handled by per-candidate bounds instead of assuming one beta.

Let W_p be actual smaller-neighborhood base enumeration work and B_p the refined base triangles visited. Pair work is the sum over those triangles of the cheapest two incident shift-set cardinality products. Convolution work is `sum_t C_backend(h,t)`, including dense indicator construction, product decoding and membership queries. Construction additionally pays native interval sorting/validation, repeated-prefix traversal and F_p records. Since each nonempty native run contains at least one integer root, repeated complete pattern scans are bounded by emitted roots; remaining prefix scans are charged to R.

A loose dense arithmetic model gives `B_p * O(h log h)` for an appropriate exact transform backend, not the present Python multiplication implementation. B_p may grow as fast as the original base-triangle count times p^3. A smaller transform can therefore cause substantially more total work.

## Measured Logical Trade-Off, Not A Speed Claim

For L=60 and full shift sets on the three base pairs consisting respectively of multiples of 2, 3 and 5, both implemented backends return T=7,200. The following counters were executed directly:

| p | Compiled classes F_p | Residue rows | Largest convolution length | Convolution calls | Cheapest-pair probes |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 62 | 3 | 60 | 1 | 240 |
| 2 | 124 | 6 | 30 | 4 | 240 |
| 3 | 186 | 9 | 20 | 9 | 360 |
| 5 | 310 | 15 | 12 | 25 | 600 |
| 10 | 620 | 30 | 6 | 100 | 1,200 |
| 30 | 1,860 | 90 | 2 | 900 | 3,600 |
| 60 | 3,720 | 180 | 1 | 7,200 | 7,200 |

At p=60, base-neighbor probes are 137,040 versus six at p=1. This is a concrete reason not to market transform-length reduction as latency reduction. All rows agree with the stated global total; this table is a source-structured logical experiment, not a real workload or Neo4j/GDS comparison.

## Strong Comparator: Block The Existing Convolution

For an all-full base triangle, split each length-L indicator polynomial into chunks of B coefficients. Multiply every relevant pair of chunks using an admitted exact backend. A chunk product has at most 2B-1 coefficients. For every product coefficient, add its multiplicity to the scalar count only if its global index modulo L is in the closing displacement set. Do not retain an entire length-L result vector.

This directly constructed control has O(M+B) logical resident state for supplied shift indexes and one chunk product, plus backend scratch. In a dense arithmetic model there are O((L/B)^2) products, costing O((L^2/B)*log B) arithmetic work with an appropriate transform. Source reads, coefficient widths and conversion remain paid. The sparse cheapest-pair alternative is another required control.

The control does not duplicate the graph's base vertices or shift classes. Therefore a claim that representation refinement is necessary to bound all-full convolution memory would be false. It is a stronger comparator constructed here, not evidence that our exact graph compiler was already published. Whether refinement wins for particular partial-periodic masks or locality layouts needs a matched experiment; no general dominance or novelty claim is made.

For native periodic partial masks, refinement first exposes a full-class problem; blocking is then available at the smallest admitted refinement too. Increasing p beyond that point must justify itself against this combined control, not just against a dense original transform.

## Avoid A Global Period Requirement

A more promising follow-on is **motif-local period composition**. A single aperiodic edge mask should not force every unrelated triangle to use p=L. For each original base triangle t, derive an admitted p_t from just its three incident pair families and execute its refined kernel separately. Retain its three periodic local-count vectors as additive contributions. Compute degrees separately from edge classes; summing each triangle's degrees would overcount edges.

For a vertex `(a,g)`, the local triangle count becomes the exact sum of incident contribution vectors evaluated at `g mod p_t`. The global total is the sum of each base triangle's total, because each actual triangle has exactly one original base triple. Neither operation requires taking a global least common multiple of all p_t values. Retained local-contribution cells are bounded by `3*sum_t p_t`, before combining identical periods. Degree representations need separate storage. A shared base can have a huge combined period even when this additive representation is small.

This specific retained-vector variant is a derivation, not an implemented result. It must pay repeated pair reads, each local compile, output summation, and disk traffic. Evaluating every contribution for every vertex can be expensive. Contributions can be stored externally and replayed into a fixed-size output block, at the cost of repeated scans. No free constant-time arbitrary output lookup or cheap refresh follows.

**Implemented successor:** [Shared-residue composition](Triangles-Shared-Residue-Composition.md) now realizes the motif-local goal by a different representation: retain native predicates, compute each closing triple's scalar count with CRT marginals, and regenerate its local predicate contributions into bounded output blocks. It does not retain the 3*sum_t p_t vector cells described above. Its independently reviewed scalar identity has close published antecedents; event work can still be exponential in period bit length. [Elementary arithmetic controls](Triangles-Elementary-Counting-Control.md) are implemented for the simplest such adversaries. This successor changes the next research question; motif-local composition in general is no longer merely an unimplemented proposal.

The research question is whether this local-period/output-composition compiler has a useful time-space separation against a comparator given the same local periodic source. Merely beating a deliberately global-LCM implementation would be insufficient.

## Prior Art And Claim Ledger

| Inspected primary source | Relevant established result | Boundary of this manuscript |
| --- | --- | --- |
| [Dalfo et al., On quotient digraphs and voltage digraphs, 2017](https://ajc.maths.uq.edu.au/pdf/69/ajc_v69_p368.pdf), Sections 3-4, Lemmas 3.1 and 4.1 | Group-labelled lifts have structured adjacency; polynomial coefficients encode lifted walk counts | Voltage representation and recovering short closed-walk counts are established. The interval-period certificate, carry-explicit compiler and paid planner are our concrete derivation, not evidence of priority |
| [Barrett, Francis and Webb, Equitable Decompositions of Graphs with Symmetries](https://arxiv.org/pdf/1510.04366), v2, Lemma 2.4 and Theorem 3.8 | Uniform automorphisms induce block-circulant structure and smaller spectral blocks | Exploiting symmetry is not new. We keep exact integer local counts and label carries, rather than treating one unlabelled quotient's spectrum as the whole answer |
| [Bringmann, Fischer and Nakos, Sparse Nonnegative Convolution](https://arxiv.org/pdf/2107.07625), Theorem 1 | Sparse nonnegative convolution has established specialized algorithms under stated models | Our carry-free multiplication is an exact finite reference, not an implementation or experimental validation of their sparse bound |

The first two sources' relevant definition/theorem passages were read directly in this continuation. The sparse-convolution paper was also inspected during the preceding hybrid work. The publisher PDF initially failed access but succeeded on retry; no missing full-text claim is hidden. The polynomial-ring notation in extracted text is not used as an implementation specification: all our closure operations are explicitly modulo the cycle length, preserving wraparound.

There is no claim here of inventing graph covers, cyclic convolution, symmetry reduction, blocked multiplication, or periodic compression. The candidate scientific contribution would have to be a nontrivial compiler/selector/composition result with a demonstrated applicable source regime. The global refinement alone is not yet such a contribution.

## Executed Verification And Limitations

The first seven tests failed on an explicit missing-function assertion, then passed after the compiler and compressed solver were implemented. Three native-period tests next failed because the API had no `mask_periods` argument; after implementing that source contract, ten tests passed. Together with the unchanged fifteen masked/hybrid tests, the focused suite reports 25 passing tests:

```sh
python3.11 -m unittest discover -s research_algorithms_20260920/experiments -p 'test_*triangle*.py'
```

Evidence includes:

- 120 new deterministic-seed periodic graphs, each checked under pair and convolution backends against independently expanded adjacency, every degree/local count and total. Two backends on one input are not two independent input graphs.
- Full-mask carry/closure cases at four refinement divisors, an alternating partial-mask case, empty graph/isolates and six storage/workspace trade-off settings.
- Invalid/refuted mask periods, nondivisors, booleans, fractional shifts and mismatched native-period mappings are rejected.
- A mixed native source uses q values 1, 2 and 3; both backends match all 36 expanded rows at p=6.
- A symbolic native source at L=10^12 with three alternating masks stores three input runs, compiles three full classes and returns six residue rows with T=5*10^11. A separate full-mask case returns three rows. The pair backend avoids a length-L transform. Neither case enumerates all logical vertices or demonstrates their full-output delivery speed.
- An independently edited in-memory carry-omission mutant returns zero rather than six on the retained closure fixture. No on-disk source mutation was retained.

The output iterator expands original IDs in order using bounded incremental state, but producing bL rows still costs Omega(bL) work and bytes. A caller converting it to a list restores full output residency. Native arbitrary-precision arithmetic, Python containers, physical memory enforcement, external compilation, dataset eligibility, refresh/recovery and real full-output benchmarks remain unimplemented or unverified.

## Decision

Retain periodic refinement as a tested exact source/layout option and a way to expose, rather than hide, memory/work trade-offs. Do not promote it as a new general triangle algorithm or universal speedup. Motif-local composition now has the native-predicate successor linked above. The next scientific step is to establish a nontrivial useful separation against same-source elementary/local/blocked and fixed-dimensional counting controls, with complete output and source-preparation costs attached; another coordinate encoding by itself is insufficient.
