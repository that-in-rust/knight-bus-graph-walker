# Hybrid Orbit Aggregation With Disjoint Triangle Ownership

Date: 2026-09-20. A07 follow-through from the masked-matching review. The mixed-case proof and finite implementation now have a [completed separate independent review](Triangles-Hybrid-Independent-Review.md). No production, external-memory or global-novelty claim.

## The Algorithmic Step

The [masked-matching interval algorithm](Triangles-Masked-Matching-Runs.md) can enumerate quadratically many closing shift triples even when the final answer is constant on each fibre. Full-period edge classes have additional translation symmetry. The review proved that ordinary cyclic convolution can count all their closing triples before enumeration.

The remaining question is how to use that aggregate inside a graph that also has arbitrary partial masks, without either missing mixed triangles or enumerating the full-class triples again. The answer is a disjoint ownership partition: assign every non-full triangle to its first partial edge class in a fixed canonical ordering.

```text
Canonical base triangle: a < c < z
Edge-class order:        (a,c), (c,z), (a,z)

              Class-mask statuses
                       |
             +---------+---------+
             |                   |
            FFF             contains P
             |                   |
       Convolution       first P owns it
             |          /        |        \
             |        P**       FP*       FFP
             |          \        |        /
             |          exact interval joins
             +-------------------+
                       |
            Complete local counts + degrees
```

F means the canonical mask is exactly [0,L); P means any other nonempty canonical mask. `*` means either F or P. Empty classes are absent. The four routes are mutually exclusive and exhaustive, including every interaction between partial insertions and removals. Classifying a mask as F requires validated/coalesced interval metadata; it is not inferred from a sample.

## Full-Class Aggregate

For a base triangle, let A, B, D be its full-mask displacement sets on the three ordered pairs. Define the cyclic multiplicity convolution

```text
w[s] = sum_x 1_A[x] * 1_B[(s-x) mod L]
k    = sum_(s in D) w[s].
```

Add L*k to the global count and k to the local triangle count of every vertex in each of the three fibres. This uses one weighted constant range per fibre, not k separately enumerated shift-triple ranges. Degrees still come from every actual edge mask, not just the full ones.

Each shift pair closes exactly when its sum is in D, and every closing pair gives one triangle for each root coordinate. Fixing a vertex at either non-root fibre uniquely recovers the root coordinate, so its local contribution is also k. Coefficients must be integer multiplicities, not Boolean existence. All vectors are nonnegative; this is not an application of a nonnegative-convolution theorem to signed defects.

The ordinary product of length-L indicator polynomials has length at most 2L-1. Fold coefficient s+L into s before querying D. No floating equality or approximate zero test is needed.

## Partial Ownership Join

Let G_i be all nonempty shifts on edge position i, F_i its full subset and P_i its partial subset. Execute exactly these three Cartesian-domain patterns:

```text
(P_0, G_1, G_2)
(F_0, P_1, G_2)
(F_0, F_1, P_2).
```

For each pattern choose the pair of domains with the smallest product of cardinalities. Derive the missing displacement using addition or subtraction modulo L, and check membership in the third domain. Every found triple is then processed using the original a-root alignment:

```text
I = S_ac intersect S_az intersect (S_cz - d_ac).
```

Changing the enumerated pair does not change the root or the output-coordinate shifts. Empty domains skip a pattern immediately. The implementation enumerates only actual base triangles, using smaller-neighborhood intersection; it does not enter a shift Cartesian product when the closing base pair is absent.

## Mixed-Case Correctness Theorem

Under the validated integer Boolean cross-base source contract, this hybrid returns the same complete local counts, degrees and global triangle count as the interval-only algorithm.

**Proof.** Every actual triangle has a unique ordered base triple, unique closing displacement triple and root coordinate. Its three mask statuses are either FFF or have a unique first P. In FFF, convolution counts its shift triple once and its root coordinate is one of the L coordinates represented by the aggregate. Otherwise exactly one of the three partial patterns includes it. Within that pattern, either two chosen displacements uniquely determine the third; membership checks enumerate the class triple once, and the original intersection includes precisely its legal roots. The cases are disjoint, so adding their contributions neither loses nor duplicates a triangle. Degree processing is unchanged. Weighted boundary events preserve exact integer local counts; the complete sweep includes isolated vertices. This proves equality for arbitrary mixtures, not just full connected components.

The proof extends the review's all-full specialization with an ownership rule. It does not claim a new convolution primitive or a new general join identity.

## Paid Cost Model

Let W_B be the paid base-triangle enumeration work. The finite implementation scans the smaller adjacency set for each base pair, so its `base_neighbor_probes` equals a sum of minimum base degrees, separately from shift work. Let D_B sum the three incident shift-set sizes over base triangles: the present implementation classifies full/partial shifts while visiting each triangle and pays D_B. Preclassifying once is a possible separate optimization, not silently assumed here.

For each partial pattern with domain sizes x,y,z, charge min(x*y,x*z,y*z) exact pair probes. Sum these as C_P. Let Q_P be closing partial triples, J_P their interval-reading upper charge and H_P their actual root intervals. For each eligible all-full base triangle t, let F_t be the complete cost of the selected exact convolution backend, including indicator construction, folding and querying its closing set. Then

```text
hybrid logical work = O(b + M + R + W_B + D_B + C_P + J_P)
                      + sum_t F_t + output work

full numeric output = Theta(bL) rows.
```

There is no Q_F term for enumerating all-full closing triples. The computed sum of k values may be large without allocating or visiting those triples. Do not confuse the `full_closed_triples` numerical aggregate with enumerated work.

For partial event emission use the reviewed bound `min(10H_P,6H_P+4Q_P_plus)`. Each all-full base triangle with k>0 adds six weighted boundary records. Degree records are at most 4R+2M. Thus a valid initial event bound is

```text
E <= 4R + 2M + min(10H_P,6H_P+4Q_P_plus) + 6B_F_plus.
```

These are actual-data quantities, not a free advance planning oracle. Use conservative source-derived allowances or charged preparation before promising disk admission. Weighted event payloads must store k, not just a one-bit sign. The original 256-bit signed-accumulator allowance is sufficient in the N<2^64 profile. Sorting, merge generations, indexes, source copies, old snapshots and output retention still count.

## What Is Actually Implemented

The existing research probe accepts `strategy="interval"` (unchanged mathematical baseline) or `strategy="hybrid"`. The hybrid implements base-triangle filtering, cheapest-pair partial joins, the four-way ownership partition, weighted aggregate events, complete rows and local/global/degree consistency checks.

Its exact convolution backend packs indicator coefficients into digits of a Python integer, multiplies the integers, decodes coefficients and folds them modulo L. The digit base is 256^width with `width=ceil(bit_length(L)/8)`, at least one; it is strictly larger than the largest ordinary convolution coefficient L. Therefore no coefficient carry corrupts the polynomial product. This is a finite carry-free arithmetic reference, not a claimed implementation of a sparse-convolution paper, NTT service or bounded external backend.

The reference materializes length-L indicator/digit buffers and a length-O(L) product; Python integers, event maps and all bL output rows remain resident. The algorithm currently chooses convolution whenever all three full subsets are nonempty, even when sparse pair enumeration would be cheaper. Therefore it is an exact alternative plan, not a universally faster optimizer. A production planner must compare admitted backends and charge their actual working storage; no physical-RAM cap is implemented by this probe.

The full-orbit optimization is related to established circulant/voltage algebra and nonnegative convolution. [Bringmann, Fischer and Nakos](https://arxiv.org/pdf/2107.07625), Theorem 1, supplies a deterministic sparse-convolution bound under its own word model. The reference here does not claim that bound. [Hu et al.](https://cs.uwaterloo.ca/~xiaohu/papers/sigmod22-temporal.pdf) supplies multiway interval-join precedent, but applying its exact runtime theorem to tuple-dependent cyclic mask shifts requires an explicit reduction, including any duplicated contexts. A hypothetical comparator copying this hybrid is not independent historical evidence; see [the evidence policy](Novelty-Baseline-Evidence-Policy.md).

## Executed Evidence

The independent review first found integer-domain failures in the earlier probe. A new ten-subcase regression test reproduced nine silently accepted inputs and one late TypeError. Strict integer validation now rejects fractional/bool IDs, shifts, coordinates, signs and dimensions before processing; degree conservation was added as a diagnostic. Eleven tests then passed.

Four new hybrid tests next failed because the strategy was not implemented. After implementing it, all fifteen tests passed, using:

```sh
python3.11 -m unittest discover -s research_algorithms_20260920/experiments -p test_masked_matching_triangles.py
```

- The same 4,096 exhaustive three-base/L=2 graphs were checked under both plans against independently expanded adjacency. These are not 8,192 independent input graphs.
- Another 160 deterministic-seed mixed full/partial cases were checked against every oracle degree/local count and total. Existing 180 interval cases and 80 independent flat-defect compilation cases remain passing.
- An irregular full-orbit L=31 fixture uses displacement-set sizes (24,18,17). One exact convolution computes k=236 without partial pair probes, returns T=7,316 and all 93 local rows with degrees (41,42,35) by fibre and local triangle count 236. This does not use a complete-tripartite closed form.
- A partial-mask L=128 fixture uses 128 hybrid pair probes versus 16,384 baseline pair probes, with identical complete rows and total. This is a logical join-work comparison, not a 128-fold elapsed-time claim.

The review's separate all-full, adversarial empty-intersection, event-sharpness and gauge checks remain attributed to the reviewer. The lead did not rerun all those unchanged probes. The new hybrid tests are directly executed lead evidence. There is no benchmark of a fast native convolution backend or a physical 4 GB run.

## Lifecycle And Next Research Step

Dense multi-shift full classes can be native input of size O(L), yet require Theta(L^2) flat defects relative to an old one-shift cover. The necessary flat-source scan then remains quadratic. The aggregate-before-Q separation is a query/native-format result, or an amortized result after charged compilation, not a subquadratic end-to-end claim for that flat source.

The independent challenge and precise closest-art comparison are now complete for this hybrid. Next is a backend/resource selector tested on both friendly and adversarial source regimes. Full-period symmetry, partial-mask run structure and output reuse are distinct measurable properties. The kernel removes an enumerated triangle-class term in an admitted regime, but doing so follows directly from published polynomial lift-walk algebra. Whether the broader encoded-input compiler and bounded execution can supply a paper contribution remains open.

## Independent Review Integration

The reviewer found no new valid-profile counting or packing bug, using 70 fresh concrete-graph oracle fixtures, 24 separate cost/event fixtures and 806 individual convolution-coefficient comparisons. These are reviewer-executed results; the lead read the report rather than rerunning them. The report also checks width transitions through L=65,537 and confirms the already disclosed sparse case where unconditional convolution wastes work. Those findings are not evidence of physical-memory enforcement.

The closest-art reductions are now explicit. [Dalfo et al., An algebraic approach to lifts of digraphs](https://arxiv.org/pdf/1612.08855), Lemma 4.1 and its circular-convolution discussion, specialize to `[z^0] A(z)B(z)D(z^-1)=k`. [DBToaster](https://dbtoaster.github.io/papers/pvldb2012-dbtoaster.pdf), Sections 3.2 and 5, supplies aggregate product distributivity yielding the four ownership terms. [Kara et al., Counting Triangles under Updates](https://drops.dagstuhl.de/storage/00lipics/lipics-vol127-icdt2019/LIPIcs.ICDT.2019.4/LIPIcs.ICDT.2019.4.pdf), Definition 6 and Section 3, supplies partitioned triangle views with different evaluation directions. None of these citations automatically transfers a runtime theorem to compressed masks or supplies our physical-memory implementation, but the identities themselves are established primitives.

A separate [periodic-refinement study](Triangles-Periodic-Refinement.md) now implements native repeated-mask sources, exact coordinate carries, compressed local output and a base-size/transform-length trade-off. It also derives a blocked-convolution control that can make refinement unnecessary on all-full sources. That new extension was excluded from this independent review and is not promoted as globally novel.
