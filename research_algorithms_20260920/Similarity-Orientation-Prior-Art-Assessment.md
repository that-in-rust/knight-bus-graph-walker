# A05: Independent Closest-Art Assessment

Date: 2026-09-21. Bounded literature and mathematical challenge. Only this file is owned or edited. No candidate code was read, imported, changed, or run; no public timing, long test suite, commit, or push was performed.

## 1. Premise Check And Verdict

**Retain the specific reduction as a candidate contribution; do not claim publication priority or a new optimization primitive.** The inspected primary texts establish strong-backdoor enumeration, laminar cardinality optimization, discrete concave convolution, and related reconstruction/search algorithms. None of the inspected procedures supplies the complete two-set, exact-union, attained-subtree-maximum envelope theorem with this parameter. That is a bounded comparison result, not evidence of global absence.

The strongest challenge is a composition of established ideas: identify a small set of disjunctive choices, condition them, and solve integral laminar allocation problems. What must still be attributed to the candidate is the *model-specific proof* that the choices are precisely the strict splits, that fixing them removes union coupling at the count level, and that either row's best overlap remains individually realizable with a companion. Saying an incumbent could adopt those steps would not establish earlier publication.

There is useful further work beyond another implementation: additive-region contraction appears to sharpen the arithmetic bound from `O(G*2^r)` to `O(G+(r+1)*2^r)`. Section 6 gives a constructive proof sketch and a limited local check, not an independently accepted extension of the reviewed theorem. A separate meet-in-the-middle improvement applies to the covering-gadget subfamily, not automatically to arbitrary metadata.

### Scope Read

Read the four requested notes, without following their links into the rest of the portfolio:

- [Similarity-Orientation-Parameter-Recovery.md](Similarity-Orientation-Parameter-Recovery.md).
- [Similarity-Orientation-Parameter-Review.md](Similarity-Orientation-Parameter-Review.md).
- [Similarity-Overlap-Frontier-Compression.md](Similarity-Overlap-Frontier-Compression.md).
- [Novelty-Baseline-Evidence-Policy.md](Novelty-Baseline-Evidence-Policy.md).

Their implementation checks, hardness review, and negative public measurements are reports from those notes, not independently rerun evidence here. This assessment does not close the wider research objective.

### Exact Claim Under Challenge

There are exactly two sets, with exact union `U` partitioned by a binary tree with `G` leaves. At every node, `H_v=|U_v|` and `C_v=max(|T0 intersect U_v|,|T1 intersect U_v|)` is **attained**, not merely an upper quota. Root row sizes are `u=C_root` and `ell<=u`. Query information is leaf counts `q_j` and total size `a`, including elements outside `U`.

The output is the largest Jaccard value of either row over **all compatible complete pairs**, not a reconstruction of the original hidden pair. Zero overlap scores zero, including empty/empty. With `D_v=C_left+C_right-C_v`, the proposed evaluator enumerates at most `2^r` orientations, where `r=|{v:D_v>0}|`, and uses two capped-linear row optimizations per branch. Its reviewed cost is `O(G*2^r)` arithmetic operations and `O(G)` integer words; binary integer widths, exact fraction operations, histogram preparation, and explicit witness emission have separate costs.

## 2. Lenses And Search Boundary

The comparison used four lenses: disjunctive optimization, reconstruction/identifiability, database summary semantics, and adversarial parameterized complexity. Six focused query clusters were used, with targeted fulltext follow-through rather than a citation census:

| Cluster | Names and queries explored | Useful outcome |
|---|---|---|
| 1. Allocation/disjunction | Laminar resource allocation; separable concave/nested constraints; piecewise-linear allocation; Balas disjunctive programming | Murota's closure results; Fife-Oxley redundancy lemma; explicit conditioned-flow formulation |
| 2. Two-vector reconstruction | Haplotype assembly; two binary strings; max/union aggregates; reconstruction from unlabeled sums | Exact haplotype DP, but different observations and complementarity |
| 3. Database summaries | Abstract interpretation of database query languages; aggregate MAX consistency/reconstruction; set synopsis and Jaccard bounds | Direct count-vector/query-node filtering precedent; abstract-interpretation fulltext access gap |
| 4. Branch parameters | Backdoors; mixed-integer branching; conflict variables; strong backdoors to tractable subsolvers | Precise published precedent for exhaustive conditioning on a supplied small variable set |
| 5. Better algorithms | Automaton-constrained tree knapsack; budgeted laminar matroid; subset-sum/knapsack meet-in-the-middle | Numeric-state and exponential-time alternatives with explicit model/resource limitations |
| 6. Measurement aliases | Max-affine/max-linear regression; two-vector maximum measurements; unlabeled sensing; union/subtree/laminar reconstruction | A genuinely close measurement equation, but a statistical recovery theorem rather than an exact compatible-world envelope |

All substantive external evidence below comes from accessible author/institutional or arXiv fulltexts, inspected on this date. PDF pages are one-based; printed page numbers are specified when different. Search snippets, abstracts alone, secondary summaries, and inaccessible texts are not positive evidence.

Access limitations: Halder-Cortesi's *Abstract interpretation of database query languages* author PDF could not be retrieved; the direct request timed out. Balas's 1979 chapter appeared in an accessible collected-volume response, but subsequent page retrieval failed, so no theorem from it is used. These remain priority-search gaps, not negative matches. A general resource-allocation paper by Schoot Uiterkamp et al. was screened, but no uninspected theorem from its survey is credited here.

## 3. Closest Verified Sources

### S1. Conditional Optimization: Murota

[Kazuo Murota, *Discrete Convex Analysis: A Tool for Economics and Game Theory* (2016), fulltext](https://www.mechanism-design.org/arch/v001-1/p_05.pdf). Printed p. 190 / PDF p. 40, equations (4.34)-(4.35), give separable and laminar concavity. Printed p. 196 / PDF p. 46, Theorem 6.1(5),(9), gives interval restriction and integer supremal-convolution closure; the proof of (9) follows on pp. 197-198. The paper calls the operation **"Integer (supremal) convolution"**.

This directly supports the conditional allocation algebra. Maximizing `sum min(q_j,x_j)` subject to laminar integer quotas is not a new general class; preserving concavity under a merge is not a new theorem. The particularly simple slopes `{1,0}` permit the candidate's four-scalar implementation. These cited sections do not establish the two-row union-completion lemma, strict-split count, or a frontier-size theorem after taking the maximum over orientations. This is an author exposition with a proof, not a claim that 2016 is the origin of these foundations.

### S2. Branching Parameter: Williams, Gomes, Selman

[Ryan Williams, Carla P. Gomes, Bart Selman, *Backdoors To Typical Case Complexity* (2003), author fulltext](https://www.cs.cornell.edu/gomes/papers/backdoors.pdf). PDF p. 3, Definitions 2.2-2.4 and the paragraph after Definition 2.4 distinguish subsolvers, weak backdoors, and strong backdoors. The enumeration prescription is explicit: **"Simply check all possible assignments of V."**

The corresponding A05 interpretation is a supplied strong backdoor of strict-split bits to a tractable count-allocation solver: *every* complete assignment can be solved or rejected efficiently. The published definition is for CSP solving, not this optimization contract. The candidate still has to exhibit the backdoor from metadata in `O(G)`, prove that no additive/tie choices were missed, and establish the residual solver. A search over all small variable subsets is unnecessary here. Do not present `2^r` enumeration itself, or the concept of branching on conflicts, as novel.

### S3. Laminar Redundancy: Fife And Oxley

[Tara Fife and James Oxley, *Laminar Matroids*, arXiv:1606.08354v1 (2016)](https://arxiv.org/pdf/1606.08354v1). PDF/printed p. 1 defines the upper-cardinality independent sets. Section 2, pp. 3-4, defines essential constraints and proves Lemma 2.3: **"If c(A) >= b(A), then A is not essential."** Here the inequality is transcribed in ASCII; `b(A)` is residual ground-set size plus child capacities.

For a partition node with `C_parent=C_left+C_right`, the upper quota is redundant, exactly the inequality-side fact relevant to additive contraction. The additional *attainment* implication, forcing a parent-major row to attain every component boundary maximum, is a separate step in A05. Ordinary matroid independence does not encode the disjunction that one of two rows must attain each quota. This source is especially relevant to the proposed strengthening in Section 6, and prevents claiming that deleting redundant laminar capacities is itself new.

### S4. Closest Search Application: Zhang Et Al.

[Yong Zhang, Jiacheng Wu, Jin Wang, Chunxiao Xing, *A Transformation-based Framework for KNN Set Similarity Search*, author manuscript](https://www.jinwang18.net/files/tkde19-setknn.pdf). Sections 3-5 define token-group count vectors and query-node bounds. PDF/printed p. 5, Lemma 2 and equations (3)-(5), bound Jaccard distance using an R-tree node's coordinate intervals. Page 6, Theorem 2, establishes no false negatives for Algorithm 2; Appendix C is on p. 15.

This is direct precedent for grouping tokens and filtering batches from count-vector summaries. It is not a theorem that every best point in a coordinate box extends to two complete sets jointly realizing exact subtree maxima and an exact union. Its R-tree hierarchy groups **records**; A05's laminar hierarchy partitions **features**. Those are different trees. Giving its bound the orientation solver would construct a strengthened control, not show the added reduction was historically present. The inspected PDF identifies itself as a submitted manuscript, so page references here refer to that version, not an assumed final pagination.

### S5. Haplotype Reconstruction: He Et Al.

[Dan He, Arthur Choi, Knot Pipatsrisawat, Adnan Darwiche, Eleazar Eskin, *Optimal algorithms for haplotype assembly from whole-genome sequence data* (2010), institutional fulltext](https://escholarship.org/content/qt77t900hh/qt77t900hh.pdf). Sections 3.1-3.2, printed pp. i184-i185 / PDF pp. 3-4, specify ternary fragment observations and a complementary pair of binary haplotypes after removing homozygous sites. Equation (1) and its following discussion give exact minimum-error-correction DP, with a bound stated as `O(m*2^k*n)` in the fixed read-length description.

This is strong precedent for reconstructing two unlabeled rows with an exponential structural parameter. It does not match compressed laminar aggregate counts: reads reveal linked feature identities, its retained binary rows are complementary, and its objective repairs read disagreements. A05 allows intersection, observes only union/count information within leaves, and optimizes a query envelope. Neither the read-length parameter nor its sufficient DP state is identified with `r` by a demonstrated reduction.

### S6. Closest Measurement Equation: Kim, Bahmani, Lee

[Seonho Kim, Sohail Bahmani, Kiryung Lee, *Max-Linear Regression by Convex Programming*, arXiv:2103.07020v2 (2024)](https://arxiv.org/pdf/2103.07020). Page 1, equations (I.1)-(I.2), observes maxima of linear forms. Page 3, Theorem 1, assumes independent standard-Gaussian measurement vectors and an initial estimate satisfying (II.3); it provides a high-probability estimation guarantee.

The measurement equation genuinely matches one part of A05: let each measurement vector indicate a subtree's leaves and the two unknown vectors be row leaf counts. Then `C_v=max(w_v^T*x,w_v^T*y)`. But A05's measurement design is fixed and laminar, with integer counts, union-covering inequalities, prescribed row totals, and potentially many compatible solutions. The cited recovery theorem cannot be applied by calling those indicators Gaussian or by treating one recovered pair as an optimum over all pairs. This is a closer algebraic analogy than a generic haplotype reference, but not an exact algorithm for A05.

### S7. Alternative Numeric Schedule: Kumabe, Maehara, Sin'ya

[Soh Kumabe, Takanori Maehara, Ryoma Sin'ya, *Linear Pseudo-Polynomial Factor Algorithm for Automaton Constrained Tree Knapsack Problem*, arXiv:1807.04942v2 (2018)](https://arxiv.org/pdf/1807.04942v2). Page 3, Theorem 1, gives `O(n^(log2(1+delta(n)))*C)` time. Section 2.1, p. 4, explicitly assumes a constant number of automaton states. The PDF labels this **Theorem 1**, whereas the HTML rendering numbers it 1.1.

This is the strongest inspected alternative for improving a generic tree-knapsack convolution schedule. A05 does not automatically satisfy its encoding assumptions: node-specific attained integer maxima and two row cardinalities must be represented without count-sized automaton states or unary expansion. No such bounded-state reduction was found here. Thus it challenges claims about generic convolution novelty, and motivates investigating the Q-indexed solver, but supplies neither an unconditional replacement nor the count-independent `r` bound.

### S8. Budgeted Laminar DP: Doron-Arad, Kulik, Shachnai

[Ilan Doron-Arad, Ariel Kulik, Hadas Shachnai, *An FPTAS for Budgeted Laminar Matroid Independent Set* (2023)](https://arxiv.org/pdf/2304.13984). Page 1 defines an explicit ground set with item costs/profits and laminar upper quotas. Page 3, Theorem 1.3, gives an FPTAS; Section 3 gives the exact profit-indexed DP, and Section 4 rounds profits.

This supports the older overlap note's attribution of profit-indexed laminar DP. It is not the closest algorithm for the conditioned row problem, which has interchangeable count units and only two reward slopes. Expanding counts into individual items changes the input-size contract. Approximation also does not meet the exact attained-envelope output requirement. There is no fair general speed comparison between its arbitrary-cost budgeted problem and A05's specialized conditioned pass.

### S9. Better Exponential Algorithms: Nederlof And Wegrzycki

[Jesper Nederlof and Karol Wegrzycki, *Improving Schroeppel and Shamir's Algorithm for Subset Sum via Orthogonal Vectors*, arXiv:2010.08576v2 (2021)](https://arxiv.org/pdf/2010.08576). Printed p. 2 / PDF p. 3, Theorem 1.1, is Monte Carlo with constant success probability. Printed p. 4 / PDF p. 5, Corollary 1.4, extends the `O*(2^(n/2))` time and `O*(2^(0.249999*n))` space result to knapsack; the binary-integer-programming extension has an additional constraint-dependent factor.

This is relevant to the covering-gadget subfamily and to overclaims that subset-sum hardness requires enumerating all `2^r` assignments. It does not prove an `O*(2^(r/2))` algorithm for arbitrary A05 metadata. Counting only its Boolean orientations while ignoring residual integer allocations is not a valid substitution for its input dimension. Its randomized guarantee also must not silently replace deterministic exact-envelope certification.

## 4. What Actually Reduces To What

**Strongest applicable established machinery:** exhaustive disjunctive conditioning plus integral laminar resource allocation. **Strongest inspected published algorithm for the entire exact A05 model:** none identified. These statements are deliberately different.

For a fixed label at each node, replace `max(x_v,y_v)=C_v` by one equality and one upper bound. Each row has subtree flow `x_v=x_left+x_right`, lower/upper edge quotas, and its prescribed root flow. At a leaf use a reward-one arc of capacity `q_j` and reward-zero arc of capacity `H_j-q_j`. The leaf's common incoming quota bounds total occupancy. Integer lower-bounded flow realizes the conditional row problem with `O(G)` vertices/arcs, without expanding `H_j` units. The four-scalar pass is a specialization of this network, not a different general primitive.

Three model-specific facts are needed before that known machinery solves the desired problem:

1. **Choice elimination.** At an additive node, the parent-major row reaches both child maxima. At a strict node the children have opposite, untied majors. This supplies at most `r` freely branched bits after fixing the root-major name. Generic branching does not by itself identify this parameter.
2. **Cartesian count feasibility.** Once one leaf row equals `C_j`, the companion's lower bound `H_j-C_j` is enough. For any selected subset `X_j`, the companion can contain `U_j minus X_j` and `x_j+y_j-H_j` elements from `X_j`. Both independent row systems must be feasible. This is an explicit reduction, not a consequence of flow integrality alone.
3. **Marginal objective realization.** Query-first selection realizes `sum min(q_j,x_j)` for the scored row; the other row can complete the union but need not realize its own optimum in that same pair. Maximizing over either row legitimizes separate witnesses. Optimizing a sum/product of both overlaps would need a different proof.

These are the candidate's residual contribution boundary. A solver that imports them and reproduces the bound is an instantiation/control, not independent historical evidence. Conversely, establishing these short lemmas does not by itself establish a substantial or publishable advance; a reviewer can reasonably find the resulting application of known machinery modest.

## 5. Exact Counterexample To Immediate Flow/Convexification

This is a fresh small witness, not a claim that any cited paper makes the invalid reduction.

Take two leaves `L={a,b,c,d}` and `R={e,f,g,h}`. Let `H_L=H_R=4`, `C_L=C_R=3`, `C_root=ell=u=4`, and query `S={a,b,e,f}`. Thus `I=0` and `r=1`.

Because both rows have size four and union size eight, they are complements. Attaining each leaf maximum forces occupancies `(3,1)` or `(1,3)`. The root totals force opposite orientations across the leaves. Either row therefore meets the query in at most `min(2,3)+min(2,1)=3` elements, and this is attainable. The exact envelope is `3/(4+4-3)=3/5`.

Dropping attainment admits `T0=S`, `T1=U minus S`: both rows have leaf counts `(2,2)`, obey all upper quotas, have the right root sizes, and cover the exact union. This integral solution has score one but fails both leaf maxima. Consequently an integral upper-quota flow is still only a relaxation.

Moreover, the local count pair `(2,2)` is the midpoint of `(3,1)` and `(1,3)`. Even the **global count-only convex hull** contains the point obtained by averaging the two opposite complete count orientations. Optimizing the concave reward `sum min(q_j,x_j)` over that hull can improve the objective to four. Convexifying counts without retaining the branch-specific reward relation is therefore insufficient. This does not refute disjunctive convex-hull methods in the full lifted count/reward space, nor prove that every possible LP formulation must be large.

An independent in-memory enumeration checked the 70 four-element first rows on this eight-element universe, using the complement as the second row. Exactly **32 ordered pairs** satisfy the metadata; their maximum overlap is **3**, confirming **3/5**, against the relaxation's **1**. No candidate implementation or floating-point scoring was used.

The one-leaf example already in the review supplies another guard: with `H=3`, both row sizes two and query size two, the two separately optimal rows cannot both equal the query and still cover the union. It rules out simultaneous-witness inference, not the maximum-of-either-row envelope.

## 6. Structural Improvements Worth Pursuing

### 6.1 An Additive-Region Core

**New derivation in this assessment, not located published precedent and not part of the completed independent review.** The following contraction appears to yield `O(G+(r+1)*2^r)` arithmetic work. It leaves the exponential factor unchanged and does not promise a practical speedup.

Delete the strict internal vertices conceptually. The remaining connected pieces consist of additive vertices and ordinary leaves; singleton leaves count as pieces. There are at most `2r+1` nonempty pieces. Keep the `r` strict vertices and replace each remaining piece by a component node. Its downward boundary consists of strict subtrees and ordinary leaves belonging to that piece.

All labels inside one piece are equal. Repeated additivity gives `C_top=sum C_boundary`. For the major row, every boundary child is fixed at its maximum, so all deleted interior equalities follow. For the minor row, every boundary count is at most its maximum, so all deleted interior upper quotas follow. This is why deleting an additive vertex is safe **with label propagation and boundary constraints retained**, not merely because its upper quota is redundant.

Bundle all ordinary leaves `J` in the same piece into one virtual leaf. Define

```text
L = sum_j (H_j-C_j)       U = sum_j C_j
b = sum_j min(q_j,H_j-C_j)
B = sum_j min(q_j,C_j)    A = L-b
F_minor(x) = min(x-A,B),   L <= x <= U
F_major(U) = B.
```

The minor formula follows by taking reward-one increments before reward-zero increments across those leaves. For the major row all leaf sizes are fixed to `C_j`. Equivalently, the same capped-linear record is restricted to `[U,U]`; its value there is `B`. A piece with no ordinary leaves uses the zero record. **Do not replace this bundle by only `sum H`, `sum C`, and `sum q`:** forced nonquery occupancy and leaf-specific query caps are preserved by `A,B`.

For example, leaf triples `(H,C,q)=(3,2,0),(3,2,3)` give `L=2,U=4,A=1,B=2`, hence minor values `1,2,2` at sizes `2,3,4`. A fictitious single leaf with aggregate query count three would incorrectly give `2,3,3`.

The retained strict vertices, component nodes, virtual leaves, and their total edges number `O(r+1)`, even if a component has high degree. Preprocessing visits each original node once. Each orientation then propagates labels and runs both row passes on this small structure, preserving the original root totals and both-row feasibility check. This gives the proposed bound

```text
preprocessing + enumeration: O(G + (r+1)*2^r) integer operations
retained input/work allowance: O(G) words suffices
post-preprocessing core: O(r+1) words, excluding input and witness expansion.
```

Feature-level witness reconstruction may revisit the original leaves and is not an `O(r)` output claim. Integer bit lengths remain relevant; this is not a polynomial kernel in encoded bits merely because the core has few vertices. The transformation retains the original orientation reduction, so it is not a historically independent comparator.

**Limited check performed:** enumerated all 17 valid leaf triples with `0<=H<=3`, `ceil(H/2)<=C<=H`, and `0<=q<=H`; checked every integer occupancy for every one- or two-leaf bundle. All **306 bundles** matched the displayed formula, including its major endpoint. This checks only the local bundle summary, not the entire contracted-tree theorem or an implementation of it. The structural proof above still needs an independent challenge on nested strict nodes, ties, root constraints, and invalid metadata.

### 6.2 A Useful Parameter Inequality

Another direct consequence of the metadata, derived here, is

```text
Delta = sum_internal D_v = sum_leaf C_j - u
r <= Delta <= H_root-u = ell-I.
```

The equality telescopes; strict integer deficits are at least one; and `C_j<=H_j`. For any realizing pair with leaf count difference `d_j=x_j-y_j`, also

```text
Delta = (sum_j |d_j| - (u-ell))/2.
```

Thus strict-split count is bounded by the smaller row's exclusive population and measures some cancellation of leaf-major differences, not intersection size. This helps explain a near-containment regime with large shared counts. It is a modest corollary, not a new general parameterized-complexity technique or an optimality lower bound. The converse "r=0 implies actual set containment" is false: one leaf with two distinct size-two subsets of a three-element union already has `r=0`.

### 6.3 Meet-In-The-Middle On The Covering Gadgets

The overlap note's special construction gives

```text
loss(t) = min { sum_{i in J} d_i : sum_{i in J} w_i >= t }.
```

For those independent item gadgets, complementing `J` makes this ordinary 0/1 knapsack: maximize the omitted `d_i` subject to omitted weight at most `sum w_i-t`. A deterministic two-list algorithm enumerates half-subsets, sorts one half by weight, stores prefix-best profits, and queries the best compatible completion for each other-half subset. It uses `O*(2^(r/2))` time and exponential space on this subfamily. S9 provides a stronger randomized space result for the associated knapsack problem, with its stated error qualification.

This means the hardness gadget is **not** evidence that base-two full enumeration is optimal. But it is a reduction *from this special construction to knapsack*, not from arbitrary A05 metadata. For nested strict nodes, a half-assignment can expose several boundary count functions, not one weight/profit pair. A general meet-in-the-middle claim needs a bounded-dimensional compatibility summary and a faster join than the Cartesian product. It also must account for losing the candidate's linear retained-state guarantee.

## 7. Final Synthesis And Claim Recommendation

| Claim | Evidence-based disposition |
|---|---|
| New Boolean branching, laminar flow, concavity, or convolution primitive | Reject that framing; S1-S3 directly cover the foundations. |
| Exact two-row/full-union reduction and the strict-split parameter theorem | Preserve as a candidate problem-specific result; inspected sources do not establish its historical identity. |
| Ordinary flow or count-hull convexification immediately eliminates orientations | False for the stated relaxations; Section 5 gives an integral, realizable-union counterexample. |
| Haplotype or max-linear recovery already solves this precise envelope | Not supported: observation model, assumptions, output, and parameter differ. |
| `2^r` is optimal, or the covering hardness proof forces that base | Unsupported; the independent-gadget subfamily admits meet-in-the-middle. |
| Paper readiness or improved public query performance | Not established. The supplied notes report negative complete-query results, which this literature exercise does not alter. |

Recommended paper statement, subject to the existing theorem's precise qualifications:

> For complete two-set summaries with exact union and attained laminar maxima, we identify the strict splits as a sufficient orientation parameter. Conditioning these choices separates feasible row-count systems and yields an exact maximum-of-either-row Jaccard envelope using capped-linear laminar allocation. The resulting evaluator uses O(G*2^r) arithmetic work and O(G) integer words after count preparation.

Present this as a **specialized reconstruction/envelope reduction using established optimization machinery**. Attribute the ingredients explicitly. Do not say "first", "novel FPT paradigm", "optimal", "constant memory", or "faster similarity search" on this evidence. If the component-core strengthening survives independent verification, replace the weaker work bound and include its preservation theorem; do not describe contraction itself as an invented generic operation.

**Publication priority remains open.** The strongest residual historical question is whether deterministic two-vector max-linear reconstruction with laminar measurements and union-covering constraints already has this attainable-envelope/backdoor characterization. The failed database-abstraction fulltext retrieval is a specific remaining gap. Neither a missed search result nor a newly constructed equivalent control resolves priority.

## 8. Next Decisive Experiment

Prioritize the additive-region core before another public timing run. Have an independent checker formalize the boundary/virtual-leaf preservation theorem, then compare **every feasible count domain and optimal-overlap frontier**, for both row roles and every orientation, between original and contracted summaries on a small exact universe. Check witnesses for frontier optima, not just one final score; the four scalars do not purport to retain every suboptimal overlap value. Include skewed and balanced trees, adjacent/nested strict nodes, root ties, empty leaves, an infeasible companion, outside-union query elements, and the `A>0` bundle example above. Return the smallest counterexample if any; otherwise establish the lifting proof and the `O(r+1)` core-size bound explicitly.

The decisive outcome is either a counterexample invalidating the contraction, or a stronger exact structural theorem with a matching arithmetic schedule. A count-only family with fixed `r` and arbitrarily many additive leaves can then certify removal of the repeated `G` factor without public timing. This would strengthen the technical story while still leaving publication priority and practical usefulness as separate questions.
