# Independent Skeptical Review: Factor-Defect PageRank

Date: 2026-09-20. Scope: research correctness, nearest primary work, numerical certification, and resource claims. This is not an endorsement or a production code review. Only this review file was written; computations were ephemeral.

Reviewed [Factor-Defect-PageRank.md](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Factor-Defect-PageRank.md), including numerical admission and optional Jacobi, and [PageRank-Probe-Evidence.md](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/PageRank-Probe-Evidence.md), including its source and Jacobi comparison. Section names are authoritative if concurrent edits move the line numbers below. Review snapshots had SHA-256 values `de7040996796466eafca96f687b521fbe6be5cd9431c826bd65263a7c406545e` and `4433fe2ff4c831895def5b792c5b6ceeb1efce8c15d8baab9ef4e53da19383d4`, respectively.

## Findings First

**Verdict: not paper-ready as a new PageRank algorithm.** The exact-arithmetic mechanism survives the challenges below. That is different from establishing a novel contribution, a floating-point certificate, or a useful end-to-end bounded-RAM solver. The current caveats correctly acknowledge several of these limits; acknowledging them does not resolve them.

Severity: P1 blocks the corresponding publication/implementation claim; P2 is a material scope or experiment gap. No P0 mathematical contradiction was found in the stated fixed-point theorem.

| ID | Severity | Finding | Disposition |
| --- | --- | --- | --- |
| IR-01 | P1 | Low-rank elimination, factor-count awareness, implicit application, and compressed stationary PageRank are already occupied territory. | Narrow the claim to a specific compiler/execution/certification contract and compare directly with the closest algorithms. |
| IR-02 | P1 | Minimum factor count is not minimum solve cost. Non-singleton diagonal cancellation can make the base iteration arbitrarily slow on an unchanged two-node graph. | Price representation-dependent convergence; test Jacobi and refactorization controls, not only singleton pruning. |
| IR-03 | P1 | The two-array row schedule is plausible, but bounded end-to-end memory is unimplemented and unmeasured. | Demonstrate the builder, oversized rows, validation, certificate arithmetic, and output under actual limits. |
| IR-04 | P1 | The residual theorem does not yet certify the intended source operator in floating point. | Numerical admission must cover degree classification, factor scaling, accumulation, reconstruction, and operator error. |
| IR-05 | P2 | General diagonal pruning is not tested by the supplied compiler, which always cancels the entire base diagonal. | Add explicit arbitrary-Q tests and a nonnegative cancellation-budget invariant. |
| IR-06 | P2 | Star-cover optimality is correct only for a fixed residual and a restricted factor class; it is not state optimality for PageRank generally. | Preserve that restriction and benchmark general low-rank and specialized star solvers. |
| IR-07 | P2 | Equal-size covers can have identical original-space iterates but radically different factor stopping bounds. | Measure certificate conservatism separately from solver convergence. |
| IR-08 | P2 | The 5,376-case evidence reproduces, but covers a much narrower numerical and compilation domain than the theorem. | Expand the adversarial matrix suite and strengthen oracle/certificate checks. |

## IR-01: Novelty Threats

### Primary Evidence And Access

The following are primary papers, not survey descriptions. Claims of absence are deliberately limited to the inspected material.

| Work | Inspected evidence | Collision and remaining question |
| --- | --- | --- |
| [Karande, Chellapilla, Andersen, 2009, journal version](https://www.internetmathematicsjournal.com/article/1489-speeding-up-algorithms-on-compressed-web-graphs/attachment/4433.pdf) | Full 26-page PDF obtained in memory despite browser-fetch failures. Reviewed printed pp. 379-389, especially Proposition 3.1 and Section 4.1/Algorithm 3/Theorem 4.1. | Sequential compressed multiplication is explicit. A separate algorithm computes a stationary distribution on real plus virtual nodes, projects onto real nodes, and renormalizes. This is more than an edge-compression baseline. Its displayed state includes real nodes; the inspected algorithm does not establish an r-only entity-free iterate. A fair control must include both of its approaches. Its alpha denotes jump probability, unlike this draft's link-following a. |
| [Shen and Carpentieri, 2021](https://journals.sagepub.com/doi/pdf/10.3233/FAIA210212) | Full eight-page paper obtained; Section 2, Algorithm 1, and conclusion inspected. | Printed p. 400 optimizes stored nonzeros; p. 401 explicitly limits factor count because it determines dense capacitance dimension; p. 404 gives whole-matrix `A=D+FH`. Thus even factor-count awareness is prior art. Its displayed design does not establish this draft's exact mixed-defect-cover, entity-free row schedule and original-space stopping contract. That distinction needs an algorithm comparison, not a claim that earlier work ignored solver state. |
| [Shen et al., 2019, ODLR](https://pure.rug.nl/ws/portalfiles/portal/103307690/1_s2.0_S0377042718304357_main.pdf) | Full paper accessible; printed pp. 461-462, Theorem 5.1 and Algorithm 2 inspected. | Uses the capacitance matrix `I+HD^-1 F`, proves invertibility, and explicitly applies the preconditioner implicitly. Consequently, "matrix-free" cannot mean merely not forming a full inverse. Specify which matrices, vectors, intermediate solves, and original-node state the new schedule avoids. |
| [Shen et al., WDBMC, 2025](https://www.sciencedirect.com/science/article/pii/S0377042724005806) | Publisher-indexed abstract, highlights and introduction; DOI `10.1016/j.cam.2024.116332`, JCAM 458, April 2025, 116332. Full publisher fetch unavailable. The [author institution record](https://bia.unibz.it/esploro/outputs/journalArticle/Weak-dangling-block-reordering-and-multi-step/991006942025301241) exposes only a DOI link, not a manuscript. | Reordering, Schur-complement properties, multistep compression, low-memory preconditioning and updates directly threaten broad claims. Section 3 full-text exclusion is still outstanding. The 2024 DOI/online history and 2025 issue are not two different papers. No assertion here that WDBMC either contains or lacks the specific proposed cover-and-row mechanism. |
| [Francisco et al., 2022](https://koeppl.github.io/bin/paper/sncs22graph.pdf) | Full PDF; printed p. 6, "Computation with Bicliques". | Explicit biclique-plus-residual multiplication gathers biclique values, allocates an original-node result vector, and adds residual contributions. This is a particularly clean control for measuring entity-state elimination. Its preprocessing and memory-access results also warn against predicting wall time from nonzero counts alone. |
| [Gleich, PageRank Beyond the Web](https://arxiv.org/pdf/1407.5107) | Section 2, Aside 2.3 and Theorem 2.5. | The original-space residual-to-error bound and normalized pseudo-PageRank equivalence are established. They are foundations, not independent novelty components. |

### Direct Algebraic Collision

An independent mapping of the draft into the standard capacitance construction is:

```text
D_lit = I + a Q Dinv
F_lit = -a Ubar
H_lit = Vbar^T

I - a P = D_lit + F_lit H_lit
I + H_lit D_lit^-1 F_lit = I - a Vbar^T H Ubar.
```

Here the draft's H is the inverse of `D_lit`, not the literature's right factor. The reduced system is exactly the corresponding Schur/capacitance system. This is a specialization, not a new elimination identity. The meaningful engineering restriction is that eliminating all positive exceptional edges into factors leaves a diagonal core whose inverse is row-local.

The surviving **hypothesis**, not an established innovation, is therefore: a compiler deliberately completes a fixed nonnegative factorization with a small mixed star cover, even at negative edge-compression gain, to obtain a row-local diagonal core; it executes and validates an original-space answer without resident entity iterates or a dense reduced operator. Each ingredient is simple enough that the burden is to show a non-obvious useful combination, not merely attach a new name.

Three candidate positions remain: a new algebraic PageRank method, a restricted parameterized compiler theorem, or a bounded-memory systems result. The first is not supported. The latter two require a precise cost model and comparisons that can falsify the benefit. An exact cover theorem for a fixed E does not by itself provide a novel global factor compiler.

## IR-02: A Smaller Factorization Can Be Arbitrarily Worse

References: lead "Remove Diagonal-Only Work", "Convergence Claim", and "Optional Ordinary Jacobi Variant".

Use the same graph for every K>0:

```text
A = [[0,1], [1,0]],  d = [1,1],  p = [1,0],  a = 0.85.

u = [K,1]^T,  v = [1,1/K]^T,
Q = diag(K,1/K),  A = u v^T - Q.
```

Both sides of this single factor have support two. It is not a singleton that the proposed pruning removes. The stationary answer is always `[1/(1+a), a/(1+a)]`, independent of K. There are no dangling vertices; the generic dangling coordinate is identically zero.

The nonzero scalar iteration coefficient is

```text
lambda(K) = a * (K/(1+a*K) + (1/K)/(1+a/K))
          = 1 - (1/a-a)/K + O(1/K^2).
```

Thus the iteration approaches non-contraction as K grows, although graph size, graph weights, a, and factor count are unchanged. This is not just a loose beta bound. With the stated warm start, the scalar error actually follows lambda. The initial residual certificate tends to 0.85 rather than vanishing.

| Representation | Factors, excluding dangling | Executed updates to factor bound <= 1e-10 | Interpretation |
| --- | --- | --- | --- |
| Direct two column stars, Q=0 | 2 | 156 | Same graph and p; base schedule. |
| Above construction, K=100 | 1 | 7,129 | lambda = 0.9968004519721898. |
| Above construction, K=1,000 | 1 | 70,147 in f64 | lambda = 0.9996741899819935; ideal scalar recurrence predicts about 70,157. Last-digit residual subtraction affects termination. |
| Above construction, K=10,000 | 1 | Not iterated to completion | Scalar formula predicts about 700,443 updates; this is analytical extrapolation, not a measured run. |

For K=100 the final reconstructed vector was approximately `[0.5405405405943369, 0.4594594595053344]`; the measured factor bound was `9.9697e-11`. No timing claim follows from these small calculations.

**Jacobi qualification:** the optional factor-Jacobi method solves this one-dimensional active system in one exact update. It is an essential control and means this example does not refute the whole proposed method family. However, split the same factor into two identical half-contributions. The nonzero factor-Jacobi eigenvalue becomes `lambda/(2-lambda)`, still tending to one. At K=100 it is `0.9936213128602377`. Duplicate merging repairs that particular construction, but is not the stated singleton pruning rule. The lesson is representation admission and solver selection, not "Jacobi is wrong".

Required experiment: vary representation and Q while holding A fixed; compare ordinary iteration, factor Jacobi, duplicate/proportional-factor merging, and a Q-free refactorization. Factor count alone cannot select the best plan. Report invalid or over-budget plans explicitly.

## IR-03: Memory Schedule Is Conditional, Not Demonstrated

References: lead "Fuse Reconstruction And Gathering" and "Resource Model".

The core loop can genuinely avoid an N-entry x array. A sufficient concrete layout is `row header, all U entries, all V entries`, with p, d, q and output ID in the header or synchronized streams. Consume arbitrarily many bounded U chunks into one scalar, then consume V chunks. h is read-only throughout the pass, and all gather writes target next. This also avoids explicitly forming the dense reduced matrix. No algebraic flaw was found in this schedule.

The resource theorem still needs these conditions made executable:

- Both `h` and `next` fit in RAM: `16*(r+1)` bytes for f64, not merely `r+1` scalar words. Stream certificate weights or charge their third array. Chunking rows does not bound random factor-vector accesses if r itself exceeds the admitted RAM.
- An interleaved U/V record cannot generally gather V before xi is known. Row-local rereading requires a seekable/framed format; network streams or compressed blocks spanning many rows do not automatically support it at the stated L bytes.
- Factor column sums for degrees and c, matching/cover construction, factor discovery, sparse row assembly, duplicate aggregation, joins, ID mapping and validation are separate operations. A later bounded scan does not imply a bounded compiler.
- Compensated scatter summation can require per-factor compensation state. Arbitrary-precision/interval accumulators may need more than one f64 per entry. Hiding these in a constant C invalidates an exact memory comparison when they scale with r.
- Arbitrary p and original IDs can be streamed, but require explicit ordering and charged joins. Output sorting, backpressure, restart receipts, and simultaneous old/new artifacts need real limits, not assumed free storage.
- L alone does not establish bandwidth: CPU decoding, unlocalized factor reads/writes and certificate scans may dominate. Parallel next arrays multiply state unless ownership changes the schedule.

The weighted star gives a concrete traffic warning. When r=2 but p has N arbitrary values, the supplied row loop still scans N entities on each iteration. A dense 2-by-2 reduced solve, or the closed form below, uses constant mutable state too and avoids iterative full-row scans. A blanket ban on dense reduced matrices is not a competitive baseline when r is tiny. An adaptive small-core solver must be priced and compared.

An out-of-core ordinary solver is another necessary memory baseline: it may retain N-sized logical iterates on disk while using bounded resident buffers. The proposed advantage would then be reduced changing-state storage and I/O, not a general claim that every incumbent needs N resident values.

## IR-04: Certification Of Which Operator?

The newly added numerical-admission caveat is correct and necessary. I independently reproduced its failure mechanism with the non-singleton construction above at `K=2^54`:

```text
column sum of UV^T at source 0 = K+1
q_0 = K
exact d_0 = 1
f64 evaluation (K+1)-K = 0.
```

The factors themselves have representable positive entries. Expanded off-diagonal A contains a unit edge; forming the degree through a large column sum loses it. Tests that expand A first, as the supplied probe does, bypass this particular production-builder hazard. A checksum or residual against the rounded wrong operator will not repair it.

Even with exact degree classification, current f64 certification remains unimplemented. Empty/zero factors, overflow, underflow, near-unit a, accumulation order, rounded p normalization, factor scaling and the final reconstruction must be covered. Rescaling corresponding U and V columns reciprocally preserves A and the exact weighted certificate, but can change intermediate floating-point ranges catastrophically.

A useful acceptance target is an explicit bound of the following form, not just an unspecified arithmetic allowance:

```text
||x - x_source*||_1 <=
  (R_computed + eta_residual + eta_b
   + a * eta_P * ||x||_1) / (1-a),

||P_computed - P_source||_1 <= eta_P,
||b_computed - b_source||_1 <= eta_b.
```

Here `R_computed` bounds the residual for the actually returned x, and `eta_residual` includes arithmetic/reconstruction evaluation error. This follows by residual perturbation and the stochastic resolvent bound. Establishing these quantities without N resident error accumulators is part of the engineering problem. The formula is a review requirement, not a completed numerical algorithm.

Likewise, normalizing an approximately stationary reconstructed vector is not free of error consequences. For a nonnegative x with positive sum and original error at most eps, the simple triangle bound gives normalized error at most `2*eps`; a tighter claim needs its own argument. The current decision to certify and return X(h), not silently normalize it, is appropriate.

## IR-05: Diagonal Pruning And General Dangling Input

### Pruning Is Valid With A Budget Invariant

Removing `m e_i e_i^T` from `UV^T` and replacing `q_i` with `q_i-m` preserves A exactly, provided the remaining q is nonnegative. Multiple removals at the same vertex must collectively satisfy `sum m <= q_i`; each factor cannot independently consume the original budget.

Example: one vertex, two singleton contributions 2 and 3, q=3. Original A=2. Removing the mass-2 factor leaves contribution 3 and q=1, hence A=2. Removing both against the original q produces q=-2, outside the theorem. Resetting q to the diagonal of the remaining factors instead would incorrectly delete the real loop. At q=0 no positive singleton contribution is canceled at all.

The prose restricts removal to wholly canceled mass, so this is **not a counterexample to the stated identity**. It is a missing general compiler contract. In the supplied [probe compiler](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/PageRank-Probe-Evidence.md:133), `originalQ=diag(CB^T)` and the replacement q is always recomputed as the remaining base diagonal. It has no arbitrary-Q input and cannot test partial cancellation or uncanceled base self-loops. Self-loops introduced through E do not cover those cases.

### The General Dangling Formula Survives

For `0 -> 1`, a=17/20 and p=(1/2,1/2), an exact rational check gives `(20/57,37/57)`. Vertex 1 has positive incoming mass despite zero outgoing degree. The explicit z channel handles it correctly.

A sharper admitted representation is `U=(0,1)^T`, `V=(1,2)^T`, `Q=diag(0,2)`, yielding the same graph. It contains positive canceled diagonal mass at a dangling vertex but is not a same-singleton-support factor. Here `Dinv_11=0`, `H_11=1` and `t_1=0` are essential; q/d must not be evaluated there. Setting a dangling vertex's reconstructed value to `(1-a)p_i`, or deleting it because its outgoing column is zero, is wrong.

The extra dangling coordinate is a correct convenient representation, not a universal minimality theorem. It is removable when z=0. With the same p for teleportation and dangling redistribution, normalized pseudo-PageRank supplies another established approach that can avoid an explicit dangling unknown, at the cost of normalization and a different stopping/output analysis. This is another baseline, not a defect in the present formulation.

## IR-06: Cover Identity Is Correct; Its Optimality Is Narrow

References: lead "Compile Defects Into Solver Factors" and the weighted-star example.

For every nonzero E[i,j], destination-first assignment either places it in the selected destination's row factor, or, if that destination is unselected, in a selected source's column factor. These alternatives are exhaustive and disjoint. Exact weights are preserved. If one included the complete selected row and complete selected column without excluding their overlap, a one-edge graph with both endpoints selected would already double its weight.

Every nonnegative row/column-star factor has a center incident to all nonzeros it produces. Hence the centers of any exact such factorization cover the support of E. A minimum bipartite vertex cover is therefore a lower bound on this restricted factor count, and the construction attains it. Empty-factor removal is safe. No disagreement with that theorem was found.

I independently enumerated all 512 three-by-three supports, assigned positive integer weight `3*i+j+1` to each present edge, and tested every valid one of the 64 possible covers. All **13,009 cover identities** held exactly. Brute-force minimum cover, maximum matching size and minimum nonempty constructed factor count agreed on all 512 supports.

Restrictions that must not disappear in a headline:

- E and the base decomposition are fixed. Refactoring the base and residual jointly can yield fewer factors.
- Centers are restricted to original source/destination copies. For an all-ones k-by-k E, minimum star cover is k, while one general outer product suffices. That is an arbitrarily large gap, not a small implementation detail.
- Equal cost per factor is the optimized objective. Metadata, nonzeros, validation, precision requirements, certificate tightness and builder cost are not minimized by the cover theorem.
- The number of factors is not a lower bound on the amount of information required for every PageRank solver; dependencies, personalization and closed forms can reduce the unknowns further.

### The Weighted Star Does Not Establish Algorithmic Novelty

Let the center be 0, let `b_i>0` be its edge weight to leaf i, and let every leaf have one positive-weight edge back to 0. Write `w_i=b_i/sum b_i`. Every leaf's normalized transition back to 0 is one, regardless of its return-edge weight. For arbitrary normalized p:

```text
x_0 = [a + (1-a)*p_0] / (1+a)
x_i = (1-a)*p_i + a*w_i*x_0,  i != 0.
```

This follows by substituting `sum_i!=0 x_i=1-x_0` into the center equation. The weights do not make this toy a difficult low-rank problem. A streamed normalization of the center's outgoing weights followed by output reconstruction needs O(1) mutable scalars and O(N) work, with no iterative solver or minimum-cover computation. Input validation still has to be paid for.

At a=.85, p=(.1,.2,.3,.4), center-to-leaf weights (2,3,5), and return weights (7,11,13), the formula gives:

```text
[0.46756756756756757,
 0.10948648648648648,
 0.16422972972972974,
 0.2587162162162162]
```

An independent dense linear solve differed by `1.2490e-16` in L1. The mixed cover of size two is indeed minimum: there are two disjoint source-copy/destination-copy edges in a matching. Its factor nonzeros total `2(N-1)+2`, versus `2(N-1)` edge weights. That demonstrates state reduction without edge compression, but does not show a new ability to solve the graph economically. Include this closed form, a tiny dense reduced solver, and a generic low-rank solver as controls.

## IR-07: Contraction And Residual Audit

### Exact Fixed Points And Residual: No Missing H

Under the stated finite nonnegative data, nonnegative diagonal Q, exact degrees, normalized p and `0<=a<1`, the following independently rederived identities hold:

```text
Ubar Vbar^T = P + QDinv
H^-1 X(h) = b + a Ubar h
r(X(h)) = b + a P X(h) - X(h)
        = a Ubar [Vbar^T X(h) - h].
```

The residual is `a Ubar delta`, not `a H Ubar delta`; inserting H would understate the original residual. Conversely h=Vbar^T x* reconstructs x*, and any reduced fixed point gives the unique PageRank solution. Column-stochastic P has a convergent nonnegative resolvent with L1 norm `1/(1-a)`. The stated error bound follows. It also holds for mixed-sign h, although that is not the usual initialized trajectory.

### Weighted Contraction: Correct, Not Unweighted Contraction

After removing zero-mass Ubar columns, every c_j is strictly positive. With `M=a Vbar^T H Ubar` and t=0 on dangling sources:

```text
c^T M = a*(1+t)^T H Ubar <= beta*c^T
beta = max_i a*(1+t_i)/(1+a*t_i) < 1.
```

For nonnegative M, the induced weighted L1 norm is exactly `max_j (c^T M)_j/c_j`, proving the assertion. No assumption of strictly positive personalization on every vertex is needed; sum p=1 makes its appended column nonzero. The empty graph and a=0 still need the stated separate handling.

Concrete check: graph `0 -> 1`, U=(0,1000), V=(.001,0), p=(.5,.5), a=.85 gives

```text
M = [[0, 0.000425], [850, 0.425]], c=(1000,1).
||M||_1 = 850, but ||M||_c = .85.
```

Thus an unweighted-norm replacement would be false even on two vertices. beta is a valid upper bound, not an assertion of faster convergence than ordinary PageRank.

### Cover Choice Does Not Change These Original Iterates

For the base schedule with `h_0=Vbar^T p`, write `x_k=X(h_k)`. Direct substitution gives

```text
x_0     = H[b+a(P+QDinv)p]
x_(k+1) = H[b+a(P+QDinv)x_k].
```

Consequently all exact factorizations with the same A and Q give **identical reconstructed iterates**, regardless of mixed/source/destination cover. Their nonzero reduced spectra likewise equal those of `a H(P+QDinv)` by the AB/BA eigenvalue relation. This applies to the base schedule, not generally to factor Jacobi or unrelated factor-space warm starts.

The factor triangle certificate can nevertheless differ. For `A=ones(2,2)`, Q=0, p=(1,0), a=.85, compare two minimum covers:

| Cover factorization | X(h_0) | True residual | Factor error bound |
| --- | --- | --- | --- |
| Row stars U=I, V^T=A | (.575,.425) | 0 | 0 |
| Column stars U=A, V^T=I | (.575,.425) | 0 | 4.816666666666666 |

For the second cover, delta=(-.2125,.2125,0) and c=(2,2,1). The cancellation in Ubar delta is exact, but the absolute factor sum loses it. This example incurs only one extra gather iteration, so it is not evidence of an arbitrarily large observed pass penalty. It does prove that minimum factor count alone does not optimize stopping tightness. For arbitrary h the ratio of this bound to the actual residual can be unbounded.

An additional streamed pass evaluating `sum_i |a*(Ubar delta)_i|` obtains the original residual norm without an N-array. It requires a delta representation, extra traffic and numerical analysis. Compare that option near convergence; do not silently call the cheap triangle bound the actual full residual norm.

### Optional Jacobi: Proof Holds, Performance Is Not Uniform

Let m=diag(M), D=diag(1-m), and c'=c*(1-m). Since `m_j<=beta<1`, D and c' are positive. For `J=D^-1(M-diag(m))`,

```text
c'^T J = c^T(M-diag(m)) <= c*(beta-m)
max_j (c'^T J)_j/c'_j <= max_j (beta-m_j)/(1-m_j) <= beta.
```

This proves the stated contraction. `T(h)-h` remains the right residual input. Substituting the Jacobi update difference without the D factor would be a different certificate. Close-to-zero `1-m_j` and cancellation in `T(h)_j-m_j*h_j` need validation; computing a positive exact denominator does not mean its floating-point approximation is reliable.

I reproduced the reported Jacobi comparison: maximum updates 1,208, but 666,596 residual and bound checks, versus 2,899 and 310,992 for the base schedule. These are workload-dependent iteration counts, not timings or a universal improvement. The draft's balanced interpretation is warranted.

## IR-08: What The Probes Actually Establish

### Reproduced Evidence

The saved JavaScript source was run directly in Node with `text` mapped to JSON output and `store` discarded. It exactly reproduced 5,376 cases, 310,992 residual checks and 310,992 bound checks, maximum identity discrepancy `1.1568555765032948e-15`, maximum final reference error `9.994219341002974e-11`, maximum 2,899 updates, and final seed 1609677230. The Jacobi changes described in the file likewise reproduced its reported output. Neither rerun is an independent implementation.

The probe has valuable checks: the original matrix uses unpruned C/B, and mixed directed/dangling/looped graphs do occur. However:

1. Of the cases, 4,608 use no base factors and hence Q=0. The remaining 768 are 192 small weighted shapes at four damping values, always with the entire base diagonal canceled. They are not general arbitrary-Q coverage.
2. Pruned and empty base columns remain in the probe's factor arrays. Their c weights can be zero. This does not invalidate its residual identity, but it does not test the strict weighted-norm admission or realized state-size claim.
3. The oracle computes expanded A before summing degrees. It does not exercise the proposed factor-column-sum-minus-q builder, degree enclosures, physical factor row layout, or streamed c reduction.
4. The reference uses power iteration with a successive-difference threshold of 1e-14. At a=.97 that threshold alone is not a 1e-14 solution-error guarantee. The loop also lacks an explicit failure flag if its 20,000-step cap is exhausted. No such exhaustion was observed in the rerun.
5. `error <= bound + 1e-10` uses an allowance equal to the requested final tolerance. It can conceal a bad or zero bound when the true error is already below that allowance. Earlier large-error checks are still meaningful; it would be incorrect to call the whole test vacuous. Near-tolerance certification needs higher precision or an enclosure oracle.
6. Residual checks occur on solver trajectories, not arbitrary h. Many successive checks are correlated. The 310,992 figure is not the number of independent graphs.
7. The reported worst-case `max_q_over_d` uses the original C/B diagonal, even after pruning. It is not generally the ratio for the executed pruned operator. Record both if using it to explain convergence.
8. Fixed iteration caps, discrete small weights, a<=.97 and tiny N do not test high dynamic range, near-unit damping, memory behavior, builder feasibility, or real-data wins. Numerical proofs do not become rigorous by increasing the number of f64 checks.

### Additional Independent Computation

A separate NumPy implementation used dense linear solves for both the original and reduced systems, not the supplied power oracle. Seed 20260920; 384 factor shapes with n=1..6, initial r=0..5, integer U/V entries 0..4 independently sparsified; q selected per vertex from `{0,1/4,1/2,1}*diag(UV^T)`; normalized nonnegative p including zeros; a in `.01,.85,.97,.9999`; zero-sided factors removed. Each case additionally tested a randomly signed h and both weighted contraction inequalities.

| Check | Observed result |
| --- | --- |
| Graph/parameter cases | 1,536 |
| Cases with some partial positive diagonal cancellation | 580 |
| Cases containing a dangling vertex with incoming edges | 600 |
| Random signed-h residual/error checks | 1,536 |
| Largest original/reduced direct-solve L1 discrepancy | `2.4270585541330547e-12` |
| Largest residual identity discrepancy divided by max(1, the two residual norms) | `1.891715950552708e-15` |
| Largest computed weighted contraction excess above beta | `4.440892098500626e-16` |
| Largest computed Jacobi contraction excess above beta | `2.220446049250313e-16` |

The tiny positive contraction excesses are floating-point discrepancies, not refutations of the exact inequality. These tests also are not interval certificates. Exact integer cover enumeration, the rational sink check, and the analytical adversarial families provide different evidence rather than simply more copies of the lead probe.

## Required Gates

Before a paper-worthy innovation claim, require all of the following:

1. **Nearest-art gate:** obtain WDBMC Section 3 and document a line-by-line algorithm comparison. Include the 2009 stationary approach, ODLR, multistep low-rank preprocessing, generic matrix-free Schur application, and ordinary PageRank. Do not use unavailability of a paper as evidence of novelty.
2. **Compiler gate:** specify inputs, admissible base factors, arbitrary diagonal cancellation, exact E identity, cover output/certificate, fallback, and priced resource limits. Show where the algorithm chooses a better representation rather than merely consuming a favorable supplied one.
3. **Numerical gate:** certify or explicitly reject the degree-cancellation example; validate reconstruction and stopping against an independent high-precision source operator. Add failure reporting for nonfinite values, overflow, insufficient precision and unresolved degree sign.
4. **Cost gate:** vary factor count, Q, cover orientation, solver and certificate strategy independently. Include the fixed-A family, dense bicliques with bad star cover, wide rows, bad locality, all-dangling graphs, and graphs with no reduced-state advantage.
5. **Baseline gate:** weighted-star closed form and tiny dense core are mandatory. Compare at equal original-space error and include all preparation, initialization, output and amortized refresh costs. Match access to the same source factors when testing solver-only effects.
6. **Systems gate:** actually execute within a RAM and live-disk budget. Record resident/peak memory, changing-state bytes, prepared bytes, scans, bytes reread, build scratch, output, failure recovery and wall time. No O(1)-state toy substitutes for this.
7. **Benefit gate:** use nontrivial real or realistically generated hub-plus-regular-plus-defect graphs where specialized star solving is inadequate. Demonstrate a completed job that incumbents cannot complete under the same limits, or a defensible Pareto improvement. Report no-win cases and admission failures.

Open questions: Can the narrow cover completion be distinguished from a routine Woodbury specialization? Does it provide measurable benefit after an incumbent is allowed the same factors and matrix-free application? Does a validated certificate fit the advertised memory? What graph population admits small state without unacceptable passes? Until answered, the defensible status is **a mathematically sound restricted prototype design with unresolved novelty and systems feasibility**, not an established new algorithm.

## Reproduction Appendix

The following independent script performs the random-Q audit and integer cover enumeration described above. It is intentionally in this review, not a new production/test file. Run it in a Python process with NumPy. It writes no files. Named helper functions are unnecessary; operations are local to each case.

```python
import numpy as np
from itertools import permutations

rng = np.random.default_rng(20260920)
out = dict(cases=0, partial_q_cases=0, dangling_input_cases=0,
           max_fixed_point_error=0., max_identity_scaled_error=0.,
           max_weighted_contraction_excess=0., max_jacobi_contraction_excess=0.)
for case in range(384):
    n = int(rng.integers(1, 7))
    r = int(rng.integers(0, 6))
    U = rng.integers(0, 5, (n, r)).astype(float)
    V = rng.integers(0, 5, (n, r)).astype(float)
    U[rng.random((n, r)) < .55] = 0
    V[rng.random((n, r)) < .55] = 0
    keep = (U.sum(0) > 0) & (V.sum(0) > 0)
    U, V = U[:, keep], V[:, keep]
    W = U @ V.T
    q = np.diag(W) * rng.choice([0, .25, .5, 1.], n)
    A = W - np.diag(q)
    d = A.sum(0)
    inv = np.divide(1., d, out=np.zeros_like(d), where=d > 0)
    z = (d == 0).astype(float)
    p = rng.integers(0, 5, n).astype(float)
    p[0] += 1
    p /= p.sum()
    for a in [.01, .85, .97, .9999]:
        Ub = np.column_stack((U, p))
        Vt = np.vstack((V.T * inv, z))
        c = Ub.sum(0)
        H = 1 / (1 + a * q * inv)
        P = A * inv + np.outer(p, z)
        b = (1 - a) * p
        M = a * (Vt * H) @ Ub
        g = Vt @ (H * b)
        hs = np.linalg.solve(np.eye(len(c)) - M, g)
        x = H * (b + a * Ub @ hs)
        ref = np.linalg.solve(np.eye(n) - a * P, b)
        out['max_fixed_point_error'] = max(
            out['max_fixed_point_error'], float(np.linalg.norm(x - ref, 1)))
        h = rng.normal(size=len(c))
        x = H * (b + a * Ub @ h)
        delta = g + M @ h - h
        residual = b + a * P @ x - x
        predicted = a * Ub @ delta
        scale = max(1., np.linalg.norm(residual, 1), np.linalg.norm(predicted, 1))
        out['max_identity_scaled_error'] = max(
            out['max_identity_scaled_error'],
            float(np.linalg.norm(residual - predicted, 1) / scale))
        bound = a / (1 - a) * np.dot(c, abs(delta))
        assert np.linalg.norm(x - ref, 1) <= bound + 1e-8 * max(1., bound)
        beta = max(a * (1 + q * inv) / (1 + a * q * inv))
        out['max_weighted_contraction_excess'] = max(
            out['max_weighted_contraction_excess'], float(max((c @ M) / c) - beta))
        diagonal = np.diag(M)
        cp = c * (1 - diagonal)
        J = (M - np.diag(diagonal)) / (1 - diagonal)[:, None]
        out['max_jacobi_contraction_excess'] = max(
            out['max_jacobi_contraction_excess'], float(max((cp @ J) / cp) - beta))
        out['cases'] += 1
        out['partial_q_cases'] += int(np.any((q > 0) & (q < np.diag(W))))
        out['dangling_input_cases'] += int(np.any((d == 0) & (A.sum(1) > 0)))
print(out)

valid = 0
for mask in range(512):
    E = [[((mask >> (3*i+j)) & 1) * (3*i+j+1) for j in range(3)]
         for i in range(3)]
    minimum, min_nonempty = 7, 7
    for bits in range(64):
        S = {j for j in range(3) if bits >> j & 1}
        T = {i for i in range(3) if bits >> (3+i) & 1}
        if any(E[i][j] and i not in T and j not in S
               for i in range(3) for j in range(3)):
            continue
        valid += 1
        U, V = [], []
        for i in T:
            if any(E[i]):
                U.append([int(k == i) for k in range(3)])
                V.append(E[i])
        for j in S:
            col = [E[i][j] if i not in T else 0 for i in range(3)]
            if any(col):
                U.append(col)
                V.append([int(k == j) for k in range(3)])
        restored = [[sum(u[i] * v[j] for u, v in zip(U, V))
                     for j in range(3)] for i in range(3)]
        assert restored == E
        minimum = min(minimum, len(S) + len(T))
        min_nonempty = min(min_nonempty, len(U))
    matching = max(sum(E[i][perm[i]] > 0 for i in range(3))
                   for perm in permutations(range(3)))
    assert minimum == matching == min_nonempty
print(dict(integer_cover_identities=valid, minimum_equals_matching_cases=512))
```

Minimal executable checks of the main analytical challenges:

```python
import numpy as np
from fractions import Fraction as F

a = .85
p = np.array([1., 0.])
for K in [100., 1000., 10000.]:
    U = np.array([K, 1.])
    V = np.array([1., 1/K])
    H = 1 / (1 + a * np.array([K, 1/K]))
    lam = a * np.dot(V * H, U)
    h = np.dot(V, p)
    x = H * ((1-a) * p + a * U * h)
    delta = np.dot(V, x) - h
    bound = a / (1-a) * U.sum() * abs(delta)
    predicted = int(np.ceil(np.log(1e-10/bound) / np.log(lam)))
    print('scalar_family', K, lam, bound, predicted, 'Jacobi_split', lam/(2-lam))
assert (float(2**54) + 1) - float(2**54) == 0

P = np.array([[0., 1.], [1., 0.]])
x = (1-a) * p + a * P @ p
updates = 0
while np.linalg.norm((1-a)*p + a*P@x - x, 1)/(1-a) > 1e-10:
    x = (1-a)*p + a*P@x
    updates += 1
print('Q_free_updates', updates)

p = np.array([.1, .2, .3, .4])
A = np.zeros((4, 4))
A[1:, 0], A[0, 1:] = [2, 3, 5], [7, 11, 13]
P = A / A.sum(0)
x0 = (a + (1-a)*p[0]) / (1+a)
x = np.r_[x0, (1-a)*p[1:] + a*A[1:, 0]/10*x0]
ref = np.linalg.solve(np.eye(4)-a*P, (1-a)*p)
print('star', x, 'direct_error', np.linalg.norm(x-ref, 1))

alpha = F(17, 20)
P = [[F(0), F(1, 2)], [F(1), F(1, 2)]]
x = [F(20, 57), F(37, 57)]
assert all((1-alpha)*F(1, 2) + alpha*sum(P[i][j]*x[j] for j in range(2)) == x[i]
           for i in range(2))
print('sink_exact', x)
```

## Revision Review: Release And Positive Kernel

Date: 2026-09-20. This is a bounded follow-up, not a replacement for the historical review above. Reviewed the current main manuscript, [representation revision](PageRank-Representation-Revision.md), and the **latest sparse-input** [positive release kernel](PageRank-Positive-Release-Kernel.md). Re-executed both saved Python blocks together. The kernel reviewed here includes group-weight storage, selected-ordinal/cursor layout requirements, and dense oracle construction outside the update function. No lead, probe, kernel, or production file was edited. Prior-art conclusions are delegated to the completed [primary-work comparison](PageRank-Prior-Art-Comparison.md); no duplicate literature search was performed.

**Assessment:** these are genuine algorithm corrections, not merely better caveats. Exact release preserves the original graph and degrees. Heavy-vertex release bounds cancellation-induced contraction deterioration while retaining factor-sized state. The row-only positive kernel also avoids materializing the potentially dense restored rows. I found no algebraic counterexample to those claims under the stated admission contract. In fact, the heavy-vertex count admits the stronger bound **`|R|<=r`, hence at most `2r` abstract factors**, proved below. None of this establishes a physical 4 GB solver, a numerical certificate implementation, or publication novelty.

### Remaining Findings

| ID | Severity | Finding and required boundary |
|---|---|---|
| RR-01 | P1, remaining gate | Positive arithmetic does not implement the source-operator error allowances. Validated degrees, loop remainders, reconstruction, residual evaluation, and output are still required before claiming certified numerical answers. |
| RR-02 | P1, remaining gate | The sparse logical schedule removes the restored-edge expansion, but is not a measured builder/decoder/worker memory contract. Price original-factor scratch, indices, validation, old/new overlap, and output. |
| RR-03 | P2, planner limit | Checking the original initial certificate does not prevent harmful releases. An exact near-K=1 example below changes certified updates from 45 to 156 even though that initial certificate fails. Keep a priced pilot/D0 comparison; do not claim dominance. |
| RR-04 | P2, admission order | An unconditional original-state initial check may allocate a representation the worker cannot admit. Check its reservation first; price simultaneous old/new coordinates or discard/spill them explicitly. |
| RR-05 | P2, evidence precision | The revision's released reference checks use positive constant states, despite the results table's signed-state wording. The new kernel separately tests signed states. Neither receipt tests certified floating stopping, external layout construction, or physical pass budgets. |

IR-02 now has a substantive mitigation, not a universal performance cure. IR-05's cumulative partial-cancellation rule is corrected and has exact tests. The optional original residual scan addresses IR-07's certificate conservatism without changing the underlying iterates. IR-01, IR-03, and IR-04 remain publication/performance gates, with more concrete proposed mechanisms than before.

### Release Identity And Degrees

For disjoint R,C, partition each matrix entry into selected destination rows, selected source columns outside those rows, or the remaining rectangle. Diagonal Q survives only in that rectangle. Therefore

```text
A = (I-D_R) U V^T (I-D_C) - Q_outside
      + D_R A + (I-D_R) A D_C.
```

This is an entrywise identity, so **degrees do not change**: `d'_j=sum_i A'_ij=sum_i A_ij=d_j`. It is incorrect to take degrees from the masked UV rectangle alone. Restoring A rather than W=UV^T preserves partial cancellation and real loops. For `U=V=(1,1)^T`, `q=(1/2,3/4)`, R={0}, C={1}, the true degrees are `(3/2,5/4)`. Restoring the full selected column without excluding R changes them to `(3/2,9/4)`; restoring the W row instead of the A row changes them to `(2,5/4)`. These are falsifiers of tempting implementations, not of the stated disjoint construction.

The threshold inversion is correct for `0<a<1`, `a<=gamma<1`. Leaving an active above-threshold vertex unreleased leaves its q/d unchanged, hence violates the **same maximum-ratio beta certificate**. This necessity is not necessity for the actual induced norm, spectral radius, or a different factorization. Concrete example: `U=V=(1,1)^T`, `Q=diag(1,0)`, `p=(1/2,1/2)`, `a=17/20` gives `beta=34/37>9/10`, but the actual c-weighted induced norm of M is `1309/1480<9/10`. Threshold release is unnecessary for that sharper contraction certificate.

### Stronger Heavy-Count Theorem

The submitted normalized-diagonal proof is valid: `s_i=d_i+q_i`, `q_i<=W_ii`, and `c_f V_if<=s_i` imply `sum_(s_i>0) q_i/s_i<=r`. Its general bound for `a<gamma<1` follows. But at the proposed heavy threshold it is weaker than an elementary principal-submatrix argument.

Let `S={j:d_j>0 and q_j>d_j}`. For each j in S,

```text
W_jj = q_j + A_jj > d_j >= sum_(i != j) A_ij
                              = sum_(i != j) W_ij.
```

Thus `W[S,S]` is strictly column-diagonally dominant. It is nonsingular: apply the usual maximum-component argument to its transpose and a hypothetical nonzero null vector. Consequently

```text
|S| = rank(W[S,S]) <= rank(W) <= r,
r_new <= r+|S| <= 2r,
beta_new <= 2a/(1+a),
1-beta_new >= (1-a)/(1+a).
```

This proof does not require the nonnegative rank to be minimal; r is the supplied factor count after empty-side removal. It covers partial Q and retained self-loops. It strengthens, rather than contradicts, the submitted `<2r` selected / `<3r` factor bound. The rank argument is standard linear algebra, not by itself a priority claim.

Both limitations matter. First, the inequality is **strictly** `q>d`: with `W=ones(2,2)`, Q=I, both vertices have q=d=1 although rank(W)=1. Second, the `2r` factor conclusion is specific to the heavy threshold, not all gamma. With `W=ones(3,3)`, Q=I, `a=1/2`, `gamma=7/12`, all three vertices exceed threshold 2/5 and row release requires three nonempty row factors although r=1. At gamma=a, the same construction with arbitrary N requires releasing all N vertices. The submitted general bound diverges consistently as gamma approaches a.

The stronger factor bound can be attained: use disjoint two-node blocks with `u=(2,1)`, `v=(1,1/2)`, `q=(2,1/2)`. Each block represents the unweighted two-cycle, has exactly one active heavy vertex, and row release retains one base factor and adds one row factor. For r such blocks, `|S|=r` and `r_new=2r`. At r=0, the admitted graph is edgeless with q=0; the vertex universe need not be empty, and its answer is p under the stated dangling policy.

**Applicability gap:** the representation revision added a binary-membership caveat during this review, and it is correct under its stated projection semantics. After singleton removal, each incident feature has size at least two, so `d_i=sum_f w_f*(size_f-1)>=sum_f w_f=q_i`. The heavy branch selects nothing. Evidence for its practical value therefore needs naturally occurring skewed directed/weighted factors with excess cancellation, not just that ordinary projection workload or deliberately ill-conditioned encodings of a trivial graph. The positive kernel remains algebraically usable for other selected sets, but the heavy-count guarantee alone does not establish a useful application distribution.

### Positive Kernel, Scores, And Norm

For row-only R and `y_i=x_i/d_i` on active sources (zero otherwise), the key equality is

```text
(Ay)_j = ell_j*y_j
         + sum_f U_jf*(g_off[f] + sum_(i in R, i != j) V_if*y_i),  j in R.
```

The second pass computes exactly this equality. Prefix/suffix exclusion must join selected source and destination memberships by ID, not by position; their support sets can differ. The retained base gather is the full `V_f^T y`, not merely g_off. This proves that the kernel applies the released T, so the existing reconstruction, contraction, and original-answer theorems transfer. No normalization or different PageRank problem is introduced. Arbitrary reconstructed X(h) need not yet sum to one; normalizing output still needs its own error allowance.

Dropping a base coordinate does **not** permit dropping that original factor's gather scratch or selected memberships. Small falsifier:

```text
U = [[2,1],[1,0]], V = [[1,0],[1/2,1]], q=(2,1/2), A=[[0,2],[1,0]],
R={0}, a=1/2, p=(1/2,1/2), h_base=(1), h_R=(0), h_d=0.
x=(1/4,2/3), y=(1/4,1/3).
```

The second original factor has no retained base coordinate and no selected-source membership, but `g_off[1]=1/3` is needed by the selected destination. Correct `next_R[0]=2/3`; skipping that factor gives 1/3. The current kernel correctly retains it. This also explains why its scratch dimension is original r, not r0 or an optimistically pruned released count.

The norm must use the released U masses:

```text
c_base[f] = sum_(i outside R) U_if,   c_R[j]=1,   c_d=1.
residual_i = a*(sum_f U_if*delta_base[f] + p_i*delta_d),  i outside R;
residual_j = a*(delta_R[j] + p_j*delta_d),               j in R.
```

There is no H in this residual. The selected rows require neither implicit dense V factors nor restored A rows to certify X(h). Old base masses can give a conservative triangle bound, but do not inherit the claimed released weighted contraction proof. Positivity holds for the initialized base iteration, not arbitrary signed h, residual differences, or every accelerated solver.

The latest scalar accounting is internally consistent as a reservation for the specified arrays: `2*r0+r+7*k+4`, including both group-weight lists. Combining it with `k<=r` gives **at most `10r+4` scalar slots**, not the earlier `<17r+4`. IDs, maps, row decoder buffers, certificate weights, numerical compensation/enclosures, runtime and output remain additional charges. This is not a measurement of the Python fixture's allocations: for example its concatenated return value and resident input lists have separate Python lifetimes.

The selected index really has at most Z extra membership records, and the sparse logical update can be `O(N+Z+Z_UR+Z_VR+r+k)`. Original IDs compiled to selected ordinals and a sorted row cursor support that claim. The saved fixture uses Python maps/sets for the adapter and lookup; it does not implement the external builder or prove physical worst-case lookup/decoder behavior. Row-only release now avoids the O(Nr) restored-edge artifact; the mixed row/column builder does not automatically inherit this result.

### Initialization And Pass Counterexample

The new initial-certificate check repairs the K=1 regression, but does not make release generally beneficial. Independently iterating with exact Fractions for the same two-cycle family, `K=100000001/100000000`, `a=17/20`, `p=(1,0)`, `gamma=9/10`, and error target `10^-10` gives:

| Measurement | Original one-factor plan | Release both rows |
|---|---:|---:|
| Initial cheap error bound | `1133333339/268431375233333340` (about 4.2221e-9) | `289/30` |
| First update index meeting true L1 error | 45 | 141 |
| First update index meeting cheap certificate | 45 | 156 |

The initial original bound is above target, so the new guard legitimately does not stop. The release still worsens iteration count before counting build or extra I/O. This falsifies an extrapolated dominance claim, not the actual beta theorem. A priced pilot and the new representation's D0 remain meaningful planner inputs.

Coordinates and c change after release. Do not copy old h or D0 into the new representation. Gather `h0=Vbar_new^T p`; then apply the new T to measure D0. For the positive kernel each gather/update has a row phase and a selected-factor phase. Thus a bound on k updates is not a bound on k file scans. Charge initialization, D0 evaluation, any pilot, optional tighter residual, and output separately. Warm-starting from an old reconstructed x instead of p is possible but needs an explicit old/new lifetime and gather schedule.

Also admit the **original** initial-check workspace before allocating it. For example m duplicate factors `u_f=(1,1)`, `v_f=(1/m,1/m)`, Q=I represent a fixed two-node graph: old state grows with m, while explicit release of both rows has two factors. Duplicate merging is another operation, not an assumed free rescue. The positive implicit kernel still needs original-r g_off scratch; it cannot claim the two-coordinate abstract release count as its total workspace. Keeping a pilot state alive while constructing another candidate must be charged as overlap, or explicitly discarded/spilled.

### Numerical Certificate And Two-Vector Lifetime

The main manuscript's source allowance is mathematically sufficient if its terms are actually certified:

```text
(R_computed + eta_eval + eta_b + a*eta_P*||x||_1)/(1-a) + eta_publish.
```

Use the resolvent bound for stochastic P_source and the same a, with `||P_source-P_hat||_1<=eta_P` and the specified right-hand-side bound. The x in every term must be the same reconstructed answer; eta_eval must cover evaluating its represented residual, including reconstruction/gather discrepancy. A subsequent output transformation belongs in eta_publish. `||x||_1` can be accumulated while streaming x, without an N-array. The source factor validation remains indispensable: `P_hat=I`, P_source the two-cycle swap, `a=1/2`, p=x=e0 gives represented residual zero but true answer error 2/3.

For exact base iteration, preserve h, overwrite next with `delta=T(h)-h`, scan the original residual, and restore `next=h+delta` only if continuing. Return X(h), which that delta certifies. For optional Jacobi, continuation instead needs `h+delta/(1-m)` coordinatewise. Already at a scalar reduced system with m=1/2, h=0 and T(h)=1/2, these continuations differ: base gives 1/2 and Jacobi gives 1.

The floating round trip is not lossless: h=2^54 and next=1/4 yield delta=-2^54 and restored next=0 in binary64. The manuscript now acknowledges arithmetic error here; an implementation must either retain the intended next value with priced storage/recomputation or include the changed update in its error analysis. For `h_(k+1)=T(h_k)+e_k`, the exact residual increments satisfy

```text
delta_(k+1) = M*delta_k + (M-I)*e_k,
||delta_(k+1)||_c <= gamma*||delta_k||_c + (1+gamma)*||e_k||_c.
```

Consequently pure `gamma^k D0` decay is an exact-iteration bound, not automatically an inexact pass guarantee. A positive gather avoids a dangerous dynamic q*y subtraction, but does not eliminate coefficient cancellation, overflow, underflow, signed residual cancellation, or a numerical error floor exceeding the requested tolerance.

### Probe Scope And Independent Checks

Latest saved-source extraction returned exit 0. Revision receipt: 216 rational cases, 36 partial-Q cases, 216 original signed-state checks, 274 release orientations, 3,168 selection subsets, 2,112 singleton-ledger checks, 216 released reference checks, and 216 each normalized-diagonal/heavy-release checks. Kernel receipt: 128 fixtures, 128 warm checks, 128 signed checks, 128 nonnegative checks, 640 positive trajectory steps, maximum selected-source group size 6, and the separate exact K=2^54 fixture.

Interpret these counts narrowly. The revision has 72 sampled factor shapes reused at three dampings; released and original checks do not represent independent graph samples. Its released direct-solve check calls `validate_exact_case_contract` on `[1/3,...,1/3]`, not signed states. Its subset sweep is row-only; orientations vary on the chosen bad set. The main random gamma is `(9a+1)/10`. The singleton test exhausts q only up to total singleton mass, so it does not test a positive q remainder owned by other factors. Its float controls stop against a known answer, not the reported certificate, and exclude build/output traffic. The kernel's sparse visit counters validate membership accounting, not encoded bytes or total execution time. Its K=2^54 fixture checks nonnegativity using exact admitted coefficients, not successful binary64 admission or a certified answer at that scale.

Additional ephemeral standard-library computations performed independently during this review:

| Test domain | Result |
|---|---|
| Rank-one n=3, nonzero u,v in `{0,1,2}^3`, q_i in `{0,1/2,1}*u_i*v_i` | 18,252 parameter tuples; normalized mass<=1 and heavy count<=1 throughout. These include duplicate represented graphs. |
| Same tuples, three `(a,gamma)` choices: `(1/2,7/12)`, `(1/2,2/3)`, `(17/20,9/10)` | 54,756 checks of the general count inequality; no failures. |
| Rank-one n=2 with zero sides allowed, partial Q, every assignment to unselected/row/column | 6,561 release/degree/count checks and 19,683 threshold checks, including gamma=a; no failures. |
| Separate positive-kernel implementation, 64 seeded shapes n=1..5, r=1..4, every selected subset, a=17/20 | 730 exact comparisons against explicit released rows; includes original residual/cheap-bound checks, 691 selected-support mismatches and 187 needed contributions from dropped base factors. No failures. |
| Sharpness, norm-versus-beta, near-K=1, source mismatch, next/delta round trip | Concrete boundary/counterexamples described above; minimal reproduction below. |

These finite results are adversarial evidence, not proofs of general correctness or experiments establishing a runtime advantage. The separately proved identities and bounds do the mathematical work. The strongest surviving research candidate is now the specific combination of contraction-budgeted exact release, bounded factor-state growth, sparse positive application without restored-edge materialization, and source-aware certification. Its priority and practical advantage still require the closest-work comparison and measured implementations; neither the trace bound nor leave-one-out summation makes that combination novel by assertion.

### Revision Reproduction

Run the two author blocks and then only this review's follow-up Python block. This command writes no files and uses no third-party Python dependencies:

```sh
awk '
  /^## Revision Review:/{review=1}
  FILENAME ~ /Independent-Review/ && !review {next}
  /^```python$/{inside=1;next}
  /^```$/{if(inside){inside=0;next}}
  inside{print}
' research_algorithms_20260920/PageRank-Representation-Revision.md \
  research_algorithms_20260920/PageRank-Positive-Release-Kernel.md \
  research_algorithms_20260920/PageRank-Independent-Review.md | python3
```

The following compact falsifiers reuse the exact builder only as an oracle; the near-K recurrence and stopping conditions are checked here, independently of the author's float iteration counter.

```python
def count_exact_review_updates(op):
    a = op['a']
    h = [sum(v*p for v,p in zip(row,op['p'])) for row in op['Vt']]
    n = len(op['p'])
    ref = solve_exact_linear_system(
        [[F(i==j)-a*op['P'][i][j] for j in range(n)] for i in range(n)], op['b'])
    hits = [None,None]
    initial = None
    for k in range(300):
        x = [op['H'][i]*(op['b'][i]+a*sum(u*v for u,v in zip(row,h)))
             for i,row in enumerate(op['Ub'])]
        nxt = [g+sum(m*v for m,v in zip(row,h)) for g,row in zip(op['g'],op['M'])]
        cheap = a*sum(c*abs(v-w) for c,v,w in zip(op['c'],nxt,h))/(1-a)
        if initial is None:
            initial = cheap
        for j,value in enumerate([sum(abs(v-w) for v,w in zip(x,ref)),cheap]):
            if hits[j] is None and value <= F(1,10**10):
                hits[j] = k
        if all(value is not None for value in hits):
            return initial,hits
        h = nxt
    raise AssertionError('review cap exhausted')

K,a,p = F(100000001,100000000),F(17,20),[F(1),F(0)]
U,V,q = [[K],[F(1)]],[[F(1)],[1/K]],[K,1/K]
old = build_exact_pagerank_operator(U,V,q,p,a)
Un,Vn,qn = release_selected_diagonal_vertices(U,V,q,old['A'],{0,1},set())
new = build_exact_pagerank_operator(Un,Vn,qn,p,a)
before,after = count_exact_review_updates(old),count_exact_review_updates(new)
assert before == (F(1133333339,268431375233333340),[45,45])
assert after == (F(289,30),[141,156])
print('near_K_certificate_regression',str(before[0]),before[1],str(after[0]),after[1])

U=V=[[F(1)],[F(1)]]
op = build_exact_pagerank_operator(U,V,[F(1),F(0)],[F(1,2)]*2,F(17,20))
actual = max(sum(op['c'][i]*op['M'][i][j] for i in range(len(op['c'])))/c
             for j,c in enumerate(op['c']) if c)
assert op['beta'] == F(34,37) and actual == F(1309,1480)
assert actual < F(9,10) < op['beta']
boundary = build_exact_pagerank_operator(U,V,[F(1)]*2,[F(1,2)]*2,F(1,2))
assert sum(q>=d for q,d in zip([F(1)]*2,boundary['d'])) == 2

U,V = [[F(2),F(1)],[F(1),F(0)]],[[F(1),F(0)],[F(1,2),F(1)]]
plan = prepare_sparse_release_fixture(U,V,[F(2),F(1,2)],[F(1,2)]*2,{0})
value,active,_ = apply_positive_released_kernel(plan,F(1,2),{0},[F(1),F(0),F(0)])
assert active == [0] and value == [F(5,12),F(2,3),F(0)]
assert F(2,3)-F(1,3) == F(1,3)  # Lost goff from the dropped base factor.

for blocks in [1,2,3]:
    U = [[F(0)]*blocks for _ in range(2*blocks)]
    V = [[F(0)]*blocks for _ in range(2*blocks)]
    for f in range(blocks):
        U[2*f][f],U[2*f+1][f] = F(2),F(1)
        V[2*f][f],V[2*f+1][f] = F(1),F(1,2)
    q = [F(2),F(1,2)]*blocks
    op = build_exact_pagerank_operator(U,V,q,[F(1,2*blocks)]*(2*blocks),F(17,20))
    R = {i for i,d in enumerate(op['d']) if d and q[i]>d}
    Ur,Vr,qr = release_selected_diagonal_vertices(U,V,q,op['A'],R,set())
    assert len(R) == blocks and len(Ur[0]) == 2*blocks

U=V=[[F(1)]]*3
op = build_exact_pagerank_operator(U,V,[F(1)]*3,[F(1,3)]*3,F(1,2))
Ur,Vr,qr = release_selected_diagonal_vertices(U,V,[F(1)]*3,op['A'],{0,1,2},set())
assert all(F(1)/d>F(2,5) for d in op['d']) and len(Ur[0]) == 3

x,reference,b = [F(1),F(0)],[F(2,3),F(1,3)],[F(1,2),F(0)]
assert all(b[i]+F(1,2)*x[i]-x[i] == 0 for i in range(2))
assert sum(abs(v-w) for v,w in zip(x,reference)) == F(2,3)
assert all(b[i]+F(1,2)*reference[1-i] == reference[i] for i in range(2))
h,nxt = float(2**54),.25
assert h+(nxt-h) == 0.0 and h+(nxt-h) != nxt
print('revision_boundary_falsifiers', 'passed')
```

## Staircase Review: Count, Stream, And Sharpness

Date: 2026-09-20. Scope is only the new [release staircase](PageRank-Release-Staircase.md): its packing theorem, candidate stream, sharpness, and resulting resource/contraction interpretation. The standalone saved probe was extracted and executed, returning exit 0 with 1,600 random threshold cases, 24 sharp-rank cases, and eight equality cases. No older release tests were rerun. No additional priority claim or literature search is made here; the attributed convex-factor interpretation is not treated as new.

**Verdict:** no proof-level counterexample found under the nonnegative, exact admitted-coefficient contract. The `r*ceil(1/t)` count and the stated fixed-diagonal sharpness are correct. The candidate stream is genuinely bounded **before** deduplication, but only when it tests canonical matrix entries with correct strict comparisons. Numerical admission and stream construction remain unimplemented conditions, not consequences of the packing proof.

### Findings And Boundaries

| ID | Severity | Independent finding |
|---|---|---|
| SR-01 | P2, numerical admission | Both `ceil(1/t)` and witness comparisons have discontinuities at reciprocal integers. A binary64 threshold can under-reserve the exact candidate count and miss every selected vertex; explicit example below. The document's exact/enclosed requirement is necessary, not optional. |
| SR-02 | P2, stream contract | Testing raw duplicate membership records instead of coalesced U_if can miss a required witness. The emitter must inherit the kernel's duplicate-coalescing requirement and price it before claiming completeness. This is not a counterexample when U_if already denotes a canonical matrix entry. |
| SR-03 | P2, resource interpretation | At most m emitted witnesses per factor does not mean at most m selected memberships per factor. The selected execution index can contain `2*m*r^2` memberships even when the candidate stream contains only mr records. Do not shrink group scratch or index budgets using the witness count. |
| SR-04 | P2, scope | The lower bound fixes the resulting Q_outside. It is not global PageRank state optimality, optimal factorization allowing another diagonal, or an iteration/runtime lower bound. The manuscript mostly preserves this distinction; keep it adjacent to any shortened claim of sharpness. |

### Proof Audit

After removing zero-mass factor columns, `S_f>0`. At an active j, `s_j=d_j+q_j>0`, and

```text
sum_f S_f*V_jf/s_j = 1,
W_jj/s_j = sum_f (S_f*V_jf/s_j)*(U_jf/S_f).
```

Thus `q_j>t*d_j` implies a factor witness with `U_jf/S_f>tau=t/(1+t)`. Strict positivity of the mixture excess is enough; the witness need not be unique. For one normalized U column, k such coordinates give `k*tau<1`, hence `k<1+1/t`, whose integer form is exactly `k<=ceil(1/t)`. Summing these **record counts** over factors gives the pre-deduplication bound; taking the union cannot increase it. Partial Q, positive real loops, zero V entries, and unselected dangling columns do not invalidate the argument. r=0 is vacuous. This uses the supplied nonnegative factor count, not an unproved replacement by ordinary rank for arbitrary t.

Substitution into the existing row-release theorem is correct: `k<=mr`, `r_new<=r+k`, and `beta<=a*(1+t)/(1+a*t)`. At `t=1/m`, the scalar reservation `2*r0+r+7*k+4` is at most `(3+7*m)*r+4`. These are upper bounds, not equalities for every graph. The extra dangling coordinate is outside the abstract factor count and already inside the scalar allowance.

The guarantee is O(mr), not O(r) with a constant uniform over every requested gamma. In particular `gamma_m-a=a*(1-a)/(m+a)`. Driving gamma toward a can make m arbitrarily large; the actual selected count also has the trivial cap N. The staircase bounds payload and a particular contraction certificate, not D0, actual pass count, floating precision, output traffic, or total physical memory. The supplied numeric table is consistent with eight-byte scalar arithmetic, not measured residency.

### Sharpness Survives, And Extends

For the submitted block, selected rows have ratio `1/(m-1+epsilon)>1/m`, while the last row has `epsilon/m<1/m`. The post-release matrix `B=A+Q_outside=u*1^T-diag(1,...,1,0)` is nonsingular: its last row forces `sum(x)=0` for a null vector, then the first m rows force all their coordinates to zero, and the last coordinate follows. Thus ordinary rank already forces m+1 factors for this **fixed** B. Disjoint blocks add ranks and give the claimed `(m+1)r` lower bound, with the construction attaining it after empty-side removal.

This extends to every finite t>0. Put `m=ceil(1/t)` and choose

```text
0 < epsilon < min(1, 1/t-(m-1)).
```

The upper endpoint is positive, including when 1/t is integral. The first m ratios exceed t; the last ratio `epsilon/m<t` because `m*t>=1` and epsilon<1. The same nonsingularity argument applies. Therefore the general selected-count bound and fixed-Q factor cap are also attained, not only their reciprocal-integer specialization. The independent probe below checks 27 thresholds, including both sides of reciprocal boundaries. This is a straightforward extension of the supplied witness, not a novelty claim.

Conversely, allowing Q to change again recovers the original one-factor representation. The sharpness result consequently does not settle the best representation of A, nor the best PageRank solver under a memory or contraction target. These blocks remain easy structured linear systems, not performance victories over direct or structured baselines.

### Candidate-Stream Falsifiers

**Threshold rounding:** take `t=1/3-10^-20`, `U=V=ones(4,1)`, Q=I. Every vertex has d=3 and q=1, so all four are selected and all four are witnesses. The exact budget is `ceil(1/t)=4`. Binary64 rounds this t to its representation of 1/3, gives `ceil(1/float(t))=3`, and evaluates the candidate threshold as 1/4, missing all four equality-looking witnesses. A stable division alone does not fix this: the exact threshold and the integer budget must be validated together.

An enclosure-based emitter also needs an explicit ambiguity policy. At exact `t=1/m`, a uniform column on m+1 vertices has no strict witnesses. Emitting all entries whose intervals *possibly* exceed tau can nevertheless emit m+1 records, violating the stated m-record cap. Emitting only definite witnesses can omit a true but unresolved witness. Refine/reject unresolved comparisons, or separately admit a conservative superset without claiming the exact stream cap. The manuscript already recognizes this issue for selection; apply the same rule to candidate discovery and the ceiling operation.

**Uncoalesced records:** at t=1, use `u=(1,1/4)`, v=(1,1), Q=diag(u). Vertex 0 must be selected, and its canonical U entry 1 exceeds `tau*S=5/8`. If that entry arrives as two records of weight 1/2 and the emitter tests each record separately, neither passes. The bound is about U_if, not arbitrary fragments of U_if. This admission requirement is already present in the positive-kernel contract and must propagate to the earlier candidate pass.

**Witnesses are not execution memberships:** use r=3, m=2 copies of the sharp block with epsilon=1/4, then add 1/1000 to every entry of both U and V and set Q=diag(UV^T). The exact probe finds six selected vertices and six witness records. Yet every original factor has all six selected vertices in its U and V membership lists: group size six, not m=2, and 36 selected memberships, not six. The existing O(k) group allowance and O(Z) index allowance survive; an inferred O(m) group allowance or O(mr) execution index does not. The candidate list is an ID-discovery filter, not the selected-factor execution index.

Duplicate witnesses also remain distinct pre-dedup records: three identical factors with normalized shape proportional to `(1,1,1/4)` at t=1/2 emit six records for only two selected vertices. Store or stream the witness records within the mr allowance and deduplicate IDs before constructing row coordinates. Neither case refutes the proposed emitted-record bound.

Factor totals S_f need a completed/validated pass or existing metadata before the witness test. Canonicalization, totals, candidate sorting, the q/d join, and selected-order construction are build work, not included for free in the query scalar formula. The U threshold does not use V and can emit false positives even when q=0; the final q/d filter remains necessary. A direct q/d row scan is a valid alternative, as the manuscript notes.

### New Independent Probe

This retained standard-library probe adds 27,621 exhaustive local packing checks, 27 arbitrary-threshold sharp-rank checks, and the numerical/coalescing/group-size falsifiers above. It does not repeat the old release identity or PageRank solver suites. Exhaustive packing uses all 1,023 nonzero five-entry vectors over `{0,1,2,3}` and 27 thresholds; these are parameter cases, not distinct normalized vectors. The coupled example tests overlapping factors rather than only disjoint sharp blocks. No mismatches with the exact theorem were found.

To execute just this new review probe, without any older code blocks:

```sh
awk '
  /^## Staircase Review:/{review=1}
  !review{next}
  /^```python$/{inside=1;next}
  /^```$/{if(inside){inside=0;next}}
  inside{print}
' research_algorithms_20260920/PageRank-Independent-Review.md | python3
```

```python
from fractions import Fraction as F
from itertools import product
from math import ceil

def inspect_exact_staircase_candidate(U,V,q,t):
    n,r=len(U),len(U[0])
    W=[[sum(U[i][f]*V[j][f] for f in range(r)) for j in range(n)] for i in range(n)]
    d=[sum(W[i][j] for i in range(n))-q[j] for j in range(n)]
    assert all(F(0)<=q[i]<=W[i][i] for i in range(n))
    totals=[sum(row[f] for row in U) for f in range(r)]
    emissions=[(i,f) for f,S in enumerate(totals) if S for i in range(n)
               if (1+t)*U[i][f]>t*S]
    chosen={i for i in range(n) if d[i]>0 and q[i]>t*d[i]}
    assert chosen <= {i for i,f in emissions}
    assert len(chosen)<=len(emissions)<=r*ceil(1/t)
    return chosen,emissions,W

def compute_fraction_matrix_rank(A):
    rows=[list(map(F,row)) for row in A]
    pivots=0
    for j in range(len(rows[0])):
        pivot=next((i for i in range(pivots,len(rows)) if rows[i][j]),None)
        if pivot is None: continue
        rows[pivots],rows[pivot]=rows[pivot],rows[pivots]
        base=rows[pivots][j]
        rows[pivots]=[x/base for x in rows[pivots]]
        for i in range(pivots+1,len(rows)):
            scale=rows[i][j]
            rows[i]=[x-scale*y for x,y in zip(rows[i],rows[pivots])]
        pivots+=1
    return pivots

thresholds=[F(1,m)+sign*F(1,1000) for m in range(1,9) for sign in [-1,0,1]]+[F(2,5),F(3,2),F(4)]
packing=0
for raw in product(range(4),repeat=5):
    S=sum(raw)
    if not S: continue
    for t in thresholds:
        assert sum((1+t)*u>t*S for u in raw)<=ceil(1/t)
        packing+=1
sharp=0
for t in thresholds:
    m=ceil(1/t)
    eps=min(F(1),1/t-(m-1))/2
    u=[F(1)]*m+[eps]
    R,E,W=inspect_exact_staircase_candidate([[x] for x in u],[[F(1)]]*(m+1),u,t)
    assert len(R)==len(E)==m
    B=[[W[i][j]-(u[i] if i==j and i in R else 0) for j in range(m+1)] for i in range(m+1)]
    assert compute_fraction_matrix_rank(B)==m+1
    sharp+=1

t=F(1,3)-F(1,10**20)
R,E,_=inspect_exact_staircase_candidate([[F(1)]]*4,[[F(1)]]*4,[F(1)]*4,t)
assert len(R)==len(E)==ceil(1/t)==4
assert ceil(1/float(t))==3
assert not any(.25>float(t)/(1+float(t)) for _ in range(4))

R,E,_=inspect_exact_staircase_candidate([[F(1)],[F(1,4)]],[[F(1)]]*2,[F(1),F(1,4)],F(1))
assert R=={0} and E==[(0,0)]
assert all(weight<=F(1,2)*F(5,4) for weight in [F(1,2),F(1,2),F(1,4)])

m,r=2,3
n=(m+1)*r; eta=F(1,1000)
U=[[eta+(F(1) if i%(m+1)<m else F(1,4))*(i//(m+1)==f) for f in range(r)] for i in range(n)]
V=[[eta+F(i//(m+1)==f) for f in range(r)] for i in range(n)]
q=[sum(u*v for u,v in zip(ui,vi)) for ui,vi in zip(U,V)]
R,E,_=inspect_exact_staircase_candidate(U,V,q,F(1,m))
assert len(R)==len(E)==m*r
assert all(sum(V[i][f]>0 for i in R)==m*r>m for f in range(r))
assert sum(bool(U[i][f])+bool(V[i][f]) for i in R for f in range(r))==2*m*r*r

U=[[F(1)]*3,[F(1)]*3,[F(1,4)]*3]
V=[[F(1)]*3 for _ in U]
q=[sum(u*v for u,v in zip(ui,vi)) for ui,vi in zip(U,V)]
R,E,_=inspect_exact_staircase_candidate(U,V,q,F(1,2))
assert len(E)==6 and len(R)==2
R,E,_=inspect_exact_staircase_candidate([[],[]],[[],[]],[F(0)]*2,F(1,2))
assert not R and not E
print(dict(packing_checks=packing,arbitrary_threshold_sharp_ranks=sharp,
           float_budget_exact=4,float_budget_rounded=3,dense_group_size=6,
           dense_selected_memberships=36,duplicate_witness_records=6,duplicate_witness_vertices=2,
           coalescing_falsifier=True,empty_factors_checked=True))
```

## Independent Anisotropic And Frontier Challenge (2026-09-20)

Append-only scope: [anisotropic release](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/PageRank-Anisotropic-Release.md), SHA256 `9af28abb465595784c5d2690b60a146f4986e464da7eafd4cdd0fe8bce84d9bb`, and [mixed reservation frontier](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/PageRank-Mixed-Reservation-Frontier.md), SHA256 `fc8b77ff2a38a7dc278bd9602473a9f590db1138f13137e2305595f7c6fe94a7`. The lead's 64/64/50 and 240-case suites were not rerun. No implementation or physical-RAM claim is established.

### Findings And Tightenings

1. **[P2, omitted candidate] The cheap all-column specialization is already available from the mixed schedule.** The frontier explicitly excludes it, so its declared-portfolio theorem is not false. But when `R=empty`, phase 3 and `g_off` are unused: omit that r-sized array and its accumulation. The remaining reservation is `B_col=2*r0(empty)+r+7*k+4`, with unchanged reconstruction and residual identities. This can beat both declared candidates even when all restored stars are nonempty. Exact witness:
   `U=[[1,1],[1,0],[0,1]], V=[[1,0],[0,1],[0,0]], q=(1,0,0), J={0}`.
   Here `A=[[0,1,0],[1,0,0],[0,1,0]]`, `d=(1,2,0)`; J is mandatory at t=1/2. Base counts are one for all-column, two for all-row. The declared portfolio has `L=OPT=U=17`, but the specialized column plan reserves 15. Thus `H<L` is not a rejection certificate for this immediate extension. Admit `B_col` and take its minimum with both L and U; the same sandwich, additive-gap and ratio proofs remain valid for the enlarged portfolio. This is a schedule deduction, not measured memory.

2. **[P2, admission wording] Fractional budgets cannot use the integer cap.** Anisotropic Theorem 1 correctly requires integer `b_f>=0`, but the final admission sentence (line 139) permits "rational candidate budgets" without repeating that restriction. For `r=1, U=(3/4,1/4)^T, V=(1,1)^T, q=(3/4,1/4)`, take `b=1/2, theta=tau=2/3`. Every mixture-envelope check passes, yet there is one witness and one heavy active vertex, exceeding `B=1/2`. This does not refute the integer theorem. Tightening: round rational relaxation candidates upward to integer budgets before reserving `sum b`; alternatively keep fractional thresholds but reserve `sum ceil(b_f)`. Envelope certification alone cannot validate a fractional count reservation.

3. **[P2, positioning] The survival objective is a standard directed all-or-nothing hypergraph cut.** This strengthens the manuscript's existing prior-art acknowledgment, not its mathematical theorem. Let `X={source} union R`; tails `T_f=(K_f intersect J)` additionally contain source when K_f has an outside-J member; heads `H_f=(L_f intersect J)` additionally contain sink when L_f has an outside-J member. Then `survive_f=1[T_f intersects X AND H_f is not a subset of X]`. That exact splitting function is printed in [Kenneth, September 2023, Section 1.1, printed p. 3 / PDF p. 6](https://www.wisdom.weizmann.ac.il/~robi/files/Yotam.Kenneth-MScThesis-2024_01.pdf#page=6). The two-auxiliary construction is an incidence-restricted Lawler-style expansion: [Veldt, Benson and Kleinberg, Section 4.1, p. 12](https://arxiv.org/pdf/2001.02817#page=12) explicitly displays the undirected all-or-nothing version. The latter is not itself evidence for unequal tail/head sets; the displayed substitution and gadget proof establish that specialization here. Renaming terminals or adding unary prices does not establish a new optimization class.

4. **[P3, objective scope] Keep factor optimality separate from buffer optimality and preserve the narrow obstruction claim.** A simple additional witness is `U=V=(1,1,1)^T, q=(1,1,0), J={0,1}`, mandatory at t=1/3. All orientations retain one base and two nonempty stars, but the declared mixed bills, in order R=empty,{0},{1},J, are `22,18,18,22`. An arbitrary factor-minimum cut can miss the smaller reservation. This bill violates submodularity. The manuscript's separate empty-star counts `4,2,3,2` also violate submodularity in the common source-means-row labels, but complementing one variable in that two-variable example makes it submodular. Neither example alone excludes different encodings or polynomial algorithms. Auxiliary minimization preserves submodularity: [Veldt et al., Eq. (4.2), Theorem 4.2 and Corollary 4.3, p. 15](https://arxiv.org/pdf/2001.02817#page=15). Do not infer its converse for arbitrary higher-order objectives.

### Correctness And Admission

**Premise and lenses.** This is exact supplied-representation algebra. The challenge used packing/admission, combinatorial optimization, numerical certification, and resource accounting lenses. Under integer-budget, canonical-input and exact-coefficient assumptions, no counterexample was found to the two anisotropic theorems or three-phase identities.

- **Budget and contraction:** `q_j/s_j <= sum_f pi_jf*z_jf`; no strict witness implies at most the admitted envelope. At most integer b_f normalized entries exceed `1/(b_f+1)`. Deduplication and the heavy filter give k<=B; unreleased `q/d<=tau/(1-tau)` gives `beta<=a/[1-(1-a)tau]`. Every positive-s column needs the pointwise admission check. The conditional cap is not a smaller universal rank bound.
- **Strict rank witness:** The displayed U_O and V_O minors make W_OO nonsingular. Its original Schur complement is zero, and removing the two positive diagonal debts changes it to `diag(-q_0,-q_1)`. Rank five therefore follows, but only for that fixed `W'=A+Q'`. This was a proof check, not a duplicate determinant suite or a lower bound on all PageRank solvers.
- **Cut:** Outside forcing, incidence directions, zero factor prices and the finite-infinity rule check out. Standard graph machinery already allows more than nonnegative unaries: shift each finite signed unary pair by its minimum, and unite the graph with nonnegative directed pair penalties or other graph-representable gadgets. This is a direct construction; arbitrary empty-star savings and balance penalties are not thereby admitted. A flow certificate verifies the additive objective, not whether it models actual encoded/build cost.
- **Mixed evaluation:** Dropped bases must remain in original-r w and g_off computations. The targeted fixtures exercise dropped bases, multi-vertex groups with unequal sizes, partial cancellation, a dangling source with incoming edges, zeros in p, and an empty column star. Zero plus every coordinate basis state checks the entire affine map per fixture; one signed state additionally checks exclusion arithmetic without a positivity premise.
- **Residual:** With `F=Ubar', G=Vbar', x(h)=H(b+a Fh), T(h)=G^T x(h)`, the identity is `b+a P x(h)-x(h)=a F(T(h)-h)`, without H on the right. Use `c=1^T F`, including `c_C[j]=sum_(i outside R) A_ij`. A zero-mass column must be removed or treated through the quotient seminorm. The probe checks this residual, its weighted bound, and `c^T M<=beta*c^T`. Original-space error conversion remains division by 1-a, plus source/evaluation/output allowances. Exact identities and positivity do not certify rounded ell, c_C, support/envelope decisions, or signed residual evaluation.
- **Lifetime bill:** `2(r0+k+1)+2r+k+2(M+1)+2M=2r0+2r+3k+4M+4` is consistent as a conservative arithmetic-buffer reservation. Both original-r arrays coexist in the general entity pass. The all-column specialization avoids one because there is no row-restoration pass. IDs, indices, maps, certificate weights, scalar/enclosure widths, decoding, planning and output remain extra. This establishes neither allocator lifetimes nor source-scoped physical RAM.

### Bounded Frontier Check

The frontier sandwich is correct for exactly its declared portfolio. Every mixed plan has `r0>=r_min` and `max(ell,k-ell)>=ceil(k/2)`, proving L_mixed. A cut plan is feasible, and its excess over an optimal mixed plan is at most `4*floor(k/2)`. Taking minima with the same B_row preserves both inequalities and this additive bound. If a specialized candidate is optimal, the upper bound equals it; otherwise the optimum is mixed and at least `2r+5k+4`. This proves the stated strict 7/5 ratio, k=0 exactness and the k<=m*r tier bounds. Adding the B_col candidate above preserves that argument. It does not convert reservation formulas into intrinsic memory lower bounds.

The strict nonempty-star family checks analytically for all even k>=4: the outside vertex preserves the global factor; the J-only factor survives exactly for proper nonempty R. Selected degrees are 2k-1, so `2/(2k-1)>1/k` verifies mandatory selection. The best balanced mixed bill is `5k+12`, while both specialized pure bills are `7k+8`. Thus the new all-column candidate does not remove the separation. The factor-minimum portfolio ratio `(7k+8)/(5k+12)` tends to 7/5: the universal constant is asymptotically sharp for this portfolio and planner, not merely supported by the finite observed ratio 13/10.

For any proper set of size ell, its uniform-price objective exceeds the empty set by `1+lambda*ell>0` when lambda>=0, and exceeds the full set by `1+lambda*(ell-k)>0` when lambda<=0. This proves sweep failure for every real price, not just sampled prices. An independent k=12 cardinality-class check gives `(L,OPT,U)=(70,72,92)`; symmetry covers all orientations. No 240-case replay was needed.

### Synthesis And Open Boundaries

Conventional direct/matrix-free capacitance remains a baseline. The conditional budget, ordinary directed-cut planner, and priced mixed schedule form a testable representation-aware combination; a reservation-aware planner is a different objective, and both immediate pure specializations should be admitted. The surviving hypothesis is that the complete combination pays for preprocessing, passes and certified numerics on a specified source format and workload. Elementary packing, generic cut optimization and the sandwich inequality should not carry standalone novelty claims.

The earlier 2009/2019/2021 comparisons were not repeated. WDBMC 2025 Section 3 remains uninspected in the existing audit and was not obtained in this challenge. Its inaccessibility supports no absence or novelty claim. The newly inspected primary text establishes the close cut-objective match, not a stronger PageRank contraction theorem.

### New Exact Probe Receipt

| Targeted new risk | Result |
|---|---|
| Fractional budget versus count reservation | Exact counterexample confirmed |
| Pointwise cut gadget: 225 nonempty support pairs, eight labels, weights 0, 1/7, 2 | 5,400 identities passed, including overlaps and outside forcing |
| Four mixed boundary fixtures | 37 state checks passed; dimensions 8,8,9,4; surviving bases 3,3,4,1 |
| Original residual, weighted bound, nonnegative-state positivity and columnwise contraction | Passed, including one zero-mass coordinate |
| All-column specialization fixture | Five further state checks passed; declared interval 17 versus specialized bill 15 |
| New strict-family k=12 cardinality classes | L=70, OPT=72, U=92 |
| Factor-count tie versus stated mixed bill | 22,18,18,22 confirmed |

The retained rational probe imports no implementation, runs no flow solver, writes no files and does not establish numerical robustness, timing or RAM.

### Retained Anisotropic Boundary Probe

```python
from fractions import Fraction as F
from itertools import product

# Fractional candidates need integer rounding before using B=sum(b).
b = F(1, 2)
theta = 1 / (b + 1)
z = [F(3, 4), F(1, 4)]
assert theta == F(2, 3) and sum(v > theta for v in z) == 1 > b

# Pointwise auxiliary minimization, including outside forcing and zero prices.
universe, selected = set(range(4)), set(range(3))
supports = [{i for i in universe if mask >> i & 1} for mask in range(1, 16)]
gadget_checks = 0
for left, right, labels, weight in product(
        supports, supports, range(8), [F(0), F(1, 7), F(2)]):
    rows = {i for i in selected if labels >> i & 1}
    cols = selected - rows
    big = weight + 1
    values = []
    for alpha, beta in product([0, 1], repeat=2):
        forced = ((len(right & rows) + bool(right - selected)) * (1 - alpha)
                  + (len(left & cols) + bool(left - selected)) * beta)
        values.append(weight * alpha * (1 - beta) + big * forced)
    assert min(values) == weight * bool(left - rows) * bool(right - cols)
    gadget_checks += 1

def evaluate_exclusion_prefix_suffix(values):
    prefix, suffix = [F(0)], [F(0)] * (len(values) + 1)
    for value in values:
        prefix.append(prefix[-1] + value)
    for i in reversed(range(len(values))):
        suffix[i] = values[i] + suffix[i + 1]
    return [prefix[i] + suffix[i + 1] for i in range(len(values))]

def inspect_mixed_release_fixture(u, v, q, rows, cols):
    u, v, q = [[F(t) for t in row] for row in u], [[F(t) for t in row] for row in v], list(map(F, q))
    n, r, a = len(u), len(u[0]), F(17, 20)
    nodes, factors = range(n), range(r)
    rows, cols = sorted(rows), sorted(cols)
    selected = set(rows + cols)
    matrix = [[sum(u[i][f] * v[j][f] for f in factors) - (q[i] if i == j else 0)
               for j in nodes] for i in nodes]
    assert all(t >= 0 for row in matrix for t in row)
    degree = [sum(matrix[i][j] for i in nodes) for j in nodes]
    assert all(degree[j] > 0 for j in selected)
    invdegree = [1 / d if d else F(0) for d in degree]
    p = [F(i % 2 == 0 or i == n - 1) for i in nodes]
    total = sum(p)
    p = [t / total for t in p]
    bias = [(1 - a) * t for t in p]
    remaining_q = [F(0) if i in selected else q[i] for i in nodes]
    diagonal = [1 / (1 + a * remaining_q[i] * invdegree[i]) for i in nodes]
    bases = [f for f in factors if any(u[i][f] for i in nodes if i not in rows)
             and any(v[j][f] for j in nodes if j not in cols)]
    fcols = [[u[i][f] if i not in rows else F(0) for i in nodes] for f in bases]
    gcols = [[v[j][f] * invdegree[j] if j not in cols else F(0) for j in nodes] for f in bases]
    fcols += [[F(i == j) for i in nodes] for j in rows]
    gcols += [[matrix[i][j] * invdegree[j] for j in nodes] for i in rows]
    fcols += [[matrix[i][j] if i not in rows else F(0) for i in nodes] for j in cols]
    gcols += [[F(i == j) * invdegree[j] for i in nodes] for j in cols]
    fcols += [p]
    gcols += [[F(not d) for d in degree]]
    dimension = len(fcols)
    masses = list(map(sum, fcols))
    transition = [[matrix[i][j] * invdegree[j] + (p[i] if not degree[j] else 0)
                   for j in nodes] for i in nodes]
    beta = max([a] + [a * (degree[i] + remaining_q[i]) /
                         (degree[i] + a * remaining_q[i]) for i in nodes if degree[i]])
    for i, j in product(nodes, repeat=2):
        assert sum(fcols[t][i] * gcols[t][j] for t in range(dimension)) == (
            transition[i][j] + (remaining_q[i] * invdegree[i] if i == j else 0))

    def evaluate_three_phase_kernel(state):
        base = {f: state[t] for t, f in enumerate(bases)}
        hrow = {i: state[len(bases) + t] for t, i in enumerate(rows)}
        hcol = {i: state[len(bases) + len(rows) + t] for t, i in enumerate(cols)}
        w = [sum(v[j][f] * hcol[j] for j in cols) for f in factors]
        e = {i: matrix[i][i] * hcol[i] for i in cols}
        for f in factors:
            excluded = evaluate_exclusion_prefix_suffix([v[j][f] * hcol[j] for j in cols])
            for t, i in enumerate(cols):
                e[i] += u[i][f] * excluded[t]
        x = []
        for i in nodes:
            base_term = sum(u[i][f] * base.get(f, F(0)) for f in factors)
            if i in rows:
                value = bias[i] + a * (hrow[i] + p[i] * state[-1])
            elif i in cols:
                value = bias[i] + a * (base_term + e[i] + p[i] * state[-1])
            else:
                value = diagonal[i] * (bias[i] + a * (
                    base_term + sum(u[i][f] * w[f] for f in factors) + p[i] * state[-1]))
            x.append(value)
        y = [x[i] * invdegree[i] for i in nodes]
        off = [sum(v[j][f] * y[j] for j in nodes if j not in rows) for f in factors]
        nextrow = {i: matrix[i][i] * y[i] for i in rows}
        for f in factors:
            excluded = evaluate_exclusion_prefix_suffix([v[j][f] * y[j] for j in rows])
            for t, i in enumerate(rows):
                nextrow[i] += u[i][f] * (off[f] + excluded[t])
        new = [sum(v[j][f] * y[j] for j in nodes if j not in cols) for f in bases]
        new += [nextrow[i] for i in rows] + [y[j] for j in cols]
        new += [sum(x[j] for j in nodes if not degree[j])]
        return x, new

    zero = [F(0)] * dimension
    _, intercept = evaluate_three_phase_kernel(zero)
    states = [zero] + [[F(t == j) for t in range(dimension)] for j in range(dimension)]
    states += [[F((-1) ** t * (t + 1), t + 2) for t in range(dimension)]]
    for state in states:
        x, new = evaluate_three_phase_kernel(state)
        expected_x = [diagonal[i] * (bias[i] + a * sum(
            fcols[t][i] * state[t] for t in range(dimension))) for i in nodes]
        expected_new = [sum(gcols[t][i] * expected_x[i] for i in nodes) for t in range(dimension)]
        assert x == expected_x and new == expected_new
        delta = [new[t] - state[t] for t in range(dimension)]
        residual = [bias[i] + a * sum(transition[i][j] * x[j] for j in nodes) - x[i] for i in nodes]
        assert residual == [a * sum(fcols[t][i] * delta[t] for t in range(dimension)) for i in nodes]
        assert sum(map(abs, residual)) <= a * sum(masses[t] * abs(delta[t]) for t in range(dimension))
        if all(t >= 0 for t in state):
            assert all(t >= 0 for t in x + new)
    for j in range(dimension):
        _, image = evaluate_three_phase_kernel(states[j + 1])
        assert sum(masses[t] * (image[t] - intercept[t]) for t in range(dimension)) <= beta * masses[j]
    return len(states), dimension, len(bases), sum(t == 0 for t in masses)

u = [[2,1,0,0,3],[0,2,1,0,0],[1,0,0,0,0],[0,3,0,2,1],[0,1,2,0,0],[0,4,0,1,0]]
v = [[0,0,2,0,0],[1,2,0,0,1],[0,0,0,3,2],[2,1,0,0,0],[3,0,1,1,0],[0,0,0,0,0]]
q = [sum(F(u[i][f] * v[i][f], 2) for f in range(5)) for i in range(6)]
fixtures = [(u, v, q, {0,2}, {1,3}), (u, v, q, {0}, {1,2,3}), (u, v, q, {0,1,2}, {3}),
            ([[1,1],[0,1],[0,0]], [[0,1],[0,1],[1,1]], [1,1,0], {0}, {1})]
receipts = [inspect_mixed_release_fixture(*fixture) for fixture in fixtures]

# All stars nonempty; factor-optimal cuts can still lose on the scalar bill.
bills = [2*1 + 2*1 + 3*2 + 4*max(len(rows), 2-len(rows)) + 4
         for rows in [set(), {0}, {1}, {0,1}]]
assert bills == [22,18,18,22] and bills[0] + bills[3] > bills[1] + bills[2]
print(dict(fractional_budget_counterexample=True, pointwise_gadget_checks=gadget_checks,
           mixed_fixture_receipts=receipts, mixed_state_checks=sum(t[0] for t in receipts),
           factor_optimal_scalar_bills=bills))

# Newly requested frontier: a free all-column specialization and a new k=12 class oracle.
column_fixture = ([[1,1],[1,0],[0,1]], [[1,0],[0,1],[0,0]], [1,0,0], set(), {0})
column_receipt = inspect_mixed_release_fixture(*column_fixture)
assert column_receipt[:3] == (5,3,1)
cu, cv = column_fixture[:2]
candidate_bases = [sum(any(cu[i][f] for i in range(3) if i not in rows)
                       and any(cv[j][f] for j in range(3) if j not in ({0}-rows))
                       for f in range(2)) for rows in [set(), {0}]]
assert candidate_bases == [1,2]
mixed_bills = [2*bases+4+3+4+4 for bases in candidate_bases]
row_bill = 2*candidate_bases[1]+2+7+4
column_bill = 2*candidate_bases[0]+2+7+4
lower = min(2*min(candidate_bases)+4+3+4+4, row_bill)
cut_bill = min(bill for bases,bill in zip(candidate_bases,mixed_bills)
               if bases == min(candidate_bases))
column_model = dict(L=lower, OPT=min(mixed_bills+[row_bill]),
                    U=min(cut_bill,row_bill), specialized_column=column_bill)
assert column_model == dict(L=17,OPT=17,U=17,specialized_column=15)
assert column_model["specialized_column"] < column_model["L"]

k, r = 12, 2
u = [[1,1] for _ in range(k)] + [[1,0]]
jset = set(range(k))
plans = []
for count in range(k+1):
    rows = set(range(count))
    cols = jset - rows
    r0 = sum(any(u[i][f] for i in range(k+1) if i not in rows)
             and any(u[i][f] for i in range(k+1) if i not in cols) for f in range(r))
    assert r0 == (1 if count in (0,k) else 2)
    plans.append((count, r0, 2*r0+2*r+3*k+4*max(count,k-count)+4))
lower, rowbill = 2+2*r+3*k+4*((k+1)//2)+4, 2+r+7*k+4
upper = min(rowbill, max(bill for _, r0, bill in plans if r0 == 1))
optimum = min(rowbill, min(bill for _, _, bill in plans))
assert (lower,optimum,upper) == (70,72,92)
assert F(2,2*k-1) > F(1,k)
assert upper-optimum == 2*k-4
assert F(upper,optimum) < F(7,5)
# Universal sweep exclusion: for lambda>=0 compare to empty, else compare to full.
for count, r0, _ in plans[1:-1]:
    assert r0-1 == 1 and count > 0 and k-count > 0
print(dict(column_specialization_receipt=column_receipt, column_portfolio_counterexample=column_model,
           new_strict_family=(k,lower,optimum,upper), asymptotic_ratio="7/5"))
```

## Independent Factor-Conditioned Planner Check (2026-09-20)

Bounded follow-up to [PageRank-Factor-Conditioned-Planner.md](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/PageRank-Factor-Conditioned-Planner.md), inspected SHA256 `4e1cb61781e349685eb06590fbd02e9e952d451277fff93b895d2bd64e92dedb`. No lead probe or old suite was rerun, and the lead's local Python compatibility issue was not modified.

**Finding: no counterexample to the exact envelope or clamp corollary under the declared support/reservation model.** Two integration tightenings remain: include the immediately available B_col candidate from the preceding review when naming the executable portfolio, and retain the full `O(gH+g+H)` word bound with paid metadata, not just `O(g*k/w)`. In particular, g=0 does not justify erasing H, the retained orientation/ID requirements, or reconstruction work. These are scope/accounting tightenings, not failures of the document's explicitly stated theorem.

**Independent proof check.** A consistent branch forces P into R and N into C and kills d distinct factors. Each integer cardinality in [|P|,k-|N|] is attained by choosing the requisite number of free vertices. Every completion has r0<=r-d, so F(ell) is at most every branch charge covering ell. Conversely, any orientation supplies one legal mode for each factor it kills and an unconstrained mode for each survivor. Its branch is consistent and has charge exactly r0. Taking an optimal orientation proves the reverse inequality. Simultaneous row/column killing of one factor needs only one witness, never two credits.

This also verifies the essential `*` semantics. A free factor may disappear accidentally: the resulting slack is harmless. Treating `*` as a survival constraint would invalidate the interval, while excluding it would lose optima. Exterior memberships must prohibit the corresponding kill mode; masks truncated to J without exterior flags are insufficient.

For a branch, clamping floor(k/2) into its interval minimizes the mixed cardinality term, including odd k. Its charge upper-bounds an executable completion. A branch induced by an optimal orientation has exact factor charge and a clamped cardinality term no greater than that orientation's. Hence the minimum branch bill equals the mixed optimum, and any completion of a globally winning branch at its chosen cardinality attains that bill. Comparing against the known specialized-row bill is exact; additionally comparing against the known B_col bill is equally exact. Optional extra selected vertices, discarded empty stars, different source encodings and orientation-dependent precision remain outside this proof.

**Execution and admission.** The stated streamed enumeration and one retained branch witness support the proposed O(3^g) dependence without retaining exponentially many branches. A witness/frontier tag needs O(g) bits, as the note already warns. The single-plan time bound assumes the declared word operations and reconstructing the winner once; the retained lead probe reconstructs on incumbent improvements and is not itself a packed implementation of that bound. Support validation, exterior-flag discovery, source ordering/passes, metadata and output construction must be charged. Stopping early supplies an upper bound only. This check neither benchmarks planning nor proves source-scoped physical RAM or standalone novelty; the note's own generic witness-branching comparator already matches its combinatorial procedure.

**New exact receipt.** A separate set-based probe, with no `int.bit_count` and no implementation imports, checked four small support fixtures and all ten associated cardinalities. It includes k=0, g=0, exterior supports, overlapping incompatible forces, and a factor with both killing modes satisfied. Results: 31 consistent branches, 26 rejected conflicts, 44 accidental-kill completions, five simultaneous-mode kills; envelope, winning reconstruction and both pure-specialization comparisons passed. These are support-level tests, not stationary-vector or runtime-memory tests.

### Retained Factor-Conditioned Boundary Probe

```python
from itertools import product

def count_active_support_factors(supports, rows, selected):
    cols = selected - rows
    return sum(not left <= rows and not right <= cols for left, right in supports)

fixtures = [
    (0, []),
    (0, [({0}, {0})]),
    (3, [(set(range(4)), set(range(4)))]),
    (3, [({0}, {1}), ({1}, {0}), ({2,3}, {0,2}),
         ({0,3}, {1,3}), ({0,1}, {2}), (set(range(4)), set(range(4)))])
]
cardinality_checks = consistent = conflicts = slack = dual_kills = 0
for k, supports in fixtures:
    selected, r = set(range(k)), len(supports)
    modes = []
    for left, right in supports:
        choices = [(set(), set(), 0)]
        if left <= selected:
            choices.append((left, set(), 1))
        if right <= selected:
            choices.append((set(), right, 1))
        modes.append(choices)
    envelope = [r+1]*(k+1)
    best_charge, best_rows = None, None
    for choices in product(*modes):
        positive, negative, killed = set(), set(), 0
        for p, n, d in choices:
            positive |= p
            negative |= n
            killed += d
        if positive & negative:
            conflicts += 1
            continue
        consistent += 1
        low, high, charge = len(positive), k-len(negative), r-killed
        for ell in range(low, high+1):
            rows = positive | set(sorted(selected-positive-negative)[:ell-low])
            actual = count_active_support_factors(supports, rows, selected)
            assert actual <= charge
            slack += actual < charge
            envelope[ell] = min(envelope[ell], charge)
        ell = min(high, max(low, k//2))
        bill = 2*charge+2*r+3*k+4*max(ell,k-ell)+4
        if best_charge is None or bill < best_charge:
            best_charge = bill
            best_rows = positive | set(sorted(selected-positive-negative)[:ell-low])
    oracle, mixed_optimum = [r+1]*(k+1), None
    for labels in product([False, True], repeat=k):
        rows = {j for j in selected if labels[j]}
        actual = count_active_support_factors(supports, rows, selected)
        oracle[len(rows)] = min(oracle[len(rows)], actual)
        dual_kills += sum(left <= rows and right <= selected-rows for left,right in supports)
        bill = 2*actual+2*r+3*k+4*max(len(rows),k-len(rows))+4
        mixed_optimum = bill if mixed_optimum is None else min(mixed_optimum,bill)
    assert envelope == oracle and best_charge == mixed_optimum
    assert best_charge == (2*count_active_support_factors(supports,best_rows,selected)
                           +2*r+3*k+4*max(len(best_rows),k-len(best_rows))+4)
    pure_row = 2*count_active_support_factors(supports,selected,selected)+r+7*k+4
    pure_col = 2*count_active_support_factors(supports,set(),selected)+r+7*k+4
    assert min(best_charge,pure_row,pure_col) == min(mixed_optimum,pure_row,pure_col)
    cardinality_checks += k+1
assert conflicts and slack and dual_kills
print(dict(factor_fixtures=len(fixtures), cardinality_checks=cardinality_checks,
           consistent_branches=consistent, conflicting_branches=conflicts,
           accidental_kill_completions=slack, simultaneous_mode_kills=dual_kills,
           g_zero_and_k_zero=True))
```
