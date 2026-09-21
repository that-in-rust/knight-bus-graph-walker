# Factor-Defect PageRank: Bounded Prior-Art Comparison

Date: 2026-09-20. Research only; no implementation, numerical probes, or commits. Sole output: this file. Inputs read: [Factor-Defect-PageRank.md](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Factor-Defect-PageRank.md) and [PageRank-Independent-Review.md](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/PageRank-Independent-Review.md). The lead's subsequent row/column-release proposal is included below; concurrent numerical/representation fixes are not independently tested here.

## Premise Check

**The basic reduced solve is established algebra.** Investigate a useful, certified representation/execution tradeoff instead. WDBMC Section 3 remains unresolved; inaccessibility is not novelty evidence.

## Expert Lenses

Lenses: numerical linear algebra, representation/compiler design, external-memory execution, and skeptical attribution. Distinguish an available algebraic specialization from an explicitly published algorithm and a measured benefit.

## Candidate Approaches

1. **New elimination method:** reject this positioning; the capacitance mapping below is exact.
2. **Restricted representation theorem:** investigate fixed-factor completion and contraction-budgeted release, with narrowly stated optimality.
3. **Bounded-memory execution result:** compare complete jobs with incumbents given the same factors, including preparation and output.
4. **Certified plan selection:** investigate choosing representation, solver and residual evaluation jointly, rather than minimizing factor count alone.

## Chosen Thesis

Investigate **restricted representation policies using established elimination**, especially whether replacing cancellation-heavy factored rows/columns with exact stars meets a requested contraction bound at acceptable physical cost. This is a hypothesis, not a priority claim.

## Evidence And Verification

### Access Ledger

Access attempts were ordinary public retrievals; no paywall circumvention, credentials, author messages, or manuscript requests were used.

| Primary work | Material actually inspected and precise limits |
| --- | --- |
| **2025 WDBMC**, DOI `10.1016/j.cam.2024.116332` | [Publisher article](https://www.sciencedirect.com/science/article/pii/S0377042724005806) returned HTTP 403 through the web tool; the [DOI resolver](https://doi.org/10.1016/j.cam.2024.116332) was inaccessible. Only indexed [publisher introduction excerpts](https://www.sciencedirect.com/science/article/abs/pii/S0377042724005806) were readable: they locate reordering, multicompression, preconditioning and analysis in Section 3. The [author-institution record](https://bia.unibz.it/esploro/outputs/journalArticle/Weak-dangling-block-reordering-and-multi-step/991006942025301241) supplies metadata/abstract and exactly one file/link entry, the DOI, not a manuscript. It identifies JCAM 458 (2025), 17 pages. **No Section 3 page, equation, algorithm or proof was obtained.** |
| **2009 Karande, Chellapilla, Andersen** | [Journal PDF](https://www.internetmathematicsjournal.com/article/1489-speeding-up-algorithms-on-compressed-web-graphs/attachment/4433.pdf): web fetch timed out, but ordinary Python HTTPS retrieval returned 200, 315,518 bytes, 26 pages; parsed in memory, no saved copy. Inspected printed pp. 376-389, especially Proposition 3.1, Algorithms 2-3, Theorems 4.1/4.3, and equations (4.1)-(4.4). PDF page = printed page minus 372. |
| **2019 ODLR, Shen et al.** | [Repository publisher PDF](https://pure.rug.nl/ws/portalfiles/portal/103307690/1_s2.0_S0377042718304357_main.pdf): inspected pp. 458-462, equations (7)-(11), (14)-(16), Lemma/Theorem 5.1, Algorithms 1-2; p. 465, Section 6.1. Printed p. 461 = PDF page 7, including repository cover. |
| **2021 multistep, Shen and Carpentieri** | [Publisher PDF](https://journals.sagepub.com/doi/pdf/10.3233/FAIA210212), eight pages: indexed full text available. Inspected Section 2, equation (7)/compression objective on printed p. 400, factor cap on p. 401, Algorithm 1 on p. 403, and conclusion on p. 404. PDF pages 4, 5, 7, 8 respectively. Screenshot retrieval timed out; page/equation evidence here is extracted text. |
| **Gleich, PageRank Beyond the Web** | [Author preprint](https://arxiv.org/pdf/1407.5107), Section 2, PDF p. 3, Aside 2.3: original residual bounds original L1 error by division by `1-a`. PDF p. 4, Theorem 2.5: normalized pseudo-PageRank equivalence. |

Additional WDBMC discovery checks: exact-title/DOI and manuscript/PDF searches, including arXiv and author-profile searches, yielded no accessible manuscript. The [ResearchGate author-associated record](https://www.researchgate.net/publication/385259883_Weak_dangling_block_reordering_and_multi-step_block_compression_for_efficiently_computing_and_updating_PageRank_solutions) explicitly reports no full text and offers an author request. The public [Elsevier API URL](https://api.elsevier.com/content/article/doi/10.1016/j.cam.2024.116332?httpAccept=text/xml) and [Crossref record URL](https://api.crossref.org/works/10.1016%2Fj.cam.2024.116332) were inaccessible through the web tool; those failures do not establish an entitlement/paywall status. Stop here for this bounded task. A lawful author manuscript or library-supplied copy is still needed.

### Algorithm-Level Comparison

| Work | Established mechanism versus the candidate |
| --- | --- |
| **2009** | Sequential compressed multiplication already exists (p. 379, Proposition 3.1; p. 382, Algorithm 2). Separately, Algorithm 3 computes stationary state on **real plus virtual nodes**, then projects and normalizes (p. 386); Theorem 4.1 proves equivalence. Thus it is not merely an edge-storage baseline, but its displayed solver does not eliminate original-node state. Its jump probability is `1-a` in the candidate's notation. [Primary PDF](https://www.internetmathematicsjournal.com/article/1489-speeding-up-algorithms-on-compressed-web-graphs/attachment/4433.pdf#page=14). |
| **2019 ODLR** | Core-hub partitioning gives `A_lit=D+FH`, with a capacitance system, implicit inverse application, original-dimensional D solves and an outer iteration, not an entity-free row scan. [Equations (9), (14)-(15), Algorithms 1-2](https://pure.rug.nl/ws/portalfiles/portal/103307690/1_s2.0_S0377042718304357_main.pdf#page=7). |
| **2021 multistep** | Repeated rank-one extraction constructs `A_lit=D+FH`, optimizing stored nonzero reduction while limiting factor count because it determines dense capacitance dimension. Factor/state-cost awareness therefore predates this proposal. The residual core is not required to be diagonal; Algorithm 1 is preprocessing, not the proposed streamed stationary solver. [Section 2 and Algorithm 1](https://journals.sagepub.com/doi/pdf/10.3233/FAIA210212#page=4). |
| **2025 WDBMC** | Directly relevant by its disclosed scope. Neither equivalence nor absence of mixed-star completion, cancellation-budgeted release, factor-only iteration, or its certificate can be established from the accessible material. Do not fill these cells with inferred Section 3 details. [Publisher introduction](https://www.sciencedirect.com/science/article/abs/pii/S0377042724005806). |

### Construct Audit

Here `A` is adjacency, not the papers' PageRank coefficient matrix. Using the candidate's definitions, set `K=I+a Q Dinv`, `b=(1-a)p`, and append the dangling channel to obtain `Ubar,Vbar`.

```text
I-aP = K-a Ubar Vbar^T

F_lit = -a Ubar,  H_lit = Vbar^T,  D_lit = K
C_lit = I+H_lit D_lit^-1 F_lit
      = I-a Vbar^T K^-1 Ubar

C_lit h = Vbar^T K^-1 b
x = K^-1(b+a Ubar h)
```

This is our specialization of [ODLR equation (14)](https://pure.rug.nl/ws/portalfiles/portal/103307690/1_s2.0_S0377042718304357_main.pdf#page=7), not its implemented algorithm.

| Construct | Status |
| --- | --- |
| Schur/capacitance elimination | Known; the mapping is exact. |
| An `r`-state solve | Already follows from low-rank elimination with an invertible core. Here the generic count is `r+1` including dangling mass, not a universal minimum. Avoiding both N-state intermediates and a dense reduced matrix is an execution constraint, not a new inverse identity. |
| Diagonal cancellation | Absorbing `Q` into the core is routine rearrangement. Arbitrary supplied factors with removable excess diagonal mass are not explicit in the inspected algorithms. That scope distinction does not establish novelty. |
| Choosing factors | Already central to the cited preprocessing methods. Mixed-star completion may target a different feasible core; minimizing a cover of fixed E optimizes only that restricted factor class, not arbitrary nonnegative rank, memory, or solve time. |
| Original residual certificate | The resolvent error bound is known. The following factor identity is also a direct elimination consequence; potential benefit is its validated, bounded-state evaluation. |

For any h, not only a solver trajectory, write `X(h)=K^-1(b+a Ubar h)` and `delta=Vbar^T X(h)-h`. Then

```text
b+a P X(h)-X(h) = a Ubar delta
||X(h)-x*||_1 <= a/(1-a) * sum_j c_j |delta_j|
c_j = sum_i Ubar[i,j]
```

The original residual contains **no left multiplier K^-1**. Its error conversion is the [Aside 2.3 bound](https://arxiv.org/pdf/1407.5107#page=3). The weighted triangle bound can lose cancellations; an additional streamed evaluation of `||a Ubar delta||_1` is a distinct cost option. Neither exact identity certifies a rounded, misclassified source operator.

### New Contraction-Budgeted Release

**Answer to the follow-up:** not explicit in the inspected ODLR Algorithms 1-2 or multistep Section 2/Algorithm 1; **unknown for WDBMC Section 3**. ODLR uses structural/density selection; multistep uses compression gain and a factor cap, not the supplied `q_i/d_i` contraction threshold. [ODLR pp. 460-461](https://pure.rug.nl/ws/portalfiles/portal/103307690/1_s2.0_S0377042718304357_main.pdf#page=6), [multistep p. 401](https://journals.sagepub.com/doi/pdf/10.3233/FAIA210212#page=5).

The following is an algebraic inspection of the **lead's proposal**, not an implementation or new empirical result. Let `M_R` and `M_C` be diagonal masks zeroing selected vertices R and C, with R and C disjoint. Strip U on R and V on C, and set `Q'=M_R Q M_C`. The restoration identity is

```text
A = M_R A M_C + (I-M_R) A + M_R A (I-M_C)
  = (M_R U)(M_C V)^T - Q'
    + selected original row stars
    + selected original column stars restricted outside R.
```

The restored terms are nonnegative and disjoint in matrix support. At most `r+|R|+|C|` factor columns suffice, before removing empty factors; this is not a minimal-rank theorem. A, its degrees and its PageRank solution stay fixed, while q becomes zero on R union C.

For `0<a<=gamma<1`, the scalar function `f(t)=a(1+t)/(1+a*t)` is strictly increasing. Thus retaining the candidate's **specific bound** `beta=max_i f(q_i/d_i)<=gamma` requires and suffices to release every active vertex in

```text
S_gamma = { i : d_i>0 and q_i/d_i > (gamma-a)/(a*(1-gamma)) }.
```

Unselected vertices keep their original q/d; selected ones have `f(0)=a`. Equality at the threshold is permitted. At `gamma=a`, every active positive-q vertex is mandatory; at `a=0`, return p and do not evaluate the threshold. Dangling vertices use t=0, not q/d.

This necessity is **within the stated stripping family for this conservative beta certificate**, not for actual spectral radius, the sharp induced norm, fast observed convergence, or every refactorization. Fixing gamma below one removes the fixed-A example's unbounded contraction-factor deterioration, but does not bound total build/output cost or promise uniform wall time. Explicit rows/columns can expand dramatically. Select orientation using actual encoding/build costs, including overlaps and required source access; independent per-vertex prices need not capture those interactions.

## Final Synthesis

Three narrow hypotheses remain worth testing, subject to the unresolved nearest-art gate:

| Hypothesis | Falsifiable evaluation, for the lead's subsequent work |
| --- | --- |
| **H1: Contraction-budgeted representation admission** | On fixed-A, varying-Q families, release exactly the mandatory set, price orientation and expanded rows, and compare with Q-free refactorization, Jacobi and a small direct core. Benefit fails if ordinary alternatives meet the same error/resource target more cheaply. The threshold alone is elementary algebra, not a substantial novelty claim. |
| **H2: State-oriented exact completion** | On nontrivial regular-plus-defect graphs, allow negative edge-compression gain and test whether mixed-star completion enables useful jobs under a fixed RAM/live-disk cap. Give generic matrix-free capacitance solvers identical factors; include source-only/destination-only, ordinary out-of-core PageRank and specialized-star controls. Losses after compiler/output costs falsify the claimed advantage. |
| **H3: Joint certificate/execution selection** | Compare factor triangle bounds with occasional streamed original residuals at identical returned-vector error. Charge precision, compensation arrays and scans. Test whether an admission/selection policy improves completed-job cost without weakening source-operator certification. No such measured improvement is established here. |

The new release rule is a more precise research target than "low-rank PageRank" or "factor-count awareness." It remains a combination of familiar masks, stars and contraction algebra whose distinctness and practical value must be demonstrated.

## Open Questions

- Obtain lawful WDBMC Section 3 and compare its selection rules, diagonal/core treatment, state dimensions, contraction controls and stopping equations directly. Until then, its cells remain unknown.
- Does release plus a fused row schedule improve anything once incumbents receive the same representation and can select a tiny dense core when appropriate?
- Can the lead's numerical admission and physical compiler preserve the intended operator within the advertised resource budget? This side task did not rerun or duplicate those probes.

Changed path: `research_algorithms_20260920/PageRank-Prior-Art-Comparison.md` only. No commits.
