# Anisotropic Budgets And Priced PageRank Release

Date: 2026-09-20. Bounded A01 follow-on. Sole owned output: this file. No lead papers, previous reviews, production files, or A02 artifacts were edited; no commits. Inputs: the current [staircase](PageRank-Release-Staircase.md), [positive row kernel](PageRank-Positive-Release-Kernel.md), and [prior-art comparison](PageRank-Prior-Art-Comparison.md). Only new probes were run.

## Premise Check

**A conditional strengthening survives, with a useful negative result.**

1. Nonuniform factor budgets give a proved mixture-dependent candidate/state/contraction frontier, valid for every admitted Q on the same supplied U,V. They can strictly improve the uniform staircase's *guaranteed cap*, without enumerating the heavy set first.
2. For a fixed selected set, a directed minimum cut exactly optimizes surviving original factors plus additive orientation prices. Under an explicit nonempty-star condition this also optimizes actual factor count within the supplied-factor masking/star family.
3. A positive three-phase mixed-orientation evaluator gives the latter plan a concrete scalar and pass reservation. It does not inherit the row-only kernel's reservation for free.
4. Neither method dominates an unrestricted exact subset/orientation optimizer. In particular, arbitrary empty-star removal makes the actual orientation objective non-submodular in an explicit example. The cut theorem must not be generalized to that objective.

These are derived specializations, not a claim of publication priority. Expert lenses used: nonnegative matrix geometry, combinatorial optimization, numerical certification, and external-memory execution. The conventional low-rank direct/Krylov solver with the same factors remains a required baseline.

## Scope And Notation

Keep the original admitted factorization, with destination rows:

```text
A = U V^T - Q >= 0;  U,V >= 0;  Q = diag(q) >= 0;
d_j = sum_i A_ij;  0<a<1;  p>=0;  sum_i p_i=1.
S_f = sum_i U_if > 0;  s_j = d_j+q_j.
z_jf = U_jf/S_f;
pi_jf = S_f V_jf/s_j,  when s_j>0.
```

Remove empty-sided supplied factors. For each positive-s column, pi is a probability vector; each z column is a probability vector. Release still means the established exact masking plus disjoint original-row/original-column stars. We do not change A, d, personalization, or the original-space residual certificate.

Throughout, factor counts exclude the extra dangling coordinate. Numerical enclosures, IDs, maps, indices, and runtime bytes are charged separately from scalar slots.

## Theorem 1: Nonuniform Budget Frontier

Choose integers `b_f>=0`, and define

```text
theta_f = 1/(b_f+1)
B       = sum_f b_f
tau_b   = max_(j:s_j>0) sum_f pi_jf theta_f.
```

Require `tau_b<1`. For a requested `tau>=tau_b`, `tau<1`, select exactly

```text
J_tau = {j: d_j>0 and q_j > tau*s_j}.
```

Emit candidate records (j,f) only if `z_jf>theta_f`, deduplicate, and apply the exact q/d filter. Then

```text
candidate records <= B
|J_tau|           <= B
released factors <= r+B
beta_new         <= a/[1-(1-a)*tau].
```

For row-only release, the existing positive kernel reserves at most `3r+7B+4` scalar slots; the tighter expression remains `2r0+r+7k+4`.

**Proof.** If j has no witness, then every `z_jf<=theta_f`. Thus
`q_j/s_j <= W_jj/s_j = sum_f pi_jf z_jf <= tau_b <= tau`, contradicting selection. One normalized U column has at most b_f entries strictly above `1/(b_f+1)`; sum these record bounds, before deduplication. Outside J_tau, `q/d<=tau/(1-tau)`; substitute in the existing phi bound. Release preserves degrees, and the old factor/state and residual proofs then apply unchanged.

A zero budget is legal: theta_f=1 emits no witnesses from that factor. It is safe only because the *pointwise mixture envelope* covers that factor's possible contribution. It does not delete the factor, change its coefficients, or ignore it during reconstruction.

This statement is stronger than a measured count of J_tau: its reservation depends on U-column normalization and the V-side mixture envelope, and holds for **all** admissible Q on those supplied factors. No selected-subset enumeration is used to obtain B. It improves an a priori admission bound; the actual mandatory set at a fixed tau remains the same set an exact optimizer would enumerate.

### Structural Corollary

Suppose a designated set G of g factors carries at least `1-eta` of the mixture at every positive-s column:

```text
sum_(f outside G) pi_jf <= eta < 1.
```

Give each factor in G budget b>=1 and every other factor budget zero. Then

```text
B = b*g
tau_b <= eta + (1-eta)/(b+1)
gamma_b <= a/[1-(1-a)*(eta+(1-eta)/(b+1))].
```

The proof uses only the certified uniform envelope eta, not the observed heavy count. This is useful when many supplied factors have small V-side mixture shares everywhere. It is not valid merely because their average shares are small.

For g=1, eta=1/10, r=3, a=17/20:

| Budget policy | Candidate cap | Factor cap | Contraction cap | Row-kernel scalar cap |
|---|---:|---:|---:|---:|
| Uniform (1,1,1) | 3 | 6 | 34/37 | 34 |
| Anisotropic (2,0,0) | 2 | 5 | 85/94 | 27 |
| Uniform (2,2,2) | 6 | 9 | 17/19 | 55 |
| Anisotropic (3,0,0) | 3 | 6 | 680/761 | 34 |

Both anisotropic rows improve the corresponding displayed uniform row in guaranteed state and contraction. These are conditional caps, not comparisons against the exact realized factor count of the uniform plan.

### Strict Witness Without Proportional Factors

Use these exact normalized U columns and V mixture rows:

```text
U = [[49/100, 9/10,  1/100],
     [49/100, 1/100, 9/10 ],
     [ 1/150, 2/100, 1/100],
     [ 1/150, 2/100, 3/100],
     [ 1/150, 5/100, 5/100]]

V = [[90/100, 9/100, 1/100],
     [90/100, 1/100, 9/100],
     [95/100, 4/100, 1/100],
     [94/100, 2/100, 4/100],
     [96/100, 3/100, 1/100]]

Q = diag(UV^T).
```

Every S_f and s_j equals one. Budgets (2,0,0) give tau_b=2/5, exactly two candidates and J={0,1}. All three original factors survive row masking, so the released factor count is five.

This is not redundant rank-one splitting: rank(W)=3. After release `W'=A+Q_outside = W-diag(q_0,q_1,0,0,0)` has rank five. For a proof certificate, let O={2,3,4}. The determinants of U[O,:] and V[O,:] are respectively -1/250000 and 3/10000. Thus W[O,O] is nonsingular and W's Schur complement on J is zero. Subtracting the two positive diagonal values q_0=q_1=5221/10000 makes that Schur complement nonsingular. Consequently **any factorization preserving this particular post-release diagonal** needs at least five factors, and the construction attains it.

This lower bound does not forbid another Q or another solver from using less state. The unreleased representation already has three factors; it simply has a different convergence certificate.

### Admission And Negative Results

The envelope can be checked without storing pi: a rowwise scan validates

```text
sum_f S_f V_jf/(b_f+1) <= tau * sum_f S_f V_jf.
```

Store factor totals and budgets, use a scalar row accumulator, and emit canonical U witnesses in a subsequent scan/order. Price completed totals, factor arrays, joins, selected-index construction, and exact comparisons. Strict-threshold/ceiling ambiguity and duplicate coalescing have the same obligations as the staircase. A proposed budget is not admitted until every required row inequality is certified.

A potential budget planner is the continuous relaxation

```text
minimize sum_f x_f
subject to x_f>=0,
           sum_f pi_jf/(x_f+1) <= tau for every positive-s j.
```

These constraints are convex. Rounding a feasible x upward coordinatewise preserves feasibility and adds fewer than r to the total budget. The relaxed optimum is a lower bound on the integer-budget optimum, so an exactly solved relaxation plus this rounding has an additive-<r guarantee. This is a proposed planning option, not an implemented optimizer or a fixed-pass algorithm. A bounded implementation must price its iterations and repeated separation scans. Supplied rational relaxation candidates must be rounded upward to integer b before invoking the stated sum(b) count reservation; alternatively reserve sum(ceil(b_f)) while retaining the fractional thresholds. Certifying the mixture envelope alone does not make fractional counts valid. Independent review's r=1, b=1/2, U=(3/4,1/4), V=(1,1), q=(3/4,1/4) gives one witness/heavy vertex despite B=1/2, exposing that former wording error without refuting the integer theorem.

Two limitations prevent overclaiming:

- If each factor has a column whose mixture is the corresponding unit vector, feasibility forces every `b_f>=m` for tau=1/(m+1). Uniform mr is optimal in this budget family. The old sharp block families therefore still prevent an unconditional improved staircase.
- Average mixture weights are unsafe. Ten columns supported only on factor 0 and one only on factor 1 give a low average value for theta=(1/3,1), yet the last column's envelope is one. Set its q/s=9/10: it is heavy at tau=1/2 and has no witness. The exact pointwise check rejects this false plan.

Changing witness budgets cannot release fewer mandatory vertices while retaining the same maximum-q/d beta certificate. A more permissive induced-norm certificate is a different admission rule, already distinguished in the earlier review; it is not silently substituted here.

## Theorem 2: Fixed-Set Orientation By Minimum Cut

Fix an admitted selected set J, k=|J|. Assign each j in J either row release R or column release C. All vertices outside J remain unselected. Let

```text
L_f = {i: U_if>0}
K_f = {j: V_jf>0}
survive_f(R,C) = 1[L_f is not a subset of R AND K_f is not a subset of C].
```

For nonnegative supplied prices w_f, rho_j, chi_j, minimize

```text
E(R,C) = sum_f w_f*survive_f(R,C)
         + sum_(j in R) rho_j + sum_(j in C) chi_j.
```

This is exactly a directed minimum-cut problem, not orientation enumeration.

**Construction.** Source-side selected vertices mean R, sink-side mean C. For each factor introduce nodes alpha_f,beta_f and edge `alpha_f -> beta_f` of capacity w_f. Add infinite-capacity edges:

- `j -> alpha_f` for j in K_f intersect J; force alpha_f to source if K_f has an outside-J member.
- `beta_f -> i` for i in L_f intersect J; force beta_f to sink if L_f has an outside-J member.

Add `j -> sink` of capacity rho_j and `source -> j` of capacity chi_j. Replace infinity by a checked finite capacity larger than the sum of all finite prices.

**Proof.** A remaining V membership forces alpha_f source-side; a remaining U membership forces beta_f sink-side. Exactly when both occur, the w_f edge must cross. Otherwise auxiliary labels can avoid that edge. Unary edges charge the chosen orientations. Minimizing over auxiliaries and selected labels therefore gives exactly min E.

The graph has k+2r+2 nodes and O(k+r+Z_J) arcs, where Z_J is selected U/V incidence. A standard flow solver gives a polynomial algorithm. Its graph and residual-network storage are *build/planning* costs, not O(r+k) query scratch. Z_J can be large even when candidate witnesses are few; reject or fall back to an admitted pure orientation when the planner does not fit.

### When This Is Actual Factor Optimality

With unit prices, E equals the actual factor count in the masking-plus-one-star-per-selected-vertex family if every possible restored star is nonempty. A sufficient, checkable condition is:

```text
A[i,:] is nonzero for every i in J;
A[outside J,j] is nonzero for every j in J.
```

Then every orientation contributes exactly k nonempty stars, and only surviving base factors vary. The cut exactly matches the fixed-J exhaustive orientation optimizer. It does not optimize optional extra selected vertices, factor merging/refactorization, nonlinear encoded costs, or arbitrary source-native compression.

Without that condition, E with unit star prices is a valid reserved-slot upper bound, not the exact nonempty-factor objective. Real additive build prices or certified additive upper bills can also be used, but interactions must not be represented as independent prices without justification. The full mixed-kernel scalar bill below includes max(|R|,|C|), so minimizing E alone does not minimize every physical reservation.

### Strict Orientation Improvement

```text
U = [[1,0,0], [0,10,1], [0,1,1]]
V = [[10,0,1], [0,1,0], [1,0,1]]
q = (10,10,0)

A = [[0,0,1], [1,0,1], [1,1,1]]
d = (2,1,3),  J={0,1}.
```

At t=1 both selected vertices are mandatory. Nonempty-star conditions hold.

| R | C | Surviving bases | Total factors |
|---|---|---:|---:|
| empty | {0,1} | 2 | 4 |
| {0} | {1} | 1 | 3 |
| {1} | {0} | 3 | 5 |
| {0,1} | empty | 2 | 4 |

The cut selects R={0}, C={1}; Q'=0 and beta=a. A has determinant one, so three factors are necessary for this Q'=0 matrix and the mixed plan attains that bound. Either pure orientation requires four factors in the same masking/star family. Again, this is not a global solver lower bound.

For fixed J, orientations leave Q' unchanged. They therefore do not improve beta relative to one another and have the same reconstructed base iterates under corresponding initialization. Their state, storage, and factor-certificate tightness can differ.

### Counterexample To The Unrestricted Cut Claim

Use

```text
U = [[1,1],[0,1],[0,0]]
V = [[0,1],[0,1],[1,1]]
q = (1,1,0)
A = [[0,1,2],[1,0,1],[0,0,0]]
J = {0,1},  t=1/2.
```

The exact factor counts after removing empty stars, in order R=empty,{0},{1},{0,1}, are `4,2,3,2`. They violate submodularity: `4+2 > 2+3`. Thus the exact count function cannot be represented by the same binary-label min-cut framework even with auxiliary nodes. This is not a proof of NP-hardness or of impossibility of another polynomial algorithm. It does refute extending the stated graph construction to arbitrary empty-star savings.

## A Priced Mixed Positive Evaluator

Here is an exact extension of the row-only schedule, needed before treating mixed orientation as a usable execution plan. Let l=|R|, c=|C|, k=l+c, r0 be surviving original base coordinates. Store h and next for bases, row stars, column stars, and dangling mass. Original r-sized scratch is still necessary after dropping base coordinates.

Let `ell_i=A_ii` be admitted exactly. The column-star coordinate is `h_C[j]=x_j/d_j` at a fixed point.

1. **Selected-C factor pass.** From old h_C compute `w_f=sum_(j in C) V_jf h_C[j]`. Positive prefix/suffix exclusion also computes `e_i=ell_i*h_C[i]+sum_f U_if sum_(j in C,j!=i) V_jf h_C[j]` for i in C. Thus e_i is exactly the selected-column contribution at that selected row, without q*h subtraction.
2. **Original entity pass.** For i in R use `x_i=b_i+a*(h_R[i]+p_i*h_d)`. For i in C use `x_i=b_i+a*(U_i*h_base+e_i+p_i*h_d)`. Outside J use `x_i=H_i*(b_i+a*(U_i*(h_base+w)+p_i*h_d))`, interpreting absent base coordinates as zero. Gather next_C[j]=y_j, next_base[f]=sum_(i outside C) V_if*y_i, g_off[f]=sum_(i outside R) V_if*y_i, selected y_R, and global dangling mass.
3. **Selected-R factor pass.** Use the existing positive row formula: `next_R[i]=ell_i*y_i+sum_f U_if*(g_off[f]+sum_(j in R,j!=i) V_jf*y_j)`, evaluated with prefix/suffix sums.

The formulas exactly apply the disjoint released operator: C stars act only outside R, R stars restore full original rows, and bases gather only sources outside C. For nonnegative h all runtime products and sums are nonnegative. No selected dense A row/column is materialized. Signed test states preserve the algebra, not positivity.

A conservative scalar reservation, with M=max(l,c), is

```text
h,next                  2*(r0+k+1)
w,g_off                 2*r
e_C,y_R                 k
prefix/suffix scratch   2*(M+1)
decoded group weights   2*M
total                   2*r0+2*r+3*k+4*M+4
                    <=  2*r0+2*r+7*k+4.
```

This deliberately allows w and g_off to coexist during the entity pass. Free phase-specific buffers when their lifetimes end. IDs, maps, weights for certification, scalar/enclosure widths, oversized row decoding, and runtime remain extra. Separate selected-C and selected-R indices together contain at most Z selected memberships because the sets are disjoint, but headers, ordering, and builder overlap still cost bytes.

The three-factor mixed example reserves 22 scalar slots by this bill, versus 25 under the existing all-row bill. It has three logical phases rather than two. This is an exact schedule comparison, not a memory benchmark or a latency win. A pure-row plan remains preferable when its simpler phases or smaller index cost outweigh the state saving.

**Independent-review addition: specialized all-column plan.** When R is empty, no phase-3 row restoration or g_off accumulation is needed. Omit that r-sized array while retaining w for the entity pass. The conservative reservation becomes `B_col=2*r0(empty)+r+7*k+4`, symmetric in formula with the existing all-row bill but not necessarily in surviving-factor count. The reviewed nonempty-star fixture `U=[[1,1],[1,0],[0,1]], V=[[1,0],[0,1],[0,0]], q=(1,0,0), J={0}` reserves 15 slots, versus 17 for the previous mixed-plus-row portfolio. Both the interval and exact planners now include this immediate specialization; lower bounds for an older restricted portfolio cannot reject it. Five independent affine-map/residual states support the fixture, not a physical allocation measurement.

The residual remains `a Ubar'*(T(h)-h)`, with

```text
c_base[f] = sum_(i outside R) U_if
c_R[i]    = 1
c_C[j]    = sum_(i outside R) A_ij
c_d       = 1.
```

Remove zero-mass columns from the norm. Computing c_C and ell requires admitted exact/enclosed coefficients, not an unchecked large subtraction. A tighter original residual scan can use the same selected-C preprocessing on delta_C and then a row scan; it incurs those additional phases. Preserve h and next and evaluate differences as needed. The error conversion and source/evaluation/output allowances are unchanged. Positivity does not supply those numerical allowances.

Initialization gathers the new Vbar'^T p; it is not reuse of the old coordinates. D0 measurement, optional pilots, certificate passes, output, and planner/build work are separately charged. The mixed kernel adds an execution option, not a guaranteed improvement over direct low-rank elimination.

## Primary Prior-Art Inspection

Inspected primary material in this task; no secondary-source novelty inference:

| Work and inspected location | Consequence for positioning |
|---|---|
| [Cohen and Rothblum (1993), Section 3, Theorem 3.2 and following paragraph, printed p. 156](https://lab.rockefeller.edu/cohenje/assets/file/211CohenRothblumNonnegativeRanksDecompositionsFactorsNonnegativeMatricesLAA1993.pdf#page=8) | Full-text stochastic normalization and convex-combination interpretation were inspected. They supply the foundation; nonuniform witness thresholds here are a specialization of that geometry plus elementary packing, not a new factorization principle. |
| [Kolmogorov and Zabih (2004), Section 3/Definition 3.1, regularity and Section 7](https://www.cs.cornell.edu/~rdz/Papers/KZ-PAMI04.pdf#page=4) | Full text explicitly treats graph representations using minimized auxiliary labels and the submodularity restriction. The cut construction above uses established graph-representation machinery; its application to supplied-factor survival is the local derivation. This also supports the empty-star obstruction, not an NP-hardness claim. |
| [Lawler (1973), publisher abstract](https://onlinelibrary.wiley.com/doi/pdf/10.1002/net.3230030306) | Hypergraph cut optimization via network flow is old. Only the publisher abstract was accessible; the Berkeley report PDF returned 403. No particular full-text gadget is attributed on that basis. |
| [Shen and Carpentieri (2021), printed pp. 400-401, Section 2](https://journals.sagepub.com/doi/pdf/10.3233/FAIA210212#page=5) | Full text again confirms rank-one compression plus a factor cap tied to dense capacitance-system dimension. Optimizing factor-state cost and exploiting supplied low rank are not new. Compare the complete anisotropic/mixed plan against direct and matrix-free capacitance solvers given the same factors. |

The 2009 compressed-graph and 2019 low-rank comparisons remain in the [existing audit](PageRank-Prior-Art-Comparison.md); they were not searched again. WDBMC 2025 Section 3 remains unresolved in that audit and was not newly obtained here. There is no claim that these local combinations are absent from all prior work.

The [completed independent challenge](PageRank-Independent-Review.md#independent-anisotropic-and-frontier-challenge-2026-09-20) identifies an exact stronger collision: set X={source} union R, tails T_f=(K_f intersect J) plus source when K_f has exterior support, and heads H_f=(L_f intersect J) plus sink when L_f has exterior support. Then survival is `1[T_f intersects X and H_f is not a subset of X]`, the standard directed all-or-nothing hypergraph splitting function. The reviewer inspected [Kenneth, Section 1.1, PDF page 6](https://www.wisdom.weizmann.ac.il/~robi/files/Yotam.Kenneth-MScThesis-2024_01.pdf#page=6); this lead's subsequent fetch timed out. The explicit substitution itself shows why the survival objective must not be presented as a newly discovered optimization class.

Diagonal dominance already gives the m=1 ordinary-rank bound. The present conditional cap does not replace rank by a smaller universal quantity; it uses additional pointwise mixture information. The orientation theorem is a standard cut representation once the survival objective is exposed, not new low-rank elimination.

## Evidence And Final Synthesis

Fresh exact standard-library probe, seed 20260922:

| Check | Result |
|---|---|
| Nonuniform mixture-envelope fixtures with partial Q | 64, coverage and budget assertions passed |
| Explicit anisotropic witness | Candidate cap 2, original rank 3, post-release rank 5 |
| Minimum cut versus exhaustive fixed-set orientation oracle | 64 weighted-cost fixtures, identical optima |
| Strict mixed-orientation witness | 3 factors versus 4 for either pure orientation |
| Empty-star extension falsifier | Exact counts (4,2,3,2), non-submodular |
| New mixed evaluator versus explicit released operator | 50 exact state/residual checks, including signed states and weighted contraction |
| Global-average mixture heuristic | Explicit unsafe example retained |

The probe uses dense matrices and a dense residual-capacity array only as small exact oracles. It is not the sparse external-memory implementation, a builder benchmark, or a validated floating certificate. The max-flow routine is a small augmenting-path verifier; a deployed planner should use an established flow implementation and admit its actual memory. Exhaustive orientation is only the test oracle, not the proposed planner.

**Surviving contribution within this note:** a structural, Q-robust nonuniform admission frontier, an exact fixed-set/additive orientation planner, and an algebraically priced mixed positive schedule. These improve uniform guaranteed caps and selected pure-orientation plans, not the optimum of the unchanged exhaustive feasible family. The full-rank examples rule out explaining every improvement as merging proportional factors.

**Open gates:** empirical prevalence of pointwise mixture dominance; planning cost versus a direct heavy-row scan; exact/enclosed coefficient and threshold admission; physical index/flow/decoder/output ownership; nonlinear orientation pricing; optional extra-vertex selection; same-factor direct/Krylov comparisons; and narrower priority analysis. No 4 GB, 50 GB, latency, or paper-readiness guarantee is established.

## Standalone New Probe

Run only this file's Python fence; no older probes are imported or rerun:

```sh
awk '/^```python$/{inside=1;next} /^```$/{if(inside){inside=0;next}} inside{print}' \
  research_algorithms_20260920/PageRank-Anisotropic-Release.md | python3
```

```python
from fractions import Fraction as F
from itertools import product
from random import Random
from collections import deque

def multiply_exact_factor_rows(U,V):
    return [[sum((a*b for a,b in zip(u,v)),F(0)) for v in V] for u in U]

def compute_exact_rational_rank(A):
    B=[list(map(F,row)) for row in A]; rank=0
    for j in range(len(B[0])):
        pivot=next((i for i in range(rank,len(B)) if B[i][j]),None)
        if pivot is None: continue
        B[rank],B[pivot]=B[pivot],B[rank]
        scale=B[rank][j]; B[rank]=[v/scale for v in B[rank]]
        for i in range(rank+1,len(B)):
            scale=B[i][j]; B[i]=[v-scale*w for v,w in zip(B[i],B[rank])]
        rank+=1
    return rank

def inspect_anisotropic_budget_contract(U,V,q,budgets):
    n,r=len(U),len(U[0]); W=multiply_exact_factor_rows(U,V)
    totals=[sum(row[f] for row in U) for f in range(r)]
    s=list(map(sum,zip(*W))); theta=[F(1,b+1) for b in budgets]
    tau=max((sum(totals[f]*V[i][f]*theta[f] for f in range(r))/s[i]
             for i in range(n) if s[i]),default=F(0))
    assert tau<1
    emissions=[(i,f) for f in range(r) if totals[f]
               for i in range(n) if U[i][f]>totals[f]*theta[f]]
    selected={i for i in range(n) if s[i]>q[i] and q[i]>tau*s[i]}
    assert all(F(0)<=q[i]<=W[i][i] for i in range(n))
    assert selected<={i for i,f in emissions}
    assert len(emissions)<=sum(budgets)
    return tau,selected,len(emissions),W

def construct_orientation_flow_graph(U,V,S,weights,rowcost,colcost):
    ids=sorted(S); k=len(ids); r=len(U[0]); size=k+2*r+2
    source,sink=size-2,size-1; pos={i:j for j,i in enumerate(ids)}
    capacity=[[0]*size for _ in range(size)]
    infinity=1+sum(weights)+sum(rowcost.values())+sum(colcost.values())
    for i in ids:
        capacity[source][pos[i]]+=colcost[i]
        capacity[pos[i]][sink]+=rowcost[i]
    for f in range(r):
        left,right=k+2*f,k+2*f+1
        capacity[left][right]+=weights[f]
        for i in range(len(U)):
            if V[i][f]:
                capacity[pos[i] if i in S else source][left]+=infinity
            if U[i][f]:
                capacity[right][pos[i] if i in S else sink]+=infinity
    return capacity,source,sink,ids

def solve_integral_minimum_cut(capacity,source,sink,ids):
    residual=[row[:] for row in capacity]; value=0
    while True:
        parent={source:None}; queue=deque([source])
        while queue and sink not in parent:
            i=queue.popleft()
            for j,c in enumerate(residual[i]):
                if c>0 and j not in parent:
                    parent[j]=i; queue.append(j)
        if sink not in parent:
            return value,{v for i,v in enumerate(ids) if i in parent}
        path=[]; j=sink
        while j!=source:
            i=parent[j]; path.append((i,j)); j=i
        amount=min(residual[i][j] for i,j in path)
        for i,j in path:
            residual[i][j]-=amount; residual[j][i]+=amount
        value+=amount

def evaluate_orientation_surrogate_cost(U,V,S,R,weights,rowcost,colcost):
    C=S-R
    surviving=sum(weights[f] for f in range(len(weights))
                  if any(U[i][f] for i in range(len(U)) if i not in R)
                  and any(V[i][f] for i in range(len(V)) if i not in C))
    return surviving+sum(rowcost[i] for i in R)+sum(colcost[i] for i in C)

def build_exact_oriented_factors(U,V,q,R,C):
    n,r=len(U),len(U[0]); W=multiply_exact_factor_rows(U,V)
    A=[[W[i][j]-(q[i] if i==j else 0) for j in range(n)] for i in range(n)]
    d=list(map(sum,zip(*A)))
    active=[f for f in range(r) if any(U[i][f] for i in range(n) if i not in R)
            and any(V[i][f] for i in range(n) if i not in C)]
    rr,cc=sorted(R),sorted(C)
    Ur=[[F(0) if i in R else U[i][f] for f in active]
        +[F(i==j) for j in rr]+[F(0) if i in R else A[i][j] for j in cc] for i in range(n)]
    Vr=[[F(0) if i in C else V[i][f] for f in active]
        +[A[j][i] for j in rr]+[F(i==j) for j in cc] for i in range(n)]
    qr=[F(0) if i in R|C else q[i] for i in range(n)]
    assert [[x-(qr[i] if i==j else 0) for j,x in enumerate(row)]
            for i,row in enumerate(multiply_exact_factor_rows(Ur,Vr))]==A
    return A,d,active,Ur,Vr,qr

def accumulate_positive_selected_group(U,V,ids,values,ell):
    r=len(U[0]); total=[F(0)]*r
    rows={j:ell[j]*values[j] for j in ids}
    for f in range(r):
        sources=[j for j in ids if V[j][f]]
        prefix=[F(0)]
        for j in sources: prefix.append(prefix[-1]+V[j][f]*values[j])
        suffix=[F(0)]*(len(sources)+1)
        for k in range(len(sources)-1,-1,-1):
            j=sources[k]; suffix[k]=suffix[k+1]+V[j][f]*values[j]
        cursor=0
        for j in ids:
            while cursor<len(sources) and sources[cursor]<j: cursor+=1
            other=(prefix[cursor]+suffix[cursor+1]
                   if cursor<len(sources) and sources[cursor]==j else prefix[-1])
            rows[j]+=U[j][f]*other
        total[f]=prefix[-1]
    return total,rows

def apply_mixed_positive_kernel(U,V,q,p,a,R,C,h):
    A,d,active,Ur,Vr,qr=build_exact_oriented_factors(U,V,q,R,C)
    n,r=len(U),len(U[0]); rr,cc=sorted(R),sorted(C); b0=len(active)
    hb=dict(zip(active,h[:b0])); hr=dict(zip(rr,h[b0:b0+len(rr)]))
    hc=dict(zip(cc,h[b0+len(rr):-1])); ell={i:A[i][i] for i in R|C}
    w,ec=accumulate_positive_selected_group(U,V,cc,hc,ell)
    goff=[F(0)]*r; yr={}; nextb={f:F(0) for f in active}; nextc={}; hd=F(0); x=[]
    for i in range(n):
        base=sum(U[i][f]*hb[f] for f in active)
        value=(hr[i] if i in R else base+ec[i] if i in C
               else base+sum(U[i][f]*w[f] for f in range(r)))
        Hi=F(1) if i in R|C or not d[i] else 1/(1+a*q[i]/d[i])
        xi=Hi*((1-a)*p[i]+a*(value+p[i]*h[-1])); x.append(xi)
        yi=xi/d[i] if d[i] else F(0)
        if not d[i]: hd+=xi
        if i in C: nextc[i]=yi
        else:
            for f in active: nextb[f]+=V[i][f]*yi
        if i in R: yr[i]=yi
        else:
            for f in range(r): goff[f]+=V[i][f]*yi
    _,nextr=accumulate_positive_selected_group(U,V,rr,yr,ell)
    for i in rr: nextr[i]+=sum(U[i][f]*goff[f] for f in range(r))
    result=[nextb[f] for f in active]+[nextr[i] for i in rr]+[nextc[i] for i in cc]+[hd]
    return result,x,(A,d,Ur,Vr,qr)

# A full-rank, non-proportional supplied-factor example.
U=[[F(49,100),F(9,10),F(1,100)],[F(49,100),F(1,100),F(9,10)],
   [F(1,150),F(2,100),F(1,100)],[F(1,150),F(2,100),F(3,100)],
   [F(1,150),F(5,100),F(5,100)]]
V=[[F(9,10),F(9,100),F(1,100)],[F(9,10),F(1,100),F(9,100)],
   [F(95,100),F(4,100),F(1,100)],[F(94,100),F(2,100),F(4,100)],
   [F(96,100),F(3,100),F(1,100)]]
W=multiply_exact_factor_rows(U,V); q=[W[i][i] for i in range(5)]
tau,R,emitted,W=inspect_anisotropic_budget_contract(U,V,q,[2,0,0])
B=[[W[i][j]-(q[i] if i==j and i in R else 0) for j in range(5)] for i in range(5)]
assert tau==F(2,5) and R=={0,1} and emitted==2
assert compute_exact_rational_rank(W)==3 and compute_exact_rational_rank(B)==5

rng=Random(20260922); anisotropic=0
for trial in range(64):
    n,r=5,3
    u=[[F(rng.randrange(1,6)) for _ in range(r)] for _ in range(n)]
    masses=list(map(sum,zip(*u)))
    u=[[value/masses[f] for f,value in enumerate(row)] for row in u]
    v=[]
    for i in range(n):
        weak=F(rng.randrange(1,10),100); split=F(rng.randrange(11),10)
        v.append([1-weak,weak*split,weak*(1-split)])
    w=multiply_exact_factor_rows(u,v)
    qq=[w[i][i]*F(rng.randrange(5),4) for i in range(n)]
    ta,_,_,_=inspect_anisotropic_budget_contract(u,v,qq,[2,0,0])
    assert ta<=F(2,5); anisotropic+=1

# A global average of mixture weights is not a pointwise certificate.
u=[[F(1,10),F(0)] for _ in range(10)]+[[F(0),F(1)]]
v=[[F(1),F(0)] for _ in range(10)]+[[F(0),F(1)]]
assert (10*F(1,3)+1)/11<F(1,2)
theta=[F(1,3),F(1)]
ww=multiply_exact_factor_rows(u,v)
ss=list(map(sum,zip(*ww))); qq=[F(0)]*10+[F(9,10)]
assert {i for i in range(11) if ss[i]>qq[i] and qq[i]>F(1,2)*ss[i]}=={10}
assert not [(i,f) for i in range(11) for f in range(2) if u[i][f]>theta[f]]

orientations=0
for trial in range(64):
    n,r=4,3; S={0,1,2}
    u=[[F(rng.randrange(3)) for _ in range(r)] for _ in range(n)]
    v=[[F(rng.randrange(3)) for _ in range(r)] for _ in range(n)]
    weights=[rng.randrange(1,5) for _ in range(r)]
    rc={i:rng.randrange(5) for i in S}; cc={i:rng.randrange(5) for i in S}
    value,R=solve_integral_minimum_cut(*construct_orientation_flow_graph(u,v,S,weights,rc,cc))
    brute=min(evaluate_orientation_surrogate_cost(u,v,S,{i for i,b in zip(sorted(S),bits) if b},weights,rc,cc)
              for bits in product([0,1],repeat=len(S)))
    assert value==brute==evaluate_orientation_surrogate_cost(u,v,S,R,weights,rc,cc)
    orientations+=1

u=[[F(1),F(0),F(0)],[F(0),F(10),F(1)],[F(0),F(1),F(1)]]
v=[[F(10),F(0),F(1)],[F(0),F(1),F(0)],[F(1),F(0),F(1)]]
qq=[F(10),F(10),F(0)]; S={0,1}; ones={i:1 for i in S}
value,R=solve_integral_minimum_cut(*construct_orientation_flow_graph(u,v,S,[1]*3,ones,ones))
assert value==3 and R=={0}
A,d,active,ur,vr,qr=build_exact_oriented_factors(u,v,qq,R,S-R)
assert compute_exact_rational_rank(A)==3 and len(active)==1
for RR in [set(),S]:
    _,_,act,_,_,_=build_exact_oriented_factors(u,v,qq,RR,S-RR)
    assert len(act)+2==4

# Removing empty stars can make the exact orientation objective non-submodular.
uu=[[F(1),F(1)],[F(0),F(1)],[F(0),F(0)]]
vv=[[F(0),F(1)],[F(0),F(1)],[F(1),F(1)]]
actual=[]
for RR in [set(),{0},{1},S]:
    AA,_,act,_,_,_=build_exact_oriented_factors(uu,vv,[F(1),F(1),F(0)],RR,S-RR)
    actual.append(len(act)+sum(any(AA[i]) for i in RR)
                  +sum(any(AA[i][j] for i in range(3) if i not in RR) for j in S-RR))
assert actual==[4,2,3,2] and actual[0]+actual[3]>actual[1]+actual[2]

fixtures=[(u,v,qq,{0},{1})]
for trial in range(24):
    n,r=4,rng.randrange(1,4)
    u=[[F(rng.randrange(1,4)) for _ in range(r)] for _ in range(n)]
    v=[[F(rng.randrange(1,4)) for _ in range(r)] for _ in range(n)]
    w=multiply_exact_factor_rows(u,v)
    qq=[w[i][i]*F(rng.randrange(3),2) for i in range(n)]
    fixtures.append((u,v,qq,{0},{1}))
kernel_checks=0
for u,v,qq,R,C in fixtures:
    n=len(u); p=[F(1,n)]*n; a=F(17,20)
    A,d,active,ur,vr,qr=build_exact_oriented_factors(u,v,qq,R,C)
    ub=[row+[p[i]] for i,row in enumerate(ur)]
    vt=[[vr[i][f]/d[i] if d[i] else F(0) for i in range(n)] for f in range(len(ur[0]))]
    vt.append([F(di==0) for di in d])
    warm=[sum(v*pi for v,pi in zip(row,p)) for row in vt]
    for h in [warm,[F(rng.randrange(-3,4),3) for _ in warm]]:
        nxt,x,_=apply_mixed_positive_kernel(u,v,qq,p,a,R,C,h)
        H=[1/(1+a*qr[i]/d[i]) if d[i] else F(1) for i in range(n)]
        reference=[H[i]*((1-a)*p[i]+a*sum(v*z for v,z in zip(ub[i],h))) for i in range(n)]
        assert x==reference and nxt==[sum(v*z for v,z in zip(row,x)) for row in vt]
        delta=[v-z for v,z in zip(nxt,h)]
        residual=[(1-a)*p[i]+a*sum((A[i][j]/d[j] if d[j] else p[i])*x[j]
                                 for j in range(n))-x[i] for i in range(n)]
        assert residual==[a*sum(v*z for v,z in zip(row,delta)) for row in ub]
        c=list(map(sum,zip(*ub)))
        beta=max(a*(1+(qr[i]/d[i] if d[i] else 0))/(1+a*(qr[i]/d[i] if d[i] else 0))
                 for i in range(n))
        for f,cf in enumerate(c):
            column=[a*sum(vt[g][i]*H[i]*ub[i][f] for i in range(n)) for g in range(len(c))]
            assert sum(cg*m for cg,m in zip(c,column))<=beta*cf
        if h==warm: assert all(v>=0 for v in nxt)
        kernel_checks+=1
print(dict(anisotropic_envelope_cases=anisotropic,anisotropic_candidate_cap=2,
           anisotropic_postrelease_rank=5,cut_vs_exhaustive_cases=orientations,
           mixed_state_factors=3,pure_orientation_factors=4,
           empty_star_non_submodular=actual,mixed_kernel_residual_checks=kernel_checks))

```
