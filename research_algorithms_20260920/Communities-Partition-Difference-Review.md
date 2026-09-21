# Independent A04 Review: Capped Partition Differences

Date: 2026-09-21. Scope: independent mathematics, primary literature, and a
small exact checker. This file alone is owned by this review. No native
workloads, probe changes, receipt changes, resource measurements, or commits.

Read the final **Concrete Next Mathematical Object** in
[Cancellation Native Results](Communities-Cancellation-Native-Results.md)
and the previous [Cap Review](Communities-Cancellation-Cap-Review.md), including
its optional stronger scalar bounds. Those sources and their receipts are
unchanged. This review does not transfer their native evidence to this proposal.

## 1. Premise Check and Verdict

**The proposed interval is valid. It is a standard separable per-vertex LP
relaxation, efficiently evaluated using occupied partition-intersection cells,
not a new relaxation algorithm or an exact capped-completion solver.**

- It cancels agreement before bounding uncertainty. Equivalent partitions,
  including different names for the same blocks, give exactly `[0,0]`.
- A five-vertex crossing example strictly beats separate *exact* marginal
  intervals, the previous review's best scalar caps, refinement/identity
  controls, meet/join controls, and positive-only/negative-only incident caps.
  The combined upper decreases from 3 to 5/2; the true joint upper is 2.
- Raw row bounds are not uniformly stronger than scalar differences. Always
  intersect valid bounds; in the example the raw lower is worse.
- It is not the strongest linear-work variant: elementary signed-class
  endpoint matching dominates it and attains upper 2 on the same witness.
- Independent rows lose endpoint consistency. Even the intersected result can
  be loose. A separate four-vertex ambiguity pair proves that *no* method using
  only this summary can always determine which crossing partition is better.
- The relevant deduction is an inexpensive application of shared-uncertainty
  cancellation to this summary. Priority, native utility, physical resource
  savings, and global community-optimization guarantees are not established
  by this independent review. Separately reported lead results appear below
  with their attribution boundary.

Lenses used: fractional degree realization, robust comparison under shared
uncertainty, implementation cost, and adversarial information loss. Approaches
compared: separate scalar envelopes, elementary partition controls, this row
relaxation, and exact joint capped LP optimization. The chosen path is an
intersection certificate with explicit controls, followed by the lead's tests,
prototype, and a separately frozen pair-comparison protocol.

## 2. Exact Assumptions and Proof

Let V have K vertices. There must exist an undirected loopless residual with
one weight `x_ij=x_ji` per unordered pair, satisfying

\[
0\le x_{ij}\le D,\quad D\ge0,\qquad
\sum_{j\ne i}x_{ij}=r_i\ge0.
\]

Weights can be real for the theorem; use exact rationals for this checker and
for deterministic certificates without rounding qualifications. A and B are
partitions of the same V. Define `I_R(P)=sum_{i<j, P(i)=P(j)} x_ij`, counting
each edge once, and `M=sum_i r_i/2`. The feasible set may contain every pair;
known support restrictions only shrink it and do not invalidate the bounds.

The theorem needs a certified pair cap, not MG specifically. Recovering
`D=M/(b+1)` requires the prior review's fixed-capacity, reduce-by-minimum,
positive-update MG contract with b>=1. It is not justified by final occupancy,
variable budgets, deletions, independently sketched rows, or a discard-all b=0
path. Residual mass may exist on retained pairs too. D caps residual weight,
not total original pair weight. Nonnegative residuals are essential; a signed
objective does not license signed residual edge weights.

Put `t_i=min(r_i,D)` and `T=sum_i t_i`. Use clipped sums by A block, B block,
and occupied cell `(A(i),B(i))`. For each i let

\[
p_i=T_{A(i)}-T_{A(i),B(i)},\qquad
n_i=T_{B(i)}-T_{A(i),B(i)},\qquad
z_i=T-t_i-p_i-n_i.
\]

The three disjoint neighbor classes are: internal only to A (`+`), internal
only to B (`-`), and internal to both or neither (`0`). The self vertex belongs
to neither neighbor class and must be removed by the `-t_i` term. For every
neighbor, `x_ij<=min(r_j,D)=t_j`. Consequently actual incident masses
`u_i^+,u_i^-,u_i^0` satisfy

\[
u_i^++u_i^-+u_i^0=r_i,\quad
0\le u_i^+\le p_i,\quad 0\le u_i^-\le n_i,\quad
0\le u_i^0\le z_i.
\]

Thus `u_i^+<=min(r_i,p_i)` and
`u_i^->=max(0,r_i-p_i-z_i)`. Summing the difference counts each signed edge
twice, proving

\[
I_R(A)-I_R(B)\le U_{AB}
=\frac12\sum_i\left[\min(r_i,p_i)-(r_i-p_i-z_i)_+\right].
\]

**The lower bound is `L_AB=-U_BA`, not `U_BA`.** Swapping A/B interchanges
p and n while leaving z unchanged. Neutral edges cancel algebraically, not by
assuming their mass is zero.

This is also the exact optimum of each isolated three-bin row problem: put
`min(r_i,p_i)` in positive, fill neutral next, then negative. The remaining
negative mass fits because feasibility implies `r_i<=p_i+n_i+z_i=T-t_i`.
In particular the summand equals

\[
\min\{r_i,\ p_i,\ 2p_i+z_i-r_i\}.
\]

This identity supplies a second formulation for testing and a local dual
certificate. It does **not** show that the isolated optima coexist in one
symmetric residual graph.

If A refines B, p=0 and U_AB<=0, as it must; if B refines A the lower is
nonnegative. If their blocks are identical up to relabeling, p=n=0 and
feasibility gives r<=z, so both endpoints are zero. D=0 requires r=0; empty
V and zero-degree vertices have zero contribution. These are residual-mass
identities, not necessarily ordering statements for modularity with a changed
null-model penalty.

Existence of a capped completion is an assumption, not an output of these
formulas. Even `r_i<=T-t_i` and nonempty bounds are insufficient: with
`r=(3,3,1,1), D=1`, the degree-three vertices need more degree at the other
endpoints than those vertices possess. The checker detects infeasibility by
the exact oracle. Reject inconsistent metadata; do not repair it by clamping.

## 3. Strong Comparator and Strict Crossing Separation

Write `[L_P,U_P]` for the previous cap review's **strongest checked scalar
interval**, including its two-ended boundary upper and sorted-subset boundary
lower improvements. For clarity, with `s_C=sum_{i in C} r_i` and clipped `T_C`,
the checker uses

\[
\alpha_C=\max\left\{\sum_{i\in C}(r_i-T_C+t_i)_+,
\max_{1\le h\le |C|}\left[\sum_{k\le h}r_{(k)}-Dh(h-1)
-\sum_{k>h}\min(r_{(k)},hD)\right]\right\},
\]
\[
q_C=\min\{2M-s_C,\ \sum_{i\in C}\min(r_i,T-T_C),
\sum_{j\notin C}\min(r_j,T_C)\},
\]
\[
L_P=M-\min\{M,\tfrac12\sum_Cq_C,\sum_Cq_C-\max_Cq_C\},\quad
U_P=M-\max\{\max_C\alpha_C,\tfrac12\sum_C\alpha_C\}.
\]

Here r_(k) is the descending degree list inside C. These are restated controls,
not new claims about scalar cap bounds. Exact marginal LP intervals are an
even stronger oracle control on the explicit witness.

The checker combines the following controls before adding the proposed row
interval. This avoids counting self-comparison or partition refinement as a
research gain.

1. Separate differences: `[L_A-U_B, U_A-L_B]`.
2. Identity up to block renaming and refinement signs. Let C be the common
   refinement (occupied cells) and J the least common coarsening. Propagate
   `I_C<=I_A,I_B<=I_J` to tighten all four scalar intervals. Also use the
   consequent bounds `I_A-I_B<=min(U_A-L_C,U_J-L_B)` and the reversed lower
   bounds. Some of these are redundant after propagation; include them openly.
3. Even ignoring forced opposite-sign mass, signed-edge totals are bounded by
   `H_+=sum_i min(r_i,p_i)/2`, `H_-=sum_i min(r_i,n_i)/2`. Intersect with
   `[-H_-,H_+]`. These also dominate simple D times signed-pair-count caps.

Call the resulting interval S. Report `S intersect [L_AB,U_AB]`, never replace
S with the raw row interval. This is a precisely defined strong control, not
a claim to include every elementary inequality anyone could invent.

### A Crossing Example Beyond a Single Move

Use vertices 0,...,4, `D=1`, `r=(1,2,2,2,3)`, `M=5`, and

```text
A = {0,1} | {2,3,4}       labels (0,0,1,1,1)
B = {0,1,4} | {2} | {3}   labels (0,0,1,2,0)
```

Neither refines the other. Their common refinement has block {0,1} and three
singletons; their join has one block. Deleting any one vertex does not make
the induced partitions equal, so this is not just a renamed single-vertex
move. All t_i=1. The positive pairs are 23,24,34; the negative pairs are 04,14.
Pair 01, which is internal to both, cancels.

| Quantity | Value |
| --- | --- |
| Best scalar cap intervals, also exact marginal LP intervals | I_A in [2,4], I_B in [1,2] |
| Separate best interval difference | [0,3] |
| Meet/join/refinement/identity and sign-only controls combined | [0,3] |
| Raw row interval | [-3/2,5/2] |
| Intersected proposed certificate | [0,5/2] |
| Exact joint capped LP difference | [0,2] |

The upper row summands at vertices 0,...,4 are `(0,0,2,2,1)`: vertex 4 must
place at least one degree unit on negative pairs. The scalar maximum I_A=4
and minimum I_B=1 cannot coexist. Both endpoints of the exact joint range
are attained by the following unit-weight edge sets, respectively:

```text
delta=0: {04,13,14,23,24}   I_A=2, I_B=2
delta=2: {01,14,23,24,34}   I_A=4, I_B=2
```

This upper improvement remains strict if the comparator gets exact marginal
LP intervals for A, B, C, and J for free. Reversing A/B gives a strict lower
improvement. Scale all degrees and D by any positive rational to obtain the
same separation at that scale. These graphs are also genuine possible MG
residuals: feed their five distinct unit pairs to capacity b=4; all counters
cancel once, leaving h=0 and D=1.

For a concrete decision crossover, take these summaries as the entire graph,
W=5, h=0, gamma=4. Original degree-square penalties are 58/100 for A and
44/100 for B. The known difference is therefore -14/25. The strong control
allows `Q(A)-Q(B)<=1/25`; the proposed intersection proves
`Q(A)-Q(B)<=-3/50`. The exact maximum is -4/25. Thus a decision certificate,
not just an interval width, can improve on this control. This is a constructed
resolution-parameter example, not a native result or a recommended gamma.

## 4. Two Different Obstructions

### Non-Tightness: Relaxation Loss

The preceding example already shows non-tightness *after* intersecting all
listed controls: 5/2 exceeds the true upper 2. An elementary stronger
certificate explains the gap. Write `s=x24+x34<=2`, `n=x04+x14=3-s` from
vertex 4's degree, and `x23<=1`. Then

\[
I_A-I_B=x23+s-n=x23+2s-3\le1+4-3=2.
\]

The row method counts vertex 4's forced negative unit only as a half-unit
in the summed bound; the other endpoints' separately optimized rows need not
confirm that same mass. Likewise the row lower -3/2 is weaker than the scalar
lower 0. A standard degree-potential dual certificate (Section 6) attains 2:
take `y4=-1`, the other y_i=0. This is not an integrality gap: the exact
comparison is to a fractional LP, and the displayed inequality holds for
all real feasible residuals.

Further elementary signed-class endpoint matching can also dominate the row
formula. If `a_i=min(r_i,p_i)` and `f_i=(r_i-p_i-z_i)_+`, positive mass is at
most `min(sum a/2, sum a-max a)` and negative mass is at least
`max(sum f/2,max f)`. Their difference is a valid upper no larger than U_AB,
and gives 2 on this witness. Each positive edge has an endpoint other than
any chosen vertex, which proves the `sum a-max a` term; negative total mass
is at least any one vertex's negative incident mass, proving `max f`.
Swapping A/B supplies the corresponding lower. Empty maxima are zero.
This costs the same asymptotic work and scratch as the row method. The checker
validates its containment and dominance over the row bound on the entire grid.
This is an additional inexpensive standard
control, **not** evidence of new algorithmic priority. The strict separation
above is against the explicitly listed controls, not this stronger derived
signed-class control, arbitrary dual certificates, or the joint LP. A lead
that adds it should freeze it as a separately named comparator/tier.

### Indistinguishability: Information Loss

Let `K=4`, `r=(1,1,1,1)`, `D=1`, and

```text
A = {0,1} | {2,3}
B = {0,2} | {1,3}
R_plus  = {01,23}, each of weight 1
R_minus = {02,13}, each of weight 1
```

Both yield exactly the same h=0, r, D, M=2, original degrees, no loops, and
configured MG capacity b=1: the first unit pair fills the one counter, the
second distinct unit pair cancels it. But their differences are +2 and -2.
For both partitions the original degree-square penalty is 1/2, so their
modularity differences are +1 and -1 for every common gamma. The row bound
is exactly `[-2,2]`, and neither full capped LP nor any summary-only procedure
can distinguish the two inputs or certify one ordering. Convex combinations
give all intermediate differences in the capped model.

This impossibility is for this specified information model, not for every
graph sketch of the same asymptotic size. Additional source support, retained
history, or a cut/spectral sketch could separate the graphs. No input rescan
or alteration of the immutable native receipts was used to construct it.

## 5. Score Signs and Complexity

With total original edge mass W>0, original degrees d (loops counted twice),
exact retained internal mass J(P), and resolution gamma, the relevant score is

\[
Q(P)=\frac{J(P)+I_R(P)}{W}
-\gamma\sum_{C\in P}\left(\frac{\sum_{i\in C}d_i}{2W}\right)^2.
\]

Exact loop mass can be included in J; it is internal under both partitions
and cancels. Put

\[
\kappa_{AB}=\frac{J(A)-J(B)}{W}
-\gamma\left[\sum_{C\in A}(d_C/2W)^2
-\sum_{C\in B}(d_C/2W)^2\right].
\]

For any valid residual interval `[L,U]`, the score difference interval is
`[kappa_AB+L/W, kappa_AB+U/W]`. Never normalize by retained mass or M, replace
original degrees by retained/residual degrees in the penalty, lose the minus
before the penalty difference, or treat neutral residual edges as absent.
W=0 needs the application's explicit empty-graph convention, not division.

For a fixed winner B in a finite family F, use
`regret<=max(0,max_{A in F, A not equivalent to B} upper(Q(A)-Q(B)))`.
Equivalence means the same blocks, not the same candidate name. This proves
only finite-family claims. These deterministic inequalities hold pointwise
for all partitions, so choosing a partition adaptively does not itself create
a probability/union-bound issue. Search work and label storage still count.

Two degree/label passes suffice for the proposed raw row interval: accumulate
T, sums by A, sums by B, and sums by occupied cell, then sum the row formulas.
There are at most K occupied cells, not `|A|*|B|` stored entries.

| Task | Logical work and additional state |
| --- | --- |
| One residual pair bound | Expected O(K) arithmetic/hash operations; O(|A|+|B|+occupied cells) records |
| Exact retained and penalty differences | O(K+b) if not already cached; no original-edge rescan |
| Arbitrary comparable labels without constant-time hashing | O(K log K) by sorting; O(K) records. Dense integer/radix assumptions can restore linear word work |
| Optional strongest scalar controls used in the checker | O(K log K) with the previous review's sorted implementation; the tiny checker uses direct sums, not that optimized implementation |
| Meet/join control construction | Meet via cells; join can use block-incidence connectivity in O(K+|A|+|B|) graph work. The tiny checker instead uses explicit pairs |
| All F candidate pairs | O(F^2 K) row work after caching F scalar offsets; not O(K) for the family |
| Exact capped joint LP | Theta(K^2) potential variables; this checker uses exponential vertex enumeration |

Existing degree/label arrays and retained counters remain O(K+b) logical
state. Cache marginal sums where useful, but do not call that zero scratch.
These are arithmetic/record bounds, not bit complexity: rational numerators,
denominators, label lengths, allocations, and comparisons carry costs. A
floating implementation needs outward-rounded interval arithmetic or another
certified error bound. An epsilon added to final scores without analysis is
not the exact theorem. No elapsed-time or memory measurements are asserted.

## 6. Prior Art and Priority Challenge

Primary-source search performed 2026-09-21, using queries on capacitated
fractional b-matching duals, per-vertex/Lagrangian relaxation, shared uncertain
parameters, and graph-cut sketches. The following attributions are deliberately
narrow; failing to locate this exact formula is not proof of priority.

| Primary source | What was checked and what is not claimed |
| --- | --- |
| [Barrus, On Fractional Realizations of Graph Degree Sequences, 2014](https://www.combinatorics.org/ojs/index.php/eljc/article/download/v21i2p18/pdf/), equations (1.1)-(1.2), Section 2 | Prescribed incident sums and unit pair caps are explicitly the fractional realization polytope. Scaling by D gives this constraint form. Its graphic integer-degree focus is not a theorem about our arbitrary rational partitions or formula. |
| [Behrend, Fractional Perfect b-Matching Polytopes. I](https://arxiv.org/abs/1301.7356), abstract/general model | Prescribed nonnegative vertex demands define classical fractional perfect b-matching. The graph-theoretic b vector is unrelated to MG counter capacity b. Capacities and signed objective are our explicit specialization. |
| [Bhattacharya, Henzinger, Italiano, Dynamic Algorithms via the Primal-Dual Method, 2018](https://doi.org/10.1016/j.ic.2018.02.005), Section 2.1, LPs (1)-(6) | The primary article's indexed text displays the capacitated fractional hypergraph b-matching primal and vertex-plus-edge-capacity dual. It uses degree inequalities and a nonnegative cardinality objective; equality demands and signed coefficients below are a deduction, not its stated algorithm. Direct publisher/PDF opens were intermittent/blocked; the displayed LP text and author repository record were accessible. |
| [Fisher, The Lagrangian Relaxation Method for Solving Integer Programming Problems, 1981](https://pubsonline.informs.org/doi/10.1287/mnsc.27.1.1), publisher abstract | Relaxing complicating side constraints to obtain bounds is established methodology. Checked abstract only; no claim that this paper gives the exact partition formula. |
| [Mutapcic and Boyd, Cutting-Set Methods for Robust Convex Optimization with Pessimizing Oracles, 2009](https://web.stanford.edu/~boyd/papers/prac_robust.html), Section 1.2 in indexed author PDF | Worst-case evaluation over one uncertainty set is standard robust optimization. Cancellation before maximizing is our direct support-function deduction, not a new shared-uncertainty principle. |
| [Andoni, Krauthgamer, Woodruff, The Sketching Complexity of Graph Cuts, 2014 preprint](https://arxiv.org/abs/1403.7058), abstract and paper | Cut sketches distinguish simultaneous all-cut guarantees from fixed-query randomized guarantees and have different space/accuracy tradeoffs. Our deterministic summary-conditional outer interval is neither such an approximation sketch nor a way around their lower bounds. |
| [Newman, Modularity and Community Structure in Networks, 2006](https://arxiv.org/abs/physics/0602124) | Modularity compares observed within-group weight to a degree-based null model. The signed difference and original-normalization accounting here are algebraic deductions; the certificate is not a new modularity objective. |

### Per-Vertex Relaxation and Shared Uncertainty

Duplicate each undirected edge into endpoint variables y_ij and y_ji, keep
each row sum r_i and `0<=y_ij<=t_j`, and drop `y_ij=y_ji`. Maximizing
`sum_ij c_ij*y_ij/2`, where `c_ij=1[Ai=Aj]-1[Bi=Bj]`, separates by row.
Within a row all coefficients are +1,0,-1, so clipped group sums suffice.
This is exactly the proposed bound. The mathematical primitive is classical
constraint relaxation; the occupied-cell aggregation is the useful compact
representation of this particular objective.

For the common capped feasible set K, write `h_K(c)=max_{x in K} c dot x`.
Then `h_K(c_A-c_B)<=h_K(c_A)+h_K(-c_B)`. Separate intervals optimize the two
terms with independent completions; the left side uses one completion. Strict
inequality in the witness is therefore expected shared-uncertainty behavior,
not a newly discovered cancellation principle. Taking a row relaxation of the
left side can still exceed the right side on other instances, explaining the
need to intersect.

### Fractional b-Matching Dual Bound

The exact reference problem is

\[
\max\ \sum_{i<j}c_{ij}x_{ij},\quad
\sum_{j\ne i}x_{ij}=r_i,\quad 0\le x_{ij}\le D.
\]

For arbitrary free vertex potentials y_i, substitute the degree equalities
and bound each remaining coefficient over `[0,D]`. This gives the classical
weak-duality certificate

\[
U^*=\min_{y\in\mathbb R^K}
\left[\sum_i r_i y_i+D\sum_{i<j}(c_{ij}-y_i-y_j)_+\right].
\]

Equality here is LP strong duality for the nonempty bounded primal; any one
y gives an upper, even without solving the minimization. Equivalently use
nonnegative edge variables q_ij with `y_i+y_j+q_ij>=c_ij`. Node potentials
are **free**, not nonnegative, because degrees are equalities. Demanding
integral x would be a different problem; integer-matching blossom constraints
cannot simply be imposed on allowed fractional residuals.

For the witness, y4=-1 gives degree term -3 and three positive reduced
coefficients: 23 has coefficient 1 and 24,34 have coefficient 2, giving upper
2. This elementary certificate beats the proposed 5/2. General optimal dual
search and dense edge sums are not magically linear work just because the
input summary is short. The isolated-row dual has only breakpoints
lambda in {-1,0,1} and values `2p+z-r`, p, r; that separability, not a new
matching algorithm, explains the cheap formula.

### Cut Sketches

Let cut_R(C) be residual mass leaving C. Then
`I_R(P)=M-sum_{C in P}cut_R(C)/2`, so the pair difference is
`(sum_{C in B}cut_R(C)-sum_{C in A}cut_R(C))/2`. With two-block partitions
this is simply the difference of their cut weights in reversed order.
Cut/spectral sketches are therefore a natural alternate information source.
Subtracting two approximate cuts can have error proportional to their sizes
even when their difference is tiny. A simultaneous all-cut guarantee may
support adaptive selection; a fixed-query guarantee cannot be silently reused
for adaptive candidates. None of this makes the present degree/cap summary
a cut sketch with their guarantees, or our two-graph obstruction a general
space lower bound for cut sketches.

## 7. Independent Exact Checker

For n>=3, the complete loopless incidence matrix has rank n (a triangle forces
every vector satisfying `y_i+y_j=0` to be zero). At any capped-polytope vertex,
the columns for non-bound variables are independent, or a small two-sided
perturbation would contradict extremality. Extend those columns to a basis,
fix all other variables to 0 or D, and solve. The enumeration below therefore
covers *all* vertices, including fractional vertices introduced by caps and
degenerate ones. Compactness makes its linear extrema exact. n<=2 is handled
directly. This proof is independent of integrality or a numerical LP solver.

The comparator directly rederives the prior formulas in this file; no imports
from the old checker or lead probes are used. The grid tests include every
partition pair, not only pairs for which the proposal improves. Synthetic
signed checks use nonuniform retained weights, nonzero exact loops, original
degrees, multiple gamma values, and direct original-score subtraction.

The executable block below imports only Python's standard library. It neither
imports nor executes the lead's implementation. Replay from the repo root:

```sh
awk '/^```python$/{copy=1;next} /^```$/{if(copy){exit}} copy' research_algorithms_20260920/Communities-Partition-Difference-Review.md | python3 -B
```

```python
from fractions import Fraction as F
from itertools import combinations, product


def invert_linear_matrix_exact(matrix):
    n = len(matrix)
    rows = [[F(x) for x in row] + [F(i == j) for j in range(n)]
            for i, row in enumerate(matrix)]
    for col in range(n):
        pivot = next((i for i in range(col, n) if rows[i][col]), None)
        if pivot is None:
            return None
        rows[col], rows[pivot] = rows[pivot], rows[col]
        scale = rows[col][col]
        rows[col] = [x / scale for x in rows[col]]
        for i in range(n):
            if i != col:
                scale = rows[i][col]
                rows[i] = [x - scale*y for x, y in zip(rows[i], rows[col])]
    return [row[n:] for row in rows]


def prepare_incidence_basis_inverses(n):
    edges = tuple(combinations(range(n), 2))
    bases = []
    for free in combinations(range(len(edges)), n):
        inverse = invert_linear_matrix_exact(
            [[int(i in edges[e]) for e in free] for i in range(n)])
        if inverse is not None:
            fixed = tuple(e for e in range(len(edges)) if e not in free)
            bases.append((free, fixed, inverse))
    return edges, bases


def enumerate_capped_polytope_vertices(r, cap, prepared):
    edges, bases = prepared
    n = len(r)
    if n <= 1:
        return {()} if sum(r) == 0 else set()
    if n == 2:
        return {(r[0],)} if r[0] == r[1] and 0 <= r[0] <= cap else set()
    vertices = set()
    for free, fixed, inverse in bases:
        for bits in product((0, 1), repeat=len(fixed)):
            values, rhs = [F(0)] * len(edges), list(r)
            for e, bit in zip(fixed, bits):
                values[e] = cap * bit
                for i in edges[e]:
                    rhs[i] -= values[e]
            solved = [sum(a*b for a, b in zip(row, rhs)) for row in inverse]
            if all(0 <= x <= cap for x in solved):
                for e, x in zip(free, solved):
                    values[e] = x
                assert all(sum(values[e] for e, pair in enumerate(edges)
                               if i in pair) == r[i] for i in range(n))
                vertices.add(tuple(values))
    return vertices


def enumerate_partition_label_vectors(n, prefix=()):
    if len(prefix) == n:
        yield prefix
    else:
        for label in range(max(prefix, default=-1) + 2):
            yield from enumerate_partition_label_vectors(n, prefix + (label,))


def calculate_scalar_cap_interval(r, labels, cap):
    mass = sum(r) / 2
    t = [min(x, cap) for x in r]
    alpha, q = [], []
    for label in set(labels):
        inside = [i for i in range(len(r)) if labels[i] == label]
        outside = [i for i in range(len(r)) if labels[i] != label]
        tc, volume = sum(t[i] for i in inside), sum(r[i] for i in inside)
        low = sum(max(F(0), r[i] - tc + t[i]) for i in inside)
        high = min(2*mass - volume,
                   sum(min(r[i], sum(t)-tc) for i in inside),
                   sum(min(r[i], tc) for i in outside))
        ranked = sorted((r[i] for i in inside), reverse=True)
        for h in range(1, len(ranked) + 1):
            low = max(low, sum(ranked[:h]) - cap*h*(h-1)
                      - sum(min(x, h*cap) for x in ranked[h:]))
        alpha.append(low)
        q.append(high)
    return (mass - min(mass, sum(q)/2, sum(q)-max(q, default=F(0))),
            mass - max(max(alpha, default=F(0)), sum(alpha)/2))


def calculate_signed_row_bounds(r, a, b, cap):
    t = [min(x, cap) for x in r]
    total = sum(t)
    sa, sb, cells = {}, {}, {}
    for x, la, lb in zip(t, a, b):
        sa[la] = sa.get(la, F(0)) + x
        sb[lb] = sb.get(lb, F(0)) + x
        cells[la, lb] = cells.get((la, lb), F(0)) + x
    upper = reverse = positive = negative = F(0)
    max_positive = max_negative = forced_positive = forced_negative = F(0)
    for i, degree in enumerate(r):
        p = sa[a[i]] - cells[a[i], b[i]]
        n = sb[b[i]] - cells[a[i], b[i]]
        z = total - t[i] - p - n
        assert min(p, n, z) >= 0 and degree <= p+n+z
        upper += min(degree, p) - max(F(0), degree-p-z)
        assert min(degree, p) - max(F(0), degree-p-z) == min(degree, p, 2*p+z-degree)
        reverse += min(degree, n) - max(F(0), degree-n-z)
        positive += min(degree, p)
        negative += min(degree, n)
        max_positive = max(max_positive, min(degree, p))
        max_negative = max(max_negative, min(degree, n))
        forced_negative = max(forced_negative, max(F(0), degree-p-z))
        forced_positive = max(forced_positive, max(F(0), degree-n-z))
    endpoint_upper = min(positive/2, positive-max_positive) - max((positive-upper)/2, forced_negative)
    endpoint_reverse = min(negative/2, negative-max_negative) - max((negative-reverse)/2, forced_positive)
    return ((-reverse/2, upper/2), (-negative/2, positive/2),
            (-endpoint_reverse, endpoint_upper))


def partition_refines_other_partition(a, b):
    mapping = {}
    for la, lb in zip(a, b):
        if la in mapping and mapping[la] != lb:
            return False
        mapping[la] = lb
    return True


def construct_partition_meet_join(a, b):
    meet = tuple(zip(a, b))
    join = list(range(len(a)))
    for i, j in combinations(range(len(a)), 2):
        if a[i] == a[j] or b[i] == b[j]:
            old, new = join[j], join[i]
            join = [new if x == old else x for x in join]
    return meet, tuple(join)


def compute_partition_internal_mass(values, edges, labels):
    return sum((x for x, (i, j) in zip(values, edges)
                if labels[i] == labels[j]), F(0))


def calculate_strong_comparator_bounds(r, a, b, cap, oracle=None):
    meet, join = construct_partition_meet_join(a, b)
    labels = (meet, a, b, join)
    bounds = []
    for p in labels:
        if oracle is None:
            bounds.append(calculate_scalar_cap_interval(r, p, cap))
        else:
            vertices, edges = oracle
            scores = [compute_partition_internal_mass(v, edges, p) for v in vertices]
            bounds.append((min(scores), max(scores)))
    # Monotonicity closes marginal intervals along all four refinement relations.
    tightened = []
    for p, (lo, hi) in zip(labels, bounds):
        lo = max([lo] + [qlo for q, (qlo, _) in zip(labels, bounds)
                         if partition_refines_other_partition(q, p)])
        hi = min([hi] + [qhi for q, (_, qhi) in zip(labels, bounds)
                         if partition_refines_other_partition(p, q)])
        tightened.append((lo, hi))
    (lc, uc), (la, ua), (lb, ub), (lj, uj) = tightened
    _, sign, _ = calculate_signed_row_bounds(r, a, b, cap)
    lo = max(la-ub, lc-ub, la-uj, sign[0])
    hi = min(ua-lb, ua-lc, uj-lb, sign[1])
    if partition_refines_other_partition(a, b):
        hi = min(hi, F(0))
    if partition_refines_other_partition(b, a):
        lo = max(lo, F(0))
    return lo, hi


def check_all_partition_pairs(r, cap, prepared):
    vertices = enumerate_capped_polytope_vertices(r, cap, prepared)
    if not vertices:
        return 0, 0, 0, 0
    edges = prepared[0]
    partitions = tuple(enumerate_partition_label_vectors(len(r)))
    scores = {a: [compute_partition_internal_mass(v, edges, a) for v in vertices]
              for a in partitions}
    count = wins = loose = 0
    for a, b in product(partitions, repeat=2):
        exact = [x-y for x, y in zip(scores[a], scores[b])]
        row, _, endpoint = calculate_signed_row_bounds(r, a, b, cap)
        strong = calculate_strong_comparator_bounds(r, a, b, cap)
        combined = (max(row[0], strong[0]), min(row[1], strong[1]))
        assert row[0] <= min(exact) <= max(exact) <= row[1]
        assert row[0] <= endpoint[0] <= min(exact) <= max(exact) <= endpoint[1] <= row[1]
        assert combined[0] <= min(exact) <= max(exact) <= combined[1]
        assert calculate_signed_row_bounds(r, b, a, cap)[0] == (-row[1], -row[0])
        if a == b:
            assert row == strong == (0, 0)
        wins += combined != strong
        loose += combined != (min(exact), max(exact))
        count += 1
    return 1, count, wins, loose


prepared = {n: prepare_incidence_basis_inverses(n) for n in range(6)}
vectors = feasible = cases = wins = loose = 0
for n in range(5):
    for raw in product(range(max(n, 1)), repeat=n):
        r = tuple(map(F, raw))
        result = check_all_partition_pairs(r, F(1), prepared[n])
        vectors += 1
        feasible += result[0]
        cases += result[1]
        wins += result[2]
        loose += result[3]
print('grid:', vectors, feasible, cases, wins, loose)

extra = 0
for r, cap in [((F(1, 3), F(2, 3), F(2, 3), F(1)), F(1, 3)),
               ((F(0),)*3, F(0)),
               (tuple(map(F, (1, 1, 1, 1, 2))), F(1)),
               (tuple(map(F, (1, 2, 2, 2, 3))), F(1))]:
    extra += check_all_partition_pairs(r, cap, prepared[len(r)])[1]
print('extra_pairs:', extra)

r = tuple(map(F, (1, 2, 2, 2, 3)))
a, b = (0, 0, 1, 1, 1), (0, 0, 1, 2, 0)
vertices = enumerate_capped_polytope_vertices(r, F(1), prepared[5])
edges = prepared[5][0]
row, sign, endpoint = calculate_signed_row_bounds(r, a, b, F(1))
strong = calculate_strong_comparator_bounds(r, a, b, F(1))
oracle = calculate_strong_comparator_bounds(r, a, b, F(1), (vertices, edges))
scores = [compute_partition_internal_mass(v, edges, a)
          - compute_partition_internal_mass(v, edges, b) for v in vertices]
assert not partition_refines_other_partition(a, b)
assert not partition_refines_other_partition(b, a)
assert not any(all((a[i] == a[j]) == (b[i] == b[j])
                   for i, j in combinations([v for v in range(5) if v != removed], 2))
               for removed in range(5))
assert row[1] < min(strong[1], oracle[1])
assert row == (-F(3, 2), F(5, 2)) and strong == oracle == (0, 3)
assert (min(scores), max(scores)) == (0, 2)
assert endpoint[1] == 2
assert calculate_signed_row_bounds(r, tuple(7+x for x in a), a, F(1))[0] == (0, 0)
raw_penalties = [sum(sum(r[i] for i in range(5) if p[i] == label)**2
                     for label in set(p)) / 100 for p in (a, b)]
assert raw_penalties == [F(58, 100), F(44, 100)]
assert -4*(raw_penalties[0]-raw_penalties[1]) == -F(14, 25)
assert -F(14, 25) + strong[1]/5 == F(1, 25)
assert -F(14, 25) + row[1]/5 == -F(3, 50)
print('witness:', 'row', row, 'sign', sign, 'strong', strong,
      'oracle_control', oracle, 'exact', (min(scores), max(scores)))
print('marginals:', calculate_scalar_cap_interval(r, a, F(1)),
      calculate_scalar_cap_interval(r, b, F(1)))
potentials = (F(0), F(0), F(0), F(0), F(-1))
dual = sum(x*y for x, y in zip(r, potentials)) + sum(
    max(F(0), int(a[i] == a[j])-int(b[i] == b[j])-potentials[i]-potentials[j])
    for i, j in edges)
assert dual == 2 and max(scores) <= dual
print('endpoint_control:', endpoint, 'dual_upper:', dual)

# Signed residual, retained-edge, and original-degree penalty cancellation.
h = [F((e % 3) + 1, 3) for e in range(len(edges))]
loops = [F(i, 7) for i in range(len(r))]
d = [r[i] + 2*loops[i] + sum(h[e] for e, pair in enumerate(edges) if i in pair)
     for i in range(len(r))]
mass = sum(d) / 2
penalty = lambda p: sum(sum(d[i] for i in range(len(r)) if p[i] == label)**2
                        for label in set(p)) / (4*mass*mass)
delta_h = compute_partition_internal_mass(h, edges, a) - compute_partition_internal_mass(h, edges, b)
signed_checks = 0
for gamma in (F(0), F(1, 2), F(1), F(3, 2)):
    offset = delta_h/mass - gamma*(penalty(a)-penalty(b))
    for v in vertices:
        actual = [x+y for x, y in zip(v, h)]
        qa = (compute_partition_internal_mass(actual, edges, a) + sum(loops))/mass - gamma*penalty(a)
        qb = (compute_partition_internal_mass(actual, edges, b) + sum(loops))/mass - gamma*penalty(b)
        delta = compute_partition_internal_mass(v, edges, a) - compute_partition_internal_mass(v, edges, b)
        assert qa-qb == offset + delta/mass
        assert offset+row[0]/mass <= qa-qb <= offset+row[1]/mass
        signed_checks += 1

# Same MG summary: two positive unit updates with b=1 cancel completely.
a, b = (0, 0, 1, 1), (0, 1, 0, 1)
graphs = [((0, 1), (2, 3)), ((0, 2), (1, 3))]
for graph, expected in zip(graphs, (2, -2)):
    assert all(sum(i in pair for pair in graph) == 1 for i in range(4))
    counters, cancellation = {}, F(0)
    for key in graph:
        if not counters:
            counters[key] = F(1)
        else:
            assert key not in counters and len(counters) == 1
            cancellation += F(1)
            counters = {}
    assert counters == {} and cancellation == 1
    assert sum(int(a[i] == a[j])-int(b[i] == b[j]) for i, j in graph) == expected
assert calculate_signed_row_bounds((F(1),)*4, a, b, F(1))[0] == (-2, 2)
assert not enumerate_capped_polytope_vertices(tuple(map(F, (3, 3, 1, 1))), F(1), prepared[4])
print('signed_checks:', signed_checks, 'indistinguishable_graphs:', len(graphs))
print('PASS: exact LP containment, strong controls, signed cancellation, and obstructions')
```

### Executed Results

Executed the replay command above on 2026-09-21 using Python 3.9.6, standard
library only. Exit status 0. Exact stdout:

```text
grid: 289 83 15985 324 3612
extra_pairs: 5658
witness: row (Fraction(-3, 2), Fraction(5, 2)) sign (Fraction(-2, 1), Fraction(3, 1)) strong (Fraction(0, 1), Fraction(3, 1)) oracle_control (Fraction(0, 1), Fraction(3, 1)) exact (Fraction(0, 1), Fraction(2, 1))
marginals: (Fraction(2, 1), Fraction(4, 1)) (Fraction(1, 1), Fraction(2, 1))
endpoint_control: (Fraction(-1, 1), Fraction(2, 1)) dual_upper: 2
signed_checks: 24 indistinguishable_graphs: 2
PASS: exact LP containment, strong controls, signed cancellation, and obstructions
```

The grid line means: 289 degree vectors (including empty V), 83 feasible,
15,985 ordered partition-pair cases, 324 strict improvements after intersecting
the row interval with S, and 3,612 cases where that intersection is still
non-exact. It covers all `r in {0,...,n-1}^n` for 1<=n<=4, D=1, plus n=0.
Feasible fractional non-graphic integer degree lists are included, not silently
dropped by a simple-graph enumerator.

The additional 5,658 ordered pair cases cover all partitions for a rational
cap/degree example, a zero-cap example, and two five-vertex degree lists. Total:
**21,643 exact LP/partition-pair cases**, plus 24 direct signed modularity
checks. All cases test the row formula, its endpoint strengthening, and the
explicit strong comparator. The main crossing witness additionally tests an
exact-marginal comparator, the non-single-move condition, label-renaming
invariance, the constructed decision crossover, and the free-potential dual
certificate. The final two graphs check identical MG summaries and opposite
signed differences. The invalid `(3,3,1,1)` metadata has no oracle completion.

These are finite diagnostic counts, not native success rates, typical
performance estimates, or a replacement for the proof. The scalar comparator
and oracle deliberately spend more work than a production linear evaluator;
their work is not being claimed as the proposed runtime.

## 8. Final Synthesis and Handoff

The mathematical gate passes for soundness and for strict improvement over
the explicitly defined separate-cap/refinement/identity comparator on crossing
nonidentical partitions. It does **not** pass a novelty gate for the underlying
relaxation, a sharpness gate, or a claim to be the best elementary O(K) bound.
The endpoint strengthening should be visible alongside the original row
formula, not discovered after claiming a new native contribution.

The original handoff was tests, prototype, and a **new** frozen pair comparison
protocol. The lead has since reported completion below. These acceptance points
remain review criteria for those artifacts and any later extension, not a
request to rerun the completed native gate:

1. Test exact signs, equivalent-label partitions, nested and crossing pairs,
   zero cap/residual, rational weights, and nonempty versus infeasible metadata.
2. Keep the proposed raw row bound, its intersection with existing controls,
   and any endpoint-strengthened tier separately identifiable. Price the
   optional sorted scalar comparator separately from the linear scalar tier.
3. Compare the same fixed candidates, orders, budgets, gamma values, and
   original normalization. Exclude self/equivalent comparisons; classify
   crossing versus refinement cases before attributing an improvement.
4. Check actual original score differences against both endpoints, retaining
   all failures and no-improvement cases. Report new finite-family dominance
   or regret certificates separately from width-only gains and actual quality.
5. Preserve the exact replay alternative and every old source/receipt. This
   note supplies no independently collected native numbers or execution-resource
   evidence, nor permission to reinterpret an old frozen receipt as evidence
   for a new bound.

### Lead Update: Reported, Not Independently Audited

The lead subsequently reported a completed fixed gate with 2,800 pairs,
840 operational: best-of-three certificate counts 87 for independent caps,
120 after refinement controls, and 140 with joint comparison, hence 20 new
useful certificates over that refinement control. For 560 certify-or-replay
kernel runs, the lead reported 20 rescans/703,368 records for the strongest
refinement control versus zero rescans and 20 core calls with joint comparison.
Requiring an **exact scalar score** instead forces 70 rescans. These are
lead-supplied figures, not outputs of this review's checker; no native run or
receipt audit was performed here. Certifying a selection does not imply
recovering exact scores, and neither result establishes global novelty.

Two especially relevant comparator/attribution questions remain open and were
**not run** as ablations here:

- **Generic cap versus MG cap:** repeat the matched comparison with D=M,
  which is always a valid residual pair cap, versus D=M/(b+1). The generic
  cap is redundant given feasible degrees. This isolates benefits of joint
  degree/partition cancellation from benefits specifically attributable to
  the tighter MG cap. Do not assign all joint gains to MG without this test.
- **Changed-vertex disagreement bounds:** compare with simple support/degree
  bounds on vertices incident to changed pair indicators, including tight
  single-move local formulas. Align blocks or use equivalence relations;
  differing arbitrary label IDs do not define moved vertices. Such controls,
  the endpoint strengthening above, and small families of dual potentials
  could account for some or all of the observed gains. No dominance claim
  over these untested comparators is warranted.

Other open questions concern how much of the reported fixed-family benefit
survives these stronger controls, and whether additional sketch information
is preferable to stronger optimization of the same ambiguous summary. Any
priority claim needs a more exhaustive literature comparison; any workflow
claim belongs with the lead's separately recorded evidence, not this small
mathematical checker.
