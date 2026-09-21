# Independent A04 Review: Implicit Free-Potential Certificates

Date: 2026-09-21. Owned output: this file only. Read Sections 2, 4, and 6
of [Partition Difference Review](Communities-Partition-Difference-Review.md),
its comparator definitions/checker, and
[Novelty Evidence Policy](Novelty-Baseline-Evidence-Policy.md).
No lead probe imports, native experiments, timing/RSS measurements, receipt
changes, goal operations, or commits. The lead's comparator ablations and
compact implementation are separate work and are not audited here.

## 1. Premise Check and Verdict

**The identity, strict-active subgradient, and O(K log K) arithmetic / O(K)
record bound are correct under the stated complete-pair, uniform-cap model.**
A finite search can produce strictly stronger valid certificates without
solving the dual globally. Two exact separating examples appear below; one
has a nonconstant feasible objective and nonzero active hinges.

The negative coefficients are not a license to subtract arbitrary
subgradients. Use the same strict-positive indicator in every group term;
the resulting signed counts equal the actual active-pair degrees exactly,
including at ties. The final function is convex because of its original
edgewise expression, not because a signed sum of convex functions is convex.

Lenses: LP duality, combinatorial aggregation, numerical certification, and
adversarial comparator/prior-art review. Alternatives considered: explicit
dense evaluation; selected vertex/subset degree inequalities; and the
implicit all-potential evaluator. The latter permits richer bounded search,
but cheap hand-derived controls already explain the small witnesses.

**Surviving delta:** a precisely specified composition of occupied-cell
inclusion-exclusion with established sorted pairwise-hinge primitives gives
an exact value/subgradient oracle for this partition-difference dual using
linear records. It enables an anytime certificate tier beyond the previously
specified controls. This is not new LP duality, a new general hinge primitive,
the first implicit dense matching method, or an established priority claim
for the composition. The optional theorem below establishes a crude
deterministic additive approximation bound, not finite-round exact global
optimality, native utility, or paper-worthy novelty.

**Lead update, reported rather than independently audited:** the simple
sign-support degree control already certifies 140/140 on the old native gate,
matching all tested row/endpoint/generic/capped tiers. That gate is saturated:
no old-row native benefit survives against this control, and this dual review
claims none. The witnesses below separate the explicitly defined prior-review
controls, not every newly constructed sign-support/subset inequality. The
lead's new `probe_implicit_partition_dual.py` was neither read nor imported.

## 2. Assumptions and Weak Duality

There are K vertices, two fixed partitions A and B of those same vertices,
finite D >= 0, and finite r_i >= 0. A nonempty feasible residual set is assumed:

\[
\sum_{j\ne i}x_{ij}=r_i,\qquad 0\le x_{ij}=x_{ji}\le D\quad(i<j).
\]

The residual is loopless, undirected, and fractional weights are allowed.
Let c_ij = 1[A_i=A_j] - 1[B_i=B_j]. For any finite real y, the degree
equalities give

\[
c\cdot x=r\cdot y+\sum_{i<j}(c_{ij}-y_i-y_j)x_{ij}
\le F_{AB}(y):=r\cdot y+D\sum_{i<j}(c_{ij}-y_i-y_j)_+.
\]

Equality constraints make y free, not nonnegative. Classical finite LP
strong duality gives min_y F_AB(y) = max_x c dot x, but evaluating one y
requires neither optimality nor convergence. Obtain a lower certificate as
`-F_BA(z)` with an independently chosen z, not by negating F_AB(y).

Boundary qualifications:

- Feasibility is assumed, not detected by this evaluator. D=0 forces r=0;
  K<=1 also forces r=0. Empty sums are zero. K=2 has its usual equality of
  endpoint degrees. Negative residual weights invalidate the proof.
- D caps each residual pair, including residual weight on retained pairs;
  it does not cap the original total edge weight. No MG-specific cap formula
  is inferred here. M=sum(r)/2 need not be integral.
- Ignoring known forbidden pairs is a valid relaxation. The fast expression
  is exact for the complete-pair relaxation, not automatically for arbitrary
  restricted support, arbitrary pair caps D_ij, or arbitrary edge costs.
- The certificates concern one fixed pair of partitions. They do not solve
  community search or eliminate the summary indistinguishability obstruction
  in the prior review. Retained terms and original-degree score penalties
  must still be added with the prior review's normalization/signs.
- Potentials may be rounded or clipped as a search choice and still give a
  valid bound when re-evaluated exactly. Restricting search is not proof that
  an unrestricted optimum lies in that region.

## 3. Identity and Tie-Safe Subgradient

Write h_t(s)=(t-s)_+, a=1[A_i=A_j], b=1[B_i=B_j], and s=y_i+y_j.
The required scalar identity is

\[
h_{a-b}(s)=h_0(s)+a(h_1(s)-h_0(s))
+b(h_{-1}(s)-h_0(s))-ab(h_1(s)+h_{-1}(s)-2h_0(s)).
\]

Its four cases (a,b)=(0,0),(1,0),(0,1),(1,1) reduce to h_0,h_1,h_-1,h_0,
respectively. Summing over unordered distinct pairs proves the proposed
formula, where the sums indexed by A and B run over blocks:

\[
E(y)=H_0(V)+\sum_{C\in A}(H_1(C)-H_0(C))
+\sum_{C\in B}(H_{-1}(C)-H_0(C))
-\sum_{C\in A\wedge B}(H_1(C)+H_{-1}(C)-2H_0(C)).
\]

Only occupied intersections in A meet B are needed. Notice `E` is the hinge
sum; the edge contribution to F is `D*E`, not E unless D=1.

Replace every h_t(s) in the four-case identity by d_t(s)=1[s<t]. The same
four reductions still hold, including s=-1,0,1. Define

\[
d_t(S,i)=\#\{j\in S\setminus\{i\}:y_i+y_j<t\}.
\]

For i in cell C_i, its actual strict-active degree is

\[
d_i=d_0(V,i)+d_1(A_i,i)-d_0(A_i,i)
+d_{-1}(B_i,i)-d_0(B_i,i)
-d_1(C_i,i)-d_{-1}(C_i,i)+2d_0(C_i,i).
\]

Here A_i/B_i denote i's blocks. The result is an integer in [0,K-1], even
though intermediate signed sums need not be. Thus

\[
g_i=r_i-Dd_i\in\partial F_{AB}(y).
\]

Proof of the last assertion is edgewise: use slope -1 at a positive hinge
and slope 0 at a zero or negative hinge. Zero is an allowed hinge slope at
equality. Summing those supporting affine functions with nonnegative D gives
the subgradient inequality for every other potential vector. This argument
does not use an invalid negative-coefficient subgradient sum rule.

**Concrete tie trap.** Take K=2, A=B=one block, y=(1/2,1/2), D=1,
r=(1,1). The actual c=0 hinge is locally zero; its edge gradient must be
(0,0). Both the A-block H_1 and the subtracted cell H_1 are tied. Choosing
slope -1 for the former but 0 for the latter produces the spurious edge
gradient (-1,-1). The resulting full candidate g=(0,0) is invalid: at
z=(2/5,2/5), F(z)=4/5 < F(y)+g dot (z-y)=1. Consistent strict-zero tie
selection cancels correctly. Consistent non-strict selection also admits a
separate proof, but it is a different oracle; do not mix conventions.

## 4. Linear Records, Not Quadratic Pairs

For a group S of size m, sort its indexed potentials as v_0<=...<=v_(m-1)
and form prefix sums P_q=sum_(j<q) v_j. For each k let

\[
q_k=\#\{j:v_j<t-v_k\},\qquad s_k=1[2v_k<t].
\]

As k increases, q_k is nonincreasing, so one pointer starting at m computes
all q_k in O(m) comparisons after sorting. Then

\[
d_t(S,k)=q_k-s_k,
\quad
H_t(S)=\tfrac12\sum_k\{q_k(t-v_k)-P_{q_k}-s_k(t-2v_k)\}.
\]

The subtraction removes the diagonal; the half removes the two orientations
of every off-diagonal pair. With strict comparisons, all tied potentials
behave correctly, regardless of their relative sorted order. Binary searches
instead of the pointer give the same overall O(K log K) bound.

Count all memberships: K in V, K across A blocks, K across B blocks, and K
across occupied cells, for 4K total. Sorting each group once costs at most
4K log(max(2,K)) comparisons; process only thresholds needed for that group.
The constant number of sweeps costs O(K). Store indexed group lists, prefix
sums, y, r, one accumulated degree/subgradient vector, and optional best y:
O(K) records in total. **Do not allocate a length-K derivative array for
every group**; return/accumulate only its m indexed entries.

Partition grouping is expected O(K) with suitable hashing, or deterministic
O(K log K) with comparable labels and sorting. This does not require a dense
|A| by |B| table. Large string labels, arbitrary-precision arithmetic, and
bit lengths are not constant-cost words. These are arithmetic/record bounds,
not bit-complexity, elapsed-time, byte-count, or RSS claims.

T complete oracle evaluations plus O(K) updates cost O(T K log K), with O(K)
live records if only current/best vectors are kept. Re-sorting changed
potentials counts every round. Retaining all iterates costs O(TK); a bundle
or line-search method must account for its own extra work/state. Running
both orientations only changes a constant. Testing every pair in an N-member
partition family additionally multiplies work by O(N^2).

Numerical certification is separate from the combinatorial proof. For
negative coefficients, an upper endpoint of a group term must be paired with
a lower endpoint when that term is subtracted. Use exact arithmetic or
proper outward interval operations, including prefix sums and comparisons.
An arbitrary epsilon on the final answer is not a proof. Exact rational
evaluation of a stored floating-point y, regarded as dyadic rationals, also
certifies that particular vector. Approximate gradients may be useful for
search, but do not call them the exact subgradient without a tie/error audit.

## 5. Bounded Search and Separating Witnesses

For any finite evaluated sequence, keep
`U=min(U_existing, F_AB(y_0), ..., F_AB(y_T))` and analogously intersect lower
bounds from B,A. Each candidate is valid, even if search is adaptive, worsens
the objective, or stops early. An arbitrary bounded search schedule does not
promise a strict gain on every input, a prescribed approximation gap, or
optimality. The specific projected schedule below has a coarse gap bound.
Basic subgradient descent is not monotone; preserve the incumbent. A nonzero
chosen subgradient at a tied optimum does not refute optimality.

### Optional Theorem: Bounded Half-Integral Optimum

**The proposed theorem is sound for K>=3 under Section 2's assumptions.**
There exists an optimal y in `[-K,K]^K` with each coordinate in `(1/2)Z`.
This existence statement does not restrict iterative search to a half-grid.

1. The nonempty bounded primal and finite LP strong duality ensure that F
   attains a finite global minimum. Eliminating the nonnegative edge dual
   variables gives exactly F, so an optimal potential vector exists.
2. Choose a closed arrangement cell containing such a minimizer, using one
   of the two inequalities `y_i+y_j <= c_ij` or `>= c_ij` for every pair.
   F is affine on that cell, even on its boundary. Every cell is pointed:
   a line direction v would satisfy v_i+v_j=0 for all pairs; a triangle
   forces v=0, and the remaining vertices then also have zero coordinate.
3. The cell's nonempty optimal face contains a vertex. One direct argument:
   if active normals have rank below K, choose a nonzero null direction.
   Small moves in both directions remain feasible; optimality forces zero
   objective slope. At least one direction hits an additional independent
   constraint, since otherwise there would be a line. Repeat until rank K.
   This produces an optimal vertex, not merely an infimum along a ray.
4. Select K independent tight equations `y_i+y_j=c_ij`. Their graph has K
   vertices and K edges. Every connected component must have full column
   rank, so it cannot be bipartite. Each needs at least as many edges as
   vertices; equality of the totals makes every component unicyclic with an
   odd cycle and possibly attached trees.
5. Around an odd cycle of length ell, twice any cycle potential is an
   alternating sum of ell integers in {-1,0,1}. Hence all cycle coordinates
   are half-integral and have absolute value at most ell/2. Propagating along
   a tree path of length h preserves half-integrality and increases magnitude
   by at most h. Since h+ell<=K, `|y_i|<=h+ell/2<=K`.

This proof actually works for any complete-pair objective with coefficients
in {-1,0,1}; the partition structure is needed for the fast oracle, not for
the box theorem. Deleting hyperplanes because an objective coefficient is
zero, or using only a bipartite support graph, breaks the full-rank argument.
With forbidden pairs ignored, the complete-pair relaxation still qualifies.
The theorem is attributed to the lead's proposed challenge, independently
checked here, not claimed as historically new matching theory.

For D>0, feasibility gives `0<=r_i<=D(K-1)`. Thus every strict-active
subgradient obeys `||g||_2<=G=D(K-1)sqrt(K)`. Start at y_0=0 and project each
update onto `[-K,K]^K`. Some optimum has distance at most `R=K sqrt(K)`.
For T>=1 evaluated iterates and constant step

\[
\alpha=\frac{R}{G\sqrt{T}}=\frac{K}{D(K-1)\sqrt{T}},
\qquad y_{t+1}=\Pi_{[-K,K]^K}(y_t-\alpha g_t),
\]

nonexpansiveness of projection and the subgradient inequality imply

\[
2\alpha\sum_{t=0}^{T-1}(F(y_t)-F^*)
\le R^2+T\alpha^2G^2,
\quad
0\le\min_{t<T}F(y_t)-F^*
\le\frac{DK^2(K-1)}{\sqrt{T}}.
\]

This is the standard projected-subgradient argument with these explicit
constants, not a new optimization method. D=0 has r=0 and F=0, so bypass the
step formula. The result bounds error relative to the **maximum over all
completions**; subtracting that error does not give a lower bound on each
individual completion's objective. Use the reversed dual for that purpose.

This is a bounded-record approximation theorem: O(K) live records and
O(T K log K) arithmetic work. It is not a fixed-bit memory theorem. Exact
real arithmetic is assumed; square-root steps may be irrational, while
perfect-square T allows rational steps for rational D. Certified approximate
arithmetic needs a separate accumulated-error analysis. Rounding each iterate
to half-integers is not part of this proof. The K^3-scale error constant is
crude and supplies no practical improvement claim on the saturated old gate.

### Four Vertices: A Degree-Identity Gap

Take D=1 and

```text
r = (1,2,1,1)
A = {0} | {1,2} | {3}       labels (0,1,1,2)
B = {0,3} | {1} | {2}       labels (0,1,2,0)
y = (-1/2,1/2,1/2,-1/2)
```

Only c_12=1 and c_03=-1 are nonzero. In fact c_ij=y_i+y_j for every pair,
so every hinge vanishes and every feasible residual has
`x12-x03=(r1+r2-r0-r3)/2=1/2`. For example, weights 1/2 on 01,03,13 and
weight 1 on 12 realize the degrees. No degree is zero or saturated at K-1.

The endpoint-strengthened upper is 1. Even granting exact marginal intervals
for A, B, meet, and join, the prior specified strong control upper is 1:
`I_A in [1/2,1]`, `I_B in [0,1/2]`. The true upper is 1/2. This is a
useful negative control for overclaiming: a plain subset degree equality
explains the entire gain, without iterative optimization or cap hinges.

Starting y_0=0 and taking six strict-subgradient updates with step 1/2 reaches
the displayed y. Seven evaluated objective values are
`1, 5/2, 1, 2, 3/2, 1, 1/2`. This only demonstrates one bounded schedule;
it is not a general rate theorem or the lead's search policy.

### Five Vertices: Nonconstant Objective and Active Caps

Take D=1 and

```text
r = (1,3,3,2,2)
A = {0,3} | {1} | {2} | {4}  labels (0,1,2,0,3)
B = {0,4} | {1,2} | {3}       labels (0,1,1,2,0)
y = (1/2,-1/2,-1/2,1/2,-1/2)
```

Here delta=x03-x04-x12. The degree term is -5/2 and the only strictly
positive reduced coefficients are those of 14 and 24, both 1. Equivalently,

\[
\delta=-5/2+x_{14}+x_{24}-x_{04}\le-1/2.
\]

The following feasible weights attain -1/2: `x02=x03=x23=1/2`,
`x12=x13=x14=x24=1`, all others zero. Therefore **the exact fractional capped
maximum is -1/2**, without relying on enumeration or an optimizer's status.
The independent rational vertex enumeration below also finds the minimum
-3/2. All degrees are strictly between 0 and K-1; the partitions cross.

The endpoint-strengthened upper is 1. Giving the comparator exact marginals
still leaves upper 0: `I_A in [0,1/2]`, `I_B in [1/2,3/2]`. Thus the dual
beats even that oracle-strengthened version of the prior scalar, refinement,
meet/join, and sign controls, as well as their intersection with endpoint
strengthening. Reversing A/B gives the corresponding strict lower improvement.
Scaling r and D by any positive rational scales every mass bound.

These are constructed mathematical witnesses, not native workload results.
The strongest scalar formulas and all listed controls are independently
re-evaluated below, not replaced by weaker caps. Selected subset potential
controls incorporating these two inequalities would match these witnesses;
that would narrow the advantage further, not establish historical priority.

**Subsequent lead result, attribution boundary retained:** the lead reports
that its normalized diminishing schedule with 12 updates returns
`-24847/55440` on the five-vertex witness, beating the oracle-scalar upper 0.
It reports regression coverage for the supplied optimal vectors and this
bounded-search decision. Those implementation/test claims were not inspected
or independently replayed here. With source graph equal to the displayed
realizing edges, gamma=1/2, W=11/2, and known score offset 9/121, the reported
dual implies score upper `9/121 + (-24847/55440)/(11/2) = -197/27720 < 0`,
whereas the oracle-scalar upper gives 9/121 > 0. Only this rational arithmetic
was separately checked here. For this example D=1 is separately certified
by the simple weighted input, **not** inferred from b=0 MG or from
`D=M/(b+1)`: M=11/2 cannot give D=1 at any integer b. This does not alter the
saturated old native-gate result or attribute the lead's search to this review.

## 6. Bounded Primary-Source Review

Search date: 2026-09-21. Budget used: three batches of four search queries,
followed by targeted primary-text opens and citation follow-through. Topics:
implicit/dense b-matching, signed clique objectives, sorted pairwise hinges,
and sorted pair-sum matrices. Secondary results were discovery pointers only.
The bounded search does not establish absence of earlier equivalent work.

| Inspected primary source | Relevant precedent and comparison boundary |
| --- | --- |
| [Behrend, Fractional Perfect b-Matching Polytopes I, arXiv:1301.7356](https://arxiv.org/abs/1301.7356), abstract/model | Prescribed nonnegative vertex demands and fractional perfect b-matchings are established. Uniform pair caps and the signed objective here are specializations. No claim to invent the LP or free equality multipliers. |
| [Huang and Jebara, Fast b-Matching via Sufficient Selection Belief Propagation, AISTATS 2011, author corrected PDF](https://berthuang.com/papers/fast_bmatching_corrected.pdf), Sections 2.1-2.3, Algorithms 1-2 | Implicit descriptor-based dense bipartite weights, linear-memory belief reconstruction, and sorted-component sufficient selection are explicit. Section 2.2 initially retains quadratic per-iteration work; Section 2.3 accelerates selection. The corrected abstract describes approximately n^2.5 running time as empirical, not a universal expected guarantee. Different model/output: binary bipartite matching with integer demands, versus a scalar certificate and subgradient for fractional nonbipartite completion. It defeats a broad first-linear-memory-dense-matching claim, not by itself this oracle contract. |
| [Fujiwara et al., Efficient Algorithm for the b-Matching Graph, KDD 2020](https://kdd.org/kdd2020/accepted-papers/view/efficient-algorithm-for-the-b-matching-graph.html), official abstract only | b-dash prunes message updates and incrementally computes regression weights. This is relevant dense-graph construction precedent. Full procedures were not inspected; no same-contract dominance or complexity theorem is attributed to it. |
| [Joachims, Training Linear SVMs in Linear Time, KDD 2006](https://www.cs.cornell.edu/people/tj/publications/joachims_06a.pdf), Sections 2.2 and 3 | Ordinal-regression pairwise hinge constraints and structural reformulation already avoid treating every pair as an independent stored training example. The inspected text states O(sn log n) ordinal-regression training under its analysis. This is precedent for structured pairwise optimization, not a matching certificate theorem; a later targeted PDF fetch timed out. |
| [Ghanbari, Li, Scheinberg, Novel and Efficient Approximations for Zero-One and Ranking Losses, 2025](https://link.springer.com/article/10.1007/s10013-025-00767-6), Section 2 | Explicitly states that pairwise linear hinge values and subgradients can be computed in O(n log n) by sorting scores, with earlier references. This directly defeats novelty for the generic sorted linear-hinge primitive. Their new smooth-loss results are not claimed here. |
| [Rust and Hocking, All Pairs Squared Hinge Loss, arXiv:2302.11062v1, 2023](https://arxiv.org/html/2302.11062v1), Section 3.2, equations (20)-(25), Algorithm 2 | Sorts augmented scores and accumulates polynomial coefficients to evaluate squared pairwise hinges in log-linear work. Our linear hinge needs only counts and first moments, not second moments. This is a close inspected aggregation procedure; the square/linear distinction does not support inventing sorted hinge sums. |
| [Bansal, Blum, Chawla, Correlation Clustering, author manuscript 2003-10-27](https://www.cs.cmu.edu/~shuchi/papers/clusteringfull.pdf), abstract/problem formulation | Signed complete-graph agreement objectives over partitions are established. Their decision variable is the partition; here A and B are fixed and edge masses vary with prescribed degrees. A difference of two partition equivalence matrices is a restricted signed objective, not arbitrary correlation clustering. |
| [Boyd with Park, Subgradient Methods, May 2014](https://web.stanford.edu/class/ee364b/lectures/subgrad_method_notes.pdf), Section 2.1 | Non-descent steps and retaining the best iterate are standard. Bounded search here adds no new subgradient algorithm or convergence theorem. |

The Frederickson-Johnson 1984 sorted-matrix DOI was located, but the accessible
publisher response supplied metadata rather than inspected algorithm text.
It is not used as substantive priority evidence.

**Explicit reduction to published primitives (our deduction).** For one
group, make two copies with scores u_i=y_i and v_j=t-y_j-1. Then the ordinary
cross-list margin-one hinge is `(1-u_i+v_j)_+=(t-y_i-y_j)_+`.
Subtract the m diagonal hinges and divide by two to recover H_t. Sorting,
active counts, and prefix first moments are thus an ordinary instantiation
of the documented pairwise-ranking hinge operation, with O(m) preparation
and diagonal corrections. The four partition/cell passes add O(K) memberships
and the proved signed count identity. Negative coefficients require the
specific cancellation/tie argument above, not a new hinge algorithm.

Under the evidence policy: published primitives and their explicit reduction
narrow the candidate; independently constructed endpoint/subset controls
test advantage but are not historical prior art. A lead implementation that
imports the candidate's occupied-cell decomposition tests reproducibility,
not independent priority. Remaining questions are whether the exact
composition merits more than an application lemma, whether preselected
degree/subset controls capture the useful gains, and whether bounded search
has useful native quality/resource tradeoffs. Those remain open.

## 7. Independent Executable Exact Checker

Python standard library only. This block is self-contained and does not read
or import another review, probe, receipt, or data file. Direct dense sums are
tiny reference oracles, not native experiments. The implemented group oracle
uses local indexed lists, prefix sums, and a monotone pointer.

The value/subgradient tests do not require feasible r; certificate/optimum
comparisons below use only enumerated feasible residuals. The optional
theorem sanity checks cover all independent K-edge systems with right-hand
sides in {-1,0,1} for K=3,4, plus eight projected schedules on the witnesses.
Those finite checks supplement, rather than replace, the proof for general K.

The exact capped optimum checker enumerates polytope vertices: for K>=3 the
complete loopless incidence matrix has full row rank K. At an extreme point,
columns for non-bound variables are independent; extend them to a K-column
basis, set all other variables to 0 or D, and solve exactly. This covers every
vertex, including fractional and degenerate vertices. K<=2 is handled
directly. Linear extrema of the nonempty compact polytope occur at vertices.

Replay from the repository root:

```sh
awk '/^```python$/{copy=1;next} /^```$/{if(copy){exit}} copy' research_algorithms_20260920/Communities-Implicit-Dual-Review.md | python3 -B
```

```python
from fractions import Fraction as F
from itertools import combinations, product
from random import Random


def enumerate_partition_label_vectors(n, prefix=()):
    if len(prefix) == n:
        yield prefix
    else:
        for label in range(max(prefix, default=-1) + 2):
            yield from enumerate_partition_label_vectors(n, prefix + (label,))


def collect_partition_group_indices(labels):
    groups = {}
    for i, label in enumerate(labels):
        groups.setdefault(label, []).append(i)
    return list(groups.values())


def evaluate_sorted_group_hinges(y, ids, threshold):
    ordered = sorted(ids, key=lambda i: y[i])
    prefix = [F(0)]
    for i in ordered:
        prefix.append(prefix[-1] + y[i])
    q, twice, degrees = len(ordered), F(0), []
    for i in ordered:
        while q and y[ordered[q - 1]] + y[i] >= threshold:
            q -= 1
        diagonal = int(2*y[i] < threshold)
        degrees.append((i, q - diagonal))
        twice += q*(threshold-y[i]) - prefix[q]
        twice -= diagonal*(threshold-2*y[i])
    return twice / 2, degrees


def evaluate_implicit_dual_certificate(r, a, b, cap, y):
    n, total, degrees = len(r), F(0), [0]*len(r)
    passes = [([list(range(n))], ((0, 1),)),
              (collect_partition_group_indices(a), ((1, 1), (0, -1))),
              (collect_partition_group_indices(b), ((-1, 1), (0, -1))),
              (collect_partition_group_indices(tuple(zip(a, b))),
               ((1, -1), (-1, -1), (0, 2)))]
    for groups, terms in passes:
        for ids in groups:
            for threshold, sign in terms:
                value, counts = evaluate_sorted_group_hinges(y, ids, threshold)
                total += sign*value
                for i, count in counts:
                    degrees[i] += sign*count
    assert all(0 <= count <= max(0, n-1) for count in degrees)
    return (sum((ri*yi for ri, yi in zip(r, y)), F(0)) + cap*total,
            tuple(ri-cap*count for ri, count in zip(r, degrees)))


def evaluate_explicit_dual_certificate(r, a, b, cap, y):
    value = sum((ri*yi for ri, yi in zip(r, y)), F(0))
    gradient = list(r)
    for i, j in combinations(range(len(r)), 2):
        hinge = int(a[i] == a[j])-int(b[i] == b[j])-y[i]-y[j]
        value += cap*max(F(0), hinge)
        if hinge > 0:
            gradient[i] -= cap
            gradient[j] -= cap
    return value, tuple(gradient)


def solve_square_system_exact(matrix, rhs):
    n = len(rhs)
    rows = [[F(v) for v in row] + [F(v)] for row, v in zip(matrix, rhs)]
    for col in range(n):
        pivot = next((i for i in range(col, n) if rows[i][col]), None)
        if pivot is None:
            return None
        rows[col], rows[pivot] = rows[pivot], rows[col]
        scale = rows[col][col]
        rows[col] = [v/scale for v in rows[col]]
        for i in range(n):
            if i != col:
                scale = rows[i][col]
                rows[i] = [v-scale*w for v, w in zip(rows[i], rows[col])]
    return tuple(row[-1] for row in rows)


def enumerate_capped_polytope_vertices(r, cap):
    n = len(r)
    edges = tuple(combinations(range(n), 2))
    if n <= 1:
        return edges, {()} if not any(r) else set()
    if n == 2:
        return edges, {(r[0],)} if r[0] == r[1] and 0 <= r[0] <= cap else set()
    vertices = set()
    for basis in combinations(range(len(edges)), n):
        matrix = [[int(i in edges[e]) for e in basis] for i in range(n)]
        if solve_square_system_exact(matrix, (0,)*n) is None:
            continue
        fixed = [e for e in range(len(edges)) if e not in basis]
        for bits in product((0, 1), repeat=len(fixed)):
            x, rhs = [F(0)]*len(edges), list(r)
            for e, bit in zip(fixed, bits):
                x[e] = cap*bit
                for i in edges[e]:
                    rhs[i] -= x[e]
            solved = solve_square_system_exact(matrix, rhs)
            if all(0 <= v <= cap for v in solved):
                for e, v in zip(basis, solved):
                    x[e] = v
                assert all(sum(v for v, pair in zip(x, edges) if i in pair)
                           == r[i] for i in range(n))
                vertices.add(tuple(x))
    return edges, vertices


def calculate_scalar_cap_interval(r, labels, cap):
    mass, t = sum(r)/2, [min(v, cap) for v in r]
    alpha, q = [], []
    for ids in collect_partition_group_indices(labels):
        outside = [i for i in range(len(r)) if i not in ids]
        tc, ranked = sum(t[i] for i in ids), sorted((r[i] for i in ids), reverse=True)
        low = sum(max(F(0), r[i]-tc+t[i]) for i in ids)
        for h in range(1, len(ids)+1):
            low = max(low, sum(ranked[:h])-cap*h*(h-1)
                      - sum(min(v, h*cap) for v in ranked[h:]))
        alpha.append(low)
        q.append(min(2*mass-sum(ranked),
                     sum(min(r[i], sum(t)-tc) for i in ids),
                     sum(min(r[i], tc) for i in outside)))
    return (mass-min(mass, sum(q)/2, sum(q)-max(q, default=F(0))),
            mass-max(max(alpha, default=F(0)), sum(alpha)/2))


def calculate_endpoint_row_upper(r, a, b, cap):
    t, allowed, forced = [min(v, cap) for v in r], [], []
    for i in range(len(r)):
        p = sum(t[j] for j in range(len(r)) if a[i] == a[j] and b[i] != b[j])
        neg = sum(t[j] for j in range(len(r)) if b[i] == b[j] and a[i] != a[j])
        neutral = sum(t)-t[i]-p-neg
        allowed.append(min(r[i], p))
        forced.append(max(F(0), r[i]-p-neutral))
    endpoint = min(sum(allowed)/2, sum(allowed)-max(allowed, default=F(0)))
    endpoint -= max(sum(forced)/2, max(forced, default=F(0)))
    return endpoint, (sum(allowed)-sum(forced))/2, sum(allowed)/2


def partition_refines_other_partition(a, b):
    return all(a[i] != a[j] or b[i] == b[j]
               for i, j in combinations(range(len(a)), 2))


def construct_partition_meet_join(a, b):
    joined = list(range(len(a)))
    for i, j in combinations(range(len(a)), 2):
        if a[i] == a[j] or b[i] == b[j]:
            old, new = joined[j], joined[i]
            joined = [new if v == old else v for v in joined]
    return tuple(zip(a, b)), tuple(joined)


def calculate_partition_internal_mass(x, edges, labels):
    return sum((v for v, (i, j) in zip(x, edges) if labels[i] == labels[j]), F(0))


def calculate_strong_comparator_interval(r, a, b, cap, oracle=None):
    meet, join = construct_partition_meet_join(a, b)
    partitions = (meet, a, b, join)
    bounds = []
    for p in partitions:
        if oracle is None:
            bounds.append(calculate_scalar_cap_interval(r, p, cap))
        else:
            edges, vertices = oracle
            vals = [calculate_partition_internal_mass(x, edges, p) for x in vertices]
            bounds.append((min(vals), max(vals)))
    closed = []
    for p in partitions:
        lo = max(lo for q, (lo, hi) in zip(partitions, bounds)
                 if partition_refines_other_partition(q, p))
        hi = min(hi for q, (lo, hi) in zip(partitions, bounds)
                 if partition_refines_other_partition(p, q))
        closed.append((lo, hi))
    (lc, uc), (la, ua), (lb, ub), (lj, uj) = closed
    lo = max(la-ub, lc-ub, la-uj, -calculate_endpoint_row_upper(r, b, a, cap)[2])
    hi = min(ua-lb, ua-lc, uj-lb, calculate_endpoint_row_upper(r, a, b, cap)[2])
    if partition_refines_other_partition(a, b):
        hi = min(hi, F(0))
    if partition_refines_other_partition(b, a):
        lo = max(lo, F(0))
    return lo, hi


def check_dual_oracle_case(r, a, b, cap, y, z):
    value, gradient = evaluate_implicit_dual_certificate(r, a, b, cap, y)
    assert (value, gradient) == evaluate_explicit_dual_certificate(r, a, b, cap, y)
    other = evaluate_explicit_dual_certificate(r, a, b, cap, z)[0]
    assert other >= value + sum(g*(v-u) for g, u, v in zip(gradient, y, z))


group_cases = 0
for n in range(6):
    for y in product((F(-1), F(-1, 2), F(0), F(1, 2), F(1)), repeat=n):
        for threshold in (-1, 0, 1):
            value, degree = evaluate_sorted_group_hinges(y, list(range(n)), threshold)
            assert value == sum((max(F(0), threshold-y[i]-y[j])
                                 for i, j in combinations(range(n), 2)), F(0))
            assert dict(degree) == {i: sum(y[i]+y[j] < threshold
                                          for j in range(n) if j != i) for i in range(n)}
            group_cases += 1
print('group_hinge_and_degree_cases:', group_cases)

identity_cases = 0
for n in range(5):
    parts = tuple(enumerate_partition_label_vectors(n))
    r = tuple(F(i, 3) for i in range(n))
    for a, b in product(parts, repeat=2):
        for y in product((F(-1), F(0), F(1)), repeat=n):
            z = tuple(-v+F(1, 3) for v in reversed(y))
            check_dual_oracle_case(r, a, b, F(2, 3), y, z)
            identity_cases += 1
rng = Random(60921)
for case in range(200):
    n = rng.randrange(9)
    r = tuple(F(rng.randrange(8), 3) for _ in range(n))
    a = tuple(rng.randrange(4) for _ in range(n))
    b = tuple(rng.randrange(4) for _ in range(n))
    y = tuple(F(rng.randrange(-7, 8), 3) for _ in range(n))
    z = tuple(F(rng.randrange(-7, 8), 4) for _ in range(n))
    check_dual_oracle_case(r, a, b, F(case % 4, 3), y, z)
print('partition_identity_and_subgradient_cases:', identity_cases, '+ 200 rational cases')

tie_r = (F(1), F(1))
tie_y, tie_z = (F(1, 2),)*2, (F(2, 5),)*2
fy, good = evaluate_implicit_dual_certificate(tie_r, (0, 0), (0, 0), F(1), tie_y)
fz = evaluate_explicit_dual_certificate(tie_r, (0, 0), (0, 0), F(1), tie_z)[0]
assert good == (1, 1) and fz < fy
print('inconsistent_tie_subgradient_rejected:', fy, fz)

basis_cases = 0
for n in (3, 4):
    edges = tuple(combinations(range(n), 2))
    for basis in combinations(edges, n):
        matrix = [[int(i in pair) for i in range(n)] for pair in basis]
        if solve_square_system_exact(matrix, (0,)*n) is None:
            continue
        for rhs in product((-1, 0, 1), repeat=n):
            solved = solve_square_system_exact(matrix, rhs)
            assert all((2*v).denominator == 1 and abs(v) <= n for v in solved)
            basis_cases += 1
print('half_integral_bounded_basis_cases:', basis_cases)

projected_cases = 0
for raw_r, a, b, y, expected in [
        ((1, 2, 1, 1), (0, 1, 1, 2), (0, 1, 2, 0),
         (F(-1, 2), F(1, 2), F(1, 2), F(-1, 2)), (F(1, 2), F(1, 2))),
        ((1, 3, 3, 2, 2), (0, 1, 2, 0, 3), (0, 1, 1, 2, 0),
         (F(1, 2), F(-1, 2), F(-1, 2), F(1, 2), F(-1, 2)), (F(-3, 2), F(-1, 2)))]:
    r, cap = tuple(map(F, raw_r)), F(1)
    edges, vertices = enumerate_capped_polytope_vertices(r, cap)
    scores = [calculate_partition_internal_mass(x, edges, a)
              - calculate_partition_internal_mass(x, edges, b) for x in vertices]
    exact = min(scores), max(scores)
    endpoint = (-calculate_endpoint_row_upper(r, b, a, cap)[0],
                calculate_endpoint_row_upper(r, a, b, cap)[0])
    strong = calculate_strong_comparator_interval(r, a, b, cap)
    oracle = calculate_strong_comparator_interval(r, a, b, cap, (edges, vertices))
    dual = evaluate_implicit_dual_certificate(r, a, b, cap, y)[0]
    assert exact == expected and dual == exact[1] < min(endpoint[1], strong[1], oracle[1])
    assert max(endpoint[0], strong[0], oracle[0]) <= exact[0]
    assert not partition_refines_other_partition(a, b)
    assert not partition_refines_other_partition(b, a)
    marginal = []
    for p in (a, b):
        values = [calculate_partition_internal_mass(x, edges, p) for x in vertices]
        marginal.append((min(values), max(values)))
    assert marginal == ([(F(1, 2), F(1)), (F(0), F(1, 2))] if len(r) == 4 else
                        [(F(0), F(1, 2)), (F(1, 2), F(3, 2))])
    print('witness', len(r), 'vertices', len(vertices), 'endpoint', tuple(map(str, endpoint)),
          'strong', tuple(map(str, strong)), 'oracle', tuple(map(str, oracle)),
          'exact', tuple(map(str, exact)), 'dual', dual)
    for scale in (F(1, 3), F(2)):
        assert evaluate_implicit_dual_certificate(tuple(scale*v for v in r), a, b,
                                                  scale, y)[0] == scale*dual
    n = len(r)
    for root in (1, 2, 3, 4):
        rounds, step, current, values = root*root, F(n, (n-1)*root), (F(0),)*n, []
        for iteration in range(rounds):
            value, gradient = evaluate_implicit_dual_certificate(r, a, b, cap, current)
            values.append(value)
            assert sum(g*g for g in gradient) <= n*(n-1)**2
            updated = tuple(min(F(n), max(F(-n), v-step*g))
                            for v, g in zip(current, gradient))
            old_distance = sum((v-w)**2 for v, w in zip(current, y))
            new_distance = sum((v-w)**2 for v, w in zip(updated, y))
            assert new_distance <= old_distance-2*step*(value-dual) + step**2*sum(g*g for g in gradient)
            current = updated
        assert F(0) <= min(values)-dual <= F(n*n*(n-1), root)
        projected_cases += 1
print('projected_schedule_recurrence_checks:', projected_cases)

r, a, b, y = tuple(map(F, (1, 2, 1, 1))), (0, 1, 1, 2), (0, 1, 2, 0), (F(0),)*4
trajectory = []
for iteration in range(7):
    value, gradient = evaluate_implicit_dual_certificate(r, a, b, F(1), y)
    trajectory.append(value)
    if iteration < 6:
        y = tuple(v-F(1, 2)*g for v, g in zip(y, gradient))
assert trajectory == list(map(F, (1,))) + [F(5, 2), F(1), F(2), F(3, 2), F(1), F(1, 2)]
assert min(trajectory) == F(1, 2)
assert evaluate_implicit_dual_certificate(r, b, a, F(1), tuple(-v for v in y))[0] == -F(1, 2)
print('bounded_search_values:', ','.join(map(str, trajectory)))
assert enumerate_capped_polytope_vertices((F(3), F(3), F(1), F(1)), F(1))[1] == set()
assert evaluate_implicit_dual_certificate((F(0),)*3, (0, 0, 0), (0, 1, 2), F(0), (F(-2),)*3)[0] == 0
print('scaling_reverse_zero_cap_and_infeasibility_checks: passed')
print('ALL EXACT CHECKS PASSED')
```

### Execution Record

Final full execution, including the optional theorem's finite checks,
exited 0 with the following exact results:

```text
group_hinge_and_degree_cases: 11718
partition_identity_and_subgradient_cases: 18940 + 200 rational cases
inconsistent_tie_subgradient_rejected: 1 4/5
half_integral_bounded_basis_cases: 999
witness 4 vertices 3 endpoint ('-1', '1') strong ('0', '1') oracle ('0', '1') exact ('1/2', '1/2') dual 1/2
witness 5 vertices 9 endpoint ('-2', '1') strong ('-3/2', '1/2') oracle ('-3/2', '0') exact ('-3/2', '-1/2') dual -1/2
projected_schedule_recurrence_checks: 8
bounded_search_values: 1,5/2,1,2,3/2,1,1/2
scaling_reverse_zero_cap_and_infeasibility_checks: passed
ALL EXACT CHECKS PASSED
```

## 8. Handoff Boundary

The proofs support an O(K log K)-arithmetic implicit oracle, safe
best-so-far intersections, and the optional bounded-record approximation
theorem. They do not require changing any frozen result or imply a gain on
the saturated 140/140 native gate.
Lead-side validation should preserve strict ties, diagonal exclusion,
occupied-cell cancellation, free potentials, two-orientation signs, certified
rounding, total search work, and the stronger comparator intersection.
This review does not duplicate or certify that implementation.
