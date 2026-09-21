# Boolean Two-Membership PageRank

Date: 2026-09-21. Independent A01 domain-extension investigation; A02 is outside this note. Only this file is owned. No production implementation, demand evidence, novelty priority, speed, or physical-RAM claim.

Lead follow-through: a [separate streamed implementation and paired experiment](PageRank-Boolean-Streaming-Evidence.md)
now exists. This manuscript's exact checker remains a dense mathematical
oracle, not the numerical implementation. The experiment includes measured
wins, ties and losses, a repaired P comparator, and explicit certificate/range
refusals. It does not establish a physical-RAM reduction or paper readiness.

## Premise Check

**Finding:** the proposed formulas are correct for a loop-free Boolean union of native cliques with at most two distinct memberships per original vertex. An exact F-coordinate reduction exists even with Theta(F^2) nonempty pair signatures. Its coefficients can be streamed without retaining pair-class iterates. The reduction is a standard block-Schur/Woodbury specialization; a generic control reproduces it exactly. The domain extension survives, but a claim of new generic factorization does not.

The [factor-defect note](Factor-Defect-PageRank.md) and [native-incidence experiment](PageRank-Native-Incidence-Evidence.md) were read first. The latter uses **weighted shared-membership counts**, not Boolean adjacency. Its data allow far more than two memberships per vertex. Neither its eligibility, convergence constants nor timings transfer here.

Expert lenses used: Boolean graph semantics; quotient/Schur linear algebra; numerical certification; adversarial resource/prior-art comparison. The alternatives are an n-coordinate original solve, a P-coordinate signature quotient, and an F-coordinate elimination of that quotient. All receive the same native representation and stationary-answer contract.

### Concrete Native Hypothesis

Let each transaction/event be a vertex, with memberships `(customer, customer_id)` and `(merchant, merchant_id)`. Two distinct events are adjacent exactly when they share at least one membership. Repeated events for the same pair create a large class H, but their mutual edge still has weight **one**, not two. Native event records supply the incidence directly: no discovery of a clique cover from arbitrary adjacency is assumed. Sorting, counting, ID validation and personalization joins are still paid work.

For sender/recipient data, choose semantics explicitly: role-tagged IDs mean matching in the same role; untagged party IDs also match a sender against another event's recipient. A self-transfer in the untagged model has one distinct membership. These are different graphs. The hypothesis supplies neither demand nor prevalence evidence and says nothing about generic Neo4j or dependency graphs. F can remain near n; repeated events are not proof that the F solve beats the P quotient.

## Exact Contract

There are n original IDs, a deduplicated membership set sigma(v) of size 0, 1 or 2, and

```text
A_uv = 1{u != v and sigma(u) intersects sigma(v)}.
0 <= alpha < 1; p >= 0; sum_v p_v = 1.
d_v = sum_u A_uv; Dinv_vv = 1/d_v if d_v>0, otherwise 0.
T = A Dinv + p z^T, with z_v = 1{d_v=0}.
x* = (1-alpha)p + alpha T x*.
```

Duplicate memberships are canonicalized as a set, not accumulated. Duplicate event records with the **same original ID** must be resolved/rejected by the source contract; distinct IDs with the same signature remain distinct vertices. Output covers the entire original universe, including IDs with no memberships. An empty universe returns an empty result. Alpha=0 returns p; alpha=1 is excluded. This is exact stationary PageRank semantics, not equality with a finite-iteration product API.

### Isolates First

Symmetry makes every dangling vertex an isolate. Write p_Z for their total personalization and zeta for their PageRank mass. They receive no adjacency inflow:

```text
zeta = (1-alpha)p_Z + alpha*p_Z*zeta
gamma = (1-alpha)/(1-alpha*p_Z)
b_v = gamma*p_v
x_v = b_v on isolates; (D-alpha*A)q=b on active vertices; x_v=d_v*q_v.
```

Thus zeta=gamma*p_Z, including p_Z=0 and p_Z=1. If all vertices are isolated, gamma=1 and x=p: no division by zero and no linear solve. If p is supported entirely on isolates while active components exist, their solution is zero. The formula would fail for a directed dangling vertex receiving incoming edges.

Remove isolates from solver incidences and remove resulting empty factors. Define **N_a as the number of active members** of group a from here onward. A group containing an isolate had size one, so removing it cannot change any active degree. A size-one group containing an active vertex can be retained; optionally pruning such redundant factors requires reclassification. No q_v is formed for d_v=0.

## Class Elimination

Let H denote the active class of one nonempty signature, h=|H|, B_H=sum_(v in H) b_v, Q_H=sum_(v in H) q_v, and S_a=sum_(v in a) q_v. Counts and degrees are exact integers.

| Signature | Degree d_H | Neighbor sum `(Aq)_v` |
| --- | --- | --- |
| singleton `{a}` | `N_a-1` | `S_a-q_v` |
| pair `{a,b}` | `N_a+N_b-h-1` | `S_a+S_b-Q_H-q_v` |

The pair subtraction removes the extra counting of H, then removes v itself. Vertices outside H cannot share both a and b under the two-membership restriction. Consequently

```text
singleton: (d+alpha)q_v = b_v + alpha*S_a
           Q_H = (B_H + alpha*h*S_a)/(d+alpha)
pair:      (d+alpha)q_v = b_v + alpha*(S_a+S_b-Q_H)
           Q_H = (B_H + alpha*h*(S_a+S_b))/(d+alpha+alpha*h).
```

Let u_H be the F-vector indicating the one/two groups in H, k_H=|sigma(H)|, ell_H=1+(k_H-1)h, and t_H=d_H+alpha*ell_H. On active classes t_H>0. Both cases become

```text
Q_H(S) = (B_H + alpha*h*u_H^T S)/t_H
c = sum_H (B_H/t_H) u_H
M = alpha * sum_H (h/t_H) u_H u_H^T
C = I-M;              C S = c.
```

One pass over class records computes `c+MS`: calculate one Q_H, scatter it to its one/two groups, discard it. M is symmetric, entrywise nonnegative and positive semidefinite, despite the local inverse below having negative off-diagonals. Do not materialize M: pair records can occupy Theta(F^2) space. The pair-class graph on F group vertices is not necessarily sparse; the admitted operation is a sparse-record stream with P actual classes.

For any S, not just the solution, the exact local reconstruction is

```text
q_v = Q_H(S)/h + (b_v-B_H/h)/(d_H+alpha).
```

This retains arbitrary within-class personalization. In particular `x_u-x_v = d_H*(b_u-b_v)/(d_H+alpha)` within a class. Dividing class mass equally loses that term.

### P Coordinates Versus F Coordinates

Let P be the number of active nonempty signatures, not the transition matrix T. Signature classes are true-twin cliques with identical outside neighbors and equal degrees. Their class masses X_H=sum_(v in H)x_v=d_H Q_H obey the ordinary exact lumped chain: from source class G to a distinct intersecting H, transition probability is h_H/d_G; within G it is (h_G-1)/d_G. All other active transitions are zero. Aggregate personalization by class, handle dangling mass, then restore the individual contrast above. The aggregate chain works for arbitrary p; equal per-vertex lifting does not.

The nonnegative quotient equations can also be written

```text
X_H = B_H + alpha*((h_H-1)*X_H/d_H
                 + h_H*sum_(G != H, sigma(G) intersects sigma(H)) X_G/d_G).
```

An ordinary quotient implementation needs P changing coordinates, plus group gathers to avoid expanding class-to-class adjacency. It can keep P scores in files instead of RAM, paying their repeated reads/writes. This is a required control, not an impossibility result for quotient methods. Eliminating those class coordinates yields exactly C above; their disappearance is not just an n-to-P grouping claim.

P<=min(n_active, F+binom(F,2)). A complete underlying group-pair graph has P=binom(F,2); a customer/merchant split gives at most F_customer*F_merchant pair classes. Thus P can be Theta(F^2). Conversely one repeated pair has P=1 and F=2: the quotient is smaller and its scalar equation has a closed form. F is not always the smallest representation.

### Known Block Control

Let U be active vertex-to-group incidence, and J_H the all-ones block on class H. Then

```text
A = U U^T - I - sum_(pair H) J_H
W = D + alpha*I + alpha*sum_(pair H) J_H
D-alpha*A = W-alpha*U U^T
W_H = (d_H+alpha) I_h + alpha*(k_H-1) J_h.
```

The inverse on the class-constant subspace has eigenvalue 1/t_H and on its zero-sum contrasts has eigenvalue 1/(d_H+alpha). Applying ordinary Schur elimination gives

```text
C = I-alpha*U^T W^-1 U; c = U^T W^-1 b
q(S) = W^-1(b+alpha*U S).
```

This is precisely the proposed class formula, not a separate competing theorem. The checker independently assembles W and solves its dense blocks to confirm coefficient equality. Block Woodbury, equitable partitioning and resolving the true-twin contrast already explain the mechanism.

## Symmetry And Convergence

On active vertices, for any nonzero real v,

```text
v^T(D-alpha*A)v = (1-alpha)*sum_i d_i*v_i^2
                 + alpha*sum_{unordered edges {i,j}} (v_i-v_j)^2 > 0.
```

W is positive definite, so its block Schur complement C is symmetric positive definite. Alternatively the bound below gives positivity directly. Dependent or identical columns of U are harmless: C still has an identity term. C is a symmetric nonsingular M-matrix, and C^-1 is nonnegative. These are exact-arithmetic statements, not unconditional floating-point stability guarantees.

### Boolean-Specific Bound

Use N=(N_a)>0. For each class, `u_H^T N=d_H+ell_H`. Hence

```text
(M N)_a = sum_(H incident a) h * alpha*(d_H+ell_H)/t_H
        <= beta*N_a
beta = max_H alpha*(d_H+ell_H)/(d_H+alpha*ell_H) < 1.
```

M is symmetric, so this is also `N^T M <= beta*N^T`. It contracts both `||S||_(N,1)=sum_a N_a|S_a|` and `||S||_(N,infinity)=max_a |S_a|/N_a`. This distinction matters: unweighted row sums can exceed one.

For singleton classes ell/d<=1. For pairs, d>=h-1: when h>=2, `(h+1)/d <= (h+1)/(h-1) <=3`; when h=1 and d>0, ell/d<=2. Therefore

```text
beta <= 4*alpha/(1+3*alpha) < 1.
eigenvalues(C) are in [1-beta, 1]
condition_2(C) <= 1/(1-beta) <= (1+3*alpha)/(1-alpha).
```

The bound is sharp for two vertices with the same pair signature: h=2,d=1,N_a=N_b=2 and rho(M)=4*alpha/(1+3*alpha). Redundant-factor simplification can improve this particular representation; sharpness does not imply a graph-intrinsic lower bound. At alpha=17/20 the Boolean bound is 68/71, exceeding 34/37. In particular the earlier `2*alpha/(1+alpha)` guarantee for an admitted weighted/released representation cannot simply be transferred to this different operator and representation.

**The restriction is membership count <=2, not graph degree <=2.** Degrees can reach n-1. Degree 0 must be removed; degree 1 includes the sharp h=2 example; degree 2 admits h<=3 and needs no special exception. At three or more memberships, different signatures can overlap twice and the correction is no longer confined to diagonal class blocks. For signatures `{a,b,c}` and `{a,b}`, U U^T has off-diagonal 2 while A has 1. The claimed W identity and the degree shortcut fail. This note establishes no analogous F-only theorem there.

### Positive Grounded Form

Directly forming `1-M_aa` can subtract nearly equal numbers. Let R_N=diag(N), set S=R_N y and define

```text
L = R_N C R_N
g_a = (1-alpha)*N_a*sum_(H incident a) h*d_H/t_H > 0
w_ab = alpha*h_ab*N_a*N_b/t_ab    for each pair class
L = diag(g) + sum_(pair {a,b}) w_ab*(e_a-e_b)(e_a-e_b)^T
L y = R_N c.
```

All ground and edge coefficients are nonnegative, assembled by addition; no `1-M_aa` cancellation is needed. This follows by computing `N_a*(C N)_a=g_a`, and matching the off-diagonals. The energy is `sum_a g_a*y_a^2 + sum_ab w_ab*(y_a-y_b)^2`, strictly positive. A class-stream matvec uses edge differences and ground terms. It is symmetric strictly diagonally dominant with a positive RHS, not a claim that every signed matvec is subtraction-free.

Ordinary CG applies to C or L; in exact arithmetic it terminates in at most F steps. For C, the standard energy-error estimate is `2*((sqrt(kappa)-1)/(sqrt(kappa)+1))^j` times the initial error. Scaling to L can worsen conditioning by at most `(N_max/N_min)^2`; do not transfer C's bound unmodified. Preconditioning L by R_N^2 returns C. Ordinary diagonal/Jacobi preconditioning is another paid control.

Unpreconditioned fixed point `S_next=c+MS` also converges. Ordinary Jacobi on C has linear part `diag(1-M_aa)^-1*(M-diag(M_aa))`, whose N-weighted infinity ratios are at most `(beta-M_aa)/(1-M_aa)<=beta`. None of these upper bounds promises fewer passes or faster complete output than the quotient or original solver. Check the zero initial state's certificate before taking a step; special scalar/disconnected cases may need no iterative solve. CG's recursive residual is only a stopping screen until recomputed and certified.

## Original Answer Certificate

For any S and its exact local reconstruction q(S), let `delta=U^T q(S)-S=c+MS-S`. With analytic isolate values, the original PageRank residual is exactly

```text
r = b+alpha*Aq-Dq = alpha*U*delta    on active vertices; r=0 on isolates.
||x(S)-x*||_1 <= ||r||_1/(1-alpha)
              = alpha/(1-alpha)*sum_H h*|u_H^T delta|
              <= alpha/(1-alpha)*sum_a N_a*|delta_a|.
```

Proof: `Wq=b+alpha*US`; substituting `D-alpha*A=W-alpha*UU^T` gives the identity. The stochastic resolvent norm is bounded by 1/(1-alpha). The tighter class sum needs another class scan, not an n-vector. Retain the S and next vectors or a separately charged residual vector. Certify and return the reconstruction of the same S, not an unchecked next state. This matches the proposed certificate with **active** N, and remains valid for signed intermediate scores.

### Reconstruction And Cancellation

The mean-plus-contrast formula is good algebra but can cancel. For a two-vertex pair class, p=(1,0), alpha=2^-60, the smaller exact score is `1/(2^60+1)`. Binary64 evaluation as `1/2 + (0-1/2)/(1+alpha)` yields zero. This example concerns tiny-score relative accuracy, not a large absolute-error failure.

A better equivalent evaluation is

```text
s_H = u_H^T S
R_H = s_H - (k_H-1)*Q_H(S)
q_v = (b_v + alpha*R_H)/(d_H+alpha).
```

At the true nonnegative solution, singleton R_H=S_a>=0; for a pair, `s_H=2Q_H+E_H`, where E_H is the nonnegative mass in all other adjacent classes, so R_H=Q_H+E_H>=0 and Q_H<=s_H/2. The pair subtraction is then well-conditioned: `(s_H+Q_H)/(s_H-Q_H)<=3` when nonzero. This avoids the mean/contrast cancellation without an n-vector. It is **not** an unconditional positivity property of an approximate state: S=0 for the same two vertices at alpha=1/2 reconstructs `(4/15,-1/15)`. Even positive factor iteration does not ensure positive original scores at every step.

Use validated/adequate-precision arithmetic, recompute residuals, and check the published values. Near alpha=1 the problem is genuinely harder; positive assembly does not remove conditioning. Degrees and counts must be formed exactly before conversion, including overflow checks. The finite rational checker is not a tested production interval implementation.

### Certificate For Actual Output Bytes

The reduced residual does not automatically certify rounded, reordered or truncated output. Treat each published binary64 number as its exact rational value (or enclose its exact bits using directed arithmetic). A proposed bounded-state verification schedule is:

1. Validate the original-ID manifest, uniqueness, completeness, query p and class/degree metadata against the frozen source. Arrange output in class order with a paid join/sort if needed.
2. Scan class-grouped published rows: form q_v=x_v/d_v for active vertices, accumulate interval group sums S'_a and actual dangling mass zeta'. Emit each **published-score** class total Q'_H into a sequential scratch stream. Only one current class total and O(F) accumulators are resident.
3. Rescan rows joined to that scratch stream. For active v in H, enclose `r_v=(1-alpha)p_v+alpha*(p_v*zeta'+u_H^T S'-(k_H-1)Q'_H-q_v)-x_v`; for isolates, omit adjacency terms. Sum absolute residual upper bounds upward and divide upward by 1-alpha.

All subtractions, including self exclusion, must be enclosed. This uses a paid O(P) changing scratch file, not P resident Q values. It uses the actual published dangling mass, not an assumed gamma after rounding. Accept only when the bound meets the requested absolute L1 tolerance and the ID/finite-value/output contract passes. Normalizing or clipping scores changes the output and requires recertification. Large cancellation intervals may require higher precision or an additional positive exclusion-sum schedule, both charged. Hashes identify inputs/outputs; they do not prove that degrees encode the intended graph.

## Resource Accounting

Let L_C be prepared class-record bytes, L_V the original-ID/membership/degree/personalization row bytes, P the active signature count, F the active factor count, j the number of matrix/iteration passes, and B the explicitly bounded scan buffers. Arithmetic costs below count scalar operations, not arbitrary-precision bit complexity.

| Item | Required charge |
| --- | --- |
| Fixed-point changing state | Two F-vectors: 16F bytes for f64; accumulate c from the B_H stream rather than store it if desired |
| CG changing/state payload | For example S,r,direction,matvec plus RHS: 40F f64 bytes; preconditioner, extra vector, recomputed residual and coefficient arrays are additional |
| Counts and certificate state | N_a and any resident grounds/diagonals each cost their actual width times F; intervals/compensation can require multiple larger arrays |
| General working reservation | `scalar_width*k*F + integer_width*m*F + B + dictionaries + numerical_workspace + runtime`; specify k,m for the chosen solver/certifier, not just O(F) |
| Class artifact | O(P) records with one/two group IDs, h,d and query-specific B_H; can be Theta(F^2), never silently resident |
| Source and metadata | O(n) original IDs and up to 2n membership incidences, class association, personalization, degree/class metadata; store or reproduce with paid joins |
| Query preparation | Read arbitrary p, compute p_Z and per-class sums B_H; O(n+P+F) work once class-ordered, plus required sort/join traffic and scratch |
| Each solve pass | O(P+F) work and L_C reads; no full adjacency, P-vector or n-vector required in this particular schedule |
| Full publication | O(n+P) reconstruction/read work and n score/ID records written; preserve every ID. Required original-ID ordering needs a paid permutation/sort if rows are class-ordered |
| Rounded certificate | Two row/output scans plus writing/reading O(P) scratch totals and any ordering joins; O(F) numerical accumulators plus buffers |
| Dense F control | F^2 matrix entries plus factorization/workspace; never the matrix-free reservation |
| P-quotient control | P changing values, possibly file-backed, and factor gathers; supply the same native factors, fast solvers and output certificate |
| Builder/lifecycle | Canonicalization, external sorting, counts/degree validation, B_H joins, source acquisition, refreshed snapshots, pinned generations and scratch have independent budgets |

Total traffic includes preparation, j*L_C, full publication, both certificate scans and class-total scratch; arbitrary personalization and full output retain an Omega(n) floor. A source with F near n or a dense pair stream may erase the state advantage. No benchmark, disk throughput, RSS reduction or complete physical-RAM limit was measured. The checker below intentionally retains dense data and proves none of those engineering properties. Exact rational outputs can require unbounded per-scalar bit growth; f64 byte formulas describe a numerical implementation with a residual contract, not constant-byte exact rationals.

Those statements describe this theoretical checker. In the separate measured
prototype, raw supplied F includes inactive factors, C includes degree-zero
classes, all class scans visit C records, and certificate scratch contains C
records rather than only P active records. Its accounting is therefore O(F)
solver/factor state plus O(C) disk totals and paid original-row output; it does
not silently substitute the smaller active counts in measured claims.

## Focused Prior Art

These are specific primary-source comparisons, not an exhaustive novelty survey. Mathematical specializations and differences in the right column are this note's deductions.

| Primary source and inspected location | What blocks a broad novelty claim; what is different here |
| --- | --- |
| [Evans and Lambiotte, Line Graphs, Link Partitions and Overlapping Communities (2009), Sections II and III, equations (8)-(9)](https://arxiv.org/pdf/0903.2181) | Line-graph incidence projection and link-link random walks already supply the underlying construction. The paper explicitly assumes a simple underlying network; distinct edges then cannot share both endpoints. Repeated events correspond to parallel edges, where its raw incidence product counts two and Boolean collapse needs the class correction. Its differently weighted link-node-link walk is not the target here. |
| [Boldi, Lonati, Santini and Vigna, Graph Fibrations, Graph Isomorphism, and PageRank (2006), Theorem 5.2, Section 5.1 and Section 6.2](https://www.numdam.org/item/10.1051/ita:2006004.pdf) | Exact structural PageRank reduction is established. Their fibrewise-equal lifting uses a fibrewise-constant preference vector. Here arbitrary within-signature preferences are handled by the explicitly resolved zero-sum true-twin contrast, rather than by asserting equal scores. This does not make quotienting itself new. |
| [Shen and Carpentieri, Multi-Step Low-Rank Decomposition of Large PageRank Matrices (2021), pp. 400-401](https://journals.sagepub.com/doi/pdf/10.3233/FAIA210212) | The paper writes the system as D+FH and prices the Woodbury capacitance dimension. Here native incidence supplies the factors and the residual D is replaced by an explicitly invertible class block W. The standard block-Woodbury control matches our coefficients exactly; the elimination identity and dimension reduction are not new. |
| [Gleich, PageRank Beyond the Web, Section 2, Aside 2.3 and Section 3.1](https://arxiv.org/pdf/1407.5107) | The stochastic PageRank residual/resolvent bound and choices of dangling redistribution are established. The class-stream evaluation and Boolean correction specialize those facts; the 1/(1-alpha) certificate is not a new bound. |

Precisely, create a loopless **multigraph on the group IDs**: every pair-member event is one edge, and h repeated events are parallel edges. The target is its simple line graph, meaning two distinct edges are adjacent once if they share any endpoint. A singleton event can be represented by an edge from its group to a fresh private leaf; a zero-member isolate by an edge on two private leaves. This establishes the line-graph connection without actually adding those private factor coordinates to the solver. The class-pair graph is the underlying group multigraph after coalescing parallel edges, not itself the line graph.

No inspected paper is claimed to contain this exact entire native-source/class-stream/output schedule. That observation does not prove priority: the direct known-control derivation is already enough to defeat a generic algorithm-novelty claim. A deeper search on simple line graphs of multigraphs and block aggregation could further narrow even the application contribution.

## Executable Verification

The single Python block below is a standard-library exact-rational checker, not a low-memory implementation. Its dense matrices and retained fixtures deliberately belong to the oracle. It writes no files. Tests and the dense/quotient oracles were written first: the initial run failed with `AssertionError: F-coordinate reduction is not implemented`. The completed version has no missing implementation.

Reproduce from the repository root without extracting another file:

```sh
awk '/^```python$/{block=1;next} /^```$/{if(block)exit} block' research_algorithms_20260920/PageRank-Boolean-Two-Membership.md | python3 -B
```

```python
from fractions import Fraction as R
from itertools import combinations, product
from random import Random
import sys


def solve_fraction_linear_system(matrix, rhs):
    n = len(rhs)
    rows = [[R(v) for v in matrix[i]] + [R(rhs[i])] for i in range(n)]
    for col in range(n):
        pivot = next(i for i in range(col, n) if rows[i][col])
        rows[col], rows[pivot] = rows[pivot], rows[col]
        scale = rows[col][col]
        rows[col] = [v / scale for v in rows[col]]
        for i in range(n):
            if i != col:
                scale = rows[i][col]
                rows[i] = [v - scale * w for v, w in zip(rows[i], rows[col])]
    return [row[-1] for row in rows]


def build_dense_boolean_adjacency(memberships, weighted=False):
    sets = list(map(set, memberships))
    return [[R(0 if i == j else (len(a & b) if weighted else bool(a & b)))
             for j, b in enumerate(sets)] for i, a in enumerate(sets)]


def solve_expanded_pagerank_oracle(adjacency, p, alpha):
    n = len(p)
    degrees = [sum(row) for row in adjacency]
    transition = [[adjacency[i][j] / degrees[j] if degrees[j] else p[i]
                   for j in range(n)] for i in range(n)]
    system = [[R(i == j) - alpha * transition[i][j]
               for j in range(n)] for i in range(n)]
    return solve_fraction_linear_system(system, [(1 - alpha) * v for v in p])


def solve_signature_quotient_oracle(memberships, adjacency, p, alpha):
    buckets = {}
    for i, signature in enumerate(memberships):
        buckets.setdefault(tuple(sorted(set(signature))), []).append(i)
    cells = list(buckets.values())
    degrees = [sum(row) for row in adjacency]
    sums = [sum(p[i] for i in cell) for cell in cells]
    matrix = [[R(i == j) - alpha *
               (sum(adjacency[v][source[0]] for v in dest) / degrees[source[0]]
                if degrees[source[0]] else sums[i])
               for j, source in enumerate(cells)] for i, dest in enumerate(cells)]
    totals = solve_fraction_linear_system(matrix, [(1 - alpha) * v for v in sums])
    dangling = sum(totals[j] for j, cell in enumerate(cells) if not degrees[cell[0]])
    scale = 1 - alpha + alpha * dangling
    answer = [R(0)] * len(p)
    for j, cell in enumerate(cells):
        for i in cell:
            d = degrees[i]
            answer[i] = (totals[j] / len(cell) +
                         d * scale * (p[i] - sums[j] / len(cell)) / (d + alpha)
                         if d else scale * p[i])
    return answer


def build_two_membership_reduction(memberships, p, alpha):
    signatures = [tuple(sorted(set(s))) for s in memberships]
    if any(len(s) > 2 for s in signatures):
        raise ValueError("at most two distinct memberships required")
    if not 0 <= alpha < 1 or len(p) != len(signatures):
        raise ValueError("invalid damping or personalization length")
    if any(v < 0 for v in p) or (p and sum(p) != 1):
        raise ValueError("personalization must be a probability vector")
    buckets, counts = {}, {}
    for i, signature in enumerate(signatures):
        buckets.setdefault(signature, []).append(i)
        for a in signature:
            counts[a] = counts.get(a, 0) + 1
    degree = [0] * len(p)
    for signature, cell in buckets.items():
        d = (sum(counts[a] for a in signature) -
             (len(signature) - 1) * len(cell) - 1) if signature else 0
        for i in cell:
            degree[i] = d
    factors = sorted({a for i, s in enumerate(signatures) if degree[i] for a in s})
    index = {a: i for i, a in enumerate(factors)}
    sizes = [sum(degree[i] > 0 and a in s for i, s in enumerate(signatures))
             for a in factors]
    z = sum(p[i] for i, d in enumerate(degree) if not d)
    gamma = (1 - alpha) / (1 - alpha * z)
    b = [gamma * v for v in p]
    f = len(factors)
    matrix = [[R(0) for _ in factors] for _ in factors]
    rhs, ground = [R(0)] * f, [R(0)] * f
    laplace = [[R(0) for _ in factors] for _ in factors]
    records = []
    beta = R(0)
    for signature, cell in buckets.items():
        d, h = degree[cell[0]], len(cell)
        if not d:
            continue
        s = tuple(index[a] for a in signature)
        total = sum(b[i] for i in cell)
        debt = 1 + (len(s) - 1) * h
        denominator = d + alpha * debt
        records.append((s, h, d, total, denominator, cell))
        beta = max(beta, alpha * (d + debt) / denominator)
        for a in s:
            rhs[a] += total / denominator
            ground[a] += (1 - alpha) * sizes[a] * h * d / denominator
            for c in s:
                matrix[a][c] += alpha * h / denominator
        if len(s) == 2:
            a, c = s
            conductance = alpha * h * sizes[a] * sizes[c] / denominator
            laplace[a][a] += conductance
            laplace[c][c] += conductance
            laplace[a][c] -= conductance
            laplace[c][a] -= conductance
    for a in range(f):
        laplace[a][a] += ground[a]
    core = [[R(a == c) - matrix[a][c] for c in range(f)] for a in range(f)]
    return dict(signatures=signatures, degree=degree, sizes=sizes, records=records,
                gamma=gamma, b=b, matrix=matrix, core=core, rhs=rhs,
                laplace=laplace, ground=ground, beta=beta)


def lift_original_vertex_scores(data, state, alpha):
    answer = list(data["b"])
    for signature, h, d, total, denominator, cell in data["records"]:
        qtotal = (total + alpha * h * sum(state[a] for a in signature)) / denominator
        for i in cell:
            answer[i] = d * (qtotal / h + (data["b"][i] - total / h) / (d + alpha))
    return answer


def lift_stable_local_equations(data, state, alpha):
    answer = list(data["b"])
    for signature, h, d, total, denominator, cell in data["records"]:
        s = sum(state[a] for a in signature)
        qtotal = (total + alpha * h * s) / denominator
        incoming = s - (len(signature) - 1) * qtotal
        for i in cell:
            answer[i] = d * (data["b"][i] + alpha * incoming) / (d + alpha)
    return answer


def apply_stream_factor_matrix(data, state, alpha):
    result = [R(0)] * len(state)
    for signature, h, d, total, denominator, cell in data["records"]:
        value = alpha * h * sum(state[a] for a in signature) / denominator
        for a in signature:
            result[a] += value
    return result


def verify_positive_symmetric_matrix(matrix):
    n = len(matrix)
    assert all(matrix[i][j] == matrix[j][i] for i in range(n) for j in range(n))
    work = [list(row) for row in matrix]
    for k in range(n):
        assert work[k][k] > 0
        for i in range(k + 1, n):
            for j in range(k + 1, n):
                work[i][j] -= work[i][k] * work[k][j] / work[k][k]


def evaluate_boolean_stream_residual(data, p, alpha, answer):
    state = [R(0)] * len(data["sizes"])
    q = [answer[i] / d if d else R(0) for i, d in enumerate(data["degree"])]
    dangling = sum(answer[i] for i, d in enumerate(data["degree"]) if not d)
    result = [(1 - alpha) * p[i] + alpha * p[i] * dangling - answer[i]
              for i in range(len(p))]
    for signature, h, d, total, denominator, cell in data["records"]:
        qtotal = sum(q[i] for i in cell)
        for a in signature:
            state[a] += qtotal
    for signature, h, d, total, denominator, cell in data["records"]:
        qtotal = sum(q[i] for i in cell)
        incoming = sum(state[a] for a in signature) - (len(signature) - 1) * qtotal
        for i in cell:
            result[i] += alpha * (incoming - q[i])
    return result


def certify_published_original_scores(data, p, alpha, ids, published):
    if len(published) != len(ids) or len({key for key, _ in published}) != len(ids):
        raise ValueError("missing or duplicate output ID")
    lookup = dict(published)
    if set(lookup) != set(ids):
        raise ValueError("wrong output ID")
    answer = [lookup[key] for key in ids]
    residual = evaluate_boolean_stream_residual(data, p, alpha, answer)
    return sum(map(abs, residual)) / (1 - alpha)


def check_generic_block_elimination(memberships, p, alpha):
    data = build_two_membership_reduction(memberships, p, alpha)
    active = [i for i, d in enumerate(data["degree"]) if d]
    signatures = data["signatures"]
    factors = sorted({a for i in active for a in signatures[i]})
    incidence = [[R(a in signatures[i]) for a in factors] for i in active]
    block = [[R(i == j) * (data["degree"][i] + alpha) +
              (alpha if len(signatures[i]) == 2 and signatures[i] == signatures[j] else 0)
              for j in active] for i in active]
    inverse_b = solve_fraction_linear_system(block, [data["b"][i] for i in active])
    generic_rhs = [sum(incidence[i][a] * inverse_b[i] for i in range(len(active)))
                   for a in range(len(factors))]
    assert generic_rhs == data["rhs"]
    for a in range(len(factors)):
        inverse_u = solve_fraction_linear_system(block, [row[a] for row in incidence])
        for c in range(len(factors)):
            assert alpha * sum(incidence[i][c] * inverse_u[i] for i in range(len(active))) == data["matrix"][c][a]


def check_explicit_negative_controls():
    alpha = R(1, 2)
    overlap = ((0, 1), (0, 1), (0, 2))
    uniform = [R(1, 3)] * 3
    boolean = solve_expanded_pagerank_oracle(build_dense_boolean_adjacency(overlap), uniform, alpha)
    weighted = solve_expanded_pagerank_oracle(build_dense_boolean_adjacency(overlap, True), uniform, alpha)
    assert boolean == [R(1, 3)] * 3 and weighted == [R(5, 14), R(5, 14), R(2, 7)]
    pair, p = ((0, 1), (0, 1)), [R(1), R(0)]
    data = build_two_membership_reduction(pair, p, alpha)
    state = solve_fraction_linear_system(data["core"], data["rhs"])
    answer = lift_original_vertex_scores(data, state, alpha)
    assert answer == [R(2, 3), R(1, 3)] and answer != [R(1, 2)] * 2
    assert data["beta"] == R(4, 5) > 2 * alpha / (1 + alpha)
    zero_lift = lift_original_vertex_scores(data, [R(0), R(0)], alpha)
    assert zero_lift == [R(4, 15), -R(1, 15)]
    star = build_two_membership_reduction(((0, 1), (0, 2), (0, 3)), uniform, R(3, 4))
    assert max(map(sum, star["matrix"])) == R(9, 7) > 1
    outside = ((0, 1, 2), (0, 1))
    assert len(set(outside[0]) & set(outside[1])) == 2
    assert build_dense_boolean_adjacency(outside)[0][1] == 1
    tiny = 2.0 ** -60
    mean_contrast = 0.5 + (0.0 - 0.5) / (1.0 + tiny)
    local_lift = tiny * (2.0 - 1.0) / (1.0 + tiny)
    assert mean_contrast == 0.0 and local_lift > 0.0
    tiny_data = build_two_membership_reduction(pair, p, R(1, 2 ** 60))
    assert lift_original_vertex_scores(tiny_data, [R(1), R(1)], R(1, 2 ** 60))[1] == R(1, 2 ** 60 + 1)
    rejected = 0
    for memberships, preference, damping in ((outside, p, alpha), (pair, p, R(1)),
                                            (pair, [R(-1), R(2)], alpha)):
        try:
            build_two_membership_reduction(memberships, preference, damping)
        except ValueError:
            rejected += 1
    assert rejected == 3
    empty = build_two_membership_reduction((), [], alpha)
    assert lift_original_vertex_scores(empty, [], alpha) == []
    ids = [101, 909]
    malformed = [[(101, answer[0])], [(101, answer[0]), (101, answer[1])],
                 [(101, answer[0]), (910, answer[1])]]
    rejected = 0
    for published in malformed:
        try:
            certify_published_original_scores(data, p, alpha, ids, published)
        except ValueError:
            rejected += 1
    assert rejected == 3
    wrong = [(101, answer[0] + R(1, 10)), (909, answer[1])]
    assert certify_published_original_scores(data, p, alpha, ids, wrong) >= R(1, 10)
    print("counterexamples: weighted-vs-Boolean; unequal-class-p; negative-zero-lift;")
    print("  copied-weighted-beta; unweighted-row-contraction; three-membership-extension")
    print("edge checks: invalid-input=3 malformed-ID-output=3 perturbed-output=1 empty=1")
    print("floating cancellation witness: contrast=0; local-equation-lift>0")


def evaluate_original_dense_residual(adjacency, p, alpha, answer):
    degrees = [sum(row) for row in adjacency]
    dangling = sum(answer[i] for i, d in enumerate(degrees) if not d)
    return [(1 - alpha) * p[i] + alpha *
            (p[i] * dangling + sum(adjacency[i][j] * answer[j] / degrees[j]
                                   for j in range(len(p)) if degrees[j])) - answer[i]
            for i in range(len(p))]


def check_one_rational_case(memberships, p, alpha):
    adjacency = build_dense_boolean_adjacency(memberships)
    expected = solve_expanded_pagerank_oracle(adjacency, p, alpha)
    quotient = solve_signature_quotient_oracle(memberships, adjacency, p, alpha)
    assert quotient == expected
    data = build_two_membership_reduction(memberships, p, alpha)
    assert data is not None, "F-coordinate reduction is not implemented"
    assert data["degree"] == [sum(row) for row in adjacency]
    state = solve_fraction_linear_system(data["core"], data["rhs"])
    answer = lift_original_vertex_scores(data, state, alpha)
    assert answer == expected == lift_stable_local_equations(data, state, alpha)
    assert sum(answer) == 1 and min(answer) >= 0
    n, f = len(p), len(state)
    sizes, matrix = data["sizes"], data["matrix"]
    assert all(v >= 0 for row in matrix for v in row)
    assert data["beta"] <= 4 * alpha / (1 + 3 * alpha) < 1
    for j in range(f):
        assert sum(sizes[i] * matrix[i][j] for i in range(f)) <= data["beta"] * sizes[j]
        for i in range(f):
            assert data["laplace"][i][j] == sizes[i] * data["core"][i][j] * sizes[j]
    verify_positive_symmetric_matrix(data["core"])
    verify_positive_symmetric_matrix(data["laplace"])
    scaled = solve_fraction_linear_system(data["laplace"],
                                          [sizes[i] * data["rhs"][i] for i in range(f)])
    assert state == [sizes[i] * scaled[i] for i in range(f)]
    for probe in ([R(0)] * f, [R((-1) ** i * (i + 1), 7) for i in range(f)]):
        trial = lift_original_vertex_scores(data, probe, alpha)
        assert trial == lift_stable_local_equations(data, probe, alpha)
        action = apply_stream_factor_matrix(data, probe, alpha)
        assert action == [sum(matrix[i][j] * probe[j] for j in range(f)) for i in range(f)]
        delta = [data["rhs"][i] + action[i] - probe[i] for i in range(f)]
        predicted = [R(0)] * n
        tight = R(0)
        for signature, h, d, total, denominator, cell in data["records"]:
            value = alpha * sum(delta[a] for a in signature)
            tight += h * abs(value)
            for i in cell:
                predicted[i] = value
        residual = evaluate_original_dense_residual(adjacency, p, alpha, trial)
        assert residual == predicted == evaluate_boolean_stream_residual(data, p, alpha, trial)
        error = sum(abs(v - w) for v, w in zip(trial, expected))
        coarse = alpha * sum(sizes[a] * abs(delta[a]) for a in range(f))
        assert error <= tight / (1 - alpha) <= coarse / (1 - alpha)
    ids = [1009 + 37 * i for i in range(n)]
    rounded = [R.from_float(float(v)) for v in answer]
    published = list(reversed(list(zip(ids, rounded))))
    bound = certify_published_original_scores(data, p, alpha, ids, published)
    residual = evaluate_original_dense_residual(adjacency, p, alpha, rounded)
    assert bound == sum(map(abs, residual)) / (1 - alpha)
    assert sum(abs(v - w) for v, w in zip(rounded, expected)) <= bound


def run_complete_rational_suite():
    signatures = [()] + [(i,) for i in range(3)] + list(combinations(range(3), 2))
    cases = 0
    for n in range(1, 4):
        for memberships in product(signatures, repeat=n):
            for alpha in (R(0), R(1, 2), R(17, 20), R(99, 100)):
                for weights in ([1] * n, [1] + [0] * (n - 1), list(range(1, n + 1))):
                    p = [R(w, sum(weights)) for w in weights]
                    check_one_rational_case(memberships, p, alpha)
                    cases += 1
    exhaustive = cases
    rng = Random(20260921)
    options = [()] + [(i,) for i in range(4)] + list(combinations(range(4), 2))
    layouts = [tuple(rng.choice(options) for _ in range(rng.randrange(1, 9))) for _ in range(64)]
    targeted = [((0, 1),) * 2, ((0, 1),) * 7,
                ((0, 1), (0, 1), (0, 2)),
                ((0, 1), (0, 1), (0,), (1,), (), (2, 3)),
                ((0,), (0,), (1,), (1,), (), (2, 3)),
                ((), (0,), (1, 2)),
                ((0, 0, 1), (1, 0, 1), (1,), ())]
    for memberships in layouts + targeted:
        n = len(memberships)
        for alpha in (R(0), R(1, 2), R(17, 20), R(99, 100)):
            for weights in ([1] * n, [1] + [0] * (n - 1), list(range(1, n + 1))):
                check_one_rational_case(memberships, [R(w, sum(weights)) for w in weights], alpha)
                cases += 1
    for memberships in targeted:
        n = len(memberships)
        for alpha in (R(0), R(1, 2), R(17, 20), R(99, 100)):
            for weights in ([1] * n, [1] + [0] * (n - 1), list(range(1, n + 1))):
                check_generic_block_elimination(memberships, [R(w, sum(weights)) for w in weights], alpha)
    assert cases == 5640 and exhaustive == 4788
    print("python:", sys.version.split()[0])
    print("layouts: exhaustive=399 seeded=64 targeted=7 (parameter repetitions included below)")
    print("exact answers: dense=5640 signature-quotient=5640 F-core=5640 grounded=5640")
    print("signed/zero lift residual identities and bounds:", 2 * cases)
    print("rounded original-ID output certificates:", cases)
    print("generic block-Woodbury coefficient matches: 84")
    check_explicit_negative_controls()


if __name__ == "__main__":
    run_complete_rational_suite()
```

### Exact Run Receipt

Executed 2026-09-21 with the reproduction command above; process exit code 0. Standard output, verbatim:

```text
python: 3.9.6
layouts: exhaustive=399 seeded=64 targeted=7 (parameter repetitions included below)
exact answers: dense=5640 signature-quotient=5640 F-core=5640 grounded=5640
signed/zero lift residual identities and bounds: 11280
rounded original-ID output certificates: 5640
generic block-Woodbury coefficient matches: 84
counterexamples: weighted-vs-Boolean; unequal-class-p; negative-zero-lift;
  copied-weighted-beta; unweighted-row-contraction; three-membership-extension
edge checks: invalid-input=3 malformed-ID-output=3 perturbed-output=1 empty=1
floating cancellation witness: contrast=0; local-equation-lift>0
```

The 399 exhaustive layouts are all ordered assignments of seven signatures (empty, three singletons, three pairs) to 1-3 vertices: 7+49+343. Each gets four damping values and three personalizations, making 4,788 cases. Seed 20260921 supplies 64 additional layouts of up to eight vertices and four group labels; seven targeted layouts cover repeated pairs, disconnected components, isolates and duplicate memberships. These add 852 cases, for 5,640 parameterized cases total. Layouts/personalizations can repeat; this is not 5,640 independent datasets or four times that many independent queries.

Every case compares dense expanded PageRank, the signature quotient, the F core and the grounded system exactly; checks degree equality, positivity/normalization, symmetric positive-definite pivots, the contraction inequality, the grounded congruence, and both equivalent lifts. Two arbitrary-state probes check the original residual identity and both error bounds. Rounded scores are converted from binary64 back to exact rationals and checked using **in-memory keyed output records**, including an independent dense residual. This validates rounded-value certificate algebra, not a production disk reader or directed-rounding implementation. The generic block inverse matches c and M in 84 targeted cases. CG/Jacobi convergence is proved above, not benchmarked or implemented by this direct-solve checker.

### Concrete Counterexamples

| Tempting shortcut | Exact witness retained by the checker |
| --- | --- |
| Reuse weighted clique adjacency | Signatures `(ab,ab,ac)`, uniform p, alpha=1/2: Boolean scores `(1/3,1/3,1/3)`; weighted scores `(5/14,5/14,2/7)` |
| Lift every class uniformly | `(ab,ab)`, p=(1,0), alpha=1/2: scores `(2/3,1/3)`, not `(1/2,1/2)` |
| Assume nonnegative S implies nonnegative output | Same case at S=0: reconstructed scores `(4/15,-1/15)` |
| Copy weighted convergence constant | Same representation: rho(M)=4/5, larger than `2*alpha/(1+alpha)=2/3` |
| Use unweighted row contraction | `(ab,ac,ad)`, alpha=3/4: largest M row sum is 9/7, although its N-weighted bound is below one |
| Extend the diagonal-class correction to three memberships | `(abc,ab)`: cross-class incidence product is 2 but Boolean adjacency is 1 |

The additional alpha=2^-60 cancellation witness, three input rejections, three malformed-ID rejections, perturbed-output bound and empty-universe test are printed separately. All-isolate layouts and queries supported only on isolates are included in the exact comparisons.

## Frozen Contribution Assessment

**Useful positive result:** the exact two-membership Boolean operator admits an explicitly derived F-state class-stream solve, arbitrary-personalization recovery on every original ID, analytic isolate handling, a symmetric positive grounded formulation, a sharp Boolean-specific fixed-point bound, and an original-output residual certificate schedule. It extends the earlier weighted-incidence scope and distinguishes P class coordinates from F group coordinates even when P is quadratic in F. Native event input avoids factor discovery, not preparation/output costs.

**Boundary:** Schur complements, block Woodbury, quotient chains, true-twin decomposition, CG/Jacobi and resolvent certification are established tools. The independent generic control reproduces the core exactly. The four-paper comparison establishes close precedents, not global priority. There is no identified irreducible new solver primitive, no demonstrated application demand, no performance win, and no physical-RAM result. It remains reasonable to pursue the explicit Boolean domain composition; it is not reasonable to claim generic factorization novelty.

The next empirical discriminator, outside this owned-file task, would compare this schedule with the same-factor P quotient (including file-backed state), ordinary symmetric solvers, scalar/special-case solutions and an admitted reused dense F core, charging native build, arbitrary-p joins, every class pass, complete output and rounded-output certification. The theorem and this contribution assessment are frozen here; no further survey or edits outside this file were made.
