# PageRank Representation And Contraction Budget

Date: 2026-09-20. A01 revision following [independent review](PageRank-Independent-Review.md). This file supplies a stronger algorithmic option and exact small-case evidence, not a measured storage implementation or a novelty guarantee.

## Why The First Planner Was Insufficient

Minimizing factor count does not minimize execution work. For the fixed graph `A=[[0,1],[1,0]]`, choose `u=(K,1)`, `v=(1,1/K)` and `Q=diag(K,1/K)`. Then `A=uv^T-Q` for every positive K. With damping a, the nonzero reduced iteration eigenvalue is

```text
lambda(K) = a*K/(1+a*K) + a/(K+a).
```

It approaches one although the actual graph and its ordinary PageRank problem do not change. Neither factor is a singleton. Splitting the factor into proportional columns can also make scalar Jacobi ineffective until those columns are merged. Factor count, encoded bytes, convergence and certificate tightness are separate planning dimensions.

## New Option: Release Diagonal Debt

The following construction turns a requested contraction bound into a concrete factor/storage tradeoff. It is a proposed specialization of established exact factorization and stationary-iteration machinery; prior-art separation is still under investigation.

Use the main manuscript's destination-row convention and validated `A=UV^T-Q>=0`. Select disjoint vertex sets R and C. Remove rows R from U and rows C from V, keeping zeros conceptually rather than allocating new dense matrices. Remove q only at vertices in `R union C`. Restore the lost original edges as follows:

1. For each i in R, append the destination-star `e_i A[i,:]`.
2. For each j in C, append the source-star `A[outside R,j] e_j^T`.
3. Remove factors with a zero side. Retain any real self-loop in the restored original row or column.

Writing D_R and D_C for the diagonal selection masks, the identity is

```text
A = (I-D_R) U V^T (I-D_C) - Q_outside
    + D_R A + (I-D_R) A D_C.
```

Every original matrix entry belongs to exactly one of the remaining rectangle, selected rows, or selected columns outside those rows. The diagonal correction is retained only in the rectangle. This proves identity without a rank approximation. There are at most `r+|R|+|C|` factors, before empty-factor removal. The outgoing degrees, source graph and PageRank answer are unchanged.

### Contraction Selection Theorem

Fix `0<a<1` and a requested `a<=gamma<1`. Define t_i=q_i/d_i for positive outgoing degree, and t_i=0 otherwise. The base solver has the weighted contraction bound `phi(t)=a*(1+t)/(1+a*t)`. This function increases with t.

```text
threshold = (gamma-a) / (a*(1-gamma))
bad       = {i : d_i>0 and q_i/d_i > threshold}.
```

Release every vertex in bad, choosing a row or column orientation for each. Then the rebuilt operator has `beta_new<=gamma`, with at most `r+|bad|` factors. Within this exact row/column release family, any plan satisfying that same maximum-ratio certificate must release every bad vertex: leaving one leaves its degree and diagonal ratio unchanged. This is a restricted necessity result, not a lower bound for other factorizations or for actual spectral radius.

No full vertex-sized changing array is introduced. The selected-ID stream and reconstructed adjacency are build artifacts whose bytes and external joins must fit the build budget. A selected dense row can be expensive. The following counting bound strengthens the state guarantee, without bounding restored adjacency bytes.

### Heavy Diagonal Release Uses At Most Twice The Factors

Let W=UV^T, let `c_f=sum_i U_if>0` after empty-factor removal, and let `s_i=sum_k W_ki=d_i+q_i`. Nonnegativity of A implies `q_i<=W_ii`. For s_i>0, the quantity `c_f*V_if/s_i` is between zero and one. Consequently,

```text
sum_{i:s_i>0} q_i/s_i
  <= sum_{i:s_i>0} W_ii/s_i
   = sum_{i,f:s_i>0} (U_if/c_f) * (c_f*V_if/s_i)
  <= sum_{i,f} U_if/c_f
   = r.
```

This is a standard normalized-diagonal/rank counting idea specialized to the admitted factors, not a claim to have invented a new trace inequality. It includes completely canceled dangling columns in the sum; selecting only active vertices can only reduce the counted mass.

The first counting proof gave fewer than 2r selected vertices. Independent review strengthened it to **at most r**. Let S contain active vertices with q_i>d_i. For every j in S,

```text
W_jj = q_j + A_jj > d_j >= sum_(i != j) A_ij
                              = sum_(i != j) W_ij.
```

The principal submatrix W[S,S] is strictly column-diagonally dominant, hence nonsingular by the standard maximum-component argument applied to its transpose. Therefore `|S|=rank(W[S,S])<=rank(W)<=r`. Release exactly S:

```text
selected vertices <= r
new factors       <= 2r
new beta          <= 2a/(1+a) < 1.
```

The bound can be attained by disjoint two-node factors u=(2,1), v=(1,1/2), q=(2,1/2): each has one heavy vertex and becomes two factors after release. The strict threshold matters: W=ones(2,2), Q=I has two q=d vertices but rank one. At r=0, A=0 and q=0; the vertex universe can still contain isolates and its answer is p. The generic dangling channel is an extra coordinate, not one of these r factors. This is standard diagonal-dominance/rank reasoning applied to the release policy, not a newly invented nonsingularity theorem.

This removes arbitrary cancellation-induced deterioration while keeping O(r), not O(N), changing state. The gap `1-beta_new >= (1-a)/(1+a)` is explicit. A sufficiently small initial residual may still make the unreleased representation cheaper; certify it before rebuilding. A contraction gap controls a worst-case residual-decay bound, not the exact observed iteration count.

More generally, for a<gamma<1, above-threshold vertices have `q_i/s_i > (gamma-a)/(gamma*(1-a))`. Thus a nonempty selected set obeys `|bad| < r*gamma*(1-a)/(gamma-a)`. This gives a family of certified factor-state/decay tradeoffs. The bound becomes vacuous as gamma approaches a, consistent with the possibility that eliminating all positive Q needs many extracted vertices.

A subsequent [quantized release theorem](PageRank-Release-Staircase.md) sharpens this general count using the convex-mixture interpretation of the supplied nonnegative factors: for every t>0, selected count is at most `r*ceil(1/t)`. In particular t=1/m gives at most `(m+1)r` released factors and contraction `a*(m+1)/(m+a)`, with a sharp fixed-post-release-diagonal family. Its exact probe and independent challenge passed at the stated mathematical scope; threshold/coalescing and execution-index boundaries are explicit. This is a factor-count result, not an unqualified substitution of ordinary rank for r.

The heavy-release branch is not necessarily useful on the repository's ordinary binary-membership projections. If feature f has positive weight w_f and at least two members after singleton removal, then `q_i=sum_f w_f` and `d_i=sum_f w_f*(size_f-1)` over i's memberships, so q_i<=d_i. No vertex needs heavy release. Its additional scope is skewed directed/weighted factorizations with actual excess cancellation, not a default speedup for every incidence graph. Continuous, unequally weighted membership coefficients do not inherit that binary-membership inequality automatically.

**Storage is the remaining price.** Restoring O(r) original rows/columns can require O(Nr) nonzeros, even if the supplied factors had much smaller total incidence. The theorem guarantees the factor-state count and contraction of a completed valid release, not existence of a 50 GB stored plan or a cheap builder. Orientation and reuse need empirical evaluation. On graphs with r already comparable to N, even the state guarantee may be less useful than an ordinary solver.

The subsequent [positive row-release kernel](PageRank-Positive-Release-Kernel.md) removes the O(Nr) restored-edge artifact for the row-only variant. It applies the same abstract factors using original rows plus selected factor-major memberships and prefix/suffix sums, with O(r+k) working state and at most Z additional membership records. It still pays for the extra order and its build/scratch; mixed row/column release does not automatically inherit that kernel's bound.

Row-only, column-only and mixed orientations give the same reconstructed base iterates when they leave the same Q. They can have different encoding size, build traffic and conservative factor-certificate tightness. Compare these costs explicitly; the algorithm does not claim globally optimal orientation.

### A Real Pass Certificate, Not A Deadline Claim

After normalization into the revised factors, let `D0=||T(h0)-h0||_c`. Exact base iteration gives `||T(hk)-hk||_c <= gamma^k D0`. Thus

```text
||X(hk)-x*||_1 <= a/(1-a) * gamma^k * D0.
```

For positive D0, an admitted pass cap can use the smallest nonnegative k satisfying that expression <= the solver's error allowance. D0 itself needs an initialization and gather pass. Computing its upper bound, logs and rounding conservatively is part of numerical admission. The formula bounds iterations of an exact admitted operator; it is not a wall-clock guarantee, does not include the output pass, and does not repair a miscomputed source degree.

### Builder And Solver Procedure

```text
validate original semantics and exact/enclosed degrees
consume canceled singleton mass with a cumulative q ledger
if original-state reservation fits:
    certify the initial answer, optionally try a priced short pilot
    if already certified: emit it without rebuilding
for each proposed gamma and row/column orientation:
    stream bad vertices using the ratio threshold
    price factor count, factor bytes, joins, scratch and overlap
    build only an admitted candidate using bounded external joins
    validate the released factors against the source contract
    choose base iteration, paid Jacobi, or an admitted tiny core
    measure D0 and admit the remaining pass/output allowance
    execute; certify the ORIGINAL answer, or report no result
```

Do not allocate an inadmissible original representation just to run its initial check. If an old pilot is retained while building a candidate, price their overlap; otherwise discard or explicitly spill it. New coordinates and certificate weights require a new initialization/D0 evaluation, not a copied old h or old D0. The implicit positive kernel retains original-r scratch even if its abstract released coordinate count is small.

Materializing `A[i,:]` from factors requires enumerating and externally reducing the relevant factor memberships. This can cost much more than writing the resulting row. A bounded dry-run or degree estimate must not be mistaken for a completed factor-discovery certificate. The new theorem applies to validated available factors; arbitrary graph factor discovery remains a separate problem.

## Two Additional Corrections

### Cumulative Singleton Cancellation

For a same-vertex singleton factor of diagonal mass m, consume `s=min(m,q_remaining[i])`. Decrease q by s and decrease that factor's mass by s, deleting it only if its remaining mass is zero. Processing the next factor uses the updated q. This preserves `UV^T-Q` at every step, including partial cancellation and real self-loops. For masses 2 and 3 with q=3, the final retained diagonal mass is 2, not zero. Provenance needed for refresh remains separately retained.

### A Streamed Original Residual

The cheap certificate `a*sum_j c_j*|delta_j|` is an upper bound, not the actual residual norm. An optional U-row pass computes `a*sum_i |(Ubar*delta)_i|` with one scalar row accumulator and no N-vector. Preserve both h and next=T(h), and expose delta as a read-only difference view: evaluate next[j]-h[j] when a row needs that coordinate, with the declared arithmetic enclosure. The additional pass and repeated subtraction work are charged. Continue base iteration by swapping the unchanged vectors; return X(h) only when its certificate passes.

This replaces the earlier destructive next-to-delta-to-next round trip. With h=2^54 and next=1/4, binary64 subtraction and restoration can turn next into zero. Retaining both original vectors avoids that update corruption, although it does not itself certify the residual subtraction. A Jacobi continuation uses its own admitted update rule, not an unconditional base swap or exact-arithmetic restoration formula.

For the two-node all-ones graph at a=.85 and p=(1,0), minimum row and column covers reconstruct the same exact answer immediately. Their cheap error bounds are respectively 0 and `289/60`. The streamed original residual is zero for both. This is a certificate-selection issue, not a change in the mathematical answer.

## Reproducible Probe

Run the sole Python code block below with Python 3. It uses standard-library rational arithmetic for identities, direct solves, contraction inequalities, partial-Q cases and release selection. The final fixed-A timing-control section uses floats and reports iteration counts only. It does not write files or benchmark physical memory. Fractions are an oracle, not a proposed bounded-memory production representation.

```python
from fractions import Fraction as F
from itertools import product, permutations
from random import Random
import json


def multiply_exact_dense_matrices(A, B):
    return [[sum((a*b for a, b in zip(row, col)), F(0))
             for col in zip(*B)] for row in A]


def solve_exact_linear_system(A, b):
    n = len(b)
    H = [list(row)+[b[i]] for i, row in enumerate(A)]
    for j in range(n):
        pivot = next(i for i in range(j, n) if H[i][j])
        H[j], H[pivot] = H[pivot], H[j]
        d = H[j][j]
        H[j] = [x/d for x in H[j]]
        for i in range(n):
            if i != j:
                f = H[i][j]
                H[i] = [x-f*y for x, y in zip(H[i], H[j])]
    return [row[-1] for row in H]


def build_exact_pagerank_operator(U, V, q, p, a):
    n, r = len(p), len(U[0])
    A = [[sum((U[i][f]*V[j][f] for f in range(r)), F(0))
          - (q[i] if i == j else 0) for j in range(n)] for i in range(n)]
    assert all(x >= 0 for row in A for x in row)
    d = [sum(col) for col in zip(*A)]
    inv = [1/x if x else F(0) for x in d]
    z = [F(x == 0) for x in d]
    Ub = [row+[p[i]] for i, row in enumerate(U)]
    Vt = [[V[i][f]*inv[i] for i in range(n)] for f in range(r)]+[z]
    H = [1/(1+a*q[i]*inv[i]) for i in range(n)]
    b = [(1-a)*v for v in p]
    P = [[A[i][j]*inv[j]+p[i]*z[j] for j in range(n)] for i in range(n)]
    M = multiply_exact_dense_matrices(Vt, [[a*H[i]*x for x in row]
                                                        for i, row in enumerate(Ub)])
    g = [sum(Vt[j][i]*H[i]*b[i] for i in range(n)) for j in range(r+1)]
    c = [sum(col) for col in zip(*Ub)]
    beta = max(a*(1+q[i]*inv[i])/(1+a*q[i]*inv[i]) for i in range(n))
    return dict(A=A, d=d, Ub=Ub, Vt=Vt, H=H, b=b, P=P, M=M, g=g, c=c,
                beta=beta, p=p, a=a)


def release_selected_diagonal_vertices(U, V, q, A, R, C):
    n, r = len(U), len(U[0])
    assert not R & C
    columns = []
    for f in range(r):
        columns.append(([F(0) if i in R else U[i][f] for i in range(n)],
                        [F(0) if i in C else V[i][f] for i in range(n)]))
    for i in sorted(R):
        columns.append(([F(j == i) for j in range(n)], list(A[i])))
    for j in sorted(C):
        columns.append(([F(0) if i in R else A[i][j] for i in range(n)],
                        [F(i == j) for i in range(n)]))
    columns = [(u, v) for u, v in columns if any(u) and any(v)]
    Un = [[u[i] for u, v in columns] for i in range(n)]
    Vn = [[v[i] for u, v in columns] for i in range(n)]
    qn = [F(0) if i in R | C else q[i] for i in range(n)]
    return Un, Vn, qn


def validate_exact_case_contract(op, h):
    a, n, r = op['a'], len(op['p']), len(h)
    x = [op['H'][i]*(op['b'][i]+a*sum(u*v for u, v in zip(op['Ub'][i], h)))
         for i in range(n)]
    delta = [op['g'][j]+sum(op['M'][j][f]*h[f] for f in range(r))-h[j]
             for j in range(r)]
    residual = [op['b'][i]+a*sum(op['P'][i][j]*x[j] for j in range(n))-x[i]
                for i in range(n)]
    predicted = [a*sum(u*v for u, v in zip(row, delta)) for row in op['Ub']]
    assert residual == predicted
    ref = solve_exact_linear_system([[F(i == j)-a*op['P'][i][j]
                                      for j in range(n)] for i in range(n)], op['b'])
    hs = solve_exact_linear_system([[F(i == j)-op['M'][i][j]
                                     for j in range(r)] for i in range(r)], op['g'])
    xs = [op['H'][i]*(op['b'][i]+a*sum(u*v for u, v in zip(op['Ub'][i], hs)))
          for i in range(n)]
    assert xs == ref
    cheap = a*sum(c*abs(v) for c, v in zip(op['c'], delta))/(1-a)
    streamed = sum(map(abs, predicted))/(1-a)
    error = sum(abs(u-v) for u, v in zip(x, ref))
    assert error <= streamed <= cheap
    for j, cj in enumerate(op['c']):
        assert sum(op['c'][i]*op['M'][i][j] for i in range(r)) <= op['beta']*cj
    return cheap, streamed


def count_float_solver_updates(op, reference, cap=200000):
    a = float(op['a'])
    h = [float(sum(v*p for v, p in zip(row, op['p']))) for row in op['Vt']]
    M = [[float(v) for v in row] for row in op['M']]
    g = list(map(float, op['g']))
    for k in range(cap+1):
        x = [float(op['H'][i])*(float(op['b'][i])+a*sum(float(u)*v for u, v in zip(row, h)))
             for i, row in enumerate(op['Ub'])]
        if sum(abs(u-v) for u, v in zip(x, reference)) <= 1e-10:
            return k
        h = [g[j]+sum(u*v for u, v in zip(row, h)) for j, row in enumerate(M)]
    raise AssertionError('iteration cap exhausted')


out = dict(seed=20260920, rational_cases=0, partial_q_cases=0,
           signed_state_checks=0, release_orientations=0, selection_subsets=0,
           singleton_ledger_checks=0, release_reference_checks=0,
           normalized_diagonal_checks=0, heavy_release_checks=0)
rng = Random(out['seed'])
for case in range(72):
    n, r = rng.randrange(1, 6), rng.randrange(4)
    U = [[F(rng.randrange(4)) if rng.random() > .45 else F(0) for f in range(r)] for i in range(n)]
    V = [[F(rng.randrange(4)) if rng.random() > .45 else F(0) for f in range(r)] for i in range(n)]
    keep = [f for f in range(r) if any(U[i][f] for i in range(n)) and any(V[i][f] for i in range(n))]
    U = [[row[f] for f in keep] for row in U]
    V = [[row[f] for f in keep] for row in V]
    q = [sum(u*v for u, v in zip(U[i], V[i]))*rng.choice([F(0), F(1,4), F(1,2), F(1)]) for i in range(n)]
    p = [F(rng.randrange(4)) for i in range(n)]
    p[0] += 1
    total = sum(p)
    p = [x/total for x in p]
    for a in [F(1,10), F(17,20), F(97,100)]:
        op = build_exact_pagerank_operator(U, V, q, p, a)
        h = [F(rng.randrange(-4,5), rng.randrange(1,5)) for _ in op['c']]
        validate_exact_case_contract(op, h)
        out['rational_cases'] += 1
        out['signed_state_checks'] += 1
        out['partial_q_cases'] += int(any(0 < q[i] < sum(u*v for u, v in zip(U[i],V[i])) for i in range(n)))
        rank = len(U[0])
        mass = sum(q[i]/(d+q[i]) for i,d in enumerate(op['d']) if d+q[i])
        assert mass <= rank
        out['normalized_diagonal_checks'] += 1
        heavy = {i for i,d in enumerate(op['d']) if d and q[i] > d}
        Hu,Hv,Hq = release_selected_diagonal_vertices(U,V,q,op['A'],heavy,set())
        hop = build_exact_pagerank_operator(Hu,Hv,Hq,p,a)
        assert hop['A'] == op['A'] and hop['beta'] <= 2*a/(1+a)
        if rank:
            assert len(heavy) <= rank and len(Hu[0]) <= 2*rank
        else:
            assert not heavy and not Hu[0]
        out['heavy_release_checks'] += 1
        gamma = (9*a+1)/10
        threshold = (gamma-a)/(a*(1-gamma))
        bad = {i for i, d in enumerate(op['d']) if d and q[i]/d > threshold}
        if bad:
            assert len(bad) < rank*gamma*(1-a)/(gamma-a)
        for bits in product([0,1], repeat=n):
            S = {i for i, b in enumerate(bits) if b}
            Un,Vn,qn = release_selected_diagonal_vertices(U,V,q,op['A'],S,set())
            ns = build_exact_pagerank_operator(Un,Vn,qn,p,a)
            assert (ns['beta'] <= gamma) == (bad <= S)
            assert ns['A'] == op['A']
            out['selection_subsets'] += 1
        for bits in product([0,1], repeat=len(bad)):
            R = {i for i,b in zip(sorted(bad),bits) if b}
            C = bad-R
            Un,Vn,qn = release_selected_diagonal_vertices(U,V,q,op['A'],R,C)
            ns = build_exact_pagerank_operator(Un,Vn,qn,p,a)
            assert ns['A'] == op['A'] and ns['d'] == op['d']
            assert ns['beta'] <= gamma
            assert len(Un[0]) <= len(U[0])+len(bad)
            out['release_orientations'] += 1
        Un,Vn,qn = release_selected_diagonal_vertices(U,V,q,op['A'],bad,set())
        ns = build_exact_pagerank_operator(Un,Vn,qn,p,a)
        validate_exact_case_contract(ns,[F(1,3)]*len(ns['c']))
        out['release_reference_checks'] += 1

for masses in product(range(4), repeat=3):
    for q in range(sum(masses)+1):
        for order in permutations(range(3)):
            kept = list(map(F,masses))
            remaining = F(q)
            original = sum(kept)-remaining
            for f in order:
                consume = min(kept[f], remaining)
                kept[f] -= consume
                remaining -= consume
                assert sum(kept)-remaining == original
            assert remaining == 0
            out['singleton_ledger_checks'] += 1

A = [[F(1),F(1)],[F(1),F(1)]]
I = [[F(1),F(0)],[F(0),F(1)]]
cover = []
for U,V in [(I,A),(A,I)]:
    op = build_exact_pagerank_operator(U,V,[F(0),F(0)],[F(1),F(0)],F(17,20))
    h = [sum(u*v for u,v in zip(row,op['p'])) for row in op['Vt']]
    cover.append(tuple(map(str,validate_exact_case_contract(op,h))))
assert cover == [('0','0'),('289/60','0')]
out['cover_cheap_and_streamed_error_bounds'] = cover

out['fixed_graph_controls'] = []
for K in [1,100,1000]:
    U,V,q = [[F(K)],[F(1)]],[[F(1)],[F(1,K)]],[F(K),F(1,K)]
    op = build_exact_pagerank_operator(U,V,q,[F(1),F(0)],F(17,20))
    bad = {i for i,d in enumerate(op['d']) if d and q[i]/d > F(10,17)}
    Un,Vn,qn = release_selected_diagonal_vertices(U,V,q,op['A'],bad,set())
    ns = build_exact_pagerank_operator(Un,Vn,qn,op['p'],op['a'])
    assert ns['beta'] <= F(9,10) and ns['A'] == op['A']
    ref = [20/37,17/37]
    out['fixed_graph_controls'].append(dict(K=K, released=sorted(bad),
        factors_before=len(U[0]),factors_after=len(Un[0]),
        beta_before=str(op['beta']),beta_after=str(ns['beta']),
        float_updates_before=count_float_solver_updates(op,ref),
        float_updates_after=count_float_solver_updates(ns,ref)))
K = 2**54
assert (float(K)+1)-float(K) == 0 and F(K)+1-F(K) == 1
out['degree_rounding_trap_detected'] = True
print(json.dumps(out, indent=2))
```

## Results And Remaining Gates

The saved source above was executed using `awk` to extract its sole Python block and Python 3 to run it. Exit status was 0. Seed: 20260920.

| Exact check | Count and result |
| --- | --- |
| Source/factor direct solves and arbitrary signed-state residuals | 216 cases; zero discrepancies |
| Cases with partial positive diagonal cancellation | 36 of those 216 cases |
| Additional released-operator direct solves and constant-positive-state checks | 216; zero discrepancies; arbitrary signed-state tests are on the original operator and in the separate sparse kernel |
| Row/column release orientations | 274; identity, unchanged degrees, factor bound and requested contraction all held |
| Released-vertex subset selection | 3,168; the threshold set was necessary and sufficient for this certificate |
| Normalized diagonal mass and heavy-release bound | 216 each; mass<=r, selected<=r, factors<=2r, and beta<=2a/(1+a) |
| Cumulative singleton ledgers across factor orders | 2,112; every intermediate original operator was preserved |
| All-ones graph cover certificates | Cheap bounds 0 and 289/60; streamed bounds both exactly 0 |
| Degree-cancellation trap | Exact degree 1 versus f64 subtraction result 0 at K=2^54; detected |

The direct-solve and arbitrary-state checks use exact rationals, with no additive comparison tolerance. They are much smaller than the earlier f64 sweep and test a different claim. They still do not exercise encoded disk rows, input parsing, allocation, floating enclosures or general factor discovery.

The following float controls stop when the reconstructed answer's L1 error against the known two-node answer is <=1e-10. Updates exclude initialization, build and output. The requested contraction cap is .9.

| K | Factors before/after | Released vertices | Certified beta before/after | Float updates before/after |
| --- | --- | --- | --- | --- |
| 1 | 1 / 2 | 0, 1 | 34/37 / 17/20 | 0 / 141 |
| 100 | 1 / 2 | 0 | 1717/1720 / 1717/2017 | 7,129 / 133 |
| 1,000 | 1 / 2 | 0 | 17017/17020 / 17017/20017 | 70,159 / 140 |

The K=1 loss changed the proposed planner: check the initial answer before releasing debt. Improving a worst-case bound does not guarantee improvement of an already favorable initial residual. The last large iteration count differs slightly from the independent review's float implementation because near-threshold rounding and evaluation order differ; neither count is an exact arithmetic certificate or latency measurement. The verified algebraic beta bound does not depend on that count.

The follow-up independent review found a stronger planner regression at K=100000001/100000000: the original initial error bound is about 4.2221e-9, so it fails a 1e-10 target, yet the original reaches its cheap certificate in 45 exact updates and the .9-cap release needs 156. True-error first hits are 45 and 141. Thus even the initial-check guard does not establish dominance. Compare a priced pilot and each new plan's D0, and retain a no-release option. The independent review retains the exact reproducer and also tests a sharper induced norm that can beat the conservative beta threshold without any release.

Independent physical row-layout implementation, validated floating arithmetic, source-native factor acquisition, and direct comparisons with the closest compression solvers remain required for a performance or paper-readiness claim. The release construction is a strengthened candidate procedure, not a claim that known row/column factorization or convergence theory has been newly discovered.
