# PageRank Release With A Quantized State Budget

Date: 2026-09-20. A01 extension. A proved-in-document resource theorem with a reproducible exact probe, pending independent challenge. It sharpens the existing general threshold bound. It does not introduce a new matrix-factorization identity or establish publication priority.

## Result

For the admitted representation `A=UV^T-Q>=0`, with U,V nonnegative and r factors, release every active vertex whose canceled-diagonal/outgoing-degree ratio exceeds `1/m`, where m is a positive integer. Then

```text
selected vertices       <= m*r
released factor count   <= (m+1)*r
base contraction bound  <= a*(m+1)/(m+a)
positive-kernel scalars <= (3+7*m)*r + 4.
```

The factor-count bound is sharp for this release policy, including after empty-factor removal. The scalar count is only the existing kernel's payload allowance, not total physical memory. The construction uses no expanded selected adjacency rows.

This generalizes the m=1 heavy-release theorem from [the representation revision](PageRank-Representation-Revision.md). It improves the earlier normalized-trace bound, which only gave fewer than `(m+1)*r` selected vertices and therefore fewer than `(m+2)*r` factors at these thresholds.

## Why A Sharper Bound Exists

Let W=UV^T, `S_f=sum_i U_if`, and `s_j=sum_i W_ij=d_j+q_j`. Remove empty factors. At a column with s_j>0,

```text
W_jj/s_j = sum_f pi_jf * z_jf
pi_jf = S_f*V_jf/s_j,      sum_f pi_jf=1
z_jf  = U_jf/S_f,          sum_j z_jf=1.
```

Thus each normalized diagonal is a convex combination of coordinates of normalized factor columns. This is a standard probability interpretation of nonnegative factorization; [Cohen and Rothblum, 1993, Section 3 and Theorem 3.2](https://lab.rockefeller.edu/cohenje/assets/file/211CohenRothblumNonnegativeRanksDecompositionsFactorsNonnegativeMatricesLAA1993.pdf) explicitly develops column-stochastic factorization and this convex-combination interpretation. Those pages were inspected. We use that foundation, not claim it as new.

Fix any positive threshold t and set tau=t/(1+t). If active j has q_j>t*d_j, then

```text
W_jj/s_j >= q_j/(d_j+q_j) > tau.
```

At least one f must therefore have z_jf>tau. A probability vector has at most `ceil(1/t)` coordinates strictly greater than tau: if it had k of them, `k*tau<1`, hence `k<1+1/t` and the stated integer bound follows. There are r such factor columns, so

```text
|{j : d_j>0 and q_j>t*d_j}| <= r*ceil(1/t).       (S1)
```

Multiple witnessing factors for the same vertex only reduce the union size. Vertices with d_j=0 are not selected; a completely canceled dangling column does not require this active-vertex release. r=0 means an edgeless graph, possibly with isolates, handled by the original PageRank contract.

This proof uses the supplied **nonnegative factor count**, not a substitution of ordinary matrix rank into (S1). For t>=1, the previous diagonal-dominance proof additionally bounds the count by rank(W). We do not assume that stronger rank replacement for arbitrary t.

## State Versus Contraction

The release identity preserves A and d while setting q_j to zero at selected rows. Remaining active ratios are <=t, and the existing weighted contraction certificate gives

```text
gamma(t) = a*(1+t)/(1+a*t).
```

For t=1/m this yields the result above. The existing positive row-release evaluator has scalar allowance `2*r0+r+7*k+4`, with r0<=r and k<=m*r. Substituting gives `(3+7*m)*r+4`. Selected source/destination membership lists together add at most their corresponding original nonzero memberships, not an N-by-k edge artifact. Charge selected IDs, maps, factor metadata, exact/enclosed coefficients, output, runtime, file buffers and page residency separately.

For an arbitrary requested a<gamma<1, use

```text
t = (gamma-a)/(a*(1-gamma))
m = ceil(1/t)
factor cap <= (m+1)*r.
```

At gamma=a, t=0 and there is no finite m guarantee independent of N. Approaching the ordinary PageRank contraction bound can require extracting many vertices. The input graph and answer remain unchanged; these are alternative representations of the same solve.

Compute the ceiling and the strict witness test from the same exact parameter, or certify both with an explicit ambiguity policy. The independent review's `t=1/3-10^-20` example selects four vertices of a uniform four-node factor; binary64 can round it to a threshold yielding budget three and no emitted witnesses. Emitting all possible interval witnesses can exceed the exact cap; emitting only definite witnesses can miss required vertices. Refine or decline uncertain comparisons, or reserve a separately justified conservative superset.

For a=0.85, the following arithmetic illustration uses r=100,000 and eight-byte scalar payloads only:

| m | Factor cap | Certified gamma | Scalar payload allowance |
| --- | --- | --- | --- |
| 1 | 200,000 | 0.918919 | 8,000,032 B |
| 2 | 300,000 | 0.894737 | 13,600,032 B |
| 4 | 500,000 | 0.876289 | 24,800,032 B |
| 8 | 900,000 | 0.864407 | 47,200,032 B |
| 16 | 1,700,000 | 0.857567 | 92,000,032 B |

These are not measured application RAM or latency estimates. Certified numerical scalars can be wider than eight bytes. A smaller contraction bound does not prove fewer actual iterations or lower lifecycle cost; the earlier 45-versus-156 iteration regression still applies to unpriced release decisions. Each chosen plan needs its own initial residual, numerical-error allowance and build/initialization budget.

## Sharpness And Threshold Boundary

For one factor, use m+1 vertices with

```text
u = (1, ..., 1, epsilon),  v = (1, ..., 1, 1)
Q = diag(u),              0 < epsilon < 1.
```

There are m unit entries in u. The graph has no self loops and off-diagonal A_ij=u_i. For each of the first m vertices, q/d=1/(m-1+epsilon)>1/m. The last vertex has q/d=epsilon/m<1/m. Exactly m vertices are selected, and the original factor survives on the last row.

After release, the nonnegative factorized matrix is `A+Q_outside = u*1^T - diag(1,...,1,0)`. It is nonsingular: if it maps x to zero, its last row gives sum(x)=0; each first row then gives x_i=0, and consequently the last coordinate is also zero. Its rank is m+1. Therefore **any** factorization preserving this post-release diagonal requires at least m+1 factors. The row-release construction attains that number. r disjoint copies give selected count mr and required factor count (m+1)r.

Strict inequality is essential. A uniform block of m+1 vertices has q/d=1/m at every vertex. Selecting equality would choose m+1 vertices from one factor and violate the mr count. Compare exact rationals or use an enclosing decision procedure; rounding a threshold equality upward is not allowed. Keeping an unresolved equality unselected requires proving its ratio is <=t; otherwise numerical admission needs refinement or a separately budgeted conservative superset.

Independent review extends sharpness to any finite t>0: let m=ceil(1/t) and choose `0<epsilon<min(1,1/t-(m-1))`. Exactly m vertices are selected, the last is not, and the same fixed-post-release-diagonal rank argument applies. This does not establish globally optimal PageRank state when another diagonal/factorization or a different solver is allowed.

## A Bounded Candidate Discovery Pass

The proof also gives a construction without an N-sized membership bitmap:

```text
reserve the plan's factor totals and its at-most-mr selection allowance
coalesce all duplicate (vertex,factor) U records into canonical entries
compute or validate S_f from the original nonnegative factor stream
stream U_if; emit (vertex, factor witness) only when U_if/S_f > t/(1+t)
deduplicate vertex IDs through the admitted small buffer or external sort
join with exact/enclosed q_i,d_i rows; retain only active q_i>t*d_i
build selected-factor order and execute the positive release kernel
```

Each factor emits at most m candidates, so the pre-deduplication stream itself has at most mr records. This is not a bound on all original membership reads. A rowwise pass testing q_i/d_i is also valid; choose the cheaper already-available order. The candidate test uses exact/enclosed products, not a rounded division. Overflow, S_f uncertainty and retained old/new orders are admission obligations. No arbitrary approximate factorization is made exact by this theorem.

The predicate tests canonical U_if, not individual fragments: splitting a required entry of weight one into two raw half-weight records can make both fail separately. Coalescing and completed factor totals therefore precede admission. Also, **candidate witnesses are not all selected execution memberships**. A vertex selected through one factor can belong to every other factor. The review constructs mr witnesses but 2*m*r^2 selected U/V membership records. Keep the original O(Z) selected-index allowance and O(k) per-factor group scratch; do not replace either with the much smaller witness count.

## Claim Boundary And Next Comparison

The new local contribution is a sharp, quantized state/contraction guarantee for the specified release compiler, plus an at-most-mr candidate stream. Convex probability mixtures, diagonal elimination, star decomposition and PageRank residual bounds are established ingredients. The earlier [primary-art comparison](PageRank-Prior-Art-Comparison.md) still governs positioning; inaccessible WDBMC Section 3 remains a real unknown, not evidence of absence.

Compare a complete solve against the strongest same-factor direct, Krylov or structured elimination method, not merely an N-state CSR iteration. Rank-one sharpness blocks have easy direct solvers and are theorem witnesses, not performance victories. Ordinary binary incidence with q<=d does not activate m=1 release, although tighter thresholds m>1 can select its vertices. Factor discovery and the full output scan remain charged.

## Exact Falsification Probe

The standalone fence checks arbitrary supplied nonnegative factors, partial diagonal cancellation, candidate coverage, release identity, contraction bounds, strict-threshold witnesses and a rank lower bound on the sharpness family. Small dense oracle matrices are used here, not by the proposed streamed kernel. This is a new theorem probe, not another claim that random tests constitute a proof.

```python
from fractions import Fraction as F
from random import Random

def ceil_positive_rational_value(x):
    return -((-x.numerator)//x.denominator)

def compute_exact_matrix_rank(A):
    A=[row[:] for row in A]; rank=0
    for col in range(len(A[0])):
        pivot=next((i for i in range(rank,len(A)) if A[i][col]),None)
        if pivot is None: continue
        A[rank],A[pivot]=A[pivot],A[rank]
        value=A[rank][col]; A[rank]=[v/value for v in A[rank]]
        for i in range(rank+1,len(A)):
            value=A[i][col]
            A[i]=[v-value*w for v,w in zip(A[i],A[rank])]
        rank+=1
    return rank

def check_quantized_release_fixture(U,V,q,t,a):
    n,r=len(U),len(U[0])
    W=[[sum(U[i][f]*V[j][f] for f in range(r)) for j in range(n)] for i in range(n)]
    A=[[W[i][j]-(q[i] if i==j else 0) for j in range(n)] for i in range(n)]
    assert all(x>=0 for row in A for x in row)
    d=[sum(A[i][j] for i in range(n)) for j in range(n)]
    selected={j for j in range(n) if d[j] and q[j]>t*d[j]}
    m=ceil_positive_rational_value(1/t); tau=t/(1+t)
    candidates=[]
    for f in range(r):
        total=sum(U[i][f] for i in range(n))
        local=[i for i in range(n) if total and U[i][f]>tau*total]
        assert len(local)<=m
        candidates.extend(local)
    assert len(candidates)<=m*r and selected<=set(candidates)
    assert len(selected)<=m*r
    active=[f for f in range(r) if any(U[i][f] for i in range(n) if i not in selected)]
    ids=sorted(selected)
    Ur=[[F(0) if i in selected else U[i][f] for f in active]+[F(i==j) for j in ids] for i in range(n)]
    Vr=[[V[i][f] for f in active]+[A[j][i] for j in ids] for i in range(n)]
    qr=[F(0) if i in selected else q[i] for i in range(n)]
    assert len(Ur[0])<=(m+1)*r
    for i in range(n):
        for j in range(n):
            assert sum(u*v for u,v in zip(Ur[i],Vr[j]))-(qr[i] if i==j else 0)==A[i][j]
    beta=max([a]+[a*(1+qr[i]/d[i])/(1+a*qr[i]/d[i]) for i in range(n) if d[i]])
    assert beta<=a*(1+t)/(1+a*t)
    return len(selected),len(Ur[0]),[[A[i][j]+(qr[i] if i==j else 0) for j in range(n)] for i in range(n)]

rng=Random(20260920); counts=dict(random_thresholds=0,sharp_cases=0,equality_cases=0)
thresholds=[F(1,8),F(1,4),F(1,3),F(2,5),F(1,2),F(3,4),F(1),F(3,2),F(2),F(4)]
for case in range(160):
    n,r=rng.randrange(2,9),rng.randrange(1,5)
    U=[[F(rng.randrange(5),rng.randrange(1,4)) for _ in range(r)] for _ in range(n)]
    V=[[F(rng.randrange(5),rng.randrange(1,4)) for _ in range(r)] for _ in range(n)]
    q=[sum(U[i][f]*V[i][f] for f in range(r))*F(rng.randrange(5),4) for i in range(n)]
    for t in thresholds:
        check_quantized_release_fixture(U,V,q,t,F(17,20))
        counts['random_thresholds']+=1
for r in range(1,4):
    for m in range(1,9):
        n=r*(m+1); U=[[F(0)]*r for _ in range(n)]; V=[[F(0)]*r for _ in range(n)]
        for block in range(r):
            for j in range(m+1):
                i=block*(m+1)+j
                U[i][block]=F(1) if j<m else F(1,4)
                V[i][block]=F(1)
        q=[sum(row) for row in U]
        selected,factors,Wr=check_quantized_release_fixture(U,V,q,F(1,m),F(17,20))
        assert selected==m*r and factors==(m+1)*r
        assert compute_exact_matrix_rank(Wr)==(m+1)*r
        counts['sharp_cases']+=1
for m in range(1,9):
    U=V=[[F(1)] for _ in range(m+1)]
    selected,_,_=check_quantized_release_fixture(U,V,[F(1)]*(m+1),F(1,m),F(17,20))
    assert selected==0
    counts['equality_cases']+=1
print(counts)
print('all exact identities, coverage, counts, contraction and sharp-rank assertions passed')
```

## Verification Status

The retained source executed with exit status 0: 1,600 random threshold cases, 24 sharpness/rank cases and eight equality cases passed. Candidate coverage, emitted-record counts, exact release identity and contraction inequalities all matched their oracles. [Independent staircase review](PageRank-Independent-Review.md#staircase-review-count-stream-and-sharpness) completed, found no proof-level counterexample, strengthened arbitrary-t sharpness, and supplied 27,621 packing checks, 27 sharp-rank cases and threshold/coalescing/group-size falsifiers. The concrete boundaries above incorporate those findings. No physical or floating-point performance claim follows from these exact rational tests.
