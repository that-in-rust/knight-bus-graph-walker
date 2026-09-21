# Boolean PageRank: A Native Tree Gap Without Complete Pair Coverage

Date: 2026-09-21. Independently reviewed derivation, implemented weighted
actual-byte certificate, and completed [native experiment](PageRank-Boolean-Native-Implementation.md).
No new generic canonical-path theorem is claimed. The independent review
covers mathematics/resources/prior art, not the new implementation.

## The Applicability Problem

The uniform all-pair benchmark has a strong exact inverse. The first unequal
height extension still required every group pair to exist. Native records such
as transactions can instead supply two endpoints directly, with very uneven
pair counts and many missing pairs. This is a plausible input shape, not
evidence that customers want this particular PageRank operator.

This note removes complete pair coverage. Its goal is a justified, streamed
spectral-gap certificate from F groups and P native pair classes, without
building the much larger Boolean overlap graph on n original records.
The original graph/query semantics remain undirected, unweighted Boolean
overlap. Direction, transaction amounts and time windows must not be silently
substituted into this operator.

## Contract And Main Bound

Let H be the simple group graph. Each present pair c={a,b} is an edge of H
with positive integer weight h_c, the number of original vertices in that
pair class. Require F>=3, connected H, no unused groups, no singleton/empty
memberships, and all original degrees positive. Missing pairs are allowed.

```text
N_a   = sum_{c incident a} h_c       group population / weighted degree in H
n     = sum_c h_c
d_c   = N_a + N_b - h_c - 1         original Boolean degree
d_max = max_c d_c
N_min = min_a N_a
```

If delta_H is any proved lower bound on the normalized Laplacian gap of the
weighted group graph H, then the original Boolean graph G has gap at least

```text
delta_G = min(1, N_min * delta_H / d_max).
```

Consequently lambda_nonstationary(G) <= 1-delta_G. This uses the largest
algebraic transition eigenvalue, not its magnitude. The next sections give
both a derivation and an O(F)-retained-state way to certify delta_H from a
spanning tree while paying for every P-class scan.

## Why The Gap Transfers

As in the [unequal all-pair derivation](PageRank-Boolean-Heterogeneous-Gap.md),
within-class zero-sum functions have transition eigenvalue -1/d_c and hence
Laplacian gap greater than one. It remains to bound class-constant functions y.

Define a different, auxiliary class walk R: choose one endpoint of a pair
uniformly, then choose an incident pair proportional to its multiplicity.

```text
R_ce = (h_e/2) * sum_{a in c intersect e} 1/N_a.
```

R includes self transitions. It is NOT the requested Boolean graph walk.
Its stationary class weights are proportional to h_c. Writing B for the
unsigned F-by-P incidence matrix and D_N=diag(N), its symmetric Gram matrix is

```text
R_sim = (1/2) diag(sqrt(h)) B^T D_N^-1 B diag(sqrt(h)).
```

The reversed Gram product is
(I + D_N^-1/2 A_H D_N^-1/2)/2. Nonzero eigenvalues agree. Remaining class
eigenvalues are zero. Therefore R has gap at least delta_H/2; possible
bipartiteness of H does not invalidate this statement.

For distinct classes, a shared endpoint is unique. Their original Dirichlet
conductance is h_c h_e, whereas R's conductance under measure h is
h_c h_e/(2N_a). Thus, for every class function,

```text
E_G(y) >= 2 N_min E_R(y)
       >= N_min delta_H min_t sum_c h_c (y_c-t)^2.

Var_G(y) = min_t sum_c h_c d_c (y_c-t)^2
         <= d_max min_t sum_c h_c (y_c-t)^2.
```

The Rayleigh quotient and the within-class invariant spaces prove delta_G.
The auxiliary walk is only a proof device. Executing R instead of the original
Boolean operator would generally compute a different PageRank answer.

### Tightening For Non-Mediating Groups

The [independent review](PageRank-Boolean-Native-Gap-Review.md) observes that
only groups incident to at least two distinct classes occur in the
off-diagonal conductance comparison. Let M be those groups and replace N_min
by N_med=min_{a in M} N_a. Connected F>=3 guarantees M is nonempty. The same
proof gives `delta_G=min(1,N_med*delta_H/d_max)`, never worse than the original.
This costs one F-class-incidence counter array in the first pass, released
before tree moments. The implementation retains both scalar bounds in its
receipt. The checker below intentionally preserves the earlier conservative
formula; the independent review and current tests also check the refinement.

Complete-pair coverage permits the maximum with
`min(1,m^2*F/(M_height*d_max))`. The reviewed extension includes F=3, but its
clip at one is not the exact gap of the uniform clique K_(3h), whose gap is
3h/(3h-1)>1. No exact-gap claim is made for that endpoint.

## A Streamed Canonical-Tree Certificate

Select a spanning tree T of H. The proposed implementation uses the first
connecting edges in the existing canonical pair stream, with union-find on
F groups. Tree quality is not assumed. A better tree is a separately paid
option; non-tree edges continue contributing to original degrees N.

Let V=sum_a N_a=2n. For each tree edge e=(parent p, child v), let h_e be its
original group-graph weight and define

```text
A_v = sum_{a in subtree(v)} N_a
U_v = sum_{a in subtree(v)} N_a * distance_T(v,a)
D_p = sum_a N_a * distance_T(p,a)
B_v = V-A_v.

J_e = B_v*U_v + A_v*(D_p-U_v-A_v) + A_v*B_v
C_e = J_e / (V*h_e)
C   = max_{e in T} C_e
delta_H = 1/C.
```

Distances count tree edges, not inverse edge weights. The edge weight h_e
appears in the congestion denominator. Mixing the two conventions changes
the proof and must not happen silently.

### Proof Of The Congestion Formula

Degree-weighted variance is
`V^-1 sum_{a<b} N_a N_b (z_a-z_b)^2`. Along the unique tree path, ordinary
Cauchy-Schwarz bounds `(z_a-z_b)^2` by path length times the sum of squared
edge differences. Collecting the coefficient of each tree edge gives

```text
Var_N(z) <= sum_{e in T} [J_e/V] * (z_parent-z_child)^2
         <= C * sum_{e in T} h_e * (z_parent-z_child)^2
         <= C * E_H(z).
```

For a pair crossing e, path length is distance(a,v)+1+distance(p,b). Its
degree-weighted sum is exactly J_e above: U_v is the inside distance moment,
and D_p-U_v-A_v is the outside distance moment. No F-by-F pair list is needed.

Postorder computes A and U. Rerooting computes every D using
`D_child=D_parent+V-2*A_child`. A final tree scan obtains C with exact rational
arithmetic. These are standard tree moment and canonical-path techniques;
the claimed research direction is their scoped original-operator integration,
not a new generic spectral comparison primitive.

## Work, State And Failure Behavior

- First canonical class pass: validate distinct ordered pairs/heights; accumulate
  F group populations; select F-1 tree edges by union-find; compute n and class
  weight totals. Charge O(P alpha(F)) union-find work and exact integer costs.
- Second pass: check every original degree against N_a+N_b-h_c-1, counts,
  original volume, source identity and stream consistency. Charge P reads.
- Tree phase: O(F) arithmetic operations for rooting, moments, rerooting and
  congestion; O(F) retained arrays/tree arcs and exact integer/rational bits.
  There is no P-state vector, expanded edge list or dense matrix.
- In a complete-pair source, the separately proved all-pair height bound can
  be combined by taking the maximum of two valid lower gaps. Its eligibility
  must be checked. It often compensates for a poor tree on a dense source.
- Return scalar gap and volume metadata; release tree/counter arrays before
  allocating the original residual certificate's group endpoint arrays.
- Disconnected, zero-degree, malformed or over-reservation sources refuse
  this global-gap plan. Componentwise stationary modes require separate work.

O(F) words is not a physical RAM ceiling. Input source/indexes, class scratch,
2n original output reads, complete 16n-byte result, allocator/runtime state,
cache and all preparation remain additional. A weak tree can make C large,
producing a mathematically valid but unusable error bound. No timing or useful
real-customer result follows from this proof. The separate native implementation
study now supplies synthetic complete-workflow timings and retained refusals;
it still does not establish a physical RAM reduction.

## Actual Output, Not Auxiliary-Walk Output

The implemented certificate computes the original Boolean residual r from
staged binary64 x and uses stationary pi_v=d_class(v)/vol_G:

```text
V_residual = sum_v r_v^2 * vol_G/d_class(v) - [(1-alpha)(1-sum(x))]^2

error_L1 <= abs(sum(x)-1)
            + sqrt(V_residual)/[(1-alpha)+alpha*delta_G].
```

The implementation uses directed weighted squares, a lower bound for the
subtracted mass-square, and the actual-byte ownership/hash contract. It was
not obtained by changing only n or lambda in the uniform verifier. Full
measurement and correctness scope is recorded in the implementation note.

## Prior Art And Review Questions

The initial lead search on 2026-09-21 found close, important precedent:

- [Evans and Lambiotte, Line graphs, link partitions, and overlapping communities](https://journals.aps.org/pre/abstract/10.1103/PhysRevE.80.016105)
  describes link-space and weighted line-graph constructions. An indexed
  author-PDF excerpt exposes its incidence-walk equation (14); full PDF fetch
  timed out in this lead pass. That excerpt is not full-paper inspection.
- [Their weighted-network follow-up](https://arxiv.org/abs/0912.4389)
  discusses multiple weighted line-graph representations. Abstract inspected
  through the search result; operator equality beyond the derivation above
  still needs full-text comparison.
- [Sinclair, Improved bounds for mixing rates of Markov chains and multicommodity flow](https://publish.lfcs.inf.ed.ac.uk/reports/91/ECS-LFCS-91-178/)
  is direct canonical-path/flow precedent. The accessible primary report
  record describes flow-based mixing bounds. A PDF-copy fetch failed; no
  claim of inspected theorem text or priority clearance is made.

Review must challenge constants/factors of two, unequal degree measures,
auxiliary-versus-target semantics, tree moment bookkeeping, source eligibility
and stronger equally informed gap certificates. A positive finite checker does
not answer these literature or product questions.

The completed independent review subsequently inspected reachable full text:
[Evans-Lambiotte 2009 equation 14](https://arxiv.org/html/0903.2181#S3.SS3),
[the weighted follow-up Section 5](https://arxiv.org/html/0912.4389#S5), and
[Sinclair's congestion theorem, Section 2](https://people.eecs.berkeley.edu/~sinclair/flow.pdf#page=8).
It identifies the auxiliary walk itself in the first two and the exact
canonical-path congestion mechanism in the third. The earlier retrieval
limitations above are historical, not the final literature assessment.
Operator composition, tree moments and reversible comparison are established
ingredients. Scoped integration is a research candidate, not priority clearance.

## Exact Finite Checker

The checker enumerates all group-edge multiplicities in {0,1,2} for F=4,
retaining connected cases, plus seeded F=5 cases. It compares moment congestion
with explicit all-pair tree paths, then checks positive semidefiniteness of
`L_G-delta_G*(D_G-d*d^T/vol_G)` using exact arithmetic. It rejects the false
application of an all-pair gap to a sparse path. Dense oracle storage below
is validation-only, not the proposed streamed implementation.

```python
from fractions import Fraction as Q
from itertools import combinations, product
import random


def check_rational_positive_semidefinite(matrix):
    a = [[Q(x) for x in row] for row in matrix]
    for k in range(len(a)):
        if a[k][k] < 0:
            return False
        if not a[k][k]:
            if any(a[k][j] for j in range(k+1,len(a))):
                return False
            continue
        for i in range(k+1,len(a)):
            for j in range(i,len(a)):
                a[i][j] -= a[i][k]*a[k][j]/a[k][k]
                a[j][i] = a[i][j]
    return True


def calculate_native_tree_certificate(factors, edges):
    roots = list(range(factors))
    populations = [0]*factors
    tree = [[] for _ in range(factors)]
    def find_group_component_root(v):
        while roots[v] != v:
            roots[v] = roots[roots[v]]
            v = roots[v]
        return v
    for a,b,h in edges:
        populations[a] += h
        populations[b] += h
        ra,rb = find_group_component_root(a),find_group_component_root(b)
        if ra != rb:
            roots[ra] = rb
            tree[a].append((b,h))
            tree[b].append((a,h))
    if len({find_group_component_root(v) for v in range(factors)}) != 1:
        return None
    parent,weight,order = [0]*factors,[0]*factors,[0]
    for v in order:
        for u,h in tree[v]:
            if u != parent[v]:
                parent[u],weight[u] = v,h
                order.append(u)
    volume = sum(populations)
    subtree,inside = populations[:],[0]*factors
    for v in reversed(order[1:]):
        p = parent[v]
        subtree[p] += subtree[v]
        inside[p] += inside[v]+subtree[v]
    distances = [0]*factors
    distances[0] = inside[0]
    congestion = {}
    for v in order[1:]:
        p,A = parent[v],subtree[v]
        B,U = volume-A,inside[v]
        distances[v] = distances[p]+volume-2*A
        J = B*U + A*(distances[p]-U-A) + A*B
        congestion[frozenset((p,v))] = Q(J,volume*weight[v])
    # Validation-only explicit path summation; not part of the O(F) schedule.
    brute = {edge:Q() for edge in congestion}
    for a,b in combinations(range(factors),2):
        stack = [(a,-1,[])]
        while stack:
            v,p,path = stack.pop()
            if v == b:
                for edge,h in path:
                    brute[edge] += Q(populations[a]*populations[b]*len(path),volume*h)
                break
            stack.extend((u,v,path+[(frozenset((v,u)),h)]) for u,h in tree[v] if u != p)
    assert congestion == brute
    delta_h = 1/max(congestion.values())
    dmax = max(populations[a]+populations[b]-h-1 for a,b,h in edges)
    delta_g = min(Q(1),Q(min(populations),dmax)*delta_h)
    return delta_h,delta_g


def construct_original_gap_matrix(edges, delta):
    signatures = [(a,b) for a,b,h in edges for _ in range(h)]
    adjacency = [[int(i!=j and bool(set(a)&set(b))) for j,b in enumerate(signatures)]
                 for i,a in enumerate(signatures)]
    degrees = list(map(sum,adjacency))
    assert min(degrees)>0
    volume,n = sum(degrees),len(degrees)
    return [[Q((degrees[i] if i==j else 0)-adjacency[i][j])
             - delta*(Q(degrees[i] if i==j else 0)-Q(degrees[i]*degrees[j],volume))
             for j in range(n)] for i in range(n)]


fixtures = [(4,weights) for weights in product((0,1,2),repeat=6)]
rng = random.Random(20260922)
fixtures += [(5,tuple(rng.randrange(4) for _ in range(10))) for _ in range(32)]
passed = disconnected = 0
for factors,weights in fixtures:
    edges = [(a,b,h) for (a,b),h in zip(combinations(range(factors),2),weights) if h]
    answer = calculate_native_tree_certificate(factors,edges)
    if answer is None:
        disconnected += 1
        continue
    dh,dg = answer
    assert 0<dh<=2 and 0<dg<=1
    assert check_rational_positive_semidefinite(construct_original_gap_matrix(edges,dg))
    passed += 1
sparse_path = [(a,a+1,1) for a in range(4)]
assert not check_rational_positive_semidefinite(construct_original_gap_matrix(sparse_path,Q(1)))
print('Exact native-gap/moment checks:',passed,'disconnected cases refused:',disconnected)
print('False complete-pair gap on sparse path: rejected')
print('No production integration, physical resource or timing claim.')
```

Lead replay of the complete checker with Python 3.11 and bytecode writes
disabled exited 0:

```text
Exact native-gap/moment checks: 656 disconnected cases refused: 105
False complete-pair gap on sparse path: rejected
No production integration, physical resource or timing claim.
```

The 656 checks validate the stated bound on those complete vector spaces and
moment sums on those trees. They are not 656 customer datasets or independent
theorem review. The code is a resident oracle, not the streamed validator.
