# Boolean PageRank Mass Deflation Frontier

Date: 2026-09-21. Mathematical follow-up to a measured certificate refusal,
not a new generic spectral theorem. The subsequent [actual-byte implementation
and comparison](PageRank-Boolean-Deflation-Implementation.md) is now measured;
the rational checker in this note remains a diagnostic, not that publisher.
The seven-family contribution goal remains open.

## Why This Is The Next Question

The [frozen streamed experiment](PageRank-Boolean-Streaming-Evidence.md)
refuses every near-one attempt. Yet a subsequent independent expanded rational
oracle finds actual L1 errors of about `6.29e-17` for F and `8.33e-16` for P,
both below requested epsilon `1e-10`. Their generic residual certificates
bound error by approximately 0.451 and 0.500. There is no permission to publish
merely because a tiny dense oracle happened to succeed.

The mismatch suggests three approaches, with materially different costs:

1. Higher precision in the verifier: reduces arithmetic enclosure width, but
   cannot remove the actual residual already present in rounded output bytes.
2. More accurate solver/output arithmetic: can help, but still faces a very
   unfavorable generic `1/(1-alpha)` amplification. More CG iterations alone
   are not a certificate.
3. **Separate conserved mass from the other graph modes.** Charge mass error
   directly and use a rigorously justified graph spectral bound for the rest.
   This led to the separately reviewed and implemented experiment linked above;
   it is not a new default or production engine feature.

The mechanism is standard reversible spectral analysis. The possible systems
contribution would be obtaining a useful spectral certificate from the same
compact source, checking actual output with bounded storage, and selecting
this plan without adding expensive failed attempts to ordinary jobs.

## Exact Scope And Theorem

Let A be the adjacency matrix of a finite connected undirected graph with
strictly positive degrees. Let T=A*D^-1 be its column-stochastic random-walk
operator and pi_i=d_i/sum_j d_j its normalized stationary distribution.
Assume an independently justified upper bound `lambda_bar<1` on the largest
nonstationary eigenvalue of T. The bound is on the largest eigenvalue, not
its absolute value. Bipartite eigenvalue -1 is harmless to this resolvent.

For normalized personalization p, `0<=alpha<1`, exact PageRank x*, and an
arbitrary finite provisional vector x, define

```text
s     = sum_i x_i
r     = (1-alpha)*p + alpha*T*x - x
rho   = sum_i r_i = (1-alpha)*(1-s)
r0    = r - rho*pi
V     = sum_i r_i^2/pi_i - rho^2
      = sum_i r0_i^2/pi_i >= 0

||x-x*||_1 <= |s-1| + sqrt(V)/(1-alpha*lambda_bar).
```

The candidate need not have exact mass one. Dropping the mass term is
incorrect. Source degrees, pi, p, residuals, and the spectral bound must refer
to the SAME frozen graph/query. A sampled eigenvalue or heuristic mixing
estimate is not an admissible upper bound for a deterministic certificate.

### Proof

Write `e=x-x*=(s-1)*pi+e0`, with `sum e0=0`. The stationary relation gives
`r=-(I-alpha*T)e`, and consequently `r0=-(I-alpha*T)e0`.

Under the norm `||v||_(pi^-1)^2=sum_i v_i^2/pi_i`, reversibility makes T
self-adjoint. On the zero-mass subspace, every eigenvalue mu is at most
lambda_bar; each inverse multiplier is
`1/(1-alpha*mu) <= 1/(1-alpha*lambda_bar)`. Thus
`||e0||_(pi^-1) <= ||r0||_(pi^-1)/(1-alpha*lambda_bar)`.
Cauchy-Schwarz with `sum pi=1` gives `||e0||_1<=||e0||_(pi^-1)`.
Triangle inequality adds `|(s-1)|*||pi||_1=|s-1|`.
Expanding the squared centered residual gives V. This proves the bound.

It is not an improvement for every source. A small spectral gap, tiny pi,
or a loose residual enclosure can make it weak. For disconnected graphs,
there is a stationary mode per component; one global mass term is insufficient.
Componentwise masses and justified gaps would be needed. Isolates require
separate dangling/personalization handling. This first theorem does not
silently include those cases.

## A Checkable Boolean Source Family

For F>=4, take every unordered pair of distinct group IDs exactly once as a
signature and place h>=1 original vertices in each signature. There are no
singletons/isolates and all class heights are h. This is a supplied regular
Boolean two-membership graph, not an assumption about arbitrary data.

```text
n = h * F*(F-1)/2
d = h*(2*F-3)-1
pi_i = 1/n
lambda_bar = (h*(F-3)-1) / d.
```

To derive the spectrum, let B be the unsigned F-by-binomial(F,2) incidence
matrix of the complete graph on group IDs. Then `B*B^T=(F-2)*I+J` and the
pair-class adjacency is `B^T*B-2I`. Replacing each class by its h-clique makes
the original adjacency `(B^T*B-I) tensor J_h - I`. Its eigenvalues are d,
`h*(F-3)-1`, `-h-1`, and `-1` with their applicable multiplicities. For F>=4
the stated numerator is the largest nonprincipal eigenvalue. This supplies
the required bound exactly; no numerical eigensolver or uncharged dense
matrix is required by the derivation.

Eligibility can be checked by a canonical class stream enumerating ALL
unordered pair signatures and verifying the same positive h and derived d.
That is paid O(P) record work, not O(1) source validation. A frozen builder
could persist this eligibility receipt, but refreshing it also has a cost.
This narrow family is an initial falsification fixture. A general low-cost
gap certificate for useful native sources is still an open design problem.

## Storage And Numerical Options

The existing two-pass actual-output schedule already computes each original
residual from O(F) factor intervals and sequential class scratch. It could
also accumulate total score mass and a weighted squared residual bound. On
a regular graph, pi is exact `1/n`, so no resident n-vector is needed.

The exact formula is not automatically a correct directed implementation.
To form an upper bound on V, upper-bound `sum r_i^2/pi_i` and subtract a
LOWER bound on rho^2; enclose mass separately. Do not subtract two nearest-
rounded numbers or silently clip an invalid negative upper bound to zero.
A centered per-row accumulation is an alternative with different interval
width and pass costs. Choose only after actual-output tests.

The following checker uses exact Fractions and an upward dyadic square root.
It is a resident oracle, NOT the bounded numerical implementation. Generic
rational denominators may grow with input degrees; a constant number of
Fractions does not establish constant bytes or an acceptable CPU cost.

Full result emission remains Omega(n). The new scalar formula does not itself
remove the existing F interval arrays, C-record scratch, output bytes or
source preparation. A second possible plan is a stationary-prior envelope
`||x-pi||_1 + (1-alpha)/(1-alpha*lambda_bar)*sqrt(sum p_i^2/pi_i-1)`.
It could avoid residual factor arrays for eligible nearly stationary jobs,
but can be too loose at ordinary alpha and requires its own directed proof,
complete byte verification and strongest known comparison. It is not enabled.

## Falsification And Prior Art

- **Mass omission:** x=pi/2 with p=pi has zero centered residual but L1 error
  1/2. Any bound returning zero here is wrong.
- **Wrong graph:** a missing pair signature invalidates the closed-form gap;
  merely having two memberships per vertex is not sufficient.
- **Disconnected input:** one global stationary component does not capture
  independent component-mass errors.
- **Eigenvalue estimates:** power iteration typically supplies a numerical
  estimate, not a certified upper bound on the largest omitted eigenvalue.
- **Accuracy versus cost:** an exact dense oracle demonstrates a formula,
  not an economical publisher. All old refusal timings remain unchanged.

[Grolmusz, A Note on the PageRank of Undirected Graphs, arXiv v2 (2012),
equations (5)-(8) and Theorem 2](https://arxiv.org/pdf/1205.1960) already
relates personalized PageRank to the stationary degree distribution through
the resolvent and bounds their L1 difference. The four-page PDF was read
end-to-end. It is direct precedent, not evidence that degree-proportional
scores equal arbitrary personalized PageRank. Our mass/zero-mode split above
is an explicit standard spectral deduction, not a claimed new theorem.

[Aldous and Fill, Reversible Markov Chains and Random Walks on Graphs](https://www.stat.berkeley.edu/users/aldous/RWG/book.pdf)
is a primary mathematical reference for reversibility and spectral methods.
This turn retrieved its table of contents, which locates spectral
representation in Section 3.4; subsequent section/image retrieval timed out.
No claim of having newly read that whole monograph or section is made.
The proof above is supplied explicitly rather than delegated to that citation.

## Executable Rational Check

Run from repository root. The oracle uses the lead's existing expanded
stationary solver; this is a new cross-check, not another independent author.
It also checks the supplied eigenvalue upper bounds by exact PSD elimination
on the zero-sum subspace. No NumPy, output publication or timing comparison.

```python
from fractions import Fraction as Q
from itertools import combinations
from pathlib import Path
import math
import sys

sys.path.insert(0, 'research_algorithms_20260920/experiments')
from test_stream_boolean_rank_solver import compute_expanded_rational_pagerank

def enclose_rational_square_root(value, bits=128):
    assert value >= 0
    scale = 1 << bits
    integer = (value.numerator * scale * scale + value.denominator - 1) // value.denominator
    root = math.isqrt(integer)
    if root * root < integer:
        root += 1
    result = Q(root, scale)
    assert result * result >= value
    return result

def check_zero_subspace_positive(adjacency, degree, bound):
    n = len(adjacency)
    matrix = [[bound * (int(i == j) + 1) - Q(adjacency[i][j] - adjacency[i][-1]
              - adjacency[-1][j] + adjacency[-1][-1], degree)
               for j in range(n-1)] for i in range(n-1)]
    while matrix:
        assert all(matrix[i][i] >= 0 for i in range(len(matrix)))
        pivot = next((i for i in range(len(matrix)) if matrix[i][i]), None)
        if pivot is None:
            assert all(not value for row in matrix for value in row)
            break
        matrix[0], matrix[pivot] = matrix[pivot], matrix[0]
        for row in matrix:
            row[0], row[pivot] = row[pivot], row[0]
        matrix = [[matrix[i][j] - matrix[i][0]*matrix[0][j]/matrix[0][0]
                   for j in range(1,len(matrix))] for i in range(1,len(matrix))]

def calculate_exact_deflated_bound(rows, alpha, values, spectral):
    n, a = len(rows), Q(alpha)
    adjacency = [[int(i != j and bool(set(rows[i][1]) & set(rows[j][1])))
                  for j in range(n)] for i in range(n)]
    degrees = list(map(sum, adjacency))
    assert len(set(degrees)) == 1 and degrees[0] > 0
    total = sum(Q(row[2]) for row in rows)
    x = [Q(values[row[0]]) for row in rows]
    p = [Q(row[2])/total for row in rows]
    residual = [(1-a)*p[i] + a*sum(Q(adjacency[i][j],degrees[j])*x[j]
                for j in range(n)) - x[i] for i in range(n)]
    mass, rho = sum(x), sum(residual)
    assert rho == (1-a)*(1-mass)
    variance = n*sum(r*r for r in residual) - rho*rho
    assert variance >= 0
    bound = abs(mass-1) + enclose_rational_square_root(variance)/(1-a*spectral)
    generic = sum(map(abs,residual))/(1-a)
    return bound, generic, variance

families = []
for n in (2,3,5):
    families.append(([(i,(0,),1.) for i in range(n)], Q(-1,n-1)))
for f,h in ((4,1),(4,2),(5,1),(6,1)):
    signatures = [pair for pair in combinations(range(f),2) for _ in range(h)]
    families.append(([(i,pair,1.) for i,pair in enumerate(signatures)],
                     Q(h*(f-3)-1,h*(2*f-3)-1)))
checks = 0
for base,spectral in families:
    adj = [[int(i != j and bool(set(u[1]) & set(v[1]))) for j,v in enumerate(base)]
            for i,u in enumerate(base)]
    check_zero_subspace_positive(adj, sum(adj[0]), spectral)
    for skew in (False, True):
        rows = [(v,g,float(1+(v%5) if skew else 1)) for v,g,_ in base]
        for alpha in (0.,.5,.85,math.nextafter(1.,0.)):
            truth = compute_expanded_rational_pagerank(rows,alpha)
            for mode in ('rounded','half','perturbed'):
                values = {v:Q(float(x)) for v,x in truth.items()}
                if mode == 'half':
                    values = {v:x/2 for v,x in values.items()}
                if mode == 'perturbed':
                    values[rows[0][0]] += Q(1,100)
                bound,_,_ = calculate_exact_deflated_bound(rows,alpha,values,spectral)
                error = sum(abs(values[v]-truth[v]) for v in truth)
                assert error <= bound
                checks += 1
rows = [(0,(0,),1.),(1,(0,),1.)]
bound,_,variance = calculate_exact_deflated_bound(rows,.85,{0:Q(1,4),1:Q(1,4)},Q(-1))
assert variance == 0 and bound == Q(1,2)
print('Exact PSD source checks:', len(families), 'error-bound cases:', checks)
print('Mass-omission negative control: centered variance zero, required error bound 1/2')

from boolean_rank_sqlite_source import SqliteBooleanRankSource
from stream_boolean_rank_solver import solve_boolean_rank_state, iterate_boolean_rank_output
path = Path('research_algorithms_20260920/evidence/boolean-rank-sqlite-20260921/near-one.sqlite')
with SqliteBooleanRankSource(path) as source:
    classes = list(source.iterate_class_records())
    assert source.factor_count == 8 and source.vertex_count == 56
    assert [record[1] for record in classes] == list(combinations(range(8),2))
    assert all(record[2:4] == (2,25) for record in classes)
    rows = [(v,groups,w) for cid,groups,_,_,_ in classes for v,w in source.iterate_class_vertex_rows(cid)]
    alpha = math.nextafter(1.,0.)
    truth = compute_expanded_rational_pagerank(rows,alpha)
    for method in ('factor-cg','class-cg'):
        state = solve_boolean_rank_state(source,alpha=alpha,method=method,
                    max_factor_slots=8,max_class_slots=28)
        values = dict(iterate_boolean_rank_output(source,state))
        bound,generic,_ = calculate_exact_deflated_bound(rows,alpha,values,Q(9,25))
        error = sum(abs(Q(values[v])-truth[v]) for v in truth)
        assert error <= bound < Q(1,10**10) < generic
        print(method, 'actual',float(error),'generic',float(generic),'deflated',float(bound))
print('No output published; exact-rational diagnostic only.')
```

## Executed Receipt

Lead execution on Python 3.11, after the frozen timing study completed:

```text
Exact PSD source checks: 7 error-bound cases: 168
Mass-omission negative control: centered variance zero, required error bound 1/2
factor-cg actual 6.289259834586463e-17 generic 0.45072079401236576 deflated 1.4097849144397902e-16
class-cg actual 8.326672684688674e-16 generic 0.4995932313699968 deflated 9.326207751613067e-16
No output published; exact-rational diagnostic only.
```

These are seven constructed spectral-source checks and 168 candidate-bound
checks, not 168 independent customer datasets. The two last comparisons use
the existing 56-vertex prepared source and the current repaired solvers.
All original benchmark bytes, hashes, acceptance counts and timings remain
unchanged. Subsequent directed arithmetic, independent reviews and full-output
measurements are recorded in the linked implementation note. Physical resource
enforcement remains unproved. These diagnostic outputs are not relabeled as
published bytes.

Reproduction:

```sh
awk '/^```python$/{block=1;next} /^```$/{if(block)exit} block' research_algorithms_20260920/PageRank-Boolean-Mass-Deflation.md | /Users/amuldotexe/.local/bin/python3.11 -B
```

## Next Acceptance Gate

The first three gates (independent challenge, directed implementation, and
uniform-source complete-publication comparison) are now executed in the linked
review/implementation documents: 113 focused tests and 180 fresh serial attempts.
The stronger exact-target control prevents promoting deflation as a universal
winner on this symmetric source.

1. Independently review the [unequal-class gap](PageRank-Boolean-Heterogeneous-Gap.md),
   then implement its degree-weighted actual-byte variance with explicit source
   validation and the same stable-output boundary.
2. Compare general F/P and equally informed direct/certificate controls on the
   broader source; retain all rejected bounds and slow cases.
3. Find a useful native or public source with a paid justified gap before
   claiming product relevance, physical RAM improvement or scientific priority.

This proposal does not justify relabeling the frozen experiment's 18 refusals
as successes, enabling a new default, or declaring a paper contribution.
