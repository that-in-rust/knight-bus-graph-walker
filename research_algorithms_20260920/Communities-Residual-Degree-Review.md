# Independent Review: Residual-Degree Partition Bounds

Review date: 2026-09-21. Scope: mathematical challenge and targeted primary-source
prior-art search only. This sidecar does not read or import the lead implementation,
tests, probe, or manuscript. No build, timing/RSS measurement, commit, or push.

## 1. Premise Check and Verdict

**The two proposed endpoints are correct and attained for unrestricted,
nonnegative rational edge weights on a finite loopless graph.** A constructive
proof is given below; finite checks are supplementary, not the proof. The same
proof gives real-weight endpoints. All rational values between the endpoints
are attainable by rational convex combinations of the endpoint graphs.

Let a partition P have nonempty blocks C, and write

\[
R=\sum_i r_i=2m,\quad s_C=\sum_{i\in C}r_i,\quad
a_C=\max_{i\in C}r_i,\quad \ell_C=(2a_C-s_C)_+.
\]

Assume r_i >= 0 and max_i r_i <= m. Every distinct pair is an available edge
with arbitrary nonnegative rational weight; zero weight is allowed. There are
no individual edge upper capacities, 0/1-weight or integrality requirements,
connectivity requirements, or additional prescribed cuts. Edge mass counts
each unordered edge once. For total internal mass I(P), the exact range is

\[
L(P)=\max(0,\max_C s_C-m),\qquad
U(P)=m-\max\left(\max_C\ell_C,\frac12\sum_C\ell_C\right).
\]

With forbidden residual pairs these remain valid *outer bounds*, but either
endpoint can cease to be attainable. They are not integer-completion formulas.
They are also not a claim that one completion simultaneously attains endpoints
for different partitions.

The review uses four lenses: degree feasibility, extremal/constructive
optimization, adversarial model changes, and prior-art attribution. The chosen
route is a self-contained construction plus independent exact LP-vertex
enumeration. The proof explicitly establishes attainment, not just necessary
inequalities; finite LP checks alone would not prove the universal statement.

## 2. Constructive Building Blocks

### Lemma A: Loopless and Multipartite Completion

For nonnegative demands u_i of total 2t, unrestricted loopless completion is
possible if and only if max_i u_i <= t. Necessity follows because all degree
at vertex i must be matched to degree outside i.

For sufficiency, partition the interval [0,2t) consecutively into vertex
intervals of lengths u_i. For each z in [0,t), pair the vertex containing z
with the vertex containing z+t. Give a pair weight equal to the length of the
set of z producing that unordered pair. Each point of [0,2t) is an endpoint
exactly once, so degrees are exactly u_i. An interval of length at most t
cannot contain both z and z+t, apart from irrelevant endpoints, so no loop
appears. There are finitely many breakpoints. Rational input makes every
breakpoint, and hence every weight, rational. For t=0 use the zero graph.

More generally, suppose the vertices have blocks and only pairs in distinct
blocks are allowed. A completion exists exactly when every block demand is
at most t. For sufficiency put each block's vertex intervals consecutively,
then use the same antipodal pairing. A block interval has length at most t,
so every produced edge crosses blocks. Necessity is the same outside-demand
argument. In particular, the block totals are the only constraints for this
complete-multipartite support, regardless of the individual demands in a block.

### Lemma B: Exposing Any Feasible Block Boundary Mass

Consider one block with demand vector r, sum s, largest entry a, and
ell = max(0,2a-s). For every rational b in [ell,s], there is an internal
loopless weighted graph using some degrees h_i <= r_i and leaving total
outgoing demand b. Its internal mass is t=(s-b)/2.

Indeed, ell <= b <= s is equivalent to

\[
0\le t\le\min(s/2,s-a).
\]

We can choose h_i in [0,min(r_i,t)] with sum_i h_i=2t because

\[
\sum_i\min(r_i,t)\ge 2t.
\]

To prove the last inequality: if two entries are at least t, they suffice;
if exactly one is at least t, the sum is t+s-a >= 2t; if none is at least t,
the sum is s >= 2t. The case t=0 is immediate. Fill these capacities in a
fixed vertex order until 2t is reached. This is a finite rational procedure.
Now max_i h_i <= t, so Lemma A realizes the internal degrees. The remaining
degrees q_i=r_i-h_i are nonnegative and have total b.

Conversely, any such internal graph has boundary demand at least ell. If v
has degree a and its internal degree is h_v, looplessness implies
h_v <= sum_{i != v} h_i <= s-a. Therefore its outgoing demand alone is
at least a-(s-a) when that is positive.

Together the lemmas give an exact reduction: choose block boundary totals
b_C in [ell_C,s_C]. They extend to a whole graph if and only if

\[
\max_C b_C\le \frac12\sum_C b_C.
\]

For the sufficient direction, first use Lemma B separately inside each block,
then use Lemma A's multipartite version on the leftover vertex demands. If
B=sum_C b_C, the crossing mass is B/2 and the internal mass is m-B/2.

## 3. Lower Endpoint: Necessity and Attainment

For any block C let I_C be its internal mass and b_C its boundary mass.
Degree accounting gives s_C=2I_C+b_C. The boundary cannot consume more than
the degree outside C, so b_C <= 2m-s_C. Consequently

\[
I(P)\ge I_C\ge s_C-m,\quad I(P)\ge0.
\]

**Case 1: max_C s_C <= m.** Apply Lemma A directly with the partition blocks
and demands r_i. All edges cross blocks, so I(P)=0=L(P).

**Case 2: a block D has s_D>m.** It is unique. Put t=s_D-m. The assumptions
a_D <= m and s_D <= 2m imply

\[
t\le s_D-a_D,\qquad t\le s_D/2.
\]

Lemma B constructs internal mass t inside D, leaving demand
b_D=s_D-2t=2m-s_D. Use no internal edges outside D. The total outside degree
is also 2m-s_D. Match these two sides by a rational transportation fill (or
Lemma A with two blocks D and its complement). Every new edge crosses the
original partition. The resulting total internal mass is t=L(P).

This also covers s_D=2m: the outside demand is zero and the graph is entirely
inside D. Thus the lower endpoint is attained in every case.

## 4. Upper Endpoint: Necessity and Attainment

Let E=max_C ell_C and T=sum_C ell_C, and let X denote crossing mass.
Lemma B's necessary direction implies b_C >= ell_C. Since b_C <= X and
sum_C b_C=2X,

\[
X\ge E,\qquad X\ge T/2,\qquad
I(P)=m-X\le m-\max(E,T/2).
\]

To attain this bound it suffices to choose feasible block boundary totals
with sum 2 max(E,T/2).

**Case 1: 2E <= T.** Set b_C=ell_C. These totals satisfy
max_C b_C <= (sum_C b_C)/2. The two lemmas complete them to a graph with
X=T/2. This includes T=E=0, when every block completes internally.

**Case 2: 2E > T.** There is a unique block D with ell_D=E>0. Set b_D=E.
The other blocks must expose a total of E. Their minimum total is T-E<E,
whereas their maximum total is

\[
\sum_{C\ne D}s_C=2m-s_D\ge 2a_D-s_D=E,
\]

using a_D <= m. Start with b_C=ell_C for C != D, and increase these values,
one block at a time, never exceeding s_C, until their sum is E. This finite
rational fill exists by the preceding inequality. Each of these nonnegative
values is at most E because their total is E. Now sum_C b_C=2E and
max_C b_C=E, so the two lemmas give a completion with X=E.

Both cases attain U(P). Every step uses rational arithmetic, finite interval
lengths, or capped fills. This supplies an explicit rational construction,
not just an appeal to compactness or an approximate optimizer. Finally,
convexly combine the two endpoint graphs to get every intermediate mass
(rational coefficients for rational target masses).

## 5. Model Changes and Counterexamples

### Integer Weights and Odd Degree Mass

Integer demands need not admit integer-weight completion: r=(1,1,1) meets
max r <= R/2 but R=3 is odd. The rational realization is the triangle with
all weights 1/2. Every integer-weight graph has even total degree.

Even total degree does not restore the proposed *optimal endpoints* in the
integer model. Take six vertices of degree one, partitioned into two triples.
Here m=3 and both ell_C=0, so U=3. Two half-weight triangles attain 3
fractionally. In an integer graph, each triple has odd degree sum and hence
must have an odd, positive boundary count, since b_C=s_C-2I_C. At least one
crossing edge is forced; internal mass is at most 2, attained by one internal
edge per triple and one crossing edge. Thus integer U=2, strictly below 3.

Odd *individual* degrees are not themselves a problem: (1,1) is realized
by one unit edge. The obstructions concern parity of total or block demand.
For unrestricted loopless multigraph feasibility with integer demands, even
R and max r <= R/2 do suffice: the integer-breakpoint version of Lemma A
has integer edge weights. This does not assert integer optimality of U.

### Forbidden Pairs and Capacities

With r=(1,1,1,1) and P={{1,2},{3,4}}, the unrestricted range is [0,2].
If pairs 12 and 34 are forbidden, every allowed edge crosses, so the actual
range is [0,0]. If all crossing pairs are forbidden, only 12 and 34 remain,
and the actual range is [2,2]. Both restricted models are feasible. Thus
both relaxed endpoints can be strictly loose, even with tiny exact inputs.

Fully retained edges may impose precisely this kind of residual zero-pair
restriction. The actual feasible residual set is a subset of the unrestricted
set, so [L,U] still contains its scores. Pair capacities, simple 0/1 edges,
and further known constraints also give subsets, if feasible. The scalar
condition max r <= m is not sufficient to certify restricted feasibility.
Existence of a known actual residual graph supplies that feasibility witness.

### Loops

The proof requires loopless *residual* support. With the conventional degree
rule, a loop of mass w contributes 2w to degree and w to internal edge mass.
For two singleton communities with r=(1,1), the loopless formula has U=0;
allowing loops of mass 1/2 at both vertices gives internal mass 1. Therefore
unknown residual loops invalidate the loopless upper bound.

Known loops can be retained separately: include their mass once in J and
their degree twice in the retained degrees subtracted from original d.
Then apply the theorem only to the remaining loopless demands. When loops
are unrestricted unknowns, U=m instead; that is a different model. Merely
dropping a diagonal adjacency entry without checking its degree convention
is not a justified reduction.

### Zero Mass and Other Boundaries

- If R=0, nonnegativity forces all r_i=0 and L=U=0; use the zero graph.
- A zero-degree block has s=a=ell=0 and causes no problem.
- With an empty vertex set, define maxima over empty sets as zero for this
  statement; again the unique graph is empty. Empty labels can be omitted.
- One nonempty block gives L=U=m, because global feasibility makes its ell=0.
- All singleton blocks give L=U=0, because sum ell=2m and max ell<=m.
- Negative/signed weights and overlapping community memberships are outside
  the theorem. Rounded residual degrees require separate error accounting;
  clamping an infeasible vector is not proof of a bound for the original one.

## 6. Original-Modularity Envelope and Adaptivity

Let the original graph be a nonnegative undirected graph decomposed additively
as retained K plus actual residual H. Let k and r be their exact degrees,
d=k+r the original degrees, W=sum_i d_i/2>0, and J(P) the retained internal
mass, counting every edge once. Known loops are handled as above. Set
D_C=sum_{i in C} d_i. For the usual degree-product null model at resolution
gamma, grouping the double sum by community gives

\[
Q_{\rm original}(P)
=\frac{J(P)+I_H(P)}{W}
-\gamma\sum_C\left(\frac{D_C}{2W}\right)^2.
\]

Thus the proposed interval is exactly the affine image of [L(P),U(P)]:

\[
\boxed{
\frac{J(P)+L(P)}W-\gamma\sum_C(D_C/(2W))^2
\le Q_{\rm original}(P)\le
\frac{J(P)+U(P)}W-\gamma\sum_C(D_C/(2W))^2.}
\]

This is valid for any fixed finite real gamma, not only gamma>=0, because
the null-model term is fixed and the coefficient of I_H is positive. Do not
replace d or W by residual or retained degrees/mass: that changes the score.
If R=0 but W>0 the interval collapses to the known score. If W=0 the displayed
modularity expression is undefined; an application may choose a documented
convention, but it is not obtained by division or a limiting argument here.

The degree-weighted modularity definition predates this envelope; see
[Newman, Analysis of weighted networks (2004), Eq. (9) and following paragraph](https://arxiv.org/pdf/cond-mat/0407503)
and the resolution-parameter framework in
[Reichardt and Bornholdt (2006)](https://arxiv.org/pdf/cond-mat/0603718).
The displayed affine-envelope deduction is this review's direct algebra.

The quantifiers are deterministic: for every feasible H and every partition P,
L(P)<=I_H(P)<=U(P). Hence substituting a partition selected adaptively from
K, degrees, prior scores, previous partitions, or even H itself preserves the
inequality. There is no pointwise-probability event requiring a union bound.
This also holds for a randomized selector, separately for every output.

Uniform coverage does not make the selector optimal. If two candidate score
intervals are disjoint, lower(P)>upper(P') certifies their order, but merely
maximizing lower(P) does not certify the true maximizer. Nor must the endpoint
completions for different partitions coincide. Approximate or probabilistic
input-degree estimates need their own simultaneous-validity argument.

## 7. Targeted Primary Prior Art

Search performed 2026-09-21. Representative queries included `fractional
perfect b matching prescribed degrees complete multipartite graph`,
`nonnegative symmetric matrix prescribed row sums zero diagonal`,
`degree sequence partition maximum cut realization`, `Graph Realizations
Constrained by Skeleton Graphs`, and `Realizing Graphs with Cut Constraints`.
The following are primary papers, not search-result or encyclopedia citations.

| Primary source | Verified connection and scope |
| --- | --- |
| [S. L. Hakimi (1962), On Realizability of a Set of Integers as Degrees of the Vertices of a Linear Graph. I](https://epubs.siam.org/doi/10.1137/0110037), 10(3), 496-506 | Publisher abstract explicitly includes a weighted-graph variant and degree realizability. Historical warning against novelty claims about realizing degree data. Only the abstract/bibliographic record was checked; do not attribute these exact partition formulas to this paper from that evidence. |
| [Mordechai Lewin (1977), On the extreme points of the polytope of symmetric matrices with given row sums](https://www.sciencedirect.com/science/article/pii/0097316577900437), JCTA 23(2), 223-231 | Publisher abstract describes a graph characterization of extreme points of nonnegative symmetric matrices with prescribed row sums. This is a classical surrounding optimization framework, not verification of the present closed forms. |
| [Roger E. Behrend (2013 preprint), Fractional Perfect b-Matching Polytopes. I: General Theory](https://arxiv.org/pdf/1301.7356), Theorem 6, Proposition 11, Theorem 22 | Direct formulation: nonnegative edge variables with prescribed incident sums. Theorem 6 gives general support-sensitive feasibility. For loopless support its condition is equivalently b(S)<=b(N(S)) for every independent set S. Specializing to a clique or complete multipartite graph recovers Lemma A's feasibility conditions. Proposition 11 and Theorem 22 describe basic solutions via trees and odd cycles. |
| [P. L. Erdos, S. G. Hartke, L. van Iersel, I. Miklos (2017), Graph Realizations Constrained by Skeleton Graphs](https://www.combinatorics.org/ojs/index.php/eljc/article/download/v24i2p47/pdf/), Sec. 3.1; [author preprint](https://arxiv.org/pdf/1508.00542) | Especially close prior art: prescribed degrees and a fixed partition; Sec. 3.1 obtains minimum and maximum crossing-edge counts for two classes using weighted perfect matching, with parity restrictions on intermediate counts. Its realizations are simple graphs, not arbitrary rational-weight completions, and it does not establish these formulas for general partitions. |
| [Lucas de Oliveira Silva et al. (2025), Realizing Graphs with Cut Constraints](https://arxiv.org/abs/2502.09358) | Direct modern neighboring problem: simultaneous degrees and specified cut sizes for simple graphs. The abstract distinguishes polynomial cases with cut sets of size at most three from hardness with size-four sets, even for degree-one inputs. These additional integral cut constraints are not the present relaxation. |

The specialization of Behrend's feasibility theorem just noted is an inference
checked against its statement: independent sets of a complete multipartite
graph lie inside one block, and the whole-block inequality dominates all its
subsets because demands are nonnegative. This establishes a concrete bridge
to known theory, rather than treating the vocabulary as a loose analogy.
Behrend counts a loop once in its incident-edge constraint (notation section); this
review counts loop degree twice. The cited loopless specialization is unaffected.

**Attribution recommendation:** describe the result as an explicit
complete-support specialization of fractional degree-constrained realization,
with a self-contained constructive proof and an application to an
original-degree modularity certificate. This bounded search did not locate
the exact two general-partition formulas stated together; that is not evidence
of global novelty or priority. Any contribution claim should be localized to
the application, exposition, or separately demonstrated algorithmic result.
Further exhaustive novelty review is outside this bounded sidecar.

## 8. Independent Exact Checker

The checker below solves the finite LPs by enumerating their basic feasible
solutions, not by constructing solutions using the claimed formulas:

\[
\min/\max\ \sum_{i<j:\,P(i)=P(j)}x_{ij},\qquad
\sum_{j\ne i}x_{ij}=r_i,\quad x_{ij}=x_{ji}\ge0\ (i\ne j),\quad x_{ii}=0.
\]

For
n>=3, the unoriented incidence matrix of K_n has rank n: a vector annihilating
all columns has y_i+y_j=0 for each pair, and a triangle forces all entries to
zero. Enumerating every invertible n-column basis therefore captures every
vertex, including degenerate vertices with fewer than n positive edges.
Every nonempty feasible polytope is bounded (0<=x_ij<=min(r_i,r_j)), so its
linear extrema occur at these vertices. Cases n<=2 are handled directly.

All inversion uses `fractions.Fraction`. The code checks that each computed
basis inverse has half-integer entries before using doubled integral
coordinates; it does not assume this as a substitute for computing them.
No tolerances, floating-point optimizer, random sampling, repository imports,
or third-party packages are used. Rational test vectors are represented by
integer numerators and a common denominator. Forbidden-pair checks filter
vertices on the face x_forbidden=0, which preserves all vertices of that face.

Replay from the repository root, without creating another file:

```sh
awk '/^```python$/{copy=1;next} /^```$/{if(copy){exit}} copy' research_algorithms_20260920/Communities-Residual-Degree-Review.md | python3
```

```python
from fractions import Fraction as F
from itertools import combinations, product


def invert_square_matrix_exact(matrix):
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
                rows[i] = [x - scale * y for x, y in zip(rows[i], rows[col])]
    return [row[n:] for row in rows]


def prepare_incidence_bases_exact(n):
    edges = tuple(combinations(range(n), 2))
    bases = []
    if n >= 3:
        for chosen in combinations(range(len(edges)), n):
            matrix = [[int(i in edges[e]) for e in chosen] for i in range(n)]
            inverse = invert_square_matrix_exact(matrix)
            if inverse is not None:
                assert all((2*x).denominator == 1 for row in inverse for x in row)
                doubled = tuple(tuple(int(2*x) for x in row) for row in inverse)
                bases.append((chosen, doubled))
    return edges, bases


def enumerate_feasible_vertices_exact(r, edges, bases):
    n = len(r)
    if n == 1:
        return {()} if r == (0,) else set()
    if n == 2:
        return {(2*r[0],)} if r[0] == r[1] else set()
    vertices = set()
    for chosen, doubled in bases:
        values = tuple(sum(a*b for a, b in zip(row, r)) for row in doubled)
        if min(values) < 0:
            continue
        candidate = [0] * len(edges)
        for e, value in zip(chosen, values):
            candidate[e] = value
        assert all(sum(candidate[e] for e, pair in enumerate(edges) if i in pair)
                   == 2*r[i] for i in range(n))
        vertices.add(tuple(candidate))
    return vertices


def generate_set_partitions_exact(n, prefix=()):
    if len(prefix) == n:
        yield prefix
        return
    for label in range(max(prefix, default=-1) + 2):
        yield from generate_set_partitions_exact(n, prefix + (label,))


def calculate_proposed_bounds_exact(r, labels):
    blocks = [[r[i] for i in range(len(r)) if labels[i] == label]
              for label in range(max(labels) + 1)]
    sums = [sum(block) for block in blocks]
    excesses = [max(0, 2*max(block) - sum(block)) for block in blocks]
    mass = F(sum(r), 2)
    return (max(F(0), max(sums) - mass),
            mass - max(max(excesses), F(sum(excesses), 2)))


def compare_partition_extrema_exact(r, labels, edges, vertices, denominator=1):
    internal = [e for e, (i, j) in enumerate(edges) if labels[i] == labels[j]]
    scores = [F(sum(vertex[e] for e in internal), 2*denominator)
              for vertex in vertices]
    observed = (min(scores), max(scores))
    expected = tuple(x / denominator for x in calculate_proposed_bounds_exact(r, labels))
    assert observed == expected, (r, denominator, labels, observed, expected)


def enumerate_perfect_pairings_exact(vertices):
    if not vertices:
        yield ()
        return
    first = vertices[0]
    for other in vertices[1:]:
        remaining = tuple(v for v in vertices if v not in (first, other))
        for rest in enumerate_perfect_pairings_exact(remaining):
            yield ((first, other),) + rest


cache = {n: prepare_incidence_bases_exact(n) for n in range(1, 6)}
total_vectors = total_feasible = total_cases = 0
for n in range(1, 6):
    edges, bases = cache[n]
    partitions = tuple(generate_set_partitions_exact(n))
    feasible = cases = 0
    for r in product(range(3), repeat=n):
        total_vectors += 1
        vertices = enumerate_feasible_vertices_exact(r, edges, bases)
        assert bool(vertices) == (2*max(r) <= sum(r)), r
        if not vertices:
            continue
        feasible += 1
        for labels in partitions:
            compare_partition_extrema_exact(r, labels, edges, vertices)
            cases += 1
    total_feasible += feasible
    total_cases += cases
    print(f'n={n}: bases={len(bases)}, partitions={len(partitions)}, '
          f'feasible_vectors={feasible}, endpoint_pairs={cases}')

rational_cases = 0
for r, denominator in [((1, 1, 1), 3), ((1, 2, 3, 4, 4), 6),
                       ((9, 1, 4, 3, 3), 7)]:
    edges, bases = cache[len(r)]
    vertices = enumerate_feasible_vertices_exact(r, edges, bases)
    assert vertices
    for labels in generate_set_partitions_exact(len(r)):
        compare_partition_extrema_exact(r, labels, edges, vertices, denominator)
        rational_cases += 1

edges, bases = cache[4]
vertices = enumerate_feasible_vertices_exact((1, 1, 1, 1), edges, bases)
labels = (0, 0, 1, 1)
internal = [e for e, (i, j) in enumerate(edges) if labels[i] == labels[j]]
for allowed_internal, expected in [(False, (F(0), F(0))), (True, (F(2), F(2)))]:
    allowed = {e for e in range(len(edges)) if (e in internal) == allowed_internal}
    face = [v for v in vertices if all(v[e] == 0 for e in range(len(edges))
                                     if e not in allowed)]
    scores = [F(sum(v[e] for e in internal), 2) for v in face]
    assert (min(scores), max(scores)) == expected

pairings = tuple(enumerate_perfect_pairings_exact(tuple(range(6))))
integer_scores = [sum((i < 3) == (j < 3) for i, j in matching) for matching in pairings]
assert len(pairings) == 15 and max(integer_scores) == 2
assert calculate_proposed_bounds_exact((1,)*6, (0, 0, 0, 1, 1, 1))[1] == 3
print(f'degree_vectors={total_vectors}, feasible_vectors={total_feasible}, '
      f'exhaustive_endpoint_pairs={total_cases}')
print(f'rational_endpoint_pairs={rational_cases}, '
      f'total_endpoint_comparisons={2*(total_cases+rational_cases)}')
print('restricted_support_cases=2, integer_pairings=15, integer_max=2, rational_max=3')
print('PASS: all exact comparisons agree; integer/support gaps confirmed')
```

### Executed Results

Executed the replay command above with Python 3.9.6, standard library only,
on 2026-09-21. Exit status: 0. Exact stdout:

```text
n=1: bases=0, partitions=1, feasible_vectors=1, endpoint_pairs=1
n=2: bases=0, partitions=2, feasible_vectors=3, endpoint_pairs=6
n=3: bases=1, partitions=5, feasible_vectors=15, endpoint_pairs=75
n=4: bases=12, partitions=15, feasible_vectors=61, endpoint_pairs=915
n=5: bases=162, partitions=52, feasible_vectors=213, endpoint_pairs=11076
degree_vectors=363, feasible_vectors=293, exhaustive_endpoint_pairs=12073
rational_endpoint_pairs=109, total_endpoint_comparisons=24364
restricted_support_cases=2, integer_pairings=15, integer_max=2, rational_max=3
PASS: all exact comparisons agree; integer/support gaps confirmed
```

Coverage: all 363 labeled vectors in {0,1,2}^n for 1<=n<=5, including a
feasibility comparison for each; all set partitions for the 293 feasible
vectors; and all partitions for the three additional rational vectors
listed in the code. This is 12,182 degree/partition pairs and 24,364 endpoint
comparisons, plus two support-restricted cases and all 15 integer perfect
matchings on six vertices. The n<=2 cases are direct, not basis LPs.
There are no statistical confidence claims or performance measurements.

## 9. Final Synthesis and Remaining Limits

The mathematical hypothesis is established under its stated loopless,
complete-support, arbitrary-rational-weight assumptions. The modularity
envelope follows with original d and W and is deterministic under adaptive
partition selection. Tightness must be advertised only for the relaxed
completion class, not for forbidden-pair or integer completions. Existing
fractional b-matching and fixed-partition degree-realization literature must
be acknowledged. Global novelty remains unestablished by this bounded search.
