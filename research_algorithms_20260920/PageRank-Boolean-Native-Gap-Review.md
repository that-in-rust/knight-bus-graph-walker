# Independent Review: Native Boolean Tree Gap

Date: 2026-09-21. Terminal bounded mathematical/resource/prior-art review.
Scope: `PageRank-Boolean-Native-Tree-Gap.md` and its prerequisite
`PageRank-Boolean-Heterogeneous-Gap.md`. Only this new file is owned here.
No source-validator implementation, prior uniform actual-byte implementation,
old solver/control, timing, physical-memory measurement, code edit, or commit
is reviewed or supplied.

## Findings And Verdict

**GO on the theorem, under the stated hypotheses.** No mathematical defect was
found in the native transfer, Gram spectrum, or tree constants. This is not a
production-integration approval or a useful-error-bound claim.

1. **Novelty is not established.** Reachable primary text gives the auxiliary
   walk itself, not merely a related abstract. The tree bound is a direct
   specialization of ordinary canonical-path congestion. Combining these with
   reversible comparison does not establish a research contribution.
2. **The target/auxiliary distinction is essential.** They have different
   transitions and generally different stationary measures. Boolean overlap
   must count parallel-record adjacency once, not once per shared endpoint.
3. **Resource claims are conditional.** The stated schedule is plausible in
   arithmetic-operation/retained-state terms, not a physical RAM bound. Strict
   canonical order, replayable source identity, integer bit costs, and a
   suitably balanced union-find are necessary qualifications.
4. **The supplied finite checker is not independent evidence.** Its replay
   succeeded, as did the prerequisite replay. The independent checker below
   uses different trees, cut enumeration, matrix construction, and fixtures.

## Mathematical Audit

Let `g_H` denote the actual weighted normalized-Laplacian gap and let `b_H>0`
be the certified lower bound called `delta_H` in the note. This distinction
avoids treating a lower bound as an exact eigenvalue. All energies below sum
over unordered edges; stationary measures are unnormalized unless specified.

### Auxiliary Spectrum

Set `T_ca=B_ac/2` and `S_ac=B_ac*h_c/N_a`. These are stochastic kernels from
classes to groups and groups to classes. Direct multiplication gives

```text
R = T S,
K = S T = (I + D_N^-1 A_H)/2.
h_c R_ce = h_e R_ec,
pi_R(c) = h_c/n.
```

This supplies an alternate rectangular-kernel derivation of the Gram claim,
without square roots. After conjugation, `R` is `X^T X` and `K` is `X X^T`,
where `X=D_N^-1/2 B diag(sqrt(h))/sqrt(2)`. Their nonzero spectra agree;
zero eigenvalues are padded or removed according to dimensions and rank.
All eigenvalues of `R` are in `[0,1]`. Since H is connected, eigenvalue one
is simple. Its nonstationary eigenvalues are at most `1-b_H/2`.

If H is bipartite, its transition eigenvalue `-1` becomes zero in K, not an
extra stationary mode. For connected H the unsigned incidence rank is `F-1`
in the bipartite case and `F` otherwise. In particular a tree has `P=F-1`:
K has one more zero than R. Do not blindly append `P-F` zero eigenvalues.
The note's phrasing in terms of nonzero spectra handles this correctly.

### Transfer To The Boolean Operator

Each record has exactly two *distinct* endpoints. Aggregating identical pairs
gives one positive integer `h_c` per class. The actual simple Boolean graph has

```text
d_c = (h_c-1) + sum_{e != c, e intersects c} h_e
    = N_a + N_b - h_c - 1.
pi_G(record in c) = d_c / sum_e h_e d_e.
```

Class-constant functions and within-class zero-sum functions are orthogonal
in the degree-weighted inner product and invariant under the actual walk.
For the latter, the adjacency eigenvalue is `-1`, hence the Laplacian
eigenvalue is `1+1/d_c`. These modes exist only when `h_c>1`.

For distinct intersecting classes the one shared group a yields conductances
`h_c*h_e` in G and `h_c*h_e/(2*N_a)` in R. Therefore

```text
E_G(y) >= 2*N_min*E_R(y)
       >= N_min*b_H*Var_h(y),
Var_hd(y) <= d_max*Var_h(y).
```

For the second inequality choose the h-weighted minimizing center on the
right, then evaluate the left variance at that same center. It does not
assume the h-weighted and hd-weighted means agree. Combining the quotient
with the within-class spaces proves exactly

```text
gap(G) >= min(1, N_min*b_H/d_max).
```

This comparison is for the largest *algebraic* nonstationary transition
eigenvalue. It is not an absolute mixing-gap guarantee. For `0<=alpha<1`
the corresponding PageRank resolvent denominator is
`(1-alpha)+alpha*delta_G`; negative transition eigenvalues do not worsen it.
No prior actual-byte implementation has been re-reviewed here.

### Tree Moments And Constants

Write `V=sum N=2n`, distinct from `vol_G=sum h*d`. For an edge separating
subtree S (root v) from its complement (root p), direct crossing-pair summation
is

```text
J_e = sum_{a in S, b outside S} N_a*N_b*dist_T(a,b)
    = B_v*U_v + A_v*(D_p-U_v-A_v) + A_v*B_v.
```

The subtraction `U_v+A_v` removes the entire contribution of S to distances
from p. Also `D_v-D_p=V-2*A_v`. Both identities hold for any root and tree,
including leaves and highly unequal populations. No balance assumption enters.

The identity `Var_N(z)=sum_{a<b}N_a*N_b*(z_a-z_b)^2/V`, followed by ordinary
Cauchy-Schwarz on each tree path, gives `Var_N<=C*E_H` with
`C=max_e J_e/(V*h_e)`. Thus `b_H=1/C`. There is no missing factor of two.
Non-tree edges must still contribute to N and to the full group energy.
Tree distances here count edges. Resistance-length paths are a different
valid method with different formulas, not an interchangeable convention.

### Optional Stronger Bounds

For complete coverage the prerequisite states `F>=4`, but the same proof
extends to **F=3**: `BB^T=I+J`, and the class graph is K3 with combinatorial
Laplacian spectrum `(0,3,3)`. There is no incidence-null space in this case.
Thus `b_all=min(1,m^2*F/(M*d_max))` is valid for complete coverage at `F>=3`.
The variance comparison still works with unequal class degrees, and clipping
at 1 handles all within-class modes. The prerequisite's claim of matching the
uniform *exact* gap does not extend to F=3: uniform height h gives Boolean
K_(3h), whose actual gap is `3h/(3h-1)>1`, while this bound clips to 1.
Taking `max(b_tree,b_all)` is safe after checking
both contracts on the same source; adding them is not justified. Counting P
alone proves completeness only after validating unique in-range pairs.
For unit K4 the canonical star-tree certificate gives `1/5`, versus `1` from
the all-pair bound. This is a useful concrete reason to keep the optional max.

A further elementary comparison, not a novelty claim or implementation
request, replaces `N_min` by the minimum N over groups incident to at least
two *distinct pair classes*. Only those groups mediate off-diagonal class
conductance, so the same proof applies. Such a group exists for connected
`F>=3`. Class-incidence counts require O(F) counters, not expanded records.
On a unit k-edge star it gives `min(1,2k^2/((3k-2)*(k-1)))`, whereas the
stated bound is `2k/((3k-2)*(k-1))`; G is the clique K_k. For k=5 these are
`25/26` and `5/26`. Using the *exact* group gap 1 instead would clip the
refinement to 1. The stated bound is correct but can be weak even when tree
quality is not the main problem.

## Eligibility And Adversarial Cases

- Missing pairs are allowed for the native bound. For the unit five-group
  path, G is the four-vertex path: vector `(1,1/2,-1/2,-1)` has degree mean
  zero and Rayleigh quotient `1/2`. Applying the complete-pair formula with
  min/max over only present classes would claim 1 and is false.
- Nonuniform degrees: group path with heights `(1,2,1)` produces G with
  record degrees `(2,3,3,2)`. Auxiliary class masses are `(1/4,1/2,1/4)`,
  Boolean class masses are `(1/5,3/5,1/5)`. Replacing one walk by the other
  changes the operator; stationary weighting by n alone is wrong here.
- Bipartite extreme: the unit three-group path gives G=K2 with transition
  eigenvalue `-1` and actual algebraic gap 2, despite absolute gap zero.
  The certified clipped gap is 1. Both H and G can be bipartite; neither
  invalidates this Poincare statement.
- Parallel-record trap: heights `(2,1)` on a three-group path give Boolean
  K3. Two records in the repeated pair share two groups but have one edge.
  Using raw incidence inner products gives erroneous degrees `(3,3,2)`
  instead of `(2,2,2)`.
- Disconnected support is inadmissible for this global positive gap, even
  when all original degrees are positive. Two disjoint height-two classes
  give two disjoint K2 components. Unused groups also violate the contract;
  deleting them would be a separately specified input transformation.
- Empty/singleton memberships, repeated endpoints, negative/zero/noninteger
  heights, duplicate unaggregated classes, or invalid group IDs are outside
  the theorem. Connected `F>=3` with valid positive pairs already implies
  positive original degrees, but an explicit degree check is still useful
  at the source boundary. F=2 is outside scope even when its G is a clique.
- Large common multiplicity rescales both group energy and population, so
  `b_H` is unchanged. Highly skewed multiplicity can weaken both comparisons;
  it does not invalidate either proof. The checker separates explicit
  expansions from huge-integer compressed calculations.

## Resource Audit

The two P-class passes plus tree work need no resident P-vector if the source
is already a replayable strictly ordered stream. Validate monotonic unique
`(a,b)` keys with one preceding key. If the source is unsorted, uniqueness,
sorting/indexing, and replay have additional costs; O(F) state does not
magically provide them. Stream identity and consistency are assumptions to
enforce, not properties proved by a mathematical gap formula.

The familiar `O(P*alpha(F))` union-find bound presumes rank/size balancing
with path compression, or an equivalently justified implementation. The lead
oracle's arbitrary `roots[ra]=rb` assignment is not itself a proof of that
complexity. Its dense storage is also not evidence for the streamed schedule.
The concurrently developed production validator is outside this review.

With positive heights, `A<=V`, `U,D<=(F-1)V`, and
`J<=(F-1)*A*(V-A)<=(F-1)*V^2/4`. Hence moment integers need
`O(log F+log n)` bits (including the constant-factor doubling for n-squared).
Tree weights, rational numerator/denominator comparisons, original volume,
and counter growth must be charged accordingly. O(F) arithmetic operations
is not O(F) fixed-size machine operations for arbitrary heights. Tree state
has `O(F*(log F+log n))` mathematical bits before object/container overhead.

This does not remove source construction, source/index retention, P scratch,
the stated 2n output reads and full output storage, or runtime/allocator
overhead. Releasing tree arrays before residual arrays is a schedule to
implement and verify, not an RSS result. No timing or physical measurement
was taken in this review.

## Reachable Primary Sources And Novelty Boundary

1. [Evans and Lambiotte, 2009, Section III.3, equation (14)](https://arxiv.org/html/0903.2181#S3.SS3):
   the inspected primary HTML gives `E=B^T D_k^-1 B`, includes self loops,
   and states strength two. Thus for unit heights `R=E/2`. Section V also
   discusses implicit line-graph operation and multiedges. The author-hosted
   published PDF timed out here; this assessment uses the reachable full-text
   HTML passages, not an assertion of inspected publisher PDF.
2. [Evans and Lambiotte, weighted-network follow-up, Sections 3 and 5](https://arxiv.org/html/0912.4389#S5):
   Section 5 explicitly lists self-loop adjacency
   `E_alpha,beta=sum_i tildeB_alpha,i B_i,beta/v_i` with `v_i=s_i` among its
   choices. Substituting target weight `w_alpha=h_alpha` and strength `s_i=N_i`
   gives outgoing column sum two. Therefore `E/2=R^T` in the note's row-walk
   convention. This is operator equality, derived from the inspected formula.
   By contrast Section 3 equation (12) excludes return to the same edge and
   uses `s_i-w_beta`; that particular operator is not R. Distinguishing these
   passages prevents a false attribution of equality to the wrong formula.
3. [Sinclair, 1992, Section 2, equation (4), Theorem 5 and proof](https://people.eecs.berkeley.edu/~sinclair/flow.pdf#page=8):
   inspected author-hosted PDF text, PDF pages 8-9 (printed pages 6-7), defines
   congestion with ordinary path length and proves the algebraic second-
   eigenvalue bound. Substitute `pi(a)=N_a/V`, `Q(e)=h_e/V`: its oriented-edge
   congestion equals `J_e/(V*h_e)` for tree paths. Each unordered crossing
   pair contributes once in a specified orientation, so no extra two appears.
   The same section credits Diaconis and Stroock and distinguishes this from
   inverse-capacity path lengths. Retain that standard attribution.

The spectral products, Poincare comparison, canonical paths, and tree distance
moments remain ordinary ingredients. What might warrant further evaluation is
a narrowly specified, resource-accounted certificate for the *original*
Boolean PageRank output on useful native sources. These passages neither
establish that contribution nor prove its absence. This is a bounded
three-work primary-source check, not an exhaustive search, priority clearance,
or assertion that this application formula appears verbatim in those works.

## Verification Record

Replayed with `/Users/amuldotexe/.local/bin/python3.11 -B`, exit 0:

```text
Exact native-gap/moment checks: 656 disconnected cases refused: 105
False complete-pair gap on sparse path: rejected
No production integration, physical resource or timing claim.
Exact whole-space heterogeneous gap checks: 83
Overclaimed uniform gap negative control: rejected
No production certificate, timing or physical memory claim.
```

These are **replays**, not independent runs in the methodological sense.

### Independent Runnable Checker

The checker below imports no project or lead-checker function. It enumerates
connected simple supports on three/four groups and all their spanning trees,
then adds deliberately nonuniform and huge-integer cases. Tree selection uses
connectivity searches, not the lead's union-find; congestion uses explicit
cross-cut pairs and independent all-vertex distances. It checks the moment
formula and reroot identity at every possible root. Rational symmetric
pivoting tests full Poincare matrices, not sampled eigenvectors.

The rectangular products are checked through exact trace powers (zero-padded
characteristic spectra) and exact PSD tests. Small cases construct the actual
Boolean graph by pairwise record overlap, then independently lump the
record-level endpoint experiment to classes. Huge cases check exact class
matrices plus the analytic within-class eigenvalue formula, **without** claiming
an explicit original-vertex expansion. This is a resident mathematical oracle,
not a source validator or bounded-memory implementation.

Run from repository root:

```sh
awk '/^```python$/{block=1;next} /^```$/{if(block)exit} block' research_algorithms_20260920/PageRank-Boolean-Native-Gap-Review.md | /Users/amuldotexe/.local/bin/python3.11 -B
```

```python
from collections import deque
from fractions import Fraction as Q
from itertools import combinations, product


def multiply_rational_matrix_pair(a, b):
    return [[sum(x*y for x, y in zip(row, col))
             for col in zip(*b)] for row in a]


def subtract_scaled_matrix_pair(a, b, scale):
    return [[x-scale*y for x, y in zip(ar, br)] for ar, br in zip(a, b)]


def construct_weighted_variance_matrix(w):
    return [[Q(w[i] if i == j else 0)-Q(w[i]*w[j], sum(w))
             for j in range(len(w))] for i in range(len(w))]


def construct_weighted_laplacian_matrix(a):
    return [[Q(sum(a[i]) if i == j else 0)-a[i][j]
             for j in range(len(a))] for i in range(len(a))]


def verify_symmetric_matrix_nonnegative(a):
    a = [[Q(x) for x in row] for row in a]
    assert a == list(map(list, zip(*a)))
    while a:
        diagonal = [a[i][i] for i in range(len(a))]
        if min(diagonal) < 0:
            return False
        pivot = max(range(len(a)), key=lambda i: diagonal[i])
        if diagonal[pivot] == 0:
            return not any(any(row) for row in a)
        rest = [i for i in range(len(a)) if i != pivot]
        a = [[a[i][j]-a[i][pivot]*a[pivot][j]/diagonal[pivot]
              for j in rest] for i in rest]
    return True


def collect_graph_breadth_distances(f, edges, root):
    distances = {root: 0}
    queue = deque([root])
    while queue:
        v = queue.popleft()
        for a, b, h in edges:
            u = b if a == v else a if b == v else None
            if u is not None and u not in distances:
                distances[u] = distances[v]+1
                queue.append(u)
    return distances


def validate_canonical_pair_source(f, edges):
    if type(f) is not int or f < 3 or not edges:
        return False
    previous = (-1, -1)
    for a, b, h in edges:
        if not all(type(x) is int for x in (a, b, h)):
            return False
        if not (0 <= a < b < f and h > 0 and previous < (a, b)):
            return False
        previous = (a, b)
    return len(collect_graph_breadth_distances(f, edges, 0)) == f


def select_stream_connecting_tree(f, edges):
    tree = []
    for a, b, h in edges:
        if b not in collect_graph_breadth_distances(f, tree, a):
            tree.append((a, b, h))
    assert len(tree) == f-1
    return tree


def calculate_independent_cut_certificate(f, edges, tree, population):
    volume = sum(population)
    distances = [collect_graph_breadth_distances(f, tree, a) for a in range(f)]
    assert all(len(d) == f for d in distances)
    total = [sum(population[b]*distances[a][b] for b in range(f))
             for a in range(f)]
    congestion = []
    for index, (a, b, h) in enumerate(tree):
        remaining = tree[:index]+tree[index+1:]
        side_a = set(collect_graph_breadth_distances(f, remaining, a))
        side_b = set(range(f))-side_a
        crossing = sum(population[x]*population[y]*distances[x][y]
                       for x in side_a for y in side_b)
        for root in range(f):
            p, v, inside = (a, b, side_b) if root in side_a else (b, a, side_a)
            mass = sum(population[x] for x in inside)
            moment = sum(population[x]*distances[v][x] for x in inside)
            outside = volume-mass
            formula = outside*moment+mass*(total[p]-moment-mass)+mass*outside
            assert formula == crossing
            assert total[v] == total[p]+volume-2*mass
        congestion.append(Q(crossing, volume*h))
    bound = 1/max(congestion)
    adjacency = [[0]*f for _ in range(f)]
    for a, b, h in edges:
        adjacency[a][b] = adjacency[b][a] = h
    assert verify_symmetric_matrix_nonnegative(subtract_scaled_matrix_pair(
        construct_weighted_laplacian_matrix(adjacency),
        construct_weighted_variance_matrix(population), bound))
    return bound


def verify_native_fixture_independently(f, edges, all_trees=False):
    assert validate_canonical_pair_source(f, edges)
    pairs = [frozenset((a, b)) for a, b, h in edges]
    heights = [h for a, b, h in edges]
    p, n = len(edges), sum(heights)
    population = [sum(h for a, b, h in edges if v in (a, b)) for v in range(f)]
    class_count = [sum(v in pair for pair in pairs) for v in range(f)]
    degrees = [h-1+sum(heights[j] for j in range(p) if i != j and pair & pairs[j])
               for i, (pair, h) in enumerate(zip(pairs, heights))]
    assert degrees == [population[a]+population[b]-h-1 for a, b, h in edges]
    assert min(degrees) > 0
    transition_cg = [[Q(int(v in pair), 2) for v in range(f)] for pair in pairs]
    transition_gc = [[Q(h, population[v]) if v in pair else Q(0)
                      for pair, h in zip(pairs, heights)] for v in range(f)]
    r = multiply_rational_matrix_pair(transition_cg, transition_gc)
    k = multiply_rational_matrix_pair(transition_gc, transition_cg)
    assert all(sum(row) == 1 for row in r+k)
    group_adjacency = [[sum(h for a, b, h in edges if {a, b} == {i, j})
                        if i != j else 0 for j in range(f)] for i in range(f)]
    assert k == [[Q(int(i == j), 2)+Q(group_adjacency[i][j], 2*population[i])
                  for j in range(f)] for i in range(f)]
    rp, kp = r, k
    for exponent in range(1, max(f, p)+1):
        assert sum(rp[i][i] for i in range(p)) == sum(kp[i][i] for i in range(f))
        rp = multiply_rational_matrix_pair(rp, r)
        kp = multiply_rational_matrix_pair(kp, k)
    conductance = [[heights[i]*r[i][j] for j in range(p)] for i in range(p)]
    assert verify_symmetric_matrix_nonnegative(conductance)
    lr = construct_weighted_laplacian_matrix(conductance)
    assert verify_symmetric_matrix_nonnegative(lr)
    class_adjacency = [[heights[i]*heights[j] if i != j and pairs[i] & pairs[j]
                        else 0 for j in range(p)] for i in range(p)]
    lg = construct_weighted_laplacian_matrix(class_adjacency)
    wh = construct_weighted_variance_matrix(heights)
    wd = construct_weighted_variance_matrix([h*d for h, d in zip(heights, degrees)])
    active_min = min(population[v] for v in range(f) if class_count[v] >= 2)
    assert verify_symmetric_matrix_nonnegative(subtract_scaled_matrix_pair(
        lg, lr, 2*active_min))
    original_l = original_w = None
    if n <= 40:
        records = [pair for pair, h in zip(pairs, heights) for _ in range(h)]
        owner = [i for i, h in enumerate(heights) for _ in range(h)]
        adjacency = [[int(i != j and bool(x & y)) for j, y in enumerate(records)]
                     for i, x in enumerate(records)]
        direct_degree = list(map(sum, adjacency))
        assert direct_degree == [degrees[i] for i in owner]
        original_l = construct_weighted_laplacian_matrix(adjacency)
        original_w = construct_weighted_variance_matrix(direct_degree)
        # Explicit endpoint experiment on individual records, then class lumping.
        for i, record in enumerate(records):
            lumped = [Q(0)]*p
            for endpoint in record:
                targets = [j for j, other in enumerate(records) if endpoint in other]
                for j in targets:
                    lumped[owner[j]] += Q(1, 2*len(targets))
            assert lumped == r[owner[i]]
    if all_trees:
        trees = [list(t) for t in combinations(edges, f-1)
                 if len(collect_graph_breadth_distances(f, t, 0)) == f]
    else:
        trees = [select_stream_connecting_tree(f, edges)]
        reverse = select_stream_connecting_tree(f, list(reversed(edges)))
        if set(reverse) != set(trees[0]):
            trees.append(reverse)
    for tree in trees:
        dh = calculate_independent_cut_certificate(f, edges, tree, population)
        assert verify_symmetric_matrix_nonnegative(subtract_scaled_matrix_pair(lr, wh, dh/2))
        dg = min(Q(1), Q(min(population), max(degrees))*dh)
        active = min(Q(1), Q(active_min, max(degrees))*dh)
        assert 0 < dg <= active <= 1
        all_pair = Q(0)
        if f >= 3 and p == f*(f-1)//2:
            all_pair = min(Q(1), Q(min(heights)**2*f, max(heights)*max(degrees)))
        bound = max(active, all_pair)
        assert verify_symmetric_matrix_nonnegative(subtract_scaled_matrix_pair(lg, wd, bound))
        assert all(1+Q(1, d) >= bound for h, d in zip(heights, degrees) if h > 1)
        if original_l is not None:
            assert verify_symmetric_matrix_nonnegative(
                subtract_scaled_matrix_pair(original_l, original_w, bound))
    return len(trees), int(original_l is not None)


cases = []
for f in (3, 4):
    pairs = list(combinations(range(f), 2))
    for present in product((0, 1), repeat=len(pairs)):
        edges = [(a, b, 1) for (a, b), keep in zip(pairs, present) if keep]
        if validate_canonical_pair_source(f, edges):
            cases.append((f, edges, True))
special = [
    (3, [(0, 1, 2), (1, 2, 1)]),
    (3, [(0, 1, 1), (0, 2, 2), (1, 2, 5)]),
    (4, [(0, 1, 1), (1, 2, 2), (2, 3, 1)]),
    (5, [(i, i+1, 1) for i in range(4)]),
    (5, [(0, i, h) for i, h in enumerate((1, 2, 3, 5), 1)]),
    (4, [(0, 1, 1), (0, 3, 2), (1, 2, 7), (2, 3, 1)]),
    (5, [(0, 1, 3), (0, 2, 1), (1, 2, 2), (2, 3, 1), (3, 4, 7)]),
    (6, [(0, 1, 2), (0, 2, 1), (1, 2, 3), (2, 3, 1),
         (3, 4, 1), (3, 5, 2), (4, 5, 1)]),
    (4, [(a, b, h) for (a, b), h in zip(combinations(range(4), 2), (1, 2, 3, 1, 2, 1))]),
    (4, [(a, b, 31 if (a, b) == (0, 1) else 1) for a, b in combinations(range(4), 2)]),
    (3, [(0, 1, 7), (0, 2, 7), (1, 2, 7)]),
    (3, [(0, 1, 1), (0, 2, 1), (1, 2, 31)]),
    (3, [(0, 1, 1), (0, 2, 2), (1, 2, 10**60)]),
]
cases += [(f, edges, False) for f, edges in special]
for huge in (2**60, 10**60):
    cases += [(f, [(a, b, huge if i == 0 else h) for i, (a, b, h) in enumerate(edges)], False)
              for f, edges in (special[2], special[5], special[7], special[8])]
trees = expanded = 0
for f, edges, exhaustive in cases:
    count, explicit = verify_native_fixture_independently(f, edges, exhaustive)
    trees += count
    expanded += explicit

invalid = [
    (2, [(0, 1, 3)]), (3, []), (4, [(0, 1, 2), (2, 3, 2)]),
    (4, [(0, 1, 1), (1, 2, 1)]), (3, [(0, 0, 2), (0, 1, 1)]),
    (3, [(1, 0, 1), (1, 2, 1)]), (3, [(0, 1, 0), (1, 2, 1)]),
    (3, [(0, 1, -1), (1, 2, 1)]), (3, [(0, 1, Q(3, 2)), (1, 2, 1)]),
    (3, [(0, 1, True), (1, 2, 1)]), (3, [(0, 1, 1), (0, 1, 2), (1, 2, 1)]),
    (3, [(1, 2, 1), (0, 1, 1)]), (3, [(0, 1, 1), (1, 3, 1)]),
]
assert all(not validate_canonical_pair_source(f, e) for f, e in invalid)

# Exact negative witnesses and known boundary spectra.
path_degree = [1, 2, 2, 1]
witness = [Q(1), Q(1, 2), Q(-1, 2), Q(-1)]
energy = sum((witness[i]-witness[i+1])**2 for i in range(3))
variance = sum(d*x*x for d, x in zip(path_degree, witness))
assert sum(d*x for d, x in zip(path_degree, witness)) == 0
assert energy/variance == Q(1, 2) < 1
assert [Q(h, 4) for h in (1, 2, 1)] != [Q(h*d, 10) for h, d in zip((1, 2, 1), (2, 3, 2))]
assert verify_symmetric_matrix_nonnegative([[1, -1], [-1, 1]])
assert not verify_symmetric_matrix_nonnegative([[0, 1], [1, 0]])
assert not verify_symmetric_matrix_nonnegative([[1, 2], [2, 1]])
parallel = [{0, 1}, {0, 1}, {1, 2}]
assert [sum(len(a & b) for j, b in enumerate(parallel) if i != j)
        for i, a in enumerate(parallel)] == [3, 3, 2]
assert [sum(bool(a & b) for j, b in enumerate(parallel) if i != j)
        for i, a in enumerate(parallel)] == [2, 2, 2]
assert 1-Q(-1) == 2 and 1-abs(Q(-1)) == 0
complete = [(a, b, 1) for a, b in combinations(range(4), 2)]
canonical = select_stream_connecting_tree(4, complete)
complete_dh = calculate_independent_cut_certificate(4, complete, canonical, [3]*4)
assert complete_dh == Q(4, 15) and Q(3, 4)*complete_dh == Q(1, 5)
scaled = [(a, b, 10**60*h) for a, b, h in complete]
assert calculate_independent_cut_certificate(
    4, scaled, select_stream_connecting_tree(4, scaled), [3*10**60]*4) == complete_dh
for leaves in (2, 3, 5):
    star = [(0, i, 1) for i in range(1, leaves+1)]
    star_dh = calculate_independent_cut_certificate(
        leaves+1, star, star, [leaves]+[1]*leaves)
    assert star_dh == Q(2*leaves, 3*leaves-2)
    expected = Q(25, 26) if leaves == 5 else Q(1)
    assert min(Q(1), Q(leaves, leaves-1)*star_dh) == expected
print('Independent fixtures:', len(cases), 'expanded:', expanded, 'huge compressed:', len(cases)-expanded)
print('Tree certificates:', trees, '(moments checked at every root)')
print('Malformed/ineligible streams refused:', len(invalid))
print('Exact Gram/variance/Boolean-gap/optional-max checks: passed')
print('Missing-pair, wrong-measure, parallel-count and absolute-gap controls: passed')
print('K4 optional-max, star refinement and common-height scaling controls: passed')
print('F=3 complete-pair extension, including unequal/huge heights: passed')
print('No project imports, source-validator review, timing or physical-memory claim.')
```

Independent execution with Python 3.11 and bytecode writes disabled exited 0:

```text
Independent fixtures: 63 expanded: 54 huge compressed: 9
Tree certificates: 170 (moments checked at every root)
Malformed/ineligible streams refused: 13
Exact Gram/variance/Boolean-gap/optional-max checks: passed
Missing-pair, wrong-measure, parallel-count and absolute-gap controls: passed
K4 optional-max, star refinement and common-height scaling controls: passed
F=3 complete-pair extension, including unequal/huge heights: passed
No project imports, source-validator review, timing or physical-memory claim.
```

The 170 count covers fixture-tree combinations; the final analytic controls
add five cut-certificate calculations. These finite checks support, rather
than replace, the proof. No scientific novelty, practical usefulness, or
implementation readiness follows from their passing.

Review-draft correction: an added control initially asserted that the
leaf-excluding refinement always clips to 1 on stars. It failed for k=5.
The draft had conflated the exact group gap with its tree lower bound; the
correct tree-derived result is 25/26. The text and control above now preserve
that distinction. This was not a failure of the source note's native theorem.

## Terminal Disposition

Bounded review complete: native theorem and complete-pair F=3 extension GO,
retained-state schedule conditionally
credible, novelty unestablished, independent runnable checker supplied.
No further repository work is required by this review. The separate native
source validator and any later useful-source experiment remain outside scope.
