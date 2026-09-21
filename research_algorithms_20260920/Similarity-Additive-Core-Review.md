# Similarity Additive-Core Independent Review

Date: 2026-09-21. Mathematical review only; this file is the sole owned artifact.

## Verdict And Scope

**Accept the additive-region contraction as an exact value/domain reduction, with the boundary conditions stated below.** It supports `O(G + (r+1)*2^r)` arithmetic work for the exact attained-maxima two-row Jaccard envelope. The proof preserves every feasible retained count tuple and the maximum selected-row overlap for each such tuple. It does **not** preserve all suboptimal overlap values, jointly optimal overlaps of both rows, or an expanded witness-output bound.

The inputs reviewed were [Prior-Art Assessment, Sections 6.1/6.2](Similarity-Orientation-Prior-Art-Assessment.md), [Parameter Recovery](Similarity-Orientation-Parameter-Recovery.md), and its [independent mathematical review](Similarity-Orientation-Parameter-Review.md). No lead implementation, probe, or test file was inspected, imported, or run. No production GDS or PageRank material was examined or changed. This is neither a code audit nor a priority or practical-speed claim.

The review uses three checks: exact projection of count constraints, constructive lifting of the optimal overlap, and explicit tree-size accounting. A bounded independent enumeration is retained below. The proof, not the finite check, establishes the general result.

## 1. Contract And Canonical Labels

Let `T` be a nonempty full rooted binary tree with `G>=1` leaves. Leaf `j` represents a disjoint part of the exact target union, with population `H_j`, attained maximum row count `C_j`, and query population `q_j`. At every node `v`, `C_v` is the attained maximum of **exactly two** row counts. Root row sizes are `(u,ell)`, with `u=C_root` and `ell<=u`. The whole query size is `a>=sum q_j`, including query elements outside the union.

All counts are nonnegative integers. Validate the tree and arithmetic metadata before contracting: additive union populations; `0<=q_j<=H_j`; `C_v<=H_v<=2*C_v`; `C_parent>=max(C_children)`; `D_v=C_left+C_right-C_v>=0`; and `H_root<=u+ell`, among the root/query bounds above. Such necessary checks are not sufficient for global realizability. Empty feasible domains must still be rejected during enumeration.

Write `S={v:D_v>0}` and `r=|S|`. Fix the root's designated major row to row 0. At an additive node, propagate its label to both children. At a strict node, choose either ordered pair of opposite child labels, independently of the parent's label. There are at most `2^r` choices. A label designates an attaining row, not a strictly larger row.

These rules cover every compatible pair: at an additive split, a row attaining the sum of the two child maxima must attain each; at a strict split, a shared child-major row would exceed the parent maximum. A tied child at a strict split is likewise impossible in a realizing pair. Ties at additive nodes or at the root do not require extra bits. Infeasible assignments are harmless only when rejected.

For either row `i`, fix one labeling `sigma`. Its original count constraints are:

- Major leaf: `x_j=C_j`. Minor leaf: `H_j-C_j<=x_j<=C_j`.
- Major internal node: `x_v=C_v`. Minor internal node: `x_v<=C_v`.
- Child counts sum to their parent's count; the root also has its prescribed row size.

Both row problems, including their prescribed root sizes, must be feasible before using either optimum.

## 2. Boundary-Preserving Contraction

Delete the strict vertices and their incident edges. Each remaining connected component `K` consists of additive internal nodes and ordinary leaves, possibly just one leaf. Let `t_K` be its highest vertex, `J_K` its ordinary leaves, and `B_K` the strict vertices reached on downward edges leaving it. These strict vertices are the roots of disjoint boundary subtrees, not vertices absorbed into `K`.

Repeated additivity gives

```text
C_tK = sum_{s in B_K} C_s + sum_{j in J_K} C_j.
```

More importantly, this identity holds at **every** deleted additive vertex `v`, using just the boundary strict roots and ordinary leaves below `v`. All vertices of `K`, and all its immediate boundary strict roots, inherit the same designated label. Keep each strict vertex, its original quota, and its orientation bit. Keep direct strict-to-strict edges; they are not additive paths.

For one row, retain boundary occupancies `z_s` and the bundle total `x=sum_{j in J_K} x_j`. The following is an exact projection statement, before any ancestor or prescribed-root constraint is imposed.

### Major Row Of The Component

The boundary strict subproblems require `z_s=C_s`; each ordinary leaf requires `x_j=C_j`. Thus `x=U_K=sum C_j`. Every removed major equality follows from the identity above at its original vertex. Conversely, these equalities were necessary in the original problem. No additional boundary-count restriction is lost.

The requirement at each strict boundary must remain attached to that boundary's own feasible domain. Do not overwrite an infeasible strict subtree with the singleton `C_s` merely because its component is major.

### Minor Row Of The Component

Each boundary subproblem supplies `z_s<=C_s`. Each ordinary leaf supplies an interval `[l_j,h_j]=[H_j-C_j,C_j]`. The possible bundle totals are precisely

```text
L_K=sum l_j <= x <= U_K=sum h_j, over integers.
```

For any such total, an allocation to the original leaf intervals exists: start at all lower bounds and distribute the remaining `x-L_K` units without exceeding upper bounds. For **any** allocation in these intervals, and any feasible boundary subtree assignments, every removed minor quota holds: its count is a sum over a subset of those boundary/leaf terms, each at most the corresponding capacity, whose capacities sum to that vertex's `C_v`.

Consequently there are no hidden intermediate restrictions on the projected tuple `( (z_s)_{s in B_K}, x )`. Its feasible set is exactly the Cartesian product of the boundary subtree size domains and `[L_K,U_K]`. The component-top count is the sum of the tuple. A constraint from above or a prescribed root size restricts this sum in the usual way.

### Why Nesting Does Not Break The Argument

Apply the projection statement bottom-up. Each strict boundary subtree retains its complete already-contracted feasible domain; replacing its interior does not weaken its root constraint. At a strict node, merge its two child domains and **intersect** with `[C_v,C_v]` for its major row or `[0,C_v]` for its minor row. Reject an empty intersection.

Thus adjacent strict nodes retain their direct edge and both constraints. Nested strict nodes separated by additive paths retain the intervening component and its boundaries. A component can have many strict children, but they represent disjoint original subtrees. There is no copying of a subtree or conflation of nested boundaries.

By induction, feasible assignments on the core are exactly projections of original feasible count assignments. This includes infeasible subtrees, both row roles, and all retained boundary counts, not merely the eventual root optimum.

## 3. Optimal-Overlap Lifting

For a component's ordinary leaves define

```text
L = sum_j (H_j-C_j)       U = sum_j C_j
b = sum_j min(q_j,H_j-C_j)
B = sum_j min(q_j,C_j)    A = L-b.
```

At a minor leaf, start at `l_j=H_j-C_j`. The next

```text
g_j = min(q_j,C_j)-min(q_j,l_j)
```

increments each increase the best overlap by one; the remaining increments increase it by zero. All `g_j` are nonnegative. Given total `x`, allocate the first `min(x-L,sum g_j)` increments among the gain-one prefixes, then any remaining increments among gain-zero suffixes. No zero increment is needed to unlock a gain-one increment. There is sufficient capacity because `x<=U`.

This constructs an allocation achieving, for every feasible integer `x`,

```text
F_K(x) = b + min(x-L,B-b) = min(x-A,B).
```

No allocation can do better: its gain is at most the number `x-L` of added members and at most the sum `B-b` of available gain-one increments. Hence the formula is an attained maximum, not just an upper bound. For a component-major row, restrict the **same** record to `[U,U]`; since `B-b<=U-L`, its value at `U` is exactly `B`.

For a fixed boundary tuple, the original subtree optimum is

```text
F_K(x) + sum_{s in B_K} F_s(z_s).
```

Its terms concern disjoint feature leaves. Lift each strict-boundary optimum inductively and use the constructed ordinary-leaf allocation. Section 2 guarantees all removed constraints. This proves optimal lifting for every feasible boundary tuple, a stronger statement than preserving one best score.

When only the total size is fixed, merge any number of child records `(L_i,U_i,A_i,B_i)` by

```text
L0   = sum L_i                  U0 = sum U_i
base = sum F_i(L_i)             top = sum F_i(U_i)
A0   = L0-base                  B0 = top.
```

The same gain-one-first allocation proves the merged frontier `min(x-A0,B0)` on `[L0,U0]`. At each retained node intersect that domain with its quota; keep the curve on the intersection. At the root additionally intersect with its prescribed size. This handles high-degree components in work linear in their number of children.

### Completing The Companion

After lifting the scored row, lift **any** feasible count assignment of the other row, not necessarily its optimal feature set. At every original leaf one row has size `C_j`; the other lies in `[H_j-C_j,C_j]`. Therefore their sizes `x_j,y_j` satisfy `x_j+y_j>=H_j` and neither exceeds `H_j`.

Choose the scored row's actual features query-first to realize `min(q_j,x_j)`. Complete the other row with all `H_j-x_j` complementary features and any `x_j+y_j-H_j` features of the scored row. The latter number lies between zero and `x_j`. This preserves the exact leaf union and both count assignments, hence every attained maximum and both root sizes.

The two marginal optima need not coexist as feature sets. With one leaf of population three, row sizes two, and query population two, each row can individually have overlap two; both doing so would omit the third union feature. The envelope maximizes over either target and compatible pairs, so separate witnesses suffice.

For fixed target size `s`, Jaccard `c/(a+s-c)` is increasing in overlap `c` wherever the denominator is positive. Use score zero when `c=0`, including empty/empty. Maximize the two marginal scores only on feasible paired branches, then maximize over orientations. Projection, lifting, and canonical-label completeness establish exact equality with the original envelope.

## 4. Adversarial Edge Cases

**Positive `A` is necessary.** Ordinary leaf triples `(H,C,q)=(3,2,0),(3,2,3)` give `(L,U,A,B)=(2,4,1,2)`. The minor frontier at sizes `2,3,4` is `1,2,2`; the major endpoint is `2`. An aggregate physical leaf `(H,C,q)=(6,4,3)` would return `2,3,3`. The virtual leaf is a value/domain record, not a fictitious physical leaf with aggregated query population.

**Empty cases have three meanings.** A component containing no ordinary leaves uses `(0,0,0,0)` as the identity bundle; it can still have strict children. A component with ordinary leaves but zero population also contributes zero. Deleting adjacent strict nodes creates no intervening component at all; preserve their edge without inventing a zero-capacity intermediate node. An entirely empty union has no strict node and returns zero if root/query data are valid. A single original leaf is an ordinary component, even when `r=0`.

**Root ties are ordinary domain checks.** Fixing `sigma_root=0` remains complete when `ell=u`. Row 1 must also reach that same root count. At a component root this can force all its boundaries to their caps even though its labels call row 1 minor; intersecting the root domain enforces this. At a strict root the children remain oppositely labeled. No tie bit is added and no row is omitted.

**A feasible scored row is insufficient.** Take two leaves with `H=C=(1,2)`, root `u=2,ell=1`, and `q=(1,1),a=2`. The root is strict. The branch labeling the first leaf row 0 and the second row 1 permits row-0 counts `(1,1)` and overlap two, but row 1 needs at least two members and cannot have prescribed size one. Keeping that branch would incorrectly return `1`. The valid opposite branch has counts `(0,2)` and `(1,0)` and envelope `max(1/3,1/2)=1/2`. This uses the smallest possible number of leaves for a strict split.

**Necessary local validation is insufficient.** Two leaves with `H=C=(2,2)`, root maximum three and smaller-row size one pass the listed local numerical checks. Every orientation forces each row to contain a full size-two leaf, so no companion of size one exists. The evaluator must report impossible metadata, not an exact score of zero.

**The whole suboptimal spectrum is not retained.** A minor bundle consisting of `(H,C,q)=(3,2,2)` and a bundle consisting of `(1,1,1),(2,1,2)` both have `(L,U,A,B)=(1,2,0,2)`. At total size one the first can have overlap zero or one; the second must have overlap one. At total size two the first can have overlap one or two; the second must have overlap two. Their optimal frontiers agree but their attainable nonoptimal overlaps differ. No claim about preserving every overlap value, joint two-row reward, or additional within-leaf constraints follows from this reduction.

## 5. Exact Core-Size Constants

Let `k` be the number of original edges with both endpoints strict, and let `t` be one if the original root is strict, zero otherwise. The number `c` of nonempty graph components after removing the strict vertices is exactly

```text
c = 1-r + sum_{v in S} deg_T(v) - k
  = 2r+1-t-k.
```

This counts components as remaining vertices minus remaining edges; strict vertices have undirected degree three except a strict root, which has degree two. It holds also for `r=0`. Ordinary leaves are never removed, so there is always at least one component.

The structural quotient, before adding explicit virtual leaves, is a tree with

```text
N_struct = r+c = 3r+1-t-k        E_struct = N_struct-1 = 3r-t-k.
```

Strict nodes still have exactly two children. The total number of component-to-strict downward edges is `r-t-k`. These facts expose the high-degree cost: summing component child counts is `O(r)`, not a hidden `O(r^2)` scan.

With **one explicit virtual leaf per component**, including zero bundles, the exact counts are

```text
N_core = r+2c = 5r+2-2t-2k <= 5r+2
E_core = N_core-1         <= 5r+1
N_core+E_core             <= 10r+3.
```

These are vertex and edge counts, not exact arithmetic-instruction counts. If only components containing ordinary leaves get explicit virtual vertices, replace the second `c` by their number `b<=c`; empty bundles are implicit identities. If every bundle is a payload on its component, there are just `N_struct` vertices. These are alternate representations, not conflicting bounds. A component that is itself an original leaf may be fused with its virtual leaf, but that optimization is unnecessary for the stated constants.

The uniform constants `2r+1`, `3r+1`, and `5r+2` are attained in their respective representations. Start with an exclusive row-0 leaf of population two (`T_0`). Recursively let `T_j=(p_j,S_j)` and `S_j=(n_j,T_{j-1})`, where `p_j` is an exclusive row-0 singleton and `n_j` an exclusive row-1 singleton. Each `T_j` has row-count difference two and is additive; each `S_j` has difference one and strict deficit one. Thus `G=2r+1`, the root is additive, no strict nodes are adjacent, and each of the `2r+1` components contains an ordinary leaf. This is valid attained-maxima metadata for every `r>=0`.

Preprocessing can validate the input, identify components, and accumulate each bundle's four scalars in one traversal, using each original node/edge a constant number of times: `O(G)` arithmetic work. It must not repeatedly expand the original component for every orientation. Each orientation labels the quotient and performs two row passes, touching `O(r+1)` vertices and edges. A length-`r` bit vector suffices for enumeration; no list of all assignments and no single-word orientation-mask assumption is needed.

Hence the sharpened schedule is

```text
arithmetic work:           O(G + (r+1)*2^r)
preprocessing allowance:  O(G) integer words
post-preprocessing core:   O(r+1) words, excluding input and witnesses.
```

For fixed `r`, arbitrarily large additive refinements no longer multiply the exponential term. This is an arithmetic scheduling improvement for supplied counts, not avoidance of input histogram construction. Bundle query statistics must still be formed from the supplied leaf `q_j` values for the query being evaluated.

### Bit Cost And Output Limits

For validated input, take `b=ceil(log2(2+a+H_root+G))`, covering count magnitudes and node indices. Scalar sums, minima, and comparisons use `O(b)` bit arithmetic. Compare unreduced exact candidate fractions by cross-products, with multiplication cost `M(b)`. An explicit arithmetic bit-cost bound is

```text
O((G + (r+1)*2^r)*b + 2^r*M(b)),
```

with `O(G*b)` preprocessing payload bits and `O((r+1)*b)` core payload bits. Optional final fraction reduction adds its own gcd cost. This is not constant-bit arithmetic, constant physical RAM, or a polynomial kernel in `r` encoded bits: retained capacities and scores can require arbitrarily many bits as numeric inputs grow.

The construction proves witness **existence**. Outputting all original leaf counts requires `Omega(G)` entries; outputting two explicit target sets requires `Omega(u+ell)` feature entries, and feature identities must be available from somewhere beyond count-only metadata. Expanding a chosen optimum may revisit original leaves. Neither an `O(r)` witness-output bound nor witness recovery from the bare four scalars alone is promised.

## 6. Section 6.2 Parameter Inequality

The proposed corollary is correct for realizable metadata:

```text
Delta = sum_internal D_v = sum_leaf C_j-u
r <= Delta <= H_root-u = ell-I,
where I=u+ell-H_root.
```

The first equality telescopes. Every strict integer deficit contributes at least one. The upper bound uses `C_j<=H_j`. For any realizing pair with leaf-count differences `d_j=x_j-y_j`, `2C_j=H_j+|d_j|` is **not** generally true, because `H_j` is a union count rather than the sum of row counts. The correct identity is

```text
2C_j = x_j+y_j+|d_j|,
sum_j(x_j+y_j) = u+ell,
Delta = (sum_j |d_j| - (u-ell))/2.
```

Thus the displayed difference formula in the assessment is still correct, but its proof must use row-count sums, not union populations. The quantity `ell-I` is the smaller row's exclusive population. It does not bound intersection size. Zero strict nodes does not imply actual set containment: two different size-two subsets of one three-element leaf give `r=0`.

This inequality offers a sufficient near-containment parameter bound. It is not a realizability test, a converse characterization, or an exponential lower bound.

## 7. Retained Independent Check

The following standard-library Python 3.11 code is self-contained. It reads no project modules or data. It tests fixed tiny metadata families, every orientation of each family, both rows, and zero/full/asymmetric queries. It compares the contracted scalar frontier against direct enumeration of original leaf-count vectors at every retained subtree. At every component it also compares **every boundary tuple's** optimum and domain. Finally, direct feature-set pair enumeration checks root envelopes with zero or two query features outside the union.

The families include a single leaf, an empty union, `A>0`, a component with no ordinary leaves, adjacent strict nodes, separated nested strict nodes, sharp core-size constants, an infeasible companion branch, and globally impossible locally admissible metadata. The example sizes are at most six union features. The code is a bounded corroboration, not an exhaustive enumeration of all trees or a production audit.

```python
from collections import Counter
from fractions import Fraction
from itertools import product

checks = Counter()


def list_tree_nodes_preorder(t):
    return [t] if isinstance(t, int) else [t] + sum(
        (list_tree_nodes_preorder(ch) for ch in t), [])


def list_tree_leaves_ordered(t):
    return [v for v in list_tree_nodes_preorder(t) if isinstance(v, int)]


def evaluate_scalar_curve_value(rec, x):
    return min(x - rec[2], rec[3])


def clip_scalar_curve_domain(rec, lo, hi):
    if rec is None:
        return None
    l, u, a, b = rec
    return (max(l, lo), min(u, hi), a, b) if max(l, lo) <= min(u, hi) else None


def merge_scalar_curve_records(records):
    if any(rec is None for rec in records):
        return None
    l = sum(rec[0] for rec in records)
    u = sum(rec[1] for rec in records)
    base = sum(evaluate_scalar_curve_value(rec, rec[0]) for rec in records)
    top = sum(evaluate_scalar_curve_value(rec, rec[1]) for rec in records)
    return l, u, l - base, top


def build_additive_core_tree(t, strict):
    if t in strict:
        return t, None, [build_additive_core_tree(ch, strict) for ch in t]
    leaves, children = [], []

    def collect_component_boundary_nodes(v):
        if v in strict:
            children.append(build_additive_core_tree(v, strict))
        elif isinstance(v, int):
            leaves.append(v)
        else:
            for ch in v:
                collect_component_boundary_nodes(ch)

    collect_component_boundary_nodes(t)
    return t, leaves, children


def collect_feasible_count_vectors(t, h, cap, labels, row):
    leaves = list_tree_leaves_ordered(t)
    ranges = [[cap[j]] if labels[j] == row else range(h[j] - cap[j], cap[j] + 1)
              for j in leaves]
    vectors = []
    for values in product(*ranges):
        counts = dict(zip(leaves, values))
        if all((total := sum(counts[j] for j in list_tree_leaves_ordered(v))) <= cap[v]
               and (labels[v] != row or total == cap[v])
               for v in list_tree_nodes_preorder(t)):
            vectors.append(counts)
    return vectors


def solve_core_row_frontier(node, h, cap, q, labels, row):
    t, leaves, children = node
    records = [solve_core_row_frontier(ch, h, cap, q, labels, row) for ch in children]
    vectors = collect_feasible_count_vectors(t, h, cap, labels, row)
    if leaves is not None:
        l = sum(h[j] - cap[j] for j in leaves)
        u = sum(cap[j] for j in leaves)
        base = sum(min(q[j], h[j] - cap[j]) for j in leaves)
        top = sum(min(q[j], cap[j]) for j in leaves)
        bundle = (u if labels[t] == row else l, u, l - base, top)
        original = {}
        for vec in vectors:
            key = tuple(sum(vec[j] for j in list_tree_leaves_ordered(ch[0]))
                        for ch in children) + (sum(vec[j] for j in leaves),)
            reward = sum(min(q[j], x) for j, x in vec.items())
            original[key] = max(original.get(key, -1), reward)
        records.append(bundle)
        contracted = {}
        if all(rec is not None for rec in records):
            for key in product(*(range(rec[0], rec[1] + 1) for rec in records)):
                contracted[key] = sum(evaluate_scalar_curve_value(rec, x)
                                      for rec, x in zip(records, key))
        assert original == contracted, ("boundary", t, row, original, contracted)
        checks["boundary_maps"] += 1
        checks["boundary_tuples"] += len(original)
    rec = clip_scalar_curve_domain(merge_scalar_curve_records(records),
                                   cap[t] if labels[t] == row else 0, cap[t])
    original = {}
    for vec in vectors:
        size = sum(vec.values())
        reward = sum(min(q[j], x) for j, x in vec.items())
        original[size] = max(original.get(size, -1), reward)
    contracted = {} if rec is None else {
        x: evaluate_scalar_curve_value(rec, x) for x in range(rec[0], rec[1] + 1)}
    assert original == contracted, ("frontier", t, row, original, contracted)
    checks["subtree_frontiers"] += 1
    return rec


def label_tree_orientation_nodes(t, strict, bits):
    choices, labels = dict(zip(strict, bits)), {}

    def propagate_chosen_major_labels(v, label):
        labels[v] = label
        if not isinstance(v, int):
            propagate_chosen_major_labels(v[0], choices[v] if v in choices else label)
            propagate_chosen_major_labels(v[1], 1 - choices[v] if v in choices else label)

    propagate_chosen_major_labels(t, 0)
    return labels


def calculate_exact_jaccard_score(overlap, size, query):
    return Fraction(overlap, query + size - overlap) if overlap else Fraction(0)


def enumerate_actual_pair_envelope(t, h, cap, sizes, q, outside):
    offset, masks, query = 0, {}, 0
    for j, population in enumerate(h):
        masks[j] = ((1 << population) - 1) << offset
        query |= ((1 << q[j]) - 1) << offset
        offset += population
    nodes = list_tree_nodes_preorder(t)
    masks = {v: sum(masks[j] for j in list_tree_leaves_ordered(v)) for v in nodes}
    full = (1 << offset) - 1
    candidates = [[x for x in range(full + 1) if x.bit_count() == size] for size in sizes]
    best = None
    for x, y in product(*candidates):
        if x | y != full or any(max((x & masks[v]).bit_count(),
                                   (y & masks[v]).bit_count()) != cap[v] for v in nodes):
            continue
        score = max(calculate_exact_jaccard_score((z & query).bit_count(), size,
                                                  sum(q) + outside)
                    for z, size in zip((x, y), sizes))
        best = score if best is None else max(best, score)
    return best


def run_independent_family_check(name, t, h, x, y, query, overrides):
    nodes = list_tree_nodes_preorder(t)
    cap = {v: max(sum(x[j] for j in list_tree_leaves_ordered(v)),
                  sum(y[j] for j in list_tree_leaves_ordered(v))) for v in nodes}
    cap.update(overrides)
    sizes = sum(x), sum(y)
    strict = [v for v in nodes if not isinstance(v, int) and cap[v[0]] + cap[v[1]] > cap[v]]
    core = build_additive_core_tree(t, strict)
    packed = []

    def collect_packed_core_nodes(node):
        packed.append(node)
        for ch in node[2]:
            collect_packed_core_nodes(ch)

    collect_packed_core_nodes(core)
    r = len(strict)
    k = sum(ch in strict for v in strict for ch in v)
    c = sum(node[1] is not None for node in packed)
    assert c == 2 * r + 1 - (t in strict) - k
    assert len(packed) + c == 5 * r + 2 - 2 * (t in strict) - 2 * k
    if name == "sharp_size":
        assert len(packed) + c == 5 * r + 2
    for q in sorted({tuple(query), (0,) * len(h), tuple(h)}):
        frontiers = []
        for bits in product((0, 1), repeat=r):
            labels = label_tree_orientation_nodes(t, strict, bits)
            records = [solve_core_row_frontier(core, h, cap, q, labels, row) for row in (0, 1)]
            roots = [clip_scalar_curve_domain(rec, size, size) for rec, size in zip(records, sizes)]
            checks["orientations"] += 1
            checks["one_sided_root_feasible"] += (roots[0] is None) != (roots[1] is None)
            if all(rec is not None for rec in roots):
                frontiers.append([evaluate_scalar_curve_value(rec, size)
                                  for rec, size in zip(roots, sizes)])
        for outside in (0, 2):
            best = max((max(calculate_exact_jaccard_score(value, size, sum(q) + outside)
                            for value, size in zip(values, sizes)) for values in frontiers), default=None)
            expected = enumerate_actual_pair_envelope(t, h, cap, sizes, q, outside)
            assert best == expected, (name, q, outside, best, expected)
            checks["envelopes"] += 1


families = [
    ("empty", (0, 1), [0, 0], [0, 0], [0, 0], [0, 0], {}),
    ("root_tie", 0, [3], [2], [2], [2], {}),
    ("positive_A", (0, 1), [3, 3], [2, 2], [1, 1], [0, 3], {}),
    ("empty_bundle", ((0, 1), (2, 3)), [1] * 4, [1, 0, 1, 0], [0, 1, 0, 1], [1, 0, 0, 1], {}),
    ("adjacent", ((0, 1), (2, 3)), [2, 1, 2, 1], [2, 0, 0, 1], [0, 1, 2, 0], [1, 1, 0, 1], {}),
    ("nested", (((0, 1), 2), 3), [2, 1, 1, 2], [2, 0, 1, 0], [0, 1, 0, 2], [1, 1, 0, 1], {}),
    ("sharp_size", (0, (1, (2, (3, 4)))), [1, 1, 1, 1, 2], [1, 0, 1, 0, 2], [0, 1, 0, 1, 0], [1, 0, 1, 1, 1], {}),
    ("companion", (0, 1), [1, 2], [0, 2], [1, 0], [1, 1], {}),
    ("impossible", (0, 1), [2, 2], [2, 1], [0, 1], [2, 0], {1: 2}),
]
for family in families:
    run_independent_family_check(*family)
assert checks["one_sided_root_feasible"] > 0
print(dict(sorted(checks.items())))
```

Executed successfully with exit status zero using the code extracted directly from this memo:

```sh
sed -n '/^```python$/,/^```$/p' research_algorithms_20260920/Similarity-Additive-Core-Review.md | sed '1d;$d' | python3.11
```

```text
boundary_maps: 590
boundary_tuples: 1007
envelopes: 50
one_sided_root_feasible: 36
orientations: 79
subtree_frontiers: 902
```

These are aggregate assertion counts over nine fixed families and 25 query configurations; orientation and boundary counts include repeated structures under different queries/row roles. They are not counts of distinct input trees. The first attempt with the system `python3` stopped because that interpreter lacks `int.bit_count`; the successful command above uses Python 3.11. The checker deliberately enumerates small domains and is not an implementation of the claimed resource bound. No timing comparison or long suite was run.

## 8. Limited Conclusion

The contraction theorem follows from the boundary projection and optimal lifting proofs above. It strengthens the supplied-count arithmetic schedule without changing the envelope, exponential parameter, access index, or prior public workload results. A primary-source context check confirms that separable and laminar concavity are established machinery: [Murota (2016), equations 4.34/4.35, printed page 190](https://www.mechanism-design.org/arch/v001-1/p_05.pdf). The elementary gain-prefix proof here suffices without importing a general optimization theorem. This source check does not establish publication priority for the specific metadata reduction.

The remaining implementation obligation belongs to the lead: construct the quotient once, retain direct strict edges and all boundary/root constraints, intersect rather than overwrite domains, keep `A,B`, reject infeasible companions, and price large integers and witness expansion separately. This memo does not certify that any implementation does so.
