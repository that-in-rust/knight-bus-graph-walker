# Similarity Query-Induced Feasibility Review

Date: 2026-09-21. Independent, bounded mathematical challenge of
[the proposal](Similarity-Query-Induced-Feasibility.md). Only this review file
is owned by the reviewer. No production modules, project imports, repository
tests, public data, timings, or commits are part of this review.

## 1. Verdict And Qualifications

**Accept the neutral/offset substitution and query-local branching theorem,
subject to the explicit representation and input-cost conditions below.**
It preserves the maximum Jaccard score over all complete compatible pairs,
not just the score of a particular generating pair. No mathematical
counterexample to that claim was found in the bounded challenge.

The necessary qualifications are:

1. The interval and role-symmetry theorem is inherited from
   [the earlier review, Sections 3 and 9](Similarity-Overlap-Frontier-Review.md).
   Neither that theorem nor its zero-query recurrence is new here.
2. A neutral capsule is not a physical leaf. Actual population `H` and the
   floor contribution `E` must both survive. The offset formula is correct;
   setting inactive `E` to zero gives a concrete false envelope.
3. Scan-free query preparation needs cached boundary totals, paid parent
   links, sparse marking, and active adjacency. Caching zero-query answers
   while still traversing every child does not prove the stated bound.
4. `Kq <= min(K, s(h+1))` is the general bound. Replace `h` by `log2(G)` only
   for the specified power-of-two balanced compiler. A zero-support query
   still costs constant work and one root capsule; use a `+1` in a uniform
   work/space formula.
5. This does not beat a generic memoized tree DP equipped with the same
   feasibility summaries and additive aggregation. The remaining contribution
   is the specialized full-pair substitution/offset theorem and its explicit
   query-local parameter/accounting, not a new memoization primitive or a
   historical-priority result.

## 2. Boundary Feasibility Is Inherited

Write `kappa_v = 2*C_v-H_v`, reserving `K` for the static core record count.
For a feasible subtree the inherited intersection domain is the integer
interval `[E_v,kappa_v]`. In either row labeling its sizes are

```text
major = C_v
minor = H_v-C_v+e,       E_v <= e <= kappa_v.
```

This is a domain before imposing the root's particular intersection. Do not
cache domains already truncated to one query/root-size request. Label exchange
proves role symmetry even when subtree constraints force `E_v>0`. At the
upper endpoint both rows have size `C_v`; ties do not introduce another bit.

At an additive node, intersections add and hence so do their minimum values.
For a strict node put `D=C_L+C_R-C_v>0`, `X=kappa_L-D`, `Y=kappa_R-D`.
Both child fixed arguments must be feasible: `E_L<=X` and `E_R<=Y`.
One branch fixes the right intersection to `Y` and varies the left over
`[E_L,X]`; the other fixes the left to `X` and varies the right over
`[E_R,Y]`. The parent domains are

```text
[Y+E_L, X+Y] and [X+E_R, X+Y].
```

They share the upper endpoint, so their union is a contiguous interval with
minimum `min(Y+E_L,X+E_R)`. This derives exactly the proposed cached scalar
from the inherited theorem, with no new domain result. Leaf validity,
nonnegative deficits, exact populations, and infeasible-child propagation
are prerequisites, not consequences of an unchecked scalar computation.

Root admission also requires `0<=ell<=C_root` and
`E_root<=ell+C_root-H_root<=2*C_root-H_root`. Even a query with zero supported
features must reject an impossible companion before returning score zero.

## 3. Neutral And Offset Substitution

### Neutral Capsules

For a subtree containing no query features, the two scored-row rewards are
identically zero. Every feasible ordered boundary-size pair is therefore
represented exactly by `(C,H,E)` and either choice of the major label.
Every such boundary state lifts to an original complete subtree pair by the
inherited domain theorem. Conversely, projection of an original pair cannot
leave this domain. Hidden strict orientations may depend on the selected
boundary size; they need not be fixed globally or enumerated by the query.

The two minor-domain endpoints sum to `H+E`, not `H`. Inferring population
from those endpoints changes intersection arithmetic when `E>0`.

### Offset Bundles

Let the component's ordinary leaves have the inherited bundle `(L,U,A,B)`,
and let `b=L-A`. Their minor reward is `min(b+t,B)` at size `L+t`, where
`0<=t<=U-L` and `0<=B-b<=U-L`. Their major reward is `B`.

For inactive boundary subtrees write totals `C0,H0,E0`. Their minor sizes fill
the interval `[H0-C0+E0,C0]`: sums of integer intervals have no holes. All
their majors have the component-major label, because the component is
additive. Define

```text
Cb = U+C0                 Hb = L+U+H0
lb = Hb-Cb+E0             w0 = 2*C0-H0-E0 >= 0.
```

At aggregate minor size `x=lb+t`, distribute `t` increments between the
ordinary leaves and inactive region. An upper bound on reward is
`min(b+t,B)`. It is attainable: allocate `min(t,U-L)` increments to ordinary
leaves and the remaining `max(0,t-(U-L))` to the inactive region. The latter
quantity is at most `w0`, and `B-b<=U-L` ensures that this allocation attains
the upper bound. The existing ordinary-leaf allocation lemma lifts its
optimum to actual leaf subsets; the neutral lemma lifts the inactive sizes.
Thus, for every integer in the entire feasible interval,

```text
minor: lb <= x <= Cb,      f(x) = min(x-(lb-b), B)
major: x = Cb,            best reward B.
```

For any companion size in its feasible interval, a major-row optimum also
lifts: ordinary-leaf companion choices depend on their sizes, and the
inactive witnesses carry no query reward. This is important when a retained
strict parent fixes the companion's count.

Bundle intersection is `e=x-(Hb-Cb)`, not `x-lb`. For `0<=c<=B`, the exact
inverse thresholds are therefore

```text
tau_major[c] = E0
tau_minor[c] = E0+max(0,c-b).
```

Requests `c>B` are unattainable. In particular both zero thresholds are `E0`.
The final minor threshold fits the upper intersection bound because
`B-b<=U-L<=2*Cb-Hb-E0`. These are thresholds for at least `c`, not a claim
that every exact reward or both independently optimal rewards coexist.

### Full-Compatible-Pair Envelope

Fix one scored row and one admissible boundary tuple. Substitute the neutral
and offset witnesses above. Different children use disjoint feature sets,
so their witnesses unite without cross-child identity constraints. Deleted
additive interior nodes follow from the inherited additive-region lemma.
Every retained strict node still enforces the original attained-maximum
disjunction and its ordered child positions. Induction proves both projection
and optimal scored-row lifting, including existence of the companion.

At the root let `I=ell+C_root-H_root`. Maximizing overlap `z` is equivalent
to maximizing `z/(a+n-z)` for each fixed row size `n` (`C_root` or `ell`).
Take the maximum over the two roles, treating zero overlap as zero under the
existing empty-set convention. The two role optima may use different complete
pairs; `max_pair max_role` permits exactly that. This proves the requested
envelope, not reconstruction of the stored rows, a simultaneous reward-pair
frontier, or a joint sum-of-scores objective. Outside-union query elements
remain in `a` even though they activate no owner.

For the existing conditioned row optimizer, the primitive tuple can be
represented by its floor, ceiling, intercept `lb-b`, and reward ceiling `B`.
The separate true `Hb` must not be inferred from that tuple. Retained strict
bits orient only the visible children; the complete-pair lifting argument
justifies eliminating the hidden bits. Both rows must pass feasibility before
either row's score is used. This is a mathematical compatibility statement,
not certification of any implementation reuse.

## 4. Nonzero-E Falsifier

Take ordinary queried singleton `{a}` and an inactive strict subtree with
two leaf populations/capacities `(2,2)`, and parent `(H,C)=(4,3)`. The latter
has `E=1`, not zero. The additive root has `H=5,C=4,ell=2`, hence `I=1`.
The singleton has `L=0,U=1,b=0,B=1`; the combined offset is

```text
Cb=4, Hb=5, E0=1, lb=2, minor reward min(x-2,1).
```

At the root minor size two its reward is zero, while the major score is
`1/4`. Setting `E0=0` would lower the floor to one and falsely give the
minor reward one, hence score `1/2`. An actual compatible pair is
`{a,b,c,d}` and `{d,e}`, with inactive leaves `{b,c}` and `{d,e}`.
Thus the summary is realizable; the counterexample is not an invalid input.

Lead erratum after the later code review: the originally written second row
`{b,e}` gave attained maximum one on `{d,e}`, not two. The corrected witness
above has the stated capacities. The formulas and retained independent
checker used the correct direct metadata and are unchanged.

## 5. Sparse Construction And Bounds

The static core must retain, once, each node's `C,H,E`, parent ID, each
strict node's two ordered children, and each component's ordinary `L,U`
and boundary sums of `C,H,E`. Computing these sums may scan all static
children during preprocessing. That scan is not part of each query.

Process only the `s` positive leaf counts supplied by the query stream.
Look up their owners and leaf deviations; accumulate `b,B` only for those
owners. For each such owner walk parents until the first already marked
node. On marking a new child, record its edge to its parent even if that
parent was already marked. Starting from an already marked owner still
updates its rewards but adds no duplicate edge. There are exactly `Kq-1`
recorded edges for nonempty support and a connected root closure.

An active component enumerates just those recorded child edges. Subtract
their static `C,H,E` from its paid boundary totals. No full child list,
per-query `K`-entry zero array, dense owner map initialization, global
postorder filter, or global strict-index renumbering belongs in this phase.
Sparse postorder and fresh strict IDs are generated by traversing the active
adjacency. A hash map gives expected constant-time membership; paid static
ID-indexed epoch marks can support the corresponding RAM bound without
clearing all `K` entries each query. Epoch maintenance must itself be paid.

At a marked strict node at least one child is marked. Its two ordered slots
can be inspected in constant work, replacing at most one missing child with
a neutral capsule. Therefore, for `s>0`,

```text
Kq = number of marked static nodes
rq = number of marked strict nodes <= r
Ks = Kq + number of missing strict children <= Kq+rq.
```

Components carry their offset bundle as fields, not as additional vertices.
If a binary DP plan makes bundles and additive merges explicit, its node
count is larger, still `O(Ks)`, and must be counted separately.

Every core ancestor on an owner path represents a distinct original-tree
ancestor of the queried leaf. With original height `h` in edges,

```text
Kq <= min(K, s(h+1)).
```

For the specified perfect `G`-leaf tree, `h=log2(G)`. This includes `G=1`.
For a general tree retain `h`, or a proved height bound, instead of an exact
logarithm. Unary active paths may contain many strict nodes, so there is no
`O(s)` virtual-tree assertion. As a further structural bound for this core,
`Ks<=3*rq+1`: every nonroot component or capsule has a strict parent, so
there are at most `2*rq+1` nonstrict records. This follows from the inherited
core shape, not a new graph-compression theorem.

For zero support, define the special output as one neutral root with no bits;
its feasibility still needs checking. A uniform preparation bound is

```text
O(1 + s*(log(R+1)+log(d+2)) + Kq) arithmetic/index work
O(1 + s + Kq) transient records
O(K+d) additional/static records under the existing R=O(K) owner-run scheme.
```

If owner runs are not already covered by that scheme, state `O(K+d+R)`
instead. This bound starts with supplied sparse leaf counts, not raw feature
mapping, union/posting access, sorting, or reading a dense `G`-leaf stream.
Validation of the supplied sparse stream is paid. It is not possible to
validate a hidden dense input in `O(s)` work.

Exactly `rq` retained strict choices give at most `2^rq` assignments. A
conditioned label/row pass visits `O(Ks)` records and edges, giving
`O(Ks*2^rq)` counted work and `O(Ks)` transient row/label records. Reserve
before enumeration, after already-paid query validation and construction.
No list of all assignments is needed. Counts, rational arithmetic, static
storage, and integer bit widths are not physical-RAM or timing claims.

## 6. What Separates From Which Baseline

In the many-conflict family, each sibling pair has leaf `H=4,C=3` and parent
`H=8,C=4,E=0`. Higher nodes are additive. Query two elements in each leaf of
only the first pair, and set both root row sizes to `2G`.

The queried pair must split its two leaf majors between rows; a row can take
two queried elements from its size-three leaf and one from its size-one
leaf. Thus the exact overlap is three and the score is `3/(2G+1)`.
For `G>=4`, the static core has `r=G/2`, `K=3r+1`; the query skeleton has
`Kq=Ks=4`, `rq=1`, and two assignments. The root component subtracts the
one active strict boundary from its totals, absorbing every other pair into
one offset. For `G=2`, the strict root has three records instead.

This separates the reduced schedule from full-core scanning and full-core
orientation enumeration. It does not separate from the following fair
controls:

| Baseline | What this review establishes |
| --- | --- |
| Memoize zero-query results but still scan every child | Avoided `Theta(K)` preparation is a real scheduling improvement over this specific implementation. |
| Full-core inverse/profit DP | Already avoids `2^r`; sparse reduction can remove inactive visits, not claim invention of a polynomial alternative. |
| Generic tree DP with cached `(C,H,E)`, parent marking, and additive sibling totals | Can perform the same reduction and obtain the same query-local bounds. No asymptotic advantage over this equipped baseline is proved. |

Ordinary caching, ancestor closure, associative aggregation, and partial
evaluation are the mechanisms, not the novelty claim. Given the already
proved feasibility and bundle lemmas, the remaining specialized derivation
is the exact neutral sufficient statistic plus nonzero-E offset composition,
with complete-pair lifting and an explicit `rq`-parameterized schedule.
It is a useful composition/refinement; whether it is publishably new is not
decided here. No literature or historical-priority certification was attempted.

The same reduction could feed the existing inverse evaluator with shifted
base thresholds. Its previously proved plan argument gives
`O(Ks*(B+1)+B^2)` counted work and `O(Ks*(B+1))` cells, with
`B=sum_j min(q_j,C_j)`, after sparse preparation. This is a mathematical
composition observation, not a claim that this integration exists. Small
`s` need not make numeric `B` small. No speedup, practical prevalence, new
pruning strength, or reversal of earlier negative public receipts follows.

## 7. Bounded Independent Checker

The listing below imports only the Python standard library. It is a finite
count/pair experiment, not the lead's implementation. It deliberately uses
small explicit feature sets and dense fixture descriptions to establish the
oracle; it does not purport to implement compact static storage or raw-query
lookup. Only its reduction routine consumes sparse owner updates. Component
child collections are replaced by objects that raise on iteration/indexing,
so the query routine cannot silently scan inactive boundaries.

The independent reference enumerates arbitrary ordered child count pairs,
filters by the attained parent maximum, and maximizes scored-row overlap.
It uses neither the winner recurrence nor orientation branching. A second
oracle enumerates all ordered feature-set pairs with the exact fixture union,
groups them by every node's attained maximum, and compares their full
ordered-size/reward maps. All queries in each small feature universe are
checked, with all possible smaller root sizes and both zero/two outside-union
query features. Infeasible sizes produce `None`, not score zero.

Additional count fixtures cover the nonzero-E false shortcut, a generalized
ordinary bundle with forced nonquery occupancy, multiple inactive positive-E
boundaries, impossible metadata, and the bounded many-conflict family.
The structural checks count active records/edges, capsule additions, role
domains, offset inverse thresholds, and reduced branch assignments. They do
not test a production budget API or certify a particular implementation.

Run this exact retained listing with Python 3.11 from the repository root
without creating a second file:

```sh
awk '/^```python$/{emit=1;next} emit && /^```$/{exit} emit{print}' research_algorithms_20260920/Similarity-Query-Induced-Review.md | python3.11 -B -
```

```python
from collections import defaultdict
from fractions import Fraction
from itertools import product


COUNTS = defaultdict(int)


class ForbiddenBoundaryScan:
    def __iter__(self):
        raise AssertionError("query scanned static component children")

    def __getitem__(self, index):
        raise AssertionError("query indexed static component children")


def build_feature_tree_layout(populations, shape):
    masks, offset = [], 0
    for population in populations:
        masks.append(((1 << population) - 1) << offset)
        offset += population
    layout = []

    def append_feature_tree_node(part):
        if isinstance(part, int):
            mask, children, leaf = masks[part], (), part
        else:
            children = tuple(append_feature_tree_node(p) for p in part)
            mask = layout[children[0]][0] | layout[children[1]][0]
            leaf = None
        layout.append((mask, children, leaf))
        return len(layout) - 1

    append_feature_tree_node(shape)
    return layout, masks, offset


def derive_cached_feasibility_floors(nodes):
    floors = []
    for h, c, children, leaf in nodes:
        if not children:
            floor = 0 if 0 <= c <= h <= 2*c else None
        else:
            left, right = children
            hl, cl = nodes[left][:2]
            hr, cr = nodes[right][:2]
            el, er = floors[left], floors[right]
            deficit = cl + cr - c
            if (el is None or er is None or deficit < 0 or h != hl+hr
                    or not 0 <= c <= h <= 2*c):
                floor = None
            elif deficit == 0:
                floor = el + er
            else:
                x, y = 2*cl-hl-deficit, 2*cr-hr-deficit
                floor = min(y+el, x+er) if el <= x and er <= y else None
        floors.append(floor)
    return floors


def combine_ordered_count_maps(left, right, capacity):
    result = {}
    for (a, b), reward in left.items():
        for (x, y), other in right.items():
            aa, bb = a+x, b+y
            if aa <= capacity and bb <= capacity:
                key = aa, bb
                result[key] = max(result.get(key, -1), reward+other)
    return result


def enumerate_original_count_frontiers(nodes, query):
    tables = []
    for h, c, children, leaf in nodes:
        if not children:
            table = {(a, b): min(query[leaf], a)
                     for a in range(c+1) for b in range(c+1)
                     if max(a, b) == c and a+b >= h and c <= h}
        else:
            left, right = children
            table = combine_ordered_count_maps(tables[left], tables[right], c)
            table = {key: value for key, value in table.items() if max(key) == c}
        tables.append(table)
    return tables


def verify_cached_role_domains(nodes, floors, tables):
    for (h, c, children, leaf), floor, table in zip(nodes, floors, tables):
        domain = {a+b-h for a, b in table}
        expected = set() if floor is None else set(range(floor, 2*c-h+1))
        assert domain == expected
        assert all((b, a) in table for a, b in table)
        for e in expected:
            minor = h+e-c
            assert (c, minor) in table and (minor, c) in table
        COUNTS["cached_subtree_domains"] += 1


def compile_static_core_records(nodes, floors):
    records, owners, leaves = [], {}, {}

    def append_compressed_core_record(index):
        h, c, children, leaf = nodes[index]
        strict = bool(children) and sum(nodes[k][1] for k in children) > c
        ordinary, boundaries = [], []
        if strict:
            kids = tuple(append_compressed_core_record(k) for k in children)
        else:
            def collect_additive_region_members(current):
                hh, cc, kk, jj = nodes[current]
                if kk and sum(nodes[k][1] for k in kk) > cc:
                    boundaries.append(append_compressed_core_record(current))
                elif kk:
                    for k in kk:
                        collect_additive_region_members(k)
                else:
                    ordinary.append((jj, hh-cc, cc))

            collect_additive_region_members(index)
            kids = tuple(boundaries)
        record = dict(kind="strict" if strict else "component", h=h, c=c,
                      e=floors[index], kids=kids, parent=None,
                      lower=sum(x[1] for x in ordinary),
                      upper=sum(x[2] for x in ordinary),
                      totals=tuple(sum(records[k][f] for k in kids)
                                   for f in ("c", "h", "e")))
        current = len(records)
        records.append(record)
        for k in kids:
            records[k]["parent"] = current
        for j, lower, upper in ordinary:
            owners[j], leaves[j] = current, (lower, upper)
        return current

    root = append_compressed_core_record(len(nodes)-1)
    for record in records:
        if record["kind"] == "component":
            record["kids"] = ForbiddenBoundaryScan()
    return records, owners, leaves, root


def construct_sparse_query_skeleton(static, sparse):
    records, owners, leaves, root = static
    marked, edges, rewards = set(), defaultdict(list), defaultdict(lambda: [0, 0])
    for leaf, q in sparse:
        owner = owners[leaf]
        lower, upper = leaves[leaf]
        rewards[owner][0] += min(q, lower)
        rewards[owner][1] += min(q, upper)
        current = owner
        while current not in marked:
            marked.add(current)
            parent = records[current]["parent"]
            if parent is None:
                break
            edges[parent].append(current)
            current = parent
    plan, visits, subtractions = [], 0, 0

    def append_neutral_capsule_record(index):
        record = records[index]
        plan.append(dict(kind="base", c=record["c"], h=record["h"],
                         e=record["e"], b=0, reward=0, kids=()))
        return len(plan)-1

    def append_active_skeleton_record(index):
        nonlocal visits, subtractions
        visits += 1
        record = records[index]
        if record["kind"] == "strict":
            kids = tuple(append_active_skeleton_record(k) if k in marked
                         else append_neutral_capsule_record(k)
                         for k in record["kids"])
            plan.append(dict(kind="strict", c=record["c"], h=record["h"],
                             kids=kids))
        else:
            c0, h0, e0 = record["totals"]
            active = edges.get(index, ())
            for k in active:
                child = records[k]
                c0, h0, e0 = c0-child["c"], h0-child["h"], e0-child["e"]
                subtractions += 1
            kids = tuple(append_active_skeleton_record(k) for k in active)
            b, reward = rewards.get(index, (0, 0))
            base = (record["upper"]+c0, record["lower"]+record["upper"]+h0,
                    e0, b, reward)
            plan.append(dict(kind="component", c=record["c"], h=record["h"],
                             base=base, kids=kids))
        return len(plan)-1

    if marked:
        append_active_skeleton_record(root)
    else:
        append_neutral_capsule_record(root)
    kq = len(marked)
    rq = sum(records[k]["kind"] == "strict" for k in marked)
    edge_count = sum(len(e) for e in edges.values())
    assert visits == kq and edge_count == max(0, kq-1)
    assert subtractions <= edge_count
    assert len(plan) <= kq+rq if marked else len(plan) == 1
    assert len(plan) <= 3*rq+1
    COUNTS["skeletons"] += 1
    COUNTS["active_records"] += visits
    COUNTS["active_edges"] += edge_count
    COUNTS["boundary_subtractions"] += subtractions
    return plan, (kq, rq, len(plan))


def enumerate_offset_count_frontier(c, h, e, b, reward):
    lower = h-c+e
    assert 0 <= lower <= c and 0 <= b <= reward and reward-b <= c-lower
    table = {}
    for minor in range(lower, c+1):
        table[c, minor] = max(table.get((c, minor), -1), reward)
        table[minor, c] = max(table.get((minor, c), -1), min(b+minor-lower, reward))
    for target in range(reward+1):
        major = min(a+z-h for (a, z), r in table.items() if a == c and r >= target)
        minor = min(a+z-h for (a, z), r in table.items() if z == c and r >= target)
        assert major == e and minor == e+max(0, target-b)
        COUNTS["offset_threshold_entries"] += 2
    return table


def enumerate_reduced_count_frontiers(plan):
    tables = []
    for record in plan:
        kind, c = record["kind"], record["c"]
        if kind == "base":
            table = enumerate_offset_count_frontier(
                c, record["h"], record["e"], record["b"], record["reward"])
        else:
            table = ({(0, 0): 0} if kind == "strict"
                     else enumerate_offset_count_frontier(*record["base"]))
            for k in record["kids"]:
                table = combine_ordered_count_maps(table, tables[k], c)
            table = {key: value for key, value in table.items() if max(key) == c}
        tables.append(table)
    return tables[-1]


def recover_exact_root_envelope(table, capacity, minor, source):
    if (capacity, minor) not in table:
        return None
    scores = []
    for size, other in ((capacity, minor), (minor, capacity)):
        reward = table[size, other]
        scores.append(Fraction(reward, source+size-reward) if reward else Fraction(0))
    return max(scores)


def calculate_original_tree_height(nodes):
    heights = []
    for h, c, kids, leaf in nodes:
        heights.append(0 if not kids else 1+max(heights[k] for k in kids))
    return heights[-1]


def check_query_against_reference(nodes, floors, static, query, feature=None):
    original = enumerate_original_count_frontiers(nodes, query)
    sparse = tuple((j, q) for j, q in enumerate(query) if q)
    plan, sizes = construct_sparse_query_skeleton(static, sparse)
    reduced = enumerate_reduced_count_frontiers(plan)
    assert reduced == original[-1]
    COUNTS["count_frontier_comparisons"] += 1
    if feature is not None:
        assert feature == original[-1] == reduced
        COUNTS["feature_frontier_comparisons"] += 1
        COUNTS["feature_ordered_size_states"] += len(feature)
    kq, rq, ks = sizes
    if sparse:
        assert kq <= min(len(static[0]), len(sparse)*(calculate_original_tree_height(nodes)+1))
    c = nodes[-1][1]
    for minor in range(c+1):
        for outside in (0, 2):
            source = sum(query)+outside
            answer = recover_exact_root_envelope(original[-1], c, minor, source)
            assert answer == recover_exact_root_envelope(reduced, c, minor, source)
            if feature is not None:
                assert answer == recover_exact_root_envelope(feature, c, minor, source)
            COUNTS["root_envelope_comparisons"] += 1
            COUNTS["infeasible_root_envelopes"] += answer is None
    return reduced, sizes


def check_exhaustive_feature_families():
    cases = (
        ((0,), 0),
        ((2,), 0),
        ((1, 3), (0, 1)),
        ((1, 2, 1, 2), ((0, 1), (2, 3))),
        ((1, 2, 1, 2), (0, (1, (2, 3)))),
        ((0, 1, 0, 2), ((0, 1), (2, 3))),
    )
    for populations, shape in cases:
        layout, masks, n = build_feature_tree_layout(populations, shape)
        groups = defaultdict(list)
        for labels in product((1, 2, 3), repeat=n):
            a = sum(1 << j for j, label in enumerate(labels) if label & 1)
            b = sum(1 << j for j, label in enumerate(labels) if label & 2)
            caps = tuple(max((a & mask).bit_count(), (b & mask).bit_count())
                         for mask, kids, leaf in layout)
            groups[caps].append((a, b))
            COUNTS["ordered_full_union_pairs"] += 1
        for caps, pairs in groups.items():
            COUNTS["actual_summary_groups"] += 1
            nodes = [(mask.bit_count(), cap, kids, leaf)
                     for (mask, kids, leaf), cap in zip(layout, caps)]
            floors = derive_cached_feasibility_floors(nodes)
            verify_cached_role_domains(nodes, floors, enumerate_original_count_frontiers(
                nodes, (0,)*len(populations)))
            static = compile_static_core_records(nodes, floors)
            for mask in range(1 << n):
                query = tuple((mask & leaf).bit_count() for leaf in masks)
                feature = {}
                for a, b in pairs:
                    key = a.bit_count(), b.bit_count()
                    feature[key] = max(feature.get(key, -1), (mask & a).bit_count())
                check_query_against_reference(nodes, floors, static, query, feature)
        COUNTS["feature_families"] += 1


def check_explicit_offset_fixtures():
    fixtures = (
        # Queried singleton plus one inactive E=1 strict subtree.
        ([(1, 1, (), 0), (2, 2, (), 1), (2, 2, (), 2),
          (4, 3, (1, 2), None), (5, 4, (0, 3), None)], (1, 0, 0)),
        # Nontrivial ordinary b/B plus two inactive E=1 boundaries.
        ([(3, 2, (), 0), (3, 2, (), 1), (6, 4, (0, 1), None),
          (2, 2, (), 2), (2, 2, (), 3), (4, 3, (3, 4), None),
          (2, 2, (), 4), (2, 2, (), 5), (4, 3, (6, 7), None),
          (8, 6, (5, 8), None), (14, 10, (2, 9), None)], (0, 3, 0, 0, 0, 0)),
    )
    for index, (nodes, query) in enumerate(fixtures):
        floors = derive_cached_feasibility_floors(nodes)
        static = compile_static_core_records(nodes, floors)
        verify_cached_role_domains(nodes, floors, enumerate_original_count_frontiers(nodes, query))
        correct, sizes = check_query_against_reference(nodes, floors, static, query)
        plan, unused = construct_sparse_query_skeleton(static, tuple(
            (j, q) for j, q in enumerate(query) if q))
        assert sizes == (1, 0, 1)
        assert plan[-1]["base"][2] == index+1
        if index == 0:
            c, h, e, b, reward = plan[-1]["base"]
            false = enumerate_offset_count_frontier(c, h, 0, b, reward)
            assert recover_exact_root_envelope(correct, 4, 2, 1) == Fraction(1, 4)
            assert recover_exact_root_envelope(false, 4, 2, 1) == Fraction(1, 2)
            print("nonzero_E_exact=1/4 dropped_E_false=1/2")
        COUNTS["explicit_offset_fixtures"] += 1
    invalid = [
        [(3, 1, (), 0)],
        [(3, 2, (), 0), (3, 2, (), 1), (6, 2, (0, 1), None)],
        [(2, 1, (), 0), (2, 1, (), 1), (4, 3, (0, 1), None)],
        [(2, 2, (), 0), (2, 2, (), 1), (4, 3, (0, 1), None),
         (2, 2, (), 2), (2, 2, (), 3), (4, 3, (3, 4), None),
         (8, 4, (2, 5), None)],
    ]
    for nodes in invalid:
        floors = derive_cached_feasibility_floors(nodes)
        leaf_count = sum(not node[2] for node in nodes)
        table = enumerate_original_count_frontiers(nodes, (0,)*leaf_count)
        verify_cached_role_domains(nodes, floors, table)
        assert floors[-1] is None and not table[-1]
        COUNTS["invalid_summary_fixtures"] += 1


def check_local_conflict_scaling():
    print("G r K Kq rq Ks reduced_assignments envelope")
    for g in (2, 4, 8, 16, 32, 64, 128, 256):
        nodes = []

        def append_balanced_conflict_node(start, width):
            if width == 1:
                node = (4, 3, (), start)
            else:
                half = width//2
                left = append_balanced_conflict_node(start, half)
                right = append_balanced_conflict_node(start+half, half)
                c = 4 if width == 2 else nodes[left][1]+nodes[right][1]
                node = (4*width, c, (left, right), None)
            nodes.append(node)
            return len(nodes)-1

        append_balanced_conflict_node(0, g)
        floors = derive_cached_feasibility_floors(nodes)
        static = compile_static_core_records(nodes, floors)
        plan, sizes = construct_sparse_query_skeleton(static, ((0, 2), (1, 2)))
        table = enumerate_reduced_count_frontiers(plan)
        answer = recover_exact_root_envelope(table, 2*g, 2*g, 4)
        assert answer == Fraction(3, 2*g+1)
        kq, rq, ks = sizes
        assert rq == 1 and kq == ks == (3 if g == 2 else 4)
        assert len(static[0]) == (3 if g == 2 else 3*(g//2)+1)
        print(g, g//2, len(static[0]), kq, rq, ks, 2**rq, answer)
        COUNTS["scaling_fixtures"] += 1


check_exhaustive_feature_families()
check_explicit_offset_fixtures()
check_local_conflict_scaling()
for name in sorted(COUNTS):
    print(name + "=" + str(COUNTS[name]))
print("PASS: bounded independent challenge; no project imports")
```

### Executed Results

The retained listing completed with exit code zero using
`/Users/amuldotexe/.local/bin/python3.11 -B -`. An initial launch with the
machine's older default `python3` stopped at the first use of `int.bit_count`,
before enumeration results; the reproduction command therefore explicitly
selects Python 3.11. No mathematical mismatch was observed.

Exact successful output:

```text
nonzero_E_exact=1/4 dropped_E_false=1/2
G r K Kq rq Ks reduced_assignments envelope
2 1 3 3 1 3 2 3/5
4 2 7 4 1 4 2 1/3
8 4 13 4 1 4 2 3/17
16 8 25 4 1 4 2 1/11
32 16 49 4 1 4 2 3/65
64 32 97 4 1 4 2 1/43
128 64 193 4 1 4 2 3/257
256 128 385 4 1 4 2 1/171
active_edges=5329
active_records=7948
actual_summary_groups=50
boundary_subtractions=815
cached_subtree_domains=346
count_frontier_comparisons=2659
explicit_offset_fixtures=2
feature_families=6
feature_frontier_comparisons=2657
feature_ordered_size_states=13633
infeasible_root_envelopes=10552
invalid_summary_fixtures=4
offset_threshold_entries=27704
ordered_full_union_pairs=1576
root_envelope_comparisons=26858
scaling_fixtures=8
skeletons=2669
PASS: bounded independent challenge; no project imports
```

These are repeated finite checks, not that many independent datasets. There
are 50 summary groups across six fixed-union/topology fixtures, generated
from 1,576 ordered full-union pairs. Exhaustive query masks give 2,657
feature-oracle frontier comparisons; two extra offset cases give 2,659 count
comparisons total. The 26,858 root comparisons include 10,552 correctly
infeasible size/source cases. Two repeated skeleton constructions expose
the explicit offset fields, and eight more are the scaling fixtures, yielding
2,669 skeletons. The scaling table evaluates reduced count maps against the
proved family formula; it does not execute the full-core orientation solver
or a production branch-cap refusal. No timing numbers are reported.

The component-child guard remained installed in every skeleton construction.
This corroborates the listing's no-full-child-scan property, not the lead's
future implementation. Similarly, counting `2^rq` verifies the number of
retained choices, not a measured branch evaluator cost. All listed
comparisons/assertions passed; none were skipped after a budget refusal.

## 8. Closure

The bounded proof challenge is complete. The proof obligations are discharged
subject to the qualifications in Sections 1 and 5, with the executed finite
corroboration retained above. Only this review file was written. No lead
module, repository suite, public source, timing, or commit was touched.
Implementation, test-first integration, budget guards, public workloads, and
any priority assessment remain outside this review's scope. No further
rechecking is scheduled by this memo.
