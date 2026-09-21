# Independent Review: Compressed Interval Control

Date: 2026-09-21. Bounded mathematical sidecar for A05. Owned artifact: this
file only. No project probe imports, implementation changes, public datasets,
full-suite runs, timing measurements, commits, or historical-priority claims.

## Verdict And Proof Obligations

The proposed equivalence is correct for trusted, realizable complete-two-row
metadata, provided clipping uses current endpoint rewards, generalized bundles
keep all four scalars, and each control retains its own root-scoring rule.
The argument also preserves the interval relaxation on nonempty relaxed domains;
it does not turn that relaxation into a companion-feasibility test.

The retained checker exited zero: 42,875 associativity triples, 16,089 retained
subtree frontiers, 10,874 three-control score cases, and 3,962 exact `r=0`
envelopes passed. No mathematical counterexample to that qualified theorem was
found. The concrete
proof/documentation issues to carry into implementation are:

1. **Specify union/laminar scoring, not just rank equality.** The old routines
   return `R/(a+max(ell,R)-R)` when `R>0`, otherwise zero. Only the population-two
   interval control evaluates sizes `ell` and `u`. Applying that endpoint rule
   to all three controls would change the baseline. Union rank needs `Q=sum q`
   and `u`; its score additionally needs `a` and `ell`.
2. **Associativity is of independent frontiers on their current domains.** A
   strict-subtree clip cannot be moved to a different grouping or discarded.
   Raw `(d,b)` records need not be identical when their restricted functions
   agree. Empty intersections must remain infeasible, not become zero bundles.
3. **Supply the companion construction for `r=0`.** Feasible root sizes alone
   are not generally enough when `r>0`. The special additive case works because
   one globally designated major row can attain every leaf capacity at once.
4. **Separate prepared evaluation, sparse preparation and upstream work.** The
   evaluator's `O(K)` statement is sound as logical scalar work, but does not
   include reading `s` query entries, static compilation, raw feature mapping,
   or integer/rational bit costs. A `K`-record state budget is not a budget for
   every simultaneously live object.

These are explicit qualifications and missing proof details, not a claim that
the lead's separately implemented code violates them. That code was not read
or executed by this reviewer.

## Exact Control Being Preserved

Codebase-memory discovery located the existing research project index and these
unchanged definitions; their current source was also read directly:

- [Population-two interval control](experiments/probe_selective_capacity_similarity.py#L21)
  uses leaf domains `[H_j-C_j,C_j]`, leaf reward `min(x,q_j)`, sum convolution,
  node clipping, and only the two declared root sizes.
- [Union and laminar controls](experiments/probe_laminar_capacity_similarity.py#L21)
  use `R_union=min(Q,u)` and the leaf-rank/parent-cap laminar recurrence, followed
  by the rank score above, not interval endpoint scoring.
- [Prior additive-core theorem](Similarity-Additive-Region-Core.md) and its
  [independent review](Similarity-Additive-Core-Review.md) supply the component
  boundaries and optimal-overlap lifting contract.
- [Modal preparation](Similarity-Query-Limit-Exceptions.md#11-stronger-modal-query-signatures)
  and `prepare_exception_query_core` supply sorted-stream ownership lookup and
  exact bundle rewards under the declared query limit.

This is equality of functions and returned bounds, not equality of internal
record tuples, preservation of every suboptimal reward, or an implementation
audit. The finite checker below independently enumerates count vectors and
feature-set pairs. Its separate dense recurrence is a small mathematical
restatement of the inspected control, not an imported reference oracle.

## Composition And Deletion

For child `i`, let its nonempty integer domain be `[l_i,u_i]`, and evaluate
`p_i=min(l_i-d_i,b_i)`, `t_i=min(u_i-d_i,b_i)` AFTER all descendant clipping.
Its reward increments consist of `t_i-p_i` ones followed by zeros. For any
total increment `z` from zero through `sum(u_i-l_i)`, take rewarding increments
first, then nonrewarding ones. The resulting optimum is

```text
sum p_i + min(z, sum(t_i-p_i))
= min(x - (sum l_i - sum p_i), sum t_i), x=sum l_i+z.
```

Every total is achievable because sums of integer intervals have no holes.
For any chosen child occupancies, child optima can be attained simultaneously
on disjoint leaf sets. Thus taking maxima over allocations commutes with
parenthesization and child permutation. Clipping the resulting domain leaves
the formula valid there; the next merge must evaluate its new endpoints.
The identity is `(0,0,0,0)`; infeasibility is a different state.

At an additive node, `H_v=sum H_i` and `C_v=sum C_i`. Each child domain has
`l_i>=H_i-C_i` and `u_i<=C_i`, so its combined domain is already inside
`[H_v-C_v,C_v]`. This proves redundancy of BOTH the union-derived lower quota
and the capacity upper quota. Repeating this argument over the connected
region preserves the domain for every fixed tuple of strict-boundary counts,
not just the best root score. Internal strict constraints remain attached to
their original boundary subtrees.

For ordinary leaves, repeated composition gives exactly
`(L,U,A,B)`, with `A=L-sum min(q_j,H_j-C_j)` and `B=sum min(q_j,C_j)`.
For a retained component, combine that bundle with every strict-boundary
frontier. Its population is `L+U+sum H_boundary`; this must not be confused
with query intersection. Components without ordinary leaves have the identity
bundle but can still have nonempty boundary children.

Laminar ranks satisfy `rank_i<=C_i`. Therefore their additive parent cap is
redundant too; the ordinary-leaf rank is exactly `B`. Strict nodes retain
`min(C_v,sum rank_i)`. These two inductions establish full-tree equality.
At the root, compute interval scores at the feasible members of `{ell,u}`;
do not optimize over every intermediate root size. The distinct union/laminar
score follows from maximizing `min(x,R)/(a+x-min(x,R))` for `ell<=x<=u`.
All zero-overlap cases use score zero, including empty/empty.

## Counterexamples To Shortcuts

**Stale clipped endpoints.** Child `f(x)=min(x,3)` restricted to `[0,1]` has
actual top reward one, not three. Add child `g(y)=0` on `[0,2]`. At total size
three, the true optimum is one. Summing raw ceilings with raw debts gives
`min(3,3)=3`. Conversely, clipping `min(x,1)` to `[2,3]` requires lower reward
one: its normalized debt is `2-1=1`, not zero. Identity-merging must preserve
reward one at size two. The manuscript's endpoint rule avoids both errors.

**Ordinary aggregate leaf.** Two leaves `(H,C,q)=(3,2,0),(3,2,3)` give bundle
`(L,U,A,B)=(2,4,1,2)` and rewards `1,2,2` at sizes `2,3,4`. Aggregate physical
leaf `(H,C,q)=(6,4,3)` instead gives `2,3,3`. With `ell=2,u=4,a=3`, the correct
interval/exact envelope is `2/5`; the aggregate shortcut gives `3/4`.

**Wrong root scoring.** Two singleton leaves with `H=C=(1,1)`, additive root
`u=2`, `ell=0`, and `q=(1,0),a=1` are realized by the empty/full pair. Union
and laminar both return one. Their rank is one and the minimizing denominator
uses size one. Endpoint scoring would return only `1/2`. The interval and exact
complete-pair envelope correctly return `1/2` here.

**Relaxation is not a complete companion.** Leaves `H=C=(1,2)`, strict root
`u=2`, `ell=1`, and query `q=(1,1),a=2` admit relaxed row counts `(1,1)` and
interval score one. The size-two leaf must be wholly occupied by the actual
size-two row; the other row is the first singleton. The exact complete-pair
envelope is `max(1/3,1/2)=1/2`. Compression must preserve one for the interval
control, not silently strengthen it. These are realizable metadata.

## Why Zero Strict Nodes Is Exact

When `r=0`, `u=sum C_j=U`, `H_root=L+U`, and each leaf satisfies
`0<=H_j-C_j<=C_j<=H_j`. A major row reaching `u` must have count `C_j` in every
leaf. Every smaller-row count vector with `H_j-C_j<=x_j<=C_j` and total `ell`
is allowed; such a vector exists exactly when `L<=ell<=U`.

Fix an optimal minor subset of size `x_j` in a leaf. Its complement has size
`H_j-x_j<=C_j`; fill that complement with `C_j-(H_j-x_j)` elements of the minor
subset. The resulting major subset has size `C_j` and covers the leaf union
together with the chosen minor. Conversely, fix an optimal major subset of
size `C_j`; the minor includes its complement of size `H_j-C_j`, then takes
`x_j-(H_j-C_j)` elements of the major subset. These constructions work leafwise
and hence globally, realizing every additive-node maximum and the root sizes.

Therefore both marginal root optima lift to complete pairs and the interval
envelope is exact for `r=0`. The two optima need not use the same companion.
Root ties, a single original leaf, and empty unions are included. This is a
summary envelope, not recovery of the particular hidden rows, and does not
assert set containment. For `r>0`, the preceding counterexample rules out
unconditional exactness.

## Resource Scope

For a prepared core with `K<=3r+1` nodes, a postorder pass initializes `K`
scalar states, processes `K-1` child edges, and processes at most `K` bundle
payloads. High component degree is harmless only if edges are processed once;
it does not justify an independent boundary scan per original leaf. A traversal
stack/order or child references are additional `O(K)` auxiliary state unless
already supplied. The prepared query core itself also occupies `K` records.

Sorted sparse preparation copies `K` records, advances a cursor through
`R=O(r+1)` owner runs in total, and makes `s` deviation searches costing
`O(log(d+2))` each. Thus its work is `O(K+R+s log(d+2))`, and evaluation adds
`O(K)`. Even at `r=d=0`, reading a query with `s` entries costs `Omega(s)`.
Static modal storage is `O(r+d+1)` records; the existing modal compiler pays
`O(G log G)` comparisons and `O(G)` scratch on supplied counts. Original count
construction and raw query mapping/deduplication/aggregation/sorting are outside
these statements. No timing or physical-memory inference follows.

The query signature only clips rewards/validity; retained hard `L,U,C` values
must remain exact. Integer magnitudes, comparisons, multiplication and Fraction
normalization have bit costs. A record reservation available from static `K`
can be rejected before `iter(stream)` or `next(stream)`; refusal then says
nothing about unread stream validity. The standalone preparation helper does
not itself promise a record-budget gate. These admission and implementation
properties remain the lead's test obligations, not results of this checker.

## Bounded Independent Checker

Python 3.11, standard library only. The listing has no project imports or data
reads. It enumerates 35 distinct capped frontiers with `0<=l<=u<=3` and
`0<=f(x)<=x`, all ordered triples, and every subinterval clip of every pair.
The eight fixed tree/population layouts below have at most six union elements.
For each layout it enumerates every complete pair covering that union, groups
by attained maxima and sorted root sizes, then checks every leaf query-count
vector and zero/one outside-union query element. Repeated summaries, subtrees,
and query configurations are counted explicitly, not presented as datasets.

For every retained subtree, direct leaf-count enumeration checks the entire
interval frontier and the upper-only laminar rank. A dense recurrence supplies
a separate baseline-contract cross-check. Complete feature-set enumeration
checks every `r=0` envelope and the inequalities for all other cases. The
checker intentionally expands tiny domains; it does NOT implement the claimed
sparse resource bound or test serialized cores, stream validation, or budgets.

```python
from collections import Counter, defaultdict
from fractions import Fraction
from itertools import product

checks = Counter()


def evaluate_capped_record_value(rec, x):
    return min(x - rec[2], rec[3])


def expand_record_count_frontier(rec):
    return {} if rec is None else {
        x: evaluate_capped_record_value(rec, x)
        for x in range(rec[0], rec[1] + 1)}


def combine_capped_child_records(records):
    if any(rec is None for rec in records):
        return None
    lo = sum(rec[0] for rec in records)
    hi = sum(rec[1] for rec in records)
    base = sum(evaluate_capped_record_value(rec, rec[0]) for rec in records)
    top = sum(evaluate_capped_record_value(rec, rec[1]) for rec in records)
    return lo, hi, lo - base, top


def clip_capped_record_domain(rec, lo, hi):
    if rec is None:
        return None
    lo, hi = max(lo, rec[0]), min(hi, rec[1])
    return (lo, hi, rec[2], rec[3]) if lo <= hi else None


def enumerate_count_sum_frontier(records):
    result = {}
    for values in product(*(range(rec[0], rec[1] + 1) for rec in records)):
        reward = sum(evaluate_capped_record_value(rec, x)
                     for rec, x in zip(records, values))
        total = sum(values)
        result[total] = max(result.get(total, -1), reward)
    return result


def check_clipped_composition_algebra():
    records = [(lo, hi, lo - base, base + gain)
               for lo in range(4) for hi in range(lo, 4)
               for base in range(lo + 1) for gain in range(hi - lo + 1)]
    assert len(records) == 35
    identity = (0, 0, 0, 0)
    for first, second, third in product(records, repeat=3):
        expected = enumerate_count_sum_frontier((first, second, third))
        variants = [combine_capped_child_records((first, second, third)),
                    combine_capped_child_records((
                        combine_capped_child_records((first, second)), third)),
                    combine_capped_child_records((first,
                        combine_capped_child_records((second, third)))),
                    combine_capped_child_records((third, second, first))]
        assert all(expand_record_count_frontier(rec) == expected for rec in variants)
        checks['associativity_triples'] += 1
    for first, second in product(records, repeat=2):
        rec = combine_capped_child_records((first, second))
        oracle = enumerate_count_sum_frontier((first, second))
        for lo in range(rec[0], rec[1] + 1):
            for hi in range(lo, rec[1] + 1):
                clipped = clip_capped_record_domain(rec, lo, hi)
                expected = {x: y for x, y in oracle.items() if lo <= x <= hi}
                assert expand_record_count_frontier(clipped) == expected
                assert expand_record_count_frontier(
                    combine_capped_child_records((clipped, identity))) == expected
                checks['pair_domain_clips'] += 1
        assert clip_capped_record_domain(rec, rec[1] + 1, rec[1] + 2) is None
        checks['empty_domain_clips'] += 1
    assert combine_capped_child_records((None, identity)) is None
    assert expand_record_count_frontier(combine_capped_child_records(())) == {0: 0}
    stale = (0, 1, 0, 3)
    zero = (0, 2, 0, 0)
    assert evaluate_capped_record_value(combine_capped_child_records((stale, zero)), 3) == 1
    assert evaluate_capped_record_value((0, 3, 0, 3), 3) == 3
    assert expand_record_count_frontier(combine_capped_child_records(
        ((2, 3, 0, 1), identity))) == {2: 1, 3: 1}


def list_original_tree_nodes(tree):
    if isinstance(tree, int):
        return [tree]
    return [tree] + [node for child in tree for node in list_original_tree_nodes(child)]


def list_original_leaf_ids(tree):
    return [node for node in list_original_tree_nodes(tree) if isinstance(node, int)]


def build_compressed_tree_record(tree, strict):
    if tree in strict:
        return tree, None, tuple(build_compressed_tree_record(ch, strict) for ch in tree)
    owned, children = [], []

    def collect_component_boundary_records(node):
        if node in strict:
            children.append(build_compressed_tree_record(node, strict))
        elif isinstance(node, int):
            owned.append(node)
        else:
            for child in node:
                collect_component_boundary_records(child)

    collect_component_boundary_records(tree)
    return tree, tuple(owned), tuple(children)


def evaluate_compressed_tree_state(core, h, cap, query, states):
    top, owned, children = core
    child_states = [evaluate_compressed_tree_state(ch, h, cap, query, states)
                    for ch in children]
    records = [state[0] for state in child_states]
    rank = sum(state[1] for state in child_states)
    population = sum(state[2] for state in child_states)
    if owned is not None:
        lo = sum(h[j] - cap[j] for j in owned)
        hi = sum(cap[j] for j in owned)
        debt = lo - sum(min(query[j], h[j] - cap[j]) for j in owned)
        ceiling = sum(min(query[j], cap[j]) for j in owned)
        records.append((lo, hi, debt, ceiling))
        rank += ceiling
        population += lo + hi
    rec = combine_capped_child_records(records)
    if owned is None:
        rec = clip_capped_record_domain(rec, population - cap[top], cap[top])
        rank = min(rank, cap[top])
    else:
        assert rec == clip_capped_record_domain(rec, population - cap[top], cap[top])
        assert rank <= cap[top]
    states[top] = rec, rank, population
    checks['compressed_node_visits'] += 1
    checks['compressed_child_reductions'] += len(children)
    return states[top]


def evaluate_old_tree_state(tree, h, cap, query):
    if isinstance(tree, int):
        return (h[tree] - cap[tree], cap[tree], 0, query[tree]), min(query[tree], cap[tree]), h[tree]
    left, left_rank, left_h = evaluate_old_tree_state(tree[0], h, cap, query)
    right, right_rank, right_h = evaluate_old_tree_state(tree[1], h, cap, query)
    floor = left[0] + right[0]
    debt = floor - min(left[0] - left[2], left[3]) - min(right[0] - right[2], right[3])
    ceiling = min(left[1] - left[2], left[3]) + min(right[1] - right[2], right[3])
    population = left_h + right_h
    rec = (max(population - cap[tree], floor), min(cap[tree], left[1] + right[1]), debt, ceiling)
    assert rec[0] <= rec[1]
    return rec, min(cap[tree], left_rank + right_rank), population


def enumerate_subtree_count_vectors(tree, h, cap, lower):
    leaves = list_original_leaf_ids(tree)
    nodes = list_original_tree_nodes(tree)
    vectors = []
    for values in product(*(range(cap[j] + 1) for j in leaves)):
        vector = dict(zip(leaves, values))
        if all((sum(h[j] for j in list_original_leaf_ids(v)) - cap[v] if lower else 0)
               <= sum(vector[j] for j in list_original_leaf_ids(v)) <= cap[v]
               for v in nodes):
            vectors.append(vector)
    return vectors


def enumerate_full_count_frontier(vectors, query):
    frontier = {}
    for vector in vectors:
        size = sum(vector.values())
        reward = sum(min(query[j], count) for j, count in vector.items())
        frontier[size] = max(frontier.get(size, -1), reward)
    return frontier


def compute_jaccard_score_fraction(overlap, size, query_size):
    return Fraction(overlap, query_size + size - overlap) if overlap else Fraction(0)


def compute_rank_bound_fraction(rank, minimum, query_size):
    return compute_jaccard_score_fraction(rank, max(minimum, rank), query_size)


def enumerate_complete_pair_groups(tree, h):
    leaf_masks, offset = {}, 0
    for j, population in enumerate(h):
        leaf_masks[j] = ((1 << population) - 1) << offset
        offset += population
    nodes = list_original_tree_nodes(tree)
    masks = {v: sum(leaf_masks[j] for j in list_original_leaf_ids(v)) for v in nodes}
    groups = defaultdict(set)
    for labels in product((1, 2, 3), repeat=offset):
        left = sum(1 << j for j, label in enumerate(labels) if label & 1)
        right = sum(1 << j for j, label in enumerate(labels) if label & 2)
        capacities = tuple(max((left & masks[v]).bit_count(), (right & masks[v]).bit_count()) for v in nodes)
        sizes = tuple(sorted((left.bit_count(), right.bit_count())))
        groups[capacities, sizes].add(tuple(sorted((left, right))))
        checks['ordered_covering_pairs'] += 1
    return nodes, groups


def check_small_summary_layout(tree, h):
    nodes, groups = enumerate_complete_pair_groups(tree, h)
    checks['layouts'] += 1
    for (capacities, sizes), pairs in groups.items():
        cap = dict(zip(nodes, capacities))
        strict = {v for v in nodes if not isinstance(v, int) and cap[v[0]] + cap[v[1]] > cap[v]}
        core = build_compressed_tree_record(tree, strict)
        interval_vectors = {v: enumerate_subtree_count_vectors(v, h, cap, True) for v in nodes}
        upper_vectors = {v: enumerate_subtree_count_vectors(v, h, cap, False) for v in nodes}
        checks['summary_groups'] += 1
        checks['r_' + str(len(strict)) + '_summaries'] += 1
        for query in product(*(range(population + 1) for population in h)):
            states = {}
            root, rank, population = evaluate_compressed_tree_state(core, h, cap, query, states)
            assert len(states) <= 3 * len(strict) + 1
            assert population == sum(h)
            for v, (rec, subrank, subpopulation) in states.items():
                oracle = enumerate_full_count_frontier(interval_vectors[v], query)
                assert expand_record_count_frontier(rec) == oracle
                upper = enumerate_full_count_frontier(upper_vectors[v], query)
                assert subrank == max(upper.values())
                assert subpopulation == sum(h[j] for j in list_original_leaf_ids(v))
                checks['retained_subtree_frontiers'] += 1
                checks['retained_frontier_points'] += len(oracle)
                checks['retained_laminar_ranks'] += 1
            old, old_rank, old_h = evaluate_old_tree_state(tree, h, cap, query)
            assert expand_record_count_frontier(root) == expand_record_count_frontier(old)
            assert (rank, population) == (old_rank, old_h)
            query_mask, offset = 0, 0
            for count, leaf_h in zip(query, h):
                query_mask |= ((1 << count) - 1) << offset
                offset += leaf_h
            for outside in (0, 1):
                a = sum(query) + outside
                interval = max(compute_jaccard_score_fraction(evaluate_capped_record_value(root, x), x, a)
                               for x in sizes if root[0] <= x <= root[1])
                old_interval = max(compute_jaccard_score_fraction(evaluate_capped_record_value(old, x), x, a)
                                   for x in sizes if old[0] <= x <= old[1])
                laminar = compute_rank_bound_fraction(rank, sizes[0], a)
                union = compute_rank_bound_fraction(min(sum(query), sizes[1]), sizes[0], a)
                oracle = enumerate_full_count_frontier(interval_vectors[tree], query)
                assert interval == old_interval == max(compute_jaccard_score_fraction(oracle[x], x, a) for x in sizes)
                assert laminar == compute_rank_bound_fraction(old_rank, sizes[0], a)
                assert union == compute_rank_bound_fraction(min(sum(query), cap[tree]), sizes[0], a)
                exact = max(compute_jaccard_score_fraction((row & query_mask).bit_count(), row.bit_count(), a)
                            for pair in pairs for row in pair)
                assert exact <= interval <= laminar <= union
                checks['three_control_score_cases'] += 1
                checks['strict_relaxation_gaps'] += exact < interval
                if not strict:
                    assert exact == interval
                    checks['zero_strict_exact_cases'] += 1


def check_explicit_shortcut_failures():
    bundle = (2, 4, 1, 2)
    aggregate = (2, 4, 0, 3)
    assert expand_record_count_frontier(bundle) == {2: 1, 3: 2, 4: 2}
    assert max(compute_jaccard_score_fraction(evaluate_capped_record_value(bundle, x), x, 3)
               for x in (2, 4)) == Fraction(2, 5)
    assert max(compute_jaccard_score_fraction(evaluate_capped_record_value(aggregate, x), x, 3)
               for x in (2, 4)) == Fraction(3, 4)
    assert compute_rank_bound_fraction(1, 0, 1) == 1
    assert max(compute_jaccard_score_fraction(min(x, 1), x, 1) for x in (0, 2)) == Fraction(1, 2)
    tree, h = (0, 1), (1, 2)
    cap = {tree: 2, 0: 1, 1: 2}
    rec, _, _ = evaluate_old_tree_state(tree, h, cap, (1, 1))
    assert max(compute_jaccard_score_fraction(evaluate_capped_record_value(rec, x), x, 2)
               for x in (1, 2)) == 1
    _, groups = enumerate_complete_pair_groups(tree, h)
    pairs = groups[((2, 1, 2), (1, 2))]
    assert max(compute_jaccard_score_fraction((row & 3).bit_count(), row.bit_count(), 2)
               for pair in pairs for row in pair) == Fraction(1, 2)
    checks['named_shortcut_counterexamples'] = 4


check_clipped_composition_algebra()
layouts = [
    (0, (0,)),
    (0, (3,)),
    ((0, 1), (1, 2)),
    ((0, 1), (3, 3)),
    (((0, 1), (2, 3)), (1, 1, 1, 1)),
    (((0, 1), (2, 3)), (2, 1, 2, 1)),
    ((((0, 1), 2), 3), (2, 1, 1, 2)),
    ((((0, 1), (2, 3)), ((4, 5), (6, 7))), (1, 0, 1, 0, 1, 0, 1, 0)),
]
for layout in layouts:
    check_small_summary_layout(*layout)
check_explicit_shortcut_failures()
assert checks['strict_relaxation_gaps'] > 0
assert checks['compressed_node_visits'] - checks['compressed_child_reductions'] == checks['three_control_score_cases'] // 2
for key, value in sorted(checks.items()):
    print(f'{key}: {value}')
```

Run from the repository root; extraction executes this exact retained listing:

```sh
sed -n '/^```python$/,/^```$/p' research_algorithms_20260920/Similarity-Compressed-Interval-Review.md | sed '1d;$d' | /Users/amuldotexe/.local/bin/python3.11 -B
```

Executed once from this retained listing with exit status zero:

```text
associativity_triples: 42875
compressed_child_reductions: 10652
compressed_node_visits: 16089
empty_domain_clips: 1225
layouts: 8
named_shortcut_counterexamples: 4
ordered_covering_pairs: 2431
pair_domain_clips: 10339
r_0_summaries: 83
r_1_summaries: 92
r_2_summaries: 23
r_3_summaries: 1
retained_frontier_points: 49685
retained_laminar_ranks: 16089
retained_subtree_frontiers: 16089
strict_relaxation_gaps: 1333
summary_groups: 199
three_control_score_cases: 10874
zero_strict_exact_cases: 3962
```

The 199 summary groups yield 5,437 query-count configurations and 10,874 score
cases after adding the two outside-union choices. Each score case checks all
three controls, not three distinct inputs. The 2,431 ordered covering pairs
include 2,404 across the eight layouts plus 27 enumerated again for the named
companion counterexample. Equal row-swapped pairs are deduplicated within each
summary's witness set. Of the 199 summaries, 83 have `r=0`; the remaining
summaries exercise `r=1,2,3`, including empty ordinary-leaf bundles, adjacent
strict nodes, skewed trees, ties and zero populations.

The aggregate count identity `16089-10652=5437` corroborates one node visit and
one edge reduction per prepared tree per query configuration in this tiny
checker. It is not a measurement of the lead implementation or sparse prep.
The 1,333 strict relaxation gaps are expected: exact pairs can be more
constrained than a single interval-relaxed row. There were no equality failures
between the compressed and full-tree controls, nor any `r=0` envelope mismatch.

This is the stopping point for the bounded mathematical check. No larger-universe
search, randomized retry, public run, full suite, or production certification
was performed. Only this review document was authored; the lead retains the
separate implementation and test obligations.
