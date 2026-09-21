# Paths Cut Partitioned Rows

Date: 2026-09-21. Bounded A03 sidecar. Only this file is owned. [Paths-Forest-Cut-Repair.md](Paths-Forest-Cut-Repair.md) is terminal and unchanged. No production/shared-document edits, timings, RAM measurements, commits, or multicut redesign.

## Finding

**Yes: replace overlapping descendant rows by implicit canonical tail regions, and report colors only inside their actual intervals.** For a fixed cut and source, every surviving old ledger record belongs to exactly one region and one interval. If `Z_Q` counts the distinct-head reports over all intervals actually queried, then

```text
Z_Q <= number of covered surviving ledger records <= A.
```

Nested overlapping rows can instead have `Theta(a*A)` nonempty range/head incidences even with the same colored-reporting and pair-min indexes. This is an avoided repeated-enumeration cost, not an unconditional shortest-path lower bound. The strongest equal-index generic control can adopt the same implicit partition and match it.

Partitioning alone is insufficient: inspecting every old head catalog for each small nonempty region can still cost `Theta(a*A)` with only `A` useful reports. Design 1 below retains that failure; Design 2 removes it with a paid static colored range index. These are the only two designs considered.

## Inherited Contract

Use the frozen note's single deletion `f=(u,b)`, immutable original forest `F`, feasible potential `h`, old gates `P`, owners `pi`, complete old ledger `L`, and revived raw arcs `R`. Let `q=|P|`, `A=|L|`, `r=|R|`, and `N=({b} union heads(R))-P`, `a=|N|<=r+1`. `D` is the number of omitted, originally forest-dominated raw arcs. All reduced costs stay nonnegative. The retained graph is `(F-f) union X union R`; all other omitted raw arcs still have live zero-forest replacements. Raw edges, dormant self-arcs, parallel identities and old crossing-forest connectors remain covered exactly as in the frozen contract.

This sidecar changes row access, not revival discovery or the admissible edits. It assumes the paid raw-revival index, forest metadata, and lexicographic per-pair indexes from that contract. It adds a static colored index on **old row order**, never on rewritten current owners. No new raw source scan is hidden in a query. An absent base/revival/color index must be built and charged, or the declared indexed profile is unavailable.

For source `s`, let `sigma=1` if `s` is not already in `P union N`, otherwise zero. Let `M=N union ({s}-P)` and `t=|M|=a+sigma`. All roots of `F-f` are in `P union M`. Old capsule rows are sorted by `(tin(tail), original_ID)`; their positions, not vertices, are the range coordinates. Multiple records at the same tail are kept together for subtree bounds.

## Partition Without Owner Writes

For each old capsule root `g`, form a small laminar inclusion tree on `g` and the marks in `M` with old owner `g`. The parent of a mark is its nearest strict **original-forest** marked ancestor in this same capsule. Sort marks by old owner/preorder and use an ancestor stack. This takes `O(t log(t+2))` from unsorted marks, not a traversal of all original vertices.

For entry `v`, let `J_v` be the interval of old row `pi(v)` whose tails lie in the original subtree of `v`; an old root's interval is its entire row. Define

```text
C_v = J_v minus the J_z of immediate marked children z of v.
```

Then exclude the deleted identity if it was an old crossing-forest ledger record. Exclude its **record rank**, not all parallel records or all records at its tail. An interval may be empty because that forest region has no ledger tails; keep its port/link metadata anyway.

**A hole is not necessarily a live connector.** When `b` is a new root, retain `J_b` as a hole in its original inclusion parent's region, but do not create the deleted incoming connector. Dropping this hole because the new root has no live parent gives upper and lower rows ownership of the same records. If `b` was already an old gate, old capsule separation already excludes its subtree from the upper row; only the crossing record's tombstone is needed.

Source promotion uses the same construction. A source above the cut acquires the cut-root hole if appropriate; a source inside a fragment splits that fragment. Its old descendants below an intervening mark are not assigned twice. No independent source row is overlaid on top of an unchanged ancestor row.

### Partition And Hole Lemma

Every mark in `M` contributes exactly one inclusion edge/hole, including a detached cut root: **`t` holes total**. Nested holes are subtracted only at their immediate parent, not at every ancestor. After merging empty/adjacent pieces, the total number `I` of nonempty record intervals satisfies

```text
I <= q + 2t + delta,
I <= A_live,
delta = 1 if f is an old ledger connector, otherwise 0,
A_live = A-delta.
```

The first bound sums `1 + number_of_children` over `q+t` regions and adds at most one split for the tombstone. The second holds because the resulting intervals are pairwise disjoint and each contains a record. These are loose upper bounds, including empty old rows in the first one.

For any surviving record with tail `x`, descent through the inclusion tree stops at its deepest marked ancestor in the old capsule. Because `b` is marked, that ancestor is also the deepest marked ancestor connected to `x` in `F-f`. Thus its region is exactly the canonical current tail owner. This proves disjointness, coverage and valid prefixes without writing a vertex-owner plane, moving ledger records, or generating a current pair table. Crossings into old gates stay ordinary ledger records; dormant arcs are assigned by their tails like every other record.

Only `O(t+1)` new descriptors are needed: old rows with no new child/tombstone use an implicit whole-row default. Explicitly constructing `q` fresh row objects is not part of the publication bound. The generic canonical quotient is known; the update fact here is that its ledger-tail partition is represented by these few holes in unchanged old coordinates.

## Two Row Iterators

### Design 1: Disjoint Regions, All Old Heads

For each nonempty interval in `C_v`, query every head catalog in old row `pi(v)`. Correct pair minima result, but the work parameter is

```text
J_Q = sum over visited intervals B of degree_old(pi(B)),
```

including empty answers. `J_Q` need not be bounded by `A`; the negative fixture below has `J_Q=a*A` while only `A` heads actually occur. Disjoint record ownership does not imply cheap head discovery. Do not carry this iterator forward as the claimed improvement.

### Design 2: Disjoint Regions, Actual Colors

Treat a ledger record's original head as its color. Pay for one static distinct-color reporting index per old row, then for interval `[l,r)`:

1. Report only heads with a record in `[l,r)`.
2. For each reported head `p`, binary-search `[l,r)` into the old `(pi(v),p)` rank catalog and perform its RMQ with key `(reduced_cost, depth(tail), original_ID)`.
3. Emit that original-edge macro from `v` to `p` and its forest prefix. Emit multiple alternatives to the same head if it occurs in different pieces of `C_v`; no row-wide head accumulator is required.

An RMQ on the whole `J_v` after color discovery is **wrong for this canonical row contract**: its cheaper witness might lie inside a removed child hole. That witness may remain a valid bypass of the frozen design, so this error need not change distances; it breaks the claimed owner/interval provenance. A cut-root hole can also hide an actually broken prefix. Each pair minimum here must be clipped to the exact interval which supplied the color.

The color primitive is standard. Store `prev[i]`, the previous position of the same head in this old row, or `-1`. The first occurrence of each head in `[l,r)` is exactly a position with `prev[i]<l`. An argmin RMQ on `prev` reports such a position, then recurses on the two remaining subranges, always comparing against the **original** `l`. Stop when the minimum is at least `l`. This reports each head once, with at most `2k+1` nonempty RMQ calls for `k` outputs. Processing the smaller subrange first permits an `O(log(A+2))` pending stack, even when the argmin recursion is unbalanced.

With an ordinary static constant-query-time, linear-space RMQ, color reporting is `O(1+k)`; with the simpler aggregate tree used by the checker it is `O((1+k)log(A+2))`. Pair-catalog lookup, rank searches and RMQ conservatively cost `O(log(A+2))` per report under either profile. Preparation is `O(A)` extra words and at most `O(A log(A+2))` comparison-based build work beyond the already paid row order; there is no per-port index copy. Empty old rows need no new index, using their existing empty-row metadata.

[Muthukrishnan's SODA 2002 paper](https://www.researchwithrutgers.org/en/publications/efficient-algorithms-for-document-retrieval-problems/) is the primary provenance for document/color listing via range-query encodings. The institutional abstract and indexed paper excerpt were inspected; the PDF mirrors did not fetch. The small `prev` argument above states the operation used here directly. This was one targeted primitive check, not a fresh graph-literature survey. No novelty is assigned to color listing, RMQ, or canonical contraction.

## Exact Macro And Path Procedure

Run Dijkstra on states `P union M`, starting at `s`, with lexicographic labels `(reduced_cost, original_edge_count)`.

- Emit old ledger macros by Design 2 over `C_v`. A selected edge `(x,p)` has weight `(c(x,p), depth(x)-depth(v)+1)` and stores its own ID/incarnation.
- For each marked child `z`, emit its actual zero-forest connector from its inclusion parent, unless `z=b` is the cut root. Its hop count is the depth difference. These connectors replace forest edges crossing into new marks; forest edges crossing into old gates were already in `L`.
- Emit each revived raw arc from its **current** tail owner to its actual head. Revived tails are above the cut and cannot be below a member of `N`. Without source promotion their owner is their old `pi`; source promotion can split one such old-owner group by its tail-subtree interval. Use grouped, tail-sorted `R` catalogs and complementary slices, so each revived record is examined at most once per source query. Do not retain the old owner's revived macro after transferring its tail to the source region.
- Recover target owner `rho_s(v)` by old `pi(v)` plus the marked interval events. Return `D(rho_s(v))+h(v)-h(s)` and append the live forest suffix from that owner to `v`.

This is exactly the ordinary entry-closed quotient of the retained graph, except that parallel transitions to a head can be emitted once per interval instead of minimized across the entire region. Minimizing within each interval preserves their union's optimum. Every retained nonforest arc and surviving forest boundary is covered, and each macro lifts through its own current region. The inherited forest-domination argument then supplies exact primary distances in the full original graph after deletion. This is a short interface proof, not a new quotient-correctness claim.

The same correspondence preserves the minimum original-edge count **within the retained graph**: subtracting constant entry depth makes the paid pair key correct. Positive hop counts disambiguate zero-cost cycles. A minimum-cost/minimum-hop retained walk cannot repeat a vertex; deleting the intervening nonnegative cycle improves one of its two coordinates. The target suffix has the same canonical entry property. Thus output is a simple original-edge shortest path. Minimum hops in the full raw graph are not claimed, since an omitted dominated shortcut can have fewer hops than its forest replacement.

Pin the raw version, cut manifest and source-local descriptor through solving and output. Original-edge membership, incarnation, endpoints, continuity, reduced/original cost and the forest tombstone are checked during recovery. An old pinned snapshot may still use the deleted identity; the current one cannot. Use predecessor macro descriptors, not full expanded paths in solver state. Forward output may require a charged stack/spool; output length and forest/ID reads remain explicit costs.

## Incidence And Cost Theorem

Let `I_Q` be the nonempty intervals visited when states settle, and

```text
Z_Q = sum over those intervals B of number_of_distinct_heads(B),
W_Q = sum over those intervals B of number_of_records(B).
```

Each state settles at most once. The interval pieces across all states are disjoint, so

```text
I_Q <= I;  Z_Q <= W_Q <= A_live.
```

Inject each interval/head report into its first record in that interval. Distinct reports use distinct records, even when the same head occurs in different intervals. This proves the bound without assuming that heads are globally unique. `Z_Q` is exactly the number of useful pair-min queries in Design 2. The frozen overlap iterator's `H_Q` includes empty catalog probes; even giving it actual-color reporting leaves an overlapping nonempty incidence `Z_overlap` that can be `a*A`.

For that overlap representation, a record can belong to at most one old row and all `t` added/source descendant rows, giving `Z_overlap <= W_overlap <= (t+1)A`. In Design 1, an old capsule with `t_g` marks has at most `1+2t_g+delta_g` pieces, each probing at most `A_g` head catalogs; hence `J_Q <= (2t+2)A` globally, with the quadratic fixture below attaining the order. There is no pointwise assertion that `Z_Q <= Z_overlap`: the same head may need reports from several residual pieces where an overlapping whole-row minimum needed one. The proved improvement is the disjoint worst-case cap and the stated adversarial separation, not dominance on every query.

These are logical interval/head/record-coverage bounds, **not** a claim that all index cells or disk pages are read at most once. Pair binary searches and RMQs may revisit common index nodes; their cost is retained. Conversely, `W_Q=A` does not mean every raw ledger record is physically fetched: reporting plus minima can return one winner for many parallel records.

### Setup And Storage

Base preparation adds the `O(A)` color index to the existing per-pair and raw-revival indexes. Charge all raw retention, immutable forest/owner metadata, indexes and pinned versions; no physical RAM admission follows from this bound.

For one cut, reporting/grouping `R` and sorting ports retain the frozen cost. Mapping the `a` marked subtree endpoints to old-row ranks and locating the possible connector tombstone gives the conservative publication bound

```text
O(log(D+2) + (r+1)log(r+2) + (a+1)log(A+2)) work,
O(r+a+1) new retained words,
zero rewritten old vertex owners, ledger records, or old pair catalogs.
```

Save sorted marks, rank endpoints, inclusion links, nonempty pieces and owner events. For a source already marked, reuse them. Otherwise insert one source into the saved order, obtain its two rank endpoints, and resweep just these sparse marked groups (untouched old rows stay defaults). A deliberately simple query-local rebuild costs

```text
S_s = O(a+1 + log(A+2) + log(r+2)),
O(a+1) temporary descriptor words.
```

Existing port endpoints are reused; only the source needs new rank searches. Revived source slices need at most two searches in one grouped `R` catalog. This pays an `O(a)` resweep instead of claiming constant-time source surgery. No `O(n)`, `O(A)` or `O(r)` source-owner rewrite is required. The checker uses more expensive reference discovery for tiny graphs and does not implement this sparse publication engine.

### Query And Output

Let `R_Q<=r` count revived records examined and `B_Q<=t` the live marked-connector relaxations. With streaming interval/head outputs,

```text
E_Q <= Z_Q + R_Q + B_Q,
query = O(S_s + I_Q + Z_Q log(A+2)
          + (q+t+E_Q)log(q+t+2)) + target lookup + path/output delivery.
```

This uses the strong static color-RMQ profile. The checker's balanced-tree profile replaces `I_Q+Z_Q` color work by `O((I_Q+Z_Q)log(A+2))`. Optional unchanged whole-row caches are legal: replace their color calls with cached head reads, whose count plus remaining `Z_Q` is still bounded by `A_live` via the same disjoint-record injection.

Mutable solver state is `O(q+t)` with an indexed heap, plus the `O(a+1)` descriptor overlay and bounded index/output buffers. The streaming iterator needs no `O(Z_Q)` result cache; its pending color stack can be logarithmic. Grouped `R` and all base indexes may be immutable external data, but page-I/O bounds, external joins/sorting and actual host fit require separate accounting. Target ownership costs `O(log(a+2))` after old-owner lookup; emitting all original targets still pays their output size. Repeated source queries pay again: this is a per-query partition bound, not cross-query memoization.

## Two Counting Adversaries

### Overlap Costs `a*A`

Let `g->p1->...->pa->z` be a zero forest chain, cut `g->p1`, and query from `p1`. Add omitted zero-cost raw arcs `g->pi` for `i=2..a`, so every `pi` becomes a new port (`p1` is the new root). At `z`, put one outgoing arc to each of `H` distinct separate gate roots, plus the dormant return `z->g`. Then `A=H+1`, `r=a-1`, and all ports are reachable. The old upper row is empty after clipping.

Every overlapping new-port row contains all `A` records and all `A` colors: `W_overlap=Z_overlap=a*A`. Colored reporting does not cure this overlap. In the implicit partition, only the deepest port owns the ledger tails: `W_Q=Z_Q=A`, `I_Q=1`; the other ports still have live forest links. Their absence of ledger records does not make them unreachable. At `a=32,H=64`, the retained check compares 2,080 overlapping incidences with 65 disjoint incidences. All controls get the same raw revivals and indexes.

### All-Catalog Probing Still Dominates

Use the same cut and port chain but put one arc `pi->hi` at each port, with `a` distinct separate old heads and no dormant return needed. Now `A=a`, each canonical region has one record, `I_Q=Z_Q=A`, yet every region's **old** row head directory has size `A`. Design 1 issues `a*A` pair probes, mostly empty. Design 2 reports exactly `A` actual colors. The retained `a=32` fixture has 1,024 catalog probes versus 32 useful pair queries.

This is not a claim that pair minima alone read `a*A` parallel records: a good RMQ already skips parallel candidates. Distinct colors in the first adversary, and absent colors in the second, are essential to the stated separations. When `Z_Q=A` and all those heads matter, no sublinear incidence claim remains. When only a few ports settle, the overlapping iterator can already be cheap; the new color-index storage and sparse-source setup may not pay for themselves.

## Equal-Index Controls And Decision

| Control | Comparison |
| --- | --- |
| Frozen overlapping macro rows plus the same colored index | Still emits `a*A` nonempty incidences on the nested common-tail fixture. The separation is duplicated valid domains, not inferior head discovery. |
| Implicit canonical regions plus every old head catalog | Correct, but the second fixture retains `a*A` empty/useful probes. This isolates the need for actual-interval color reporting. |
| Strong generic dynamic-tree/canonical-quotient control with the same raw, port, color and pair indexes | Can construct the same inclusion holes, clip the same regions and obtain the same bounds. No advantage over this control is proved, and no historical conclusion follows merely from that match. |
| Eager canonical ownership/partial contraction update | Must pay only for the objects it actually chooses to materialize or propagate. The sidecar avoids ownership/membership rewrites; it does not prove superiority to every selective contraction strategy. Any control may use the same original-edge witness coverage and omit the same dominated arcs. |

**Concrete decision:** for the single-cut, many-nested-port profile, carry forward Design 2 instead of repeated descendant-row enumeration. It caps useful old-ledger interval/head incidences by `A` and adds only a linear static color index plus sparse version/source descriptors. Do not claim a new general canonical quotient, a dynamic-tree innovation, a measured RAM saving, or the missing seven-family novelty result. The cost/separation finding is complete at this boundary; no multicut extension or further survey is opened here.

## Retained Bounded Checker

One self-contained standard-library snippet follows. It builds real static aggregate-tree color and pair indexes and checks their outputs against sets/minima on tiny intervals. The source/partition setup deliberately discovers all tiny reference rows; this is not the sparse setup implementation or a memory measurement. Full paths, eager forest-prefix expansion, collected head lists, explicit reference owners and a duplicate-entry Python heap are oracle conveniences. The set/minimum scans inside assertions are independent verification reads, not the indexed procedure's access path. The proof supplies the asymptotic sparse representation; counts below are logical range/head incidences, not timings or page reads.

```sh
awk '/^<!-- CUT-PARTITION-PROBE-START -->/{p=1;next} /^<!-- CUT-PARTITION-PROBE-END -->/{p=0} p && !/^```/' research_algorithms_20260920/Paths-Cut-Partitioned-Rows.md | python3 -
```

<!-- CUT-PARTITION-PROBE-START -->
```python
from bisect import bisect_left
from collections import defaultdict
from heapq import heappop, heappush
from math import inf
from random import Random


def build_static_minimum_index(keys):
    size = 1
    while size < len(keys):
        size *= 2
    tree = [(inf,)] * (2 * size)
    tree[size:size + len(keys)] = keys
    for position in range(size - 1, 0, -1):
        tree[position] = min(tree[2 * position], tree[2 * position + 1])
    return size, tree


def query_static_minimum_index(index, left, right):
    assert left < right
    size, tree = index
    left, right, answer = left + size, right + size, (inf,)
    while left < right:
        if left % 2:
            answer, left = min(answer, tree[left]), left + 1
        if right % 2:
            right -= 1
            answer = min(answer, tree[right])
        left, right = left // 2, right // 2
    return answer


def report_interval_head_colors(index, row, left, right, stats):
    threshold, stack = left, [(left, right)]
    while stack:
        low, high = stack.pop()
        if low >= high:
            continue
        stats["color_rmq"] += 1
        previous, position = query_static_minimum_index(index, low, high)
        if previous >= threshold:
            continue
        yield row[position][2]
        pieces = [(low, position), (position + 1, high)]
        stack.extend(sorted(pieces, key=lambda pair: pair[1] - pair[0], reverse=True))


def test_original_forest_ancestry(base, ancestor, vertex):
    return base["tin"][ancestor] <= base["tin"][vertex] < base["end"][ancestor]


def recover_original_forest_prefix(base, ancestor, vertex, cut):
    path = []
    while vertex != ancestor:
        if vertex == cut or base["parent"][vertex] < 0:
            return None
        path.append(base["forest_id"][vertex])
        vertex = base["parent"][vertex]
    return tuple(reversed(path))


def prepare_immutable_ledger_indexes(parent, extras, positive=False):
    children = defaultdict(list)
    for vertex, ancestor in enumerate(parent):
        children[ancestor].append(vertex)
    tin, end, depth, order = {}, {}, {}, []
    def visit_original_forest_vertex(vertex, level):
        tin[vertex], depth[vertex] = len(order), level
        order.append(vertex)
        for child in children[vertex]:
            visit_original_forest_vertex(child, level + 1)
        end[vertex] = len(order)
    for root in children[-1]:
        visit_original_forest_vertex(root, 0)
    forest_id, raw = {}, []
    for vertex, ancestor in enumerate(parent):
        if ancestor >= 0:
            forest_id[vertex] = len(raw)
            raw.append((len(raw), ancestor, vertex, 0, 7))
    forest_count = len(raw)
    potential = [depth[v] if positive else 0 for v in range(len(parent))]
    for tail, head, cost in extras:
        reduced = cost + max(0, potential[tail] - potential[head])
        raw.append((len(raw), tail, head, reduced, 7))
    assert all(e[3] >= 0 and e[3] - potential[e[1]] + potential[e[2]] >= 0 for e in raw)
    base = dict(parent=tuple(parent), tin=tin, end=end, depth=depth, order=order,
                forest_id=forest_id, forest_count=forest_count, raw=tuple(raw), h=potential)
    omitted = [e for e in raw[forest_count:] if test_original_forest_ancestry(base, e[1], e[2])]
    exceptions = [e for e in raw[forest_count:] if e not in omitted]
    gates = set(children[-1]) | {e[2] for e in exceptions}
    owner = {}
    for vertex in order:
        owner[vertex] = vertex if vertex in gates else owner[parent[vertex]]
    ledger = exceptions + [e for e in raw[:forest_count] if owner[e[1]] != owner[e[2]]]
    rows = {gate: [] for gate in gates}
    for edge in ledger:
        rows[owner[edge[1]]].append(edge)
    color_indexes, catalogs, ranks = {}, {}, {}
    for gate, row in rows.items():
        row.sort(key=lambda e: (tin[e[1]], e[0]))
        previous, last, pairs = [], {}, defaultdict(list)
        for position, edge in enumerate(row):
            previous.append((last.get(edge[2], -1), position))
            last[edge[2]] = position
            pairs[edge[2]].append(position)
            ranks[edge[0]] = (gate, position)
        color_indexes[gate] = build_static_minimum_index(previous)
        catalogs[gate] = {}
        for head, positions in pairs.items():
            keys = [(row[i][3], depth[row[i][1]], row[i][0], i) for i in positions]
            catalogs[gate][head] = (positions, build_static_minimum_index(keys))
    base.update(gates=frozenset(gates), owner=owner, omitted=tuple(omitted),
                exceptions=tuple(exceptions), ledger=tuple(ledger), rows=rows,
                color_indexes=color_indexes, catalogs=catalogs, ranks=ranks)
    return base


def subtract_sorted_interval_holes(left, right, holes):
    pieces, cursor = [], left
    for start, stop in sorted(holes):
        start, stop = max(left, start), min(right, stop)
        if start >= stop or stop <= cursor or start >= right:
            continue
        if cursor < start:
            pieces.append((cursor, start))
        cursor = max(cursor, stop)
    if cursor < right:
        pieces.append((cursor, right))
    return pieces


def locate_current_marked_owner(base, states, vertex):
    while vertex not in states:
        vertex = base["parent"][vertex]
        assert vertex >= 0
    return vertex


def prepare_source_partition_view(base, cut, source):
    revived = [e for e in base["omitted"] if cut is not None
               and test_original_forest_ancestry(base, cut, e[2])
               and base["depth"][e[1]] < base["depth"][cut]]
    ports = (({cut} if cut is not None else set()) | {e[2] for e in revived}) - base["gates"]
    states = base["gates"] | ports | {source}
    marks = states - base["gates"]
    bounds, holes, links = {}, defaultdict(list), defaultdict(list)
    for entry in states:
        row = base["rows"][base["owner"][entry]]
        keys = [base["tin"][e[1]] for e in row]
        bounds[entry] = (bisect_left(keys, base["tin"][entry]),
                         bisect_left(keys, base["end"][entry]))
    grouped = defaultdict(list)
    for mark in marks:
        grouped[base["owner"][mark]].append(mark)
    for gate, group in grouped.items():
        stack = [gate]
        for mark in sorted(group, key=base["tin"].get):
            while not test_original_forest_ancestry(base, stack[-1], mark):
                stack.pop()
            ancestor = stack[-1]
            holes[ancestor].append(bounds[mark])
            prefix = recover_original_forest_prefix(base, ancestor, mark, cut)
            if prefix is None:
                assert mark == cut
            else:
                links[ancestor].append((mark, prefix))
            stack.append(mark)
    assert sum(map(len, holes.values())) == len(marks)
    deleted = base["forest_id"].get(cut)
    delta = int(deleted in base["ranks"])
    if delta:
        gate, position = base["ranks"][deleted]
        entry = locate_current_marked_owner(base, states, base["parent"][cut])
        assert base["owner"][entry] == gate
        holes[entry].append((position, position + 1))
    parts = {v: subtract_sorted_interval_holes(*bounds[v], holes[v]) for v in states}
    coverage = defaultdict(int)
    for entry, intervals in parts.items():
        row = base["rows"][base["owner"][entry]]
        for left, right in intervals:
            for edge in row[left:right]:
                coverage[edge[0]] += 1
                assert locate_current_marked_owner(base, states, edge[1]) == entry
                assert recover_original_forest_prefix(base, entry, edge[1], cut) is not None
    assert dict(coverage) == {e[0]: 1 for e in base["ledger"] if e[0] != deleted}
    count = sum(map(len, parts.values()))
    assert count <= len(base["gates"]) + 2 * len(marks) + delta
    assert count <= len(base["ledger"]) - delta
    revival_rows = defaultdict(list)
    for edge in revived:
        revival_rows[base["owner"][edge[1]]].append(edge)
    for row in revival_rows.values():
        row.sort(key=lambda e: (base["tin"][e[1]], e[0]))
    revival_keys = {gate: [base["tin"][e[1]] for e in row] for gate, row in revival_rows.items()}
    return dict(cut=cut, source=source, revived=revived, ports=ports, states=states,
                parts=parts, bounds=bounds, links=links, revival_rows=revival_rows,
                revival_keys=revival_keys, deleted=deleted, delta=delta, marks=marks)


def generate_partition_macro_neighbors(base, view, entry, stats):
    gate = base["owner"][entry]
    row = base["rows"][gate]
    for left, right in view["parts"][entry]:
        stats["intervals"] += 1
        stats["covered"] += right - left
        stats["all_catalogs"] += len(base["catalogs"][gate])
        heads = list(report_interval_head_colors(base["color_indexes"][gate], row, left, right, stats))
        assert len(heads) == len(set(heads)) and set(heads) == {e[2] for e in row[left:right]}
        for head in heads:
            stats["heads"] += 1
            positions, index = base["catalogs"][gate][head]
            low, high = bisect_left(positions, left), bisect_left(positions, right)
            stats["pair_rmq"] += 1
            key = query_static_minimum_index(index, low, high)
            edge = row[key[-1]]
            oracle = min((e for e in row[left:right] if e[2] == head),
                         key=lambda e: (e[3], base["depth"][e[1]], e[0]))
            assert edge == oracle
            prefix = recover_original_forest_prefix(base, entry, edge[1], view["cut"])
            assert prefix is not None and edge[0] != view["deleted"]
            yield head, edge[3], prefix + (edge[0],)
    for mark, prefix in view["links"].get(entry, ()):
        stats["links"] += 1
        yield mark, 0, prefix
    source = view["source"]
    original_states = base["gates"] | view["ports"]
    source_added = source not in original_states
    revived = view["revival_rows"].get(gate, ())
    keys = view["revival_keys"].get(gate, ())
    low = bisect_left(keys, base["tin"][source]) if source_added and gate == base["owner"][source] else 0
    high = bisect_left(keys, base["end"][source]) if source_added and gate == base["owner"][source] else 0
    slices = []
    if entry in base["gates"]:
        slices = [(0, low), (high, len(revived))]
    elif source_added and entry == source:
        slices = [(low, high)]
    for left, right in slices:
        for edge in revived[left:right]:
            stats["revived"] += 1
            assert locate_current_marked_owner(base, view["states"], edge[1]) == entry
            prefix = recover_original_forest_prefix(base, entry, edge[1], view["cut"])
            assert prefix is not None
            yield edge[2], edge[3], prefix + (edge[0],)


def solve_partition_snapshot_paths(base, view):
    source, states = view["source"], view["states"]
    distances = {v: (inf, inf) for v in states}
    distances[source] = (0, 0)
    paths, queue, settled = {source: ()}, [(0, 0, source)], set()
    stats = dict(intervals=0, covered=0, all_catalogs=0, color_rmq=0,
                 heads=0, pair_rmq=0, links=0, revived=0)
    while queue:
        cost, hops, entry = heappop(queue)
        if (cost, hops) != distances[entry]:
            continue
        assert entry not in settled
        settled.add(entry)
        for head, weight, path in generate_partition_macro_neighbors(base, view, entry, stats):
            candidate = (cost + weight, hops + len(path))
            if candidate < distances[head]:
                assert head not in settled
                distances[head], paths[head] = candidate, paths[entry] + path
                heappush(queue, (*candidate, head))
    assert stats["heads"] == stats["pair_rmq"] <= stats["covered"] <= len(base["ledger"]) - view["delta"]
    assert stats["revived"] <= len(view["revived"])
    assert stats["links"] <= len(view["marks"])
    assert stats["color_rmq"] <= 2 * stats["heads"] + stats["intervals"]
    return distances, paths, settled, stats


def verify_partition_snapshot_query(base, view):
    distances, paths, settled, stats = solve_partition_snapshot_paths(base, view)
    states, source, raw = view["states"], view["source"], base["raw"]
    owners = {v: locate_current_marked_owner(base, states, v) for v in base["order"]}
    canonical = []
    retained = {e[0] for e in base["exceptions"] + tuple(view["revived"])}
    for edge in raw:
        identity, tail, head, cost, _ = edge
        if identity == view["deleted"]:
            continue
        if identity >= base["forest_count"] and identity not in retained:
            continue
        if identity < base["forest_count"] and owners[tail] == owners[head]:
            continue
        assert head in states
        canonical.append((owners[tail], head, cost, base["depth"][tail] - base["depth"][owners[tail]] + 1))
    reference = {v: (inf, inf) for v in states}
    reference[source] = (0, 0)
    for _ in range(len(states) - 1):
        for tail, head, cost, hops in canonical:
            value = (reference[tail][0] + cost, reference[tail][1] + hops)
            reference[head] = min(reference[head], value)
    assert distances == reference
    original = [inf] * len(base["parent"])
    original[source] = 0
    for _ in range(len(original) - 1):
        for identity, tail, head, cost, _ in raw:
            if identity != view["deleted"]:
                original[head] = min(original[head], original[tail] + cost - base["h"][tail] + base["h"][head])
    checked = 0
    for target in base["order"]:
        owner = owners[target]
        assert distances[owner][0] + base["h"][target] - base["h"][source] == original[target]
        if distances[owner][0] == inf:
            continue
        suffix = recover_original_forest_prefix(base, owner, target, view["cut"])
        path = paths[owner] + suffix
        vertex, cost, seen = source, 0, {source}
        for identity in path:
            edge_id, tail, head, reduced, incarnation = raw[identity]
            assert edge_id == identity and incarnation == 7 and identity != view["deleted"]
            assert tail == vertex and head not in seen
            seen.add(head)
            vertex, cost = head, cost + reduced - base["h"][tail] + base["h"][head]
        assert vertex == target and cost == original[target]
        assert len(path) == distances[owner][1] + len(suffix)
        checked += 1
    return checked, stats, settled


def build_nested_port_fixture(ports, heads, distributed=False):
    parent = [-1] + list(range(ports)) + [ports] + [-1] * heads
    terminal, first_head = ports + 1, ports + 2
    extras = [(0, v, 0) for v in range(2, ports + 1)]
    if distributed:
        assert ports == heads
        extras += [(v, first_head + v - 1, 1) for v in range(1, ports + 1)]
    else:
        extras += [(terminal, first_head + h, 1) for h in range(heads)]
        extras += [(terminal, 0, 0)]
    return prepare_immutable_ledger_indexes(parent, extras)


base = build_nested_port_fixture(32, 64)
view = prepare_source_partition_view(base, 1, 1)
_, stats, settled = verify_partition_snapshot_query(base, view)
overlap_records, overlap_heads = 0, 0
for port in view["ports"] & settled:
    row = base["rows"][base["owner"][port]]
    left, right = view["bounds"][port]
    overlap_records += right - left
    overlap_heads += len({e[2] for e in row[left:right]})
assert len(view["ports"]) == 32 and len(base["ledger"]) == 65
assert overlap_records == overlap_heads == 2080
assert stats["intervals"] == 1 and stats["covered"] == stats["heads"] == 65
print("nested_common_tail", dict(A=65, ports=32, overlap=2080, partition_heads=65, intervals=1))

base = build_nested_port_fixture(32, 32, distributed=True)
view = prepare_source_partition_view(base, 1, 1)
_, stats, _ = verify_partition_snapshot_query(base, view)
assert stats["all_catalogs"] == 1024 and stats["heads"] == stats["covered"] == 32
print("all_catalog_negative", dict(A=32, intervals=32, catalog_probes=1024, actual_heads=32))

# A root without an incoming link must still subtract its old-parent tail hole.
base = prepare_immutable_ledger_indexes([-1, 0, 1, -1], [(2, 3, 1), (2, 0, 0)])
view = prepare_source_partition_view(base, 1, 0)
assert not view["parts"][0] and len(base["rows"][0]) == 2
assert not view["links"].get(0)
_, stats, _ = verify_partition_snapshot_query(base, view)
assert stats["heads"] == 0
print("detached_root_hole retained_without_live_incoming_link")

# Child holes cannot be ignored when selecting a minimum for a reported color.
base = prepare_immutable_ledger_indexes([-1, 0, 1, 2, -1],
                                        [(0, 2, 0), (1, 4, 5), (3, 4, 0), (3, 0, 0)])
view = prepare_source_partition_view(base, 1, 1)
stats = dict(intervals=0, covered=0, all_catalogs=0, color_rmq=0,
             heads=0, pair_rmq=0, links=0, revived=0)
local = [cost for head, cost, _ in generate_partition_macro_neighbors(base, view, 1, stats) if head == 4]
assert local == [5]
assert min(e[3] for e in base["rows"][0] if e[2] == 4) == 0
verify_partition_snapshot_query(base, view)
print("whole_subtree_rmq_mutation child_hole_witness_rejected")

# The same head in separate pieces must be reported again, with a clipped minimum.
base = prepare_immutable_ledger_indexes([-1, 0, 0, 0, 2, -1],
                                        [(1, 5, 2), (3, 5, 1), (4, 5, 0), (4, 0, 0)])
view = prepare_source_partition_view(base, 2, 0)
checked, stats, _ = verify_partition_snapshot_query(base, view)
assert stats["intervals"] == stats["heads"] == stats["covered"] == 2
current_distances, current_paths, _, _ = solve_partition_snapshot_paths(base, view)
old_view = prepare_source_partition_view(base, None, 0)
verify_partition_snapshot_query(base, old_view)
old_distances, old_paths, _, _ = solve_partition_snapshot_paths(base, old_view)
assert current_distances[5][0] == 1 and old_distances[5][0] == 0
assert base["forest_id"][2] in old_paths[5] and base["forest_id"][2] not in current_paths[5]
print("repeated_head_pieces", dict(intervals=2, reports=2, old_cost=0, current_cost=1), "pinned_ids_valid")

# Promoting a source above the cut transfers a revival, even after its old owner settles.
base = prepare_immutable_ledger_indexes([-1, 0, 1, 2], [(1, 3, 0), (3, 0, 0)])
view = prepare_source_partition_view(base, 2, 1)
assert 1 not in base["gates"] | view["ports"]
_, stats, settled = verify_partition_snapshot_query(base, view)
assert 0 in settled and stats["revived"] == len(view["revived"]) == 1
assert not view["parts"][1] and 2 not in settled
print("source_revival_slice examined_once_after_old_owner_settles")

# Nested ports, old-gate connector, dormant zero return, parallel revival, and source splits.
parent = [-1, 0, 1, 2, 3, 2, 0, -1]
extras = [(0, 2, 0), (0, 3, 0), (4, 0, 0), (5, 7, 2),
          (6, 3, 1), (0, 1, 0), (5, 7, 1), (4, 4, 0)]
base = prepare_immutable_ledger_indexes(parent, extras, positive=True)
assert 3 in base["gates"] and base["forest_id"][3] in base["ranks"]
target_queries, target_paths = 0, 0
for cut in (1, 2, 3, None):
    for source in range(len(parent)):
        checked, _, _ = verify_partition_snapshot_query(base, prepare_source_partition_view(base, cut, source))
        target_queries, target_paths = target_queries + 1, target_paths + checked
print("targeted_sources", dict(queries=target_queries, paths=target_paths))

# Focused random one-cut/source partitions, plus pinned uncut replays; no multicut run.
rng = Random(20260922)
queries, paths, cuts_checked = 0, 0, 0
for trial in range(40):
    parent = [-1] + [rng.randrange(v) if rng.randrange(5) else -1 for v in range(1, 10)]
    extras = [(rng.randrange(10), rng.randrange(10), rng.randrange(3)) for _ in range(14)]
    extras += extras[:2]
    base = prepare_immutable_ledger_indexes(parent, extras, positive=bool(trial % 2))
    frozen = (base["raw"], tuple((g, tuple(row)) for g, row in sorted(base["rows"].items())))
    for cut in [v for v, p in enumerate(parent) if p >= 0] + [None]:
        cuts_checked += 1
        for source in range(10):
            checked, _, _ = verify_partition_snapshot_query(base, prepare_source_partition_view(base, cut, source))
            queries, paths = queries + 1, paths + checked
        assert frozen == (base["raw"], tuple((g, tuple(row)) for g, row in sorted(base["rows"].items())))
print("bounded_partitions", dict(cuts=cuts_checked, queries=queries, paths=paths), "mismatches=0")
```
<!-- CUT-PARTITION-PROBE-END -->

### Execution Receipt

The extraction command ran with Python 3 on 2026-09-21 and exited successfully. No timing or RAM result is recorded.

| Check | Result |
| --- | --- |
| Nested common-tail ports | `a=32`, `A=65`: 2,080 overlapping record/head incidences versus 65 canonical head reports in one nonempty interval. |
| All-catalog negative | `a=A=32`: 1,024 all-head probes versus 32 useful colored pair queries. |
| Detached root hole | Upper row stays empty although the new root has no incoming live link. |
| Whole-subtree RMQ mutation | Parent interval selects cost 5; the inadmissible child-hole minimum has cost 0 and is rejected. |
| Repeated head in separate pieces | Two intervals produce two reports; old pinned distance 0 uses the forest identity, current distance 1 excludes it. |
| Promoted source above the cut | A revived arc is examined once, not again when its old owner settles through the zero-cost return. |
| Targeted arbitrary sources | 32 source/version queries and 188 returned-path checks, including a deleted old crossing connector and a parallel revival. |
| Seeded partition suite | 323 cut/uncut versions, 3,230 source queries, 19,550 returned-path checks; zero mismatches. |

The two source suites total **3,262 queries and 19,738 returned-path checks**, excluding the small counting/mutation fixtures. Every verified query checks disjoint record coverage, actual-color enumeration, clipped pair minima, the `Z_Q<=W_Q<=A_live` injection, one examination per revived record, canonical lexicographic labels, raw-graph distances and simple original-edge output. All original vertices are tried as sources in each source suite. The pinned-base replay also checks that raw and ledger rows were not changed.

This bounded evidence supports the interface and counting argument; it is not an exhaustive graph proof, sparse-publication implementation, concurrency/crash test, identity-reuse test, physical-I/O measurement or historical priority result. It ends the sidecar at the concrete incidence separation above.
