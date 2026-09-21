# Paths Forest Cut Repair

Date: 2026-09-21. Independent A03 follow-up. Only this new file is owned. The previous [update/query note](Paths-Update-Query-Separation.md) is frozen and unchanged. No production edits, shared journal/portfolio edits, public timings, or commits.

## Result First

**Affirmative, with a paid revival fanout:** one deleted original zero-forest edge admits an exact lazy macro-quotient without rewriting vertex owners, moving affected ledger records, or materializing all changed pair minima. There is a useful graph-specific locality fact: **old quotient witness invalidation is confined to at most one old capsule row**, even when arbitrarily many pair winners in that row fail. Newly exposed raw arcs must still be discovered; their distinct previously nongate heads become entry ports.

Let `r` be the number of previously forest-dominated raw arcs revived by the deletion, and `a` the number of added ports, including a new forest root when needed. Then `a <= r+1`. With the paid indexes specified below, publication takes `O(log(D+2)+(r+1)log(r+2))` word-RAM work, adds `O(r+a+1)` retained words, and need not enumerate the `mu` old ledger records whose canonical owners change or the `phi` broken saved pair witnesses. A query needs at most `q+a+1` mutable distance states, not one per original vertex or per affected pair. Row reads, range probes, revival edges, and original-edge output remain charged separately.

The construction preserves exact original distances and supplies a **simple original-edge shortest path**, including under zero-cost cycles, parallel alternatives and dormant returns. It does not claim constant total work for a one-edge batch, arbitrary subsequent edits, or a physical 4 GB implementation.

This is concrete progress beyond a cut/link data structure: it specifies which original arcs must revive, which old quotient rows stay valid, why old macro arcs may bypass new ports, how arbitrary source domains change, and how every output is lifted. Optimized equal-information dynamic-tree/range-min controls can implement the same locality; no separation from that strongest constructed control or historical priority for the complete construction is established. That does not erase the representation theorem or its avoided materialization costs.

## Scope And Paid Base

Read the A03 [base manuscript](Certified-Paths.md), the previous note's frozen contract/next question, and the already reviewed source-seeded/elimination premises. This is not a repository resurvey. Analytical lenses: graph entry closure, dependency accounting, and adversarial witness reconstruction; not independent reviewers.

Start from one exact immutable raw snapshot `G0`, feasible potential `h`, selected directed acyclic zero-reduced-cost forest `F`, and the original capsule preparation. Delete exactly one original forest identity `f=(u,b)`. All other original arc identities, endpoints and costs remain unchanged. Reduced costs `c(x,y)=w(x,y)+h(x)-h(y)` remain nonnegative automatically under deletion. Arithmetic is checked exact integer/fixed-point arithmetic; infinity and absence are separate statuses. Original weights are also nonnegative in the checker. No proof covers an accompanying reweight, insertion, second cut, changed projection, or invalid potential.

The paid base contains:

- All original arcs and identities/incarnations, not just the retained quotient. No original parallel alternative is lost without its own justified recoverable representation.
- Original forest parents/edge identities, depth, preorder/subtree end, feasible `h`, old gates `P`, and old owner `pi`; `q=|P|`.
- `X`: all non-forest arcs not dominated by an original directed `F` path. Old gates contain all roots and all heads of `X`.
- `L`: the complete tail-sensitive ledger, including `X`, dormant self-arcs, and original `F` connectors crossing old capsules. `A=|L|`. Rows are ordered by `(tail preorder, stable ID)` within old owner.
- `D0`: every omitted non-forest raw arc whose original tail is an `F` ancestor of its head, including parallel alternatives to a selected forest edge. `D=|D0|`. Loops may be omitted permanently; including them in `D0` is harmless.
- A paid reporting index on `D0`, per-pair tail-range minima on `L`, old row head directories, and original-edge witness lookup. Raw/extraction, `h/F/pi`, all joins/sorts and index construction count before the first cut.

Use fixed original preorder coordinates; the cut does not renumber vertices. Quotient witnesses have a **single original in-capsule forest prefix followed by one ledger edge**, not arbitrary recursively unpacked CH shortcuts. The one-row result depends on this form.

## First Falsification: One Receipt, Unbounded Fanout

For arbitrary `K,M >= 1`, let `g` have forest children `b` and `x`; `b` has `K*M` leaves `l_(i,j)`. There are separate gate roots `p_i`. All forest reduced costs are zero. Add:

```text
l_(i,j) -> p_i : cost 0, with a parallel cost-1 alternative
x         -> p_i : cost 2
l_(0,0)   -> g   : cost 1    (dormant old quotient self-arc).
```

Delete only `g->b`. Every old saved winner `(g,p_i)` used a prefix containing the failed edge. The exact new distance from `g` to every `p_i` is 2, not 0; from `b` it remains 0. The return now gives a real `b->g` transition and makes upper-side alternatives available to sources in the cut side. Discarding the old dormant record breaks that behavior.

Here there are no revived omitted raw arcs: `r=0`, and only one added port `b`. Nevertheless `phi=K` nonself saved pair winners are invalid, `mu=2KM+1` old ledger records change canonical owner, and `KM+1` original vertices change owner. Any algorithm actually rewriting those objects pays their output size, not `O(batch)=O(1)`. Even the parallel alternative at the **same lower tail** still has a broken prefix from `g`; only the upper-tail alternative repairs that pair.

This falsifies a materialized-update bound, not an unconditional dynamic-path lower bound. A lazy representation can avoid writing those objects, as the theorem below does. The retained checker constructs this family first and verifies changed distances, the dormant return, and all broken original prefixes.

## Two Designs

### Design 1: Split Only The Existing Ledger

Add `b` as a fragment gate, filter the old `g` row by which side of the cut contains the tail, retain all old heads, and ignore omitted raw arcs. This is exact **only if no omitted arc revives**, a condition requiring paid verification. It handles the fanout family, but is not a general solution for the original graph.

Counterexample: forest `g->b`, `b->v`, `b->w`, all cost zero, and raw arc `g->v` of reduced cost 2. The raw arc was dominated and absent from `L`. After deleting `g->b`, `v` is reachable from `g` at cost 2; `b` and sibling `w` are unreachable. Ignoring the raw arc loses `v`. Redirecting its head to the new fragment root `b` invents reachability of both `b` and `w`. A port at the actual head `v`, or an equivalent exact entry representation, is necessary for this contraction scheme.

**Disposition:** retain as the certified `r=0` specialization, not the general design.

### Design 2: Revival-Closed Lazy Macro Quotient

First discover revived raw arcs and close the new entry set. Keep valid old macro arcs even when their forest prefixes pass through newly added ports. Such bypasses are real paths; a strict canonical quotient is unnecessary. This avoids invalidating many otherwise valid rows merely because a new head was marked.

The exact data and invariants follow. No third design is explored.

## Revival And One-Row Lemmas

### Exactly Which Raw Arcs Revive

For an omitted arc `e=(x,y)` in `D0`, its original forest witness crosses `f=(u,b)` exactly when

```text
tin(b) <= tin(y) < tout(b)  AND  depth(x) < depth(b).
```

Proof: `x` is already an ancestor of `y`. If `y` is below `b`, its ancestors form one chain, so an ancestor of smaller depth than `b` lies above the cut. Conversely a path crossing the cut has these two properties. Distinct trees cannot give false positives because membership in `D0` established ancestry in the same tree.

Let `R` be this reported set. All its tails lie on the ancestor chain ending at `u`; all heads lie below `b`. Remaining arcs in `D0-R` still have live zero-cost forest witnesses, so `G0-f` has the same shortest distances as

```text
G_ret = (F-f) union X union R.
```

Use a static three-sided reporting index on `(tin(head), depth(tail))`, breaking tied head coordinates by ID and including the entire tied group at query endpoints. [McCreight's priority search trees](https://epubs.siam.org/doi/10.1137/0214021) support `O(D)` space and `O(log(D+2)+r)` reporting work; a comparison-based static build can be charged `O(D log(D+2))`. This is an imported geometric operation, not a new index. Alternatively scan all `D0`, charging `Theta(D)` reads. A missing revival index is not a free small-update result.

Let `N = ({b} union heads(R)) - P` and `a=|N|`. These are genuine entry ports at original vertices. `P*=P union N` is entry-closed for `G_ret`, and contains every root of `F-f`. In particular `a<=r+1`. A single cut can have `r=Theta(m)` and `a=Theta(n)`; no uniform small-state claim is made in that case.

### At Most One Bad Old Row

Let `g=pi(u)`. If `b` was not an old gate, then `pi(b)=g` and every old in-capsule witness prefix that crosses `f` starts at `g`. Its original ledger tail is in `subtree_F(b)` and old capsule `g`. Thus the **only** old row needing prefix restriction is row `g`.

If `b` was already a gate, no old in-capsule prefix contains `f`: it was itself a crossing ledger connector. Only its own identity must be excluded from row `g`; a parallel connector can remain valid. Again, at most one old-`L` row needs restriction. Revived arcs can add contributions to several old rows; the lemma does not say those additions are confined to `g`.

New ports do not invalidate the other saved witnesses. A prefix passing through a newly marked head is still a live original forest path. This assertion is about validity of actual macro arcs, not preservation of a literal canonical partition.

For any new port `v`, its useful old-ledger tails are the one interval

```text
J_v = {x: pi(x)=pi(v), v is an original F ancestor of x}.
```

The path from `v` downward never crosses the cut, since every new port lies at/below `b`. Old `g`'s surviving row is at most two intervals (subtract the cut-subtree block, or the single deleted connector's **record rank**). Every other old row is unchanged. This is the source of the compact row interface; it is not a claim that every affected record or pair remains valid.

## Exact Macro Representation

Build a port forest within each old capsule: for each `v in N`, find its nearest strict ancestor in `P*` reachable by `F-f`; add a zero-reduced-cost macro from that ancestor to `v`. There is none into a newly created root `b`. The macro stores its two endpoints; its lift is the actual forest path. Sorting the `a` ports by old capsule and preorder and using a laminar stack constructs these links in `O(a log(a+2))` work, without visiting all descendants.

The query graph `K_f` is implicit:

1. **Old rows:** retain old pair minima, except row `g`, which is evaluated over its surviving one/two intervals. A pair minimum carries the actual original edge and tail. A failed winner is replaced by the best eligible parallel record, not by its stale cost or by the next cost-sorted record with another broken prefix.
2. **New-port rows:** for each `v in N`, use pair minima from `J_v` to the old heads `P`. These are zero forest prefixes from `v`, then one original ledger edge. Dormant old self-arcs participate normally and can now leave the new port.
3. **Revived raw arcs:** add a macro `pi(x)->y` for each `(x,y) in R`, using the intact old forest prefix `pi(x)->x` and this original edge. Its tail is above the cut, so no new port lies on that prefix. Group these records by old tail owner; do not expand a cross product with outgoing ledger records.
4. **Port links:** include the port-forest macros described above.

Do not split and rewrite all old rows at every new port. Old rows and new-port rows may overlap; every overlapping macro still has a valid lift. Account for repeated query probes rather than asserting that overlap is free.

### Arbitrary Source And Target Domains

If source `s` is not in `P*`, add it as one query-local state. Its original same-capsule descendant ledger range is clipped by the cut exactly when its prefix crosses `f`; this leaves at most two intervals. Its row uses that range and any revived arcs whose tails are reachable from `s` before entering an old gate. The latter can occur only when `s` is above the cut. Give `s` zero macros to the new ports reachable within its old capsule and an optional incoming zero macro from its current nearest gate. Existing port links and valid old bypass macros need not be rewritten for source promotion.

Define `rho_s(v)` as the deepest ancestor of `v` in `P* union {s}` connected to `v` by `F-f`. This is always defined. Then

```text
dist_(G0-f)(s,v) = D_K(rho_s(v)) + h(v) - h(s).
```

To locate `rho`, retain only the new-port interval events grouped by **old** capsule. A stack sweep yields `O(a+1)` additional owner-breakpoint records. Look up old `pi(v)` first, then the deepest new mark at its preorder; apply a source override only when it is deeper and has an intact prefix. This avoids an `n`-element new owner plane. Streaming full output still reads all requested original vertices and costs at least its output size.

### Exactness Proof

Every macro is an original path in `G_ret`, so no macro answer can be shorter than the corresponding original shortest distance. For the other inequality consider the ordinary, fully materialized entry-closed quotient of `G_ret`, promoted at `s` if necessary.

- A canonical ledger edge with an old gate as owner is covered by that old row; the cut restriction excludes only tails whose prefix is actually unavailable.
- A canonical ledger edge with a new port/source as owner is covered by that port/source's descendant interval. Its old head remains in `P`.
- A revived edge has its actual new/old head in `P*`; its tail is above the cut and is handled by its old gate or by the source's special row.
- A surviving forest edge into a new port is represented by the port links, with source links when source promotion intervenes. Edges into old gates were already original ledger connectors.

Taking a minimum over an even larger valid row may only improve each simulated canonical transition. Therefore every canonical path has a no-longer macro route, while every macro route lifts. Equality follows. Head closure also proves the target formula: after its last entry, a path to a nongate follows forest edges, and every gate can reach its current domain at zero reduced cost.

The new property is the **one-old-row plus revival-head interface**, including valid bypass reuse. Ordinary vertex elimination supplies the underlying quotient exactness; it does not by itself specify this failure dependency/update representation.

## Simple Original-Edge Output

Overlapping macros can otherwise produce zero-cost expanded walks with repeated vertices. To obtain simple paths without a large cycle-erasure map, run lexicographic Dijkstra on

```text
(total reduced cost, total number of original edges).
```

A ledger macro from entry `v` through original tail `x` has secondary length `depth(x)-depth(v)+1`; a port link has secondary length `depth(port)-depth(parent_port)`. These are positive. In each pair-range index use priority `(c(e), depth(tail(e)), stable_ID)`. Subtracting the entry's constant depth gives the exact secondary ordering for that row. This needs a paid tie-aware base index/cache; an old cost/ID-only winner is not silently assumed to minimize expanded hops. Grant the same preparation to controls.

The simulation proof above holds lexicographically **within `G_ret`** when pair minima use this ordering. Omitted zero-cost raw shortcuts may have fewer hops than their forest replacement, so no minimum-hop claim for the full raw graph is made. Primary distances remain exact for it. A repeated original vertex on a minimum-cost/minimum-hop retained walk would contain a nonnegative cycle: a positive-cost cycle contradicts minimum cost, and a zero-cost cycle contradicts minimum hops. Hence the delivered expanded path is simple.

Store one predecessor macro descriptor per gate state, not a copy of its whole expanded path. Recover its forest prefix and original edge/incarnation from the pinned snapshot, and append the final `rho_s(v)->v` forest path. Output verifies membership, endpoints, incidence continuity and cost. An equal-weight parallel replacement returns its own original ID. The deleted forest identity cannot appear in any lift. For forward-order delivery, charge a bounded external stack/spool for reversing predecessor chains and parent paths; output length and original-ID lookups are not free. The tiny checker keeps full paths only as an oracle convenience.

## Dependency And Resource Ledger

Keep these parameters distinct:

| Parameter | Meaning and consequence |
| --- | --- |
| `r` | Reported raw revival records, possibly large; this design pays them during publication. |
| `a` | New port count, at most `r+1`; controls additional distance states. |
| `mu` | Old ledger records that would move in the canonical refined ownership; not materialized by this design. |
| `phi` | Saved nonself pair winners whose original lift actually contains the deleted edge; no publication-time scan/update required. |
| `H_Q` | Per-head interval-min oracle calls actually issued for bad/new/source rows in a query. |
| `C_Q` | Saved old pair headers read from valid cached rows. |
| `R_Q` | Revival records examined by a query, at most `2r` with grouping by old owner and one additional source-range pass. |

### Concrete Index And Work Bounds

Store each old `(owner,head)` catalog in tail-rank order, with an ordinary static linear-space range-min structure, or a balanced aggregate tree with `O(log(A+2))` queries. There are only `O(A)` catalog occurrences: each record belongs to **one** pair catalog, not every forest edge on its prefix. Include tail-range endpoint search, values, IDs and depth tie keys. Total extra pair-index storage can be `O(A)` words, construction `O(A log(A+2))`; no per-failure pair table is built. The baseline range-min machinery is established, not claimed new.

A simple row iterator first obtains its surviving intervals; if none remain it stops without scanning the head directory. Otherwise it scans that paid directory and makes one/two range queries per head; empty answers are allowed. Thus `H_Q` is explicit, and new ports in a high-degree old capsule can be expensive. A paid colored range-reporting index may replace this with actual-head enumeration, but that is not needed for the claimed bounds and must be granted to the control as well. Never call a whole row's head enumeration one RMQ.

With the linear-space revival reporting index, publication costs

```text
O(log(D+2) + (r+1)log(r+2)) CPU,
O(r+a+1) new retained words, zero rewritten old ledger/pair/owner entries.
```

This includes sorting/grouping `R`, deduplicating new heads, port links and owner breakpoints. It assumes direct access to the paid immutable coordinate/ID rows; external joins or random page reads replace those unit costs on disk. If `r` exceeds a resident buffer, price external sorting and streaming. No `O(1)` claim survives arbitrary revival fanout.

Let `E_Q` be the total yielded macro relaxations, including old/new row minima, revived arcs, port links and source links. With an indexed heap, a conservative query bound is

```text
O((q+a+1+E_Q) log(q+a+2)
  + H_Q log(A+2) + C_Q + R_Q + a_s + endpoint_lookups)
plus original output/path delivery.
```

`a_s<=a` counts source-reachable new ports enumerated for source links. Additional mutable solver state is `O(q+a+1)` words; descriptors/breakpoints are `O(a+1)`, revival records may stay on disk, and bounded caches/index traversal/output buffers are separate. This avoids `O(mu+phi)` *materialized repair* state, not the `O(n+m)` immutable raw source and metadata. On a bad workload `H_Q` can be `Theta(a * degree(old_capsule))`; no universal read reduction is proved.

The useful specialization is `r=0`: one cut descriptor and at most one added state repair a cut whose `mu` and `phi` are arbitrarily large. Current `(g,p)` lookup is at most two tail-range queries regardless of how many invalid low-cost parallel candidates lie below the cut. That is a concrete avoided record-read/materialization cost against scanning those candidates or rewriting their memberships. A well-indexed equal-information lazy control can match it.

### Versions And Low-RAM Admission

Publish a manifest binding raw snapshot plus the forest tombstone, unchanged base indexes, revival stream, port links and ownership breakpoints. It identifies an **implicit current** quotient, not a stale old `Q` presented as current. Pin it through source lookup, solving and path delivery. A pinned older manifest may use `f`; a new one may not. Recovery publishes nothing until the reporting/grouping closure is complete. Incarnations protect identity reuse, although no restoration is admitted within this one-cut update.

Old manifests share immutable pages; each distinct cut version pays its own `O(r+a+1)` overlay and retained output. Building a new base or applying additional cuts requires a separately priced procedure. No generic copy-on-write guarantee hides a full raw-copy or full index rebuild.

The provisional host budget must include source/runtime, `q+a` solver state, reporting/index workspace, sorting/cache buffers, pinned-reader state and output buffering. The 50 GB prepared allowance is the **shared seven-family union**, including the `O(D)` revival index, raw edges and all pinned versions. `D` can approach `m`, so even the linear-space added index may be unaffordable. Use a charged raw scan or decline this profile; a resident RAM theorem is not a measured 4 GB or external-I/O result.

## Equal-Information Controls And Prior Art

| Control / primary source | Exact comparison boundary |
| --- | --- |
| [Sleator and Tarjan, A Data Structure for Dynamic Trees](https://www.sciencedirect.com/science/article/pii/0022000083900065) | Cut/link/ancestry maintenance is established. Give the control the same full raw coverage, revival index, tail coordinates and pair catalogs. An eager control that rewrites `mu` members or `phi` winners pays those writes; a lazy prefix-validity plus range-min control can match the one-row specialization. The logarithmic tree cut does not include discovery of revived original arcs. |
| [Lueker, dynamic range queries, printed pp. 3-7](https://escholarship.org/content/qt9k40c7jc/qt9k40c7jc.pdf), and [McCreight, priority search trees](https://epubs.siam.org/doi/10.1137/0214021) | Range minima and three-sided reporting are imported operations. The graph facts are the revival coordinates and one-row witness locality, not a renamed search tree. A control gets exactly the same factors and witnesses. |
| [Dibbelt, Strasser, Wagner, CCH, Section 7.7](https://ben-strasser.net/paper/customizable_contraction_hierarchies_arxiv_preprint.pdf) | Partial updates propagate affected metric changes rather than rebuilding every shortcut. A deletion may be represented by infinity when the retained contracted topology permits it; pay actual propagation and valid unpacking. Newly exposed entry heads in this forest-specific representation are not automatically handled by a fixed pruned skeleton. Its optional stale-answer queue smoothing is disallowed here. No general speedup over partial CCH is proved. |
| [Demetrescu, Thorup, Chowdhury, Ramachandran, single-failure distance oracles](https://www3.cs.stonybrook.edu/~rezaul/papers/oracles.html) | This is an established exact failure-query problem, including path recovery, not an invented application. Dense all-pairs prepared oracles are a different space/preparation tradeoff; they must not be excluded if they fit the same admitted budget. No lower bound against sensitivity oracles is asserted. |

The publisher abstracts and primary report text were inspected; CCH's actual update procedure was read. The distance-oracle authors' HTML abstract explicitly confirms exact failure queries and output-proportional path recovery; a subsequent PDF full-text read timed out, so no sharper theorem from that failed read is used. The priority-search-tree PDF was scanned/image-only in this interface, so its publisher's explicit operation-(4) bound supplies the reporting attribution. The failed fault-tolerant-reachability endpoint is not counted as evidence. This is bounded precedent checking, not an exhaustive priority search.

**Claim status:** exact representation/output theorem and an explicit fanout-compressed update are affirmative. The strongest ordinary lazy control remains competitive. Matching it is not proof that the complete graph composition was previously published, and primitive similarity is not a reason to discard the graph lemma. Conversely, this note does not promote the construction to the missing seven-family novelty result without a stronger comparison or useful end-to-end evidence.

## Retained Bounded Checker

The single self-contained snippet below checks the graph interface, not storage-engine performance. Its range-min and revival-report oracles use linear scans deliberately; counters count abstract range calls, not those scans as a fast implementation. Its full path tuples, full reference owner maps and duplicate-entry Python heap are not the theorem's compact predecessor/indexed-heap implementation. It compares the lazy macro solver with both original-graph Bellman-Ford and a separately materialized, entry-closed current quotient. Returned paths are checked for original ID/incarnation, adjacency, cost, absence of the failed edge, and no repeated vertex.

The mutation checks precede the suites. Exhaustive small raw-edge choices and seeded branching forests include internal/cross-capsule cuts, revived nongate heads, parallel forest alternatives, loops, disconnected roots, dormant returns, positive potentials, zero reduced cycles and source promotion. Old and cut manifests share an unchanged base and are replayed independently; this is not an arbitrary multicut sequence or crash test.

```sh
awk '/^<!-- FOREST-CUT-PROBE-START -->/{p=1;next} /^<!-- FOREST-CUT-PROBE-END -->/{p=0} p && !/^```/' research_algorithms_20260920/Paths-Forest-Cut-Repair.md | python3 -
```

<!-- FOREST-CUT-PROBE-START -->
```python
from bisect import bisect_left
from collections import defaultdict
from heapq import heappop, heappush
from itertools import product
from math import inf
from random import Random


def derive_forest_ancestry_tables(parent):
    children = [[] for _ in parent]
    for vertex, ancestor in enumerate(parent):
        if ancestor >= 0:
            children[ancestor].append(vertex)
    tin, end, depth, order = {}, {}, {}, []
    def visit_original_forest_vertex(vertex, level):
        tin[vertex], depth[vertex] = len(order), level
        order.append(vertex)
        for child in children[vertex]:
            visit_original_forest_vertex(child, level + 1)
        end[vertex] = len(order)
    for vertex, ancestor in enumerate(parent):
        if ancestor < 0:
            visit_original_forest_vertex(vertex, 0)
    return tin, end, depth, order


def test_original_tree_ancestry(base, ancestor, vertex):
    return base["tin"][ancestor] <= base["tin"][vertex] < base["end"][ancestor]


def recover_original_forest_prefix(base, ancestor, vertex, cut):
    path = []
    while vertex != ancestor:
        if vertex < 0 or base["parent"][vertex] < 0 or vertex == cut:
            return None
        path.append((10000 + vertex, 0))
        vertex = base["parent"][vertex]
    return tuple(reversed(path))


def prepare_base_path_snapshot(parent, extras, positive=False):
    tin, end, depth, order = derive_forest_ancestry_tables(parent)
    potential = [depth[v] if positive else 0 for v in range(len(parent))]
    # Arc record: (ID, tail, head, reduced cost, incarnation).
    forest = [(10000 + v, p, v, 0, 0) for v, p in enumerate(parent) if p >= 0]
    raw = list(forest) + [(i, u, v, c + max(0, potential[u] - potential[v]), 0)
                           for i, (u, v, c) in enumerate(extras)]
    base = dict(parent=tuple(parent), tin=tin, end=end, depth=depth,
                order=order, potential=potential, raw=tuple(raw), forest=tuple(forest))
    omitted = [e for e in raw if e[0] < 10000 and test_original_tree_ancestry(base, e[1], e[2])]
    exceptions = [e for e in raw if e[0] < 10000 and e not in omitted]
    gates = {v for v, p in enumerate(parent) if p < 0} | {e[2] for e in exceptions}
    owner = {}
    for vertex in order:
        owner[vertex] = vertex if vertex in gates else owner[parent[vertex]]
    ledger = exceptions + [e for e in forest if owner[e[1]] != owner[e[2]]]
    rows, cached = defaultdict(list), defaultdict(dict)
    for edge in ledger:
        rows[owner[edge[1]]].append(edge)
    for gate in gates:
        rows[gate].sort(key=lambda e: (tin[e[1]], e[0]))
        cached[gate] = {}
        for edge in rows[gate]:
            head = edge[2]
            key = (edge[3], depth[edge[1]], edge[0])
            previous = cached[gate].get(head)
            if previous is None or key < (previous[3], depth[previous[1]], previous[0]):
                cached[gate][head] = edge
    base.update(omitted=tuple(omitted), exceptions=tuple(exceptions), gates=frozenset(gates),
                owner=owner, ledger=tuple(ledger), rows=dict(rows), cached=dict(cached))
    return base


def prepare_cut_port_snapshot(base, cut):
    if cut is None:
        return dict(cut=None, revived=(), revival_rows={}, ports=frozenset(), bad=None, links={})
    assert base["parent"][cut] >= 0
    revived = tuple(e for e in base["omitted"]
                    if test_original_tree_ancestry(base, cut, e[2])
                    and base["depth"][e[1]] < base["depth"][cut])
    direct = tuple(e for e in base["omitted"]
                   if recover_original_forest_prefix(base, e[1], e[2], cut) is None)
    assert revived == direct
    revival_rows = defaultdict(list)
    for edge in revived:
        revival_rows[base["owner"][edge[1]]].append(edge)
    ports = ({cut} | {e[2] for e in revived}) - base["gates"]
    states = base["gates"] | ports
    links = defaultdict(list)
    for port in ports:
        ancestors = [v for v in states if v != port
                     and recover_original_forest_prefix(base, v, port, cut) is not None]
        if ancestors:
            entry = max(ancestors, key=base["depth"].get)
            links[entry].append(port)
    bad = base["owner"][base["parent"][cut]]
    for gate, row in base["cached"].items():
        if gate != bad:
            for edge in row.values():
                assert edge[0] != 10000 + cut
                assert recover_original_forest_prefix(base, gate, edge[1], cut) is not None
    assert len(ports) <= len(revived) + 1
    return dict(cut=cut, revived=revived, revival_rows=dict(revival_rows),
                ports=frozenset(ports), bad=bad, links=dict(links))


def locate_current_entry_owner(base, snap, states, vertex):
    choices = [gate for gate in states
               if recover_original_forest_prefix(base, gate, vertex, snap["cut"]) is not None]
    return max(choices, key=base["depth"].get)


def derive_valid_row_intervals(base, snap, entry):
    row = base["rows"][base["owner"][entry]]
    keys = [base["tin"][e[1]] for e in row]
    lo = bisect_left(keys, base["tin"][entry])
    hi = bisect_left(keys, base["end"][entry])
    forbidden = None
    cut = snap["cut"]
    if cut is not None:
        if cut not in base["gates"] and test_original_tree_ancestry(base, entry, base["parent"][cut]):
            forbidden = (bisect_left(keys, base["tin"][cut]),
                         bisect_left(keys, base["end"][cut]))
        else:
            positions = [i for i, e in enumerate(row) if e[0] == 10000 + cut]
            if positions:
                forbidden = (positions[0], positions[0] + 1)
    if forbidden is None or forbidden[1] <= lo or hi <= forbidden[0]:
        return row, [(lo, hi)] if lo < hi else []
    left, right = forbidden
    return row, [(a, b) for a, b in ((lo, min(hi, left)), (max(lo, right), hi)) if a < b]


def generate_lazy_macro_neighbors(base, snap, source, entry, states, stats):
    cut, owner, depth = snap["cut"], base["owner"], base["depth"]
    if entry in base["gates"] and entry != snap["bad"]:
        records = list(base["cached"][entry].values())
        stats["cached"] += len(records)
    else:
        row, intervals = derive_valid_row_intervals(base, snap, entry)
        records = []
        for head in sorted(base["cached"][owner[entry]]) if intervals else ():
            minima = []
            for lo, hi in intervals:
                stats["rmq"] += 1
                points = [edge for edge in row[lo:hi] if edge[2] == head]
                if points:
                    minima.append(min(points, key=lambda e: (e[3], depth[e[1]], e[0])))
            if minima:
                records.append(min(minima, key=lambda e: (e[3], depth[e[1]], e[0])))
    for edge in records:
        prefix = recover_original_forest_prefix(base, entry, edge[1], cut)
        assert prefix is not None and edge[0] != (10000 + cut if cut is not None else -1)
        path = prefix + ((edge[0], edge[4]),)
        yield edge[2], edge[3], len(path), path
    inspect_revivals = entry in base["gates"] or (
        entry == source and cut is not None
        and test_original_tree_ancestry(base, source, base["parent"][cut]))
    if inspect_revivals:
        for edge in snap["revival_rows"].get(owner[entry], ()):
            stats["revival"] += 1
            prefix = recover_original_forest_prefix(base, entry, edge[1], cut)
            if prefix is not None:
                path = prefix + ((edge[0], edge[4]),)
                yield edge[2], edge[3], len(path), path
    for port in snap["links"].get(entry, ()):
        prefix = recover_original_forest_prefix(base, entry, port, cut)
        assert prefix
        yield port, 0, len(prefix), prefix
    original_states = base["gates"] | snap["ports"]
    if source not in original_states:
        if entry == source:
            for port in snap["ports"]:
                if owner[port] == owner[source]:
                    prefix = recover_original_forest_prefix(base, source, port, cut)
                    if prefix is not None:
                        yield port, 0, len(prefix), prefix
        elif entry == locate_current_entry_owner(base, snap, original_states, source):
            prefix = recover_original_forest_prefix(base, entry, source, cut)
            yield source, 0, len(prefix), prefix


def solve_lazy_macro_distances(base, snap, source, hop_ties=True):
    states = base["gates"] | snap["ports"] | {source}
    distances = {v: (inf, inf) for v in states}
    paths, queue, stats = {source: ()}, [(0, 0, source)], dict(rmq=0, cached=0, revival=0)
    distances[source] = (0, 0)
    while queue:
        cost, hops, tail = heappop(queue)
        if (cost, hops) != distances[tail]:
            continue
        for head, weight, count, path in generate_lazy_macro_neighbors(base, snap, source, tail, states, stats):
            candidate = (cost + weight, hops + count if hop_ties else 0)
            if candidate < distances[head]:
                distances[head], paths[head] = candidate, paths[tail] + path
                heappush(queue, (*candidate, head))
    assert stats["revival"] <= 2 * len(snap["revived"])
    return states, distances, paths, stats


def solve_materialized_quotient_distances(base, snap, source, states):
    owners = {v: locate_current_entry_owner(base, snap, states, v) for v in base["order"]}
    adjacency = defaultdict(list)
    for edge in base["raw"]:
        identity, tail, head, cost, _ = edge
        if snap["cut"] is not None and identity == 10000 + snap["cut"]:
            continue
        prefix = recover_original_forest_prefix(base, tail, head, snap["cut"])
        if identity < 10000 and prefix is not None:
            continue
        if identity >= 10000 and owners[tail] == owners[head]:
            continue
        assert head in states
        entry = owners[tail]
        adjacency[entry].append((head, cost, base["depth"][tail] - base["depth"][entry] + 1))
    distances = {v: (inf, inf) for v in states}
    distances[source], queue = (0, 0), [(0, 0, source)]
    while queue:
        cost, hops, tail = heappop(queue)
        if (cost, hops) != distances[tail]:
            continue
        for head, weight, count in adjacency[tail]:
            candidate = (cost + weight, hops + count)
            if candidate < distances[head]:
                distances[head] = candidate
                heappush(queue, (*candidate, head))
    return distances


def solve_original_snapshot_distances(base, snap, source):
    distances = [inf] * len(base["parent"])
    distances[source] = 0
    h = base["potential"]
    for _ in range(len(distances) - 1):
        changed = False
        for identity, tail, head, cost, _ in base["raw"]:
            if snap["cut"] is not None and identity == 10000 + snap["cut"]:
                continue
            weight = cost - h[tail] + h[head]
            assert weight >= 0
            if distances[tail] + weight < distances[head]:
                distances[head], changed = distances[tail] + weight, True
        if not changed:
            break
    return distances


def verify_cut_snapshot_query(base, snap, source):
    states, distances, paths, stats = solve_lazy_macro_distances(base, snap, source)
    assert distances == solve_materialized_quotient_distances(base, snap, source, states)
    oracle = solve_original_snapshot_distances(base, snap, source)
    edges = {e[0]: e for e in base["raw"]}
    h, checked = base["potential"], 0
    for target in base["order"]:
        gate = locate_current_entry_owner(base, snap, states, target)
        cost, hops = distances[gate]
        assert cost + h[target] - h[source] == oracle[target]
        if cost == inf:
            continue
        prefix = recover_original_forest_prefix(base, gate, target, snap["cut"])
        path = paths[gate] + prefix
        vertex, weight, seen = source, 0, {source}
        for identity, token in path:
            _, tail, head, reduced, incarnation = edges[identity]
            assert token == incarnation and tail == vertex
            assert snap["cut"] is None or identity != 10000 + snap["cut"]
            assert head not in seen
            seen.add(head)
            vertex, weight = head, weight + reduced - h[tail] + h[head]
        assert vertex == target and weight == oracle[target]
        assert len(path) == hops + len(prefix)
        checked += 1
    return checked, stats


def build_branching_fanout_fixture(heads, multiplicity):
    parent = [-1, 0, 0] + [1] * (heads * multiplicity) + [-1] * heads
    start = 3 + heads * multiplicity
    extras = [(2, start + i, 2) for i in range(heads)]
    for i in range(heads):
        for j in range(multiplicity):
            leaf = 3 + i * multiplicity + j
            extras.extend(((leaf, start + i, 0), (leaf, start + i, 1)))
    extras.append((3, 0, 1))
    return prepare_base_path_snapshot(parent, extras), start


# Regression: a terminal gate still has an explicit empty cached row.
base = prepare_base_path_snapshot([-1, -1], [(0, 1, 1)])
verify_cut_snapshot_query(base, prepare_cut_port_snapshot(base, None), 0)

# Falsify materialized O(batch), including parallel alternatives and a dormant return.
base, start = build_branching_fanout_fixture(32, 4)
snap = prepare_cut_port_snapshot(base, 1)
broken = [edge for head, edge in base["cached"][0].items() if head != 0
          and recover_original_forest_prefix(base, 0, edge[1], 1) is None]
moved = [edge for edge in base["ledger"] if test_original_tree_ancestry(base, 1, edge[1])]
assert len(broken) == 32 and len(moved) == 257
assert not snap["revived"] and snap["ports"] == {1}
assert all(solve_original_snapshot_distances(base, snap, 0)[p] == 2
           for p in range(start, start + 32))
assert solve_original_snapshot_distances(base, snap, 3)[0] == 1
for source in (0, 1, 2, 3, start):
    verify_cut_snapshot_query(base, snap, source)
print("one_receipt_fanout", dict(broken_pairs=len(broken), moved_records=len(moved),
      revived=0, new_ports=1), "naive_bound_falsified")

# Fixed head count: eligible range probes do not grow with invalid alternatives.
probe_counts = []
for multiplicity in (1, 8, 32):
    base, start = build_branching_fanout_fixture(2, multiplicity)
    snap = prepare_cut_port_snapshot(base, 1)
    _, stats = verify_cut_snapshot_query(base, snap, 0)
    moved = sum(test_original_tree_ancestry(base, 1, e[1]) for e in base["ledger"])
    assert moved == 4 * multiplicity + 1
    states = len(base["gates"] | snap["ports"])
    assert stats == dict(rmq=3, cached=0, revival=0) and states == 4
    probe_counts.append((multiplicity, moved, states, stats["rmq"]))
print("fixed_heads_probes", probe_counts, "fields=(multiplicity,moved,states,rmq_calls)")

# Splitting b only either loses v, or invents b/w when the raw head is collapsed.
base = prepare_base_path_snapshot([-1, 0, 1, 1], [(0, 2, 2)])
snap = prepare_cut_port_snapshot(base, 1)
oracle = solve_original_snapshot_distances(base, snap, 0)
assert oracle == [0, inf, 2, inf] and snap["ports"] == {1, 2}
verify_cut_snapshot_query(base, snap, 0)
print("revival_head_mutation lost_entry_or_invented_sibling_detected")

# A parallel original forest alternative must replace the deleted identity in output.
base = prepare_base_path_snapshot([-1, 0, 1], [(0, 1, 0)])
snap = prepare_cut_port_snapshot(base, 1)
_, distances, paths, _ = solve_lazy_macro_distances(base, snap, 0)
assert distances[1][0] == 0 and (0, 0) in paths[1] and (10001, 0) not in paths[1]
verify_cut_snapshot_query(base, snap, 0)
verify_cut_snapshot_query(base, prepare_cut_port_snapshot(base, None), 0)
print("parallel_recovery new_identity_and_pinned_old_snapshot_valid")

# A scalar-tie solver can return a cyclic expansion through bypassed ports.
base = prepare_base_path_snapshot([-1, 0, 1, 2, 3],
                                  [(0, 2, 0), (0, 3, 0), (0, 4, 0), (4, 0, 0)])
snap = prepare_cut_port_snapshot(base, 1)
_, _, unsafe_paths, _ = solve_lazy_macro_distances(base, snap, 2, hop_ties=False)
raw_heads = {edge[0]: edge[2] for edge in base["raw"]}
expanded_vertices = [2] + [raw_heads[identity] for identity, _ in unsafe_paths[4]]
assert len(expanded_vertices) > len(set(expanded_vertices))
for source in range(5):
    verify_cut_snapshot_query(base, snap, source)
print("scalar_tie_mutation cyclic_expansion_detected_lexicographic_output_valid")

# Independent fanout axis: many revived entry heads, no broken nonself old pair.
base = prepare_base_path_snapshot([-1, 0] + [1] * 16,
                                  [(0, v, v % 5) for v in range(2, 18)] + [(2, 0, 1)])
snap = prepare_cut_port_snapshot(base, 1)
assert len(snap["revived"]) == 16 and len(snap["ports"]) == 17
assert not [head for head in base["cached"][0] if head != 0]
for source in (0, 1, 2, 17):
    verify_cut_snapshot_query(base, snap, source)
print("revival_fanout", dict(revived=16, new_ports=17, broken_nonself_pairs=0))

snapshots, queries, path_checks = 0, 0, 0
max_ports, max_revival = 0, 0
parent = [-1, 0, 1, 1]
pairs = [(0, 2), (0, 1), (2, 0), (3, 2)]
for choices in product((None, 0, 2), repeat=len(pairs)):
    extras = [(u, v, cost) for (u, v), cost in zip(pairs, choices) if cost is not None]
    base = prepare_base_path_snapshot(parent, extras)
    for cut in (1, 2, 3, None):
        snap = prepare_cut_port_snapshot(base, cut)
        snapshots += 1
        for source in range(4):
            checked, _ = verify_cut_snapshot_query(base, snap, source)
            queries, path_checks = queries + 1, path_checks + checked
        max_ports = max(max_ports, len(snap["ports"]))
        max_revival = max(max_revival, len(snap["revived"]))
print("exhaustive_small", dict(snapshots=snapshots, queries=queries, paths=path_checks), "mismatches=0")

rng = Random(20260921)
random_snapshots, random_queries, random_paths = 0, 0, 0
for trial in range(64):
    parent = [-1] + [rng.randrange(v) if rng.randrange(5) else -1 for v in range(1, 9)]
    extras = [(rng.randrange(9), rng.randrange(9), rng.randrange(4)) for _ in range(18)]
    # Repeated endpoints deliberately preserve parallel original identities.
    extras += extras[:2]
    base = prepare_base_path_snapshot(parent, extras, positive=bool(trial % 2))
    manifest_list = [prepare_cut_port_snapshot(base, v) for v, p in enumerate(parent) if p >= 0]
    manifest_list.append(prepare_cut_port_snapshot(base, None))
    for snap in manifest_list:
        random_snapshots += 1
        for source in range(9):
            checked, _ = verify_cut_snapshot_query(base, snap, source)
            random_queries, random_paths = random_queries + 1, random_paths + checked
        max_ports = max(max_ports, len(snap["ports"]))
        max_revival = max(max_revival, len(snap["revived"]))
print("seeded_forests", dict(snapshots=random_snapshots, queries=random_queries,
      paths=random_paths, max_ports=max_ports, max_revival=max_revival), "mismatches=0")
```
<!-- FOREST-CUT-PROBE-END -->

### Execution Receipt

Ran the extraction command above with Python 3 on 2026-09-21; exit status 0. No elapsed-time or memory benchmark is claimed.

| Retained check | Observed result |
| --- | --- |
| One-receipt branching capsule | `phi=32`, `mu=257`, `r=0`, `a=1`; the materialized `O(batch)` claim fails. |
| Fixed two-head query, multiplicities `1,8,32` | Moved records `5,33,129`; always four gate states and three abstract range-min calls from the upper source. |
| Revival-head mutation | Correct distances `[0,infinity,2,infinity]`; ignoring the head loses an entry and root-collapsing invents sibling reachability. |
| Parallel recovery and pinned base | The cut path uses the parallel original identity; the old manifest remains valid with the original forest identity. |
| Scalar-only tie mutation | An expanded cyclic witness is detected; lexicographic recovery passes for every source in the fixture. |
| Independent revival fanout | `r=16`, `a=17`, no broken nonself old pair. |
| Exhaustive four-vertex suite | 324 snapshots, 1,296 source queries, 3,903 returned-path checks; zero mismatches. |
| Seeded nine-vertex suite | 484 snapshots, 4,356 source queries, 31,830 returned-path checks; zero mismatches. |

The two suites total **808 snapshots, 5,652 source queries and 35,733 returned-path checks**, excluding the targeted fixtures. The four-vertex suite exhausts the stated four extra-arc choices, not all four-vertex graphs; the seeded suite is not exhaustive. The suite-only maxima printed by the snippet exclude the targeted 17-port fixture. An initially missing empty cache row for a terminal gate was corrected and retained as the first regression check.

The fixed-head result isolates an avoided invalid-candidate scan: the paid index implementation probes ranges instead of reading every broken low-cost alternative. It does not give constant CPU time as `A` grows, eliminate immutable index storage, or beat the equally indexed lazy control. The scan-based oracle's physical reads do grow. The proof, not these bounded runs, supplies general correctness; the runs do not establish index implementation, concurrency/crash safety, external-memory bounds, or historical novelty.

## Next Decision

Preserve Design 2 as an affirmative exact representation candidate. The next bounded action is **not** another correctness resurvey: price one immutable tail-pair index plus the raw-revival index on a declared ledger with controlled `(r,a,mu,phi)`, and compare publication writes, current row-range probes and path-delivery reads against an ordinary lazy dynamic-tree/pair-min control using precisely those same indexes. No timing claim is needed for the first comparison.

Use both axes independently: many broken winners with `r=0`, and many revived nongate heads with small `phi`. Include repeated queries that reach many nested ports so overlapping macro rows are penalized. Stop if index/pinned bytes or `H_Q` erase the avoided materialization. A genuine next improvement would need to reduce that overlap or revival preparation under a stated graph restriction while preserving original-edge output; this note does not assert that improvement.
