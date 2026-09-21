# Paths Update Query Separation

Date: 2026-09-21. Independent, bounded A03 follow-up. This file alone is owned by this task. No production edits, shared-journal updates, public timings, commits, or seven-family completion claim.

## Finding First

**Small versioned batches permit a concrete static-storage/dynamic-query tradeoff, including deletions. They do not, by themselves, establish the missing graph-specific separation.**

The useful result is an additive bound: an immutable, paid orthogonal argmin index can evaluate the exact selected-route guard using at most `r + 2 + 2d` rectangle queries, plus a scan of the live patch. Here `r` is current cumulative route-drift/failure fragmentation and `d` counts shadowed base records in the queried source range. It is not `r*d`, and neither parameter is the latest batch size. No access-index rewrite is required between checkpoints. A linear-space static index is therefore a real control even when access records undergo small batches of deletions, reweights, and endpoint moves.

This closes a specific gap in the previous static comparator, which stopped at "rebuild or a separately proved dynamic layer." The layer and proof are given below. The primitive is geometric and the graph lift is the already reviewed access-closure theorem. Historical priority for this exact composition is not established; matching a constructed control is not a priority argument.

**Decision:** do not implement another B-bin variant as the next novelty experiment. Retain this deletion-aware static control. For a new graph result, the next specific question is repair of the source/core witness contract after a deleted zero-forest edge, not another encoding of the same fixed-domain range predicate. That question is specified at the end, not explored as a third candidate here.

## Premise Check

Read only the requested research neighborhood: [Contribution Audit, A03](Contribution-Audit.md), [Access Frontier](Paths-Access-Frontier.md), [Correlated Guard Study](Paths-Correlated-Guard-Study.md), the linked [independent review, R1 and AF1-AF4](Connectivity-Paths-Independent-Review.md), and the audit's [comparator policy](Novelty-Baseline-Evidence-Policy.md). No repository resurvey or old-probe replay.

Preserved premises: quotient elimination is established; the guard reduces to weighted rectangles; arbitrary independent slacks cannot have a constant-word self-contained exact summary; the constructed factored-transit control matches the earlier toy family. Neither additional passing checks nor a faster generic operator fills the seven-family innovation requirement.

Three analytical lenses are used, not represented as independent reviewers: exact graph witnesses, dynamic/static data structures, and adversarial full-lifecycle accounting.

## Contract And Workflow

The user supplies a source snapshot, not a ready geometric oracle. Pay source extraction, identity normalization, feasible `h`, intact zero-reduced-cost forest `F`, fixed gates `P` and ownership `pi`, complete tail-sensitive access ledger, and independently exact quotient `Q`. Pay its pair-minimum update index or the actual scans used instead. The following construction changes only source-access certification, not those costs.

Within a checkpoint epoch:

- `F`, `h`, `pi`, gates, the tail ordering, and directed route-tree topology `Z` rooted at `z` remain valid. Source access is the existing capsule-filtered tail interval `I`. A changed original tail uses its new key; the immutable base does not renumber.
- Bind each tree pair to its exact current `Q` minimum and original witness, as in AF2. Pair absence makes the corresponding route fail; an equal-weight replacement still changes provenance. Selected-identity binding also works if used consistently, but is not mixed with pair binding here.
- `L0(p)` is the checkpoint's finite tree-route length. For a live route, `delta(p)=L(p)-L0(p)`. Absent pairs contribute separate subtree failure counts. Restoring an ancestor does not clear a missing descendant. Recheckpoint only with a new verified finite spanning route tree for the indexed covered heads, or leave the fast profile.
- The immutable base has `A` records with weight `sigma(e)=c0(e)-L0(head(e))`. Give `z` coordinate zero and weight `c0(e)`. Give outside-`Z` heads one extra sentinel coordinate with any finite priority, such as `c0(e)`, because only occupancy matters there.
- Base tail keys `(capsule, F-preorder, stable edge ID)` have unique ranks. Head ties are retained; rank conversion includes the entire tied group. Weights and provenance are recoverable from the paid base record, not just its priority rank.

Use nonnegative current reduced edge costs, signed certificate slacks, checked exact arithmetic occupying a declared constant number of words, and a separate FAILED/EMPTY representation. Arbitrary-precision bit costs are additional if this word assumption fails. No infinity subtraction is performed.

The current view is `V = (base minus D) union P_live`. `D` shadows every base identity touched since this checkpoint; `P_live` contains the latest live record for each touched or newly inserted identity. A reweight/move shadows the old point and inserts the new one. Deletion has no live replacement. Restoration publishes a new incarnation token, even when endpoints and weight equal the old bytes. The conservative scheme keeps a touched base identity shadowed until checkpoint. All edits to a shared access/core witness update both roles before publication.

`b` counts distinct touched access identities during the epoch, not event count; `|D| <= b`, `|P_live| <= b`. Repeated edits of one ID need one current patch entry, but old pinned versions and recovery receipts still consume storage. Let `t` count tree pairs whose numerical weight/absence currently differs from the epoch base. Signed subtree events give `r <= 2t+1` head bands before coalescing. An identity-only pair replacement may leave `t` unchanged but must update the pinned witness map.

After each batch, publish one immutable manifest naming compatible source, raw-ledger, base-index, patch, `Q`, route-label, and witness-map roots. Query and path delivery pin that manifest. Updating an index is not permission to read another generation's raw witnesses.

## Candidate 1: Scalar Base Plus Patch

**Precisely tested claim:** answer each head-band minimum and the leader minimum by `min(base minimum, latest live patch minimum)`, treating a tombstone as `+infinity`, without any further base access or deletion-aware index change. This would retain static storage and make a small batch cheap at both update and query time.

**Rejected.** Minimum has no inverse. In one source range take base records `(z,0), (z,1), (p,6)` and a route `z->p` of length 2. Delete only the cost-zero leader. The actual leader is now cost 1. The proposed union still emits the deleted cost-zero edge. With the second leader's cost 5 instead of 1, all available scalar summaries are the same before deletion: leader minimum 0, nonleader slack 4, and the same deletion receipt. After deletion the first instance passes the exact guard and the second fails (`5+2>6`). Neither a tombstone minimum nor the old winning ID distinguishes them.

The retained checker exercises the stronger minimal witness failure too: delete the *only* `z` access from `{(z,0),(p,2)}`, with route length 1. The stale test accepts, but `z` is unreachable from the source. Numeric equality alone would not rescue that certificate's nonexistent first edge.

This is a two-summary information obstruction, not a lower bound on data structures that retain/revisit the base. A static index can find the next survivor. Deleting a nonleader may merely turn a stale minimum into a false rejection; deleting or increasing a leader can produce an invalid accepted witness.

Primary precedent is explicit: [Lueker, printed pp. 3-4](https://escholarship.org/content/qt9k40c7jc/qt9k40c7jc.pdf) admits `min` aggregation but conditions the two-structure subtraction trick on an inverse. [Bentley and Saxe, Sections 2 and 6](https://paperman.name/biblio/Bentley80.pdf) distinguish decomposability from inverse-based deletion. Their general transformation lower bound is **not** imported as a lower bound for this geometry-specific problem.

## Candidate 2: Immutable Argmin With Shadow Repair

**Precisely tested claim:** keep the base index immutable, represent all intervening access changes in a versioned shadow/patch overlay, and repair only deleted/replaced argmins during each rectangle query. Combine this with the shared current route bands. This supports exact selected-route certificates at the bounds below, without rewriting all access records affected by shared drift.

### Procedure

Assume a static oracle returns an actual minimum-priority point, or EMPTY, in a half-open rectangle. Tie-break by stable base ID. Let `T(A)`, `S(A)`, and `P(A)` be its query, retained-space, and paid-build bounds, including declared witness lookup costs where appropriate.

For rectangle `[l,u) x J` in unique base tail ranks:

1. Ask the immutable argmin oracle. EMPTY finishes this subrectangle.
2. If the returned identity is not in `D`, it is the exact minimum of all live base points in that subrectangle. Return it; do not enumerate larger survivors.
3. Otherwise its tail rank is `x`. Recurse into `[l,x) x J` and `[x+1,u) x J`; return the smaller surviving result. Splitting a unique *record rank*, not an entire tail coordinate, preserves parallel records and other accesses at that tail.

Run this for disjoint head categories: `{z}`, each of the `r` nonroot route bands, and the outside sentinel. Merge the live patch separately using actual current tail containment and head membership. Endpoint-moved patches need not be inside the old base interval. Compare current finite costs, not stale patch slacks. The root result is the actual minimum leader `a` and its current witness.

Return EMPTY if there are no current accesses. Otherwise reject if no leader exists, an outside head is occupied, or a failed band is occupied. For each occupied live band with drift `d_j`, require `a <= min_live_base_sigma(j)-d_j`; check each matching patch record directly with `a+L(head)<=c`. All-z ranges need no route. Rejection means ordinary exact seeds/fallback, not an unreachable destination or an incorrect distance.

### Additive Repair Theorem

Let `d_I` be the number of shadowed **base** records whose old tail keys lie in `I`. A full, non-short-circuit evaluation uses at most

```text
static rectangle calls <= r + 2 + 2*d_I.
```

Proof: the initial `r+2` head rectangles partition the base points in `I`. A shadowed argmin makes at most two child calls. Its own unique rank is removed; children are disjoint; and it cannot occur in any other initial head band. Thus every discovered shadow is charged at most once. There are at most `d_I` such discoveries. A live argmin is sufficient because removing other points cannot create a smaller point below it. Induction on the remaining shadow count proves exactness. This proof applies to all signed base priorities and arbitrary record/head duplication.

The bound counts empty child calls. It can be smaller if live minima hide most shadows. It does not presume `d_I` counts only *deleted* edges: reweighted and endpoint-moved base records are also excluded. A head move is charged in its **old** band, while its live replacement is checked at the new head. The disjoint-band argument is why a multiplicative `r*b` repair bill is unnecessary.

With a deterministic shadow dictionary and patch scan, a conservative current-view query bound is

```text
O(log(A+1)
  + (r+2+2*d_I) * (T(A)+log(b+2))
  + b*log(r+2))
```

after head-band boundaries have been converted to base ranks at publication. The final term locates patch heads in the current bands; `z`/outside are constant-time cases given paid head metadata. Tail endpoints cost `O(log(A+1))` once. The bound includes checking all `b` patch entries even if none belongs to this source. Finding only local patch entries can improve it but needs an additional paid index.

DFS repair uses `O(d_I+1)` stack words in the worst case, not automatically `O(log A)`. Add the static oracle's own workspace, bounded caches, and `O(b+r)` if patch/envelope are resident. The theorem counts oracle calls, not disk pages or wall time.

### Explicit Paid Update And Checkpoint Schedule

One intentionally simple, predictable implementation publishes flat immutable patch/envelope arrays each batch. For `k` receipts, it merges the previous sorted patch with sorted receipts in `O(b + k log(k+1))` work, plus stable-ID base lookups `O(k log(A+1))`. Retain a cumulative ordered tree-pair difference map; merge its updates and rebuild the event envelope in `O(t log(t+2))`, plus receipt sorting and head-rank conversion `O(r log(A+1))`. Charge exact `Q` pair maintenance, admission checks and witness rebinding separately as `U_Q(k)`; they can dominate.

No claim of `O(k)` total batch work is made. Between checkpoints the extra live state is `S(A)+O(A+b+q+r)` words including the base payload, identity lookup and route metadata; shared objects are counted once. A worst-case flat publication writes `O(b+t+r+k)` metadata words plus changed raw/`Q` data and receipts. Retaining `v` pinned versions can cost the sum of their overlay/envelope sizes, not merely the latest `O(b+r)`.

Declare caps `B` and `R` on cumulative patch size and fragmentation. Before a publication would exceed either cap, admit a paid checkpoint/rebuild or use ordinary exact fallback. A new checkpoint streams the current ledger, rejoins `L0`/head coordinates, constructs the static index at cost `P(A')`, and resets the overlay. The old base remains pinned until its readers/output complete. Do not silently discard readers to meet a byte cap, or label this foreground rebuild a worst-case small update. Background rebuilding/deamortization is not proved here.

For an epoch with batch costs `U_j` and queries `i`, the relevant access-stage bill is

```text
P(A) + sum_j U_j
     + sum_i [rank_lookup_i
              + (r_i+2+2*d_i)*(T(A)+log(b_i+2))
              + b_i*log(r_i+2)].
```

Add extraction/base preparation, `Q` solving, fallback streams, original output/path expansion, recovery and next-checkpoint coexistence. Latest-batch size cannot replace `b_i` or `r_i`. A history of one-record batches can make either linear in epoch history; repeated edits of one record need not.

For the portfolio's provisional resource envelope, admission must check the actual byte inequalities, not just asymptotic words:

```text
host RAM = source/runtime + core state + index workspace/cache
         + resident overlays/envelopes + repair stack + output buffers <= 4 GB
prepared union = all families' unique base/index/raw pages
               + unique pinned versions + newly building retained pages <= 50 GB
host disk = prepared union + source staging + merge scratch
          + recovery logs + retained output <= available disk.
```

An external static layout with bounded buffers needs its own oracle-I/O and build-workspace proof. The cited resident RAM query bound does not supply it. A low-RAM fallback can stream the current source ledger exactly, paying its reads and the same core/output obligations. There is no measured 4 GB implementation here. In particular, holding two static indexes across a checkpoint can erase the apparent retained-space saving.

## Graph Certificate And Witness Validity

For each admitted current access `e`, the repaired query establishes a real current leader and `a+L(head(e)) <= c(e)`. The existing access-closure proof gives

```text
min_e [c(e)+dist_Q(head(e),v)] = a+dist_Q(z,v).
```

This uses the unchanged zero-cost `F` path from the actual source to the leader's current tail, followed by its live original edge. The `Z` paths are sufficient upper-bound witnesses, not required to remain shortest. Nothing prunes `Q`; its pair minima still use the complete current ledger. An absent route with no occupied access is irrelevant.

For a returned path, lift each settled core predecessor using the **same pinned version's** pair witness and original `F` prefix. Store/check incarnation as well as identity, endpoints and weight. Deleting an old pair winner and recovering a parallel equal-weight replacement must return the replacement ID. Reweighting a live ID cannot reuse its old cost. Endpoint movement invalidates the old lift even if the ID survives. Nonnegative zero-cost ties use settlement-ordered predecessors or equivalent acyclic reconstruction. Expansion costs at least its delivered edge count and may require nonsequential provenance reads.

A certificate names its manifest and source interval; it is not valid for a later generation unless rechecked. A pinned old generation can legitimately return an edge since deleted from the newest graph, provided the answer is explicitly for the old snapshot. Crash recovery must choose a fully committed manifest or replay the complete batch into all roles before publishing. These are required interface invariants, not storage-engine features tested by this note.

Changing `F`, invalidating `h`, adding a head outside the fixed gate set, or altering source projection leaves this contract. A guard rejection after a `Z` deletion can fall back; a broken `F` prefix cannot be repaired merely by rejecting that guard.

## Matched Controls And Claim Boundary

All controls receive the same source snapshot, `F/pi`, raw ledger, `Q`, tail/head ranks, route bands, complete receipts, discovered factors and witness/output obligations. None must emit an exact per-head vector when a closure-preserving seed suffices.

| Control | Paid behavior and consequence |
| --- | --- |
| Dynamic weighted range tree | The previous fixed head-tree plus augmented tail catalogs uses `O(A log(q+1)+q)` words and `O(log(q+1) log(A+1))` per point edit/rectangle. It shares the same route bands, does not rewrite access priorities after every route edit, and avoids cumulative shadow repair. Its exact point deletion is not a tombstone-min merge. |
| Strong static ORM plus this overlay | [Nekrich](https://arxiv.org/pdf/2007.11094), Section 4 and Appendix D, gives `S(A)=O(A)`, `T(A)=O(log^epsilon A)` for fixed positive epsilon, or `O(A log log A)` space and `O(log log A)` queries, in rank-space word RAM. Substitution in the additive theorem is valid; construction `P(A)`, payload/rank conversion, and physical layout remain paid. This is a constructed control using the new composition, not evidence that Nekrich published the overlay. |
| Fixed-cell current minima | Given a hot source's per-head slack minima, an ordinary lazy head tree handles route-subtree numerical changes and separate failure counts. Its query can avoid enumerating `r`; updates pay materialized-cell fanout, access-minimum changes and storage. Caching/replaying edits is allowed and charged. This can beat the overlay on hot, fragmented epochs. |
| Factored transit access | On `m_i(p)=u_i+v_p` with common occupied-head support, verified and retained at paid cost, test the shared profile under current drift and compare each source's leader against its offset. Keep original witness lookup. The old family remains matched; the patch does not create a new advantage there. Deleting a minimum can expose an unfactored second-best record: charge raw/indexed repair or renewed factor verification, not a free surviving profile. Factor-aware controls may use the same paid overlay. No claim is made that the transit paper published it. |

**What is and is not separated:** static retained space plus query-time shadow repair is a genuine tradeoff against the specified mutable replicated catalogs; it is not a dominance result over all dynamic indexes. It may reduce writes and retained catalogs while increasing queries, checkpoints and pinned coexistence. The same geometric problem can use exactly that tradeoff, so a fixed-topology graph benefit must come from elsewhere.

The reduction is explicit: erase graph names and keep weighted `(tail,head)` points, a shadow dictionary, patch records and a piecewise-constant head function. Every operation in Candidate 2, including its additive accounting proof, still has the same input/output and bound. Conversely, even a single source with zero-forest paths to arbitrary access tails and a route star realizes independent nonnegative slack values and one-edge threshold tests. These admitted graph instances supply no automatic slack restriction. This is not a reduction from *all* arbitrary planar rectangle queries to source subtrees: source intervals are laminar, and no such stronger lower bound is claimed.

**Historical boundary:** the published primitives below occupy RMQ, dynamization, persistence and route domination. The exact exclusion composition and its local proof are derived in this note; a same-step implementation is a correctness control, not independent historical prior art. No claim of prior publication or global impossibility follows from "a competitor could copy it." The negative conclusion is narrower: neither examined candidate currently supports a new graph-specific property or a demonstrated advantage against the strongest equal-information control.

## Primary Papers Inspected

| Paper | Inspected scope; what is imported |
| --- | --- |
| [Lueker, A Data Structure for Dynamic Range Queries, 1978 technical report](https://escholarship.org/content/qt9k40c7jc/qt9k40c7jc.pdf) | Printed pp. 3-7: admissible minimum aggregation, inverse qualification, multidimensional auxiliary trees and deletion support. The conservative fixed-universe catalog bound above is directly specified, not attributed to a stronger unexplained theorem. |
| [Bentley and Saxe, Decomposable Searching Problems I, 1980](https://paperman.name/biblio/Bentley80.pdf) | Sections 2, 3.1 and 6.1-6.2: static-to-dynamic decomposition and the separate inverse/deletion issue. Their restricted transformation-model lower bound is not a graph lower bound. |
| [Nekrich, New Data Structures for Orthogonal Range Reporting and Range Minima Queries, 2020 preprint](https://arxiv.org/pdf/2007.11094) | Model paragraph, Section 4 minima reduction and Appendix D/Theorem 5. Static rank-space RAM minima bounds only; not dynamic-update or external-I/O bounds. Original numeric costs and witness IDs remain necessary. |
| [Driscoll, Sarnak, Sleator, Tarjan, Making Data Structures Persistent, 1989](https://www.cs.cmu.edu/~sleator/papers/making-data-structures-persistent.pdf) | Introduction, Section 2 and Section 4.1: version access and copying are established. This note uses explicitly charged flat snapshots, not the paper's bounded-indegree per-step overhead as an unearned graph-update bound. Persistence does not itself make multiple indexes transactionally consistent. |
| [Arz, Luxen, Sanders, Transit Node Routing Reconsidered](https://arxiv.org/pdf/1302.5611) | Section 4, post-search-stalling inequality and Lemma 2: access elimination by another access plus a route is established. The constructive factored control comes from AF4, not an asserted published arbitrary-ledger equivalence. |
| [Dibbelt, Strasser, Wagner, Customizable Contraction Hierarchies](https://ben-strasser.net/paper/customizable_contraction_hierarchies_arxiv_preprint.pdf) | Section 7.7 handles partial metric updates on fixed contracted topology. A competent core comparator is not forced to recustomize everything. Its optional queue smoothing permits outdated answers and is excluded by this note's exact-publication contract. No CCH bound in the overlay's `b,r` parameters is inferred. |

These are primary papers actually opened during this task, not a keyword-based novelty certificate. No external timings are used. The first noSplash Lueker endpoint failed on a later read; the working repository PDF above supplied the inspected text.

## Retained Bounded Checker

The snippet below is the only executable artifact. It uses an intentionally linear-scan *static argmin oracle* to check the split/merge procedure and count oracle calls. That scan is not a competitive baseline or an implementation of Nekrich's index. A separate direct current-ledger predicate checks exact guard outcomes. Original-graph Bellman-Ford checks tiny snapshot distances; a gate solver using original edge/incarnation sequences checks complete delivered paths.

Coverage: exhaustive small signed route states, base slack combinations and tombstone masks; same-tail records; nested failures; empty and partial ranges; cumulative mixed batches with inserts, deletions, restoration, reweights, endpoint moves, equal-weight parallel route replacements; historical pinned snapshots. Randomness has a fixed seed. It does not implement disk pages, fixed-width overflow, transactions/crash injection, asymptotic static construction or a physical RAM cap.

Run from the repo root:

```sh
awk '/^<!-- UPDATE-QUERY-PROBE-START -->/{p=1;next} /^<!-- UPDATE-QUERY-PROBE-END -->/{p=0} p && !/^```/' research_algorithms_20260920/Paths-Update-Query-Separation.md | python3 -
```

<!-- UPDATE-QUERY-PROBE-START -->
```python
from bisect import bisect_left
from heapq import heappop, heappush
from itertools import groupby, product
from math import inf
from random import Random

# Record: (stable_id, tail, head, cost, incarnation). Heads 0..3 are in Z.
PARENT = (None, 0, 1, 1)
L0 = (0, 3, 6, 6, 0)


def derive_current_route_lengths(routes):
    lengths = [0]
    for head in range(1, 4):
        options = [row[2] for row in routes.values()
                   if row[:2] == (PARENT[head], head)]
        previous = lengths[PARENT[head]]
        lengths.append(None if not options or previous is None
                       else previous + min(options))
    return lengths


def form_current_ledger_records(base, patch):
    current = {row[0]: row for row in base if row[0] not in patch}
    current.update({key: row for key, row in patch.items() if row is not None})
    return list(current.values())


def evaluate_direct_route_guard(records, lo, hi, lengths):
    rows = [row for row in records if lo <= row[1] < hi]
    if not rows:
        return "empty", None
    leaders = [row for row in rows if row[2] == 0]
    if not leaders:
        return "reject", None
    leader = min(leaders, key=lambda row: (row[3], row[0]))
    for row in rows:
        if row[2] == 4 or lengths[row[2]] is None:
            return "reject", None
        if leader[3] + lengths[row[2]] > row[3]:
            return "reject", None
    return "accept", leader


def evaluate_overlay_route_guard(base, patch, lo, hi, lengths):
    tails = [row[1] for row in base]
    left, right = bisect_left(tails, lo), bisect_left(tails, hi)
    bands = []
    offset = 1
    drift = [None if lengths[h] is None else lengths[h] - L0[h]
             for h in range(1, 4)]
    for value, run in groupby(drift):
        size = len(list(run))
        bands.append((offset, offset + size, value))
        offset += size
    categories = [(0, 1, 0)] + bands + [(4, 5, None)]
    calls, discovered, leaders, constraints = 0, set(), [], []

    for head_lo, head_hi, value in categories:
        stack, winner = [(left, right)], None
        while stack:
            start, end = stack.pop()
            calls += 1
            eligible = [i for i in range(start, end)
                        if head_lo <= base[i][2] < head_hi]
            if not eligible:
                continue
            rank = min(eligible, key=lambda i: (base[i][3] - L0[base[i][2]],
                                               base[i][0]))
            row = base[rank]
            if row[0] in patch:
                assert row[0] not in discovered
                discovered.add(row[0])
                stack.extend(((start, rank), (rank + 1, end)))
                continue
            key = (row[3] - L0[row[2]], row[0])
            if winner is None or key < winner[0]:
                winner = (key, row)
        if winner is not None:
            if head_lo == 0:
                leaders.append(winner[1])
            else:
                constraints.append(None if value is None else winner[0][0] - value)

    for row in patch.values():
        if row is None or not lo <= row[1] < hi:
            continue
        if row[2] == 0:
            leaders.append(row)
        else:
            length = None if row[2] == 4 else lengths[row[2]]
            constraints.append(None if length is None else row[3] - length)
    shadows = sum(row[0] in patch for row in base[left:right])
    assert calls <= len(bands) + 2 + 2 * shadows
    assert len(discovered) <= shadows
    if not leaders and not constraints:
        result = ("empty", None)
    elif not leaders:
        result = ("reject", None)
    else:
        leader = min(leaders, key=lambda row: (row[3], row[0]))
        accepted = all(value is not None and leader[3] <= value
                       for value in constraints)
        result = ("accept", leader) if accepted else ("reject", None)
    return result, calls


def construct_snapshot_graph_edges(records, routes):
    # Original tails 0,1,2; gates 3..7; capsule gate 8.
    edges = {1000: (8, 0, 0, 0), 1001: (0, 1, 0, 0),
             1002: (1, 2, 0, 0), 4000: (3, 7, 2, 0),
             4001: (7, 8, 1, 0), 4002: (5, 3, 0, 0)}
    for identity, tail, head, cost, token in records:
        edges[identity] = (tail, head + 3, cost, token)
    for identity, (tail, head, cost, token) in routes.items():
        edges[identity] = (tail + 3, head + 3, cost, token)
    return edges


def solve_original_graph_distances(edges, source):
    distance = [inf] * 9
    distance[source] = 0
    for _ in range(8):
        changed = False
        for tail, head, cost, _ in edges.values():
            if distance[tail] + cost < distance[head]:
                distance[head] = distance[tail] + cost
                changed = True
        if not changed:
            break
    return distance


def recover_compressed_snapshot_paths(records, routes, source, result):
    edges = construct_snapshot_graph_edges(records, routes)
    state, leader = result
    seeds = ([leader] if state == "accept" else
             [row for row in records if source <= row[1]])
    adjacency = {vertex: [] for vertex in range(3, 9)}
    pairs = {}
    for identity, tail, head, cost, token in records:
        witness = tuple((1000 + i, 0) for i in range(tail + 1)) + ((identity, token),)
        key = (8, head + 3)
        candidate = (cost, identity, witness)
        if key not in pairs or candidate[:2] < pairs[key][:2]:
            pairs[key] = candidate
    for (tail, head), (cost, _, witness) in pairs.items():
        adjacency[tail].append((head, cost, witness))
    for identity, (tail, head, cost, token) in edges.items():
        if tail >= 3 and head >= 3:
            adjacency[tail].append((head, cost, ((identity, token),)))
    distance, paths, queue = [inf] * 9, [None] * 9, []
    for identity, tail, head, cost, token in seeds:
        vertex = head + 3
        if cost < distance[vertex]:
            distance[vertex] = cost
            paths[vertex] = tuple((1000 + i, 0) for i in range(source + 1, tail + 1))
            paths[vertex] += ((identity, token),)
            heappush(queue, (cost, vertex))
    while queue:
        cost, tail = heappop(queue)
        if cost != distance[tail]:
            continue
        for head, weight, witness in adjacency[tail]:
            if cost + weight < distance[head]:
                distance[head] = cost + weight
                paths[head] = paths[tail] + witness
                heappush(queue, (distance[head], head))
    for tail in range(3):
        if source <= tail:
            distance[tail] = 0
            paths[tail] = tuple((1000 + i, 0) for i in range(source + 1, tail + 1))
        elif distance[8] < inf:
            distance[tail] = distance[8]
            paths[tail] = paths[8] + tuple((1000 + i, 0) for i in range(tail + 1))
    assert distance == solve_original_graph_distances(edges, source)
    checked = 0
    for target, path in enumerate(paths):
        if distance[target] == inf:
            assert path is None
            continue
        vertex, total = source, 0
        for identity, token in path:
            tail, head, cost, incarnation = edges[identity]
            assert token == incarnation and tail == vertex
            vertex, total = head, total + cost
        assert vertex == target and total == distance[target]
        checked += 1
    return checked


# Candidate 1 fails even before drift changes.
naive_base = [(10, 0, 0, 0, 0), (11, 0, 1, 2, 0)]
naive_patch = {10: None}
naive_lengths = [0, 1, None, None]
assert evaluate_direct_route_guard(naive_base, 0, 1, naive_lengths)[0] == "accept"
assert evaluate_overlay_route_guard(naive_base, naive_patch, 0, 1, naive_lengths)[0][0] == "reject"
naive_current = form_current_ledger_records(naive_base, naive_patch)
naive_edges = construct_snapshot_graph_edges(naive_current, {2001: (0, 1, 1, 0)})
assert solve_original_graph_distances(naive_edges, 0)[3] == inf
for replacement, expected in ((1, "accept"), (5, "reject")):
    records = [(10, 0, 0, 0, 0), (12, 0, 0, replacement, 0), (11, 0, 1, 6, 0)]
    assert min(row[3] for row in records if row[2] == 0) == 0
    survivors = form_current_ledger_records(records, {10: None})
    assert evaluate_direct_route_guard(survivors, 0, 1, [0, 2, None, None])[0] == expected
print("naive_scalar_overlay deleted_leader_and_indistinguishable_summaries_detected")

# Same numeric answer, wrong incarnation: the path validator must still fail.
restored = [(10, 0, 0, 0, 1), (11, 0, 1, 2, 0)]
try:
    recover_compressed_snapshot_paths(restored, {2001: (0, 1, 1, 0)}, 0,
                                      ("accept", naive_base[0]))
except AssertionError:
    pass
else:
    raise AssertionError("stale incarnation was not detected")
assert evaluate_overlay_route_guard(naive_base, {10: (10, 2, 0, 0, 1)},
                                    0, 1, naive_lengths)[0][0] == "reject"
print("witness_mutations stale_incarnation_and_moved_leader_detected")

cases, maximum_calls = 0, 0
for slacks in product(range(3), repeat=3):
    base = [(10, 0, 0, 0, 0)] + [
        (10 + head, head - 1, head, L0[head] + slacks[head - 1], 0)
        for head in range(1, 4)]
    base.sort(key=lambda row: (row[1], row[0]))
    for weights in product((0, 3, 6, None), repeat=3):
        routes = {2000 + head: (PARENT[head], head, weight, 0)
                  for head, weight in enumerate(weights, 1) if weight is not None}
        lengths = derive_current_route_lengths(routes)
        for mask in range(16):
            patch = {row[0]: None for i, row in enumerate(base) if mask & (1 << i)}
            current = form_current_ledger_records(base, patch)
            for lo in range(4):
                for hi in range(lo, 4):
                    result, calls = evaluate_overlay_route_guard(base, patch, lo, hi, lengths)
                    assert result == evaluate_direct_route_guard(current, lo, hi, lengths)
                    cases += 1
                    maximum_calls = max(maximum_calls, calls)
print("exhaustive_guard_cases", cases, "maximum_rectangle_calls", maximum_calls, "mismatches=0")

rng = Random(20260921)
versions, source_queries, path_checks, accepted = 0, 0, 0, 0
for trial in range(40):
    base = [(10, 0, 0, 0, 0), (11, 0, 2, 8, 0),
            (12, 1, 1, 5, 0), (13, 1, 3, 8, 0),
            (14, 2, 2, 9, 0), (15, 2, 0, 1, 0)]
    patch = {}
    routes = {2000 + h: (PARENT[h], h, 3, 0) for h in range(1, 4)}
    saved = []
    for version in range(18):
        if version == 1:
            patch[10] = None
        elif version == 2:
            patch[10] = (10, 0, 0, 0, version)
            routes[3001] = (0, 1, 3, version)
            routes.pop(2001)
        elif version == 3:
            routes.pop(3001)
            routes.pop(2002)
        elif version == 4:
            routes[2001] = (0, 1, 3, version)
            assert derive_current_route_lengths(routes)[2] is None
        elif version == 5:
            routes[2002] = (1, 2, 0, version)
        elif version > 5:
            for _ in range(1 + version % 3):
                identity = rng.randrange(10, 19)
                patch[identity] = (None if rng.randrange(4) == 0 else
                                   (identity, rng.randrange(3), rng.randrange(5),
                                    rng.randrange(11), version))
            head = rng.randrange(1, 4)
            identity = 2000 + head
            if rng.randrange(4) == 0:
                routes.pop(identity, None)
            else:
                new_head = head if rng.randrange(5) else rng.randrange(1, 5)
                routes[identity] = (PARENT[head], new_head, rng.randrange(7), version)
        saved.append((dict(patch), dict(routes)))
        versions += 1
    # Replay pinned old roots after all later batches have been generated.
    for old_patch, old_routes in saved:
        current = form_current_ledger_records(base, old_patch)
        lengths = derive_current_route_lengths(old_routes)
        for source in range(3):
            result, _ = evaluate_overlay_route_guard(base, old_patch, source, 3, lengths)
            assert result == evaluate_direct_route_guard(current, source, 3, lengths)
            path_checks += recover_compressed_snapshot_paths(current, old_routes, source, result)
            source_queries += 1
            accepted += result[0] == "accept"
print("versioned_graph_checks", dict(versions=versions, source_queries=source_queries,
      recovered_paths=path_checks, accepted=accepted), "mismatches=0")
```
<!-- UPDATE-QUERY-PROBE-END -->

Executed from the retained block, exit 0:

```text
naive_scalar_overlay deleted_leader_and_indistinguishable_summaries_detected
witness_mutations stale_incarnation_and_moved_leader_detected
exhaustive_guard_cases 276480 maximum_rectangle_calls 13 mismatches=0
versioned_graph_checks {'versions': 720, 'source_queries': 2160, 'recovered_paths': 18599, 'accepted': 655} mismatches=0
```

The exhaustive domain is `3^3` slack vectors, `4^3` route states (weight 0, 3, 6, or absent per tree pair), 16 base-shadow masks, and 10 half-open intervals including empties: 276,480 cases. These are finite algebraic cases, not that many distinct production graphs. The mixed suite is 40 deterministic trials, 18 versions each, and three actual source suffixes; all nine original vertices are checked for distance/reachability and every finite answer has its returned path checked. The `accepted` count is a coverage observation, not a success-rate prediction for real source ledgers. The two deliberate stale-witness mutations are expected failures caught by the checker, not ignored test failures.

## Next Research Action

The two candidates terminate here: Candidate 1 is falsified; Candidate 2 is exact but supplies no established graph-specific separation. Its deletion-aware static control must accompany any later claimed win for dynamic access catalogs. No additional fixture resembling the settled factored family is justified as novelty evidence.

**One next question, not a third candidate explored here:** after deleting one original zero-forest edge, can we repair source domains *and* affected quotient lifts with work bounded by a paid dependency cone, without rebuilding all `pi` or leaving a stale `F` prefix inside `Q`? The required output is an exact original-edge path, not just a scalar distance.

The next bounded study should first build a branching capsule where many quotient witnesses share that prefix, including parallel alternatives and a dormant return. Count distinct damaged original witnesses, distinct affected pair minima, and changed source domains separately. Require a proof or counterexample to any proposed dependency-cone bound before choosing an implementation. Compare with ordinary dynamic-tree bookkeeping, reverified pair minima and partial contraction updates given the same dependencies. If arbitrary many pair winners require replacement after one forest deletion, record that fanout explicitly; do not call the receipt count the repair bound.

This changes the next action from tuning a generic guard to testing the currently unhandled graph-validity boundary. It makes no promise that this next question will produce novelty. Whole-host RAM, shared prepared bytes, snapshot overlap and output obligations remain open.
