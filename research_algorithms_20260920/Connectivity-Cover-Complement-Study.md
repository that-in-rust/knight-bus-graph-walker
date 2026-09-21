# Covered-Edit Connectivity: A Capped Complement Schedule

Date: 2026-09-21. Independent algorithm-design study, owning only this file.
No lead implementation is read, imported, executed, or changed. This is a
prepared-source query theorem and finite checker, not a physical-memory
result or a claim of publication-level novelty.

## Result

**A work theorem survives; an exclusive asymptotic RAM advantage does not.**
Replace repeated blocked posting scans by factor-local ordinary complement
BFS, with an outside-connected seed class when available. Cap deletion-row
openings, deletion records, and candidate tests. Finish capped factors in
ONE shared row-major fallback, not one fallback per factor.

With definitions below, the additional search/fallback work satisfies

```text
K = sum_f min(q_f, h_f),             q_f = t_f * p_f <= t_f * b_f
metered search events + fallback posting records <= 2K.

Total logical work:
O(n + F + k + Z + delta + J_U + K)
  + O((n + Z + |A| + K) * alpha(F+k+1)).
```

Here `p_f` counts still-unattached core members at factor entry (all `t_f`
for a nontrivial core-only factor); `h_f` is the precisely defined
counterfactual COMPLETE local-search cost, including repeated GLOBAL
deletion rows, not merely deletions inside the factor. Already-resolved
factors have `p_f=h_f=0`. State is `O(F+k)` words using existing paid views,
without a deletion-to-factor table or resident per-original-vertex state.
Full original-ID output and exact component minima are retained.

A core-only clique with a deleted perfect matching now has linear work
instead of `C_block=k^2` postings. Ordinary complement BFS already achieves
that special case. The tested addition is the explicit composition with
covered-edit outside contraction, seeding, cancellation, and shared fallback,
not a new search primitive.

## Premises And Controls

Read: [the quotient design](Connectivity-Fault-Cover-Quotient.md) and the
completed [independent review](Connectivity-Fault-Cover-Review.md), including
its checker receipt and strong forest countercontrol. Keep their contract:

```text
E0 = union_f binom(S_f,2)
G  = (V, (E0 minus D) union A)
D subset E0; A consists of nonbase pairs; both are normalized simple pairs
C covers D union A; k=|C|; all IDs in V must be output
C_f=S_f intersect C; T_f=S_f minus C; t_f=|C_f|; m_f=|T_f|
Z=sum_f |S_f|; Z_C=sum_f t_f; delta=|D|+|A|.
```

Duplicate factor identities are allowed, duplicate members/change pairs are
not. A deletion suppresses its pair through EVERY overlapping factor. The
cover need not be minimum. Input, indexes, cover preparation, sorting,
scratch, and output are paid separately from query working state.

The review proves for ANY fixed true DFS forest of the base clique union:
at most `F` nontrivial rooted leaves, `|D intersect T|<=2k+F`, and at most
`2F+3k` explicit fragments with its isolate handling. Those bounds remain
in force. This study supplies no separation from that fragment-state bound.

Primary-source check, deliberately narrow:

* [Dahlhaus, Gustedt and McConnell (2002), Definition 1.1 and Theorem 1,
  pp. 149-150](https://www.cs.colostate.edu/~rmm/cstacks.pdf): searches in
  partially complemented representations are established. The unvisited-list
  procedure below is ordinary complement search, with a direct proof for
  the source accounting needed here.
* [Ullah and Pothen, DCC paper, Section 4.2/Figure 7/Lemma 4.8 and
  Section 4.3/Figure 8/Lemma 4.9](https://arxiv.org/pdf/2604.28096#page=14),
  full-text inspected on 2026-09-21: DFS uses per-vertex discovery/parent
  state and monotone per-clique cursors with incidence-size work. CC uses
  vertex DSU and clique stars; it does not require the incidence dual.
  These are competent representation-aware base controls, not expanded-clique
  strawmen. The inspected procedures operate on intact cliques; directly
  retaining their star unions after arbitrary pair deletions is unsound.
  This is not a claim about every possible extension of the paper. Our
  inference: charge native DFS preparation and fragment repair fairly;
  neither base DFS nor factor-native CC is a contribution of this study.

Expert lenses: complement-search accounting, exact contraction, source
lifecycle, and adversarial comparison. These are analytical perspectives,
not a claim that separate agents ran.

## Ordinary Control First

For one factor, keep an unvisited linked list of core members. Start a BFS
tree at an arbitrary unvisited member. To process `s`, read `D_s`, mark its
deleted core neighbors, then walk the list. Remove and enqueue unmarked
members, unioning them with `s`; leave marked members on the list. Restart
when the queue empties but unvisited members remain. Stop immediately when
the unvisited list empties; remaining queue rows cannot add connectivity.

Each successful test removes a vertex. Each unsuccessful test witnesses a
distinct unordered deleted pair: after `s` is processed it is never again
unvisited, so the reverse orientation is never tested. Candidate tests are
at most `t_f+e_f`, where `e_f=|D intersect binom(C_f,2)|`. For processed row
owners `P_f`, the ACTUAL metered cost is

```text
h_f^ordinary = sum_{s in P_f}(1+|D_s|) + x_f,
x_f <= t_f + e_f,                    P_f subset C_f.
```

The `1` pays a row opening, including an empty row. Filtering to a local
factor does not make other returned records free. Without a paid local
negative view, claiming `O(t_f+e_f)` SOURCE work is wrong. Overlapping factors
can repeatedly read `D_s`. Providing all local-negative rows can expand one
deletion into arbitrarily many factor occurrences.

## Outside-Connected Seeding

First complete ALL outside-factor unions, exact reattachments, and insertions
from the quotient construction. Cache `d_s=|D_s|` and `c_s=|D_s intersect C|`
in `O(k)` words during its first deletion-row pass. Do not retain a blocked
incidence matrix or all-factor blocked lists.

For each factor in fixed order, load its `C_f` posting into at most `k`
slots. If it has at most one member, or all core members already have the
same DSU root, skip its core work: no core edge can change the partition.
All outside attachments are complete, so this cannot omit an outside edge.

For an unresolved factor with `m_f>0`, define ONCE at entry:

```text
R_f = {s in C_f : find(s)=find(f)}
B_f = C_f minus R_f; p_f=|B_f|.
```

`R_f` contains all nonblocked incidences, possibly plus core members connected
to `f` through insertions or earlier factors. Thus `B_f` is a subset of the
original blocked set and `p_f<=b_f`. Members of `R_f` have certified REAL
paths to one another, possibly outside this factor. Freeze these sets.
For `m_f=0`, use `R_f=empty`, `B_f=C_f`, `p_f=t_f=b_f`; do not invent an
outside connector.

### Seed Test

If `R_f` is nonempty, `s in B_f` reaches its connected class through this
factor iff `|D_s intersect R_f| < |R_f|`. When `c_s<|R_f|`, certify that
inequality without another deletion read. Otherwise scan COMPLETE `D_s`
and count its members in `R_f`. A positive result unions `s` with `f`,
removes it from the unvisited list, and enqueues it. This union uses a real
surviving `s-r` edge and the certified `r-to-f` path, not a claimed direct
original edge to a virtual node.

Seeding is necessary. With `S_f={s,r,u}`, `C={s,r}`, `D={su}`, `r` attaches
to outside `u` and blocked `s` must be seeded through `sr`. A search solely
on the singleton `{s}` loses that connection. Conversely, enqueueing every
blocked member as attached is wrong if `sr` is deleted too.

### Remaining Search And Cost

Run ordinary unvisited-list complement BFS on `B_f`, initialized with ALL
certified seeds in one queue. Process seed rows if unvisited members remain,
even though those seeds are already outside-connected. After exhausting the
seed queue, start components from the remaining list. Queue each vertex at
most once; no row belonging to `R_f` is required. For empty `R_f`, omit
seeding and use ordinary BFS.

Let `Y_f` be seed-test row owners not certified by the floor, `P_f` the BFS
row owners processed, and `x_f` candidate tests in the uncapped local run on
the frozen sets. Define

```text
h_f = sum_{s in Y_f}(1+d_s) + sum_{s in P_f}(1+d_s) + x_f
Y_f,P_f subset B_f;                 x_f <= p_f + e_B(f)
e_B(f) = |D intersect binom(B_f,2)|
rho_f = sum_{s in B_f} d_s
h_f <= 3p_f + 2rho_f + e_B(f) <= 3p_f + (5/2)rho_f.
```

A row can be read TWICE in one factor and again in another factor. Neither
read is hidden. List building, seed iteration/unions, queue bookkeeping and
touched-state cleanup add `O(t_f)` work; `h_f` meters potentially dominant
work beyond that. `rho_f` includes full rows, not just internal negatives.
The actual `h_f` can be much smaller because queue tails and floor-certified
seed rows are never read.

Consequently the theorem also gives the less execution-sensitive upper
bound `K <= sum_f min(t_f*p_f, 3p_f+(5/2)rho_f)`. One can compute `rho_f`
from the cached row lengths while classifying the posting, without reading
those rows. This is an upper bound, not a prediction that every such row
will be read. The sharper `h_f` remains useful for early-stop executions.

### Exactness Lemma

Contract the certified class containing `R_f` and outside `T_f` to a
supernode. Edges from blocked vertices to this supernode exist exactly when
their seed test succeeds; exact reattachment excludes surviving edges from
`B_f` directly to `T_f`. The other local edges are the complement of `D`
on `B_f`. Multi-source complement BFS preserves this contracted graph's
components. Empty `R_f` leaves the outside supernode separate; `m_f=0`
means no such supernode exists. Cross-factor paths only contract TRUE
connectivity. Every union is sound, and every skipped local edge already
has a path. Across all factors every surviving base edge is preserved;
consulting global normalized `D` prevents overlap from reviving an absence.

## Cap And Shared Fallback

For an unresolved factor use budget `q_f=t_f*p_f`. Meter one event per
deletion-row open, returned deletion record, or unvisited candidate test.
Stop BEFORE the event exceeding `q_f`. An abandoned count/mark must never
justify a union. A completed run costs `h_f<=q_f`; interruption spends
exactly `q_f<h_f` and sets one unresolved bit. Keep completed unions, all
of which are already justified. No rollback is required.

`h_f` is defined by continuing with frozen entry sets and fixed list order,
without testing changing DSU roots inside the traversal. Its definition is
not circular or affected by partial unions; it is an analysis quantity,
not a planning oracle.

After attempting ALL factors, process unresolved bits in ONE cover-row pass.
For each `s in C`, read `D_s` once into reused forbidden-core marks, read
its factor memberships, and visit unresolved incident factors. For `m_f>0`,
skip if `find(s)=find(f)` now. Otherwise scan `C_f` and union every distinct,
nondeleted pair. For `m_f=0`, scan without claiming an outside attachment.
Per-row candidate stamps may deduplicate unions without changing postings.

**Coverage:** a live edge has an endpoint that scans, or both endpoints
skip because they reach the same factor root. Core-only factors scan both.
Roots only merge, so a previously skipped endpoint stays connected. This
remains true after partial BFS unions.

**Cost:** a positive-outside scanning endpoint was in `B_f` at entry;
members of `R_f` cannot become detached. At most `p_f` rows scan `t_f`
records, also true for core-only factors. Hence

```text
attempt events = sum_f min(q_f,h_f) = K
fallback posting records <= sum_{f:h_f>q_f} q_f <= K.
```

One additional deletion sweep costs at most `sum_s d_s<=2|D|`, and one
cover-membership sweep costs `Z_C`. They are NOT repeated per failed factor.
A private fallback inside each factor would not prove the theorem. The
entry resolved gate adds `O(t_f)` finds, paid even when `K=0`.

## Source/API And Output

These are proposed executor changes, not existing lead-code capabilities:

| Surface | Change or guarantee |
| --- | --- |
| Phase ordering | Finish attachments and insertions before local factor search; output only after shared fallback. |
| First deletion pass | Cache full length `d_s` and core degree `c_s`, only `O(k)` counters. |
| Factor posting | Open `C_f` once for search, retaining ONE posting of at most `k` IDs; reopen as needed in fallback. No blocked-flag spool. |
| Deletion cursor | Snapshot-checked replay, bounded-buffer `next`, immediate close/cancel. Never materialize an unbounded row before honoring the cap. Cached exact length controls EOF. |
| Membership cursor | Existing complete indexed rows; every open and returned record is paid. |
| State | Reused `O(k)` local marks, links, FIFO and seed state; `O(F)` deferred bits, plus existing DSU/count/stamp state. |
| Output sink | Backpressured stream of every original ID and minimum-ID label, not an `n`-label dictionary. |

Concretely, proposed provider methods are
`open_cover_deletion_row(snapshot_id, original_id)` and
`open_factor_core_posting(snapshot_id, factor_id)`, returning an immutable
row length and a cursor with `read_next_neighbor_exact()` and
`close_current_cursor_exact()`. For postings the cursor returns core IDs;
for deletion rows it returns ALL deleted-neighbor IDs. Existing membership
lookup returns complete factor IDs, not a query-filtered negative view.
Charge an open before calling the provider and a record before requesting
the next element. Admission must bound the number and size of cursor buffers;
any prefetch/read-ahead bytes are additional measured physical I/O, not
unreported logical records. A core-only search requires no new outside-ID
lookup. There is no required `(factor,deleted pair)` index or seed-witness
materialization. These names specify the proposed API, not symbols claimed
to exist in lead code.

Use stamps or touched lists instead of `k` clears per factor. A dictionary
for only the current factor costs its touched entries. Epoch wrap, finite
counter widths, corrupt lengths, duplicates, stale snapshots and I/O failure
require production validation not supplied by this checker.

For an explicit source ledger, let `U` be uncertain attachment row owners,
`L_U=sum_{s in U}|D_s minus C|`, `J_U=sum_{s in U} sum_{u in D_s minus C}|F_u|`,
and `I=1` iff any factor is deferred. Besides the two complete vertex passes,
edit validation/preparation and insertion streams:

```text
indexed membership opens:    k + L_U + I*k
membership records:          O(Z_C + J_U)
deletion row opens:          k + |U| + I*k + sum_f a_f
deletion records:            sum_s d_s + sum_{s in U}d_s
                            + I*sum_s d_s + sum_f r_f
factor posting opens:        <= F + sum_{deferred f} v_f
factor posting records:      Z_C + sum_{deferred f} t_f*v_f

a_f,r_f = actual local opens/records, including interrupted prefixes
v_f <= p_f = fallback rows that actually scan factor f
sum_f(a_f+r_f+x_f^attempt) = K.
```

The fallback described scans all cover rows, including empty/unneeded ones.
The unchanged attachment pass looks up each deleted outside neighbor for
each uncertain owner. An indexed row does not license an `n`-entry resident
offset array. Cover-ID dictionaries give expected constant lookup; sorted
deterministic lookup adds `log(k+1)` to affected ID tests. Rank/compression
gives the opening DSU bound. Opens are logical events, not one-page I/O or
latency promises. Apply actual lookup tariffs to separate counts. The cap
requires bounded source fetching; eager unbounded rows violate its contract.
Weighted metering could price known tariffs; this theorem is unweighted.

Minima/output are unchanged: initialize every core ID; contribute every
factor-supported outside ID; an incidence-free outside insertion endpoint
unions ALL inserted core neighbors and contributes its own ID. Every union
reduces minima. The final vertex scan emits its core root, incident factor
root, inserted core-neighbor root, or its own ID for an untouched isolate.
Reordering justified unions cannot lose an isolate, bridge minimum, or ID.

## Separations And Losses

1. **Core-only sparse deletions.** One clique on even `k>=4` core vertices
   minus a consecutive perfect matching has `C_block=k^2`, `J_U=0`.
   Ascending early-stop BFS opens two rows, reads two deletion records,
   and tests `k` candidates: `h_f=k+4`. Ordinary complement BFS matches;
   this is not a new special-case result.
2. **Outside seed class.** One clique with `t` cores, one outside vertex,
   deleted core perfect matching and outside deletions to half the cores
   has `b=t/2`, `C_block=t^2/2`, `J_U=t/2`. For `t>=4`, core deletion
   degree 1 is smaller than `|R_f|=t/2`. All blocked vertices seed without
   rereading `D`: `h_f=0`, plus `O(t)` setup/unions. Ordinary all-core
   complement traversal is also linear on this particular family.
3. **Uncapped replay loses asymptotically.** Core vertices `s,x_1,...,x_r`,
   factors `{s,x_i}`, and one factor containing `s` and `N` outside vertices.
   Delete every outside edge from `s`, keep all `sx_i`. Process small
   factors first, each with `s` first. Raw/ordinary traversal rereads `D_s`
   `r` times: `r(N+2)` events. Blocked scanning needs `4r+1` postings and
   shared `D_s`. For `N>2` the cap spends `4r`, then at most `4r` fallback
   postings plus ONE global deletion sweep. Full work is `O(N+r)`, including
   `J_U=N`. No local negative occurrence table is prepared.
4. **Dense negatives lose constants.** Delete every pair in a core-only
   clique. BFS makes `binom(t,2)` failed tests and reads quadratic deletion
   records. The cap may run out and then pay blocked scanning too. It is
   constant-competitive in the stated model, not pointwise faster.
5. **Other limits.** Cold row lookups can favor sequential blocked scanning;
   `J_U` remains arbitrarily amplified by overlap, and output is `Omega(n)`.
   Seeding may scan many high-degree irrelevant rows while one low-degree
   member of `R_f` could discover all cores in ordinary BFS. This theorem
   competes with blocked scanning, NOT every complement traversal order.

Matched controls receive the same normalized pairs, factor views, cover,
outside attachments, insertions, full-ID output and lookup model:

| Control | Classification and conclusion |
| --- | --- |
| Existing blocked executor | Same state/views; replaces `C_block` with `K<=C_block`, up to constants and paid linear passes. |
| Ordinary factor-local complement BFS | Established primitive instantiated with full global-row charges, early stop and DSU resolved gate. Matches sparse one-factor wins; uncapped replay can lose. Independently implemented below. |
| Seed contraction plus cap/shared fallback | Derived HERE; giving these steps to another engine is a reproduction control, not historical evidence. |
| DCC CC / DFS plus fragment repair | Published base control plus review's fragment bound. Equal `O(F+k)` mutable-state opportunity; preparation and replacement-edge discovery remain paid. No strict time separation from an optimally scheduled repair method is proved. |

The differentiating theorem is the explicit `sum min(q_f,h_f)` query-work
composition against the existing blocked schedule, without expanded negative
occurrences. Elementary reductions and cap amortization do not establish a
new primitive or literature-level composite novelty.

## Independent Tiny Checker

The following standard-library block never imports lead code. An expanded
graph oracle checks all original minimum-ID labels for four schedules:
blocked, ordinary complement, uncapped seeded, and capped seeded. Ordinary
traversal uses all cores, not the seed-class reduction. Completed local
traces are replayed with no-op union callbacks ONLY by the test harness to
measure counterfactual `h_f` and verify the cap identity. Those audit reads
are excluded from algorithm query counters and are not performance claims.

Tiny fixture sources, oracle edges, and output dictionaries are materialized
by the harness, not evidence of a packed builder or RSS bound. Query state
contains only one factor's local marks/links, no negative-occurrence table.

```sh
awk '/^```python$/{p=1;next} p && /^```$/{exit} p' research_algorithms_20260920/Connectivity-Cover-Complement-Study.md | python3 -B -
```

```python
from collections import deque
from itertools import combinations, combinations_with_replacement
from random import Random

independent_checker_total_counts = dict(snapshots=0, labels=0, local_audits=0)


def make_unordered_pair_exact(u, v):
    return (min(u, v), max(u, v))


def enumerate_all_subsets_exact(items):
    return tuple(tuple(x for i, x in enumerate(items) if mask >> i & 1)
                 for mask in range(1 << len(items)))


def expand_base_pairs_exact(factors):
    return {make_unordered_pair_exact(u, v)
            for members in factors for u, v in combinations(members, 2)}


def compute_oracle_labels_exact(vertices, factors, deleted, added):
    edges = (expand_base_pairs_exact(factors) - set(deleted)) | set(added)
    adjacency = {u: [] for u in vertices}
    for u, v in edges:
        adjacency[u].append(v)
        adjacency[v].append(u)
    labels = {}
    for u in vertices:
        if u in labels:
            continue
        reached, stack = {u}, [u]
        while stack:
            for v in adjacency[stack.pop()]:
                if v not in reached:
                    reached.add(v)
                    stack.append(v)
        labels.update((v, min(reached)) for v in reached)
    return labels


def prepare_tiny_source_exact(vertices, factors, deleted, added, cover):
    universe, core = set(vertices), set(cover)
    base = expand_base_pairs_exact(factors)
    assert len(universe) == len(vertices) and len(core) == len(cover)
    assert core <= universe
    assert all(len(set(row)) == len(row) and set(row) <= universe for row in factors)
    assert len(set(deleted)) == len(deleted) and len(set(added)) == len(added)
    assert set(deleted) <= base and not (set(added) & base)
    assert all(u < v and u in universe and v in universe and (u in core or v in core)
               for u, v in (*deleted, *added))
    rows = {u: tuple(f for f, row in enumerate(factors) if u in row) for u in vertices}
    postings = tuple(tuple(u for u in row if u in core) for row in factors)
    failures = {s: tuple(v if s == u else u for u, v in sorted(deleted) if s in (u, v))
                for s in cover}
    inserts = {u: tuple(v if u == s else s for s, v in added if u in (s, v))
               for u in vertices if u not in core}
    core_added = tuple((u, v) for u, v in added if u in core and v in core)
    return vertices, rows, postings, failures, inserts, core_added


class FactorSearchBudgetExhausted(Exception):
    pass


def traverse_factor_complement_exact(blocked, ready, failures, degrees, core, join,
                                     anchor, budget=None, mutant=None):
    counts = dict(opens=0, records=0, tests=0, seeds=0, spent=0)
    queue, seeded = deque(), set()

    def charge_local_event_exact(kind):
        if budget is not None and counts["spent"] == budget:
            raise FactorSearchBudgetExhausted()
        counts[kind] += 1
        counts["spent"] += 1

    def read_deleted_marks_exact(s):
        charge_local_event_exact("opens")
        row = failures[s]
        marks = set()
        for index in range(degrees[s][0]):
            charge_local_event_exact("records")
            neighbor = row[index]
            if neighbor in core:
                marks.add(neighbor)
        return marks

    try:
        if ready and mutant != "omit_seeds":
            for s in blocked:
                attaches = degrees[s][1] < len(ready)
                if not attaches:
                    marks = read_deleted_marks_exact(s)
                    attaches = sum(v in ready for v in marks) < len(ready)
                if attaches or mutant == "all_seeds":
                    join(s, anchor)
                    counts["seeds"] += 1
                    seeded.add(s)
                    queue.append(s)
        remaining = [s for s in blocked if s not in seeded]
        links = {s: remaining[i + 1] if i + 1 < len(remaining) else None
                 for i, s in enumerate(remaining)}
        head = remaining[0] if remaining else None
        while head is not None:
            if not queue:
                s, head = head, links[head]
                queue.append(s)
            if head is None:
                break
            s = queue.popleft()
            marks = read_deleted_marks_exact(s)
            previous, v = None, head
            while v is not None:
                charge_local_event_exact("tests")
                following = links[v]
                if v in marks:
                    previous = v
                else:
                    if previous is None:
                        head = following
                    else:
                        links[previous] = following
                    join(s, v)
                    queue.append(v)
                v = following
        return True, counts
    except FactorSearchBudgetExhausted:
        return False, counts


def compute_query_labels_exact(source, cover, mode, mutant=None):
    vertices, rows, postings, failures, inserts, core_added = source
    core, size = set(cover), len(postings)
    node = {s: size + i for i, s in enumerate(cover)}
    parent = list(range(size + len(cover)))
    rank = [0] * len(parent)
    minimum = [None] * size + list(cover)
    outside = [0] * size
    degrees, deferred = {}, [False] * size
    stats = dict(block=0, J=0, attach_D=0, attempt=0, extra_D=0, opens=0,
                 tests=0, seeds=0, caps=0, fallback=0, fallback_D=0,
                 fallback_opens=0, K=0, virtual=0, slots=len(parent))

    def find_query_root_exact(v):
        while v != parent[v]:
            parent[v] = parent[parent[v]]
            v = parent[v]
        return v

    def reduce_query_minimum_exact(v, value):
        v = find_query_root_exact(v)
        if minimum[v] is None or value < minimum[v]:
            minimum[v] = value

    def merge_query_nodes_exact(u, v):
        u, v = find_query_root_exact(u), find_query_root_exact(v)
        if u == v:
            return
        if rank[u] < rank[v]:
            u, v = v, u
        parent[v] = u
        rank[u] += rank[u] == rank[v]
        if minimum[v] is not None:
            reduce_query_minimum_exact(u, minimum[v])

    for u in vertices:
        if u in core:
            continue
        for f in rows[u]:
            outside[f] += 1
            merge_query_nodes_exact(rows[u][0], f)
        if rows[u]:
            reduce_query_minimum_exact(rows[u][0], u)
        if inserts[u]:
            anchor = rows[u][0] if rows[u] else node[inserts[u][0]]
            for s in inserts[u]:
                merge_query_nodes_exact(anchor, node[s])
            if mutant != "omit_minimum" or rows[u]:
                reduce_query_minimum_exact(anchor, u)
    for u, v in core_added:
        merge_query_nodes_exact(node[u], node[v])

    for s in cover:
        forbidden, d_out, length = set(), 0, 0
        for u in failures[s]:
            length += 1
            stats["attach_D"] += 1
            if u in core:
                forbidden.add(u)
            else:
                d_out += 1
        degrees[s] = (length, len(forbidden))
        uncertain = {f: 0 for f in rows[s] if 0 < outside[f] <= d_out}
        if uncertain:
            for u in failures[s]:
                stats["attach_D"] += 1
                if u not in core:
                    for f in rows[u]:
                        stats["J"] += 1
                        if f in uncertain:
                            uncertain[f] += 1
        for f in rows[s]:
            attaches = outside[f] > d_out or (f in uncertain and uncertain[f] < outside[f])
            if attaches:
                merge_query_nodes_exact(node[s], f)
            elif mode == "blocked":
                for v in postings[f]:
                    stats["block"] += 1
                    if v != s and v not in forbidden:
                        merge_query_nodes_exact(node[s], node[v])

    if mode != "blocked":
        for f, posting in enumerate(postings):
            if len(posting) < 2:
                continue
            if all(find_query_root_exact(node[s]) == find_query_root_exact(node[posting[0]])
                   for s in posting):
                continue
            ready = (set(s for s in posting if find_query_root_exact(node[s]) == find_query_root_exact(f))
                     if outside[f] and mode != "ordinary" else set())
            blocked = tuple(s for s in posting if s not in ready)
            q = len(posting) * len(blocked)

            def merge_local_pair_exact(s, v):
                merge_query_nodes_exact(node[s], f if v is None else node[v])

            done, measured = traverse_factor_complement_exact(
                blocked, ready, failures, degrees, core, merge_local_pair_exact, None,
                q if mode == "capped" else None, mutant)
            # Accounting replay belongs only to the independent test harness.
            unused, full = traverse_factor_complement_exact(
                blocked, ready, failures, degrees, core, lambda s, v: None, None,
                mutant=mutant)
            # These extra source reads verify bounds, not query traffic.
            blocked_set = set(blocked)
            rho = sum(degrees[s][0] for s in blocked)
            internal = sum(v in blocked_set for s in blocked for v in failures[s]) // 2
            assert full["opens"] <= 2 * len(blocked)
            assert full["records"] <= 2 * rho
            assert full["tests"] <= len(blocked) + internal
            assert 2 * full["spent"] <= 6 * len(blocked) + 5 * rho
            independent_checker_total_counts["local_audits"] += 1
            stats["virtual"] += full["spent"]
            stats["K"] += min(q, full["spent"])
            stats["attempt"] += measured["spent"]
            stats["extra_D"] += measured["records"]
            stats["opens"] += measured["opens"]
            stats["tests"] += measured["tests"]
            stats["seeds"] += measured["seeds"]
            if mode == "capped":
                assert measured["spent"] == min(q, full["spent"])
                assert done == (full["spent"] <= q)
            if not done:
                deferred[f] = True
                stats["caps"] += 1

    if any(deferred):
        for s in cover:
            stats["fallback_opens"] += 1
            forbidden = set()
            for u in failures[s]:
                stats["fallback_D"] += 1
                if u in core:
                    forbidden.add(u)
            for f in rows[s]:
                if not deferred[f] or (outside[f] and find_query_root_exact(node[s]) == find_query_root_exact(f)):
                    continue
                for v in postings[f]:
                    stats["fallback"] += 1
                    if v != s and v not in forbidden:
                        merge_query_nodes_exact(node[s], node[v])
    if mode == "capped":
        assert stats["fallback"] <= stats["K"]
        assert stats["attempt"] + stats["fallback"] <= 2 * stats["K"]
        assert stats["fallback_D"] <= sum(len(failures[s]) for s in cover)
    labels = {}
    for u in vertices:
        if u in core:
            anchor = node[u]
        elif rows[u]:
            anchor = rows[u][0]
        elif inserts[u]:
            anchor = node[inserts[u][0]]
        else:
            labels[u] = u
            continue
        labels[u] = minimum[find_query_root_exact(anchor)]
    return labels, stats


def check_snapshot_schedules_exact(vertices, factors, deleted, added, cover):
    source = prepare_tiny_source_exact(vertices, factors, deleted, added, cover)
    expected = compute_oracle_labels_exact(vertices, factors, deleted, added)
    result = {}
    for mode in ("blocked", "ordinary", "raw", "capped"):
        labels, stats = compute_query_labels_exact(source, cover, mode)
        assert labels == expected, (mode, vertices, factors, deleted, added, cover, labels, expected)
        assert stats["slots"] == len(factors) + len(cover)
        independent_checker_total_counts["labels"] += 1
        result[mode] = stats
    core = set(cover)
    negatives = {s: set(source[3][s]) for s in cover}
    block = sum(sum(set(factor) - core <= negatives[s] for s in factor if s in core)
                * len(set(factor) & core) for factor in factors)
    assert result["blocked"]["block"] == block
    assert result["capped"]["K"] <= block
    assert len({stats["J"] for stats in result.values()}) == 1
    assert len({stats["attach_D"] for stats in result.values()}) == 1
    independent_checker_total_counts["snapshots"] += 1
    return result


def run_exhaustive_schedules_exact():
    snapshots = covers = 0
    for n in range(5):
        vertices = (41, 7, 103, 29)[:n]
        subsets = enumerate_all_subsets_exact(vertices)
        pairs = tuple(make_unordered_pair_exact(u, v) for u, v in combinations(vertices, 2))
        for count in range(3):
            for factors in combinations_with_replacement(subsets, count):
                base = expand_base_pairs_exact(factors)
                for current in enumerate_all_subsets_exact(pairs):
                    deleted, added = base - set(current), set(current) - base
                    for cover in subsets:
                        if all(u in cover or v in cover for u, v in deleted | added):
                            check_snapshot_schedules_exact(vertices, factors, deleted, added, cover)
                            covers += 1
                    snapshots += 1
    print("exhaustive snapshots:", snapshots)
    print("exhaustive snapshot/cover checks:", covers)
    print("exhaustive full-label schedule checks:", 4 * covers)


def run_random_schedules_exact():
    random = Random(20260921)
    for unused in range(2000):
        vertices = tuple(random.sample(range(1, 2000), random.randrange(11)))
        factors = tuple(tuple(u for u in vertices if random.randrange(2))
                        for unused_factor in range(random.randrange(9)))
        base = expand_base_pairs_exact(factors)
        current = {make_unordered_pair_exact(u, v) for u, v in combinations(vertices, 2)
                   if random.randrange(2)}
        deleted, added = base - current, current - base
        cover = set()
        edits = sorted(deleted | added)
        random.shuffle(edits)
        for u, v in edits:
            if u not in cover and v not in cover:
                cover.update((u, v))
        cover.update(u for u in vertices if random.randrange(5) == 0)
        check_snapshot_schedules_exact(vertices, factors, deleted, added, tuple(sorted(cover)))
    print("seeded random snapshot/cover checks:", 2000)
    print("seeded random full-label schedule checks:", 8000)


def run_adversarial_schedules_exact():
    fixtures = (
        ("omit_seeds", (10, 20, 1), ((10, 20, 1),), ((1, 10),), (), (10, 20)),
        ("all_seeds", (10, 20, 1), ((10, 20, 1),), ((1, 10), (10, 20)), (), (10, 20)),
        ("omit_minimum", (20, 10, 1), (), (), ((1, 10), (1, 20)), (10, 20)),
    )
    for mutant, *arguments in fixtures:
        check_snapshot_schedules_exact(*arguments)
        source = prepare_tiny_source_exact(*arguments)
        labels, unused = compute_query_labels_exact(source, arguments[-1], "raw", mutant)
        assert labels != compute_oracle_labels_exact(*arguments[:-1]), mutant
    print("targeted mutants rejected:", len(fixtures))
    invalid = (
        ((1, 2), (), (), ((1, 2),), ()),
        ((1, 2), ((1, 2),), ((1, 2), (1, 2)), (), (1,)),
        ((1, 2), ((1, 1, 2),), (), (), ()),
    )
    for arguments in invalid:
        try:
            prepare_tiny_source_exact(*arguments)
        except AssertionError:
            continue
        raise AssertionError("invalid fixture accepted")
    print("invalid fixtures rejected:", len(invalid))
    core = tuple(range(64))
    matching = tuple((s, s + 1) for s in range(0, 64, 2))
    result = check_snapshot_schedules_exact(core, (core,), matching, (), core)
    assert result["blocked"]["block"] == 4096
    assert (result["capped"]["attempt"], result["capped"]["extra_D"],
            result["capped"]["opens"], result["capped"]["tests"]) == (68, 2, 2, 64)
    assert result["ordinary"]["attempt"] == 68
    print("core matching: blocked=4096; capped=ordinary=68; D=2; opens=2; tests=64")
    vertices = core + (1000,)
    result = check_snapshot_schedules_exact(vertices, (vertices,),
        matching + tuple((s, 1000) for s in range(32)), (), core)
    assert result["blocked"]["block"] == 2048
    assert (result["capped"]["attempt"], result["capped"]["seeds"],
            result["capped"]["J"]) == (0, 32, 32)
    print("outside seeding: blocked=2048; capped=0; seeds=32; J=32")
    core, exterior = tuple(range(13)), tuple(range(1000, 1100))
    factors = tuple((0, s) for s in core[1:]) + ((0,) + exterior,)
    result = check_snapshot_schedules_exact(core + exterior, factors,
                                            tuple((0, u) for u in exterior), (), core)
    assert result["raw"]["attempt"] == result["ordinary"]["attempt"] == 1224
    stats = result["capped"]
    assert (stats["attempt"], stats["fallback"], stats["caps"], stats["extra_D"],
            stats["fallback_D"], stats["fallback_opens"], stats["J"]) == (48, 48, 12, 36, 100, 13, 100)
    assert result["blocked"]["block"] == 49
    print("replay loss: raw=ordinary=1224; blocked=49; capped=48+48; caps=12")
    print("replay source ledger: prefix D=36; fallback D=100; fallback opens=13; J=100")
    core = tuple(range(24))
    result = check_snapshot_schedules_exact(core, (core,), tuple(combinations(core, 2)), (), core)
    stats = result["capped"]
    assert result["raw"]["attempt"] == 828 and result["blocked"]["block"] == 576
    assert (stats["attempt"], stats["fallback"], stats["caps"]) == (576, 576, 1)
    print("dense loss: raw=828; blocked=576; capped=576+576; caps=1")
    check_snapshot_schedules_exact((7, 1), ((7, 1), (7, 1)), ((1, 7),), (), (7,))
    check_snapshot_schedules_exact((10, 20, 1, 2), ((10, 1), (20, 2), (10, 20)), (), (), (10, 20))
    check_snapshot_schedules_exact((9, 5, 0, -2), (), (), ((0, 5), (0, 9)), (5, 9))
    print("additional overlap/bridge/isolate regressions:", 3)


run_exhaustive_schedules_exact()
run_random_schedules_exact()
run_adversarial_schedules_exact()
print("total valid snapshot/cover checks:", independent_checker_total_counts["snapshots"])
print("total exact full-label comparisons:", independent_checker_total_counts["labels"])
print("local trace/bound audits (including mutant probes):", independent_checker_total_counts["local_audits"])
print("PASS: independent capped complement schedule checker")
```

## Verification Receipt

Executed the exact final Python block above with Python 3.9.6 on
2026-09-21, using the extraction command above. Exit status 0. Output:

```text
exhaustive snapshots: 10191
exhaustive snapshot/cover checks: 85530
exhaustive full-label schedule checks: 342120
seeded random snapshot/cover checks: 2000
seeded random full-label schedule checks: 8000
targeted mutants rejected: 3
invalid fixtures rejected: 3
core matching: blocked=4096; capped=ordinary=68; D=2; opens=2; tests=64
outside seeding: blocked=2048; capped=0; seeds=32; J=32
replay loss: raw=ordinary=1224; blocked=49; capped=48+48; caps=12
replay source ledger: prefix D=36; fallback D=100; fallback opens=13; J=100
dense loss: raw=828; blocked=576; capped=576+576; caps=1
additional overlap/bridge/isolate regressions: 3
total valid snapshot/cover checks: 87540
total exact full-label comparisons: 350160
local trace/bound audits (including mutant probes): 119392
PASS: independent capped complement schedule checker
```

The exhaustive suite enumerates universe sizes 0 through 4, all multisets
of 0, 1 or 2 factors, every current simple graph, and every valid change
cover. There are 2,000 seeded random cases with up to 10 vertices and 8
factors. The additional 10 valid cases comprise 3 mutant-control fixtures,
4 measured work fixtures and 3 overlap/bridge/isolate regressions. Hence
`85,530+2,000+10=87,540` valid inputs, each checked across four schedules.
The 119,392 local audits also include the deliberately wrong mutant runs;
they check metering bounds, not the mutants' graph correctness. All three
mutants give incorrect labels as required, and all three invalid-source
fixtures are rejected.

For every valid capped execution the checker asserts the exact prefix
identity `attempt=K`, `fallback<=K`, `attempt+fallback<=2K`, `K<=C_block`,
and at most one complete fallback deletion sweep. Local audit replays check
at most two opens/full-row copies per blocked member, the candidate-test
bound, and `2h_f<=6p_f+5rho_f`. The ordinary control matches the 68-event
sparse-clique result. The replay and dense fixtures preserve the losses
rather than presenting only wins.

These counts are logical events, not time or disk-byte measurements.
In particular, `capped=0` in the seed fixture excludes its paid setup,
32 seed unions, attachment work and full output. No production test,
bounded physical builder, or memory benchmark is inferred. Only this
Markdown was edited; no commits or pushes were performed.

## Remaining Gate

The query-schedule bound is the positive result. No exclusive RAM theorem,
latency theorem, or priority claim over a literature-matched composite is
established. Production integration needs bounded cancellable row providers,
byte admission, snapshot validation and source/page measurements; none is
implemented here or silently assigned to the lead.
