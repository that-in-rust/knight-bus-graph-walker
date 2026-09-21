# Connectivity And Paths Evidence

Date: 2026-09-20. Research sidecar only. No production files changed, no new code project, no 4 GB benchmark. Programs ran ephemerally through `python3 - <<'PY'` in the repository working directory; they do not read or write files. Python integers provide exact arithmetic; `float("inf")` is only an unreachable sentinel, never an approximate finite weight. Full runnable commands follow.

## Results

| Probe | Scope | Actual result |
| --- | --- | --- |
| WCC witness-hit debit | All five-vertex simple undirected graphs and every subset of their edges deleted; k=1,2,3 | 177,147 tests; 28,624 accepted; zero wrong partitions |
| WCC beyond total failure budget | Same enumeration | 3,792 accepts have total deletions >= k, but fewer than k deleted certificate edges |
| WCC changed answers | Same enumeration | 8,495 accepted cases have a partition different from the original |
| Laminar cover | All 4^6=4,096 assignments absent/surviving/deleted/inserted to four-vertex pairs; fixed eight-leaf hierarchy, differing certificate orders/costs | Zero mismatches against enumerated legal covers; zero partition mismatches |
| Active forest-fragment quotient | All 4,096 four-vertex graph/edit states, cross-shard certificates, permuted original IDs | Zero partition/minimum mismatches; 3,329 q<n cases; 108 q=0 cases |
| Nested-cut interval sweep | 720 increasing-label seven-vertex trees, all 64 cut subsets, all seven owners | 46,080 tree/cut cases; 322,560 owner checks; zero mismatches |
| Weighted path single edit | All 729 three-vertex directed graphs with absent/0/1 arcs, all anchors, every single arc change, all query sources | 78,732 trials; 70,470 admitted; 8,262 negative-reduced-cost rejections; 28,458 admitted strict portal-state reductions |
| Weighted path batches | 3,000 seeded eight-vertex instances; 1-8 edit attempts; all eight sources | 24,000 trials; 15,528 admitted; 8,472 rejected; 6,535 admitted strict state reductions |
| Unweighted traversal | Every four-vertex loop-free directed graph, every anchor, every source | 65,536 comparisons; 32,768 strict state reductions |
| Path correctness and witnesses | All admitted cases above | Zero distance/reachability mismatches; zero invalid, cyclic, or wrong-length returned parent chains |
| Dense forward fracture | Twelve-vertex chain plus all heavier forward shortcuts, delete 5->6 | Gate count rises from one at source 0 before fracture to seven; new distances downstream increase by three |
| Source-row promotion | All four-vertex unweighted digraphs with anchor zero and all sources | 16,384 ledger/owner comparisons against full source-specific rebuilding; zero mismatches |
| Dormant quotient self-arc | Chain 0->1->2 plus 2->0 weight ten | Source 2 needs distances (10,11,0); deleting dormant arc loses reachability |

Path distance total: 168,268 trials, 151,534 admitted and independently checked, 16,734 fast-path rejections. The 16,384 source-overlay structural checks are additional, not included in that distance total. A rejected case was not validated as a fast-path answer. Floyd-Warshall is the independent distance oracle; quotient execution uses a heap. WCC truth uses graph traversal; witnesses use forest packing.

The tests are not distributional estimates of production acceptance, independent peer review, formal machine proofs, performance measurements, stable-ID/CDC tests, allocator tests, overflow tests, or evidence that preparation finishes under 4 GB.

Final verification extracted and replayed all six shell-code blocks directly from this Markdown file; all exited zero and reproduced the recorded outputs. ASCII/trailing-whitespace/fence checks passed for all three owned documents, and all eight local document links resolved. No production tests/builds were run because this sidecar changed only research Markdown.

## WCC Command

```sh
python3 - <<'PY'
from itertools import combinations,product
from collections import Counter

def compute_partition_oracle_exact(n,edges):
    adj=[[] for _ in range(n)]
    for u,v in edges: adj[u].append(v); adj[v].append(u)
    labels=[-1]*n
    for root in range(n):
        if labels[root] >= 0: continue
        labels[root]=root
        stack=[root]
        while stack:
            u=stack.pop()
            for v in adj[u]:
                if labels[v] < 0: labels[v]=root; stack.append(v)
    return labels

def pack_residual_forests_exact(n,edges,k):
    remaining=set(edges)
    certificate=set()
    for _ in range(k):
        parent=list(range(n))
        def find_vertex_root_exact(v):
            while parent[v] != v:
                parent[v]=parent[parent[v]]
                v=parent[v]
            return v
        selected=set()
        for u,v in sorted(remaining):
            a,b=find_vertex_root_exact(u),find_vertex_root_exact(v)
            if a != b: parent[a]=b; selected.add((u,v))
        certificate |= selected
        remaining -= selected
    return certificate

counts=Counter()
pairs=list(combinations(range(5),2))
for state in product((0,1,2),repeat=len(pairs)):
    old={e for e,x in zip(pairs,state) if x}
    deleted={e for e,x in zip(pairs,state) if x==2}
    truth=compute_partition_oracle_exact(5,old-deleted)
    original=compute_partition_oracle_exact(5,old)
    for k in (1,2,3):
        cert=pack_residual_forests_exact(5,old,k)
        counts["trials"]+=1
        if len(deleted & cert) < k:
            counts["accepted"]+=1
            counts["accepted_over_total_failure_budget"]+=len(deleted)>=k
            counts["accepted_changed_partitions"]+=truth!=original
            assert compute_partition_oracle_exact(5,cert-deleted)==truth
print("witness_hit_debit",dict(sorted(counts.items())),"mismatches=0")

# Eight fixed ID slots, six possible edges, binary laminar tree with 26 covers.
pairs=list(combinations(range(4),2))
counts=Counter()
for state in product((0,1,2,3),repeat=6):
    old={e for e,x in zip(pairs,state) if x in (1,2)}
    deleted={e for e,x in zip(pairs,state) if x==2}
    inserted={e for e,x in zip(pairs,state) if x==3}
    nodes={}
    def build_laminar_tree_exact(lo,hi):
        edges={pairs[i] for i in range(lo,min(hi,6)) if pairs[i] in old}
        k=1+(lo+hi)%2
        cert=pack_residual_forests_exact(4,edges,k)
        own=2+3*len(cert)
        valid=len(cert & deleted)<k
        if hi-lo==1:
            # A raw leaf is always exact, even if its certificate is unusable.
            raw_cost=4+5*len(edges)
            options=[(raw_cost,edges-deleted)]
            if valid: options.append((own,cert-deleted))
            best=min(options,key=lambda x:x[0])
        else:
            mid=(lo+hi)//2
            left=build_laminar_tree_exact(lo,mid)
            right=build_laminar_tree_exact(mid,hi)
            options=[(a+b,x|y) for a,x in left for b,y in right]
            if valid: options.append((own,cert-deleted))
            split=nodes[lo,mid][0]+nodes[mid,hi][0]
            best=(split,nodes[lo,mid][1]|nodes[mid,hi][1])
            if valid and own<=split: best=(own,cert-deleted)
        nodes[lo,hi]=best
        return options
    alternatives=build_laminar_tree_exact(0,8)
    chosen=nodes[0,8]
    counts["trials"]+=1
    assert chosen[0]==min(cost for cost,edges in alternatives)
    assert compute_partition_oracle_exact(4,chosen[1]|inserted)==compute_partition_oracle_exact(4,(old-deleted)|inserted)
print("laminar_cover",dict(counts),"cost_mismatches=0 partition_mismatches=0")

# A false certificate from restarting debt each generation.
old={(0,1),(0,2),(1,2)}
cert=pack_residual_forests_exact(3,old,1)
removed=next(iter(cert))
assert compute_partition_oracle_exact(3,cert-{removed})!=compute_partition_oracle_exact(3,old-{removed})
print("debt_reset_counterexample",sorted(cert),"deleted",removed,"must_not_reset_debt=confirmed")
PY
```

Observed stdout:

```text
witness_hit_debit {'accepted': 28624, 'accepted_changed_partitions': 8495, 'accepted_over_total_failure_budget': 3792, 'trials': 177147} mismatches=0
laminar_cover {'trials': 4096} cost_mismatches=0 partition_mismatches=0
debt_reset_counterexample [(0, 1), (0, 2)] deleted (0, 1) must_not_reset_debt=confirmed
```

The final counterexample is a single failed forest witness, followed conceptually by an empty update interval. Resetting its debt for that empty interval would falsely revive the old certificate. It does not implement a storage engine.

## Weighted Single-Edit Command

```sh
python3 - <<'PY'
from itertools import combinations, product
from heapq import heappop, heappush
from collections import Counter, deque

def compute_all_pairs_exact(n, edges):
    d = [[float("inf")]*n for _ in range(n)]
    for v in range(n): d[v][v] = 0
    for (u,v),w in edges.items(): d[u][v] = min(d[u][v],w)
    for k in range(n):
        for u in range(n):
            for v in range(n): d[u][v] = min(d[u][v],d[u][k]+d[k][v])
    return d

def prepare_tight_forest_exact(n, old, root):
    d = compute_all_pairs_exact(n,old)[root]
    cap = max(x for x in d if x != float("inf"))
    h = [cap if x == float("inf") else x for x in d]
    parent = [-1]*n
    seen = {root}
    queue = deque([root])
    while queue:
        u = queue.popleft()
        for v in range(n):
            if (u,v) in old and v not in seen and d[u]+old[u,v] == d[v]:
                parent[v] = u
                seen.add(v)
                queue.append(v)
    return h,parent

def solve_fractured_portal_exact(n, current, h, parent, source):
    if any(w+h[u]-h[v] < 0 for (u,v),w in current.items()): return None
    forest = [u if u >= 0 and (u,v) in current and current[u,v]+h[u] == h[v] else -1 for v,u in enumerate(parent)]
    def check_forest_ancestor_exact(u,v):
        while v >= 0:
            if u == v: return True
            v = forest[v]
        return False
    exceptions = {(u,v):w for (u,v),w in current.items() if not check_forest_ancestor_exact(u,v)}
    gates = {v for v in range(n) if forest[v] < 0} | {source} | {v for u,v in exceptions}
    owner = []
    for v in range(n):
        while v not in gates: v = forest[v]
        owner.append(v)
    arcs = [[] for _ in range(n)]
    for (u,v),w in current.items():
        if (forest[v] == u or (u,v) in exceptions) and owner[u] != owner[v]:
            assert v in gates
            arcs[owner[u]].append((owner[v],w+h[u]-h[v],u,v))
    delta = [float("inf")]*n
    delta[source] = 0
    queue = [(0,source)]
    done = set()
    entry = {}
    while queue:
        distance,u = heappop(queue)
        if u in done: continue
        done.add(u)
        for v,w,x,y in arcs[u]:
            if v not in done and distance+w < delta[v]:
                delta[v] = distance+w
                entry[v] = (x,y)
                heappush(queue,(delta[v],v))
    answer = [delta[owner[v]]+h[v]-h[source] for v in range(n)]
    for target in range(n):
        if answer[target] == float("inf"): continue
        v,total,seen = target,0,set()
        while v != source:
            assert v not in seen
            seen.add(v)
            u = entry[v][0] if v in gates else forest[v]
            assert (u,v) in current
            total += current[u,v]
            v = u
        assert total == answer[target]
    return answer,len(gates)

counts = Counter()
n = 3
pairs = [(u,v) for u in range(n) for v in range(n) if u != v]
for state in product((-1,0,1),repeat=len(pairs)):
    old = {e:w for e,w in zip(pairs,state) if w >= 0}
    for anchor in range(n):
        h,parent = prepare_tight_forest_exact(n,old,anchor)
        for index,e in enumerate(pairs):
            for new_weight in (-1,0,1):
                if new_weight == state[index]: continue
                current = dict(old)
                if new_weight < 0: current.pop(e,None)
                else: current[e] = new_weight
                truth = compute_all_pairs_exact(n,current)
                for source in range(n):
                    counts["trials"] += 1
                    actual = solve_fractured_portal_exact(n,current,h,parent,source)
                    if actual is None:
                        counts["potential_rejections"] += 1
                        continue
                    counts["accepted"] += 1
                    counts["strict_state_reductions"] += actual[1] < n
                    assert actual[0] == truth[source],(old,current,anchor,source,actual,truth)
print("weighted_single_edit",dict(sorted(counts.items())),"mismatches=0 witness_failures=0")
n=12
old={(u,v):v-u+(0 if v==u+1 else 3) for u in range(n) for v in range(u+1,n)}
h,parent=prepare_tight_forest_exact(n,old,0)
current=dict(old)
del current[5,6]
for source in (0,4,6):
    actual=solve_fractured_portal_exact(n,current,h,parent,source)
    assert actual[0] == compute_all_pairs_exact(n,current)[source]
    print("dense_forward_fracture",source,"gates",actual[1],"distances",actual[0])
PY
```

Observed stdout:

```text
weighted_single_edit {'accepted': 70470, 'potential_rejections': 8262, 'strict_state_reductions': 28458, 'trials': 78732} mismatches=0 witness_failures=0
dense_forward_fracture 0 gates 7 distances [0, 1, 2, 3, 4, 5, 9, 10, 11, 12, 13, 14]
dense_forward_fracture 4 gates 8 distances [inf, inf, inf, inf, 0, 1, 5, 6, 7, 8, 9, 10]
dense_forward_fracture 6 gates 7 distances [inf, inf, inf, inf, inf, inf, 0, 1, 2, 3, 4, 5]
```

## Batch And BFS Command

This standalone command repeats the preparation, quotient, and oracle functions; it does not run the single-edit loops.

```sh
python3 - <<'PY'
from itertools import combinations, product
from heapq import heappop, heappush
from collections import Counter, deque

def compute_all_pairs_exact(n, edges):
    d = [[float("inf")]*n for _ in range(n)]
    for v in range(n): d[v][v] = 0
    for (u,v),w in edges.items(): d[u][v] = min(d[u][v],w)
    for k in range(n):
        for u in range(n):
            for v in range(n): d[u][v] = min(d[u][v],d[u][k]+d[k][v])
    return d

def prepare_tight_forest_exact(n, old, root):
    d = compute_all_pairs_exact(n,old)[root]
    cap = max(x for x in d if x != float("inf"))
    h = [cap if x == float("inf") else x for x in d]
    parent = [-1]*n
    seen = {root}
    queue = deque([root])
    while queue:
        u = queue.popleft()
        for v in range(n):
            if (u,v) in old and v not in seen and d[u]+old[u,v] == d[v]:
                parent[v] = u
                seen.add(v)
                queue.append(v)
    return h,parent

def solve_fractured_portal_exact(n, current, h, parent, source):
    if any(w+h[u]-h[v] < 0 for (u,v),w in current.items()): return None
    forest = [u if u >= 0 and (u,v) in current and current[u,v]+h[u] == h[v] else -1 for v,u in enumerate(parent)]
    def check_forest_ancestor_exact(u,v):
        while v >= 0:
            if u == v: return True
            v = forest[v]
        return False
    exceptions = {(u,v):w for (u,v),w in current.items() if not check_forest_ancestor_exact(u,v)}
    gates = {v for v in range(n) if forest[v] < 0} | {source} | {v for u,v in exceptions}
    owner = []
    for v in range(n):
        while v not in gates: v = forest[v]
        owner.append(v)
    arcs = [[] for _ in range(n)]
    for (u,v),w in current.items():
        if (forest[v] == u or (u,v) in exceptions) and owner[u] != owner[v]:
            assert v in gates
            arcs[owner[u]].append((owner[v],w+h[u]-h[v],u,v))
    delta = [float("inf")]*n
    delta[source] = 0
    queue = [(0,source)]
    done = set()
    entry = {}
    while queue:
        distance,u = heappop(queue)
        if u in done: continue
        done.add(u)
        for v,w,x,y in arcs[u]:
            if v not in done and distance+w < delta[v]:
                delta[v] = distance+w
                entry[v] = (x,y)
                heappush(queue,(delta[v],v))
    answer = [delta[owner[v]]+h[v]-h[source] for v in range(n)]
    for target in range(n):
        if answer[target] == float("inf"): continue
        v,total,seen = target,0,set()
        while v != source:
            assert v not in seen
            seen.add(v)
            u = entry[v][0] if v in gates else forest[v]
            assert (u,v) in current
            total += current[u,v]
            v = u
        assert total == answer[target]
    return answer,len(gates)


import random
counts=Counter()
rng=random.Random(20260920)
for trial in range(3000):
    n=8
    old={(u,v):rng.randrange(10) for u in range(n) for v in range(n) if u!=v and rng.randrange(5)==0}
    anchor=rng.randrange(n)
    h,parent=prepare_tight_forest_exact(n,old,anchor)
    current=dict(old)
    for _ in range(1+rng.randrange(8)):
        u,v=rng.sample(range(n),2)
        if rng.randrange(3)==0: current.pop((u,v),None)
        else: current[u,v]=rng.randrange(10)
    truth=compute_all_pairs_exact(n,current)
    for source in range(n):
        counts["trials"]+=1
        actual=solve_fractured_portal_exact(n,current,h,parent,source)
        if actual is None: counts["potential_rejections"]+=1; continue
        counts["accepted"]+=1
        counts["strict_state_reductions"]+=actual[1]<n
        assert actual[0]==truth[source]
print("seeded_weighted_batches",dict(sorted(counts.items())),"mismatches=0 witness_failures=0")
counts=Counter()
n=4
pairs=[(u,v) for u in range(n) for v in range(n) if u!=v]
for state in product((0,1),repeat=len(pairs)):
    old={e:1 for e,x in zip(pairs,state) if x}
    truth=compute_all_pairs_exact(n,old)
    for anchor in range(n):
        h,parent=prepare_tight_forest_exact(n,old,anchor)
        for source in range(n):
            actual=solve_fractured_portal_exact(n,old,h,parent,source)
            counts["trials"]+=1
            counts["strict_state_reductions"]+=actual[1]<n
            assert actual[0]==truth[source]
print("unweighted_all_graphs",dict(sorted(counts.items())),"mismatches=0 witness_failures=0")

PY
```

Observed stdout:

```text
seeded_weighted_batches {'accepted': 15528, 'potential_rejections': 8472, 'strict_state_reductions': 6535, 'trials': 24000} mismatches=0 witness_failures=0
unweighted_all_graphs {'strict_state_reductions': 32768, 'trials': 65536} mismatches=0 witness_failures=0
```

## Source Promotion Command

This compares the reusable tail-aware ledger and one-row source overlay against a freshly built source-specific quotient. It checks effective owners and the complete multiset of effective ledger arcs, not only distances.

```sh
python3 - <<'PY'
from itertools import combinations, product
from heapq import heappop, heappush
from collections import Counter, deque

def compute_all_pairs_exact(n, edges):
    d = [[float("inf")]*n for _ in range(n)]
    for v in range(n): d[v][v] = 0
    for (u,v),w in edges.items(): d[u][v] = min(d[u][v],w)
    for k in range(n):
        for u in range(n):
            for v in range(n): d[u][v] = min(d[u][v],d[u][k]+d[k][v])
    return d

def prepare_tight_forest_exact(n, old, root):
    d = compute_all_pairs_exact(n,old)[root]
    cap = max(x for x in d if x != float("inf"))
    h = [cap if x == float("inf") else x for x in d]
    parent = [-1]*n
    seen = {root}
    queue = deque([root])
    while queue:
        u = queue.popleft()
        for v in range(n):
            if (u,v) in old and v not in seen and d[u]+old[u,v] == d[v]:
                parent[v] = u
                seen.add(v)
                queue.append(v)
    return h,parent

def solve_fractured_portal_exact(n, current, h, parent, source):
    if any(w+h[u]-h[v] < 0 for (u,v),w in current.items()): return None
    forest = [u if u >= 0 and (u,v) in current and current[u,v]+h[u] == h[v] else -1 for v,u in enumerate(parent)]
    def check_forest_ancestor_exact(u,v):
        while v >= 0:
            if u == v: return True
            v = forest[v]
        return False
    exceptions = {(u,v):w for (u,v),w in current.items() if not check_forest_ancestor_exact(u,v)}
    gates = {v for v in range(n) if forest[v] < 0} | {source} | {v for u,v in exceptions}
    owner = []
    for v in range(n):
        while v not in gates: v = forest[v]
        owner.append(v)
    arcs = [[] for _ in range(n)]
    for (u,v),w in current.items():
        if (forest[v] == u or (u,v) in exceptions) and owner[u] != owner[v]:
            assert v in gates
            arcs[owner[u]].append((owner[v],w+h[u]-h[v],u,v))
    delta = [float("inf")]*n
    delta[source] = 0
    queue = [(0,source)]
    done = set()
    entry = {}
    while queue:
        distance,u = heappop(queue)
        if u in done: continue
        done.add(u)
        for v,w,x,y in arcs[u]:
            if v not in done and distance+w < delta[v]:
                delta[v] = distance+w
                entry[v] = (x,y)
                heappush(queue,(delta[v],v))
    answer = [delta[owner[v]]+h[v]-h[source] for v in range(n)]
    for target in range(n):
        if answer[target] == float("inf"): continue
        v,total,seen = target,0,set()
        while v != source:
            assert v not in seen
            seen.add(v)
            u = entry[v][0] if v in gates else forest[v]
            assert (u,v) in current
            total += current[u,v]
            v = u
        assert total == answer[target]
    return answer,len(gates)

from itertools import product
from collections import Counter

def materialize_portal_layout_exact(n,current,h,parent,source=None):
    forest=[u if u>=0 and (u,v) in current and current[u,v]+h[u]==h[v] else -1 for v,u in enumerate(parent)]
    def check_forest_ancestor_exact(u,v):
        while v>=0:
            if u==v: return True
            v=forest[v]
        return False
    exceptions={(u,v):w for (u,v),w in current.items() if not check_forest_ancestor_exact(u,v)}
    gates={v for v in range(n) if forest[v]<0}|{v for u,v in exceptions}
    if source is not None: gates.add(source)
    owner=[]
    for v in range(n):
        while v not in gates: v=forest[v]
        owner.append(v)
    ledger=[(u,v,w+h[u]-h[v]) for (u,v),w in exceptions.items()]
    ledger += [(u,v,0) for v,u in enumerate(forest) if u>=0 and owner[u]!=owner[v]]
    return forest,gates,owner,ledger,check_forest_ancestor_exact

counts=Counter()
n=4
pairs=[(u,v) for u in range(n) for v in range(n) if u!=v]
for state in product((0,1),repeat=len(pairs)):
    graph={e:1 for e,x in zip(pairs,state) if x}
    h,parent=prepare_tight_forest_exact(n,graph,0)
    forest,gates,owner,ledger,ancestor=materialize_portal_layout_exact(n,graph,h,parent)
    for source in range(n):
        promoted=source not in gates
        effective=[source if promoted and owner[v]==owner[source] and ancestor(source,v) else owner[v] for v in range(n)]
        overlaid=[(effective[u],effective[v],r) for u,v,r in ledger if effective[u]!=effective[v]]
        if promoted: overlaid.append((effective[forest[source]],source,0))
        f2,g2,o2,l2,_=materialize_portal_layout_exact(n,graph,h,parent,source)
        rebuilt=[(o2[u],o2[v],r) for u,v,r in l2 if o2[u]!=o2[v]]
        assert effective==o2 and sorted(overlaid)==sorted(rebuilt)
        counts["trials"]+=1
print("source_overlay_vs_rebuild",dict(counts),"mismatches=0")
graph={(0,1):1,(1,2):1,(2,0):10}
h,parent=prepare_tight_forest_exact(3,graph,0)
f,g,o,ledger,_=materialize_portal_layout_exact(3,graph,h,parent)
print("dormant_loop_counterexample","gates",sorted(g),"ledger",ledger,
      "source2_distances",solve_fractured_portal_exact(3,graph,h,parent,2)[0],
      "dropping_loop_loses_reachability_to_0=confirmed")

PY
```

Observed stdout:

```text
source_overlay_vs_rebuild {'trials': 16384} mismatches=0
dormant_loop_counterexample gates [0] ledger [(2, 0, 12)] source2_distances [10, 11, 0] dropping_loop_loses_reachability_to_0=confirmed
```

## Fragment Quotient Command

Tiny oracle programs below intentionally use n-sized Python lists and recursive traversal. They test mathematical identities only; they are NOT the proposed bounded-RAM storage implementation. The solver retains only active fragments in its union dictionary, but its helper/oracle arrays and final answer list are in memory. The document's disk-only untouched-root and output-stream claims are proved at the procedure level, not measured here.

The exhaustive four-vertex cases use certificate orders one and two on alternating edge-ID shards. These shards do not align with the global forest. External IDs are permuted so minimum original ID is not interchangeable with an internal root or Euler position.

```sh
python3 - <<'PY'
from itertools import combinations,product
from collections import Counter

def compute_partition_oracle_exact(n,edges):
    adj=[[] for _ in range(n)]
    for u,v in edges: adj[u].append(v); adj[v].append(u)
    labels=[-1]*n
    for root in range(n):
        if labels[root] >= 0: continue
        labels[root]=root
        stack=[root]
        while stack:
            u=stack.pop()
            for v in adj[u]:
                if labels[v] < 0: labels[v]=root; stack.append(v)
    return labels

def pack_residual_forests_exact(n,edges,k):
    remaining=set(edges)
    certificate=set()
    for _ in range(k):
        parent=list(range(n))
        def find_vertex_root_exact(v):
            while parent[v] != v:
                parent[v]=parent[parent[v]]
                v=parent[v]
            return v
        selected=set()
        for u,v in sorted(remaining):
            a,b=find_vertex_root_exact(u),find_vertex_root_exact(v)
            if a != b: parent[a]=b; selected.add((u,v))
        certificate |= selected
        remaining -= selected
    return certificate

from itertools import combinations,product
from collections import Counter

def build_original_forest_exact(n,edges):
    adj=[[] for _ in range(n)]
    for u,v in sorted(edges): adj[u].append(v); adj[v].append(u)
    parent=[-1]*n
    base_root=[-1]*n
    order=[]
    entry=[-1]*n
    end=[-1]*n
    def visit_forest_vertex_exact(v,root):
        base_root[v]=root
        entry[v]=len(order); order.append(v)
        for u in adj[v]:
            if base_root[u]<0:
                parent[u]=v
                visit_forest_vertex_exact(u,root)
        end[v]=len(order)
    for v in range(n):
        if base_root[v]<0: visit_forest_vertex_exact(v,v)
    return parent,base_root,order,entry,end

def solve_sparse_fragments_exact(n,old,deleted,inserted,selected,external):
    parent,roots,order,tin,tout=build_original_forest_exact(n,old)
    cuts={v for v in range(n) if parent[v]>=0 and tuple(sorted((v,parent[v]))) in deleted}
    damaged={roots[v] for v in cuts}
    active_roots=damaged|{roots[v] for e in inserted for v in e}
    active_fragments=cuts|active_roots
    assert len(active_fragments)<=2*len(cuts)+2*len(inserted)
    descriptors=sorted(cuts,key=lambda v:tin[v])
    # Tiny probe uses ancestor filtering, equivalent to interval sweep/predecessor lookup.
    def find_current_fragment_exact(v):
        containing=[c for c in descriptors if tin[c]<=tin[v]<tout[c]]
        return max(containing,key=lambda c:tin[c]) if containing else roots[v]
    uf={v:v for v in active_fragments}
    def find_active_root_exact(v):
        while uf[v]!=v: v=uf[v]
        return v
    def union_active_pair_exact(u,v):
        a,b=find_active_root_exact(u),find_active_root_exact(v)
        if a!=b: uf[a]=b
    for u,v in selected|inserted:
        if roots[u] not in active_roots:
            assert roots[u]==roots[v] and roots[v] not in active_roots
            continue
        union_active_pair_exact(find_current_fragment_exact(u),find_current_fragment_exact(v))
    minima={}
    old_min={}
    for v in range(n):
        old_min[roots[v]]=min(old_min.get(roots[v],external[v]),external[v])
        if roots[v] in active_roots:
            representative=find_active_root_exact(find_current_fragment_exact(v))
            minima[representative]=min(minima.get(representative,external[v]),external[v])
    answer=[minima[find_active_root_exact(find_current_fragment_exact(v))] if roots[v] in active_roots else old_min[roots[v]] for v in range(n)]
    truth=compute_partition_oracle_exact(n,(old-deleted)|inserted)
    truth_min={c:min(external[v] for v in range(n) if truth[v]==c) for c in set(truth)}
    assert answer==[truth_min[c] for c in truth]
    return len(active_fragments)

counts=Counter()
pairs=list(combinations(range(4),2))
for state in product((0,1,2,3),repeat=6):
    old={e for e,x in zip(pairs,state) if x in (1,2)}
    deleted={e for e,x in zip(pairs,state) if x==2}
    inserted={e for e,x in zip(pairs,state) if x==3}
    selected=set()
    # Certificates from disjoint edge shards, deliberately unrelated to the global forest.
    for k,shard in enumerate((set(pairs[::2]),set(pairs[1::2])),start=1):
        raw=old&shard
        cert=pack_residual_forests_exact(4,raw,k)
        selected |= (cert if len(cert&deleted)<k else raw)-deleted
    q=solve_sparse_fragments_exact(4,old,deleted,inserted,selected,[91,7,55,2])
    counts["trials"]+=1
    counts["no_active_state"]+=q==0
    counts["strict_state_reductions"]+=q<4
print("fragment_quotient",dict(sorted(counts.items())),"partition_or_minimum_mismatches=0")
n=9
old={(i,i+1) for i in range(5)}
deleted={(0,1),(2,3)}
inserted={(2,4),(6,8)}
q=solve_sparse_fragments_exact(n,old,deleted,inserted,old-deleted,[90,80,2,70,1,60,50,4,3])
print("nested_cuts_isolates","active_fragments",q,"canonical_minimum_test=passed")

PY
```

Observed stdout:

```text
fragment_quotient {'no_active_state': 108, 'strict_state_reductions': 3329, 'trials': 4096} partition_or_minimum_mismatches=0
nested_cuts_isolates active_fragments 5 canonical_minimum_test=passed
```

## Nested Interval Command

This enumerates all 720 increasing-label rooted trees on seven vertices (parent of i is chosen from 0..i-1), every subset of their six edges cut, and every vertex. It is not an enumeration of all labeled rooted trees. It tests event-pop ordering, nested containment, holes, predecessor ownership, and the 2d+1 span bound against direct ancestor containment.

```sh
python3 - <<'PY'
from itertools import combinations,product
from collections import Counter

def build_original_forest_exact(n,edges):
    adj=[[] for _ in range(n)]
    for u,v in sorted(edges): adj[u].append(v); adj[v].append(u)
    parent=[-1]*n
    base_root=[-1]*n
    order=[]
    entry=[-1]*n
    end=[-1]*n
    def visit_forest_vertex_exact(v,root):
        base_root[v]=root
        entry[v]=len(order); order.append(v)
        for u in adj[v]:
            if base_root[u]<0:
                parent[u]=v
                visit_forest_vertex_exact(u,root)
        end[v]=len(order)
    for v in range(n):
        if base_root[v]<0: visit_forest_vertex_exact(v,v)
    return parent,base_root,order,entry,end

from itertools import product
from bisect import bisect_right

def build_cut_intervals_exact(n, cuts, tin, tout):
    starts={tin[c]:c for c in cuts}
    ends={}
    for c in cuts: ends.setdefault(tout[c],[]).append(c)
    boundaries=sorted({0,n}|set(starts)|set(ends))
    stack=[]
    intervals=[]
    for lo,hi in zip(boundaries,boundaries[1:]):
        while stack and tout[stack[-1]]<=lo: stack.pop()
        if lo in starts: stack.append(starts[lo])
        intervals.append((lo,hi,stack[-1] if stack else 0))
    assert len(intervals)<=2*len(cuts)+1
    return intervals

trials=0
vertices=0
n=7
for choices in product(*(range(i) for i in range(1,n))):
    edges={(choices[v-1],v) for v in range(1,n)}
    parent,roots,order,tin,tout=build_original_forest_exact(n,edges)
    for mask in range(1<<(n-1)):
        cuts={v for v in range(1,n) if mask&(1<<(v-1))}
        intervals=build_cut_intervals_exact(n,cuts,tin,tout)
        starts=[lo for lo,_,_ in intervals]
        for v in range(n):
            index=bisect_right(starts,tin[v])-1
            owner=intervals[index][2]
            containing=[c for c in cuts if tin[c]<=tin[v]<tout[c]]
            expected=max(containing,key=lambda c:tin[c]) if containing else 0
            assert owner==expected
            vertices+=1
        trials+=1
print("euler_interval_sweep","tree_cut_cases",trials,"vertex_owner_checks",vertices,"mismatches=0")
edges={(0,1),(0,6),(1,2),(1,4),(2,3),(4,5),(6,7),(6,8)}
parent,roots,order,tin,tout=build_original_forest_exact(9,edges)
intervals=build_cut_intervals_exact(9,{1,2},tin,tout)
print("nested_holes","preorder",order,"intervals",intervals)

PY
```

Observed stdout:

```text
euler_interval_sweep tree_cut_cases 46080 vertex_owner_checks 322560 mismatches=0
nested_holes preorder [0, 1, 2, 3, 4, 5, 6, 7, 8] intervals [(0, 1, 0), (1, 2, 1), (2, 4, 2), (4, 6, 1), (6, 9, 0)]
```

## Inspection Ledger

Primary literature was inspected selectively, not claimed read cover-to-cover:

- [Nagamochi and Ibaraki, Algorithmica 1992](https://gi.cebitec.uni-bielefeld.de/_media/teaching/2013summer/936nagamochi-ibaraki-1992-sparse-k-connected-subgraph.pdf): primary PDF opening and published scope; subsequent targeted PDF fetches were unreliable. The residual-forest cut argument is supplied independently in Witness-Connectivity, not attributed to an uninspected implementation.
- [Eppstein, Galil, Italiano, Improved Sparsification, 1993](https://ics.uci.edu/~eppstein/pubs/EppGalIta-TR-93-20.pdf): Section 3 definitions of strong/stable certificates and tree updates, introduction and Section 4 opening. This is the main WCC novelty challenge.
- [Duan and Pettie, Connectivity Oracles for Graphs Subject to Vertex Failures](https://arxiv.org/pdf/1607.06865): Section 2 through Corollary 2.2's proof, including Figure 1. This directly anticipates Euler-interval fragments and their connectivity quotient; it is the strongest challenge to the optional WCC state-reduction claim.
- [Patrascu and Thorup, Planning for Fast Connectivity Updates](https://people.csail.mit.edu/mip/papers/edgedel/paper.pdf): Sections 1-3 opening, expander recovery Lemmas 1-2 and Theorem 3, insertion handling, and the preparation/approximation distinction. It is the broader batch-connectivity comparator, not the source of the Euler-interval mechanism.
- [Cheng et al., VC-index, SIGMOD 2012](https://www.cse.cuhk.edu.hk/~jcheng/papers/VCindex_sigmod12.pdf): Sections 3-5, Algorithms 1-5, distance-graph Theorem 3 and two-hop Lemma 3. Both simple-index limitations and the actual hierarchical solution were inspected.
- [Bonnet et al., ICALP 2026](https://drops.dagstuhl.de/storage/00lipics/lipics-vol374-icalp2026/html/LIPIcs.ICALP.2026.40/LIPIcs.ICALP.2026.40.html): Theorems 1-2, dynamic Theorem 12, definitions in Section 2, and Section 3 Lemmas 24-27 and rectangle proof. This is not merely an abstract citation.
- [Eppstein, Finding the k Shortest Paths](https://www.ics.uci.edu/~eppstein/pubs/Epp-SJC-98.pdf): Section 2, reduced-cost Lemmas 1-3 and persistent sidetrack-heap construction.
- [Bilo et al., Fixed-Parameter Sensitivity Oracles](https://arxiv.org/html/2112.03059): Section 4, slack telescoping Lemma 16, layered graph construction, failure-copy expansion, and Lemma 17; vertex-cover oracle Theorem 15 also inspected.
- [Dellin and Srinivasa, LazySP](https://personalrobotics.cs.washington.edu/publications/dellin2016lazysp.pdf): Algorithm 1 and completeness/optimality conditions. Used to discard generic lazy witness checking as an invention.
- [Lokshtanov et al., A Brief Note on Single Source Fault Tolerant Reachability](https://arxiv.org/pdf/1904.08150): main bound and important-separator construction; stronger Baswana et al. result checked through its author-hosted indexed primary PDF and the explicit use in Bilo et al. Direct full fetch of the former failed.
- [Mehlhorn and Meyer, External-Memory BFS](https://resources.mpi-inf.mpg.de/departments/d1/teaching/ws10/models_of_computation/ExternalMemoryBFS.pdf): model and exact graph-shape qualifications in the opening.
- [Dibbelt, Strasser, Wagner, CCH](https://ben-strasser.net/paper/customizable_contraction_hierarchies_arxiv_preprint.pdf): primary preprint located/opened; only introductory workflow inspected, not an implementation audit.

Local context: portfolio README; Architecture-Decision-Map D01-D06 and quantitative/lifecycle sections; complete Final-Research-Decision-Brief; End-To-End-Workflow first 165 lines; Architecture-Candidates-v1 certificate and incidence passages; Sol-01 factorization, skeletons, lineage, and per-family layouts. No production-code discovery was required. No claim about current production algorithm availability is inferred.

## What Changed During Research

WCC: discarded uniform "f deletions since the previous refresh" in favor of cumulative witness-hit debit per immutable certificate, with exact mixed-depth cover selection. This avoids both false expiry from irrelevant deletions and false revival after a generation change.

WCC state revision: the first draft still required 9n DSU bytes. The optional route contracts one global surviving original forest and leaves untouched components implicit on disk. Nested-cut holes and minimum-ID reconstruction forced an event sweep and a separate reduction/output pass. Primary literature then narrowed this to a composition with known sensitivity-oracle machinery, not a new dynamic-forest algorithm.

Paths: discarded "a surviving tight forest can always be contracted". Incoming exception heads and the query source must become gates. Dominated arcs are excluded only while their entire directed tight-tree witness survives; fracture triggers reclassification. Zero cycles never substitute for a rooted witness chain.

Paths source revision: dropping internal quotient arcs or losing their tail IDs looked safe until an interior source was promoted. The dormant-loop counterexample forced retention of the full tail-aware exception ledger and its one-row split.

Both mathematical mechanisms passed the bounded probes. The hardest missing evidence is whether their auxiliary representation, source interpretation, rebuild work, and complete output beat the strongest baseline within the physical resource envelope.
