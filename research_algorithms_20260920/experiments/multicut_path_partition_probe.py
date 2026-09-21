"""Finite reference probe for the multicut revival/partition theorem.

Deliberately resident: three-sided and colored-minimum oracles use scans,
owners/parts are materialized, paths are tuples, heap entries are duplicated.
These validate exact semantics, not the claimed indexed publication costs,
external I/O, physical RAM, or a competitive production implementation.
"""

from bisect import bisect_left
from collections import defaultdict
from heapq import heappop, heappush
from math import inf


def original_forest_ancestor_test(base, ancestor, vertex):
    return base["tin"][ancestor] <= base["tin"][vertex] < base["end"][ancestor]


def build_multicut_path_fixture(parent, extras, positive=False):
    n=len(parent)
    if any(type(p) is not int or not -1 <= p < v for v,p in enumerate(parent)):
        raise ValueError("reference fixture requires parent-before-child numbering")
    children=defaultdict(list)
    for v,p in enumerate(parent):
        children[p].append(v)
    tin,end,depth,order={},{},{},[]
    def visit_forest_reference_vertex(v,level):
        tin[v],depth[v]=len(order),level
        order.append(v)
        for child in children[v]:
            visit_forest_reference_vertex(child,level+1)
        end[v]=len(order)
    for root in children[-1]:
        visit_forest_reference_vertex(root,0)
    h=[depth[v] if positive else 0 for v in range(n)]
    raw,forest_id=[],{}
    for v,p in enumerate(parent):
        if p>=0:
            forest_id[v]=len(raw)
            raw.append((len(raw),p,v,0,7))
    forest_count=len(raw)
    for u,v,cost in extras:
        if not (type(u) is type(v) is type(cost) is int and 0<=u<n and 0<=v<n and cost>=0):
            raise ValueError("invalid reference raw arc")
        raw.append((len(raw),u,v,cost+max(h[u]-h[v],0),11))
    base=dict(parent=tuple(parent),tin=tin,end=end,depth=depth,order=order,h=h,
              raw=tuple(raw),forest_count=forest_count,forest_id=forest_id)
    omitted=tuple(e for e in raw[forest_count:] if original_forest_ancestor_test(base,e[1],e[2]))
    exceptions=tuple(e for e in raw[forest_count:] if not original_forest_ancestor_test(base,e[1],e[2]))
    gates=set(children[-1])|{e[2] for e in exceptions}
    owner={}
    for v in order:
        owner[v]=v if v in gates else owner[parent[v]]
    ledger=exceptions+tuple(e for e in raw[:forest_count] if owner[e[1]]!=owner[e[2]])
    rows={g:[] for g in gates}
    for edge in ledger:
        rows[owner[edge[1]]].append(edge)
    ranks={}
    for g,row in rows.items():
        row.sort(key=lambda e:(tin[e[1]],e[0]))
        for index,edge in enumerate(row):
            ranks[edge[0]]=(g,index)
    base.update(omitted=omitted,exceptions=exceptions,gates=frozenset(gates),owner=owner,
                ledger=ledger,rows=rows,ranks=ranks)
    return base


def subtract_reference_interval_holes(left,right,holes):
    result,cursor=[],left
    for start,stop in sorted(holes):
        start,stop=max(left,start),min(right,stop)
        if start>=stop or stop<=cursor:
            continue
        if cursor<start:
            result.append((cursor,start))
        cursor=max(cursor,stop)
    if cursor<right:
        result.append((cursor,right))
    return result


def validate_multicut_child_set(base,cuts):
    if any(type(c) is not int or c not in base["forest_id"] for c in cuts):
        raise ValueError("cut must name a selected forest child")


def report_multicut_revived_records(base,cuts):
    validate_multicut_child_set(base,cuts)
    ordered=sorted(cuts,key=base["tin"].get)
    children,stack=defaultdict(list),[]
    for child in ordered:
        while stack and not original_forest_ancestor_test(base,stack[-1],child):
            stack.pop()
        if stack:
            children[stack[-1]].append(child)
        stack.append(child)
    bands=[]
    for child in ordered:
        holes=[(base["tin"][v],base["end"][v]) for v in children[child]]
        bands.extend((left,right,child) for left,right in subtract_reference_interval_holes(
            base["tin"][child],base["end"][child],holes))
    bands.sort()
    assert len(bands)<=2*len(cuts)
    assert all(a[1]<=b[0] for a,b in zip(bands,bands[1:]))
    revived=[]
    for left,right,child in bands:
        # Linear reference for a paid three-sided reporting oracle.
        revived.extend(e for e in base["omitted"] if left<=base["tin"][e[2]]<right
                       and base["depth"][e[1]]<base["depth"][child])
    assert len({e[0] for e in revived})==len(revived)
    return bands,revived


def locate_multicut_current_owner(base,states,vertex):
    while vertex not in states:
        vertex=base["parent"][vertex]
        assert vertex>=0
    return vertex


def recover_multicut_forest_prefix(base,ancestor,vertex,cuts):
    path=[]
    while vertex!=ancestor:
        if vertex in cuts or vertex not in base["forest_id"]:
            return None
        path.append(base["forest_id"][vertex])
        vertex=base["parent"][vertex]
    return tuple(reversed(path))


def prepare_multicut_source_view(base,cuts,source):
    validate_multicut_child_set(base,cuts)
    if type(source) is not int or not 0<=source<len(base["parent"]):
        raise ValueError("source must be an original vertex")
    cuts=frozenset(cuts)
    bands,revived=report_multicut_revived_records(base,cuts)
    # Different oracle: directly inspect each omitted witness's parent edges.
    expected=[]
    for e in base["omitted"]:
        if recover_multicut_forest_prefix(base,e[1],e[2],cuts) is None:
            expected.append(e[0])
    assert sorted(e[0] for e in revived)==sorted(expected)
    ports=(set(cuts)|{e[2] for e in revived})-base["gates"]
    states=base["gates"]|ports|{source}
    marks=states-base["gates"]
    bounds,holes,links={},defaultdict(list),defaultdict(list)
    for entry in states:
        row=base["rows"][base["owner"][entry]]
        keys=[base["tin"][e[1]] for e in row]
        bounds[entry]=(bisect_left(keys,base["tin"][entry]),bisect_left(keys,base["end"][entry]))
    grouped=defaultdict(list)
    for mark in marks:
        grouped[base["owner"][mark]].append(mark)
    for gate,group in grouped.items():
        stack=[gate]
        for mark in sorted(group,key=base["tin"].get):
            while not original_forest_ancestor_test(base,stack[-1],mark):
                stack.pop()
            ancestor=stack[-1]
            holes[ancestor].append(bounds[mark])
            prefix=recover_multicut_forest_prefix(base,ancestor,mark,cuts)
            if prefix is None:
                assert mark in cuts
            else:
                links[ancestor].append((mark,prefix))
            stack.append(mark)
    deleted={base["forest_id"][c] for c in cuts}
    deleted_ledger=0
    for identity in deleted:
        if identity in base["ranks"]:
            gate,rank=base["ranks"][identity]
            tail=base["raw"][identity][1]
            owner=locate_multicut_current_owner(base,states,tail)
            assert base["owner"][owner]==gate
            holes[owner].append((rank,rank+1))
            deleted_ledger+=1
    parts={entry:subtract_reference_interval_holes(*bounds[entry],holes[entry]) for entry in states}
    covered=defaultdict(int)
    for entry,pieces in parts.items():
        row=base["rows"][base["owner"][entry]]
        for left,right in pieces:
            for e in row[left:right]:
                covered[e[0]]+=1
                assert locate_multicut_current_owner(base,states,e[1])==entry
                assert recover_multicut_forest_prefix(base,entry,e[1],cuts) is not None
    assert dict(covered)=={e[0]:1 for e in base["ledger"] if e[0] not in deleted}
    interval_count=sum(map(len,parts.values()))
    assert interval_count<=len(base["gates"])+2*len(marks)+deleted_ledger
    assert interval_count<=len(base["ledger"])-deleted_ledger
    revival_rows=defaultdict(list)
    for e in revived:
        owner=locate_multicut_current_owner(base,states,e[1])
        assert recover_multicut_forest_prefix(base,owner,e[1],cuts) is not None
        revival_rows[owner].append(e)
    assert len(ports)<=len(cuts)+len(revived)
    return dict(source=source,cuts=cuts,deleted=deleted,bands=bands,revived=revived,ports=ports,
        states=states,marks=marks,parts=parts,links=links,revival_rows=revival_rows,
        deleted_ledger_count=deleted_ledger)


def iterate_multicut_macro_neighbors(base,view,entry,stats):
    row=base["rows"][base["owner"][entry]]
    for left,right in view["parts"][entry]:
        stats["intervals"]+=1
        stats["covered"]+=right-left
        # Scanning oracle for color reporting and clipped pair minima.
        heads=sorted({e[2] for e in row[left:right]})
        for head in heads:
            edge=min((e for e in row[left:right] if e[2]==head),
                     key=lambda e:(e[3],base["depth"][e[1]],e[0]))
            prefix=recover_multicut_forest_prefix(base,entry,edge[1],view["cuts"])
            assert prefix is not None and edge[0] not in view["deleted"]
            stats["heads"]+=1
            yield head,edge[3],prefix+(edge[0],)
    for head,prefix in view["links"].get(entry,()):
        stats["links"]+=1
        yield head,0,prefix
    for edge in view["revival_rows"].get(entry,()):
        prefix=recover_multicut_forest_prefix(base,entry,edge[1],view["cuts"])
        assert prefix is not None
        stats["revived"]+=1
        yield edge[2],edge[3],prefix+(edge[0],)


def solve_multicut_reference_paths(base,view):
    source,states=view["source"],view["states"]
    distances={v:(inf,inf) for v in states}
    distances[source]=(0,0)
    paths,queue,settled={source:()},[(0,0,source)],set()
    stats=dict(intervals=0,covered=0,heads=0,links=0,revived=0)
    while queue:
        cost,hops,entry=heappop(queue)
        if (cost,hops)!=distances[entry]:
            continue
        assert entry not in settled
        settled.add(entry)
        for head,weight,path in iterate_multicut_macro_neighbors(base,view,entry,stats):
            candidate=(cost+weight,hops+len(path))
            if candidate<distances[head]:
                assert head not in settled
                distances[head],paths[head]=candidate,paths[entry]+path
                heappush(queue,(*candidate,head))
    assert stats["heads"]<=stats["covered"]<=len(base["ledger"])-view["deleted_ledger_count"]
    assert stats["revived"]<=len(view["revived"])
    assert stats["links"]<=len(view["marks"])
    return distances,paths


def verify_multicut_original_paths(base,view):
    distances,paths=solve_multicut_reference_paths(base,view)
    source,states,raw=view["source"],view["states"],base["raw"]
    owners={}
    for v in base["order"]:
        owners[v]=v if v in states or v in view["cuts"] or base["parent"][v]<0 else owners[base["parent"][v]]
        assert owners[v]==locate_multicut_current_owner(base,states,v)
    retained={e[0] for e in base["exceptions"]}|{e[0] for e in view["revived"]}
    canonical=[]
    for identity,u,v,cost,_ in raw:
        if identity in view["deleted"]:
            continue
        if identity>=base["forest_count"] and identity not in retained:
            continue
        if identity<base["forest_count"] and owners[u]==owners[v]:
            continue
        assert v in states
        canonical.append((owners[u],v,cost,base["depth"][u]-base["depth"][owners[u]]+1))
    reference={v:(inf,inf) for v in states}
    reference[source]=(0,0)
    for _ in range(len(states)-1):
        for u,v,cost,hops in canonical:
            reference[v]=min(reference[v],(reference[u][0]+cost,reference[u][1]+hops))
    assert distances==reference
    original=[inf]*len(base["parent"])
    original[source]=0
    for _ in range(len(original)-1):
        for identity,u,v,reduced,_ in raw:
            if identity not in view["deleted"]:
                original[v]=min(original[v],original[u]+reduced-base["h"][u]+base["h"][v])
    finite=0
    for target in base["order"]:
        owner=owners[target]
        assert distances[owner][0]+base["h"][target]-base["h"][source]==original[target]
        if original[target]==inf:
            continue
        suffix=recover_multicut_forest_prefix(base,owner,target,view["cuts"])
        assert suffix is not None
        path=paths[owner]+suffix
        vertex,cost,seen=source,0,{source}
        for identity in path:
            stored,u,v,reduced,incarnation=raw[identity]
            assert stored==identity and incarnation==(7 if identity<base["forest_count"] else 11)
            assert identity not in view["deleted"] and u==vertex and v not in seen
            seen.add(v)
            vertex,cost=v,cost+reduced-base["h"][u]+base["h"][v]
        assert vertex==target and cost==original[target]
        assert len(path)==distances[owner][1]+len(suffix)
        finite+=1
    return finite
