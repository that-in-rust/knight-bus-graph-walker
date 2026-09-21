"""Resident semantic/source profiler; no low-RAM or timing claim.

Both preparation and verification intentionally materialize the source graph,
owners and quotient. Unit-hop and zero-cost reachability are distinct queries.
"""

import argparse
from collections import deque
import csv
import hashlib
from heapq import heappop, heappush
import json
from pathlib import Path


def hash_source_snapshot_file(path):
    with open(path,"rb") as handle:
        return hashlib.file_digest(handle,"sha256").hexdigest()


def load_dependency_source_snapshot(root):
    root = Path(root)
    paths = [root/"family-file-nodes.tsv",root/"family-file-edges.tsv"]
    hashes = {str(path):hash_source_snapshot_file(path) for path in paths}
    nodes = []
    lookup = {}
    with paths[0].open(newline="",encoding="utf-8") as handle:
        reader = csv.DictReader(handle,delimiter="\t")
        if reader.fieldnames != ["repo","file"]:
            raise ValueError("unexpected node schema")
        for row in reader:
            key = (row["repo"],row["file"])
            if not all(key) or key in lookup:
                raise ValueError("empty or duplicate node identity")
            lookup[key] = len(nodes)
            nodes.append(key)
    edges = set()
    raw_count = duplicates = loops = 0
    with paths[1].open(newline="",encoding="utf-8") as handle:
        reader = csv.DictReader(handle,delimiter="\t")
        if reader.fieldnames != ["repo","source_file","target_file"]:
            raise ValueError("unexpected edge schema")
        for row in reader:
            raw_count += 1
            keys = [(row["repo"],row[field]) for field in ("source_file","target_file")]
            if any(key not in lookup for key in keys):
                raise ValueError("edge endpoint absent from declared nodes")
            u,v = [lookup[key] for key in keys]
            if u == v:
                loops += 1
            elif (u,v) in edges:
                duplicates += 1
            else:
                edges.add((u,v))
    if any(hash_source_snapshot_file(path) != hashes[str(path)] for path in paths):
        raise ValueError("source changed during profiling")
    return nodes,sorted(edges),dict(input_sha256=hashes,raw_edge_rows=raw_count,
                                  duplicate_edges_removed=duplicates,loops_removed=loops,
                                  nodes=len(nodes),simple_edges=len(edges))


def construct_degree_one_forest(incoming):
    parent = [row[0] if len(row) == 1 else -1 for row in incoming]
    color = [0]*len(parent)
    cycles = 0
    for start in range(len(parent)):
        if color[start]:
            continue
        v, trail = start, []
        while v != -1 and color[v] == 0:
            color[v] = 1
            trail.append(v)
            v = parent[v]
        if v != -1 and color[v] == 1:
            cycle, current = [v], parent[v]
            while current != v:
                cycle.append(current)
                current = parent[current]
            parent[min(cycle)] = -1
            cycles += 1
        for vertex in trail:
            color[vertex] = 2
    return parent,cycles


def construct_reachability_dfs_forest(outgoing):
    parent = [-1]*len(outgoing)
    seen = set()
    for root in range(len(outgoing)):
        if root in seen:
            continue
        seen.add(root)
        stack = [(root,iter(outgoing[root]))]
        while stack:
            u, cursor = stack[-1]
            v = next(cursor,None)
            if v is None:
                stack.pop()
            elif v not in seen:
                seen.add(v)
                parent[v] = u
                stack.append((v,iter(outgoing[v])))
    return parent


def compute_forest_coordinate_arrays(parent):
    n = len(parent)
    children = [[] for _ in parent]
    for v,p in enumerate(parent):
        if p != -1:
            children[p].append(v)
    tin,end,depth,order = [0]*n,[0]*n,[0]*n,[]
    for root,p in enumerate(parent):
        if p != -1:
            continue
        stack = [(root,False)]
        while stack:
            v,exiting = stack.pop()
            if exiting:
                end[v] = len(order)
                continue
            tin[v] = len(order)
            order.append(v)
            stack.append((v,True))
            for child in reversed(children[v]):
                depth[child] = depth[v]+1
                stack.append((child,False))
    if len(order) != n:
        raise ValueError("selected parent relation is not a forest")
    return tin,end,depth,order


def prepare_source_forest_profile(n,edges,mode):
    if type(n) is not int or n < 0 or mode not in ("unit","reachability"):
        raise ValueError("invalid source contract")
    edges = list(edges)
    if any(len(e) != 2 or any(type(v) is not int or not 0 <= v < n for v in e) or e[0] == e[1] for e in edges):
        raise ValueError("requires simple loopless valid integer endpoints")
    if len(set(edges)) != len(edges):
        raise ValueError("duplicate edge: normalize explicitly before profiling")
    outgoing,incoming = [[] for _ in range(n)],[[] for _ in range(n)]
    for u,v in sorted(edges):
        outgoing[u].append(v)
        incoming[v].append(u)
    if mode == "unit":
        parent,cycles = construct_degree_one_forest(incoming)
    else:
        parent,cycles = construct_reachability_dfs_forest(outgoing),None
    tin,end,depth,order = compute_forest_coordinate_arrays(parent)
    h = depth[:] if mode == "unit" else [0]*n
    weight = int(mode == "unit")
    forest = {(p,v) for v,p in enumerate(parent) if p != -1}
    omitted,exceptions = [],[]
    for u,v in edges:
        assert weight+h[u]-h[v] >= 0
        if (u,v) in forest:
            assert weight+h[u]-h[v] == 0
        elif tin[u] <= tin[v] < end[u]:
            omitted.append((u,v))
        else:
            exceptions.append((u,v))
    gates = {v for v,p in enumerate(parent) if p == -1} | {v for u,v in exceptions}
    owner = [-1]*n
    for v in order:
        owner[v] = v if v in gates else owner[parent[v]]
    ledger = exceptions+[(u,v) for u,v in sorted(forest) if owner[u] != owner[v]]
    if mode == "unit":
        assert not omitted
        assert len(gates) == n-sum(len(row) == 1 for row in incoming)+cycles
    return dict(n=n,m=len(edges),mode=mode,edges=tuple(edges),outgoing=outgoing,
                parent=parent,tin=tin,end=end,depth=depth,order=order,h=h,
                forest=forest,omitted=omitted,exceptions=exceptions,gates=gates,
                owner=owner,ledger=ledger,q=len(gates),A=len(ledger),D=len(omitted),
                indegree_one=sum(len(row) == 1 for row in incoming),
                degree_one_cycles=cycles,max_depth=max(depth,default=0))


def prepare_current_cut_regions(base,cuts,source=None):
    n,parent = base["n"],base["parent"]
    if any(type(v) is not int or not 0 <= v < n or parent[v] == -1 for v in cuts):
        raise ValueError("cut must identify a selected forest edge by child")
    if source is not None and (type(source) is not int or not 0 <= source < n):
        raise ValueError("invalid source")
    deepest = [-1]*n
    for v in base["order"]:
        deepest[v] = v if v in cuts else (-1 if parent[v] == -1 else deepest[parent[v]])
    revived = [(u,v) for u,v in base["omitted"]
               if deepest[v] != -1 and base["depth"][u] < base["depth"][deepest[v]]]
    states = base["gates"] | set(cuts) | {v for u,v in revived}
    if source is not None:
        states.add(source)
    owner = [-1]*n
    for v in base["order"]:
        owner[v] = v if v in states else owner[parent[v]]
    assert len(states) <= base["q"]+len(cuts)+len(revived)+(source is not None)
    return states,owner,revived


def verify_source_cut_query(base,cuts,source,all_paths=False):
    states,owner,revived = prepare_current_cut_regions(base,cuts,source)
    parent,h,n = base["parent"],base["h"],base["n"]
    removed = {(parent[v],v) for v in cuts}
    retained = (base["forest"]-removed) | set(base["exceptions"]) | set(revived)
    weight = int(base["mode"] == "unit")
    quotient = {v:[] for v in states}
    for u,v in sorted(retained):
        g = owner[u]
        if (u,v) in base["forest"] and owner[v] == g:
            continue
        assert v in states
        quotient[g].append((v,weight+h[u]-h[v],base["depth"][u]-base["depth"][g]+1,u))
    distance, predecessor, heap = {source:(0,0)}, {}, [(0,0,source)]
    while heap:
        cost,hops,g = heappop(heap)
        if distance[g] != (cost,hops):
            continue
        for v,w,length,tail in quotient[g]:
            candidate = cost+w,hops+length
            if v not in distance or candidate < distance[v]:
                distance[v] = candidate
                predecessor[v] = g,tail,v
                heappush(heap,(*candidate,v))
    original = {source:0}
    queue = deque([source])
    while queue:
        u = queue.popleft()
        for v in base["outgoing"][u]:
            if (u,v) not in removed and v not in original:
                original[v] = original[u]+1
                queue.append(v)
    for v in range(n):
        actual = None if owner[v] not in distance else distance[owner[v]][0]+h[v]-h[source]
        expected = (original[v] if weight else 0) if v in original else None
        assert actual == expected,(base["mode"],source,v,actual,expected)
    targets = sorted(original) if all_paths else sorted(original)[::max(1,len(original)//16)][:16]
    original_edges = set(base["edges"])
    for target in targets:
        g,macros = owner[target],[]
        while g != source:
            start,tail,head = predecessor[g]
            macros.append((start,tail,head))
            g = start
        path = []
        for start,tail,head in reversed(macros):
            suffix = []
            current = tail
            while current != start:
                assert current not in cuts and parent[current] != -1
                suffix.append((parent[current],current))
                current = parent[current]
            path.extend(reversed(suffix))
            path.append((tail,head))
        suffix,current = [],target
        while current != owner[target]:
            assert current not in cuts and parent[current] != -1
            suffix.append((parent[current],current))
            current = parent[current]
        path.extend(reversed(suffix))
        current,seen = source,{source}
        for u,v in path:
            assert u == current and (u,v) in original_edges and (u,v) not in removed and v not in seen
            current = v
            seen.add(v)
        assert current == target
        if weight:
            assert len(path) == original[target]
    return dict(target_checks=n,reachable_targets=len(original),path_checks=len(targets),
                states=len(states),revivals=len(revived),cuts=len(cuts))


def select_source_profile_cuts(base,k):
    children = [v for v in base["order"] if base["parent"][v] != -1]
    count = min(k,len(children))
    return {children[i*len(children)//count] for i in range(count)} if count else set()


def run_dependency_source_study(source_dir,output_dir):
    nodes,edges,receipt = load_dependency_source_snapshot(source_dir)
    results = []
    for name in ("family","neo4j-src","neo4j-gds-src"):
        selected = [i for i,(repo,_) in enumerate(nodes) if name == "family" or repo == name]
        translate = {old:new for new,old in enumerate(selected)}
        induced = [(translate[u],translate[v]) for u,v in edges if u in translate and v in translate]
        for reverse in (False,True):
            directed = sorted((v,u) if reverse else (u,v) for u,v in induced)
            for mode in ("unit","reachability"):
                base = prepare_source_forest_profile(len(selected),directed,mode)
                row = {key:base[key] for key in ("n","m","q","A","D","indegree_one","degree_one_cycles","max_depth")}
                row.update(dataset=name,direction="reverse-impact" if reverse else "forward-dependency",mode=mode,
                           state_fraction=base["q"]/base["n"],ledger_fraction=base["A"]/base["m"])
                outgoing = base["outgoing"]
                sources = sorted({0,len(selected)-1,max(range(len(selected)),key=lambda v:(len(outgoing[v]),-v)),
                                  max(range(len(selected)),key=lambda v:(base["depth"][v],-v))})
                row["sources"] = [dict(vertex=s,repo=nodes[selected[s]][0],file=nodes[selected[s]][1]) for s in sources]
                row["cut_batches"] = []
                for k in (0,1,8,32):
                    cuts = select_source_profile_cuts(base,k)
                    states,_,revived = prepare_current_cut_regions(base,cuts)
                    audits = [verify_source_cut_query(base,cuts,s) for s in sources]
                    row["cut_batches"].append(dict(requested_k=k,k=len(cuts),cut_children=sorted(cuts),
                                                    r=len(revived),states=len(states),audits=audits))
                results.append(row)
    receipt.update(profiles=results,kind="resident eligibility and exact semantics, not timing or RAM measurement",
                   code_sha256={path.name:hash_source_snapshot_file(path) for path in
                                (Path(__file__),Path(__file__).with_name("test_path_source_eligibility.py"))})
    destination = Path(output_dir)/"receipt.json"
    destination.parent.mkdir(parents=True,exist_ok=True)
    with destination.open("x",encoding="utf-8") as handle:
        json.dump(receipt,handle,indent=2,sort_keys=True)
        handle.write("\n")
    for row in results:
        print(json.dumps({key:row[key] for key in ("dataset","direction","mode","n","m","q","A","D","state_fraction")},sort_keys=True))
    print("receipt",destination)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir",required=True,type=Path)
    parser.add_argument("--output-dir",required=True,type=Path)
    args = parser.parse_args()
    run_dependency_source_study(args.source_dir,args.output_dir)
