"""Resident exact-decision workload study; no elapsed-time or RAM claims."""

import argparse
from collections import Counter, defaultdict
from fractions import Fraction
import gzip
import hashlib
from heapq import heappush, heapreplace, merge
from itertools import groupby
import json
from pathlib import Path

from bench_bounded_seed_similarity import read_undirected_graph_source
from probe_compressed_dual_similarity import compute_compressed_threshold_bound
from probe_exception_core_similarity import compile_modal_capacity_core, compute_exception_core_bound
from probe_laminar_capacity_similarity import compute_laminar_jaccard_bound
from probe_selective_capacity_similarity import compute_interval_capacity_bound
from test_bounded_seed_similarity import compute_finite_similarity_oracle


MODES = ("union","interval","modal_branch","modal_dual","posting_merge")
BUDGET_ERRORS = {
    "threshold state or work budget exceeded before enumeration",
    "branch or work budget exceeded before enumeration",
}


def hash_similarity_input_file(path):
    with open(path,"rb") as stream:
        return hashlib.file_digest(stream,"sha256").hexdigest()


def prepare_large_group_source(targets,groups,query_limit=8):
    if type(groups) is not int or not 1 <= groups <= 256 or groups & (groups-1):
        raise ValueError("invalid groups")
    if type(query_limit) is not int or query_limit < 0:
        raise ValueError("invalid query limit")
    targets = sorted((node,frozenset(values)) for node,values in targets)
    if len({node for node,_ in targets}) != len(targets):
        raise ValueError("duplicate target ID")
    if any(type(node) is not int or any(type(v) is not int or v < 0 for v in values) for node,values in targets):
        raise ValueError("invalid target or feature IDs")
    blocks,block_posts,target_posts = [],defaultdict(list),defaultdict(list)
    paid = Counter(target_rows=len(targets),source_memberships=sum(len(row) for _,row in targets))
    sizes = {node:len(values) for node,values in targets}
    for position,(node,values) in enumerate(targets):
        for feature in sorted(values):
            target_posts[feature].append(node)
    for lo in range(0,len(targets),2):
        rows = targets[lo:lo+2]
        union = set().union(*(row for _,row in rows))
        caps = [0]*(2*groups)
        for _,row in rows:
            occupancy = [0]*(2*groups)
            for feature in row:
                occupancy[groups+feature%groups] += 1
            for j in range(groups-1,0,-1):
                occupancy[j] = occupancy[2*j]+occupancy[2*j+1]
            caps = [max(c,x) for c,x in zip(caps,occupancy)]
        populations = [0]*groups
        for feature in union:
            populations[feature%groups] += 1
            block_posts[feature].append(len(blocks))
        core = None
        if len(rows) == 2:
            metrics = {}
            core = compile_modal_capacity_core(caps,populations,query_limit=query_limit,stats=metrics)
            paid["modal_core_nodes"] += len(core["nodes"])
            paid["modal_owner_runs"] += len(core["owner_runs"])
            paid["modal_defaults_slots"] += len(core["component_defaults"])
            paid["modal_deviation_records"] += len(core["exceptions"])
            paid["modal_deviation_keys"] += len(core["exception_keys"])
            paid["strict_nodes"] += core["strict_nodes"]
            paid["modal_compile_original_node_visits"] += metrics["preprocessing_node_visits"]
            paid["modal_compile_ownership_node_visits"] += metrics["ownership_node_visits"]
            paid["modal_compile_signature_records"] += metrics["modal_signature_records_peak"]
        paid["dense_capacity_values"] += 2*groups-1
        paid["dense_population_values"] += groups
        paid["union_posting_memberships"] += len(union)
        blocks.append(dict(rows=rows,caps=caps,populations=populations,core=core,
                           minimum=min(map(lambda row:len(row[1]),rows)),min_id=rows[0][0]))
    return dict(targets=targets,blocks=blocks,block_posts=block_posts,target_posts=target_posts,
                sizes=sizes,groups=groups,query_limit=query_limit,preparation=dict(paid))


def run_large_group_query(source,query,sid,k,mode,*,state_cap=4096,work_cap=65536,query_work_cap=10**7):
    if mode not in MODES or any(type(v) is not int or v < 0 for v in (k,state_cap,work_cap,query_work_cap)):
        raise ValueError("invalid mode or budget")
    query = frozenset(query)
    if any(type(v) is not int or v < 0 for v in query):
        raise ValueError("invalid query feature")
    count = min(k,len(source["targets"])-int(sid in source["sizes"]))
    stats = Counter(a=len(query),k=count,source_features=len(query))
    heap = []

    def update_exact_witness_heap(node,overlap):
        if node == sid or overlap == 0 or count == 0:
            return
        size = source["sizes"][node]
        record = (Fraction(overlap,len(query)+size-overlap),-node,node,len(query),size,overlap)
        if len(heap) < count:
            heappush(heap,record)
        elif record > heap[0]:
            heapreplace(heap,record)

    def current_upper_bound_prunes(bound,min_id):
        return len(heap) == count and (bound < heap[0][0] or (bound == heap[0][0] and min_id > heap[0][2]))

    if count and mode == "posting_merge":
        streams = [source["target_posts"].get(feature,()) for feature in sorted(query)]
        stats["posting_streams"] = sum(bool(stream) for stream in streams)
        for node,occurrences in groupby(merge(*streams)):
            overlap = sum(1 for _ in occurrences)
            stats["target_posting_memberships"] += overlap
            stats["posted_targets"] += 1
            update_exact_witness_heap(node,overlap)
    elif count:
        def iterate_group_posting_records(feature):
            for bid in source["block_posts"].get(feature,()):
                yield bid,feature%source["groups"]

        streams = [iterate_group_posting_records(feature) for feature in sorted(query)]
        stats["posting_streams"] = sum(bool(source["block_posts"].get(feature)) for feature in query)
        for bid,occurrences in groupby(merge(*streams),key=lambda record:record[0]):
            groups = source["groups"]
            counts = [0]*groups
            stats["histogram_initialized_cells"] += groups
            for _,leaf in occurrences:
                counts[leaf] += 1
                stats["block_memberships"] += 1
            stats["blocks_seen"] += 1
            block = source["blocks"][bid]
            caps,populations = block["caps"],block["populations"]
            if len(heap) == count:
                bound = compute_laminar_jaccard_bound(len(query),block["minimum"],caps,counts,"union")
                stats["union_evaluations"] += 1
                if not current_upper_bound_prunes(bound,block["min_id"]) and mode != "union":
                    stats["capacity_payload_values"] += 2*groups-1
                    bound = compute_laminar_jaccard_bound(len(query),block["minimum"],caps,counts)
                    stats["capacity_evaluations"] += 1
                    if not current_upper_bound_prunes(bound,block["min_id"]):
                        stats["population_payload_values"] += groups
                        refined = compute_interval_capacity_bound(len(query),block["minimum"],caps,counts,populations,len(block["rows"]))
                        assert refined <= bound
                        bound = refined
                        stats["interval_evaluations"] += 1
                        if mode.startswith("modal_") and block["core"] is not None and not current_upper_bound_prunes(bound,block["min_id"]):
                            stats["modal_preflights"] += 1
                            stats["tau_count_checks"] += groups
                            if max(counts) > source["query_limit"]:
                                stats["modal_tau_refusals"] += 1
                            elif mode == "modal_branch" and len(block["core"]["nodes"]) > state_cap:
                                stats["modal_budget_refusals"] += 1
                            else:
                                sparse = [(j,q) for j,q in enumerate(counts) if q]
                                stats["sparse_histogram_scan_cells"] += groups
                                stats["sparse_histogram_pairs"] += len(sparse)
                                core,paid = block["core"],{}
                                for key,records in (("supplied_core_nodes",core["nodes"]),("supplied_owner_runs",core["owner_runs"]),
                                                    ("supplied_default_slots",core["component_defaults"]),("supplied_deviations",core["exceptions"])):
                                    stats[key] += len(records)
                                remaining = min(work_cap,query_work_cap-stats["solver_reserved_work"])
                                try:
                                    if mode == "modal_branch":
                                        refined = compute_exception_core_bound(core,len(query),block["minimum"],iter(sparse),
                                                                              branch_cap=65536,work_cap=remaining,stats=paid)
                                    else:
                                        refined = compute_compressed_threshold_bound(core,len(query),block["minimum"],iter(sparse),
                                                                                    state_cap=state_cap,work_cap=remaining,stats=paid)
                                except ValueError as error:
                                    if str(error) not in BUDGET_ERRORS:
                                        raise
                                    stats["modal_budget_refusals"] += 1
                                else:
                                    assert refined <= bound
                                    stats["modal_evaluations"] += 1
                                    stats["modal_tighter"] += int(refined < bound)
                                    stats["modal_extra_pruned_blocks"] += int(current_upper_bound_prunes(refined,block["min_id"]))
                                    stats["solver_reserved_work"] += paid.get("reserved_branch_work",paid.get("reserved_operations",0))
                                    stats["solver_actual_work"] += paid.get("branch_work",paid.get("operations",0))
                                    stats["solver_states_peak"] = max(stats["solver_states_peak"],paid.get("row_record_slots_peak",paid.get("states",0)))
                                    bound = refined
                                for key in ("query_core_copies","query_owner_lookups","query_exception_hits","owner_intervals_crossed","sparse_pairs_read","plan_nodes"):
                                    stats[key] += paid.get(key,0)
                if current_upper_bound_prunes(bound,block["min_id"]):
                    stats["blocks_pruned"] += 1
                    continue
            for node,features in block["rows"]:
                stats["body_targets"] += 1
                stats["body_memberships"] += len(features)
                update_exact_witness_heap(node,len(query & features))
    if len(heap) < count:
        positive = {record[2] for record in heap}
        for node,features in source["targets"]:
            stats["zero_id_visits"] += 1
            if node == sid or node in positive:
                continue
            heappush(heap,(Fraction(),-node,node,len(query),len(features),0))
            stats["zero_rows"] += 1
            if len(heap) == count:
                break
    assert len(heap) == count and stats["solver_reserved_work"] <= query_work_cap
    return [record[2:] for record in sorted(heap,reverse=True)],stats


def run_frozen_group_study(root,output_dir):
    root,output_dir = Path(root),Path(output_dir)
    output_dir.mkdir(parents=True,exist_ok=True)
    destination = output_dir/"receipt.json"
    if destination.exists():
        raise FileExistsError(destination)
    receipt = dict(kind="resident exact workload decisions, no timing or RAM",groups=[4,16,64],
                   query_limit=8,state_cap=4096,work_cap=65536,query_work_cap=10**7,runs=[],preparation=[],inputs=[])
    for label,filename in (("GrQc","ca-GrQc.txt.gz"),("Facebook","facebook_combined.txt.gz")):
        old_path = root/("Similarity-Overlap-"+label+"-Results.json")
        old = json.loads(old_path.read_text())
        source_path = root/"experiments"/"data"/filename
        assert hash_similarity_input_file(source_path) == old["sha256"]
        with gzip.open(source_path,"rt",encoding="utf-8") as stream:
            graph,info = read_undirected_graph_source(stream)
        assert info == old["source"]
        targets = sorted(graph.items())
        expected = {sid:compute_finite_similarity_oracle(targets,graph[sid],sid,10) for sid in old["selected_source_ids"]}
        receipt["inputs"].append(dict(dataset=label,source=str(source_path),sha256=old["sha256"],
                                      prior_receipt=str(old_path),prior_sha256=hash_similarity_input_file(old_path),
                                      normalization=info,source_ids=old["selected_source_ids"]))
        old_runs = {(run["node"],run["mode"]):run["metrics"] for run in old["runs"] if run["repeat"] == 0}
        for groups in receipt["groups"]:
            source = prepare_large_group_source(targets,groups)
            receipt["preparation"].append(dict(dataset=label,groups=groups,metrics=source["preparation"]))
            for sid in old["selected_source_ids"]:
                for mode in MODES:
                    rows,stats = run_large_group_query(source,graph[sid],sid,10,mode)
                    assert rows == expected[sid],(label,groups,sid,mode)
                    if groups == 4 and mode in ("union","interval"):
                        for key in ("blocks_seen","block_memberships","body_targets","body_memberships","zero_rows"):
                            assert stats[key] == old_runs[sid,mode].get(key,0),(label,sid,mode,key,stats[key],old_runs[sid,mode].get(key,0))
                    if mode == "posting_merge":
                        assert stats["target_posting_memberships"] == old_runs[sid,mode].get("membership_visits",0)
                    receipt["runs"].append(dict(dataset=label,groups=groups,sid=sid,mode=mode,metrics=dict(stats),rows=rows))
            print(json.dumps(dict(dataset=label,groups=groups,queries_checked=30*len(MODES)),sort_keys=True),flush=True)
    names = ["profile_large_group_similarity.py","test_large_group_similarity.py","probe_compressed_dual_similarity.py",
             "probe_exception_core_similarity.py","probe_additive_core_similarity.py","probe_overlap_frontier_similarity.py",
             "probe_selective_capacity_similarity.py","probe_laminar_capacity_similarity.py","bench_bounded_seed_similarity.py",
             "test_bounded_seed_similarity.py"]
    receipt["code_sha256"] = {name:hash_similarity_input_file(root/"experiments"/name) for name in names}
    receipt["complete_results_checked"] = len(receipt["runs"])
    with destination.open("x",encoding="utf-8") as stream:
        json.dump(receipt,stream,sort_keys=True,indent=2)
        stream.write("\n")
    print("receipt",destination,flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root",type=Path,default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--output-dir",type=Path,required=True)
    args = parser.parse_args()
    run_frozen_group_study(args.root,args.output_dir)
