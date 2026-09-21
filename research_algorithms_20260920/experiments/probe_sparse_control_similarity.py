"""Resident top-k control with sparse histograms and no dense query summaries."""

import argparse
from collections import Counter
from fractions import Fraction
import gzip
from heapq import heappush, heapreplace, merge
from itertools import groupby
import json
from pathlib import Path

from probe_compressed_interval_similarity import compute_compressed_interval_bounds
from profile_large_group_similarity import hash_similarity_input_file, prepare_large_group_source
from bench_bounded_seed_similarity import read_undirected_graph_source


def prepare_compact_source_view(source):
    """Discard dense query metadata; the supplied resident builder is still paid."""
    blocks = [dict(rows=block["rows"], core=block["core"], minimum=block["minimum"],
                   major=block["caps"][1], min_id=block["min_id"]) for block in source["blocks"]]
    return dict(blocks=blocks, block_posts=source["block_posts"], sizes=source["sizes"],
                target_ids=tuple(node for node, _ in source["targets"]), groups=source["groups"],
                query_limit=source["query_limit"])


def run_sparse_interval_query(source, query, sid, k, *, record_cap=4096):
    if any(type(value) is not int or value < 0 for value in (k, record_cap)):
        raise ValueError("invalid k or record cap")
    query = frozenset(query)
    if any(type(value) is not int or value < 0 for value in query):
        raise ValueError("invalid query feature")
    count = min(k, len(source["target_ids"]) - int(sid in source["sizes"]))
    stats = Counter(a=len(query), k=count, source_features=len(query),
                    dense_histogram_cells=0, sparse_pairs_peak=0)
    heap = []

    def update_sparse_witness_heap(node, overlap):
        if node == sid or not overlap or not count:
            return
        size = source["sizes"][node]
        record = (Fraction(overlap, len(query) + size - overlap), -node, node, len(query), size, overlap)
        if len(heap) < count:
            heappush(heap, record)
        elif record > heap[0]:
            heapreplace(heap, record)

    def bound_rejects_current_block(bound, minimum_id):
        return bound < heap[0][0] or (bound == heap[0][0] and minimum_id > heap[0][2])

    def iterate_sparse_posting_records(feature):
        for bid in source["block_posts"].get(feature, ()):
            yield bid, feature % source["groups"]

    if count:
        streams = [iterate_sparse_posting_records(feature) for feature in sorted(query)]
        stats["posting_streams"] = sum(bool(source["block_posts"].get(feature)) for feature in query)
        for bid, occurrences in groupby(merge(*streams), key=lambda row: row[0]):
            # Lexicographic merge yields each block's positive leaves in order.
            sparse = [(leaf, sum(1 for _ in repeated))
                      for leaf, repeated in groupby(occurrences, key=lambda row: row[1])]
            total = sum(value for _, value in sparse)
            stats["sparse_pairs_peak"] = max(stats["sparse_pairs_peak"], len(sparse))
            stats["sparse_pairs_constructed"] += len(sparse)
            stats["block_memberships"] += total
            stats["blocks_seen"] += 1
            block = source["blocks"][bid]
            if len(heap) == count:
                rank = min(total, block["major"])
                bound = Fraction(rank, len(query) + max(block["minimum"], rank) - rank) if rank else Fraction()
                stats["union_evaluations"] += 1
                if not bound_rejects_current_block(bound, block["min_id"]) and block["core"] is not None:
                    if max(value for _, value in sparse) > source["query_limit"]:
                        stats["profile_refusals"] += 1
                    elif len(block["core"]["nodes"]) > record_cap:
                        stats["record_refusals"] += 1
                    else:
                        paid = {}
                        values = compute_compressed_interval_bounds(block["core"], len(query),
                            block["minimum"], iter(sparse), record_cap=record_cap, stats=paid)
                        assert values["union"] == bound and values["interval"] <= values["laminar"] <= bound
                        bound = values["interval"]
                        stats["compact_evaluations"] += 1
                        for key in ("query_core_copies", "query_owner_lookups", "query_exception_hits",
                                    "owner_intervals_crossed", "control_nodes_visited", "control_child_reductions"):
                            stats[key] += paid.get(key, 0)
                        stats["control_records_peak"] = max(stats["control_records_peak"], paid["control_records_peak"])
                if bound_rejects_current_block(bound, block["min_id"]):
                    stats["blocks_pruned"] += 1
                    continue
            for node, features in block["rows"]:
                stats["body_targets"] += 1
                stats["body_memberships"] += len(features)
                update_sparse_witness_heap(node, len(query & features))
    if len(heap) < count:
        positive = {record[2] for record in heap}
        for node in source["target_ids"]:
            stats["zero_id_visits"] += 1
            if node == sid or node in positive:
                continue
            heappush(heap, (Fraction(), -node, node, len(query), source["sizes"][node], 0))
            stats["zero_rows"] += 1
            if len(heap) == count:
                break
    assert len(heap) == count
    return [record[2:] for record in sorted(heap, reverse=True)], stats


def run_frozen_sparse_study(root, output_dir):
    root, output_dir = Path(root), Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / "receipt.json"
    if destination.exists():
        raise FileExistsError(destination)
    previous_path = root / "evidence/similarity-large-groups-20260921/receipt.json"
    previous = json.loads(previous_path.read_text())
    controls = {(row["dataset"], row["groups"], row["sid"]): row
                for row in previous["runs"] if row["mode"] == "interval"}
    receipt = dict(kind="resident sparse-control equivalence; no time or physical RAM", inputs=previous["inputs"],
                   prior_receipt=str(previous_path), prior_sha256=hash_similarity_input_file(previous_path),
                   runs=[], preparation=[])
    for item in previous["inputs"]:
        source_path = Path(item["source"])
        assert hash_similarity_input_file(source_path) == item["sha256"]
        with gzip.open(source_path, "rt", encoding="utf-8") as stream:
            graph, info = read_undirected_graph_source(stream)
        assert info == item["normalization"]
        targets = sorted(graph.items())
        # A universal admitted leaf-count profile for this finite feature universe.
        query_limit = sum(len(row) for _, row in targets)
        for groups in previous["groups"]:
            dense = prepare_large_group_source(targets, groups, query_limit=query_limit)
            compact, build = prepare_compact_source_view(dense), dense["preparation"]
            receipt["preparation"].append(dict(dataset=item["dataset"], groups=groups,
                query_limit=query_limit, resident_build_metrics=build,
                discarded_dense_values=build["dense_capacity_values"] + build["dense_population_values"]))
            del dense
            for sid in item["source_ids"]:
                rows, stats = run_sparse_interval_query(compact, graph[sid], sid, 10)
                old = controls[item["dataset"], groups, sid]
                assert [list(row) for row in rows] == old["rows"]
                for key in ("blocks_seen", "block_memberships", "body_targets", "body_memberships", "zero_rows"):
                    assert stats[key] == old["metrics"].get(key, 0), (item["dataset"], groups, sid, key)
                assert not stats["profile_refusals"] and not stats["record_refusals"]
                receipt["runs"].append(dict(dataset=item["dataset"], groups=groups, sid=sid,
                                             rows=rows, metrics=dict(stats)))
            print(json.dumps(dict(dataset=item["dataset"], groups=groups, complete_outputs=30)), flush=True)
    names = ("probe_sparse_control_similarity.py", "test_sparse_control_similarity.py",
             "probe_compressed_interval_similarity.py", "test_compressed_interval_similarity.py",
             "profile_large_group_similarity.py", "probe_exception_core_similarity.py",
             "probe_additive_core_similarity.py", "probe_overlap_frontier_similarity.py",
             "bench_bounded_seed_similarity.py")
    receipt["code_sha256"] = {name: hash_similarity_input_file(root / "experiments" / name) for name in names}
    receipt["complete_results_checked"] = len(receipt["runs"])
    with destination.open("x", encoding="utf-8") as stream:
        json.dump(receipt, stream, sort_keys=True, indent=2)
        stream.write("\n")
    print("receipt", destination, flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--output-dir", type=Path, required=True)
    options = parser.parse_args()
    run_frozen_sparse_study(options.root, options.output_dir)
