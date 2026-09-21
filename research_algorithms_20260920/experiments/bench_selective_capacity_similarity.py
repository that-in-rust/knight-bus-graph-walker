"""Serial public-data comparison with paid block access and exact interval control."""

import argparse
import gzip
import hashlib
import json
import platform
import statistics
import tempfile
from pathlib import Path
from time import perf_counter

from bench_bounded_seed_similarity import read_undirected_graph_source, select_public_query_sources
from probe_laminar_capacity_similarity import build_laminar_capacity_sidecar
from probe_posting_merge_similarity import select_posting_merge_answers
from probe_selective_capacity_similarity import build_selective_capacity_index, select_indexed_capacity_answers
from test_bounded_seed_similarity import compute_finite_similarity_oracle, load_runwise_reference_module


def run_selective_public_comparison(data_path, output_path, groups=4, block_size=2,
                                    sample_count=24, repeats=3, sample_seed=920520,
                                    include_overlap=False, merge_cap=128, pair_state_cap=4096,
                                    pair_work_cap=65536, pair_query_work_cap=10**7):
    assert type(repeats) is int and repeats > 0
    reference = load_runwise_reference_module(sort_capacity=4096, sort_fanin=16)
    began = perf_counter()
    with gzip.open(data_path, "rt", encoding="ascii") as stream:
        graph, source_info = read_undirected_graph_source(stream)
    normalization_seconds = perf_counter() - began
    targets = sorted(graph.items())
    selected = select_public_query_sources(graph, sample_count, sample_seed)
    methods = ("union", "partition", "laminar", "interval", "paired", "posting_merge")
    if include_overlap:
        methods += ("overlap_dense", "overlap_threshold", "overlap")
    began = perf_counter()
    shared = [len(targets[j][1] & targets[j + 1][1]) for j in range(0, len(targets) - 1, 2)]
    profile = {"full_pairs": len(shared), "disjoint_pairs": shared.count(0),
               "shared_memberships": sum(shared), "max_shared": max(shared, default=0),
               "seconds": perf_counter() - began,
               "scope": "Original-ID consecutive pair characterization; excluded from query timing; no learned reordering"}
    report = {
        "dataset_name": data_path.name,
        "dataset_url": "https://snap.stanford.edu/data/" + data_path.name
                       if data_path.name in ("ca-GrQc.txt.gz", "facebook_combined.txt.gz") else None,
        "sha256": hashlib.sha256(data_path.read_bytes()).hexdigest(), "source": source_info,
        "normalization_seconds": normalization_seconds, "machine": platform.platform(), "python": platform.python_version(),
        "groups": groups, "block_size": block_size, "sample_count": sample_count,
        "sample_seed": sample_seed, "repeats": repeats, "selected_source_ids": selected,
        "methods": methods, "physical_pair_profile": profile,
        "query_admission": {"merge_cap": merge_cap, "selective_source_cap": 4096,
                            "pair_state_cap": pair_state_cap, "pair_work_cap": pair_work_cap,
                            "pair_query_work_cap": pair_query_work_cap},
        "scope": "Cached Python/file experiment; fixture/oracle resident; no physical RAM, GDS or dedicated-host claim",
        "timing_scope": "Run after tests/reviewers terminate. Source file creation and oracle excluded; reads, bounds, body scoring, staged output, test readback and cleanup included.",
        "storage_scope": "Common full selective metadata for matched ablations; simpler modes skip unused payload reads but could retain smaller standalone formats. DAAT has its independent paid target postings.",
        "build_scope": "Incremental selective conversion reads paid laminar sidecar and target postings. Original Snapshot builder also makes unused RMQ; not an optimized standalone build for either architecture. All stages reported separately.",
        "runs": [], "summaries": [],
    }
    with tempfile.TemporaryDirectory(prefix="selective-public-") as temporary:
        root = Path(temporary) / "index"
        began = perf_counter()
        prepared = reference.write_normalized_target_fixture(root, targets)
        report["canonical_seconds"] = perf_counter() - began
        began = perf_counter()
        snapshot = reference.Snapshot(prepared)
        report["shared_index_seconds"] = perf_counter() - began
        report["shared_file_bytes"] = {path.name: path.stat().st_size for path in root.iterdir() if path.is_file()}
        before = reference.stats.copy()
        sidecar, metrics = build_laminar_capacity_sidecar(reference, snapshot, groups=groups, block_size=block_size)
        report["sidecar_build"] = dict(metrics)
        report["sidecar_build"]["logical_counters"] = dict(reference.stats - before)
        before = reference.stats.copy()
        index, metrics = build_selective_capacity_index(reference, snapshot, sidecar)
        report["selective_build"] = dict(metrics)
        report["selective_build"]["logical_counters"] = dict(reference.stats - before)
        report["selective_file_bytes"] = {path.name: path.stat().st_size for path in index.root.iterdir()}
        for query, node in enumerate(selected):
            values, k = graph[node], 10
            source = root / "source"
            reference.write_normalized_source_file(source, values)
            expected = compute_finite_similarity_oracle(targets, values, node, k)
            for repeat in range(repeats):
                offset = (query + repeat) % len(methods)
                for mode in methods[offset:] + methods[:offset]:
                    before = reference.stats.copy()
                    began = perf_counter()
                    if mode == "posting_merge":
                        rows, metrics = select_posting_merge_answers(reference, snapshot, source, node, k,
                                                                     merge_cap=merge_cap, heap_cap=k)
                    else:
                        rows, metrics = select_indexed_capacity_answers(reference, snapshot, index, source, node,
                                                                        k, mode=mode, merge_cap=merge_cap, heap_cap=k,
                                                                        pair_state_cap=pair_state_cap, pair_work_cap=pair_work_cap,
                                                                        pair_query_work_cap=pair_query_work_cap)
                    elapsed = perf_counter() - began
                    assert rows == expected, (node, mode)
                    report["runs"].append({"node": node, "degree": len(values), "k": k, "repeat": repeat,
                                           "mode": mode, "seconds": elapsed, "metrics": dict(metrics),
                                           "logical_counters": dict(reference.stats - before)})
            summary = {"node": node, "degree": len(values)}
            for mode in methods:
                runs = [row for row in report["runs"] if row["node"] == node and row["mode"] == mode]
                summary[mode] = {"median_seconds": statistics.median(row["seconds"] for row in runs),
                                 "metrics": runs[0]["metrics"], "logical_counters": runs[0]["logical_counters"]}
            report["summaries"].append(summary)
            assert not list(root.glob("query-*"))
    report["complete_results_checked"] = len(report["runs"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--groups", type=int, default=4)
    parser.add_argument("--block-size", type=int, default=2)
    parser.add_argument("--sample-count", type=int, default=24)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--include-overlap", action="store_true")
    parser.add_argument("--merge-cap", type=int, default=128)
    parser.add_argument("--pair-state-cap", type=int, default=4096)
    parser.add_argument("--pair-work-cap", type=int, default=65536)
    parser.add_argument("--pair-query-work-cap", type=int, default=10**7)
    arguments = parser.parse_args()
    result = run_selective_public_comparison(arguments.data, arguments.output, arguments.groups,
                                             arguments.block_size, arguments.sample_count, arguments.repeats,
                                             include_overlap=arguments.include_overlap, merge_cap=arguments.merge_cap,
                                             pair_state_cap=arguments.pair_state_cap, pair_work_cap=arguments.pair_work_cap,
                                             pair_query_work_cap=arguments.pair_query_work_cap)
    print(json.dumps({key: result[key] for key in ("source", "groups", "block_size", "complete_results_checked")}, indent=2))
