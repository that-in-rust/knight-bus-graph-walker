"""Matched summary ablations and DAAT, with all metadata and result costs paid."""

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
from probe_laminar_capacity_similarity import build_laminar_capacity_sidecar, select_laminar_capacity_answers
from probe_posting_merge_similarity import select_posting_merge_answers
from test_bounded_seed_similarity import compute_finite_similarity_oracle, load_runwise_reference_module


def run_laminar_public_comparison(data_path, output_path, groups=16, block_size=64,
                                  sample_count=24, repeats=3, sample_seed=920520, include_paired=False):
    assert type(repeats) is int and repeats > 0
    reference = load_runwise_reference_module(sort_capacity=4096, sort_fanin=16)
    began = perf_counter()
    with gzip.open(data_path, "rt", encoding="ascii") as stream:
        graph, source_info = read_undirected_graph_source(stream)
    normalization_seconds = perf_counter() - began
    targets = sorted(graph.items())
    selected = select_public_query_sources(graph, sample_count, sample_seed)
    methods = ("union", "partition", "laminar", "posting_merge")
    if include_paired:
        methods = methods[:-1] + ("paired", "paired_lazy", methods[-1])
    report = {
        "dataset_name": data_path.name,
        "dataset_url": "https://snap.stanford.edu/data/ca-GrQc.txt.gz" if data_path.name == "ca-GrQc.txt.gz" else None,
        "sha256": hashlib.sha256(data_path.read_bytes()).hexdigest(),
        "source": source_info, "normalization_seconds": normalization_seconds,
        "machine": platform.platform(), "python": platform.python_version(),
        "groups": groups, "block_size": block_size, "sample_count": sample_count,
        "sample_seed": sample_seed, "repeats": repeats, "selected_source_ids": selected,
        "include_paired": include_paired,
        "scope": "Small cached Python/file experiment; normalization and oracle resident; no physical RAM cap, GDS baseline, confidence interval or customer distribution",
        "timing_scope": "Source-file creation and oracle excluded; source reads, summaries, body scoring, staged output, test readback and cleanup included. Sidecar builds timed separately.",
        "storage_scope": "Each summary mode has its own specialized sidecar. Alternative sidecars coexist only for experiment convenience. DAAT needs postings/directory/inverse/meta, not the RMQ tree or canonical pairs at query time.",
        "snapshot_scope": "Shared Snapshot builder includes an unused RMQ; do not charge that entire builder to either standalone implementation. Each sidecar is an incremental build from canonical rows.",
        "runs": [], "summaries": [], "sidecar_builds": {},
    }
    with tempfile.TemporaryDirectory(prefix="laminar-public-") as temporary:
        root = Path(temporary) / "index"
        began = perf_counter()
        prepared = reference.write_normalized_target_fixture(root, targets)
        report["canonical_seconds"] = perf_counter() - began
        began = perf_counter()
        snapshot = reference.Snapshot(prepared)
        report["shared_index_seconds"] = perf_counter() - began
        report["shared_file_bytes"] = {path.name: path.stat().st_size for path in root.iterdir() if path.is_file()}
        sidecars = {}
        for mode in ("union", "partition", "laminar"):
            before = reference.stats.copy()
            sidecar, metrics = build_laminar_capacity_sidecar(reference, snapshot, groups=groups,
                                                             block_size=block_size, mode=mode)
            sidecars[mode] = sidecar
            report["sidecar_builds"][mode] = dict(metrics)
            report["sidecar_builds"][mode]["logical_counters"] = dict(reference.stats - before)
        for query, node in enumerate(selected):
            source = root / "source"
            values, k = graph[node], 10
            reference.write_normalized_source_file(source, values)
            expected = compute_finite_similarity_oracle(targets, values, node, k)
            for repeat in range(repeats):
                offset = (query + repeat) % len(methods)
                for mode in methods[offset:] + methods[:offset]:
                    before = reference.stats.copy()
                    began = perf_counter()
                    if mode == "posting_merge":
                        rows, metrics = select_posting_merge_answers(reference, snapshot, source, node, k,
                                                                     merge_cap=128, heap_cap=k)
                    else:
                        sidecar = sidecars["laminar" if mode in ("paired", "paired_lazy") else mode]
                        rows, metrics = select_laminar_capacity_answers(reference, snapshot, sidecar,
                                                                        source, node, k, mode=mode, heap_cap=k)
                    elapsed = perf_counter() - began
                    assert rows == expected, (node, mode)
                    counters = dict(reference.stats - before)
                    report["runs"].append({"node": node, "degree": len(values), "k": k, "repeat": repeat,
                                           "mode": mode, "seconds": elapsed, "metrics": dict(metrics),
                                           "logical_counters": counters})
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
    parser.add_argument("--groups", type=int, default=16)
    parser.add_argument("--block-size", type=int, default=64)
    parser.add_argument("--sample-count", type=int, default=24)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--include-paired", action="store_true")
    arguments = parser.parse_args()
    result = run_laminar_public_comparison(arguments.data, arguments.output, arguments.groups,
                                           arguments.block_size, arguments.sample_count, arguments.repeats,
                                           include_paired=arguments.include_paired)
    print(json.dumps({key: result[key] for key in ("source", "groups", "block_size", "complete_results_checked")}, indent=2))
