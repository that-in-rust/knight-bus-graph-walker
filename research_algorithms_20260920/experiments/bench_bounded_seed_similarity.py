"""Small public-data comparison; resident fixture/oracle is not a RAM-cap claim."""

import argparse
import gzip
import hashlib
import json
import platform
import random
import statistics
import tempfile
from pathlib import Path
from time import perf_counter

from probe_bounded_seed_similarity import (
    scan_exact_similarity_targets,
    select_bounded_runwise_answers,
    select_merged_runwise_answers,
    select_pruned_runwise_answers,
)
from test_bounded_seed_similarity import (
    compute_finite_similarity_oracle,
    load_runwise_reference_module,
)
from probe_posting_merge_similarity import select_posting_merge_answers


def read_undirected_graph_source(lines):
    graph = {}
    raw, loops, duplicates = 0, 0, 0
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split()
        if len(fields) != 2:
            raise ValueError("expected two endpoint IDs")
        left, right = map(int, fields)
        if not 0 <= left < 2**63 or not 0 <= right < 2**63:
            raise ValueError("source IDs must fit nonnegative signed 64-bit fields")
        raw += 1
        graph.setdefault(left, set())
        graph.setdefault(right, set())
        if left == right:
            loops += 1
        elif right in graph[left]:
            duplicates += 1
        else:
            graph[left].add(right)
            graph[right].add(left)
    memberships = sum(map(len, graph.values()))
    return graph, {"raw_rows": raw, "self_loop_rows": loops,
                   "duplicate_rows": duplicates, "nodes": len(graph),
                   "edges": memberships // 2, "memberships": memberships}


def select_public_query_sources(graph, sample_count=0, sample_seed=920520):
    if not graph or type(sample_count) is not int or sample_count < 0:
        raise ValueError("nonempty graph and nonnegative integer sample count required")
    ranked = sorted(graph, key=lambda node: (len(graph[node]), node))
    selected = list(dict.fromkeys(ranked[i] for i in (0, len(ranked) // 4, len(ranked) // 2,
                                                     3 * len(ranked) // 4, len(ranked) - 2, len(ranked) - 1)))
    available = sorted(set(graph) - set(selected))
    if sample_count > len(available):
        raise ValueError("sample exceeds remaining source population")
    return selected + random.Random(sample_seed).sample(available, sample_count)


def run_public_similarity_comparison(data_path, output_path, sort_capacity=4, sort_fanin=2, merge_cap=128,
                                     include_pruned=False, sample_count=0, sample_seed=920520,
                                     include_daat=False):
    reference = load_runwise_reference_module(sort_capacity=sort_capacity, sort_fanin=sort_fanin)
    start = perf_counter()
    with gzip.open(data_path, "rt", encoding="ascii") as stream:
        graph, source_info = read_undirected_graph_source(stream)
    normalize_seconds = perf_counter() - start
    targets = sorted(graph.items())
    selected = select_public_query_sources(graph, sample_count=sample_count, sample_seed=sample_seed)
    report = {
        "dataset_url": "https://snap.stanford.edu/data/ca-GrQc.txt.gz",
        "sha256": hashlib.sha256(data_path.read_bytes()).hexdigest(),
        "source": source_info, "normalization_seconds": normalize_seconds,
        "machine": platform.platform(), "python": platform.python_version(),
        "scope": "Small cached Python/file comparison; resident normalization/oracle; no physical RAM cap or GDS run",
        "query_selection": "Up to six degree-order strata, followed by a seeded uniform sample without replacement from remaining node IDs; not a customer workload distribution",
        "sample_count": sample_count, "sample_seed": sample_seed, "selected_source_ids": selected,
        "sort_capacity_records": sort_capacity, "sort_fanin": sort_fanin, "endpoint_merge_cap": merge_cap,
        "budget_scope": "Separate admitted record budgets, not matched measured RAM bytes; Python objects and cursor/file overhead remain unmeasured",
        "runs": [], "summaries": [],
    }
    with tempfile.TemporaryDirectory(prefix="bounded-seed-public-") as temporary:
        root = Path(temporary) / "index"
        start = perf_counter()
        prepared = reference.write_normalized_target_fixture(root, targets)
        report["canonical_files_seconds"] = perf_counter() - start
        start = perf_counter()
        snapshot = reference.Snapshot(prepared)
        report["index_build_seconds"] = perf_counter() - start
        report["prepared_files_bytes"] = {path.name: path.stat().st_size for path in root.iterdir() if path.is_file()}
        report["index_intervals"] = snapshot.L_all
        report["build_logical_counters"] = dict(reference.stats)
        methods = ("all_heads", "bounded_seeds", "merged_seeds", "stream_scan")
        if include_pruned:
            methods = methods[:-1] + ("pruned_seeds", methods[-1])
        if include_daat:
            methods = methods[:-1] + ("posting_merge", methods[-1])
        for query, node in enumerate(selected):
            values, k = graph[node], 10
            source = root / "source"
            reference.write_normalized_source_file(source, values)
            expected = compute_finite_similarity_oracle(targets, values, node, k)
            for repeat in range(3):
                offset = (query + repeat) % len(methods)
                for mode in methods[offset:] + methods[:offset]:
                    before = reference.stats.copy()
                    start = perf_counter()
                    if mode == "all_heads":
                        rows, metrics = reference.select_runwise_exact_answers(snapshot, source, node, k)
                    elif mode == "bounded_seeds":
                        rows, metrics = select_bounded_runwise_answers(reference, snapshot, source, node, k, heap_cap=19)
                    elif mode == "merged_seeds":
                        rows, metrics = select_merged_runwise_answers(reference, snapshot, source, node, k,
                                                                      heap_cap=19, merge_cap=merge_cap)
                    elif mode == "pruned_seeds":
                        rows, metrics = select_pruned_runwise_answers(reference, snapshot, source, node, k,
                                                                      heap_cap=19, merge_cap=merge_cap)
                    elif mode == "posting_merge":
                        rows, metrics = select_posting_merge_answers(reference, snapshot, source, node, k,
                                                                     heap_cap=10, merge_cap=merge_cap)
                    else:
                        rows, metrics = scan_exact_similarity_targets(reference, snapshot, source, node, k, heap_cap=10)
                    elapsed = perf_counter() - start
                    assert rows == expected
                    delta = {key: value - before.get(key, 0) for key, value in reference.stats.items()
                             if value != before.get(key, 0) and not key.endswith("seconds")}
                    report["runs"].append({"node": node, "degree": len(values), "k": k,
                                           "repeat": repeat, "mode": mode, "seconds": elapsed,
                                           "metrics": dict(metrics), "logical_counters": delta})
            same_query = [row for row in report["runs"] if row["node"] == node]
            summary = {"node": node, "degree": len(values), "k": k}
            for mode in methods:
                runs = [row for row in same_query if row["mode"] == mode]
                summary[mode] = {"median_seconds": statistics.median(row["seconds"] for row in runs),
                                  "heap_peak": max(row["metrics"].get("heap_peak", 0) for row in runs),
                                  "metrics": runs[0]["metrics"]}
            try:
                reference.select_runwise_exact_answers(snapshot, source, node, k, heap_cap=19)
            except AssertionError as error:
                assert str(error) == "heap admission"
                summary["old_under_19_slots"] = "refused"
            else:
                summary["old_under_19_slots"] = "admitted"
            assert not list(root.glob("query-*"))
            report["summaries"].append(summary)
        report["complete_results_checked"] = len(report["runs"])
        report["builder_sort_peak_records"] = reference.stats["sort_buffer_peak"]
        report["builder_fanin_peak"] = reference.stats["fanin_peak"]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({key: report[key] for key in ("source", "normalization_seconds", "canonical_files_seconds",
                                                  "index_build_seconds", "prepared_files_bytes", "index_intervals",
                                                  "complete_results_checked")}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sort-capacity", type=int, default=4)
    parser.add_argument("--sort-fanin", type=int, default=2)
    parser.add_argument("--merge-cap", type=int, default=128)
    parser.add_argument("--include-pruned", action="store_true")
    parser.add_argument("--sample-count", type=int, default=0)
    parser.add_argument("--sample-seed", type=int, default=920520)
    parser.add_argument("--include-daat", action="store_true")
    arguments = parser.parse_args()
    run_public_similarity_comparison(arguments.data, arguments.output, arguments.sort_capacity,
                                    arguments.sort_fanin, arguments.merge_cap, arguments.include_pruned,
                                    arguments.sample_count, arguments.sample_seed, arguments.include_daat)
