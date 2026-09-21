"""Serial research measurement, with full original-ID binary output audits.

Fresh processes do not mean cold OS cache. RSS is observed, never enforced.
All cases are constructed families, not workload-prevalence evidence.
"""

import argparse
import hashlib
from itertools import chain, combinations
import json
import os
from pathlib import Path
import platform
import resource
import sqlite3
import statistics
import struct
import subprocess
import sys
import time

from fault_cover_sqlite_source import build_sqlite_fault_source, SqlitePreparedFaultSource
from probe_fault_cover_connectivity import run_prepared_fault_connectivity


CASES = (
    ("sparse-matching", 1024, 0),
    ("outside-seeding", 1024, 0),
    ("global-row-replay", 64, 4096),
    ("dense-negative", 128, 0),
    ("deleted-center", 10000, 0),
)
PLANS = ("blocked", "complement", "capped")


def construct_generated_source_streams(kind, size, extra):
    if kind == "sparse-matching":
        return range(size), (range(size),), ((v, v + 1) for v in range(0, size, 2)), range(size)
    if kind == "outside-seeding":
        negatives = chain(((v, v + 1) for v in range(0, size, 2)), ((v, size) for v in range(size // 2)))
        return range(size + 1), (range(size + 1),), negatives, range(size)
    if kind == "global-row-replay":
        outside = range(size + 1, size + extra + 1)
        groups = chain(((0, v) for v in range(1, size + 1)), (chain((0,), outside),))
        return range(size + extra + 1), groups, ((0, v) for v in outside), range(size + 1)
    if kind == "dense-negative":
        return range(size), (range(size),), combinations(range(size), 2), range(size)
    if kind == "deleted-center":
        return range(size), (range(size),), ((0, v) for v in range(1, size)), (0,)
    raise ValueError("unknown generated family")


def hash_file_contents_streamed(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(65536), b""):
            digest.update(block)
    return digest.hexdigest()


def inspect_prepared_storage_pages(path):
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    try:
        page_size = connection.execute("PRAGMA page_size").fetchone()[0]
        page_count = connection.execute("PRAGMA page_count").fetchone()[0]
        try:
            objects = dict(connection.execute("SELECT name, SUM(pgsize) FROM dbstat GROUP BY name"))
        except sqlite3.OperationalError:
            objects = None
        return dict(page_size=page_size, page_count=page_count, bytes_by_object=objects)
    finally:
        connection.close()


def run_sqlite_connectivity_worker(path, output, plan):
    started = time.perf_counter()
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    try:
        cover = tuple(row[0] for row in connection.execute("SELECT vertex_id FROM cover ORDER BY vertex_id"))
    finally:
        connection.close()
    digest, rows = hashlib.sha256(), 0
    with SqlitePreparedFaultSource(path, cache_kib=256, max_open_cursors=4) as source:
        with open(output, "xb", buffering=65536) as stream:
            def emit_complete_binary_label(vertex, label):
                nonlocal rows
                record = struct.pack("<QQ", vertex, label)
                stream.write(record)
                digest.update(record)
                rows += 1
            receipt = run_prepared_fault_connectivity(
                source, cover, emit_complete_binary_label,
                max_state_nodes=source.factor_count + len(cover), core_plan=plan,
            )
            stream.flush()
            os.fsync(stream.fileno())
        if source.events["active_cursors"]:
            raise AssertionError("query leaked a prepared cursor")
        source_events = dict(source.events)
    elapsed = time.perf_counter() - started
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return dict(plan=plan, query_output_seconds=elapsed, kernel=receipt, source=source_events,
                output_rows=rows, output_bytes=output.stat().st_size, output_sha256=digest.hexdigest(),
                process_maxrss_bytes=int(peak if sys.platform == "darwin" else peak * 1024))


def audit_generated_output_labels(path, kind, size, extra):
    started = time.perf_counter()
    count = size + (1 if kind == "outside-seeding" else extra + 1 if kind == "global-row-replay" else 0)
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for expected_vertex in range(count):
            record = stream.read(16)
            if len(record) != 16:
                raise AssertionError("incomplete original-ID output")
            vertex, label = struct.unpack("<QQ", record)
            expected = 0
            if kind == "dense-negative":
                expected = expected_vertex
            elif kind == "deleted-center":
                expected = int(expected_vertex != 0)
            elif kind == "global-row-replay" and expected_vertex > size:
                expected = size + 1
            if (vertex, label) != (expected_vertex, expected):
                raise AssertionError((kind, vertex, label, expected_vertex, expected))
            digest.update(record)
        if stream.read(1):
            raise AssertionError("unexpected trailing output")
    return dict(rows=count, output_sha256=digest.hexdigest(), audit_seconds=time.perf_counter() - started)


def execute_serial_workflow_measurement(destination):
    destination.mkdir(parents=True, exist_ok=False)
    script = Path(__file__).absolute()
    manifest = dict(schema="capped-sqlite-v1", platform=platform.platform(), python=sys.version,
                    sqlite=sqlite3.sqlite_version, cases=CASES, plans=PLANS, rounds=3,
                    source_sha256={name: hash_file_contents_streamed(script.with_name(name)) for name in
                                   (script.name, "fault_cover_sqlite_source.py", "probe_fault_cover_connectivity.py")},
                    scope="constructed families; uncontrolled OS cache; serial fresh workers; no physical cap",
                    builds=[], runs=[])
    with open(destination / "events.jsonl", "x") as events:
        def save_generated_evidence_event(event):
            events.write(json.dumps(event, sort_keys=True) + "\n")
            events.flush()
        save_generated_evidence_event(dict(type="header", metadata=manifest))
        for kind, size, extra in CASES:
            path = destination / f"{kind}.sqlite"
            vertices, groups, deleted, cover = construct_generated_source_streams(kind, size, extra)
            build = build_sqlite_fault_source(path, vertices, groups, deleted, cover=cover,
                                             cache_kib=256, batch_records=4096)
            build.update(case=kind, storage=inspect_prepared_storage_pages(path), artifact_sha256=hash_file_contents_streamed(path))
            manifest["builds"].append(build)
            save_generated_evidence_event(dict(type="build", receipt=build))
            for round_number in range(3):
                order = PLANS[round_number:] + PLANS[:round_number]
                for plan in order:
                    output = destination / f"{kind}-{round_number}-{plan}.labels"
                    worker = subprocess.run([sys.executable, "-B", str(script), "--worker", str(path),
                                             "--output", str(output), "--plan", plan],
                                            check=True, capture_output=True, text=True)
                    run = json.loads(worker.stdout)
                    audit = audit_generated_output_labels(output, kind, size, extra)
                    if audit["output_sha256"] != run["output_sha256"] or audit["rows"] != run["output_rows"]:
                        raise AssertionError("worker and independent output receipts disagree")
                    run.update(case=kind, round=round_number, audit=audit, output_file=output.name)
                    manifest["runs"].append(run)
                    save_generated_evidence_event(dict(type="run", receipt=run))
            times = {plan: statistics.median(run["query_output_seconds"] for run in manifest["runs"]
                                             if run["case"] == kind and run["plan"] == plan) for plan in PLANS}
            print(json.dumps(dict(case=kind, median_seconds=times)), flush=True)
        with open(destination / "receipt.json", "x") as receipt_file:
            json.dump(manifest, receipt_file, indent=2, sort_keys=True)
            receipt_file.write("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--plan", choices=PLANS)
    options = parser.parse_args()
    if options.worker:
        if not options.plan:
            parser.error("--worker requires --plan")
        print(json.dumps(run_sqlite_connectivity_worker(options.worker.absolute(), options.output, options.plan)))
    else:
        execute_serial_workflow_measurement(options.output.absolute())
