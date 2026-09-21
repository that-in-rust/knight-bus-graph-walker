"""Run complete-output checks or fresh-process measurements for A06.

Generated sources and results live in a temporary directory. Receipts survive
in an exclusively created JSONL file; existing evidence is never overwritten.
"""

import argparse
from datetime import datetime, timezone
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import platform
import re
import struct
import subprocess
import tempfile

from verify_embedding_packed_results import verify_complete_packed_output


MODES = ("sum", "mean", "disk")
BITS = {"sum": 24, "mean": 26, "disk": 26, "arena": 26, "disk6": 26}
TAU = "0.0001"
SMALL_CASES = (
    ("zero_depth", 4, 2, 1, 0, 0),
    ("single_step", 8, 3, 4, 1, 1),
    ("signed_layers", 12, 4, 7, 4, 7),
    ("overlap", 24, 7, 8, 6, 42),
    ("nonsquare_dimension", 32, 11, 3, 5, 99),
    ("longer_history", 48, 13, 2, 8, 65537),
    ("inactive_features", 12, 6, 7, 4, 42),
)
BENCH_CASES = (
    ("cardinality", 262144, 4096, 64, 8, 42),
    ("alignment", 65536, 16384, 32, 8, 42),
    ("depth", 32768, 512, 16, 24, 42),
)


def digest_complete_file_bytes(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def compare_complete_result_files(left, right):
    with left.open("rb") as first, right.open("rb") as second:
        while True:
            a, b = first.read(1024 * 1024), second.read(1024 * 1024)
            if a != b:
                raise AssertionError("complete mean/disk output files differ")
            if not a:
                return


def append_durable_evidence_record(stream, record):
    stream.write(json.dumps(record, sort_keys=True) + "\n")
    stream.flush()
    print(json.dumps({key: record[key] for key in
                      ("event", "case", "repeat", "mode", "returncode", "status")
                      if key in record}), flush=True)


def execute_measured_engine_command(binary, arguments, measured):
    command = [str(binary), *map(str, arguments)]
    if measured:
        command = ["/usr/bin/time", "-l", *command]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    record = {"command": command, "returncode": result.returncode,
              "receipt": json.loads(result.stdout), "stderr": result.stderr}
    if measured:
        for label, field in (("maximum resident set size", "max_rss_bytes"),
                             ("peak memory footprint", "peak_footprint_bytes")):
            match = re.search(r"(?m)^\s*(\d+)\s+" + label + r"\s*$", result.stderr)
            if match is None:
                raise AssertionError(f"missing Darwin time metric: {label}")
            record[field] = int(match[1])
        match = re.search(r"([0-9.]+) real\s+([0-9.]+) user\s+([0-9.]+) sys", result.stderr)
        if match is None:
            raise AssertionError("missing fresh-process elapsed/CPU measurement")
        record.update(zip(("wall_seconds", "user_seconds", "sys_seconds"), map(float, match.groups())))
    return record


def verify_successful_run_receipt(record, source, output, nodes, dimension):
    receipt = record["receipt"]
    assert record["returncode"] == 0 and receipt["admitted"], record
    assert receipt["rows"] == receipt["staged_rows"] == nodes
    expected = 40 + 8 * (dimension + 1) * nodes
    assert receipt["output_bytes"] == expected == (output / "result.bin").stat().st_size
    assert receipt["bound_units_2neg35"] <= receipt["tau_floor_units_2neg35"]
    files = {path.name: path.stat().st_size for path in output.iterdir() if path.is_file()}
    assert set(files) == {"prepared.bin", "result.bin"}, files
    assert receipt["retained_disk_bytes"] == sum(files.values())
    assert receipt["peak_disk_bytes"] >= receipt["retained_disk_bytes"]
    assert digest_complete_file_bytes(source) == digest_complete_file_bytes(output / "prepared.bin")
    record["result_sha256"] = digest_complete_file_bytes(output / "result.bin")
    record["retained_files"] = files


def run_complete_evidence_suite(arguments):
    if arguments.suite == "bench" and platform.system() != "Darwin":
        raise SystemExit("bench time parser is explicitly Darwin-only")
    if arguments.repeats < 1:
        raise SystemExit("repeats must be positive")
    binary = arguments.binary.resolve(strict=True)
    source_code = Path(__file__).with_name("embedding_packed_workflow.rs")
    cases = SMALL_CASES if arguments.suite == "verify" else BENCH_CASES
    repeats = 1 if arguments.suite == "verify" else arguments.repeats
    modes = tuple(arguments.modes)
    if len(set(modes)) != len(modes):
        raise SystemExit("duplicate modes are not a matched experiment")
    with arguments.receipts.open("x") as log, tempfile.TemporaryDirectory(prefix="kb-packed-evidence-") as temporary:
        root = Path(temporary)
        append_durable_evidence_record(log, {
            "event": "environment", "date_utc": datetime.now(timezone.utc).isoformat(),
            "suite": arguments.suite, "repeats": repeats, "platform": platform.platform(),
            "machine": platform.machine(), "python": platform.python_version(),
            "compiler": subprocess.check_output(["rustc", "--version"], text=True).strip(),
            "binary_sha256": digest_complete_file_bytes(binary),
            "engine_source_sha256": digest_complete_file_bytes(source_code),
            "runner_source_sha256": digest_complete_file_bytes(Path(__file__)),
            "oracle_source_sha256": digest_complete_file_bytes(Path(__file__).with_name("verify_embedding_packed_results.py")),
            "cache_policy": "no eviction; source just generated; OS buffered I/O; fresh process each run",
            "scope": "single-threaded research modes, not Neo4j or a physical-4GB experiment",
            "modes": modes,
            "hardware": subprocess.check_output(["sysctl", "hw.memsize", "hw.physicalcpu",
                "hw.logicalcpu", "machdep.cpu.brand_string"], text=True).strip()
                if platform.system() == "Darwin" else "not measured",
        })
        total_runs, coordinate_checks, file_comparisons = 0, 0, 0
        for name, nodes, features, dimension, depth, seed in cases:
            source = root / (name + ".source")
            generated = execute_measured_engine_command(binary, ["generate", source, nodes, features, seed], False)
            if name == "inactive_features":
                # Three active columns and three unused columns are valid input.
                source.write_bytes(b"KBMEAN01" + struct.pack("<QQQ", nodes, features, seed) +
                    b"".join(struct.pack("<QII", i, i % 3, (i + 1) % 3) for i in range(nodes)))
                generated["fixture_override"] = "cyclic memberships among first three columns; remaining columns unused"
            append_durable_evidence_record(log, {"event": "source", "case": name,
                "source_sha256": digest_complete_file_bytes(source), **generated})
            assert generated["returncode"] == 0
            for repeat in range(repeats):
                # Rotate first position to expose, not eliminate, cache/order effects.
                order = modes[repeat % len(modes):] + modes[:repeat % len(modes)]
                with tempfile.TemporaryDirectory(dir=root, prefix=name + "-") as case_directory:
                    directories = {}
                    run_receipts = {}
                    for mode in order:
                        output = Path(case_directory) / mode
                        directories[mode] = output
                        record = execute_measured_engine_command(binary,
                            ["run", mode, source, output, dimension, depth, BITS[mode], TAU],
                            arguments.suite == "bench")
                        record.update(event="run", case=name, repeat=repeat, mode=mode)
                        verify_successful_run_receipt(record, source, output, nodes, dimension)
                        run_receipts[mode] = record["receipt"]
                        if arguments.suite == "verify":
                            claimed = Fraction(record["receipt"]["bound_units_2neg35"], 1 << 35)
                            record["oracle"] = verify_complete_packed_output(source, output / "result.bin", claimed)
                            record["requested_tolerance"] = TAU
                            coordinate_checks += record["oracle"]["coordinates_checked"]
                        append_durable_evidence_record(log, record)
                        total_runs += 1
                    anchor = "mean" if "mean" in directories else "disk"
                    for equivalent in ("disk", "arena", "disk6"):
                        if anchor in directories and equivalent in directories and equivalent != anchor:
                            compare_complete_result_files(directories[anchor] / "result.bin", directories[equivalent] / "result.bin")
                            assert run_receipts[anchor]["bound_units_2neg35"] == run_receipts[equivalent]["bound_units_2neg35"]
                            append_durable_evidence_record(log, {"event": "complete_byte_equality", "case": name,
                                "repeat": repeat, "anchor": anchor, "compared": equivalent,
                                "exact_publication_bound_equal": True, "status": "passed"})
                            file_comparisons += 1
                    if "disk" in run_receipts and "disk6" in run_receipts:
                        old, new = run_receipts["disk"], run_receipts["disk6"]
                        width = 5 if depth <= 1 else 6
                        saved_plane = (16 - width) * nodes * dimension
                        for key in ("logical_bytes_read", "logical_bytes_written"):
                            assert old[key] - new[key] == (depth + 1) * saved_plane
                        assert old["peak_disk_bytes"] - new["peak_disk_bytes"] == saved_plane
                        assert old["peak_packed_plane_payload"] == new["peak_packed_plane_payload"]
                        append_durable_evidence_record(log, {"event": "c6_accounting_identity", "case": name,
                            "repeat": repeat, "record_bytes": width, "plane_bytes_saved": saved_plane,
                            "traffic_bytes_saved": 2 * (depth + 1) * saved_plane, "status": "passed"})
        refusal_runs = 0
        if arguments.suite == "verify":
            source = root / "signed_layers.source"
            broken = root / "broken.source"
            broken.write_bytes(source.read_bytes()[:-1])
            for mode in modes:
                for label, candidate, bits, tolerance in (("precision", source, 0, "0.000000000001"),
                                                         ("truncated", broken, 26, TAU)):
                    output = root / f"refused-{mode}-{label}"
                    record = execute_measured_engine_command(binary,
                        ["run", mode, candidate, output, 7, 4, bits, tolerance], False)
                    assert record["returncode"] == 2 and not record["receipt"]["admitted"], record
                    assert record["receipt"]["rows"] == 0
                    assert not (output / "result.bin").exists() and not (output / "result.stage").exists()
                    append_durable_evidence_record(log, {"event": "refusal", "case": label, "mode": mode, **record})
                    refusal_runs += 1
        append_durable_evidence_record(log, {"event": "complete", "status": "passed", "successful_runs": total_runs,
            "coordinate_checks": coordinate_checks, "full_file_comparisons": file_comparisons,
            "refusal_runs": refusal_runs})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", choices=("verify", "bench"))
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--receipts", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--modes", nargs="+", choices=tuple(BITS), default=MODES)
    run_complete_evidence_suite(parser.parse_args())
