#!/usr/bin/env python3
"""Run codebase-memory over gitrefrepo repos and record per-repo status.

The active corpus objective asks for codebase-memory browsing of every repo in
gitrefrepo. This script turns that requirement into a resumable TSV ledger.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Dict, Iterable, List, Optional


CBM = Path("/Users/amuldotexe/.codex/tooling/code-intelligence/bin/codebase-memory-mcp")

FIELDS = [
    "repo_path",
    "repo_name",
    "assigned_file",
    "inspection_level",
    "codebase_memory_status",
    "nodes",
    "edges",
    "files_discovered",
    "elapsed_sec",
    "cache_dir",
    "log_path",
    "project",
    "error_summary",
    "updated_at",
]


def read_ledger(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def read_existing(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        return {}
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))
    return {row["repo_path"]: row for row in rows}


def write_status(path: Path, rows: Iterable[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=FIELDS,
            delimiter="\t",
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in FIELDS})
    tmp.replace(path)


def safe_slug(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("_") or "repo"


def last_json_object(text: str) -> Optional[Dict[str, object]]:
    result: Optional[Dict[str, object]] = None
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            result = parsed
    return result


def discover_file_count(text: str) -> str:
    matches = re.findall(r"pipeline\.discover files=(\d+)", text)
    return matches[-1] if matches else ""


def compact_error(text: str) -> str:
    cleaned = " ".join(text.split())
    return cleaned[:400]


def run_repo(
    row: Dict[str, str],
    cache_dir: Path,
    logs_dir: Path,
    timeout_sec: int,
) -> Dict[str, str]:
    repo_path = row["repo_path"]
    repo_name = row["repo_name"]
    slug = safe_slug(repo_name)
    log_path = logs_dir / f"{slug}.log"
    env = os.environ.copy()
    env["CBM_CACHE_DIR"] = str(cache_dir)
    payload = json.dumps({"repo_path": repo_path})
    cmd = [str(CBM), "cli", "index_repository", payload]
    start = time.monotonic()

    try:
        completed = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            timeout=timeout_sec,
            check=False,
        )
        elapsed = time.monotonic() - start
        output = completed.stdout or ""
        log_path.write_text(output)
        parsed = last_json_object(output)
        if completed.returncode == 0 and parsed and parsed.get("status") == "indexed":
            status = "indexed"
            error = ""
        else:
            status = "error"
            error = compact_error(output)
        return {
            "repo_path": repo_path,
            "repo_name": repo_name,
            "assigned_file": row.get("assigned_file", ""),
            "inspection_level": row.get("inspection_level", ""),
            "codebase_memory_status": status,
            "nodes": str(parsed.get("nodes", "")) if parsed else "",
            "edges": str(parsed.get("edges", "")) if parsed else "",
            "files_discovered": discover_file_count(output),
            "elapsed_sec": f"{elapsed:.2f}",
            "cache_dir": str(cache_dir),
            "log_path": str(log_path),
            "project": str(parsed.get("project", "")) if parsed else "",
            "error_summary": error,
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
    except subprocess.TimeoutExpired as exc:
        elapsed = time.monotonic() - start
        output = exc.stdout or ""
        if isinstance(output, bytes):
            output = output.decode(errors="replace")
        log_path.write_text(output + f"\nTIMEOUT after {timeout_sec}s\n")
        return {
            "repo_path": repo_path,
            "repo_name": repo_name,
            "assigned_file": row.get("assigned_file", ""),
            "inspection_level": row.get("inspection_level", ""),
            "codebase_memory_status": "timeout",
            "elapsed_sec": f"{elapsed:.2f}",
            "cache_dir": str(cache_dir),
            "log_path": str(log_path),
            "error_summary": f"timeout after {timeout_sec}s",
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--logs-dir", type=Path, required=True)
    parser.add_argument("--timeout-sec", type=int, default=120)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--only-missing", action="store_true")
    parser.add_argument("--start-after", default="")
    args = parser.parse_args()

    if not CBM.exists():
        print(f"codebase-memory binary not found: {CBM}", file=sys.stderr)
        return 2

    ledger_rows = read_ledger(args.ledger)
    existing = read_existing(args.output)
    output_rows: Dict[str, Dict[str, str]] = dict(existing)
    args.cache_dir.mkdir(parents=True, exist_ok=True)
    args.logs_dir.mkdir(parents=True, exist_ok=True)

    started = not bool(args.start_after)
    processed = 0
    for row in ledger_rows:
        repo_path = row["repo_path"]
        repo_name = row["repo_name"]
        if not started:
            started = repo_name == args.start_after
            continue
        current = output_rows.get(repo_path)
        if args.only_missing and current and current.get("codebase_memory_status") == "indexed":
            continue
        print(f"[cbm] indexing {repo_name}", flush=True)
        output_rows[repo_path] = run_repo(row, args.cache_dir, args.logs_dir, args.timeout_sec)
        write_status(args.output, [output_rows.get(r["repo_path"], r) for r in ledger_rows])
        processed += 1
        if args.limit and processed >= args.limit:
            break

    write_status(args.output, [output_rows.get(r["repo_path"], r) for r in ledger_rows])
    print(f"[cbm] processed={processed} output={args.output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
