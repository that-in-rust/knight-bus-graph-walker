#!/usr/bin/env python3
"""Run Clarity over the Neo4j family reference repos and parse file edges."""

from __future__ import annotations

import csv
import re
import subprocess
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
FAMILY_ROOT = REPO_ROOT / "gitrefrepo" / "Neo4j family"
OUT_DIR = REPO_ROOT / "docs_PRD03" / "reference-learning" / "neo4j-family-dependency-graphs"
CLARITY = Path("/tmp/codex-clarity-npm/node_modules/.bin/clarity")
TIMEOUT_SECONDS = 300

EXCLUDES = ",".join(
    [
        ".git",
        "build",
        "target",
        "node_modules",
        "dist",
        "out",
        ".gradle",
        "__pycache__",
        ".pytest_cache",
        ".tox",
        "venv",
        ".venv",
        ".idea",
        ".vscode",
        "coverage",
        "bin",
        "obj",
    ]
)


NODE_RE = re.compile(r'^\s+"([^"]+)"\s+\[')
EDGE_RE = re.compile(r'^\s+"([^"]+)"\s*->\s*"([^"]+)"')
CYCLE_RE = re.compile(r"^\s*// C\d+:")


def clean(value: object) -> str:
    return str(value).replace("\t", " ").replace("\n", " ").strip()


def write_tsv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, delimiter="\t", fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: clean(row.get(field, "")) for field in fieldnames})


def parse_dot(repo_name: str, dot_text: str) -> tuple[list[dict[str, str]], list[dict[str, str]], int]:
    nodes: set[str] = set()
    edges: list[dict[str, str]] = []
    cycle_count = 0

    for line in dot_text.splitlines():
        if CYCLE_RE.match(line):
            cycle_count += 1
            continue
        edge = EDGE_RE.match(line)
        if edge:
            source, target = edge.groups()
            nodes.add(source)
            nodes.add(target)
            edges.append({"repo": repo_name, "source_file": source, "target_file": target})
            continue
        node = NODE_RE.match(line)
        if node and "->" not in line:
            nodes.add(node.group(1))

    node_rows = [{"repo": repo_name, "file": node} for node in sorted(nodes)]
    return node_rows, edges, cycle_count


def run_clarity(repo_path: Path) -> dict[str, object]:
    repo_name = repo_path.name
    dot_path = OUT_DIR / "dot" / f"{repo_name}.dot"
    stderr_path = OUT_DIR / "stderr" / f"{repo_name}.stderr.txt"

    command = [
        str(CLARITY),
        "show",
        "--repo",
        str(repo_path),
        "--all",
        "-f",
        "dot",
        "--no-stats",
        "--exclude",
        EXCLUDES,
    ]

    started = time.monotonic()
    try:
        proc = subprocess.run(
            command,
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            timeout=TIMEOUT_SECONDS,
            check=False,
        )
        duration = time.monotonic() - started
        dot_path.write_text(proc.stdout, encoding="utf-8")
        stderr_path.write_text(proc.stderr, encoding="utf-8")
        nodes, edges, cycle_count = parse_dot(repo_name, proc.stdout)
        status = "ok" if proc.returncode == 0 else f"exit_{proc.returncode}"
        return {
            "repo": repo_name,
            "status": status,
            "duration_seconds": f"{duration:.2f}",
            "dot_file": str(dot_path.relative_to(REPO_ROOT)),
            "stderr_file": str(stderr_path.relative_to(REPO_ROOT)),
            "dot_bytes": dot_path.stat().st_size if dot_path.exists() else 0,
            "node_count": len(nodes),
            "edge_count": len(edges),
            "cycle_count": cycle_count,
            "stderr_excerpt": proc.stderr[:500],
            "nodes": nodes,
            "edges": edges,
        }
    except subprocess.TimeoutExpired as err:
        duration = time.monotonic() - started
        stdout = err.stdout or ""
        stderr = err.stderr or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", errors="ignore")
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", errors="ignore")
        dot_path.write_text(stdout, encoding="utf-8")
        stderr_path.write_text(stderr, encoding="utf-8")
        nodes, edges, cycle_count = parse_dot(repo_name, stdout)
        return {
            "repo": repo_name,
            "status": "timeout",
            "duration_seconds": f"{duration:.2f}",
            "dot_file": str(dot_path.relative_to(REPO_ROOT)),
            "stderr_file": str(stderr_path.relative_to(REPO_ROOT)),
            "dot_bytes": dot_path.stat().st_size if dot_path.exists() else 0,
            "node_count": len(nodes),
            "edge_count": len(edges),
            "cycle_count": cycle_count,
            "stderr_excerpt": stderr[:500],
            "nodes": nodes,
            "edges": edges,
        }


def write_markdown(summary_rows: list[dict[str, object]]) -> None:
    lines = [
        "# Neo4j Family Clarity Dependency Graphs",
        "",
        "Generated with `clarity show --all -f dot --no-stats` per nested repository in `gitrefrepo/Neo4j family`.",
        "",
        "Artifacts:",
        "",
        "- `repo-summary.tsv`: per-repo status and graph size.",
        "- `family-file-nodes.tsv`: parsed file nodes from all successful/partial DOT outputs.",
        "- `family-file-edges.tsv`: parsed file-to-file dependency edges from all successful/partial DOT outputs.",
        "- `dot/*.dot`: raw Clarity DOT per repo.",
        "- `stderr/*.stderr.txt`: command stderr per repo.",
        "",
        "| Repo | Status | Nodes | Edges | Cycles | DOT bytes |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in summary_rows:
        lines.append(
            "| {repo} | {status} | {node_count} | {edge_count} | {cycle_count} | {dot_bytes} |".format(**row)
        )
    lines.extend(
        [
            "",
            "Interpretation notes:",
            "",
            "- This is a file-level dependency graph, not a call graph.",
            "- Clarity extracts structural imports/references for supported languages only.",
            "- Documentation/spec repositories can have low edge counts even when strategically important.",
            "- Large Neo4j/GDS repos should be explored from this graph with focused reach queries around specific files.",
        ]
    )
    (OUT_DIR / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    if not CLARITY.exists():
        raise SystemExit(f"Clarity binary not found: {CLARITY}")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "dot").mkdir(exist_ok=True)
    (OUT_DIR / "stderr").mkdir(exist_ok=True)

    summary_rows: list[dict[str, object]] = []
    node_rows: list[dict[str, str]] = []
    edge_rows: list[dict[str, str]] = []

    for repo_path in sorted(path for path in FAMILY_ROOT.iterdir() if path.is_dir()):
        result = run_clarity(repo_path)
        summary_rows.append({key: value for key, value in result.items() if key not in {"nodes", "edges"}})
        node_rows.extend(result["nodes"])
        edge_rows.extend(result["edges"])
        print(
            f"{result['repo']}\t{result['status']}\t"
            f"nodes={result['node_count']}\tedges={result['edge_count']}\tcycles={result['cycle_count']}"
        )

    write_tsv(
        OUT_DIR / "repo-summary.tsv",
        [
            "repo",
            "status",
            "duration_seconds",
            "dot_file",
            "stderr_file",
            "dot_bytes",
            "node_count",
            "edge_count",
            "cycle_count",
            "stderr_excerpt",
        ],
        summary_rows,
    )
    write_tsv(OUT_DIR / "family-file-nodes.tsv", ["repo", "file"], node_rows)
    write_tsv(OUT_DIR / "family-file-edges.tsv", ["repo", "source_file", "target_file"], edge_rows)
    write_markdown(summary_rows)


if __name__ == "__main__":
    main()
