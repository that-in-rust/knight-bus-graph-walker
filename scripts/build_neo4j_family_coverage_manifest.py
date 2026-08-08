#!/usr/bin/env python3
"""Build the auditable all-file denominator for the Neo4j-family evidence pass."""

from __future__ import annotations

import csv
import os
from pathlib import Path
import subprocess
import sys


REPOSITORY_LANES = {
    "neo4j-src": ("01-core-compatibility", "agent-01"),
    "neo4j-docs-bolt-src": ("01-core-compatibility", "agent-01"),
    "cypher-shell-src": ("01-core-compatibility", "agent-01"),
    "cypher-dsl-src": ("01-core-compatibility", "agent-01"),
    "neo4j-gds-src": ("02-gds-lowram", "agent-02"),
    "neo4j-gds-client-src": ("02-gds-lowram", "agent-02"),
    "gds-agent-src": ("02-gds-lowram", "agent-02"),
    "graph-data-science-src": ("02-gds-lowram", "agent-02"),
    "neo4j-apoc-procedures-src": ("02-gds-lowram", "agent-02"),
    "neo4j-apoc-src": ("02-gds-lowram", "agent-02"),
    "opencypher-src": ("03-verification-ecosystem", "agent-03"),
    "neo4j-testkit-src": ("03-verification-ecosystem", "agent-03"),
    "neo4j-java-driver-src": ("03-verification-ecosystem", "agent-03"),
    "neo4j-go-driver-src": ("03-verification-ecosystem", "agent-03"),
    "neo4j-python-driver-src": ("03-verification-ecosystem", "agent-03"),
    "neo4j-javascript-driver-src": ("03-verification-ecosystem", "agent-03"),
    "neo4j-dotnet-driver-src": ("03-verification-ecosystem", "agent-03"),
    "neo4rs-src": ("03-verification-ecosystem", "agent-03"),
    "neo4j-browser-src": ("03-verification-ecosystem", "agent-03"),
    "neo4j-ogm-src": ("03-verification-ecosystem", "agent-03"),
}

CODE_EXTENSIONS = {
    ".c", ".cc", ".clj", ".cpp", ".cs", ".css", ".ex", ".exs", ".fs",
    ".go", ".groovy", ".h", ".hpp", ".html", ".java", ".js", ".jsx",
    ".kt", ".kts", ".mjs", ".php", ".py", ".rb", ".rs", ".scala",
    ".sh", ".sql", ".swift", ".ts", ".tsx", ".vue", ".wasm", ".xml",
}

BINARY_EXTENSIONS = {
    ".7z", ".a", ".bin", ".bmp", ".class", ".dll", ".dylib", ".eot",
    ".gif", ".gz", ".ico", ".jar", ".jpeg", ".jpg", ".lockb", ".mp3",
    ".mp4", ".o", ".otf", ".pdf", ".png", ".so", ".tar", ".tgz",
    ".ttf", ".webm", ".webp", ".woff", ".woff2", ".xz", ".zip",
}

NONCODE_EXTENSIONS = {
    "", ".adoc", ".conf", ".csv", ".dockerignore", ".editorconfig", ".gitattributes",
    ".gitignore", ".graphql", ".ini", ".json", ".md", ".properties", ".rst",
    ".toml", ".tsv", ".txt", ".yaml", ".yml",
}

GENERATED_PATH_MARKERS = (
    "/dist/", "/generated/", "/gen/", "/target/", "/vendor/",
    "/build/generated/", "/src/generated/",
)


def run_git_command_checked(repo_path: Path, arguments: list[str], input_bytes: bytes | None = None) -> bytes:
    result = subprocess.run(
        ["git", "-C", str(repo_path), *arguments],
        input=input_bytes,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        message = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"git failed for {repo_path.name}: {message}")
    return result.stdout


def query_git_blob_sizes(repo_path: Path, blob_hashes: list[str]) -> dict[str, int]:
    unique_hashes = sorted(set(blob_hashes))
    request = "".join(f"{blob_hash}\n" for blob_hash in unique_hashes).encode("ascii")
    response = run_git_command_checked(
        repo_path,
        ["cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)"],
        request,
    )
    sizes: dict[str, int] = {}
    for line in response.decode("utf-8", errors="strict").splitlines():
        object_hash, object_type, object_size = line.split(" ", 2)
        if object_type != "blob":
            raise RuntimeError(f"expected blob {object_hash}, found {object_type}")
        sizes[object_hash] = int(object_size)
    return sizes


def classify_default_file_class(path_text: str, mode: str) -> str:
    normalized = f"/{path_text.lower()}"
    extension = Path(path_text).suffix.lower()
    if mode == "160000":
        return "gitlink_classified"
    if any(marker in normalized for marker in GENERATED_PATH_MARKERS):
        return "generated_classified"
    if extension in BINARY_EXTENSIONS:
        return "binary_classified"
    if extension in CODE_EXTENSIONS:
        return "source_candidate"
    if extension in NONCODE_EXTENSIONS:
        return "noncode_classified"
    return "unknown_classified"


def collect_repository_file_rows(repo_path: Path, lane: str, assigned_agent: str) -> list[dict[str, object]]:
    raw_entries = run_git_command_checked(repo_path, ["ls-files", "-s", "-z"])
    parsed_entries: list[tuple[str, str, str]] = []
    for raw_entry in raw_entries.split(b"\0"):
        if not raw_entry:
            continue
        metadata, raw_path = raw_entry.split(b"\t", 1)
        mode, blob_hash, stage = metadata.decode("ascii").split(" ")
        if stage != "0":
            raise RuntimeError(f"unmerged index entry in {repo_path.name}: {raw_path!r}")
        path_text = raw_path.decode("utf-8", errors="surrogateescape")
        parsed_entries.append((mode, blob_hash, path_text))

    regular_blob_hashes = [blob_hash for mode, blob_hash, _ in parsed_entries if mode != "160000"]
    blob_sizes = query_git_blob_sizes(repo_path, regular_blob_hashes)
    rows: list[dict[str, object]] = []
    for mode, blob_hash, path_text in parsed_entries:
        extension = Path(path_text).suffix.lower().lstrip(".")
        rows.append(
            {
                "lane": lane,
                "repo": repo_path.name,
                "path": path_text,
                "git_blob": blob_hash,
                "bytes": 0 if mode == "160000" else blob_sizes[blob_hash],
                "extension": extension,
                "default_file_class": classify_default_file_class(path_text, mode),
                "assigned_agent": assigned_agent,
            }
        )
    return rows


def build_all_coverage_rows(family_root: Path) -> list[dict[str, object]]:
    actual_repositories = {entry.name for entry in family_root.iterdir() if entry.is_dir()}
    expected_repositories = set(REPOSITORY_LANES)
    missing_repositories = sorted(expected_repositories - actual_repositories)
    unexpected_repositories = sorted(actual_repositories - expected_repositories)
    if missing_repositories or unexpected_repositories:
        raise RuntimeError(
            f"repository-set mismatch; missing={missing_repositories}, unexpected={unexpected_repositories}"
        )

    rows: list[dict[str, object]] = []
    for repository_name, (lane, assigned_agent) in sorted(REPOSITORY_LANES.items()):
        rows.extend(
            collect_repository_file_rows(
                family_root / repository_name,
                lane,
                assigned_agent,
            )
        )
    return sorted(rows, key=lambda row: (str(row["lane"]), str(row["repo"]), str(row["path"])))


def write_denominator_manifest_file(output_path: Path, rows: list[dict[str, object]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "lane", "repo", "path", "git_blob", "bytes", "extension",
        "default_file_class", "assigned_agent",
    ]
    temporary_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
    with temporary_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary_path, output_path)


def run_manifest_generation_now() -> int:
    workspace_root = Path(__file__).resolve().parents[1]
    family_root = workspace_root / "gitrefrepo" / "Neo4j family"
    output_path = (
        workspace_root
        / "docs_PRD04"
        / "reference-learning"
        / "neo4j-compat-lowram"
        / "evidence"
        / "all-files-denominator.tsv"
    )
    rows = build_all_coverage_rows(family_root)
    write_denominator_manifest_file(output_path, rows)

    total_bytes = sum(int(row["bytes"]) for row in rows)
    lane_counts: dict[str, int] = {}
    for row in rows:
        lane = str(row["lane"])
        lane_counts[lane] = lane_counts.get(lane, 0) + 1
    print(f"wrote {len(rows)} tracked-file rows ({total_bytes} blob bytes) to {output_path}")
    for lane, count in sorted(lane_counts.items()):
        print(f"{lane}: {count} files")
    return 0


if __name__ == "__main__":
    sys.exit(run_manifest_generation_now())
