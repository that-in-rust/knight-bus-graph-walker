#!/usr/bin/env python3
"""Build a SQLite navigation database from Neo4j-family Clarity outputs."""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
GRAPH_DIR = REPO_ROOT / "docs_PRD03" / "reference-learning" / "neo4j-family-dependency-graphs"
FAMILY_ROOT = REPO_ROOT / "gitrefrepo" / "Neo4j family"
DB_PATH = GRAPH_DIR / "neo4j_family_graph.sqlite"
GUIDE_PATH = GRAPH_DIR / "sqlite-navigation-guide.md"


REPO_ROLES = {
    "neo4j-src": ("oltp_kernel", "Neo4j OLTP source of truth: records, kernel, Cypher, procedures, transactions."),
    "neo4j-gds-src": ("olap_gds", "GDS source of truth: graph catalog, projections, algorithms, estimators, artifacts."),
    "neo4j-gds-client-src": ("gds_client", "Python GDS client workflows and user-facing GDS API expectations."),
    "opencypher-src": ("cypher_spec", "Cypher grammar, TCK, and procedure calling specification material."),
    "neo4j-docs-bolt-src": ("bolt_spec", "Bolt protocol docs and state-machine vocabulary."),
    "neo4j-testkit-src": ("driver_verification", "Official driver behavior and protocol verification pressure."),
    "neo4j-python-driver-src": ("official_driver", "Python driver compatibility surface."),
    "neo4j-java-driver-src": ("official_driver", "Java driver compatibility surface."),
    "neo4j-javascript-driver-src": ("official_driver", "JavaScript driver compatibility surface."),
    "neo4j-go-driver-src": ("official_driver", "Go driver compatibility surface."),
    "neo4j-dotnet-driver-src": ("official_driver", ".NET driver compatibility surface."),
    "neo4rs-src": ("rust_driver", "Rust Bolt client ergonomics and type boundary reference."),
    "cypher-dsl-src": ("cypher_client", "Generated Cypher query-shape compatibility pressure."),
    "cypher-shell-src": ("cypher_client", "CLI client behavior and Cypher execution workflow."),
    "neo4j-apoc-src": ("procedure_ecosystem", "APOC procedure ecosystem and out-of-core support boundary."),
    "neo4j-apoc-procedures-src": ("procedure_ecosystem", "Historical/contrib APOC procedure compatibility pressure."),
    "neo4j-browser-src": ("application_compatibility", "Human-facing Neo4j browser workflows."),
    "neo4j-ogm-src": ("application_compatibility", "Object graph mapper compatibility pressure."),
    "gds-agent-src": ("gds_orchestration", "GDS orchestration and agent-facing procedure workflow reference."),
    "graph-data-science-src": ("gds_examples", "Graph Data Science examples/reference shell."),
}


def read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def execute_many(conn: sqlite3.Connection, sql: str, rows: list[tuple]) -> None:
    if rows:
        conn.executemany(sql, rows)


def file_kind(path: str) -> str:
    lower = path.lower()
    if "/src/test/" in lower or "/test/" in lower or "/tests/" in lower or lower.endswith("test.java"):
        return "test"
    if lower.endswith((".md", ".adoc", ".markdown", ".txt", ".bnf")):
        return "docs_or_spec"
    if lower.endswith((".xml", ".gradle", ".toml", ".json", ".yaml", ".yml", ".properties", ".pom")):
        return "build_or_config"
    if "/resources/" in lower or lower.endswith((".feature", ".cypher")):
        return "fixture_or_resource"
    return "source"


def folder_bucket(path: str) -> str:
    parts = [part for part in path.split("/") if part]
    if len(parts) <= 1:
        return "."
    if len(parts) >= 2 and parts[1] == "src":
        return parts[0]
    return "/".join(parts[:2])


def add_tag(tags: list[tuple[str, str]], tag: str, reason: str) -> None:
    tags.append((tag, reason))


def classify_tags(repo: str, file_path: str) -> list[tuple[str, str]]:
    lower = f"{repo}/{file_path}".lower()
    tags: list[tuple[str, str]] = []

    role = REPO_ROLES.get(repo, ("unknown", ""))[0]
    add_tag(tags, f"repo:{role}", "repo role")

    if repo == "neo4j-gds-src" or "gds" in lower:
        add_tag(tags, "plane:olap_gds", "GDS/OLAP repo or path")
    if repo == "neo4j-src":
        add_tag(tags, "plane:oltp_kernel", "Neo4j kernel repo")
    if "bolt" in lower or "driver" in role or repo == "neo4j-testkit-src":
        add_tag(tags, "surface:bolt_driver", "Bolt or driver surface")
    if "cypher" in lower or repo == "opencypher-src":
        add_tag(tags, "surface:cypher", "Cypher surface")
    if "procedure" in lower or "/proc/" in lower or "apoc" in lower or "callable" in lower:
        add_tag(tags, "surface:procedure", "procedure/callable path")
    if "projection" in lower or "graphproject" in lower or "graph-project" in lower:
        add_tag(tags, "olap:projection", "projection path")
    if "graph-store" in lower or "graphstore" in lower or "catalog" in lower:
        add_tag(tags, "olap:catalog", "graph/model/pipeline catalog path")
    if "memory" in lower or "estimate" in lower or "estimation" in lower or "mem/" in lower:
        add_tag(tags, "olap:memory", "memory/estimation path")
    if "algorithm" in lower or "/algo" in lower or "pagerank" in lower or "shortest" in lower:
        add_tag(tags, "olap:algorithm", "algorithm path")
    if "write" in lower or "export" in lower or "import" in lower:
        add_tag(tags, "io:write_import_export", "write/import/export path")
    if "record-storage" in lower or "recordstorage" in lower or "/store/" in lower:
        add_tag(tags, "oltp:record_storage", "record storage path")
    if "transaction" in lower or "/tx" in lower or "bookmark" in lower:
        add_tag(tags, "oltp:transaction", "transaction/bookmark path")
    if "index" in lower or "schema" in lower:
        add_tag(tags, "oltp:schema_index", "schema/index path")
    if "test" in file_kind(file_path):
        add_tag(tags, "kind:test", "test file")
    if file_kind(file_path) == "docs_or_spec":
        add_tag(tags, "kind:docs_or_spec", "documentation/spec file")

    return tags


def line_count(repo: str, file_path: str) -> int | None:
    actual = FAMILY_ROOT / repo / file_path
    if not actual.exists() or not actual.is_file():
        return None
    try:
        text = actual.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return None
    return text.count("\n") + (1 if text and not text.endswith("\n") else 0)


def build_db() -> None:
    if DB_PATH.exists():
        DB_PATH.unlink()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode = DELETE")
    conn.execute("PRAGMA synchronous = NORMAL")

    conn.executescript(
        """
        CREATE TABLE repos (
            repo TEXT PRIMARY KEY,
            status TEXT,
            role TEXT,
            role_note TEXT,
            duration_seconds REAL,
            dot_file TEXT,
            stderr_file TEXT,
            dot_bytes INTEGER,
            node_count INTEGER,
            edge_count INTEGER,
            cycle_count INTEGER,
            stderr_excerpt TEXT
        );

        CREATE TABLE files (
            repo TEXT NOT NULL,
            file TEXT NOT NULL,
            absolute_path TEXT,
            folder TEXT,
            extension TEXT,
            kind TEXT,
            line_count INTEGER,
            PRIMARY KEY (repo, file)
        );

        CREATE TABLE edges (
            repo TEXT NOT NULL,
            source_file TEXT NOT NULL,
            target_file TEXT NOT NULL,
            edge_kind TEXT NOT NULL DEFAULT 'clarity_dependency'
        );

        CREATE TABLE file_tags (
            repo TEXT NOT NULL,
            file TEXT NOT NULL,
            tag TEXT NOT NULL,
            reason TEXT NOT NULL,
            PRIMARY KEY (repo, file, tag)
        );

        CREATE TABLE file_metrics (
            repo TEXT NOT NULL,
            file TEXT NOT NULL,
            fan_in INTEGER NOT NULL,
            fan_out INTEGER NOT NULL,
            total_degree INTEGER NOT NULL,
            PRIMARY KEY (repo, file)
        );

        CREATE TABLE folder_metrics (
            repo TEXT NOT NULL,
            folder TEXT NOT NULL,
            file_count INTEGER NOT NULL,
            source_file_count INTEGER NOT NULL,
            total_lines INTEGER NOT NULL,
            fan_in INTEGER NOT NULL,
            fan_out INTEGER NOT NULL,
            total_degree INTEGER NOT NULL,
            PRIMARY KEY (repo, folder)
        );

        CREATE TABLE attack_candidates (
            rank INTEGER PRIMARY KEY,
            repo TEXT NOT NULL,
            file TEXT NOT NULL,
            attack_lane TEXT NOT NULL,
            fan_in INTEGER NOT NULL,
            fan_out INTEGER NOT NULL,
            total_degree INTEGER NOT NULL,
            reason TEXT NOT NULL,
            suggested_read_question TEXT NOT NULL
        );

        CREATE TABLE query_recipes (
            name TEXT PRIMARY KEY,
            intent TEXT NOT NULL,
            sql TEXT NOT NULL
        );
        """
    )

    summary_rows = read_tsv(GRAPH_DIR / "repo-summary.tsv")
    repo_rows = []
    for row in summary_rows:
        role, role_note = REPO_ROLES.get(row["repo"], ("unknown", "Unclassified repo."))
        repo_rows.append(
            (
                row["repo"],
                row["status"],
                role,
                role_note,
                float(row["duration_seconds"] or 0),
                row["dot_file"],
                row["stderr_file"],
                int(row["dot_bytes"] or 0),
                int(row["node_count"] or 0),
                int(row["edge_count"] or 0),
                int(row["cycle_count"] or 0),
                row["stderr_excerpt"],
            )
        )
    execute_many(
        conn,
        "INSERT INTO repos VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        repo_rows,
    )

    node_rows = read_tsv(GRAPH_DIR / "family-file-nodes.tsv")
    if (GRAPH_DIR / "opencypher-fallback-file-inventory.tsv").exists():
        for row in read_tsv(GRAPH_DIR / "opencypher-fallback-file-inventory.tsv"):
            node_rows.append({"repo": row["repo"], "file": row["file"]})

    file_rows = []
    tag_rows = []
    seen_files: set[tuple[str, str]] = set()
    for row in node_rows:
        repo = row["repo"]
        file_path = row["file"]
        key = (repo, file_path)
        if key in seen_files:
            continue
        seen_files.add(key)
        actual = FAMILY_ROOT / repo / file_path
        ext = Path(file_path).suffix.lower()
        file_rows.append(
            (
                repo,
                file_path,
                str(actual),
                folder_bucket(file_path),
                ext,
                file_kind(file_path),
                line_count(repo, file_path),
            )
        )
        for tag, reason in classify_tags(repo, file_path):
            tag_rows.append((repo, file_path, tag, reason))
    execute_many(conn, "INSERT INTO files VALUES (?, ?, ?, ?, ?, ?, ?)", file_rows)
    execute_many(conn, "INSERT OR IGNORE INTO file_tags VALUES (?, ?, ?, ?)", tag_rows)

    edge_rows = []
    for row in read_tsv(GRAPH_DIR / "family-file-edges.tsv"):
        edge_rows.append((row["repo"], row["source_file"], row["target_file"], "clarity_dependency"))
    if (GRAPH_DIR / "opencypher-fallback-reference-edges.tsv").exists():
        for row in read_tsv(GRAPH_DIR / "opencypher-fallback-reference-edges.tsv"):
            edge_rows.append((row["repo"], row["source_file"], row["target_file"], row["edge_kind"]))
    execute_many(conn, "INSERT INTO edges VALUES (?, ?, ?, ?)", edge_rows)

    conn.executescript(
        """
        CREATE INDEX idx_edges_source ON edges(repo, source_file);
        CREATE INDEX idx_edges_target ON edges(repo, target_file);
        CREATE INDEX idx_file_tags_tag ON file_tags(tag);
        CREATE INDEX idx_files_kind ON files(kind);
        CREATE INDEX idx_files_folder ON files(repo, folder);

        INSERT INTO file_metrics(repo, file, fan_in, fan_out, total_degree)
        SELECT
            f.repo,
            f.file,
            COALESCE(incoming.fan_in, 0) AS fan_in,
            COALESCE(outgoing.fan_out, 0) AS fan_out,
            COALESCE(incoming.fan_in, 0) + COALESCE(outgoing.fan_out, 0) AS total_degree
        FROM files f
        LEFT JOIN (
            SELECT repo, target_file AS file, COUNT(*) AS fan_in
            FROM edges
            GROUP BY repo, target_file
        ) incoming ON incoming.repo = f.repo AND incoming.file = f.file
        LEFT JOIN (
            SELECT repo, source_file AS file, COUNT(*) AS fan_out
            FROM edges
            GROUP BY repo, source_file
        ) outgoing ON outgoing.repo = f.repo AND outgoing.file = f.file;

        INSERT INTO folder_metrics(repo, folder, file_count, source_file_count, total_lines, fan_in, fan_out, total_degree)
        SELECT
            f.repo,
            f.folder,
            COUNT(*) AS file_count,
            SUM(CASE WHEN f.kind = 'source' THEN 1 ELSE 0 END) AS source_file_count,
            COALESCE(SUM(f.line_count), 0) AS total_lines,
            COALESCE(SUM(m.fan_in), 0) AS fan_in,
            COALESCE(SUM(m.fan_out), 0) AS fan_out,
            COALESCE(SUM(m.total_degree), 0) AS total_degree
        FROM files f
        JOIN file_metrics m ON m.repo = f.repo AND m.file = f.file
        GROUP BY f.repo, f.folder;

        CREATE VIEW v_file_hubs AS
        SELECT r.role, f.repo, f.folder, f.file, f.kind, f.line_count, m.fan_in, m.fan_out, m.total_degree
        FROM file_metrics m
        JOIN files f ON f.repo = m.repo AND f.file = m.file
        JOIN repos r ON r.repo = f.repo
        ORDER BY m.total_degree DESC, m.fan_in DESC;

        CREATE VIEW v_folder_hubs AS
        SELECT r.role, fm.repo, fm.folder, fm.file_count, fm.source_file_count, fm.total_lines, fm.fan_in, fm.fan_out, fm.total_degree
        FROM folder_metrics fm
        JOIN repos r ON r.repo = fm.repo
        ORDER BY fm.total_degree DESC, fm.fan_in DESC;

        CREATE VIEW v_olap_files AS
        SELECT DISTINCT f.repo, f.folder, f.file, f.kind, f.line_count
        FROM files f
        JOIN file_tags t ON t.repo = f.repo AND t.file = f.file
        WHERE t.tag IN (
            'plane:olap_gds',
            'olap:projection',
            'olap:catalog',
            'olap:memory',
            'olap:algorithm',
            'io:write_import_export'
        );

        CREATE VIEW v_olap_hubs AS
        SELECT f.repo, f.folder, f.file, f.kind, f.line_count, m.fan_in, m.fan_out, m.total_degree
        FROM v_olap_files f
        JOIN file_metrics m ON m.repo = f.repo AND m.file = f.file
        ORDER BY m.total_degree DESC, m.fan_in DESC;

        CREATE VIEW v_oltp_boundary_hubs AS
        SELECT f.repo, f.folder, f.file, f.kind, f.line_count, m.fan_in, m.fan_out, m.total_degree
        FROM files f
        JOIN file_metrics m ON m.repo = f.repo AND m.file = f.file
        JOIN file_tags t ON t.repo = f.repo AND t.file = f.file
        WHERE t.tag IN ('plane:oltp_kernel', 'oltp:record_storage', 'oltp:transaction', 'oltp:schema_index')
        ORDER BY m.total_degree DESC, m.fan_in DESC;

        CREATE VIEW v_verification_surface_hubs AS
        SELECT f.repo, f.folder, f.file, f.kind, f.line_count, m.fan_in, m.fan_out, m.total_degree
        FROM files f
        JOIN file_metrics m ON m.repo = f.repo AND m.file = f.file
        JOIN file_tags t ON t.repo = f.repo AND t.file = f.file
        WHERE t.tag IN ('surface:bolt_driver', 'surface:cypher', 'surface:procedure')
        ORDER BY m.total_degree DESC, m.fan_in DESC;
        """
    )

    candidate_rows = []
    sql = """
        SELECT f.repo, f.file, GROUP_CONCAT(DISTINCT t.tag) AS tags, m.fan_in, m.fan_out, m.total_degree
        FROM files f
        JOIN file_metrics m ON m.repo = f.repo AND m.file = f.file
        JOIN file_tags t ON t.repo = f.repo AND t.file = f.file
        WHERE f.kind IN ('source', 'test')
          AND t.tag IN ('olap:projection','olap:catalog','olap:memory','olap:algorithm','surface:procedure','io:write_import_export')
        GROUP BY f.repo, f.file
        ORDER BY
          CASE WHEN f.repo = 'neo4j-gds-src' THEN 0 ELSE 1 END,
          m.total_degree DESC,
          m.fan_in DESC
        LIMIT 150
    """
    for rank, row in enumerate(conn.execute(sql).fetchall(), start=1):
        repo, file_path, tags, fan_in, fan_out, degree = row
        if "olap:memory" in tags:
            lane = "memory_estimator"
            question = "What exact memory terms, reject conditions, and estimator inputs must the Rust rewrite preserve?"
        elif "olap:projection" in tags:
            lane = "projection_build"
            question = "How does this file transform Neo4j-shaped input into graph projection semantics?"
        elif "olap:catalog" in tags:
            lane = "catalog_lifecycle"
            question = "What identity, lifecycle, and artifact state transitions must the Rust catalog preserve?"
        elif "surface:procedure" in tags:
            lane = "procedure_surface"
            question = "What procedure signature, mode, result, and unsupported behavior does this file define?"
        elif "io:write_import_export" in tags:
            lane = "write_import_export"
            question = "What write-back/import/export side effect and buffer cost must be isolated from OLAP reads?"
        else:
            lane = "olap_algorithm"
            question = "Which algorithm inputs, output artifacts, and high-water states dominate implementation risk?"
        reason = f"tags={tags}; fan_in={fan_in}; fan_out={fan_out}; degree={degree}"
        candidate_rows.append((rank, repo, file_path, lane, fan_in, fan_out, degree, reason, question))
    execute_many(conn, "INSERT INTO attack_candidates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", candidate_rows)

    recipes = [
        (
            "top_olap_attack_candidates",
            "Start here when choosing GDS/OLAP files to read deeply.",
            "SELECT * FROM attack_candidates ORDER BY rank LIMIT 50;",
        ),
        (
            "top_gds_hubs",
            "Largest structural hubs inside the GDS repo.",
            "SELECT * FROM v_file_hubs WHERE repo='neo4j-gds-src' LIMIT 50;",
        ),
        (
            "top_gds_folder_hubs",
            "Largest GDS folders by aggregate graph degree and line count.",
            "SELECT * FROM v_folder_hubs WHERE repo='neo4j-gds-src' LIMIT 50;",
        ),
        (
            "projection_candidates",
            "Projection Build Store source candidates.",
            "SELECT a.* FROM attack_candidates a WHERE attack_lane='projection_build' ORDER BY rank LIMIT 50;",
        ),
        (
            "memory_candidates",
            "Strict RAM estimator source candidates.",
            "SELECT a.* FROM attack_candidates a WHERE attack_lane='memory_estimator' ORDER BY rank LIMIT 50;",
        ),
        (
            "dependents_of_file",
            "Replace :repo and :file with a file to see direct dependents.",
            "SELECT source_file FROM edges WHERE repo=:repo AND target_file=:file ORDER BY source_file;",
        ),
        (
            "dependencies_of_file",
            "Replace :repo and :file with a file to see direct dependencies.",
            "SELECT target_file FROM edges WHERE repo=:repo AND source_file=:file ORDER BY target_file;",
        ),
        (
            "verification_hubs",
            "Bolt, Cypher, and procedure surface hubs that should drive tests.",
            "SELECT * FROM v_verification_surface_hubs LIMIT 100;",
        ),
        (
            "oltp_boundary_hubs",
            "OLTP files to read only as compatibility boundary, not OLAP implementation path.",
            "SELECT * FROM v_oltp_boundary_hubs LIMIT 100;",
        ),
    ]
    execute_many(conn, "INSERT INTO query_recipes VALUES (?, ?, ?)", recipes)

    conn.commit()
    conn.close()


def markdown_escape(value: object) -> str:
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ")


def markdown_table(headers: list[str], rows: list[tuple]) -> str:
    if not rows:
        return "_No rows._\n"
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(markdown_escape(value) for value in row) + " |")
    return "\n".join(lines) + "\n"


def fetch_rows(conn: sqlite3.Connection, sql: str, limit: int | None = None) -> list[tuple]:
    if limit is not None:
        sql = f"{sql} LIMIT {limit}"
    return [tuple(row) for row in conn.execute(sql).fetchall()]


def write_guide() -> None:
    conn = sqlite3.connect(DB_PATH)
    counts = {
        table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in (
            "repos",
            "files",
            "edges",
            "file_tags",
            "folder_metrics",
            "attack_candidates",
            "query_recipes",
        )
    }
    repo_rows = fetch_rows(
        conn,
        """
        SELECT repo, role, node_count, edge_count, cycle_count
        FROM repos
        ORDER BY node_count DESC
        """,
        12,
    )
    gds_folder_rows = fetch_rows(
        conn,
        """
        SELECT folder, file_count, source_file_count, total_lines, fan_in, fan_out, total_degree
        FROM v_folder_hubs
        WHERE repo = 'neo4j-gds-src'
        """,
        25,
    )
    candidate_rows = fetch_rows(
        conn,
        """
        SELECT rank, attack_lane, file, fan_in, fan_out, total_degree
        FROM attack_candidates
        WHERE repo = 'neo4j-gds-src'
        ORDER BY rank
        """,
        25,
    )
    verification_rows = fetch_rows(
        conn,
        """
        SELECT repo, folder, file, fan_in, fan_out, total_degree
        FROM v_verification_surface_hubs
        """,
        20,
    )
    conn.close()

    content = """# SQLite Navigation Guide For Neo4j Family Graphs

This database is the map room for using Clarity output in LLM coding.

Database:

```text
docs_PRD03/reference-learning/neo4j-family-dependency-graphs/neo4j_family_graph.sqlite
```

Builder:

```text
docs_PRD03/reference-learning/build_neo4j_family_graph_db.py
```

## Mental Model

The PRD says OLTP surface area remains Neo4j-shaped, while OLAP/GDS is where the low-RAM rewrite needs new architecture. So use this DB in two modes:

1. **Verification mode**: Bolt, Cypher, procedure ABI, and driver behavior define tests.
2. **Attack mode**: GDS projection, graph catalog, memory estimation, algorithms, and write/export paths define the first code-reading targets.

The raw Clarity graph is file-level. It is not a call graph. Use it to choose what to read next and to avoid losing yourself in the repo forest.

## Current Scale

| Table | Rows |
| --- | ---: |
"""
    for table, count in counts.items():
        content += f"| `{table}` | {count} |\n"

    content += """
## Largest Repositories

"""
    content += markdown_table(["repo", "role", "nodes", "edges", "cycles"], repo_rows)

    content += """
## Core Tables

| Table | Purpose |
| --- | --- |
| `repos` | One row per Neo4j-family repo, with role and graph size. |
| `files` | One row per file node, with folder bucket, kind, extension, path, and line count when available. |
| `edges` | File-to-file dependency edges from Clarity plus openCypher fallback reference edges. |
| `file_tags` | Heuristic tags such as `plane:olap_gds`, `olap:projection`, `olap:memory`, `surface:bolt_driver`. |
| `file_metrics` | Fan-in, fan-out, total degree. |
| `folder_metrics` | Aggregated file count, line count, and graph degree by folder bucket. |
| `attack_candidates` | Ranked OLAP/GDS files to read first. |
| `query_recipes` | Copy-paste SQL for common navigation tasks. |

## GDS Folder Map

These are not final architecture boundaries. They are reading zones: start with a folder, then pull the ranked files plus direct dependencies/dependents.

"""
    content += markdown_table(
        ["folder", "files", "source_files", "lines", "fan_in", "fan_out", "degree"],
        gds_folder_rows,
    )

    content += """
## First GDS Attack Candidates

Each candidate should become a small evidence dossier: contract, invariants, verification oracle, and the smallest Rust coding prompt it unlocks.

"""
    content += markdown_table(
        ["rank", "lane", "file", "fan_in", "fan_out", "degree"],
        candidate_rows,
    )

    content += """
## Verification Surface Hubs

Use these for compatibility tests and unsupported-behavior registration. They should not drive the low-RAM storage design directly.

"""
    content += markdown_table(
        ["repo", "folder", "file", "fan_in", "fan_out", "degree"],
        verification_rows,
    )

    content += """
## Map Levels

Use the DB at four levels:

1. **Repo level**: separate compatibility repos from implementation repos.
2. **Folder level**: choose a reading zone such as graph catalog, projection, memory estimation, or procedure facade.
3. **File level**: read high-degree files first, because they encode shared vocabulary and stable contracts.
4. **Neighborhood level**: read direct dependencies and dependents around a chosen file before writing any summary.

## First Queries

Top OLAP/GDS files to read:

```sql
SELECT rank, repo, file, attack_lane, fan_in, fan_out, total_degree, suggested_read_question
FROM attack_candidates
ORDER BY rank
LIMIT 50;
```

Top GDS folders:

```sql
SELECT folder, file_count, source_file_count, total_lines, fan_in, fan_out, total_degree
FROM v_folder_hubs
WHERE repo = 'neo4j-gds-src'
LIMIT 50;
```

Top GDS hubs:

```sql
SELECT folder, file, kind, line_count, fan_in, fan_out, total_degree
FROM v_file_hubs
WHERE repo = 'neo4j-gds-src'
LIMIT 50;
```

Projection Build Store candidates:

```sql
SELECT rank, file, fan_in, fan_out, suggested_read_question
FROM attack_candidates
WHERE attack_lane = 'projection_build'
ORDER BY rank
LIMIT 50;
```

Strict RAM candidates:

```sql
SELECT rank, file, fan_in, fan_out, suggested_read_question
FROM attack_candidates
WHERE attack_lane = 'memory_estimator'
ORDER BY rank
LIMIT 50;
```

What depends on a file:

```sql
SELECT source_file
FROM edges
WHERE repo = 'neo4j-gds-src'
  AND target_file = 'applications/services/src/main/java/org/neo4j/gds/applications/services/GraphDimensionFactory.java'
ORDER BY source_file;
```

What a file depends on:

```sql
SELECT target_file
FROM edges
WHERE repo = 'neo4j-gds-src'
  AND source_file = 'applications/services/src/main/java/org/neo4j/gds/applications/services/GraphDimensionFactory.java'
ORDER BY target_file;
```

## How To Use This With LLMs

For each attack candidate, ask the LLM to produce exactly four outputs:

```text
1. Contract: what behavior this file defines.
2. Invariants: what a Rust rewrite must preserve.
3. Verification oracle: what test proves it.
4. Coding prompt: the smallest implementation task this unlocks.
```

Do not ask the LLM to summarize whole repos. Ask it to summarize **one ranked candidate plus its direct dependencies and dependents**.

## Practical Next Step

Start with the first 25 rows of `attack_candidates`, but filter to:

```sql
WHERE repo = 'neo4j-gds-src'
  AND attack_lane IN ('projection_build', 'memory_estimator', 'catalog_lifecycle', 'procedure_surface')
```

That gives the shortest path from the current PRD to implementation-grade verification.
"""
    GUIDE_PATH.write_text(content, encoding="utf-8")


def main() -> None:
    build_db()
    write_guide()
    print(DB_PATH)
    print(GUIDE_PATH)


if __name__ == "__main__":
    main()
