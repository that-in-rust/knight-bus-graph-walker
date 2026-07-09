---
name: codebase-memory-mcp
description: Use the codebase-memory-mcp CLI to index this repo into a code graph and query it (search, trace paths, architecture summaries, ADRs). Use when exploring code structure, dependencies, or persisting architectural insights across sessions.
---

# codebase-memory-mcp

A local code-graph indexer + query engine (https://github.com/DeusData/codebase-memory-mcp).
Installed at `~/.local/bin/codebase-memory-mcp` (ensure `~/.local/bin` is on PATH).
If missing, install with:

```bash
curl -fsSL https://raw.githubusercontent.com/DeusData/codebase-memory-mcp/main/install.sh | bash -s -- --ui
```

## Core workflow

1. **Index the repo** (required once per session / after big changes; argument is `repo_path`, not `path`):

```bash
codebase-memory-mcp cli index_repository '{"repo_path":"/home/ubuntu/repos/knight-bus-graph-walker"}'
```

   Large vendored dirs (`reference-repos-neo4j-family/`, `reference-repos-competitors/`, `.git`) are auto-excluded.

2. **Query the graph**:

```bash
codebase-memory-mcp cli list_projects '{}'
codebase-memory-mcp cli get_architecture '{"project":"home-ubuntu-repos-knight-bus-graph-walker","aspects":["all"]}'
codebase-memory-mcp cli search_graph '{"project":"home-ubuntu-repos-knight-bus-graph-walker","query":"snapshot builder"}'
codebase-memory-mcp cli trace_path '{"project":"home-ubuntu-repos-knight-bus-graph-walker","from":"...","to":"..."}'
codebase-memory-mcp cli get_code_snippet '{"project":"home-ubuntu-repos-knight-bus-graph-walker","node_id":"..."}'
```

Other tools: `query_graph`, `get_graph_schema`, `search_code`, `index_status`, `detect_changes`, `manage_adr` (persist Architecture Decision Records across sessions), `ingest_traces`, `delete_project`.

Run `codebase-memory-mcp cli <tool> --help` for exact flags; passing raw JSON works but is deprecated in favor of flags.

## Optional UI

Graph visualization HTTP UI (default port 9749): enable with `--ui=true`, set port with `--port=N`. Run `codebase-memory-mcp` with no args to serve MCP over stdio for MCP-capable agents.

## Notes

- Project name is derived from the path (slashes become dashes), e.g. `home-ubuntu-repos-knight-bus-graph-walker`.
- If an indexing worker crashes, the hint says to re-run; also check `~/.cache/codebase-memory-mcp/logs/`. A common cause is a wrong argument name (must be `repo_path`).
- After indexing, consider `get_architecture` + `manage_adr(mode='update')` to persist insights.
