---
name: serena
description: Use Serena for LSP-powered semantic code exploration — find symbols, references, definitions across the repo with language-server precision. Use when navigating unfamiliar code, tracing symbol usage, or making symbol-precise edits.
---

# Serena

Semantic code toolkit (https://github.com/oraios/serena), LSP-backed (rust-analyzer for this repo).
Installed via uv: `uv tool install -p 3.13 serena-agent` → `serena` on PATH (`~/.local/bin`).

## Setup (once per repo)

```bash
cd /home/ubuntu/repos/knight-bus-graph-walker
yes n | serena project index      # generates .serena/project.yml and indexes symbols
```

`.serena/` holds the index + per-project memories (gitignored).

## Usage

Serena is primarily an MCP server:

```bash
serena start-mcp-server --project /home/ubuntu/repos/knight-bus-graph-walker
```

Key tools it exposes: find_symbol, find_referencing_symbols, get_symbols_overview,
read/insert/replace at symbol level, plus project memories (`serena memories --help`).

Useful CLI subcommands: `serena project index`, `serena memories`, `serena config`,
`serena print-system-prompt`. Run `serena --help` for the full list.

## Notes

- Language servers are auto-managed; first symbol query in a session may be slow while rust-analyzer warms up.
- Re-run `serena project index` after large refactors.
- Prefer Serena for symbol-precise questions ("who references X?"); use codebase-memory-mcp/GitNexus for architecture-level graph queries.
