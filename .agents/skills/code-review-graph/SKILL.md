---
name: code-review-graph
description: Use code-review-graph (crg) for a local-first persistent code graph with review-oriented queries — minimal PR context, blast radius, token-budgeted context packs.
---
# code-review-graph

Local-first code graph, CLI + MCP (https://github.com/tirth8205/code-review-graph).
Install: `uv tool install code-review-graph` (executables: `code-review-graph`, `crg-daemon`; verify: `code-review-graph --version`).

## Setup (once per repo)
```
cd /home/ubuntu/repos/knight-bus-graph-walker
code-review-graph build      # full graph build (~40 files, 808 nodes, 6744 edges here)
```

## Usage
- `code-review-graph build` — (re)build the graph
- `code-review-graph --help` — list query commands (context, impact, review)
- MCP server available via `code-review-graph mcp` for agent integration
