---
name: better-code-review-graph
description: Use better-code-review-graph as an MCP server for review-context queries — minimal diff context, blast radius, and review questions per change set.
---
# better-code-review-graph

Review-layer MCP server (https://github.com/n24q02m/better-code-review-graph).
Install: `uv tool install better-code-review-graph` (or run ad hoc with `uvx better-code-review-graph`).

## Usage
Runs as an MCP stdio server from the repo root:
```
cd /home/ubuntu/repos/knight-bus-graph-walker
better-code-review-graph      # stdio MCP server
```
MCP client config:
```json
{"mcpServers": {"better-code-review-graph": {"command": "uvx", "args": ["better-code-review-graph"]}}}
```
Use its tools when preparing or reviewing a PR: minimal context for a diff, blast radius of changed symbols, suggested review questions.
