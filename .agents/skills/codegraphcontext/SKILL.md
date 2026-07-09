---
name: codegraphcontext
description: Use CodeGraphContext (cgc) to index code into a graph database and query call chains with Cypher or natural language via MCP; supports embedded KuzuDB (no server needed).
---
# CodeGraphContext (cgc)

Graph-DB code indexer, CLI + MCP (https://github.com/CodeGraphContext/CodeGraphContext).
Install (tree-sitter extras are required or indexing silently finds 0 functions):
```
uv tool install codegraphcontext --with tree-sitter --with tree-sitter-language-pack
```
Verify: `cgc doctor` (tree-sitter checks must pass).

## Setup (once per repo, embedded KuzuDB — no Neo4j server needed)
```
cd /home/ubuntu/repos/knight-bus-graph-walker
cgc --db kuzudb index src        # here: 365 functions, 66 structs, 25 enums, 51 modules
```
Note: indexing the repo root can fail on non-code files ("NoneType has no attribute 'language'") — index code dirs (`src`, `scripts`) instead.

## Usage
- `cgc --db kuzudb stats` — index stats
- `cgc --db kuzudb query "<cypher>"` — Cypher over the code graph
- `cgc report` — CGC_REPORT.md with god nodes, complexity, cross-module connections
- `cgc visualize` — interactive graph UI
- MCP: `cgc mcp` subcommands for client config
