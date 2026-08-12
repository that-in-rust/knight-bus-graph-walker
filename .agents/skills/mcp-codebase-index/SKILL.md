---
name: mcp-codebase-index
description: Use mcp-codebase-index as a zero-dependency structural indexer MCP server — functions, classes, imports, dependency graphs, cross-file call chains with automatic git-aware incremental reindexing.
---
# mcp-codebase-index

Structural indexer + MCP server (https://github.com/MikeRecognex/mcp-codebase-index).
Install: `uv tool install 'mcp-codebase-index[mcp]'` (Python 3.11+; verify binary `mcp-codebase-index` on PATH).

## Usage
```
PROJECT_ROOT=/home/ubuntu/repos/knight-bus-graph-walker mcp-codebase-index   # stdio MCP server
```
- 18 query tools over functions, classes, imports, dependency graphs, call chains.
- Auto-incremental: checks `git diff`/`git status` before every query; no manual reindex.
- Persists cache to `.codebase-index-cache.pkl` (gitignored) for instant restarts.
- Rust support is regex-based (Python uses full `ast`) — prefer Serena for symbol-precise Rust work.
