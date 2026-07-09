---
name: code-graph-mcp
description: Use sdsrs code-graph for hybrid FTS5 structural search and /understand, /trace, /impact style code-graph queries over a SQLite-backed knowledge graph.
---
# code-graph-mcp (sdsrs)

SQLite-backed code knowledge graph, CLI + MCP (https://github.com/sdsrss/code-graph-mcp).
Install: `sudo npm install -g @sdsrs/code-graph` (binaries `code-graph`, `code-graph-mcp`).

NOTE: the npm prebuilt Linux binary requires GLIBC 2.39; on Ubuntu 22.04 (GLIBC 2.35) build from source and swap the binary:
```
git clone --depth 1 https://github.com/sdsrss/code-graph-mcp.git && cd code-graph-mcp
cargo build --release   # needs Rust 1.85+ (rustup update stable)
sudo cp target/release/code-graph-mcp /usr/lib/node_modules/@sdsrs/code-graph/node_modules/@sdsrs/code-graph-linux-x64/code-graph-mcp
```

## Setup (once per repo)
```
cd /home/ubuntu/repos/knight-bus-graph-walker
code-graph reindex        # index into .code-graph/ (gitignored); 283 files, 7644 nodes here
```

## Usage
- `code-graph search <query>` — FTS5 concept search
- `code-graph stats` — index stats
- `code-graph-mcp` — MCP stdio server (tools: semantic_code_search, impact, trace, project_map)
