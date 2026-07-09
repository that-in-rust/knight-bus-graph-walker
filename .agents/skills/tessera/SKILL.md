---
name: tessera
description: Use tessera for token-frugal code-graph navigation — find-references, impact (callers), call-path tracing (connect), hallucination validation. Rust CLI + MCP.
---
# tessera

Rust code-graph CLI + MCP (https://github.com/iamsaquib8/tessera).
Install: `cargo install tessera-codegraph` (needs Rust 1.85+; `rustup update stable` first). npm/brew/curl installs are unreliable (npm 404, no release assets). Binary: `~/.cargo/bin/tessera`.

## Setup (once per repo)
```
cd /home/ubuntu/repos/knight-bus-graph-walker
tessera index .            # index into .tessera/ (gitignored)
```

## Usage
- `tessera impact <Symbol>` — PageRank-ranked callers / blast radius
- `tessera find-references <Symbol>` — direct references
- `tessera connect <A> <B>` — call-path tracing between symbols
- `tessera validate <Symbol>` — hallucination check / near-miss suggestions
- `tessera mcp` — MCP server mode
Output includes a `_meta` token count — pick the cheapest verb for the job.
