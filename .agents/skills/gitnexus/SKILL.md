---
name: gitnexus
description: Use GitNexus to explore the codebase via its knowledge graph — impact/blast-radius analysis, symbol context, execution-flow tracing, change detection. Use before editing symbols, when tracing call chains, or for architecture overviews.
---

# GitNexus

Code-intelligence knowledge graph, CLI + MCP (https://github.com/abhigyanpatwari/GitNexus).
Install: `sudo npm install -g gitnexus@latest` (verify: `gitnexus --version`).

## Setup (once per repo, re-run when stale)

```bash
cd /home/ubuntu/repos/knight-bus-graph-walker
gitnexus analyze          # indexes into .gitnexus/ (gitignored, ~100MB)
```

This repo indexes to ~8.5k nodes / 11.4k edges / 300 execution flows in ~10s.
Large vendored files (>512KB) are skipped automatically.

## Usage

MCP server: `gitnexus setup` writes MCP config for detected agents, or run the MCP
directly per the README. Key tools (17): `query` (process-grouped concept search),
`context` (360° symbol view: callers/callees/flows), `impact` (blast radius, e.g.
`impact({target:"symbolName", direction:"upstream"})`), `trace`, `detect_changes`,
`tool_map`, `api_impact`, `rename`, plus cross-repo group tools.

CLI equivalents: `gitnexus status`, `gitnexus wiki`, `gitnexus clean`,
`gitnexus serve` (bridges to the web UI graph explorer at gitnexus.vercel.app).

Recommended flow: `impact` before editing a symbol; `detect_changes` before committing
(`detect_changes({scope:"compare", base_ref:"main"})` for branch review).

## Notes

- `gitnexus analyze` also writes a GitNexus block into AGENTS.md/CLAUDE.md and skills into `.claude/skills/`.
- npm 11 `npx` crash workaround: install globally (see README issue #1939).
- FTS extension may be unavailable; graph features still work without it.
