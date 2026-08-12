## Clarity

This project uses `clarity` to visualize code changes, provide design feedback, and guide refactoring.

### When to Use Clarity

1. **After making changes** - Run `clarity` to visualize your changes, understand impact, and prepare context for developer review.
    - **Always run `clarity show` when you modify 3 or more files** to ensure the developer can review the full scope of changes
2. **Discussing design** - Use `clarity` to visualize architecture and dependencies for specific files, directories, or commits when discussing design decisions with the developer.
3. **Refactoring verification** - After implementing design changes, run `clarity` to verify the resulting structure aligns with the discussed design.

### How to Use Clarity

**For developer review (visualize):**
- Generate and render graphs for the developer to review
- For CLI agents, default to DOT output (`clarity show` or `clarity show -f dot`)
- For CLI agents, generate a URL with `clarity show -u`, then open that URL in the system browser with the platform command:
  - macOS: `open "<url>"`
  - Linux: `xdg-open "<url>"`
  - Windows (cmd): `start "" "<url>"`
  - Windows (PowerShell): `Start-Process "<url>"`
- Use `clarity show -f mermaid` if your environment supports Mermaid rendering (desktop apps, IDEs)
- Use `clarity show` or `clarity show -f dot` if your environment supports Graphviz rendering or has dot tools installed (supports SVG, PNG, etc.)
- Do not assume `clarity show -u` auto-opens a browser in CLI environments; always open the generated URL explicitly
- Choose the visualization method that works best for your coding environment

**For agent verification (feedback and analysis):**
- Run `clarity show` and read the dot/mermaid output directly
- Parse the graph structure to verify dependencies and relationships
- No visualization needed - the text output contains all structural information
- Use this during refactoring iterations to confirm progress
- Before refactoring a file, inspect its dependents with `clarity show <file> --reach up`

### Quick Reference

```bash
clarity show                                 # Visualize uncommitted changes (most common)
clarity show -c HEAD                         # Visualize changes in last commit
clarity show <files/dirs>                    # Build graph from specific files or directories (space-separated)
clarity show <file> --reach up               # Find what depends on a file before changing it
clarity show <file> --reach both --depth 2   # Bounded blast radius around a file
clarity show --all --collapse                # Whole-tree view collapsed into declared modules
clarity show --between <a,b>                  # Find all paths between two or more files (comma-separated)
clarity show -f mermaid                      # Output in mermaid format (default 'dot' Graphviz format)
clarity show -u                              # Generate visualization URL
```

For full reference, use `clarity show -h`

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **knight-bus-graph-walker** (16273 symbols, 20715 relationships, 300 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> Index stale? Run `node .gitnexus/run.cjs analyze` from the project root — it auto-selects an available runner. No `.gitnexus/run.cjs` yet? `npx gitnexus analyze` (npm 11 crash → `npm i -g gitnexus`; #1939).

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows. For regression review, compare against the default branch: `detect_changes({scope: "compare", base_ref: "main"})`.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `query({search_query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `context({name: "symbolName"})`.
- For security review, `explain({target: "fileOrSymbol"})` lists taint findings (source→sink flows; needs `analyze --pdg`).

## Never Do

- NEVER edit a function, class, or method without first running `impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `rename` which understands the call graph.
- NEVER commit changes without running `detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/knight-bus-graph-walker/context` | Codebase overview, check index freshness |
| `gitnexus://repo/knight-bus-graph-walker/clusters` | All functional areas |
| `gitnexus://repo/knight-bus-graph-walker/processes` | All execution flows |
| `gitnexus://repo/knight-bus-graph-walker/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
