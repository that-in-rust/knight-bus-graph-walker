---
name: clarity
description: Use the clarity CLI to visualize code-change dependency graphs, blast radius, module boundaries, and cycles. Use after modifying 3+ files, before refactoring, or when discussing architecture/design impact.
---

# clarity

Dependency-impact graph CLI (https://github.com/LegacyCodeHQ/clarity-cli).
If missing, install: `sudo npm install -g @legacycodehq/clarity` (verify with `clarity --version`).

## Core commands (run from repo root)

```bash
clarity show                                 # Visualize uncommitted changes (most common)
clarity show -c HEAD                         # Changes in the last commit
clarity show <files/dirs>                    # Graph for specific files or directories
clarity show <file> --reach up               # What depends on a file (do this BEFORE refactoring it)
clarity show <file> --reach both --depth 2   # Bounded blast radius around a file
clarity show --between a,b                   # Dependency paths between files (comma-separated)
clarity show --all --collapse                # Whole-tree view collapsed into modules
clarity show -f mermaid                      # Mermaid output (renders in Markdown/GitHub)
clarity show -f dot                          # Graphviz DOT output (default)
clarity show -u                              # Generate a shareable visualization URL
clarity watch                                # Live graph while coding (human use)
```

## Agent workflow

1. **Before refactoring a file**: `clarity show <file> --reach up` to see dependents.
2. **After changing 3+ files**: run `clarity show` and read the dot/mermaid text output to verify the change shape; for the developer, generate a URL with `clarity show -u` and open it with `xdg-open "<url>"` (it does not auto-open).
3. **Design discussions**: use `clarity show <dir> -f mermaid` and paste the diagram.

Notes:
- If the working directory is clean, `clarity show` prints a hint instead of a graph — use `-c HEAD` or name files explicitly.
- Full reference: `clarity show -h`.
