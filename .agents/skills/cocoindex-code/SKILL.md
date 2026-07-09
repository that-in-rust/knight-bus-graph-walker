---
name: cocoindex-code
description: Use cocoindex-code (ccc) to build a chunked, language-aware search index of the codebase for concept search across code and docs.
---
# cocoindex-code (ccc)

Codebase indexing + search CLI (https://github.com/cocoindex-io/cocoindex-code).
Install: `uv tool install --upgrade 'cocoindex-code[full]'` (binaries `ccc`, `cocoindex-code`).

## Setup (once per machine, then per repo)
```
ccc init                                     # writes ~/.cocoindex_code/global_settings.yml
cd /home/ubuntu/repos/knight-bus-graph-walker
ccc index                                    # per-repo settings + index in .cocoindex_code/ (gitignored)
```

## Usage
- `ccc index` — (re)build the index (chunks per language: rust, python, markdown, toml, ...)
- `ccc search "<query>"` — concept search over indexed chunks
- `ccc --help` — full command list
