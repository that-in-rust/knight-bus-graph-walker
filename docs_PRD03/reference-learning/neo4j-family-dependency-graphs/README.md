# Neo4j Family Clarity Dependency Graphs

Generated with `clarity show --all -f dot --no-stats` per nested repository in `gitrefrepo/Neo4j family`.

Artifacts:

- `repo-summary.tsv`: per-repo status and graph size.
- `family-file-nodes.tsv`: parsed file nodes from all successful/partial DOT outputs.
- `family-file-edges.tsv`: parsed file-to-file dependency edges from all successful/partial DOT outputs.
- `dot/*.dot`: raw Clarity DOT per repo.
- `stderr/*.stderr.txt`: command stderr per repo.
- `gds-read-progress-dashboard.md`: current rewrite-read progress (files completed/remaining, batch priorities, Shreyas-style next-step framing).
- `opencypher-fallback-file-inventory.tsv`: fallback inventory for `opencypher-src`, because Clarity 0.28.1 found no supported import-bearing source files in that spec/TCK repo.
- `opencypher-fallback-reference-edges.tsv`: lightweight intra-repo AsciiDoc/reference edges for `opencypher-src`.

| Repo | Status | Nodes | Edges | Cycles | DOT bytes |
|---|---:|---:|---:|---:|---:|
| cypher-dsl-src | ok | 669 | 3173 | 33 | 760953 |
| cypher-shell-src | ok | 127 | 512 | 1 | 98494 |
| gds-agent-src | ok | 43 | 48 | 1 | 11112 |
| graph-data-science-src | ok | 5 | 0 | 0 | 605 |
| neo4j-apoc-procedures-src | ok | 828 | 3205 | 20 | 487312 |
| neo4j-apoc-src | ok | 466 | 1564 | 5 | 244382 |
| neo4j-browser-src | ok | 695 | 1565 | 8 | 273205 |
| neo4j-docs-bolt-src | ok | 1 | 0 | 0 | 245 |
| neo4j-dotnet-driver-src | ok | 886 | 3341 | 18 | 625085 |
| neo4j-gds-client-src | ok | 818 | 3873 | 2 | 696394 |
| neo4j-gds-src | ok | 4921 | 30609 | 74 | 6621861 |
| neo4j-go-driver-src | ok | 213 | 1092 | 11 | 98514 |
| neo4j-java-driver-src | ok | 884 | 4378 | 24 | 885519 |
| neo4j-javascript-driver-src | ok | 603 | 1561 | 4 | 263575 |
| neo4j-ogm-src | ok | 1127 | 4261 | 64 | 1024372 |
| neo4j-python-driver-src | ok | 443 | 1621 | 5 | 194403 |
| neo4j-src | ok | 10738 | 85273 | 215 | 19642724 |
| neo4j-testkit-src | ok | 266 | 638 | 3 | 83085 |
| neo4rs-src | ok | 239 | 586 | 3 | 83532 |
| opencypher-src | exit_1 | 0 | 0 | 0 | 0 |

Interpretation notes:

- This is a file-level dependency graph, not a call graph.
- Clarity extracts structural imports/references for supported languages only.
- Documentation/spec repositories can have low edge counts even when strategically important.
- `opencypher-src` is strategically important but not represented by Clarity's import graph; use the fallback inventory/reference edges for that repo.
- Large Neo4j/GDS repos should be explored from this graph with focused reach queries around specific files.
