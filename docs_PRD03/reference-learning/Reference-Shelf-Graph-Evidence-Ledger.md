# Reference Shelf Graph Evidence Ledger

Date: 2026-06-24

This control artifact records the shelf-wide graph-evidence truthcheck required by
[`/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/V003-Reference-Folder-Learning-Spec.md`](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/V003-Reference-Folder-Learning-Spec.md).
It replaces file-existence optimism with semantic readiness checks drawn from
actual follow-up queries.

## Verdict

- The canonical reference shelf remains
  `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo`.
- The spec still resolves to `71` concrete repo targets: `67` code-bearing
  repos and `4` docs/spec-first repos.
- The live `gitrefrepo/` shelf currently contains `106` top-level clones, so
  this truthcheck is complete for the current learning-spec scope, not for
  every clone already present on disk.
- `codebase-memory-mcp` is the stronger shelf-wide substrate in this pass. It is
  semantically queryable on `66` of `67` code-bearing repos after direct per-repo follow-up probes.
- CodeGraphContext is useful on a narrower tranche than the earlier readiness
  ledger suggested. Only `18` repos are `DualSemanticReady`;
  `16` repos produced zero indexed semantic counts and `33` repos timed
  out before a reusable semantic `stats.txt` artifact was written.
- The real follow-up bucket is now small and explicit: `clickhouse-src`.
- A fresh long-leash rerun for `clickhouse-src` on 2026-06-24 still left
  zero-byte `index_repository.json` and `index_repository.stderr` artifacts in
  `codebase-memory-mcp` after `150s`, while CodeGraphContext only reached a
  partial `index.txt` plus SQLite/WAL state and still never produced
  `stats.txt` or `functions_find.txt`, so `NeedsRerun` remains the truthful
  status.
- The earlier `DualToolReady` notion was too operational. Output-file presence is
  not the same thing as symbol-level evidence.

## What Changed

The earlier shelf ledger treated a repo as effectively ready when wrapper output
files existed. This truthcheck tightens the bar:

1. `codebase-memory-mcp` must answer a direct per-repo query or search with a usable file,
   function, or class row.
2. CodeGraphContext must expose non-zero semantic stats and return at least one
   function row.
3. Docs/spec repos are still tracked explicitly as `GraphToolLowYield`.

## Scope Boundary Versus Live Shelf

- The current learning spec names `71` concrete repo targets.
- The live `gitrefrepo/` shelf currently contains `106` top-level clones.
- A row-set equality check currently shows `71` spec repo names, `71`
  truthcheck TSV rows, `0` missing rows, and `0` extra rows.
- The remaining `35` clones are real local inputs, but they are not yet
  mandatory learning targets until the spec or requirement tracker expands to
  include them.
- Future agents should therefore read this ledger as "graph-tool execution is
  truthchecked for the current spec scope" rather than "every clone on disk has
  already been semantically audited."

## Folder And Sub-Repo Coverage Semantics

- Repo-root graph indexing is the intended mechanism for nested folder coverage.
  When the spec names subpaths such as `community/record-storage-engine`,
  `docs_PRD03/`, `src/`, or `tests/`, the wrapper still runs on the enclosing
  repo root and the follow-up proof comes from `rg` plus direct reads inside
  the named folders.
- This means the current truthcheck covers all spec-named reference repos and
  the current Knight Bus workspace as graph substrates, while still requiring
  file-level verification for every folder-specific claim.
- Docs/spec-first repos remain `GraphToolLowYield` even when specific folders
  inside them are named, because text-first reading is still the evidence path
  that matters there.

## Method

Skills used:

- `/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/SKILL.md`
- `/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/SKILL.md`

Wrapper commands used during the sweep:

```bash
/Users/amuldotexe/.codex/skills/codebase-memory-evidence-reader/scripts/scan_current_repo_only.sh <ABSOLUTE_REPO_PATH>
/Users/amuldotexe/.codex/skills/codegraphcontext-evidence-reader/scripts/scan_current_repo_only.sh <ABSOLUTE_REPO_PATH>
```

Truthcheck follow-up used after the wrapper stage:

- For every code-bearing repo, direct `codebase-memory-mcp` follow-up probes were
  run against the repo-specific cache with an explicit `project` name.
- For every available CodeGraphContext run, `stats.txt` was parsed and `find type Function`
  was used when the semantic counts were non-zero.
- The machine-readable result lives at
  [Reference-Shelf-Graph-Tool-Truthcheck.tsv](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/Reference-Shelf-Graph-Tool-Truthcheck.tsv).

## Status Summary

| status | count | meaning |
| --- | ---: | --- |
| `DualSemanticReady` | 18 | Both graph tools yielded usable semantic evidence after follow-up checks. |
| `CbmSemanticReadyCgcLowYield` | 48 | `codebase-memory` yielded usable evidence; CodeGraphContext was zero-indexed or timeout-heavy for that repo. |
| `NeedsRerun` | 1 | The cached evidence is still not trustworthy enough for clean reuse. |
| `GraphToolLowYield` | 4 | Docs/spec-first repos tracked explicitly as text-first. |

## Critical Repo Truthcheck

| repo | combined status | cbm example | cgc status | implication |
| --- | --- | --- | --- | --- |
| `neo4j-src` | `CbmSemanticReadyCgcLowYield` | indexes @ `community/kernel-test/src/test/java/org/neo4j/kernel/api/index/EntityValueUpdatesTest.java` | `ZeroIndexed` | usable via codebase-memory; CGC is low-yield here |
| `neo4j-gds-src` | `CbmSemanticReadyCgcLowYield` | unpack @ `core/src/main/java/org/neo4j/gds/core/compression/packed/AdjacencyUnpacking.java` | `ZeroIndexed` | usable via codebase-memory; CGC is low-yield here |
| `neo4j-testkit-src` | `DualSemanticReady` | test_dumps_full @ `boltstub/tests/simple_jolt/v3/test_parse.py` | `QueryReady` | usable in both tools |
| `neo4j-java-driver-src` | `CbmSemanticReadyCgcLowYield` | decodePrivate @ `driver/src/main/java/org/neo4j/driver/internal/pki/PemFormats.java` | `ZeroIndexed` | usable via codebase-memory; CGC is low-yield here |
| `neo4j-python-driver-src` | `CbmSemanticReadyCgcLowYield` | snake_case_to_pascal_case @ `testkitbackend/_async/requests.py` | `ZeroIndexed` | usable via codebase-memory; CGC is low-yield here |
| `apache-iggy-src` | `CbmSemanticReadyCgcLowYield` | FromCode @ `foreign/go/errors/errors_gen.go` | `ZeroIndexed` | usable via codebase-memory; CGC is low-yield here |
| `rocksdb-src` | `CbmSemanticReadyCgcLowYield` | T @ `third-party/gtest-1.8.1/fused-src/gtest/gtest.h` | `NoStats` | usable via codebase-memory; CGC is low-yield here |
| `ladybug-src` | `CbmSemanticReadyCgcLowYield` | make_unique @ `third_party/httplib/httplib.h` | `ZeroIndexed` | usable via codebase-memory; CGC is low-yield here |
| `gapbs-src` | `DualSemanticReady` | .PHONY @ `Makefile` | `QueryReady` | usable in both tools |
| `graphchi-cpp-src` | `CbmSemanticReadyCgcLowYield` | jQuerySub @ `conf/adminhtml/bootstrap/js/jquery.js` | `NoStats` | usable via codebase-memory; CGC is low-yield here |
| `gridgraph-src` | `DualSemanticReady` | all @ `Makefile` | `QueryReady` | usable in both tools |
| `ligra-src` | `DualSemanticReady` | all @ `apps/Makefile` | `QueryReady` | usable in both tools |
| `thunderrw-src` | `DualSemanticReady` | sfmt_genrand_uint32 @ `dependency/SFMT-src-1.5.1/SFMT.h` | `QueryReady` | usable in both tools |
| `graphblas-src` | `CbmSemanticReadyCgcLowYield` | GB_accum_mask @ `Source/mask/GB_accum_mask.c` | `NoStats` | usable via codebase-memory; CGC is low-yield here |
| `lagraph-src` | `CbmSemanticReadyCgcLowYield` | difference @ `experimental/test/test_edgeBetweennessCentrality.c` | `NoStats` | usable via codebase-memory; CGC is low-yield here |


## DualSemanticReady Repos

- `cypher-shell-src`
- `gapbs-src`
- `graphblas-pointers-src`
- `gridgraph-src`
- `ldbc_graphalytics_platforms_graphblas-src`
- `ligra-src`
- `minigraph-src`
- `neo4rs-src`
- `sparsetools-src`
- `sprs-src`
- `thunderrw-src`
- `timely-dataflow-src`
- `fjall-src`
- `graph-data-science-src`
- `neo4j-testkit-src`
- `petgraph-src`
- `redb-src`
- `tracing-src`

## CBM-Strong, CGC-Low-Yield Repos

- `apache-datafusion-src`
- `apache-arrow-rs-src`
- `differential-dataflow-src`
- `duckdb-src`
- `falkordb-src`
- `graphblas-src`
- `graphblas_sparse_linear_algebra-src`
- `graphchi-cpp-src`
- `igraph-src`
- `helix-db-src`
- `jemalloc-src`
- `kuzu-src`
- `lagraph-src`
- `ldbc_graphalytics-src`
- `ldbc_snb_interactive_v1_driver-src`
- `ldbc_snb_interactive_v1_impls-src`
- `ldbc_snb_interactive_v2_driver-src`
- `ldbc_snb_interactive_v2_impls-src`
- `materialize-src`
- `memgraph-src`
- `neo4j-browser-src`
- `networkit-src`
- `nornicdb-src`
- `python-graphblas-src`
- `qdrant-src`
- `redisgraph-src`
- `risingwave-src`
- `rocksdb-src`
- `rustworkx-src`
- `snap-src`
- `tantivy-src`
- `age-src`
- `apache-iggy-src`
- `cypher-dsl-src`
- `gds-agent-src`
- `ladybug-src`
- `neo4j-apoc-procedures-src`
- `neo4j-apoc-src`
- `neo4j-dotnet-driver-src`
- `neo4j-gds-client-src`
- `neo4j-gds-src`
- `neo4j-go-driver-src`
- `neo4j-java-driver-src`
- `neo4j-javascript-driver-src`
- `neo4j-ogm-src`
- `neo4j-python-driver-src`
- `neo4j-src`
- `tikv-src`

## Needs Rerun

- `clickhouse-src`
- Fresh long-leash reruns `clickhouse-src-20260624-150515` and
  `clickhouse-src-20260624-150516` still failed the semantic-ready bar:
  `codebase-memory-mcp` left zero-byte index files after `150s`, and
  CodeGraphContext still never produced `stats.txt` or `functions_find.txt`.

## GraphToolLowYield Repos

- `apache-parquet-format-src`
- `ldbc_graphalytics_docs-src`
- `neo4j-docs-bolt-src`
- `opencypher-src`

## Current Workspace Note

- CBM indexed 1860 nodes and 3942 edges in knight-bus-graph-walker-20260624-120354.
- CGC indexed 78 files, 345 functions, 3 classes, and 64 modules in knight-bus-graph-walker-20260624-120043.

## Requirement Impact

| requirement | effect of this artifact |
| --- | --- |
| `REQ-LEARN-040.0` | strengthened: isolated per-repo runs are now checked for semantic output, not just wrapper file presence. |
| `REQ-LEARN-036.0` | strengthened: clone coverage now has a truthchecked graph-tool status for every concrete repo target currently named by the spec. |
| `REQ-LEARN-053.0` | preserved: the live shelf remains `gitrefrepo/`, while `ref-repo-folder/` remains a legacy alias only. |

## Skeptical Note

This artifact still does not prove architecture correctness. It proves something
narrower but important: which local graph-evidence substrates are genuinely
usable across the named repo shelf today. Future agents should prefer
`codebase-memory-mcp` as the default structural navigator across the shelf,
then use CodeGraphContext selectively where this ledger marks real semantic
readiness.
