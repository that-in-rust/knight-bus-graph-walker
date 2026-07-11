# SPEC: Graph Learning Corpus — Clone, Map, Extract, Publish

| Field | Value |
| --- | --- |
| Version | 3 — restructured per `executable-specs-01` skill; repo selection removed (owned by `corpus-ledger.tsv`) |
| Status | Active — awaiting owner "go" for execution |
| Owner | That in Rust |
| Executor | Devin sessions attached to this repo |
| Home | `docs_PRD06/graph-learning/` |
| Corpus | The 172 verified repos in `corpus-ledger.tsv` (selection is DONE and out of scope here) |

## 0. Request Parse (five inputs)

| Input | Value |
| --- | --- |
| Feature outcome | A pattern library teaching graph/vector/search/storage systems in depth from real source code: paired ASCII+Mermaid docs, every claim file-path-cited into local shallow clones |
| Actors and boundaries | Executor: Devin sessions. Oracle for selection: `corpus-ledger.tsv` (frozen; changes only via owner). Inputs: 172 shallow clones. Outputs: MD pairs + `pattern-index.md` in this folder only |
| Failure modes | clone failures (rename/404/size), mapping-tool failures on giant JVM/C++ trees, uncited claims (memory instead of source), doc pairs that disagree, index drift, disk exhaustion |
| Performance and reliability limits | ≤ 50 GB new clone volume; 10–15 repos mapped per session; doc 150–400 lines; ≥ 2 cited repos per pattern; incremental commits |
| Language/runtime constraints | Reading-only research — no building or benchmarking cloned systems; plain `git clone --depth 1` for everything (owner directive), no sparse/partial clones |

Prior assets (inputs, not work): `corpus-ledger.tsv` (172 rows, API-verified),
`corpus-research-findings.md`, `domain-keywords-glossary.md`,
`research-papers-ledger.md`, `proprietary-tools-landscape.md`, 40 legacy
clones in `reference-repos-neo4j-family/` + `reference-repos-competitors/`
(~3.4 GB, reused in place), 11 exploration tools in `.agents/skills/`, and
the 202606 pattern digests (new docs extend, never repeat them).

## 1. Executable Requirements

### Acquisition

#### REQ-GLC-010.1: Shallow-clone every corpus repo

**WHEN** a ledger repo has no `local_clone` path
**THEN** the executor SHALL run plain `git clone --depth 1` into `reference-repos-corpus/<name>-src/`
**AND** SHALL record clone date and `du -sh` size in the ledger row
**SHALL** reuse the 40 legacy clones in place and never re-clone them

#### REQ-GLC-011.1: No partial or filtered clones

**WHEN** a repo is large (Spark, Flink, Elasticsearch, Milvus, Velox)
**THEN** the executor SHALL still use a plain full-tree `--depth 1` clone
**AND** SHALL record any clone > 2 GB in the ledger and inform the owner
**SHALL** never use sparse-checkout or blob filters

#### REQ-GLC-012.1: Clone failures are recorded, never silent

**WHEN** a clone fails (404, rename, network)
**THEN** the executor SHALL record the failure and cause in the ledger `flags` column
**AND** SHALL attempt the renamed/canonical URL once before flagging
**SHALL** never drop the row from the ledger

#### REQ-GLC-013.1: Disk budget enforcement

**WHEN** a clone batch is about to start
**THEN** the executor SHALL check free disk and cumulative new-clone volume
**AND** SHALL stop and inform the owner before exceeding 50 GB of new clones

### Mapping

#### REQ-GLC-020.1: Structural map per repo

**WHEN** a repo is cloned
**THEN** the executor SHALL build at least one structural index (mcp-codebase-index or cocoindex-code for breadth; Serena/GitNexus/tessera for precision)
**AND** SHALL capture three artifacts per ledger row: top-level module map, core engine entry points, storage-layer directory
**SHALL** record a mapping failure + manual-skim substitute when tools cannot parse the repo

### Extraction

#### REQ-GLC-030.1: Pattern kind and evidence floor

**WHEN** a pattern is extracted
**THEN** it SHALL be classified as exactly one of `algorithm`, `storage`, or `execution`
**AND** SHALL cite at least 2 corpus repos with file paths into local clones
**SHALL** be rejected (not published) if only one witness repo exists

#### REQ-GLC-031.1: No claims from memory

**WHEN** any factual claim about a repo is written
**THEN** it SHALL be traceable to an inspected file path in a local clone
**AND** paper citations SHALL use verified IDs from `research-papers-ledger.md`
**SHALL** cite proprietary systems descriptively only (no source claims)

#### REQ-GLC-032.1: Extend, don't repeat, the 202606 digests

**WHEN** a pattern overlaps `graph-database-rewrite-references-202606/`
**THEN** the new doc SHALL link the prior digest
**AND** SHALL add only cross-corpus deltas

### Publication

#### REQ-GLC-040.1: Four-word ASCII+Mermaid pairs

**WHEN** a pattern is published
**THEN** it SHALL produce exactly two MD files named `<noun>-<noun>-<noun>-<form>.md` with form ∈ {ascii, mermaid}
**AND** each file SHALL be 150–400 lines
**SHALL** place both files in `docs_PRD06/graph-learning/`

#### REQ-GLC-041.1: ASCII doc content contract

**WHEN** an ASCII doc is written
**THEN** it SHALL contain: the pattern's job, raw data shape, memory/disk layout in ASCII, step-by-step walkthrough, two worked numeric examples
**AND** SHALL contain a citing-repos table (repo, path, one-line role)

#### REQ-GLC-042.1: Mermaid doc parity

**WHEN** a Mermaid doc is written
**THEN** it SHALL render the same pattern as Mermaid diagrams with the same citing-repos table
**AND** the pair SHALL agree such that either file alone teaches the pattern

#### REQ-GLC-043.1: Index updated atomically

**WHEN** any doc pair is added
**THEN** `pattern-index.md` SHALL be updated in the same commit (name, kind, repos cited, pair, date)
**SHALL** never leave a published pair unindexed

### Cadence

#### REQ-GLC-050.1: Incremental commits

**WHEN** a session works this spec
**THEN** it SHALL commit completed doc pairs incrementally (never one end-of-session dump)
**AND** SHALL push to the working branch after each pattern pair or clone batch

#### REQ-GLC-051.1: Category synthesis

**WHEN** all repos of a category are mapped and its patterns written
**THEN** a synthesis pair `<category>-pattern-synthesis-{ascii,mermaid}.md` SHALL summarize the dominant patterns of that category

## 2. Test Matrix (traceability)

| req_id | test_id | test_type | assertion | target |
| --- | --- | --- | --- | --- |
| REQ-GLC-010.1 | CHK-CLONE-001 | script | every ledger row has non-empty local_clone + size after its batch | completeness |
| REQ-GLC-011.1 | CHK-CLONE-002 | script | `git rev-parse --is-shallow-repository` = true for every clone; no `.git/info/sparse-checkout` | clone discipline |
| REQ-GLC-012.1 | CHK-CLONE-003 | inspect | failed rows carry a `clone-failed:<cause>` flag, none deleted | integrity |
| REQ-GLC-013.1 | CHK-DISK-001 | script | `du -s reference-repos-corpus` ≤ 50 GB before each batch | budget |
| REQ-GLC-020.1 | CHK-MAP-001 | inspect | ledger row has module map + entry points + storage dir, or failure+skim note | mapping |
| REQ-GLC-030.1 | CHK-PAT-001 | script | every published pattern cites ≥ 2 distinct repos | evidence floor |
| REQ-GLC-031.1 | CHK-PAT-002 | spot-check | every cited path exists in the local clone (`test -e` sample per doc) | citation integrity |
| REQ-GLC-032.1 | CHK-PAT-003 | inspect | overlapping patterns link the 202606 digest | non-duplication |
| REQ-GLC-040.1 | CHK-PUB-001 | script | docs come in pairs; names match `^\w+-\w+-\w+-(ascii\|mermaid)\.md$`; 150–400 lines | naming/shape |
| REQ-GLC-041.1 | CHK-PUB-002 | inspect | ASCII doc has all six required sections | content |
| REQ-GLC-042.1 | CHK-PUB-003 | inspect | Mermaid doc diagrams + table match ASCII twin | parity |
| REQ-GLC-043.1 | CHK-PUB-004 | script | commit adding a pair also touches pattern-index.md | atomicity |
| REQ-GLC-050.1 | CHK-GIT-001 | inspect | git log shows per-pair/per-batch commits | cadence |
| REQ-GLC-051.1 | CHK-SYN-001 | inspect | one synthesis pair per completed category | synthesis |

## 3. Execution Plan (STUB → VERIFY, adapted for research)

1. **STUB** — Create `pattern-index.md` skeleton and `reference-repos-corpus/`
   (gitignored). Add the CHK scripts above as a single `verify-corpus-spec.sh`
   in this folder so every gate is runnable, not narrative.
2. **RED** — Run `verify-corpus-spec.sh`: expect failures (no clones, no
   docs). Recorded baseline proves the checks detect absence.
3. **GREEN** — Work category batches in this order, satisfying the
   requirements batch by batch:
   storage-engine → graph-analytics → vector-ann → full-text-search →
   graph-db → neo4j-ecosystem/dataflow-compute/bench-testing.
   Per batch: clone (REQ-010–013) → map (REQ-020) → extract (REQ-030–032)
   → publish pairs (REQ-040–043) → commit/push (REQ-050).
4. **REFACTOR** — After each category: write the synthesis pair (REQ-051),
   prune weak patterns (single-witness ones), tighten the index.
5. **VERIFY** — Run `verify-corpus-spec.sh` at end of every session; all
   CHK gates green before the session reports done.

Pattern backlog seeding the GREEN phase (kind, likely witnesses — final
citations at write time): CSR adjacency layout; HNSW greedy descent;
graph-on-disk memory budget; LSM compaction tradeoff; push-pull frontier
switching; posting-list skip compression; immutable segment merging;
sparse-matrix graph algebra; roaring bitmap ID sets; WAL group commit;
MVCC snapshot visibility; copy-on-write tree snapshots; product
quantization; vertex-centric supersteps; differential incremental
computation; two-phase query planning; Louvain–Leiden refinement;
delta-varint edge encoding; bloom-filter read shortcut; differential
oracle testing. Count floats; expected 30–50 pairs.

## 4. Quality Gates (pre-commit, every session)

- [ ] Every requirement above has a stable `REQ-GLC-*.*` ID.
- [ ] Every `REQ-GLC-*` has at least one CHK gate in §2, and
      `verify-corpus-spec.sh` passes.
- [ ] Every published doc pair: four-word names, both forms, index updated
      in the same commit.
- [ ] Every factual repo claim has a file path that exists on disk.
- [ ] No clone is non-shallow, sparse, or filtered.
- [ ] Disk under budget; clone sizes recorded.
- [ ] No TODO/STUB/FIXME in published docs.
- [ ] Commits are incremental and pushed.

## 5. Open Questions

None blocking. The corpus ledger is frozen at 172; changes to it are owner
decisions outside this spec. Execution starts on owner "go".
