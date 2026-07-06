# Executable Spec: GDS Complete-Read Corpus (Goal-Ready)

## 1) Executable Requirements

This spec is the canonical goal contract for reading the first batch of files from:

- `docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv`
- `docs_PRD03/reference-learning/neo4j-family-dependency-graphs/neo4j_family_graph.sqlite`
- Source under `gitrefrepo/Neo4j family/neo4j-gds-src`
- Output in `docs_PRD03/reference-learning/gds-v2-dossiers/`

### REQ-GDS-SCOPE-001.0: Queue is truth
**WHEN** the goal starts  
**THEN** the agent SHALL read `gds-complete-read-queue.tsv` as UTF-8 TSV  
**AND** SHALL validate headers: `priority,tier,lane,repo,folder,file,kind,line_count,fan_in,fan_out,total_degree,purpose,read_prompt,dossier_path`  
**AND** SHALL fail globally only if the queue or critical columns are missing  
**SHALL** record any global read failure in `journals/gds-complete-read-batch1.md` and stop.

### REQ-GDS-BATCH-001.0: Exact bounded selection
**WHEN** queue rows are valid  
**THEN** the agent SHALL choose at most 10 rows by ascending numeric `priority`  
**AND** SHALL include only rows where `dossier_path` is missing or missing required headings  
**AND** SHALL persist the selected manifest before reading files  
**SHALL** process fewer than 10 rows only when fewer remain unprocessed.

### REQ-GDS-READ-001.0: Full-file evidence
**WHEN** a row is selected  
**THEN** the agent SHALL read the complete file at `gitrefrepo/Neo4j family/<repo>/<file>`  
**AND** SHALL apply that row's `read_prompt`  
**AND** SHALL not use only snippets or symbol grep-only reads for dossier claims  
**SHALL** mark row as blocked and continue to next row if the file is missing/unreadable.

### REQ-GDS-GRAPH-001.0: Dependency neighborhood capture
**WHEN** a row is processed  
**THEN** the agent SHALL query direct dependencies and dependents from SQLite:
```sql
SELECT target_file FROM edges WHERE repo = :repo AND source_file = :file ORDER BY target_file;
SELECT source_file FROM edges WHERE repo = :repo AND target_file = :file ORDER BY source_file;
```
**AND** SHALL include both lists in the dossier  
**SHALL** explicitly record when either query returns zero rows.

### REQ-GDS-DOSS-001.0: One dossier per row with fixed schema
**WHEN** processing a row  
**THEN** the agent SHALL create exactly one dossier at `dossier_path`  
**AND** SHALL include all headings:
- `## Source`
- `## Why This File Matters`
- `## Public Contract`
- `## Internal Mechanics`
- `## Memory And Storage Implications`
- `## Snapshot And Catalog Implications`
- `## Verification Oracles`
- `## Rust Rewrite Notes`
- `## Dependencies Read Next`
- `## Dependents As Tests`
- `## Open Questions`
- `## Coding Prompt Unlocked`
**AND** SHALL not merge multiple rows into one dossier.

### REQ-GDS-EVID-001.0: Evidence-first claims
**WHEN** writing behavior or architecture claims  
**THEN** claims SHALL distinguish `Evidence` (symbol names, line ranges, configs, errors, signatures) from `Inference`  
**AND** SHALL avoid uncited architecture/compatibility claims  
**AND** SHALL keep compatibility statements scoped to PRD-boundary requirements only.

### REQ-GDS-ORACLE-001.0: Verifiable Rust-orientated contracts
**WHEN** writing `## Verification Oracles`  
**THEN** each dossier SHALL include at least one executable `WHEN / THEN / SHALL` contract  
**AND** each contract SHALL name a target test type/fixture or function-style behavior  
**AND** SHALL include expected result, expected error, or expected state transition.

### REQ-GDS-RUST-001.0: Rewrite mapping
**WHEN** writing `## Rust Rewrite Notes`  
**THEN** the agent SHALL propose concrete module/type/trait/error candidates  
**AND** SHALL classify each candidate as L1 / L2 / L3 where inferable  
**AND** SHALL preserve external/public API naming intent where compatibility matters.

### REQ-GDS-ROLLUP-001.0: Rollup consolidation
**WHEN** the bounded batch finishes  
**THEN** the agent SHALL update `docs_PRD03/reference-learning/gds-v2-dossiers/ROLLUP.md` with only cross-file findings  
**AND** SHALL include: strengthened decisions, module candidates, verification specs, memory terms, catalog/projection invariants, unsupported behavior candidates, blockers, and next 10 recommended files  
**AND** SHALL not duplicate dossier prose.

### REQ-GDS-JOURNAL-001.0: Row-level intent tracking
**WHEN** starting each selected batch  
**THEN** the agent SHALL append a timestamped entry in `journals/gds-complete-read-batch1.md` with selected priorities, blockers, and completion status.

### REQ-GDS-VERIFY-001.0: Mechanical completion checks
**WHEN** reporting completion  
**THEN** the agent SHALL verify:
- processed priorities == selected manifest count (except documented row-local blockers),
- every created dossier path is unique,
- every non-blocked dossier has all required headings,
- all blocked rows are logged with exact path/reason,
- `git diff --check` passes.

## 2) Test Matrix

| req_id | test_id | type | assertion | target |
| --- | --- | --- | --- | --- |
| REQ-GDS-SCOPE-001.0 | TEST-QUEUE-001 | preflight | queue exists and headers are valid | `gds-complete-read-queue.tsv` |
| REQ-GDS-SCOPE-001.0 | TEST-QUEUE-002 | preflight | each selected `priority` parses as integer | queue parser |
| REQ-GDS-BATCH-001.0 | TEST-BATCH-001 | selection | first N uncompleted rows selected by ascending priority | manifest builder |
| REQ-GDS-BATCH-001.0 | TEST-BATCH-002 | selection | batch size <= 10 unless fewer rows remain | manifest builder |
| REQ-GDS-READ-001.0 | TEST-READ-001 | integration | selected file path resolves under `gitrefrepo/Neo4j family` | filesystem |
| REQ-GDS-GRAPH-001.0 | TEST-SQL-001 | integration | dependency query executes and returns ordered rows | `neo4j_family_graph.sqlite` |
| REQ-GDS-GRAPH-001.0 | TEST-SQL-002 | integration | dependent query executes and returns ordered rows | `neo4j_family_graph.sqlite` |
| REQ-GDS-DOSS-001.0 | TEST-DOSS-001 | filesystem | one dossier exists per selected row path | `docs_PRD03/reference-learning/gds-v2-dossiers` |
| REQ-GDS-DOSS-001.0 | TEST-DOSS-002 | schema | all 11 headings exist in each dossier | dossier parser |
| REQ-GDS-EVID-001.0 | TEST-EVID-001 | review | non-trivial claims include evidence source or marked inference | dossier content |
| REQ-GDS-ORACLE-001.0 | TEST-ORACLE-001 | schema | each dossier includes WHEN/THEN/SHALL oracle line | dossier content |
| REQ-GDS-RUST-001.0 | TEST-RUST-001 | review | Rust candidates and L1/L2/L3 labels included | dossier content |
| REQ-GDS-ROLLUP-001.0 | TEST-ROLLUP-001 | schema | rollup sections present and updated | `ROLLUP.md` |
| REQ-GDS-JOURNAL-001.0 | TEST-JOURNAL-001 | process | selected priorities and blockers logged with blockers reasons | `journals/gds-complete-read-batch1.md` |
| REQ-GDS-VERIFY-001.0 | TEST-COMPLETE-001 | verification | uniqueness and heading checks are consistent | validation script |
| REQ-GDS-VERIFY-001.0 | TEST-DIFF-001 | quality | `git diff --check` exits 0 | git |

## 3) TDD Plan

### STUB
- Read queue and confirm source files/paths from `repo`, `folder`, `file`, `dossier_path`.
- Create a manifest list with `(priority, lane, repo, file, dossier_path)` and computed status.
- Prepare expected heading checklist from `gds-complete-read-plan-v1.md` plus rollup schema.

### RED
- Validate that manifest builder and preflight checks fail when queue/DB is malformed.
- Verify required headings list is strict; intentionally run on current state to identify pre-existing gaps.
- Record expected blockers and confirm they are row-local or global.

### GREEN
- For each manifest row:
  1. Read full file.
  2. Run dependency and dependent SQLite queries.
  3. Populate dossier schema with evidence/inference labels and explicit verification oracle(s).
  4. Persist row-local blocker notes if applicable and continue remaining rows.

### REFACTOR
- Normalize language and section depth across dossiers.
- Consolidate cross-file, non-local findings into `ROLLUP.md`.
- Ensure all claims map cleanly to source evidence and PRD constraints.

### VERIFY
- Run mechanical dossier validation and compare manifest completion count.
- Update `ROLLUP.md` and next 10 queue recommendations.
- Run `git diff --check` and report blockers or pass status.

## 4) Quality Gates

- [ ] Queue file exists and contains required columns.
- [ ] SQLite DB opens and `edges` table is queryable for dependency/dependent reads.
- [ ] Exactly one dossier per selected queue row is created.
- [ ] Every processed non-blocked dossier contains all required headings.
- [ ] Every dossier contains at least one `WHEN / THEN / SHALL` oracle in `Verification Oracles`.
- [ ] All dossier claims in `Memory And Storage Implications` and `Snapshot And Catalog Implications` are supported by evidence or explicitly labeled inference.
- [ ] `ROLLUP.md` includes all required cross-file sections and `Next 10 Files`.
- [ ] Journal captures blockers with priority and reason.
- [ ] `git diff --check` returns no issues.

## 5) Open Questions

1. Should row-local blockers keep consuming batch budget (count as processed) or reduce target count?
2. Should `BATCH_LIMIT` remain constant at 10, or scale by file size/line_count budget?
3. Should dependency/dependent queries include only direct edges or include top-N 1-hop weighted edges?
4. Should `Coding Prompt Unlocked` include explicit fixture shape for every file, or only large/high-risk files?
5. Should we move the manifest and heading validation into a tracked helper script for all future batches?

## 6) Validation Snippet (final check)

Run at the end of every batch:

```bash
python3 - <<'PY'
from pathlib import Path
import csv

root = Path('/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker')
queue = root / 'docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv'
required_headings = [
    '## Source',
    '## Why This File Matters',
    '## Public Contract',
    '## Internal Mechanics',
    '## Memory And Storage Implications',
    '## Snapshot And Catalog Implications',
    '## Verification Oracles',
    '## Rust Rewrite Notes',
    '## Dependencies Read Next',
    '## Dependents As Tests',
    '## Open Questions',
    '## Coding Prompt Unlocked',
]

def has_required_headings(path: Path) -> bool:
    try:
        text = path.read_text(encoding='utf-8')
    except FileNotFoundError:
        return False
    return all(h in text for h in required_headings)

with queue.open(newline='', encoding='utf-8') as handle:
    rows = list(csv.DictReader(handle, delimiter='\t'))

for row in rows:
    dossier = root / row['dossier_path']
    if dossier.exists() and not has_required_headings(dossier):
        raise SystemExit(f"Missing headings in {dossier}")

selected = []
for row in sorted(rows, key=lambda r: int(r['priority'])):
    dossier = root / row['dossier_path']
    if not dossier.exists() or not has_required_headings(dossier):
        selected.append(row['priority'])
    if len(selected) == 10:
        break

print('pending_or_incomplete_selected', len(selected), selected[:10])
PY

git diff --check
```

## 7) Copy-Paste Goal

```text
/goal Use the spec at /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-executable-spec-goal-ready.md as the executable contract.

Scope is only:
- /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv
- /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/neo4j_family_graph.sqlite
- /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/gds-v2-dossiers

Process the first 10 uncompleted rows (ascending numeric priority). For each row:
- read the complete source file from "/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/<repo>/<file>";
- run both dependency SQL queries against the SQLite graph;
- generate one dossier at row `dossier_path` using the required headings;
- include evidence-backed claims, at least one WHEN/THEN/SHALL oracle, and Rust rewrite guidance with L1/L2/L3 classification.

Log row-local blockers in journals/gds-complete-read-batch1.md and continue. Then update docs_PRD03/reference-learning/gds-v2-dossiers/ROLLUP.md with cross-file decisions and next 10 files. Finish with dossier heading validation and `git diff --check`. Stop only after all gates pass.
```
