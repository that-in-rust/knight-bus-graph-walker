# Executable Spec: Neo4j GDS Complete-Read Batch (Goal-Ready)

## Purpose

Generate a verification-first evidence corpus for the Rust rewrite by reading a bounded batch from the Neo4j GDS complete-read queue, writing one dossier per selected file, and updating a rollup with cross-file decisions.

## Context Inputs

| Input | Value |
| --- | --- |
| Queue | `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv` |
| Graph DB | `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/neo4j_family_graph.sqlite` |
| Source root | `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/neo4j-gds-src` |
| Dossier output | `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/gds-v2-dossiers/` |
| Rollup output | `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/gds-v2-dossiers/ROLLUP.md` |
| Journal | `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/journals/gds-complete-read-batch1.md` |
| Existing dossier schema | `docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-plan-v1.md` |

## Parsed Inputs

| Input | Value |
| --- | --- |
| Feature outcome | Produce complete-read dossiers for exactly one bounded batch and capture reusable rewrite-oriented evidence. |
| Actors and boundaries | Local repo only; no external web fetches. |
| Failure modes | Missing/unreadable queue/DB, missing source file, malformed row, blocked write, malformed dossier heading set. |
| Throughput/reliability | Fixed batch size target = 10 rows by default (`BATCH_LIMIT`, default `10`), unless fewer rows remain or global blocker occurs. |
| Constraints | Full-file reading required for selected rows, every claim traceable to file symbols and/or direct dependency queries. |

## Executable Requirements

### REQ-GDS-SCOPE-001.0: Read queue and validate schema

**WHEN** the goal starts  
**THEN** the agent SHALL read `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv` as UTF-8 TSV  
**AND** SHALL verify required headers: `priority`, `tier`, `lane`, `repo`, `folder`, `file`, `kind`, `line_count`, `fan_in`, `fan_out`, `total_degree`, `purpose`, `read_prompt`, `dossier_path`  
**AND** SHALL fail the batch if queue is unreadable or malformed.  
**SHALL** record this blocker in `ROLLUP.md`.

### REQ-GDS-BATCH-002.0: Bounded selection by priority

**WHEN** queue rows are valid and complete  
**THEN** the agent SHALL select rows in ascending numeric `priority` where `dossier_path` either does not exist or is incomplete  
**AND** SHALL process at most `BATCH_LIMIT` rows in one run (default `10`)  
**AND** SHALL log selected priorities and file paths in a run manifest (journal or temporary manifest file) before reading source code.  
**SHALL** process fewer than 10 only when fewer rows remain or a row-local blocker is unavoidable.

### REQ-GDS-READ-003.0: Read full files, not snippets

**WHEN** a row is selected  
**THEN** the agent SHALL read the complete file at `/Users/.../gitrefrepo/Neo4j family/<repo>/<file>`  
**AND** SHALL apply the row's `read_prompt` to drive interpretation  
**AND** SHALL treat partial snippets as insufficient evidence.  
**SHALL** classify a missing file as a row-local blocker and continue.

### REQ-GDS-GRAPH-004.0: Capture direct neighborhood

**WHEN** file evidence is extracted for a selected row  
**THEN** the agent SHALL run both SQL queries against `neo4j_family_graph.sqlite`:

```sql
SELECT target_file FROM edges WHERE repo = :repo AND source_file = :file ORDER BY target_file;
SELECT source_file FROM edges WHERE repo = :repo AND target_file = :file ORDER BY source_file;
```

**AND** the dossier SHALL include top direct dependencies and direct dependents lists with rationale.  
**SHALL** explicitly record when queries return zero rows.

### REQ-GDS-DOSS-005.0: Produce one dossier per row

**WHEN** file and neighborhood extraction is complete for a row  
**THEN** the agent SHALL create exactly one dossier at `dossier_path` using the schema in `gds-complete-read-plan-v1.md`  
**AND** SHALL include metadata in `## Source` matching the row: `repo`, `file`, `lane`, `tier`, `priority`, `line_count`, `fan_in / fan_out`, `purpose`, `read_prompt`.  
**SHALL** NOT merge multiple rows into one dossier.

### REQ-GDS-EVID-006.0: Distinguish evidence from inference

**WHEN** writing behavioral, architectural, or memory claims  
**THEN** claims SHALL be tagged as **Evidence** with symbol names / line-level anchors or filenames where visible  
**OR** as **Inference** when only derived interpretation exists.  
**SHALL** avoid unbacked compatibility or performance claims.

### REQ-GDS-VERIFY-007.0: Include executable oracles

**WHEN** writing `## Verification Oracles`  
**THEN** each dossier SHALL include at least one **WHEN / THEN / SHALL** item that maps to a concrete test target/fixture (unit or integration)  
**AND** SHALL include expected output or expected error state.  
**SHALL** include an explicit "No direct oracle" note if behavior is non-testable in isolation.

### REQ-GDS-RUST-008.0: Extract Rust rewrite guidance

**WHEN** closing a dossier  
**THEN** the agent SHALL provide concrete rewrite candidates in `## Rust Rewrite Notes` including module/type/trait/error candidates  
**AND** SHALL classify each candidate as L1/L2/L3 where inferable.  
**AND** SHALL preserve external compatibility names where boundary compatibility matters.

### REQ-GDS-ROLLUP-009.0: Update batch rollup

**WHEN** the bounded batch is done or blocked  
**THEN** the agent SHALL update `/Users/.../docs_PRD03/reference-learning/gds-v2-dossiers/ROLLUP.md` with:
- cross-file decisions
- memory/storage and projection/catalog inferences
- blockers (if any) by priority
- next 10 candidate files
- no copied dossier prose.

### REQ-GDS-COMPLETE-010.0: Mechanical completion checks

**WHEN** reporting batch completion  
**THEN** the agent SHALL verify:
- count of dossiers written equals selected non-blocked rows,
- each dossier contains all required headings,
- each processed priority maps to one dossier path,
- `git diff --check` returns 0.  
**AND** SHALL list any mismatches as blockers before finalizing.

### REQ-GDS-HALT-011.0: Handle blockers deterministically

**WHEN** required queue/DB/source/target path cannot be read or written  
**THEN** the batch SHALL either continue row-locally (for missing single-file rows) or halt globally (for structural read/write failures)  
**AND** SHALL log exact reason and impacted path in rollup and journal.

## Test Matrix

| req_id | test_id | type | assertion | target |
| --- | --- | --- | --- | --- |
| REQ-GDS-SCOPE-001.0 | TEST-QUEUE-001 | preflight | queue exists and headers are present | `gds-complete-read-queue.tsv` |
| REQ-GDS-SCOPE-001.0 | TEST-QUEUE-002 | preflight | numeric priorities parse as integers | queue parser |
| REQ-GDS-BATCH-002.0 | TEST-BATCH-001 | selection | selected rows are first N unprocessed priorities | selection manifest |
| REQ-GDS-READ-003.0 | TEST-FILE-001 | integration | each selected row file exists/readable | source files |
| REQ-GDS-GRAPH-004.0 | TEST-SQL-001 | integration | dependency query executes and returns ordered rows | `neo4j_family_graph.sqlite` |
| REQ-GDS-GRAPH-004.0 | TEST-SQL-002 | integration | dependent query executes and returns ordered rows | `neo4j_family_graph.sqlite` |
| REQ-GDS-DOSS-005.0 | TEST-DOSS-001 | filesystem | one dossier exists at each row's `dossier_path` | output folder |
| REQ-GDS-DOSS-005.0 | TEST-DOSS-002 | schema | dossier has all required headings | Markdown validation |
| REQ-GDS-EVID-006.0 | TEST-EVID-001 | review | evidence/inference tags exist for non-trivial claims | dossier content |
| REQ-GDS-VERIFY-007.0 | TEST-ORACLE-001 | schema | each dossier has non-empty oracle section with WHEN/THEN/SHALL | dossier content |
| REQ-GDS-RUST-008.0 | TEST-RUST-001 | review | rewrite notes include module/type/trait candidates with L1/L2/L3 labels | dossier content |
| REQ-GDS-ROLLUP-009.0 | TEST-ROLLUP-001 | schema | rollup has cross-file sections and `Next 10 Files` | `ROLLUP.md` |
| REQ-GDS-COMPLETE-010.0 | TEST-COMPLETE-001 | verification | processed rows, dossier count, and heading checks are consistent | validation script |
| REQ-GDS-HALT-011.0 | TEST-BLOCK-001 | negative | missing file is logged and does not silently vanish | rollup |

## TDD Plan

### STUB

- Build selection manifest from queue (sorted by priority) with expected fields.
- Define dossier heading checklist from the plan schema.
- Capture DB query templates and expected columns.

### RED

- Run preflight checks and confirm blockers for any missing inputs.
- Validate current state to detect pre-existing dossier issues.
- Log manifest and selected priorities before reading file contents.

### GREEN

- For each selected row:
  1. Read full source file.
  2. Execute two direct graph queries.
  3. Write one dossier using schema, clearly separating Evidence and Inference.
  4. Record row-local blockers and continue.

### REFACTOR

- Normalize language to be consistent across dossiers.
- De-duplicate cross-file observations into rollup only.
- Tighten oracle statements to deterministic fixtures and expected results.

### VERIFY

- Run dossier schema validator and diff check.
- Update rollup with blockers and the next batch list.
- Report exactly which priorities were processed and why anything was skipped.

## Quality Gates

- [ ] queue file exists and has required headers.
- [ ] SQLite database exists and dependency tables are queryable.
- [ ] batch size is exactly `min(BATCH_LIMIT, remaining_rows)` unless global blocker.
- [ ] every selected row has dossier path unique and single-file dossier result.
- [ ] every non-blocked dossier contains required headings:
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
- [ ] at least one WHEN/THEN/SHALL oracle in each dossier (or explicit non-testable rationale).
- [ ] rollup includes `Next 10 Files`.
- [ ] `git diff --check` exits with no issues.
- [ ] no `TODO`, `STUB`, or `FIXME` introduced in generated narrative that imply implementation code.

## Open Questions

1. Should row blocking stop the whole batch only for global blockers, or should we continue until 10 processed dossiers are complete?
2. Should `BATCH_LIMIT` remain `10` after the first two batches, or adapt by file-size/time budget?
3. Should line-number citations be mandatory for every claim, or only for algorithmic and behavioral claims?
4. Should rollup separate decisions into dedicated docs for memory, catalog, and procedures once cross-file density exceeds 20 dossiers?
5. Should we convert the validation snippet into a tracked script after this batch completes?

## Copy-Paste Goal (Use This as a `/goal`)

```text
/goal You are in /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker.
Use /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-executable-spec-v2.md and /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv as hard constraints.
Process up to BATCH_LIMIT=10 priority rows where dossiers are missing/incomplete, in ascending priority.
For each selected row, read the complete source file from /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/<repo>/<file>, apply its read_prompt, run direct dependency/dependent queries against /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/neo4j_family_graph.sqlite, and write exactly one dossier at the row's dossier_path using the dossier schema in gds-complete-read-plan-v1.md.
Each dossier must separate Evidence vs Inference, include at least one WHEN/THEN/SHALL verification oracle, and propose concrete Rust rewrite candidates with L1/L2/L3 classification.
Record row-local blockers in journals/gds-complete-read-batch1.md and ROLLUP.md without fabricating claims.
After the batch, update /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/gds-v2-dossiers/ROLLUP.md with cross-file decisions and Next 10 Files.
Then run git diff --check and the dossier-completion validator before reporting completion.
```

## Validation Snippet (run at end)

```bash
python3 - <<'PY'
from pathlib import Path
import csv

root = Path('/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker')
queue = root / 'docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv'
rollup = root / 'docs_PRD03/reference-learning/gds-v2-dossiers/ROLLUP.md'
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
    return all(heading in text for heading in required_headings)

with queue.open(newline='') as handle:
    rows = list(csv.DictReader(handle, delimiter='\t'))

selected = []
for row in sorted(rows, key=lambda r: int(r['priority'])):
    dossier = root / row['dossier_path']
    if not dossier.exists() or not has_required_headings(dossier):
        selected.append((row['priority'], row['repo'], row['file'], dossier))
    if len(selected) == 10:
        break

for priority, repo, file, dossier in selected:
    text = dossier.read_text(encoding='utf-8')
    missing = [heading for heading in required_headings if heading not in text]
    if missing:
        raise SystemExit(f'missing headings in {dossier}: {missing}')

print('selected_count', len(selected))
print('selected_priorities', [p for p, *_ in selected])
print('rollup_exists', rollup.exists())
PY
```
