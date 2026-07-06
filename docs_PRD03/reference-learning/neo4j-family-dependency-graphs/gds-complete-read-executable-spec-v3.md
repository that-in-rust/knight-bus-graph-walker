# Executable Spec: Neo4j GDS Complete-Read Loop (Goal-Ready v3)

## 1) Purpose

Create a repeatable, verification-first evidence pipeline to extract high-signal architectural and behavioral facts from the Neo4j GDS corpus for the Rust rewrite. The output is not implementation code; it is a complete-read dossier corpus and rollup that can drive executable decisions in later implementation goals.

## 2) Scope

Allowed inputs and outputs for this goal:

- Queue: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv`
- Graph DB: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/neo4j_family_graph.sqlite`
- Source root: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/neo4j-gds-src`
- Dossier folder: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/gds-v2-dossiers`
- Rollup: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/gds-v2-dossiers/ROLLUP.md`
- Journal: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/journals/gds-complete-read-batch1.md`
- Baseline dossier schema: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-plan-v1.md`

## 3) Parsed Request Inputs

- Feature outcome: generate complete, evidence-linked dossiers for exactly one bounded batch and produce an updated rollup for rewrite planning
- Actors/boundaries: local local repo only; no web fetch
- Failure modes: unreadable queue/db, missing file, malformed row, dependency query failure, missing heading constraints, duplicate dossier paths
- Performance/reliability limits: read full files only for at most 10 rows per goal turn; deterministic selection and strict pass/fail validation
- Language/runtime constraints: no source code changes required in this run; outputs are `.tsv`, `.sqlite` query reads, and Markdown notes

## 4) Executable Requirements

### REQ-GDS-SCOPE-001.0: Controlled objective envelope
**WHEN** the goal starts
**THEN** the agent SHALL treat only the inputs in scope above as authoritative data sources
**AND** SHALL NOT rewrite files outside `docs_PRD03/reference-learning/gds-v2-dossiers`, `journals/gds-complete-read-batch1.md`, or this spec workflow
**SHALL** record a global blocker if queue or DB is unreadable.

### REQ-GDS-BATCH-002.0: Deterministic selection bounded to work unit
**WHEN** queue rows are valid
**THEN** the agent SHALL process rows in ascending numeric `priority`
**AND** SHALL select only rows where `dossier_path` does not exist or is missing required headings
**AND** SHALL process at most `BATCH_LIMIT=10` rows unless fewer uncompleted rows remain
**AND** SHALL write the selected manifest (priorities + file paths + reason) before reading any source file.

### REQ-GDS-READ-003.0: Full-file-read obligation
**WHEN** a row is selected
**THEN** the agent SHALL read the full file at `gitrefrepo/Neo4j family/<repo>/<file>`
**AND** SHALL apply that row’s `read_prompt`
**AND** SHALL not rely on symbol-only greps for claims in dossier sections
**SHALL** classify missing/unreadable files as row-local blockers and continue remaining rows.

### REQ-GDS-SQL-004.0: Neighborhood capture for each row
**WHEN** processing a selected row
**THEN** the agent SHALL execute both queries against `neo4j_family_graph.sqlite`:

```sql
SELECT target_file FROM edges WHERE repo = :repo AND source_file = :file ORDER BY target_file;
SELECT source_file FROM edges WHERE repo = :repo AND target_file = :file ORDER BY source_file;
```

**AND** SHALL include both dependency directions in the dossier under dedicated headings
**AND** SHALL explicitly record `none` when result sets are empty.

### REQ-GDS-DOSS-005.0: One dossier per selected row with fixed schema
**WHEN** extraction for a row completes
**THEN** the agent SHALL create exactly one dossier at `dossier_path`
**AND** the dossier SHALL contain all headings below:

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

### REQ-GDS-EVID-006.0: Evidence-first claim policy
**WHEN** writing each non-trivial claim
**THEN** the agent SHALL label it as either **Evidence** (symbol names, method names, visible exceptions, field names, line-number evidence) or **Inference**.
**AND** SHALL avoid uncited claims in contract, memory, and lifecycle sections.

### REQ-GDS-ORACLE-007.0: Executable verification oracles per dossier
**WHEN** writing `## Verification Oracles`
**THEN** each dossier SHALL include at least one `WHEN / THEN / SHALL` oracle
**AND** SHALL define a concrete target test assertion (unit or integration)
**AND** SHALL include expected result or expected failure state.

### REQ-GDS-RUST-008.0: Rewrite mapping with architecture level tags
**WHEN** writing `## Rust Rewrite Notes`
**THEN** the agent SHALL propose 2-6 concrete candidates (module/type/trait/error) for Rust mapping
**AND** SHALL label each as one of `L1`, `L2`, or `L3`.

### REQ-GDS-ROLLUP-009.0: Cross-file rollup update after each batch
**WHEN** batch work is finished
**THEN** the agent SHALL update `ROLLUP.md` with at least:
- cross-file decisions
- module candidates
- verification specs
- memory/accounting terms
- projection/catalog invariants
- unsupported behavior candidates
- blockers
- next 10 file recommendations
**AND** SHALL not copy dossier prose verbatim.

### REQ-GDS-JOURNAL-010.0: Row and blocker traceability
**WHEN** batch starts and after each blocker
**THEN** the agent SHALL append timestamped entries to `journals/gds-complete-read-batch1.md`
**AND** SHALL include selected priorities, blockers, and completion status.

### REQ-GDS-COMPLETE-011.0: Mechanical completion checks
**WHEN** reporting completion
**THEN** the agent SHALL verify:
- number of dossiers created == number of selected non-blocked rows (or documented exception)
- every processed dossier has all required headings
- dossier_path uniqueness across created files
- blocked rows have explicit reasons and paths
- `git diff --check` returns 0.

## 5) Test Matrix

| req_id | test_id | type | assertion | target |
| --- | --- | --- | --- | --- |
| REQ-GDS-SCOPE-001.0 | TEST-QUEUE-001 | preflight | queue is readable and required headers present | `gds-complete-read-queue.tsv` |
| REQ-GDS-SCOPE-001.0 | TEST-DB-001 | preflight | SQLite opens and `edges` table exists | `neo4j_family_graph.sqlite` |
| REQ-GDS-BATCH-002.0 | TEST-BATCH-001 | selection | selected priorities are first uncompleted rows (ascending) | manifest builder |
| REQ-GDS-READ-003.0 | TEST-FILE-001 | integration | selected source file path exists/readable | source root |
| REQ-GDS-SQL-004.0 | TEST-SQL-001 | integration | both SQL neighborhood queries return expected schema and ordered output | `neo4j_family_graph.sqlite` |
| REQ-GDS-DOSS-005.0 | TEST-DOSS-001 | filesystem | one dossier exists at each selected `dossier_path` | dossier folder |
| REQ-GDS-DOSS-005.0 | TEST-DOSS-002 | schema | all required headings exist in each dossier | parser check |
| REQ-GDS-EVID-006.0 | TEST-EVID-001 | review | evidence/inference labels are present in non-trivial claims | dossier content |
| REQ-GDS-ORACLE-007.0 | TEST-ORACLE-001 | schema | each dossier has WHEN/THEN/SHALL oracle | dossier content |
| REQ-GDS-RUST-008.0 | TEST-RUST-001 | review | rewrite notes include tagged L1/L2/L3 candidates | dossier content |
| REQ-GDS-ROLLUP-009.0 | TEST-ROLLUP-001 | schema | rollup contains required sections and Next 10 files | `ROLLUP.md` |
| REQ-GDS-JOURNAL-010.0 | TEST-JOURNAL-001 | audit | blockers and selected priorities appended each run | journal |
| REQ-GDS-COMPLETE-011.0 | TEST-COMPLETE-001 | verification | heading checks and uniqueness checks pass | validation script |

## 6) TDD Plan (Executable)

### STUB
1. Parse queue headers and validate required columns.
2. Build immutable manifest of target rows (`priority`, `repo`, `file`, `dossier_path`).
3. Persist manifest summary in command output and journal before reading source.
4. Draft expected headings checklist and SQL query list.

### RED
1. Run preflight checks and intentionally confirm failures for malformed input conditions.
2. Confirm missing headings in already-created dossiers are detected by validator.
3. Record expected blocker format in journal.

### GREEN
For each selected row:
1. Read full source file (not snippets).
2. Apply row read_prompt.
3. Run both SQLite dependency queries.
4. Generate one dossier with the required sections.
5. Include explicit Evidence/Inference split in behavior and architecture claims.
6. Add at least one concrete oracle and 2–6 Rust rewrite suggestions.
7. Mark blocked rows clearly and continue remaining rows.

### REFACTOR
1. Normalize dossier language across files in the batch.
2. Move only cross-file conclusions to rollup.
3. Eliminate duplicated or contradictory entries.

### VERIFY
1. Run automated checks and print result summary.
2. Run `git diff --check`.
3. Update `ROLLUP.md` + journal and stop only on green completion gates.

## 7) Quality Gates

- [ ] Scope file + DB + source root readability validated
- [ ] Manifest selection is bounded and deterministic (<=10 rows)
- [ ] Each non-blocked row has one dossier with full schema
- [ ] Each dossier has at least one `WHEN / THEN / SHALL` oracle
- [ ] Evidence/Inference policy is used for claims in 5+ sections
- [ ] `ROLLUP.md` includes required sections and next 10 files
- [ ] Journal records blockers and completion state
- [ ] `git diff --check` passes with no whitespace/merge issues
- [ ] No TODO/Stub/FIXME assertions in output narratives that imply implementation code

## 8) Validation Snippet (run before reporting)

```bash
python3 - <<'PY'
from pathlib import Path
import csv, sqlite3

root = Path('/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker')
queue = root / 'docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv'
dossier_root = root / 'docs_PRD03/reference-learning/gds-v2-dossiers'
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

def read_queue(path):
    with path.open(newline='', encoding='utf-8') as h:
        return list(csv.DictReader(h, delimiter='\t'))

def dossier_ok(path):
    if not path.exists():
        return False
    text = path.read_text(encoding='utf-8')
    return all(h in text for h in required_headings)

rows = read_queue(queue)
selected = []
for row in sorted(rows, key=lambda r: int(r['priority'])):
    path = root / row['dossier_path']
    if not path.exists() or not dossier_ok(path):
        selected.append(row)
    if len(selected) >= 10:
        break

print('SELECTED_PRIORITIES', [r['priority'] for r in selected])

for row in selected:
    path = root / row['dossier_path']
    if path.exists() and not dossier_ok(path):
        print('INVALID_HEADINGS', row['priority'], row['dossier_path'])

# Optional dependency sanity check for selected rows only
conn = sqlite3.connect(root / 'docs_PRD03/reference-learning/neo4j-family-dependency-graphs/neo4j_family_graph.sqlite')
cur = conn.cursor()
for row in selected:
    repo = row['repo']
    file = row['file']
    for sql in [
        "SELECT target_file FROM edges WHERE repo = ? AND source_file = ? ORDER BY target_file",
        "SELECT source_file FROM edges WHERE repo = ? AND target_file = ? ORDER BY source_file",
    ]:
        cur.execute(sql, (repo, file))
        cur.fetchall()
print('SQL_OK')
PY

git diff --check
```

## 9) Copy-Paste Goal

```text
/goal You are in /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker.
Use /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-executable-spec-v3.md as hard constraints.

Process up to BATCH_LIMIT=10 queue rows where dossiers are missing/incomplete, sorted by ascending `priority` from:
/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv.

For each selected row:
- read the full source file from `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family/<repo>/<file>` using the row's `read_prompt`
- run the direct dependency and dependent queries against `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/neo4j_family_graph.sqlite`
- create exactly one dossier at `dossier_path` with all required headings
- add clearly separated Evidence/Inference claims
- include at least one concrete WHEN/THEN/SHALL oracle per dossier
- include Rust rewrite candidates tagged L1/L2/L3

Record row-local blockers in `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/journals/gds-complete-read-batch1.md` and continue remaining rows.
After the batch, update `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/gds-v2-dossiers/ROLLUP.md` with cross-file findings and next 10 files.
Run git diff --check and the dossier completion validator before finalizing.
``` 

## 10) Open Questions

1. For this loop, is 10 rows per run still the right unit, or should `BATCH_LIMIT` adapt to file complexity?
2. If dependency query returns no rows, should we still require a short rationale from the queue purpose line or mark as unknown?
3. Should rollup evidence terms split into separate evidence classes (`algorithm`, `catalog`, `memory`, `procedure`) after 20 dossiers?
4. Should missing-row evidence in dossier be represented as `Blocked` or `No Data` for strict comparability?
