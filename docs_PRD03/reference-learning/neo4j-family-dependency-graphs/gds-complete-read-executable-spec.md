# Executable Spec: Neo4j GDS Complete-Read Loop (Goal-Ready v4)

This document is the authoritative execution contract for one bounded complete-read batch. It is intentionally verbose to prevent ambiguous interpretation during resumptions, handoffs, and automated execution.

## 1) Executive Intent

This goal is to reduce uncertainty for the Rust rewrite by building a small, deterministic, evidence-backed corpus:

- select a bounded batch of unread/unfinished files from the Neo4j GDS queue,
- read each file completely using its row-specific prompt,
- capture neighborhood context from the SQLite dependency graph,
- write dossiers with evidence discipline,
- and update one cross-file rollup and checkpointed journal entries.

If this is done in full, the next implementation round starts from facts, not assumptions.

## 2) Scope (Single Source of Truth)

Absolute paths are required:

- Queue: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv`
- Graph DB: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/neo4j_family_graph.sqlite`
- Base repo root: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family`
- Dossier output: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/gds-v2-dossiers/`
- Rollup output: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/gds-v2-dossiers/ROLLUP.md`
- Journal: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/journals/gds-complete-read-batch1.md`
- Dossier template reference: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-plan-v1.md`

No other repository is in scope.

## 3) Parsed Inputs

- **Feature outcome:** a verifiable evidence batch for rewrite architecture and RAM-risk planning.
- **Actors and boundaries:** local research agent only; code writes only to docs/journals.
- **Failure modes:** unreadable queue/DB/file, malformed manifest, missing headings, duplicate dossier path, missing SQL table, missing journal checkpoint.
- **Constraints:** no Neo4j source changes, no remote calls, no network, no manual editing of unrelated paths.
- **Performance limit:** bounded deterministic batch size, default `BATCH_LIMIT=10` unless fewer eligible rows remain.
- **Reproducibility requirement:** every acceptance check can be re-run from this document plus local files.

## 4) Fixed Constants and Canonical Rules

- `BATCH_LIMIT = 10`
- `QUEUE_COLUMNS_REQUIRED = priority,tier,lane,repo,folder,file,kind,line_count,fan_in,fan_out,total_degree,purpose,read_prompt,dossier_path`
- `DOSSIER_HEADINGS_REQUIRED`:
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
- `EVIDENCE_TAGS = ["Evidence", "Inference", "Blocked"]`

## 5) Mandatory TDD Progress Retention (`tdd-task-progress-context-retainer`)

This section enforces the skill contract for resumable TDD work.

Required checkpoint sections must be present in checkpoints:
- `Current Phase`
- `Tests Written`
- `Implementation Progress`
- `Current Focus`
- `Next Steps`
- `Context Notes`
- `Performance/Metrics`

### 5.1 Required checkpoint cadence

Capture/append checkpoints at these triggers:
1. Start of run.
2. End of manifest selection (before file reads).
3. Any row-level blocker.
4. Phase transitions (`Red` -> `Green` -> `Refactor` -> `Verify`).
5. Before and after final gate checks.

### 5.2 Tool availability and fallback

The documented skill references `scripts/progress_journal_orchestrator.py`, but this repository path currently does **not** contain that script.  
So we use a strict fallback: append checkpoints directly to:

`/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/journals/gds-complete-read-batch1.md`

with the exact schema below.

```markdown
## TDD Session State: 2026-07-06T12:34:56Z

### Current Phase: Red | Green | Refactor | Verify

### Tests Written:
- TEST-XXX-001: pass|fail|pending - <short assertion>

### Implementation Progress:
- <file>: <status - what changed>
- <file>: <status - what changed>

### Current Focus:
- <1 sentence>

### Next Steps:
1. ...
2. ...
3. ...

### Context Notes:
- why this decision was made
- blockers or risk
- cross-file dependencies

### Performance/Metrics:
- rows_selected=...
- blockers=...
- elapsed=...
- tokens=...
```

## 6) Executable Requirements

### REQ-NFVL-001.0 Scope Lock
**WHEN** run starts  
**THEN** the agent SHALL only read from queue, DB, and source root above, and only write to dossier folder, rollup, and journal path.
**AND** SHALL fail any path outside scope as a hard protocol violation.

### REQ-NFVL-002.0 Preflight
**WHEN** run starts  
**THEN** the run SHALL validate readability of queue file, SQLite DB, dossier output directory, and journal path.
**AND** SHALL parse queue headers and verify `QUEUE_COLUMNS_REQUIRED`.
**AND** SHALL log and stop as a global blocker if any required asset is missing.

### REQ-NFVL-003.0 Deterministic Selection
**WHEN** queue passes preflight  
**THEN** select rows not yet completed in ascending integer `priority`.
**AND** process no more than `BATCH_LIMIT` rows.
**AND** include rows where:
- dossier is missing, or
- required heading check fails.

### REQ-NFVL-004.0 Manifest-First Enforcement
**WHEN** selection is complete  
**THEN** the run SHALL persist selected manifest before reading any source file.
**AND** manifest SHALL include all selected priorities, file paths, lane, tier, and reason.

### REQ-NFVL-005.0 Full-File Read
**WHEN** a row is selected  
**THEN** the agent SHALL read the full file at `gitrefrepo/Neo4j family/<repo>/<file>`.
**AND** SHALL apply the exact row `read_prompt`.
**AND** SHALL avoid making claims based only on symbol-level snippets.
**AND** SHALL classify missing/unreadable files as row-local blocked with path, reason, and next action.

### REQ-NFVL-006.0 Dependency Graph Capture
**WHEN** source file processing succeeds  
**THEN** run both SQL queries:

```sql
SELECT target_file FROM edges WHERE repo = :repo AND source_file = :file ORDER BY target_file;
SELECT source_file FROM edges WHERE repo = :repo AND target_file = :file ORDER BY source_file;
```

**AND** include both result lists in dossier sections.
**AND** explicitly record empty result as `none`.

### REQ-NFVL-007.0 Dossier Contract
**WHEN** dossier write completes for a row  
**THEN** exactly one dossier SHALL be written at `dossier_path`.
**AND** it SHALL contain all required headings in section 4.
**AND** source metadata SHALL include priority, tier, lane, repo, file, line_count, fan_in, fan_out, total_degree.

### REQ-NFVL-008.0 Evidence Discipline
**WHEN** writing sections beyond metadata  
**THEN** each non-trivial claim SHALL be labeled `Evidence`, `Inference`, or `Blocked`.
**AND** `Evidence` SHALL include symbol/path and ideally line range.
**AND** `Inference` SHALL include uncertainty reason + verification step.

### REQ-NFVL-009.0 Oracle Coverage
**WHEN** writing `## Verification Oracles`  
**THEN** each dossier SHALL include at least one `WHEN / THEN / SHALL` statement.
**AND** this SHALL include expected outputs or expected failure signatures.

### REQ-NFVL-010.0 Rust Rewrite Guidance
**WHEN** writing `## Rust Rewrite Notes`  
**THEN** include 2–6 concrete candidate mappings (type/module/trait/error/contract).
**AND** assign each candidate as `L1`, `L2`, or `L3`.

### REQ-NFVL-011.0 Rollup Synthesis
**WHEN** batch ends  
**THEN** append to `ROLLUP.md` only cross-file conclusions.
**AND** include:
- reinforced decisions,
- memory/storage terms,
- catalog/snapshot invariants,
- blockers + unsupported candidates,
- next 10 recommended rows.
**AND** do not duplicate full per-row prose.

### REQ-NFVL-012.0 Journal Traceability
**WHEN** batch starts, block occurs, and batch finishes  
**THEN** append corresponding checkpoints to `journals/gds-complete-read-batch1.md` with exact selected priorities and blocker paths.

### REQ-NFVL-013.0 Completion Gates
**WHEN** run is reported complete  
**THEN** all conditions in section 10 must hold and be verifiable.

### REQ-NFVL-014.0 Replayability
**WHEN** a new operator resumes work  
**THEN** they must be able to continue from latest checkpoint and manifest without external memory.

## 7) Test Matrix

| req_id | test_id | type | assertion | target |
| --- | --- | --- | --- | --- |
| REQ-NFVL-001.0 | TEST-SCOPE-001 | audit | assets are in scope | run plan |
| REQ-NFVL-002.0 | TEST-PRE-001 | preflight | required files exist + queue has required headers | queue/db/journal |
| REQ-NFVL-002.0 | TEST-PRE-002 | preflight | `priority` parses int for all rows | parser |
| REQ-NFVL-003.0 | TEST-BATCH-001 | selection | selected rows are lowest uncompleted priorities | manifest |
| REQ-NFVL-003.0 | TEST-BATCH-002 | cap | selected count <= `BATCH_LIMIT` | manifest |
| REQ-NFVL-004.0 | TEST-MANIFEST-001 | data integrity | manifest includes path + lane + reason | manifest |
| REQ-NFVL-005.0 | TEST-READ-001 | integration | source file read works | source root |
| REQ-NFVL-006.0 | TEST-SQL-001 | query | dependency query executes | sqlite |
| REQ-NFVL-006.0 | TEST-SQL-002 | query | dependent query executes | sqlite |
| REQ-NFVL-007.0 | TEST-DOSS-001 | filesystem | one dossier per selected non-blocked row | dossier path |
| REQ-NFVL-007.0 | TEST-DOSS-002 | schema | every required heading present | dossier parser |
| REQ-NFVL-008.0 | TEST-EVID-001 | content | claims include required evidence tags | dossier content |
| REQ-NFVL-009.0 | TEST-ORACLE-001 | content | at least one oracle in each dossier | dossier content |
| REQ-NFVL-010.0 | TEST-RUST-001 | content | rewrite candidates with L1/L2/L3 | dossier content |
| REQ-NFVL-011.0 | TEST-ROLLUP-001 | schema | rollup has next 10 + cross-file sections | rollup |
| REQ-NFVL-012.0 | TEST-JOURNAL-001 | audit | run start/blocker/finish checkpoints | journal |
| REQ-NFVL-013.0 | TEST-GATE-001 | quality | completion gates all pass | validation script |
| REQ-NFVL-013.0 | TEST-GIT-001 | quality | `git diff --check` clean | repo |

## 8) TDD Plan (Execution)

### STUB
1. Read queue and validate headers.
2. Build immutable manifest sorted by priority.
3. Verify manifest and journal paths are writable.
4. Add initial Red checkpoint.

### RED
1. Run preflight checks and query a few rows from queue for header + path sanity.
2. Assert selection rules on uncompleted rows.
3. Ensure manifest exists and checkpoint with selected rows is written before file reads.

### GREEN
For each selected row:
1. Read full file.
2. Run both SQL queries and capture outputs.
3. Write one dossier with:
   - required headings,
   - evidence labels,
   - one or more oracles,
   - 2–6 rewrite candidates.
4. Blockers are logged and processing continues.

### REFACTOR
1. Normalize all dossiers to same evidence format and oracle style.
2. Remove duplicated prose by consolidating non-local findings into rollup.
3. Verify no row-local claims are promoted as cross-file conclusions without at least two evidence points.

### VERIFY
1. Run validator scripts in section 9.
2. Run `git diff --check`.
3. Append final Verify checkpoint and close phase.

## 9) Validation Snippets

### 9.1 Manifest+completion validator

```bash
python3 - <<'PY'
from pathlib import Path
import csv
import sqlite3
import json
from datetime import datetime, timezone

ROOT = Path("/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker")
QUEUE = ROOT / "docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv"
DB = ROOT / "docs_PRD03/reference-learning/neo4j-family-dependency-graphs/neo4j_family_graph.sqlite"
ROLLUP = ROOT / "docs_PRD03/reference-learning/gds-v2-dossiers/ROLLUP.md"
JOURNAL = ROOT / "journals/gds-complete-read-batch1.md"

REQUIRED_COLUMNS = [
    "priority","tier","lane","repo","folder","file","kind","line_count","fan_in","fan_out",
    "total_degree","purpose","read_prompt","dossier_path"
]
REQUIRED_HEADINGS = [
    "## Source","## Why This File Matters","## Public Contract","## Internal Mechanics",
    "## Memory And Storage Implications","## Snapshot And Catalog Implications",
    "## Verification Oracles","## Rust Rewrite Notes","## Dependencies Read Next",
    "## Dependents As Tests","## Open Questions","## Coding Prompt Unlocked",
]

def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

with QUEUE.open(newline="", encoding="utf-8") as f:
    rows = list(csv.DictReader(f, delimiter="\t"))

if not rows:
    raise SystemExit("FAIL: queue empty")
for col in REQUIRED_COLUMNS:
    if col not in rows[0]:
        raise SystemExit(f"FAIL: missing queue column {col}")
for row in rows:
    int(row["priority"])

def dossier_ok(path: Path) -> bool:
    try:
        txt = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return False
    return all(h in txt for h in REQUIRED_HEADINGS)

selected = []
for row in sorted(rows, key=lambda r: int(r["priority"])):
    p = ROOT / row["dossier_path"]
    if not p.exists() or not dossier_ok(p):
        selected.append({
            "priority": int(row["priority"]),
            "repo": row["repo"],
            "file": row["file"],
            "dossier_path": row["dossier_path"],
            "lane": row["lane"],
        })
    if len(selected) >= 10:
        break

if not DB.exists():
    raise SystemExit(f"FAIL: sqlite missing at {DB}")
conn = sqlite3.connect(DB)
cur = conn.cursor()
for row in selected:
    cur.execute("SELECT target_file FROM edges WHERE repo=? AND source_file=? ORDER BY target_file", (row["repo"], row["file"]))
    cur.fetchall()
    cur.execute("SELECT source_file FROM edges WHERE repo=? AND target_file=? ORDER BY source_file", (row["repo"], row["file"]))
    cur.fetchall()

if not ROLLUP.exists():
    raise SystemExit("FAIL: rollup missing")
if not JOURNAL.exists():
    raise SystemExit("FAIL: journal missing")

print("COMPLETION_GATE_RUNNING")
print(json.dumps({
    "ts": now(),
    "selected_count": len(selected),
    "selected_priorities": [r["priority"] for r in selected],
    "unique_paths": len(set(r["dossier_path"] for r in selected)),
}, indent=2))
PY

git diff --check
```

### 9.2 Manifest artifact helper (optional but recommended)

```bash
cat > /tmp/gds-complete-read-manifest.json <<'EOF'
{
  "manifest_version": "v1",
  "generated_at_utc": "REPLACE_WITH_TS",
  "batch_limit": 10,
  "selected": []
}
EOF
```

## 10) Quality Gates

- [ ] Preflight pass: queue, db, source root, dossier output dir, journal.
- [ ] `REQUIRED_COLUMNS` validated and numeric priorities parse.
- [ ] Selected rows are sorted ascending by priority and capped at 10.
- [ ] One manifest entry per selected row is recorded before source reads.
- [ ] Each non-blocked selected row has one dossier at expected path.
- [ ] Each required dossier heading exists.
- [ ] Each dossier includes:
  - Evidence/Inference discipline
  - at least one oracle
  - 2-6 L1/L2/L3 candidates.
- [ ] Rollup contains only cross-file patterns and next 10 recommendation.
- [ ] Journal includes all required checkpoint sections at start, blocker points, and completion.
- [ ] Duplicate dossier paths are not present in selected rows.
- [ ] `git diff --check` exits 0.

## 11) Copy-Paste Goal

```text
/goal You are in /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker.
Use /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-executable-spec.md as hard constraints.

1) Preflight all assets in Section 2.
2) Select up to BATCH_LIMIT=10 rows using ascending numeric priority and write manifest first.
3) For each selected row: full-file read, dependency queries, dossier write with required headings.
4) For each dossier: add Evidence/Inference labels, at least one WHEN/THEN/SHALL oracle, and 2-6 Rust candidate mappings (L1/L2/L3).
5) On blocker, log `path`, `reason`, and action in journal and continue remaining rows.
6) Update ROLLUP.md with cross-file findings, blockers, and next 10 files.
7) Add Red/Green/Verify checkpoints to journals/gds-complete-read-batch1.md per section 5.
8) Run completion validator in section 9 and `git diff --check`.
9) Finish only if all gates pass.
```

## 12) Open Questions

1. Should this run count blocked rows toward quota or retry them in the next run? (Current interpretation: count them in manifest with status.)
2. Should empty query results trigger explicit queue-purpose rationale notes?
3. Should line-range anchors be mandatory for all `Evidence` claims or only symbol anchors for long files?
4. Should we raise `BATCH_LIMIT` dynamically by file-size/complexity, or keep deterministic `10` rows always?
5. After this pass, should we introduce a dedicated tracked validator script instead of inline snippets?

