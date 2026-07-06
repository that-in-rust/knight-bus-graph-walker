# Executable Spec: Neo4j Family Verification-First Read Loop (v4)

## 1) Purpose

Create a deterministic, evidence-first workflow that selects high-priority Neo4j-family files, reads complete source, captures graph neighborhoods, and produces:

- per-file dossiers for rewrite decisions
- a cross-file rollup for architecture/planning
- minimal, machine-checkable completion evidence

This is not implementation code. It is the research/verification engine that enables safer Rust rewrite decisions.

## 2) Inputs and Scope

Absolute paths are mandatory:

- Queue and metadata:
  - `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv`
  - `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/neo4j_family_graph.sqlite`
- Source:
  - `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family`
- Outputs:
  - `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/gds-v2-dossiers`
  - `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/gds-v2-dossiers/ROLLUP.md`
  - `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/journals/gds-complete-read-batch1.md`

In-scope repos are only those referenced in queue rows.

## 3) Parsed Request Inputs

- Feature outcome: identify and fully summarize the top-priority files that most directly affect OLAP-aligned rewrite decisions and RAM risk.
- Actors: verification-first LLM execution loop, PRD owner, architecture reviewer, Rust implementer, benchmark author.
- Failure modes: missing file/db/queue, unreadable files, duplicate dossier writes, malformed headings, unsupported claim provenance, malformed SQL neighborhood reads.
- Reliability limit: every dossier must be traceable to evidence and at least one executable oracle.
- Constraint: no source writes in Neo4j-family repos; outputs only in `docs_PRD03` + `journals`.

## 4) Fixed Constants

- `BATCH_LIMIT = 10` (can be revised by explicit goal)
- `NEED_EVIDENCE_LABELS = ["Evidence", "Inference", "Blocked"]`
- `REQ_PREFIX = REQ-NFVL`
- `LANG = markdown` for artifacts

## 5) Executable Requirements

### REQ-NFVL-001.0: Scope and preflight
**WHEN** goal starts  
**THEN** the agent SHALL validate all three source assets: queue, graph DB, and Neo4j family source root  
**AND** SHALL abort with a global blocker if any asset is unreadable  
**SHALL** include blocker details in the journal before continuing.

### REQ-NFVL-002.0: Deterministic batch selection
**WHEN** queue rows are valid  
**THEN** the agent SHALL select rows in ascending numeric `priority` where dossier is missing or does not contain all required headings  
**AND** SHALL cap the batch at `BATCH_LIMIT` unless fewer such rows remain  
**AND** SHALL persist the selected manifest before source read begins.

### REQ-NFVL-003.0: Full-file read contract
**WHEN** a row is selected  
**THEN** the agent SHALL read the complete source file from `gitrefrepo/Neo4j family/<repo>/<file>`  
**AND** SHALL follow row `read_prompt` intent explicitly  
**AND** SHALL not treat symbol-only snippets as final evidence for architecture claims  
**SHALL** skip with row-blocker on missing/unreadable file and continue remaining rows.

### REQ-NFVL-004.0: Graph neighborhood capture
**WHEN** processing a row  
**THEN** the agent SHALL execute both SQL queries against `neo4j_family_graph.sqlite`:
```sql
SELECT target_file FROM edges WHERE repo = :repo AND source_file = :file ORDER BY target_file;
SELECT source_file FROM edges WHERE repo = :repo AND target_file = :file ORDER BY source_file;
```
**AND** SHALL record both result sets (including empty results) in the dossier.

### REQ-NFVL-005.0: Dossier schema guarantee
**WHEN** dossier is created for a selected row  
**THEN** exactly one dossier file SHALL exist at `dossier_path`  
**AND** it SHALL include all required headings:
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
**AND** SHALL include no duplicate dossier for same row in same batch.

### REQ-NFVL-006.0: Evidence discipline
**WHEN** writing non-trivial claims  
**THEN** claim labels SHALL be one of `Evidence`, `Inference`, or `Blocked`  
**AND** any `Evidence` claim SHALL include symbol/file/line or exact query evidence  
**AND** any `Inference` claim SHALL explain uncertainty and verification next step.

### REQ-NFVL-007.0: Oracle coverage per dossier
**WHEN** any dossier is completed  
**THEN** `## Verification Oracles` SHALL include at least one `WHEN / THEN / SHALL` contract  
**AND** SHALL reference a target behavior, expected success state, and failure state where useful.

### REQ-NFVL-008.0: Rewrite guidance
**WHEN** writing `## Rust Rewrite Notes`  
**THEN** the agent SHALL propose 2–6 concrete mapping candidates (trait/type/function/error)  
**AND** SHALL classify each with `L1`, `L2`, or `L3`.

### REQ-NFVL-009.0: Cross-file rollup
**WHEN** batch completes  
**THEN** `ROLLUP.md` SHALL be updated with:
- high-confidence decisions
- repeated patterns
- memory/structure risks
- blockers and open risks
- candidate verification suite changes
- explicit next 10 recommended files.

### REQ-NFVL-010.0: Journal traceability
**WHEN** batch starts and each blocker is encountered  
**THEN** `journals/gds-complete-read-batch1.md` SHALL be updated with timestamp, selected priorities, blockers, and completed/remaining list.

### REQ-NFVL-011.0: Prompt discipline for file reads
**WHEN** row has `lane` and `read_prompt` metadata  
**THEN** read task prompt SHALL include:
1. behavioral contract question
2. storage/memory implications question
3. failure and compatibility boundary question  
with these sections serialized into `## Coding Prompt Unlocked`.

### REQ-NFVL-012.0: Completion gates
**WHEN** goal is reported as done  
**THEN** the run SHALL satisfy:
- manifest count equals attempted selections
- dossier uniqueness and heading completeness
- blocked rows logged with exact reason
- at least one oracle per non-blocked dossier
- `git diff --check` returns zero issues.

## 6) Test Matrix

| req_id | test_id | type | assertion | target |
| --- | --- | --- | --- | --- |
| REQ-NFVL-001.0 | TEST-NFVL-001 | preflight | queue/db/root are readable | queue, sqlite, source root |
| REQ-NFVL-002.0 | TEST-NFVL-002 | selection | manifest selects first missing/incomplete rows by ascending priority | queue validator |
| REQ-NFVL-002.0 | TEST-NFVL-003 | selection | batch size <= `BATCH_LIMIT` | manifest validator |
| REQ-NFVL-003.0 | TEST-NFVL-004 | file-read | selected files resolve and open | filesystem |
| REQ-NFVL-004.0 | TEST-NFVL-005 | sql | both dependency queries execute | sqlite |
| REQ-NFVL-005.0 | TEST-NFVL-006 | schema | each selected row has exactly one dossier | dossier output |
| REQ-NFVL-005.0 | TEST-NFVL-007 | schema | required headings exist | dossier parser |
| REQ-NFVL-006.0 | TEST-NFVL-008 | evidence | claims include Evidence/Inference/Blocked labels | dossier content |
| REQ-NFVL-007.0 | TEST-NFVL-009 | oracles | each dossier has WHEN/THEN/SHALL contract | dossier parser |
| REQ-NFVL-008.0 | TEST-NFVL-010 | rewrite | L1/L2/L3 tags appear in Rust section | dossier parser |
| REQ-NFVL-009.0 | TEST-NFVL-011 | rollup | rollup includes required cross-file blocks + next 10 files | rollup parser |
| REQ-NFVL-010.0 | TEST-NFVL-012 | journal | batch start + blocker events logged | journal |
| REQ-NFVL-012.0 | TEST-NFVL-013 | quality | `git diff --check` clean | git |

## 7) TDD Plan

### STUB
- Define required headings, prompt format, and validator script checks.
- Draft empty manifesto with no source claims to confirm gating fails until files are filled.

### RED
- Run preflight and heading validator on empty/partial state.
- Confirm blocked rows are treated as non-fatal and do not block unrelated rows.

### GREEN
- Process manifest rows in strict order.
- For each row: full read, SQL neighborhood queries, dossier generation, oracle insertion, rewrite candidates.
- Append blockers to journal and continue through remaining rows.

### REFACTOR
- Normalize claim labeling across dossiers.
- Consolidate recurring observations into rollup language.
- De-duplicate repeated facts.

### VERIFY
- Run completion script, rollup consistency checks, and `git diff --check`.
- Report a go/no-go state only when all gates pass.

## 8) Prompt Library (for `## Coding Prompt Unlocked`)

### Template A: Contract + compatibility (default)
```text
Read the full file and produce: 
1) the externally visible contract (procedures, public types, config entries, error cases),
2) what must remain API-compatible for Neo4j-facing behavior,
3) exact compatibility risks if omitted.
```

### Template B: Memory + storage (OLAP-first)
```text
Focus on storage and memory behavior:
1) what structures are allocated by ownership/state,
2) where lifecycle boundaries are enforced,
3) where exactness/approximation is configurable,
4) where memory estimation hooks guard execution.
```

### Template C: Verification-driven rewrite
```text
From this file, derive 1–3 concrete verification scenarios for Rust rewrite:
state transition, failure mode, and one negative/edge test.
```

### Template D: Failure-mode hardening
```text
Identify what exceptions/errors are raised, preconditions asserted, and boundaries that can fail:
input validation, catalog misses, missing graph, incompatible config, overflow, nullability.
```

## 9) Validation Snippet (run at batch end)

```bash
python3 - <<'PY'
from pathlib import Path
import csv, sqlite3

ROOT = Path("/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker")
QUEUE = ROOT / "docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv"
DB = ROOT / "docs_PRD03/reference-learning/neo4j-family-dependency-graphs/neo4j_family_graph.sqlite"
ROLLUP = ROOT / "docs_PRD03/reference-learning/gds-v2-dossiers/ROLLUP.md"
required = [
    "## Source","## Why This File Matters","## Public Contract","## Internal Mechanics",
    "## Memory And Storage Implications","## Snapshot And Catalog Implications",
    "## Verification Oracles","## Rust Rewrite Notes","## Dependencies Read Next",
    "## Dependents As Tests","## Open Questions","## Coding Prompt Unlocked",
]

def read_queue(path):
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))

rows = read_queue(QUEUE)
selected = []

def has_headings(path: Path) -> bool:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return False
    return all(h in text for h in required)

for row in sorted(rows, key=lambda r: int(r["priority"])):
    dossier = ROOT / row["dossier_path"]
    if not dossier.exists() or not has_headings(dossier):
        selected.append(row)
    if len(selected) >= 10:
        break

print("selected_priorities:", [r["priority"] for r in selected])

for row in selected:
    dossier = ROOT / row["dossier_path"]
    if dossier.exists() and not has_headings(dossier):
        raise SystemExit(f"Invalid headings in {dossier}")

conn = sqlite3.connect(DB)
cur = conn.cursor()
for row in selected:
    repo = row["repo"]
    file = row["file"]
    cur.execute("SELECT target_file FROM edges WHERE repo=? AND source_file=? ORDER BY target_file", (repo, file))
    cur.fetchall()
    cur.execute("SELECT source_file FROM edges WHERE repo=? AND target_file=? ORDER BY source_file", (repo, file))
    cur.fetchall()

if not ROLLUP.exists():
    raise SystemExit("Rollup file missing")
print("VALIDATION_OK")
PY

git diff --check
```

## 10) Quality Gates

- [ ] Scope files are readable and not empty.
- [ ] Selection is deterministic by ascending priority.
- [ ] All required dossier headings are present.
- [ ] Blockers are explicit and non-silent.
- [ ] Every dossier has at least one oracle contract.
- [ ] `Memory And Storage` and `Snapshot And Catalog` sections include evidence or explicit inference.
- [ ] Rollup has next-10 recommendation and blocker tracking.
- [ ] Journal includes batch start and each blocker event.
- [ ] `git diff --check` passes.

## 11) Open Questions

1. Should `BATCH_LIMIT` remain fixed or adapt to remaining time/file complexity?
2. Should blockers reduce batch size, or still count as processed?
3. Should oracles include expected micro-benchmark thresholds from first-party scripts?
4. Should failed SQL queries be fatal globally or row-local with continuation?
5. Should low-yield/unsupported claims be tagged as `Blocked` or `Inference`?

## 12) Copy-Paste Goal

```text
/goal Use the spec at /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/neo4j-family-verification-loop-executable-spec-v4.md as the hard constraints.

Scope is only:
- /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/gds-complete-read-queue.tsv
- /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/reference-learning/neo4j-family-dependency-graphs/neo4j_family_graph.sqlite
- /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/Neo4j family

Build a bounded next batch of dossiers from the queue using BATCH_LIMIT=10 in priority order, choosing rows missing or incomplete against this schema:
`## Source`, `## Why This File Matters`, `## Public Contract`, `## Internal Mechanics`,
`## Memory And Storage Implications`, `## Snapshot And Catalog Implications`,
`## Verification Oracles`, `## Rust Rewrite Notes`, `## Dependencies Read Next`,
`## Dependents As Tests`, `## Open Questions`, `## Coding Prompt Unlocked`.

For each row:
- read complete source file,
- run direct inbound/outbound SQL neighborhood queries,
- write one dossier with Evidence/Inference labels and at least one WHEN/THEN/SHALL oracle,
- append blockers in journals/gds-complete-read-batch1.md and continue.

After the batch, update ROLLUP.md with cross-file findings and next 10 file recommendations.
End only when heading checks and `git diff --check` are clean.
```

