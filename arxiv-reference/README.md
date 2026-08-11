# arXiv Pattern Foundry

## Operating Contract

The foundry is a bounded research-to-decision system for Knight Bus. Later
goals may turn repository questions and external literature into normalized
mechanism cards, failure cards, constraint-transfer cards, architecture
candidates, and falsifying experiments. Every artifact must remain traceable
to one goal packet and to the architecture decision it can change.

The foundry is not:

- a PDF archive;
- a paper-summary collection;
- an authorization to browse, download, or read papers outside the active goal;
- an architecture generator without evidence and falsification boundaries;
- a production algorithm implementation program; or
- a replacement for customer discovery or the A007 product contract.

Only one campaign goal may be active at a time. A goal must stop at its declared
outputs and caps. Starting the recommended next goal always requires explicit
authorization.

## G00 Empty-Corpus State

G00 initializes one minimum scaffold. The campaign is intentionally empty:
there are no external queries, paper records, downloaded papers, evidence
cards, architecture candidates, or experiments. Governance files are scaffold
artifacts, not research artifacts. Exact research counts live in
`governance/campaign-status.md`.

An empty corpus is valid only when the scaffold contract can distinguish an
intentional zero-row corpus from a missing or malformed required artifact.
G00 is complete: its integrated exit criteria pass for the zero-research
scaffold. This says nothing about the quality of literature, evidence cards,
architecture candidates, or product performance because G00 created none.

The final campaign state, zero decision-yield counts, verification evidence,
and G01 boundary are recorded in `governance/campaign-status.md` and
`journals/G00-progress.md`. G01 remains `NOT_STARTED`.

The G00 requirement closure matrix lives in
`governance/G00-goal-packet.md`. It separates requirements that apply to the
zero-corpus scaffold from record-level behavior owned by later goals. The SOP's
49 one-to-one REQ-to-TEST links are traceability metadata; G00 does not claim
that all 49 behavioral tests are implemented.

No G00 artifact contains a literature claim or invention. Later goals must use
the epistemic labels as follows:

| Label | Operational meaning |
|---|---|
| `SOURCE_CLAIM` | A claim made by an identified source. It requires a precise source pointer. |
| `DERIVED_INFERENCE` | A consequence derived from identified evidence and explicit assumptions. It is not a quoted source result. |
| `SPECULATIVE_TRANSFER` | A proposed mechanism transfer or invention. It is not published evidence and requires a falsifier. |

## A007 Product Filter

The product north star is
`docs_PRD04/A007-spc-founder-interview-prep-v7.md`:

> Knight Bus should make a resource estimate enforceable through admission, a
> bounded plan, execution, verification, and a receipt.

The exact A007 product uncertainty is whether a stricter contract - a hard
budget, bounded execution, and a receipt - changes behavior for a narrow
security/dependency/access-path segment. G00 enables later evidence collection
for that question, records `NO_DECISION_IMPACT`, and does not answer the product
question.

Every later research item must name the A007 uncertainty it reduces and must do
at least one of the following:

1. change or constrain an architecture decision;
2. expose a failure boundary;
3. improve an estimator or symbolic resource consequence; or
4. create a falsifying experiment.

Work that only increases general knowledge about graph systems is background
material, not completed campaign progress. Estimates, modeled expectations,
and speculative transfers must never be presented as measured product results.

## Goal Lifecycle

Each goal follows the same bounded lifecycle:

1. **STUB:** Read the SOP and governing inputs, initialize or resume the one
   journal, declare exact outputs and caps, and record expected failures.
2. **RED:** Run goal-specific validators, confirm that failures identify real
   contract gaps, and preserve the exact failures without weakening tests.
3. **GREEN:** Produce only the minimum complete batch owned by the active goal
   and validate each meaningful batch.
4. **REFACTOR:** Normalize IDs, terms, links, confidence, and resource symbols
   while preserving variants, rejected evidence, and lineage.
5. **VERIFY:** Run goal tests, the full validator, Git and license gates, and cap
   checks; write the handoff; then stop before the next goal.

The active journal is the resumable source of phase, failures, work in motion,
and next steps. Campaign-wide state and exact counts live in
`governance/campaign-status.md`.

## Goal And Directory Ownership

Later goals create or update only the outputs named by their goal packets.
Directories in the SOP repository contract are targets, not permission for G00
to pre-populate later research artifacts.

| Goal | Owned directory surface and outputs |
|---|---|
| `G00` | Root scaffold, governance/schema contracts including `governance/G00-generation-ledger.md`, ignore policy, validator, tests, and `journals/G00-progress.md`. |
| `G01` | `governance/architecture-question-ledger.md`, `governance/keyword-taxonomy.tsv`, and decision-linked planned query families. |
| `G02` | Executed `governance/query-ledger.tsv` rows and `sources/paper-manifest.tsv`. |
| `G03` | `sources/citation-edges.tsv` and citation-driven updates to the paper manifest. |
| `G04` | `sources/download-ledger.tsv`, ignored local full text under `sources/papers/`, and goal-declared extraction cache outputs. |
| `G05` | `evidence/mechanism-cards/` and goal-declared pattern relationships or retrieval updates. |
| `G06` | `evidence/failure-cards/` and `evidence/evidence-conflicts.tsv`. |
| `G07` | `evidence/constraint-transfer-cards/`. |
| `G08` | `synthesis/architecture-genomes/`, `synthesis/architecture-candidates/`, and `synthesis/pareto-archive.tsv`. |
| `G09` | `synthesis/architecture-decision-atlas.md` and `synthesis/experiment-backlog.md`. |
| `G10` | One campaign audit: coverage report, decision delta, next-campaign proposal, and closure updates explicitly named by its packet. |

`retrieval/` and `prompts/` are later-goal target surfaces. They remain absent
until a later goal packet explicitly names the exact paths it owns. Each goal
owns only its journal under `journals/`; all goals may update campaign status as
part of an authorized handoff.

## Canonical Validation Commands

Run these commands from the repository root after all G00 lanes are integrated:

```bash
python3 -m unittest discover -s arxiv-reference/tests -p 'test_*.py'
python3 arxiv-reference/tools/validate_arxiv_corpus_contract.py --root arxiv-reference
git diff --check
git ls-files --others --exclude-standard -z -- arxiv-reference |
while IFS= read -r -d '' g00_file; do
  findings=$(git diff --no-index --check -- /dev/null "$g00_file" 2>&1)
  diff_status=$?
  if [ "$diff_status" -gt 1 ] || [ -n "$findings" ]; then
    printf '%s\n' "$findings"
    exit 1
  fi
done
git ls-files -- ':(glob)arxiv-reference/sources/papers/**/*.pdf'
git diff --cached --name-only --diff-filter=ACMR -- ':(glob)arxiv-reference/sources/papers/**/*.pdf'
git status --short
```

`git diff --check` covers tracked working-tree changes. The per-file
`git diff --no-index --check` loop covers untracked G00 files read-only and does
not stage or otherwise alter the user's working tree. The two PDF commands must
produce no paths. `git status --short` must be audited against the active goal's
owned outputs and pre-existing user changes. Command presence here is not a
claim about current results.
