# G07 Goal Packet: Constraint Time Machine

## Mission

G07 translates a frozen set of proven G05 mechanisms into rigorously bounded
Knight Bus transfer possibilities. It does **not** select an architecture.

The north star is
`docs_PRD04/A007-spc-founder-interview-prep-v7.md`: a user declares a hard
budget, Knight Bus estimates the total working set, chooses
fit/spill/approximate/refuse before execution, and returns a verification
receipt afterward.

- Goal ID: G07
- Objective: translate exactly 20 decision-relevant G05 mechanisms into rigorously specified Knight Bus transfer possibilities without selecting a final architecture.
- A007 uncertainty reduced: which mechanism invariants survive modern hardware and can become enforceable algorithm-specific admission, storage, execution, or receipt terms after every G06 failure is applied?
- Inputs: the completed and cleared G05/G06 corpus, exactly 67 mechanism cards and 79 failure cards, the G05 and G06 reports and reviews, the campaign SOP and evidence contracts, current Knight Bus implementation anchors, and `docs_PRD04/A007-spc-founder-interview-prep-v7.md`.
- Owned outputs: this packet, `governance/g07-constraint-transfer-contract.md`, `governance/g07-transfer-plan.tsv`, at most 20 constraint-transfer cards, `sources/G07-constraint-transfer-report.md`, `governance/reviews/G07-adversarial-review.md`, `journals/G07-progress.md`, G07 pipeline and tests, shared-validator extensions, campaign status, README, and Markdown-index updates.
- Batch caps: exactly 20 frozen mechanisms, four disjoint reader lanes of five, at most 20 canonical transfers, one independent non-author reviewer, zero external requests, zero new papers or repositories, and no explicit token cap.
- Excluded work: new research retrieval, downloads, repository acquisition, G08 architecture candidates, G09 experiments or benchmarks, Knight Bus implementation, product performance claims, commit, push, and starting G08.
- Entry tests: G06 is complete, verified, and cleared; all 92 plan rows are reviewer/checksum-bound; 67 mechanisms and 79 failures validate; G06 review has P0=P1=P2=0; the shared corpus validator passes; no G07 semantic artifact exists.
- Exit tests: all 20 patterns have exactly one terminal disposition; every emitted transfer has an invariant, recalculated five-part symbolic resource model, complete G06 challenge coverage, unknown constants, and a smallest falsifier; independent review is cleared with P0=P1=P2=0; all G00-G07 tests and the shared validator pass.
- Stop conditions: frozen-input drift, metaphorical analogy without an invariant, imported historical ratio, missing G06 challenge, unsupported source pointer, hidden numeric assumption, later-goal artifact, external request, or unresolved P0/P1/P2 review finding.
- Journal: `arxiv-reference/journals/G07-progress.md`.

## Entry Gate

G07 began only after a fresh local verification established all of the
following:

- G06 is `COMPLETE_VERIFIED_CLEARED`.
- `arxiv-reference/tools/validate_arxiv_corpus_contract.py` reports `PASS`.
- G06's independent review records zero P0, P1, and P2 findings.
- All 92 G06 plan rows have a reviewer and a 64-character result checksum.
- All 67 G05 mechanism cards have a terminal G06 disposition.
- The corpus contains 79 retained failure cards; contradictory evidence was
  not erased.
- No G07 cards, plan, report, or review existed at entry.

## Current Knight Bus Grounding

The transfer exercise is anchored in the implementation rather than an
imagined blank slate:

- `src/bolt.rs::KnightBusBoltBackend::execute` already produces a narrow
  neighborhood-walk receipt, but marks `resource_high_water_status` as
  `unavailable`.
- `src/gds/execution.rs::memory_estimate_detail_map_now` already separates
  topology references, sidecars, catalog metadata, heap, page cache, direct I/O
  buffers, algorithm state, overlays, and scratch.
- `src/low_ram.rs::build_snapshot_from_paths_low_ram` already uses bounded
  external runs and records phase RSS peaks for snapshot construction.

G07 therefore concentrates on the missing bridge between these seeds:
algorithm-specific admission terms, bounded execution choices, modern storage
and scheduling alternatives, and explicit falsifiers.

## Frozen Selection

Exactly 20 G05 mechanisms are frozen before semantic transfer work. The four
lanes are disjoint and contain exactly five mechanisms each.

### Lane 1: Bound Live State

1. `PAT-BOUND-SEARCH-CANDIDATE-FRONTIER`
2. `PAT-DECOMPOSE-DIFFUSION-INTO-STAGES`
3. `PAT-PRUNE-SETTLED-SEARCH-STATE`
4. `PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH`
5. `PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY`

### Lane 2: Shape Stored State

1. `PAT-COMPRESS-SORTED-ID-STREAMS`
2. `PAT-INLINE-LOW-DEGREE-ADJACENCIES`
3. `PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS`
4. `PAT-PACK-NONEMPTY-SPARSE-ROWS`
5. `PAT-INFER-UNWEIGHTED-EDGE-VALUES`

### Lane 3: Schedule Bounded Work

1. `PAT-PARTITION-UPDATES-BY-DESTINATION`
2. `PAT-PIPELINE-ASYNC-IO-COMPUTE`
3. `PAT-PRIORITIZE-RESIDENT-ACTIVE-BLOCKS`
4. `PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS`
5. `PAT-SELECT-PARTITION-SCATTER-MODE`

### Lane 4: Preserve Useful Exactness

1. `PAT-EXPLORE-SUPERSET-VERIFY-RESULTS`
2. `PAT-NAVIGATE-BINARY-RERANK-EXACTLY`
3. `PAT-STREAM-SPARSE-KEEP-DENSE`
4. `PAT-REFINE-COMMUNITIES-BEFORE-AGGREGATION`
5. `PAT-BALANCE-BUCKETED-PACKED-SETS`

This set covers BFS/WCC, PageRank and local diffusion, node similarity/kNN,
Louvain/Leiden, triangle-style set intersection, and sparse-matrix or embedding
work. It also includes non-graph source domains such as compact set
representations and sparse linear algebra.

## Selection Rubric

Selection is a decision-priority judgment, not a performance measurement. Each
row in `g07-transfer-plan.tsv` is scored from five explicit components:

| Component | Points | Question |
|---|---:|---|
| A007 contract leverage | 30 | Can it change admission, a bound, a plan branch, or a receipt term? |
| Priority algorithm coverage | 25 | Does it cover an A007 algorithm family or a reusable kernel? |
| Transfer and invariant clarity | 20 | Is the mechanism operational enough to transplant and falsify? |
| G06 challenge value | 15 | Is there retained negative evidence that can constrain the transfer? |
| Domain distance and diversity | 10 | Does it expand the option vocabulary beyond one graph-system lineage? |

The score ranks reading attention only. It is not a claim that a mechanism will
work or outperform Neo4j/GDS.

## Required Work

1. Four reader lanes inspect their five mechanism cards and every linked G06
   failure card.
2. Integration normalizes surviving proposals into at most 20 canonical
   constraint-transfer cards.
3. Each selected mechanism receives exactly one terminal disposition.
4. Every surviving transfer states the original constraint profile, surviving
   invariant, reversed assumptions, modern costs, symbolic resource model,
   unknown constants, target algorithms, analogy failures, and smallest
   falsifier.
5. One independent reviewer who authored no card challenges the complete set.
6. The report answers the eight G07 decision questions and recommends G08 only
   after review clearance.

## Hard Boundaries

- No network calls, new papers, downloads, repositories, or implementation-code
  research beyond the current repository.
- No G08 architecture candidates or winner selection.
- No G09 benchmark execution or invented measurements.
- No product or performance claim may be upgraded from speculative to measured.
- No commit or push unless separately authorized.

## Completion Evidence

G07 can close only when:

- exactly 20 plan rows are terminal and checksum-backed;
- the four lanes remain disjoint with five mechanisms each;
- every selected mechanism has exactly one allowed disposition;
- every emitted transfer card passes the frozen schema;
- the independent review has P0 = P1 = P2 = 0;
- the shared corpus validator passes;
- campaign status, README, Markdown index, journal, report, and review agree;
- G07 is marked `COMPLETE_VERIFIED_CLEARED`, recommends G08, and stops.
