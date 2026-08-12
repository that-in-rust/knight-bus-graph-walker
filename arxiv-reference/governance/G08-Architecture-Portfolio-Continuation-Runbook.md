# G08 Architecture Portfolio Continuation Runbook

## Executive Decision

G08 is **well specified but materially incomplete**. The next task should not
repeat research, codebase orientation, schema design, or test scaffolding. It
should convert the frozen G07 transfers into the actual architecture portfolio.

The recommended path is **Timeline A: Staged Exact G08**:

1. generate 48 concise, genuinely distinct, G06-blind candidate genomes;
2. combine them with the two existing draft baselines;
3. freeze all 50 raw candidates and their provenance;
4. normalize all 50 into the existing canonical contract;
5. run one post-freeze G06 challenge pass;
6. retain 12-18 candidates across explicit Pareto niches;
7. hand 3-8 falsifying experiments to G09; and
8. close only after one independent review and the existing closure checks.

This is the best balance of rigor and remaining-token efficiency. It preserves
the exact G08 contract while reserving expensive prose and adversarial reasoning
for the decisions that matter.

---

## Decision Frame

### The actual decision

The choice is not “which architecture wins?” G08 has a prior decision:

> How should we finish a diverse, evidence-traceable portfolio without wasting
> context on repeated scaffolding or prematurely collapsing to one design?

### North star

The governing product contract remains
`docs_PRD04/A007-spc-founder-interview-prep-v7.md`:

> A user declares a hard resource budget. Knight Bus chooses fit, lower
> concurrency, stream, spill, approximate, reference, or refuse before
> execution; enforces the selected plan; verifies the result; and emits a
> resource receipt.

Architecture novelty that cannot change admission, a bounded plan, a fallback,
verification, or a receipt is not decision progress.

### Hard constraints

- Preserve exactly 50 raw candidates.
- Preserve six niches of eight candidates plus two explicit baselines.
- Do not load G06 failure cards during divergent generation.
- Do not mutate G05-G07 semantic evidence.
- Do not add papers, repositories, web research, implementation, or benchmarks.
- Do not invent numeric RAM or latency improvements.
- Do not start G09.
- Do not recommend a universal graph representation.
- Do not commit or push without separate authorization.

### What counts as a win

G08 wins when it produces a small family of algorithm-layout capsules that:

- covers the dependency/security/access-path first wedge;
- covers BFS, PageRank, WCC, Louvain/Leiden, and node similarity/kNN;
- exposes honest RAM/latency/preparation/adoption trade-offs;
- can admit, switch, spill, reference, or refuse before execution;
- preserves exactness and compatibility boundaries;
- identifies 3-8 cheap falsifiers for G09; and
- makes the first implementation choice substantially easier.

---

## Timeline A: Staged Exact G08

**Recommendation: choose this timeline.**

### Starting action

Generate compact candidate genomes in six isolated lane tasks. Each task emits
eight candidates with candidate-specific decisions, not full repeated boilerplate.
A later controller normalizes those genomes into the complete card schema.

### First work block: divergence

1. Run one bounded task per niche.
2. Give each lane A007 and the 20 G07 transfer cards.
3. Explicitly prohibit G06/failure-card reading.
4. Save one lane JSON containing exactly eight concise genomes.
5. Do not ask the lane to write final report prose or judge candidates.

Expected result: 48 candidate genomes plus the two existing baseline drafts.

### Second work block: raw freeze

1. Confirm IDs `ARCH-G08-001` through `ARCH-G08-050` exist exactly once.
2. Confirm niche counts are `8 + 8 + 8 + 8 + 8 + 8 + 2`.
3. Confirm all seven workload families are covered.
4. Record lane identity, prompt identity, timestamps, and raw SHA-256 hashes.
5. Create one raw-portfolio freeze manifest.

Only after this byte freeze may any worker read G06 failure cards.

### Third work block: normalization

Normalize each genome into one canonical Markdown-plus-JSON card. Repeated
mechanical accounting can use a shared template, but the following fields must
remain candidate-specific:

- target workload and semantics;
- inherited transfers and composition logic;
- topology and prepared variants;
- state placement and multiplicity;
- scheduling and concurrency;
- minimum resident kernel;
- complete peak-RAM equation and crossover guards;
- preparation, persistent storage, and old/new coexistence;
- fallback ladder;
- correctness, determinism, and oracle;
- Neo4j/Cypher/Bolt/GDS boundary;
- conditions where the candidate loses; and
- smallest G09 falsifier.

Expected result: 50 canonical cards, still without terminal dispositions.

### Fourth work block: challenge

Open G06 only now. For every candidate:

1. select failures linked through inherited G07 transfers;
2. add relevant algorithm-family failures not linked mechanically;
3. record whether each failure applies;
4. add a guard, fallback, narrowed contract, calibration dependency, merger, or
   rejection;
5. update the symbolic model when the repair adds state or I/O; and
6. assign exactly one terminal disposition.

Expected result: all 50 candidates remain preserved and all 50 are terminal.

### Fifth work block: Pareto and G09 handoff

Retain 12-18 candidates across the ten mandated axes. Use
`NON_COMPARABLE` when unknown constants prevent dominance. Do not rank with one
score.

From the survivors, select only 3-8 G09 experiments. Prefer experiments that
collapse a shared uncertainty across several candidates, such as:

- mmap/page-cache/RSS accounting calibration;
- frontier spill crossover for exact access paths;
- compressed-ID decode versus bytes avoided;
- resident-block reuse versus fairness/tail risk;
- sparse-stream/dense-state minimum kernel for PageRank;
- destination-log memory amplification;
- compact-navigation candidate recall plus exact rerank; and
- snapshot/reference correctness and receipt calibration.

### Sixth work block: independent review and closure

One non-author reviewer inspects all 50 cards, not just survivors. Repair P0,
P1, and P2 findings, then run the existing focused and closure validators once.

### Likely lived experience

- **Attention cost:** moderate and controlled because each task owns one lane or
  one phase.
- **Stress:** low to moderate; progress is durable after each lane.
- **Risk of context loss:** low because all boundaries and candidate IDs are
  preassigned.
- **Risk of shallow output:** moderate during genome generation, recovered by
  focused normalization and challenge.
- **Reversibility:** high; every raw candidate and rejection remains preserved.

### Rough effort

- 6 lane tasks.
- 1 freeze/normalization task, possibly split into two batches of 25.
- 1 challenge task, possibly split by algorithm family.
- 1 synthesis/report task.
- 1 independent-review and closure task.
- Approximately 9-12 bounded Codex tasks.

Estimated total model consumption: roughly 350k-800k tokens, depending on card
verbosity and whether challenge work is split. Confidence in this range: 55%.

---

## Timeline B: Quality-Maximized Exact G08

### Starting action

Assign a separate author and critic to each niche, then have a synthesis agent
recombine only after lane-level critique.

### How it unfolds

1. Six authors generate eight candidates each without G06.
2. Six niche critics inspect diversity and composition without rejecting on
   external failures.
3. Authors repair duplicate or incoherent candidates while G06 remains hidden.
4. The controller freezes 50 raw candidates.
5. Algorithm-family challengers apply G06 independently.
6. A Pareto synthesizer reconciles cross-family trade-offs.
7. A final independent reviewer clears the complete portfolio.

### Upside

- Strongest candidate diversity and compositional reasoning.
- Lower risk that a single controller's vocabulary dominates all 50 designs.
- Better-quality rejected-candidate memory.
- More defensible architecture report.

### Downside

- Repeated context loading of the same 20 transfer cards.
- Coordination and provenance overhead can exceed the value of the extra
  critique.
- Previous in-task subagent attempts stalled and produced no usable lane files;
  this timeline should therefore use separate durable Codex tasks, not another
  large subagent fan-out.

### Likely lived experience

- **Attention cost:** high.
- **Stress:** moderate because many outputs need reconciliation.
- **Risk of context loss:** moderate across many tasks.
- **Risk of shallow output:** lowest.
- **Reversibility:** high.

### Rough effort

Approximately 15-22 bounded tasks and 800k-1.8M model tokens. Confidence: 45%.

Choose this only if architecture research quality is more important than
reaching the first falsifying implementation quickly.

---

## Timeline C: Product-First Contract Amendment

### Starting action

Explicitly amend G08 from 50 full candidates to approximately 18 candidates:
two baselines, two candidates for each priority algorithm family, and two
cross-family compositions.

### How it unfolds

1. Write an authorized contract amendment; do not silently narrow the goal.
2. Generate approximately 18 deep candidates.
3. Apply G06 immediately after the smaller raw freeze.
4. Retain approximately 8-12 survivors.
5. Select 3-5 G09 falsifiers centered on the access-path wedge and PageRank.
6. Begin measured verification sooner.

### Upside

- Fastest route to a working verification loop.
- More tokens per candidate.
- Less architecture-documentation inventory.
- Better aligned with the product truth that evidence comes from measurements,
  not the number of generated designs.

### Downside

- Does **not** satisfy the current exact G08 contract.
- Shrinks exploration before seeing the full niche surface.
- Higher regret risk if the omitted design family held the best bounded-RAM
  idea.
- Requires explicit user authorization and status/document amendments.

### Likely lived experience

- **Attention cost:** lowest.
- **Stress:** lowest after the amendment, but there may be discomfort about
  abandoning the original breadth commitment.
- **Risk of context loss:** low.
- **Risk of shallow output:** low for retained candidates, high for unexplored
  space.
- **Reversibility:** medium; omitted lanes can be reopened later.

### Rough effort

Approximately 4-7 tasks and 180k-450k model tokens. Confidence: 60%.

This is strategically reasonable, but it is not the current authorized goal.

---

## Cross-Timeline Analysis

| Dimension | Timeline A: staged exact | Timeline B: quality-max exact | Timeline C: amended product-first |
|---|---|---|---|
| Satisfies current G08 | Yes | Yes | No, requires amendment |
| Time to G09 | Medium | Slowest | Fastest |
| Candidate diversity | High | Highest | Medium |
| Reasoning depth per candidate | Medium, deepens on survivors | Highest | High |
| Token exposure | Medium | Highest | Lowest |
| Coordination risk | Low-medium | High | Low |
| Risk of premature convergence | Low | Lowest | Highest |
| Verification-first alignment | Good | Delayed | Best |
| Reversibility | High | High | Medium |
| Recommended now | **Yes** | No | Only with explicit amendment |

### Shared inflection points

All timelines should stop and reconsider at these moments:

1. **After raw divergence:** Are the candidates materially different, or merely
   renamed combinations of the same transfers?
2. **After G06 challenge:** Did more than half collapse for one shared reason?
   If yes, that shared reason may deserve the first G09 experiment.
3. **After Pareto placement:** Are there more than eight distinct uncertainty
   classes? If no, choose one experiment per class rather than one per candidate.
4. **After first G09 measurements:** Does one calibrated constant dominate the
   estimated advantage? If yes, stop architecture proliferation and improve
   the estimator or fallback policy.

### Regret analysis

- The largest regret in Timeline A is spending time normalizing dominated
  candidates. Mitigation: keep raw genomes concise and normalize mechanically.
- The largest regret in Timeline B is delaying reality while improving paper
  architectures. Mitigation: do not choose it under tight usage limits.
- The largest regret in Timeline C is missing a design with a genuinely
  different RAM-bound mechanism. Mitigation: preserve the six-niche seed map
  and reopen only the missing lane if G09 results disappoint.

---

## Decision Filter

Choose **Timeline A** unless one of these conditions is true:

- Choose Timeline B only when there is enough model budget for at least six
  durable author tasks plus six critique tasks and no pressure to begin G09.
- Choose Timeline C only when the user explicitly prioritizes first measured
  proof over compliance with the current 50-candidate contract.

The immediate next action under Timeline A is:

> Generate `ARCH-G08-001` through `ARCH-G08-008` in the lowest-RAM lane as one
> concise G06-blind lane dossier. Do not revisit setup, code graphs, tests, or
> G07 selection.

---

## Authoritative Resume State

### Repository state at handoff

- Branch: `ideation_20260525`.
- HEAD: `a234891` (`origin/ideation_20260525` at the same commit).
- Worktree was clean immediately before this runbook was created.
- This runbook is the only intended new file from this handoff turn.

### Trustworthy completed work

| Item | Evidence | Status |
|---|---|---|
| G07 entry gate | `campaign-status.md`, G07 report/review/plan | Complete and cleared |
| G08 goal packet | `governance/G08-goal-packet.md` | Complete |
| G08 executable contract | `governance/g08-architecture-evolution-contract.md` | Complete |
| Candidate plan shell | `governance/g08-candidate-plan.tsv` | 50 placeholder rows only |
| Focused validator | `tools/g08_architecture_evolution_pipeline.py` | Implemented |
| Focused tests | `tests/test_validate_g08_architecture_contract.py` | 5 passing |
| Current-code grounding | G08 packet and journal | Sufficient for generation |
| Draft baselines | `synthesis/g08-generation-lanes/G08-lane-base.json` | 2 raw drafts |

### Incomplete or misleading state

| Item | Current truth | Required truth |
|---|---|---|
| Canonical candidate cards | 0 | Exactly 50 |
| Non-baseline raw candidates | 0 | Exactly 48 |
| Raw portfolio freeze | Missing | Hash/provenance for all 50 |
| G06 challenge | Not started | Every candidate challenged post-freeze |
| Terminal dispositions | 0 real dispositions | Exactly 50 |
| Pareto archive | Missing | 12-18 survivors |
| Final report | Missing | Answers all 10 G08 questions |
| Independent review | Missing | P0=P1=P2=0 |
| Navigation/status closure | Not done | README/index/status/journal agree |
| G09 shortlist | Missing | 3-8 reserved falsifiers |

### Important warning

`g08-candidate-plan.tsv` currently labels all rows `RAW_GENERATED`, but those
rows are placeholders with `UNASSIGNED` authors and blank lineage, failures,
dispositions, and checksums. They are **not evidence that 50 candidates exist**.

Likewise:

```bash
python3 arxiv-reference/tools/g08_architecture_evolution_pipeline.py --repo-root .
```

passes only the active/non-closure mode. It does not prove G08 completion. The
authoritative completion command is:

```bash
python3 arxiv-reference/tools/g08_architecture_evolution_pipeline.py \
  --repo-root . --closure
```

That closure command is expected to fail until the portfolio, Pareto archive,
review, checksums, and reports exist.

### Prior execution problem

Two rounds of six in-task generation agents were attempted. All remained
running without writing usable lane files and were shut down. Do not represent
those attempts as independent generation evidence. Prefer one durable Codex
task per lane, or controller generation with explicit disclosure if task-level
parallelism is unavailable.

### Code-graph conclusion

No further code-graph exploration is needed for G08. Codebase Memory already
confirmed the relevant implementation anchors. CodeGraphContext built a
partial 78 MB database and found the same named anchors before its indexer hit
`NoneType.split`. This limitation is recorded; rerunning it does not unblock
the architecture portfolio.

---

## Frozen Candidate Seed Map

This map prevents the next lane authors from producing eight synonyms. It is a
starting vocabulary, not a decision or final card content.

### Lowest RAM: ARCH-G08-001..008

| ID | Workload | Distinct architecture seed | Principal G07 transfers |
|---|---|---|---|
| 001 | Dependency/access paths | external frontier runs over forward-only blocks | `XFER-CAP-STREAMING-TRAVERSAL-SKETCH`, `XFER-PRUNE-FINALIZED-TRAVERSAL-STATE` |
| 002 | BFS/reachability | scan-bounded semi-external traversal with refusal on long-path envelope | `XFER-CAP-STREAMING-TRAVERSAL-SKETCH`, `XFER-BOUND-PHASE-WORK-SCHEDULING` |
| 003 | BFS/WCC | delta/XOR compressed sorted ID streams with plain-stream crossover | `XFER-GUARD-COMPRESSED-ID-STREAMS` |
| 004 | Unweighted paths | topology with derivable edge-value elision | `XFER-ELIDE-DERIVABLE-EDGE-VALUES` |
| 005 | PageRank | destination-partitioned bounded update logs | `XFER-BOUND-DESTINATION-UPDATE-LOGS` |
| 006 | PageRank/SpMV | stream sparse topology while retaining one bounded dense state column | `XFER-STREAM-SPARSE-RETAIN-DENSE` |
| 007 | Node similarity/kNN | compact navigation blocks with vectors fetched only for survivors | `XFER-PROBE-SPLIT-NAVIGATION-VECTORS`, `XFER-NAVIGATE-COMPACT-RERANK-EXACTLY` |
| 008 | Louvain/Leiden | affected-community queue plus bounded fallback sweep | `XFER-REQUEUE-DEPENDENCY-AFFECTED-STATE`, `XFER-REFINE-COMMUNITIES-PRESERVE-CONNECTIVITY` |

### Lowest tail latency: ARCH-G08-009..016

| ID | Workload | Distinct architecture seed | Principal G07 transfers |
|---|---|---|---|
| 009 | Dependency/access paths | query-family hot projection resident in RAM | `XFER-GUARD-INLINED-ADJACENCY-THRESHOLD` |
| 010 | BFS | degree-banded inline low-degree adjacency with reference overflow | `XFER-GUARD-INLINED-ADJACENCY-THRESHOLD` |
| 011 | PageRank | resident active-block scheduling with bounded fairness | `XFER-CAP-RESIDENT-BLOCK-REUSE` |
| 012 | BFS/WCC | phase-specialized deterministic worker allocation | `XFER-BOUND-PHASE-WORK-SCHEDULING` |
| 013 | PageRank/WCC | bounded direct-I/O pipeline overlapping reads and compute | `XFER-BOUND-ASYNC-IO-PIPELINE` |
| 014 | PageRank/BFS | calibrated partition scatter mode with safe default | `XFER-CALIBRATE-PARTITION-SCATTER-MODE` |
| 015 | Vector kNN | resident compact navigation and exact rerank cache | `XFER-NAVIGATE-COMPACT-RERANK-EXACTLY` |
| 016 | Louvain/Leiden | prepared refined-community hierarchy with exact stop receipt | `XFER-REFINE-COMMUNITIES-PRESERVE-CONNECTIVITY` |

### Highest predictability: ARCH-G08-017..024

| ID | Workload | Distinct architecture seed | Principal G07 transfers |
|---|---|---|---|
| 017 | Vector kNN | hard candidate/frontier cap with recall contract and exact fallback | `XFER-BOUND-SEARCH-FRONTIER-STATE` |
| 018 | Access paths | fixed edge-slot traversal sketch and strict output cap | `XFER-CAP-STREAMING-TRAVERSAL-SKETCH` |
| 019 | PageRank | barriered staged diffusion with explicit per-stage state | `XFER-STAGE-LINEAR-DIFFUSION-STATE` |
| 020 | PageRank | fixed partition/update-log reservations | `XFER-BOUND-DESTINATION-UPDATE-LOGS` |
| 021 | SpMV/PageRank | one-complete-dense-column minimum resident kernel | `XFER-STREAM-SPARSE-RETAIN-DENSE` |
| 022 | WCC/BFS | level-synchronous bounded phase scheduler | `XFER-BOUND-PHASE-WORK-SCHEDULING` |
| 023 | Louvain/Leiden | full-cap affected queue plus deterministic fallback sweep | `XFER-REQUEUE-DEPENDENCY-AFFECTED-STATE` |
| 024 | Filtered paths/kNN | no-false-negative superset followed by exact verification | `XFER-VERIFY-SUPERSET-RESULTS-EXACTLY` |

### Lowest preparation/storage amplification: ARCH-G08-025..032

| ID | Workload | Distinct architecture seed | Principal G07 transfers |
|---|---|---|---|
| 025 | Access paths | forward-only unweighted artifact without reverse/value sidecars | `XFER-ELIDE-DERIVABLE-EDGE-VALUES` |
| 026 | BFS/WCC | retain plain shared streams unless compression sampling crosses over | `XFER-GUARD-COMPRESSED-ID-STREAMS` |
| 027 | BFS/PageRank | thresholded low-degree inlining without global reorder | `XFER-GUARD-INLINED-ADJACENCY-THRESHOLD` |
| 028 | Sparse linear algebra | query-time nonempty-row packing with reference reuse | `XFER-GUARD-SPARSE-ROW-PACKING` |
| 029 | Incremental graph jobs | immutable base plus bounded destination-log overlays | `XFER-BOUND-DESTINATION-UPDATE-LOGS` |
| 030 | PageRank | source-order streamed sparse blocks with no algorithm-specific topology copy | `XFER-STREAM-SPARSE-RETAIN-DENSE` |
| 031 | kNN | shared navigation base plus lazy vector sidecar | `XFER-PROBE-SPLIT-NAVIGATION-VECTORS` |
| 032 | Set intersections | on-demand packed adjacency sets with scalar fallback | `XFER-BALANCE-PACKED-ADJACENCY-SETS` |

### Lowest adoption friction: ARCH-G08-033..040

| ID | Workload | Distinct architecture seed | Principal G07 transfers |
|---|---|---|---|
| 033 | Access paths | Bolt/Cypher adapter over conservative snapshot traversal | `XFER-CAP-STREAMING-TRAVERSAL-SKETCH` |
| 034 | Access paths | narrow Cypher compiler choosing resident or streamed reference plan | `XFER-PRUNE-FINALIZED-TRAVERSAL-STATE` |
| 035 | Multi-algorithm | GDS estimate-first registry with explicit unsupported entries | `XFER-BOUND-PHASE-WORK-SCHEDULING` |
| 036 | PageRank | `gds.pageRank` facade over shared CSR plus sparse-stream option | `XFER-STREAM-SPARSE-RETAIN-DENSE` |
| 037 | WCC | GDS-compatible WCC over bounded streaming sketch | `XFER-CAP-STREAMING-TRAVERSAL-SKETCH` |
| 038 | Louvain/Leiden | GDS-compatible community procedure with refinement receipt | `XFER-REFINE-COMMUNITIES-PRESERVE-CONNECTIVITY` |
| 039 | Node similarity/kNN | GDS facade over compact navigation plus exact rerank | `XFER-NAVIGATE-COMPACT-RERANK-EXACTLY` |
| 040 | Multi-algorithm | Neo4j sidecar that falls back to Neo4j and returns a comparative receipt | `XFER-VERIFY-SUPERSET-RESULTS-EXACTLY` |

### Unconventional but composable: ARCH-G08-041..048

| ID | Workload | Distinct architecture seed | Principal G07 transfers |
|---|---|---|---|
| 041 | Multi-algorithm | algorithm cartridges sharing only ID map, manifest, and receipt protocol | multiple compatible transfers selected per cartridge |
| 042 | BFS/PageRank/WCC | memory-token scheduler where every partition and I/O buffer spends explicit budget tokens | `XFER-BOUND-PHASE-WORK-SCHEDULING`, `XFER-BOUND-ASYNC-IO-PIPELINE` |
| 043 | Access paths | reversible compressed adjacency tapes replayed under a scan-count guard | `XFER-GUARD-COMPRESSED-ID-STREAMS`, `XFER-CAP-STREAMING-TRAVERSAL-SKETCH` |
| 044 | Incremental community/path jobs | event log plus affected-neighborhood materialization rather than full reprojection | `XFER-REQUEUE-DEPENDENCY-AFFECTED-STATE`, `XFER-BOUND-DESTINATION-UPDATE-LOGS` |
| 045 | PageRank/WCC | fixed-size cache tiles scheduled like explicit scratchpad memory | `XFER-CAP-RESIDENT-BLOCK-REUSE`, `XFER-CALIBRATE-PARTITION-SCATTER-MODE` |
| 046 | Filtered paths/kNN | cheap speculative superset engine wrapped by exact verifier and refusal guard | `XFER-VERIFY-SUPERSET-RESULTS-EXACTLY` |
| 047 | PageRank/BFS | deployment-calibrated layout selector choosing scatter, compression, and resident reuse | `XFER-CALIBRATE-PARTITION-SCATTER-MODE`, `XFER-GUARD-COMPRESSED-ID-STREAMS`, `XFER-CAP-RESIDENT-BLOCK-REUSE` |
| 048 | Multi-algorithm | artifact broker retaining several narrow prepared views under a storage/RAM lease | compatible subset of representation transfers with explicit coexistence |

### Explicit baselines: ARCH-G08-049..050

The two draft baseline records already exist in
`synthesis/g08-generation-lanes/G08-lane-base.json`:

- `ARCH-G08-049`: Neo4j/GDS-like fully resident shared projection.
- `ARCH-G08-050`: conservative Knight Bus dual-CSR/reference layout.

Do not regenerate them unless normalization exposes a concrete contract defect.

---

## Lean Execution Runbook

### Phase 1: Generate six lane dossiers

For each lane:

1. Read this runbook, A007, the G08 contract, the G07 report, and all 20 G07
   transfer cards.
2. Do not read any path under `evidence/failure-cards/`.
3. Use the frozen seed map but permit a better replacement when it is genuinely
   distinct and traceable.
4. Emit exactly eight concise genomes to the assigned lane JSON.
5. Record the actual author identity and that G06 context was not loaded.
6. Stop. Do not normalize, judge, or update campaign status.

Lane outputs:

```text
arxiv-reference/synthesis/g08-generation-lanes/G08-lane-ram.json
arxiv-reference/synthesis/g08-generation-lanes/G08-lane-tail.json
arxiv-reference/synthesis/g08-generation-lanes/G08-lane-predict.json
arxiv-reference/synthesis/g08-generation-lanes/G08-lane-prep.json
arxiv-reference/synthesis/g08-generation-lanes/G08-lane-adopt.json
arxiv-reference/synthesis/g08-generation-lanes/G08-lane-wild.json
```

### Phase 2: Freeze and normalize

1. Validate the raw count and ID/niche ranges.
2. Hash every lane file and candidate JSON record.
3. Write the freeze manifest.
4. Normalize all 50 records to
   `synthesis/architecture-candidates/ARCH-G08-NNN.md`.
5. Update the plan from placeholder to real author/transfer information.
6. Keep `highest_completed_stage` below G06 review and terminal disposition
   blank until challenge.

### Phase 3: Challenge once

1. Load the 79 G06 failure cards only after freeze.
2. Build a transfer-to-failure and family-to-failure map once.
3. Challenge candidates in algorithm-family batches.
4. Record each repair in the candidate rather than a separate essay.
5. Assign terminal dispositions.
6. Preserve the raw freeze so repaired cards do not erase generation history.

### Phase 4: Pareto and report

1. Construct each ten-axis view independently.
2. Retain 12-18 candidates; preserve both Pareto and narrow specialized value.
3. State which designs cannot be composed and why.
4. Select 3-8 G09 falsifiers by uncertainty coverage, not excitement.
5. Write the report around the 10 mandatory questions.

### Phase 5: Review and close

1. Independent reviewer reads all 50 cards and the archive/report.
2. Repair only bounded P0/P1/P2 findings.
3. Finalize plan checksums.
4. Run the focused unit test once.
5. Run the G08 `--closure` validator once.
6. Run the shared corpus validator once.
7. Update README, Markdown index, campaign status, and progress journal.
8. Mark `G08_COMPLETE_VERIFIED_CLEARED`, recommend G09, and stop.

---

## Minimal Verification Policy

To avoid overthinking:

- Do not rerun code graph indexes.
- Do not add more schema fields unless a real candidate cannot be expressed.
- Do not expand the test framework before real candidate validation reveals a
  missing invariant.
- Do not repair unrelated historical G01-G04 tests.
- Do not run the full test suite after every lane.
- Do not review candidate prose for style before the raw freeze.
- Do not calculate numeric speedups or RAM savings in G08.
- Do not introduce a new research campaign.

Run checks only at these gates:

| Gate | Minimum check |
|---|---|
| Per lane | JSON parses, exactly eight expected IDs, G06 not loaded |
| Raw freeze | exactly 50 unique IDs and exact niche counts |
| After normalization | focused candidate schema validation |
| After challenge | 50 failure-response sets and 50 dispositions |
| After Pareto | 12-18 unique survivors and all ten axes |
| Closure | focused tests, `--closure`, shared corpus validator, review P0=P1=P2=0 |

---

## Ready-To-Paste Next Goal Prompt

Use this next. It deliberately executes only the first durable lane so a usage
reset cannot erase the whole portfolio.

```text
/goal Resume G08 from:
arxiv-reference/governance/G08-Architecture-Portfolio-Continuation-Runbook.md

Execute only Phase 1, lane G08-LANE-RAM.

Read:
1. the continuation runbook;
2. docs_PRD04/A007-spc-founder-interview-prep-v7.md;
3. arxiv-reference/governance/g08-architecture-evolution-contract.md;
4. arxiv-reference/sources/G07-constraint-transfer-report.md; and
5. all 20 cards under arxiv-reference/evidence/constraint-transfer-cards/.

Do not read G06, arxiv-reference/evidence/failure-cards/, or other future G08
lane outputs. Do not browse, benchmark, implement, redesign validation, rerun
code-graph indexing, modify G05-G07 evidence, commit, or push.

Create exactly ARCH-G08-001 through ARCH-G08-008 with primary niche
LOWEST_RAM in:
arxiv-reference/synthesis/g08-generation-lanes/G08-lane-ram.json

Use the frozen seed map in the runbook. Each candidate genome must state:
- identity and secondary niches;
- exact workload family and semantics;
- inherited G07 transfer IDs;
- topology, ordering, state placement, and scheduling;
- admission unit and minimum resident kernel;
- complete symbolic peak-RAM equation;
- symbolic I/O, preparation, storage, recomputation, and concurrency;
- state multiplicity and page-cache/mmap/direct-I/O accounting;
- preparation and old/new artifact coexistence;
- fallback ladder;
- exactness, determinism, oracle, and refusal;
- separate Neo4j/Cypher/Bolt/GDS boundaries;
- composition guards and conditions where it loses;
- receipt and estimator-feedback fields; and
- smallest reserved G09 falsifier.

Unknowns remain symbolic. Make no numeric performance claims. Record the real
author identity, prompt identity, timestamp, and g06_failure_context_loaded=false.
Keep prose concise but candidate-specific. Verify JSON parsing and the exact
eight IDs once, update arxiv-reference/journals/G08-progress.md with a small
checkpoint, and stop without starting another lane.
```

## Generic Prompts For Later Lanes

Reuse the prompt above and change only:

| Lane | IDs | Primary niche | Output |
|---|---|---|---|
| `G08-LANE-TAIL` | 009-016 | `LOWEST_TAIL_LATENCY` | `G08-lane-tail.json` |
| `G08-LANE-PREDICT` | 017-024 | `HIGHEST_PREDICTABILITY` | `G08-lane-predict.json` |
| `G08-LANE-PREP` | 025-032 | `LOWEST_PREPARATION_STORAGE` | `G08-lane-prep.json` |
| `G08-LANE-ADOPT` | 033-040 | `LOWEST_ADOPTION_FRICTION` | `G08-lane-adopt.json` |
| `G08-LANE-WILD` | 041-048 | `UNCONVENTIONAL_COMPOSABLE` | `G08-lane-wild.json` |

Each lane task should stop after its own durable JSON and journal checkpoint.

---

## Integration Goal Prompt

Use only after all six lane JSON files exist.

```text
/goal Resume G08 from
arxiv-reference/governance/G08-Architecture-Portfolio-Continuation-Runbook.md
and execute Timeline A Phase 2 only: raw freeze and normalization.

Verify six lanes of eight plus the two draft baselines produce exactly 50
unique IDs and the frozen niche counts. Do not load G06 or failure cards.
Create a checksummed raw-portfolio freeze manifest, then normalize all 50 into
arxiv-reference/synthesis/architecture-candidates/ using the existing G08
contract and validator. Populate real generation lineage and inherited
transfer IDs in g08-candidate-plan.tsv, but leave G06 review and terminal
dispositions pending. Run only the focused G08 schema checks, checkpoint the
G08 journal, and stop. No web, implementation, benchmark, commit, or push.
```

## Challenge And Pareto Goal Prompt

Use only after the raw freeze and 50 canonical pre-challenge cards exist.

```text
/goal Resume G08 from
arxiv-reference/governance/G08-Architecture-Portfolio-Continuation-Runbook.md
and execute Timeline A Phases 3 and 4 only.

First verify the raw-portfolio freeze. Only then load G06 failure cards and the
G07 adversarial review. Challenge all 50 candidates in algorithm-family
batches. For every applicable failure, add a guard, fallback, narrowed scope,
calibration dependency, merger, or rejection and update symbolic resource
terms when repairs add state or I/O. Assign exactly one terminal disposition
to every candidate while preserving the raw freeze.

Construct the ten-axis qualitative Pareto archive with 12-18 survivors and
NON_COMPARABLE where constants are unknown. Write the G08 architecture report
answering all 10 required questions and select 3-8 smallest G09 falsifiers by
shared uncertainty coverage. Do not run experiments or benchmarks. Checkpoint
the journal and stop before independent review. No commit or push.
```

## Independent Review And Closure Prompt

Use only after the report and Pareto archive exist.

```text
/goal Independently review and close G08 using
arxiv-reference/governance/G08-Architecture-Portfolio-Continuation-Runbook.md.

Act as a non-author reviewer. Inspect all 50 canonical candidates, all linked
G07 transfers and G06 responses, the raw freeze, candidate plan, Pareto archive,
and G08 report. Check exact candidate/niche/workload counts, whole-process
symbolic RAM, state multiplicity, page-cache/mmap/direct-I/O, preparation,
old/new coexistence, correctness, compatibility, fallback, receipt, loses_when,
and reserved G09 falsifiers. Verify that no measured RAM or latency claim was
invented and that all 50 candidates have terminal dispositions.

Record P0/P1/P2 findings in G08-adversarial-review.md. Repair bounded findings
and require P0=P1=P2=0. Finalize plan checksums, run the focused tests, the G08
--closure validator, and the shared corpus validator once each. Reconcile the
G08 journal, report, review, Pareto archive, README, Markdown-Value-Index.md,
and campaign-status.md. Mark G08 COMPLETE_VERIFIED_CLEARED, recommend G09, and
stop without starting G09. Do not commit or push.
```

---

## Final Handoff Principle

Do not spend the next usage window proving that the scaffolding exists. It
does. Spend it producing the architecture decisions that the scaffolding was
built to protect.

---

# Beyond G08: Product And Engineering Futures

This section begins only after G08 is `COMPLETE_VERIFIED_CLEARED`. It is a
rough causal roadmap, not an authorization to start G09 early and not a claim
that any architecture will outperform Neo4j/GDS.

## Post-G08 Decision Frame

### The next fork

Once G08 has produced 12-18 survivors and 3-8 falsifying experiments, Knight
Bus must choose what kind of proof to build first:

1. a customer-shaped dependency/security/access-path proof;
2. a technically famous PageRank/iterative-analytics proof; or
3. a compatibility-first Neo4j sidecar that reduces adoption friction.

These are not merely different feature priorities. They produce different
learning, users, architecture pressure, and failure modes.

### Assumptions

- One founder or a very small team works primarily with capable coding LLMs.
- The existing Night Bus dataset can be an initial fixture but is not general
  proof.
- Neo4j/GDS remains the comparison oracle only where semantics genuinely
  overlap.
- No RAM or latency claim becomes public until whole-process measurement exists.
- The A007 promise remains more important than broad Neo4j surface parity.
- Storage specialization is valuable only if build cost, freshness, temporary
  coexistence, and fallback are included.

### What a three-month win looks like

By day 90, a user can submit one production-shaped analytical request through
a declared compatibility boundary and receive:

1. an estimate tied to a hard RAM budget;
2. an admitted plan or pre-execution refusal;
3. exact or explicitly bounded semantics;
4. execution under measured whole-process resource accounting;
5. comparison against a reference oracle; and
6. a machine-readable receipt containing predicted versus observed high water.

One proof-carrying slice is a win. A broad but unverifiable Neo4j rewrite is not.

## Post-G08 Timeline A: Customer-Wedge Proof

**Recommended first future.** Build the dependency/security/access-path slice
that most directly expresses A007.

### First two weeks: G09 decision atlas

1. Select two or three access-path survivors plus both baselines.
2. Convert their smallest falsifiers into executable experiment packets.
3. Freeze one exact path/reachability semantic contract.
4. Define graph-shape fixtures: shallow-wide, deep-narrow, skewed hubs,
   disconnected regions, large output, and frontier-spill crossover.
5. Define the independent result oracle and canonical digest.
6. Instrument whole-process RSS, page-cache policy, mmap residency, direct-I/O
   buffers, build peak, spill bytes, output bytes, and wall-clock distribution.

**Gate at day 14:** every contender has a runnable verification loop before it
has an optimized implementation.

### Weeks three and four: conservative baseline first

1. Make the current Knight Bus reference path produce the complete A007
   receipt.
2. Execute the same semantic fixture against Neo4j where the query overlaps.
3. Calibrate estimator constants on separate calibration fixtures.
4. Reserve holdout shapes that implementation agents cannot optimize against.
5. Demonstrate pre-execution refusal below the minimum resident kernel.

**Gate at day 30:** result parity and resource observability are trustworthy,
even if performance has not improved.

### Month two: one specialized capsule

1. Implement the lowest-risk survivor, likely a forward-only or streamed exact
   access-path capsule.
2. Add bounded frontier, worker, output, and I/O admission units.
3. Implement fit, lower-concurrency, stream/spill, reference, and refuse paths
   only where their guards are measurable.
4. Preserve the reference path as the correctness fallback.
5. Run calibration fixtures, then sealed holdouts.
6. Repair the estimator before tuning throughput when observed high water
   escapes the envelope.

**Gate at day 60:** the specialized capsule either meets a user-declared
budget/latency objective on holdouts or is rejected/narrowed. “Promising” is
not a terminal result.

### Month three: thin production surface

1. Expose only the Cypher/Bolt subset needed by the chosen access-path workflow.
2. Return stable unsupported-surface errors for everything else.
3. Add a GDS-like estimate/run facade only if it helps the same workflow.
4. Package the fixture, oracle, resource receipt, and comparison command.
5. Put the artifact in front of design partners with graphs too costly or
   unpredictable for their current workflow.

**Gate at day 90:** at least one external user can understand why Knight Bus
admitted, changed, or refused the plan and can reproduce the result.

### Months four to six

- Add PageRank as the second cartridge because it exercises dense iterative
  state and a very different storage/scheduling regime.
- Introduce an artifact broker only after two capsules prove that shared IDs,
  manifests, receipts, and fallback can be reused without forcing shared
  topology.
- Calibrate concurrent-query admission and artifact replacement peaks.

### Months six to twelve

- Add WCC or Louvain/Leiden only when a real workload justifies the next state
  shape.
- Turn receipts and estimator errors into a deployment-specific calibration
  corpus.
- Generalize the budget planner to non-graph deterministic analytical jobs only
  after at least two graph algorithms validate the abstraction.

### Lived experience

- Daily work feels concrete because every optimization has an oracle and budget.
- Product conversations begin early.
- Technical fame may arrive more slowly than with a PageRank headline.
- The main stress is resisting requests for broad Cypher/GDS compatibility.

### Main risk

The access-path workload may be too narrow or already cheap enough in existing
systems. The mitigation is early design-partner testing and a day-60 kill gate.

## Post-G08 Timeline B: PageRank Flagship

Build the clearest algorithm-shaped storage demonstration first: bounded
PageRank/SpMV with streamed sparse topology and controlled dense state.

### First two weeks

1. Select the resident shared-layout baseline, conservative Knight Bus
   baseline, stream-sparse/retain-dense candidate, destination-log candidate,
   and resident-block candidate.
2. Freeze numerical tolerance, convergence, iteration cap, ordering, and
   deterministic reduction contracts.
3. Build graph-shape fixtures spanning degree skew, partition imbalance,
   sparse/dense crossover, and slow convergence.

### Month one

1. Make baseline PageRank execution and receipts reproducible.
2. Measure topology, rank vectors, next-state vectors, worker scratch, queues,
   page cache/direct-I/O, output, and runtime separately.
3. Calibrate one-complete-dense-column and concurrency minimum kernels.

### Month two

1. Implement one semi-external PageRank capsule.
2. Compare source-order streaming, destination logs, and resident-block reuse
   only through controlled variants of the same kernel.
3. Use sealed holdouts to detect partition and cache-policy overfitting.
4. Reject any variant whose preparation or temporary storage erases its runtime
   value under realistic reuse.

### Month three

1. Publish a reproducible technical artifact with correctness, whole-process
   RAM, preparation, storage amplification, latency distribution, and receipts.
2. Demonstrate budget-driven concurrency reduction and refusal.
3. Offer a narrow GDS `estimate`/`run` surface for the proven PageRank contract.

### Months four to twelve

- Extend the sparse-stream/dense-state kernel to personalized PageRank or SpMV.
- Build a reusable fixed-memory iterative analytics runtime.
- Explore non-graph matrix/aggregation jobs only when the state and I/O model
  genuinely transfers.

### Lived experience

- The technical story is easier to explain and benchmark.
- The work is engineering-heavy and can become cache/partition tuning rather
  than customer discovery.
- Publication and open-source attention may arrive earlier.
- Adoption may lag because PageRank alone is not a full workflow.

### Main risk

The result may be a strong benchmark with weak product pull. The mitigation is
requiring a real repeated PageRank-like customer job before month three.

## Post-G08 Timeline C: Compatibility-First Sidecar

Lead with a Neo4j-shaped interface and route supported operations to Knight Bus
reference or specialized capsules.

### First two weeks

1. Freeze the exact Bolt messages, Cypher subset, GDS procedure names,
   parameters, outputs, errors, and notifications required by one workflow.
2. Build differential fixtures against Neo4j for that subset.
3. Declare every unsupported construct explicitly.

### Month one

1. Wire the supported query into the current Knight Bus reference layout.
2. Preserve exact semantic and error parity where claimed.
3. Emit estimates and receipts even before storage specialization exists.
4. Make fallback to Neo4j explicit rather than transparent magic.

### Month two

1. Add one optimized access-path or PageRank backend behind the same facade.
2. Let admission choose optimized Knight Bus, conservative Knight Bus,
   Neo4j/reference, or refusal.
3. Compare the routes with the same oracle and resource instrumentation.

### Month three

1. Test migration friction with existing Neo4j users.
2. Expand only surface elements demanded by the target workflow.
3. Publish the compatibility matrix beside the performance/resource receipt.

### Months four to twelve

- Add algorithm cartridges behind stable procedure contracts.
- Keep the compatibility layer thin and separate from storage/algorithm cores.
- Consider becoming a bounded analytical accelerator for Neo4j rather than a
  replacement database.

### Lived experience

- Demos feel real quickly because existing clients can connect.
- User feedback arrives earlier.
- Compatibility edge cases consume attention and can crowd out differentiation.
- There is a constant temptation to chase full Cypher/Bolt/GDS parity.

### Main risk

Knight Bus becomes an adapter project before proving its lower-RAM thesis. The
mitigation is a hard rule: no compatibility feature unless it unlocks a proven
bounded analytical workflow.

## Post-G08 Cross-Timeline Analysis

| Dimension | A: customer wedge | B: PageRank flagship | C: compatibility sidecar |
|---|---|---|---|
| First proof | Exact access path under budget | Iterative analytics under budget | Existing Neo4j-shaped query runs |
| Fastest customer learning | **High** | Medium-low | High |
| Clearest technical benchmark | Medium | **Highest** | Medium |
| Strongest A007 expression | **Highest** | High | Medium-high |
| Differentiated storage pressure | High | **Highest** | Initially low |
| Adoption friction | Medium | High | **Lowest** |
| Scope-creep risk | Medium | Medium | **Highest** |
| Risk of benchmark without product | Low-medium | **High** | Low-medium |
| Risk of adapter without moat | Low | Low | **High** |
| Reversibility at day 30 | High | High | Medium-high |

### Robust hybrid

The strongest sequence is not a simultaneous blend. It is an ordered hybrid:

```text
Customer-shaped access-path proof
        -> thin Cypher/Bolt skin for that proof
        -> PageRank as second algorithm-shaped cartridge
        -> shared admission/receipt/artifact broker
        -> broader deterministic analytical compute
```

The ordering matters. Building compatibility before the proof risks an adapter;
building PageRank before a customer wedge risks a benchmark project; building
both after the access-path verification loop turns them into leverage.

### Shared kill criteria

Pause or pivot when any of these persists through a bounded repair cycle:

1. Whole-process RSS cannot be explained by named estimator terms.
2. Preparation and old/new coexistence make the specialized artifact
   uneconomic at realistic reuse.
3. The low-RAM plan has unacceptable tail latency under the declared deadline.
4. Correctness requires a reference fallback so often that specialization has
   no useful operating envelope.
5. Users do not value predictable refusal/receipts enough to change behavior.
6. Compatibility work repeatedly exceeds algorithm/storage work.
7. Holdout results reverse the apparent calibration-fixture advantage.

## Post-G08 Decision Filter

Choose Timeline A by default.

Choose Timeline B first only if a real workload runs iterative graph analytics
repeatedly, cost is visibly dominated by resident topology/state, and a
reproducible PageRank result would unlock users or contributors.

Choose Timeline C first only if a committed design partner requires an existing
Neo4j query/client to participate in the verification loop and the exact
compatibility subset is small.

### The uncertainty-collapsing first experiment

Regardless of the product timeline, the first experiment should answer:

> Can Knight Bus predict and enforce whole-process peak memory for one exact
> analytical request, including runtime, page cache/direct I/O, output, spill,
> and temporary coexistence, while matching an independent oracle?

If the answer is no, more storage inventions are premature. If the answer is
yes, every later algorithm becomes a bounded variation on a proven control
loop.
