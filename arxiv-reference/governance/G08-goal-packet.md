# G08 Goal Packet: Architecture Evolution Arena

## Mission

G08 converts the 20 frozen and independently cleared G07 constraint-transfer
cards into a deliberately plural architecture portfolio for bounded graph
OLAP. It does not pick one universal representation, implement a candidate,
run a benchmark, or begin G09.

The north star remains
`docs_PRD04/A007-spc-founder-interview-prep-v7.md`: a user declares a hard
resource budget; Knight Bus chooses fit, lower concurrency, stream, spill,
approximate, reference, or refuse before execution; it enforces the chosen
plan, verifies the result, and emits a resource receipt.

- Goal ID: `G08`
- Objective: produce exactly 50 traceable architecture candidates across six
  qualitative niches and two explicit baselines, challenge all of them after
  divergent generation, and retain 12-18 non-dominated candidates.
- Central question: which small family of algorithm-layout capsules spans the
  useful RAM, latency, predictability, preparation, and adoption Pareto surface
  without requiring one universal graph representation?
- Product lens: dependency, security, and access-path traversal remains first;
  breadth across PageRank, WCC, Louvain/Leiden, node similarity/kNN, and shared
  artifacts is supporting portfolio coverage.
- Owned outputs: this packet, the G08 executable contract, 50 candidate cards,
  candidate plan, Pareto archive, synthesis report, independent review,
  progress journal, focused validation pipeline/tests, and navigation/status
  updates.
- Supporting reproducibility outputs: six frozen generation-lane dossiers and
  a raw-portfolio freeze manifest.
- Batch cap: exactly 50 raw candidates, six independent lanes of eight plus two
  controller-authored baselines, one post-freeze challenge pass, one
  independent non-author reviewer, and 12-18 archive survivors.
- External request cap: zero.
- Excluded work: web research, papers, repositories, downloads, implementation,
  microbenchmarks, workload benchmarks, measured deltas, G09 execution, commit,
  and push.
- Journal: `arxiv-reference/journals/G08-progress.md`.

## Entry Gate

G08 semantic generation begins only after local checks establish:

1. G07 is `COMPLETE_VERIFIED_CLEARED` and recommends G08.
2. The G07 plan contains exactly 20 `COMPLETE`, checksum-backed rows.
3. All 20 G07 transfer cards pass the shared corpus validator.
4. G07's independent review is `CLEARED` with P0 = P1 = P2 = 0.
5. A007, the G07 report, review, plan, and all transfer cards are readable.
6. No G08 candidate exists before the contract and focused tests are created.

The entry gate passed locally on 2026-08-12. G05-G07 semantic artifacts are
frozen inputs and SHALL NOT be mutated by G08.

## Current Knight Bus Grounding

Codebase Memory was indexed before candidate generation. The portfolio is
anchored to existing implementation facts rather than an imagined blank slate:

- `src/low_ram.rs::build_snapshot_from_paths_low_ram` already constructs a
  snapshot through bounded external runs and verifies emitted artifacts.
- `src/gds/execution.rs::memory_estimate_detail_map_now` already exposes
  topology, sidecar, heap, page-cache, direct-I/O, algorithm-state, overlay,
  and scratch estimate categories.
- `src/gds/execution.rs::execute_registered_gds_entry` currently dispatches
  graph-catalog and projection surface entries; broad GDS algorithm execution
  is not thereby implemented.
- `src/bolt.rs::KnightBusBoltBackend::execute` and the narrow Cypher walk
  contract provide a compatibility seed, not general Neo4j/Cypher parity.
- The snapshot path supplies shared dual-CSR/reference-layout baselines; it
  does not prove that one representation is efficient for every algorithm.

CodeGraphContext is the independent structural cross-check. Its output path,
index identity, queries, and any disagreements with Codebase Memory are recorded
in the G08 journal before closure.

## Frozen Portfolio Budget

| Lane | Primary niche | Count | Candidate IDs |
|---|---|---:|---|
| G08-LANE-RAM | Lowest peak RAM | 8 | ARCH-G08-001..008 |
| G08-LANE-TAIL | Lowest p99/P100 risk | 8 | ARCH-G08-009..016 |
| G08-LANE-PREDICT | Highest predictability | 8 | ARCH-G08-017..024 |
| G08-LANE-PREP | Lowest preparation/storage amplification | 8 | ARCH-G08-025..032 |
| G08-LANE-ADOPT | Lowest Neo4j adoption friction | 8 | ARCH-G08-033..040 |
| G08-LANE-WILD | Unconventional but composable | 8 | ARCH-G08-041..048 |
| G08-LANE-BASE | Explicit baselines | 2 | ARCH-G08-049..050 |

The baselines are a Neo4j/GDS-like fully resident shared-layout baseline and a
conservative Knight Bus reference-layout baseline. They are not straw men and
must carry the same complete cost, correctness, preparation, and compatibility
accounting as every other candidate.

## Generation Isolation

Six lane authors read A007, the 20 normalized G07 transfer cards, and current
Knight Bus anchors. They do not read raw G06 failure cards. Each author emits
exactly eight candidates in its assigned ID range and niche. The controller
adds the two baselines, freezes all 50 raw candidate bytes and lineage, and
only then opens G06 counterexamples to a separate challenger.

This sequence is an epistemic control, not ceremony: failure evidence is
withheld during divergence so criticism cannot collapse the search space. The
post-freeze challenger may repair, specialize, defer, merge, or reject a
candidate, but the raw candidate and lineage remain recoverable.

## Evaluation Gates

1. Schema completeness.
2. Source and transfer lineage.
3. Symbolic resource completeness.
4. Invariant composition.
5. Post-freeze G06 counterexamples.
6. A007 enforceability.
7. Preparation and storage amplification.
8. Qualitative Pareto placement.
9. Independent adversarial review.

G08 records the highest completed stage and one terminal disposition for every
candidate. It does not execute the microbenchmark or workload-benchmark stages
reserved by the campaign SOP for later goals.

## Exit Tests

G08 closes only when:

- exactly 50 candidate cards and 50 checksum-backed plan rows exist;
- primary niche counts are exactly 8, 8, 8, 8, 8, 8, and 2 baselines;
- all seven priority workload families are represented;
- every card includes complete symbolic resource, preparation, temporary
  coexistence, correctness, compatibility, receipt, and falsifier terms;
- every card has a post-freeze G06 challenge record and terminal disposition;
- 12-18 non-dominated or specialized candidates appear in the Pareto archive;
- no numeric RAM or latency improvement is presented as measured;
- the independent review inspects all 50 cards and closes with P0=P1=P2=0;
- focused G08 tests and the shared corpus validator pass;
- report, review, journal, README, Markdown index, and campaign status agree;
- the handoff recommends G09 without starting it.

## Stop Conditions

Stop and repair if candidate count drifts; a candidate has no workload
contract; an unknown resource term is treated as zero; preparation or old/new
artifact coexistence disappears; page cache or mmap is treated as free; a
composition has no surviving invariant; G06 is loaded before the raw freeze; a
numeric delta is invented; exactness or compatibility scope expands silently;
an external request occurs; or review retains an unresolved P0, P1, or P2.

