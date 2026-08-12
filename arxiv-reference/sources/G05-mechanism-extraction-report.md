# G05 Mechanism Extraction Report

## Result

G05 semantically read the complete 427-page selected corpus: exactly 25 of the
34 checksum-verified papers acquired and parsed by G04. Every selected paper
reached `MECHANISM_EXTRACTED`. The result is 67 source-grounded mechanism cards
and 47 typed relationships. The other nine G04-eligible papers remain
`DEEP_READ` and were not read by G05.

This is an evidence-extraction result, not an architecture recommendation.
G05 did not create failure cards, modern constraint transfers, architecture
candidates, experiments, implementations, performance promises, or product
claims. Those are later-goal responsibilities.

The final 195-file evidence freeze had aggregate SHA-256
`E6A843D8FA082474316436884BDEE30F01B90DEE4760B4CD63A831A9E2CC25D9`.
An independent skeptical reviewer recomputed that aggregate before and after
review, reproduced the 158-test and corpus-validation gates, and cleared G05
with no unresolved P0, P1, or P2 findings. The durable review is
`governance/reviews/G05-adversarial-review.md`.

## Deterministic Scope

Selection was frozen before semantic reading. Eligible papers had to be both
`ACQUIRED` and `PARSED` in the G04 terminal ledger, `DEEP_READ` in the manifest,
and linked to local PDF and text files whose paths and SHA-256 values matched
the ledger. The selector ordered that 34-paper set by:

1. relevance score descending;
2. G04 queue rank ascending; and
3. paper ID ascending.

It selected the first 25, then assigned selection ranks round-robin to five
disjoint batches of five. The frozen reading plan covers all 12 architecture
questions and 427 PDF pages. Readers used only local G04 PDFs and extracted
texts. External requests made by G05: **zero**.

## Reader Provenance

| Batch | Reader agent | Papers | Pages | Initial/final cards | Initial dossier SHA-256 | Repair dossier SHA-256 |
|---|---|---:|---:|---:|---|---|
| G05-BATCH-1 | `019ff41e-ac83-7793-86db-1f0b3e883afa` | 5 | 104 | 13/13 | `44D9C4E00ECE412AD44359944398879EEFF349D07A8F67CE2E7F785D06B6C266` | `NOT_APPLICABLE` |
| G05-BATCH-2 | `019ff41e-ad6c-7791-80c0-b6a701cd3639` | 5 | 75 | 14/15 | `BA7EE4EE41EE4E3394CDC92F711670293003CA3ABA9F22E08B72A3BE1534D5D8` | `C85D5CB8D9220C70B50908E68D99BC2578F1D796EDB20BAEE5F8C2FDCB343FBA` |
| G05-BATCH-3 | `019ff41e-acf6-7771-8451-a65c16a84220` | 5 | 89 | 11/12 | `06946637B53F6AC99A87FF807FF8BF992C8FEC0FC2AC884288EB6E876F45257E` | `E65DB016F87D9DD83025E5FF477C4E526A1C1C390ACFD727670D790DE197E58E` |
| G05-BATCH-4 | `019ff41e-ae5f-7992-a0b1-5e1a33825021` | 5 | 81 | 12/17 | `E026D0637B2EC1DD7325664DCC63681BB27C8D3FB40D026FAFB1D599620C593F` | `BA3459EAA9A7F37FB895BBD2286E420787D971357C5C0041D78BD0D04B9A300B`; `F857A8920D0A78B5F9535CEBC00214C1066EF0363BDB14A070151C2BBD85313C` |
| G05-BATCH-5 | `019ff41e-ade2-7481-b8c6-e02f38b82848` | 5 | 78 | 8/10 | `BBE5C23F7AEBA91262F2427F3606B8CBB0088A1914BBB257EDA11A40858FB0B4` | `1E39611878D89A1436F97548253BF3E10F18B526C98EA30963667717F08C99B1` |

Each reader inspected every page assigned to its batch and recorded a terminal
outcome per paper. The controller parsed all proposed card envelopes, rejected
no final card for schema failure, canonicalized relationship direction and
pointer qualification, and bound every terminal reading row to its card
payloads with a SHA-256 result checksum.

## Adversarial Repair Pass

The first independent audit declined clearance with P0=0, P1=3, and P2=3. G05
then repaired the evidence corpus before closure:

1. twenty-three extractor confidence appraisals and all extractor-owned
   absence judgments were relabeled from `SOURCE_CLAIM` to
   `DERIVED_INFERENCE` with explicit premises, assumptions, and uncertainty;
2. eight cross-page figure, table, theorem, and section pointers were made
   page-precise;
3. an empirical residual-prefix precision trend was removed from the invariant
   field and retained only as qualified empirical evidence;
4. one broad active-set similarity edge was removed and the actual
   changed-neighborhood scheduling pair was linked;
5. over-combined BFS direction, bitmap, and distance-write mechanisms were
   separated; and
6. eight omitted reusable mechanisms were added for PQ plus exact reranking,
   cache management, all-in-storage neighbor codes, pipelined disk reads,
   bounded global score tables, visited bitmaps, deferred distance writes, and
   displaced-state prefetching; and
7. a second audit split continuous per-completion I/O replenishment from generic
   asynchronous overlap and added five missing duplicate/extension links.

All affected card links, typed relationships, navigation caches, and terminal
result checksums were regenerated. The repair pass still extracts evidence; it
does not transfer or combine mechanisms into a Knight Bus architecture.

## Exact Output Accounting

| Measure | Count |
|---|---:|
| G04-eligible papers | 34 |
| G05-selected papers | 25 |
| Fully read PDF pages | 427 |
| `MECHANISM_EXTRACTED` outcomes | 25 |
| `NO_MECHANISM` outcomes | 0 |
| Papers left `DEEP_READ` | 9 |
| Mechanism cards | 67 |
| Typed pattern edges | 47 |
| `COMPLEMENTS` edges | 41 |
| `SHARES_MECHANISM_WITH` edges | 6 |
| G05 external requests | 0 |

Every card has one canonical JSON envelope, one or more valid source papers,
page-bounded source pointers, sourced/derived/unknown resource terms, explicit
works/fails/unknown boundaries, an A007 consequence, and a smallest falsifying
test description. A `RESERVED-G09-FOR-<PATTERN-ID>` value reserves later
experiment ownership; it does not claim that an experiment exists.

## Evidence Strength

| Evidence grade | Cards | Operational meaning in G05 |
|---|---:|---|
| `C_PAPER_BENCHMARK` | 59 | Paper reports an evaluated mechanism, but G05 did not reproduce it or inspect implementation code. |
| `D_THEORETICAL_OR_INCOMPLETE` | 8 | Mechanism is analytical or materially incomplete for an implementation or resource claim. |
| `A_REPRODUCED` | 0 | No paper mechanism was reproduced in G05. |
| `B_CODE_BACKED` | 0 | No implementation repository was inspected in G05. |

The cards intentionally distinguish source claims from controller-derived
consequences. No source-reported benchmark number is converted into a Knight
Bus performance estimate. No unknown byte coefficient is silently replaced by
an assumption.

## Resource-Term Coverage

| Resource term | Sourced | Derived | Unknown |
|---|---:|---:|---:|
| RAM | 42 | 10 | 15 |
| I/O | 39 | 4 | 24 |
| Preprocessing | 42 | 3 | 22 |
| Persistent storage | 24 | 4 | 39 |
| Temporary storage | 20 | 17 | 30 |

This table is the most important brake on overclaiming. The literature often
describes a useful state arrangement while omitting whole-process RSS,
persistent byte coefficients, temporary overlap, durability, or a calibrated
admission equation. `UNKNOWN` therefore remains a first-class result with an
explicit measurement requirement.

## Mechanism Families Learned

The cards expose reusable mechanisms, not one proposed system. Important
evidence families include:

### Bound And Stage Live State

- bound a graph-ANN candidate frontier;
- store random-walk endpoints rather than complete walks;
- decompose diffusion into stages and select high-residual subgraphs;
- prune settled semi-external BFS state;
- threshold in-memory sketch growth;
- checkpoint pruning work between foreground stalls; and
- bound scheduler overrun from observed idle-window samples.

These mechanisms are relevant to A007 because they expose countable resident
state or a pause boundary. They do not by themselves prove whole-process RSS.

### Shape Storage Around Access

- partition updates by destination;
- stream partitioned updates sequentially;
- inline low-degree adjacencies;
- decouple ANN navigation from raw-vector blocks;
- colocate ANN neighbors within pages;
- pack only nonempty sparse rows;
- infer unweighted edge values rather than storing them; and
- compress sorted ID streams used by out-of-core subgrids.

These are direct precedents for algorithm-shaped storage. Their transfer to
Knight Bus remains unperformed until G07 states the preserved invariant and
recalculates modern resource costs.

### Avoid Work That Cannot Matter

- skip inactive bins hierarchically;
- skip finalized vector chunks;
- reactivate or revisit only changed neighborhoods;
- shrink already visited BFS partition bounds;
- scan rare filter postings and approximately test frequent filters;
- route filtering plans by estimated cost; and
- select partition scatter mode from active communication volume.

The shared idea is conditional work suppression. Correctness depends on the
specific activity, finality, label, or filter invariant in each card; these
mechanisms are not interchangeable merely because they all skip work.

### Schedule I/O And Compute Deliberately

- prioritize resident active blocks;
- pipeline asynchronous I/O with compute;
- prefetch candidate blocks asynchronously;
- search every record on an already fetched page;
- navigate a small memory graph before disk search;
- grow beam width progressively; and
- interleave partition updates only under explicit visibility conditions.

These mechanisms improve overlap or useful work per fetch under stated source
conditions. None proves lower P100 latency in Knight Bus without later workload
and device-specific falsification.

### Preserve Exactness Around Approximation

- explore an approximate superset and verify final results;
- refine hashed set candidates exactly;
- navigate with binary distance and rerank using original vectors;
- probe quantized-topology compatibility before selecting an index plan; and
- prune ANN graph neighbors by directional diversity while bounding the search
  candidate structure separately.

This family is especially relevant to a verification-first product: compact or
approximate state can be useful when its false positives, false negatives, and
exact verification boundary are explicit.

### Maintain And Rebuild Dynamic State

- batch updates before consolidation;
- relog predicted active edges;
- recycle surviving stream edges during aging;
- merge thread-local coarsenings;
- refine communities before aggregation; and
- preserve point-in-time connectivity queries inside the stream order.

These mechanisms reveal temporary-state and consistency obligations that a RAM
estimate must include. They do not imply that one update model suits every
algorithm.

## Consequence For A007

G05 improves the vocabulary needed to make A007 measurable. The evidence shows
that a credible resource receipt may need to name, per algorithm and plan:

1. the exact resident structures and their cardinality bounds;
2. the storage unit and access schedule, such as partition, page, block, tile,
   log, candidate frontier, or residual subgraph;
3. streamed and recomputed state rather than only persistent graph bytes;
4. preparation and temporary-overlap costs;
5. the conditions under which work can be skipped;
6. the exactness or verification boundary around approximation;
7. device and workload assumptions that control useful I/O; and
8. a refusal or falsification condition when the bound cannot be established.

That is a stronger evidence base for later architecture work. It is not yet a
validated Knight Bus estimator, admission controller, storage format, or
customer promise.

## Preserved Uncertainty

- Fifty-nine cards rely on source-reported benchmarks without campaign
  reproduction or implementation inspection.
- Persistent-storage cost is unknown in 39 cards; temporary-storage cost is
  unknown in 30; I/O cost is unknown in 24.
- ANN mechanisms are sensitive to graph topology, recall target, dimension,
  page size, cache state, and device behavior.
- BFS, PageRank, connectivity, and community mechanisms preserve different
  semantics; superficial scheduling similarity is not a transfer proof.
- One extracted BFS source has malformed G04 form-feed segmentation, though
  page pointers remain bounded to the PDF.
- A semi-external BFS paper may reverse time and I/O labels in one page-13
  statement; the card preserves the uncertainty rather than resolving it.
- A non-atomic BFS optimization invoking undefined C++ behavior was excluded
  from the mechanism corpus.
- No card establishes Bolt, Cypher, Neo4j GDS, durability, or transactional
  compatibility.
- No card establishes whole-process RSS enforcement or calibrated refusal.

## Reproducibility And Stop Boundary

The canonical sources of truth are:

- `governance/g05-reading-plan.tsv` for selection, agents, coverage, terminal
  outcomes, card links, and result checksums;
- `evidence/mechanism-cards/` for the 67 source-grounded envelopes;
- `evidence/pattern-edges.tsv` for the 47 typed relationships; and
- `sources/paper-manifest.tsv` for exactly 25 `READ_COMPLETE` transitions.

G05 stops at evidence extraction. G06 may use this corpus to extract
counterexamples, adversarial workloads, and explicit evidence conflicts only
after G05 receives independent clearance. G06 must not silently synthesize an
architecture or treat a source benchmark as a Knight Bus result.
