# G06 Counterexample Extraction Report

## Executive Result

G06 completed the adversarial pass over the frozen G05 evidence corpus without
expanding it. Five disjoint read-only lanes inspected every page of the same 25
papers and challenged every one of the 67 mechanism cards. The controller
accepted 79 canonical failure cards after collapsing six exact cross-lane
rediscoveries and two semantic duplicate pairs from 87 lane records.

The principal result is not that the mechanisms are unusable. It is that none
may be admitted as an unconditional bounded-compute primitive. Their usefulness
depends on workload shape, graph density and skew, representation width,
frontier behavior, update intensity, cache residency, queue growth, device
parallelism, or approximation tolerance. A007 therefore needs an admission
model and receipt vocabulary that expose these conditions rather than treating
an algorithm name and graph size as a sufficient resource contract.

G06 ran no independent reproduction or benchmark. Source-reported limitations
are paraphrased with exact page pointers; derived consequences and fixtures
remain explicitly analytical.

## Frozen Corpus And Scope

| Measure | Result |
|---|---:|
| Papers inspected | 25 |
| Pages inspected | 427 |
| G05 mechanisms adversarialized | 67 |
| Preserved G05 pattern edges | 47 |
| Adversarial-plan subjects | 92 |
| External requests | 0 |
| Added paper identities | 0 |
| Repository acquisitions | 0 |
| Later-goal artifacts | 0 |

The paper, manifest, request-ledger, mechanism-card, and pattern-edge inputs
remain bound to the fingerprints frozen in
`governance/g06-counterexample-contract.md`. Every paper row records exact
`ALL_PAGES` coverage, and every pattern row points back to completed source-paper
rows.

## Negative Evidence Accounting

Papers inspected: 25

Pages inspected: 427

Patterns disposed: 67

Failure cards: 79

Evidence conflicts: 0

Explicit evidence gaps: 0

| Evidence classification | Count | Meaning in this campaign |
|---|---:|---|
| `SOURCE_REPORTED` / `SOURCE_CLAIM` | 41 | The cited paper explicitly reports the bounded limitation, reversal, or sensitivity. |
| `SOURCE_SUPPORTED_DERIVATION` / `DERIVED_INFERENCE` | 20 | Sourced premises support the consequence, but the consequence is not an author benchmark claim. |
| `ANALYTICAL_COUNTEREXAMPLE` / `DERIVED_INFERENCE` | 18 | A minimal oracle-bearing fixture challenges a sourced mechanism premise; it was not executed. |
| Source pointers | 174 | Page-bounded pointers across all 25 frozen papers. |
| Symbolic breakpoint expressions | 78 | Condition equations whose constants remain unmeasured. |
| Fully unknown breakpoint expressions | 1 | The corpus does not support an honest crossover expression. |
| Numeric breakpoint constants | 0 | G06 invented no numeric threshold. |

All 25 paper rows use `NEGATIVE_EVIDENCE_EXTRACTED`. Of the 67 mechanism rows,
35 use `SOURCE_FAILURE_LINKED` and 32 use `ANALYTICAL_TEST_LINKED`. Zero rows
use `EXPLICIT_EVIDENCE_GAP`, but this does not imply complete quantitative
knowledge: unknown constants, target-machine crossovers, and missing
measurements remain visible inside the cards and their fixtures.

### Semantic Merge Ledger

The first independent review found two duplicate pairs that the frozen lexical
signature could not detect. The controller retired the redundant IDs below and
rewired their paper rows to the surviving canonical cards. Each pair has the
same source paper, affected mechanism, triggering workload class, observable
symptom, and failure-boundary meaning. The surviving card retains every valid
body locator needed by either record; forbidden abstract locators were not
carried forward.

| Retired lane failure ID | Canonical failure ID | Canonicalization basis |
|---|---|---|
| `FAIL-BINARY-QUANTIZATION-GEOMETRY-COLLAPSE` | `FAIL-INCOMPATIBLE-GEOMETRY-COLLAPSES-RECALL` | Same `PAPER-2605.02171` geometry mismatch, binary-navigation premise, exact-rerank boundary, and recall-collapse symptom; canonical pointers retain Tables 10-11 and Section 6. |
| `FAIL-REORDERING-PREPROCESSING-DOMINATES-TRAVERSAL` | `FAIL-FULL-REORDER-DOMINATES-TRAVERSAL` | Same `PAPER-2012.10026` low-reuse RCM workload, preprocessing-amortization inequality, locality/scheduling consequence, and end-to-end slowdown; canonical pointers retain the body introduction and Table 4 discussion. |

## Failure Taxonomy

The strongest recurring boundary is a crossover, not a universal defeat. A
layout or schedule saves one resource only while it does not amplify another:
compression versus decode work, locality versus preprocessing, prefetch versus
wasted reads, batching versus query freshness, sparse state versus density,
bounded candidates versus recall, and asynchronous overlap versus contention.

Repair options are non-decisional response classes. Their frequency shows what
later architecture and experiment goals must make explicit; it does not select
a design.

| Possible response class | Card occurrences |
|---|---:|
| `ADD_ADMISSION_GUARD` | 58 |
| `ADD_FALLBACK_PATH` | 54 |
| `MEASURE_UNKNOWN` | 32 |
| `CHANGE_SCHEDULE` | 29 |
| `ADD_RESOURCE_BOUND` | 26 |
| `SPECIALIZE_WORKLOAD` | 18 |
| `CHANGE_REPRESENTATION` | 13 |

The 79 fixtures divide into 54 combined graph-and-execution profiles, 13 graph
fixtures, and 12 execution-profile fixtures. That distribution matters for
A007: most failures cannot be predicted from static graph bytes alone. They
depend jointly on graph shape and runtime scheduling, I/O, or concurrency.

## Pattern Coverage

Every pattern has exactly one terminal disposition and at least one linked
failure card. The complete canonical matrix follows; the plan remains the
machine-readable authority.

<!-- G06_PATTERN_MATRIX_START -->
| Pattern | Terminal disposition | Linked failure cards |
|---|---|---|
| `PAT-BALANCE-BUCKETED-PACKED-SETS` | `ANALYTICAL_TEST_LINKED` | `FAIL-WIDE-FIELDS-ERASE-PACKING` |
| `PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS` | `SOURCE_FAILURE_LINKED` | `FAIL-PHASE-SCHEDULING-WORKLOAD-REVERSAL` |
| `PAT-BATCH-UPDATES-BEFORE-CONSOLIDATION` | `SOURCE_FAILURE_LINKED` | `FAIL-QUERY-HEAVY-OVERLAY-CROSSOVER` |
| `PAT-BOUND-GLOBAL-SCORE-TABLE` | `SOURCE_FAILURE_LINKED` | `FAIL-BOUNDED-TABLE-DROPS-CANDIDATES` |
| `PAT-BOUND-OVERRUN-FROM-SAMPLES` | `ANALYTICAL_TEST_LINKED` | `FAIL-RECENT-SAMPLES-MISS-SHIFTS` |
| `PAT-BOUND-SEARCH-CANDIDATE-FRONTIER` | `SOURCE_FAILURE_LINKED` | `FAIL-BOUNDED-FRONTIER-MISSES-NEAREST` |
| `PAT-CACHE-ENTRY-NEARBY-VERTICES` | `ANALYTICAL_TEST_LINKED` | `FAIL-STATIC-ENTRY-CACHE-SHIFT` |
| `PAT-CHECKPOINT-PRUNING-BETWEEN-STALLS` | `SOURCE_FAILURE_LINKED` | `FAIL-IOBOUND-UPDATES-EMPTY-STALLS` |
| `PAT-COLOCATE-NEIGHBOR-CODES-ONDISK` | `SOURCE_FAILURE_LINKED` | `FAIL-NEIGHBOR-CODES-AMPLIFY-STORAGE` |
| `PAT-COLOCATE-NEIGHBORS-WITHIN-PAGES` | `SOURCE_FAILURE_LINKED` | `FAIL-PAGE-SHUFFLE-EXCEEDS-MEMORY`, `FAIL-PAGE-SHUFFLE-LOSES-UTILITY` |
| `PAT-COMPOSE-MULTIRESOLUTION-CLUSTER-ORDERS` | `SOURCE_FAILURE_LINKED` | `FAIL-GRAPH-RESIDENCY-ADDS-PASSES`, `FAIL-SINGLE-RESOLUTION-LOSES-ORDERING` |
| `PAT-COMPRESS-SORTED-ID-STREAMS` | `ANALYTICAL_TEST_LINKED` | `FAIL-XOR-STREAM-COMPRESSION-CROSSOVER` |
| `PAT-DECOMPOSE-DIFFUSION-INTO-STAGES` | `ANALYTICAL_TEST_LINKED` | `FAIL-RESIDUAL-PRUNING-BREAKS-EXACTNESS` |
| `PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS` | `SOURCE_FAILURE_LINKED` | `FAIL-DECOUPLED-LAYOUT-DOUBLES-READS` |
| `PAT-EXPLORE-SUPERSET-VERIFY-RESULTS` | `ANALYTICAL_TEST_LINKED` | `FAIL-FALSE-NEGATIVES-ESCAPE-VERIFICATION`, `FAIL-FALSE-POSITIVES-AMPLIFY-WORKLOAD` |
| `PAT-FILTER-COMPRESSED-RERANK-EXACTLY` | `ANALYTICAL_TEST_LINKED` | `FAIL-COMPRESSED-FILTER-DROPS-NEIGHBOR` |
| `PAT-GROW-BEAM-WIDTH-PROGRESSIVELY` | `ANALYTICAL_TEST_LINKED` | `FAIL-PROGRESSIVE-BEAM-IO-EXPANSION` |
| `PAT-INFER-UNWEIGHTED-EDGE-VALUES` | `SOURCE_FAILURE_LINKED` | `FAIL-WEIGHTED-EDGES-NEED-VALUES` |
| `PAT-INLINE-LOW-DEGREE-ADJACENCIES` | `SOURCE_FAILURE_LINKED` | `FAIL-INLINE-THRESHOLD-INFLATES-MEMORY` |
| `PAT-INTERLEAVE-PARTITION-UPDATES-SAFELY` | `ANALYTICAL_TEST_LINKED` | `FAIL-INTERLEAVING-LOSES-EXCLUSIVE-OWNERSHIP` |
| `PAT-MATERIALIZE-EACH-PIPELINE-STAGE` | `SOURCE_FAILURE_LINKED` | `FAIL-FIXED-ITERATIONS-MISS-CONVERGENCE`, `FAIL-INMEMORY-SORT-EXCEEDS-BUDGET`, `FAIL-STAGE-MATERIALIZATION-EXCEEDS-MEMORY` |
| `PAT-MERGE-THREAD-LOCAL-COARSENINGS` | `ANALYTICAL_TEST_LINKED` | `FAIL-PARTIAL-COARSE-GRAPH-AMPLIFICATION` |
| `PAT-NAVIGATE-BINARY-RERANK-EXACTLY` | `SOURCE_FAILURE_LINKED` | `FAIL-INCOMPATIBLE-GEOMETRY-COLLAPSES-RECALL` |
| `PAT-NAVIGATE-MEMORY-BEFORE-DISK` | `SOURCE_FAILURE_LINKED` | `FAIL-NAVIGATION-SAMPLE-MISSES-REGIONS` |
| `PAT-ORDER-QUERIES-WITH-STREAM` | `SOURCE_FAILURE_LINKED` | `FAIL-AGING-SUSPENDS-STREAM-QUERIES`, `FAIL-LARGE-OUTPUTS-SERIALIZE-QUERIES` |
| `PAT-PACK-CONNECTIVITY-STATE-PREFIX` | `SOURCE_FAILURE_LINKED` | `FAIL-FULL-RING-REJECTS-INGESTION` |
| `PAT-PACK-NONEMPTY-SPARSE-ROWS` | `ANALYTICAL_TEST_LINKED` | `FAIL-SPARSE-ROW-ADVANTAGE-VANISHES` |
| `PAT-PARTITION-UPDATES-BY-DESTINATION` | `SOURCE_FAILURE_LINKED` | `FAIL-ALLACTIVE-MUTATIONS-AMPLIFY-LOGGING`, `FAIL-INTERVAL-LOG-EXCEEDS-MEMORY` |
| `PAT-PIPELINE-ASYNC-IO-COMPUTE` | `SOURCE_FAILURE_LINKED` | `FAIL-SPECULATIVE-READS-SATURATE-DEVICE`, `FAIL-SYNCHRONOUS-BARRIERS-DEFEAT-PIPELINE` |
| `PAT-PIPELINE-ASYNCHRONOUS-DISK-READS` | `ANALYTICAL_TEST_LINKED` | `FAIL-SERIAL-DEPENDENCIES-ELIMINATE-OVERLAP` |
| `PAT-PLACE-SCALE-GROWING-STATE` | `SOURCE_FAILURE_LINKED` | `FAIL-ESTIMATOR-OMITS-PEAK-OVERHEAD`, `FAIL-TIGHT-MEMORY-INCREASES-RUNTIME` |
| `PAT-PREFETCH-CANDIDATE-BLOCKS-ASYNCHRONOUSLY` | `SOURCE_FAILURE_LINKED` | `FAIL-SPECULATIVE-PREFETCH-READ-AMPLIFICATION` |
| `PAT-PREFETCH-DISPLACED-SEARCH-STATE` | `ANALYTICAL_TEST_LINKED` | `FAIL-LATE-PREFETCH-MISSES-REUSE` |
| `PAT-PRIORITIZE-RESIDENT-ACTIVE-BLOCKS` | `SOURCE_FAILURE_LINKED` | `FAIL-RESIDENT-REUSE-STARVES-PRIORITIES` |
| `PAT-PROBE-QUANTIZED-TOPOLOGY-COMPATIBILITY` | `ANALYTICAL_TEST_LINKED` | `FAIL-SAMPLED-PROBE-MISCLASSIFIES-DRIFT` |
| `PAT-PROBE-SMALLEST-SET-FIRST` | `ANALYTICAL_TEST_LINKED` | `FAIL-SYMMETRIC-SETS-REVERSE-COST` |
| `PAT-PRUNE-NEIGHBORS-BY-DIVERSITY` | `ANALYTICAL_TEST_LINKED` | `FAIL-DIVERSITY-PRUNING-CONNECTIVITY-LOSS` |
| `PAT-PRUNE-SETTLED-SEARCH-STATE` | `ANALYTICAL_TEST_LINKED` | `FAIL-LATE-SETTLEMENT-ERASES-PRUNING` |
| `PAT-PUSHBACK-LARGE-RESIDUALS-LOCALLY` | `SOURCE_FAILURE_LINKED` | `FAIL-HIGH-INDEGREE-DESTROYS-RUNTIME` |
| `PAT-REACTIVATE-CHANGED-NEIGHBORS-ONLY` | `ANALYTICAL_TEST_LINKED` | `FAIL-EARLY-THRESHOLD-STOPS-STABILIZATION` |
| `PAT-RECOMPUTE-NEIGHBOR-WEIGHTS-LOCALLY` | `SOURCE_FAILURE_LINKED` | `FAIL-THREAD-VECTORS-EXCEED-BUDGET` |
| `PAT-RECYCLE-SURVIVORS-DURING-AGING` | `SOURCE_FAILURE_LINKED` | `FAIL-AGING-CAPACITY-ABORTS-INSERTIONS`, `FAIL-AGING-SURVIVOR-CAPACITY-OVERRUN` |
| `PAT-REFINE-COMMUNITIES-BEFORE-AGGREGATION` | `ANALYTICAL_TEST_LINKED` | `FAIL-EARLY-STOPPING-WEAKENS-GUARANTEES` |
| `PAT-REFINE-ENDPOINT-SAMPLES-RECURSIVELY` | `ANALYTICAL_TEST_LINKED` | `FAIL-RECURSION-FANOUT-AMPLIFIES-STATE` |
| `PAT-REFINE-HASHED-CANDIDATES-EXACTLY` | `ANALYTICAL_TEST_LINKED` | `FAIL-NONMONOTONE-OPERATORS-BREAK-PRESERVATION` |
| `PAT-RELABEL-VERTICES-FOR-LOCALITY` | `SOURCE_FAILURE_LINKED` | `FAIL-FULL-REORDER-DOMINATES-TRAVERSAL` |
| `PAT-RELOG-PREDICTED-ACTIVE-EDGES` | `ANALYTICAL_TEST_LINKED` | `FAIL-ACTIVE-EDGE-PREDICTION-MISS` |
| `PAT-REPLENISH-IO-EACH-COMPLETION` | `SOURCE_FAILURE_LINKED` | `FAIL-SATURATED-PIPELINE-AMPLIFIES-CONTENTION` |
| `PAT-REUSE-LOADED-SUBGRIDS-SELECTIVELY` | `ANALYTICAL_TEST_LINKED` | `FAIL-SUBGRID-SKIP-BREAKS-CONVERGENCE` |
| `PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY` | `ANALYTICAL_TEST_LINKED` | `FAIL-DENSE-MOVES-SATURATE-QUEUE` |
| `PAT-ROUTE-FILTERS-BY-COST` | `SOURCE_FAILURE_LINKED` | `FAIL-CLUSTERED-FILTERS-MISROUTE-PLANS` |
| `PAT-SCAN-RARE-FILTER-FREQUENT` | `ANALYTICAL_TEST_LINKED` | `FAIL-CORRELATED-LABEL-FILTER-REVERSAL` |
| `PAT-SCHEDULE-SUBGRIDS-BY-DEPENDENCY` | `ANALYTICAL_TEST_LINKED` | `FAIL-PREMATURE-SUBGRID-READS-STALE` |
| `PAT-SEARCH-EVERY-FETCHED-RECORD` | `SOURCE_FAILURE_LINKED` | `FAIL-PAGESEARCH-COMPUTE-IDLES-DEVICE` |
| `PAT-SELECT-HIGH-RESIDUAL-SUBGRAPHS` | `ANALYTICAL_TEST_LINKED` | `FAIL-DIFFUSE-RESIDUALS-ERASE-SPEEDUP` |
| `PAT-SELECT-PARTITION-SCATTER-MODE` | `SOURCE_FAILURE_LINKED` | `FAIL-LARGE-PARTITIONS-THRASH-CACHE`, `FAIL-WRONG-SCATTER-MODE-WASTES` |
| `PAT-SHRINK-VISITED-PARTITION-BOUNDS` | `SOURCE_FAILURE_LINKED` | `FAIL-PARTITION-BOUND-SHRINK-STAGNATION` |
| `PAT-SKIP-FINALIZED-VECTOR-CHUNKS` | `SOURCE_FAILURE_LINKED` | `FAIL-EARLY-ITERATIONS-PAY-CHECKS` |
| `PAT-SKIP-INACTIVE-BINS-HIERARCHICALLY` | `ANALYTICAL_TEST_LINKED` | `FAIL-DENSE-ACTIVITY-OVERLOADS-LISTS` |
| `PAT-SORT-THEN-WRITE-DISTANCES` | `ANALYTICAL_TEST_LINKED` | `FAIL-LARGE-FRONTIERS-OVERWHELM-SORTING` |
| `PAT-STORE-RANDOM-WALK-ENDPOINTS` | `SOURCE_FAILURE_LINKED` | `FAIL-LONG-WALKS-MULTIPLY-SCANS`, `FAIL-TIED-RANKS-RESIST-SAMPLING` |
| `PAT-STREAM-PARTITIONED-UPDATES-SEQUENTIALLY` | `SOURCE_FAILURE_LINKED` | `FAIL-CACHE-PARTITION-SPILL-REVERSAL` |
| `PAT-STREAM-SPARSE-KEEP-DENSE` | `ANALYTICAL_TEST_LINKED` | `FAIL-DENSE-COLUMN-EXCEEDS-MEMORY` |
| `PAT-SWITCH-TRAVERSAL-BY-FRONTIER` | `ANALYTICAL_TEST_LINKED` | `FAIL-FRONTIER-HEURISTIC-MISSELECTS-DIRECTION` |
| `PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH` | `ANALYTICAL_TEST_LINKED` | `FAIL-LONG-PATHS-REPEAT-SCANS` |
| `PAT-TRACK-VISITED-WITH-BITMAPS` | `SOURCE_FAILURE_LINKED` | `FAIL-BITMAP-OVERHEAD-OUTWEIGHS-LOCALITY` |
| `PAT-TUNE-IDLE-WINDOW-UTILIZATION` | `SOURCE_FAILURE_LINKED` | `FAIL-FEEDBACK-LATENCY-TARGET-OVERSHOOT` |
<!-- G06_PATTERN_MATRIX_END -->

## Evidence Conflicts

No qualifying two-sided evidence conflict was found.

The lanes did report workload reversals and source limitations, but those were
conditioned failure boundaries on the same mechanisms rather than mutually
incompatible claims satisfying the conflict ledger's two-endpoint contract.
Six cross-lane card rediscoveries were byte-identical and two additional pairs
described the same source, trigger, mechanism, symptom, and failure boundary;
all eight were merged rather than treated as evidence disagreement.
The empty ledger therefore preserves its exact header and does not assert that
the wider literature is conflict-free.

## A007 Decision Yield

G06 changes A007 from a size-only estimate into a conditional admission
obligation. A later implementation must be able to identify, estimate, monitor,
and receipt at least the following classes of terms when they apply:

| A007 stage | Failure-derived obligation |
|---|---|
| Admission | Reject or specialize when degree skew, frontier density, identifier width, active-set density, update/query ratio, output size, or approximation tolerance falls outside a verified envelope. |
| Bounded plan | Account for primary representation, temporary state, sort and merge buffers, queues, replicated guidance data, preprocessing, speculative reads, and concurrency state. |
| Execution | Observe frontier or residual evolution, read amplification, queue growth, cache or block residency, contention, convergence, and candidate recall rather than assuming the initial estimate remains valid. |
| Verification | Compare exact outputs or independent oracles where required and make approximation loss observable when exactness is intentionally relaxed. |
| Receipt | Record admitted assumptions, resource ceilings, selected guard or fallback class, observed high-water terms, completion or refusal reason, and unresolved measurement constants. |

The decision yield is `ADMISSION_TERMS_AND_FALSIFIERS_EXTRACTED`. G06 does not
choose how these obligations are implemented. It supplies the failure evidence
that G07 may use for constraint transfer, G08 may use after divergent candidate
generation, and G09 may turn into executable experiments.

## Explicit Unknowns

1. The corpus supports no portable numeric crossover for any failure card.
2. Hardware-specific constants for cache, storage, prefetch depth, queueing,
   bandwidth, synchronization, and decompression remain unmeasured.
3. The cost of preparing specialized layouts must be measured against reuse
   count and update invalidation; paper-local ratios cannot be imported.
4. Analytical fixtures have independent oracles but have not been executed.
5. Approximate-search and sampling failures need target-distribution recall and
   accuracy thresholds supplied by a product contract.
6. The conflict ledger's emptiness is scoped to the frozen 25-paper corpus and
   the strict two-sided conflict schema.
7. Thirty-two cards explicitly include `MEASURE_UNKNOWN`; the remaining cards
   still contain symbolic coefficients whose values are not campaign evidence.

## Scope Boundary

G06 created no transfer card, architecture identity, Pareto candidate,
experiment packet, implementation, benchmark, or product performance estimate.
It made no network request and added no paper identity. Repair options remain
classes of possible responses rather than dispositions. The report makes no
claim that any failure is universal beyond its stated graph, workload,
resource, and execution conditions.

## Verification Handoff

The final semantic controller pass is green: 25 paper rows, 67 pattern rows, 79
canonical cards, 174 pointers, and zero conflict rows validate. All 92 plan rows
bind reader identity, independent reviewer identity
`019ff4ec-552b-7a10-bab1-0a7742bae998`, and uppercase SHA-256 result receipts.

Independent review required three frozen passes. The first found six P1 and one
P2 defects; the second found two remaining P1 source-scope defects; the third
recomputed page coverage, foreign keys, claim labels, pointer support, symbolic
breakpoints, fixture oracles, duplicate merges, conflict treatment, scope
boundaries, and the complete validation suite. Its final verdict is `CLEARED`
with P0=0, P1=0, and P2=0. The review evidence is preserved in
`governance/reviews/G06-adversarial-review.md`.

G06 is complete, verified, and independently cleared. It stops here. G07
constraint transfer remains a recommended but separately authorized goal.

The final controller verification ran 45 focused G06 tests and the complete
203-test G00-G06 suite with zero failures. The production corpus validator
returned `PASS arxiv corpus contract`; Git whitespace, ignored-full-text,
tracked-PDF, license-state, later-goal, external-request, and Markdown-index
gates also passed.
