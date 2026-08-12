# G07 Constraint Transfer Report

## Executive Decision

G07 finds a coherent transfer family, but it does **not** find one universal
storage architecture.

The 20 selected mechanisms all retain an operational invariant after G06, so
all 20 receive `TRANSFER_CREATED`. The result is not “these 20 optimizations
work.” The result is narrower and more useful:

> Knight Bus can express algorithm execution as a collection of guarded plan
> branches. Each branch declares an admission unit, a symbolic resource
> envelope, a crossover condition, an exactness or approximation contract, a
> fallback, and receipt counters that determine whether its premise held.

This extends A007's product contract:

```text
artifact + algorithm + budget + semantics
                  |
                  v
      guarded algorithm-layout branches
                  |
                  v
       fit / spill / approximate / refuse
                  |
                  v
       execute + verify + receipt + learn
```

G08 should therefore evolve **multiple guarded algorithm-layout genomes**, not
choose a single universal graph store. G07 stops before constructing or ranking
those genomes.

- Frozen mechanisms: 20
- Terminal dispositions: 20
- Canonical transfer cards: 20
- External requests: 0
- New papers: 0
- New repositories: 0
- G08 candidates created: 0
- G09 experiments run: 0

All canonical cards are labeled `SPECULATIVE_TRANSFER`. The source mechanisms
are paper-backed at their stated evidence grades, but no Knight Bus transfer is
measured, reproduced, or architecture-selected by G07.

## Corpus And Method

G07 began from the completed and cleared G05/G06 corpus:

- 67 mechanism cards;
- 79 failure cards;
- 20 frozen decision-relevant mechanisms;
- four disjoint reader lanes of five mechanisms;
- every selected mechanism's complete G06 failure set;
- the A007 hard-budget and receipt thesis;
- current Knight Bus implementation anchors for Bolt receipts, GDS-style
  estimate terms, and bounded snapshot construction.

Each lane reconstructed the original constraint profile by constrained
resource, access medium, predictability requirement, data mutability,
communication model, and hardware/operating assumptions. It then separated:

1. a source-backed invariant;
2. historical assumptions that should not be carried forward unchanged;
3. modern Knight Bus constraints;
4. a speculative operational transfer;
5. symbolic RAM, I/O, preprocessing, storage, and concurrency terms;
6. unknown constants for G09;
7. every applicable G06 failure and required response;
8. the smallest falsifier.

The controller revalidated all four dossiers before canonicalization. No
historical benchmark ratio was imported as a modern estimate. Modern-hardware
statements below are derived operating hypotheses for G08/G09, not claims from
the original papers.

## Transfer Dispositions

| Source mechanism | Canonical transfer | Primary decision use | G06 boundary retained |
|---|---|---|---|
| `PAT-BOUND-SEARCH-CANDIDATE-FRONTIER` | `XFER-BOUND-SEARCH-FRONTIER-STATE` | Countable ANN/kNN candidate and visited-state admission | A cap does not guarantee recall |
| `PAT-DECOMPOSE-DIFFUSION-INTO-STAGES` | `XFER-STAGE-LINEAR-DIFFUSION-STATE` | Stage-local PageRank/diffusion peak state | Residual pruning is not exact decomposition |
| `PAT-PRUNE-SETTLED-SEARCH-STATE` | `XFER-PRUNE-FINALIZED-TRAVERSAL-STATE` | Release proof-finalized BFS state | Late settlement may not repay rewrite work |
| `PAT-THRESHOLD-INMEMORY-SKETCH-GROWTH` | `XFER-CAP-STREAMING-TRAVERSAL-SKETCH` | Fixed exact-BFS resident sketch | Long paths may multiply scans |
| `PAT-REVISIT-CHANGED-NEIGHBORHOODS-ONLY` | `XFER-REQUEUE-DEPENDENCY-AFFECTED-STATE` | Bounded affected-node queue for communities | Dense moves can saturate the queue |
| `PAT-COMPRESS-SORTED-ID-STREAMS` | `XFER-GUARD-COMPRESSED-ID-STREAMS` | Lossless compressed iterative edge streams | Lifecycle cost can erase compression gain |
| `PAT-INLINE-LOW-DEGREE-ADJACENCIES` | `XFER-GUARD-INLINED-ADJACENCY-THRESHOLD` | Degree-shaped RAM/SSD placement | The inlined side can dominate RAM |
| `PAT-DECOUPLE-NAVIGATION-VECTOR-BLOCKS` | `XFER-PROBE-SPLIT-NAVIGATION-VECTORS` | Coupled/split/mixed kNN records | Split records can double reads |
| `PAT-PACK-NONEMPTY-SPARSE-ROWS` | `XFER-GUARD-SPARSE-ROW-PACKING` | Exact sparse row packing for SpMM/FastRP | Row metadata and conversion can dominate |
| `PAT-INFER-UNWEIGHTED-EDGE-VALUES` | `XFER-ELIDE-DERIVABLE-EDGE-VALUES` | Semantics-gated omission of edge values | Weighted operations require explicit values |
| `PAT-PARTITION-UPDATES-BY-DESTINATION` | `XFER-BOUND-DESTINATION-UPDATE-LOGS` | Hot-interval bounded update staging | All-active work and hot logs reverse the win |
| `PAT-PIPELINE-ASYNC-IO-COMPUTE` | `XFER-BOUND-ASYNC-IO-PIPELINE` | Explicit pool, queue, and speculation caps | Saturation and barriers defeat overlap |
| `PAT-PRIORITIZE-RESIDENT-ACTIVE-BLOCKS` | `XFER-CAP-RESIDENT-BLOCK-REUSE` | Locality reuse with fairness cap | Reuse can starve global priorities |
| `PAT-BALANCE-PHASE-SPECIFIC-WORKLOADS` | `XFER-BOUND-PHASE-WORK-SCHEDULING` | Phase-shaped BFS/WCC work assignment | Coordination can exceed balance benefit |
| `PAT-SELECT-PARTITION-SCATTER-MODE` | `XFER-CALIBRATE-PARTITION-SCATTER-MODE` | Per-partition active/streaming mode switch | Wrong mode and cache thrash reverse the win |
| `PAT-EXPLORE-SUPERSET-VERIFY-RESULTS` | `XFER-VERIFY-SUPERSET-RESULTS-EXACTLY` | Approximate filter plus exact output gate | False negatives break correctness; false positives amplify work |
| `PAT-NAVIGATE-BINARY-RERANK-EXACTLY` | `XFER-NAVIGATE-COMPACT-RERANK-EXACTLY` | Compact navigation plus exact vector rerank | Incompatible geometry collapses reachability |
| `PAT-STREAM-SPARSE-KEEP-DENSE` | `XFER-STREAM-SPARSE-RETAIN-DENSE` | Sparse-streamed SpMV/SpMM with a dense resident unit | One complete dense unit may not fit |
| `PAT-REFINE-COMMUNITIES-BEFORE-AGGREGATION` | `XFER-REFINE-COMMUNITIES-PRESERVE-CONNECTIVITY` | Leiden-style refinement and guarantee tiers | Early stop cannot claim later convergence guarantees |
| `PAT-BALANCE-BUCKETED-PACKED-SETS` | `XFER-BALANCE-PACKED-ADJACENCY-SETS` | Exact packed intersections for triangles/similarity | Wide fields and directories erase packing benefit |

## 1. Historical Constraints Still Relevant

### 1.1 Peak live state remains distinct from data size

The recurring constraint is not merely “the graph is large.” It is that a
particular phase needs topology, algorithm vectors, queues, worker-local state,
buffers, mapping tables, and scratch at the same time. Faster storage does not
remove this coexistence. A007 must estimate the phase maximum rather than input
bytes or one named data structure.

### 1.2 The access unit remains architectural

Pages, blocks, compressed words, destination intervals, tiles, adjacency lists,
and dense columns remain meaningful because execution consumes data in units.
The exact device changes; the need to align representation with the algorithm's
read and update unit survives.

### 1.3 Skew still determines the real bound

Average degree, average frontier size, average update volume, and average
selectivity are insufficient. Hot destination intervals, high-degree vertices,
dense community moves, weak filters, and repeated block reactivation determine
peak state and tail work. Admission needs histograms, maxima, quantiles, or an
explicit conservative cap.

### 1.4 Conversion and preparation still need amortization

Sorting IDs, relabeling vertices, building split layouts, packing sparse rows,
encoding signatures, and preparing partitions are not free. The relevant
question remains whether reuse repays preprocessing, temporary storage, and
coexistence of old and new forms.

### 1.5 Correctness conditions survive hardware changes

Lossless decoding, exact value derivation, no-false-negative filtering, exact
reranking, dependency closure, proof-finalization, linear diffusion identities,
and convergence witnesses are semantic invariants. NVMe, mmap, SIMD, more cores,
or a different runtime cannot relax them.

### 1.6 Synchronization and backpressure remain resource terms

Async queues, barriers, work stealing, block-state machines, and worker-local
buffers determine both latency and RAM. Concurrency is not a free multiplier;
it creates state multiplicity and ordering constraints that must appear in the
quote.

## 2. Invariants That Survive

The 20 cards yield six reusable invariant classes.

| Invariant class | Surviving rule | Examples |
|---|---|---|
| Explicit capacity | State cannot exceed a declared count if every insertion and overflow path enforces it | candidate frontier, sketch slots, async pool, update-log split |
| Exact representation | A transformed encoding reconstructs the same logical object under named preconditions | XOR streams, implicit unit values, sparse rows, packed sets |
| Exactness wrapper | An approximate stage may narrow work only when an exact stage protects the promised result | superset verification, compact navigation plus exact rerank |
| Dependency closure | Work may be skipped only when all state that can change the result is retained or revisited | changed-neighborhood queue, finalized BFS state |
| Algebraic identity | Staging preserves the result only while all terms required by the identity remain represented | linear diffusion decomposition |
| Guarantee witness | A stronger semantic guarantee is earned by a named completion or convergence condition | community refinement and stop tier |

These classes are more durable than any one paper's layout. G08 can use them as
genome constraints: a candidate may change representation or schedule, but it
cannot omit the invariant that makes the transfer operational.

## 3. Assumptions Reversed By Modern Hardware

This section records `DERIVED_INFERENCE` hypotheses that G09 must calibrate.
They are not sourced performance conclusions.

### 3.1 NVMe narrows, but does not erase, the random/sequential gap

Older external-memory designs often treated random I/O as categorically
prohibitive. Modern NVMe may move some crossovers toward finer-grained reads and
more parallel requests. It does not make every random access cheap: queue depth,
read amplification, page size, cache state, interference, and tail latency
remain unknown. Therefore G08 should keep both stream-oriented and demand-read
branches.

### 3.2 mmap changes ownership, not physical residency

A mapped artifact can avoid explicit copies and simplify addressability, but
mapped bytes may still occupy page cache and fault under pressure. “Not on the
heap” is not “not in RAM.” G08 resource envelopes must charge page-cache
residency or use an enforceable eviction/direct-I/O policy.

### 3.3 SIMD strengthens compact kernels but sharpens width gates

Modern vector instructions make packed IDs, bitmaps, implicit values, and
compact signatures more attractive. The benefit remains conditional on field
width, alignment, decode cost, lane utilization, and fallback behavior. The
word-model invariant transfers; historical speed ratios do not.

### 3.4 Multicore makes worker multiplicity first-class

Parallelism can reduce wall time while increasing peak RAM through duplicated
buffers, queues, partial aggregates, and scratch. G08 must represent
`worker_count * worker_state` and in-flight I/O explicitly, then allow a lower
concurrency plan when the hard RAM budget is more important than latency.

### 3.5 Bounded cloud containers increase the value of refusal

In an opaque or constrained runtime, an estimator cannot assume spare host RAM,
stable page cache, exclusive SSD bandwidth, or unlimited pinned buffers. This
does not prove a market. It does make conservative headroom, hard limits,
fallbacks, and early refusal part of correctness for the resource contract.

### 3.6 Fast I/O raises the relative cost of software overhead

As device latency falls, sorting, encoding, queue management, synchronization,
cache restoration, and redundant work can become the crossover. Every storage
transfer therefore includes lifecycle CPU work and scheduler state rather than
modeling only bytes moved.

## 4. Transfers For RAM And Predictability

### 4.1 Hard-count transfers

The strongest predictability candidates expose user-visible count limits:

- `XFER-BOUND-SEARCH-FRONTIER-STATE`: candidate, result, visited, and worker
  caps;
- `XFER-CAP-STREAMING-TRAVERSAL-SKETCH`: `(K + 1) * n` edge-slot cap plus
  node attributes;
- `XFER-BOUND-DESTINATION-UPDATE-LOGS`: hot-interval records, page buffers, and
  sort workspace;
- `XFER-BOUND-ASYNC-IO-PIPELINE`: pool blocks, submit/completion queues,
  speculation, and workers;
- `XFER-CAP-RESIDENT-BLOCK-REUSE`: fixed pool plus active-frontier and worker
  activation state.

These are attractive because enforcement can compare observed high-water
counts with the admitted variables. They do not by themselves bound total RSS;
runtime, allocator, page-cache, and kernel terms remain calibration constants.

### 4.2 Representation transfers

The primary lower-RAM possibilities are conditional encodings and placements:

- compressed sorted IDs;
- degree-banded adjacency inlining;
- split or mixed navigation/vector blocks;
- nonempty sparse rows;
- derivable-value elision;
- compact navigation signatures;
- packed adjacency suffixes.

Each is exact or exact-at-output under its card's semantic precondition. Each
also has a reference representation fallback. G08 should treat these as artifact
variants selected by data profiling, not as one on-disk format.

### 4.3 Work-elision transfers

Finalized-state pruning, affected-neighborhood queues, staged diffusion, phase
scheduling, scatter-mode selection, and resident-block reuse can reduce active
work. Their predictable **RAM** value is weaker than their work/I/O value
because many retain the full conservative backing state. Receipts must separate
“bytes reserved” from “work skipped.”

### 4.4 Exactness-preserving approximate transfers

`XFER-VERIFY-SUPERSET-RESULTS-EXACTLY` and
`XFER-NAVIGATE-COMPACT-RERANK-EXACTLY` are not permission to call approximate
search exact. The exact promise applies only to final predicate verification or
reranking inside the reached candidate set. Candidate reachability and recall
remain separately scoped and falsifiable.

### 4.5 Minimum-resident-unit transfer

`XFER-STREAM-SPARSE-RETAIN-DENSE` gives G08 a useful refusal primitive: if one
complete dense column partition plus bounded worker buffers does not fit, the
semi-external plan is inadmissible. This is stronger than hoping spill will
rescue an algorithm after it starts.

## 5. Attractive Transfers Killed By G06

No selected mechanism is fully rejected after narrowing, but G06 kills the
following attractive **unqualified** proposals:

| Killed proposal | Why it is killed | Surviving form |
|---|---|---|
| A fixed ANN frontier guarantees target recall | Some graphs reach a recall ceiling under the cap | Cap is a resource contract; recall is a separate envelope with fallback |
| Pruning PageRank residuals preserves exact staged diffusion | Exact linear decomposition needs every nonzero continuation | Full residual support for exact mode; pruning only under an approximation contract |
| Settled-state pruning always reduces BFS cost | Late settlement can fail to repay reduction and reconstruction | Online payback guard plus unpruned stream |
| A bounded BFS sketch implies bounded runtime | High diameter or repeated corrections multiply scans | Separate resident-state and scan/rewrite envelopes |
| Changed-neighborhood queues are always sparse | Dense moves can approach a global sweep | Queue cap and switch to deduplicated sweep |
| Compression is always lower-cost | Sort/encode/decode/merge and poor deltas erase the benefit | Compressibility/reuse probe plus plain stream |
| Low-degree inlining always saves RAM and I/O | Resident mini-adjacencies or metadata can dominate | Histogram-selected threshold plus overflow/reference layout |
| Split ANN records always reduce reads | Weak pruning or small vectors require two fetches | Coupled/split/mixed probe |
| Async I/O always hides latency | Saturated devices and barriers leave no useful overlap | Bounded demand-first pipeline or synchronous fallback |
| Resident-first scheduling always improves locality | Reuse can delay higher-priority work and add repeats | Consecutive-reuse cap and fairness fallback |
| Approximate filtering plus exact verification is universally correct | False negatives never reach verification | Only no-false-negative selectors, otherwise exact path |
| Binary navigation plus exact rerank preserves exact neighbors | Bad geometry may never reach the right candidates | Compatibility probe and alternative navigation |
| Fixed community iterations imply convergence guarantees | Stronger guarantees require their own witnesses | Receipt names the actual stop and guarantee tier |
| Packed set operations are universally faster | Wide fields, directories, and conversion erase word parallelism | Measured width/balance guard plus scalar exact fallback |

This is G06's largest contribution to G07: it turns mechanisms into branches
with crossovers rather than slogans.

## 6. Constants G09 Must Measure

Every card records exact symbol names. G09 should organize them into calibration
bundles rather than run 100 unrelated microbenchmarks.

| Bundle | Representative unknowns | Why it changes admission |
|---|---|---|
| Process baseline | runtime RSS, allocator headroom, filesystem and mapped-page charge | Converts structure bytes into whole-process peak |
| Representation widths | candidate, queue, edge-slot, ID, bitmap, metadata, padding bytes | Determines the slope of RAM and storage equations |
| Worker multiplicity | per-worker buffers, queues, scratch, partial state | Determines RAM/latency trade-off under concurrency |
| Physical I/O amplification | cached/cold read amplification, reloads, spill, write amplification | Converts logical records into device bytes and tail risk |
| Codec lifecycle | sort, encode, decode, merge, conversion rates and scratch | Determines whether compact formats repay preparation |
| Sparsity and skew | degree histogram, frontier density, hot-interval size, nonempty-row ratio | Selects layout and scheduler branches |
| Filter/selectivity | false-positive expansion, candidate uniqueness, pruning rate | Bounds deferred exact work and page traffic |
| Geometry compatibility | compact-versus-exact neighborhood reachability | Gates binary or quantized navigation |
| Convergence behavior | residual support, community move density, path/correction count | Bounds iterations, queues, and scan envelopes |
| Scheduler/device crossover | queue saturation, useful overlap, partition cache fit, reuse streak payback | Selects async, scatter, phase, and resident-reuse modes |

The minimum G09 method should measure both a controlled fixture and a holdout,
record cold and warm conditions separately, and compare predicted versus actual
whole-process peak, bytes, wall time, result checksum, and selected fallback.
That is a proposed experiment discipline, not a G09 experiment created here.

## 7. Architecture Vocabulary For G08

G07 contributes the following architecture primitives.

| Term | Meaning in G08 |
|---|---|
| **Admission unit** | Smallest state that must fit before a branch can start, such as one dense column or one interval sort buffer |
| **Resource envelope** | Symbolic upper model for RAM, I/O, preprocessing, storage, and concurrency, including unknown coefficients |
| **Crossover guard** | Pre-run or online predicate that chooses the specialized branch only when its premise is supported |
| **Reference fallback** | Correct baseline representation or schedule retained when specialization fails |
| **Fallback ladder** | Ordered choice among fit, lower concurrency, streaming, spill, approximate, reference, and refuse |
| **Exactness envelope** | Precise boundary of what remains exact, what is approximate, and which oracle verifies output |
| **Minimum resident kernel** | Irreducible live state below which the selected algorithm cannot execute correctly |
| **State multiplicity vector** | Base state plus per-worker, per-partition, per-stage, and in-flight copies |
| **Prepared artifact variant** | Reusable graph representation whose build peak, persistent bytes, and amortization horizon are quoted |
| **Algorithm-layout capsule** | One algorithm family, artifact variant, scheduler, resource equation, guard, fallback, and receipt schema |
| **Runtime switch** | Receipt-visible transition between specialized and reference modes when observed conditions reverse |
| **Guarantee tier** | Result property earned by a named stop condition, such as early-stopped partition versus stable community partition |
| **Bound certificate** | Pre-run variables and post-run high-water counters sufficient to audit the resource contract |
| **Calibration debt** | Unknown coefficients that prevent a branch from being admitted as predictable until G09 measures them |

The key G08 design question is not “which storage format wins?” It is “which
small set of capsules spans the useful RAM/latency/accuracy Pareto surface for
each priority algorithm without exploding preparation and maintenance cost?”

## 8. Explicit Non-Transfers

G07 does **not** transfer or establish any of the following:

1. A complete Neo4j or Neo4j GDS rewrite architecture.
2. One universal storage format for all graph algorithms.
3. A claim that Rust, mmap, io_uring, NVMe, SIMD, or multicore alone improves
   latency or RAM.
4. A numeric RAM or latency delta versus Neo4j/GDS.
5. A guarantee that file-backed or mapped state is absent from physical RAM.
6. Exact nearest-neighbor recall from a bounded or compact navigation stage.
7. Convergence guarantees from arbitrary iteration limits.
8. Unbounded worker parallelism under a bounded-memory contract.
9. A promise that spill always completes faster or cheaper than refusal.
10. Product-market evidence that customers will pay for receipts.

There are also no selected-pattern non-transfers at G07 closure: all 20 retain a
surviving invariant and become guarded cards. This is not a positive performance
verdict. It means each is sufficiently operational and falsifiable to enter the
G08 option arena.

## G08 Recommendation

Recommendation: `PROCEED_TO_G08`

G08 should consume the 20 cards as a constraint library and generate multiple
algorithm-layout candidates. It should require every candidate to declare:

1. priority algorithm family and semantics;
2. admission unit and total resource envelope;
3. prepared artifact variants and coexistence peak;
4. crossover guards and calibration dependencies;
5. fallback ladder and refusal point;
6. exactness/approximation envelope and independent oracle;
7. receipt fields and estimator-error feedback;
8. linked G06 failures and G09 falsifiers.

G08 should not begin until the independent G07 review clears the cards, plan,
and this report. G07 creates no G08 candidate and stops after that clearance.
