# SUM01: What docs_PRD03 Is About (ELI5, ASCII edition)

This folder is the planning brain for **Knight Bus v003**: rewriting Neo4j in
Rust so that huge graphs (50 GB-class) can be analyzed on small machines
(8 GB-class RAM), while existing Neo4j/GDS client code keeps working unchanged.

No production code lives here. It is research, contracts, and readiness plans.

## The One Big Idea

There are two very different jobs a graph database does, and v003 refuses to
mix them:

```text
OLTP = the busy kitchen        OLAP = the printed cookbook
  (live writes, transactions,    (read-only, optimized, snapshot
   locks, point reads)            of the kitchen at time W)
```

Between them sits a **factory** that turns kitchen output into cookbooks:

```text
             READ/WRITE PATHS

OLTP query/write
      |
      v
+-----------------------------+
| Neo4j-shaped OLTP storage   |   <-- the source of truth
| records / WAL / tx / locks  |       users read+write here (OLTP)
+-----------------------------+
      |
      | committed facts / receipts
      v
+-----------------------------+
| Projection Build Store      |   <-- the FACTORY
| analytical IR / build plane |       users NEVER query this
+-----------------------------+
      |
      | compile / validate / publish (atomic)
      v
+-----------------------------+
| OLAP snapshot W             |   <-- immutable, low-RAM format
| flat CSR / sidecars / cells |       users read here (OLAP/GDS)
+-----------------------------+
      |
      v
OLAP answers, exact as of watermark W
```

ELI5 with a metaphor the docs use themselves:

```text
OLTP records       = source code
Projection Store   = compiler intermediate representation (IR)
OLAP snapshot      = optimized machine code
OLAP runtime       = CPU executing that machine code
```

Or the foundry version:

```text
OLTP emits ore.
The Build Store refines ore into standard ingots.
Snapshot compilers cast ingots into specialized tools:
  topology snapshots, property sidecars, result/model sidecars,
  memory estimates, catalog manifests.
```

## The Hard Rules (Non-Negotiables)

```text
1. Existing Neo4j/GDS client code needs ZERO changes where support is claimed.
2. OLTP reads/writes stay on Neo4j-shaped OLTP storage. Always.
3. OLAP/GDS reads open ONLY published, immutable snapshots.
4. The middle Build Store is a factory, never a third database users query.
5. Freshness = publish a newer snapshot (W -> W+1).
   NEVER merge fresh writes into a running OLAP query.
6. Snapshot publication is atomic: readers see all of W or all of W+1,
   never half-built files.
7. RAM accounting is HOLISTIC: heap + RSS + page cache + mmap residency +
   scratch + sidecars + algorithm state. If the budget cannot fit,
   reject BEFORE running.
8. Single-node deployment for the community target.
```

## Map of the Folder

```text
docs_PRD03/
|
|-- prd-l1.md ......................... THE TOP. Product constraints and the
|                                       three-plane answer. Everything else
|                                       supports this file.
|
|-- Arch-options.md ................... Decision ledger of OLAP storage options
|                                       (A: flat CSR -> B: +sidecars ->
|                                        C: cellular CSR -> D: hybrid ->
|                                        E: multi-generation catalog).
|                                       Verdict: A is the MVP, B is required
|                                       next, E is required ops, C/D only if
|                                       measurements justify them.
|
|-- github-repo-longlist.md ........... Scored list of public repos worth
|                                       studying (neo4j=100, gds=98, ...).
|
|-- V003-Reference-Folder-Learning-Spec.md
|                                       Contract for HOW to study the local
|                                       gitrefrepo/ reference clones: every
|                                       claim needs source path + symbol +
|                                       why it matters. No vague summaries.
|
|-- GDS-PRD-L1-Evidence-Dossier-Executable-Spec.md
|-- GDS-PRD-L1-Evidence-Dossier-v2-Executable-Spec.md
|                                       Specs for mining the GDS source into
|                                       evidence dossiers (v1 baseline, v2
|                                       comprehensive with TSV tables).
|
|-- Reference-Learning-Critique-Gaps.md Honest self-critique: "the research
|                                       shelf is strong; the implementation
|                                       contract is still thin."
|
|-- Gap-Closure-Executable-Spec.md ..... Requirements to close those gaps.
|-- Gap-Closure-Implementation-Plan.md . Phase 0-5 plan that produced the
|                                        implementation-readiness/ folder.
|
|-- reference-learning/ ................ OUTPUT of the study passes:
|       GDS-Public-Surface-Inventory.tsv   (575 gds.* procedures found)
|       Batch-04/05/07/08/10 notes         (publication, compat boundary,
|                                           low-RAM priors, hard GDS families,
|                                           projection internals)
|       gds-v2-dossiers/                   (per-topic evidence dossiers plus
|                                           verification oracles from GDS tests)
|
`-- implementation-readiness/ .......... OUTPUT of gap closure: buildable
        contracts, not prose:
        GDS-Procedure-Support-Registry.tsv (which of the 575 procedures get
                                            P0/P1/P2/unsupported treatment)
        Memory-Estimate-Formula-Book.tsv   (per-procedure RAM formulas)
        Snapshot-Publication-State-Machine.md (atomic W -> W+1 publish)
        OLTP-Record-Store-Rust-Contract.md
        Projection-Build-Store-Physical-Contract.md
        Neo4j-Compatibility-Canary-Matrix.md
        Benchmark-Proof-Plan.md, Cells-Adoption-Falsifier-Plan.md, tracker...
```

## The Story Arc (How the Folder Evolved)

```text
1. prd-l1.md              "Here is what v003 must be."
2. Arch-options.md        "Here are the storage designs we weighed, and why."
3. Learning specs         "Study real Neo4j/GDS source, with evidence rules."
4. reference-learning/    "Here is what we actually found (575 procedures,
                           memory formulas, publication semantics, ...)."
5. Critique-Gaps          "Good research. Not yet a blueprint. Gaps: no
                           per-procedure support decisions, no memory formula
                           book, no publication state machine, ..."
6. Gap-Closure plan       "Convert research into contracts, phase by phase."
7. implementation-readiness/  "The contracts. Next stop: writing Rust."
```

## Key Anti-Fooling-Ourselves Themes

```text
- "CSR exists" does NOT mean "Neo4j/GDS is supported." The GDS surface is
  575 procedures with configs, results, catalogs, and memory estimates.
- Every unsupported procedure must fail in a deterministic, registered way,
  not with a random crash.
- mmap does not magically give deterministic RAM; residency counts.
- Claims are proven by harnesses and falsifier tests, not by prose.
- Every learning claim must cite a source file and symbol, so a future
  (possibly weaker) agent can re-verify it.
```

## One-Sentence Summary

```text
docs_PRD03 = the blueprint shelf that says: keep Neo4j-shaped OLTP as truth,
manufacture immutable low-RAM OLAP snapshots through a never-queried factory
layer, promise only what a 575-procedure registry and per-procedure memory
formulas can prove, and publish freshness as new snapshot generations.
```
