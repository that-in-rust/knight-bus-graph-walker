# Knight Bus Graph Walker — Knowledge Wiki

*Master index of all research, analysis, and decisions in this repository.*
*60 documents. ~19,000 lines. Every conclusion cross-referenced.*

**Last updated:** 2026-05-25

---

## How to Use This Wiki

1. **Before any new analysis:** Search this index for existing conclusions
2. **Cite, don't re-derive:** If a conclusion exists, reference it by document and line
3. **Only add NEW information:** New docs should extend, correct, or supersede existing ones

---

## Table of Contents

- [I. Proven Facts (Benchmarked, Tested, Committed)](#i-proven-facts)
- [II. Foundational Thesis](#ii-foundational-thesis)
- [III. Neo4j Understanding](#iii-neo4j-understanding)
- [IV. Architecture Decisions](#iv-architecture-decisions)
- [V. PMF & Go-To-Market](#v-pmf--go-to-market)
- [VI. v0.0.3 Implementation Plan](#vi-v003-implementation-plan)
- [VII. Test Journals & Benchmark Evidence](#vii-test-journals--benchmark-evidence)
- [VIII. Playbooks & Process](#viii-playbooks--process)
- [IX. Document Inventory (All 60 Files)](#ix-document-inventory)
- [X. Superseded Conclusions (Corrected)](#x-superseded-conclusions)

---

## I. Proven Facts

These are facts backed by running code, passing tests, or published benchmarks.
Not hypotheses. Not designs. PROVEN.

### Benchmark Results (v0.0.2, 2026-04-16)

Source: `Final-Testing-Journal-v002.md`, `v002-research/competitor-2gb-benchmark.md`

| Dataset | KB p99 | Neo4j p99 | p99 Win | KB RSS | Neo4j RSS | RAM Win |
|---|---|---|---|---|---|---|
| 1 MB | 26 µs | 12.6 ms | 494x | 6.7 MB | 525.9 MB | 78.9x |
| 50 MB | 36 µs | 52.2 ms | 1,439x | 14.5 MB | 616.1 MB | 42.5x |
| 2 GB | 45 µs | 1.51 s | 33,695x | 234.3 MB | 1.07 GB | 4.5x |

**Measurement contract:** Runtime-only RSS (via `getrusage`). Neo4j = server process.
Knight Bus build/verify RSS tracked separately.

**Important caveat:** This compares Knight Bus (direct CSR slice) against Neo4j
Cypher over Bolt — NOT against Neo4j GDS on a projected in-memory graph.
Source: `docs_PRD01/Previous-learnings-01.md` lines 35-41

### Codebase (v0.0.2)

Source: `docs_PRD01/Knight-Bus-Inventory-and-Gap-Analysis.md`

- **4,710 LOC Rust** across 12 source files + 532 LOC tests
- **4 traits:** `WalkQueryRuntime`, `GraphTruth`, `SnapshotBuilder`, `BenchmarkRunner`
- **23 passing tests**, zero clippy warnings
- **Test fixtures:** 39 nodes, 67 edges (`tests/fixtures/valid/`)
- **2 GB corpus:** 4M nodes, 36M edges (`v002-research/`)

### Storage Format (Proven, In Production)

Source: `docs/pre-v002/STORAGE_RUNTIME_ALIGNMENT.md`, `docs_PRD01/Knight-Bus-Inventory-and-Gap-Analysis.md`

```
snapshot/
  manifest.json              metadata (version, counts, file names)
  node_table.bin             16-byte NodeRecords (key_offset: u64, key_len: u32, flags: u32)
  strings.bin                concatenated UTF-8 node key strings
  key_index.bin              sorted dense IDs for binary search
  forward.offsets.bin        u64[node_count + 1]  (CSR offsets)
  forward.peers.bin          u32[edge_count]      (CSR adjacency)
  reverse.offsets.bin        u64[node_count + 1]  (reverse CSR offsets)
  reverse.peers.bin          u32[edge_count]      (reverse CSR adjacency)
```

Read path: `key → binary_search(key_index) → dense_id → offsets[id]..offsets[id+1] → peers[start..end]`

### Build Pipeline (Proven at 2 GB)

Source: `src/low_ram.rs` (1,703 LOC)

- External merge-sort for bounded-memory operation
- Configurable `BuildMemoryBudget` (default 64 MB)
- RSS tracking per build phase
- Verification of snapshot against CSV truth

---

## II. Foundational Thesis

These documents define WHY Knight Bus exists and HOW it should work.
They were written BEFORE the code and remain the governing principles.

### The Core Thesis

Source: `docs/pre-v002/KNIGHT_BUS_THESIS.md` (508 lines)

> "The most practical way to build knight-bus-graph-walker is: build a native
> Rust walk runtime first."

Key principles:
- One frozen graph world, one workload family
- Dual CSR (forward + reverse adjacency) with dense u32 IDs
- Build-time heavy, walk-time boring
- mmap for OS-managed page cache
- Benchmark three things separately: build, key lookup, walking

### Storage-Runtime Alignment Doctrine

Source: `docs/pre-v002/STORAGE_RUNTIME_ALIGNMENT.md` (359 lines)

> "The storage is 'aligned to runtime' only when the hot traversal path
> is already visible in the on-disk bytes."

Two influences:
- **Parseltongue:** dense IDs, dual adjacency, build-time preprocessing
- **Apache Iggy:** payload shaped for read path, tiny sidecar indexes, immutable sealed artifacts

Rules:
- Peers are the payload
- Offsets are the seek aid
- Key lookup is a sidecar concern
- No database or log lookup in the traversal loop
- No reverse-edge derivation at query time

### The Meta Pattern

Source: `docs_PRD02/Storage-Risk-Meta-Pattern.md` (482 lines)

The "mise en place" kitchen analogy:

> General-purpose storage (Neo4j) = a 2,000-page cookbook at service time.
> Operation-aligned storage (Knight Bus) = a professional kitchen with mise en place.

This pattern recurs: row vs column stores, SSR vs SSG, document scan vs inverted index.
The only novelty is applying it to GRAPH traversal specifically.

### ELI5 Documents

Source: `docs/pre-v002/A-20260416*.md` (5 documents, ~1,291 lines total)

These explain the thesis in plain English:
- `storage-runtime-alignment-eli5.md` — "Store like a map, not like a filing cabinet"
- `v001-proof-ladder-eli5.md` — "Prove one narrow thing well"
- `tiny-harness-validation.md` — First 39-node test validation
- `open-path-and-minimum-proof-eli5.md` — Why open is fast: "map and validate, not load and rebuild"
- `rust-vs-neo4j-proof-eli5.md` — First direct Rust-vs-Neo4j benchmark

---

## III. Neo4j Understanding

These documents map what Neo4j IS, what it HAS, and what Knight Bus replaces.

### Neo4j Architecture Map

Source: `docs_PRD01/Neo4j-Architecture-Map.md` (276 lines)

Complete stack diagram: Network → Cypher → Kernel → Storage → Support Subsystems.
Knight Bus replacement map:

| Neo4j Component | Knight Bus Equivalent |
|---|---|
| Record stores (Node/Rel/Prop) | Dual CSR arrays |
| Property index (GBPTree) | Sorted key_index + binary search |
| Page Cache (Muninn) | `mmap` (OS page cache) |
| Transaction engine / WAL | Not needed (immutable snapshots) |
| Cypher parser/planner/runtime | Direct function calls |
| Linked-list traversal | Contiguous array slice |

**Variant designs (already specified):**
- `variant_low_RAM`: aggressive mmap, external merge sort, smaller node table (line 244)
- `variant_low_latency`: pin hot pages, prefetch, inline short keys (line 250)

### Neo4j Component Breakdown

Source: `docs_PRD01/Neo4j-Component-Breakdown.md` (822 lines)

Neo4j = 6 things bundled into one Java process:
1. **DATABASE** (storage engine): ~195K LOC
2. **QUERY LANGUAGE** (Cypher): ~702K LOC (44% of codebase!)
3. **RUNTIME** (execution engine): inside Cypher module
4. **WIRE PROTOCOL** (Bolt): 42K LOC
5. **SERVER** (HTTP + admin): ~34K LOC
6. **TOOLING** (import, shell): ~78K LOC

Total: **1.58M LOC** (989K Java + 536K Scala)

### Neo4j LOC Results

Source: `docs_PRD01/neo4j-loc-results.md` (247 lines)

Per-module breakdown of `community/`. Key modules:
- `cypher/`: 701,841 LOC (the paradox: adoption driver = performance killer)
- `kernel/`: 83,297 LOC
- `record-storage-engine/`: 69,646 LOC
- `bolt/`: 42,064 LOC

### Neo4j Frontend vs Backend Split

Source: `docs_PRD01/Neo4j-Frontend-Backend-Split.md` (704 lines)

**Frontend (must be flawless):** Cypher surface (325K), Bolt (42K), Server (34K), CLI tools (24K), Types/Errors (45K) = ~470K LOC
**Backend (gets Knight Bus treatment):** Kernel (83K), Record Storage (70K), IO/PageCache (14K), Index/GBPTree (13K), WAL (9K), Locks (6K) = ~195K LOC

### Rubber Duck: Frontend/Backend Split

Source: `docs_PRD01/Rubber-Duck-Frontend-Backend-Split.md` (1,281 lines)

The 4 traits, the 23 tests, and what they DO and DON'T prove.
**Appendix B: The 13 Algorithm Storage Atlas** (see below).

### Neo4j's "Obvious Mistake"

Source: `docs_PRD02/PMF-Viral-The-Obvious-Mistake.md` (555 lines)

Neo4j GDS internally uses CSR. But disk format is linked-list records.
So every `gds.pageRank()`:
1. Linked lists on disk (0 sec)
2. REBUILD CSR from linked lists (60-120 sec, 2-4 GB heap) ← THE WASTE
3. Run algorithm on CSR (2-10 sec)
4. Throw away CSR on restart

**Knight Bus:** Store as CSR from the start. Step 2 disappears.

### 1000 IQ Deeper Insight

Source: `docs_PRD02/1000IQ-The-Deeper-Insight.md` (517 lines)

Kills the shallow "obvious mistake" pitch. Why Neo4j CAN'T fix it:
1. 1.58M LOC depends on 15B/34B records
2. JVM trap: `MappedByteBuffer` 2GB limit, GC interferes with mmap, `Unsafe` being deprecated
3. Backward compatibility locks format
4. Innovator's Dilemma: $36K/yr revenue depends on the complexity

**Grafeo** (Rust, 489 stars) already does CSR + mutable overlay (63x less memory, 116x faster).
CSR is table stakes. The REAL moat:

| Layer | What | Who Else | Switchable? |
|---|---|---|---|
| 1: Store CSR | Eliminate projection | Grafeo, LSMGraph | Yes |
| 2: Algorithm-specific CSR | 13 layouts optimized per algorithm | **Nobody** | No (2 years R&D) |
| 3: Adaptive engine | Auto-select OLAP-RAM vs OLAP-Latency | **Nobody** | No |

### Neo4j Multinode — Do We Need It?

Source: `docs_PRD02/Neo4j-Multinode-Do-We-Need-It.md` (377 lines)

**No.** Neo4j GDS (analytics) runs on SINGLE SERVER even in enterprise clusters.
Their own docs: "GDS workloads are not load-balanced."

Three deployment tiers:
- Community: single server, free, no clustering
- Enterprise (~$36K/yr): cluster (HA), but GDS still single-server
- Infinigraph (Sep 2025): property sharding only — topology stays on ONE shard

**DuckDB precedent:** No clustering → $750M+ valuation. "Your laptop is the cluster."
**Conclusion:** Single-server for v0.0.3–v0.1.0. Multi-node only at v1.0+ if users demand.

---

## IV. Architecture Decisions

### The 13 Algorithm Storage Atlas

Source: `docs_PRD02/Storage-Formats-Hope-Not-Blind.md` lines 146-175,
`docs_PRD01/Rubber-Duck-Frontend-Backend-Split.md` Appendix B

> "One universal base format was rejected. One fully bespoke engine per
> algorithm was also rejected. The chosen pattern is a small family of
> reusable layout types plus per-algorithm contracts."

| # | Layout Family | Optimizes For | Algorithms |
|---|---|---|---|
| 1 | **AnchorDualCsr** | Exact anchor → adjacency slice | 4 |
| 2 | **InboundPower** | Repeated inbound score accumulation (PageRank) | 4 |
| 3 | **ConnectivityLowlink** | DFS numbering, lowlinks (Tarjan SCC) | 4 |
| 4 | **OrderedWedge** | Sorted-neighbor intersection (triangle count) | 9 |
| 5 | **PartitionRefinement** | Community assignment (Louvain) | 9 |
| 6 | **PeelBucket** | Low-degree peeling (k-core) | 3 |
| 7 | **RelaxationFrontier** | Weighted frontier relaxation (Dijkstra) | 10 |
| 8 | **EdgeOrderForest** | Globally ordered edge scan (MST) | 2 |
| 9 | **FlowResidual** | Mutable residual arc updates (max flow) | 4 |
| 10 | **FeatureMetric** | Row-major feature distance (k-NN) | 4 |
| 11 | **EmbeddingSample** | Neighborhood sampling (Node2Vec) | 4 |
| 12 | **DagOrder** | Topological replay (longest path) | 2 |
| 13 | **InfluenceMonteCarlo** | Stochastic cascade simulation (CELF) | 1 |

**Status: 1 PROVEN (AnchorDualCsr), 12 DESIGN-ONLY**

### Three Architecture Options (Folder-by-Folder)

Source: `docs_PRD02/Three-Architecture-Options-Folder-By-Folder.md` (622 lines)

| | A: "Backpacker" | B: "Dragster" | C: "Architect" |
|---|---|---|---|
| I/O model | mmap + rayon | compio (io_uring) | mmap (OLAP) + compio (OLTP) |
| v0.0.3 LOC | **800** | 1,750 | **800** (= A) |
| v0.0.3 ships | **7-10 days** | 28-42 days | **7-10 days** |
| PageRank 100M edges | **8-22 sec** | 18-30 sec | **8-22 sec** |
| Risk | LOW | HIGH | LOW now, MEDIUM at v0.1.0 |

**Finding:** A and C are identical through v0.0.5. The fork is at v0.1.0.
B (full compio) is actively slower for PageRank.

### OLTP/OLAP Split (HTAP Architecture)

Source: `docs_PRD02/Timeline-OLTP-OLAP-Split.md` (738 lines)

Precedents: Oracle In-Memory, TiDB+TiFlash, AlloyDB, SAP HANA, GART (USENIX ATC'23)

> "You don't use ONE format for everything. You use a row/record format for
> writes and a columnar/analytical format for reads."

GART does EXACTLY what Knight Bus proposes: MySQL OLTP → WAL → mutable CSR → GraphScope analytics.

### Three OLAP Engine Variants

Source: `docs_PRD02/Timeline-Three-Engine-OLAP-Variants.md` (749 lines)

- **OLAP-Latency (Level 1):** mmap + rayon → fastest, variable RAM
- **OLAP-RAM (Level 2):** compio O_DIRECT + rayon → controlled 161 MB, slight speed cost
- **OLAP-Minimal (Level 3):** compio + edge-centric sort → fixed 41 MB, unlimited scale

**Key finding:** OLAP-RAM and OLAP-Latency are the SAME engine with different mmap hints.
~10 lines of difference (`mlock()` vs letting OS manage).
Source: `docs_PRD02/1000IQ-Rubber-Duck-Lowest-RAM-Wins.md` line ~250

### Why Compio IS Right for OLAP-RAM

Source: `docs_PRD02/Why-Compio-IS-Right-For-OLAP-RAM.md` (486 lines)

**Corrects** the earlier analysis (`Rubber-Duck-Compio-io_uring-Analysis.md`) which
said compio was wrong for OLAP. That analysis optimized for SPEED. OLAP optimizes
for RAM DETERMINISM.

- mmap delegates RAM to the kernel → nondeterministic RSS
- O_DIRECT bypasses page cache → user controls every byte → deterministic
- Edge-centric PageRank (X-Stream SOSP 2013) eliminates random access → everything streams
- compio + rayon ARE compatible (producer-consumer via channel, same as Iggy pattern)

**compio serves 3 of 4 engines. It's infrastructure, not an optimization.**

### Compio/io_uring Analysis (from Iggy Study)

Source: `docs_PRD02/Rubber-Duck-Compio-io_uring-Analysis.md` (562 lines)

Studied Apache Iggy's I/O architecture. Conclusion:
- Iggy uses compio because it's I/O-bound (message streaming)
- Knight Bus v0.0.3 PageRank is CPU-bound → mmap + rayon wins
- compio becomes correct at v0.1.0 for OLTP WAL and concurrent reads
- **Techniques adopted from Iggy:** `madvise(SEQUENTIAL)`, buffer pooling, vectored writes

**PARTIALLY SUPERSEDED** by `Why-Compio-IS-Right-For-OLAP-RAM.md` which shows
compio IS right for OLAP when the goal is RAM determinism (not speed).

### 13 Formats Meet Unknown Queries

Source: `docs_PRD02/Timeline-Formats-Unknown-Queries.md` (710 lines)

How to handle arbitrary Cypher when you have 13 specialized layouts:
- Strategy: AnchorDualCsr as universal fallback, algorithm-specific layouts built on demand
- Disk space concern: 13 layouts × 50GB = 650GB is absurd → build lazily
- User doesn't pick layout → query router selects automatically

### Performance Estimates (Hard Numbers)

Source: `docs_PRD02/Timeline-Performance-Estimates.md` (712 lines)

Reference workload predictions grounded in real benchmarks:

| Scale | Nodes | Edges | Import | PageRank | RAM |
|---|---|---|---|---|---|
| Small | 1M | 10M | ~30 sec | ~1 sec | ~200 MB |
| Medium | 10M | 100M | ~5 min | ~8-22 sec | ~700 MB |
| Large | 100M | 1B | ~50 min | ~80-200 sec | ~7 GB |

### User Journey: 50GB OLTP/OLAP Lag

Source: `docs_PRD02/User-Journey-50GB-OLTP-OLAP-Lag.md` (766 lines)

**Rule:** NEVER stop queries. Never. Not in any strategy.

4 strategies analyzed:
- A: Snapshot versioning (2-5 min stale, zero blocking)
- B: Overlay model (zero stale, Grafeo-style)
- C: Query router (TiDB/Oracle/AlloyDB pattern)
- D: Incremental CSR (zero stale, high complexity)

### User Journeys: 50GB Deep Think

Source: `docs_PRD02/User-Journeys-50GB-Deep-Think.md` (893 lines)

5 personas mapped minute-by-minute:

| Persona | Can Use v0.0.3? | Wow Moment |
|---|---|---|
| Priya (data scientist) | YES | "PageRank on my laptop! 12 sec vs Neo4j OOM" |
| Tom (consultant) | YES | "No cloud instance, no $50/day" |
| Pipeline (automated) | YES | "Simpler than Neo4j GDS, no JVM" |
| Marcus (backend eng) | NO | Needs write path |
| Aisha (SRE) | NO | Needs writes, too stale |

**Key insight:** "CRUD" for analytics users = REBUILD, not INSERT.
Neo4j GDS is ALREADY stale (point-in-time projection).
Knight Bus's snapshot model has the EXACT SAME freshness guarantee.

---

## V. PMF & Go-To-Market

### RAM vs Latency — Doshi PMF Analysis

Source: `docs_PRD02/PMF-RAM-vs-Latency-Doshi.md` (440 lines)

Real Neo4j user complaints researched (forums, Reddit, HN, GitHub):
- **RAM complaints outnumber latency complaints 2:1**
- NASA switched from Neo4j to Memgraph over COST (RAM scales pricing)
- Brazilian startup: "We need more storage but the RAM and CPU is fine. The pricing forces us to overpay."

**Strategy:** Lead with RAM (get users) → Reveal Latency (keep users)

### Doshi Architecture Scenarios

Source: `docs_PRD02/PMF-Doshi-Architecture-Scenarios.md` (655 lines)

4 timelines simulated through the Doshi lens:

| Timeline | Positioning | Time to First User | Risk |
|---|---|---|---|
| A: DuckDB Play | Embedded analytics engine | 2 weeks | Low |
| B: TETRA Play | Direct Neo4j replacement | 3-6 months | **High** |
| C: Research Play | Algorithm innovation | 1-2 months | Medium |
| D: Composable Play | Start as library | 2 weeks | **Lowest** |

**Cypher parser is "destined to fail" risk** — months of overhead before user value.

### The Viral Path

Source: `docs_PRD02/PMF-Viral-The-Obvious-Mistake.md` (555 lines)

One-sentence pitch: "Neo4j already uses CSR for algorithms. They just rebuild
it from scratch every time. We don't."

**Sidecar positioning:** Export your Neo4j graph once, store as CSR, run
algorithms 10-30x faster forever. "Export once. Run forever."

### Storage Architecture Risk (Doshi Lens)

Source: `docs_PRD02/Timeline-Storage-Risk-Doshi.md` (688 lines)

LNO framework applied:
- **Leverage:** Getting storage architecture right (10x impact)
- **Neutral:** Which B+tree library to use (1x)
- **Overhead:** Porting Neo4j config format exactly (0.1x)

### "Just Rewrite in Rust" Thesis

Source: `docs_PRD02/Timeline-Just-Rewrite-In-Rust.md` (611 lines)

"Rewrite in Rust" is actually THREE things:
1. Language (Java → Rust): ~1.3-1.5x faster (smallest factor)
2. Storage (linked-list → CSR): ~10-100x for traversals (BIG factor)
3. Runtime (JVM heap → mmap): skips 60-100 sec projection

> "The language enables the storage format, and the format IS the optimization."

---

## VI. v0.0.3 Implementation Plan

### Rewrite Strategy (v2, Corrected)

Source: `docs_PRD02/Rewrite-Strategy-Folders-And-Estimates.md` (992 lines)

**Corrected by:** `docs_PRD02/Rubber-Duck-Rewrite-Strategy.md` (933 lines)

Key corrections from rubber duck:
- PageRank time: ~~"2-7 sec"~~ → **8-22 sec** (rayon 4 cores), 30-60 sec single-thread
- LOC: ~~670~~ → **~800** (forgot unsafe casts, dangling nodes, CSV output, Cargo.toml)
- Timeline: ~~5 days~~ → **7-10 days** (Day 6 = high-risk 10M scale test)
- **rayon is MANDATORY** — without it, Knight Bus is slower than Neo4j GDS algorithm-only
- +2 new dependencies: `rand`, `rayon`

**Honest headline:** "PageRank in 10 seconds, 720 MB. Neo4j: 90 seconds, 12 GB."
Speedup comes from skipping projection, not from faster algorithm.

### v0.0.3 Claims (Rubber Duck)

Source: `docs_PRD02/Rubber-Duck-v003-Claims.md` (813 lines)

Claims killed:
- Hash index speedup: "10-50x" → only 1.3-2x total query speedup, costs 132 MB RAM
- madvise RSS reduction: "30-50% less" → negligible for traversals
- Streaming PageRank: "100 MB, 5 sec" → ~165 MB, 3-5 sec NVMe, ~60 sec HDD
- Combined headline: "40x less RAM, 24x faster" → **6-30x less RAM, 10-30x faster**

### v0.0.3 Consolidated Plan (From Self-Audit)

Source: `docs_PRD02/Self-Audit-Wiki-Not-Used.md` (302 lines)

1. Add `InboundPower` layout (Family #2) as `page_rank.rs` (~120 LOC + 50 LOC integration)
2. Add synthetic graph generator as `synthetic.rs` (~100 LOC)
3. Benchmark against Neo4j GDS (the FAIR comparison, not against Cypher traversal)
4. Design `io/` module traits for compio from day 1 (don't implement yet)

Total: ~800 LOC, 7-10 days

### Version Roadmap

Source: `docs_PRD02/Rewrite-Strategy-Folders-And-Estimates.md`

```
v0.0.3  PageRank + Synthetic + Benchmark        +800 LOC     7-10 days
v0.0.4  Dijkstra + BFS + Python bindings         +800 LOC     2-3 weeks
v0.0.5  Overlay model (zero-stale writes)        +400 LOC     1-2 weeks
v0.1.0  OLTP record store + query router         +2,600 LOC   4-6 weeks
```

---

## VII. Test Journals & Benchmark Evidence

### v0.0.2 Final Testing Journal (AUTHORITATIVE)

Source: `Final-Testing-Journal-v002.md` (79 lines)

Runtime-only RSS measurement. Corrects v0.0.1 journal where RSS included
CSV truth machinery. Three datasets: 1 MB, 50 MB, 2 GB. All parity passed.

### v0.0.1 Final Testing Journal (HISTORICAL)

Source: `v001-learnings/Final-Testing-Journal.md` (75 lines)

First Rust-vs-Neo4j direct comparison. **NOTE:** The 2 GB RSS (3.5 GB) was high
because bench-corpus still loaded CSV truth in-process.
See: `v001-learnings/why-rust-rss-was-high.md`

### v0.0.2 Transition

Source: `docs/pre-v002/v002-transition.md` (17 lines)

v0.0.2 priorities: reduce memory pressure, separate parity/truth from benchmarking,
improve runtime-only RSS claims, scale beyond 2 GB.

### Test Journal (April 2026)

Source: `v001-learnings/journal-tests-202604.md` (179 lines)

Append-only log of all test runs during April 2026 development.

### 2 GB Competitor Benchmark Matrix

Source: `v002-research/competitor-2gb-benchmark.md` (110 lines)

Knight Bus vs 8 competitors on code_sparse_2gb dataset.
Only Neo4j wired and tested. Others (Memgraph, Kuzu, FalkorDB, HugeGraph,
Apache AGE, JanusGraph, Dgraph) have adapter placeholders but are untested.

### Neo4j Smoke Runbook

Source: `docs/pre-v002/neo4j-smoke-runbook.md` (60 lines)

How to install Neo4j, run the smoke ladder, and compare at 1 MB / 50 MB / 2 GB.
Scripts: `install_neo4j_brew.sh`, `run_neo4j_smoke_ladder.sh`

---

## VIII. Playbooks & Process

### Product/Feature Development Flow

Source: `docs_PRD01/playbook-product-feature-flow.md` (46 lines)

Classify → Discover → PRD → Architecture → TDD → Ship.
"Let architecture remove scope. Do not treat first PRD as sacred."

### TDD Workflow (STUB-RED-GREEN-REFACTOR)

Source: `docs_PRD01/playbook-tdd-workflow-rust.md` (48 lines)

Phase 1: STUB (compile). Phase 2: RED (failing tests). Phase 3: GREEN (minimum code).
Phase 4: REFACTOR (clean up, clippy, fmt).

### 12 Principles of LLM-Native Development

Source: `docs_PRD01/knowledge-llm-principles.md` (14 lines)

Key: LLMs are retrieval systems. Iteration required. Context windows forget.
Rubber duck debugging. Tests are the spec. Four-word names.

### LLM Workflow

Source: `docs_PRD01/knowledge-llm-workflow.md` (39 lines)

### Rust Coding Conventions

Source: `docs_PRD01/knowledge-rust-coding-conventions.md` (66 lines)

---

## IX. Document Inventory (All 60 Files)

### Root (4 files)
| File | Lines | Purpose |
|---|---|---|
| `README.md` | 175 | Public benchmark results and explanation |
| `Final-Testing-Journal-v002.md` | 79 | **AUTHORITATIVE** v0.0.2 benchmark record |
| `journal-tests-202604-v002.md` | 15 | Quick test journal |
| `v003/v003-prd.md` | 10 | Empty placeholder for v0.0.3 PRD |

### docs/pre-v002/ (11 files, ~2,433 lines)
| File | Lines | Key Conclusion |
|---|---|---|
| `KNIGHT_BUS_THESIS.md` | 508 | Core thesis: native Rust walk runtime, CSR snapshot, mmap |
| `STORAGE_RUNTIME_ALIGNMENT.md` | 359 | "Storage aligned to runtime = hot path visible in on-disk bytes" |
| `v001-PRD.md` | 534 | v0.0.1 product requirements: CSV truth → snapshot → verify → query → bench |
| `v002-transition.md` | 17 | v0.0.2 priorities: reduce memory, separate parity from benchmarking |
| `neo4j-smoke-runbook.md` | 60 | How to run the Neo4j comparison benchmark |
| `A-*-storage-runtime-alignment-eli5.md` | 295 | ELI5: "Store like a map, not like a filing cabinet" |
| `A-*-v001-proof-ladder-eli5.md` | 300 | ELI5: "Prove one narrow thing well" |
| `A-*-tiny-harness-validation.md` | 155 | 39-node test validation details |
| `A-*-open-path-and-minimum-proof-eli5.md` | 334 | ELI5: Why open is fast (map and validate, not load and rebuild) |
| `A-*-rust-vs-neo4j-proof-eli5.md` | 207 | ELI5: First direct Rust-vs-Neo4j benchmark |

### docs_PRD01/ (16 files, ~5,118 lines)
| File | Lines | Key Conclusion |
|---|---|---|
| `Neo4j-Architecture-Map.md` | 276 | Complete Neo4j stack map + Knight Bus replacement table |
| `Neo4j-Component-Breakdown.md` | 822 | Neo4j = 6 products: DB (195K), Cypher (702K), Bolt (42K), Server (34K), Tools (78K) |
| `Neo4j-Frontend-Backend-Split.md` | 704 | Frontend (~470K LOC, must match) vs Backend (~195K LOC, replace) |
| `Rubber-Duck-Frontend-Backend-Split.md` | 1,281 | 4 traits audit + **13 Algorithm Storage Atlas** |
| `Knight-Bus-Inventory-and-Gap-Analysis.md` | 405 | "5% of code needed, but the HARDEST 5%" + 50GB onboarding journey |
| `Faithful-Rust-Port-Analysis.md` | 646 | v1: analysis of faithful port approach |
| `Faithful-Rust-Port-Analysis-v2.md` | 1,023 | **v2 (corrected):** 12 rubber-duck corrections. Bun comparison flawed. |
| `Timeline-Traversal-Architecture-Paths.md` | 506 | 4 timelines: Faithful Port (30%), Split (50%), Knight Bus First (70%), Minimal (40%) |
| `Previous-learnings-01.md` | 90 | Why KB is fast: immutable snapshot, contiguous arrays, no pointer chasing |
| `journal20260525083839685.md` | 131 | Session journal: Cypher subset strategy, variant implications |
| `neo4j-loc-results.md` | 247 | cloc analysis: 1.58M LOC total |
| `knowledge-llm-principles.md` | 14 | 12 principles of LLM-native development |
| `knowledge-llm-workflow.md` | 39 | LLM workflow reference |
| `knowledge-rust-coding-conventions.md` | 66 | Rust coding conventions |
| `playbook-product-feature-flow.md` | 46 | Product development flow |
| `playbook-tdd-workflow-rust.md` | 48 | TDD workflow: STUB → RED → GREEN → REFACTOR |

### docs_PRD02/ (24 files, ~15,059 lines)
| File | Lines | Key Conclusion |
|---|---|---|
| `Copy-Structure-Invent-Storage.md` | 422 | "Copy Neo4j's module structure. Invent the storage format." |
| `Storage-Formats-Hope-Not-Blind.md` | 491 | 13 layout families + precedent inventory (sled, TiKV, LMDB, MeshDB, DuckDB) |
| `Storage-Risk-Meta-Pattern.md` | 482 | Mise en place analogy. Operation-aligned vs general-purpose storage. |
| `Prompt-Storage-Architecture-Risk.md` | 93 | LLM prompt for external rubber-ducking of storage architecture |
| `Three-Architecture-Options-Folder-By-Folder.md` | 622 | A (Backpacker), B (Dragster), C (Architect) — file-by-file tables |
| `Timeline-OLTP-OLAP-Split.md` | 738 | HTAP precedents: Oracle, TiDB, AlloyDB, GART. Proven architecture. |
| `Timeline-Three-Engine-OLAP-Variants.md` | 749 | OLAP-Latency / OLAP-RAM / OLAP-Minimal — same engine, different hints |
| `Timeline-Formats-Unknown-Queries.md` | 710 | How 13 layouts handle unknown Cypher: AnchorDualCsr as fallback |
| `Timeline-Performance-Estimates.md` | 712 | Hard numbers: 1M/10M/100M scale predictions |
| `Timeline-Storage-Risk-Doshi.md` | 688 | LNO framework applied to storage architecture decisions |
| `Timeline-Just-Rewrite-In-Rust.md` | 611 | "Rewrite in Rust" = language + storage + runtime. Format IS optimization. |
| `Rewrite-Strategy-Folders-And-Estimates.md` | 992 | v2 rewrite strategy: every file, every folder, corrected run times |
| `Rubber-Duck-Rewrite-Strategy.md` | 933 | Kills 3 claims: PageRank time, LOC, timeline. rayon mandatory. |
| `Rubber-Duck-v003-Claims.md` | 813 | Kills hash index speedup, madvise claims. Honest: 6-30x RAM, 10-30x speed. |
| `Rubber-Duck-Compio-io_uring-Analysis.md` | 562 | Iggy study: mmap+rayon for v0.0.3, compio for v0.1.0 OLTP |
| `Why-Compio-IS-Right-For-OLAP-RAM.md` | 486 | **CORRECTS** above: compio IS right for OLAP-RAM (O_DIRECT = deterministic) |
| `1000IQ-The-Deeper-Insight.md` | 517 | Real moat: 13 layouts + adaptive engine, not just CSR |
| `1000IQ-Rubber-Duck-Lowest-RAM-Wins.md` | 557 | OLAP-RAM and OLAP-Latency = same engine, different mmap hints |
| `PMF-RAM-vs-Latency-Doshi.md` | 440 | RAM complaints 2:1 over latency. Lead with RAM. |
| `PMF-Doshi-Architecture-Scenarios.md` | 655 | DuckDB Play (2 weeks, low risk) vs TETRA Play (6 months, high risk) |
| `PMF-Viral-The-Obvious-Mistake.md` | 555 | "Neo4j rebuilds CSR every time. We don't." Sidecar positioning. |
| `Neo4j-Multinode-Do-We-Need-It.md` | 377 | No multi-node needed. Neo4j GDS is single-server. DuckDB precedent. |
| `User-Journey-50GB-OLTP-OLAP-Lag.md` | 766 | Never block queries. 4 staleness strategies. |
| `User-Journeys-50GB-Deep-Think.md` | 893 | 5 personas. "CRUD" for analytics users = REBUILD, not INSERT. |
| `Self-Audit-Wiki-Not-Used.md` | 302 | Identified 7 conclusions re-derived instead of cited |

### v001-learnings/ (3 files, ~302 lines)
| File | Lines | Key Conclusion |
|---|---|---|
| `Final-Testing-Journal.md` | 75 | v0.0.1 benchmark (NOTE: 2 GB RSS was inflated, see below) |
| `journal-tests-202604.md` | 179 | Append-only test log |
| `why-rust-rss-was-high.md` | 48 | RSS included CSV truth loading, not pure walker memory |

### v002-research/ (2 files, ~148 lines)
| File | Lines | Key Conclusion |
|---|---|---|
| `README.md` | 38 | Research directory purpose |
| `competitor-2gb-benchmark.md` | 110 | 8-competitor matrix. Only Neo4j tested. Others = adapter placeholders. |

---

## X. Superseded Conclusions (Corrected)

Some earlier documents contain conclusions that were later corrected.
Listed here to prevent re-using outdated claims.

| Original Claim | Document | Corrected By | Correct Conclusion |
|---|---|---|---|
| "compio is wrong for OLAP" | `Rubber-Duck-Compio-io_uring-Analysis.md` | `Why-Compio-IS-Right-For-OLAP-RAM.md` | compio IS right for OLAP-RAM (O_DIRECT = deterministic memory) |
| "compio + rayon are incompatible" | `Rubber-Duck-Compio-io_uring-Analysis.md` | `Why-Compio-IS-Right-For-OLAP-RAM.md` | Compatible via channel-based producer-consumer (Iggy pattern) |
| "PageRank 100M edges: 2-7 sec" | `Rewrite-Strategy-Folders-And-Estimates.md` v1 | `Rubber-Duck-Rewrite-Strategy.md` | 8-22 sec (rayon 4 cores), 30-60 sec single-thread |
| "~670 LOC for v0.0.3" | `Rewrite-Strategy-Folders-And-Estimates.md` v1 | `Rubber-Duck-Rewrite-Strategy.md` | ~800 LOC (forgot unsafe casts, dangling nodes, Cargo.toml) |
| "5 days to ship v0.0.3" | `Rewrite-Strategy-Folders-And-Estimates.md` v1 | `Rubber-Duck-Rewrite-Strategy.md` | 7-10 days (Day 6 = high-risk 10M-scale test) |
| "40x less RAM, 24x faster" | `Rubber-Duck-v003-Claims.md` original | `Rubber-Duck-v003-Claims.md` corrected | 6-30x less RAM, 10-30x faster |
| "Hash index = 10-50x faster" | `Rubber-Duck-v003-Claims.md` | Same doc | Only 1.3-2x total query speedup, costs 132 MB extra RAM |
| "Bun proves Zig→Rust port is easy" | `Faithful-Rust-Port-Analysis.md` v1 | `Faithful-Rust-Port-Analysis-v2.md` | "Bun is inspiration, not precedent. The situations differ." |
| "5:1 to 8:1 LOC compression Scala→Rust" | `Faithful-Rust-Port-Analysis.md` v1 | `Faithful-Rust-Port-Analysis-v2.md` | 1.5:1 to 5:1 depending on tier |
| "3-8x throughput" | `Faithful-Rust-Port-Analysis.md` v1 | `Faithful-Rust-Port-Analysis-v2.md` | 1.5-3x steady-state for CPU-bound hot loops |
| "v0.0.2 2 GB RSS = 3.5 GB" | `v001-learnings/Final-Testing-Journal.md` | `Final-Testing-Journal-v002.md` | 234 MB runtime-only (was including CSV truth loading) |

---

## Quick Reference: What Document Answers What Question

| Question | Start Here |
|---|---|
| "What IS Knight Bus?" | `docs/pre-v002/KNIGHT_BUS_THESIS.md` |
| "Why is it fast?" | `docs_PRD01/Previous-learnings-01.md`, `README.md` |
| "What does Neo4j look like inside?" | `docs_PRD01/Neo4j-Architecture-Map.md`, `Neo4j-Component-Breakdown.md` |
| "What do we have vs what we need?" | `docs_PRD01/Knight-Bus-Inventory-and-Gap-Analysis.md` |
| "How should storage work?" | `docs/pre-v002/STORAGE_RUNTIME_ALIGNMENT.md` |
| "What are the 13 algorithm layouts?" | `docs_PRD02/Storage-Formats-Hope-Not-Blind.md` lines 146-175 |
| "Which architecture option?" | `docs_PRD02/Three-Architecture-Options-Folder-By-Folder.md` |
| "What about writes / OLTP?" | `docs_PRD02/Timeline-OLTP-OLAP-Split.md` |
| "What about multi-node / clustering?" | `docs_PRD02/Neo4j-Multinode-Do-We-Need-It.md` |
| "What should v0.0.3 be?" | `docs_PRD02/Rewrite-Strategy-Folders-And-Estimates.md` (v2) |
| "What numbers are honest?" | `docs_PRD02/Rubber-Duck-Rewrite-Strategy.md`, `Rubber-Duck-v003-Claims.md` |
| "Is compio useful?" | `docs_PRD02/Why-Compio-IS-Right-For-OLAP-RAM.md` |
| "What's the PMF play?" | `docs_PRD02/PMF-RAM-vs-Latency-Doshi.md`, `PMF-Doshi-Architecture-Scenarios.md` |
| "What's the viral angle?" | `docs_PRD02/PMF-Viral-The-Obvious-Mistake.md` |
| "What does a 50GB user journey look like?" | `docs_PRD02/User-Journeys-50GB-Deep-Think.md` |
| "What benchmark results do we have?" | `Final-Testing-Journal-v002.md` |
| "How do I run the benchmarks?" | `docs/pre-v002/neo4j-smoke-runbook.md` |
| "What were we wrong about?" | [Section X: Superseded Conclusions](#x-superseded-conclusions) |
