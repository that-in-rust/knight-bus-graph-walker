# Self-Audit: I Didn't Use the Wiki I Built

*The user pointed out that I've been doing analysis from scratch each time
instead of building on the knowledge base already in this repo. They're right.
This document enumerates what was already known, what I re-derived wastefully,
and what I missed by not reading my own wiki.*

---

## Core Facts I Enumerated (First)

```
FACT 1: This repo has 50+ markdown documents across 4 directories
  docs/pre-v002/     — 11 docs (thesis, storage alignment, PRDs, ELI5s)
  docs_PRD01/        — 16 docs (architecture maps, gap analysis, timelines, playbooks)
  docs_PRD02/        — 23 docs (timelines, rubber ducks, PMF analysis, user journeys)
  neo4j-reference/   — agent skills, principles, workflows

FACT 2: These docs contain PROVEN conclusions from previous deep analysis
  - Not hypotheses. Conclusions. Backed by code, benchmarks, and rubber duck.

FACT 3: I was asked to do multiple analyses in this session. In each one,
  I started from scratch instead of citing the existing wiki.
```

---

## What the Wiki Already Knew (That I Re-Derived)

### 1. "Neo4j's linked-list storage is the real bottleneck, not Java"

**Already documented in:**
- `docs_PRD02/Storage-Formats-Hope-Not-Blind.md` (lines 32-89)
  - Complete breakdown: 15B node records, 34B rel records, pointer chasing
  - "The gap isn't 'Java is slow.' The gap is that Neo4j's storage format
    forces random I/O where contiguous reads would suffice."
- `docs_PRD02/Storage-Risk-Meta-Pattern.md` (lines 89-140)
  - "Mise en place" analogy explaining operation-aligned storage
  - "For a node with 100 neighbors, that's ~200 random page reads"
- `docs_PRD01/Neo4j-Architecture-Map.md` (lines 92-148)
  - Full record store layout, traversal step-by-step, Knight Bus comparison
- `docs_PRD01/Previous-learnings-01.md` (entire document)
  - "Knight Bus is faster because it turns the problem into immutable
    snapshot replay over contiguous adjacency arrays"

**What I did instead:** In `PMF-Viral-The-Obvious-Mistake.md`, I "discovered"
that Neo4j uses linked lists and rebuilds CSR — AS IF THIS WERE NEWS.
The repo already had 4 documents explaining this in detail.

### 2. "The 13 algorithm-specific layout families"

**Already documented in:**
- `docs_PRD02/Storage-Formats-Hope-Not-Blind.md` (lines 146-175)
  - Complete table: all 13 families, what they optimize, why Neo4j is bad,
    which algorithms each serves
  - Status: "1 PROVEN (AnchorDualCsr), 12 DESIGN-ONLY"
- `docs_PRD01/Rubber-Duck-Frontend-Backend-Split.md` (Appendix B)
  - Original source of the 13 families
  - Pattern: "small family of reusable layout types plus per-algorithm contracts"

**What I did instead:** In `1000IQ-The-Deeper-Insight.md`, I proposed
"13 algorithm-specific layouts" as a NEW insight. It was already in the
wiki — documented, categorized, and stress-tested.

### 3. "OLTP/OLAP split matches TiDB, Oracle, AlloyDB"

**Already documented in:**
- `docs_PRD02/Timeline-OLTP-OLAP-Split.md` (lines 12-65)
  - Complete survey: Oracle In-Memory, TiDB+TiFlash, AlloyDB, SAP HANA
  - GART paper analysis (USENIX ATC'23) — mutable CSR from WAL replay
  - "You're not inventing a new architecture. You're applying a PROVEN
    architecture to a graph database."
- `docs_PRD02/Timeline-Three-Engine-OLAP-Variants.md` (lines 1-80)
  - Three-engine architecture: OLTP + OLAP-RAM + OLAP-Latency
  - Shows `MmapWalkRuntime` is ALREADY the OLAP-RAM seed
  - `BuildMemoryBudget` already supports memory-constrained operation

**What I did instead:** In `Three-Architecture-Options-Folder-By-Folder.md`,
I presented the OLTP/OLAP split as though it were a new comparison of 3
options. The wiki already had the answer: Option C (The Architect) was
already designed in the Three-Engine doc.

### 4. "Storage-runtime alignment is the core thesis"

**Already documented in:**
- `docs/pre-v002/STORAGE_RUNTIME_ALIGNMENT.md` (THE foundational doc)
  - "The storage is 'aligned to runtime' only when the hot traversal path
    is already visible in the on-disk bytes."
  - Copy from Parseltongue: dense IDs, dual adjacency, build-time heavy
  - Copy from Iggy: payload shaped for read path, tiny sidecar indexes
  - Concrete storage contract: every file, every type, every width
- `docs/pre-v002/KNIGHT_BUS_THESIS.md` (the original thesis)
  - "The most practical way to build knight-bus-graph-walker is:
    native Rust walk runtime first"
  - "The winning hackathon move is: generate, compile, mmap, expose, benchmark"

**What I did instead:** I repeatedly explained why mmap + CSR is the right
design without citing these documents. The thesis was written BEFORE any
code existed.

### 5. "Knight Bus has only 5% of what a Neo4j replacement needs"

**Already documented in:**
- `docs_PRD01/Knight-Bus-Inventory-and-Gap-Analysis.md` (lines 340-374)
  - "Knight Bus provides ~5% of the code needed for a Neo4j replacement.
    But it provides the HARDEST 5%"
  - Complete gap inventory: Cypher (5-10K), planner (15-25K), runtime (25-40K),
    Bolt (10-15K), mutable storage (30-50K), etc.
  - Total needed: ~100-165K LOC
  - "Nobody else has a working dual-CSR with mmap and external merge sort"
- Same doc: user onboarding journey (50GB scenario, 5-minute test, compatibility
  reporter)

**What I did instead:** In `Neo4j-Multinode-Do-We-Need-It.md`, I researched
whether we need multi-node — but I could have started from the gap analysis
which already showed that the IMMEDIATE priorities are Cypher, Bolt, and
mutable storage, not clustering.

### 6. "The variant_low_RAM and variant_low_latency distinction"

**Already documented in:**
- `docs_PRD01/Neo4j-Architecture-Map.md` (lines 244-255)
  - `variant_low_RAM`: aggressive mmap, external merge sort, smaller node table
  - `variant_low_latency`: pin hot pages, prefetch, inline short keys
  - "Trade: slightly higher latency for much lower RSS"
  - "Trade: higher RSS for lower p99"

**What I did instead:** In `Why-Compio-IS-Right-For-OLAP-RAM.md`, I
"discovered" that OLAP-RAM and OLAP-Latency are different mmap hint
strategies. The Architecture Map already had this under "Variant Design
Implications."

### 7. "The faithful port takes 180-250K LOC and loses the thesis"

**Already documented in:**
- `docs_PRD01/Timeline-Traversal-Architecture-Paths.md` (lines 22-77)
  - Timeline A: "The Faithful Port" — Year 1 = 180-250K LOC
  - "You've rebuilt the exact architecture that Knight Bus proved was slow"
  - "A technically impressive but strategically confused project"
- `docs_PRD01/Faithful-Rust-Port-Analysis-v2.md` (rubber-duck corrected)
  - RD-01 through RD-12: every wrong assumption identified and corrected
  - "Bun is inspiration, not precedent. The situations differ."
  - Honest LOC ratios: 1.5:1 to 5:1 (not 5:1 to 8:1)

**What I did instead:** I never referenced these timeline estimates when
discussing v0.0.3 scope. The faithful port doc already proved that going
beyond CSR analytics is months of work — which supports the "just ship
PageRank" conclusion I reached by other means.

---

## What I MISSED by Not Reading the Wiki

### MISSED 1: The 13 layout families are the REAL moat (not "rewrite in Rust")

The `Storage-Formats-Hope-Not-Blind.md` doc is explicit:

> "One universal base format was rejected. One fully bespoke engine
> per algorithm was also rejected. The chosen pattern is a small
> family of reusable layout types plus per-algorithm contracts."

This is MORE sophisticated than anything I proposed in my recent analyses.
The "rewrite in Rust" thesis I championed is INCOMPLETE — the real
competitive advantage is that EACH ALGORITHM gets an optimized data layout.
AnchorDualCsr (proven) is just family #1 of 13.

My recent docs treated all algorithms as running on the same CSR. The wiki
says: no, PageRank gets InboundPower, Dijkstra gets RelaxationFrontier,
Louvain gets PartitionRefinement, etc. The moat is the ATLAS, not the
language.

### MISSED 2: The cookbook/mise en place analogy is better than any explanation I gave

`Storage-Risk-Meta-Pattern.md` explains the entire architecture using a
cooking analogy that's more accessible than anything I wrote:

> "A professional kitchen doesn't use the cookbook at service time.
> Before service, the chef does mise en place."

I should have CITED this in every analysis instead of re-explaining why
CSR is faster than linked lists.

### MISSED 3: The gap analysis already maps the user onboarding journey

`Knight-Bus-Inventory-and-Gap-Analysis.md` has a complete Shreyas Doshi
onboarding journey (Steps 0-5) for a 50GB Neo4j user. When I was later
asked to analyze user journeys, I should have started from this, not
from scratch.

### MISSED 4: GART (USENIX ATC'23) already validates our OLTP/OLAP split

`Timeline-OLTP-OLAP-Split.md` found GART — an academic system doing
EXACTLY what we proposed (MySQL OLTP → WAL → mutable CSR for OLAP).
I never referenced this when discussing the "just rewrite in Rust"
approach, even though GART proves the architectural pattern works.

### MISSED 5: variant_low_RAM was already designed

The Architecture Map already had both variants designed. When I was asked
"is compio useful for OLAP-RAM?", I should have started from the existing
variant design, not from first principles.

---

## What This Means Going Forward

```
RULE 1: Before any new analysis, READ the wiki first
  - Search for keywords in existing docs
  - Cite specific documents and line numbers
  - Only derive NEW conclusions, not re-derive old ones

RULE 2: Build on the wiki, don't replace it
  - New docs should reference existing docs as sources
  - If a conclusion exists, cite it: "As established in
    Storage-Formats-Hope-Not-Blind.md (line 89)..."
  - Only add NEW information or corrections

RULE 3: The wiki IS the institutional memory
  - 50+ documents = thousands of hours of analysis
  - Every rubber duck, every timeline, every correction is there
  - Starting from scratch wastes the most valuable asset in the repo

RULE 4: The 13 layout families are the roadmap
  - Not "add PageRank." Add InboundPower layout.
  - Not "add Dijkstra." Add RelaxationFrontier layout.
  - Each algorithm gets its own optimized data shape.
  - This is the moat. This is the thesis. The wiki says so.
```

---

## What the Wiki Says v0.0.3 Should Be (Consolidated)

Reading ALL the docs together, the wiki converges on ONE answer for v0.0.3:

```
FROM KNIGHT_BUS_THESIS.md:
  "Build a native Rust walk runtime first"
  "One frozen graph world, one workload family"

FROM STORAGE_RUNTIME_ALIGNMENT.md:
  "Peers are the payload, offsets are the seek aid"
  "No database or log lookup in the traversal loop"

FROM Storage-Formats-Hope-Not-Blind.md:
  Family #2: InboundPower — "repeated inbound score accumulation (PageRank)"
  Status: DESIGN-ONLY → needs implementation

FROM Previous-learnings-01.md:
  "A fairer next benchmark would compare Knight Bus against:
   Neo4j GDS BFS or traversal on a projected in-memory graph"

FROM Rubber-Duck-Rewrite-Strategy.md:
  "rayon is MANDATORY for the demo"
  "~800 LOC change, 7-10 days"

FROM PMF-RAM-vs-Latency-Doshi.md:
  "Lead with RAM (get users) → Reveal Latency (keep users)"

FROM Timeline-Just-Rewrite-In-Rust.md:
  "The language enables the storage format, and the format IS the optimization"

FROM 1000IQ-Rubber-Duck-Lowest-RAM-Wins.md:
  "OLAP-RAM and OLAP-Latency aren't two engines. They're the SAME engine
   with different mmap hints. ~10 lines of difference."

FROM Neo4j-Architecture-Map.md (variant_low_RAM):
  "Aggressive mmap with minimal resident pages"
  "External merge sort during build (already in low_ram.rs)"

FROM Why-Compio-IS-Right-For-OLAP-RAM.md:
  Level 1 (mmap+rayon) for speed, Level 3 (compio+sort) for minimum RAM
  "compio is infrastructure, not an optimization"
```

**Consolidated v0.0.3:**

1. Add `InboundPower` layout (Family #2) as `page_rank.rs`
   - Uses existing reverse CSR (reverse_offsets + reverse_peers)
   - Jacobi iteration with rayon parallelism
   - madvise(SEQUENTIAL) for streaming access
   - ~120 LOC algorithm + ~50 LOC integration

2. Add synthetic graph generator as `synthetic.rs`
   - Deterministic keys (already specified in thesis)
   - Scale-free + clustered hybrid (already specified)
   - ~100 LOC

3. Benchmark against Neo4j GDS (the FAIR comparison the wiki says we need)
   - Not against Cypher traversal (that was v0.0.2's benchmark)
   - Against projected in-memory GDS PageRank
   - This is the headline: "same algorithm, same data, different storage format"

4. Design the `io/` module for compio from day 1
   - Don't implement yet — just the traits
   - Level 2 and Level 3 come at v0.0.4/v0.0.5
   - "compio is infrastructure" (from the latest analysis)

**Total: ~800 LOC, 7-10 days. This was already the answer.**

The wiki knew. I just didn't read it.
