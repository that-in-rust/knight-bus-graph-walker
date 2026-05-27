# Knight Bus Mutation Strategy Analysis

> **Context:** Knight Bus uses immutable CSR (Compressed Sparse Row) snapshots.
> Queries are O(1) array reads. But editing the graph currently requires a full
> snapshot rebuild. This document analyzes the mutation problem from first
> principles, proposes solutions, stress-tests those proposals, and recommends
> a path forward.

---

## Table of Contents

1. [The Problem](#the-problem)
2. [Why CSR Resists Edits](#why-csr-resists-edits)
3. [Rebuild Cost at Scale](#rebuild-cost-at-scale)
4. [Solution A: Paged CSR](#solution-a-paged-csr)
5. [Solution B: Delta Layer + Binary Rebuild](#solution-b-delta-layer--binary-rebuild)
6. [Solution C: Do Nothing](#solution-c-do-nothing)
7. [50 GB Simulation](#50-gb-simulation)
8. [Rubber-Duck Corrections](#rubber-duck-corrections)
9. [Timeline Traverser Analysis](#timeline-traverser-analysis)
10. [Recommendation](#recommendation)

---

## The Problem

Knight Bus stores graphs as CSR: a flat `offsets[]` array and a flat `peers[]` array.

```
offsets:  [0,    3,    7,    9,    ...]
peers:   [A₁ A₂ A₃ B₁ B₂ B₃ B₄ C₁ C₂ ...]
          ←─ A's ─→ ←── B's ──→ ←─C's─→
```

`offsets[i]..offsets[i+1]` gives a contiguous slice of all neighbors for node `i`.
This is why queries are fast: one offset lookup + one sequential read.

But adding one neighbor to node B means shifting C's neighbors, D's neighbors,
and everything after them by one position, then updating every offset after B.
That is O(N + E) — essentially a full rebuild for a single edit.

**Source:** `src/graph.rs:65-85` — `flatten_adjacency_lists_now()` packs
`Vec<Vec<u32>>` into flat `(Vec<u64>, Vec<u32>)`. Once packed, there is no
insertion path.

**Source:** `src/runtime.rs:320-331` — `read_neighbor_ids()` reads
`offsets[dense_id]..offsets[dense_id+1]` directly from mmap. The read path
assumes a single contiguous peers array.

---

## Why CSR Resists Edits

The tension is fundamental:

| Property | Linked List (Neo4j) | Flat CSR (Knight Bus) |
|---|---|---|
| Write cost (1 edge) | O(1) — append record | O(N+E) — rebuild |
| Read cost (all neighbors) | O(degree) — chase pointers | O(1) — contiguous slice |
| Space per edge | 34 bytes (Neo4j records) | 4 bytes (one `u32`) |

Neo4j pays on every read so it can be cheap on writes.
Knight Bus pays on every write so it can be cheap on reads.

The question: is there a middle ground?

---

## Rebuild Cost at Scale

### From-CSV Rebuild (current implementation)

The build pipeline in `src/low_ram.rs` has 7 phases:

1. `BuildNodeRuns` — parse node CSV, sort into runs
2. `WriteNodeCatalog` — merge runs, write `node_table.bin` + `strings.bin`
3. `BuildEdgeRuns` — parse edge CSV, sort into runs
4. `ResolveFromKeys` — merge-join edges with node catalog (from_key → dense_id)
5. `ResolveToKeys` — merge-join (to_key → dense_id)
6. `EmitForwardCsr` — merge sorted pairs, write `forward.offsets.bin` + `forward.peers.bin`
7. `EmitReverseCsr` — sort reversed pairs, write reverse CSR

Each phase does external merge sort or streaming merge. Multiple full passes over all data.

| Dataset | Nodes | Edges | Snapshot | Est. CSV Rebuild |
|---|---|---|---|---|
| 2 GB | 4M | 36M | 490 MB | ~30-60 seconds |
| 50 GB | 100M | 900M | ~13 GB | ~15-25 minutes |
| 200 GB | 400M | 3.6B | ~50 GB | ~1-2 hours |
| 2 TB | 4B | 36B | ~500 GB | ~10-20 hours |

### Binary Rebuild (not yet implemented, but possible)

Instead of re-parsing CSV, read the existing binary snapshot files, merge in a sorted delta, and write new snapshot files. No CSV parsing, no key resolution, no external sort.

```
Binary rebuild for 50 GB graph:
  Read forward offsets + peers:  ~4.1 GB
  Merge delta (pre-sorted):     trivial
  Write new forward files:       ~4.1 GB
  Same for reverse:              ~4.1 GB read + ~4.1 GB write
  Total I/O: ~16.4 GB at 1 GB/s = ~16 seconds
```

This is 55-90× faster than CSV rebuild. It changes the calculus significantly.

---

## Solution A: Paged CSR

### Core Idea

Break the flat `peers[]` array into fixed-size pages (64 KB each). Each page holds
neighbors for a group of ~1,000 nodes. Each page has ~30% slack space for growth.

```
CURRENT (flat CSR):
  offsets.bin:  [0, 5, 8, 14, 17, ...]
  peers.bin:   [AAAAABBBCCCCCCDDDEEE...]    ← ALL neighbors, one packed array

PAGED CSR:
  page_dir.bin: for each node → (page_id, offset, count, capacity)
  page_0000.bin: [A's nbrs][B's nbrs][___slack___]  (64 KB)
  page_0001.bin: [C's nbrs][D's nbrs][___slack___]  (64 KB)
  ...
```

Edit = rewrite ONE page (64 KB). Everything else untouched.

### Data Structures

```rust
struct PageDirEntry {        // 10 bytes per node (vs current 8 bytes for offsets)
    page_id: u32,            // which 64 KB page
    offset_in_page: u16,     // position within page (0..16383)
    count: u16,              // current neighbor count
    capacity: u16,           // allocated slots (count + slack)
}
```

### Query Path Change

```rust
// CURRENT (src/runtime.rs:326-327):
let start = read_u64_from_mmap(offsets_mmap, dense_id.get() as usize) as usize;
let end = read_u64_from_mmap(offsets_mmap, dense_id.get() as usize + 1) as usize;

// PAGED CSR:
let entry = read_page_dir_entry(page_dir_mmap, dense_id.get() as usize);
let page_base = entry.page_id as usize * PAGE_SIZE;
let start = page_base + entry.offset_in_page as usize;
let end = start + entry.count as usize;
```

Almost identical. One extra multiply for page location. ~4 ns overhead.

### Edit Operations

**Add edge (A → B):**
1. Look up A's page_dir entry → page_id, offset, count, capacity
2. If count < capacity: write B's dense_id at slot `offset + count`, increment count
3. If count == capacity: split page (like B-tree page split)
4. Rewrite ONE page file (64 KB)
5. Repeat for B's reverse page

Cost: ~200 μs (two 64 KB page writes with fsync)

**Add node:** Append page_dir entry, allocate slots in last page. Cost: ~1 μs.

**Delete node:** Set page_dir count = 0 (tombstone). Cost: ~1 μs.

### Page Splits

When a page's slack is exhausted:
1. Split page into two halves
2. Each half gets fresh 30% slack
3. Update page_dir entries for moved nodes

Cost: copy ~64 KB + update ~637 page_dir entries = ~50 KB I/O.
Frequency: depends on edit rate and degree distribution (see Rubber-Duck section).

### Design Decisions

1. **Stable Dense IDs:** New nodes get IDs at the end (N, N+1, ...). Never reassign.
   Deleted nodes are tombstoned. Requires restructuring the key_index from sorted
   array (binary search) to a B-tree or hash map.

2. **Locality-Aware Page Assignment:** At build time, assign connected nodes to the
   same page (BFS/DFS ordering). Multi-hop queries hit fewer pages.

3. **Background Compaction:** Merge underfull pages, redistribute slack, reclaim
   tombstoned IDs. Optional — system works without it, just gets gradually
   less space-efficient.

4. **Large Nodes:** Nodes with degree > 16,384 span multiple chained pages.
   Rare but must be handled.

5. **Crash Consistency:** Adding edge touches TWO pages (forward + reverse).
   Needs a WAL (write-ahead log) for atomicity.

### Implementation Estimate

| Component | Lines of Rust |
|---|---|
| Core page system (build, query, edit) | ~500 |
| Large node handling (multi-page chains) | ~100 |
| Stable dense ID + key_index restructuring | ~200 |
| WAL for crash consistency | ~300 |
| Concurrent reader/writer handling | ~500 |
| Tests | ~400 |
| **Total** | **~2,000** |

---

## Solution B: Delta Layer + Binary Rebuild

### Core Idea

Keep the immutable CSR as the fast path. Accumulate mutations in a small in-memory
delta buffer. Queries check CSR then delta. Periodically merge delta into CSR via
binary rebuild (~16 seconds for 50 GB).

```
┌─────────────────────────────────┐
│ IMMUTABLE CSR (99.9% of reads)  │ ← existing snapshot, untouched
│ offsets[] + peers[]             │
└─────────────────────────────────┘
            +
┌─────────────────────────────────┐
│ DELTA BUFFER (small, mutable)   │ ← recent writes
│ added_edges: Vec<(u32, u32)>    │
│ deleted_edges: BitVec           │
└─────────────────────────────────┘
```

### Query Path Change

```rust
// ~15 lines of new code:
fn read_neighbor_ids_with_delta(
    &self,
    dense_id: DenseNodeId,
    direction: WalkDirection,
) -> Vec<u32> {
    let mut base = self.read_neighbor_ids(dense_id, direction);  // existing CSR
    base.retain(|&edge_idx| !self.delta.is_deleted(edge_idx));   // filter deletes
    base.extend(self.delta.added_neighbors(dense_id, direction)); // add new
    base
}
```

### Binary Rebuild

```
fn binary_rebuild(existing_snapshot: &Path, delta: &Delta, output: &Path):
    // Stream-read existing offsets + peers
    // Merge sorted delta additions, skip delta deletions
    // Write new offsets + peers
    // ~16 seconds for 50 GB (stream I/O, no CSV parsing)
```

Double-buffer: build new snapshot while serving from old one. Atomic swap when done.
Zero query downtime during rebuild.

### Implementation Estimate

| Component | Lines of Rust |
|---|---|
| DeltaBuffer struct (add, delete, query) | ~80 |
| Query path integration | ~30 |
| Binary rebuild (stream-merge) | ~150 |
| Double-buffer swap | ~40 |
| Tests | ~100 |
| **Total** | **~400** |

### Performance Characteristics

| Delta Size | Write Cost | Read Overhead | Rebuild Trigger |
|---|---|---|---|
| 100 entries | O(1) append | +0.01% | optional |
| 10,000 entries | O(1) append | +0.1% | optional |
| 1,000,000 entries | O(1) append | +1-2% | recommended |
| 10,000,000 entries | O(1) append | +5-9% | strongly recommended |

---

## Solution C: Do Nothing

Keep the current implementation. Users who need mutations rebuild from updated CSV.

- Pro: Zero complexity, zero maintenance, zero risk
- Con: 15-25 minute rebuild for 50 GB, data staleness measured in minutes/hours
- Con: Competitive disadvantage vs mutable graph databases

---

## 50 GB Simulation

### Dataset Parameters (scaled from 2 GB benchmark)

```
Raw CSV:     50 GB
Nodes:       100,000,000 (100M)
Edges:       900,000,000 (900M)
Avg degree:  9
```

### Snapshot Size Comparison

| File | Flat CSR | Paged CSR |
|---|---|---|
| Forward offsets / page_dir | 763 MB | 954 MB |
| Forward peers / pages | 3,354 MB | 4,906 MB |
| Reverse offsets / page_dir | 763 MB | 954 MB |
| Reverse peers / pages | 3,354 MB | 4,906 MB |
| node_table.bin | 1,526 MB | 1,526 MB |
| strings.bin | 2,861 MB | 2,861 MB |
| key_index.bin | 381 MB | 381 MB |
| **Total** | **13.0 GB** | **16.5 GB (+27%)** |

### Edit Operation Comparison

| Operation | Flat CSR (CSV) | Flat CSR (Binary) | Paged CSR | Delta Layer |
|---|---|---|---|---|
| Add 1 edge | 15-25 min | ~16 sec | ~200 μs | ~1 μs |
| Add 10K edges | 15-25 min | ~16 sec | ~2.3 sec | ~10 μs + 16s rebuild |
| Add 1M edges | 15-25 min | ~16 sec | ~20 sec | ~1 ms + 16s rebuild |
| Add 100M edges | 20-35 min | ~20 sec | ~2.5 min | ~100 ms + 20s rebuild |

### Query Performance Comparison (1-hop, avg degree 9)

| Step | Flat CSR | Paged CSR | Delta Layer (10K delta) |
|---|---|---|---|
| Key resolution (log₂(100M) × 200ns) | ~5.4 μs | ~5.4 μs | ~5.4 μs |
| Neighbor range lookup | ~210 ns | ~214 ns (+4ns) | ~210 ns |
| Read 9 neighbors | ~100 ns | ~100 ns | ~100 ns |
| Delta check | N/A | N/A | ~100 ns |
| Resolve neighbor keys | ~48.6 μs | ~48.6 μs | ~48.6 μs |
| **Total** | **~54.3 μs** | **~54.3 μs** | **~54.4 μs** |
| **Overhead vs flat** | **baseline** | **+0.007%** | **+0.2%** |

### Steady-State Simulation (30 days, 10K edits/hour)

| Metric | Flat CSR | Delta Layer |
|---|---|---|
| Rebuild frequency | every 1-6 hours | every hour (16s each) |
| Time spent rebuilding/mo | 40-240 hours | 11.5 minutes |
| Data freshness | 1-6 hours stale | <16 seconds stale |
| Edit I/O per month | 8,600-66,960 GB | ~168 GB |
| Query throughput | reduced 6-33% | ~100% (no pauses) |

### Slack Longevity (Paged CSR)

At 10K edits/hour uniformly distributed across 78,493 pages:

```
Edits per page per hour: 10,000 × 1,274 / 100,000,000 = 0.127
Slack per page: 4,915 slots
Hours until first page split: 4,915 / 0.127 = 38,700 hours = 4.4 years

At   100K edits/hour: first split in 161 days
At 1,000K edits/hour: first split in 16 days
```

**Caveat:** These numbers assume uniform distribution. Real graphs have power-law
degree distributions. Low-degree nodes (majority) get very little per-node slack
and will trigger page rearrangements much sooner. See Rubber-Duck section.

---

## Rubber-Duck Corrections

After completing the Paged CSR analysis, the following errors were identified
through critical self-review:

### Correction 1: Rebuild Cost Was Overstated by 55-90×

The original analysis compared Paged CSR edit time (200 μs) against CSV rebuild
(15-25 minutes). But binary rebuild — reading existing snapshot, merging delta,
writing new snapshot — takes only ~16 seconds for 50 GB. The real speedup of
Paged CSR over binary rebuild is 80,000×, not 4,500,000×.

### Correction 2: Implementation Estimate Was Understated by 3-4×

Original claim: "~500 lines of Rust." Honest count including large-node handling,
stable dense ID restructuring, WAL for crash consistency, concurrent access, and
tests: ~1,500-2,000 lines.

### Correction 3: Stable Dense IDs Break Key Index

Current `key_index.bin` is a sorted array enabling binary search
(`src/runtime.rs:97-116`). Append-only dense IDs for new nodes break sorted
order. Requires replacing binary search with B-tree index or hash map —
additional complexity affecting the query hot path.

### Correction 4: Degree Distribution Undermines Slack Uniformity

"30% slack" sounds uniform, but in power-law graphs:

| Degree | Nodes (%) | Slack slots | Edits to fill |
|---|---|---|---|
| 1-2 | ~40% | 1 slot | 1 edit |
| 3-5 | ~25% | 1-2 slots | 1-2 edits |
| 5-20 | ~20% | 2-6 slots | 2-6 edits |
| 20-100 | ~10% | 6-30 slots | 6-30 edits |
| 100+ | ~5% | 30+ slots | 30+ edits |

Low-degree nodes (the majority) exhaust their slack after 1-2 edits, triggering
page rearrangements far more often than the "4.4 years" uniform estimate suggests.

Fix: use a minimum slack floor (e.g., `max(ceil(degree × 0.3), 4)`) so every node
gets at least 4 slots of slack regardless of degree.

### Correction 5: Crash Consistency Was Hand-Waved

Adding edge A→B touches two pages (A's forward, B's reverse). A crash between
the two writes leaves the graph inconsistent. A write-ahead log (WAL) is needed
for atomicity — ~300 additional lines and measurable write-path overhead.

---

## Timeline Traverser Analysis

### Decision Frame

- **Fork in the road:** How should Knight Bus handle graph mutations?
- **Desired outcome:** Sub-second edge edits on 50 GB+ graphs without sacrificing O(1) query speed.
- **Hard constraints:** mmap-compatible, <5% read overhead, implementable by small team.
- **Time horizon:** Week 1 → Month 1 → Quarter 1 → Year 1.
- **What counts as failure:** Shipping complex infrastructure nobody needs, OR losing users to the "no mutations" limitation.

### Timeline A: Ship Paged CSR

- **Opening move:** Implement PageDirEntry, page allocator, page-aware build pipeline.
- **Week 1:** Read path works with paged snapshots. Team feels excited.
- **Month 1:** Hit stable-ID problem, large-node problem, crash consistency problem. Scope triples from ~500 to ~1,800 lines. Team asks: "Are we building a database engine now?"
- **Quarter 1:** Works end-to-end but test coverage is incomplete for edge cases. Space overhead is 35-40% (not 27%) due to per-node slack issues. Three months spent on infrastructure, zero new features shipped.
- **Year 1:** Stable and battle-tested, but only 2 users actually needed real-time edits. Permanent maintenance burden for over-engineered solution.
- **Likelihood:** 60%. Technically achievable but high scope risk.

### Timeline B: Ship Delta Layer + Binary Rebuild

- **Opening move:** Add DeltaBuffer struct (~200 lines). Modify read_neighbor_ids to merge CSR with delta.
- **Week 1:** Delta buffer works. Binary rebuild works (16 seconds). Shipping candidate. Team has time left for features.
- **Month 1:** In production. Users batch edits, trigger binary rebuild. 16-second rebuild is invisible with double-buffering. No problems surface.
- **Quarter 1:** Power user hits 1M edits/hour. Delta check adds ~1 μs per query (54 μs → 55 μs). Nobody notices.
- **Quarter 2 (stress test):** 10M edits/hour. Delta check adds ~5 μs (+9%). Rebuild every 10 minutes keeps delta small. 2.7% time spent rebuilding. Acceptable.
- **Year 1:** Covers 99% of users. The 1% with extreme needs: upgrade path to Paged CSR exists but is not yet justified.
- **Likelihood:** 90%. Minimal risk, proven pattern.

### Timeline C: Do Nothing

- **Opening move:** Ship features instead of mutation infrastructure.
- **Month 1:** First user asks about mutations. Answer: "Rebuild from CSV."
- **Quarter 1:** Three users have asked. Some accept. Some leave for Neo4j.
- **Year 1:** "No mutations" is the #1 complaint. Simple and reliable core, but growing competitive pressure.
- **Likelihood:** 100% (default path).

### Timeline D: Hybrid (Delta Layer now, Paged CSR if data justifies)

- **Opening move:** Ship Delta Layer. Instrument edit rates.
- **Month 3:** Data shows 95% of users do <10K edits/hour. Nobody does >10M.
- **Quarter 2:** Paged CSR not justified. Keep Delta Layer. Revisit in 6 months.
- **Year 1:** Delta Layer covers everyone. Paged CSR is a documented design, not shipped code. Zero maintenance burden for unused infrastructure.
- **Likelihood:** 85%. Data-driven, avoids premature optimization.

### Cross-Timeline Comparison

| Path | Upside | Downside | Reversibility | Regret Risk |
|---|---|---|---|---|
| A: Paged CSR | 80,000× faster edits, never degrades | 3 months, ~2000 lines, crash consistency complexity | Low | High if users don't need real-time edits |
| B: Delta Layer | ~400 lines, ships in 1 week, 95% of benefit | Reads degrade with large delta, periodic rebuild | High | Low — worst case it's a stepping stone |
| C: Do Nothing | Zero work, zero risk | #1 user complaint, competitive gap | High | Medium — may lose early adopters |
| D: Hybrid | Data-driven, right-sized | Requires discipline to not over-build | High | Lowest |

---

## Recommendation

### Start with Delta Layer + Binary Rebuild (Timeline D)

1. **Ship Delta Layer** (~400 lines, 1 week). O(1) writes, negligible read overhead for small deltas.
2. **Ship Binary Rebuild** (stream-merge existing snapshot + delta → new snapshot in ~16 seconds for 50 GB).
3. **Instrument edit rates** in production. The Paged CSR vs Delta Layer debate hinges on one number: how many edits per hour do real users make?
4. **Document Paged CSR** as a future upgrade path. Don't build it until data proves it's needed.

### Decision Criteria for Upgrading to Paged CSR

Upgrade when ALL of these are true:
- Real users consistently exceed 10M edits/hour
- The +5-9% query overhead from large deltas is unacceptable to those users
- 16-second binary rebuilds (even with double-buffering) are unacceptable

Until then, Delta Layer is sufficient and 5× simpler.

### The Principle

The most dangerous thing in engineering is building an elegant solution to a
problem nobody has yet. Paged CSR is elegant. Delta Layer is sufficient.
Ship sufficient in 1 week, let data decide if elegant is worth 3 months.
