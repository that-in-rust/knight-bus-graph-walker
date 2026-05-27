# Rubber Duck Debugging: v0.0.3 Claims — Are We Lying to Ourselves?

*Deep stress-test of every claim made in the v0.0.3 proposal.
Each claim is examined against the actual code, the actual math,
and the actual engineering required. Follows the Timeline
Traverser template to explore what happens when each claim
meets reality.*

---

## Decision Frame

- **Fork in the road:** We proposed 4 features for v0.0.3 with
  specific performance claims. Before building, we need to know:
  which claims are solid, which are aspirational, and which are
  fantasy?

- **Desired outcome:** Honest assessment of what v0.0.3 can
  actually deliver, so we don't build something and then discover
  the headline number was wrong.

- **Hard constraints:** 4,710 LOC existing codebase. Single
  developer. v0.0.3 must ship, not stall in perfectionism.

- **What would count as failure:** We ship v0.0.3 with a
  benchmark that shows WORSE numbers than claimed. Or we spend
  3 months on something we estimated at 2 weeks.

---

## Claim 1: "Hash Index → 10-50x Faster Key Lookup"

### What the code actually does today

```rust
// src/runtime.rs lines 97-116
pub fn resolve_dense_id(&self, entity_key: &NodeKey) -> Result<DenseNodeId, KnightBusError> {
    let mut low = 0_usize;
    let mut high = self.manifest.node_count as usize;
    let target = entity_key.as_str();

    while low < high {
        let middle = low + (high - low) / 2;
        let dense_id = self.read_key_index_value(middle)?;       // 4B from key_index mmap
        let middle_key = self.key_str_for_dense_id(dense_id)?;   // 16B from node_table mmap
                                                                  // + variable from strings mmap
        match middle_key.cmp(target) {
            Ordering::Less => low = middle + 1,
            Ordering::Greater => high = middle,
            Ordering::Equal => return Ok(DenseNodeId::new(dense_id)),
        }
    }
    // ...
}
```

### The honest analysis

**Each comparison in the binary search touches 3 mmap regions:**
1. `key_index` — read 4 bytes at random offset → possible page fault
2. `node_table` — read 16 bytes (`NodeRecord`) at random offset → possible page fault
3. `strings` — read variable-length key at random offset → possible page fault

For 10M nodes: `log₂(10,000,000) = ~23` comparisons. Each
comparison: 3 potential page faults. Worst case (cold): 23 × 3
= 69 page faults × ~2-5 μs each = **140-350 μs**.

Best case (warm, everything in page cache): 23 comparisons ×
~100ns per comparison = **~2.3 μs**.

**A hash index would do:**
1. Hash the key string → ~50-100ns
2. Read hash table bucket → 1 page fault if cold
3. Compare stored key → 1 read from strings mmap
4. Total: 2 potential page faults

Worst case (cold): 2 × 2-5 μs = **4-10 μs**
Best case (warm): **~150-300ns**

### The 10-50x claim: VERDICT

```
Warm cache:
  Binary search: ~2.3 μs
  Hash index:    ~0.15-0.3 μs
  Speedup: 8-15x ✓ (close to 10x claim)

Cold cache:
  Binary search: 140-350 μs
  Hash index:    4-10 μs
  Speedup: 14-87x ✓ (EXCEEDS the 50x claim)

Mixed (realistic):
  Binary search: ~5-50 μs
  Hash index:    ~0.5-5 μs
  Speedup: 5-20x (within the 10-50x range)
```

**VERDICT: The 10-50x claim is DEFENSIBLE.** The cold-cache case
actually exceeds 50x. The warm-cache case is closer to 10x.
The range is honest.

### BUT — here's what I was hiding from myself:

**Problem 1: Hash table memory overhead.**
A hash table for 10M keys at 70% load factor needs:
- 10M / 0.7 = ~14.3M slots
- Each slot: 8 bytes (u64 offset into key store) + 4 bytes (u32 dense_id) = 12 bytes
- Total: 14.3M × 12B = **~172 MB**
- Current key_index: 10M × 4B = **40 MB**
- **Hash index uses 4.3x more memory than binary search index**

This is not "small overhead" as I claimed. For the OLAP-RAM
variant that's trying to MINIMIZE memory, adding 132 MB of hash
table is counter-productive.

**Problem 2: Hash table file format.**
Current snapshot format has 7 files. Adding a hash table means:
- New file: `hash_index.bin` (~172 MB for 10M nodes)
- Build pipeline must generate it
- Snapshot version must increment (v2 → v3)
- Backward compatibility with v2 snapshots

**Problem 3: String hashing is not free.**
Node keys in this codebase are arbitrary strings (e.g., file
paths, identifiers). Hashing a 50-character string: ~50-100ns.
This is fine for one lookup but becomes measurable in bulk
operations (10K lookups = 0.5-1ms just for hashing).

**Problem 4: The 10-50x speedup only matters if key lookup is
the bottleneck.** For a 1-hop traversal, key lookup is ONE step.
The rest is: read offsets (1 page), read peers (1 page), decode
results. If key lookup is 5 μs out of a 20 μs traversal, making
it 0.5 μs saves 4.5 μs — a 1.3x total speedup, not 10-50x.

### Revised claim

> "Hash index: 10-50x faster KEY LOOKUP, but only ~1.3-2x faster
> TOTAL TRAVERSAL. Costs 132 MB extra RAM. Not a priority for
> OLAP-RAM variant."

**Should this be in v0.0.3?** MAYBE. It's a real improvement but
the headline number (10-50x) is misleading because it only
applies to one step of the query pipeline.

---

## Claim 2: "madvise → 30-50% RSS Reduction"

### What the code actually does today

```rust
// src/runtime.rs line 381-385
fn map_file_read_only(path: PathBuf) -> Result<Mmap, KnightBusError> {
    let file = File::open(&path).map_err(|source| KnightBusError::io(&path, source))?;
    unsafe { Mmap::map(&file) }.map_err(|source| KnightBusError::io(path, source))
}
```

No `madvise` hints. The OS uses its default page replacement
policy (LRU or similar). Pages stay resident until memory
pressure forces eviction.

### The honest analysis

**What `madvise(MADV_SEQUENTIAL)` actually does:**
- Tells the kernel: "I'm reading this file sequentially"
- Kernel response: aggressive read-ahead, drop pages after
  they've been read
- Effect: for sequential scans (PageRank iterating over reverse
  CSR), only a small window of pages is resident at any time

**What `madvise(MADV_DONTNEED)` actually does:**
- Tells the kernel: "I'm done with these pages"
- Kernel immediately drops the pages from the page cache
- Effect: after finishing an iteration, explicitly release pages

**But here's the problem: we're not doing sequential scans yet.**

The current `MmapWalkRuntime` does RANDOM ACCESS traversals
(look up a key → read offsets → read peers). Random access
patterns don't benefit from `MADV_SEQUENTIAL`. They might even
be HURT by it (kernel does read-ahead on pages you don't need).

`MADV_RANDOM` would be the correct hint for traversals. Its
effect: disable read-ahead, reducing wasted I/O. But this
doesn't reduce RSS — it reduces WASTED RSS from unnecessary
read-ahead. The savings are typically 5-15%, not 30-50%.

**When would we see 30-50% reduction?**

Only when running SEQUENTIAL SCANS (PageRank, BFS over all
nodes, full-graph algorithms). In that case:
- Without hint: OS keeps all touched pages resident → for a
  400 MB peers array, ~400 MB stays resident after full scan
- With `MADV_SEQUENTIAL` + `MADV_DONTNEED`: only the read
  window (~4-16 MB) stays resident during scan, pages are
  dropped after use → ~4-16 MB resident

That IS a 30-50x reduction for the peers array during a scan.
But it only applies DURING algorithm execution, and only if we
implement streaming algorithms that scan sequentially.

### Revised claim

> "madvise: 30-50% RSS reduction DURING SEQUENTIAL ALGORITHM
> EXECUTION (PageRank, BFS). Negligible effect on random-access
> traversals. Requires streaming algorithm implementation to
> realize the benefit."

**The 30-50% number is misleading for the current codebase.**
Without streaming algorithms, adding `madvise` to the existing
traversal code would save maybe 5-15% RSS. The 30-50% claim
is only true if we ALSO implement streaming PageRank (Claim 3).

**Dependency:** This claim depends on Claim 3. They're not
independent features — they're one feature (streaming algorithms
with memory hints).

---

## Claim 3: "Streaming PageRank — 100 MB Resident, 5 Seconds"

### The math

**PageRank needs:**
1. Score array: `N × 8 bytes` (f64 per node)
   - 10M nodes: 80 MB
   - 100M nodes: 800 MB ← THIS DOESN'T FIT IN "100 MB"

2. Reverse CSR traversal: read all incoming edges for each node
   - Streaming: only a window of peers array resident at a time
   - With `MADV_SEQUENTIAL`: ~4-16 MB window

3. New score array (for the next iteration): `N × 8 bytes`
   - Can overwrite in-place? Only if using Gauss-Seidel update.
   - Standard Jacobi iteration: needs old + new = 2 × 80 MB = 160 MB

**For 10M nodes:**
```
Score array (old):      80 MB   (MUST be fully resident)
Score array (new):      80 MB   (MUST be fully resident)
mmap window (CSR):    4-16 MB   (streaming)
Damping/working vars:   <1 MB
────────────────────────────────
Total:               164-176 MB
```

### The "100 MB" claim: VERDICT

**WRONG for Jacobi iteration (standard PageRank).** The minimum
is ~164 MB for 10M nodes because you need TWO score arrays.

**Could be close to 100 MB if:**
- Use in-place update (Gauss-Seidel): only 1 score array = 80 MB
  + 4-16 MB streaming window = 84-96 MB
- But Gauss-Seidel PageRank converges differently (update order
  matters, results are iteration-order-dependent, not parallelizable)

**At 100M nodes:** Score array alone = 800 MB. There is no
streaming trick that avoids this — every node needs a score,
and you need random access to ANY node's score when processing
its incoming edges. You can't stream the score array.

**Wait — can you partition?** Yes, if you partition the graph and
process one partition at a time, you could keep only one
partition's scores in memory. But this requires:
- Graph partitioning (non-trivial, adds build-time complexity)
- Cross-partition edges (need special handling)
- Multiple passes over the CSR data
- This is NOT "streaming PageRank" — it's distributed PageRank
  on one machine

### Revised claim

> "Streaming PageRank: ~165 MB for 10M nodes (Jacobi), ~85 MB
> (Gauss-Seidel, with convergence caveats). Grows linearly:
> 800+ MB for 100M nodes regardless of streaming. Not as
> dramatic as claimed for large graphs."

**The "100 MB" headline was wrong.** Honest number for 10M nodes
is 85-165 MB depending on algorithm variant. For 100M nodes,
it's 800+ MB minimum (score array alone).

### The "5 seconds" claim: analysis

**PageRank computation for 10M nodes, 100M edges, 20 iterations:**

Each iteration:
- For each node (10M), read its incoming edges from reverse CSR
- Sum incoming neighbor scores / neighbor out-degree
- Memory bandwidth: read 400 MB peers + 80 MB scores per iteration
- Total read per iteration: ~480 MB
- Memory bandwidth (DDR4): ~30-50 GB/s
- Time per iteration (bandwidth-bound): 480 MB / 40 GB/s = **~12 ms**
- 20 iterations: **~240 ms**

But this assumes everything in L3 cache or RAM. With mmap:
- First iteration: cold pages, many faults
- Subsequent iterations: pages warm, but may be evicted if
  we used MADV_DONTNEED (the exact hint I proposed for saving RSS)
- If we KEEP pages resident (no DONTNEED): fast iterations but
  more RSS
- If we DROP pages (DONTNEED): low RSS but slow re-read on next
  iteration

**THE FUNDAMENTAL TRADEOFF I WAS HIDING:**

```
Low RSS  (MADV_DONTNEED between iterations) → slow (re-fault pages)
Fast     (keep pages resident)               → high RSS

You CANNOT have both low RSS AND fast PageRank at the same time.
```

This is the entire RAM vs Latency tradeoff in one sentence.

**Realistic timing estimates:**

```
Scenario 1: Pages pinned (OLAP-Latency mode)
  Score arrays: in RAM (80-160 MB)
  CSR: in RAM (400 MB reverse peers)
  Per iteration: ~12-20 ms (memory bandwidth bound)
  20 iterations: ~240-400 ms
  Total: ~0.5-1 sec ✓ (claim was "5 sec", this is BETTER)

Scenario 2: Pages streaming (OLAP-RAM mode, MADV_SEQUENTIAL)
  Score arrays: in RAM (80-160 MB, must be)
  CSR: streaming (re-read from SSD each iteration)
  SSD sequential read: ~2-3 GB/s
  Per iteration: 400 MB / 2.5 GB/s = ~160 ms
  20 iterations: ~3.2 sec
  Total: ~3-5 sec ✓ (matches claim, but only on NVMe SSD)

Scenario 3: Pages streaming (OLAP-RAM mode, on spinning disk)
  Per iteration: 400 MB / 150 MB/s = ~2.7 sec
  20 iterations: ~54 sec
  Total: ~1 min (MUCH slower, claim doesn't hold on HDD)
```

### Revised claim

> "Streaming PageRank on NVMe SSD: 3-5 sec for 10M nodes
> (matches claim). On spinning disk: ~1 min (doesn't match).
> Pinned mode: 0.5-1 sec (exceeds claim). RSS: 85-165 MB for
> 10M nodes, 800+ MB for 100M nodes (not 100 MB as claimed)."

---

## Claim 4: "40x Less RAM AND 24x Faster — In the Same Sentence"

### Where these numbers came from

```
"Neo4j: 4 GB RAM, 120 seconds. Knight Bus: 100 MB RAM, 5 seconds."
RAM ratio: 4000 MB / 100 MB = 40x
Time ratio: 120 sec / 5 sec = 24x
```

### The honest analysis

**Neo4j's 4 GB for GDS:**
- GDS projection creates an in-memory CSR from the record store
- For 10M nodes, 100M edges: GDS estimates ~2-4 GB heap
  for the projection + algorithm working set
- Plus Neo4j's own heap overhead: ~1-2 GB for the base server
- Total: 3-6 GB (4 GB is in the range, claim is fair)

**But the comparison is apples to oranges:**

Neo4j GDS:
1. Projects graph from record store → in-memory CSR (this is where
   the RAM goes — building a CSR that you ALREADY HAVE)
2. Runs PageRank on the in-memory CSR
3. GDS PageRank computation itself is also ~0.5-2 sec
4. The "120 seconds" includes projection time, not just algorithm

Knight Bus:
1. CSR already exists (it IS the storage format)
2. Runs PageRank on the CSR directly
3. Knight Bus skips step 1 entirely

**So the "24x faster" is really "we skip the 100-second projection
and the actual computation is similar."** If Neo4j GDS had an
option to run PageRank directly on a pre-built CSR (which it
doesn't, but hypothetically), the speed difference would be
much smaller — maybe 2-5x (Rust vs JVM + GC pauses).

**The RAM comparison is more honest:**
- Neo4j GDS: builds a SECOND copy of the graph in heap (CSR
  projection) → ~2-4 GB heap for the projection
- Knight Bus: CSR IS the storage, no second copy → ~85-165 MB
  for score arrays only
- The RAM savings are REAL: you avoid duplicating the graph

### But there's a catch I wasn't admitting:

**Knight Bus today has NO properties, NO node labels, NO
relationship types.** The current CSR stores only topology
(who connects to whom). Neo4j GDS projects properties,
relationships by type, and node labels.

A fair comparison requires Knight Bus to also store and access
properties. Once you add property columns:
- 10M nodes × 5 properties × 8B = 400 MB property data
- Some properties must be resident for algorithms (e.g., edge
  weights for weighted PageRank)
- RSS grows from 85-165 MB to 200-500 MB

**Revised RAM comparison:**
```
Neo4j GDS (10M nodes, 100M edges): 3-6 GB
Knight Bus (same, with properties): 200-500 MB
RAM ratio: 6-30x (not 40x)
```

**Revised speed comparison:**
```
Neo4j GDS total (projection + algorithm): 60-120 sec
Knight Bus streaming PageRank: 3-5 sec
Speed ratio: 12-40x (overlaps with claim but lower bound is 12x, not 24x)

But if you compare JUST the algorithm (no projection):
Neo4j GDS PageRank computation only: ~2-10 sec
Knight Bus streaming PageRank: 3-5 sec
Speed ratio: 0.4-3x (Knight Bus might be SLOWER in streaming mode!)
```

### The devastating finding

**If you compare algorithm-only time (no projection), Knight Bus
in streaming/OLAP-RAM mode might not be faster than Neo4j GDS
at all.** Neo4j GDS runs PageRank on a pinned in-memory CSR.
Knight Bus OLAP-RAM streams through mmap'd CSR with page faults.

The speed advantage comes ENTIRELY from skipping projection.
If Neo4j GDS pre-built and cached the projection (which it CAN
do with named graph projections), the gap narrows dramatically.

**The HONEST headline:**
> "Knight Bus eliminates the 60-100 second projection step that
> Neo4j GDS requires. The algorithm itself runs at similar speed.
> Total speedup: 10-30x. RAM savings: 6-30x (because no second
> copy of the graph)."

This is still impressive! But it's a different story than
"fundamentally faster algorithm execution." It's "we skip a
step that Neo4j can't skip because of its storage format."

---

## Claim 5: "~1,300 LOC Total for All 4 Features"

### Feature-by-feature estimate reality check

**Hash index (~300 LOC claimed):**
```
hash_index.rs:
  - Build hash table from key_index at snapshot build time
  - FxHash or xxHash implementation (or use a crate)
  - Probe function with collision handling (linear probing)
  - Serialize/deserialize hash table to snapshot file
  - Integration with MmapWalkRuntime (new field, new method)
  - Snapshot format version bump (v2 → v3)
  - Tests (at least 5: empty, single, collision, missing, large)
  
Realistic LOC: 400-600 (not 300)
If using a crate for hashing: maybe 250-350
```

**madvise hints (~100 LOC claimed):**
```
Changes to map_file_read_only:
  - Platform-conditional madvise calls (Linux only, no-op on macOS)
  - New function: advise_sequential(mmap), advise_random(mmap)
  - Integration: call advise_random on open, advise_sequential
    before algorithm scan
  - cfg(target_os) guards

Realistic LOC: 50-80 (claim of 100 was actually too HIGH)
But: madvise alone is useless without algorithms that scan.
So the real cost is madvise + streaming algorithm = combined.
```

**Streaming PageRank (~500 LOC claimed):**
```
page_rank.rs:
  - PageRankConfig struct (damping, max_iterations, tolerance)
  - Score initialization (1/N for all nodes)
  - Iteration loop:
    · For each node, sum scores of incoming neighbors
    · Apply damping factor
    · Check convergence (L1 norm of delta)
  - Result type (scores + metadata)
  - Integration with MmapWalkRuntime:
    · read_reverse_neighbors(dense_id) → iterate reverse CSR
    · read_out_degree(dense_id) → compute from offsets
  - madvise integration (SEQUENTIAL before iteration, DONTNEED after)
  - Tests:
    · Empty graph → all scores 1/N
    · Single node → score 1.0
    · Linear chain → expected distribution
    · Star graph → center has highest score
    · Convergence within max_iterations
    · Match known PageRank values (e.g., Wikipedia example)

Realistic LOC: 300-500 (claim was 500, might be achievable)
But: this is the MINIMUM PageRank. No parallelism, no SIMD,
no partitioning, no weighted edges. Just basic Jacobi iteration.
```

**Benchmark harness (~400 LOC claimed):**
```
benchmark.rs (or extend existing bench.rs):
  - Generate synthetic graph (N nodes, M edges, random Erdős–Rényi)
  - Build snapshot from synthetic data
  - Measure: wall time, peak RSS (via sysinfo, already used)
  - Run PageRank, record time + RSS
  - Output: JSON report with timing + memory
  - CLI: `knight-bus benchmark --nodes 10000000 --edges 100000000`
  
BUT: existing bench.rs is already 458 LOC and has measurement
infrastructure (BenchmarkScenarioRunner, etc.)

Realistic NEW LOC: 200-400 (can reuse existing bench infrastructure)
```

### Total LOC revised

```
Hash index:        400-600 (claimed 300)
madvise:            50-80  (claimed 100)
Streaming PageRank: 300-500 (claimed 500)
Benchmark harness:  200-400 (claimed 400)
──────────────────────────────────────────
Total:            950-1,580 (claimed 1,300)
```

**VERDICT: The 1,300 LOC estimate is within the revised range
(950-1,580). The claim is roughly right but the distribution
across features was wrong.** Hash index was underestimated,
madvise was overestimated, PageRank and benchmark were roughly
right.

### The hidden LOC cost I wasn't counting:

**Test code.** The existing codebase has high test standards (23
tests, zero clippy warnings). For 4 new features:
- Hash index tests: ~100-200 LOC
- PageRank tests: ~150-300 LOC
- Benchmark tests: ~50-100 LOC
- Integration tests: ~100-200 LOC

Total test LOC: ~400-800

**Grand total including tests: 1,350-2,380 LOC.** The headline
"~1,300 LOC" excluded tests entirely.

---

## Timeline A: "Ship All 4 Features As Proposed"

### Opening Move
Start building hash index, madvise, PageRank, and benchmark
harness in parallel.

### Week 1
- Hash index: basic FxHash probe implemented, tests passing
- madvise: trivial, done in day 1
- PageRank: score initialization, first iteration working
- Benchmark: synthetic graph generator working

**Lived experience:** Productive but scattered. Four features
in parallel = frequent context switching. Each feature touches
different parts of the codebase.

### Month 1
- All 4 features merged, ~1,500-2,000 LOC total (with tests)
- Benchmark shows: PageRank on 10M nodes in ~3-5 sec, ~165 MB RSS
- BUT: headline "40x less RAM, 24x faster" needs caveats
- Blog post / README update with benchmark results
- Snapshot format is now v3 (backward compatibility issue)

**Lived experience:** The numbers ARE impressive (10-30x faster,
6-30x less RAM) but you spend 2 weeks writing caveats instead
of celebrating. "Well, the 40x is really 6-30x, and the 24x is
really 10-30x, and it depends on SSD vs HDD, and..."

### Quarter 1
- v0.0.3 shipped with caveated benchmark
- Some interest from Neo4j users searching for alternatives
- But: only PageRank is implemented. Users ask: "What about
  Dijkstra? BFS? Community detection?"
- You're now committed to a hash index that costs 132 MB RAM
  and a streaming-only algorithm model

### Likelihood: 60%

Risk: scope creep (4 features), benchmark messaging complexity.

### Stress Points
- Week 2: Hash index collision handling is more complex than
  expected (string keys with common prefixes)
- Month 1: The benchmark numbers are good but not "dazzling"
  because of caveats. "10-30x faster" doesn't have the same
  ring as "100x faster"
- Month 2: Users find that OLAP-RAM streaming PageRank is
  ~same speed as Neo4j GDS computation-only. The win is
  projection elimination, not algorithm speed.

### Inflection Points
- If benchmark shows >20x on real-world graph: viral potential
- If benchmark shows <5x: "why did we bother?" moment
- Hash index RAM cost (132 MB) may undercut the "less RAM"
  narrative

---

## Timeline B: "PageRank Only — One Feature, Done Perfectly"

### Opening Move
Drop hash index and madvise as standalone features. Build ONLY
streaming PageRank with integrated madvise and existing binary
search for key lookup.

### Week 1
- `page_rank.rs`: 300-500 LOC
- Jacobi iteration, no frills, on existing `MmapWalkRuntime`
- madvise hints baked into the PageRank scan loop (not a
  separate feature)
- First test: 5-node graph, verify scores match Wikipedia example

### Week 2
- Synthetic graph generator (100 LOC, Erdős-Rényi)
- Benchmark: generate 10M-node graph, build snapshot, run PageRank
- Measure wall time + RSS via existing sysinfo infrastructure
- First real number: "PageRank on 10M nodes: X sec, Y MB"

### Month 1
- v0.0.3 ships with ONE new capability: `knight-bus pagerank`
- Benchmark in README: "PageRank on 10M nodes, 100M edges:
  3.5 sec, 165 MB RSS" (or whatever the ACTUAL number is)
- Compare with Neo4j GDS: "Same algorithm, same data, 15x
  faster, 20x less RAM" (honest, no caveats needed)
- No snapshot format change (no hash index = still v2)
- Total new LOC: ~500-800 (with tests)

**Lived experience:** FOCUSED. One feature, done well, measured
honestly. The README benchmark is simple and compelling. No
caveats needed because the comparison is straightforward.

### Quarter 1
- v0.0.3 gets attention from "Neo4j PageRank is slow" searchers
- Users try it, confirm the numbers on their own data
- Requests pour in for more algorithms → clear roadmap for v0.0.4
- No backward compatibility issues (snapshot v2 unchanged)

### Likelihood: 85%

Low scope, low risk, high focus.

### Stress Points
- Week 1: "But we only have ONE algorithm. Is that enough?"
  (yes — one algorithm, perfectly benchmarked, is more
  convincing than four half-finished features)
- Month 2: "When will you add Dijkstra?" (roadmap, not crisis)

### Inflection Points
- The ACTUAL benchmark number determines everything. If PageRank
  is genuinely 15x+ faster on a real 10M-node graph, this is the
  demo that goes viral.
- If PageRank is only 2-3x faster (because streaming mmap is
  slow on this machine's SSD), we learn that OLAP-Latency (pinned)
  is necessary — and that's a USEFUL finding.

---

## Timeline C: "Benchmark First — Measure Before Building"

### Opening Move
Don't build any new features yet. Instead, benchmark the EXISTING
`MmapWalkRuntime` at scale to establish a baseline. THEN decide
what to build.

### Week 1
- Build synthetic graph generator (100 LOC)
- Generate 1M, 10M node graphs
- Build snapshots using existing `build_snapshot_from_paths`
- Measure: snapshot build time, snapshot size, query latency
- Measure: RSS during build, RSS during query
- Run existing traversal benchmarks (already in bench.rs)

### Week 2
- Implement MINIMAL PageRank (200 LOC, no optimization)
- Run it on 1M and 10M node snapshots
- Measure: iteration time, total time, RSS
- Compare with published Neo4j GDS numbers
- Answer: "How fast is Knight Bus PageRank WITHOUT any
  optimization? Is mmap + CSR already enough?"

**This is the experiment that collapses all uncertainty.**

If unoptimized PageRank on existing MmapWalkRuntime is already
5-10x faster than Neo4j GDS total (projection + computation):
→ Ship it as v0.0.3. The storage format IS the optimization.
→ No need for hash index, madvise, or streaming tricks.

If unoptimized PageRank is only 1-3x faster:
→ madvise + streaming optimizations ARE needed
→ But now you know the BASELINE, so you can measure the impact
  of each optimization precisely.

### Month 1
- v0.0.3 ships with PageRank + honest benchmark
- Benchmark methodology is published (reproducible)
- Numbers are MEASURED, not estimated

### Likelihood: 90%

Lowest risk. No scope creep. Answers the most important question
first: "Is the existing storage format already fast enough?"

### Stress Points
- Week 1: "We're not building anything new? That feels unproductive."
  (measuring IS building — building confidence in the numbers)
- Month 1: If the baseline is disappointing (2-3x, not 10-30x),
  that's emotionally tough but informationally priceless.

### Inflection Points
- The baseline measurement is the SINGLE MOST IMPORTANT DATA POINT
  for the entire project. All future decisions depend on it.
- If baseline PageRank is fast: the thesis is proven, celebrate.
- If baseline PageRank is slow: we learn that mmap isn't enough,
  and pinned/OLAP-Latency is necessary. Also valuable.

---

## Cross-Timeline Analysis

| | A: All 4 Features | B: PageRank Only | C: Benchmark First |
|---|---|---|---|
| **Upside** | Impressive feature list | One feature, zero caveats | Measured truth |
| **Downside** | Caveated benchmarks, scope creep | "Only one algorithm?" | "We didn't ship features" |
| **Reversibility** | Low (hash index format change) | High (no format change) | **Highest** (decide after data) |
| **Regret risk** | "We built the wrong 4 things" | "We should've built more" | "We wasted 2 weeks measuring" |
| **LOC** | 1,500-2,400 | 500-800 | 300-500 |
| **Time to ship** | 4-6 weeks | 2-3 weeks | **1-2 weeks** |
| **Benchmark honesty** | Requires caveats | Clean comparison | **Measured, not estimated** |
| **Risk of wrong headline** | HIGH (40x → actually 6-30x) | MEDIUM (need to verify) | **ZERO** (headline IS the measurement) |

---

## Decision Filter

### Which path is strongest if everything goes normally?

**Timeline B: PageRank Only.** One algorithm, done perfectly,
with an honest benchmark. No format changes, no memory overhead
from hash tables, no caveats. "PageRank on 10M nodes: 3 sec,
165 MB. Neo4j GDS: 120 sec, 4 GB." Clean. Dazzling. True.

### Which path is safest if things go badly?

**Timeline C: Benchmark First.** If the numbers are
disappointing, you learn that BEFORE committing to features.
If the numbers are great, you ship them AS the feature. Zero
wasted work either way.

### What would reduce uncertainty fastest?

**Timeline C, specifically:**

Build a 10M-node synthetic graph. Run unoptimized PageRank on
existing MmapWalkRuntime. Measure wall time and RSS. This ONE
MEASUREMENT answers:

1. Is the CSR storage format already fast enough? (yes/no)
2. How much does mmap page-faulting cost? (seconds or μs)
3. What's the baseline RSS without any optimization? (MB)
4. How does this compare to Neo4j GDS published numbers? (Xx faster)

If the answer to #1 is YES → ship it (Timeline B, 2 weeks)
If the answer to #1 is NO → optimize (Timeline A minus hash
index, 4-6 weeks)

**My revised recommendation: Do C first (1 week), then B or A
based on what you learn. Don't guess — measure.**

---

## The Claims Scorecard

| Original Claim | Honest Assessment | Status |
|---|---|---|
| Hash index: 10-50x faster key lookup | **TRUE for key lookup alone. FALSE for total query speedup (1.3-2x). Costs 132 MB extra RAM.** | ⚠️ Misleading |
| madvise: 30-50% RSS reduction | **TRUE only during sequential algorithm execution. Negligible for random traversals. Requires streaming algorithms.** | ⚠️ Conditional |
| Streaming PageRank: 100 MB, 5 sec | **~165 MB (not 100 MB) for Jacobi, ~85 MB for Gauss-Seidel. 3-5 sec on NVMe SSD. ~60 sec on HDD.** | ⚠️ Close but overstated |
| Benchmark: 40x less RAM, 24x faster | **6-30x less RAM, 10-30x faster (total including projection skip). Algorithm-only speed similar to Neo4j GDS.** | ⚠️ Overstated |
| ~1,300 LOC total | **950-1,580 production LOC + 400-800 test LOC = 1,350-2,380 total.** | ⚠️ Underestimated |

### The honest headline for v0.0.3:

> "PageRank on 10M nodes: ~3-5 sec, ~165 MB. Neo4j GDS same
> graph: ~60-120 sec, ~3-6 GB. Knight Bus is 10-30x faster
> and 6-30x less RAM — because it skips the 60-second
> projection step that Neo4j's storage format forces."

That's still a GREAT headline. It's just not "40x and 24x."
The magic is in the projection elimination, not in some secret
algorithm optimization. And that's a BETTER story — because it
means EVERY algorithm you add gets the same speedup for free.
The storage format IS the moat.
