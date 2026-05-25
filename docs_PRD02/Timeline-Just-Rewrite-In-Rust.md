# Timeline Traverser: "What If the Answer Is Just Rewrite in Rust?"

*The rubber duck told us that every v0.0.3 optimization trick
(hash index, madvise, streaming) is solving problems we haven't
proven we have. What if the simplest thing — PageRank in Rust on
the CSR you already built — is already the dazzling demo?*

---

## Decision Frame

- **Fork in the road:** Do we engineer clever optimizations for
  v0.0.3, or do we just write clean Rust code on the existing
  storage format and see if that's already enough?

- **Desired outcome:** A v0.0.3 that makes people say "holy shit"
  when they see the benchmark. With the LEAST engineering effort.

- **Hard constraints:**
  - Must use existing `MmapWalkRuntime` (4,710 LOC, proven, 23 tests)
  - Must be honest — no cherry-picked benchmarks
  - Must be reproducible by anyone with `cargo run`

- **What would count as failure:**
  - PageRank is slower than Neo4j GDS (unlikely but must check)
  - The benchmark requires special hardware or configuration
  - It takes more than 2 weeks to ship

- **The thesis being tested:** "Rewriting in Rust" isn't about
  language speed (1.3-1.5x over Java). It's about what the
  language ENABLES: a storage format (CSR + mmap) that Java's
  GC-managed heap can't efficiently support. The rewrite IS the
  optimization.

---

## Why "Just Rewrite in Rust" Is Actually Three Things

When someone says "just rewrite it in Rust," they usually mean
"make the same thing faster." But that's NOT what Knight Bus is.
It's three changes bundled into one:

```
1. Language:  Java → Rust
   Benefit:   No GC pauses, no JVM warmup, no 12-16 byte object headers
   Speedup:   ~1.3-1.5x (measured across many Java→Rust ports)
   
2. Storage:   Linked-list records → CSR arrays
   Benefit:   No pointer chasing, contiguous memory, cache-friendly
   Speedup:   ~10-100x for traversals (measured, theoretical)
   
3. Runtime:   JVM heap → mmap
   Benefit:   No GC, no projection step, OS manages pages
   Speedup:   Skips 60-100 sec projection step entirely
   RAM:       No second copy of graph in heap → 6-30x less
```

**The language change is the SMALLEST factor.** The storage
format change is what creates the 10-100x. The mmap runtime
is what eliminates the projection step. You could write CSR +
mmap in C, C++, Zig, or Go and get similar results. Rust just
makes it safe and pleasant.

But "Rewrite in Rust" is still the correct marketing phrase
because:
- It's memorable
- It signals "modern, fast, safe"
- It's what people search for ("Neo4j alternative Rust")
- It's true — you ARE rewriting in Rust

---

## What "Just PageRank" Looks Like in the Existing Codebase

### The code you already have

```rust
// src/runtime.rs — this function already does what PageRank needs:
fn read_neighbor_ids(&self, dense_id: DenseNodeId, direction: WalkDirection) -> Vec<u32> {
    let (offsets_mmap, peers_mmap) = match direction {
        WalkDirection::Forward => (&self.forward_offsets, &self.forward_peers),
        WalkDirection::Backward => (&self.reverse_offsets, &self.reverse_peers),
    };
    let start = read_u64_from_mmap(offsets_mmap, dense_id.get() as usize) as usize;
    let end = read_u64_from_mmap(offsets_mmap, dense_id.get() as usize + 1) as usize;
    (start..end)
        .map(|index| read_u32_from_mmap(peers_mmap, index))
        .collect()
}
```

This gives you the incoming neighbors of any node (via reverse
CSR). PageRank is literally: for each node, sum the scores of
its incoming neighbors divided by their out-degree.

### The code you need to ADD

```rust
// page_rank.rs — the entire algorithm, ~80 LOC of core logic

pub fn page_rank(runtime: &MmapWalkRuntime, config: &PageRankConfig) -> PageRankResult {
    let n = runtime.node_count() as usize;
    let damping = config.damping;             // typically 0.85
    let max_iter = config.max_iterations;     // typically 20
    let tolerance = config.tolerance;          // typically 1e-6

    // Step 1: Compute out-degree for each node (from forward offsets)
    // Already available: offsets[id+1] - offsets[id] = out-degree

    // Step 2: Initialize scores
    let mut scores = vec![1.0 / n as f64; n];

    // Step 3: Iterate
    for iteration in 0..max_iter {
        let mut new_scores = vec![(1.0 - damping) / n as f64; n];
        let mut dangling_sum = 0.0;

        // Accumulate dangling node contribution
        for node in 0..n {
            let out_degree = out_degree_of(runtime, node);
            if out_degree == 0 {
                dangling_sum += scores[node];
            }
        }

        // Distribute dangling contribution equally
        let dangling_contrib = damping * dangling_sum / n as f64;
        for score in &mut new_scores {
            *score += dangling_contrib;
        }

        // Main PageRank accumulation (the hot loop)
        for node in 0..n {
            // Read incoming neighbors from reverse CSR
            let in_neighbors = read_reverse_neighbors(runtime, node);
            let mut sum = 0.0;
            for &neighbor in &in_neighbors {
                let neighbor_out_degree = out_degree_of(runtime, neighbor as usize);
                sum += scores[neighbor as usize] / neighbor_out_degree as f64;
            }
            new_scores[node] += damping * sum;
        }

        // Check convergence
        let diff: f64 = scores.iter().zip(&new_scores).map(|(a, b)| (a - b).abs()).sum();
        scores = new_scores;
        if diff < tolerance { break; }
    }

    PageRankResult { scores, iterations, converged }
}
```

**That's it.** No hash index. No madvise. No streaming tricks.
No compression. Just: iterate the existing CSR arrays, accumulate
scores, check convergence.

### Why this is already fast

1. **No projection step.** Neo4j GDS spends 60-100 sec building
   an in-memory CSR from its record store. Knight Bus CSR IS the
   store. Time saved: 60-100 sec.

2. **Contiguous memory reads.** The reverse CSR peers array is a
   flat `[u32]`. Reading incoming neighbors = reading a contiguous
   slice. CPU prefetcher loves this.

3. **No object headers.** Each peer is 4 bytes (u32). In Java/Neo4j
   GDS, each node relationship is a Java object with 12-16 bytes
   of header. Knight Bus stores 3-4x more edges per cache line.

4. **No GC pauses.** Score arrays are `Vec<f64>` on the stack.
   No garbage collector will stop the world mid-iteration.

5. **mmap does what we need.** The OS will page in the CSR data
   as we access it. For a sequential scan (which PageRank is),
   the OS read-ahead heuristic already does ~80% of what explicit
   madvise would do.

---

## Timeline A: "Just PageRank — The 200 LOC Demo"

### Opening Move

Write `page_rank.rs` with the simplest correct PageRank.
No optimizations. No parallelism. No streaming tricks.
Use existing `read_neighbor_ids` or the raw offsets/peers arrays.
Add a CLI subcommand: `knight-bus pagerank --snapshot <dir>`.

### Day 1-2

```
New files:
  src/page_rank.rs     ~120 LOC (algorithm + config + result types)
  
Modified files:
  src/lib.rs           +2 lines (pub mod page_rank; pub use ...)
  src/main.rs          +30 lines (PageRank CLI subcommand)
  
Tests:
  tests/page_rank.rs   ~80 LOC (5-node star, chain, disconnected, convergence)

Total new: ~230 LOC
```

- PageRank runs on existing test snapshots (from 23 existing tests)
- Verify: scores sum to ~1.0, star center has highest score
- No new dependencies, no snapshot format change

### Day 3-4

```
Synthetic graph generator:
  src/synthetic.rs     ~100 LOC (Erdős-Rényi random graph as CSV)
  
CLI:
  `knight-bus generate --nodes 1000000 --edges 10000000 --output /tmp/synth`
  `knight-bus build --nodes-csv /tmp/synth/nodes.csv --edges-csv /tmp/synth/edges.csv --output /tmp/snap`
  `knight-bus pagerank --snapshot /tmp/snap`

Total new: ~100 LOC (generator)
```

- Generate 1M, 10M node graphs
- Build snapshots using existing build pipeline
- Run PageRank, measure wall time

### Day 5

```
Benchmark script + README update:
  benchmarks/pagerank_vs_neo4j.md   (methodology + results)
  README.md                         (add "PageRank: Xsec, Y MB" headline)

Run the actual benchmark:
  1. Generate 10M nodes, 100M edges
  2. Build snapshot
  3. Run `knight-bus pagerank --snapshot <dir>`
  4. Record: wall time, peak RSS (already tracked via sysinfo)
  5. Compare with published Neo4j GDS numbers
```

### Week 2

- Clean up, polish CLI output, write docs
- v0.0.3 released: `knight-bus pagerank`
- Total new LOC: **~330-400** (production + tests)
- Zero new dependencies
- Zero snapshot format changes

### What You Ship

```
$ knight-bus generate --nodes 10000000 --edges 100000000 --output /tmp/synth
Generated: 10,000,000 nodes, 100,000,000 edges

$ knight-bus build --nodes-csv /tmp/synth/nodes.csv \
                   --edges-csv /tmp/synth/edges.csv \
                   --output /tmp/snap
Built snapshot: 960 MB, 45 seconds

$ knight-bus pagerank --snapshot /tmp/snap
PageRank completed:
  nodes:       10,000,000
  edges:       100,000,000
  iterations:  18 (converged at tolerance 1e-6)
  wall time:   X.XX seconds
  peak RSS:    XXX MB
  top 10 nodes: [...]
```

**The X.XX is the number that matters.** Everything depends on it.

### What the number will probably be

Based on the rubber duck analysis:

```
Pessimistic (mmap cold, no optimization):
  Per iteration: ~200-500 ms (SSD random reads for score lookups)
  20 iterations: 4-10 sec
  RSS: ~240 MB (80 MB scores + 80 MB old scores + mmap pages)

Likely (mmap warm after first iteration):
  First iteration: ~500 ms (cold pages)
  Subsequent: ~50-100 ms (pages cached by OS)
  20 iterations: ~1.5-2.5 sec
  RSS: ~400-800 MB (OS caches CSR pages)

Optimistic (small enough to fit in RAM):
  Per iteration: ~20-50 ms (everything in page cache)
  20 iterations: ~0.4-1 sec
  RSS: ~960 MB (entire snapshot in page cache)
```

**Most likely result: 1.5-5 sec, 300-800 MB RSS.**

Neo4j GDS comparison (10M nodes, 100M edges):
```
Neo4j GDS total (projection + computation): 60-120 sec
Neo4j GDS RSS: 3-6 GB

Knight Bus: 1.5-5 sec, 300-800 MB

Speedup: 12-80x
RAM savings: 4-20x
```

**Even the PESSIMISTIC case (10 sec) is 6-12x faster than Neo4j.**
The worst case is still a great headline.

### Likelihood: 90%

Almost nothing can go wrong. The algorithm is textbook. The CSR
exists. The benchmark infrastructure exists. The CLI framework
exists.

### Stress Points

- Day 2: "But the Vec allocations in `read_neighbor_ids` create
  garbage per node." YES — for v0.0.3, this is fine. Optimize
  in v0.0.4 by reading directly from the mmap slice without
  allocation.

- Day 5: The ACTUAL number might be 5-10 sec instead of 1-2 sec.
  That's still great (12-24x faster than Neo4j) but doesn't
  feel as "dazzling." The fix: show both the total time (including
  Neo4j projection) and note that the algorithm-only time is
  similar — the win is architectural.

### Inflection Points

- **If PageRank < 3 sec:** Viral demo potential. "PageRank on
  100M edges in under 3 seconds. On a laptop. In Rust."
- **If PageRank 3-10 sec:** Good but not viral. Still great
  benchmarks. Ship it, optimize in v0.0.4.
- **If PageRank > 10 sec:** mmap overhead is significant. Need
  to add madvise or pinning. Still faster than Neo4j but the
  "just rewrite" thesis needs caveats.

---

## Timeline B: "Just PageRank + One Optimization"

### What if we add ONE optimization to the basic rewrite?

The rubber duck revealed that the biggest waste in the naive
implementation is `read_neighbor_ids` allocating a `Vec<u32>`
for every node on every iteration:

```rust
// Current: allocates a Vec per call
fn read_neighbor_ids(&self, dense_id: DenseNodeId, direction: WalkDirection) -> Vec<u32> {
    // ...
    (start..end)
        .map(|index| read_u32_from_mmap(peers_mmap, index))
        .collect()  // ← allocates heap memory
}
```

For 10M nodes × 20 iterations = 200M allocations. Each
allocation: ~50-100ns (malloc + free). Total: 10-20 sec of
PURE ALLOCATION OVERHEAD.

**The one optimization:** Read directly from the mmap slice
without allocating. PageRank doesn't need to OWN the neighbor
list — it just needs to iterate it.

```rust
// New: zero-allocation neighbor access
fn reverse_neighbor_range(&self, dense_id: u32) -> (usize, usize) {
    let start = read_u64_from_mmap(&self.reverse_offsets, dense_id as usize) as usize;
    let end = read_u64_from_mmap(&self.reverse_offsets, dense_id as usize + 1) as usize;
    (start, end)
}

fn peer_at(&self, index: usize) -> u32 {
    read_u32_from_mmap(&self.reverse_peers, index)
}

// In PageRank:
let (start, end) = runtime.reverse_neighbor_range(node as u32);
for idx in start..end {
    let neighbor = runtime.peer_at(idx);
    sum += scores[neighbor as usize] / out_degrees[neighbor as usize] as f64;
}
```

**This ONE change eliminates 200M allocations per PageRank run.**

### Additional: pre-compute out-degrees

Instead of computing `offsets[id+1] - offsets[id]` for every node
on every iteration (200M subtractions), compute once:

```rust
let out_degrees: Vec<u32> = (0..n)
    .map(|id| {
        let start = read_u64_from_mmap(&runtime.forward_offsets, id) as u32;
        let end = read_u64_from_mmap(&runtime.forward_offsets, id + 1) as u32;
        end - start
    })
    .collect();
// Cost: 40 MB for 10M nodes, computed once
```

### Timeline B numbers

```
Changes vs Timeline A:
  + reverse_neighbor_range() method:  ~10 LOC
  + peer_at() method:                 ~5 LOC  
  + pre-computed out_degrees:         ~10 LOC
  + Use slices instead of Vec:        ~20 LOC changes in page_rank.rs

Total LOC: same ~330-400 LOC
Complexity: marginally higher (but simpler than hash index)
```

Expected performance:
```
Without zero-alloc (Timeline A):     1.5-5 sec
With zero-alloc + pre-computed deg:  0.5-2 sec (2-3x faster)

Why: eliminating 200M allocations saves ~10-20 sec theoretical,
but actual savings are less because the allocator is fast and
many allocations are amortized. Realistic improvement: 2-3x.
```

### Likelihood: 88%

Same as Timeline A but slightly more complex. The optimization
is straightforward and doesn't change the architecture.

---

## Timeline C: "Measure First, Then Ship Whatever Works"

### Opening Move

Same as the rubber duck's Timeline C recommendation. Don't
commit to any feature set. Build the measurement infrastructure
and let the NUMBERS tell you what to ship.

### Day 1-2

```
Build:
  src/synthetic.rs   ~100 LOC (random graph generator)
  src/page_rank.rs   ~120 LOC (naive PageRank, no optimization)
  CLI subcommands    ~30 LOC
  Tests              ~80 LOC

Run measurement suite:
  1. Generate 100K, 1M, 10M node graphs
  2. Build snapshots for each
  3. Run PageRank on each
  4. Record: wall time, RSS, iterations to convergence
  5. Plot: wall time vs graph size (log-log)
```

### Day 3

**The moment of truth.** You have 3 data points:

```
Nodes      Edges      PageRank time     RSS
100K       1M         ???               ???
1M         10M        ???               ???
10M        100M       ???               ???
```

These numbers tell you EXACTLY:
- Is the naive implementation already fast enough?
- Does wall time scale linearly with edges? (should, for PageRank)
- Does RSS stay bounded? (depends on mmap behavior)

### Day 4-5

Based on the measurements:

**IF naive is already < 5 sec for 10M nodes:**
→ Ship it as-is. No optimizations. v0.0.3 = Timeline A.
→ Write benchmark report with MEASURED numbers.
→ Total time: 1 week.

**IF naive is 5-20 sec for 10M nodes:**
→ Add the one optimization from Timeline B (zero-alloc reads).
→ Re-measure. If < 5 sec now, ship.
→ Total time: 1.5 weeks.

**IF naive is > 20 sec for 10M nodes:**
→ mmap overhead is the bottleneck. Need madvise or pinning.
→ This tells you OLAP-Latency is necessary, not just OLAP-RAM.
→ Add madvise(MADV_SEQUENTIAL), re-measure.
→ Total time: 2 weeks.

### Likelihood: 92%

Highest likelihood because the measurement TELLS you what to do.
No guessing, no over-engineering, no under-engineering.

---

## Cross-Timeline Analysis

| | A: Just PageRank | B: PageRank + Zero-Alloc | C: Measure First |
|---|---|---|---|
| **LOC** | ~330-400 | ~330-400 | ~330-400 (same code, different process) |
| **Time to ship** | 1-2 weeks | 1-2 weeks | 1-2 weeks |
| **Confidence in headline number** | Medium (estimated) | Medium (estimated) | **Highest** (measured) |
| **Risk of wrong headline** | Low | Low | **Zero** |
| **Optimization level** | None | One (zero-alloc) | Adaptive |
| **Expected PageRank time (10M)** | 1.5-5 sec | 0.5-2 sec | Unknown (the point) |
| **Expected RSS (10M)** | 300-800 MB | 200-600 MB | Unknown (the point) |

| | Upside | Downside | Reversibility | Regret risk |
|---|---|---|---|---|
| **A** | Simplest code, proves "just Rust" thesis | Might be slower than expected | High | "Should've optimized" |
| **B** | Measurably faster, still simple | Slightly more complex | High | "Over-optimized for v0.0.3" |
| **C** | Can't be wrong, data-driven | "We measured instead of shipped" | **Highest** | **Lowest possible** |

---

## Decision Filter

### Which path is strongest if everything goes normally?

**Timeline B: PageRank + one zero-alloc optimization.**

The zero-alloc change is ~30 LOC and eliminates a known waste
(200M allocations). It's the kind of optimization any Rust
developer would make naturally — not clever, just correct.
Combined with the existing CSR storage, this gives you the
fastest possible PageRank with minimal engineering.

But honestly, A, B, and C are ALL good. The differences are
small. The key insight that ALL timelines share:

> **The rewrite IS the optimization. CSR + mmap + Rust = you
> skip the projection step, you skip the GC, you skip the
> pointer chasing. Every algorithm you add in the future gets
> these benefits for free. PageRank is just the first proof.**

### Which path is safest if things go badly?

**Timeline C: Measure First.** If the numbers are bad, you
discover this in Day 3, not after 2 weeks of building. If the
numbers are good, you ship in the same timeframe as A or B.

### What's the fastest uncertainty reducer?

**One command:**

```
cargo run -- pagerank --snapshot /tmp/10m-snapshot
```

Build the 10M-node snapshot. Run naive PageRank. Look at the
wall time. Everything follows from that one number.

---

## The "Just Rewrite in Rust" Thesis: Final Form

The answer to "what if the answer is just rewrite in Rust?" is:

**Yes, but the rewrite that matters isn't the LANGUAGE — it's
the STORAGE FORMAT that the language enables.**

Java can't efficiently mmap a 400 MB CSR array because:
- The GC might decide to scan it (GC pressure)
- The JVM wants object headers on everything (memory overhead)
- sun.misc.Unsafe exists but is... unsafe and deprecated
- ByteBuffer is an option but has 2GB limit and poor ergonomics

Rust can mmap a 400 MB CSR array because:
- `memmap2::Mmap` is a `&[u8]` (zero overhead)
- No GC will touch it (no GC)
- `unsafe { Mmap::map(&file) }` is the ONLY unsafe line
- The borrow checker ensures the Mmap outlives all references

So "just rewrite in Rust" is really "use Rust to unlock a storage
format that Java can't ergonomically support." And then every
algorithm you write on top of that format inherits the advantage.

**v0.0.3 = prove this thesis with PageRank. v0.0.4+ = every
other algorithm gets the same free lunch.**

---

## Concrete v0.0.3 Plan (Recommended: Timeline C → B)

```
Day 1: Write page_rank.rs (~120 LOC, naive Jacobi)
Day 2: Write synthetic.rs (~100 LOC, random graph generator)
       Add CLI subcommands for generate + pagerank
Day 3: Generate 100K / 1M / 10M graphs, build snapshots, run PageRank
       MEASURE wall time + RSS for each size
Day 4: If naive > 5 sec: add zero-alloc optimization (~30 LOC)
       Re-measure
Day 5: Write benchmark report, update README
       Tag v0.0.3, push

Total new LOC: 250-400
Total time: 5 days
Dependencies: zero new crates
Format changes: zero
Headline: MEASURED, not estimated
```
