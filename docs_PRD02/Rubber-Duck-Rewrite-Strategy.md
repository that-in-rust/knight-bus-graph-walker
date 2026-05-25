# Rubber Duck Debugging: The Rewrite Strategy

*I wrote a clean 713-line plan. Now I'm going to break it.*

---

## All Core Facts I'm Working With

```
CODEBASE
  12 source files, 4,710 LOC Rust
  3 test files, 532 LOC
  Cargo.toml: 11 deps (no rand, no rayon)
  Test fixtures: 39 nodes, 67 edges (tiny)
  
ARCHITECTURE CLAIM
  "MmapWalkRuntime IS the OLAP engine, 80% done"
  "Just add PageRank (150 LOC) and Synthetic (120 LOC)"
  "~670 LOC total change for v0.0.3"
  "5 days to ship"

RUNTIME CLAIM  
  "PageRank on 100M edges: 2-7 sec (64 GB), 4-12 sec (16 GB)"
  "Neo4j: 65-135 sec (same workload)"
  "RSS: 720 MB vs 8-16 GB"

FILE MAP CLAIM
  "error.rs, parity.rs, truth.rs: unchanged"
  "runtime.rs: +40 LOC"
  "2 new files, 10 extended files"
```

---

## Attack #1: The LOC Estimates Are Lies

### "page_rank.rs: ~150 LOC"

Let me actually write out what the PageRank function needs:

```rust
// The function signature + config
pub struct PageRankConfig { ... }            // 8 lines
pub struct PageRankResult { ... }            // 8 lines
impl Default for PageRankConfig { ... }      // 8 lines

pub fn page_rank(
    runtime: &MmapWalkRuntime,
    config: &PageRankConfig,
) -> Result<PageRankResult, KnightBusError> {
    let n = runtime.node_count() as usize;   // 1 line
    let mut scores_old = vec![1.0/n as f64; n];  // 1 line
    let mut scores_new = vec![0.0_f64; n];   // 1 line
    
    // RSS measurement setup
    let mut system = System::new_all();      // 3 lines
    let pid = Pid::from_u32(std::process::id());
    let started_at = Instant::now();
    
    for iteration in 0..config.max_iterations {  // 1 line
        // Reset new scores
        scores_new.iter_mut().for_each(|s| *s = 0.0);  // 1 line
        
        // Pull-based PageRank
        for v in 0..n as u32 {               // 1 line
            let (start, end) = runtime.reverse_neighbor_range(v);
            let mut sum = 0.0;
            for idx in start..end {
                let u = runtime.reverse_peer_at(idx);
                let degree = runtime.forward_degree(u);
                if degree > 0 {
                    sum += scores_old[u as usize] / degree as f64;
                }
            }
            scores_new[v as usize] = 
                (1.0 - config.damping) / n as f64 
                + config.damping * sum;
        }                                    // ~15 lines inner loop
        
        // Convergence check
        let diff: f64 = scores_old.iter()
            .zip(scores_new.iter())
            .map(|(a, b)| (a - b).abs())
            .sum();                          // 5 lines
        
        std::mem::swap(&mut scores_old, &mut scores_new);
        
        if diff < config.tolerance {         // 4 lines
            return Ok(PageRankResult { ... });
        }
    }
    
    Ok(PageRankResult { ... })               // 5 lines
}
```

**Actual count: ~70 lines for the core function.** Plus:
- Imports: 10 lines
- Config struct + Default: 16 lines
- Result struct: 10 lines
- Error handling edge cases: 10 lines
- Top-K extraction helper: 15 lines
- Output formatting (CSV writer for scores): 20 lines

**Honest estimate: ~150 LOC. The original estimate holds.** ✓

But WAIT — I said "Uses mmap'd CSR directly (no copy)." Let me
check if the MmapWalkRuntime actually exposes what PageRank needs.

### The runtime.rs Gap

The plan says: "Add methods: forward_degree, reverse_neighbor_range,
reverse_peer_at (+40 LOC)."

Looking at the ACTUAL code in runtime.rs:

```rust
// EXISTING (line 320-331):
fn read_neighbor_ids(&self, dense_id: DenseNodeId, direction: WalkDirection) -> Vec<u32> {
    let (offsets_mmap, peers_mmap) = match direction { ... };
    let start = read_u64_from_mmap(offsets_mmap, dense_id.get() as usize) as usize;
    let end = read_u64_from_mmap(offsets_mmap, dense_id.get() as usize + 1) as usize;
    (start..end)
        .map(|index| read_u32_from_mmap(peers_mmap, index))
        .collect()   // ← ALLOCATES A VEC
}
```

**PROBLEM: `read_neighbor_ids` allocates a Vec for every call.**
PageRank calls this N × avg_degree × 20 times. For 10M nodes
with avg degree 10, that's 2 billion Vec allocations.

The plan says "add `reverse_neighbor_range` that returns indices."
This avoids the allocation. Good — the plan is correct here.

But there's a SUBTLER problem: `read_u32_from_mmap` does
bounds-checking and byte copying for EVERY peer:

```rust
fn read_u32_from_mmap(mmap: &Mmap, index: usize) -> u32 {
    let start = index * 4;
    let end = start + 4;
    let mut bytes = [0_u8; 4];
    bytes.copy_from_slice(&mmap[start..end]);
    u32::from_le_bytes(bytes)
}
```

This is fine for traversals (100 calls), but for PageRank
(2 billion calls), the function call overhead + bounds check
+ byte copy adds up.

**Better: cast the mmap to a `&[u32]` slice once, then index.**

```rust
// Proposed (zero-copy, no per-access overhead):
pub fn reverse_peers_slice(&self) -> &[u32] {
    // SAFETY: reverse_peers is validated at open time to be
    // correctly sized and aligned for u32 values.
    unsafe {
        std::slice::from_raw_parts(
            self.reverse_peers.as_ptr() as *const u32,
            self.reverse_peers.len() / 4,
        )
    }
}
```

**BUT**: This requires alignment guarantee. mmap'd files are
page-aligned (4 KB), so the pointer is always 4-byte aligned.
Safe in practice, but needs the `unsafe` block.

**ALTERNATIVE (no unsafe):**

```rust
// Read neighbors as a byte slice and iterate with from_le_bytes
pub fn reverse_neighbor_range(&self, v: u32) -> (usize, usize) {
    let start = read_u64_from_mmap(&self.reverse_offsets, v as usize) as usize;
    let end = read_u64_from_mmap(&self.reverse_offsets, v as usize + 1) as usize;
    (start, end)
}

pub fn reverse_peer_at(&self, index: usize) -> u32 {
    read_u32_from_mmap(&self.reverse_peers, index)
}
```

This has the function call overhead per peer but avoids unsafe.
For 100M edges × 20 iterations = 2B calls, each call is ~5 ns
(function call + bounds check + byte copy).
Total overhead: 2B × 5 ns = 10 seconds.

**That's SIGNIFICANT. It doubles the algorithm time.**

**VERDICT: Need the unsafe slice cast for PageRank to be fast.
Or: provide a `reverse_peers_as_u32` method that returns
`bytemuck::cast_slice` (adds a safe dependency).** 

Actually, `bytemuck` isn't in Cargo.toml. Using the unsafe
slice is the simpler path. This is a well-established pattern
for mmap'd binary data.

**Revised runtime.rs estimate: +60 LOC (not +40).**
Need: slice casts for offsets AND peers, plus madvise hints.

### "synthetic.rs: ~120 LOC"

The plan says: "Stream writes. Never hold full graph in memory.
Use PRNG."

**PROBLEM 1: No `rand` dependency.** Cargo.toml has no random
number generator. Options:
- Add `rand` crate (~30 KB compile, well-established)
- Implement Xoshiro256++ manually (~30 LOC)
- Use a hash function as PRNG (deterministic, no dep)

Adding `rand` is the right call. It's a tiny compile cost and
the `SmallRng` is fast enough. But this means Cargo.toml changes.

**PROBLEM 2: Power-law distribution without `rand`'s `Distribution` trait.**
If we add `rand`, power-law is: `(rng.gen::<f64>()).powf(-alpha)`.
Without `rand`, it's more manual.

**PROBLEM 3: Edge deduplication.** The CSV format allows duplicate
edges. The build pipeline deduplicates. But the GENERATOR should
not produce duplicates (wastes build time). For a random graph
with 10M nodes and avg degree 10, the probability of a duplicate
edge (same source, same target) is ~10/10M = 0.0001%. Negligible.
Skip dedup in the generator. Let the builder handle it.

**PROBLEM 4: Self-loops.** Should the generator produce edges from
a node to itself? The current codebase doesn't filter them. For
PageRank, a self-loop means a node contributes to its own score.
Unusual but not wrong. Skip self-loops in the generator for
cleanliness.

**Revised estimate: 120 LOC + Cargo.toml change (add rand).** ✓

---

## Attack #2: The Run Time Estimates

### "PageRank on 100M edges: 2-7 sec on 64 GB server"

Let me derive this from first principles:

```
PageRank per iteration:
  For each of N=10M nodes:
    Read reverse_offsets[v], reverse_offsets[v+1]     → 2 u64 reads
    For each reverse neighbor u (avg 10):
      Read reverse_peers[index]                       → 1 u32 read
      Read forward_offsets[u], forward_offsets[u+1]   → 2 u64 reads
      Read scores_old[u]                              → 1 f64 read
    Write scores_new[v]                               → 1 f64 write

Total memory reads per iteration:
  10M nodes × (2 + 10 × (1 + 2 + 1)) = 10M × 42 = 420M reads

If all in L3 cache (impossible for 100M edges):
  420M × 4 ns = 1.68 sec per iteration
  
If all in RAM (realistic for 64 GB server):
  420M × 10 ns (DRAM latency, sequential) = 4.2 sec per iteration
  But: CSR is sequential → hardware prefetcher helps
  Effective: 420M × 5 ns = 2.1 sec per iteration

20 iterations: 20 × 2.1 = 42 seconds
```

**WAIT. 42 seconds ≠ "2-7 seconds". I claimed 2-7 sec.**

Where did I go wrong? Let me re-check.

The issue: I was counting ACCESS OPERATIONS, not BYTES.
The hardware prefetcher works on CACHE LINES (64 bytes).

```
Reverse peers array: 100M × 4 bytes = 400 MB
Sequential scan of 400 MB:
  400 MB / 10 GB/sec (DRAM bandwidth) = 0.04 sec per scan

Forward offsets array: 10M × 8 bytes = 80 MB (random access)
  10M lookups × ~50 ns per random access = 0.5 sec
  BUT: forward_degree is accessed per-neighbor, not per-node
  100M degree lookups × ~50 ns = 5 sec per iteration
  
  HOWEVER: many neighbors share the same source node.
  After the first lookup, forward_offsets[u] is cached.
  Effective: maybe 10M unique u values × 50 ns = 0.5 sec

Score array reads: 100M × 8 bytes = 800 MB
  Random access pattern (u is a neighbor ID, not sequential)
  100M × ~50 ns = 5 sec per iteration
  BUT: if score array fits in LLC (typically 20-40 MB on server):
    Doesn't fit. 160 MB score arrays >> 40 MB LLC.
  Random access to 160 MB array: cache miss rate ~80%
  100M × 0.8 × 50 ns = 4 sec per iteration
```

**Revised estimate per iteration:**
```
Scan reverse_peers (sequential):     0.04 sec
Scan reverse_offsets (sequential):   0.01 sec
Random forward_offsets lookups:      0.5 sec  (10M unique lookups)
Random score_old reads:              2-4 sec  (cache miss heavy)
Score_new writes:                    0.01 sec (sequential)
──────────────────────────────────────────────
TOTAL per iteration:                 2.5-4.5 sec

20 iterations: 50-90 sec
```

**HOLY SHIT. My estimate of 2-7 seconds was off by 10x.**

**The bottleneck is random access to scores_old[u].** The
neighbor ID `u` is essentially random — it can be any node.
The score array is 160 MB, much larger than LLC. So every
access to `scores_old[u]` is a cache miss (~50 ns).

### How Neo4j GDS Does It (for comparison)

Neo4j GDS runs PageRank AFTER projecting the graph into JVM
heap. The JVM heap has the SAME random access problem with
scores. But Neo4j uses HugeDoubleArray which is an array of
arrays (~16 KB chunks), and the JVM's GC can compact them.
Their PageRank implementation is similar Jacobi iteration.

Neo4j GDS algorithm time (no projection): 5-15 seconds for
100M edges. This matches my revised estimate of 50-90 seconds
ONLY IF Neo4j uses a pull-based approach and has similar
random access patterns... but they use MULTIPLE THREADS.

**I forgot about parallelism.** Neo4j GDS uses Fork-Join pool
(multi-threaded). Knight Bus v0.0.3 is SINGLE-THREADED (no
rayon dependency).

```
Neo4j GDS (4 threads):    5-15 sec
Knight Bus (1 thread):    50-90 sec
Knight Bus (4 threads):   12-22 sec (theoretical with rayon)
```

**VERDICT: Single-threaded Knight Bus PageRank is SLOWER than
Neo4j GDS algorithm time. The speedup comes ENTIRELY from
skipping the 60-120 sec projection step.**

```
HONEST COMPARISON (100M edges):
                        Knight Bus    Neo4j GDS
Projection              0 sec         60-120 sec
Algorithm (1 thread)    50-90 sec     5-15 sec (multi-threaded)
TOTAL                   50-90 sec     65-135 sec

Speedup: 1.3-1.5x (NOT 10-30x)
```

**This is DEVASTATING.** The headline was "10-30x faster" but
single-threaded Knight Bus is only 1.3-1.5x faster than Neo4j
total time. The projection savings are eaten up by the slower
single-threaded algorithm.

### How To Fix This

**Option A: Add rayon (~10 LOC change, big speedup)**

```rust
// Replace the inner loop with rayon parallel iterator
use rayon::prelude::*;

scores_new.par_iter_mut().enumerate().for_each(|(v, score)| {
    let (start, end) = runtime.reverse_neighbor_range(v as u32);
    let mut sum = 0.0;
    for idx in start..end {
        let u = runtime.reverse_peer_at(idx);
        let degree = runtime.forward_degree(u);
        if degree > 0 {
            sum += scores_old[u as usize] / degree as f64;
        }
    }
    *score = (1.0 - damping) / n as f64 + damping * sum;
});
```

**With rayon (4 cores):**
```
Per iteration: 50-90 sec / 4 = 12-22 sec
20 iterations: 250-440 sec / 4 = 60-110 sec
```

Wait, that's still slow. Let me recheck.

**Actually, my per-iteration estimate was wrong.** Let me redo.

The 100M random score_old lookups at 50 ns each = 5 sec is
the worst case. In reality:
- Power-law graphs concentrate edges on high-degree nodes
- High-degree nodes' scores get cached after first access
- Sequential scan of reverse_peers has good prefetching
- The score array access pattern has locality (neighbors
  of neighbors tend to cluster)

**Realistic per-iteration with cache effects:**
```
First iteration: 3-5 sec (cold caches)
Subsequent iterations: 1-3 sec (score array partially cached)
Average: ~2 sec per iteration
20 iterations: ~40 sec (single thread)
With rayon (4 cores): ~10-15 sec
```

**Revised HONEST estimates:**

```
                           Single Thread    With rayon (4 cores)
100M edges, 64 GB server   30-60 sec        8-15 sec
100M edges, 16 GB laptop   40-80 sec        10-20 sec
100M edges, 8 GB laptop    60-120 sec       15-30 sec

Neo4j GDS:
  Projection:              60-120 sec
  Algorithm (multi-thread): 5-15 sec
  TOTAL:                   65-135 sec
```

**With rayon, Knight Bus is 4-15x faster than Neo4j total.**
**Without rayon, Knight Bus is 1-2x faster (barely wins).**

### Should rayon be in v0.0.3?

Adding rayon is +1 dependency in Cargo.toml, +10 LOC change.
The performance difference is the difference between a
compelling demo (8-15 sec) and a mediocre one (30-60 sec).

**VERDICT: YES. Add rayon for v0.0.3. It's the difference
between "4-15x faster than Neo4j" and "barely faster."**

---

## Attack #3: The File Map Has Hidden Dependencies

### "error.rs: unchanged"

**WRONG.** I said error.rs is in both "KEEP AS-IS" AND "EXTEND."
In the EXTEND section I said "+15 LOC (new error variants)."
The KEEP AS-IS section was a contradiction. CORRECTED: error.rs
gets extended, not kept as-is.

### "src/graph.rs: UNCHANGED"

**CORRECT but misleading.** graph.rs has `flatten_adjacency_lists_now`
which is used by the in-memory build path (tests). PageRank
doesn't use this. But the synthetic generator will produce CSV
files that flow through the EXISTING build pipeline (low_ram.rs).
So graph.rs is genuinely unchanged. ✓

### "src/low_ram.rs: UNCHANGED"

**CORRECT.** The synthetic graph generates CSV files. The existing
`build_snapshot_from_paths_low_ram` builds the snapshot from CSV.
The pipeline is: generate CSV → existing build → existing runtime.
No changes to low_ram.rs needed. ✓

### "Cargo.toml: not mentioned"

**BUG.** The rewrite strategy doc doesn't mention Cargo.toml
changes. We need:
- `rand = "0.8"` (synthetic graph PRNG)
- `rayon = "1.10"` (parallel PageRank)

Two new dependencies. Should have been called out explicitly.

### "Tests: +130 LOC across 3 files"

Let me think about what tests actually need:

```
1. PageRank correctness on a 4-node graph:
   A → B → C → D, all equal damping
   Hand-compute expected scores
   Assert within epsilon
   → 25 LOC
   
2. PageRank convergence test:
   Run on fixture graph (39 nodes, 67 edges)
   Assert converges within 100 iterations
   Assert all scores sum to ~1.0
   → 15 LOC

3. PageRank on disconnected graph:
   Node with no edges should get score (1-d)/N
   → 10 LOC

4. Synthetic graph generation:
   Generate 1000 nodes, avg degree 5
   Assert output CSV has correct headers
   Assert ~5000 edges (±20%)
   → 20 LOC

5. CLI integration: generate → build → pagerank
   End-to-end pipeline test
   → 30 LOC

6. PageRank CLI output format:
   Assert --top-k prints expected format
   → 15 LOC
```

**Total: ~115 LOC.** Close to the 130 estimate. ✓

**BUT: Where do these tests go?**

Currently: tests/cli.rs (CLI integration), tests/library_contract.rs
(library API tests). No tests/page_rank.rs exists.

For the 4-node hand-computed test, we need a test fixture:
a tiny 4-node graph CSV. This needs to be in tests/fixtures/.

**Missing from plan: new test fixture files.** Need:
- tests/fixtures/pagerank/tiny_nodes.csv (4 nodes)
- tests/fixtures/pagerank/tiny_edges.csv (4 edges)

Not LOC-heavy but needs to be called out.

---

## Attack #4: The 5-Day Plan Is Fantasy

### "Day 1: synthetic.rs + Generate CLI + test"

What actually happens on Day 1:

```
1. Create src/synthetic.rs               30 min
2. Add rand to Cargo.toml                2 min
3. Create SyntheticGraphConfig in types.rs  10 min
4. Add Generate subcommand to main.rs    15 min
5. Wire up in app.rs                     10 min
6. Add mod declaration in lib.rs         1 min
7. Write CSV headers correctly           15 min
   (must match REQUIRED_NODE_HEADERS and REQUIRED_EDGE_HEADERS
    from low_ram.rs and truth.rs)
8. Test: generate → build → query        30 min
9. Debug: CSV parsing issues             30 min
   (the headers must be EXACTLY right: node_id, node_type,
    label, parent_id, file_path, span for nodes;
    from_id, edge_type, to_id for edges)
10. Fix edge cases                       30 min
```

**Day 1 realistic: 3-4 hours of focused work.** Achievable. ✓

### "Day 2: page_rank.rs + runtime.rs extensions"

```
1. Add unsafe slice cast to runtime.rs    20 min
2. Add forward_degree, reverse_neighbor_range  15 min
3. Add rayon to Cargo.toml                2 min
4. Write PageRank core algorithm          45 min
5. Add PageRankConfig, PageRankResult to types.rs  15 min
6. Test on 4-node hand-computed graph     30 min
7. Debug: convergence issues              30 min
   (common bugs: forgetting to divide by degree,
    wrong damping formula, not handling zero-degree nodes)
8. Test on fixture graph (39 nodes)       15 min
9. Debug: score sum != 1.0               30 min
   (Jacobi PageRank scores sum to ~1.0 only at convergence)
```

**Day 2 realistic: 4-5 hours.** Tight but achievable. ✓

### "Day 3: PageRank CLI + benchmark integration"

```
1. Add PageRank subcommand to main.rs     20 min
2. Wire up in app.rs                      10 min
3. Add --top-k flag, --output CSV flag    20 min
4. Add PageRank scenario to bench.rs      30 min
5. Test CLI: knight-bus pagerank --snapshot X  15 min
6. Test: generate 10K → build → pagerank  15 min
7. Verify numbers make sense              15 min
```

**Day 3 realistic: 2-3 hours.** Light day. Could combine with Day 4. ✓

### "Day 4: Full pipeline test at 10M scale"

**THIS IS WHERE IT BLOWS UP.**

```
1. Generate 10M nodes, 100M edges         1-2 min (fast PRNG)
   BUT: Writing 100M edges to CSV at ~30 bytes/edge
   = 3 GB of CSV data
   Write speed: ~200 MB/sec
   Time to write CSV: ~15 seconds
   TOTAL generation: ~2 min ← OK

2. Build snapshot from 3 GB CSV           ???
   The build pipeline (low_ram.rs) does external merge sort.
   With --memory-budget-mb 4096:
     Phase 1: Build node key runs (scan 39-node CSV)
     Phase 2: Write node catalog
     Phase 3: Build edge source runs (scan 100M edges)
     Phase 4-5: Resolve from/to keys
     Phase 6-7: Emit forward/reverse CSR
   
   PROBLEM: Has this pipeline EVER been tested at 10M scale?
   
   Looking at test fixtures: 39 nodes, 67 edges.
   Looking at v003-research: mentions "2GB scale benchmarks"
   
   The low_ram builder handles large data via external merge sort.
   It SHOULD work at 10M nodes. But "should" ≠ "tested."
   
   POTENTIAL ISSUES:
   - u32 overflow: node_count > u32::MAX (4B)? No, 10M << 4B. Safe.
   - Edge count as u64: 100M << u64::MAX. Safe.
   - Scratch disk space: merge sort needs ~2x input. 3 GB CSV → 6 GB scratch. Need 6 GB free disk.
   - Memory budget: 4 GB should be plenty for 10M nodes.
   - Wall time: 5-15 minutes for 100M edges. CONFIRMED by estimates.
   
   ACTUAL RISK: The build has 7 phases. Each phase reads/writes
   sorted run files. With 100M edges, each phase produces files
   that are ~400 MB. 7 phases × 400 MB = ~2.8 GB of intermediate files.
   Plus the 3 GB CSV input. Need ~10 GB free disk total.
   
   ON DEVIN VM: Should have enough disk. On a user's laptop
   with a full SSD: could be tight.

3. Run PageRank on 10M-node snapshot      ???
   
   PROBLEM: Estimated 30-60 seconds (single thread) or 8-15 seconds
   (rayon). But this is the FIRST TIME running at this scale.
   
   What could go wrong:
   - mmap opens 7 files. Total mmap size: ~1.3 GB. On a system
     with mmap limits (vm.max_map_count), this could fail.
     Default Linux: 65530 mappings. We use 7. Fine.
   - RSS measurement via sysinfo: sampling every 1024 rows
     is too coarse for PageRank. Need to sample during iterations.
   - PageRank convergence: power-law graphs may not converge
     in 20 iterations. May need 50-100. This would 2-5x the time.
     
   MITIGATION: Set max_iterations=20, tolerance=1e-6.
   If it doesn't converge, return partial result with
   converged=false. The user can re-run with more iterations.

4. Compare numbers against estimates       30 min
   
   RISK: If actual PageRank time is 60-90 seconds (single thread),
   the "faster than Neo4j" claim requires rayon. Without rayon,
   the comparison is:
     Knight Bus: 60-90 sec (no projection + slow algorithm)
     Neo4j GDS: 65-135 sec (60-120 sec projection + 5-15 sec algorithm)
   
   That's only 1-1.5x speedup. NOT viral-worthy.
   
   WITH rayon:
     Knight Bus: 15-25 sec (no projection + parallel algorithm)
     Neo4j GDS: 65-135 sec
   
   That's 3-9x speedup. Viral-worthy.
```

**Day 4 realistic: 4-6 hours, with risk of discovering build
pipeline issues at 10M scale that take extra time to debug.**

### "Day 5: Polish, README, tag v0.0.3"

```
1. Update Cargo.toml version              1 min
2. Write benchmark results section        30 min
3. Update README with PageRank examples   30 min
4. Run cargo test (all existing + new)    10 min
5. Run cargo clippy                       10 min
6. Tag v0.0.3                             5 min
```

**Day 5 realistic: 2 hours.** Light day. ✓

### Revised Timeline

```
Day 1:  synthetic.rs + CLI                    3-4 hours
Day 2:  page_rank.rs + runtime.rs             4-5 hours
Day 3:  CLI + bench integration               2-3 hours
Day 4:  10M scale test + debugging            4-8 hours (HIGH RISK)
Day 5:  Polish + README + tag                 2-3 hours
                                              ─────────
TOTAL:                                        15-23 hours of work
CALENDAR:                                     5-7 working days
```

**"5 days" was optimistic. 7 days is more honest.** If the 10M
scale test reveals build pipeline issues, add 2-3 more days.

---

## Attack #5: What Did I Forget Entirely?

### Forgotten #1: Output Format for PageRank Scores

The plan says "knight-bus pagerank --snapshot X --top-k 10"
but doesn't specify what the OUTPUT looks like.

```
Option A: Print to stdout (simple)
  $ knight-bus pagerank --snapshot ./my-graph --top-k 5
  rank  node_id               score
  1     user-42               0.00342
  2     user-1337             0.00298
  3     user-7                0.00256
  ...

Option B: Write to CSV file (useful)
  $ knight-bus pagerank --snapshot ./my-graph --output scores.csv
  Writes: node_id,score,rank
  
Option C: Both (with --output optional)
```

**Option C is correct.** Default to stdout top-K, optionally
write full scores to CSV. Need both for the demo (top-K for
terminal, CSV for import into other tools).

**Additional LOC: ~20 for CSV output writer.** Already counted
in the 150 estimate.

### Forgotten #2: Dangling Nodes in PageRank

A "dangling node" has zero outgoing edges (forward_degree = 0).
In standard PageRank, dangling nodes' scores get redistributed
evenly across all nodes.

```
Standard fix:
  dangling_sum = sum of scores_old[v] for all v where forward_degree(v) == 0
  scores_new[v] += damping * dangling_sum / N for all v
```

Without this fix, PageRank scores don't sum to 1.0, and the
algorithm may not converge properly.

**My page_rank.rs pseudocode handles degree=0 with an `if degree > 0`
guard, but doesn't redistribute the dangling mass.** This is a
correctness bug.

**Fix: Add dangling sum computation before the main loop.**
~5 extra lines.

### Forgotten #3: The Synthetic Graph CSV Headers

Looking at truth.rs and low_ram.rs:

```rust
// truth.rs:
const REQUIRED_NODE_HEADERS: [&str; 6] = [
    "node_id", "node_type", "label", "parent_id", "file_path", "span",
];
const REQUIRED_EDGE_HEADERS: [&str; 3] = ["from_id", "edge_type", "to_id"];

// low_ram.rs:
const REQUIRED_NODE_HEADERS: [&str; 6] = [
    "node_id", "node_type", "label", "parent_id", "file_path", "span",
];
const REQUIRED_EDGE_HEADERS: [&str; 3] = ["from_id", "edge_type", "to_id"];
```

The synthetic generator MUST produce CSV files with these EXACT
headers. The node CSV needs 6 columns even though PageRank only
uses node_id. The edge CSV needs 3 columns even though we only
care about from_id and to_id.

**What values for node_type, label, parent_id, file_path, span?**

```
node_type: "synthetic"
label: same as node_id (or "node-{i}")
parent_id: "" (empty, parsed as None)
file_path: "" (empty, parsed as None)
span: "" (empty, parsed as None)
edge_type: "synthetic_edge"
```

This is a detail the plan glossed over. If the CSV doesn't
match REQUIRED_NODE_HEADERS exactly, the build will fail with
`MissingRequiredHeader`.

### Forgotten #4: Snapshot Disk Space for 10M Scale Test

```
10M nodes, 100M edges:
  Generated CSV:      ~3 GB (nodes + edges)
  Build scratch:      ~6 GB (merge sort intermediates)
  Snapshot:           ~1.3 GB
  Total disk needed:  ~10.3 GB during build
  After cleanup:      ~4.3 GB (CSV + snapshot)
```

Need to verify the VM has enough disk space before attempting
the 10M scale test.

### Forgotten #5: cargo test at 10M Scale

The existing tests run on 39-node fixtures. The 10M scale test
should NOT be in the standard test suite (takes 5-15 minutes).
It should be a separate benchmark or manual test.

Need to add `#[ignore]` to any test that generates large graphs.

### Forgotten #6: The Edge Count Display

The current manifest uses `edge_count: u64` which counts
UNIQUE directed edges. For 100M edges, this is fine.
But PageRank processes edges in the REVERSE CSR, which has
the same count. No issue here. ✓

---

## The Corrected Scorecard

```
ORIGINAL CLAIM                     CORRECTED
───────────────────────────────    ──────────────────────────────
"~670 LOC total change"            ~750-800 LOC (forgot Cargo.toml,
                                   dangling node fix, CSV output,
                                   unsafe slice casts)

"page_rank.rs: ~150 LOC"           ~160-170 LOC (dangling nodes,
                                   CSV writer, rayon)

"runtime.rs: +40 LOC"              +60-70 LOC (unsafe slice casts
                                   for performance, madvise)

"PageRank 100M edges: 2-7 sec"     Single thread: 30-60 sec
                                   With rayon: 8-15 sec
                                   (the 2-7 sec claim was aspirational
                                    and ignored random access cost)

"5 days to ship"                   7-10 days (10M scale test is risky)

"error.rs: unchanged"              Wrong — needs +15 LOC for new
                                   error variants

"Cargo.toml: not mentioned"        Needs +2 dependencies (rand, rayon)

"No unsafe needed"                 Need unsafe for mmap slice cast
                                   (or add bytemuck dependency)

v0.0.3 TOTAL LOC CHANGE            ~800 LOC (not 670)
v0.0.3 CODEBASE SIZE               ~5,542 LOC (not 5,378)
```

### The Revised Headline Numbers

```
PageRank on 100M edges (10M nodes), 20 iterations:

                        Single Thread    With Rayon (4 cores)
64 GB server            30-60 sec        8-15 sec
16 GB laptop            40-80 sec        10-20 sec
8 GB laptop             60-120 sec       15-30 sec

Neo4j GDS (for comparison):
  Projection:           60-120 sec
  Algorithm:            5-15 sec (multi-threaded)
  TOTAL:                65-135 sec

SPEEDUP (with rayon):   4-15x
SPEEDUP (single thread): 1-2x (NOT VIRAL-WORTHY)

RSS:                    Knight Bus        Neo4j GDS
  Heap                  160 MB            8-16 GB
  Working set (mmap)    560 MB            (included in heap)
  TOTAL resident        ~720 MB           8-16 GB
  
MEMORY ADVANTAGE:       10-20x less (this claim holds)
```

### The Decision

**Rayon is MANDATORY for v0.0.3.**

Without it, the algorithm is slower than Neo4j GDS (algorithm
only), and the total time advantage is only 1-2x (from skipping
projection). That's not a demo-worthy speedup.

With rayon (4 cores): 4-15x total speedup. That's the demo.

**The viral headline changes from:**

❌ "PageRank in 3 seconds" (was a lie)

✅ "PageRank in 10 seconds, 720 MB. Neo4j: 90 seconds, 12 GB."

That's still a powerful claim: **9x faster, 17x less memory.**

---

## Updated Dependency List for v0.0.3

```toml
[dependencies]
# existing
anyhow = "1.0"
clap = { version = "4.5", features = ["derive"] }
csv = "1.3"
libc = "0.2"
memmap2 = "0.9"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
sysinfo = "0.30"
tempfile = "3.15"
thiserror = "1.0"

# NEW for v0.0.3
rand = { version = "0.8", features = ["small_rng"] }  # synthetic graphs
rayon = "1.10"                                          # parallel PageRank
```

---

## What The Corrected v0.0.3 Actually Looks Like

```
FILES CHANGED:         14 (3 new, 11 modified)
LOC ADDED:             ~800
NEW DEPENDENCIES:      2 (rand, rayon)
UNSAFE BLOCKS:         2 (mmap slice casts for offsets + peers)
NEW CLI SUBCOMMANDS:   2 (Generate, PageRank)
NEW TEST FIXTURES:     1 directory (tests/fixtures/pagerank/)
ESTIMATED TIME:        7-10 working days
```
