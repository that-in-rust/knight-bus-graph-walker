# Timeline Traverser: 13 Formats Meet Unknown Queries

*What happens when you pre-bake 13 algorithm-specific layouts but
don't know which query the user will run? Simulating multiple
strategies for handling format uncertainty.*

---

## Decision Frame

- **Fork in the road:** You designed 13 layout families, each shaped
  for a specific algorithm's inner loop. But a user connects over
  Bolt and types an arbitrary Cypher query. How does the system
  decide which format to read — or do you even build all 13?

- **Desired outcome:** The user runs `CALL gds.pageRank.stream()`
  and gets 10-100x performance. They also run
  `MATCH (n)-[:KNOWS]->(m) RETURN m.name` and it doesn't crash.

- **Hard constraints:**
  - Users submit arbitrary Cypher — you cannot predict the query
  - Some queries are algorithm calls (GDS), some are ad-hoc traversal
  - Disk space is finite (13 layouts × 50GB graph = 650GB is absurd)
  - Build time is finite (13 rebuilds per import is slow)
  - Single-node v1 (no distributed coordinator)
  - Knight Bus thesis: speed comes from format alignment, not from
    language speed

- **Time horizon:** Week 1 → Month 1 → Quarter 1 → Year 1

- **What would count as failure:**
  - "Format not available" errors on common queries
  - Building 13 layouts takes 2 hours and 500GB for a 50GB graph
  - Ad-hoc Cypher queries (not algorithm calls) return no results
    because no format supports them
  - User must manually pick which layout to use

---

## The Core Tension

The Atlas says: *store the graph in the shape the algorithm wants.*

But "the algorithm" implies you know which algorithm in advance.

Three possible realities:

1. **The user always knows in advance.** They call `gds.pageRank()`
   or `gds.shortestPath()`. The system picks the right format. This
   is the happy path.

2. **The user writes ad-hoc Cypher.** `MATCH (a)-[:FRIENDS]->(b)
   WHERE b.age > 30 RETURN b.name` — this isn't any GDS algorithm.
   Which format handles it?

3. **The user switches between both.** Import data, run PageRank,
   then run ad-hoc queries on the results, then run Louvain, then
   browse the graph manually. This is the REAL usage pattern.

The question isn't "which format is best." It's "what strategy for
managing formats works when queries are unpredictable?"

---

## Timeline A: "One Format to Rule Them All"

### The Bet

Extend AnchorDualCsr to handle ALL queries. Add property columns,
edge weights, and filters to the existing dual-CSR format. Never
build specialized layouts.

### Opening Move

Keep the current `forward.offsets + forward.peers + reverse.offsets
+ reverse.peers` format. Add property planes as typed column files:
`props.name.strings.bin`, `props.age.u32.bin`, etc.

### Week 1

- Add property column files alongside existing CSR arrays
- All Cypher queries resolve against the same snapshot directory
- No format selection logic needed — there's only one format
- Ad-hoc `MATCH ... WHERE ... RETURN` works because properties
  are accessible

### Month 1

- PageRank runs over the reverse CSR (inbound adjacency). Not
  optimal (no pre-computed mass array, no dangling bitset), but
  correct and still 5-20x faster than Neo4j because reads are
  contiguous
- Dijkstra runs over forward CSR + weight property column. Not
  optimal (weight not inlined in edge data), but correct and still
  3-10x faster
- Triangle counting runs over forward CSR. Not optimal (adjacency
  not pre-sorted by degree), but correct and 2-5x faster
- Ad-hoc Cypher works for any query shape

### Quarter 1

- Every algorithm works, but none hits the theoretical maximum
  performance. PageRank is 10x faster instead of 50x. Dijkstra is
  5x instead of 30x.
- Performance is "uniformly good" but never "spectacularly optimal"
  for any specific workload
- Format is getting complex: CSR + properties + edge attributes +
  labels. Starting to resemble a columnar database engine

### Year 1

- The format has grown into a general-purpose columnar graph store
- It's 5-20x faster than Neo4j for most things
- But the "100x for PageRank" headline from the original thesis
  is gone — you traded peak performance for generality
- Competitor ships a graph engine with algorithm-specific storage
  and publishes benchmarks showing 3x over your system

### Long-term Shape

A respectable, general-purpose columnar graph engine. Faster than
Neo4j. Not as fast as a specialized system for any single algorithm.
The Knight Bus thesis is diluted.

### Likelihood

**85% ships, 60% succeeds commercially.**

Very likely to ship because there's only one format to build, test,
and maintain. Moderate success because the performance story becomes
"we're faster" instead of "we're *dramatically* faster."

### Stress Points

- Month 3: Someone benchmarks your PageRank against Neo4j GDS
  (which uses its own in-memory projection). Your advantage is 5x,
  not 100x. The headline dies.
- Month 6: Adding properties, labels, and edge attributes to the
  CSR format creates 40% of the complexity you eliminated by
  avoiding Neo4j's record store. You now have a different kind of
  record store.
- Month 9: Users ask "why should I switch from Neo4j for only 5x?"

### Inflection Points

- **Month 2:** If early users only care about traversal speed (not
  algorithm benchmarks), this path wins easily. No need for
  specialized formats.
- **Month 6:** If users start requesting specific algorithm
  performance, you face the "add specialization or accept
  mediocrity" fork.

---

## Timeline B: "Build All 13, Let the Router Decide"

### The Bet

Build every layout family. When a query arrives, a router inspects
the query and dispatches to the optimal format. The user never sees
format selection — it's automatic.

### Opening Move

Implement a `FormatRouter` that maps query patterns to layout
families:
- `gds.pageRank()` → InboundPowerLayout
- `gds.shortestPath()` → RelaxationFrontierLayout
- `gds.triangleCount()` → OrderedWedgeLayout
- `MATCH (n)-[r]->(m)` → AnchorDualCsr (fallback)

### Week 1

- Build the router interface: `fn select_format(query: &CypherAst)
  -> LayoutFamily`
- Only AnchorDualCsr actually exists behind the router
- All queries fall through to AnchorDualCsr (same as Timeline A
  initially)
- Start building InboundPowerLayout as second format

### Month 1

- 2-3 layout families implemented (AnchorDualCsr, InboundPower,
  maybe RelaxationFrontier)
- Import pipeline must now build 2-3 snapshots per data load
- Build time for 50GB graph: single CSR ~2-5 minutes, three
  layouts ~6-15 minutes
- Disk usage: 50GB × 3 = ~150GB (some data shared, but offsets
  and specialized arrays duplicate)
- Router correctly dispatches PageRank to InboundPower format
- Ad-hoc Cypher falls through to AnchorDualCsr

### Quarter 1

- 5-6 layout families implemented
- Import pipeline is now a significant operation: 25-40 minutes
  for 50GB across 5-6 layouts
- Disk: 250-400GB for a 50GB logical graph
- Each new algorithm is FAST — this is where the 50-100x
  benchmarks shine
- But: you've spent ~60% of engineering time on storage formats
  and only ~40% on Cypher/Bolt
- Users report: "PageRank is blazing, but I can't run MERGE or
  WHERE clauses on properties yet"

### Year 1

- 10-13 layout families exist
- Disk usage is the elephant in the room: 500GB-1TB for a 50GB
  logical graph
- Import takes 1-2 hours for 50GB (all layouts must rebuild)
- Some layouts are rarely used (InfluenceMonteCarlo serves 1
  algorithm used by <1% of users)
- Performance stories are incredible when they hit: "100x
  PageRank, 50x Dijkstra, 30x Triangle Count"
- But the operational story is bad: "import takes an hour and
  needs a terabyte"
- Incremental updates are a nightmare: change one edge, rebuild
  13 layouts

### Long-term Shape

A research-grade system with spectacular benchmarks and terrible
operational ergonomics. Loved by PhD students. Rejected by
production teams who can't spare 1TB for a 50GB graph.

### Likelihood

**50% ships (all 13), 25% succeeds commercially.**

Hard to ship because each layout family is 3-8K LOC of unique
storage code, build pipeline, validation, and testing. 13 × 5K =
65K LOC just for storage formats, before any Cypher/Bolt work.
Low commercial success because the operational costs (disk, build
time, incremental update complexity) outweigh the per-algorithm
speedup for most users.

### Stress Points

- Month 2: Second format doubles the testing surface. Every bug
  must be verified across all built formats.
- Month 4: Disk usage complaints start. "Why does my 10GB graph
  need 80GB on disk?"
- Month 6: Incremental update design crisis. One edge insert must
  update N layouts. Do you rebuild all? Queue them? Accept staleness
  per layout?
- Month 9: Team burnout. Building storage formats is not the fun
  part. Cypher and Bolt are starved of attention.
- Month 12: Users love algorithm performance but hate the import
  time. "Neo4j imports in 3 minutes. Yours takes 45."

### Inflection Points

- **Month 1:** If you build ONLY the P0 layouts (5 families), the
  disk/time cost is manageable. This is a different timeline (see
  Timeline C).
- **Month 6:** If a competitor ships "good enough" speed with one
  format, the 13-layout advantage becomes academic.

---

## Timeline C: "One Base + On-Demand Specialization"

### The Bet

Build ONE base format (extended AnchorDualCsr with properties) that
handles ALL queries correctly. Then build specialized layouts ON
DEMAND — only when a user actually calls the algorithm, and only if
the data isn't already built.

Like a materialized view in a database: the base table always
exists, specialized views are built when needed and cached.

### Opening Move

- Ship the base format: dual CSR + property columns
- Define a `SpecializedLayout` trait:
  `fn build_from_base(base: &BaseSnapshot) -> Self`
- First specialized layout: InboundPower (for PageRank)
- Router: if specialized layout exists, use it. Otherwise, fall
  through to base.

### Week 1

- Base format handles all ad-hoc Cypher (traversal + properties)
- `gds.pageRank()` triggers: "building optimized PageRank layout,
  please wait..." → builds InboundPower from base CSR → caches it
- Second call to `gds.pageRank()` hits the cached layout instantly
- User doesn't choose formats — system builds them transparently

### Month 1

- 2-3 specialized layouts buildable on demand
- First-call latency for an algorithm: 30-120 seconds (building
  the specialized layout from the base snapshot)
- Subsequent calls: instant (cached)
- Ad-hoc Cypher always works against the base format
- Disk: base format (~60GB for 50GB graph) + cached layouts
  (only the ones actually used, ~10-30GB each)
- Typical user with 3-4 algorithms: ~120-180GB total
- User who only does traversal: ~60GB (just the base)

### Quarter 1

- 5-8 specialized layouts available
- System transparently builds + caches what users actually need
- Usage telemetry shows which layouts are popular:
  - PageRank (InboundPower): 80% of users call it
  - Dijkstra (RelaxationFrontier): 60%
  - Triangle Count (OrderedWedge): 40%
  - K-Core (PeelBucket): 15%
  - Max Flow (FlowResidual): 3%
- This means most installs only have 3-4 specialized layouts
  cached, not 13
- Disk usage is proportional to actual usage, not theoretical
  completeness

### Year 1

- 10-13 specialized layouts available
- Build-on-demand means only popular ones get exercised and
  battle-tested
- Rarely-used layouts (InfluenceMonteCarlo) have fewer users but
  also fewer bugs reported, lower maintenance cost
- Incremental updates: rebuild only the base format + invalidate
  cached specialized layouts. Rebuilds happen lazily when the
  algorithm is next called
- Performance is nearly as good as Timeline B for algorithms
  that are used (same specialized layout, just built lazily
  instead of eagerly)
- First-call latency is the tradeoff: 30-120 seconds to build a
  layout the first time after import

### Long-term Shape

A practical system: fast for everything, *spectacularly* fast for
algorithms users actually call, with operational costs proportional
to actual usage. The "materialized view" metaphor makes sense to
database people.

### Likelihood

**75% ships, 55% succeeds commercially.**

Good odds because the base format is shippable by Month 1 (same as
Timeline A), and specialized layouts are additive — you can ship
with zero specialized layouts and add them incrementally. Each
layout is an independent work item. Commercial success depends on
whether first-call latency is acceptable to users.

### Stress Points

- Month 2: First-call latency creates UX confusion. User calls
  `gds.pageRank()` and waits 60 seconds with no output. "Is it
  broken?"
  → Fix: progress bar, "Building optimized PageRank index..."
- Month 4: Cache invalidation complexity. When does a specialized
  layout become stale? After any write? After N writes? After a
  schema change?
  → Fix: version stamp on base snapshot. If base version != cached
  layout's base version, rebuild.
- Month 8: Users want to "pre-warm" all layouts after import.
  This is just Timeline B with extra steps.
  → Fix: `knrt build --precompute pagerank,dijkstra,triangles`
  command. Let users opt into eager building for specific algorithms.

### Inflection Points

- **Month 1:** If first-call latency is <10 seconds for 50GB,
  this strategy is clearly dominant. If it's >2 minutes, users
  will demand pre-building (converges toward Timeline B).
- **Month 6:** If users mostly stick to 3-4 algorithms, this
  path's disk savings over Timeline B are massive (150GB vs 650GB).
  If users run all 13 algorithms, the savings disappear.

---

## Timeline D: "Base Format Only, Algorithms Compute In-Memory"

### The Bet

Never build specialized on-disk layouts. Instead, store the graph
in one base format (CSR + properties). When an algorithm runs,
project the needed structure INTO MEMORY (like Neo4j GDS does) and
compute there.

This is what Neo4j GDS already does — but with a CSR base instead
of linked-list records. The question is: does the CSR base give
enough of an advantage that in-memory projection is still fast?

### Opening Move

- Same base format as Timeline C: dual CSR + property columns
- Algorithm execution: read CSR arrays + property columns into
  algorithm-specific in-memory structures
- No on-disk specialized layouts at all

### Week 1

- All Cypher works against CSR base (same as Timeline A)
- PageRank: read reverse CSR → build in-memory score arrays →
  iterate. Projection time: 1-5 seconds for 50GB (contiguous read
  from CSR is fast). Execution: same speed as specialized layout
  because the in-memory structure IS the same data, just built at
  runtime
- No disk overhead beyond the base format
- No cache invalidation problem

### Month 1

- 3-4 algorithms work with in-memory projection
- Projection cost per algorithm: 1-10 seconds (depends on data size
  and what needs to be derived)
- This is MUCH faster than Neo4j GDS projection because CSR base
  → algorithm structure is a sequential read, while Neo4j record
  store → algorithm structure requires linked-list pointer chasing
- Memory cost: algorithm working set is in RAM during execution
  (PageRank on 50GB graph: ~1-2GB for score arrays + some overhead)

### Quarter 1

- 8-10 algorithms available
- Performance story: "first query takes 5 seconds for projection,
  then algorithm runs at 50-100x speed"
- But: projection must happen EVERY TIME the algorithm is called
  (no persistent specialized layout)
- For iterative workflows (run PageRank → tweak parameters → run
  again), the 5-second projection happens each time
- Memory pressure: large algorithms on large graphs may need
  significant RAM during projection (2-8GB for 50GB graph)

### Year 1

- System is simple: one storage format, all algorithms compute
  in-memory
- Performance is very good (5-50x over Neo4j GDS) because CSR →
  projection is fast sequential I/O, not linked-list pointer chasing
- Not as fast as persistent specialized layouts (Timeline B/C) for
  repeated algorithm calls — you re-project each time
- Operational story is clean: import once, one set of files, any
  algorithm works
- Memory requirements are the main concern: 50GB graph + 4-8GB
  projection working set = need a 64GB+ machine

### Long-term Shape

The "pragmatic" path. Gives up 20-40% of peak algorithm speed
compared to pre-built layouts, but eliminates ALL format management
complexity. Equivalent to "Neo4j GDS but with CSR base instead of
record store" — which is already a 5-50x win.

### Likelihood

**90% ships, 50% succeeds commercially.**

Easiest to ship because there's only one format and algorithms are
pure computation. Commercial success is moderate because the story
is "faster Neo4j GDS" rather than "fundamentally different
architecture." Less defensible moat.

### Stress Points

- Month 3: Benchmarks against Timeline B/C show 2-3x slower for
  PageRank because projection adds 5 seconds that pre-built layouts
  avoid. "Why isn't it as fast as your thesis promised?"
- Month 6: Users running interactive workflows (run algorithm →
  browse results → run another) hit projection latency on every
  algorithm switch. "It's fast once it starts, but the startup
  is annoying."
- Month 9: Memory pressure on 50GB+ graphs becomes the limiting
  factor. Users with 32GB machines can't run large algorithms.

### Inflection Points

- **Month 1:** If projection is <2 seconds for 50GB, this path is
  nearly as good as pre-built layouts. If >10 seconds, the UX gap
  becomes noticeable.
- **Month 6:** If the dominant use case is "run one algorithm per
  session," projection cost is a one-time tax and barely matters.
  If users chain 5-10 algorithm calls, it accumulates painfully.

---

## Cross-Timeline Analysis

### Raw Comparison

| | A: One Format | B: All 13 Layouts | C: Base + On-Demand | D: In-Memory Projection |
|---|---|---|---|---|
| **Ad-hoc Cypher** | Works always | Fallback to base | Works always | Works always |
| **Algorithm speed** | 5-20x | 50-100x | 50-100x (after first call) | 10-50x |
| **First algorithm call** | Instant | Instant | 30-120s build | 1-10s project |
| **Repeat algorithm call** | Instant | Instant | Instant (cached) | 1-10s project again |
| **Disk (50GB graph)** | ~60GB | ~500-650GB | ~120-200GB typical | ~60GB |
| **Import time (50GB)** | 2-5 min | 1-2 hours | 2-5 min + lazy builds | 2-5 min |
| **Incremental update** | Rebuild base | Rebuild ALL layouts | Rebuild base + invalidate cache | Rebuild base |
| **LOC for storage** | ~15-25K | ~65-80K | ~25-40K | ~15-25K |
| **LOC for algorithms** | ~30-50K | ~15-25K (simpler inner loops) | ~20-35K | ~30-50K |
| **Engineering focus** | 80% Cypher/Bolt, 20% storage | 60% storage, 40% Cypher/Bolt | 60% Cypher/Bolt, 40% storage | 80% Cypher/Bolt, 20% storage |
| **Year 1 LOC total** | ~80-110K | ~120-160K | ~90-130K | ~80-110K |

### The Real Question in One Table

| | Upside | Downside | Reversibility | Regret risk | What must cooperate |
|---|---|---|---|---|---|
| **A: One Format** | Simplest. Ships fastest. Good for all queries. | "Fast" not "spectacular." Thesis diluted. | High — can add specialization later | "We settled for 5x when we could have had 100x" | Nothing — just build it |
| **B: All 13** | Spectacular benchmarks for every algorithm | Disk explosion. Import hours. Maintenance nightmare. | Low — hard to remove formats once users depend on them | "We built a research lab, not a product" | Disk, team patience, users tolerating build time |
| **C: Base + On-Demand** | Best of both: fast default + spectacular when needed | First-call latency. Cache invalidation complexity. | High — can add or remove specialized layouts freely | "First-call latency killed adoption" or "cache bugs caused stale results" | Users accepting 30-120s first-call wait |
| **D: In-Memory** | Simplest operations. One format. Clean mental model. | Re-projects every call. Memory-hungry. Not the thesis vision. | High — can add on-disk caching later (becomes C) | "We gave up the Knight Bus thesis for simplicity" | RAM on user machines |

### Inflection Points Across All Timelines

**Inflection 1: How fast is projection from CSR?**

If reading the base CSR and building an algorithm-specific structure
takes <2 seconds for 50GB:
- Timeline D becomes nearly as good as B/C for performance
- Timeline C's first-call latency is negligible
- Timeline A can silently project behind the scenes

If it takes >30 seconds:
- Timeline C's first-call latency is a real UX problem
- Timeline D's repeated projection becomes painful
- Timeline B's pre-built layouts are genuinely necessary

**Inflection 2: How many algorithms does a typical user call?**

If 1-3 algorithms per dataset:
- Timeline C wins (small disk footprint, fast for what matters)
- Timeline B wastes 70% of its disk on unused layouts

If 8-13 algorithms per dataset:
- Timeline C converges to Timeline B (everything gets built anyway)
- Timeline B's eager building is actually more predictable

**Inflection 3: How large are real user graphs?**

If graphs are <5GB (most analytics workloads):
- All timelines work fine. Disk and build time are non-issues.
- Timeline B's 13× overhead is 65GB — acceptable on any laptop.
- Timeline D's projection is <1 second. No one notices.

If graphs are 50-500GB (enterprise analytics):
- Timeline B is prohibitive (650GB-6.5TB of layouts)
- Timeline D's memory requirements become limiting (need 128GB+ RAM)
- Timeline C is the only path that scales

**Inflection 4: Does the user know which algorithm they want?**

If the user always calls named GDS algorithms:
- Format selection is trivial (function name → layout family)
- All timelines work

If the user writes ad-hoc Cypher that HAPPENS to be equivalent to
an algorithm (e.g., iterative PageRank via Cypher loops):
- No router can detect this. Falls back to base format.
- Only Timelines A and D handle this gracefully

---

## The Hidden Timeline: What Neo4j GDS Actually Does

Worth noting: Neo4j GDS already solved this problem. Their answer:

1. Store everything in the generic record store (linked lists)
2. When user calls `gds.graph.project()`, build an in-memory
   projected graph (CSR-like adjacency + property arrays)
3. Run algorithms on the projected graph
4. Drop the projection when done

This is essentially **Timeline D, but with a linked-list base
instead of CSR base.** The key difference: Neo4j's projection
from linked-list records is SLOW (pointer chasing across pages).
Knight Bus's projection from CSR base would be FAST (contiguous
reads).

So the real question is: **is Knight Bus's base CSR format so
much better than Neo4j's record store that in-memory projection
is fast enough to skip specialized on-disk layouts entirely?**

If yes: Timeline D wins (simple, fast enough, easy to maintain).
If no: Timeline C wins (need specialized layouts, but build lazily).

---

## Decision Filter

### Which path is strongest if everything goes normally?

**Timeline C: Base + On-Demand Specialization.**

It gives you:
- All queries work from day 1 (base format)
- Spectacular algorithm performance when needed (specialized layouts)
- Disk usage proportional to actual usage (not theoretical)
- Each specialized layout is an independent, additive work item
- The "materialized view" metaphor is well-understood

### Which path is safest if things go badly?

**Timeline A: One Format to Rule Them All.**

If things go badly (team is small, deadlines are tight, users only
care about traversal speed), having one format means:
- One thing to build, test, and maintain
- All engineering time goes to Cypher/Bolt (where users feel pain)
- 5-20x over Neo4j is still a compelling story
- You can ALWAYS add specialization later (A → C is a natural
  evolution)

### What experiment would reduce uncertainty fastest?

**Two measurements that collapse the decision in one day:**

**Experiment 1: Projection latency from CSR base**

```
1. Take a 50M-edge graph (50GB-ish) in base CSR format
2. Build the InboundPower layout (PageRank) from it
3. Measure: how long does the build take?

If < 2 seconds → Timeline D is viable (in-memory is fine)
If 2-30 seconds → Timeline C is the sweet spot (cache on disk)
If > 30 seconds → Timeline B is necessary (must pre-build)
```

**Experiment 2: Storage overhead per layout**

```
1. Take the same 50M-edge graph
2. Build AnchorDualCsr, InboundPower, and RelaxationFrontier
3. Measure: what's the total disk? What % is duplicated?

If layouts share >60% of bytes (CSR arrays are the same) →
  disk overhead is manageable even with many layouts
If layouts share <30% of bytes →
  disk explosion is real, must be selective
```

These two experiments, doable in a single day with the existing
codebase, would tell you whether you're in the "one format is
enough" world or the "need specialized layouts" world.

---

## The Verdict: What "Unknown Queries" Really Means

The 13 layout families are a design document, not a deployment
requirement. The question "what if we don't know the query?" has
a simpler answer than it appears:

**You always need a base format that handles any query.** This is
non-negotiable. Without it, ad-hoc Cypher breaks.

**You optionally add specialized layouts for known, named algorithm
calls.** These are performance optimizations, not correctness
requirements.

The real choice is between:
- Building specializations **eagerly** (import time, all layouts,
  Timeline B)
- Building specializations **lazily** (on first algorithm call,
  Timeline C)
- Building specializations **never** (in-memory projection,
  Timeline D)
- Building **no specializations** and accepting good-not-great
  speed (Timeline A)

The "we don't know the query" fear is overblown because:

1. **80% of ad-hoc Cypher is traversal.** The base CSR format
   handles traversal 10-100x faster than Neo4j already.

2. **Algorithm calls are named.** `gds.pageRank()` is not an
   unknown query — it's an explicit declaration of intent.

3. **The remaining 20% is property filters and aggregations.**
   These are CPU-bound operations where format doesn't matter much
   — a columnar property store (already in the base format) handles
   them well.

The 13 formats aren't for "unknown queries." They're for KNOWN
algorithms that deserve custom byte layouts. That's a much simpler
problem than format selection under true query uncertainty.

---

## Recommended Sequence

```
Month 1:    Ship base format (extended AnchorDualCsr + property columns)
            All Cypher works. 10-100x traversal. Timeline A.

Month 1-2:  Run Experiment 1 (projection latency measurement)
            Run Experiment 2 (storage overhead measurement)
            This collapses the A vs C vs D decision.

Month 2-3:  If projection < 2s → stay with Timeline A/D
            If projection 2-30s → evolve to Timeline C
            If projection > 30s → pre-build P0 layouts (Timeline B-lite)

Month 3-6:  Add specialized layouts for P0 algorithms
            (PageRank, Dijkstra, Triangle Count, SCC/WCC, BFS)
            This is 5 layouts, not 13.

Month 6-12: Add P1-P2 layouts based on actual user demand.
            Never build a layout no user has asked for.
```

The Atlas is a MENU, not a build plan. You build what users order.
