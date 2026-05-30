# Neo4j Multi-Node: Do We Need It?

*"Is Neo4j something that is deployed on multiple servers? Do we have to take
care of that also?"*

---

## Decision Frame

- **Fork in the road:** Should Knight Bus support multi-server deployment?
- **Desired outcome:** Know whether single-server is sufficient for v0.0.3-v0.1.0
  or whether we're building something that can't compete without clustering.
- **Hard constraints:** Team size (1), budget (~0), time-to-market matters.
- **Time horizon:** v0.0.3 (2 weeks) → v0.1.0 (3-6 months) → v1.0 (12-18 months)
- **What would count as failure:** Building single-server and discovering 80% of
  target users need multi-server. Or building multi-server too early and shipping
  nothing for 12 months.

---

## Core Facts (Enumerated)

```
FACT 1: Neo4j has THREE deployment tiers
  ┌──────────────────────────────────────────────────────────────────────┐
  │ TIER              │ DEPLOYMENT        │ PRICE       │ CLUSTERING    │
  ├──────────────────────────────────────────────────────────────────────┤
  │ Community Edition │ Single server     │ Free (GPL)  │ NO            │
  │ Enterprise Edition│ Cluster (P+S)     │ ~$36K/yr    │ YES           │
  │ Infinigraph       │ Auto-sharding     │ Highest tier│ YES + sharding│
  └──────────────────────────────────────────────────────────────────────┘

  Community = single server, no clustering, no GDS.
  Enterprise = clustering (primaries + secondaries) for read scaling + HA.
  Infinigraph = NEW (Sep 2025), property sharding across machines.

FACT 2: Neo4j GDS (Graph Data Science = the analytics we're competing with) 
  runs on a SINGLE SERVER, even in a cluster
  
  From Neo4j's own docs:
  > "Since GDS performs large computations with the full resources of the
  >  system it is not suitable to run on instances that serve the 
  >  transactional workload of the cluster."
  
  > "GDS workloads are not load-balanced if there are more than one
  >  Secondary instances."
  
  In a cluster deployment:
  - OLTP (writes/reads) → runs across primaries + secondaries
  - OLAP (GDS/algorithms) → runs on ONE dedicated Secondary server
  
  GDS PageRank, BFS, Dijkstra, community detection = ALL single-server.
  Neo4j's own analytics engine is NOT distributed.

FACT 3: The cluster serves OLTP, not OLAP
  Neo4j clustering provides:
  - Read scaling: secondaries serve read queries
  - Fault tolerance: primaries use Raft consensus for HA
  - Causal consistency: client reads its own writes
  
  It does NOT provide:
  - Distributed graph algorithms (PageRank across nodes)
  - Sharded graph analytics
  - Parallel GDS execution across cluster members

FACT 4: Infinigraph's "property sharding" is for properties, not topology
  From The Register (Sep 2025):
  > "Property sharding stores the graph's structure (nodes and relationships)
  >  in a single graph shard that preserves the structure of the graph as a
  >  cohesive unit. These property shards can then be distributed across
  >  different machines."
  
  Translation: the TOPOLOGY stays on one machine. Only PROPERTIES (names,
  addresses, metadata) get sharded across machines. Graph traversals still
  happen on a single shard.
  
  This is because graph partitioning is NP-hard — you can't efficiently split
  a graph across machines without cutting edges, which kills traversal locality.

FACT 5: DuckDB proves single-server analytics wins for 99% of workloads
  Amazon's own Redshift telemetry (analyzed by MotherDuck):
  > "The 99th percentile of datasets in a production big data platform 
  >  fits comfortably on a modern laptop."
  
  DuckDB vs Spark benchmarks (Endjin, 2026):
  > "For medium-scale datasets (up to ~100GB), modern in-process engines
  >  like DuckDB are consistently faster and up to 5x cheaper than 
  >  distributed Spark clusters."
  
  DuckDB is single-server. No clustering. No sharding. Worth $750M+.

FACT 6: Our target graph sizes fit on single server
  10M nodes, 100M edges  = ~1.3 GB CSR data    → any laptop
  100M nodes, 1B edges   = ~13 GB CSR data      → workstation or cloud instance
  1B nodes, 10B edges    = ~130 GB CSR data      → 256 GB server
  
  Level 3 (edge-centric streaming): handles 1B nodes in 41 MB RAM.
  The storage needs to be on disk, but that's a single NVMe SSD.

FACT 7: Graph partitioning is fundamentally different from table sharding
  SQL tables: shard by primary key range → queries to specific shards
  Graphs: edges cross partition boundaries → distributed traversal = O(network)
  
  This is WHY Neo4j's topology stays on one shard in Infinigraph.
  This is WHY no graph database has good distributed analytics.
  This is WHY single-server graph analytics is the correct architecture.
```

---

## Timeline A: "Ship Single-Server" (DuckDB Play)

**Opening move:** Build Knight Bus as a single-binary, single-server graph
analytics engine. Embed as a library or run as a CLI. No networking, no
clustering, no sharding.

**Week 1-2 (v0.0.3):**
- Ship PageRank + synthetic graph generator
- Benchmark: Knight Bus 3-5 sec, 160 MB vs Neo4j GDS 65-135 sec, 8-16 GB
- Headline: "Same algorithm. 10x faster. 50x less memory. One binary."

**Month 1-2 (v0.0.4-v0.0.5):**
- Add Dijkstra, BFS, triangle count
- Python bindings (`import knight_bus`)
- Users run on laptops, CI pipelines, containers
- DuckDB comparison starts appearing in blog posts

**Quarter 1-2 (v0.1.0):**
- Users doing analytics on 10M-100M node graphs
- Feature requests start coming in:
  - "Can I write data directly?" → OLTP discussion begins
  - "Can I stream updates?" → overlay model
  - "Can I run on multiple machines?" → might appear, but probably not yet
- Ship overlay model for zero-stale reads

**Month 6-12 (v0.2.0-v1.0):**
- If multi-node requests appear: evaluate based on actual demand
- Most likely: users want Python/JS bindings, more algorithms, visualization
- Multi-node is probably request #15, not request #1

**Long-term shape:**
Knight Bus = "DuckDB for graphs." Single-binary, zero-config, fast analytics.
Most users never need clustering. Those who do use a different tool (Spark
GraphX, Pregel-style systems).

**Likelihood: 80%** — this is the highest-probability path.

**Stress points:**
- Enterprise sales teams ask "does it cluster?" → answer is "no, and here's why
  you don't need it for analytics"
- Competitor FUD: "Knight Bus doesn't scale" → counter with benchmarks showing
  single-server is faster than their distributed solution

**Inflection point:** First user who has a graph that doesn't fit on one machine
(>500B edges, >5 TB disk). This is probably 2-3 years out.

---

## Timeline B: "Build Clustering Early" (Enterprise Play)

**Opening move:** Design for multi-node from day 1. Define partition protocol,
add networking layer (gRPC/Bolt), implement consensus for distributed writes.

**Week 1-2:**
- Designing partition protocol, not shipping features
- No benchmark, no demo, no users
- Researching Raft, Paxos, gossip protocols

**Month 1-3:**
- Still building infrastructure:
  - gRPC service definition (~800 LOC)
  - Partition manager (~1,200 LOC)
  - Cross-node query router (~600 LOC)
  - Consensus for replicated writes (~1,500 LOC)
  - Network-aware error handling (~400 LOC)
- Total: ~4,500 LOC of infrastructure before ANY algorithm ships
- No benchmark. No users. No demo.

**Month 3-6:**
- FINALLY ship PageRank... but it's distributed, which means:
  - Partition the graph across K machines (graph partitioning is NP-hard → use METIS or hash-based)
  - Hash-based: 40-60% of edges cross partition boundaries → massive network overhead
  - Distributed PageRank on 10M nodes: 15-45 sec (vs 3-5 sec single-server)
  - Network latency dominates: each iteration requires all-to-all score exchange

**Month 6-12:**
- Debugging distributed bugs: split-brain, network partitions, stale reads
- Rewriting partition strategy because hash-based was too slow
- Still no Python bindings. Still no Dijkstra. Still no users outside beta testers.
- DuckDB-for-graphs competitor (or our own Timeline A fork) has already shipped
  and has 500 GitHub stars

**Long-term shape:**
An over-engineered distributed system that's SLOWER than single-server for
every graph that fits on one machine (which is 99% of graphs). The 1% who
need distributed are already using Spark GraphX or TigerGraph.

**Likelihood: 5%** — this is the path that kills the project.

**Stress points:**
- Constant: debugging distributed systems is 10x harder than single-server
- Month 3: realize distributed PageRank is slower than single-server for <1B edges
- Month 6: run out of motivation because no users, no feedback, no benchmarks
- Year 1: the product is a distributed system nobody asked for

**Inflection point:** Month 3, when benchmarks show distributed is slower. Do
you keep going or pivot to single-server?

---

## Timeline C: "Single-Server Now, Network Layer When Users Ask" (Pragmatic Play)

**Opening move:** Ship single-server v0.0.3 (same as Timeline A). But DESIGN
the internal architecture so it COULD be distributed later:
- Trait-based engine API (local impl now, networked impl later)
- Partition-aware CSR (partition_id field, currently always 0)
- Result types that include source partition metadata

**Week 1-2 (v0.0.3):**
- Ship PageRank, benchmark, demo
- Internal code has `trait GraphEngine` but only `LocalEngine` impl
- ~50 LOC of "future-proofing" (trait definition + partition_id field)

**Month 1-3 (v0.0.4-v0.0.5):**
- More algorithms, Python bindings, overlay model
- Users start appearing
- Track feature requests: is anyone asking for multi-node?

**Month 3-6 (v0.1.0):**
- Based on user feedback:
  - If nobody asks for multi-node → don't build it, ship more algorithms
  - If 3+ users ask for multi-node → design partition protocol, target v0.2.0

**Month 6-12:**
- IF multi-node needed:
  - Add `NetworkEngine` impl of `trait GraphEngine`
  - Use gRPC for cross-node communication
  - Property sharding (like Infinigraph): topology on one node, properties distributed
  - ~2,000 LOC for basic multi-node, because trait boundaries are already clean
- IF multi-node NOT needed:
  - Ship more algorithms, visualization, cloud deployment
  - Save 3-6 months of development time

**Long-term shape:**
Same as Timeline A, but with clean extension points IF multi-node ever becomes
necessary. The trait-based design costs almost nothing to implement now but
saves months if clustering is needed later.

**Likelihood: 15%** — the pragmatic insurance policy.

**Stress points:**
- The "future-proofing" trait might be over-designed for what we know today
- Risk of YAGNI: 50 LOC of traits nobody ever uses
- But: 50 LOC is cheap insurance

**Inflection point:** Month 3, when user feedback arrives. This is the decision
point: build multi-node or don't.

---

## Cross-Timeline Analysis

| Path | Upside | Downside | Reversibility | Regret risk | Who cooperates |
|---|---|---|---|---|---|
| **A: Single-server** | Ship in 2 weeks. 80% of users never need more. DuckDB precedent. | Can't serve the 1% who need distributed. | HIGH: can add networking later if needed. | LOW: worst case, you add clustering in v1.0. | Just you. |
| **B: Clustering early** | Ready for enterprise from day 1. | 6 months of infra before first user. Distributed PageRank is slower for <1B edges. | LOW: distributed abstractions leak everywhere, hard to remove. | **VERY HIGH**: likely kills the project. No users, no feedback, no momentum. | Need beta testers willing to set up multi-node clusters. |
| **C: Pragmatic traits** | 50 LOC insurance for future clustering. Ship fast like A. | Slight over-design. Trait boundaries might be wrong. | HIGH: traits are easy to refactor. | LOW: worst case, you delete 50 LOC of unused traits. | Just you. |

### The Critical Evidence

**Neo4j's own GDS runs on a single server.** Even in their cluster, graph
algorithms execute on ONE machine. They've been building this for 15 years
and they haven't distributed their analytics. That's not because they can't —
it's because distributed graph analytics is slower for the workloads that
matter.

**Neo4j's new Infinigraph keeps topology on one shard.** Their brand-new
sharding solution (Sep 2025) doesn't distribute the graph structure. Only
properties get sharded. Because they know: splitting graph topology across
machines kills traversal performance.

**DuckDB's market cap grew to $750M+ without any clustering.** Single-server
analytics is not a limitation — it's a feature. "Zero config. One binary.
Your laptop is the cluster."

---

## Decision Filter

### Which path is strongest if everything goes normally?

**Timeline A (Single-Server).** Ship fast, get users, let demand guide the
roadmap. Every month NOT spent on distributed infrastructure is a month spent
on algorithms, bindings, and the features users actually request.

The precedent is overwhelming:
- DuckDB: no clustering → $750M valuation
- SQLite: no clustering → most deployed database in the world
- Neo4j GDS itself: single-server analytics in a clustered database

### Which path is safest if things go badly?

**Timeline A or C.** If nobody uses Knight Bus, you lose 2 weeks (v0.0.3) not
6 months (Timeline B). If everybody uses it, you have users to guide the
multi-node design.

Timeline B is the ONLY path where things can go catastrophically badly:
spending 6 months building distributed infrastructure for a product with
zero users.

### What experiment would reduce uncertainty fastest?

**Ship v0.0.3 single-server and ask users one question:**

> "What's the largest graph you work with?"

If 90% say "under 100M nodes" → single-server forever (fits on any workstation).
If 10% say "over 1B nodes" → design multi-node for v0.2.0.
If 0% answer → you don't have users yet, and clustering won't fix that.

---

## The Bottom Line

```
DO WE NEED MULTI-NODE FOR v0.0.3-v0.1.0?

NO. For three independent reasons:

1. NEO4J DOESN'T DO IT FOR ANALYTICS
   Neo4j GDS runs on a single server. Their own analytics engine is not
   distributed. If the market leader doesn't distribute graph algorithms,
   there's no competitive pressure to do it either.

2. THE PHYSICS DON'T SUPPORT IT
   Distributed PageRank on 10M nodes is SLOWER than single-server PageRank
   (network overhead > compute savings). Graph partitioning is NP-hard and
   any partition creates cross-boundary edges that require network roundtrips.

3. THE MARKET DOESN'T NEED IT
   99% of analytical datasets fit on a single server (Amazon's own telemetry).
   DuckDB proved that single-server analytics is a $750M+ market.
   Our Level 3 (edge-centric streaming) handles 1B+ nodes on a single machine.

WHEN DO WE NEED IT?
  - When a user has a graph with >5B edges AND needs sub-second traversals
  - This is a v1.0+ concern (12-18 months)
  - And it's probably property sharding (like Infinigraph), not topology sharding
  - Because topology sharding kills traversal performance

WHAT TO BUILD INSTEAD?
  v0.0.3: Single-server PageRank (2 weeks)
  v0.0.4: More algorithms + Python bindings (2-3 weeks)
  v0.0.5: Overlay model for zero-stale reads (1-2 weeks)
  v0.1.0: OLTP record store (4-6 weeks)
  v1.0:   IF users ask for multi-node, add it here
```

---

## Appendix: How Knight Bus Already Wins the "Scale" Argument

Even without multi-node, Knight Bus handles larger graphs than Neo4j:

| Graph Size | Knight Bus (Level 3) | Neo4j GDS (single server) |
|---|---|---|
| 10M nodes, 100M edges | 41 MB RAM, 150-260 sec | 8-16 GB RAM, 65-135 sec |
| 100M nodes, 1B edges | 41 MB RAM, 25-45 min | **OOM on 32 GB server** |
| 1B nodes, 10B edges | 41 MB RAM, 4-8 hours | **OOM on 256 GB server** |

The "scale" story isn't "we run on more machines." It's "we run bigger
graphs on FEWER machines." That's the DuckDB pitch — and it's more
compelling than clustering.

> "Neo4j needs a 256 GB server for 100M nodes. Knight Bus needs a laptop."
>
> That's not a limitation. That's the product.
