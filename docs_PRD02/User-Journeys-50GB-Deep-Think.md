# User Journeys: 50 GB Baseline Graph Under CRUD + Queries

*Thinking from first principles. Not architecture diagrams — actual
humans, actual minutes, actual frustrations.*

---

## Core Facts Enumerated

Before reasoning, everything I know:

### The Graph

```
50 GB Neo4j export
~50M nodes, ~500M edges
Avg degree: ~10 edges per node
Avg 5 properties per node, 2 per edge
Node record: 15B (Neo4j) → 16B (Knight Bus NodeRecord)
Edge record: 34B (Neo4j) → 8B (CSR peers, u32 pair)
Property: 41B per record (Neo4j) → 8B columnar (KB, future)
```

### The CSR Snapshot (Built from Export)

```
forward_peers:    ~2 GB (500M × 4 bytes, u32 dense IDs)
reverse_peers:    ~2 GB (500M × 4 bytes)
forward_offsets:  ~400 MB (50M × 8 bytes, u64)
reverse_offsets:  ~400 MB (50M × 8 bytes)
node_table:       ~800 MB (50M × 16 bytes NodeRecord)
key_index:        ~200 MB (50M × 4 bytes sorted keys)
strings:          ~1-3 GB (variable, key strings)
────────────────────────────────────────────────────
Total snapshot:   ~7-9 GB on disk
```

### Build Times (Estimated from code analysis)

```
CSV parse + sort:   ~5-15 minutes (external merge sort in low_ram.rs)
CSR construction:   ~2-5 minutes (flatten_adjacency_lists)
Verification:       ~2-5 minutes (optional)
Total initial build: ~10-25 minutes
```

### Query Times on CSR (Estimated)

```
Key lookup (binary search):  5-50 μs cold, 1-5 μs warm
1-hop traversal:             10-200 μs cold, 5-50 μs warm
3-hop traversal:             1-5 ms cold, 50-500 μs warm
PageRank (20 iterations):    8-40 sec (depends on free RAM)
Full rebuild from CSV:       10-25 minutes
```

### What Knight Bus Can Do Today (v0.0.2)

```
✓  Build snapshot from CSV (nodes.csv + edges.csv)
✓  Query: lookup a node, get forward/reverse/bidirectional neighbors
✓  Query: multi-hop traversal (1, 2, 3+ hops)
✓  Benchmark: measure RSS and wall time
✓  Verify: check snapshot matches CSV truth
✓  Low-RAM build: external merge sort with configurable budget

✗  No PageRank or any graph algorithm
✗  No CRUD on the snapshot (immutable)
✗  No import from Neo4j directly (CSV only)
✗  No Cypher parser
✗  No Bolt protocol
✗  No write path at all
```

### What the Industry Shows (from research)

```
Neo4j capacity planning doc (real customer):
  - 75M nodes, 150M relationships
  - 200 queries/second peak
  - 4-5 batch imports per day
  - Each batch: ~5M nodes, ~20 GB
  - Max ingest time: 1 hour per batch
  - 100-200 end users

Annie (infrastructure knowledge graph):
  - Neo4j for Kubernetes + AWS + monitoring data
  - "Continuous ingestion" from cloud APIs
  - Queries: "what changed before this broke?"
  - Pattern: import → query → import more → query again

NYC Taxi pipeline:
  - Kafka → Neo4j → GDS PageRank/BFS
  - Stream processing: continuous writes
  - Analytics: periodic (after batch settles)
```

### Missing Information I Need to Address

```
? How long does a Neo4j CSV export of 50GB take?
? What format does the export come in? (neo4j-admin dump? CSV? JSON?)
? How many writes per second in a typical "editing" session?
  → Neo4j capacity doc says 4-5 batches/day of 5M nodes
  → That's NOT continuous CRUD. It's batch-oriented.
? What does "CRUD on a graph" actually look like?
  → CREATE a node: add a Person with properties
  → READ: traverse, lookup, pattern match
  → UPDATE: change a property value
  → DELETE: remove a node and its relationships
? How often do users run GDS algorithms?
  → Research suggests: nightly or after batch imports
  → NOT continuously (too expensive)
? What percentage of queries are reads vs writes?
  → Industry standard: 80-95% reads, 5-20% writes
  → Graph databases: even more read-heavy (~95%+ reads)
```

---

## The Personas

### Persona A: "The Data Scientist" — Priya

**Context:** Priya works at a fintech. Her team has a 50 GB
fraud detection graph: 30M accounts, 200M transactions, 300M
edges (account→transaction→merchant→account). She runs graph
algorithms to find suspicious patterns.

**Current tools:** Neo4j Enterprise + GDS on a 64 GB server.
Monthly cost: ~$3,000 (Aura) or $800 (self-hosted EC2 r5.2xlarge).

**Pain:** GDS projection takes 3-5 minutes every time she
restarts Neo4j. The 64 GB server is 70% consumed by Neo4j heap.
She can't run algorithms on her laptop (16 GB MacBook).

**Write pattern:** Near-zero during analysis. Batch import
nightly from the transaction database (Kafka → Neo4j). She
doesn't edit the graph — the pipeline does.

### Persona B: "The Backend Engineer" — Marcus

**Context:** Marcus builds a social network feature. Graph:
50M users, 500M follower relationships. He needs real-time
lookups (show a user's followers) AND periodic analytics
(compute influence scores for the recommendation engine).

**Current tools:** Neo4j Community on a 32 GB server.
Pain: Community edition has no GDS. He wrote his own PageRank
in Python (slow). Considering upgrading to Enterprise ($36K/yr)
or switching to Memgraph.

**Write pattern:** Continuous. Users follow/unfollow. 100-500
writes per second during peak hours. The graph is ALWAYS
changing.

### Persona C: "The DevOps/SRE Lead" — Aisha

**Context:** Aisha manages an infrastructure knowledge graph
(like the Annie example). Kubernetes pods, AWS resources,
monitoring data. 10M nodes, 100M edges but growing fast.
She queries: "what changed before this alert fired?"

**Current tools:** Neo4j + custom ETL pipeline.
Pain: Continuous ingestion from cloud APIs means the graph
is always being updated. Queries need to be fresh (within
seconds, not minutes).

**Write pattern:** Continuous and bursty. Cloud API syncs
every 30 seconds. Kubernetes events arrive in real-time.
50-200 writes per second sustained, spikes to 1000/sec during
deployments.

### Persona D: "The Analyst" — Tom

**Context:** Tom runs a consulting firm. Clients give him
data dumps (CSV, SQL exports). He imports them into Neo4j,
runs analyses, produces reports. Then moves to the next client.

**Current tools:** Neo4j Desktop on a 16 GB laptop.
Pain: Large datasets OOM on his laptop. He has to use a cloud
instance for anything over 5M nodes.

**Write pattern:** Bulk import ONCE, then read-only analysis.
Zero writes after import. Classic batch analytics.

---

## Journey A: Priya the Data Scientist

### Day in Priya's Life with Knight Bus

```
08:00  Priya arrives. Nightly batch import completed at 03:00.
       The pipeline exported a fresh CSV from the transaction DB.
       
       ON NEO4J TODAY:
       The pipeline ran: neo4j-admin import → took 45 minutes
       Then: CALL gds.graph.project(...) → took 3 minutes
       GDS projection sitting in 12 GB of heap memory
       Server total memory: 48 GB used / 64 GB
       
       ON KNIGHT BUS:
       The pipeline ran: knight-bus build --nodes tx_nodes.csv \
         --edges tx_edges.csv --output /data/snapshot-2026-05-25
       Build took: 15 minutes (external merge sort + CSR)
       Snapshot on disk: 8 GB
       Server memory used: 2 GB during build, now 0 (released)
       
08:15  Priya opens her Jupyter notebook.
       She wants to run PageRank to find influential accounts.
       
       ON NEO4J:
       CALL gds.pageRank.stream('fraud-graph', {maxIterations: 20})
       → Projection already exists → algorithm runs in 45 seconds
       → RSS: 48 GB (12 GB projection + 4 GB algorithm + 32 GB base)
       → She gets results. Good.
       
       ON KNIGHT BUS:
       let runtime = MmapWalkRuntime::open("/data/snapshot-2026-05-25")?;
       let scores = page_rank(&runtime, &PageRankConfig::default())?;
       → First run: 12 seconds (paging in CSR from NVMe)
       → RSS: 1.2 GB (score arrays + some cached mmap pages)
       → She gets results. Faster. Way less memory.
       
08:30  Priya adjusts damping factor, runs again.
       
       ON NEO4J:
       → Projection still in memory → algorithm runs: 45 seconds
       → Same RSS: 48 GB
       
       ON KNIGHT BUS:
       → CSR is cached by OS from first run → runs: 5 seconds
       → RSS: 1.2 GB
       → 9x faster for iteration #2+
       
09:00  Priya wants to try on her LAPTOP (train ride home).
       She has the CSV on her MacBook (16 GB, 8 GB free).
       
       ON NEO4J:
       neo4j-admin import → OK (takes 1 hour)
       CALL gds.graph.project(...) → OOM.
       "The server does not have enough memory to perform this
       operation. Estimated minimum: 24 GB. Available: 6 GB."
       DEAD. Can't run. Period.
       
       ON KNIGHT BUS:
       knight-bus build --nodes tx.csv --edges tx_edges.csv \
         --output ~/snapshot --memory-budget-mb 2048
       → Builds with 2 GB budget (external merge sort) → 25 min
       → PageRank: 30-40 seconds (heavy mmap paging)
       → IT RUNS. Results are correct. Just slower.
       → Priya can work on the train. 

10:00  Nightly batch import is PARTIAL (only new transactions).
       5M new edges arrived overnight. Total is now 505M edges.
       
       ON NEO4J:
       Pipeline MERGEs 5M new relationships → takes 20 minutes
       Drop old GDS projection → gds.graph.project(...) again
       → 3 minutes to rebuild entire projection from scratch
       → Even though only 1% of data changed
       
       ON KNIGHT BUS (v0.0.3 — snapshot model):
       Option 1: Rebuild entire snapshot from fresh CSV export
       → 15 minutes (same as initial build)
       → Wasteful: 99% of data unchanged
       
       Option 2: Future overlay model (v0.0.5)
       → Append 5M edges to overlay → seconds
       → PageRank runs on CSR + overlay → 13 seconds
       → Recompact when convenient → 15 minutes, non-blocking
```

### What Priya Experiences (Emotional Layer)

```
WITH NEO4J:
  08:00  Waiting for GDS projection... "this again" (annoyed)
  08:15  PageRank running... 45 seconds... "not terrible" (neutral)
  09:00  OOM on laptop. "Fuck." (angry) → SSH back to server
  10:00  Drop projection, rebuild... "every single morning" (resigned)

WITH KNIGHT BUS v0.0.3:
  08:00  Snapshot already built by nightly pipeline (relieved)
  08:15  PageRank: 12 seconds first run. "Wait, it's done?" (surprised)
  08:30  Second run: 5 seconds. "I can iterate!" (delighted)
  09:00  Works on laptop! 30 seconds. "Slower but works!" (happy)
  10:00  Need to rebuild snapshot... 15 minutes (annoyed but OK)
         → v0.0.5 overlay model: seconds (delighted)
```

### What Breaks in This Journey

```
BREAK #1: "How do I get my data INTO Knight Bus?"
  Today: Must export from Neo4j to CSV, then knight-bus build.
  Pain: Neo4j CSV export of 50 GB takes 30-60 minutes.
  TOTAL: 30-60 min export + 15 min build = 45-75 min pipeline.
  Fix (v0.0.4): Direct Bolt connection to stream graph data.
  
BREAK #2: "I need to rebuild the entire snapshot for 1% new data."
  Today: Yes. Full rebuild is the only option.
  Pain: 15 minutes for 1% change is wasteful.
  Fix (v0.0.5): Overlay model. Append new edges, recompact later.
  
BREAK #3: "I can't query the graph while rebuilding."
  Today: True IF using the same output directory. 
  Fix: Build to a new directory, atomically swap symlink.
  → /data/snapshot-current → /data/snapshot-2026-05-25
  → Build new snapshot to /data/snapshot-2026-05-26
  → mv symlink: /data/snapshot-current → new snapshot
  → Zero-downtime rebuild. Old snapshot serves until swap.
```

---

## Journey B: Marcus the Backend Engineer

### Day in Marcus's Life

```
09:00  Marcus deploys new follow/unfollow feature.
       500 writes/second during peak. Social graph: 50M users.
       
       THE REALITY: Marcus needs BOTH:
       - Real-time reads: "Show me Alice's 500 followers" (< 10 ms)
       - Periodic analytics: "Compute influence scores" (nightly)
       
09:01  User hits "Follow" → CREATE (alice)-[:FOLLOWS]->(bob)
       
       ON NEO4J:
       Write goes to record store → ~1 ms → committed
       Immediately visible in next read query ✓
       
       ON KNIGHT BUS v0.0.3:
       🚫 CANNOT DO THIS.
       Knight Bus is read-only. No write path.
       Marcus CANNOT use Knight Bus for OLTP.
       
       ON KNIGHT BUS v0.1.0 (future OLTP/OLAP split):
       Write goes to OLTP store → ~0.5 ms → committed
       OLAP CSR snapshot: not updated yet (stale by minutes)
       Read "Alice's followers" → routes to OLTP → fresh ✓
       
12:00  Marcus runs influence score computation.
       
       ON NEO4J:
       CALL gds.graph.project('social', 'User', 'FOLLOWS')
       → 3 minutes projection
       → Meanwhile, writes are blocked? NO — but projection
         is a point-in-time snapshot. New follows during
         projection are not included. This is ALREADY stale.
       CALL gds.pageRank.stream('social', ...)
       → 60 seconds
       Total: 4 minutes. Stale by 3-4 minutes.
       
       ON KNIGHT BUS v0.0.3:
       Export CSV from Neo4j → knight-bus build → pagerank
       Total: 45+ minutes. Too slow for "midday check."
       But: for NIGHTLY analytics, this is fine.
       
       ON KNIGHT BUS v0.1.0:
       Background rebuild from OLTP WAL → 2-5 min
       pagerank on fresh snapshot → 10 sec
       Total: available within 5 minutes of last write.
```

### The Hard Truth About Marcus

```
Marcus CANNOT use Knight Bus v0.0.3 AT ALL for his primary
use case (real-time follow/unfollow). He needs a write path.

Knight Bus v0.0.3 is NOT a Neo4j replacement for Marcus.
It's a Neo4j GDS REPLACEMENT — an analytics sidecar.

Marcus's journey with Knight Bus:
1. Keep Neo4j for OLTP (follows, lookups)
2. Nightly export to CSV
3. knight-bus build → snapshot
4. knight-bus pagerank → influence scores
5. Import scores back into Neo4j as node properties
6. Use scores in his recommendation engine

This is VIABLE but it's a data pipeline, not a database switch.
```

### What Breaks for Marcus

```
BREAK #1: "I need writes." (v0.0.3 → can't help)
BREAK #2: "Export-build-import loop is too slow for hourly refresh."
  → 45-75 min total pipeline for each refresh
  → Maybe acceptable for nightly, NOT for hourly

BREAK #3: "Two systems to maintain." (Neo4j + Knight Bus)
  → Operational overhead
  → Data consistency concerns

Marcus is NOT the v0.0.3 target user. He's the v0.1.0 user.
```

---

## Journey C: Aisha the SRE Lead

### Day in Aisha's Life

```
09:00  Kubernetes cluster is humming. Cloud sync runs every 30 sec.
       50-200 writes/second to the infrastructure graph.
       10M nodes, 100M edges (smaller than 50 GB but growing).
       
       Aisha's queries:
       - "What pods are running on node X?" → traversal (real-time)
       - "What changed in the last hour?" → temporal query
       - "What's the blast radius of this deployment?" → multi-hop
       
       ALL NEED FRESH DATA. 2-minute staleness is NOT acceptable.
       If a pod crashed 30 seconds ago, Aisha needs to see it NOW.
       
       ON KNIGHT BUS v0.0.3:
       🚫 CANNOT SERVE AISHA.
       No write path. No real-time updates. No temporal queries.
       Snapshot staleness is unacceptable for incident response.
       
       ON KNIGHT BUS v0.1.0 (overlay model):
       Writes go to overlay → visible immediately
       "What changed?" → query overlay (last N seconds of writes)
       "Blast radius" → traversal on CSR + overlay → fresh, fast
       BUT: needs overlay model + temporal query support
       → This is v0.2.0+ territory
```

### The Hard Truth About Aisha

```
Aisha is NOT a Knight Bus user. Not at v0.0.3. Not at v0.1.0.
Maybe at v1.0 when we have:
- Full write path with sub-second visibility
- Temporal query support
- Continuous ingestion pipeline (Kafka → KB)

Aisha's workload is OLTP-dominant. She needs a database,
not an analytics engine. Neo4j is actually a reasonable fit.
Her pain is scale (she'll hit OOM at 100M+ nodes) not speed.
```

---

## Journey D: Tom the Analyst

### Day in Tom's Life

```
09:00  Client sends Tom a 50 GB CSV export of their CRM graph.
       Customers, products, purchases, support tickets.
       50M nodes, 500M edges.
       
       ON NEO4J (Tom's 16 GB laptop):
       neo4j-admin import --nodes customers.csv --relationships purchases.csv
       → 1 hour import
       → Database size: 50 GB on disk
       → Start Neo4j... heap = 4 GB (half his free RAM)
       → "MATCH (c:Customer)-[:PURCHASED]->(p:Product) RETURN count(*)"
       → Works. Slow (30 sec for full scan) but works.
       → CALL gds.graph.project(...)
       → OOM. "Need 24 GB heap." 
       → DEAD for analytics. Tom needs a cloud instance.
       → Spins up AWS: $50/day for r5.2xlarge. Transfers data.
       → 2 hours of setup before he can run PageRank.
       
       ON KNIGHT BUS v0.0.3 (Tom's 16 GB laptop):
       knight-bus build --nodes customers.csv --edges purchases.csv \
         --output ./client-snapshot --memory-budget-mb 2048
       → 20 minutes build (external merge sort, 2 GB budget)
       → Snapshot: 8 GB on disk
       → knight-bus pagerank --snapshot ./client-snapshot
       → 30-40 seconds (mmap paging with 6 GB free RAM)
       → RESULTS. ON HIS LAPTOP. No cloud. No $50/day.
       
09:30  Tom wants to explore the graph interactively.
       "Show me the top customer and their purchase network."
       
       ON NEO4J:
       MATCH (c:Customer {name: 'Acme Corp'})-[:PURCHASED]->(p)
       RETURN p.name LIMIT 20
       → Works via Neo4j Browser. Nice visualization.
       → Interactive. Click-to-expand. Beautiful.
       
       ON KNIGHT BUS v0.0.3:
       knight-bus query --snapshot ./client-snapshot \
         --entity "Acme Corp" --dir forward --hops 1 --format json
       → Returns JSON list of neighbor IDs. 
       → No visualization. No click-to-expand. CLI only.
       → Tom: "OK but where's the UI?" 😐
       
10:00  Tom finds an anomaly. Wants to tag it.
       
       ON NEO4J:
       SET c.flagged = true, c.notes = "Suspicious cluster"
       → Done. Persisted. Property updated.
       
       ON KNIGHT BUS v0.0.3:
       🚫 CANNOT WRITE.
       Tom writes the finding in his notes app instead.
       → Acceptable for analysis workflow. Not ideal.
       
11:00  Client sends updated data (5 GB delta, new transactions).
       Tom needs to re-analyze with the latest data.
       
       ON NEO4J:
       LOAD CSV FROM 'file:///delta.csv' AS row
       MERGE (c:Customer {id: row.customer_id})...
       → 30 minutes for 5 GB MERGE operation
       → Drop + rebuild GDS projection → 3 min
       → Re-run PageRank → 60 seconds
       
       ON KNIGHT BUS v0.0.3:
       → Append delta to original CSV files (cat delta.csv >> edges.csv)
       → Rebuild entire snapshot from full CSV
       → 20 minutes rebuild (same as initial)
       → PageRank: 30-40 seconds
       
       Both: ~25-35 minutes total for "add 10% new data + re-analyze"
       Neither is great. Both work.
```

### What Makes Tom LOVE Knight Bus

```
1. "It runs on my laptop."
   This is the #1 win. No cloud instance. No $50/day.
   Tom can do the same analysis he used to need a server for.
   
2. "It's fast enough for iteration."
   30-40 seconds for PageRank. Not instant, but he can run it
   5-6 times in an hour while exploring different parameters.
   Neo4j: couldn't even start GDS on his laptop.
   
3. "No setup."
   No Java. No JVM tuning. No heap configuration.
   cargo install knight-bus → knight-bus build → knight-bus pagerank.
```

### What Makes Tom FRUSTRATED

```
1. "No visualization."
   Neo4j Browser is beautiful. Knight Bus is CLI only.
   → v0.1.0 could add a simple web UI.
   → Or: output JSON that tools like Gephi can import.
   
2. "I can't annotate my findings."
   No write path means he can't tag nodes or add notes.
   → v0.0.5 overlay model would allow this.
   
3. "Rebuilding from scratch for 10% delta is wasteful."
   → v0.0.5 overlay model fixes this.
```

---

## Journey E: The Pipeline (No Human)

### The 24/7 Data Pipeline

```
This is the most common real-world pattern: no human in the loop
during steady state. A pipeline writes data, analytics run on
a schedule.

00:00  Cron job: export from PostgreSQL → CSV
       50M nodes, 500M edges (full export, ~50 GB)
       Time: 30-60 minutes
       
01:00  knight-bus build --nodes export/nodes.csv \
         --edges export/edges.csv \
         --output /data/snapshots/$(date +%Y%m%d) \
         --memory-budget-mb 4096
       Time: 15-20 minutes
       
01:20  knight-bus pagerank --snapshot /data/snapshots/20260525
       Time: 10 seconds (64 GB server, CSR fits in RAM)
       Output: scores.csv
       
01:21  knight-bus dijkstra --snapshot /data/snapshots/20260525 \
         --source "root-account" --output shortest_paths.csv
       Time: 5 seconds
       
01:22  Upload scores.csv → PostgreSQL / S3 / Dashboard
       Time: 1 minute
       
01:23  Done. Total pipeline: ~50-80 minutes.
       Results available for dashboards by 02:00.
       
       Compare Neo4j pipeline:
       00:00  Export from PostgreSQL → neo4j-admin import
       01:00  Start Neo4j, wait for warmup
       01:05  gds.graph.project(...) → 3-5 minutes
       01:10  gds.pageRank(...) → 60 seconds
       01:11  Export results → CSV
       01:15  Done. Total: ~75 minutes.
       
       Similar total time. Knight Bus is simpler (no JVM, no server,
       no heap tuning, no GDS license).
```

### The Incremental Pipeline (Daily Delta)

```
This is where it gets interesting. Most real-world graphs
don't get fully exported daily. They get DELTAS.

00:00  Export ONLY today's new/changed records from PostgreSQL
       Delta: 500K new nodes, 5M new edges (~5 GB)
       Time: 5 minutes
       
00:05  ON KNIGHT BUS v0.0.3 (snapshot model):
       MUST rebuild entire snapshot from full data.
       Option A: Keep full CSV, append delta, rebuild all
       → 15-20 minutes rebuild for entire 500M+5M edges
       
       Option B: Export full CSV from PostgreSQL nightly
       → 30-60 minutes export + 15-20 minutes build
       
       Both options: rebuild EVERYTHING for 1% change.
       
       ON KNIGHT BUS v0.0.5 (overlay model):
       Append 5M edges to overlay: ~2 seconds
       Overlay size: 5M × 8 bytes = 40 MB
       PageRank on CSR + overlay: 11 seconds (vs 10 sec pure CSR)
       Negligible overhead for 1% new data.
       
       Recompact weekly (merge overlay into new CSR):
       15-20 minutes, non-blocking, nightly maintenance.
       
       ON KNIGHT BUS v0.1.0 (WAL replay):
       PostgreSQL WAL → knight-bus ingest (streaming)
       Changes appear in overlay within seconds
       No CSV export needed. No batch pipeline.
       Real-time sync. Dream state.
```

---

## The Consolidated View

### Who Is the v0.0.3 Customer?

```
Persona    Can use v0.0.3?   Primary pain solved          What's missing
─────────  ───────────────   ────────────────────         ──────────────
Priya      ✓ YES             "PageRank on my laptop"      Incremental updates
Tom        ✓ YES             "No cloud instance needed"   Visualization, writes
Pipeline   ✓ YES             "Simpler than Neo4j GDS"     Incremental updates
Marcus     ✗ NO              needs real-time writes        Everything OLTP
Aisha      ✗ NO              needs sub-second freshness    Everything OLTP+temporal
```

**v0.0.3 serves TWO personas (Priya, Tom) and ONE use case
(batch pipeline) well. Marcus and Aisha need v0.1.0+.**

### The Critical CRUD Journey for Each Persona

#### Priya's CRUD (batch analytics):
```
CREATE: Nightly pipeline inserts 5M new edges → rebuild snapshot
READ:   PageRank, community detection → runs on snapshot
UPDATE: Pipeline updates fraud flags → rebuild snapshot
DELETE: Pipeline removes expired accounts → rebuild snapshot

CRUD frequency: 1x/day batch, 5-10x/day analytics reads
Acceptable staleness: hours (nightly batch is already stale)
Knight Bus v0.0.3 fit: EXCELLENT
```

#### Tom's CRUD (consulting analyst):
```
CREATE: Import client data once → build snapshot
READ:   PageRank, traversals, exploration → runs on snapshot
UPDATE: Wants to annotate findings → CAN'T (no writes)
DELETE: Doesn't delete data during analysis

CRUD frequency: 1 import, 10-50 reads, 0-5 desired writes
Acceptable staleness: N/A (data doesn't change during analysis)
Knight Bus v0.0.3 fit: GOOD (annotation gap is livable)
```

#### Pipeline CRUD (automated):
```
CREATE: Daily delta of new records → rebuild or overlay
READ:   Run algorithms, export results → runs on snapshot
UPDATE: Rare (pipeline replaces, doesn't update in-place)
DELETE: Occasional cleanup → rebuild snapshot

CRUD frequency: 1 write cycle/day, 5-20 algorithm runs
Acceptable staleness: hours (pipeline runs on a schedule)
Knight Bus v0.0.3 fit: GOOD (rebuild cost is the main gap)
```

#### Marcus's CRUD (real-time backend):
```
CREATE: 100-500 follows/second (continuous)
READ:   200 queries/second (show followers, feed, suggestions)
UPDATE: Profile changes, 10-50/second
DELETE: Unfollows, 50-200/second

CRUD frequency: continuous, ~1000 ops/second
Acceptable staleness: 0 for reads, hours for analytics
Knight Bus v0.0.3 fit: IMPOSSIBLE (no write path)
```

#### Aisha's CRUD (infrastructure monitoring):
```
CREATE: 50-200 writes/second (cloud sync every 30 sec)
READ:   "What changed?" queries need sub-second freshness
UPDATE: Pod status changes, health checks
DELETE: Resources deprovisioned

CRUD frequency: continuous, 100-500 ops/second
Acceptable staleness: <30 seconds for incident response
Knight Bus v0.0.3 fit: IMPOSSIBLE (no write path, too stale)
```

---

## The 50 GB Baseline Journey — Step by Step

### For the v0.0.3 target user (Priya/Tom/Pipeline):

```
STEP 1: INITIAL LOAD (happens once)
────────────────────────────────────
User has: 50 GB of CSV files (nodes.csv + edges.csv)
Action:   knight-bus build --nodes nodes.csv --edges edges.csv \
            --output /data/my-graph --memory-budget-mb 4096
Time:     15-25 minutes
Result:   8 GB CSR snapshot on disk
Memory:   4 GB during build (configurable), 0 after
Feeling:  "A bit slow but reasonable. Neo4j import took 45 min."

STEP 2: FIRST QUERY (the "wow" moment)
────────────────────────────────────────
Action:   knight-bus pagerank --snapshot /data/my-graph
Time:     8-12 seconds (first run, paging CSR into OS cache)
Memory:   ~1.2 GB RSS
Feeling:  "WAIT. 12 seconds? Neo4j GDS takes 4 minutes for this."
          This is the viral moment.

STEP 3: ITERATE (the "I can work" phase)
────────────────────────────────────────
Action:   Run PageRank 5 more times with different parameters
Time:     5-6 seconds each (CSR cached by OS)
Memory:   ~1.2 GB RSS (stable)
Feeling:  "I can try 10 things in a minute. On Neo4j I tried 1."

STEP 4: EXPLORE (traversal queries)
────────────────────────────────────
Action:   knight-bus query --entity "user-12345" --dir forward --hops 2
Time:     < 1 ms (warm cache)
Memory:   negligible
Feeling:  "Fast, but I wish I had a visual browser."

STEP 5: DATA CHANGES (the "now what?" moment)
──────────────────────────────────────────────
The source database got 5M new edges (1% of graph).
User needs to update the analytics.

v0.0.3 OPTION: Rebuild from scratch
  Action: Re-export CSV (with new data) → knight-bus build
  Time:   15-25 minutes rebuild
  Feeling: "25 minutes to add 1%? That's wasteful."
           But: "Still beats Neo4j's 45-min import + 4-min projection."
  
v0.0.5 OPTION (future): Overlay
  Action: knight-bus append --snapshot /data/my-graph \
            --edges delta_edges.csv
  Time:   ~5 seconds to add 5M edges to overlay
  Feeling: "Now THAT's what I expected."

STEP 6: CONTINUOUS OPERATION (daily rhythm)
───────────────────────────────────────────
Day 1: Build snapshot → run algorithms → export results
Day 2: Rebuild snapshot with new data → run algorithms
Day 3: Same
...
Day 30: Snapshot has been rebuilt 30 times.
        Each rebuild overwrites the previous.
        
Alternative (v0.0.5): overlay accumulates 30 days of deltas.
  Overlay grows: 30 × 5M = 150M edges in overlay.
  PageRank slows: ~15 seconds (vs 10 sec pure CSR).
  User runs recompact → fresh CSR → back to 10 seconds.
  
STEP 7: GRAPH DOUBLES IN SIZE (stress test)
───────────────────────────────────────────
6 months later: graph grew from 50 GB to 100 GB.
1B edges, 100M nodes. CSR snapshot: ~16 GB.

On 64 GB server: PageRank in 15-25 seconds ✓
On 16 GB laptop: PageRank in 60-120 seconds (heavy paging) ✓
On Neo4j: GDS projection needs 32+ GB heap → most servers OOM

The mmap model SCALES. It gets slower, not broken.
Neo4j's heap model BREAKS. It OOMs at a threshold.
```

---

## What I Was Missing (and Now See)

### Missing Fact #1: "CRUD" for graph analytics users ≠ "CRUD" for app developers

```
Priya/Tom/Pipeline never do:
  INSERT INTO graph VALUES ('alice', 'follows', 'bob')
  UPDATE graph SET weight = 0.5 WHERE ...
  DELETE FROM graph WHERE ...

They do:
  REBUILD graph FROM new_data_source
  
This is not CRUD. This is ETL → REBUILD → ANALYZE.
The "write path" for analytics users IS the rebuild.
```

### Missing Fact #2: Neo4j GDS is ALREADY stale

```
When Neo4j projects a graph for GDS, it takes a point-in-time
snapshot. Any writes during or after projection are NOT visible
to the algorithm. The GDS projection IS a stale snapshot.

So Knight Bus's snapshot model isn't WORSE than Neo4j GDS.
It's the SAME freshness model, just faster to query.

The difference: Neo4j can rebuild the projection in 3-5 min
(it reads from its own record store). Knight Bus rebuilds in
15-25 min (it reads from CSV). 

Fix (v0.0.4): Direct Bolt import from Neo4j → 3-5 min rebuild.
Fix (v0.0.5): Overlay → seconds for incremental updates.
```

### Missing Fact #3: The rebuild doesn't block reads

```
Knight Bus can build a new snapshot to a DIFFERENT directory
while the old snapshot continues serving queries.

  /data/snapshots/v1  ← currently serving queries
  /data/snapshots/v2  ← being built in background
  
When v2 is done, swap the symlink:
  /data/current → /data/snapshots/v2
  
Next query opens v2. Old queries on v1 finish naturally
(mmap handles this — the OS keeps pages mapped until all
readers close).

Zero-downtime rebuild. No blocking. Already possible today.
```

---

## The Bottom Line

### For v0.0.3:

The honest user journey is:

> **"Import your graph once (15-25 min). Run algorithms instantly
> (5-12 sec). When data changes, rebuild (15-25 min). Queries
> never block during rebuild. Your laptop is enough."**

The users who love this: analysts, data scientists, batch
pipelines. The users who can't use this yet: app developers,
SREs needing real-time writes.

### The CRUD reality:

```
What "CRUD" means for our v0.0.3 users:
  C = Build a new snapshot from new data
  R = Run algorithms and traversals on the snapshot
  U = Rebuild the snapshot (there is no in-place update)
  D = Delete the snapshot directory

This is not a database. It's an analytics engine.
And that's OK. DuckDB started the same way.
```
