# LLM Prompt: Knight Bus Storage Architecture — What Am I Missing?

*Copy everything below the line and share with an HQ LLM.*

---

## Prompt

I'm building a Rust-based graph engine that should eventually replace Neo4j for analytics and algorithm workloads. I have a working prototype (4,710 LOC Rust) called Knight Bus that uses a radically different storage format from Neo4j. I need help understanding the real risk and real complexity of this storage choice.

### What I Have Today

Knight Bus is a **read-only, single-node, immutable snapshot engine**:

- Stores graphs as Compressed Sparse Row (CSR) arrays: contiguous `offsets[]` + `peers[]` arrays
- Uses `mmap` to memory-map binary files (no custom page cache)
- Builds snapshots from CSV truth sources at build time
- Queries by: binary search for key → dense ID → offset slice → peer array
- 4 Rust traits, 23 passing tests, zero clippy warnings
- Proven fast for exact-key, fixed-hop traversal over static snapshots

### What Neo4j Does Differently

Neo4j uses **linked-list record stores** with full database machinery:

- 15-byte node records, 34-byte relationship records with next/prev pointers
- MuninnPageCache (custom off-heap page manager with clock-sweep eviction)
- Full ACID transactions with MVCC
- Write-ahead log for crash recovery
- Read/write locks with deadlock detection
- Schema constraints (uniqueness, existence)
- Causal Clustering for multi-node (Enterprise edition)
- Total: 1.58M LOC (Java + Scala)

### What I Want to Build

A Neo4j replacement where:
- **Frontend is flawlessly like Neo4j**: Cypher syntax, Bolt protocol, Neo4j driver compatibility, same error codes — users change one connection string
- **Backend uses Knight Bus-style CSR storage** instead of Neo4j's linked-list records

I also have a design document (the "Algorithm Storage Atlas") that maps 60 Neo4j GDS algorithms to 13 specialized CSR-based layout families. The thesis is: store the graph in the shape the algorithm's inner loop wants to walk, not in a generic format.

### Where I'm Confused

I initially claimed that choosing CSR storage makes the hard database problems (concurrency, durability, consistency) **easier**. I now think I was wrong — or at least misleading. Here's my confusion:

**Claim I made:** "Immutable snapshots mean no read locks, trivial crash recovery, always-consistent data."

**Why I think I was wrong:** That's true only because Knight Bus is **read-only**. The moment I need writes (CREATE, SET, DELETE in Cypher), I need:
- A write-ahead log so committed writes survive crashes
- Transaction isolation so concurrent readers don't see partial writes
- Some form of MVCC or locking
- Constraint enforcement

These are the same hard problems any database faces. CSR doesn't help with any of them.

**But I'm not sure I'm right either.** Some databases (like ClickHouse, DuckDB, Apache Kudu) use immutable segments with append-only writes and periodic compaction. They still provide ACID-like guarantees but with a fundamentally different write model than traditional row stores. Maybe CSR + append log + periodic recompile is a legitimate architecture that genuinely IS simpler than Neo4j's approach — I just don't know enough about database internals to be sure.

### Specific Questions

1. **Is the "immutable snapshots + append log + periodic recompile" model a real database architecture, or am I kidding myself?** Systems like ClickHouse, DuckDB, and LSM-tree databases seem to work this way. Is this a proven pattern for analytics workloads, or does it have hidden gotchas I'm not seeing?

2. **What database complications am I definitely underestimating?** I listed: ACID transactions, concurrent writes, crash recovery, schema constraints, auth, backup, monitoring, clustering. What else? What's the "you don't know what you don't know" list for someone who's never built a database?

3. **Does the CSR storage format genuinely simplify any database problems, or does it only simplify the read path while leaving write-path complexity identical to any other database?** I want to know if my original instinct ("CSR makes things easier") had any truth, or if it was entirely an artifact of comparing a read-only engine to a full database.

4. **For a single-node, analytics-focused graph engine (not OLTP, not multi-node), what's the minimum viable database machinery I actually need?** I don't need distributed consensus. I probably don't need sub-millisecond write latency. I DO need: don't lose committed data, don't serve garbage reads, handle concurrent Bolt connections. What's the smallest correct implementation of those requirements?

5. **Is the "append log + periodic recompile" write model acceptable for a system targeting 50GB graphs with low write rates (say <1000 mutations/minute)?** If the recompile takes 30 seconds for a 50GB graph, and I can serve stale-but-consistent reads from the previous snapshot during recompile, is that a legitimate architecture? Or is there a fundamental reason this breaks down?

6. **What's the real risk spectrum?** I wrote a risk table that said "read-only analytics = LOW risk, real-time ACID writes = HIGH risk, multi-node = VERY HIGH risk." Is that approximately right, or am I miscalibrating?

7. **Are there existing single-node, analytics-focused databases (not graph-specific) whose architecture I should study as a model?** DuckDB seems closest in spirit (embedded, analytics-focused, columnar). Are there others? What can I learn from their write models?

### Context That Might Help Your Answer

- The target workload is graph analytics: PageRank, shortest paths, community detection, centrality — not real-time transactional processing
- Users would import data from Neo4j (via CSV export or Bolt streaming), run algorithms, and read results
- Write rates are expected to be low (batch imports, not continuous OLTP)
- Single-node only for v1. Multi-node is a future concern, not a v1 requirement
- The team has deep Rust experience but limited database internals experience
- Knight Bus's 4,710 LOC proves the read path works. The open question is entirely about the write path and the database machinery around it

### What I'm Hoping To Get

An honest assessment of:
1. Where my risk estimates are wrong (too optimistic or too pessimistic)
2. What database complications I'm blind to
3. Whether the "immutable snapshots + append log" model is legitimate for this use case
4. What the minimum viable database machinery looks like for a single-node analytics engine
5. Specific systems I should study as architectural models

I don't want reassurance. I want the "here's what will actually bite you" list from someone who understands database internals better than I do.
