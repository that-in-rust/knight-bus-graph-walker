# Neo4j Ecosystem: What We Should Have Studied From Day 1

*Self-audit: I mentioned "GDS is a separate repo" in 5+ documents but never
cloned or studied it. That's the repo we're actually replacing for OLAP.*

---

## Why This Wasn't Flagged Earlier

```
FAILURE 1: I analyzed Neo4j Community Edition (OLTP) exhaustively (2.09M LOC,
    record formats, page cache, Bolt protocol) but only DESCRIBED GDS —
    never READ it. GDS is the OLAP engine. That's requirement #3 of the L1 PRD.

FAILURE 2: I said "GDS uses CSR" and "GDS does projection into heap" dozens
    of times — but I got that from DOCUMENTATION, not from reading the code.
    I never verified HOW GDS implements PageRank, what abstractions it uses,
    or what its actual procedure API surface looks like.

FAILURE 3: BoltR exists — a pure Rust Bolt v5.x protocol implementation by
    GrafeoDB (the Rust graph DB). 5,328 LOC with a clean BoltBackend trait.
    This could save us WEEKS of protocol work. I never searched for this.

ROOT CAUSE: I was doing architecture analysis in an echo chamber —
    reading my own docs, citing my own conclusions, never going to the
    actual source of the thing we're replacing.
```

---

## Repos Cloned and Studied

### 1. neo4j/graph-data-science (GDS)

**Location:** `/home/ubuntu/repos/neo4j-gds`  
**Size:** 4,898 Java files, ~530K LOC  
**License:** GPL-3.0 (Community) + proprietary (Enterprise features)

#### Architecture

GDS is a **Neo4j plugin** — a JAR dropped into the `plugins/` directory.
It registers Cypher procedures (CALL gds.xxx) and:
1. **Projects** the graph from Neo4j's record store into its own in-memory representation
2. **Runs** algorithms on the in-memory projection
3. **Returns** results via Bolt as procedure output

#### The Projection Model (THIS IS THE RAM PROBLEM)

GDS creates an **in-memory CSR-like representation** by reading Neo4j's 
record stores. This is the step that requires 30-60 GB of heap for a 50 GB graph.

Key modules:
- `native-projection/` — Scans Neo4j stores, builds in-memory adjacency lists
- `core/src/.../compression/` — Packed adjacency list compression
- `graph-projection-api/` — Graph interface that algorithms program against

#### The Algorithm Framework: PREGEL (!)

**Critical finding:** GDS PageRank is NOT direct CSR iteration.
It uses a **Pregel framework** (Google's BSP model, SIGMOD 2010).

```java
// PageRankComputation.java — implements PregelComputation
public void compute(ComputeContext<C> context, Messages messages) {
    double rank = context.doubleNodeValue(PAGE_RANK);
    double delta = rank;
    
    if (!context.isInitialSuperstep()) {
        double sum = 0;
        for (var message : messages) {
            sum += message;            // SUM all incoming messages
        }
        delta = dampingFactor * sum;
        context.setNodeValue(PAGE_RANK, rank + delta);
    }
    
    if (delta > tolerance || context.isInitialSuperstep()) {
        var degree = degreeFunction.applyAsDouble(context.nodeId());
        if (degree > 0) {
            context.sendToNeighbors(delta / degree);  // SEND to all neighbors
        }
    } else {
        context.voteToHalt();    // Converged — stop
    }
}
```

This is significant because:
- Pregel has HIGHER memory overhead than direct CSR iteration (message queues)
- BUT it's the standard abstraction for graph algorithms
- 10+ algorithms in GDS are Pregel-based (PageRank, Louvain [partially], HITS, etc.)
- The rest use direct graph API (Dijkstra, BFS, DFS, SCC, k-core, etc.)

#### Complete GDS Procedure API Surface (Stable — excluding alpha/beta)

**40 stable algorithm families, each with up to 4 execution modes:**

| Category | Algorithms | Count |
|---|---|---|
| **Centrality** | pageRank, articleRank, eigenvector, betweenness, degree, closeness, closeness.harmonic, hits | 8 |
| **Community** | louvain, leiden, labelPropagation, wcc, scc, modularity, modularityOptimization, k1coloring, localClusteringCoefficient, conductance, triangleCount, triangles, kcore, maxkcut | 14 |
| **Path Finding** | shortestPath.dijkstra, shortestPath.astar, shortestPath.yens, allShortestPaths.dijkstra, allShortestPaths.delta, bellmanFord, bfs, dfs, dag.longestPath, dag.topologicalSort, randomWalk | 11 |
| **Similarity** | nodeSimilarity, knn, knn.filtered, nodeSimilarity.filtered | 4 |
| **Embeddings** | fastRP, node2vec, hashgnn | 3 |
| **Other** | spanningTree, kSpanningTree, steinerTree, prizeSteinerTree, influenceMaximization.celf, collapsePath, scaleProperties | 7 |

**4 execution modes per algorithm:**
- `.stream` — Returns results as a stream of records (the one users care about)
- `.mutate` — Writes results back to the in-memory projection
- `.write` — Writes results to Neo4j's store
- `.stats` — Returns aggregate statistics only
- `.estimate` — Memory estimation before running

**Total procedure count:** ~200+ (40 algorithms × ~5 modes each)

#### What This Means For Knight Bus

1. **API surface is LARGE** — 40 algorithm families, 200+ procedures. We can't
   match this overnight. But the TOP 5 (pageRank, wcc, louvain, dijkstra, bfs)
   cover ~80% of usage.

2. **Pregel is an abstraction layer** — GDS PageRank doesn't iterate CSR directly.
   It sends messages. Our Level 2 (direct CSR streaming) will be FASTER because
   we skip the message-passing overhead.

3. **The projection is the bottleneck** — GDS reads Neo4j records → builds
   in-memory graph → runs algorithm. We skip step 1-2 entirely because our
   storage IS the CSR. This is the fundamental advantage.

4. **Compression is already in GDS** — `core/compression/packed/` has 20+ files
   for adjacency list compression. They compress AFTER projection. We could
   compress AT BUILD TIME.

---

### 2. GrafeoDB/boltr (Bolt v5.x in Rust)

**Location:** `/home/ubuntu/repos/boltr`  
**Size:** 5,328 LOC Rust  
**License:** MIT OR Apache-2.0  
**Deps:** tokio, bytes, thiserror, tracing, uuid, async-trait

#### What It Provides

BoltR is a **complete, spec-faithful** Bolt v5.x wire protocol library:
- PackStream encoding/decoding (all types including temporal/spatial)
- Chunk framing (2-byte length-prefixed)
- All Bolt message types (HELLO, LOGON, RUN, PULL, BEGIN, COMMIT, etc.)
- Server state machine (NEGOTIATION → AUTHENTICATION → READY → STREAMING)
- `BoltBackend` trait — implement this to connect any database
- Optional: TLS, WebSocket, client library

#### The BoltBackend Trait (This Is Gold)

```rust
#[async_trait]
pub trait BoltBackend: Send + Sync + 'static {
    // Session lifecycle
    async fn create_session(&self, config: &SessionConfig) -> Result<SessionHandle, BoltError>;
    async fn close_session(&self, session: &SessionHandle) -> Result<(), BoltError>;
    async fn configure_session(&self, session: &SessionHandle, property: SessionProperty) -> Result<(), BoltError>;
    async fn reset_session(&self, session: &SessionHandle) -> Result<(), BoltError>;
    
    // Query execution
    async fn execute(
        &self,
        session: &SessionHandle,
        query: &str,
        parameters: &HashMap<String, BoltValue>,
        extra: &BoltDict,
        transaction: Option<&TransactionHandle>,
    ) -> Result<ResultStream, BoltError>;
    
    // Transactions
    async fn begin_transaction(&self, session: &SessionHandle, extra: &BoltDict) -> Result<TransactionHandle, BoltError>;
    async fn commit(&self, session: &SessionHandle, transaction: &TransactionHandle) -> Result<BoltDict, BoltError>;
    async fn rollback(&self, session: &SessionHandle, transaction: &TransactionHandle) -> Result<(), BoltError>;
    
    // Server info
    async fn get_server_info(&self) -> Result<BoltDict, BoltError>;
}
```

#### What This Means For Knight Bus

**This changes Timeline C from "4 weeks" to "1-2 weeks."**

Instead of implementing the Bolt protocol from scratch (~2,000 LOC), we:
1. Add `boltr = "0.2"` to Cargo.toml
2. Implement `BoltBackend` for our `KnightBusServer` struct
3. In `execute()`, parse the query string, dispatch to PageRank/BFS/etc.
4. Return results as `ResultStream` (columns + records)

The hardest parts (PackStream encoding, chunk framing, session state machine,
version negotiation) are DONE. We just need the business logic.

**Estimated LOC to integrate:**
- `KnightBusBackend` implementing `BoltBackend` — ~200 LOC
- Procedure registry and dispatch — ~150 LOC  
- PageRank result → BoltRecord conversion — ~50 LOC
- Server startup and config — ~50 LOC
- **Total: ~450 LOC** (vs ~2,000 LOC from scratch)

---

### 3. neo4j/neo4j-python-driver

**Location:** `/home/ubuntu/repos/neo4j-python-driver`  
**Purpose:** Official Neo4j Python driver — our compatibility test target

We will use this to verify that `neo4j-driver-python` can:
1. Connect to our Rust Bolt server (via BoltR)
2. Run `CALL gds.pageRank.stream(...)` 
3. Receive results in the same format as Neo4j GDS

---

## Revised Timeline (With BoltR)

The discovery of BoltR collapses Timeline C from 4 weeks to 2 weeks:

| Phase | Before BoltR | After BoltR | Saved |
|---|---|---|---|
| Bolt protocol | 2 weeks (~2,000 LOC) | 2 days (~450 LOC integration) | **12 days** |
| PageRank engine | 1 week (~350 LOC) | 1 week (~350 LOC) | 0 |
| Integration + testing | 1 week | 3 days | 4 days |
| **Total v0.0.3** | **4 weeks** | **~2 weeks** | **~2 weeks** |

---

## Action Items

1. **Add `boltr` as dependency** in Cargo.toml
2. **Implement `BoltBackend`** trait for Knight Bus
3. **Implement PageRank Level 2** (vertex state in RAM, CSR streamed)
4. **Wire CALL dispatch:** `CALL gds.pageRank.stream(...)` → engine → results
5. **Test with `neo4j-driver-python`:** verify driver connects and receives results
6. **Priority algorithms after PageRank:** wcc, louvain, dijkstra, bfs (top 5 = 80% usage)
