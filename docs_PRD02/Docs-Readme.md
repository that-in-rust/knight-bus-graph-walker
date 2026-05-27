# Reference Repos: What We Cloned and Why

All repos cloned for studying the Neo4j ecosystem we're replacing with Knight Bus.

---

## Cloned Repos

| # | Repo | GitHub URL | Branch/Tag | Local Path | Size | Why We Need It |
|---|---|---|---|---|---|---|
| 1 | **Neo4j Community** | [neo4j/neo4j](https://github.com/neo4j/neo4j) | `release/5.26.0` (tag 5.26.1) | `neo4j-reference/neo4j/` (inside Knight Bus repo) | ~2.25M LOC (Java+Scala) | OLTP database engine — the thing we're rewriting. Record formats, kernel, Bolt protocol, Cypher engine. |
| 2 | **Neo4j GDS** | [neo4j/graph-data-science](https://github.com/neo4j/graph-data-science) | `2.13` | `/home/ubuntu/repos/neo4j-gds/` | ~530K LOC Java | OLAP analytics plugin — 40 algorithm families, Pregel framework, CSR projection, procedure dispatch. **This is what we replace for OLAP.** |
| 3 | **BoltR** | [GrafeoDB/boltr](https://github.com/GrafeoDB/boltr) | `main` (v0.2.0) | `/home/ubuntu/repos/boltr/` | ~5.3K LOC Rust | Pure Rust Bolt v5.x wire protocol library. **We use this as a dependency** (not rewrite). Implement `BoltBackend` trait → Neo4j drivers connect. |
| 4 | **Neo4j Python Driver** | [neo4j/neo4j-python-driver](https://github.com/neo4j/neo4j-python-driver) | `6.x` | `/home/ubuntu/repos/neo4j-python-driver/` | ~38K LOC Python | Official Neo4j Python driver. **Compatibility test target** — verify our Bolt server works with real drivers. |

---

## How to Clone (If Starting Fresh)

```bash
# Neo4j Community (already in repo under neo4j-reference/)
git clone https://github.com/neo4j/neo4j.git --branch release/5.26.0 --depth 1

# Neo4j GDS
git clone https://github.com/neo4j/graph-data-science.git --branch 2.13 --depth 1 neo4j-gds

# BoltR (Pure Rust Bolt v5.x)
git clone https://github.com/GrafeoDB/boltr.git --depth 1

# Neo4j Python Driver
git clone https://github.com/neo4j/neo4j-python-driver.git --branch 6.x --depth 1
```

---

## What We Learned From Each Repo

### 1. Neo4j Community — OLTP Architecture

- **Record formats confirmed:** 15B node, 34B relationship (4 linked-list pointers), 41B property
- **Relationship traversal:** Pointer-chase linked list (slow for analytics, fine for OLTP)
- **Dense nodes:** Extra group indirection layer (group → incoming → outgoing → loop)
- **Cypher engine:** 450K LOC (182K LOC Scala planner is the largest single module)
- **Storage engine:** Pluggable via `StorageEngine` interface (service-loaded)
- **Page cache:** Custom "Muninn" implementation (we replace with mmap)
- **Built-in algorithms:** Only Dijkstra, A*, BFS in `graph-algo/` (7K LOC)
- **Single node confirmed:** Only `community/` directory exists, no `enterprise/`

### 2. Neo4j GDS — OLAP Analytics

- **40 stable algorithm families**, ~200 procedures (`.stream`, `.mutate`, `.write`, `.stats`, `.estimate`)
- **PageRank uses PREGEL** (BSP message-passing), not direct CSR iteration — our direct approach is faster
- **The Graph interface** is the API boundary all algorithms program against: `forEachRelationship()`, `degree()`, `nodeCount()`
- **Projection is the bottleneck:** GDS copies the entire graph from Neo4j stores into Java heap (30-60 GB for 50 GB graph). We skip this — our storage IS the CSR.
- **Compression:** `core/compression/packed/` has 30K LOC of adjacency list compression (delta encoding, bit packing)
- **Procedure dispatch:** `@Procedure("gds.pageRank.stream")` → facade → algorithm → Graph trait

### 3. BoltR — Wire Protocol (USE DIRECTLY)

- **Spec-faithful Bolt v5.x** (v5.1-5.4), all PackStream types, all message types
- **`BoltBackend` trait:** 8 methods to implement (create_session, execute, begin_transaction, commit, rollback, etc.)
- **Pure Rust:** tokio, bytes, thiserror, tracing — minimal deps, no C/C++
- **Saves us ~2 weeks** of protocol implementation
- **Features:** TCP, TLS, WebSocket, auth, graceful shutdown

### 4. Neo4j Python Driver — Compatibility Testing

- **Used for:** Verifying `neo4j-driver` can connect to our Rust Bolt server
- **Test pattern:** `driver.session().run("CALL gds.pageRank.stream(...)")` should return same format as Neo4j

---

## Important Notes

- **Do NOT commit cloned repos to Knight Bus.** They live in `/home/ubuntu/repos/` (separate from our repo), except Neo4j Community which is under `neo4j-reference/` and is already gitignored.
- **Do NOT modify cloned repos.** They are read-only reference material.
- **BoltR is the only repo we use as a code dependency** (via `boltr = "0.2"` in Cargo.toml).
