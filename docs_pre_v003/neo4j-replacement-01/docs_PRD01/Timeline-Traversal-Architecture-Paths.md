# Timeline Traverser: Architecture Paths for Neo4j Replacement

*Generated using the [Timeline Traverser](https://app.devin.ai/settings/playbooks/77ff5d83719749b9a8e14a3ea578fd47) playbook.*
*Simulating multiple plausible futures for the Neo4j replacement architecture decision.*

---

## Decision Frame

- **Fork in the road:** How much should the backend diverge from Neo4j's architecture while keeping the frontend (Cypher, Bolt, drivers) Neo4j-compatible?
- **Desired outcome:** A Rust-based graph engine that Neo4j users can switch to with zero learning curve, that is dramatically faster for the workloads Knight Bus has proven (traversal, algorithms), and that ships within a realistic timeline for a small team.
- **Hard constraints:**
  - Frontend must be flawlessly like Neo4j (Cypher syntax, Bolt protocol, driver compatibility, error codes)
  - Must handle 50GB+ graphs (the onboarding scenario)
  - Must be single-binary, no JVM dependency
  - Current proven asset: 4,710 LOC Rust, 4 traits, 23 tests, dual-CSR mmap runtime
- **Time horizon:** Week 1 → Month 1 → Quarter 1 → Year 1
- **What would count as failure:** A system that either (a) can't run real Neo4j Cypher queries, or (b) isn't measurably faster than Neo4j for the target workloads, or (c) takes so long to build that the team loses momentum.

---

## Timeline A: "The Faithful Port"

*Exact Rust rewrite of Neo4j's architecture. Same record store, same page cache, same kernel, same everything — just in Rust instead of Java.*

### Opening Move

Fork the Neo4j 5.x community architecture 1:1 into Rust. Start with the storage engine: 15-byte node records, 34-byte relationship records, 41-byte property records, MuninnPageCache, GBPTree indexes. Write it all from scratch in Rust, matching Neo4j's on-disk format byte-for-byte.

### Week 1

- Set up Rust workspace with module structure mirroring Neo4j's 68 folders
- Stub the `StorageEngine` trait matching `kernel-api/` (18,542 LOC in Java)
- Begin porting `record-storage-engine/` record format decoders
- Reality: just reading and understanding Neo4j's record format takes most of the week. The format has evolved over 15 years and has subtle backward-compat branches

### Month 1

- Node record store compiles and can read/write 15-byte records
- Relationship record store partially working (34-byte linked-list records)
- Page cache prototype using `mmap` (simpler than MuninnPageCache's clock-sweep)
- LOC written: ~8-12K Rust
- Reality: you're debugging byte-layout bugs constantly. Neo4j's record format documentation is embedded in Java code, not in specs. Every field position is a magic number
- **What you DON'T have:** Any query capability. Can't parse Cypher, can't serve Bolt, can't answer a single query

### Quarter 1

- Record storage engine: ~25-35K LOC (vs Neo4j's 70K Java). Node, Relationship, Property stores work
- Basic kernel transaction lifecycle: begin/commit/rollback
- WAL (write-ahead log) prototype: crash recovery works for simple cases
- GBPTree B+tree index: ~5K LOC
- Id generator with free-list recycling
- LOC total: ~45-60K
- Reality: you have a working storage engine that nobody can talk to. No Cypher, no Bolt, no CLI. You can write unit tests that insert/read nodes, but no user can use it
- **Stress:** High. Months of unglamorous plumbing with zero user-visible progress. Every "just port this" turns into "wait, why does Neo4j do it this way?" investigations

### Year 1

- Storage engine complete: ~50-70K LOC
- Kernel with transactions, recovery, constraints: ~40-50K LOC
- Cypher parser (ANTLR-equivalent in Rust): ~15-25K LOC
- Query planner (basic cost-based): ~30-40K LOC
- Interpreted runtime (Volcano operators): ~20-30K LOC
- Bolt protocol v4/v5: ~10-15K LOC
- Total: ~180-250K LOC
- Reality: you can run basic Cypher queries over Bolt. But you're debugging Cypher edge cases (NULL propagation, three-valued logic, implicit coercions) for months. The planner is the hardest part — Neo4j's IDP solver is 182K LOC of Scala for a reason
- **What you gained:** Full Neo4j compatibility on paper
- **What you lost:** Knight Bus's entire thesis. The record store uses linked-list records, not CSR. Traversal is pointer-chasing, not contiguous reads. You've rebuilt the exact architecture that Knight Bus proved was slow for traversal workloads

### Long-term Shape

A technically impressive but strategically confused project. You have a Rust Neo4j that is ~1.5-2x faster than Java Neo4j due to no GC pauses and better memory layout, but NOT the 100x traversal speedup that was the original motivation. The project is "Neo4j but slightly faster" — a hard sell against Neo4j's 15 years of battle-testing, community, tooling, and documentation.

### Likelihood

- Completion by Year 1: **30%** (Neo4j's architecture is deeply complex; the planner alone could take 6 months)
- Measurable speedup: **1.5-2x** (Rust vs Java gains, no architectural advantage)
- User adoption: **Low** (why switch for 2x when Neo4j has a massive ecosystem?)

### Stress Points

- Month 2-4: "We've written 30K LOC and still can't answer a single query"
- Month 6: "The planner is way harder than we thought"
- Month 9: "We're debugging Cypher conformance edge cases instead of building cool stuff"
- Month 12: "We're Neo4j but worse because we haven't had 15 years of bug fixes"

### Inflection Points

- If the team discovers that Rust's performance advantages alone don't justify the effort (likely around Month 3), there's pressure to either abandon or pivot
- If a Neo4j compatibility test suite exists, it could accelerate validation — but it could also reveal how many edge cases remain

---

## Timeline B: "The Conservative Hybrid"

*Frontend identical to Neo4j. Backend mostly conventional but uses CSR snapshots as a read-acceleration layer alongside a traditional record store.*

### Opening Move

Accept that writes need a conventional storage engine. Build a lightweight record store for mutations, BUT add a CSR snapshot materialization step that periodically compiles the mutable store into Knight Bus-style immutable snapshots for reads. The read path uses CSR when available, falls back to the record store for recently-written data.

### Week 1

- Stub module structure: `mutable_store/` + `snapshot_store/` + `cypher/` + `bolt/`
- Reuse Knight Bus's existing 4,710 LOC as the snapshot engine
- Stub a minimal `MutableRecordStore` trait
- Begin Bolt protocol implementation (this is the fastest path to "users can connect")

### Month 1

- Bolt v4 prototype: clients can connect, authenticate, send queries
- Minimal Cypher parser: `MATCH (n) RETURN n`, `MATCH (n {id: $id}) RETURN n`, `MATCH (n)-[:REL]->(m) RETURN m`
- Record store: simple append-only log for writes (not a full Neo4j store)
- CSR snapshot builder triggered on write quiescence
- LOC: ~12-18K
- Reality: users can connect with Neo4j drivers and run simple queries. The experience is rough but something works. Reads go through CSR (fast), writes go through append log (simple)
- **What you DON'T have:** Complex Cypher (aggregation, OPTIONAL MATCH, WITH, UNWIND), transactions, crash recovery

### Quarter 1

- Cypher coverage: ~40% of production queries (MATCH, WHERE, RETURN, CREATE, SET, DELETE)
- Bolt v4 fully working with official Neo4j drivers (Python, Java, JavaScript)
- Record store upgraded: proper MVCC for concurrent reads/writes
- CSR snapshot refresh: background thread materializes every N seconds
- Index support: basic property indexes using B-tree
- LOC: ~40-55K
- Reality: a usable system for simple workloads. READ queries on CSR snapshots are dramatically fast (proven by Knight Bus). WRITE queries go through a conventional path. The gap is Cypher completeness — users will hit "unsupported Cypher feature" errors regularly

### Year 1

- Cypher coverage: ~70-80%
- Full Bolt protocol (v3/v4/v5)
- Record store with WAL and crash recovery
- CSR snapshots for all read-only workloads
- Algorithm acceleration: 3-5 Atlas layout families implemented (AnchorDualCsr, InboundPower, RelaxationFrontier)
- Property support in snapshots (PropertyPlane concept realized)
- LOC: ~120-160K
- Reality: a dual-engine system where reads are 10-100x faster on CSR snapshots and writes are "normal speed." The complexity cost is managing two storage paths and keeping them consistent

### Long-term Shape

A pragmatic system that ships earlier and delivers real value on reads. But it carries permanent complexity: two storage engines, a materialization pipeline, consistency between mutable and immutable views. Every new feature must work on both paths. This is the "good enough" path — it delivers Knight Bus's proven wins without throwing away the ability to do writes.

### Likelihood

- Completion by Year 1: **55%** (simpler architecture, reuses existing Knight Bus code)
- Measurable speedup: **10-50x on reads** (CSR path), **1x on writes** (conventional)
- User adoption: **Medium** (compelling for read-heavy workloads, which is most analytics)

### Stress Points

- Month 2: "The dual-engine consistency model is tricky — what happens when a user reads data that was just written but hasn't been snapshot-materialized yet?"
- Month 4: "Cypher coverage gaps are frustrating users"
- Month 8: "Every new feature needs to work on two storage paths"

### Inflection Points

- If read-heavy workloads dominate (likely in analytics/GDS use cases), this architecture wins big
- If write-heavy workloads dominate, the dual engine adds complexity without much benefit
- The materialization lag (time between write and CSR availability) is the critical UX parameter — if it's seconds, fine; if it's minutes, users will complain

---

## Timeline C: "The Knight Bus Doctrine"

*Frontend identical to Neo4j. Backend is radically different: immutable CSR snapshots as the primary storage model, with algorithm-specific layout families from the Atlas. Writes are handled through a truth-layer rebuild model (like a compiler), not through a mutable store.*

### Opening Move

Commit fully to the Knight Bus storage-runtime alignment doctrine: the graph is compiled from a truth source (CSV, Neo4j export, streaming ingestion log) into immutable, algorithm-optimized snapshots. There is no mutable record store. "Writes" are either:
1. Full rebuild from updated truth source (for batch workloads)
2. Append to an ingestion log + periodic recompile (for streaming workloads)

The backend uses `FormatSelectionProfile` to choose the right layout family per algorithm.

### Week 1

- Reuse Knight Bus's 4,710 LOC as the foundation
- Stub `FormatSelectionProfile`: algorithm → layout family router
- Begin Bolt protocol (same as Timeline B — fastest path to user connection)
- Design the truth-layer ingestion model: how does new data enter the system?

### Month 1

- Bolt v4 prototype: clients connect and query
- Minimal Cypher parser for read queries
- AnchorDualCsr fully working (already proven)
- InboundPowerLayoutV1 stubbed (PageRank is the poster child)
- Truth layer: `neo4j-admin export` → CSV → Knight Bus build pipeline → CSR snapshot
- LOC: ~10-15K (much less new code because Knight Bus exists)
- Reality: users can import a Neo4j database via CSV export, build a snapshot, and query it over Bolt. Reads are blazing fast. But: NO write support. The system is read-only. Users must re-import to update data
- **What you DON'T have:** Any write capability, complex Cypher, real-time updates

### Quarter 1

- Cypher coverage: ~35% (read-only subset: MATCH, WHERE, RETURN, ORDER BY, LIMIT)
- 3 Atlas families implemented: AnchorDualCsr, InboundPower, RelaxationFrontier
- PageRank benchmark shows 50-100x over Neo4j GDS (if the thesis holds)
- Dijkstra shortest path on RelaxationFrontier layout
- Ingestion log prototype: append-only event log + background recompile
- LOC: ~30-40K
- Reality: an incredible read-only analytics engine. Users export their Neo4j, build snapshots, and get dramatic speedups for traversals and algorithms. But the "no writes" limitation means it's a companion to Neo4j, not a replacement
- **The hard question surfaces:** Is this a Neo4j replacement or a Neo4j accelerator?

### Year 1

- Cypher read coverage: ~60-70%
- 5-7 Atlas families implemented (P0 + some P1)
- Streaming ingestion: real-time event log with sub-minute recompile for graphs < 10M edges
- Incremental snapshot updates (don't rebuild everything on small changes)
- Write support through ingestion log (CREATE, SET, DELETE translated to log entries)
- LOC: ~80-120K
- Reality: a radically different system. Reads are 10-100x faster. The architecture is clean and elegant. But the write model is unfamiliar to Neo4j users (append + recompile instead of immediate mutation). Some users love it (analytics, batch processing). Some hate it ("I just want to UPDATE a property")

### Long-term Shape

The most architecturally pure path. If the Knight Bus thesis holds beyond traversal (and the Atlas suggests it does for 60 algorithms), this becomes a genuinely new kind of graph engine — not "Neo4j in Rust" but "a graph compiler." The risk is that the write model alienates users who need real-time mutations. The opportunity is that it's 10-100x faster for the workloads that matter most (analytics, algorithms, batch queries).

### Likelihood

- Completion by Year 1: **65%** (less code to write, more focused scope)
- Measurable speedup: **10-100x on reads/algorithms** (the core thesis)
- User adoption: **Medium-High for analytics**, **Low for OLTP** (the write model is the barrier)

### Stress Points

- Month 1: "It's read-only. Is this really a Neo4j replacement?"
- Month 3: "The ingestion log model is unfamiliar to users. How do we explain 'your writes are compiled'?"
- Month 6: "Incremental recompile is harder than we thought for large graphs"

### Inflection Points

- If the team proves 50-100x on PageRank and Dijkstra (the P0 Atlas families), the project becomes a clear "graph analytics accelerator" with an expansion path toward general graph engine
- If the write model proves too limiting, there's pressure to add a mutable layer (which pushes toward Timeline B)
- If sub-second recompile is achievable for typical update batches, the "no writes" limitation becomes invisible

---

## Timeline D: "The Aggressive Divergence"

*Frontend: Neo4j-compatible Cypher and Bolt, but with intentional extensions. Backend: fully Knight Bus, algorithm-specific layouts, NO backward compat with Neo4j's storage format. Plus: a new query language layer on top of Cypher for Knight Bus-native operations.*

### Opening Move

Go all-in on the Knight Bus doctrine AND extend the frontend. Keep Cypher as the base language but add Knight Bus-native extensions:
- `COMPILE GRAPH FROM 'source.csv' USING InboundPowerLayout` — explicit layout control
- `WALK (n)-[:X*1..2]->(m) RETURN m` — a new `WALK` keyword that guarantees CSR fast-path
- `EXPLAIN FORMAT` — show which Atlas layout family a query would use

The backend uses all 13 Atlas families from the start. The product publishes bespoke algorithm contracts.

### Week 1

- Design the language extensions (grammar, semantics)
- Stub 5 Atlas layout families
- Begin Bolt protocol with custom extensions for layout-aware queries
- Write the "why this is better" pitch document

### Month 1

- Cypher + extensions parser
- 3 Atlas families implemented
- `COMPILE GRAPH` command working
- `WALK` keyword for CSR fast-path
- Bolt with custom message types for layout control
- LOC: ~15-20K
- Reality: exciting demos, but you've forked from Neo4j compatibility. Neo4j drivers need patches to support custom message types. Users can't just change a connection string

### Quarter 1

- 5-7 Atlas families
- Language extensions maturing
- Custom tooling (CLI, monitoring, layout inspector)
- LOC: ~40-55K
- Reality: a genuinely novel system, but it's NOT "flawlessly like Neo4j" anymore. Users need to learn new concepts (layout families, compile commands, walk semantics). The switching cost is no longer zero

### Year 1

- Full Atlas coverage (all 13 families)
- Rich language extensions
- Custom drivers (not Neo4j-compatible)
- LOC: ~100-140K
- Reality: a powerful, innovative graph engine that is distinctly NOT Neo4j. It's its own thing. Users who adopt it love it, but the audience is smaller because there's a learning curve

### Long-term Shape

The most innovative path. If it succeeds, it defines a new category ("graph compiler" vs "graph database"). But it violates the user's core constraint: "the usage interface SHOULD BE FLAWLESSLY LIKE NEO4J." This timeline is the highest-risk, highest-reward option.

### Likelihood

- Completion by Year 1: **50%** (new language design adds scope)
- Measurable speedup: **50-100x** (fully optimized backend)
- User adoption: **Low initially** (high switching cost), **potentially high long-term** (if the category catches on)

### Stress Points

- Month 1: "We're designing a new query language AND building an engine. That's two hard problems"
- Month 3: "Neo4j driver compatibility is broken. Users can't just switch"
- Month 6: "We're building a startup product, not a Neo4j replacement"

### Inflection Points

- If the language extensions prove genuinely useful (e.g., explicit layout control is a killer feature for data engineers), the project could attract a new audience beyond Neo4j users
- If the extensions feel like unnecessary complexity, the team will wish they'd stayed compatible

---

## Timeline E: "The Trojan Horse"

*Ship Timeline C first (read-only Knight Bus accelerator with Neo4j-compatible Cypher/Bolt). Once adopted, gradually expand into writes and full Neo4j replacement.*

### Opening Move

Position the product as a "Neo4j Read Accelerator" — not a replacement. Users keep Neo4j for writes and use KNRT for reads. The pitch: "Export your Neo4j, build a KNRT snapshot, get 100x faster analytics."

### Week 1

- Same as Timeline C Week 1
- Add: marketing positioning as "accelerator, not replacement"
- Add: `neo4j-export` integration tool that automates the CSV export → KNRT build pipeline

### Month 1

- Bolt v4 working (read-only queries)
- One-command import: `knrt import --from-neo4j bolt://localhost:7687`
- AnchorDualCsr working with proven tests
- LOC: ~12-16K
- Reality: users run KNRT alongside Neo4j. Neo4j handles writes, KNRT handles reads. The switching cost is near-zero because users don't replace anything — they add acceleration

### Quarter 1

- 3 Atlas families implemented
- Automated sync: KNRT watches Neo4j's transaction log and rebuilds snapshots incrementally
- Cypher read coverage: ~40-50%
- LOC: ~35-45K
- Reality: a compelling "sidecar" product. Analytics teams love it. But it's not a standalone system yet

### Year 1

- 5-7 Atlas families
- Cypher read coverage: ~70%
- Ingestion log for direct writes (users can bypass Neo4j for new data)
- Write-through proxy: KNRT accepts writes, forwards to Neo4j, rebuilds snapshot
- LOC: ~90-120K
- Reality: KNRT is gradually absorbing Neo4j's role. Users start doing more work directly in KNRT. The "accelerator" is becoming a "replacement" organically

### Long-term Shape

The lowest-risk path to adoption. Users never have to "switch" — they add KNRT alongside Neo4j, then gradually shift workload. By the time KNRT handles writes, users are already dependent on it for reads. The Trojan Horse strategy.

### Likelihood

- Ship v1 by Month 2: **80%** (smallest scope, most focused)
- Measurable speedup: **10-100x on reads** (same as Timeline C)
- User adoption: **Highest** (no switching cost, additive value)

### Stress Points

- Month 3: "Are we building a real product or just a Neo4j accessory?"
- Month 6: "Users want writes in KNRT. The sidecar model is limiting"
- Month 9: "We need to decide: stay a sidecar forever or commit to standalone?"

### Inflection Points

- If the sidecar model proves sufficient for analytics teams, the product could succeed without ever handling writes
- If users demand standalone operation, the transition from sidecar to full engine is the critical challenge
- The automated Neo4j sync is the key feature — if it works reliably, adoption is easy; if it's flaky, users won't trust it

---

## Cross-Timeline Analysis

| Path | Speed to first user value | Read speedup | Write support | Neo4j compat | LOC Year 1 | Architecture risk | Adoption risk |
|---|---|---|---|---|---|---|---|
| **A: Faithful Port** | ~6 months | 1.5-2x | Full (Month 4) | 95%+ | 180-250K | Low (proven arch) | High (why switch?) |
| **B: Conservative Hybrid** | ~1 month | 10-50x reads | Full (Month 3) | 80% | 120-160K | Medium (dual engine) | Medium |
| **C: Knight Bus Doctrine** | ~1 month reads | 10-100x reads | Append-recompile (Month 6) | 70% reads | 80-120K | Medium (new write model) | Medium-High for analytics |
| **D: Aggressive Divergence** | ~2 months | 50-100x | Append-recompile | 60% (extensions break compat) | 100-140K | High (new language) | High (switching cost) |
| **E: Trojan Horse** | ~2 weeks (sidecar) | 10-100x reads | None initially, grows | 90%+ reads | 90-120K | Low (additive) | **Lowest** |

### Upside / Downside / Reversibility

| Path | Best case | Worst case | Reversibility | Regret risk |
|---|---|---|---|---|
| **A: Faithful Port** | "Neo4j but in Rust" — solid, trustworthy | 2 years of work for 2x speedup nobody cares about | Low — you've committed to Neo4j's architecture | **High** — if Neo4j's architecture is the problem, you've just rebuilt the problem |
| **B: Conservative Hybrid** | Best of both worlds — fast reads, normal writes | Permanent complexity tax of dual engines | Medium — can drop CSR path or drop mutable path | Medium — complexity may be manageable |
| **C: Knight Bus Doctrine** | A genuinely new, 100x faster graph analytics engine | "No writes" alienates OLTP users | Medium — can add mutable layer later (becomes B) | Low — the read-only core is always valuable |
| **D: Aggressive Divergence** | Defines a new category; "graph compiler" | Built a product nobody asked for; lost Neo4j compat | Low — language extensions are hard to walk back | **High** — violates the user's core constraint |
| **E: Trojan Horse** | Zero-risk adoption, gradual expansion, users love it | Stuck as a "Neo4j accessory" forever | **High** — can always expand scope later | **Lowest** — the read accelerator is always useful |

### Who/What Has to Cooperate

| Path | Dependencies |
|---|---|
| **A** | Deep Neo4j internals knowledge; tolerance for 6+ months of no user value |
| **B** | Dual-engine consistency model; users tolerating Cypher coverage gaps |
| **C** | Users accepting append-recompile write model; sub-minute recompile for <10M edges |
| **D** | Users willing to learn a new language; ecosystem willing to adopt non-Neo4j drivers |
| **E** | Neo4j staying stable enough to sync against; users trusting a sidecar for analytics |

---

## Decision Filter

### Which path is strongest if everything goes normally?

**Timeline E (Trojan Horse)**, then **Timeline C (Knight Bus Doctrine)**.

Timeline E ships fastest, has the lowest adoption barrier, and preserves the option to expand into any other timeline. If the sidecar proves its value, it can grow into C (add writes via append-recompile) or B (add mutable store). It never requires users to "switch" — they add.

Timeline C is the purest expression of the Knight Bus thesis and the most architecturally elegant. If the team is confident in the storage-runtime alignment doctrine and wants to build a new kind of engine, C is the intellectually honest path.

### Which path is safest if things go badly?

**Timeline E (Trojan Horse)**.

If the project fails or loses momentum:
- Timeline E's v1 (read-only sidecar) is still useful — it's a fast analytics cache for Neo4j
- No user has abandoned Neo4j, so they haven't lost anything
- The codebase is the smallest (90-120K LOC) and the most focused
- Every feature built for E is reusable in B or C

Timeline A is the **least safe** — if it fails, you have a half-built Neo4j clone that nobody can use.

### Which path should NOT be pursued?

**Timeline D (Aggressive Divergence)** violates the user's explicitly stated constraint: "the usage interface SHOULD BE FLAWLESSLY LIKE NEO4J." Building custom language extensions and non-standard Bolt messages abandons this constraint. It's included for completeness but should be rejected unless the constraint changes.

**Timeline A (Faithful Port)** wastes Knight Bus's proven advantage. You already have evidence that CSR snapshots are dramatically faster than Neo4j's record store for traversal. Rebuilding Neo4j's record store throws that away.

### What experiment would reduce uncertainty fastest?

**Build the Timeline E v1 in 2 weeks:**

1. `knrt import --from-neo4j-csv export/` → builds AnchorDualCsr snapshot
2. `knrt serve --bolt 7688` → Bolt v4 read-only server
3. Support 5 Cypher patterns: `MATCH (n) RETURN n`, `MATCH (n {id:$id})`, `MATCH (n)-[:R]->(m)`, `MATCH (n)-[:R*1..2]->(m)`, `RETURN count(*)`
4. Benchmark: same query on Neo4j vs KNRT, measure latency

This experiment proves or disproves:
- Can Bolt be implemented quickly enough? (Estimates say 10-15K LOC — is that right?)
- Is the Cypher subset useful enough for a v1?
- Does the 10-100x read speedup hold when going through Bolt instead of direct Rust calls?
- Will Neo4j users actually try a sidecar?

If this 2-week experiment succeeds, the path forward is clear: ship E, expand toward C. If it fails (Bolt is harder than expected, Cypher subset is useless, speedup disappears over Bolt), the team knows before investing months.

---

## Recommended Path

### Primary: Timeline E → Timeline C

```text
Month 1-2:     Ship Timeline E v1 (read-only Neo4j sidecar)
Month 3-6:     Expand Cypher coverage, add Atlas families (moving toward C)
Month 6-9:     Add ingestion log for writes (entering Timeline C territory)
Month 9-12:    Incremental recompile, streaming sync, standalone operation
Year 1+:       Full Neo4j replacement with read-optimized architecture
```

### Fallback: Timeline E → Timeline B

If the append-recompile write model proves too limiting (users demand immediate writes), pivot from C to B:

```text
Month 6:       Recognize that analytics-only isn't enough
Month 6-9:     Add lightweight mutable record store alongside CSR snapshots
Month 9-12:    Dual-engine mode (mutable for writes, CSR for reads)
```

### What NOT to do

- Do NOT start with Timeline A. It wastes the proven Knight Bus advantage.
- Do NOT pursue Timeline D. It violates the Neo4j compatibility constraint.
- Do NOT build the full query planner before shipping a useful v1. A rule-based planner for simple patterns is enough for the first 3 months.

---

## Connection to Current State

### What we have today (TDD-verified)

| Asset | Status |
|---|---|
| AnchorDualCsr snapshot engine | **PROVEN** (4,710 LOC, 23 tests) |
| `WalkQueryRuntime` trait | **PROVEN** (exact-key, 1-2 hop) |
| `SnapshotArtifactWriter` trait | **PROVEN** (CSV → binary) |
| `TruthGraphSource` trait | **PROVEN** (extensible input) |
| Low-RAM build | **PROVEN** (external merge sort) |
| Parity verification | **PROVEN** (snapshot vs truth) |
| Benchmark suite | **PROVEN** (p50/p95/p99, RSS) |

### What every timeline needs first

| Need | Timeline A | Timeline B | Timeline C | Timeline D | Timeline E |
|---|---|---|---|---|---|
| Bolt protocol | Month 4 | Month 1 | Month 1 | Month 1 | **Month 1** |
| Basic Cypher parser | Month 6 | Month 1 | Month 1 | Month 1 | **Month 1** |
| Neo4j CSV import | Month 8 | Month 1 | Month 1 | Month 1 | **Month 1** |
| Mutable storage | Month 3 | Month 3 | Month 6+ | Month 6+ | **Never (initially)** |
| Query planner | Month 9 | Month 4 | Month 4 | Month 2 | **Month 3** |

### The honest gap from the Rubber Duck scorecard

8 PROVEN, 3 PARTIALLY, 9 UNPROVEN claims. Timeline E is the only path that ships before any of the 9 UNPROVEN claims need to be resolved. It lets you prove them incrementally, in public, with users giving feedback.
