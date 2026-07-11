# PMF Analysis of the Graph-Systems Space — Through a Shreyas Doshi Lens

> Companion to the technical corpus (`pattern-index.md`, 28 patterns, 8 category
> syntheses, 172 repos). The corpus answered *how these systems work*; this
> document asks the Shreyas Doshi question: *do they solve a top-3 customer
> problem, and at what level of product-market fit?*
>
> **Sourcing note.** All market claims below come from internet research
> (2024–2026 press, analyst posts, and reporting) — not from the corpus source
> code — and should be independently verified before being used for decisions.
> Corpus-grounded claims cite pattern numbers.

---

## 0. The frameworks applied

Doshi's tools used here (from his Stripe/Google/Twitter product work and
public writing):

1. **Customer Problem Stack Rank (CPSR)** — a product only earns durable PMF
   if the problem it solves is in the customer's *top three* business
   priorities; solving problem #7 gets you "great product, but not now"
   forever. There is no universal PMF template — fit is context-specific.
2. **Three Levels of Product Work** — *impact* work (moves the business),
   *execution* work (ships correctly), *optics* work (looks good). Categories
   below are scored on whether their center of gravity is impact or optics.
3. **PMF is not binary** — it progresses through levels (nascent → developing
   → strong → extreme, per the First Round articulation Doshi's circle uses):
   graded here per category.
4. **Pre-mortem** — for each category: "it's 2028 and this category failed —
   why?"
5. **Opportunity-cost thinking (not ROI thinking)** — the question is never
   "does this return more than it costs?" but "is this the best use of the
   same engineering hours?"

---

## 1. Category-by-category PMF verdicts

### 1.1 Vector databases (corpus: vector-ann, patterns 13–16)

- **CPSR verdict: was top-1 in 2023, fell to ~#5 by 2025.** During the RAG
  gold rush, "store embeddings, retrieve neighbors" was every AI team's top
  problem. Then incumbents made it a *feature*: pgvector in Postgres,
  Elasticsearch/OpenSearch dense vectors, Neo4j native vector search — and
  the standalone category commoditized.
- **Market evidence:** vector DB market ≈ $2.55B (2025), 24–28% CAGR — but
  Pinecone, the category's poster child, was reported (VentureBeat, Nov 2025)
  to be exploring a sale amid churn, with revenue reportedly *declining*
  (~$26.6M 2024 → ~$14M 2025 per one analysis); CEO change Sept 2025.
  Meanwhile Milvus/Qdrant run production at Salesforce, PayPal, Discord —
  but as open source, monetization lags adoption.
- **PMF level: strong → regressing.** Extreme adoption of the *capability*,
  weak fit for the *standalone product*. Classic Doshi trap: the customer's
  real top-3 problem was "make my AI app accurate," not "operate another
  database" — so anything that solved accuracy *inside the existing stack*
  won.
- **Pre-mortem (already happened):** "we differentiated on ANN recall curves
  (pattern 16's Pareto frontier) while the buyer's decision criterion was
  'is it already in my Postgres?'" — an *execution*-level moat sold as an
  *impact*-level product.
- **Corpus tie-in:** patterns 13–15 show why the moat was thin — HNSW/IVF/PQ
  are ~3 well-published ideas (greedy descent, partition, quantize) that any
  competent team embeds in a quarter; the corpus itself contains five
  independent HNSW implementations.

### 1.2 Graph databases (corpus: graph-db, patterns 20–22; neo4j-ecosystem, 23–24)

- **CPSR verdict: top-3 only inside specific verticals** — fraud rings,
  supply-chain traceability, identity resolution, network topology — where
  the *relationship* is the data. Outside those, "graph" is a nice-to-have
  modeling preference, and Postgres-with-joins wins the stack rank.
- **Market evidence:** graph DB market ≈ $2.85B (2025), similar CAGR to
  vectors. Neo4j passed $200M ARR (Nov 2024), doubling in 3 years, used by
  84% of Fortune 100; announced a $100M GenAI/agentic push (Oct 2025)
  repositioning as "the knowledge layer for agents." Counter-signal: Kuzu —
  the corpus's best-engineered embedded graph engine (patterns 20–21) — was
  abandoned Oct 2025 (repo archived; reporting says the team was
  acqui-hired by Apple), leaving community forks (LadybugDB, bighorn).
- **PMF level: strong at the top, nascent below.** One durable winner
  (Neo4j) with genuine enterprise fit; everyone else fights for the
  remainder. Kuzu is the sharpest lesson: *exceptional execution-level work
  (factorized processing, columnar CSR) did not convert to a business*,
  because embedded-analytical-graph was nobody's top-3 problem at
  paying-customer scale.
- **GraphRAG twist:** 2025–26 reporting shows knowledge-graph-powered
  retrieval gaining on pure vector RAG for accuracy — the first time in a
  decade the graph category's pitch ("relationships carry meaning") aligns
  with a genuinely top-3 buyer problem (AI answer quality). This is
  Neo4j's $100M bet.
- **Pre-mortem 2028:** "GraphRAG turned out to be a feature too — the LLM
  vendors built graph memory into their platforms, and graph DBs stayed a
  vertical niche."

### 1.3 Full-text search (corpus: full-text-search, patterns 17–19)

- **CPSR verdict: permanently top-3 — but the problem is owned.** Search over
  logs, products, and documents never leaves the priority list; the fit
  question was settled a decade ago by Lucene's descendants.
- **Market evidence:** Elasticsearch + OpenSearch dominate general-purpose
  enterprise search; OpenSearch hit 1.4B downloads under Linux Foundation
  governance (2026), doubling in ~2 years; Elastic was a 2025 Forrester
  Cognitive-Search leader after absorbing the vector wave (dense vectors,
  ELSER, RRF hybrid fusion). Algolia holds a defended e-commerce niche;
  Tantivy/Quickwit take the Rust/infra tail.
- **PMF level: extreme — and instructive.** FTS shows what surviving a hype
  wave looks like: a decade of production hardening (pattern 17's segment
  machinery, 18's WAND, 19's FSTs) meant *adding* vectors was easy for
  Elastic, while vector-native startups adding BM25/filters/aggregations
  found the reverse direction brutal. Moats live in the boring layers
  (L1/L2 of the capstone), not the demo-able ones (L4).
- **Pre-mortem 2028:** "LLMs collapsed search into 'ask the model' and
  ripped out result-list UX" — plausible for consumer, unlikely for the
  machine-scale log/observability workloads that pay the bills.

### 1.4 Storage engines (corpus: storage-engine, patterns 1–6)

- **CPSR verdict: never the customer's problem — always the *vendor's*.**
  Nobody stack-ranks "I need an LSM tree"; they rank latency, cost,
  durability. Storage engines have PMF only as *components*: RocksDB is
  embedded in half the corpus (and industry), yet is a cost center at Meta,
  not a product.
- **PMF level: extreme as infrastructure, nil as product.** The winners
  (RocksDB, LMDB, SQLite) are free, and the companies that tried to sell
  engines directly mostly became acquisitions or pivots. Doshi's
  opportunity-cost lens: building a new general-purpose engine is almost
  never the best use of hours — building a *specialized* one (pattern 15's
  DiskANN sector layout; this repo's mmap snapshots) for a workload the
  general engine serves badly, sometimes is.

### 1.5 Dataflow / incremental compute (corpus: dataflow-compute, patterns 25–26)

- **CPSR verdict: top-10 problem chronically mistaken for top-3.** "My
  dashboards/features should be fresh" is real but rarely urgent enough to
  displace batch. Differential dataflow (pattern 25) is the most
  intellectually profound machinery in the whole corpus — and Materialize,
  its commercialization (>$100M raised, McSherry as chief scientist), spent
  years repositioning (streaming SQL DB → operational data warehouse → live
  data layer for AI agents) in search of the segment that ranks freshness
  top-3.
- **PMF level: developing, after a long nascent phase.** The 2025-26 agent
  wave may finally supply the missing urgency (agents need fresh context;
  pattern 25's arrangements are exactly "pay once, answer forever").
- **Pre-mortem 2028:** "micro-batch (Spark/Flink every 30s) stayed good
  enough for 95% of buyers, and the elegant-incremental premium never
  cleared its complexity cost" — the corpus's own COST-paper skepticism
  (graph-analytics synthesis) applied to the market.

### 1.6 Graph analytics & bench-testing (corpus: patterns 7–12, 27–28)

- **Graph analytics: PMF as a feature, not a company.** Ligra/GBBS-grade
  single-machine execution (patterns 7–8) beat clusters per COST, but the
  buyer gets it via GDS inside Neo4j or NetworkX inside notebooks — the
  standalone "graph compute engine" category (GraphScope et al.) remains
  research-adjacent.
- **Bench-testing: negative-space PMF.** SQLancer, Jepsen, Graphalytics
  (patterns 27–28) have enormous *industry impact* (hundreds of real bugs)
  and almost no market — verification is a public good the market
  under-prices. Jepsen survives as one consultant's practice. For this
  repo, that's an *asymmetry to exploit*: the convergence thesis's fitness
  function is built from assets nobody will ever charge for.

---

## 2. The cross-category Doshi synthesis

```text
stack-rank position of the problem  ->  observed PMF outcome
"the data IS relationships" verticals   Neo4j: strong PMF, $200M ARR
"search must work" (permanent top-3)    Lucene family: extreme PMF
"AI answers must be accurate" (new #1)  the current battleground: GraphRAG
                                        vs pgvector vs Elastic hybrid
"another database to operate" (anti-    Pinecone regression; Kuzu
 problem: buyers rank it negatively)    abandonment; engine commoditization
"freshness would be nice" (top-10)      Materialize's decade of pivots
"correctness verification" (unpriced)   Jepsen/SQLancer: impact, no market
```

Three laws fall out:

1. **Capabilities commoditize toward the platform the buyer already runs.**
   Vectors → Postgres/Elastic; graph algorithms → GDS; storage → RocksDB.
   A standalone engine survives only where integration is *impossible*, not
   merely inconvenient.
2. **The moat is the boring 80%** — the capstone's L1/L2 (durability, layout)
   plus enterprise scar tissue — never the L4 demo layer that launches the
   hype cycle. Elastic absorbing vectors while Pinecone couldn't absorb
   search is the cleanest proof.
3. **Execution-level excellence is not impact-level fit.** Kuzu had the best
   code in the corpus and no business; Neo4j has 15-year-old record formats
   (pattern 20) and 84% of the Fortune 100. Doshi's three-levels framework,
   verified at category scale.

---

## 3. What this means for *this* repo (the knight-bus thesis)

Applying the same lens to the mission (low-RAM GDS-class analytics +
Neo4j-endpoint rewrite):

- **CPSR check:** "run 50GB graphs on 8GB machines" is a top-3 problem only
  for buyers already locked into graph workloads with cloud-bill pain — a
  real but narrow segment. "Neo4j-compatible at a fraction of the cost" is
  the stronger stack-rank claim, because it attacks a line item, not a
  preference (the ScyllaDB-vs-Cassandra playbook).
- **The Kuzu warning applies directly:** superb embedded graph engineering,
  no distribution wedge, abandoned. Compatibility with an installed base
  (the corpus's neo4j-ecosystem patterns 23–24: Bolt, PackStream, TCK,
  testkit) *is* the distribution wedge Kuzu never had.
- **The bench-testing asymmetry is the edge:** verification assets are free
  (pattern 27–28), unpriced by the market, and — per the convergence thesis
  — the durable artifact of an AI-era rewrite. Opportunity-cost thinking
  says the harness, not another storage innovation, is the highest-leverage
  next 1,000 engineering hours.
- **Pre-mortem for the rewrite, 2028:** "we achieved parity on the 80%
  observable surface, but enterprises wouldn't migrate without the
  unobservable 20% (crash/concurrency behavior) being provably equivalent —
  and we never built the history-model harness." The failure mode is named
  in advance; pattern 27's Jepsen machinery is the prevention.

---

## 4. Citations and further reading (external; verify independently)

- Doshi frameworks: Lenny's Podcast ep. 3 (pre-mortems, LNO, three levels);
  YourStory interview on customer problem stack rank; First Round "Levels
  of PMF".
- Vector shakeout: VentureBeat "From shiny object to sober reality" (Nov
  2025); The AI Engineer "What is Pinecone?" (2026).
- Graph market: Neo4j $200M ARR press release (Nov 2024); Neo4j $100M
  GenAI investment (Oct 2025); The Register on KuzuDB abandonment (Oct
  2025); gdotv on the Kuzu legacy / Apple acqui-hire reporting (2026).
- Search: Techzine on OpenSearch 1.4B downloads (2026); Pureinsights
  "From Vector Hype to Hybrid Reality" (2026).
- Dataflow: Materialize funding and positioning pages.
- Corpus internals: `pattern-index.md`; capstone synthesis
  (`corpus-capstone-pattern-synthesis-ascii.md`) for the five-layer/seven-idea
  model referenced throughout.
