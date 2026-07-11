# PMF Analysis of the Apache Ecosystem — Through a Shreyas Doshi Lens

> Variant of `pmf-analysis-shreyas-doshi-lens.md`, applying the same frameworks
> (customer problem stack rank, three levels of product work, PMF-as-levels,
> pre-mortems, opportunity-cost thinking) to Apache Software Foundation
> projects — the ecosystem that supplies a large slice of the 172-repo corpus
> (Lucene, Spark, Flink, Giraph, AGE, TinkerPop, HugeGraph, Jena, Solr,
> OpenSearch-lineage…).
>
> **Sourcing note.** Market and governance claims come from internet research
> (ASF Attic records, SEC filings, press, 2024–2026) — not from corpus source
> code — and should be independently verified. Corpus-grounded claims cite
> pattern numbers.

---

## 0. Why Apache needs its own PMF analysis

The ASF is a *governance* body, not a business — so "PMF" splits into two
distinct questions the first document didn't have to separate:

1. **Adoption fit**: does the project solve a top-3 problem for *users*?
2. **Sustainer fit**: does maintaining it solve a top-3 problem for the
   *companies that pay the committers*?

Apache's unique instrument is that failure is **public and dated**: the
Attic. Retirement resolutions ("terminated due to inactivity") are the
closest thing infrastructure software has to an official PMF death
certificate. 2025 alone retired twelve TLPs — Mesos, Oozie, Gora, Griffin,
jclouds, Kibble among them — most of them former big-data darlings. The
Attic list is a longitudinal PMF dataset no vendor ecosystem publishes.

---

## 1. Project-by-project verdicts

### 1.1 Lucene (corpus: full-text-search, patterns 17–19)

- **CPSR: permanent top-3, solved once, embedded everywhere.** Lucene is the
  purest case of *component PMF* in the study: it never sells anything, yet
  Elasticsearch, OpenSearch, and Solr — the systems that dominate enterprise
  search — are all Lucene inside. The corpus's segment/postings/FST patterns
  (17–19) are Lucene's inventions industrialized.
- **The governance twist:** the value Lucene created was captured by a
  *vendor* (Elastic) and then re-contested via license drama — Elastic's
  2021 relicense triggered the AWS-led OpenSearch fork, which under Linux
  Foundation governance hit 1.4B downloads by 2026. Doshi lens: the *problem*
  (search) had extreme fit; the *fight* was over which entity's top-3 business
  priority (Elastic's monetization vs AWS's cloud consumption) got served.
- **PMF level: extreme, as infrastructure.** Pre-mortem is nearly
  unwriteable — Lucene dies only if search itself is absorbed by models, and
  even then the log/observability workloads remain.

### 1.2 Kafka / Spark / Flink (corpus: dataflow-compute, patterns 25–26)

- **The three Apache success stories share one shape:** a top-3 problem
  (move data / batch compute / stream compute), an open-source engine with
  extreme adoption fit, and a *single anchor vendor* converting it to
  sustainer fit — Confluent for Kafka ($1.12B FY2025 subscription revenue,
  +21% YoY, per SEC filing), Databricks for Spark (private; reported
  multi-billion run-rate), Ververica→Alibaba then Confluent for Flink
  (Confluent's managed Flink ~$10M ARR and tripling, 2025).
- **Doshi reading:** the ASF supplies the *execution level* (neutral
  governance keeps competitors contributing); the vendor supplies the
  *impact level* (packaging the engine as the answer to a stack-ranked
  problem: "our data platform," not "a JVM framework"). Neither works alone
  — which is precisely why equally well-engineered projects without an
  anchor vendor fell to the Attic.
- **PMF level: extreme (Kafka, Spark), strong (Flink).** Flink's fit is
  gated by the same freshness stack-rank problem as Materialize (first
  document, §1.5): streaming-first is top-3 for fewer buyers than
  micro-batch-is-fine.
- **Corpus tie-in:** pattern 26 (superstep message convergence) documents
  GraphX's Pregel-on-Spark; the market lesson is that GraphX survives as a
  *free rider on Spark's PMF*, not on its own.

### 1.3 Giraph — the Attic as PMF instrument (corpus: pattern 26)

- **Retired Sept 2023, "due to inactivity."** Giraph is the corpus's
  clearest natural experiment: Pregel-class BSP graph processing (pattern
  26) at genuine Facebook scale — a trillion edges — and yet: one dominant
  sustainer (Meta), whose board reports openly tracked contribution decline
  ("Meta has been the main contributor… activity has slowed") until
  termination.
- **Doshi reading:** Giraph had *user* adoption fit at exactly one company,
  which is sustainer fit by accident, not by market. When Meta's internal
  priorities shifted, PMF evaporated — because it was never market fit, it
  was *one customer's problem stack, externalized*. Contrast Kafka: born at
  LinkedIn, but Confluent converted it into a thousand companies' top-3.
- **The COST paper coda (graph-analytics synthesis):** single-machine
  engines beat Giraph-class clusters for most real graphs, so the problem
  Giraph solved ("BSP at cluster scale") was quietly demoted on nearly
  everyone's stack rank while the code was still being polished. Execution
  excellence, again, ≠ impact.

### 1.4 TinkerPop / Gremlin — standard without a market

- **CPSR: solves the *vendor's* #7 problem, no user's #1.** TinkerPop gave
  every graph store a common traversal language (Gremlin) — adopted by
  JanusGraph, Neptune, Cosmos DB, HugeGraph (all in the corpus/proprietary
  ledgers). But interoperability is rarely a buyer's top-3 problem at
  purchase time; query-language lock-in is a *post-purchase regret*, and
  regret doesn't fund committers.
- **Outcome:** persistent but thin — the anchor implementations
  (JanusGraph) drift; the momentum moved to openCypher→GQL (ISO standard,
  2024), which had what Gremlin lacked: an anchor vendor (Neo4j) whose
  top-3 priority *was* the standard's success. Standards inherit the PMF
  of their sponsors.

### 1.5 Apache AGE and HugeGraph — graph inside vs graph beside Postgres

- **AGE (openCypher inside PostgreSQL):** the *architecturally correct*
  answer to the first document's law 1 ("capabilities commoditize toward
  the platform the buyer already runs") — graph as a Postgres extension,
  exactly the pgvector playbook. Active (v1.7 for PG17, 100 contributors,
  releases through 2026) but adoption remains modest (~4.6k stars) —
  because pgvector rode an existing top-1 problem (RAG accuracy), while
  AGE still has to *create* the "I need Cypher in my Postgres" moment.
  Right strategy, waiting on the wave; GraphRAG may be it.
- **HugeGraph (graduated to TLP, Feb 2026):** the China-ecosystem
  counterpoint — Baidu-sustained, Lalamove and others in production,
  TinkerPop-compatible, now marketing "graph + AI" positioning. Its
  graduation memo explicitly argued adoption evidence over "a fancy user
  wall" — an ASF culture artifact: the foundation *audits* sustainer fit
  at graduation the way a VC audits PMF at Series B.

### 1.6 Arrow / DataFusion — the new Apache playbook (corpus: velox kin)

- **The most instructive *current* story:** DataFusion (Rust query engine,
  TLP 2024) is accruing PMF the component way — InfluxDB 3 rebuilt on it,
  Spice.ai, LakeSail's Spark-compatible Sail, and a predicted "1,000
  DataFusion systems"; it topped ClickBench on Parquet in late 2024, the
  first Rust engine to do so.
- **Doshi reading:** DataFusion never asks to be on anyone's stack rank —
  it rides *its embedders'* stack ranks, the Lucene/RocksDB pattern
  reborn in Rust. Sustainer fit is diversified by design (94+ contributors
  per release across many companies), the structural fix for the
  Giraph single-sustainer failure mode.
- **Direct relevance to this repo:** Sail's "Spark-compatible semantics on
  a Rust engine" is a live, funded instance of the known-endpoint rewrite
  thesis — an existing API surface treated as the executable spec, exactly
  the convergence loop of `Rewrite-Sampling-And-Convergence-Thesis.md`.

---

## 2. The Apache synthesis — four laws on top of the first document's three

1. **Foundation governance is execution-level machinery; PMF is set
   elsewhere.** The ASF can keep code neutral, licensed, and releasable —
   it cannot put a problem into a buyer's top three. Every extreme-PMF
   Apache project pairs neutral governance with a vendor whose *business*
   depends on the project winning (Kafka/Confluent, Spark/Databricks,
   Lucene/Elastic). Every Attic graph project lacked that pairing.
2. **Single-sustainer fit is deferred abandonment.** Giraph (Meta), and
   before it Mesos (Twitter-era) — when the sustainer's internal stack
   rank shifts, the project dies regardless of technical quality. The
   corpus's Kuzu lesson (first document §1.2) is the startup mirror of the
   same law.
3. **Component PMF outlives product PMF.** Lucene has outlived multiple
   generations of search *products* built on it; Arrow/DataFusion are
   engineered from day one for embedding. In Doshi terms: a component's
   customer is a *developer* whose problem stack changes slowly; a
   product's customer is a *buyer* whose stack re-ranks every budget cycle.
4. **The Attic is a free pre-mortem library.** Before building in any
   category, read its Attic entries: graph-on-Hadoop (Giraph, Hama),
   Hadoop workflow (Oozie), Hadoop ORM (Gora), big-data quality (Griffin)
   — the pattern is "born on a platform whose own PMF decayed." Platform
   riders inherit platform mortality.

```text
Apache project   anchor sustainer      PMF outcome
Kafka            Confluent ($1.1B)     extreme
Spark            Databricks            extreme
Lucene           Elastic/AWS (contested) extreme (component)
Flink            Confluent/Alibaba     strong, freshness-gated
DataFusion       diversified embedders rising (component playbook)
HugeGraph        Baidu                 developing (watch law 2)
AGE              Bitnine + community   nascent, well-positioned
TinkerPop        none durable          thin, standard w/o sponsor
Giraph           Meta (single)         Attic 2023
Mesos/Oozie/Gora (platform decayed)    Attic 2025
```

---

## 3. What the Apache lens adds for *this* repo

- **The rewrite thesis has an Apache-world proof-in-progress:** LakeSail's
  Sail (Spark API on Rust/DataFusion) is executing "known endpoint as
  executable spec" commercially. Watching its parity strategy — which
  Spark surfaces it treats as contract vs accident — is free R&D for the
  Neo4j-endpoint program.
- **Component strategy beats product strategy for a small team:** the
  DataFusion/Lucene law suggests the mmap graph runtime is more durable as
  an *embeddable engine* (the thing others build products on) than as a
  standalone database competing for a buyer's stack rank — while the
  Neo4j-compatible surface (patterns 23–24) remains the distribution wedge
  for those who *do* buy databases.
- **Sustainer-fit check is a real risk register item:** a single-sponsor
  open-source release of this engine would sit on the Giraph/Kuzu failure
  line. Either diversify contributors early (DataFusion model) or stay
  proprietary until the wedge holds.
- **Pre-mortem, Apache edition, 2028:** "we open-sourced the engine, one
  company's priorities carried it, those priorities changed" — the Attic
  has a shelf waiting; law 2 names the prevention.

---

## 4. Citations and further reading (external; verify independently)

- ASF Attic: retired-projects list and 2023–2025 tracking (Giraph ATTIC-217,
  Mesos ATTIC-245, Oozie ATTIC-232, Gora ATTIC-236, Griffin ATTIC-246);
  Giraph board minutes ("Meta has been the main contributor… activity has
  slowed").
- Kafka/Flink economics: Confluent FY2025 results (SEC 8-K exhibit,
  $1,119.7M subscription revenue); diginomica Q2-2025 coverage (Flink ~$10M
  ARR, ~3x in two quarters).
- Search governance: Techzine on OpenSearch 1.4B downloads under Linux
  Foundation (2026); Pureinsights on Elastic's hybrid-search position (2026).
- Graph projects: ASF announcement of HugeGraph TLP graduation (Feb 2026)
  and apache/hugegraph graduation evidence issue #2852; apache/age release
  history (v1.7 for PG17, 2026).
- DataFusion: InfluxData "2025: The Year of 1,000 DataFusion-Based Systems";
  Spice.ai and LakeSail engineering posts (Sail's Spark-compatible layer).
- Companion: `pmf-analysis-shreyas-doshi-lens.md` (the three base laws);
  corpus internals via `pattern-index.md` and the capstone synthesis.
