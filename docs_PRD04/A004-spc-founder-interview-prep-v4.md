# SPC Founder Interview Prep V4

Created: 2026-08-01  
Primary objective: prepare Amul Badjatya for the South Park Commons Founder Fellowship interview around Knight Walker / Knight Bus Graph Walker.  
Variant basis: `A000-spc-founder-interview-prep.md`, `A001-spc-submission-source-draft.md`, `A002-spc-founder-interview-prep-v2.md`, `A003-spc-founder-interview-prep-v3.md`, and the pasted `Graph Adoption Wave Thesis` note.  
External URL handling: URLs below are included for interview prep and source recall. Verify live status independently before repeating strong market claims to investors.

## What v4 adds

V3 had the right category correction:

> Graph compute grows, but graph databases may not become the next Oracle-style category.

V4 makes that correction evidence-backed and more memorable:

> The right analogy is not RDBMS in 1985. It is OLAP around 2019: the second act happens when the useful compute pattern becomes embedded, zero-ceremony, and cheap enough to run locally or inside existing workflows.

The v4 thesis:

> Graph computation may be approaching its DuckDB moment. Graph databases already had a hype wave; what has not happened yet is zero-ceremony graph execution: run real graph algorithm families on the machine or runtime you already have, with deterministic paths, predictable cost, and no memory roulette.

The v4 phrase to remember:

> Not the Oracle of graphs. The DuckDB of graph computation — but with quote-before-run as the commercial wedge.

## The one answer this interview must leave behind

Knight Walker is worth SPC's attention because graph compute has a real new demand curve, but the winning delivery model is likely embedded execution rather than a standalone graph database server.

The concise answer:

```text
I am not betting that graph databases become the next RDBMS category. I am betting that graph computation becomes routine inside agent memory, GraphRAG, provenance, security, code intelligence, fraud, and dependency workflows.

The missing layer is zero-ceremony execution: graph algorithms that can run without server setup, cluster sizing, or memory roulette.

Knight Walker is my Rust POC toward that layer. The current proof is scoped: same answers as Neo4j on the tracked walk benchmark, with 4.5x lower runtime RSS on the tracked 2GB walk path. The deeper product direction is quote-before-run: read a compact snapshot manifest, estimate the job, then admit, spill, or reject before execution.

Deterministic paths, deterministic bills.
```

## The v4 opening 60 seconds

Use this if they ask, "Tell us what you're building."

```text
Graph databases already had their hype cycle and stalled. So I am not pitching "graphs are the next RDBMS."

What I do believe is that graph computation is approaching its DuckDB moment. The algorithms have been useful for decades, but running them still often means server ceremony, cluster sizing, and memory roulette.

Two things changed. First, agents are becoming query authors, and machines have no loyalty to tables; they use the structure that fits the question. Second, AI memory and audit workflows need deterministic paths. Embeddings retrieve by similarity, but they cannot produce a replayable audit trail. In agent systems, the path is the proof.

Knight Walker is my Rust POC toward zero-ceremony graph execution. Today the proof is scoped: same answers as Neo4j on the tracked walk benchmark, with 4.5x lower runtime RSS on the tracked 2GB walk path. The deeper direction is quote-before-run: price the graph job from a compact manifest, then admit, spill, or reject before it fails late.

The company wedge is deterministic paths and deterministic bills.
```

## The 20-second version

```text
I am building toward the DuckDB-shaped wave for graph computation: embedded, zero-ceremony graph execution for agent memory, provenance, security, and code workflows — with quote-before-run so graph jobs become deterministic in both path and bill.
```

## The sentence to say instead of the RDBMS analogy

```text
Graph databases already had their hype cycle; what has not happened yet is graph computation's DuckDB moment. The algorithms have been right for decades, but running them still means server ceremony, cluster sizing, and memory roulette. LLMs are making graph construction cheaper, and agent/audit workloads are making deterministic paths valuable in a way similarity scores cannot satisfy. Knight Walker is the zero-ceremony execution layer for that wave: traversal that is not just cheaper, but priceable, auditable, and rejectable before execution.
```

## Evidence-backed category thesis

### 1. The cautionary record says not to pitch another graph database wave

The graph database category has already had many "mainstream now" moments.

Useful URLs:

- ZDNet / Linked Data Orchestration, 2018: `The year of the graph is here`  
  <https://www.zdnet.com/article/the-year-of-the-graph-getting-graphic-going-native-reshaping-the-landscape/>
- Year of the Graph archive / continuing brand  
  <https://yearofthegraph.xyz/about/>
- KDnuggets archive, 2018: `Graph Databases Burst into the Mainstream`  
  <https://web.archive.org/web/20181113075758/https:/www.kdnuggets.com/2018/02/graph-databases-burst-into-the-mainstream.html>
- TechTarget coverage of Gartner's 2021 graph-technology prediction  
  <https://www.techtarget.com/searchbusinessanalytics/news/252507769/Gartner-predicts-exponential-growth-of-graph-technology>
- Console.today skeptic framing, 2026: graph databases as performance miracle or niche toy  
  <https://www.console.today/data-engineering/graph-databases-performance-miracle-or-niche-toy>
- 2026 podcast/paper title: `Can Graph Databases Go Mainstream?`  
  <https://doi.org/10.5281/zenodo.19790810>
- Corvic, 2026: `You Don't Need a Graph Database — You Need a Graph. There's a Difference.`  
  <https://www.corvic.ai/blog/-graphs-database-problem>

Interview implication:

```text
I want to acknowledge the graveyard. "Year of the graph" has been called before. That is why I am not pitching a standalone graph database wave. I am pitching embedded graph computation where the delivery physics change.
```

### 2. The RDBMS analogy fails in three ways

Do not say graph algorithms will pick up "just like RDBMS did."

The analogy is brittle because:

1. SQL standardized early; graph query languages fragmented across Cypher, Gremlin, SPARQL, and only recently moved toward standardization.
2. Every business already had tables; graph edges often have to be constructed before algorithms can run.
3. Records were mandatory; many graph queries have historically been better answers to optional questions.

Interview implication:

```text
Graphs are universal, but universality does not automatically create a database category. Everything is also a matrix; the commercial form became BLAS, NumPy, and embedded libraries, not a matrix database category.
```

### 3. The OLAP / DuckDB analogy fits mechanically

The better analogy is OLAP's second act.

DuckDB's 2019 paper framed the need for embedded analytical systems: analytical queries inside another process, low setup ceremony, and good behavior in local/intermediate data-science workflows.

Useful URLs:

- DuckDB SIGMOD 2019 paper: `DuckDB: an Embeddable Analytical Database`  
  <https://duckdb.org/pdf/SIGMOD2019-demo-duckdb.pdf>
- DuckDB embedded analytics page  
  <https://duckdb.org/library/embedded-analytics/>
- CloudRPS explainer: DuckDB and embedded OLAP  
  <https://cloudrps.com/blog/duckdb-olap-embedded-analytics/>

Interview implication:

```text
The algorithms are not new. BFS, Dijkstra, PageRank, Louvain — these have existed. What has not collapsed is delivery: run important graph algorithm families on real data without a server, cluster, or memory roulette.
```

### 4. Kùzu proves the embedded-graph slot is real, but not sufficient

Kùzu explicitly pursued an embeddable graph DBMS, inspired by DuckDB/SQLite. That is evidence the slot exists.

But the v4 thesis should not be "Kùzu, but again." The differentiated survival trait should be:

> GDS-workload compatibility plus quote-before-run.

Useful URLs:

- Kùzu launch / positioning page from source note  
  <https://blog.kuzudb.com/post/meet-kuzu/>
- Kùzu design paper  
  <https://cs.uwaterloo.ca/~ssalihog/papers/kuzu-tr.pdf>
- HN launch thread for Kùzu  
  <https://news.ycombinator.com/item?id=33609082>

Safe interview phrasing:

```text
Kùzu is an important precedent because it recognized the embedded-graph gap. I do not want to claim that embedded graph alone is enough. My differentiating bet is the execution contract: quote before run, admit/spill/reject, and receipts after execution for graph algorithm workloads.
```

### 5. Public memory-roulette anecdotes show the pain is not imaginary

The old complaint is not just "graph is slow." It is fear: the job may crash late, after wasting time and cloud spend.

Useful URLs:

- HN, 2016: OutOfMemory and graph database frustration thread  
  <https://news.ycombinator.com/item?id=12352190>
- HN, 2013: graph loading / crash / memory thread  
  <https://news.ycombinator.com/item?id=6713015>

Interview implication:

```text
The pain is predictability. If the system cannot tell me whether the graph job fits before I run it, I have memory roulette. Runtime RAM reduction helps, but quote-before-run attacks the emotional pain directly.
```

### 6. The new demand is AI memory, auditable reasoning, and cheap graph construction

The failed 2018 wave had graph-database supply without enough new demand.

The 2026 wave may have new demand:

- LLMs make entity/relation extraction cheaper.
- GraphRAG and agent memory create provenance needs.
- Embeddings retrieve by similarity; graphs preserve structure and paths.
- Agent verification needs replayable audit trails.

Useful URLs:

- GraphRAG survey  
  <https://arxiv.org/pdf/2408.08921v1>
- Knowledge graphs vs vector stores  
  <https://sqldocs.org/knowledge-graphs-vs-vector-stores/>
- Knowledge graph vs vector store retrieval primitive  
  <https://tianpan.co/blog/2026-04-18-knowledge-graph-vs-vector-store-retrieval-primitive>
- Semantic Web Journal paper from source note  
  <https://www.semantic-web-journal.net/system/files/swj4027.pdf>

Interview implication:

```text
The honest why-now is not "graphs are universal." That was always true. The why-now is that LLMs reduce graph-construction friction, while agent memory and audit workflows make deterministic paths more valuable.
```

## The v4 business insight

V3 said:

> Rejectable is a pricing model.

V4 keeps that and makes it central.

Usage-based infrastructure pricing requires cost predictability. If a graph system cannot quote a job before execution, it is hard to offer:

- fixed-price graph queries;
- per-query SLAs;
- admission guarantees;
- predictable hosted tiers;
- cost receipts;
- customer trust on large workloads.

The company answer:

```text
The library is the proof. The company is the metered execution contract: quote, admit, spill, reject, execute, and emit receipts.
```

The strongest compressed phrase:

> The path is the proof; the quote is the product.

## Official interview rubric from the email

SPC wants to cover:

1. Team dynamic.
2. Ideation.
3. Next steps.

Everything below maps to that rubric.

## 1. Team dynamic

Email prompt:

```text
Tell us how you divide responsibilities and highlight the experiences that make your team special. Please don't recite your LinkedIn or career chronologically.
```

Answer:

```text
Right now I am a solo founder, and I want to say that cleanly.

The operating model is AI-native but judgment-led. I use Devin/Codex-style workflows to expand my build surface area, but I personally own the core judgment: control flow, data flow, benchmark claims, product framing, and what the measurements actually mean.

My edge is the combination. Analytics taught me how data is stored and consumed. Product and enterprise work taught me adoption friction. Rust OSS and Knight Walker gave me the systems execution proof. I am unusual because I care both about the kernel and whether anyone adopts it.

The first hires would map to missing surfaces: a Rust/storage-systems person, a graph algorithms/data-infra person, and a design-partner/customer-development person in the first wedge.
```

## 2. Ideation

Email prompt:

```text
What problems do you want to solve? What initial solutions have you already invalidated, if any? What is your process for generating and validating new ideas?
```

Answer:

```text
The problem I want to solve is not "build the next graph database." The problem is that graph computation is likely to become much more common in agent memory, provenance, security, code, fraud, and dependency workflows, but graph jobs are still too hard to run predictably.

What I have invalidated is the RDBMS analogy. Graph databases already had a hype cycle. The better analogy is OLAP's DuckDB moment: useful compute becoming embedded, zero-ceremony, and routine.

The first solution direction is an embedded graph runtime: storage layout plus pre-run estimation. The runtime should inspect a compact snapshot manifest, estimate the workload, and decide whether to admit, spill, or reject before execution.

My process is measurement-first. I form a thesis, build a narrow proof, compare against a trusted baseline, and let the claim shrink if reality demands it. The strongest signal is not that the first number is huge; it is that the system and the founder both produce receipts.
```

Strongest phrase:

> The path is the proof; the quote is the product.

## 3. Next steps

Email prompt:

```text
If you receive funding, what do you want to accomplish over the next couple months? What hypotheses do you want to validate? How would you spend your time, and why can't you do that now?
```

Answer:

```text
Over the next couple months, I would use funding to turn Knight Walker from a narrow graph-walk POC into a design-partner-ready runtime proof.

The technical milestone is to expand beyond the current walk path into high-usage graph analytics families: WCC, Louvain or Leiden, PageRank, NodeSimilarity or KNN, shortest paths, FastRP, and triangles.

The product milestone is to validate the estimation contract: can a compact snapshot manifest let us quote memory/cost before execution and then admit, spill, or reject the job?

The market milestone is to choose the first wedge. My current bias is agent memory / GraphRAG because funded teams are improvising graph-shaped memory and provenance right now. IAM and SBOM/security are strong sequel wedges because budgets exist, but their enterprise sales cycles may be slower for a solo founder.

I can keep building slowly now, but funding buys focused time, benchmark infrastructure, possibly a small amount of research help, and the credibility to run design-partner conversations seriously.
```

## If forced to pick the first wedge

Say:

```text
If you force me to pick today, I would start with agent memory / GraphRAG.

The reason is that the demand is current and messy. Funded teams are already improvising memory, provenance, and retrieval. Embeddings are useful, but they cannot give a replayable audit path. IAM and SBOM are strong sequel wedges, but their sales cycles may be heavier for a solo founder.
```

## The proof today

```text
The current proof is narrow but real. The public Rust POC returns the same answers as Neo4j on the tracked walk benchmark, and on the tracked 2GB walk path it used 4.5x lower runtime RSS.

I do not want to overclaim this as a database replacement. The proof says the storage-layout direction is alive enough to expand across algorithms and validate the pre-run estimation contract.
```

Always say:

> 4.5x lower runtime RSS on the tracked 2GB walk path.

## Why now

Use this instead of "LLMs let me build more."

```text
Why now is demand-side first.

First, agents are becoming query authors. Humans did not learn graph traversal languages at SQL scale, but agents do not care about representational loyalty.

Second, AI memory and audit workflows are becoming must-have graph workloads. Verification of agent behavior is one of the central AI problems now, and embeddings alone cannot show provenance. The path is the proof.

Third, LLMs reduce graph-construction friction. In the earlier graph wave, edges often had to be painfully constructed. Now entity and relationship extraction is cheaper and increasingly standard in GraphRAG pipelines.

Fourth, modern single-node machines plus mmap and CSR-style layouts make graph analytics operationally boring in a way distributed graph systems were not.

Fifth, the relational ecosystem is absorbing graph patterns, which is a signal that graph compute is becoming routine. That hurts a standalone graph database story, but helps an embedded runtime story.
```

## Why this is a company, not just a library

```text
The library is the proof. The company is the metered execution contract.

If the runtime can quote a graph workload before execution, it can support things a library alone cannot: fixed-price tiers, per-query SLAs, admission guarantees, workload receipts, and eventually a hosted commercial surface around one vertical.

That is why "rejectable" matters. It is not only a safety feature. It is the foundation for pricing.
```

## Why not Neo4j?

```text
Neo4j is not dumb, and I should assume incumbents are smart.

My wedge is not "incumbents cannot optimize." My wedge is that a new runtime can start from an estimation-and-execution contract: quote before run, deterministic path, deterministic bill.

Incumbents have product compatibility and cloud economics built around the existing model. Retrofitting a quote-before-run contract is different from copying one storage optimization.
```

## Why not embeddings or giant context?

```text
Embeddings retrieve by similarity. Graphs preserve relationships and paths.

Long context helps, but it does not replace explicit structure. It is poor at durable temporal versioning, replayable audit, multi-hop aggregation, and deterministic provenance. Also, token economics favor structured recall over stuffing every memory trace and relationship path into the prompt.

So the point is not graphs versus embeddings. The point is that agent systems need both: embeddings for fuzzy recall, graphs for auditable paths.
```

## What could kill this?

```text
The macro bear case is that most graph questions remain shallow enough for joins, and the explicit-structure need is smaller than I think.

The product bear case is that DuckDB/Postgres/Neo4j/vector vendors absorb enough graph capability that a standalone runtime has no room.

The AI bear case is that long context or model memory becomes good enough for the first wedge.

My response is to focus on workloads where paths, provenance, iteration, temporal versioning, and audit actually matter. If those workloads do not pull, this should not become a company.
```

## Which graph algorithms should you name fluently?

Name these:

1. WCC / weakly connected components
2. Louvain / Leiden
3. PageRank
4. NodeSimilarity / KNN
5. Shortest paths
6. FastRP
7. Triangles

Rehearsal sentence:

```text
The expansion is not random. I want to cover the high-usage graph analytics families: WCC, Louvain or Leiden, PageRank, NodeSimilarity or KNN, shortest paths, FastRP, and triangles.
```

## Likely questions and v4 answers

### 1. What exactly are you building?

```text
An embedded graph computation runtime for agent-era workloads: deterministic paths, low-RAM execution, and pre-run workload pricing.
```

### 2. Is this a graph database company?

```text
Not in the classic standalone database sense. I think graph compute grows, but the runtime may become embedded and invisible. The company wedge is the metered execution surface around the runtime.
```

### 3. Why should this wave work when prior graph waves disappointed?

```text
Because I am not depending on the old graph database wave. The new demand is different: agents write queries, LLMs lower graph-construction cost, and agent memory / audit workflows need deterministic paths. The delivery model is also different: embedded, DuckDB-shaped execution rather than a standalone graph server.
```

### 4. What is your non-obvious insight?

```text
Graph workloads need to be quoteable before they are run. If you can price, admit, spill, or reject a graph job before execution, you can turn graph analytics from memory roulette into routine infrastructure.
```

### 5. Why is agent memory / GraphRAG the first wedge?

```text
Because funded teams are already improvising there. They need memory, provenance, and verification. Embeddings retrieve, but they do not produce replayable audit paths. That is where deterministic graph execution can matter first.
```

### 6. Why are you the right person?

```text
My edge is the combination. I have worked where messy behavior becomes data, where data becomes models, where product adoption matters, and now where the systems kernel has to prove itself. I am not a pure infra person who only cares about elegance; I care whether the workflow gets adopted.
```

### 7. What do you need from SPC?

```text
I need help with wedge selection and design partners. I can keep pushing the runtime, but I want SPC's judgment on whether agent memory / GraphRAG is the right first wedge, what proof is enough before raising, and how to turn an OSS/runtime proof into a commercial surface.
```

## What to ask SPC

Ask two or three, not all.

1. "Do you buy agent memory / GraphRAG as the first wedge, or would you push me toward IAM/SBOM/security where budgets are clearer?"
2. "When you see infra founders at this stage, what evidence separates an interesting runtime from a company-worthy wedge?"
3. "If a graph runtime can quote, admit, spill, or reject jobs before execution, does that feel like a product wedge, a pricing model, or just an implementation detail?"
4. "What would make you pass on this even if the benchmark surface keeps improving?"
5. "Who are the 3-5 design partners or founders you would send me to first?"

## What not to say

Avoid:

- "Graphs are the next RDBMS."
- "I am building the Oracle of graphs."
- "Neo4j is dumb."
- "Graphs beat embeddings."
- "Long context does not matter."
- "This is definitely venture-scale."
- "The 4.5x number proves the whole company."
- "Kùzu failed, so I can win." Better: Kùzu proves the slot and the difficulty.

Say instead:

- "Graph compute grows, but the runtime may become embedded and invisible."
- "The right analogy is DuckDB-shaped embedded analytics, not RDBMS category creation."
- "Incumbents are smart, but retrofitting quote-before-run is hard."
- "Graphs and embeddings are complementary: similarity plus paths."
- "Long context helps, but explicit structure wins on audit, temporal memory, and multi-hop aggregation."
- "The company question depends on the first wedge."
- "The current proof is scoped; the next proof is algorithm breadth plus estimation."

## Interview strategy for the 15-minute slot

### Minute 0-2: establish the v4 thesis

- Concede the graph database hype cycle.
- Shift to graph computation's DuckDB moment.
- State the two new demand changes: LLM-built graphs and agent/audit workloads.
- State the wedge: deterministic paths and deterministic bills.
- Scope the proof.

### Minute 2-5: team dynamic

- Solo founder.
- AI-native leverage in one sentence.
- Judgment remains with you.
- First hires by missing surface.

### Minute 5-9: ideation

- Invalidated RDBMS analogy.
- Embedded graph computation thesis.
- Quote-before-run product.
- Measurement honesty.

### Minute 9-13: next steps

- Expand algorithm families.
- Validate compact-manifest estimation.
- Pick first wedge: agent memory / GraphRAG, with IAM/SBOM as sequels.
- Find design partners.

### Minute 13-15: close

```text
The main help I want from SPC is wedge judgment. If graph computation is going to have its DuckDB moment, I want to find the first workflow where deterministic paths and deterministic bills are urgent enough to build a company around.
```

## Rehearsal card

```text
I am not pitching "graphs are the next RDBMS."
Graph databases already had their hype cycle.

I am pitching graph computation's DuckDB moment:
embedded, zero-ceremony, routine.

Two things changed:
LLMs make graph construction cheaper;
agent/audit workloads make deterministic paths valuable.

Embeddings retrieve by similarity.
Graphs provide audit paths.
The path is the proof.

Knight Walker direction:
compact snapshot manifest → pre-run estimate → admit/spill/reject → receipts.

Current proof:
same answers as Neo4j on tracked walk benchmark;
4.5x lower runtime RSS on tracked 2GB walk path.

Company wedge:
deterministic paths, deterministic bills.
The quote is the product.

First wedge:
agent memory / GraphRAG.
Sequels:
IAM and SBOM/security.

Ask from SPC:
help pressure-test wedge, design partners, and what proof is enough before raising.
```

## Relevant URL pack

Use these only as supporting references. Do not overstuff the 15-minute interview with URLs.

### Prior graph hype / category skepticism

- <https://www.zdnet.com/article/the-year-of-the-graph-getting-graphic-going-native-reshaping-the-landscape/>
- <https://yearofthegraph.xyz/about/>
- <https://web.archive.org/web/20181113075758/https:/www.kdnuggets.com/2018/02/graph-databases-burst-into-the-mainstream.html>
- <https://www.techtarget.com/searchbusinessanalytics/news/252507769/Gartner-predicts-exponential-growth-of-graph-technology>
- <https://www.console.today/data-engineering/graph-databases-performance-miracle-or-niche-toy>
- <https://doi.org/10.5281/zenodo.19790810>
- <https://www.corvic.ai/blog/-graphs-database-problem>

### DuckDB / embedded OLAP analogy

- <https://duckdb.org/pdf/SIGMOD2019-demo-duckdb.pdf>
- <https://duckdb.org/library/embedded-analytics/>
- <https://cloudrps.com/blog/duckdb-olap-embedded-analytics/>

### Kùzu / embedded graph precedent

- <https://blog.kuzudb.com/post/meet-kuzu/>
- <https://cs.uwaterloo.ca/~ssalihog/papers/kuzu-tr.pdf>
- <https://news.ycombinator.com/item?id=33609082>

### Memory roulette / public pain

- <https://news.ycombinator.com/item?id=12352190>
- <https://news.ycombinator.com/item?id=6713015>

### GraphRAG / knowledge graph versus vector retrieval

- <https://arxiv.org/pdf/2408.08921v1>
- <https://sqldocs.org/knowledge-graphs-vs-vector-stores/>
- <https://tianpan.co/blog/2026-04-18-knowledge-graph-vs-vector-store-retrieval-primitive>
- <https://www.semantic-web-journal.net/system/files/swj4027.pdf>

## V4 caveats to preserve

- Knight Walker is still a Rust POC, not a finished database.
- The 4.5x claim is scoped to runtime RSS on the tracked 2GB walk path.
- The compact-manifest quote-before-run layer is the deeper product direction; do not overstate it as fully proven unless you can demonstrate it.
- Graph computation growing does not imply a standalone graph database category.
- Agent memory / GraphRAG is a wedge hypothesis, not a proven market.
- Kùzu is a precedent that cuts both ways; it proves the embedded slot and the difficulty.
- Hype-cycle skepticism is already priced into sophisticated audiences.
- The founder signal is measurement discipline and wedge judgment, not maximal certainty.

## Last-mile checklist

- [ ] Memorize the v4 opening sentence: "Graph databases already had their hype cycle; I am pitching graph computation's DuckDB moment."
- [ ] Say `runtime`, not engine/database/tool interchangeably.
- [ ] Say `4.5x lower runtime RSS on the tracked 2GB walk path`.
- [ ] Prepare the compact-manifest → estimate → admit/spill/reject explanation.
- [ ] Prepare the DuckDB analogy without overexplaining it.
- [ ] Prepare the "year of the graph" caveat.
- [ ] Prepare the "why now is different" answer: LLM-built graphs + agent/audit paths.
- [ ] Prepare the "model just remembers" answer.
- [ ] Pick agent memory / GraphRAG as first wedge if forced.
- [ ] Ask SPC whether they buy that wedge.

## Source map

| Claim | Source |
|---|---|
| Official interview rubric and logistics | `A000-spc-founder-interview-prep.md` |
| Submitted application context | `A001-spc-submission-source-draft.md` |
| V2 graph-universality and deterministic-pathways variant | `A002-spc-founder-interview-prep-v2.md` |
| V3 category correction and quote-before-run business model | `A003-spc-founder-interview-prep-v3.md` |
| V4 graph adoption wave thesis and URL pack | `/Users/amuldotexe/.codex/attachments/0372c7e7-a7f9-47e2-a8fc-2f47b0b850e7/pasted-text.txt` |
| DuckDB embedded analytics precedent | <https://duckdb.org/pdf/SIGMOD2019-demo-duckdb.pdf> and <https://duckdb.org/library/embedded-analytics/> |
| Kùzu embedded graph precedent | <https://cs.uwaterloo.ca/~ssalihog/papers/kuzu-tr.pdf> |
| GraphRAG / KG-vs-vector demand shift | <https://arxiv.org/pdf/2408.08921v1>, <https://sqldocs.org/knowledge-graphs-vs-vector-stores/>, <https://tianpan.co/blog/2026-04-18-knowledge-graph-vs-vector-store-retrieval-primitive> |
| Public repo | <https://github.com/that-in-rust/knight-bus-graph-walker> |

