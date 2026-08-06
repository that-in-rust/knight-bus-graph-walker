# SPC Founder Interview Prep V2

Created: 2026-08-01  
Primary objective: prepare Amul Badjatya for the South Park Commons Founder Fellowship interview around Knight Walker / Knight Bus Graph Walker.  
Variant basis: `A000-spc-founder-interview-prep.md`, `A001-spc-submission-source-draft.md`, pasted critique dated 2026-07-31, and Amul's new graph-universality / deterministic-pathways framing.

## What changed from v1

V1 was mechanically strong: it followed SPC's interview rubric, stayed honest about the 4.5x runtime-RAM claim, and gave a good 15-minute flow.

V2 changes the center of gravity.

The pitch should no longer sound like:

> I made graph walking use less RAM with mmap.

The stronger pitch is:

> Graphs are a universal structure for computation, systems, decisions, code, identity, supply chains, fraud, and AI memory. Embeddings are useful but approximate; graphs preserve deterministic pathways. Knight Walker is testing whether graph workloads can become routine infrastructure by making traversal not only cheaper, but priceable, auditable, and rejectable before execution.

The new emphasis is:

1. Graph universality, not Neo4j comparison.
2. Deterministic pathways, not only embeddings.
3. Estimation-first memory honesty, not only low-RAM execution.
4. Measurement discipline, not only benchmark bragging.
5. A wedge-finding process, not vague market exploration.

## The one answer this interview must leave behind

Knight Walker is worth SPC's attention because it sits at the intersection of three important shifts:

1. More important workloads are becoming graph-shaped.
2. Existing graph analytics can be too memory-hungry and operationally scary for routine cloud use.
3. AI-native engineering lets a product-minded founder build enough systems proof to test a hard infra thesis before assembling a large team.

The compact thesis:

> I am building a graph analytics runtime that makes graph workloads cheaper, more predictable, and more deterministic by designing execution from the storage layout and workload-estimation layer upward.

The important nuance:

> The product is not just lower RAM. The deeper product is memory honesty: can we price, admit, spill, or reject a graph workload before running it?

That is more defensible than a benchmark alone. Incumbents can copy an optimization. It is harder to retrofit an estimation-and-refusal contract across a mature graph analytics surface.

## The graph-universality thesis

Graphs are not a niche database category. Graphs are a way of representing relationships, dependencies, control flow, access, influence, similarity, and causality.

Examples:

- compilers use graphs: ASTs, CFGs, SSA graphs, call graphs, dependency graphs;
- codebases are graphs: modules, symbols, imports, interfaces, ownership boundaries, call paths;
- identity systems are graphs: users, roles, permissions, groups, resources, sessions;
- supply chains are graphs: vendors, shipments, parts, locations, risks;
- fraud and risk are graphs: accounts, devices, transactions, counterparties, collusion rings;
- recommendations are graphs: users, items, sessions, co-occurrences, preferences;
- AI memory and GraphRAG are graphs: documents, entities, claims, citations, tools, execution traces.

This is the "why now" beyond "I can build more with LLMs."

The graph algorithm world may become a different game altogether because enterprises increasingly need explainable pathways through complex systems. Embeddings are powerful, but they are approximate similarity surfaces. They can retrieve useful things, but they do not naturally give deterministic paths, obligations, provenance, or explainable traversal.

The beauty of graph synthesis is the determinism of pathways:

> If A depends on B, B grants C, C reaches D, and D violates policy, the path itself is the explanation.

For enterprise adoption, this matters. Deterministic paths are easier to audit, verify, replay, compare, and govern.

## The v2 opening 60 seconds

Use this if they ask, "Tell us what you're building."

```text
I am building Knight Walker, a Rust graph-analytics runtime for making graph workloads cheaper and more predictable.

The broader thesis is that graphs are becoming a universal substrate: compilers are graphs, codebases are graphs, IAM permissions are graphs, supply chains and fraud rings are graphs, and AI memory is increasingly graph-shaped. Embeddings are useful, but they are approximate. Graph pathways are deterministic and auditable.

The practical problem is that graph analytics often becomes too RAM-heavy for routine cloud use. Teams may know graph algorithms are useful, but if a 50GB graph needs a 128GB server and can still fail late, they avoid the workload.

Knight Walker attacks this from the storage and estimation layer up. The current Rust POC uses dense ids, CSR-style adjacency, and mmap-backed traversal; on the tracked 2GB walk benchmark it returned the same answers as Neo4j while using 4.5x lower runtime RSS. The deeper product direction is not just the RAM number: it is a runtime that can price a graph workload from a compact snapshot manifest before running it, then admit, spill, or reject instead of failing expensively at runtime.
```

## The 20-second version

```text
I am testing whether graph algorithms can become as routine as relational aggregations by making graph workloads deterministic, priceable, and low-RAM enough to run in normal cloud environments.
```

## What SPC already knows from the application

SPC has already seen the written version:

- You are solo.
- You applied through the Founder Fellowship path.
- You have not raised funding or actively fundraised for Knight Walker.
- You are building technical proof before fundraising.
- The main artifact is the MIT-licensed Rust repo.
- The strongest supporting artifacts are the two public X thesis threads and the Apache Iggy PR.
- You want help turning a technical proof into adoption, GTM, design partners, and OSS monetization.

Primary links:

- Knight Bus Graph Walker repo: <https://github.com/that-in-rust/knight-bus-graph-walker>
- Storage-layer-up / Devin Ambassador thread: <https://x.com/amuldotexe/status/2073247774674710970>
- RAM-heavy graph OLAP thesis thread: <https://x.com/amuldotexe/status/2068194152941326836>
- Apache Iggy PR: <https://github.com/apache/iggy/pull/2815>
- SPC application page: <https://www.southparkcommons.com/apply>
- Prateek Mehta SPC post: <https://x.com/prateekmehta42/status/2079148824082432163>

## Official interview rubric from the email

The SPC email says they want to cover:

1. Team dynamic.
2. Ideation.
3. Next steps.

This v2 keeps that structure. Do not over-explain the program. Their email said they will not spend time on introductions or program Q&A live.

## 1. Team dynamic

Email prompt:

```text
Tell us how you divide responsibilities and highlight the experiences that make your team special. Please don't recite your LinkedIn or career chronologically.
```

Answer:

```text
Right now I am a solo founder. I want to say that plainly rather than pretend there is a team.

The operating model is that I use AI-native engineering workflows to expand my build surface area, but final judgment stays with me. Devin and Codex help me explore more paths, but I still review the core control-flow, data-flow, benchmark, and product decisions.

What makes the "team" special right now is my combination: analytics gave me a feel for how data is stored and consumed; games and product work gave me behavior and adoption instincts; enterprise product work taught me workflow friction; and the recent Rust/OSS phase gave me the systems muscle to build the proof myself.

The first people I would recruit are not generic hires. I would look for three missing surfaces: a Rust/storage-systems person, a graph algorithms/data-infra person, and a design-partner/customer-development person in a graph-heavy domain.
```

Keep the AI-native part to one sentence in live speech. It is a strength, but too much emphasis invites the wrong question: "Did the AI build it?" The better emphasis is judgment.

## 2. Ideation

Email prompt:

```text
What problems do you want to solve? What initial solutions have you already invalidated, if any? What is your process for generating and validating new ideas?
```

Answer:

```text
The problem I want to solve is that graph-shaped questions are becoming more important, but graph analytics is still not routine infrastructure for many teams because the memory and operational cost is intimidating.

The big belief is that graphs are universal. Compilers are graphs. Codebases are graphs. IAM permissions are graphs. Fraud, recommendations, supply chains, and AI memory are graphs. Embeddings help with approximate retrieval, but graph pathways are deterministic, explainable, and auditable.

The first solution I am testing is storage-specialized graph execution: dense ids, offsets, CSR-style adjacency, mmap-backed traversal, and now an estimation-first layer that can price memory before execution. I do not want the runtime to merely fail cheaper. I want it to tell you whether the job fits before you run it.

What I have deprioritized is starting as a broad Neo4j replacement or a graph UI wrapper. Those are too wide and do not attack the deepest cost and trust structure first. The sharper wedge is deterministic traversal economics plus pre-run memory honesty.

My ideation process is measurement-first. I form a thesis, build a narrow proof, compare against a trusted baseline, then let the numbers shrink if reality demands it. For example, assumptions like "disk-backed means slower" or large early speedup estimates have to survive measurement. If they do not, I keep the smaller, truer claim.
```

The strongest phrase:

> I want to build graph infrastructure that is deterministic enough to trust and cheap enough to use routinely.

## 3. Next steps

Email prompt:

```text
If you receive funding, what do you want to accomplish over the next couple months? What hypotheses do you want to validate? How would you spend your time, and why can't you do that now?
```

Answer:

```text
Over the next couple of months, I would use funding to turn Knight Walker from a narrow graph-walk POC into a design-partner-ready benchmark and estimation surface.

The technical milestone is to expand beyond the current walk path into the graph algorithm families that cover most practical graph analytics: WCC, Louvain or Leiden, PageRank, NodeSimilarity or KNN, shortest paths, FastRP, and triangles.

The product milestone is to test whether the deeper contract matters: can a runtime estimate memory from a compact snapshot manifest, then admit, spill, or reject the workload before execution?

The market milestone is to pressure-test the first wedge. I am looking at graph-heavy domains such as IAM access paths, SBOM blast radius, fraud/risk, recommendations, code intelligence, supply-chain analytics, and AI knowledge graphs. The goal is to find the first workflow where deterministic low-RAM traversal is urgent, not merely impressive.

I can keep building slowly now, but funding buys focused time, benchmark infrastructure, a small amount of research or intern help, and the credibility to run design-partner conversations seriously rather than as a side project.
```

## The strongest proof so far

Use this if asked what is real today:

```text
The proof is narrow but real. The public Rust POC returns the same answers as Neo4j on the tracked walk benchmark, and on the tracked 2GB dataset it used 4.5x lower runtime RSS.

I am careful not to overclaim that as a database replacement. Neo4j has a mature product surface and may still win on some opening/cold-start paths. What the proof shows is that the storage-layout thesis is alive enough to expand into more algorithms and to add an estimation-first contract.
```

The scoping phrase:

> 4.5x lower runtime RSS on the tracked 2GB walk path.

Say the qualifier every time.

## The better why-now answer

V1's why-now leaned too much on "LLMs let me build more." Keep that as a founder-supply-side point, but lead with demand.

Better answer:

```text
Why now is demand-side first.

More workloads are becoming graph-shaped: code intelligence, GraphRAG, agent memory, fraud, identity, supply-chain risk, and enterprise knowledge systems. These are teams that may need graph algorithms but will not casually buy a giant RAM-heavy graph setup just to experiment.

At the same time, AI is making more systems relationship-heavy. We need deterministic pathways, not only approximate embeddings. A graph can say exactly why this code path, permission path, dependency path, or evidence path exists.

The founder-supply-side reason is secondary: LLMs let a product-minded founder like me get deeper into systems implementation than before, but the market reason is that graph-shaped workloads are becoming more common and need cheaper, more predictable execution.
```

## Why this is a company, not just a library

```text
Right now it is a technical proof, and I am honest about that. The company question depends on whether one wedge has acute enough pain.

My belief is that the library alone is not the whole product. The product is the execution contract around graph workloads: deterministic traversal, pre-run memory estimation, admission/spill/rejection, receipts after execution, and eventually a commercial surface around one vertical.

The first company milestone is to find a workflow where this changes adoption behavior: where a team goes from "this graph job is too risky or expensive to run routinely" to "we can run this as normal infrastructure."
```

## Why won't Neo4j or another incumbent just do this?

```text
They may improve parts of it, and I should assume serious incumbents are smart.

My wedge is not that incumbents are incompetent. My wedge is that I am starting from a narrower contract: make graph execution memory-honest and deterministic from the storage layer up.

Incumbents have compatibility, product-surface, and revenue constraints. A new runtime can be opinionated around a smaller promise first: estimate the workload, prove the memory behavior, and make low-RAM traversal credible for one painful wedge.
```

## Which graph algorithms should you name fluently?

The critique says the "top seven algorithms" should be named, not left vague.

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
The next expansion is not random. I want to cover the high-usage graph analytics families: WCC, Louvain or Leiden, PageRank, NodeSimilarity or KNN, shortest paths, FastRP, and triangles.
```

## The deterministic-pathways answer

Use this if they push on embeddings, GraphRAG, or "why graphs?"

```text
Embeddings are valuable, but they are approximate. They tell you what is similar.

Graphs tell you what is connected, how, through which path, and under what relationship. That matters in enterprise systems because explanations often need pathways: which permission granted access, which dependency created the blast radius, which code path called the risky function, which transaction cluster formed a fraud ring.

So I do not see graphs as competing with embeddings. I see them as the deterministic layer that embeddings often need around them.
```

## The measurement-honesty story

This is one of the best ways to sound like a serious founder.

```text
One thing I want to emphasize is that I am not trying to protect a flashy benchmark. My process is to let measurements correct me.

Some early assumptions became smaller or more precise after measurement. The right conclusion was not "disk-backed is always slower" or "we have a giant universal speedup." The right conclusion was narrower: storage layout can remove specific runtime memory pressure, and the claim has to be scoped by workload.

That is the kind of company I want to build: one where the runtime produces receipts and the founder also lives by receipts.
```

This is powerful because SPC is likely screening for judgment more than for a polished sales narrative.

## The one-screen demo decision

If they ask to see the repo, do not improvise a tour.

The best 20-second artifact is:

```text
Here is the same-answer check against Neo4j, and here are the runtime RSS numbers side by side on the tracked walk benchmark.
```

Do not open many files. Do not walk them through architecture. The interview is too short.

## Likely questions and v2 answers

### 1. What exactly are you building?

```text
A Rust graph analytics runtime that makes graph workloads deterministic, low-RAM, and eventually priceable before execution.
```

### 2. Is this just a faster graph database?

```text
No. The database surface may come later or may never be the first wedge. The first wedge is execution: graph algorithms that can run with predictable memory and deterministic traversal behavior.
```

### 3. What is your non-obvious insight?

```text
The non-obvious insight is that graph execution should be shaped from the storage layout and workload-estimation contract upward. If the runtime can know the graph shape and price the workload before execution, it becomes much safer to use graph algorithms routinely.
```

### 4. Why graphs versus embeddings?

```text
Embeddings are approximate similarity. Graphs provide deterministic pathways. For enterprise workflows, the path is often the proof.
```

### 5. What have you invalidated?

```text
I have deprioritized broad graph database replacement, graph UI wrappers, and unscoped benchmark claims. The more interesting measured wedge is low-RAM deterministic traversal plus pre-run memory honesty.
```

### 6. Who is the customer?

```text
I am still choosing the wedge, but the likely users are teams where graph questions are valuable and expensive to run routinely: IAM access paths, SBOM blast radius, fraud/risk, recommendations, code intelligence, supply-chain analytics, and AI knowledge graphs.
```

### 7. What could kill this?

```text
Three things.

One, the memory advantage may narrow as the algorithm surface expands. Two, the pain may be real but not urgent enough in the first wedge. Three, adoption may require too much database compatibility before the execution contract matters.

That is why the next phase has to be design-partner-led and benchmark-led.
```

### 8. What do you need from SPC?

```text
I need high-quality judgment on wedge selection. I can keep building the technical proof, but I want SPC's help finding the first design partners, pressure-testing whether this is a venture-scale company, and deciding what evidence is enough before raising.
```

## What to ask SPC

Ask two or three, not all.

1. "When you see infra founders at this stage, what evidence separates an interesting technical POC from a company-worthy wedge?"
2. "Which first market would you pressure-test for deterministic low-RAM graph execution: IAM, SBOM/security, fraud/risk, recommendations, code intelligence, supply chain, or AI knowledge graphs?"
3. "If the runtime can estimate and refuse memory-expensive graph jobs before execution, does that sound like a product wedge or just an implementation detail?"
4. "For OSS infrastructure, what do you think is the fastest path to credible adoption: benchmarks, design partners, developer community, or a hosted commercial surface?"
5. "What would make you pass on this idea even if the benchmark keeps improving?"

## What not to say

Avoid:

- "Neo4j is dumb."
- "This is a 10x cloud-cost reduction."
- "Graphs beat embeddings."
- "LLMs let me build anything alone."
- "This is definitely a giant company."
- "It is just a useful OSS library."

Say instead:

- "Incumbents are smart but constrained."
- "The current proof is scoped: 4.5x lower runtime RSS on the tracked 2GB walk path."
- "Graphs provide deterministic pathways; embeddings provide approximate similarity. They are complementary."
- "AI-native workflows expand my surface area, but judgment stays with me."
- "The company question depends on finding the first urgent wedge."
- "The library is the proof; the product is the execution contract and adoption surface around a painful workflow."

## Interview strategy for the 15-minute slot

### Minute 0-2: establish the upgraded thesis

- Graphs are universal.
- Embeddings are approximate; graph paths are deterministic.
- Current graph analytics is too memory-heavy for routine use.
- Knight Walker's proof is scoped but real.
- The deeper product is memory honesty and pre-run pricing.

### Minute 2-5: team dynamic

- Solo founder, clearly.
- AI-native leverage in one sentence.
- Your unusual combination.
- First hires by missing surface area.

### Minute 5-9: ideation

- Problem: graph workloads are universal but not routine enough.
- Invalidations: broad database replacement, UI wrapper, unscoped benchmark claims.
- Process: thesis → narrow proof → baseline comparison → measurement shrinks claims → wedge search.

### Minute 9-13: next steps

- Expand algorithm surface.
- Add estimation/admission/spill/refusal contract.
- Test 50GB-class dataset on normal machine target.
- Find first urgent design-partner wedge.

### Minute 13-15: close with ask

```text
The main thing I want from SPC is help finding the first wedge where deterministic low-RAM graph execution is urgent. I can keep pushing the runtime, but I want sharper judgment on design partners, GTM, OSS adoption, and fundraising readiness.
```

## Rehearsal card

```text
I am not trying to clone Neo4j.
I am testing whether graph algorithms can become routine cloud infrastructure.

Graphs are universal:
compilers, codebases, IAM, supply chains, fraud, recommendations, AI memory.

Embeddings are approximate.
Graph paths are deterministic.

The wedge is storage + estimation:
dense ids + CSR adjacency + mmap traversal + pre-run memory pricing.

The proof is scoped:
same answers as Neo4j on the tracked walk benchmark;
4.5x lower runtime RSS on the tracked 2GB walk path.

The next proof:
top graph algorithm families + 50GB-class workload + normal-machine target + design partners.

The ask from SPC:
help find the first urgent wedge and pressure-test whether this is a company.
```

## V2 caveats to preserve

- Knight Walker is a Rust POC, not a finished database.
- The 4.5x claim is scoped to runtime RSS on the tracked 2GB walk path.
- Neo4j may still win on some mature product surfaces and cold-open paths.
- Estimation-first / pre-run pricing is a product direction to validate, not a fully proven finished feature unless the repo artifacts demonstrate it live.
- The first market wedge is still being chosen.
- The strongest founder signal is not certainty; it is measurement discipline.

## Last-mile checklist

- [ ] Say `Knight Walker` verbally; use `Knight Bus Graph Walker` for the repo.
- [ ] Keep category noun consistent: `runtime`.
- [ ] Memorize the seven graph algorithm families.
- [ ] Prepare the one-screen same-answer/RSS demo.
- [ ] Prepare one short deterministic-pathways example: IAM access path, SBOM blast radius, or compiler call graph.
- [ ] Prepare one answer for "why not embeddings?"
- [ ] Prepare one answer for "why not Neo4j?"
- [ ] Prepare one answer for "what is the first wedge?"
- [ ] Do not overclaim the estimation-first layer; phrase it as the next product contract if not yet fully implemented.

## Source map

| Claim | Source |
|---|---|
| Official interview rubric | Gmail screenshot / email summarized in `A000-spc-founder-interview-prep.md` |
| Application content | `A001-spc-submission-source-draft.md` |
| V1 interview structure | `A000-spc-founder-interview-prep.md` |
| Critique of v1 | `/Users/amuldotexe/.codex/attachments/80461bcd-a79f-4d41-8605-90f4c36e6522/pasted-text.txt` |
| Graph universality / deterministic pathways framing | Amul note in current Codex thread, 2026-08-01 |
| Public repo | <https://github.com/that-in-rust/knight-bus-graph-walker> |

