# SPC Founder Interview Prep

Created: 2026-07-31  
Primary objective: prepare Amul Badjatya for the South Park Commons Founder Fellowship interview around Knight Walker / Knight Bus Graph Walker.

## Interview logistics

Calendar source: Google Calendar search for `SPC` in the in-app browser, account `amuldotexe / amul.exe@gmail.com`.

| Item | Detail |
|---|---|
| Interview / meeting title | `Amul Badjatya and Harshit Madan + Prateek Mehta` |
| Date | Wednesday, 2026-08-05 |
| Time | 2:00 PM - 2:20 PM IST |
| Format | Zoom |
| Meeting link | Present in calendar; intentionally not copied into this repo note to avoid leaking a live meeting URL if the repo is pushed. |
| Likely interviewers / participants | Harshit Madan and Prateek Mehta |

Official email source: `SPC Founder Fellowship Interview - Amul Badjatya`, from South Park Commons, received Wednesday, 2026-07-29 at 8:19 AM.

The email says:

- The interview is **15 minutes**.
- One or two SPC Founder Fellowship committee members will attend.
- One committee member will take the lead on questions.
- SPC will **not spend time introducing themselves or answering program questions live**.
- The three primary areas are:
  - team dynamic;
  - ideation;
  - next steps.

Calendar context also shows prior SPC-adjacent touchpoints:

- 2026-02-17: `-1 to Anthropic CTO with Rahul Patil | At South Park Commons, BLR`
- 2026-04-23: `escape velocity (SPC BLR demo night)` — accepted
- 2026-05-26: `Fireside Chat with Dr. Boyd Fowler | Moderated by Vrinda Kapoor | SPC India` — accepted
- 2026-06-27: `Physical AI - Architects of Reality` — accepted

The useful implication: do not talk about SPC as a cold, abstract fellowship. Talk about it as a high-talent-density community you have already been orbiting, and now want to use as the proving ground for a hard systems company.

## The one answer this interview must leave behind

Knight Walker is worth SPC's attention because it is not a generic AI-era founder idea; it is the convergence of Amul's 15-year analytics/product arc, newly acquired Rust systems depth, and a concrete thesis that graph execution can be made dramatically cheaper by redesigning storage around traversal access patterns.

In 15 minutes, the goal is not to explain every benchmark. The goal is to make them believe four things:

1. You have found a real, expensive pain point.
2. You have a non-obvious technical wedge.
3. You are unusually suited to pursue it because you combine customer/product empathy with hard technical persistence.
4. SPC can help with the thing you need most: turning an OSS/runtime thesis into adoption, GTM, and eventually a durable company.

## The opening 60 seconds

Use this if they ask, "Tell us what you're building."

```text
I am building Knight Walker, a Rust graph-analytics runtime that makes graph walking cheaper by reshaping graph storage around traversal access patterns.

The pain point is that graph algorithms are useful in fraud, recommendations, dependency analysis, supply-chain risk, AI/code graphs, and enterprise analytics, but OLAP graph runtimes often load too much graph state into RAM. In cloud environments, that makes graph workloads expensive and scary to run routinely.

Knight Walker attacks this from the storage layer up. The POC precompiles forward/reverse CSR-style adjacency, dense ids, offsets, and mmap-backed traversal so walking can use direct storage arithmetic instead of loading everything into runtime RAM. On the tracked 2GB dataset it returned the same answers as Neo4j while using 4.5x lower runtime RAM.

The larger question I want to test with SPC is whether this can become default infrastructure for graph-shaped workloads, not just a clever benchmark.
```

## The more human version

Use this if the conversation becomes founder-market-fit oriented.

```text
The non-chronological version is: I have repeatedly worked on systems where messy real-world behavior has to become queryable structure.

In analytics, that meant turning raw business data into decisioning and risk models. In games and mobility, it meant understanding behavior and incentives through telemetry. In enterprise product, it meant learning why adoption and workflow friction matter as much as the technical system. In my recent Rust/AI-native work, the same pattern became more explicit: large systems are often graph-shaped, but we lack cheap enough infrastructure to traverse and reason over them routinely.

Knight Walker is the point where that pattern turns into an infrastructure thesis. Useful graph questions exist everywhere; the problem is that current execution patterns can make them too expensive or operationally intimidating to ask every day.
```

## The sharpest positioning

Do not pitch this as:

> "I am making a faster Neo4j."

Pitch it as:

> "I am testing whether graph algorithms can become as routine as relational aggregations by making traversal cheap enough from the storage layer up."

The category is not "graph database clone." The better category is:

- storage-specialized graph analytics runtime;
- low-RAM graph OLAP;
- graph execution layer for cloud-cost-sensitive workloads;
- eventually, graph-shaped retrieval infrastructure for AI/code/enterprise memory.

## What SPC already knows from the application

The application was framed as Founder Fellowship:

```text
Founder Fellowship: I'm starting or started a company and am ready to pitch and fundraise
```

The core application materials said:

- You are solo.
- You have not raised funding or actively fundraised for Knight Walker yet.
- You are building the technical proof before fundraising.
- You are based in Bengaluru.
- The source was Prateek Mehta's X post about building what only you can build.
- The main artifact is the public MIT-licensed Rust repo.
- The strongest supporting artifacts are the two public X thesis threads and the Apache Iggy PR.

Primary links:

- Knight Bus Graph Walker repo: <https://github.com/that-in-rust/knight-bus-graph-walker>
- Storage-layer-up / Devin Ambassador thread: <https://x.com/amuldotexe/status/2073247774674710970>
- RAM-heavy graph OLAP thesis thread: <https://x.com/amuldotexe/status/2068194152941326836>
- Apache Iggy PR: <https://github.com/apache/iggy/pull/2815>
- SPC application page: <https://www.southparkcommons.com/apply>
- Prateek Mehta SPC post: <https://x.com/prateekmehta42/status/2079148824082432163>

## Official interview rubric from the email

The email says SPC wants to cover three areas. This should control the interview prep more than generic founder-pitch instincts.

### 1. Team dynamic

Email prompt:

```text
Tell us how you divide responsibilities and highlight the experiences that make your team special. Please don't recite your LinkedIn or career chronologically.
```

Since you are solo, answer the team question directly instead of pretending there is a team:

```text
Right now I am a solo founder. The "team dynamic" is mostly a deliberate operating model: I am using AI-native engineering workflows to expand my surface area while keeping final judgment and review with me.

My special advantage is the combination, not a chronological resume. I have product/customer judgment from analytics, games, mobility, and enterprise B2B; I have recently built the systems muscle through Rust OSS, Apache Iggy, Parseltongue, and Knight Walker; and I use Devin/Codex-style workflows to move faster without giving up manual review of control-flow, data-flow, and product sense.

The first hires would not be generic engineers. I would recruit around the exact missing surfaces: a Rust/storage-systems person, a graph algorithms/data-infra person, and a design-partner/customer-development person in a graph-heavy domain.
```

Keep this answer under 90 seconds. The trick is to say "solo" cleanly, then turn the question into founder-market fit and operating leverage.

### 2. Ideation

Email prompt:

```text
What problems do you want to solve? What initial solutions have you already invalidated, if any? What is your process for generating and validating new ideas?
```

Answer shape:

```text
The problem I want to solve is that graph-shaped questions are valuable but not routine because graph OLAP can be too RAM-heavy and operationally expensive.

The first solution I am testing is storage-specialized graph walking: dense ids, offsets, CSR-style adjacency, and mmap-backed traversal so algorithms can work closer to the persisted layout rather than loading huge state into runtime RAM.

What I have already invalidated, or at least deprioritized, is starting with a broad graph database replacement or a UI/query-layer wrapper. Those are too wide and do not attack the deepest cost structure first. The sharper wedge is traversal economics.

My ideation process is to look for repeated pain patterns across domains, reduce them to a systems bottleneck, build a narrow proof, compare against a trusted baseline, and then use design partners to decide which market wedge is urgent enough.
```

Potential "invalidated" list:

- A broad `Neo4j replacement` is too wide for the first wedge.
- A prettier graph UI does not solve the cloud-cost bottleneck.
- Pure AI/codebase intelligence may be a compelling application, but the deeper reusable layer is cheaper graph traversal.
- Benchmark-only work is not enough; the next phase needs design partners.

### 3. Next steps

Email prompt:

```text
If you receive funding, what do you want to accomplish over the next couple months? What hypotheses do you want to validate? How would you spend your time, and why can't you do that now?
```

Answer shape:

```text
Over the next couple of months, I would use funding to turn Knight Walker from a narrow graph-walk POC into a design-partner-ready benchmark surface.

The technical milestone is to expand beyond the current walk path into the top graph algorithms that cover most practical graph OLAP use cases, and test whether a meaningful 50GB-class dataset can run on a 16GB RAM CPU machine instead of requiring a 128GB RAM server.

The market milestone is to speak with design partners in fraud/risk, recommendations, code intelligence, supply-chain analytics, and AI knowledge graph teams, and identify the first workflow where low-RAM traversal is urgent rather than merely impressive.

I can keep building slowly now, but funding buys focused time, benchmark infrastructure, possibly a small amount of intern/research help, and the credibility to run a serious design-partner process instead of treating it as a side project.
```

Hypotheses to name:

1. Low-RAM graph traversal is valuable enough to change adoption behavior, not merely benchmark aesthetics.
2. The storage-layer-up approach generalizes from current graph walking to the top practical graph algorithms.
3. One initial segment has urgent enough pain to become the first wedge.
4. An MIT-licensed runtime can be monetized through hosted infrastructure, enterprise support, benchmarking credibility, or workflow-specific commercial surfaces.

## The application thesis, distilled

### Situation

Graph-shaped workloads are becoming more common across fraud, recommendations, dependency analysis, supply-chain analytics, risk, AI/code graphs, and enterprise knowledge workflows.

### Complication

Graph OLAP is often too RAM-heavy for routine cloud use. Teams may know graph algorithms are useful, but the memory footprint, cost anxiety, and operational fragility make them avoid graph workloads or reserve them for special projects.

### Question

Can graph execution be redesigned so traversal becomes cheap enough to use as default infrastructure?

### Answer

Knight Walker attacks the cost problem from the storage layer up by making graph storage mirror graph-algorithm access patterns. The Rust POC uses dense ids, sorted keys, offsets, contiguous peer slices, forward/reverse CSR-style adjacency, and mmap-backed traversal; on the tracked 2GB dataset it returned the same answers as Neo4j while using 4.5x lower runtime RAM.

## Why SPC

The strongest answer is not "I need money." It is:

```text
I need the right community to pressure-test whether this can become a company.
```

More complete version:

```text
I think the hard part is not only building the runtime. The hard part is finding the wedge where people actually adopt it.

I can keep pushing the technical POC forward. But I want SPC because I need high-quality founder and operator feedback on GTM, adoption, design partners, OSS monetization, and whether this is a venture-scale company or a powerful but narrow infrastructure project.

I have already attended SPC India events and found the room talent-dense. For this idea, that matters because the next bottleneck is judgment: who has this pain, how acute it is, where the first design partners should come from, and how to convert technical proof into adoption.
```

## What you should ask SPC for

Ask for specific help. Specific asks make you sound more fundable.

1. Help finding design partners in graph-heavy domains:
   - fraud/risk teams;
   - recommendations/personalization teams;
   - code intelligence / AI coding infrastructure teams;
   - supply-chain analytics teams;
   - enterprise knowledge graph / search teams.
2. Help deciding the first market wedge:
   - low-RAM graph OLAP;
   - codebase intelligence;
   - fraud/recommendations;
   - personal/enterprise knowledge graph retrieval;
   - cloud-cost reduction for graph workloads.
3. Help thinking through OSS adoption:
   - MIT-licensed runtime;
   - hosted/commercial layer;
   - support/enterprise contracts;
   - benchmarking credibility;
   - developer trust.
4. Help with fundraising readiness:
   - what evidence is enough before raising;
   - which benchmark/design-partner proof matters;
   - whether this is seed-stage or still `-1` exploration.

## Likely interview questions and crisp answers

### 1. Why is this a company, not a library?

Best answer:

```text
Right now it is a technical proof, and I am honest about that. The company question depends on whether the pain is acute enough in one wedge where graph execution cost blocks adoption.

My hypothesis is that if traversal becomes cheap enough, several markets open up: fraud/risk, recommendations, code intelligence, supply-chain analytics, and AI-native knowledge graphs. The first company milestone is not to replace Neo4j broadly. It is to find a workflow where low-RAM graph walking is the difference between "we can run this routinely" and "this is too expensive or fragile."
```

### 2. Why won't Neo4j or another graph database just do this?

Best answer:

```text
They might improve parts of this, and I should assume serious incumbents are smart. My wedge is not that incumbents are incompetent. My wedge is that I am starting from a very specific constraint: make traversal storage look like the algorithm access pattern, and optimize for low-RAM cloud execution from the beginning.

Incumbents also have product, compatibility, and revenue constraints. A new runtime can be opinionated and benchmark-led around a narrower path first.
```

### 3. What is the strongest proof so far?

Best answer:

```text
The strongest proof is narrow but concrete: the public Rust POC returns the same answers as Neo4j on tracked fixed corpora, and on the tracked 2GB dataset it used 4.5x lower runtime RAM on the walk path.

I should not overclaim this as a full database replacement. Neo4j still has advantages, including cold-start/opening behavior and mature product surface. But the storage-layer thesis has crossed the threshold where it is worth expanding across more algorithms and workloads.
```

### 4. What is the next milestone?

Best answer:

```text
Expand the POC surface area from the current graph-walk proof into the top graph algorithms that cover most practical graph OLAP use cases. I have already identified the top seven algorithms that likely account for a large share of Neo4j-style graph analytics usage.

The milestone is: for a meaningful 50GB-class dataset, prove that important graph workloads can run on a 16GB RAM CPU machine instead of needing a 128GB RAM server, with reproducible benchmarks and same-answer validation against Neo4j or another baseline.
```

### 5. Who is the customer?

Best answer:

```text
I am still choosing the wedge. The likely early users are teams who already know graph questions are valuable but find them too expensive or operationally risky: fraud/risk, recommendations, code intelligence, supply-chain analytics, and AI knowledge graph teams.

I want SPC's help here because choosing the first customer segment is the most important company-building decision now.
```

### 6. Why are you the right person?

Best answer:

```text
I have an unusual combination for this problem. Analytics taught me how data gets stored, denormalized, aggregated, and consumed by models and humans. Games and Gojek taught me behavioral systems and telemetry. Target taught me enterprise adoption and workflow constraints. Rust OSS and Knight Walker gave me the systems execution proof.

Most deep infra people optimize for elegance and latency. I care about that, but I also care about whether the planet adopts the thing. My edge is that I can combine the systems work with customer empathy and GTM curiosity.
```

### 7. Why now?

Best answer:

```text
Two things changed.

First, graph-shaped workloads are growing because AI, code intelligence, agent memory, fraud, risk, and knowledge systems all involve relationships rather than only rows.

Second, LLMs let a product-minded founder like me get deeper into systems implementation than would have been practical earlier. I am not outsourcing judgment to agents, but I am using Devin/Codex-style workflows to increase the surface area I can personally explore.
```

### 8. What could kill this?

Best answer:

```text
Three things.

One, the pain may be real but not urgent enough in a first market. Two, the benchmark advantage may narrow as workloads broaden. Three, the product may need to become too close to a full graph database before adoption happens.

That is why the next phase has to be design-partner-led and benchmark-led. I want to find the smallest painful workflow where low-RAM traversal changes the economics.
```

## The most important caveats to preserve

These caveats make you sound more trustworthy, not weaker:

- Knight Walker is currently a Rust POC, not a finished database.
- The `4.5x lower runtime RAM` claim is scoped to the tracked 2GB dataset and the walk path.
- Neo4j may still open some datasets faster; do not claim universal superiority.
- The company wedge is still being discovered.
- You are solo and have not fundraised yet.
- You want help with adoption and GTM, not only technical advice.

## What not to say

Avoid these shapes:

- "Neo4j is dumb." Better: "Incumbents have compatibility/product constraints; I am starting from a narrower low-RAM traversal constraint."
- "This will reduce cloud cost 10x" as a current proof. Better: "This is the long-term ambition; the current proof is 4.5x lower runtime RAM on a tracked 2GB walk benchmark."
- "I do not need technical help." Better: "I can push the POC, but I want sharper systems/design-partner feedback."
- "I just need funding." Better: "I need talent-dense judgment on the first wedge, adoption path, and fundraising readiness."
- "It is not a large company." Better: "I want to discover whether there is a venture-scale path; my bias is to build a small, high-leverage team unless the market pull justifies more."

## Interview strategy for the official 15-minute slot

This is a very short slot. Do not wander, and do not spend time asking them to explain the program. Their email explicitly says they will not spend time on program introduction or program Q&A live.

### Minute 0-2: establish the thesis

- One-liner.
- Pain point.
- Technical wedge.
- Current proof.

### Minute 2-5: answer team dynamic

- Solo founder, said plainly.
- AI-native operating model.
- Special combination, not chronological resume.
- First hires by missing surface area.

### Minute 5-9: answer ideation

- Problem to solve.
- Solutions invalidated/deprioritized.
- Validation process.
- Why the wedge is traversal economics.

### Minute 9-13: answer next steps

- Two-month technical milestone.
- Design-partner milestone.
- Hypotheses to validate.
- Why funding/SPC changes speed and quality.

### Minute 13-15: close with one ask

```text
The main thing I want from SPC is help finding the right first wedge and design partners. If low-RAM graph traversal is valuable, I want to find the first market where it becomes urgent, not merely impressive.
```

If there is time, ask one pressure-test question:

```text
If you were evaluating this as a company, which part would you pressure-test first: customer pain, technical defensibility, or GTM/adoption?
```

## Questions to ask Harshit and Prateek

Use 2-3, not all.

1. "When you see infra founders at this stage, what evidence separates an interesting technical POC from a company-worthy wedge?"
2. "Which first market would you pressure-test for a low-RAM graph runtime: fraud/risk, recommendations, code intelligence, supply-chain analytics, or AI knowledge graphs?"
3. "For OSS infrastructure, what do you think is the fastest path to credible adoption: benchmarks, design partners, developer community, or a hosted commercial surface?"
4. "If you were me, what would you try to prove before raising?"
5. "Who are the 3-5 people or teams you would want me to talk to first?"
6. "What would make you pass on this idea even if the technical benchmark keeps improving?"

## Source evidence map

| Claim | Source |
|---|---|
| Founder Fellowship application path | `A04-Browsed-Notes-202607/A008-spc-founder-application-draft.md` and `A04-Browsed-Notes-202607/A010-spc-submission-source-draft.md` |
| Main project is Knight Bus Graph Walker | `A04-Browsed-Notes-202607/A010-spc-submission-source-draft.md` |
| Storage-layer-up thesis | `A04-Browsed-Notes-202607/A009-knight-walker-founder-pitch.md` and X thread `2073247774674710970` |
| RAM-heavy graph OLAP pain | `A04-Browsed-Notes-202607/A008-spc-founder-application-draft.md` and X thread `2068194152941326836` |
| 4.5x lower runtime RAM on tracked 2GB dataset | `A04-Browsed-Notes-202607/A008-spc-founder-application-draft.md`, `A009`, `A010` |
| Amul's product-engineering arc | `A06-Amul-Job-Search/zz-archive-01/A001-product-engineer-profile.md` |
| Apache Iggy PR as Rust OSS proof | `A04-Browsed-Notes-202607/A008-spc-founder-application-draft.md`; PR link `https://github.com/apache/iggy/pull/2815` |
| SPC why / help sought | `A04-Browsed-Notes-202607/A010-spc-submission-source-draft.md` |
| Interview logistics | Google Calendar search for `SPC`, checked 2026-07-31 |
| Official interview areas | Screenshot of Gmail email `SPC Founder Fellowship Interview - Amul Badjatya`, received 2026-07-29 |

## Last-mile prep checklist

Before 2026-08-05 2:00 PM IST:

- [ ] Rehearse the three official sections: team dynamic, ideation, next steps.
- [ ] Open the Knight Bus Graph Walker README and make sure the benchmark wording is fresh in your head.
- [ ] Prepare a one-screen demo or terminal output if they ask to see the repo.
- [ ] Decide whether to consistently say `Knight Walker` or `Knight Bus Graph Walker`. Recommendation: say `Knight Walker` verbally; use `Knight Bus Graph Walker` for the repo.
- [ ] Write down the top seven graph algorithms you believe cover 80-90% of graph OLAP use cases.
- [ ] Prepare one design-partner target list with 10 candidate teams/companies.
- [ ] Prepare a crisp answer for "why this is venture-scale."
- [ ] Prepare a crisp answer for "what if this is just a useful OSS library?"
- [ ] Keep the Zoom link from the calendar ready, but do not paste it into public notes.

## Tiny rehearsal card

```text
I am not trying to clone Neo4j.
I am testing whether graph traversal can become cheap enough to be routine cloud infrastructure.

The wedge is storage layout:
dense ids + offsets + CSR-style adjacency + mmap-backed walking.

The proof is narrow but real:
same answers as Neo4j on tracked corpora;
4.5x lower runtime RAM on the tracked 2GB walk benchmark.

The founder-market fit is my arc:
analytics → telemetry/product systems → enterprise workflows → Rust OSS → AI-native systems building.

The ask from SPC:
help me find the first painful wedge, design partners, and OSS-to-company adoption path.
```
