# SPC Founder Interview Prep V7 - Evidence-Honest GTM

Created: 2026-08-05  
Primary objective: update the SPC Founder Fellowship prep narrative for Knight Walker / Knight Bus Graph Walker using the new customer-evidence dossier and evidence matrix.  
Variant basis: `A006-spc-founder-interview-prep-v6.md`, `/Users/amuldotexe/Downloads/graph-compute-customer-evidence-dossier.md`, and `/Users/amuldotexe/Downloads/graph-compute-evidence-matrix.xlsx`.  
Style target: Shreyas Doshi-style product judgment: precise customer, precise pain, precise wedge, precise invalidations, and no claim that collapses under one smart follow-up question.

## What v7 changes

V6 said:

```text
The wedge is quote-before-run graph compute.
```

The new evidence says that is directionally right but competitively incomplete.

Neo4j GDS already has memory estimation. Neo4j Aura Graph Analytics estimates and provisions an isolated in-memory session. Amazon Neptune Analytics and other systems also expose capacity estimates, limits, or controls. Kuzu already occupies part of the "embedded, disk-based graph analytics" position. GraphChi and related systems already proved much of the disk-first technical lineage.

Therefore, v7 should not say:

```text
Nobody gives pre-run estimates.
```

V7 should say:

```text
The market already estimates graph memory. Knight Walker's bet is to turn estimation into an enforceable workload contract.
```

The sharper v7 thesis:

```text
Knight Walker is bounded graph compute for teams that already have graph artifacts but do not trust the run.

Existing systems validate the pain by exposing estimates, guards, capacity controls, and serverless provisioning. But the unresolved product gap is a portable and enforceable contract: declare a hard budget, estimate the full working set, choose fit/spill/approximate/refuse before execution, and return a receipt afterward with actual memory, I/O, runtime, checksum, and estimator error.
```

The v7 phrase to remember:

```text
Incumbents estimate how much memory to provision.
Knight Walker should make the estimate enforceable.
```

## Evidence snapshot

The new evidence matrix contains:

| Evidence dimension | Count |
|---|---:|
| Total evidence rows | 65 |
| Grade A, high-confidence evidence | 26 |
| Grade B, strong public evidence | 16 |
| Grade C, directional evidence | 21 |
| Grade D, weak / biased evidence | 2 |
| Supporting rows | 46 |
| Mixed rows | 11 |
| Counterevidence rows | 8 |

The important interpretation:

```text
The evidence supports the pain.
The evidence also kills the lazy version of the differentiation.
```

That is good for the interview. SPC will probably prefer the founder who says, "My evidence made my claim more precise," over the founder who ignores obvious competitors.

## The governing thought

```text
Knight Walker should not sell "graph compute without estimates."
It should sell "graph compute with an enforceable budget."
```

Everything else follows from that:

1. The customer pain is not abstract love of graphs; it is fear of failed or expensive graph runs.
2. The incumbent validation is not bad news; it proves customers want memory estimation and capacity control.
3. The defensible wedge is stronger than estimation: hard-budget enforcement, spill/refusal/approximation semantics, portable manifests, and receipts.
4. The first ICP should be security/dependency/access-path teams because their graph-shaped work is concrete, high-stakes, and publicly evidenced.

## The 20-second SPC pitch

```text
Knight Walker is bounded graph compute for teams that already have graph artifacts. Existing products validate the need by estimating graph memory before provisioning, but estimation alone is not the contract I want. Knight Walker's promise is stricter: set a hard budget, get a fit/spill/approximate/refuse decision before execution, and receive a receipt after the run. The first wedge is dependency and security graphs where a failed traversal can block an investigation or release.
```

## The 60-second SPC pitch

```text
I started with a simple low-RAM graph-runtime thesis, but the customer evidence made the story sharper.

The market already knows memory estimation matters. Neo4j GDS has estimate procedures and memory guards. Aura Graph Analytics estimates and provisions an isolated in-memory session. AWS Neptune Analytics has capacity controls. That validates the pain, but it means "quote before run" alone is not a company.

The gap I want to test is an enforceable graph-workload contract. The user brings a portable graph artifact and declares a hard budget. Knight Walker estimates the graph representation plus the algorithm's auxiliary state, then decides: fit, spill, approximate, or refuse. After execution, it returns a receipt: actual peak memory, retained memory, I/O, spill volume, wall time, checksum, and estimator error.

So the wedge is not "we have estimates." The wedge is: the estimate becomes a promise the runtime must obey.
```

## SCQA: the evidence-honest narrative

### Situation

Graph algorithms are useful for real work:

- security path analysis;
- dependency and SBOM blast radius;
- fraud/entity resolution;
- infrastructure topology;
- recommendations;
- GraphRAG and AI/codebase memory;
- centrality and community detection.

The market also already understands that graph memory is hard. This is why mature systems expose estimators, memory guards, capacity controls, and provisioning choices.

### Complication

The painful customer experience remains fragmented.

Users may see:

- an algorithm estimate that does not cover total process memory;
- an in-memory projection that must fit before the algorithm can run;
- a guard that warns but does not guarantee completion;
- platform ceremony around provisioning, monitoring, backup, licensing, and procurement;
- GPU or distributed-system requirements that make the workflow too heavy;
- ingestion, traversal, timeout, or memory-limit failures in real security graphs.

The new evidence therefore creates a sharper question:

```text
If estimation exists, what is still missing?
```

### Question

What is the Knight Walker wedge that remains defensible after accounting for Neo4j GDS, Aura Graph Analytics, Neptune Analytics, Kuzu, GraphFrames, cuGraph, GraphChi, and other systems?

### Answer

```text
The wedge is an enforceable portable graph-workload contract.

Not "we estimate."

"You declare a hard budget. We estimate the full working set. We choose fit/spill/approximate/refuse. We enforce the plan. We prove what happened with a receipt."
```

## The central narrative correction

### Unsafe version

```text
Graph algorithms still do not have pre-run certainty. Knight Walker will quote jobs before execution.
```

Why unsafe:

- Neo4j GDS has `.estimate` procedures and automatic memory guards.
- Aura Graph Analytics estimates memory and provisions a serverless in-memory session.
- Neptune Analytics and other systems have capacity controls or limits.
- Kuzu and DuckPGQ already validate embedded graph-adjacent positioning.

### Evidence-honest version

```text
Existing graph systems increasingly estimate memory because customers hate capacity guesswork. But an estimate is not the same as an enforceable, portable workload contract.

Knight Walker should make the estimate executable: hard budget, bounded plan, spill/refusal/approximation, and receipt.
```

### Shreyas-style version

```text
The insight is not "we discovered estimation."
The insight is "estimation without enforcement does not fully change behavior."

Customers do not only need a number. They need a contract they can trust enough to run the job, schedule the job, or let an agent plan around refusal.
```

## The strongest differentiation sentence

Use this exact line:

```text
Incumbents estimate how much memory to provision; Knight Walker should make the estimate enforceable.
```

If given one more sentence:

```text
The product is not a better warning label. It is a runtime contract: fit, spill, approximate, or refuse under a declared budget, then return a receipt.
```

## What the evidence supports

### 1. Memory roulette is real, but it is ecosystem-wide

The evidence supports memory roulette across multiple systems, not as a Neo4j-only dunk.

Examples from the dossier:

- Neo4j GDS users hit in-memory projection limitations and staff confirm no spill-to-disk for GDS projection/algorithm execution in the cited thread.
- Neo4j documentation validates `.estimate` procedures and guards, while also noting that passing memory control does not guarantee successful completion without memory depletion.
- Amazon Neptune Analytics documents operations that are not memory bounded and may OOM depending on capacity and dataset shape.
- TigerGraph staff gives algorithm-dependent RAM guidance, including around 2x data size and around 3x for Louvain-style workloads.
- NVIDIA cuGraph staff identify auxiliary-memory fit as a source of "not obvious beforehand" failure.
- Memgraph, Dgraph, and ArangoDB evidence points to memory growth, OOM, approximate limits, or incomplete tracking in different forms.

The safe conclusion:

```text
Graph working sets are representation- and algorithm-dependent, and users often cannot infer total process risk from the input artifact alone.
```

### 2. Pre-run estimation is validated demand, not empty whitespace

This is the big correction.

The dossier's strongest competitive fact:

```text
Pre-run memory estimation already exists in Neo4j GDS, Neo4j Aura Graph Analytics, Amazon Neptune Analytics, and other systems' resource controls.
```

The implication:

```text
If customers did not care about memory estimation, incumbents would not expose it.
```

This is not a problem for Knight Walker if the pitch is precise. It is a problem only if the pitch pretends no one estimates.

The safe conclusion:

```text
Customers already value estimates, guards, provisioning, and capacity controls. Knight Walker must make the next step credible: estimator accuracy, explicit error bars, total-working-set accounting, enforcement, portable manifests, and receipts.
```

### 3. Some teams want graph-shaped capability without graph-platform appetite

The strongest production evidence here is the Trendyol migration from Neo4j Enterprise to Apache AGE/PostgreSQL.

The useful learning is not "PostgreSQL beats Neo4j." The evidence is subtler:

```text
Neo4j worked technically, but a separate graph platform introduced licensing, procurement, monitoring, backup, pooling, and operational ceremony. Trendyol accepted slower variable-depth traversals and custom fixed-depth optimization to consolidate into PostgreSQL.
```

This matters because it shows the customer preference:

```text
Some teams want graph capability without adopting and operating a separate graph platform.
```

Cisco's Crosswork release note also says topology graph data moved from Neo4j to PostgreSQL for performance and stability, though it does not explain the full workload or trade-offs.

The safe conclusion:

```text
There is a real class of teams whose graph-shaped work survives, but whose appetite for a separate graph platform is low.
```

### 4. Security and access-path workflows are the best evidenced first ICP

BloodHound is the closest public analogue to the first ICP.

The dossier cites public issues involving:

- default shortest-path query timeout on a dataset with roughly 351k relationships and 225k ACLs;
- an OpenGraph artifact with roughly 240k edges taking almost 11 hours to ingest;
- API memory-limit problems and incomplete metrics on large Entra ID datasets;
- SharpHound collection stalling beyond 15 hours;
- requests for multi-gigabyte artifact uploads;
- progressive first-degree previews;
- PostgreSQL multi-hop query slowness.

The safe conclusion:

```text
Security graph workflows contain concrete, high-stakes situations where bounded execution, early refusal, progressive results, and receipts could matter.
```

The caution:

```text
The buyer may not care about a general graph runtime. They may simply want BloodHound-specific ingest/query fixes. The design-partner work must separate these.
```

### 5. The DuckDB moment has competitors and ancestors

V6 used the "DuckDB moment" framing. The evidence keeps it, but makes it more humble.

Important corrections:

- Kuzu already describes itself as embedded, columnar, disk-based graph analytics for analytical workloads and very large graphs.
- DuckPGQ brings graph querying into DuckDB.
- GraphFrames/Spark supports memory-and-disk persistence and checkpointing.
- GraphChi, GraphChi-DB, FlashGraph, BigSparse, and related systems already established the storage-first lineage.

The safe conclusion:

```text
The opportunity is not "nobody thought of disk-backed graphs."

The opportunity is to package a narrow iterative graph-algorithm workflow around bounded execution, estimator accuracy, and receipts.
```

## The product contract v7 supports

The receipt must become testable, otherwise it is just rhetoric.

### Before execution

The user should receive:

- graph artifact format and version;
- manifest checksum;
- node, edge, property, and index counts;
- graph representation bytes;
- algorithm and parameters;
- estimated fixed state;
- estimated per-node state;
- estimated per-edge state;
- estimated frontier / queue state;
- estimated output state;
- estimated conversion / projection state;
- estimate range and confidence;
- selected plan: in-memory, spill, approximate, or refuse;
- declared hard RSS or cgroup memory ceiling;
- expected disk I/O and temporary storage;
- expected runtime range, or an explicit "runtime not estimable yet."

### During execution

The runtime should expose:

- enforced memory high-water mark;
- phase-level progress;
- bytes read;
- bytes written;
- spill volume;
- cancellation or refusal reason;
- cold-cache versus warm-cache indicator.

### After execution

The user should receive:

- actual peak RSS;
- heap, off-heap, mapped pages, and retained memory where available;
- estimator absolute error;
- estimator percentage error;
- wall time;
- CPU time;
- output cardinality;
- output checksum;
- approximation quality or error bound if applicable;
- reproducible manifest and engine version.

The interview line:

```text
A receipt is not a PDF. It is a trust primitive.
```

## The ICP ranking after evidence

V6 ranked AI/codebase/SBOM/security as the first wedge. The new evidence strengthens the security/access-path side.

| Rank | ICP / wedge | Evidence support | Why it fits | Main caution | V7 call |
|---:|---|---|---|---|---|
| 1 | Security, IAM, dependency, SBOM, and access-path graphs | Strongest public analogue from BloodHound issues; concrete timeouts, ingest delays, large artifacts, memory limits, and result explosion. | High-stakes graph questions; bounded traversal and early refusal can change investigations/releases. | Pain may be product-specific to BloodHound pipelines rather than general graph compute. | Lead wedge for design-partner discovery. |
| 2 | Codebase intelligence / AI-native dependency graphs | Strong founder-market fit through Parseltongue and Knight Walker; easy to generate artifacts; short feedback loops. | Lets Amul demo without enterprise data access; naturally combines graph compute with AI-native code review. | Buyer urgency and budget may be weaker than security. | Use as demo wedge and personal unfair advantage. |
| 3 | Existing Neo4j/GDS users with too-large projections | Strong direct pain from Neo4j forum evidence. | Clear "slower but bounded is better than impossible" message. | Extreme workloads may not map to first buyer; migration/export friction may dominate. | Interview/research wedge, not first product beachhead. |
| 4 | Fraud / entity-resolution teams | Strong graph-algorithm relevance: WCC, similarity, PageRank, Louvain. | High ROI if it works. | Data access and procurement slow the learning loop. | Later paid design-partner wedge. |
| 5 | Generic GraphRAG / AI memory | Strong why-now narrative but not directly proven by dossier. | Agents may create more graph-shaped demand. | Evidence validates execution pain, not agent-driven demand. | Use as macro thesis only. |

The Shreyas-style judgment:

```text
Start where the pain is public, specific, and artifact-shaped.
Do not start where the narrative is largest.
```

## Recommended first wedge

The evidence-backed first wedge:

```text
Security and dependency teams with graph-shaped blast-radius, access-path, and artifact-analysis workflows.
```

The demo wedge:

```text
Given a BloodHound-like, SBOM-like, repo-dependency, service-topology, or IAM/access graph artifact, run a bounded path/component/centrality job with a pre-run contract and post-run receipt.
```

The customer sentence:

```text
My first customer is the team that already has a graph artifact and a high-stakes question, but today either overprovisions, times out, or avoids running the analysis because it cannot predict memory/runtime behavior.
```

The design-partner ask:

```text
Give me one artifact, one graph question, the machine you would run it on, and the failure mode you fear.
```

## Competitive framing to use in the interview

### Versus Neo4j GDS / Aura Graph Analytics

```text
Neo4j validates the customer need by estimating memory before graph analytics work. I should not claim to invent estimation.

The differentiation I want to test is stricter: a portable artifact, a declared hard budget, disk-backed or approximate plans where needed, explicit refusal semantics, and a receipt showing estimate-versus-actual behavior.
```

### Versus Amazon Neptune Analytics

```text
Neptune Analytics validates that cloud graph analytics needs capacity controls. But my wedge is not managed cloud capacity selection. It is a portable workload contract that can run in a small CPU/container/local environment and report what happened.
```

### Versus Kuzu

```text
Kuzu already validates the embedded, disk-based graph direction. Knight Walker must not pretend that local graph analytics is empty space.

The narrower question is whether iterative graph algorithms can get a better bounded-execution contract: working-set estimates, hard ceilings, spill/refusal/approximation, and receipts.
```

### Versus DuckPGQ / DuckDB

```text
DuckPGQ validates graph querying inside the DuckDB ecosystem. Knight Walker's wedge is not graph pattern matching alone; it is bounded algorithm execution for practical graph workloads.
```

### Versus GraphFrames / Spark

```text
Spark can spill and checkpoint, but it brings distributed-system ceremony. Knight Walker should make specific graph-algorithm jobs routine on a CPU box or small container.
```

### Versus cuGraph

```text
cuGraph is powerful when the GPU path fits. The customer I want first may not want to buy GPU memory or set up multi-GPU workflows to answer a security or dependency traversal.
```

### Versus NetworkX

```text
NetworkX has the local developer experience people love, but Python object overhead creates a scale ceiling. Knight Walker should target the next rung: local feeling, systems-level bounded execution.
```

### Versus GraphChi / FlashGraph / BigSparse

```text
The storage-first technical lineage exists. The missing product is maintained, adopted, evidence-honest, and receipt-driven.
```

## Claim audit for your mouth

Do not let these claims escape unqualified in the interview.

| Tempting claim | Verdict | Say this instead |
|---|---|---|
| Neo4j has no pre-run memory estimate. | Reject | Neo4j GDS and Aura Graph Analytics provide memory estimation; the gap I want is enforceable, portable workload contracts. |
| A successful estimate guarantees the job will fit. | Reject | Neo4j documentation itself warns that clearing a memory guard does not guarantee completion without memory depletion. |
| The market has no estimate-before-run graph product. | Reject | The market has estimates and capacity controls; the open question is enforcement, portability, spill/refusal semantics, and receipts. |
| No embedded larger-than-memory graph system exists. | Reject | Kuzu and others validate the area; Knight Walker needs a narrower algorithm/receipt wedge. |
| Disk-based graph computation is technically novel. | Reject | GraphChi and successors proved the lineage; the novelty must be product contract and packaging. |
| Customers experience graph memory roulette. | Supported with scope | Public users across Neo4j, TigerGraph, Memgraph, Dgraph, Neptune, and cuGraph report capacity-sensitive OOMs, long stalls, or hard sizing trade-offs. |
| Security/access-path teams have concrete pain. | Supported | BloodHound issues show timeouts, slow ingest, memory-limited results, large artifacts, and collection hangs. |
| The receipt is already a paid category. | Unproven | The sources validate estimation and memory pain; I still need to test whether customers pay for receipt-grade certainty. |
| Agents create the market. | Plausible but not proven here | Agents are my why-now hypothesis; this evidence validates execution pain, not agent-driven demand growth. |
| 50GB on 16GB proves the company. | Internal milestone only | It proves technical credibility only if paired with real artifacts and a decision-changing receipt. |

## What to say if SPC challenges the evidence

### If they say: "Doesn't Neo4j already do estimates?"

Answer:

```text
Yes, and that is exactly the correction my evidence forced.

Neo4j GDS has estimate procedures and Aura Graph Analytics estimates capacity before provisioning. I should not claim Knight Walker invents estimates.

The gap I want to test is whether estimates can become enforceable workload contracts: hard budget, total-working-set accounting, fit/spill/approximate/refuse, and post-run estimator error. The question is whether that changes behavior for a narrow ICP.
```

### If they say: "Isn't Kuzu already DuckDB for graphs?"

Answer:

```text
Kuzu validates the embedded graph direction. That is useful counterevidence.

I would not differentiate by saying "local graph database." The narrower differentiation is iterative graph algorithms under a declared memory budget with receipts. If Kuzu already solves the first ICP's problem, that invalidates or narrows Knight Walker. That is exactly what I want to learn in design-partner work.
```

### If they say: "Is this just GraphChi with Rust?"

Answer:

```text
GraphChi is part of the lineage. The technical idea that disk can participate in large graph computation is not new.

The product gap is the maintained developer workflow and the enforceable contract: portable graph artifact, estimator, hard ceiling, refusal/spill/approximation, and actual-versus-estimated receipt. The company is not "we discovered disk." The company is "we made bounded graph jobs trusted and routine."
```

### If they say: "Why start with security graphs?"

Answer:

```text
Because the pain is public, concrete, and high-stakes.

BloodHound-like workflows show path-query timeouts, slow ingest, memory-limit failures, large artifacts, and progressive-preview needs. That does not prove Knight Walker is the answer, but it gives me a sharper first ICP than generic GraphRAG.

The design-partner test is whether those teams want a general bounded graph-compute contract or just product-specific pipeline fixes.
```

### If they say: "Why should anyone pay?"

Answer:

```text
That is the biggest open GTM hypothesis.

The evidence proves people care about estimates, guards, capacity, and memory failures. It does not prove they will pay for receipts as a standalone category.

So my first paid test is not a grand platform. It is: can a security/dependency team with a real graph artifact justify paying for a bounded runner or support contract because the receipt changes whether they run the job?
```

## Rewritten interview answers

### Team dynamic

```text
I am applying as a solo founder.

The honest team dynamic is that I am using LLMs and Devin for leverage while I keep the customer and technical learning loop tight. I do not want to hire around a vague category story.

The evidence has actually made me more disciplined. If the bottleneck is algorithm surface area, I add systems help. If the bottleneck is security/devtools GTM, I add customer-facing help. If the bottleneck is production hardening, I add infra help.

But first I need to know which customer segment treats enforceable graph-compute receipts as a workflow-changing product rather than a neat benchmark.
```

### Ideation

```text
The broad idea is that graph algorithms should be as routine as tabular aggregations, but they are not because graph jobs are often memory- and setup-scary.

My earlier version was "low-RAM graph runtime with quote-before-run." The evidence improved that. Pre-run estimation already exists in strong products, so the more precise idea is an enforceable workload contract for portable graph artifacts.

The user declares a budget. Knight Walker estimates full working set, including algorithm auxiliary state and representation/projection costs. Then it must fit, spill, approximate, or refuse before wasting the run. Afterward it returns a receipt with actual-versus-estimated behavior.

The invalidations are important: I am not replacing Neo4j broadly, not claiming graph databases are the next RDBMS, not pretending Kuzu/GraphChi/GraphFrames do not exist, and not saying lower RAM alone is the business.
```

### Next steps

```text
If funded, I would validate three hypotheses over the next couple of months.

First, customer hypothesis: security/dependency/access-path teams are the first ICP because their graph artifacts and failures are concrete enough to test.

Second, product hypothesis: an enforceable pre-run contract and post-run receipt changes behavior more than a benchmark does.

Third, technical hypothesis: Knight Walker can expand from the current proof into the practical algorithm set while preserving hard-budget execution and estimator calibration.

The near-term milestone is not "build the whole graph database." It is one real artifact, one high-stakes graph job, a trusted estimate, bounded execution, and a receipt the user would rely on for the next run.
```

## Falsification plan

V7 should sound more falsifiable than v6.

Ask every design partner for one real graph artifact and one job they avoid, overprovision, or distrust.

### Customer questions

1. What graph-shaped question did you recently want to answer?
2. What graph artifact did you have at the time?
3. What tool did you try?
4. What broke: ingestion, projection, algorithm memory, timeout, output size, trust, cost, or integration?
5. What machine or cloud capacity did you choose, and why?
6. Did the previous system estimate memory? Did you trust it?
7. Would a conservative refusal have helped you plan, or just annoyed you?
8. What estimator error is acceptable: +/-10%, +/-25%, or only a conservative upper bound?
9. Would a post-run receipt change your trust in the next run?
10. Would Kuzu, DuckPGQ, GraphFrames, Neo4j Aura, or a tuned existing workflow already solve this?

### Strong signal

```text
We currently avoid or overprovision this job because we cannot predict it. If your bounded plan and receipt were trustworthy, we would run it regularly.
```

### Weak signal

```text
This benchmark is cool.
```

### Kill signal

```text
The pain is not the graph algorithm; the pain is ingestion, schema design, permissions, UI, or product-specific workflow.
```

## The 6-week learning plan

### Week 1: evidence-backed ICP conversations

Output:

- 15 customer conversations;
- at least 5 security/IAM/dependency conversations;
- a table of avoided graph jobs;
- top 3 failure modes;
- list of available graph artifact formats.

Success:

```text
At least 3 people can provide or describe a real artifact and a graph job they hesitate to run because of memory/runtime/cost/trust.
```

### Week 2: receipt specification

Output:

- a receipt JSON schema;
- a pre-run estimate schema;
- a refusal schema;
- a calibration/error schema;
- a user-readable terminal receipt.

Success:

```text
A design partner says the receipt contains the fields they would need to trust the next run.
```

### Week 3-4: bounded runner prototype

Output:

- one path/component/centrality job on one real or realistic security/dependency artifact;
- hard memory ceiling;
- fit/spill/refuse behavior;
- post-run estimate-versus-actual comparison.

Success:

```text
The prototype refuses or runs under budget rather than failing late.
```

### Week 5: competitor comparison

Output:

- same artifact tested against the closest feasible alternatives: Neo4j/GDS if possible, Kuzu/DuckPGQ where applicable, NetworkX baseline where useful, GraphFrames/Spark if the workflow naturally belongs there.

Success:

```text
The comparison clarifies a real workflow gap rather than only a synthetic benchmark delta.
```

### Week 6: paid pilot / design-partner decision

Output:

- one pilot proposal;
- one ICP decision;
- one explicit "continue / pivot / narrow" memo.

Success:

```text
At least one team says the receipt-grade contract is valuable enough to keep testing, pay, or introduce to another buyer.
```

## What funding buys in v7

Do not frame funding as general runway only.

Say:

```text
Funding buys faster falsification of the enforceable-contract wedge.
```

Concretely:

1. full-time focus on hard-budget runtime work;
2. benchmark and calibration corpus;
3. real graph artifact ingestion paths;
4. receipt schema and estimator calibration;
5. enough customer discovery to choose security/dependency versus other wedges;
6. small helper capacity only after the work becomes parallelizable.

The crisp line:

```text
I do not need funding to write more code in the abstract. I need funding to compress the path to knowing whether a receipt changes customer behavior.
```

## Product shape

The initial product should be narrow:

```text
Artifact-to-answer bounded graph runner.
```

Not:

```text
General graph database.
```

Not:

```text
AI memory platform.
```

Not:

```text
Every graph algorithm.
```

Potential packaging:

1. OSS core runtime for trust and adoption;
2. paid local/pro runner with support;
3. managed receipt runner for private artifacts;
4. enterprise integration packs for security/dependency/IAM artifacts;
5. calibration and workload packs for specific algorithm families.

The monetization hypothesis:

```text
The commercial value is paid certainty: production support, private runners, receipt retention, workload packs, and integrations.
```

## Algorithm priority after evidence

Do not lead with algorithm breadth. Lead with the wedge.

For security/dependency/access-path workflows, the priority set is:

| Priority | Algorithm family | Why |
|---:|---|---|
| 1 | BFS / shortest paths / bounded path search | Directly maps to access paths, blast radius, attack paths, dependency reachability. |
| 2 | WCC / connected components | Finds islands, clusters, entity groups, package/service dependency regions. |
| 3 | PageRank / centrality | Ranks critical nodes, services, identities, repositories, dependencies. |
| 4 | NodeSimilarity / kNN | Finds similar entities, duplicate patterns, shared-device/shared-dependency style structures. |
| 5 | Louvain / Leiden | Community detection for segmentation and graph condensation, but heavier auxiliary state. |
| 6 | Triangles / clustering coefficient | Useful for local density and suspicious structure, but more specialized. |
| 7 | FastRP / embeddings | Useful later for ML features, but less ideal for first receipt demo. |

The line:

```text
I want the practical 80/20 algorithm set, but I want it ordered by the first ICP's job, not by graph-theory aesthetics.
```

## The two-axis moat

The moat is not "Rust."

The moat, if it exists, is the intersection of two axes:

### Axis 1: systems contract

- storage-aware graph representation;
- algorithm-specific working-set model;
- hard memory ceiling;
- spill/refusal/approximation;
- estimate-versus-actual calibration;
- deterministic checksum and receipt.

### Axis 2: adoption contract

- portable artifacts;
- boring local/container execution;
- explicit trust surface;
- OSS inspectability;
- design-partner-specific workload packs;
- no "buy a graph platform first" ceremony.

The line:

```text
The company is not won by being clever at one layer. It is won if the systems contract and adoption contract reinforce each other.
```

## The best v7 answer to "why you?"

```text
I have an unusual fit because I have lived both sides of this problem.

From analytics, I understand how data gets reshaped into forms humans and models can actually consume. From games and product telemetry, I learned how aggregate behavior becomes product insight. From enterprise product at Target, I learned that adoption is often killed by procurement, operations, monitoring, compliance, and unclear ownership—not just by bad technology.

Then I deliberately went deeper technically: Rust OSS, Apache Iggy, product engineering, and Knight Bus Graph Walker.

That matters because this is not only an engine problem. It is an adoption problem. A pure systems person might optimize RAM. A pure PM might write a story. The work here is to make a hard systems contract that customers trust enough to adopt.
```

## The best v7 answer to "why now?"

```text
The why-now has two parts.

First, the demand side is changing. Agents and LLMs make graph-shaped questions cheaper to generate: code graphs, dependency graphs, access paths, AI memory, evidence graphs. I should be honest that the new evidence dossier validates execution pain more directly than it validates agent-driven demand, so this remains a hypothesis to test.

Second, the supply side is more plausible. Rust, mmap, NVMe, cgroups, local SSDs, and modern machines make it more realistic to build a bounded CPU/container graph runner than it was in the GraphChi era.

The combination is interesting: more graph questions, but still insufficient trust in graph execution.
```

## The best v7 answer to "is this a company?"

```text
I do not want to answer that by waving at a huge graph market.

The honest company question is: will one narrow segment pay for enforced graph-compute certainty?

The evidence supports the pain. It does not yet prove the purchase. So the next two months are about whether a security/dependency team with a real artifact changes behavior when offered a hard-budget graph runner with a receipt.

If yes, the expansion path is algorithm families, artifact formats, managed runners, enterprise support, and agent-native graph workflows. If no, I should either narrow to a product-specific tool or treat Knight Walker as infrastructure inside another product.
```

## The best v7 answer to "what have you invalidated?"

```text
I invalidated the lazy version of my own pitch.

I cannot say that no one does estimates. Neo4j GDS, Aura Graph Analytics, Neptune Analytics, and others already validate that need.

I cannot say embedded graph analytics is empty. Kuzu and DuckPGQ are real.

I cannot say disk-backed graph computation is novel. GraphChi and successors established that lineage.

I cannot say 50GB-on-16GB proves the company. It proves only a technical milestone.

What remains is a narrower and better hypothesis: enforceable graph workload contracts might change behavior for teams that already have graph artifacts and fear failed runs.
```

## The one-page rehearsal card

```text
Main thesis:
The market already estimates graph memory. Knight Walker's bet is to turn estimation into an enforceable workload contract.

Do not say:
Nobody estimates graph jobs.

Say:
Incumbents estimate how much memory to provision; Knight Walker should make the estimate enforceable.

Product:
Portable graph artifact + hard budget + full-working-set estimate + fit/spill/approximate/refuse + receipt.

First ICP:
Security/dependency/access-path teams with graph artifacts and high-stakes traversal/blast-radius questions.

Evidence:
65 rows in the matrix; 26 Grade A, 16 Grade B, 21 Grade C, 2 Grade D.
Neo4j validates estimates.
Neo4j GDS projection pain supports memory roulette.
Trendyol/Cisco support graph-shaped pain without graph-platform appetite.
BloodHound supports security/access-path wedge.
Kuzu/DuckPGQ/GraphChi are competitive corrections.

Open risk:
Receipt as paid product is unproven.

Next two months:
15 design-partner conversations, receipt schema, one bounded runner demo, competitor comparison, one paid-pilot test.

Close:
I need SPC to help me avoid building a beautiful runtime for a vague market. I want sharp GTM feedback and design partners who can give real graph artifacts.
```

## Updated one-minute SPC close

```text
The main value I want from SPC is sharper GTM judgment.

The evidence made the thesis more precise. I am not claiming to invent graph memory estimation. The market already estimates. The open question is whether a stricter contract—hard budget, bounded execution, and receipt—changes behavior for a narrow segment.

My current best wedge is security/dependency/access-path graphs because the pain is public and specific. I want SPC's help finding the right design partners, stress-testing whether this is a paid product or just a feature, and keeping me honest about where the adoption loop is shortest.
```

## Final v7 thesis

```text
Knight Walker is not "quote-before-run graphs" in a vacuum.
The market already quotes and estimates in pieces.

Knight Walker is a bet that the next adoption unlock is enforceability:

Declare the budget.
Estimate the full working set.
Fit, spill, approximate, or refuse.
Return the receipt.

The first market is not everyone with graphs.
The first market is teams with graph artifacts whose work is blocked by memory uncertainty and late failure.
```

## Evidence source map

Primary local sources:

- `/Users/amuldotexe/Desktop/TauriAppsOSS/A08-Interview-Pep/A006-spc-founder-interview-prep-v6.md`
- `/Users/amuldotexe/Downloads/graph-compute-customer-evidence-dossier.md`
- `/Users/amuldotexe/Downloads/graph-compute-evidence-matrix.xlsx`

Key public evidence links from the supplied dossier:

- [Neo4j GDS memory estimation](https://neo4j.com/docs/graph-data-science/current/common-usage/memory-estimation/)
- [Neo4j Aura Graph Analytics technical deep dive](https://neo4j.com/blog/aura-graph-analytics/graph-analytics-basics/)
- [Neo4j Aura Graph Analytics product page](https://neo4j.com/product/aura-graph-analytics/)
- [Neo4j community: GDS algorithms without a projection](https://community.neo4j.com/t/gds-algorithms-without-a-projection/73039)
- [Amazon Neptune Analytics service limits](https://docs.aws.amazon.com/neptune-analytics/latest/userguide/analytics-limits.html)
- [TigerGraph memory requirement forum thread](https://dev.tigergraph.com/forum/t/is-there-a-minimum-memory-size-requirement-for-tigergraph/4506)
- [TigerGraph space requirement forum thread](https://dev.tigergraph.com/forum/t/how-much-space-is-required/3870)
- [NVIDIA cuGraph pain points](https://forums.developer.nvidia.com/t/cugraph-pain-points/249667)
- [BloodHound default shortest-path timeout](https://github.com/SpecterOps/BloodHound/issues/106)
- [BloodHound OpenGraph ingest performance](https://github.com/SpecterOps/BloodHound/issues/2415)
- [Memgraph memory growth issue](https://github.com/memgraph/memgraph/issues/2099)
- [ArangoDB memory footprint documentation](https://docs.arango.ai/arangodb/stable/operations/administration/reduce-memory-footprint/)
- [Kuzu documentation](https://kuzudb.github.io/docs/)
- [GraphChi OSDI paper](https://www.usenix.org/system/files/conference/osdi12/osdi12-final-126.pdf)
- [Trendyol migration from Neo4j to Apache AGE](https://medium.com/trendyol-tech/migrating-graph-operations-to-apache-age-from-writes-to-reads-3b8334628e1c)
- [Cisco Crosswork release note mentioning Neo4j to PostgreSQL topology migration](https://www.cisco.com/c/en/us/td/docs/cloud-systems-management/crosswork-network-controller/7-1/Release-Notes/release-notes-for-cisco-crosswork-network-controller-release-7-1-0.html)

Important caveats to preserve:

- Do not claim Neo4j lacks estimates.
- Do not claim estimates guarantee completion.
- Do not claim Kuzu/DuckPGQ/GraphChi do not exist.
- Do not claim security/access-path buyers will pay until design-partner interviews validate it.
- Do not claim agents created the market; treat agent-driven graph demand as a why-now hypothesis.
- Do not treat `50GB on 16GB RAM` as market validation; it is a technical milestone that must be paired with real artifacts and a trusted receipt.
