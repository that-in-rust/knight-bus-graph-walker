# The-One-Question-Left — Final Conclusion And Decision

Date: 2026-07-26
Status: **conclusion.** Not a survey. This file reasons from everything already
established and ends in a decision, probabilities, a three-week test, and kill
criteria. No new research; nothing read to produce it.
Predecessors: `Everything-We-Know-Now.md` (state), `evidence01.md` (facts),
`Real-Pain-Wrong-Product.md` (verdict), `Not-First-Still-Different.md` (priority),
`Win-The-Whales-Vision.md` (enterprise strategy).

---

## 1. The compression

Twenty-seven documents, ~13,000 lines, four months. All of it reduces to one
falsifiable question:

```text
+---------------------------------------------------------------------------+
|                                                                           |
|   IS "OUR ANALYTICS JOB DEGRADED OR TOOK DOWN PRODUCTION" A REAL,          |
|   RECURRING, BUDGET-WORTHY EVENT AT LARGE NEO4J SHOPS?                     |
|                                                                           |
+---------------------------------------------------------------------------+
```

Everything else is downstream of that. If yes, there is a business, the plugin is
the vehicle, and the four things we own are the product. If no, there is a good
open-source contribution and no company.

Why this and not "is the memory wall real" (settled: yes) or "do people complain
about cost" (settled: yes, loudly, but about licensing): **it is the only pain
that is orthogonal to budget.** A 12 TB machine does not prevent a projection from
eating the heap. Money does not buy immunity from co-tenancy. It is therefore the
only pain that exists in the segment that can pay.

Nobody in four months has asked five people this question. It takes three weeks.

---

## 2. Why the evidence looked contradictory: the pain is bimodal

This resolves the argument that ran through this entire engagement — the corpus
kept finding loud complaints and inferring a market; I kept finding the 12 TB
quote and inferring none. **Both readings were right about different populations,
and neither of us separated them.**

```text
                    CAN THEY PAY?
                    NO                          YES
              +---------------------------+---------------------------+
   DO THEY    |  MODE 1: THE BLOCKED      |  MODE 3: THE SQUEEZED     |
   FEEL IT?   |  students, solo devs,     |  ???                      |
   YES        |  GraphRAG builders, labs, |  big enough to hurt,      |
              |  small shops              |  not big enough to shrug  |
              |                           |                           |
              |  LOUD. NUMEROUS.          |  SIZE: COMPLETELY UNKNOWN |
              |  NO BUDGET.               |  <- the entire business   |
              |  -> leave for Postgres,   |                           |
              |     igraph, or give up    |                           |
              +---------------------------+---------------------------+
   NO         |  (graph fits; most users) |  MODE 2: THE INSULATED    |
              |                           |  "some even had 12TB ram" |
              |                           |  buy the box, move on     |
              |                           |  QUIET. FEW. RICH.        |
              +---------------------------+---------------------------+
```

- **Mode 1 explains `simulation01`.** Every loud complaint in that dossier — the
  52 GiB refusal on a 16 GB box, the Aura free tier failing Neo4j's own course,
  "insane licenses", "heart attack" — is Mode 1. Real pain. Real people. No
  purchase order. This is why the turf was empty: **it is not that nobody noticed,
  it is that nobody could monetize it.**
- **Mode 2 explains paul.horn.** Enterprises solve it with capex and never file a
  feature request. This is why Neo4j deprioritized out-of-core, rationally.
- **Mode 3 is the whole bet, and no evidence in this corpus speaks to it.** Zero
  interviews, zero invoices, zero procurement documents.

**The strategic consequence is severe:** every piece of demand evidence we have
is drawn from the mode that cannot pay, and every piece of budget evidence is
drawn from the mode that does not hurt. The corpus mistook Mode 1's volume for
Mode 3's existence.

**And this reframes the interview.** The question is not "do you feel pain" —
everyone says yes. It is *"which mode are you in?"*, which you determine by asking
what they did the last time they hit the wall. *Bought a bigger box* = Mode 2.
*Sampled the graph / gave up / moved to Spark* = Mode 1 wearing an enterprise
badge. *"We're still living with it and it costs us"* = Mode 3 exists.

---

## 3. Why three competitors appeared now — supply, not demand

The convergence has been read as validation. Examined properly, it is closer to a
warning.

```text
  What changed between 2015 (turf empty) and 2026 (three entrants)?

  NOT demand:   the memory wall is 14 years old; GraphChi proved the physics
                in 2012; Neo4j staff still say "never had enough people ask"

  SUPPLY:       - Rust matured: one person can now build a DB engine
                - LLM-assisted coding compressed build time by an order
                  of magnitude (Slater: empty repo -> Bolt engine in ~6 weeks)
                - GraphRAG/agent-memory minted a visible, vocal Mode 1
                  population that LOOKS like a market from the outside
```

**Three teams did not independently discover new demand. Three teams independently
discovered it had become cheap to build.** That is a supply shock, and supply
shocks into unproven demand produce a crowded field of free tools and no revenue —
which is exactly what we observe: Slater, Grafeo and LadybugDB are all
permissively licensed with **no stated monetization** between them.

This has a sharp implication for how we read them. **They are not competitors for
revenue. They are competitors for default status.** They cannot take a customer we
don't have; they can take the *reason for us to exist*, by being the obvious free
answer when someone asks "how do I run Louvain without a big machine?"

---

## 4. The competitors are ahead in code and behind in adoption path

This is the finding that partially rescues the position, and it follows from a
single structural fact.

```text
   EVERY rival replaces Neo4j on the read path.
   Therefore every rival must WIN A DATABASE EVALUATION.

   Database evaluations are decided on:
        maturity | support | ecosystem | references | risk | roadmap

   A 6-week-old project with 93 stars loses ALL SIX to Neo4j.
   A 6-month-old project with 707 stars loses ALL SIX to Neo4j.
```

Slater and Grafeo are further from the enterprise than we are, not closer —
because they ask for more. Their engineering lead is real and their **adoption
path is worse**. They will convert Mode 1 (who will happily try a new embedded
engine) and stall at Mode 2/3 (who will not migrate a system of record for an
analytics feature).

**The plugin asks for less than anything else on the board:** one jar, one config
line, no export, no migration, parity provable by one `diff`, and deletable. It is
the only artifact here that can be adopted *without a decision being made about
the database.*

That is the asymmetry. It is genuine, it is durable for as long as Neo4j's gravity
holds, and it is worth more than the four features underneath it.

---

## 5. The honest bull and bear

**BEAR — and it is strong:**

1. Mode 3 may be empty. Mode 1 has no money; Mode 2 has no pain.
2. Our four features are copyable in a quarter by anyone with a working engine, and
   three people now have working engines.
3. The distinguishing headline is gone — low RAM, off disk, Rust, Bolt are all
   commodity as of 2026.
4. Cost anger is licensing-shaped; our product doesn't touch it.
5. Neo4j is moving on the over-provisioning axis (InfiniGraph) and has 15 years of
   switching costs working for it.
6. We have zero code shipped, zero users, zero interviews, and a 14-week track
   record of producing documents instead of experiments.

**BULL — and it is narrow but real:**

1. Blast radius is the one pain orthogonal to budget, and it is **completely
   unserved by everyone including the three new entrants**, because none of them
   can run inside a live Neo4j.
2. The trigger moment is reachable only from our position.
3. Four features nobody else has, one of which (manifest-as-closed-form-estimator)
   nobody else has even attempted.
4. Neo4j has publicly, in writing, declined to build this — a durable commitment
   they cannot cheaply reverse.
5. The adoption tax asymmetry (§4) is structural, not a head start.
6. The test is cheap: three weeks to a real answer.

---

## 6. The decision

**Build the plugin. Do not build a database. Test Mode 3 in parallel. Decide in
three weeks on evidence.**

Calibrated probabilities — judgment, not measurement:

| Proposition | P |
| --- | --- |
| The memory wall persists in Neo4j through 2027 | **0.90** |
| The plugin works technically (JNI, parity, zero heap contact) | **0.85** |
| Mode 3 exists at meaningful scale | **0.35** |
| Blast-radius is a budget-worthy pain at large shops | **0.45** |
| This becomes a real business within 18 months | **0.15** |
| This becomes a respected, adopted OSS component | **0.55** |
| Doing nothing for another quarter improves any of the above | **~0.00** |

The 0.15 is not a reason to stop. It is a reason to **stop spending like it's
0.60** — no 575-procedure registry, no format spec, no OLTP, no more architecture
documents. Spend three weeks, not three months, and let the evidence re-price it.

---

## 7. The three-week test — the only plan that matters

```text
  DAY 1        ESTIMATOR DRY RUN            zero code, ~4 hours
               gds.*.estimate for the seven families on a public 50GB-class
               graph, stock Neo4j. Every document leans on these numbers.
               Nobody has produced them.

  WEEK 1       grain.ping                   the technical gate
               Java shim -> Rust cdylib -> mmap a snapshot -> stream 1,000
               (nodeId, degree) pairs back. De-risks classloader, JNI
               transport, and ID mapping simultaneously.
               PASS = the only uncontested position is real.

  WEEK 2       WCC PARITY + RSS PROOF       the product gate
               grain.wcc.stream vs gds.wcc.stream, canonicalize by min member,
               diff must be empty. Measure heap on the Neo4j process
               throughout. "Zero heap contact" becomes measured, not claimed.
               ALSO: benchmark Slater/Grafeo/Ladybug on a graph 5-10x RAM --
               do they bound ALGORITHM state, or only avoid the projection?

  WEEKS 1-3    FIVE INTERVIEWS              the existential gate  (parallel)
               Platform owners at self-hosted Neo4j shops, 100GB+ graphs.
               Two questions only:
                 Q1 "Last time an analytics job hit a memory limit, what did
                     you actually do next?"        -> classifies Mode 1/2/3
                 Q2 "Has an analytics job ever degraded or taken down a
                     production system? What happened after?"
                                               -> tests blast-radius directly
               Do not pitch. Do not describe the product. Just listen.

  ALSO         Legal read on plugin licensing (U1 -- gates everything)
  ALSO         Snapshot the Neo4j rate-card JSON before 2026-08-01
```

Three gates, three weeks, run in parallel. Every one of them produces a fact that
no amount of further analysis can produce.

---

## 8. Kill criteria — written now, while it is still cheap to be honest

```text
  KILL IF:
   - grain.ping cannot get a clean JNI round trip in 2 weeks
       -> the only uncontested position is not reachable. Stop.
   - 4 of 5 interviews answer Q1 with "we bought a bigger box"
       -> Mode 3 is empty. The market is Mode 1. Make it a free tool and stop
          pretending there is a company.
   - 4 of 5 answer Q2 with "no, never"
       -> blast-radius is not a real pain. The whole whale thesis dies and
          only the Mode 1 laptop story remains -- already served by three
          free competitors.
   - Slater/Grafeo/Ladybug bound algorithm state well on a 5-10x-RAM graph
       -> the remaining gap is one quarter of work for them. Contribute
          instead of building (see §9).
   - Counsel says the plugin cannot be licensed independently
       -> the delivery vehicle is gone; re-plan from zero.

  PROCEED HARD IF:
   - parity diff is empty AND Neo4j heap is flat during a grain run
   - 2+ interviews describe a real production incident caused by analytics
   -> you have a demo a skeptic can verify and a pain a budget owner owns.
```

---

## 9. The fork this corpus has never named

Slater is Apache-2.0, six weeks old, and lacks **exactly the four things we have**:
a closed-form estimator, admission control on algorithm state, a printed receipt,
and a reject path. Its author is publicly discussing Elias-Fano and superhub
packing — the precise problems `Arch05` scheduled and never spiked.

```text
   PATH A: BUILD           our engine, our plugin, our company
     + control, equity, the whole idea is ours
     - zero users, zero code, 0.15 business probability
     - three competitors compounding while we start

   PATH B: CONTRIBUTE      the admission/receipt layer, where an engine runs
     + live users on day one, no zero-to-one risk, idea reaches the world
     + our best idea (manifest-as-estimator) gets built and used
     - no company, no control, no equity

   PATH C: BOTH            plugin for Neo4j (uncontested), and publish the
                           estimator contract as a spec anyone can implement
     + the plugin is ours and unreachable by them
     + the estimator spreads even if our engine never does
     - splits a solo maintainer's attention
```

**These are not equivalent, and the choice depends on something I do not know:
whether the goal is a company or an impact.** The corpus has always silently
assumed a company. That assumption has never been examined and it should be, in
writing, before another quarter is spent. If the honest answer is "I want this
idea to exist in the world," Path B gets there faster and more certainly than Path
A. If the answer is "I want to own it," Path A is correct and the three weeks in
§7 are how you find out whether it's viable.

Path C is my recommendation if the goal is ambiguous: the plugin is genuinely
uncontested, and the estimator contract costs almost nothing to publish alongside
it.

---

## 10. The trap to name explicitly

Twenty-seven documents create an obligation to justify them. That obligation is
the most dangerous thing in this repository — it will argue for the 575-procedure
registry, for GRAIN as a format, for the OLTP rewrite, because those things are
*written down* and writing feels like commitment.

Doshi's rule applies with force: **strategy is what you refuse.** The hardest
refusal here is not to a competitor or a feature. It is to the corpus itself.

The four months were not wasted — they produced a genuinely superior architectural
understanding, an idea (manifest-as-estimator) nobody else has, and a complete map
of the market. They were *mispriced*: the same insight was worth ten times more
shipped in week three than documented in week fourteen.

```text
  Slater      2026-06-10 -> 2026-07-21   ~6 weeks   -> working engine, HN launch
  This repo   2026-04-16 -> 2026-07-26  ~14 weeks   -> 27 documents, 0 experiments

  On paper our architecture is the more sophisticated one.
  The difference was never insight. They were compiling.
```

---

## 11. Conclusion, in five sentences

The memory wall is real, vendor-documented, and deliberately unfixed — that is
settled and needs no further research. The idea was never ours, the turf filled
while we were writing, and the cost anger everyone cites is about licensing rather
than gigabytes, so the original pitch is dead in all three of its parts. What
survives is one uncontested position — a deletable jar inside a running Neo4j,
reachable from the exact moment the pain occurs, asking the customer for nothing —
plus four narrow features and one genuinely original idea. Whether that is a
company depends entirely on whether Mode 3 exists, which nobody has checked, and
which five phone calls would settle. **Spend three weeks: one spike, one parity
proof, five interviews — and let the answer, not the corpus, decide what happens
next.**

---

## 12. What I would do, stated plainly

If this were mine, on Monday I would:

1. Not open a single document in this folder.
2. Run `gds.*.estimate` on a public graph. Four hours. Post the numbers in a text
   file with no commentary.
3. Start `grain.ping`. Nothing else in the codebase until a JNI round trip returns
   1,000 rows.
4. Send five emails to platform owners asking the two questions in §7. No pitch.
5. Email a lawyer about U1.
6. Write nothing else until at least three of those five produce a result.

And I would put one line at the top of `prd-l1.md` before doing any of it:

```text
  SUPERSEDED 2026-07-26: this document specifies a database. The product is a
  plugin. See The-One-Question-Left.md.
```
