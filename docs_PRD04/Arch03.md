# Arch03: The Evidence Corpus Re-Prices The Architectures — And The Real Fork Is Build Order

Date: 2026-07-08
Method: Timeline Traverser, third iteration. Arch01 simulated five candidate
architectures; Arch02 stress-tested them against the seven adoption-driving
algorithm families. This iteration ingests the
`graph-database-rewrite-references-202606` corpus (five canonical pattern
files, 106 repos, ~9,900 lines of source-cited evidence) and asks what that
evidence CHANGES — then simulates the fork that survives: not "which
architecture" (Arch01/Arch02 already converged on a composite) but
"in which order do we build the composite."

---

## Phase 0: Core Facts — What The Corpus Adds (enumerated before opining)

Facts C1-C12 are new since Arch02, each sourced to a canonical pattern file.

```text
C1.  Compatibility is FIVE separately-evolving contracts, not one: kernel
     procedure, Cypher, Bolt, GDS, and client-ergonomics contracts
     (patterns-1 Executive Takeaways). Each has its own oracle.
C2.  Testkit exists as a protocol oracle (patterns-1 Pattern 10): driver
     conformance can be tested with protocol scripts BEFORE internals exist.
     Compatibility risk is testable early, cheaply.
C3.  GDS already exposes memory estimation as a PUBLIC API (patterns-1
     Pattern 13: estimate objects, .estimate procedures). Our "receipts"
     idea is NOT novel as an estimate — it is novel as ENFORCEMENT
     (reject-before-execute, budgeted plans, honest completion).
C4.  Termination flags + progress tracking are MANDATORY execution hooks in
     GDS (patterns-1 Pattern 15). Cancellation/progress must be in the
     kernel trait from day one, not retrofitted.
C5.  Storage consensus across 106 repos (patterns-2 Bottom Line): fixed core
     records + prefix adjacency keyspaces + Kuzu-style CSR checkpoint
     segments + streaming cursors + explicit WAL/snapshot boundaries.
     Store API narrower than graph API (patterns-2 Pattern 5).
C6.  A six-plane applied storage blueprint already exists in the corpus
     (patterns-3 §27): topology / property / index / transaction /
     manifest-root / execution planes — essentially Arch01-A's bytes with
     named seams, independently derived from Arrow/DataFusion/RocksDB/
     page-store evidence.
C7.  RAII memory reservations + spill-aware memory pools are PROVEN PRIOR
     ART (patterns-3 §8, DataFusion): Arch01-C's admission substrate is not
     research, it is adaptation. Cooperative scheduling (§9) and write
     stalls (§20) likewise.
C8.  "External-memory processing is an architecture, not a spill flag"
     (patterns-4 Takeaway 8: GridGraph, GraphChi, MiniGraph, ThunderRW —
     shards, file-backed vectors, active-shard scheduling). Out-of-core
     must be designed as a first-class layout, not bolted on.
C9.  The algorithm-facing trait shape is already converged (patterns-4
     Recommended Sketch): projection -> compact snapshot -> EXPLICIT
     WORKSPACE with estimate_workspace_memory_bytes() -> semantic output.
     Frontiers are an enum (sparse/bitmap/dense), not a Vec.
C10. GraphBLAS is validated as a SERIOUS OPTIONAL BACKEND, not a primary
     organization (patterns-4 Takeaway 6: LAGraph, SuiteSparse,
     Graphalytics). Arch01-E lands as "backend behind capability checks."
C11. Snapshot capability flags (can_pull/can_push/can_weight/
     can_temporal_window, patterns-4 synthesis) are the cheap, proven form
     of Arch01-D's portfolio manifest.
C12. A full operational baseline exists (patterns-5): metrics/trace field
     minimums, four benchmark tiers (smoke/regular/large/agent), failure-
     injection families, "no performance claim without benchmark artifacts."
```

### What the evidence re-prices (the headline of this iteration)

| Arch01/Arch02 position | corpus verdict | re-priced |
| --- | --- | --- |
| C (Budget Machine) is novel, ~1.5-2x engineering per family | C7/C8/C9: substrate has direct prior art in DataFusion pools + GridGraph shards + workspace-estimate traits | cost DOWN ~30-40%; likelihood UP (~75%→~85%) |
| B (Tilehouse) rejected as primary serving layout | C8: shards/cells are the PROVEN shape of out-of-core execution | B is reborn INSIDE C as the spill layout — not a serving bet, an executor mechanism |
| E (Algebra) ~50% as primary organization | C10: every serious system treats GraphBLAS as optional backend | settle it: E is a backend, stop simulating it as primary |
| D (Portfolio) planner-complexity fear | C11: capability flags are the industry's whole "planner" | D shrinks to a manifest + flags; fear was overpriced |
| A (Monolith) bytes | C5/C6: independently re-derived by 106 repos | confirmed; adopt patterns-3 §27 plane names verbatim |
| Compatibility risk (575 procedures) diffuse and scary | C1/C2: five contracts, each with an existing oracle (Testkit, grammar corpora, signature dumps) | risk becomes testable and schedulable |
| Receipts as differentiation | C3: GDS already HAS estimate APIs | differentiation narrows to ENFORCEMENT + honest out-of-core completion (feeds PMF02) |

So the architecture question is settled beyond usefulness: **A's planes
(C6) + C's substrate (C7/C8) + E as backend (C10) + D as capability flags
(C11), with B's cells as C's shard format (C8).** What is genuinely still
open — and what this iteration simulates — is BUILD ORDER, because the
corpus shows the components are separable and each ordering creates a
different first year.

---

## Decision Frame

- **Fork in the road:** Given the settled composite, which subsystem do we
  build FIRST — the memory/spill substrate, the compatibility surface, the
  seven algorithm families, or the out-of-core shard architecture?
- **Desired outcome:** Within a year: the seven families run honestly on
  8 GB (Arch02's bar), at least two compatibility contracts pass their
  oracles (Bolt via Testkit scripts, gds.* signatures for supported
  procedures), and the flagship benchmark (NodeSim/FastRP finishing where
  big-box GDS can't load) is published with patterns-5-grade artifacts.
- **Hard constraints:** PRD F01-F09 unchanged; solo/small-team capacity —
  orderings are mutually exclusive for the first two quarters; C4's
  mandate (cancellation/progress hooks in the kernel trait from day one)
  applies to every ordering.
- **Time horizon:** Week 1 / Month 1 / Quarter 1 / Year 1.
- **What counts as failure:** a year spent where EITHER nothing runs
  end-to-end (substrate perfectionism) OR everything runs but only on
  graphs that fit in RAM (the Arch02 Timeline-A trap, now inexcusable
  given C7/C8 prior art).

Assumptions stated: (1) corpus patterns transfer — DataFusion's pool design
works for graph workspaces (high confidence; same Rust ecosystem, same
Arc-buffer discipline per patterns-3 §1-8). (2) Testkit scripts can be
adapted without a full server (medium confidence; patterns-1 Pattern 10
shows scripted stub servers are exactly how drivers test). (3) One
maintainer-quarter ≈ one subsystem to "honest v1".

---

## Timeline A: Substrate-First ("pour the foundation")

- **Opening move:** Port the DataFusion-style memory pool + RAII
  reservations (C7) and the `GraphAlgorithm` workspace trait with
  `estimate_workspace_memory_bytes` (C9); no Bolt, no Cypher, CLI-only
  harness against fixture graphs.
- **Week 1:** Pool, reservation guards, and the trait compile; WCC runs
  under a reservation on the 2 GB fixture. No demo anyone outside the
  project cares about — the week's output is invisible discipline.
- **Month 1:** Spill substrate v1: external-sort spill file + file-backed
  vector (patterns-4 Pattern 11 shapes). PageRank runs windowed under a
  deliberately tiny budget to force the spill path early. The C4 hooks
  (termination/progress) go into the trait now, cheaply. Morale risk is
  real: a month in, there is still nothing a Neo4j user could touch.
- **Quarter 1:** All seven families implemented against the trait —
  and because the substrate predates them, Louvain's coarsening and
  NodeSim's candidate state are budgeted FROM THEIR FIRST COMMIT; the
  Arch02 crises never occur as crises. Flagship benchmark runs internally.
  Bolt/Cypher still absent; the demo audience is benchmark readers, not
  Neo4j users.
- **Long-term shape (Year 1):** Engine with honest execution and superb
  internals, compatibility surface begun only in Q3; Testkit passes late
  in the year. Total risk profile: low technical, high adoption-latency —
  the product story exists a full year before a Neo4j user can point
  their driver at it.
- **Likelihood of reaching year-end shape:** ~80%.
- **Stress points:** motivation through the invisible first quarter;
  the temptation to gold-plate the pool instead of shipping family #1.
- **Inflection points:** whether the month-1 spill v1 is declared "good
  enough" (healthy) or becomes a research project (the failure mode C7's
  prior art exists precisely to prevent).
- **Lived experience:** quiet, monastic, low-drama; the anxiety is
  entirely about relevance, never about correctness.

## Timeline B: Compat-First ("earn the right to be tried")

- **Opening move:** Bolt handshake + PackStream + FSM (patterns-1
  Pattern 8), Cypher subset parser, Testkit scripts running against the
  stub (C2). Storage is a toy in-memory map.
- **Week 1:** A real Neo4j driver connects, HELLO/RESET round-trips pass.
  Screenshot-able immediately; the adoption story starts week one.
- **Month 1:** Enough Cypher for MATCH/RETURN on toy graphs; first gds.*
  stub procedures registered with signature-first metadata (patterns-1
  Pattern 3) and deterministic unsupported errors. Users can CONNECT but
  not COMPUTE anything real — the demo flatters, the engine is hollow.
- **Quarter 1:** The hollowness bites in the exact way Arch02 predicted:
  the first real user loads a real graph, runs gds.wcc, and the toy
  storage dies. The team now builds substrate + families UNDER an
  already-public compatibility promise — every internal change risks
  breaking passing Testkit runs; velocity drops under the weight of
  keeping the shop window intact.
- **Long-term shape (Year 1):** Bolt/Cypher contracts genuinely solid
  (they got the most calendar time), algorithm honesty arrives last and
  rushed; the flagship benchmark slips to year end. The product is
  "compatible but not yet remarkable" — the most dangerous positioning,
  per PMF01 PM2.
- **Likelihood:** ~65%.
- **Stress points:** public promises outrunning the engine; the
  quarter-1 real-user disappointment.
- **Inflection points:** whether the team freezes the compat surface
  after month 1 (healthy: oracle locked, go build the engine) or keeps
  polishing it because Testkit progress is so measurable and satisfying.
- **Lived experience:** dopamine-rich start, grinding middle; the
  compatibility test suite becomes both proudest asset and daily jailer.

## Timeline C: Families-First ("Arch02's Timeline A, now with a warning label")

- **Opening move:** Flat CSR (patterns-4 Pattern 1 shapes) + WCC +
  Dijkstra, no pool, no Bolt; benchmark CLI only.
- **Week 1:** Two families run fast. Identical to Arch02 Timeline A
  week 1 — the corpus changes nothing about how good this week feels.
- **Month 1:** PageRank + Louvain; the coarsening allocation appears and —
  exactly as Arch02 simulated — gets a bespoke fix, because the pool that
  would have caught it wasn't built. The difference from Arch02: THE TEAM
  NOW KNOWS (C7) that a proven substrate was one import away, so the
  bespoke fix is a conscious debt, not an innocent one.
- **Quarter 1:** NodeSim wall, bespoke spill #2; the retrofit begins —
  rewriting month-1/2 algorithms against the workspace trait they should
  have started on. Retrofit cost ≈ 3-5 weeks, paid at the moment of
  maximum external interest (the benchmark is almost publishable).
- **Long-term shape (Year 1):** Converges to Timeline A's engine minus
  one quarter of substrate maturity, plus a residue of "v1" code paths
  that keep resurfacing in profiles. Compatibility starts Q3, same as A.
- **Likelihood:** ~75%.
- **Stress points:** the quarter-1 retrofit under external pressure; the
  standing knowledge that this path was chosen against documented
  evidence (a uniquely demoralizing kind of debt).
- **Inflection points:** whether the retrofit happens at family #4
  (recoverable) or is deferred to "after the benchmark" (the debt
  compounds past recovery — this is how engines get two executors).
- **Lived experience:** fast, fun, then sour; the corpus turned this
  path's surprises into foreseen-and-ignored warnings, which changes the
  emotional texture from adventure to negligence.

## Timeline D: Shard-Native ("out-of-core is the architecture")

- **Opening move:** Per C8, build the GridGraph/GraphChi-shaped shard
  store FIRST: partitioned CSR segments on disk, file-backed vectors,
  active-shard scheduler. RAM-resident execution is the special case
  (all shards resident), not the design center.
- **Week 1:** Shard format + loader; WCC as a shard-sweep algorithm.
  Slower to first algorithm than every other timeline (~2x Timeline C's
  week), because even the trivial case pays the shard abstraction.
- **Month 1:** PageRank as scheduled shard rotation — the windowed
  streaming plan falls out of the architecture for free rather than
  being C's month-1 special plan. Louvain forces the first hard question:
  coarsened graphs need RE-sharding per level, an expense the shard-first
  frame makes explicit (and painful) instead of hidden.
- **Quarter 1:** NodeSim's candidate state maps naturally onto shard-
  bucketed passes — this timeline's best moment; the flagship benchmark
  arrives EARLIER here than anywhere else (~week 10-11) because
  out-of-core was never a degraded mode. But in-memory small-graph
  latency is 1.3-2x worse than flat CSR across the board, and the
  falsifier question from Arch01-B returns wearing new clothes: the
  shard abstraction taxes the 80% of runs that would have fit in RAM.
- **Long-term shape (Year 1):** An engine whose signature capability
  (bigger-than-RAM honesty) is best-in-class and whose common case is
  perpetually a little slow, sprouting "resident fast path" special
  cases that converge — from the opposite direction — on the same
  two-tier design Timeline A reaches by adding spill to a resident core.
- **Likelihood:** ~55%.
- **Stress points:** every small-graph benchmark against Kuzu/flat-CSR
  baselines; re-sharding costs for coarsening algorithms.
- **Inflection points:** the month-4-ish decision to add a resident fast
  path — taken early it's healthy convergence, taken late the shard
  abstraction has ossified into every API.
- **Lived experience:** proud, contrarian, and lonely; the team is
  permanently explaining why the common case is slower "on purpose."

---

## Cross-Timeline Analysis

| path | upside | downside | reversibility | regret risk | who/what has to cooperate |
| --- | --- | --- | --- | --- | --- |
| A Substrate-First | crises pre-paid; families land budgeted from first commit; cleanest year-1 engine | invisible first month; adoption story starts latest | high | low-medium (latency of relevance) | maintainer morale through the quiet quarter |
| B Compat-First | earliest external proof; oracle-driven; adoption funnel opens week 1 | hollow engine at first contact; builds under public promises | medium (compat surface hard to walk back) | HIGH | Testkit discipline; resisting shop-window polish |
| C Families-First | fastest to running algorithms; benchmark momentum | known-in-advance crises + quarter-1 retrofit under pressure | medium | medium-high (now a *chosen* debt, per C7) | willingness to stop at family #4 and retrofit |
| D Shard-Native | flagship benchmark earliest; out-of-core is native, never degraded | common case taxed forever; re-sharding pain; converges to A anyway | low-medium (abstraction ossifies) | medium | patience with slow small-graph numbers |

Variance note: B has the widest outcome spread (great if the engine work
stays disciplined, brand-damaging if the hollow quarter leaks). D has the
narrowest — it reliably produces a specific, slightly-off-center engine.

Inflection shared by all four: the moment the first REAL user graph
arrives. A and D absorb it structurally; B and C absorb it with overtime.

---

## Decision Filter

**Which path is strongest if everything goes normally?**
**A (Substrate-First), with two week-1 borrowings that cost almost
nothing:** run Testkit scripts against a 300-line Bolt stub from week 1
(B's oracle, C2 — hours, not weeks, and it de-risks the compat contract
without promising anything publicly), and take Timeline C's fixture/bench
CLI so every substrate piece lands with a visible algorithm win attached.
The corpus specifically demolished A's old objection ("substrate is
research"): C7/C9 show it is adaptation of proven designs.

**Which path is safest if things go badly?**
Still A — its failure mode (slow start) is recoverable by re-scoping,
whereas B's failure mode (public compatibility promises over a hollow
engine) and C's (retrofit deferred past the event horizon) damage things
that don't heal: reputation and codebase respectively.

**What experiment would collapse uncertainty fastest?**
```text
X1. Pool-port spike (1 week): adapt DataFusion-style reservation/spill to
    one algorithm (WCC) and one hostile one (NodeSim candidate buckets).
    Confirms C7's transfer assumption — the load-bearing assumption of
    Timeline A. If it fails, Timeline C's sequencing becomes rational.
X2. Testkit-stub spike (2-3 days): Bolt handshake stub + scripted
    conformance run. Prices the entire compat contract (C1/C2) and
    inoculates against B's siren call by making its first dopamine hit
    cheap and private.
X3. Shard-tax measurement (1 week): flat CSR vs sharded CSR on resident
    graphs across the seven families' access patterns. Puts a number on
    Timeline D's permanent tax; if it's <10%, D deserves reconsideration
    as the substrate's native layout (and B's cells finally get their
    honest funeral or vindication).
```

---

## Chain of Verification

| # | question | answer | status |
| --- | --- | --- | --- |
| V1 | Does patterns-1 really identify five separate compatibility contracts and Testkit as an oracle? | Yes — Executive Takeaways enumerate the five; Pattern 10 documents Testkit protocol scripts. | verified |
| V2 | Does GDS expose memory estimation publicly (weakening receipt novelty)? | Yes — patterns-1 Pattern 13; novelty claim narrowed to enforcement accordingly. | verified, claim adjusted |
| V3 | Is the DataFusion reservation/spill prior art actually in the corpus? | Yes — patterns-3 §8 (RAII reservations, spill-aware pools), §9, §20. | verified |
| V4 | Does patterns-4 really recommend the workspace-estimate trait shape? | Yes — Recommended Rust Architecture Sketch: `estimate_workspace_memory_bytes` before workspace creation. | verified |
| V5 | Is "external memory is an architecture, not a spill flag" a fair reading? | Yes — patterns-4 Takeaway 8, verbatim concept; GridGraph/GraphChi/MiniGraph/ThunderRW cited. | verified |
| V6 | Are the timeline likelihoods and week-counts measured? | No — judgment calibrated on Arch01/Arch02 reasoning plus corpus cost re-pricing; X1-X3 exist to replace the two most load-bearing guesses. | honest-uncertainty |

## One-Sentence Summary

```text
The 106-repo corpus settles the architecture (A's six planes, C's
pool+spill substrate with direct DataFusion/GridGraph prior art, E demoted
to backend, D reduced to capability flags, B reborn as C's shard format)
and moves the real fork to build order — where Substrate-First, borrowing
B's cheap Testkit oracle and C's visible benchmark CLI from week one, is
both the strongest normal path and the safest bad-day path.
```
