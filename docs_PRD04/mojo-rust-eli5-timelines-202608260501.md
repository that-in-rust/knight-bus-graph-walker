# Mojo Rust ELI5 Timelines

ELI5 + ASCII edition of "if we leverage Mojo, what does Rust do and what does
Mojo do?" — then Timeline Traverser simulations of the three ways to actually
adopt it. Companion to `mojo-rust-duck-debug-202608250657.md` (the rigor lives
there; the pictures live here).

---

## Part 1 — ELI5: The Restaurant

Imagine Knight Bus is a restaurant that serves graph answers.

```
                    THE KNIGHT BUS RESTAURANT

  +---------------------------------------------------------------+
  |                     FRONT OF HOUSE  (Rust)                     |
  |                                                                |
  |   greeter        menu           cashier          manager       |
  |   (Bolt/Cypher   (GDS surface)  (receipts:       (Budget       |
  |    door)                         what you got,    Machine:     |
  |                                  what it cost)    "do we have  |
  |                                                   enough       |
  |                                                   kitchen for  |
  |                                                   this order?  |
  |                                                   yes/no")     |
  +---------------------------------------------------------------+
  |                     PANTRY  (Rust)                             |
  |                                                                |
  |   Every ingredient washed, chopped, labeled, sealed in boxes   |
  |   = the immutable CSR snapshot: sorted, checksummed, mmap'd    |
  |   Nobody EVER cooks from unwashed ingredients.                 |
  +---------------------------------------------------------------+
  |                     KITCHEN                                    |
  |                                                                |
  |   +------------------------+   +---------------------------+  |
  |   | Rust stove (always on) |   | Mojo blast oven (optional)|  |
  |   |                        |   |                           |  |
  |   | cooks EVERY dish       |   | only for BIG BATCH dishes |  |
  |   | correctly, every time  |   | (many identical trays at  |  |
  |   |                        |   |  once), only if the timer |  |
  |   |                        |   |  proves it's faster       |  |
  |   +------------------------+   +---------------------------+  |
  +---------------------------------------------------------------+

  RULES OF THE HOUSE:
  1. The blast oven never talks to customers.        (Mojo has no API)
  2. The blast oven never touches the pantry.        (read-only artifact)
  3. Every blast-oven dish is taste-tested against   (Rust = correctness
     the stove version before it goes on the menu.    oracle)
  4. If the blast oven breaks, the stove cooks       (Rust baseline
     everything. Menu unchanged. Nobody notices.      forever)
```

ELI5 in one line: **Rust runs the restaurant; Mojo is one fancy oven Rust is
allowed to use for big batches — after proving the food comes out the same,
faster.**

## Part 2 — ELI5: Who does what, byte by byte

```
   RAW EDGES (CSV / source of truth)
        |
        v
  +-----------------------------------------------+
  | RUST: "make the graph boring"                 |
  |  - parse, dedupe, dense u32 ids               |
  |  - forward + reverse CSR, sorted neighbors    |
  |  - degrees, checksums, manifest (~1KB)        |
  |  - PRICE the job before running (admit/spill/ |
  |    refuse)                                    |
  +----------------------+------------------------+
                         |
                         v
              SEALED SNAPSHOT (mmap files)
              "the boring, perfect graph"
                         |
          +--------------+---------------+
          |                              |
          v                              v
  +-----------------+          +---------------------+
  | RUST engine     |          | MOJO kernel (maybe) |
  |                 |          |                     |
  | anything        |          | ONLY regular, dense |
  | irregular:      |          | batch work:         |
  |  - BFS/walks    |          |  - PageRank sweeps  |
  |  - sparse       |          |  - batched set      |
  |    frontiers    |          |    intersections    |
  |  - out-of-core  |          |  - multi-source     |
  |  - small queries|          |    scans            |
  |                 |          |  (BUILD-time work,  |
  |                 |          |   not query-time)   |
  +--------+--------+          +----------+----------+
           |                              |
           +--------------+---------------+
                          v
  +-----------------------------------------------+
  | RUST again: verify vs oracle, write RECEIPT   |
  |  (engine used, peak unified RAM, output hash/ |
  |   tolerance tier, time, cost)                 |
  +-----------------------------------------------+
```

Why this split (ELI5): GPUs and SIMD are like a thousand kids who must all do
the **same** homework problem at once. Graph *walking* gives every kid a
different problem (irregular). Graph *building* (PageRank iterations over a
pre-sorted CSR) can be arranged into identical trays. So Mojo only ever gets
the tray-shaped work, and only at build time — where nobody is waiting.

Mac Mini bonus (ELI5): Apple Silicon has ONE pot of memory shared by CPU and
GPU. Good: no copying food between kitchens. Bad: both cooks share the same
pantry shelf — so the Budget Machine must price the oven's shelf space too.

---

## Part 3 — Timeline Traverser: three ways to leverage Mojo

### Decision Frame

- **Fork in the road:** how much Mojo, and when?
- **Desired outcome:** bigger admissible graphs / cheaper builds on a fixed Mac
  Mini, without ever risking correctness, receipts, or the Rust product core.
- **Hard constraints:** solo founder; Cypher/Bolt compat + measurement sprint
  already queued; Mojo needs macOS 15+/Xcode and is a startup-controlled
  toolchain; results must stay parity-verified.
- **Time horizon:** week 1 → quarter 1 → year 1.
- **Failure looks like:** two engines that quietly disagree; a second toolchain
  that stalls shipping; a "Mojo-powered" claim with no customer-visible win.

### Timeline A: All-In Rewrite ("Mojo + Rust product")

- **Opening move:** announce Mojo+Rust architecture; start porting kernels and
  the runtime hot path to Mojo now.
- **Week 1:** toolchain setup eats days (macOS 15, Xcode, MAX); first kernel
  compiles; the borrow-checker-shaped bugs move to a language with fewer tools.
- **Month 1:** Cypher/Bolt work stalls; the irregular traversal paths are *slower*
  in Mojo (the KMP-shaped result); you now maintain two half-engines.
- **Quarter 1:** parity harness catches float drift between engines; weeks go to
  reconciliation instead of the wedge; interview story blurs from "priceable
  graph compute" to "we use a cool language."
- **Long-term shape:** a two-language codebase a solo founder can't carry.
- **Likelihood of the win it promises:** low.
- **Stress points:** constant — every feature lands twice or not at all.
- **Inflection point:** the day an enterprise asks "what happens if Modular
  changes licensing?" and the answer is "we're stuck."

### Timeline B: Rust-Only Forever

- **Opening move:** explicitly decide "no Mojo," delete the question.
- **Week 1:** zero cost; measurement sprint proceeds.
- **Month 1:** WCC wedge + receipts ship; one toolchain, one truth.
- **Quarter 1:** builds on the biggest fixtures start pressing against the
  refresh cadence; you hand-write SIMD in Rust (fine) and skip the Apple GPU
  entirely (leaves known silicon idle on a fixed-hardware appliance).
- **Long-term shape:** maximally dependable; possibly leaves a 2–5× build-window
  win unclaimed on hardware every customer identically owns.
- **Likelihood:** high that it works; certain that it never surprises.
- **Stress points:** only when a competitor demos "same Mini, 3× bigger graph."
- **Inflection point:** the first customer whose graph is refused *only* because
  the build window, not RAM, is the binding constraint.

### Timeline C: Rust Core + Bounded Mojo Trial (the duck's schedule)

- **Opening move:** do nothing now. Finish measurement sprint + ship one
  algorithm family with receipts. THEN run a one-week trial: same sealed
  snapshot, PageRank build kernel, three columns (Rust / Mojo-CPU / Mojo-GPU).
- **Week 1 (of the trial, ~month 2–3):** kernel reads the mmap'd artifact
  read-only; parity checked via the existing equivalence-tier harness; measure
  end-to-end build wall time, peak unified memory, cost-per-job.
- **Month after trial:** binary outcome. Win → Mojo becomes an *optional,
  receipted* backend behind a flag, pinned toolchain, Rust baseline in CI
  forever. Loss → one markdown file of numbers, zero architecture debt, and a
  great honesty story ("we measured the shiny thing and kept the boring one").
- **Quarter 1:** product story unchanged either way: "priceable, auditable,
  rejectable" — with possibly one more receipt line: `engine: mojo-gpu`.
- **Long-term shape:** an option held cheaply, exercised only on evidence.
- **Likelihood:** high — because both outcomes are acceptable by design.
- **Stress points:** one contained week; the only real risk is scope creep
  ("let's also try Louvain while we're here" — don't).
- **Inflection point:** the trial's cost-per-job number; it converts the whole
  question from opinion to arithmetic.

### Cross-Timeline Analysis

| path | upside | downside | reversibility | regret risk | who/what must cooperate |
| --- | --- | --- | --- | --- | --- |
| A: all-in | language buzz | split engine, stalled wedge, toolchain hostage | poor (sunk rewrite) | HIGH | Modular's roadmap, your calendar |
| B: Rust-only | one truth, zero drag | idle GPU on known silicon; possible build-window loss | trivial (can revisit) | medium (unclaimed win) | nobody |
| C: bounded trial | keeps B's safety, buys A's upside as an option | one week + permanent dual-CI cost *if* adopted | excellent (kill switch in receipt) | LOW | a Mac Mini + one honest benchmark |

### Decision Filter

- **Strongest if everything goes normally:** C — it degrades gracefully into B.
- **Safest if things go badly:** B, with C a close second (its failure mode *is* B).
- **Experiment that collapses uncertainty fastest:** C's trial week — one
  PageRank kernel, one fixture, three columns, four numbers (parity tier,
  end-to-end build time, peak unified memory, cost-per-job). Everything else
  about "should we use Mojo" is downstream of those four numbers.

ELI5 verdict: **Rust builds and runs the restaurant. Mojo auditions, once, for
the job of batch oven — after the restaurant is open — and only keeps the job
if the stopwatch and the taste test both say yes.**
