# Not-First-Still-Different — Priority, Originality, And What Is Actually Ours

Date: 2026-07-25
Status: judgment. Facts sourced in `evidence01.md` (URL-cited, verified 2026-07-25).
Scope: this file answers exactly one question — **"we thought of the low-RAM idea
first, but others have piled on already."** Is that true, and if not, what *is*
true?

Short answer: **we were not first, nobody piled on, and there are still four
things in this folder that nobody else has.** All three of those matter, and the
middle one changes what we should do next.

---

## 1. The timeline

```
  2012  GraphChi (OSDI) -- billion-edge graphs on ONE LAPTOP, from disk
  2013  X-Stream (SOSP)     2015  GridGraph (ATC)     2017  Mosaic (EuroSys)
  2023  Kuzu (CIDR paper) -- disk-based columnar graph engine
  2025  Gem -- 42.5B edges, out-of-core, single machine
   |
  2025-10-07  LadybugDB created (Kuzu renamed; algos scan from disk)  <-- 6 mo BEFORE us
  2026-01-26  Grafeo created ("low memory use", transparent spilling,
                               + boltr for Neo4j driver compatibility) <-- 3 mo BEFORE us
   |
  2026-04-16  knight-bus-graph-walker, FIRST COMMIT                    <-- we start here
              "docs: establish knight bus storage runtime thesis"
   |
  2026-06-10  Slater created (bounded cache budget, Bolt, Elias-Fano)
  2026-07-21  Slater launches on HN, 43 points
  2026-07-25  today
```

**Two of the three live competitors had public code before this repo's first
commit.** Grafeo by ~3 months, LadybugDB by ~6 months.

Honest caveat: git history only starts 2026-04-16. The *thinking* here may well
predate that — earlier folders (`v001-learnings`, `docs_PRD02`) suggest it does.
But "first" in any sense that matters commercially means **shipped or published
first**, and on that measure it is not close.

## 2. Nobody piled on

"Piled on" implies they saw our work and followed. They did not, and could not:

```
  - This repo is unpublished. There is nothing to copy.
  - The idea has been public since 2012. There is nothing to copy FROM us.
```

What happened is **convergent discovery**: three independent teams reached
substantially the same architecture within twelve months of each other. That is a
different phenomenon from being copied, and it carries different information
(see §4).

**Our own corpus already said the idea was not original — twice:**

```text
simulation01.md §4, P2 (the pre-written HN rebuttal):
  "You could do this with GraphChi in 2012 / with NetworKit / with a 750-line
   Rust program. Out-of-core graph processing is a solved research problem."
  -> and the document's own verdict on that objection: "This objection is TRUE"

simulation01.md §12.1:
  the technique "is public and thirteen years old"
```

The folder never claimed the idea was new. It claimed the *product gap* was open —
and that claim was true when written and is now contested.

## 3. What genuinely was ours first

This is the part worth keeping. Reading all four competitors' documentation, here
is what **none of them claim**:

| Genuinely ours | Where in this folder | Status elsewhere |
| --- | --- | --- |
| **Manifest as a closed-form estimator** — memory cost of every algorithm as arithmetic over ~1 KB of metadata, computable before reading a single graph byte | `Arch05.md` G3 | Not claimed by any of the four. Slater has cache *knobs* you tune; nobody *derives* a bill. |
| **Reject-before-execute as a product surface** — admission control on algorithm state, with a printed receipt and a deterministic refusal path | `Arch01`-C, `Arch02` column C | Grafeo ships timeouts, property-size caps, an HNSW element bound. That is resource limiting, not admission control. |
| **`gds.*`-shaped coexistence inside a running Neo4j** — a plugin beside GDS, no export, no migration, parity provable by one `diff` | `gtm-POC-01.md` | All four **replace** Neo4j on the read path and pay the export/compile cost. |
| **Holistic RAM accounting** — heap + RSS + page cache + mmap residency + scratch + sidecars + spill + retained generations as ONE budget | `prd-l1.md`, `Arch-options.md` | Nobody documents this discipline anywhere. |

That is a real contribution. It is also **four features, all narrow**, and all
copyable within a quarter by anyone who already has a working engine.

### What was never ours

```
   low RAM  +  off disk  +  in Rust  +  over Bolt
   ^
   |  the headline. Three teams reached it independently within a year.
   |  Slater's README states it in our exact words:
   |    "graphs that don't fit in memory ... over standard Bolt, so any
   |     neo4j driver just works ... Resident memory is set by a cache
   |     budget you choose, NOT by the size of the graph."
```

Also not ours, and worth admitting plainly: Slater independently arrived at
content-addressed immutable generation directories, Elias-Fano encoding, and
superhub-aware block packing — `Arch05`'s G1, G2 and G5, and `Arch-Summary` §5
AXIS 1. Same conclusions. Theirs compile.

## 4. Why the diagnosis changes the action

```
  IF THE STORY IS "we were first, they piled on"
     -> the instinct is DEFENSIVE:
          stake priority, publish the spec, establish provenance,
          build the moat, prove we got there first
     -> ALL OF IT IS WASTED MOTION, because the premise is false

  IF THE STORY IS "the idea's time arrived and we are one of four"
     -> the instinct is OFFENSIVE AND NARROW:
          take the slice nobody has taken, ship it in weeks,
          or contribute the missing layer where an engine already runs
     -> this is the correct playbook, and it is available today
```

Getting this right is the whole value of the question. A defensive response to a
convergent-discovery situation burns the remaining time on the one thing that
cannot be won.

**And the convergence is genuinely good news about the thesis.** Three independent
teams reaching the same architecture in twelve months is strong evidence the
approach is *correct*. It is simultaneously the evidence that the advantage is
gone. Both readings come from the same fact:

```
   WE WERE RIGHT.  BEING RIGHT WAS NEVER THE SCARCE THING.
```

## 5. The comparison that stings, and should

```
  SAME LANGUAGE. SAME IDEA. OVERLAPPING WINDOW.

  Slater      2026-06-10 -> 2026-07-21   (~6 weeks)
              empty repo -> Bolt-speaking, Elias-Fano, bounded-memory engine,
              Wikidata imported (~14 bytes/edge), HN launch, 93 stars

  This repo   2026-04-16 -> 2026-07-25   (~14 weeks)
              24 planning documents, ~12,300 lines in docs_PRD04 alone,
              8 scheduled experiments, 0 run
```

On paper our architecture is the more sophisticated of the two — the estimator
contract, the seven access plans, the holistic budget. **The difference was not
insight. The difference was that they were compiling.**

## 6. What to hold onto

The position that survives every finding in `evidence01.md` is the one this folder
is least attached to, and the one no competitor can take without abandoning their
own design:

```
   LIVE INSIDE A RUNNING NEO4J.

     no export        no migration        no new database to trust
     gds.* call shape                    parity provable by one diff
     one jar -- delete it and nothing changed

   Slater and Grafeo CANNOT follow us here.
   They ARE the replacement database. That is their entire architecture.
```

Smaller than "we rewrote Neo4j." Also the only uncontested ground on the board,
and roughly two weeks of work to find out whether it is real.

---

## 7. Cross-references

```text
evidence01.md ................. verified facts, URLs, dates for every claim above
                               (§3.6 Slater, §3.7 Grafeo, §3.2 LadybugDB)
Real-Pain-Wrong-Product.md .... the full verdict; §3.2b is the competitor finding
gtm-POC-01.md ................. the plugin design -- the surviving position
Arch05.md ..................... G3, the manifest-as-estimator idea that IS ours
Arch02.md ..................... R2: Louvain/NodeSim die on STATE, which is the gap
                               Slater and Grafeo do not document closing
simulation01.md ............... §4 P2 and §12.1 already conceded the idea's age
```
