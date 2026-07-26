# ASCII-innovation-mega-arch-20260726v1: ELI5 Terminal Edition

Plain-English companion to `innovation-mega-arch-20260726v1.md`.
Same six architectures, same numbers, no jargon, ASCII only.

Canvas: 72 columns. Read top to bottom. Every number here is
an ESTIMATE, not a measurement. Section 8 says what would
prove them wrong.

---

## 1. The one big idea

Normally a database must be ready for any question. We are
not normal. We know the question BEFORE we build the storage.

```text
   ORDINARY DATABASE                  OURS
   +--------------------+             +--------------------+
   | store everything   |             | told: "PageRank,   |
   | in a general shape |             |  damping 0.85"     |
   +--------------------+             +--------------------+
             |                                  |
             | any question may come            | build a shape
             v                                  v that serves
   +--------------------+             +--------------------+
   | answer it at       |             | most of the work   |
   | query time         |             | already done       |
   +--------------------+             +--------------------+

   The trick is NOT "squeeze the graph smaller".
   The trick is "move work to build time, and DELETE the
   parts the algorithm provably never looks at".
```

Squeezing gets you 2x to 4x. Moving and deleting gets you
10x to 1000x. That is the whole document in four lines.

---

## 2. The surprise: we counted the wrong thing

Everyone assumed PageRank is slow because it reads a lot of
edges. It is not. Here is one round, drawn honestly.

```text
   THE LIBRARIAN PROBLEM

   Step 1: read the edge list, top to bottom.
           Like walking down one shelf in order.

           [edge][edge][edge][edge][edge][edge] ...
            -->   -->   -->   -->   -->   -->
           5.22 GiB, sequential, about  1 SECOND

   Step 2: for EVERY edge, look up the source's score.
           Like sprinting to a random shelf in a huge
           library. One billion times.

                  shelf   4,912,003   <-- jump
                  shelf      71,884   <-- jump
                  shelf 199,004,551   <-- jump
                  ... one billion jumps ...
           about 80 SECONDS
```

```text
   TIME PER ROUND

   reading edges    |#|                                1 s
   random lookups   |################################| 80 s

   The lookups cost 50x to 100x more than the reading.
```

Why this matters: the old plan was to compress the edge
list. That turns the 1-second part into half a second. It
does nothing to the 80 seconds. We were sharpening the
wrong end of the pencil.

Everything in Section 3 attacks the 80 seconds.

---

## 3. The six ideas

Six cards, same shape. Read the metaphor line first.

```text
+--------------------------------------------------------------------+
| ARCH-I    THE ANSWER IS ALREADY WRITTEN DOWN                       |
+--------------------------------------------------------------------+
| Like     : doing the homework before the exam                      |
|                                                                    |
| A sealed snapshot never changes. So "which groups exist in this    |
| graph" has exactly ONE answer, forever. Working it out again on    |
| every single call is pure waste.                                   |
|                                                                    |
|   BUILD  : work it out once, save the answer                       |
|   QUERY  : read the answer. Never touch the edges at all.          |
|                                                                    |
| Best for : WCC (grouping), triangles, k-core, degree               |
| Not for  : anything where the user picks the settings              |
| Payoff   : about 100x faster queries. Exactly correct.             |
+--------------------------------------------------------------------+

+--------------------------------------------------------------------+
| ARCH-II   BIG NUMBERS GET BIG BOXES, TINY NUMBERS TINY BOXES       |
+--------------------------------------------------------------------+
| Like     : the 20 books you reread stay on your desk, the          |
|            other 2000 go in the basement                           |
|                                                                    |
| A few "famous" nodes get looked up constantly. Most nodes are      |
| looked at almost never, and their scores are so tiny that a        |
| rough number is good enough.                                       |
|                                                                    |
|   top   1% of nodes -> full precision   ->  16 MB  <- fits in CPU  |
|   next  9% of nodes -> half precision   ->  72 MB     cache!       |
|   last 90% of nodes -> tiny precision   -> 360 MB                  |
|                                total  ~448 MB  (was 1.6 GB)        |
|                                                                    |
| The 3.6x saving is NOT the point. The point is those 16 MB sit     |
| inside the CPU's own memory, so most "sprint to a random shelf"    |
| trips become "reach across the desk".                              |
|                                                                    |
| Payoff   : 3.6x less memory, 2x-4x faster lookups                  |
+--------------------------------------------------------------------+

+--------------------------------------------------------------------+
| ARCH-III  THROW AWAY THE PAGES YOU HAVE FINISHED                   |
+--------------------------------------------------------------------+
| Like     : a book that gets thinner as you read it                 |
|                                                                    |
| PageRank runs about 20 rounds. Nodes stop changing early:           |
|                                                                    |
|   round     1     2     3     4     5   ...  20                    |
|   still     ####  ##    #     .     .        .                     |
|   moving    100%  40%   12%   3%    0.5%     0.1%                  |
|                                                                    |
| After round 3, rewrite the file to keep only the edges that        |
| still matter. Rounds 4-20 then read a tiny file.                   |
|                                                                    |
|   before : 20 rounds x 5.22 GiB  = 104 GiB read                    |
|   after  : about 21 GiB read                                       |
|                                                                    |
| Payoff   : about 5x less reading. Exactly correct.                 |
+--------------------------------------------------------------------+

+--------------------------------------------------------------------+
| ARCH-IV   MOST OF THE GRAPH IS DECORATION                          |
+--------------------------------------------------------------------+
| Like     : a spelling test where half the words are already        |
|            written on the board                                    |
|                                                                    |
| Many nodes never need the slow repeated maths at all:              |
|                                                                    |
|   nobody points at it    -> fixed number. Forever.                 |
|   ONE thing points at it -> one-line formula at the end            |
|   it points at nothing   -> one global rule handles it             |
|   a plain chain A->B->C  -> squash into one edge                   |
|                                                                    |
| Strip those out, do the 20 rounds on what is left, then fill       |
| the easy ones back in with one final pass.                         |
|                                                                    |
| On real lumpy graphs this removes 30% to 60% of all nodes.         |
| Payoff   : scales with how lumpy the graph is. Exact.              |
+--------------------------------------------------------------------+

+--------------------------------------------------------------------+
| ARCH-V    BUILD THE HIGHWAY MAP BEFORE ANYONE ASKS                 |
+--------------------------------------------------------------------+
| Like     : a satnav that precomputed the motorways, so it          |
|            never searches every side street                        |
|                                                                    |
| If we know the QUESTION shape, not just the algorithm, we can      |
| precompute an index that changes the rules of the game:            |
|                                                                    |
|   "shortest path A to B"  -> motorway shortcuts -> 100x-1000x      |
|   "who is similar to who" -> pre-bucket likely pairs               |
|   "nearest neighbours"    -> disk-friendly vector index            |
|                                                                    |
| Payoff   : biggest single speedup on this page, for paths.         |
| Cost     : the index can be bigger than the graph. Only worth      |
|            it when the same question gets asked repeatedly.        |
+--------------------------------------------------------------------+

+--------------------------------------------------------------------+
| ARCH-VI   TEN THOUSAND TWINS NEED ONE SEAT            <== BEST BET |
+--------------------------------------------------------------------+
| Like     : 10,000 pupils share one timetable, so you print         |
|            ONE timetable                                           |
|                                                                    |
| If two nodes connect to EXACTLY the same things, no algorithm      |
| here can tell them apart. So squash them into one node that        |
| remembers "I am really 10,000 nodes", solve the small graph,       |
| then hand the answer back to all 10,000.                          |
|                                                                    |
|   BEFORE                          AFTER                            |
|   10,000 records, all sharing     1 node, count = 10,000           |
|   {email_A, device_B}             2 edges instead of 20,000        |
|                                                                    |
| WHY THIS IS SPECIAL FOR IDENTITY WORK:                             |
|   an identity graph is BUILT by joining records that share an      |
|   email or a device. Twins are not a rare accident here.           |
|   They are the main thing in the data.                             |
|                                                                    |
| Payoff   : 2x-10x smaller graph, EXACTLY correct, and it makes     |
|            every other idea on this page bigger too.               |
+--------------------------------------------------------------------+
```

---

## 4. Stacking them up: PageRank memory

Each idea multiplies the last. Bars are to scale. The widest
bar is the tool everyone uses today.

```text
   MEMORY FOR PAGERANK  (200M nodes, 1B edges)

   Neo4j GDS today      |##################################| 88-144 GiB
   our plain version    |###|                                 9.7 GiB
   + precision boxes    |#|                                   1.0 GiB
   + strip decoration   |#|                                   0.6 GiB
   + squash the twins   ||                                    0.2 GiB

                        ^
                        about 100x to 400x less memory
```

```text
   SPEED, HONESTLY

   Today (GDS)  : fast, because it holds everything in memory
   Ours         : 0.5x to 1.2x of that time
                  SIMILAR SPEED, on 1/100th the memory

   Why can less memory also be faster? Because there are two
   ways to use less memory, and they are opposites:

     MOVING bytes to disk -> less memory, SLOWER  (the usual way)
     DELETING bytes       -> less memory, FASTER  (what we do)

   Ideas II, IV and VI all DELETE. That is why this does not
   contradict the earlier warning that disk-based PageRank
   runs 1.5x-5x slower.
```

---

## 5. Which idea for which job

```text
   WCC / GROUPING                        <- the most used one
     use : ARCH-I, then ARCH-VI
     buys: about 100x. Precompute it. No debate.
           Exactly correct either way.

   PAGERANK
     use : ARCH-II + IV + III
           plus ARCH-VI on identity data
     buys: about 100x memory, similar speed.
           Precision becomes a declared mode.

   NODESIMILARITY                        <- the OOM king
     use : ARCH-VI, then ARCH-V
     buys: the family that always ran out of memory.
           Twins ARE the worst pairs, so squashing
           answers them for free.

   LOUVAIN
     use : ARCH-VI + III + I
     buys: 10.4 GiB floor drops to 1-3 GiB.

   SHORTEST PATH
     use : ARCH-V
     buys: 100x to 1000x. Biggest single win here.

   TRIANGLES
     use : ARCH-I
     buys: precompute it. About 50x.

   FASTRP
     use : ARCH-II + VI
     buys: 4x smaller vectors.
```

The two I would defend hardest: precomputing WCC (a week of
work, ~100x, cannot be wrong), and squashing twins for
NodeSimilarity. The family that breaks every design gets
broken by its own data's repetition.

---

## 6. Why any of this is allowed

```text
   +------------------------------------------+
   | A published snapshot NEVER CHANGES       |
   +------------------------------------------+
                       |
                       | therefore
                       v
   +------------------------------------------+
   | an answer computed once STAYS correct    |
   | for the whole life of that snapshot      |
   +------------------------------------------+
                       |
                       | therefore
                       v
   +------------------------------------------+
   | precomputing hard is SAFE, not a cache   |
   | that might quietly go stale              |
   +------------------------------------------+

   We chose "snapshots never change" for a boring reason:
   so readers never see a half-written file.
   It turns out to be the thing that makes all six ideas
   legal. Lucky accident.
```

---

## 7. Three numbers decide everything

Do not build any of this yet. Measure three things first.
Each is one pass over the data.

```text
   +---------------------------------------------------+
   | Y1  HOW MANY TWINS?                       1 week  |
   |     Count identical-neighbour groups in a real    |
   |     customer graph.                               |
   |     One number. It reprices all six ideas.        |
   |     If under 1.3x -> ARCH-VI is a footnote.       |
   +---------------------------------------------------+
   | Y2  HOW OFTEN DOES THE DESK HELP?         1 week  |
   |     What share of lookups land in the hot 1%?     |
   |     If under 40% -> ARCH-II saves space only,     |
   |     not time.                                     |
   +---------------------------------------------------+
   | Y3  HOW MUCH IS DECORATION?               2 days  |
   |     Share of nodes with 0 or 1 things pointing    |
   |     at them.                                      |
   |     If under 20% -> skip ARCH-IV.                 |
   +---------------------------------------------------+
```

---

## 8. Honest warnings

```text
   1. EVERY NUMBER HERE IS A GUESS, carefully made.
      Not one has been measured.

   2. The MEMORY guesses are solid. They are arithmetic.
      The SPEED guesses are the shaky ones. They rest on
      two untested bets: the hot-1% hit rate, and the
      twin ratio.

   3. This project's own track record: the ONE time it
      checked a modelled number against real source code,
      "50x-100x" became "10x-30x". Expect the same here.

   4. None of it matters until Y1, Y2 and Y3 are run.
      Three numbers. Three weeks.
```

---

## Reading notes

```text
   Section 2 is load-bearing. If the "80 seconds of random
   lookups" claim is wrong, ideas II and VI lose most of
   their value and this page needs rewriting.

   Section 3 cards are ordered by how much I trust them:
     ARCH-I   safest, smallest, do it now
     ARCH-II  most likely to be the big speed win
     ARCH-VI  highest ceiling, highest variance, best fit
              for identity work specifically

   Section 5 is the takeaway if you read nothing else.

   Full version with formulas, sources and the timeline
   simulation: innovation-mega-arch-20260726v1.md
```
