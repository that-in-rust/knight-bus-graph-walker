# The Seven Algorithm Families, Explained in ASCII

Companion to `simulation01.md` §13 (who uses these) and `Arch06.md` (our
bespoke storage plans). For each family: the practical use case, what the raw
data looks like, how it is stored, and how the algorithm actually works —
with two worked examples each, all in ASCII.

A note on storage: every family below reads the same on-disk substrate — a
degree-ranked CSR snapshot (GRAIN). The generic picture, referenced
throughout:

```
RAW EDGES (CSV)          CSR ON DISK (two arrays, mmap'd)
src,dst                  offsets: [0,   2,   3,   5,   6]   (V+1 entries)
A,B                                A    B    C    D
A,C          ------>     targets: [B, C,   D,   A, D,   B]  (E entries)
B,D                                \A's/  \B's/ \C's/  \D's/
C,A
C,D                      neighbors(C) = targets[offsets[C]..offsets[C+1]]
D,B                                   = targets[3..5] = [A, D]
```

The algorithm-specific question is always: *what must stay resident in RAM
(the O(V) plane), and what can stream from disk (the O(E) plane)?*

---

## 1. WCC — Weakly Connected Components

### Practical use case
"Which of these records are secretly the same thing?" Entity resolution:
customer dedup, fraud-ring discovery, master data management.

### Example 1: fraud rings from shared attributes

Raw data — three tables, flattened into edges when two accounts share
anything:

```
ACCOUNTS            SHARED ATTRIBUTES (raw)          DERIVED EDGES
acct  email         acct1  acct2  shared_via         acct1 -- acct2
----  -----------   -----  -----  ----------
a1    x@mail.com    a1     a2     email x@mail.com   a1 -- a2
a2    x@mail.com    a2     a3     device D-7731      a2 -- a3
a3    y@mail.com    a4     a5     card **** 4412     a4 -- a5
a4    z@mail.com
a5    w@mail.com    a6 shares nothing
a6    q@mail.com
```

How the algorithm works — label propagation until fixpoint. Every vertex
starts as its own component id; each pass, every vertex takes the MINIMUM
label among itself and its neighbors:

```
        a1 -- a2 -- a3        a4 -- a5        a6

init:   [1]   [2]   [3]       [4]   [5]       [6]
pass 1: [1]   [1]   [2]       [4]   [4]       [6]   <- labels flow one hop
pass 2: [1]   [1]   [1]       [4]   [4]       [6]   <- converged

RESULT: component {a1,a2,a3}  = one fraud ring / one real customer
        component {a4,a5}     = another
        component {a6}        = singleton, clean
```

Storage & RAM shape — the only resident state is the label array
(8 B/vertex). Edges stream from disk each pass:

```
RAM  (O(V), stays):   labels[V]           [1][1][2][4][4][6]
DISK (O(E), streams): CSR edges ==========================>  pass 1
                      CSR edges ==========================>  pass 2
```

### Example 2: product catalog dedup

```
RAW: listings that share a barcode or near-identical title
   L1 "iPhone 15 Pro 256GB"  --barcode-->  L2 "Apple iPhone15Pro 256"
   L2                        --title---->  L3 "iphone 15 pro (256 gb)"

WCC output: {L1, L2, L3} = one canonical product page.
```

Same machinery, different edge-derivation rule. WCC does not care what an
edge *means* — that is why it is the universal first step.

---

## 2. Louvain / Leiden — Community Detection

### Practical use case
Finding "clumps" denser inside than outside: fraud rings that don't share
hard attributes (only transact together), customer segments, GraphRAG
community summaries.

### Example 1: money-mule clusters in a transfer graph

Raw data — a transaction log:

```
src   dst   amount        The graph (edge thickness ~ transfer count):
----  ----  ------
m1    m2    500           m1 ==== m2         c1 ---- c2
m2    m3    480                \\  //             |
m1    m3    510                 m3                c3
m3    c1     20  <-- one thin bridge edge
c1    c2     30
c2    c3     25
```

How the algorithm works — greedily move each vertex into the neighboring
community that most increases *modularity* (in-community edge weight vs.
expected), then collapse communities into super-nodes and repeat:

```
LEVEL 0: each vertex alone     LEVEL 0 after moves      LEVEL 1: collapse
 (m1)(m2)(m3)(c1)(c2)(c3)      [m1 m2 m3] [c1 c2 c3]     (M)---20---(C)
                                     |thin bridge|        no move improves
                                                          modularity -> STOP

RESULT: community M = {m1,m2,m3} (mule ring), C = {c1,c2,c3} (normal cluster)
```

The per-vertex move decision needs a tally: "how much edge weight do I have
into each neighboring community?"

```
tallying m3's neighbors:   into M: 480+510 = 990   into C: 20
                           moving to M wins by a landslide
```

Storage & RAM shape — community ids resident (O(V)); the tally map is the
scratch that explodes on hub vertices, so we cap it (top-k communities per
tally, slight approximation):

```
RAM:  community[V] + capped tally (k slots/vertex, not degree slots)
DISK: edges streamed once per move-pass; each collapsed LEVEL is a smaller
      graph written back to disk -> later levels are cheap
WARM START: yesterday's communities are today's init -> most vertices never
      move -> 10-50x faster re-runs
```

### Example 2: GraphRAG document communities

```
RAW: LLM-extracted entities and co-mention edges from a corpus
   (Einstein)---(Relativity)---(Spacetime)      (Picasso)---(Cubism)
        \____________|                              |
         (Physics)                              (Guernica)

Leiden groups them:  [Physics cluster]         [Art cluster]
Each cluster gets one LLM-written summary -> hierarchical index for RAG.
```

The corpus re-index re-runs Leiden on a slightly-grown graph — the
warm-start case again.

---

## 3. PageRank

### Practical use case
"Which nodes matter?" — importance that flows through links: influence
scoring, money-flow centrality in fraud, recommendation re-ranking.

### Example 1: who matters in a citation / follow graph

Raw data:

```
follower  followed        A --> B <-- C
--------  --------              |
A         B                     v
C         B                     D --> A
B         D
D         A
```

How the algorithm works — every vertex starts with equal rank; each
iteration, it splits its rank among its out-neighbors; ranks accumulate
where many (or important) edges point:

```
init:      A=.25  B=.25  C=.25  D=.25
iter 1:    B receives from A(.25) and C(.25) -> B rank rises
           D receives from B(.25)            -> middling
           A receives from D(.25)            -> middling
           C receives nothing                -> sinks toward the floor

after ~20 iterations (damping 0.85):
           B ~= .38   D ~= .32   A ~= .22   C ~= .08
           ^^^ most-pointed-at vertex wins, even with equal edge counts
```

Storage & RAM shape — two rank vectors resident (current + next, 8 B each
per vertex); edges stream per iteration:

```
RAM:  rank[V], next[V]                      (ping-pong)
DISK: CSR edges =========> iter 1
      CSR edges =========> iter 2  ... x20
DELTA TRICK: late iterations, ~97% of vertices have converged -> only
      stream blocks containing still-active vertices -> most passes shrink
```

### Example 2: mule-account centrality in a payments graph

```
RAW: directed transfer edges (like Louvain ex.1, but direction matters)

   many small accounts --> [HUB account] --> few cash-out accounts

Personalized PageRank seeded at known-bad accounts:
   rank mass leaks outward along transfer edges;
   high-rank unseeded accounts = likely undiscovered mules.
```

Same iteration, different seed vector — the storage plan is identical.

---

## 4. NodeSimilarity / kNN

### Practical use case
"Which nodes behave alike?" over bipartite graphs: recommendation candidates
("users who bought what you bought"), fraud pattern matching (accounts
sharing devices/IPs).

### Example 1: co-purchase similarity

Raw data — a purchase log, forming a bipartite graph:

```
user  product        u1 --- p1        neighbor sets:
----  -------        u1 --- p2          N(u1) = {p1,p2,p3}
u1    p1             u1 --- p3          N(u2) = {p2,p3,p4}
u1    p2             u2 --- p2          N(u3) = {p9}
u1    p3             u2 --- p3
u2    p2             u2 --- p4
u2    p3             u3 --- p9
u2    p4
u3    p9
```

How the algorithm works — Jaccard similarity between neighbor sets:

```
J(u1,u2) = |N(u1) ∩ N(u2)| / |N(u1) ∪ N(u2)|
         = |{p2,p3}|       / |{p1,p2,p3,p4}|
         = 2/4 = 0.5              -> u1 and u2 are similar; recommend p4 to u1
J(u1,u3) = 0/4 = 0                -> unrelated
```

The naive cost is ALL PAIRS — V² comparisons, and GDS additionally
materializes a second uncompressed copy of every neighbor list as scratch.

Our plan — MinHash sketches (fixed 128-256 bytes/vertex) estimate Jaccard
without comparing full lists; only top candidates get exact reranking:

```
full list  N(u1) = {p1,p2,p3, ...}  --hash--> sketch(u1) = [17, 3, 42, ...]
full list  N(u2) = {p2,p3,p4, ...}  --hash--> sketch(u2) = [17, 3, 99, ...]
                                              matching slots / total slots
                                              ~= Jaccard, from fixed bytes
RAM:  sketches[V] (fixed) + top-k heap
DISK: neighbor lists read in place (CSR is already the list) only for the
      finalists' exact rerank
PRUNE: degree bands — |N| = 3 can never have J > t with |N| = 30,000
```

### Example 2: shared-device fraud

```
RAW: account --uses--> device
   a1 -- d1   a1 -- d2   a2 -- d1   a2 -- d2   a2 -- d3

J(a1,a2) = |{d1,d2}| / |{d1,d2,d3}| = 0.67  -> flag pair for review
```

High similarity over *devices/IPs* (instead of products) is a fraud signal,
not a recommendation.

---

## 5. Shortest Paths / BFS / Dijkstra

### Practical use case
"How do I get from A to B, and how far is it?" — routing, supply-chain
impact ("what breaks if this supplier fails"), degrees of separation,
data lineage.

### Example 1: supply-chain blast radius (BFS)

Raw data — a dependency edge list:

```
supplier   feeds        S ──> P1 ──> P3 ──> RETAIL
--------   -----          └─> P2 ──┘
S          P1
S          P2           BFS from S, level by level:
P1         P3           level 0: {S}
P2         P3           level 1: {P1, P2}       <- direct impact
P3         RETAIL       level 2: {P3}           <- cascades
                        level 3: {RETAIL}       <- customer-visible
```

How BFS works — a frontier expands one hop per round; a visited-bitset
(1 bit/vertex) stops revisits:

```
frontier: [S] -> [P1 P2] -> [P3] -> [RETAIL] -> []  done
visited:   S      SP1P2     +P3      +RETAIL
```

Storage & RAM shape — O(V) bits resident; the disk win is *not touching*
most of the graph:

```
RAM:  visited bitset (V/8 bytes) + frontier queue
DISK: demand-page ONLY the CSR blocks the frontier enters.
      GDS instead projects 100% of the graph into heap to answer this
      one query — minutes of boot-up for a 4-hop question.
```

### Example 2: cheapest route (weighted Dijkstra)

```
RAW: edges with weights          A --2--> B --2--> D
   A B 2                          \--5--> C --1--/
   A C 5
   B D 2       Dijkstra explores in cost order (priority queue):
   C D 1       settle A(0) -> B(2) -> D(4 via B) ... C(5) arrives too late
               shortest A->D = 4  (path A-B-D, NOT through the 1-weight edge)
```

The priority queue and distance array are O(V); edges are only read for
vertices actually settled. Bidirectional search (run from both ends, meet in
the middle) shrinks the touched region by roughly the square root.

---

## 6. FastRP — Graph Embeddings

### Practical use case
Turn every node into a short vector so ordinary ML can consume graph
structure: fraud-model features, churn prediction, recommendation
embeddings.

### Example 1: fraud features for XGBoost

Raw data — the same account/transfer graph as before, but the *output* is a
table:

```
GRAPH IN                          EMBEDDINGS OUT (dim=4 shown; real: 256)
   a1 -- a2 -- a3                 acct  v0     v1     v2     v3
         |                        ----  -----  -----  -----  -----
   a4 -- a5                       a1    +0.12  -0.80  +0.31  +0.05
                                  a2    +0.15  -0.77  +0.29  +0.09   <- near a1
                                  a5    -0.60  +0.11  -0.44  +0.72   <- far away
                                  ...then: XGBoost(embeddings + amounts) -> fraud score
```

How the algorithm works — (1) give every node a RANDOM sparse vector; (2)
repeatedly replace each node's vector with the (weighted) AVERAGE of its
neighbors' vectors; (3) the final embedding is a weighted sum of the
iterations. Structure emerges because averaging mixes neighborhoods:

```
init (random):   a1:[+1  0  0 -1]   a2:[ 0 +1 -1  0]   a3:[-1  0 +1  0]

iter 1:          a2 <- avg(a1, a3, a5) = mix of its neighborhood
iter 2:          a2 <- avg of 2-hop neighborhood
iter 3:          a2 <- avg of 3-hop neighborhood

nodes with similar neighborhoods average toward similar vectors --
no training, no gradients; just sparse random projection + smoothing.
```

Storage & RAM shape — GDS keeps THREE full float arrays (result + two
iteration buffers): at dim=256 that is ~3 KB/node -> 254 GB on LDBC100.
Our plan:

```
GDS:   [f32 x 256] x 3 per node, all resident        ~3 KB/node
OURS:  [int8 x 256] x 2 ping-pong (quantized)        ~0.5 KB/node
       + process one stratum at a time, flush finished embeddings to a
         sidecar file, stream edges per iteration
RAM:   ~8-10 GB for the same job (modeled)
```

### Example 2: churn prediction from a social graph

```
RAW: user friendships + monthly activity table
Embedding captures "who you sit near in the graph";
churn model learns: users embedded near past churners churn next.
No hand-engineered graph features (degree, triangles) needed — the
embedding subsumes them.
```

---

## 7. Triangle Counting / Clustering Coefficient

### Practical use case
"How clumpy is each node's neighborhood?" — fake-account detection (bots
have no mutual friends), social-capital scoring, community quality.

### Example 1: bot detection

Raw data — an undirected friendship graph:

```
REAL USER r (friends know each other)    BOT b (bought followers, strangers)

      f1 --- f2                              g1    g2
       \    /  \                               \   /
        \  /    \                               \ /
         r ----- f3                              b     g3
                                                  \   /
r's triangles: (r,f1,f2) (r,f2,f3)                 (none: g1,g2,g3 have
clustering(r) = 2 triangles / 3 possible = 0.67     no edges to each other)
                                                   clustering(b) = 0.0  <- flag
```

How the algorithm works — for each edge (u,v), count common neighbors by
intersecting sorted adjacency lists:

```
N(r)  = [f1, f2, f3]         intersect N(r) x N(f2):
N(f2) = [f1, r,  f3]           f1 in both?  yes -> triangle (r,f2,f1)
                               f3 in both?  yes -> triangle (r,f2,f3)
```

The classic optimization — RANK-ORIENT the graph: order vertices by degree,
keep only edges pointing from lower rank to higher rank. Every triangle is
then counted exactly once, and hub-vertex list intersections shrink
massively:

```
undirected:  r -- f1, r -- f2, r -- f3, f1 -- f2, f2 -- f3   (2E entries)
rank-oriented (degree order f1 < f3 < f2 < r):
             f1 -> f2, f1 -> r, f3 -> f2, f3 -> r, f2 -> r   (E entries,
             each triangle has exactly one "apex" wedge)
```

Storage & RAM shape — GRAIN's degree-ranked layout IS rank orientation,
already paid at snapshot-build time:

```
GDS:   undirected projection = ~2x edges, all in heap
OURS:  forward lists only (already rank-sorted on disk), stream wedge
       blocks; RAM = counts[V] + pinned hot-stratum hubs  -> ~1-4 GB
       on a 100 GB-class job, and the count is EXACT (no approximation).
```

### Example 2: community quality audit

```
After Louvain (family #2) assigns communities, compute the average
clustering coefficient per community:

  community M (mule ring):   clustering = 0.71  <- genuinely dense, real ring
  community X (accidental):  clustering = 0.04  <- an artifact, ignore it

Triangles grade the output of the other algorithms.
```

---

## The One-Picture Summary

```
                     RESIDENT IN RAM (O(V))      STREAMS FROM DISK (O(E))
WCC                  labels[V]                   edges, per pass
Louvain              community[V] + capped tally edges, per level (shrinking)
PageRank             rank[V] x2                  edges, per iter (delta-shrinking)
NodeSimilarity       sketches[V] + top-k heap    finalists' exact lists only
Shortest paths       bitset/dist[V] + queue      ONLY the blocks the search enters
FastRP               int8 buffers[V] x2          edges, per iter
Triangles            counts[V] + hot hubs        wedge blocks, once

The common trick: never require the O(E) plane to be resident.
The common product: the manifest knows V, E, and the degree CDF, so every
row of this table is a number you can print BEFORE the run starts.
```
