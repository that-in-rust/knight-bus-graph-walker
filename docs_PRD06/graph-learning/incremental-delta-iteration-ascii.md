# Incremental Delta Iteration — ASCII

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `incremental-delta-iteration-ascii.md` / `incremental-delta-iteration-mermaid.md` |
| One-line job | Compute over CHANGES instead of states: collections are streams of (data, time, diff) updates, iteration is semi-naive (only recent tuples drive the next round), and progress is tracked by antichain frontiers — so a graph algorithm updates in milliseconds when one edge changes |

## 1. The job

Batch engines recompute BFS from scratch when one edge changes.
Differential dataflow's answer: represent every collection as
an append-only stream of signed updates, and build all
operators to be correct over updates. Then "one edge changed"
is literally one input record, and the output changes by
exactly the affected deltas.

## 2. The data shape: (data, time, diff)

```text
differential-dataflow-src/differential-dataflow/src/
    collection.rs:24
    pub struct Collection<'scope, T: Timestamp, C> {
        pub inner: Stream<'scope, T, C>,   // timely stream of
    }                                      // (data, time, diff)
a collection at time t = SUM of all diffs with time <= t.
insert = diff +1, delete = diff -1 (Abelian difference.rs);
counts, not sets — so retractions compose algebraically.
trace/mod.rs: Trace/TraceReader — the same updates ARRANGED
(indexed by key, like an LSM of batches) for reuse by joins:
arrange once, share across every operator that needs the index.
```

## 3. Progress: antichain frontiers

```text
timely-dataflow-src/timely/src/progress/frontier.rs:
    :20  pub struct Antichain<T>        set of mutually
                                        incomparable times
    :162 pub fn less_equal(&self, time) frontier test
    :380 pub struct MutableAntichain    frontier w/ counted
                                        changes (change_batch.rs
                                        ChangeBatch :16)
meaning: "no future message will carry a time earlier than any
frontier element." When the frontier passes t, every operator
KNOWS the collection at t is final — that's when reduce can
emit, and when traces can compact times behind the frontier.
timestamps can be PRODUCTS of outer time x loop counter
(differential's lattice.rs join/meet :31/:72) — nested
iteration falls out of the partial order, no special machinery.
```

## 4. Semi-naive iteration: the datafrog miniature

```text
datafrog-src/src/variable.rs:26-47 (the same idea, 200 lines):
    pub stable: Vec<Relation>   all facts found so far
    pub recent: Relation        facts found LAST round
    to_add:    Vec<Relation>    facts found THIS round
each changed(): recent -> stable; to_add (dedup vs stable)
    -> recent
rule: joins must involve at least one RECENT input — work per
round is proportional to NEW facts, not all facts. Differential
generalizes: 'recent' becomes 'updates not yet behind the
frontier', and deletions work too (signed diffs).
```

## 5. BFS in six lines

```text
differential-dataflow-src/differential-dataflow/src/
    algorithms/graphs/bfs.rs:25-43 (bfs_arranged):
    nodes = roots.map(|x| (x, 0))
    nodes.iterate(|scope, inner|
        inner.join_core(edges, |_k, l, d| Some((d, l+1)))
             .concat(nodes)
             .reduce(|_, s, t| t.push((*s[0].0, 1))))  // min
edges is arranged ONCE (arrange_by_key, bfs.rs:17) and entered
into the loop. reduce keeps the minimum distance per node.
This one definition is simultaneously: batch BFS, incremental
BFS under edge insert/delete, and multi-root BFS — because the
operators are update-correct, the algorithm is too.
```

## 6. Worked example — one edge insertion

```text
graph: a->b, b->c, root a. distances: a:0 b:1 c:2.
insert edge a->c at time t1:
    input update:  ((a,c), t1, +1)
    loop round 1:  join produces (c, 1) at (t1, round 1)
    reduce at c:   old min 2 retracted, new min 1 asserted:
        output updates: ((c,2), t1, -1), ((c,1), t1, +1)
    downstream of c: nothing changes (c has no out-edges)
total work: proportional to the 2 changed tuples — the
untouched million-node remainder costs ZERO. A batch engine
pays O(V+E) again.
```

## 7. Worked example — why frontiers gate emission

```text
reduce(min) at node c must NOT emit "min=1" while an earlier
retraction might still arrive. Frontier protocol:
    frontier = {(t1, round 1)}  -> updates at (t1, round 0)
                                   are complete: safe to emit
    antichain, not a single time: with Product timestamps
    {(t1, r2), (t2, r0)} can be simultaneously pending —
    less_equal (frontier.rs:162) is the ONLY safe test
compaction: once the frontier passes t, the trace may coalesce
all diffs at times <= t into one — memory stays proportional
to the CURRENT collection plus in-flight changes, not history.
```

## 8. Why this matters for the corpus

Every OLAP graph pattern so far (frontier push/pull, Pregel
supersteps, PageRank power iteration) RECOMPUTES. This category
is the alternative regime: pay once at definition time (arrange
+ update-correct operators), then queries stay fresh under
mutation. For the docs_PRD06 rewrite thesis it is directly
load-bearing: GDS's "project a snapshot graph then run" lag
could instead be maintained incrementally — WCC/BFS/PageRank
as standing differential computations over the transaction
stream.

## 9. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| timely-dataflow | `reference-repos-corpus/timely-dataflow-src/timely/src/progress/frontier.rs` | Antichain/MutableAntichain (20, 162, 380) |
| timely-dataflow | `reference-repos-corpus/timely-dataflow-src/timely/src/progress/change_batch.rs` | counted progress changes (16) |
| differential-dataflow | `reference-repos-corpus/differential-dataflow-src/differential-dataflow/src/collection.rs` | Collection = stream of updates (24) |
| differential-dataflow | `reference-repos-corpus/differential-dataflow-src/differential-dataflow/src/lattice.rs` | join/meet for Product times (31, 72) |
| differential-dataflow | `reference-repos-corpus/differential-dataflow-src/differential-dataflow/src/algorithms/graphs/bfs.rs` | six-line incremental BFS (12-43) |
| datafrog | `reference-repos-corpus/datafrog-src/src/variable.rs` | semi-naive stable/recent/to_add (26-47) |

## 10. Cross-references

- Sibling patterns: `frontier-push-pull-switching` (analytics —
  the batch regime this replaces for standing queries),
  `pull-operator-pipeline` (21 — pull executes ONE query;
  differential maintains one FOREVER), `lsm-compaction-leveling`
  (storage — traces compact batches exactly like an LSM).
- Kin outside the pair: Flink's DataStream watermarks are
  single-dimensional frontiers; differential's antichains
  generalize them to nested iteration.
- Verification note: incremental vs from-scratch is a free
  differential oracle — run the same dataflow both ways, the
  consolidated outputs must be identical. The engine tests
  itself this way; a rewrite should too.
- Next: bench-testing (SQLancer/Jepsen/LDBC) to close the
  corpus categories.
