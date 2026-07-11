# Incremental Delta Iteration — Mermaid

| Field | Value |
| --- | --- |
| Kind | execution |
| Pair | `incremental-delta-iteration-ascii.md` / `incremental-delta-iteration-mermaid.md` |
| One-line job | Compute over CHANGES instead of states: collections are streams of (data, time, diff) updates, iteration is semi-naive (only recent tuples drive the next round), and progress is tracked by antichain frontiers — so a graph algorithm updates in milliseconds when one edge changes |

## 1. The regime change

```mermaid
flowchart TD
    B["batch regime (analytics category):<br/>one edge changes -> recompute BFS<br/>from scratch, O(V+E)"]
    D["differential regime:<br/>one edge changes -> ONE input record;<br/>output changes by exactly the<br/>affected deltas"]
    B -->|"represent collections as<br/>signed update streams"| D
    D --> HOW["how: every operator is built to be<br/>correct over (data, time, diff) updates —<br/>then every algorithm is automatically<br/>batch AND incremental"]
```

## 2. The data shape

```mermaid
flowchart LR
    C["Collection&lt;T, C&gt;<br/>(collection.rs:24) =<br/>timely Stream of<br/>(data, time, diff)"]
    C --> SUM["collection AT time t =<br/>sum of all diffs with time <= t<br/>insert: +1, delete: -1<br/>(Abelian, difference.rs)"]
    C --> ARR["Trace/TraceReader (trace/mod.rs):<br/>the same updates INDEXED by key —<br/>an LSM of update batches;<br/>arrange once, share across joins"]
```

## 3. Frontiers: knowing when a time is done

```mermaid
flowchart TD
    A["Antichain&lt;T&gt; (frontier.rs:20):<br/>mutually incomparable times;<br/>less_equal test (:162);<br/>MutableAntichain (:380) counts<br/>pending times via ChangeBatch (:16)"]
    A --> M["meaning: no future message carries<br/>a time earlier than any element"]
    M --> E1["frontier passes t -> collection at t<br/>is FINAL: reduce may emit"]
    M --> E2["trace may COMPACT diffs behind<br/>the frontier: memory ~ current<br/>collection, not history"]
    A --> P["times can be Products:<br/>outer time x loop counter —<br/>lattice join/meet (lattice.rs:31/:72);<br/>nested iteration falls out of<br/>the partial order"]
```

## 4. Semi-naive iteration (the datafrog miniature)

```mermaid
flowchart LR
    TA["to_add:<br/>facts found THIS round"]
    RC["recent:<br/>facts found LAST round"]
    ST["stable:<br/>all facts so far"]
    TA -->|"changed(): dedup vs stable"| RC
    RC -->|"changed(): fold in"| ST
    RC --> RULE["rule: every join uses at least one<br/>RECENT input -> work per round ~<br/>NEW facts, not all facts<br/>(variable.rs:26-47)"]
    RULE --> GEN["differential generalizes: 'recent' =<br/>updates not yet behind the frontier;<br/>and signed diffs make DELETION work"]
```

## 5. BFS in six lines

```mermaid
flowchart TD
    R["roots.map(|x| (x, 0))"]
    R --> IT["iterate(|inner| ...)  (bfs.rs:25-43)"]
    IT --> J["inner.join_core(edges_arranged,<br/>|_k, l, d| (d, l+1))"]
    J --> CC["concat(nodes)"]
    CC --> RD["reduce: keep MIN distance per node"]
    RD --> IT
    IT --> OUT["one definition = batch BFS +<br/>incremental BFS under insert/delete +<br/>multi-root BFS — operators are<br/>update-correct, so the algorithm is"]
```

## 6. Worked example — one edge insertion

```mermaid
sequenceDiagram
    participant I as input
    participant J as join (round 1)
    participant R as reduce at c
    participant O as output
    Note over I: graph a->b->c, root a;<br/>dists a:0 b:1 c:2
    I->>J: ((a,c), t1, +1)
    J->>R: (c, 1) at (t1, round 1)
    R->>O: ((c,2), t1, -1)  retract old min
    R->>O: ((c,1), t1, +1)  assert new min
    Note over O: total work ~ 2 changed tuples;<br/>the untouched million nodes cost ZERO —<br/>batch engines pay O(V+E) again
```

## 7. Worked example — frontiers gate emission

```mermaid
flowchart TD
    Q["reduce(min) at c: may I emit 'min=1'?"]
    Q --> F1["frontier = {(t1, round 1)} -><br/>all updates at (t1, round 0)<br/>are complete: SAFE"]
    Q --> F2["with Product times the frontier is an<br/>ANTICHAIN: {(t1,r2), (t2,r0)} pending<br/>simultaneously — less_equal (:162)<br/>is the only safe test"]
    F1 & F2 --> CP["and once the frontier passes t:<br/>coalesce all diffs <= t into one —<br/>compaction is the LSM story<br/>(storage synthesis) replayed in time"]
```

## 8. Position in the corpus

```mermaid
flowchart TD
    OLD["every OLAP pattern so far —<br/>push/pull frontiers, Pregel supersteps,<br/>power iteration — RECOMPUTES"]
    OLD --> NEW["this category: pay once at definition<br/>(arrange + update-correct operators),<br/>stay fresh under mutation forever"]
    NEW --> RW["rewrite thesis: GDS's project-then-run<br/>snapshot lag could be replaced by standing<br/>differential WCC/BFS/PageRank over the<br/>transaction stream — the OLAP-visibility<br/>problem dissolves instead of being managed"]
    NEW --> FL["kin: Flink watermarks =<br/>single-dimension frontiers; antichains<br/>generalize them to nested loops"]
```

## 9. The self-testing property

```mermaid
flowchart LR
    SAME["one dataflow definition"]
    SAME --> B2["run from scratch<br/>on final input"]
    SAME --> I2["run incrementally<br/>through 1000 updates"]
    B2 & I2 --> EQ["consolidated outputs MUST be equal —<br/>a free differential oracle the engine<br/>uses on itself; a rewrite inherits<br/>the same self-check for free"]
```

## 9b. Cost model — when incremental wins and loses

```mermaid
flowchart TD
    Q["should a workload go differential?"]
    Q --> W["WINS: small updates against big state —<br/>1 edge into 10M: work ~ affected deltas;<br/>standing queries re-answered per commit;<br/>deeply iterative logic (nested loops via<br/>Product timestamps) that batch engines<br/>restart from round 0"]
    Q --> L["LOSES: one-shot full scans (arrangement<br/>overhead buys nothing you'll reuse);<br/>updates touching most keys anyway<br/>(delta ~ state: pay batch price PLUS<br/>trace bookkeeping); tiny inputs where<br/>frontier coordination dominates"]
    W & L --> H["heuristic from the sources: the win is<br/>proportional to state/delta ratio times<br/>reuse count — arrangements amortize<br/>across operators AND across time"]
```

## 10. Citing repos

| Repo | Path | Role |
| --- | --- | --- |
| timely-dataflow | `reference-repos-corpus/timely-dataflow-src/timely/src/progress/frontier.rs` | Antichain/MutableAntichain (20, 162, 380) |
| timely-dataflow | `reference-repos-corpus/timely-dataflow-src/timely/src/progress/change_batch.rs` | counted progress changes (16) |
| differential-dataflow | `reference-repos-corpus/differential-dataflow-src/differential-dataflow/src/collection.rs` | Collection = update stream (24) |
| differential-dataflow | `reference-repos-corpus/differential-dataflow-src/differential-dataflow/src/lattice.rs` | Product-time join/meet (31, 72) |
| differential-dataflow | `reference-repos-corpus/differential-dataflow-src/differential-dataflow/src/algorithms/graphs/bfs.rs` | six-line incremental BFS (12-43) |
| datafrog | `reference-repos-corpus/datafrog-src/src/variable.rs` | semi-naive stable/recent/to_add (26-47) |

## 11. Cross-references

- Sibling patterns: `frontier-push-pull-switching` (the batch
  regime), `pull-operator-pipeline` (21 — one query vs one
  standing computation), `lsm-compaction-leveling` (traces
  compact like LSMs).
- Reading order: datafrog's variable.rs first (the 200-line
  miniature), then differential's bfs.rs, then frontier.rs
  when the "when may I emit?" question bites.
- Next: bench-testing (SQLancer/Jepsen/LDBC) closes the
  corpus categories.
- Repo layout note for readers: the differential crate lives at
  `differential-dataflow-src/differential-dataflow/` (workspace
  root also carries dogsdogsdogs — worst-case-optimal joins —
  and the SIGMOD/OSDI paper PDFs, worth reading beside the
  code); timely's progress machinery is all under
  `timely-dataflow-src/timely/src/progress/`.
- Terminology bridge: "arrangement" = shared index of update
  batches; "trace" = its storage; "frontier" = watermark
  generalized to partially ordered time; "consolidation" =
  summing diffs at equal (data, time) — canonicalization,
  in the corpus's verification vocabulary.
