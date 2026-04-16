# Knight Bus Rust Vs Neo4j ELI5

## Big Idea

The new result is simple:

**we now have a fresh, same-machine, same-dataset, same-query-corpus comparison where the Rust `knight-bus` binary is measured directly against Neo4j.**

That matters because this is no longer the older Python snapshot reader pretending to speak for the Rust product.

## Why It Matters

Commit `cb834a3` gave us a local Neo4j smoke ladder.

That was useful, but it still left one important gap:

- the Neo4j runner was comparing Neo4j against the old Python WALK snapshot path
- our current product claim is about the Rust binary

So the next step had to be:

- keep the same graph generator
- keep the same query corpus idea
- keep the same Neo4j import path
- but swap the WALK side to the real Rust CLI

In plain English:

we stopped saying "this older helper is close enough" and made the benchmark use the thing we actually want to stand behind.

## What Changed After The Last Commit

After commit `cb834a3`, the work became more exact.

We added a new Rust CLI path:

```bash
knight-bus bench-corpus \
  --snapshot <dir> \
  --nodes-csv <path> \
  --edges-csv <path> \
  --corpus <path> \
  --report <path>
```

That command does four important jobs:

1. opens the Rust snapshot once
2. validates parity on the exact benchmark corpus
3. measures only the corpus families shared with the Neo4j harness
4. writes an engine-measurement JSON file in the same shape as the Neo4j report bundle

We also changed the smoke ladder so it now:

- builds a Rust snapshot for the real Rust benchmark
- keeps a separate Python-format snapshot only to generate the old query corpus format
- imports the same CSV truth into Neo4j
- benchmarks both engines on the same corpus
- writes a root-level `Final-Testing-Journal.md`
- appends the same facts into `docs/journal-tests-202604.md`

That middle trick is important.

Think of it like printing the same map in two formats:

- one format is only used to choose the questions
- the other format is the real one used in the race

So we did **not** benchmark two different graphs.
We benchmarked the same graph truth, but used the minimum adapter needed to keep the shared corpus machinery alive.

## Core Ideas Made Simple

### 1. Same Questions, Same Test

The fair part of the benchmark is not just "same dataset."

It is:

- same synthetic graph
- same query corpus
- same hop families
- same machine
- same run window

The shared query families are:

- `forward_one`
- `reverse_one`
- `reverse_two`

That means both systems are being asked the same neighborhood questions.

This is like giving two students the same exam instead of asking one algebra and the other spelling.

### 2. The Rust Side Is Now Real

Before this pass, the Neo4j ladder used a Python snapshot reader on the WALK side.

After this pass, the benchmark actually shells out to the Rust binary and reads back the Rust report.

That means the journal is now grounded in:

- Rust snapshot open time
- Rust parity on the chosen corpus
- Rust hop latency
- Rust process RSS during that benchmark command

So the benchmark is finally about the real executable, not a nearby cousin.

### 3. What The Fresh Rerun Proved

On the fresh rerun:

- `1 MB` completed with both engines `ok`
- `50 MB` completed with both engines `ok`
- parity passed in both tiers

The top-line story is:

- Rust won hard on open time and hop latency at both tiers
- Rust also won on RSS at `1 MB`
- Neo4j won on RSS at `50 MB`

That last line is the one that needs the most honesty.

### 4. Why The `50 MB` Rust RSS Got So Big

The `50 MB` Rust RSS number is real for the command we ran, but it does **not** mean:

- the immutable walk runtime by itself needs `388 MB`
- the snapshot access path secretly loads the whole graph into RAM

The current `bench-corpus` command does more than pure walking.

It also:

- loads `nodes.csv`
- loads `edges.csv`
- builds the truth index
- runs parity before timing
- then runs the latency passes

So the measured RSS is for the whole benchmark process, not just the lean mmap walker.

The best analogy is:

- we measured the chef, the kitchen table, the recipe book, and the stove together
- not just the stove flame

That is why the `1 MB` result looks wonderfully lean, but the `50 MB` result reminds us that our current benchmark command is still mixing "correctness machinery" with "engine runtime."

## Tiny Example

Here is the plain-English version of the benchmark stack now:

```text
same nodes.csv + edges.csv
  -> Rust snapshot
  -> query corpus
  -> Rust bench-corpus
  -> Neo4j import
  -> Neo4j benchmark
  -> one merged report
```

That is much better than:

```text
same dataset
  -> old helper on one side
  -> real database on the other side
  -> hope the comparison is close enough
```

The newer version is a real face-to-face test.

## What The Numbers Say Right Now

The fresh root journal says:

- at `1 MB`, Rust was much faster on open and hop latency, and also lower on RSS
- at `50 MB`, Rust was still much faster on open and hop latency, but Neo4j showed lower RSS for the full benchmark process

So the honest public claim right now is:

**on the shared `1 MB` and `50 MB` hop corpus, the Rust binary matched Neo4j’s answers and was dramatically faster on traversal latency.**

The honest non-claim is:

**we have not yet shown that the Rust runtime itself has the final best memory story at `50 MB`, because the current benchmark command still includes truth-loading and parity work in the same process.**

## What To Do Next

The next improvement is obvious:

- split "truth/parity setup" from "engine-only latency and RSS measurement"

That will let us answer the sharper question:

- how much memory does the mmap walk runtime itself need once correctness setup is out of the way?

Only after that should we try to turn the `50 MB` memory story into a stronger public claim.

## What To Remember

This pass turned the benchmark from "Rust-adjacent" into "Rust for real," and it proved the speed story clearly, even while it also exposed that our current `50 MB` memory number still includes more kitchen than stove.
