# Knight Bus Open Path And Minimum Proof ELI5

## Big Idea

The code now makes the main idea much clearer:

**Knight Bus is fast because it opens a prebuilt graph image and walks it like a map, instead of rebuilding the graph like a database every time it starts.**

After the latest commit, the important conversation became smaller and sharper:

- what exactly makes the open path fast
- what that storage phenomenon is called
- what the bare minimum proof should be even if the larger `20 GiB` target slips

## Why It Matters

Commit `47bc65b` was a real checkpoint.

That commit:

- rechecked the tiny harness against the real Parseltongue fixture
- switched benchmark summaries to nanoseconds
- recorded the macOS RSS caveat
- confirmed that build, verify, query, and bench are working on the tiny harness

That was useful, but it still left a deeper question:

**why does this runtime shape feel so much lighter than a normal graph database runtime?**

The answer is not just "CSR is fast."

The deeper answer is:

- build time does the organizing
- snapshot files keep only the data the walk needs
- open time maps those files instead of rebuilding rich objects
- query time mostly does direct indexed reads

That is why the runtime feels like calculation plus page access, not like relationship reconstruction.

## What Changed After The Last Commit

After commit `47bc65b`, the discussion became more code-backed and more honest.

Before, the repo had already explained the storage thesis well.

After reading the code closely, the newer understanding became:

1. the implementation really does follow the thesis
2. the open path win is mostly "map and validate", not "load and rebuild"
3. the query path win is mostly "dense id plus offsets plus contiguous peers"
4. the current built-in macOS `peak_rss_bytes` field is still not trustworthy enough for big claims
5. the bare minimum finish line is correctness parity plus different memory shape on the same fixed dataset, even if we do not reach `20 GiB` yet

So the conversation moved from:

"this storage idea sounds promising"

to:

"the code now shows why this shape should be fast, and we know exactly what must still be proved honestly."

## Core Ideas Made Simple

### 1. Open Path Means "Map The Library", Not "Unload The Library"

Think about two ways to use a library.

Bad way:

- carry all the books into your room first
- sort them into piles
- rebuild the shelves
- then start reading

Better way:

- open the catalog
- walk to the right shelf
- take only the book you need

Knight Bus is trying to do the second thing.

The runtime open path is roughly:

- read `manifest.json`
- `mmap` `forward.offsets.bin`
- `mmap` `forward.peers.bin`
- `mmap` `reverse.offsets.bin`
- `mmap` `reverse.peers.bin`
- `mmap` `node_table.bin`
- `mmap` `strings.bin`
- `mmap` `key_index.bin`
- validate that the files are the right shape

The crucial point is:

- the runtime does **not** rebuild the graph into heap objects
- the runtime does **not** derive reverse edges at startup
- the runtime does **not** parse one edge record at a time into richer structures

It simply maps already-prepared bytes and checks that they are safe to trust.

That is why open can stay small and boring.

### 2. The Query Path Turns Graph Work Into Indexed Reads

The code now shows a very simple hot path.

In plain English, query time feels like this:

```text
entity key
  -> binary search sorted key index
  -> dense integer id
  -> offsets[id]
  -> offsets[id + 1]
  -> peers[start..end]
  -> maybe repeat for one more hop
```

This is the important transformation.

We are turning:

- "find relationships"
- "decode rows"
- "rebuild neighbors"

into:

- "calculate where the slice starts"
- "calculate where the slice ends"
- "read the slice"

So yes, a good short way to say it is:

**the runtime converts graph operations into predictable calculation-driven reads over immutable arrays.**

That is not the whole story, but it is the right story.

### 3. What This Phenomenon Is Called

There is no single perfect label, but these are the best names for what is happening.

#### Memory-mapped immutable snapshot

This means:

- the graph is stored as frozen files
- the runtime maps those files into memory
- the OS brings in pages on demand

This explains why the system can avoid loading the whole graph into heap memory at open.

#### Data-oriented design

This means:

- shape the bytes for the access pattern
- keep the runtime path close to arrays and arithmetic
- avoid pointer-heavy object graphs if the workload does not need them

This explains why the walk path feels direct and dense.

#### Storage-runtime alignment

This means:

- store the graph in the same shape the runtime wants to read it
- let the hot path be visible in the bytes

This is probably the best repo-specific phrase.

#### Direct indexing

This means:

- dense IDs
- offsets as small seek aids
- peers as compact payload

This explains why a hop becomes "index, slice, continue."

#### Demand paging

This means:

- the OS pulls in file pages when touched
- untouched pages can stay out of the active working set

This explains why a large snapshot does not automatically mean a large heap image.

So if someone asks for one short sentence, the safest answer is:

**Knight Bus uses a memory-mapped immutable snapshot with storage-runtime alignment, so graph hops become direct indexed reads instead of object reconstruction.**

### 4. Why This Feels Different From A Graph Database

It is not that other graph systems were too dumb to think of CSR.

The real issue is that many graph databases are trying to preserve broader promises:

- live updates
- richer edge and property filtering
- arbitrary query shapes
- transactions
- planner and executor flexibility
- product features that go beyond neighborhood walking

Those promises are real, but they make the hot path heavier.

Knight Bus is making a different trade:

- frozen snapshot
- narrow workload
- exact key handoff
- precomputed reverse plane
- tiny set of traversal families

That is why the runtime can stay so simple.

This is not "the only smart graph design."

It is "a sharper design for one narrower workload."

### 5. The Code Makes One More Hidden Trade Very Clear

The truth layer keeps richer rows:

- `node_type`
- `label`
- `parent_id`
- `file_path`
- `span`
- `edge_type`

But the runtime layer mostly throws that away for walking.

The normalized graph keeps:

- sorted node keys
- forward offsets
- forward peers
- reverse offsets
- reverse peers

The snapshot then adds only enough metadata to recover keys:

- `node_table.bin`
- `strings.bin`
- `key_index.bin`

That is a very important lesson.

The runtime gets faster not only because the files are flat, but also because the hot path is carrying much less semantic furniture.

## Tiny Example

Imagine the query is:

```text
fn:login_user_flow_now
```

The simplified runtime story is:

1. look up that key in the sorted key index
2. get dense id `23`
3. read `forward_offsets[23]`
4. read `forward_offsets[24]`
5. slice `forward_peers[start..end]`
6. map those peer ids back to keys
7. if this is a two-hop query, repeat for the one-hop frontier

That is why the current code feels closer to opening drawers than to asking a general database a fresh question at every step.

## What The Last Commit Already Proved

The latest commit already proved some real things on the tiny harness.

It showed that:

- build works
- verify works
- query works
- bench works
- nanosecond reporting is in place
- the snapshot answers match the CSV truth layer on the tiny harness

That matters because it proves the basic pipeline is not hypothetical anymore.

But it did **not** prove everything.

The most important open caveat is still:

- the built-in macOS `peak_rss_bytes` field is not trustworthy yet for strong memory claims

So we should treat the current memory comparison path with caution until it is fixed or replaced with a more reliable measurement method.

## What The Bare Minimum Proof Is Now

If we finish the project honestly, the bare minimum thing we want to be able to say is:

1. for the same fixed `nodes.csv` and `edges.csv` dataset, Knight Bus matched the Node.js plus Neo4j baseline on the agreed forward and backward one-hop and two-hop query set
2. for that same dataset and query set, Knight Bus showed a materially different memory shape because it walked a compiled immutable snapshot instead of running a general graph-database runtime

That is still a good minimum proof even if the larger `20 GiB` target is not reached yet.

But there is one important honesty rule:

- if we do not reach `20 GiB`, we do **not** get to claim the `20 GiB on a 16 GB laptop` result yet

In that case, the honest claim becomes:

- correctness matched on the tested dataset
- memory and latency shape looked different on the tested dataset
- larger-size proof remains a later milestone

That is not failure.

That is the smallest believable proof.

## What To Remember

- The open path is fast because it maps and validates bytes instead of rebuilding the graph.
- The walk path is fast because dense IDs plus offsets turn neighbor lookup into direct indexed reads.
- This is best described as storage-runtime alignment over a memory-mapped immutable snapshot.
- The latest commit proved the tiny harness path is working, but not yet the full memory story.
- The bare minimum final proof is correctness parity plus different memory behavior on the same fixed CSV dataset, even if the `20 GiB` stretch goal slips.

## Sticky Sentence

Knight Bus wins by doing the hard graph thinking once at build time, so runtime mostly just opens the map and follows the shelves.
