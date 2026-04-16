# Why Rust RSS Looked High

Short answer: partly yes, but that is not the full story.

## What was true

During build-time, Knight Bus has to turn `nodes.csv` and `edges.csv` into the persisted dual-CSR walk snapshot. That means:

- reading the full CSV truth layer
- assigning dense IDs
- sorting keys
- deduplicating edges
- compiling forward and reverse adjacency
- writing the immutable snapshot files

That CSV-to-walk-snapshot work can push RAM up on large datasets.

## What was also true

The high Rust `rss_bytes` in the `bench-corpus` reports was not only about CSV-to-CSR conversion.

By the time `bench-corpus` runs, the snapshot already exists. The current `v001` benchmark path still:

- reloads `nodes.csv` and `edges.csv`
- rebuilds truth/parity state in-process
- checks corpus parity before timing
- then runs the snapshot walker

So the reported Rust RSS is a mixed number:

- some of it is truth-layer/parity machinery
- some of it is snapshot runtime working set
- it is not a pure walker-only runtime number yet

## Best v001 phrasing

The most accurate way to say it is:

Rust looked high on RAM in `v001` because the current pipeline still mixes CSV-truth handling and parity scaffolding with the walk runtime. Build-time CSV-to-dual-CSR compilation is part of that story, but the benchmark RSS also includes extra CSV truth loading during `bench-corpus`.

## What to fix later

To get a cleaner runtime-memory claim, split the flow into two processes:

1. parity/truth precheck process
2. pure snapshot query benchmark process

That would let the future RSS number represent the walker itself much more honestly.
