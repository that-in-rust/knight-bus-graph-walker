# Rewrite Sampling And The Known-Endpoint Convergence Thesis

Date: 2026-07-08
Origin: live session experiment plus dialogue; supersedes parts of the
sequencing logic in `docs_PRD05/Sol-01.md` under the AI-regime assumption.

---

## 1. Answer First

```text
Old model (PRD05):   effort ~ lines of behavior to implement
                     -> pick one algorithm, go deep, 90 days

New model (PRD06):   effort ~ cost of the error signal, not the code
                     -> code is nearly free and regenerable
                     -> the differential harness IS the critical path
                     -> build the fitness function first,
                        then let generation grind against it
```

A rewrite with a known, running endpoint (stock Neo4j + GDS) is not a design
problem. It is a **search problem**: the endpoint is an executable
specification you can query, diff against, and use as an error signal. AI
collapses the iteration cost of that loop to near zero. What it does not
collapse is the cost of *seeing* mismatches — and that is where all the
remaining human effort belongs.

---

## 2. The Sampling Experiment

To test the "typing is the bottleneck" assumption, a rewrite was started cold
in `rust-rewrite-01/` (left uncommitted; it is a probe, not a product):

- 7-crate cargo workspace mirroring Neo4j's layer decomposition
  (`kb-values` → `kb-graph` → `kb-store` → `kb-cypher` → `kb-gds` →
  `kb-procedures` → `kb-shell`), clippy `unwrap/expect/panic = deny`.
- `kb-values` (~430 lines, complete): the PackStream-parallel `Value`
  universe, Cypher three-valued NULL logic, the ORDER BY global sort order,
  typed errors, 5 REQ contracts including a property test for total ordering.
- `kb-graph` (~340 lines, complete): newtype IDs, a fully immutable `Graph`
  where every write is a pure `Graph -> Graph` function with Arc structure
  sharing (functional MVCC / reader isolation), label scans, directional
  expand, `db.labels` parity, 6 REQ contracts.

Elapsed writing time: about five minutes. Test-first, spec-first
(`REQ-KB-VAL-*`, `REQ-KB-GRAPH-*`), idiomatic functional Rust.

### What the sample proves

- Line count is no longer the cost driver. Two clean layers of a graph
  database appeared at a rate that would have been a week of 2010s work.

### What the sample deliberately does not prove

- Nothing was compiled or run; by the project's own TDD standard, nothing
  counts as done.
- The two crates are the *easiest* layers — the parts every graph database
  has. The moat parts (WAL/recovery, Cypher semantics, GDS parity, low-RAM
  execution, receipts) were untouched.
- As a fraction of "an exact parallel of Neo4j": well under 0.1%.

The sample is a measurement of generation speed, not of program progress.

---

## 3. Effort Re-Estimation Under The AI Regime

The dialogue moved through three positions, each partially correct:

### Position 1: "It took Neo4j ~20 years, so a rewrite is 3–6 years"

Wrong as stated. The 3–6 year figure (from
`docs_PRD05/Neo4j-Rust-Rewrite-Feasibility.md`) already assumed a rewrite,
not re-discovery. But it was derived under a pre-AI cost model.

### Position 2: "It's a rewrite, so the discovery cost disappears"

True but insufficient. The rewrite discount is real — no product discovery,
no design dead-ends, a frozen target, a free oracle. Precedents suggest 2–4x:
ScyllaDB took ~4 years to production parity with Cassandra's surface;
Memgraph took years for Bolt+Cypher compatibility alone and still diverges.
The cost of a rewrite was never "figuring out how" — it is reproducing and
**verifying** tens of thousands of observable behaviors.

### Position 3: "We did not have AI then"

The strongest correction, but the acceleration is asymmetric:

| Work type | AI speedup | Why |
| --- | ---: | --- |
| Typing volume: boilerplate, mechanical ports, encoders, test scaffolding, the ~400-procedure config surface | 10–50x | Pure generation; the sampling experiment measured this directly |
| Semantic archaeology: deciding which behaviors are contract vs. accident; cross-component debugging; concurrency/recovery correctness; verification wall-clock | 2–3x | Never typing-bound; requires runs, datasets, and judgment |

And AI adds a new cost: plausible-looking wrong code is produced at the same
speed as right code. Nobody can eye-review 500k generated lines, so the
differential oracle becomes *more* essential, not less.

### Revised planning estimates

```text
Selected surface (Bolt canary + Cypher subset + top GDS procedures,
differential-verified):        1-2 quarters   (was 2-4 quarters)

Full Community + OpenGDS behavioral parity:
                               1-2 years      (was 3-6 years)
```

Not weeks — because the tail is verification and semantic archaeology, which
AI accelerates least.

---

## 4. The Convergence Thesis

> A known endpoint turns a rewrite from a design problem into a search
> problem.

Stock Neo4j is an executable spec. The loop is:

```text
        +-------------------------------------------+
        |                                           |
        v                                           |
   generate Rust  ->  run differential tests  ->  mismatches
   (nearly free)      against stock Neo4j/GDS      as error signal
                          |
                          v
                   zero mismatches on covered surface
                   = converged (for that surface)
```

For everything reachable by this loop, convergence is iterative and almost
mechanical. That is plausibly ~80% of the Neo4j surface, and that 80% just
became cheap.

### The three convergence conditions

The guarantee holds only where three conditions are met. They define exactly
where the residual human work lives.

#### Condition 1: The endpoint must be observable

| Observable through query diffing | Not observable through query diffing |
| --- | --- |
| Cypher results | Crash recovery correctness |
| Bolt/PackStream bytes | Race windows and lock fairness |
| GDS procedure outputs | Memory behavior under pressure |
| Error codes and messages | Timing-dependent behavior |

No fitness signal, no gradient. The right column needs engineered harnesses
(failpoint injection, deterministic schedulers, cgroup measurement) before
the loop can even *see* those behaviors.

#### Condition 2: The test signal must cover the surface

The loop converges to the **tests**, not to Neo4j. Weak coverage produces
reward hacking: code that passes the suite and diverges everywhere else.
Corollary:

> The generated Rust is a regenerable artifact. The coverage of the
> differential harness is the asset — it is the compressed form of the
> endpoint.

#### Condition 3: The endpoint must be a point, not a cloud

Some Neo4j behavior is deliberately unspecified: result order without
`ORDER BY`, Louvain nondeterminism, timing-dependent errors. There the loop
oscillates unless a human first defines what "equal" means — the same
canonicalization decisions PRD05 already made for WCC (partition parity under
min-member label normalization).

---

## 5. Program Design: Harness First

This inverts PRD05's sequencing. Sol-01 allocated ~15% of the quarter to the
WCC kernel and ~60% to verification infrastructure around it, treating the
algorithm as the deliverable. Under the convergence thesis the allocation
was accidentally right but the framing was backwards:

```text
PRD05 framing:  the algorithm is the product; verification protects it.
PRD06 framing:  the harness is the product; algorithms fall out of the loop.
```

### Human effort goes only to the three conditions

1. **Observability harnesses**: dockerized stock Neo4j/GDS as the oracle;
   failpoint and crash-schedule injection; cgroup-measured runs for the
   low-RAM claims.
2. **Coverage**: differential test corpus spanning the claimed surface —
   openCypher TCK scenarios for Cypher, GDS fixture extraction for
   procedures, adversarial ID/topology generators for the kernel.
3. **Equivalence definitions**: a written, versioned statement per surface of
   what "same as Neo4j" means (exact bytes, canonicalized partition,
   tolerance band, or explicitly-unspecified).

Everything else is loop fodder: generate against the harness, feed failures
back, regenerate. Breadth stops being expensive, so the old advice "pick one
algorithm and go deep" weakens; the new constraint is only how much surface
the harness can *judge*.

### Consequences for the existing plans

- `gtm-POC-01.md` (WCC-first) survives, but its justification changes: WCC
  is no longer "the one affordable algorithm" — it is the first equivalence
  definition simple enough to bootstrap the harness (exact partition parity).
- The 90-day plan's publication-hardening and receipt workstreams remain
  valid as *observability harnesses* for the non-query-diffable behaviors.
- The first buildable artifact of the next session should therefore be the
  loop itself: stock Neo4j in a container + a differential runner that any
  generated Rust must pass. The fitness function first; then let it grind.

---

## 6. Honest Limits Of This Thesis

- The "~80% mechanically convergent" figure is a judgment, not a
  measurement. It should be falsified the same way PRD05 falsified the
  seven-family percentages: instrument the loop and count.
- Convergence speed is bounded by test wall-clock, not generation. A corpus
  of thousands of TCK scenarios against a containerized Neo4j has a real
  cycle time; harness engineering includes making the loop *fast*.
- The sampling experiment measured greenfield layers with no integration
  pressure. Generation speed on cross-component semantic bugs (condition-1
  territory) is unmeasured and probably much lower.
- Licensing boundary unchanged from PRD05: the endpoint for differential
  testing is Community + OpenGDS surface; Enterprise behavior remains out of
  scope as an oracle.
