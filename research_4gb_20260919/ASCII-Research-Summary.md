# Useful Graph Answers On Four Gigabytes

The research proposes a complete graph-analysis workflow on
a small machine: choose storage for the actual question,
avoid unnecessary computation, and account for every stage.
Its strongest new architectures still need implementation
and scale measurements. This is a summary of the notes.

## 1. The Product And Its Budget

The intended outcome is a correct, useful answer before the
customer's deadline, followed by another useful answer after
the data changes. Dependency investigation is the recommended
first experiment because it extends an existing narrow proof.

```text
Customer supplies
  data + question + budget + deadline + freshness
                         |
                         v
              +---------------------+
              | Supported job plan  |
              +---------------------+
                         |
                         v
              +---------------------+
              | Algorithm-specific  |
              | storage + execution |
              +---------------------+
                         |
                         v
              +---------------------+
              | Complete answer     |
              | Original IDs        |
              | Snapshot identity   |
              +---------------------+
```

The budget uses decimal units. These are design targets,
not demonstrated whole-workflow limits:

```text
4 GB physical RAM
  |
  +-- 3 GB provisional worker allowance
  |     build OR query OR refresh: one heavy stage at a time
  |
  +-- 1 GB provisional OS / other-process reserve

50 GB retained prepared storage
  |
  +-- selected graph layouts, indexes and saved answers
  +-- unique blocks across retained / pinned versions

Input files + build scratch + recovery + extra output
  |
  +-- separately admitted peak workspace
```

The 50 GB figure here means prepared disk storage. Earlier
notes used 50 GB to mean a GDS in-memory projection; those
are different scenarios. A memory cap, completed job and
deadline guarantee are also three different claims. [S1], [S2]

## 2. Three Architectures, With Different Strengths

All three share input validation, IDs, versioning, resource
accounting and output handling. Their layouts and algorithms
remain different. An embedded partner is a delivery channel
for these architectures, not a fourth storage architecture.

```text
A. SNAPSHOT INVESTIGATION
   Typed adjacency on SSD
       |
       v
   Bounded frontier + reads grouped by physical location
       |
       v
   Dependency / reachability answers

   Strength: repeated questions on a reusable snapshot.
   Price: cold reads, large frontiers, output and refresh.

----------------------------------------------------------

B. STRUCTURAL COMPUTATION
   Memberships / exact repeated structure
       |
       v
   Avoid expanded edges or compute shared state once
       |
       v
   Answers reconstructed with the required semantics

   Strength: can reduce both memory and total work.
   Price: restricted operators; structure must be available.

----------------------------------------------------------

C. BUDGETED BATCH ANALYTICS
   Scan-friendly files on SSD
       |
       v
   Small active batches + external state + repeated passes
       |
       v
   Scheduled rank / similarity / embedding results

   Strength: complete jobs that exceed available RAM.
   Price: more I/O; useful only if the deadline still holds.
```

The notes prioritize five mechanisms: avoid materialization,
avoid recomputation, collapse equivalent computation,
improve locality, and externalize the remaining state.
The first three can remove work. Spilling state primarily
buys capacity and may increase elapsed time. [S3]

## 3. The Most Consequential Idea: Preserve Structure

A shared group can imply many entity-to-entity edges.
When the requested algorithm permits it, operate through
the memberships instead of constructing those edges.

```text
ONE GROUP WITH 1,000 MEMBERS

Expanded directed graph        Membership representation
  999,000 neighbor entries       1,000 memberships
             |                           |
             v                           v
  Read explicit pair links      Traverse via the group

Both must implement the same declared graph meaning.
```

D06 applies this to supported incidence-derived operators.
For 10 million entities in 10,000 disjoint groups of 1,000,
the model gives about 40.04 GB of expanded topology versus
0.16008 GB of two-way incidence topology: about 250x smaller.
This compares selected representations of a constructed
graph, not measured whole-Neo4j memory. [S3]

D16 goes further for a particular stationary PageRank
problem: symmetric edges weighted by shared-feature count,
without self edges and with specified dangling behavior.
It does not require identical entity neighborhoods.

```text
Entity rows on SSD
  entity --> feature IDs + degree
                  |
                  v
       +------------------------+
       | Feature state in RAM   |
       | current[F] + next[F]   |
       +------------------------+
                  |
          repeated row scans
                  |
                  v
       +------------------------+
       | Certified error bound  |
       +------------------------+
                  |
                  v
       Stream reconstructed entity scores
```

The feature matrix is applied through rows; it must not be
materialized as a potentially dense F-by-F matrix. [S4]

```text
CONSTRUCTED D16 EXAMPLE

Entities                         1 billion
Features                         1 million
Memberships                      2 billion

Two entity-sized f64 vectors     16 GB
Two feature-sized f64 vectors    16 MB
Selected vector reduction        1,000x

Named prepared portfolio         about 36.032 GB
Full binary ID + score export    16 GB before framing
```

That is 16 MB of selected changing vectors, not 16 MB for
the whole application. The example still scans 20 GB of
rows per update. Its stated 1e-8 error target at damping
0.85 allows 113 updates: 2.26 TB of row traffic, or about
75 minutes of service at an assumed 500 MB/s, before other
work. This is arithmetic, not a measured runtime. [S4]

Stationary error accuracy is not automatically GDS's
finite-iteration output. Arbitrary directed graphs and
binary "shares any feature" projections need other proofs.

## 4. D17: Make The Next Snapshot Cheaper

D17 tests whether the previous feature state is already
accurate enough for a changed membership snapshot. It
requires fixed entity/feature universes, IDs, damping and
personalization, plus complete change discovery. [S5]

```text
Membership changes
       |
       v
Find ALL affected old / new rows
       |
       v
Update feature residual, including global correction
       |
       +-- error bound passes --> reconstruct new answer
       |
       +-- error bound fails ---> more solver updates
                                      |
                                      v
                                 reconstruct answer

Either route --> physical storage gate --> publish version
```

Its three feature arrays occupy 24 MB at one million
features. Complete affected-row discovery, numerical error
accounting, storage and output cost remain additional.
Reusing solver state generally does not mean old scores
are unchanged or that an entire output can be reused.

Independent review found two concrete hazards, now addressed
in the design: keep singleton membership provenance for
later reactivation, and budget physical file replacement.
A tiny logical change can rewrite a large packed file.
The example's 44.040 GB portfolio plus a distinct new 8 GB
score plane exceeds the 50 GB retained-storage cap. [S5], [S6]

## 5. The Seventeen Options At A Glance

These are research candidates. The IDs are useful pointers,
not seventeen features required for the first release. [S3]

```text
FOUNDATION
  D01  Bounded builder, including hubs and wide properties
  D02  Account for physical allocations and retained versions

INVESTIGATION AND REUSE
  D03  Physical-order reads, logical-order traversal results
  D04  Certify unchanged distance / component answers
  D05  Certify the selected projection did not change

STRUCTURAL ANALYTICS
  D06  Incidence execution without expanded pairwise edges
  D07  PageRank class mass for identical probability rows
  D10  Shared top-k tables for identical feature-set classes
  D16  Feature-state stationary PageRank
  D17  Changed-row feature-residual refresh

GLOBAL AND JOIN-HEAVY WORK
  D08  PageRank with one resident contribution vector
  D09  Residual / changed-score bounds to skip valid work
  D11  Source-batched exact similarity, including zero ties
  D12  Safe multiresolution bounds for intersections
  D13  Bounded factorized fixed-length joins
  D14  Embedding dimension stripes with normalization replay
  D15  Community histograms with revalidated moves
```

Several foundations are established prior art. The notes'
contribution is narrower operator contracts, schedules,
counterexamples and proposed integrations. They make no
verified worldwide novelty claim.

## 6. What The Numbers Actually Establish

Keep historical measurements separate from selected-state
calculations. None of the ratios below can be multiplied
into a universal speed or RAM estimate. [S1], [S3]

```text
HISTORICAL NARROW BOLT BENCHMARK

Metric                      Neo4j        Knight Bus
Warm p99                    5.3027 ms    3.9703 ms
Sampled stack RSS           374.05 MB    234.18 MB

Recorded change             25.1% lower p99
                            37.4% lower sampled RSS

Scope: three dependency query families, small outputs.
Not rerun for this summary; not a 4 GB lifecycle proof.

----------------------------------------------------------

SELECTED-COMPONENT MODELS, NOT MEASURED PRODUCT RATIOS

Candidate   Reference payload    Candidate payload
D06         40.04 GB topology    0.16008 GB topology
D07         16 GB vectors        0.16 GB class vectors
D08         3.2 GB vectors       1.6 GB contribution vector
D10         12 GB top-k tables   0.532 GB tables + class map
D11         8 GB active queues   0.016 GB batch queues
D16         16 GB vectors        0.016 GB feature vectors
```

Each model uses a different declared shape. D07 requires
10 million classes for one billion entities. D08 uses
200 million vertices. D10 assumes 100 million entities,
one million classes and k=10. D11 replaces queues for
50 million sources with a 100,000-source batch at k=10.
Candidate discovery and full output remain real work.

The research also reports small mathematical oracle checks,
including 36,864 D16 stationary comparisons and 110,592
D17 refresh checks. These support restricted identities;
they do not demonstrate production floating-point
certification, large-scale performance or a paying buyer.

## 7. The Experiment That Should Come Next

The final recommendation is one real dependency investigation,
with a second question and a changed-data replay. Check the
source for useful incidence structure early; a qualifying
customer job can move D06 or D16 ahead of traversal. [S1], [S7]

```text
ONE CUSTOMER WORKFLOW

Input snapshot
    |
    v
Bounded validation + ID mapping + preparation
    |
    v
First useful answer, independently checked
    |
    v
Second seed / question on the same prepared snapshot
    |
    v
Changed-data refresh + another verified answer

Every stage: RAM + live disk + time + correctness + output
```

The notes frame the next quarter as decision checkpoints:

```text
Week 1     Actual failed job, owner, baseline, changed input
              |
              v
Month 1    Complete source-to-answer-to-refresh experiment
              |
              v
Quarter    Repeated use and measured value: continue / pivot
           / stop; add only the next justified capability
```

Compare with the repaired incumbent and a relevant simple
or factor-aware alternative. Include cold preparation,
full output, failure recovery and unsuccessful jobs.
Stop or change direction when the required semantics,
resource limits, freshness or deadline cannot be met.

The meaningful result is a completed useful job within its
budget and decision window. Lower RAM enables that result;
a small allocation number alone does not establish it.

## Reading Notes

This summary follows the final decision brief and decision
map, then checks the workflow, D16/D17 proposals, reviews
and timelines. It does not repeat the original corpus audit
or run new algorithms or benchmarks.

The folder's audit reports coverage of 410 source prose
entries and narrower inspection of 55 supporting tables.
Those are the research run's coverage claims, not a claim
that this summary independently reread all sources. [S8]

[S1]: Final-Research-Decision-Brief.md
[S2]: End-To-End-Workflow.md
[S3]: Architecture-Decision-Map.md
[S4]: Feature-State-PageRank.md
[S5]: Feature-Refresh-Certificates.md
[S6]: 03-feature-refresh-Review.md
[S7]: Product-Decision-Timelines.md
[S8]: Source-Coverage-Completion-Audit.md
