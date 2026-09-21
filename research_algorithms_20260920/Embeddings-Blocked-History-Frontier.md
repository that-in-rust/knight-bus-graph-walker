# Embedding Histories Between Replay And Disk State

Date: 2026-09-20. A06 schedule derivation prompted by the packed-workflow baseline. This is a new costed algorithm proposal, not an implemented fourth mode or a claimed new checkpointing principle.

## Why The Binary Choice Is Incomplete

Full mean-history replay keeps L feature planes and avoids node-state I/O, but reconstructs each node repeatedly. Fused disk propagation keeps only current means and next sums and performs one update per node per layer, but repeatedly reads/writes both node state and partial normalized output. Neither endpoint must dominate the other on every machine.

A middle schedule keeps a short **block of feature histories**, checkpoints raw node state only at block boundaries, and folds a whole block's output contribution into its disk accumulator at once. It preserves exactly the same rounded trajectory and candidate output as the mean algorithm. No new quantization or approximation is introduced by changing the block partition.

The useful customer contract is therefore not only a RAM cap. It can include maximum intermediate bytes written, persistent/scratch space and a deadline. A plan that uses less RAM but rewrites hundreds of GB may not be the desired option.

## Supported Contract

Use the same fixed-grid, frozen-snapshot p=0 mean update, exact integer arithmetic, signed dyadic output weights and observed-error certificate as [the mean manuscript](Embeddings-Mean-History-Replay.md). A checkpoint is the raw integer node row, not its normalized value or accumulated output. Node integer and output accumulator widths must represent their contents exactly.

The initial row is regenerable from stable ID/seed, as in the current experiment. If the initial vectors instead arrive as a large external table, their additional rereads are real and change the first-block advantage below. Checkpoint layout must preserve node ordering/IDs and snapshot identity.

Partition the L transitions into positive block lengths b1,...,bS with sum L. L=0 is the separately handled initial-output case. All equations below assume L>=1.

## Block Algorithm

1. Stream initialization once to form exact H0 and freeze the first means a0. No raw node checkpoint or partial-output file is necessary yet.
2. At a block starting at depth k, read the raw checkpoint X_k row by row, or regenerate X0 for the first block. The block already has a_k, produced at initialization or the preceding boundary pass.
3. For local depths j=1,...,b-1, replay each row from X_k using a_k,...,a_(k+j-1), aggregate exact H_(k+j), and freeze its means. This uses b-1 source/checkpoint passes and no node-vector plane in RAM.
4. In one final block pass, replay each row through all b transitions. Fold only this block's newly owned layer contributions into the output endpoint accumulator; the first block also includes layer zero. Collect transition defects and norm minima using the original global layer indices.
5. If another block follows, overwrite the raw checkpoint with X_(k+b) after consuming the old row, update the partial-output file, and simultaneously aggregate H_(k+b). Keep the current b histories until that pass finishes. Then release them, convert the new sum plane, and start the next block.
6. At the last boundary, do not write another raw checkpoint or partial-output plane. Read any prior output endpoints, combine the last block, and write the final staged binary64 rows directly. Publish only after the complete observed-error certificate passes.

Independent sequential read/write cursors make in-place transient checkpoint/endpoint updates valid: each write concerns a row already consumed, and frozen histories remove any dependency on another old raw row. Read-ahead does not change this ordering. A failed in-place checkpoint may require restarting the job from source; this is not an atomic, recoverable checkpoint protocol. A recovery-preserving old/new-file schedule must pay the extra overlap.

```text
raw X_k on disk --> local histories a_k ... a_(k+b-1)
       |                         |
       +---- replay one row -----+
                    |
                    v
          raw X_(k+b) + block output
                    |
         +----------+-----------+
         |                      |
         v                      v
 next checkpoint + H_(k+b)   final staged result
     [nonfinal block]          [last block]
```

## Correctness And Certificate Preservation

Induct on block boundaries. Assume the checkpoint equals the full mean algorithm's X_k and the accumulated endpoints enclose exactly the candidate contributions already owned by earlier blocks. The same frozen means and deterministic integer rounding reproduce each subsequent X_(k+j). Their exact sums therefore freeze the same mean codewords as uninterrupted execution. The boundary checkpoint is X_(k+b), not an approximation to it.

Each layer contributes to the output exactly once. Endpoint addition and signed dyadic coefficient multiplication are exact; candidate normalization uses the same outward enclosure. Parenthesization across blocks cannot change endpoint sums. Observed mean and pre-clipping node-rounding maxima are measured for every global transition; recomputing a transition gives the same value, and taking maxima twice is harmless. Global norm minima and publication checks consequently retain their guarantees.

This is not a bitwise claim for arbitrary floating-point accumulators, lossy checkpoints, different scales per block or changed snapshots. Those changes need separate error terms or a different contract.

## Exact Schedule Counts

Let C be one packed mean-plane payload, H one exact-sum-plane payload, V the raw node-checkpoint bytes, and A the partial-output endpoint bytes. In the current Rust format V=4*n*d and A=16*n*d. Metadata, I/O pools, local gathers, certificates and final output are additional.

Ignoring fixed framing bytes, the extra state I/O compared with full history replay is

```text
raw checkpoint reads    (L-b1)*V
raw checkpoint writes   (S-1)*V
partial output reads    (S-1)*A
partial output writes   (S-1)*A

total extra state I/O = (L-b1+S-1)*V + 2*(S-1)*A.
```

The first block regenerates initialization rather than reading a raw checkpoint. Every later block reads that checkpoint b_j times. Only nonfinal blocks write a checkpoint or partial output. Source passes total L+1: one initial H0 pass, then b_j passes per block. All plans freeze L mean planes in total, so their mean-conversion traffic is common under the chosen conversion schedule. Output normalization is done for each requested layer once, during its block's final pass, not during history-building replays.

Node transition replays per node are exactly

```text
T = sum_j b_j*(b_j+1)/2.
```

This counts reconstructed transitions, not seconds; membership gathers, integer arithmetic, packing and output work remain priced separately. A nonfinal block of length b has peak kernel-plane payload b*C+H because its final pass retains b histories while accumulating the next block's H. A final block has peak max((b-1)*C+H,b*C). O(b*d) gather state must be added. Omitting the next-block accumulator underprices the middle plans.

For uniform B dividing L, S=L/B:

```text
T = L*(B+1)/2
extra state I/O = (L-B+L/B-1)*V + 2*(L/B-1)*A.
```

B=L recovers full history replay with zero state I/O. B=1 recovers an endpoint-optimized fused disk algorithm with 2*(L-1)*(V+A) extra state I/O. An implementation that initializes both disk planes eagerly or rewrites/rereads final endpoints pays more; benchmark receipts must say which version was actually run.

### Eight-Layer Illustration

Use the prior conditional C=25.6 MB, H=32 MB, n=20 million and d=64. Then V=5.12 GB and A=20.48 GB. This is arithmetic, not a dataset, machine measurement or admitted full lifecycle.

| Uniform Block B | Transition Replays Per Node | Peak Kernel Planes | Extra State I/O |
|---|---:|---:|---:|
| 1 | 8 | 57.6 MB | 358.4 GB |
| 2 | 12 | 83.2 MB | 168.96 GB |
| 4 | 20 | 134.4 MB | 66.56 GB |
| 8 | 36 | 211.2 MB | 0 GB |

The middle plans do not have the smallest value in every column. They occupy possible Pareto points whose elapsed-time ranking depends on effective I/O bandwidth, cache behavior, integer-compute throughput and overlap. Full output remains about 10.4 GB in the current binary64-plus-ID format, regardless of block size. Checkpoint plus accumulator is 25.6 GB when both are live; combining them with final output, input, prepared data, conversion files and old generations requires an explicit disk schedule before claiming a 50 GB fit.

## Exact Planner For This Restricted Portfolio

For a hard admitted memory cap, discard a proposed nonfinal block if b*C+H plus its other live state exceeds the cap; use the final-block formula for its last block. Each edge (k,k+b) of a DAG over depths 0..L represents one feasible block.

An edge contributes b*(b+1)/2 transition replays; b*V raw reads when k>0; and V+2A state I/O when k+b<L. The first and last block exceptions are explicit. Given nonnegative calibrated weights for replay and I/O, shortest path yields the minimum modeled cost among these partitions in O(L^2) edge work and O(L) DP state, plus reconstruction. This is ordinary DAG dynamic programming, not a new optimal checkpointing theorem. It is not optimal over arbitrary schedules, lossy representations, parallel overlap or other algorithms.

For separate write/read constraints, retain an exact nondominated label set or use a bounded discretization with a stated approximation; do not silently call a weighted sum every possible Pareto point. Memory metadata/local slabs can be handled in each edge's admission function rather than disappearing from the model. Calibration values are measurements or assumptions, not deterministic latency promises.

## Prior Art And Boundary

[Beaumont et al., Optimal checkpointing for heterogeneous chains](https://arxiv.org/pdf/1911.13214), Sections 2 and 3.1 inspected in primary full text, explicitly discusses periodic segments, saved inputs versus complete intermediate histories, operation-level peak memory and checkpoint scheduling. Its task is reverse-mode training; this proposal is forward normalized graph propagation with two differently sized state domains and paid disk output accumulation. Segment checkpointing and dynamic programming are inherited ideas, not inventions here.

The original Revolve DOI [10.1145/347837.347846](https://doi.org/10.1145/347837.347846) was located, but the primary publisher page could not be retrieved in this pass. No uninspected Revolve theorem is used in the argument above. A generic factor-aware executor can implement the same blocked schedule; novelty of the composition and a useful empirical frontier remain open.

## Verification State

The initial schedule-count execution caught an arithmetic error in the B=4 illustrative row: its extra state I/O is 66.56 GB, not 71.68 GB. After correcting that table entry, the probe exited 0 with 240 planner cases and 24,552 enumerated partition checks. A second execution added rational calibrated weights and corrected parenthesization so the weighted CPU cost is not floored. It exited 0 with 300 planner cases and 30,690 partition checks, including all four illustrative rows. This verifies the restricted model/planner, not graph propagation or file lifetimes in code. There is no fourth mode in the active Rust experiment.

```python
from itertools import product
from fractions import Fraction

def enumerate_positive_depth_partitions(depth):
    for cuts in product((0,1),repeat=depth-1):
        result,last=[],0
        for position,cut in enumerate(cuts,1):
            if cut: result.append(position-last); last=position
        yield result+[depth-last]

def price_block_partition_schedule(parts, C, H, V, A, cap, cpu, io):
    peak=steps=traffic=0
    for j,b in enumerate(parts):
        final=j==len(parts)-1
        peak=max(peak,max((b-1)*C+H,b*C) if final else b*C+H)
        steps+=b*(b+1)//2
        traffic+=(b*V if j else 0)+(0 if final else V+2*A)
    if peak>cap: return None
    return cpu*steps+io*traffic,steps,traffic,peak

def select_exact_block_partition(depth,C,H,V,A,cap,cpu,io):
    cost=[None]*(depth+1); parent=[None]*(depth+1); cost[0]=0
    for end in range(1,depth+1):
        for start in range(end):
            if cost[start] is None: continue
            b=end-start
            memory=max((b-1)*C+H,b*C) if end==depth else b*C+H
            if memory>cap: continue
            traffic=(b*V if start else 0)+(0 if end==depth else V+2*A)
            candidate=cost[start]+cpu*(b*(b+1)//2)+io*traffic
            if cost[end] is None or candidate<cost[end]: cost[end]=candidate;parent[end]=start
    if cost[-1] is None: return None
    parts=[];end=depth
    while end:
        start=parent[end];parts.append(end-start);end=start
    parts.reverse()
    return cost[-1],parts

cases=partitions=0
for depth in range(1,11):
    for cap in (7,9,12,16,24,48):
        for cpu,io in ((1,0),(0,1),(1,1),(17,2),(Fraction(11,10),Fraction(1,3))):
            candidates=[]
            for parts in enumerate_positive_depth_partitions(depth):
                priced=price_block_partition_schedule(parts,3,4,5,20,cap,cpu,io)
                if priced is not None: candidates.append(priced[0])
                expected=(depth-parts[0]+len(parts)-1)*5+2*(len(parts)-1)*20
                if priced is not None: assert priced[2]==expected
                partitions+=1
            selected=select_exact_block_partition(depth,3,4,5,20,cap,cpu,io)
            assert (selected is None)==(not candidates)
            if selected:
                assert selected[0]==min(candidates)
                assert price_block_partition_schedule(selected[1],3,4,5,20,cap,cpu,io)[0]==selected[0]
            cases+=1
for B,steps,peak,traffic in ((1,8,57600000,358400000000),(2,12,83200000,168960000000),
                             (4,20,134400000,66560000000),(8,36,211200000,0)):
    actual=price_block_partition_schedule([B]*(8//B),25600000,32000000,5120000000,20480000000,10**12,1,1)
    assert actual[1:]==(steps,traffic,peak)
print('block_cost_model',dict(planner_cases=cases,partition_checks=partitions,illustrative_rows=4))
```

The next falsifier is a real blocked executor with exactly matching mean/disk output and instrumented file/plane lifetimes. If a lifetime invalidates these counts, revise the model before quoting the table as achieved behavior.
