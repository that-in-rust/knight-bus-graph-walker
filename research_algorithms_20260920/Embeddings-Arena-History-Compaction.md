# In-Place Compaction Of Certified Embedding Histories

Date: 2026-09-20. Implemented A06 storage improvement driven by a measured failure of payload-only reasoning. Complete-output verification and 36 four-mode fresh-process runs passed. Not an established new compaction principle or a physical 4 GB claim.

## Observation And Decision

In the first fresh-process cardinality run of the packed experiment, ordinary mean histories occupied 8,650,752 peak packed bytes versus 10,485,760 for exact-sum replay, but maximum process RSS was 15,663,104 versus 13,074,432 bytes. The 17.5% payload reduction did not become a process-memory win. All repeat receipts must be considered before estimating an effect. Allocator retention and allocation sizes are plausible explanations, not a diagnosed causal finding.

The mean implementation also writes and reloads every compact plane to avoid simultaneous separate wide and compact allocations. That traffic is a consequence of the implementation, not a mathematical requirement of the algorithm. A fixed-capacity arena and forward in-place compaction can remove it while retaining exactly the same mean trajectory and output certificate.

## Contract

Use exactly the existing p=0 integer-mean update, clipping, seeded input, dyadic normalized output and observed-defect gate. This proposal changes only storage and its lifetime. The entire output must be byte-identical to ordinary mean replay and the fused disk control. Do not improve apparent performance by dropping normalization, arithmetic checks, validation, conversion-defect measurement or result synchronization.

Let C be the byte length of a mean plane. Let H' be the byte length of a workspace sum plane whose per-cell width is at least the mean width as well as the exact sum's required width. For active features with cardinality at least two, the ordinary exact-sum width already satisfies this condition. For permitted inactive features, ordinary sum storage may be narrower than mean storage; promote the workspace width for those cells, or explicitly encode omitted zeros. Silently assuming all source features are active would make the compaction proof false for some accepted inputs.

## Single-Arena Schedule

For L>=1 allocate one byte arena with capacity at least

```text
P = max((L-1)*C + H', L*C).
```

Charge its actual capacity, not just its current used prefix. There is one shared sum layout and one shared mean layout. For L=0 no feature-history arena is needed.

At the start of history stage t, the first t*C bytes contain immutable mean planes a0,...,a_(t-1). The next H' bytes are zeroed and used for the exact sums H_t. Reconstruct source rows from the immutable prefix and accumulate only into that disjoint suffix. Convert H_t forward into a_t at the same suffix start. The new immutable prefix has length (t+1)*C. Repeat. Final output replays from the L compact histories as before.

```text
Before conversion:
  +----------------------+--------------------------+
  | t immutable means    | current wide exact sums  |
  +----------------------+--------------------------+
                         ^
                         | shared start address
After forward conversion:
  +----------------------+----------------+---------+
  | t immutable means    | one new mean   | reusable|
  +----------------------+----------------+---------+
```

No conversion file, extra plane allocation, unsafe aliasing or operating-system memory-release promise is required. Ordinary safe byte slices can implement the read/round/write operation. Each new accumulator must be cleared after the preceding compact prefix is sealed; otherwise stale bytes contaminate sums.

## Why Overlap Is Safe

Flatten the current feature-major, coordinate-minor cells into indices q. Let h_q and c_q be input and output byte widths with c_q<=h_q. Before processing q, all original input cells with index less than q have been consumed. Read the entire signed input cell into the admitted i128 scratch, compute its correctly rounded mean and observed rounding defect, then write its compact signed value.

The written prefix ends at sum_(j<=q)c_j, no later than sum_(j<=q)h_j, the end of the already consumed input. Therefore no write can corrupt an unconsumed input cell. The old retained-history prefix is disjoint from the entire workspace. Induction proves that all compact codewords and defects equal the separate-buffer converter. The arithmetic, clipping, layer ordering and output gate are unchanged, so complete output equality follows.

This argument depends on the cell ordering and per-cell width inequality. It does not justify arbitrary permutation, reverse traversal, variable-length records with unknown boundaries or expansion into a smaller workspace.

## Resource Delta

| Quantity | File-Staged Mean Replay | Arena Mean Replay |
|---|---|---|
| Peak packed storage | max((L-1)C+H,LC), spread across allocations | One actual-capacity allocation for max((L-1)C+H',LC) |
| Conversion traffic | L*C reads and L*C writes | Zero conversion-file bytes |
| History reconstruction | Quadratic in depth for this executor | Unchanged |
| Mean and node quantization | Existing certified trajectory | Identical |
| Complete output bytes | Existing binary format | Identical |
| Physical RSS | Must measure; freed allocations need not disappear | Must measure; one arena does not bound all process or machine memory |

For an all-active source H'=H. In the cardinality scenario this targets the same 8,650,752 packed bytes while removing 16,777,216 bytes of conversion traffic. It does not remove the roughly quadratic replay work or the output cost. The disk baseline can still win on latency and kernel state.

## Falsifiers And Implementation Plan

1. Before implementation, add a failing test requiring an arena mode and complete equality with ordinary mean and disk modes.
2. Exercise mixed signed cell values, tight packed-width boundaries and a source containing inactive features; compare with the non-overlapping converter and preserve old history prefixes.
3. Measure actual arena capacity and require conversion read/write counters to remain zero. Test L=0, precision refusal, successful complete output and cleanup.
4. Run the independent rational whole-output oracle against the improved executable, including its claimed error bound rather than only the much looser requested tolerance.
5. Run the same cardinality, alignment and depth scenarios in fresh processes. Preserve the old measurements and repeat uncertainty. Separate algorithmic traffic savings, allocated payload, process RSS and whole-machine memory.

The original three-mode source is archived in `experiments/packed-engine-v1.tar.gz` so its measurements remain reproducible when the main experimental source changes. No production Cargo code is affected.

## Executed Correctness Evidence

The current Rust engine adds `arena` as a fourth mode. The implementer observed its new tests fail before implementation; the lead independently compiled warnings-denied test/release binaries and observed all 16 tests pass. Focused cases include signed mixed-width compaction, unchanged retained prefixes, cleared successor workspace, actual capacity charging, depth-zero allocation, inactive-feature promotion and refusal cleanup.

The lead's expanded external suite passed 28 complete runs across seven source cases and four modes, checking all 2,352 coordinates against the exact error bound claimed by each run, not just TAU. All 14 complete mean/disk and mean/arena file comparisons passed. Eight precision/truncation cases refused publication. Receipts are in [packed-verification-v2.jsonl](experiments/packed-verification-v2.jsonl). The byte-equality checks establish equality on these cases; the schedule proof explains the general invariant.

Adding an inactive-feature case exposed an overly restrictive check in the independent oracle: it rejected cardinality zero even though the source contract only rejects cardinality one. A new failing regression was observed, then that condition was corrected; all six oracle self-tests passed. This did not require changing the graph operator or accepting singleton features. Earlier all-active results are unaffected.

The four-mode common-build measurement completed 36 successful fresh-process runs and 18 full-file comparisons. Arena removed all conversion-file traffic. Relative to exact-sum replay, its median RSS fell 13.7% in cardinality and 17.2% in depth, with respective median time reductions of 2.2% and 2.7%. Alignment showed essentially unchanged RSS and a 1.5% time regression. Absolute RSS savings were about 1.79 MB and 0.62 MB, not GB-scale observations. The fused disk control remained faster in all three cases. Full medians, ranges, separate footprint statistics and scope are in [V2 evidence](Embeddings-Packed-Workflow-Evidence.md#v2-arena-results).

The measurements support this specific allocation/compaction improvement, not a claim that quantization always lowers memory or that the chosen B values are optimal for the requested tolerance. Arena allocation and compaction can also be adopted by a strong generic factor-aware comparator.

## Scientific Boundary

Forward in-place compaction and arena allocation are established engineering techniques. The original input/output-order proof above supports their use in this certified mean-history schedule; it is not evidence that the general technique is new. A generic factor-aware implementation can adopt the same optimization. This experiment can establish a useful storage/lifetime result and reject an inferior implementation, but cannot by itself establish a paper-worthy scientific contribution or all seven requested innovations.

The inspected [Rust Vec guarantees](https://doc.rust-lang.org/std/vec/struct.Vec.html#guarantees) distinguish length from allocated capacity, allow the allocator to return more capacity than requested, and do not automatically shrink a vector. Those API guarantees support charging actual capacity and avoiding reliance on a cleared vector releasing memory. They do not explain the measured RSS difference by themselves. The web page served Rust 1.98.1; this experiment compiles on 1.93.1, and its actual capacities and builds are checked locally. A version-pinned 1.93.1 page and an OpenXLA buffer-assignment page were attempted but unavailable in this browsing pass; neither is used as inspected evidence.
